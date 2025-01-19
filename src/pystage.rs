use crate::isolator::{PartitionIsolatorExec, ShadowPartitionNumber};
use crate::protobuf::StreamMeta;
use crate::util::{bytes_to_physical_plan, physical_plan_to_bytes, ResultExt};
use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{FlightClient, FlightDescriptor, PutResult};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::error::DataFusionError;
use datafusion::parquet::data_type::AsBytes;
use datafusion::physical_plan::displayable;
use datafusion_python::physical_plan::PyExecutionPlan;
use object_store::aws::AmazonS3Builder;
use prost::Message;
use std::borrow::Cow;
use std::env;
use std::sync::Arc;
use tonic::async_trait;
use tonic::transport::{Channel, Uri};
use url::Url;

use arrow::array::RecordBatch;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::{
    execution::SendableRecordBatchStream,
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
};
use datafusion_python::utils::wait_for_future;
use futures::{StreamExt, TryStreamExt};
use pyo3::prelude::*;

use anyhow::Result;

use crate::context::{CoordinatorId, ExchangeAddr};

#[pyclass]
pub struct PyStage {
    client: FlightClient,
    #[pyo3(get)]
    stage_id: String,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
    fraction: f64,
    shadow_partition_number: Option<usize>,
}

#[pymethods]
impl PyStage {
    #[new]
    #[pyo3(signature = (stage_id, plan_bytes, exchange_addr, shadow_partition_number=None, bucket=None, fraction=1.0))]
    pub fn from_bytes(
        py: Python,
        stage_id: String,
        plan_bytes: Vec<u8>,
        exchange_addr: String,
        shadow_partition_number: Option<usize>,
        bucket: Option<String>,
        fraction: f64,
    ) -> PyResult<Self> {
        println!(
            "PyStage[{}-s{}] from_bytes, bucket {} fraction {}",
            stage_id,
            shadow_partition_number.or(Some(0)).unwrap(),
            bucket.as_ref().or(Some(&"None".to_string())).unwrap(),
            fraction
        );
        let mut config = SessionConfig::new();

        // this only matters if the plan includes an PartitionIsolatorExec
        // and will be ignored otherwise
        if let Some(shadow) = shadow_partition_number {
            config = config.with_extension(Arc::new(ShadowPartitionNumber(shadow)));
        }

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);

        if let Some(bucket) = bucket {
            let mut s3 = AmazonS3Builder::from_env().with_bucket_name(&bucket);

            let s3 = s3.build().to_py_err()?;

            let path = format!("s3://{bucket}");
            let s3_url = Url::parse(&path).to_py_err()?;
            let arc_s3 = Arc::new(s3);
            ctx.register_object_store(&s3_url, arc_s3.clone());
            println!("registered object store {s3_url}");
        }

        println!("creating physical plan from bytes");
        let plan = bytes_to_physical_plan(&ctx, &plan_bytes).to_py_err()?;
        println!(
            "created physical plan:\n{}",
            displayable(plan.as_ref()).indent(true)
        );

        let url = format!("http://{exchange_addr}");

        let chan = Channel::from_shared(url).to_py_err()?;
        let fut = async { chan.connect().await };
        let channel = match wait_for_future(py, fut) {
            Ok(channel) => channel,
            _ => {
                return Err(pyo3::exceptions::PyException::new_err(
                    "error connecting to exchange".to_string(),
                ));
            }
        };

        let client = FlightClient::new(channel);

        Ok(Self {
            client,
            stage_id,
            plan,
            ctx,
            fraction,
            shadow_partition_number,
        })
    }

    pub fn execute(&mut self, py: Python, partition: usize) -> PyResult<()> {
        println!("PyStage[{}] executing", self.stage_id);
        wait_for_future(py, self.consume(partition)).to_py_err()
    }

    pub fn num_output_partitions(&self) -> usize {
        self.plan.output_partitioning().partition_count()
    }

    /// How many partitions are we shadowing if at all
    pub fn num_shadow_partitions(&self) -> Option<usize> {
        let mut result = None;
        self.plan
            .clone()
            .transform_down(|node: Arc<dyn ExecutionPlan>| {
                if let Some(isolator) = node.as_any().downcast_ref::<PartitionIsolatorExec>() {
                    let children = isolator.children();
                    if children.len() != 1 {
                        return internal_err!("PartitionIsolatorExec must have exactly one child");
                    }
                    result = Some(children[0].output_partitioning().partition_count());
                    //TODO: break early
                }
                Ok(Transformed::no(node))
            });
        result
    }

    pub fn execution_plan(&self) -> PyExecutionPlan {
        PyExecutionPlan::new(self.plan.clone())
    }

    pub fn schema(&self) -> PyArrowType<Schema> {
        let schema = (*self.plan.schema()).clone();
        PyArrowType(schema)
    }

    pub fn plan_bytes(&self) -> PyResult<Cow<[u8]>> {
        let plan_bytes = physical_plan_to_bytes(self.plan.clone())?;
        Ok(Cow::Owned(plan_bytes))
    }
}

impl PyStage {
    pub async fn consume(&mut self, partition: usize) -> Result<()> {
        println!(
            "PyStage[{}:{}-{}] consuming",
            self.stage_id,
            partition,
            self.shadow_partition_number
                .map(|s| s.to_string())
                .unwrap_or("n/a".into())
        );
        let ctx = self.ctx.task_ctx();

        let stream_meta = StreamMeta {
            stage_num: self.stage_id.parse::<u32>()?,
            partition_num: partition as u32,
            fraction: self.fraction as f32,
        };

        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: stream_meta.encode_to_vec().into(),
            path: vec![],
        };

        let stream = self
            .plan
            .execute(partition, ctx)?
            .map_err(|e| FlightError::from_external_error(Box::new(e)));
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(stream);
        println!(
            "PyStage[{}:{}-{}] got stream",
            self.stage_id,
            partition,
            self.shadow_partition_number
                .map(|s| s.to_string())
                .unwrap_or("n/a".into())
        );

        let mut response = self
            .client
            .do_put(flight_data_stream)
            .await
            .map_err(|e| internal_datafusion_err!("error getting back do put result: {}", e))?;

        println!(
            "PyStage[{}:{}-{}] did do_put",
            self.stage_id,
            partition,
            self.shadow_partition_number
                .map(|s| s.to_string())
                .unwrap_or("n/a".into())
        );

        while let Some(result) = response.next().await {
            match result {
                Err(e) => {
                    return Err(internal_datafusion_err!(
                        "error getting back do put result: {}",
                        e
                    )
                    .into());
                }
                _ => (),
            }
        }
        Ok(())
    }
}

#[pyclass]
pub struct PyRecordBatch {
    batch: RecordBatch,
}

#[pymethods]
impl PyRecordBatch {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        self.batch.to_pyarrow(py)
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(batch: RecordBatch) -> Self {
        Self { batch }
    }
}

#[pyclass]
pub struct PyRecordBatchStream {
    stream: SendableRecordBatchStream,
}

impl PyRecordBatchStream {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

#[pymethods]
impl PyRecordBatchStream {
    fn next(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        let result = self.stream.next();
        match wait_for_future(py, result) {
            None => Ok(None),
            Some(Ok(b)) => Ok(Some(b.to_pyarrow(py)?)),
            Some(Err(e)) => Err(e.into()),
        }
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        self.next(py)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}
