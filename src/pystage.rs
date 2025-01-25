use crate::isolator::{PartitionIsolatorExec, ShadowPartitionNumber};
use crate::protobuf::StreamMeta;
use crate::util::{bytes_to_physical_plan, physical_plan_to_bytes, ResultExt};
use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use arrow::util::pretty::pretty_format_batches;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{FlightClient, FlightDescriptor};
use async_stream::stream;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::physical_plan::{collect, displayable};
use datafusion_python::physical_plan::PyExecutionPlan;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use object_store::aws::AmazonS3Builder;
use prost::Message;
use std::borrow::Cow;
use std::future::Future;
use std::sync::Arc;
use tokio::{join, try_join};
use tonic::transport::Channel;
use url::Url;

use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_python::utils::wait_for_future;
use futures::{StreamExt, TryStreamExt};
use pyo3::prelude::*;

use anyhow::Result;

pub struct ExchangeFlightClient(pub FlightClient);

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

        // make our own clone as FlightClient is not Clone, but inner is
        let inner = client.inner().clone();
        let client_clone = FlightClient::new_from_inner(inner);

        let mut config =
            SessionConfig::new().with_extension(Arc::new(ExchangeFlightClient(client_clone)));

        // this only matters if the plan includes an PartitionIsolatorExec
        // and will be ignored otherwise
        if let Some(shadow) = shadow_partition_number {
            config = config.with_extension(Arc::new(ShadowPartitionNumber(shadow)))
        }

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);

        if let Some(bucket) = bucket {
            let s3 = AmazonS3Builder::from_env().with_bucket_name(&bucket);

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

        Ok(Self {
            client,
            stage_id,
            plan,
            ctx,
            fraction,
            shadow_partition_number,
        })
    }

    pub fn execute(&mut self, py: Python) -> PyResult<()> {
        println!("PyStage[{}] executing", self.stage_id);

        let futs = (0..self.num_output_partitions()).map(|partition| {
            let ctx = self.ctx.task_ctx();
            // make our own clone as FlightClient is not Clone, but inner is
            let inner = self.client.inner().clone();
            let client_clone = FlightClient::new_from_inner(inner);
            let plan = self.plan.clone();
            let stage_id = self.stage_id.clone();
            let fraction = self.fraction;
            let shadow_partition_number = self.shadow_partition_number;

            tokio::spawn(consume_stage(
                stage_id,
                shadow_partition_number,
                fraction,
                ctx,
                partition,
                plan,
                client_clone,
            ))
        });

        let stage_id = self.stage_id.clone();
        let fut = async {
            match try_join_all(futs).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    println!("PyStage[{stage_id}] ERROR executing {e}");
                    Err(e)
                }
            }
        };

        wait_for_future(py, fut).to_py_err()
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

pub async fn consume_stage(
    stage_id: String,
    shadow_partition_number: Option<usize>,
    fraction: f64,
    ctx: Arc<TaskContext>,
    partition: usize,
    plan: Arc<dyn ExecutionPlan>,
    mut client: FlightClient,
) -> Result<()> {
    let name = format!(
        "PyStage[{}:{}-{}]",
        stage_id,
        partition,
        shadow_partition_number
            .map(|s| s.to_string())
            .unwrap_or("n/a".into())
    );

    println!("{name} consuming");

    let stream_meta = StreamMeta {
        stage_num: stage_id.parse::<u32>()?,
        partition_num: partition as u32,
        fraction: fraction as f32,
    };

    let descriptor = FlightDescriptor {
        r#type: DescriptorType::Cmd.into(),
        cmd: stream_meta.encode_to_vec().into(),
        path: vec![],
    };
    let mut total_rows = 0;
    let mut plan_output_stream = plan.execute(partition, ctx)?;

    let name_c = name.clone();
    let counting_stream = stream! {
        while let Some(batch) = plan_output_stream.next().await {
            total_rows += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
            //println!("{name_c}: yielding batch:{}", batch.as_ref().map(|b| pretty_format_batches(&[b.clone()]).unwrap().to_string()).unwrap_or("".to_string()));

            yield batch;
        }
        println!(
            "{name_c} produced {total_rows} total rows",
        );
    };

    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(descriptor))
        .with_schema(plan.schema())
        .build(counting_stream.map_err(|e| FlightError::from_external_error(Box::new(e))));

    let name_c = name.clone();
    let mut response = client
        .do_put(flight_data_stream)
        .await
        .map_err(|e| internal_datafusion_err!("{name_c}: error getting back do put result: {e}"))?;

    let name_c = name.clone();
    while let Some(result) = response.next().await {
        if let Err(e) = result {
            return Err(internal_datafusion_err!(
                "{name_c}: error getting back do put result: {e}"
            )
            .into());
        }
    }
    Ok(())
}
