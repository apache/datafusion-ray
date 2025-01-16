use crate::isolator::{PartitionIsolatorExec, ShadowPartitionNumber};
use crate::util::{bytes_to_physical_plan, physical_plan_to_bytes, ResultExt};
use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use datafusion::common::internal_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::displayable;
use datafusion_python::physical_plan::PyExecutionPlan;
use object_store::aws::AmazonS3Builder;
use std::borrow::Cow;
use std::env;
use std::sync::Arc;
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
use futures::StreamExt;
use pyo3::prelude::*;

use crate::context::CoordinatorId;

#[pyclass]
pub struct PyStage {
    #[pyo3(get)]
    stage_id: String,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
}

#[pymethods]
impl PyStage {
    #[new]
    #[pyo3(signature = (stage_id, plan_bytes, coordinator_id, shadow_partition_number=None, bucket=None))]
    pub fn from_bytes(
        stage_id: String,
        plan_bytes: Vec<u8>,
        coordinator_id: String,
        shadow_partition_number: Option<usize>,
        bucket: Option<String>,
    ) -> PyResult<Self> {
        println!(
            "PyStage[{}-s{}] from_bytes, bucket {}",
            stage_id,
            shadow_partition_number.or(Some(0)).unwrap(),
            bucket.as_ref().or(Some(&"None".to_string())).unwrap()
        );
        let mut config =
            SessionConfig::new().with_extension(Arc::new(CoordinatorId(coordinator_id)));

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

        Ok(Self {
            stage_id,
            plan,
            ctx,
        })
    }

    pub fn execute(&self, py: Python, partition: usize) -> PyResult<PyRecordBatchStream> {
        println!("PyStage[{}] executing", self.stage_id);
        let ctx = self.ctx.task_ctx();

        let result = async { self.plan.execute(partition, ctx) };
        let stream = wait_for_future(py, result)?;
        println!("PyStage[{}] got stream", self.stage_id);
        Ok(PyRecordBatchStream::new(stream))
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
    pub fn new(stage_id: String, plan: Arc<dyn ExecutionPlan>, coordinator_id: String) -> Self {
        let config = SessionConfig::new().with_extension(Arc::new(CoordinatorId(coordinator_id)));

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);

        Self {
            stage_id,
            plan,
            ctx,
        }
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
