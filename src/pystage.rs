use crate::util::{bytes_to_physical_plan, physical_plan_to_bytes, ResultExt};
use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use datafusion_python::physical_plan::PyExecutionPlan;
use std::borrow::Cow;
use std::sync::Arc;

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
    pub fn from_bytes(
        stage_id: String,
        plan_bytes: Vec<u8>,
        coordinator_id: String,
    ) -> PyResult<Self> {
        let config = SessionConfig::new().with_extension(Arc::new(CoordinatorId(coordinator_id)));

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);
        println!("creating physical plan from bytes");
        let plan = bytes_to_physical_plan(&ctx, &plan_bytes).to_py_err()?;
        println!("done");

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
        //let stream = self.plan.execute(partition, ctx)?;
        println!("PyStage[{}] got stream", self.stage_id);
        Ok(PyRecordBatchStream::new(stream))
    }

    pub fn num_output_partitions(&self) -> usize {
        self.plan.output_partitioning().partition_count()
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
