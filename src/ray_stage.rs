use std::{fmt::Formatter, sync::Arc};

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::record_batch::RecordBatchReader;
use datafusion::arrow::pyarrow::FromPyArrow;
use datafusion::common::internal_datafusion_err;
use datafusion::error::Result;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::stream::{self, TryStreamExt};
use log::debug;
use prost::Message;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::codec::RayCodec;
use crate::context::CoordinatorId;

#[derive(Debug)]
pub struct RayStageExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Output partitioning
    properties: PlanProperties,
    pub stage_id: String,
}

impl RayStageExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, stage_id: String) -> Self {
        let properties = input.properties().clone();

        Self {
            input,
            properties,
            stage_id,
            // unique names
        }
    }
}
impl DisplayAs for RayStageExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayStageExec[{}] (output_partitioning={:?})",
            self.stage_id,
            self.properties().partitioning
        )
    }
}

impl ExecutionPlan for RayStageExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn name(&self) -> &str {
        "RayStageExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: handle more general case
        assert_eq!(children.len(), 1);
        let child = children[0].clone();
        Ok(Arc::new(RayStageExec::new(child, self.stage_id.clone())))
    }

    /// We will have to defer this functionality to python as Ray does not yet have Rust bindings.
    fn execute(
        &self,
        _partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("Ray Stage Exec")
    }
}
