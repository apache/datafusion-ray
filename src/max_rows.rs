use std::{fmt::Formatter, sync::Arc};

use datafusion::{
    error::Result,
    execution::SendableRecordBatchStream,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};

use crate::util::max_rows_stream;

#[derive(Debug)]
pub struct MaxRowsExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub max_rows: usize,
}

impl MaxRowsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, max_rows: usize) -> Self {
        Self { input, max_rows }
    }
}

impl DisplayAs for MaxRowsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MaxRowsExec[max_rows={}]", self.max_rows)
    }
}

impl ExecutionPlan for MaxRowsExec {
    fn name(&self) -> &str {
        "MaxRowsExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: generalize this
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(children[0].clone(), self.max_rows)))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input
            .execute(partition, context)
            .map(|stream| max_rows_stream(stream, self.max_rows))
    }
}
