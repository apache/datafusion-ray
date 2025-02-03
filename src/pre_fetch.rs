use std::{fmt::Formatter, sync::Arc};

use datafusion::error::Result;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};
use futures::stream::{Stream, StreamExt};
use tokio::sync::mpsc::channel;

#[derive(Debug)]
pub struct PrefetchExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// maximum amount of buffered RecordBatches
    pub(crate) buf_size: usize,
    /// our plan Properties, the same as our input
    properties: PlanProperties,
}

impl PrefetchExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, buf_size: usize) -> Self {
        // check for only one input
        if input.children().len() != 1 {
            panic!("PrefetchExec must have exactly one input");
        }
        let properties = input.children()[0].properties().clone();
        Self {
            input,
            buf_size,
            properties,
        }
    }
}
impl DisplayAs for PrefetchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PrefetchExec [num={}]", self.buf_size)
    }
}

impl ExecutionPlan for PrefetchExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn name(&self) -> &str {
        "PrefetchExec"
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
        Ok(Arc::new(PrefetchExec::new(child, self.buf_size)))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (tx, mut rx) = channel(self.buf_size);

        let mut input_stream = self.input.execute(partition, context)?;

        let consume_fut = async move {
            while let Some(batch) = input_stream.next().await {
                // TODO: how to neatly errors within this macro?
                tx.send(batch).await.unwrap();
            }
        };

        tokio::spawn(consume_fut);

        let out_stream = async_stream::stream! {
            while let Some(batch) = rx.recv().await {
                yield batch;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema().clone(),
            out_stream,
        )))
    }
}
