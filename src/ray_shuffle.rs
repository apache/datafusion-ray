use std::pin::{self, Pin};
use std::{fmt::Formatter, sync::Arc};

use arrow::array::RecordBatchIterator;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use async_stream::stream;
use datafusion::arrow::pyarrow::{FromPyArrow, IntoPyArrow, ToPyArrow};
use datafusion::common::internal_datafusion_err;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion::{
    arrow::{datatypes::SchemaRef, ffi_stream::ArrowArrayStreamReader},
    execution::SendableRecordBatchStream,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_python::context::PySessionContext;
use futures::stream::{self, TryStreamExt};
use futures::{Stream, StreamExt};
use prost::Message;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::runtime::Runtime;

#[derive(Debug)]
pub struct RayShuffleExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Output partitioning
    properties: PlanProperties,

    py_inner: Arc<PyObject>,
}

impl RayShuffleExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, py_inner: Arc<PyObject>) -> Self {
        let properties = input.properties().clone();
        println!("new ray shuffle exec");

        Self {
            input,
            properties,
            py_inner,
        }
    }
}
impl DisplayAs for RayShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayShuffleExec(output_partitioning={:?})",
            self.properties().partitioning
        )
    }
}

impl ExecutionPlan for RayShuffleExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn name(&self) -> &str {
        "RayShuffleExec"
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
        Ok(Arc::new(RayShuffleExec::new(child, self.py_inner.clone())))
    }

    /// We will spawn a Ray Task for our child inputs and consume their output stream.
    /// We will have to defer this functionality to python as Ray does not yet have Rust bindings.
    ///

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // serialize our input plan
        let codec = DefaultPhysicalExtensionCodec {};
        let proto = datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(
            self.input.clone(),
            &codec,
        )?;
        let bytes = proto.encode_to_vec();

        // defer execution to the python RayShuffle object which will spawn a Ray Task
        // to execute this partition and send us back a stream of the results
        Python::with_gil(|py| {
            let proto_bytes = PyBytes::new_bound(py, &bytes);
            let py_obj = self
                .py_inner
                .bind(py)
                .call_method1("execute_partition", (proto_bytes, partition))?;
            let record_batch_reader = ArrowArrayStreamReader::from_pyarrow_bound(&py_obj)?;
            Ok::<ArrowArrayStreamReader, PyErr>(record_batch_reader)
        })
        .map_err(|e| internal_datafusion_err!("{e}"))
        .and_then(|py_stream| into_rust_stream(py_stream))
    }
}

/// Convert an ArrowArrayStreamReader from python to a rust SendableRecordBatchStream
fn into_rust_stream(py_stream: ArrowArrayStreamReader) -> Result<SendableRecordBatchStream> {
    let schema = py_stream.schema();

    let the_stream = stream::iter(py_stream).map_err(|e| internal_datafusion_err!("{e}"));

    let adapted_stream = RecordBatchStreamAdapter::new(schema, the_stream);

    Ok(Box::pin(adapted_stream))
}

struct StreamIteratorAdapter<S: Stream<Item = T> + Unpin + Send, T> {
    stream: Pin<Box<S>>,
    runtime: Runtime,
}

impl<S: Stream<Item = T> + Unpin + Send, T> StreamIteratorAdapter<S, T> {
    fn new(stream: S) -> Self {
        Self {
            stream: Pin::new(Box::new(stream)),
            runtime: Runtime::new().unwrap(),
        }
    }
}

impl<S: Stream<Item = T> + Unpin + Send, T> Iterator for StreamIteratorAdapter<S, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.runtime.block_on(self.stream.next())
    }
}

#[pyfunction]
pub fn internal_execute_partition(
    py: Python,
    plan_bytes: Vec<u8>,
    partition: usize,
    ctx: PySessionContext,
) -> PyResult<PyObject> {
    let proto_plan = datafusion_proto::protobuf::PhysicalPlanNode::try_decode(&plan_bytes)
        .map_err(|e| {
            PyRuntimeError::new_err(format!(
                "Unable to decode logical node from serialized bytes: {}",
                e
            ))
        })?;

    let ctx = ctx.ctx;
    let codec = DefaultPhysicalExtensionCodec {};
    let plan = proto_plan.try_into_physical_plan(&ctx, &ctx.runtime_env(), &codec)?;

    let stream_out = plan.execute(partition, ctx.task_ctx())?;

    let py_out =
        StreamIteratorAdapter::new(stream_out.map_err(|e| ArrowError::ExternalError(Box::new(e))));

    let py_out = RecordBatchIterator::new(py_out, plan.schema());

    let reader: Box<dyn RecordBatchReader + Send> = Box::new(py_out);
    reader.into_pyarrow(py)
}
