use std::fmt::Display;
use std::pin::Pin;
use std::{fmt::Formatter, sync::Arc};

use arrow::array::{RecordBatch, RecordBatchIterator};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use datafusion::arrow::pyarrow::{FromPyArrow, IntoPyArrow};
use datafusion::common::internal_datafusion_err;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{displayable, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::{
    arrow::datatypes::SchemaRef,
    execution::SendableRecordBatchStream,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_python::utils::wait_for_future;
use futures::stream::TryStreamExt;
use futures::{Stream, StreamExt};
use log::{debug, trace};
use prost::Message;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyIterator};
use uuid::{uuid, Uuid};

use crate::codec::ShufflerCodec;

#[derive(Debug)]
pub struct RayShuffleExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Output partitioning
    properties: PlanProperties,

    pub output_partitions: usize,
    pub input_partitions: usize,

    pub unique_id: String,
}

impl RayShuffleExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        output_partitions: usize,
        input_partitions: usize,
        unique_id: String,
    ) -> Self {
        let properties = input.properties().clone();
        println!("new ray shuffle exec");

        Self {
            input,
            properties,
            output_partitions,
            input_partitions,
            unique_id,
            // unique names
        }
    }
}
impl DisplayAs for RayShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayShuffleExec[{}] (output_partitioning={:?})",
            self.unique_id,
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
        Ok(Arc::new(RayShuffleExec::new(
            child,
            self.output_partitions,
            self.input_partitions,
            self.unique_id.clone(),
        )))
    }

    /// We will spawn a Ray Task for our child inputs and consume their output stream.
    /// We will have to defer this functionality to python as Ray does not yet have Rust bindings.
    fn execute(
        &self,
        partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // serialize our input plan
        let codec = ShufflerCodec {};
        let proto = datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(
            self.input.clone(),
            &codec,
        )?;
        let bytes = proto.encode_to_vec();

        debug!("RayShuffleExec{} ::execute", self.unique_id);

        // defer execution to the python RayShuffle object which will spawn a Ray Task
        // to execute this partition and send us back a stream of the results
        let unbound_iterable = Python::with_gil(|py| {
            let proto_bytes = PyBytes::new_bound(py, &bytes);

            let module = PyModule::import_bound(py, "datafusion_ray.context")?;
            let ray_shuffler = module.getattr("RayShuffler")?.call0()?;

            let py_obj = ray_shuffler.call_method1(
                "execute_partition",
                (
                    proto_bytes,
                    partition,
                    self.output_partitions,
                    self.input_partitions,
                    self.unique_id.clone(),
                ),
            )?;
            py_obj.iter().map(|i| i.unbind())
        })
        .map_err(|e| internal_datafusion_err!("{e}"))?;

        let sendable_iterator = SendableIterator::new(unbound_iterable);

        let stream = futures::stream::iter(sendable_iterator);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

struct SendableIterator {
    /// our unbound python iterator.  When we are asked to produce
    /// the next item, we'll rebind it to the GIL
    inner: Py<PyIterator>,
}

impl SendableIterator {
    fn new(inner: Py<PyIterator>) -> Self {
        Self { inner }
    }
}

impl Iterator for SendableIterator {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        Python::with_gil(|py| {
            let inner = self.inner.clone_ref(py);
            let mut bound = inner.into_bound(py);
            bound.next().map(|next| {
                next.and_then(|next| RecordBatch::from_pyarrow_bound(&next))
                    .map_err(|e| internal_datafusion_err!("{e}"))
            })
        })
    }
}

struct StreamToIteratorAdapter<S: Stream<Item = T> + Unpin + Send, T: Send> {
    name: String,
    stream: Pin<Box<S>>,
}

impl<S: Stream<Item = T> + Unpin + Send, T: Send> StreamToIteratorAdapter<S, T> {
    fn new(name: String, stream: S) -> Self {
        Self {
            name,
            stream: Pin::new(Box::new(stream)),
        }
    }
}

impl<S: Stream<Item = T> + Unpin + Send, T: Send + std::fmt::Debug> Iterator
    for StreamToIteratorAdapter<S, T>
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        Python::with_gil(|py| {
            wait_for_future(py, async {
                let next = self.stream.next().await;
                match next {
                    Some(n) => {
                        println!("{} got a thing from stream: {:?}", self.name, n);
                        Some(n)
                    }
                    None => None,
                }
            })
        })
    }
}

pub struct ShadowPartitionNumber(pub usize);

#[pyclass]
pub struct PartitionExecutor {
    name: String,
    plan: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
}

#[pymethods]
impl PartitionExecutor {
    #[new]
    pub fn new(name: String, plan_bytes: Vec<u8>, shadow_partition: usize) -> PyResult<Self> {
        let config =
            SessionConfig::new().with_extension(Arc::new(ShadowPartitionNumber(shadow_partition)));
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);

        let proto_plan = datafusion_proto::protobuf::PhysicalPlanNode::try_decode(&plan_bytes)
            .map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "Unable to decode logical node from serialized bytes: {}",
                    e
                ))
            })?;

        let codec = ShufflerCodec {};
        let plan = proto_plan.try_into_physical_plan(&ctx, &ctx.runtime_env(), &codec)?;

        trace!(
            "New ParititonExecutor shadow_partition {} plan:\n{}",
            shadow_partition,
            displayable(plan.as_ref()).indent(true)
        );

        Ok(Self { name, plan, ctx })
    }

    pub fn output_partition(&self, py: Python, partition: usize) -> PyResult<PyObject> {
        let stream_out = self.plan.execute(partition, self.ctx.task_ctx())?;

        let py_out = StreamToIteratorAdapter::new(
            self.name.clone(),
            stream_out.map_err(|e| ArrowError::ExternalError(Box::new(e))),
        );

        let py_out = RecordBatchIterator::new(py_out, self.plan.schema());

        let reader: Box<dyn RecordBatchReader + Send> = Box::new(py_out);
        reader.into_pyarrow(py)
    }
}
