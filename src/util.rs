use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::{root_as_message, MetadataVersion};
use arrow::pyarrow::*;
use arrow::util::pretty;
use arrow_flight::{FlightClient, FlightData, Ticket};
use async_stream::stream;
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, SessionStateBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use tonic::transport::Channel;

use crate::codec::RayCodec;
use crate::protobuf::FlightTicketData;
use crate::ray_stage_reader::RayStageReaderExec;
use crate::stage_service::ServiceClients;
use prost::Message;
use tokio::macros::support::thread_rng_n;

pub(crate) trait ResultExt<T> {
    fn to_py_err(self) -> PyResult<T>;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: std::fmt::Debug,
{
    fn to_py_err(self) -> PyResult<T> {
        match self {
            Ok(x) => Ok(x),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "{:?}",
                e
            ))),
        }
    }
}

/// we need these two functions to go back and forth between IPC representations
/// from rust to rust to avoid using the C++ implementation from pyarrow as it
/// will generate unaligned data causing us errors
///
/// not used in current arrow flight implementation, but leaving these here
#[pyfunction]
pub fn batch_to_ipc(py: Python, batch: PyArrowType<RecordBatch>) -> PyResult<Py<PyBytes>> {
    let batch = batch.0;

    let bytes = batch_to_ipc_helper(&batch).to_py_err()?;

    //TODO:  unsure about this next line.  Compiler is happy but is this correct?
    Ok(PyBytes::new(py, &bytes).unbind())
}

#[pyfunction]
pub fn ipc_to_batch(bytes: &[u8], py: Python) -> PyResult<PyObject> {
    let batch = ipc_to_batch_helper(bytes).to_py_err()?;
    batch.to_pyarrow(py)
}

fn batch_to_ipc_helper(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let schema = batch.schema();
    let buffer: Vec<u8> = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)
        .map_err(|e| internal_datafusion_err!("Cannot create ipcwriteoptions {e}"))?;

    let mut stream_writer = StreamWriter::try_new_with_options(buffer, &schema, options)?;
    stream_writer.write(batch)?;
    stream_writer.into_inner()
}

fn ipc_to_batch_helper(bytes: &[u8]) -> Result<RecordBatch, ArrowError> {
    let mut stream_reader = StreamReader::try_new_buffered(Cursor::new(bytes), None)?;

    match stream_reader.next() {
        Some(Ok(batch_res)) => Ok(batch_res),
        Some(Err(e)) => Err(e),
        None => Err(ArrowError::IpcError("Expected a valid batch".into())),
    }
}

pub fn physical_plan_to_bytes(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>, DataFusionError> {
    let codec = RayCodec {};
    let proto = datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
    let bytes = proto.encode_to_vec();
    Ok(bytes)
}

pub fn bytes_to_physical_plan(
    ctx: &SessionContext,
    plan_bytes: &[u8],
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let proto_plan = datafusion_proto::protobuf::PhysicalPlanNode::try_decode(plan_bytes)?;

    let codec = RayCodec {};
    let plan = proto_plan.try_into_physical_plan(ctx, ctx.runtime_env().as_ref(), &codec)?;
    Ok(plan)
}

pub fn flight_data_to_schema(flight_data: &FlightData) -> anyhow::Result<SchemaRef> {
    let message = root_as_message(&flight_data.data_header[..])
        .map_err(|_| ArrowError::CastError("Cannot get root as message".to_string()))?;

    let ipc_schema: arrow::ipc::Schema = message
        .header_as_schema()
        .ok_or_else(|| ArrowError::CastError("Cannot get header as Schema".to_string()))?;
    let schema = fb_to_schema(ipc_schema);
    let schema = Arc::new(schema);
    Ok(schema)
}

pub fn extract_ticket(ticket: Ticket) -> anyhow::Result<usize> {
    let data = ticket.ticket;

    let tic = FlightTicketData::decode(data)?;
    Ok(tic.partition as usize)
}

/// produce a new SendableRecordBatchStream that will respect the rows
/// limit in the batches that it produces.  
///
/// It does this in a naive way, but it does honor the limit.  It will
///
/// For example, if the stream produces batches with length 8,
/// and the max row limit is 5, then this new stream will yield
/// batches with length 5, then 3, then 5, then 3 etc.  Simply
/// slicing on the max rows
pub fn max_rows_stream(
    mut in_stream: SendableRecordBatchStream,
    max_rows: usize,
) -> SendableRecordBatchStream
where
{
    let schema = in_stream.schema();
    let fixed_stream = stream! {
        while let Some(batch_res) = in_stream.next().await {
            match batch_res {
                Ok(batch) => {
                    if batch.num_rows() > max_rows {
                        let mut rows_remaining = batch.num_rows();
                        let mut offset = 0;
                        while rows_remaining > max_rows {
                            let s = batch.slice(offset, max_rows);

                            offset += max_rows;
                            rows_remaining -= max_rows;
                            yield Ok(s);
                        }
                        // yield remainder of the batch
                        yield Ok(batch.slice(offset, rows_remaining));
                    } else {
                        yield Ok(batch);
                    }
                },
                Err(e) => yield Err(e)
            }
        }
    };
    let adapter = RecordBatchStreamAdapter::new(schema, fixed_stream);

    Box::pin(adapter)
}

#[pyfunction]
pub fn prettify(batches: Bound<'_, PyList>) -> PyResult<String> {
    let b: Vec<RecordBatch> = batches
        .iter()
        .map(|b| RecordBatch::from_pyarrow_bound(&b))
        .collect::<Result<Vec<_>, _>>()
        .to_py_err()?;

    pretty::pretty_format_batches(&b)
        .to_py_err()
        .map(|d| d.to_string())
        .to_py_err()
}

pub async fn make_client(exchange_addr: &str) -> Result<FlightClient, DataFusionError> {
    let url = format!("http://{exchange_addr}");

    let chan = Channel::from_shared(url.clone())
        .map_err(|e| internal_datafusion_err!("Cannot create channel from url {url}: {e}"))?;
    let channel = chan
        .connect()
        .await
        .map_err(|e| internal_datafusion_err!("Cannot connect to channel {e}"))?;
    let flight_client = FlightClient::new(channel);
    Ok(flight_client)
}

pub fn input_stage_ids(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<usize>, DataFusionError> {
    let mut result = vec![];
    plan.clone()
        .transform_down(|node: Arc<dyn ExecutionPlan>| {
            if let Some(reader) = node.as_any().downcast_ref::<RayStageReaderExec>() {
                result.push(reader.stage_id);
            }
            Ok(Transformed::no(node))
        })?;
    Ok(result)
}

pub async fn report_on_lag<F, T>(name: &str, fut: F) -> T
where
    F: Future<Output = T>,
{
    let name = name.to_owned();
    let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
    let expire = Duration::from_secs(2);

    let report = async move {
        tokio::time::sleep(expire).await;
        while rx.try_recv().is_err() {
            println!("{name} waiting to complete");
            tokio::time::sleep(expire).await;
        }
    };
    tokio::spawn(report);

    let out = fut.await;
    tx.send(()).unwrap();
    out
}

/// A utility wrapper for a stream that will print a message if it has been over
/// 2 seconds since receiving data.  Useful for debugging which streams are stuck
pub fn lag_reporting_stream<S, T>(name: &str, in_stream: S) -> impl Stream<Item = T> + Send
where
    S: Stream<Item = T> + Send,
    T: Send,
{
    let mut stream = Box::pin(in_stream);
    let name = name.to_owned();

    let out_stream = async_stream::stream! {
        while let Some(item) = report_on_lag(&name, stream.next()).await {
            yield item;
        };
    };

    Box::pin(out_stream)
}

/// ParquetExecs do not correctly preserve their options when serialized to substrait.
/// So we fix it here.
///
/// Walk the plan tree and update any ParquetExec nodes to set the options we need.
/// We'll use this method until we are using a DataFusion version which includes thf
/// fix https://github.com/apache/datafusion/pull/14465
pub fn fix_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    Ok(plan
        .transform_up(|node| {
            if let Some(parquet) = node.as_any().downcast_ref::<ParquetExec>() {
                let new_parquet_node = parquet.clone().with_pushdown_filters(true);
                Ok(Transformed::yes(Arc::new(new_parquet_node)))
            } else {
                Ok(Transformed::no(node))
            }
        })?
        .data)
}

pub async fn collect_from_stage(
    stage_id: usize,
    partition: usize,
    stage_addr: &str,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let mut client_map = HashMap::new();

    let client = make_client(stage_addr).await?;

    client_map.insert((stage_id, partition), Mutex::new(vec![client]));
    let config = SessionConfig::new().with_extension(Arc::new(ServiceClients(client_map)));

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .build();
    let ctx = SessionContext::new_with_state(state);

    plan.execute(partition, ctx.task_ctx())
}

/// Copied from datafusion_physical_plan::union as its useful and not public
pub struct CombinedRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
    /// Stream entries
    entries: Vec<SendableRecordBatchStream>,
}

impl CombinedRecordBatchStream {
    /// Create an CombinedRecordBatchStream
    pub fn new(schema: SchemaRef, entries: Vec<SendableRecordBatchStream>) -> Self {
        Self { schema, entries }
    }
}

impl RecordBatchStream for CombinedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for CombinedRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;

        let start = thread_rng_n(self.entries.len() as u32) as usize;
        let mut idx = start;

        for _ in 0..self.entries.len() {
            let stream = self.entries.get_mut(idx).unwrap();

            match Pin::new(stream).poll_next(cx) {
                Ready(Some(val)) => return Ready(Some(val)),
                Ready(None) => {
                    // Remove the entry
                    self.entries.swap_remove(idx);

                    // Check if this was the last entry, if so the cursor needs
                    // to wrap
                    if idx == self.entries.len() {
                        idx = 0;
                    } else if idx < start && start <= self.entries.len() {
                        // The stream being swapped into the current index has
                        // already been polled, so skip it.
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
                Pending => {
                    idx = idx.wrapping_add(1) % self.entries.len();
                }
            }
        }

        // If the map is empty, then the stream is complete.
        if self.entries.is_empty() {
            Ready(None)
        } else {
            Pending
        }
    }
}

pub fn display_plan_with_partition_counts(plan: &Arc<dyn ExecutionPlan>) -> impl Display {
    let mut output = String::with_capacity(1000);

    print_node(plan, 0, &mut output);
    output
}

fn print_node(plan: &Arc<dyn ExecutionPlan>, indent: usize, output: &mut String) {
    let extra = if let Some(parquet) = plan.as_any().downcast_ref::<ParquetExec>() {
        &format!(
            " [pushdown filters: {}]",
            parquet.table_parquet_options().global.pushdown_filters
        )
    } else {
        ""
    };
    output.push_str(&format!(
        "[ output_partitions: {}]{:>indent$}{}{}",
        plan.output_partitioning().partition_count(),
        "",
        displayable(plan.as_ref()).one_line(),
        extra,
        indent = indent
    ));

    for child in plan.children() {
        print_node(child, indent + 2, output);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };
    use futures::stream;

    use super::*;

    #[test]
    fn test_ipc_roundtrip() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let bytes = batch_to_ipc_helper(&batch).unwrap();
        let batch2 = ipc_to_batch_helper(&bytes).unwrap();
        assert_eq!(batch, batch2);
    }

    #[tokio::test]
    async fn test_max_rows_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap();

        // 24 total rows
        let batches = (0..3).map(|_| Ok(batch.clone())).collect::<Vec<_>>();

        let in_stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream::iter(batches)));

        let out_stream = max_rows_stream(in_stream, 3);
        let batches: Vec<_> = out_stream.collect().await;

        println!("got {} batches", batches.len());
        for batch in batches.iter() {
            println!("batch length: {}", batch.as_ref().unwrap().num_rows());
        }

        assert_eq!(batches.len(), 9);
        assert_eq!(batches[0].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[1].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[2].as_ref().unwrap().num_rows(), 2);
        assert_eq!(batches[3].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[4].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[5].as_ref().unwrap().num_rows(), 2);
        assert_eq!(batches[6].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[7].as_ref().unwrap().num_rows(), 3);
        assert_eq!(batches[8].as_ref().unwrap().num_rows(), 2);
    }
}
