use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
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
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_python::utils::wait_for_future;
use futures::{Stream, StreamExt};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use rust_decimal::prelude::*;
use tokio::time::timeout;
use tonic::transport::Channel;

use crate::codec::RayCodec;
use crate::protobuf::StreamMeta;
use prost::Message;

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

// we need these two functions to go back and forth between IPC representations
// from rust to rust to avoid using the C++ implementation from pyarrow as it
// will generate unaligned data causing us errors

#[pyfunction]
pub fn batch_to_ipc(py: Python, batch: PyArrowType<RecordBatch>) -> PyResult<Py<PyBytes>> {
    let batch = batch.0;

    let bytes = batch_to_ipc_helper(&batch).to_py_err()?;

    //TODO:  unsure about this next line.  Compiler is happy but is this correct?
    Ok(PyBytes::new_bound(py, &bytes).unbind())
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

pub fn extract_stream_meta(flight_data: &FlightData) -> anyhow::Result<(usize, usize, Decimal)> {
    let descriptor = flight_data
        .flight_descriptor
        .as_ref()
        .ok_or(internal_datafusion_err!("No flight descriptor"))?;

    let cmd = descriptor.cmd.clone(); // TODO: Can this be avoided?

    let stream_meta = StreamMeta::decode(cmd)?;
    Ok((
        stream_meta.stage_id as usize,
        stream_meta.partition as usize,
        Decimal::from_str(&stream_meta.fraction)?,
    ))
}

pub fn extract_ticket(ticket: Ticket) -> anyhow::Result<(usize, usize, Decimal)> {
    let data = ticket.ticket;

    let stream_meta = StreamMeta::decode(data)?;
    Ok((
        stream_meta.stage_id as usize,
        stream_meta.partition as usize,
        Decimal::from_str(&stream_meta.fraction)?,
    ))
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
        .map(|b| RecordBatch::from_pyarrow_bound(&b).unwrap())
        .collect();

    pretty::pretty_format_batches(&b)
        .to_py_err()
        .map(|d| d.to_string())
        .to_py_err()
}

pub fn make_client(py: Python, exchange_addr: &str) -> PyResult<FlightClient> {
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

    let flight_client = FlightClient::new(channel);
    Ok(flight_client)
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
