use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::{root_as_message, MetadataVersion};
use arrow::pyarrow::*;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::FlightData;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::codec::RayCodec;
use crate::protobuf::StreamMeta;
use prost::{bytes::Bytes, Message};

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

pub fn extract_stream_meta(flight_data: &FlightData) -> anyhow::Result<(usize, usize, f64)> {
    let descriptor = flight_data
        .flight_descriptor
        .as_ref()
        .ok_or(internal_datafusion_err!("No flight descriptor"))?;

    let cmd = descriptor.cmd.clone(); // TODO: Can this be avoided?

    let stream_meta = StreamMeta::decode(cmd)?;
    Ok((
        stream_meta.stage_num as usize,
        stream_meta.partition_num as usize,
        stream_meta.fraction as f64,
    ))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };

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
}
