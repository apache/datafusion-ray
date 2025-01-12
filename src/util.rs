use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::MetadataVersion;
use arrow::pyarrow::*;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::codec::RayCodec;
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
