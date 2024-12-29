// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::planner::{make_execution_graph, PyExecutionGraph};
use crate::shuffle::ShuffleCodec;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::*;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf;
use futures::StreamExt;
use prost::Message;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

type PyResultSet = Vec<PyObject>;

pub(crate) fn execution_plan_from_pyany(
    py_plan: &Bound<PyAny>,
) -> PyResult<Arc<dyn ExecutionPlan>> {
    let py_proto = py_plan.call_method0("to_proto")?;
    let plan_bytes: &[u8] = py_proto.extract()?;
    let plan_node = protobuf::PhysicalPlanNode::try_decode(plan_bytes).map_err(|e| {
        PyRuntimeError::new_err(format!(
            "Unable to decode physical plan protobuf message: {}",
            e
        ))
    })?;

    let codec = ShuffleCodec {};
    let runtime = RuntimeEnv::default();
    let registry = SessionContext::new();
    plan_node
        .try_into_physical_plan(&registry, &runtime, &codec)
        .map_err(|e| e.into())
}

#[pyfunction]
pub fn execute_partition(
    plan_bytes: &Bound<'_, PyBytes>,
    part: usize,
    py: Python,
) -> PyResult<PyResultSet> {
    let plan = deserialize_execution_plan(plan_bytes)?;
    _execute_partition(plan, part)
        .unwrap()
        .into_iter()
        .map(|batch| batch.to_pyarrow(py))
        .collect()
}

pub fn serialize_execution_plan(
    plan: Arc<dyn ExecutionPlan>,
    py: Python,
) -> PyResult<Bound<'_, PyBytes>> {
    let codec = ShuffleCodec {};
    let proto =
        datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(plan.clone(), &codec)?;

    let bytes = proto.encode_to_vec();
    Ok(PyBytes::new_bound(py, &bytes))
}

pub fn deserialize_execution_plan(proto_msg: &Bound<PyBytes>) -> PyResult<Arc<dyn ExecutionPlan>> {
    let bytes: &[u8] = proto_msg.extract()?;
    let proto_plan =
        datafusion_proto::protobuf::PhysicalPlanNode::try_decode(bytes).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "Unable to decode logical node from serialized bytes: {}",
                e
            ))
        })?;

    let ctx = SessionContext::new();
    let codec = ShuffleCodec {};
    let plan = proto_plan
        .try_into_physical_plan(&ctx, &ctx.runtime_env(), &codec)
        .map_err(DataFusionError::from)?;

    Ok(plan)
}

/// Execute a partition of a query plan.
fn _execute_partition(plan: Arc<dyn ExecutionPlan>, part: usize) -> Result<Vec<RecordBatch>> {
    let ctx = Arc::new(TaskContext::new(
        Some("task_id".to_string()),
        "session_id".to_string(),
        SessionConfig::default(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Arc::new(RuntimeEnv::default()),
    ));

    // create a Tokio runtime to run the async code
    let rt = Runtime::new().unwrap();

    let fut: JoinHandle<Result<Vec<RecordBatch>>> = rt.spawn(async move {
        let mut stream = plan.execute(part, ctx)?;
        let mut results = vec![];
        while let Some(result) = stream.next().await {
            results.push(result?);
        }
        Ok(results)
    });

    // block and wait on future
    let results = rt.block_on(fut).unwrap()?;
    Ok(results)
}
