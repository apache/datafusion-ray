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
use crate::shuffle::{RayShuffleReaderExec, ShuffleCodec};
use crate::utils::wait_for_future;
use datafusion::arrow::pyarrow::FromPyArrow;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::options::ReadOptions;
use datafusion::execution::registry::MemoryFunctionRegistry;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::*;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf;
use datafusion_python::physical_plan::PyExecutionPlan;
use futures::StreamExt;
use prost::{DecodeError, Message};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyLong, PyTuple};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

type PyResultSet = Vec<PyObject>;

#[pyclass(name = "Context", module = "datafusion_ray", subclass)]
pub struct PyContext {
    pub(crate) py_ctx: PyObject,
}

#[pymethods]
impl PyContext {
    #[new]
    pub fn new(session_ctx: PyObject) -> Result<Self> {
        Ok(Self {
            py_ctx: session_ctx,
        })
    }

    // pub fn register_csv(
    //     &self,
    //     name: &str,
    //     path: &str,
    //     has_header: bool,
    //     py: Python,
    // ) -> PyResult<()> {
    //     let options = CsvReadOptions::default().has_header(has_header);
    //     wait_for_future(py, self.ctx.register_csv(name, path, options))?;
    //     Ok(())
    // }

    // pub fn register_parquet(&self, name: &str, path: &str, py: Python) -> PyResult<()> {
    //     let options = ParquetReadOptions::default();
    //     wait_for_future(py, self.ctx.register_parquet(name, path, options))?;
    //     Ok(())
    // }

    // pub fn register_datalake_table(
    //     &self,
    //     name: &str,
    //     path: Vec<String>,
    //     py: Python,
    // ) -> PyResult<()> {
    //     // let options = ParquetReadOptions::default();
    //     // let listing_options = options.to_listing_options(&self.ctx.state().config());
    //     // wait_for_future(py, self.ctx.register_listing_table(name, path, listing_options, None, None))?;
    //     // Ok(())
    //     unimplemented!()
    // }

    /// Execute SQL directly against the DataFusion context. Useful for statements
    /// such as "create view" or "drop view"
    pub fn sql(&self, query: &str, py: Python) -> PyResult<()> {
        println!("Executing {}", query);
        // let _df = wait_for_future(py, self.ctx.sql(sql))?;
        let _df = self.run_sql(query, py);
        Ok(())
    }

    fn run_sql(&self, query: &str, py: Python) -> PyResult<Py<PyAny>> {
        let args = PyTuple::new_bound(py, [query]);
        self.py_ctx.call_method1(py, "sql", args)
    }

    /// Plan a distributed SELECT query for executing against the Ray workers
    pub fn plan(&self, sql: &str, py: Python) -> PyResult<PyExecutionGraph> {
        println!("Planning {}", sql);
        // let df = wait_for_future(py, self.ctx.sql(sql))?;
        let py_df = self.run_sql(sql, py)?;
        let py_plan = py_df.call_method0(py, "execution_plan")?;
        let py_proto = py_plan.call_method0(py, "to_proto")?;
        let plan_bytes: &[u8] = py_proto.extract(py)?;
        let plan_node = protobuf::PhysicalPlanNode::decode(plan_bytes).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "Unable to decode physical plan protobuf message: {}",
                e
            ))
        })?;

        let codec = DefaultPhysicalExtensionCodec {};
        let runtime = RuntimeEnv::default();
        let registry = SessionContext::new();
        let plan = plan_node.try_into_physical_plan(&registry, &runtime, &codec)?;

        let graph = make_execution_graph(plan.clone())?;

        // debug logging
        let mut stages = graph.query_stages.values().collect::<Vec<_>>();
        stages.sort_by_key(|s| s.id);
        for stage in stages {
            println!(
                "Query stage #{}:\n{}",
                stage.id,
                displayable(stage.plan.as_ref()).indent(false)
            );
        }

        Ok(PyExecutionGraph::new(graph))
    }

    /// Execute a partition of a query plan. This will typically be executing a shuffle write and write the results to disk
    pub fn execute_partition(
        &self,
        plan: PyExecutionPlan,
        part: usize,
        inputs: PyObject,
        py: Python,
    ) -> PyResultSet {
        execute_partition(plan, part, inputs, py)
    }
}

#[pyfunction]
pub fn execute_partition(
    plan: PyExecutionPlan,
    part: usize,
    inputs: PyObject,
    py: Python,
) -> PyResultSet {
    _execute_partition(plan, part, inputs)
        .unwrap()
        .into_iter()
        .map(|batch| batch.to_pyarrow(py).unwrap()) // TODO(@lsf) handle error
        .collect()
}

// TODO(@lsf) change this to use pickle
#[pyfunction]
pub fn serialize_execution_plan(plan: PyExecutionPlan) -> PyResult<Vec<u8>> {
    let codec = ShuffleCodec {};
    Ok(physical_plan_to_bytes_with_extension_codec(plan.plan, &codec)?.to_vec())
}

#[pyfunction]
pub fn deserialize_execution_plan(bytes: Vec<u8>) -> PyResult<PyExecutionPlan> {
    let ctx = SessionContext::new();
    let codec = ShuffleCodec {};
    Ok(PyExecutionPlan::new(
        physical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &codec)?,
    ))
}

/// Iterate down an ExecutionPlan and set the input objects for RayShuffleReaderExec.
fn _set_inputs_for_ray_shuffle_reader(
    plan: Arc<dyn ExecutionPlan>,
    input_partitions: &Bound<'_, PyList>,
) -> Result<()> {
    if let Some(reader_exec) = plan.as_any().downcast_ref::<RayShuffleReaderExec>() {
        let exec_stage_id = reader_exec.stage_id;
        // iterate over inputs, wrap in PyBytes and set as input objects
        for item in input_partitions.iter() {
            let pytuple = item
                .downcast::<PyTuple>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
            let stage_id = pytuple
                .get_item(0)
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .downcast::<PyLong>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .extract::<usize>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
            if stage_id != exec_stage_id {
                continue;
            }
            let part = pytuple
                .get_item(1)
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .downcast::<PyLong>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                .extract::<usize>()
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
            let batch = RecordBatch::from_pyarrow_bound(
                &pytuple
                    .get_item(2)
                    .map_err(|e| DataFusionError::Execution(format!("{}", e)))?,
            )
            .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
            reader_exec.add_input_partition(part, batch)?;
        }
    } else {
        for child in plan.children() {
            _set_inputs_for_ray_shuffle_reader(child.to_owned(), input_partitions)?;
        }
    }
    Ok(())
}

/// Execute a partition of a query plan. This will typically be executing a shuffle write and
/// write the results to disk, except for the final query stage, which will return the data.
/// inputs is a list of tuples of (stage_id, partition_id, bytes) for each input partition.
fn _execute_partition(
    plan: PyExecutionPlan,
    part: usize,
    inputs: PyObject,
) -> Result<Vec<RecordBatch>> {
    let ctx = Arc::new(TaskContext::new(
        Some("task_id".to_string()),
        "session_id".to_string(),
        SessionConfig::default(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Arc::new(RuntimeEnv::default()),
    ));
    Python::with_gil(|py| {
        let input_partitions = inputs
            .bind(py)
            .downcast::<PyList>()
            .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
        _set_inputs_for_ray_shuffle_reader(plan.plan.clone(), input_partitions)
    })?;

    // create a Tokio runtime to run the async code
    let rt = Runtime::new().unwrap();

    let fut: JoinHandle<Result<Vec<RecordBatch>>> = rt.spawn(async move {
        let mut stream = plan.plan.execute(part, ctx)?;
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
