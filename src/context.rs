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

use datafusion::arrow::pyarrow::{FromPyArrow, IntoPyArrow, ToPyArrow};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::displayable;
use datafusion::{
    execution::SessionStateBuilder,
    logical_expr::logical_plan,
    physical_plan::{collect, ExecutionPlan, ExecutionPlanProperties},
    prelude::*,
};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_python::{
    context::PySessionContext, dataframe::PyDataFrame, errors::*, utils::wait_for_future,
};
use prost::Message;
use pyo3::prelude::*;
use std::{sync::Arc, time::Instant};

use crate::ray_stage::RayStageExec;
use crate::{codec::ShufflerCodec, physical::RayShuffleOptimizerRule};

pub struct CoordinatorId(pub String);

#[pyclass]
pub struct RayContext {
    ctx: SessionContext,
}

#[pymethods]
impl RayContext {
    #[new]
    pub fn new() -> PyResult<Self> {
        let rule = RayShuffleOptimizerRule::new();

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(rule))
            .build();

        let ctx = SessionContext::new_with_state(state);

        Ok(Self { ctx })
    }

    pub fn register_parquet(&self, name: String, path: String) -> PyResult<()> {
        futures::executor::block_on(self.ctx.register_parquet(
            &name,
            &path,
            ParquetReadOptions::default(),
        ))?;
        Ok(())
    }

    pub fn sql(&self, py: Python, query: String) -> PyResult<PyDataFrame> {
        let df = wait_for_future(py, self.ctx.sql(&query))?;
        Ok(PyDataFrame::new(df))
    }

    pub fn set(&self, option: String, value: String) -> PyResult<()> {
        let state = self.ctx.state_ref();
        let mut guard = state.write();
        let config = guard.config_mut();
        let options = config.options_mut();
        options.set(&option, &value)?;

        Ok(())
    }

    pub fn set_coordinator_id(&self, id: String) -> PyResult<()> {
        let state = self.ctx.state_ref();
        let mut guard = state.write();
        let config = guard.config_mut();
        config.set_extension(Arc::new(CoordinatorId(id)));
        Ok(())
    }

    fn sql_to_physical_plan_bytes(&self, py: Python, sql: String) -> PyResult<Vec<u8>> {
        let logical_plan = wait_for_future(py, self.ctx.sql(&sql))?.into_optimized_plan()?;

        let plan = wait_for_future(py, self.ctx.state().create_physical_plan(&logical_plan))?;

        let codec = ShufflerCodec {};
        let proto =
            datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
        let bytes = proto.encode_to_vec();
        Ok(bytes)
    }

    fn basic_physical_plan_bytes(&self, py: Python, sql: String) -> PyResult<Vec<u8>> {
        let logical_plan = wait_for_future(py, self.ctx.sql(&sql))?.into_optimized_plan()?;

        let plan = wait_for_future(py, self.ctx.state().create_physical_plan(&logical_plan))?;

        let plan = Arc::new(RayStageExec::new(
            plan.clone(),
            plan.output_partitioning().partition_count(),
            plan.output_partitioning().partition_count(),
            "stage 1".into(),
        ));

        let plan = Arc::new(CoalescePartitionsExec::new(plan.clone()));

        println!("basic plan = {}", displayable(plan.as_ref()).indent(true));

        let codec = ShufflerCodec {};
        let proto =
            datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
        let bytes = proto.encode_to_vec();
        Ok(bytes)
    }

    fn two_step_physical_plan_bytes(&self, py: Python, sql: String) -> PyResult<Vec<u8>> {
        let logical_plan = wait_for_future(py, self.ctx.sql(&sql))?.into_optimized_plan()?;

        let plan = wait_for_future(py, self.ctx.state().create_physical_plan(&logical_plan))?;

        let plan: Arc<dyn ExecutionPlan> = Arc::new(RayStageExec::new(
            plan.clone(),
            plan.output_partitioning().partition_count(),
            plan.output_partitioning().partition_count(),
            "stage 1".into(),
        ));
        let plan = Arc::new(RayStageExec::new(
            plan.clone(),
            plan.output_partitioning().partition_count(),
            plan.output_partitioning().partition_count(),
            "stage 2".into(),
        ));

        let plan = Arc::new(CoalescePartitionsExec::new(plan.clone()));

        println!(
            "two step plan = {}",
            displayable(plan.as_ref()).indent(true)
        );

        let codec = ShufflerCodec {};
        let proto =
            datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
        let bytes = proto.encode_to_vec();
        Ok(bytes)
    }

    pub fn execute_all(&self, sql: String, py: Python) -> PyResult<Vec<PyObject>> {
        //let stream_out = execute_stream(self.plan.clone(), self.ctx.task_ctx())?;

        let df = wait_for_future(py, self.ctx.sql(&sql))?;
        let pplan = wait_for_future(py, df.create_physical_plan())?;
        let now = Instant::now();
        let batches = wait_for_future(py, collect(pplan, self.ctx.task_ctx()))?;
        let elapsed = now.elapsed();
        println!(
            "collecting with my own plan batches took {}ms",
            elapsed.as_millis()
        );

        let logical_plan = wait_for_future(py, self.ctx.sql(&sql))?.into_optimized_plan()?;

        let plan = wait_for_future(py, self.ctx.state().create_physical_plan(&logical_plan))?;

        let codec = ShufflerCodec {};
        let proto =
            datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;
        let bytes = proto.encode_to_vec();

        let proto_plan = datafusion_proto::protobuf::PhysicalPlanNode::try_decode(&bytes)?;

        let plan = proto_plan.try_into_physical_plan(&self.ctx, &self.ctx.runtime_env(), &codec)?;

        let now = Instant::now();
        let batches = wait_for_future(py, collect(plan, self.ctx.task_ctx()))?;
        let elapsed = now.elapsed();
        println!("collecting batches took {}ms", elapsed.as_millis());

        let now = Instant::now();
        let out = batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect();
        let elapsed = now.elapsed();
        println!("batches to py took {}ms", elapsed.as_millis());
        out

        /*let py_out =
            block_on_stream(stream_out.map_err(|e| ArrowError::ExternalError(Box::new(e))));

        let py_out = RecordBatchIterator::new(py_out, self.plan.schema());

        let reader: Box<dyn RecordBatchReader + Send> = Box::new(py_out);
        reader.into_pyarrow(py)
        */
    }
}
