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

use datafusion::{execution::SessionStateBuilder, physical_plan::ExecutionPlan, prelude::*};
use datafusion_python::{errors::*, utils::wait_for_future};
use pyo3::prelude::*;
use std::sync::Arc;

use crate::dataframe::RayDataFrame;
use crate::physical::RayShuffleOptimizerRule;

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

    pub fn sql(&self, py: Python, query: String, coordinator_id: String) -> PyResult<RayDataFrame> {
        let physical_plan = wait_for_future(py, self.sql_to_physical_plan(query))?;

        Ok(RayDataFrame::new(physical_plan, coordinator_id))
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
}
impl RayContext {
    async fn sql_to_physical_plan(&self, sql: String) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.ctx.sql(&sql).await?.into_optimized_plan()?;

        let plan = self.ctx.state().create_physical_plan(&logical_plan).await?;

        Ok(plan)
    }
}
