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

use arrow::util::pretty::pretty_format_batches;
use datafusion::common::internal_datafusion_err;
use datafusion::physical_plan::{collect, displayable};
use datafusion::{execution::SessionStateBuilder, physical_plan::ExecutionPlan, prelude::*};
use datafusion_python::dataframe::PyDataFrame;
use datafusion_python::{errors::*, utils::wait_for_future};
use object_store::aws::AmazonS3Builder;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::dataframe::{PyRecordBatchStream, RayDataFrame};
use crate::physical::RayShuffleOptimizerRule;
use crate::util::{bytes_to_physical_plan, physical_plan_to_bytes, ResultExt};
use url::Url;

pub struct CoordinatorId(pub String);

#[pyclass]
pub struct RayContext {
    ctx: SessionContext,
    ctx_test: SessionContext,
    bucket: Option<String>,
}

#[pymethods]
impl RayContext {
    #[new]
    pub fn new(bucket: Option<String>) -> PyResult<Self> {
        let rule = RayShuffleOptimizerRule::new();

        let config = SessionConfig::default().with_information_schema(true);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(rule))
            .with_config(config)
            .build();

        let ctx = SessionContext::new_with_state(state);
        let ctx_test = SessionContext::new();

        Ok(Self {
            ctx,
            ctx_test,
            bucket,
        })
    }

    pub fn register_s3(&self, bucket_name: String) -> PyResult<()> {
        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(&bucket_name)
            .build()
            .to_py_err()?;

        let path = format!("s3://{bucket_name}");
        let s3_url = Url::parse(&path).to_py_err()?;
        let arc_s3 = Arc::new(s3);
        self.ctx.register_object_store(&s3_url, arc_s3.clone());
        Ok(())
    }

    pub fn register_parquet(&self, py: Python, name: String, path: String) -> PyResult<()> {
        let mut options = ParquetReadOptions::default();
        options.file_extension = ".parquet";

        wait_for_future(py, self.ctx.register_parquet(&name, &path, options.clone()))?;
        wait_for_future(py, self.ctx_test.register_parquet(&name, &path, options))?;
        Ok(())
    }

    pub fn sql(&self, py: Python, query: String, coordinator_id: String) -> PyResult<RayDataFrame> {
        let physical_plan = wait_for_future(py, self.sql_to_physical_plan(query))?;

        Ok(RayDataFrame::new(
            physical_plan,
            coordinator_id,
            self.bucket.clone(),
        ))
    }

    pub fn test(&self, py: Python, sql: String) -> PyResult<()> {
        let ctx = self.ctx_test.clone();
        let logical_plan = wait_for_future(py, ctx.sql(&sql))?.into_optimized_plan()?;

        let og_plan = wait_for_future(py, ctx.state().create_physical_plan(&logical_plan))?;
        println!(
            "original plan :\n{}",
            displayable(og_plan.as_ref()).indent(true)
        );

        let batches = wait_for_future(py, collect(og_plan.clone(), ctx.task_ctx()))?;
        println!(
            "test before round trip batch:{}",
            pretty_format_batches(&batches).unwrap()
        );

        let bytes = physical_plan_to_bytes(og_plan)?;
        let plan = bytes_to_physical_plan(&ctx, &bytes)?;

        println!(
            "round trip plan :\n{}",
            displayable(plan.as_ref()).indent(true)
        );

        let batches = wait_for_future(py, collect(plan, ctx.task_ctx()))?;
        println!(
            "test after round trip batch:{}",
            pretty_format_batches(&batches).unwrap()
        );

        //println!("are they equal {}", );
        Ok(())
    }

    pub fn set(&self, option: String, value: String) -> PyResult<()> {
        let state = self.ctx.state_ref();
        let mut guard = state.write();
        let config = guard.config_mut();
        let options = config.options_mut();
        options.set(&option, &value)?;

        let state = self.ctx_test.state_ref();
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
