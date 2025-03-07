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

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_python::utils::wait_for_future;
use log::debug;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::dataframe::DFRayDataFrame;
use crate::physical::RayStageOptimizerRule;
use crate::util::{maybe_register_object_store, ResultExt};
use url::Url;

/// Internal Session Context object for the python class DFRayContext
#[pyclass]
pub struct DFRayContext {
    /// our datafusion context
    ctx: SessionContext,
}

#[pymethods]
impl DFRayContext {
    #[new]
    pub fn new() -> PyResult<Self> {
        let rule = RayStageOptimizerRule::new();

        let config = SessionConfig::default().with_information_schema(true);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(rule))
            .with_config(config)
            .build();

        let ctx = SessionContext::new_with_state(state);

        Ok(Self { ctx })
    }

    pub fn register_parquet(&self, py: Python, name: String, path: String) -> PyResult<()> {
        let options = ParquetReadOptions::default();

        let url = ListingTableUrl::parse(&path).to_py_err()?;

        maybe_register_object_store(&self.ctx, url.as_ref()).to_py_err()?;
        debug!("register_parquet: registering table {} at {}", name, path);

        wait_for_future(py, self.ctx.register_parquet(&name, &path, options.clone()))?;
        Ok(())
    }

    pub fn register_csv(&self, py: Python, name: String, path: String) -> PyResult<()> {
        let options = CsvReadOptions::default();

        let url = ListingTableUrl::parse(&path).to_py_err()?;

        maybe_register_object_store(&self.ctx, url.as_ref()).to_py_err()?;
        debug!("register_csv: registering table {} at {}", name, path);

        wait_for_future(py, self.ctx.register_csv(&name, &path, options.clone()))?;
        Ok(())
    }

    #[pyo3(signature = (name, path, file_extension=".parquet"))]
    pub fn register_listing_table(
        &mut self,
        py: Python,
        name: &str,
        path: &str,
        file_extension: &str,
    ) -> PyResult<()> {
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::new())).with_file_extension(file_extension);

        let path = format!("{path}/");
        let url = ListingTableUrl::parse(&path).to_py_err()?;

        maybe_register_object_store(&self.ctx, url.as_ref()).to_py_err()?;

        debug!(
            "register_listing_table: registering table {} at {}",
            name, path
        );
        wait_for_future(
            py,
            self.ctx
                .register_listing_table(name, path, options, None, None),
        )
        .to_py_err()
    }

    pub fn sql(&self, py: Python, query: String) -> PyResult<DFRayDataFrame> {
        let df = wait_for_future(py, self.ctx.sql(&query))?;

        Ok(DFRayDataFrame::new(df))
    }

    pub fn set(&self, option: String, value: String) -> PyResult<()> {
        let state = self.ctx.state_ref();
        let mut guard = state.write();
        let config = guard.config_mut();
        let options = config.options_mut();
        options.set(&option, &value)?;

        Ok(())
    }

    pub fn get_target_partitions(&self) -> usize {
        let state = self.ctx.state_ref();
        let guard = state.read();
        let config = guard.config();
        let options = config.options();
        options.execution.target_partitions
    }
}
