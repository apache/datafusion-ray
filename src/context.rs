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

use arrow::datatypes::SchemaRef;
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::internal_datafusion_err;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::physical_plan::{collect, displayable};
use datafusion::{execution::SessionStateBuilder, physical_plan::ExecutionPlan, prelude::*};
use datafusion_python::context::convert_table_partition_cols;
use datafusion_python::dataframe::PyDataFrame;
use datafusion_python::expr::sort_expr::PySortExpr;
use datafusion_python::{errors::*, utils::wait_for_future};
use object_store::aws::AmazonS3Builder;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::dataframe::{PyRecordBatchStream, RayDataFrame};
use crate::physical::RayStageOptimizerRule;
use crate::util::{bytes_to_physical_plan, physical_plan_to_bytes, ResultExt};
use url::Url;

#[pyclass]
pub struct RayContext {
    ctx: SessionContext,
    bucket: Option<String>,
}

#[pymethods]
impl RayContext {
    #[new]
    pub fn new(bucket: Option<String>) -> PyResult<Self> {
        let rule = RayStageOptimizerRule::new();

        let config = SessionConfig::default().with_information_schema(true);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(rule))
            .with_config(config)
            .build();

        let ctx = SessionContext::new_with_state(state);

        Ok(Self { ctx, bucket })
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

        wait_for_future(
            py,
            self.ctx
                .register_listing_table(name, path, options, None, None),
        )
        .to_py_err()
    }

    pub fn sql(&self, py: Python, query: String) -> PyResult<RayDataFrame> {
        let df = wait_for_future(py, self.ctx.sql(&query))?;

        Ok(RayDataFrame::new(df, self.bucket.clone()))
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
