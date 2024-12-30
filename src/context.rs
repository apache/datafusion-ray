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

use datafusion::{execution::SessionStateBuilder, prelude::*};
use datafusion_python::context::PySessionContext;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::planner::RayShuffleOptimizerRule;

#[pyfunction]
pub fn make_ray_context(ray_shuffler: PyObject) -> PyResult<PySessionContext> {
    let rule = RayShuffleOptimizerRule::new(ray_shuffler);

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(rule))
        .build();

    let ctx = SessionContext::new_with_state(state);

    Ok(PySessionContext { ctx })
}
