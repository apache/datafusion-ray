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

extern crate core;

use pyo3::prelude::*;

mod proto;
pub use proto::generated::protobuf;

pub mod codec;
pub mod context;
pub mod dataframe;
pub mod exchange;
pub mod flight;
pub mod isolator;
pub mod physical;
pub mod pystage;
pub mod ray_stage;
pub mod ray_stage_reader;
mod util;

/// A Python module implemented in Rust.
#[pymodule]
fn _datafusion_ray_internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // register classes that can be created directly from Python code
    m.add_class::<context::RayContext>()?;
    m.add_class::<dataframe::RayDataFrame>()?;
    m.add_class::<dataframe::PyDataFrameStage>()?;
    m.add_class::<pystage::PyStage>()?;
    m.add_class::<exchange::PyExchange>()?;
    m.add_function(wrap_pyfunction!(util::batch_to_ipc, m)?)?;
    m.add_function(wrap_pyfunction!(util::ipc_to_batch, m)?)?;
    Ok(())
}
