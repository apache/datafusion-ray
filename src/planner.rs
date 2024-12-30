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

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use pyo3::prelude::*;
use std::cell::RefCell;
use std::sync::Arc;

use crate::ray_shuffle::RayShuffleExec;

#[derive(Debug)]
pub struct RayShuffleOptimizerRule {
    py_inner: Arc<PyObject>,
}

impl RayShuffleOptimizerRule {
    pub fn new(py_inner: PyObject) -> Self {
        Self {
            py_inner: Arc::new(py_inner),
        }
    }
}

impl PhysicalOptimizerRule for RayShuffleOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!(
            "optimizing physical plan:\n{}",
            displayable(plan.as_ref()).indent(false)
        );

        let parents = RefCell::new(vec![]);

        let down = |plan: Arc<dyn ExecutionPlan>| {
            let my_parent = parents.borrow().last();

            let mut parents = parents.borrow_mut();
            parents.push(plan.clone());

            if let Some(parent) = my_parent {
                if parent.as_any().downcast_ref::<RayShuffleExec>().is_some() {
                    return Ok(Transformed::no(plan));
                }
            }

            println!("checking plan: {}", displayable(plan.as_ref()).one_line());

            //if plan.as_any().downcast_ref::<RepartitionExec>().is_some() {
            if plan
                .as_any()
                .downcast_ref::<datafusion::datasource::physical_plan::ParquetExec>()
                .is_some()
            {
                //let input = plan.children();
                // TODO: are there ever more than one child?
                //assert_eq!(input.len(), 1);
                //let input = input[0].clone();
                let input = plan;

                let new_plan = Arc::new(RayShuffleExec::new(input, self.py_inner.clone()));
                Ok(Transformed::yes(new_plan as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(plan))
            }
        };

        let up = |plan: Arc<dyn ExecutionPlan>| {
            let mut parents = parents.borrow_mut();
            parents.pop();
            Ok(Transformed::no(plan))
        };

        let out = plan.transform_down_up(down, up)?.data;

        println!(
            "optimized physical plan:\n{}",
            displayable(out.as_ref()).indent(false)
        );
        Ok(out)
    }

    fn name(&self) -> &str {
        "RayShuffleOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
