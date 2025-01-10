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
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties};
use std::cell::RefCell;
use std::sync::Arc;
use uuid::Uuid;

use crate::ray_stage::RayStageExec;

#[derive(Debug)]
pub struct RayShuffleOptimizerRule {}

impl Default for RayShuffleOptimizerRule {
    fn default() -> Self {
        Self::new()
    }
}

impl RayShuffleOptimizerRule {
    pub fn new() -> Self {
        Self {}
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

        let parents: RefCell<Vec<Arc<dyn ExecutionPlan>>> = RefCell::new(vec![]);

        let down = |plan: Arc<dyn ExecutionPlan>| {
            let mut my_parent = None;
            {
                let parents = parents.borrow();
                if !parents.is_empty() {
                    //TODO: should this be parents.last()?
                    let par = &parents[parents.len() - 1];
                    my_parent = Some(par.clone());
                }
            };
            let mut parents = parents.borrow_mut();
            parents.push(plan.clone());

            println!(
                "checking plan: {} parent:{} ",
                displayable(plan.as_ref()).one_line(),
                my_parent
                    .as_ref()
                    .map(|p| displayable(p.as_ref()).one_line().to_string())
                    .unwrap_or("None".to_string())
            );

            if let Some(ref parent) = my_parent {
                if parent.as_any().downcast_ref::<RayStageExec>().is_some() {
                    return Ok(Transformed::no(plan));
                }
            }

            //if let Some(true) =
            //    my_parent.map(|p| p.as_any().downcast_ref::<RepartitionExec>().is_some())
            if plan.as_any().downcast_ref::<RepartitionExec>().is_some() {
                let children = plan.children();
                // TODO: generalize
                assert_eq!(children.len(), 1);
                let child = children[0].clone();

                // how many partitions there are before the repartition
                let input_partitions = child.output_partitioning().partition_count();

                // how many after the repartition
                let output_partitions = plan.output_partitioning().partition_count();

                //let repartition =
                //    plan.with_new_children(vec![Arc::new(PartitionIsolatorExec::new(child))])?;

                let new_plan = Arc::new(RayStageExec::new(
                    plan,
                    output_partitions,
                    input_partitions,
                    Uuid::new_v4().to_string()[..8].to_string(), // TODO: use a short
                                                                 // unique name vs this hack
                ));

                parents.push(new_plan.clone());
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
