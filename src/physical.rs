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
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use std::sync::Arc;

use crate::ray_stage::RayStageExec;

/// This optimizer rule walks up the physical plan tree
/// and inserts RayStageExec nodes where appropriate to denote where we will split
/// the plan into stages.
///
/// The RayStageExec nodes are merely markers to inform where to break the plan up.
///
/// Later, the plan will be examined again to actually split it up.
/// These RayStageExecs serve as markers where we know to break it up on a network
/// boundary and we can insert readers and writers as appropriate.
#[derive(Debug)]
pub struct RayStageOptimizerRule {}

impl Default for RayStageOptimizerRule {
    fn default() -> Self {
        Self::new()
    }
}

impl RayStageOptimizerRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for RayStageOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!(
            "optimizing physical plan:\n{}",
            displayable(plan.as_ref()).indent(false)
        );

        let mut stage_counter = 0;

        let up = |plan: Arc<dyn ExecutionPlan>| {
            if plan.as_any().downcast_ref::<RepartitionExec>().is_some()
                || plan.as_any().downcast_ref::<SortExec>().is_some()
                || plan.as_any().downcast_ref::<NestedLoopJoinExec>().is_some()
            {
                let stage = Arc::new(RayStageExec::new(plan, stage_counter));
                stage_counter += 1;
                Ok(Transformed::yes(stage as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(plan))
            }
        };

        let plan = plan.transform_up(up)?.data;
        let final_plan = Arc::new(RayStageExec::new(plan, stage_counter));

        println!(
            "optimized physical plan:\n{}",
            displayable(final_plan.as_ref()).indent(true)
        );
        Ok(final_plan)
    }

    fn name(&self) -> &str {
        "RayStageOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
