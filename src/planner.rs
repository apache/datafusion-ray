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
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

use crate::ray_shuffle::RayShuffleExec;

#[derive(Debug)]
struct RayShuffleOptimizerRule {}

impl PhysicalOptimizerRule for RayShuffleOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let down = |plan: Arc<dyn ExecutionPlan>| {
            if plan.as_any().downcast_ref::<RepartitionExec>().is_some() {
                let new_plan = Arc::new(RayShuffleExec::new(plan));
                Ok(Transformed::yes(new_plan as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(plan))
            }
        };

        Ok(plan.transform_down(down)?.data)
    }

    fn name(&self) -> &str {
        "RayShuffleOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
