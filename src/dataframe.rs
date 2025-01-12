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

use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_python::physical_plan::PyExecutionPlan;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::pystage::PyStage;
use crate::ray_stage::RayStageExec;
use crate::ray_stage_reader::RayStageReaderExec;

pub struct CoordinatorId(pub String);

#[pyclass]
pub struct RayDataFrame {
    physical_plan: Arc<dyn ExecutionPlan>,
    #[pyo3(get)]
    coordinator_id: String,
}

impl RayDataFrame {
    pub fn new(physical_plan: Arc<dyn ExecutionPlan>, coordinator_id: String) -> Self {
        Self {
            physical_plan,
            coordinator_id,
        }
    }
}

#[pymethods]
impl RayDataFrame {
    #[pyo3(signature = (batch_size=8192))]
    fn stages(&self, batch_size: usize) -> PyResult<Vec<PyStage>> {
        let mut stages = vec![];

        // TODO: This can be done more efficiently, likely in one pass but I'm
        // struggling to get the TreeNodeRecursion return values to make it do
        // what I want. So, two steps for now

        // Step 2: we walk down this stage and replace stages earlier in the tree with
        // RayStageReaderExecs
        let down = |plan: Arc<dyn ExecutionPlan>| {
            //println!("examining plan: {}", displayable(plan.as_ref()).one_line());

            if let Some(stage_exec) = plan.as_any().downcast_ref::<RayStageExec>() {
                let input = plan.children();
                assert!(input.len() == 1, "RayStageExec must have exactly one child");
                let input = input[0];

                let replacement = Arc::new(RayStageReaderExec::try_new_from_input(
                    input.clone(),
                    stage_exec.stage_id.clone(),
                    self.coordinator_id.clone(),
                )?) as Arc<dyn ExecutionPlan>;

                Ok(Transformed::yes(replacement))
            } else {
                Ok(Transformed::no(plan))
            }
        };

        // Step 1: we walk up the tree from the leaves to find the stages
        let up = |plan: Arc<dyn ExecutionPlan>| {
            println!("examining plan: {}", displayable(plan.as_ref()).one_line());

            if let Some(stage_exec) = plan.as_any().downcast_ref::<RayStageExec>() {
                let input = plan.children();
                assert!(input.len() == 1, "RayStageExec must have exactly one child");
                let input = input[0];

                let fixed_plan = input.clone().transform_down(down)?.data;

                // insert a coalescing batches here too so that we aren't sending
                // too small of batches over the network
                let final_plan = Arc::new(CoalesceBatchesExec::new(fixed_plan, batch_size))
                    as Arc<dyn ExecutionPlan>;

                let stage = PyStage::new(
                    stage_exec.stage_id.clone(),
                    final_plan,
                    self.coordinator_id.clone(),
                );

                /*println!(
                    "made new stage {}: plan:\n{}",
                    stage_exec.stage_id,
                    displayable(stage.plan.as_ref()).indent(true)
                );*/

                stages.push(stage);
            }

            Ok(Transformed::no(plan))
        };

        self.physical_plan.clone().transform_up(up)?;

        Ok(stages)
    }

    fn execution_plan(&self) -> PyResult<PyExecutionPlan> {
        Ok(PyExecutionPlan::new(self.physical_plan.clone()))
    }
}
