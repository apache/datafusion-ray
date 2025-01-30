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

use arrow::array::RecordBatch;
use arrow::pyarrow::ToPyArrow;
use arrow_flight::FlightClient;
use datafusion::common::internal_datafusion_err;
use datafusion::common::internal_err;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::DataFrame;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_python::physical_plan::PyExecutionPlan;
use datafusion_python::sql::logical::PyLogicalPlan;
use datafusion_python::utils::wait_for_future;
use futures::stream::StreamExt;
use pyo3::prelude::*;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

use crate::isolator::PartitionIsolatorExec;
use crate::max_rows::MaxRowsExec;
use crate::pystage::ExchangeAddrs;
use crate::ray_stage::RayStageExec;
use crate::ray_stage_reader::RayStageReaderExec;
use crate::util::make_client;
use crate::util::physical_plan_to_bytes;
use crate::util::ResultExt;

pub struct CoordinatorId(pub String);

#[pyclass]
pub struct RayDataFrame {
    df: DataFrame,
    #[pyo3(get)]
    coordinator_id: String,
    #[pyo3(get)]
    bucket: Option<String>,
    final_plan: Option<Arc<dyn ExecutionPlan>>,
}

impl RayDataFrame {
    pub fn new(df: DataFrame, coordinator_id: String, bucket: Option<String>) -> Self {
        Self {
            df,
            coordinator_id,
            bucket,
            final_plan: None,
        }
    }
}

#[pymethods]
impl RayDataFrame {
    fn stages(
        &mut self,
        py: Python,
        batch_size: usize,
        isolate_partitions: bool,
    ) -> PyResult<Vec<PyDataFrameStage>> {
        let mut stages = vec![];
        let max_rows = batch_size;
        let inner_batch_size = batch_size;

        // TODO: This can be done more efficiently, likely in one pass but I'm
        // struggling to get the TreeNodeRecursion return values to make it do
        // what I want. So, two steps for now

        // Step 2: we walk down this stage and replace stages earlier in the tree with
        // RayStageReaderExecs as we will need to consume their output instead of
        // execute that part of the tree ourselves
        let down = |plan: Arc<dyn ExecutionPlan>| {
            /*println!(
                "examining plan down: {}",
                displayable(plan.as_ref()).indent(true)
            );*/

            if let Some(stage_exec) = plan.as_any().downcast_ref::<RayStageExec>() {
                let input = plan.children();
                assert!(input.len() == 1, "RayStageExec must have exactly one child");
                let input = input[0];

                let replacement = Arc::new(RayStageReaderExec::try_new_from_input(
                    input.clone(),
                    stage_exec.stage_id,
                    self.coordinator_id.clone(),
                )?) as Arc<dyn ExecutionPlan>;

                Ok(Transformed {
                    data: replacement,
                    transformed: true,
                    tnr: TreeNodeRecursion::Jump,
                })
            } else {
                Ok(Transformed::no(plan))
            }
        };

        let mut consume_all_partitions = false;
        // Step 1: we walk up the tree from the leaves to find the stages
        let up = |plan: Arc<dyn ExecutionPlan>| {
            //println!("examining plan up: {}", displayable(plan.as_ref()).one_line());
            //

            if let Some(stage_exec) = plan.as_any().downcast_ref::<RayStageExec>() {
                let input = plan.children();
                assert!(input.len() == 1, "RayStageExec must have exactly one child");
                let input = input[0];

                let fixed_plan = input.clone().transform_down(down)?.data;

                let stage =
                    PyDataFrameStage::new(stage_exec.stage_id, fixed_plan, consume_all_partitions);

                stages.push(stage);
                Ok(Transformed::no(plan))
            } else if plan.as_any().downcast_ref::<RepartitionExec>().is_some() {
                let mut replacement = plan.clone();
                if isolate_partitions {
                    let children = plan.children();
                    assert!(children.len() == 1, "Unexpected plan structure");

                    let child = children[0];

                    let new_child = Arc::new(PartitionIsolatorExec::new(child.clone()));
                    replacement = replacement.clone().with_new_children(vec![new_child])?;
                }
                // insert a coalescing batches here too so that we aren't sending
                // too small (or too big) of batches over the network
                replacement = Arc::new(MaxRowsExec::new(
                    Arc::new(CoalesceBatchesExec::new(replacement, inner_batch_size))
                        as Arc<dyn ExecutionPlan>,
                    max_rows,
                )) as Arc<dyn ExecutionPlan>;

                consume_all_partitions = true;

                Ok(Transformed::yes(replacement))
            } else if plan.as_any().downcast_ref::<SortExec>().is_some() {
                let mut replacement = plan.clone();

                replacement = Arc::new(MaxRowsExec::new(
                    Arc::new(CoalesceBatchesExec::new(replacement, inner_batch_size))
                        as Arc<dyn ExecutionPlan>,
                    max_rows,
                )) as Arc<dyn ExecutionPlan>;

                consume_all_partitions = false;

                Ok(Transformed::yes(replacement))
            } else if plan.as_any().downcast_ref::<NestedLoopJoinExec>().is_some() {
                let mut replacement = plan.clone();

                replacement = Arc::new(MaxRowsExec::new(
                    Arc::new(CoalesceBatchesExec::new(replacement, inner_batch_size))
                        as Arc<dyn ExecutionPlan>,
                    max_rows,
                )) as Arc<dyn ExecutionPlan>;

                consume_all_partitions = true;

                Ok(Transformed::yes(replacement))
            } else {
                Ok(Transformed::no(plan))
            }
        };

        let physical_plan = wait_for_future(py, self.df.clone().create_physical_plan())?;

        physical_plan.transform_up(up)?;

        // add coalesce and max rows to last stage
        let mut last_stage = stages
            .pop()
            .ok_or(internal_datafusion_err!("No stages found"))?;

        if last_stage.num_output_partitions() > 1 {
            return internal_err!("Last stage expected to have one partition").to_py_err();
        }

        last_stage = PyDataFrameStage::new(
            last_stage.stage_id,
            Arc::new(MaxRowsExec::new(
                Arc::new(CoalesceBatchesExec::new(last_stage.plan, inner_batch_size))
                    as Arc<dyn ExecutionPlan>,
                max_rows,
            )) as Arc<dyn ExecutionPlan>,
            true, // this doesn't really matter here as there is only one
                  // partition on the final stage
        );

        // done fixing last stage

        let reader_plan = Arc::new(RayStageReaderExec::try_new_from_input(
            last_stage.plan.clone(),
            last_stage.stage_id,
            self.coordinator_id.clone(),
        )?) as Arc<dyn ExecutionPlan>;

        stages.push(last_stage);

        self.final_plan = Some(reader_plan);

        Ok(stages)
    }

    fn execution_plan(&self, py: Python) -> PyResult<PyExecutionPlan> {
        let plan = wait_for_future(py, self.df.clone().create_physical_plan())?;
        Ok(PyExecutionPlan::new(plan))
    }

    fn logical_plan(&self) -> PyResult<PyLogicalPlan> {
        Ok(PyLogicalPlan::new(self.df.logical_plan().clone()))
    }

    fn optimized_logical_plan(&self) -> PyResult<PyLogicalPlan> {
        Ok(PyLogicalPlan::new(self.df.clone().into_optimized_plan()?))
    }

    pub fn execute(
        &self,
        exchange_addrs: HashMap<(usize, usize), String>,
    ) -> PyResult<PyRecordBatchStream> {
        let config = SessionConfig::new().with_extension(Arc::new(ExchangeAddrs(exchange_addrs)));

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);

        println!(
            "consuming query results using plan:\n{}",
            self.final_plan
                .as_ref()
                .map(|plan| displayable(plan.as_ref()).indent(true).to_string())
                .unwrap_or("NotFound".to_string())
        );

        self.final_plan
            .clone()
            .ok_or_else(|| internal_datafusion_err!("No final plan found"))
            .and_then(|plan| plan.execute(0, ctx.task_ctx()))
            .map(PyRecordBatchStream::new)
            .to_py_err()
    }
}

#[pyclass]
pub struct PyDataFrameStage {
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    consume_all_partitions: bool,
}
impl PyDataFrameStage {
    fn new(stage_id: usize, plan: Arc<dyn ExecutionPlan>, consume_all_partitions: bool) -> Self {
        Self {
            stage_id,
            plan,
            consume_all_partitions,
        }
    }
}

#[pymethods]
impl PyDataFrameStage {
    #[getter]
    fn stage_id(&self) -> usize {
        self.stage_id
    }
    pub fn num_output_partitions(&self) -> usize {
        self.plan.output_partitioning().partition_count()
    }

    /// How many partitions are we shadowing if at all
    pub fn num_shadow_partitions(&self) -> Option<usize> {
        let mut result = None;
        self.plan
            .clone()
            .transform_down(|node: Arc<dyn ExecutionPlan>| {
                if let Some(isolator) = node.as_any().downcast_ref::<PartitionIsolatorExec>() {
                    let children = isolator.children();
                    if children.len() != 1 {
                        return internal_err!("PartitionIsolatorExec must have exactly one child");
                    }
                    result = Some(children[0].output_partitioning().partition_count());
                    //TODO: break early
                }
                Ok(Transformed::no(node))
            });
        result
    }

    pub fn consume_all_partitions(&self) -> bool {
        self.consume_all_partitions
    }

    #[getter]
    pub fn input_stage_ids(&self) -> PyResult<Vec<usize>> {
        let mut result = vec![];
        self.plan
            .clone()
            .transform_down(|node: Arc<dyn ExecutionPlan>| {
                if let Some(reader) = node.as_any().downcast_ref::<RayStageReaderExec>() {
                    result.push(reader.stage_id);
                }
                Ok(Transformed::no(node))
            })?;
        Ok(result)
    }

    pub fn execution_plan(&self) -> PyExecutionPlan {
        PyExecutionPlan::new(self.plan.clone())
    }

    pub fn plan_bytes(&self) -> PyResult<Cow<[u8]>> {
        let plan_bytes = physical_plan_to_bytes(self.plan.clone())?;
        Ok(Cow::Owned(plan_bytes))
    }
}

#[pyclass]
pub struct PyRecordBatch {
    batch: RecordBatch,
}

#[pymethods]
impl PyRecordBatch {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        self.batch.to_pyarrow(py)
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(batch: RecordBatch) -> Self {
        Self { batch }
    }
}

#[pyclass]
pub struct PyRecordBatchStream {
    stream: SendableRecordBatchStream,
}

impl PyRecordBatchStream {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

#[pymethods]
impl PyRecordBatchStream {
    fn next(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        let result = self.stream.next();
        match wait_for_future(py, result) {
            None => Ok(None),
            Some(Ok(b)) => Ok(Some(b.to_pyarrow(py)?)),
            Some(Err(e)) => Err(e.into()),
        }
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        self.next(py)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}
