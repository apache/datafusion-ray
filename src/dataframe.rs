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
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_python::physical_plan::PyExecutionPlan;
use datafusion_python::utils::wait_for_future;
use futures::stream::StreamExt;
use pyo3::prelude::*;
use std::borrow::Cow;
use std::sync::Arc;
use tonic::transport::Channel;

use crate::isolator::PartitionIsolatorExec;
use crate::max_rows::MaxRowsExec;
use crate::pystage::ExchangeFlightClient;
use crate::ray_stage::RayStageExec;
use crate::ray_stage_reader::RayStageReaderExec;
use crate::util::physical_plan_to_bytes;
use crate::util::ResultExt;

pub struct CoordinatorId(pub String);

#[pyclass]
pub struct RayDataFrame {
    physical_plan: Arc<dyn ExecutionPlan>,
    #[pyo3(get)]
    coordinator_id: String,
    #[pyo3(get)]
    bucket: Option<String>,
    final_plan: Option<Arc<dyn ExecutionPlan>>,
}

impl RayDataFrame {
    pub fn new(
        physical_plan: Arc<dyn ExecutionPlan>,
        coordinator_id: String,
        bucket: Option<String>,
    ) -> Self {
        Self {
            physical_plan,
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
            //println!("examining plan: {}", displayable(plan.as_ref()).one_line());

            if let Some(stage_exec) = plan.as_any().downcast_ref::<RayStageExec>() {
                let input = plan.children();
                assert!(input.len() == 1, "RayStageExec must have exactly one child");
                let input = input[0];

                let fixed_plan = input.clone().transform_down(down)?.data;

                let stage = PyDataFrameStage::new(stage_exec.stage_id.clone(), fixed_plan);

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
                Ok(Transformed::yes(replacement))
            } else {
                Ok(Transformed::no(plan))
            }
        };

        self.physical_plan.clone().transform_up(up)?;

        // add coalesce and max rows to last stage
        let mut last_stage = stages
            .pop()
            .ok_or(internal_datafusion_err!("No stages found"))?;

        last_stage = PyDataFrameStage::new(
            last_stage.stage_id,
            Arc::new(MaxRowsExec::new(
                Arc::new(CoalesceBatchesExec::new(last_stage.plan, inner_batch_size))
                    as Arc<dyn ExecutionPlan>,
                max_rows,
            )) as Arc<dyn ExecutionPlan>,
        );

        // done fixing last stage

        let reader_plan = Arc::new(RayStageReaderExec::try_new_from_input(
            last_stage.plan.clone(),
            last_stage.stage_id.clone(),
            self.coordinator_id.clone(),
        )?) as Arc<dyn ExecutionPlan>;

        stages.push(last_stage);

        if reader_plan.output_partitioning().partition_count() > 1 {
            self.final_plan = Some(Arc::new(CoalescePartitionsExec::new(reader_plan)));
        } else {
            self.final_plan = Some(reader_plan);
        };

        Ok(stages)
    }

    fn execution_plan(&self) -> PyResult<PyExecutionPlan> {
        Ok(PyExecutionPlan::new(self.physical_plan.clone()))
    }

    pub fn execute(&self, py: Python, exchange_addr: String) -> PyResult<PyRecordBatchStream> {
        // TODO: consolidate this code

        let url = format!("http://{exchange_addr}");

        let chan = Channel::from_shared(url).to_py_err()?;
        let fut = async {
            let connect = chan.connect();
            connect.await
        };
        let channel = match wait_for_future(py, fut) {
            Ok(channel) => channel,
            _ => {
                return Err(pyo3::exceptions::PyException::new_err(
                    "error connecting to exchange".to_string(),
                ));
            }
        };

        let client = FlightClient::new(channel);

        let config = SessionConfig::new().with_extension(Arc::new(ExchangeFlightClient(client)));

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
    stage_id: String,
    plan: Arc<dyn ExecutionPlan>,
}
impl PyDataFrameStage {
    fn new(stage_id: String, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { stage_id, plan }
    }
}

#[pymethods]
impl PyDataFrameStage {
    #[getter]
    fn stage_id(&self) -> &str {
        &self.stage_id
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
