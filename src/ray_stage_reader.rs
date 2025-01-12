use std::{fmt::Formatter, sync::Arc};

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::record_batch::RecordBatchReader;
use datafusion::arrow::pyarrow::FromPyArrow;
use datafusion::common::internal_datafusion_err;
use datafusion::error::Result;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};
use futures::stream::{self, TryStreamExt};
use log::debug;
use pyo3::prelude::*;

use crate::context::CoordinatorId;

#[derive(Debug)]
pub struct RayStageReaderExec {
    properties: PlanProperties,
    schema: SchemaRef,
    pub stage_id: String,
    pub coordinator_id: String,
}

impl RayStageReaderExec {
    pub fn try_new_from_input(
        input: Arc<dyn ExecutionPlan>,
        stage_id: String,
        coordinator_id: String,
    ) -> Result<Self> {
        let properties = input.properties().clone();

        Self::try_new(
            properties.partitioning.clone(),
            input.schema(),
            stage_id,
            coordinator_id,
        )
    }

    pub fn try_new(
        partitioning: Partitioning,
        schema: SchemaRef,
        stage_id: String,
        coordinator_id: String,
    ) -> Result<Self> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            partitioning,
            ExecutionMode::Unbounded,
        );

        Ok(Self {
            properties,
            schema,
            stage_id,
            coordinator_id,
        })
    }
}
impl DisplayAs for RayStageReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayStageReaderExec[{}] (output_partitioning={:?})",
            self.stage_id,
            self.properties().partitioning
        )
    }
}

impl ExecutionPlan for RayStageReaderExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn name(&self) -> &str {
        "RayStageReaderExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: handle more general case
        unimplemented!()
    }

    /// We will spawn a Ray Task for our child inputs and consume their output stream.
    /// We will have to defer this functionality to python as Ray does not yet have Rust bindings.
    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // serialize our input plan
        let coordinator_id = context
            .session_config()
            .get_extension::<CoordinatorId>()
            .ok_or(internal_datafusion_err!("CoordinatorId not set"))?
            .0
            .clone();

        debug!("RayStageReaderExec[stage={}] ::execute", self.stage_id);

        // TODO: Move initialization of ray stage reader object to constructor
        // but it causes problems with serialization at this time.

        // defer execution to the python object which will spawn a Ray Task
        // to execute this partition and send us back a stream of the results
        //
        println!(
            "RayStageReaderExec[{}] executing partition {}",
            self.stage_id, partition
        );
        let record_batch_reader = Python::with_gil(|py| {
            println!(
                "RayStageReaderExec[{}] partition {} in python",
                self.stage_id, partition
            );
            let module = PyModule::import_bound(py, "datafusion_ray.context")?;
            println!(
                "RayStageReaderExec[{}] partition {} in python got module",
                self.stage_id, partition
            );
            let py_stage_reader = module.call_method1("StageReader", (coordinator_id,))?;
            println!(
                "RayStageReaderExec[{}] partition {} in python created stage reader",
                self.stage_id, partition
            );

            let py_obj =
                py_stage_reader.call_method1("reader", (self.stage_id.clone(), partition))?;
            println!(
                "RayStageReaderExec[{}] partition {} in python called method",
                self.stage_id, partition
            );
            let record_batch_reader = ArrowArrayStreamReader::from_pyarrow_bound(&py_obj)?;
            Ok::<ArrowArrayStreamReader, PyErr>(record_batch_reader)
        })
        .map_err(|e| internal_datafusion_err!("Error executing RayStageReaderExec: {:?}", e));

        let record_batch_reader = match record_batch_reader {
            Ok(rbr) => rbr,
            Err(e) => {
                println!(
                    "RayStageReaderExec[{}] partition {} error: {:?}",
                    self.stage_id, partition, e
                );
                return Err(e);
            }
        };
        println!(
            "RayStageReaderExec[{}] partition {} made reader",
            self.stage_id, partition
        );

        let schema = record_batch_reader.schema();

        let the_stream = stream::iter(record_batch_reader)
            .map_err(|e| internal_datafusion_err!("Error reading record batch: {:?}", e));

        let adapted_stream = RecordBatchStreamAdapter::new(schema, the_stream);

        Ok(Box::pin(adapted_stream))
    }
}
