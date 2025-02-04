use std::{fmt::Formatter, sync::Arc};

use datafusion::{
    common::internal_datafusion_err,
    error::Result,
    execution::SendableRecordBatchStream,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties},
};

pub struct ShadowPartitionNumber(pub usize);

/// This is a simple execution plan that isolates a partition from the input plan
/// It will advertise that it has a single partition and when
/// asked to execute, it will execute a particular partition from the child
/// input plan.
///
/// This allows us to execute Repartition Exec's on different processes
/// by showing each one only a single child partition
#[derive(Debug)]
pub struct PartitionIsolatorExec {
    pub input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl PartitionIsolatorExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        // We advertise that we only have one partition
        let properties = input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(1));

        Self { input, properties }
    }
}

impl DisplayAs for PartitionIsolatorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PartitionIsolatorExec")
    }
}

impl ExecutionPlan for PartitionIsolatorExec {
    fn name(&self) -> &str {
        "PartitionIsolatorExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: generalize this
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(children[0].clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(internal_datafusion_err!(
                "Partition Isolator Expects partiton zero only for execute"
            ));
        }

        let config = context.session_config();
        let shadow =
            config
                .get_extension::<ShadowPartitionNumber>()
                .ok_or(internal_datafusion_err!(
                    "ShadowPartitionNumber not set in session config"
                ))?;

        let actual_partition_number: usize = shadow.0;

        self.input.execute(actual_partition_number, context)
    }
}
