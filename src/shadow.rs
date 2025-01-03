use std::{fmt::Formatter, sync::Arc};

use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::{DataFusionError, Result},
    execution::{FunctionRegistry, SendableRecordBatchStream},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
        PlanProperties,
    },
};
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};

use crate::ray_shuffle::ShadowPartitionNumber;

#[derive(Debug)]
pub struct ShadowPartitionExec {
    pub input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl ShadowPartitionExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(1));

        Self { input, properties }
    }
}

impl DisplayAs for ShadowPartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShadowPartitionExec")
    }
}

impl ExecutionPlan for ShadowPartitionExec {
    fn name(&self) -> &str {
        "ShadowPartitionExec"
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
        assert_eq!(partition, 0);

        let config = context.session_config();
        let shadow =
            config
                .get_extension::<ShadowPartitionNumber>()
                .ok_or(internal_datafusion_err!(
                    "ShadowPartitionNumber not set in session config"
                ))?;

        let actual_partition_number: usize = shadow.0;

        let total_partitions = self.input.output_partitioning().partition_count();

        println!(
            "Shadowing partition {}/{}",
            actual_partition_number, total_partitions
        );

        self.input.execute(actual_partition_number, context)
    }
}
