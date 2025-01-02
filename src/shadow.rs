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

#[derive(Debug)]
pub struct ShadowCodec {}

impl PhysicalExtensionCodec for ShadowCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if buf == "ShadowPartitionExec".as_bytes() {
            // TODO: generalize
            assert_eq!(inputs.len(), 1);
            Ok(Arc::new(ShadowPartitionExec::new(inputs[0].clone())))
        } else {
            internal_err!("Not supported")
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if node
            .as_any()
            .downcast_ref::<ShadowPartitionExec>()
            .is_some()
        {
            buf.extend_from_slice("ShadowPartitionExec".as_bytes());
            Ok(())
        } else {
            internal_err!("Not supported")
        }
    }
}
