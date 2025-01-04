use std::{fmt::Formatter, ops::Index, sync::Arc};

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

use crate::{
    ray_shuffle::{RayShuffleExec, ShadowPartitionNumber},
    shadow::ShadowPartitionExec,
};

#[derive(Debug)]
pub struct ShufflerCodec {}

impl PhysicalExtensionCodec for ShufflerCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: make this more robust
        assert_eq!(inputs.len(), 1);
        if buf == "ShadowPartitionExec".as_bytes() {
            Ok(Arc::new(ShadowPartitionExec::new(inputs[0].clone())))
        } else if buf.starts_with("RayShuffleExec".as_bytes()) {
            let mut end = "RayShuffleExec".len();

            let delim: u8 = b"|"[0];

            let mut start = end + 1;
            /*println!(
                "og buf {}",
                std::str::from_utf8(&buf).expect("valid string")
            );
            println!(
                "searching buf {}",
                std::str::from_utf8(&buf[start..]).expect("valid string 2")
            );*/
            end = start
                + buf[start..]
                    .iter()
                    .position(|b| b == &delim)
                    .ok_or(internal_datafusion_err!("Invalid buffer"))?;

            //println!("1start: {}, end: {}", start, end);

            let unique_id = std::str::from_utf8(&buf[start..end])
                .map_err(|e| internal_datafusion_err!("{e}"))?;

            start = end + 1;
            //println!(
            //    "searching buf {}",
            //    std::str::from_utf8(&buf[start..]).expect("valid string 2")
            //);

            let end = start
                + buf[start..]
                    .iter()
                    .position(|b| b == &delim)
                    .ok_or(internal_datafusion_err!("Invalid buffer"))?;

            //println!("2start: {}, end: {}", start, end);

            let output_partitions = std::str::from_utf8(&buf[start..end])
                .map_err(|e| internal_datafusion_err!("{e}"))
                .and_then(|s| {
                    s.parse::<usize>()
                        .map_err(|e| internal_datafusion_err!("{e}"))
                })?;

            start = end + 1;

            /*println!(
                "searching buf {}",
                std::str::from_utf8(&buf[start..]).expect("valid string 2")
            );*/
            let end = buf.len();
            //println!("3start: {}, end: {}", start, end);

            let input_partitions = std::str::from_utf8(&buf[start..end])
                .map_err(|e| internal_datafusion_err!("{e}"))
                .and_then(|s| {
                    s.parse::<usize>()
                        .map_err(|e| internal_datafusion_err!("{e}"))
                })?;

            Ok(Arc::new(RayShuffleExec::new(
                inputs[0].clone(),
                output_partitions,
                input_partitions,
                unique_id.to_owned(),
            )))
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
        } else if let Some(ray_shuffle) = node.as_any().downcast_ref::<RayShuffleExec>() {
            buf.extend_from_slice(
                format!(
                    "RayShuffleExec|{}|{}|{}",
                    ray_shuffle.unique_id,
                    ray_shuffle.output_partitions,
                    ray_shuffle.input_partitions
                )
                .as_bytes(),
            );
            Ok(())
        } else {
            internal_err!("Not supported")
        }
    }
}
