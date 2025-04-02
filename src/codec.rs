use std::sync::Arc;

use crate::{
    isolator::PartitionIsolatorExec,
    max_rows::MaxRowsExec,
    pre_fetch::PrefetchExec,
    protobuf::{
        DfRayStageReaderExecNode, MaxRowsExecNode, PartitionIsolatorExecNode, PrefetchExecNode,
    },
};

use arrow::datatypes::Schema;
use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::Result,
    execution::FunctionRegistry,
    physical_plan::ExecutionPlan,
};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec, from_proto::parse_protobuf_partitioning,
    to_proto::serialize_partitioning,
};
use datafusion_proto::protobuf;

use prost::Message;

use crate::stage_reader::DFRayStageReaderExec;

#[derive(Debug)]
/// Physical Extension Codec for for DataFusion for Ray plans
pub struct RayCodec {}

impl PhysicalExtensionCodec for RayCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: clean this up
        if let Ok(node) = PartitionIsolatorExecNode::decode(buf) {
            if inputs.len() != 1 {
                Err(internal_datafusion_err!(
                    "PartitionIsolatorExec requires one input"
                ))
            } else {
                Ok(Arc::new(PartitionIsolatorExec::new(
                    inputs[0].clone(),
                    node.partition_count as usize,
                )))
            }
        } else if let Ok(node) = DfRayStageReaderExecNode::decode(buf) {
            let schema: Schema = node
                .schema
                .as_ref()
                .ok_or(internal_datafusion_err!("missing schema in proto"))?
                .try_into()?;

            let part = parse_protobuf_partitioning(
                node.partitioning.as_ref(),
                registry,
                &schema,
                &DefaultPhysicalExtensionCodec {},
            )?
            .ok_or(internal_datafusion_err!("missing partitioning in proto"))?;

            Ok(Arc::new(DFRayStageReaderExec::try_new(
                part,
                Arc::new(schema),
                node.stage_id as usize,
            )?))
        } else if let Ok(node) = MaxRowsExecNode::decode(buf) {
            if inputs.len() != 1 {
                Err(internal_datafusion_err!(
                    "MaxRowsExec requires one input, got {}",
                    inputs.len()
                ))
            } else {
                Ok(Arc::new(MaxRowsExec::new(
                    inputs[0].clone(),
                    node.max_rows as usize,
                )))
            }
        } else if let Ok(node) = PrefetchExecNode::decode(buf) {
            if inputs.len() != 1 {
                Err(internal_datafusion_err!(
                    "MaxRowsExec requires one input, got {}",
                    inputs.len()
                ))
            } else {
                Ok(Arc::new(PrefetchExec::new(
                    inputs[0].clone(),
                    node.buf_size as usize,
                )))
            }
        } else {
            internal_err!("Should not reach this point")
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(reader) = node.as_any().downcast_ref::<DFRayStageReaderExec>() {
            let schema: protobuf::Schema = reader.schema().try_into()?;
            let partitioning: protobuf::Partitioning = serialize_partitioning(
                reader.properties().output_partitioning(),
                &DefaultPhysicalExtensionCodec {},
            )?;

            let pb = DfRayStageReaderExecNode {
                schema: Some(schema),
                partitioning: Some(partitioning),
                stage_id: reader.stage_id as u64,
            };

            pb.encode(buf)
                .map_err(|e| internal_datafusion_err!("can't encode ray stage reader pb: {e}"))?;
            Ok(())
        } else if let Some(pi) = node.as_any().downcast_ref::<PartitionIsolatorExec>() {
            let pb = PartitionIsolatorExecNode {
                dummy: 0.0,
                partition_count: pi.partition_count as u64,
            };

            pb.encode(buf)
                .map_err(|e| internal_datafusion_err!("can't encode partition isolator pb: {e}"))?;

            Ok(())
        } else if let Some(max) = node.as_any().downcast_ref::<MaxRowsExec>() {
            let pb = MaxRowsExecNode {
                max_rows: max.max_rows as u64,
            };
            pb.encode(buf)
                .map_err(|e| internal_datafusion_err!("can't encode max rows pb: {e}"))?;

            Ok(())
        } else if let Some(pre) = node.as_any().downcast_ref::<PrefetchExec>() {
            let pb = PrefetchExecNode {
                dummy: 0,
                buf_size: pre.buf_size as u64,
            };
            pb.encode(buf)
                .map_err(|e| internal_datafusion_err!("can't encode prefetch pb: {e}"))?;

            Ok(())
        } else {
            internal_err!("Not supported")
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::stage_reader::DFRayStageReaderExec;
    use arrow::datatypes::DataType;
    use datafusion::{
        physical_plan::{Partitioning, display::DisplayableExecutionPlan, displayable},
        prelude::SessionContext,
    };
    use datafusion_proto::physical_plan::AsExecutionPlan;

    use std::sync::Arc;

    #[test]
    fn stage_reader_round_trip() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", DataType::Int32, false),
            arrow::datatypes::Field::new("b", DataType::Int32, false),
        ]));
        let ctx = SessionContext::new();
        let part = Partitioning::UnknownPartitioning(2);
        let exec = Arc::new(DFRayStageReaderExec::try_new(part, schema, 1).unwrap());
        let codec = RayCodec {};
        let mut buf = vec![];
        codec.try_encode(exec.clone(), &mut buf).unwrap();
        let decoded = codec.try_decode(&buf, &[], &ctx).unwrap();
        assert_eq!(exec.schema(), decoded.schema());
    }
    #[test]
    fn max_rows_and_reader_round_trip() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", DataType::Int32, false),
            arrow::datatypes::Field::new("b", DataType::Int32, false),
        ]));
        let ctx = SessionContext::new();
        let part = Partitioning::UnknownPartitioning(2);
        let exec = Arc::new(MaxRowsExec::new(
            Arc::new(DFRayStageReaderExec::try_new(part, schema, 1).unwrap()),
            10,
        ));
        let codec = RayCodec {};

        // serialize execution plan to proto
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(exec.clone(), &codec)
                .expect("to proto");

        // deserialize proto back to execution plan
        let runtime = ctx.runtime_env();
        let result_exec_plan: Arc<dyn ExecutionPlan> = proto
            .try_into_physical_plan(&ctx, runtime.as_ref(), &codec)
            .expect("from proto");

        let input = displayable(exec.as_ref()).indent(true).to_string();
        let round_trip = {
            let plan: &dyn ExecutionPlan = result_exec_plan.as_ref();
            DisplayableExecutionPlan::new(plan)
        }
        .indent(true)
        .to_string();
        assert_eq!(input, round_trip);
    }
}
