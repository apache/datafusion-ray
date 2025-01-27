use std::sync::Arc;

use crate::{
    isolator::PartitionIsolatorExec,
    max_rows::MaxRowsExec,
    protobuf::{MaxRowsExecNode, RayStageReaderExecNode},
};

use arrow::datatypes::Schema;
use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::Result,
    execution::FunctionRegistry,
    physical_plan::{displayable, ExecutionPlan},
};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::{
    from_proto::parse_protobuf_partitioning, to_proto::serialize_partitioning,
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf;

use prost::Message;

use crate::ray_stage_reader::RayStageReaderExec;

#[derive(Debug)]
pub struct RayCodec {}

impl PhysicalExtensionCodec for RayCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("decoding {:?}", buf);
        // TODO: clean this up
        if buf == "PartitionIsolatorExec".as_bytes() {
            if inputs.len() != 1 {
                Err(internal_datafusion_err!(
                    "PartitionIsolatorExec requires one input"
                ))
            } else {
                Ok(Arc::new(PartitionIsolatorExec::new(inputs[0].clone())))
            }
        } else if let Ok(node) = RayStageReaderExecNode::decode(buf) {
            println!("decoding as stage reader");

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

            Ok(Arc::new(RayStageReaderExec::try_new(
                part,
                Arc::new(schema),
                node.stage_id as usize,
                node.coordinator_id,
            )?))
        } else if let Ok(node) = MaxRowsExecNode::decode(buf) {
            println!("decoding as max rows");
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
        } else {
            internal_err!("Should not reach this point")
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        println!("try encoding {} ", displayable(node.as_ref()).one_line());
        if let Some(reader) = node.as_any().downcast_ref::<RayStageReaderExec>() {
            let schema: protobuf::Schema = reader.schema().try_into()?;
            let partitioning: protobuf::Partitioning = serialize_partitioning(
                reader.properties().output_partitioning(),
                &DefaultPhysicalExtensionCodec {},
            )?;

            let pb = RayStageReaderExecNode {
                schema: Some(schema),
                partitioning: Some(partitioning),
                stage_id: reader.stage_id as u64,
                coordinator_id: reader.coordinator_id.clone(),
            };

            pb.encode(buf)
                .map_err(|e| internal_datafusion_err!("can't encode ray stage reader pb: {e}"))?;
            println!("encoded into: {:?}", buf);
            Ok(())
        } else if node
            .as_any()
            .downcast_ref::<PartitionIsolatorExec>()
            .is_some()
        {
            println!("encoding as ray parition isolator");
            buf.extend_from_slice(b"PartitionIsolatorExec");
            println!("encoded into: {:?}", buf);

            Ok(())
        } else if let Some(max) = node.as_any().downcast_ref::<MaxRowsExec>() {
            println!("encoding as max rows");
            let pb = MaxRowsExecNode {
                max_rows: max.max_rows as u64,
            };
            pb.encode(buf)
                .map_err(|e| internal_datafusion_err!("can't encode max rows pb: {e}"))?;

            println!("encoded into: {:?}", buf);
            Ok(())
        } else {
            internal_err!("Not supported")
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::ray_stage_reader::RayStageReaderExec;
    use arrow::datatypes::DataType;
    use datafusion::{physical_plan::Partitioning, prelude::SessionContext};

    use std::sync::Arc;

    #[test]
    fn stage_reader_round_trip() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", DataType::Int32, false),
            arrow::datatypes::Field::new("b", DataType::Int32, false),
        ]));
        let ctx = SessionContext::new();
        let part = Partitioning::UnknownPartitioning(2);
        let exec = Arc::new(
            RayStageReaderExec::try_new(part, schema, 1, "coordinator_id".to_owned()).unwrap(),
        );
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
            Arc::new(
                RayStageReaderExec::try_new(part, schema, 1, "coordinator_id".to_owned()).unwrap(),
            ),
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
        let round_trip = displayable(result_exec_plan.as_ref())
            .indent(true)
            .to_string();
        assert_eq!(input, round_trip);
    }
}
