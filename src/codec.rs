use std::sync::Arc;

use crate::{isolator::PartitionIsolatorExec, protobuf::RayStageReaderExecNode};

use arrow::datatypes::Schema;
use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::Result,
    execution::FunctionRegistry,
    physical_plan::ExecutionPlan,
};
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
        if buf == "PartitionIsolatorExec".as_bytes() {
            if inputs.len() != 1 {
                Err(internal_datafusion_err!(
                    "PartitionIsolatorExec requires one input"
                ))
            } else {
                Ok(Arc::new(PartitionIsolatorExec::new(inputs[0].clone())))
            }
        } else {
            let node = RayStageReaderExecNode::decode(buf).map_err(|e| {
                internal_datafusion_err!("Couldn't decode ray stage reader proto {e}")
            })?;

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
                node.stage_id,
                node.coordinator_id,
            )?))
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(reader) = node.as_any().downcast_ref::<RayStageReaderExec>() {
            let schema: protobuf::Schema = reader.schema().try_into()?;
            let partitioning: protobuf::Partitioning = serialize_partitioning(
                &reader.properties().output_partitioning(),
                &DefaultPhysicalExtensionCodec {},
            )?;

            let pb = RayStageReaderExecNode {
                schema: Some(schema),
                partitioning: Some(partitioning),
                stage_id: reader.stage_id.clone(),
                coordinator_id: reader.coordinator_id.clone(),
            };

            pb.encode(buf)
                .map_err(|e| internal_datafusion_err!("can't encode ray stage reader pb"))?;

            Ok(())
        } else if node
            .as_any()
            .downcast_ref::<PartitionIsolatorExec>()
            .is_some()
        {
            buf.extend_from_slice(b"PartitionIsolatorExec");

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

    use pyo3::prelude::*;
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
            RayStageReaderExec::try_new(part, schema, "1".to_owned(), "coordinator_id".to_owned())
                .unwrap(),
        );
        let codec = RayCodec {};
        let mut buf = vec![];
        codec.try_encode(exec.clone(), &mut buf).unwrap();
        let decoded = codec.try_decode(&buf, &[], &ctx).unwrap();
        assert_eq!(exec.schema(), decoded.schema());
    }
}
