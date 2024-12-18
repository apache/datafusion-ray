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

use crate::protobuf::ray_sql_exec_node::PlanType;
use crate::protobuf::{RaySqlExecNode, ShuffleReaderExecNode, ShuffleWriterExecNode};
use crate::shuffle::{ShuffleReaderExec, ShuffleWriterExec};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::{self, PhysicalHashRepartition};
use prost::Message;
use std::sync::Arc;

#[derive(Debug)]
pub struct ShuffleCodec {}

impl PhysicalExtensionCodec for ShuffleCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // decode bytes to protobuf struct
        let node = RaySqlExecNode::decode(buf)
            .map_err(|e| DataFusionError::Internal(format!("failed to decode plan: {e:?}")))?;
        let extension_codec = DefaultPhysicalExtensionCodec {};
        match node.plan_type {
            Some(PlanType::ShuffleReader(reader)) => {
                let schema = reader.schema.as_ref().unwrap();
                let schema: SchemaRef = Arc::new(schema.try_into().unwrap());
                let hash_part = parse_protobuf_hash_partitioning(
                    reader.partitioning.as_ref(),
                    registry,
                    &schema,
                    &extension_codec,
                )?;
                Ok(Arc::new(ShuffleReaderExec::new(
                    reader.stage_id as usize,
                    schema,
                    hash_part.unwrap(),
                    &reader.shuffle_dir,
                )))
            }
            Some(PlanType::ShuffleWriter(writer)) => {
                let plan = inputs[0].clone();
                let hash_part = parse_protobuf_hash_partitioning(
                    writer.partitioning.as_ref(),
                    registry,
                    plan.schema().as_ref(),
                    &extension_codec,
                )?;
                Ok(Arc::new(ShuffleWriterExec::new(
                    writer.stage_id as usize,
                    plan,
                    hash_part.unwrap(),
                    &writer.shuffle_dir,
                )))
            }
            _ => Err(DataFusionError::Execution(
                "Missing or unexpected plan_type".into(),
            )),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        let plan = if let Some(reader) = node.as_any().downcast_ref::<ShuffleReaderExec>() {
            let schema: protobuf::Schema = reader.schema().try_into().unwrap();
            let partitioning =
                encode_partitioning_scheme(reader.properties().output_partitioning())?;
            let reader = ShuffleReaderExecNode {
                stage_id: reader.stage_id as u32,
                schema: Some(schema),
                partitioning: Some(partitioning),
                shuffle_dir: reader.shuffle_dir.clone(),
            };
            PlanType::ShuffleReader(reader)
        } else if let Some(writer) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            let partitioning =
                encode_partitioning_scheme(writer.properties().output_partitioning())?;
            let writer = ShuffleWriterExecNode {
                stage_id: writer.stage_id as u32,
                // No need to redundantly serialize the child plan, as input plan(s) are recursively
                // serialized by PhysicalPlanNode and will be available as `inputs` in `try_decode`.
                // TODO: remove this field from the proto definition?
                plan: None,
                partitioning: Some(partitioning),
                shuffle_dir: writer.shuffle_dir.clone(),
            };
            PlanType::ShuffleWriter(writer)
        } else {
            return Err(DataFusionError::Execution(format!(
                "Unsupported plan node: {}",
                node.name()
            )));
        };
        plan.encode(buf);
        Ok(())
    }
}

fn encode_partitioning_scheme(partitioning: &Partitioning) -> Result<PhysicalHashRepartition> {
    match partitioning {
        Partitioning::Hash(expr, partition_count) => Ok(protobuf::PhysicalHashRepartition {
            hash_expr: expr
                .iter()
                .map(|expr| serialize_physical_expr(expr, &DefaultPhysicalExtensionCodec {}))
                .collect::<Result<Vec<_>, DataFusionError>>()?,
            partition_count: *partition_count as u64,
        }),
        Partitioning::UnknownPartitioning(n) => Ok(protobuf::PhysicalHashRepartition {
            hash_expr: vec![],
            partition_count: *n as u64,
        }),
        other => Err(DataFusionError::Plan(format!(
            "Unsupported shuffle partitioning scheme: {other:?}"
        ))),
    }
}
