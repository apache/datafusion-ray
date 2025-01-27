use std::{fmt::Formatter, sync::Arc};

use arrow::record_batch::RecordBatchReader;
use arrow_flight::{FlightClient, Ticket};
use datafusion::common::internal_datafusion_err;
use datafusion::error::Result;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};
use futures::stream::TryStreamExt;
use futures::StreamExt;
use prost::Message;
use rust_decimal::prelude::*;

use crate::protobuf::StreamMeta;
use crate::pystage::ExchangeFlightClient;

#[derive(Debug)]
pub struct RayStageReaderExec {
    properties: PlanProperties,
    schema: SchemaRef,
    pub stage_id: usize,
    pub coordinator_id: String,
}

impl RayStageReaderExec {
    pub fn try_new_from_input(
        input: Arc<dyn ExecutionPlan>,
        stage_id: usize,
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
        stage_id: usize,
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

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        println!(
            "RayStageReaderExec[{}-{}] execute",
            self.stage_id, partition
        );
        let in_client_map = &context
            .session_config()
            .get_extension::<ExchangeFlightClient>()
            .ok_or(internal_datafusion_err!("Flight Client not in context"))?
            .clone()
            .0;

        let mut client = in_client_map
            .get(&self.stage_id)
            .map(|flight_client| FlightClient::new_from_inner(flight_client.inner().clone()))
            .ok_or(internal_datafusion_err!(
                "Flight Client not found for stage {}",
                self.stage_id
            ))?;

        let meta = StreamMeta {
            stage_id: self.stage_id as u64,
            partition: partition as u64,
            fraction: Decimal::zero().to_string(), // not used in this context
        };

        let ticket = Ticket {
            ticket: meta.encode_to_vec().into(),
        };

        let stage_id = self.stage_id.clone();
        let out_stream = async_stream::stream! {
            let flight_rbr_stream = client.do_get(ticket).await;

            let mut total_rows = 0;
            if let Ok(mut flight_rbr_stream) = flight_rbr_stream {

                while let Some(batch) = flight_rbr_stream.next().await {
                    total_rows += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
                    yield batch
                        .map_err(|e| internal_datafusion_err!("Error reading batch: {}", e));
                }
            } else {
                yield Err(internal_datafusion_err!("Error getting stream"));
            }
            println!(
                "RayStageReaderExec[{}-{}] read {} total rows",
                stage_id, partition, total_rows
            );


        };

        let adapter = RecordBatchStreamAdapter::new(self.schema.clone(), out_stream);

        Ok(Box::pin(adapter))
    }
}
