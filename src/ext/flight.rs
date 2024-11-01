use crate::ext::Extension;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_table_providers::flight::codec::FlightPhysicalCodec;
use datafusion_table_providers::flight::sql::FlightSqlDriver;
use datafusion_table_providers::flight::FlightTableFactory;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub(super) struct FlightSqlTables {}

#[async_trait]
impl Extension for FlightSqlTables {
    async fn init(
        &self,
        ctx: &SessionContext,
        _settings: &HashMap<String, String>,
    ) -> datafusion::common::Result<()> {
        ctx.state_ref().write().table_factories_mut().insert(
            "FLIGHT_SQL".into(),
            Arc::new(FlightTableFactory::new(
                Arc::new(FlightSqlDriver::default()),
            )),
        );
        Ok(())
    }

    fn codecs(&self) -> Vec<Box<dyn PhysicalExtensionCodec>> {
        vec![Box::new(FlightPhysicalCodec::default())]
    }
}
