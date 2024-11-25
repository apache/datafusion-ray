use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_python::context::PySessionContext;
use datafusion_python::utils::wait_for_future;
use pyo3::{pyfunction, PyResult, Python};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

mod built_in;

#[cfg(feature = "flight-sql-tables")]
mod flight;

/// Creates a datafusion session context preconfigured with the enabled extensions
/// that will register additional table providers, catalogs etc.
/// If no extensions are required, the plain `datafusion.SessionContext()` will work just fine.
/// # Arguments
/// * `settings` - dictionary containing extension-specific key/value config options
#[pyfunction]
pub fn extended_session_context(
    settings: HashMap<String, String>,
    py: Python,
) -> PyResult<PySessionContext> {
    let future_context = Extensions::session_context(&settings);
    let ctx = wait_for_future(py, future_context)?;
    Ok(ctx.into())
}

/// Allows third party table/catalog providers, object stores, etc.
/// to be registered with the DataFusion context.
#[async_trait]
trait Extension: Debug + Send + Sync + 'static {
    /// SessionContext initialization, using the provided key/value settings if needed.
    /// Declared async to allow implementers to perform network or other I/O operations.
    async fn init(&self, ctx: &SessionContext, settings: &HashMap<String, String>) -> Result<()> {
        let _ = ctx;
        let _ = settings;
        Ok(())
    }

    /// Codecs for the custom physical plan nodes created by this extension, if any.
    fn codecs(&self) -> Vec<Box<dyn PhysicalExtensionCodec>> {
        vec![]
    }
}

/// A composite extension registry for enabled extensions.
#[derive(Debug)]
pub(crate) struct Extensions(Box<[Box<dyn Extension>]>);

#[async_trait]
impl Extension for Extensions {
    async fn init(&self, ctx: &SessionContext, settings: &HashMap<String, String>) -> Result<()> {
        for ext in &self.0 {
            ext.init(ctx, settings).await?;
        }
        Ok(())
    }

    fn codecs(&self) -> Vec<Box<dyn PhysicalExtensionCodec>> {
        self.0.iter().flat_map(|ext| ext.codecs()).collect()
    }
}

impl Extensions {
    fn new() -> Self {
        Self(Box::new([
            Box::new(built_in::DefaultExtension::default()),
            #[cfg(feature = "flight-sql-tables")]
            Box::new(flight::FlightSqlTables::default()),
        ]))
    }

    fn singleton() -> &'static Self {
        static EXTENSIONS: OnceLock<Extensions> = OnceLock::new();
        EXTENSIONS.get_or_init(Self::new)
    }

    pub(crate) async fn session_context(
        settings: &HashMap<String, String>,
    ) -> Result<SessionContext> {
        let ctx = SessionContext::new();
        Self::singleton().init(&ctx, settings).await?;
        Ok(ctx)
    }

    pub(crate) fn codec() -> &'static CompositeCodec {
        static COMPOSITE_CODEC: OnceLock<CompositeCodec> = OnceLock::new();
        COMPOSITE_CODEC.get_or_init(|| CompositeCodec(Extensions::singleton().codecs().into()))
    }
}

/// For both encoding and decoding, tries all the registered extension codecs and returns the first successful result.
#[derive(Debug)]
pub(crate) struct CompositeCodec(Box<[Box<dyn PhysicalExtensionCodec>]>);

impl PhysicalExtensionCodec for CompositeCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.0
            .iter()
            .filter_map(|codec| codec.try_decode(buf, inputs, registry).ok())
            .next()
            .ok_or_else(|| DataFusionError::Execution("No compatible codec found".into()))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        self.0
            .iter()
            .filter_map(|codec| codec.try_encode(node.clone(), buf).ok())
            .next()
            .ok_or_else(|| {
                DataFusionError::Execution(format!("No compatible codec found for {}", node.name()))
            })
    }
}
