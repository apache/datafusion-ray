use crate::ext::Extension;
use crate::shuffle::ShuffleCodec;
use async_trait::async_trait;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

#[derive(Debug, Default)]
pub(super) struct DefaultExtension {}

#[async_trait]
impl Extension for DefaultExtension {
    fn codecs(&self) -> Vec<Box<dyn PhysicalExtensionCodec>> {
        vec![Box::new(ShuffleCodec {})]
    }
}
