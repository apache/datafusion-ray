#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RayStageReaderExecNode {
    /// stage to read from
    #[prost(string, tag = "1")]
    pub stage_id: ::prost::alloc::string::String,
    /// identifier of the RayQueryCoordinator Actor we need to contact
    #[prost(string, tag = "2")]
    pub coordinator_id: ::prost::alloc::string::String,
    /// schema of the stage we will consume
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    /// properties of the stage we will consume
    #[prost(message, optional, tag = "4")]
    pub partitioning: ::core::option::Option<::datafusion_proto::protobuf::Partitioning>,
}
