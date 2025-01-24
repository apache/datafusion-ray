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
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaxRowsExecNode {
    #[prost(uint64, tag = "1")]
    pub max_rows: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamMeta {
    /// stage id of the stream
    #[prost(uint32, tag = "1")]
    pub stage_num: u32,
    /// parittion id of the stream
    #[prost(uint32, tag = "2")]
    pub partition_num: u32,
    /// what fraction of the work do we represent
    #[prost(float, tag = "3")]
    pub fraction: f32,
}
