#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RayStageReaderExecNode {
    /// schema of the stage we will consume
    #[prost(message, optional, tag = "1")]
    pub schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    /// properties of the stage we will consume
    #[prost(message, optional, tag = "2")]
    pub partitioning: ::core::option::Option<::datafusion_proto::protobuf::Partitioning>,
    /// stage to read from
    #[prost(uint64, tag = "3")]
    pub stage_id: u64,
    /// identifier of the RayQueryCoordinator Actor we need to contact
    #[prost(string, tag = "4")]
    pub coordinator_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaxRowsExecNode {
    #[prost(uint64, tag = "1")]
    pub max_rows: u64,
}
/// TODO: why, if StreamMeta has  the uint64 field first can it also be decoded also
/// MaxRowsExecNode?  There is something I don't understand here
/// Same with RayStageReaderExecNode, for now, I will ensure they have different
/// typed first fields
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamMeta {
    /// what fraction of the work do we represent, serialized as a string
    #[prost(string, tag = "1")]
    pub fraction: ::prost::alloc::string::String,
    /// stage id of the stream
    #[prost(uint64, tag = "2")]
    pub stage_id: u64,
    /// parittion id of the stream
    #[prost(uint64, tag = "3")]
    pub partition: u64,
}
