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
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaxRowsExecNode {
    #[prost(uint64, tag = "1")]
    pub max_rows: u64,
}
/// TODO: why, if FlightTicketData has  the uint64 field first can it also be decoded also
/// MaxRowsExecNode?  There is something I don't understand here
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlightTicketData {
    /// stage id of the stream
    /// parittion id of the stream
    #[prost(uint64, tag = "2")]
    pub partition: u64,
}
