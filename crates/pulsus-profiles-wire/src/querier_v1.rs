//! `querier.v1`, the messages of `proto/querier/v1/querier.proto` the
//! profiles query API uses (issue #821), with the JSON mapping of
//! [`crate::types_v1`].

use serde::{Deserialize, Serialize};

use crate::types_v1::{Labels, ProfileType};

/// `ProfileTypes`' request.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfileTypesRequest {
    #[prost(int64, tag = "1")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub start: i64,
    #[prost(int64, tag = "2")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub end: i64,
}

/// `ProfileTypes`' response.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfileTypesResponse {
    #[prost(message, repeated, tag = "1")]
    #[serde(
        rename = "profileTypes",
        alias = "profile_types",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub profile_types: Vec<ProfileType>,
}

/// `Series`' request.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SeriesRequest {
    #[prost(string, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub matchers: Vec<String>,
    #[prost(string, repeated, tag = "2")]
    #[serde(
        rename = "labelNames",
        alias = "label_names",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub label_names: Vec<String>,
    #[prost(int64, tag = "3")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub start: i64,
    #[prost(int64, tag = "4")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub end: i64,
}

/// `Series`' response: field 2, as the proto has it.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SeriesResponse {
    #[prost(message, repeated, tag = "2")]
    #[serde(
        rename = "labelsSet",
        alias = "labels_set",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub labels_set: Vec<Labels>,
}

/// A message without its encoding yet (issue #823's tests-first commit).
macro_rules! unencoded {
    ($t:ty) => {
        impl ::prost::Message for $t {
            fn encode_raw(&self, _buf: &mut impl ::prost::bytes::BufMut) {}
            fn merge_field(
                &mut self,
                tag: u32,
                wire_type: ::prost::encoding::WireType,
                buf: &mut impl ::prost::bytes::Buf,
                ctx: ::prost::encoding::DecodeContext,
            ) -> Result<(), ::prost::DecodeError> {
                ::prost::encoding::skip_field(wire_type, tag, buf, ctx)
            }
            fn encoded_len(&self) -> usize {
                0
            }
            fn clear(&mut self) {
                *self = Self::default();
            }
        }
    };
}
pub(crate) use unencoded;

/// `SelectMergeStacktraces`' request.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SelectMergeStacktracesRequest {
    pub profile_type_id: String,
    pub label_selector: String,
    pub start: i64,
    pub end: i64,
    pub max_nodes: Option<i64>,
    pub format: i32,
    pub stack_trace_selector: Option<crate::types_v1::StackTraceSelector>,
    pub profile_id_selector: Vec<String>,
    pub trace_id_selector: Vec<String>,
    pub span_selector: Vec<String>,
}
unencoded!(SelectMergeStacktracesRequest);

/// `SelectMergeStacktraces`' response.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SelectMergeStacktracesResponse {
    pub flamegraph: Option<FlameGraph>,
}
unencoded!(SelectMergeStacktracesResponse);

/// A flame graph.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FlameGraph {
    pub names: Vec<String>,
    pub levels: Vec<Level>,
    pub total: i64,
    pub max_self: i64,
}
unencoded!(FlameGraph);

/// One level of a flame graph.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Level {
    pub values: Vec<i64>,
}
unencoded!(Level);

/// The answer's format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ProfileFormat {
    Unspecified = 0,
    Flamegraph = 1,
    Tree = 2,
    Dot = 3,
    Pprof = 4,
}
