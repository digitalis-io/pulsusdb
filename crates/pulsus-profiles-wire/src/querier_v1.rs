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

/// `SelectMergeStacktraces`' request (issue #823). Tag 9, `async`, is
/// read and ignored.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SelectMergeStacktracesRequest {
    #[prost(string, tag = "1")]
    #[serde(
        rename = "profileTypeID",
        alias = "profile_typeID",
        skip_serializing_if = "String::is_empty"
    )]
    pub profile_type_id: String,
    #[prost(string, tag = "2")]
    #[serde(
        rename = "labelSelector",
        alias = "label_selector",
        skip_serializing_if = "String::is_empty"
    )]
    pub label_selector: String,
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
    #[prost(int64, optional, tag = "5")]
    #[serde(
        rename = "maxNodes",
        alias = "max_nodes",
        serialize_with = "crate::int64::ser_opt",
        deserialize_with = "crate::int64::de_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_nodes: Option<i64>,
    #[prost(enumeration = "ProfileFormat", tag = "6")]
    #[serde(
        serialize_with = "crate::profile_format::ser",
        deserialize_with = "crate::profile_format::de",
        skip_serializing_if = "crate::profile_format::is_unspecified"
    )]
    pub format: i32,
    #[prost(message, optional, tag = "7")]
    #[serde(
        rename = "stackTraceSelector",
        alias = "stack_trace_selector",
        skip_serializing_if = "Option::is_none"
    )]
    pub stack_trace_selector: Option<crate::types_v1::StackTraceSelector>,
    #[prost(string, repeated, tag = "8")]
    #[serde(
        rename = "profileIdSelector",
        alias = "profile_id_selector",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub profile_id_selector: Vec<String>,
    #[prost(string, repeated, tag = "10")]
    #[serde(
        rename = "traceIdSelector",
        alias = "trace_id_selector",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub trace_id_selector: Vec<String>,
    #[prost(string, repeated, tag = "11")]
    #[serde(
        rename = "spanSelector",
        alias = "span_selector",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub span_selector: Vec<String>,
}

/// `SelectMergeStacktraces`' response: the flame graph, field 1, and the
/// trimmed tree's bytes, field 2. The DOT and pprof fields are later
/// parts' (#828, #829).
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SelectMergeStacktracesResponse {
    #[prost(message, optional, tag = "1")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flamegraph: Option<FlameGraph>,
    #[prost(bytes = "vec", tag = "2")]
    #[serde(
        serialize_with = "crate::json::bytes_ser",
        deserialize_with = "crate::json::bytes",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub tree: Vec<u8>,
}

/// A flame graph: the names, and per level the nodes as
/// `[x offset delta, total, self, name index]`.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct FlameGraph {
    #[prost(string, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
    #[prost(message, repeated, tag = "2")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub levels: Vec<Level>,
    #[prost(int64, tag = "3")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub total: i64,
    #[prost(int64, tag = "4")]
    #[serde(
        rename = "maxSelf",
        alias = "max_self",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub max_self: i64,
}

/// One level of a flame graph.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Level {
    #[prost(int64, repeated, tag = "1")]
    #[serde(
        serialize_with = "crate::int64::ser_vec",
        deserialize_with = "crate::int64::de_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub values: Vec<i64>,
}

/// The format `SelectMergeStacktraces` answers in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ProfileFormat {
    Unspecified = 0,
    Flamegraph = 1,
    Tree = 2,
    Dot = 3,
    Pprof = 4,
}

impl ProfileFormat {
    /// The value's name in the proto.
    pub fn as_str_name(self) -> &'static str {
        match self {
            ProfileFormat::Unspecified => "PROFILE_FORMAT_UNSPECIFIED",
            ProfileFormat::Flamegraph => "PROFILE_FORMAT_FLAMEGRAPH",
            ProfileFormat::Tree => "PROFILE_FORMAT_TREE",
            ProfileFormat::Dot => "PROFILE_FORMAT_DOT",
            ProfileFormat::Pprof => "PROFILE_FORMAT_PPROF",
        }
    }

    /// The value a proto name names.
    pub fn from_str_name(name: &str) -> Option<Self> {
        [
            ProfileFormat::Unspecified,
            ProfileFormat::Flamegraph,
            ProfileFormat::Tree,
            ProfileFormat::Dot,
            ProfileFormat::Pprof,
        ]
        .into_iter()
        .find(|f| f.as_str_name() == name)
    }
}

/// `SelectSeries`' request (issue #824).
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SelectSeriesRequest {
    #[prost(string, tag = "1")]
    #[serde(
        rename = "profileTypeID",
        alias = "profile_typeID",
        skip_serializing_if = "String::is_empty"
    )]
    pub profile_type_id: String,
    #[prost(string, tag = "2")]
    #[serde(
        rename = "labelSelector",
        alias = "label_selector",
        skip_serializing_if = "String::is_empty"
    )]
    pub label_selector: String,
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
    #[prost(string, repeated, tag = "5")]
    #[serde(
        rename = "groupBy",
        alias = "group_by",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub group_by: Vec<String>,
    /// Seconds.
    #[prost(double, tag = "6")]
    #[serde(
        serialize_with = "crate::double::ser",
        deserialize_with = "crate::double::de",
        skip_serializing_if = "crate::double::is_zero"
    )]
    pub step: f64,
    #[prost(
        enumeration = "crate::types_v1::TimeSeriesAggregationType",
        optional,
        tag = "7"
    )]
    #[serde(
        serialize_with = "crate::aggregation_type::ser",
        deserialize_with = "crate::aggregation_type::de",
        skip_serializing_if = "Option::is_none"
    )]
    pub aggregation: Option<i32>,
    #[prost(message, optional, tag = "8")]
    #[serde(
        rename = "stackTraceSelector",
        alias = "stack_trace_selector",
        skip_serializing_if = "Option::is_none"
    )]
    pub stack_trace_selector: Option<crate::types_v1::StackTraceSelector>,
    #[prost(int64, optional, tag = "9")]
    #[serde(
        serialize_with = "crate::int64::ser_opt",
        deserialize_with = "crate::int64::de_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit: Option<i64>,
    #[prost(enumeration = "crate::types_v1::ExemplarType", tag = "10")]
    #[serde(
        rename = "exemplarType",
        alias = "exemplar_type",
        serialize_with = "crate::exemplar_type::ser",
        deserialize_with = "crate::exemplar_type::de",
        skip_serializing_if = "crate::exemplar_type::is_unspecified"
    )]
    pub exemplar_type: i32,
}

/// `SelectSeries`' response (issue #824).
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SelectSeriesResponse {
    #[prost(message, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub series: Vec<crate::types_v1::Series>,
}

/// `Diff`'s request (issue #827): the two selections. Each side's
/// `format` is ignored.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct DiffRequest {
    #[prost(message, optional, tag = "1")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left: Option<SelectMergeStacktracesRequest>,
    #[prost(message, optional, tag = "2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right: Option<SelectMergeStacktracesRequest>,
}

/// `Diff`'s response (issue #827): the diff flame graph, field 1.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct DiffResponse {
    #[prost(message, optional, tag = "1")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flamegraph: Option<FlameGraphDiff>,
}

/// A diff flame graph (issue #827): the names, and per level the node pairs
/// as `[left x offset, left total, left self, right x offset, right total,
/// right self, name index]`, both x offsets delta-encoded; each side's
/// ticks.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct FlameGraphDiff {
    #[prost(string, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
    #[prost(message, repeated, tag = "2")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub levels: Vec<Level>,
    #[prost(int64, tag = "3")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub total: i64,
    #[prost(int64, tag = "4")]
    #[serde(
        rename = "maxSelf",
        alias = "max_self",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub max_self: i64,
    #[prost(int64, tag = "5")]
    #[serde(
        rename = "leftTicks",
        alias = "left_ticks",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub left_ticks: i64,
    #[prost(int64, tag = "6")]
    #[serde(
        rename = "rightTicks",
        alias = "right_ticks",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub right_ticks: i64,
}

/// Issue #825: what a heatmap counts — each profile's total, or each
/// span's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum HeatmapQueryType {
    Unspecified = 0,
    Individual = 1,
    Span = 2,
}

impl HeatmapQueryType {
    /// The value's name in the proto.
    pub fn as_str_name(self) -> &'static str {
        match self {
            HeatmapQueryType::Unspecified => "HEATMAP_QUERY_TYPE_UNSPECIFIED",
            HeatmapQueryType::Individual => "HEATMAP_QUERY_TYPE_INDIVIDUAL",
            HeatmapQueryType::Span => "HEATMAP_QUERY_TYPE_SPAN",
        }
    }

    /// The value a proto name names.
    pub fn from_str_name(name: &str) -> Option<Self> {
        [
            HeatmapQueryType::Unspecified,
            HeatmapQueryType::Individual,
            HeatmapQueryType::Span,
        ]
        .into_iter()
        .find(|t| t.as_str_name() == name)
    }
}

/// `SelectHeatmap`'s request (issue #825).
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SelectHeatmapRequest {
    #[prost(string, tag = "1")]
    #[serde(
        rename = "profileTypeID",
        alias = "profile_typeID",
        skip_serializing_if = "String::is_empty"
    )]
    pub profile_type_id: String,
    #[prost(string, tag = "2")]
    #[serde(
        rename = "labelSelector",
        alias = "label_selector",
        skip_serializing_if = "String::is_empty"
    )]
    pub label_selector: String,
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
    /// Seconds.
    #[prost(double, tag = "5")]
    #[serde(
        serialize_with = "crate::double::ser",
        deserialize_with = "crate::double::de",
        skip_serializing_if = "crate::double::is_zero"
    )]
    pub step: f64,
    #[prost(string, repeated, tag = "6")]
    #[serde(
        rename = "groupBy",
        alias = "group_by",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub group_by: Vec<String>,
    #[prost(enumeration = "HeatmapQueryType", tag = "7")]
    #[serde(
        rename = "queryType",
        alias = "query_type",
        serialize_with = "crate::heatmap_query_type::ser",
        deserialize_with = "crate::heatmap_query_type::de",
        skip_serializing_if = "crate::heatmap_query_type::is_unspecified"
    )]
    pub query_type: i32,
    #[prost(enumeration = "crate::types_v1::ExemplarType", tag = "8")]
    #[serde(
        rename = "exemplarType",
        alias = "exemplar_type",
        serialize_with = "crate::exemplar_type::ser",
        deserialize_with = "crate::exemplar_type::de",
        skip_serializing_if = "crate::exemplar_type::is_unspecified"
    )]
    pub exemplar_type: i32,
    #[prost(int64, optional, tag = "9")]
    #[serde(
        serialize_with = "crate::int64::ser_opt",
        deserialize_with = "crate::int64::de_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit: Option<i64>,
}

/// `SelectHeatmap`'s response (issue #825).
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SelectHeatmapResponse {
    #[prost(message, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub series: Vec<crate::types_v1::HeatmapSeries>,
}
