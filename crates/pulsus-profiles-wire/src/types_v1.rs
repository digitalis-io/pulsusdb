//! `types.v1`, the messages of `proto/types/v1/types.proto` the profiles
//! query API uses (issue #821), with the proto3 JSON mapping: lowerCamelCase
//! names out, either name in, a default omitted, an `int64` as a string.

use serde::{Deserialize, Serialize};

/// A label name and value.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct LabelPair {
    #[prost(string, tag = "1")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[prost(string, tag = "2")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub value: String,
}

/// A profile type id and its five parts. No tag 3.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfileType {
    #[prost(string, tag = "1")]
    #[serde(rename = "ID", skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[prost(string, tag = "2")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[prost(string, tag = "4")]
    #[serde(
        rename = "sampleType",
        alias = "sample_type",
        skip_serializing_if = "String::is_empty"
    )]
    pub sample_type: String,
    #[prost(string, tag = "5")]
    #[serde(
        rename = "sampleUnit",
        alias = "sample_unit",
        skip_serializing_if = "String::is_empty"
    )]
    pub sample_unit: String,
    #[prost(string, tag = "6")]
    #[serde(
        rename = "periodType",
        alias = "period_type",
        skip_serializing_if = "String::is_empty"
    )]
    pub period_type: String,
    #[prost(string, tag = "7")]
    #[serde(
        rename = "periodUnit",
        alias = "period_unit",
        skip_serializing_if = "String::is_empty"
    )]
    pub period_unit: String,
}

/// One label set.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Labels {
    #[prost(message, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<LabelPair>,
}

/// `LabelNames`' request.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct LabelNamesRequest {
    #[prost(string, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub matchers: Vec<String>,
    #[prost(int64, tag = "2")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub start: i64,
    #[prost(int64, tag = "3")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub end: i64,
}

/// `LabelNames`' response.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct LabelNamesResponse {
    #[prost(string, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
}

/// `LabelValues`' request.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct LabelValuesRequest {
    #[prost(string, tag = "1")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[prost(string, repeated, tag = "2")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub matchers: Vec<String>,
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

/// `LabelValues`' response.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct LabelValuesResponse {
    #[prost(string, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
}

/// Issue #823: the samples a merge keeps by their stack. Root at
/// `call_site[0]`.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct StackTraceSelector {
    #[prost(message, repeated, tag = "1")]
    #[serde(
        rename = "callSite",
        alias = "call_site",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub call_site: Vec<Location>,
    #[prost(message, optional, tag = "2")]
    #[serde(
        rename = "goPgo",
        alias = "go_pgo",
        skip_serializing_if = "Option::is_none"
    )]
    pub go_pgo: Option<GoPgo>,
}

/// One frame of a call site.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Location {
    #[prost(string, tag = "1")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
}

/// `GoPGO`: read, and not served yet (#828).
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct GoPgo {
    #[prost(uint32, tag = "1")]
    #[serde(
        rename = "keepLocations",
        alias = "keep_locations",
        skip_serializing_if = "is_zero_u32"
    )]
    pub keep_locations: u32,
    #[prost(bool, tag = "2")]
    #[serde(
        rename = "aggregateCallees",
        alias = "aggregate_callees",
        skip_serializing_if = "is_false"
    )]
    pub aggregate_callees: bool,
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// A profile annotation (issue #817). The push route drops the client's.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfileAnnotation {
    #[prost(string, tag = "1")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub key: String,
    #[prost(string, tag = "2")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub value: String,
}

/// Issue #824: one time series — its labels and its points.
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Series {
    #[prost(message, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<LabelPair>,
    #[prost(message, repeated, tag = "2")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub points: Vec<Point>,
}

/// Issue #824: one point of a time series. Tag 4, exemplars, is #826's;
/// `annotations` is always empty: no ingest path stores one.
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Point {
    #[prost(double, tag = "1")]
    #[serde(
        serialize_with = "crate::double::ser",
        deserialize_with = "crate::double::de",
        skip_serializing_if = "crate::double::is_zero"
    )]
    pub value: f64,
    #[prost(int64, tag = "2")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub timestamp: i64,
    #[prost(message, repeated, tag = "3")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub annotations: Vec<ProfileAnnotation>,
}

/// Issue #824: how a time series' step combines its profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeSeriesAggregationType {
    Sum = 0,
    Average = 1,
}

impl TimeSeriesAggregationType {
    /// The value's name in the proto.
    pub fn as_str_name(self) -> &'static str {
        match self {
            TimeSeriesAggregationType::Sum => "TIME_SERIES_AGGREGATION_TYPE_SUM",
            TimeSeriesAggregationType::Average => "TIME_SERIES_AGGREGATION_TYPE_AVERAGE",
        }
    }

    /// The value a proto name names.
    pub fn from_str_name(name: &str) -> Option<Self> {
        [
            TimeSeriesAggregationType::Sum,
            TimeSeriesAggregationType::Average,
        ]
        .into_iter()
        .find(|t| t.as_str_name() == name)
    }
}

/// Issue #824: the exemplars a time series request asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ExemplarType {
    Unspecified = 0,
    None = 1,
    Individual = 2,
    Span = 3,
}

impl ExemplarType {
    /// The value's name in the proto.
    pub fn as_str_name(self) -> &'static str {
        match self {
            ExemplarType::Unspecified => "EXEMPLAR_TYPE_UNSPECIFIED",
            ExemplarType::None => "EXEMPLAR_TYPE_NONE",
            ExemplarType::Individual => "EXEMPLAR_TYPE_INDIVIDUAL",
            ExemplarType::Span => "EXEMPLAR_TYPE_SPAN",
        }
    }

    /// The value a proto name names.
    pub fn from_str_name(name: &str) -> Option<Self> {
        [
            ExemplarType::Unspecified,
            ExemplarType::None,
            ExemplarType::Individual,
            ExemplarType::Span,
        ]
        .into_iter()
        .find(|t| t.as_str_name() == name)
    }
}

/// Issue #825: one heatmap series — its labels and its slots.
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct HeatmapSeries {
    #[prost(message, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<LabelPair>,
    #[prost(message, repeated, tag = "2")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub slots: Vec<HeatmapSlot>,
}

/// Issue #825: one slot of a heatmap series — its right edge, the 20
/// bucket starts and each bucket's count. Tag 4, exemplars, is #826's.
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct HeatmapSlot {
    #[prost(int64, tag = "1")]
    #[serde(
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub timestamp: i64,
    #[prost(double, repeated, tag = "2")]
    #[serde(
        rename = "yMin",
        alias = "y_min",
        serialize_with = "crate::double::ser_vec",
        deserialize_with = "crate::double::de_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub y_min: Vec<f64>,
    #[prost(int32, repeated, tag = "3")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub counts: Vec<i32>,
}
