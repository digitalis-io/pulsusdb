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
