//! `push.v1`, the push route's request and response, with the field numbers
//! and types of `proto/push/v1/push.proto`. The `serde` forms are proto3
//! JSON (`codec.go:170-186` of the Connect library): each field under its
//! JSON name or its proto name, `bytes` as base64, unknown fields ignored.

use serde::Deserialize;

use crate::types_v1::{LabelPair, ProfileAnnotation};

/// The push route's answer: always empty.
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PushResponse {}

/// One push: its series, each with its raw pprof profiles.
#[derive(Clone, PartialEq, ::prost::Message, Deserialize)]
#[serde(default)]
pub struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    #[serde(deserialize_with = "crate::json::list")]
    pub series: Vec<RawProfileSeries>,
}

/// One series: its labels, its profiles and the client's annotations.
#[derive(Clone, PartialEq, ::prost::Message, Deserialize)]
#[serde(default)]
pub struct RawProfileSeries {
    #[prost(message, repeated, tag = "1")]
    #[serde(deserialize_with = "crate::json::list")]
    pub labels: Vec<LabelPair>,
    #[prost(message, repeated, tag = "2")]
    #[serde(deserialize_with = "crate::json::list")]
    pub samples: Vec<RawSample>,
    #[prost(message, repeated, tag = "3")]
    #[serde(deserialize_with = "crate::json::list")]
    pub annotations: Vec<ProfileAnnotation>,
}

/// One pprof profile, raw or gzipped, and the client's id for it.
#[derive(Clone, PartialEq, ::prost::Message, Deserialize)]
#[serde(default)]
pub struct RawSample {
    #[prost(bytes = "vec", tag = "1")]
    #[serde(
        rename = "rawProfile",
        alias = "raw_profile",
        deserialize_with = "crate::json::bytes"
    )]
    pub raw_profile: Vec<u8>,
    #[prost(string, tag = "2")]
    #[serde(rename = "ID", deserialize_with = "crate::json::string")]
    pub id: String,
}
