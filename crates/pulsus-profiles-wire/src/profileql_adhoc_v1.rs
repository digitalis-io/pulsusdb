//! `adhocprofiles.v1`, the messages of
//! `proto/profileql_adhoc/v1/profileql_adhoc.proto` (issue #831): the
//! exploration app's ad hoc profile uploads, with the JSON mapping of
//! [`crate::types_v1`]. Each multi-word field is also read under its proto
//! name.

use serde::{Deserialize, Serialize};

/// `Upload`'s request: the file's name, the file in base64, and the
/// flame graph's node limit.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct AdHocProfilesUploadRequest {
    #[prost(string, tag = "1")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[prost(string, tag = "2")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub profile: String,
    #[prost(int64, optional, tag = "3")]
    #[serde(
        rename = "maxNodes",
        alias = "max_nodes",
        serialize_with = "crate::int64::ser_opt",
        deserialize_with = "crate::int64::de_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_nodes: Option<i64>,
}

/// `Get`'s request: the upload's id, the type to draw, the node limit.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct AdHocProfilesGetRequest {
    #[prost(string, tag = "1")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[prost(string, optional, tag = "2")]
    #[serde(
        rename = "profileType",
        alias = "profile_type",
        skip_serializing_if = "Option::is_none"
    )]
    pub profile_type: Option<String>,
    #[prost(int64, optional, tag = "3")]
    #[serde(
        rename = "maxNodes",
        alias = "max_nodes",
        serialize_with = "crate::int64::ser_opt",
        deserialize_with = "crate::int64::de_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_nodes: Option<i64>,
}

/// `Upload`'s and `Get`'s answer: the upload, the type drawn, every type,
/// and the flame graph as the flamebearer's JSON text.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct AdHocProfilesGetResponse {
    #[prost(string, tag = "1")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[prost(string, tag = "2")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Milliseconds.
    #[prost(int64, tag = "3")]
    #[serde(
        rename = "uploadedAt",
        alias = "uploaded_at",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub uploaded_at: i64,
    #[prost(string, tag = "4")]
    #[serde(
        rename = "profileType",
        alias = "profile_type",
        skip_serializing_if = "String::is_empty"
    )]
    pub profile_type: String,
    #[prost(string, repeated, tag = "5")]
    #[serde(
        rename = "profileTypes",
        alias = "profile_types",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub profile_types: Vec<String>,
    #[prost(string, tag = "6")]
    #[serde(
        rename = "flamebearerProfile",
        alias = "flamebearer_profile",
        skip_serializing_if = "String::is_empty"
    )]
    pub flamebearer_profile: String,
}

/// `List`'s request.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct AdHocProfilesListRequest {}

/// `List`'s answer: the tenant's uploads, newest first.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct AdHocProfilesListResponse {
    #[prost(message, repeated, tag = "1")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<AdHocProfilesProfileMetadata>,
}

/// One listed upload.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct AdHocProfilesProfileMetadata {
    #[prost(string, tag = "1")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[prost(string, tag = "2")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Milliseconds.
    #[prost(int64, tag = "3")]
    #[serde(
        rename = "uploadedAt",
        alias = "uploaded_at",
        serialize_with = "crate::int64::ser",
        deserialize_with = "crate::int64::de",
        skip_serializing_if = "crate::int64::is_zero"
    )]
    pub uploaded_at: i64,
}

/// `Diff`'s request: the two uploads, the type, the node limit.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct AdHocProfilesDiffRequest {
    #[prost(string, tag = "1")]
    #[serde(
        rename = "leftId",
        alias = "left_id",
        skip_serializing_if = "String::is_empty"
    )]
    pub left_id: String,
    #[prost(string, tag = "2")]
    #[serde(
        rename = "rightId",
        alias = "right_id",
        skip_serializing_if = "String::is_empty"
    )]
    pub right_id: String,
    #[prost(string, optional, tag = "3")]
    #[serde(
        rename = "profileType",
        alias = "profile_type",
        skip_serializing_if = "Option::is_none"
    )]
    pub profile_type: Option<String>,
    #[prost(int64, optional, tag = "4")]
    #[serde(
        rename = "maxNodes",
        alias = "max_nodes",
        serialize_with = "crate::int64::ser_opt",
        deserialize_with = "crate::int64::de_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_nodes: Option<i64>,
}

/// `Diff`'s answer: the types both uploads hold, and the diff as the
/// flamebearer's JSON text.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct AdHocProfilesDiffResponse {
    #[prost(string, repeated, tag = "1")]
    #[serde(
        rename = "profileTypes",
        alias = "profile_types",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub profile_types: Vec<String>,
    #[prost(string, tag = "2")]
    #[serde(
        rename = "flamebearerProfile",
        alias = "flamebearer_profile",
        skip_serializing_if = "String::is_empty"
    )]
    pub flamebearer_profile: String,
}
