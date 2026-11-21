//! Stub (tests first): `settings.v1`, the messages of `proto/settings/v1/setting.proto` (issue
//! #830): the exploration app's per-tenant settings, with the JSON mapping
//! of [`crate::types_v1`].

use serde::{Deserialize, Serialize};

/// One setting: its name, its value, and when it was written, in
/// milliseconds.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct Setting {
    #[prost(string, tag = "101")]
    pub name: String,
    #[prost(string, tag = "102")]
    pub value: String,
    #[prost(int64, tag = "103")]
    pub modified_at: i64,
}

/// `Get`'s request.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct GetSettingsRequest {}

/// `Get`'s response: the tenant's settings, in name order.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct GetSettingsResponse {
    #[prost(message, repeated, tag = "101")]
    pub settings: Vec<Setting>,
}

/// `Set`'s request.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SetSettingsRequest {
    #[prost(message, optional, tag = "101")]
    pub setting: Option<Setting>,
}

/// `Set`'s response: the setting as stored.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct SetSettingsResponse {
    #[prost(message, optional, tag = "101")]
    pub setting: Option<Setting>,
}

/// `Delete`'s request.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct DeleteSettingsRequest {
    #[prost(string, tag = "101")]
    pub name: String,
}

/// `Delete`'s response.
#[derive(Clone, PartialEq, Eq, ::prost::Message, Serialize, Deserialize)]
#[serde(default)]
pub struct DeleteSettingsResponse {}
