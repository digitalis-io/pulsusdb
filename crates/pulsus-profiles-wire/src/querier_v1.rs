//! Stub.

use serde::{Deserialize, Serialize};

use crate::types_v1::{Labels, ProfileType, stub_codec};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProfileTypesRequest {
    pub start: i64,
    pub end: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProfileTypesResponse {
    pub profile_types: Vec<ProfileType>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SeriesRequest {
    pub matchers: Vec<String>,
    pub label_names: Vec<String>,
    pub start: i64,
    pub end: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SeriesResponse {
    pub labels_set: Vec<Labels>,
}

stub_codec!(
    ProfileTypesRequest,
    ProfileTypesResponse,
    SeriesRequest,
    SeriesResponse
);
