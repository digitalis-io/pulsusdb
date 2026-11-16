//! Stub.

use serde::{Deserialize, Serialize};

macro_rules! stub_codec {
    ($($t:ty),*) => {$(
        impl $t {
            pub fn encode_to_vec(&self) -> Vec<u8> { Vec::new() }
            pub fn decode(_: &[u8]) -> Result<Self, prost::DecodeError> { Ok(Self::default()) }
        }
    )*};
}
pub(crate) use stub_codec;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelPair {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProfileType {
    pub id: String,
    pub name: String,
    pub sample_type: String,
    pub sample_unit: String,
    pub period_type: String,
    pub period_unit: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Labels {
    pub labels: Vec<LabelPair>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelNamesRequest {
    pub matchers: Vec<String>,
    pub start: i64,
    pub end: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelNamesResponse {
    pub names: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelValuesRequest {
    pub name: String,
    pub matchers: Vec<String>,
    pub start: i64,
    pub end: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelValuesResponse {
    pub names: Vec<String>,
}

stub_codec!(
    LabelPair,
    ProfileType,
    Labels,
    LabelNamesRequest,
    LabelNamesResponse,
    LabelValuesRequest,
    LabelValuesResponse
);
