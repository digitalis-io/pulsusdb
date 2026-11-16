//! `push.v1`, the push route's request and response, with the field numbers
//! and types of `proto/push/v1/push.proto`.

use crate::types_v1::{LabelPair, ProfileAnnotation};

/// The push route's answer: always empty.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct PushResponse {}

/// One push: its series, each with its raw pprof profiles.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct PushRequest {
    pub series: Vec<RawProfileSeries>,
}

/// One series: its labels, its profiles and the client's annotations.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct RawProfileSeries {
    pub labels: Vec<LabelPair>,
    pub samples: Vec<RawSample>,
    pub annotations: Vec<ProfileAnnotation>,
}

/// One pprof profile, raw or gzipped, and the client's id for it.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct RawSample {
    pub raw_profile: Vec<u8>,
    pub id: String,
}

crate::stub_message!(PushResponse);
crate::stub_message!(PushRequest);
crate::stub_message!(RawProfileSeries);
crate::stub_message!(RawSample);
