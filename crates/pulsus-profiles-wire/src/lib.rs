//! The profiles wire messages, written by hand as `prost` structs so no
//! code generator runs at build time. `google_v1` is the pprof profile
//! message (`proto/google/v1/profile.proto`, kept verbatim as the record the
//! field numbers are checked against by `tests/tags.rs`). `types_v1` and
//! `querier_v1` are the query API's messages (issue #821,
//! `proto/types/v1/types.proto` and `proto/querier/v1/querier.proto`), with
//! the proto3 JSON mapping as `serde` attributes; `push_v1` is the push
//! route's (`proto/push/v1/push.proto`, kept verbatim for the same reason).
//! `connect` is the Connect protocol's error rule.

pub mod connect;
pub mod google_v1;
pub mod int64;
mod json;
pub mod push_v1;
pub mod querier_v1;
pub mod types_v1;
