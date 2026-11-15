//! The profiles wire messages, written by hand as `prost` structs so no
//! code generator runs at build time. `google_v1` is the pprof profile
//! message (`proto/google/v1/profile.proto`, kept verbatim as the record the
//! field numbers are checked against by `tests/tags.rs`).

pub mod google_v1;
