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

/// Issue #823: the proto3 JSON form of `querier.v1.ProfileFormat` — written
/// as its name, read from its name or its number.
pub mod profile_format {
    use serde::Serializer;
    use serde::de::{self, Deserializer, Visitor};

    use crate::querier_v1::ProfileFormat;

    /// Writes a known value as its name, another as its number.
    pub fn ser<S: Serializer>(v: &i32, s: S) -> Result<S::Ok, S::Error> {
        match ProfileFormat::try_from(*v) {
            Ok(f) => s.serialize_str(f.as_str_name()),
            Err(_) => s.serialize_i32(*v),
        }
    }

    /// Reads a value's name or its number; an unknown name is refused.
    pub fn de<'de, D: Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = i32;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a ProfileFormat name or number")
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<i32, E> {
                i32::try_from(v).map_err(|_| E::custom(format!("{v} is out of range for an enum")))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<i32, E> {
                i32::try_from(v).map_err(|_| E::custom(format!("{v} is out of range for an enum")))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<i32, E> {
                ProfileFormat::from_str_name(v)
                    .map(|f| f as i32)
                    .ok_or_else(|| E::custom(format!("invalid value for enum field format: {v:?}")))
            }
            fn visit_unit<E: de::Error>(self) -> Result<i32, E> {
                Ok(0)
            }
        }
        d.deserialize_any(V)
    }

    /// `true` for the proto3 default, so the field is omitted.
    pub fn is_unspecified(v: &i32) -> bool {
        *v == 0
    }
}
