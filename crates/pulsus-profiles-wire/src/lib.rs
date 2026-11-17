//! The profiles wire messages, written by hand as `prost` structs so no
//! code generator runs at build time. `google_v1` is the pprof profile
//! message (`proto/google/v1/profile.proto`, kept verbatim as the record the
//! field numbers are checked against by `tests/tags.rs`). `types_v1` and
//! `querier_v1` are the query API's messages (issue #821,
//! `proto/types/v1/types.proto` and `proto/querier/v1/querier.proto`), with
//! the proto3 JSON mapping as `serde` attributes; `push_v1` is the push
//! route's (`proto/push/v1/push.proto`, kept verbatim for the same reason).
//! `connect` is the Connect protocol's error rule.
//!
//! Issue #824: `double` is a `double` field's JSON form, and
//! `aggregation_type` and `exemplar_type` are the time series request's
//! two enums, in the shape of `profile_format`.

/// Tests-first stub: a message that encodes nothing.
macro_rules! empty_message {
    ($t:ty) => {
        impl ::prost::Message for $t {
            fn encode_raw(&self, _buf: &mut impl ::prost::bytes::BufMut) {}
            fn merge_field(
                &mut self,
                tag: u32,
                wire_type: ::prost::encoding::WireType,
                buf: &mut impl ::prost::bytes::Buf,
                ctx: ::prost::encoding::DecodeContext,
            ) -> Result<(), ::prost::DecodeError> {
                ::prost::encoding::skip_field(wire_type, tag, buf, ctx)
            }
            fn encoded_len(&self) -> usize {
                0
            }
            fn clear(&mut self) {
                *self = Self::default();
            }
        }
    };
}

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

/// Issue #824: the proto3 JSON form of a `double`. An integral value within
/// `int64` is written as an integer, as the reference writes it
/// (`28885319484`, not `28885319484.0`); any other as a JSON number. Read
/// from a number or a numeric string.
pub mod double {
    use serde::Serializer;
    use serde::de::{self, Deserializer, Visitor};

    /// Writes `v`: an integer when it is one within `int64`.
    pub fn ser<S: Serializer>(v: &f64, s: S) -> Result<S::Ok, S::Error> {
        // 2^63 is the first value above `int64`; every integral `f64`
        // below it in magnitude converts exactly.
        const LIMIT: f64 = 9_223_372_036_854_775_808.0;
        if v.fract() == 0.0 && *v > -LIMIT && *v < LIMIT {
            s.serialize_i64(*v as i64)
        } else {
            s.serialize_f64(*v)
        }
    }

    /// Reads a JSON number or a numeric string.
    pub fn de<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = f64;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a double as a number or a numeric string")
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<f64, E> {
                Ok(v as f64)
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<f64, E> {
                Ok(v as f64)
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<f64, E> {
                Ok(v)
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<f64, E> {
                v.trim()
                    .parse()
                    .map_err(|_| E::custom(format!("invalid value for double field: {v:?}")))
            }
            fn visit_unit<E: de::Error>(self) -> Result<f64, E> {
                Ok(0.0)
            }
        }
        d.deserialize_any(V)
    }

    /// `true` for the proto3 default, so the field is omitted.
    pub fn is_zero(v: &f64) -> bool {
        v.to_bits() == 0
    }
}

/// Reads an enum's name or its number; `name` maps a name to its number.
fn enum_visitor<'de, D: serde::de::Deserializer<'de>>(
    d: D,
    what: &'static str,
    name: fn(&str) -> Option<i32>,
) -> Result<Option<i32>, D::Error> {
    use serde::de::{self, Visitor};
    struct V {
        what: &'static str,
        name: fn(&str) -> Option<i32>,
    }
    impl Visitor<'_> for V {
        type Value = Option<i32>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "a {} name or number", self.what)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Option<i32>, E> {
            i32::try_from(v)
                .map(Some)
                .map_err(|_| E::custom(format!("{v} is out of range for an enum")))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Option<i32>, E> {
            i32::try_from(v)
                .map(Some)
                .map_err(|_| E::custom(format!("{v} is out of range for an enum")))
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Option<i32>, E> {
            (self.name)(v).map(Some).ok_or_else(|| {
                E::custom(format!("invalid value for enum field {}: {v:?}", self.what))
            })
        }
        fn visit_unit<E: de::Error>(self) -> Result<Option<i32>, E> {
            Ok(None)
        }
    }
    d.deserialize_any(V { what, name })
}

/// Issue #824: the proto3 JSON form of the `optional`
/// `types.v1.TimeSeriesAggregationType` — written as its name, read from
/// its name or its number.
pub mod aggregation_type {
    use serde::Serializer;
    use serde::de::Deserializer;

    use crate::types_v1::TimeSeriesAggregationType;

    /// Writes a known value as its name, another as its number.
    pub fn ser<S: Serializer>(v: &Option<i32>, s: S) -> Result<S::Ok, S::Error> {
        match v.map(TimeSeriesAggregationType::try_from) {
            None => s.serialize_none(),
            Some(Ok(t)) => s.serialize_str(t.as_str_name()),
            Some(Err(_)) => s.serialize_i32(v.unwrap_or_default()),
        }
    }

    /// Reads a value's name or its number; `null` is absent; an unknown
    /// name is refused.
    pub fn de<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i32>, D::Error> {
        super::enum_visitor(d, "aggregation", |n| {
            TimeSeriesAggregationType::from_str_name(n).map(|t| t as i32)
        })
    }
}

/// Issue #824: the proto3 JSON form of `types.v1.ExemplarType` — written
/// as its name, read from its name or its number.
pub mod exemplar_type {
    use serde::Serializer;
    use serde::de::Deserializer;

    use crate::types_v1::ExemplarType;

    /// Writes a known value as its name, another as its number.
    pub fn ser<S: Serializer>(v: &i32, s: S) -> Result<S::Ok, S::Error> {
        match ExemplarType::try_from(*v) {
            Ok(t) => s.serialize_str(t.as_str_name()),
            Err(_) => s.serialize_i32(*v),
        }
    }

    /// Reads a value's name or its number; an unknown name is refused.
    pub fn de<'de, D: Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
        super::enum_visitor(d, "exemplar_type", |n| {
            ExemplarType::from_str_name(n).map(|t| t as i32)
        })
        .map(Option::unwrap_or_default)
    }

    /// `true` for the proto3 default, so the field is omitted.
    pub fn is_unspecified(v: &i32) -> bool {
        *v == 0
    }
}
