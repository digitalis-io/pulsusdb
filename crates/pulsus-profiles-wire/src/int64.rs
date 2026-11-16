//! The proto3 JSON form of an `int64` (issue #821): written as a decimal
//! string, read from a number or a decimal string.

use serde::Serializer;
use serde::de::{self, Deserializer, Visitor};

/// Writes `v` as a decimal string.
pub fn ser<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

/// Reads a JSON number or a decimal string as an `int64`.
pub fn de<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    struct V;
    impl Visitor<'_> for V {
        type Value = i64;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an int64 as a number or a decimal string")
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<i64, E> {
            Ok(v)
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<i64, E> {
            i64::try_from(v).map_err(|_| E::custom(format!("{v} is out of range for int64")))
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> Result<i64, E> {
            if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
                Ok(v as i64)
            } else {
                Err(E::custom(format!("{v} is not an int64")))
            }
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<i64, E> {
            v.parse()
                .map_err(|_| E::custom(format!("invalid value for int64 field: {v:?}")))
        }
    }
    d.deserialize_any(V)
}

/// `true` for the proto3 default, so the field is omitted.
pub fn is_zero(v: &i64) -> bool {
    *v == 0
}
