//! Issue #828: the proto3 JSON form of a `uint64`, as [`crate::int64`]
//! writes an `int64`: written as a decimal string, read from a number or a
//! decimal string.

use serde::de::{self, Deserializer, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serializer};

/// Writes `v` as a decimal string.
pub fn ser<S: Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

/// Reads a JSON number or a decimal string as a `uint64`.
pub fn de<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    d.deserialize_any(V)
}

/// One `uint64`, as [`de`] reads it.
struct One(u64);

impl<'de> Deserialize<'de> for One {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        de(d).map(One)
    }
}

/// Writes each value as a decimal string.
pub fn ser_vec<S: Serializer>(v: &[u64], s: S) -> Result<S::Ok, S::Error> {
    let mut seq = s.serialize_seq(Some(v.len()))?;
    for x in v {
        seq.serialize_element(&x.to_string())?;
    }
    seq.end()
}

/// Reads an array of JSON numbers or decimal strings; `null` is empty.
pub fn de_vec<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u64>, D::Error> {
    let v: Option<Vec<One>> = Option::deserialize(d)?;
    Ok(v.unwrap_or_default().into_iter().map(|One(x)| x).collect())
}

/// `true` for the proto3 default, so the field is omitted.
pub fn is_zero(v: &u64) -> bool {
    *v == 0
}

struct V;
impl Visitor<'_> for V {
    type Value = u64;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a uint64 as a number or a decimal string")
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<u64, E> {
        u64::try_from(v).map_err(|_| E::custom(format!("{v} is out of range for uint64")))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<u64, E> {
        Ok(v)
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<u64, E> {
        if v.fract() == 0.0 && v >= 0.0 && v <= u64::MAX as f64 {
            Ok(v as u64)
        } else {
            Err(E::custom(format!("{v} is not a uint64")))
        }
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<u64, E> {
        v.parse()
            .map_err(|_| E::custom(format!("invalid value for uint64 field: {v:?}")))
    }
}
