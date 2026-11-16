//! The proto3 JSON form of an `int64` (issue #821): written as a decimal
//! string, read from a number or a decimal string. Issue #823: the same for
//! a repeated and an optional `int64`.

use serde::de::{self, Deserializer, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serializer};

/// Writes `v` as a decimal string.
pub fn ser<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

/// Reads a JSON number or a decimal string as an `int64`.
pub fn de<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    d.deserialize_any(V)
}

/// One `int64`, as [`de`] reads it.
struct One(i64);

impl<'de> Deserialize<'de> for One {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        de(d).map(One)
    }
}

/// Writes each value as a decimal string.
pub fn ser_vec<S: Serializer>(v: &[i64], s: S) -> Result<S::Ok, S::Error> {
    let mut seq = s.serialize_seq(Some(v.len()))?;
    for x in v {
        seq.serialize_element(&x.to_string())?;
    }
    seq.end()
}

/// Reads an array of JSON numbers or decimal strings; `null` is empty.
pub fn de_vec<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<i64>, D::Error> {
    let v: Option<Vec<One>> = Option::deserialize(d)?;
    Ok(v.unwrap_or_default().into_iter().map(|One(x)| x).collect())
}

/// Writes a present value as a decimal string.
pub fn ser_opt<S: Serializer>(v: &Option<i64>, s: S) -> Result<S::Ok, S::Error> {
    match v {
        Some(x) => s.serialize_str(&x.to_string()),
        None => s.serialize_none(),
    }
}

/// Reads a JSON number or a decimal string as a present `int64`; `null` is
/// absent.
pub fn de_opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    let v: Option<One> = Option::deserialize(d)?;
    Ok(v.map(|One(x)| x))
}

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

/// `true` for the proto3 default, so the field is omitted.
pub fn is_zero(v: &i64) -> bool {
    *v == 0
}
