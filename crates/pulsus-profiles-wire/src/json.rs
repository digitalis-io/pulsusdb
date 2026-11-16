//! proto3 JSON's field rules for the push messages: `null` is the field's
//! default, and `bytes` is base64, standard or URL-safe, padded or not.

use base64::Engine;
use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use serde::{Deserialize, Deserializer};

pub(crate) fn string<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(Option::<String>::deserialize(d)?.unwrap_or_default())
}

pub(crate) fn list<'de, D, T>(d: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(d)?.unwrap_or_default())
}

pub(crate) fn bytes<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    let Some(text) = Option::<String>::deserialize(d)? else {
        return Ok(Vec::new());
    };
    let text = text.trim_end_matches('=');
    let engine = if text.contains(['-', '_']) {
        URL_SAFE_NO_PAD
    } else {
        STANDARD_NO_PAD
    };
    engine.decode(text).map_err(serde::de::Error::custom)
}
