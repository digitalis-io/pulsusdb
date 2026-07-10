//! Redacted secret newtype.
//!
//! docs/configuration.md documents two secret-shaped values: `auth_password`
//! and the password half of `clickhouse.auth`. Both must never surface in the
//! redacted config dump served by the `/config` endpoint (issue #6). Rather
//! than scrubbing secrets out of a dump at runtime, `Secret` makes redaction
//! a type-level property: `Debug` and `Serialize` always print `"***"`, and
//! the only way to read the real value is the explicit `.expose()` escape
//! hatch. A future field holding a secret string directly (not wrapped in
//! `Secret`) would be the only way to regress this — see `tests/redaction.rs`.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A secret string. Redacted in `Debug` and `Serialize`; only `.expose()`
/// yields the real value.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps `value` as a secret.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the real (unredacted) value. The only way to read a `Secret`.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// True if the underlying value is the empty string.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"***\"")
    }
}

impl Serialize for Secret {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("***")
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Secret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expose_returns_the_real_value() {
        let secret = Secret::new("hunter2");
        assert_eq!(secret.expose(), "hunter2");
    }

    #[test]
    fn is_empty_reflects_the_underlying_string() {
        assert!(Secret::new("").is_empty());
        assert!(!Secret::new("x").is_empty());
    }

    #[test]
    fn debug_is_always_redacted() {
        let secret = Secret::new("hunter2");
        assert_eq!(format!("{secret:?}"), "\"***\"");
    }

    #[test]
    fn serialize_is_always_redacted() {
        let secret = Secret::new("hunter2");
        let yaml = serde_norway::to_string(&secret).expect("serialize secret");
        assert!(yaml.contains("***"));
        assert!(!yaml.contains("hunter2"));
    }

    #[test]
    fn deserialize_is_transparent() {
        let secret: Secret = serde_norway::from_str("\"hunter2\"").expect("deserialize secret");
        assert_eq!(secret.expose(), "hunter2");
    }
}
