//! Domain error type for `pulsus-config`. Every variant carries enough
//! context (offending file/variable/field name, expected format) that the
//! binary can print an actionable startup error and exit non-zero.

use thiserror::Error;

/// Errors from reading, parsing, or validating a [`crate::Config`].
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The YAML file at `path` could not be read from disk.
    #[error("failed to read config file {path}: {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },

    /// The YAML file at `path` did not parse (schema mismatch, syntax
    /// error, or an unknown key rejected by `deny_unknown_fields`).
    #[error("invalid YAML in {path}: {source}")]
    Yaml {
        path: String,
        source: serde_norway::Error,
    },

    /// An environment variable's value did not parse.
    #[error("{var}: {msg}")]
    Env { var: String, msg: String },

    /// A field's value failed a startup validation rule (not tied to a
    /// single environment variable, e.g. a `--mode` CLI override or a
    /// cross-field rule).
    #[error("invalid value for {field}: {msg} (expected {expected})")]
    Value {
        field: String,
        msg: String,
        expected: String,
    },

    /// A `downsampling.tiers` invariant (docs/configuration.md §7) was
    /// violated.
    #[error("invalid downsampling tiers: {0}")]
    Tier(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_error_message_names_the_variable() {
        let err = ConfigError::Env {
            var: "PULSUS_PORT".to_string(),
            msg: "not a number".to_string(),
        };
        assert_eq!(err.to_string(), "PULSUS_PORT: not a number");
    }

    #[test]
    fn value_error_message_includes_field_msg_and_expected() {
        let err = ConfigError::Value {
            field: "port".to_string(),
            msg: "must not be 0".to_string(),
            expected: "1-65535".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "invalid value for port: must not be 0 (expected 1-65535)"
        );
    }

    #[test]
    fn tier_error_message_wraps_the_reason() {
        let err = ConfigError::Tier("duplicate tier name \"5m\"".to_string());
        assert_eq!(
            err.to_string(),
            "invalid downsampling tiers: duplicate tier name \"5m\""
        );
    }
}
