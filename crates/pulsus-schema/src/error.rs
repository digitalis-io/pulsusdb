//! `SchemaError` taxonomy for the server checks and the trace replay.

use thiserror::Error;

use pulsus_clickhouse::ChError;

/// Errors from `pulsus-schema`. Every variant carries enough context for
/// `pulsus-server` to print an actionable message before it refuses to
/// start.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Propagated from `pulsus-clickhouse` (connection, timeout, server, ...).
    #[error("clickhouse: {0}")]
    Clickhouse(#[from] ChError),

    /// The connected server's `SELECT version()` is older than the M0
    /// minimum (docs/schemas.md §8: ClickHouse 24.8 LTS).
    #[error(
        "unsupported ClickHouse version {found:?}: PulsusDB requires >= 26.3 \
         (docs/schemas.md §8 — the supported LTS line; older servers do not \
         tag an HTTP-200 mid-stream exception, so it cannot be told apart \
         from result text — issue #412)"
    )]
    UnsupportedVersion { found: String },

    /// The server's reported version string could not be parsed at all.
    #[error("could not parse ClickHouse version string {0:?}")]
    Version(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_version_message_names_the_found_version() {
        let err = SchemaError::UnsupportedVersion {
            found: "24.8.14.39".to_string(),
        };
        assert!(err.to_string().contains("24.8.14.39"));
        assert!(err.to_string().contains("26.3"));
    }

    #[test]
    fn clickhouse_error_converts_via_from() {
        let ch_err = ChError::Config("bad config".to_string());
        let err: SchemaError = ch_err.into();
        assert!(matches!(err, SchemaError::Clickhouse(_)));
    }
}
