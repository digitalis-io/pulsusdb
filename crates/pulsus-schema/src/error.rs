//! `SchemaError` taxonomy for the schema controller.

use thiserror::Error;

use pulsus_clickhouse::ChError;

/// Errors from `pulsus-schema`. Every variant carries enough context that
/// `pulsus-server` can map it to a distinct process exit code and print an
/// actionable message.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Propagated from `pulsus-clickhouse` (connection, timeout, server, ...).
    #[error("clickhouse: {0}")]
    Clickhouse(#[from] ChError),

    /// The connected server's `SELECT version()` is older than the M0
    /// minimum (docs/schemas.md §8: ClickHouse 24.8 LTS).
    #[error(
        "unsupported ClickHouse version {found:?}: PulsusDB requires >= 24.8 \
         (docs/schemas.md §8 — projections + modern TTL/SimpleAggregateFunction behavior)"
    )]
    UnsupportedVersion { found: String },

    /// The server's reported version string could not be parsed at all.
    #[error("could not parse ClickHouse version string {0:?}")]
    Version(String),

    /// A previously-applied migration's id now renders to a different
    /// checksum than the one recorded in `schema_migrations` — the shipped
    /// template (or a config value it renders from) changed after it was
    /// already applied. Migrations are append-only and immutable; this is a
    /// hard error, never a silent re-apply (docs/schemas.md §6).
    #[error(
        "migration {id} drifted: the rendered DDL no longer matches the checksum recorded in \
         schema_migrations — migrations are immutable, ship the change as a new migration id"
    )]
    MigrationDrift { id: u32 },

    /// `--mode init` was requested together with `PULSUS_SKIP_DDL=1`: a
    /// contradictory intent (init exists to run DDL; skip exists to avoid
    /// it during normal startup). Refused rather than silently ignoring one
    /// of the two flags.
    #[error(
        "--mode init refuses to run with PULSUS_SKIP_DDL=1 (contradictory: init's purpose is to \
         apply DDL; unset PULSUS_SKIP_DDL or use a different mode)"
    )]
    SkipDdlInInit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_version_message_names_the_found_version() {
        let err = SchemaError::UnsupportedVersion {
            found: "24.3.2.1".to_string(),
        };
        assert!(err.to_string().contains("24.3.2.1"));
        assert!(err.to_string().contains("24.8"));
    }

    #[test]
    fn migration_drift_message_names_the_id() {
        let err = SchemaError::MigrationDrift { id: 5 };
        assert!(err.to_string().contains('5'));
    }

    #[test]
    fn skip_ddl_in_init_message_names_both_flags() {
        let err = SchemaError::SkipDdlInInit;
        assert!(err.to_string().contains("init"));
        assert!(err.to_string().contains("PULSUS_SKIP_DDL"));
    }

    #[test]
    fn clickhouse_error_converts_via_from() {
        let ch_err = ChError::Config("bad config".to_string());
        let err: SchemaError = ch_err.into();
        assert!(matches!(err, SchemaError::Clickhouse(_)));
    }
}
