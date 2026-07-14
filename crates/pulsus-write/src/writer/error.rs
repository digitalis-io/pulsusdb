//! `WriteError`: the taxonomy of terminal outcomes a flush generation can
//! settle with (issue #9 architect plan, mirrors
//! `pulsus_clickhouse::ChError`'s style — one variant per distinguishable,
//! actionable case). Every `FlushWait`/join future a sync-mode caller
//! awaits resolves to one of these internally; `writer::mod` maps it to
//! `crate::error::LogsIngestError::FlushFailed` at the issue #8 seam
//! boundary (that crate's fixed `FlushWait` error type).

use thiserror::Error;

/// A flush generation's terminal, non-success outcome.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// The writer was shutting down before this generation's batch could
    /// be confirmed durable — either admission was rejected outright
    /// (architect plan amendment 2, phase 1: "stop admitting") or the
    /// drain deadline expired while this generation was still open/
    /// in-flight (phase 2: forced settlement).
    #[error("writer is shutting down")]
    ShuttingDown,
    /// The insert failed with a non-retryable error, or a retryable one
    /// whose retry budget was exhausted; the batch was spooled to
    /// `{spool_dir}/poison/{table}/` and dropped (never auto-replayed).
    #[error("insert failed and the batch was spooled to poison: {0}")]
    Poisoned(String),
    /// `pulsus_clickhouse::ChError::InsertUncertain`: the insert's commit
    /// fate is unknown (the server may have partially applied it). The
    /// batch was spooled to `{spool_dir}/uncertain/{table}/` for manual
    /// audit and is NEVER auto-replayed — replaying a partially-committed
    /// block would duplicate rows and permanently inflate
    /// materialized-view aggregates (docs/schemas.md §2.2/§8, the one
    /// hard invariant this crate enforces).
    #[error("insert commit fate is unknown; the batch was spooled for manual audit: {0}")]
    Uncertain(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutting_down_message_is_stable() {
        assert_eq!(
            WriteError::ShuttingDown.to_string(),
            "writer is shutting down"
        );
    }

    #[test]
    fn poisoned_message_names_the_reason() {
        let err = WriteError::Poisoned("bad SQL".to_string());
        assert!(err.to_string().contains("bad SQL"));
    }

    #[test]
    fn uncertain_message_names_the_reason() {
        let err = WriteError::Uncertain("timed out".to_string());
        assert!(err.to_string().contains("timed out"));
    }

    #[test]
    fn write_error_is_clone_and_eq() {
        // Load-bearing: `Generation::settle` clones the terminal result to
        // send it to every joined waiter (a generation can have many).
        let a = WriteError::Poisoned("x".to_string());
        let b = a.clone();
        assert_eq!(a, b);
    }
}
