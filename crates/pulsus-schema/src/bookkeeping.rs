//! Reads and writes for the two bookkeeping tables (docs/schemas.md §6).
//!
//! Both tables are `ReplacingMergeTree` — applied-id/mv-name reads must use
//! `FINAL` or duplicate rows (from a retried idempotent `execute`, or the
//! natural at-least-once nature of `ReplacingMergeTree` merges) misreport
//! applied state (issue #5 plan, edge case bullet).
//!
//! `applied_at`/`updated_at` are stored as ClickHouse `DateTime` (`UInt32`
//! seconds-since-epoch on the wire) — no `chrono`/`time` feature is enabled
//! on the workspace's `clickhouse` dependency, so the plain `u32` the crate
//! already round-trips for that CH type is used directly rather than adding
//! a date/time crate for two timestamp columns nothing else reads.

//! Writes use hand-built `INSERT ... VALUES` text executed via
//! [`ChClient::execute`], not [`ChClient::insert_block`]: the `clickhouse`
//! crate's typed `.insert()` path escapes its whole `table` argument as a
//! *single* SQL identifier (for the unqualified-name-in-current-database
//! case its own tests assume), so a `{{db}}.table`-qualified name comes out
//! backtick-quoted whole — `` `mydb.mytable` `` — and the server resolves it
//! as one (wrong) identifier inside whatever database the connection is
//! bound to. `execute`'s plain-SQL-text path has no such restriction. This
//! only affects the schema controller's own bookkeeping writes: the
//! controller's `ChClient` is deliberately bound to a database that is
//! guaranteed to exist ("default") rather than `ctx.db` (which may not
//! exist yet on a first `--mode init`, docs/schemas.md's "CREATE DATABASE
//! chicken-and-egg" edge case) — real sample-row writers (issue #6) bind
//! their connection's database directly to the target db and are unaffected.

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, Idempotency, QuerySettings, Row};

use crate::error::SchemaError;
use crate::render::RenderCtx;

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
pub(crate) struct MigrationRow {
    pub id: u32,
    pub checksum: String,
    pub applied_at: u32,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
pub(crate) struct MvChecksumRow {
    pub mv_name: String,
    pub checksum: String,
    pub updated_at: u32,
}

fn now_unix() -> u32 {
    // Wall-clock only, for a human-readable bookkeeping timestamp — never
    // used for ordering/causality (that's `id`/`mv_name` uniqueness plus
    // ReplacingMergeTree(applied_at/updated_at)'s "last write wins" merge,
    // which this crate treats as advisory bookkeeping, not a consistency
    // mechanism).
    u32::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    )
    .unwrap_or(u32::MAX)
}

/// Server error code for "table/database doesn't exist" (`UNKNOWN_TABLE` /
/// `UNKNOWN_DATABASE`). Both `find_migration` and `find_mv_checksum` must
/// tolerate this: on the very first `reconcile` run neither bookkeeping
/// table exists yet, and that must read back as "not yet applied", not as
/// an error (chicken-and-egg, docs/schemas.md §6).
const UNKNOWN_TABLE_CODES: &[i32] = &[60, 81];

fn is_missing_table(err: &ChError) -> bool {
    matches!(err, ChError::Server { code, .. } if UNKNOWN_TABLE_CODES.contains(code))
}

/// Reads the applied `(id, checksum)` row for one migration id, `FINAL`.
/// Returns `Ok(None)` both when no row exists yet and when
/// `schema_migrations` itself doesn't exist yet.
pub(crate) async fn find_migration(
    client: &ChClient,
    ctx: &RenderCtx,
    id: u32,
) -> Result<Option<MigrationRow>, SchemaError> {
    let sql = format!(
        "SELECT id, checksum, applied_at FROM {}.schema_migrations FINAL WHERE id = {id}",
        ctx.db
    );
    match client
        .query_stream::<MigrationRow>(&sql, &QuerySettings::new())
        .await
    {
        Ok(mut stream) => match stream.next().await {
            Some(Ok(row)) => Ok(Some(row)),
            // The `clickhouse` crate's HTTP transport streams a 200 OK
            // before the server has necessarily finished planning the
            // query, so a genuinely missing table often surfaces here
            // (mid-stream), not from the `query_stream(...).await` call
            // below — both sites must tolerate it identically.
            Some(Err(e)) if is_missing_table(&e) => Ok(None),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        },
        Err(e) if is_missing_table(&e) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Records that migration `id` was just applied with `checksum`. Marked
/// [`Idempotency::Idempotent`]: unlike `pulsus-clickhouse::insert_block`'s
/// "never auto-retried" sample-data rule (a retried block would permanently
/// inflate tier aggregates), a retried bookkeeping `INSERT` merely adds a
/// duplicate row that `ReplacingMergeTree` + `FINAL` reads already tolerate
/// by construction (this module's top-level doc comment).
pub(crate) async fn record_migration(
    client: &ChClient,
    ctx: &RenderCtx,
    id: u32,
    checksum: &str,
) -> Result<(), SchemaError> {
    let checksum = checksum.replace('\'', "''");
    let now = now_unix();
    let sql = format!(
        "INSERT INTO {}.schema_migrations (id, checksum, applied_at) VALUES ({id}, '{checksum}', {now})",
        ctx.db
    );
    client
        .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await?;
    Ok(())
}

/// Reads the recorded checksum for one MV name, `FINAL`. `Ok(None)` both
/// when no row exists and when `mv_checksums` doesn't exist yet.
pub(crate) async fn find_mv_checksum(
    client: &ChClient,
    ctx: &RenderCtx,
    mv_name: &str,
) -> Result<Option<String>, SchemaError> {
    let escaped = mv_name.replace('\'', "''");
    let sql = format!(
        "SELECT mv_name, checksum, updated_at FROM {}.mv_checksums FINAL WHERE mv_name = '{escaped}'",
        ctx.db
    );
    match client
        .query_stream::<MvChecksumRow>(&sql, &QuerySettings::new())
        .await
    {
        Ok(mut stream) => match stream.next().await {
            Some(Ok(row)) => Ok(Some(row.checksum)),
            Some(Err(e)) if is_missing_table(&e) => Ok(None),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        },
        Err(e) if is_missing_table(&e) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Upserts the checksum for one MV. Called strictly *after* the MV has been
/// dropped and recreated (issue #5 plan amendment 1: checksum is the LAST
/// step, so a crash before this point leaves the checksum stale/absent and
/// `reconcile` self-heals on retry rather than masking a missing view).
pub(crate) async fn upsert_mv_checksum(
    client: &ChClient,
    ctx: &RenderCtx,
    mv_name: &str,
    checksum: &str,
) -> Result<(), SchemaError> {
    let mv_name_escaped = mv_name.replace('\'', "''");
    let checksum_escaped = checksum.replace('\'', "''");
    let now = now_unix();
    let sql = format!(
        "INSERT INTO {}.mv_checksums (mv_name, checksum, updated_at) VALUES ('{mv_name_escaped}', '{checksum_escaped}', {now})",
        ctx.db
    );
    client
        .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await?;
    Ok(())
}

/// `xxh64`-hex checksum over rendered DDL (drift detection is not
/// adversarial — an operator-visible mismatch, not a security boundary — so
/// the already-vendored `xxhash-rust` primitive is reused rather than adding
/// `sha2`).
pub(crate) fn checksum_hex(rendered: &str) -> String {
    format!("{:016x}", xxhash_rust::xxh64::xxh64(rendered.as_bytes(), 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_hex_is_deterministic_and_16_hex_chars() {
        let a = checksum_hex("CREATE TABLE x");
        let b = checksum_hex("CREATE TABLE x");
        assert_eq!(a, b);
        assert_eq!(a.len(), 16);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn checksum_hex_differs_for_different_input() {
        assert_ne!(checksum_hex("a"), checksum_hex("b"));
    }

    #[test]
    fn is_missing_table_matches_unknown_table_and_database_codes() {
        assert!(is_missing_table(&ChError::Server {
            code: 60,
            message: "Table doesn't exist".to_string()
        }));
        assert!(is_missing_table(&ChError::Server {
            code: 81,
            message: "Database doesn't exist".to_string()
        }));
        assert!(!is_missing_table(&ChError::Server {
            code: 62,
            message: "Syntax error".to_string()
        }));
        assert!(!is_missing_table(&ChError::Timeout("x".to_string())));
    }
}
