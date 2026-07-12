//! DDL / maintenance scenario — decision-critical (issue #3 amendment,
//! Codex finding 2). Runs the exact docs/schemas.md §2.2 `CREATE TABLE` /
//! `CREATE MATERIALIZED VIEW` plus a chunked `INSERT ... SELECT` backfill
//! through **both** candidates, each over its own transport (`clickhouse`
//! over HTTP, `klickhouse` over native TCP), to decide whether one crate's
//! transport suffices for reliable DDL + maintenance (configuration.md §2).

use super::CrateUnderTest;

#[derive(Clone, Debug, serde::Serialize)]
pub struct DdlReport {
    pub crate_name: &'static str,
    pub create_table_ok: bool,
    pub create_mv_ok: bool,
    pub backfill_chunks_ok: usize,
    pub backfill_chunks_total: usize,
    pub error: Option<String>,
}

impl DdlReport {
    pub fn reliable(&self) -> bool {
        self.create_table_ok
            && self.create_mv_ok
            && self.backfill_chunks_ok == self.backfill_chunks_total
            && self.error.is_none()
    }
}

/// `CREATE TABLE metric_samples_5m`, byte-identical to docs/schemas.md §2.2.
pub fn tier_table_ddl(tier_table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {tier_table} (
            metric_name   LowCardinality(String),
            fingerprint   UInt64                                 CODEC(Delta(8), ZSTD(1)),
            ts            DateTime                               CODEC(DoubleDelta, ZSTD(1)),
            val_min       SimpleAggregateFunction(min, Float64)  CODEC(Gorilla, ZSTD(1)),
            val_max       SimpleAggregateFunction(max, Float64)  CODEC(Gorilla, ZSTD(1)),
            val_sum       SimpleAggregateFunction(sum, Float64)  CODEC(Gorilla, ZSTD(1)),
            val_sum_sq    SimpleAggregateFunction(sum, Float64)  CODEC(Gorilla, ZSTD(1)),
            val_count     SimpleAggregateFunction(sum, UInt64)   CODEC(T64, ZSTD(1)),
            first_time    SimpleAggregateFunction(min, Int64)    CODEC(DoubleDelta, ZSTD(1)),
            last_time     SimpleAggregateFunction(max, Int64)    CODEC(DoubleDelta, ZSTD(1)),
            first_value   AggregateFunction(argMin, Float64, Int64),
            last_value    AggregateFunction(argMax, Float64, Int64)
        ) ENGINE = AggregatingMergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (metric_name, fingerprint, ts)"
    )
}

/// `CREATE MATERIALIZED VIEW metric_samples_5m_mv`, byte-identical to
/// docs/schemas.md §2.2 (reading `raw_table` instead of `metric_samples`).
pub fn tier_mv_ddl(mv_name: &str, tier_table: &str, raw_table: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} TO {tier_table} AS
        SELECT metric_name, fingerprint,
               toStartOfInterval(fromUnixTimestamp64Milli(unix_milli), INTERVAL 300 SECOND) AS ts,
               min(value) AS val_min, max(value) AS val_max, sum(value) AS val_sum,
               sum(value * value) AS val_sum_sq, count() AS val_count,
               min(unix_milli) AS first_time, max(unix_milli) AS last_time,
               argMinState(value, unix_milli) AS first_value,
               argMaxState(value, unix_milli) AS last_value
        FROM {raw_table}
        GROUP BY metric_name, fingerprint, ts"
    )
}

/// One chunk of the one-shot backfill (docs/schemas.md §2.2): the same
/// aggregation shape as the MV, applied to pre-existing data, restricted to
/// one hash bucket of `chunks` so the backfill is chunked rather than a
/// single unbounded `INSERT ... SELECT`.
fn backfill_chunk_sql(tier_table: &str, raw_table: &str, chunk: usize, chunks: usize) -> String {
    format!(
        "INSERT INTO {tier_table}
        SELECT metric_name, fingerprint,
               toStartOfInterval(fromUnixTimestamp64Milli(unix_milli), INTERVAL 300 SECOND) AS ts,
               min(value) AS val_min, max(value) AS val_max, sum(value) AS val_sum,
               sum(value * value) AS val_sum_sq, count() AS val_count,
               min(unix_milli) AS first_time, max(unix_milli) AS last_time,
               argMinState(value, unix_milli) AS first_value,
               argMaxState(value, unix_milli) AS last_value
        FROM {raw_table}
        WHERE cityHash64(fingerprint) % {chunks} = {chunk}
        GROUP BY metric_name, fingerprint, ts"
    )
}

/// Runs the §2.2 DDL + chunked backfill through `c`'s own transport.
/// `raw_table` must already contain rows (populated by the insert scenario).
pub async fn bench_ddl<C: CrateUnderTest>(
    c: &C,
    raw_table: &str,
    tier_table: &str,
    mv_name: &str,
    backfill_chunks: usize,
) -> DdlReport {
    let mut report = DdlReport {
        crate_name: c.name(),
        create_table_ok: false,
        create_mv_ok: false,
        backfill_chunks_ok: 0,
        backfill_chunks_total: backfill_chunks,
        error: None,
    };

    // Clean slate: DDL scenario re-runs are idempotent via IF EXISTS/IF NOT EXISTS.
    let _ = c
        .execute_ddl(&format!("DROP VIEW IF EXISTS {mv_name}"))
        .await;
    let _ = c
        .execute_ddl(&format!("DROP TABLE IF EXISTS {tier_table}"))
        .await;

    if let Err(e) = c.execute_ddl(&tier_table_ddl(tier_table)).await {
        report.error = Some(format!("create table: {e}"));
        return report;
    }
    report.create_table_ok = true;

    if let Err(e) = c
        .execute_ddl(&tier_mv_ddl(mv_name, tier_table, raw_table))
        .await
    {
        report.error = Some(format!("create mv: {e}"));
        return report;
    }
    report.create_mv_ok = true;

    for chunk in 0..backfill_chunks {
        match c
            .execute_ddl(&backfill_chunk_sql(
                tier_table,
                raw_table,
                chunk,
                backfill_chunks,
            ))
            .await
        {
            Ok(()) => report.backfill_chunks_ok += 1,
            Err(e) => {
                report.error = Some(format!("backfill chunk {chunk}: {e}"));
                break;
            }
        }
    }
    report
}
