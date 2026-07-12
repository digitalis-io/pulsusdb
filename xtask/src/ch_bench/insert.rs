//! Bulk columnar insert scenario (docs/schemas.md §2.1 metric shape, §3.1 log
//! shape). Row shapes and codecs are byte-identical to the authoritative DDL
//! (architect amendment, issue #3 Codex finding 1) — not a narrowed tuple.

use std::time::Instant;

use super::rows::{LogRow, MetricRow, gen_log_rows, gen_metric_rows};
use super::{CrateUnderTest, Stats, stats};

#[derive(Clone, Debug, serde::Serialize)]
pub struct InsertReport {
    pub crate_name: &'static str,
    pub shape: &'static str,
    pub rows: u64,
    pub block_rows: u64,
    pub stats: Stats,
    pub rows_per_sec_p50: f64,
    pub mib_per_sec_p50: f64,
    pub parts_after: u64,
}

/// `CREATE TABLE` for the metric-shaped bench table, byte-identical to
/// docs/schemas.md §2.1 `metric_samples` (partition/order/TTL trimmed to a
/// short benchmark-friendly TTL; codecs and column types are unchanged).
pub fn metric_table_ddl(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table} (
            metric_name  LowCardinality(String),
            fingerprint  UInt64   CODEC(Delta(8), ZSTD(1)),
            unix_milli   Int64    CODEC(DoubleDelta, ZSTD(1)),
            value        Float64  CODEC(Gorilla, ZSTD(1))
        ) ENGINE = MergeTree
        PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))
        ORDER BY (metric_name, fingerprint, unix_milli)"
    )
}

/// `CREATE TABLE` for the log-shaped bench table, byte-identical to
/// docs/schemas.md §3.1 `log_samples` (skip indexes omitted — the architect
/// amendment notes they are a write-path concern, not a client-crate axis).
pub fn log_table_ddl(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table} (
            service       LowCardinality(String),
            fingerprint   UInt64,
            timestamp_ns  Int64   CODEC(DoubleDelta, ZSTD(1)),
            severity      Int8    DEFAULT 0,
            body          String  CODEC(ZSTD(1))
        ) ENGINE = MergeTree
        PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
        ORDER BY (service, fingerprint, timestamp_ns)"
    )
}

pub async fn bench_metric_insert<C: CrateUnderTest>(
    c: &C,
    table: &str,
    total_rows: u64,
    block_rows: u64,
    reps: usize,
) -> anyhow::Result<InsertReport> {
    c.execute_ddl(&metric_table_ddl(table)).await?;
    let rows = gen_metric_rows(total_rows, 0, 1);
    let bytes: usize = rows.iter().map(MetricRow::payload_bytes).sum();

    let mut durations = Vec::with_capacity(reps);
    for _ in 0..reps {
        c.execute_ddl(&format!("TRUNCATE TABLE {table}")).await?;
        let start = Instant::now();
        for chunk in rows.chunks(block_rows as usize) {
            c.insert_metric_block(table, chunk).await?;
        }
        durations.push(start.elapsed());
    }
    let st = stats(&durations);
    let parts_after = c.part_count(table).await?;
    Ok(InsertReport {
        crate_name: c.name(),
        shape: "metric",
        rows: total_rows,
        block_rows,
        rows_per_sec_p50: total_rows as f64 / (st.p50_ms / 1000.0),
        mib_per_sec_p50: (bytes as f64 / (1024.0 * 1024.0)) / (st.p50_ms / 1000.0),
        stats: st,
        parts_after,
    })
}

pub async fn bench_log_insert<C: CrateUnderTest>(
    c: &C,
    table: &str,
    total_rows: u64,
    block_rows: u64,
    reps: usize,
) -> anyhow::Result<InsertReport> {
    c.execute_ddl(&log_table_ddl(table)).await?;
    let rows = gen_log_rows(total_rows, 0, 1);
    let bytes: usize = rows.iter().map(LogRow::payload_bytes).sum();

    let mut durations = Vec::with_capacity(reps);
    for _ in 0..reps {
        c.execute_ddl(&format!("TRUNCATE TABLE {table}")).await?;
        let start = Instant::now();
        for chunk in rows.chunks(block_rows as usize) {
            c.insert_log_block(table, chunk).await?;
        }
        durations.push(start.elapsed());
    }
    let st = stats(&durations);
    let parts_after = c.part_count(table).await?;
    Ok(InsertReport {
        crate_name: c.name(),
        shape: "log",
        rows: total_rows,
        block_rows,
        rows_per_sec_p50: total_rows as f64 / (st.p50_ms / 1000.0),
        mib_per_sec_p50: (bytes as f64 / (1024.0 * 1024.0)) / (st.p50_ms / 1000.0),
        stats: st,
        parts_after,
    })
}
