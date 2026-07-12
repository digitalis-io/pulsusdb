//! Comparative benchmark harness for the ClickHouse client spike (issue #3).
//!
//! `CrateUnderTest` is implemented once per candidate crate so every scenario
//! (insert/fetch/aggstate/ddl/tls/pool) runs identically against both. Timing
//! is hand-rolled wall-clock (rows/s, MiB/s, p50/p95 over R reps) rather than
//! criterion: these are multi-second, high-variance, network-bound bulk
//! transfers, not nanosecond microbenchmarks.

pub mod aggstate;
pub mod candidates;
pub mod ddl;
pub mod fetch;
pub mod insert;
pub mod pool;
pub mod rows;
pub mod tls;

use std::time::Duration;

pub use candidates::{ChCandidate, KlCandidate};
pub use rows::{AggRow, LogRow, MetricRow};

/// Wall-clock timing summary over `reps` repetitions of a scenario.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Stats {
    pub reps: usize,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub mean_ms: f64,
}

/// Reduces a set of per-rep durations to [`Stats`]. Empty input yields zeros
/// rather than panicking, since a scenario may be skipped for one candidate.
pub fn stats(durations: &[Duration]) -> Stats {
    if durations.is_empty() {
        return Stats {
            reps: 0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            mean_ms: 0.0,
        };
    }
    let mut millis: Vec<f64> = durations.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    millis.sort_by(|a, b| a.partial_cmp(b).expect("duration millis are always finite"));
    let mean_ms = millis.iter().sum::<f64>() / millis.len() as f64;
    Stats {
        reps: millis.len(),
        p50_ms: percentile(&millis, 0.50),
        p95_ms: percentile(&millis, 0.95),
        mean_ms,
    }
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    let idx = (((sorted_ms.len() - 1) as f64) * p).round() as usize;
    sorted_ms[idx.min(sorted_ms.len() - 1)]
}

/// Crate-agnostic operations every candidate must expose so scenarios run
/// identically against both `clickhouse` (HTTP) and `klickhouse` (native).
///
/// Deliberately synchronous-looking `async fn` in a trait: these two impls
/// are always called concretely (never as `dyn CrateUnderTest`), so the
/// object-safety cost of native `async fn` in traits does not apply here.
pub trait CrateUnderTest {
    /// Short name used in reports (`"clickhouse"` / `"klickhouse"`).
    fn name(&self) -> &'static str;

    /// DDL / maintenance statement over the crate's own transport. No rows
    /// returned. Used for CREATE TABLE / CREATE MATERIALIZED VIEW / chunked
    /// INSERT ... SELECT (the `ddl` scenario, decision-critical).
    fn execute_ddl(&self, sql: &str) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// One columnar insert of metric-shaped rows (docs/schemas.md §2.1).
    fn insert_metric_block(
        &self,
        table: &str,
        rows: &[MetricRow],
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// One columnar insert of log-shaped rows (docs/schemas.md §3.1).
    fn insert_log_block(
        &self,
        table: &str,
        rows: &[LogRow],
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Streaming fetch of the narrow §2.3 hot-path projection
    /// `(fingerprint, unix_milli, value)` out of the full metric table.
    /// Returns `(row_count, checksum)` where `checksum` is a cheap
    /// order-independent fold used to assert both candidates read identical
    /// data; decode does not buffer the whole result set.
    fn fetch_metric_projection(
        &self,
        table: &str,
        metric_name: &str,
    ) -> impl Future<Output = anyhow::Result<(u64, u64)>> + Send;

    /// `system.parts` count for `table` (post-insert part count per scenario).
    fn part_count(&self, table: &str) -> impl Future<Output = anyhow::Result<u64>> + Send;

    /// Runs a SELECT against the aggregate-state tier and decodes the exact
    /// docs/schemas.md §2.3 shape (`finalizeAggregation(argMinMergeState(...))`,
    /// `SimpleAggregateFunction(sum, UInt64)`).
    fn select_agg_rows(
        &self,
        sql: &str,
    ) -> impl Future<Output = anyhow::Result<Vec<AggRow>>> + Send;
}
