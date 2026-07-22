//! `WriterRuntime`: resolves `pulsus_config::WriterConfig` plus the
//! constants this issue's task-manager resolution documents in code rather
//! than as new `PULSUS_*` variables (retry budget, `StreamLru` capacity,
//! spool root) — see each constant's doc comment. Promote any of these to
//! a documented env var if a deployment needs to tune it; this crate's own
//! wiring (issue #9, "out of scope": env/mode wiring) is not the place to
//! add one speculatively.

use std::path::PathBuf;
use std::time::Duration;

use pulsus_config::WriterConfig;

/// A small, bounded number of *pre-send* retry attempts (architect plan:
/// `pulsus_clickhouse::ChClient::insert_block` downgrades every *post-send*
/// retryable failure to the non-retryable `ChError::InsertUncertain`, so a
/// retryable error ever reaching the writer's classifier can only be a
/// pre-send failure — pool acquisition, connection setup). Capped so a
/// persistently unreachable ClickHouse cannot turn one stalled batch into
/// an unbounded retry loop; the batch spools to poison once exhausted.
const RETRY_MAX_ATTEMPTS: u32 = 5;
/// Base delay for the exponential-backoff-with-full-jitter retry policy
/// (`writer::table`'s hand-rolled xorshift jitter — no `rand` dependency).
const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
/// Upper bound on any single retry delay, regardless of attempt count.
const RETRY_MAX_DELAY: Duration = Duration::from_secs(10);

/// Hand-rolled `StreamLru` capacity (task-manager resolution, issue #9:
/// "documented constants now"; promote to a `PULSUS_*` var if a deployment
/// needs to tune it). 1,000,000 `(fingerprint, month)` entries. Also reused
/// as-is for `MetricWriter`'s `SeriesLru` (issue #26 architect plan: "reuse
/// existing `LRU_CAPACITY` for the series LRU").
const LRU_CAPACITY: usize = 1_000_000;

/// Hand-rolled `MetadataCache` capacity (issue #26 architect plan): 1,000,000
/// distinct `metric_name` entries, each holding one last-emitted `(type,
/// help, unit)` descriptor — a documented constant for now, promote to a
/// `PULSUS_*` var if a deployment needs to tune it, same precedent as
/// [`LRU_CAPACITY`].
const METADATA_LRU_CAPACITY: usize = 1_000_000;

/// Spool root, relative to the process working directory (task-manager
/// resolution, issue #9 — mirrors issue #8's `MAX_DECOMPRESSED_BYTES`
/// documented-constant precedent). Holds `poison/{table}/` and
/// `uncertain/{table}/` subdirectories, created on first use.
const SPOOL_DIR: &str = "./spool";

/// Cadence of the `log_streams` registration-backfill re-insert task
/// (issue #134): every interval, any Poisoned-flush registration rows
/// still pending in the in-memory backlog are re-inserted once. A
/// documented constant per the issue-#9 constants-not-env-vars precedent
/// ([`LRU_CAPACITY`]); promote to a `PULSUS_*` var if a deployment needs
/// to tune it.
const REGISTRATION_BACKFILL_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Byte cap (`LogStreamRow::est_bytes` accounting) on the in-memory
/// registration backfill backlog (issue #134). New keys that would exceed
/// this cap are rejected and counted via `backfill_dropped_total` — under
/// sustained ClickHouse failure the sample path is failing too, so few new
/// orphans are being minted in that state. Same documented-constant
/// precedent as [`REGISTRATION_BACKFILL_RETRY_INTERVAL`].
const REGISTRATION_BACKFILL_MAX_BYTES: u64 = 32 * 1024 * 1024;

/// The resolved set of tunables a `LogWriter` and its per-table flush
/// tasks read from on every admit/flush — computed once at construction,
/// never re-read from the environment afterward.
#[derive(Debug, Clone)]
pub struct WriterRuntime {
    /// `PULSUS_BATCH_BYTES`: a table buffer flushes once its current
    /// generation reaches this many bytes.
    pub batch_bytes: u64,
    /// `PULSUS_BATCH_MS`: a table buffer flushes once its oldest
    /// unflushed row has been buffered this long, even under
    /// `batch_bytes`.
    pub batch_age: Duration,
    /// `PULSUS_INGEST_QUEUE_BYTES`: the combined buffered-plus-in-flight
    /// byte bound across both `log_samples` and `log_streams` (the
    /// backpressure gate).
    pub queue_bytes_limit: u64,
    pub retry_max_attempts: u32,
    pub retry_base_delay: Duration,
    pub retry_max_delay: Duration,
    pub lru_capacity: usize,
    /// [`MetricWriter`](crate::writer::MetricWriter)'s `MetadataCache`
    /// capacity — unused by [`crate::writer::LogWriter`].
    pub metadata_lru_capacity: usize,
    pub spool_dir: PathBuf,
    /// [`crate::writer::LogWriter`]'s registration-backfill re-insert
    /// cadence (issue #134) — unused by the metric/trace writers.
    pub backfill_retry_interval: Duration,
    /// [`crate::writer::LogWriter`]'s registration-backfill backlog byte
    /// cap (issue #134) — unused by the metric/trace writers.
    pub backfill_max_bytes: u64,
}

impl WriterRuntime {
    pub fn from_config(cfg: &WriterConfig) -> Self {
        WriterRuntime {
            batch_bytes: cfg.batch_bytes.0,
            batch_age: Duration::from_millis(cfg.batch_ms),
            queue_bytes_limit: cfg.ingest_queue_bytes.0,
            retry_max_attempts: RETRY_MAX_ATTEMPTS,
            retry_base_delay: RETRY_BASE_DELAY,
            retry_max_delay: RETRY_MAX_DELAY,
            lru_capacity: LRU_CAPACITY,
            metadata_lru_capacity: METADATA_LRU_CAPACITY,
            spool_dir: PathBuf::from(SPOOL_DIR),
            backfill_retry_interval: REGISTRATION_BACKFILL_RETRY_INTERVAL,
            backfill_max_bytes: REGISTRATION_BACKFILL_MAX_BYTES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_maps_batch_ms_to_a_duration() {
        let cfg = WriterConfig {
            batch_ms: 250,
            ..Default::default()
        };
        let runtime = WriterRuntime::from_config(&cfg);
        assert_eq!(runtime.batch_age, Duration::from_millis(250));
    }

    #[test]
    fn from_config_carries_the_configured_byte_limits() {
        let cfg = WriterConfig::default();
        let runtime = WriterRuntime::from_config(&cfg);
        assert_eq!(runtime.batch_bytes, cfg.batch_bytes.0);
        assert_eq!(runtime.queue_bytes_limit, cfg.ingest_queue_bytes.0);
    }

    #[test]
    fn retry_budget_is_bounded() {
        const { assert!(RETRY_MAX_ATTEMPTS > 0 && RETRY_MAX_ATTEMPTS <= 10) };
    }
}
