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

/// Spool root, relative to the process working directory (task-manager
/// resolution, issue #9 — mirrors issue #8's `MAX_DECOMPRESSED_BYTES`
/// documented-constant precedent). Holds `poison/{table}/` and
/// `uncertain/{table}/` subdirectories, created on first use.
const SPOOL_DIR: &str = "./spool";

/// Cadence of every registration-backfill re-insert task (issues
/// #134/#139: `log_streams` and `trace_attrs_idx` — the metrics path has
/// no backlog, issue #603): every interval, any Poisoned-flush registration
/// rows still pending in that table's in-memory backlog are re-inserted
/// once. A documented constant per the issue-#9 constants-not-env-vars
/// precedent ([`LRU_CAPACITY`]); promote to a `PULSUS_*` var if a
/// deployment needs to tune it.
const REGISTRATION_BACKFILL_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Byte cap (row `est_bytes` accounting) on each in-memory registration
/// backfill backlog (issues #134/#139). PER-BACKLOG: with two backlogs
/// (`log_streams` and `trace_attrs_idx`), the worst-case process-wide
/// footprint under total ClickHouse failure is 2 × 32 MiB = 64 MiB —
/// acceptable, since the
/// sample/span paths are failing too in that state, minting few new
/// orphans. New keys that would exceed a backlog's cap are rejected and
/// counted via its `BackfillMetrics::dropped_total`. Same
/// documented-constant precedent as
/// [`REGISTRATION_BACKFILL_RETRY_INTERVAL`].
const REGISTRATION_BACKFILL_MAX_BYTES: u64 = 32 * 1024 * 1024;

/// How long past its buffers' age-flush trigger a claim may stay open
/// before the suppression index gives up on it and writes a tombstone
/// (issue #494). Added to `PULSUS_BATCH_MS`, so at the defaults a claim
/// ages at `120.2 s`: the age-flush trigger plus the bound on the insert
/// it triggers.
///
/// This is `PULSUS_QUERY_TIMEOUT`'s documented default (`2m`,
/// docs/configuration.md §1) written as a constant rather than read from
/// the knob, because the writer's construction seam takes a
/// [`WriterConfig`] and that knob is not in it. Same documented-constant
/// precedent as [`LRU_CAPACITY`]; promote both together if a deployment
/// needs to tune it.
const CLAIM_INSERT_BOUND: Duration = Duration::from_secs(120);

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
    pub spool_dir: PathBuf,
    /// The registration-backfill re-insert cadence (issues #134/#139),
    /// shared by both backfill tasks (`log_streams` and
    /// `trace_attrs_idx`).
    pub backfill_retry_interval: Duration,
    /// The PER-BACKLOG registration-backfill byte cap (issues #134/#139)
    /// — two backlogs ⇒ 64 MiB worst-case process-wide (see
    /// [`REGISTRATION_BACKFILL_MAX_BYTES`]).
    pub backfill_max_bytes: u64,
    /// `PULSUS_LOG_PATTERNS` (M7-C3, issue #171): the ingest-time log-pattern
    /// extraction kill-switch. `false` ⇒ zero extraction work and zero
    /// `log_patterns` appends; the read endpoint stays mounted and serves
    /// empty data.
    pub log_patterns: bool,
    /// `PULSUS_INGEST_DEDUP` (issue #494): retried-push suppression.
    /// `false` ⇒ no index is built at all and every push is admitted, which
    /// is the pre-#494 behaviour.
    pub ingest_dedup: bool,
    /// `PULSUS_INGEST_DEDUP_WINDOW`: how long a writer remembers an
    /// accepted push.
    pub ingest_dedup_window: Duration,
    /// `PULSUS_INGEST_DEDUP_MAX_BYTES`: the whole per-signal index's byte
    /// bound.
    pub ingest_dedup_max_bytes: u64,
    /// How long a claim may stay open before it becomes a tombstone —
    /// derived, not a knob (see [`CLAIM_INSERT_BOUND`]).
    pub claim_deadline: Duration,
    /// `PULSUS_METRICS_LANDING_RETRIES` (issue #603): resends of a failed
    /// metrics landing insert. [`Self::landing_budget`] is the wall-clock
    /// bound on the whole loop; whichever binds first ends it.
    pub metrics_landing_retries: u32,
    /// `PULSUS_METRICS_LANDING_INSERTERS` (issue #603): insert workers on
    /// the metrics landing queue, so the landing inserts in flight at once.
    pub metrics_landing_inserters: u32,
    /// `PULSUS_METRICS_LANDING_MAX_ROWS` (issue #603): the per-push landing
    /// row ceiling, and the `max_insert_block_size` every landing insert
    /// pins, so an admitted push is strictly under the value the server
    /// would split a block at.
    pub metrics_landing_max_rows: u64,
    /// The wall-clock bound on one metrics landing block: the queue wait,
    /// every attempt and every sleep, measured from the push's admission
    /// (issue #603). Derived rather than configured, from
    /// [`CLAIM_INSERT_BOUND`] — the shipped claim deadline
    /// (`batch_ms + CLAIM_INSERT_BOUND`) already reserves it, so the loop
    /// settles strictly before the claim could age into a tombstone. It is a
    /// field because the constant is private to this module and the loop
    /// that reads it lives in another.
    pub landing_budget: Duration,
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
            spool_dir: PathBuf::from(SPOOL_DIR),
            backfill_retry_interval: REGISTRATION_BACKFILL_RETRY_INTERVAL,
            backfill_max_bytes: REGISTRATION_BACKFILL_MAX_BYTES,
            log_patterns: cfg.log_patterns,
            ingest_dedup: cfg.ingest_dedup,
            ingest_dedup_window: cfg.ingest_dedup_window.0,
            ingest_dedup_max_bytes: cfg.ingest_dedup_max_bytes.0,
            claim_deadline: Duration::from_millis(cfg.batch_ms) + CLAIM_INSERT_BOUND,
            metrics_landing_retries: cfg.metrics_landing_retries,
            metrics_landing_inserters: cfg.metrics_landing_inserters,
            metrics_landing_max_rows: cfg.metrics_landing_max_rows,
            landing_budget: CLAIM_INSERT_BOUND,
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

    /// Issue #494: the claim deadline is the age-flush trigger plus the
    /// bound on the insert it triggers — `120.2 s` at the defaults.
    #[test]
    fn claim_deadline_is_the_flush_trigger_plus_the_insert_bound() {
        let runtime = WriterRuntime::from_config(&WriterConfig::default());
        assert_eq!(runtime.claim_deadline, Duration::from_millis(120_200));
        let slow = WriterConfig {
            batch_ms: 5_000,
            ..Default::default()
        };
        assert_eq!(
            WriterRuntime::from_config(&slow).claim_deadline,
            Duration::from_secs(125)
        );
    }

    /// Issue #494: a claim must never outlive the window that decides
    /// whether a retry is still suppressible, or an open claim would age
    /// into a tombstone only after the entry it belongs to was already
    /// forgotten. At the smallest accepted window the deadline is longer,
    /// which is why the index treats whichever comes first as the end of
    /// the claim — the window sweep runs before the age sweep in `tick`.
    #[test]
    fn the_accepted_window_range_brackets_the_derived_deadline() {
        let runtime = WriterRuntime::from_config(&WriterConfig::default());
        assert!(runtime.claim_deadline < runtime.ingest_dedup_window);
        assert_eq!(runtime.ingest_dedup_window, Duration::from_secs(300));
        assert_eq!(runtime.ingest_dedup_max_bytes, 16 * 1024 * 1024);
        assert!(runtime.ingest_dedup);
    }

    #[test]
    fn retry_budget_is_bounded() {
        const { assert!(RETRY_MAX_ATTEMPTS > 0 && RETRY_MAX_ATTEMPTS <= 10) };
    }

    #[test]
    fn from_config_carries_the_log_patterns_kill_switch_both_states() {
        // Default ON (M7-C3, issue #171).
        assert!(WriterRuntime::from_config(&WriterConfig::default()).log_patterns);
        let off = WriterConfig {
            log_patterns: false,
            ..Default::default()
        };
        assert!(!WriterRuntime::from_config(&off).log_patterns);
    }
}
