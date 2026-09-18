//! `LogWriter`: a generic per-table columnar writer core (issue #9
//! architect plan), wired for the two log tables (`log_samples`,
//! `log_streams`) — metric tables land in M2. Implements issue #8's
//! [`crate::ingest::LogSink`] seam: `admit` buffers and returns
//! immediately (async mode); `admit_flush` buffers and returns a
//! [`crate::ingest::FlushWait`] that resolves once every touched flush
//! generation is durable (sync mode).
//!
//! **Consistency model** (architect plan, "cross-table atomicity"):
//! `log_samples` and `log_streams` flush independently — there is no
//! cross-table atomic insert. A sync caller only observes success once
//! *both* its samples and its (newly-registered) streams are durable
//! (the wait-join below); an async caller's stream registration may lag
//! its samples by up to one `log_streams` flush cycle, an accepted
//! eventual-consistency window. `StreamLru` promotion happens only after
//! a confirmed `log_streams` flush (never optimistically at admission),
//! so a concurrent duplicate registration is possible but harmless
//! (`ReplacingMergeTree` collapses it) — see `writer::registration`'s doc
//! comment.
//!
//! **Registration backfill** (issues #134/#139): a registration flush
//! that fails *definitely* (`Poisoned` — provably not-committed) enqueues
//! its rows into a bounded in-memory backlog (`writer::backfill`) that a
//! dedicated task re-inserts every
//! `WriterRuntime::backfill_retry_interval` (5s) until confirmed, capped
//! at `backfill_max_bytes` (32 MiB) per backlog — so a "samples/spans
//! committed, registration lost" orphan self-heals while the process
//! lives. One generic mechanism serves all four registration tables:
//! `log_streams` (here, with success-only `StreamLru` promotion on a
//! confirmed heal), `metric_series`/`metric_metadata`
//! (`writer::metric` — `SeriesLru` promotion / invalidate-only
//! `MetadataCache` heal respectively), and `trace_attrs_idx`
//! (`writer::trace`, no cache). The append-only tables (`log_samples`,
//! `metric_samples`, `metric_hist_samples`, `trace_spans`) are
//! structurally excluded (`on_flush_poisoned: None`). Uncertain-fate
//! flushes are NEVER backfilled (issue #9: an `InsertUncertain` batch is
//! never auto-replayed), and a backfill re-insert that itself returns
//! `InsertUncertain` is terminally abandoned, not retried. Honest
//! residuals — every spool write is best-effort (`finish_generation` only
//! logs and counts `spool_write_failures_total` on spool I/O failure), so
//! no spool record is ever guaranteed to exist:
//! - R1: an uncertain-fate registration whose samples committed is not
//!   healed; an `uncertain/` audit record exists iff that batch's spool
//!   write succeeded (audit-only per #9, never replayed).
//! - R2: a backfill re-insert returning `InsertUncertain` is terminally
//!   abandoned (counted + warn-logged) — same residual class as R1.
//! - R3: the backlog is memory-only — an orphan persists across a crash
//!   only if the stream also never pushes again in that month (a restart
//!   empties the LRU, so any later push re-registers naturally). A
//!   poison-spool repair record exists only when the spool write
//!   succeeded; manually re-inserting a poison-spooled `log_streams`
//!   batch is safe (definitely-not-committed + `ReplacingMergeTree`
//!   collapse), unlike `uncertain/`.
//! - R4: byte-cap drops under sustained failure are counted
//!   (`backfill_dropped_total`); a poison-spool record for the dropped
//!   batch exists iff that batch's spool write succeeded.
//! - R5: on spool-write failure there is no durable record — the
//!   in-memory backlog is the only (best-effort) remedy; surfaced via the
//!   error log + `spool_write_failures_total`.
//! - R4∧R5 compound: a byte-cap-dropped entry whose generation's spool
//!   write also failed is lost entirely (counters + logs are the sole
//!   evidence) — acknowledged-lost by design, never claimed healed; the
//!   same compound applies to every backlog exit without heal (tick
//!   deterministic/uncertain abandons ∧ R5).
//!
//! **Backpressure** (architect plan amendment 1): `queued_bytes` is
//! reserved atomically at admission (`fetch_add` first, roll back on
//! overflow) and counts buffered *and* in-flight bytes, decremented
//! exactly once when the owning flush generation settles
//! (`writer::table`'s single settle path) — never briefly under-reserved
//! under concurrent admits.
//!
//! **Shutdown** (architect plan amendment 2): [`LogWriter::shutdown`]
//! stops admission immediately (`Backpressure`), then drains every
//! open/in-flight generation up to a deadline; anything still unsettled
//! at the deadline is force-settled with [`WriteError::ShuttingDown`]
//! through the same settle path flush success/failure use.

mod backfill;
mod buffer;
mod config;
mod error;
mod metric;
mod metrics;
mod push_dedup;
mod registration;
pub(crate) mod rows;
mod spool;
mod table;
mod trace;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use pulsus_clickhouse::ChClient;
use pulsus_config::WriterConfig;
use tokio::sync::{Notify, oneshot};
use tracing::warn;

pub use config::WriterRuntime;
pub use error::WriteError;
pub use metric::{MetricWriter, MetricWriterTables};
pub use metrics::{
    BackfillMetricsSnapshot, MetricWriterMetrics, MetricWriterMetricsSnapshot,
    TableMetricsSnapshot, TraceWriterMetrics, TraceWriterMetricsSnapshot, WriterMetrics,
    WriterMetricsSnapshot,
};
pub use push_dedup::{
    Admission, Capacities, ClaimGuard, ClaimOutcome, DedupMetrics, DedupMetricsSnapshot, PushDedup,
    PushDigest, PushIdentity, TargetOutcome, WaitGuard, WaitMode, index_bytes, plan_capacities,
};
pub use registration::{MetadataCache, SeriesLru, StreamLru};
pub use rows::{
    LogPatternRow, LogSampleRow, LogStreamRow, MetricHistSampleRow, MetricMetadataRow,
    MetricSampleRow, MetricSeriesRow, TraceAttrRow, TraceSpanRow,
};
pub use table::{BlockInserter, ChBlockInserter};
pub use trace::{TraceWriter, TraceWriterTables};

use crate::error::LogsIngestError;
use crate::ingest::{AdmitRefusal, Backpressure, FlushWait, LogSink, PushHeaders};
use crate::patterns::{
    AGG_BASE_OVERHEAD, MAX_DISTINCT_PATTERNS_PER_BATCH, PATTERN_ROW_OVERHEAD, aggregate_patterns,
    est_template_bound,
};
use crate::protocols::otlp_logs::{ParsedLogs, StreamRow};
use table::{ShutdownSignal, TableContext};

const SAMPLES_TABLE: &str = "log_samples";
const STREAMS_TABLE: &str = "log_streams";
const PATTERNS_TABLE: &str = "log_patterns";

/// The two target table names a [`LogWriter`] inserts into (issue #15
/// architect plan, Design A): cluster-mode deployments write through the
/// `_dist` Distributed wrappers, mirroring the reader's own
/// `chconfig::engine_config_from` `_dist` derivation — schemas.md §7's
/// mandate is that "all inserts go through the `_dist` wrappers … the
/// writer never freelances shard placement". `Arc<str>` (not
/// `&'static str`): the cluster-suffixed name is computed once at server
/// startup from `Config`, not known at compile time.
#[derive(Debug, Clone)]
pub struct WriterTables {
    pub samples: Arc<str>,
    pub streams: Arc<str>,
    /// `log_patterns` (M7-C3, issue #171) — a fourth Logs-family table,
    /// `_dist`-aware exactly like `samples`/`streams` (co-sharded on
    /// `fingerprint`).
    pub patterns: Arc<str>,
}

impl WriterTables {
    /// Unclustered defaults: the bare local table names, matching this
    /// module's pre-issue-#15 hardcoded behavior exactly — every existing
    /// caller (`new`/`with_inserters`) delegates here so single-node
    /// behavior and every pre-existing test are unchanged.
    pub fn logs_default() -> Self {
        WriterTables {
            samples: Arc::from(SAMPLES_TABLE),
            streams: Arc::from(STREAMS_TABLE),
            patterns: Arc::from(PATTERNS_TABLE),
        }
    }
}

/// The atomic queue-bytes admission reservation shared by the log,
/// metric, and trace writers (issue #133 extraction — behavior-identical
/// to the three inline blocks it replaced): reserve first, roll back and
/// count a backpressure rejection on overflow. The counter may
/// transiently over-reserve under concurrent admits (a race loser
/// subtracts back) but never under-reserves, so the memory bound holds.
/// Extracted so the 503 backpressure guard is provable at the maximum
/// config-accepted `writer.ingest_queue_bytes`
/// (`pulsus_config::INGEST_QUEUE_BYTES_CEILING`) with synthetic
/// counters — no real queue is allocated.
pub(crate) fn reserve_queued_bytes(
    queued_bytes: &AtomicU64,
    backpressure_total: &AtomicU64,
    total_bytes: u64,
    queue_bytes_limit: u64,
) -> Result<(), Backpressure> {
    let previous = queued_bytes.fetch_add(total_bytes, Ordering::AcqRel);
    if previous + total_bytes > queue_bytes_limit {
        queued_bytes.fetch_sub(total_bytes, Ordering::AcqRel);
        backpressure_total.fetch_add(1, Ordering::Relaxed);
        return Err(Backpressure);
    }
    Ok(())
}

struct Shared {
    samples: Arc<buffer::TableBuffer<LogSampleRow>>,
    streams: Arc<buffer::TableBuffer<LogStreamRow>>,
    /// `log_patterns` buffer (M7-C3, issue #171). Derived data: appended in
    /// async mode only (never joins the `admit_flush` durability ack), so a
    /// `log_patterns` insert failure never 500s an ingest whose log lines
    /// landed.
    patterns: Arc<buffer::TableBuffer<LogPatternRow>>,
    samples_notify: Arc<Notify>,
    streams_notify: Arc<Notify>,
    patterns_notify: Arc<Notify>,
    queued_bytes: Arc<AtomicU64>,
    runtime: Arc<WriterRuntime>,
    metrics: Arc<WriterMetrics>,
    lru: Arc<Mutex<StreamLru>>,
    /// Issue #494's per-signal push-suppression index. `None` while
    /// `PULSUS_INGEST_DEDUP` is off, which restores the pre-#494 behaviour
    /// exactly: no index is built, no digest is computed, no claim is taken.
    dedup: Option<Arc<PushDedup>>,
    shutdown: ShutdownSignal,
    shutting_down: AtomicBool,
    samples_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    streams_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    patterns_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    backfill_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Implements issue #8's `LogSink` over a generic per-table columnar
/// writer core. See the module-level docs above.
pub struct LogWriter {
    shared: Arc<Shared>,
}

impl LogWriter {
    /// Production constructor: batches and flushes through a real
    /// ClickHouse connection, against the unclustered default table names
    /// ([`WriterTables::logs_default`]). Delegates to
    /// [`Self::new_with_tables`] — zero behavior change from before issue
    /// #15.
    pub fn new(client: Arc<ChClient>, cfg: &WriterConfig) -> Self {
        Self::new_with_tables(client, cfg, WriterTables::logs_default())
    }

    /// [`Self::new`], but against `tables` (issue #15 architect plan,
    /// Design A) — the server's cluster-aware constructor for `_dist`
    /// table names.
    pub fn new_with_tables(
        client: Arc<ChClient>,
        cfg: &WriterConfig,
        tables: WriterTables,
    ) -> Self {
        let inserter: Arc<ChBlockInserter> = Arc::new(ChBlockInserter::new(client));
        Self::with_inserters_with_tables(inserter.clone(), inserter.clone(), inserter, cfg, tables)
    }

    /// Test/mock constructor: any [`BlockInserter`] triple — e.g. a
    /// scriptable mock that can fail/hang on demand (architect plan: "no
    /// real ClickHouse in unit tests") — against the unclustered default
    /// table names. Delegates to [`Self::with_inserters_with_tables`]. The
    /// third inserter is `log_patterns`' (M7-C3, issue #171).
    pub fn with_inserters(
        samples_inserter: Arc<dyn BlockInserter<LogSampleRow>>,
        streams_inserter: Arc<dyn BlockInserter<LogStreamRow>>,
        patterns_inserter: Arc<dyn BlockInserter<LogPatternRow>>,
        cfg: &WriterConfig,
    ) -> Self {
        Self::with_inserters_with_tables(
            samples_inserter,
            streams_inserter,
            patterns_inserter,
            cfg,
            WriterTables::logs_default(),
        )
    }

    /// [`Self::with_inserters`], but against `tables` (issue #15 architect
    /// plan, Design A).
    pub fn with_inserters_with_tables(
        samples_inserter: Arc<dyn BlockInserter<LogSampleRow>>,
        streams_inserter: Arc<dyn BlockInserter<LogStreamRow>>,
        patterns_inserter: Arc<dyn BlockInserter<LogPatternRow>>,
        cfg: &WriterConfig,
        tables: WriterTables,
    ) -> Self {
        let runtime = Arc::new(WriterRuntime::from_config(cfg));
        let metrics = Arc::new(WriterMetrics::default());
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let spool = Arc::new(spool::SpoolWriter::new(
            runtime.spool_dir.clone(),
            metrics.clone(),
        ));
        let (shutdown, shutdown_rx) = ShutdownSignal::new();
        let lru = Arc::new(Mutex::new(StreamLru::new(runtime.lru_capacity)));

        // Issue #494: one index per signal, reserved once from the knob.
        // `log_patterns` is a target of a claim but never joins the sync
        // durability ack (`:566-568` below), so its buffer reports with
        // `counts_for_ack = false` — a pattern-only failure must reproduce
        // the original caller's success, not turn it into one.
        let dedup = runtime.ingest_dedup.then(|| {
            PushDedup::new(
                runtime.ingest_dedup_max_bytes,
                runtime.ingest_dedup_window,
                runtime.claim_deadline,
            )
        });

        let samples = Arc::new(buffer::TableBuffer::with_dedup(dedup.clone(), true));
        let streams = Arc::new(buffer::TableBuffer::with_dedup(dedup.clone(), true));
        let patterns = Arc::new(buffer::TableBuffer::with_dedup(dedup.clone(), false));
        let samples_notify = Arc::new(Notify::new());
        let streams_notify = Arc::new(Notify::new());
        let patterns_notify = Arc::new(Notify::new());

        // `log_streams`'s success-only LRU promotion (architect plan
        // amendment 1): populated ONLY here, after a confirmed flush —
        // never optimistically at admission.
        let lru_for_hook = lru.clone();
        let on_stream_flush_success: table::FlushSuccessHook<LogStreamRow> =
            Arc::new(move |rows: &[LogStreamRow]| {
                let mut guard = lru_for_hook.lock().expect("stream lru mutex poisoned");
                for row in rows {
                    guard.insert((row.fingerprint, row.month));
                }
            });

        // `log_streams`'s Poisoned-only registration backfill (issue
        // #134): a definitely-failed registration flush enqueues its rows
        // for the backfill task's 5s re-insert cadence. The closure
        // delegates to `backfill::enqueue_failed` verbatim (the one seam
        // the compound-failure unit test exercises against production
        // logic).
        let backlog = Arc::new(Mutex::new(backfill::RegistrationBacklog::new(
            runtime.backfill_max_bytes,
        )));
        let backlog_for_hook = backlog.clone();
        let backfill_metrics_for_hook = metrics.backfill.clone();
        let on_stream_flush_poisoned: table::FlushPoisonedHook<LogStreamRow> =
            Arc::new(move |rows: &[LogStreamRow]| {
                backfill::enqueue_failed(&backlog_for_hook, &backfill_metrics_for_hook, rows);
            });

        // Success-only `StreamLru` promotion on a confirmed HEAL (issue
        // #134; an `on_healed` hook since the #139 generalization — same
        // semantics: a confirmed backfill re-insert is a confirmed
        // flush). Safe under any eviction interleaving: a pure membership
        // set over the full logical identity — no value to go stale.
        let lru_for_heal = lru.clone();
        let on_stream_healed: backfill::BackfillHealedHook<LogStreamRow> =
            Arc::new(move |rows: &[LogStreamRow]| {
                let mut guard = lru_for_heal.lock().expect("stream lru mutex poisoned");
                for row in rows {
                    guard.insert((row.fingerprint, row.month));
                }
            });

        let samples_ctx = TableContext {
            table: tables.samples,
            buffer: samples.clone(),
            notify: samples_notify.clone(),
            inserter: samples_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.samples.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: None,
            on_flush_poisoned: None,
            dedup: dedup.clone(),
        };
        let streams_ctx = TableContext {
            table: tables.streams.clone(),
            buffer: streams.clone(),
            notify: streams_notify.clone(),
            inserter: streams_inserter.clone(),
            runtime: runtime.clone(),
            table_metrics: metrics.streams.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: Some(on_stream_flush_success),
            on_flush_poisoned: Some(on_stream_flush_poisoned),
            dedup: dedup.clone(),
        };

        // `log_patterns` (M7-C3, issue #171): a fourth generic flush task,
        // append-only like `log_samples` — no success/poison hook (derived
        // data is never backfilled; patterns re-arrive continuously), no
        // sync-ack waiter (registered in async mode only in `admit_batch`).
        let patterns_ctx = TableContext {
            table: tables.patterns,
            buffer: patterns.clone(),
            notify: patterns_notify.clone(),
            inserter: patterns_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.patterns.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: None,
            on_flush_poisoned: None,
            dedup: dedup.clone(),
        };

        let samples_task = table::spawn(samples_ctx, shutdown_rx.clone());
        let streams_task = table::spawn(streams_ctx, shutdown_rx.clone());
        let patterns_task = table::spawn(patterns_ctx, shutdown_rx.clone());
        let backfill_task = backfill::spawn_backfill(
            backlog,
            streams_inserter,
            tables.streams,
            Some(on_stream_healed),
            metrics.backfill.clone(),
            runtime.clone(),
            shutdown_rx,
        );

        let shared = Arc::new(Shared {
            samples,
            streams,
            patterns,
            samples_notify,
            streams_notify,
            patterns_notify,
            queued_bytes,
            runtime,
            metrics,
            lru,
            dedup,
            shutdown,
            shutting_down: AtomicBool::new(false),
            samples_task: Mutex::new(Some(samples_task)),
            streams_task: Mutex::new(Some(streams_task)),
            patterns_task: Mutex::new(Some(patterns_task)),
            backfill_task: Mutex::new(Some(backfill_task)),
        });

        LogWriter { shared }
    }

    /// Admits `batch`, appending to the samples/streams buffers under one
    /// atomic byte reservation. `mode` selects sync- vs async-mode
    /// admission: [`AdmitMode::Sync`] registers a waiter per touched
    /// generation and returns their receivers for the caller to join;
    /// [`AdmitMode::Async`] registers none.
    ///
    /// Issue #494: the claim is taken **before** the byte reservation, so
    /// a request refused by backpressure leaves no claim behind and the
    /// same body sent again is stored.
    fn admit_batch(
        &self,
        batch: ParsedLogs,
        mode: AdmitMode,
        push: PushHeaders,
    ) -> Result<Admitted, AdmitRefusal> {
        if self.shared.shutting_down.load(Ordering::Acquire) {
            return Err(AdmitRefusal::Backpressure);
        }

        // A parse that failed a stream's label bounds is never claimed: its
        // admitted subset is not the whole request, so a later identical
        // request is not the same push.
        let mut guard = match (&self.shared.dedup, batch.stream_errors.is_empty()) {
            (Some(dedup), true) => {
                let id = push_dedup::log_identity(&batch, &push);
                match dedup.admit(id, mode.wait_mode()) {
                    Admission::Admit(guard) => Some(guard),
                    Admission::SuppressedSettled(outcome) => {
                        dedup.count_suppressed(id.declared_retry, batch.rows.len() as u64);
                        return Ok(Admitted::Suppressed(Suppressed::Settled(outcome)));
                    }
                    Admission::SuppressedPending { guard, rx } => {
                        dedup.count_suppressed(id.declared_retry, batch.rows.len() as u64);
                        return Ok(Admitted::Suppressed(Suppressed::Pending(guard, rx)));
                    }
                    Admission::KeyReused => return Err(AdmitRefusal::KeyReused),
                    Admission::Shed => return Err(AdmitRefusal::DedupShed),
                    Admission::WaitShed => return Err(AdmitRefusal::DedupWaitShed),
                }
            }
            _ => None,
        };
        let claim = guard.as_ref().map(ClaimGuard::key);

        self.shared
            .metrics
            .collisions_total
            .fetch_add(batch.collisions, Ordering::Relaxed);
        self.shared
            .metrics
            .rejected_total
            .fetch_add(batch.rejected, Ordering::Relaxed);

        // Reserve-before-materialize (architect plan amendment 3, finding
        // 2): estimate bytes straight off the source `LogRow`/`StreamRow`
        // refs and decide which streams are LRU misses *before* cloning
        // or canonicalizing anything into the target
        // `LogSampleRow`/`LogStreamRow` shapes — so a request that loses
        // the reservation race below never pays for the clone or the
        // label canonicalization.
        let sample_bytes: u64 = batch.rows.iter().map(LogSampleRow::est_source_bytes).sum();

        // LRU-gate stream registration (architect plan): a hit means this
        // `(fingerprint, month)` was already durably registered by a
        // prior confirmed flush, so this request's copy is skipped
        // entirely — never appended, never counted toward the byte
        // reservation. A miss (including a concurrent, still-unconfirmed
        // duplicate) is appended; the duplicate-tolerant design is
        // documented on `writer::registration`.
        let mut new_streams: Vec<&StreamRow> = Vec::new();
        {
            let mut lru = self.shared.lru.lock().expect("stream lru mutex poisoned");
            for stream in &batch.streams {
                let key = (stream.fingerprint, stream.month.days_since_epoch());
                if lru.contains(&key) {
                    self.shared
                        .metrics
                        .lru_hits_total
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.shared
                        .metrics
                        .lru_misses_total
                        .fetch_add(1, Ordering::Relaxed);
                    new_streams.push(stream);
                }
            }
        }
        let stream_bytes: u64 = new_streams
            .iter()
            .copied()
            .map(LogStreamRow::est_source_bytes)
            .sum();

        // Log-pattern reservation (M7-C3, issue #171, the fixed-ceiling model):
        // reserve = Σ template_bound(row) + AGG_BASE_OVERHEAD
        //         + min(rows, CAP) × PATTERN_ROW_OVERHEAD
        // charged off the source `LogRow` refs BEFORE any extraction, so a
        // request that loses the reservation race below never pays for
        // extraction/aggregation. The map is a per-request-batch structure over
        // this request's already-decode-capped rows (never the flush buffer);
        // the base + capped-per-entry terms upper-bound its peak plus the
        // materialized rows. Disabled ⇒ zero charge and zero work
        // (`PULSUS_LOG_PATTERNS=false`).
        let pattern_reserve: u64 = if self.shared.runtime.log_patterns && !batch.rows.is_empty() {
            let template_bounds: u64 = batch.rows.iter().map(est_template_bound).sum();
            let distinct_cap = batch.rows.len().min(MAX_DISTINCT_PATTERNS_PER_BATCH) as u64;
            template_bounds + AGG_BASE_OVERHEAD + distinct_cap * PATTERN_ROW_OVERHEAD
        } else {
            0
        };

        let total_bytes = sample_bytes + stream_bytes + pattern_reserve;

        // Atomic reservation (architect plan amendment 1): reserve first,
        // roll back on overflow — the counter may transiently
        // over-reserve (a race loser subtracts back) but never
        // under-reserves, so the memory bound holds under concurrent
        // admits.
        reserve_queued_bytes(
            &self.shared.queued_bytes,
            &self.shared.metrics.backpressure_total,
            total_bytes,
            self.shared.runtime.queue_bytes_limit,
        )
        // The guard is still un-sealed here, so returning drops it and the
        // claim is removed: the same body sent again is stored (issue #494).
        .map_err(AdmitRefusal::from)?;

        if self.shared.shutting_down.load(Ordering::Acquire) {
            // Lost the race with `shutdown()`: give the bytes back rather
            // than admitting into buffers a drain pass may never observe
            // (phase 1 of the architect plan amendment 2: "no new
            // generations are created or joined" once shutdown begins).
            self.shared
                .queued_bytes
                .fetch_sub(total_bytes, Ordering::AcqRel);
            return Err(AdmitRefusal::Backpressure);
        }

        // Reservation secured: only now materialize the target rows
        // (clone + canonicalize labels) — the work the reservation gate
        // above exists to admit-or-reject ahead of.
        let sample_rows: Vec<LogSampleRow> = batch.rows.iter().map(LogSampleRow::from).collect();
        let stream_rows: Vec<LogStreamRow> = new_streams
            .iter()
            .copied()
            .map(LogStreamRow::from)
            .collect();

        let mut receivers = Vec::new();

        if !sample_rows.is_empty() {
            note_target(&mut guard, true);
            let should_notify = if mode == AdmitMode::Sync {
                let (should_notify, _generation, rx) = self.shared.samples.append_and_wait(
                    sample_rows,
                    sample_bytes,
                    self.shared.runtime.batch_bytes,
                    claim,
                );
                receivers.push(rx);
                should_notify
            } else {
                self.shared
                    .samples
                    .append(
                        sample_rows,
                        sample_bytes,
                        self.shared.runtime.batch_bytes,
                        claim,
                    )
                    .0
            };
            if should_notify {
                self.shared.samples_notify.notify_one();
            }
        }

        if !stream_rows.is_empty() {
            self.shared
                .metrics
                .stream_registrations_total
                .fetch_add(stream_rows.len() as u64, Ordering::Relaxed);
            note_target(&mut guard, true);
            let should_notify = if mode == AdmitMode::Sync {
                let (should_notify, _generation, rx) = self.shared.streams.append_and_wait(
                    stream_rows,
                    stream_bytes,
                    self.shared.runtime.batch_bytes,
                    claim,
                );
                receivers.push(rx);
                should_notify
            } else {
                self.shared
                    .streams
                    .append(
                        stream_rows,
                        stream_bytes,
                        self.shared.runtime.batch_bytes,
                        claim,
                    )
                    .0
            };
            if should_notify {
                self.shared.streams_notify.notify_one();
            }
        }

        // Log patterns (M7-C3, issue #171): extract + aggregate this batch,
        // append the pre-aggregated rows in ASYNC mode (no waiter — patterns
        // never join the `admit_flush` durability ack), then release the
        // reservation surplus back to `queued_bytes` immediately. `count`
        // inserts as a plain `UInt64` into the `SimpleAggregateFunction(sum)`
        // column. Runs only after the reservation succeeded above, so a
        // reservation-race loser pays for no extraction. `pattern_reserve == 0`
        // covers both the kill-switch-off and the empty-batch cases.
        if pattern_reserve > 0 {
            let agg = aggregate_patterns(&batch.rows);
            if agg.dropped > 0 {
                self.shared
                    .metrics
                    .patterns_dropped_total
                    .fetch_add(agg.dropped, Ordering::Relaxed);
            }
            // The buffered charge is the actual materialized rows' footprint,
            // clamped by the reservation (the D1/D3 bound guarantees
            // `actual <= pattern_reserve`; the clamp is defensive). The buffer
            // releases this charge when its flush generation settles; the
            // surplus is released here and now — so `queued_bytes` stays
            // exactly balanced (reserve = buffered charge + released surplus).
            let actual_bytes: u64 = agg.rows.iter().map(LogPatternRow::est_bytes).sum();
            let charge = actual_bytes.min(pattern_reserve);
            let surplus = pattern_reserve - charge;
            if surplus > 0 {
                self.shared
                    .queued_bytes
                    .fetch_sub(surplus, Ordering::AcqRel);
            }
            if !agg.rows.is_empty() {
                // A claim target, but never one that joins the durability
                // ack: the buffer was built with `counts_for_ack = false`,
                // so a pattern-only failure reproduces the original
                // caller's success rather than turning it into a failure.
                note_target(&mut guard, false);
                if self
                    .shared
                    .patterns
                    .append(agg.rows, charge, self.shared.runtime.batch_bytes, claim)
                    .0
                {
                    self.shared.patterns_notify.notify_one();
                }
            }
        }

        // Arms the claim: from here a drop no longer removes it, and the
        // target set is closed.
        if let Some(guard) = guard {
            guard.seal();
        }
        Ok(Admitted::Stored(receivers))
    }

    /// A point-in-time metrics snapshot (`/metrics` exposition is the
    /// server's job, architect plan "out of scope"; this crate only
    /// maintains the atomics).
    pub fn metrics(&self) -> WriterMetricsSnapshot {
        self.shared.metrics.snapshot(
            self.shared.queued_bytes.load(Ordering::Relaxed),
            self.shared
                .dedup
                .as_ref()
                .map(|d| d.snapshot())
                .unwrap_or_default(),
        )
    }

    /// Graceful shutdown (architect plan amendment 2): stops admitting
    /// immediately (subsequent `admit`/`admit_flush` calls return
    /// `Backpressure`), then drains every open/in-flight generation up to
    /// `deadline`. Any generation still unsettled at the deadline is
    /// force-settled with [`WriteError::ShuttingDown`] through the same
    /// single settle path flush success/failure use. Returns once both
    /// per-table flush tasks and the registration-backfill task have
    /// exited — bounded by `deadline` plus whatever bounded work happens
    /// after each task observes the signal (an in-flight backfill insert
    /// is deadline-bounded and dropped on elapse, issue #134 plan §A).
    /// Idempotent: a second call after the first has completed is a
    /// no-op.
    pub async fn shutdown(&self, deadline: Duration) {
        self.shared.shutting_down.store(true, Ordering::Release);
        self.shared.shutdown.begin(Instant::now() + deadline);

        let samples_task = self
            .shared
            .samples_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();
        let streams_task = self
            .shared
            .streams_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();
        let patterns_task = self
            .shared
            .patterns_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();
        let backfill_task = self
            .shared
            .backfill_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();

        if let Some(task) = samples_task
            && let Err(e) = task.await
        {
            warn!(error = %e, table = SAMPLES_TABLE, "flush task panicked during shutdown");
        }
        if let Some(task) = streams_task
            && let Err(e) = task.await
        {
            warn!(error = %e, table = STREAMS_TABLE, "flush task panicked during shutdown");
        }
        if let Some(task) = patterns_task
            && let Err(e) = task.await
        {
            warn!(error = %e, table = PATTERNS_TABLE, "flush task panicked during shutdown");
        }
        if let Some(task) = backfill_task
            && let Err(e) = task.await
        {
            warn!(
                error = %e,
                table = STREAMS_TABLE,
                "registration backfill task panicked during shutdown"
            );
        }
    }
}

impl LogSink for LogWriter {
    fn admit(&self, batch: ParsedLogs, push: PushHeaders) -> Result<(), AdmitRefusal> {
        self.admit_batch(batch, AdmitMode::Async, push).map(|_| ())
    }

    fn admit_flush(
        &self,
        batch: ParsedLogs,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
        match self.admit_batch(batch, AdmitMode::Sync, push)? {
            Admitted::Stored(receivers) => Ok(FlushWait::new(async move {
                join_generations(receivers)
                    .await
                    .map_err(|e| LogsIngestError::FlushFailed(e.to_string()))
            })),
            Admitted::Suppressed(suppressed) => Ok(FlushWait::new(suppressed.into_answer())),
        }
    }
}

/// Which admission mode a request asked for (`X-Pulsus-Async`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmitMode {
    /// Buffer and return; the handler answers `202`.
    Async,
    /// Buffer and wait for the flush; the handler answers `200`/`204`.
    Sync,
}

impl AdmitMode {
    pub(crate) fn wait_mode(self) -> push_dedup::WaitMode {
        match self {
            AdmitMode::Async => push_dedup::WaitMode::None,
            AdmitMode::Sync => push_dedup::WaitMode::Register,
        }
    }
}

/// What an admission did with a request's rows (issue #494).
#[derive(Debug)]
pub(crate) enum Admitted {
    /// Rows were buffered. Empty in async mode, one receiver per touched
    /// generation in sync mode.
    Stored(Vec<oneshot::Receiver<Result<(), WriteError>>>),
    /// This writer already accepted this push and still remembers it, so
    /// nothing was stored and the answer is the original push's.
    Suppressed(Suppressed),
}

/// How a suppressed caller learns the original push's outcome.
#[derive(Debug)]
pub(crate) enum Suppressed {
    /// Already known: the claim had settled, or the caller is async and is
    /// answered without waiting.
    Settled(ClaimOutcome),
    /// The original is still in flight. The guard holds this caller's
    /// registration — and its bytes — for exactly as long as the wait.
    Pending(push_dedup::WaitGuard, oneshot::Receiver<ClaimOutcome>),
}

impl Suppressed {
    /// The answer a suppressed sync caller receives, byte-identical in the
    /// success case to the one this body received when it was fresh.
    pub(crate) async fn into_answer(self) -> Result<(), LogsIngestError> {
        let outcome = match self {
            Suppressed::Settled(outcome) => outcome,
            Suppressed::Pending(guard, rx) => {
                let outcome = rx.await.unwrap_or(ClaimOutcome::Failed);
                // The guard is held across the await deliberately: its drop
                // is what returns this caller's bytes to the waiter budget,
                // and a cancelled future drops it at exactly the right
                // moment.
                drop(guard);
                outcome
            }
        };
        match outcome {
            ClaimOutcome::Ok => Ok(()),
            ClaimOutcome::Failed => Err(LogsIngestError::FlushFailed(
                "the identical push this one repeats did not become durable".to_string(),
            )),
        }
    }
}

/// Records one appended target on a claim, if there is a claim.
pub(crate) fn note_target(guard: &mut Option<ClaimGuard>, counts_for_ack: bool) {
    if let Some(guard) = guard {
        guard.note_target(counts_for_ack);
    }
}

/// Awaits every joined flush-generation receiver, short-circuiting on the
/// first `Err` (architect plan amendment 1: "await all, short-circuiting
/// on first error") via `futures::future::try_join_all`.
async fn join_generations(
    receivers: Vec<oneshot::Receiver<Result<(), WriteError>>>,
) -> Result<(), WriteError> {
    try_join_all(receivers.into_iter().map(|rx| async move {
        match rx.await {
            Ok(result) => result,
            Err(_dropped_sender) => {
                // Unreachable by construction (architect plan amendment
                // 2): every generation resolves all of its waiters
                // (`buffer::Generation::settle`) before it is dropped, on
                // every path including forced shutdown settlement — a
                // sender dropped without a prior `send` would mean some
                // code path forgot to settle a generation.
                debug_assert!(
                    false,
                    "flush generation waiter dropped without settling (violates the \
                     single-settle-path invariant)"
                );
                Err(WriteError::ShuttingDown)
            }
        }
    }))
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #133 (plan v6 delta 2): the 503 backpressure guard still
    /// fires at the maximum config-accepted `writer.ingest_queue_bytes` —
    /// a reservation landing exactly at the ceiling admits; the next
    /// byte crosses and is rejected LOUDLY (`Err(Backpressure)`, counted
    /// in `backpressure_total`, reservation rolled back). Synthetic
    /// counters only — nothing is allocated; the extracted
    /// [`reserve_queued_bytes`] IS the three writers' admission gate.
    #[test]
    fn queue_reservation_still_fires_at_the_max_accepted_ingest_queue_bytes() {
        let limit = pulsus_config::INGEST_QUEUE_BYTES_CEILING;
        let queued = AtomicU64::new(0);
        let backpressure = AtomicU64::new(0);

        assert!(
            reserve_queued_bytes(&queued, &backpressure, limit, limit).is_ok(),
            "a reservation landing exactly at the ceiling must admit"
        );
        assert_eq!(queued.load(Ordering::Acquire), limit);
        assert_eq!(backpressure.load(Ordering::Relaxed), 0);

        assert!(
            reserve_queued_bytes(&queued, &backpressure, 1, limit).is_err(),
            "the guard must fire at the accepted maximum"
        );
        assert_eq!(
            queued.load(Ordering::Acquire),
            limit,
            "a failed reservation rolls its bytes back"
        );
        assert_eq!(
            backpressure.load(Ordering::Relaxed),
            1,
            "the rejection is counted"
        );
    }
}
