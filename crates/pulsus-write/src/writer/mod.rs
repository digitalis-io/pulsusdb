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

mod buffer;
mod config;
mod error;
mod metric;
mod metrics;
mod registration;
mod rows;
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
    MetricWriterMetrics, MetricWriterMetricsSnapshot, TableMetricsSnapshot, TraceWriterMetrics,
    TraceWriterMetricsSnapshot, WriterMetrics, WriterMetricsSnapshot,
};
pub use registration::{MetadataCache, SeriesLru, StreamLru};
pub use rows::{
    LogSampleRow, LogStreamRow, MetricHistSampleRow, MetricMetadataRow, MetricSampleRow,
    MetricSeriesRow, TraceAttrRow, TraceSpanRow,
};
pub use table::{BlockInserter, ChBlockInserter};
pub use trace::{TraceWriter, TraceWriterTables};

use crate::error::LogsIngestError;
use crate::ingest::{Backpressure, FlushWait, LogSink};
use crate::protocols::otlp_logs::{ParsedLogs, StreamRow};
use table::{ShutdownSignal, TableContext};

const SAMPLES_TABLE: &str = "log_samples";
const STREAMS_TABLE: &str = "log_streams";

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
        }
    }
}

struct Shared {
    samples: Arc<buffer::TableBuffer<LogSampleRow>>,
    streams: Arc<buffer::TableBuffer<LogStreamRow>>,
    samples_notify: Arc<Notify>,
    streams_notify: Arc<Notify>,
    queued_bytes: Arc<AtomicU64>,
    runtime: Arc<WriterRuntime>,
    metrics: Arc<WriterMetrics>,
    lru: Arc<Mutex<StreamLru>>,
    shutdown: ShutdownSignal,
    shutting_down: AtomicBool,
    samples_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    streams_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
        Self::with_inserters_with_tables(inserter.clone(), inserter, cfg, tables)
    }

    /// Test/mock constructor: any [`BlockInserter`] pair — e.g. a
    /// scriptable mock that can fail/hang on demand (architect plan: "no
    /// real ClickHouse in unit tests") — against the unclustered default
    /// table names. Delegates to [`Self::with_inserters_with_tables`].
    pub fn with_inserters(
        samples_inserter: Arc<dyn BlockInserter<LogSampleRow>>,
        streams_inserter: Arc<dyn BlockInserter<LogStreamRow>>,
        cfg: &WriterConfig,
    ) -> Self {
        Self::with_inserters_with_tables(
            samples_inserter,
            streams_inserter,
            cfg,
            WriterTables::logs_default(),
        )
    }

    /// [`Self::with_inserters`], but against `tables` (issue #15 architect
    /// plan, Design A).
    pub fn with_inserters_with_tables(
        samples_inserter: Arc<dyn BlockInserter<LogSampleRow>>,
        streams_inserter: Arc<dyn BlockInserter<LogStreamRow>>,
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

        let samples = Arc::new(buffer::TableBuffer::new());
        let streams = Arc::new(buffer::TableBuffer::new());
        let samples_notify = Arc::new(Notify::new());
        let streams_notify = Arc::new(Notify::new());

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
        };
        let streams_ctx = TableContext {
            table: tables.streams,
            buffer: streams.clone(),
            notify: streams_notify.clone(),
            inserter: streams_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.streams.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: Some(on_stream_flush_success),
        };

        let samples_task = table::spawn(samples_ctx, shutdown_rx.clone());
        let streams_task = table::spawn(streams_ctx, shutdown_rx);

        let shared = Arc::new(Shared {
            samples,
            streams,
            samples_notify,
            streams_notify,
            queued_bytes,
            runtime,
            metrics,
            lru,
            shutdown,
            shutting_down: AtomicBool::new(false),
            samples_task: Mutex::new(Some(samples_task)),
            streams_task: Mutex::new(Some(streams_task)),
        });

        LogWriter { shared }
    }

    /// Admits `batch`, appending to the samples/streams buffers under one
    /// atomic byte reservation. `with_waiters` selects sync- vs
    /// async-mode admission: `true` registers a waiter per touched
    /// generation and returns their receivers for the caller to join;
    /// `false` (async mode) registers none.
    fn admit_batch(
        &self,
        batch: ParsedLogs,
        with_waiters: bool,
    ) -> Result<Vec<oneshot::Receiver<Result<(), WriteError>>>, Backpressure> {
        if self.shared.shutting_down.load(Ordering::Acquire) {
            return Err(Backpressure);
        }

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
        let total_bytes = sample_bytes + stream_bytes;

        // Atomic reservation (architect plan amendment 1): reserve first,
        // roll back on overflow — the counter may transiently
        // over-reserve (a race loser subtracts back) but never
        // under-reserves, so the memory bound holds under concurrent
        // admits.
        let previous = self
            .shared
            .queued_bytes
            .fetch_add(total_bytes, Ordering::AcqRel);
        if previous + total_bytes > self.shared.runtime.queue_bytes_limit {
            self.shared
                .queued_bytes
                .fetch_sub(total_bytes, Ordering::AcqRel);
            self.shared
                .metrics
                .backpressure_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(Backpressure);
        }

        if self.shared.shutting_down.load(Ordering::Acquire) {
            // Lost the race with `shutdown()`: give the bytes back rather
            // than admitting into buffers a drain pass may never observe
            // (phase 1 of the architect plan amendment 2: "no new
            // generations are created or joined" once shutdown begins).
            self.shared
                .queued_bytes
                .fetch_sub(total_bytes, Ordering::AcqRel);
            return Err(Backpressure);
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
            if with_waiters {
                let (should_notify, rx) = self.shared.samples.append_and_wait(
                    sample_rows,
                    sample_bytes,
                    self.shared.runtime.batch_bytes,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.samples_notify.notify_one();
                }
            } else if self.shared.samples.append(
                sample_rows,
                sample_bytes,
                self.shared.runtime.batch_bytes,
            ) {
                self.shared.samples_notify.notify_one();
            }
        }

        if !stream_rows.is_empty() {
            self.shared
                .metrics
                .stream_registrations_total
                .fetch_add(stream_rows.len() as u64, Ordering::Relaxed);
            if with_waiters {
                let (should_notify, rx) = self.shared.streams.append_and_wait(
                    stream_rows,
                    stream_bytes,
                    self.shared.runtime.batch_bytes,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.streams_notify.notify_one();
                }
            } else if self.shared.streams.append(
                stream_rows,
                stream_bytes,
                self.shared.runtime.batch_bytes,
            ) {
                self.shared.streams_notify.notify_one();
            }
        }

        Ok(receivers)
    }

    /// A point-in-time metrics snapshot (`/metrics` exposition is the
    /// server's job, architect plan "out of scope"; this crate only
    /// maintains the atomics).
    pub fn metrics(&self) -> WriterMetricsSnapshot {
        self.shared
            .metrics
            .snapshot(self.shared.queued_bytes.load(Ordering::Relaxed))
    }

    /// Graceful shutdown (architect plan amendment 2): stops admitting
    /// immediately (subsequent `admit`/`admit_flush` calls return
    /// `Backpressure`), then drains every open/in-flight generation up to
    /// `deadline`. Any generation still unsettled at the deadline is
    /// force-settled with [`WriteError::ShuttingDown`] through the same
    /// single settle path flush success/failure use. Returns once both
    /// per-table flush tasks have exited — bounded by `deadline` plus
    /// whatever bounded work happens after each task observes the
    /// signal. Idempotent: a second call after the first has completed is
    /// a no-op.
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
    }
}

impl LogSink for LogWriter {
    fn admit(&self, batch: ParsedLogs) -> Result<(), Backpressure> {
        self.admit_batch(batch, false).map(|_| ())
    }

    fn admit_flush(&self, batch: ParsedLogs) -> Result<FlushWait, Backpressure> {
        let receivers = self.admit_batch(batch, true)?;
        Ok(FlushWait::new(async move {
            join_generations(receivers)
                .await
                .map_err(|e| LogsIngestError::FlushFailed(e.to_string()))
        }))
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
