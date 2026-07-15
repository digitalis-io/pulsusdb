//! `TraceWriter`: the two-table trace writer core (issue #54), wired for
//! `trace_spans` and `trace_attrs_idx` (docs/schemas.md §4.1). Implements
//! [`crate::ingest::traces::TraceSink`] — structurally
//! [`crate::writer::MetricWriter`] minus the registration/metadata caches:
//! spans are never deduplicated (every admitted span/attr row is written),
//! and `trace_tag_catalog` is populated by the T1 materialized view, never
//! by this writer, so neither table carries an `on_flush_success` hook.
//!
//! **Consistency model**: `trace_spans` and `trace_attrs_idx` flush
//! independently on two separate generations — no cross-table atomic
//! insert (the same eventual-consistency model `LogWriter`/`MetricWriter`'s
//! module docs accept). The `join_generations` wait guarantees a sync
//! caller never receives a false success: `admit_flush`'s `200` resolves
//! only once this admission's spans *and* attrs generations are both
//! durable, or it gets an `Err`. A concurrent reader can still observe a
//! span durable without its attr rows during the settle window — legal;
//! the TraceQL read path (T4+) intersects the index against the payload
//! table and tolerates a lagging index row exactly as the log path
//! tolerates a lagging stream registration.
//!
//! **Backpressure/shutdown**: identical shape to `LogWriter`'s — see its
//! module doc for the byte-reservation and drain/force-settle semantics,
//! shared here across two tables via one `queued_bytes` counter.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pulsus_clickhouse::ChClient;
use pulsus_config::WriterConfig;
use tokio::sync::{Notify, oneshot};
use tracing::warn;

use crate::error::LogsIngestError;
use crate::ingest::traces::{ParsedTraces, TraceSink};
use crate::ingest::{Backpressure, FlushWait};
use crate::writer::buffer;
use crate::writer::config::WriterRuntime;
use crate::writer::error::WriteError;
use crate::writer::metrics::{TraceWriterMetrics, TraceWriterMetricsSnapshot};
use crate::writer::rows::{TraceAttrRow, TraceSpanRow};
use crate::writer::spool;
use crate::writer::table::{self, BlockInserter, ChBlockInserter, ShutdownSignal, TableContext};

const SPANS_TABLE: &str = "trace_spans";
const ATTRS_TABLE: &str = "trace_attrs_idx";

/// The two target table names a [`TraceWriter`] inserts into (docs/
/// schemas.md §4.1/§7, mirroring [`crate::writer::WriterTables`]'s issue
/// #15 `_dist`-awareness): cluster-mode deployments write both
/// `trace_spans` and `trace_attrs_idx` through their `_dist` wrappers
/// (`Family::Traces`, sharded by `cityHash64(trace_id)`).
/// `trace_tag_catalog` is deliberately absent — it is MV-populated, never
/// writer-written. `Arc<str>` (not `&'static str`): the cluster-suffixed
/// names are computed once at server startup from `Config`, not known at
/// compile time.
#[derive(Debug, Clone)]
pub struct TraceWriterTables {
    pub spans: Arc<str>,
    pub attrs: Arc<str>,
}

impl TraceWriterTables {
    /// Unclustered defaults: the bare local table names. Every existing
    /// caller (`new`/`with_inserters_with_tables` tests) delegates here so
    /// single-node behavior is the default.
    pub fn traces_default() -> Self {
        TraceWriterTables {
            spans: Arc::from(SPANS_TABLE),
            attrs: Arc::from(ATTRS_TABLE),
        }
    }
}

struct Shared {
    spans: Arc<buffer::TableBuffer<TraceSpanRow>>,
    attrs: Arc<buffer::TableBuffer<TraceAttrRow>>,
    spans_notify: Arc<Notify>,
    attrs_notify: Arc<Notify>,
    queued_bytes: Arc<AtomicU64>,
    runtime: Arc<WriterRuntime>,
    metrics: Arc<TraceWriterMetrics>,
    shutdown: ShutdownSignal,
    shutting_down: AtomicBool,
    spans_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    attrs_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Implements issue #54's `TraceSink` over the generic per-table columnar
/// writer core. See the module-level docs above.
pub struct TraceWriter {
    shared: Arc<Shared>,
}

impl TraceWriter {
    /// Production constructor: batches and flushes through a real
    /// ClickHouse connection, against the unclustered default table names
    /// ([`TraceWriterTables::traces_default`]).
    pub fn new(client: Arc<ChClient>, cfg: &WriterConfig) -> Self {
        Self::new_with_tables(client, cfg, TraceWriterTables::traces_default())
    }

    /// [`Self::new`], but against `tables` — the server's cluster-aware
    /// constructor for `_dist` table names (docs/schemas.md §7).
    pub fn new_with_tables(
        client: Arc<ChClient>,
        cfg: &WriterConfig,
        tables: TraceWriterTables,
    ) -> Self {
        let inserter: Arc<ChBlockInserter> = Arc::new(ChBlockInserter::new(client));
        Self::with_inserters_with_tables(inserter.clone(), inserter, cfg, tables)
    }

    /// Test/mock constructor: any [`BlockInserter`] pair — e.g. a
    /// scriptable mock that can fail/hang on demand — against `tables`.
    pub fn with_inserters_with_tables(
        spans_inserter: Arc<dyn BlockInserter<TraceSpanRow>>,
        attrs_inserter: Arc<dyn BlockInserter<TraceAttrRow>>,
        cfg: &WriterConfig,
        tables: TraceWriterTables,
    ) -> Self {
        let runtime = Arc::new(WriterRuntime::from_config(cfg));
        let metrics = Arc::new(TraceWriterMetrics::default());
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let spool = Arc::new(spool::SpoolWriter::new(
            runtime.spool_dir.clone(),
            metrics.clone(),
        ));
        let (shutdown, shutdown_rx) = ShutdownSignal::new();

        let spans = Arc::new(buffer::TableBuffer::new());
        let attrs = Arc::new(buffer::TableBuffer::new());
        let spans_notify = Arc::new(Notify::new());
        let attrs_notify = Arc::new(Notify::new());

        // No `on_flush_success` hook on either table: nothing to promote —
        // spans are never deduplicated and `trace_tag_catalog` is
        // MV-populated (issue #53), so the writer holds no caches.
        let spans_ctx = TableContext {
            table: tables.spans,
            buffer: spans.clone(),
            notify: spans_notify.clone(),
            inserter: spans_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.spans.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: None,
        };
        let attrs_ctx = TableContext {
            table: tables.attrs,
            buffer: attrs.clone(),
            notify: attrs_notify.clone(),
            inserter: attrs_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.attrs.clone(),
            spool,
            queued_bytes: queued_bytes.clone(),
            on_flush_success: None,
        };

        let spans_task = table::spawn(spans_ctx, shutdown_rx.clone());
        let attrs_task = table::spawn(attrs_ctx, shutdown_rx);

        let shared = Arc::new(Shared {
            spans,
            attrs,
            spans_notify,
            attrs_notify,
            queued_bytes,
            runtime,
            metrics,
            shutdown,
            shutting_down: AtomicBool::new(false),
            spans_task: Mutex::new(Some(spans_task)),
            attrs_task: Mutex::new(Some(attrs_task)),
        });

        TraceWriter { shared }
    }

    /// Admits `batch`, appending to the spans/attrs buffers under one
    /// atomic byte reservation. `with_waiters` selects sync- vs async-mode
    /// admission, mirroring `LogWriter::admit_batch`.
    fn admit_batch(
        &self,
        batch: ParsedTraces,
        with_waiters: bool,
    ) -> Result<Vec<oneshot::Receiver<Result<(), WriteError>>>, Backpressure> {
        if self.shared.shutting_down.load(Ordering::Acquire) {
            return Err(Backpressure);
        }

        self.shared
            .metrics
            .rejected_total
            .fetch_add(batch.rejected, Ordering::Relaxed);

        // Reserve-before-materialize (mirrors `LogWriter::admit_batch`):
        // estimate bytes straight off the source records before cloning
        // anything into the target row shapes.
        let span_bytes: u64 = batch.spans.iter().map(TraceSpanRow::est_source_bytes).sum();
        let attr_bytes: u64 = batch.attrs.iter().map(TraceAttrRow::est_source_bytes).sum();
        let total_bytes = span_bytes + attr_bytes;

        // Atomic reservation (mirrors `LogWriter::admit_batch`): reserve
        // first, roll back on overflow.
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
            self.shared
                .queued_bytes
                .fetch_sub(total_bytes, Ordering::AcqRel);
            return Err(Backpressure);
        }

        // Reservation secured: only now materialize the target rows.
        let span_rows: Vec<TraceSpanRow> = batch.spans.iter().map(TraceSpanRow::from).collect();
        let attr_rows: Vec<TraceAttrRow> = batch.attrs.iter().map(TraceAttrRow::from).collect();

        let mut receivers = Vec::new();

        if !span_rows.is_empty() {
            if with_waiters {
                let (should_notify, rx) = self.shared.spans.append_and_wait(
                    span_rows,
                    span_bytes,
                    self.shared.runtime.batch_bytes,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.spans_notify.notify_one();
                }
            } else if self.shared.spans.append(
                span_rows,
                span_bytes,
                self.shared.runtime.batch_bytes,
            ) {
                self.shared.spans_notify.notify_one();
            }
        }

        if !attr_rows.is_empty() {
            if with_waiters {
                let (should_notify, rx) = self.shared.attrs.append_and_wait(
                    attr_rows,
                    attr_bytes,
                    self.shared.runtime.batch_bytes,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.attrs_notify.notify_one();
                }
            } else if self.shared.attrs.append(
                attr_rows,
                attr_bytes,
                self.shared.runtime.batch_bytes,
            ) {
                self.shared.attrs_notify.notify_one();
            }
        }

        Ok(receivers)
    }

    /// A point-in-time metrics snapshot.
    pub fn metrics(&self) -> TraceWriterMetricsSnapshot {
        self.shared
            .metrics
            .snapshot(self.shared.queued_bytes.load(Ordering::Relaxed))
    }

    /// Graceful shutdown, mirroring [`crate::writer::LogWriter::shutdown`]:
    /// stops admitting immediately (subsequent `admit`/`admit_flush` calls
    /// return `Backpressure`), then drains every open/in-flight generation
    /// up to `deadline`. Idempotent.
    pub async fn shutdown(&self, deadline: Duration) {
        self.shared.shutting_down.store(true, Ordering::Release);
        self.shared.shutdown.begin(Instant::now() + deadline);

        let spans_task = self
            .shared
            .spans_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();
        let attrs_task = self
            .shared
            .attrs_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();

        if let Some(task) = spans_task
            && let Err(e) = task.await
        {
            warn!(error = %e, table = SPANS_TABLE, "flush task panicked during shutdown");
        }
        if let Some(task) = attrs_task
            && let Err(e) = task.await
        {
            warn!(error = %e, table = ATTRS_TABLE, "flush task panicked during shutdown");
        }
    }
}

impl TraceSink for TraceWriter {
    fn admit(&self, batch: ParsedTraces) -> Result<(), Backpressure> {
        self.admit_batch(batch, false).map(|_| ())
    }

    fn admit_flush(&self, batch: ParsedTraces) -> Result<FlushWait, Backpressure> {
        let receivers = self.admit_batch(batch, true)?;
        Ok(FlushWait::new(async move {
            super::join_generations(receivers)
                .await
                .map_err(|e| LogsIngestError::FlushFailed(e.to_string()))
        }))
    }
}
