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

use pulsus_clickhouse::{ChClient, QuerySettings};
use pulsus_config::WriterConfig;
use tokio::sync::Notify;
use tracing::warn;

use crate::error::LogsIngestError;
use crate::ingest::traces::{ParsedTraceLanding, ParsedTraces, TraceSink};
use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};
use crate::writer::backfill::{self, RegistrationBacklog};
use crate::writer::buffer;
use crate::writer::config::WriterRuntime;
use crate::writer::drain::DrainBoundary;
use crate::writer::metrics::{
    TraceLandingSnapshot, TraceWriterMetrics, TraceWriterMetricsSnapshot,
};
use crate::writer::push_dedup::PushDedup;
use crate::writer::push_dedup::{self, Admission, ClaimTicket};
use crate::writer::rows::{TraceAttrRow, TraceLandingRow, TraceSpanRow};
use crate::writer::spool;
use crate::writer::table::{self, BlockInserter, ChBlockInserter, ShutdownSignal, TableContext};
use crate::writer::trace_landing::{self, TraceLandingPath};
use crate::writer::{AdmitMode, Admitted, Suppressed, note_target};

const SPANS_TABLE: &str = "trace_spans";
const ATTRS_TABLE: &str = "trace_attrs_idx";

/// The settings every insert into `trace_spans`/`trace_spans_dist` carries
/// (issue #560): a repeated identical span block is dropped by the derived
/// trace tables whatever the server profile says. A span row carries its
/// own `span_id`, so two byte-identical span blocks are the same spans.
pub(crate) fn span_insert_settings() -> QuerySettings {
    QuerySettings::deduplicate_through_views()
}

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
    /// The landing table (issue #586). No `_dist` wrapper in any mode: a
    /// push carries many trace ids, so inserting the push itself through one
    /// would split one push per shard. The routing happens one step later,
    /// on the way out of the two per-trace views.
    pub landing: Arc<str>,
}

impl TraceWriterTables {
    /// Unclustered defaults: the bare local table names. Every existing
    /// caller (`new`/`with_inserters_with_tables` tests) delegates here so
    /// single-node behavior is the default.
    pub fn traces_default() -> Self {
        TraceWriterTables {
            spans: Arc::from(SPANS_TABLE),
            attrs: Arc::from(ATTRS_TABLE),
            landing: Arc::from(trace_landing::LANDING_TABLE),
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
    attrs_backfill_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The landing path (issue #586): its queue, its insert workers and its
    /// own byte counter. Additional to the two tables above, which keep
    /// writing until task 20.
    landing: TraceLandingPath,
    /// Issue #494's per-signal push-suppression index, extended to the trace
    /// push by issue #586. `None` while `PULSUS_INGEST_DEDUP` is off.
    dedup: Option<Arc<PushDedup>>,
    /// The landing half's shutdown boundary: the admission gate, the
    /// announced deadline and every task that half spawned
    /// (`writer::drain`). The two old flush tasks and the backfill task keep
    /// their own [`ShutdownSignal`] above.
    boundary: DrainBoundary,
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
        // Issue #560: only the span table's inserts carry the deduplication
        // pins; the attribute index keeps the default settings.
        let spans_inserter: Arc<ChBlockInserter> = Arc::new(ChBlockInserter::with_settings(
            client.clone(),
            span_insert_settings(),
        ));
        let attrs_inserter: Arc<ChBlockInserter> = Arc::new(ChBlockInserter::new(client.clone()));
        let landing_inserter = TraceLandingPath::production_inserter(client);
        Self::with_inserters_with_tables(
            spans_inserter,
            attrs_inserter,
            landing_inserter,
            cfg,
            tables,
        )
    }

    /// Test/mock constructor: any [`BlockInserter`] trio — e.g. a
    /// scriptable mock that can fail/hang on demand — against `tables`.
    pub fn with_inserters_with_tables(
        spans_inserter: Arc<dyn BlockInserter<TraceSpanRow>>,
        attrs_inserter: Arc<dyn BlockInserter<TraceAttrRow>>,
        landing_inserter: Arc<dyn BlockInserter<TraceLandingRow>>,
        cfg: &WriterConfig,
        tables: TraceWriterTables,
    ) -> Self {
        Self::with_inserters_and_runtime(
            spans_inserter,
            attrs_inserter,
            landing_inserter,
            WriterRuntime::from_config(cfg),
            tables,
        )
    }

    /// [`Self::with_inserters_with_tables`], but against a caller-supplied
    /// [`WriterRuntime`] rather than one derived from a `&WriterConfig`. The
    /// seam exists because `spool_dir` is set from a constant, so nothing
    /// outside this crate could otherwise point a spool anywhere; the
    /// constructors above build the runtime themselves and delegate here, so
    /// no production call site and no production behaviour depends on it.
    #[doc(hidden)]
    pub fn with_inserters_and_runtime(
        spans_inserter: Arc<dyn BlockInserter<TraceSpanRow>>,
        attrs_inserter: Arc<dyn BlockInserter<TraceAttrRow>>,
        landing_inserter: Arc<dyn BlockInserter<TraceLandingRow>>,
        runtime: WriterRuntime,
        tables: TraceWriterTables,
    ) -> Self {
        let runtime = Arc::new(runtime);
        let metrics = Arc::new(TraceWriterMetrics::default());
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let spool = Arc::new(spool::SpoolWriter::new(
            runtime.spool_dir.clone(),
            metrics.clone(),
        ));
        let (shutdown, shutdown_rx) = ShutdownSignal::new();
        let boundary = DrainBoundary::new();

        // Issue #494's index, built from `WriterRuntime` exactly as
        // `writer::metric`'s is, so no constructor signature changes and no
        // `TraceWriter::new*` call site moves.
        let dedup = runtime.ingest_dedup.then(|| {
            PushDedup::new(
                runtime.ingest_dedup_max_bytes,
                runtime.ingest_dedup_window,
                runtime.claim_deadline,
            )
        });

        // **The push's claim records three targets** (issue #586): the two
        // old tables, which count toward the acknowledgement, and the
        // landing block, which does not. A claim over the landing block
        // alone would store twice — a push whose landing insert failed and
        // whose two old-path inserts committed would leave the claim
        // released, so the client's retry would be admitted and the old
        // path would store it a second time.
        let spans = Arc::new(buffer::TableBuffer::with_dedup(dedup.clone(), true));
        let attrs = Arc::new(buffer::TableBuffer::with_dedup(dedup.clone(), true));
        let spans_notify = Arc::new(Notify::new());
        let attrs_notify = Arc::new(Notify::new());

        // `trace_attrs_idx`'s Poisoned-only registration backfill (issue
        // #139): a definitely-failed attr-index flush enqueues its rows
        // for the 5s re-insert cadence — without it, a span whose attrs
        // insert failed is fetchable by ID but invisible to
        // attribute-scoped search forever (traces have no LRU, so no
        // later re-registration exists). `trace_spans` keeps
        // `on_flush_poisoned: None` — the structural append-only #9
        // exclusion.
        let attrs_backlog = Arc::new(Mutex::new(RegistrationBacklog::<TraceAttrRow>::new(
            runtime.backfill_max_bytes,
        )));
        let attrs_backlog_for_hook = attrs_backlog.clone();
        let attrs_backfill_metrics = metrics.attrs_backfill.clone();
        let on_attrs_flush_poisoned: table::FlushPoisonedHook<TraceAttrRow> =
            Arc::new(move |rows: &[TraceAttrRow]| {
                backfill::enqueue_failed(&attrs_backlog_for_hook, &attrs_backfill_metrics, rows);
            });
        let attrs_inserter_for_backfill = attrs_inserter.clone();
        let attrs_table_for_backfill = tables.attrs.clone();

        // No `on_flush_success` hook on either table: nothing to promote —
        // spans are never deduplicated and `trace_tag_catalog` is
        // MV-populated (issue #53), so the writer holds no caches (and
        // the backfill's `on_healed` is likewise `None`).
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
            on_flush_poisoned: None,
            // This field drives `tick()` alone, and the landing path's
            // `spawn_dedup_ticker` does that on a task of its own (issue
            // #586). This table's claim is on its buffer, built with
            // `with_dedup`.
            dedup: None,
        };
        let attrs_ctx = TableContext {
            table: tables.attrs,
            buffer: attrs.clone(),
            notify: attrs_notify.clone(),
            inserter: attrs_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.attrs.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: None,
            on_flush_poisoned: Some(on_attrs_flush_poisoned),
            // This field drives `tick()` alone, and the landing path's
            // `spawn_dedup_ticker` does that on a task of its own (issue
            // #586). This table's claim is on its buffer, built with
            // `with_dedup`.
            dedup: None,
        };

        let landing = TraceLandingPath::new(
            tables.landing,
            landing_inserter,
            runtime.clone(),
            metrics.clone(),
            spool,
            dedup.clone(),
            &boundary,
        );

        let spans_task = table::spawn(spans_ctx, shutdown_rx.clone());
        let attrs_task = table::spawn(attrs_ctx, shutdown_rx.clone());
        let attrs_backfill_task = backfill::spawn_backfill(
            attrs_backlog,
            attrs_inserter_for_backfill,
            attrs_table_for_backfill,
            None, // traces have no cache — nothing to promote on heal
            metrics.attrs_backfill.clone(),
            runtime.clone(),
            shutdown_rx,
        );

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
            attrs_backfill_task: Mutex::new(Some(attrs_backfill_task)),
            landing,
            dedup,
            boundary,
        });

        TraceWriter { shared }
    }

    /// Admits `batch` onto the old two-table path **and** `landing` as one
    /// sealed landing block, under one joint admission (issue #586).
    ///
    /// **Admission is the one place the two paths are coupled, and it has to
    /// be.** The order is the suppression index, then both paths' ceilings —
    /// where either refusal refuses the whole push and stores nothing on
    /// either path — then the old path's two buffered inserts and the new
    /// path's landing block. Admitting the old path and silently dropping
    /// what will not fit one landing block would leave the new tables short
    /// behind a `200`.
    ///
    /// **The two byte counters are separate and the order they are taken in
    /// is load-bearing**: the old path's reservation is taken first, so a
    /// refusal between the two rolls that one back and never the landing
    /// one — which is why nothing but `LandingBlock::release` ever subtracts
    /// the landing counter.
    fn admit_batch(
        &self,
        batch: ParsedTraces,
        landing: ParsedTraceLanding,
        mode: AdmitMode,
        push: PushHeaders,
    ) -> Result<Admitted, AdmitRefusal> {
        if self.shared.shutting_down.load(Ordering::Acquire) {
            return Err(AdmitRefusal::Backpressure);
        }
        // The landing half's admission gate. The pass is held for the whole
        // of this body, which never awaits, and `shutdown` waits for every
        // outstanding pass before it announces the deadline — so a block
        // admitted here is in the queue before the drain can close it.
        let Some(pass) = self.shared.boundary.enter() else {
            return Err(AdmitRefusal::Backpressure);
        };

        // Every landing row of a push carries the same stamp, so the block
        // lies in one landing partition.
        let received_ms = now_unix_millis();
        // The landing budget's anchor, taken before the claim and before any
        // admission work, because the claim deadline it must settle inside of
        // starts here too.
        let admitted_at = tokio::time::Instant::now();

        // Issue #494's claim, extended to the trace push. Taken before
        // either byte reservation, so a request refused by backpressure
        // leaves no claim behind.
        //
        // **A suppressed trace push sends nothing at all.** Unlike metrics
        // there is no descriptor exception: nothing on this path is emitted
        // once and cached, so there is nothing a suppressed push still owes.
        let mut suppressed: Option<Suppressed> = None;
        let mut guard = match &self.shared.dedup {
            Some(dedup) => {
                let id = push_dedup::trace_identity(&batch, &push);
                let rows = landing.spans.len() as u64;
                match dedup.admit(id, mode.wait_mode()) {
                    Admission::Admit(guard) => Some(guard),
                    Admission::SuppressedSettled(outcome) => {
                        dedup.count_suppressed(id.declared_retry, rows);
                        suppressed = Some(Suppressed::Settled(outcome));
                        None
                    }
                    Admission::SuppressedPending { guard, rx } => {
                        dedup.count_suppressed(id.declared_retry, rows);
                        suppressed = Some(Suppressed::Pending(guard, rx));
                        None
                    }
                    Admission::KeyReused => return Err(AdmitRefusal::KeyReused),
                    Admission::Shed => return Err(AdmitRefusal::DedupShed),
                    Admission::WaitShed => return Err(AdmitRefusal::DedupWaitShed),
                }
            }
            None => None,
        };

        // The three per-push ceilings, counted over the landing batch's own
        // four kinds and decided **before** either path queues anything.
        // Returning here drops the un-sealed guard, which removes the claim,
        // so the client's retry of an unstored push is stored rather than
        // suppressed — and no bytes have been reserved yet.
        //
        // **A valid push with no span makes no block and no insert** and is
        // charged for nothing: one block's fixed overhead exceeds the
        // smallest accepted value of either byte limit, so charging an empty
        // push for a block that is never created would refuse it.
        let landing_rows = landing.total_rows();
        let landing_bytes = trace_landing::charge_of(&landing);
        if landing_rows > 0 {
            // Traces' own ceiling: `spans`, `traces` and `resources` are
            // partitioned by UTC day, so one view's insert produces one part
            // per day in the push and the server throws above
            // `max_partitions_per_insert_block`.
            let days = trace_landing::distinct_span_days(&landing);
            if days > trace_landing::TRACE_LANDING_DAY_LIMIT {
                return Err(AdmitRefusal::PushSpansTooManyDays {
                    days,
                    day_limit: trace_landing::TRACE_LANDING_DAY_LIMIT,
                });
            }
            if landing_rows >= self.shared.runtime.trace_landing_max_rows
                || landing_bytes > self.shared.runtime.batch_bytes
            {
                return Err(AdmitRefusal::PushTooLarge {
                    rows: landing_rows,
                    row_limit: self.shared.runtime.trace_landing_max_rows,
                    bytes: landing_bytes,
                    byte_limit: self.shared.runtime.batch_bytes,
                });
            }
        }

        // The suppressed push stops here, having stored nothing on either
        // path, and is answered what the original caller was told — which is
        // the old path's answer.
        if let Some(suppressed) = suppressed {
            return Ok(Admitted::Suppressed(suppressed));
        }

        self.shared
            .metrics
            .rejected_total
            .fetch_add(batch.rejected, Ordering::Relaxed);

        // Reserve-before-materialize: estimate bytes straight off the source
        // records before cloning anything into the target row shapes.
        let span_bytes: u64 = batch.spans.iter().map(TraceSpanRow::est_source_bytes).sum();
        let attr_bytes: u64 = batch.attrs.iter().map(TraceAttrRow::est_source_bytes).sum();
        let total_bytes = span_bytes + attr_bytes;

        // The OLD path's reservation, first (see this function's own note).
        super::reserve_queued_bytes(
            &self.shared.queued_bytes,
            &self.shared.metrics.backpressure_total,
            total_bytes,
            self.shared.runtime.queue_bytes_limit,
        )
        .map_err(AdmitRefusal::from)?;

        if self.shared.shutting_down.load(Ordering::Acquire) {
            self.shared
                .queued_bytes
                .fetch_sub(total_bytes, Ordering::AcqRel);
            return Err(AdmitRefusal::Backpressure);
        }

        // The LANDING path's reservation, second. Either path's
        // backpressure refuses the whole push: a push the old path would
        // have accepted is answered `429` when only the landing queue is
        // saturated, and the converse is today's behaviour.
        if landing_rows > 0
            && let Err(backpressure) = self.shared.landing.reserve(landing_bytes)
        {
            self.shared
                .queued_bytes
                .fetch_sub(total_bytes, Ordering::AcqRel);
            return Err(AdmitRefusal::from(backpressure));
        }

        // Both reservations secured: only now materialize any row.
        let span_rows: Vec<TraceSpanRow> = batch.spans.iter().map(TraceSpanRow::from).collect();
        let attr_rows: Vec<TraceAttrRow> = batch.attrs.iter().map(TraceAttrRow::from).collect();
        let claim_key = guard.as_ref().map(|g| g.key());

        let mut receivers = Vec::new();

        // **The two old tables count toward the acknowledgement.**
        if !span_rows.is_empty() {
            if mode == AdmitMode::Sync {
                let (should_notify, _generation, rx) = self.shared.spans.append_and_wait(
                    span_rows,
                    span_bytes,
                    self.shared.runtime.batch_bytes,
                    claim_key,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.spans_notify.notify_one();
                }
            } else if self
                .shared
                .spans
                .append(
                    span_rows,
                    span_bytes,
                    self.shared.runtime.batch_bytes,
                    claim_key,
                )
                .0
            {
                self.shared.spans_notify.notify_one();
            }
            note_target(&mut guard, true);
        }

        if !attr_rows.is_empty() {
            if mode == AdmitMode::Sync {
                let (should_notify, _generation, rx) = self.shared.attrs.append_and_wait(
                    attr_rows,
                    attr_bytes,
                    self.shared.runtime.batch_bytes,
                    claim_key,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.attrs_notify.notify_one();
                }
            } else if self
                .shared
                .attrs
                .append(
                    attr_rows,
                    attr_bytes,
                    self.shared.runtime.batch_bytes,
                    claim_key,
                )
                .0
            {
                self.shared.attrs_notify.notify_one();
            }
            note_target(&mut guard, true);
        }

        // **The landing block does not count toward the acknowledgement**,
        // which is what `counts_for_ack = false` says: the route's answer
        // stays the old path's, byte for byte, so the block carries no
        // waiter and its endings reach the client through nothing. A claim
        // over the landing block alone would store twice — a push whose
        // landing insert failed and whose two old-path inserts committed
        // would leave the claim released, so the client's retry would be
        // admitted and the old path would store it a second time.
        if landing_rows > 0 {
            let mut claim = ClaimTicket::new(self.shared.dedup.clone(), false);
            if let Some(g) = guard.as_ref() {
                claim.push(g.key());
            }
            note_target(&mut guard, false);
            let rows = trace_landing::rows_of(received_ms, landing);
            debug_assert_eq!(
                rows.len() as u64,
                landing_rows,
                "the rows materialized are the rows the push was charged and \
                 measured for"
            );
            self.shared.landing.queue(
                &pass,
                &self.shared.boundary,
                rows,
                landing_bytes,
                claim,
                admitted_at,
            );
        }

        // Arms the claim: from here a drop no longer removes it, and the
        // target set is closed.
        if let Some(guard) = guard {
            guard.seal();
        }
        Ok(Admitted::Stored(receivers))
    }

    /// A point-in-time metrics snapshot. `queue_bytes` is the **old** path's
    /// counter; the landing path's is [`Self::landing_metrics`].
    pub fn metrics(&self) -> TraceWriterMetricsSnapshot {
        self.shared.metrics.snapshot(
            self.shared.queued_bytes.load(Ordering::Relaxed),
            self.shared
                .dedup
                .as_ref()
                .map(|d| d.snapshot())
                .unwrap_or_default(),
        )
    }

    /// The landing path's own figures, which no exported series carries
    /// (issue #586, `TraceLandingSnapshot`).
    pub fn landing_metrics(&self) -> TraceLandingSnapshot {
        self.shared.landing.snapshot()
    }

    /// This writer's push-suppression index (issue #494), or `None` while
    /// `PULSUS_INGEST_DEDUP` is off.
    pub fn dedup(&self) -> Option<&Arc<PushDedup>> {
        self.shared.dedup.as_ref()
    }

    /// Graceful shutdown, mirroring [`crate::writer::LogWriter::shutdown`]:
    /// stops admitting immediately (subsequent `admit`/`admit_flush` calls
    /// return `Backpressure`), then drains every open/in-flight generation
    /// up to `deadline`. Idempotent.
    /// Re-opens the landing half's admission gate after [`Self::shutdown`]
    /// has returned, so the next admission takes a pass and meets the closed
    /// queue. Nothing in the server calls this.
    #[doc(hidden)]
    pub fn reopen_admission_for_test(&self) {
        self.shared.boundary.reopen_for_test();
    }

    pub async fn shutdown(&self, deadline: Duration) {
        self.shared.shutting_down.store(true, Ordering::Release);
        self.shared.shutdown.begin(Instant::now() + deadline);
        // The landing half's own boundary, beside the two flush tasks and
        // the backfill task below: admission closes, the admissions inside
        // it finish, the deadline is announced, and every task that half
        // spawned is awaited.
        self.shared.boundary.shutdown(deadline).await;

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
        let attrs_backfill_task = self
            .shared
            .attrs_backfill_task
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
        if let Some(task) = attrs_backfill_task
            && let Err(e) = task.await
        {
            warn!(
                error = %e,
                table = ATTRS_TABLE,
                "registration backfill task panicked during shutdown"
            );
        }
    }
}

impl TraceSink for TraceWriter {
    fn admit(
        &self,
        batch: ParsedTraces,
        landing: ParsedTraceLanding,
        push: PushHeaders,
    ) -> Result<(), AdmitRefusal> {
        self.admit_batch(batch, landing, AdmitMode::Async, push)
            .map(|_| ())
    }

    fn admit_flush(
        &self,
        batch: ParsedTraces,
        landing: ParsedTraceLanding,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
        match self.admit_batch(batch, landing, AdmitMode::Sync, push)? {
            Admitted::Stored(receivers) => Ok(FlushWait::new(async move {
                super::join_generations(receivers)
                    .await
                    .map_err(|e| LogsIngestError::FlushFailed(e.to_string()))
            })),
            Admitted::Suppressed(suppressed) => Ok(FlushWait::new(suppressed.into_answer())),
        }
    }
}

/// The writer's UTC epoch-millisecond stamp for one push. A clock before the
/// epoch is a broken-clock scenario, not one that happens on a deployed
/// system; it degrades to `0` rather than panicking, the same way the
/// receivers' own stamp does.
fn now_unix_millis() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #560: the span inserter's settings pin both block
    /// deduplication settings on, so criterion 4 does not depend on the
    /// server profile.
    #[test]
    fn the_span_inserts_pin_block_deduplication_on() {
        let s = span_insert_settings();
        let mut mismatches: Vec<(&str, Option<&str>, Option<&str>)> = Vec::new();
        for (key, want) in [
            ("deduplicate_insert", "enable"),
            ("deduplicate_blocks_in_dependent_materialized_views", "1"),
        ] {
            let got = s.get(key);
            if got != Some(want) {
                mismatches.push((key, Some(want), got));
            }
        }
        assert!(mismatches.is_empty(), "mismatches: {mismatches:?}");
    }
}
