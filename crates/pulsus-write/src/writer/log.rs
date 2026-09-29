//! `LogWriter`: the logs write path. Implements issue #8's
//! [`crate::ingest::LogSink`] seam.
//!
//! **One push is one `INSERT` of one block into `log_landing`** (issue #603).
//! Five materialized views maintain `log_samples`, `log_streams`,
//! `log_streams_idx`, `log_metrics_<res>` and `log_patterns` from that one
//! source table, and the writer inserts into none of them. It replaces three
//! independently flushed table buffers, where some of a push's tables could
//! commit and others time out.
//!
//! **Three kinds, five views.** `log_streams_idx_mv` is an `ARRAY JOIN` over
//! the very same landed kind-1 row `log_streams_mv` takes, so it reads that
//! kind rather than one of its own — a kind of its own would make the writer
//! land the label blob twice per stream. No view's source is written by a
//! view.
//!
//! **The pattern rows are pre-aggregated before they land.** Template
//! extraction is Rust, not SQL (`crate::patterns`), so the writer lands one
//! row per `(fingerprint, bucket, template)` and `log_patterns_mv` copies it
//! through.
//!
//! A push's rows are never spread over two inserts and an insert never carries
//! two pushes. A push too large for one block is refused whole
//! ([`AdmitRefusal::PushTooLarge`]), never split — two blocks can commit a
//! prefix, and a prefix is not all-or-nothing.
//!
//! **Retry safety** is the `insert_deduplication_token` the writer mints per
//! sealed block and repeats byte-identically on every resend, so a resend of
//! a block the server already accepted stores nothing twice while the tables'
//! deduplication windows still hold it. The token is never derived from the
//! content: two byte-identical log blocks can be two genuine pushes.
//!
//! **Registration**: `StreamLru` promotion stays success-only — the LRU is
//! promoted when the block commits, never at admission — keyed
//! `(fingerprint, month)`. A registration is a kind-1 row inside the lines'
//! own block rather than its own insert, so there is no "lines committed,
//! registration lost" orphan left to heal and **no registration backfill on
//! this path** (issue #134's mechanism goes with it).
//!
//! **Backpressure/shutdown**: `queued_bytes` is reserved atomically at
//! admission and released exactly once, by whichever ending settles the block,
//! through `writer::landing`'s `LandingBlock::release` — which owns when, and
//! is the only place either signal subtracts. What is reserved is the whole
//! block's cost, not its rows' alone, and a push with no rows makes no block
//! and is charged for none. Every ending but a commit spools the block — it is
//! the push's only copy — reports the claim its fate, and answers a waiting
//! sync caller `500`.
//!
//! **The shutdown boundary is `writer::drain`'s**, whole. This module asks the
//! boundary for nothing but an admission pass; the loop that asks it for an
//! attempt is `writer::landing`'s.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChClient, QuerySettings};
use pulsus_config::WriterConfig;
use tokio::sync::{mpsc, oneshot};

use crate::error::LogsIngestError;
use crate::ingest::{AdmitRefusal, FlushWait, LogSink, PushHeaders};
use crate::patterns::{
    AGG_BASE_OVERHEAD, MAX_DISTINCT_PATTERNS_PER_BATCH, PATTERN_ROW_OVERHEAD, aggregate_patterns,
    est_template_bound,
};
use crate::protocols::otlp_logs::{ParsedLogs, StreamRow};
use crate::writer::config::WriterRuntime;
use crate::writer::drain::{AdmissionPass, DrainBoundary};
use crate::writer::error::WriteError;
use crate::writer::landing::{
    LandingBlock, LandingContext, LandingFate, MSG_SHUTDOWN_QUEUED, TerminalCause, landing_charge,
    mint_landing_token, settle_block, spawn_landing_worker,
};
use crate::writer::metrics::{WriterMetrics, WriterMetricsSnapshot};
use crate::writer::push_dedup::{self, Admission, ClaimTicket, PushDedup};
use crate::writer::registration::StreamLru;
use crate::writer::rows::{LogLandingRow, LogSampleRow, LogStreamRow};
use crate::writer::spool::SpoolWriter;
use crate::writer::table::{BlockInserter, ChBlockInserter, XorShift64, spawn_dedup_ticker};
use crate::writer::{AdmitMode, Admitted, Suppressed, note_target};

/// The one table the logs writer inserts into (issue #603).
const LANDING_TABLE: &str = "log_landing";

/// The table name a [`LogWriter`] inserts into. One name: the five target
/// tables are maintained by materialized view and the writer never names
/// them (issue #603). The landing table has no `Distributed` wrapper, so
/// this is the base name in every mode — `Arc<str>` because the server
/// resolves it once at startup from `Config` rather than at compile time.
#[derive(Debug, Clone)]
pub struct WriterTables {
    pub landing: Arc<str>,
}

impl WriterTables {
    /// The default: the bare local table name.
    pub fn logs_default() -> Self {
        WriterTables {
            landing: Arc::from(LANDING_TABLE),
        }
    }
}

/// The `StreamLru` keys a committed block registers, derived from the block's
/// own kind-1 rows rather than carried beside them: such a row holds the
/// fingerprint and the month, which is the whole key. Nothing is retained for
/// a block that never commits, and what a queued block holds does not grow
/// with the number of streams a push registers.
fn promotion_keys(
    rows: &[LogLandingRow],
) -> impl Iterator<Item = (pulsus_model::Fingerprint, u16)> {
    rows.iter()
        .filter(|row| row.kind == LogLandingRow::KIND_STREAM)
        .map(|row| (row.fingerprint, row.month))
}

struct Shared {
    /// The landing queue's sender. Bounded by **bytes, not by length**:
    /// [`landing_charge`] runs through `reserve_queued_bytes` before a block is
    /// sent and is the only gate, so what the queue holds is bounded by
    /// `PULSUS_INGEST_QUEUE_BYTES`, whatever number of blocks that comes to.
    landing_tx: mpsc::UnboundedSender<LandingBlock<LogLandingRow>>,
    ctx: Arc<LandingContext<LogLandingRow>>,
    /// Issue #494's per-signal push-suppression index. `None` while
    /// `PULSUS_INGEST_DEDUP` is off.
    dedup: Option<Arc<PushDedup>>,
    queued_bytes: Arc<AtomicU64>,
    runtime: Arc<WriterRuntime>,
    metrics: Arc<WriterMetrics>,
    lru: Arc<Mutex<StreamLru>>,
    /// The token generator. One lock per admitted push: a token minted from
    /// the clock alone would repeat for two pushes inside one millisecond,
    /// and the second would be dropped as a resend of the first.
    token_rng: Mutex<XorShift64>,
    /// The shutdown boundary: the admission gate, the announced deadline and
    /// every task this writer spawned (`writer::drain`).
    boundary: DrainBoundary,
}

/// Implements issue #8's `LogSink` over one landing table. See the
/// module-level docs above.
pub struct LogWriter {
    shared: Arc<Shared>,
}

impl LogWriter {
    /// Production constructor: inserts through a real ClickHouse connection,
    /// against the default table name.
    pub fn new(client: Arc<ChClient>, cfg: &WriterConfig) -> Self {
        Self::new_with_tables(client, cfg, WriterTables::logs_default())
    }

    /// [`Self::new`], but against `tables`.
    pub fn new_with_tables(
        client: Arc<ChClient>,
        cfg: &WriterConfig,
        tables: WriterTables,
    ) -> Self {
        let inserter: Arc<ChBlockInserter> = Arc::new(ChBlockInserter::new(client));
        Self::with_landing_inserter_and_tables(inserter, cfg, tables)
    }

    /// Test/mock constructor: any [`BlockInserter`] — e.g. a scriptable mock
    /// that can fail or hang on demand — against the default table name.
    pub fn with_landing_inserter(
        landing: Arc<dyn BlockInserter<LogLandingRow>>,
        cfg: &WriterConfig,
    ) -> Self {
        Self::with_landing_inserter_and_tables(landing, cfg, WriterTables::logs_default())
    }

    /// [`Self::with_landing_inserter`], but against `tables`.
    pub fn with_landing_inserter_and_tables(
        landing: Arc<dyn BlockInserter<LogLandingRow>>,
        cfg: &WriterConfig,
        tables: WriterTables,
    ) -> Self {
        Self::with_landing_inserter_and_runtime(landing, WriterRuntime::from_config(cfg), tables)
    }

    /// [`Self::with_landing_inserter_and_tables`], but against a
    /// caller-supplied [`WriterRuntime`] rather than one derived from a
    /// `&WriterConfig`. The seam exists because `spool_dir` is set from a
    /// constant, so nothing outside this crate could otherwise point a spool
    /// anywhere; the two constructors above build the runtime themselves and
    /// delegate here, so no production call site and no production behaviour
    /// depends on it.
    #[doc(hidden)]
    pub fn with_landing_inserter_and_runtime(
        landing: Arc<dyn BlockInserter<LogLandingRow>>,
        runtime: WriterRuntime,
        tables: WriterTables,
    ) -> Self {
        let runtime = Arc::new(runtime);
        let metrics = Arc::new(WriterMetrics::default());
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let spool = Arc::new(SpoolWriter::new(runtime.spool_dir.clone(), metrics.clone()));
        let boundary = DrainBoundary::new();
        let lru = Arc::new(Mutex::new(StreamLru::new(runtime.lru_capacity)));

        // Issue #494: one index per signal. The landing insert is the one
        // target, so it is the one thing a suppressed caller's answer waits
        // on — and `log_patterns`' `counts_for_ack = false` goes with the
        // pattern insert that could fail on its own.
        let dedup = runtime.ingest_dedup.then(|| {
            PushDedup::new(
                runtime.ingest_dedup_max_bytes,
                runtime.ingest_dedup_window,
                runtime.claim_deadline,
            )
        });

        // `log_streams`'s success-only LRU promotion: populated ONLY here,
        // after a confirmed commit — never optimistically at admission. A push
        // whose insert failed emits its registration rows again next time,
        // which is what removed the need for a registration backfill.
        let lru_for_hook = lru.clone();
        let on_commit: crate::writer::landing::CommitHook<LogLandingRow> =
            Arc::new(move |rows: &[LogLandingRow]| {
                let mut guard = lru_for_hook.lock().expect("stream lru mutex poisoned");
                for key in promotion_keys(rows) {
                    guard.insert(key);
                }
            });

        let ctx = Arc::new(LandingContext {
            table: tables.landing,
            inserter: landing,
            runtime: runtime.clone(),
            table_metrics: metrics.landing.clone(),
            queued_bytes: queued_bytes.clone(),
            spool,
            on_commit: Some(on_commit),
            retries: runtime.log_landing_retries,
            max_rows: runtime.log_landing_max_rows,
        });

        let (landing_tx, landing_rx) = mpsc::unbounded_channel::<LandingBlock<LogLandingRow>>();
        // A mutex over the receiver rather than a `Notify` beside a deque,
        // which stores one permit and would leave a second queued block
        // waiting for a later push. A worker holds the lock only while
        // taking a block, so up to `log_landing_inserters` inserts overlap.
        let landing_rx = Arc::new(tokio::sync::Mutex::new(landing_rx));
        // Every task goes into the boundary, which is what joins them: a task
        // this writer spawned and nothing awaits can be cancelled by runtime
        // teardown mid-settlement.
        boundary.track_background(
            (0..runtime.log_landing_inserters.max(1))
                .map(|_| spawn_landing_worker(ctx.clone(), landing_rx.clone(), boundary.watch()))
                .collect::<Vec<_>>(),
        );
        // The suppression index used to be ticked off the flush loops, and
        // there is no flush loop left here. Without this an open claim would
        // never age into a tombstone and a waiting caller would be told
        // nothing at all.
        boundary.track_background(
            dedup
                .clone()
                .map(|d| spawn_dedup_ticker(d, runtime.batch_age, boundary.watch())),
        );

        let shared = Arc::new(Shared {
            landing_tx,
            ctx,
            dedup,
            queued_bytes,
            runtime,
            metrics,
            lru,
            token_rng: Mutex::new(XorShift64::seeded()),
            boundary,
        });

        LogWriter { shared }
    }

    /// Admits `batch` as one sealed landing block under one atomic byte
    /// reservation. `mode` selects sync- versus async-mode admission.
    fn admit_batch(
        &self,
        batch: ParsedLogs,
        mode: AdmitMode,
        push: PushHeaders,
    ) -> Result<Admitted, AdmitRefusal> {
        // The admission gate. The pass is held for the whole of this body,
        // which never awaits, and `shutdown` waits for every outstanding pass
        // before it announces the deadline — so a block admitted here is in
        // the queue before the drain can close it. **This replaces the shipped
        // shutting-down rollback**, which was the one place outside `release`
        // that this admission subtracted the gauge.
        let Some(pass) = self.shared.boundary.enter() else {
            return Err(AdmitRefusal::Backpressure);
        };

        // Every row of a push carries the same stamp, so the block lies in
        // one partition.
        let received_ms = now_unix_millis();
        // The landing budget's anchor, taken before the claim and before any
        // admission work, because the claim deadline it must settle inside of
        // starts here too. A budget measured from the enqueue instead would
        // believe it had its full allowance after admission had already spent
        // part of the deadline.
        let admitted_at = tokio::time::Instant::now();

        // Issue #494: the claim, taken before the byte reservation so a
        // request refused by backpressure leaves no claim behind. A parse that
        // failed a stream's label bounds is never claimed: its admitted subset
        // is not the whole request, so a later identical request is not the
        // same push.
        let mut suppressed: Option<Suppressed> = None;
        let mut guard = match (&self.shared.dedup, batch.stream_errors.is_empty()) {
            (Some(dedup), true) => {
                let id = push_dedup::log_identity(&batch, &push);
                match dedup.admit(id, mode.wait_mode()) {
                    Admission::Admit(guard) => Some(guard),
                    Admission::SuppressedSettled(outcome) => {
                        dedup.count_suppressed(id.declared_retry, batch.rows.len() as u64);
                        suppressed = Some(Suppressed::Settled(outcome));
                        None
                    }
                    Admission::SuppressedPending { guard, rx } => {
                        dedup.count_suppressed(id.declared_retry, batch.rows.len() as u64);
                        suppressed = Some(Suppressed::Pending(guard, rx));
                        None
                    }
                    Admission::KeyReused => return Err(AdmitRefusal::KeyReused),
                    Admission::Shed => return Err(AdmitRefusal::DedupShed),
                    Admission::WaitShed => return Err(AdmitRefusal::DedupWaitShed),
                }
            }
            _ => None,
        };

        // Reserve-before-materialize: estimate bytes and decide which streams
        // are cache misses BEFORE cloning or canonicalizing anything into a
        // row shape. Each landing row is charged the row the QUEUE holds — the
        // union of the three kinds' columns — plus the text that kind's target
        // estimator prices; [`landing_charge`] adds what the block holds
        // besides its rows.
        let line_bytes: u64 = batch
            .rows
            .iter()
            .map(|r| LogLandingRow::est_landing_bytes(LogSampleRow::est_source_bytes(r)))
            .sum();

        // LRU-gate stream registration: a hit means this `(fingerprint,
        // month)` was already durably registered by a prior committed block,
        // so this request's copy is skipped entirely. A miss (including a
        // concurrent, still-uncommitted duplicate) is landed; the
        // duplicate-tolerant design is documented on `writer::registration`.
        //
        // It runs for a suppressed push too, because the push-size decision
        // below is over the whole push and has to be complete before any
        // branch can queue anything; its counters do not move for one, so
        // issue #494's suppression path is observably unchanged.
        let count = suppressed.is_none();
        let mut new_streams: Vec<&StreamRow> = Vec::new();
        {
            let mut lru = self.shared.lru.lock().expect("stream lru mutex poisoned");
            for stream in &batch.streams {
                let key = (stream.fingerprint, stream.month.days_since_epoch());
                if lru.contains(&key) {
                    if count {
                        self.shared
                            .metrics
                            .lru_hits_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    if count {
                        self.shared
                            .metrics
                            .lru_misses_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    new_streams.push(stream);
                }
            }
        }
        let stream_bytes: u64 = new_streams
            .iter()
            .copied()
            .map(|s| LogLandingRow::est_landing_bytes(LogStreamRow::est_source_bytes(s)))
            .sum();

        // The pattern rows are charged a BOUND, and the bound is what the
        // block carries: their count and their text are only known after
        // extraction, and extraction runs after the reservation. The first
        // three terms are the shipped reservation; the slot term is added
        // because the queue now holds a landing row per pattern rather than a
        // `LogPatternRow`. `PATTERN_ROW_OVERHEAD`'s own doc already allows for
        // an output-row slot, so the bound over-charges each landed pattern row
        // by that allowance — the safe direction, and deliberate: the bound is
        // the block's `bytes`, reserved once and released once. Disabled or
        // empty ⇒ zero charge and zero work.
        let patterns_on = self.shared.runtime.log_patterns && !batch.rows.is_empty();
        let distinct_cap = if patterns_on {
            batch.rows.len().min(MAX_DISTINCT_PATTERNS_PER_BATCH) as u64
        } else {
            0
        };
        let pattern_bound: u64 = if patterns_on {
            let template_bounds: u64 = batch.rows.iter().map(est_template_bound).sum();
            template_bounds
                + AGG_BASE_OVERHEAD
                + distinct_cap
                    * (PATTERN_ROW_OVERHEAD + crate::writer::rows::LOG_LANDING_ROW_SLOT_BYTES)
        } else {
            0
        };

        // Bounded before anything is queued. Extraction yields at most one row
        // per distinct non-empty template, so the actual row count never
        // exceeds this and an admitted block is strictly under both pinned row
        // limits.
        let total_rows_bound = batch.rows.len() as u64 + new_streams.len() as u64 + distinct_cap;

        // **A valid push with no rows of any kind is answered here**, before
        // the charge, the two ceilings and the reservation. It makes no block
        // and no insert, so it is charged for none: one block's fixed overhead
        // exceeds the smallest accepted value of either byte limit, and
        // charging an empty push for a block that is never created would have
        // refused it as too large.
        //
        // Its claim seals with no targets, so it completes at admission.
        if total_rows_bound == 0 {
            if let Some(suppressed) = suppressed {
                return Ok(Admitted::Suppressed(suppressed));
            }
            self.count_parse_outcomes(&batch);
            if let Some(guard) = guard {
                guard.seal();
            }
            return Ok(Admitted::Stored(Vec::new()));
        }

        let total_bytes =
            landing_charge::<LogLandingRow>(line_bytes + stream_bytes + pattern_bound);

        // The two per-push ceilings, counted over all three kinds, and decided
        // **before** either branch below queues anything. At or above the row
        // limit the block would not be strictly under the count both pinned row
        // limits carry (`QuerySettings::landing_insert`), and the server would
        // end a block at it. Returning here drops the un-sealed guard, which
        // removes the claim, so the client's retry of an unstored push is
        // stored rather than suppressed — and no bytes have been reserved yet.
        //
        // A push the index recognises as a repeat is refused the same way, and
        // for the same reason: it does not fit one block, so storing any part
        // of it would leave a `413` that stored something. One push gets one
        // answer whether or not it raced a copy of itself.
        if total_rows_bound >= self.shared.runtime.log_landing_max_rows
            || total_bytes > self.shared.runtime.batch_bytes
        {
            return Err(AdmitRefusal::PushTooLarge {
                rows: total_rows_bound,
                row_limit: self.shared.runtime.log_landing_max_rows,
                bytes: total_bytes,
                byte_limit: self.shared.runtime.batch_bytes,
            });
        }

        // **A suppressed push stores nothing and makes no block.** Logs has no
        // counterpart to the metrics descriptor exception: nothing on this path
        // carries a version the push identity excludes, so the push it repeats
        // already stored everything it owes. It reaches this step only if the
        // ceilings let it past.
        if let Some(suppressed) = suppressed {
            return Ok(Admitted::Suppressed(suppressed));
        }

        self.count_parse_outcomes(&batch);

        // Atomic reservation: reserve first, roll back on overflow.
        super::reserve_queued_bytes(
            &self.shared.queued_bytes,
            &self.shared.metrics.backpressure_total,
            total_bytes,
            self.shared.runtime.queue_bytes_limit,
        )
        .map_err(AdmitRefusal::from)?;

        // Reservation secured: only now materialize the rows — the clone, the
        // label canonicalization and the pattern extraction the gate above
        // exists to admit-or-reject ahead of.
        let mut rows: Vec<LogLandingRow> = Vec::with_capacity(total_rows_bound as usize);
        rows.extend(
            batch
                .rows
                .iter()
                .map(|r| LogLandingRow::line(received_ms, r)),
        );
        rows.extend(
            new_streams
                .iter()
                .copied()
                .map(|s| LogLandingRow::stream(received_ms, s)),
        );
        if patterns_on {
            let agg = aggregate_patterns(&batch.rows);
            if agg.dropped > 0 {
                self.shared
                    .metrics
                    .patterns_dropped_total
                    .fetch_add(agg.dropped, Ordering::Relaxed);
            }
            rows.extend(
                agg.rows
                    .into_iter()
                    .map(|r| LogLandingRow::pattern(received_ms, r)),
            );
        }

        if !new_streams.is_empty() {
            self.shared
                .metrics
                .stream_registrations_total
                .fetch_add(new_streams.len() as u64, Ordering::Relaxed);
        }

        debug_assert!(
            rows.len() as u64 <= total_rows_bound,
            "the rows materialized never exceed the bound the push was charged \
             and measured against"
        );

        let mut claim = ClaimTicket::new(self.shared.dedup.clone(), true);
        if let Some(g) = guard.as_ref() {
            claim.push(g.key());
        }
        note_target(&mut guard, true);

        let mut receivers = Vec::new();
        let waiter = if mode == AdmitMode::Sync {
            let (tx, rx) = oneshot::channel();
            receivers.push(rx);
            Some(tx)
        } else {
            None
        };

        self.queue_block(&pass, rows, total_bytes, claim, waiter, admitted_at);

        // Arms the claim: from here a drop no longer removes it, and the
        // target set is closed.
        if let Some(guard) = guard {
            guard.seal();
        }
        Ok(Admitted::Stored(receivers))
    }

    /// The parser's own per-push accounting, counted once per admitted push
    /// that is not a suppressed repeat — including one whose rows all came to
    /// nothing, which is exactly the push whose records were all rejected.
    fn count_parse_outcomes(&self, batch: &ParsedLogs) {
        self.shared
            .metrics
            .collisions_total
            .fetch_add(batch.collisions, Ordering::Relaxed);
        self.shared
            .metrics
            .rejected_total
            .fetch_add(batch.rejected, Ordering::Relaxed);
    }

    /// Seals one block — minting its token, so two byte-identical pushes carry
    /// different ones — and hands it to the queue.
    ///
    /// **A block that reaches here after the queue closed is settled by a task
    /// of this admission's own**, through the same ending a worker gives a
    /// block still queued at shutdown. Settling needs the spool, which is
    /// asynchronous, and admission is not.
    ///
    /// **Nothing reaches that branch from outside**: the pass this takes is
    /// held for the whole admission, and the drain waits for every pass before
    /// the deadline that closes the queue is announced.
    fn queue_block(
        &self,
        pass: &AdmissionPass<'_>,
        rows: Vec<LogLandingRow>,
        bytes: u64,
        claim: ClaimTicket,
        waiter: Option<oneshot::Sender<Result<(), WriteError>>>,
        admitted_at: tokio::time::Instant,
    ) {
        let token = {
            let mut rng = self
                .shared
                .token_rng
                .lock()
                .expect("token rng mutex poisoned");
            mint_landing_token(&mut rng)
        };
        let block = LandingBlock::seal(
            rows,
            bytes,
            QuerySettings::landing_insert(&token, self.shared.ctx.max_rows),
            claim,
            waiter,
            admitted_at,
        );
        if let Err(closed) = self.shared.landing_tx.send(block) {
            let ctx = self.shared.ctx.clone();
            let block = closed.0;
            let mut fate = LandingFate::NeverSent(String::new());
            fate.saw_pre_send(MSG_SHUTDOWN_QUEUED.to_string());
            self.shared.boundary.spawn_settlement(pass, async move {
                settle_block(&ctx, block, fate, TerminalCause::ShuttingDown).await;
            });
        }
    }

    /// Re-opens the admission gate after [`Self::shutdown`] has returned, so
    /// the next admission takes a pass and meets the closed queue.
    ///
    /// That race is otherwise unreachable from outside. Nothing in the server
    /// calls this.
    #[doc(hidden)]
    pub fn reopen_admission_for_test(&self) {
        self.shared.boundary.reopen_for_test();
    }

    /// This writer's push-suppression index (issue #494), or `None` while
    /// `PULSUS_INGEST_DEDUP` is off.
    pub fn dedup(&self) -> Option<&Arc<PushDedup>> {
        self.shared.dedup.as_ref()
    }

    /// A point-in-time metrics snapshot.
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

    /// Graceful shutdown, which is the boundary's whole job
    /// (`writer::drain`): admission closes and the admissions inside it
    /// finish, the deadline is announced, and every task this writer spawned
    /// is awaited — the insert workers, the suppression ticker and any
    /// settlement an admission spawned. Each worker closes the queue and
    /// settles whatever is still in it, which is what makes the drain
    /// terminate. Idempotent.
    pub async fn shutdown(&self, deadline: Duration) {
        self.shared.boundary.shutdown(deadline).await;
    }
}

impl LogSink for LogWriter {
    fn admit(&self, batch: ParsedLogs, push: PushHeaders) -> Result<(), AdmitRefusal> {
        self.admit_batch(batch, AdmitMode::Async, push).map(|_| ())
    }

    fn admit_flush(&self, batch: ParsedLogs, push: PushHeaders) -> Result<FlushWait, AdmitRefusal> {
        match self.admit_batch(batch, AdmitMode::Sync, push)? {
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

/// The `LogWriter` cases that need this module's own private surface: the
/// block the charge is walked over, the promotion keys, and the two
/// release-ordering observations reached by parking the writer inside a hook
/// or inside a spool write.
///
/// The rest of the logs write path is `crates/pulsus-write/tests/log_landing.rs`.
#[cfg(test)]
mod tests {

    use std::sync::atomic::AtomicUsize;

    use pulsus_clickhouse::ChError;
    use pulsus_model::{Date, Fingerprint, LabelSet, UnixNano};

    use super::*;
    use crate::protocols::otlp_logs::LogRow;
    use crate::writer::push_dedup::PushDigest;
    use crate::writer::rows::{LOG_LANDING_ROW_SLOT_BYTES, LogPatternRow};

    /// What a [`ClaimTicket`]'s first `push` allocates: a `Vec` of
    /// [`PushDigest`], whose first allocation is four slots. The one claim a
    /// landing block carries fills one of them.
    const CLAIM_FIRST_ALLOCATION_BYTES: u64 = 4 * std::mem::size_of::<PushDigest>() as u64;

    /// What a sync caller's `oneshot` channel allocates: a state word, two waker
    /// slots and one `Result<(), WriteError>` come to 8 + 2 × 16 + 32 = 72. Stated
    /// at 128 because `oneshot`'s `Inner` is private to another crate and cannot be
    /// measured from here; a channel larger than this is the one thing below that
    /// neither this figure nor the charge would catch.
    const WAITER_CHANNEL_BYTES: u64 = 128;

    /// A service name long enough that a per-row owned copy of it is visible
    /// beside the rows' own charge.
    const SERVICE: &str = "checkout-api-gateway-edge";

    /// A landing inserter that reports the call and then parks until the case
    /// releases it. That gap is where the block is admitted, charged, out of the
    /// queue and not yet committed — the one point from which a case can take the
    /// registration mutex knowing the commit has not reached it.
    struct ParkingInserter {
        /// One permit per call, added as the insert begins.
        entered: tokio::sync::Semaphore,
        /// The case adds the permit that lets the insert return `Ok`.
        release: tokio::sync::Semaphore,
    }

    impl ParkingInserter {
        fn new() -> Self {
            ParkingInserter {
                entered: tokio::sync::Semaphore::new(0),
                release: tokio::sync::Semaphore::new(0),
            }
        }
    }

    impl BlockInserter<LogLandingRow> for ParkingInserter {
        fn insert<'a>(
            &'a self,
            _table: &'a str,
            _rows: &'a [LogLandingRow],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>>
        {
            Box::pin(async move {
                self.entered.add_permits(1);
                self.release
                    .acquire()
                    .await
                    .expect("the release semaphore is never closed")
                    .forget();
                Ok(())
            })
        }
    }

    /// An inserter that always fails the same way, counting its calls.
    struct FailingInserter {
        calls: AtomicUsize,
    }

    impl FailingInserter {
        fn new() -> Self {
            FailingInserter {
                calls: AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl BlockInserter<LogLandingRow> for FailingInserter {
        fn insert<'a>(
            &'a self,
            _table: &'a str,
            _rows: &'a [LogLandingRow],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>>
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Err(ChError::Decode("mock poison".to_string())) })
        }
    }

    /// Waits for `done` in real time, naming what never happened. The worker runs
    /// on a task of its own, so a case cannot yield to it.
    async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !done() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn labels_of(service: &str) -> LabelSet {
        LabelSet::from_normalized([
            ("service_name".to_string(), service.to_string()),
            ("env".to_string(), "production".to_string()),
        ])
        .0
    }

    fn log_row(fingerprint: u128, timestamp_ns: i64, body: &str) -> LogRow {
        LogRow {
            service: SERVICE.to_string(),
            fingerprint: Fingerprint::from_raw(fingerprint),
            timestamp_ns: UnixNano(timestamp_ns),
            severity: 0,
            body: body.to_string(),
            structured_metadata: String::new(),
        }
    }

    fn stream_row(fingerprint: u128, timestamp_ns: i64) -> StreamRow {
        StreamRow {
            month: Date::start_of_month_utc(timestamp_ns).expect("a representable month"),
            fingerprint: Fingerprint::from_raw(fingerprint),
            service: SERVICE.to_string(),
            labels: labels_of(SERVICE),
            updated_ns: timestamp_ns,
        }
    }

    fn spool_root(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pulsus-log-writer-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create the spool root");
        dir
    }

    fn runtime_at(cfg: &WriterConfig, spool: &std::path::Path) -> WriterRuntime {
        let mut runtime = WriterRuntime::from_config(cfg);
        runtime.spool_dir = spool.to_path_buf();
        runtime
    }

    /// Every spool record under `kind` for the landing table, parsed.
    fn spool_records(root: &std::path::Path, kind: &str) -> Vec<serde_json::Value> {
        let dir = root.join(kind).join(LANDING_TABLE);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("read a spool file");
            out.push(serde_json::from_str(&text).expect("a spool file is JSON"));
        }
        out
    }

    /// Yields until `done` holds, so a test can let the insert workers make
    /// progress without advancing a paused clock.
    async fn settle_until(mut done: impl FnMut() -> bool) {
        for _ in 0..1024 {
            if done() {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    /// **T18.** The `StreamLru` keys a committed block registers come from the
    /// block's own kind-1 rows: such a row holds the fingerprint and the month,
    /// which is the whole key. Deriving them is what keeps a block from retaining
    /// one owned key per new stream beside its rows — a push registering ten
    /// thousand streams would otherwise hold ten thousand of them, charged for
    /// none.
    ///
    /// **In this module rather than the integration suite** because
    /// `promotion_keys` is private to it, exactly as the metrics twin
    /// (`the_promotion_keys_are_derived_from_the_blocks_kind_2_rows`) is private to
    /// `writer::metric`.
    #[test]
    fn the_promotion_keys_are_derived_from_the_blocks_kind_1_rows() {
        let month = Date::start_of_month_utc(1_700_000_000_000_000_000)
            .expect("a representable month")
            .days_since_epoch();
        let rows = vec![
            LogLandingRow::line(5, &log_row(7, 1_700_000_000_000_000_000, "hello")),
            LogLandingRow::stream(5, &stream_row(7, 1_700_000_000_000_000_000)),
            LogLandingRow::pattern(
                5,
                LogPatternRow {
                    fingerprint: Fingerprint::from_raw(7),
                    bucket_ns: 1_700_000_000_000_000_000,
                    pattern: "hello".to_string(),
                    count: 1,
                },
            ),
            LogLandingRow::stream(5, &stream_row(9, 1_700_000_000_000_000_000)),
        ];

        let keys: Vec<(Fingerprint, u16)> = promotion_keys(&rows).collect();

        assert_eq!(
            keys,
            vec![
                (Fingerprint::from_raw(7), month),
                (Fingerprint::from_raw(9), month),
            ],
            "one key per kind-1 row and none for any other kind"
        );
    }

    /// **T6.** What a push is charged must cover everything the queue holds for
    /// it, not its rows alone. This prices a sealed block by walking it — every
    /// field, and every `String` reached through one, **by capacity rather than by
    /// length**, since the capacity is what the allocator is holding.
    ///
    /// **Three shapes, because a block's charge and what it holds do not scale
    /// together.** Lines only is where the per-block half bites: every row carries
    /// the whole block's fixed cost. Lines plus a new stream adds the canonical
    /// label JSON the kind-1 row owns. Extraction at the distinct-pattern cap is
    /// where the pattern bound has to cover a landing row per template rather than
    /// a `LogPatternRow`.
    ///
    /// Two of the walk's terms are stated rather than measured —
    /// [`CLAIM_FIRST_ALLOCATION_BYTES`] and [`WAITER_CHANNEL_BYTES`], both inside
    /// types whose fields are private to another module. Everything else is read
    /// off the block, and no figure here comes from the constant the charge is
    /// built with.
    #[tokio::test]
    async fn the_charge_covers_everything_a_queued_log_block_holds() {
        struct Shape {
            name: &'static str,
            rows: Vec<LogRow>,
            streams: Vec<StreamRow>,
        }

        const TS: i64 = 1_700_000_000_000_000_000;

        let shapes = vec![
            Shape {
                name: "two lines, no new stream",
                rows: vec![
                    log_row(1, TS, "request 1 completed"),
                    log_row(1, TS, "request 2 completed"),
                ],
                streams: Vec::new(),
            },
            Shape {
                name: "two lines plus a new stream",
                rows: vec![
                    log_row(1, TS, "request 1 completed"),
                    log_row(1, TS, "request 2 completed"),
                ],
                streams: vec![stream_row(1, TS)],
            },
            Shape {
                name: "extraction at the distinct-pattern cap",
                rows: (0..MAX_DISTINCT_PATTERNS_PER_BATCH)
                    .map(|i| log_row(1, TS, &format!("token_{i:05}_alpha beta gamma")))
                    .collect(),
                streams: vec![stream_row(1, TS)],
            },
        ];

        for shape in shapes {
            // The three expressions `admit_batch` prices a push with.
            let line_bytes: u64 = shape
                .rows
                .iter()
                .map(|r| LogLandingRow::est_landing_bytes(LogSampleRow::est_source_bytes(r)))
                .sum();
            let stream_bytes: u64 = shape
                .streams
                .iter()
                .map(|s| LogLandingRow::est_landing_bytes(LogStreamRow::est_source_bytes(s)))
                .sum();
            let distinct_cap = shape.rows.len().min(MAX_DISTINCT_PATTERNS_PER_BATCH) as u64;
            let pattern_bound: u64 = if shape.rows.is_empty() {
                0
            } else {
                shape.rows.iter().map(est_template_bound).sum::<u64>()
                    + AGG_BASE_OVERHEAD
                    + distinct_cap * (PATTERN_ROW_OVERHEAD + LOG_LANDING_ROW_SLOT_BYTES)
            };

            // The rows `admit_batch` materializes for it, in its order.
            let mut rows: Vec<LogLandingRow> = Vec::with_capacity(
                (shape.rows.len() as u64 + shape.streams.len() as u64 + distinct_cap) as usize,
            );
            rows.extend(shape.rows.iter().map(|r| LogLandingRow::line(5, r)));
            rows.extend(shape.streams.iter().map(|s| LogLandingRow::stream(5, s)));
            rows.extend(
                aggregate_patterns(&shape.rows)
                    .rows
                    .into_iter()
                    .map(|r| LogLandingRow::pattern(5, r)),
            );

            let (tx, _rx) = oneshot::channel();
            let block = LandingBlock::seal(
                rows,
                landing_charge::<LogLandingRow>(line_bytes + stream_bytes + pattern_bound),
                QuerySettings::landing_insert(
                    &mint_landing_token(&mut XorShift64::seeded()),
                    1_048_576,
                ),
                ClaimTicket::inert(),
                Some(tx),
                tokio::time::Instant::now(),
            );

            let held = held_bytes(&block);
            assert!(
                block.bytes >= held,
                "{}: the charge ({}) must cover the {held} bytes this block holds",
                shape.name,
                block.bytes
            );
        }
    }

    /// What one queued block holds, walked field by field. `bytes` and
    /// `admitted_at` are inline in the struct the first term prices; `rows`,
    /// `settings`, `claim` and `waiter` each own heap beyond their slots.
    fn held_bytes(block: &LandingBlock<LogLandingRow>) -> u64 {
        // The block, and the queue slot the channel moves it into.
        let mut held = 2 * std::mem::size_of::<LandingBlock<LogLandingRow>>() as u64;

        held += block.rows.capacity() as u64 * std::mem::size_of::<LogLandingRow>() as u64;
        for row in &block.rows {
            held += (row.service.capacity()
                + row.body.capacity()
                + row.structured_metadata.capacity()
                + row.labels.capacity()
                + row.pattern.capacity()) as u64;
        }

        // By capacity, through the accessor: a `String` holds its capacity and a
        // `Vec` grows by doubling, so a figure over the entries' lengths would
        // understate the settings' retained allocation and this walk would clear a
        // charge that does not cover it.
        held += block.settings.allocated_bytes();

        // Stated, not measured: see each constant.
        held += CLAIM_FIRST_ALLOCATION_BYTES;
        held += WAITER_CHANNEL_BYTES;
        held
    }

    /// **T9.** A committed block keeps its reservation until the rows it was
    /// charged for are gone. The commit exit promotes the push's new streams into
    /// the registration LRU behind a mutex that every admission and every other
    /// worker's commit also takes, so a worker can wait there while holding a
    /// whole block. Handing the allowance back before that point gives it to a new
    /// admission while this block's rows — and the rows of every worker queued
    /// behind the same mutex — are all still live, and
    /// `PULSUS_INGEST_QUEUE_BYTES` permits more than it names.
    ///
    /// **The contention is genuine**: the case holds the writer's own registration
    /// mutex while a successful insert runs to its commit. `flushes_total` is the
    /// rendezvous — the commit records the flush before it calls the hook — so the
    /// assertion is made at a point the worker has provably reached rather than
    /// after a sleep, and it is the release that has to have waited.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_committing_log_block_stays_charged_while_it_promotes() {
        const TS: i64 = 1_700_000_000_000_000_000;
        let inserter = Arc::new(ParkingInserter::new());
        let root = spool_root("promote-charge");
        let mut runtime = runtime_at(&WriterConfig::default(), &root);
        runtime.log_landing_inserters = 1;
        let writer = LogWriter::with_landing_inserter_and_runtime(
            inserter.clone(),
            runtime,
            WriterTables::logs_default(),
        );

        let batch = ParsedLogs {
            rows: vec![log_row(7, TS, "hello")],
            streams: vec![stream_row(7, TS)],
            ..Default::default()
        };
        writer
            .admit(batch, PushHeaders::default())
            .expect("the push is admitted");
        let charged = writer.shared.queued_bytes.load(Ordering::SeqCst);
        assert!(charged > 0, "an admitted push holds a reservation");

        // The insert has begun, so the block has left the queue and admission has
        // finished with the registration mutex.
        inserter
            .entered
            .acquire()
            .await
            .expect("the entered semaphore is never closed")
            .forget();

        // A blocking thread of its own holds the mutex the commit has to take, so
        // nothing in this case holds a guard across an await.
        let (held_tx, held_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let lru = writer.shared.lru.clone();
        let holder = tokio::task::spawn_blocking(move || {
            let guard = lru.lock().expect("stream lru mutex poisoned");
            held_tx.send(()).expect("the case waits for the mutex");
            release_rx.blocking_recv().ok();
            drop(guard);
        });
        held_rx.await.expect("the mutex is held");

        // Let the insert succeed. Once the flush is recorded the worker is inside
        // the commit and blocked on the promotion this case is holding.
        inserter.release.add_permits(1);
        wait_for("the commit to record its flush", || {
            writer.metrics().landing.flushes_total == 1
        })
        .await;

        assert_eq!(
            writer.shared.queued_bytes.load(Ordering::SeqCst),
            charged,
            "the reservation was handed back while the block's rows were still \
             live: the worker is blocked on the registration mutex this case \
             holds, so it has released nothing yet"
        );

        release_tx.send(()).expect("the holder waits for this");
        holder.await.expect("the holding thread finishes");

        wait_for("the reservation to be released", || {
            writer.shared.queued_bytes.load(Ordering::SeqCst) == 0
        })
        .await;
        let registered = {
            let mut lru = writer.shared.lru.lock().expect("stream lru mutex poisoned");
            lru.contains(&(
                Fingerprint::from_raw(7),
                Date::start_of_month_utc(TS)
                    .expect("a representable month")
                    .days_since_epoch(),
            ))
        };
        assert!(
            registered,
            "the commit promoted the push's new stream while the block was still \
             charged"
        );

        writer.shutdown(Duration::from_secs(5)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// **T11.** A failed block's queue reservation stays charged until its spool
    /// copy has been **written**. The rows are what that write reads, so releasing
    /// first lets a new admission take the allowance while they are still live —
    /// the bound would then permit more than it names, by the size of whatever is
    /// being spooled.
    ///
    /// The seam is tokio's blocking pool: `tokio::fs` runs on it, this runtime has
    /// exactly one blocking thread, and the case holds that thread while it reads
    /// the reservation, so the write is **parked rather than failed**. A plain-file
    /// spool root does not serve here: `create_dir_all` fails at once and nothing
    /// parks, so nothing is observed during a write.
    #[test]
    fn a_failed_log_blocks_reservation_is_held_until_its_spool_copy_is_written() {
        const TS: i64 = 1_700_000_000_000_000_000;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_time()
            .build()
            .expect("build a runtime with one blocking thread");

        runtime.block_on(async {
            let root = spool_root("spool-reservation");
            let inserter = Arc::new(FailingInserter::new());
            let writer = LogWriter::with_landing_inserter_and_runtime(
                inserter.clone(),
                runtime_at(&WriterConfig::default(), &root),
                WriterTables::logs_default(),
            );

            // Occupy the single blocking thread, so the settling worker's first
            // `tokio::fs` call queues behind it rather than running.
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let (occupied_tx, occupied_rx) = std::sync::mpsc::channel::<()>();
            let held = tokio::task::spawn_blocking(move || {
                occupied_tx.send(()).expect("the case is still waiting");
                release_rx.recv().expect("the case releases the thread");
            });
            occupied_rx
                .recv()
                .expect("the blocking thread is now occupied");

            let batch = ParsedLogs {
                rows: vec![log_row(7, TS, "hello")],
                streams: vec![stream_row(7, TS)],
                ..Default::default()
            };
            let wait = writer
                .admit_flush(batch, PushHeaders::default())
                .expect("queue has room");
            let charged = writer.metrics().queue_bytes;
            assert!(charged > 0, "an admitted push holds a reservation");

            // The worker reaches its ending and then parks on the spool write.
            settle_until(|| inserter.call_count() == 1).await;
            settle_until(|| false).await;
            assert_eq!(
                writer.metrics().queue_bytes,
                charged,
                "the reservation must still be charged while the spool copy is \
                 being built and written"
            );
            assert_eq!(
                spool_records(&root, "poison").len(),
                0,
                "nothing is written yet, which is what makes the charge above the \
                 ordering claim"
            );

            release_tx.send(()).expect("the held task is still parked");
            held.await.expect("the held task finishes");
            let answer = tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .expect("settles")
                .expect_err("never a success");
            assert!(answer.to_string().contains("poison"), "{answer}");
            assert_eq!(spool_records(&root, "poison").len(), 1);
            assert_eq!(
                writer.metrics().queue_bytes,
                0,
                "released once, after the write"
            );

            writer.shutdown(Duration::from_secs(2)).await;
            std::fs::remove_dir_all(&root).ok();
        });
    }
}
