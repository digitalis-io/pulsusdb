//! `MetricWriter`: the metrics write path. Implements issue #26's
//! [`crate::ingest::metrics::MetricSink`] seam.
//!
//! **One push is one `INSERT` of one block into `metric_landing`** (issue
//! #603). Four materialized views maintain `metric_samples`,
//! `metric_series`, `metric_metadata` and `metric_hist_samples` from that
//! one source table, and the writer inserts into none of them. It replaces
//! four independently flushed table buffers, where some of a push's tables
//! could commit and others time out.
//!
//! What that gives: the engine performs the fan-out synchronously as part
//! of insert processing, with a documented retry mechanism for ambiguous
//! inserts. What it does not give: nothing makes the fan-out across the four
//! targets a transaction.
//!
//! A push's rows are never spread over two inserts and an insert never
//! carries two pushes. A push too large for one block is refused whole
//! ([`AdmitRefusal::PushTooLarge`]), never split — two blocks can commit a
//! prefix, and a prefix is not all-or-nothing.
//!
//! **Retry safety** is the `insert_deduplication_token` the writer mints per
//! sealed block and repeats byte-identically on every resend, so a resend of
//! a block the server already accepted stores nothing twice while the
//! tables' deduplication windows still hold it. The token is never derived
//! from the content: two byte-identical metric blocks can be two genuine
//! pushes.
//!
//! **Registration**: `SeriesLru` promotion stays success-only — the LRU is
//! promoted when the block commits, never at admission — keyed
//! `(metric_name, fingerprint, bucket, value_type)`. A registration is a
//! kind-2 row inside the samples' own block rather than its own insert, so
//! there is no "samples committed, registration lost" orphan left to heal
//! and no registration backfill on this path. Every push emits its
//! descriptors as kind-3 rows: `metric_metadata` is a
//! `ReplacingMergeTree(updated_ns)` keyed on `metric_name`, so a second row
//! carrying the same descriptor collapses on merge, and the read takes one
//! whole tuple.
//!
//! **Backpressure/shutdown**: `queued_bytes` is reserved atomically at
//! admission and released exactly once, by whichever ending settles the
//! block, through [`LandingBlock::release`] — which owns when, and is the
//! only place on this path that subtracts. What is reserved is the whole
//! block's cost, not its rows' alone
//! ([`LANDING_BLOCK_OVERHEAD_BYTES`]), and a push with no rows makes no block
//! and is charged for none. Every ending but a commit spools the block — it is
//! the push's only copy — reports the claim its fate, and answers a waiting
//! sync caller `500`.
//!
//! **The shutdown boundary is `writer::drain`'s**, whole: no attempt is
//! authorized after the announced deadline, and nothing this writer spawned is
//! abandoned at it. That module owns the rule, the two residuals it leaves and
//! what the deadline does not bound, which is settling a block. This module
//! asks the boundary for an attempt, waits between attempts through it, admits
//! inside a pass it hands out, and spawns nothing it does not track. Nothing
//! here reads the deadline or reasons about it.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChError, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_model::{Fingerprint, floor_to_activity_bucket};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::error::LogsIngestError;
use crate::ingest::metrics::{MetricMetadata, MetricSink, ParsedMetrics, SeriesRef};
use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};
use crate::writer::config::WriterRuntime;
use crate::writer::drain::{AdmissionPass, AttemptEnd, DrainBoundary, DrainWatch};
use crate::writer::error::WriteError;
use crate::writer::metrics::{MetricWriterMetrics, MetricWriterMetricsSnapshot};
use crate::writer::push_dedup::{self, Admission, ClaimTicket, PushDedup, TargetOutcome};
use crate::writer::registration::{SeriesKey, SeriesLru};
use crate::writer::rows::{
    MetricHistSampleRow, MetricLandingRow, MetricMetadataRow, MetricSampleRow, MetricSeriesRow,
};
use crate::writer::spool::{SpoolKind, SpoolWriter};
use crate::writer::table::{BlockInserter, ChBlockInserter, XorShift64, spawn_dedup_ticker};
use crate::writer::{AdmitMode, Admitted, Suppressed, note_target};

/// The one table the metrics writer inserts into (issue #603).
const LANDING_TABLE: &str = "metric_landing";

/// The `metric_series.value_type` discriminant for a float sample.
const VALUE_TYPE_FLOAT: u8 = 0;
/// The `metric_series.value_type` discriminant for a native-histogram sample.
const VALUE_TYPE_HISTOGRAM: u8 = 1;

/// The message a block still queued when the budget ran out settles with.
const MSG_BUDGET_QUEUED: &str = "the landing budget was spent before the block was sent";
/// The message a block whose budget ran out between attempts settles with.
const MSG_BUDGET_BETWEEN: &str = "the landing budget was spent between attempts";
/// The message an attempt interrupted by the budget settles with.
const MSG_BUDGET_IN_ATTEMPT: &str = "the landing budget elapsed during an attempt";
/// The message a block abandoned mid-attempt at the shutdown deadline
/// settles with.
const MSG_SHUTDOWN_INFLIGHT: &str = "the writer shut down with an attempt in flight";
/// The message a block still queued at shutdown settles with.
const MSG_SHUTDOWN_QUEUED: &str = "the writer shut down before the block was sent";

/// The table name a [`MetricWriter`] inserts into. One name: the four target
/// tables are maintained by materialized view and the writer never names
/// them (issue #603). The landing table has no `Distributed` wrapper, so
/// this is the base name in every mode — `Arc<str>` because the server
/// resolves it once at startup from `Config` rather than at compile time.
#[derive(Debug, Clone)]
pub struct MetricWriterTables {
    pub landing: Arc<str>,
}

impl MetricWriterTables {
    /// The default: the bare local table name.
    pub fn metrics_default() -> Self {
        MetricWriterTables {
            landing: Arc::from(LANDING_TABLE),
        }
    }
}

/// What the queue holds for one block **besides its rows**, charged once per
/// block so that `PULSUS_INGEST_QUEUE_BYTES` bounds everything a queued push
/// retains rather than its rows alone.
///
/// Everything a `LandingBlock` holds, field by field, at the most any one of
/// them holds. `rows` is priced per row by
/// [`MetricLandingRow::est_landing_bytes`]; `bytes` and `admitted_at` are
/// inline in the struct; every other field is a term below.
///
/// | what it holds | bytes |
/// |---|---|
/// | the block itself, and the queue slot it is moved into | `2 * size_of::<LandingBlock>()` |
/// | `settings`: ten owned key/value pairs | their vector, sixteen slots — its first allocation is four, the fifth pair doubles it and the ninth doubles it again — plus 408 for their text **by capacity**: 18 + 6, 50 + 10, 26 + 36, 21 + 8, 27 + 10, 33 + 10, 26 + 8, 27 + 10, 32 + 10, 30 + 10, at a 36-byte token and the largest accepted row ceiling. A value rendered from an integer literal is allocated ten bytes whatever its digits; one rendered from the `u64` ceiling is allocated its digits, which is why the largest ceiling is the term (406 at the default, 400 at the floor) |
/// | `claim`: the `Vec<PushDigest>` its one key allocates, four slots at its first push | `4 * size_of::<PushDigest>()` |
/// | `waiter`: one `oneshot` channel in sync mode — a state word, two waker slots and one `Result<(), WriteError>` | 256. Those four come to 8 + 2 × 16 + 32 = 72; the rest is allowance, because the channel's own bookkeeping is private to it |
///
/// **So a block may hold nothing whose size grows with the push except its
/// rows.** A field that did would need its own per-push term and this one
/// would stop bounding it: that is what carrying one owned `SeriesKey` per new
/// series did before `promotion_keys` derived them from the committed rows
/// instead. `the_charge_covers_everything_a_queued_block_holds` prices a
/// sealed block by walking it, over the push shapes where each half of this
/// figure binds.
pub const LANDING_BLOCK_OVERHEAD_BYTES: u64 = 2 * std::mem::size_of::<LandingBlock>() as u64
    + 16 * std::mem::size_of::<(String, String)>() as u64
    + 408
    + 4 * std::mem::size_of::<push_dedup::PushDigest>() as u64
    + 256;

/// What the queue is charged for a push whose landing rows price `row_bytes`.
/// **One figure prices a push**: the per-push byte ceiling refuses against it,
/// `reserve_queued_bytes` takes it, whichever ending settles releases it and
/// `record_flush` reports it, so no two halves of the accounting can drift
/// apart.
fn landing_charge(row_bytes: u64) -> u64 {
    row_bytes + LANDING_BLOCK_OVERHEAD_BYTES
}

/// The `SeriesLru` keys a committed block registers, derived from the block's
/// own kind-2 rows rather than carried beside them: such a row holds the
/// metric name, the fingerprint, the activity-bucket floor in `unix_milli` and
/// the value type, which is the whole key. Nothing is retained for a block
/// that never commits, and what a queued block holds does not grow with the
/// number of series a push registers — see
/// [`LANDING_BLOCK_OVERHEAD_BYTES`].
fn promotion_keys(rows: &[MetricLandingRow]) -> impl Iterator<Item = SeriesKey> + '_ {
    rows.iter()
        .filter(|row| row.kind == MetricLandingRow::KIND_SERIES)
        .map(|row| {
            (
                Arc::from(row.metric_name.as_str()),
                row.fingerprint,
                row.unix_milli,
                row.value_type,
            )
        })
}

/// One admitted push, sealed and queued. A worker runs it to exactly one
/// ending and is the only place it settles.
///
/// **What one of these costs the queue, and what that requires of the fields
/// below, is [`LANDING_BLOCK_OVERHEAD_BYTES`].**
pub(crate) struct LandingBlock {
    /// The push's landing rows, every kind in one vector. Nothing depends on
    /// their order inside the block: the table's sorting key orders what is
    /// stored, and every consumer matches rows by `kind`. The keys a commit
    /// promotes are read back off them ([`promotion_keys`]).
    rows: Vec<MetricLandingRow>,
    /// Exactly what `reserve_queued_bytes` took for this block
    /// ([`landing_charge`]). Released once, and in one place —
    /// [`LandingBlock::release`], which owns when.
    bytes: u64,
    /// [`QuerySettings::landing_insert`], built once here and sent
    /// byte-identical on every resend.
    settings: QuerySettings,
    /// The push's issue-#494 obligation, or an inert ticket for a
    /// descriptor-only block and whenever `PULSUS_INGEST_DEDUP` is off.
    claim: ClaimTicket,
    /// The sync caller's waiter; `None` in async mode.
    waiter: Option<oneshot::Sender<Result<(), WriteError>>>,
    /// Taken with the claim. The landing budget runs from here, so a block's
    /// queue wait is spent out of its own budget.
    ///
    /// `tokio::time::Instant`, not `std::time::Instant`: the budget is
    /// compared against the same clock the attempt's own timeout and the
    /// retry sleeps use, so a test that drives the loop on a paused clock
    /// measures one clock rather than two.
    admitted_at: tokio::time::Instant,
}

impl LandingBlock {
    /// The one place a `LandingBlock` is built, so the case that prices what
    /// one holds prices the same value the queue does.
    fn seal(
        rows: Vec<MetricLandingRow>,
        bytes: u64,
        settings: QuerySettings,
        claim: ClaimTicket,
        waiter: Option<oneshot::Sender<Result<(), WriteError>>>,
        admitted_at: tokio::time::Instant,
    ) -> Self {
        LandingBlock {
            rows,
            bytes,
            settings,
            claim,
            waiter,
            admitted_at,
        }
    }

    /// **The one place a landing block's reservation is released, and the order
    /// it is released in** (issue #603 code review round 10, finding 1).
    ///
    /// The charge is the queue's allowance for what this block holds, so it is
    /// given back only once the block is gone: this consumes it, drops the rows
    /// and the settings — everything the charge prices that grows with the push
    /// — reports the claim, which is where the ticket's own charged vector goes,
    /// and subtracts last. An ending that subtracted for itself would hand the
    /// allowance to a new admission with its own rows still in memory, and with
    /// the rows of every worker waiting on the registration mutex behind it, so
    /// `PULSUS_INGEST_QUEUE_BYTES` would permit more than it names.
    ///
    /// The waiter is returned rather than answered here: the caller resolves it
    /// last, so a sync caller reading its answer has by then seen the bytes
    /// released and the claim reported. What crosses the release is that one
    /// `oneshot` channel — a fixed term of [`LANDING_BLOCK_OVERHEAD_BYTES`],
    /// jointly owned with the caller, and therefore outside any ordering this
    /// writer could choose.
    fn release(
        self,
        ctx: &LandingContext,
        outcome: TargetOutcome,
    ) -> Option<oneshot::Sender<Result<(), WriteError>>> {
        let LandingBlock {
            rows,
            bytes,
            settings,
            claim,
            waiter,
            admitted_at: _,
        } = self;
        drop(rows);
        drop(settings);
        claim.settle(outcome);
        ctx.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
        waiter
    }
}

/// What a landing worker needs to run a block to an ending.
pub(crate) struct LandingContext {
    table: Arc<str>,
    inserter: Arc<dyn BlockInserter<MetricLandingRow>>,
    runtime: Arc<WriterRuntime>,
    metrics: Arc<MetricWriterMetrics>,
    queued_bytes: Arc<AtomicU64>,
    spool: Arc<SpoolWriter>,
    series_lru: Arc<Mutex<SeriesLru>>,
}

/// What the loop knows about a block short of a commit — **a value the loop
/// carries, not a decision each ending makes**. Two methods write it and
/// there is no third, and `Uncertain` is terminal short of a commit: an
/// ending whose own knowledge is "this did not send" can never walk the fate
/// back from an earlier attempt that may have.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LandingFate {
    NeverSent(String),
    Uncertain(String),
}

/// Why a block reached [`settle_block`]. It decides the waiter's error and
/// nothing else: the spool directory and the claim's outcome come from the
/// fate. A shutdown is not a fate — a block abandoned at the shutdown
/// deadline may have committed, exactly as one abandoned at the budget may —
/// so it travels beside one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalCause {
    Normal,
    ShuttingDown,
}

impl LandingFate {
    /// What an ending whose own knowledge is "this did not send" calls. It
    /// never reverses an earlier `Uncertain`, and the argument is dropped in
    /// that case.
    fn saw_pre_send(&mut self, msg: String) {
        if let LandingFate::NeverSent(m) = self {
            *m = msg;
        }
    }

    /// What an ending that may have sent calls.
    fn saw_uncertain(&mut self, msg: String) {
        *self = LandingFate::Uncertain(msg);
    }

    /// The spool directory, the claim's outcome and the message. The only
    /// place the landing path names a `SpoolKind` or a non-`Committed`
    /// `TargetOutcome`, so no ending picks its own.
    fn settle(self) -> (SpoolKind, TargetOutcome, String) {
        match self {
            LandingFate::NeverSent(m) => (SpoolKind::Poison, TargetOutcome::NotCommitted, m),
            LandingFate::Uncertain(m) => (SpoolKind::Uncertain, TargetOutcome::Uncertain, m),
        }
    }
}

struct Shared {
    /// The landing queue's sender. Bounded by **bytes, not by length**:
    /// [`landing_charge`] runs through `reserve_queued_bytes` before a block is
    /// sent and is the only gate, so what the queue holds is bounded by
    /// `PULSUS_INGEST_QUEUE_BYTES`, whatever number of blocks that comes to.
    landing_tx: mpsc::UnboundedSender<LandingBlock>,
    ctx: Arc<LandingContext>,
    /// Issue #494's per-signal push-suppression index. `None` while
    /// `PULSUS_INGEST_DEDUP` is off.
    dedup: Option<Arc<PushDedup>>,
    queued_bytes: Arc<AtomicU64>,
    runtime: Arc<WriterRuntime>,
    metrics: Arc<MetricWriterMetrics>,
    series_lru: Arc<Mutex<SeriesLru>>,
    /// The token generator. One lock per admitted push: a token minted from
    /// the clock alone would repeat for two pushes inside one millisecond,
    /// and the second would be dropped as a resend of the first.
    token_rng: Mutex<XorShift64>,
    /// The `metric_series` activity-bucket width in milliseconds
    /// (`pulsus_config::ReaderConfig::series_activity_bucket`, resolved by
    /// the caller — not read from `WriterConfig`).
    bucket_ms: i64,
    /// The shutdown boundary: the admission gate, the announced deadline and
    /// every task this writer spawned (`writer::drain`).
    boundary: DrainBoundary,
}

/// Implements issue #26's `MetricSink` over one landing table. See the
/// module-level docs above.
pub struct MetricWriter {
    shared: Arc<Shared>,
}

impl MetricWriter {
    /// Production constructor: inserts through a real ClickHouse connection,
    /// against the default table name. `bucket_ms` is the `metric_series`
    /// activity-bucket width, the caller's job to resolve from config.
    pub fn new(client: Arc<ChClient>, cfg: &WriterConfig, bucket_ms: i64) -> Self {
        Self::new_with_tables(
            client,
            cfg,
            bucket_ms,
            MetricWriterTables::metrics_default(),
        )
    }

    /// [`Self::new`], but against `tables`.
    pub fn new_with_tables(
        client: Arc<ChClient>,
        cfg: &WriterConfig,
        bucket_ms: i64,
        tables: MetricWriterTables,
    ) -> Self {
        let inserter: Arc<ChBlockInserter> = Arc::new(ChBlockInserter::new(client));
        Self::with_landing_inserter_and_tables(inserter, cfg, bucket_ms, tables)
    }

    /// Test/mock constructor: any [`BlockInserter`] — e.g. a scriptable mock
    /// that can fail or hang on demand — against the default table name.
    pub fn with_landing_inserter(
        landing: Arc<dyn BlockInserter<MetricLandingRow>>,
        cfg: &WriterConfig,
        bucket_ms: i64,
    ) -> Self {
        Self::with_landing_inserter_and_tables(
            landing,
            cfg,
            bucket_ms,
            MetricWriterTables::metrics_default(),
        )
    }

    /// [`Self::with_landing_inserter`], but against `tables`.
    pub fn with_landing_inserter_and_tables(
        landing: Arc<dyn BlockInserter<MetricLandingRow>>,
        cfg: &WriterConfig,
        bucket_ms: i64,
        tables: MetricWriterTables,
    ) -> Self {
        Self::with_landing_inserter_and_runtime(
            landing,
            WriterRuntime::from_config(cfg),
            bucket_ms,
            tables,
        )
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
        landing: Arc<dyn BlockInserter<MetricLandingRow>>,
        runtime: WriterRuntime,
        bucket_ms: i64,
        tables: MetricWriterTables,
    ) -> Self {
        // Config-validated by `pulsus_config::validate` — a non-positive
        // bucket would make `floor_to_activity_bucket`'s own
        // `debug_assert!` fire, or divide by zero in a release build.
        debug_assert!(bucket_ms >= 1, "bucket_ms must be >= 1");

        let runtime = Arc::new(runtime);
        let metrics = Arc::new(MetricWriterMetrics::default());
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let spool = Arc::new(SpoolWriter::new(runtime.spool_dir.clone(), metrics.clone()));
        let boundary = DrainBoundary::new();
        let series_lru = Arc::new(Mutex::new(SeriesLru::new(runtime.lru_capacity)));

        // Issue #494: one index per signal. The landing insert is the one
        // target, so it is the one thing a suppressed caller's answer waits
        // on.
        let dedup = runtime.ingest_dedup.then(|| {
            PushDedup::new(
                runtime.ingest_dedup_max_bytes,
                runtime.ingest_dedup_window,
                runtime.claim_deadline,
            )
        });

        let ctx = Arc::new(LandingContext {
            table: tables.landing,
            inserter: landing,
            runtime: runtime.clone(),
            metrics: metrics.clone(),
            queued_bytes: queued_bytes.clone(),
            spool,
            series_lru: series_lru.clone(),
        });

        let (landing_tx, landing_rx) = mpsc::unbounded_channel::<LandingBlock>();
        // A mutex over the receiver rather than a `Notify` beside a deque,
        // which stores one permit and would leave a second queued block
        // waiting for a later push. A worker holds the lock only while
        // taking a block, so up to `metrics_landing_inserters` inserts
        // overlap.
        let landing_rx = Arc::new(tokio::sync::Mutex::new(landing_rx));
        // Every task goes into the boundary, which is what joins them: a task
        // this writer spawned and nothing awaits can be cancelled by runtime
        // teardown mid-settlement.
        boundary.track_background(
            (0..runtime.metrics_landing_inserters.max(1))
                .map(|_| spawn_landing_worker(ctx.clone(), landing_rx.clone(), boundary.watch()))
                .collect::<Vec<_>>(),
        );
        // The suppression index used to be ticked off a flush loop, and
        // there is no flush loop left here.
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
            series_lru,
            token_rng: Mutex::new(XorShift64::seeded()),
            bucket_ms,
            boundary,
        });

        MetricWriter { shared }
    }

    /// Admits `batch` as one sealed landing block under one atomic byte
    /// reservation. `mode` selects sync- versus async-mode admission.
    fn admit_batch(
        &self,
        batch: ParsedMetrics,
        mode: AdmitMode,
        push: PushHeaders,
    ) -> Result<Admitted, AdmitRefusal> {
        // The admission gate. The pass is held for the whole of this body,
        // which never awaits, and `shutdown` waits for every outstanding pass
        // before it announces the deadline — so a block admitted here is in
        // the queue before the drain can close it (issue #603 code review
        // round 5, finding 3).
        let Some(pass) = self.shared.boundary.enter() else {
            return Err(AdmitRefusal::Backpressure);
        };

        // Every row of a push carries the same stamp, so the block lies in
        // one partition.
        let received_ms = now_unix_millis();
        // The landing budget's anchor, taken before the claim and before any
        // admission work, because the claim deadline it must settle inside of
        // starts here too (issue #603 code review, finding 7). A budget
        // measured from the enqueue instead would believe it had its full
        // allowance after admission had already spent part of the deadline.
        let admitted_at = tokio::time::Instant::now();

        // Issue #494: the claim, taken before the byte reservation so a
        // request refused by backpressure leaves no claim behind. What this
        // change leaves exactly as it shipped is everything about a
        // CLIENT's re-push: its suppression, its responses and its index.
        //
        // Suppression applies to the rows where duplication is harmful and
        // not to the one where omission is: a suppressed push stores no
        // sample, series or histogram row, and still lands its descriptors.
        let mut suppressed: Option<Suppressed> = None;
        let mut guard = match &self.shared.dedup {
            Some(dedup) => {
                let id = push_dedup::metric_identity(&batch, &push);
                let rows = (batch.samples.len() + batch.hist_samples.len()) as u64;
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

        // A push's descriptors are its parser's metadata entries — one per
        // metric name per request already — locally deduped to the last
        // occurrence per name, and gated, cached and promoted by nothing.
        // Every push emits them: the table is a
        // `ReplacingMergeTree(updated_ns)` keyed on `metric_name`, so a
        // repeated descriptor collapses on merge, while NOT writing one can
        // be wrong, because the version is a receiver clock the push
        // identity deliberately excludes.
        let mut last_by_name: HashMap<&Arc<str>, &MetricMetadata> = HashMap::new();
        for meta in &batch.metadata {
            last_by_name.insert(&meta.metric_name, meta);
        }
        let descriptors: Vec<&MetricMetadata> = last_by_name.into_values().collect();
        let metadata_bytes: u64 = descriptors
            .iter()
            .map(|m| MetricLandingRow::est_landing_bytes(MetricMetadataRow::est_source_bytes(m)))
            .sum();

        // Reserve-before-materialize: estimate bytes and decide which
        // series are cache misses BEFORE cloning anything into a row shape.
        // Each landing row is charged the row the QUEUE holds — the union of
        // the four kinds' columns — plus the buffers that kind's target
        // estimator prices; [`landing_charge`] adds what the block holds
        // besides its rows.
        let sample_bytes: u64 = batch
            .samples
            .iter()
            .map(|s| MetricLandingRow::est_landing_bytes(MetricSampleRow::est_source_bytes(s)))
            .sum();
        let hist_sample_bytes: u64 = batch
            .hist_samples
            .iter()
            .map(|h| MetricLandingRow::est_landing_bytes(MetricHistSampleRow::est_source_bytes(h)))
            .sum();

        // Which kind-2 rows the push has to register, and what they cost.
        // This runs for a suppressed push too, because the push-size decision
        // below is over the whole push and has to be complete before any
        // branch can queue anything; its counters do not move for one, so
        // issue #494's suppression path is observably unchanged.
        let new_series = self.series_to_register(&batch, suppressed.is_none());
        let series_bytes: u64 = new_series
            .iter()
            .map(|(s, _, _)| {
                MetricLandingRow::est_landing_bytes(MetricSeriesRow::est_source_bytes(s))
            })
            .sum();

        let total_rows =
            (batch.samples.len() + batch.hist_samples.len() + new_series.len() + descriptors.len())
                as u64;

        // **A valid push with no rows of any kind is answered here**, before
        // the charge, the two ceilings and the reservation (issue #603 code
        // review round 5, finding 1). It makes no block and no insert, so it
        // is charged for none: one block's fixed overhead exceeds the smallest
        // accepted value of either byte limit, and charging an empty push for
        // a block that is never created refused it `413` or `429`.
        //
        // Its claim seals with no targets, so it completes at admission.
        if total_rows == 0 {
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
            landing_charge(sample_bytes + series_bytes + metadata_bytes + hist_sample_bytes);

        // The two per-push ceilings, counted over all four kinds, and decided
        // **before** either branch below queues anything (issue #603 code
        // review, finding 5). At or above the row limit the block would not be
        // strictly under the count both pinned row limits carry
        // (`QuerySettings::landing_insert`), and the server would end a block
        // at it. Returning here drops the un-sealed guard,
        // which removes the claim, so the client's retry of an unstored push is
        // stored rather than suppressed — and no bytes have been reserved yet.
        //
        // A push the index recognises as a repeat is refused the same way, and
        // for the same reason: it does not fit one block, so storing any part
        // of it — its descriptors included — would leave a `413` that stored
        // something. Its size is its own, over all four kinds, so one push
        // gets one answer whether or not it raced a copy of itself.
        if total_rows >= self.shared.runtime.metrics_landing_max_rows
            || total_bytes > self.shared.runtime.batch_bytes
        {
            return Err(AdmitRefusal::PushTooLarge {
                rows: total_rows,
                row_limit: self.shared.runtime.metrics_landing_max_rows,
                bytes: total_bytes,
                byte_limit: self.shared.runtime.batch_bytes,
            });
        }

        // The suppressed push stops here: its samples are already stored by
        // the push it repeats, and the only thing it still owes is its
        // descriptors. That block takes a reservation of its own below, and a
        // queue with no room for it answers this push backpressure instead of
        // the ORIGINAL push's outcome, with no descriptor row built or queued.
        // With the reservation granted the insert carries no claim and no
        // waiter, the caller is answered with the ORIGINAL push's outcome, and
        // the block runs `run_block` like any other: a failure before the send
        // stored nothing, one from the send onward leaves whether the
        // descriptors landed unknown. Nothing gates or caches a descriptor at
        // any of those endings, so the next push carrying the same ones emits
        // them again.
        if let Some(suppressed) = suppressed {
            if !descriptors.is_empty() {
                let descriptor_bytes = landing_charge(metadata_bytes);
                super::reserve_queued_bytes(
                    &self.shared.queued_bytes,
                    &self.shared.metrics.backpressure_total,
                    descriptor_bytes,
                    self.shared.runtime.queue_bytes_limit,
                )
                .map_err(AdmitRefusal::from)?;
                let rows: Vec<MetricLandingRow> = descriptors
                    .iter()
                    .map(|m| MetricLandingRow::metadata(received_ms, m))
                    .collect();
                self.shared
                    .metrics
                    .metadata_upserts_total
                    .fetch_add(rows.len() as u64, Ordering::Relaxed);
                self.queue_block(
                    &pass,
                    rows,
                    descriptor_bytes,
                    ClaimTicket::inert(),
                    None,
                    admitted_at,
                );
            }
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

        // Reservation secured: only now materialize the rows.
        let mut rows: Vec<MetricLandingRow> = Vec::with_capacity(total_rows as usize);
        rows.extend(
            batch
                .samples
                .iter()
                .map(|s| MetricLandingRow::float_sample(received_ms, s)),
        );
        rows.extend(
            batch
                .hist_samples
                .iter()
                .map(|h| MetricLandingRow::hist_sample(received_ms, h)),
        );
        for (series, bucket, value_type) in &new_series {
            rows.push(MetricLandingRow::series(
                received_ms,
                series,
                *bucket,
                *value_type,
            ));
        }
        rows.extend(
            descriptors
                .iter()
                .map(|m| MetricLandingRow::metadata(received_ms, m)),
        );

        if !new_series.is_empty() {
            self.shared
                .metrics
                .series_registrations_total
                .fetch_add(new_series.len() as u64, Ordering::Relaxed);
        }
        if !descriptors.is_empty() {
            self.shared
                .metrics
                .metadata_upserts_total
                .fetch_add(descriptors.len() as u64, Ordering::Relaxed);
        }

        debug_assert_eq!(
            rows.len() as u64,
            total_rows,
            "the rows materialized are the rows the push was charged and \
             measured for"
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
    /// nothing, which is exactly the push whose points were all rejected.
    fn count_parse_outcomes(&self, batch: &ParsedMetrics) {
        self.shared
            .metrics
            .collisions_total
            .fetch_add(batch.collisions, Ordering::Relaxed);
        self.shared
            .metrics
            .rejected_total
            .fetch_add(batch.rejected, Ordering::Relaxed);
    }

    /// The `(series, bucket, value_type)` triples a push has to register,
    /// derived read-only from the registration LRU.
    ///
    /// Buckets are derived per-*sample*, not per-series, so a
    /// backfilled/straddling request emits one kind-2 row per touched
    /// `(metric_name, fingerprint, bucket, value_type)`. Both float samples
    /// (`value_type = 0`) and native-histogram samples (`value_type = 1`)
    /// drive registration, so a series carrying both in one bucket registers
    /// BOTH rows.
    ///
    /// `count` is false for a suppressed push, which stores no registration
    /// row: it is asked only so the push-size decision is over the whole push,
    /// and the LRU hit/miss counters must not move for it.
    fn series_to_register<'a>(
        &self,
        batch: &'a ParsedMetrics,
        count: bool,
    ) -> Vec<(&'a SeriesRef, i64, u8)> {
        // An exact `(metric_name, fingerprint) -> &SeriesRef` index, built
        // once per admission and consulted per touched bucket. One
        // `SeriesRef` serves whichever of the float and histogram samples
        // reference that `(metric_name, fingerprint)`.
        let series_by_key: HashMap<(&str, Fingerprint), &SeriesRef> = batch
            .series
            .iter()
            .map(|s| ((s.metric_name.as_ref(), s.fingerprint), s))
            .collect();

        let mut seen_in_request: HashSet<SeriesKey> = HashSet::new();
        let mut new_series: Vec<(&'a SeriesRef, i64, u8)> = Vec::new();
        let mut lru = self
            .shared
            .series_lru
            .lock()
            .expect("series lru mutex poisoned");
        let float_keys = batch.samples.iter().map(|s| {
            (
                &s.metric_name,
                s.fingerprint,
                s.unix_milli,
                VALUE_TYPE_FLOAT,
            )
        });
        let hist_keys = batch.hist_samples.iter().map(|h| {
            (
                &h.metric_name,
                h.fingerprint,
                h.unix_milli,
                VALUE_TYPE_HISTOGRAM,
            )
        });
        for (metric_name, fingerprint, unix_milli, value_type) in float_keys.chain(hist_keys) {
            let bucket = floor_to_activity_bucket(unix_milli, self.shared.bucket_ms);
            let key: SeriesKey = (metric_name.clone(), fingerprint, bucket, value_type);
            if !seen_in_request.insert(key.clone()) {
                continue; // already queued by an earlier sample this request
            }
            if lru.contains(&key) {
                if count {
                    self.shared
                        .metrics
                        .series_lru_hits_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }
            if count {
                self.shared
                    .metrics
                    .series_lru_misses_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            let Some(series_ref) = series_by_key
                .get(&(metric_name.as_ref(), fingerprint))
                .copied()
            else {
                // The receiver's contract requires a `SeriesRef` for every
                // distinct series a request's samples touch — the writer never
                // panics on a caller-side contract violation, it just cannot
                // register a series it was never told the labels of. The
                // sample is still admitted.
                continue;
            };
            new_series.push((series_ref, bucket, value_type));
        }
        new_series
    }

    /// Seals one block — minting its token, so two byte-identical pushes
    /// carry different ones — and hands it to the queue.
    ///
    /// **A block that reaches here after the queue closed is settled by a task
    /// of this admission's own**, through the same ending a worker gives a
    /// block still queued at shutdown. Settling needs the spool, which is
    /// asynchronous, and admission is not; the task holds an `Arc` of the
    /// context and owns the block, so it needs nothing that could go away
    /// underneath it, and it resolves the waiter last — a caller awaiting its
    /// answer has by then seen the bytes released and the claim reported.
    ///
    /// **Nothing reaches that branch from outside**: the pass this takes is
    /// held for the whole admission, and the drain waits for every pass before
    /// the deadline that closes the queue is announced. It is reachable only
    /// through [`Self::reopen_admission_for_test`], which puts admission back
    /// past a gate the drain has already closed. The settlement is registered
    /// with the boundary either way, so it is a task the drain awaits rather
    /// than one runtime teardown can cancel mid-spool.
    fn queue_block(
        &self,
        pass: &AdmissionPass<'_>,
        rows: Vec<MetricLandingRow>,
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
            QuerySettings::landing_insert(&token, self.shared.runtime.metrics_landing_max_rows),
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
    /// That race is otherwise unreachable from outside: the drain closes the
    /// gate and waits for every pass before it announces the deadline a worker
    /// closes the queue on. Nothing in the server calls this.
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
    pub fn metrics(&self) -> MetricWriterMetricsSnapshot {
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

impl MetricSink for MetricWriter {
    fn admit(&self, batch: ParsedMetrics, push: PushHeaders) -> Result<(), AdmitRefusal> {
        self.admit_batch(batch, AdmitMode::Async, push).map(|_| ())
    }

    fn admit_flush(
        &self,
        batch: ParsedMetrics,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
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

/// A fresh time-ordered UUID (version 7) as the canonical hyphenated
/// lowercase string: 48 bits of Unix milliseconds, the version nibble `7`,
/// 12 bits of randomness, the two variant bits `10`, and 62 more bits of
/// randomness.
///
/// Written here rather than taken from a dependency: this crate needs one
/// string per admitted push and nothing else a UUID library offers, and the
/// layout is 128 bits of two fields it already has to hand.
///
/// It is the batch's identity for retry purposes only — it is not a landed
/// event's identity, which is the landing table's own `event_id` column, and
/// it is never derived from the block's content.
pub(crate) fn mint_landing_token(rng: &mut XorShift64) -> String {
    let unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
        & 0xFFFF_FFFF_FFFF;
    let rand_a = (rng.next_u64() & 0x0FFF) as u16;
    let rand_b = rng.next_u64() & 0x3FFF_FFFF_FFFF_FFFF;
    format!(
        "{:08x}-{:04x}-7{:03x}-{:04x}-{:012x}",
        ((unix_ms >> 16) & 0xFFFF_FFFF) as u32,
        (unix_ms & 0xFFFF) as u16,
        rand_a,
        0x8000u16 | ((rand_b >> 48) as u16 & 0x3FFF),
        rand_b & 0xFFFF_FFFF_FFFF,
    )
}

/// Spawns one insert worker on the landing queue. Every worker runs the same
/// loop: take the next block, run it to an ending, repeat; once the shutdown
/// signal fires, close the queue and settle what is left.
pub(crate) fn spawn_landing_worker(
    ctx: Arc<LandingContext>,
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<LandingBlock>>>,
    mut watch: DrainWatch,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Seeded from the clock, unless a case fixed the seed so its retry
        // delays are the same every run (`WriterRuntime::retry_jitter_seed`).
        let mut rng = match ctx.runtime.retry_jitter_seed {
            Some(seed) => XorShift64::from_seed(seed),
            None => XorShift64::seeded(),
        };
        loop {
            if watch.is_announced() {
                drain_queue(&ctx, &rx).await;
                return;
            }
            let taken = {
                let mut queue = rx.lock().await;
                tokio::select! {
                    block = queue.recv() => Taken::Block(block),
                    () = watch.until_announced() => Taken::ShuttingDown,
                }
            };
            match taken {
                Taken::Block(Some(block)) => {
                    run_block(&ctx, block, &mut watch, &mut rng).await;
                }
                // Every sender is gone, so nothing more can arrive.
                Taken::Block(None) => return,
                Taken::ShuttingDown => {
                    drain_queue(&ctx, &rx).await;
                    return;
                }
            }
        }
    })
}

enum Taken {
    Block(Option<LandingBlock>),
    ShuttingDown,
}

/// Closes the queue, then settles every block still in it through the
/// queued-shutdown ending — no attempt ever ran for one of them, so each
/// gets a fresh fate and is filed as provably not committed. Closing before
/// draining is what makes the drain terminate.
async fn drain_queue(
    ctx: &Arc<LandingContext>,
    rx: &Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<LandingBlock>>>,
) {
    let mut queue = rx.lock().await;
    queue.close();
    while let Some(block) = queue.recv().await {
        let mut fate = LandingFate::NeverSent(String::new());
        fate.saw_pre_send(MSG_SHUTDOWN_QUEUED.to_string());
        settle_block(ctx, block, fate, TerminalCause::ShuttingDown).await;
    }
}

/// Runs one block to an ending. Returns only once that block has settled.
///
/// **Two bounds, and the loop owns one of them.** The budget —
/// `WriterRuntime::landing_budget`, measured from the push's admission —
/// bounds the queue wait, every attempt and every sleep. It is **recomputed
/// after every attempt**, so a sleep can never carry the block past it, and
/// the check at the top of the loop is what ends a block whose sleep spent
/// what was left.
///
/// The announced shutdown deadline is `writer::drain`'s, and this loop neither
/// reads nor reasons about it: it asks [`DrainWatch::attempt`] for an attempt
/// and gets one of two shutdown endings instead when the deadline has passed
/// or passes in flight, and every wait between attempts is
/// [`DrainWatch::sleep`]. So the drain terminates by the deadline it announced
/// (`crates/pulsus-server/src/serve.rs:61`), and starts nothing after it but
/// what `writer::drain`'s two residuals name.
async fn run_block(
    ctx: &Arc<LandingContext>,
    block: LandingBlock,
    watch: &mut DrainWatch,
    rng: &mut XorShift64,
) {
    let mut fate = LandingFate::NeverSent(String::new());
    let mut resends = 0u32;
    loop {
        let remaining = ctx
            .runtime
            .landing_budget
            .saturating_sub(block.admitted_at.elapsed());
        if remaining.is_zero() {
            let msg = if resends == 0 {
                MSG_BUDGET_QUEUED
            } else {
                MSG_BUDGET_BETWEEN
            };
            fate.saw_pre_send(msg.to_string());
            settle_block(ctx, block, fate, TerminalCause::Normal).await;
            return;
        }

        ctx.metrics.landing.inflight.fetch_add(1, Ordering::Relaxed);
        // The budget encloses the two phases the client's own deadline does
        // not: the connection checkout and the health ping it may issue.
        let end = watch
            .attempt(remaining, || {
                ctx.inserter
                    .insert_with(&ctx.table, &block.rows, &block.settings)
            })
            .await;
        ctx.metrics.landing.inflight.fetch_sub(1, Ordering::Relaxed);

        let sent = match end {
            // The shutdown deadline had passed, so no attempt was created and
            // nothing was sent. The block keeps whatever fate its earlier
            // attempts left, and the queued-shutdown message is read only when
            // that fate is `NeverSent` — where it is exactly what happened,
            // since every pre-send ending means nothing was sent.
            AttemptEnd::NotStarted => {
                fate.saw_pre_send(MSG_SHUTDOWN_QUEUED.to_string());
                settle_block(ctx, block, fate, TerminalCause::ShuttingDown).await;
                return;
            }
            // The shutdown deadline with an attempt in flight. The attempt
            // is abandoned, and it may have committed.
            AttemptEnd::Abandoned => {
                fate.saw_uncertain(MSG_SHUTDOWN_INFLIGHT.to_string());
                settle_block(ctx, block, fate, TerminalCause::ShuttingDown).await;
                return;
            }
            // The budget elapsing mid-attempt. It encloses the checkout and
            // the health ping as well as the send and cannot tell which of
            // the three it interrupted, so it reports the fate as unknown
            // for a block that may never have left the process. That
            // over-reports and never under-reports, which is the direction
            // a claim has to err in.
            AttemptEnd::BudgetElapsed => {
                fate.saw_uncertain(MSG_BUDGET_IN_ATTEMPT.to_string());
                settle_block(ctx, block, fate, TerminalCause::Normal).await;
                return;
            }
            AttemptEnd::Done(result) => result,
        };

        // Every `ChError` here other than `InsertUncertain` is pre-send:
        // `insert_block_with` downgrades every failure from the send onward to
        // `InsertUncertain` whatever its class, and the only errors before it
        // are the connection checkout's and the target's column-metadata
        // read's.
        let resendable = match sent {
            Ok(()) => {
                let latency = block.admitted_at.elapsed();
                commit_block(ctx, block, latency).await;
                return;
            }
            Err(ChError::InsertUncertain(msg)) => {
                fate.saw_uncertain(msg);
                true
            }
            Err(e) if e.is_retryable() => {
                fate.saw_pre_send(e.to_string());
                true
            }
            Err(e) => {
                fate.saw_pre_send(e.to_string());
                false
            }
        };

        if !resendable || resends >= ctx.runtime.metrics_landing_retries {
            settle_block(ctx, block, fate, TerminalCause::Normal).await;
            return;
        }

        ctx.metrics
            .landing
            .retries_total
            .fetch_add(1, Ordering::Relaxed);
        // The remainder AFTER the attempt, not the one captured before it: a
        // failure arriving near expiry would otherwise sleep past the
        // budget.
        let left = ctx
            .runtime
            .landing_budget
            .saturating_sub(block.admitted_at.elapsed());
        let delay = crate::writer::table::backoff_delay(
            ctx.runtime.retry_base_delay,
            ctx.runtime.retry_max_delay,
            resends + 1,
            rng,
        )
        .min(left);
        watch.sleep(delay).await;
        resends += 1;
    }
}

/// The commit exit: the landing block holds the push's rows, and the four
/// targets are written by that insert's own processing.
///
/// **The promotion runs while the block is still charged.** It takes a mutex
/// admission and every other worker's commit take too, so a worker can wait
/// there holding a whole block; [`LandingBlock::release`] owns the rule and
/// says what waiting there would otherwise cost.
async fn commit_block(ctx: &Arc<LandingContext>, block: LandingBlock, latency: Duration) {
    ctx.metrics
        .landing
        .record_flush(block.rows.len() as u64, block.bytes, latency);
    {
        let mut lru = ctx.series_lru.lock().expect("series lru mutex poisoned");
        for key in promotion_keys(&block.rows) {
            lru.insert(key);
        }
    }
    if let Some(waiter) = block.release(ctx, TargetOutcome::Committed) {
        let _ = waiter.send(Ok(()));
    }
}

/// Every other exit. The fate decides the spool directory and the claim's
/// outcome; `cause` decides only the waiter's error.
///
/// The block is spooled even at the shutdown deadline — deliberately unlike
/// the shipped flush task, which spools nothing there — because the block is
/// the push's only copy. A spool write that itself fails is logged and
/// counted and never changes the outcome.
async fn settle_block(
    ctx: &Arc<LandingContext>,
    block: LandingBlock,
    fate: LandingFate,
    cause: TerminalCause,
) {
    let (kind, outcome, msg) = fate.settle();
    // The spool write runs while the block is still charged, on its error path
    // too (issue #603 code review, finding 4). The block's rows are what the
    // write reads, one at a time, so they are live until it returns. What the
    // write itself holds on top of them is one chunk and one row —
    // `writer::spool`'s `write_record` owns that bound — and the release order
    // that makes this the right place for it is
    // [`LandingBlock::release`]'s.
    if let Err(spool_err) = ctx.spool.write(kind, &ctx.table, &block.rows, &msg).await {
        ctx.metrics
            .landing
            .spool_write_failures_total
            .fetch_add(1, Ordering::Relaxed);
        error!(
            table = %ctx.table,
            error = %spool_err,
            "failed to spool a landing block to disk"
        );
    }
    if let Some(waiter) = block.release(ctx, outcome) {
        let err = match cause {
            TerminalCause::ShuttingDown => WriteError::ShuttingDown,
            TerminalCause::Normal => match kind {
                SpoolKind::Uncertain => WriteError::Uncertain(msg),
                SpoolKind::Poison => WriteError::Poisoned(msg),
            },
        };
        let _ = waiter.send(Err(err));
    }
}

#[cfg(test)]
mod tests {
    use pulsus_model::LabelSet;

    use super::*;
    use crate::ingest::metrics::{HistogramPoint, MetricPoint};
    use crate::writer::push_dedup::PushDigest;

    /// A metric name long enough that a per-series owned copy of it is visible
    /// beside the rows' own charge.
    const LONG_NAME: &str = "http_request_duration_seconds_bucket_by_upstream_cluster";

    /// What a [`ClaimTicket`]'s first `push` allocates: a `Vec` of
    /// [`PushDigest`], whose first allocation is four slots. The one claim a
    /// landing block carries fills one of them.
    const CLAIM_FIRST_ALLOCATION_BYTES: u64 = 4 * std::mem::size_of::<PushDigest>() as u64;

    /// What a sync caller's `oneshot` channel allocates: a state word, two
    /// waker slots and one `Result<(), WriteError>` come to
    /// 8 + 2 × 16 + 32 = 72. Stated at 128 because `oneshot`'s `Inner` is
    /// private to another crate and cannot be measured from here; a channel
    /// larger than this is the one thing below that neither this figure nor
    /// the charge would catch.
    const WAITER_CHANNEL_BYTES: u64 = 128;

    /// The activity-bucket width the cases below build a writer with. One hour
    /// in milliseconds, the shipped default.
    const BUCKET_MS: i64 = 3_600_000;

    /// A landing inserter that reports the call and then parks until the case
    /// releases it. That gap is where the block is admitted, charged, out of
    /// the queue and not yet committed — the one point from which a case can
    /// take the registration mutex knowing the commit has not reached it.
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

    impl BlockInserter<MetricLandingRow> for ParkingInserter {
        fn insert<'a>(
            &'a self,
            _table: &'a str,
            _rows: &'a [MetricLandingRow],
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

    /// Waits for `done` in real time, naming what never happened. The worker
    /// runs on a task of its own, so a case cannot yield to it.
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

    /// **A committed block keeps its reservation until the rows it was charged
    /// for are gone** (issue #603 code review round 10, finding 1). The commit
    /// exit promotes the push's new series into the registration LRU behind a
    /// mutex that every admission and every other worker's commit also takes,
    /// so a worker can wait there while holding a whole block. Handing the
    /// allowance back before that point gives it to a new admission while this
    /// block's rows — and the rows of every worker queued behind the same mutex
    /// — are all still live, and `PULSUS_INGEST_QUEUE_BYTES` permits more than
    /// it names.
    ///
    /// **The contention is genuine**: the case holds the writer's own
    /// registration mutex while a successful insert runs to its commit.
    /// `flushes_total` is the rendezvous — [`commit_block`] records the flush
    /// before it asks for the mutex — so the assertion is made at a point the
    /// worker has provably reached rather than after a sleep, and it is the
    /// release that has to have waited.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_committing_block_stays_charged_until_its_rows_are_released() {
        let inserter = Arc::new(ParkingInserter::new());
        let mut runtime = WriterRuntime::from_config(&pulsus_config::WriterConfig::default());
        // A committed block spools nothing. The root is this case's own
        // anyway, so a spool write it did not expect would not land in the
        // process working directory.
        runtime.spool_dir =
            std::env::temp_dir().join(format!("pulsus-landing-charge-{}", std::process::id()));
        runtime.metrics_landing_inserters = 1;
        let writer = MetricWriter::with_landing_inserter_and_runtime(
            inserter.clone(),
            runtime,
            BUCKET_MS,
            MetricWriterTables::metrics_default(),
        );

        let batch = ParsedMetrics {
            samples: vec![sample(7, 1_000)],
            series: vec![series_ref(7)],
            ..Default::default()
        };
        writer
            .admit(batch, PushHeaders::default())
            .expect("the push is admitted");
        let charged = writer.shared.queued_bytes.load(Ordering::SeqCst);
        assert!(charged > 0, "an admitted push holds a reservation");

        // The insert has begun, so the block has left the queue and admission
        // has finished with the registration mutex.
        inserter
            .entered
            .acquire()
            .await
            .expect("the entered semaphore is never closed")
            .forget();

        // A blocking thread of its own holds the mutex the commit has to take,
        // so nothing in this case holds a guard across an await.
        let (held_tx, held_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let lru = writer.shared.series_lru.clone();
        let holder = tokio::task::spawn_blocking(move || {
            let guard = lru.lock().expect("series lru mutex poisoned");
            held_tx.send(()).expect("the case waits for the mutex");
            release_rx.blocking_recv().ok();
            drop(guard);
        });
        held_rx.await.expect("the mutex is held");

        // Let the insert succeed. Once the flush is recorded the worker is
        // inside the commit and blocked on the promotion this case is holding.
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
            let mut lru = writer
                .shared
                .series_lru
                .lock()
                .expect("series lru mutex poisoned");
            lru.contains(&(
                Arc::from(LONG_NAME),
                Fingerprint::from_raw(7),
                floor_to_activity_bucket(1_000, BUCKET_MS),
                VALUE_TYPE_FLOAT,
            ))
        };
        assert!(
            registered,
            "the commit promoted the push's new series while the block was \
             still charged"
        );

        writer.shutdown(Duration::from_secs(5)).await;
    }

    /// **Every release of a landing block's reservation goes through one
    /// function**, and every take goes through `reserve_queued_bytes`
    /// (issue #603 code review round 10, finding 1).
    ///
    /// The charge covers what the block holds, so it can only be given back
    /// once the block is gone: [`LandingBlock::release`] owns that order, and
    /// an exit path that subtracted for itself would hand the allowance to a
    /// new admission with its own rows still live — which is what the commit
    /// exit did before this round. No behavioural case can be written against
    /// an exit path nobody has added yet, so this is what the next one is held
    /// to: it either goes through that function or this census names it.
    ///
    /// Lexical, over this module's own source, and the needles are built at
    /// run time so the census cannot match itself.
    #[test]
    fn the_landing_reservation_is_released_in_one_place() {
        const SRC: &str = include_str!("metric.rs");
        let gauge = "queued_bytes";
        let subtract = format!("{gauge}.{}", "fetch_sub");
        let add = format!("{gauge}.{}", "fetch_add");

        /// The function a line declares, if it declares one: what precedes
        /// `fn` has to be visibility and qualifiers, so prose naming a
        /// function does not open one.
        fn declared_fn(line: &str) -> Option<&str> {
            let (before, after) = line.trim_start().split_once("fn ")?;
            before
                .split_whitespace()
                .all(|w| {
                    matches!(
                        w,
                        "pub" | "pub(crate)" | "pub(super)" | "async" | "const" | "unsafe"
                    )
                })
                .then(|| after.split(['(', '<', ' ']).next().unwrap_or_default())
        }

        let lines: Vec<&str> = SRC.lines().collect();
        let mut current = "<the module body>";
        let mut subtracting: Vec<&str> = Vec::new();
        let mut adding: Vec<&str> = Vec::new();
        for (i, line) in lines.iter().enumerate() {
            if let Some(name) = declared_fn(line) {
                current = name;
            }
            // A statement `rustfmt` may have split over a receiver and a
            // method call, read as one string with its whitespace removed. It
            // ends at this line, so the function named is the one the call sits
            // in rather than one three lines above it.
            let window: String = lines[i.saturating_sub(2)..=i]
                .join("")
                .split_whitespace()
                .collect();
            if window.contains(&subtract) && !subtracting.contains(&current) {
                subtracting.push(current);
            }
            if window.contains(&add) && !adding.contains(&current) {
                adding.push(current);
            }
        }

        assert_eq!(
            subtracting,
            vec!["release"],
            "the queue gauge is decremented outside LandingBlock::release, \
             where the rows it is charged for are still live"
        );
        assert!(
            adding.is_empty(),
            "the queue gauge is incremented here rather than through \
             reserve_queued_bytes, which is the only gate that refuses over \
             the limit: {adding:?}"
        );
    }

    /// The `SeriesLru` keys a committed block registers come from the block's
    /// own kind-2 rows: such a row holds the metric name, the fingerprint, the
    /// activity-bucket floor in `unix_milli` and the value type, which is the
    /// whole key. Deriving them is what keeps a block from retaining one owned
    /// key per new series beside its rows (issue #603 code review round 4,
    /// finding 2) — a push of 10,000 new series would otherwise hold 10,000 of
    /// them, charged for none.
    #[test]
    fn the_promotion_keys_are_derived_from_the_blocks_kind_2_rows() {
        let series = series_ref(7);
        let rows = vec![
            MetricLandingRow::float_sample(5, &sample(7, 1_000)),
            MetricLandingRow::series(5, &series, 3_600_000, VALUE_TYPE_FLOAT),
            MetricLandingRow::metadata(5, &descriptor()),
            MetricLandingRow::series(5, &series, 7_200_000, VALUE_TYPE_HISTOGRAM),
        ];

        let keys: Vec<SeriesKey> = promotion_keys(&rows).collect();

        let expected: Vec<SeriesKey> = vec![
            (
                Arc::from(LONG_NAME),
                Fingerprint::from_raw(7),
                3_600_000,
                VALUE_TYPE_FLOAT,
            ),
            (
                Arc::from(LONG_NAME),
                Fingerprint::from_raw(7),
                7_200_000,
                VALUE_TYPE_HISTOGRAM,
            ),
        ];
        assert_eq!(
            keys, expected,
            "one key per kind-2 row and none for any other kind, each carrying that \
             row's own bucket floor and value type"
        );
    }

    /// Issue #603 code review round 4, finding 2: what a push is charged must
    /// cover everything the queue holds for it, not its rows alone. This
    /// prices a sealed block by walking it — every field, and every `String`
    /// and `Vec` reached through one, **by capacity rather than by length**,
    /// since the capacity is what the allocator is holding.
    ///
    /// **Three shapes, because a block's charge and what it holds do not scale
    /// together.** A one-row push is where the per-block half bites: every row
    /// carries the whole block's fixed cost in one row's charge. A
    /// series-heavy push is where a per-series retained key does: 60
    /// registrations of a 56-byte metric name held one owned copy each, and
    /// the charge covered none of them. The descriptor-only shape is the
    /// suppressed push's branch, which reserves and queues on its own.
    ///
    /// Two of the walk's terms are stated rather than measured —
    /// [`CLAIM_FIRST_ALLOCATION_BYTES`] and [`WAITER_CHANNEL_BYTES`], both
    /// inside types whose fields are private to another module. Everything
    /// else is read off the block, and no figure here comes from the constant
    /// the charge is built with.
    #[tokio::test]
    async fn the_charge_covers_everything_a_queued_block_holds() {
        struct Shape {
            name: &'static str,
            samples: Vec<MetricPoint>,
            hist_samples: Vec<HistogramPoint>,
            series: Vec<SeriesRef>,
            descriptors: Vec<MetricMetadata>,
        }

        let shapes = vec![
            Shape {
                name: "one sample",
                samples: vec![sample(1, 1_000)],
                hist_samples: Vec::new(),
                series: Vec::new(),
                descriptors: Vec::new(),
            },
            Shape {
                name: "one descriptor",
                samples: Vec::new(),
                hist_samples: Vec::new(),
                series: Vec::new(),
                descriptors: vec![descriptor()],
            },
            Shape {
                name: "sixty new series",
                samples: (0..60u128).map(|i| sample(i, 1_000)).collect(),
                hist_samples: vec![hist_sample(61, 1_000)],
                series: (0..60u128).map(series_ref).collect(),
                descriptors: vec![descriptor()],
            },
        ];

        for shape in shapes {
            // The four expressions `admit_batch` prices a push with.
            let row_bytes: u64 = shape
                .samples
                .iter()
                .map(|s| MetricLandingRow::est_landing_bytes(MetricSampleRow::est_source_bytes(s)))
                .sum::<u64>()
                + shape
                    .hist_samples
                    .iter()
                    .map(|h| {
                        MetricLandingRow::est_landing_bytes(MetricHistSampleRow::est_source_bytes(
                            h,
                        ))
                    })
                    .sum::<u64>()
                + shape
                    .series
                    .iter()
                    .map(|s| {
                        MetricLandingRow::est_landing_bytes(MetricSeriesRow::est_source_bytes(s))
                    })
                    .sum::<u64>()
                + shape
                    .descriptors
                    .iter()
                    .map(|m| {
                        MetricLandingRow::est_landing_bytes(MetricMetadataRow::est_source_bytes(m))
                    })
                    .sum::<u64>();

            // The rows `admit_batch` materializes for it, in its order.
            let total_rows = shape.samples.len()
                + shape.hist_samples.len()
                + shape.series.len()
                + shape.descriptors.len();
            let mut rows: Vec<MetricLandingRow> = Vec::with_capacity(total_rows);
            rows.extend(
                shape
                    .samples
                    .iter()
                    .map(|s| MetricLandingRow::float_sample(5, s)),
            );
            rows.extend(
                shape
                    .hist_samples
                    .iter()
                    .map(|h| MetricLandingRow::hist_sample(5, h)),
            );
            rows.extend(
                shape
                    .series
                    .iter()
                    .map(|s| MetricLandingRow::series(5, s, 3_600_000, VALUE_TYPE_FLOAT)),
            );
            rows.extend(
                shape
                    .descriptors
                    .iter()
                    .map(|m| MetricLandingRow::metadata(5, m)),
            );
            assert_eq!(rows.len(), total_rows, "{}", shape.name);

            let (tx, _rx) = oneshot::channel();
            let block = LandingBlock::seal(
                rows,
                landing_charge(row_bytes),
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
    fn held_bytes(block: &LandingBlock) -> u64 {
        // The block, and the queue slot the channel moves it into.
        let mut held = 2 * std::mem::size_of::<LandingBlock>() as u64;

        held += block.rows.capacity() as u64 * std::mem::size_of::<MetricLandingRow>() as u64;
        for row in &block.rows {
            held += (row.metric_name.capacity()
                + row.labels.capacity()
                + row.metric_type.capacity()
                + row.help.capacity()
                + row.unit.capacity()) as u64;
            held += (row.hist_pos_span_offsets.capacity() * std::mem::size_of::<i32>()
                + row.hist_pos_span_lengths.capacity() * std::mem::size_of::<u32>()
                + row.hist_pos_bucket_deltas.capacity() * std::mem::size_of::<i64>()
                + row.hist_neg_span_offsets.capacity() * std::mem::size_of::<i32>()
                + row.hist_neg_span_lengths.capacity() * std::mem::size_of::<u32>()
                + row.hist_neg_bucket_deltas.capacity() * std::mem::size_of::<i64>()
                + row.hist_custom_values.capacity() * std::mem::size_of::<f64>())
                as u64;
        }

        // By capacity, through the accessor: a `String` holds its capacity and
        // a `Vec` grows by doubling, so a figure over the entries' lengths
        // would understate the settings' retained allocation and this walk
        // would clear a charge that does not cover it (issue #603 code review
        // round 5, finding 4).
        held += block.settings.allocated_bytes();

        // Stated, not measured: see each constant.
        held += CLAIM_FIRST_ALLOCATION_BYTES;
        held += WAITER_CHANNEL_BYTES;
        held
    }

    fn label_set() -> LabelSet {
        LabelSet::from_normalized([
            ("job".to_string(), "checkout".to_string()),
            ("instance".to_string(), "10.0.0.7:9100".to_string()),
        ])
        .0
    }

    fn series_ref(fingerprint: u128) -> SeriesRef {
        SeriesRef {
            metric_name: Arc::from(LONG_NAME),
            fingerprint: Fingerprint::from_raw(fingerprint),
            labels: label_set(),
        }
    }

    fn sample(fingerprint: u128, unix_milli: i64) -> MetricPoint {
        MetricPoint {
            metric_name: Arc::from(LONG_NAME),
            fingerprint: Fingerprint::from_raw(fingerprint),
            unix_milli,
            value: 1.5,
        }
    }

    fn hist_sample(fingerprint: u128, unix_milli: i64) -> HistogramPoint {
        HistogramPoint {
            metric_name: Arc::from(LONG_NAME),
            fingerprint: Fingerprint::from_raw(fingerprint),
            unix_milli,
            histogram: pulsus_model::NativeHistogram {
                counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                schema: 0,
                zero_threshold: 0.0,
                zero_count: 0,
                count: 1,
                sum: 5.0,
                positive_spans: vec![pulsus_model::Span {
                    offset: 1,
                    length: 1,
                }],
                negative_spans: vec![],
                positive_buckets: vec![1],
                negative_buckets: vec![],
                custom_values: vec![],
            },
        }
    }

    fn descriptor() -> MetricMetadata {
        MetricMetadata {
            metric_name: Arc::from(LONG_NAME),
            metric_type: "histogram".to_string(),
            help: "the request duration".to_string(),
            unit: "seconds".to_string(),
            updated_ns: 7,
        }
    }

    /// The fate is a value the loop carries: an ending whose own knowledge
    /// is "this did not send" must not reverse an earlier attempt that may
    /// have sent, or a block that might be stored would be reported as
    /// provably not stored.
    #[test]
    fn a_pre_send_ending_never_walks_the_fate_back_from_uncertain() {
        let mut fate = LandingFate::NeverSent(String::new());
        fate.saw_uncertain("in flight".to_string());
        fate.saw_pre_send("never sent".to_string());
        assert_eq!(fate, LandingFate::Uncertain("in flight".to_string()));
        let (kind, outcome, msg) = fate.settle();
        assert_eq!(kind, SpoolKind::Uncertain);
        assert_eq!(outcome, TargetOutcome::Uncertain);
        assert_eq!(msg, "in flight");
    }

    /// Pre-send endings only: the block is provably not stored, so the claim
    /// is released and the client's retry is stored.
    #[test]
    fn pre_send_endings_settle_poison_and_not_committed() {
        let mut fate = LandingFate::NeverSent(String::new());
        fate.saw_pre_send("first".to_string());
        fate.saw_pre_send("second".to_string());
        assert_eq!(
            fate.settle(),
            (
                SpoolKind::Poison,
                TargetOutcome::NotCommitted,
                "second".to_string()
            )
        );
    }

    /// Two mints in one process differ — a token derived from the clock
    /// alone would repeat inside one millisecond, and the second push would
    /// be dropped as a resend of the first — and each is a version-7,
    /// variant-`10` UUID in canonical form.
    #[test]
    fn two_minted_tokens_differ_and_are_version_7_uuids() {
        let mut rng = XorShift64::seeded();
        let a = mint_landing_token(&mut rng);
        let b = mint_landing_token(&mut rng);
        assert_ne!(a, b, "two sealed blocks must not share a token");
        for token in [&a, &b] {
            assert_eq!(token.len(), 36, "canonical hyphenated form: {token}");
            let bytes = token.as_bytes();
            assert_eq!(bytes[8], b'-');
            assert_eq!(bytes[13], b'-');
            assert_eq!(bytes[18], b'-');
            assert_eq!(bytes[23], b'-');
            assert_eq!(bytes[14] as char, '7', "version nibble: {token}");
            assert!(
                matches!(bytes[19] as char, '8' | '9' | 'a' | 'b'),
                "variant bits 10: {token}"
            );
            assert!(
                token
                    .chars()
                    .all(|c| c == '-' || c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                "lowercase hex only: {token}"
            );
        }
    }
}
