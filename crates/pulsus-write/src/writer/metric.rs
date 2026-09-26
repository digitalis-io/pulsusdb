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
//! block. Every ending but a commit spools the block — it is the push's only
//! copy — reports the claim its fate, and answers a waiting sync caller
//! `500`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_model::{Fingerprint, floor_to_activity_bucket};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::warn;

use crate::error::LogsIngestError;
use crate::ingest::metrics::{MetricMetadata, MetricSink, ParsedMetrics, SeriesRef};
use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};
use crate::writer::config::WriterRuntime;
use crate::writer::error::WriteError;
use crate::writer::metrics::{MetricWriterMetrics, MetricWriterMetricsSnapshot};
use crate::writer::push_dedup::{self, Admission, ClaimTicket, PushDedup, TargetOutcome};
use crate::writer::registration::{SeriesKey, SeriesLru};
use crate::writer::rows::{
    MetricHistSampleRow, MetricLandingRow, MetricMetadataRow, MetricSampleRow, MetricSeriesRow,
};
use crate::writer::spool::{SpoolKind, SpoolWriter};
use crate::writer::table::{
    BlockInserter, ChBlockInserter, ShutdownSignal, XorShift64, spawn_dedup_ticker,
};
use crate::writer::{AdmitMode, Admitted, Suppressed, note_target};

/// The one table the metrics writer inserts into (issue #603).
const LANDING_TABLE: &str = "metric_landing";

/// The `metric_series.value_type` discriminant for a float sample.
const VALUE_TYPE_FLOAT: u8 = 0;
/// The `metric_series.value_type` discriminant for a native-histogram sample.
const VALUE_TYPE_HISTOGRAM: u8 = 1;

/// The message a block still queued when the budget ran out settles with.
#[allow(
    dead_code,
    reason = "the insert loop that reaches it arrives with the code"
)]
const MSG_BUDGET_QUEUED: &str = "the landing budget was spent before the block was sent";
/// The message a block whose budget ran out between attempts settles with.
#[allow(
    dead_code,
    reason = "the insert loop that reaches it arrives with the code"
)]
const MSG_BUDGET_BETWEEN: &str = "the landing budget was spent between attempts";
/// The message an attempt interrupted by the budget settles with.
#[allow(
    dead_code,
    reason = "the insert loop that reaches it arrives with the code"
)]
const MSG_BUDGET_IN_ATTEMPT: &str = "the landing budget elapsed during an attempt";
/// The message a block abandoned mid-attempt at the shutdown deadline
/// settles with.
#[allow(
    dead_code,
    reason = "the insert loop that reaches it arrives with the code"
)]
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

/// One admitted push, sealed and queued. A worker runs it to exactly one
/// ending and is the only place it settles.
#[allow(
    dead_code,
    reason = "the insert loop that reads every field arrives with the code"
)]
pub(crate) struct LandingBlock {
    /// The push's landing rows, every kind in one vector. Nothing depends on
    /// their order inside the block: the table's sorting key orders what is
    /// stored, and every consumer matches rows by `kind`.
    rows: Vec<MetricLandingRow>,
    /// Exactly what `reserve_queued_bytes` took for these rows. Released
    /// once, by whichever ending settles.
    bytes: u64,
    /// [`QuerySettings::landing_insert`], built once here and sent
    /// byte-identical on every resend.
    settings: QuerySettings,
    /// The push's issue-#494 obligation, or an inert ticket for a
    /// descriptor-only block and whenever `PULSUS_INGEST_DEDUP` is off.
    claim: ClaimTicket,
    /// The sync caller's waiter; `None` in async mode.
    waiter: Option<oneshot::Sender<Result<(), WriteError>>>,
    /// The `(metric_name, fingerprint, bucket, value_type)` keys this
    /// block's kind-2 rows register. Inserted into `SeriesLru` only on a
    /// commit.
    promote: Vec<SeriesKey>,
    /// Taken with the claim. The landing budget runs from here, so a block's
    /// queue wait is spent out of its own budget.
    ///
    /// `tokio::time::Instant`, not `std::time::Instant`: the budget is
    /// compared against the same clock the attempt's own timeout and the
    /// retry sleeps use, so a test that drives the loop on a paused clock
    /// measures one clock rather than two.
    admitted_at: tokio::time::Instant,
}

/// What a landing worker needs to run a block to an ending.
#[allow(
    dead_code,
    reason = "the insert loop that reads every field arrives with the code"
)]
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
#[allow(
    dead_code,
    reason = "the insert loop that writes Uncertain arrives with the code"
)]
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
    #[allow(
        dead_code,
        reason = "the insert loop that calls it arrives with the code"
    )]
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
    /// `reserve_queued_bytes` runs before a block is sent and is the only
    /// gate, so what the queue holds is bounded by
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
    shutdown: ShutdownSignal,
    shutting_down: AtomicBool,
    worker_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    dedup_ticker: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
        let (shutdown, shutdown_rx) = ShutdownSignal::new();
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
        let worker_tasks: Vec<_> = (0..runtime.metrics_landing_inserters.max(1))
            .map(|_| spawn_landing_worker(ctx.clone(), landing_rx.clone(), shutdown_rx.clone()))
            .collect();

        // The suppression index used to be ticked off a flush loop, and
        // there is no flush loop left here.
        let dedup_ticker = dedup
            .clone()
            .map(|d| spawn_dedup_ticker(d, runtime.batch_age, shutdown_rx.clone()));

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
            shutdown,
            shutting_down: AtomicBool::new(false),
            worker_tasks: Mutex::new(worker_tasks),
            dedup_ticker: Mutex::new(dedup_ticker),
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
        if self.shared.shutting_down.load(Ordering::Acquire) {
            return Err(AdmitRefusal::Backpressure);
        }

        // Every row of a push carries the same stamp, so the block lies in
        // one partition.
        let received_ms = now_unix_millis();

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
            .map(|m| MetricMetadataRow::est_source_bytes(m))
            .sum();

        // The suppressed push stops here: its samples are already stored by
        // the push it repeats, and the only thing it still owes is its
        // descriptors. That insert carries no claim and no waiter — the
        // caller is answered with the ORIGINAL push's outcome — and if it
        // fails nothing of it is stored, so the next push carrying the same
        // descriptors emits them again.
        if let Some(suppressed) = suppressed {
            return Ok(Admitted::Suppressed(suppressed));
        }

        self.shared
            .metrics
            .collisions_total
            .fetch_add(batch.collisions, Ordering::Relaxed);
        self.shared
            .metrics
            .rejected_total
            .fetch_add(batch.rejected, Ordering::Relaxed);

        // Reserve-before-materialize: estimate bytes and decide which
        // series are cache misses BEFORE cloning anything into a row shape.
        // A landing row costs what the target row it becomes costs, so the
        // queue keeps today's units.
        let sample_bytes: u64 = batch
            .samples
            .iter()
            .map(MetricSampleRow::est_source_bytes)
            .sum();
        let hist_sample_bytes: u64 = batch
            .hist_samples
            .iter()
            .map(MetricHistSampleRow::est_source_bytes)
            .sum();

        // An exact `(metric_name, fingerprint) -> &SeriesRef` index, built
        // once per admission and consulted per touched bucket. One
        // `SeriesRef` serves whichever of the float and histogram samples
        // reference that `(metric_name, fingerprint)`.
        let series_by_key: HashMap<(&str, Fingerprint), &SeriesRef> = batch
            .series
            .iter()
            .map(|s| ((s.metric_name.as_ref(), s.fingerprint), s))
            .collect();

        // Buckets are derived per-*sample*, not per-series, so a
        // backfilled/straddling request emits one kind-2 row per touched
        // `(metric_name, fingerprint, bucket, value_type)`. Both float
        // samples (`value_type = 0`) and native-histogram samples
        // (`value_type = 1`) drive registration, so a series carrying both
        // in one bucket registers BOTH rows.
        let mut seen_in_request: HashSet<SeriesKey> = HashSet::new();
        let mut new_series: Vec<(&SeriesRef, i64, u8)> = Vec::new();
        {
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
                    self.shared
                        .metrics
                        .series_lru_hits_total
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                self.shared
                    .metrics
                    .series_lru_misses_total
                    .fetch_add(1, Ordering::Relaxed);
                let Some(series_ref) = series_by_key
                    .get(&(metric_name.as_ref(), fingerprint))
                    .copied()
                else {
                    // The receiver's contract requires a `SeriesRef` for
                    // every distinct series a request's samples touch — the
                    // writer never panics on a caller-side contract
                    // violation, it just cannot register a series it was
                    // never told the labels of. The sample is still
                    // admitted below.
                    continue;
                };
                new_series.push((series_ref, bucket, value_type));
            }
        }
        let series_bytes: u64 = new_series
            .iter()
            .map(|(s, _, _)| MetricSeriesRow::est_source_bytes(s))
            .sum();

        let total_bytes = sample_bytes + series_bytes + metadata_bytes + hist_sample_bytes;
        let total_rows =
            (batch.samples.len() + batch.hist_samples.len() + new_series.len() + descriptors.len())
                as u64;

        // Atomic reservation: reserve first, roll back on overflow.
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
        let mut promote: Vec<SeriesKey> = Vec::with_capacity(new_series.len());
        for (series, bucket, value_type) in &new_series {
            rows.push(MetricLandingRow::series(
                received_ms,
                series,
                *bucket,
                *value_type,
            ));
            promote.push((
                Arc::from(series.metric_name.as_ref()),
                series.fingerprint,
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

        // A valid push with no rows of any kind makes no block and no
        // insert: its claim seals with no targets, completes at admission,
        // and it is answered a success.
        if rows.is_empty() {
            self.shared
                .queued_bytes
                .fetch_sub(total_bytes, Ordering::AcqRel);
            if let Some(guard) = guard {
                guard.seal();
            }
            return Ok(Admitted::Stored(Vec::new()));
        }

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

        self.queue_block(rows, total_bytes, claim, waiter, promote);

        // Arms the claim: from here a drop no longer removes it, and the
        // target set is closed.
        if let Some(guard) = guard {
            guard.seal();
        }
        Ok(Admitted::Stored(receivers))
    }

    /// Seals one block — minting its token, so two byte-identical pushes
    /// carry different ones — and hands it to the queue.
    ///
    /// **A block that reaches here after the queue closed is settled by this
    /// task**, through the same ending a worker gives a block still queued
    /// at shutdown: admission has already refused for a while by then
    /// (`shutting_down` is stored before the signal fires), so this is the
    /// narrow race where a push passed that check and the drain closed the
    /// queue before the send. Settling needs the spool, which is
    /// asynchronous, and admission is not, so the settle runs on a task of
    /// its own; it holds an `Arc` of the context and owns the block, so it
    /// needs nothing that could go away underneath it, and it resolves the
    /// waiter last — a caller awaiting its answer has by then seen the bytes
    /// released and the claim reported.
    fn queue_block(
        &self,
        rows: Vec<MetricLandingRow>,
        bytes: u64,
        claim: ClaimTicket,
        waiter: Option<oneshot::Sender<Result<(), WriteError>>>,
        promote: Vec<SeriesKey>,
    ) {
        let token = {
            let mut rng = self
                .shared
                .token_rng
                .lock()
                .expect("token rng mutex poisoned");
            mint_landing_token(&mut rng)
        };
        let block = LandingBlock {
            rows,
            bytes,
            settings: QuerySettings::landing_insert(
                &token,
                self.shared.runtime.metrics_landing_max_rows,
            ),
            claim,
            waiter,
            promote,
            admitted_at: tokio::time::Instant::now(),
        };
        if let Err(closed) = self.shared.landing_tx.send(block) {
            let ctx = self.shared.ctx.clone();
            let block = closed.0;
            let mut fate = LandingFate::NeverSent(String::new());
            fate.saw_pre_send(MSG_SHUTDOWN_QUEUED.to_string());
            tokio::spawn(async move {
                settle_block(&ctx, block, fate, TerminalCause::ShuttingDown).await;
            });
        }
    }

    /// Clears the shutting-down flag after [`Self::shutdown`] has returned,
    /// so the next admission passes that check and meets the closed queue.
    ///
    /// The race it reaches — a push past the flag check whose send finds the
    /// queue already closed — is otherwise unreachable from outside: the
    /// flag is stored before the signal fires, so by the time a worker has
    /// closed the queue admission is already refusing. Nothing in the server
    /// calls this.
    #[doc(hidden)]
    pub fn reopen_admission_for_test(&self) {
        self.shared.shutting_down.store(false, Ordering::Release);
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

    /// Graceful shutdown: stops admitting immediately (subsequent
    /// `admit`/`admit_flush` calls return `Backpressure`), then awaits every
    /// insert worker and the suppression index's ticker. Each worker closes
    /// the queue and settles whatever is still in it, which is what makes
    /// the drain terminate. Idempotent.
    pub async fn shutdown(&self, deadline: Duration) {
        self.shared.shutting_down.store(true, Ordering::Release);
        self.shared.shutdown.begin(Instant::now() + deadline);

        let workers: Vec<_> = std::mem::take(
            &mut *self
                .shared
                .worker_tasks
                .lock()
                .expect("task handle mutex poisoned"),
        );
        let ticker = self
            .shared
            .dedup_ticker
            .lock()
            .expect("task handle mutex poisoned")
            .take();

        for task in workers {
            if let Err(e) = task.await {
                warn!(error = %e, table = LANDING_TABLE, "landing insert worker panicked during shutdown");
            }
        }
        if let Some(task) = ticker
            && let Err(e) = task.await
        {
            warn!(error = %e, "the push-suppression ticker panicked during shutdown");
        }
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
    // Stubbed: the minting arrives with the code.
    let _ = rng;
    String::new()
}

/// Spawns one insert worker on the landing queue. Every worker runs the same
/// loop: take the next block, run it to an ending, repeat; once the shutdown
/// signal fires, close the queue and settle what is left.
pub(crate) fn spawn_landing_worker(
    ctx: Arc<LandingContext>,
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<LandingBlock>>>,
    mut shutdown_rx: watch::Receiver<Option<Instant>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut rng = XorShift64::seeded();
        loop {
            if shutdown_rx.borrow_and_update().is_some() {
                drain_queue(&ctx, &rx).await;
                return;
            }
            let taken = {
                let mut queue = rx.lock().await;
                tokio::select! {
                    block = queue.recv() => Taken::Block(block),
                    _ = shutdown_rx.changed() => Taken::ShuttingDown,
                }
            };
            match taken {
                Taken::Block(Some(block)) => {
                    run_block(&ctx, block, &mut shutdown_rx, &mut rng).await;
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
/// The budget — `WriterRuntime::landing_budget`, measured from the push's
/// admission — bounds the whole loop: the queue wait, every attempt and
/// every sleep. It is **recomputed after every attempt**, so a sleep can
/// never carry the block past it, and the check at the top of the loop is
/// what ends a block whose sleep spent what was left.
async fn run_block(
    ctx: &Arc<LandingContext>,
    block: LandingBlock,
    shutdown_rx: &mut watch::Receiver<Option<Instant>>,
    rng: &mut XorShift64,
) {
    // The loop inserts nothing yet: it takes the block, settles the claim
    // provably-not-committed and resolves the waiter, so a case fails on what
    // it asserts rather than timing out on a block nobody settles.
    let _ = (shutdown_rx, rng);
    let mut fate = LandingFate::NeverSent(String::new());
    fate.saw_pre_send("the landing loop is not built yet".to_string());
    settle_block(ctx, block, fate, TerminalCause::Normal).await;
}

/// The commit exit: the landing block holds the push's rows, and the four
/// targets are written by that insert's own processing.
#[allow(
    dead_code,
    reason = "the insert loop that reaches the commit exit arrives with the code"
)]
async fn commit_block(ctx: &Arc<LandingContext>, block: LandingBlock, latency: Duration) {
    ctx.queued_bytes.fetch_sub(block.bytes, Ordering::AcqRel);
    ctx.metrics
        .landing
        .record_flush(block.rows.len() as u64, block.bytes, latency);
    {
        let mut lru = ctx.series_lru.lock().expect("series lru mutex poisoned");
        for key in block.promote {
            lru.insert(key);
        }
    }
    block.claim.settle(TargetOutcome::Committed);
    if let Some(waiter) = block.waiter {
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
    ctx.queued_bytes.fetch_sub(block.bytes, Ordering::AcqRel);
    // Stubbed: writing the block to its spool directory arrives with the
    // code. The block is still settled, so a case fails on what it asserts
    // rather than timing out.
    let _ = &ctx.spool;
    block.claim.settle(outcome);
    if let Some(waiter) = block.waiter {
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
    use super::*;

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
