//! `ProfileWriter`: **one profiles push is one `INSERT` of one block into
//! `profile_landing`**, the shape the log and trace writers have. Implements
//! [`crate::ingest::profiles::ProfileSink`].
//!
//! Five materialized views maintain `profile_samples`, `profile_stacks`,
//! `profile_series`, `profile_entries` and `profile_locations` from that one
//! source table, and this writer inserts into none of them. The loop, its two
//! fates, the budget, the shutdown boundary, the byte accounting and the
//! spool copy are `writer::landing`'s; what is here is what profiles decide
//! for themselves.
//!
//! **The stack and location cache** is the log stream cache's rule: keyed
//! `(tenant, id, receive UTC day)`, filled only at a commit from the
//! committed kind-1 and kind-4 rows, and emptied whole at every other ending.
//! A stack or location the cache holds is left out of the block. The day in
//! the key is what rewrites each stack on every day it is used, so there is
//! no hourly reset. A false miss writes a row the target's
//! `ReplacingMergeTree` collapses.
//!
//! **The acknowledgement is the landing insert's.** The claim is not replay
//! safe: samples are an append-only table, so a replayed sample doubles a
//! value, which is the log block's case. A retry after an insert that may
//! have sent is answered the original's outcome.
//!
//! **Nothing here subtracts the landing counter.** The reservation is taken
//! at admission and released exactly once, by whichever ending settles the
//! block, through `writer::landing`'s `LandingBlock::release`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChClient, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_model::Tenant;
use tokio::sync::{mpsc, oneshot};

use crate::error::LogsIngestError;
use crate::ingest::profiles::{ParsedProfileLanding, ProfileSink};
use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};
use crate::writer::config::WriterRuntime;
use crate::writer::drain::{AdmissionPass, DrainBoundary};
use crate::writer::error::WriteError;
use crate::writer::landing::{
    CommitHook, FailureHook, LandingBlock, LandingContext, LandingFate, LandingQueue,
    MSG_SHUTDOWN_QUEUED, TerminalCause, day_span, landing_charge, settle_block,
    spawn_landing_worker,
};
use crate::writer::metrics::{TraceWriterMetrics, TraceWriterMetricsSnapshot};
use crate::writer::push_dedup::{self, Admission, ClaimTicket, PushDedup};
use crate::writer::registration::LruSet;
use crate::writer::rows::{ProfileLandingRow, ProfileSeriesColumns};
use crate::writer::spool::SpoolWriter;
use crate::writer::table::{BlockInserter, ChBlockInserter, XorShift64, spawn_dedup_ticker};
use crate::writer::{AdmitMode, Admitted, Suppressed, note_target};

/// The bare landing table's name. On a cluster the writer names its
/// `Distributed` wrapper instead, `profile_landing<PULSUS_DIST_SUFFIX>`,
/// keyed on `cityHash64(received_ms)`: every row of a push carries one
/// receive stamp, so the push and every resend of it reach one shard.
pub(crate) const LANDING_TABLE: &str = "profile_landing";

/// One millisecond-resolution UTC day.
const MS_PER_DAY: i64 = 86_400_000;
/// One nanosecond-resolution UTC day.
const NS_PER_DAY: i64 = 86_400_000_000_000;

/// The UTC day, as days since the epoch, of a millisecond stamp: the day
/// the stacks and locations views store a row under.
fn receive_day(received_ms: i64) -> u16 {
    received_ms.div_euclid(MS_PER_DAY) as u16
}

/// A cache key: the tenant, a stack or location id, and the receive day.
type CacheKey = (Arc<str>, u128, u16);

/// What this writer's commits wrote since the cache was last emptied.
struct ProfileCache {
    stacks: LruSet<CacheKey>,
    locations: LruSet<CacheKey>,
}

/// The one table a [`ProfileWriter`] inserts into: the landing table's
/// `Distributed` wrapper on a cluster, the bare table single-node.
#[derive(Debug, Clone)]
pub struct ProfileWriterTables {
    pub landing: Arc<str>,
}

impl ProfileWriterTables {
    /// The bare landing table's name.
    pub fn profiles_default() -> Self {
        ProfileWriterTables {
            landing: Arc::from(LANDING_TABLE),
        }
    }

    /// The landing table for a deployment: the bare name for `None`, and
    /// `profile_landing<suffix>` for `Some(suffix)`.
    pub fn landing_for(dist_suffix: Option<&str>) -> Self {
        ProfileWriterTables {
            landing: Arc::from(format!("{LANDING_TABLE}{}", dist_suffix.unwrap_or(""))),
        }
    }
}

struct Shared {
    /// The landing queue's sender. Bounded by **bytes, not by length**: the
    /// reservation is the only gate.
    tx: mpsc::UnboundedSender<LandingBlock<ProfileLandingRow>>,
    ctx: Arc<LandingContext<ProfileLandingRow>>,
    queued_bytes: Arc<AtomicU64>,
    runtime: Arc<WriterRuntime>,
    metrics: Arc<TraceWriterMetrics>,
    /// The push-suppression index. `None` while `PULSUS_INGEST_DEDUP` is
    /// off.
    dedup: Option<Arc<PushDedup>>,
    cache: Arc<Mutex<ProfileCache>>,
    /// The generator of the profile ids this writer mints.
    id_rng: Mutex<XorShift64>,
    /// The shutdown boundary: the admission gate, the announced deadline and
    /// every task this writer spawned (`writer::drain`).
    boundary: DrainBoundary,
}

/// Cancels the drain boundary **before** any field is dropped, so no insert
/// worker is authorized an attempt once the writer's teardown has begun.
impl Drop for Shared {
    fn drop(&mut self) {
        self.boundary.cancel();
    }
}

/// Implements [`ProfileSink`] over one landing table. See the module docs.
pub struct ProfileWriter {
    shared: Arc<Shared>,
}

impl ProfileWriter {
    /// Production constructor against the landing table.
    pub fn new(client: Arc<ChClient>, cfg: &WriterConfig) -> Self {
        Self::new_with_tables(client, cfg, ProfileWriterTables::profiles_default())
    }

    /// [`Self::new`], but against `tables`.
    pub fn new_with_tables(
        client: Arc<ChClient>,
        cfg: &WriterConfig,
        tables: ProfileWriterTables,
    ) -> Self {
        Self::with_inserters_with_tables(Arc::new(ChBlockInserter::new(client)), cfg, tables)
    }

    /// Test/mock constructor: any landing [`BlockInserter`] against
    /// `tables`.
    pub fn with_inserters_with_tables(
        landing_inserter: Arc<dyn BlockInserter<ProfileLandingRow>>,
        cfg: &WriterConfig,
        tables: ProfileWriterTables,
    ) -> Self {
        Self::with_inserters_and_runtime(landing_inserter, WriterRuntime::from_config(cfg), tables)
    }

    /// [`Self::with_inserters_with_tables`], but against a caller-supplied
    /// [`WriterRuntime`], so a test can point the spool anywhere. No
    /// production call site depends on it.
    #[doc(hidden)]
    pub fn with_inserters_and_runtime(
        landing_inserter: Arc<dyn BlockInserter<ProfileLandingRow>>,
        runtime: WriterRuntime,
        tables: ProfileWriterTables,
    ) -> Self {
        let runtime = Arc::new(runtime);
        let metrics = Arc::new(TraceWriterMetrics::default());
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let spool = Arc::new(SpoolWriter::new(runtime.spool_dir.clone(), metrics.clone()));
        let boundary = DrainBoundary::new();
        let dedup = runtime.ingest_dedup.then(|| {
            PushDedup::new(
                runtime.ingest_dedup_max_bytes,
                runtime.ingest_dedup_window,
                runtime.claim_deadline,
            )
        });
        let cache = Arc::new(Mutex::new(ProfileCache {
            stacks: LruSet::new(runtime.lru_capacity),
            locations: LruSet::new(runtime.lru_capacity),
        }));

        // Filled only here, from a committed batch's own kind-1 and kind-4
        // rows, under the day the views store them under.
        let promote = cache.clone();
        let on_commit: CommitHook<ProfileLandingRow> =
            Arc::new(move |parts: &[&[ProfileLandingRow]]| {
                let mut cache = promote.lock().expect("profile cache mutex poisoned");
                for row in parts.iter().flat_map(|rows| rows.iter()) {
                    let day = receive_day(row.received_ms);
                    match row.kind {
                        ProfileLandingRow::KIND_STACK => {
                            cache
                                .stacks
                                .insert((Arc::clone(&row.org_id), row.stack_id, day))
                        }
                        ProfileLandingRow::KIND_LOCATION => {
                            cache
                                .locations
                                .insert((Arc::clone(&row.org_id), row.location_id, day))
                        }
                        _ => {}
                    }
                }
            });
        // Every ending but a commit empties both, and what they held is
        // dropped after the lock is released.
        let forget = cache.clone();
        let on_failure: FailureHook = Arc::new(move || {
            let dropped = {
                let mut cache = forget.lock().expect("profile cache mutex poisoned");
                (cache.stacks.take(), cache.locations.take())
            };
            drop(dropped);
        });

        let ctx = Arc::new(LandingContext {
            table: tables.landing,
            inserter: landing_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.landing.clone(),
            queued_bytes: queued_bytes.clone(),
            spool,
            on_commit: Some(on_commit),
            on_failure: Some(on_failure),
            retries: runtime.profile_landing_retries,
            max_rows: runtime.profile_landing_max_rows,
            // Profile blocks are batched, and each batch's insert is built
            // when it closes, under a token minted for it.
            batch_settings: Some(QuerySettings::landing_insert),
            token_rng: Mutex::new(XorShift64::seeded()),
        });

        let (tx, rx) = mpsc::unbounded_channel::<LandingBlock<ProfileLandingRow>>();
        let rx = Arc::new(tokio::sync::Mutex::new(LandingQueue::new(rx)));
        boundary.track_background(
            (0..runtime.profile_landing_inserters.max(1))
                .map(|_| spawn_landing_worker(ctx.clone(), rx.clone(), boundary.watch()))
                .collect::<Vec<_>>(),
        );
        boundary.track_background(
            dedup
                .clone()
                .map(|d| spawn_dedup_ticker(d, runtime.batch_age, boundary.watch())),
        );

        ProfileWriter {
            shared: Arc::new(Shared {
                tx,
                ctx,
                queued_bytes,
                runtime,
                metrics,
                dedup,
                cache,
                id_rng: Mutex::new(XorShift64::seeded()),
                boundary,
            }),
        }
    }

    /// Admits `landing` as one sealed block.
    ///
    /// The order is the suppression index, the cache, the per-push ceilings,
    /// then the byte reservation; any refusal stores nothing. A `Sync`
    /// push's block carries the waiter its answer comes from.
    fn admit_batch(
        &self,
        tenant: &Tenant,
        mut landing: ParsedProfileLanding,
        mode: AdmitMode,
        push: PushHeaders,
    ) -> Result<Admitted, AdmitRefusal> {
        // The admission gate, held for the whole of this body, which never
        // awaits.
        let Some(pass) = self.shared.boundary.enter() else {
            return Err(AdmitRefusal::Backpressure);
        };
        // Every landing row of a push carries the same stamp, so the block
        // lies in one landing partition.
        let received_ms = now_unix_millis();
        let admitted_at = tokio::time::Instant::now();
        let day = receive_day(received_ms);

        // The claim, taken before the byte reservation, so a request refused
        // by backpressure leaves no claim behind. **A suppressed push sends
        // nothing at all.**
        let mut suppressed: Option<Suppressed> = None;
        let mut guard = match &self.shared.dedup {
            Some(dedup) => {
                let id = push_dedup::profile_identity(&landing, &push, tenant);
                let rows = landing.samples.len() as u64;
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

        // The stacks and locations this writer has committed today for this
        // tenant are left out. The lookup runs for a suppressed push too,
        // because the size decision below is over what would be sent.
        {
            let mut cache = self
                .shared
                .cache
                .lock()
                .expect("profile cache mutex poisoned");
            let org = tenant.as_arc();
            landing
                .stacks
                .retain(|s| !cache.stacks.contains(&(Arc::clone(org), s.stack_id, day)));
            landing.locations.retain(|l| {
                !cache
                    .locations
                    .contains(&(Arc::clone(org), l.location_id, day))
            });
        }

        // **A push with no entries makes no block and no insert** and is
        // charged for nothing. Its claim seals with no targets.
        if landing.is_empty() {
            if let Some(suppressed) = suppressed {
                return Ok(Admitted::Suppressed(suppressed));
            }
            if let Some(guard) = guard {
                guard.seal();
            }
            return Ok(Admitted::Stored(Vec::new()));
        }

        // The per-push ceilings, over every kind that would be sent.
        // Returning here drops the un-sealed guard, which removes the claim.
        let landing_rows = landing.total_rows();
        let landing_bytes = charge_of(&landing) + tenant.as_str().len() as u64;
        if landing_rows >= self.shared.runtime.profile_landing_max_rows
            || landing_bytes > self.shared.runtime.batch_bytes
        {
            return Err(AdmitRefusal::PushTooLarge {
                rows: landing_rows,
                row_limit: self.shared.runtime.profile_landing_max_rows,
                bytes: landing_bytes,
                byte_limit: self.shared.runtime.batch_bytes,
            });
        }

        // The suppressed push stops here, having stored nothing, and is
        // answered what the original caller was told.
        if let Some(suppressed) = suppressed {
            return Ok(Admitted::Suppressed(suppressed));
        }

        super::reserve_queued_bytes(
            &self.shared.queued_bytes,
            &self.shared.metrics.backpressure_total,
            landing_bytes,
            self.shared.runtime.queue_bytes_limit,
        )
        .map_err(AdmitRefusal::from)?;

        let span = day_span(landing.entries.iter().map(|e| {
            landing.series[e.series as usize]
                .timestamp_ns
                .div_euclid(NS_PER_DAY)
        }));
        let rows = self.rows_of(tenant.as_arc(), received_ms, landing);
        debug_assert_eq!(
            rows.len() as u64,
            landing_rows,
            "the rows materialized are the rows the push was charged and measured for"
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
        self.queue(&pass, rows, landing_bytes, claim, waiter, admitted_at, span);

        // Arms the claim: from here a drop no longer removes it, and the
        // target set is closed.
        if let Some(guard) = guard {
            guard.seal();
        }
        Ok(Admitted::Stored(receivers))
    }

    /// The push's landed rows, every kind in one vector: series, entries,
    /// samples, then the stacks and locations not already cached.
    ///
    /// One profile id is minted per input whose entries carry none, and
    /// every entry of that input and every sample of those entries takes it.
    fn rows_of(
        &self,
        org_id: &Arc<str>,
        received_ms: i64,
        landing: ParsedProfileLanding,
    ) -> Vec<ProfileLandingRow> {
        let ParsedProfileLanding {
            series,
            entries,
            samples,
            stacks,
            locations,
        } = landing;
        let mut rows = Vec::with_capacity(
            series.len() + entries.len() + samples.len() + stacks.len() + locations.len(),
        );

        let columns: Vec<ProfileSeriesColumns> =
            series.iter().map(ProfileSeriesColumns::of).collect();
        rows.extend(
            series
                .into_iter()
                .map(|s| ProfileLandingRow::series(org_id, received_ms, s)),
        );

        let mut minted: HashMap<u32, (u64, u64)> = HashMap::new();
        let mut entry_meta: Vec<(u32, (u64, u64), i64)> = Vec::with_capacity(entries.len());
        for e in entries {
            let id = match &e.profile_id {
                Some(bytes) => ProfileLandingRow::profile_id_of(bytes),
                None => *minted
                    .entry(e.input)
                    .or_insert_with(|| self.mint_profile_id(received_ms)),
            };
            entry_meta.push((e.series, id, e.period));
            let cols = &columns[e.series as usize];
            rows.push(ProfileLandingRow::entry(org_id, received_ms, cols, e, id));
        }

        rows.extend(samples.into_iter().map(|s| {
            let (series, id, period) = entry_meta[s.entry as usize];
            ProfileLandingRow::sample(
                org_id,
                received_ms,
                &columns[series as usize],
                s,
                id,
                period,
            )
        }));
        rows.extend(
            stacks
                .into_iter()
                .map(|s| ProfileLandingRow::stack(org_id, received_ms, s)),
        );
        rows.extend(
            locations
                .into_iter()
                .map(|l| ProfileLandingRow::location(org_id, received_ms, l)),
        );
        rows
    }

    /// A fresh time-ordered UUID (version 7) as the column's two halves,
    /// laid out as `mint_landing_token` lays out its text: 48 bits of the
    /// push's receive milliseconds, the version nibble `7`, 12 random bits,
    /// the variant bits `10` and 62 more random bits.
    fn mint_profile_id(&self, received_ms: i64) -> (u64, u64) {
        let mut rng = self.shared.id_rng.lock().expect("profile id rng poisoned");
        let ms = (received_ms as u64) & 0xFFFF_FFFF_FFFF;
        let rand_a = rng.next_u64() & 0x0FFF;
        let rand_b = rng.next_u64() & 0x3FFF_FFFF_FFFF_FFFF;
        ((ms << 16) | 0x7000 | rand_a, 0x8000_0000_0000_0000 | rand_b)
    }

    /// Seals one block and hands it to the queue. Its insert's settings and
    /// token are the batch's, built when the batch closes. A block that
    /// reaches here after the queue closed is settled by a task of this
    /// admission's own, as a shut-down ending.
    #[allow(clippy::too_many_arguments)] // one admission's pass, claim and waiter travel together
    fn queue(
        &self,
        pass: &AdmissionPass<'_>,
        rows: Vec<ProfileLandingRow>,
        bytes: u64,
        claim: ClaimTicket,
        waiter: Option<oneshot::Sender<Result<(), WriteError>>>,
        admitted_at: tokio::time::Instant,
        span: Option<(i64, i64)>,
    ) {
        let block = LandingBlock::seal(
            rows,
            bytes,
            QuerySettings::new(),
            claim,
            waiter,
            admitted_at,
            span,
        );
        if let Err(closed) = self.shared.tx.send(block) {
            let ctx = self.shared.ctx.clone();
            let block = closed.0;
            let mut fate = LandingFate::NeverSent(String::new());
            fate.saw_pre_send(MSG_SHUTDOWN_QUEUED.to_string());
            self.shared.boundary.spawn_settlement(pass, async move {
                settle_block(&ctx, block, fate, TerminalCause::ShuttingDown).await;
            });
        }
    }

    /// A point-in-time metrics snapshot.
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

    /// This writer's push-suppression index, or `None` while
    /// `PULSUS_INGEST_DEDUP` is off.
    pub fn dedup(&self) -> Option<&Arc<PushDedup>> {
        self.shared.dedup.as_ref()
    }

    /// Graceful shutdown, which is the boundary's whole job
    /// (`writer::drain`). Idempotent.
    pub async fn shutdown(&self, deadline: Duration) {
        self.shared.boundary.shutdown(deadline).await;
    }
}

/// What the queue is charged for one push's landed rows, over the kinds
/// that would be sent, plus what the block itself holds. Taken before any
/// row is built. Shared text is charged on every row that holds it, which
/// over-charges: the safe direction.
fn charge_of(landing: &ParsedProfileLanding) -> u64 {
    let est = ProfileLandingRow::est_landing_bytes;
    let series: u64 = landing
        .series
        .iter()
        .map(|s| est(ProfileLandingRow::est_series_bytes(s)))
        .sum();
    let entries: u64 = landing
        .entries
        .iter()
        .map(|e| est(ProfileLandingRow::est_entry_bytes(e)))
        .sum();
    let samples = landing.samples.len() as u64 * est(0);
    let stacks: u64 = landing
        .stacks
        .iter()
        .map(|s| est(ProfileLandingRow::est_stack_bytes(s)))
        .sum();
    let locations: u64 = landing
        .locations
        .iter()
        .map(|l| est(ProfileLandingRow::est_location_bytes(l)))
        .sum();
    landing_charge::<ProfileLandingRow>(series + entries + samples + stacks + locations)
}

impl ProfileSink for ProfileWriter {
    fn admit(
        &self,
        tenant: &Tenant,
        landing: ParsedProfileLanding,
        push: PushHeaders,
    ) -> Result<(), AdmitRefusal> {
        self.admit_batch(tenant, landing, AdmitMode::Async, push)
            .map(|_| ())
    }

    fn admit_flush(
        &self,
        tenant: &Tenant,
        landing: ParsedProfileLanding,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
        match self.admit_batch(tenant, landing, AdmitMode::Sync, push)? {
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
/// epoch degrades to `0` rather than panicking.
fn now_unix_millis() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pulsus_clickhouse::{ChError, QuerySettings};
    use pulsus_profiles_wire::google_v1::{Function, Line, Location, Profile, Sample, ValueType};

    use super::*;
    use crate::ingest::profiles::{
        LandingEntry, LandingLocation, LandingSample, LandingSeries, LandingStack, ProfileInput,
    };
    use crate::writer::teardown_probe::spool_root;

    // -- the mock inserter --------------------------------------------

    /// What one insert call does.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Act {
        Ok,
        /// A non-retryable error: provably not committed.
        Poison,
        /// A failure after the block was sent: its fate is unknown.
        Uncertain,
    }

    /// A scripted mock [`BlockInserter`], `tests/trace_landing.rs`'s: the
    /// script is consumed front to back and its last step repeats. Records
    /// every call's table, rows and settings.
    struct MockInserter {
        script: Vec<Act>,
        calls: AtomicUsize,
        rows: Mutex<Vec<Vec<ProfileLandingRow>>>,
        settings: Mutex<Vec<Vec<(String, String)>>>,
        tables: Mutex<Vec<String>>,
        empty: QuerySettings,
    }

    impl MockInserter {
        fn new(script: Vec<Act>) -> Arc<Self> {
            Arc::new(MockInserter {
                script,
                calls: AtomicUsize::new(0),
                rows: Mutex::new(Vec::new()),
                settings: Mutex::new(Vec::new()),
                tables: Mutex::new(Vec::new()),
                empty: QuerySettings::new(),
            })
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn rows_of(&self, call: usize) -> Vec<ProfileLandingRow> {
            self.rows.lock().expect("mock mutex")[call].clone()
        }

        fn settings_of(&self, call: usize) -> Vec<(String, String)> {
            self.settings.lock().expect("mock mutex")[call].clone()
        }

        fn tables(&self) -> Vec<String> {
            self.tables.lock().expect("mock mutex").clone()
        }
    }

    impl BlockInserter<ProfileLandingRow> for MockInserter {
        fn insert<'a>(
            &'a self,
            table: &'a str,
            rows: &'a [ProfileLandingRow],
        ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
            self.insert_with(table, rows, &self.empty)
        }

        fn insert_with<'a>(
            &'a self,
            table: &'a str,
            rows: &'a [ProfileLandingRow],
            extra: &'a QuerySettings,
        ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.tables
                .lock()
                .expect("mock mutex")
                .push(table.to_string());
            self.rows.lock().expect("mock mutex").push(rows.to_vec());
            self.settings.lock().expect("mock mutex").push(
                extra
                    .entries()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
            let act = *self
                .script
                .get(call)
                .or(self.script.last())
                .expect("a non-empty script");
            Box::pin(async move {
                match act {
                    Act::Ok => Ok(()),
                    Act::Poison => Err(ChError::Decode("mock poison".to_string())),
                    Act::Uncertain => Err(ChError::InsertUncertain("mock uncertain".to_string())),
                }
            })
        }
    }

    // -- the fixture --------------------------------------------------

    fn now_ms() -> i64 {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .expect("a representable stamp")
    }

    /// Part A §0's profile: two sample types, locations `main`, `a`, `b`,
    /// stacks `main→a` and `main→b`, each value multiplied by `scale`.
    fn example_profile(time_nanos: i64, scale: i64) -> Profile {
        let st = [
            "",
            "samples",
            "count",
            "cpu",
            "nanoseconds",
            "main",
            "a",
            "b",
            "main.go",
        ];
        Profile {
            sample_type: vec![
                ValueType { r#type: 1, unit: 2 },
                ValueType { r#type: 3, unit: 4 },
            ],
            sample: vec![
                Sample {
                    location_id: vec![2, 1],
                    value: vec![3 * scale, 30_000_000 * scale],
                    label: Vec::new(),
                },
                Sample {
                    location_id: vec![3, 1],
                    value: vec![scale, 10_000_000 * scale],
                    label: Vec::new(),
                },
            ],
            function: (1..=3u64)
                .map(|id| Function {
                    id,
                    name: 4 + id as i64,
                    system_name: 0,
                    filename: 8,
                    start_line: 1,
                })
                .collect(),
            location: (1..=3u64)
                .map(|id| Location {
                    id,
                    mapping_id: 0,
                    address: 0,
                    line: vec![Line {
                        function_id: id,
                        line: 10,
                    }],
                    is_folded: false,
                })
                .collect(),
            string_table: st.iter().map(|s| (*s).to_owned()).collect(),
            time_nanos,
            duration_nanos: 10_000_000_000,
            period_type: Some(ValueType { r#type: 3, unit: 4 }),
            period: 10_000_000,
            ..Profile::default()
        }
    }

    fn example_input(scale: i64) -> ProfileInput {
        ProfileInput {
            labels: vec![
                ("__name__".to_owned(), "process_cpu".to_owned()),
                ("service_name".to_owned(), "checkout".to_owned()),
            ],
            profile: example_profile(now_ms() * 1_000_000, scale),
            decompressed_bytes: 1_000,
            id: None,
            annotations: Vec::new(),
            language: None,
        }
    }

    /// §0's push P, parsed by part A; `scale` 2 is P′.
    fn push(scale: i64) -> ParsedProfileLanding {
        crate::parse_profiles(vec![example_input(scale)], now_ms()).expect("the push is accepted")
    }

    fn tenant(name: &str) -> Tenant {
        Tenant::from_header(
            Some(&axum::http::HeaderValue::from_str(name).expect("a header value")),
            false,
        )
        .expect("a valid tenant")
    }

    fn config() -> WriterConfig {
        WriterConfig {
            landing_batch_ms: 0,
            ingest_dedup: false,
            profile_landing_retries: 0,
            ..WriterConfig::default()
        }
    }

    fn writer(cfg: &WriterConfig, root: &Path, inserter: Arc<MockInserter>) -> ProfileWriter {
        let mut runtime = WriterRuntime::from_config(cfg);
        runtime.spool_dir = root.to_path_buf();
        ProfileWriter::with_inserters_and_runtime(
            inserter,
            runtime,
            ProfileWriterTables::profiles_default(),
        )
    }

    async fn sync_push(
        writer: &ProfileWriter,
        who: &Tenant,
        landing: ParsedProfileLanding,
    ) -> Result<(), crate::error::LogsIngestError> {
        writer
            .admit_flush(who, landing, PushHeaders::default())
            .expect("the push is admitted")
            .await
    }

    fn count_kind(rows: &[ProfileLandingRow], kind: u8) -> usize {
        rows.iter().filter(|r| r.kind == kind).count()
    }

    fn spool_files(root: &Path, kind: &str) -> usize {
        std::fs::read_dir(root.join(kind).join("profile_landing"))
            .map(|d| {
                d.flatten()
                    .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("json"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// **W1.** The stack and location cache is keyed by tenant and is
    /// emptied by a failed insert, `tests/log_landing.rs`'s
    /// forget-on-failure case for the profile writer. Both failures: an
    /// unknown fate and a refused insert.
    #[tokio::test(start_paused = true)]
    async fn the_stack_cache_is_per_tenant_and_forgets_on_failure() {
        for (failure, spool_kind) in [(Act::Uncertain, "uncertain"), (Act::Poison, "poison")] {
            let root = spool_root("profile-cache");
            let inserter = MockInserter::new(vec![Act::Ok, Act::Ok, Act::Ok, failure, Act::Ok]);
            let writer = writer(&config(), &root, inserter.clone());
            let (t1, t2) = (tenant("t1"), tenant("t2"));

            sync_push(&writer, &t1, push(1))
                .await
                .expect("P as t1 commits");
            sync_push(&writer, &t1, push(2))
                .await
                .expect("P′ as t1 commits");
            sync_push(&writer, &t2, push(1))
                .await
                .expect("P as t2 commits");
            sync_push(&writer, &t1, push(2))
                .await
                .expect_err("the fourth insert fails");
            sync_push(&writer, &t1, push(2))
                .await
                .expect("P′ as t1 commits again");

            assert_eq!(inserter.call_count(), 5, "{failure:?}: one insert per push");
            let per_call: Vec<(usize, usize)> = (0..5)
                .map(|call| {
                    let rows = inserter.rows_of(call);
                    (
                        count_kind(&rows, ProfileLandingRow::KIND_STACK),
                        count_kind(&rows, ProfileLandingRow::KIND_LOCATION),
                    )
                })
                .collect();
            assert_eq!(
                per_call,
                vec![(2, 3), (0, 0), (2, 3), (0, 0), (2, 3)],
                "{failure:?}: stack and location rows per insert"
            );
            assert_eq!(
                spool_files(&root, spool_kind),
                1,
                "{failure:?}: the failed block was spooled"
            );

            writer.shutdown(Duration::from_secs(2)).await;
            std::fs::remove_dir_all(&root).ok();
        }
    }

    /// **W2.** One push is one insert into the landing table, carrying every
    /// kind, the tenant on every row, and one minted version-7 id shared by
    /// the input's entries and their samples.
    #[tokio::test(start_paused = true)]
    async fn one_push_is_one_insert_into_the_landing_table() {
        let root = spool_root("profile-one-insert");
        let inserter = MockInserter::new(vec![Act::Ok]);
        let writer = writer(&config(), &root, inserter.clone());

        sync_push(&writer, &tenant("t1"), push(1))
            .await
            .expect("the push commits");

        assert_eq!(inserter.call_count(), 1, "one insert");
        assert_eq!(inserter.tables(), vec!["profile_landing".to_string()]);
        let rows = inserter.rows_of(0);
        let kinds: Vec<usize> = (0..=4u8).map(|k| count_kind(&rows, k)).collect();
        assert_eq!(kinds, vec![4, 2, 2, 2, 3], "rows by kind 0 to 4");
        assert!(
            rows.iter().all(|r| &*r.org_id == "t1"),
            "every row carries the tenant"
        );
        let ids: Vec<(u64, u64)> = rows
            .iter()
            .filter(|r| r.kind == ProfileLandingRow::KIND_ENTRY)
            .map(|r| r.profile_id)
            .collect();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], ids[1], "one input, one id");
        assert_ne!(ids[0], (0, 0), "the id is minted");
        assert_eq!((ids[0].0 >> 12) & 0xF, 7, "a version-7 UUID");
        assert_eq!(ids[0].1 >> 62, 0b10, "the UUID variant");
        assert!(
            rows.iter()
                .filter(|r| r.kind == ProfileLandingRow::KIND_SAMPLE)
                .all(|r| r.profile_id == ids[0]),
            "every sample carries its entry's id"
        );

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// **W3.** The bare name without a suffix, the wrapper with one.
    #[test]
    fn landing_for_names_the_wrapper_only_when_given_a_suffix() {
        assert_eq!(
            &*ProfileWriterTables::landing_for(None).landing,
            "profile_landing"
        );
        assert_eq!(
            &*ProfileWriterTables::landing_for(Some("_d")).landing,
            "profile_landing_d"
        );
        assert_eq!(
            &*ProfileWriterTables::profiles_default().landing,
            "profile_landing"
        );
    }

    /// **W4.** The insert carries the landing pins, and a resend carries the
    /// very token the first attempt did.
    #[tokio::test(start_paused = true)]
    async fn the_insert_carries_the_landing_pins_and_one_token_across_resends() {
        let root = spool_root("profile-token");
        let inserter = MockInserter::new(vec![Act::Uncertain, Act::Ok]);
        let cfg = WriterConfig {
            profile_landing_retries: 1,
            ..config()
        };
        let writer = writer(&cfg, &root, inserter.clone());

        sync_push(&writer, &tenant("t1"), push(1))
            .await
            .expect("the resend commits");

        assert_eq!(inserter.call_count(), 2, "one attempt and one resend");
        let value = |call: usize, key: &str| {
            inserter
                .settings_of(call)
                .into_iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v)
                .unwrap_or_else(|| panic!("call {call} carries no {key}"))
        };
        let token = value(0, "insert_deduplication_token");
        assert!(!token.is_empty());
        assert_eq!(value(1, "insert_deduplication_token"), token);
        assert_eq!(
            value(0, "max_insert_block_size"),
            cfg.profile_landing_max_rows.to_string()
        );
        let pins = QuerySettings::landing_insert(&token, cfg.profile_landing_max_rows);
        let want: Vec<(String, String)> = pins
            .entries()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(inserter.settings_of(0), want, "the landing pins, whole");

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// **W5.** A push with no entries is answered `Ok`, makes no insert and
    /// is charged nothing.
    #[tokio::test(start_paused = true)]
    async fn an_empty_push_is_a_success_and_is_charged_for_nothing() {
        let root = spool_root("profile-empty");
        let inserter = MockInserter::new(vec![Act::Ok]);
        let writer = writer(&config(), &root, inserter.clone());

        let wait = writer.admit_flush(
            &tenant("t1"),
            ParsedProfileLanding::default(),
            PushHeaders::default(),
        );
        assert!(wait.is_ok(), "the empty push is admitted: {wait:?}");
        assert_eq!(writer.metrics().queue_bytes, 0, "nothing is charged");
        wait.expect("admitted").await.expect("answered Ok");
        assert_eq!(inserter.call_count(), 0, "no insert");

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A hand-built push of 2 series, 2 entries, `samples` samples, 3 stacks
    /// and 3 locations.
    fn sized_push(samples: usize) -> ParsedProfileLanding {
        let series = |t: &str| LandingSeries {
            type_id: Arc::from(t),
            service: Arc::from("checkout"),
            fingerprint: pulsus_model::Fingerprint::from_raw(7),
            timestamp_ns: now_ms() * 1_000_000,
            time_defaulted: false,
            labels: "{}".to_owned(),
        };
        let entry = |s: u32| LandingEntry {
            series: s,
            input: 0,
            profile_id: None,
            total: 1,
            duration_ns: 1,
            period: 1,
            annotation_keys: Vec::new(),
            annotation_values: Vec::new(),
        };
        ParsedProfileLanding {
            series: vec![series("a"), series("b")],
            entries: vec![entry(0), entry(1)],
            samples: (0..samples)
                .map(|i| LandingSample {
                    entry: (i % 2) as u32,
                    stack_id: (i % 3) as u128 + 1,
                    value: 1,
                    span_id: i as u64,
                    trace_id: [0; 16],
                })
                .collect(),
            stacks: (1..=3u128)
                .map(|id| LandingStack {
                    stack_id: id,
                    node_ids: vec![id],
                    names: vec![Arc::from("f")],
                    location_ids: vec![id],
                })
                .collect(),
            locations: (1..=3u128)
                .map(|id| LandingLocation {
                    location_id: id,
                    mapping_file: Arc::from(""),
                    mapping_build_id: Arc::from(""),
                    mapping_start: 0,
                    mapping_limit: 0,
                    mapping_offset: 0,
                    mapping_flags: 0,
                    address: 0,
                    line_functions: vec![Arc::from("f")],
                    line_system_names: vec![Arc::from("f")],
                    line_files: vec![Arc::from("x.go")],
                    line_start_lines: vec![1],
                    line_numbers: vec![1],
                })
                .collect(),
        }
    }

    /// **W6.** A push whose rows reach the row ceiling is refused whole and
    /// makes no insert.
    #[tokio::test(start_paused = true)]
    async fn a_push_at_the_row_ceiling_is_refused_whole() {
        let root = spool_root("profile-ceiling");
        let inserter = MockInserter::new(vec![Act::Ok]);
        let cfg = WriterConfig {
            profile_landing_max_rows: 1_000,
            ..config()
        };
        let writer = writer(&cfg, &root, inserter.clone());

        let landing = sized_push(990);
        assert_eq!(landing.total_rows(), 1_000);
        match writer.admit_flush(&tenant("t1"), landing, PushHeaders::default()) {
            Err(AdmitRefusal::PushTooLarge {
                rows: 1_000,
                row_limit: 1_000,
                ..
            }) => {}
            other => panic!("expected PushTooLarge at 1000 of 1000 rows, got {other:?}"),
        }
        // One row fewer fits.
        writer
            .admit_flush(&tenant("t1"), sized_push(989), PushHeaders::default())
            .expect("999 rows are admitted")
            .await
            .expect("and commit");
        assert_eq!(inserter.call_count(), 1, "only the push under the ceiling");

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// **W7.** The landing counter is subtracted nowhere in this module:
    /// `writer::landing`'s `LandingBlock::release` is the one place, as for
    /// traces.
    #[test]
    fn the_profile_landing_reservation_is_released_in_one_place() {
        const SOURCE: &str = include_str!("profile.rs");
        let body = SOURCE
            .split_once("#[cfg(test)]")
            .map(|(before, _)| before)
            .unwrap_or(SOURCE);
        let subtractions: Vec<&str> = body
            .lines()
            .filter(|line| line.contains("fetch_sub") || line.contains("saturating_sub"))
            .collect();
        assert!(
            subtractions.is_empty(),
            "this module must subtract the landing counter nowhere: {subtractions:?}"
        );
    }

    /// **W10.** One id is minted per input profile: a push of two inputs,
    /// each with two sample types and no id, has four entries under two
    /// distinct ids, and each sample carries its entry's.
    #[tokio::test(start_paused = true)]
    async fn one_id_is_minted_per_input_profile() {
        let root = spool_root("profile-ids");
        let inserter = MockInserter::new(vec![Act::Ok]);
        let writer = writer(&config(), &root, inserter.clone());

        let landing = crate::parse_profiles(vec![example_input(1), example_input(1)], now_ms())
            .expect("the push is accepted");
        let inputs: Vec<u32> = landing.entries.iter().map(|e| e.input).collect();
        assert_eq!(inputs, vec![0, 0, 1, 1], "two entries per input");
        sync_push(&writer, &tenant("t1"), landing)
            .await
            .expect("the push commits");

        let rows = inserter.rows_of(0);
        let entries: Vec<&ProfileLandingRow> = rows
            .iter()
            .filter(|r| r.kind == ProfileLandingRow::KIND_ENTRY)
            .collect();
        assert_eq!(entries.len(), 4);
        let (a, b) = (entries[0].profile_id, entries[2].profile_id);
        assert_eq!(entries[1].profile_id, a, "input 0's entries share one id");
        assert_eq!(entries[3].profile_id, b, "input 1's entries share one id");
        assert_ne!(a, b, "two inputs, two ids");
        assert_ne!(a, (0, 0));
        assert_ne!(b, (0, 0));
        for id in [a, b] {
            let types: Vec<&str> = entries
                .iter()
                .filter(|e| e.profile_id == id)
                .map(|e| &*e.type_id)
                .collect();
            let samples: Vec<&ProfileLandingRow> = rows
                .iter()
                .filter(|r| r.kind == ProfileLandingRow::KIND_SAMPLE && r.profile_id == id)
                .collect();
            assert_eq!(samples.len(), 4, "two types by two stacks under each id");
            assert!(
                samples.iter().all(|s| types.contains(&&*s.type_id)),
                "a sample's id is its own entry's"
            );
        }
        assert_eq!(
            count_kind(&rows, ProfileLandingRow::KIND_SAMPLE),
            8,
            "no sample is left without an id"
        );

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }
}
