//! Issue #778 part 1: the per-process read cache of PromQL range answers.
//!
//! A range answer's points that can no longer change are kept, keyed by the
//! request's tenant, query text, grid and span. A repeat of the request is
//! answered from the kept points plus an evaluation of the points after them
//! (the tail), routed as the whole request is. Only plans built from an
//! allow-list ([`shape_allowed`]) use the cache; every other request is
//! evaluated in full and nothing is kept.
//!
//! **What is kept.** A point is kept once it is [`SETTLE_MS`] older than
//! `V` (the request time, or the label cache's last sweep when that is
//! older), its rollup buckets are built, and, clustered, only while every
//! `metric_*` replica has lagged under [`LAG_FILL_LIMIT_S`] since
//! `V − 10 s`. A kept point is what an uncached read of it returned when it
//! was kept. A sample pushed more than [`ON_TIME_MS`] after its own
//! timestamp can be missing from it until it is evaluated again, at the
//! latest [`ENTRY_MAX_AGE_MS`] after the entry was first filled.
//!
//! **Memory.** Every retained allocation is charged by capacity
//! ([`charge`]); past the budget the least recently used entries go, a
//! tenant past its share losing its own first. A float answer's points are
//! one allocation, so with the allocator's mapping threshold fixed
//! (`pulsus_server::allocator`) an evicted answer that was its own mapping
//! goes back to the system; one served from heap space stays unless glibc trims it.
//!
//! A plain in-memory, byte-weighted LRU: no new crate, nothing on disk,
//! nothing shared between nodes.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use pulsus_clickhouse::{ChClient, QuerySettings, Row};
use pulsus_model::{FloatHistogram, Tenant};
use pulsus_promql::plan::RangeSource;
use pulsus_promql::{AggOp, Annotations, FillValues, PlanExpr, QueryPlan, SelectorSpec, SetOp};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use super::exec::{MetricQueryParams, PlanFacts};
use super::rollup::LevelSlot;
use crate::logql::escape::ch_string;
use crate::logql::{HistMatrixSeries, HistOrFloat, MatrixSeries, QueryResult};

/// A sample whose push reaches PulsusDB within this of its timestamp is in
/// every kept point it affects (`L`).
pub const ON_TIME_MS: i64 = 60_000;
/// Receive to visible on every replica a read can use (`D`): the landing
/// bound 120 s, the lag gate 15 s, clock skew 5 s, spare 40 s.
pub const RECEIVE_TO_VISIBLE_MS: i64 = 180_000;
/// The freshness window `S = L + D`: no point newer than `V − S` is kept.
pub const SETTLE_MS: i64 = ON_TIME_MS + RECEIVE_TO_VISIBLE_MS;
/// How often [`spawn_poll_loop`] reads replica lag, watermarks and identity.
pub const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
/// A `metric_*` replica lagging this many seconds or more stops fills.
pub const LAG_FILL_LIMIT_S: u64 = 5;
/// The longest gap between two lag reads a fill accepts.
pub const LAG_READ_MAX_AGE_MS: i64 = 10_000;
/// An entry first filled longer ago than this is evaluated again in full
/// (`T_max`).
pub const ENTRY_MAX_AGE_MS: i64 = 3_600_000;
/// Map bucket, LRU links and the `Arc` header, per entry.
pub const ENTRY_OVERHEAD_BYTES: u64 = 256;
/// How far back the lag history reaches.
const LAG_HISTORY_MS: i64 = 300_000;
/// How long a successful identity read stays fresh.
const IDENTITY_FRESH_MS: i64 = 10_000;
/// The span `V − 10 s` of the lag gate.
const LAG_GATE_SPAN_MS: i64 = 10_000;
/// The annotation caps the response encoder renders with.
const ANNOTATION_CAP: usize = 10;

/// The cache's size and tenant share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadCacheConfig {
    pub budget_bytes: u64,
    pub tenant_share_pct: u8,
    /// Clustered: fills need the replica-lag gate.
    pub clustered: bool,
}

/// What one request is looked up by.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub tenant: Tenant,
    /// The raw query text: annotation positions index into it.
    pub query: Arc<str>,
    pub step_ms: i64,
    /// `start mod step`.
    pub phase_ms: i64,
    /// `end − start`, what `range()` would resolve to and the routing span.
    pub span_ms: i64,
    pub lookback_ms: i64,
    /// The rollup resolution the whole request reads at, `0` raw.
    pub resolution_ms: i64,
}

impl CacheKey {
    /// The key of a range request.
    pub fn new(
        tenant: Tenant,
        query: &str,
        p: &MetricQueryParams,
        lookback_ms: i64,
        resolution_ms: Option<i64>,
    ) -> Self {
        CacheKey {
            tenant,
            query: Arc::from(query),
            step_ms: p.step_ms,
            phase_ms: p.start_ms.rem_euclid(p.step_ms.max(1)),
            span_ms: p.end_ms - p.start_ms,
            lookback_ms,
            resolution_ms: resolution_ms.unwrap_or(0),
        }
    }
}

/// One series' kept points.
#[derive(Debug, Clone, PartialEq)]
pub enum Points {
    Float(Vec<(i64, f64)>),
    Mixed(Vec<(i64, HistOrFloat)>),
    /// A float series' points: its range of the one allocation every
    /// series of the entry shares.
    Flat(Arc<[(i64, f64)]>, Range<usize>),
}

impl Points {
    fn shrink(&mut self) {
        match self {
            Points::Float(v) => v.shrink_to_fit(),
            Points::Flat(..) => {}
            Points::Mixed(v) => {
                v.shrink_to_fit();
                for (_, p) in v.iter_mut() {
                    if let HistOrFloat::Hist(h) = p {
                        shrink_histogram(h);
                    }
                }
            }
        }
    }
}

fn shrink_histogram(h: &mut FloatHistogram) {
    h.positive_spans.shrink_to_fit();
    h.negative_spans.shrink_to_fit();
    h.positive_buckets.shrink_to_fit();
    h.negative_buckets.shrink_to_fit();
    h.custom_values.shrink_to_fit();
}

/// A kept answer: its rendered annotations, its series' points in
/// `[c0, c1]`, when it was first filled, and the charge bound.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    /// The answer's annotations as the encoder renders them, each prefixed
    /// `w:` (warning) or `i:` (info).
    pub annotations: Vec<String>,
    pub series: Vec<(Vec<(String, String)>, Points)>,
    pub c0_ms: i64,
    pub c1_ms: i64,
    pub first_fill_ms: i64,
    /// The samples a fresh evaluation of the kept answer's data charges at
    /// most.
    pub charge_bound: u64,
    /// What [`charge`] counted when the entry was stored.
    pub bytes: u64,
}

/// What [`ReadCache::store`] did, decided in one hold of the store's lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOutcome {
    /// The entry is stored; `through` is its `c1`.
    Published { through: Option<i64> },
    /// Nothing was stored; `through` is the `c1` of what is stored under
    /// the key, if anything.
    NotPublished { through: Option<i64> },
    /// A clear moved the generation: nothing was stored.
    Stale,
}

/// What the cache decided for one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Hit,
    Miss,
    Bypass(BypassReason),
}

/// Why a request did not use the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BypassReason {
    Instant,
    Explain,
    Shape,
    Retention,
    NoFill,
}

impl BypassReason {
    pub const ALL: [BypassReason; 5] = [
        BypassReason::Instant,
        BypassReason::Explain,
        BypassReason::Shape,
        BypassReason::Retention,
        BypassReason::NoFill,
    ];

    /// The `reason` label of `pulsus_read_cache_bypass_total`.
    pub fn as_str(self) -> &'static str {
        match self {
            BypassReason::Instant => "instant",
            BypassReason::Explain => "explain",
            BypassReason::Shape => "shape",
            BypassReason::Retention => "retention",
            BypassReason::NoFill => "no_fill",
        }
    }

    fn index(self) -> usize {
        BypassReason::ALL
            .iter()
            .position(|r| *r == self)
            .expect("listed")
    }
}

/// The value of `X-Pulsus-Read-Cache`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Header {
    Hit,
    Miss,
    Bypass,
}

impl Header {
    pub fn as_str(self) -> &'static str {
        match self {
            Header::Hit => "hit",
            Header::Miss => "miss",
            Header::Bypass => "bypass",
        }
    }
}

/// The cache's counters, read by `/metrics`.
#[derive(Debug, Default)]
pub struct ReadCacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    bypasses: AtomicU64,
    bypass_reasons: [AtomicU64; 5],
    evictions: AtomicU64,
    clears: AtomicU64,
    poll_failures: AtomicU64,
}

/// A copy of the counters and gauges, for `/metrics`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadCacheSnapshot {
    pub hits_total: u64,
    pub misses_total: u64,
    pub bypasses_total: u64,
    /// `(reason, count)` for every [`BypassReason`].
    pub bypass_total: Vec<(&'static str, u64)>,
    pub bytes: u64,
    pub entries: u64,
    pub evictions_total: u64,
    pub clears_total: u64,
    pub poll_failures_total: u64,
}

/// What the 5 s poll last read.
#[derive(Debug, Default)]
struct PollState {
    /// `(read at, max delay s)`, the last five minutes.
    lag_history: VecDeque<(i64, u64)>,
    /// `(read at, [(level width, watermark)])`.
    watermarks: Option<(i64, Vec<(i64, i64)>)>,
    identity: Option<Arc<[String]>>,
    /// The start of the last successful identity read.
    identity_ok_at: Option<i64>,
}

#[derive(Debug, Default)]
struct Store {
    map: HashMap<CacheKey, (Arc<Entry>, u64)>,
    /// LRU order: tick to key.
    lru: BTreeMap<u64, CacheKey>,
    tick: u64,
    total: u64,
    per_tenant: HashMap<Tenant, u64>,
}

impl Store {
    fn remove(&mut self, key: &CacheKey) -> Option<Arc<Entry>> {
        let (entry, tick) = self.map.remove(key)?;
        self.lru.remove(&tick);
        self.total = self.total.saturating_sub(entry.bytes);
        if let Some(t) = self.per_tenant.get_mut(&key.tenant) {
            *t = t.saturating_sub(entry.bytes);
            if *t == 0 {
                self.per_tenant.remove(&key.tenant);
            }
        }
        Some(entry)
    }

    fn clear(&mut self) {
        *self = Store {
            tick: self.tick,
            ..Store::default()
        };
    }
}

type Clock = Box<dyn Fn() -> i64 + Send + Sync>;
#[cfg(test)]
type Hook = Box<dyn Fn(&ReadCache) + Send + Sync>;

/// The per-process read cache.
pub struct ReadCache {
    store: Mutex<Store>,
    generation: AtomicU64,
    config: ReadCacheConfig,
    metrics: ReadCacheMetrics,
    poll: RwLock<PollState>,
    clock: Clock,
    /// TEST SEAM: run between a hit's merge and its store or final
    /// generation read.
    #[cfg(test)]
    after_merge: Option<Hook>,
}

fn wall_clock_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

impl ReadCache {
    pub fn new(config: ReadCacheConfig) -> Self {
        ReadCache {
            store: Mutex::new(Store::default()),
            generation: AtomicU64::new(0),
            config,
            metrics: ReadCacheMetrics::default(),
            poll: RwLock::new(PollState::default()),
            clock: Box::new(wall_clock_ms),
            #[cfg(test)]
            after_merge: None,
        }
    }

    #[cfg(test)]
    fn with_after_merge(mut self, hook: impl Fn(&ReadCache) + Send + Sync + 'static) -> Self {
        self.after_merge = Some(Box::new(hook));
        self
    }

    /// TEST SEAM: a clock other than the wall clock.
    #[doc(hidden)]
    pub fn with_clock(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.clock = Box::new(clock);
        self
    }

    pub fn config(&self) -> ReadCacheConfig {
        self.config
    }

    pub fn now_ms(&self) -> i64 {
        (self.clock)()
    }

    /// The fence a clear moves.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Empties the store and moves the generation, so a request that
    /// started before neither serves nor stores.
    pub fn clear(&self) {
        let mut store = lock(&self.store);
        self.generation.fetch_add(1, Ordering::SeqCst);
        store.clear();
        self.metrics.clears.fetch_add(1, Ordering::Relaxed);
    }

    /// The entry under `key`, made most recently used.
    pub fn lookup(&self, key: &CacheKey) -> Option<Arc<Entry>> {
        let mut store = lock(&self.store);
        let store = &mut *store;
        store.tick += 1;
        let tick = store.tick;
        let (entry, old) = store.map.get_mut(key)?;
        let entry = Arc::clone(entry);
        let old_tick = std::mem::replace(old, tick);
        store.lru.remove(&old_tick);
        store.lru.insert(tick, key.clone());
        Some(entry)
    }

    /// Stores `entry` under `key`, deciding in one hold of the store's lock
    /// (addendum 5 §2.3), in this order:
    ///
    /// 1. a clear moved the generation past `generation`: [`StoreOutcome::Stale`];
    /// 2. the entry's charge is above the tenant share or the budget: counted
    ///    as an eviction, and whatever is stored under `key` stays;
    /// 3. least recently used entries other than `key`'s are evicted, a
    ///    tenant past its share losing its own first, until the stored total
    ///    with the entry fits the budget; then `key`'s own if still needed;
    /// 4. the entry replaces whatever is stored under `key`.
    ///
    /// `through` is the `c1` of the entry stored under `key` when the lock is
    /// released. The entry is built and charged before the lock. Inside it
    /// the step allocates only for its own bookkeeping, the map and LRU
    /// order growing and copies of the key; an allocation failure there
    /// aborts the process, as anywhere in it, and nothing in the step
    /// panics, so no hold ends with the store half changed.
    pub fn store(&self, key: CacheKey, mut entry: Entry, generation: u64) -> StoreOutcome {
        entry.bytes = charge(&key, &entry);
        let entry = Arc::new(entry);
        let mut store = lock(&self.store);
        let store = &mut *store;
        if self.generation() != generation {
            return StoreOutcome::Stale;
        }
        let budget = self.config.budget_bytes;
        let share = budget * u64::from(self.config.tenant_share_pct) / 100;
        let stored_through = |store: &Store| store.map.get(&key).map(|(e, _)| e.c1_ms);
        if entry.bytes > share || entry.bytes > budget {
            self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            return StoreOutcome::NotPublished {
                through: stored_through(store),
            };
        }
        // What `key`'s tenant would hold with the entry in place of `key`'s.
        let tenant_with_entry = |store: &Store| {
            let own = store.map.get(&key).map_or(0, |(e, _)| e.bytes);
            store
                .per_tenant
                .get(&key.tenant)
                .copied()
                .unwrap_or(0)
                .saturating_sub(own)
                + entry.bytes
        };
        while store.total + entry.bytes > budget {
            let held = |store: &Store, t: &Tenant| {
                if *t == key.tenant {
                    tenant_with_entry(store)
                } else {
                    store.per_tenant.get(t).copied().unwrap_or(0)
                }
            };
            let over = store
                .lru
                .values()
                .find(|k| **k != key && held(store, &k.tenant) > share)
                .cloned();
            let victim = over
                .or_else(|| store.lru.values().find(|k| **k != key).cloned())
                .or_else(|| store.map.contains_key(&key).then(|| key.clone()));
            let Some(victim) = victim else {
                break;
            };
            store.remove(&victim);
            self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
        }
        if store.total + entry.bytes > budget {
            return StoreOutcome::NotPublished {
                through: stored_through(store),
            };
        }
        store.remove(&key);
        let through = entry.c1_ms;
        store.tick += 1;
        let tick = store.tick;
        store.total += entry.bytes;
        *store.per_tenant.entry(key.tenant.clone()).or_insert(0) += entry.bytes;
        store.lru.insert(tick, key.clone());
        store.map.insert(key, (entry, tick));
        StoreOutcome::Published {
            through: Some(through),
        }
    }

    /// The `c1` of the entry stored under `key`, without making it recently
    /// used.
    pub fn kept_through(&self, key: &CacheKey) -> Option<i64> {
        lock(&self.store).map.get(key).map(|(e, _)| e.c1_ms)
    }

    /// The charged bytes and entry count.
    pub fn usage(&self) -> (u64, u64) {
        let store = lock(&self.store);
        (store.total, store.map.len() as u64)
    }

    fn count(&self, header: Header, reason: Option<BypassReason>) {
        match header {
            Header::Hit => self.metrics.hits.fetch_add(1, Ordering::Relaxed),
            Header::Miss => self.metrics.misses.fetch_add(1, Ordering::Relaxed),
            Header::Bypass => self.metrics.bypasses.fetch_add(1, Ordering::Relaxed),
        };
        if let Some(r) = reason {
            self.metrics.bypass_reasons[r.index()].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Counts a request that bypassed the cache before reaching it.
    pub fn record_bypass(&self, reason: BypassReason) {
        self.count(Header::Bypass, Some(reason));
    }

    pub fn snapshot(&self) -> ReadCacheSnapshot {
        let (bytes, entries) = self.usage();
        let m = &self.metrics;
        ReadCacheSnapshot {
            hits_total: m.hits.load(Ordering::Relaxed),
            misses_total: m.misses.load(Ordering::Relaxed),
            bypasses_total: m.bypasses.load(Ordering::Relaxed),
            bypass_total: BypassReason::ALL
                .iter()
                .map(|r| {
                    (
                        r.as_str(),
                        m.bypass_reasons[r.index()].load(Ordering::Relaxed),
                    )
                })
                .collect(),
            bytes,
            entries,
            evictions_total: m.evictions.load(Ordering::Relaxed),
            clears_total: m.clears.load(Ordering::Relaxed),
            poll_failures_total: m.poll_failures.load(Ordering::Relaxed),
        }
    }

    // -- the poll's state ---------------------------------------------------

    fn poll_read(&self) -> std::sync::RwLockReadGuard<'_, PollState> {
        match self.poll.read() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn poll_write(&self) -> std::sync::RwLockWriteGuard<'_, PollState> {
        match self.poll.write() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// P1: the worst `metric_*` replica lag, read at `at_ms`.
    pub fn record_lag(&self, at_ms: i64, max_delay_s: u64) {
        let mut poll = self.poll_write();
        poll.lag_history.push_back((at_ms, max_delay_s));
        while poll
            .lag_history
            .front()
            .is_some_and(|(t, _)| *t < at_ms - LAG_HISTORY_MS)
        {
            poll.lag_history.pop_front();
        }
    }

    /// P2: each level's watermark, `(width, watermark)`, read at `at_ms`.
    pub fn record_watermarks(&self, at_ms: i64, watermarks: Vec<(i64, i64)>) {
        self.poll_write().watermarks = Some((at_ms, watermarks));
    }

    /// P3: the database identity set, read by a statement started at
    /// `started_ms`. A set other than the last clears the cache.
    pub fn record_identity(&self, started_ms: i64, ids: Vec<String>) {
        let ids: Arc<[String]> = ids.into();
        let changed = {
            let mut poll = self.poll_write();
            let changed = poll.identity.as_ref().is_some_and(|last| **last != *ids);
            poll.identity = Some(ids);
            poll.identity_ok_at = Some(started_ms);
            changed
        };
        if changed {
            self.clear();
        }
    }

    /// A poll statement failed.
    pub fn record_poll_failure(&self) {
        self.metrics.poll_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// The last successful identity read started under 10 s ago.
    pub fn identity_fresh(&self) -> bool {
        let now = self.now_ms();
        self.poll_read()
            .identity_ok_at
            .is_some_and(|t| now - t < IDENTITY_FRESH_MS)
    }

    /// Alone: true. Clustered: the lag history covers `[V − 10 s, now]` with
    /// no gap above [`LAG_READ_MAX_AGE_MS`], every read under
    /// [`LAG_FILL_LIMIT_S`].
    pub fn fills_allowed(&self, v_ms: i64) -> bool {
        if !self.config.clustered {
            return true;
        }
        let now = self.now_ms();
        let from = v_ms - LAG_GATE_SPAN_MS;
        let poll = self.poll_read();
        let reads: Vec<(i64, u64)> = poll
            .lag_history
            .iter()
            .copied()
            .filter(|(t, _)| *t <= now)
            .collect();
        let before = reads.iter().rev().find(|(t, _)| *t < from).copied();
        let mut covered: Vec<(i64, u64)> = before.into_iter().collect();
        covered.extend(reads.iter().copied().filter(|(t, _)| *t >= from));
        if covered.is_empty() {
            return false;
        }
        // Gaps are between consecutive reads, the read before `V − 10 s`
        // included; with none before it, the first gap is from `V − 10 s`.
        let mut edge = covered[0].0.min(from);
        for (t, lag) in &covered {
            if *lag >= LAG_FILL_LIMIT_S || *t - edge > LAG_READ_MAX_AGE_MS {
                return false;
            }
            edge = edge.max(*t);
        }
        now - edge <= LAG_READ_MAX_AGE_MS
    }

    /// `W`: the least latest watermark over the levels of width at most
    /// `res_ms`, `None` when none was read.
    pub fn watermark(&self, res_ms: i64) -> Option<i64> {
        let poll = self.poll_read();
        let (_, wms) = poll.watermarks.as_ref()?;
        wms.iter()
            .filter(|(w, _)| *w > 0 && *w <= res_ms)
            .map(|(_, wm)| *wm)
            .min()
    }
}

// -- what is kept, and its charge -------------------------------------------

/// The last grid point kept (`c1`), or `None` when nothing is kept (§5.4).
/// `res_ms` is `0` raw; at a level, `watermark` is `W`.
pub fn keep_through(
    v_ms: i64,
    reach_ms: i64,
    p: &MetricQueryParams,
    res_ms: i64,
    watermark: Option<i64>,
) -> Option<i64> {
    if p.step_ms <= 0 || p.end_ms < p.start_ms {
        return None;
    }
    let reach = reach_ms.max(0);
    let t_last = p.start_ms + (p.end_ms - p.start_ms).div_euclid(p.step_ms) * p.step_ms;
    let k = if res_ms == 0 {
        v_ms - SETTLE_MS - reach
    } else {
        let w = watermark?;
        let gl = t_last.div_euclid(res_ms) * res_ms;
        (v_ms - SETTLE_MS).min(w).min(gl) - reach - res_ms
    };
    let k = k.min(t_last);
    if k < p.start_ms {
        return None;
    }
    Some(p.start_ms + (k - p.start_ms).div_euclid(p.step_ms) * p.step_ms)
}

/// The rendered annotations an entry compares, as the encoder renders
/// them.
pub fn rendered_annotations(annotations: &Annotations, query: &str) -> Vec<String> {
    let (warnings, infos) = annotations.as_strings(query, ANNOTATION_CAP, ANNOTATION_CAP);
    warnings
        .into_iter()
        .map(|w| format!("w:{w}"))
        .chain(infos.into_iter().map(|i| format!("i:{i}")))
        .collect()
}

/// The indices of `points` with a timestamp in `[start, c1]`; the points
/// are in time order, as an evaluation returns them.
fn kept_range<P>(points: &[(i64, P)], start_ms: i64, c1_ms: i64) -> Range<usize> {
    let lo = points.partition_point(|(t, _)| *t < start_ms);
    let hi = points.partition_point(|(t, _)| *t <= c1_ms).max(lo);
    lo..hi
}

/// The points of `result` in `[start, c1]`, as an entry; `None` when the
/// result is not a matrix. A float answer's points go in one allocation
/// every series holds a range of; an answer with a histogram point in that
/// range keeps each series' points in a vector of its own, at its length.
fn keep(
    result: &QueryResult,
    start_ms: i64,
    c1_ms: i64,
    annotations: Vec<String>,
    first_fill_ms: i64,
    charge_bound: u64,
) -> Option<Entry> {
    let series = match result {
        QueryResult::Matrix(items) => {
            let parts: Vec<(&MatrixSeries, &[(i64, f64)])> = items
                .iter()
                .map(|s| (s, &s.points[kept_range(&s.points, start_ms, c1_ms)]))
                .filter(|(_, kept)| !kept.is_empty())
                .collect();
            flat_series(parts.iter().map(|(s, kept)| (&s.labels, *kept)), |p| *p)
        }
        QueryResult::MatrixHist(items) => {
            let parts: Vec<(&HistMatrixSeries, &[(i64, HistOrFloat)])> = items
                .iter()
                .map(|s| (s, &s.points[kept_range(&s.points, start_ms, c1_ms)]))
                .filter(|(_, kept)| !kept.is_empty())
                .collect();
            let any_hist = parts
                .iter()
                .any(|(_, kept)| kept.iter().any(|(_, x)| matches!(x, HistOrFloat::Hist(_))));
            if any_hist {
                parts
                    .iter()
                    .map(|(s, kept)| {
                        let mut v = Vec::with_capacity(kept.len());
                        v.extend(kept.iter().cloned());
                        (s.labels.clone(), Points::Mixed(v))
                    })
                    .collect()
            } else {
                flat_series(
                    parts.iter().map(|(s, kept)| (&s.labels, *kept)),
                    |(t, x)| match x {
                        HistOrFloat::Float(f) => (*t, *f),
                        HistOrFloat::Hist(_) => unreachable!("no histogram point"),
                    },
                )
            }
        }
        _ => return None,
    };
    let mut entry = Entry {
        annotations,
        series,
        c0_ms: start_ms,
        c1_ms,
        first_fill_ms,
        charge_bound,
        bytes: 0,
    };
    shrink_entry(&mut entry);
    Some(entry)
}

/// Every series' kept float points in one allocation, each series holding
/// its range of it.
fn flat_series<'a, P: 'a>(
    parts: impl Iterator<Item = (&'a Vec<(String, String)>, &'a [P])> + Clone,
    point: impl Fn(&P) -> (i64, f64),
) -> Vec<(Vec<(String, String)>, Points)> {
    let total: usize = parts.clone().map(|(_, kept)| kept.len()).sum();
    let mut all: Vec<(i64, f64)> = Vec::with_capacity(total);
    let mut ranges = Vec::new();
    for (labels, kept) in parts {
        let from = all.len();
        all.extend(kept.iter().map(&point));
        ranges.push((labels.clone(), from..all.len()));
    }
    let all: Arc<[(i64, f64)]> = Arc::from(all);
    ranges
        .into_iter()
        .map(|(labels, r)| (labels, Points::Flat(Arc::clone(&all), r)))
        .collect()
}

/// `shrink_to_fit` on every vector and string an entry keeps.
pub fn shrink_entry(entry: &mut Entry) {
    entry.annotations.shrink_to_fit();
    for a in &mut entry.annotations {
        a.shrink_to_fit();
    }
    entry.series.shrink_to_fit();
    for (labels, points) in &mut entry.series {
        labels.shrink_to_fit();
        for (k, v) in labels.iter_mut() {
            k.shrink_to_fit();
            v.shrink_to_fit();
        }
        points.shrink();
    }
}

/// Every retained allocation of `key` and `entry`, by capacity (§5.6).
pub fn charge(key: &CacheKey, entry: &Entry) -> u64 {
    use std::mem::size_of;
    let mut bytes = size_of::<CacheKey>() + key.query.len() + key.tenant.as_str().len();
    bytes += size_of::<Entry>() + ENTRY_OVERHEAD_BYTES as usize;
    bytes += entry.annotations.capacity() * size_of::<String>();
    bytes += entry
        .annotations
        .iter()
        .map(String::capacity)
        .sum::<usize>();
    bytes += entry.series.capacity() * size_of::<(Vec<(String, String)>, Points)>();
    for (labels, points) in &entry.series {
        bytes += labels.capacity() * size_of::<(String, String)>();
        bytes += labels
            .iter()
            .map(|(k, v)| k.capacity() + v.capacity())
            .sum::<usize>();
        bytes += match points {
            Points::Float(v) => v.capacity() * size_of::<(i64, f64)>(),
            // The one allocation every series shares, and its `Arc` counts,
            // counted on the series whose range starts it.
            Points::Flat(all, r) if r.start == 0 => all.len() * size_of::<(i64, f64)>() + 16,
            Points::Flat(..) => 0,
            Points::Mixed(v) => {
                v.capacity() * size_of::<(i64, HistOrFloat)>()
                    + v.iter()
                        .map(|(_, p)| match p {
                            HistOrFloat::Hist(h) => histogram_bytes(h),
                            HistOrFloat::Float(_) => 0,
                        })
                        .sum::<usize>()
            }
        };
    }
    bytes as u64
}

fn histogram_bytes(h: &FloatHistogram) -> usize {
    use std::mem::size_of;
    size_of::<FloatHistogram>()
        + h.positive_spans.capacity() * size_of::<pulsus_model::Span>()
        + h.negative_spans.capacity() * size_of::<pulsus_model::Span>()
        + h.positive_buckets.capacity() * size_of::<f64>()
        + h.negative_buckets.capacity() * size_of::<f64>()
        + h.custom_values.capacity() * size_of::<f64>()
}

/// A tail series' points.
enum TailPoints {
    Float(Vec<(i64, f64)>),
    Mixed(Vec<(i64, HistOrFloat)>),
}

/// One series' kept points as the merge reads them.
enum KeptPoints<'a> {
    Float(&'a [(i64, f64)]),
    Mixed(&'a [(i64, HistOrFloat)]),
}

fn kept_points(p: &Points) -> KeptPoints<'_> {
    match p {
        Points::Float(v) => KeptPoints::Float(v),
        Points::Flat(all, r) => KeptPoints::Float(&all[r.clone()]),
        Points::Mixed(v) => KeptPoints::Mixed(v),
    }
}

/// The kept points in `[start, c1]`, then the tail's (§5.3). A series with
/// no point in `[start, end]` is dropped; the answer is `MatrixHist` when a
/// histogram point remains. `None` when the tail is not a matrix. Each
/// output series is built once, at its length, and its labels cloned once.
pub fn merge(entry: &Entry, tail: QueryResult, start_ms: i64, end_ms: i64) -> Option<QueryResult> {
    let c1 = entry.c1_ms;
    let kept_in = |t: i64| t >= start_ms && t <= end_ms && t <= c1;
    let tail_in = |t: i64| t >= start_ms && t <= end_ms && t > c1;
    let mut tails: Vec<(Vec<(String, String)>, TailPoints)> = match tail {
        QueryResult::Matrix(items) => items
            .into_iter()
            .map(|s| (s.labels, TailPoints::Float(s.points)))
            .collect(),
        QueryResult::MatrixHist(items) => items
            .into_iter()
            .map(|s| (s.labels, TailPoints::Mixed(s.points)))
            .collect(),
        _ => return None,
    };
    let kept: Vec<(&Vec<(String, String)>, KeptPoints<'_>)> = entry
        .series
        .iter()
        .map(|(labels, p)| (labels, kept_points(p)))
        .collect();
    let kept_count = |k: &KeptPoints<'_>| match k {
        KeptPoints::Float(v) => v.iter().filter(|(t, _)| kept_in(*t)).count(),
        KeptPoints::Mixed(v) => v.iter().filter(|(t, _)| kept_in(*t)).count(),
    };
    let tail_count = |p: &TailPoints| match p {
        TailPoints::Float(v) => v.iter().filter(|(t, _)| tail_in(*t)).count(),
        TailPoints::Mixed(v) => v.iter().filter(|(t, _)| tail_in(*t)).count(),
    };
    let any_hist = kept.iter().any(|(_, k)| match k {
        KeptPoints::Mixed(v) => v
            .iter()
            .any(|(t, x)| kept_in(*t) && matches!(x, HistOrFloat::Hist(_))),
        KeptPoints::Float(_) => false,
    }) || tails.iter().any(|(_, p)| match p {
        TailPoints::Mixed(v) => v
            .iter()
            .any(|(t, x)| tail_in(*t) && matches!(x, HistOrFloat::Hist(_))),
        TailPoints::Float(_) => false,
    });
    // The tail series each kept series continues. A kept series with no
    // point in range leaves its tail to be placed in the tail's order.
    let mut used = vec![false; tails.len()];
    let pairs: Vec<(usize, Option<usize>)> = {
        let index: HashMap<&Vec<(String, String)>, usize> = tails
            .iter()
            .enumerate()
            .rev()
            .map(|(i, (labels, _))| (labels, i))
            .collect();
        kept.iter()
            .enumerate()
            .filter(|(_, (_, k))| kept_count(k) > 0)
            .map(|(i, (labels, _))| {
                let ti = index.get(*labels).copied().filter(|ti| !used[*ti]);
                if let Some(ti) = ti {
                    used[ti] = true;
                }
                (i, ti)
            })
            .collect()
    };
    let mut out = Output::new(any_hist, pairs.len() + used.iter().filter(|u| !**u).count());
    for (i, ti) in pairs {
        let (labels, k) = &kept[i];
        let n = kept_count(k) + ti.map_or(0, |ti| tail_count(&tails[ti].1));
        let mut series = out.series(n);
        match k {
            KeptPoints::Float(v) => series.floats(v.iter().filter(|(t, _)| kept_in(*t)).copied()),
            KeptPoints::Mixed(v) => series.mixed(v.iter().filter(|(t, _)| kept_in(*t)).cloned()),
        }
        if let Some(ti) = ti {
            series.tail(
                std::mem::replace(&mut tails[ti].1, TailPoints::Float(Vec::new())),
                tail_in,
            );
        }
        out.push((*labels).clone(), series);
    }
    for (ti, (labels, points)) in tails.into_iter().enumerate() {
        if used[ti] {
            continue;
        }
        let n = tail_count(&points);
        if n == 0 {
            continue;
        }
        let mut series = out.series(n);
        series.tail(points, tail_in);
        out.push(labels, series);
    }
    Some(out.finish())
}

/// The merge's answer, float or histogram, built series by series.
enum Output {
    Float(Vec<MatrixSeries>),
    Mixed(Vec<HistMatrixSeries>),
}

/// One output series' points, at the capacity it was counted to need.
enum OutSeries {
    Float(Vec<(i64, f64)>),
    Mixed(Vec<(i64, HistOrFloat)>),
}

impl Output {
    fn new(any_hist: bool, series: usize) -> Self {
        if any_hist {
            Output::Mixed(Vec::with_capacity(series))
        } else {
            Output::Float(Vec::with_capacity(series))
        }
    }

    fn series(&self, n: usize) -> OutSeries {
        match self {
            Output::Float(_) => OutSeries::Float(Vec::with_capacity(n)),
            Output::Mixed(_) => OutSeries::Mixed(Vec::with_capacity(n)),
        }
    }

    fn push(&mut self, labels: Vec<(String, String)>, series: OutSeries) {
        match (self, series) {
            (Output::Float(out), OutSeries::Float(points)) => {
                out.push(MatrixSeries { labels, points })
            }
            (Output::Mixed(out), OutSeries::Mixed(points)) => {
                out.push(HistMatrixSeries { labels, points })
            }
            _ => unreachable!("a series is built for its answer"),
        }
    }

    fn finish(self) -> QueryResult {
        match self {
            Output::Float(s) => QueryResult::Matrix(s),
            Output::Mixed(s) => QueryResult::MatrixHist(s),
        }
    }
}

impl OutSeries {
    fn floats(&mut self, points: impl Iterator<Item = (i64, f64)>) {
        match self {
            OutSeries::Float(v) => v.extend(points),
            OutSeries::Mixed(v) => v.extend(points.map(|(t, f)| (t, HistOrFloat::Float(f)))),
        }
    }

    fn mixed(&mut self, points: impl Iterator<Item = (i64, HistOrFloat)>) {
        match self {
            OutSeries::Float(v) => v.extend(points.map(|(t, x)| match x {
                HistOrFloat::Float(f) => (t, f),
                HistOrFloat::Hist(_) => unreachable!("no histogram point"),
            })),
            OutSeries::Mixed(v) => v.extend(points),
        }
    }

    fn tail(&mut self, points: TailPoints, tail_in: impl Fn(i64) -> bool) {
        match points {
            TailPoints::Float(v) => self.floats(v.into_iter().filter(|(t, _)| tail_in(*t))),
            TailPoints::Mixed(v) => self.mixed(v.into_iter().filter(|(t, _)| tail_in(*t))),
        }
    }
}

// -- the plan-shape gate (§5.9) ---------------------------------------------

/// `true` only when the plan uses no duration expression and every node and
/// selector is on the allow-list: a point at `t` depends only on samples in
/// a fixed-width window ending at `t − offset`, and its labels are decided
/// by that step alone.
pub fn shape_allowed(plan: &QueryPlan) -> bool {
    !plan.uses_duration_expr
        && plan.selectors.iter().all(selector_allowed)
        && node_allowed(&plan.root)
}

fn selector_allowed(s: &SelectorSpec) -> bool {
    s.fetch.at_ms.is_none() && s.at_ms.is_none() && !s.anchored && !s.smoothed && !s.in_subquery
}

fn is_scalar(e: &PlanExpr) -> bool {
    matches!(e, PlanExpr::Scalar(_))
}

fn operand_allowed(e: &PlanExpr) -> bool {
    is_scalar(e) || node_allowed(e)
}

fn node_allowed(e: &PlanExpr) -> bool {
    match e {
        PlanExpr::Selector(_) | PlanExpr::Scalar(_) => true,
        PlanExpr::RangeFn { source, .. } | PlanExpr::OverTime { source, .. } => {
            matches!(source, RangeSource::Selector(_))
        }
        PlanExpr::OverTimeParam { source, args, .. } => {
            matches!(source, RangeSource::Selector(_)) && args.iter().all(|a| is_scalar(a))
        }
        PlanExpr::Aggregate {
            op, expr, param, ..
        } => {
            matches!(
                op,
                AggOp::Sum
                    | AggOp::Avg
                    | AggOp::Min
                    | AggOp::Max
                    | AggOp::Count
                    | AggOp::Topk
                    | AggOp::Bottomk
            ) && param.as_deref().is_none_or(is_scalar)
                && node_allowed(expr)
        }
        PlanExpr::Binary { lhs, rhs, fill, .. } => {
            *fill == FillValues::default() && operand_allowed(lhs) && operand_allowed(rhs)
        }
        PlanExpr::SetOp { op, lhs, rhs, .. } => {
            matches!(op, SetOp::And | SetOp::Unless) && node_allowed(lhs) && node_allowed(rhs)
        }
        PlanExpr::HistogramQuantile { quantile, expr, .. } => {
            is_scalar(quantile) && node_allowed(expr)
        }
        _ => false,
    }
}

// -- the hit and miss paths (§5.2) ------------------------------------------

/// One range request, as the cache sees it.
pub struct RangeRequest<'a> {
    pub key: CacheKey,
    /// The raw query text the annotations render against.
    pub query: &'a str,
    pub params: MetricQueryParams,
    pub facts: PlanFacts,
    /// `PULSUS_PROMQL_MAX_SAMPLES`.
    pub max_samples: u64,
    /// `now − retention_days·1 d + 1 d`; `None` when the retention is not
    /// known yet.
    pub retention_floor_ms: Option<i64>,
    /// The label cache's last sweep for the tenant, `None` when cold.
    pub sweep_time_ms: Option<i64>,
}

/// An evaluation's answer: the result, its annotations, and what it
/// charged.
pub type Evaluated = (QueryResult, Annotations, u64);

/// What a request is answered with.
pub struct Answer {
    pub result: QueryResult,
    pub annotations: Annotations,
    pub outcome: Outcome,
    /// `X-Pulsus-Read-Cache`.
    pub header: Header,
    /// `X-Pulsus-Read-Cache-Through`: the last point kept, on `hit` and
    /// `miss` when anything is kept.
    pub through_ms: Option<i64>,
}

/// Answers `req` from the cache when it can, else from `full`. `tail` is
/// evaluated over the points after an entry's last kept one. The caller's
/// error, if any, is the answer, as it is without the cache.
pub async fn answer_range<E, F, FF, T, TF>(
    cache: &ReadCache,
    req: RangeRequest<'_>,
    full: F,
    tail: T,
) -> Result<Answer, E>
where
    F: FnOnce(MetricQueryParams) -> FF,
    FF: Future<Output = Result<Evaluated, E>>,
    T: FnOnce(MetricQueryParams) -> TF,
    TF: Future<Output = Result<Evaluated, E>>,
{
    let g0 = cache.generation();
    // The generation the miss path stores under: `g0`, or the one read
    // after a hit found a clear had run.
    let mut g = g0;
    let p = req.params;
    let refuse = |reason: BypassReason| {
        cache.count(Header::Bypass, Some(reason));
    };
    if !req.facts.shape_allowed {
        refuse(BypassReason::Shape);
        return bypass_full(full, p, BypassReason::Shape).await;
    }
    let retention_ok = req
        .retention_floor_ms
        .is_some_and(|floor| req.facts.oldest_input_ms >= floor);
    if !retention_ok {
        refuse(BypassReason::Retention);
        return bypass_full(full, p, BypassReason::Retention).await;
    }
    let now = cache.now_ms();
    let v = now.min(req.sweep_time_ms.unwrap_or(now));
    let res = req.key.resolution_ms;
    // `W`, read before the evaluation.
    let watermark = if res > 0 { cache.watermark(res) } else { None };
    let through = |answer: &QueryResult| {
        keep_through(v, req.facts.reach_ms, &p, res, watermark)
            .filter(|_| matches!(answer, QueryResult::Matrix(_) | QueryResult::MatrixHist(_)))
    };

    if let Some(e) = cache.lookup(&req.key)
        && e.c0_ms <= p.start_ms
        && p.start_ms <= e.c1_ms + p.step_ms
        && now - e.first_fill_ms < ENTRY_MAX_AGE_MS
        && cache.identity_fresh()
    {
        let tail_p = MetricQueryParams {
            start_ms: e.c1_ms + p.step_ms,
            end_ms: p.end_ms,
            step_ms: p.step_ms,
        };
        let (tail_result, tail_ann, tail_charged) = if tail_p.start_ms > tail_p.end_ms {
            (QueryResult::Matrix(Vec::new()), Annotations::new(), 0)
        } else {
            tail(tail_p).await?
        };
        let bound = e.charge_bound.saturating_add(tail_charged);
        if !tail_ann.has_mergeable()
            && rendered_annotations(&tail_ann, req.query) == e.annotations
            && bound <= req.max_samples
            && cache.identity_fresh()
            && cache.generation() == g0
            && let Some(answer) = merge(&e, tail_result, p.start_ms, p.end_ms)
        {
            #[cfg(test)]
            if let Some(hook) = &cache.after_merge {
                hook(cache);
            }
            let stored = if cache.fills_allowed(v)
                && cache.identity_fresh()
                && let Some(c1) = through(&answer)
                && let Some(entry) = keep(
                    &answer,
                    p.start_ms,
                    c1,
                    e.annotations.clone(),
                    e.first_fill_ms,
                    bound,
                ) {
                Some(cache.store(req.key.clone(), entry, g0))
            } else {
                None
            };
            let through_ms = match stored {
                Some(StoreOutcome::Published { through })
                | Some(StoreOutcome::NotPublished { through }) => Some(through),
                // A clear after the lookup: the merge is discarded.
                Some(StoreOutcome::Stale) => None,
                // Nothing stored: the kept points served only while no
                // clear has run since the lookup.
                None if cache.generation() == g0 => Some(cache.kept_through(&req.key)),
                None => None,
            };
            if let Some(through_ms) = through_ms {
                cache.count(Header::Hit, None);
                return Ok(Answer {
                    result: answer,
                    annotations: tail_ann,
                    outcome: Outcome::Hit,
                    header: Header::Hit,
                    through_ms,
                });
            }
            g = cache.generation();
        }
    }

    let (result, annotations, charged) = full(p).await?;
    let mut stored_through = None;
    if !annotations.has_mergeable()
        && cache.fills_allowed(v)
        && cache.identity_fresh()
        && let Some(c1) = through(&result)
        && let Some(entry) = keep(
            &result,
            p.start_ms,
            c1,
            rendered_annotations(&annotations, req.query),
            now,
            charged,
        )
        && let StoreOutcome::Published { through } = cache.store(req.key.clone(), entry, g)
    {
        stored_through = through;
    }
    let header = if stored_through.is_some() {
        cache.count(Header::Miss, None);
        Header::Miss
    } else {
        cache.count(Header::Bypass, Some(BypassReason::NoFill));
        Header::Bypass
    };
    Ok(Answer {
        result,
        annotations,
        outcome: Outcome::Miss,
        header,
        through_ms: stored_through,
    })
}

async fn bypass_full<E, F, FF>(
    full: F,
    p: MetricQueryParams,
    reason: BypassReason,
) -> Result<Answer, E>
where
    F: FnOnce(MetricQueryParams) -> FF,
    FF: Future<Output = Result<Evaluated, E>>,
{
    let (result, annotations, _) = full(p).await?;
    Ok(Answer {
        result,
        annotations,
        outcome: Outcome::Bypass(reason),
        header: Header::Bypass,
        through_ms: None,
    })
}

// -- the poll (§5.5) ---------------------------------------------------------

#[derive(Debug, Row, Serialize, Deserialize)]
struct LagRow {
    lag_s: u64,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct WatermarkRow {
    level: u32,
    shard: u32,
    watermark_ms: i64,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct IdentityRow {
    ids: Vec<String>,
}

/// P1: the worst replica lag of the metrics tables.
pub fn lag_sql(cluster: &str, db: &str) -> String {
    format!(
        "SELECT max(absolute_delay) AS lag_s FROM clusterAllReplicas({}, system.replicas) \
         WHERE database = {} AND startsWith(table, 'metric_') SETTINGS skip_unavailable_shards = 1",
        ch_string(cluster),
        ch_string(db)
    )
}

/// P2: every level's run-row watermark, per shard; `None` with no level.
pub fn watermark_sql(db: &str, tables: &[String], clustered: bool) -> Option<String> {
    if tables.is_empty() {
        return None;
    }
    let shard = if clustered {
        "_shard_num"
    } else {
        "toUInt32(1)"
    };
    Some(
        tables
            .iter()
            .enumerate()
            .map(|(i, t)| {
                format!(
                    "SELECT toUInt32({i}) AS level, toUInt32({shard}) AS shard, max(t_end) AS watermark_ms \
                     FROM {db}.{t} WHERE kind = 1 AND org_id = '' AND fingerprint = 0 GROUP BY level, shard"
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL "),
    )
}

/// P3: the database identity set.
pub fn identity_sql(cluster: Option<&str>, db: &str) -> String {
    match cluster {
        Some(c) => format!(
            "SELECT arraySort(groupUniqArray(toString(uuid))) AS ids \
             FROM clusterAllReplicas({}, system.databases) WHERE name = {} \
             SETTINGS skip_unavailable_shards = 1",
            ch_string(c),
            ch_string(db)
        ),
        None => format!(
            "SELECT arraySort(groupUniqArray(toString(uuid))) AS ids \
             FROM system.databases WHERE name = {}",
            ch_string(db)
        ),
    }
}

async fn fetch<R: pulsus_clickhouse::ChRow>(
    client: &ChClient,
    sql: &str,
) -> Result<Vec<R>, pulsus_clickhouse::ChError> {
    use futures::StreamExt;
    let mut stream = client.query_stream::<R>(sql, &QuerySettings::new()).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?);
    }
    Ok(out)
}

/// One poll: P1 (clustered), P2, P3.
pub async fn poll_once(
    cache: &ReadCache,
    client: &ChClient,
    db: &str,
    cluster: Option<&str>,
    levels: &LevelSlot,
) {
    if let Some(c) = cluster {
        let at = cache.now_ms();
        match fetch::<LagRow>(client, &lag_sql(c, db)).await {
            Ok(rows) => cache.record_lag(at, rows.first().map_or(0, |r| r.lag_s)),
            Err(err) => {
                cache.record_poll_failure();
                tracing::warn!(error = %err, "read cache: replica lag read failed");
            }
        }
    }
    if let Some(lv) = levels.get().filter(|l| !l.levels.is_empty()) {
        let tables: Vec<String> = lv.levels.iter().map(|l| l.table.clone()).collect();
        if let Some(sql) = watermark_sql(db, &tables, cluster.is_some()) {
            let at = cache.now_ms();
            match fetch::<WatermarkRow>(client, &sql).await {
                Ok(rows) => {
                    let shards = lv.shards.max(1);
                    let wms = lv
                        .levels
                        .iter()
                        .enumerate()
                        .map(|(i, l)| {
                            let wm = (1..=shards)
                                .map(|s| {
                                    rows.iter()
                                        .find(|r| r.level as usize == i && r.shard == s)
                                        .map_or(0, |r| r.watermark_ms)
                                })
                                .min()
                                .unwrap_or(0);
                            (l.res_ms, wm)
                        })
                        .collect();
                    cache.record_watermarks(at, wms);
                }
                Err(err) => {
                    cache.record_poll_failure();
                    tracing::warn!(error = %err, "read cache: watermark read failed");
                }
            }
        }
    }
    let started = cache.now_ms();
    match fetch::<IdentityRow>(client, &identity_sql(cluster, db)).await {
        Ok(rows) => cache.record_identity(
            started,
            rows.into_iter().next().map_or_else(Vec::new, |r| r.ids),
        ),
        Err(err) => {
            cache.record_poll_failure();
            tracing::warn!(error = %err, "read cache: identity read failed");
        }
    }
}

/// The poll loop: every [`POLL_INTERVAL`], the first at once.
pub fn spawn_poll_loop(
    cache: Arc<ReadCache>,
    client: ChClient,
    db: String,
    cluster: Option<String>,
    levels: Arc<LevelSlot>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            poll_once(&cache, &client, &db, cluster.as_deref(), &levels).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicUsize};

    const V: i64 = 10_000_000_000;

    struct Rig {
        cache: ReadCache,
        clock: Arc<AtomicI64>,
    }

    fn rig_with(budget: u64, share: u8, clustered: bool) -> Rig {
        let clock = Arc::new(AtomicI64::new(V));
        let c = Arc::clone(&clock);
        let cache = ReadCache::new(ReadCacheConfig {
            budget_bytes: budget,
            tenant_share_pct: share,
            clustered,
        })
        .with_clock(move || c.load(Ordering::SeqCst));
        Rig { cache, clock }
    }

    /// A single-node cache with a fresh identity read.
    fn rig() -> Rig {
        let r = rig_with(64 * 1024 * 1024, 50, false);
        r.cache.record_identity(V, vec!["db".to_string()]);
        r
    }

    impl Rig {
        fn now(&self) -> i64 {
            self.clock.load(Ordering::SeqCst)
        }
        fn advance(&self, ms: i64) {
            self.clock.fetch_add(ms, Ordering::SeqCst);
        }
        fn refresh_identity(&self) {
            self.cache
                .record_identity(self.now(), vec!["db".to_string()]);
        }
    }

    fn tenant(t: &str) -> Tenant {
        if t.is_empty() {
            Tenant::from_header(None, false).expect("the empty tenant")
        } else {
            let h = axum_header(t);
            Tenant::from_header(Some(&h), false).expect("a tenant")
        }
    }

    fn axum_header(t: &str) -> http::HeaderValue {
        http::HeaderValue::from_str(t).expect("a header value")
    }

    fn params(start: i64, end: i64, step: i64) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: start,
            end_ms: end,
            step_ms: step,
        }
    }

    fn key_for(q: &str, p: &MetricQueryParams) -> CacheKey {
        CacheKey::new(tenant(""), q, p, 300_000, None)
    }

    fn allowed() -> PlanFacts {
        PlanFacts {
            shape_allowed: true,
            oldest_input_ms: 0,
            reach_ms: 0,
        }
    }

    fn request<'a>(q: &'a str, p: MetricQueryParams, max_samples: u64) -> RangeRequest<'a> {
        RangeRequest {
            key: key_for(q, &p),
            query: q,
            params: p,
            facts: allowed(),
            max_samples,
            retention_floor_ms: Some(i64::MIN),
            sweep_time_ms: None,
        }
    }

    /// A one-series matrix over the grid of `p`, value `base + t / 1000`.
    fn grid_matrix(p: &MetricQueryParams, base: f64) -> QueryResult {
        let mut points = Vec::new();
        let mut t = p.start_ms;
        while t <= p.end_ms {
            points.push((t, base + (t / 1000) as f64));
            t += p.step_ms;
        }
        QueryResult::Matrix(vec![MatrixSeries {
            labels: vec![("job".to_string(), "x".to_string())],
            points,
        }])
    }

    /// Counts and answers the full and tail evaluations.
    #[derive(Default)]
    struct Calls {
        full: AtomicUsize,
        tail: AtomicUsize,
    }

    type R = Result<Evaluated, ()>;

    #[allow(clippy::too_many_arguments)]
    async fn run_with(
        r: &Rig,
        calls: &Calls,
        req: RangeRequest<'_>,
        full_ann: Annotations,
        full_charge: u64,
        tail_ann: Annotations,
        tail_charge: u64,
        during_tail: impl FnOnce(),
    ) -> Answer {
        let full = |q: MetricQueryParams| {
            calls.full.fetch_add(1, Ordering::SeqCst);
            async move { R::Ok((grid_matrix(&q, 0.0), full_ann, full_charge)) }
        };
        let tail = |q: MetricQueryParams| {
            calls.tail.fetch_add(1, Ordering::SeqCst);
            during_tail();
            async move { R::Ok((grid_matrix(&q, 0.0), tail_ann, tail_charge)) }
        };
        answer_range(&r.cache, req, full, tail)
            .await
            .expect("answered")
    }

    async fn run(r: &Rig, calls: &Calls, req: RangeRequest<'_>) -> Answer {
        run_with(
            r,
            calls,
            req,
            Annotations::new(),
            10,
            Annotations::new(),
            10,
            || {},
        )
        .await
    }

    /// A two-hour range at 15 s, ending `ago` before the rig's clock.
    fn window(r: &Rig, ago: i64) -> MetricQueryParams {
        let end = (r.now() - ago).div_euclid(15_000) * 15_000;
        params(end - 7_200_000, end, 15_000)
    }

    fn shifted(p: &MetricQueryParams, by: i64) -> MetricQueryParams {
        params(p.start_ms + by, p.end_ms + by, p.step_ms)
    }

    fn published(o: StoreOutcome) -> bool {
        matches!(o, StoreOutcome::Published { .. })
    }

    // --- §5.4: what is kept --------------------------------------------------

    #[test]
    fn u1_raw_keeps_to_the_last_grid_point_four_minutes_old() {
        let p = params(0, V, 15_000);
        let want = (V - 240_000).div_euclid(15_000) * 15_000;
        assert_eq!(keep_through(V, 0, &p, 0, None), Some(want));
    }

    #[test]
    fn u2_a_level_keeps_to_its_watermark_less_reach_and_width() {
        let p = params(0, V, 15_000);
        let w = V - 400_000;
        let want = (w - 300_000 - 60_000).div_euclid(15_000) * 15_000;
        assert_eq!(keep_through(V, 300_000, &p, 60_000, Some(w)), Some(want));
    }

    #[test]
    fn u3_a_level_keeps_nothing_past_the_last_bucket_before_t_last() {
        let p = params(0, V - 500_000, 600_000);
        let t_last = (V - 500_000).div_euclid(600_000) * 600_000;
        let gl = t_last.div_euclid(60_000) * 60_000;
        assert!(V - SETTLE_MS > gl, "V − S and W both exceed gl");
        let want = (gl - 60_000).div_euclid(600_000) * 600_000;
        assert_eq!(
            keep_through(V, 0, &p, 60_000, Some(V - 100_000)),
            Some(want)
        );
    }

    // --- §5.3: the merge -----------------------------------------------------

    fn labels(n: &str) -> Vec<(String, String)> {
        vec![("s".to_string(), n.to_string())]
    }

    #[test]
    fn u4_merge_keeps_continuing_series_and_drops_series_out_of_range() {
        let entry = Entry {
            annotations: Vec::new(),
            series: vec![
                (
                    labels("a"),
                    Points::Float(vec![(0, 1.0), (10, 2.0), (20, 3.0)]),
                ),
                (labels("b"), Points::Float(vec![(0, 9.0)])),
            ],
            c0_ms: 0,
            c1_ms: 20,
            first_fill_ms: 0,
            charge_bound: 0,
            bytes: 0,
        };
        let tail = QueryResult::Matrix(vec![
            MatrixSeries {
                labels: labels("a"),
                points: vec![(30, 4.0)],
            },
            MatrixSeries {
                labels: labels("c"),
                points: vec![(30, 7.0)],
            },
        ]);
        let merged = merge(&entry, tail, 10, 30);
        assert!(
            matches!(merged, Some(QueryResult::Matrix(_))),
            "a float matrix: {merged:?}"
        );
        let Some(QueryResult::Matrix(mut m)) = merged else {
            unreachable!()
        };
        m.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert_eq!(
            m,
            vec![
                MatrixSeries {
                    labels: labels("a"),
                    points: vec![(10, 2.0), (20, 3.0), (30, 4.0)],
                },
                MatrixSeries {
                    labels: labels("c"),
                    points: vec![(30, 7.0)],
                },
            ]
        );
    }

    fn hist() -> Box<FloatHistogram> {
        Box::new(
            pulsus_model::NativeHistogram {
                counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                schema: 0,
                zero_threshold: 0.0,
                zero_count: 0,
                count: 1,
                sum: 1.0,
                positive_spans: vec![pulsus_model::Span {
                    offset: 0,
                    length: 1,
                }],
                negative_spans: vec![],
                positive_buckets: vec![1],
                negative_buckets: vec![],
                custom_values: vec![],
            }
            .to_float(),
        )
    }

    #[test]
    fn u5_a_histogram_point_makes_the_answer_matrix_hist_only_while_in_range() {
        let entry = Entry {
            annotations: Vec::new(),
            series: vec![(labels("a"), Points::Float(vec![(0, 1.0), (10, 2.0)]))],
            c0_ms: 0,
            c1_ms: 10,
            first_fill_ms: 0,
            charge_bound: 0,
            bytes: 0,
        };
        let tail = || {
            QueryResult::MatrixHist(vec![HistMatrixSeries {
                labels: labels("a"),
                points: vec![(20, HistOrFloat::Hist(hist()))],
            }])
        };
        let merged = merge(&entry, tail(), 0, 20);
        assert!(
            matches!(merged, Some(QueryResult::MatrixHist(_))),
            "MatrixHist, got {merged:?}"
        );
        if let Some(QueryResult::MatrixHist(m)) = merged {
            assert_eq!(m.len(), 1);
            assert_eq!(
                m[0].points[..2],
                [(0, HistOrFloat::Float(1.0)), (10, HistOrFloat::Float(2.0))]
            );
            assert!(matches!(m[0].points[2], (20, HistOrFloat::Hist(_))));
        }
        let mixed = Entry {
            series: vec![(
                labels("a"),
                Points::Mixed(vec![
                    (0, HistOrFloat::Hist(hist())),
                    (10, HistOrFloat::Float(2.0)),
                ]),
            )],
            ..entry
        };
        let float_tail = QueryResult::Matrix(vec![MatrixSeries {
            labels: labels("a"),
            points: vec![(20, 3.0)],
        }]);
        assert_eq!(
            merge(&mixed, float_tail, 10, 20),
            Some(QueryResult::Matrix(vec![MatrixSeries {
                labels: labels("a"),
                points: vec![(10, 2.0), (20, 3.0)],
            }]))
        );
    }

    // --- §5.6: limits ----------------------------------------------------------

    fn small_entry() -> Entry {
        Entry {
            annotations: Vec::new(),
            series: vec![(labels("a"), Points::Float(vec![(0, 1.0)]))],
            c0_ms: 0,
            c1_ms: 0,
            first_fill_ms: 0,
            charge_bound: 0,
            bytes: 0,
        }
    }

    #[test]
    fn u8_a_tenant_past_its_share_loses_its_own_oldest_first() {
        let p = params(0, 0, 15_000);
        let k = |t: &str, q: &str| CacheKey::new(tenant(t), q, &p, 300_000, None);
        let mut e = small_entry();
        shrink_entry(&mut e);
        let unit = charge(&k("a", "q1"), &e);
        assert!(unit > 0, "an entry is charged");
        // Budget 1,000 for entries of 300, share 50 %, in the same ratio.
        let r = rig_with(unit * 10 / 3, 50, false);
        let g = r.cache.generation();
        assert!(published(r.cache.store(k("b", "q1"), e.clone(), g)));
        assert!(published(r.cache.store(k("a", "q1"), e.clone(), g)));
        assert!(published(r.cache.store(k("a", "q2"), e.clone(), g)));
        assert_eq!(r.cache.snapshot().evictions_total, 0);
        assert!(published(r.cache.store(k("a", "q3"), e.clone(), g)));
        assert!(
            r.cache.lookup(&k("a", "q1")).is_none(),
            "a's oldest evicted"
        );
        assert!(r.cache.lookup(&k("b", "q1")).is_some(), "b kept");
        assert!(r.cache.lookup(&k("a", "q3")).is_some());
        let (total, _) = r.cache.usage();
        assert!(total <= unit * 10 / 3, "total {total}");
        assert_eq!(r.cache.snapshot().evictions_total, 1);
    }

    #[tokio::test]
    async fn u9_an_entry_first_filled_t_max_ago_is_evaluated_in_full() {
        let r = rig();
        let c = Calls::default();
        let q = "rate(x[5m])";
        let p = window(&r, 1_800_000);
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.header,
            Header::Miss
        );
        r.advance(ENTRY_MAX_AGE_MS - 1);
        r.refresh_identity();
        // Control: a millisecond inside T_max the entry still serves.
        let a = run(&r, &c, request(q, p, u64::MAX)).await;
        assert_eq!(a.outcome, Outcome::Hit, "control");
        r.advance(1);
        r.refresh_identity();
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.outcome,
            Outcome::Miss
        );
    }

    fn warned(text: &str) -> Annotations {
        let mut a = Annotations::new();
        a.plain_warning(text);
        a
    }

    #[tokio::test]
    async fn u10_tail_annotations_other_than_the_entry_s_mean_a_full_evaluation() {
        let r = rig();
        let c = Calls::default();
        let q = "rate(x[5m])";
        let p = window(&r, 0);
        let a = run_with(
            &r,
            &c,
            request(q, p, u64::MAX),
            warned("x"),
            10,
            warned("x"),
            10,
            || {},
        )
        .await;
        assert_eq!(a.header, Header::Miss);
        // Control: the same annotations hit.
        let a = run_with(
            &r,
            &c,
            request(q, p, u64::MAX),
            warned("x"),
            10,
            warned("x"),
            10,
            || {},
        )
        .await;
        assert_eq!(a.outcome, Outcome::Hit, "control");
        let a = run_with(
            &r,
            &c,
            request(q, p, u64::MAX),
            warned("x"),
            10,
            warned("y"),
            10,
            || {},
        )
        .await;
        assert_eq!(a.outcome, Outcome::Miss);
    }

    #[test]
    fn u11_tenant_phase_and_span_each_part_the_key() {
        let k = |t: &str, phase: i64, span: i64| {
            let p = params(1_000_000 + phase, 1_000_000 + phase + span, 15_000);
            CacheKey::new(tenant(t), "up", &p, 300_000, None)
        };
        let keys: std::collections::HashSet<CacheKey> = [
            k("a", 0, 7_200_000),
            k("b", 0, 7_200_000),
            k("a", 5_000, 7_200_000),
            k("a", 0, 10_800_000),
            k("b", 5_000, 10_800_000),
            k("b", 0, 10_800_000),
        ]
        .into_iter()
        .collect();
        assert_eq!(keys.len(), 6);
    }

    #[tokio::test]
    async fn u12_a_clear_between_the_tail_and_the_store_stores_nothing() {
        let r = rig();
        let c = Calls::default();
        let q = "rate(x[5m])";
        let p = window(&r, 0);
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.header,
            Header::Miss
        );
        // Control: a hit.
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.outcome,
            Outcome::Hit,
            "control"
        );
        let a = run_with(
            &r,
            &c,
            request(q, p, u64::MAX),
            Annotations::new(),
            10,
            Annotations::new(),
            10,
            || r.cache.clear(),
        )
        .await;
        assert_eq!(a.outcome, Outcome::Miss);
        assert!(r.cache.lookup(&key_for(q, &p)).is_none(), "nothing stored");
        assert_eq!(r.cache.usage().1, 0);
    }

    /// The charge of an entry, summed here from the key, the entry and
    /// every allocation it keeps (§5.6, addendum 5 §2.2).
    fn independent_charge(key: &CacheKey, e: &Entry) -> usize {
        use std::mem::size_of;
        let mut n = size_of::<CacheKey>() + key.query.len() + key.tenant.as_str().len();
        n += size_of::<Entry>() + ENTRY_OVERHEAD_BYTES as usize;
        n += e.annotations.capacity() * size_of::<String>();
        n += e.annotations.iter().map(|a| a.capacity()).sum::<usize>();
        n += e.series.capacity() * size_of::<(Vec<(String, String)>, Points)>();
        let mut flat_counted = 0;
        for (labels, points) in &e.series {
            n += labels.capacity() * size_of::<(String, String)>();
            n += labels
                .iter()
                .map(|(k, v)| k.capacity() + v.capacity())
                .sum::<usize>();
            match points {
                Points::Flat(all, r) => {
                    if r.start == 0 {
                        flat_counted += 1;
                        n += all.len() * size_of::<(i64, f64)>() + 16;
                    }
                }
                Points::Float(v) => n += v.capacity() * size_of::<(i64, f64)>(),
                Points::Mixed(v) => {
                    n += v.capacity() * size_of::<(i64, HistOrFloat)>();
                    for (_, x) in v {
                        if let HistOrFloat::Hist(h) = x {
                            n += size_of::<FloatHistogram>()
                                + h.positive_buckets.capacity() * 8
                                + h.negative_buckets.capacity() * 8
                                + h.custom_values.capacity() * 8
                                + h.positive_spans.capacity() * size_of::<pulsus_model::Span>()
                                + h.negative_spans.capacity() * size_of::<pulsus_model::Span>();
                        }
                    }
                }
            }
        }
        assert!(flat_counted <= 1, "one flat term per entry");
        n
    }

    #[test]
    fn u13_keep_makes_one_float_allocation_and_charges_it_once() {
        let p = params(0, 1_440 * 15_000, 15_000);
        let key = CacheKey::new(tenant(""), "f", &p, 300_000, None);
        // Float: three series of 1,441 points.
        let floats = QueryResult::Matrix(
            ["a", "b", "c"]
                .iter()
                .map(|n| MatrixSeries {
                    labels: labels(n),
                    points: (0..1_441).map(|i| (i * 15_000, i as f64)).collect(),
                })
                .collect(),
        );
        let e = keep(&floats, p.start_ms, p.end_ms, Vec::new(), 0, 0).expect("a matrix");
        assert_eq!(e.series.len(), 3);
        let flats: Vec<&Arc<[(i64, f64)]>> = e
            .series
            .iter()
            .filter_map(|(_, pts)| match pts {
                Points::Flat(all, r) => {
                    assert_eq!(r.len(), 1_441);
                    Some(all)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            flats.len(),
            3,
            "every float series is Flat: {:?}",
            e.series[0].1
        );
        assert!(
            flats.iter().all(|a| Arc::ptr_eq(a, flats[0])),
            "one allocation"
        );
        assert_eq!(flats[0].len(), 3 * 1_441);
        assert_eq!(charge(&key, &e) as usize, independent_charge(&key, &e));
        // Histogram: 100 points with 50-element bucket vectors, built with
        // spare capacity.
        let hist_points: Vec<(i64, HistOrFloat)> = (0..100)
            .map(|i| {
                let mut h = *hist();
                let mut b = Vec::with_capacity(200);
                b.extend(std::iter::repeat_n(1.0, 50));
                h.positive_buckets = b;
                (i * 15_000, HistOrFloat::Hist(Box::new(h)))
            })
            .collect();
        let mut spare = Vec::with_capacity(400);
        spare.extend(hist_points);
        let hists = QueryResult::MatrixHist(vec![HistMatrixSeries {
            labels: labels("h"),
            points: spare,
        }]);
        let e = keep(&hists, p.start_ms, p.end_ms, Vec::new(), 0, 0).expect("a matrix");
        let Points::Mixed(v) = &e.series[0].1 else {
            panic!("a histogram series stays Mixed: {:?}", e.series[0].1)
        };
        assert_eq!(v.len(), 100);
        assert_eq!(v.capacity(), v.len());
        for (_, x) in v {
            let HistOrFloat::Hist(h) = x else {
                unreachable!()
            };
            assert_eq!(h.positive_buckets.len(), 50);
            assert_eq!(h.positive_buckets.capacity(), h.positive_buckets.len());
            assert_eq!(h.positive_spans.capacity(), h.positive_spans.len());
        }
        assert_eq!(charge(&key, &e) as usize, independent_charge(&key, &e));
    }

    #[tokio::test]
    async fn u14_a_hit_whose_charge_could_pass_the_cap_is_evaluated_in_full() {
        let r = rig();
        let c = Calls::default();
        let q = "rate(x[5m])";
        let p = window(&r, 0);
        let go = |tail_charge: u64| {
            run_with(
                &r,
                &c,
                request(q, p, 1_000),
                Annotations::new(),
                900,
                Annotations::new(),
                tail_charge,
                || {},
            )
        };
        assert_eq!(go(0).await.header, Header::Miss);
        // Control: 900 + 100 is within the cap.
        assert_eq!(go(100).await.outcome, Outcome::Hit, "control");
        assert_eq!(go(200).await.outcome, Outcome::Miss);
    }

    #[test]
    fn u15_fills_need_a_whole_lag_history_under_five_seconds() {
        let r = rig_with(1 << 20, 50, true);
        let now = r.now();
        let v = now;
        // A 12 s gap inside [V − 10 s, now].
        r.cache.record_lag(now - 20_000, 0);
        r.cache.record_lag(now - 15_000, 0);
        r.cache.record_lag(now - 3_000, 0);
        assert!(!r.cache.fills_allowed(v), "a 12 s gap");
        let r = rig_with(1 << 20, 50, true);
        for t in [20_000, 15_000, 10_000, 5_000, 0] {
            r.cache.record_lag(now - t, if t == 5_000 { 5 } else { 0 });
        }
        assert!(!r.cache.fills_allowed(v), "one read of 5");
        let r = rig_with(1 << 20, 50, true);
        for t in [20_000, 15_000, 10_000, 5_000, 0] {
            r.cache.record_lag(now - t, 4);
        }
        assert!(r.cache.fills_allowed(v), "reads 5 s apart, all under 5");
    }

    #[test]
    fn u16_a_new_database_identity_clears_the_cache() {
        let r = rig();
        let p = params(0, 0, 15_000);
        let g = r.cache.generation();
        assert!(published(r.cache.store(
            key_for("up", &p),
            small_entry(),
            g
        )));
        r.cache.record_identity(r.now(), vec!["other".to_string()]);
        assert_eq!(r.cache.generation(), g + 1);
        assert_eq!(r.cache.usage().1, 0);
        assert_eq!(r.cache.snapshot().clears_total, 1);
    }

    #[tokio::test]
    async fn u17_a_hit_without_the_lag_gate_serves_but_keeps_no_more() {
        let r = rig_with(64 * 1024 * 1024, 50, true);
        r.refresh_identity();
        let lag = |r: &Rig, v: u64| {
            for t in [20_000, 15_000, 10_000, 5_000, 0] {
                r.cache.record_lag(r.now() - t, v);
            }
        };
        lag(&r, 0);
        let c = Calls::default();
        let q = "rate(x[5m])";
        let p = window(&r, 0);
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.header,
            Header::Miss
        );
        let before = r.cache.lookup(&key_for(q, &p)).expect("kept");
        r.advance(60_000);
        r.refresh_identity();
        lag(&r, 9);
        let p2 = shifted(&p, 60_000);
        let a = run(&r, &c, request(q, p2, u64::MAX)).await;
        assert_eq!(a.outcome, Outcome::Hit);
        assert_eq!(a.through_ms, Some(before.c1_ms));
        let after = r.cache.lookup(&key_for(q, &p2)).expect("still kept");
        assert_eq!(*after, *before, "the entry is unchanged");
    }

    #[tokio::test]
    async fn u18_identity_unread_for_ten_seconds_bypasses_until_read_again() {
        let r = rig();
        let c = Calls::default();
        let q = "rate(x[5m])";
        let p = window(&r, 1_800_000);
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.header,
            Header::Miss
        );
        r.advance(11_000);
        let a = run(&r, &c, request(q, p, u64::MAX)).await;
        assert_eq!(a.header, Header::Bypass, "no hit and nothing stored");
        assert_eq!(a.outcome, Outcome::Miss);
        r.refresh_identity();
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.outcome,
            Outcome::Hit
        );
    }

    #[tokio::test]
    async fn u19_a_replacement_keeps_the_first_fill_time() {
        let r = rig();
        let c = Calls::default();
        let q = "rate(x[5m])";
        let mut p = window(&r, 0);
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.header,
            Header::Miss
        );
        let first = r.cache.lookup(&key_for(q, &p)).expect("kept").first_fill_ms;
        let mut hits = 0;
        loop {
            r.advance(60_000);
            r.refresh_identity();
            p = shifted(&p, 60_000);
            let a = run(&r, &c, request(q, p, u64::MAX)).await;
            if r.now() - first >= ENTRY_MAX_AGE_MS {
                assert_eq!(a.outcome, Outcome::Miss, "at first_fill + T_max");
                break;
            }
            assert_eq!(a.outcome, Outcome::Hit);
            hits += 1;
            let e = r.cache.lookup(&key_for(q, &p)).expect("replaced");
            assert_eq!(e.first_fill_ms, first);
        }
        assert_eq!(hits, 59);
    }

    fn planned(q: &str, step: i64) -> QueryPlan {
        let expr = pulsus_promql::parse(q).unwrap_or_else(|e| panic!("{q}: {e}"));
        let params = pulsus_promql::PlanParams {
            start_ms: 3_600_000,
            end_ms: if step == 0 { 3_600_000 } else { 10_800_000 },
            step_ms: step,
            lookback_ms: 300_000,
            experimental_functions: true,
        };
        pulsus_promql::plan(&expr, params).unwrap_or_else(|e| panic!("{q}: {e}"))
    }

    #[test]
    fn u20_the_gate_admits_the_listed_shapes_only() {
        // One query per §5.9 row; the instant-only rows are planned instant.
        let allowed = [
            ("Selector, offset", "x offset 5m", 60_000),
            ("Selector, negative offset", "x offset -5m", 60_000),
            ("RangeFn", "rate(x[5m])", 60_000),
            ("OverTime", "max_over_time(x[5m])", 60_000),
            ("OverTimeParam", "quantile_over_time(0.9, x[5m])", 60_000),
            ("Aggregate", "sum by (i) (rate(x[5m]))", 60_000),
            ("Aggregate topk", "topk(3, x)", 60_000),
            ("Binary", "x * on(i) group_left(j) y", 60_000),
            ("Binary bool", "x > bool 1", 60_000),
            ("Unary minus", "-x", 60_000),
            ("SetOp and", "x and y", 60_000),
            ("SetOp unless", "x unless y", 60_000),
            (
                "HistogramQuantile",
                "histogram_quantile(0.9, rate(h[5m]))",
                60_000,
            ),
            ("Scalar", "1 + 2", 60_000),
        ];
        let refused = [
            ("duration expression", "rate(x[range() / 24])", 60_000),
            ("@", "rate(x[5m] @ end())", 60_000),
            ("subquery", "rate(x[5m:1m])", 60_000),
            ("SetOp or", "(x > 50) or (x + 1)", 60_000),
            ("anchored", "rate(x[5m] anchored)", 60_000),
            ("smoothed", "rate(x[5m] smoothed)", 60_000),
            ("Binary fill", "x + fill(0) y", 60_000),
            ("Aggregate group", "group(x)", 60_000),
            ("Aggregate stddev", "stddev(x)", 60_000),
            ("Aggregate stdvar", "stdvar(x)", 60_000),
            ("Aggregate quantile", "quantile(0.9, x)", 60_000),
            ("Aggregate limitk", "limitk(2, x)", 60_000),
            ("Aggregate limit_ratio", "limit_ratio(0.5, x)", 60_000),
            ("CountValues", "count_values(\"v\", x)", 60_000),
            ("Absent", "absent(x)", 60_000),
            ("AbsentOverTime", "absent_over_time(x[5m])", 60_000),
            (
                "LabelReplace",
                "label_replace(x, \"a\", \"$1\", \"i\", \"(.*)\")",
                60_000,
            ),
            ("LabelJoin", "label_join(x, \"a\", \",\", \"i\")", 60_000),
            (
                "HistogramQuantiles",
                "histogram_quantiles(h, \"q\", 0.5, 0.9)",
                60_000,
            ),
            ("HistogramAccessor", "histogram_count(h)", 60_000),
            ("HistogramFraction", "histogram_fraction(0, 1, h)", 60_000),
            ("MathFn", "abs(x)", 60_000),
            ("ScalarFn", "clamp_min(x, 0)", 60_000),
            ("Time", "time()", 60_000),
            ("DateFn", "hour()", 60_000),
            ("Timestamp", "timestamp(x)", 60_000),
            ("ScalarOf", "scalar(x)", 60_000),
            ("VectorOf", "vector(1)", 60_000),
            ("Info", "info(x)", 60_000),
            ("Sort", "sort(x)", 0),
            ("SortByLabel", "sort_by_label(x, \"i\")", 0),
            ("RangeVector", "x[5m]", 0),
            ("StringLiteral", "\"s\"", 0),
        ];
        for (row, q, step) in allowed {
            assert!(shape_allowed(&planned(q, step)), "allowed row {row}: {q}");
        }
        for (row, q, step) in refused {
            assert!(!shape_allowed(&planned(q, step)), "refused row {row}: {q}");
        }
    }

    #[tokio::test]
    async fn u21_identity_lapsing_during_the_tail_means_a_full_evaluation() {
        let r = rig();
        let c = Calls::default();
        let q = "rate(x[5m])";
        let p = window(&r, 0);
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.header,
            Header::Miss
        );
        let c = Calls::default();
        let a = run_with(
            &r,
            &c,
            request(q, p, u64::MAX),
            Annotations::new(),
            10,
            Annotations::new(),
            10,
            || r.advance(11_000),
        )
        .await;
        assert_eq!(c.tail.load(Ordering::SeqCst), 1, "the tail ran");
        assert_eq!(c.full.load(Ordering::SeqCst), 1, "then a full evaluation");
        assert_eq!(a.outcome, Outcome::Miss);
        assert_eq!(a.result, grid_matrix(&p, 0.0), "the full answer");
    }

    fn forced() -> Annotations {
        let mut a = Annotations::new();
        a.forced_monotonicity_info_at(
            0,
            "PromQL info: input to histogram_quantile needed to be fixed for monotonicity",
            pulsus_promql::ForcedMonotonicityDetail::single(1_000, 0.5, 1.0, 1.0),
        );
        a
    }

    #[tokio::test]
    async fn u22_a_mergeable_annotation_is_never_kept_nor_answered_from_kept_points() {
        let q = "histogram_quantile(0.9, rate(h_bucket[5m]))";
        // (a) control: rendered annotations equal and no detail: a hit.
        let r = rig();
        let p = window(&r, 0);
        let c = Calls::default();
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.header,
            Header::Miss
        );
        let c = Calls::default();
        let a = run(&r, &c, request(q, p, u64::MAX)).await;
        assert_eq!(c.tail.load(Ordering::SeqCst), 1, "(a) tail");
        assert_eq!(c.full.load(Ordering::SeqCst), 0, "(a) full");
        assert_eq!(a.outcome, Outcome::Hit, "(a)");
        // (b) the tail carries a forced-monotonicity detail whose rendering
        // equals the entry's.
        let entry_text = rendered_annotations(&forced(), q);
        let key = key_for(q, &p);
        let mut e = (*r.cache.lookup(&key).expect("kept")).clone();
        e.annotations = entry_text;
        assert!(published(r.cache.store(
            key.clone(),
            e,
            r.cache.generation()
        )));
        let before = r.cache.lookup(&key).expect("kept");
        let c = Calls::default();
        let a = run_with(
            &r,
            &c,
            request(q, p, u64::MAX),
            forced(),
            10,
            forced(),
            10,
            || {},
        )
        .await;
        assert_eq!(c.tail.load(Ordering::SeqCst), 1, "(b) tail");
        assert_eq!(c.full.load(Ordering::SeqCst), 1, "(b) full");
        assert_eq!(a.outcome, Outcome::Miss, "(b)");
        assert_eq!(
            *r.cache.lookup(&key).expect("kept"),
            *before,
            "(b) unchanged"
        );
        // (c) a miss whose answer carries one is not stored.
        let r = rig();
        let c = Calls::default();
        let a = run_with(
            &r,
            &c,
            request(q, p, u64::MAX),
            forced(),
            10,
            forced(),
            10,
            || {},
        )
        .await;
        assert_eq!(a.header, Header::Bypass, "(c) not stored");
        let c = Calls::default();
        run_with(
            &r,
            &c,
            request(q, p, u64::MAX),
            forced(),
            10,
            forced(),
            10,
            || {},
        )
        .await;
        assert_eq!(c.tail.load(Ordering::SeqCst), 0, "(c) no tail");
        assert_eq!(c.full.load(Ordering::SeqCst), 1, "(c) full");
    }

    /// A request with the cache off never reaches a cache, so no bypass
    /// reason names that case.
    #[test]
    fn the_bypass_reasons_are_the_ones_a_cache_records() {
        let labels: Vec<&str> = BypassReason::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(
            labels,
            ["instant", "explain", "shape", "retention", "no_fill"]
        );
    }

    // --- addendum 5 §4 ---------------------------------------------------------

    #[test]
    fn u23_keep_then_merge_returns_every_kept_float_bit_for_bit() {
        let specials = [f64::NAN, f64::INFINITY, -0.0, f64::NEG_INFINITY, 0.0, 1.5];
        let series: Vec<MatrixSeries> = ["a", "b"]
            .iter()
            .enumerate()
            .map(|(j, n)| MatrixSeries {
                labels: labels(n),
                points: (0..40)
                    .map(|i| (i * 10, specials[(i as usize + j) % specials.len()]))
                    .collect(),
            })
            .collect();
        let (start, c1) = (50, 300);
        let e = keep(
            &QueryResult::Matrix(series.clone()),
            start,
            c1,
            Vec::new(),
            0,
            0,
        )
        .expect("a matrix");
        assert!(
            e.series.iter().all(|(_, p)| matches!(p, Points::Flat(..))),
            "keep made the float series Flat: {:?}",
            e.series[0].1
        );
        let merged = merge(&e, QueryResult::Matrix(Vec::new()), start, 390);
        let Some(QueryResult::Matrix(m)) = merged else {
            panic!("a float matrix: {merged:?}")
        };
        assert_eq!(m.len(), 2);
        for (got, want) in m.iter().zip(&series) {
            assert_eq!(got.labels, want.labels);
            let want: Vec<(i64, u64)> = want
                .points
                .iter()
                .filter(|(t, _)| *t >= start && *t <= c1)
                .map(|(t, x)| (*t, x.to_bits()))
                .collect();
            let got: Vec<(i64, u64)> = got.points.iter().map(|(t, x)| (*t, x.to_bits())).collect();
            assert_eq!(got, want);
        }
    }

    #[test]
    fn u25_merge_builds_each_series_once_at_its_exact_length() {
        let step = 15_000;
        let c1 = 1_440 * step;
        let entry_series: Vec<MatrixSeries> = (0..10)
            .map(|j| MatrixSeries {
                labels: labels(&format!("s{j}")),
                points: (0..1_441).map(|i| (i * step, (i + j) as f64)).collect(),
            })
            .collect();
        let e = keep(
            &QueryResult::Matrix(entry_series.clone()),
            0,
            c1,
            Vec::new(),
            0,
            0,
        )
        .expect("a matrix");
        let tail: Vec<MatrixSeries> = (0..10)
            .map(|j| MatrixSeries {
                labels: labels(&format!("s{j}")),
                points: (1_441..1_461).map(|i| (i * step, (i * j) as f64)).collect(),
            })
            .collect();
        let end = 1_460 * step;
        let merged = merge(&e, QueryResult::Matrix(tail.clone()), 0, end);
        let Some(QueryResult::Matrix(m)) = merged else {
            panic!("a float matrix: {merged:?}")
        };
        assert_eq!(m.len(), 10);
        for (j, s) in m.iter().enumerate() {
            assert_eq!(s.points.len(), 1_461);
            assert_eq!(s.points.capacity(), s.points.len(), "series {j}");
            let mut want = entry_series[j].points.clone();
            want.extend(tail[j].points.iter().copied());
            assert_eq!(s.labels, entry_series[j].labels);
            assert_eq!(s.points, want, "series {j}");
        }
    }

    /// An entry of one series whose last kept point is `c1`.
    fn entry_through(c1: i64, n: usize) -> Entry {
        Entry {
            series: vec![(
                labels("a"),
                Points::Float((0..n as i64).map(|i| (i, 1.0)).collect()),
            )],
            c1_ms: c1,
            ..small_entry()
        }
    }

    #[test]
    fn u34_store_decides_publication_and_through_in_one_step() {
        let r = rig_with(200_000, 50, false);
        let p = params(0, 0, 15_000);
        let k = key_for("up", &p);
        let g = r.cache.generation();
        // Control.
        assert_eq!(
            r.cache.store(k.clone(), entry_through(100, 1), g),
            StoreOutcome::Published { through: Some(100) },
            "control"
        );
        assert_eq!(
            r.cache.store(k.clone(), entry_through(200, 1), g),
            StoreOutcome::Published { through: Some(200) }
        );
        assert_eq!(r.cache.lookup(&k).expect("stored").c1_ms, 200);
        r.cache.clear();
        assert_eq!(
            r.cache.store(k.clone(), entry_through(250, 1), g),
            StoreOutcome::Stale
        );
        assert!(r.cache.lookup(&k).is_none(), "nothing stored");
        let g = r.cache.generation();
        assert_eq!(
            r.cache.store(k.clone(), entry_through(300, 1), g),
            StoreOutcome::Published { through: Some(300) }
        );
        // 10,000 points: 160,000 bytes, above the 100,000-byte share.
        let before = r.cache.snapshot().evictions_total;
        assert_eq!(
            r.cache.store(k.clone(), entry_through(400, 10_000), g),
            StoreOutcome::NotPublished { through: Some(300) }
        );
        assert_eq!(r.cache.snapshot().evictions_total, before + 1);
        assert_eq!(r.cache.lookup(&k).expect("the 300 entry stays").c1_ms, 300);
    }

    /// U37: a fill made, then a request whose hook runs after the merge.
    async fn u37_case(
        clustered: bool,
        fills: bool,
        clear: bool,
    ) -> (Answer, MetricQueryParams, usize) {
        let r = rig_with(64 * 1024 * 1024, 50, clustered);
        r.refresh_identity();
        let lag = |r: &Rig, v: u64| {
            for t in [20_000, 15_000, 10_000, 5_000, 0] {
                r.cache.record_lag(r.now() - t, v);
            }
        };
        if clustered {
            lag(&r, 0);
        }
        let q = "rate(x[5m])";
        let p = window(&r, 0);
        let c = Calls::default();
        assert_eq!(
            run(&r, &c, request(q, p, u64::MAX)).await.header,
            Header::Miss
        );
        let Rig { cache, clock } = r;
        let cache = if clear {
            cache.with_after_merge(|c| c.clear())
        } else {
            cache.with_after_merge(|_| {})
        };
        let r = Rig { cache, clock };
        if clustered && !fills {
            lag(&r, 9);
        }
        let full_calls = AtomicUsize::new(0);
        let full = |q: MetricQueryParams| {
            full_calls.fetch_add(1, Ordering::SeqCst);
            async move { R::Ok((grid_matrix(&q, 1_000.0), Annotations::new(), 10)) }
        };
        let tail = |q: MetricQueryParams| async move {
            R::Ok((grid_matrix(&q, 0.0), Annotations::new(), 10))
        };
        let a = answer_range(&r.cache, request(q, p, u64::MAX), full, tail)
            .await
            .expect("answered");
        (a, p, full_calls.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn u37_a_clear_after_the_merge_means_a_full_evaluation() {
        // Control: no clear, a hit, the full closure not called.
        let (a, _, full) = u37_case(false, true, false).await;
        assert_eq!(a.header, Header::Hit, "control");
        assert_eq!(full, 0, "control");
        let (a, _, full) = u37_case(true, false, false).await;
        assert_eq!(a.header, Header::Hit, "control, no fill");
        assert_eq!(full, 0, "control, no fill");
        // (a) fills allowed: the store finds the generation moved.
        let (a, p, full) = u37_case(false, true, true).await;
        assert_eq!(full, 1, "(a) full");
        assert_eq!(a.result, grid_matrix(&p, 1_000.0), "(a) the full answer");
        assert_ne!(a.header, Header::Hit, "(a)");
        // (b) fills not allowed: the final generation read finds it moved.
        let (a, p, full) = u37_case(true, false, true).await;
        assert_eq!(full, 1, "(b) full");
        assert_eq!(a.result, grid_matrix(&p, 1_000.0), "(b) the full answer");
        assert_ne!(a.header, Header::Hit, "(b)");
    }
}
