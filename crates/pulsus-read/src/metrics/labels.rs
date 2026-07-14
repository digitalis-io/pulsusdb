//! [`LabelCache`]: the resident, time-aware label cache and its
//! synchronous, pure [`SeriesResolver::resolve`] — docs/architecture.md
//! §5.2. [`LabelCache::refresh`] (async, ClickHouse-touching) lives in
//! [`super::refresh`]; this module owns the snapshot shape, the resolver
//! contract, and in-process matcher evaluation (including the bounded
//! compiled-regex cache).
//!
//! **Purity is deliberate** (architect plan): `resolve` never awaits and
//! never talks to ClickHouse — it reads the current [`CacheSnapshot`]
//! (cloned out from behind a brief read lock, matching `AppState.pool`'s
//! "async-filled, read constantly" discipline) and every degradation (cold,
//! stale, out-of-window, over-cardinality, unsupported regex) is a
//! data-driven branch returning [`Resolution::SqlFallback`] — never a wrong
//! result, never a panic. [`resolve_over`] is the pure algorithmic core,
//! factored out of [`LabelCache`] itself so it is unit-testable against a
//! hand-built [`CacheSnapshot`] with no `ChClient`/network dependency at
//! all — only [`LabelCache::new`]/[`LabelCache::refresh`] need a real
//! connection.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use pulsus_clickhouse::ChClient;
use pulsus_model::{Fingerprint, LabelSet};
use regex::Regex;

use super::matcher::{DataWindow, LabelMatcher, MatchOp};
use super::stats::{CacheMetrics, CacheMetricsSnapshot};

/// The documented staleness constant (task-manager resolution #2 on issue
/// #30): a warm cache older than `staleness_multiplier * ttl` degrades to
/// the SQL path, following the writer's "documented constant until a
/// deployment needs to tune it" precedent (`WriterRuntime`'s own constants).
/// Promote to a dedicated `PULSUS_CACHE_STALENESS` env var only if a
/// deployment needs to tune it independently of `PULSUS_CACHE_TTL`.
pub const DEFAULT_STALENESS_MULTIPLIER: u32 = 3;

/// The bounded compiled-regex cache's capacity (architect plan amendment
/// §1: "bounded compiled-regex cache" — a documented constant, same
/// precedent as every other hand-rolled cap in this workspace, e.g. #9's
/// spool dir/LRU cap). Once full, a *new* pattern is never admitted (no
/// eviction policy) — [`resolve_over`] maps that to
/// [`FallbackReason::RegexUnsupported`], never a panic or a wrong result.
const REGEX_CACHE_CAPACITY: usize = 4_096;

/// Owned, `EngineConfig`-style construction parameters for a [`LabelCache`]
/// (mirrors `logql::EngineConfig`'s "owned `String`s/values, no borrowed
/// lifetime on the engine itself" shape).
#[derive(Debug, Clone)]
pub struct LabelCacheConfig {
    /// `CLICKHOUSE_DB` — carried through for parity with `EngineConfig`;
    /// the sweep SQL itself does not table-prefix (the connection's default
    /// database resolves the unqualified `series_table` name).
    pub db: String,
    /// `metric_series` (or its `_dist`-suffixed wrapper — cluster-aware
    /// resolution lives in the server's config wiring, not here).
    pub series_table: String,
    /// `PULSUS_SERIES_ACTIVITY_BUCKET`, milliseconds.
    pub bucket_ms: i64,
    /// `PULSUS_CACHE_WINDOW`, milliseconds — bounds cache *residency*
    /// (reading 1, task-manager resolution #1 on issue #30).
    pub window_ms: i64,
    /// `PULSUS_CACHE_MAX_SERIES` — the **per-selector** guard (reading 1):
    /// an in-process match exceeding this falls back to the SQL/JOIN
    /// sub-query, never a resident-cache size cap.
    pub cache_max_series: u64,
    /// `PULSUS_CACHE_TTL` — the refresh sweep interval.
    pub ttl: Duration,
    /// See [`DEFAULT_STALENESS_MULTIPLIER`].
    pub staleness_multiplier: u32,
}

/// Resident snapshot: `fingerprint -> LabelSet` plus `metric_name ->
/// sorted [fingerprint]`. Immutable once built — [`super::refresh`] builds
/// a whole new one and atomically swaps it in (never mutated in place), so
/// readers always observe either the whole old or whole new snapshot, never
/// a partial map.
#[derive(Debug, Default)]
pub struct CacheSnapshot {
    pub(crate) by_fingerprint: HashMap<Fingerprint, LabelSet>,
    /// Values are sorted, deduped fingerprint lists — a consequence of the
    /// sweep's `LIMIT 1 BY metric_name, fingerprint` dedup, re-sorted after
    /// the sweep completes (see [`super::refresh`]).
    pub(crate) by_metric: HashMap<String, Vec<Fingerprint>>,
    /// The sweep's own `now_ms` (wall-clock milliseconds since the Unix
    /// epoch, [`super::refresh::now_unix_ms`]) — meaningless (`0`) only for
    /// the never-swept, `generation == 0` default snapshot. This is the
    /// cache's **recency anchor** (code-review round-2 fix, replacing a
    /// wall-clock-`Instant`-since-swap staleness check): a periodic-refresh
    /// cache structurally cannot know about a series whose first activity
    /// is after this timestamp, so [`resolve_over`]'s upper-bound gate and
    /// [`LabelCache::age_ms`]'s `/metrics` gauge both measure against it,
    /// not against when the snapshot happened to be swapped in.
    pub(crate) sweep_time_ms: i64,
    /// The floored lower bound the sweep queried from
    /// (`floor_to_activity_bucket(now - window_ms, bucket_ms)`) — the
    /// cache's covered-from edge for the out-of-window check.
    pub(crate) covered_from_ms: i64,
    /// `0` = cold (no successful sweep yet); every successful sweep
    /// increments this by one.
    pub(crate) generation: u64,
}

impl CacheSnapshot {
    /// Resident fingerprint count (used by tests/metrics; the authoritative
    /// gauge value lives in [`CacheMetrics`], updated at the same time this
    /// snapshot is swapped in).
    pub fn series_count(&self) -> usize {
        self.by_fingerprint.len()
    }
}

/// Why a query degraded to the SQL/JOIN fallback — explain-friendly (#31's
/// `X-Pulsus-Explain`). Pattern value/regex text is deliberately never
/// carried here (architect plan amendment §1: "kept OUT of the reason" to
/// keep explain output injection-free) — only structural facts (a label
/// key, a match count, an age).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FallbackReason {
    /// No successful sweep yet.
    ColdCache,
    /// `window.end_ms > snapshot.sweep_time_ms + staleness_threshold_ms`
    /// (code-review round-2 fix: this **replaces** a wall-clock-only
    /// staleness check) — the query's upper edge reaches far enough past
    /// the cache's last sweep that a series whose first activity falls in
    /// that gap could be invisible to the cache. `age_ms =
    /// window.end_ms.saturating_sub(snapshot.sweep_time_ms)`, i.e. how far
    /// past the sweep the query's own end reaches — not wall-clock age.
    StaleCache { age_ms: u64 },
    /// `window.start_ms < snapshot.covered_from_ms` — the query reaches
    /// further back than the cache's covered window (docs/architecture.md
    /// §5.2's correctness rule, never an optimization).
    OutOfWindow,
    /// The in-process match exceeded `cache_max_series` (a per-selector
    /// guard, not a resident-cache cap — reading 1).
    OverCardinality { matched: usize, cap: u64 },
    /// A `Re`/`Nre` matcher's pattern was uncompilable, or the bounded
    /// compiled-regex cache had no room to admit a new pattern. `key` is
    /// the offending matcher's label key; the pattern/value is deliberately
    /// omitted (architect plan amendment §1).
    RegexUnsupported { key: String },
}

/// Either fully answered in-process, or an injection-safe `metric_series`
/// sub-query for the caller (issue #31) to inline as
/// `fingerprint IN ( <sql> )`.
#[derive(Debug, Clone, PartialEq)]
pub enum Resolution {
    /// Sorted, deduped fingerprints, fully answered from the current
    /// snapshot (§2.3 fast path).
    Fingerprints(Vec<Fingerprint>),
    /// `sql` is the un-doubled, injection-safe `metric_series` sub-query
    /// (see [`super::sql`]'s placeholder-doubling contract) with
    /// bucket-floored bounds; `reason` names why the cache degraded.
    SqlFallback { sql: String, reason: FallbackReason },
}

/// [`LabelCache::resolve_labelled`]'s result — like [`Resolution`], but the
/// cache fast path carries each matched fingerprint's resolved
/// [`LabelSet`], not just the bare fingerprint. Needed by issue #31's
/// zero-ClickHouse `count`/`group` fast path (task-manager resolution #2
/// on issue #31): that AC requires *labels* (to compute a
/// `by`/`without` grouping key), not just fingerprints. On the SQL/
/// out-of-window path this returns the identical [`FallbackReason`] and
/// sub-query [`Resolution::resolve`] would, so `count`/`group` historical
/// variants route through `metric_series` exactly like any other query.
#[derive(Debug, Clone, PartialEq)]
pub enum LabelledResolution {
    /// Sorted by fingerprint, fully answered from the current snapshot.
    Series(Vec<(Fingerprint, LabelSet)>),
    /// Identical contract to [`Resolution::SqlFallback`].
    SqlFallback { sql: String, reason: FallbackReason },
}

/// Pure over the current snapshot. Implemented by [`LabelCache`]; a
/// separate trait (rather than an inherent method) so issue #31 can depend
/// on the contract without depending on the concrete refresh/ClickHouse
/// machinery.
pub trait SeriesResolver {
    /// `matchers` excludes `__name__` (metric-scoped by `metric_name`,
    /// docs/schemas.md §2.1's structural model). Fingerprints are sorted
    /// and deduped.
    fn resolve(
        &self,
        metric_name: &str,
        matchers: &[LabelMatcher],
        window: DataWindow,
    ) -> Resolution;
}

/// A bounded compiled-regex cache: `pattern -> compiled ^(?:pattern)$`.
/// Once at capacity, a pattern not already present is never admitted (no
/// eviction) — [`RegexCache::is_match`] returns `None`, which
/// [`resolve_over`] maps to [`FallbackReason::RegexUnsupported`]. Regexes
/// are always rendered fully anchored (`^(?:...)$`), mirroring ClickHouse
/// RE2's `match()` semantics on the SQL fallback path
/// (`escape::ch_regex_anchored`) — load-bearing for the cache-vs-SQL
/// differential test. Interior mutability via a `std::sync::Mutex` is sound
/// here despite `resolve` taking `&self`: the critical section is entirely
/// synchronous (a hashmap lookup/insert plus a regex compile), never held
/// across an `.await`.
#[derive(Debug)]
pub(crate) struct RegexCache {
    capacity: usize,
    compiled: Mutex<HashMap<String, Arc<Regex>>>,
}

impl RegexCache {
    pub(crate) fn new(capacity: usize) -> Self {
        RegexCache {
            capacity,
            compiled: Mutex::new(HashMap::new()),
        }
    }

    /// `None` means "cannot evaluate this pattern in-process" — either it
    /// failed to compile, or the cache has no room for it. Never panics: a
    /// poisoned lock (only reachable if a prior holder panicked while
    /// holding it, which nothing in this short, infallible critical section
    /// does) still degrades gracefully rather than propagating a panic.
    fn is_match(&self, pattern: &str, value: &str) -> Option<bool> {
        let mut guard = match self.compiled.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(re) = guard.get(pattern) {
            return Some(re.is_match(value));
        }
        if guard.len() >= self.capacity {
            return None;
        }
        let anchored = format!("^(?:{pattern})$");
        let re = Arc::new(Regex::new(&anchored).ok()?);
        guard.insert(pattern.to_string(), Arc::clone(&re));
        Some(re.is_match(value))
    }
}

/// Evaluates every matcher against `labels`, using Prometheus absent-label
/// semantics: a missing label reads as `""` — identical to what
/// `JSONExtractString(labels, k)` returns for an absent key on the SQL path
/// (architect plan: load-bearing for the differential AC). Returns `Err`
/// with the first matcher whose regex could not be evaluated in-process
/// (short-circuits the whole selector, per the architect plan amendment: a
/// `RegexUnsupported` selector never completes in-process matching).
fn matches(
    regex_cache: &RegexCache,
    labels: &LabelSet,
    matchers: &[LabelMatcher],
) -> Result<bool, FallbackReason> {
    for m in matchers {
        let value = labels.get(&m.key).unwrap_or("");
        let ok = match m.op {
            MatchOp::Eq => value == m.value,
            MatchOp::Neq => value != m.value,
            MatchOp::Re => regex_cache
                .is_match(&m.value, value)
                .ok_or_else(|| FallbackReason::RegexUnsupported { key: m.key.clone() })?,
            MatchOp::Nre => !regex_cache
                .is_match(&m.value, value)
                .ok_or_else(|| FallbackReason::RegexUnsupported { key: m.key.clone() })?,
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Renders the fallback sub-query and pairs it with `reason` — shared by
/// both [`resolve_over`] (wraps into [`Resolution::SqlFallback`]) and
/// [`resolve_labelled_over`] (wraps into
/// [`LabelledResolution::SqlFallback`]), so the two resolution paths can
/// never disagree on *which* sub-query a given fallback reason renders.
fn sql_fallback_sql(
    config: &LabelCacheConfig,
    metric_name: &str,
    window: DataWindow,
    matchers: &[LabelMatcher],
) -> String {
    super::sql::historical_series_subquery(
        &config.series_table,
        metric_name,
        window,
        config.bucket_ms,
        matchers,
    )
}

fn sql_fallback(
    config: &LabelCacheConfig,
    metric_name: &str,
    window: DataWindow,
    matchers: &[LabelMatcher],
    reason: FallbackReason,
) -> Resolution {
    Resolution::SqlFallback {
        sql: sql_fallback_sql(config, metric_name, window, matchers),
        reason,
    }
}

fn labelled_sql_fallback(
    config: &LabelCacheConfig,
    metric_name: &str,
    window: DataWindow,
    matchers: &[LabelMatcher],
    reason: FallbackReason,
) -> LabelledResolution {
    LabelledResolution::SqlFallback {
        sql: sql_fallback_sql(config, metric_name, window, matchers),
        reason,
    }
}

/// The pure resolution algorithm: `resolve`'s whole contract, factored out
/// of [`LabelCache`] so it is testable against a hand-built
/// [`CacheSnapshot`] with no `ChClient` at all. `metrics` is updated with
/// exactly one hit/miss counter per call (architect plan: every
/// degradation is a data-driven branch, recorded for `/metrics`).
///
/// **Recency gate (code-review round-2 fix, adjudicated):** the cache is
/// authoritative for a query iff **both** `window.start_ms >=
/// snapshot.covered_from_ms` **and** `window.end_ms <=
/// snapshot.sweep_time_ms + staleness_threshold_ms`. The lower-bound half
/// is the original out-of-window rule; the upper-bound half **replaces**
/// the previous wall-clock-only staleness check (`now - refreshed_at`) — a
/// periodic-refresh cache structurally cannot contain a series whose first
/// activity is after its last sweep, so a query whose end reaches past
/// `sweep_time_ms` risks missing brand-new series. For a live `end == now`
/// query the two formulations are equivalent; for a `end` further in the
/// past the new rule is correctly *more* permissive (no new-series risk
/// there), and for `end` beyond the staleness threshold it is correctly
/// *stricter*. This is a bounded, documented recency gap, not an
/// unbounded one: a brand-new series is invisible to the cache for at most
/// one refresh interval in normal operation (worst case
/// `staleness_threshold_ms = staleness_multiplier * ttl`), after which the
/// query is forced to the SQL fallback — see docs/architecture.md §5.2.
pub(crate) fn resolve_over(
    snapshot: &CacheSnapshot,
    regex_cache: &RegexCache,
    metrics: &CacheMetrics,
    config: &LabelCacheConfig,
    metric_name: &str,
    matchers: &[LabelMatcher],
    window: DataWindow,
) -> Resolution {
    if snapshot.generation == 0 {
        metrics.miss_cold_total.fetch_add(1, Ordering::Relaxed);
        return sql_fallback(
            config,
            metric_name,
            window,
            matchers,
            FallbackReason::ColdCache,
        );
    }

    if window.start_ms < snapshot.covered_from_ms {
        metrics
            .miss_out_of_window_total
            .fetch_add(1, Ordering::Relaxed);
        return sql_fallback(
            config,
            metric_name,
            window,
            matchers,
            FallbackReason::OutOfWindow,
        );
    }

    let staleness_threshold_ms =
        config.ttl.as_millis() as i64 * i64::from(config.staleness_multiplier);
    let recency_edge_ms = snapshot
        .sweep_time_ms
        .saturating_add(staleness_threshold_ms);
    if window.end_ms > recency_edge_ms {
        let age_ms = window.end_ms.saturating_sub(snapshot.sweep_time_ms).max(0) as u64;
        metrics.miss_stale_total.fetch_add(1, Ordering::Relaxed);
        return sql_fallback(
            config,
            metric_name,
            window,
            matchers,
            FallbackReason::StaleCache { age_ms },
        );
    }

    let Some(candidates) = snapshot.by_metric.get(metric_name) else {
        metrics.hits_total.fetch_add(1, Ordering::Relaxed);
        return Resolution::Fingerprints(Vec::new());
    };

    let mut matched = Vec::new();
    for &fp in candidates {
        let Some(labels) = snapshot.by_fingerprint.get(&fp) else {
            continue;
        };
        match matches(regex_cache, labels, matchers) {
            Ok(true) => matched.push(fp),
            Ok(false) => {}
            Err(reason) => {
                metrics
                    .miss_regex_unsupported_total
                    .fetch_add(1, Ordering::Relaxed);
                return sql_fallback(config, metric_name, window, matchers, reason);
            }
        }
    }

    if matched.len() as u64 > config.cache_max_series {
        metrics
            .miss_over_cardinality_total
            .fetch_add(1, Ordering::Relaxed);
        let matched_count = matched.len();
        return sql_fallback(
            config,
            metric_name,
            window,
            matchers,
            FallbackReason::OverCardinality {
                matched: matched_count,
                cap: config.cache_max_series,
            },
        );
    }

    matched.sort_unstable();
    matched.dedup();
    metrics.hits_total.fetch_add(1, Ordering::Relaxed);
    Resolution::Fingerprints(matched)
}

/// [`LabelCache::resolve_labelled`]'s pure core — issue #31's addition,
/// mirroring [`resolve_over`]'s every gate (cold/out-of-window/stale/
/// regex-unsupported/over-cardinality) exactly, but collecting each
/// matched fingerprint's [`LabelSet`] alongside it rather than discarding
/// it. Kept as a near-duplicate of [`resolve_over`] rather than having one
/// call the other: [`resolve_over`]'s `Resolution::Fingerprints` is a
/// stable public contract already depended on by other call sites/tests,
/// and re-deriving labels from fingerprints after the fact would mean a
/// second (possibly-swapped) snapshot read — reading the snapshot once and
/// walking it once for both fingerprint and label output avoids that
/// race entirely.
pub(crate) fn resolve_labelled_over(
    snapshot: &CacheSnapshot,
    regex_cache: &RegexCache,
    metrics: &CacheMetrics,
    config: &LabelCacheConfig,
    metric_name: &str,
    matchers: &[LabelMatcher],
    window: DataWindow,
) -> LabelledResolution {
    if snapshot.generation == 0 {
        metrics.miss_cold_total.fetch_add(1, Ordering::Relaxed);
        return labelled_sql_fallback(
            config,
            metric_name,
            window,
            matchers,
            FallbackReason::ColdCache,
        );
    }

    if window.start_ms < snapshot.covered_from_ms {
        metrics
            .miss_out_of_window_total
            .fetch_add(1, Ordering::Relaxed);
        return labelled_sql_fallback(
            config,
            metric_name,
            window,
            matchers,
            FallbackReason::OutOfWindow,
        );
    }

    let staleness_threshold_ms =
        config.ttl.as_millis() as i64 * i64::from(config.staleness_multiplier);
    let recency_edge_ms = snapshot
        .sweep_time_ms
        .saturating_add(staleness_threshold_ms);
    if window.end_ms > recency_edge_ms {
        let age_ms = window.end_ms.saturating_sub(snapshot.sweep_time_ms).max(0) as u64;
        metrics.miss_stale_total.fetch_add(1, Ordering::Relaxed);
        return labelled_sql_fallback(
            config,
            metric_name,
            window,
            matchers,
            FallbackReason::StaleCache { age_ms },
        );
    }

    let Some(candidates) = snapshot.by_metric.get(metric_name) else {
        metrics.hits_total.fetch_add(1, Ordering::Relaxed);
        return LabelledResolution::Series(Vec::new());
    };

    let mut matched: Vec<(Fingerprint, LabelSet)> = Vec::new();
    for &fp in candidates {
        let Some(labels) = snapshot.by_fingerprint.get(&fp) else {
            continue;
        };
        match matches(regex_cache, labels, matchers) {
            Ok(true) => matched.push((fp, labels.clone())),
            Ok(false) => {}
            Err(reason) => {
                metrics
                    .miss_regex_unsupported_total
                    .fetch_add(1, Ordering::Relaxed);
                return labelled_sql_fallback(config, metric_name, window, matchers, reason);
            }
        }
    }

    if matched.len() as u64 > config.cache_max_series {
        metrics
            .miss_over_cardinality_total
            .fetch_add(1, Ordering::Relaxed);
        let matched_count = matched.len();
        return labelled_sql_fallback(
            config,
            metric_name,
            window,
            matchers,
            FallbackReason::OverCardinality {
                matched: matched_count,
                cap: config.cache_max_series,
            },
        );
    }

    matched.sort_unstable_by_key(|(fp, _)| *fp);
    metrics.hits_total.fetch_add(1, Ordering::Relaxed);
    LabelledResolution::Series(matched)
}

/// The resident label cache: owns the snapshot slot, config, the compiled-
/// regex cache, the `ChClient` the refresh sweep queries through, and the
/// metrics atomics. Fields are `pub(crate)` — visible to [`super::refresh`]
/// (which owns the sweep + swap) without leaking outside this crate.
pub struct LabelCache {
    pub(crate) client: ChClient,
    pub(crate) config: LabelCacheConfig,
    pub(crate) snapshot: RwLock<Arc<CacheSnapshot>>,
    pub(crate) regex_cache: RegexCache,
    pub(crate) metrics: CacheMetrics,
}

impl LabelCache {
    /// Starts cold (`generation == 0`) — [`LabelCache::is_warm`] is `false`
    /// until the first successful [`super::refresh::spawn_refresh_loop`]
    /// sweep.
    pub fn new(client: ChClient, cfg: LabelCacheConfig) -> Self {
        LabelCache {
            client,
            config: cfg,
            snapshot: RwLock::new(Arc::new(CacheSnapshot::default())),
            regex_cache: RegexCache::new(REGEX_CACHE_CAPACITY),
            metrics: CacheMetrics::default(),
        }
    }

    /// One refresh sweep + atomic swap (delegates to [`super::refresh`],
    /// the only ClickHouse-touching code in this module). A failed sweep
    /// leaves the last good snapshot in place — see
    /// [`super::refresh::run_sweep`]'s doc comment.
    pub async fn refresh(&self) -> Result<(), pulsus_clickhouse::ChError> {
        super::refresh::run_sweep(self).await
    }

    /// `true` once at least one sweep has succeeded (task-manager
    /// resolution #3 on issue #30: warm-empty counts as warm — a fresh
    /// cluster with no `metric_series` rows yet must still become ready).
    /// Never holds the snapshot lock across an `.await` (this method isn't
    /// even async): a brief read-lock clone-then-drop, mirroring
    /// `ops::ready`'s own discipline.
    pub fn is_warm(&self) -> bool {
        self.current_snapshot().generation >= 1
    }

    /// Age since the last successful sweep, in milliseconds
    /// (`now_ms - snapshot.sweep_time_ms`) — `None` when cold (`generation
    /// == 0`, no successful sweep yet). Code-review round-2 fix: the
    /// `/metrics` age gauge. Deliberately **not** a stored atomic — age
    /// changes continuously, so it is derived fresh on every call from the
    /// snapshot's `sweep_time_ms` (the same recency anchor
    /// [`resolve_over`]'s upper-bound gate uses) and the wall clock.
    pub fn age_ms(&self) -> Option<u64> {
        let snapshot = self.current_snapshot();
        if snapshot.generation == 0 {
            return None;
        }
        let now_ms = super::refresh::now_unix_ms();
        Some(now_ms.saturating_sub(snapshot.sweep_time_ms).max(0) as u64)
    }

    pub fn metrics(&self) -> CacheMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub(crate) fn current_snapshot(&self) -> Arc<CacheSnapshot> {
        let guard = match self.snapshot.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        Arc::clone(&guard)
    }
}

/// The resident cache's own summary of itself: `numSeries` plus a
/// bounded, sorted-descending top-N by per-metric series count (issue #32
/// `status/tsdb`, task-manager resolution #2: "from the resident #30 cache
/// snapshot, zero extra ClickHouse — freshness = cache TTL"). Deliberately
/// never a full unbounded metric-name listing: a cardinality report is
/// only ever useful as a bounded "top offenders" view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsdbCacheSnapshot {
    pub num_series: u64,
    /// Sorted descending by count, ties broken ascending by name, capped
    /// at [`TSDB_TOP_METRIC_NAMES`].
    pub series_count_by_metric_name: Vec<(String, u64)>,
}

/// The bound on `status/tsdb`'s `seriesCountByMetricName` (issue #32) — a
/// documented constant, same "cap first, promote to a config knob only if a
/// deployment needs it" precedent as [`REGEX_CACHE_CAPACITY`].
pub const TSDB_TOP_METRIC_NAMES: usize = 10;

/// [`LabelCache::tsdb_snapshot`]'s pure core, factored out the same way
/// [`resolve_over`] is: testable against a hand-built [`CacheSnapshot`]
/// with no `ChClient` at all.
pub(crate) fn tsdb_snapshot_over(snapshot: &CacheSnapshot) -> TsdbCacheSnapshot {
    let mut by_metric: Vec<(String, u64)> = snapshot
        .by_metric
        .iter()
        .map(|(name, fps)| (name.clone(), fps.len() as u64))
        .collect();
    by_metric.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    by_metric.truncate(TSDB_TOP_METRIC_NAMES);
    TsdbCacheSnapshot {
        num_series: snapshot.series_count() as u64,
        series_count_by_metric_name: by_metric,
    }
}

impl LabelCache {
    /// Issue #32's `status/tsdb` accessor: a lock-free read of the current
    /// snapshot (mirrors [`LabelCache::is_warm`]'s own discipline — never
    /// held across an `.await`, and this method isn't even async). A cold
    /// cache (`generation == 0`) yields an all-zero, empty summary rather
    /// than a ClickHouse fallback query (task-manager resolution #2: "no
    /// SQL variant for M2").
    pub fn tsdb_snapshot(&self) -> TsdbCacheSnapshot {
        tsdb_snapshot_over(&self.current_snapshot())
    }
}

impl SeriesResolver for LabelCache {
    fn resolve(
        &self,
        metric_name: &str,
        matchers: &[LabelMatcher],
        window: DataWindow,
    ) -> Resolution {
        let snapshot = self.current_snapshot();
        resolve_over(
            &snapshot,
            &self.regex_cache,
            &self.metrics,
            &self.config,
            metric_name,
            matchers,
            window,
        )
    }
}

impl LabelCache {
    /// Issue #31's addition: like [`SeriesResolver::resolve`], but carries
    /// each matched fingerprint's [`LabelSet`] — the zero-ClickHouse
    /// `count`/`group` fast path needs labels (to compute a
    /// `by`/`without` grouping key), not just fingerprints. Not part of
    /// the [`SeriesResolver`] trait itself (that contract is #30's and
    /// already depended on elsewhere with its narrower fingerprints-only
    /// signature) — an inherent method alongside it instead.
    pub fn resolve_labelled(
        &self,
        metric_name: &str,
        matchers: &[LabelMatcher],
        window: DataWindow,
    ) -> LabelledResolution {
        let snapshot = self.current_snapshot();
        resolve_labelled_over(
            &snapshot,
            &self.regex_cache,
            &self.metrics,
            &self.config,
            metric_name,
            matchers,
            window,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::from_verbatim(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    fn config() -> LabelCacheConfig {
        LabelCacheConfig {
            db: "pulsus".to_string(),
            series_table: "metric_series".to_string(),
            bucket_ms: 3_600_000,
            window_ms: 24 * 3_600_000,
            cache_max_series: 50_000,
            ttl: Duration::from_secs(60),
            staleness_multiplier: DEFAULT_STALENESS_MULTIPLIER,
        }
    }

    /// `(metric_name, fingerprint, label pairs)` — a test-only shorthand for
    /// [`snapshot`]'s fixture entries, factored into a `type` alias purely
    /// to keep the function signature under clippy's type-complexity lint.
    type SnapshotEntry<'a> = (&'a str, Fingerprint, &'a [(&'a str, &'a str)]);

    /// A `sweep_time_ms` baseline far larger than any `window`/
    /// `covered_from_ms` value used by tests that are not specifically
    /// exercising the recency gate — keeps every such test's `window.end_ms
    /// <= sweep_time_ms + staleness_threshold_ms` trivially true regardless
    /// of `ttl`/`staleness_multiplier`, so only the tests that intend to
    /// probe [`FallbackReason::StaleCache`] need to reason about it.
    const BASE_SWEEP_MS: i64 = 1_000_000_000_000;

    fn snapshot(
        entries: Vec<SnapshotEntry<'_>>,
        covered_from_ms: i64,
        sweep_time_ms: i64,
        generation: u64,
    ) -> CacheSnapshot {
        let mut by_fingerprint = HashMap::new();
        let mut by_metric: HashMap<String, Vec<Fingerprint>> = HashMap::new();
        for (metric_name, fp, pairs) in entries {
            by_fingerprint.insert(fp, labels(pairs));
            by_metric
                .entry(metric_name.to_string())
                .or_default()
                .push(fp);
        }
        for fps in by_metric.values_mut() {
            fps.sort_unstable();
        }
        CacheSnapshot {
            by_fingerprint,
            by_metric,
            sweep_time_ms,
            covered_from_ms,
            generation,
        }
    }

    fn window(start_ms: i64, end_ms: i64) -> DataWindow {
        DataWindow { start_ms, end_ms }
    }

    fn resolve(
        snap: &CacheSnapshot,
        cfg: &LabelCacheConfig,
        metric_name: &str,
        matchers: &[LabelMatcher],
        w: DataWindow,
    ) -> Resolution {
        let regex_cache = RegexCache::new(REGEX_CACHE_CAPACITY);
        let metrics = CacheMetrics::default();
        resolve_over(snap, &regex_cache, &metrics, cfg, metric_name, matchers, w)
    }

    #[test]
    fn a_cold_cache_always_falls_back_regardless_of_window() {
        let snap = CacheSnapshot::default();
        assert_eq!(snap.generation, 0);
        let res = resolve(&snap, &config(), "up", &[], window(0, 1_000));
        match res {
            Resolution::SqlFallback { reason, .. } => {
                assert_eq!(reason, FallbackReason::ColdCache)
            }
            other => panic!("expected SqlFallback, got {other:?}"),
        }
    }

    #[test]
    fn a_warm_in_window_query_with_no_matchers_returns_sorted_fingerprints() {
        let snap = snapshot(
            vec![("up", 20, &[("job", "api")]), ("up", 10, &[("job", "web")])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        match resolve(&snap, &config(), "up", &[], window(0, 1_000)) {
            Resolution::Fingerprints(fps) => assert_eq!(fps, vec![10, 20]),
            other => panic!("expected Fingerprints, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_metric_name_returns_an_empty_fingerprint_vector_not_a_fallback() {
        let snap = snapshot(vec![], 0, BASE_SWEEP_MS, 1);
        match resolve(&snap, &config(), "unknown_metric", &[], window(0, 1_000)) {
            Resolution::Fingerprints(fps) => assert!(fps.is_empty()),
            other => panic!("expected empty Fingerprints, got {other:?}"),
        }
    }

    #[test]
    fn eq_matcher_filters_to_the_matching_series() {
        let snap = snapshot(
            vec![("up", 1, &[("job", "api")]), ("up", 2, &[("job", "web")])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "api".to_string(),
        };
        match resolve(&snap, &config(), "up", &[m], window(0, 1_000)) {
            Resolution::Fingerprints(fps) => assert_eq!(fps, vec![1]),
            other => panic!("expected Fingerprints, got {other:?}"),
        }
    }

    /// Absent-label negative-matcher parity (architect plan edge case 4):
    /// `label != "x"` must include series lacking the label entirely
    /// (Prometheus absent == `""`).
    #[test]
    fn neq_matcher_includes_series_missing_the_label_entirely() {
        let snap = snapshot(
            vec![("up", 1, &[("job", "api")]), ("up", 2, &[])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Neq,
            value: "api".to_string(),
        };
        match resolve(&snap, &config(), "up", &[m], window(0, 1_000)) {
            Resolution::Fingerprints(fps) => assert_eq!(fps, vec![2]),
            other => panic!("expected Fingerprints, got {other:?}"),
        }
    }

    #[test]
    fn re_matcher_evaluates_in_process_and_returns_sorted_fingerprints() {
        let snap = snapshot(
            vec![
                ("http_requests_total", 30, &[("status", "500")]),
                ("http_requests_total", 10, &[("status", "503")]),
                ("http_requests_total", 20, &[("status", "200")]),
            ],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "status".to_string(),
            op: MatchOp::Re,
            value: "5..".to_string(),
        };
        match resolve(
            &snap,
            &config(),
            "http_requests_total",
            &[m],
            window(0, 1_000),
        ) {
            Resolution::Fingerprints(fps) => assert_eq!(fps, vec![10, 30]),
            other => panic!("expected Fingerprints, got {other:?}"),
        }
    }

    #[test]
    fn nre_matcher_negates_the_regex_result() {
        let snap = snapshot(
            vec![
                ("http_requests_total", 1, &[("status", "500")]),
                ("http_requests_total", 2, &[("status", "200")]),
            ],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "status".to_string(),
            op: MatchOp::Nre,
            value: "5..".to_string(),
        };
        match resolve(
            &snap,
            &config(),
            "http_requests_total",
            &[m],
            window(0, 1_000),
        ) {
            Resolution::Fingerprints(fps) => assert_eq!(fps, vec![2]),
            other => panic!("expected Fingerprints, got {other:?}"),
        }
    }

    #[test]
    fn an_uncompilable_regex_degrades_to_sql_fallback_with_the_matcher_key() {
        let snap = snapshot(vec![("up", 1, &[("job", "api")])], 0, BASE_SWEEP_MS, 1);
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Re,
            value: "(unclosed".to_string(),
        };
        match resolve(&snap, &config(), "up", &[m], window(0, 1_000)) {
            Resolution::SqlFallback { reason, sql } => {
                assert_eq!(
                    reason,
                    FallbackReason::RegexUnsupported {
                        key: "job".to_string()
                    }
                );
                assert!(sql.contains("metric_series"));
            }
            other => panic!("expected SqlFallback, got {other:?}"),
        }
    }

    #[test]
    fn a_stale_cache_falls_back_with_the_observed_age() {
        let mut cfg = config();
        cfg.ttl = Duration::from_millis(1);
        cfg.staleness_multiplier = 1; // staleness_threshold_ms = 1
        // sweep_time_ms = 0; a query whose end reaches far past the 1ms
        // threshold must fall back, and `age_ms` must reflect how far past
        // the sweep the query's own end reaches (not wall-clock age).
        let snap = snapshot(vec![], 0, 0, 1);
        match resolve(&snap, &cfg, "up", &[], window(0, 1_000)) {
            Resolution::SqlFallback { reason, .. } => {
                assert_eq!(reason, FallbackReason::StaleCache { age_ms: 1_000 });
            }
            other => panic!("expected SqlFallback, got {other:?}"),
        }
    }

    /// Recency-gate boundary tests (code-review round-2 fix, adjudicated
    /// rule): cache authoritative iff `start_ms >= covered_from_ms` AND
    /// `end_ms <= sweep_time_ms + staleness_threshold_ms`. `ttl = 100ms`,
    /// `staleness_multiplier = 3` -> `staleness_threshold_ms = 300`;
    /// `sweep_time_ms = 1_000`.
    fn recency_config() -> LabelCacheConfig {
        let mut cfg = config();
        cfg.ttl = Duration::from_millis(100);
        cfg.staleness_multiplier = 3;
        cfg
    }

    #[test]
    fn recency_gate_end_equal_to_sweep_time_is_cache_authoritative() {
        let snap = snapshot(vec![("up", 1, &[("job", "api")])], 0, 1_000, 1);
        assert!(matches!(
            resolve(&snap, &recency_config(), "up", &[], window(0, 1_000)),
            Resolution::Fingerprints(_)
        ));
    }

    #[test]
    fn recency_gate_end_equal_to_sweep_time_plus_ttl_is_cache_authoritative() {
        let snap = snapshot(vec![("up", 1, &[("job", "api")])], 0, 1_000, 1);
        // sweep_time_ms(1000) + ttl(100) = 1100.
        assert!(matches!(
            resolve(&snap, &recency_config(), "up", &[], window(0, 1_100)),
            Resolution::Fingerprints(_)
        ));
    }

    #[test]
    fn recency_gate_end_equal_to_the_staleness_threshold_is_cache_authoritative_inclusive() {
        let snap = snapshot(vec![("up", 1, &[("job", "api")])], 0, 1_000, 1);
        // sweep_time_ms(1000) + staleness_threshold_ms(300) = 1300, inclusive.
        assert!(matches!(
            resolve(&snap, &recency_config(), "up", &[], window(0, 1_300)),
            Resolution::Fingerprints(_)
        ));
    }

    #[test]
    fn recency_gate_end_past_the_staleness_threshold_falls_back_to_sql() {
        let snap = snapshot(vec![("up", 1, &[("job", "api")])], 0, 1_000, 1);
        match resolve(&snap, &recency_config(), "up", &[], window(0, 1_301)) {
            Resolution::SqlFallback { reason, .. } => {
                assert_eq!(reason, FallbackReason::StaleCache { age_ms: 301 });
            }
            other => panic!("expected SqlFallback(StaleCache), got {other:?}"),
        }
    }

    /// Correctness case for the recency gate (code-review round-2 fix): a
    /// series first seen *after* `sweep_time_ms` is structurally invisible
    /// to the in-process snapshot — this is the bounded, documented gap
    /// architecture.md §5.2 accepts, not a bug. It is recovered two ways:
    /// (a) once the query's own `end_ms` crosses the staleness threshold,
    /// the resolver forces the SQL fallback (which sees the real table);
    /// (b) after the *next* refresh, the snapshot itself picks the series
    /// up and answers it in-process.
    #[test]
    fn a_series_first_seen_after_sweep_time_is_recovered_via_fallback_then_via_the_next_refresh() {
        let cfg = recency_config();
        // Snapshot A: swept at sweep_time_ms = 1_000, before the new series
        // (fingerprint 2) was ever registered — it simply isn't there.
        let snap_a = snapshot(vec![("up", 1, &[("job", "api")])], 0, 1_000, 1);

        // (within the recency window) the cache answers, but of course
        // cannot know about a series it never swept.
        match resolve(&snap_a, &cfg, "up", &[], window(0, 1_100)) {
            Resolution::Fingerprints(fps) => assert_eq!(fps, vec![1]),
            other => panic!("expected Fingerprints, got {other:?}"),
        }

        // (b) once `end_ms` crosses the staleness threshold, the resolver
        // itself forces the SQL fallback rather than risk answering from a
        // snapshot that predates the query's own reach.
        match resolve(&snap_a, &cfg, "up", &[], window(0, 1_301)) {
            Resolution::SqlFallback {
                reason: FallbackReason::StaleCache { .. },
                ..
            } => {}
            other => panic!("expected SqlFallback(StaleCache), got {other:?}"),
        }

        // (a) after the next refresh sweeps the new series in, the
        // snapshot answers it in-process again — no permanent gap.
        let snap_b = snapshot(
            vec![("up", 1, &[("job", "api")]), ("up", 2, &[("job", "web")])],
            0,
            1_100,
            2,
        );
        match resolve(&snap_b, &cfg, "up", &[], window(0, 1_150)) {
            Resolution::Fingerprints(fps) => assert_eq!(fps, vec![1, 2]),
            other => panic!("expected Fingerprints, got {other:?}"),
        }
    }

    /// The whole point of the time-awareness invariant (architect plan edge
    /// case 2): a query reaching further back than the cache's covered
    /// window must never be answered from the cache, even though the cache
    /// is warm and fresh.
    #[test]
    fn a_query_starting_before_the_covered_window_falls_back_out_of_window() {
        let snap = snapshot(vec![("up", 1, &[("job", "api")])], 10_000, BASE_SWEEP_MS, 1);
        match resolve(&snap, &config(), "up", &[], window(0, 20_000)) {
            Resolution::SqlFallback { reason, .. } => {
                assert_eq!(reason, FallbackReason::OutOfWindow)
            }
            other => panic!("expected SqlFallback, got {other:?}"),
        }
    }

    #[test]
    fn a_query_fully_inside_the_covered_window_is_never_out_of_window() {
        let snap = snapshot(vec![("up", 1, &[("job", "api")])], 10_000, BASE_SWEEP_MS, 1);
        assert!(matches!(
            resolve(&snap, &config(), "up", &[], window(10_000, 20_000)),
            Resolution::Fingerprints(_)
        ));
    }

    #[test]
    fn a_match_exceeding_cache_max_series_falls_back_to_sql_not_a_giant_in_list() {
        let mut cfg = config();
        cfg.cache_max_series = 1;
        let snap = snapshot(
            vec![("up", 1, &[("job", "api")]), ("up", 2, &[("job", "api")])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "api".to_string(),
        };
        match resolve(&snap, &cfg, "up", &[m], window(0, 1_000)) {
            Resolution::SqlFallback { reason, sql } => {
                assert_eq!(
                    reason,
                    FallbackReason::OverCardinality { matched: 2, cap: 1 }
                );
                assert!(!sql.contains(" IN ("));
            }
            other => panic!("expected SqlFallback, got {other:?}"),
        }
    }

    #[test]
    fn is_warm_semantics_are_generation_at_least_one() {
        // Warm-empty readiness (task-manager resolution #3): generation 1
        // with zero resident series must still be warm; generation 0 must
        // not be.
        assert!(CacheSnapshot::default().generation < 1);
        let warm_empty = snapshot(vec![], 0, BASE_SWEEP_MS, 1);
        assert!(warm_empty.generation >= 1);
    }

    #[test]
    fn regex_cache_returns_none_once_at_capacity_for_a_new_pattern() {
        let cache = RegexCache::new(1);
        assert_eq!(cache.is_match("a", "a"), Some(true));
        // Capacity is 1 and "a" already occupies it; a second, distinct
        // pattern must not be admitted.
        assert_eq!(cache.is_match("b", "b"), None);
        // The already-cached pattern keeps working.
        assert_eq!(cache.is_match("a", "b"), Some(false));
    }

    #[test]
    fn regex_cache_anchors_every_pattern() {
        let cache = RegexCache::new(4);
        // Unanchored "b" would match "ab" as a substring; anchored, it must not.
        assert_eq!(cache.is_match("b", "ab"), Some(false));
        assert_eq!(cache.is_match("b", "b"), Some(true));
    }

    #[test]
    fn regex_cache_rejects_an_uncompilable_pattern() {
        let cache = RegexCache::new(4);
        assert_eq!(cache.is_match("(unclosed", "x"), None);
    }

    // --- resolve_labelled (issue #31) ---

    fn resolve_labelled(
        snap: &CacheSnapshot,
        cfg: &LabelCacheConfig,
        metric_name: &str,
        matchers: &[LabelMatcher],
        w: DataWindow,
    ) -> LabelledResolution {
        let regex_cache = RegexCache::new(REGEX_CACHE_CAPACITY);
        let metrics = CacheMetrics::default();
        resolve_labelled_over(snap, &regex_cache, &metrics, cfg, metric_name, matchers, w)
    }

    #[test]
    fn resolve_labelled_returns_fingerprints_paired_with_their_labels() {
        let snap = snapshot(
            vec![("up", 20, &[("job", "api")]), ("up", 10, &[("job", "web")])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        match resolve_labelled(&snap, &config(), "up", &[], window(0, 1_000)) {
            LabelledResolution::Series(series) => {
                assert_eq!(series.len(), 2);
                // Sorted by fingerprint (10, 20).
                assert_eq!(series[0].0, 10);
                assert_eq!(series[0].1.get("job"), Some("web"));
                assert_eq!(series[1].0, 20);
                assert_eq!(series[1].1.get("job"), Some("api"));
            }
            other => panic!("expected Series, got {other:?}"),
        }
    }

    #[test]
    fn resolve_labelled_applies_matchers_identically_to_resolve() {
        let snap = snapshot(
            vec![("up", 1, &[("job", "api")]), ("up", 2, &[("job", "web")])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "api".to_string(),
        };
        match resolve_labelled(&snap, &config(), "up", &[m], window(0, 1_000)) {
            LabelledResolution::Series(series) => {
                assert_eq!(series, vec![(1, labels(&[("job", "api")]))]);
            }
            other => panic!("expected Series, got {other:?}"),
        }
    }

    #[test]
    fn resolve_labelled_of_a_cold_cache_falls_back_with_the_same_reason_as_resolve() {
        let snap = CacheSnapshot::default();
        match resolve_labelled(&snap, &config(), "up", &[], window(0, 1_000)) {
            LabelledResolution::SqlFallback { reason, .. } => {
                assert_eq!(reason, FallbackReason::ColdCache)
            }
            other => panic!("expected SqlFallback, got {other:?}"),
        }
    }

    #[test]
    fn resolve_labelled_out_of_window_falls_back_to_the_same_sql_metric_series_routes_through() {
        let snap = snapshot(vec![("up", 1, &[("job", "api")])], 10_000, BASE_SWEEP_MS, 1);
        match resolve_labelled(&snap, &config(), "up", &[], window(0, 20_000)) {
            LabelledResolution::SqlFallback { reason, sql } => {
                assert_eq!(reason, FallbackReason::OutOfWindow);
                assert!(sql.contains("metric_series"));
            }
            other => panic!("expected SqlFallback, got {other:?}"),
        }
    }

    #[test]
    fn resolve_labelled_an_unknown_metric_name_is_an_empty_series_not_a_fallback() {
        let snap = snapshot(vec![], 0, BASE_SWEEP_MS, 1);
        match resolve_labelled(&snap, &config(), "unknown_metric", &[], window(0, 1_000)) {
            LabelledResolution::Series(series) => assert!(series.is_empty()),
            other => panic!("expected empty Series, got {other:?}"),
        }
    }

    #[test]
    fn resolve_labelled_over_cardinality_falls_back_to_sql() {
        let mut cfg = config();
        cfg.cache_max_series = 1;
        let snap = snapshot(
            vec![("up", 1, &[("job", "api")]), ("up", 2, &[("job", "api")])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "api".to_string(),
        };
        match resolve_labelled(&snap, &cfg, "up", &[m], window(0, 1_000)) {
            LabelledResolution::SqlFallback { reason, .. } => {
                assert_eq!(
                    reason,
                    FallbackReason::OverCardinality { matched: 2, cap: 1 }
                );
            }
            other => panic!("expected SqlFallback, got {other:?}"),
        }
    }

    #[test]
    fn resolve_labelled_matches_resolve_on_which_fingerprints_hit() {
        // Differential-style sanity check: the two paths' matcher
        // evaluation must never diverge on the *set* of matched
        // fingerprints, only on whether labels are carried alongside.
        let snap = snapshot(
            vec![
                ("http_requests_total", 30, &[("status", "500")]),
                ("http_requests_total", 10, &[("status", "503")]),
                ("http_requests_total", 20, &[("status", "200")]),
            ],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "status".to_string(),
            op: MatchOp::Re,
            value: "5..".to_string(),
        };
        let cfg = config();
        let plain = resolve(
            &snap,
            &cfg,
            "http_requests_total",
            std::slice::from_ref(&m),
            window(0, 1_000),
        );
        let labelled = resolve_labelled(&snap, &cfg, "http_requests_total", &[m], window(0, 1_000));
        match (plain, labelled) {
            (Resolution::Fingerprints(fps), LabelledResolution::Series(series)) => {
                let labelled_fps: Vec<Fingerprint> = series.iter().map(|(fp, _)| *fp).collect();
                assert_eq!(fps, labelled_fps);
            }
            other => panic!("expected matching Fingerprints/Series, got {other:?}"),
        }
    }

    // --- tsdb_snapshot_over (issue #32) ---

    #[test]
    fn tsdb_snapshot_over_a_cold_cache_is_empty() {
        let snap = tsdb_snapshot_over(&CacheSnapshot::default());
        assert_eq!(snap.num_series, 0);
        assert!(snap.series_count_by_metric_name.is_empty());
    }

    #[test]
    fn tsdb_snapshot_over_counts_series_and_sorts_by_metric_descending() {
        let snap = snapshot(
            vec![
                ("up", 1, &[]),
                ("up", 2, &[]),
                ("http_requests_total", 3, &[]),
            ],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let summary = tsdb_snapshot_over(&snap);
        assert_eq!(summary.num_series, 3);
        assert_eq!(
            summary.series_count_by_metric_name,
            vec![
                ("up".to_string(), 2),
                ("http_requests_total".to_string(), 1)
            ]
        );
    }

    #[test]
    fn tsdb_snapshot_over_ties_break_ascending_by_name() {
        let snap = snapshot(
            vec![("zeta", 1, &[]), ("alpha", 2, &[])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let summary = tsdb_snapshot_over(&snap);
        assert_eq!(
            summary.series_count_by_metric_name,
            vec![("alpha".to_string(), 1), ("zeta".to_string(), 1)]
        );
    }

    #[test]
    fn tsdb_snapshot_over_caps_at_the_top_n_metric_names() {
        const NAMES: [&str; TSDB_TOP_METRIC_NAMES + 5] = [
            "m00", "m01", "m02", "m03", "m04", "m05", "m06", "m07", "m08", "m09", "m10", "m11",
            "m12", "m13", "m14",
        ];
        let entries: Vec<SnapshotEntry<'_>> = NAMES
            .iter()
            .enumerate()
            .map(|(i, name)| -> SnapshotEntry<'_> { (name, i as Fingerprint, &[][..]) })
            .collect();
        let snap = snapshot(entries, 0, BASE_SWEEP_MS, 1);
        let summary = tsdb_snapshot_over(&snap);
        assert_eq!(
            summary.series_count_by_metric_name.len(),
            TSDB_TOP_METRIC_NAMES
        );
    }
}
