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

/// [`LabelCache::resolve_labelled`]'s result — like [`Resolution`], but
/// carries each matched fingerprint's resolved [`LabelSet`], not just the
/// bare fingerprint. Originally added for issue #31's `count`/`group`
/// cache-only fast path (task-manager resolution #2 on issue #31, since
/// removed — issue #33 architect adjudication, bucket-granularity cache
/// resolution cannot reproduce PromQL's exact 5-minute lookback), it is
/// now the ordinary fetch path's own label-hydration mechanism
/// (`pulsus-read`'s `MetricsEngine::query_inner`): every fetched selector
/// needs each matched series' labels to build its final `(labels,
/// samples)` pair for the evaluator, not just fingerprints. On the SQL/
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

/// One matched metric's series set from a multi-metric resolution
/// (issue #85, M6-08c): the metric name plus its matcher-passing
/// `(fingerprint, labels)` pairs, sorted by fingerprint.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSeriesGroup {
    pub metric_name: String,
    pub series: Vec<(Fingerprint, LabelSet)>,
}

/// [`LabelCache::resolve_multi_metric`]'s result (issue #85, M6-08c) —
/// the name-less/regex-`__name__` selector resolution. Unlike
/// [`Resolution`]/[`LabelledResolution`] there is **no SQL fallback
/// variant**: the metric-scoped `historical_series_subquery` shape cannot
/// express "every metric name matching these name matchers", so a
/// degraded cache is a named, bounded failure — never an unbounded scan.
#[derive(Debug, Clone, PartialEq)]
pub enum MultiMetricResolution {
    /// Matched series grouped per metric name — names sorted ascending,
    /// fingerprints sorted within each group. Only non-empty groups are
    /// returned (a metric whose series all fail the label matchers never
    /// enters the fetch `IN` set).
    Groups(Vec<MetricSeriesGroup>),
    /// The cache is not authoritative for this window (cold / out-of-
    /// window / stale) or a matcher regex could not be evaluated
    /// in-process — the caller surfaces a named error.
    Unresolvable { reason: FallbackReason },
    /// More metric names matched than the configured fan-out cap
    /// (`ReaderConfig::promql_max_metric_fanout`, default 1000 — the #85
    /// adjudication) — the caller surfaces the named
    /// metric-fan-out-exceeded error; operator-scale tuning routes to
    /// issue #25.
    FanoutExceeded { matched: usize, cap: u64 },
    /// Issue #89 (retroactive re-review): the walk *examined* more cache
    /// entries (metric names plus candidate fingerprints) than
    /// `ReaderConfig::promql_max_cache_scan` (default 200_000) before it
    /// could finish — a regex/negated-`__name__` selector with a low or
    /// zero match rate can examine the whole resident cache without
    /// tripping [`FanoutExceeded`]/`OverCardinality` (which count only
    /// matched results). This is an independent, examined-entry bound: the
    /// walk bails the instant the count would cross the budget, so
    /// `examined` is a **lower bound** (it never exceeds `cap + 1`, and
    /// reaches exactly `cap + 1` whenever the walk had that many entries to
    /// examine). Never routed to the discovery `Probe` fallback — a warm
    /// cache that reaches this bound is not degraded, it is genuinely too
    /// broad.
    ScanBudgetExceeded { examined: usize, cap: u64 },
}

/// Issue #85: evaluates a selector's `name_matchers` against one concrete
/// candidate metric name. Regexes compile directly (anchored `^(?:…)$`,
/// the same anchoring as [`RegexCache`]/the SQL path) rather than through
/// the bounded cache — this runs once per query per matcher, never
/// per-series. `Err` carries the uncompilable pattern's label key
/// (unreachable through `parse()`, which validates matcher regexes —
/// kept total rather than trusting that upstream invariant).
pub(crate) fn concrete_name_matches(
    name_matchers: &[LabelMatcher],
    name: &str,
) -> Result<bool, FallbackReason> {
    for m in name_matchers {
        let ok = match m.op {
            MatchOp::Eq => name == m.value,
            MatchOp::Neq => name != m.value,
            MatchOp::Re | MatchOp::Nre => {
                let re = Regex::new(&format!("^(?:{})$", m.value))
                    .map_err(|_| FallbackReason::RegexUnsupported { key: m.key.clone() })?;
                let is_match = re.is_match(name);
                if m.op == MatchOp::Re {
                    is_match
                } else {
                    !is_match
                }
            }
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Evaluates `name_matchers` against a candidate metric name via the
/// bounded compiled-regex cache — the per-cache-key hot path of
/// [`resolve_multi_metric_over`].
fn cached_name_matches(
    regex_cache: &RegexCache,
    name_matchers: &[LabelMatcher],
    name: &str,
) -> Result<bool, FallbackReason> {
    for m in name_matchers {
        let ok = match m.op {
            MatchOp::Eq => name == m.value,
            MatchOp::Neq => name != m.value,
            MatchOp::Re => regex_cache
                .is_match(&m.value, name)
                .ok_or_else(|| FallbackReason::RegexUnsupported { key: m.key.clone() })?,
            MatchOp::Nre => !regex_cache
                .is_match(&m.value, name)
                .ok_or_else(|| FallbackReason::RegexUnsupported { key: m.key.clone() })?,
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The issue #85 name-less/regex-`__name__` resolution, pure over the
/// snapshot (the [`resolve_over`] factoring precedent): walk the
/// name-keyed `by_metric` map in native `HashMap` iteration order, keep
/// the names passing `name_matchers`, evaluate the ordinary label
/// `matchers` over each kept name's series, and return the non-empty
/// per-metric groups (sorted by name before returning — see below). Shares
/// [`resolve_over`]'s cold/out-of-window/stale gates verbatim (a snapshot
/// that cannot answer a single-metric query cannot answer a multi-metric
/// one either).
///
/// Issue #89 (retroactive re-review): the fan-out cap and the
/// `cache_max_series` total-series guard bound only the **matched**
/// result, not the **walk** — a selector whose name/label matchers yield
/// few or no matches (e.g. `{__name__=~".*", nonexistent="x"}`) previously
/// completed a full scan of every resident name (and every resident
/// fingerprint) before either guard could fire. `scan_budget` closes that
/// hole independently: `examined` counts one unit per metric name
/// inspected plus one per candidate fingerprint inspected, and the walk
/// bails to [`MultiMetricResolution::ScanBudgetExceeded`] the instant that
/// count would cross `scan_budget`. Per-query in-process work is therefore
/// bounded by `scan_budget + 1` examined entries regardless of resident
/// cache size — the fan-out/`cache_max_series` guards still bound the
/// matched result, `scan_budget` bounds the examined universe.
/// Pure scan-budget decision (issue #133, mirroring #96's
/// `probe_fanout_bound` extraction): `true` when the walk has examined
/// more entries than the budget admits. Extracted so the bound is
/// provable at the max config-accepted `promql_max_cache_scan`
/// (`pulsus_config::PROMQL_MAX_CACHE_SCAN_CEILING`) without an
/// O(ceiling) resident cache. Behavior-identical to the inline
/// `examined > scan_budget` it replaced.
#[inline]
fn scan_budget_exhausted(examined: u64, scan_budget: u64) -> bool {
    examined > scan_budget
}

/// Pure matched-series cardinality decision (issue #133): `true` when a
/// matched set exceeds `cache_max_series`. One decision point for the
/// three guard sites (multi-metric total, fingerprint resolve, labelled
/// resolve), provable at the max config-accepted cap
/// (`pulsus_config::CACHE_MAX_SERIES_CEILING`) with synthetic counts.
#[inline]
fn match_exceeds_series_cap(count: usize, cap: u64) -> bool {
    count as u64 > cap
}

#[allow(clippy::too_many_arguments)] // mirrors resolve_over/resolve_labelled_over's shape
pub(crate) fn resolve_multi_metric_over(
    snapshot: &CacheSnapshot,
    regex_cache: &RegexCache,
    metrics: &CacheMetrics,
    config: &LabelCacheConfig,
    name_matchers: &[LabelMatcher],
    matchers: &[LabelMatcher],
    window: DataWindow,
    fanout_cap: u64,
    scan_budget: u64,
) -> MultiMetricResolution {
    if snapshot.generation == 0 {
        metrics.miss_cold_total.fetch_add(1, Ordering::Relaxed);
        return MultiMetricResolution::Unresolvable {
            reason: FallbackReason::ColdCache,
        };
    }
    if window.start_ms < snapshot.covered_from_ms {
        metrics
            .miss_out_of_window_total
            .fetch_add(1, Ordering::Relaxed);
        return MultiMetricResolution::Unresolvable {
            reason: FallbackReason::OutOfWindow,
        };
    }
    let staleness_threshold_ms =
        config.ttl.as_millis() as i64 * i64::from(config.staleness_multiplier);
    let recency_edge_ms = snapshot
        .sweep_time_ms
        .saturating_add(staleness_threshold_ms);
    if window.end_ms > recency_edge_ms {
        let age_ms = window.end_ms.saturating_sub(snapshot.sweep_time_ms).max(0) as u64;
        metrics.miss_stale_total.fetch_add(1, Ordering::Relaxed);
        return MultiMetricResolution::Unresolvable {
            reason: FallbackReason::StaleCache { age_ms },
        };
    }

    // Issue #89 fix: walk `by_metric` directly in native `HashMap` order —
    // NO pre-loop `keys().collect()`/sort. The old sorted-name walk
    // allocated and sorted every resident name before either the fan-out
    // or `cache_max_series` guard could fire, an O(resident cache) cost
    // regardless of match rate. `examined` gates the enumeration itself:
    // it bails to `ScanBudgetExceeded` the instant it would cross
    // `scan_budget`, so at most `scan_budget + 1` names/fingerprints are
    // ever inspected. Determinism is preserved on the success path by
    // sorting the bounded `groups` output below (never the input
    // universe): when the walk completes under budget, every name was
    // examined, so the matched set is order-independent regardless of
    // which order the map was walked in.
    let mut groups: Vec<MetricSeriesGroup> = Vec::new();
    let mut total_series = 0usize;
    let mut examined: u64 = 0;
    for (name, candidates) in &snapshot.by_metric {
        examined += 1;
        if scan_budget_exhausted(examined, scan_budget) {
            metrics
                .miss_scan_budget_total
                .fetch_add(1, Ordering::Relaxed);
            return MultiMetricResolution::ScanBudgetExceeded {
                examined: examined as usize,
                cap: scan_budget,
            };
        }
        match cached_name_matches(regex_cache, name_matchers, name) {
            Ok(true) => {}
            Ok(false) => continue,
            Err(reason) => {
                metrics
                    .miss_regex_unsupported_total
                    .fetch_add(1, Ordering::Relaxed);
                return MultiMetricResolution::Unresolvable { reason };
            }
        }
        let mut matched: Vec<(Fingerprint, LabelSet)> = Vec::new();
        for &fp in candidates {
            examined += 1;
            if scan_budget_exhausted(examined, scan_budget) {
                metrics
                    .miss_scan_budget_total
                    .fetch_add(1, Ordering::Relaxed);
                return MultiMetricResolution::ScanBudgetExceeded {
                    examined: examined as usize,
                    cap: scan_budget,
                };
            }
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
                    return MultiMetricResolution::Unresolvable { reason };
                }
            }
        }
        if matched.is_empty() {
            continue;
        }
        total_series += matched.len();
        if match_exceeds_series_cap(total_series, config.cache_max_series) {
            metrics
                .miss_over_cardinality_total
                .fetch_add(1, Ordering::Relaxed);
            return MultiMetricResolution::Unresolvable {
                reason: FallbackReason::OverCardinality {
                    matched: total_series,
                    cap: config.cache_max_series,
                },
            };
        }
        if groups.len() as u64 >= fanout_cap {
            // One more non-empty group would exceed the cap — a named,
            // early-bailing rejection, never an unbounded IN set. The
            // reported `matched` is a lower bound (the walk stops here).
            metrics
                .miss_over_cardinality_total
                .fetch_add(1, Ordering::Relaxed);
            return MultiMetricResolution::FanoutExceeded {
                matched: groups.len() + 1,
                cap: fanout_cap,
            };
        }
        matched.sort_unstable_by_key(|(fp, _)| *fp);
        groups.push(MetricSeriesGroup {
            metric_name: name.clone(),
            series: matched,
        });
    }

    // Sort only the BOUNDED matched output (<= fanout_cap groups), not the
    // resident name universe: the walk completed under budget, so every
    // name was examined and the matched set is independent of `HashMap`
    // iteration order — sorting here reproduces the deterministic `IN`
    // list / explain trace the old pre-loop sort provided, at O(matched)
    // cost instead of O(resident).
    groups.sort_unstable_by(|a, b| a.metric_name.cmp(&b.metric_name));

    metrics.hits_total.fetch_add(1, Ordering::Relaxed);
    MultiMetricResolution::Groups(groups)
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

    if match_exceeds_series_cap(matched.len(), config.cache_max_series) {
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

    if match_exceeds_series_cap(matched.len(), config.cache_max_series) {
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
    /// each matched fingerprint's [`LabelSet`] — see [`LabelledResolution`]'s
    /// own doc for why the ordinary fetch path needs this (not just the
    /// now-removed cache-only fast path it was originally added for). Not
    /// part of the [`SeriesResolver`] trait itself (that contract is #30's
    /// and already depended on elsewhere with its narrower
    /// fingerprints-only signature) — an inherent method alongside it
    /// instead.
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

    /// Issue #85 (M6-08c): resolves a name-less/regex-`__name__` selector
    /// over the name-keyed snapshot — see [`resolve_multi_metric_over`]
    /// and [`MultiMetricResolution`] for the contract (no SQL fallback;
    /// degraded caches and cap breaches are named outcomes). `fanout_cap`
    /// is `ReaderConfig::promql_max_metric_fanout`; `scan_budget` (issue
    /// #89) is `ReaderConfig::promql_max_cache_scan` — both threaded per
    /// call by `MetricsEngine` rather than stored here (they are
    /// reader/query caps, not cache-shape parameters).
    pub fn resolve_multi_metric(
        &self,
        name_matchers: &[LabelMatcher],
        matchers: &[LabelMatcher],
        window: DataWindow,
        fanout_cap: u64,
        scan_budget: u64,
    ) -> MultiMetricResolution {
        let snapshot = self.current_snapshot();
        resolve_multi_metric_over(
            &snapshot,
            &self.regex_cache,
            &self.metrics,
            &self.config,
            name_matchers,
            matchers,
            window,
            fanout_cap,
            scan_budget,
        )
    }
}

/// Issue #89 (plan v3/v4): a narrow, `#[doc(hidden)]` test seam letting the
/// isolated allocation-gate binary (`tests/multi_metric_scan_alloc.rs`)
/// reach the `pub(crate)` resolver core without exposing it as public API.
/// These items cannot live under `#[cfg(test)]` — an external integration
/// test binary does not see this crate's `cfg(test)` items, and gating
/// them behind a cargo feature would either not compile under the plain
/// `cargo test --workspace` CI lane (feature off) or ship the seam anyway
/// (feature on by default) — so `#[doc(hidden)] pub` is the smallest
/// CI-runnable surface (plan review round 3). Internals (`RegexCache`,
/// `CacheMetrics`, `LabelCacheConfig` construction) stay `pub(crate)`/
/// private; only the two opaque entry points below are public.
impl CacheSnapshot {
    /// `n` one-series metrics (`m000000`..), each with an empty
    /// [`LabelSet`] — mirrors the `#[cfg(test)]` `many_metric_snapshot`
    /// helper, but public so the isolated alloc-gate binary can build a
    /// large resident-cache fixture with no `ChClient`/network dependency.
    /// Warm and window-open (`generation: 1`, `covered_from_ms: 0`, a
    /// sweep timestamp far in the future) so control reaches the walk
    /// rather than one of the cold/stale/out-of-window short-circuits.
    #[doc(hidden)]
    pub fn with_distinct_metric_names_for_test(n: usize) -> CacheSnapshot {
        let mut by_fingerprint = HashMap::with_capacity(n);
        let mut by_metric = HashMap::with_capacity(n);
        for i in 0..n {
            by_fingerprint.insert(i as Fingerprint, LabelSet::from_verbatim(Vec::new()));
            by_metric.insert(format!("m{i:06}"), vec![i as Fingerprint]);
        }
        CacheSnapshot {
            by_fingerprint,
            by_metric,
            // Mirrors the `#[cfg(test)]` `BASE_SWEEP_MS` baseline: far
            // larger than any window used against this fixture, so the
            // staleness gate never fires.
            sweep_time_ms: 1_000_000_000_000,
            covered_from_ms: 0,
            generation: 1,
        }
    }
}

/// Issue #89: an opaque probe over [`resolve_multi_metric_over`], reusing
/// [`RegexCache`] — the isolated alloc-gate binary's only way to reach the
/// `pub(crate)` resolver. Its name matcher is fixed at construction (a
/// reject-all `__name__` regex that matches no
/// [`CacheSnapshot::with_distinct_metric_names_for_test`] fixture name),
/// so every call examines up to `scan_budget + 1` names and rejects — the
/// binary measures only the resolver's own aux heap over that walk, never
/// any network/ClickHouse machinery (none exists here).
#[doc(hidden)]
pub struct MultiMetricScanProbe {
    regex_cache: RegexCache,
    metrics: CacheMetrics,
    config: LabelCacheConfig,
    name_matchers: Vec<LabelMatcher>,
    matchers: Vec<LabelMatcher>,
    window: DataWindow,
    fanout_cap: u64,
}

impl MultiMetricScanProbe {
    #[doc(hidden)]
    pub fn new_reject_all_for_test() -> Self {
        MultiMetricScanProbe {
            regex_cache: RegexCache::new(REGEX_CACHE_CAPACITY),
            metrics: CacheMetrics::default(),
            config: LabelCacheConfig {
                db: "pulsus".to_string(),
                series_table: "metric_series".to_string(),
                bucket_ms: 3_600_000,
                window_ms: 24 * 3_600_000,
                cache_max_series: 50_000,
                ttl: Duration::from_secs(60),
                staleness_multiplier: DEFAULT_STALENESS_MULTIPLIER,
            },
            // Never matches any `m######` fixture name — every examined
            // name `continue`s, so the fingerprint loop never runs and no
            // `groups`/`matched` allocation happens on the success path.
            name_matchers: vec![LabelMatcher {
                key: "__name__".to_string(),
                op: MatchOp::Re,
                value: "zzz_no_such_metric_.*".to_string(),
            }],
            matchers: Vec::new(),
            window: DataWindow {
                start_ms: 0,
                end_ms: 1_000,
            },
            fanout_cap: 1_000,
        }
    }

    #[doc(hidden)]
    pub fn resolve_for_test(
        &self,
        snapshot: &CacheSnapshot,
        scan_budget: u64,
    ) -> MultiMetricResolution {
        resolve_multi_metric_over(
            snapshot,
            &self.regex_cache,
            &self.metrics,
            &self.config,
            &self.name_matchers,
            &self.matchers,
            self.window,
            self.fanout_cap,
            scan_budget,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Issue #133: guard-fires-at-the-accepted-max boundary proofs ---
    // Synthetic counts (the #96 `probe_fanout_bound` shape) — no
    // O(ceiling) resident cache is built; the extracted pure decisions
    // ARE the guard sites' comparisons.

    /// The examined-entry walk budget still trips at the maximum
    /// config-accepted `reader.promql_max_cache_scan` — the guard is not
    /// disable-able by any value config load accepts.
    #[test]
    fn scan_budget_still_trips_at_the_max_accepted_cache_scan() {
        let cap = pulsus_config::PROMQL_MAX_CACHE_SCAN_CEILING;
        assert!(scan_budget_exhausted(cap + 1, cap));
        assert!(!scan_budget_exhausted(cap, cap));
    }

    /// The matched-series cardinality guard still trips at the maximum
    /// config-accepted `reader.cache_max_series`.
    #[test]
    fn series_cap_still_trips_at_the_max_accepted_cache_max_series() {
        let cap = pulsus_config::CACHE_MAX_SERIES_CEILING;
        assert!(match_exceeds_series_cap(cap as usize + 1, cap));
        assert!(!match_exceeds_series_cap(cap as usize, cap));
    }

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

    // --- resolve_multi_metric_over (issue #85, M6-08c) ---

    fn name_re(pattern: &str) -> LabelMatcher {
        LabelMatcher {
            key: "__name__".to_string(),
            op: MatchOp::Re,
            value: pattern.to_string(),
        }
    }

    fn resolve_multi(
        snap: &CacheSnapshot,
        cfg: &LabelCacheConfig,
        name_matchers: &[LabelMatcher],
        matchers: &[LabelMatcher],
        w: DataWindow,
        fanout_cap: u64,
        scan_budget: u64,
    ) -> MultiMetricResolution {
        let regex_cache = RegexCache::new(REGEX_CACHE_CAPACITY);
        let metrics = CacheMetrics::default();
        resolve_multi_metric_over(
            snap,
            &regex_cache,
            &metrics,
            cfg,
            name_matchers,
            matchers,
            w,
            fanout_cap,
            scan_budget,
        )
    }

    #[test]
    fn multi_metric_matcher_only_groups_every_metric_with_matching_series() {
        let snap = snapshot(
            vec![
                ("bbb", 2, &[("job", "api")]),
                ("aaa", 1, &[("job", "api")]),
                ("ccc", 3, &[("job", "web")]),
            ],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "api".to_string(),
        };
        match resolve_multi(
            &snap,
            &config(),
            &[],
            &[m],
            window(0, 1_000),
            1_000,
            u64::MAX,
        ) {
            MultiMetricResolution::Groups(groups) => {
                // Sorted by name; `ccc` (no matching series) is absent.
                let names: Vec<&str> = groups.iter().map(|g| g.metric_name.as_str()).collect();
                assert_eq!(names, vec!["aaa", "bbb"]);
                assert_eq!(groups[0].series[0].0, 1);
                assert_eq!(groups[1].series[0].0, 2);
            }
            other => panic!("expected Groups, got {other:?}"),
        }
    }

    #[test]
    fn multi_metric_name_regex_prunes_the_key_set_first() {
        let snap = snapshot(
            vec![
                ("http_total", 1, &[]),
                ("http_errors", 2, &[]),
                ("grpc_total", 3, &[]),
            ],
            0,
            BASE_SWEEP_MS,
            1,
        );
        match resolve_multi(
            &snap,
            &config(),
            &[name_re("http_.*")],
            &[],
            window(0, 1_000),
            1_000,
            u64::MAX,
        ) {
            MultiMetricResolution::Groups(groups) => {
                let names: Vec<&str> = groups.iter().map(|g| g.metric_name.as_str()).collect();
                assert_eq!(names, vec!["http_errors", "http_total"]);
            }
            other => panic!("expected Groups, got {other:?}"),
        }
    }

    #[test]
    fn multi_metric_negative_name_matcher_excludes_the_named_metric() {
        let snap = snapshot(
            vec![("keep", 1, &[]), ("drop", 2, &[])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let neq = LabelMatcher {
            key: "__name__".to_string(),
            op: MatchOp::Neq,
            value: "drop".to_string(),
        };
        match resolve_multi(
            &snap,
            &config(),
            &[neq],
            &[],
            window(0, 1_000),
            1_000,
            u64::MAX,
        ) {
            MultiMetricResolution::Groups(groups) => {
                assert_eq!(groups.len(), 1);
                assert_eq!(groups[0].metric_name, "keep");
            }
            other => panic!("expected Groups, got {other:?}"),
        }
    }

    /// A snapshot with `n` one-series metrics (`m0000`..), for the cap
    /// boundary tests.
    fn many_metric_snapshot(n: usize) -> CacheSnapshot {
        let mut by_fingerprint = HashMap::new();
        let mut by_metric: HashMap<String, Vec<Fingerprint>> = HashMap::new();
        for i in 0..n {
            by_fingerprint.insert(i as Fingerprint, labels(&[]));
            by_metric.insert(format!("m{i:04}"), vec![i as Fingerprint]);
        }
        CacheSnapshot {
            by_fingerprint,
            by_metric,
            sweep_time_ms: BASE_SWEEP_MS,
            covered_from_ms: 0,
            generation: 1,
        }
    }

    /// The adjudicated boundary (issuecomment-4997289437/-4997456745):
    /// exactly 1000 matched metric names pass under the default cap;
    /// 1001 reject with the named fan-out outcome.
    #[test]
    fn multi_metric_fanout_cap_passes_at_1000_and_rejects_at_1001() {
        let cfg = config();
        match resolve_multi(
            &many_metric_snapshot(1_000),
            &cfg,
            &[],
            &[],
            window(0, 1_000),
            1_000,
            u64::MAX,
        ) {
            MultiMetricResolution::Groups(groups) => assert_eq!(groups.len(), 1_000),
            other => panic!("1000 matched metrics must pass at cap 1000, got {other:?}"),
        }
        match resolve_multi(
            &many_metric_snapshot(1_001),
            &cfg,
            &[],
            &[],
            window(0, 1_000),
            1_000,
            u64::MAX,
        ) {
            MultiMetricResolution::FanoutExceeded { matched, cap } => {
                assert_eq!(cap, 1_000);
                assert!(matched > 1_000, "reported matched is the breach point");
            }
            other => panic!("1001 matched metrics must reject at cap 1000, got {other:?}"),
        }
    }

    /// The cap is a config knob, not a constant: an override is honored
    /// in both directions.
    #[test]
    fn multi_metric_fanout_cap_override_is_honored() {
        let snap = many_metric_snapshot(3);
        assert!(matches!(
            resolve_multi(&snap, &config(), &[], &[], window(0, 1_000), 2, u64::MAX),
            MultiMetricResolution::FanoutExceeded { cap: 2, .. }
        ));
        assert!(matches!(
            resolve_multi(&snap, &config(), &[], &[], window(0, 1_000), 3, u64::MAX),
            MultiMetricResolution::Groups(_)
        ));
    }

    #[test]
    fn multi_metric_cold_stale_and_out_of_window_are_unresolvable_not_scans() {
        let cfg = config();
        // Cold.
        assert!(matches!(
            resolve_multi(
                &CacheSnapshot::default(),
                &cfg,
                &[],
                &[],
                window(0, 1_000),
                1_000,
                u64::MAX,
            ),
            MultiMetricResolution::Unresolvable {
                reason: FallbackReason::ColdCache
            }
        ));
        // Out of window.
        let snap = snapshot(vec![("up", 1, &[])], 10_000, BASE_SWEEP_MS, 1);
        assert!(matches!(
            resolve_multi(&snap, &cfg, &[], &[], window(0, 20_000), 1_000, u64::MAX),
            MultiMetricResolution::Unresolvable {
                reason: FallbackReason::OutOfWindow
            }
        ));
        // Stale.
        let mut stale_cfg = config();
        stale_cfg.ttl = Duration::from_millis(1);
        stale_cfg.staleness_multiplier = 1;
        let snap = snapshot(vec![("up", 1, &[])], 0, 0, 1);
        assert!(matches!(
            resolve_multi(
                &snap,
                &stale_cfg,
                &[],
                &[],
                window(0, 1_000),
                1_000,
                u64::MAX
            ),
            MultiMetricResolution::Unresolvable {
                reason: FallbackReason::StaleCache { .. }
            }
        ));
    }

    #[test]
    fn multi_metric_total_series_over_cache_max_series_is_unresolvable() {
        let mut cfg = config();
        cfg.cache_max_series = 1;
        let snap = snapshot(vec![("a", 1, &[]), ("b", 2, &[])], 0, BASE_SWEEP_MS, 1);
        assert!(matches!(
            resolve_multi(&snap, &cfg, &[], &[], window(0, 1_000), 1_000, u64::MAX),
            MultiMetricResolution::Unresolvable {
                reason: FallbackReason::OverCardinality { matched: 2, cap: 1 }
            }
        ));
    }

    #[test]
    fn multi_metric_uncompilable_name_regex_is_unresolvable() {
        let snap = snapshot(vec![("up", 1, &[])], 0, BASE_SWEEP_MS, 1);
        assert!(matches!(
            resolve_multi(
                &snap,
                &config(),
                &[name_re("(unclosed")],
                &[],
                window(0, 1_000),
                1_000,
                u64::MAX,
            ),
            MultiMetricResolution::Unresolvable {
                reason: FallbackReason::RegexUnsupported { .. }
            }
        ));
    }

    // --- scan-budget bound (issue #89, retroactive re-review) ---

    /// One metric name with `n` fingerprints, for the fingerprint-dimension
    /// scan-budget tests.
    fn many_fingerprint_snapshot(name: &str, n: usize) -> CacheSnapshot {
        let mut by_fingerprint = HashMap::new();
        let mut fps = Vec::with_capacity(n);
        for i in 0..n {
            by_fingerprint.insert(i as Fingerprint, labels(&[]));
            fps.push(i as Fingerprint);
        }
        let mut by_metric = HashMap::new();
        by_metric.insert(name.to_string(), fps);
        CacheSnapshot {
            by_fingerprint,
            by_metric,
            sweep_time_ms: BASE_SWEEP_MS,
            covered_from_ms: 0,
            generation: 1,
        }
    }

    /// v2 test (a): the fingerprint-dimension bound — a single name with
    /// `N` (much greater than the budget) candidate fingerprints, a label
    /// matcher matching none, proves the per-fingerprint counter bails the
    /// walk at `budget + 1` regardless of `N`.
    #[test]
    fn multi_metric_scan_budget_bounds_the_walk() {
        let snap = many_fingerprint_snapshot("m", 10_000);
        let never_matches = LabelMatcher {
            key: "nonexistent".to_string(),
            op: MatchOp::Eq,
            value: "x".to_string(),
        };
        match resolve_multi(
            &snap,
            &config(),
            &[],
            &[never_matches],
            window(0, 1_000),
            1_000,
            4,
        ) {
            MultiMetricResolution::ScanBudgetExceeded { examined, cap } => {
                assert_eq!(
                    examined, 5,
                    "examined must stop at budget + 1, not scale with N"
                );
                assert_eq!(cap, 4);
            }
            other => panic!("expected ScanBudgetExceeded, got {other:?}"),
        }
    }

    /// v2 test (b): a selective query well under the budget still resolves
    /// — the scan budget must never false-reject a query that finishes
    /// examining a small candidate set.
    #[test]
    fn multi_metric_selective_query_under_scan_budget_still_groups() {
        let snap = snapshot(
            vec![("aaa", 1, &[("job", "api")]), ("bbb", 2, &[("job", "api")])],
            0,
            BASE_SWEEP_MS,
            1,
        );
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: "api".to_string(),
        };
        match resolve_multi(&snap, &config(), &[], &[m], window(0, 1_000), 1_000, 10) {
            MultiMetricResolution::Groups(groups) => assert_eq!(groups.len(), 2),
            other => panic!("expected Groups under budget, got {other:?}"),
        }
    }

    /// Demoted secondary check (v2/v3 plan): the name-enumeration
    /// dimension bails at `budget + 1` regardless of the resident name
    /// universe's size — a reject-all name matcher costs exactly one
    /// budget unit per examined name, so the fingerprint loop never runs.
    /// **This test alone is vacuous against a regressed pre-loop
    /// `keys().collect() + sort` — a broken implementation that restores
    /// that O(N) step but keeps this loop counter reports the identical
    /// `examined == B + 1`.** The primary, non-vacuous proof of the fix is
    /// the bytes allocation gate in
    /// `crates/pulsus-read/tests/multi_metric_scan_alloc.rs` (round-2 plan
    /// review finding); this unit test is retained only as a cheap,
    /// hermetic secondary bound.
    #[test]
    fn multi_metric_scan_budget_is_scale_invariant_over_the_name_universe() {
        const B: u64 = 4;
        let nomatch = name_re("nomatch.*");
        for n in [2 * B as usize, 4 * B as usize] {
            match resolve_multi(
                &many_metric_snapshot(n),
                &config(),
                std::slice::from_ref(&nomatch),
                &[],
                window(0, 1_000),
                1_000,
                B,
            ) {
                MultiMetricResolution::ScanBudgetExceeded { examined, cap } => {
                    assert_eq!(examined, (B + 1) as usize, "universe size {n}");
                    assert_eq!(cap, B);
                }
                other => panic!("expected ScanBudgetExceeded for universe {n}, got {other:?}"),
            }
        }
    }

    // --- concrete_name_matches (issue #85: name_matchers over a
    // concrete-name selector) ---

    #[test]
    fn concrete_name_matches_evaluates_every_operator_anchored() {
        let name = "http_requests_total";
        for (m, want) in [
            (name_re("http_.*"), true),
            (name_re("http"), false), // anchored: no substring match
            (
                LabelMatcher {
                    key: "__name__".to_string(),
                    op: MatchOp::Nre,
                    value: "grpc_.*".to_string(),
                },
                true,
            ),
            (
                LabelMatcher {
                    key: "__name__".to_string(),
                    op: MatchOp::Eq,
                    value: name.to_string(),
                },
                true,
            ),
            (
                LabelMatcher {
                    key: "__name__".to_string(),
                    op: MatchOp::Neq,
                    value: name.to_string(),
                },
                false,
            ),
        ] {
            assert_eq!(
                concrete_name_matches(std::slice::from_ref(&m), name).unwrap(),
                want,
                "{m:?}"
            );
        }
        assert!(matches!(
            concrete_name_matches(&[name_re("(unclosed")], name),
            Err(FallbackReason::RegexUnsupported { .. })
        ));
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
