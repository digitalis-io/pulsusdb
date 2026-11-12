//! Issue #18 part B: reading the rollup levels.
//!
//! A range query is evaluated at the request's own times; its time range
//! chooses a resolution, the widest level a selector may read (issue #18).
//! A selector whose range and time fit a level at or below the resolution
//! reads that level's rows as pseudo-samples, then the finer levels and
//! raw over what the level does not cover, and the engine evaluates them
//! unchanged (design r13 sections 2 and 5).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use pulsus_clickhouse::{ChClient, QuerySettings, Row};
use pulsus_model::{Fingerprint, STALE_NAN_BITS, Tenant};
use pulsus_promql::{PlanParams, QueryPlan, RawOverlay, RollupUse, Sample, SelectorSpec};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use super::TenantSql;
use super::exec::MetricQueryParams;
use crate::logql::escape::ch_string;

/// One rollup level: its bucket width, the table a read names, and the
/// query range from which it is the resolution; a range below the first
/// level's reads raw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    pub res_ms: i64,
    pub table: String,
    pub from_ms: u64,
}

/// The levels the database holds, smallest first, the shard count a
/// level's coverage is intersected over, and the buckets of a level under
/// 720 s a `rate` or `increase` range covers at least.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Levels {
    pub levels: Vec<Level>,
    pub shards: u32,
    pub counter_buckets: u32,
}

/// Issue #836: how long after a bucket's end the level of width `res_ms`
/// normally has built it, at worst (`building.md` §3.3): `2p + 150 s` at the
/// first level, `p = max(R_1, 60 s)`, and `R_i + d_i` above it, with
/// `d_1 = ⌈150 s ÷ p⌉ × p` and `d_i = d_{i−1} + 60 s`.
pub fn build_age_ms(levels: &Levels, res_ms: i64) -> i64 {
    let Some(first) = levels.levels.first() else {
        return 0;
    };
    let p = first.res_ms.max(60_000);
    if res_ms <= first.res_ms {
        return 2 * p + 150_000;
    }
    let mut d = 150_000_i64.div_euclid(p) * p;
    if d < 150_000 {
        d += p;
    }
    for l in levels.levels.iter().skip(1) {
        d += 60_000;
        if l.res_ms >= res_ms {
            return l.res_ms + d;
        }
    }
    res_ms + d
}

/// Issue #836: a read's built point from its run rows, `(shard, newest
/// run-row end)`: the minimum over the `shards` shards of each one's newest
/// end; `lo` when a shard has none. Not capped at the window's end: a step
/// whose bucket a level has built is answered whatever its read's end.
pub fn built_point(runs: &[(u32, i64)], shards: u32, lo: i64) -> i64 {
    let mut newest: std::collections::BTreeMap<u32, i64> = std::collections::BTreeMap::new();
    for &(shard, t_end) in runs {
        let e = newest.entry(shard).or_insert(t_end);
        *e = (*e).max(t_end);
    }
    if (newest.len() as u32) < shards.max(1) {
        return lo;
    }
    newest.values().copied().min().unwrap_or(lo)
}

/// The process-wide level list, refreshed every 60 s. Empty until the
/// first successful discovery; nothing routes before it.
#[derive(Debug, Default)]
pub struct LevelSlot {
    current: RwLock<Option<Arc<Levels>>>,
    failures: AtomicU64,
    /// Issue #18 part 2c: the latest replica-lag read, `None` before the
    /// first.
    replicas: RwLock<Option<Vec<ReplicaDelay>>>,
    /// Issue #18 part 2c: every `(host, table)` any read has returned, its
    /// last delay, and whether the latest read returned it.
    replica_history: RwLock<std::collections::BTreeMap<(String, String), (u64, bool)>>,
}

/// Issue #18 part 2c: one replica's lag for one PulsusDB table, from
/// `system.replicas` on every replica of the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct ReplicaDelay {
    pub host: String,
    pub table: String,
    pub absolute_delay: u64,
}

/// Issue #18 part 2c: the replica-lag statement, every replica of
/// `cluster`, a replica it cannot reach left out.
pub fn replica_delay_sql(cluster: &str, db: &str) -> String {
    format!(
        "SELECT hostName() AS host, table, absolute_delay FROM clusterAllReplicas({}, system.replicas) WHERE database = {} ORDER BY host, table SETTINGS skip_unavailable_shards = 1",
        ch_string(cluster),
        ch_string(db)
    )
}

impl LevelSlot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self) -> Option<Arc<Levels>> {
        match self.current.read() {
            Ok(g) => g.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    pub fn set(&self, levels: Levels) {
        let levels = Some(Arc::new(levels));
        match self.current.write() {
            Ok(mut g) => *g = levels,
            Err(poisoned) => *poisoned.into_inner() = levels,
        }
    }

    /// Discoveries that failed since the process started.
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    /// Issue #18 part 2c: records the latest replica-lag read.
    pub fn set_replicas(&self, rows: Vec<ReplicaDelay>) {
        {
            let mut history = match self.replica_history.write() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            for seen in history.values_mut() {
                seen.1 = false;
            }
            for r in &rows {
                history.insert((r.host.clone(), r.table.clone()), (r.absolute_delay, true));
            }
        }
        match self.replicas.write() {
            Ok(mut g) => *g = Some(rows),
            Err(poisoned) => *poisoned.into_inner() = Some(rows),
        }
    }

    /// Issue #18 part 2c: the latest replica-lag read, `None` before the
    /// first.
    pub fn replicas(&self) -> Option<Vec<ReplicaDelay>> {
        match self.replicas.read() {
            Ok(g) => g.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Issue #18 part 2c: every `(host, table)` any read has returned, with
    /// its last delay and whether the latest read returned it.
    pub fn replica_gauges(&self) -> Vec<(String, String, u64, bool)> {
        let history = match self.replica_history.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        history
            .iter()
            .map(|((host, table), (delay, seen))| (host.clone(), table.clone(), *delay, *seen))
            .collect()
    }
}

/// The level-discovery statement: each level's width, switch point and
/// counter bucket count, from the comment the schema build wrote.
pub fn discovery_sql(db: &str) -> String {
    format!(
        "SELECT name, toUInt32(extract(comment, 'rollup_res_ms=(\\\\d+)')) AS res_ms,\n       \
         toUInt64(extract(comment, 'rollup_from_ms=(\\\\d+)')) AS from_ms,\n       \
         toUInt32(extract(comment, 'counter_buckets=(\\\\d+)')) AS counter_buckets\n\
         FROM system.tables\n\
         WHERE database = {} AND startsWith(comment, 'rollup_res_ms=') AND engine != 'Distributed'\n\
         ORDER BY res_ms",
        ch_string(db)
    )
}

/// The shard count of a cluster.
pub fn shards_sql(cluster: &str) -> String {
    format!(
        "SELECT toUInt32(uniqExact(shard_num)) AS shards FROM system.clusters WHERE cluster = {}",
        ch_string(cluster)
    )
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct LevelNameRow {
    name: String,
    res_ms: u32,
    from_ms: u64,
    counter_buckets: u32,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct ShardsRow {
    shards: u32,
}

async fn fetch<R: pulsus_clickhouse::ChRow>(
    client: &ChClient,
    sql: &str,
) -> Result<Vec<R>, pulsus_clickhouse::ChError> {
    use futures::StreamExt;
    let mut stream = client.query_stream::<R>(sql, &level_settings()).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?);
    }
    Ok(out)
}

/// What one level pass read (issue #803): the levels (with the shard
/// count), and, clustered, the replica-lag rows (`None` single-node, where
/// that statement is not sent). Saved by [`commit_levels`].
pub type LevelsOutcome = (
    Result<Levels, pulsus_clickhouse::ChError>,
    Option<Result<Vec<ReplicaDelay>, pulsus_clickhouse::ChError>>,
);

/// One attempt of the level pass (issue #803): discovery, then the shard
/// count only if discovery succeeded, then — clustered — the replica
/// statement whatever discovery did. Touches no slot.
pub async fn levels_attempt(
    client: &ChClient,
    db: &str,
    dist_suffix: &str,
    cluster: Option<&str>,
) -> LevelsOutcome {
    let levels = async {
        let names: Vec<LevelNameRow> = fetch(client, &discovery_sql(db)).await?;
        let shards = match cluster {
            Some(c) => fetch::<ShardsRow>(client, &shards_sql(c))
                .await?
                .first()
                .map_or(0, |r| r.shards),
            None => 1,
        };
        let suffix = if cluster.is_some() { dist_suffix } else { "" };
        let counter_buckets = names.first().map_or(0, |r| r.counter_buckets);
        Ok(Levels {
            levels: names
                .into_iter()
                .map(|r| Level {
                    res_ms: i64::from(r.res_ms),
                    table: format!("{}{suffix}", r.name),
                    from_ms: r.from_ms,
                })
                .collect(),
            shards,
            counter_buckets,
        })
    }
    .await;
    // Issue #18 part 2c: clustered, each replica's lag, read with the
    // levels.
    let replicas = match cluster {
        Some(c) => Some(fetch::<ReplicaDelay>(client, &replica_delay_sql(c, db)).await),
        None => None,
    };
    (levels, replicas)
}

/// Saves an accepted level pass into `slot` (issue #803): the replica rows
/// when they were read (a failed read keeps the last one), then the levels,
/// or one more failure and the last good list.
pub fn commit_levels(
    slot: &LevelSlot,
    (levels, replicas): LevelsOutcome,
) -> Result<(), pulsus_clickhouse::ChError> {
    match replicas {
        Some(Ok(rows)) => slot.set_replicas(rows),
        Some(Err(err)) => {
            tracing::warn!(error = %err, "replica delay read failed; keeping the last read")
        }
        None => {}
    }
    match levels {
        Ok(levels) => {
            slot.set(levels);
            Ok(())
        }
        Err(e) => {
            slot.failures.fetch_add(1, Ordering::Relaxed);
            Err(e)
        }
    }
}

/// One discovery: reads the levels and, clustered, the shard count and the
/// replica lag, and sets `slot`. On failure the slot keeps its last good
/// list.
pub async fn refresh_levels(
    slot: &LevelSlot,
    client: &ChClient,
    db: &str,
    dist_suffix: &str,
    cluster: Option<&str>,
) -> Result<(), pulsus_clickhouse::ChError> {
    // Issue #803: one read request, every statement on one server, run
    // once more on another when that server fails. Only the accepted
    // attempt is saved.
    let outcome = client
        .run_read_request(
            move |avoid| async move {
                client.rebind_avoiding(avoid);
                let read = levels_attempt(client, db, dist_suffix, cluster).await;
                pulsus_clickhouse::Attempt {
                    result: Ok::<_, pulsus_clickhouse::ChError>(read),
                    failure: client.server_failure(),
                }
            },
            pulsus_clickhouse::ChError::from,
        )
        .await;
    match outcome {
        Ok(read) => commit_levels(slot, read),
        Err(e) => {
            slot.failures.fetch_add(1, Ordering::Relaxed);
            Err(e)
        }
    }
}

/// How often the levels are read.
pub const LEVEL_REFRESH: std::time::Duration = std::time::Duration::from_secs(60);

/// The discovery loop: every 60 s, the first at once.
pub fn spawn_level_loop(
    slot: Arc<LevelSlot>,
    client: ChClient,
    db: String,
    dist_suffix: String,
    cluster: Option<String>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(LEVEL_REFRESH);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(err) =
                refresh_levels(&slot, &client, &db, &dist_suffix, cluster.as_deref()).await
            {
                tracing::warn!(
                    error = %err,
                    "rollup level discovery failed; keeping the last good level list"
                );
            }
        }
    })
}

/// The level a routed selector reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// The chosen level's index in [`Levels::levels`].
    pub level: usize,
    pub res_ms: i64,
    pub table: String,
    pub use_: RollupUse,
}

/// Issue #18: a range query's resolution, the largest level whose switch
/// point is at most `end − start`: the widest level any selector may
/// read. `None` for an instant query, no levels, or a range below the
/// first switch point.
pub fn resolution(p: &MetricQueryParams, levels: &Levels) -> Option<i64> {
    if p.end_ms < p.start_ms {
        return None;
    }
    resolution_for_span(p.end_ms - p.start_ms, p.step_ms, levels)
}

/// Issue #778: [`resolution`] for a range of `span_ms`, whatever the grid
/// it is evaluated on: a read cache tail is routed as its whole request is.
pub fn resolution_for_span(span_ms: i64, step_ms: i64, levels: &Levels) -> Option<i64> {
    if step_ms <= 0 || span_ms < 0 {
        return None;
    }
    let span = i128::from(span_ms);
    levels
        .levels
        .iter()
        .rev()
        .find(|l| l.res_ms > 0 && i128::from(l.from_ms) <= span)
        .map(|l| l.res_ms)
}

/// Issue #810 (D5): an instant query's resolution, the largest level whose
/// switch point is at most its smallest selector range (a selector with no
/// range counts 0); `None` when no level's is.
pub fn instant_resolution(plan: &QueryPlan, levels: &Levels) -> Option<i64> {
    let span = plan
        .selectors
        .iter()
        .map(|s| s.range_ms.unwrap_or(0))
        .min()?;
    let span = i128::from(span);
    levels
        .levels
        .iter()
        .rev()
        .find(|l| l.res_ms > 0 && i128::from(l.from_ms) <= span)
        .map(|l| l.res_ms)
}

/// D6: for a range query's selector of use `Counter` that is `eligible`
/// and has no level under `route`, the range it is read with, `r'`, and
/// the level it then routes to. `None` when the query is instant, the
/// selector has a level already, or no level of width at most `res` fits
/// its time.
///
/// Of the levels of width at most `res` whose time fits: the largest that
/// `⌈r / w⌉ × w` covers at least `min_buckets(w)` times (`counter_buckets`
/// under 720 s, one from 720 s), read over that range; else `min_buckets`
/// buckets of the smallest.
pub fn round_counter_range(
    sel: &SelectorSpec,
    p: &PlanParams,
    levels: &Levels,
    res: i64,
) -> Option<i64> {
    if p.step_ms <= 0
        || sel.rollup_use != Some(RollupUse::Counter)
        || !eligible(sel)
        || route(sel, p, levels, res).is_some()
    {
        return None;
    }
    let r = i128::from(sel.range_ms?);
    let fit: Vec<i128> = levels
        .levels
        .iter()
        .filter(|l| l.res_ms > 0 && l.res_ms <= res && fits_time(sel, p, l.res_ms))
        .map(|l| i128::from(l.res_ms))
        .collect();
    let rounded = fit
        .iter()
        .rev()
        .map(|&w| (w, (r + w - 1).div_euclid(w)))
        .find(|&(w, n)| n >= i128::from(min_buckets(levels, RollupUse::Counter, w as i64)))
        .map(|(w, n)| n * w)
        .or_else(|| {
            fit.iter()
                .min()
                .map(|&w| i128::from(min_buckets(levels, RollupUse::Counter, w as i64)) * w)
        })?;
    i64::try_from(rounded).ok()
}

/// D4: the buckets of a level of width `w` a range of `use_` covers at least.
pub fn min_buckets(levels: &Levels, use_: RollupUse, w: i64) -> i64 {
    if use_ == RollupUse::Counter && w < START_POINT_FROM_MS {
        i64::from(levels.counter_buckets)
    } else {
        1
    }
}

/// D5: a lattice-path range query. Issue #810 (D3): every step is read at
/// a bucket end, up to `⌈t_last / L⌉ × L`; there is no tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lattice {
    /// `L`, the request's resolution.
    pub width: i64,
    /// `gu = ⌈t_last / L⌉ × L`; `None` when the request's own times are
    /// bucket ends.
    pub until_ms: Option<i64>,
}

/// D3: the range a selector is read with at the bucket ends; `None` without a range or a use.
pub fn lattice_range(sel: &SelectorSpec, levels: &Levels, width: i64) -> Option<i64> {
    let (r, use_) = (i128::from(sel.range_ms?), sel.rollup_use?);
    let w = i128::from(width);
    let n = ((r + w - 1) / w).max(i128::from(min_buckets(levels, use_, width)));
    i64::try_from(n * w).ok()
}

/// D2 and D5: the lattice of a range query, or `None` for `main`'s path.
pub fn lattice(plan: &QueryPlan, p: &MetricQueryParams, levels: &Levels) -> Option<Lattice> {
    lattice_with(plan, p, levels, resolution(p, levels)?)
}

/// Issue #778: [`lattice`] at a given resolution `width`.
pub fn lattice_with(
    plan: &QueryPlan,
    p: &MetricQueryParams,
    levels: &Levels,
    width: i64,
) -> Option<Lattice> {
    if p.step_ms <= 0 {
        return None;
    }
    if plan.selectors.is_empty()
        || plan.selectors.iter().any(|s| s.at_ms.is_some())
        || pulsus_promql::reads_eval_time(&plan.root)
    {
        return None;
    }
    let (start, end, step, w) = (
        i128::from(p.start_ms),
        i128::from(p.end_ms),
        i128::from(p.step_ms),
        i128::from(width),
    );
    let t_last = start + (end - start).div_euclid(step) * step;
    let g0 = -(-start).div_euclid(w) * w;
    let gu = -(-t_last).div_euclid(w) * w;
    if g0 > gu {
        return None;
    }
    let on_grid = start.rem_euclid(w) == 0 && step.rem_euclid(w) == 0;
    let until_ms = if on_grid {
        None
    } else {
        Some(i64::try_from(gu).ok()?)
    };
    let epp = match until_ms {
        None => plan.params,
        Some(gu) => PlanParams {
            start_ms: i64::try_from(g0).ok()?,
            end_ms: gu,
            step_ms: width,
            ..plan.params
        },
    };
    let fits = plan.selectors.iter().all(|sel| {
        let mut s = sel.clone();
        if let Some(r) = lattice_range(sel, levels, width) {
            s.range_ms = Some(r);
        }
        eligible(&s) && route(&s, &epp, levels, width).is_some()
    });
    fits.then_some(Lattice { width, until_ms })
}

/// Plan section 3 and issue #18 follow-up R3: the largest level of width
/// at most `res` that the selector's time and range fit, or `None`.
pub fn route(sel: &SelectorSpec, p: &PlanParams, levels: &Levels, res: i64) -> Option<Route> {
    let use_ = sel.rollup_use?;
    if excluded(sel) {
        return None;
    }
    let (index, level) = levels.levels.iter().enumerate().rev().find(|(_, l)| {
        l.res_ms > 0
            && l.res_ms <= res
            && fits(sel, p, use_, l.res_ms, min_buckets(levels, use_, l.res_ms))
    })?;
    Some(Route {
        level: index,
        res_ms: level.res_ms,
        table: level.table.clone(),
        use_,
    })
}

/// Plan section 3's two conditions for one level (G2 and G1): the window
/// ends on the level's bucket ends, and the range is a whole number of
/// buckets, at least one, or `min_buckets` for a counter (`counter_buckets`
/// under 720 s).
fn fits(sel: &SelectorSpec, p: &PlanParams, use_: RollupUse, res: i64, buckets: i64) -> bool {
    let range = match (sel.range_ms, use_) {
        (None, _) => true,
        (Some(r), RollupUse::Counter) => r.rem_euclid(res) == 0 && r >= buckets * res,
        (Some(r), _) => r.rem_euclid(res) == 0 && r >= res,
    };
    fits_time(sel, p, res) && range
}

/// G2: every window of the selector ends on a bucket end of the level of
/// width `res`: the `@` time less the offset, or `start` less the offset
/// and the step, are whole numbers of buckets.
fn fits_time(sel: &SelectorSpec, p: &PlanParams, res: i64) -> bool {
    match sel.at_ms {
        Some(t) => (t - sel.offset_ms).rem_euclid(res) == 0,
        None => {
            (p.start_ms - sel.fetch.total_offset_ms).rem_euclid(res) == 0
                && (p.step_ms == 0 || p.step_ms.rem_euclid(res) == 0)
        }
    }
}

/// Plan Q5: what keeps a selector on raw whatever its step and range.
fn excluded(sel: &SelectorSpec) -> bool {
    sel.rollup_use.is_none()
        || sel.in_subquery
        || sel.info_family
        || sel.histogram_stats
        || sel.anchored
        || sel.smoothed
}

/// Plan Q1 and Q5: whether a selector may read a level at all, however
/// its series were found (issue #18 follow-up, R4).
pub fn eligible(sel: &SelectorSpec) -> bool {
    sel.metric_name.is_some() && sel.name_matchers.is_empty() && !excluded(sel)
}

/// Issue #810 (D11): the tables one routed statement reads.
#[derive(Debug, Clone, Copy)]
pub struct RoutedTables<'a> {
    /// The route's level table.
    pub level: &'a str,
    pub samples: &'a str,
    pub hist: &'a str,
    pub labels: &'a str,
    /// Issue #836: the expression naming the shard a level row was read
    /// from, `_shard_num` over a distributed table, else `1`.
    pub shard: &'a str,
}

/// Issue #810 (D11): the windows one routed statement reads.
#[derive(Debug, Clone, Copy)]
pub struct RoutedWindow {
    /// `(lo, hi]`, the selector's fetch window, `hi` at `t_last` less the
    /// offset (D3).
    pub level: (i64, i64),
    /// `(rlo, rhi]`, the selector's window in the routing-off plan.
    pub raw: (i64, i64),
}

/// Issue #810 (D9, addendum r6): what a routed statement reads.
#[derive(Debug, Clone, Copy)]
pub enum RoutedFetch<'a> {
    /// The level arm, and the raw arms gated on `hist_n`: the statement's
    /// own series' native-histogram samples, named as it reads them, and
    /// `others`', each another routed selector's listed condition. The
    /// `others` are dropped when the statement would pass the text cap.
    Gated { others: &'a [&'a str] },
    /// The raw arms alone, ungated: a selector read again raw.
    Raw,
}

/// Issue #810 (addendum r6): one routed selector's native-histogram
/// samples over its raw window, its series named by `series` (a literal
/// list, or an ID statement on lines of its own).
pub fn hist_part(series: &str, (rlo, rhi): (i64, i64)) -> String {
    format!("(fingerprint IN ({series}) AND unix_milli > {rlo} AND unix_milli <= {rhi})")
}

/// Issue #810 (D11): `metric_hist_samples`' value columns as one tuple.
pub const HIST_TUPLE: &str = "Tuple(Int8, Float64, UInt64, UInt64, Float64, Array(Int32), Array(UInt32), \
Array(Int64), Array(Int32), Array(UInt32), Array(Int64), Array(Float64), UInt8)";

/// Issue #810 (D11): one routed selector's one statement, and whether it
/// names its series by `ids` (with the labels arm) rather than `known`.
/// `known` is used when the statement it renders is under `text_cap`,
/// measured over its escaped text as the dispatcher measures it.
#[allow(clippy::too_many_arguments)]
pub fn routed_fetch(
    tenant: &Tenant,
    tables: &RoutedTables<'_>,
    name: &str,
    window: &RoutedWindow,
    fetch: RoutedFetch<'_>,
    known: Option<&str>,
    ids: &str,
    text_cap: u64,
) -> (String, bool) {
    let fits =
        |sql: &str| (crate::logql::exec::escape_query_placeholders(sql).len() as u64) < text_cap;
    let others: &[&str] = match fetch {
        RoutedFetch::Gated { others } => others,
        RoutedFetch::Raw => &[],
    };
    // The statement's own form first, by the list's fit; then the others'
    // lists, kept only while the statement fits.
    let mut forms: Vec<(Option<&str>, &[&str])> = Vec::with_capacity(4);
    if let Some(list) = known {
        if !others.is_empty() {
            forms.push((Some(list), others));
        }
        forms.push((Some(list), &[]));
    }
    if !others.is_empty() {
        forms.push((None, others));
    }
    let last = (None, &[][..]);
    for (list, with) in forms {
        let sql = routed_statement(tenant, tables, name, window, fetch, list, ids, with);
        if fits(&sql) {
            return (sql, list.is_none());
        }
    }
    (
        routed_statement(tenant, tables, name, window, fetch, last.0, ids, last.1),
        true,
    )
}

/// Plan section 2.3: the level arm, the raw arms (read only with
/// `hist_n > 0`), with no list the labels arm, and `hist_n`'s own row.
/// Read again raw (addendum r6): the raw arms alone, ungated, and with no
/// list the labels arm. Issue #836: the level arm's scan also returns the
/// level's run rows overlapping the window (ID `0`, with their shard), so
/// the replica that answers a shard's data rows answers its built point.
#[allow(clippy::too_many_arguments)]
fn routed_statement(
    tenant: &Tenant,
    tables: &RoutedTables<'_>,
    name: &str,
    window: &RoutedWindow,
    fetch: RoutedFetch<'_>,
    known: Option<&str>,
    ids: &str,
    others: &[&str],
) -> String {
    let org = tenant.sql_literal();
    let (lo, hi) = window.level;
    let (rlo, rhi) = window.raw;
    let hist = format!("Array({HIST_TUPLE})");
    let (level_filter, raw_filter) = match known {
        Some(list) => (
            format!("fingerprint IN ({list})"),
            format!("fingerprint IN ({list})"),
        ),
        None => (
            format!("fingerprint GLOBAL IN (\n{ids}\n)"),
            format!("fingerprint IN (\n{ids}\n)"),
        ),
    };
    let labels_arm = match known {
        Some(_) => String::new(),
        None => format!(
            "  UNION ALL
  SELECT toUInt8(2) AS arm, fingerprint, toInt64(0) AS t, toInt64(0) AS t_last, 0. AS last, 0. AS min,
         0. AS max, 0. AS sum, toUInt32(0) AS count, 0. AS inc, toUInt8(0) AS flags, CAST([], '{hist}') AS h, labels, toUInt32(0) AS shard
  FROM (
{}
  )
",
            super::sql::series_labels_by_fingerprint(
                tenant,
                tables.labels,
                &[name.to_string()],
                ids
            )
        ),
    };
    let raw_arms = |gate: &str| {
        format!(
            "  SELECT toUInt8(3) AS arm, fingerprint, unix_milli AS t, toInt64(0) AS t_last, value AS last, 0. AS min,
         0. AS max, 0. AS sum, toUInt32(0) AS count, 0. AS inc, toUInt8(0) AS flags, CAST([], '{hist}') AS h, '' AS labels, toUInt32(0) AS shard
  FROM {samples}
  WHERE org_id = {org} AND unix_milli > {rlo} AND unix_milli <= {rhi} AND {raw_filter}{gate}
  UNION ALL
  SELECT toUInt8(1) AS arm, hs.fingerprint AS fingerprint, hs.unix_milli AS t, toInt64(0) AS t_last,
         0. AS last, 0. AS min, 0. AS max, 0. AS sum, toUInt32(0) AS count, 0. AS inc, toUInt8(0) AS flags,
         [CAST((hs.schema, hs.zero_threshold, hs.zero_count, hs.count, hs.sum, hs.pos_span_offsets,
                hs.pos_span_lengths, hs.pos_bucket_deltas, hs.neg_span_offsets, hs.neg_span_lengths,
                hs.neg_bucket_deltas, hs.custom_values, hs.counter_reset_hint), '{HIST_TUPLE}')] AS h, '' AS labels, toUInt32(0) AS shard
  FROM {hist_table} AS hs
  WHERE hs.org_id = {org} AND hs.unix_milli > {rlo} AND hs.unix_milli <= {rhi} AND {raw_filter}{gate}
",
            samples = tables.samples,
            hist_table = tables.hist,
        )
    };
    if let RoutedFetch::Raw = fetch {
        return format!(
            "SELECT arm, fingerprint, t, t_last, last, min, max, sum, count, inc, flags, h, labels, shard FROM (
{}{labels_arm})
ORDER BY arm, fingerprint, t, reinterpretAsUInt64(last)",
            raw_arms("")
        );
    }
    // The statement's own series as it names them, then the others'.
    let own = match known {
        Some(list) => hist_part(list, (rlo, rhi)),
        None => hist_part(&format!("\n{ids}\n"), (rlo, rhi)),
    };
    let hist_where = std::iter::once(own.as_str())
        .chain(others.iter().copied())
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "WITH ifNull((SELECT count() FROM {hist_table} WHERE org_id = {org} AND ({hist_where})), 0) AS hist_n
SELECT arm, fingerprint, t, t_last, last, min, max, sum, count, inc, flags, h, labels, shard FROM (
  SELECT toUInt8(0) AS arm, fingerprint, t_end AS t, t_last, last, min, max, sum, count, inc, flags,
         CAST([], '{hist}') AS h, '' AS labels, toUInt32({shard}) AS shard
  FROM {level}
  WHERE kind = 0 AND org_id = {org} AND t_end > {lo} AND t_end <= {hi} AND {level_filter} AND hist_n = 0
     OR kind = 1 AND org_id = '' AND fingerprint = 0 AND t_end > {lo} AND t_last < {hi} AND hist_n = 0
  UNION ALL
{}{labels_arm}  UNION ALL
  SELECT toUInt8(4) AS arm, toUInt128(0) AS fingerprint, toInt64(hist_n) AS t, toInt64(0) AS t_last, 0. AS last,
         0. AS min, 0. AS max, 0. AS sum, toUInt32(0) AS count, 0. AS inc, toUInt8(0) AS flags, CAST([], '{hist}') AS h, '' AS labels,
         toUInt32(0) AS shard
)
ORDER BY arm, fingerprint, t, reinterpretAsUInt64(last)",
        raw_arms(" AND hist_n > 0"),
        hist_table = tables.hist,
        level = tables.level,
        shard = tables.shard,
    )
}

/// One histogram sample of a routed statement's arm 1, in
/// `metric_hist_samples`' column order: [`HIST_TUPLE`], read as a tuple.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HistTuple {
    pub schema: i8,
    pub zero_threshold: f64,
    pub zero_count: u64,
    pub count: u64,
    pub sum: f64,
    pub pos_span_offsets: Vec<i32>,
    pub pos_span_lengths: Vec<u32>,
    pub pos_bucket_deltas: Vec<i64>,
    pub neg_span_offsets: Vec<i32>,
    pub neg_span_lengths: Vec<u32>,
    pub neg_bucket_deltas: Vec<i64>,
    pub custom_values: Vec<f64>,
    pub counter_reset_hint: u8,
}

impl<'de> Deserialize<'de> for HistTuple {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = HistTuple;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a histogram sample tuple of 13 elements")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> Result<HistTuple, A::Error> {
                let missing = |i| serde::de::Error::invalid_length(i, &self);
                Ok(HistTuple {
                    schema: a.next_element()?.ok_or_else(|| missing(0))?,
                    zero_threshold: a.next_element()?.ok_or_else(|| missing(1))?,
                    zero_count: a.next_element()?.ok_or_else(|| missing(2))?,
                    count: a.next_element()?.ok_or_else(|| missing(3))?,
                    sum: a.next_element()?.ok_or_else(|| missing(4))?,
                    pos_span_offsets: a.next_element()?.ok_or_else(|| missing(5))?,
                    pos_span_lengths: a.next_element()?.ok_or_else(|| missing(6))?,
                    pos_bucket_deltas: a.next_element()?.ok_or_else(|| missing(7))?,
                    neg_span_offsets: a.next_element()?.ok_or_else(|| missing(8))?,
                    neg_span_lengths: a.next_element()?.ok_or_else(|| missing(9))?,
                    neg_bucket_deltas: a.next_element()?.ok_or_else(|| missing(10))?,
                    custom_values: a.next_element()?.ok_or_else(|| missing(11))?,
                    counter_reset_hint: a.next_element()?.ok_or_else(|| missing(12))?,
                })
            }
        }
        d.deserialize_tuple(13, V)
    }
}

/// Issue #810 (D11): one row of a routed statement.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct RoutedRow {
    pub arm: u8,
    pub fingerprint: Fingerprint,
    pub t: i64,
    pub t_last: i64,
    pub last: f64,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub count: u32,
    pub inc: f64,
    pub flags: u8,
    pub h: Vec<HistTuple>,
    pub labels: String,
    /// Issue #836: on a run row (`arm` 0, ID `0`), the shard it was read
    /// from.
    pub shard: u32,
}

/// One data row of a level, as a routed statement's level arm returns it.
#[derive(Debug, Clone, Copy, PartialEq, Row, Serialize, Deserialize)]
pub struct LevelRow {
    pub fingerprint: Fingerprint,
    pub t_end: i64,
    pub t_last: i64,
    pub last: f64,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub count: u32,
    pub inc: f64,
    /// Bit 1: the bucket's last sample is a stale marker; bit 2: the
    /// bucket holds a NaN that is not a stale marker.
    pub flags: u8,
}

/// Sorted, disjoint, left-open right-closed intervals `(a, b]`.
pub type Intervals = Vec<(i64, i64)>;

/// `a` and `b` both.
pub fn intersect(a: &[(i64, i64)], b: &[(i64, i64)]) -> Intervals {
    let merged = |x: &[(i64, i64)]| {
        let mut all: Vec<(i64, i64)> = x.iter().copied().filter(|(lo, hi)| lo < hi).collect();
        all.sort_unstable();
        let mut out: Intervals = Vec::new();
        for (lo, hi) in all {
            match out.last_mut() {
                Some(last) if lo <= last.1 => last.1 = last.1.max(hi),
                _ => out.push((lo, hi)),
            }
        }
        out
    };
    let b = merged(b);
    let mut out = Vec::new();
    for &(lo, hi) in &merged(a) {
        for &(c, d) in &b {
            let (x, y) = (lo.max(c), hi.min(d));
            if x < y {
                out.push((x, y));
            }
        }
    }
    merged(&out)
}

/// One element of a series' merged sequence (plan section 4 step 4): a
/// kept level row, tagged with its level's width, or a kept raw sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Piece {
    Level { res_ms: i64, row: LevelRow },
    Raw { t_ms: i64, v: f64 },
}

impl Piece {
    /// Where the piece sits in time: a level row at its `t_end`.
    pub fn at(&self) -> i64 {
        match self {
            Piece::Level { row, .. } => row.t_end,
            Piece::Raw { t_ms, .. } => *t_ms,
        }
    }
}

/// One element of a series' merged sequence (plan section 4 step 4): a
/// kept level row, tagged with its level's width, or a kept raw sample.
#[derive(Debug, Clone, PartialEq)]
pub enum Tagged {
    Level { res_ms: i64, row: LevelRow },
    Raw(Sample),
}

impl Tagged {
    /// Where the element sits in time: a level row at its `t_end`.
    pub fn at(&self) -> i64 {
        match self {
            Tagged::Level { row, .. } => row.t_end,
            Tagged::Raw(s) => s.t_ms,
        }
    }
}

/// The levels from which a counter row also gives a start point (plan
/// Q9), and under which a counter's raw samples go to the overlay (Q12).
const START_POINT_FROM_MS: i64 = 720_000;

/// Whether a route of `use_` at `res_ms` reads its raw samples through
/// the overlay (plan Q12).
pub fn overlaid(use_: RollupUse, res_ms: i64) -> bool {
    use_ == RollupUse::Counter && res_ms < START_POINT_FROM_MS
}

/// Plan sections 5 and Q12: a series' points from its time-ordered merged
/// sequence, and, for a counter routed to a level under 720 s, the raw
/// overlay: the uncovered intervals `pieces` and the `Raw` entries as
/// read (the caller merges the series' histogram samples into it).
pub fn assemble(
    use_: RollupUse,
    res_ms: i64,
    seq: &[Tagged],
    pieces: &Intervals,
) -> (Vec<Sample>, Option<RawOverlay>) {
    let mut out = Vec::with_capacity(seq.len() * 2);
    if overlaid(use_, res_ms) {
        let levels: Vec<Tagged> = seq
            .iter()
            .filter(|t| matches!(t, Tagged::Level { .. }))
            .cloned()
            .collect();
        counter(&levels, &mut out);
        let samples = seq
            .iter()
            .filter_map(|t| match t {
                Tagged::Raw(s) => Some(s.clone()),
                Tagged::Level { .. } => None,
            })
            .collect();
        return (
            out,
            Some(RawOverlay {
                pieces: pieces.clone(),
                samples,
            }),
        );
    }
    if use_ == RollupUse::Counter {
        counter(seq, &mut out);
        return (out, None);
    }
    for t in seq {
        match t {
            Tagged::Raw(s) => out.push(s.clone()),
            Tagged::Level { row, .. } => level_points(use_, row, &mut out),
        }
    }
    (out, None)
}

/// The bucket ended on a stale marker (plan r9 section 2).
pub const FLAG_STALE_END: u8 = 1;
/// The bucket holds a NaN that is not a stale marker.
pub const FLAG_NAN: u8 = 2;

/// Plan r9 section 3 and r10 section 1: the points one level row gives
/// for a use other than the counter, so a stale marker and a NaN read as
/// they do raw.
fn level_points(use_: RollupUse, row: &LevelRow, out: &mut Vec<Sample>) {
    let nan = row.flags & FLAG_NAN != 0;
    match use_ {
        RollupUse::Last => {
            if row.t_last > 0 {
                out.push(Sample::float(row.t_last, row.last));
            }
            if row.flags & FLAG_STALE_END != 0 {
                out.push(Sample::float(row.t_end, f64::from_bits(STALE_NAN_BITS)));
            }
        }
        RollupUse::Max | RollupUse::Min => {
            if row.count > 0 {
                let v = if use_ == RollupUse::Max {
                    row.max
                } else {
                    row.min
                };
                out.push(Sample::float(row.t_end, v));
            } else if nan {
                // Every sample that is not a stale marker is NaN: `last` is
                // the last of them, its payload kept.
                out.push(Sample::float(row.t_end, row.last));
            }
        }
        RollupUse::Sum | RollupUse::Avg => {
            if nan {
                out.push(Sample::float(row.t_end, f64::NAN));
            } else if row.count > 0 {
                let v = if use_ == RollupUse::Sum {
                    row.sum
                } else {
                    row.sum / f64::from(row.count)
                };
                out.push(Sample::float(row.t_end, v));
            }
        }
        RollupUse::Counter => {}
    }
}

/// Plan section 5's counter: one value `c` that never resets, folded over
/// the merged sequence in time order. A level row adds its `inc`, which
/// counts the step from the last sample before its bucket; a raw sample
/// adds `v − p`, or `v` on a drop, `p` the previous raw value or the last
/// level row's `last`. The first element starts `c` at its own value. A
/// NaN value, stale marker or not, passes through as itself and moves
/// neither `c` nor `p`.
fn counter(seq: &[Tagged], out: &mut Vec<Sample>) {
    let mut c: Option<f64> = None;
    let mut p: Option<f64> = None;
    for t in seq {
        match t {
            Tagged::Level { row, .. } if row.t_last == 0 => {
                // No sample that is not a stale marker: no point; its
                // increase, 0, still counts (plan r9 section 3).
                if let Some(x) = c {
                    c = Some(x + row.inc);
                }
            }
            Tagged::Level { res_ms, row } => {
                let now = match c {
                    Some(c) => c + row.inc,
                    None if row.last.is_nan() => {
                        out.push(Sample::float(row.t_last, row.last));
                        continue;
                    }
                    None => row.last,
                };
                c = Some(now);
                if row.last.is_nan() {
                    out.push(Sample::float(row.t_last, row.last));
                    continue;
                }
                if *res_ms >= START_POINT_FROM_MS {
                    out.push(Sample::float(row.t_end - res_ms + 1, now - row.inc));
                }
                out.push(Sample::float(row.t_last, now));
                p = Some(row.last);
            }
            Tagged::Raw(s) => {
                let v = s.v;
                if s.h.is_some() || v.is_nan() {
                    out.push(s.clone());
                    continue;
                }
                let now = match (c, p) {
                    (Some(c), Some(p)) if v >= p => c + (v - p),
                    (Some(c), Some(_)) => c + v,
                    (Some(c), None) => c,
                    (None, _) => v,
                };
                c = Some(now);
                p = Some(v);
                out.push(Sample::float(s.t_ms, now));
            }
        }
    }
}

/// `86400000` → `1d`, `7200000` → `2h`, `720000` → `12m`, `60000` → `1m`,
/// `5000` → `5s`: the largest whole unit.
pub fn duration_text(ms: i64) -> String {
    for (unit, size) in [
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1_000),
    ] {
        if ms != 0 && ms % size == 0 {
            return format!("{}{unit}", ms / size);
        }
    }
    format!("{ms}ms")
}

/// A range as PromQL writes it, the largest units first and zero units
/// left out: `135000` → `2m15s`, `3615000` → `1h15s`, `1500` → `1s500ms`.
/// For the range asked in explain's `rollup_route` note.
pub fn promql_duration_text(ms: i64) -> String {
    if ms <= 0 {
        return format!("{ms}ms");
    }
    let mut left = ms;
    let mut out = String::new();
    for (unit, size) in [
        ("y", 31_536_000_000),
        ("w", 604_800_000),
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1_000),
        ("ms", 1),
    ] {
        if left >= size {
            out.push_str(&format!("{}{unit}", left / size));
            left %= size;
        }
    }
    out
}

/// The column a use reads, as explain names it.
pub fn column_of(use_: RollupUse) -> &'static str {
    match use_ {
        RollupUse::Last => "last",
        RollupUse::Max => "max",
        RollupUse::Min => "min",
        RollupUse::Sum => "sum",
        RollupUse::Avg => "sum / count",
        RollupUse::Counter => "last, inc",
    }
}

/// The settings of every statement the level discovery sends, its replica
/// delay read included: a `log_comment` naming the task.
pub(crate) fn level_settings() -> QuerySettings {
    QuerySettings::new().set("log_comment", "background:rollup-levels")
}

#[cfg(test)]
mod tests {
    /// U43 (issue #778, addendum 6): the level discovery's statements name
    /// their task, so none is taken for a request's.
    #[test]
    fn u43_level_discovery_statements_name_their_task() {
        assert_eq!(
            super::level_settings().get("log_comment"),
            Some("background:rollup-levels")
        );
    }

    use pulsus_promql::{DEFAULT_LOOKBACK_MS, parse, plan};

    use super::*;

    /// Issue #803: `commit_levels` does what `refresh_levels` did after its
    /// reads — the replica rows whenever they were read, the levels or one
    /// more failure.
    #[test]
    fn commit_levels_applies_what_the_pass_read() {
        let slot = LevelSlot::new();
        let lv = levels(&[5, 60]);
        let row = |host: &str, delay| ReplicaDelay {
            host: host.to_string(),
            table: "metric_samples".to_string(),
            absolute_delay: delay,
        };
        let rows = vec![row("h1", 1)];
        commit_levels(&slot, (Ok(lv.clone()), Some(Ok(rows.clone())))).expect("ok");
        assert_eq!(slot.get().as_deref(), Some(&lv));
        assert_eq!(slot.replicas(), Some(rows));

        let rows2 = vec![row("h2", 2)];
        let err = pulsus_clickhouse::ChError::Io("reset".to_string());
        assert!(commit_levels(&slot, (Err(err), Some(Ok(rows2.clone())))).is_err());
        assert_eq!(slot.get().as_deref(), Some(&lv), "the levels are kept");
        assert_eq!(slot.replicas(), Some(rows2), "the replica rows still move");
        assert_eq!(slot.failures(), 1);
    }

    /// A UTC midnight: on every level's grid.
    const BASE: i64 = 1_788_825_600_000;
    const MIN: i64 = 60_000;
    const HOUR: i64 = 3_600_000;

    fn levels(secs: &[i64]) -> Levels {
        Levels {
            levels: secs
                .iter()
                .map(|s| Level {
                    res_ms: s * 1000,
                    table: format!("metric_samples_{s}s"),
                    from_ms: 0,
                })
                .collect(),
            shards: 1,
            counter_buckets: 5,
        }
    }

    const DAY: i64 = 86_400_000;

    fn query(start: i64, end: i64, step: i64) -> MetricQueryParams {
        MetricQueryParams {
            start_ms: start,
            end_ms: end,
            step_ms: step,
        }
    }

    /// The default levels, 60 s first, each the resolution from its own
    /// switch point, and `buckets` the counter bucket count.
    fn defaults(buckets: u32) -> Levels {
        Levels {
            levels: pulsus_schema::DEFAULT_ROLLUP_LEVELS
                .iter()
                .zip(pulsus_schema::DEFAULT_ROLLUP_RANGES)
                .map(|(&s, &from)| Level {
                    res_ms: i64::from(s) * 1000,
                    table: format!("metric_samples_{s}s"),
                    from_ms: u64::from(from) * 1000,
                })
                .collect(),
            shards: 1,
            counter_buckets: buckets,
        }
    }

    /// **U4 (issue #18): a range query's resolution by its range.** The
    /// default levels, `start` one millisecond after a midnight, at 1 s:
    /// under 3 h none, then each level from its own switch point (3 h,
    /// 16 h, 7 days, 90 days).
    #[test]
    fn resolution_by_range() {
        let lv = defaults(5);
        assert_eq!(lv.levels.len(), 4, "{lv:?}");
        let start = BASE + 1;
        for (range, res) in [
            (0, None),
            (5 * MIN, None),
            (HOUR, None),
            (3 * HOUR - 1, None),
            (3 * HOUR, Some(60_000)),
            (16 * HOUR - 1, Some(60_000)),
            (16 * HOUR, Some(720_000)),
            (DAY, Some(720_000)),
            (7 * DAY, Some(7_200_000)),
            (90 * DAY, Some(86_400_000)),
        ] {
            assert_eq!(
                resolution(&query(start, start + range, 1_000), &lv),
                res,
                "range {range} ms"
            );
        }
        assert_eq!(
            resolution(&query(start, start + DAY, 0), &lv),
            None,
            "step 0"
        );
        assert_eq!(
            resolution(&query(start, start + DAY, 1_000), &levels(&[])),
            None,
            "no levels"
        );
    }

    /// The plan parameters of a range query over `range` from `start` at
    /// `step`, `end = start + range`.
    fn over(start: i64, range: i64, step: i64) -> (MetricQueryParams, PlanParams) {
        let p = query(start, start + range, step);
        (p, p.plan_params(true, DEFAULT_LOOKBACK_MS))
    }

    /// The level the query's one selector routes to on the request's own
    /// grid, at the resolution its range selects.
    fn routed(q: &str, start: i64, range: i64, step: i64, lv: &Levels) -> Option<i64> {
        let (p, pp) = over(start, range, step);
        let res = resolution(&p, lv).expect("a resolution");
        let sels = selectors(q, &pp);
        assert_eq!(sels.len(), 1, "{q}: one selector");
        route(&sels[0], &pp, lv, res).map(|r| r.res_ms)
    }

    /// **U5 (issue #18): the guards on the request's grid.** Each selector
    /// reads the largest level, at most the resolution, whose buckets end
    /// at its window edges.
    #[test]
    fn the_guards() {
        let lv = defaults(5);
        let day = |q: &str| routed(q, BASE + 6 * MIN, DAY, 2 * MIN, &lv);
        assert_eq!(day("x"), Some(60_000));
        assert_eq!(day("max_over_time(x[1h])"), Some(60_000));
        assert_eq!(day("rate(x[2m15s])"), None);
        assert_eq!(day("rate(x[5m])"), Some(60_000));
        assert_eq!(day("x offset 90s"), None);
        assert_eq!(routed("x", BASE, 7 * DAY, 10 * MIN, &lv), Some(60_000));
        assert_eq!(
            routed("max_over_time(x[2h])", BASE, 7 * DAY, 2 * HOUR, &lv),
            Some(7_200_000)
        );
        assert_eq!(routed("x", BASE, 30 * DAY, HOUR, &lv), Some(720_000));
        assert_eq!(
            routed("rate(x[1h])", BASE, 30 * DAY, HOUR, &lv),
            Some(720_000)
        );
        assert_eq!(
            routed("rate(x[12m])", BASE, DAY, 12 * MIN, &lv),
            Some(720_000),
            "one 720 s bucket answers a rate"
        );
        assert_eq!(
            routed("rate(x[4m])", BASE + 6 * MIN, 6 * HOUR, 2 * MIN, &lv),
            None,
            "under 720 s a rate needs `counter_buckets` buckets"
        );
    }

    /// The range `round_counter_range` reads the query's one selector
    /// with, on the request's grid at the resolution its range selects.
    fn rounded(q: &str, start: i64, range: i64, step: i64, lv: &Levels) -> Option<i64> {
        let (p, pp) = over(start, range, step);
        let res = resolution(&p, lv).unwrap_or(lv.levels[0].res_ms);
        let sels = selectors(q, &pp);
        assert_eq!(sels.len(), 1, "{q}: one selector");
        round_counter_range(&sels[0], &pp, lv, res)
    }

    /// **U6 (issue #18): `round_counter_range` pins the range.** D6's three
    /// cases: the largest fitting level the rounded range covers at least
    /// `counter_buckets` times; else that many buckets of the finest
    /// fitting level; else none.
    #[test]
    fn round_counter_range_pins_the_range() {
        let lv = defaults(5);
        let day = |q: &str| rounded(q, BASE + 6 * MIN, DAY, 2 * MIN, &lv);
        assert_eq!(day("rate(x[2m15s])"), Some(300_000));
        assert_eq!(day("increase(x[4m])"), Some(300_000), "case 2");
        assert_eq!(day("rate(x[4m30s])"), Some(300_000), "case 1: five buckets");
        assert_eq!(day("rate(x[5m])"), None, "an exact level");
        assert_eq!(day("rate(x[2m15s] offset 90s)"), None);
        assert_eq!(day("irate(x[2m15s])"), None);
        assert_eq!(day("max_over_time(x[2m15s])"), None);
        assert_eq!(
            rounded("rate(x[1m15s])", BASE + 6 * MIN, DAY, MIN, &lv),
            Some(300_000)
        );
        assert_eq!(
            rounded("rate(x[1m])", BASE, HOUR, 15_000, &lv),
            None,
            "no level fits the time"
        );
        let month = |q: &str| rounded(q, BASE, 30 * DAY, HOUR, &lv);
        assert_eq!(month("rate(x[1h0m15s])"), Some(4_320_000));
        assert_eq!(
            month("rate(x[2880001ms])"),
            Some(3_600_000),
            "720 s, five buckets"
        );
        assert_eq!(month("rate(x[61m])"), None, "exact at 60 s");
        assert_eq!(
            rounded("rate(x[2h0m15s])", BASE, 7 * DAY, 2 * HOUR, &lv),
            Some(14_400_000),
            "7200 s: one bucket is enough from 720 s, so ⌈2h0m15s / 2h⌉ = 2"
        );
        let instant = query(BASE, BASE, 0).plan_params(true, DEFAULT_LOOKBACK_MS);
        let sel = &selectors("rate(x[2m15s])", &instant)[0];
        assert_eq!(
            round_counter_range(sel, &instant, &lv, lv.levels[0].res_ms),
            None,
            "an instant query"
        );
        let at = |q: &str| rounded(q, BASE + 6 * MIN + 30_000, DAY, 2 * MIN, &lv);
        assert_eq!(
            at("rate(x[2m15s] @ 1788825960)"),
            Some(300_000),
            "the @ time is on a minute"
        );
        assert_eq!(at("rate(x[2m15s] @ 1788825990)"), None);
        let three = defaults(3);
        assert_eq!(
            rounded("rate(x[2m15s])", BASE + 6 * MIN, DAY, 2 * MIN, &three),
            Some(180_000),
            "three buckets, case 1"
        );
        assert_eq!(
            rounded("rate(x[1m15s])", BASE + 6 * MIN, DAY, MIN, &three),
            Some(180_000),
            "three buckets, case 2"
        );
    }

    /// **U9 (issue #18): the range at the bucket ends rounds up to whole
    /// buckets.** At least one, and `counter_buckets` for a `rate` under
    /// 720 s; an instant selector has none.
    #[test]
    fn lattice_range_rounds_up_to_whole_buckets() {
        let lv = defaults(5);
        let pp = query(BASE, BASE + DAY, 2 * MIN).plan_params(true, DEFAULT_LOOKBACK_MS);
        let at = |q: &str, width: i64| {
            let sels = selectors(q, &pp);
            assert_eq!(sels.len(), 1, "{q}: one selector");
            lattice_range(&sels[0], &lv, width)
        };
        for (q, width, want) in [
            ("rate(x[2m15s])", 720_000, Some(720_000)),
            ("rate(x[1h0m15s])", 720_000, Some(4_320_000)),
            ("rate(x[18m])", 720_000, Some(1_440_000)),
            ("max_over_time(x[12m])", 720_000, Some(720_000)),
            ("max_over_time(x[13m])", 720_000, Some(1_440_000)),
            ("x", 720_000, None),
            ("rate(x[1m15s])", 60_000, Some(300_000)),
            ("rate(x[5m15s])", 60_000, Some(360_000)),
            ("max_over_time(x[30s])", 60_000, Some(60_000)),
            ("rate(x[10m15s])", 7_200_000, Some(7_200_000)),
        ] {
            assert_eq!(at(q, width), want, "{q} at {width} ms");
        }
    }

    /// The lattice of `q` over `(start, start + span)` at `step`.
    fn lattice_of(q: &str, start: i64, span: i64, step: i64) -> Option<Lattice> {
        let lv = defaults(5);
        let p = query(start, start + span, step);
        let planned = plan(&parse(q).unwrap(), p.plan_params(true, DEFAULT_LOOKBACK_MS)).unwrap();
        lattice(&planned, &p, &lv)
    }

    /// **U10 (issue #18): the lattice of a request.** The width and the last
    /// bucket end the steps are read at, `⌈t_last / L⌉ × L` (issue #810, no
    /// tail); `None` where D2 keeps the request's own steps.
    #[test]
    fn the_lattice_of_a_request() {
        let s = BASE + 6 * MIN;
        let two_days = |q: &str| lattice_of(q, s, 2 * DAY, 2 * MIN);
        assert_eq!(
            two_days("rate(x[2m15s])"),
            Some(Lattice {
                width: 720_000,
                until_ms: Some(BASE + 2 * DAY + 12 * MIN),
            }),
            "(a)"
        );
        assert_eq!(
            lattice_of("rate(x[2m15s])", BASE, 2 * DAY, 2 * MIN),
            Some(Lattice {
                width: 720_000,
                until_ms: Some(BASE + 2 * DAY),
            }),
            "(b)"
        );
        assert_eq!(
            lattice_of("rate(x[2m15s])", BASE, 7 * DAY, 2 * HOUR),
            Some(Lattice {
                width: 7_200_000,
                until_ms: None,
            }),
            "(c)"
        );
        assert_eq!(lattice_of("rate(x[2m15s])", s, HOUR, 2 * MIN), None, "(d)");
        assert_eq!(two_days("x offset 90s"), None, "(e)");
        assert_eq!(two_days("irate(x[5m])"), None, "(f)");
        assert_eq!(two_days("rate(x[5m] @ 1788825960)"), None, "(g)");
        assert!(two_days("x offset 1m").is_some(), "(h)");
        assert_eq!(two_days("vector(1)"), None, "(i)");
        assert_eq!(
            lattice_of("rate(x[2m15s])", s, 2 * DAY, 3 * DAY),
            Some(Lattice {
                width: 720_000,
                until_ms: Some(BASE + 12 * MIN),
            }),
            "(j): one step, read at the bucket end that holds it"
        );
        assert_eq!(two_days("x * 0 + time()"), None, "(k)");
        assert_eq!(two_days("timestamp(x)"), None, "(l)");
        assert_eq!(two_days("hour()"), None, "(m)");
        assert!(two_days("hour(x)").is_some(), "(n)");
        assert_eq!(two_days("predict_linear(x[5m], 60)"), None, "(o)");
        assert_eq!(two_days("max_over_time(rate(x[5m])[1h:5m])"), None, "(p)");
    }

    /// **T4 (issue #810): the lattice has no tail.** The default levels
    /// (60 s from 3 h), `10:00:00` to `13:00:30` at 15 s: every step is read
    /// at its bucket end, the last at `⌈13:00:30 / 1 m⌉ × 1 m = 13:01:00`.
    #[test]
    fn lattice_has_no_tail() {
        let l = lattice_of("rate(x[5m])", BASE + 10 * HOUR, 3 * HOUR + 30_000, 15_000)
            .expect("a lattice");
        assert_eq!(l.width, 60_000, "the width");
        assert_eq!(l.until_ms, Some(BASE + 13 * HOUR + MIN), "until");
    }

    /// **T12 (issue #810): a long list reads by its IDs.** The cache's list
    /// of 10 series renders under the statement cap; at a cap of 200 bytes
    /// the statement names its series by the ID statement and reads their
    /// labels in its own arm.
    #[test]
    fn a_long_list_reads_by_its_ids() {
        let tenant = Tenant::from_header(None, false).expect("the empty tenant");
        let list = (1..=10)
            .map(|i| format!("toUInt128('{i}')"))
            .collect::<Vec<_>>()
            .join(", ");
        let tables = RoutedTables {
            level: "metric_samples_60s",
            samples: "metric_samples",
            hist: "metric_hist_samples",
            labels: "metric_labels",
            shard: "1",
        };
        let window = RoutedWindow {
            level: (1_000, 2_000),
            raw: (900, 2_000),
        };
        let ids = "SELECT fingerprint FROM metric_series";
        let gated = RoutedFetch::Gated { others: &[] };
        let (small, by_ids) =
            routed_fetch(&tenant, &tables, "m", &window, gated, Some(&list), ids, 200);
        assert!(by_ids, "at 200 bytes: by the IDs");
        assert!(small.contains("fingerprint GLOBAL IN ("), "{small}");
        assert!(small.contains("toUInt8(2) AS arm"), "{small}");
        let cap = crate::querytext::MAX_QUERY_TEXT_BYTES;
        let (big, by_ids) =
            routed_fetch(&tenant, &tables, "m", &window, gated, Some(&list), ids, cap);
        assert!(!by_ids, "at the cap: by the list");
        assert!(big.contains(&list), "{big}");
        assert!(!big.contains("toUInt8(2)"), "{big}");
    }

    fn all() -> Levels {
        levels(&[60, 720, 7200, 86400])
    }

    fn params(start: i64, step: i64, experimental: bool) -> PlanParams {
        PlanParams {
            start_ms: start,
            end_ms: start + 86_400_000,
            step_ms: step,
            lookback_ms: DEFAULT_LOOKBACK_MS,
            experimental_functions: experimental,
        }
    }

    fn selectors(query: &str, p: &PlanParams) -> Vec<SelectorSpec> {
        plan(&parse(query).unwrap(), *p).unwrap().selectors
    }

    /// The level each selector of `query` reads, in milliseconds.
    fn chosen(query: &str, p: &PlanParams, lv: &Levels) -> Vec<Option<i64>> {
        selectors(query, p)
            .iter()
            .map(|s| route(s, p, lv, i64::MAX).map(|r| r.res_ms))
            .collect()
    }

    /// The level the query's one selector reads.
    fn one(query: &str, start: i64, step: i64, lv: &Levels) -> Option<i64> {
        let got = chosen(query, &params(start, step, true), lv);
        assert_eq!(got.len(), 1, "{query}: one selector");
        got[0]
    }

    /// **B1 (issue #18 part B): the router chooses the largest level the
    /// step, start, offset and range fit, as plan section 3 and Q5 give.**
    #[test]
    fn b1_the_router_chooses_the_largest_level_that_fits() {
        let lv = all();
        let two = levels(&[720, 86400]);
        let none = levels(&[]);

        // The plan's named examples.
        assert_eq!(one("rate(m[1h])", BASE, 12 * MIN, &lv), Some(720_000));
        assert_eq!(one("rate(m[30m])", BASE, 12 * MIN, &lv), Some(60_000));
        assert_eq!(one("rate(m[30m])", BASE, 12 * MIN, &two), None);
        assert_eq!(
            one("max_over_time(m[1d])", BASE, 2 * HOUR, &lv),
            Some(7_200_000)
        );
        assert_eq!(one("max_over_time(m[5m])", BASE, 90_000, &lv), None);
        assert_eq!(one("irate(m[1h])", BASE, 12 * MIN, &lv), None);
        assert_eq!(one("rate(m[1h])", BASE, 12 * MIN, &none), None);

        // The counter functions: range on the level and at least five
        // times it under 720 s, once from 720 s (issue #18, D4); one
        // millisecond off the range or the step reads raw.
        for f in ["rate", "increase"] {
            assert_eq!(
                one(&format!("{f}(m[1h])"), BASE, 12 * MIN, &lv),
                Some(720_000)
            );
            assert_eq!(
                one(&format!("{f}(m[48m])"), BASE, 12 * MIN, &lv),
                Some(720_000),
                "{f}: 4 x res, one bucket being enough from 720 s"
            );
            assert_eq!(
                one(&format!("{f}(m[3600001ms])"), BASE, 12 * MIN, &lv),
                None,
                "{f}: range off"
            );
            assert_eq!(
                one(&format!("{f}(m[1h])"), BASE, 12 * MIN + 1, &lv),
                None,
                "{f}: step off"
            );
            assert_eq!(
                one(&format!("{f}(m[1h])"), BASE + 1, 12 * MIN, &lv),
                None,
                "{f}: start off"
            );
            assert_eq!(
                one(&format!("{f}(m[50h])"), BASE, 2 * HOUR, &lv),
                Some(7_200_000)
            );
            assert_eq!(
                one(&format!("{f}(m[5d])"), BASE, 24 * HOUR, &lv),
                Some(86_400_000)
            );
        }

        // Every other served function: range on the level and at least
        // once it.
        for f in [
            "max_over_time",
            "min_over_time",
            "sum_over_time",
            "avg_over_time",
            "last_over_time",
            "present_over_time",
            "absent_over_time",
        ] {
            assert_eq!(
                one(&format!("{f}(m[12m])"), BASE, 12 * MIN, &lv),
                Some(720_000),
                "{f}"
            );
            assert_eq!(
                one(&format!("{f}(m[719999ms])"), BASE, 12 * MIN, &lv),
                None,
                "{f}: res - 1"
            );
            assert_eq!(
                one(&format!("{f}(m[720001ms])"), BASE, 12 * MIN, &lv),
                None,
                "{f}: range off"
            );
            assert_eq!(
                one(&format!("{f}(m[12m])"), BASE, 12 * MIN + 1, &lv),
                None,
                "{f}: step off"
            );
            assert_eq!(
                one(&format!("{f}(m[1d])"), BASE, 24 * HOUR, &lv),
                Some(86_400_000),
                "{f}"
            );
        }

        // An instant selector and `timestamp`: the step and start only.
        for q in ["m", "timestamp(m)"] {
            assert_eq!(one(q, BASE, 12 * MIN, &lv), Some(720_000), "{q}");
            assert_eq!(one(q, BASE, 2 * HOUR, &lv), Some(7_200_000), "{q}");
            assert_eq!(one(q, BASE, MIN, &lv), Some(60_000), "{q}");
            assert_eq!(one(q, BASE, 12 * MIN + 1, &lv), None, "{q}: step off");
            assert_eq!(one(q, BASE + 1, 12 * MIN, &lv), None, "{q}: start off");
        }
        // An instant query needs only the start.
        let instant = PlanParams {
            start_ms: BASE + 12 * MIN,
            end_ms: BASE + 12 * MIN,
            step_ms: 0,
            lookback_ms: DEFAULT_LOOKBACK_MS,
            experimental_functions: false,
        };
        assert_eq!(
            chosen("max_over_time(m[12m])", &instant, &lv),
            vec![Some(720_000)]
        );

        // Every function kept on raw.
        for q in [
            "irate(m[1h])",
            "delta(m[1h])",
            "idelta(m[1h])",
            "resets(m[1h])",
            "changes(m[1h])",
            "count_over_time(m[1h])",
            "quantile_over_time(0.5, m[1h])",
            "stddev_over_time(m[1h])",
            "stdvar_over_time(m[1h])",
            "mad_over_time(m[1h])",
            "first_over_time(m[1h])",
            "ts_of_min_over_time(m[1h])",
            "ts_of_max_over_time(m[1h])",
            "ts_of_first_over_time(m[1h])",
            "ts_of_last_over_time(m[1h])",
            "deriv(m[1h])",
            "predict_linear(m[1h], 60)",
            "double_exponential_smoothing(m[1h], 0.5, 0.5)",
        ] {
            assert_eq!(one(q, BASE, 12 * MIN, &lv), None, "{q}");
        }

        // `@ t`: only `t - offset` must be on the level's grid, whatever
        // the request's start and step.
        let on = (BASE + 12 * MIN) / 1000;
        let off = format!("{}.001", (BASE + 12 * MIN) / 1000);
        assert_eq!(
            one(
                &format!("max_over_time(m[12m] @ {on})"),
                BASE + 1,
                12 * MIN,
                &lv
            ),
            Some(720_000),
            "@ on the grid, start off it"
        );
        assert_eq!(
            one(&format!("max_over_time(m[12m] @ {on})"), BASE, 90_000, &lv),
            Some(720_000),
            "@ on the grid, step off it"
        );
        assert_eq!(
            one(
                &format!("max_over_time(m[12m] @ {off})"),
                BASE,
                12 * MIN,
                &lv
            ),
            None,
            "@ off the grid, start on it"
        );
        assert_eq!(
            one(
                &format!("max_over_time(m[12m] @ {on} offset 1m)"),
                BASE,
                12 * MIN,
                &lv
            ),
            Some(60_000),
            "@ minus offset off the 720 s grid"
        );

        // `offset` moves the start off the grid.
        assert_eq!(
            one("max_over_time(m[12m] offset 1m)", BASE, 12 * MIN, &lv),
            Some(60_000)
        );
        assert_eq!(
            one("max_over_time(m[12m] offset 12m)", BASE, 12 * MIN, &lv),
            Some(720_000)
        );
        assert_eq!(
            one("max_over_time(m[12m] offset 1ms)", BASE, 12 * MIN, &lv),
            None
        );

        // Q5's exclusions.
        let p = params(BASE, 12 * MIN, true);
        assert_eq!(
            chosen("max_over_time(max_over_time(m[12m])[1h:12m])", &p, &lv),
            vec![None],
            "a selector in a subquery"
        );
        assert_eq!(
            chosen("rate(m[1h] anchored)", &p, &lv),
            vec![None],
            "anchored"
        );
        assert_eq!(
            chosen("rate(m[1h] smoothed)", &p, &lv),
            vec![None],
            "smoothed"
        );
        let info: Vec<(bool, Option<i64>)> = selectors("info(m)", &p)
            .iter()
            .map(|s| (s.info_family, route(s, &p, &lv, i64::MAX).map(|r| r.res_ms)))
            .collect();
        assert!(
            info.contains(&(true, None)) && info.contains(&(false, Some(720_000))),
            "info(): the info-family selector reads raw, its base the level: {info:?}"
        );

        // The route names the level's table and the consumer's column.
        let sel = &selectors("rate(m[1h])", &p)[0];
        assert_eq!(
            route(sel, &p, &lv, i64::MAX),
            Some(Route {
                level: 1,
                res_ms: 720_000,
                table: "metric_samples_720s".to_string(),
                use_: RollupUse::Counter,
            })
        );
    }

    fn row(t_end: i64, t_last: i64, last: f64, inc: f64, count: u32) -> LevelRow {
        LevelRow {
            fingerprint: Fingerprint::from_raw(7),
            t_end,
            t_last,
            last,
            min: last,
            max: last,
            sum: last,
            count,
            inc,
            flags: 0,
        }
    }

    fn points(s: &[Sample]) -> Vec<(i64, f64)> {
        s.iter().map(|x| (x.t_ms, x.v)).collect()
    }

    /// **B1c (issue #18 part B): `assemble`, plan sections 5 and Q12.** A
    /// counter that never resets, crossing the reset; at 720 s the start
    /// points and the raw samples continuing the counter, no overlay; at
    /// 60 s the level points alone and the raw samples, as read, in the
    /// overlay with the piece they cover. `count = 0` rows give no
    /// aggregate point.
    #[test]
    fn b1c_assemble() {
        let r = 720_000;
        let (e1, e2) = (BASE + r, BASE + 2 * r);
        let rows = [
            row(e1, e1 - 5_000, 900.0, 900.0, 48),
            row(e2, e2 - 5_000, 100.0, 200.0, 48),
        ];
        let seq = |res: i64| {
            vec![
                Tagged::Level {
                    res_ms: res,
                    row: rows[0],
                },
                Tagged::Level {
                    res_ms: res,
                    row: rows[1],
                },
                Tagged::Raw(Sample::float(e2 + 15_000, 130.0)),
                Tagged::Raw(Sample::float(e2 + 30_000, 20.0)),
            ]
        };
        let piece: Intervals = vec![(e2, e2 + 60_000)];
        let (pts, overlay) = assemble(RollupUse::Counter, r, &seq(r), &piece);
        assert_eq!(
            points(&pts),
            vec![
                (e1 - r + 1, 0.0),
                (e1 - 5_000, 900.0),
                (e2 - r + 1, 900.0),
                (e2 - 5_000, 1100.0),
                (e2 + 15_000, 1130.0),
                (e2 + 30_000, 1150.0),
            ]
        );
        assert_eq!(overlay, None, "no overlay at 720 s");

        let (pts, overlay) = assemble(RollupUse::Counter, 60_000, &seq(60_000), &piece);
        assert_eq!(
            points(&pts),
            vec![(e1 - 5_000, 900.0), (e2 - 5_000, 1100.0)],
            "the level points alone, no start point, below 720 s"
        );
        let overlay = overlay.expect("an overlay below 720 s");
        assert_eq!(overlay.pieces, piece);
        assert_eq!(
            points(&overlay.samples),
            vec![(e2 + 15_000, 130.0), (e2 + 30_000, 20.0)],
            "the raw samples as read"
        );

        // The aggregate columns at `t_end`, raw as read; `last` at `t_last`.
        let gauge = [
            row(e1, e1 - 5_000, 4.0, 0.0, 2),
            row(e2, e2 - 9_000, 6.0, 0.0, 0),
        ];
        let gseq = vec![
            Tagged::Level {
                res_ms: r,
                row: gauge[0],
            },
            Tagged::Level {
                res_ms: r,
                row: gauge[1],
            },
            Tagged::Raw(Sample::float(e2 + 1_000, 7.5)),
        ];
        let at = |u: RollupUse, res: i64| {
            let (pts, overlay) = assemble(u, res, &gseq, &piece);
            assert_eq!(overlay, None, "{u:?}: no overlay");
            points(&pts)
        };
        assert_eq!(
            at(RollupUse::Max, r),
            vec![(e1, 4.0), (e2 + 1_000, 7.5)],
            "a row with count = 0 gives no point"
        );
        assert_eq!(
            at(RollupUse::Max, 60_000),
            vec![(e1, 4.0), (e2 + 1_000, 7.5)]
        );
        assert_eq!(
            at(RollupUse::Avg, r),
            vec![(e1, 2.0), (e2 + 1_000, 7.5)],
            "sum / count"
        );
        assert_eq!(
            at(RollupUse::Last, r),
            vec![(e1 - 5_000, 4.0), (e2 - 9_000, 6.0), (e2 + 1_000, 7.5)]
        );
    }

    fn bits(s: &[Sample]) -> Vec<(i64, u64)> {
        s.iter().map(|x| (x.t_ms, x.v.to_bits())).collect()
    }

    /// **S3 (issue #18 part B, plan r9 section 3 and r10 section 1): each
    /// level row's flags and an empty `t_last` read as raw would.** A
    /// stale ending rebuilds a stale marker at the bucket's end; a bucket
    /// holding a NaN gives NaN for `sum_` and `avg_`, and an all-NaN bucket
    /// its last NaN, payload and all, for `max_` and `min_`; a row with no
    /// sample that is not a stale marker gives no counter point and still
    /// adds its increase.
    #[test]
    fn s3_flags_and_empty_rows() {
        use pulsus_model::STALE_NAN_BITS;
        let r = 720_000;
        let (e1, e2, e3) = (BASE + r, BASE + 2 * r, BASE + 3 * r);
        let payload = f64::from_bits(9_221_120_237_041_090_562);
        let flagged = |t_end: i64, t_last: i64, last: f64, count: u32, flags: u8| LevelRow {
            sum: if count > 0 { 12.0 } else { 0.0 },
            min: 3.0,
            max: 7.0,
            flags,
            ..row(t_end, t_last, last, 0.0, count)
        };
        let one = |use_: RollupUse, row: LevelRow| {
            let (pts, overlay) =
                assemble(use_, r, &[Tagged::Level { res_ms: r, row }], &Vec::new());
            assert_eq!(overlay, None);
            bits(&pts)
        };
        let stale = STALE_NAN_BITS;
        // `Last`: the last sample that is not a stale marker, then the
        // marker at the bucket's end.
        assert_eq!(
            one(RollupUse::Last, flagged(e1, e1 - 9_000, 5.0, 1, 1)),
            vec![(e1 - 9_000, 5.0f64.to_bits()), (e1, stale)],
            "a bucket ending stale"
        );
        assert_eq!(
            one(RollupUse::Last, flagged(e1, 0, 0.0, 0, 1)),
            vec![(e1, stale)],
            "a bucket of stale markers only"
        );
        assert_eq!(
            one(RollupUse::Last, flagged(e1, e1 - 9_000, 5.0, 1, 0)),
            vec![(e1 - 9_000, 5.0f64.to_bits())]
        );
        // `Max`, `Min`.
        for (u, v) in [(RollupUse::Max, 7.0f64), (RollupUse::Min, 3.0)] {
            assert_eq!(
                one(u, flagged(e1, e1 - 9_000, 7.0, 2, 2)),
                vec![(e1, v.to_bits())],
                "{u:?}: a bucket holding a NaN beside numbers"
            );
            assert_eq!(
                one(u, flagged(e1, e1 - 9_000, payload, 0, 2)),
                vec![(e1, payload.to_bits())],
                "{u:?}: an all-NaN bucket gives its last NaN"
            );
            assert_eq!(
                one(u, flagged(e1, 0, 0.0, 0, 1)),
                vec![],
                "{u:?}: stale only"
            );
        }
        // `Sum`, `Avg`.
        for (u, v) in [(RollupUse::Sum, 12.0f64), (RollupUse::Avg, 6.0)] {
            let nan = one(u, flagged(e1, e1 - 9_000, 7.0, 2, 2));
            assert_eq!(nan.len(), 1, "{u:?}");
            assert_eq!(nan[0].0, e1);
            assert!(
                f64::from_bits(nan[0].1).is_nan(),
                "{u:?}: NaN when the bucket holds a NaN"
            );
            assert_eq!(
                one(u, flagged(e1, e1 - 9_000, 7.0, 2, 0)),
                vec![(e1, v.to_bits())]
            );
            assert_eq!(
                one(u, flagged(e1, 0, 0.0, 0, 1)),
                vec![],
                "{u:?}: stale only"
            );
        }
        // `Counter`: the empty row gives no point and adds its increase.
        let seq = [
            Tagged::Level {
                res_ms: r,
                row: row(e1, e1 - 9_000, 100.0, 0.0, 48),
            },
            Tagged::Level {
                res_ms: r,
                row: LevelRow {
                    flags: 1,
                    ..row(e2, 0, 0.0, 0.0, 0)
                },
            },
            Tagged::Level {
                res_ms: r,
                row: row(e3, e3 - 9_000, 130.0, 30.0, 48),
            },
        ];
        let (pts, _) = assemble(RollupUse::Counter, r, &seq, &Vec::new());
        assert_eq!(
            points(&pts),
            vec![
                (e1 - r + 1, 100.0),
                (e1 - 9_000, 100.0),
                (e3 - r + 1, 100.0),
                (e3 - 9_000, 130.0),
            ]
        );
    }

    /// **U5 (issue #18 part 2c): the replica-delay statement skips an
    /// unreachable replica.**
    #[test]
    fn the_replica_delay_statement_skips_an_unreachable_replica() {
        assert_eq!(
            replica_delay_sql("pulsus_test_cluster", "pulsus"),
            "SELECT hostName() AS host, table, absolute_delay FROM clusterAllReplicas('pulsus_test_cluster', system.replicas) WHERE database = 'pulsus' ORDER BY host, table SETTINGS skip_unavailable_shards = 1"
        );
    }

    /// **B1f, unit half (issue #18 part B): `eligible` refuses every class
    /// plan Q1 keeps on raw**, each otherwise eligible; the plain selector
    /// is the control.
    #[test]
    fn b1f_eligible_refuses_the_unrouted_classes() {
        let p = params(BASE, 12 * MIN, false);
        let sel = |q: &str| selectors(q, &p).remove(0);
        assert!(eligible(&sel("rate(m[1h])")), "the control");
        assert!(!eligible(&sel(r#"rate({job="x"}[1h])"#)), "no metric name");
        // The parser refuses a bare name beside a `__name__` matcher, so
        // the name is the first `__name__="m"` and the matcher beside it
        // is the second.
        for q in [
            r#"rate({__name__="m",__name__=~"m.*"}[1h])"#,
            r#"rate({__name__="m",__name__!~"x"}[1h])"#,
            r#"rate({__name__="m",__name__!="x"}[1h])"#,
            r#"rate({__name__="m",__name__="m"}[1h])"#,
        ] {
            let s = sel(q);
            assert_eq!(s.metric_name.as_deref(), Some("m"), "{q}: a concrete name");
            assert_eq!(s.name_matchers.len(), 1, "{q}: one matcher beside it");
            assert!(!eligible(&s), "{q}");
        }
        // T3 (issue #18 follow-up): `eligible` reads the selector alone;
        // that a selector the label cache cannot answer is routed is held
        // where routing is decided, `exec::routed_series`
        // (`a_selector_the_label_cache_cannot_answer_is_routed`), and live
        // by `a_week_reads_two_hour_rows`.
        let stats = selectors("histogram_count(rate(h[5m]))", &p).remove(0);
        assert!(
            stats.histogram_stats,
            "the case is a histogram-stats selector"
        );
        assert!(!eligible(&stats), "histogram_stats");
    }

    /// The range asked, as explain's route note names it.
    #[test]
    fn promql_duration_text_is_the_written_form() {
        assert_eq!(promql_duration_text(135_000), "2m15s");
        assert_eq!(promql_duration_text(75_000), "1m15s");
        assert_eq!(promql_duration_text(3_615_000), "1h15s");
        assert_eq!(promql_duration_text(2_880_001), "48m1ms");
        assert_eq!(promql_duration_text(300_000), "5m");
        assert_eq!(promql_duration_text(8 * DAY), "1w1d");
    }

    /// The `runtimeinfo` form of a level.
    #[test]
    fn duration_text_is_the_largest_whole_unit() {
        assert_eq!(duration_text(86_400_000), "1d");
        assert_eq!(duration_text(7_200_000), "2h");
        assert_eq!(duration_text(720_000), "12m");
        assert_eq!(duration_text(60_000), "1m");
        assert_eq!(duration_text(5_000), "5s");
        assert_eq!(duration_text(300_000), "5m");
    }
}
