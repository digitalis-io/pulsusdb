//! Issue #18 part B: reading the rollup levels.
//!
//! A selector whose step, start, offset and range fit a rollup level reads
//! that level's rows as pseudo-samples, then the finer levels and raw for
//! what the level does not cover, and the engine evaluates them unchanged
//! (design r13 sections 2 and 5).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use pulsus_clickhouse::{ChClient, QuerySettings, Row};
use pulsus_model::{Fingerprint, FpLiteral, Tenant};
use pulsus_promql::{PlanParams, RollupUse, Sample, SelectorSpec};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use super::TenantSql;
use super::labels::LabelledResolution;
use super::sample_sql::render_fingerprint_list;
use crate::logql::escape::ch_string;

/// One rollup level: its bucket width and the table a read names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    pub res_ms: i64,
    pub table: String,
}

/// The levels the database holds, smallest first, and the shard count a
/// level's coverage is intersected over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Levels {
    pub levels: Vec<Level>,
    pub shards: u32,
}

/// The process-wide level list, refreshed every 60 s. Empty until the
/// first successful discovery; nothing routes before it.
#[derive(Debug, Default)]
pub struct LevelSlot {
    current: RwLock<Option<Arc<Levels>>>,
    failures: AtomicU64,
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
}

/// The level-discovery statement.
pub fn discovery_sql(db: &str) -> String {
    format!(
        "SELECT name, toUInt32(substring(comment, 15)) AS res_ms\n\
         FROM system.tables\n\
         WHERE database = {} AND startsWith(comment, 'rollup_res_ms=') AND engine != 'Distributed'\n\
         ORDER BY res_ms",
        ch_string(db)
    )
}

/// The shard count of a cluster.
pub fn shards_sql(cluster: &str) -> String {
    format!(
        "SELECT toUInt32(uniqExact(shard_num)) FROM system.clusters WHERE cluster = {}",
        ch_string(cluster)
    )
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct LevelNameRow {
    name: String,
    res_ms: u32,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct ShardsRow {
    #[serde(rename = "toUInt32(uniqExact(shard_num))")]
    shards: u32,
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

/// One discovery: reads the levels and, clustered, the shard count, and
/// sets `slot`. On failure the slot keeps its last good list.
pub async fn refresh_levels(
    slot: &LevelSlot,
    client: &ChClient,
    db: &str,
    dist_suffix: &str,
    cluster: Option<&str>,
) -> Result<(), pulsus_clickhouse::ChError> {
    let result = async {
        let names: Vec<LevelNameRow> = fetch(client, &discovery_sql(db)).await?;
        let shards = match cluster {
            Some(c) => fetch::<ShardsRow>(client, &shards_sql(c))
                .await?
                .first()
                .map_or(0, |r| r.shards),
            None => 1,
        };
        let suffix = if cluster.is_some() { dist_suffix } else { "" };
        Ok(Levels {
            levels: names
                .into_iter()
                .map(|r| Level {
                    res_ms: i64::from(r.res_ms),
                    table: format!("{}{suffix}", r.name),
                })
                .collect(),
            shards,
        })
    }
    .await;
    match result {
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

/// Plan section 3: the largest level the selector's time and range fit,
/// or `None`.
pub fn route(sel: &SelectorSpec, p: &PlanParams, levels: &Levels) -> Option<Route> {
    let use_ = sel.rollup_use?;
    if excluded(sel) {
        return None;
    }
    let (index, level) = levels
        .levels
        .iter()
        .enumerate()
        .rev()
        .find(|(_, l)| l.res_ms > 0 && fits(sel, p, use_, l.res_ms))?;
    Some(Route {
        level: index,
        res_ms: level.res_ms,
        table: level.table.clone(),
        use_,
    })
}

/// Plan section 3's two conditions for one level.
fn fits(sel: &SelectorSpec, p: &PlanParams, use_: RollupUse, res: i64) -> bool {
    let time = match sel.at_ms {
        Some(t) => (t - sel.offset_ms).rem_euclid(res) == 0,
        None => {
            (p.start_ms - sel.fetch.total_offset_ms).rem_euclid(res) == 0
                && (p.start_ms == p.end_ms || p.step_ms.rem_euclid(res) == 0)
        }
    };
    let range = match (sel.range_ms, use_) {
        (None, _) => true,
        (Some(r), RollupUse::Counter) => r.rem_euclid(res) == 0 && r >= 5 * res,
        (Some(r), _) => r.rem_euclid(res) == 0 && r >= res,
    };
    time && range
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

/// Plan Q1 and Q5: whether a selector may read a level at all.
pub fn eligible(sel: &SelectorSpec, resolution: &LabelledResolution) -> bool {
    sel.metric_name.is_some()
        && sel.name_matchers.is_empty()
        && matches!(resolution, LabelledResolution::Series(_))
        && !excluded(sel)
}

/// Plan section 2: one level fetch over `(lo, hi]` for one fingerprint
/// chunk, its data rows and the run rows that overlap the interval.
pub fn level_fetch(
    tenant: &Tenant,
    table: &str,
    fps: &[FpLiteral],
    lo: i64,
    hi: i64,
    clustered: bool,
) -> String {
    let shard = if clustered {
        "_shard_num"
    } else {
        "toUInt32(1)"
    };
    format!(
        "SELECT {shard} AS shard, kind, fingerprint, t_end, t_last, last, min, max, sum, count, inc\n\
         FROM {table}\n\
         WHERE (kind = 0 AND org_id = {} AND fingerprint IN ({}) AND t_end > {lo} AND t_end <= {hi})\n   \
         OR (kind = 1 AND org_id = '' AND fingerprint = 0 AND t_end > {lo} AND t_last < {hi})\n\
         ORDER BY kind, fingerprint, t_end",
        tenant.sql_literal(),
        render_fingerprint_list(fps),
    )
}

/// One row of a level fetch: a data row (`kind = 0`) or a run row
/// (`kind = 1`).
#[derive(Debug, Clone, Copy, PartialEq, Row, Serialize, Deserialize)]
pub struct LevelRow {
    pub shard: u32,
    pub kind: u8,
    pub fingerprint: Fingerprint,
    pub t_end: i64,
    pub t_last: i64,
    pub last: f64,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub count: u32,
    pub inc: f64,
}

/// One run row: the shard that answered it and its interval
/// `(t_last, t_end]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunRow {
    pub shard: u32,
    pub t_last: i64,
    pub t_end: i64,
}

/// Sorted, disjoint, left-open right-closed intervals `(a, b]`.
pub type Intervals = Vec<(i64, i64)>;

/// Plan Q7: per shard, the union of its run rows' intervals; the
/// intersection over shards `1..=shards`. A shard with no run row covers
/// nothing.
pub fn coverage(runs: &[RunRow], shards: u32) -> Intervals {
    let mut out: Option<Intervals> = None;
    for shard in 1..=shards {
        let mine = union(
            runs.iter()
                .filter(|r| r.shard == shard)
                .map(|r| (r.t_last, r.t_end)),
        );
        out = Some(match out {
            None => mine,
            Some(acc) => intersect(&acc, &mine),
        });
    }
    out.unwrap_or_default()
}

/// The union of `(a, b]` intervals, adjacent ones merged.
fn union(intervals: impl IntoIterator<Item = (i64, i64)>) -> Intervals {
    let mut all: Vec<(i64, i64)> = intervals.into_iter().filter(|(a, b)| a < b).collect();
    all.sort_unstable();
    let mut out: Intervals = Vec::new();
    for (a, b) in all {
        match out.last_mut() {
            Some(last) if a <= last.1 => last.1 = last.1.max(b),
            _ => out.push((a, b)),
        }
    }
    out
}

/// `a` without `b`.
pub fn subtract(a: &[(i64, i64)], b: &[(i64, i64)]) -> Intervals {
    let b = union(b.iter().copied());
    let mut out = Vec::new();
    for &(lo, hi) in &union(a.iter().copied()) {
        let mut at = lo;
        for &(c, d) in &b {
            if d <= at || c >= hi {
                continue;
            }
            if c > at {
                out.push((at, c));
            }
            at = at.max(d);
            if at >= hi {
                break;
            }
        }
        if at < hi {
            out.push((at, hi));
        }
    }
    out
}

/// `a` and `b` both.
pub fn intersect(a: &[(i64, i64)], b: &[(i64, i64)]) -> Intervals {
    let b = union(b.iter().copied());
    let mut out = Vec::new();
    for &(lo, hi) in &union(a.iter().copied()) {
        for &(c, d) in &b {
            let (x, y) = (lo.max(c), hi.min(d));
            if x < y {
                out.push((x, y));
            }
        }
    }
    union(out)
}

/// The smallest interval holding every interval of `a`.
pub fn hull(a: &[(i64, i64)]) -> Option<(i64, i64)> {
    let lo = a.iter().map(|x| x.0).min()?;
    let hi = a.iter().map(|x| x.1).max()?;
    Some((lo, hi))
}

/// Whether `t` lies in one of `a`'s intervals.
pub fn contains(a: &[(i64, i64)], t: i64) -> bool {
    let i = a.partition_point(|&(_, hi)| hi < t);
    a.get(i).is_some_and(|&(lo, hi)| lo < t && t <= hi)
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

/// Plan section 5: the pseudo-samples of one series' merged sequence.
pub fn pseudo_samples(use_: RollupUse, seq: &[Piece]) -> Vec<Sample> {
    let mut out = Vec::with_capacity(seq.len() * 2);
    if use_ == RollupUse::Counter {
        counter(seq, &mut out);
        return out;
    }
    for piece in seq {
        match *piece {
            Piece::Raw { t_ms, v } => out.push(Sample::float(t_ms, v)),
            Piece::Level { row, .. } => match use_ {
                RollupUse::Last => out.push(Sample::float(row.t_last, row.last)),
                _ if row.count == 0 => {}
                RollupUse::Max => out.push(Sample::float(row.t_end, row.max)),
                RollupUse::Min => out.push(Sample::float(row.t_end, row.min)),
                RollupUse::Sum => out.push(Sample::float(row.t_end, row.sum)),
                RollupUse::Avg => {
                    out.push(Sample::float(row.t_end, row.sum / f64::from(row.count)))
                }
                RollupUse::Counter => {}
            },
        }
    }
    out
}

/// The levels from which a counter row also gives a start point (plan
/// Q9).
const START_POINT_FROM_MS: i64 = 720_000;

/// Plan section 5's counter: one value `c` that never resets, folded over
/// the merged sequence in time order. A level row adds its `inc`, which
/// counts the step from the last sample before its bucket; a raw sample
/// adds `v − p`, or `v` on a drop, `p` the previous raw value or the last
/// level row's `last`. The first piece starts `c` at its own value. A NaN
/// value (a stale marker among them) passes through as itself and moves
/// neither `c` nor `p`.
fn counter(seq: &[Piece], out: &mut Vec<Sample>) {
    let mut c: Option<f64> = None;
    let mut p: Option<f64> = None;
    for piece in seq {
        match *piece {
            Piece::Level { res_ms, row } => {
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
                if res_ms >= START_POINT_FROM_MS {
                    out.push(Sample::float(row.t_end - res_ms + 1, now - row.inc));
                }
                out.push(Sample::float(row.t_last, now));
                p = Some(row.last);
            }
            Piece::Raw { t_ms, v } => {
                if v.is_nan() {
                    out.push(Sample::float(t_ms, v));
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
                out.push(Sample::float(t_ms, now));
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

/// The column a use reads, as explain names it.
pub fn column_of(use_: RollupUse) -> &'static str {
    match use_ {
        RollupUse::Last => "last",
        RollupUse::Max => "max",
        RollupUse::Min => "min",
        RollupUse::Sum => "sum",
        RollupUse::Avg => "sum/count",
        RollupUse::Counter => "inc",
    }
}

#[cfg(test)]
mod tests {
    use pulsus_promql::{DEFAULT_LOOKBACK_MS, parse, plan};

    use super::*;
    use crate::metrics::labels::FallbackReason;

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
                })
                .collect(),
            shards: 1,
        }
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
            .map(|s| route(s, p, lv).map(|r| r.res_ms))
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
        // times it; one millisecond off the range or the step reads raw.
        for f in ["rate", "increase"] {
            assert_eq!(
                one(&format!("{f}(m[1h])"), BASE, 12 * MIN, &lv),
                Some(720_000)
            );
            assert_eq!(
                one(&format!("{f}(m[48m])"), BASE, 12 * MIN, &lv),
                Some(60_000),
                "{f}: 4 x res"
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
            .map(|s| (s.info_family, route(s, &p, &lv).map(|r| r.res_ms)))
            .collect();
        assert!(
            info.contains(&(true, None)) && info.contains(&(false, Some(720_000))),
            "info(): the info-family selector reads raw, its base the level: {info:?}"
        );

        // The route names the level's table and the consumer's column.
        let sel = &selectors("rate(m[1h])", &p)[0];
        assert_eq!(
            route(sel, &p, &lv),
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
            shard: 1,
            kind: 0,
            fingerprint: Fingerprint::from_raw(7),
            t_end,
            t_last,
            last,
            min: last,
            max: last,
            sum: last,
            count,
            inc,
        }
    }

    fn points(s: &[Sample]) -> Vec<(i64, f64)> {
        s.iter().map(|x| (x.t_ms, x.v)).collect()
    }

    /// **B1c (issue #18 part B): the pseudo-samples of plan section 5.**
    /// A counter that never resets, crossing the reset and continuing into
    /// raw; the start point only from 720 s; `count = 0` rows give no
    /// aggregate point.
    #[test]
    fn b1c_the_pseudo_samples() {
        let r = 720_000;
        let (e1, e2) = (BASE + r, BASE + 2 * r);
        let rows = [
            row(e1, e1 - 5_000, 900.0, 900.0, 48),
            row(e2, e2 - 5_000, 100.0, 200.0, 48),
        ];
        let seq = |res: i64| {
            vec![
                Piece::Level {
                    res_ms: res,
                    row: rows[0],
                },
                Piece::Level {
                    res_ms: res,
                    row: rows[1],
                },
                Piece::Raw {
                    t_ms: e2 + 15_000,
                    v: 130.0,
                },
                Piece::Raw {
                    t_ms: e2 + 30_000,
                    v: 20.0,
                },
            ]
        };
        assert_eq!(
            points(&pseudo_samples(RollupUse::Counter, &seq(r))),
            vec![
                (e1 - r + 1, 0.0),
                (e1 - 5_000, 900.0),
                (e2 - r + 1, 900.0),
                (e2 - 5_000, 1100.0),
                (e2 + 15_000, 1130.0),
                (e2 + 30_000, 1150.0),
            ]
        );
        assert_eq!(
            points(&pseudo_samples(RollupUse::Counter, &seq(60_000))),
            vec![
                (e1 - 5_000, 900.0),
                (e2 - 5_000, 1100.0),
                (e2 + 15_000, 1130.0),
                (e2 + 30_000, 1150.0),
            ],
            "no start point below 720 s"
        );

        // The aggregate columns at `t_end`, raw as read; `last` at `t_last`.
        let gauge = [
            row(e1, e1 - 5_000, 4.0, 0.0, 2),
            row(e2, e2 - 9_000, 6.0, 0.0, 0),
        ];
        let gseq = vec![
            Piece::Level {
                res_ms: r,
                row: gauge[0],
            },
            Piece::Level {
                res_ms: r,
                row: gauge[1],
            },
            Piece::Raw {
                t_ms: e2 + 1_000,
                v: 7.5,
            },
        ];
        assert_eq!(
            points(&pseudo_samples(RollupUse::Max, &gseq)),
            vec![(e1, 4.0), (e2 + 1_000, 7.5)],
            "a row with count = 0 gives no point"
        );
        assert_eq!(
            points(&pseudo_samples(RollupUse::Avg, &gseq)),
            vec![(e1, 2.0), (e2 + 1_000, 7.5)],
            "sum / count"
        );
        assert_eq!(
            points(&pseudo_samples(RollupUse::Last, &gseq)),
            vec![(e1 - 5_000, 4.0), (e2 - 9_000, 6.0), (e2 + 1_000, 7.5)]
        );
    }

    fn run(shard: u32, t_last: i64, t_end: i64) -> RunRow {
        RunRow {
            shard,
            t_last,
            t_end,
        }
    }

    /// **B1d (issue #18 part B): coverage is each shard's union of run
    /// intervals, intersected over every shard of the cluster.**
    #[test]
    fn b1d_coverage_intersects_every_shard() {
        let runs = [
            run(1, 0, 100),
            run(1, 100, 200),
            run(2, 0, 50),
            run(2, 80, 200),
        ];
        assert_eq!(
            coverage(&runs, 2),
            vec![(0, 50), (80, 200)],
            "the gap of shard 2"
        );
        assert_eq!(
            coverage(&runs[..2], 1),
            vec![(0, 200)],
            "adjacent runs merge"
        );
        assert_eq!(
            coverage(&runs, 3),
            Vec::<(i64, i64)>::new(),
            "a shard with no run row covers nothing"
        );
        assert_eq!(coverage(&[], 1), Vec::<(i64, i64)>::new());
        assert_eq!(subtract(&[(0, 200)], &[(0, 50), (80, 200)]), vec![(50, 80)]);
        assert_eq!(
            subtract(&[(10, 300)], &[(0, 50), (80, 200)]),
            vec![(50, 80), (200, 300)]
        );
        assert_eq!(
            intersect(&[(10, 300)], &[(0, 50), (80, 200)]),
            vec![(10, 50), (80, 200)]
        );
        assert_eq!(hull(&[(10, 20), (30, 40)]), Some((10, 40)));
        assert_eq!(hull(&[]), None);
    }

    fn no_tenant() -> Tenant {
        Tenant::from_header(None, false).expect("the empty tenant")
    }

    /// **B1e (issue #18 part B): the level fetch is plan section 2's
    /// statement**, `toUInt32(1)` on one node and `_shard_num` clustered.
    #[test]
    fn b1e_the_level_fetch_statement() {
        let fps = [
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ];
        let body = "kind, fingerprint, t_end, t_last, last, min, max, sum, count, inc\n";
        let rest = "WHERE (kind = 0 AND org_id = '' AND fingerprint IN (toUInt128('101'), toUInt128('205')) AND t_end > 1000 AND t_end <= 9000)\n   OR (kind = 1 AND org_id = '' AND fingerprint = 0 AND t_end > 1000 AND t_last < 9000)\nORDER BY kind, fingerprint, t_end";
        assert_eq!(
            level_fetch(&no_tenant(), "metric_samples_720s", &fps, 1000, 9000, false),
            format!("SELECT toUInt32(1) AS shard, {body}FROM metric_samples_720s\n{rest}")
        );
        assert_eq!(
            level_fetch(
                &no_tenant(),
                "metric_samples_720s_dist",
                &fps,
                1000,
                9000,
                true
            ),
            format!("SELECT _shard_num AS shard, {body}FROM metric_samples_720s_dist\n{rest}")
        );
    }

    /// **B1f, unit half (issue #18 part B): `eligible` refuses every class
    /// plan Q1 keeps on raw**, each otherwise eligible; the plain selector
    /// is the control.
    #[test]
    fn b1f_eligible_refuses_the_unrouted_classes() {
        let p = params(BASE, 12 * MIN, false);
        let series = LabelledResolution::Series(Vec::new());
        let fallback = LabelledResolution::SqlFallback {
            sql: "SELECT 1".to_string(),
            reason: FallbackReason::ColdCache,
        };
        let sel = |q: &str| selectors(q, &p).remove(0);
        assert!(eligible(&sel("rate(m[1h])"), &series), "the control");
        assert!(
            !eligible(&sel(r#"rate({job="x"}[1h])"#), &series),
            "no metric name"
        );
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
            assert!(!eligible(&s, &series), "{q}");
        }
        assert!(!eligible(&sel("rate(m[1h])"), &fallback), "SqlFallback");
        let stats = selectors("histogram_count(rate(h[5m]))", &p).remove(0);
        assert!(
            stats.histogram_stats,
            "the case is a histogram-stats selector"
        );
        assert!(!eligible(&stats, &series), "histogram_stats");
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
