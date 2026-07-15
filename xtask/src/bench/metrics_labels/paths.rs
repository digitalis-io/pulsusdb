//! The three docs/schemas.md §2.1 strategy-ladder paths, benchmarked
//! against the product's own code where it exists (issue #34 architect
//! plan): path 1 drives the real
//! [`pulsus_read::metrics::LabelCache`]/[`SeriesResolver::resolve`], path 2
//! drives the real [`pulsus_read::metrics::sql::historical_series_subquery`]
//! (`JSONExtractString` fallback), and path 3 a **bench-local**
//! `metric_series_idx` prototype query (single-pass conditional
//! aggregation, the #11 logs-idx shape adapted metric-scoped — see
//! [`idx_resolve_sql`]'s doc comment) that is deliberately not wired into
//! the product catalog.
//!
//! **Cross-path correctness gate (plan amendment #3, test gap finding).**
//! [`run_all`] asserts all three paths return the **identical fingerprint
//! set** (not just count) for every selector × cardinality cell before
//! recording any perf evidence: a mismatch — including path 1 silently
//! degrading to `SqlFallback` — fails the whole bench run
//! (`anyhow::ensure!`), because it would invalidate the comparison anyway.
//! This module's own `tests` submodule carries the no-database complement:
//! a tiny hand-built corpus (including absent-label series, issue #34 CODE
//! review [medium] finding) cross-checked against a from-scratch reference
//! matcher evaluator and a from-scratch simulation of
//! [`idx_resolve_sql`]'s `uniqExactIf`/`countIf` semantics, for all four
//! [`SelectorKind`]s.
//!
//! **Frozen reference instant + activity guard band (issue #34 CODE review
//! [high] finding).** Every *bound* in this module (the resolver
//! `DataWindow`, `historical_series_subquery`'s and [`idx_resolve_sql`]'s
//! `WHERE` bounds, the hand-copied sweep SQL's bounds) derives from
//! `summary.end_ms`, the one `ref_ms` the caller (`metrics_labels::run`)
//! captured before generating the corpus — never re-derived from the wall
//! clock. This module does read the wall clock once, deliberately: the
//! guard-band drift assertion in [`run_all`] (this module's own
//! `now_unix_ms`) measures real elapsed time against `ref_ms` immediately
//! before path resolution begins, precisely *because* it needs to know how
//! much real time has passed — see `now_unix_ms`'s own doc comment for why
//! that is the one exception, and round-3 [low] finding #3 for
//! confirmation no other reads remain. The one thing `ref_ms` itself does
//! **not** control is [`LabelCache::refresh`]'s own internal sweep (product
//! code, its own wall-clock "now") — `super::corpus`'s activity guard band
//! (reserving the window-edge bucket, never assigned to any series) is what
//! neutralizes that remaining drift. See `corpus.rs`'s module doc comment
//! for the full reasoning.
//!
//! **Sweep SQL drift (architect plan edge case 6).** [`sweep_sql_copy`] is
//! hand-copied from `pulsus_read::metrics::refresh::sweep_sql` (private to
//! that crate, so it cannot be imported) — kept byte-identical to
//! docs/architecture.md §5.2 by comment discipline, not by sharing code;
//! `crates/pulsus-read/src/metrics/refresh.rs`'s own
//! `sweep_sql_renders_the_lower_bound_with_no_upper_bound` test pins the
//! product's copy, and this module's own
//! `sweep_sql_copy_matches_the_product_shape` test pins this one — if the
//! two ever diverge, both tests would need updating in lockstep, which is
//! the intended friction.

use std::time::Instant;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, QuerySettings, Row};
use pulsus_model::floor_to_activity_bucket;
use pulsus_read::logql::escape::{ch_regex_anchored, ch_string};
use pulsus_read::metrics::{
    DEFAULT_STALENESS_MULTIPLIER, DataWindow, LabelCache, LabelCacheConfig, LabelMatcher, MatchOp,
    Resolution, SeriesResolver, SeriesRow, sql as metrics_sql,
};

use super::corpus::{MetricsCorpusSummary, TierInfo};
use crate::bench::query_log::{flush_logs, flush_logs_before_shard_read, read_query_log};

/// The four selector shapes benchmarked across every cardinality tier
/// (architect plan amendment #1: `NegBroad`'s idx resolution needed a
/// distinct, conditional-aggregation SQL shape from the other three).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorKind {
    /// `pod="pod-0"` — a single matching series, whatever the tier's
    /// cardinality.
    NarrowEq,
    /// `job="j0"` — ~1/8 of the tier's series.
    BroadEq,
    /// `status=~"5.."` — ~1/2 of the tier's series (`500`/`503`).
    Regex5xx,
    /// `job!="j0"` — ~7/8 of the tier's series; the pure-negative case
    /// (empty positive set) for the idx path's conditional aggregation.
    NegBroad,
}

impl SelectorKind {
    pub const ALL: [SelectorKind; 4] = [
        SelectorKind::NarrowEq,
        SelectorKind::BroadEq,
        SelectorKind::Regex5xx,
        SelectorKind::NegBroad,
    ];

    fn matchers(self) -> Vec<LabelMatcher> {
        match self {
            SelectorKind::NarrowEq => vec![LabelMatcher {
                key: "pod".to_string(),
                op: MatchOp::Eq,
                value: "pod-0".to_string(),
            }],
            SelectorKind::BroadEq => vec![LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Eq,
                value: "j0".to_string(),
            }],
            SelectorKind::Regex5xx => vec![LabelMatcher {
                key: "status".to_string(),
                op: MatchOp::Re,
                value: "5..".to_string(),
            }],
            SelectorKind::NegBroad => vec![LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Neq,
                value: "j0".to_string(),
            }],
        }
    }

    /// Human-readable rendering, carried in [`PathEvidence::selector`].
    fn describe(self) -> String {
        match self {
            SelectorKind::NarrowEq => r#"pod="pod-0""#.to_string(),
            SelectorKind::BroadEq => r#"job="j0""#.to_string(),
            SelectorKind::Regex5xx => r#"status=~"5..""#.to_string(),
            SelectorKind::NegBroad => r#"job!="j0""#.to_string(),
        }
    }
}

/// One `(path, tier, selector)` cell's evidence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PathEvidence {
    /// `"cache"` | `"sql_fallback"` | `"idx_prototype"`.
    pub path: String,
    pub metric_name: String,
    pub cardinality: u64,
    pub selector: String,
    /// Resolved, **deduplicated** fingerprint count — the correctness
    /// cross-check number, not a raw row count (path 2's underlying scan
    /// returns one row per matching series per activity bucket; see
    /// [`fetch_fingerprints`]'s doc comment).
    pub matched_series: u64,
    pub wall_ms_p50: f64,
    pub wall_ms_p95: f64,
    pub wall_ms_p99: f64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub selected_marks: u64,
    pub total_marks: u64,
    pub memory_usage: u64,
    pub query_duration_ms: u64,
    /// `EXPLAIN indexes = 1` output — empty for the pure in-process cache
    /// path (no ClickHouse round trip to explain).
    pub explain_indexes: Vec<String>,
}

/// Path 1's refresh-sweep cost, one full + one incremental-prototype entry
/// per bucket size.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RefreshEvidence {
    pub bucket_ms: i64,
    /// `"full_sweep"` | `"incremental_sweep"`.
    pub kind: String,
    /// Full sweep: `CacheMetricsSnapshot.series_count` after
    /// `LabelCache::refresh()`. Incremental sweep (bench-local prototype,
    /// no resident cache of its own): the row count the narrower query
    /// itself returned — see [`run_refresh_evidence`]'s doc comment.
    pub resident_series: u64,
    /// Full sweep: `CacheMetricsSnapshot.oversize`. Incremental sweep:
    /// always `false` (not applicable — there is no resident cap to
    /// compare against).
    pub oversize: bool,
    pub wall_ms: f64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub selected_marks: u64,
    pub memory_usage: u64,
    pub query_duration_ms: u64,
}

/// Everything [`run_all`]/[`run_refresh_evidence`]/[`over_inclusion_read_rows`]
/// need, grouped into one parameter (clippy's argument-count lint, same
/// rationale as `queries.rs::RunConfig`).
pub struct PathsConfig<'a> {
    pub client: &'a ChClient,
    pub db: &'a str,
    pub cluster: &'a str,
    pub dist: bool,
    pub reps: usize,
    pub matcher_reps: usize,
    pub cache_max_series: u64,
}

fn table_name(base: &str, dist: bool) -> String {
    if dist {
        format!("{base}_dist")
    } else {
        base.to_string()
    }
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let idx = (((sorted_ms.len() - 1) as f64) * p).round() as usize;
    sorted_ms[idx.min(sorted_ms.len() - 1)]
}

/// Settings applied to every timed query: the clustered-reader block in
/// `--dist` mode, plain otherwise, plus the per-query `query_id` tag —
/// duplicated from `queries.rs::reader_settings` (not shared; see the
/// crate-level "duplicate rather than over-share" precedent for small,
/// module-local helpers — only `query_log.rs`'s evidence reader was judged
/// worth sharing, issue #34 task-manager resolution #1).
fn reader_settings(dist: bool, query_id: &str) -> QuerySettings {
    let base = if dist {
        QuerySettings::clustered_reader(false)
    } else {
        QuerySettings::new()
    };
    base.set("query_id", query_id)
}

/// Runs `sql` tagged `query_id`, returning the **sorted, deduplicated**
/// fingerprint set. Deduplication matters here specifically for path 2
/// (`historical_series_subquery`): that query has no `LIMIT 1 BY` (by
/// design — its product caller always inlines it as `fingerprint IN
/// (...)`, where SQL's own `IN` semantics already dedupe), so a plain
/// stream of it returns one row per matching series **per activity
/// bucket** in the queried window. `read_rows`/`read_bytes` (captured
/// separately via `system.query_log`) still reflect that full undeduped
/// scan — the real cost of the day-bucket over-inclusion this benchmark
/// measures — while `matched_series` here is the correctness-relevant
/// distinct count the cross-path gate compares.
/// Doubles every literal `?` in `sql` — the `clickhouse` crate's
/// `SqlBuilder` otherwise treats a bare `?` as an unbound bind placeholder,
/// which a rendered `match(val, '^(?:pattern)$')` regex predicate always
/// contains at least one of. Duplicated from
/// `pulsus_read::logql::exec::escape_query_placeholders` (`pub(crate)`
/// there, so it cannot be imported) — applied here at the same "once, at
/// the execution boundary" point that module's own doc comment specifies;
/// the SQL builders themselves (`historical_series_subquery`,
/// [`idx_resolve_sql`]) deliberately return un-doubled text.
fn escape_query_placeholders(sql: &str) -> String {
    sql.replace('?', "??")
}

async fn fetch_fingerprints(
    client: &ChClient,
    sql: &str,
    query_id: &str,
    dist: bool,
) -> anyhow::Result<Vec<u64>> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct FpRow {
        fingerprint: u64,
    }
    let settings = reader_settings(dist, query_id);
    let escaped = escape_query_placeholders(sql);
    let mut stream = client.query_stream::<FpRow>(&escaped, &settings).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?.fingerprint);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

async fn fetch_sweep_rows(
    client: &ChClient,
    sql: &str,
    query_id: &str,
    dist: bool,
) -> anyhow::Result<Vec<SeriesRow>> {
    let settings = reader_settings(dist, query_id);
    let escaped = escape_query_placeholders(sql);
    let mut stream = client
        .query_stream::<SeriesRow>(&escaped, &settings)
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?);
    }
    Ok(out)
}

async fn explain_lines(
    client: &ChClient,
    sql: &str,
    dist: bool,
    query_id: &str,
) -> anyhow::Result<Vec<String>> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ExplainRow {
        explain: String,
    }
    let full = format!("EXPLAIN indexes = 1 {sql}").replace('?', "??");
    let settings = reader_settings(dist, query_id);
    let mut stream = client.query_stream::<ExplainRow>(&full, &settings).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?.explain);
    }
    Ok(out)
}

/// Total marks the underlying table holds — the denominator a report can
/// use for a `selected_marks`/`total_marks` skip-index ratio, same
/// rationale as `queries.rs::total_marks` (duplicated, not shared — see
/// this module's doc comment).
async fn total_marks(
    client: &ChClient,
    db: &str,
    table: &str,
    cluster: Option<&str>,
) -> anyhow::Result<u64> {
    let base_table = table.trim_end_matches("_dist");
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct MarksRow {
        marks: u64,
    }
    let source = match cluster {
        Some(cluster) => format!("clusterAllReplicas('{cluster}', system.parts)"),
        None => "system.parts".to_string(),
    };
    let sql = format!(
        "SELECT sum(marks) AS marks FROM {source} WHERE database = '{db}' \
         AND table = '{base_table}' AND active"
    );
    let mut stream = client
        .query_stream::<MarksRow>(&sql, &QuerySettings::new())
        .await?;
    Ok(match stream.next().await {
        Some(row) => row?.marks,
        None => 0,
    })
}

/// Runs `sql` as a warmup pass (discarded) then `cfg.reps` timed, tagged
/// reps, capturing `system.query_log` + `EXPLAIN indexes = 1` from the
/// first timed rep (the corpus is static — a deterministic query's
/// server-side cost does not vary rep to rep, only wall time does).
/// Returns the evidence row plus the first rep's deduplicated fingerprint
/// set (for the cross-path gate).
async fn run_sql_path(
    cfg: &PathsConfig<'_>,
    path_name: &str,
    tier: &TierInfo,
    kind: SelectorKind,
    sql: &str,
    source_table: &str,
) -> anyhow::Result<(PathEvidence, Vec<u64>)> {
    let base_id = format!(
        "bench-metrics-{path_name}-{}-{:?}-{}",
        tier.metric_name,
        kind,
        std::process::id()
    );
    fetch_fingerprints(cfg.client, sql, &format!("{base_id}-warmup"), cfg.dist).await?;

    let mut wall_ms = Vec::with_capacity(cfg.reps);
    let mut first: Option<(Vec<u64>, String)> = None;
    for rep in 0..cfg.reps {
        let id = format!("{base_id}-r{rep}");
        let t0 = Instant::now();
        let fps = fetch_fingerprints(cfg.client, sql, &id, cfg.dist).await?;
        wall_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        if first.is_none() {
            first = Some((fps, id));
        }
    }
    wall_ms.sort_by(|a, b| a.partial_cmp(b).expect("wall-clock ms are finite"));
    let (fps, first_id) = first.expect("cfg.reps > 0");

    if cfg.dist {
        flush_logs_before_shard_read(cfg.client, cfg.cluster).await?;
    } else {
        flush_logs(cfg.client).await?;
    }
    let totals = read_query_log(cfg.client, &first_id).await?;
    let explain = explain_lines(cfg.client, sql, cfg.dist, &format!("{first_id}-explain")).await?;
    let total = total_marks(
        cfg.client,
        cfg.db,
        source_table,
        cfg.dist.then_some(cfg.cluster),
    )
    .await?;

    Ok((
        PathEvidence {
            path: path_name.to_string(),
            metric_name: tier.metric_name.clone(),
            cardinality: tier.cardinality,
            selector: kind.describe(),
            matched_series: fps.len() as u64,
            wall_ms_p50: percentile(&wall_ms, 0.50),
            wall_ms_p95: percentile(&wall_ms, 0.95),
            wall_ms_p99: percentile(&wall_ms, 0.99),
            read_rows: totals.read_rows,
            read_bytes: totals.read_bytes,
            selected_marks: totals.selected_marks,
            total_marks: total,
            memory_usage: totals.memory_usage,
            query_duration_ms: totals.query_duration_ms,
            explain_indexes: explain,
        },
        fps,
    ))
}

/// Path 1: `matcher_reps` timed, pure in-process `SeriesResolver::resolve`
/// calls (no ClickHouse round trip — `read_rows`/`read_bytes`/etc are
/// always `0`, `explain_indexes` always empty). The first call's result
/// **must** be `Resolution::Fingerprints` — a degrade to `SqlFallback`
/// here is a bench misconfiguration (architect plan amendment #3: "if it
/// degrades, that is a bench misconfiguration and must fail the same gate,
/// not silently drop the cache from the trio").
fn run_cache_path(
    cache: &LabelCache,
    matcher_reps: usize,
    tier: &TierInfo,
    kind: SelectorKind,
    matchers: &[LabelMatcher],
    window: DataWindow,
) -> anyhow::Result<(PathEvidence, Vec<u64>)> {
    let resolution = cache.resolve(&tier.metric_name, matchers, window);
    let fps = match resolution {
        Resolution::Fingerprints(fps) => fps,
        Resolution::SqlFallback { reason, .. } => anyhow::bail!(
            "path 1 (cache) degraded to SqlFallback for metric={} selector={kind:?}: {reason:?} \
             — bench misconfiguration (raise --cache-max-series or check the window/recency \
             config), not a valid three-path comparison cell",
            tier.metric_name
        ),
    };

    let mut wall_ms = Vec::with_capacity(matcher_reps);
    for _ in 0..matcher_reps {
        let t0 = Instant::now();
        let _ = cache.resolve(&tier.metric_name, matchers, window);
        wall_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    wall_ms.sort_by(|a, b| a.partial_cmp(b).expect("wall-clock ms are finite"));

    Ok((
        PathEvidence {
            path: "cache".to_string(),
            metric_name: tier.metric_name.clone(),
            cardinality: tier.cardinality,
            selector: kind.describe(),
            matched_series: fps.len() as u64,
            wall_ms_p50: percentile(&wall_ms, 0.50),
            wall_ms_p95: percentile(&wall_ms, 0.95),
            wall_ms_p99: percentile(&wall_ms, 0.99),
            read_rows: 0,
            read_bytes: 0,
            selected_marks: 0,
            total_marks: 0,
            memory_usage: 0,
            query_duration_ms: 0,
            explain_indexes: Vec::new(),
        },
        fps,
    ))
}

/// Path 3's resolution SQL: the #11 logs-idx conditional-aggregation shape
/// (`crates/pulsus-read/src/logql/sql.rs`'s `stage1`), adapted
/// metric-scoped (architect plan amendment #1). Positive matchers
/// (`Eq`/`Re`) collapse to one `uniqExactIf((key, val), ...) = n` term,
/// `n` counting **distinct positive keys** (the logs planner's own
/// collapse rule, `plan.rs:368`); negative matchers (`Neq`/`Nre`) are never
/// collapsed — `countIf(...) = 0` is correct whether one or several
/// negative branches target the same key.
///
/// **No `key IN (...)` prefilter (issue #34 CODE review [medium] finding).**
/// An earlier version filtered `WHERE ... AND key IN ({referenced keys})`
/// before `GROUP BY` — but that silently drops any fingerprint that lacks
/// the negated key's row *entirely*, even though Prometheus (and this
/// product's own absent-as-`""` matching contract, mirrored by
/// `pulsus_read::metrics::labels::matches` and `JSONExtractString`) treats
/// an absent label as matching `!=`/`!~`. Two shapes now, chosen by
/// whether the selector carries a positive matcher at all:
/// - **≥1 positive matcher:** the fingerprint is always reachable via its
///   *own* positive-branch row, so `WHERE` is an OR-list of both the
///   positive and negative branch predicates (never a `key IN` prefilter —
///   dropped entirely): `WHERE ... AND (pos_or [OR neg_or])`, `HAVING
///   uniqExactIf((key,val), pos_or) = n_pos [AND countIf(neg_or) = 0]`. A
///   fingerprint lacking the negated key simply contributes no row to
///   `neg_or`, so `countIf(neg_or) = 0` holds for it — absence semantics
///   fall out for free, no special-casing needed.
/// - **Pure-negative (`NegBroad`, no positive matcher at all):** **no key
///   filter whatsoever** — `WHERE metric_name = m AND bucket BETWEEN lo,hi`
///   scans every row for the metric-scoped window, `HAVING
///   countIf(neg_or) = 0`. Every fingerprint appears via at least one of
///   its own label rows (whichever keys it happens to carry), so an
///   absent negated key is correctly retained. This is the intended wide
///   scan, not an accident — its read cost is itself part of the recorded
///   evidence (the pure-negative case is inherently the most expensive
///   shape this path can render).
///
/// **Scope (issue #34 CODE review round-2 [adjudicated] finding #2,
/// round-3 [precision] finding #1).** This SQL is exercised, and its
/// correctness is only evidenced, against the four benchmarked
/// [`SelectorKind`]s (bounded positive equality, regex, single-negative) —
/// none of which is an *empty-accepting* matcher. Two **opposite** failure
/// modes verified against a label-less (absent-key) series: `job!=""`
/// (pure-negative form) wrongly **includes** it (`countIf` over zero rows
/// is trivially `0`, but Prometheus's absent-as-`""` semantics say
/// `"" != ""` is false, so it should be excluded); `job=~".*"`
/// (positive-branch form) wrongly **excludes** it (zero rows means it can
/// never reach `GROUP BY` at all, but `.*` matches `""`, so it should be
/// included). Both are **known open cases for the M3 idx design** if it
/// ships — not fixed here. See `idx.rs`'s module doc comment for the full
/// scope note; this is a deliberate non-generalization (the general
/// resolution semantics belong to the M3 ship design, not this evidence
/// run), not an oversight.
fn idx_resolve_sql(
    idx_table: &str,
    metric_name: &str,
    window: DataWindow,
    bucket_ms: i64,
    matchers: &[LabelMatcher],
) -> String {
    let lower = floor_to_activity_bucket(window.start_ms, bucket_ms);
    let upper = floor_to_activity_bucket(window.end_ms, bucket_ms);

    let mut positive_keys: Vec<String> = Vec::new();
    let mut pos_branches: Vec<String> = Vec::new();
    let mut neg_branches: Vec<String> = Vec::new();

    for m in matchers {
        let key_lit = ch_string(&m.key);
        match m.op {
            MatchOp::Eq => {
                pos_branches.push(format!("(key, val) = ({key_lit}, {})", ch_string(&m.value)));
                if !positive_keys.contains(&m.key) {
                    positive_keys.push(m.key.clone());
                }
            }
            MatchOp::Re => {
                pos_branches.push(format!(
                    "(key = {key_lit} AND match(val, {}))",
                    ch_regex_anchored(&m.value)
                ));
                if !positive_keys.contains(&m.key) {
                    positive_keys.push(m.key.clone());
                }
            }
            MatchOp::Neq => {
                neg_branches.push(format!("(key, val) = ({key_lit}, {})", ch_string(&m.value)));
            }
            MatchOp::Nre => {
                neg_branches.push(format!(
                    "(key = {key_lit} AND match(val, {}))",
                    ch_regex_anchored(&m.value)
                ));
            }
        }
    }

    let base_where = format!(
        "metric_name = {} AND bucket >= {lower} AND bucket <= {upper}",
        ch_string(metric_name)
    );

    if pos_branches.is_empty() {
        // Pure-negative: no key filter at all — see the doc comment above.
        format!(
            "SELECT fingerprint\nFROM {idx_table}\nWHERE {base_where}\nGROUP BY fingerprint\nHAVING countIf({}) = 0",
            neg_branches.join(" OR ")
        )
    } else {
        let where_or = if neg_branches.is_empty() {
            pos_branches.join(" OR ")
        } else {
            format!(
                "{} OR {}",
                pos_branches.join(" OR "),
                neg_branches.join(" OR ")
            )
        };
        let having = if neg_branches.is_empty() {
            format!(
                "uniqExactIf((key, val), {}) = {}",
                pos_branches.join(" OR "),
                positive_keys.len()
            )
        } else {
            format!(
                "uniqExactIf((key, val), {}) = {}\n   AND countIf({}) = 0",
                pos_branches.join(" OR "),
                positive_keys.len(),
                neg_branches.join(" OR ")
            )
        };
        format!(
            "SELECT fingerprint\nFROM {idx_table}\nWHERE {base_where}\n  AND ({where_or})\nGROUP BY fingerprint\nHAVING {having}"
        )
    }
}

/// Wall-clock now, milliseconds since the Unix epoch — used **only** by the
/// guard-band drift check in [`run_all`] (never by the SQL/idx/resolve
/// bounds themselves, which stay pinned to `summary.end_ms`'s frozen
/// `ref_ms`; see the module doc comment). Mirrors
/// `pulsus_read::metrics::refresh::now_unix_ms`'s non-panicking shape.
///
/// **Swept (issue #34 CODE review round-3 [low] finding #3):** this and
/// `metrics_labels::now_unix_ms` (the one-per-bucket-pass `ref_ms`
/// capture) are the *only* two wall-clock reads anywhere in this scenario
/// — `corpus.rs` takes `ref_ms` as a parameter and never reads the clock
/// itself, and [`super::over_inclusion_probe`]'s window derives from its
/// `end_ms` parameter (always `summary.end_ms`, i.e. `ref_ms`), never from
/// `now()`.
fn now_unix_ms() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

/// Hand-copied from `pulsus_read::metrics::refresh::sweep_sql` — see the
/// module doc comment's "Sweep SQL drift" note. `extra_predicate`, when
/// given, is ANDed in (used by [`run_refresh_evidence`]'s incremental
/// prototype).
fn sweep_sql_copy(
    series_table: &str,
    lower_bound_ms: i64,
    extra_predicate: Option<&str>,
) -> String {
    let mut sql = format!(
        "SELECT fingerprint, metric_name, labels\nFROM {series_table}\nWHERE unix_milli >= {lower_bound_ms}"
    );
    if let Some(extra) = extra_predicate {
        sql.push_str(&format!("\n  AND {extra}"));
    }
    sql.push_str("\nORDER BY unix_milli DESC\nLIMIT 1 BY metric_name, fingerprint");
    sql
}

/// Path 1's refresh-sweep cost: one real `LabelCache::refresh()` call
/// (`wall_ms` + resident stats from the real product code path), paired
/// with a `query_id`-tagged rerun of the byte-identical sweep SQL (so
/// `system.query_log` evidence can be captured — `LabelCache::refresh()`
/// itself does not expose a `query_id` hook), plus a bench-local
/// incremental-refresh **prototype**: the same sweep SQL narrowed to only
/// the most recent activity bucket (`unix_milli > {floor(now - bucket_ms)}`),
/// simulating "sweep only buckets newer than the last refresh" (§2.1's
/// planned evolution if the full sweep proves unsustainable) — this
/// prototype does not modify `LabelCache` itself; it measures the reduced
/// read cost only.
async fn run_refresh_evidence(
    cfg: &PathsConfig<'_>,
    cache: &LabelCache,
    series_table: &str,
    summary: &MetricsCorpusSummary,
) -> anyhow::Result<Vec<RefreshEvidence>> {
    let t0 = Instant::now();
    cache.refresh().await?;
    let full_wall_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let snap = cache.metrics();

    // Reuses the corpus's own frozen `ref_ms` (`summary.end_ms`) rather
    // than reading the wall clock again here — this hand-copied sweep
    // SQL's own bounds must stay anchored to the same reference instant
    // every other bound in this scenario uses (module doc comment's
    // "Frozen reference instant" note; issue #34 CODE review [high]
    // finding). The *real* `cache.refresh()` call above is the one
    // exception that cannot be pinned to `ref_ms` — see `corpus.rs`'s doc
    // comment for why the guard band exists.
    let now_ms = summary.end_ms;
    let full_lower = floor_to_activity_bucket(now_ms - summary.window_ms, summary.bucket_ms);
    let full_sql = sweep_sql_copy(series_table, full_lower, None);
    let full_id = format!("bench-metrics-refresh-full-{}", std::process::id());
    fetch_sweep_rows(cfg.client, &full_sql, &full_id, cfg.dist).await?;
    if cfg.dist {
        flush_logs_before_shard_read(cfg.client, cfg.cluster).await?;
    } else {
        flush_logs(cfg.client).await?;
    }
    let full_totals = read_query_log(cfg.client, &full_id).await?;

    let full_ev = RefreshEvidence {
        bucket_ms: summary.bucket_ms,
        kind: "full_sweep".to_string(),
        resident_series: snap.series_count,
        oversize: snap.oversize,
        wall_ms: full_wall_ms,
        read_rows: full_totals.read_rows,
        read_bytes: full_totals.read_bytes,
        selected_marks: full_totals.selected_marks,
        memory_usage: full_totals.memory_usage,
        query_duration_ms: full_totals.query_duration_ms,
    };

    let incr_lower = floor_to_activity_bucket(now_ms - summary.bucket_ms, summary.bucket_ms);
    let incr_sql = sweep_sql_copy(
        series_table,
        full_lower,
        Some(&format!("unix_milli > {incr_lower}")),
    );
    let incr_id = format!("bench-metrics-refresh-incremental-{}", std::process::id());
    let t1 = Instant::now();
    let incr_rows = fetch_sweep_rows(cfg.client, &incr_sql, &incr_id, cfg.dist).await?;
    let incr_wall_ms = t1.elapsed().as_secs_f64() * 1000.0;
    if cfg.dist {
        flush_logs_before_shard_read(cfg.client, cfg.cluster).await?;
    } else {
        flush_logs(cfg.client).await?;
    }
    let incr_totals = read_query_log(cfg.client, &incr_id).await?;

    let incr_ev = RefreshEvidence {
        bucket_ms: summary.bucket_ms,
        kind: "incremental_sweep".to_string(),
        // No resident cache of its own (bench-local, prototype-only) — the
        // row count this narrower query itself returned, not a merged
        // resident total.
        resident_series: incr_rows.len() as u64,
        oversize: false,
        wall_ms: incr_wall_ms,
        read_rows: incr_totals.read_rows,
        read_bytes: incr_totals.read_bytes,
        selected_marks: incr_totals.selected_marks,
        memory_usage: incr_totals.memory_usage,
        query_duration_ms: incr_totals.query_duration_ms,
    };

    Ok(vec![full_ev, incr_ev])
}

/// [`over_inclusion_probe`]'s result: the **physical** cost (`read_rows`)
/// and the **semantic** over-inclusion observable (`matched_candidates`) —
/// issue #34 CODE review round-2 [valid] finding #5: `read_rows` alone
/// (round-1's only capture) is explained entirely by the structural
/// primary-key finding (both bucket sizes already read the whole metric,
/// see `paths.rs`'s module doc comment) and cannot show the docs/schemas.md
/// §2.1 day-bucket over-inclusion effect at all. `matched_candidates` —
/// the same query's own **deduplicated fingerprint count** — is the right
/// observable: with this corpus's single-bucket-per-series staggering, a
/// 10-minute window floored to a `1h` bucket matches only the ~`C /
/// buckets.len()` series staggered into that one bucket, while floored to
/// a `1d` bucket (few or one bucket total) it matches nearly the whole
/// metric's `C` series — a genuine, demonstrable ratio, independent of the
/// `read_rows` finding.
#[derive(Debug, Clone, Copy)]
pub struct OverInclusionProbe {
    pub read_rows: u64,
    pub matched_candidates: u64,
}

/// The day-bucket over-inclusion probe: the product's own
/// `historical_series_subquery` over a fixed 10-minute window, for `tier`'s
/// broad selector, at the corpus's own `bucket_ms`. Called once per tier
/// per bucket size by `metrics_labels::run`; the caller pairs the `1h`- and
/// `1d`-bucket results into the report's two over-inclusion ratios (see
/// [`OverInclusionProbe`]'s doc comment).
pub async fn over_inclusion_probe(
    cfg: &PathsConfig<'_>,
    tier: &TierInfo,
    bucket_ms: i64,
    end_ms: i64,
) -> anyhow::Result<OverInclusionProbe> {
    let series_table = table_name("metric_series", cfg.dist);
    let window = DataWindow {
        start_ms: end_ms - 10 * 60_000,
        end_ms,
    };
    let matchers = SelectorKind::BroadEq.matchers();
    let sql = metrics_sql::historical_series_subquery(
        &series_table,
        &tier.metric_name,
        window,
        bucket_ms,
        &matchers,
    );
    let query_id = format!(
        "bench-metrics-overinclusion-{}-{bucket_ms}-{}",
        tier.metric_name,
        std::process::id()
    );
    // The query already returns fingerprints — `fetch_fingerprints` already
    // sorts + dedups them, so the candidate count is just its length; no
    // second query needed.
    let fps = fetch_fingerprints(cfg.client, &sql, &query_id, cfg.dist).await?;
    let matched_candidates = fps.len() as u64;
    if cfg.dist {
        flush_logs_before_shard_read(cfg.client, cfg.cluster).await?;
    } else {
        flush_logs(cfg.client).await?;
    }
    let totals = read_query_log(cfg.client, &query_id).await?;
    Ok(OverInclusionProbe {
        read_rows: totals.read_rows,
        matched_candidates,
    })
}

/// Runs every `(tier, selector)` cell across all three paths, plus the
/// refresh-sweep evidence, asserting the cross-path correctness gate on
/// each cell before recording it.
///
/// `cache_client` is a **separate, owned** `ChClient` connection dedicated
/// to the [`LabelCache`] this function builds and drives — `ChClient` is
/// not `Clone` (it owns a connection pool), and `LabelCache::new` takes
/// ownership, so the cache gets its own connection rather than sharing
/// `cfg.client`'s (used for every other query in this module).
pub async fn run_all(
    cfg: &PathsConfig<'_>,
    cache_client: ChClient,
    summary: &MetricsCorpusSummary,
) -> anyhow::Result<(Vec<PathEvidence>, Vec<RefreshEvidence>)> {
    let series_table = table_name("metric_series", cfg.dist);
    let idx_table = table_name("metric_series_idx", cfg.dist);

    let cache_cfg = LabelCacheConfig {
        db: cfg.db.to_string(),
        series_table: series_table.clone(),
        bucket_ms: summary.bucket_ms,
        window_ms: summary.window_ms,
        cache_max_series: cfg.cache_max_series,
        ttl: std::time::Duration::from_secs(3600),
        staleness_multiplier: DEFAULT_STALENESS_MULTIPLIER,
    };
    let cache = LabelCache::new(cache_client, cache_cfg);

    // `run_refresh_evidence` performs the (only) `cache.refresh()` call —
    // the same warm snapshot it produces is what every `cache.resolve(...)`
    // call below reads.
    let refresh_evidence = run_refresh_evidence(cfg, &cache, &series_table, summary).await?;

    // Guard-band drift bound (issue #34 CODE review round-2 [valid] finding
    // #3): `super::corpus`'s `buckets[0]` guard neutralizes at most one
    // `bucket_ms` of drift between the corpus's frozen `ref_ms`
    // (`summary.end_ms`) and this point, immediately before path
    // resolution begins. Nothing upstream *enforces* that the actual
    // elapsed wall-clock time (corpus load + idx build + the refresh sweep
    // just above) stayed under that bound — a silent violation would only
    // surface, if at all, as a cross-path fingerprint mismatch several
    // ClickHouse round trips later. Fail loudly and immediately instead.
    let elapsed_ms = now_unix_ms() - summary.end_ms;
    anyhow::ensure!(
        elapsed_ms < summary.bucket_ms,
        "corpus load+build ({elapsed_ms} ms) exceeded the one-bucket guard ({} ms); cross-path \
         sets may diverge — raise --corpus-window-hours or reduce cardinality",
        summary.bucket_ms
    );

    let covered_from_ms =
        floor_to_activity_bucket(summary.end_ms - summary.window_ms, summary.bucket_ms);
    let window = DataWindow {
        start_ms: covered_from_ms,
        end_ms: summary.end_ms,
    };

    let mut path_evidence = Vec::with_capacity(summary.tiers.len() * SelectorKind::ALL.len() * 3);

    for tier in &summary.tiers {
        for kind in SelectorKind::ALL {
            let matchers = kind.matchers();

            let (cache_ev, mut cache_fps) =
                run_cache_path(&cache, cfg.matcher_reps, tier, kind, &matchers, window)?;

            let sql2 = metrics_sql::historical_series_subquery(
                &series_table,
                &tier.metric_name,
                window,
                summary.bucket_ms,
                &matchers,
            );
            let (sql_ev, mut sql_fps) =
                run_sql_path(cfg, "sql_fallback", tier, kind, &sql2, &series_table).await?;

            let sql3 = idx_resolve_sql(
                &idx_table,
                &tier.metric_name,
                window,
                summary.bucket_ms,
                &matchers,
            );
            let (idx_ev, mut idx_fps) =
                run_sql_path(cfg, "idx_prototype", tier, kind, &sql3, &idx_table).await?;

            cache_fps.sort_unstable();
            sql_fps.sort_unstable();
            idx_fps.sort_unstable();
            anyhow::ensure!(
                cache_fps == sql_fps && sql_fps == idx_fps,
                "cross-path fingerprint mismatch for metric={} selector={kind:?}: \
                 cache={} distinct sql_fallback={} distinct idx_prototype={} distinct — \
                 this invalidates the three-path comparison (bench misconfiguration or a \
                 real correctness bug, either way the run must not report perf evidence \
                 for a mismatched cell)",
                tier.metric_name,
                cache_fps.len(),
                sql_fps.len(),
                idx_fps.len()
            );

            path_evidence.push(cache_ev);
            path_evidence.push(sql_ev);
            path_evidence.push(idx_ev);
        }
    }

    Ok((path_evidence, refresh_evidence))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window() -> DataWindow {
        DataWindow {
            start_ms: 0,
            end_ms: 3_600_000,
        }
    }

    #[test]
    fn idx_resolve_sql_pure_positive_eq_uses_uniq_exact_if_only_and_no_key_in() {
        let sql = idx_resolve_sql(
            "metric_series_idx",
            "up",
            window(),
            3_600_000,
            &SelectorKind::BroadEq.matchers(),
        );
        assert!(sql.contains("uniqExactIf((key, val), (key, val) = ('job', 'j0')) = 1"));
        assert!(sql.contains("AND ((key, val) = ('job', 'j0'))"));
        assert!(!sql.contains("countIf"));
        assert!(!sql.contains("key IN"));
    }

    /// Issue #34 CODE review [medium] finding: the pure-negative shape must
    /// scan the whole metric-scoped window with **no key filter at all** —
    /// a `key IN (...)` prefilter would silently drop fingerprints lacking
    /// the negated key, breaking Prometheus's absent-label-as-`""`
    /// semantics.
    #[test]
    fn idx_resolve_sql_pure_negative_has_no_key_filter_and_scans_the_whole_metric() {
        let sql = idx_resolve_sql(
            "metric_series_idx",
            "up",
            window(),
            3_600_000,
            &SelectorKind::NegBroad.matchers(),
        );
        assert!(sql.contains("countIf((key, val) = ('job', 'j0')) = 0"));
        assert!(!sql.contains("uniqExactIf"));
        assert!(!sql.contains("key IN"));
        // No branch/key filter between the metric+bucket WHERE and GROUP BY
        // — the whole metric-scoped window is scanned.
        assert!(sql.contains(
            "WHERE metric_name = 'up' AND bucket >= 0 AND bucket <= 3600000\nGROUP BY fingerprint"
        ));
    }

    #[test]
    fn idx_resolve_sql_regex_matcher_renders_anchored_match() {
        let sql = idx_resolve_sql(
            "metric_series_idx",
            "up",
            window(),
            3_600_000,
            &SelectorKind::Regex5xx.matchers(),
        );
        assert!(sql.contains("match(val, '^(?:5..)$')"));
    }

    #[test]
    fn idx_resolve_sql_mixed_pos_and_neg_ands_both_conditions() {
        let matchers = vec![
            LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Eq,
                value: "j0".to_string(),
            },
            LabelMatcher {
                key: "env".to_string(),
                op: MatchOp::Neq,
                value: "e0".to_string(),
            },
        ];
        let sql = idx_resolve_sql("metric_series_idx", "up", window(), 3_600_000, &matchers);
        assert!(sql.contains("uniqExactIf((key, val), (key, val) = ('job', 'j0')) = 1"));
        assert!(sql.contains("countIf((key, val) = ('env', 'e0')) = 0"));
        assert!(sql.contains("AND ((key, val) = ('job', 'j0') OR (key, val) = ('env', 'e0'))"));
        assert!(!sql.contains("key IN"));
    }

    #[test]
    fn idx_resolve_sql_floors_bounds_to_the_activity_bucket() {
        let sql = idx_resolve_sql(
            "metric_series_idx",
            "up",
            DataWindow {
                start_ms: 1,
                end_ms: 3_600_001,
            },
            3_600_000,
            &SelectorKind::NarrowEq.matchers(),
        );
        assert!(sql.contains("bucket >= 0 AND bucket <= 3600000"));
    }

    #[test]
    fn sweep_sql_copy_matches_the_product_shape() {
        // Pinned against `pulsus_read::metrics::refresh`'s own
        // `sweep_sql_renders_the_lower_bound_with_no_upper_bound` test —
        // the module doc comment's "Sweep SQL drift" discipline.
        let sql = sweep_sql_copy("metric_series", 1_000, None);
        assert!(sql.contains("unix_milli >= 1000"));
        assert!(!sql.contains("unix_milli <="));
        assert!(sql.ends_with("LIMIT 1 BY metric_name, fingerprint"));
    }

    #[test]
    fn sweep_sql_copy_incremental_variant_adds_the_extra_predicate() {
        let sql = sweep_sql_copy("metric_series", 0, Some("unix_milli > 3600000"));
        assert!(sql.contains("unix_milli >= 0"));
        assert!(sql.contains("AND unix_milli > 3600000"));
    }

    #[test]
    fn selector_kind_matchers_are_distinct_per_kind() {
        let mut seen: Vec<String> = SelectorKind::ALL.iter().map(|k| k.describe()).collect();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), SelectorKind::ALL.len());
    }

    // --- cross-path correctness gate: hand-built-corpus unit test
    // (architect plan amendment #3, test-gap finding) ---
    //
    // The live gate (`run_all`'s `anyhow::ensure!`) checks agreement
    // between the real `LabelCache`, the real `historical_series_subquery`,
    // and this module's `idx_resolve_sql` against a real ClickHouse. This
    // is the no-database complement: a tiny hand-built 8-series corpus, a
    // from-scratch reference matcher evaluator (mirroring the documented
    // absent-label-as-`""` semantics both the cache and the SQL paths
    // share), and a from-scratch simulation of `idx_resolve_sql`'s
    // `uniqExactIf`/`countIf` HAVING semantics — cross-checked against each
    // other for all four `SelectorKind`s, independently of the SQL-text
    // assertions above.

    /// Eight series: `job` alternates `j0`/`j1` (except two that omit it
    /// entirely — see below), `status` is `"500"` on every 4th series and
    /// `"200"` otherwise, `pod` is unique per series — enough to exercise a
    /// real (not degenerate 0-or-all) match/no-match split for every
    /// `SelectorKind`.
    ///
    /// **Absent-label series (issue #34 CODE review [medium] finding):**
    /// series `0` and `4` omit `job` entirely (both, incidentally, also
    /// `status="500"` — irrelevant, `status` and `job` are independent
    /// keys). `NegBroad` (`job!="j0"`) must include both — this is exactly
    /// the case that masked the idx prototype's `key IN (...)` bug: with
    /// every series carrying every key (the corpus's original shape),
    /// there was never a fingerprint for `countIf`'s absence path to get
    /// wrong.
    fn tiny_corpus() -> Vec<(u64, Vec<(&'static str, String)>)> {
        (0u64..8)
            .map(|i| {
                let status = if i % 4 == 0 { "500" } else { "200" }.to_string();
                let pod = format!("pod-{i}");
                let mut labels = vec![("status", status), ("pod", pod)];
                if i != 0 && i != 4 {
                    labels.push(("job", format!("j{}", i % 2)));
                }
                (i, labels)
            })
            .collect()
    }

    /// A minimal `.`-wildcard-only pattern matcher (anchored; literal chars
    /// plus `.` = any-one-char) — sufficient for this benchmark's only
    /// regex selector pattern (`"5.."`), without pulling in the `regex`
    /// crate as a second, test-only dependency for a check this narrow.
    fn matches_dot_pattern(pattern: &str, value: &str) -> bool {
        let p: Vec<char> = pattern.chars().collect();
        let v: Vec<char> = value.chars().collect();
        p.len() == v.len()
            && p.iter()
                .zip(v.iter())
                .all(|(pc, vc)| *pc == '.' || pc == vc)
    }

    /// Evaluates one matcher against a series' labels using the shared
    /// absent-label-as-`""` semantics (`pulsus_read::metrics::labels`'s
    /// `matches` / `JSONExtractString`'s own contract — both paths this
    /// benchmark drives already agree on this; re-derived here rather than
    /// imported so this test is a genuine independent cross-check, not a
    /// tautology).
    fn eval_matcher(labels: &[(&str, String)], m: &LabelMatcher) -> bool {
        let value = labels
            .iter()
            .find(|(k, _)| *k == m.key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        match m.op {
            MatchOp::Eq => value == m.value,
            MatchOp::Neq => value != m.value,
            MatchOp::Re => matches_dot_pattern(&m.value, value),
            MatchOp::Nre => !matches_dot_pattern(&m.value, value),
        }
    }

    fn reference_resolve(
        corpus: &[(u64, Vec<(&'static str, String)>)],
        matchers: &[LabelMatcher],
    ) -> Vec<u64> {
        let mut out: Vec<u64> = corpus
            .iter()
            .filter(|(_, labels)| matchers.iter().all(|m| eval_matcher(labels, m)))
            .map(|(fp, _)| *fp)
            .collect();
        out.sort_unstable();
        out
    }

    /// The raw literal **branch predicate** `idx_resolve_sql` renders for
    /// one matcher — `(key, val) = (key, value)` for `Eq`/`Neq` (both
    /// render the *same* equality shape; only which bucket, `pos_or` vs.
    /// `neg_or`, they land in differs) or a pattern match for `Re`/`Nre`.
    /// Deliberately **not** [`eval_matcher`]'s final-selector polarity: a
    /// `Neq`/`Nre` branch predicate checks for the *excluded* value's
    /// literal occurrence (which `countIf(...) = 0` then requires be
    /// absent), not the already-negated "series passes the selector"
    /// question `eval_matcher`/`reference_resolve` answer. An absent key
    /// (no row for `m.key` at all) always reads `""`, matching neither an
    /// `Eq`/`Neq` literal nor a well-formed `.`-pattern — never a
    /// `branch_hit`, whichever bucket (`pos_or`/`neg_or`) it would land in.
    fn branch_hit(labels: &[(&str, String)], m: &LabelMatcher) -> bool {
        let actual = labels
            .iter()
            .find(|(k, _)| *k == m.key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        match m.op {
            MatchOp::Eq | MatchOp::Neq => actual == m.value,
            MatchOp::Re | MatchOp::Nre => matches_dot_pattern(&m.value, actual),
        }
    }

    /// A from-scratch simulation of [`idx_resolve_sql`]'s HAVING semantics
    /// (`uniqExactIf((key,val), pos) = n_positive_keys AND countIf(neg) =
    /// 0`) over the **whole** corpus, with no key-based prefilter (issue
    /// #34 CODE review [medium] finding: matching the fixed SQL's own "no
    /// `key IN (...)`" shape) — deliberately not calling `idx_resolve_sql`
    /// itself (which only renders SQL text, never executes it), so
    /// agreement with [`reference_resolve`] is a genuine cross-check of the
    /// *algorithm*, not just of string rendering.
    fn simulated_idx_resolve(
        corpus: &[(u64, Vec<(&'static str, String)>)],
        matchers: &[LabelMatcher],
    ) -> Vec<u64> {
        let n_positive_keys = matchers
            .iter()
            .filter(|m| matches!(m.op, MatchOp::Eq | MatchOp::Re))
            .map(|m| &m.key)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let mut out = Vec::new();
        for (fp, labels) in corpus {
            let pos_hits = matchers
                .iter()
                .filter(|m| matches!(m.op, MatchOp::Eq | MatchOp::Re))
                .filter(|m| branch_hit(labels, m))
                .count();
            let neg_hits = matchers
                .iter()
                .filter(|m| matches!(m.op, MatchOp::Neq | MatchOp::Nre))
                .filter(|m| branch_hit(labels, m))
                .count();
            if pos_hits == n_positive_keys && neg_hits == 0 {
                out.push(*fp);
            }
        }
        out.sort_unstable();
        out
    }

    #[test]
    fn cross_path_reference_and_simulated_idx_agree_on_every_selector_kind() {
        let corpus = tiny_corpus();
        for kind in SelectorKind::ALL {
            let matchers = kind.matchers();
            let expected = reference_resolve(&corpus, &matchers);
            let simulated = simulated_idx_resolve(&corpus, &matchers);
            assert_eq!(expected, simulated, "{kind:?} mismatch");
            // Every kind must produce a real, non-degenerate match/no-match
            // split on this corpus (a 0-or-all result would not actually
            // exercise the matcher).
            assert!(
                !expected.is_empty() && expected.len() < corpus.len(),
                "{kind:?}: {expected:?}"
            );
        }
    }
}
