//! `MetricsEngine` — orchestrates `pulsus_promql::plan` -> resolve/fetch ->
//! `pulsus_promql::evaluate`, mirroring [`crate::logql::LogQlEngine`]'s
//! shape (`query`/`query_explained`, `EngineConfig`-style owned config).
//! This module is the **only** place in the workspace that calls into
//! both `pulsus_promql` (the pure planner/evaluator) and `pulsus_read`'s
//! own ClickHouse-touching machinery (`ChClient`, `LabelCache`) — every
//! actual PromQL semantic (resets, extrapolation, staleness, Kahan,
//! `histogram_quantile`) lives in `pulsus-promql` and is not re-derived
//! here; this module's whole job is I/O: turn a [`SelectorSpec`] into
//! fetched samples.
//!
//! **Concurrency contract (ratified, task-manager pre-approved on issue
//! #31's plan amendment §2):** `query`/`query_explained` issue **every**
//! selector's resolve+fetch concurrently via [`futures::future::join_all`]
//! over the full selector set — satisfying "binary expressions evaluate
//! both sides concurrently" at the I/O layer, since both sides of every
//! binop are themselves selectors (or trees of selectors) in that same
//! set. `pulsus_promql::evaluate` then runs serially over the assembled
//! [`SeriesData`] — the evaluator is pure CPU with no I/O, so the
//! latency-relevant concurrency is entirely at this fetch layer.
//! Fingerprint sets `>= 500` additionally split into parallel chunk
//! fetches *within* one selector (edge case 7).
//!
//! **`X-Pulsus-Explain` carries real SQL** (code review round 1, finding
//! 5): every `sample_fetch` stage's SQL is built once, synchronously, in
//! `query_inner`'s phase-1 loop (a pure function of `(selector,
//! resolution, window)` — see [`build_chunk_sqls`]/
//! [`sample_sql::sample_fetch_subquery`]) and handed pre-built into phase
//! 2's concurrent fetches, so the explain trace and the actual executed
//! SQL can never drift. A cache-hit selector splitting into >1 chunk
//! surfaces only the first chunk's SQL plus a `"(+N more chunks...)"`
//! note (chunk SQL is textually identical modulo the `IN (...)` list, so
//! per-chunk explain entries would be O(chunks) noise for no new
//! information).

use std::collections::HashMap;
use std::future::Future;

use futures::StreamExt;
use futures::future::join_all;
use pulsus_clickhouse::{ChClient, ChRow, ChRowStream, QuerySettings};
use pulsus_model::{Fingerprint, LabelSet};
use pulsus_promql::eval::aggregation;
use pulsus_promql::parser::Expr;
use pulsus_promql::{
    DEFAULT_LOOKBACK_MS, FetchedSeries, InstantSample, Labels, PlanParams, QueryValue, RangeSeries,
    Sample, SelectorSpec, SeriesData,
};

use super::labels::LabelledResolution;
use super::matcher::{DataWindow, DiscoveryFilter};
use super::sample_rows::SampleRow;
use super::sample_sql;
use crate::logql::error::ReadError;
use crate::logql::exec::{MatrixSeries, QueryResult, VectorSample, escape_query_placeholders};
use crate::logql::explain::PlanExplain;

/// Owned table configuration a [`MetricsEngine`] plans every query
/// against — mirrors [`crate::logql::EngineConfig`]'s "owned `String`s, no
/// borrowed lifetime on the engine itself" shape.
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Carried for parity with [`crate::logql::EngineConfig`]; the
    /// generated SQL never table-prefixes (the connection's default
    /// database resolves the unqualified table names).
    pub db: String,
    /// `metric_samples`.
    pub samples_table: String,
    /// `metric_series` — needed for the `SqlFallback` path's label
    /// hydration query ([`super::sql::series_labels_by_fingerprint`]) and
    /// (issue #32) the discovery endpoints' own `metric_series`-backed
    /// query ([`super::sql::discovery_query`]).
    pub series_table: String,
    /// `metric_metadata` — issue #32's `/api/v1/metadata`
    /// ([`super::sql::metadata_query`]). **Never** `_dist`-suffixed
    /// (docs/schemas.md §2.1: it is a global, unsharded catalog table) —
    /// callers deriving table names from `Config` must not apply the same
    /// `_dist` rule they use for `samples_table`/`series_table`.
    pub metadata_table: String,
}

/// The `SqlFallback` sample-fetch path's label-hydration result row
/// ([`super::sql::series_labels_by_fingerprint`]'s `SELECT fingerprint,
/// labels`) — deliberately not [`super::rows::SeriesRow`], which also
/// carries `metric_name` (that sweep query's own third column; this
/// hydration query never selects it, so reusing the 3-field row here would
/// be a column-count mismatch against the 2-column result set).
#[derive(
    Debug, Clone, PartialEq, Eq, pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize,
)]
struct HydratedLabelsRow {
    fingerprint: u64,
    labels: String,
}

/// [`MetricsEngine::metadata`]'s `metric_metadata` result row
/// ([`super::sql::metadata_query`]'s `argMax`-collapsed columns).
#[derive(
    Debug, Clone, PartialEq, Eq, pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize,
)]
struct MetricMetaRow {
    metric_name: String,
    metric_type: String,
    help: String,
    unit: String,
}

/// One `metric_metadata` row (issue #32): `name` is always the **base
/// family name** (docs/schemas.md §2.1's writer contract) — a derived
/// series' `_bucket`/`_sum`/`_count` suffix is never stripped by this type
/// or by [`MetricsEngine::metadata`]; callers must already be querying by
/// the base name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricMeta {
    pub name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}

/// `GET /api/v1/status/tsdb`'s payload (issue #32; code-review round-1
/// fix): `num_series`/`series_count_by_metric_name` reflect the resident
/// label-cache snapshot (freshness = cache TTL, docs/api.md §3.4) — **zero
/// ClickHouse**, per task-manager resolution #2. A `num_samples` field
/// previously queried `count() FROM metric_samples` live, violating that
/// contract; it is also not a real Prometheus `headStats` field (real
/// `headStats` is `numSeries`/`numLabelPairs`/`chunkCount`/`minTime`/
/// `maxTime`) and cannot be served from the cache (which holds
/// `fingerprint -> labels`, no sample counts) — removed rather than kept
/// as a live-query exception.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsdbStatus {
    pub num_series: u64,
    pub series_count_by_metric_name: Vec<(String, u64)>,
}

/// A metrics query's time span. Instant = `start_ms == end_ms`,
/// `step_ms == 0` (mirrors `pulsus_promql::PlanParams`'s own contract,
/// which this is turned into 1:1 plus the fixed M2 staleness lookback).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricQueryParams {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
}

impl MetricQueryParams {
    fn plan_params(&self) -> PlanParams {
        PlanParams {
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            step_ms: self.step_ms,
            lookback_ms: DEFAULT_LOOKBACK_MS,
        }
    }
}

pub struct MetricsEngine {
    client: ChClient,
    resolver: std::sync::Arc<super::labels::LabelCache>,
    config: MetricsConfig,
}

impl MetricsEngine {
    pub fn new(
        client: ChClient,
        resolver: std::sync::Arc<super::labels::LabelCache>,
        config: MetricsConfig,
    ) -> Self {
        Self {
            client,
            resolver,
            config,
        }
    }

    pub async fn query(
        &self,
        expr: &Expr,
        p: &MetricQueryParams,
    ) -> Result<QueryResult, ReadError> {
        self.query_inner(expr, p, None).await
    }

    /// [`MetricsEngine::query`] plus its `X-Pulsus-Explain` trace, in the
    /// same single pass (no second execution) — mirrors
    /// [`crate::logql::LogQlEngine::query_explained`]'s contract.
    pub async fn query_explained(
        &self,
        expr: &Expr,
        p: &MetricQueryParams,
    ) -> Result<(QueryResult, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("metrics");
        let result = self.query_inner(expr, p, Some(&mut explain)).await?;
        Ok((result, explain))
    }

    async fn query_inner(
        &self,
        expr: &Expr,
        p: &MetricQueryParams,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
        let plan_params = p.plan_params();
        let plan = pulsus_promql::plan(expr, plan_params)?;

        // The zero-ClickHouse `count`/`group` fast path (architect plan
        // AC): only taken when the cache can answer it; any degradation
        // (cold/stale/out-of-window/over-cardinality/regex-unsupported)
        // falls through to the ordinary per-selector fetch+evaluate path
        // below, which resolves the same selector through the identical
        // `LabelCache`/`SqlFallback` machinery — so the "historical variant
        // routes through metric_series" AC holds without special-casing.
        if let Some(ca) = plan.cache_answerable() {
            let window = DataWindow {
                start_ms: plan_params.start_ms,
                end_ms: plan_params.end_ms,
            };
            match self
                .resolver
                .resolve_labelled(&ca.metric_name, &ca.matchers, window)
            {
                LabelledResolution::Series(pairs) => {
                    if let Some(e) = explain.as_mut() {
                        e.push(
                            "cache_only",
                            format!("label cache: {} matching series", pairs.len()),
                            Some("zero ClickHouse queries".to_string()),
                        );
                    }
                    let vector: Vec<InstantSample> = pairs
                        .into_iter()
                        .map(|(_, labels)| InstantSample {
                            labels: to_promql_labels(&labels),
                            // `ca.op` is always `count`/`group` here (the
                            // only cache-answerable ops) — `aggregate`
                            // unconditionally drops `metric_name` for both
                            // (issue #37's aggregation-drops rule), so this
                            // value is never observed in the result; set to
                            // the matched metric for documentation/
                            // consistency with the ordinary fetch path.
                            metric_name: Some(ca.metric_name.clone()),
                            t_ms: plan_params.start_ms,
                            v: 1.0,
                        })
                        .collect();
                    let out = aggregation::aggregate(ca.op, &vector, ca.grouping.as_ref(), None)?;
                    return Ok(vector_to_query_result(out));
                }
                LabelledResolution::SqlFallback { .. } => {
                    // Fall through to the ordinary path below.
                }
            }
        }

        // Phase 1 (sync, cheap): resolve every selector, build its fetch
        // plan (the actual `sample_fetch` SQL — a pure function of `(sel,
        // resolution, window)`), and push both the `series_resolution` and
        // `sample_fetch` explain stages with the real generated SQL (code
        // review round 1, finding 5 — AC requires explain to carry SQL,
        // not just a table name + series count). Nothing here awaits — see
        // `LabelCache::resolve_labelled`'s own purity contract.
        let mut fetch_plans = Vec::with_capacity(plan.selectors.len());
        for sel in &plan.selectors {
            let (lower_excl, upper_incl) = sel.fetch_window(&plan_params);
            let window = DataWindow {
                start_ms: lower_excl,
                end_ms: upper_incl,
            };
            let resolution =
                self.resolver
                    .resolve_labelled(&sel.metric_name, &sel.matchers, window);
            if let Some(e) = explain.as_mut() {
                match &resolution {
                    LabelledResolution::Series(pairs) => e.push(
                        "series_resolution",
                        format!("label cache: {} matching series", pairs.len()),
                        None,
                    ),
                    LabelledResolution::SqlFallback { sql, reason } => e.push(
                        "series_resolution",
                        sql.clone(),
                        Some(format!("{reason:?}")),
                    ),
                }
            }

            let fetch_plan = match resolution {
                LabelledResolution::Series(pairs) => {
                    let labels_by_fp: HashMap<Fingerprint, LabelSet> =
                        pairs.iter().cloned().collect();
                    let fps: Vec<Fingerprint> = pairs.into_iter().map(|(fp, _)| fp).collect();
                    let total_fps = fps.len();
                    let sqls = build_chunk_sqls(
                        &self.config.samples_table,
                        &sel.metric_name,
                        fps,
                        lower_excl,
                        upper_incl,
                    );
                    if let Some(e) = explain.as_mut()
                        && let Some(first) = sqls.first()
                    {
                        // Chunk elision (finding 5): only the first chunk's
                        // SQL is surfaced verbatim; a note names how many
                        // more chunks (and total fingerprints) were fetched
                        // identically, avoiding an O(chunks) explain blow-up
                        // for a selector matching thousands of series.
                        let note = (sqls.len() > 1).then(|| {
                            format!(
                                "(+{} more chunks like this one, {total_fps} fingerprints total)",
                                sqls.len() - 1
                            )
                        });
                        e.push("sample_fetch", first.clone(), note);
                    }
                    SelectorFetchPlan::Chunks { sqls, labels_by_fp }
                }
                LabelledResolution::SqlFallback { sql, .. } => {
                    let fetch_sql = sample_sql::sample_fetch_subquery(
                        &self.config.samples_table,
                        &sel.metric_name,
                        &sql,
                        lower_excl,
                        upper_incl,
                    );
                    if let Some(e) = explain.as_mut() {
                        e.push("sample_fetch", fetch_sql.clone(), None);
                    }
                    SelectorFetchPlan::Fallback { sql: fetch_sql }
                }
            };
            fetch_plans.push(fetch_plan);
        }

        // Phase 2 (async, concurrent across the full selector set — the
        // ratified binop-concurrency contract): execute every selector's
        // already-built fetch plan at once via `join_all`.
        let fetches = plan
            .selectors
            .iter()
            .zip(fetch_plans)
            .map(|(sel, fetch_plan)| self.execute_fetch_plan(sel, fetch_plan));
        let fetched: Vec<Result<Vec<FetchedSeries>, ReadError>> = join_all(fetches).await;

        let mut data = SeriesData::new();
        for (sel, series) in plan.selectors.iter().zip(fetched) {
            data.insert(sel.id, series?);
        }

        let value = pulsus_promql::evaluate(&plan, &data)?;
        Ok(value_to_query_result(value))
    }

    /// Executes one selector's already-built [`SelectorFetchPlan`]: the
    /// cache-hit path fetches every chunk's pre-built SQL concurrently;
    /// the `SqlFallback` path issues the single nested-subquery sample
    /// fetch, then hydrates labels for just the fingerprints that returned
    /// samples.
    async fn execute_fetch_plan(
        &self,
        sel: &SelectorSpec,
        fetch_plan: SelectorFetchPlan,
    ) -> Result<Vec<FetchedSeries>, ReadError> {
        match fetch_plan {
            SelectorFetchPlan::Chunks { sqls, labels_by_fp } => {
                if sqls.is_empty() {
                    return Ok(Vec::new());
                }
                let rows = fetch_all_concurrently(sqls, |sql| self.fetch_rows(sql)).await?;
                Ok(group_rows(rows, &labels_by_fp))
            }
            SelectorFetchPlan::Fallback { sql } => {
                let rows: Vec<SampleRow> = self.fetch_rows(sql).await?;
                if rows.is_empty() {
                    return Ok(Vec::new());
                }
                let mut fps: Vec<Fingerprint> = rows.iter().map(|r| r.fingerprint).collect();
                fps.sort_unstable();
                fps.dedup();
                let hydrate_sql = super::sql::series_labels_by_fingerprint(
                    &self.config.series_table,
                    &sel.metric_name,
                    &fps,
                );
                let series_rows: Vec<HydratedLabelsRow> = self.fetch_rows(hydrate_sql).await?;
                let labels_by_fp: HashMap<Fingerprint, LabelSet> = series_rows
                    .into_iter()
                    .map(|r| (r.fingerprint, parse_canonical_labels(&r.labels)))
                    .collect();
                Ok(group_rows(rows, &labels_by_fp))
            }
        }
    }

    /// Wraps [`ChClient::query_stream`] with the placeholder-escaping fix
    /// [`crate::logql::exec::escape_query_placeholders`] applies — the
    /// `SqlFallback` sub-query's `^(?:...)$` regex predicates always carry
    /// a literal `?`, and the `clickhouse` crate's `SqlBuilder` treats a
    /// bare `?` as an unbound bind placeholder unless doubled. No scan-
    /// budget concept in M2's metrics scope (unlike `logql::exec`'s own
    /// `query_stream` wrapper) — every `ChError` passes through as
    /// [`ReadError::Clickhouse`] unmapped; a byte-budget cap for metric
    /// reads is out of scope for this issue.
    async fn fetch_rows<R: ChRow>(&self, sql: String) -> Result<Vec<R>, ReadError> {
        let sql = escape_query_placeholders(&sql);
        let mut stream: ChRowStream<'_, R> = self
            .client
            .query_stream::<R>(&sql, &QuerySettings::new())
            .await
            .map_err(ReadError::Clickhouse)?;
        let mut out = Vec::new();
        while let Some(row) = stream.next().await {
            out.push(row.map_err(ReadError::Clickhouse)?);
        }
        Ok(out)
    }

    /// Discovery resolution shared by [`MetricsEngine::label_names`],
    /// [`MetricsEngine::label_values`], and [`MetricsEngine::series`]
    /// (issue #32). **Always** resolves via [`super::sql::discovery_query`]
    /// — the metric_series-backed SQL path with bucket-floored bounds for
    /// the caller's *exact* window — never the label cache's in-process
    /// fast path ([`LabelledResolution`]/[`super::labels::Resolution`]).
    /// The cache's resident snapshot spans the whole `PULSUS_CACHE_WINDOW`
    /// (e.g. 24h) and does not track each series' own bucketed activity
    /// time, so reusing the cache-hit branch here would leak that wider
    /// residency window into a narrower discovery response (#30 handoff
    /// AC: "the cache's bucket-granularity superset must not leak into
    /// /series results"). `filters` empty is Prometheus's own "no
    /// `match[]`" contract (docs/api.md §3.3) — every series in the
    /// window, unfiltered; each element otherwise applies its own
    /// window-bound, bucket-floored `metric_series` query, concurrently
    /// (`join_all`, mirroring `query_inner`'s fetch-concurrency contract),
    /// unioned and deduplicated by `(metric_name, fingerprint)` (a
    /// fingerprint is shared across metric names — see
    /// `super::refresh::run_sweep`'s own comment on the same invariant).
    async fn discovery_series(
        &self,
        filters: &[DiscoveryFilter],
        window: DataWindow,
    ) -> Result<Vec<(String, LabelSet)>, ReadError> {
        let bucket_ms = self.resolver.config.bucket_ms;
        let effective: Vec<DiscoveryFilter> = if filters.is_empty() {
            vec![DiscoveryFilter::default()]
        } else {
            filters.to_vec()
        };
        let fetches = effective.iter().map(|filter| {
            let sql =
                super::sql::discovery_query(&self.config.series_table, filter, window, bucket_ms);
            self.fetch_rows::<super::rows::SeriesRow>(sql)
        });
        let results: Vec<Result<Vec<super::rows::SeriesRow>, ReadError>> = join_all(fetches).await;
        let mut seen: std::collections::HashSet<(String, Fingerprint)> =
            std::collections::HashSet::new();
        let mut out = Vec::new();
        for rows in results {
            for row in rows? {
                if seen.insert((row.metric_name.clone(), row.fingerprint)) {
                    out.push((row.metric_name, parse_canonical_labels(&row.labels)));
                }
            }
        }
        Ok(out)
    }

    /// `GET|POST /api/v1/labels` (issue #32): the union of label keys over
    /// every series [`DiscoveryFilter`] matches, plus `__name__` always
    /// (docs/api.md §3.3) — even when the resolved series set is empty, an
    /// absent `metric_name` from every filter, or a metric whose series
    /// carry no labels at all: Prometheus's `/labels` always advertises
    /// `__name__` as a known label name.
    pub async fn label_names(
        &self,
        filters: &[DiscoveryFilter],
        window: DataWindow,
    ) -> Result<Vec<String>, ReadError> {
        let series = self.discovery_series(filters, window).await?;
        let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        names.insert("__name__".to_string());
        for (_, labels) in &series {
            for (k, _) in labels.iter() {
                names.insert(k.to_string());
            }
        }
        Ok(names.into_iter().collect())
    }

    /// `GET /api/v1/label/{name}/values` (issue #32): distinct values of
    /// `name` across every series [`DiscoveryFilter`] matches.
    /// `name == "__name__"` returns the distinct metric names themselves
    /// (docs/api.md §3.3).
    pub async fn label_values(
        &self,
        name: &str,
        filters: &[DiscoveryFilter],
        window: DataWindow,
    ) -> Result<Vec<String>, ReadError> {
        let series = self.discovery_series(filters, window).await?;
        let mut values: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        if name == "__name__" {
            for (metric_name, _) in &series {
                values.insert(metric_name.clone());
            }
        } else {
            for (_, labels) in &series {
                if let Some(v) = labels.get(name) {
                    values.insert(v.to_string());
                }
            }
        }
        Ok(values.into_iter().collect())
    }

    /// `GET|POST /api/v1/series` (issue #32): every matching series' full
    /// label set, each with `__name__=<metric_name>` spliced in (docs/api.md
    /// §3.3), sorted deterministically. `filters` must be non-empty —
    /// enforced by the caller (`pulsus-server`'s param parsing, `match[]`
    /// required), not re-validated here.
    pub async fn series(
        &self,
        filters: &[DiscoveryFilter],
        window: DataWindow,
    ) -> Result<Vec<Vec<(String, String)>>, ReadError> {
        let series = self.discovery_series(filters, window).await?;
        let mut out: Vec<Vec<(String, String)>> = series
            .into_iter()
            .map(|(metric_name, labels)| {
                let mut pairs: Vec<(String, String)> = labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                pairs.push(("__name__".to_string(), metric_name));
                pairs.sort();
                pairs
            })
            .collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// `GET /api/v1/metadata` (issue #32): `metric_metadata` rows, already
    /// keyed by the base family name (docs/schemas.md §2.1's writer
    /// contract — never stripped/derived here).
    pub async fn metadata(
        &self,
        metric: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<MetricMeta>, ReadError> {
        let sql = super::sql::metadata_query(&self.config.metadata_table, metric, limit);
        let rows: Vec<MetricMetaRow> = self.fetch_rows(sql).await?;
        Ok(rows
            .into_iter()
            .map(|r| MetricMeta {
                name: r.metric_name,
                metric_type: r.metric_type,
                help: r.help,
                unit: r.unit,
            })
            .collect())
    }

    /// `GET /api/v1/status/tsdb` (issue #32; code-review round-1 fix):
    /// `numSeries`/`seriesCountByMetricName` from the resident label-cache
    /// snapshot — **zero ClickHouse**, task-manager resolution #2,
    /// freshness = cache age. `async` only for call-site parity with
    /// every other `MetricsEngine` method; this never actually awaits
    /// anything.
    pub async fn tsdb_status(&self) -> Result<TsdbStatus, ReadError> {
        let cache_snapshot = self.resolver.tsdb_snapshot();
        Ok(TsdbStatus {
            num_series: cache_snapshot.num_series,
            series_count_by_metric_name: cache_snapshot.series_count_by_metric_name,
        })
    }
}

/// A selector's fully pre-built fetch plan — built once, synchronously, in
/// `query_inner`'s phase-1 loop (so the actual generated SQL is available
/// for `X-Pulsus-Explain`, code review round 1 finding 5), then executed
/// in phase 2 without re-deriving anything.
enum SelectorFetchPlan {
    /// Cache-hit path: one `sample_fetch` SQL string per chunk (already
    /// ascending-fingerprint-sorted — see [`build_chunk_sqls`]), plus the
    /// labels the cache already resolved.
    Chunks {
        sqls: Vec<String>,
        labels_by_fp: HashMap<Fingerprint, LabelSet>,
    },
    /// `SqlFallback` path: the single nested-subquery `sample_fetch`
    /// SQL — labels are hydrated afterward, from whichever fingerprints
    /// the fetch actually returns.
    Fallback { sql: String },
}

/// Sorts `fps` ascending, then splits into chunks and renders each
/// chunk's `sample_fetch` SQL (code review round 1, finding 3): the
/// `sort_unstable` here is a local hardening of the ascending-fingerprint
/// accumulation-order invariant — it does not rely on the resolver already
/// returning sorted fingerprints, even though `resolve_labelled` happens
/// to. Pure — no I/O — so it runs in `query_inner`'s synchronous phase-1
/// loop, making the real SQL available for explain before any fetch
/// starts.
fn build_chunk_sqls(
    samples_table: &str,
    metric_name: &str,
    mut fps: Vec<Fingerprint>,
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> Vec<String> {
    fps.sort_unstable();
    sample_sql::chunk_fingerprints(&fps, sample_sql::CHUNK_THRESHOLD)
        .into_iter()
        .map(|chunk| {
            sample_sql::sample_fetch(
                samples_table,
                metric_name,
                chunk,
                lower_excl_ms,
                upper_incl_ms,
            )
        })
        .collect()
}

/// Fetches every already-built SQL string concurrently via `join_all`,
/// concatenating results in **dispatch order** — `join_all` returns
/// results in **input order**, regardless of which future actually
/// completes first, so out-of-order completion never reorders the merged
/// rows. Proven by
/// `fetch_all_concurrently_merges_in_dispatch_order_despite_reversed_completion`
/// below (a mock fetch layer whose earlier-dispatched SQL deliberately
/// completes *after* its later-dispatched sibling).
async fn fetch_all_concurrently<F, Fut>(
    sqls: Vec<String>,
    fetch: F,
) -> Result<Vec<SampleRow>, ReadError>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<Vec<SampleRow>, ReadError>>,
{
    let results: Vec<Result<Vec<SampleRow>, ReadError>> =
        join_all(sqls.into_iter().map(&fetch)).await;
    let mut rows = Vec::new();
    for r in results {
        rows.extend(r?);
    }
    Ok(rows)
}

/// Groups already fingerprint-ordered `rows` (the fetch `ORDER BY
/// fingerprint, unix_milli` contract) into [`FetchedSeries`], preserving
/// that order — never re-sorted via a `HashMap` (edge case 4/7: the
/// evaluator's Kahan accumulation order is pinned to ascending-fingerprint
/// input order, which must survive every merge step unchanged).
fn group_rows(
    rows: Vec<SampleRow>,
    labels_by_fp: &HashMap<Fingerprint, LabelSet>,
) -> Vec<FetchedSeries> {
    let mut out: Vec<FetchedSeries> = Vec::new();
    for row in rows {
        let sample = Sample {
            t_ms: row.unix_milli,
            v: row.value,
        };
        match out.last_mut() {
            Some(last) if last.fingerprint == row.fingerprint => {
                last.samples.push(sample);
            }
            _ => {
                let labels = labels_by_fp
                    .get(&row.fingerprint)
                    .cloned()
                    .unwrap_or_default();
                out.push(FetchedSeries {
                    fingerprint: row.fingerprint,
                    labels: to_promql_labels(&labels),
                    samples: vec![sample],
                });
            }
        }
    }
    out
}

fn to_promql_labels(ls: &LabelSet) -> Labels {
    Labels::new(ls.iter().map(|(k, v)| (k.to_string(), v.to_string())))
}

/// Parses PulsusDB's canonical flat label JSON — duplicated (not shared)
/// from [`super::refresh`]'s own private helper of the same shape, per
/// that module's own precedent (module-private, no JSON crate dependency
/// added for this single use).
fn parse_canonical_labels(json: &str) -> LabelSet {
    let mut chars = json.chars().peekable();
    let mut pairs = Vec::new();
    while let Some(&c) = chars.peek() {
        chars.next();
        if c == '{' {
            break;
        }
    }
    loop {
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        match chars.peek() {
            None | Some('}') => break,
            Some(',') => {
                chars.next();
                continue;
            }
            Some('"') => {}
            Some(_) => break,
        }
        let Some(key) = parse_json_string(&mut chars) else {
            break;
        };
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        if chars.peek() == Some(&':') {
            chars.next();
        }
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        let Some(value) = parse_json_string(&mut chars) else {
            break;
        };
        pairs.push((key, value));
    }
    LabelSet::from_verbatim(pairs)
}

fn parse_json_string<I: Iterator<Item = char>>(
    chars: &mut std::iter::Peekable<I>,
) -> Option<String> {
    if chars.next() != Some('"') {
        return None;
    }
    let mut out = String::new();
    loop {
        match chars.next()? {
            '"' => return Some(out),
            '\\' => match chars.next()? {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                '/' => out.push('/'),
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                'u' => {
                    let hex: String = (0..4).filter_map(|_| chars.next()).collect();
                    if let Ok(code) = u32::from_str_radix(&hex, 16)
                        && let Some(c) = char::from_u32(code)
                    {
                        out.push(c);
                    }
                }
                other => out.push(other),
            },
            c => out.push(c),
        }
    }
}

/// Splices `metric_name` back in as a `__name__` entry (issue #37 fix) —
/// exactly the pattern `MetricsEngine::series` already uses for `/series`'s
/// discovery results (`series.push(("__name__".to_string(), metric_name))`),
/// now applied at the query path's own label-assembly seam too, so
/// `/api/v1/query`/`/api/v1/query_range` and `/api/v1/series` agree.
/// `pulsus_promql::eval`'s per-construct-class keep/drop verdict
/// (`InstantSample`/`RangeSeries::metric_name`, see that type's doc) is the
/// single source of truth for *whether* this pushes anything; the ordering
/// within the returned `Vec` does not matter — `prom_api::encode`'s
/// `labels_object_json` renders through a `serde_json::Map` (a `BTreeMap`),
/// which always re-sorts keys, `__name__` included.
fn with_metric_name(labels: Labels, metric_name: Option<String>) -> Vec<(String, String)> {
    let mut pairs = labels.0;
    if let Some(name) = metric_name {
        pairs.push(("__name__".to_string(), name));
    }
    pairs
}

fn vector_to_query_result(vector: Vec<InstantSample>) -> QueryResult {
    QueryResult::Vector(
        vector
            .into_iter()
            .map(|s| VectorSample {
                labels: with_metric_name(s.labels, s.metric_name),
                value: s.v,
            })
            .collect(),
    )
}

fn value_to_query_result(value: QueryValue) -> QueryResult {
    match value {
        QueryValue::Vector(v) => vector_to_query_result(v),
        QueryValue::Matrix(m) => QueryResult::Matrix(
            m.into_iter()
                .map(|s: RangeSeries| MatrixSeries {
                    labels: with_metric_name(s.labels, s.metric_name),
                    points: s.points,
                })
                .collect(),
        ),
        QueryValue::Scalar(v) => QueryResult::Scalar(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ls(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::from_verbatim(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    // --- build_chunk_sqls / fetch_all_concurrently: cross-chunk merge
    // determinism (code review round 1, finding 3) ---

    #[test]
    fn build_chunk_sqls_sorts_fingerprints_before_rendering() {
        // Unsorted input: chunk_size large enough for one chunk, so the
        // single resulting SQL's `IN (...)` list must read ascending
        // regardless of input order.
        let sqls = build_chunk_sqls("metric_samples", "up", vec![3, 1, 2], 0, 100);
        assert_eq!(sqls.len(), 1);
        assert!(sqls[0].contains("IN (1, 2, 3)"), "got: {}", sqls[0]);
    }

    #[test]
    fn build_chunk_sqls_splits_at_the_chunk_threshold() {
        let fps: Vec<Fingerprint> = (0..1_200).collect();
        let sqls = build_chunk_sqls("metric_samples", "up", fps, 0, 100);
        assert_eq!(sqls.len(), 3);
    }

    /// A mock fetch layer whose **earlier-dispatched** SQL (identified by
    /// a marker substring standing in for "covers the smaller
    /// fingerprints") deliberately sleeps *longer* than its later-
    /// dispatched sibling, so it completes *after* it — proving the merged
    /// output's order tracks dispatch order, not completion order.
    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_all_concurrently_merges_in_dispatch_order_despite_reversed_completion() {
        let sqls = vec!["chunk_a(1,2)".to_string(), "chunk_b(3,4)".to_string()];
        let rows = fetch_all_concurrently(sqls, |sql| async move {
            let fps: Vec<Fingerprint> = if sql.starts_with("chunk_a") {
                // Dispatched first, but finishes last.
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                vec![1, 2]
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                vec![3, 4]
            };
            Ok(fps
                .into_iter()
                .map(|fp| SampleRow {
                    fingerprint: fp,
                    unix_milli: 0,
                    value: fp as f64,
                })
                .collect())
        })
        .await
        .unwrap();

        let fingerprints: Vec<Fingerprint> = rows.iter().map(|r| r.fingerprint).collect();
        assert_eq!(
            fingerprints,
            vec![1, 2, 3, 4],
            "merged rows must stay in dispatch order even though chunk_a completed after chunk_b"
        );
    }

    /// The same reversed-completion scenario, but asserting the
    /// **aggregation result** (Kahan/Neumaier sum) is bit-identical to a
    /// reference computed from a single, already-ordered, unchunked fetch
    /// — proving the merge doesn't just "look" ordered but actually feeds
    /// the evaluator identical input to what an unchunked fetch would.
    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_all_concurrently_result_matches_a_single_chunk_reference_bit_for_bit() {
        let values: HashMap<Fingerprint, f64> = [(1, 1e100), (2, 1.0), (3, 2.0), (4, -1e100)]
            .into_iter()
            .collect();
        let make_rows = |fps: &[Fingerprint]| -> Vec<SampleRow> {
            fps.iter()
                .map(|&fp| SampleRow {
                    fingerprint: fp,
                    unix_milli: 0,
                    value: values[&fp],
                })
                .collect()
        };

        // Chunked (2 chunks of 2), fetched concurrently with reversed
        // completion order (mirrors the test above).
        let chunked_sqls = vec!["chunk_a(1,2)".to_string(), "chunk_b(3,4)".to_string()];
        let chunked_rows = fetch_all_concurrently(chunked_sqls, |sql| async move {
            if sql.starts_with("chunk_a") {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                Ok(make_rows(&[1, 2]))
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                Ok(make_rows(&[3, 4]))
            }
        })
        .await
        .unwrap();

        // Reference: a single "chunk" covering every fingerprint, no
        // concurrency, no reordering possible.
        let reference_sqls = vec!["single_chunk(1,2,3,4)".to_string()];
        let reference_rows = fetch_all_concurrently(reference_sqls, |_sql| async move {
            Ok(make_rows(&[1, 2, 3, 4]))
        })
        .await
        .unwrap();

        let labels_by_fp = HashMap::new();
        let chunked_series = group_rows(chunked_rows, &labels_by_fp);
        let reference_series = group_rows(reference_rows, &labels_by_fp);

        let to_vector = |series: &[FetchedSeries]| -> Vec<InstantSample> {
            series
                .iter()
                .map(|s| InstantSample {
                    labels: s.labels.clone(),
                    metric_name: None,
                    t_ms: 0,
                    v: s.samples[0].v,
                })
                .collect()
        };
        let chunked_sum = aggregation::aggregate(
            pulsus_promql::AggOp::Sum,
            &to_vector(&chunked_series),
            None,
            None,
        )
        .unwrap();
        let reference_sum = aggregation::aggregate(
            pulsus_promql::AggOp::Sum,
            &to_vector(&reference_series),
            None,
            None,
        )
        .unwrap();

        assert_eq!(chunked_sum, reference_sum);
        // Also pin the actual Neumaier-compensated value (the classic
        // large-value-cancellation case, math.rs's own golden): naive
        // summation of these four values in this order loses everything
        // but the finite terms; Neumaier recovers 1.0 + 2.0 = 3.0 exactly.
        assert_eq!(chunked_sum[0].v, 3.0);
    }

    #[test]
    fn to_promql_labels_converts_and_sorts() {
        let labels = to_promql_labels(&ls(&[("job", "api"), ("env", "prod")]));
        assert_eq!(
            labels.0,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("job".to_string(), "api".to_string()),
            ]
        );
    }

    #[test]
    fn group_rows_groups_contiguous_same_fingerprint_rows() {
        let rows = vec![
            SampleRow {
                fingerprint: 1,
                unix_milli: 0,
                value: 1.0,
            },
            SampleRow {
                fingerprint: 1,
                unix_milli: 1000,
                value: 2.0,
            },
            SampleRow {
                fingerprint: 2,
                unix_milli: 0,
                value: 5.0,
            },
        ];
        let mut labels_by_fp = HashMap::new();
        labels_by_fp.insert(1, ls(&[("job", "a")]));
        labels_by_fp.insert(2, ls(&[("job", "b")]));
        let series = group_rows(rows, &labels_by_fp);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].fingerprint, 1);
        assert_eq!(series[0].samples.len(), 2);
        assert_eq!(series[1].fingerprint, 2);
        assert_eq!(series[1].samples.len(), 1);
    }

    #[test]
    fn group_rows_of_an_empty_input_is_empty() {
        assert!(group_rows(Vec::new(), &HashMap::new()).is_empty());
    }

    #[test]
    fn group_rows_defaults_to_empty_labels_for_an_unhydrated_fingerprint() {
        let rows = vec![SampleRow {
            fingerprint: 1,
            unix_milli: 0,
            value: 1.0,
        }];
        let series = group_rows(rows, &HashMap::new());
        assert!(series[0].labels.is_empty());
    }

    #[test]
    fn parse_canonical_labels_round_trips_a_flat_object() {
        let set = parse_canonical_labels(r#"{"job":"api","env":"prod"}"#);
        assert_eq!(set.get("job"), Some("api"));
        assert_eq!(set.get("env"), Some("prod"));
    }

    #[test]
    fn parse_canonical_labels_of_empty_object_is_empty() {
        assert!(parse_canonical_labels("{}").is_empty());
    }

    #[test]
    fn vector_to_query_result_carries_labels_and_values() {
        let vector = vec![InstantSample {
            labels: Labels::new(vec![("job".to_string(), "api".to_string())]),
            metric_name: None,
            t_ms: 0,
            v: 3.0,
        }];
        match vector_to_query_result(vector) {
            QueryResult::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].value, 3.0);
                assert_eq!(v[0].labels, vec![("job".to_string(), "api".to_string())]);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    /// Issue #37 regression: a selector's `metric_name: Some(...)` must be
    /// spliced back in as `__name__` — the root cause of the bug this test
    /// pins against ever regressing.
    #[test]
    fn vector_to_query_result_splices_metric_name_as_dunder_name() {
        let vector = vec![InstantSample {
            labels: Labels::new(vec![("job".to_string(), "api".to_string())]),
            metric_name: Some("up".to_string()),
            t_ms: 0,
            v: 1.0,
        }];
        match vector_to_query_result(vector) {
            QueryResult::Vector(v) => {
                assert_eq!(v.len(), 1);
                assert!(
                    v[0].labels
                        .contains(&("__name__".to_string(), "up".to_string()))
                );
                assert!(
                    v[0].labels
                        .contains(&("job".to_string(), "api".to_string()))
                );
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn vector_to_query_result_omits_dunder_name_when_metric_name_is_none() {
        let vector = vec![InstantSample {
            labels: Labels::new(vec![("job".to_string(), "api".to_string())]),
            metric_name: None,
            t_ms: 0,
            v: 1.0,
        }];
        match vector_to_query_result(vector) {
            QueryResult::Vector(v) => {
                assert!(!v[0].labels.iter().any(|(k, _)| k == "__name__"));
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn value_to_query_result_maps_scalar() {
        match value_to_query_result(QueryValue::Scalar(42.0)) {
            QueryResult::Scalar(v) => assert_eq!(v, 42.0),
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    #[test]
    fn value_to_query_result_maps_matrix() {
        let matrix = vec![RangeSeries {
            labels: Labels::new(vec![("job".to_string(), "api".to_string())]),
            metric_name: None,
            points: vec![(0, 1.0), (1000, 2.0)],
        }];
        match value_to_query_result(QueryValue::Matrix(matrix)) {
            QueryResult::Matrix(m) => {
                assert_eq!(m.len(), 1);
                assert_eq!(m[0].points, vec![(0, 1.0), (1000, 2.0)]);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    /// Issue #37 regression: a `RangeSeries`'s `metric_name: Some(...)`
    /// (a plain selector's own range query, e.g. `up` over `[start,end]`)
    /// must be spliced back in as `__name__`, matching the instant-vector
    /// case above.
    #[test]
    fn value_to_query_result_matrix_splices_metric_name_as_dunder_name() {
        let matrix = vec![RangeSeries {
            labels: Labels::new(vec![("job".to_string(), "api".to_string())]),
            metric_name: Some("up".to_string()),
            points: vec![(0, 1.0), (1000, 2.0)],
        }];
        match value_to_query_result(QueryValue::Matrix(matrix)) {
            QueryResult::Matrix(m) => {
                assert_eq!(m.len(), 1);
                assert!(
                    m[0].labels
                        .contains(&("__name__".to_string(), "up".to_string()))
                );
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }
}
