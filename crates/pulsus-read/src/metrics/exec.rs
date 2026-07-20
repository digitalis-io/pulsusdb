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
use pulsus_model::{Fingerprint, LabelSet, NativeHistogram};
use pulsus_promql::parser::Expr;
use pulsus_promql::{
    DEFAULT_LOOKBACK_MS, FetchedSeries, InstantSample, Labels, PlanParams, QueryValue, RangeSeries,
    Sample, SelectorSpec, SeriesData,
};

use super::labels::{LabelledResolution, MetricSeriesGroup, MultiMetricResolution};
use super::matcher::{DataWindow, DiscoveryFilter};
use super::sample_rows::{HistSampleRow, MultiHistSampleRow, MultiSampleRow, SampleRow};
use super::sample_sql;
use crate::logql::error::{ReadError, TooBroadReason};
use crate::logql::exec::{
    HistMatrixSeries, HistOrFloat, HistVectorSample, MatrixSeries, QueryResult, VectorSample,
    escape_query_placeholders,
};
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
    /// `metric_hist_samples` (M7-A5a) — the dual-read's complementary
    /// native-histogram table, `_dist`-aware exactly like `samples_table`
    /// (co-sharded Metrics family). Every metrics fetch reads BOTH tables
    /// (compound-PK-pruned) and 2-way-merges by `unix_milli`; a single-type
    /// series' complementary read touches zero granules (EXPLAIN gate).
    pub hist_samples_table: String,
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
    /// Issue #65 (M6-02): `ReaderConfig::promql_experimental_functions`,
    /// threaded into every query's `PlanParams` by [`MetricsEngine::
    /// query_inner`] — the planner rejects experimental functions
    /// (`max_of`/`min_of`) by name when this is `false`.
    pub experimental_functions: bool,
    /// Issue #85 (M6-08c): `ReaderConfig::promql_max_metric_fanout` — the
    /// cap on how many metric names a single name-less/regex-`__name__`
    /// selector may fan out to (default 1000, the adjudicated value).
    /// Above it the query fails with the named
    /// [`crate::logql::error::TooBroadReason::MetricFanout`] error, never
    /// an unbounded `IN` set. Operator-scale tuning routes to issue #25.
    pub max_metric_fanout: u64,
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
    /// Builds the planner's [`PlanParams`]. `experimental_functions`
    /// comes from [`MetricsConfig::experimental_functions`] — passed in
    /// by the caller because this type carries only the request's own
    /// time span, not engine config (issue #65 plan v2 Δ4; `pub` so the
    /// server's production-path composition test can exercise the exact
    /// `ReaderConfig -> MetricsConfig -> PlanParams -> plan()` chain
    /// hermetically).
    pub fn plan_params(&self, experimental_functions: bool) -> PlanParams {
        PlanParams {
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            step_ms: self.step_ms,
            lookback_ms: DEFAULT_LOOKBACK_MS,
            experimental_functions,
        }
    }
}

pub struct MetricsEngine {
    client: ChClient,
    resolver: std::sync::Arc<super::labels::LabelCache>,
    config: MetricsConfig,
    /// Issue #101: the process-wide eval-concurrency permit bounding the
    /// one CPU-bound offload below (`evaluate_offloaded`). `new` seeds a
    /// throwaway default gate so the ~28 in-crate/live-test call sites need
    /// no change; production overrides it via [`MetricsEngine::with_eval_gate`]
    /// with the `AppState`-owned shared gate (an engine-local gate would
    /// reset every request — the engine is rebuilt per query — and bound
    /// nothing).
    eval_gate: std::sync::Arc<crate::eval_gate::EvalGate>,
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
            eval_gate: std::sync::Arc::new(crate::eval_gate::EvalGate::new(
                crate::eval_gate::DEFAULT_EVAL_CONCURRENCY,
            )),
        }
    }

    /// Issue #101: installs the shared process-wide eval-concurrency gate
    /// (owned by `AppState`, so the bound survives this engine's
    /// per-request rebuild). Not a `new` parameter — that would churn ~28
    /// test call sites for no behavioural gain (the throwaway default gate
    /// `new` seeds is a single negligible `Arc` alloc, dwarfed by the CH
    /// query).
    pub fn with_eval_gate(mut self, gate: std::sync::Arc<crate::eval_gate::EvalGate>) -> Self {
        self.eval_gate = gate;
        self
    }

    /// Returns the encoded result alongside its accumulated
    /// [`pulsus_promql::Annotations`] (M7-A5b-i) — empty for every
    /// float-only query (byte-identical to the pre-A5b-i behavior).
    pub async fn query(
        &self,
        expr: &Expr,
        p: &MetricQueryParams,
    ) -> Result<(QueryResult, pulsus_promql::Annotations), ReadError> {
        self.query_inner(expr, p, None).await
    }

    /// [`MetricsEngine::query`] plus its `X-Pulsus-Explain` trace, in the
    /// same single pass (no second execution) — mirrors
    /// [`crate::logql::LogQlEngine::query_explained`]'s contract.
    pub async fn query_explained(
        &self,
        expr: &Expr,
        p: &MetricQueryParams,
    ) -> Result<(QueryResult, pulsus_promql::Annotations, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("metrics");
        let (result, annotations) = self.query_inner(expr, p, Some(&mut explain)).await?;
        Ok((result, annotations, explain))
    }

    async fn query_inner(
        &self,
        expr: &Expr,
        p: &MetricQueryParams,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<(QueryResult, pulsus_promql::Annotations), ReadError> {
        let plan_params = p.plan_params(self.config.experimental_functions);
        let plan = pulsus_promql::plan(expr, plan_params)?;

        // Issue #33 architect adjudication (superseding #31's ratified
        // zero-ClickHouse `count`/`group` AC and #40's instant-only gate on
        // it): the cache-only fast path this comment used to describe has
        // been **removed**, not merely narrowed further. The label cache
        // resolves series presence at activity-*bucket* granularity (1h,
        // `DEFAULT_ACTIVITY_BUCKET_MS`), which cannot distinguish "had a
        // sample within the 5-minute PromQL staleness lookback" from
        // "active somewhere in an up-to-24h-old 1-hour bucket" — a
        // structural granularity gap no eligibility/age check on the cache
        // itself can close, proven live by the #33 differential
        // (`count(mem_usage_bytes{service="svc-0"})`: this engine returned
        // 69 including 12 series silent for over 5 minutes, Prometheus
        // correctly returned 57). `count`/`group` are ordinary PromQL
        // aggregation with exact lookback semantics
        // (architecture.md §5.1: "100% of semantics in Rust,
        // Prometheus-exact") — a value-matrix correctness concern, never
        // eligible for an approximate answer — so every `count`/`group`
        // query (instant and range alike) now always resolves → fetches
        // `metric_samples` → evaluates, where the evaluator applies the
        // real 5-minute lookback (`eval::staleness`) per step. One extra
        // fetch versus the old fast path; correct by construction.
        //
        // `QueryPlan::cache_answerable`/`CacheAnswerable` (the structural
        // predicate this branch used to consult) are deleted from
        // `pulsus-promql` entirely, not merely left unused — a predicate
        // that can never be lookback-correct at bucket granularity is a
        // latent trap for a future caller, not a dormant optimization
        // worth keeping around. `resolve_labelled` itself is unaffected —
        // the fetch path below (and issue #32's discovery endpoints) still
        // use it for label hydration/resolution.

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

            // Issue #85 (M6-08c): a selector without a single concrete
            // metric name resolves through the name-keyed cache into a
            // capped per-metric fan-out and ONE flat IN-set fetch.
            let Some(metric_name) = &sel.metric_name else {
                let fetch_plan = self.plan_multi_metric_fetch(
                    sel,
                    window,
                    lower_excl,
                    upper_incl,
                    explain.as_deref_mut(),
                )?;
                fetch_plans.push(fetch_plan);
                continue;
            };

            // A concrete-name selector may still carry non-Eq `__name__`
            // matchers (`up{__name__!~"..."}`) — evaluated once, here,
            // against the one concrete name.
            match super::labels::concrete_name_matches(&sel.name_matchers, metric_name) {
                Ok(true) => {}
                Ok(false) => {
                    if let Some(e) = explain.as_mut() {
                        e.push(
                            "series_resolution",
                            "name matchers exclude the selector's concrete metric name \
                             (empty result)"
                                .to_string(),
                            None,
                        );
                    }
                    fetch_plans.push(SelectorFetchPlan::Empty);
                    continue;
                }
                Err(reason) => {
                    return Err(ReadError::NamelessSelectorUnresolvable {
                        reason: format!("{reason:?}"),
                    });
                }
            }

            let resolution = self
                .resolver
                .resolve_labelled(metric_name, &sel.matchers, window);
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
                    let hist_sqls = build_hist_chunk_sqls(
                        &self.config.hist_samples_table,
                        metric_name,
                        fps.clone(),
                        lower_excl,
                        upper_incl,
                    );
                    let sqls = build_chunk_sqls(
                        &self.config.samples_table,
                        metric_name,
                        fps,
                        lower_excl,
                        upper_incl,
                    );
                    if let Some(e) = explain.as_mut() {
                        // Chunk elision (finding 5): only the first chunk's
                        // SQL is surfaced verbatim; a note names how many
                        // more chunks (and total fingerprints) were fetched
                        // identically, avoiding an O(chunks) explain blow-up
                        // for a selector matching thousands of series.
                        if let Some(first) = sqls.first() {
                            let note = (sqls.len() > 1).then(|| {
                                format!(
                                    "(+{} more chunks like this one, {total_fps} fingerprints total)",
                                    sqls.len() - 1
                                )
                            });
                            e.push("sample_fetch", first.clone(), note);
                        }
                        // M7-A5a: the complementary histogram read appears in
                        // the explain trace beside the float read (built once,
                        // synchronously — never drifts from what executes).
                        if let Some(first) = hist_sqls.first() {
                            e.push("hist_sample_fetch", first.clone(), None);
                        }
                    }
                    SelectorFetchPlan::Chunks {
                        sqls,
                        hist_sqls,
                        labels_by_fp,
                    }
                }
                LabelledResolution::SqlFallback { sql, .. } => {
                    let fetch_sql = sample_sql::sample_fetch_subquery(
                        &self.config.samples_table,
                        metric_name,
                        &sql,
                        lower_excl,
                        upper_incl,
                    );
                    let hist_sql = sample_sql::hist_sample_fetch_subquery(
                        &self.config.hist_samples_table,
                        metric_name,
                        &sql,
                        lower_excl,
                        upper_incl,
                    );
                    if let Some(e) = explain.as_mut() {
                        e.push("sample_fetch", fetch_sql.clone(), None);
                        e.push("hist_sample_fetch", hist_sql.clone(), None);
                    }
                    SelectorFetchPlan::Fallback {
                        sql: fetch_sql,
                        hist_sql,
                    }
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

        // Issue #93 (finding 2): `pulsus_promql::evaluate` is CPU-bound and
        // multi-hundred-ms at scale — offload it off the reactor onto the
        // blocking pool so a heavy range eval cannot stall a tokio worker
        // (the sole latency win here). `plan`/`data` are owned
        // (`QueryPlan`/`SeriesData` carry no lifetimes) and `Send + 'static`,
        // and `fetch_rows` fully drained every `ChRowStream` into a `Vec`
        // before `join_all` completed above, so NO pooled-connection lease
        // is held across this offload.
        //
        // Cancellation/concurrency bound (issue #101, hardening #93's Δ2):
        // tokio does NOT cancel a `spawn_blocking` task when its awaiter is
        // dropped, so a disconnected/timed-out client's eval still runs to
        // completion on the blocking pool. `self.eval_gate` (the shared
        // `AppState`-owned `EvalGate`) now bounds BOTH in-flight and queued
        // evals: the permit is acquired here — AFTER the `join_all` fetch
        // above fully drained every `ChRowStream`, so no pooled-connection
        // lease is ever held across the offload — and released only when the
        // blocking eval finishes (the owned permit lives inside the
        // `spawn_blocking` closure). Exhaustion is a bounded wait
        // (`acquire().await`), bounded by the upstream `TimeoutLayer` (408,
        // `query_timeout`); a timed-out/disconnected waiter releases its
        // queued reservation cleanly. No 429/503 and no new timeout knob.
        //
        // The only reachable `JoinError` is a PANIC in `evaluate`: we own
        // the handle and `.await` it directly (never `abort`, never drop it
        // early), so cancellation is unreachable. Re-raising the panic
        // preserves today's panic-on-bug behavior exactly (no new
        // `ReadError` variant — a panic is not a domain error).
        let (value, annotations) =
            evaluate_offloaded(&self.eval_gate, plan, data, pulsus_promql::evaluate).await?;
        Ok((value_to_query_result(value), annotations))
    }

    /// Issue #85 (M6-08c): builds a name-less/regex-`__name__` selector's
    /// fetch plan — resolve `(metric_name → fingerprints)` groups from
    /// the name-keyed cache (capped by `max_metric_fanout`), then render
    /// ONE flat `PREWHERE metric_name IN (…) … fingerprint IN (…)` fetch
    /// (each PK component prunes; see `sample_sql::sample_fetch_multi`'s
    /// soundness note and the `explain_indexes.rs` gate). A degraded
    /// cache is a named error, never an unbounded scan.
    fn plan_multi_metric_fetch(
        &self,
        sel: &SelectorSpec,
        window: DataWindow,
        lower_excl: i64,
        upper_incl: i64,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<SelectorFetchPlan, ReadError> {
        let resolution = self.resolver.resolve_multi_metric(
            &sel.name_matchers,
            &sel.matchers,
            window,
            self.config.max_metric_fanout,
        );
        let groups: Vec<MetricSeriesGroup> = match resolution {
            MultiMetricResolution::Groups(groups) => groups,
            MultiMetricResolution::Unresolvable { reason } => {
                return Err(ReadError::NamelessSelectorUnresolvable {
                    reason: format!("{reason:?}"),
                });
            }
            MultiMetricResolution::FanoutExceeded { matched, cap } => {
                return Err(ReadError::QueryTooBroad(TooBroadReason::MetricFanout {
                    matched,
                    cap,
                }));
            }
        };

        let total_series: usize = groups.iter().map(|g| g.series.len()).sum();
        if let Some(e) = explain.as_mut() {
            e.push(
                "series_resolution",
                format!(
                    "label cache: {total_series} matching series across {} metric names \
                     (name-less selector fan-out, cap {})",
                    groups.len(),
                    self.config.max_metric_fanout
                ),
                None,
            );
        }
        if groups.is_empty() {
            return Ok(SelectorFetchPlan::Empty);
        }

        // Group order is sorted-by-name (the resolver's contract) and
        // fingerprints are sorted within each group, so the rendered IN
        // lists — and therefore the explain trace — are deterministic.
        let names: Vec<String> = groups.iter().map(|g| g.metric_name.clone()).collect();
        let mut fps: Vec<Fingerprint> = groups
            .iter()
            .flat_map(|g| g.series.iter().map(|(fp, _)| *fp))
            .collect();
        fps.sort_unstable();
        fps.dedup();
        let mut labels_by: HashMap<(String, Fingerprint), LabelSet> = HashMap::new();
        // Cross-pair hydration source (code review round 1, finding 1):
        // `metric_fingerprint` excludes `__name__` (docs/schemas.md §2.1),
        // so a fingerprint's label set is name-invariant — any resolved
        // `(name', fp)` entry carries the exact labels of every genuine
        // `(name, fp)` cross-pair the flat IN×IN fetch may return that
        // the cache didn't resolve (a series registered under a second
        // name inside the sanctioned post-sweep recency gap).
        let mut labels_by_fp: HashMap<Fingerprint, LabelSet> = HashMap::new();
        for g in groups {
            for (fp, labels) in g.series {
                labels_by_fp.entry(fp).or_insert_with(|| labels.clone());
                labels_by.insert((g.metric_name.clone(), fp), labels);
            }
        }

        let sql = sample_sql::sample_fetch_multi(
            &self.config.samples_table,
            &names,
            &fps,
            lower_excl,
            upper_incl,
        );
        let hist_sql = sample_sql::hist_sample_fetch_multi(
            &self.config.hist_samples_table,
            &names,
            &fps,
            lower_excl,
            upper_incl,
        );
        if let Some(e) = explain.as_mut() {
            e.push("sample_fetch", sql.clone(), None);
            e.push("hist_sample_fetch", hist_sql.clone(), None);
        }
        Ok(SelectorFetchPlan::Multi {
            sql,
            hist_sql,
            labels_by,
            labels_by_fp,
        })
    }

    /// Executes one selector's already-built [`SelectorFetchPlan`]: the
    /// cache-hit path fetches every chunk's pre-built SQL concurrently;
    /// the `SqlFallback` path issues the single nested-subquery sample
    /// fetch, then hydrates labels for just the fingerprints that returned
    /// samples; the `Multi` path (issue #85) issues its single flat
    /// IN-set fetch and groups rows per `(metric_name, fingerprint)`.
    async fn execute_fetch_plan(
        &self,
        sel: &SelectorSpec,
        fetch_plan: SelectorFetchPlan,
    ) -> Result<Vec<FetchedSeries>, ReadError> {
        match fetch_plan {
            SelectorFetchPlan::Empty => Ok(Vec::new()),
            SelectorFetchPlan::Chunks {
                sqls,
                hist_sqls,
                labels_by_fp,
            } => {
                if sqls.is_empty() && hist_sqls.is_empty() {
                    return Ok(Vec::new());
                }
                let metric_name = concrete_name(sel)?;
                // M7-A5a: dispatch the float and complementary histogram
                // chunk reads CONCURRENTLY (A1 v5 latency-hiding — the
                // single-type complementary read is zero-granule but must
                // not add a serial round trip).
                let (rows, hist_rows) = fetch_dual_concurrently(
                    fetch_all_concurrently(sqls, |sql| self.fetch_rows::<SampleRow>(sql)),
                    fetch_all_concurrently(hist_sqls, |sql| self.fetch_rows::<HistSampleRow>(sql)),
                )
                .await?;
                group_merged_rows(rows, hist_rows, &labels_by_fp, metric_name)
            }
            SelectorFetchPlan::Fallback { sql, hist_sql } => {
                let metric_name = concrete_name(sel)?;
                let (rows, hist_rows): (Vec<SampleRow>, Vec<HistSampleRow>) =
                    fetch_dual_concurrently(self.fetch_rows(sql), self.fetch_rows(hist_sql))
                        .await?;
                if rows.is_empty() && hist_rows.is_empty() {
                    return Ok(Vec::new());
                }
                // Hydrate labels over the UNION of both reads' fingerprints
                // (M7-A5a): a histogram-only fingerprint must still hydrate,
                // or its series reaches the evaluator label-less.
                let mut fps: Vec<Fingerprint> = rows
                    .iter()
                    .map(|r| r.fingerprint)
                    .chain(hist_rows.iter().map(|r| r.fingerprint))
                    .collect();
                fps.sort_unstable();
                fps.dedup();
                let hydrate_sql = super::sql::series_labels_by_fingerprint(
                    &self.config.series_table,
                    metric_name,
                    &fps,
                );
                let series_rows: Vec<HydratedLabelsRow> = self.fetch_rows(hydrate_sql).await?;
                let labels_by_fp: HashMap<Fingerprint, LabelSet> = series_rows
                    .into_iter()
                    .map(|r| (r.fingerprint, parse_canonical_labels(&r.labels)))
                    .collect();
                group_merged_rows(rows, hist_rows, &labels_by_fp, metric_name)
            }
            SelectorFetchPlan::Multi {
                sql,
                hist_sql,
                labels_by,
                labels_by_fp,
            } => {
                let (rows, hist_rows): (Vec<MultiSampleRow>, Vec<MultiHistSampleRow>) =
                    fetch_dual_concurrently(self.fetch_rows(sql), self.fetch_rows(hist_sql))
                        .await?;
                group_merged_multi_rows(rows, hist_rows, &labels_by, &labels_by_fp)
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
    ///
    /// Issue #89: a filter carrying regex/negated `__name__` matchers
    /// instead routes through [`Self::discovery_sql_for`]'s cache-resolved
    /// flat IN×IN fetch — still one `metric_series` query per filter, still
    /// window-bound in SQL.
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
        // Resolve pre-pass (synchronous, in-process cache reads only) —
        // a filter whose name matchers can be answered statically
        // contributes no query at all. A regex/negated-`__name__` filter
        // against a degraded/cold cache yields a [`DiscoveryQuery::Probe`]
        // (issue #96) rather than a named error.
        let mut fetch_sqls: Vec<String> = Vec::with_capacity(effective.len());
        let mut probe_specs: Vec<ProbeSpec> = Vec::new();
        for filter in &effective {
            match self.discovery_query_for(filter, window, bucket_ms)? {
                Some(DiscoveryQuery::Sql(sql)) => fetch_sqls.push(sql),
                Some(DiscoveryQuery::Probe {
                    name_matchers,
                    matchers,
                }) => probe_specs.push(ProbeSpec {
                    name_matchers,
                    matchers,
                }),
                None => {}
            }
        }
        // Wave 1 (issue #96): run every degraded-cache name probe
        // concurrently; each non-empty probe result feeds the SAME flat
        // `metric_name IN (…)` fetch shape (with label matchers in SQL),
        // appended to the fetch set below. Scoped so no probe's pooled
        // connection lease survives into wave 2.
        if !probe_specs.is_empty() {
            let probe_futs = probe_specs
                .iter()
                .map(|spec| self.probe_distinct_names(&spec.name_matchers, window, bucket_ms));
            let probe_results: Vec<Result<Vec<String>, ReadError>> = join_all(probe_futs).await;
            for (names, spec) in probe_results.into_iter().zip(&probe_specs) {
                let names = names?;
                if !names.is_empty() {
                    fetch_sqls.push(super::sql::discovery_fetch_by_names(
                        &self.config.series_table,
                        &names,
                        &spec.matchers,
                        window,
                        bucket_ms,
                    ));
                }
            }
        }
        // Wave 2: fetch every (direct + probe-derived) query concurrently.
        let fetches = fetch_sqls
            .into_iter()
            .map(|sql| self.fetch_rows::<super::rows::SeriesRow>(sql));
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

    /// Builds one [`DiscoveryFilter`]'s `metric_series` query (issue #89's
    /// discovery/query-path selector parity), or `None` when the filter is
    /// statically empty and needs no round-trip at all. Three routes:
    ///
    /// - **concrete name** (`metric_name = Some(n)`): any non-`Eq`
    ///   `__name__` matchers are evaluated once against `n`
    ///   ([`super::labels::concrete_name_matches`], the query path's own
    ///   helper); a miss is `None`, a hit is the existing
    ///   [`super::sql::discovery_query`] — byte-unchanged.
    /// - **name-matcher-only** (`metric_name = None`, `name_matchers`
    ///   non-empty): candidate `(metric_name, fingerprint)` pairs resolve
    ///   in-process via [`super::labels::LabelCache::resolve_multi_metric`]
    ///   under the fan-out cap, then ONE
    ///   [`super::sql::discovery_fetch_multi`] fetch — the request window
    ///   re-applied there, so the cache's wider residency window cannot
    ///   leak into the response. A degraded cache is a named error, never
    ///   an unbounded scan (`MultiMetricResolution` has no SQL-fallback
    ///   variant).
    /// - **matcher-only / unfiltered** (`name_matchers` empty): the
    ///   existing unscoped [`super::sql::discovery_query`], byte-unchanged
    ///   and deliberately NOT routed through the fan-out cap —
    ///   `{job="api"}` must not fail on a deployment with many metric
    ///   names, and its SQL already prunes without a resolved name set.
    fn discovery_query_for(
        &self,
        filter: &DiscoveryFilter,
        window: DataWindow,
        bucket_ms: i64,
    ) -> Result<Option<DiscoveryQuery>, ReadError> {
        let discovery_query = || {
            DiscoveryQuery::Sql(super::sql::discovery_query(
                &self.config.series_table,
                filter,
                window,
                bucket_ms,
            ))
        };
        let Some(name) = filter.metric_name.as_deref() else {
            if filter.name_matchers.is_empty() {
                return Ok(Some(discovery_query()));
            }
            return self.discovery_multi_query(filter, window, bucket_ms);
        };
        match super::labels::concrete_name_matches(&filter.name_matchers, name) {
            Ok(true) => Ok(Some(discovery_query())),
            Ok(false) => Ok(None),
            Err(reason) => Err(ReadError::NamelessSelectorUnresolvable {
                reason: format!("{reason:?}"),
            }),
        }
    }

    /// The name-matcher discovery route (issue #89 warm path + issue #96
    /// degraded fallback): cache resolution (capped) -> one flat
    /// `metric_name IN (…) AND fingerprint IN (…)` fetch when the cache is
    /// authoritative; a degraded/cold cache ([`MultiMetricResolution::
    /// Unresolvable`]) instead yields a [`DiscoveryQuery::Probe`] (issue
    /// #96) — a bounded `SELECT DISTINCT metric_name` probe over
    /// `metric_series` whose sorted names feed the SAME flat fetch shape
    /// with label matchers in SQL. The cap-breach error mapping stays
    /// identical to [`Self::plan_multi_metric_fetch`] (the query path keeps
    /// its degraded `422`; only discovery falls back).
    fn discovery_multi_query(
        &self,
        filter: &DiscoveryFilter,
        window: DataWindow,
        bucket_ms: i64,
    ) -> Result<Option<DiscoveryQuery>, ReadError> {
        let resolution = self.resolver.resolve_multi_metric(
            &filter.name_matchers,
            &filter.matchers,
            window,
            self.config.max_metric_fanout,
        );
        let groups: Vec<MetricSeriesGroup> = match resolution {
            MultiMetricResolution::Groups(groups) => groups,
            MultiMetricResolution::Unresolvable { .. } => {
                // Issue #96: a degraded/cold cache (cold / stale / out-of-
                // window / regex-cache-full) no longer surfaces a named
                // `422` on the discovery path — it defers to a bounded SQL
                // probe over `metric_series`. Only the NAME matchers go to
                // the probe (names-only superset cap, adjudicated); the
                // label matchers apply in the downstream fetch.
                return Ok(Some(DiscoveryQuery::Probe {
                    name_matchers: filter.name_matchers.clone(),
                    matchers: filter.matchers.clone(),
                }));
            }
            MultiMetricResolution::FanoutExceeded { matched, cap } => {
                return Err(ReadError::QueryTooBroad(TooBroadReason::MetricFanout {
                    matched,
                    cap,
                }));
            }
        };
        if groups.is_empty() {
            return Ok(None);
        }
        // Group order is sorted-by-name and fingerprints are sorted within
        // each group (the resolver's contract), so the rendered IN lists
        // are deterministic.
        let names: Vec<String> = groups.iter().map(|g| g.metric_name.clone()).collect();
        let mut fps: Vec<Fingerprint> = groups
            .iter()
            .flat_map(|g| g.series.iter().map(|(fp, _)| *fp))
            .collect();
        fps.sort_unstable();
        fps.dedup();
        Ok(Some(DiscoveryQuery::Sql(
            super::sql::discovery_fetch_multi(
                &self.config.series_table,
                &names,
                &fps,
                window,
                bucket_ms,
            ),
        )))
    }

    /// Issue #96's degraded-cache discovery probe: resolves the candidate
    /// metric-name set for a regex/negated-`__name__` selector when the
    /// label cache cannot ([`MultiMetricResolution::Unresolvable`]). Runs
    /// the bounded [`super::sql::distinct_metric_names_probe`] (`SELECT
    /// DISTINCT metric_name … LIMIT cap+1`), then enforces the fan-out
    /// **bound** on the RETURNED rows: more than `cap` distinct names is
    /// [`TooBroadReason::MetricFanout`] (a names-only superset cap — the
    /// name regex is what bounds the scan; label matchers apply later in
    /// the fetch). Never an unbounded `IN` set. Returns sorted, deduped
    /// names; an empty set means no fetch at all. The probe is NOT
    /// EXPLAIN-index-gated (a regex `metric_name` predicate can't
    /// range-prune the leading primary-key column); its bound is the gate,
    /// its scan rows are recorded (issue #25 for scale wall-time).
    async fn probe_distinct_names(
        &self,
        name_matchers: &[super::matcher::LabelMatcher],
        window: DataWindow,
        bucket_ms: i64,
    ) -> Result<Vec<String>, ReadError> {
        let cap = self.config.max_metric_fanout;
        let sql = super::sql::distinct_metric_names_probe(
            &self.config.series_table,
            name_matchers,
            window,
            bucket_ms,
            cap,
        );
        let rows: Vec<super::rows::MetricNameRow> = self.fetch_rows(sql).await?;
        // `LIMIT cap+1` caps the returned rows: seeing more than `cap`
        // means the name predicate matched more distinct names than the
        // fan-out ceiling. `matched` is a lower bound (the probe stopped at
        // cap+1), mirroring the warm path's `FanoutExceeded` reporting.
        if rows.len() as u64 > cap {
            return Err(ReadError::QueryTooBroad(TooBroadReason::MetricFanout {
                matched: rows.len(),
                cap,
            }));
        }
        let mut names: Vec<String> = rows.into_iter().map(|r| r.metric_name).collect();
        names.sort_unstable();
        names.dedup();
        Ok(names)
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

/// One discovery filter's resolved plan (issue #89 + #96): either a ready
/// `metric_series` fetch SQL, or — for a regex/negated-`__name__` filter
/// against a degraded/cold cache — a deferred [`Self::Probe`] step whose
/// bounded `SELECT DISTINCT metric_name` probe resolves the candidate name
/// set before the fetch is built. Keeps `discovery_query_for` synchronous
/// (in-process cache reads only); the async probe runs in
/// `discovery_series`'s wave 1.
enum DiscoveryQuery {
    /// A ready-to-run `metric_series` fetch SQL (concrete-name, matcher-
    /// only, or the warm name-matcher [`super::sql::discovery_fetch_multi`]
    /// path).
    Sql(String),
    /// Issue #96: the degraded-cache name-matcher route — the name matchers
    /// bound a probe; the label matchers apply in the probe-derived fetch.
    Probe {
        name_matchers: Vec<super::matcher::LabelMatcher>,
        matchers: Vec<super::matcher::LabelMatcher>,
    },
}

/// A pending [`DiscoveryQuery::Probe`] carried from `discovery_series`'s
/// synchronous pre-pass into its concurrent wave-1 probe execution.
struct ProbeSpec {
    name_matchers: Vec<super::matcher::LabelMatcher>,
    matchers: Vec<super::matcher::LabelMatcher>,
}

/// A selector's fully pre-built fetch plan — built once, synchronously, in
/// `query_inner`'s phase-1 loop (so the actual generated SQL is available
/// for `X-Pulsus-Explain`, code review round 1 finding 5), then executed
/// in phase 2 without re-deriving anything.
enum SelectorFetchPlan {
    /// Cache-hit path: one `sample_fetch` SQL string per chunk (already
    /// ascending-fingerprint-sorted — see [`build_chunk_sqls`]), plus the
    /// paired complementary `metric_hist_samples` chunk SQLs (M7-A5a
    /// dual-read — same chunker/window/PK-prune, order-locked to `sqls`),
    /// plus the labels the cache already resolved.
    Chunks {
        sqls: Vec<String>,
        hist_sqls: Vec<String>,
        labels_by_fp: HashMap<Fingerprint, LabelSet>,
    },
    /// `SqlFallback` path: the single nested-subquery `sample_fetch` SQL
    /// and its paired complementary `metric_hist_samples` subquery fetch
    /// (M7-A5a) — labels are hydrated afterward from the UNION of
    /// fingerprints both reads return (a histogram-only fingerprint must
    /// still hydrate labels).
    Fallback { sql: String, hist_sql: String },
    /// Issue #85 (M6-08c): the name-less/regex-`__name__` fan-out — one
    /// flat `metric_name IN (…) AND fingerprint IN (…)` fetch, labels
    /// pre-resolved per `(metric_name, fingerprint)` (a fingerprint can
    /// exist under several metric names, so the map key must carry both).
    /// `labels_by_fp` is the cross-pair hydration source (code review
    /// round 1, finding 1): the IN×IN can return a genuine pair the cache
    /// didn't resolve (post-sweep recency gap); its labels are recovered
    /// from the fingerprint's name-invariant label set, never fabricated
    /// empty — see [`group_multi_rows`].
    Multi {
        sql: String,
        hist_sql: String,
        labels_by: HashMap<(String, Fingerprint), LabelSet>,
        labels_by_fp: HashMap<Fingerprint, LabelSet>,
    },
    /// Provably-empty selection with no fetch at all: a concrete-name
    /// selector whose `name_matchers` exclude its own name (issue #85),
    /// or a fan-out that matched zero metric names.
    Empty,
}

/// The concrete metric name a [`SelectorFetchPlan::Chunks`]/`Fallback`
/// plan was built for. Those variants are only ever built on the
/// `Some(metric_name)` branch of `query_inner`, so the `None` arm is a
/// documented impossibility kept as a descriptive error (never a panic).
fn concrete_name(sel: &SelectorSpec) -> Result<&str, ReadError> {
    sel.metric_name
        .as_deref()
        .ok_or_else(|| ReadError::NamelessSelectorUnresolvable {
            reason: "internal: a Chunks/Fallback fetch plan was built for a name-less \
                     selector (query_inner routes those to the Multi plan)"
                .to_string(),
        })
}

/// Issue #93 (finding 2): runs `pulsus_promql::evaluate` on the blocking
/// pool so its CPU-bound, multi-hundred-ms-at-scale work cannot stall a
/// tokio reactor worker. `plan`/`data` are owned + `Send + 'static` and
/// moved into the closure; the caller has already drained every
/// `ChRowStream` so no pooled-connection lease crosses this offload. The
/// sole reachable `JoinError` is a panic in `evaluate` (we own and directly
/// await the handle, never cancel), re-raised verbatim to preserve
/// panic-on-bug behavior — no new `ReadError` variant. Extracted so the
/// reactor-non-starvation gate (`tests::offloaded_evaluate_does_not_starve_
/// the_reactor`) exercises this exact code path.
///
/// Issue #101: the offload runs through `gate.run_blocking`, so the eval
/// holds an [`crate::eval_gate::EvalGate`] permit for the whole blocking
/// closure — bounding concurrent CPU-bound evals (including disconnected-
/// client evals tokio will not cancel).
///
/// The eval body is a closure parameter (`eval`), not hard-wired: production
/// passes [`pulsus_promql::evaluate`] (a zero-sized fn item — no runtime
/// cost, no hot-path instrumentation), while tests can inject an
/// eval-equivalent closure that observes concurrency *inside* the offloaded
/// blocking task. This is the only deterministic way to gate the "permit is
/// held for the DURATION of the blocking eval" property through this exact
/// function (the gate view alone cannot see it: a regression that dropped
/// the permit before `spawn_blocking` would make the gate look *idle* while
/// N+k evals ran, so the over-admission must be counted at the eval itself).
async fn evaluate_offloaded<F>(
    gate: &crate::eval_gate::EvalGate,
    plan: pulsus_promql::QueryPlan,
    data: pulsus_promql::SeriesData,
    eval: F,
) -> Result<(pulsus_promql::QueryValue, pulsus_promql::Annotations), ReadError>
where
    F: FnOnce(
            &pulsus_promql::QueryPlan,
            &pulsus_promql::SeriesData,
        ) -> Result<
            (pulsus_promql::QueryValue, pulsus_promql::Annotations),
            pulsus_promql::PromqlError,
        > + Send
        + 'static,
{
    match gate.run_blocking(move || eval(&plan, &data)).await {
        Ok(res) => Ok(res?),
        Err(join) => std::panic::resume_unwind(join.into_panic()),
    }
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

/// [`build_chunk_sqls`]'s M7-A5a histogram counterpart — same sort, same
/// chunker (`CHUNK_THRESHOLD`), same window; only the SELECT column list
/// and table name differ ([`sample_sql::hist_sample_fetch`]). Rendered over
/// the SAME fingerprint set so the paired chunk SQLs are index-aligned
/// (both prune the identical granules; the complementary read is zero-
/// granule for a pure-float fingerprint — the EXPLAIN gate).
fn build_hist_chunk_sqls(
    hist_samples_table: &str,
    metric_name: &str,
    mut fps: Vec<Fingerprint>,
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> Vec<String> {
    fps.sort_unstable();
    sample_sql::chunk_fingerprints(&fps, sample_sql::CHUNK_THRESHOLD)
        .into_iter()
        .map(|chunk| {
            sample_sql::hist_sample_fetch(
                hist_samples_table,
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
async fn fetch_all_concurrently<R, F, Fut>(sqls: Vec<String>, fetch: F) -> Result<Vec<R>, ReadError>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<Vec<R>, ReadError>>,
{
    let results: Vec<Result<Vec<R>, ReadError>> = join_all(sqls.into_iter().map(&fetch)).await;
    let mut rows = Vec::new();
    for r in results {
        rows.extend(r?);
    }
    Ok(rows)
}

/// M7-A5a: dispatches the float and complementary histogram reads
/// CONCURRENTLY and returns both results, mirroring the injectable
/// [`fetch_all_concurrently`] seam. Both futures are handed to
/// [`futures::future::join`], so both are in flight before either is
/// awaited — the A1 v5 latency-hiding contract (a single-type series' zero-
/// granule complementary read must not add a serial round trip). The
/// AC7b rendezvous-mock gate (`fetch_dual_concurrently_dispatches_both_*`)
/// proves the two are simultaneously in flight: a serial dispatch would
/// deadlock the two-sided barrier and trip the `tokio::time::timeout`.
async fn fetch_dual_concurrently<FF, HF, T, U>(float: FF, hist: HF) -> Result<(T, U), ReadError>
where
    FF: Future<Output = Result<T, ReadError>>,
    HF: Future<Output = Result<U, ReadError>>,
{
    let (float_res, hist_res) = futures::future::join(float, hist).await;
    Ok((float_res?, hist_res?))
}

/// Decodes a `metric_hist_samples` row's value columns into a
/// [`pulsus_model::FloatHistogram`] (M7-A5a decode, M7-A5b-i `to_float`
/// eval-boundary conversion) — `from_columns` **only**, never `validate`:
/// the A4 ingest seam validated before storing, so re-validating on the hot
/// read path is wasted work that would reject nothing new (trusted-storage
/// decode). A structural failure maps to [`ReadError::HistogramDecode`].
/// `to_float` runs here, once, so no integer histogram survives past this
/// function — every sample the value model carries downstream is
/// `FloatHistogram` (M7-A5b plan v3 finding 1).
fn decode_hist(
    cols: &pulsus_model::HistogramColumns,
) -> Result<pulsus_model::FloatHistogram, ReadError> {
    Ok(NativeHistogram::from_columns(cols)?.to_float())
}

/// The float half of a mergeable sample row (`SampleRow`/`MultiSampleRow`).
trait FloatPoint {
    fn unix_milli(&self) -> i64;
    fn value(&self) -> f64;
}

/// The histogram half of a mergeable sample row
/// (`HistSampleRow`/`MultiHistSampleRow`).
trait HistPoint {
    fn unix_milli(&self) -> i64;
    fn decode(&self) -> Result<pulsus_model::FloatHistogram, ReadError>;
}

impl FloatPoint for SampleRow {
    fn unix_milli(&self) -> i64 {
        self.unix_milli
    }
    fn value(&self) -> f64 {
        self.value
    }
}

impl FloatPoint for MultiSampleRow {
    fn unix_milli(&self) -> i64 {
        self.unix_milli
    }
    fn value(&self) -> f64 {
        self.value
    }
}

impl HistPoint for HistSampleRow {
    fn unix_milli(&self) -> i64 {
        self.unix_milli
    }
    fn decode(&self) -> Result<pulsus_model::FloatHistogram, ReadError> {
        decode_hist(&self.to_columns())
    }
}

impl HistPoint for MultiHistSampleRow {
    fn unix_milli(&self) -> i64 {
        self.unix_milli
    }
    fn decode(&self) -> Result<pulsus_model::FloatHistogram, ReadError> {
        decode_hist(&self.to_columns())
    }
}

/// Per-series 2-way merge by `unix_milli` with the histogram-wins tie-break
/// (M7-A5a). Both inputs are ascending by `unix_milli` (the fetch `ORDER
/// BY` contract). At `f < h` emit the float; at `f > h` emit the (decoded)
/// histogram; at `f == h` emit the HISTOGRAM and advance BOTH cursors — a
/// same-`(name, fp, unix_milli)` collision is a data error (one value type
/// per timestamp is the A1 v5 invariant), resolved deterministically in
/// the histogram's favour. Stale markers are PRESERVED here (not filtered)
/// so `windowed_non_stale`/`staleness` own staleness exactly as the float
/// path does today — keeping float output byte-identical.
fn merge_series<F: FloatPoint, H: HistPoint>(
    float: &[F],
    hist: &[H],
) -> Result<Vec<Sample>, ReadError> {
    let mut out = Vec::with_capacity(float.len() + hist.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < float.len() && j < hist.len() {
        let (ft, ht) = (float[i].unix_milli(), hist[j].unix_milli());
        if ft < ht {
            out.push(Sample::float(ft, float[i].value()));
            i += 1;
        } else if ft > ht {
            out.push(Sample::hist(ht, hist[j].decode()?));
            j += 1;
        } else {
            out.push(Sample::hist(ht, hist[j].decode()?));
            i += 1;
            j += 1;
        }
    }
    while i < float.len() {
        out.push(Sample::float(float[i].unix_milli(), float[i].value()));
        i += 1;
    }
    while j < hist.len() {
        out.push(Sample::hist(hist[j].unix_milli(), hist[j].decode()?));
        j += 1;
    }
    Ok(out)
}

/// Groups already fingerprint-ordered `rows` (the fetch `ORDER BY
/// fingerprint, unix_milli` contract) into [`FetchedSeries`], preserving
/// that order — never re-sorted via a `HashMap` (edge case 4/7: the
/// evaluator's Kahan accumulation order is pinned to ascending-fingerprint
/// input order, which must survive every merge step unchanged).
/// `metric_name` is the metric-scoped fetch's one concrete name, stamped
/// onto every series' per-series name channel (issue #85 —
/// `FetchedSeries::metric_name`).
fn group_rows(
    rows: Vec<SampleRow>,
    labels_by_fp: &HashMap<Fingerprint, LabelSet>,
    metric_name: &str,
) -> Vec<FetchedSeries> {
    let mut out: Vec<FetchedSeries> = Vec::new();
    for row in rows {
        let sample = Sample::float(row.unix_milli, row.value);
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
                    metric_name: Some(metric_name.to_string()),
                    labels: to_promql_labels(&labels),
                    samples: vec![sample],
                });
            }
        }
    }
    out
}

/// Issue #85 (M6-08c): [`group_rows`]'s multi-metric counterpart — rows
/// arrive `ORDER BY metric_name, fingerprint, unix_milli`, so consecutive
/// grouping on the `(metric_name, fingerprint)` pair yields one
/// [`FetchedSeries`] per matched series, each carrying its own name on
/// the per-series channel. Order stays deterministic (sorted names, then
/// ascending fingerprints) without any re-sort here.
///
/// **Labels are never fabricated (code review round 1, finding 1):** a
/// pair absent from `labels_by` is a genuine cross-pair the cache didn't
/// resolve (a series registered under a second metric name after the last
/// sweep — the sanctioned recency gap). Its labels are hydrated from
/// `labels_by_fp`: `metric_fingerprint` excludes `__name__`, so the
/// fingerprint's label set is name-invariant and already known from the
/// resolved sibling pair — and those labels passed the selector's
/// matchers (matchers apply uniformly across names, the v3 Δ2 soundness
/// argument), so the pair is a legitimate member of the matched set. A
/// fingerprint absent from *both* maps is structurally impossible (the
/// `IN` list is built from the resolved set) — skipped for totality, so
/// an empty-labels series can never reach the evaluator.
fn group_multi_rows(
    rows: Vec<MultiSampleRow>,
    labels_by: &HashMap<(String, Fingerprint), LabelSet>,
    labels_by_fp: &HashMap<Fingerprint, LabelSet>,
) -> Vec<FetchedSeries> {
    let mut out: Vec<FetchedSeries> = Vec::new();
    let mut current: Option<(String, Fingerprint)> = None;
    // Whether `current` produced an output series (false = the pair was
    // skipped, so its remaining rows must not attach to `out.last_mut()`,
    // which belongs to an earlier pair).
    let mut current_kept = false;
    for row in rows {
        let sample = Sample::float(row.unix_milli, row.value);
        let same = current
            .as_ref()
            .is_some_and(|(name, fp)| *name == row.metric_name && *fp == row.fingerprint);
        if same {
            if current_kept && let Some(last) = out.last_mut() {
                last.samples.push(sample);
            }
            continue;
        }
        let key = (row.metric_name, row.fingerprint);
        let labels = labels_by
            .get(&key)
            .or_else(|| labels_by_fp.get(&key.1))
            .cloned();
        current_kept = match labels {
            Some(labels) => {
                out.push(FetchedSeries {
                    fingerprint: row.fingerprint,
                    metric_name: Some(key.0.clone()),
                    labels: to_promql_labels(&labels),
                    samples: vec![sample],
                });
                true
            }
            None => false,
        };
        current = Some(key);
    }
    out
}

/// M7-A5a: [`group_rows`]'s dual-read counterpart — merges the float and
/// complementary histogram reads (both ascending by `fingerprint,
/// unix_milli`) into per-fingerprint [`FetchedSeries`], preserving the
/// ascending-fingerprint accumulation order (the Kahan input-order
/// invariant — never a `HashMap` re-sort). When `hist` is empty the merge
/// reduces to the float-only [`group_rows`] fast path, so a pure-float
/// selector's output is byte-identical to pre-A5a. Per series the two
/// ascending-`unix_milli` streams 2-way merge via [`merge_series`]
/// (histogram-wins tie-break).
fn group_merged_rows(
    float: Vec<SampleRow>,
    hist: Vec<HistSampleRow>,
    labels_by_fp: &HashMap<Fingerprint, LabelSet>,
    metric_name: &str,
) -> Result<Vec<FetchedSeries>, ReadError> {
    if hist.is_empty() {
        return Ok(group_rows(float, labels_by_fp, metric_name));
    }
    let mut out: Vec<FetchedSeries> = Vec::new();
    let (mut fi, mut hi) = (0usize, 0usize);
    while fi < float.len() || hi < hist.len() {
        // Next fingerprint = the smaller of the two cursors' heads; both
        // streams are ascending, so advancing past all of `next_fp`'s rows
        // on both sides keeps the walk ascending and non-repeating.
        let next_fp = match (float.get(fi), hist.get(hi)) {
            (Some(f), Some(h)) => f.fingerprint.min(h.fingerprint),
            (Some(f), None) => f.fingerprint,
            (None, Some(h)) => h.fingerprint,
            (None, None) => break,
        };
        let f_start = fi;
        while fi < float.len() && float[fi].fingerprint == next_fp {
            fi += 1;
        }
        let h_start = hi;
        while hi < hist.len() && hist[hi].fingerprint == next_fp {
            hi += 1;
        }
        let samples = merge_series(&float[f_start..fi], &hist[h_start..hi])?;
        let labels = labels_by_fp.get(&next_fp).cloned().unwrap_or_default();
        out.push(FetchedSeries {
            fingerprint: next_fp,
            metric_name: Some(metric_name.to_string()),
            labels: to_promql_labels(&labels),
            samples,
        });
    }
    Ok(out)
}

/// M7-A5a: [`group_multi_rows`]'s dual-read counterpart. Both reads arrive
/// `ORDER BY metric_name, fingerprint, unix_milli`; a two-cursor walk over
/// consecutive `(metric_name, fingerprint)` groups 2-way merges each
/// series (histogram-wins). The label hydration / cross-pair skip rules are
/// [`group_multi_rows`]'s exactly (an unresolved cross-pair hydrates from
/// the fingerprint's name-invariant labels; a wholly-unknown pair is
/// skipped). When `hist` is empty this reduces to the float-only
/// [`group_multi_rows`] fast path (byte-identical).
fn group_merged_multi_rows(
    float: Vec<MultiSampleRow>,
    hist: Vec<MultiHistSampleRow>,
    labels_by: &HashMap<(String, Fingerprint), LabelSet>,
    labels_by_fp: &HashMap<Fingerprint, LabelSet>,
) -> Result<Vec<FetchedSeries>, ReadError> {
    if hist.is_empty() {
        return Ok(group_multi_rows(float, labels_by, labels_by_fp));
    }
    let mut out: Vec<FetchedSeries> = Vec::new();
    let (mut fi, mut hi) = (0usize, 0usize);
    while fi < float.len() || hi < hist.len() {
        let key: (String, Fingerprint) = match (float.get(fi), hist.get(hi)) {
            (Some(f), Some(h)) => {
                if (f.metric_name.as_str(), f.fingerprint)
                    <= (h.metric_name.as_str(), h.fingerprint)
                {
                    (f.metric_name.clone(), f.fingerprint)
                } else {
                    (h.metric_name.clone(), h.fingerprint)
                }
            }
            (Some(f), None) => (f.metric_name.clone(), f.fingerprint),
            (None, Some(h)) => (h.metric_name.clone(), h.fingerprint),
            (None, None) => break,
        };
        let f_start = fi;
        while fi < float.len() && float[fi].metric_name == key.0 && float[fi].fingerprint == key.1 {
            fi += 1;
        }
        let h_start = hi;
        while hi < hist.len() && hist[hi].metric_name == key.0 && hist[hi].fingerprint == key.1 {
            hi += 1;
        }
        // Labels: the resolved `(name, fp)` pair, else the fingerprint's
        // name-invariant set; a pair absent from both is a structural
        // impossibility (the IN list is built from the resolved set) —
        // skipped for totality (matches `group_multi_rows`).
        let Some(labels) = labels_by
            .get(&key)
            .or_else(|| labels_by_fp.get(&key.1))
            .cloned()
        else {
            continue;
        };
        let samples = merge_series(&float[f_start..fi], &hist[h_start..hi])?;
        out.push(FetchedSeries {
            fingerprint: key.1,
            metric_name: Some(key.0),
            labels: to_promql_labels(&labels),
            samples,
        });
    }
    Ok(out)
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

/// Converts the evaluator's [`QueryValue`] to the read-side
/// [`QueryResult`] — the sole `QueryValue`→wire chokepoint in the metrics
/// read path.
///
/// **M7-A5b-i histogram encoding (plan v3 finding 1, replacing the A5a
/// `HistogramResultUnsupported` reject):** a `Vector`/`Matrix` carrying at
/// least one histogram-valued element/point routes through
/// [`QueryResult::VectorHist`]/[`QueryResult::MatrixHist`] (the
/// `pulsus_model::FloatHistogram`-carrying siblings — `prom_api::encode`
/// walks `FloatHistogram::all_bucket_iterator`-equivalent
/// (`all_buckets`) to render the Prometheus `histogram` JSON shape); an
/// all-float `Vector`/`Matrix` takes the EXACT SAME path as before
/// (`vector_to_query_result`/the plain `QueryResult::Matrix` arm), so float
/// output stays byte-identical (AC5) and no path emits `0.0` for a
/// histogram.
fn value_to_query_result(value: QueryValue) -> QueryResult {
    match value {
        QueryValue::Vector(v) => {
            if v.iter().any(|s| s.h.is_some()) {
                QueryResult::VectorHist(
                    v.into_iter()
                        .map(|s| HistVectorSample {
                            labels: with_metric_name(s.labels, s.metric_name),
                            value: match s.h {
                                Some(h) => HistOrFloat::Hist(h),
                                None => HistOrFloat::Float(s.v),
                            },
                        })
                        .collect(),
                )
            } else {
                vector_to_query_result(v)
            }
        }
        QueryValue::Matrix(m) => {
            if m.iter().any(|s| s.points.iter().any(|p| p.h.is_some())) {
                QueryResult::MatrixHist(
                    m.into_iter()
                        .map(|s: RangeSeries| HistMatrixSeries {
                            labels: with_metric_name(s.labels, s.metric_name),
                            points: s
                                .points
                                .into_iter()
                                .map(|p| {
                                    let v = match p.h {
                                        Some(h) => HistOrFloat::Hist(h),
                                        None => HistOrFloat::Float(p.v),
                                    };
                                    (p.t_ms, v)
                                })
                                .collect(),
                        })
                        .collect(),
                )
            } else {
                QueryResult::Matrix(
                    m.into_iter()
                        .map(|s: RangeSeries| MatrixSeries {
                            labels: with_metric_name(s.labels, s.metric_name),
                            points: s.points.into_iter().map(|p| (p.t_ms, p.v)).collect(),
                        })
                        .collect(),
                )
            }
        }
        QueryValue::Scalar(v) => QueryResult::Scalar(v),
        // Issue #86 (M6-08d): a top-level string-literal query — value
        // only; the encoder stamps the eval-time timestamp (the Scalar
        // precedent).
        QueryValue::String(s) => QueryResult::String(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_promql::Point;
    use pulsus_promql::eval::aggregation;

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
        let chunked_series = group_rows(chunked_rows, &labels_by_fp, "m");
        let reference_series = group_rows(reference_rows, &labels_by_fp, "m");

        let to_vector = |series: &[FetchedSeries]| -> Vec<InstantSample> {
            series
                .iter()
                .map(|s| InstantSample {
                    labels: s.labels.clone(),
                    metric_name: None,
                    drop_name: false,
                    t_ms: 0,
                    v: s.samples[0].v,
                    h: None,
                })
                .collect()
        };
        let chunked_sum = aggregation::aggregate(
            pulsus_promql::AggOp::Sum,
            &to_vector(&chunked_series),
            None,
            None,
            &mut pulsus_promql::Annotations::new(),
        )
        .unwrap();
        let reference_sum = aggregation::aggregate(
            pulsus_promql::AggOp::Sum,
            &to_vector(&reference_series),
            None,
            None,
            &mut pulsus_promql::Annotations::new(),
        )
        .unwrap();

        assert_eq!(chunked_sum, reference_sum);
        // Also pin the actual Neumaier-compensated value (the classic
        // large-value-cancellation case, math.rs's own golden): naive
        // summation of these four values in this order loses everything
        // but the finite terms; Neumaier recovers 1.0 + 2.0 = 3.0 exactly.
        assert_eq!(chunked_sum[0].v, 3.0);
    }

    /// Builds a deliberately heavy in-memory range eval (a group_right
    /// binop over many series × steps) — a synthetic, ClickHouse-free
    /// stand-in for a multi-hundred-ms production query. Rebuilt per call
    /// (rather than cloned) so each arm of the reactor gate gets its own
    /// owned `plan`/`data`.
    fn heavy_eval_fixture() -> (pulsus_promql::QueryPlan, pulsus_promql::SeriesData) {
        use pulsus_promql::{FetchedSeries, Labels, PlanParams, Sample, SeriesData};
        const GROUPS: usize = 8;
        const MANY: usize = 8;
        const STEPS: i64 = 200;
        const STEP_MS: i64 = 15_000;
        let params = PlanParams {
            start_ms: 0,
            end_ms: (STEPS - 1) * STEP_MS,
            step_ms: STEP_MS,
            lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
            experimental_functions: false,
        };
        let expr = pulsus_promql::parse("foo / on(g) group_right bar").unwrap();
        let plan = pulsus_promql::plan(&expr, params).unwrap();
        let samples = |base: f64| -> Vec<Sample> {
            (0..STEPS)
                .map(|k| Sample::float(k * STEP_MS, base + k as f64))
                .collect()
        };
        let mut data = SeriesData::new();
        for sel in &plan.selectors {
            let name = sel.metric_name.clone().unwrap();
            let mut series = Vec::new();
            if name == "foo" {
                for g in 0..GROUPS {
                    series.push(FetchedSeries {
                        fingerprint: g as u64,
                        metric_name: Some("foo".to_string()),
                        labels: Labels::new([("g".to_string(), format!("g{g}"))]),
                        samples: samples(1.0),
                    });
                }
            } else {
                let mut fp = 100_000u64;
                for g in 0..GROUPS {
                    for m in 0..MANY {
                        series.push(FetchedSeries {
                            fingerprint: fp,
                            metric_name: Some("bar".to_string()),
                            labels: Labels::new([
                                ("g".to_string(), format!("g{g}")),
                                ("inst".to_string(), format!("i{m}")),
                                ("region".to_string(), "us-east-1".to_string()),
                            ]),
                            samples: samples(2.0),
                        });
                        fp += 1;
                    }
                }
            }
            data.insert(sel.id, series);
        }
        (plan, data)
    }

    /// Issue #93 (finding 2 — the reactor-non-starvation gate): on a
    /// SINGLE-THREADED (`current_thread`) runtime, a concurrently-spawned
    /// cooperative task can only run when the driving future YIELDS to the
    /// scheduler. `evaluate_offloaded` awaits `spawn_blocking`, so it
    /// yields while the CPU-bound eval runs on the blocking pool — the
    /// concurrent task is polled and makes progress DURING the eval. The
    /// contrast arm runs the SAME eval INLINE (await-free, exactly the
    /// pre-#93 shape): it never yields, so the concurrent task is starved —
    /// the failure mode the fix removes. Both assertions are booleans
    /// ("did it make progress at all"), never a wall-time bound, so the
    /// gate is robust on a loaded CI box.
    #[tokio::test(flavor = "current_thread")]
    async fn offloaded_evaluate_does_not_starve_the_reactor() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        // --- OFFLOAD path (the fix): the eval runs off-reactor, so the
        // --- concurrent task completes while it is in flight.
        let (plan, data) = heavy_eval_fixture();
        let progressed = Arc::new(AtomicBool::new(false));
        let flag = progressed.clone();
        tokio::spawn(async move { flag.store(true, Ordering::SeqCst) });
        // No `.await` between the spawn and here: the task cannot have run
        // yet on a current_thread runtime.
        assert!(
            !progressed.load(Ordering::SeqCst),
            "sanity: the spawned task must not run before the driver yields"
        );
        let gate = crate::eval_gate::EvalGate::new(crate::eval_gate::DEFAULT_EVAL_CONCURRENCY);
        let out = evaluate_offloaded(&gate, plan, data, pulsus_promql::evaluate)
            .await
            .unwrap();
        assert!(
            progressed.load(Ordering::SeqCst),
            "the concurrent task made progress during the offloaded eval — the reactor stayed live"
        );
        assert!(
            matches!(out.0, pulsus_promql::QueryValue::Matrix(ref m) if !m.is_empty()),
            "the heavy fixture must produce a non-empty matrix"
        );

        // --- CONTRAST (failure-mode proof): the identical eval run INLINE
        // --- on this runtime never yields, so the concurrent task starves.
        let (plan, data) = heavy_eval_fixture();
        let starved = Arc::new(AtomicBool::new(false));
        let flag = starved.clone();
        tokio::spawn(async move { flag.store(true, Ordering::SeqCst) });
        let inline = pulsus_promql::evaluate(&plan, &data).unwrap();
        assert!(
            !starved.load(Ordering::SeqCst),
            "inline (non-offloaded) eval starves the reactor: the concurrent task must NOT have run"
        );
        std::hint::black_box(&inline);
    }

    /// Issue #101 (AC6 — production wiring, made deterministic per the plan
    /// review): `evaluate_offloaded` takes an eval permit for the whole
    /// eval and releases it after. Proven without any wall-time race by
    /// holding the sole permit of an `EvalGate::new(1)` first, starting
    /// `evaluate_offloaded` (which must therefore QUEUE — `waiting == 1`),
    /// then releasing the held permit and asserting the eval completes and
    /// the permit is returned (`available == 1`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn offloaded_evaluate_holds_a_permit_for_the_eval_and_releases_it() {
        use std::sync::Arc;

        let gate = Arc::new(crate::eval_gate::EvalGate::new(1));
        // Hold the sole permit so the eval below is forced to queue.
        let held = gate.acquire().await;

        let (plan, data) = heavy_eval_fixture();
        let g = Arc::clone(&gate);
        let handle = tokio::spawn(async move {
            evaluate_offloaded(&g, plan, data, pulsus_promql::evaluate)
                .await
                .unwrap()
        });

        // The eval cannot start while we hold the permit: it is queued.
        loop {
            if gate.snapshot().waiting == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            gate.snapshot().available,
            0,
            "the held permit blocks the eval from starting"
        );

        // Release the permit; the eval now runs to completion.
        drop(held);
        let out = handle.await.unwrap();
        assert!(
            matches!(out.0, pulsus_promql::QueryValue::Matrix(ref m) if !m.is_empty()),
            "the heavy fixture must produce a non-empty matrix"
        );
        assert_eq!(
            gate.snapshot().available,
            1,
            "the eval permit is returned after the blocking eval finishes"
        );
    }

    /// Issue #101 (AC6, strengthened per code-review round 1): proves the
    /// permit is held for the WHOLE DURATION of the blocking eval *inside*
    /// `evaluate_offloaded`, not merely queued-before / released-after. The
    /// prior AC6 assertion could not see mid-eval state, so an
    /// acquire-release-before-`spawn_blocking` regression in
    /// `evaluate_offloaded` would pass it. This is AC2's counting-gate shape
    /// driven THROUGH `evaluate_offloaded`: `N + K` concurrent calls share an
    /// `EvalGate::new(N)`, and each injected eval closure counts the evals
    /// concurrently in flight *inside the offloaded blocking task* via a
    /// `fetch_max` (read only after every task joins — no race, no wall-time
    /// assert). If the permit were dropped before the spawn, all `N + K`
    /// eval closures would run at once and `max_seen` would exceed `N`. The
    /// closure ends by running the real `pulsus_promql::evaluate`, so the
    /// integration still exercises actual eval work.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn offloaded_evaluate_holds_its_permit_for_the_whole_eval_bounding_concurrency() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::time::Duration;
        use tokio::sync::Semaphore as TokioSemaphore;

        const N: usize = 2;
        const K: usize = 3;

        let gate = Arc::new(crate::eval_gate::EvalGate::new(N));
        let release = Arc::new(AtomicBool::new(false));
        // Counts eval closures that have started running (i.e. hold a permit).
        let entered = Arc::new(TokioSemaphore::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..(N + K) {
            let gate = Arc::clone(&gate);
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            let in_flight = Arc::clone(&in_flight);
            let max_seen = Arc::clone(&max_seen);
            let (plan, data) = heavy_eval_fixture();
            handles.push(tokio::spawn(async move {
                evaluate_offloaded(&gate, plan, data, move |plan, data| {
                    // Runs on the blocking pool while the permit is held.
                    let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(cur, Ordering::SeqCst);
                    entered.add_permits(1);
                    while !release.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    pulsus_promql::evaluate(plan, data)
                })
                .await
                .unwrap()
            }));
        }

        // Wait until exactly N eval closures are running (parked on the
        // release flag); the K extra provably queue at the gate.
        let admitted = entered.acquire_many(N as u32).await.unwrap();
        assert_eq!(
            entered.available_permits(),
            0,
            "only N eval closures may run inside evaluate_offloaded while the gate is full"
        );
        assert!(
            max_seen.load(Ordering::SeqCst) <= N,
            "evaluate_offloaded must never run more than N eval closures concurrently"
        );
        assert_eq!(
            gate.snapshot().available,
            0,
            "the gate is fully occupied by the in-flight evals"
        );

        // Release everyone and let all N+K run to completion.
        drop(admitted);
        release.store(true, Ordering::SeqCst);
        for h in handles {
            let out = h.await.unwrap();
            assert!(
                matches!(out.0, pulsus_promql::QueryValue::Matrix(ref m) if !m.is_empty()),
                "each offloaded eval must produce the heavy fixture's non-empty matrix"
            );
        }

        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            N,
            "the bound is tight: exactly N evals ran concurrently inside evaluate_offloaded"
        );
        assert_eq!(
            gate.snapshot().available,
            N,
            "every eval permit is returned after evaluate_offloaded completes"
        );
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
        let series = group_rows(rows, &labels_by_fp, "up");
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].fingerprint, 1);
        assert_eq!(series[0].samples.len(), 2);
        assert_eq!(series[1].fingerprint, 2);
        assert_eq!(series[1].samples.len(), 1);
    }

    #[test]
    fn group_rows_of_an_empty_input_is_empty() {
        assert!(group_rows(Vec::new(), &HashMap::new(), "up").is_empty());
    }

    #[test]
    fn group_rows_defaults_to_empty_labels_for_an_unhydrated_fingerprint() {
        let rows = vec![SampleRow {
            fingerprint: 1,
            unix_milli: 0,
            value: 1.0,
        }];
        let series = group_rows(rows, &HashMap::new(), "up");
        assert!(series[0].labels.is_empty());
    }

    // --- group_multi_rows (issue #85, M6-08c) ---

    #[test]
    fn group_multi_rows_splits_a_shared_fingerprint_across_metric_names() {
        // The same fingerprint under two metric names (legal:
        // metric_fingerprint excludes __name__) must yield TWO series,
        // each with its own per-series name.
        let rows = vec![
            MultiSampleRow {
                metric_name: "aaa".to_string(),
                fingerprint: 7,
                unix_milli: 0,
                value: 1.0,
            },
            MultiSampleRow {
                metric_name: "aaa".to_string(),
                fingerprint: 7,
                unix_milli: 1_000,
                value: 2.0,
            },
            MultiSampleRow {
                metric_name: "bbb".to_string(),
                fingerprint: 7,
                unix_milli: 0,
                value: 9.0,
            },
        ];
        let mut labels_by = HashMap::new();
        labels_by.insert(("aaa".to_string(), 7), ls(&[("job", "a")]));
        labels_by.insert(("bbb".to_string(), 7), ls(&[("job", "a")]));
        let mut labels_by_fp = HashMap::new();
        labels_by_fp.insert(7, ls(&[("job", "a")]));
        let series = group_multi_rows(rows, &labels_by, &labels_by_fp);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].metric_name.as_deref(), Some("aaa"));
        assert_eq!(series[0].samples.len(), 2);
        assert_eq!(series[1].metric_name.as_deref(), Some("bbb"));
        assert_eq!(series[1].samples.len(), 1);
    }

    #[test]
    fn group_multi_rows_of_an_empty_input_is_empty() {
        assert!(group_multi_rows(Vec::new(), &HashMap::new(), &HashMap::new()).is_empty());
    }

    /// Code review round 1, finding 1: a genuine cross-pair the cache
    /// didn't resolve (`(bbb, 7)` absent from `labels_by`) hydrates from
    /// the fingerprint's name-invariant labels — NEVER an empty label
    /// set.
    #[test]
    fn group_multi_rows_hydrates_an_unresolved_cross_pair_from_the_fingerprint_labels() {
        let rows = vec![
            MultiSampleRow {
                metric_name: "aaa".to_string(),
                fingerprint: 7,
                unix_milli: 0,
                value: 1.0,
            },
            MultiSampleRow {
                metric_name: "bbb".to_string(),
                fingerprint: 7,
                unix_milli: 0,
                value: 2.0,
            },
        ];
        let mut labels_by = HashMap::new();
        labels_by.insert(("aaa".to_string(), 7), ls(&[("job", "a")]));
        let mut labels_by_fp = HashMap::new();
        labels_by_fp.insert(7, ls(&[("job", "a")]));
        let series = group_multi_rows(rows, &labels_by, &labels_by_fp);
        assert_eq!(series.len(), 2);
        assert_eq!(series[1].metric_name.as_deref(), Some("bbb"));
        assert_eq!(
            series[1].labels.get("job"),
            Some("a"),
            "cross-pair labels hydrated from the fingerprint, not empty: {series:?}"
        );
    }

    /// Finding 1's totality arm: a fingerprint absent from BOTH maps
    /// (structurally impossible — the IN list is built from the resolved
    /// set) is skipped whole, including its follow-on rows, which must
    /// not attach to the preceding series.
    #[test]
    fn group_multi_rows_skips_a_wholly_unknown_pair_and_all_its_rows() {
        let rows = vec![
            MultiSampleRow {
                metric_name: "aaa".to_string(),
                fingerprint: 7,
                unix_milli: 0,
                value: 1.0,
            },
            MultiSampleRow {
                metric_name: "bbb".to_string(),
                fingerprint: 9, // unknown to both maps
                unix_milli: 0,
                value: 2.0,
            },
            MultiSampleRow {
                metric_name: "bbb".to_string(),
                fingerprint: 9,
                unix_milli: 1_000,
                value: 3.0,
            },
        ];
        let mut labels_by = HashMap::new();
        labels_by.insert(("aaa".to_string(), 7), ls(&[("job", "a")]));
        let mut labels_by_fp = HashMap::new();
        labels_by_fp.insert(7, ls(&[("job", "a")]));
        let series = group_multi_rows(rows, &labels_by, &labels_by_fp);
        assert_eq!(series.len(), 1, "unknown pair never surfaces: {series:?}");
        assert_eq!(series[0].metric_name.as_deref(), Some("aaa"));
        assert_eq!(
            series[0].samples.len(),
            1,
            "the skipped pair's rows must not leak into the previous series"
        );
    }

    #[test]
    fn group_rows_stamps_the_concrete_metric_name_on_every_series() {
        let rows = vec![SampleRow {
            fingerprint: 1,
            unix_milli: 0,
            value: 1.0,
        }];
        let series = group_rows(rows, &HashMap::new(), "up");
        assert_eq!(series[0].metric_name.as_deref(), Some("up"));
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
            drop_name: false,
            t_ms: 0,
            v: 3.0,
            h: None,
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
            drop_name: false,
            t_ms: 0,
            v: 1.0,
            h: None,
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
            drop_name: false,
            t_ms: 0,
            v: 1.0,
            h: None,
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

    /// Issue #86 (M6-08d, plan v2 Δ5): a top-level string-literal query
    /// maps value-only — the encoder stamps the eval-time timestamp
    /// externally (the Scalar precedent).
    #[test]
    fn value_to_query_result_maps_string() {
        match value_to_query_result(QueryValue::String("Foo".to_string())) {
            QueryResult::String(s) => assert_eq!(s, "Foo"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn value_to_query_result_maps_matrix() {
        let matrix = vec![RangeSeries {
            labels: Labels::new(vec![("job".to_string(), "api".to_string())]),
            metric_name: None,
            drop_name: false,
            points: vec![Point::float(0, 1.0), Point::float(1000, 2.0)],
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
            drop_name: false,
            points: vec![Point::float(0, 1.0), Point::float(1000, 2.0)],
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

    // ===================================================================
    // M7-A5a: dual-read merge + decode + concurrency + histogram rejection
    // ===================================================================

    use pulsus_model::{NativeHistogram, STALE_NAN_BITS, Span};

    /// `single_histogram` (`native_histograms.test:34`, A3 corpus fixture).
    fn single_histogram() -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, 1, -1],
            negative_buckets: vec![],
            custom_values: vec![],
        }
    }

    /// `custom_buckets_histogram` (`native_histograms.test:1078`, NHCB).
    fn custom_buckets_histogram() -> NativeHistogram {
        NativeHistogram {
            schema: -53,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, 1, -1],
            negative_buckets: vec![],
            custom_values: vec![5.0, 10.0],
        }
    }

    fn float_row(fp: u64, t: i64, v: f64) -> SampleRow {
        SampleRow {
            fingerprint: fp,
            unix_milli: t,
            value: v,
        }
    }

    fn hist_row(fp: u64, t: i64, h: &NativeHistogram) -> HistSampleRow {
        let c = h.to_columns().expect("to_columns");
        HistSampleRow {
            fingerprint: fp,
            unix_milli: t,
            schema: c.schema,
            zero_threshold: c.zero_threshold,
            zero_count: c.zero_count,
            count: c.count,
            sum: c.sum,
            pos_span_offsets: c.pos_span_offsets,
            pos_span_lengths: c.pos_span_lengths,
            pos_bucket_deltas: c.pos_bucket_deltas,
            neg_span_offsets: c.neg_span_offsets,
            neg_span_lengths: c.neg_span_lengths,
            neg_bucket_deltas: c.neg_bucket_deltas,
            custom_values: c.custom_values,
        }
    }

    // -- AC2: merge correctness, histogram-wins, lossless decode --

    #[test]
    fn decode_hist_round_trips_a_hist_row_bit_for_bit() {
        for h in [single_histogram(), custom_buckets_histogram()] {
            let row = hist_row(1, 0, &h);
            let back = decode_hist(&row.to_columns()).expect("decode");
            assert!(
                back.bits_eq(&h.to_float()),
                "round-trip mismatch: {back:?} != {h:?}"
            );
        }
    }

    #[test]
    fn merge_series_interleaves_float_and_histogram_by_unix_milli() {
        // float at 0,20; hist at 10 — merged ascending 0(f),10(h),20(f).
        let hist = single_histogram();
        let float = [float_row(1, 0, 1.0), float_row(1, 20, 2.0)];
        let h = [hist_row(1, 10, &hist)];
        let merged = merge_series(&float, &h).unwrap();
        assert_eq!(merged.len(), 3);
        assert_eq!((merged[0].t_ms, merged[0].h.is_none()), (0, true));
        assert_eq!(merged[0].v, 1.0);
        assert_eq!(merged[1].t_ms, 10);
        assert!(merged[1].h.as_deref().unwrap().bits_eq(&hist.to_float()));
        assert_eq!((merged[2].t_ms, merged[2].h.is_none()), (20, true));
    }

    #[test]
    fn merge_series_histogram_wins_at_an_equal_timestamp() {
        // Same unix_milli in both streams: the histogram is emitted, the
        // float dropped, and BOTH cursors advance (one value per timestamp).
        let hist = single_histogram();
        let float = [float_row(1, 10, 99.0)];
        let h = [hist_row(1, 10, &hist)];
        let merged = merge_series(&float, &h).unwrap();
        assert_eq!(merged.len(), 1, "the float at the collision is dropped");
        assert_eq!(merged[0].t_ms, 10);
        assert!(merged[0].h.as_deref().unwrap().bits_eq(&hist.to_float()));
    }

    #[test]
    fn merge_series_preserves_a_stale_nan_histogram_marker() {
        // A4 encodes histogram staleness as sum = STALE_NAN_BITS; the merge
        // must NOT drop it (staleness is owned at the eval layer).
        let mut stale = single_histogram();
        stale.sum = f64::from_bits(STALE_NAN_BITS);
        let merged = merge_series::<SampleRow, _>(&[], &[hist_row(1, 5, &stale)]).unwrap();
        assert_eq!(merged.len(), 1);
        assert!(merged[0].is_stale());
        assert_eq!(
            merged[0].h.as_deref().unwrap().sum.to_bits(),
            STALE_NAN_BITS
        );
    }

    #[test]
    fn group_merged_rows_builds_float_and_histogram_series() {
        // fp 1: float-only; fp 2: histogram-only. Ascending-fp order kept.
        let hist = single_histogram();
        let float = vec![float_row(1, 0, 1.0), float_row(1, 10, 2.0)];
        let h = vec![hist_row(2, 0, &hist)];
        let mut labels = HashMap::new();
        labels.insert(1u64, ls(&[("job", "a")]));
        labels.insert(2u64, ls(&[("job", "b")]));
        let series = group_merged_rows(float, h, &labels, "m").unwrap();
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].fingerprint, 1);
        assert!(series[0].samples.iter().all(|s| s.h.is_none()));
        assert_eq!(series[1].fingerprint, 2);
        assert_eq!(series[1].samples.len(), 1);
        assert!(
            series[1].samples[0]
                .h
                .as_deref()
                .unwrap()
                .bits_eq(&hist.to_float())
        );
    }

    #[test]
    fn group_merged_rows_with_empty_hist_is_the_float_only_fast_path() {
        // Byte-identical to group_rows (the pure-float dual-read case).
        let float = vec![float_row(1, 0, 1.0), float_row(2, 0, 5.0)];
        let labels = HashMap::new();
        let merged = group_merged_rows(float.clone(), Vec::new(), &labels, "m").unwrap();
        let plain = group_rows(float, &labels, "m");
        assert_eq!(merged, plain);
    }

    // -- AC7b: fetch_dual_concurrently dispatches both fetches concurrently.
    //    A two-sided rendezvous (tokio Barrier) completes ONLY if both
    //    futures are in flight at once; a serial dispatch deadlocks it and
    //    trips the timeout.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac7b_chunks_dispatches_float_and_hist_concurrently() {
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::sync::Barrier;
        let barrier = Arc::new(Barrier::new(2));
        let (bf, bh) = (barrier.clone(), barrier.clone());
        let float_fut = fetch_all_concurrently(vec!["f".to_string()], move |_| {
            let b = bf.clone();
            async move {
                b.wait().await;
                Ok(vec![float_row(1, 0, 1.0)])
            }
        });
        let hist_fut = fetch_all_concurrently(vec!["h".to_string()], move |_| {
            let b = bh.clone();
            async move {
                b.wait().await;
                Ok(Vec::<HistSampleRow>::new())
            }
        });
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_dual_concurrently(float_fut, hist_fut),
        )
        .await
        .expect("both fetches must be in flight simultaneously (serial dispatch deadlocks)")
        .unwrap();
        assert_eq!(out.0.len(), 1);
        assert!(out.1.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac7b_fallback_dispatches_float_and_hist_concurrently() {
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::sync::Barrier;
        let barrier = Arc::new(Barrier::new(2));
        let (bf, bh) = (barrier.clone(), barrier.clone());
        let float_fut = async move {
            bf.wait().await;
            Ok::<_, ReadError>(vec![float_row(1, 0, 1.0)])
        };
        let hist_fut = async move {
            bh.wait().await;
            Ok::<_, ReadError>(vec![hist_row(1, 5, &single_histogram())])
        };
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_dual_concurrently(float_fut, hist_fut),
        )
        .await
        .expect("serial dispatch would deadlock the rendezvous")
        .unwrap();
        assert_eq!(out.0.len(), 1);
        assert_eq!(out.1.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac7b_multi_dispatches_float_and_hist_concurrently() {
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::sync::Barrier;
        let barrier = Arc::new(Barrier::new(2));
        let (bf, bh) = (barrier.clone(), barrier.clone());
        let float_fut = async move {
            bf.wait().await;
            Ok::<_, ReadError>(Vec::<MultiSampleRow>::new())
        };
        let hist_fut = async move {
            bh.wait().await;
            Ok::<_, ReadError>(Vec::<MultiHistSampleRow>::new())
        };
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_dual_concurrently(float_fut, hist_fut),
        )
        .await
        .expect("serial dispatch would deadlock the rendezvous")
        .unwrap();
        assert!(out.0.is_empty());
        assert!(out.1.is_empty());
    }

    /// The failure-mode proof: a SERIAL dispatch (await the float future to
    /// completion before starting the histogram future) never reaches the
    /// rendezvous's second party and times out — the exact regression the
    /// concurrent `fetch_dual_concurrently` prevents.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac7b_serial_dispatch_deadlocks_the_rendezvous() {
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::sync::Barrier;
        let barrier = Arc::new(Barrier::new(2));
        let (bf, bh) = (barrier.clone(), barrier.clone());
        let float_fut = async move {
            bf.wait().await;
            Ok::<(), ReadError>(())
        };
        let hist_fut = async move {
            bh.wait().await;
            Ok::<(), ReadError>(())
        };
        let serial = async move {
            float_fut.await?;
            hist_fut.await?;
            Ok::<(), ReadError>(())
        };
        let res = tokio::time::timeout(Duration::from_millis(300), serial).await;
        assert!(
            res.is_err(),
            "serial dispatch must time out — the barrier's second party never arrives"
        );
    }

    // -- M7-A5b-i: histogram-valued API results now ENCODE (VectorHist/
    //    MatrixHist), replacing the A5a HistogramResultUnsupported reject.
    //    Vector and Matrix paths are exercised independently; a pure-float
    //    result still takes the unchanged float path (byte-identical, AC5).

    fn hist_instant() -> InstantSample {
        InstantSample {
            labels: Labels::new(vec![("job".to_string(), "a".to_string())]),
            metric_name: Some("m".to_string()),
            drop_name: false,
            t_ms: 0,
            v: 0.0,
            h: Some(Box::new(single_histogram().to_float())),
        }
    }

    #[test]
    fn a_histogram_vector_result_encodes_as_vector_hist() {
        match value_to_query_result(QueryValue::Vector(vec![hist_instant()])) {
            QueryResult::VectorHist(v) => {
                assert_eq!(v.len(), 1);
                assert!(matches!(v[0].value, HistOrFloat::Hist(_)));
            }
            other => panic!("expected VectorHist, got {other:?}"),
        }
    }

    #[test]
    fn a_histogram_matrix_point_encodes_as_matrix_hist() {
        let matrix = vec![RangeSeries {
            labels: Labels::new(vec![("job".to_string(), "a".to_string())]),
            metric_name: Some("m".to_string()),
            drop_name: false,
            points: vec![Point::hist(0, single_histogram().to_float())],
        }];
        match value_to_query_result(QueryValue::Matrix(matrix)) {
            QueryResult::MatrixHist(m) => {
                assert_eq!(m.len(), 1);
                assert!(matches!(m[0].points[0].1, HistOrFloat::Hist(_)));
            }
            other => panic!("expected MatrixHist, got {other:?}"),
        }
    }

    #[test]
    fn pure_float_vector_and_matrix_still_convert_unchanged() {
        let vector = QueryValue::Vector(vec![InstantSample {
            labels: Labels::new(vec![("job".to_string(), "a".to_string())]),
            metric_name: Some("m".to_string()),
            drop_name: false,
            t_ms: 0,
            v: 3.0,
            h: None,
        }]);
        assert!(matches!(
            value_to_query_result(vector),
            QueryResult::Vector(_)
        ));
        let matrix = QueryValue::Matrix(vec![RangeSeries {
            labels: Labels::new(vec![("job".to_string(), "a".to_string())]),
            metric_name: Some("m".to_string()),
            drop_name: false,
            points: vec![Point::float(0, 1.0)],
        }]);
        match value_to_query_result(matrix) {
            QueryResult::Matrix(m) => assert_eq!(m[0].points, vec![(0, 1.0)]),
            other => panic!("expected Matrix, got {other:?}"),
        }
    }
}
