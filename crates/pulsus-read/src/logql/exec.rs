//! `LogQlEngine` — executes a [`super::plan::Plan`] against ClickHouse via
//! `ChClient`, injects the scan budget, maps overflow codes to
//! [`ReadError::QueryTooBroad`], and finishes vector aggregations in Rust
//! (docs/schemas.md §3.2: "the engine maps fingerprints to `service` and
//! finishes the `sum by`"). Deliberately **not** snapshot-tested — SQL
//! generation itself is `plan`/`sql`'s job and is tested there without a
//! database; this module's own test coverage is the error-mapping unit
//! tests (architect plan amendment §4).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, ChRow, ChRowStream, QuerySettings};
use pulsus_logql::{
    BinOp, Expr, Grouping, GroupingKind, LogExpr, MatchGroup, MatchOp, Matcher, RangeAggOp, Stage,
    StreamSelector, VectorAggOp, VectorMatching,
};

use super::detected::{self, DetectedFields, DetectedLabelOut, FieldAccumulator};
use super::error::{ReadError, TooBroadReason};
use super::explain::PlanExplain;
use super::params::{Direction, PlanCtx, QueryParams, QuerySpec, TimeBounds};
use super::pipeline::{CompiledPipeline, ERROR_LABEL, MetricRun};
use super::plan::{self, ClientAgg, ClientValue, MetricNode, MetricPlan, Plan, StreamsPlan};
use super::rows::{
    DetectedLabelRow, LabelNameRow, LabelValueRow, LogStatsRow, MetricBucketRow, MetricInstantRow,
    MetricScanRow, SampleRow, StreamMetaRow, StreamRow, TailSampleRow, VolumeRow,
};

/// ClickHouse server exception code for `TOO_MANY_BYTES` — the
/// `max_bytes_to_read` overflow this module sets from
/// `reader.logql_scan_budget_bytes`. Deliberately the *only* server code
/// [`map_read_error`] maps to [`ReadError::QueryTooBroad`]:
/// `max_rows_to_read` is never set on **LogQL** read paths (the traces
/// scan budget sets it deliberately on its generator queries, where code
/// 158 maps to `TooBroadReason::TraceScanBudgetRows` via
/// `traces::exec`'s own mapper — issue #57), so on the LogQL path code
/// 158 (`TOO_MANY_ROWS`) can never masquerade as the byte budget
/// (architect plan amendment §4).
const CODE_TOO_MANY_BYTES: i32 = 307;

/// Owned table/budget configuration a [`LogQlEngine`] plans every query
/// against. Mirrors [`PlanCtx`]'s fields as owned `String`s/values so the
/// engine can hand out a borrowed [`PlanCtx`] per call without pinning a
/// lifetime on the engine itself.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub db: String,
    pub streams_idx: String,
    pub streams: String,
    pub samples: String,
    pub rollup_table: String,
    pub rollup_res_ns: u64,
    pub scan_budget_bytes: u64,
    pub max_streams: usize,
    /// `reader.logql_pipeline_scan_factor` (issue M6-09) — see
    /// [`PlanCtx::pipeline_scan_factor`].
    pub pipeline_scan_factor: u32,
}

impl EngineConfig {
    fn plan_ctx(&self) -> PlanCtx<'_> {
        PlanCtx {
            db: &self.db,
            streams_idx: &self.streams_idx,
            streams: &self.streams,
            samples: &self.samples,
            rollup_table: &self.rollup_table,
            rollup_res_ns: self.rollup_res_ns,
            scan_budget_bytes: self.scan_budget_bytes,
            max_streams: self.max_streams,
            pipeline_scan_factor: self.pipeline_scan_factor,
        }
    }
}

/// One resolved stream's response shape: labels as the raw canonical-JSON
/// string stage 2 returned (this crate parses labels only where it must —
/// vector-aggregation grouping — never to re-encode a response; #13 owns
/// the JSON envelope and already depends on a JSON crate for it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamResult {
    pub fingerprint: u64,
    pub service: String,
    pub labels_json: String,
    /// `(timestamp_ns, body)`, in the plan's requested direction.
    pub entries: Vec<(i64, String)>,
}

/// One instant-query series.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSample {
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

/// One range-query series.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSeries {
    pub labels: Vec<(String, String)>,
    /// `(step_ns, value)`, ascending by step.
    pub points: Vec<(i64, f64)>,
}

/// M7-A5b-i: one element's value — a plain float or a native histogram
/// (`FloatHistogram`, the eval-result type). Additive companion to
/// [`VectorSample`]/[`MatrixSeries`] (which stay float-only, LogQL/traces-
/// shared, untouched) — a metrics query whose result carries at least one
/// histogram-valued element/point routes through [`QueryResult::VectorHist`]/
/// [`QueryResult::MatrixHist`] instead.
#[derive(Debug, Clone)]
pub enum HistOrFloat {
    Float(f64),
    Hist(Box<pulsus_model::FloatHistogram>),
}

/// Hand-written (no `PartialEq` derive on `FloatHistogram` — NaN-bearing
/// fields, `pulsus_model`'s own doc): float arm via native `f64::eq`
/// (`NaN != NaN`), histogram arm via `FloatHistogram::bits_eq`, mirroring
/// `pulsus-promql::value`'s `Sample`/`Point` contract.
impl PartialEq for HistOrFloat {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Float(a), Self::Float(b)) => a == b,
            (Self::Hist(a), Self::Hist(b)) => a.bits_eq(b),
            _ => false,
        }
    }
}

/// One instant-query series whose value may be a native histogram.
#[derive(Debug, Clone, PartialEq)]
pub struct HistVectorSample {
    pub labels: Vec<(String, String)>,
    pub value: HistOrFloat,
}

/// One range-query series whose points may mix float and histogram values
/// (a series's underlying sample type can change mid-window).
#[derive(Debug, Clone, PartialEq)]
pub struct HistMatrixSeries {
    pub labels: Vec<(String, String)>,
    /// `(step_ns, value)`, ascending by step.
    pub points: Vec<(i64, HistOrFloat)>,
}

/// The engine's raw result — #13 encodes this into the query-API JSON
/// envelope (out of scope here per the architect plan). `Scalar` is issue
/// #31's addition (`pulsus_promql::QueryValue::Scalar` — a bare-number
/// PromQL expression, e.g. `1 + 1`, evaluated with no series involved);
/// LogQL never produces it.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    /// Log-line streams. `partial` (issue #90) signals a budget-exhausted
    /// fetch-until-limit result: the paging loop stopped because the byte
    /// scan budget was spent, not because the window ran out of matching
    /// lines. The encoder surfaces it as `stats.pulsus_partial`
    /// (skip-if-false, so ordinary responses stay byte-identical). Always
    /// `false` on the fast/non-dropping paths and on genuine exhaustion.
    Streams {
        items: Vec<StreamResult>,
        partial: bool,
    },
    Vector(Vec<VectorSample>),
    Matrix(Vec<MatrixSeries>),
    Scalar(f64),
    /// A top-level PromQL string-literal query (issue #86, M6-08d) —
    /// `pulsus_promql::QueryValue::String`, rendered by the prom API as
    /// `resultType:"string"`. Like [`QueryResult::Scalar`], the wire
    /// timestamp is stamped externally by the encoder from the request's
    /// evaluation time (`at_ms`), never carried in the variant. LogQL
    /// never produces it.
    String(String),
    /// M7-A5b-i: an instant-query result carrying at least one histogram-
    /// valued element (metrics-only — replaces the A5a
    /// `HistogramResultUnsupported` reject).
    VectorHist(Vec<HistVectorSample>),
    /// M7-A5b-i: a range-query result carrying at least one histogram-
    /// valued point (metrics-only).
    MatrixHist(Vec<HistMatrixSeries>),
}

pub struct LogQlEngine {
    client: ChClient,
    config: EngineConfig,
}

impl LogQlEngine {
    pub fn new(client: ChClient, config: EngineConfig) -> Self {
        Self { client, config }
    }

    pub async fn query(&self, expr: &Expr, params: &QueryParams) -> Result<QueryResult, ReadError> {
        let ctx = self.config.plan_ctx();
        match plan::plan(expr, params, &ctx)? {
            Plan::Streams(sp) => self
                .run_streams_inner(&sp, None)
                .await
                .map(|(items, partial)| QueryResult::Streams { items, partial }),
            Plan::Metric(mp) => self.run_metric_inner(&mp, None).await,
            Plan::MetricBinary(node) => self.run_metric_node(&node, None).await,
        }
    }

    /// One execution that also captures the plan trace (#13's
    /// `X-Pulsus-Explain`) — `run_streams_inner`/`run_metric_inner` push
    /// every stage's SQL into `explain` in the same single pass that
    /// executes it, so this incurs **zero** extra ClickHouse reads versus
    /// [`LogQlEngine::query`] (architect plan amendment §3, resolving the
    /// round-1 review finding that a naive `query()` + `explain()` pairing
    /// would double-execute and could observe different data).
    pub async fn query_explained(
        &self,
        expr: &Expr,
        params: &QueryParams,
    ) -> Result<(QueryResult, PlanExplain), ReadError> {
        let ctx = self.config.plan_ctx();
        match plan::plan(expr, params, &ctx)? {
            Plan::Streams(sp) => {
                let mut explain = PlanExplain::new("streams");
                let (items, partial) = self.run_streams_inner(&sp, Some(&mut explain)).await?;
                Ok((QueryResult::Streams { items, partial }, explain))
            }
            Plan::Metric(mp) => {
                let result_type = if mp.step_ns.is_none() {
                    "vector"
                } else {
                    "matrix"
                };
                let mut explain = PlanExplain::new(result_type);
                let result = self.run_metric_inner(&mp, Some(&mut explain)).await?;
                Ok((result, explain))
            }
            Plan::MetricBinary(node) => {
                let mut explain = PlanExplain::new(binary_result_type(&node, params));
                let result = self.run_metric_node(&node, Some(&mut explain)).await?;
                Ok((result, explain))
            }
        }
    }

    /// Labels discovery (#13 `GET|POST /api/logs/v1/labels`): distinct
    /// `log_streams_idx` keys within `b`'s months. Budget-capped like
    /// every other index scan in this module.
    pub async fn label_names(&self, b: TimeBounds) -> Result<Vec<String>, ReadError> {
        self.label_names_inner(b, None).await
    }

    /// [`LogQlEngine::label_names`] plus its `X-Pulsus-Explain` trace, in
    /// the same single pass (no second scan).
    pub async fn label_names_explained(
        &self,
        b: TimeBounds,
    ) -> Result<(Vec<String>, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("labels");
        let names = self.label_names_inner(b, Some(&mut explain)).await?;
        Ok((names, explain))
    }

    async fn label_names_inner(
        &self,
        b: TimeBounds,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<String>, ReadError> {
        let months = plan::months_overlapping(b.start_ns, b.end_ns);
        let sql = super::sql::label_names(&self.config.streams_idx, &months);
        if let Some(e) = explain.as_mut() {
            e.push("label_names", sql.clone(), None);
        }
        let mut names = Vec::new();
        let mut stream = self
            .query_stream::<LabelNameRow>(&sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
            names.push(row.name);
        }
        Ok(names)
    }

    /// Label-values discovery (#13 `GET /api/logs/v1/label/{name}/values`):
    /// distinct values of `name` within `b`'s months. **M1 scope:** returns
    /// the key's full distinct-value set; `query=`-selector narrowing is
    /// deferred to M6 parity (docs/api.md §2.3).
    pub async fn label_values(&self, name: &str, b: TimeBounds) -> Result<Vec<String>, ReadError> {
        self.label_values_inner(name, b, None).await
    }

    /// [`LogQlEngine::label_values`] plus its `X-Pulsus-Explain` trace, in
    /// the same single pass (no second scan).
    pub async fn label_values_explained(
        &self,
        name: &str,
        b: TimeBounds,
    ) -> Result<(Vec<String>, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("label_values");
        let values = self.label_values_inner(name, b, Some(&mut explain)).await?;
        Ok((values, explain))
    }

    async fn label_values_inner(
        &self,
        name: &str,
        b: TimeBounds,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<String>, ReadError> {
        let months = plan::months_overlapping(b.start_ns, b.end_ns);
        let key_literal = super::escape::ch_string(name);
        let sql = super::sql::label_values(&self.config.streams_idx, &months, &key_literal);
        if let Some(e) = explain.as_mut() {
            e.push("label_values", sql.clone(), None);
        }
        let mut values = Vec::new();
        let mut stream = self
            .query_stream::<LabelValueRow>(&sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
            values.push(row.value);
        }
        Ok(values)
    }

    /// Series discovery (#13 `GET|POST /api/logs/v1/series`): the union of
    /// every `selectors` stream resolution, hydrated into distinct
    /// canonical-labels JSON strings (already sorted-key JSON, per
    /// `docs/schemas.md` §3.1 — spliced verbatim into #13's response, never
    /// re-parsed/re-encoded here). `selectors` are expected to be bare
    /// stream selectors (`Expr::Log` with an empty pipeline, as #13 builds
    /// from `match[]`); a metric expression is planned all the same (both
    /// `Plan` variants carry `stage1_sql`/`streams_table`) since stage 1
    /// resolution does not depend on the pipeline/aggregation.
    pub async fn series(
        &self,
        selectors: &[Expr],
        b: TimeBounds,
    ) -> Result<Vec<String>, ReadError> {
        self.series_inner(selectors, b, None).await
    }

    /// [`LogQlEngine::series`] plus its `X-Pulsus-Explain` trace, in the
    /// same single pass (no second scan).
    pub async fn series_explained(
        &self,
        selectors: &[Expr],
        b: TimeBounds,
    ) -> Result<(Vec<String>, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("series");
        let result = self.series_inner(selectors, b, Some(&mut explain)).await?;
        Ok((result, explain))
    }

    async fn series_inner(
        &self,
        selectors: &[Expr],
        b: TimeBounds,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<String>, ReadError> {
        let ctx = self.config.plan_ctx();
        // `series` never buckets or filters samples — it only needs stage
        // 1's month-bounded fingerprint resolution — so `limit`/
        // `direction`/`step_ns` are unused placeholders (a nonzero
        // `step_ns` sidesteps `plan::metric_plan`'s zero-step guard on the
        // off chance a caller ever hands this a metric expression).
        let qp = QueryParams {
            spec: QuerySpec::Range {
                start_ns: b.start_ns,
                end_ns: b.end_ns,
                step_ns: 1_000_000_000,
            },
            limit: 1,
            direction: Direction::Backward,
        };
        let mut fingerprints: Vec<u64> = Vec::new();
        let mut streams_table = self.config.streams.clone();
        for expr in selectors {
            // A binary metric expression carries one stage-1 resolution
            // per leaf selector; the other plan shapes carry exactly one.
            let stage1s: Vec<(String, String)> = match plan::plan(expr, &qp, &ctx)? {
                Plan::Streams(sp) => vec![(sp.stage1_sql, sp.streams_table)],
                Plan::Metric(mp) => vec![(mp.stage1_sql, mp.streams_table)],
                Plan::MetricBinary(node) => node
                    .leaves()
                    .into_iter()
                    .map(|mp| (mp.stage1_sql.clone(), mp.streams_table.clone()))
                    .collect(),
            };
            for (stage1_sql, table) in stage1s {
                if let Some(e) = explain.as_mut() {
                    e.push("stage1_stream_resolution", stage1_sql.clone(), None);
                }
                let fps = self.resolve_fingerprints(&stage1_sql).await?;
                fingerprints.extend(fps);
                streams_table = table;
            }
        }
        fingerprints.sort_unstable();
        fingerprints.dedup();
        // Each selector's own `resolve_fingerprints` call already caps that
        // *individual* selector at `max_streams` (`check_stream_cap` inside
        // it), but says nothing about the deduped union across selectors —
        // N disjoint `match[]` values can each stay under the cap
        // individually while their union blows well past it, building an
        // oversized stage-2 `fingerprint IN (...)` hydration query (round-1
        // code review finding 1). Re-check the cap on the union before
        // proceeding.
        check_stream_cap(fingerprints.len(), self.config.max_streams)?;
        if fingerprints.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(e) = explain.as_mut() {
            e.push(
                "stage2_hydration",
                super::sql::stage2(&streams_table, &fingerprints),
                None,
            );
        }
        let meta = self.hydrate(&streams_table, &fingerprints).await?;
        let mut labels: Vec<String> = meta.into_values().map(|m| m.labels).collect();
        labels.sort();
        labels.dedup();
        Ok(labels)
    }

    pub async fn explain(
        &self,
        expr: &Expr,
        params: &QueryParams,
    ) -> Result<PlanExplain, ReadError> {
        let ctx = self.config.plan_ctx();
        match plan::plan(expr, params, &ctx)? {
            Plan::Streams(sp) => self.explain_streams(&sp).await,
            Plan::Metric(mp) => self.explain_metric(&mp).await,
            Plan::MetricBinary(node) => {
                let mut explain = PlanExplain::new(binary_result_type(&node, params));
                for leaf in node.leaves() {
                    self.explain_metric_into(leaf, &mut explain).await?;
                }
                Ok(explain)
            }
        }
    }

    /// Wraps [`ChClient::query_stream`] with the placeholder-escaping fix
    /// (see [`escape_query_placeholders`]) every call site in this module
    /// must apply — centralized here so no future call site can forget it.
    /// Issue #35: also the guard choke point — [`ensure_query_text_fits`]
    /// runs against the FINAL text (after doubling, so a `?`-heavy regex
    /// predicate is never undercounted) before the query ever reaches
    /// ClickHouse, and a dispatch-time `ChError` is mapped through
    /// [`map_read_error`] here so call sites no longer need their own
    /// outer `map_err` (per-row mapping inside the streaming loop is
    /// unchanged — a `ChRowStream` yields raw `ChError` per row, not
    /// through this wrapper).
    async fn query_stream<'a, R: ChRow>(
        &'a self,
        sql: &str,
        settings: &QuerySettings,
    ) -> Result<ChRowStream<'a, R>, ReadError> {
        let sql = escape_query_placeholders(sql);
        if let Err(reason) = crate::querytext::ensure_query_text_fits(&sql) {
            return Err(ReadError::QueryTooBroad(reason));
        }
        self.client
            .query_stream::<R>(&sql, settings)
            .await
            .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))
    }

    /// Stage 1 — stream resolution. **Budget-capped** (fix-plan amendment
    /// §1, code review finding "Stage 1 bypasses the scan budget"):
    /// docs/schemas.md §3.2 line 305 ties the "aborts with 'query too
    /// broad'" guarantee to the stage-1 index scan itself, not just
    /// stage 3/metric reads — a broad `log_streams_idx` scan must never run
    /// uncapped.
    async fn resolve_fingerprints(&self, stage1_sql: &str) -> Result<Vec<u64>, ReadError> {
        let mut fingerprints = Vec::new();
        let mut stream = self
            .query_stream::<StreamRow>(stage1_sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
            fingerprints.push(row.fingerprint);
            check_stream_cap(fingerprints.len(), self.config.max_streams)?;
        }
        Ok(fingerprints)
    }

    /// Stage 2 — hydration. **Budget-capped** for the same reason as stage 1
    /// (fix-plan amendment §1): the scan budget is a per-query cap
    /// (docs/configuration.md §6), not a stage-3-only concern.
    async fn hydrate(
        &self,
        streams_table: &str,
        fingerprints: &[u64],
    ) -> Result<HashMap<u64, StreamMetaRow>, ReadError> {
        let mut out = HashMap::with_capacity(fingerprints.len());
        if fingerprints.is_empty() {
            return Ok(out);
        }
        let sql = super::sql::stage2(streams_table, fingerprints);
        let mut stream = self
            .query_stream::<StreamMetaRow>(&sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
            // ReplacingMergeTree without FINAL may yield duplicate rows per
            // fingerprint; labels/service are identical per fingerprint, so
            // keeping any one row is safe (docs/schemas.md §3.2 edge cases).
            out.entry(row.fingerprint).or_insert(row);
        }
        Ok(out)
    }

    fn budget_settings(&self) -> QuerySettings {
        read_query_settings(self.config.scan_budget_bytes)
    }

    /// Per-page settings for the fetch-until-limit paging loop (issue
    /// #90). `remaining` is the decrementing `budget − spent` cap, an
    /// approximate best-effort scan guard (NOT a hard byte ceiling) that
    /// bounds runaway paging (see [`LogQlEngine::run_streams_paged`]): if
    /// the FIRST page alone overflows this cap the query fails
    /// `QueryTooBroad`, but once a page has returned a later page tripping
    /// its positive cap returns partial survivors instead. Because
    /// ClickHouse enforces the cap per read block per concurrent reader
    /// (per thread, and per shard on a cluster), actual bytes can exceed
    /// the budget, growing with parallelism and shard count.
    /// `wait_end_of_query = 1` forces ClickHouse to emit the FINAL
    /// `read_bytes` in the summary — the clickhouse 0.15.1 crate captures
    /// the summary from the initial response header and never updates it,
    /// so without this the per-page `read_bytes` used to decrement the
    /// remaining cap would be understated and the guard would leak scan.
    /// Each page is `LIMIT page_size`-bounded, so `wait_end_of_query`
    /// buffers only the (small) result, not the scan.
    ///
    /// `pub` for introspection: the AC5 gate asserts `wait_end_of_query = 1`
    /// is present here. That guard cannot live on `system.query_log` —
    /// `wait_end_of_query` is an HTTP-interface-only parameter (absent from
    /// `system.settings`, never recorded in `query_log.Settings`), and the
    /// summed server-side `read_bytes` is identical with or without it
    /// (the setting only affects the CLIENT-side per-page `read_bytes` this
    /// method's caller uses for budget accounting), so the wiring is only
    /// observable here, at the settings object (issue #90).
    pub fn paging_settings(&self, remaining: u64) -> QuerySettings {
        read_query_settings(remaining).set("wait_end_of_query", 1)
    }

    /// Executes a [`StreamsPlan`] end to end. When `explain` is `Some`,
    /// every stage's already-computed SQL is pushed into it in the same
    /// single pass that executes it — no second run (architect plan
    /// amendment §3; see [`LogQlEngine::query_explained`]).
    ///
    /// Three response paths (issue M6-09):
    /// - **fast** — line-filter-only pipeline (everything pushed down):
    ///   the M1 shape, byte-identical (`labels_json` verbatim, SQL `LIMIT
    ///   == limit`, zero new per-row work);
    /// - **transform** — the pipeline drops/rewrites lines but never
    ///   changes the label set: per-fingerprint grouping, `labels_json`
    ///   verbatim, entries filtered/rewritten;
    /// - **fan-out** — a parser/`label_format` (or an `__error__`-adding
    ///   numeric filter) can change the label set: surviving entries
    ///   regroup by final label set, one `StreamResult` per set with a
    ///   canonically re-rendered `labels_json`.
    ///
    /// Returns `(streams, partial)`: `partial` is set only on the
    /// fetch-until-limit dropping path when the byte scan budget is
    /// exhausted mid-paging (issue #90's signaled partial — surfaced as
    /// `stats.pulsus_partial`); the fast/non-dropping paths and genuine
    /// exhaustion always return `partial == false`.
    async fn run_streams_inner(
        &self,
        sp: &StreamsPlan,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<(Vec<StreamResult>, bool), ReadError> {
        // Compile before any I/O: a bad regex/template is a 400-class
        // rejection, never a wasted scan.
        let compiled = super::pipeline::CompiledPipeline::compile(&sp.pipeline)?;

        if let Some(e) = explain.as_mut() {
            e.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
            for probe in &sp.probes {
                e.push(
                    "selectivity_probe",
                    probe.sql.clone(),
                    Some(format!("key = {}", probe.key)),
                );
            }
        }
        let fingerprints = self.resolve_fingerprints(&sp.stage1_sql).await?;
        if fingerprints.is_empty() {
            return Ok((Vec::new(), false));
        }
        if let Some(e) = explain.as_mut() {
            e.push(
                "stage2_hydration",
                super::sql::stage2(&sp.streams_table, &fingerprints),
                None,
            );
        }
        let meta = self.hydrate(&sp.streams_table, &fingerprints).await?;
        let services = distinct_escaped_services(&meta);

        let sql = super::sql::stage3(
            &sp.samples_table,
            &services,
            &fingerprints,
            super::sql::TimeWindow {
                start_ns: sp.start_ns,
                end_ns: sp.end_ns,
            },
            &sp.line_filters,
            sp.direction,
            sp.scan_limit,
        );
        if let Some(e) = explain.as_mut() {
            e.push("stage3_samples", sql.clone(), None);
        }

        if compiled.is_line_filter_only() {
            // Fast path: today's per-fingerprint shape, `labels_json`
            // verbatim (`scan_limit == result_limit` by construction).
            // Zero-structured-metadata rows stay on this UNCHANGED path (AC-8
            // byte-identity); rows carrying structured metadata (issue #97)
            // fan out into their own merged-label-set streams below.
            let mut by_fp: HashMap<u64, Vec<(i64, String)>> = HashMap::new();
            let mut sm_rows: Vec<SampleRow> = Vec::new();
            let mut stream = self
                .query_stream::<SampleRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
                if row.structured_metadata.is_empty() {
                    by_fp
                        .entry(row.fingerprint)
                        .or_default()
                        .push((row.timestamp_ns, row.body));
                } else {
                    sm_rows.push(row);
                }
            }

            let mut streams: Vec<StreamResult> = by_fp
                .into_iter()
                .filter_map(|(fp, entries)| {
                    meta.get(&fp).map(|m| StreamResult {
                        fingerprint: fp,
                        service: m.service.clone(),
                        labels_json: m.labels.clone(),
                        entries,
                    })
                })
                .collect();
            if !sm_rows.is_empty() {
                streams.extend(fan_out_sm_fast_path(&sm_rows, &meta));
            }
            return Ok((streams, false));
        }

        // Dropping sub-case (issue #90): a label filter, or a line filter
        // after `line_format`, drops lines in-engine — a single oversampled
        // `LIMIT` scan could under-return. Keyset-page until the limit
        // fills, the window exhausts, or the budget is spent.
        if sp.fetch_until_limit {
            return self
                .run_streams_paged(sp, &compiled, &meta, &services, &fingerprints)
                .await;
        }

        // Non-dropping transform/fan-out path: collect rows in arrival
        // order (stage 3 orders globally by timestamp in the requested
        // direction, so arrival order IS the response order — the global
        // `result_limit` truncation below depends on it). A single
        // `stage3` `LIMIT = result_limit` scan, byte-identical to today.
        let mut rows: Vec<SampleRow> = Vec::new();
        let mut stream = self
            .query_stream::<SampleRow>(&sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            rows.push(row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?);
        }

        Ok((
            run_pipeline_rows(rows, &compiled, &meta, sp.result_limit),
            false,
        ))
    }

    /// The fetch-until-limit paging loop (issue #90 — the dropping
    /// sub-case). Keyset-pages PK-pruned pages **in the plan's direction**
    /// through one shared [`StreamAccumulator`] until `result_limit`
    /// survivors are collected, the window is exhausted, or the byte scan
    /// budget is spent, returning `(streams, partial)`.
    ///
    /// **Approximate best-effort scan guard — NOT a hard byte ceiling.**
    /// Each page is issued with a decrementing `max_bytes_to_read = budget −
    /// (bytes already scanned by prior pages)` and `read_overflow_mode =
    /// throw`. If the **first** page alone exceeds the budget the query
    /// fails with `QueryTooBroad` (a genuinely too-broad query), exactly as
    /// the pre-paging single-scan path did. Once at least one page has
    /// returned, the loop is best-effort: before issuing each further page
    /// the top-of-loop guard returns the survivors so far with
    /// `pulsus_partial = true` if the budget is already spent (it never
    /// issues a zero cap, which ClickHouse would treat as *unlimited*), and
    /// likewise a later page whose scan trips its positive cap returns the
    /// partial survivors. The loop always terminates and never scans
    /// `pages × window` unbounded. Because ClickHouse enforces the cap per
    /// read block per concurrent reader (per thread, and per shard on a
    /// cluster), the actual bytes scanned can exceed the budget by an
    /// amount that grows with query parallelism and shard count — the
    /// budget bounds runaway paging, not exact bytes. `wait_end_of_query =
    /// 1` (see [`LogQlEngine::paging_settings`]) makes each page's
    /// `read_bytes` the FINAL scanned total (the clickhouse 0.15.1 crate
    /// otherwise captures an understated header-time value), so `spent`
    /// tracks scan progress soundly; an unknown (`None`) read_bytes charges
    /// the full cap (conservative).
    ///
    /// **Termination.** The cursor advances past every *fetched* row
    /// (`advance_tail_cursor` over the raw page, not survivors — so a page
    /// entirely filtered out by the pipeline never stalls the loop), with
    /// occurrence-count `OFFSET` handling tie-runs larger than a page
    /// (carried from #74). Over a finite window the cursor advances
    /// monotonically, so the loop must eventually fetch `< page_size`
    /// (window exhausted) or spend the budget.
    ///
    /// **Terminal branches.** limit filled / window exhausted → `partial =
    /// false`; budget spent before issuing a later page → `partial = true`
    /// (top-of-loop guard); first-page budget overflow (`spent == 0`) →
    /// propagate `QueryTooBroad` (a genuinely too-broad query, preserving
    /// today's error); a later page (`spent > 0`) tripping its positive cap
    /// → signaled partial.
    async fn run_streams_paged(
        &self,
        sp: &StreamsPlan,
        compiled: &super::pipeline::CompiledPipeline,
        meta: &HashMap<u64, StreamMetaRow>,
        services: &[String],
        fingerprints: &[u64],
    ) -> Result<(Vec<StreamResult>, bool), ReadError> {
        let budget = self.config.scan_budget_bytes;
        let window = super::sql::TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        };
        // First-page size = the oversample hint; subsequent pages reuse it.
        let page_size = sp.scan_limit.max(1);
        let mut acc = StreamAccumulator::new(meta, sp.result_limit);
        let mut cursor: Option<TailCursor> = None;
        let mut spent: u64 = 0;

        loop {
            // Terminate before issuing: `max_bytes_to_read = 0` is
            // ClickHouse's *unlimited* sentinel, so a zero cap must never be
            // issued. Once the budget is spent, return the survivors so far
            // as a partial result (a later page's positive-cap overflow is
            // handled below; the first-page `spent == 0` case never reaches
            // here). This makes `page_cap` always > 0.
            if scan_budget_spent(spent, budget) {
                return Ok((acc.into_streams(), true));
            }
            let page_cap = budget.saturating_sub(spent); // now always > 0
            let ks_lower = match cursor {
                None => super::sql::KeysetLower::First,
                Some(c) => super::sql::KeysetLower::After {
                    tuple: c.tuple,
                    offset: c.seen,
                },
            };
            let sql = super::sql::stage3_keyset(
                &sp.samples_table,
                services,
                fingerprints,
                window,
                ks_lower,
                sp.direction,
                &sp.line_filters,
                page_size,
            );

            // Fetch and fully drain one page; `read_bytes` is meaningful
            // only after the drain (wait_end_of_query=1). Scoped so the
            // stream's pooled-connection lease releases before the next
            // page.
            let mut rows: Vec<TailSampleRow> = Vec::new();
            // Issue #35: `query_stream` now returns `Result<_, ReadError>`
            // directly (already mapped through `map_read_error` for a
            // dispatch-time failure); per-row errors are still raw
            // `ChError` from `ChRowStream::next()`, mapped explicitly below
            // with the SAME `map_read_error(_, budget)` the dispatch-time
            // path uses internally — so `page_result`'s `Err` is uniformly
            // an already-mapped `ReadError` either way, preserving the
            // first-page-vs-later-page branching below unchanged.
            let page_result: Result<Option<u64>, ReadError> = async {
                let mut stream = self
                    .query_stream::<TailSampleRow>(&sql, &self.paging_settings(page_cap))
                    .await?;
                while let Some(row) = stream.next().await {
                    rows.push(row.map_err(|e| map_read_error(e, budget))?);
                }
                Ok(stream.read_bytes())
            }
            .await;

            let read = match page_result {
                Ok(rb) => rb.unwrap_or(page_cap),
                Err(mapped) => {
                    if matches!(
                        mapped,
                        ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { .. })
                    ) {
                        // Branch split on this positive-cap overflow:
                        // `spent == 0` (first page) overflows the FULL budget
                        // ⇒ propagate `QueryTooBroad` (a genuinely too-broad
                        // query) — preserve the error the old single-scan
                        // path raised; `spent > 0` (a
                        // later page) ⇒ keep the survivors and signal partial
                        // (best-effort, not a hard byte ceiling). The
                        // budget-already-spent-before-issuing case is covered
                        // by the top-of-loop guard, which never issues a zero
                        // cap.
                        if spent == 0 {
                            return Err(mapped);
                        }
                        return Ok((acc.into_streams(), true));
                    }
                    return Err(mapped);
                }
            };
            spent = spent.saturating_add(read);

            let fetched = u32::try_from(rows.len()).unwrap_or(u32::MAX);
            cursor = advance_tail_cursor(cursor, &rows);
            let sample_rows: Vec<SampleRow> = rows
                .into_iter()
                .map(|r| SampleRow {
                    fingerprint: r.fingerprint,
                    timestamp_ns: r.timestamp_ns,
                    body: r.body,
                    structured_metadata: r.structured_metadata,
                })
                .collect();
            let filled = acc.feed(&sample_rows, compiled);

            if filled {
                // Result limit filled — a complete result, never partial.
                return Ok((acc.into_streams(), false));
            }
            if fetched < page_size {
                // Fewer rows than asked ⇒ the window is exhausted — a
                // complete result over the whole window, never partial.
                return Ok((acc.into_streams(), false));
            }
            // Budget-spent-before-issuing is handled by the top-of-loop guard
            // (which never issues a zero/unlimited cap); loop back to it.
        }
    }

    /// Executes a [`MetricPlan`] end to end. Same single-pass explain
    /// contract as [`LogQlEngine::run_streams_inner`]. A plan carrying
    /// [`MetricPlan::client`] takes the client-aggregated path (issue
    /// M6-10): a full-window `metric_raw_samples` scan (no `LIMIT`,
    /// budget-abort only) evaluated per line in-engine.
    async fn run_metric_inner(
        &self,
        mp: &MetricPlan,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
        // Compile the client pipeline before any I/O (a bad regex is a
        // 400, never a wasted scan) — and before the empty-fingerprint
        // early-outs below for the same reason.
        let compiled = match &mp.client {
            Some(client) => Some(CompiledPipeline::compile(&client.pipeline)?),
            None => None,
        };
        if let Some(e) = explain.as_mut() {
            e.set_routing(mp.routing.clone());
            e.push("stage1_stream_resolution", mp.stage1_sql.clone(), None);
            for probe in &mp.probes {
                e.push(
                    "selectivity_probe",
                    probe.sql.clone(),
                    Some(format!("key = {}", probe.key)),
                );
            }
        }
        let fingerprints = self.resolve_fingerprints(&mp.stage1_sql).await?;
        let is_instant = mp.step_ns.is_none();
        if fingerprints.is_empty() {
            // `absent_over_time` must still report absence when the
            // selector resolves NO streams at all.
            if let (Some(client), Some(compiled)) = (&mp.client, &compiled)
                && matches!(client.range_op, RangeAggOp::AbsentOverTime)
            {
                return run_client_agg_rows(
                    &[],
                    compiled,
                    &HashMap::new(),
                    client,
                    ClientWindow {
                        start_ns: mp.start_ns,
                        end_ns: mp.end_ns,
                        step_ns: mp.step_ns,
                    },
                    mp.rate_window_ns,
                );
            }
            return Ok(if is_instant {
                QueryResult::Vector(Vec::new())
            } else {
                QueryResult::Matrix(Vec::new())
            });
        }
        if let (Some(client), Some(compiled)) = (&mp.client, &compiled) {
            return self
                .run_metric_client(mp, client, compiled, &fingerprints, explain)
                .await;
        }
        if let Some(e) = explain.as_mut() {
            e.push(
                "stage2_hydration",
                super::sql::stage2(&mp.streams_table, &fingerprints),
                None,
            );
        }
        let meta = self.hydrate(&mp.streams_table, &fingerprints).await?;
        // Rollup table has no `service` column (`ORDER BY (fingerprint,
        // bucket_ns)`); the raw fallback needs it re-injected to keep
        // `log_samples`'s `(service, fingerprint, timestamp_ns)` primary-key
        // prefix engaged (fix-plan amendment §3).
        let services = if mp.rollup {
            Vec::new()
        } else {
            distinct_escaped_services(&meta)
        };
        let source = super::sql::MetricSource {
            table: &mp.table,
            bucket_col: mp.bucket_col,
            agg_expr: mp.agg_expr,
        };

        if is_instant {
            let sql = super::sql::metric_instant(
                source,
                &services,
                &fingerprints,
                super::sql::TimeWindow {
                    start_ns: mp.start_ns,
                    end_ns: mp.end_ns,
                },
                &mp.extra_predicates,
            );
            if let Some(e) = explain.as_mut() {
                e.push("metric_read", sql.clone(), Some(mp.routing.reason.clone()));
            }
            let mut stream = self
                .query_stream::<MetricInstantRow>(&sql, &self.budget_settings())
                .await?;
            let mut series: Vec<InstantSeries> = Vec::new();
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
                let Some(m) = meta.get(&row.fingerprint) else {
                    continue;
                };
                let value = apply_rate(row.n as f64, mp.rate_window_ns);
                series.push(InstantSeries {
                    labels: series_labels(m),
                    value,
                });
            }
            for (op, grouping, param) in mp.vector_aggs.iter().rev() {
                series = group_instant(series, *op, grouping.as_ref(), *param);
            }
            Ok(QueryResult::Vector(
                series
                    .into_iter()
                    .map(|s| VectorSample {
                        labels: s.labels,
                        value: s.value,
                    })
                    .collect(),
            ))
        } else {
            let step_ns = mp.step_ns.expect("checked by is_instant above");
            let sql = super::sql::metric_range(
                source,
                &services,
                &fingerprints,
                super::sql::TimeWindow {
                    start_ns: mp.start_ns,
                    end_ns: mp.end_ns,
                },
                step_ns,
                &mp.extra_predicates,
            );
            if let Some(e) = explain.as_mut() {
                e.push("metric_read", sql.clone(), Some(mp.routing.reason.clone()));
            }
            let mut stream = self
                .query_stream::<MetricBucketRow>(&sql, &self.budget_settings())
                .await?;
            let mut by_fp: HashMap<u64, BTreeMap<i64, f64>> = HashMap::new();
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
                let value = apply_rate(row.n as f64, mp.rate_window_ns);
                by_fp
                    .entry(row.fingerprint)
                    .or_default()
                    .insert(row.step, value);
            }
            let mut series: Vec<RangeSeries> = by_fp
                .into_iter()
                .filter_map(|(fp, points)| {
                    meta.get(&fp).map(|m| RangeSeries {
                        labels: series_labels(m),
                        points,
                    })
                })
                .collect();
            for (op, grouping, param) in mp.vector_aggs.iter().rev() {
                series = group_range(series, *op, grouping.as_ref(), *param);
            }
            Ok(QueryResult::Matrix(
                series
                    .into_iter()
                    .map(|s| MatrixSeries {
                        labels: s.labels,
                        points: s.points.into_iter().collect(),
                    })
                    .collect(),
            ))
        }
    }

    /// The client-aggregated metric path (issue M6-10): fetch every
    /// matching `(fingerprint, timestamp_ns, body)` row in the window —
    /// **no `LIMIT`**; the scan is complete or aborts on the byte budget
    /// (`QueryTooBroad`), never silently truncated — then run the
    /// compiled pipeline per line, bucket by step in-engine, reduce per
    /// `(final-label-set, bucket)`, and finish the vector aggregations.
    async fn run_metric_client(
        &self,
        mp: &MetricPlan,
        client: &ClientAgg,
        compiled: &CompiledPipeline,
        fingerprints: &[u64],
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
        if let Some(e) = explain.as_mut() {
            e.push(
                "stage2_hydration",
                super::sql::stage2(&mp.streams_table, fingerprints),
                None,
            );
        }
        let meta = self.hydrate(&mp.streams_table, fingerprints).await?;
        let services = distinct_escaped_services(&meta);
        let sql = super::sql::metric_raw_samples(
            &mp.table,
            &services,
            fingerprints,
            super::sql::TimeWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
            },
            &mp.extra_predicates,
        );
        if let Some(e) = explain.as_mut() {
            e.push("metric_read", sql.clone(), Some(mp.routing.reason.clone()));
        }
        // Stream the raw scan into reducer state (review round 1,
        // finding 1): rows fold into `ClientAggState` in bounded chunks,
        // so process memory is O(buckets × series) + one chunk — never
        // the whole scan. The ClickHouse byte budget
        // (`max_bytes_to_read`, `budget_settings`) is charged server-
        // side AS the scan streams and aborts mid-stream as
        // `QueryTooBroad(ScanBudgetBytes)` — complete-or-error holds
        // without buffering-driven OOM risk.
        let mut state = ClientAggState::new(
            compiled,
            &meta,
            client,
            ClientWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
                step_ns: mp.step_ns,
            },
            mp.rate_window_ns,
        )?;
        let mut chunk: Vec<MetricScanRow> = Vec::with_capacity(CLIENT_AGG_CHUNK_ROWS);
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule) — no other query
            // runs inside this block, and the lease ends at the brace.
            let mut stream = self
                .query_stream::<MetricScanRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                chunk.push(row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?);
                if chunk.len() >= CLIENT_AGG_CHUNK_ROWS {
                    state.push_rows(&chunk)?;
                    chunk.clear();
                }
            }
        }
        state.push_rows(&chunk)?;
        Ok(apply_vector_aggs(state.finish(), &mp.vector_aggs))
    }

    /// Evaluates a [`MetricNode`] tree (issue M6-10): leaves execute the
    /// ordinary metric path; `Binary`/`Scalar`/`VectorAgg` combine the
    /// results in-engine. Boxed recursion (async).
    fn run_metric_node<'a>(
        &'a self,
        node: &'a MetricNode,
        explain: Option<&'a mut PlanExplain>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<QueryResult, ReadError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut explain = explain;
            match node {
                MetricNode::Leaf(mp) => self.run_metric_inner(mp, explain.as_deref_mut()).await,
                MetricNode::Scalar(v) => Ok(QueryResult::Scalar(*v)),
                MetricNode::VectorAgg { aggs, inner } => {
                    let result = self.run_metric_node(inner, explain.as_deref_mut()).await?;
                    Ok(apply_vector_aggs(result, aggs))
                }
                MetricNode::Binary {
                    op,
                    return_bool,
                    matching,
                    lhs,
                    rhs,
                } => {
                    let l = self.run_metric_node(lhs, explain.as_deref_mut()).await?;
                    let r = self.run_metric_node(rhs, explain).await?;
                    combine_binary(*op, *return_bool, matching.as_ref(), l, r)
                }
            }
        })
    }

    async fn explain_streams(&self, sp: &StreamsPlan) -> Result<PlanExplain, ReadError> {
        let mut explain = PlanExplain::new("streams");
        explain.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
        for probe in &sp.probes {
            explain.push(
                "selectivity_probe",
                probe.sql.clone(),
                Some(format!("key = {}", probe.key)),
            );
        }
        let fingerprints = self.resolve_fingerprints(&sp.stage1_sql).await?;
        if fingerprints.is_empty() {
            return Ok(explain);
        }
        let stage2_sql = super::sql::stage2(&sp.streams_table, &fingerprints);
        explain.push("stage2_hydration", stage2_sql.clone(), None);
        let meta = self.hydrate(&sp.streams_table, &fingerprints).await?;
        let services = distinct_escaped_services(&meta);
        let stage3_sql = super::sql::stage3(
            &sp.samples_table,
            &services,
            &fingerprints,
            super::sql::TimeWindow {
                start_ns: sp.start_ns,
                end_ns: sp.end_ns,
            },
            &sp.line_filters,
            sp.direction,
            sp.scan_limit,
        );
        explain.push("stage3_samples", stage3_sql, None);
        Ok(explain)
    }

    async fn explain_metric(&self, mp: &MetricPlan) -> Result<PlanExplain, ReadError> {
        let result_type = if mp.step_ns.is_none() {
            "vector"
        } else {
            "matrix"
        };
        let mut explain = PlanExplain::new(result_type);
        self.explain_metric_into(mp, &mut explain).await?;
        Ok(explain)
    }

    /// Pushes one [`MetricPlan`]'s stages into an existing explain — the
    /// shared body of [`LogQlEngine::explain_metric`] and the per-leaf
    /// walk of a binary plan (where `set_routing` reflects the LAST
    /// leaf; each `metric_read` entry carries its own reason).
    async fn explain_metric_into(
        &self,
        mp: &MetricPlan,
        explain: &mut PlanExplain,
    ) -> Result<(), ReadError> {
        explain.set_routing(mp.routing.clone());
        explain.push("stage1_stream_resolution", mp.stage1_sql.clone(), None);
        for probe in &mp.probes {
            explain.push(
                "selectivity_probe",
                probe.sql.clone(),
                Some(format!("key = {}", probe.key)),
            );
        }
        let fingerprints = self.resolve_fingerprints(&mp.stage1_sql).await?;
        if fingerprints.is_empty() {
            return Ok(());
        }
        explain.push(
            "stage2_hydration",
            super::sql::stage2(&mp.streams_table, &fingerprints),
            None,
        );
        let meta = self.hydrate(&mp.streams_table, &fingerprints).await?;
        let services = if mp.rollup {
            Vec::new()
        } else {
            distinct_escaped_services(&meta)
        };
        let window = super::sql::TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        };
        let metric_sql = if mp.client.is_some() {
            // Client-aggregated (issue M6-10): the raw full-window fetch,
            // not a SQL aggregate.
            super::sql::metric_raw_samples(
                &mp.table,
                &services,
                &fingerprints,
                window,
                &mp.extra_predicates,
            )
        } else {
            let source = super::sql::MetricSource {
                table: &mp.table,
                bucket_col: mp.bucket_col,
                agg_expr: mp.agg_expr,
            };
            match mp.step_ns {
                Some(step_ns) => super::sql::metric_range(
                    source,
                    &services,
                    &fingerprints,
                    window,
                    step_ns,
                    &mp.extra_predicates,
                ),
                None => super::sql::metric_instant(
                    source,
                    &services,
                    &fingerprints,
                    window,
                    &mp.extra_predicates,
                ),
            }
        };
        explain.push("metric_read", metric_sql, Some(mp.routing.reason.clone()));
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Issue #74 (M6-11): `/api/logs/v1/stats` + the live-tail keyset poll.
// ---------------------------------------------------------------------

/// The `/api/logs/v1/stats` aggregate (docs/api.md §2.5). `chunks` is the
/// adjudicated selector-scoped **partition-count proxy**
/// (`uniqExact` of the row's partition date), not a physical MergeTree
/// part count — per-part fidelity, if ever demanded, routes to #25.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogStats {
    pub streams: u64,
    pub chunks: u64,
    pub entries: u64,
    pub bytes: u64,
}

// ---------------------------------------------------------------------
// Issue #169 (M7-C1): `/api/logs/v1/volume`.
// ---------------------------------------------------------------------

/// `aggregateBy` (docs/api.md §2.6): group volumes by the matched label
/// *pairs* (`series`, the default) or by bare label *names* (`labels`,
/// each entry keyed `(name, "")`). Semantics pinned against the repo's
/// interop oracle, grafana/loki:3.4.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeAggregateBy {
    Series,
    Labels,
}

/// One `/api/logs/v1/volume` request's engine parameters. `target_labels`
/// is already deduped and bounded by the API layer (`logs_api/params.rs`'s
/// `MAX_TARGET_LABELS`/`MAX_TARGET_LABEL_BYTES` caps run BEFORE any AST
/// mutation here); empty = key by the selector's own matcher names.
#[derive(Debug, Clone)]
pub struct VolumeQuery {
    pub bounds: TimeBounds,
    /// Post-aggregation top-N truncation (bytes-desc).
    pub limit: u32,
    pub aggregate_by: VolumeAggregateBy,
    /// Deduped; empty = none.
    pub target_labels: Vec<String>,
}

/// One aggregated volume entry. `labels` sorted by name; empty vec = the
/// `{}` group. In Labels mode: exactly one pair `(label_name, "")`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeEntry {
    pub labels: Vec<(String, String)>,
    pub bytes: u64,
}

/// The occurrence-count keyset cursor (issue #74 plan v4 D2 + the round-4
/// adjudication): `tuple` is the last fetched row's
/// `(timestamp_ns, fingerprint, cityHash64(body))`; `seen` counts how
/// many rows equal to `tuple` have already been delivered (the SQL
/// `OFFSET` of the next page), resetting to 0 whenever the tuple
/// changes. Split tie groups are re-fetched via the inclusive `>=`
/// predicate and skipped server-side by `OFFSET seen` — every row of a
/// tie group is delivered exactly once even when `LIMIT` splits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailCursor {
    /// `(timestamp_ns, fingerprint, cityHash64(body))`.
    pub tuple: (i64, u64, u64),
    /// Rows equal to `tuple` already delivered — the next page's `OFFSET`.
    pub seen: u32,
}

/// One tail poll's lower bound (issue #74 plan v4 D1): the two predicate
/// modes are distinct and never conflated — the first page carries the
/// API `start` bound (`timestamp_ns > start_ns`, the repo stage-3
/// convention); every later page carries the keyset term instead (the
/// cursor dominates `start`).
#[derive(Debug, Clone, Copy)]
pub enum TailLower {
    Start { start_ns: i64 },
    After(TailCursor),
}

/// One tail poll's result. `next` is the advanced boundary cursor
/// (unchanged when the page fetched no rows); `fetched` is the RAW
/// fetched-row count — `fetched == fetch_limit` means the slice may hold
/// more rows and the caller must re-poll from `next` before advancing
/// its scan watermark (see `logs_api/tail.rs`'s producer loop).
#[derive(Debug)]
pub struct TailPage {
    pub streams: Vec<StreamResult>,
    pub next: Option<TailCursor>,
    pub fetched: u32,
}

/// Registration-visibility grace at the live edge (issue #94 v6-v8): the
/// hold/scan-gate keeps `TailSetup::scan_floor_ns` frozen (full-span
/// stage-1) until the producer certifies a COMPLETED full-span poll whose
/// start dwell at the live edge was >= this grace, and thereafter bounds
/// how far the narrowed floor trails the live cursor
/// (`lower_ns - TAIL_REGISTRATION_GRACE_NS`). POLICY, not a derived
/// ceiling: covers the default `batch_ms` flush (200 ms), a ~1.5s pre-send
/// retry tail, and generous headroom for distributed-send backlogs;
/// visibility later than GRACE is issue #134's ingest-durability scope,
/// not a read-path constant to inflate. Documented constant (same precedent as
/// the writer's retry constants) — deliberately not an env/config/request
/// knob (a knob would invite masking #134-class failures with unbounded
/// read-side rescans; reconnect is the existing, no-knob remedy).
pub const TAIL_REGISTRATION_GRACE_NS: i64 = 3_600_000_000_000;

/// The per-connection tail setup (issue #74): the streams plan and the
/// compiled pipeline, built ONCE before the WebSocket upgrade (a bad
/// regex/template is a 400 rejection, never an upgraded-then-closed
/// socket). Every poll reuses both — tail runs the identical stage-1/2/3
/// plan machinery and the SAME `CompiledPipeline` as `query()`
/// (semantics-drift-free; the task-manager-ratified invariant), only the
/// fetch ordering/cursor differ.
///
/// Beyond the (public) `plan`/`compiled`, the setup carries crate-private
/// state for the bounded, atomicity-safe, scan-gated month refresh (issue
/// #94 v2/v3 + v6-v8's phase split): the original `expr` and
/// `base_params` are replanned — with **no DB I/O** — whenever the scan
/// window `[scan_floor_ns, upper_ns]` covers a different (lo_month,
/// hi_month) pair than `covered_months`.
///
/// `scan_floor_ns` is monotone and starts at the connection's
/// (retention-)clamped setup start `s`. During catch-up/fall-behind (the
/// producer's `narrow == false`) it stays FROZEN, so stage-1 scans the
/// FULL span `[scan_floor_ns, upper_ns]` — identical to the pre-#94-v6
/// behaviour, request-bounded, never lifetime-growing. Only once the
/// producer certifies a COMPLETED full-span live-edge poll whose start
/// dwell was `>= TAIL_REGISTRATION_GRACE_NS` (`narrow == true`) does the
/// floor advance, to `max(scan_floor_ns, lower_ns - GRACE)` — bounding the
/// per-poll month scan to the live poll window's width instead of the
/// connection's lifetime.
///
/// **Clamp-qualified dichotomy (issue #94 v8; the connection's scan
/// universe `U = { M : M >= month(s) }`, identical to the landed
/// pre-#94-v6 code):**
/// - `M ∉ U` (a month strictly before the clamped start): never scanned
///   by ANY scan of this connection, full-span included — pre-existing
///   (issue #134 residual class (i)); the read path cannot recover it
///   (reconnect with an earlier start, retention-permitting, or an
///   ingest-side atomicity/backfill remedy).
/// - `M ∈ U`: every registration into `M` visible by
///   `end(M) + delay + GRACE` is caught by some full-span or in-band scan
///   and cached permanently; every MISSED registration is provably later
///   than that bound (issue #134 residual class (ii)). The floor's clamp
///   arm (`scan_floor_ns` never drops below `s`) never excludes a
///   universe month — only the `lower_ns - GRACE` arm can, and only once
///   it has advanced past `s`.
///
/// Because narrowing the lower edge can prune a month whose
/// `log_streams`/`log_streams_idx` registration is the ONLY record of a
/// fingerprint whose sample now falls in a later, in-window month (writes
/// to `log_samples` and the stream tables are non-atomic —
/// `crates/pulsus-write/src/writer/mod.rs`), `resolved` is a cumulative,
/// deduped cache of every fingerprint stage-1 has ever resolved on this
/// connection: stage-2/3 read the cached union, not just the current
/// poll's narrow result, so a fingerprint resolved once (in its
/// registration month) stays resolvable for the rest of the connection.
/// The cache is capped by the same `max_streams` ceiling stage-1 already
/// obeys (reject, not silently truncate). These fields are only ever
/// constructed by [`LogQlEngine::tail_setup`], so `TailSetup`'s public
/// re-export surface is unchanged.
#[derive(Debug)]
pub struct TailSetup {
    pub plan: StreamsPlan,
    pub compiled: CompiledPipeline,
    /// The original tail expression, replanned on a covered-window change.
    expr: Expr,
    /// The setup `QueryParams` (`Copy`); both `start_ns` and `end_ns` are
    /// overridden to the scan window on refresh.
    base_params: QueryParams,
    /// The monotone scan-set lower anchor (issue #94 v6-v8): frozen
    /// during catch-up/fall-behind, advances to `max(self, lower_ns -
    /// GRACE)` only on a scan-gated live-edge refresh. Starts at the
    /// connection's clamped setup start.
    scan_floor_ns: i64,
    /// `(year_month(scan_floor_ns), year_month(upper))` the current
    /// `plan` covers.
    covered_months: ((i64, u32), (i64, u32)),
    /// Cumulative, sorted+deduped union of every fingerprint stage-1 has
    /// resolved on this connection — the orphan-cache that keeps a
    /// partial-failure (older-month-registered) stream resolvable after
    /// the stage-1 month window narrows past its registration month.
    resolved: Vec<u64>,
}

impl LogQlEngine {
    /// `/api/logs/v1/stats` (docs/api.md §2.5): stage-1 fingerprint
    /// resolution, then ONE aggregation — rollup-routed (zero body
    /// reads) when the query carries no line filter, a skip-index
    /// `log_samples` scan otherwise. `expr` must be a log stream
    /// selector with (at most) line-filter pipeline stages; anything
    /// else is a 400-class rejection.
    pub async fn stats(&self, expr: &Expr, b: TimeBounds) -> Result<LogStats, ReadError> {
        self.stats_inner(expr, b, None).await
    }

    /// [`LogQlEngine::stats`] plus its `X-Pulsus-Explain` trace, in the
    /// same single pass (no second scan) — the `query_explained`
    /// contract.
    pub async fn stats_explained(
        &self,
        expr: &Expr,
        b: TimeBounds,
    ) -> Result<(LogStats, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("stats");
        let stats = self.stats_inner(expr, b, Some(&mut explain)).await?;
        Ok((stats, explain))
    }

    async fn stats_inner(
        &self,
        expr: &Expr,
        b: TimeBounds,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<LogStats, ReadError> {
        let ctx = self.config.plan_ctx();
        // `limit`/`direction`/`step` are unused placeholders — stats
        // never reads samples through stage 3 (same idiom as `series`).
        let qp = QueryParams {
            spec: QuerySpec::Range {
                start_ns: b.start_ns,
                end_ns: b.end_ns,
                step_ns: 1_000_000_000,
            },
            limit: 1,
            direction: Direction::Forward,
        };
        let sp = match plan::plan(expr, &qp, &ctx)? {
            Plan::Streams(sp) => sp,
            Plan::Metric(_) | Plan::MetricBinary(_) => {
                return Err(ReadError::PipelineInvalid {
                    reason: "stats requires a log stream selector query (a metric query has no \
                             stream statistics)"
                        .to_string(),
                });
            }
        };
        // Only line filters have a pushdown aggregation shape; a parser/
        // format/label-filter stage would silently over-count if ignored
        // (defense in depth — the API layer rejects these before parsing
        // reaches the engine).
        if !sp
            .pipeline
            .iter()
            .all(|s| matches!(s, Stage::LineFilter(_)))
        {
            return Err(ReadError::PipelineInvalid {
                reason: "stats supports a stream selector plus line filters only".to_string(),
            });
        }

        if let Some(e) = explain.as_mut() {
            e.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
        }
        let fingerprints = self.resolve_fingerprints(&sp.stage1_sql).await?;
        if fingerprints.is_empty() {
            return Ok(LogStats::default());
        }

        let window = super::sql::TimeWindow {
            start_ns: b.start_ns,
            end_ns: b.end_ns,
        };
        let (sql, routing) = if sp.line_filters.is_empty() {
            (
                super::sql::log_stats_rollup(&self.config.rollup_table, &fingerprints, window),
                super::plan::RoutingDecision {
                    chosen: super::plan::RouteChoice::Rollup,
                    reason: "rollup: no line filter — stats served from the rollup with zero \
                             body reads"
                        .to_string(),
                },
            )
        } else {
            // The raw fallback needs the `service` PREWHERE re-injected
            // to keep `log_samples`'s primary-key prefix engaged — the
            // stage-3/metric-raw contract.
            if let Some(e) = explain.as_mut() {
                e.push(
                    "stage2_hydration",
                    super::sql::stage2(&sp.streams_table, &fingerprints),
                    None,
                );
            }
            let meta = self.hydrate(&sp.streams_table, &fingerprints).await?;
            let services = distinct_escaped_services(&meta);
            (
                super::sql::log_stats_raw(
                    &sp.samples_table,
                    &services,
                    &fingerprints,
                    window,
                    &sp.line_filters,
                ),
                super::plan::RoutingDecision {
                    chosen: super::plan::RouteChoice::Raw,
                    reason: format!(
                        "raw: {} line filter(s) force a log_samples scan (the rollup is \
                         body-content-blind)",
                        sp.line_filters.len()
                    ),
                },
            )
        };
        if let Some(e) = explain.as_mut() {
            e.set_routing(routing.clone());
            e.push("stats_read", sql.clone(), Some(routing.reason.clone()));
        }

        // An aggregation with no GROUP BY always returns exactly one row.
        let mut result = LogStats::default();
        let mut stream = self
            .query_stream::<LogStatsRow>(&sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
            result = LogStats {
                streams: row.streams,
                chunks: row.chunks,
                entries: row.entries,
                bytes: row.bytes,
            };
        }
        Ok(result)
    }

    /// `/api/logs/v1/volume` (issue #169, docs/api.md §2.6): per-label-set
    /// byte volumes over `[start, end]`, served ENTIRELY from the rollup —
    /// the endpoint accepts a matchers-only selector, so unlike
    /// [`LogQlEngine::stats`] there is no raw fallback and never a body
    /// read. Keying/sort semantics oracle-pinned (grafana/loki:3.4.2):
    /// see [`accumulate_volume`].
    pub async fn volume(
        &self,
        expr: &Expr,
        q: &VolumeQuery,
    ) -> Result<Vec<VolumeEntry>, ReadError> {
        self.volume_inner(expr, q, None).await
    }

    /// [`LogQlEngine::volume`] plus its `X-Pulsus-Explain` trace, in the
    /// same single pass (no second scan) — the `query_explained` contract.
    pub async fn volume_explained(
        &self,
        expr: &Expr,
        q: &VolumeQuery,
    ) -> Result<(Vec<VolumeEntry>, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("volume");
        let entries = self.volume_inner(expr, q, Some(&mut explain)).await?;
        Ok((entries, explain))
    }

    async fn volume_inner(
        &self,
        expr: &Expr,
        q: &VolumeQuery,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<VolumeEntry>, ReadError> {
        // Matchers-only (defense in depth — the API layer rejects any
        // pipeline stage 400 before parsing reaches the engine): the
        // rollup is body-content-blind, so even a line filter would
        // silently over-count here, and volume deliberately has NO
        // raw fallback (docs/api.md §2.6).
        let le = match expr {
            Expr::Log(le) if le.pipeline.is_empty() => le,
            Expr::Log(_) => {
                return Err(ReadError::PipelineInvalid {
                    reason: "volume supports a bare stream selector only (no pipeline stages)"
                        .to_string(),
                });
            }
            Expr::Metric(_) => {
                return Err(ReadError::PipelineInvalid {
                    reason: "volume requires a log stream selector query (a metric query has no \
                             stream volume)"
                        .to_string(),
                });
            }
        };
        let labels_to_match = volume_labels_to_match(&le.selector, &q.target_labels);
        // `targetLabels` injection (oracle `prepareLabelsAndMatchersWithTargets`):
        // each target with no matcher of its name gets a `=~ ".+"` matcher
        // appended BEFORE planning, so target-keyed streams are resolvable
        // even when the original selector never mentions the target. The
        // injected name flows through `plan`'s ordinary `escape` boundary
        // exactly like a parsed matcher (`tests/injection.rs`).
        let injected;
        let plan_expr = if q.target_labels.is_empty() {
            expr
        } else {
            injected = Expr::Log(inject_target_matchers(le, &q.target_labels));
            &injected
        };

        let ctx = self.config.plan_ctx();
        // `limit`/`direction`/`step` are unused placeholders — volume
        // never reads samples through stage 3 (the `stats_inner` idiom).
        let qp = QueryParams {
            spec: QuerySpec::Range {
                start_ns: q.bounds.start_ns,
                end_ns: q.bounds.end_ns,
                step_ns: 1_000_000_000,
            },
            limit: 1,
            direction: Direction::Forward,
        };
        let sp = match plan::plan(plan_expr, &qp, &ctx)? {
            Plan::Streams(sp) => sp,
            // Unreachable (an `Expr::Log` always plans to `Streams`) but
            // kept as a structured rejection, never a panic.
            Plan::Metric(_) | Plan::MetricBinary(_) => {
                return Err(ReadError::PipelineInvalid {
                    reason: "volume requires a log stream selector query".to_string(),
                });
            }
        };

        if let Some(e) = explain.as_mut() {
            e.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
        }
        let fingerprints = self.resolve_fingerprints(&sp.stage1_sql).await?;
        if fingerprints.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(e) = explain.as_mut() {
            e.push(
                "stage2_hydration",
                super::sql::stage2(&sp.streams_table, &fingerprints),
                None,
            );
        }
        let meta = self.hydrate(&sp.streams_table, &fingerprints).await?;

        let window = super::sql::TimeWindow {
            start_ns: q.bounds.start_ns,
            end_ns: q.bounds.end_ns,
        };
        let sql = super::sql::log_volume_rollup(&self.config.rollup_table, &fingerprints, window);
        if let Some(e) = explain.as_mut() {
            let routing = super::plan::RoutingDecision {
                chosen: super::plan::RouteChoice::Rollup,
                reason: "rollup: volume accepts matchers-only queries — always served from the \
                         rollup with zero body reads"
                    .to_string(),
            };
            e.set_routing(routing.clone());
            e.push("volume_read", sql.clone(), Some(routing.reason));
        }

        let mut rows: Vec<VolumeRow> = Vec::new();
        {
            // Scoped so the stream's pooled-connection lease drops before
            // the (pure-CPU) accumulation below.
            let mut stream = self
                .query_stream::<VolumeRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                rows.push(row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?);
            }
        }
        Ok(accumulate_volume(
            &rows,
            &meta,
            q.aggregate_by,
            &labels_to_match,
            !q.target_labels.is_empty(),
            q.limit,
        ))
    }

    /// `/api/logs/v1/detected_labels` (issue #170, docs/api.md §2.6):
    /// indexed stream labels ONLY, served by one server-side aggregation
    /// over `log_streams_idx` ([`super::sql::detected_labels`]) — never
    /// touching `log_samples`. `selector` is the optional `query=`
    /// scoping (matchers only, enforced at the API layer); `None` = the
    /// unscoped form. The reference's relevance filter applies here:
    /// static labels (`cluster`/`namespace`/`instance`/`pod`) always
    /// keep; any other key keeps iff at least one value is neither a
    /// float nor a UUID (`non_id_values > 0`).
    pub async fn detected_labels(
        &self,
        selector: Option<&Expr>,
        b: TimeBounds,
    ) -> Result<Vec<DetectedLabelOut>, ReadError> {
        self.detected_labels_inner(selector, b, None).await
    }

    /// [`LogQlEngine::detected_labels`] plus its `X-Pulsus-Explain`
    /// trace, in the same single pass (no second scan) — the
    /// `query_explained` contract.
    pub async fn detected_labels_explained(
        &self,
        selector: Option<&Expr>,
        b: TimeBounds,
    ) -> Result<(Vec<DetectedLabelOut>, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("detected_labels");
        let labels = self
            .detected_labels_inner(selector, b, Some(&mut explain))
            .await?;
        Ok((labels, explain))
    }

    async fn detected_labels_inner(
        &self,
        selector: Option<&Expr>,
        b: TimeBounds,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<DetectedLabelOut>, ReadError> {
        // Always >= 1 literal (`months_overlapping` never returns empty),
        // so the aggregation's month IN-list has no empty-IN hazard.
        let months = plan::months_overlapping(b.start_ns, b.end_ns);
        let fingerprints: Option<Vec<u64>> = match selector {
            None => None,
            Some(expr) => {
                let ctx = self.config.plan_ctx();
                // `limit`/`direction`/`step` are unused placeholders —
                // detected_labels never reads samples (the stats idiom).
                let qp = QueryParams {
                    spec: QuerySpec::Range {
                        start_ns: b.start_ns,
                        end_ns: b.end_ns,
                        step_ns: 1_000_000_000,
                    },
                    limit: 1,
                    direction: Direction::Forward,
                };
                let sp = match plan::plan(expr, &qp, &ctx)? {
                    Plan::Streams(sp) => sp,
                    // Unreachable via the API layer (it parses `query`
                    // with `parse_selector`) — kept as a structured
                    // rejection, never a panic.
                    Plan::Metric(_) | Plan::MetricBinary(_) => {
                        return Err(ReadError::PipelineInvalid {
                            reason: "detected_labels requires a log stream selector (matchers \
                                     only)"
                                .to_string(),
                        });
                    }
                };
                if let Some(e) = explain.as_mut() {
                    e.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
                }
                let fps = self.resolve_fingerprints(&sp.stage1_sql).await?;
                if fps.is_empty() {
                    // No matching streams — skip the aggregation query
                    // entirely (an empty fingerprint IN-list must never
                    // render).
                    return Ok(Vec::new());
                }
                Some(fps)
            }
        };
        let sql =
            super::sql::detected_labels(&self.config.streams_idx, &months, fingerprints.as_deref());
        if let Some(e) = explain.as_mut() {
            e.push("detected_labels", sql.clone(), None);
        }
        let mut out = Vec::new();
        let mut stream = self
            .query_stream::<DetectedLabelRow>(&sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
            // The reference keep rule: static labels always; anything
            // else only when NOT every value is float-or-UUID.
            if detected::is_static_detected_label(&row.key) || row.non_id_values > 0 {
                out.push(DetectedLabelOut {
                    label: row.key,
                    cardinality: row.cardinality,
                });
            }
        }
        Ok(out)
    }

    /// `/api/logs/v1/detected_fields` (issue #170, docs/api.md §2.6):
    /// per-entry fields from a <= `line_limit` sample of **post-pipeline
    /// matching** entries (issue #170 plan v2, reusing the #90
    /// fetch-until-limit contract):
    ///
    /// - no unpushed dropping stage ([`StreamsPlan::fetch_until_limit`]
    ///   false — bare selectors, line filters, non-dropping transforms):
    ///   ONE byte-identical [`super::sql::stage3`] scan with `LIMIT
    ///   line_limit` is provably the newest `line_limit` post-pipeline
    ///   matches (line-filter pushdown carries the exact predicate), the
    ///   O(line_limit) fast path;
    /// - a dropping stage (label filter / post-`line_format` line
    ///   filter): [`LogQlEngine::run_detected_fields_paged`] keyset-pages
    ///   until `line_limit` post-pipeline matches, window exhaustion, or
    ///   byte-budget exhaustion (`truncated = true`, surfaced as the
    ///   additive `pulsus_partial` response key).
    pub async fn detected_fields(
        &self,
        expr: &Expr,
        b: TimeBounds,
        line_limit: u32,
        field_limit: u32,
    ) -> Result<DetectedFields, ReadError> {
        self.detected_fields_inner(expr, b, line_limit, field_limit, None)
            .await
    }

    /// [`LogQlEngine::detected_fields`] plus its `X-Pulsus-Explain`
    /// trace, in the same single pass (no second scan) — the
    /// `query_explained` contract.
    pub async fn detected_fields_explained(
        &self,
        expr: &Expr,
        b: TimeBounds,
        line_limit: u32,
        field_limit: u32,
    ) -> Result<(DetectedFields, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("detected_fields");
        let fields = self
            .detected_fields_inner(expr, b, line_limit, field_limit, Some(&mut explain))
            .await?;
        Ok((fields, explain))
    }

    async fn detected_fields_inner(
        &self,
        expr: &Expr,
        b: TimeBounds,
        line_limit: u32,
        field_limit: u32,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<DetectedFields, ReadError> {
        let ctx = self.config.plan_ctx();
        // `limit = line_limit` drives the plan's scan/result sizing
        // exactly as a `/query_range` with the same limit would
        // (`scan_limit = line_limit × pipeline_scan_factor` on the
        // dropping path); newest-first sampling per the reference.
        let qp = QueryParams {
            spec: QuerySpec::Range {
                start_ns: b.start_ns,
                end_ns: b.end_ns,
                step_ns: 1_000_000_000,
            },
            limit: line_limit,
            direction: Direction::Backward,
        };
        let sp = match plan::plan(expr, &qp, &ctx)? {
            Plan::Streams(sp) => sp,
            Plan::Metric(_) | Plan::MetricBinary(_) => {
                return Err(ReadError::PipelineInvalid {
                    reason: "detected_fields requires a log stream selector query (a metric \
                             query has no per-entry fields)"
                        .to_string(),
                });
            }
        };
        // Compile before any I/O: a bad regex/template is a 400-class
        // rejection, never a wasted scan.
        let compiled = CompiledPipeline::compile(&sp.pipeline)?;

        if let Some(e) = explain.as_mut() {
            e.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
        }
        let fingerprints = self.resolve_fingerprints(&sp.stage1_sql).await?;
        if fingerprints.is_empty() {
            return Ok(DetectedFields {
                fields: Vec::new(),
                truncated: false,
            });
        }
        if let Some(e) = explain.as_mut() {
            e.push(
                "stage2_hydration",
                super::sql::stage2(&sp.streams_table, &fingerprints),
                None,
            );
        }
        let meta = self.hydrate(&sp.streams_table, &fingerprints).await?;
        let services = distinct_escaped_services(&meta);
        // Base labels parsed once per fingerprint, not per row (the
        // `StreamAccumulator` idiom).
        let base_labels: HashMap<u64, Vec<(String, String)>> = meta
            .iter()
            .map(|(fp, m)| (*fp, parse_flat_labels(&m.labels)))
            .collect();
        let window = super::sql::TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        };
        let mut acc = FieldAccumulator::new(field_limit);

        if !sp.fetch_until_limit {
            // Fast path — provably complete, not just fast: with no
            // unpushed dropping stage the pipeline cannot drop a line the
            // SQL didn't already filter exactly (line-filter pushdown
            // carries the exact predicate), so this single scan's
            // `LIMIT line_limit` rows ARE the newest `line_limit`
            // post-pipeline matches (`scan_limit == line_limit` by
            // construction). Never partial.
            let sql = super::sql::stage3(
                &sp.samples_table,
                &services,
                &fingerprints,
                window,
                &sp.line_filters,
                sp.direction,
                sp.scan_limit,
            );
            if let Some(e) = explain.as_mut() {
                e.push(
                    "detected_fields_read",
                    sql.clone(),
                    Some("single-scan: no unpushed dropping stage".to_string()),
                );
            }
            let mut rows: Vec<SampleRow> = Vec::new();
            {
                // Scoped so the stream's pooled-connection lease drops
                // before the (pure-CPU) field detection below.
                let mut stream = self
                    .query_stream::<SampleRow>(&sql, &self.budget_settings())
                    .await?;
                while let Some(row) = stream.next().await {
                    rows.push(row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?);
                }
            }
            let mut matched = 0u32;
            feed_detected_rows(
                &rows,
                &base_labels,
                &compiled,
                &mut acc,
                &mut matched,
                line_limit,
            );
            return Ok(DetectedFields {
                fields: acc.finish(),
                truncated: false,
            });
        }

        // Dropping sub-case: the pipeline can drop lines in-engine, so a
        // single pre-pipeline LIMIT could silently miss fields that match
        // only after the first `line_limit` raw rows (issue #170 plan v2's
        // review fix) — keyset-page until `line_limit` post-pipeline
        // matches, window exhaustion, or budget exhaustion.
        if let Some(e) = explain.as_mut() {
            let first_page_sql = super::sql::stage3_keyset(
                &sp.samples_table,
                &services,
                &fingerprints,
                window,
                super::sql::KeysetLower::First,
                sp.direction,
                &sp.line_filters,
                sp.scan_limit.max(1),
            );
            e.push(
                "detected_fields_read",
                first_page_sql,
                Some("paged: unpushed dropping stage".to_string()),
            );
        }
        let truncated = self
            .run_detected_fields_paged(
                &sp,
                &compiled,
                &base_labels,
                &services,
                &fingerprints,
                line_limit,
                &mut acc,
            )
            .await?;
        Ok(DetectedFields {
            fields: acc.finish(),
            truncated,
        })
    }

    /// The detected_fields fetch-until-limit paging loop (issue #170 plan
    /// v2) — a structural sibling of [`LogQlEngine::run_streams_paged`]
    /// feeding a [`FieldAccumulator`] + a post-pipeline matched-entry
    /// counter instead of a `StreamAccumulator`. Shares the #90 pieces
    /// verbatim: [`super::sql::stage3_keyset`] pages (PK-pruned,
    /// skip-index prefilters, keyset total order), [`advance_tail_cursor`]
    /// over the **raw** page (a page fully discarded by the pipeline never
    /// stalls the walk), [`LogQlEngine::paging_settings`]`(budget − spent)`
    /// with `wait_end_of_query = 1`, and the [`scan_budget_spent`]
    /// top-of-loop guard. Page row-bound = `sp.scan_limit` (`line_limit ×
    /// reader.logql_pipeline_scan_factor`). Returns `truncated`, per the
    /// #90 terminal branches:
    ///
    /// 1. `line_limit` post-pipeline matches collected → `false`;
    /// 2. page returns `< page_size` rows (window exhausted) → `false` —
    ///    the branch that reaches matches occurring after the first
    ///    `line_limit` raw rows;
    /// 3. first page alone overflows the budget → `QueryTooBroad`;
    /// 4. budget spent after >= 1 page → `true` (the fields accumulated so
    ///    far are returned, surfaced as `pulsus_partial`).
    ///
    /// The budget is `reader.logql_scan_budget_bytes` — deliberately the
    /// SAME bound a `/query_range` with the same dropping pipeline pays
    /// (detected_fields never scans more than the equivalent log query
    /// would), an approximate best-effort scan guard exactly as
    /// documented on [`LogQlEngine::run_streams_paged`].
    #[allow(clippy::too_many_arguments)]
    async fn run_detected_fields_paged(
        &self,
        sp: &StreamsPlan,
        compiled: &CompiledPipeline,
        base_labels: &HashMap<u64, Vec<(String, String)>>,
        services: &[String],
        fingerprints: &[u64],
        line_limit: u32,
        acc: &mut FieldAccumulator,
    ) -> Result<bool, ReadError> {
        let budget = self.config.scan_budget_bytes;
        let window = super::sql::TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        };
        let page_size = sp.scan_limit.max(1);
        let mut cursor: Option<TailCursor> = None;
        let mut spent: u64 = 0;
        let mut matched: u32 = 0;

        loop {
            // Never issue a zero cap (ClickHouse's *unlimited* sentinel) —
            // once the budget is spent, return partial (the first-page
            // `spent == 0` case never reaches here).
            if scan_budget_spent(spent, budget) {
                return Ok(true);
            }
            let page_cap = budget.saturating_sub(spent); // now always > 0
            let ks_lower = match cursor {
                None => super::sql::KeysetLower::First,
                Some(c) => super::sql::KeysetLower::After {
                    tuple: c.tuple,
                    offset: c.seen,
                },
            };
            let sql = super::sql::stage3_keyset(
                &sp.samples_table,
                services,
                fingerprints,
                window,
                ks_lower,
                sp.direction,
                &sp.line_filters,
                page_size,
            );

            // Fetch and fully drain one page; `read_bytes` is meaningful
            // only after the drain (wait_end_of_query=1). Scoped so the
            // stream's pooled-connection lease releases before the next
            // page.
            let mut rows: Vec<TailSampleRow> = Vec::new();
            let page_result: Result<Option<u64>, ReadError> = async {
                let mut stream = self
                    .query_stream::<TailSampleRow>(&sql, &self.paging_settings(page_cap))
                    .await?;
                while let Some(row) = stream.next().await {
                    rows.push(row.map_err(|e| map_read_error(e, budget))?);
                }
                Ok(stream.read_bytes())
            }
            .await;

            let read = match page_result {
                Ok(rb) => rb.unwrap_or(page_cap),
                Err(mapped) => {
                    if matches!(
                        mapped,
                        ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { .. })
                    ) {
                        // The #90 branch split: the FIRST page overflowing
                        // the FULL budget is a genuinely too-broad query ⇒
                        // propagate; a later page's positive-cap overflow
                        // keeps the fields so far and signals partial.
                        if spent == 0 {
                            return Err(mapped);
                        }
                        return Ok(true);
                    }
                    return Err(mapped);
                }
            };
            spent = spent.saturating_add(read);

            let fetched = u32::try_from(rows.len()).unwrap_or(u32::MAX);
            // Advance over the RAW page, not survivors — a page entirely
            // dropped by the pipeline must never stall the walk.
            cursor = advance_tail_cursor(cursor, &rows);
            let sample_rows: Vec<SampleRow> = rows
                .into_iter()
                .map(|r| SampleRow {
                    fingerprint: r.fingerprint,
                    timestamp_ns: r.timestamp_ns,
                    body: r.body,
                    structured_metadata: r.structured_metadata,
                })
                .collect();
            feed_detected_rows(
                &sample_rows,
                base_labels,
                compiled,
                acc,
                &mut matched,
                line_limit,
            );

            if matched >= line_limit {
                // Post-pipeline limit filled — complete, never partial.
                return Ok(false);
            }
            if fetched < page_size {
                // Window exhausted — complete over the whole window (this
                // is the branch that finds late-occurring matches).
                return Ok(false);
            }
        }
    }

    /// Builds a tail connection's [`TailSetup`] — plan + compiled
    /// pipeline — once, BEFORE the WebSocket upgrade. A metric
    /// expression or an uncompilable pipeline is a 400-class rejection
    /// here, never a wasted upgrade.
    pub fn tail_setup(&self, expr: &Expr, params: &QueryParams) -> Result<TailSetup, ReadError> {
        let ctx = self.config.plan_ctx();
        match plan::plan(expr, params, &ctx)? {
            Plan::Streams(sp) => {
                let compiled = CompiledPipeline::compile(&sp.pipeline)?;
                Ok(TailSetup {
                    plan: sp,
                    compiled,
                    expr: expr.clone(),
                    base_params: *params,
                    scan_floor_ns: spec_start_ns(&params.spec),
                    covered_months: (
                        plan::year_month(spec_start_ns(&params.spec)),
                        plan::year_month(spec_end_ns(&params.spec)),
                    ),
                    resolved: Vec::new(),
                })
            }
            Plan::Metric(_) | Plan::MetricBinary(_) => Err(ReadError::PipelineInvalid {
                reason: "tail requires a log stream query (metric queries cannot be tailed)"
                    .to_string(),
            }),
        }
    }

    /// Best-effort stage-1 month refresh (issue #94 v6-v8, scan-gated
    /// phase split): the stage-1 month IN-list is anchored to
    /// `[setup.scan_floor_ns, upper_ns]`, re-planning (reusing
    /// [`plan::plan`] — **no ClickHouse I/O**, a pure `stage1_sql` string
    /// rebuild) whenever that window's covered `(lo_month, hi_month)`
    /// differs from what `setup.plan` already covers.
    ///
    /// `narrow` is the producer's certification (see
    /// `logs_api/tail.rs::producer_loop`'s scan-gate rule, computed ONCE
    /// per iteration from a single clock read — recomputing it downstream
    /// from a fresh clock is a documented trap: it would misclassify
    /// steady-state live polls as catch-up and silently reintroduce
    /// lifetime-unbounded growth) that a COMPLETED full-span poll at the
    /// live edge has already dwelt >= [`TAIL_REGISTRATION_GRACE_NS`]. Only
    /// then does `scan_floor_ns` advance (monotonically, to
    /// `max(scan_floor_ns, lower_ns - GRACE)`); otherwise (catch-up,
    /// fall-behind, or still inside the hold) the floor stays frozen and
    /// the scan set widens upper-only — full-span, request-bounded, never
    /// lifetime-growing. See [`TailSetup`]'s doc for the full
    /// clamp-qualified coverage argument and residual (issue #134).
    ///
    /// The caller invokes this best-effort (`let _ = …`): on a re-plan
    /// error `setup` is left untouched and the tail keeps running on the
    /// PRIOR month set — it degrades to pre-#94 behaviour (new-month
    /// streams surface on the next successful refresh or a reconnect) and
    /// never errors the connection.
    pub fn tail_refresh_months(
        &self,
        setup: &mut TailSetup,
        lower_ns: i64,
        upper_ns: i64,
        narrow: bool,
    ) -> Result<(), ReadError> {
        let ctx = self.config.plan_ctx();
        refresh_tail_months(&ctx, setup, lower_ns, upper_ns, narrow)
    }

    /// One live-tail poll (issue #74; issue #94 resolve-and-remember
    /// revision): re-resolves stage-1 over the (now month-narrowed)
    /// `setup.plan`, MERGES the result into `setup.resolved` (the
    /// cumulative cache — see [`TailSetup`]'s doc for why this is
    /// data-loss-free despite the narrowed month window), hydrates and
    /// fetches one keyset page over the cached union, and runs the SAME
    /// `CompiledPipeline` the query path runs. The cursor advances past
    /// every *fetched* row (pipeline-dropped lines never re-fetch).
    pub async fn tail_poll(
        &self,
        setup: &mut TailSetup,
        lower: TailLower,
        upper_ns: i64,
        fetch_limit: u32,
    ) -> Result<TailPage, ReadError> {
        let prev = match lower {
            TailLower::After(c) => Some(c),
            TailLower::Start { .. } => None,
        };
        let new_fps = self.resolve_fingerprints(&setup.plan.stage1_sql).await?;
        merge_resolved(&mut setup.resolved, &new_fps);
        check_stream_cap(setup.resolved.len(), self.config.max_streams)?;
        if setup.resolved.is_empty() {
            return Ok(TailPage {
                streams: Vec::new(),
                next: prev,
                fetched: 0,
            });
        }
        // The full cumulative cache — not just this poll's narrow stage-1
        // result — feeds stage-2 hydration and the stage-3 fetch (issue
        // #94: the orphan-cache mechanism). A shared borrow, not a clone:
        // `setup` is not mutated again before this borrow's last use.
        let fingerprints = &setup.resolved;
        let meta = self
            .hydrate(&setup.plan.streams_table, fingerprints)
            .await?;
        let services = distinct_escaped_services(&meta);

        // Tail is forward-only (oldest→newest); `KeysetLower::First`
        // carries the API `start` bound in the window, later pages carry
        // the keyset (window `start_ns` is then unused by the Forward
        // After rendering).
        let start_ns = match lower {
            TailLower::Start { start_ns } => start_ns,
            TailLower::After(_) => 0,
        };
        let window = super::sql::TimeWindow {
            start_ns,
            end_ns: upper_ns,
        };
        let ks_lower = match lower {
            TailLower::Start { .. } => super::sql::KeysetLower::First,
            TailLower::After(c) => super::sql::KeysetLower::After {
                tuple: c.tuple,
                offset: c.seen,
            },
        };
        let sql = super::sql::stage3_keyset(
            &setup.plan.samples_table,
            &services,
            fingerprints,
            window,
            ks_lower,
            Direction::Forward,
            &setup.plan.line_filters,
            fetch_limit,
        );

        let mut rows: Vec<TailSampleRow> = Vec::new();
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule).
            let mut stream = self
                .query_stream::<TailSampleRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                rows.push(row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?);
            }
        }
        let fetched = u32::try_from(rows.len()).unwrap_or(u32::MAX);
        let next = advance_tail_cursor(prev, &rows);

        let sample_rows: Vec<SampleRow> = rows
            .into_iter()
            .map(|r| SampleRow {
                fingerprint: r.fingerprint,
                timestamp_ns: r.timestamp_ns,
                body: r.body,
                structured_metadata: r.structured_metadata,
            })
            .collect();
        let streams = run_pipeline_rows(sample_rows, &setup.compiled, &meta, fetch_limit);
        Ok(TailPage {
            streams,
            next,
            fetched,
        })
    }
}

/// The `start` bound of a [`QuerySpec`] (the `at` instant for an instant
/// query) — the initial covered-window lower anchor for [`TailSetup`]
/// (issue #94).
fn spec_start_ns(spec: &QuerySpec) -> i64 {
    match *spec {
        QuerySpec::Range { start_ns, .. } => start_ns,
        QuerySpec::Instant { at_ns } => at_ns,
    }
}

/// The `end` bound of a [`QuerySpec`] (the `at` instant for an instant
/// query) — the initial covered-window upper anchor for [`TailSetup`]
/// (issue #94).
fn spec_end_ns(spec: &QuerySpec) -> i64 {
    match *spec {
        QuerySpec::Range { end_ns, .. } => end_ns,
        QuerySpec::Instant { at_ns } => at_ns,
    }
}

/// Client-free core of [`LogQlEngine::tail_refresh_months`] (issue #94
/// v6-v8, scan-gated phase split), unit-testable with a [`PlanCtx`]
/// literal.
///
/// `narrow == true` is the producer's certification that a COMPLETED
/// full-span live-edge poll has already dwelt at least
/// [`TAIL_REGISTRATION_GRACE_NS`] (the scan gate — see
/// `logs_api/tail.rs::producer_loop`); ONLY then does `scan_floor_ns`
/// advance, monotonically, to `max(scan_floor_ns, lower_ns - GRACE)`.
/// Otherwise (catch-up, fall-behind, or still inside the hold) the floor
/// stays frozen and stage-1 re-plans (reusing [`plan::plan`] — **no
/// ClickHouse I/O**, a pure `stage1_sql` rebuild) to
/// `months_overlapping(scan_floor_ns, upper_ns)` whenever that window's
/// covered `(lo_month, hi_month)` pair differs from
/// `setup.covered_months` — widening upper-only, never narrowing, so a
/// connection that never reaches the live edge keeps the full-span
/// (request-bounded, not lifetime-growing) behaviour. Narrowing past a
/// fingerprint's registration month is safe ONLY because
/// [`TailSetup::resolved`] remembers it — this function alone does not
/// decide correctness. A non-`Streams` plan (unreachable — the setup expr
/// already planned as `Streams`) or a re-plan error leaves `setup`
/// untouched, so the caller can swallow the result and keep tailing on
/// the prior month set.
fn refresh_tail_months(
    ctx: &PlanCtx<'_>,
    setup: &mut TailSetup,
    lower_ns: i64,
    upper_ns: i64,
    narrow: bool,
) -> Result<(), ReadError> {
    if narrow {
        // The ONLY place the floor advances: `narrow` certifies a
        // completed full-span scan already dwelt >= GRACE at the live
        // edge (the scan-gate rule), so this can never skip a boundary in
        // compressed (catch-up) time.
        setup.scan_floor_ns = setup
            .scan_floor_ns
            .max(lower_ns.saturating_sub(TAIL_REGISTRATION_GRACE_NS));
    } // else: catch-up, fall-behind, or in-hold — floor frozen, set widens upper-only.
    let hi = upper_ns.max(lower_ns);
    let want = (plan::year_month(setup.scan_floor_ns), plan::year_month(hi));
    if want == setup.covered_months {
        return Ok(());
    }
    let mut qp = setup.base_params; // QueryParams: Copy
    if let QuerySpec::Range {
        start_ns, end_ns, ..
    } = &mut qp.spec
    {
        *start_ns = setup.scan_floor_ns;
        *end_ns = hi;
    }
    if let Plan::Streams(sp) = plan::plan(&setup.expr, &qp, ctx)? {
        setup.plan = sp;
        setup.covered_months = want;
    }
    Ok(())
}

/// Unions `new` into the cumulative resolved-fingerprint cache, sorted
/// and deduped (issue #94: the orphan-cache mechanism — a fingerprint
/// present in an earlier batch survives a later batch that no longer
/// resolves it, because its stage-1 month scrolled out of the current
/// poll window).
fn merge_resolved(cache: &mut Vec<u64>, new: &[u64]) {
    if new.is_empty() {
        return;
    }
    cache.extend_from_slice(new);
    cache.sort_unstable();
    cache.dedup();
}

/// The occurrence-count cursor update (round-4 adjudication #1): the new
/// tuple is the LAST raw row's; `seen` counts this page's trailing run
/// of rows equal to it, plus the previous `seen` when the tuple did not
/// change (the `OFFSET` already skipped those). Equal-tuple rows are
/// adjacent under the total `ORDER BY` (raw `body` tiebreaker), so the
/// trailing-run count is deterministic even under hash collisions. An
/// empty page leaves the cursor unchanged.
fn advance_tail_cursor(prev: Option<TailCursor>, rows: &[TailSampleRow]) -> Option<TailCursor> {
    let last = match rows.last() {
        Some(last) => last,
        None => return prev,
    };
    let bt = (last.timestamp_ns, last.fingerprint, last.body_hash);
    let run = rows
        .iter()
        .rev()
        .take_while(|r| (r.timestamp_ns, r.fingerprint, r.body_hash) == bt)
        .count() as u32;
    let carry = match prev {
        Some(c) if c.tuple == bt => c.seen,
        _ => 0,
    };
    Some(TailCursor {
        tuple: bt,
        seen: run.saturating_add(carry),
    })
}

/// The `resultType` a binary metric plan produces: `scalar` for a
/// leaf-less (pure-literal) tree, otherwise vector/matrix per the query
/// spec — the same rule the encoder applies to the evaluated result.
fn binary_result_type(node: &MetricNode, params: &QueryParams) -> &'static str {
    if node.leaves().is_empty() {
        "scalar"
    } else if matches!(params.spec, QuerySpec::Instant { .. }) {
        "vector"
    } else {
        "matrix"
    }
}

/// Doubles every literal `?` in `sql` before execution.
///
/// **Not part of the injection boundary** — this is a `clickhouse` crate
/// quirk, not a SQL-correctness concern: its `SqlBuilder` (`clickhouse`
/// 0.15's `sql::mod::SqlBuilder::new`) treats a bare `?` anywhere in the
/// query text as an unbound bind-argument placeholder (sqlx-style) and
/// fails the query with "unbound query argument" unless doubled (`??`
/// collapses back to one literal `?` before the text reaches the server).
/// This module's SQL is always fully rendered text with no bind
/// arguments, so every `?` is literal — most commonly from a LogQL regex's
/// own `(?:...)` non-capturing-group syntax (`escape::ch_regex_anchored`'s
/// `^(?:...)$` template always contains one), but also from any raw
/// matcher/line-filter value that happens to contain a literal `?`.
/// Applied only at the execution boundary ([`LogQlEngine::query_stream`]):
/// the canonical SQL text `plan`/`sql` generate — and what `PlanExplain`
/// surfaces to callers — is unaffected, so `tests/sql_snapshots.rs`'s
/// byte-exact assertions stay meaningful.
///
/// `pub(crate)`: issue #31's `metrics::exec::MetricsEngine` and issue
/// #57's `traces::exec::TraceEngine` reuse this same fix at their own
/// execution boundaries (their anchored `match(...)` regex predicates
/// carry the identical `^(?:...)$` literal-`?` shape), rather than
/// duplicating the doubling logic.
pub(crate) fn escape_query_placeholders(sql: &str) -> Cow<'_, str> {
    if sql.contains('?') {
        Cow::Owned(sql.replace('?', "??"))
    } else {
        Cow::Borrowed(sql)
    }
}

/// The pure transform/fan-out assembly (issue M6-09): runs already-
/// fetched stage-3 rows — **in arrival order**, which stage 3's global
/// `ORDER BY timestamp_ns` makes the requested direction's order —
/// through the compiled pipeline, truncates survivors at `result_limit`
/// **globally across streams** (AC9: never per-stream, and never
/// over-returning), then groups:
/// - transform path (`!mutates_labels`): by source fingerprint,
///   `labels_json` verbatim from hydration;
/// - fan-out path: by final label set, with a canonical re-rendered
///   `labels_json` and a deterministic content-hash fingerprint.
///
/// `pub` (not `pub(crate)`) deliberately: this is the ChClient-free pure
/// half of the streams pipeline path, and the allocation-regression
/// suite (`tests/logql_pipeline_alloc.rs`, review round 2) pins its
/// per-row allocation bounds from outside the crate — the same hermetic
/// surface the in-module unit tests use.
pub fn run_pipeline_rows(
    rows: Vec<SampleRow>,
    compiled: &super::pipeline::CompiledPipeline,
    meta: &HashMap<u64, StreamMetaRow>,
    result_limit: u32,
) -> Vec<StreamResult> {
    // A one-shot feed over the whole slice — byte-identical output and
    // per-row allocation profile to the pre-#90 monolithic function (the
    // `logql_pipeline_alloc`/`logql_pipeline_golden` suites pin both).
    let mut acc = StreamAccumulator::new(meta, result_limit);
    acc.feed(&rows, compiled);
    acc.into_streams()
}

/// The stateful grouping/counting core of [`run_pipeline_rows`], extracted
/// (issue #90) so the fetch-until-limit paging loop can stream multiple
/// keyset pages through ONE accumulator: fan-out/transform grouping and
/// the *global* `result_limit` truncation must span pages (a per-page
/// `run_pipeline_rows` + concat would regroup and re-truncate wrongly).
/// Owns the fp/label group maps + parsed base labels + the survivor
/// counter across [`StreamAccumulator::feed`] calls; the per-row label
/// scratch is re-created per `feed` (a page's borrows cannot outlive the
/// call) but reused across every row within the page, preserving the
/// zero-per-row-alloc dropped-row path.
pub struct StreamAccumulator<'m> {
    meta: &'m HashMap<u64, StreamMetaRow>,
    result_limit: u32,
    // Base labels parsed once per fingerprint, not per row.
    base_labels: HashMap<u64, Vec<(String, String)>>,
    // Transform path groups by source fingerprint; fan-out groups by the
    // canonical rendered labels JSON (sorted keys — it doubles as the
    // equality key). Two maps instead of a shared key enum so the fan-out
    // entry API can reuse its own `String` key without a per-row clone
    // (review round 2, finding 1); the fan-out value holds only the
    // per-group accumulator, and the map-owned key MOVES into
    // `StreamResult.labels_json` at final collection — never cloned out of
    // the entry, so high-cardinality fan-out (every row a new group) pays
    // no per-group key duplication either (review round 3).
    fp_groups: HashMap<u64, StreamResult>,
    label_groups: HashMap<String, FanOutGroup>,
    survivors: u32,
}

impl<'m> StreamAccumulator<'m> {
    pub fn new(meta: &'m HashMap<u64, StreamMetaRow>, result_limit: u32) -> Self {
        let mut base_labels: HashMap<u64, Vec<(String, String)>> = HashMap::new();
        for (fp, m) in meta {
            base_labels.insert(*fp, parse_flat_labels(&m.labels));
        }
        Self {
            meta,
            result_limit,
            base_labels,
            fp_groups: HashMap::new(),
            label_groups: HashMap::new(),
            survivors: 0,
        }
    }

    /// Feeds one page of rows in arrival (direction) order — arrival order
    /// IS the response order, so the global `result_limit` truncation
    /// below is correct across pages. Returns `true` once `survivors ==
    /// result_limit` (the caller stops paging).
    pub fn feed(
        &mut self,
        rows: &[SampleRow],
        compiled: &super::pipeline::CompiledPipeline,
    ) -> bool {
        let Self {
            meta,
            result_limit,
            base_labels,
            fp_groups,
            label_groups,
            survivors,
        } = self;
        let fan_out = compiled.mutates_labels();
        // One label scratch reused across every row of this page (issue
        // #72 review round 1, finding 3): `run_into` clears and refills the
        // same vector — zero per-row label-vector allocations on the
        // dropped-row (zero-structured-metadata) path.
        let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
        // The structured-metadata merge buffers (issue #97): reused across SM-
        // bearing rows (clear + refill, capacity-amortized), never a fresh
        // per-row allocation of the label vector itself. `merge_buf` holds the
        // merged result; `sm_buf` is the SM-pair parse scratch. Only SM-bearing
        // rows touch them — the empty-SM path never allocates or clears them.
        let mut merge_buf: Vec<(String, String)> = Vec::new();
        let mut sm_buf: Vec<(String, String)> = Vec::new();
        // The Cow label scratch `run_into` fills for SM-bearing rows. Held
        // `'static`-tagged (always empty) between rows and re-tagged per row so
        // its allocation is reused across the page — never a fresh per-row
        // allocation (issue #97 review round 1, finding 2 / AC-12). See
        // `eval_structured_metadata_row` for why the reuse goes through a
        // by-value helper rather than a hoisted `&mut` binding.
        let mut sm_scratch: LabelScratch<'static> = Vec::new();

        for row in rows {
            if *survivors >= *result_limit {
                break;
            }
            let Some(m) = meta.get(&row.fingerprint) else {
                continue;
            };
            let base = &base_labels[&row.fingerprint];

            if row.structured_metadata.is_empty() {
                // Zero-structured-metadata fast path — UNCHANGED (the
                // `logql_pipeline_alloc` golden pins its zero-per-row
                // profile; AC-8 byte-identity for pre-#97 data).
                let Some(line) = compiled.run_into(&row.body, base, &mut scratch) else {
                    continue;
                };
                *survivors += 1;
                if fan_out {
                    // Render the canonical JSON DIRECTLY from the sorted
                    // borrowed scratch (round-2 finding 1: no owned
                    // intermediate label vector, no second clone at render
                    // time). Per surviving row this costs exactly the
                    // `labels_json` string (needed as the group key either
                    // way) + the owned output line; the `StreamResult` fields
                    // materialize once per NEW group only.
                    scratch.sort_unstable();
                    push_fanout_entry(
                        label_groups,
                        &scratch,
                        row.timestamp_ns,
                        line.into_owned(),
                        &m.service,
                    );
                } else {
                    fp_groups
                        .entry(row.fingerprint)
                        .or_insert_with(|| StreamResult {
                            fingerprint: row.fingerprint,
                            service: m.service.clone(),
                            labels_json: m.labels.clone(),
                            entries: Vec::new(),
                        })
                        .entries
                        .push((row.timestamp_ns, line.into_owned()));
                }
            } else {
                // Structured-metadata-bearing row (issue #97): merge the
                // cached base labels + parsed SM into the reused owned buffer
                // (colliding SM keys renamed `_extracted`, per the oracle),
                // then run the pipeline over that contiguous base. SM changes
                // the label set, so these rows ALWAYS fan out (matching Loki's
                // per-entry SM fan-out). Only SM-bearing rows pay this cost.
                merge_labels_with_structured_metadata(
                    base,
                    &row.structured_metadata,
                    &mut merge_buf,
                    &mut sm_buf,
                );
                // Reuse `sm_scratch`'s allocation across rows: the helper takes
                // it by value (fresh per-row lifetime for the `merge_buf`
                // borrow), `recycle_label_scratch` returns the same allocation.
                let (survived, used) = eval_structured_metadata_row(
                    compiled,
                    &row.body,
                    &merge_buf,
                    label_groups,
                    row.timestamp_ns,
                    &m.service,
                    sm_scratch,
                );
                sm_scratch = recycle_label_scratch(used);
                if survived {
                    *survivors += 1;
                }
            }
        }

        *survivors >= *result_limit
    }

    pub fn into_streams(self) -> Vec<StreamResult> {
        self.fp_groups
            .into_values()
            .chain(
                self.label_groups
                    .into_iter()
                    .map(|(labels_json, g)| StreamResult {
                        fingerprint: g.fingerprint,
                        service: g.service,
                        labels_json,
                        entries: g.entries,
                    }),
            )
            .collect()
    }
}

// ---------------------------------------------------------------------
// Issue M6-10: the client-aggregated metric core — pure over fetched
// rows, `pub` like `run_pipeline_rows` so the hermetic golden suite
// (`tests/logql_metric_agg_golden.rs`) and the allocation gate
// (`tests/logql_pipeline_alloc.rs`) pin it from outside the crate.
// ---------------------------------------------------------------------

/// The evaluation window for one client-aggregated metric query.
/// `step_ns: None` = instant (one bucket over the whole window);
/// `Some(step)` = the M1 tumbling bucket contract (`floor(ts/step) *
/// step`, matching the SQL path's `intDiv` bucketing byte-for-byte).
#[derive(Debug, Clone, Copy)]
pub struct ClientWindow {
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: Option<u64>,
}

/// The instant-mode bucket key (any constant works — there is exactly
/// one bucket).
const INSTANT_BUCKET: i64 = 0;

/// How many rows the streaming client-aggregation fetch buffers between
/// folds into [`ClientAggState`] — bounds transient memory without
/// per-row fold overhead (review round 1, finding 1).
const CLIENT_AGG_CHUNK_ROWS: usize = 8_192;

fn bucket_of(ts_ns: i64, step_ns: Option<u64>) -> i64 {
    match step_ns {
        Some(step) => {
            // i128 intermediates (review round 3): for a timestamp near
            // `i64::MIN` with a non-dividing step, the FLOORED quotient
            // re-multiplied by step lands up to one step below the
            // timestamp — which can sit just below `i64::MIN` (e.g.
            // `i64::MIN + 1` at step 3 floors to `i64::MIN - 1`), a
            // debug panic / release wrap in i64. `step_ns > 0` and
            // `<= i64::MAX` are guaranteed by `ClientAggState::new`'s
            // grid guard before any row is bucketed.
            let step = step as i128;
            clamp_bucket((ts_ns as i128).div_euclid(step) * step)
        }
        None => INSTANT_BUCKET,
    }
}

/// Converts an i128 bucket start back to the i64 point-timestamp domain.
/// Only the sliver within one step below `i64::MIN` (or, symmetrically,
/// above `i64::MAX`) can fall outside — centuries beyond any real
/// nanosecond timestamp — and it clamps deterministically; both
/// [`bucket_of`] and [`bucket_grid`] clamp IDENTICALLY, so data-driven
/// buckets and the `absent_over_time` grid stay membership-consistent.
fn clamp_bucket(bucket: i128) -> i64 {
    i64::try_from(bucket).unwrap_or(if bucket < 0 { i64::MIN } else { i64::MAX })
}

/// Streaming per-bucket accumulator for every over-time reducer except
/// `quantile_over_time` (which needs the full value set). Welford's
/// algorithm for mean/M2 (population stddev/stdvar); first/last are
/// timestamp-anchored, order-independent.
#[derive(Debug, Clone)]
struct SimpleAcc {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    mean: f64,
    m2: f64,
    first_ts: i64,
    first_v: f64,
    last_ts: i64,
    last_v: f64,
}

impl SimpleAcc {
    fn new(ts_ns: i64, v: f64) -> Self {
        SimpleAcc {
            count: 1,
            sum: v,
            min: v,
            max: v,
            mean: v,
            m2: 0.0,
            first_ts: ts_ns,
            first_v: v,
            last_ts: ts_ns,
            last_v: v,
        }
    }

    fn add(&mut self, ts_ns: i64, v: f64) {
        self.count += 1;
        self.sum += v;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        let delta = v - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (v - self.mean);
        // Equal-timestamp tie-break (review round 2, finding 2): the
        // pinned PulsusDB rule — `first` takes the SMALLEST value among
        // samples tied at the minimum timestamp, `last` the LARGEST at
        // the maximum (`total_cmp` so NaN ties cannot flap). Fully
        // input-order-independent, so the reducer is deterministic even
        // if the scan's stable ordering ever changed. The reference's
        // own tie order for identical timestamps is unspecified; ours is
        // pinned here and documented (features.md §2).
        if ts_ns < self.first_ts || (ts_ns == self.first_ts && v.total_cmp(&self.first_v).is_lt()) {
            self.first_ts = ts_ns;
            self.first_v = v;
        }
        if ts_ns > self.last_ts || (ts_ns == self.last_ts && v.total_cmp(&self.last_v).is_gt()) {
            self.last_ts = ts_ns;
            self.last_v = v;
        }
    }
}

/// One bucket's state: streaming stats, or the full value set for
/// `quantile_over_time`.
#[derive(Debug, Clone)]
enum BucketAcc {
    Simple(SimpleAcc),
    Values(Vec<f64>),
}

impl BucketAcc {
    fn new(op: RangeAggOp, ts_ns: i64, v: f64) -> Self {
        if matches!(op, RangeAggOp::QuantileOverTime) {
            BucketAcc::Values(vec![v])
        } else {
            BucketAcc::Simple(SimpleAcc::new(ts_ns, v))
        }
    }

    fn add(&mut self, ts_ns: i64, v: f64) {
        match self {
            BucketAcc::Simple(acc) => acc.add(ts_ns, v),
            BucketAcc::Values(vals) => vals.push(v),
        }
    }

    /// Finishes the bucket into its reducer value.
    fn finish(self, op: RangeAggOp, rate_window_ns: Option<u64>, quantile: Option<f64>) -> f64 {
        match self {
            BucketAcc::Values(mut vals) => quantile_of(&mut vals, quantile.unwrap_or(f64::NAN)),
            BucketAcc::Simple(acc) => match op {
                // Oracle-probed: `rate` over an unwrapped range is the
                // per-second SUM of values (count-shaped inputs
                // contribute 1.0 each, so the un-piped semantic is
                // unchanged); `bytes_rate` likewise.
                RangeAggOp::Rate | RangeAggOp::BytesRate => apply_rate(acc.sum, rate_window_ns),
                RangeAggOp::CountOverTime => acc.count as f64,
                RangeAggOp::BytesOverTime | RangeAggOp::SumOverTime => acc.sum,
                RangeAggOp::AvgOverTime => acc.mean,
                RangeAggOp::MinOverTime => acc.min,
                RangeAggOp::MaxOverTime => acc.max,
                RangeAggOp::StddevOverTime => (acc.m2 / acc.count as f64).sqrt(),
                RangeAggOp::StdvarOverTime => acc.m2 / acc.count as f64,
                RangeAggOp::FirstOverTime => acc.first_v,
                RangeAggOp::LastOverTime => acc.last_v,
                // Absent is the dedicated presence branch in
                // `run_client_agg_rows`; quantile is `Values`-backed.
                RangeAggOp::QuantileOverTime | RangeAggOp::AbsentOverTime => {
                    unreachable!("dispatched before BucketAcc::finish")
                }
            },
        }
    }
}

/// The reference oracle's quantile semantics (live-probed: `q=0.9` over
/// `1,2,3,4` is `3.7` — linear interpolation on the sorted values):
/// `q < 0` → `-Inf`, `q > 1` → `+Inf`, NaN propagates.
fn quantile_of(values: &mut [f64], q: f64) -> f64 {
    if values.is_empty() || q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    values.sort_by(f64::total_cmp);
    let rank = q * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let weight = rank - rank.floor();
    values[lower] * (1.0 - weight) + values[upper] * weight
}

/// Renders a SORTED label set as the oracle's series shape
/// (`{a="b", c="d"}`) for the surviving-`__error__` query failure.
/// Values are escaped with the same mandatory-set escaper the canonical
/// labels JSON uses ([`push_json_string`] — quotes, backslashes, and
/// control characters; the shape the reference renders with Go-style
/// quoting), so hostile parsed label values can never produce malformed
/// `{k="v"}` text (review round 1, finding 4).
fn render_series_labels(sorted: &[(String, String)]) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in sorted.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(k);
        out.push('=');
        push_json_string(&mut out, v);
    }
    out.push('}');
    out
}

/// The full step-bucket grid over `(start, end]` — `absent_over_time`
/// must emit `1.0` for EMPTY buckets, which the data-driven accumulators
/// never see.
fn bucket_grid(start_ns: i64, end_ns: i64, step_ns: u64) -> Vec<i64> {
    if step_ns == 0 || step_ns > i64::MAX as u64 || end_ns <= start_ns {
        return Vec::new();
    }
    // i128 intermediates + the shared clamp, exactly like [`bucket_of`]
    // (review round 3): `k * step` for the lowest grid bucket can sit
    // one sliver below `i64::MIN`. Buckets containing any ts in
    // `(start, end]`: floor((start+1)/step) through floor(end/step).
    // The grid size passed `MAX_CLIENT_AGG_BUCKETS` before this runs.
    let step = step_ns as i128;
    let first = (start_ns as i128 + 1).div_euclid(step);
    let last = (end_ns as i128).div_euclid(step);
    (first..=last).map(|k| clamp_bucket(k * step)).collect()
}

/// The evaluation-bucket cap for client-aggregated range queries: a
/// `(end - start) / step` grid larger than this is rejected as
/// `QueryTooBroad(MetricBuckets)` BEFORE any grid or accumulator
/// materialization (review round 1, finding 2 — an `absent_over_time`
/// over a huge range with a tiny step must never allocate an
/// attacker-sized grid). 11 000 matches the ecosystem-standard
/// points-per-range-query ceiling. A documented constant, not a config
/// field (the `DEFAULT_MAX_STREAMS` precedent).
pub const MAX_CLIENT_AGG_BUCKETS: u64 = 11_000;

/// The exact-quantile retention cap: `quantile_over_time` is the one
/// reducer whose state grows with surviving rows (every value is kept
/// for the interpolation sort) rather than with `buckets x series`.
/// Past this many retained values (~32 MB of f64) the query aborts as
/// `QueryTooBroad(QuantileValues)` — complete-or-error, never OOM
/// (review round 1, finding 1's quantile bound).
pub const MAX_QUANTILE_VALUES: u64 = 4_000_000;

/// Derived-series cap for client-aggregated metric queries: the number
/// of distinct output groups (final label sets, or fingerprints on the
/// non-mutating path) a single query may materialize. Bounds the last
/// unbounded axis of reducer state — total accumulators are then
/// `<= MAX_CLIENT_AGG_SERIES x MAX_CLIENT_AGG_BUCKETS`. 500 matches the
/// reference oracle's default series ceiling (it likewise ERRORS, never
/// truncates, past it). A documented constant, not a config field (the
/// `DEFAULT_MAX_STREAMS` / `MAX_CLIENT_AGG_BUCKETS` precedent); operator-
/// scale tuning routes to #25.
pub const MAX_CLIENT_AGG_SERIES: u64 = 500;

/// Streaming client-aggregation state (issue M6-10, review round 1
/// finding 1): rows fold into reducer state as they arrive so process
/// memory stays `O(buckets x series)` (+ the caller's bounded chunk)
/// instead of retaining the whole raw scan. The pure
/// [`run_client_agg_rows`] wrapper drives it over a slice for the
/// hermetic golden/allocation suites; the engine drives it chunk-wise
/// off the live row stream.
struct ClientAggState<'q> {
    compiled: &'q super::pipeline::CompiledPipeline,
    client: &'q ClientAgg,
    window: ClientWindow,
    rate_window_ns: Option<u64>,
    /// Base labels once per fingerprint, in the same shape the SQL
    /// metric path exposes (`series_labels`: canonical JSON labels +
    /// the physical `service` column re-injected as `service_name`,
    /// sorted).
    base_labels: HashMap<u64, Vec<(String, String)>>,
    fan_out: bool,
    /// `absent_over_time`'s selector-wide presence set (plan v2 D2).
    present: BTreeSet<i64>,
    /// Non-mutating pipelines group by fingerprint (zero per-row
    /// allocations — the alloc-gate path).
    fp_groups: HashMap<u64, BTreeMap<i64, BucketAcc>>,
    /// Label-mutating/unwrapping pipelines group by the rendered final
    /// label set.
    label_groups: HashMap<String, (LabelSet, BTreeMap<i64, BucketAcc>)>,
    /// Total values retained across every quantile accumulator, charged
    /// against [`MAX_QUANTILE_VALUES`].
    quantile_values: u64,
}

impl<'q> ClientAggState<'q> {
    /// Validates the bucket grid BEFORE any accumulation (finding 2) and
    /// snapshots the per-fingerprint base labels.
    fn new(
        compiled: &'q super::pipeline::CompiledPipeline,
        meta: &HashMap<u64, StreamMetaRow>,
        client: &'q ClientAgg,
        window: ClientWindow,
        rate_window_ns: Option<u64>,
    ) -> Result<Self, ReadError> {
        if let Some(step) = window.step_ns {
            let buckets = grid_bucket_count(window.start_ns, window.end_ns, step);
            if buckets > MAX_CLIENT_AGG_BUCKETS {
                return Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets {
                    buckets,
                    cap: MAX_CLIENT_AGG_BUCKETS,
                }));
            }
        }
        let mut base_labels: HashMap<u64, Vec<(String, String)>> = HashMap::new();
        for (fp, m) in meta {
            base_labels.insert(*fp, series_labels(m));
        }
        Ok(ClientAggState {
            compiled,
            client,
            window,
            rate_window_ns,
            base_labels,
            fan_out: compiled.metric_mutates_labels(),
            present: BTreeSet::new(),
            fp_groups: HashMap::new(),
            label_groups: HashMap::new(),
            quantile_values: 0,
        })
    }

    /// Folds one batch of rows into the reducer state: each row runs the
    /// compiled pipeline (`run_metric_into` — unwrap executes,
    /// `__error__` annotates in stage order), FAILS the query on any
    /// surviving nonempty `__error__` (adjudication #1, oracle-matched
    /// message), and accumulates per `(final-label-set, bucket)`. One
    /// label scratch is reused across the whole batch (the #72
    /// allocation discipline).
    fn push_rows(&mut self, rows: &[MetricScanRow]) -> Result<(), ReadError> {
        let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
        let is_absent = matches!(self.client.range_op, RangeAggOp::AbsentOverTime);
        for row in rows {
            let Some(base) = self.base_labels.get(&row.fingerprint) else {
                continue;
            };
            let (line, value) = match self.compiled.run_metric_into(&row.body, base, &mut scratch) {
                MetricRun::Dropped => continue,
                MetricRun::Kept { line, value } => (line, value),
            };
            check_surviving_error(&scratch)?;
            let bucket = bucket_of(row.timestamp_ns, self.window.step_ns);
            if is_absent {
                // Selector-wide presence (plan v2 D2): count surviving
                // lines per bucket across ALL fingerprints/label sets.
                self.present.insert(bucket);
                continue;
            }
            let v = match self.client.value {
                ClientValue::Count => 1.0,
                ClientValue::Bytes => line.len() as f64,
                ClientValue::Unwrap => match value {
                    Some(v) => v,
                    // Defensive: a `None` unwrap value always carries a
                    // nonempty `__error__` (checked above) unless a
                    // filter dropped the line — unreachable, but never a
                    // silent 0.
                    None => continue,
                },
            };
            let op = self.client.range_op;
            if matches!(op, RangeAggOp::QuantileOverTime) {
                self.quantile_values += 1;
                if self.quantile_values > MAX_QUANTILE_VALUES {
                    return Err(ReadError::QueryTooBroad(TooBroadReason::QuantileValues {
                        count: self.quantile_values,
                        cap: MAX_QUANTILE_VALUES,
                    }));
                }
            }
            let buckets = if self.fan_out {
                scratch.sort_unstable();
                let key = render_labels_json_sorted(&scratch);
                let groups = self.label_groups.len() as u64;
                match self.label_groups.entry(key) {
                    std::collections::hash_map::Entry::Occupied(e) => &mut e.into_mut().1,
                    std::collections::hash_map::Entry::Vacant(e) => {
                        if groups >= MAX_CLIENT_AGG_SERIES {
                            return Err(ReadError::QueryTooBroad(TooBroadReason::MetricSeries {
                                cap: MAX_CLIENT_AGG_SERIES,
                            }));
                        }
                        let labels: LabelSet = scratch
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();
                        &mut e.insert((labels, BTreeMap::new())).1
                    }
                }
            } else {
                let groups = self.fp_groups.len() as u64;
                if !self.fp_groups.contains_key(&row.fingerprint) && groups >= MAX_CLIENT_AGG_SERIES
                {
                    return Err(ReadError::QueryTooBroad(TooBroadReason::MetricSeries {
                        cap: MAX_CLIENT_AGG_SERIES,
                    }));
                }
                self.fp_groups.entry(row.fingerprint).or_default()
            };
            match buckets.entry(bucket) {
                std::collections::btree_map::Entry::Occupied(mut e) => {
                    e.get_mut().add(row.timestamp_ns, v)
                }
                std::collections::btree_map::Entry::Vacant(e) => {
                    e.insert(BucketAcc::new(op, row.timestamp_ns, v));
                }
            }
        }
        Ok(())
    }

    /// Finishes every accumulator into the metric result.
    /// `absent_over_time` emits at most ONE series over the (pre-capped)
    /// bucket grid; the other reducers emit per surviving group.
    fn finish(self) -> QueryResult {
        let is_instant = self.window.step_ns.is_none();
        if matches!(self.client.range_op, RangeAggOp::AbsentOverTime) {
            let labels: LabelSet = self.client.absent_labels.clone();
            return if is_instant {
                if self.present.is_empty() {
                    QueryResult::Vector(vec![VectorSample { labels, value: 1.0 }])
                } else {
                    QueryResult::Vector(Vec::new())
                }
            } else {
                let step = self.window.step_ns.unwrap_or(0);
                let points: Vec<(i64, f64)> =
                    bucket_grid(self.window.start_ns, self.window.end_ns, step)
                        .into_iter()
                        .filter(|b| !self.present.contains(b))
                        .map(|b| (b, 1.0))
                        .collect();
                if points.is_empty() {
                    QueryResult::Matrix(Vec::new())
                } else {
                    QueryResult::Matrix(vec![MatrixSeries { labels, points }])
                }
            };
        }

        let base_labels = self.base_labels;
        let groups: Vec<(LabelSet, BTreeMap<i64, BucketAcc>)> = if self.fan_out {
            self.label_groups.into_values().collect()
        } else {
            self.fp_groups
                .into_iter()
                .filter_map(|(fp, buckets)| base_labels.get(&fp).map(|l| (l.clone(), buckets)))
                .collect()
        };
        let op = self.client.range_op;
        let rate_window_ns = self.rate_window_ns;
        let param = self.client.param;
        if is_instant {
            QueryResult::Vector(
                groups
                    .into_iter()
                    .filter_map(|(labels, mut buckets)| {
                        buckets.remove(&INSTANT_BUCKET).map(|acc| VectorSample {
                            labels,
                            value: acc.finish(op, rate_window_ns, param),
                        })
                    })
                    .collect(),
            )
        } else {
            QueryResult::Matrix(
                groups
                    .into_iter()
                    .map(|(labels, buckets)| MatrixSeries {
                        labels,
                        points: buckets
                            .into_iter()
                            .map(|(b, acc)| (b, acc.finish(op, rate_window_ns, param)))
                            .collect(),
                    })
                    .collect(),
            )
        }
    }
}

/// The bucket count the grid guard charges: how many step buckets the
/// `(start, end]` window touches (0 for an empty/inverted window).
/// Checked `i128` arithmetic throughout (review round 2, finding 1): the
/// full `i64` timestamp range at `step = 1` is ~2^64 buckets — a plain
/// `i64` count would panic/wrap PAST the cap. Anything unrepresentable
/// or degenerate (a zero step, a step wider than `i64` — both also
/// structurally rejected upstream) saturates to `u64::MAX`, which the
/// caller's cap comparison turns into the same named too-broad error.
fn grid_bucket_count(start_ns: i64, end_ns: i64, step_ns: u64) -> u64 {
    if end_ns <= start_ns {
        return 0;
    }
    if step_ns == 0 || step_ns > i64::MAX as u64 {
        // Defensive: a zero step is `InvalidStep` at the planner and a
        // >i64 step never comes out of `parse_step`; saturate so the
        // guard rejects rather than ever reaching `bucket_of`'s
        // `div_euclid`.
        return u64::MAX;
    }
    let step = step_ns as i128;
    // Buckets containing any ts in (start, end]: floor((start+1)/step)
    // through floor(end/step). i128 makes `start + 1` and the span
    // arithmetic overflow-free for every i64 input; `checked_*` is
    // belt-and-braces per the review disposition.
    let first = (start_ns as i128 + 1).div_euclid(step);
    let last = (end_ns as i128).div_euclid(step);
    match last.checked_sub(first).and_then(|d| d.checked_add(1)) {
        Some(count) if count <= 0 => 0,
        Some(count) => u64::try_from(count).unwrap_or(u64::MAX),
        None => u64::MAX,
    }
}

/// The pure client-aggregated evaluation (issue M6-10): the slice-driven
/// wrapper over [`ClientAggState`] the hermetic golden/allocation suites
/// pin (the engine streams rows into the same state chunk-wise instead
/// of buffering the scan — review round 1, finding 1).
///
/// Vector aggregations are NOT applied here — the caller finishes them
/// (`apply_vector_aggs`), mirroring the SQL path.
pub fn run_client_agg_rows(
    rows: &[MetricScanRow],
    compiled: &super::pipeline::CompiledPipeline,
    meta: &HashMap<u64, StreamMetaRow>,
    client: &ClientAgg,
    window: ClientWindow,
    rate_window_ns: Option<u64>,
) -> Result<QueryResult, ReadError> {
    let mut state = ClientAggState::new(compiled, meta, client, window, rate_window_ns)?;
    state.push_rows(rows)?;
    Ok(state.finish())
}

/// Adjudication #1: a line whose `__error__` is nonempty after the FULL
/// pipeline fails the metric query with the oracle-matched named error —
/// never silent exclusion.
fn check_surviving_error(labels: &[(Cow<'_, str>, Cow<'_, str>)]) -> Result<(), ReadError> {
    let Some((_, err)) = labels
        .iter()
        .find(|(k, v)| k == ERROR_LABEL && !v.is_empty())
    else {
        return Ok(());
    };
    let mut sorted: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    sorted.sort();
    Err(ReadError::MetricPipelineError {
        error_type: err.to_string(),
        series: render_series_labels(&sorted),
    })
}

// ---------------------------------------------------------------------
// Issue M6-10: binary operations over metric results.
// ---------------------------------------------------------------------

/// Applies one binary operator to a pair of numbers, operand order
/// preserved (noncommutative ops are never reordered — plan v2 D4).
fn arith(op: BinOp, l: f64, r: f64) -> f64 {
    match op {
        BinOp::Add => l + r,
        BinOp::Sub => l - r,
        BinOp::Mul => l * r,
        BinOp::Div => l / r,
        BinOp::Mod => l % r,
        BinOp::Pow => l.powf(r),
        // Comparisons/set ops never reach `arith` (dispatched below).
        _ => unreachable!("arith called with a non-arithmetic operator"),
    }
}

fn compare(op: BinOp, l: f64, r: f64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Neq => l != r,
        BinOp::Gt => l > r,
        BinOp::Gte => l >= r,
        BinOp::Lt => l < r,
        BinOp::Lte => l <= r,
        _ => unreachable!("compare called with a non-comparison operator"),
    }
}

fn is_set_op(op: BinOp) -> bool {
    matches!(op, BinOp::And | BinOp::Or | BinOp::Unless)
}

/// One scalar-side application preserving orientation:
/// `scalar_on_left = false` → `vector_value OP scalar`;
/// `true` → `scalar OP vector_value`. For comparisons the VECTOR value
/// is kept on a filter match (oracle-probed: `5 < vec(10)` keeps `10`);
/// under `bool` every sample stays with value 0/1.
fn scalar_apply(
    op: BinOp,
    return_bool: bool,
    scalar: f64,
    v: f64,
    scalar_on_left: bool,
) -> Option<f64> {
    let (l, r) = if scalar_on_left {
        (scalar, v)
    } else {
        (v, scalar)
    };
    if op.is_comparison() {
        let hit = compare(op, l, r);
        if return_bool {
            Some(if hit { 1.0 } else { 0.0 })
        } else {
            hit.then_some(v)
        }
    } else {
        Some(arith(op, l, r))
    }
}

/// Combines two evaluated metric results (issue M6-10, extended by #91).
/// Scope: vector⊗scalar in BOTH orientations, vector⊗vector and
/// matrix⊗matrix with one-to-one AND `group_left`/`group_right` vector
/// matching (`on`/`ignoring` signatures), `bool`, and the `and`/`or`/
/// `unless` set operations. Matrix binops are an INDEPENDENT per-step
/// instant join (Prometheus/Loki re-evaluate the instant join per
/// timestamp — see [`combine_matrices`]). `matching` is the parsed
/// clause, `None` for default full-label one-to-one. `pub` for the
/// hermetic golden suite.
pub fn combine_binary(
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: QueryResult,
    rhs: QueryResult,
) -> Result<QueryResult, ReadError> {
    match (lhs, rhs) {
        (QueryResult::Scalar(l), QueryResult::Scalar(r)) => {
            if is_set_op(op) {
                // Oracle-probed: a set operation against a scalar is a
                // named 400 ("unexpected literal for ... logical/set
                // binary operation").
                return Err(set_op_scalar_error(op));
            }
            // Oracle-probed: scalar⊗scalar comparison yields 0/1 with or
            // without `bool`.
            let v = if op.is_comparison() {
                if compare(op, l, r) { 1.0 } else { 0.0 }
            } else {
                arith(op, l, r)
            };
            Ok(QueryResult::Scalar(v))
        }
        (
            QueryResult::Scalar(s),
            vector_side @ (QueryResult::Vector(_) | QueryResult::Matrix(_)),
        ) => {
            if is_set_op(op) {
                return Err(set_op_scalar_error(op));
            }
            Ok(map_samples(vector_side, |v| {
                scalar_apply(op, return_bool, s, v, true)
            }))
        }
        (
            vector_side @ (QueryResult::Vector(_) | QueryResult::Matrix(_)),
            QueryResult::Scalar(s),
        ) => {
            if is_set_op(op) {
                return Err(set_op_scalar_error(op));
            }
            Ok(map_samples(vector_side, |v| {
                scalar_apply(op, return_bool, s, v, false)
            }))
        }
        (QueryResult::Vector(l), QueryResult::Vector(r)) => Ok(QueryResult::Vector(
            combine_vectors(op, return_bool, matching, l, r)?,
        )),
        (QueryResult::Matrix(l), QueryResult::Matrix(r)) => Ok(QueryResult::Matrix(
            combine_matrices(op, return_bool, matching, l, r)?,
        )),
        // Both operands evaluate under the same QuerySpec, so a
        // vector/matrix mix (or a streams/string operand) is structurally
        // impossible — defensive named error, never a panic.
        _ => Err(ReadError::PipelineInvalid {
            reason: "binary operation over incompatible result types".to_string(),
        }),
    }
}

fn set_op_scalar_error(op: BinOp) -> ReadError {
    ReadError::PipelineInvalid {
        reason: format!(
            "unexpected literal for a leg of logical/set binary operation ({op}): set \
             operations are defined between vectors only"
        ),
    }
}

/// Maps every sample of a vector/matrix result through `f` (`None`
/// drops the sample — the comparison-filter path), dropping series left
/// empty.
fn map_samples(result: QueryResult, f: impl Fn(f64) -> Option<f64>) -> QueryResult {
    match result {
        QueryResult::Vector(items) => QueryResult::Vector(
            items
                .into_iter()
                .filter_map(|s| {
                    f(s.value).map(|value| VectorSample {
                        labels: s.labels,
                        value,
                    })
                })
                .collect(),
        ),
        QueryResult::Matrix(items) => QueryResult::Matrix(
            items
                .into_iter()
                .filter_map(|s| {
                    let points: Vec<(i64, f64)> = s
                        .points
                        .into_iter()
                        .filter_map(|(ts, v)| f(v).map(|nv| (ts, nv)))
                        .collect();
                    (!points.is_empty()).then_some(MatrixSeries {
                        labels: s.labels,
                        points,
                    })
                })
                .collect(),
        ),
        other => other,
    }
}

/// A reduced match signature — the `on`/`ignoring` projection of a
/// series' (already key-sorted) labels.
type MatchSig = Vec<(String, String)>;

/// Per-matrix-series timestamp index for the per-step join: each series'
/// borrowed labels paired with its `timestamp → value` map.
type StepIndex<'a> = Vec<(&'a [(String, String)], BTreeMap<i64, f64>)>;

/// One instant-vector element for the shared join core — labels borrowed
/// from the caller's operand (a [`VectorSample`] or a per-step projection
/// of a [`MatrixSeries`]) plus the sample value.
struct JoinItem<'a> {
    labels: &'a [(String, String)],
    value: f64,
}

/// Projects a series' labels onto its match signature: `on(l)` keeps only
/// the listed keys, `ignoring(l)` drops them, `None` keeps the full set
/// (byte-identical to the pre-#91 full-`LabelSet` key). Input is
/// key-sorted (aggregation sorts labels), so the output stays sorted.
fn match_signature(labels: &[(String, String)], matching: Option<&VectorMatching>) -> MatchSig {
    match matching {
        None => labels.to_vec(),
        Some(vm) if vm.on => labels
            .iter()
            .filter(|(k, _)| vm.labels.iter().any(|l| l == k))
            .cloned()
            .collect(),
        Some(vm) => labels
            .iter()
            .filter(|(k, _)| !vm.labels.iter().any(|l| l == k))
            .cloned()
            .collect(),
    }
}

/// Sets `key`=`value` in a key-sorted label vector, replacing an existing
/// entry or inserting in sorted position (keeps the vector sorted so
/// downstream identity/equality stays canonical).
fn set_label_sorted(labels: &mut Vec<(String, String)>, key: &str, value: &str) {
    match labels.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
        Ok(i) => labels[i].1 = value.to_string(),
        Err(i) => labels.insert(i, (key.to_string(), value.to_string())),
    }
}

/// Removes `key` from a key-sorted label vector (no-op if absent).
fn remove_label_sorted(labels: &mut Vec<(String, String)>, key: &str) {
    if let Ok(i) = labels.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
        labels.remove(i);
    }
}

fn duplicate_one_side_error(swapped: bool) -> ReadError {
    // Oracle-pinned (grafana/loki:3.4.2): the "one" side is the source
    // rhs normally, the source lhs under `group_right`.
    let side = if swapped { "left" } else { "right" };
    ReadError::PipelineInvalid {
        reason: format!(
            "found duplicate series on the {side} hand-side;many-to-many matching not allowed: \
             matching labels must be unique on one side"
        ),
    }
}

fn multiple_matches_error() -> ReadError {
    // Oracle-pinned (grafana/loki:3.4.2), byte-exact.
    ReadError::PipelineInvalid {
        reason: "multiple matches for labels: many-to-one matching must be explicit \
                 (group_left/group_right)"
            .to_string(),
    }
}

fn grouping_unique_error() -> ReadError {
    // Prometheus/Loki wording for a duplicate grouped output identity;
    // unreachable with distinct many-side series, kept for completeness.
    ReadError::PipelineInvalid {
        reason: "multiple matches for labels: grouping labels must ensure unique matches"
            .to_string(),
    }
}

/// The shared instant-join core (issue #91). BOTH the vector path
/// ([`combine_vectors`], one virtual step) and the matrix path
/// ([`combine_matrices`], looped over shared timestamps) call this, so the
/// two can never diverge. Fresh per-call state ⇒ duplicate detection is
/// per-step-scoped for matrices.
///
/// Semantics verified against `pulsus_promql::eval::binop` and pinned
/// against `grafana/loki:3.4.2`:
/// - one-to-one output labels = the reduced signature; the many side
///   passes through whole under `group_left`/`group_right`, include labels
///   copied from the one side (empty value ⇒ label absent).
/// - the one-side signature map is built UNCONDITIONALLY first, so a
///   duplicate one-side signature errors for every cardinality.
/// - the empty-operand short-circuit is scoped to arithmetic/comparison
///   ONLY (adjudicated); set ops get their own empty handling in
///   [`set_op_join`].
fn instant_join(
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: &[JoinItem<'_>],
    rhs: &[JoinItem<'_>],
) -> Result<Vec<VectorSample>, ReadError> {
    if is_set_op(op) {
        return Ok(set_op_join(op, matching, lhs, rhs));
    }

    // Arithmetic/comparison empty-operand short-circuit — BEFORE the
    // one-side map is built, so an unpairable duplicate never surfaces a
    // spurious error (mirrors binop.rs). Scoped to arithmetic/comparison
    // ONLY; set ops handled above.
    if lhs.is_empty() || rhs.is_empty() {
        return Ok(Vec::new());
    }

    // Operand roles: `group_right` swaps sides so the loop always sees
    // `many` = the many side and `one` = the one side; the value
    // computation swaps back below.
    let (many, one, include, swapped) = match matching.and_then(|m| m.group.as_ref()) {
        None => (lhs, rhs, None, false),
        Some(MatchGroup::Left(inc)) => (lhs, rhs, Some(inc.as_slice()), false),
        Some(MatchGroup::Right(inc)) => (rhs, lhs, Some(inc.as_slice()), true),
    };
    let one_to_one = include.is_none();

    // The one side, hashed by match signature — a duplicate here is
    // many-to-many, an error for every cardinality.
    let mut one_by_key: HashMap<MatchSig, &JoinItem<'_>> = HashMap::with_capacity(one.len());
    for r in one {
        let key = match_signature(r.labels, matching);
        if one_by_key.insert(key, r).is_some() {
            return Err(duplicate_one_side_error(swapped));
        }
    }

    let mut one_to_one_matched: HashSet<MatchSig> = HashSet::new();
    let mut many_matched: HashMap<MatchSig, HashSet<MatchSig>> = HashMap::new();
    let mut out: Vec<VectorSample> = Vec::new();
    for l in many {
        let key = match_signature(l.labels, matching);
        let Some(r) = one_by_key.get(&key) else {
            continue;
        };
        // Restore source operand order for the value (upstream swap-back).
        let (vl, vr) = if swapped {
            (r.value, l.value)
        } else {
            (l.value, r.value)
        };
        let (value, keep) = if op.is_comparison() {
            let hit = compare(op, vl, vr);
            if return_bool {
                (if hit { 1.0 } else { 0.0 }, true)
            } else {
                (vl, hit)
            }
        } else {
            (arith(op, vl, vr), true)
        };

        let labels: MatchSig = if one_to_one {
            key.clone()
        } else {
            // Many side passes through whole; include labels copied from
            // the one side (empty value ⇒ absent, per binop.rs).
            let mut labels = l.labels.to_vec();
            if let Some(inc) = include {
                for ln in inc {
                    match r.labels.iter().find(|(k, _)| k == ln) {
                        Some((_, v)) if !v.is_empty() => set_label_sorted(&mut labels, ln, v),
                        _ => remove_label_sorted(&mut labels, ln),
                    }
                }
            }
            labels
        };

        // Duplicate detection — BEFORE the keep filter (a filtered-out
        // comparison still consumes its signature, upstream-exact).
        if one_to_one {
            if !one_to_one_matched.insert(key.clone()) {
                return Err(multiple_matches_error());
            }
        } else if !many_matched
            .entry(key.clone())
            .or_default()
            .insert(labels.clone())
        {
            return Err(grouping_unique_error());
        }

        if keep {
            out.push(VectorSample { labels, value });
        }
    }
    Ok(out)
}

/// The `and`/`or`/`unless` set operators keyed on the match signature
/// (issue #70 semantics, extended by #91 to reduced signatures under an
/// `on`/`ignoring` clause; a `group_left`/`group_right` on a set op is a
/// no-op, per the grafana/loki:3.4.2 probe). No empty-operand
/// short-circuit — each operator keeps its own empty handling
/// (`lhs and ∅`→∅; `lhs or ∅`→lhs, `∅ or rhs`→rhs; `lhs unless ∅`→lhs,
/// `∅ unless rhs`→∅), which per-step covers the matrix path.
fn set_op_join(
    op: BinOp,
    matching: Option<&VectorMatching>,
    lhs: &[JoinItem<'_>],
    rhs: &[JoinItem<'_>],
) -> Vec<VectorSample> {
    let own = |it: &JoinItem<'_>| VectorSample {
        labels: it.labels.to_vec(),
        value: it.value,
    };
    match op {
        BinOp::And => {
            let rhs_sigs: HashSet<MatchSig> = rhs
                .iter()
                .map(|s| match_signature(s.labels, matching))
                .collect();
            lhs.iter()
                .filter(|l| rhs_sigs.contains(&match_signature(l.labels, matching)))
                .map(own)
                .collect()
        }
        BinOp::Unless => {
            let rhs_sigs: HashSet<MatchSig> = rhs
                .iter()
                .map(|s| match_signature(s.labels, matching))
                .collect();
            lhs.iter()
                .filter(|l| !rhs_sigs.contains(&match_signature(l.labels, matching)))
                .map(own)
                .collect()
        }
        BinOp::Or => {
            let lhs_sigs: HashSet<MatchSig> = lhs
                .iter()
                .map(|s| match_signature(s.labels, matching))
                .collect();
            let mut out: Vec<VectorSample> = lhs.iter().map(own).collect();
            out.extend(
                rhs.iter()
                    .filter(|r| !lhs_sigs.contains(&match_signature(r.labels, matching)))
                    .map(own),
            );
            out
        }
        _ => unreachable!("is_set_op gates the arm"),
    }
}

/// Vector⊗vector: the [`instant_join`] core over one virtual step.
fn combine_vectors(
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: Vec<VectorSample>,
    rhs: Vec<VectorSample>,
) -> Result<Vec<VectorSample>, ReadError> {
    let lhs_items: Vec<JoinItem<'_>> = lhs
        .iter()
        .map(|s| JoinItem {
            labels: &s.labels,
            value: s.value,
        })
        .collect();
    let rhs_items: Vec<JoinItem<'_>> = rhs
        .iter()
        .map(|s| JoinItem {
            labels: &s.labels,
            value: s.value,
        })
        .collect();
    instant_join(op, return_bool, matching, &lhs_items, &rhs_items)
}

/// Matrix⊗matrix: an INDEPENDENT per-step instant join (issue #91 delta
/// 1). Prometheus/Loki re-evaluate the instant join at every timestamp;
/// two same-signature series whose points never share a step therefore
/// never collide, while a same-timestamp ambiguity errors. The per-step
/// core is [`instant_join`] — the exact function the vector path uses.
fn combine_matrices(
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: Vec<MatrixSeries>,
    rhs: Vec<MatrixSeries>,
) -> Result<Vec<MatrixSeries>, ReadError> {
    // Index each side's points by timestamp once (labels stay borrowable
    // from the owned operands for the whole loop).
    let lhs_maps: StepIndex<'_> = lhs
        .iter()
        .map(|s| (s.labels.as_slice(), s.points.iter().copied().collect()))
        .collect();
    let rhs_maps: StepIndex<'_> = rhs
        .iter()
        .map(|s| (s.labels.as_slice(), s.points.iter().copied().collect()))
        .collect();

    // The union of every timestamp on either side (ascending) — set ops
    // need lhs-only / rhs-only steps too (`or`/`unless`).
    let mut timestamps: BTreeSet<i64> = BTreeSet::new();
    for (_, m) in lhs_maps.iter().chain(rhs_maps.iter()) {
        timestamps.extend(m.keys().copied());
    }

    // Output series keyed by output labels, first-seen order preserved.
    let mut order: Vec<MatchSig> = Vec::new();
    let mut out: HashMap<MatchSig, Vec<(i64, f64)>> = HashMap::new();
    // Reused per-step scratch (allocation discipline).
    let mut lhs_items: Vec<JoinItem<'_>> = Vec::new();
    let mut rhs_items: Vec<JoinItem<'_>> = Vec::new();
    for &t in &timestamps {
        lhs_items.clear();
        rhs_items.clear();
        for (labels, m) in &lhs_maps {
            if let Some(v) = m.get(&t) {
                lhs_items.push(JoinItem { labels, value: *v });
            }
        }
        for (labels, m) in &rhs_maps {
            if let Some(v) = m.get(&t) {
                rhs_items.push(JoinItem { labels, value: *v });
            }
        }
        for sample in instant_join(op, return_bool, matching, &lhs_items, &rhs_items)? {
            match out.get_mut(&sample.labels) {
                Some(points) => points.push((t, sample.value)),
                None => {
                    order.push(sample.labels.clone());
                    out.insert(sample.labels, vec![(t, sample.value)]);
                }
            }
        }
    }

    Ok(order
        .into_iter()
        .map(|labels| {
            let points = out.remove(&labels).expect("every ordered key was inserted");
            MatrixSeries { labels, points }
        })
        .collect())
}

/// One fan-out group's accumulator — deliberately WITHOUT `labels_json`:
/// the map key is the single owned copy of the rendered label set, moved
/// into [`StreamResult`] when the map drains (review round 3: no
/// per-new-group key clone, which under high-cardinality fan-out is
/// effectively per-row).
struct FanOutGroup {
    fingerprint: u64,
    service: String,
    entries: Vec<(i64, String)>,
}

/// Inserts one surviving fan-out entry (its `sorted_scratch` label set already
/// sorted) into the label-set-keyed group map — shared by the label-mutating
/// pipeline path and the structured-metadata merge path (issue #97), which both
/// group by the final rendered label set. The rendered `labels_json` is the map
/// key (one owned copy, moved into [`StreamResult`] at drain — no per-new-group
/// key clone); the group's `fingerprint` is a deterministic content hash of it;
/// `service` is the merged set's `service_name` or `fallback_service`.
fn push_fanout_entry(
    label_groups: &mut HashMap<String, FanOutGroup>,
    sorted_scratch: &[(Cow<'_, str>, Cow<'_, str>)],
    timestamp_ns: i64,
    line: String,
    fallback_service: &str,
) {
    let labels_json = render_labels_json_sorted(sorted_scratch);
    let entry = (timestamp_ns, line);
    match label_groups.entry(labels_json) {
        std::collections::hash_map::Entry::Occupied(e) => {
            e.into_mut().entries.push(entry);
        }
        std::collections::hash_map::Entry::Vacant(e) => {
            let service = sorted_scratch
                .iter()
                .find(|(k, _)| k == "service_name")
                .map(|(_, v)| v.to_string())
                .unwrap_or_else(|| fallback_service.to_string());
            let fingerprint = fnv1a64(e.key().as_bytes());
            e.insert(FanOutGroup {
                fingerprint,
                service,
                entries: vec![entry],
            });
        }
    }
}

/// A reusable label scratch whose `Cow` entries borrow from the row's merged
/// base labels (lifetime `'a`) or own rewritten values — the buffer
/// `run_into` fills for structured-metadata-bearing rows (issue #97).
type LabelScratch<'a> = Vec<(Cow<'a, str>, Cow<'a, str>)>;

/// Runs one structured-metadata-bearing row through the pipeline over `merged`
/// (base + SM labels) and fans its surviving line into `label_groups`, reusing
/// `scratch`'s heap allocation across rows. `scratch` is taken BY VALUE and
/// returned (cleared) rather than borrowed `&mut`, because `run_into`'s output
/// labels borrow `merged` — whose contents are rewritten every row — so the
/// Cow scratch needs a FRESH lifetime per call; a hoisted `&mut Vec<Cow<'a>>`
/// binding cannot provide that (the merge buffer's `.clear()` would conflict
/// with an outstanding borrow). Passing by value gives each call its own
/// lifetime while [`recycle_label_scratch`] hands the same allocation back for
/// the next row (issue #97 review round 1, finding 2 / AC-12).
fn eval_structured_metadata_row<'a>(
    compiled: &'a super::pipeline::CompiledPipeline,
    body: &'a str,
    merged: &'a [(String, String)],
    label_groups: &mut HashMap<String, FanOutGroup>,
    timestamp_ns: i64,
    service: &str,
    mut scratch: LabelScratch<'a>,
) -> (bool, LabelScratch<'a>) {
    let survived = if let Some(line) = compiled.run_into(body, merged, &mut scratch) {
        let line = line.into_owned();
        scratch.sort_unstable();
        push_fanout_entry(label_groups, &scratch, timestamp_ns, line, service);
        true
    } else {
        false
    };
    // Drop every borrow of `merged` before the buffer is recycled for reuse.
    scratch.clear();
    (survived, scratch)
}

/// Re-tags a cleared borrowed-label scratch's (now empty) heap allocation as
/// `'static` so it can be reused by the next SM row, whose `merged` base labels
/// live for only one iteration. Safe: the vector is emptied first, so no borrow
/// survives the re-tag; the allocation is preserved by the in-place
/// `into_iter().map().collect()` (identical element layout). If that reuse ever
/// regressed it would only reallocate — never misbehave — and AC-12 gates the
/// reuse from outside the crate.
fn recycle_label_scratch(mut scratch: LabelScratch<'_>) -> LabelScratch<'static> {
    scratch.clear();
    scratch
        .into_iter()
        .map(|(k, v)| (Cow::Owned(k.into_owned()), Cow::Owned(v.into_owned())))
        .collect()
}

/// Feeds one page of sampled rows through the query pipeline into a
/// detected-fields accumulator (issue #170): `matched` counts
/// **post-pipeline survivors only** — a pipeline-dropped row never counts
/// toward `line_limit` (the plan-v2 contract; unit-tested below). Merge/
/// scratch buffers are reused across the page's rows (the
/// `StreamAccumulator::feed` allocation discipline); rows whose
/// fingerprint failed to hydrate are skipped.
fn feed_detected_rows(
    rows: &[SampleRow],
    base_labels: &HashMap<u64, Vec<(String, String)>>,
    compiled: &super::pipeline::CompiledPipeline,
    acc: &mut FieldAccumulator,
    matched: &mut u32,
    line_limit: u32,
) {
    let mut merge_buf: Vec<(String, String)> = Vec::new();
    let mut sm_buf: Vec<(String, String)> = Vec::new();
    // The SM pairs kept for `observe_structured_metadata` — parsed
    // separately because `merge_labels_with_structured_metadata` drains
    // its own parse scratch into the merged set. Field detection is
    // bounded by the page (and survivors by `line_limit`), so the second
    // parse per SM-bearing row is fine here.
    let mut sm_obs: Vec<(String, String)> = Vec::new();
    // Reused across rows via the by-value recycle (see
    // `eval_structured_metadata_row` for why a hoisted `&mut` cannot
    // provide the per-row lifetime).
    let mut scratch: LabelScratch<'static> = Vec::new();
    for row in rows {
        if *matched >= line_limit {
            break;
        }
        let Some(base) = base_labels.get(&row.fingerprint) else {
            continue;
        };
        let has_sm = !row.structured_metadata.is_empty();
        if has_sm {
            sm_obs.clear();
            parse_flat_labels_into(&row.structured_metadata, &mut sm_obs);
            merge_labels_with_structured_metadata(
                base,
                &row.structured_metadata,
                &mut merge_buf,
                &mut sm_buf,
            );
        }
        let run_base: &[(String, String)] = if has_sm { &merge_buf } else { base };
        let sm_pairs: &[(String, String)] = if has_sm { &sm_obs } else { &[] };
        let (survived, used) =
            observe_detected_row(compiled, &row.body, run_base, sm_pairs, acc, scratch);
        scratch = recycle_label_scratch(used);
        if survived {
            *matched += 1;
        }
    }
}

/// Runs one sampled row through the query pipeline and, when it
/// survives, feeds the [`FieldAccumulator`] (issue #170): structured-
/// metadata pairs (no parser attribution), pipeline-extracted keys not
/// present in the merged base (no parser attribution;
/// `__error__`/`__error_details__` excluded inside `observe_parsed`),
/// then json-first/logfmt-fallback auto-detection on the POST-pipeline
/// line. `scratch` is taken by value and returned for recycling — same
/// per-row-lifetime rationale as [`eval_structured_metadata_row`].
fn observe_detected_row<'a>(
    compiled: &'a super::pipeline::CompiledPipeline,
    body: &'a str,
    run_base: &'a [(String, String)],
    sm_pairs: &[(String, String)],
    acc: &mut FieldAccumulator,
    mut scratch: LabelScratch<'a>,
) -> (bool, LabelScratch<'a>) {
    let survived = if let Some(line) = compiled.run_into(body, run_base, &mut scratch) {
        acc.observe_structured_metadata(sm_pairs);
        let added: Vec<(String, String)> = scratch
            .iter()
            .filter(|(k, _)| !run_base.iter().any(|(bk, _)| bk.as_str() == k.as_ref()))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        acc.observe_parsed(&added, None);
        if let Some((parser, pairs)) = detected::auto_parse(line.as_ref()) {
            acc.observe_parsed(&pairs, Some(parser));
        }
        true
    } else {
        false
    };
    // Drop every borrow of `run_base` before the buffer is recycled.
    scratch.clear();
    (survived, scratch)
}

/// Fan-out for structured-metadata-bearing rows on the line-filter-only fast
/// path (issue #97). All filtering is already applied in SQL and no pipeline
/// runs, so each SM row's response label set is its stream's base labels merged
/// with its parsed structured metadata; each distinct merged set is its own
/// stream (Loki's per-entry structured-metadata fan-out — see the #97 oracle
/// probe). Grouping/fingerprinting matches the [`StreamAccumulator`] SM branch
/// so fast- and transform-path results are byte-consistent. **No-SM rows never
/// reach here** — they stay on the unchanged by-fingerprint fast path, so its
/// zero-per-row profile and byte-identity hold (AC-8).
fn fan_out_sm_fast_path(
    sm_rows: &[SampleRow],
    meta: &HashMap<u64, StreamMetaRow>,
) -> Vec<StreamResult> {
    let mut base_cache: HashMap<u64, Vec<(String, String)>> = HashMap::new();
    let mut groups: HashMap<String, FanOutGroup> = HashMap::new();
    // Reused across rows (clear + refill, capacity-amortized) — never a fresh
    // per-row allocation of the label vector itself. `sm_buf` is the SM-pair
    // parse scratch (see `merge_labels_with_structured_metadata`).
    let mut merge_buf: Vec<(String, String)> = Vec::new();
    let mut sm_buf: Vec<(String, String)> = Vec::new();
    for row in sm_rows {
        let Some(m) = meta.get(&row.fingerprint) else {
            continue;
        };
        let base = base_cache
            .entry(row.fingerprint)
            .or_insert_with(|| parse_flat_labels(&m.labels));
        // Merge base + SM (colliding SM keys renamed `_extracted`, per the
        // oracle — no duplicate keys under any collision pattern), then sort for
        // canonical rendering.
        merge_labels_with_structured_metadata(
            base,
            &row.structured_metadata,
            &mut merge_buf,
            &mut sm_buf,
        );
        merge_buf.sort_unstable();
        let sorted: Vec<(Cow<'_, str>, Cow<'_, str>)> = merge_buf
            .iter()
            .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str())))
            .collect();
        push_fanout_entry(
            &mut groups,
            &sorted,
            row.timestamp_ns,
            row.body.clone(),
            &m.service,
        );
    }
    groups
        .into_iter()
        .map(|(labels_json, g)| StreamResult {
            fingerprint: g.fingerprint,
            service: g.service,
            labels_json,
            entries: g.entries,
        })
        .collect()
}

/// Renders a **sorted** label set to the canonical flat-label JSON shape
/// (`{"key":"value",...}`, sorted keys, no nesting — docs/architecture.md
/// §2.3), matching what the writer produces for base streams so the
/// server encoder can splice it verbatim either way. Hand-rolled
/// escaping (byte-compatible with `serde_json`'s string escaping —
/// unit-tested below) so rendering borrows the label pairs instead of
/// cloning them into a `serde_json::Map` (round-2 finding 1).
fn render_labels_json_sorted(sorted_labels: &[(Cow<'_, str>, Cow<'_, str>)]) -> String {
    let mut out = String::with_capacity(
        2 + sorted_labels
            .iter()
            .map(|(k, v)| k.len() + v.len() + 6)
            .sum::<usize>(),
    );
    out.push('{');
    for (i, (k, v)) in sorted_labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_json_string(&mut out, k);
        out.push(':');
        push_json_string(&mut out, v);
    }
    out.push('}');
    out
}

/// Appends `s` as a quoted JSON string, escaping exactly the mandatory
/// set the same way `serde_json` does (`"`/`\` escaped, the five short
/// control escapes, `\u00xx` lowercase for the rest of C0, everything
/// else verbatim).
fn push_json_string(out: &mut String, s: &str) {
    use std::fmt::Write as _;
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                // Infallible: `write!` to a String cannot fail.
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// FNV-1a 64 — the fan-out path's deterministic label-set fingerprint
/// (`fingerprint = hash(final labels)`, plan v1). Not a stored/write-path
/// fingerprint: purely a stable response identity for derived streams.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The read-path settings every LogQL query now carries (issue #35): the
/// byte scan budget (`max_bytes_to_read` + `read_overflow_mode = 'throw'`,
/// unchanged from before this issue) plus `max_query_size` — ClickHouse's
/// own SQL-text parse-buffer setting, raised to
/// [`crate::querytext::MAX_QUERY_TEXT_BYTES`] so the documented worst-case
/// stage2/stage3 `IN` lists (at `DEFAULT_MAX_STREAMS`) never trip the
/// 262,144-byte server default. Single source of truth — [`LogQlEngine::
/// budget_settings`]/[`LogQlEngine::paging_settings`] both delegate to this
/// rather than re-deriving the trio, and the `xtask` bench sources
/// [`crate::querytext::MAX_QUERY_TEXT_BYTES`] directly (not this function)
/// to keep its own settings key-for-key identical to what produced the
/// frozen evidence JSONs (issue #35 plan v2, "Frozen-bench resolution").
pub fn read_query_settings(scan_budget_bytes: u64) -> QuerySettings {
    QuerySettings::new()
        .set("max_bytes_to_read", scan_budget_bytes)
        .set("read_overflow_mode", "throw")
        .set("max_query_size", crate::querytext::MAX_QUERY_TEXT_BYTES)
}

/// Pure paging-termination decision (issue #133, the #96
/// `probe_fanout_bound` extraction shape): `true` once the cumulative
/// per-page `read_bytes` has consumed the whole
/// `reader.logql_scan_budget_bytes` budget — the fetch-until-limit loop
/// must return its survivors as partial rather than issue another page
/// (a zero remaining cap would be ClickHouse's *unlimited* sentinel).
/// Extracted from [`LogQlEngine::run_streams_paged`]'s top-of-loop guard
/// so the termination is provable at the max config-accepted budget
/// (`pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING`) with synthetic
/// byte counts. Behavior-identical to the inline `spent >= budget`.
#[inline]
fn scan_budget_spent(spent: u64, budget: u64) -> bool {
    spent >= budget
}

/// Maps a ClickHouse error to [`ReadError`], translating the byte-budget
/// overflow code to a structured [`TooBroadReason::ScanBudgetBytes`] and
/// leaving every other server code (including 158 `TOO_MANY_ROWS`, which
/// the LogQL path never triggers because it never sets `max_rows_to_read`
/// — the traces search path sets that budget deliberately and maps 158 in
/// its **own** mapper, `traces::exec::map_trace_read_error`, issue #57) as
/// a generic [`ReadError::Clickhouse`] passthrough — never reinterpreted
/// as a timeout or vice versa.
fn map_read_error(e: ChError, budget_bytes: u64) -> ReadError {
    if let ChError::Server { code, .. } = &e
        && *code == CODE_TOO_MANY_BYTES
    {
        return ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
            budget_bytes,
            estimate: None,
        });
    }
    ReadError::Clickhouse(e)
}

/// The Rust-side structural stream cap (task-manager resolution #1 on
/// issue #11): a `count` past `cap` is [`TooBroadReason::StreamCap`], a
/// distinct "too broad" family from the ClickHouse byte budget — never a
/// ClickHouse row limit, since `max_rows_to_read` is never set on LogQL
/// read paths (the traces scan budget sets it deliberately on its
/// generator queries — issue #57); on the LogQL path code 158 cannot
/// masquerade as `StreamCap`.
fn check_stream_cap(count: usize, cap: usize) -> Result<(), ReadError> {
    if count > cap {
        Err(ReadError::QueryTooBroad(TooBroadReason::StreamCap {
            count,
            cap,
        }))
    } else {
        Ok(())
    }
}

fn apply_rate(n: f64, rate_window_ns: Option<u64>) -> f64 {
    match rate_window_ns {
        Some(window_ns) if window_ns > 0 => n / (window_ns as f64 / 1_000_000_000.0),
        _ => n,
    }
}

fn distinct_escaped_services(meta: &HashMap<u64, StreamMetaRow>) -> Vec<String> {
    let mut services: Vec<&str> = meta.values().map(|m| m.service.as_str()).collect();
    services.sort_unstable();
    services.dedup();
    services.into_iter().map(super::escape::ch_string).collect()
}

/// A stream's full exposed label set: its canonical-JSON labels plus the
/// promoted `service` physical column re-injected as `service_name`
/// (docs/architecture.md §2.3's canonical label model) so grouping by
/// `service_name` — the §3.2 canonical vector-agg example — works without
/// special-casing it against the JSON blob.
fn series_labels(meta: &StreamMetaRow) -> Vec<(String, String)> {
    let mut labels = parse_flat_labels(&meta.labels);
    labels.retain(|(k, _)| k != "service_name");
    labels.push(("service_name".to_string(), meta.service.clone()));
    labels.sort();
    labels
}

/// The label-name set a volume query keys on (issue #169, oracle
/// `PrepareLabelsAndMatchers`): the `targetLabels` set when supplied,
/// otherwise the selector's OWN matcher names — every op, including
/// `!=`/`!~` (the oracle adds every `m.Name`, so `{env!="dev"}` keys
/// results by each stream's `env` value).
fn volume_labels_to_match(selector: &StreamSelector, target_labels: &[String]) -> BTreeSet<String> {
    if target_labels.is_empty() {
        selector.matchers.iter().map(|m| m.name.clone()).collect()
    } else {
        target_labels.iter().cloned().collect()
    }
}

/// `targetLabels` matcher injection (issue #169, oracle
/// `prepareLabelsAndMatchersWithTargets`): each target with no matcher of
/// its name gets `name =~ ".+"` appended to the selector; targets already
/// matched (any op) are left alone. Pure — the caller plans the returned
/// expression, so the injected name crosses the ordinary `escape`
/// boundary like any parsed matcher.
fn inject_target_matchers(le: &LogExpr, target_labels: &[String]) -> LogExpr {
    let mut out = le.clone();
    for target in target_labels {
        if !out.selector.matchers.iter().any(|m| m.name == *target) {
            out.selector.matchers.push(Matcher {
                name: target.clone(),
                op: MatchOp::Re,
                value: ".+".to_string(),
            });
        }
    }
    out
}

/// Pure volume accumulation over the rollup rows (issue #169, oracle
/// `seriesvolume.Add`/`MapToVolumeResponse` + `instance.go getVolume`):
///
/// - **Series mode:** key = the stream's label pairs whose name is in
///   `labels_to_match`; bytes accumulate saturating. A stream matching
///   none of the names groups under the empty `{}` key.
/// - **Labels mode:** each label NAME of the stream — restricted to
///   `labels_to_match` when `restrict_label_names` (i.e. `targetLabels`
///   was supplied), otherwise ALL of the stream's names — accumulates
///   under the single-pair key `(name, "")`.
///
/// A stream with no rollup row in-window contributes nothing (the rows
/// slice simply lacks it); a returned `bytes = 0` row DOES contribute a
/// zero entry. Output sorted `(bytes desc, labels asc)` — the oracle's
/// value-desc/name-asc presentation — truncated to `limit`.
fn accumulate_volume(
    rows: &[VolumeRow],
    meta: &HashMap<u64, StreamMetaRow>,
    aggregate_by: VolumeAggregateBy,
    labels_to_match: &BTreeSet<String>,
    restrict_label_names: bool,
    limit: u32,
) -> Vec<VolumeEntry> {
    let mut acc: BTreeMap<Vec<(String, String)>, u64> = BTreeMap::new();
    for row in rows {
        // A rollup row whose fingerprint failed to hydrate (non-atomic
        // stream/sample writes) has no label set to key on — skip it, the
        // same tolerance stage 2's ReplacingMergeTree dedup documents.
        let Some(m) = meta.get(&row.fingerprint) else {
            continue;
        };
        let stream_labels = series_labels(m);
        match aggregate_by {
            VolumeAggregateBy::Series => {
                let key: Vec<(String, String)> = stream_labels
                    .into_iter()
                    .filter(|(name, _)| labels_to_match.contains(name))
                    .collect();
                let entry = acc.entry(key).or_insert(0);
                *entry = entry.saturating_add(row.bytes);
            }
            VolumeAggregateBy::Labels => {
                for (name, _) in stream_labels {
                    if restrict_label_names && !labels_to_match.contains(&name) {
                        continue;
                    }
                    let entry = acc.entry(vec![(name, String::new())]).or_insert(0);
                    *entry = entry.saturating_add(row.bytes);
                }
            }
        }
    }
    let mut out: Vec<VolumeEntry> = acc
        .into_iter()
        .map(|(labels, bytes)| VolumeEntry { labels, bytes })
        .collect();
    out.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.labels.cmp(&b.labels)));
    out.truncate(limit as usize);
    out
}

/// Parses PulsusDB's canonical flat label JSON (`{"key":"value", ...}`,
/// sorted keys, no nesting — docs/architecture.md §2.3) without a JSON
/// crate dependency (not part of this module's declared dependency set).
/// Malformed input — which should never occur, this only ever reads back
/// what the writer produced — yields whatever pairs were parsed so far
/// rather than panicking.
fn parse_flat_labels(json: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    parse_flat_labels_into(json, &mut out);
    out
}

/// [`parse_flat_labels`] that APPENDS into a caller-owned buffer instead of
/// allocating a fresh `Vec` (issue #97): the structured-metadata merge reuses
/// one buffer across rows (clear + refill), so the parse must not allocate its
/// own return vector per row.
fn parse_flat_labels_into(json: &str, out: &mut Vec<(String, String)>) {
    let mut chars = json.chars().peekable();
    while let Some(&c) = chars.peek() {
        chars.next();
        if c == '{' {
            break;
        }
    }
    loop {
        skip_ws(&mut chars);
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
        skip_ws(&mut chars);
        if chars.peek() == Some(&':') {
            chars.next();
        }
        skip_ws(&mut chars);
        let Some(value) = parse_json_string(&mut chars) else {
            break;
        };
        out.push((key, value));
    }
}

/// Merges a stream's cached base (stream/parsed) labels with one row's
/// structured metadata into `merge_buf` (cleared first — its heap allocation
/// is reused across rows; `sm_buf` is a second reused scratch the SM pairs are
/// parsed into). A structured-metadata key that collides with a base label key
/// is renamed to `<key>_extracted`; the resolved key is then UPSERTED into the
/// merged set (last-write-wins) so BOTH the base label and the renamed SM value
/// survive as distinct entries in the ordinary case. This matches
/// grafana/loki:3.4.2's DEFAULT query response (probed for issue #97): the
/// stream/parsed label keeps the original key and value, while the colliding
/// structured-metadata value surfaces under the `_extracted` suffix (and is
/// filterable there — `| key_extracted="v"` matches, `| key="v"` matches the
/// stream label).
///
/// DOUBLE collision: when the renamed `<key>_extracted` ALSO already exists —
/// e.g. base carries both `env` AND `env_extracted`, or the SM object itself
/// supplies `env_extracted` alongside a colliding `env` — the upsert OVERWRITES
/// that existing slot rather than emitting a second `<key>_extracted` entry.
/// grafana/loki:3.4.2 renders exactly one `env_extracted`, last-write-wins
/// (probed for issue #97: base `env`+`env_extracted` + SM `env` → the SM value
/// wins the `env_extracted` slot; no `env_extracted_extracted`, no numeric
/// suffix, no drop). This is the same collision precedence the `| json`
/// parser's `add_extracted` already pins, and it preserves the
/// no-duplicate-label-entries invariant under ANY collision pattern. The rename
/// decision consults only the base region; the upsert consults the FULL evolving
/// merged set (base + already-merged SM keys). The result is left UNSORTED;
/// callers sort before rendering/grouping.
fn merge_labels_with_structured_metadata(
    base: &[(String, String)],
    structured_metadata: &str,
    merge_buf: &mut Vec<(String, String)>,
    sm_buf: &mut Vec<(String, String)>,
) {
    merge_buf.clear();
    merge_buf.extend(base.iter().cloned());
    let base_len = merge_buf.len();
    sm_buf.clear();
    parse_flat_labels_into(structured_metadata, sm_buf);
    // `base_len` is small (a stream's label count), so these scans are bounded
    // by the fixed label cardinality, not by row count. `drain` moves the owned
    // key/value Strings out of the reused scratch without cloning.
    for (mut key, value) in sm_buf.drain(..) {
        if merge_buf[..base_len].iter().any(|(bk, _)| *bk == key) {
            key.push_str("_extracted");
        }
        match merge_buf.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => merge_buf.push((key, value)),
        }
    }
}

fn skip_ws<I: Iterator<Item = char>>(chars: &mut std::iter::Peekable<I>) {
    while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
        chars.next();
    }
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

type LabelSet = Vec<(String, String)>;

struct RangeSeries {
    labels: LabelSet,
    points: BTreeMap<i64, f64>,
}

struct InstantSeries {
    labels: LabelSet,
    value: f64,
}

fn group_key(labels: &[(String, String)], grouping: Option<&Grouping>) -> LabelSet {
    let Some(g) = grouping else {
        return Vec::new();
    };
    let mut kv: Vec<(String, String)> = match g.kind {
        GroupingKind::By => {
            let map: HashMap<&str, &str> = labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            g.labels
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        map.get(name.as_str())
                            .map(|v| v.to_string())
                            .unwrap_or_default(),
                    )
                })
                .collect()
        }
        GroupingKind::Without => labels
            .iter()
            .filter(|(k, _)| !g.labels.contains(k))
            .cloned()
            .collect(),
    };
    kv.sort();
    kv
}

/// Population variance (the reference oracle's `stdvar` semantics —
/// live-probed: `stdvar` of `1,2,3,4` is `1.25`, i.e. `/n`, not the
/// sample `/(n-1)`).
fn population_variance(vals: &[f64]) -> f64 {
    let n = vals.len() as f64;
    let mean = vals.iter().sum::<f64>() / n;
    vals.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>() / n
}

fn reduce(op: VectorAggOp, vals: &[f64]) -> f64 {
    match op {
        VectorAggOp::Sum => vals.iter().sum(),
        VectorAggOp::Avg => vals.iter().sum::<f64>() / vals.len() as f64,
        VectorAggOp::Min => vals.iter().cloned().fold(f64::INFINITY, f64::min),
        VectorAggOp::Max => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        VectorAggOp::Count => vals.len() as f64,
        VectorAggOp::Stddev => population_variance(vals).sqrt(),
        VectorAggOp::Stdvar => population_variance(vals),
        // Documented invariant: topk/bottomk are per-step SELECTIONS, not
        // reductions — `group_range`/`group_instant` branch to
        // `select_k_*` before ever calling `reduce`.
        VectorAggOp::Topk | VectorAggOp::Bottomk => {
            unreachable!("topk/bottomk are selections, dispatched before reduce")
        }
    }
}

/// The `topk`/`bottomk` `k`: the parameter floored to a count; a missing
/// or non-positive parameter selects nothing (the planner already
/// rejects a missing `k` — defensive here).
fn k_of(param: Option<f64>) -> usize {
    match param {
        Some(p) if p >= 1.0 => p.floor() as usize,
        _ => 0,
    }
}

/// Deterministic candidate ordering for `topk`/`bottomk` (pinned by
/// golden, plan edge case 7): NaN candidates rank LAST for BOTH
/// directions (oracle-probed: `topk(2)` over `{NaN, 5, 1}` selects
/// `{5, 1}` and `bottomk(2)` selects `{1, 5}` — a NaN is never
/// preferred over a finite value); among non-NaN values, descending for
/// topk / ascending for bottomk; ties broken by the series' label set
/// ascending.
fn sort_candidates(candidates: &mut [(usize, f64)], labels_of: &[LabelSet], largest: bool) {
    candidates.sort_by(|(ai, av), (bi, bv)| {
        av.is_nan()
            .cmp(&bv.is_nan())
            .then_with(|| {
                if av.is_nan() {
                    // Both NaN: value order is meaningless; fall through
                    // to the label tie-break.
                    std::cmp::Ordering::Equal
                } else if largest {
                    bv.total_cmp(av)
                } else {
                    av.total_cmp(bv)
                }
            })
            .then_with(|| labels_of[*ai].cmp(&labels_of[*bi]))
    });
}

/// `topk`/`bottomk` over a range result: within each group, at each step,
/// keep the k highest/lowest samples — preserving each survivor's
/// ORIGINAL series labels (selection, not reduction).
fn select_k_range(
    series: Vec<RangeSeries>,
    op: VectorAggOp,
    grouping: Option<&Grouping>,
    param: Option<f64>,
) -> Vec<RangeSeries> {
    let k = k_of(param);
    if k == 0 {
        return Vec::new();
    }
    let largest = matches!(op, VectorAggOp::Topk);
    let labels_of: Vec<LabelSet> = series.iter().map(|s| s.labels.clone()).collect();
    let mut groups: HashMap<LabelSet, Vec<usize>> = HashMap::new();
    for (idx, s) in series.iter().enumerate() {
        groups
            .entry(group_key(&s.labels, grouping))
            .or_default()
            .push(idx);
    }
    let mut keep: Vec<BTreeMap<i64, f64>> = series.iter().map(|_| BTreeMap::new()).collect();
    for members in groups.values() {
        let steps: BTreeSet<i64> = members
            .iter()
            .flat_map(|&i| series[i].points.keys().copied())
            .collect();
        for step in steps {
            let mut candidates: Vec<(usize, f64)> = members
                .iter()
                .filter_map(|&i| series[i].points.get(&step).map(|v| (i, *v)))
                .collect();
            sort_candidates(&mut candidates, &labels_of, largest);
            for (idx, v) in candidates.into_iter().take(k) {
                keep[idx].insert(step, v);
            }
        }
    }
    series
        .into_iter()
        .zip(keep)
        .filter_map(|(s, points)| {
            (!points.is_empty()).then_some(RangeSeries {
                labels: s.labels,
                points,
            })
        })
        .collect()
}

/// `topk`/`bottomk` over an instant result: keep the k highest/lowest
/// samples per group, original labels preserved.
fn select_k_instant(
    series: Vec<InstantSeries>,
    op: VectorAggOp,
    grouping: Option<&Grouping>,
    param: Option<f64>,
) -> Vec<InstantSeries> {
    let k = k_of(param);
    if k == 0 {
        return Vec::new();
    }
    let largest = matches!(op, VectorAggOp::Topk);
    let labels_of: Vec<LabelSet> = series.iter().map(|s| s.labels.clone()).collect();
    let mut groups: HashMap<LabelSet, Vec<usize>> = HashMap::new();
    for (idx, s) in series.iter().enumerate() {
        groups
            .entry(group_key(&s.labels, grouping))
            .or_default()
            .push(idx);
    }
    let mut keep: Vec<bool> = vec![false; series.len()];
    for members in groups.values() {
        let mut candidates: Vec<(usize, f64)> =
            members.iter().map(|&i| (i, series[i].value)).collect();
        sort_candidates(&mut candidates, &labels_of, largest);
        for (idx, _) in candidates.into_iter().take(k) {
            keep[idx] = true;
        }
    }
    series
        .into_iter()
        .zip(keep)
        .filter_map(|(s, kept)| kept.then_some(s))
        .collect()
}

fn group_range(
    series: Vec<RangeSeries>,
    op: VectorAggOp,
    grouping: Option<&Grouping>,
    param: Option<f64>,
) -> Vec<RangeSeries> {
    if matches!(op, VectorAggOp::Topk | VectorAggOp::Bottomk) {
        return select_k_range(series, op, grouping, param);
    }
    let mut groups: HashMap<LabelSet, Vec<BTreeMap<i64, f64>>> = HashMap::new();
    for s in series {
        groups
            .entry(group_key(&s.labels, grouping))
            .or_default()
            .push(s.points);
    }
    groups
        .into_iter()
        .map(|(labels, members)| {
            let steps: BTreeSet<i64> = members.iter().flat_map(|m| m.keys().copied()).collect();
            let points = steps
                .into_iter()
                .filter_map(|step| {
                    let vals: Vec<f64> = members
                        .iter()
                        .filter_map(|m| m.get(&step).copied())
                        .collect();
                    if vals.is_empty() {
                        None
                    } else {
                        Some((step, reduce(op, &vals)))
                    }
                })
                .collect();
            RangeSeries { labels, points }
        })
        .collect()
}

fn group_instant(
    series: Vec<InstantSeries>,
    op: VectorAggOp,
    grouping: Option<&Grouping>,
    param: Option<f64>,
) -> Vec<InstantSeries> {
    if matches!(op, VectorAggOp::Topk | VectorAggOp::Bottomk) {
        return select_k_instant(series, op, grouping, param);
    }
    let mut groups: HashMap<LabelSet, Vec<f64>> = HashMap::new();
    for s in series {
        groups
            .entry(group_key(&s.labels, grouping))
            .or_default()
            .push(s.value);
    }
    groups
        .into_iter()
        .map(|(labels, vals)| InstantSeries {
            labels,
            value: reduce(op, &vals),
        })
        .collect()
}

/// Applies an outer-to-inner vector-aggregation chain to a metric result
/// (innermost applied first — the `.rev()` matching `MetricPlan.
/// vector_aggs`' outer-first order). `pub` like [`run_pipeline_rows`]:
/// the hermetic golden suite (`tests/logql_metric_agg_golden.rs`) pins
/// the reducer/selection semantics from outside the crate.
pub fn apply_vector_aggs(result: QueryResult, aggs: &[plan::VectorAggSpec]) -> QueryResult {
    match result {
        QueryResult::Matrix(items) => {
            let mut series: Vec<RangeSeries> = items
                .into_iter()
                .map(|s| RangeSeries {
                    labels: s.labels,
                    points: s.points.into_iter().collect(),
                })
                .collect();
            for (op, grouping, param) in aggs.iter().rev() {
                series = group_range(series, *op, grouping.as_ref(), *param);
            }
            QueryResult::Matrix(
                series
                    .into_iter()
                    .map(|s| MatrixSeries {
                        labels: s.labels,
                        points: s.points.into_iter().collect(),
                    })
                    .collect(),
            )
        }
        QueryResult::Vector(items) => {
            let mut series: Vec<InstantSeries> = items
                .into_iter()
                .map(|s| InstantSeries {
                    labels: s.labels,
                    value: s.value,
                })
                .collect();
            for (op, grouping, param) in aggs.iter().rev() {
                series = group_instant(series, *op, grouping.as_ref(), *param);
            }
            QueryResult::Vector(
                series
                    .into_iter()
                    .map(|s| VectorSample {
                        labels: s.labels,
                        value: s.value,
                    })
                    .collect(),
            )
        }
        // A vector aggregation over a scalar is rejected at plan time
        // (`build_metric_node`); passthrough is defensive only.
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use pulsus_clickhouse::ChError;

    use super::*;

    /// Issue #170 plan v2 test delta 3: the detected-fields matched-entry
    /// count is POST-pipeline — rows the pipeline drops never count toward
    /// `line_limit`, and their fields are never observed.
    #[test]
    fn detected_fields_matched_count_is_post_pipeline_dropped_rows_do_not_count() {
        let expr = pulsus_logql::parse(r#"{app="x"} | json | level="rare""#).expect("parse");
        let pulsus_logql::Expr::Log(le) = expr else {
            panic!("log expr");
        };
        let compiled = super::super::pipeline::CompiledPipeline::compile(&le.pipeline)
            .expect("compile pipeline");
        let mut base_labels: HashMap<u64, Vec<(String, String)>> = HashMap::new();
        base_labels.insert(1, vec![("app".to_string(), "x".to_string())]);
        let rows = vec![
            SampleRow {
                fingerprint: 1,
                timestamp_ns: 3,
                body: r#"{"level":"common","code":1}"#.to_string(),
                structured_metadata: String::new(),
            },
            SampleRow {
                fingerprint: 1,
                timestamp_ns: 2,
                body: "not json at all".to_string(),
                structured_metadata: String::new(),
            },
            SampleRow {
                fingerprint: 1,
                timestamp_ns: 1,
                body: r#"{"level":"rare","code":7}"#.to_string(),
                structured_metadata: String::new(),
            },
        ];
        let mut acc = super::super::detected::FieldAccumulator::new(1000);
        let mut matched = 0u32;
        feed_detected_rows(&rows, &base_labels, &compiled, &mut acc, &mut matched, 100);
        assert_eq!(
            matched, 1,
            "only the post-pipeline surviving row counts toward line_limit"
        );
        let fields = acc.finish();
        let labels: Vec<&str> = fields.iter().map(|f| f.label.as_str()).collect();
        assert_eq!(labels, vec!["code", "level"]);
        let code = fields.iter().find(|f| f.label == "code").expect("code");
        assert_eq!(code.field_type, "int");
        assert_eq!(
            code.cardinality, 1,
            "dropped rows' values are never observed"
        );
        assert_eq!(code.parsers, vec!["json"]);
    }

    /// Issue #170: the post-pipeline matched count stops feeding once
    /// `line_limit` survivors are collected (the fast path's cap).
    #[test]
    fn detected_fields_feed_stops_at_the_line_limit() {
        let expr = pulsus_logql::parse(r#"{app="x"}"#).expect("parse");
        let pulsus_logql::Expr::Log(le) = expr else {
            panic!("log expr");
        };
        let compiled = super::super::pipeline::CompiledPipeline::compile(&le.pipeline)
            .expect("compile pipeline");
        let mut base_labels: HashMap<u64, Vec<(String, String)>> = HashMap::new();
        base_labels.insert(1, vec![("app".to_string(), "x".to_string())]);
        let rows: Vec<SampleRow> = (0..5)
            .map(|i| SampleRow {
                fingerprint: 1,
                timestamp_ns: i,
                body: format!(r#"{{"seq":"{i}"}}"#),
                structured_metadata: String::new(),
            })
            .collect();
        let mut acc = super::super::detected::FieldAccumulator::new(1000);
        let mut matched = 0u32;
        feed_detected_rows(&rows, &base_labels, &compiled, &mut acc, &mut matched, 2);
        assert_eq!(matched, 2);
        let fields = acc.finish();
        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields[0].cardinality, 2,
            "rows past the line_limit are never sampled"
        );
    }

    #[test]
    fn code_307_maps_to_scan_budget_bytes() {
        let e = ChError::Server {
            code: 307,
            message: "Code: 307. DB::Exception: Limit for bytes to read exceeded".to_string(),
        };
        let err = map_read_error(e, 1024);
        match err {
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
                assert_eq!(budget_bytes, 1024);
            }
            other => panic!("expected QueryTooBroad(ScanBudgetBytes), got {other:?}"),
        }
    }

    #[test]
    fn code_158_is_not_mapped_to_query_too_broad() {
        let e = ChError::Server {
            code: 158,
            message: "Code: 158. DB::Exception: Limit for rows to read exceeded".to_string(),
        };
        let err = map_read_error(e, 1024);
        assert!(matches!(err, ReadError::Clickhouse(_)));
    }

    // -- Issue #169: volume keying/aggregation, one test per oracle rule
    //    (grafana/loki:3.4.2 `PrepareLabelsAndMatchers`/`seriesvolume`) --

    fn vol_selector(matchers: &[(&str, MatchOp, &str)]) -> StreamSelector {
        StreamSelector {
            matchers: matchers
                .iter()
                .map(|(name, op, value)| Matcher {
                    name: name.to_string(),
                    op: *op,
                    value: value.to_string(),
                })
                .collect(),
        }
    }

    fn vol_names(list: &[&str]) -> BTreeSet<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// `(fingerprint, service, canonical labels JSON)` fixtures.
    fn vol_meta(entries: &[(u64, &str, &str)]) -> HashMap<u64, StreamMetaRow> {
        entries
            .iter()
            .map(|(fp, service, labels)| {
                (
                    *fp,
                    StreamMetaRow {
                        fingerprint: *fp,
                        service: service.to_string(),
                        labels: labels.to_string(),
                    },
                )
            })
            .collect()
    }

    fn vol_rows(list: &[(u64, u64)]) -> Vec<VolumeRow> {
        list.iter()
            .map(|(fingerprint, bytes)| VolumeRow {
                fingerprint: *fingerprint,
                bytes: *bytes,
            })
            .collect()
    }

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn volume_labels_to_match_uses_every_matcher_name_including_negative_ops() {
        // Oracle rule: `PrepareLabelsAndMatchers` adds EVERY `m.Name` —
        // `{env!="dev"}` keys results by each stream's `env` value.
        let sel = vol_selector(&[
            ("service_name", MatchOp::Eq, "checkout"),
            ("env", MatchOp::Neq, "dev"),
            ("app", MatchOp::Nre, "test.*"),
        ]);
        assert_eq!(
            volume_labels_to_match(&sel, &[]),
            vol_names(&["service_name", "env", "app"])
        );
    }

    #[test]
    fn volume_labels_to_match_prefers_the_target_set_over_matcher_names() {
        let sel = vol_selector(&[("service_name", MatchOp::Eq, "checkout")]);
        let targets = vec!["env".to_string(), "team".to_string()];
        assert_eq!(
            volume_labels_to_match(&sel, &targets),
            vol_names(&["env", "team"])
        );
    }

    #[test]
    fn inject_target_matchers_appends_a_dot_plus_regex_only_for_absent_names() {
        let le = LogExpr {
            selector: vol_selector(&[("service_name", MatchOp::Eq, "checkout")]),
            pipeline: Vec::new(),
        };
        let out = inject_target_matchers(&le, &["env".to_string(), "service_name".to_string()]);
        // `service_name` already has a matcher (any op counts) — only
        // `env` gains the injected `=~ ".+"`.
        assert_eq!(out.selector.matchers.len(), 2);
        assert_eq!(
            out.selector.matchers[1],
            Matcher {
                name: "env".to_string(),
                op: MatchOp::Re,
                value: ".+".to_string(),
            }
        );
    }

    #[test]
    fn series_mode_keys_by_the_matched_name_subset_of_the_stream_labels() {
        let meta = vol_meta(&[(1, "checkout", r#"{"env":"prod","team":"pay"}"#)]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 10)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            100,
        );
        // Only the matched name enters the key — `team`/`service_name`
        // are dropped from it.
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: pairs(&[("env", "prod")]),
                bytes: 10,
            }]
        );
    }

    #[test]
    fn series_mode_omits_an_absent_label_from_the_key() {
        // fp 2 has no `env` label: its key is the `service_name` pair
        // alone, never an empty-value `env` pair.
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"prod"}"#),
            (2, "checkout", r#"{}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 10), (2, 20)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env", "service_name"]),
            false,
            100,
        );
        assert_eq!(
            out,
            vec![
                VolumeEntry {
                    labels: pairs(&[("service_name", "checkout")]),
                    bytes: 20,
                },
                VolumeEntry {
                    labels: pairs(&[("env", "prod"), ("service_name", "checkout")]),
                    bytes: 10,
                },
            ]
        );
    }

    #[test]
    fn series_mode_groups_streams_matching_no_name_under_the_empty_key() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"prod"}"#),
            (2, "billing", r#"{"env":"dev"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 3), (2, 4)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["region"]),
            false,
            100,
        );
        // Neither stream carries `region`: both accumulate under `{}`.
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: Vec::new(),
                bytes: 7,
            }]
        );
    }

    #[test]
    fn labels_mode_uses_all_stream_names_when_no_targets_are_supplied() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"prod"}"#),
            (2, "checkout", r#"{"env":"dev","team":"pay"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 10), (2, 5)]),
            &meta,
            VolumeAggregateBy::Labels,
            &vol_names(&["service_name"]),
            false, // no targetLabels: every stream name counts
            100,
        );
        assert_eq!(
            out,
            vec![
                VolumeEntry {
                    labels: pairs(&[("env", "")]),
                    bytes: 15,
                },
                VolumeEntry {
                    labels: pairs(&[("service_name", "")]),
                    bytes: 15,
                },
                VolumeEntry {
                    labels: pairs(&[("team", "")]),
                    bytes: 5,
                },
            ]
        );
    }

    #[test]
    fn labels_mode_restricts_to_the_target_names_when_targets_are_supplied() {
        let meta = vol_meta(&[(1, "checkout", r#"{"env":"prod","team":"pay"}"#)]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 10)]),
            &meta,
            VolumeAggregateBy::Labels,
            &vol_names(&["env"]),
            true, // targetLabels supplied: only the target names count
            100,
        );
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: pairs(&[("env", "")]),
                bytes: 10,
            }]
        );
    }

    #[test]
    fn volume_entries_sort_bytes_desc_with_label_asc_tie_break() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"zeta"}"#),
            (2, "checkout", r#"{"env":"alpha"}"#),
            (3, "checkout", r#"{"env":"big"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 5), (2, 5), (3, 9)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            100,
        );
        // Bytes-desc first (big=9), then the 5-byte tie breaks label-asc
        // (alpha before zeta) — NEVER a plain label sort.
        assert_eq!(
            out.iter().map(|e| &e.labels[0].1).collect::<Vec<_>>(),
            vec!["big", "alpha", "zeta"]
        );
    }

    #[test]
    fn volume_limit_truncates_after_the_sort() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"small"}"#),
            (2, "checkout", r#"{"env":"large"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 1), (2, 100)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            1,
        );
        // limit=1 keeps the LARGER entry — truncation runs post-sort.
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: pairs(&[("env", "large")]),
                bytes: 100,
            }]
        );
    }

    #[test]
    fn a_zero_byte_rollup_row_still_contributes_a_zero_entry() {
        // A returned row with bytes = 0 contributes a 0 entry (a stream
        // with NO row contributes nothing — it is simply absent here).
        let meta = vol_meta(&[(1, "checkout", r#"{"env":"prod"}"#)]);
        let out = accumulate_volume(
            &vol_rows(&[(1, 0)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            100,
        );
        assert_eq!(
            out,
            vec![VolumeEntry {
                labels: pairs(&[("env", "prod")]),
                bytes: 0,
            }]
        );
    }

    #[test]
    fn volume_accumulation_saturates_instead_of_wrapping() {
        let meta = vol_meta(&[
            (1, "checkout", r#"{"env":"prod"}"#),
            (2, "checkout", r#"{"env":"prod"}"#),
        ]);
        let out = accumulate_volume(
            &vol_rows(&[(1, u64::MAX), (2, 2)]),
            &meta,
            VolumeAggregateBy::Series,
            &vol_names(&["env"]),
            false,
            100,
        );
        assert_eq!(out[0].bytes, u64::MAX, "saturating, never wrapping");
    }

    /// Issue #35: `read_query_settings` — the single source of truth
    /// `budget_settings`/`paging_settings` delegate to — carries exactly
    /// the byte-budget pair plus the raised `max_query_size`.
    #[test]
    fn read_query_settings_sets_the_scan_budget_and_the_raised_query_text_cap() {
        let s = read_query_settings(1024);
        assert_eq!(s.get("max_bytes_to_read"), Some("1024"));
        assert_eq!(s.get("read_overflow_mode"), Some("throw"));
        assert_eq!(
            s.get("max_query_size"),
            Some(crate::querytext::MAX_QUERY_TEXT_BYTES.to_string().as_str())
        );
    }

    /// Issue #133: the read settings carry the byte scan budget VERBATIM
    /// at the accepted minimum (1) and at the maximum config-accepted
    /// `reader.logql_scan_budget_bytes` — never ClickHouse's `0`
    /// (unlimited) sentinel.
    #[test]
    fn read_query_settings_carry_the_budget_verbatim_at_the_accepted_min_and_ceiling() {
        assert_eq!(read_query_settings(1).get("max_bytes_to_read"), Some("1"));
        let cap = pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING;
        let s = read_query_settings(cap);
        assert_eq!(
            s.get("max_bytes_to_read"),
            Some(cap.to_string().as_str()),
            "the ceiling budget must pass through verbatim"
        );
        assert_ne!(s.get("max_bytes_to_read"), Some("0"));
    }

    /// Issue #133: the paging loop's termination guard still fires at the
    /// maximum config-accepted budget — `spent == budget` terminates
    /// (never issues a zero/unlimited remaining cap), one byte under
    /// does not. Synthetic counts; the extracted decision IS the
    /// top-of-loop guard in `run_streams_paged`.
    #[test]
    fn paging_termination_still_fires_at_the_max_accepted_scan_budget() {
        let cap = pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING;
        assert!(scan_budget_spent(cap, cap));
        assert!(!scan_budget_spent(cap - 1, cap));
    }

    /// Issue #35 acceptance criterion 2: the full-shape admission
    /// identity — `stage2`'s worst-case rendering (100,000 `u64::MAX`
    /// fingerprint literals, the documented `DEFAULT_MAX_STREAMS` cap)
    /// fits comfortably under [`crate::querytext::MAX_QUERY_TEXT_BYTES`]
    /// while exceeding ClickHouse's 262,144-byte default — proving the
    /// raised setting is load-bearing, not vacuous.
    #[test]
    fn stage2_at_default_max_streams_worst_case_fits_under_the_query_text_cap() {
        let fps: Vec<u64> =
            std::iter::repeat_n(u64::MAX, super::super::params::DEFAULT_MAX_STREAMS).collect();
        let sql = super::super::sql::stage2("log_streams", &fps);
        let bytes = sql.len() as u64;
        assert!(
            bytes > 262_144,
            "worst-case stage2 SQL ({bytes} B) must exceed the ClickHouse default cap to prove \
             the raised setting is load-bearing"
        );
        assert!(
            bytes < crate::querytext::MAX_QUERY_TEXT_BYTES,
            "worst-case stage2 SQL ({bytes} B) must fit under the {}-byte cap",
            crate::querytext::MAX_QUERY_TEXT_BYTES
        );
    }

    /// The full guaranteed-admitted envelope this issue's plan derives:
    /// 100,000 worst-case fingerprints + 10,000 escaped 64-byte service
    /// literals + 1 MiB of pre-rendered line-filter predicate text ≈ 3.73
    /// MiB — comfortably under the 8 MiB cap, comfortably over the
    /// ClickHouse default.
    fn worst_case_envelope() -> (Vec<u64>, Vec<String>, Vec<String>) {
        let fps: Vec<u64> =
            std::iter::repeat_n(u64::MAX, super::super::params::DEFAULT_MAX_STREAMS).collect();
        // Pre-escaped 64-byte literals (`'` + 62 chars + `'`), matching
        // `stage3`'s documented "services are pre-escaped string literals"
        // contract — the SQL builders never re-escape these.
        let services: Vec<String> = (0..10_000).map(|i| format!("'{i:062}'")).collect();
        // 16 × 64 KiB pre-rendered predicates ≈ 1 MiB, a generous multiple
        // of any realistic compiled line-filter chain.
        let line_filters: Vec<String> = std::iter::repeat_n("x".repeat(65_536), 16).collect();
        (fps, services, line_filters)
    }

    #[test]
    fn stage3_at_the_full_worst_case_envelope_fits_under_the_query_text_cap() {
        let (fps, services, line_filters) = worst_case_envelope();
        let sql = super::super::sql::stage3(
            "log_samples",
            &services,
            &fps,
            super::super::sql::TimeWindow {
                start_ns: 0,
                end_ns: i64::MAX,
            },
            &line_filters,
            Direction::Backward,
            u32::MAX,
        );
        let bytes = sql.len() as u64;
        assert!(bytes > 262_144, "stage3 envelope SQL is {bytes} B");
        assert!(
            bytes < crate::querytext::MAX_QUERY_TEXT_BYTES,
            "stage3 envelope SQL is {bytes} B, expected < {}",
            crate::querytext::MAX_QUERY_TEXT_BYTES
        );
    }

    #[test]
    fn stage3_keyset_at_the_full_worst_case_envelope_fits_under_the_query_text_cap() {
        let (fps, services, line_filters) = worst_case_envelope();
        let sql = super::super::sql::stage3_keyset(
            "log_samples",
            &services,
            &fps,
            super::super::sql::TimeWindow {
                start_ns: 0,
                end_ns: i64::MAX,
            },
            super::super::sql::KeysetLower::After {
                tuple: (i64::MAX, u64::MAX, u64::MAX),
                offset: u32::MAX,
            },
            Direction::Backward,
            &line_filters,
            u32::MAX,
        );
        let bytes = sql.len() as u64;
        assert!(bytes > 262_144, "stage3_keyset envelope SQL is {bytes} B");
        assert!(
            bytes < crate::querytext::MAX_QUERY_TEXT_BYTES,
            "stage3_keyset envelope SQL is {bytes} B, expected < {}",
            crate::querytext::MAX_QUERY_TEXT_BYTES
        );
    }

    #[test]
    fn exceeding_the_stream_cap_maps_to_stream_cap_not_scan_budget_bytes() {
        let err = check_stream_cap(100_001, 100_000).unwrap_err();
        match err {
            ReadError::QueryTooBroad(TooBroadReason::StreamCap { count, cap }) => {
                assert_eq!(count, 100_001);
                assert_eq!(cap, 100_000);
            }
            other => panic!("expected QueryTooBroad(StreamCap), got {other:?}"),
        }
    }

    #[test]
    fn a_count_at_or_below_the_cap_is_not_too_broad() {
        assert!(check_stream_cap(100_000, 100_000).is_ok());
        assert!(check_stream_cap(1, 100_000).is_ok());
    }

    // -- tail month-boundary refresh (issue #94 item 2) -----------------

    const DAY_NS: i64 = 86_400_000_000_000;

    fn tail_test_ctx() -> PlanCtx<'static> {
        PlanCtx {
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        }
    }

    /// Builds a `TailSetup` client-free (no engine/DB) — the shape
    /// `LogQlEngine::tail_setup` produces, so `refresh_tail_months` can be
    /// exercised against a `PlanCtx` literal.
    fn build_tail_setup(ctx: &PlanCtx<'_>, query: &str, start_ns: i64, end_ns: i64) -> TailSetup {
        let expr = pulsus_logql::parse(query).expect("parse");
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns,
                end_ns,
                step_ns: 1_000_000_000,
            },
            limit: 100,
            direction: Direction::Forward,
        };
        match plan::plan(&expr, &params, ctx).expect("plan") {
            Plan::Streams(sp) => {
                let compiled = CompiledPipeline::compile(&sp.pipeline).expect("compile");
                TailSetup {
                    plan: sp,
                    compiled,
                    expr,
                    base_params: params,
                    scan_floor_ns: start_ns,
                    covered_months: (plan::year_month(start_ns), plan::year_month(end_ns)),
                    resolved: Vec::new(),
                }
            }
            _ => panic!("stream selector must plan to Plan::Streams"),
        }
    }

    fn month_literal(ts_ns: i64) -> String {
        let (y, m) = plan::year_month(ts_ns);
        format!("'{y:04}-{m:02}-01'")
    }

    /// Counts occurrences of a quoted ClickHouse `Date` literal
    /// (`'YYYY-MM-01'`) in a SQL string — the exact shape
    /// `months_overlapping` emits (`plan.rs`).
    fn count_month_literals(sql: &str) -> usize {
        let bytes = sql.as_bytes();
        let mut count = 0;
        let mut i = 0;
        while i + 12 <= bytes.len() {
            let is_literal = bytes[i] == b'\''
                && bytes[i + 1..i + 5].iter().all(u8::is_ascii_digit)
                && bytes[i + 5] == b'-'
                && bytes[i + 6..i + 8].iter().all(u8::is_ascii_digit)
                && bytes[i + 8] == b'-'
                && bytes[i + 9] == b'0'
                && bytes[i + 10] == b'1'
                && bytes[i + 11] == b'\'';
            if is_literal {
                count += 1;
                i += 12;
            } else {
                i += 1;
            }
        }
        count
    }

    /// Dec 1 2023 00:00:00 UTC, in ns — a fixed month-boundary instant
    /// reused across the U1-U8 scan-gate tests.
    const DEC_1_2023_NS: i64 = 1_701_388_800_000_000_000;
    /// Nov 1 2023 00:00:00 UTC, in ns.
    const NOV_1_2023_NS: i64 = 1_698_796_800_000_000_000;

    /// AC3(c) (catch-up phase, `narrow=false`): a refresh whose
    /// `[scan_floor_ns, upper_ns]` covers the SAME `(lo_month, hi_month)`
    /// pair the plan already covers leaves `stage1_sql` byte-identical (no
    /// re-plan, no fire).
    #[test]
    fn tail_refresh_months_is_a_noop_when_the_covered_window_is_unchanged() {
        let ctx = tail_test_ctx();
        // 2023-11-14T22:13:20Z — comfortably mid-month.
        let setup_end = 1_700_000_000_000_000_000i64;
        let setup_start = setup_end - DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);
        let sql0 = setup.plan.stage1_sql.clone();
        let covered0 = setup.covered_months;

        let same_month_upper = setup_end + 3_600_000_000_000; // +1h, still November
        assert_eq!(
            (
                plan::year_month(setup_start),
                plan::year_month(same_month_upper)
            ),
            covered0,
            "lower/upper stay within the setup's covered months"
        );
        refresh_tail_months(&ctx, &mut setup, setup_start, same_month_upper, false)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.plan.stage1_sql, sql0,
            "no-op keeps stage1_sql byte-identical"
        );
        assert_eq!(setup.covered_months, covered0);
    }

    /// AC3(a)+(b)+(c), adapted to the v6-v8 scan-gated phase split: (a) a
    /// catch-up (`narrow=false`) window straddling a month boundary keeps
    /// BOTH month literals (full-span from the frozen floor); (b) only
    /// once the scan gate certifies narrowing (`narrow=true`) at a window
    /// wholly in the later month is the STALE month dropped (the growth
    /// bound); (c) a repeat refresh over the same covered window is a
    /// byte-identical no-op.
    #[test]
    fn tail_refresh_months_straddles_then_narrows_dropping_the_stale_month() {
        let ctx = tail_test_ctx();
        let setup_start = 1_700_000_000_000_000_000i64; // November 2023
        let setup_end = setup_start + 3_600_000_000_000; // +1h, same month
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);
        let month_a_lit = month_literal(setup_start);

        // (a) straddle, still catch-up (narrow=false): lower stays in
        // November, upper crosses into December — both months must
        // resolve or a prior-month stream vanishes mid-straddle.
        let straddle_upper = setup_start + 40 * DAY_NS;
        let month_b_lit = month_literal(straddle_upper);
        assert_ne!(month_a_lit, month_b_lit, "the test crosses a month");
        refresh_tail_months(&ctx, &mut setup, setup_start, straddle_upper, false)
            .expect("no I/O, cannot fail");
        assert!(
            setup.plan.stage1_sql.contains(&month_a_lit)
                && setup.plan.stage1_sql.contains(&month_b_lit),
            "straddling catch-up window covers both months: {}",
            setup.plan.stage1_sql
        );

        // (b) narrow (scan gate open, `narrow=true`): the poll window
        // advances wholly into December, well past GRACE — the stale
        // November month must be DROPPED (the growth bound).
        let narrowed_lower = straddle_upper;
        let narrowed_upper = straddle_upper + 3_600_000_000_000;
        assert_eq!(
            plan::year_month(narrowed_lower),
            plan::year_month(narrowed_upper),
            "the narrowed window stays within December"
        );
        refresh_tail_months(&ctx, &mut setup, narrowed_lower, narrowed_upper, true)
            .expect("no I/O, cannot fail");
        assert!(
            setup.plan.stage1_sql.contains(&month_b_lit),
            "narrowed stage1_sql still covers December: {}",
            setup.plan.stage1_sql
        );
        assert!(
            !setup.plan.stage1_sql.contains(&month_a_lit),
            "narrowed stage1_sql must DROP the stale November month: {}",
            setup.plan.stage1_sql
        );
        assert_eq!(
            setup.covered_months,
            (
                plan::year_month(narrowed_lower),
                plan::year_month(narrowed_upper)
            )
        );

        // (c) a repeat call over the same covered window is a no-op.
        let sql_after = setup.plan.stage1_sql.clone();
        refresh_tail_months(&ctx, &mut setup, narrowed_lower, narrowed_upper, true)
            .expect("no I/O");
        assert_eq!(
            setup.plan.stage1_sql, sql_after,
            "no double-fire over an unchanged covered window"
        );
    }

    /// U1 (issue #94 v6-v8): during catch-up (`narrow=false`) the scan set
    /// stays FULL-SPAN from the frozen `scan_floor_ns` no matter how many
    /// month boundaries the poll window's upper edge crosses — the
    /// pre-#94-v6 behaviour, request-bounded. Fails under a
    /// floor-always-advances mutation (refresh ignoring `narrow`).
    #[test]
    fn tail_refresh_months_u1_catchup_stays_full_span_with_the_floor_frozen() {
        let ctx = tail_test_ctx();
        let setup_start = NOV_1_2023_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);
        let setup_month_lit = month_literal(setup_start);

        // Lowers jump forward 20 days/step, narrow=false throughout —
        // crosses at least 3 month boundaries (Nov->Dec->Jan->Feb).
        let mut lower = setup_start;
        for step in 0..6 {
            let upper = lower + 20 * DAY_NS;
            refresh_tail_months(&ctx, &mut setup, lower, upper, false)
                .expect("no I/O, cannot fail");
            assert!(
                setup.plan.stage1_sql.contains(&setup_month_lit),
                "step {step}: catch-up (narrow=false) must stay full-span from the setup \
                 floor: {}",
                setup.plan.stage1_sql
            );
            let expected = plan::months_overlapping(setup_start, upper).len();
            assert_eq!(
                count_month_literals(&setup.plan.stage1_sql),
                expected,
                "step {step}: catch-up's full-span set == months_overlapping(setup_start, \
                 upper)"
            );
            assert_eq!(
                setup.scan_floor_ns, setup_start,
                "step {step}: floor stays frozen throughout catch-up"
            );
            lower = upper;
        }
    }

    /// U2 (issue #94 v6-v8): at the scan gate (`narrow=true`), a `lower`
    /// within GRACE of a month start keeps the PREVIOUS month in the scan
    /// set (the registration-lag band); once `lower` passes GRACE, the
    /// previous month is dropped. A same-window repeat is a byte-identical
    /// no-op. Fails under `TAIL_REGISTRATION_GRACE_NS = 0`.
    #[test]
    fn tail_refresh_months_u2_grace_band_keeps_the_previous_month_within_grace() {
        let ctx = tail_test_ctx();
        // The clamp arm (scan_floor_ns starts here) sits well over a year
        // before December, so every narrow=true call below binds on the
        // `lower - GRACE` arm, never the clamp.
        let setup_start = DEC_1_2023_NS - 400 * DAY_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);

        // Within GRACE: lower is 30min past the December boundary (<
        // GRACE=1h) ⇒ lower-GRACE lands in November ⇒ both months.
        let lower_in_band = DEC_1_2023_NS + 30 * 60_000_000_000; // +30min
        let upper = lower_in_band + 60_000_000_000; // +60s
        refresh_tail_months(&ctx, &mut setup, lower_in_band, upper, true)
            .expect("no I/O, cannot fail");
        assert_eq!(
            count_month_literals(&setup.plan.stage1_sql),
            2,
            "within GRACE: the previous month is retained: {}",
            setup.plan.stage1_sql
        );

        // A same-window repeat is a byte-identical no-op.
        let sql_after_band = setup.plan.stage1_sql.clone();
        refresh_tail_months(&ctx, &mut setup, lower_in_band, upper, true)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.plan.stage1_sql, sql_after_band,
            "byte-identical no-op over an unchanged covered window"
        );

        // Past GRACE: lower advances beyond the boundary + GRACE ⇒ the
        // previous month is dropped.
        let lower_past_band = DEC_1_2023_NS + TAIL_REGISTRATION_GRACE_NS + 60_000_000_000;
        let upper2 = lower_past_band + 60_000_000_000;
        refresh_tail_months(&ctx, &mut setup, lower_past_band, upper2, true)
            .expect("no I/O, cannot fail");
        assert_eq!(
            count_month_literals(&setup.plan.stage1_sql),
            1,
            "past GRACE: the previous month is dropped: {}",
            setup.plan.stage1_sql
        );
    }

    /// U3 (issue #94 v6-v8): a live-advanced floor stays FROZEN through a
    /// fall-behind episode (`narrow=false`, upper crossing a month — the
    /// set widens upper-only, never narrows) and resumes advancing (stale
    /// months dropping again) once the connection re-enters the scan gate
    /// (`narrow=true`).
    #[test]
    fn tail_refresh_months_u3_fall_behind_freezes_the_floor_then_resumes_on_reentry() {
        let ctx = tail_test_ctx();
        let setup_start = NOV_1_2023_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);

        // Live-advance the floor deep into December.
        let live_lower = DEC_1_2023_NS + 2 * 3_600_000_000_000; // Dec 1 + 2h
        refresh_tail_months(
            &ctx,
            &mut setup,
            live_lower,
            live_lower + 60_000_000_000,
            true,
        )
        .expect("no I/O, cannot fail");
        let floor_after_live = setup.scan_floor_ns;
        assert!(
            floor_after_live > setup_start,
            "floor advanced off the setup start"
        );
        assert_eq!(
            count_month_literals(&setup.plan.stage1_sql),
            1,
            "narrowed to December alone: {}",
            setup.plan.stage1_sql
        );

        // Fall behind (narrow=false): upper crosses into January — the
        // floor's month must be RETAINED (never reset to setup_start), the
        // set widens upper-only.
        let jan_1 = DEC_1_2023_NS + 31 * DAY_NS;
        refresh_tail_months(&ctx, &mut setup, live_lower, jan_1 + DAY_NS, false)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.scan_floor_ns, floor_after_live,
            "floor frozen while fallen behind"
        );
        assert!(
            setup
                .plan
                .stage1_sql
                .contains(&month_literal(DEC_1_2023_NS))
                && setup.plan.stage1_sql.contains(&month_literal(jan_1)),
            "widened upper-only: keeps December AND adds January: {}",
            setup.plan.stage1_sql
        );

        // Re-entry (narrow=true): the floor resumes advancing — stale
        // December drops.
        let live_lower2 = jan_1 + 2 * 3_600_000_000_000;
        refresh_tail_months(
            &ctx,
            &mut setup,
            live_lower2,
            live_lower2 + 60_000_000_000,
            true,
        )
        .expect("no I/O, cannot fail");
        assert!(
            setup.scan_floor_ns > floor_after_live,
            "floor resumed advancing on re-entry"
        );
        assert!(
            !setup
                .plan
                .stage1_sql
                .contains(&month_literal(DEC_1_2023_NS)),
            "stale December dropped once the floor resumes: {}",
            setup.plan.stage1_sql
        );
    }

    /// U4 (issue #94 AC2, updated for the v6-v8 scan-gated phase split,
    /// "bound the tail month IN-list growth"): once the scan gate has
    /// opened (`narrow=true`), the LIVE poll window's own width (not the
    /// connection's elapsed lifetime) determines `stage1_sql`'s month
    /// literal count. 36 steps span ~3 elapsed years; the count never
    /// grows.
    #[test]
    fn tail_refresh_months_stays_bounded_over_a_long_lived_connection() {
        let ctx = tail_test_ctx();
        let setup_start = NOV_1_2023_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);

        const STEP_NS: i64 = 30 * DAY_NS; // the connection's elapsed lifetime, per step
        const WINDOW_NS: i64 = 60_000_000_000; // the live poll window itself (default slice)
        const STEPS: u32 = 36;
        let mut first_count = None;
        let mut last_count = 0;
        for step in 0..STEPS {
            // Past the clamp arm + GRACE from step 0 on, so every step
            // narrows on the `lower - GRACE` arm — a genuinely live poll.
            let lower = setup_start
                + i64::from(step) * STEP_NS
                + TAIL_REGISTRATION_GRACE_NS
                + 60_000_000_000;
            let upper = lower + WINDOW_NS;
            refresh_tail_months(&ctx, &mut setup, lower, upper, true).expect("no I/O, cannot fail");
            let expected =
                plan::months_overlapping(lower - TAIL_REGISTRATION_GRACE_NS, upper).len();
            let count = count_month_literals(&setup.plan.stage1_sql);
            assert_eq!(
                count, expected,
                "step {step}: stage1_sql's literal count must equal \
                 months_overlapping(lower - GRACE, upper), never the connection's elapsed \
                 month count"
            );
            assert!(
                count <= 2,
                "step {step}: bounded by the live poll window's width (60s window + GRACE \
                 stays within 2 calendar months), got {count}"
            );
            if step == 0 {
                first_count = Some(count);
            }
            last_count = count;
        }
        assert_eq!(
            Some(last_count),
            first_count,
            "no growth across {STEPS} elapsed months — the connection-lifetime blow-up is fixed"
        );
    }

    /// U5 (issue #94 v6-v8, codex test-gap 2): during catch-up
    /// (`narrow=false`), the rebuilt scan set is a PURE function of the
    /// `[scan_floor_ns, upper_ns]` window — 1000 refreshes at an identical
    /// window are byte-identical after the first (poll count/elapsed
    /// lifetime cannot grow it); advancing `upper` across exactly one
    /// month boundary adds exactly one literal.
    #[test]
    fn tail_refresh_months_u5_identical_window_refreshes_are_pure_no_accretion() {
        let ctx = tail_test_ctx();
        let setup_start = NOV_1_2023_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);

        let lower = setup_start + 5 * DAY_NS;
        let upper = lower + DAY_NS;
        refresh_tail_months(&ctx, &mut setup, lower, upper, false).expect("no I/O, cannot fail");
        let sql_after_first = setup.plan.stage1_sql.clone();
        let covered_after_first = setup.covered_months;
        let floor_after_first = setup.scan_floor_ns;

        for call in 0..1_000u32 {
            refresh_tail_months(&ctx, &mut setup, lower, upper, false)
                .expect("no I/O, cannot fail");
            assert_eq!(
                setup.plan.stage1_sql, sql_after_first,
                "call {call}: byte-identical over 1000 same-window refreshes"
            );
            assert_eq!(setup.covered_months, covered_after_first);
            assert_eq!(
                setup.scan_floor_ns, floor_after_first,
                "call {call}: floor unmoved by identical-window catch-up refreshes"
            );
        }

        // Advance upper across exactly one month boundary: exactly one
        // literal added.
        let count_before = count_month_literals(&setup.plan.stage1_sql);
        let crossed_upper = setup_start + 35 * DAY_NS; // crosses into December
        refresh_tail_months(&ctx, &mut setup, lower, crossed_upper, false)
            .expect("no I/O, cannot fail");
        let count_after = count_month_literals(&setup.plan.stage1_sql);
        assert_eq!(
            count_after,
            count_before + 1,
            "exactly one literal added on the month crossing"
        );
        assert_eq!(
            count_after,
            plan::months_overlapping(setup.scan_floor_ns, crossed_upper).len()
        );
        assert_eq!(
            setup.scan_floor_ns, floor_after_first,
            "floor still unmoved (narrow=false throughout)"
        );
    }

    /// U8 (issue #94 v8, "clamp-qualified dichotomy"): the codex
    /// counterexample (a clamped start minutes after a month boundary),
    /// committed as the DOCUMENTED class-(i) residual (issue #134) — the
    /// prior month is never scanned by this connection, at setup, through
    /// catch-up, or after the scan gate narrows. The floor's clamp arm can
    /// only ever equal `s`, never fall below it — identical to the landed
    /// pre-#94-v6 code (`refresh_tail_months` fixed `start_ns` at the
    /// setup floor).
    #[test]
    fn tail_refresh_months_u8_boundary_start_never_scans_the_prior_month() {
        let ctx = tail_test_ctx();
        let s = DEC_1_2023_NS + 5 * 60_000_000_000; // 5 minutes past the boundary
        // Setup spans into January so BOTH refresh calls below actually
        // trigger a re-plan (`want != covered_months`) — a same-month-only
        // construction would make every call a no-op and never exercise
        // the clamp arm's `qp.spec.start_ns` assignment at all (the drill
        // must actually rebuild `stage1_sql` to be non-vacuous).
        let setup_end = s + 40 * DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, s, setup_end);
        let dec_lit = month_literal(s);
        let nov_lit = month_literal(DEC_1_2023_NS - DAY_NS);
        assert_ne!(
            dec_lit, nov_lit,
            "construction sanity: s sits in a different month"
        );

        // (setup) at construction: December present, November absent.
        assert!(setup.plan.stage1_sql.contains(&dec_lit));
        assert!(!setup.plan.stage1_sql.contains(&nov_lit));

        // (catch-up, narrow=false), upper wholly within December (a
        // genuine re-plan: covered_months starts at (Dec,Jan)): the
        // clamp arm floor == s exactly, never below — November stays out
        // of the scan universe.
        refresh_tail_months(&ctx, &mut setup, s, s + 10 * DAY_NS, false)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.scan_floor_ns, s,
            "clamp arm floor == s exactly, never below"
        );
        assert!(setup.plan.stage1_sql.contains(&dec_lit));
        assert!(!setup.plan.stage1_sql.contains(&nov_lit));

        // (post-gate, narrow=true) with `lower` still within GRACE of `s`
        // but `upper` reaching into January (another genuine re-plan): the
        // clamp arm still wins on the LOWER side (`lower - GRACE < s`),
        // floor stays `s` — November must stay excluded even though the
        // window's upper edge has moved on.
        let narrow_lower = s + 30 * 60_000_000_000; // s + 30min, < s + GRACE
        let narrow_upper = narrow_lower + 35 * DAY_NS; // reaches January
        assert_ne!(
            month_literal(narrow_upper),
            dec_lit,
            "construction sanity: narrow_upper reaches a later month"
        );
        refresh_tail_months(&ctx, &mut setup, narrow_lower, narrow_upper, true)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.scan_floor_ns, s,
            "still pinned at s: lower - GRACE has not advanced past s"
        );
        assert!(setup.plan.stage1_sql.contains(&dec_lit));
        assert!(
            !setup.plan.stage1_sql.contains(&nov_lit),
            "the prior month is never scanned by this connection, per the clamp-qualified \
             dichotomy (issue #134 class (i)): {}",
            setup.plan.stage1_sql
        );
    }

    /// Issue #94 AC4 first bullet — the orphan-cache mechanism: a
    /// fingerprint present in an EARLIER merge survives a LATER merge that
    /// no longer includes it (its registration month scrolled out of the
    /// narrowed stage-1 window, but the connection still remembers it).
    #[test]
    fn merge_resolved_preserves_a_fingerprint_absent_from_a_later_batch() {
        let mut cache: Vec<u64> = Vec::new();
        merge_resolved(&mut cache, &[5, 1, 3]);
        assert_eq!(
            cache,
            vec![1, 3, 5],
            "sorted + deduped after the first batch"
        );

        // The second (later, narrowed-window) batch no longer resolves
        // fingerprint 1, repeats 3, and adds a new fingerprint 7.
        merge_resolved(&mut cache, &[7, 3]);
        assert_eq!(
            cache,
            vec![1, 3, 5, 7],
            "fingerprint 1 (absent from the second batch) survives; 3 dedups; 7 is added"
        );

        merge_resolved(&mut cache, &[]);
        assert_eq!(cache, vec![1, 3, 5, 7], "an empty batch changes nothing");
    }

    #[test]
    fn a_generic_server_error_passes_through_unmapped() {
        let e = ChError::Server {
            code: 62,
            message: "syntax error".to_string(),
        };
        assert!(matches!(map_read_error(e, 1024), ReadError::Clickhouse(_)));
    }

    #[test]
    fn a_timeout_is_never_reinterpreted_as_a_budget_error() {
        let e = ChError::Timeout("deadline".to_string());
        assert!(matches!(map_read_error(e, 1024), ReadError::Clickhouse(_)));
    }

    fn tail_row(ts: i64, fp: u64, hash: u64) -> TailSampleRow {
        TailSampleRow {
            fingerprint: fp,
            timestamp_ns: ts,
            body: format!("b{hash}"),
            body_hash: hash,
            structured_metadata: String::new(),
        }
    }

    /// Issue #74: an empty page never moves the boundary cursor (the
    /// scan watermark, owned by the caller, advances instead — round-4
    /// adjudication #2).
    #[test]
    fn advance_tail_cursor_keeps_the_previous_cursor_on_an_empty_page() {
        let prev = Some(TailCursor {
            tuple: (10, 1, 5),
            seen: 2,
        });
        assert_eq!(advance_tail_cursor(prev, &[]), prev);
        assert_eq!(advance_tail_cursor(None, &[]), None);
    }

    /// Issue #74 (round-4 adjudication #1): `seen` counts exactly the
    /// trailing run of rows equal to the last row's tuple.
    #[test]
    fn advance_tail_cursor_counts_the_trailing_tie_run() {
        let rows = [
            tail_row(10, 1, 1),
            tail_row(10, 2, 7),
            tail_row(10, 2, 7),
            tail_row(10, 2, 7),
        ];
        let next = advance_tail_cursor(None, &rows).expect("non-empty page");
        assert_eq!(next.tuple, (10, 2, 7));
        assert_eq!(next.seen, 3);
    }

    /// Issue #74: when a tie group is split across pages (`OFFSET` skipped
    /// the prior page's rows), the unchanged tuple carries `seen` forward;
    /// a changed tuple resets it.
    #[test]
    fn advance_tail_cursor_carries_seen_for_an_unchanged_tuple_and_resets_on_change() {
        let prev = Some(TailCursor {
            tuple: (10, 2, 7),
            seen: 3,
        });
        // Page 2 of the same tie group: every row still equals the tuple.
        let same = [tail_row(10, 2, 7), tail_row(10, 2, 7)];
        let next = advance_tail_cursor(prev, &same).expect("non-empty page");
        assert_eq!(next.tuple, (10, 2, 7));
        assert_eq!(next.seen, 5, "3 already delivered + 2 new");

        // The cursor tuple changed: the count restarts at the new run.
        let moved = [tail_row(10, 2, 7), tail_row(11, 1, 4)];
        let next = advance_tail_cursor(prev, &moved).expect("non-empty page");
        assert_eq!(next.tuple, (11, 1, 4));
        assert_eq!(next.seen, 1);
    }

    /// Issue #74 v4 AC2 collision seam (review round 1): two DISTINCT
    /// bodies sharing one `body_hash` — a genuine CityHash collision is
    /// impractical to construct, so the equal-hash pair is injected at
    /// the comparator seam. The cursor treats them as one tuple run
    /// (the SQL side keeps them adjacent and stably ordered via the raw
    /// `body` tiebreaker), so the occurrence count paginates each
    /// exactly once: a `LIMIT`-split collision pair carries `seen`
    /// across pages instead of re-delivering or skipping the second
    /// body.
    #[test]
    fn advance_tail_cursor_paginates_a_hash_collision_pair_exactly_once() {
        // Page 1 fetched only the first colliding body (LIMIT split the
        // pair mid-run).
        let first = TailSampleRow {
            fingerprint: 7,
            timestamp_ns: 10,
            body: "alpha".to_string(),
            body_hash: 42,
            structured_metadata: String::new(),
        };
        let second = TailSampleRow {
            fingerprint: 7,
            timestamp_ns: 10,
            body: "beta".to_string(),
            body_hash: 42, // injected collision: distinct body, same hash
            structured_metadata: String::new(),
        };
        let c1 = advance_tail_cursor(None, std::slice::from_ref(&first)).expect("cursor");
        assert_eq!(c1.tuple, (10, 7, 42));
        assert_eq!(
            c1.seen, 1,
            "one occurrence of the colliding tuple delivered"
        );

        // Page 2 (SQL: `>= tuple OFFSET 1`) fetches exactly the second
        // colliding body; the unchanged tuple carries the count forward.
        let c2 = advance_tail_cursor(Some(c1), std::slice::from_ref(&second)).expect("cursor");
        assert_eq!(c2.tuple, (10, 7, 42));
        assert_eq!(
            c2.seen, 2,
            "both distinct bodies of the collision counted — the next OFFSET skips exactly both"
        );

        // Both bodies in ONE page: the trailing run spans the whole
        // equal-hash group regardless of the differing bodies.
        let both = [first, second];
        let c = advance_tail_cursor(None, &both).expect("cursor");
        assert_eq!(c.seen, 2);
    }

    /// Issue #74 AC1 (v1, still standing): a pipeline'd tail poll and the
    /// query path evaluate lines through the SAME `CompiledPipeline`
    /// compiled from the SAME `StreamsPlan` — identical per-line output
    /// on identical rows (tail is the query path, not a parallel engine).
    #[test]
    fn tail_pipeline_output_is_identical_to_the_query_paths_on_the_same_rows() {
        let expr = pulsus_logql::parse(r#"{app="x"} |= "keep" | logfmt | y="z""#).expect("parse");
        let ctx = PlanCtx {
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 1,
            max_streams: 100,
            pipeline_scan_factor: 10,
        };
        let qp = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000,
                step_ns: 1_000_000_000,
            },
            limit: 100,
            direction: Direction::Forward,
        };
        let Ok(Plan::Streams(sp)) = plan::plan(&expr, &qp, &ctx) else {
            panic!("stream selector must plan to Plan::Streams");
        };
        // The two compile sites (tail_setup and run_streams_inner) both
        // compile `sp.pipeline` — prove the outputs coincide per line.
        let tail_compiled = CompiledPipeline::compile(&sp.pipeline).expect("compile");
        let query_compiled = CompiledPipeline::compile(&sp.pipeline).expect("compile");

        let mut meta = HashMap::new();
        meta.insert(
            1u64,
            StreamMetaRow {
                fingerprint: 1,
                service: "checkout".to_string(),
                labels: r#"{"app":"x","service_name":"checkout"}"#.to_string(),
            },
        );
        // Rows model the post-SQL fetch: the `|= "keep"` prefix is pushed
        // down into stage-3/keyset SQL on BOTH paths (never re-evaluated
        // in-engine), so every synthetic row already contains it; the
        // in-engine `logfmt | y="z"` label filter is what drops row 11.
        let rows = || {
            vec![
                SampleRow {
                    fingerprint: 1,
                    timestamp_ns: 10,
                    body: "keep y=z msg=a".to_string(),
                    structured_metadata: String::new(),
                },
                SampleRow {
                    fingerprint: 1,
                    timestamp_ns: 11,
                    body: "keep y=other".to_string(),
                    structured_metadata: String::new(),
                },
            ]
        };
        let mut tail_out = run_pipeline_rows(rows(), &tail_compiled, &meta, 100);
        let mut query_out = run_pipeline_rows(rows(), &query_compiled, &meta, 100);
        tail_out.sort_by(|a, b| a.labels_json.cmp(&b.labels_json));
        query_out.sort_by(|a, b| a.labels_json.cmp(&b.labels_json));
        assert_eq!(tail_out, query_out);
        // And the pipeline genuinely evaluated: only the `y="z"` +
        // `|= "keep"` survivor remains.
        let entries: Vec<_> = tail_out.iter().flat_map(|s| s.entries.clone()).collect();
        assert_eq!(entries, vec![(10, "keep y=z msg=a".to_string())]);
    }

    #[test]
    fn escape_query_placeholders_doubles_a_literal_question_mark() {
        assert_eq!(
            escape_query_placeholders("match(val, '^(?:prod|staging)$')"),
            "match(val, '^(??:prod|staging)$')"
        );
    }

    #[test]
    fn escape_query_placeholders_doubles_every_occurrence() {
        assert_eq!(escape_query_placeholders("a? b? c?"), "a?? b?? c??");
    }

    #[test]
    fn escape_query_placeholders_leaves_sql_without_question_marks_untouched() {
        let sql = "SELECT fingerprint FROM log_streams_idx WHERE key = 'env'";
        assert_eq!(escape_query_placeholders(sql), sql);
    }

    /// Round-2 review, finding rejected (sound round-trip, verified against
    /// `clickhouse` 0.15.1's `SqlBuilder::new`): each literal `?` maps to
    /// `??`, so a user regex containing a literal `??` (e.g. `a??`) becomes
    /// `a????` here — an even-length run of 4, which the crate's lexer
    /// pairs cleanly back into 2 literal `?`s, restoring the original `a??`
    /// exactly. The full escape→execute→unbind round-trip against a live
    /// server isn't unit-testable here (`SqlBuilder` is `pub(crate)` to
    /// the `clickhouse` crate); it's covered end-to-end by the live
    /// `stage1_regex_matcher_...` / `stage3_regex_line_filter_...` /
    /// `stage3_not_regex_line_filter_...` `EXPLAIN` cases
    /// (`tests/explain_indexes.rs`), whose `(?:...)`/metacharacter regex
    /// patterns execute successfully against ClickHouse.
    #[test]
    fn escape_query_placeholders_doubles_a_literal_double_question_mark() {
        assert_eq!(escape_query_placeholders("a??"), "a????");
        assert_eq!(escape_query_placeholders("????"), "????????");
    }

    // -----------------------------------------------------------------
    // Issue M6-09 AC9(ii): the true limit applies globally after
    // in-engine filtering — both directions, fan-out, post-line_format
    // line filters. Hermetic over `run_pipeline_rows`, the exact function
    // `run_streams_inner` hands fetched rows to.
    // -----------------------------------------------------------------

    fn pipeline_of(query: &str) -> super::super::pipeline::CompiledPipeline {
        let expr = pulsus_logql::parse(query).expect("parse");
        let pulsus_logql::Expr::Log(log) = expr else {
            panic!("expected a log expr");
        };
        super::super::pipeline::CompiledPipeline::compile(&log.pipeline).expect("compile")
    }

    fn meta_two_streams() -> HashMap<u64, StreamMetaRow> {
        HashMap::from([
            (
                1u64,
                StreamMetaRow {
                    fingerprint: 1,
                    service: "checkout".to_string(),
                    labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
                },
            ),
            (
                2u64,
                StreamMetaRow {
                    fingerprint: 2,
                    service: "billing".to_string(),
                    labels: r#"{"env":"staging","service_name":"billing"}"#.to_string(),
                },
            ),
        ])
    }

    fn sample(fp: u64, ts: i64, body: &str) -> SampleRow {
        SampleRow {
            fingerprint: fp,
            timestamp_ns: ts,
            body: body.to_string(),
            structured_metadata: String::new(),
        }
    }

    /// Issue #97 review round 1, finding 3 (+ oracle probe against
    /// grafana/loki:3.4.2's default query response): a structured-metadata key
    /// that collides with a stream label key is renamed `<key>_extracted`; the
    /// stream label keeps the original key/value, both appear exactly once (no
    /// duplicate key entries), and the non-colliding SM key merges verbatim.
    /// Same `_extracted` precedence the `| json` parser already uses for
    /// parsed-label collisions.
    #[test]
    fn structured_metadata_key_colliding_with_base_label_lands_under_extracted_suffix() {
        // fp 1 base labels: env=prod, service_name=checkout.
        let meta = meta_two_streams();
        let compiled = pipeline_of(r#"{a="b"}"#);
        let rows = vec![SampleRow {
            fingerprint: 1,
            timestamp_ns: 10,
            body: "line".to_string(),
            structured_metadata: r#"{"env":"SMVAL","trace_id":"abc"}"#.to_string(),
        }];
        let results = run_pipeline_rows(rows, &compiled, &meta, 100);
        assert_eq!(results.len(), 1);
        // Canonical sorted JSON: the stream `env` keeps "prod"; the colliding
        // SM `env` surfaces as `env_extracted`; `trace_id` merges as-is.
        assert_eq!(
            results[0].labels_json,
            r#"{"env":"prod","env_extracted":"SMVAL","service_name":"checkout","trace_id":"abc"}"#
        );
    }

    /// Issue #97 review round 2, finding 1 (+ grafana/loki:3.4.2 oracle probe):
    /// a DOUBLE collision must still not emit a duplicate label entry. Base
    /// labels already carry both `env` AND `env_extracted`; the SM `env` renames
    /// to `env_extracted`, which ALSO exists — so it overwrites that slot
    /// (last-write-wins) rather than producing two `env_extracted` entries.
    /// Probed against grafana/loki:3.4.2's default query response: base
    /// `env=prod`+`env_extracted=baseval` + SM `env=smval` renders exactly one
    /// `env_extracted`, and the SM value wins it (no `env_extracted_extracted`,
    /// no numeric suffix, no drop).
    #[test]
    fn structured_metadata_double_collision_overwrites_the_extracted_slot_once() {
        // A stream whose base labels include both `env` and `env_extracted`.
        let meta: HashMap<u64, StreamMetaRow> = [(
            7u64,
            StreamMetaRow {
                fingerprint: 7,
                service: "checkout".to_string(),
                labels: r#"{"env":"prod","env_extracted":"baseval","service_name":"checkout"}"#
                    .to_string(),
            },
        )]
        .into_iter()
        .collect();
        let compiled = pipeline_of(r#"{a="b"}"#);
        let rows = vec![SampleRow {
            fingerprint: 7,
            timestamp_ns: 10,
            body: "line".to_string(),
            structured_metadata: r#"{"env":"smval"}"#.to_string(),
        }];
        let results = run_pipeline_rows(rows, &compiled, &meta, 100);
        assert_eq!(results.len(), 1);
        // Exactly one `env_extracted`, carrying the SM value (last-write-wins);
        // the stream `env` keeps "prod"; no duplicate key entries.
        assert_eq!(
            results[0].labels_json,
            r#"{"env":"prod","env_extracted":"smval","service_name":"checkout"}"#
        );
    }

    /// Companion double-collision case: the SM object ITSELF supplies both the
    /// colliding key and its `_extracted` form. Base `env=prod`; SM
    /// `env=smval`,`env_extracted=smextra`. The renamed `env` and the literal
    /// `env_extracted` land in the same slot — last-write-wins, one entry.
    /// Matches the grafana/loki:3.4.2 oracle probe (`env_extracted=smextra`).
    #[test]
    fn structured_metadata_supplying_its_own_extracted_key_collapses_to_one_entry() {
        let meta: HashMap<u64, StreamMetaRow> = [(
            7u64,
            StreamMetaRow {
                fingerprint: 7,
                service: "checkout".to_string(),
                labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
            },
        )]
        .into_iter()
        .collect();
        let compiled = pipeline_of(r#"{a="b"}"#);
        let rows = vec![SampleRow {
            fingerprint: 7,
            timestamp_ns: 10,
            body: "line".to_string(),
            structured_metadata: r#"{"env":"smval","env_extracted":"smextra"}"#.to_string(),
        }];
        let results = run_pipeline_rows(rows, &compiled, &meta, 100);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels_json,
            r#"{"env":"prod","env_extracted":"smextra","service_name":"checkout"}"#
        );
    }

    /// Backward-direction arrival order (newest first), interleaved
    /// across two streams; every row survives the filter — the global
    /// truncation must keep the first `limit` in ARRIVAL order, not
    /// `limit` per stream.
    #[test]
    fn result_limit_applies_globally_across_streams_in_backward_order() {
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let rows = vec![
            sample(1, 40, r#"{"status":"500","m":"d"}"#),
            sample(2, 30, r#"{"status":"500","m":"c"}"#),
            sample(1, 20, r#"{"status":"500","m":"b"}"#),
            sample(2, 10, r#"{"status":"500","m":"a"}"#),
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 3);
        let total: usize = results.iter().map(|r| r.entries.len()).sum();
        assert_eq!(total, 3, "global cap, not per-stream");
        let mut kept: Vec<i64> = results
            .iter()
            .flat_map(|r| r.entries.iter().map(|(ts, _)| *ts))
            .collect();
        kept.sort_unstable();
        assert_eq!(kept, vec![20, 30, 40], "newest three in backward order");
    }

    #[test]
    fn result_limit_applies_globally_in_forward_order_too() {
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let rows = vec![
            sample(2, 10, r#"{"status":"500","m":"a"}"#),
            sample(1, 20, r#"{"status":"500","m":"b"}"#),
            sample(2, 30, r#"{"status":"500","m":"c"}"#),
            sample(1, 40, r#"{"status":"500","m":"d"}"#),
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 3);
        let mut kept: Vec<i64> = results
            .iter()
            .flat_map(|r| r.entries.iter().map(|(ts, _)| *ts))
            .collect();
        kept.sort_unstable();
        assert_eq!(kept, vec![10, 20, 30], "oldest three in forward order");
    }

    /// The fan-out path splits one source stream by parsed label set and
    /// still respects the global limit; dropped lines don't count toward
    /// it.
    #[test]
    fn fan_out_regroups_by_final_label_set_with_canonical_labels_json() {
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let rows = vec![
            sample(1, 10, r#"{"status":"500","method":"GET"}"#),
            sample(1, 20, r#"{"status":"200","method":"GET"}"#), // dropped
            sample(1, 30, r#"{"status":"500","method":"PUT"}"#),
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 100);
        assert_eq!(results.len(), 2, "one result stream per final label set");
        let total: usize = results.iter().map(|r| r.entries.len()).sum();
        assert_eq!(total, 2);
        for r in &results {
            assert!(
                r.labels_json.contains(r#""env":"prod""#)
                    && r.labels_json.contains(r#""status":"500""#),
                "canonical labels_json must carry base + parsed labels: {}",
                r.labels_json
            );
            // Canonical rendering: sorted keys.
            assert!(
                r.labels_json.find("\"env\"").unwrap() < r.labels_json.find("\"method\"").unwrap()
            );
            assert_eq!(r.fingerprint, fnv1a64(r.labels_json.as_bytes()));
            assert_eq!(r.service, "checkout");
        }
    }

    /// A post-`line_format` line filter evaluates in-engine over the
    /// REWRITTEN line, drops non-matching entries, and the survivors
    /// respect the global limit.
    #[test]
    fn a_post_line_format_line_filter_drops_in_engine_and_respects_the_limit() {
        let compiled =
            pipeline_of(r#"{a="b"} | json | line_format "{{.method}} {{.status}}" |= "500""#);
        let rows = vec![
            sample(1, 10, r#"{"status":"500","method":"GET"}"#),
            sample(1, 20, r#"{"status":"200","method":"GET"}"#), // rewritten line lacks "500"
            sample(1, 30, r#"{"status":"500","method":"PUT"}"#),
            sample(1, 40, r#"{"status":"500","method":"DELETE"}"#),
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 2);
        let mut entries: Vec<(i64, String)> = results
            .iter()
            .flat_map(|r| r.entries.iter().cloned())
            .collect();
        entries.sort();
        assert_eq!(
            entries,
            vec![(10, "GET 500".to_string()), (30, "PUT 500".to_string())],
            "rewritten survivors only, capped globally at 2"
        );
    }

    /// Issue #90 AC1 (exact fill, hermetic): a heavily-dropping pipeline
    /// fed page-by-page through ONE `StreamAccumulator` fills to exactly
    /// `result_limit`, whereas the pre-#90 single oversampled scan (one
    /// `run_pipeline_rows` over just the first page) under-returned. The
    /// accumulator's grouping and global truncation span pages.
    #[test]
    fn stream_accumulator_fills_exactly_to_the_limit_across_pages() {
        // Only every 4th line matches `status = "500"` — sparse survivors.
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let statuses = ["200", "404", "500", "503"];
        let page = |base_ts: i64| -> Vec<SampleRow> {
            (0..4)
                .map(|i| {
                    let ts = base_ts + i;
                    sample(
                        1,
                        ts,
                        &format!(r#"{{"status":"{}","m":"{ts}"}}"#, statuses[i as usize]),
                    )
                })
                .collect()
        };
        let meta = meta_two_streams();

        // The pre-#90 behaviour: a single page of 4 rows yields only ONE
        // survivor — an under-return against a limit of 3.
        assert_eq!(
            run_pipeline_rows(page(0), &compiled, &meta, 3)
                .iter()
                .map(|r| r.entries.len())
                .sum::<usize>(),
            1,
            "one page under-returns (1 < limit 3) — the old divergence",
        );

        // Fetch-until-limit: feed successive pages until the accumulator
        // reports the limit is filled.
        let mut acc = StreamAccumulator::new(&meta, 3);
        let mut pages = 0;
        let mut base_ts = 0;
        loop {
            let filled = acc.feed(&page(base_ts), &compiled);
            pages += 1;
            base_ts += 4;
            if filled {
                break;
            }
            assert!(pages < 100, "must terminate");
        }
        let total: usize = acc.into_streams().iter().map(|r| r.entries.len()).sum();
        assert_eq!(total, 3, "exact fill to the limit across pages");
        assert_eq!(
            pages, 3,
            "one survivor per 4-row page ⇒ 3 pages fill limit 3"
        );
    }

    /// Issue #90: the accumulator never over-fills — once the limit is
    /// reached, a further `feed` adds nothing and keeps reporting filled.
    #[test]
    fn stream_accumulator_never_over_returns_on_a_later_page() {
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let rows = |ts: i64| {
            vec![
                sample(1, ts, r#"{"status":"500","m":"x"}"#),
                sample(1, ts + 1, r#"{"status":"500","m":"y"}"#),
            ]
        };
        let meta = meta_two_streams();
        let mut acc = StreamAccumulator::new(&meta, 3);
        assert!(!acc.feed(&rows(0), &compiled), "2 < 3, not filled");
        assert!(acc.feed(&rows(10), &compiled), "2 + 2 ⇒ filled at 3");
        // A further page must not push the total past the limit.
        acc.feed(&rows(20), &compiled);
        let total: usize = acc.into_streams().iter().map(|r| r.entries.len()).sum();
        assert_eq!(
            total, 3,
            "global cap holds across pages, never over-returns"
        );
    }

    /// The transform path (drops/rewrites but never touches labels) keeps
    /// the hydrated `labels_json` verbatim and the source fingerprint.
    #[test]
    fn transform_path_keeps_labels_json_verbatim() {
        let compiled = pipeline_of(r#"{a="b"} | line_format "L={{.env}}" |= "L=prod""#);
        let rows = vec![
            sample(1, 10, "anything"),
            sample(2, 20, "anything"), // env=staging -> rewritten "L=staging" -> dropped
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 100);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].fingerprint, 1);
        assert_eq!(
            results[0].labels_json, r#"{"env":"prod","service_name":"checkout"}"#,
            "transform path must splice hydration labels verbatim"
        );
        assert_eq!(results[0].entries, vec![(10, "L=prod".to_string())]);
    }

    /// Round-2 finding 1: the hand-rolled borrowed-label JSON renderer
    /// must stay byte-compatible with `serde_json`'s escaping (the shape
    /// the writer/encoder ecosystem produces and splices verbatim).
    #[test]
    fn render_labels_json_sorted_matches_serde_json_escaping_byte_for_byte() {
        let pairs = [
            ("plain", "value"),
            ("quote", r#"a"b"#),
            ("backslash", r"a\b"),
            ("newline_tab", "a\nb\tc"),
            ("carriage_bs_ff", "a\rb\u{08}c\u{0C}d"),
            ("low_control", "a\u{01}b\u{1f}c"),
            ("unicode", "日本語µ"),
        ];
        let sorted: Vec<(Cow<'_, str>, Cow<'_, str>)> = pairs
            .iter()
            .map(|(k, v)| (Cow::Borrowed(*k), Cow::Borrowed(*v)))
            .collect();
        let ours = render_labels_json_sorted(&sorted);
        // serde_json reference rendering of the same ordered pairs.
        let mut reference = String::from("{");
        for (i, (k, v)) in pairs.iter().enumerate() {
            if i > 0 {
                reference.push(',');
            }
            reference.push_str(&serde_json::to_string(k).unwrap());
            reference.push(':');
            reference.push_str(&serde_json::to_string(v).unwrap());
        }
        reference.push('}');
        assert_eq!(ours, reference);
        // And the canonical shape stays round-trippable / re-parseable.
        let parsed = parse_flat_labels(&ours);
        assert_eq!(parsed.len(), pairs.len());
    }

    #[test]
    fn fnv1a64_is_stable_and_content_sensitive() {
        let a = fnv1a64(br#"{"a":"1"}"#);
        assert_eq!(a, fnv1a64(br#"{"a":"1"}"#));
        assert_ne!(a, fnv1a64(br#"{"a":"2"}"#));
    }

    #[test]
    fn parse_flat_labels_reads_simple_pairs() {
        let pairs = parse_flat_labels(r#"{"env":"prod","team":"checkout"}"#);
        assert_eq!(
            pairs,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "checkout".to_string())
            ]
        );
    }

    #[test]
    fn parse_flat_labels_handles_escaped_quotes_and_backslashes() {
        let pairs = parse_flat_labels(r#"{"msg":"a\"b\\c"}"#);
        assert_eq!(pairs, vec![("msg".to_string(), "a\"b\\c".to_string())]);
    }

    #[test]
    fn parse_flat_labels_of_empty_object_is_empty() {
        assert!(parse_flat_labels("{}").is_empty());
    }

    #[test]
    fn series_labels_injects_service_name_from_the_physical_column() {
        let meta = StreamMetaRow {
            fingerprint: 1,
            service: "checkout".to_string(),
            labels: r#"{"env":"prod"}"#.to_string(),
        };
        let labels = series_labels(&meta);
        assert!(labels.contains(&("service_name".to_string(), "checkout".to_string())));
        assert!(labels.contains(&("env".to_string(), "prod".to_string())));
    }

    #[test]
    fn group_range_sum_by_reduces_matching_steps() {
        let mut a = BTreeMap::new();
        a.insert(0i64, 1.0);
        a.insert(60, 2.0);
        let mut b = BTreeMap::new();
        b.insert(0i64, 3.0);
        let series = vec![
            RangeSeries {
                labels: vec![("service_name".to_string(), "checkout".to_string())],
                points: a,
            },
            RangeSeries {
                labels: vec![("service_name".to_string(), "checkout".to_string())],
                points: b,
            },
        ];
        let grouping = Grouping {
            kind: GroupingKind::By,
            labels: vec!["service_name".to_string()],
        };
        let grouped = group_range(series, VectorAggOp::Sum, Some(&grouping), None);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].points.get(&0), Some(&4.0));
        assert_eq!(grouped[0].points.get(&60), Some(&2.0));
    }

    /// Review round 2, finding 1: the grid count uses checked i128
    /// arithmetic — the extreme/degenerate shapes below must saturate or
    /// zero out, never panic or wrap past the cap.
    #[test]
    fn grid_bucket_count_is_overflow_safe_at_extreme_bounds() {
        // Full i64 range at step 1: ~2^64 buckets, saturates cleanly.
        assert_eq!(grid_bucket_count(i64::MIN, i64::MAX, 1), u64::MAX);
        // Deep-negative window at step 1: ~2^62 buckets, exact.
        assert_eq!(
            grid_bucket_count(i64::MIN, i64::MIN / 2, 1),
            (i64::MIN / 2).abs_diff(i64::MIN)
        );
        // Inverted/empty windows are zero buckets, never an underflow.
        assert_eq!(grid_bucket_count(i64::MAX, i64::MIN, 1), 0);
        assert_eq!(grid_bucket_count(0, 0, 1), 0);
        // A zero step (structurally `InvalidStep` upstream) and a step
        // wider than i64 (never produced by `parse_step`) both saturate
        // so the cap guard rejects them by name instead of `bucket_of`
        // ever dividing by a degenerate step.
        assert_eq!(grid_bucket_count(0, 1_000, 0), u64::MAX);
        assert_eq!(grid_bucket_count(0, 1_000, u64::MAX), u64::MAX);
        // Ordinary shapes stay exact (the 11k-boundary golden covers the
        // cap itself): (0, 120] at step 60 touches buckets 0, 60, and
        // the end-edge bucket 120.
        assert_eq!(grid_bucket_count(0, 120, 60), 3);
        // Full i64 range at a half-range step: floor((MIN+1)/s) = -3
        // through floor(MAX/s) = 2 — six buckets.
        assert_eq!(
            grid_bucket_count(i64::MIN, i64::MAX, (i64::MAX / 2) as u64),
            6
        );
    }

    /// Review round 1, finding 1 (quantile bound): the exact-quantile
    /// retention cap trips as a NAMED too-broad error the moment the
    /// value count crosses [`MAX_QUANTILE_VALUES`] — driven through the
    /// real `push_rows` fold with the counter pre-charged to the
    /// boundary (a 4M-row fixture would be pure waste).
    #[test]
    fn quantile_value_retention_past_the_cap_is_a_named_too_broad_error() {
        let stages = match pulsus_logql::parse(
            r#"quantile_over_time(0.5, {a="b"} | logfmt | unwrap v [1m])"#,
        )
        .expect("parse")
        {
            Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) => range.selector.pipeline,
            other => panic!("unexpected expr: {other:?}"),
        };
        let compiled = super::super::pipeline::CompiledPipeline::compile(&stages).expect("compile");
        let client = plan::ClientAgg {
            pipeline: stages,
            value: plan::ClientValue::Unwrap,
            range_op: RangeAggOp::QuantileOverTime,
            param: Some(0.5),
            absent_labels: Vec::new(),
        };
        let meta = HashMap::from([(
            1u64,
            StreamMetaRow {
                fingerprint: 1,
                service: "checkout".to_string(),
                labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
            },
        )]);
        let window = ClientWindow {
            start_ns: 0,
            end_ns: 60_000_000_000,
            step_ns: None,
        };
        let mut state = ClientAggState::new(&compiled, &meta, &client, window, None).unwrap();
        state.quantile_values = MAX_QUANTILE_VALUES - 1;
        let rows = [
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 1,
                body: "v=1".to_string(),
            },
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 2,
                body: "v=2".to_string(),
            },
        ];
        let err = state.push_rows(&rows).unwrap_err();
        match err {
            ReadError::QueryTooBroad(TooBroadReason::QuantileValues { count, cap }) => {
                assert_eq!(cap, MAX_QUANTILE_VALUES);
                assert_eq!(count, MAX_QUANTILE_VALUES + 1);
            }
            other => panic!("expected QueryTooBroad(QuantileValues), got {other:?}"),
        }
    }

    #[test]
    fn apply_rate_divides_by_the_window_in_seconds() {
        assert_eq!(apply_rate(300.0, Some(5_000_000_000)), 60.0);
    }

    #[test]
    fn apply_rate_is_identity_when_no_window_is_given() {
        assert_eq!(apply_rate(42.0, None), 42.0);
    }
}
