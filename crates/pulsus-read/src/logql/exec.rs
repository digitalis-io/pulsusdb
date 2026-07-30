//! `LogQlEngine` — executes a [`super::plan::Plan`] against ClickHouse via
//! `ChClient`, injects the scan budget, maps overflow codes to
//! [`ReadError::QueryTooBroad`], and finishes vector aggregations in Rust
//! (docs/schemas.md §3.2: "the engine maps fingerprints to `service` and
//! finishes the `sum by`"). Deliberately **not** snapshot-tested — SQL
//! generation itself is `plan`/`sql`'s job and is tested there without a
//! database; this module's own test coverage is the error-mapping unit
//! tests (architect plan amendment §4).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use futures::{Stream, StreamExt};
use pulsus_clickhouse::{ChClient, ChError, ChRow, ChRowStream, QuerySettings};
use pulsus_logql::{
    BinOp, Expr, Grouping, GroupingKind, LabelFilterExpr, LabelFmt, LineFilterOp, LogExpr,
    MatchGroup, MatchOp, Matcher, MetricExpr, NumericLiteral, ParserStage, RangeAggOp, Stage,
    StreamSelector, VectorAggOp, VectorMatching,
};

use super::cms;
use super::detected::{self, DetectedFieldOut, DetectedFields, DetectedLabelOut, FieldAccumulator};
use super::error::{ReadError, TooBroadReason};
use super::explain::PlanExplain;
use super::params::{Direction, PlanCtx, QueryParams, QuerySpec, TimeBounds, ValidatedDuration};
use super::pipeline::{CompiledPipeline, ERROR_DETAILS_LABEL, ERROR_LABEL, MetricRun};
use super::plan::{self, ClientAgg, ClientValue, MetricNode, MetricPlan, Plan, StreamsPlan};
use super::rows::{
    DetectedLabelRow, LabelNameRow, LabelValueRow, LogStatsRow, MetricBucketRow, MetricInstantRow,
    MetricScanRow, PatternFetchRow, SampleRow, StreamMetaRow, StreamRow, TailSampleRow, VolumeRow,
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
    /// `log_patterns` (M7-C3, issue #171), `_dist`-aware exactly like the
    /// other Logs-family tables. Only the `/api/logs/v1/patterns` read
    /// targets it.
    pub patterns_table: String,
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
            // Issue #236: [`MAX_QUERY_SERIES`] is a FINAL-RESULT cap, so it
            // is applied here — on the whole expression's output — and
            // never inside `run_metric_inner`/`run_metric_node`/
            // `apply_vector_aggs`, where it would reject on scanned or
            // intermediate groups the reference never counts.
            Plan::Metric(mp) => {
                let result = self.run_metric_inner(&mp, None).await?;
                ensure_result_series(&result)?;
                Ok(result)
            }
            Plan::MetricBinary(node) => {
                let result = self.run_metric_node(&node, None).await?;
                ensure_result_series(&result)?;
                Ok(result)
            }
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
                ensure_result_series(&result)?;
                Ok((result, explain))
            }
            Plan::MetricBinary(node) => {
                let mut explain = PlanExplain::new(binary_result_type(&node, params));
                let result = self.run_metric_node(&node, Some(&mut explain)).await?;
                ensure_result_series(&result)?;
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
            run_pipeline_rows(rows, &compiled, &meta, sp.result_limit)?,
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
            let filled = acc.feed(&sample_rows, compiled)?;

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
                    metric_plan_window(mp),
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
                mp.scan_lower,
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
                mp.scan_lower,
                step_ns.as_u64(),
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
        let is_range = mp.step_ns.is_some();
        let time_window = super::sql::TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        };
        // Issue #227: a range query reads in physical-key order
        // (`optimize_read_in_order`, no server sort) for the streaming slide;
        // an instant query keeps the total-timestamp order its reducers pin.
        let sql = client_metric_read_sql(mp, &services, fingerprints, time_window);
        let settings = if is_range {
            self.budget_settings()
                .set("optimize_read_in_order", 1)
                .set("max_memory_usage", RANGE_READ_MAX_MEMORY_BYTES)
        } else {
            self.budget_settings()
        };
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
        let window = metric_plan_window(mp);
        // Issue #236 Part B: on a range query the INNERMOST vector
        // aggregation is folded at the leaf. `vector_aggs` is outer-first
        // (`unwrap_vector_aggs`) and collapses onto the leaf whenever the
        // base is a range expr, so `.last()` is the innermost one and
        // `apply_vector_aggs`' `.rev()` walk over the remaining prefix
        // continues exactly where the fold stopped. `folded` — not the
        // caller's intent — records what the leaf actually took: the fold
        // declines the specs it cannot own (`sort`/`sort_desc`/
        // `approx_topk`), and those must still be applied here.
        let (mut state, folded) = if is_range {
            let mut range = RangeSlideState::new(
                compiled,
                &meta,
                client,
                window,
                mp.rate_window_ns,
                AggCaps::DEFAULT,
            )?;
            if let Some(spec) = mp.vector_aggs.last() {
                range.attach_fold(spec);
            }
            let folded = range.folded_aggs();
            (MetricAggState::Range(Box::new(range)), folded)
        } else {
            // `is_range` is `mp.step_ns.is_some()` and `metric_plan_window`
            // builds a `Range` window exactly then, so this narrowing
            // cannot fail — but it is expressed as a narrowing rather than
            // asserted, so the instant state simply cannot be built for a
            // stepped window (issue #236 Part D).
            let instant = window
                .as_instant()
                .ok_or_else(|| ReadError::PipelineInvalid {
                    reason: "internal: a stepped window reached the instant aggregation state"
                        .to_string(),
                })?;
            (
                MetricAggState::Instant(Box::new(ClientAggState::new(
                    compiled,
                    &meta,
                    client,
                    instant,
                    mp.rate_window_ns,
                    AggCaps::DEFAULT,
                )?)),
                0,
            )
        };
        let mut chunk: Vec<MetricScanRow> = Vec::with_capacity(CLIENT_AGG_CHUNK_ROWS);
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule) — no other query
            // runs inside this block, and the lease ends at the brace.
            let mut stream = self.query_stream::<MetricScanRow>(&sql, &settings).await?;
            while let Some(row) = stream.next().await {
                chunk.push(row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?);
                if chunk.len() >= CLIENT_AGG_CHUNK_ROWS {
                    state.push_rows(&chunk)?;
                    chunk.clear();
                }
            }
        }
        state.push_rows(&chunk)?;
        let result = state.finish()?;
        Ok(apply_vector_aggs(
            result,
            &mp.vector_aggs[..mp.vector_aggs.len() - folded],
        ))
    }

    /// Executes `variants(...) of (...)` (issue #221): ONE scan (planned
    /// from the common log range alone — same stage-1 resolution, same
    /// hydration, same single `metric_read`, byte-identical SQL to the
    /// equivalent single-extractor query) streamed once and fanned out in
    /// memory to N sub-states. Mirrors `run_metric_inner` +
    /// `run_metric_client`: the arena compiles before any I/O (a bad
    /// regex is a 400, never a wasted scan) and CONTINUES the planner's
    /// `spec_bytes` charge counter — one budget for plan-time + exec-time
    /// fan-out state.
    async fn run_variants(
        &self,
        scan: &MetricPlan,
        variants: &[plan::VariantSpec],
        spec_bytes: u64,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
        let common = scan.client.as_ref().ok_or_else(|| {
            // Defense in depth: `build_variants_node` plans the scan with
            // `force_client = true`, so `client` is always `Some`.
            ReadError::PipelineInvalid {
                reason: "internal: variants scan plan must be client-aggregated".to_string(),
            }
        })?;
        let arena = VariantArena::build(
            &common.pipeline,
            variants,
            MAX_VARIANT_FANOUT_STATE_BYTES,
            spec_bytes,
        )?;
        if let Some(e) = explain.as_mut() {
            e.set_routing(scan.routing.clone());
            e.push("stage1_stream_resolution", scan.stage1_sql.clone(), None);
            for probe in &scan.probes {
                e.push(
                    "selectivity_probe",
                    probe.sql.clone(),
                    Some(format!("key = {}", probe.key)),
                );
            }
        }
        let fingerprints = self.resolve_fingerprints(&scan.stage1_sql).await?;
        if fingerprints.is_empty() {
            // An `absent_over_time` variant must still report absence
            // when the selector resolves NO streams at all — drive the
            // sub-states over an empty scan, WITHOUT pushing
            // `stage2_hydration`/`metric_read` (nothing runs).
            let empty_meta = HashMap::new();
            let mut state = VariantsAggState::new(
                &arena,
                variants,
                &empty_meta,
                MAX_VARIANT_FANOUT_STATE_BYTES,
            )?;
            state.push_rows(&[])?;
            return state.finish();
        }
        if let Some(e) = explain.as_mut() {
            e.push(
                "stage2_hydration",
                super::sql::stage2(&scan.streams_table, &fingerprints),
                None,
            );
        }
        let meta = self.hydrate(&scan.streams_table, &fingerprints).await?;
        let services = distinct_escaped_services(&meta);
        let is_range = scan.step_ns.is_some();
        let time_window = super::sql::TimeWindow {
            start_ns: scan.start_ns,
            end_ns: scan.end_ns,
        };
        let sql = client_metric_read_sql(scan, &services, &fingerprints, time_window);
        let settings = if is_range {
            self.budget_settings()
                .set("optimize_read_in_order", 1)
                .set("max_memory_usage", RANGE_READ_MAX_MEMORY_BYTES)
        } else {
            self.budget_settings()
        };
        if let Some(e) = explain.as_mut() {
            e.push(
                "metric_read",
                sql.clone(),
                Some(scan.routing.reason.clone()),
            );
        }
        let mut state =
            VariantsAggState::new(&arena, variants, &meta, MAX_VARIANT_FANOUT_STATE_BYTES)?;
        let mut chunk: Vec<MetricScanRow> = Vec::with_capacity(CLIENT_AGG_CHUNK_ROWS);
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule).
            let mut stream = self.query_stream::<MetricScanRow>(&sql, &settings).await?;
            while let Some(row) = stream.next().await {
                chunk.push(row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?);
                if chunk.len() >= CLIENT_AGG_CHUNK_ROWS {
                    state.push_rows(&chunk)?;
                    chunk.clear();
                }
            }
        }
        state.push_rows(&chunk)?;
        state.finish()
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
                MetricNode::VectorLit { value, window } => materialize_vector_lit(*value, window),
                MetricNode::VectorAgg { aggs, inner } => {
                    let result = self.run_metric_node(inner, explain.as_deref_mut()).await?;
                    Ok(apply_vector_aggs(result, aggs))
                }
                MetricNode::Variants {
                    scan,
                    variants,
                    spec_bytes,
                } => {
                    self.run_variants(scan, variants, *spec_bytes, explain)
                        .await
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
            // not a SQL aggregate. Issue #227 review round 5, finding 3:
            // EXPLAIN must report the query that ACTUALLY executes — a range
            // query runs the PK-ordered sliding scan (`run_metric_client`),
            // so reporting `metric_raw_samples` here made the
            // `explain_indexes` gates validate a query we never issue.
            client_metric_read_sql(mp, &services, &fingerprints, window)
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
                    mp.scan_lower,
                    step_ns.as_u64(),
                    &mp.extra_predicates,
                ),
                None => super::sql::metric_instant(
                    source,
                    &services,
                    &fingerprints,
                    window,
                    mp.scan_lower,
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

/// One `/api/logs/v1/patterns` series (M7-C3, issue #171): a distinct log
/// template and its per-step counts, `(unix_seconds, count)` ascending
/// (zero-count steps omitted). The engine preserves ClickHouse's
/// total-count-desc-then-pattern-asc order (the pushed-down `ORDER BY`), so
/// the top-1000 presentation IS the contract — the response encoder must not
/// re-sort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternSeries {
    pub pattern: String,
    pub samples: Vec<(i64, u64)>,
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

/// Whether a `/stats` pipeline is fully served by pushdown aggregation: a
/// stream selector plus PUSHABLE line filters only. Any other stage — or a
/// non-pushable `ip()`/mixed-`or` line filter (no client fallback on the
/// stats path) — must reject rather than silently over-count. Consults
/// [`plan::is_pushable_line_filter`], the single pushability source of truth.
fn stats_pipeline_is_pushdown_only(pipeline: &[Stage]) -> bool {
    pipeline
        .iter()
        .all(|s| matches!(s, Stage::LineFilter(lf) if plan::is_pushable_line_filter(lf)))
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
        // Only PUSHABLE line filters have a pushdown aggregation shape; a
        // parser/format/label-filter stage — OR a non-pushable `ip()`/mixed-
        // `or` line filter — would silently over-count if ignored. This path
        // is pushdown-only (no client pipeline), so a non-pushable line filter
        // must REJECT here rather than drop (defense in depth — the API layer
        // rejects most of these before parsing reaches the engine).
        if !stats_pipeline_is_pushdown_only(&sp.pipeline) {
            return Err(ReadError::PipelineInvalid {
                reason: "stats supports a stream selector plus line filters only (ip() line \
                         filters are not supported by stats)"
                    .to_string(),
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

    /// `/api/logs/v1/patterns` (M7-C3, issue #171, docs/api.md §2.6): stage-1
    /// fingerprint resolution, then ONE pushed-down aggregate over
    /// `log_patterns` (no hydration — the response carries no labels). `expr`
    /// must be a bare log stream selector; a metric query or ANY pipeline
    /// stage (line filters included — templates are precomputed, bodies are
    /// gone) is a 400-class rejection. `step_ns` is the caller-floored (10s)
    /// bucket resolution.
    pub async fn patterns(
        &self,
        expr: &Expr,
        b: TimeBounds,
        step_ns: u64,
    ) -> Result<Vec<PatternSeries>, ReadError> {
        self.patterns_inner(expr, b, step_ns, None).await
    }

    /// [`LogQlEngine::patterns`] plus its `X-Pulsus-Explain` trace, in the
    /// same single pass (no second scan) — the `query_explained` contract.
    pub async fn patterns_explained(
        &self,
        expr: &Expr,
        b: TimeBounds,
        step_ns: u64,
    ) -> Result<(Vec<PatternSeries>, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("patterns");
        let series = self
            .patterns_inner(expr, b, step_ns, Some(&mut explain))
            .await?;
        Ok((series, explain))
    }

    async fn patterns_inner(
        &self,
        expr: &Expr,
        b: TimeBounds,
        step_ns: u64,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<PatternSeries>, ReadError> {
        let ctx = self.config.plan_ctx();
        // `limit`/`direction`/`step` are unused placeholders — patterns never
        // reads samples through stage 3 (the `stats_inner`/`volume_inner`
        // idiom); its aggregation targets `log_patterns` directly.
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
                    reason: "patterns requires a log stream selector query (a metric query has no \
                             log patterns)"
                        .to_string(),
                });
            }
        };
        // Selector only: templates are precomputed and the bodies are gone, so
        // even a line filter has no meaning here (defense in depth — the API
        // layer rejects any pipeline stage before parsing reaches the engine).
        if !sp.pipeline.is_empty() {
            return Err(ReadError::PipelineInvalid {
                reason: "patterns supports a bare stream selector only (no pipeline stages)"
                    .to_string(),
            });
        }

        if let Some(e) = explain.as_mut() {
            e.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
        }
        let fingerprints = self.resolve_fingerprints(&sp.stage1_sql).await?;
        if fingerprints.is_empty() {
            return Ok(Vec::new());
        }

        let window = super::sql::TimeWindow {
            start_ns: b.start_ns,
            end_ns: b.end_ns,
        };
        let sql = super::sql::log_patterns_read(
            &self.config.patterns_table,
            &fingerprints,
            window,
            step_ns,
        );
        if let Some(e) = explain.as_mut() {
            e.push("patterns_read", sql.clone(), None);
        }

        let mut series: Vec<PatternSeries> = Vec::new();
        {
            // Scoped so the stream's pooled-connection lease drops before the
            // (pure-CPU) ns→seconds mapping below.
            let mut stream = self
                .query_stream::<PatternFetchRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
                // ClickHouse already ordered by (total desc, pattern asc) and
                // arraySorted the samples ascending by ts_ns; each ts_ns is a
                // multiple of step_ns (≥ 10s), so ns→unix-seconds is exact.
                let samples = row
                    .samples
                    .into_iter()
                    .map(|(ts_ns, count)| (ts_ns / 1_000_000_000, count))
                    .collect();
                series.push(PatternSeries {
                    pattern: row.pattern,
                    samples,
                });
            }
        }
        Ok(series)
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
                retention_capped: false,
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
            let mut matched = 0u32;
            let mut feeder = DetectedRowFeeder::new();
            {
                // The pooled-connection lease is held across the per-row
                // field detection: the streaming feeder (issue #244)
                // processes each row as it arrives and drops it — the
                // page's rows are never re-materialised — so the extra
                // hold is bounded by this scan's own row count (`LIMIT
                // line_limit`), and the lease still releases at end of
                // scope, before `finish()`.
                let mut stream = self
                    .query_stream::<SampleRow>(&sql, &self.budget_settings())
                    .await?;
                while let Some(row) = stream.next().await {
                    let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
                    if matched >= line_limit {
                        // `line_limit` stops FEEDING, never DRAINING.
                        continue;
                    }
                    if feeder.feed_row(
                        row.fingerprint,
                        row.timestamp_ns,
                        &row.body,
                        &row.structured_metadata,
                        &base_labels,
                        &compiled,
                        &mut acc,
                    )? {
                        matched += 1;
                    }
                }
            }
            let (fields, retention_capped) = acc.finish();
            return Ok(DetectedFields {
                fields,
                truncated: false,
                retention_capped,
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
        let (fields, retention_capped) = acc.finish();
        Ok(DetectedFields {
            fields,
            truncated,
            retention_capped,
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
    ///
    /// **Streaming (issue #244, claim C1):** each page's rows are fed
    /// through a [`DetectedRowFeeder`] as they arrive and dropped — no
    /// page-sized `Vec<TailSampleRow>` and no `SampleRow`
    /// re-materialisation; one row is live at a time, and what the feeder
    /// carries between rows is bounded by [`MAX_FEEDER_SCRATCH_BYTES`].
    /// A `ScanBudgetBytes` error mid-drain on a page with `spent > 0`
    /// returns the accumulated prefix with `pulsus_partial: true` — the
    /// prefix boundary is NOT required to align with a page boundary; on
    /// the FIRST page (`spent == 0`) it stays the #90 `QueryTooBroad` 422
    /// regardless of how many rows were already delivered
    /// ([`classify_page_error`]).
    ///
    /// **Claim C3 (issue #244), stated in terms:** the per-ROW transient —
    /// the pipeline run plus the auto-parse pass over one row — is
    /// **unbounded**, pre-existing on both read paths
    /// ([`StreamAccumulator::feed`] runs `run_into` per row and
    /// `eval_structured_metadata_row` runs `run_into_with_sm`), and
    /// bounded by neither this issue nor any cap in this module; where
    /// this doc records that #244 did not worsen it, that claim holds
    /// exactly as: *not worsened on the four row shapes AC 13 measures,
    /// compared at helper granularity against an in-tree transcription of
    /// `d145ded`'s per-row shape* — never the unqualified form. The bound
    /// is issue #265, sized by #287: `| json`'s full-flatten concatenates
    /// the parent prefix into every leaf key, so a crafted 65 536-B line
    /// expands to 97 615 872 B of simultaneously-live key bytes
    /// (measured), and a 1 048 576-B line to 24 988 948 292 B by the same
    /// exactly-validated formula — `Θ(L²)`, paid identically by
    /// `/query_range`'s `| json`; auto-parse only pays it unconditionally.
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
        let mut st = DetectedPagedState {
            feeder: DetectedRowFeeder::new(),
            cursor: None,
            spent: 0,
            matched: 0,
            page_size: sp.scan_limit.max(1),
            line_limit,
            budget,
        };

        loop {
            // Never issue a zero cap (ClickHouse's *unlimited* sentinel) —
            // once the budget is spent, return partial (the first-page
            // `spent == 0` case never reaches here).
            if scan_budget_spent(st.spent, budget) {
                return Ok(true);
            }
            let page_cap = budget.saturating_sub(st.spent); // now always > 0
            let ks_lower = match st.cursor {
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
                st.page_size,
            );

            // Open, then stream-drain one page; `read_bytes` is meaningful
            // only after the drain (wait_end_of_query=1). Scoped so the
            // stream's pooled-connection lease — now held across the
            // per-row detection CPU work, bounded to this one page —
            // releases before the next page opens.
            let decision = {
                let mut stream = match self
                    .query_stream::<TailSampleRow>(&sql, &self.paging_settings(page_cap))
                    .await
                {
                    Ok(stream) => stream,
                    // The same #90 branch split applies to an open-time
                    // overflow (pre-#244 it flowed through the one page
                    // `Result`).
                    Err(mapped) => return classify_page_error(mapped, st.spent),
                };
                st.absorb_page(
                    &mut stream,
                    |s| s.read_bytes(),
                    |e| map_read_error(e, budget),
                    base_labels,
                    compiled,
                    acc,
                )
                .await
            };
            if let Some(terminal) = decision? {
                return Ok(terminal);
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
        let streams = run_pipeline_rows(sample_rows, &setup.compiled, &meta, fetch_limit)?;
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

/// The `resultType` a binary metric plan produces: `scalar` for a tree
/// that produces no series (pure-literal, e.g. `5`/`5+3`), otherwise
/// vector/matrix per the query spec — the same rule the encoder applies to
/// the evaluated result. A `vector(n)` leaf (issue #221) produces a series
/// (`{} => n`), so `produces_series()` classifies a vector-lit-bearing tree
/// as vector/matrix even though it has no [`MetricNode::Leaf`].
fn binary_result_type(node: &MetricNode, params: &QueryParams) -> &'static str {
    if !node.produces_series() {
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
) -> Result<Vec<StreamResult>, ReadError> {
    // A one-shot feed over the whole slice — byte-identical output and
    // per-row allocation profile to the pre-#90 monolithic function (the
    // `logql_pipeline_alloc`/`logql_pipeline_golden` suites pin both).
    let mut acc = StreamAccumulator::new(meta, result_limit);
    acc.feed(&rows, compiled)?;
    Ok(acc.into_streams())
}

/// Issue #230 follow-up: a template-render output-budget breach is the
/// bounded 422 (the same complete-or-error class as every other
/// `QueryTooBroad` reason — never a truncation, never an OOM).
impl From<super::pipeline::TemplateBudgetExceeded> for ReadError {
    fn from(e: super::pipeline::TemplateBudgetExceeded) -> Self {
        ReadError::QueryTooBroad(TooBroadReason::TemplateOutputBytes {
            budget_bytes: e.budget_bytes,
        })
    }
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
    ) -> Result<bool, ReadError> {
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
        // The per-row reserved-SM routing outcome (issue #238), cleared and
        // refilled by the merge — reused across rows like the buffers above.
        let mut sm_ctx = StructuredMetadataCtx::default();

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
                let Some(line) =
                    compiled.run_into(&row.body, base, row.timestamp_ns, &mut scratch)?
                else {
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
                    &mut sm_ctx,
                );
                // Reuse `sm_scratch`'s allocation across rows: the helper takes
                // it by value (fresh per-row lifetime for the `merge_buf`
                // borrow), `recycle_label_scratch` returns the same allocation.
                let (survived, used) = eval_structured_metadata_row(
                    compiled,
                    &row.body,
                    &merge_buf,
                    &sm_ctx,
                    label_groups,
                    row.timestamp_ns,
                    &m.service,
                    sm_scratch,
                );
                sm_scratch = recycle_label_scratch(used?);
                if survived {
                    *survivors += 1;
                }
            }
        }

        Ok(*survivors >= *result_limit)
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
/// `step_ns: None` = instant (one window `(at-range, at]` reduced to a
/// single sample). `Some(step)` = a range query evaluated with Loki's
/// **sliding** windows (issue #227): the `[range]` window `(t-range, t]` is
/// re-evaluated at every start-anchored grid point `t ∈ {start+k·step ≤
/// end}`. `range_ns` is that `[range]` selector width — decoupled from
/// `step_ns` so `rate({}[1m])` and `rate({}[10m])` differ (the divisor and
/// the window both track `range`, never `step`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClientWindow {
    /// A single window, already materialised into `[start_ns, end_ns]` by the
    /// planner (`start = at - range`). An instant query has **no step grid
    /// and no residual `[range]`** — both are structurally absent here, so
    /// neither can be misread downstream.
    Instant { start_ns: i64, end_ns: i64 },
    /// Sliding range evaluation. BOTH durations are
    /// [`ValidatedDuration`]s — boundary-validated and therefore non-zero,
    /// **by type** (issue #227 review round 4, finding 2). There is no
    /// "absent"/zero representation for a range window, so
    /// [`RangeSlideState`] can only ever receive a real, validated range:
    /// the previous `ValidatedDuration::NONE` sentinel (a public unvalidated
    /// mint that could be placed in a range slot) is gone entirely.
    Range {
        /// The start-anchored emit grid's first point.
        grid_start_ns: i64,
        end_ns: i64,
        step_ns: ValidatedDuration,
        /// The `[range]` selector width — the sliding span `(t-range, t]`.
        range_ns: ValidatedDuration,
    },
}

impl ClientWindow {
    /// The evaluation window's lower bound (the emit grid's first point for
    /// a range query).
    pub fn start_ns(&self) -> i64 {
        match *self {
            ClientWindow::Instant { start_ns, .. } => start_ns,
            ClientWindow::Range { grid_start_ns, .. } => grid_start_ns,
        }
    }

    pub fn end_ns(&self) -> i64 {
        match *self {
            ClientWindow::Instant { end_ns, .. } | ClientWindow::Range { end_ns, .. } => end_ns,
        }
    }

    /// `Some` only for a range query — an instant window has no step grid.
    pub fn step_ns(&self) -> Option<ValidatedDuration> {
        match *self {
            ClientWindow::Instant { .. } => None,
            ClientWindow::Range { step_ns, .. } => Some(step_ns),
        }
    }
}

/// The evaluation grid for a leafless `vector(<scalar>)` node (issue #221):
/// a constant series materialised at every step point. Deliberately a
/// SEPARATE type from [`ClientWindow`] (issue #227 review round 4): a vector
/// literal has no `[range]` selector at all, so it must not be able to
/// occupy — or be built from — a range window's range slot.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GridWindow {
    pub start_ns: i64,
    pub end_ns: i64,
    /// `Some` = the range grid (validated step); `None` = a single instant
    /// sample.
    pub step_ns: Option<ValidatedDuration>,
}

/// The client-aggregated raw fetch SQL for a planned metric leaf — the ONE
/// implementation shared by execution (`run_metric_client`) and EXPLAIN
/// (`explain_metric`), so the reported query is by construction the query
/// that runs (issue #227 review round 5, finding 3). A RANGE query reads in
/// physical-key order for the streaming slide; an instant query keeps the
/// total-timestamp order its reducers pin.
fn client_metric_read_sql(
    mp: &MetricPlan,
    services: &[String],
    fingerprints: &[u64],
    window: super::sql::TimeWindow,
) -> String {
    if mp.step_ns.is_some() {
        super::sql::metric_raw_samples_sliding(
            &mp.table,
            services,
            fingerprints,
            window,
            mp.scan_lower,
            &mp.extra_predicates,
        )
    } else {
        super::sql::metric_raw_samples(
            &mp.table,
            services,
            fingerprints,
            window,
            mp.scan_lower,
            &mp.extra_predicates,
        )
    }
}

/// Builds the evaluation window for a planned metric leaf. The planner has
/// already validated both durations, so a `Some(step)` plan yields the
/// `Range` variant carrying two [`ValidatedDuration`]s and an instant plan
/// yields `Instant` — an inconsistent window is unrepresentable.
fn metric_plan_window(mp: &MetricPlan) -> ClientWindow {
    match mp.step_ns {
        Some(step_ns) => ClientWindow::Range {
            grid_start_ns: mp.grid_start_ns,
            end_ns: mp.end_ns,
            step_ns,
            range_ns: mp.range_ns,
        },
        None => ClientWindow::Instant {
            start_ns: mp.grid_start_ns,
            end_ns: mp.end_ns,
        },
    }
}

/// How many rows the streaming client-aggregation fetch buffers between
/// folds into [`ClientAggState`] — bounds transient memory without
/// per-row fold overhead (review round 1, finding 1).
const CLIENT_AGG_CHUNK_ROWS: usize = 8_192;

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

/// One bucket's state: streaming stats, the full value set for
/// `quantile_over_time`, or the timestamped counter samples for
/// `rate_counter` (reset detection is order-dependent, so the raw
/// `(ts, value)` points are retained and walked at `finish`).
#[derive(Debug, Clone)]
enum BucketAcc {
    Simple(SimpleAcc),
    Values(Vec<f64>),
    Counter(Vec<(i64, f64)>),
}

impl BucketAcc {
    fn new(op: RangeAggOp, ts_ns: i64, v: f64) -> Self {
        match op {
            RangeAggOp::QuantileOverTime => BucketAcc::Values(vec![v]),
            RangeAggOp::RateCounter => BucketAcc::Counter(vec![(ts_ns, v)]),
            _ => BucketAcc::Simple(SimpleAcc::new(ts_ns, v)),
        }
    }

    fn add(&mut self, ts_ns: i64, v: f64) {
        match self {
            BucketAcc::Simple(acc) => acc.add(ts_ns, v),
            BucketAcc::Values(vals) => vals.push(v),
            BucketAcc::Counter(pts) => pts.push((ts_ns, v)),
        }
    }

    /// Finishes the bucket into its reducer value.
    fn finish(self, op: RangeAggOp, rate_window_ns: Option<u64>, quantile: Option<f64>) -> f64 {
        match self {
            BucketAcc::Values(mut vals) => quantile_of(&mut vals, quantile.unwrap_or(f64::NAN)),
            BucketAcc::Counter(pts) => rate_counter_extrapolated(pts, rate_window_ns),
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
                // `rate_counter` is `Counter`-backed (reset detection is
                // order-dependent), never `Simple`.
                RangeAggOp::RateCounter => unreachable!("Counter-backed"),
            },
        }
    }
}

/// `rate_counter` reducer — a bit-exact replica of the pinned reference's
/// `extrapolatedRate(samples, selRange, isCounter=true, isRate=true)`
/// (grafana/loki v3.7.3, `pkg/logql/range_vector.go`), replayed over
/// PulsusDB's nanosecond timestamps.
///
/// Two load-bearing quirks make this diverge from a plain
/// `reset-aware increase / range.Seconds()`:
///  1. The reference's window iterators store sample timestamps in
///     nanoseconds, but `extrapolatedRate` divides every span by `1000`
///     (`durationMilliseconds`) — treating those ns values as ms. We keep
///     PulsusDB's ns timestamps and divide by `1000` identically, so the
///     unit-mix is reproduced rather than corrected.
///  2. The extrapolation window is anchored on the FIRST sample, not the
///     query step: `rangeStart = samples[0].T - durationMilliseconds(selRange)`,
///     `rangeEnd = samples[last].T`. So `durationToEnd == 0` and
///     `durationToStart == selRange_ms/1000 == 60.0` for a `[1m]` window —
///     the factor is `1 + 60000/span_ns` (~1.00006), NOT PromQL's ~6x
///     full-range extrapolation.
///
/// Samples are ordered by timestamp with a STABLE sort (no value
/// tie-break): duplicate-timestamp samples keep their delivered scan order
/// — the deterministic SQL scan order `ORDER BY timestamp_ns, fingerprint,
/// body` (`metric_raw_samples`) — matching the reference, which processes
/// same-timestamp unwrapped samples in storage/scan order rather than
/// re-sorting them by value (branch-validated: `5, 10, 3, 12` with `10`/`3`
/// tied at one timestamp yields increase 17 / `0.2833…`, not the 12 / 0.2 an
/// ascending-value sort would give).
///
/// The reset-aware increase is accumulated in the reference's own order
/// (`resultValue = last - first; for s { if s < lastValue { resultValue +=
/// lastValue } }`), which is the bit-exact form to match. The reference
/// EMITS `0.0` for `<2`-point groups (verified against grafana/loki:3.7.3 —
/// a lone sample returns value `"0"`), so we return `0.0` and let `finish`
/// surface it as a 0-valued vector element; we do NOT drop the group.
fn rate_counter_extrapolated(mut pts: Vec<(i64, f64)>, rate_window_ns: Option<u64>) -> f64 {
    pts.sort_by(|(at, _), (bt, _)| at.cmp(bt));
    rate_counter_over_sorted(pts.len(), |i| pts[i], rate_window_ns)
}

/// [`rate_counter_extrapolated`]'s body over an ALREADY timestamp-sorted
/// sequence addressed by index — `at(i)` yields the `i`-th `(ts, value)` in
/// ascending-`ts` order.
///
/// Split out so the sliding evaluator can reduce **over a borrow of the
/// live window**: the retained window is already in canonical `(ts,
/// stream_hash, tie_rank)` order (whose leading key is `ts`), so the sort
/// the owning form performs is a no-op on it, and copying it into a
/// `Vec<(i64, f64)>` at every grid point — as the round-4 code did — was
/// pure waste that also doubled peak memory for the copy (issue #227 review
/// round 5, finding 2). Values are read straight out of the window instead.
/// The arithmetic is byte-identical to the owning form; only the storage
/// differs.
fn rate_counter_over_sorted<F>(len: usize, at: F, rate_window_ns: Option<u64>) -> f64
where
    F: Fn(usize) -> (i64, f64),
{
    if len < 2 {
        return 0.0;
    }
    let Some(rng_ns) = rate_window_ns.filter(|w| *w > 0) else {
        return 0.0;
    };
    let (first_t, first_f) = at(0);
    let (last_t, last_f) = at(len - 1);

    // Reset-aware increase in the reference's accumulation order.
    let mut result_value = last_f - first_f;
    let mut last_value = 0.0_f64;
    for i in 0..len {
        let f = at(i).1;
        if f < last_value {
            result_value += last_value;
        }
        last_value = f;
    }

    // `durationMilliseconds(selRange)`; window anchored on the first sample.
    //
    // Issue #227 review finding 2: every timestamp SPAN is computed in `i128`
    // before the float conversion. `first_t - sel_range_ms` underflows i64 for
    // a first sample near `i64::MIN`, and `last_t - first_t` overflows i64 for
    // a window spanning near-`MIN` to near-`MAX` — both are valid extreme
    // inputs that would panic in debug / wrap in release. The i128 widening
    // changes no in-range value (the arithmetic is identical), it only removes
    // the overflow; the reference's ns-vs-ms unit mix is preserved verbatim.
    let sel_range_ms: i128 = (rng_ns / 1_000_000) as i128;
    let range_start = first_t as i128 - sel_range_ms; // ns − ms: the reference's unit mix
    let mut duration_to_start = (first_t as i128 - range_start) as f64 / 1000.0; // == sel_range_ms/1000
    let duration_to_end = 0.0_f64; // rangeEnd == last.T ⇒ 0
    let sampled_interval = (last_t as i128 - first_t as i128) as f64 / 1000.0;
    let average_duration = sampled_interval / (len - 1) as f64;

    if result_value > 0.0 && first_f >= 0.0 {
        let duration_to_zero = sampled_interval * (first_f / result_value);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let threshold = average_duration * 1.1;
    let mut extrapolate_to = sampled_interval;
    extrapolate_to += if duration_to_start < threshold {
        duration_to_start
    } else {
        average_duration / 2.0
    };
    extrapolate_to += if duration_to_end < threshold {
        duration_to_end
    } else {
        average_duration / 2.0
    };
    result_value *= extrapolate_to / sampled_interval;
    result_value / range_seconds(rng_ns) // isRate: / selRange.Seconds()
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

/// The start-anchored step grid `{grid_start + k·step : k≥0, ≤ end}` (issue
/// #227: Loki seeds `current` at `start-step`, first point `start`, iterates
/// `≤ end`). Used by `vector(<scalar>)`'s constant matrix so it aligns with
/// the sliding data leaves' start-anchored grid. i128 intermediates keep
/// `grid_start + k·step` overflow-free; every point lies in
/// `[grid_start, end]` (no clamp needed — `k ≥ 0`). The grid passed
/// [`ensure_grid_resolution`] before this runs (≤ 22_002 points; 11_001
/// when the span fits an int64 duration — round 8's saturating fence).
fn bucket_grid(grid_start: i64, end_ns: i64, step_ns: u64) -> Vec<i64> {
    if step_ns == 0 || step_ns > i64::MAX as u64 || end_ns < grid_start {
        return Vec::new();
    }
    let step = step_ns as i128;
    let gs = grid_start as i128;
    let kmax = (end_ns as i128 - gs) / step;
    // Saturating narrowing, like the sliding evaluator's `grid_point` (issue
    // #227 arithmetic sweep): every point is in `[grid_start, end]` by
    // construction, so the clamp is defense-in-depth, never a wrap.
    (0..=kmax).map(|k| clamp_bucket(gs + k * step)).collect()
}

/// The evaluation-resolution cap for client-aggregated range queries, in
/// step INTERVALS — Loki-exact (issue #227 review round 7, finding 1):
/// the reference rejects iff `(end - start) / step > 11000` (truncating
/// integer division, `loghttp/query.go` `errStepTooSmall`), so a request
/// with exactly 11 000 intervals is SERVED, and its inclusive
/// start-anchored grid holds `intervals + 1 = 11_001` points. Every grid
/// guard funnels through [`ensure_grid_resolution`], which encodes that
/// rule — rejected as `QueryTooBroad(MetricBuckets)` BEFORE any grid or
/// accumulator materialization (review round 1, finding 2 — an
/// `absent_over_time` over a huge range with a tiny step must never
/// allocate an attacker-sized grid). A documented constant, not a config
/// field (the `DEFAULT_MAX_STREAMS` precedent).
pub const MAX_CLIENT_AGG_BUCKETS: u64 = 11_000;

/// **The one grid-resolution guard** (issue #227 review round 7,
/// finding 1): rejects iff [`fence_intervals`] — the reference's own
/// SATURATING `floor((end - grid_start) / step)` (round 8) — exceeds
/// [`MAX_CLIENT_AGG_BUCKETS`], and returns the EXACT grid POINT count
/// ([`grid_point_count`]) on success. The HTTP boundary
/// (`ensure_range_resolution` in `pulsus-server`) applies the identical
/// saturating formula over the identical `(start, end, step)` (the emit
/// grid is start-anchored, so `grid_start == start`), so the engine can
/// never 422 a request the request guard admitted — across the WHOLE i64
/// timestamp domain, not just spans that fit an int64 duration. The
/// admitted point count is still hard-bounded: an unsaturated span holds
/// ≤ 11_001 points, and a saturated one forces `step > i64::MAX / 11_001`,
/// so the true `u64` span (< 2^64) holds ≤ 22_002 points — O(fence),
/// never attacker-sized. A degenerate step (zero, or wider than i64 —
/// neither passes `validate_duration_ns`) saturates [`fence_intervals`]
/// to `u64::MAX` and still rejects.
fn ensure_grid_resolution(grid_start_ns: i64, end_ns: i64, step_ns: u64) -> Result<u64, ReadError> {
    let intervals = fence_intervals(grid_start_ns, end_ns, step_ns);
    if intervals > MAX_CLIENT_AGG_BUCKETS {
        return Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets {
            buckets: intervals,
            cap: MAX_CLIENT_AGG_BUCKETS,
        }));
    }
    Ok(grid_point_count(grid_start_ns, end_ns, step_ns))
}

/// The fence's interval count, in the reference's EXACT arithmetic (issue
/// #227 review round 8): `End.Sub(Start)` is Go's `time.Time.Sub`, which
/// SATURATES an out-of-range difference at the int64-nanosecond `Duration`
/// bound (`maxDuration = 1<<63-1`) rather than widening, and the division
/// is truncating integer `Duration / Duration`. So a full-domain span at a
/// huge step counts `i64::MAX / step` intervals — under the fence — where
/// the exact i128 span would wrongly reject a request the reference
/// serves. `end < grid_start` is zero intervals (never trips, matching
/// the guard's admit-empty behaviour); a degenerate step saturates to
/// `u64::MAX` so the guard rejects by name instead of dividing by zero.
fn fence_intervals(grid_start_ns: i64, end_ns: i64, step_ns: u64) -> u64 {
    if end_ns < grid_start_ns {
        return 0;
    }
    if step_ns == 0 || step_ns > i64::MAX as u64 {
        return u64::MAX;
    }
    // Loki-exact: the span clamped to i64 (`time.Time.Sub` saturation),
    // non-negative here (`end ≥ grid_start` above).
    end_ns.saturating_sub(grid_start_ns) as u64 / step_ns
}

/// The exact-quantile retention cap: `quantile_over_time` is the one
/// reducer whose state grows with surviving rows (every value is kept
/// for the interpolation sort) rather than with `buckets x series`.
/// Past this many retained values (~32 MB of f64) the query aborts as
/// `QueryTooBroad(QuantileValues)` — complete-or-error, never OOM
/// (review round 1, finding 1's quantile bound).
pub const MAX_QUANTILE_VALUES: u64 = 4_000_000;

/// The `rate_counter` retention cap (M8-LQ3, code review rounds 2/3):
/// like `quantile_over_time`, the reset walk is order-dependent, so every
/// unwrapped `(ts, value)` sample is retained per bucket until `finish`.
/// A dense range query retains one point per scanned sample, growing the
/// combined per-bucket vectors without bound. Past this many retained
/// points the query aborts as `QueryTooBroad(CounterValues)` —
/// complete-or-error, never OOM and never a silently truncated increase.
/// Bounds only the retained points; the reset-aware rate value is
/// unchanged below the cap. Same ceiling as [`MAX_QUANTILE_VALUES`] (the
/// shared "reducer state grows with surviving rows" class).
///
/// The `push_rows` charge (`counter_values += 1`) is a TRUE bound on the
/// combined retention: [`bucket_of`] maps each scanned row to exactly one
/// step bucket and `push_rows` performs exactly one `Counter` push per
/// charged row — overlapping range windows (`step < range`) do NOT
/// re-retain a sample, since the raw scan delivers each stored sample
/// once and the buckets partition it. So `Σ retained points ==
/// counter_values` (pinned by
/// `rate_counter_cap_bounds_total_retained_points_across_overlapping_buckets`).
pub const MAX_COUNTER_VALUES: u64 = 4_000_000;

/// The reference's `querier.max-query-series` (grafana/loki v3.7.4,
/// `pkg/validation/limits.go:373`, default 500): the number of distinct
/// series a metric query may **RETURN**.
///
/// Enforced on the **FINAL result of the whole expression**
/// (`pkg/logql/engine.go:538` instant / first step, `:588` distinct
/// series accumulated across steps; frontend duplicate at
/// `pkg/querier/queryrange/limits.go:518`) — **never** on scanned groups,
/// inner-aggregation groups or binary operands. Live-probed at v3.7.4
/// over 600 distinct `| logfmt` groups: `sum(...)`, `count(...)`,
/// `topk(3, ...)`, `sum(topk(600, ...))` and a wide-operand binop
/// collapsing to one series are all served; a bare leaf over 501 groups,
/// `sort(...)` and `sum by (id) (...)` are all rejected. Exactly 500
/// served, 501 rejected.
///
/// **Read by [`ensure_result_series`] and by nothing else** (issue #236).
/// Applying it to an intermediate would reject on a *proxy* rather than
/// on the resource consumed: an outer `sum` over an inner `sum by (id)`
/// collapses 501+ inner groups to ONE final series, which the reference
/// serves. Intermediate aggregation state is bounded by BYTES
/// ([`MAX_CLIENT_AGG_GROUP_BYTES`]) and POINTS
/// ([`MAX_METRIC_RESULT_POINTS`]) — and by nothing else.
pub const MAX_QUERY_SERIES: u64 = 500;

/// Enforces [`MAX_QUERY_SERIES`] on a metric query's **final** result.
///
/// Counts TOP-LEVEL series: one per `Vector`/`VectorHist` sample and one
/// per `Matrix`/`MatrixHist` series (the reference counts distinct series,
/// not points — `engine.go:588` accumulates a set across steps, and a
/// PulsusDB matrix already holds one entry per distinct series). `Streams`
/// is a log query (bounded by `max_entries_limit_per_query` instead),
/// and `Scalar`/`String` carry no series, so all three pass.
///
/// `> cap` is the reference's own test (`engine.go:538`), so exactly 500
/// is served.
///
/// `pub` so the conformance runner (`tests/logqltest/runner.rs`) applies
/// the identical gate on its own `Expr::Metric` arm and corpus
/// `eval_fail` cases can pin the reference's body. Exporting the FUNCTION
/// keeps [`MAX_QUERY_SERIES`] read in exactly one place.
pub fn ensure_result_series(result: &QueryResult) -> Result<(), ReadError> {
    let n = match result {
        QueryResult::Vector(v) => v.len() as u64,
        QueryResult::Matrix(m) => m.len() as u64,
        QueryResult::VectorHist(v) => v.len() as u64,
        QueryResult::MatrixHist(m) => m.len() as u64,
        QueryResult::Streams { .. } | QueryResult::Scalar(_) | QueryResult::String(_) => {
            return Ok(());
        }
    };
    if n > MAX_QUERY_SERIES {
        return Err(ReadError::QueryTooBroad(TooBroadReason::MetricSeries {
            cap: MAX_QUERY_SERIES,
        }));
    }
    Ok(())
}

/// Every per-query retention cap in ONE place (issue #221), so the
/// `variants(...)` fan-out path can divide them all at a single auditable
/// point. [`AggCaps::DEFAULT`] is today's six constants verbatim;
/// [`AggCaps::divided`] is what a variants query hands each of its `n`
/// sub-states, so the SUM over sub-states is exactly `DEFAULT` for every
/// field and the query's total retention bound is INDEPENDENT of the
/// variant count. Both aggregation states carry one `caps: AggCaps` field
/// in place of the former `group_bytes_cap`/`retention_cap` fields and
/// inline constant reads — the single-extractor path constructs with
/// `DEFAULT`, so its behaviour and every error value are byte-identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AggCaps {
    pub group_bytes: u64,
    pub retention_points: u64,
    pub quantile_values: u64,
    pub counter_values: u64,
    pub collision_members: u64,
    pub collision_bytes: u64,
    /// Issue #236: fixed-width RESULT point-slots — emitted grid points
    /// and the fold's dense per-group slots — an ADMISSION counter, see
    /// [`charge_result_points`].
    pub result_points: u64,
}

impl AggCaps {
    pub(crate) const DEFAULT: AggCaps = AggCaps {
        group_bytes: MAX_CLIENT_AGG_GROUP_BYTES,
        retention_points: MAX_RETAINED_WINDOW_POINTS,
        quantile_values: MAX_QUANTILE_VALUES,
        counter_values: MAX_COUNTER_VALUES,
        collision_members: MAX_TS_COLLISION_GROUP,
        collision_bytes: MAX_TS_COLLISION_GROUP_BYTES,
        result_points: MAX_METRIC_RESULT_POINTS,
    };

    /// The per-sub-state caps for a `variants(...)` query with `n`
    /// sub-states. PLAIN integer division — a `max(1)` floor would break
    /// the sum property and let the emitted-points term scale with N; the
    /// derived backstop [`super::plan::MAX_VARIANT_SUB_STATES`] (`==
    /// min_field()`) is what keeps every divided field ≥ 1. `n` is in
    /// `1..=MAX_VARIANT_SUB_STATES` at every call site.
    pub(crate) fn divided(self, n: u64) -> AggCaps {
        AggCaps {
            group_bytes: self.group_bytes / n,
            retention_points: self.retention_points / n,
            quantile_values: self.quantile_values / n,
            counter_values: self.counter_values / n,
            collision_members: self.collision_members / n,
            collision_bytes: self.collision_bytes / n,
            result_points: self.result_points / n,
        }
    }

    /// The smallest field — the point past which `divided(n)` would floor
    /// a cap to 0. [`super::plan::MAX_VARIANT_SUB_STATES`] is DERIVED from
    /// this (not chosen), so it moves with the caps.
    pub(crate) const fn min_field(self) -> u64 {
        let mut min = self.group_bytes;
        if self.retention_points < min {
            min = self.retention_points;
        }
        if self.quantile_values < min {
            min = self.quantile_values;
        }
        if self.counter_values < min {
            min = self.counter_values;
        }
        if self.collision_members < min {
            min = self.collision_members;
        }
        if self.collision_bytes < min {
            min = self.collision_bytes;
        }
        if self.result_points < min {
            min = self.result_points;
        }
        min
    }
}

/// The instant-window witness, in a module of its own so that its field
/// is private to it (issue #236 Part D).
///
/// The nesting is the whole point and is not decoration. A bare
/// `struct InstantWindow;` is a UNIT struct: any line in `exec.rs` can
/// write `InstantWindow` and mint one — and every `ClientAggState::new`
/// call site is in `exec.rs`, so such a guard would be merely
/// *unreachable today*, not *unrepresentable*. With the field private to
/// this module, [`InstantWindow::mint`] is the only way to obtain one
/// anywhere in the crate, INCLUDING this file's own `mod tests`.
mod instant_window {
    use super::ClientWindow;

    /// A WITNESS that a [`ClientWindow`] was narrowed to its instant
    /// case.
    ///
    /// `ClientAggState`'s per-group accumulator is a SINGLE `BucketAcc`,
    /// which is sound only because an instant window has exactly one
    /// bucket. Requiring this witness at the constructor keeps that true
    /// without a runtime check nobody re-reads — and it was already true
    /// at every call site, so nothing is newly forbidden.
    ///
    /// It carries NO bounds, a deliberate difference from plan v14's
    /// "explicit instant bounds": nothing in an instant state reads
    /// them. The two former readers — the stepped-grid guard in the
    /// constructor and the `bucket_grid` walk in `finish` — are both
    /// deleted by Part D, and a field that exists only to look used is
    /// worse than none. The witness is also the stronger of the two
    /// shapes: a pair of `i64` bounds can be hand-derived from a stepped
    /// window, an unforgeable witness cannot.
    #[derive(Debug, Clone, Copy)]
    pub(super) struct InstantWindow(());

    impl InstantWindow {
        /// The ONE mint. Refuses a stepped window, so a caller has to say
        /// what it does with one instead of assuming it cannot get one.
        pub(super) fn mint(window: ClientWindow) -> Option<Self> {
            match window {
                ClientWindow::Instant { .. } => Some(InstantWindow(())),
                ClientWindow::Range { .. } => None,
            }
        }
    }
}

use instant_window::InstantWindow;

impl ClientWindow {
    /// Narrows to the INSTANT case, or `None` for a stepped window.
    fn as_instant(self) -> Option<InstantWindow> {
        InstantWindow::mint(self)
    }
}

/// Streaming client-aggregation state (issue M6-10, review round 1
/// finding 1): rows fold into reducer state as they arrive so process
/// memory stays `O(series)` (+ the caller's bounded chunk) instead of
/// retaining the whole raw scan. The pure [`run_client_agg_rows`]
/// wrapper drives it over a slice for the hermetic golden/allocation
/// suites; the engine drives it chunk-wise off the live row stream.
///
/// **Instant-only by construction** (issue #236 Part D). It always was —
/// `run_metric_client` and `run_client_agg_rows` route every stepped
/// window to [`RangeSlideState`], and `bucket_of` returned the single
/// [`INSTANT_BUCKET`] with no step — so the per-group
/// `BTreeMap<i64, BucketAcc>` always held exactly one entry, and the map
/// (a ~1 KB/group allocation `group_entry_bytes` never saw: an #227
/// accounting hole) was pure overhead. The constructor now takes an
/// [`InstantWindow`], so the dead stepped branches are not merely unused
/// but unrepresentable.
#[derive(Debug)]
struct ClientAggState<'q> {
    compiled: &'q super::pipeline::CompiledPipeline,
    client: &'q ClientAgg,
    rate_window_ns: Option<u64>,
    /// Base labels once per fingerprint, in the same shape the SQL
    /// metric path exposes (`series_labels`: canonical JSON labels +
    /// the physical `service` column re-injected as `service_name`,
    /// sorted).
    base_labels: HashMap<u64, Vec<(String, String)>>,
    fan_out: bool,
    /// `absent_over_time`'s selector-wide presence (plan v2 D2). A FLAG,
    /// not a bucket set, since issue #236 Part D: an instant window has
    /// exactly one bucket, so the set could only ever hold
    /// `{INSTANT_BUCKET}` and the only question ever asked of it was
    /// whether it was empty.
    present: bool,
    /// Non-mutating pipelines group by fingerprint (zero per-row
    /// allocations — the alloc-gate path). ONE accumulator per group,
    /// not a bucket map — see [`ClientAggState`]'s instant-only contract.
    fp_groups: HashMap<u64, BucketAcc>,
    /// Label-mutating/unwrapping pipelines group by the rendered final
    /// label set.
    label_groups: HashMap<String, (LabelSet, BucketAcc)>,
    /// Total values retained across every quantile accumulator, charged
    /// against [`MAX_QUANTILE_VALUES`].
    quantile_values: u64,
    /// Total timestamped points retained across every `rate_counter`
    /// accumulator, charged against [`MAX_COUNTER_VALUES`].
    counter_values: u64,
    /// QUERY-LIFETIME bytes retained by `label_groups` — each distinct
    /// group's rendered key + cloned `LabelSet` + map-slot share. The same
    /// round-6 charge as the sliding path's `groups`, through the same
    /// [`charge_group_bytes`]/[`group_entry_bytes`] helpers: the group
    /// COUNT cap alone left per-group label BYTES unbounded here too.
    group_bytes: u64,
    /// Every retention cap this state checks (issue #221) — always
    /// [`AggCaps::DEFAULT`] in production single-extractor use (test seam,
    /// the former `group_bytes_cap` precedent); a `variants(...)` query
    /// hands each sub-state `AggCaps::DEFAULT.divided(n)`.
    caps: AggCaps,
}

impl<'q> ClientAggState<'q> {
    /// Snapshots the per-fingerprint base labels.
    ///
    /// The former stepped-grid guard is gone with the [`InstantWindow`]
    /// parameter (issue #236 Part D): it existed to make a stepped window
    /// that reached here obey the Loki-exact resolution rule, and a
    /// stepped window can no longer reach here. `Result` is kept because
    /// the constructor is one of the fallible-by-contract seams the
    /// variants fan-out charges through.
    fn new(
        compiled: &'q super::pipeline::CompiledPipeline,
        meta: &HashMap<u64, StreamMetaRow>,
        client: &'q ClientAgg,
        _window: InstantWindow,
        rate_window_ns: Option<u64>,
        caps: AggCaps,
    ) -> Result<Self, ReadError> {
        let mut base_labels: HashMap<u64, Vec<(String, String)>> = HashMap::new();
        for (fp, m) in meta {
            base_labels.insert(*fp, series_labels(m));
        }
        Ok(ClientAggState {
            compiled,
            client,
            rate_window_ns,
            base_labels,
            fan_out: compiled.metric_mutates_labels(),
            present: false,
            fp_groups: HashMap::new(),
            label_groups: HashMap::new(),
            quantile_values: 0,
            counter_values: 0,
            group_bytes: 0,
            caps,
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
            let (line, value) = match self.compiled.run_metric_into(
                &row.body,
                base,
                row.timestamp_ns,
                &mut scratch,
            )? {
                MetricRun::Dropped => continue,
                MetricRun::Kept { line, value } => (line, value),
            };
            check_surviving_error(&scratch)?;
            if is_absent {
                // Selector-wide presence (plan v2 D2): did ANY line
                // survive, across every fingerprint and label set. One
                // bucket, so one flag (issue #236 Part D).
                self.present = true;
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
                if self.quantile_values > self.caps.quantile_values {
                    return Err(ReadError::QueryTooBroad(TooBroadReason::QuantileValues {
                        count: self.quantile_values,
                        cap: self.caps.quantile_values,
                    }));
                }
            }
            if matches!(op, RangeAggOp::RateCounter) {
                // One charge per scanned row IS one charge per retained
                // `Counter` point: `bucket_of` below yields exactly one
                // bucket and the row is pushed into it exactly once, so
                // `counter_values` equals the combined length of every
                // `Counter` vector — a true bound even when `step < range`
                // makes the reference's windows overlap (the raw scan
                // delivers each stored sample once; buckets partition it,
                // never re-retain). See `MAX_COUNTER_VALUES`.
                self.counter_values += 1;
                if self.counter_values > self.caps.counter_values {
                    return Err(ReadError::QueryTooBroad(TooBroadReason::CounterValues {
                        count: self.counter_values,
                        cap: self.caps.counter_values,
                    }));
                }
            }
            // ONE accumulator per group (issue #236 Part D): an instant
            // window has a single bucket, so each arm either seeds the
            // group's accumulator or folds into it — there is no bucket
            // map to index.
            if self.fan_out {
                scratch.sort_unstable();
                let key = render_labels_json_sorted(&scratch);
                match self.label_groups.entry(key) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        e.into_mut().1.add(row.timestamp_ns, v);
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let labels: LabelSet = scratch
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();
                        // Issue #227 review round 6 (same class as the
                        // sliding path): the key + `LabelSet` live in
                        // `label_groups` for the whole query — charged
                        // BEFORE the insert retains them; refused means the
                        // per-row transients above drop with the error and
                        // the map never holds the entry. (`entry()` has
                        // already reserved the table slot — that growth is
                        // covered by this charge's slot term, which since
                        // issue #236 deleted the group-COUNT cap is the
                        // whole bound on this map: bytes, never a count.)
                        charge_group_bytes(
                            &mut self.group_bytes,
                            group_entry_bytes(e.key(), &labels, INSTANT_GROUP_SLOT),
                            self.caps.group_bytes,
                        )?;
                        e.insert((labels, BucketAcc::new(op, row.timestamp_ns, v)));
                    }
                }
            } else {
                // Issue #236 P1 — the premise fix. Before #236 this arm was
                // count-gated only and its per-group BYTES were never
                // charged: `base_labels` is hydrated with no charge, so
                // deleting the count cap (Part A) would have left the
                // non-mutating instant path with no bound at all. The
                // charge rides the EXISTING `contains_key` probe (no added
                // per-row work) and happens BEFORE the entry is created, so
                // a refusal never leaves the map holding it.
                if !self.fp_groups.contains_key(&row.fingerprint) {
                    charge_group_bytes(
                        &mut self.group_bytes,
                        group_entry_bytes("", base, FP_GROUP_SLOT),
                        self.caps.group_bytes,
                    )?;
                }
                match self.fp_groups.entry(row.fingerprint) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        e.get_mut().add(row.timestamp_ns, v);
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(BucketAcc::new(op, row.timestamp_ns, v));
                    }
                }
            }
        }
        Ok(())
    }

    /// Finishes every accumulator into the metric result.
    /// `absent_over_time` emits at most ONE sample; the other reducers
    /// emit one per surviving group.
    ///
    /// Always a `Vector` (issue #236 Part D): the state is instant-only by
    /// construction, so the former stepped arms — the `bucket_grid` walk
    /// for absence and the per-bucket `Matrix` emit — were dead, and are
    /// deleted rather than left as a second reading of a contract the type
    /// already states.
    fn finish(self) -> QueryResult {
        if matches!(self.client.range_op, RangeAggOp::AbsentOverTime) {
            let labels: LabelSet = self.client.absent_labels.clone();
            return if self.present {
                QueryResult::Vector(Vec::new())
            } else {
                QueryResult::Vector(vec![VectorSample { labels, value: 1.0 }])
            };
        }

        let base_labels = self.base_labels;
        let mut group_bytes = self.group_bytes;
        let groups: Vec<(LabelSet, BucketAcc)> = if self.fan_out {
            self.label_groups
                .into_iter()
                .map(|(key, (labels, acc))| {
                    // Round 6 symmetry: sized over the same unmodified
                    // key/labels as the charge, released as each entry is
                    // consumed (key dropped, labels move to the output).
                    discharge_group_bytes(
                        &mut group_bytes,
                        group_entry_bytes(&key, &labels, INSTANT_GROUP_SLOT),
                    );
                    (labels, acc)
                })
                .collect()
        } else {
            self.fp_groups
                .into_iter()
                .filter_map(|(fp, acc)| {
                    // Issue #236 P1's discharge leg, with the SAME symmetry
                    // the fan-out arm has had since round 6: sized over the
                    // same `base_labels` value the charge used (empty key,
                    // `FP_GROUP_SLOT`), released as the entry is consumed.
                    // `push_rows` only charges a fingerprint it successfully
                    // resolved in `base_labels`, so every charged entry is
                    // discharged here and `group_bytes` returns to exactly 0
                    // — the `filter_map` below can only drop an entry that
                    // was never charged.
                    let l = base_labels.get(&fp)?;
                    discharge_group_bytes(
                        &mut group_bytes,
                        group_entry_bytes("", l, FP_GROUP_SLOT),
                    );
                    Some((l.clone(), acc))
                })
                .collect()
        };
        debug_assert_eq!(
            group_bytes, 0,
            "every group-label byte charge must be discharged at finish"
        );
        let op = self.client.range_op;
        let rate_window_ns = self.rate_window_ns;
        let param = self.client.param;
        QueryResult::Vector(
            groups
                .into_iter()
                .map(|(labels, acc)| VectorSample {
                    labels,
                    value: acc.finish(op, rate_window_ns, param),
                })
                .collect(),
        )
    }
}

/// Materializes `vector(<scalar>)` (issue #221): promotes a constant to a
/// vector/matrix result with an empty label set. Instant queries yield a
/// single `{} => value` sample; range queries yield one constant `{}`
/// series populated at every evaluation-grid step.
///
/// A leafless `vector(n)` bypasses [`RangeSlideState::new`]'s resolution
/// guard (no leaf ever runs), so this checks the same Loki-exact rule via
/// [`ensure_grid_resolution`] BEFORE materializing any grid — an over-cap
/// `vector(n)` returns the identical `QueryTooBroad(MetricBuckets)`
/// 422 as every other over-cap LogQL range query, with zero allocation and
/// no DB round-trip. The grid itself REUSES [`bucket_grid`] (the same
/// i128-safe, `clamp_bucket`-narrowed, start-anchored grid a leaf
/// range query and `absent_over_time` use), so `points.len()` equals the
/// guard's admitted point count (≤ 11 001)
/// and a `data + vector(n)` binop aligns on the data's populated steps.
pub fn materialize_vector_lit(value: f64, window: &GridWindow) -> Result<QueryResult, ReadError> {
    match window.step_ns.map(|d| d.as_u64()) {
        None => Ok(QueryResult::Vector(vec![VectorSample {
            labels: vec![],
            value,
        }])),
        Some(step) => {
            ensure_grid_resolution(window.start_ns, window.end_ns, step)?;
            let points = bucket_grid(window.start_ns, window.end_ns, step)
                .into_iter()
                .map(|ts| (ts, value))
                .collect();
            Ok(QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![],
                points,
            }]))
        }
    }
}

// =====================================================================
// Issue #227: Loki sliding-window range evaluation.
//
// A range query re-evaluates the `[range]` window `(t-range, t]` at every
// start-anchored grid point `t ∈ {grid_start + k·step ≤ end}` (Loki's
// `batchRangeVectorIterator`). Rows arrive in physical-key order
// `(service, fingerprint, timestamp_ns)`, so a fingerprint's samples are
// contiguous and ascending, and same-`(fingerprint, timestamp_ns)`
// collision groups arrive consecutively.
//
// Two grouping shapes:
//  - **non-mutating** (output series == fingerprint): a true streaming
//    slide per fingerprint — interleave grid emission with the ascending
//    stream, evicting `T ≤ t-range` and loading `T ≤ t`, so peak memory is
//    one window's contents. The concurrent-retention cap trips here
//    (charge-on-load / discharge-on-evict).
//  - **mutating/regrouping** (a `label_format`/parser merges fingerprints
//    into one output set): fan each sample into every covering grid cell,
//    retaining fixed-width `(ts, stream_hash, tie_rank, value)` points, then
//    reduce each cell — class-C folds in the canonical `(ts, stream_hash,
//    tie_rank)` order, class-B order-independently.
//
// Reducer classes (finding-4 table): A invert-integer, B re-reduce
// order-independent, C re-reduce canonical-fold. See [`reducer_class`].
// =====================================================================

/// Loki `labels.StableHash` (v3.7.4 `model/labels/sharding.go:24`): the
/// same-timestamp cross-stream tie key. `xxhash64` (seed 0) over each label
/// **in sorted-by-name order** rendered `name·0xFF·value·0xFF` (`sep =
/// '\xff'`, `labels_common.go:38`). Build-tag-independent — byte-identical
/// to Loki. `sorted_labels` is the stream's base label set (as
/// [`series_labels`] renders it: canonical labels + the `service_name`
/// column), already sorted by name.
fn stream_hash(sorted_labels: &[(String, String)]) -> u64 {
    let mut buf: Vec<u8> = Vec::new();
    for (name, value) in sorted_labels {
        buf.extend_from_slice(name.as_bytes());
        buf.push(0xFF);
        buf.extend_from_slice(value.as_bytes());
        buf.push(0xFF);
    }
    xxhash_rust::xxh64::xxh64(&buf, 0)
}

/// Cap on the transient full-body buffer that ranks one consecutive
/// same-`(fingerprint, timestamp_ns)` collision group (issue #227). A
/// same-nanosecond, same-stream run larger than this is
/// pathological/adversarial, never real data; exceeding it is a clean
/// `QueryTooBroad(TsCollisionGroup)`, never an OOM. A documented constant
/// (the `DEFAULT_MAX_STREAMS` precedent).
pub const MAX_TS_COLLISION_GROUP: u64 = 10_000;

/// Byte ceiling on the staged same-`(fingerprint, timestamp_ns)` collision
/// group (issue #227 review rounds 3 + 4, finding 1).
///
/// The member-COUNT cap alone is not a memory bound: 10 000 multi-megabyte
/// members are terabytes of RAM while staying inside the byte-scan budget.
///
/// > **Bound proof (re-derived for EVERY reducer class, round 4).**
/// > 1. [`RangeSlideState::stage_member`] is the ONLY place a per-member
/// >    allocation is pushed into `coll` — no reducer class has another
/// >    path, and it is called exactly once per surviving row.
/// > 2. It charges [`RangeSlideState::member_stage_bytes`] BEFORE allocating.
/// >    That figure is an UPPER BOUND covering **every** per-member
/// >    allocation: the body clone (class C + first/last), the rendered
/// >    label JSON — sized EXACTLY by [`rendered_labels_json_len`], so the
/// >    `\u00xx` six-for-one escape expansion is charged — and the cloned
/// >    `LabelSet` with each of its owned strings (any label-mutating
/// >    pipeline; charged for ALL classes, including the integer class A that
/// >    stages no body), each through [`alloc_block_bytes`] /
/// >    [`grown_alloc_bytes`] so container growth and the allocator's
/// >    size-class rounding (round 7) are covered. Classes that allocate
/// >    nothing extra charge only the `CollMember` slot.
/// > 3. The staging is refused unless
/// >    `coll_bytes + charge <= MAX_TS_COLLISION_GROUP_BYTES`, so the
/// >    breaching allocation is never performed; on success `coll_bytes`
/// >    grows by exactly the charge.
/// > 4. `flush_collision` empties `coll` and resets `coll_bytes` together at
/// >    every `(fingerprint, timestamp_ns)` boundary, so exactly one group is
/// >    staged at a time and the counter can neither leak nor double-count
/// >    across groups (including when a group straddles a fetch chunk: the
/// >    straddling group is simply never flushed between chunks, so its
/// >    charge accumulates continuously in the same counter).
/// >
/// > Therefore `actual staged heap bytes <= coll_bytes <=
/// > MAX_TS_COLLISION_GROUP_BYTES` **at all times, for every reducer class,
/// > any body/label size and any density**. A breach is a clean
/// > `TsCollisionGroup` error — never a truncated group, so the `tie_rank`
/// > order is never silently partial.
/// >
/// > **What survives a flush.** `Vec::clear` DROPS every element (bodies,
/// > rendered JSON and cloned label sets are freed) but keeps `coll`'s own
/// > element buffer. That spare capacity is bounded by the largest group
/// > ever staged, whose slot charge (`VEC_GROWTH_FACTOR ×
/// > size_of::<CollMember>()` per member) was itself inside the cap — so
/// > the residual buffer is `<= MAX_TS_COLLISION_GROUP_BYTES` too, and no
/// > content bytes survive.
///
/// 8 MiB staged for one nanosecond in one stream is far beyond real data
/// (real groups are size 1), so nothing servable is rejected.
pub const MAX_TS_COLLISION_GROUP_BYTES: u64 = 8 * 1024 * 1024;

/// The floor one owned heap allocation costs however short its payload is:
/// `RawVec`'s `min_non_zero_cap` is 8 for byte-sized elements, every
/// mainstream allocator has a smallest size class / minimum chunk of ≤ 32
/// bytes, and glibc's minimum chunk is exactly 32 (issue #227 review
/// round 5, finding 1). The floor keeps [`alloc_block_bytes`] a bound
/// independent of whichever `ToString`/`clone` capacity specialization the
/// standard library happens to use.
const MIN_ALLOC_BYTES: u64 = 32;

/// A conservative, provable UPPER BOUND on the heap block a real allocator
/// RETAINS for one **exactly reserved** allocation of `content` payload
/// bytes (`String::clone`, `str::to_owned`, `collect` from a `TrustedLen`
/// iterator — all reserve exactly the final length in ONE allocation).
///
/// Issue #227 review round 7, finding 2: charging the request size itself
/// (`content.max(32)`) was NOT an upper bound — allocators round a request
/// up to a size class, so a 33-byte string can retain a 48- or 64-byte
/// block and adversarial label strings undercounted staging and
/// query-lifetime group memory by a constant factor. The model here is a
/// documented over-approximation, `2·content` floored at
/// [`MIN_ALLOC_BYTES`], NOT any specific allocator's class table. Why
/// `2·content` dominates every mainstream allocator's retained block
/// (header/metadata included) for a `content`-byte request:
///
/// - **Size-class allocators with out-of-band metadata** (jemalloc,
///   mimalloc, tcmalloc, snmalloc): no mainstream class grid is coarser
///   than powers of two, so `class(n) ≤ next_pow2(n) ≤ 2n` for every
///   `n ≥ 1`, and the smallest classes sit under the 32-byte floor.
/// - **Inline-header, 16-byte-binned allocators** (glibc malloc): chunk =
///   `align16(n + 8)` with a 32-byte minimum — `≤ n + 23 ≤ 2n` for
///   `n ≥ 23`, and `≤ 32` (the floor) for `n ≤ 23`.
/// - **Page-rounded huge allocations** (mmap past the ~128 KiB
///   threshold): `n + header + 4 KiB page slack ≤ 2n` at those sizes.
///
/// Over-charging is the safe direction (a breach is a clean 422, never an
/// OOM); the factor halves no real workload's headroom — the caps'
/// margins are orders of magnitude above real label/body sizes.
pub(crate) const fn alloc_block_bytes(content: u64) -> u64 {
    // `const`-compatible spelling of `content.saturating_mul(2)
    // .max(MIN_ALLOC_BYTES)` (`Ord::max` is not const): needed so
    // [`MAX_FEEDER_SCRATCH_BYTES`] derives from the SAME rounding model at
    // compile time (issue #244) — one scheme, no const twin to drift.
    let doubled = content.saturating_mul(2);
    if doubled > MIN_ALLOC_BYTES {
        doubled
    } else {
        MIN_ALLOC_BYTES
    }
}

/// An upper bound on the heap bytes a **geometrically grown** buffer of
/// `content` final payload bytes occupies at its peak, allocator rounding
/// included. `RawVec::grow_amortized` sets `cap = max(2·cap_old,
/// required)`, and `cap_old < required <= content` at the last growth, so
/// at the realloc peak two blocks are live: the new buffer (request
/// `≤ 2·content`, retained `≤ alloc_block_bytes(2·content) ≤
/// 2·alloc_block_bytes(content)`) and the old buffer (request
/// `< content`, retained `≤ alloc_block_bytes(content)`) — `3·
/// alloc_block_bytes(content)` dominates the sum for every input (review
/// round 7, finding 2: the previous `3·content` covered the request
/// bytes but not the allocator's per-block rounding).
pub(crate) fn grown_alloc_bytes(content: u64) -> u64 {
    alloc_block_bytes(content).saturating_mul(3)
}

/// The exact number of bytes [`push_json_string`] appends for `s` — the two
/// quotes plus the escaped body — computed WITHOUT allocating so the
/// collision-group charge can size the rendered JSON *before* it is built
/// (issue #227 review round 5, finding 1: charging the content once
/// under-counted the `\u00xx` six-for-one expansion of a C0 control byte).
/// [`rendered_labels_json_len_matches_the_renderer_byte_for_byte`] pins this
/// against the renderer itself, so the two cannot drift.
fn json_string_len(s: &str) -> u64 {
    let mut n: u64 = 2; // the enclosing quotes
    for c in s.chars() {
        n = n.saturating_add(match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{08}' | '\u{0C}' => 2,
            c if (c as u32) < 0x20 => 6, // `\u00xx`
            c => c.len_utf8() as u64,
        });
    }
    n
}

/// The exact byte length [`render_labels_json_sorted`] will produce for
/// `sorted_labels` (`{"k":"v",...}`), computed without allocating.
fn rendered_labels_json_len(sorted_labels: &[(Cow<'_, str>, Cow<'_, str>)]) -> u64 {
    let mut n: u64 = 2; // `{` and `}`
    for (i, (k, v)) in sorted_labels.iter().enumerate() {
        if i > 0 {
            n = n.saturating_add(1); // `,`
        }
        n = n
            .saturating_add(json_string_len(k))
            .saturating_add(1) // `:`
            .saturating_add(json_string_len(v));
    }
    n
}

/// The sliding-window **concurrent** retained-point cap (issue #227):
/// charge-on-load / discharge-on-evict across every retained window entry
/// (non-mutating deque + mutating cells). The invariant `retained ≤ cap`
/// holds at all times, so process memory is bounded to ≈ `cap × entry
/// width` regardless of `[range]`/step/density — the charge is per-load, so
/// it trips as the FIRST oversized window fills, DURING the scan, before
/// RSS grows. Generalizes the instant path's per-reducer
/// [`MAX_QUANTILE_VALUES`]/[`MAX_COUNTER_VALUES`] total-retention proofs
/// into one concurrent invariant. Same 4M magnitude, set generously so
/// nothing Loki reliably serves is rejected (see the cap-generosity corpus
/// case).
///
/// **Unit and byte relationship.** One point is one [`WinSample`]. Actual
/// process bytes are `retained × size_of::<WinSample>()` scaled by the
/// container's allocator growth slack (a `VecDeque`/`Vec` holds up to ~2×
/// its length, plus the old buffer during a realloc) — i.e. `O(cap)`, with
/// no axis that scales with `[range]`, step, cardinality or density. Any
/// per-emit scratch a reducer needs on top of the window is charged
/// per-sample up front by [`retention_points_per_sample`], so it is inside
/// the same bound.
pub const MAX_RETAINED_WINDOW_POINTS: u64 = 4_000_000;

/// **The one retention gate.** Every sliding-path allocation that scales
/// with scanned data is sized in retention POINTS, checked against the cap,
/// and accounted HERE — *before* the allocation is performed (issue #227
/// review round 5, finding 2: the charge sites used to be scattered, and
/// three of them charged AFTER inserting, so the breaching allocation was
/// made before the cap could refuse it). Every caller on the sliding path
/// funnels through this function and [`discharge_retention`], so the
/// "size → check the cap → allocate" rule has exactly one implementation.
///
/// `saturating_add` cannot mask a breach: saturation only ever makes the
/// sum LARGER, so the comparison still rejects.
fn charge_retention(retained: &mut u64, points: u64, cap: u64) -> Result<(), ReadError> {
    let next = retained.saturating_add(points);
    if next > cap {
        return Err(ReadError::QueryTooBroad(TooBroadReason::MetricRetention {
            count: next,
            cap,
        }));
    }
    *retained = next;
    Ok(())
}

/// Releases a charge made by [`charge_retention`], called as the charged
/// memory is freed (eviction, series close, cell consumption). Saturating
/// for panic-proofing only — the charge/discharge pairing is exact, which
/// `finish`'s `retained == 0` post-condition asserts.
fn discharge_retention(retained: &mut u64, points: u64) {
    *retained = retained.saturating_sub(points);
}

/// Retention points charged per retained sample, in [`WinSample`] units.
///
/// Most reducers re-reduce over a BORROW of the live window and allocate
/// nothing per emit, so one retained sample costs exactly one point.
/// `quantile_over_time` is the sole exception: its reduction must SORT the
/// values, and it cannot sort the window itself (the deque's ascending-`ts`
/// order is what eviction depends on), so it materializes a `Vec<f64>` copy
/// of the LIVE window at every grid point.
///
/// That copy is charged up front, at load, rather than at the emit that
/// makes it — which is what keeps the rule "the cap approves an allocation
/// before it happens" true for a copy taken deep inside a non-fallible
/// reduce. **Bound:** the copy is `8 × W` bytes for a live window of `W`
/// samples (`collect` from a `TrustedLen` iterator reserves exactly `W`),
/// while the extra point charges `size_of::<WinSample>()` = 32 bytes per
/// sample — 4× headroom, so the charge dominates the copy for every window
/// size and every allocator slack.
fn retention_points_per_sample(op: RangeAggOp) -> u64 {
    match op {
        RangeAggOp::QuantileOverTime => 2,
        _ => 1,
    }
}

/// The **query-lifetime** byte cap on the label-mutating client-aggregation
/// path's distinct output-group state (issue #227 review round 6): each
/// first-seen group MOVES its rendered JSON key and cloned final `LabelSet`
/// into the group map, where they live until finish — bytes charged against
/// NEITHER the collision-group cap (whose counter resets when the group
/// flushes) nor the retention cap (denominated in fixed-width points). The
/// group COUNT used to be bounded too, but multi-MiB extracted label sets
/// were a real memory hazard at any count; this cap bounds their BYTES,
/// charged through [`charge_group_bytes`] BEFORE the insertion that
/// retains each entry and released as finish consumes it.
///
/// **Since issue #236 this is the ONLY bound on the group axis.** The
/// mid-scan group-count cap is deleted (it rejected queries the reference
/// serves — see [`MAX_QUERY_SERIES`]), so every row of the table below
/// that used to lean on a count now leans on bytes or on the grid.
/// Raised 64 MiB → 256 MiB with that deletion (owner ruling O1): the
/// count cap used to keep the group axis small, and the byte cap now
/// carries the whole load.
///
/// **The query-lifetime container audit** (round 6 — earlier sweeps covered
/// the per-sample/per-window dimension; this is the per-QUERY one). Every
/// container that lives for the whole evaluation, and what bounds its bytes:
///
/// | container | bytes bounded by |
/// |---|---|
/// | `RangeSlideState::groups` keys + `MutGroup::labels` | **charged here** (before insertion; released as `finish_in_place` consumes each entry) |
/// | `ClientAggState::label_groups` keys + `LabelSet`s | **charged here** (same helpers, same discipline) |
/// | `MutGroup::int_cells`/`pt_cells`, `FpSlide::win`, `BucketAcc` retention | [`MAX_RETAINED_WINDOW_POINTS`] / [`MAX_QUANTILE_VALUES`] / [`MAX_COUNTER_VALUES`], charge-before-insert |
/// | `base_labels` / `hashes` (both states) | built once from the stage-2 hydration read, which is scan-budget-capped (`max_bytes_to_read`) and stream-capped (`max_streams`); never grown per row |
/// | `ClientAggState::fp_groups` (non-mutating instant) | **charged here** since issue #236 P1 ([`FP_GROUP_SLOT`]), inside the existing `contains_key` probe — before #236 this arm was count-gated only and its bytes were never charged |
/// | non-mutating output labels (`FpSlide::labels`, `series_out`) | **charged here** since issue #236 P2 ([`SERIES_OUT_SLOT`]), before the `labels` clone; discharged at the slider's no-points drop or as `finish_in_place` releases the vector |
/// | emitted points (`FpSlide::points`, `series_out`, fan-out `points`) | fixed-width `(i64, f64)`, charged against [`MAX_METRIC_RESULT_POINTS`] — the former `series × grid` product leaned on the deleted count cap |
/// | `present_cover` / `present` | grid-sized (≤ [`MAX_CLIENT_AGG_BUCKETS`] + 2 entries) |
/// | `absent_labels` | cloned from the parsed query text — bounded by request size |
/// | `coll` staging | NOT query-lifetime: one group at a time, ≤ [`MAX_TS_COLLISION_GROUP_BYTES`] |
/// | `approx_topk` sketch + retention (`cms::CountMinSketch`, `cms::Retention`) | compile-time constants — the 13-row table on `approx_topk_instant`, peak ≤ 7 360 882 B, no input-scaled term (issue #221) |
/// | variants: `VariantArena::{pipelines,slot}` + `VariantsAggState::{subs,sub_charged}` buffers | **charged** as ONE [`variant_driver_buffer_bytes`] term before the first is allocated (N ≥ 2) |
/// | variants: each arena entry (a distinct non-empty unwrap tail) | **charged** ([`variant_pipeline_entry_bytes`]) before `extended_with`; the regex PROGRAMS are `Arc`-shared, only cache pools are new |
/// | variants: each extra sub-state's boxed slot / `base_labels`(+`hashes`) snapshot (C table share + H payload) / range `absent_labels` clone / absent `present_cover` | **charged** ([`variant_state_bytes`]) before construction; sub-state 0 charges 0 (a 1-variant query is admitted exactly when the plain query is) |
/// | variants: `MetricNode::Variants::variants` vec + each `VariantSpec`'s tail/absent/grouping clones (incl. the CREATED `by (__variant__)` grouping and the pushed-into `Grouping::labels` realloc) | **charged at PLAN time** ([`variant_spec_bytes`] + the spec-vector buffer) into the SAME counter the arena continues (`spec_bytes`) — one budget, never two |
/// | variants: per-sub-state growing state (`groups`/retention/collision/quantile/counter) | [`AggCaps::divided`]`(n)` — the per-field SUM over sub-states is exactly the single-query bound above |
/// | post-aggregation selection/grouping keys (`select_k_*`/`group_*`'s `HashMap<LabelSet, _>` owned `group_key` copies) | **flagged, not yet charged** (issue #241): bounded only indirectly by the upstream hydration/series caps; `approx_topk` itself is exempt structurally (`grouping == None` ⇒ a single empty key) |
///
/// **Value: 256 MiB, raised from 64 MiB by issue #236 (owner ruling O1).**
/// With the group-COUNT cap deleted this became the only bound on the
/// group axis, so it had to admit the high-cardinality shapes #236 exists
/// to serve. At the stated 6-pair label model that is ≈ 80 321 admitted
/// groups — 3.4× the pre-#236 figure and comfortably above the issue's
/// reported 20 505-group probe shape. The residual, stated rather than
/// hidden: above ≈ 80 321 groups PulsusDB still refuses where the
/// reference serves. That is a bounded divergence, not a fix — the real
/// fix is a step-ordered evaluator (#250). O2 (1 GiB) was rejected
/// because per-query ceilings do not compose across concurrent queries;
/// O4 (operator-configurable) routes to #25.
pub const MAX_CLIENT_AGG_GROUP_BYTES: u64 = 256 * 1024 * 1024;

/// The exact upper bound [`ensure_grid_resolution`] can ADMIT, in grid
/// POINTS. The fence saturates ([`fence_intervals`]) while
/// [`grid_point_count`] is exact over i128, so a full-domain span at a
/// step just wide enough to pass the fence admits
/// `2 * (MAX_CLIENT_AGG_BUCKETS + 1)` points — `ensure_grid_resolution`'s
/// own doc says so. DERIVED from the fence, never chosen, and it enforces
/// nothing on its own: the gate is [`MAX_CLIENT_AGG_BUCKETS`] intervals,
/// which is the reference's rule.
pub const MAX_ADMITTED_GRID_POINTS: u64 = 2 * (MAX_CLIENT_AGG_BUCKETS + 1); // 22_002

/// The cap on emitted points, fold cells and fold slots — one counter
/// each, charged before allocation (issue #236).
///
/// DERIVED, not chosen: a result the reference will serve carries at most
/// [`MAX_QUERY_SERIES`] series, and each holds at most
/// [`MAX_ADMITTED_GRID_POINTS`] points, so every counter's provable
/// maximum for a servable result is `500 × 22 002 = 11 001 000`. The
/// shipped value is the next round figure above it, so no servable result
/// is ever refused by this cap.
///
/// It replaces the structural `series × grid` product that the deleted
/// group-count cap used to supply. Unlike that product it does not reject
/// on a group COUNT, so a wide scan collapsing to a narrow result passes
/// it untouched.
pub const MAX_METRIC_RESULT_POINTS: u64 = 12_000_000;

/// **The one group-byte gate** (round 6): every query-lifetime group-map
/// insertion is sized by [`group_entry_bytes`], checked against the cap, and
/// accounted HERE — *before* the insertion that retains the entry, so the
/// map never holds a refused group. `saturating_add` cannot mask a breach
/// (saturation only grows the sum).
fn charge_group_bytes(charged: &mut u64, bytes: u64, cap: u64) -> Result<(), ReadError> {
    let next = charged.saturating_add(bytes);
    if next > cap {
        return Err(ReadError::QueryTooBroad(
            TooBroadReason::MetricGroupLabelBytes { bytes: next, cap },
        ));
    }
    *charged = next;
    Ok(())
}

/// **The one result-point gate** (issue #236): every fixed-width point
/// slot a metric evaluation will RETAIN is reserved here, against
/// [`MAX_METRIC_RESULT_POINTS`], *before* the allocation that holds it.
///
/// An ADMISSION counter, not a concurrent-retention one — there is no
/// discharge, because the slots it reserves hold the RESULT and live
/// until the result is returned. `charge_retention`'s counters return to
/// zero at finish; this one asserts a charge IDENTITY instead (the tests
/// pin `charged == series x (kmax + 1)`).
///
/// **Charged `O(1)` per output series and per fold group, never per
/// point.** Each reservation is the grid's full width (`kmax + 1`), which
/// is both an exact upper bound on what one series can emit and the
/// model [`MAX_METRIC_RESULT_POINTS`] is derived from
/// (`MAX_QUERY_SERIES x MAX_ADMITTED_GRID_POINTS`) — so the gate and the
/// constant speak the same units, and the read path gains no per-point
/// work.
///
/// `saturating_add` cannot mask a breach (saturation only grows the sum)
/// but keeps a pathological reservation from wrapping the comparison.
fn charge_result_points(charged: &mut u64, points: u64, cap: u64) -> Result<(), ReadError> {
    let next = charged.saturating_add(points);
    if next > cap {
        return Err(ReadError::QueryTooBroad(
            TooBroadReason::MetricResultPoints { count: next, cap },
        ));
    }
    *charged = next;
    Ok(())
}

/// Releases a [`charge_group_bytes`] charge as finish consumes the entry it
/// paid for. Saturating for panic-proofing only — the pairing is exact
/// (charge and discharge run [`group_entry_bytes`] over the SAME unmodified
/// key/labels), which the finish `group_bytes == 0` post-conditions assert.
fn discharge_group_bytes(charged: &mut u64, bytes: u64) {
    *charged = charged.saturating_sub(bytes);
}

/// The map-entry slot the sliding path's group charge sizes (`groups`).
const MUT_GROUP_SLOT: usize = size_of::<(String, MutGroup)>();

/// The map-entry slot the instant path's group charge sizes
/// (`label_groups`).
const INSTANT_GROUP_SLOT: usize = size_of::<(String, (LabelSet, BucketAcc))>();

/// The map-entry slot the non-mutating instant path's group charge sizes
/// (`fp_groups`) — issue #236 P1. The key is a `u64` fingerprint, not a
/// rendered string, so the charge passes an empty key to
/// [`group_entry_bytes`] and the `LabelSet` term prices the hydrated
/// `base_labels` value the group stands for.
const FP_GROUP_SLOT: usize = size_of::<(u64, BucketAcc)>();

/// The label set a fingerprint with no hydrated meta stands for — issue
/// #236 P2 sizes its charge over this so the charge/discharge pair stays
/// exact on a fingerprint `base_labels` never saw (`cloned().unwrap_or_
/// default()` yields exactly this).
static EMPTY_LABEL_SET: LabelSet = Vec::new();

/// A `Vec` element sized through the map-entry helper — issue #236 P2.
/// `series_out` is a `Vec`, not a map, so this is a deliberate
/// OVER-charge: both leaf paths then speak ONE vocabulary
/// ([`group_entry_bytes`]) and the derivation has a single term to reason
/// about instead of two.
const SERIES_OUT_SLOT: usize = size_of::<MatrixSeries>();

/// A provable UPPER BOUND on the query-lifetime heap bytes ONE distinct
/// output group's map entry retains: the rendered-JSON key, the cloned
/// `LabelSet` (each owned string plus the element buffer), and the entry's
/// share of the map table itself. Reuses the round-5
/// [`RangeSlideState::member_stage_bytes`] sizing vocabulary
/// ([`alloc_block_bytes`] / [`grown_alloc_bytes`] / [`MIN_ALLOC_BYTES`]) —
/// one scheme, not two. Over-charging is safe; under-charging is the bug.
///
/// **Per allocation** (each retained block charged through the round-7
/// allocator-rounding model, [`alloc_block_bytes`]):
/// - **rendered key** — produced by [`render_labels_json_sorted`], whose
///   pre-size (`2 + Σ(k+v+6)`) is ≤ `len + 1` (each pair renders at least
///   its raw bytes plus the `"":"",` scaffolding, minus the final comma),
///   and whose geometric growth ends below `2·len`; by charge time
///   rendering has returned, so the LIVE request is ≤ `2·len`, retained
///   ≤ `alloc_block_bytes(2·len)` — dominated by
///   [`grown_alloc_bytes`]`(len) = 3·alloc_block_bytes(len)` for every
///   input.
/// - **each owned label `String`** — `Cow::to_string` reserves exactly the
///   length (or the generic path's `min_non_zero_cap = 8`); both requests
///   are inside [`alloc_block_bytes`]'s size-class-rounded bound.
/// - **`LabelSet` element buffer** — `collect` from a `TrustedLen` iterator
///   reserves exactly `pairs`; charged via [`alloc_block_bytes`] (an empty
///   set allocates nothing — the 32-byte floor is pure margin).
/// - **the map entry's table share** — hashbrown keeps at most ~3.43
///   bucket-slots live per entry at peak (7/8 load factor, power-of-two
///   doubling with the old table still mapped during a resize), each
///   `slot_bytes + 1` control byte wide; with each table block retained at
///   ≤ 2× its request (the [`alloc_block_bytes`] model) that peak is
///   ≤ ~6.86 slots per entry — `8×` dominates it for every entry count,
///   and the flat pad covers the table's fixed control-group padding
///   (also ≤ 2×-rounded) many times over. (`MAP_GROWTH_FACTOR = 8` is a
///   LOAD-FACTOR argument and holds for any entry count — issue #236
///   deleted the group-count cap that used to be quoted here as an
///   additional structural bound, and the bound is unaffected.)
///
/// Saturating arithmetic throughout — a hostile label set can only make
/// the charge LARGER, the safe direction.
fn group_entry_bytes(key: &str, labels: &LabelSet, slot_bytes: usize) -> u64 {
    map_entry_bytes(slot_bytes)
        .saturating_add(grown_alloc_bytes(key.len() as u64))
        .saturating_add(label_set_bytes(labels))
}

/// One map entry's TABLE share: the slot plus its control byte at the
/// growth factor, plus the flat pad. Extracted VERBATIM from
/// [`group_entry_bytes`]'s map-table arithmetic (issue #221) so the
/// variants `meta`-snapshot charge and the group charge share ONE sizing
/// scheme — `group_entry_bytes`'s output is byte-identical to the
/// pre-extraction form (pinned by its existing unit tests).
pub(crate) fn map_entry_bytes(slot_bytes: usize) -> u64 {
    /// See the map-table bullet above: 8 slots per entry dominates the
    /// ~3.43-slot live peak of a 7/8-load, doubling hash table with every
    /// block retained at ≤ 2× its request (review round 7, finding 2).
    const MAP_GROWTH_FACTOR: u64 = 8;
    /// Flat per-entry margin dominating the table's fixed control-group
    /// padding/alignment (≤ 2×-rounded) on every table the series cap
    /// permits.
    const MAP_FIXED_PAD: u64 = 128;

    (slot_bytes as u64)
        .saturating_add(1)
        .saturating_mul(MAP_GROWTH_FACTOR)
        .saturating_add(MAP_FIXED_PAD)
}

/// A cloned [`LabelSet`]'s retained heap: one owned `String` per key and
/// per value (each ≤ [`alloc_block_bytes`]'s size-class-rounded bound)
/// plus the exactly-reserved element buffer. Extracted from
/// [`group_entry_bytes`] (issue #221) so the variants charges reuse the
/// existing vocabulary — one scheme, not two.
pub(crate) fn label_set_bytes(labels: &LabelSet) -> u64 {
    let mut bytes: u64 = 0;
    for (k, v) in labels {
        bytes = bytes
            .saturating_add(alloc_block_bytes(k.len() as u64))
            .saturating_add(alloc_block_bytes(v.len() as u64));
    }
    let elems = (labels.len() as u64).saturating_mul(size_of::<(String, String)>() as u64);
    bytes.saturating_add(alloc_block_bytes(elems))
}

/// The three sliding-window reducer classes (issue #227 finding-4 table,
/// cited to Loki v3.7.4 `range_vector.go` / `syntax/ast.go:1449-1458`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReducerClass {
    /// `count`/`bytes`/`bytes_rate`/`rate`(no-unwrap): a running INTEGER,
    /// add-on-load / subtract-on-evict. Integer ± is exact-invertible, so
    /// the running value is bit-identical to a fresh reduction (finding 1);
    /// only in-window samples are retained (for eviction).
    InvertInteger,
    /// `min`/`max`/`quantile`/`first`/`last`: re-reduce the retained window
    /// per step, order-INDEPENDENT (first/last are positional in the total
    /// order, computed by argmin/argmax — the value does not depend on fold
    /// order), so mergeable across fingerprints without a global sort.
    ReduceIndependent,
    /// `sum`/`avg`/`stddev`/`stdvar`/`rate_counter`/`rate`(unwrap):
    /// re-reduce, order-DEPENDENT — the float-accumulation / reset-walk
    /// order matters, so the retained window is folded in the canonical
    /// `(ts, stream_hash, tie_rank)` order (Loki's heap order).
    CanonicalFold,
}

/// Classifies a reducer (finding 4). `bytes_rate` rejects unwrap ⇒ always
/// integer/invert; `rate` is invert only WITHOUT unwrap (integer line
/// count); with unwrap it is a float sum ⇒ canonical fold. `stdvar` is
/// canonical fold. `absent` is handled specially (a sliding presence set)
/// and nominally classed order-independent.
fn reducer_class(op: RangeAggOp, value: ClientValue) -> ReducerClass {
    match op {
        RangeAggOp::CountOverTime | RangeAggOp::BytesOverTime | RangeAggOp::BytesRate => {
            ReducerClass::InvertInteger
        }
        RangeAggOp::Rate => {
            if matches!(value, ClientValue::Unwrap) {
                ReducerClass::CanonicalFold
            } else {
                ReducerClass::InvertInteger
            }
        }
        RangeAggOp::RateCounter
        | RangeAggOp::SumOverTime
        | RangeAggOp::AvgOverTime
        | RangeAggOp::StddevOverTime
        | RangeAggOp::StdvarOverTime => ReducerClass::CanonicalFold,
        RangeAggOp::MinOverTime
        | RangeAggOp::MaxOverTime
        | RangeAggOp::QuantileOverTime
        | RangeAggOp::FirstOverTime
        | RangeAggOp::LastOverTime
        | RangeAggOp::AbsentOverTime => ReducerClass::ReduceIndependent,
    }
}

/// Start-anchored grid-point count `|{grid_start + k·step ≤ end}|` (Loki:
/// `current` seeded at `start-step`, first point `start`, iterate `≤ end`)
/// — i.e. `floor(span/step) + 1`, BOTH endpoints included, EXACT in i128
/// (the emit grid must cover the true span; only the admission fence
/// saturates — [`fence_intervals`], review round 8). Every caller runs
/// AFTER [`ensure_grid_resolution`] admitted the window, which bounds this
/// count at 22_002 (11_001 when the span fits an int64 duration). Still
/// saturates to `u64::MAX` for a degenerate step, defense-in-depth.
fn grid_point_count(grid_start: i64, end_ns: i64, step_ns: u64) -> u64 {
    if end_ns < grid_start {
        return 0;
    }
    if step_ns == 0 || step_ns > i64::MAX as u64 {
        return u64::MAX;
    }
    let step = step_ns as i128;
    let span = end_ns as i128 - grid_start as i128; // ≥ 0
    u64::try_from(span / step + 1).unwrap_or(u64::MAX)
}

/// One retained sample in a sliding window. Fixed-width (no body bytes ever
/// cross into the window — the deterministic full-body order is pre-baked
/// into `tie_rank` at collision-group formation).
#[derive(Debug, Clone, Copy)]
struct WinSample {
    ts: i64,
    stream_hash: u64,
    /// Group-local rank within the same-`(fingerprint, ts)` collision group,
    /// by ascending full-body bytes (issue #227): the deterministic
    /// same-stream tiebreak.
    tie_rank: u32,
    value: f64,
}

/// A non-empty marker slice element for the class-A (integer) emit path,
/// whose reducer reads only the running integer and the window's
/// emptiness — never the sample contents.
const WIN_SAMPLE_MARKER: WinSample = WinSample {
    ts: 0,
    stream_hash: 0,
    tie_rank: 0,
    value: 0.0,
};

/// The total order for the class-C fold and first/last argmin/argmax:
/// `(ts, stream_hash, tie_rank)` — different ts by ts; same ts different
/// stream by `stream_hash` (Loki-exact cross-stream); same ts same stream
/// (⇒ one collision group) by `tie_rank` (the ratified deterministic
/// same-stream divergence).
fn win_order(a: &WinSample, b: &WinSample) -> std::cmp::Ordering {
    a.ts.cmp(&b.ts)
        .then(a.stream_hash.cmp(&b.stream_hash))
        .then(a.tie_rank.cmp(&b.tie_rank))
}

/// One member of a consecutive same-`(fingerprint, timestamp_ns)` collision
/// run, buffered until the run closes so it can be ranked by full body.
#[derive(Debug)]
struct CollMember {
    /// The raw body bytes — the tiebreak key, held ONLY for the current
    /// group and dropped once `tie_rank` is assigned.
    body: String,
    value: f64,
    /// Mutating path only: the rendered final label-set key + labels.
    out: Option<(String, LabelSet)>,
}

/// Reduces a canonical-ordered window slice into one reducer value (`None`
/// for an empty window ⇒ a gap, except `absent`, handled by the caller).
/// `run_int` is the class-A running integer; `ordered` must already be in
/// canonical `(ts, stream_hash, tie_rank)` order for the class-C folds.
fn reduce_window(
    op: RangeAggOp,
    class: ReducerClass,
    param: Option<f64>,
    rate_window_ns: Option<u64>,
    run_int: u64,
    ordered: &[WinSample],
) -> Option<f64> {
    if ordered.is_empty() {
        return None;
    }
    match op {
        RangeAggOp::CountOverTime | RangeAggOp::BytesOverTime => Some(run_int as f64),
        RangeAggOp::BytesRate => Some(apply_rate(run_int as f64, rate_window_ns)),
        RangeAggOp::Rate => {
            let n = if matches!(class, ReducerClass::InvertInteger) {
                run_int as f64
            } else {
                ordered.iter().fold(0.0_f64, |acc, s| acc + s.value)
            };
            Some(apply_rate(n, rate_window_ns))
        }
        RangeAggOp::FirstOverTime => Some(ordered[0].value),
        RangeAggOp::LastOverTime => Some(ordered[ordered.len() - 1].value),
        RangeAggOp::QuantileOverTime => {
            // The ONE re-reduction that cannot run over a borrow: the
            // quantile needs the values SORTED, and the window itself must
            // stay in ascending-`ts` order for eviction. The copy is
            // pre-charged per retained sample by
            // [`retention_points_per_sample`] (issue #227 review round 5,
            // finding 2), so the cap has already approved it — `collect`
            // from a `TrustedLen` iterator reserves exactly `ordered.len()`
            // `f64`s, four times inside the 32-byte-per-sample charge.
            let mut vals: Vec<f64> = ordered.iter().map(|s| s.value).collect();
            Some(quantile_of(&mut vals, param.unwrap_or(f64::NAN)))
        }
        RangeAggOp::RateCounter => {
            // Over a BORROW — `ordered` is already ascending by `ts` (the
            // canonical order's leading key), which is all the reset walk
            // needs. No `Vec<(i64, f64)>` copy of the whole window per grid
            // point (issue #227 review round 5, finding 2).
            Some(rate_counter_over_sorted(
                ordered.len(),
                |i| (ordered[i].ts, ordered[i].value),
                rate_window_ns,
            ))
        }
        // sum/avg/min/max/stddev/stdvar reuse the instant path's exact
        // arithmetic (`SimpleAcc`) folded in canonical order.
        _ => {
            let mut acc = SimpleAcc::new(ordered[0].ts, ordered[0].value);
            for s in &ordered[1..] {
                acc.add(s.ts, s.value);
            }
            Some(match op {
                RangeAggOp::SumOverTime => acc.sum,
                RangeAggOp::AvgOverTime => acc.mean,
                RangeAggOp::MinOverTime => acc.min,
                RangeAggOp::MaxOverTime => acc.max,
                RangeAggOp::StddevOverTime => (acc.m2 / acc.count as f64).sqrt(),
                RangeAggOp::StdvarOverTime => acc.m2 / acc.count as f64,
                _ => unreachable!("reduce_window: op dispatched above"),
            })
        }
    }
}

/// Class-A integer-cell reduce for the mutating fan-out finish (`ordered` is
/// not retained on the integer path — the running count is authoritative).
fn reduce_int_cell(op: RangeAggOp, rate_window_ns: Option<u64>, run_int: u64) -> f64 {
    match op {
        RangeAggOp::CountOverTime | RangeAggOp::BytesOverTime => run_int as f64,
        RangeAggOp::BytesRate | RangeAggOp::Rate => apply_rate(run_int as f64, rate_window_ns),
        _ => unreachable!("reduce_int_cell: non-integer op"),
    }
}

/// `ceil(a / b)` for `b > 0`, exact over i128 (handles negative `a`).
fn ceil_div_i128(a: i128, b: i128) -> i128 {
    let q = a.div_euclid(b);
    if a.rem_euclid(b) != 0 { q + 1 } else { q }
}

/// A single fingerprint's streaming sliding window (non-mutating path). Its
/// deque is inherently in canonical order — samples load in ascending ts and
/// within a ts by ascending `tie_rank`, and `stream_hash` is constant for
/// one fingerprint.
#[derive(Debug)]
struct FpSlide {
    stream_hash: u64,
    labels: LabelSet,
    op: RangeAggOp,
    class: ReducerClass,
    param: Option<f64>,
    rate_window_ns: Option<u64>,
    grid_start: i64,
    step: u64,
    range: i64,
    kmax: i64,
    /// Next grid index to emit.
    next_k: i64,
    win: VecDeque<WinSample>,
    /// Class-A running integer (add-on-load / subtract-on-evict).
    run_int: u64,
    /// Retention points one retained sample costs — the window entry plus
    /// any per-emit re-reduce scratch the reducer needs
    /// ([`retention_points_per_sample`]). Charged on load, discharged on
    /// evict, so charge and discharge use the same unit.
    per_sample: u64,
    points: Vec<(i64, f64)>,
}

impl FpSlide {
    fn grid_point(&self, k: i64) -> i64 {
        // i128 intermediate + a SATURATING narrowing (issue #227 arithmetic
        // sweep): `k <= kmax` derives from `grid_point_count`, so every grid
        // point is in `[grid_start, end]` and the clamp never fires on a
        // real query — but a plain `as i64` would silently wrap rather than
        // saturate if that invariant were ever broken.
        clamp_bucket(self.grid_start as i128 + k as i128 * self.step as i128)
    }

    /// Emits every grid point `t` with `t < boundary` (all their samples are
    /// already loaded, since future samples have `ts ≥ boundary > t`).
    fn emit_until(&mut self, boundary: i64, retained: &mut u64) {
        while self.next_k <= self.kmax {
            let t = self.grid_point(self.next_k);
            if t >= boundary {
                break;
            }
            self.emit_at(t, retained);
            self.next_k += 1;
        }
    }

    /// Evicts `T ≤ t-range` from the window front (discharging retention),
    /// then reduces the window and records a point (empty ⇒ gap).
    fn emit_at(&mut self, t: i64, retained: &mut u64) {
        // `checked_sub` (issue #227 review round 11): for `t` near `i64::MIN`
        // (or a `range` ≫ the whole window) the logical eviction bound
        // `t-range` sits BELOW the representable domain — the window
        // `(t-range, t]` then covers every stored ts ≤ t and there is
        // NOTHING to evict. The prior saturating form clamped the bound to
        // `i64::MIN` and the `ts <= lo` eviction wrongly dropped a sample
        // stored at exactly `i64::MIN`, which the reference includes. A
        // legitimately-computed `lo == i64::MIN` (no underflow) still
        // evicts a sample at exactly that timestamp — exclusive semantics
        // are lost only when the bound is genuinely sub-domain.
        if let Some(lo) = t.checked_sub(self.range) {
            while let Some(front) = self.win.front() {
                if front.ts <= lo {
                    let ev = self.win.pop_front().expect("front present");
                    // Symmetric discharge (the concurrent-retention
                    // invariant): every evicted sample was charged on load in
                    // the same `per_sample` unit, so this is exact; the
                    // saturating form is panic-proofing, never a masked
                    // accounting path (class-A values are `1.0` or a
                    // non-negative `line.len()`, added and removed once each).
                    discharge_retention(retained, self.per_sample);
                    if matches!(self.class, ReducerClass::InvertInteger) {
                        self.run_int = self.run_int.saturating_sub(ev.value as u64);
                    }
                } else {
                    break;
                }
            }
        }
        // Class A (integer invert) needs only the running value + emptiness —
        // no per-emit slice materialization. Class B/C re-reduce the window
        // over a BORROW: `make_contiguous` rotates the deque in place and
        // hands back `&mut [T]`, so the re-reduction ALLOCATES NOTHING (issue
        // #227 review round 5, finding 2). The previous `reduce_buf` copied
        // the whole retained window every step, doubling peak memory for an
        // uncharged duplicate; removing the copy is strictly better than
        // charging it. Fields are copied out first so the `&mut self.win`
        // borrow stays disjoint.
        let (op, class, param, rate_window_ns, run_int) = (
            self.op,
            self.class,
            self.param,
            self.rate_window_ns,
            self.run_int,
        );
        let v = if matches!(class, ReducerClass::InvertInteger) {
            if self.win.is_empty() {
                None
            } else {
                // A non-empty marker slice — class A ignores its contents.
                reduce_window(
                    op,
                    class,
                    param,
                    rate_window_ns,
                    run_int,
                    std::slice::from_ref(&WIN_SAMPLE_MARKER),
                )
            }
        } else {
            let window = self.win.make_contiguous();
            reduce_window(op, class, param, rate_window_ns, run_int, window)
        };
        if let Some(v) = v {
            self.points.push((t, v));
        }
    }

    /// Loads one collision group (already `tie_rank`-ranked in `members`) at
    /// `ts`: emits grid points `< ts` first, then charges each member into the
    /// window. Reads values straight from the buffer (no intermediate `Vec`).
    fn load_group(
        &mut self,
        ts: i64,
        members: &[CollMember],
        retained: &mut u64,
        cap: u64,
    ) -> Result<(), ReadError> {
        self.emit_until(ts, retained);
        for (rank, m) in members.iter().enumerate() {
            let value = m.value;
            // Size → check the cap → allocate, through the ONE gate (issue
            // #227 review round 5, finding 2): `push_back` may grow the
            // deque, so the cap must refuse before the push, not after it.
            charge_retention(retained, self.per_sample, cap)?;
            self.win.push_back(WinSample {
                ts,
                stream_hash: self.stream_hash,
                tie_rank: rank as u32,
                value,
            });
            if matches!(self.class, ReducerClass::InvertInteger) {
                self.run_int += value as u64;
            }
        }
        Ok(())
    }

    /// Drains the remaining grid points at fingerprint close.
    fn finish(mut self, retained: &mut u64) -> Option<MatrixSeries> {
        while self.next_k <= self.kmax {
            let t = self.grid_point(self.next_k);
            self.emit_at(t, retained);
            self.next_k += 1;
        }
        // Discharge whatever is still retained (the series is closed), in
        // the same `per_sample` unit it was charged in.
        discharge_retention(retained, self.win.len() as u64 * self.per_sample);
        if self.points.is_empty() {
            None
        } else {
            Some(MatrixSeries {
                labels: self.labels,
                points: self.points,
            })
        }
    }
}

/// One class-A grid cell's DELTA (issue #236 Part C, C1): a sample
/// covering `[k_lo, k_hi]` records `(+value, +1)` at `k_lo` and
/// `(-value, -1)` at `k_hi + 1`, and the covered cells are recovered by
/// prefix-summing ascending. Two map touches per sample instead of one
/// per covered cell — the same difference-array trick `present_cover`
/// already uses for `absent_over_time`.
///
/// `dcount` cannot fold into `dvalue`: `bytes_over_time` over an empty
/// line contributes value 0 to a cell that is nonetheless COVERED and
/// must emit `0`, which only a separate coverage count distinguishes from
/// a gap.
///
/// Both fields are `i64` and the arithmetic is exact — class A is the
/// invert-INTEGER class, so the running value is integral end to end and
/// `reduce_int_cell` is the only float conversion. Neither can overflow:
/// each surviving sample contributes one `+`/`-` pair, `|value|` is
/// either 1 or a line length, and the scan is hard-bounded by
/// `LOGQL_SCAN_BUDGET_BYTES_CEILING` bytes — so both running totals are
/// bounded by the scanned byte count, itself `< 2^63` (the same structural
/// argument `present_cover`'s counter width rests on).
#[derive(Debug, Clone, Copy, Default)]
struct IntDelta {
    dvalue: i64,
    dcount: i64,
}

/// How ONE mutating output group accumulates. Which arm a query uses is
/// fixed once, at state construction, by [`mut_cells_for`] — never per
/// group and never per sample — so a group cannot hold a mixed
/// representation and the arms cannot interleave.
#[derive(Debug)]
enum MutCells {
    /// **Class A, non-overlapping windows.** One entry per COVERED grid
    /// cell, accumulated in place. Kept VERBATIM through issue #236 Part
    /// C: each sample then covers exactly one cell, so this is already
    /// `O(1)` per sample, it charges ONE retention point where the delta
    /// form would charge two, and it collapses repeated samples in the
    /// same cell into that single entry.
    IntExpanded(HashMap<i64, u64>),
    /// **Class A, overlapping windows.** A difference array over grid
    /// indices ([`IntDelta`]), prefix-summed at finish — two map touches
    /// per sample instead of `ceil(range/step)`.
    IntDeltas(HashMap<i64, IntDelta>),
    /// **Classes B/C.** Every surviving sample, retained ONCE; the
    /// covering cells are recovered at finish by sorting with
    /// [`win_order`] and sweeping two pointers over ascending grid
    /// indices. Retention is `O(samples)` and INDEPENDENT of
    /// `ceil(range/step)`, where the previous per-cell map retained one
    /// copy of the sample per covering cell.
    Samples(Vec<WinSample>),
}

impl MutCells {
    /// Retained entries, in the same unit [`charge_retention`] charged
    /// them in — one point per created class-A entry, `per_sample` points
    /// per retained class-B/C sample (the caller multiplies).
    ///
    /// Test-only observability, like [`VectorAggFold::cells`]: production
    /// code charges and discharges through the counter itself, so
    /// exposing a second way to ask would invite the two to drift.
    #[cfg(test)]
    fn charged_units(&self) -> u64 {
        match self {
            MutCells::IntExpanded(m) => m.len() as u64,
            MutCells::IntDeltas(m) => m.len() as u64,
            MutCells::Samples(v) => v.len() as u64,
        }
    }

    /// Test-only, as [`MutCells::charged_units`].
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        match self {
            MutCells::IntExpanded(m) => m.is_empty(),
            MutCells::IntDeltas(m) => m.is_empty(),
            MutCells::Samples(v) => v.is_empty(),
        }
    }
}

/// One mutating/regrouping output group's fan-out state.
#[derive(Debug)]
struct MutGroup {
    labels: LabelSet,
    cells: MutCells,
}

/// The sliding-window range evaluator (issue #227).
#[derive(Debug)]
struct RangeSlideState<'q> {
    compiled: &'q super::pipeline::CompiledPipeline,
    op: RangeAggOp,
    class: ReducerClass,
    value_kind: ClientValue,
    param: Option<f64>,
    rate_window_ns: Option<u64>,
    grid_start: i64,
    step: u64,
    range: i64,
    kmax: i64,
    fan_out: bool,
    is_absent: bool,
    /// Whether the reducer is order-DEPENDENT (class C, or first/last) and so
    /// needs the full-body `tie_rank` order within a collision group. When
    /// false (count/bytes/rate-no-unwrap, min/max/quantile) the body is never
    /// retained — no per-row clone, no per-group sort (the alloc-gate path).
    needs_body_order: bool,
    absent_labels: LabelSet,
    base_labels: HashMap<u64, LabelSet>,
    hashes: HashMap<u64, u64>,
    /// Concurrent retained-point count (charge-on-load / discharge-on-evict),
    /// gated by [`charge_retention`]/[`discharge_retention`].
    retained: u64,
    /// Retention points one retained sample costs
    /// ([`retention_points_per_sample`]) — shared by the streaming sliders
    /// and the mutating fan-out cells so both charge in the same unit.
    per_sample: u64,
    /// Every retention cap this state checks (issue #221): the
    /// concurrent-retention cap, the query-lifetime group-byte cap, the
    /// series cap and the collision-staging caps, in ONE field — always
    /// [`AggCaps::DEFAULT`] in production single-extractor use (the former
    /// `retention_cap`/`group_bytes_cap` test-seam precedent); a
    /// `variants(...)` query hands each sub-state
    /// `AggCaps::DEFAULT.divided(n)` so the per-field SUM over sub-states
    /// is exactly the single-query bound.
    caps: AggCaps,
    // Non-mutating.
    cur: Option<FpSlide>,
    cur_fp: u64,
    series_out: Vec<MatrixSeries>,
    // Mutating.
    groups: HashMap<String, MutGroup>,
    /// `absent_over_time`'s selector-wide presence, as a **grid-sized
    /// difference array** (issue #227 review round 1 finding 1): index `k`
    /// holds the coverage delta at grid point `k`, prefix-summed once at
    /// finish. Length is `kmax + 2` — O(grid), capped by
    /// `MAX_CLIENT_AGG_BUCKETS`, and completely independent of scan density
    /// (nothing per-sample is kept).
    ///
    /// **Counter width proof (review round 2 finding 1).** Each surviving
    /// collision GROUP contributes exactly `+1` at one index and `-1` at
    /// another, so `|counter| <= number of surviving collision groups <=
    /// number of scanned rows`. The scan is hard-bounded by
    /// `reader.logql_scan_budget_bytes` (`max_bytes_to_read`, ceiling
    /// `LOGQL_SCAN_BUDGET_BYTES_CEILING`), and every `log_samples` row costs
    /// at least one byte, so
    /// `rows <= LOGQL_SCAN_BUDGET_BYTES_CEILING < 2^63 = i64::MAX`.
    /// An `i64` counter therefore **cannot** overflow for any query the
    /// engine will run — whereas the previous `i32` (max 2.1e9) was genuinely
    /// reachable under a multi-GiB budget. No cap or checked arithmetic is
    /// needed; the bound is structural, and
    /// `absent_presence_counter_cannot_overflow_under_the_scan_budget` pins
    /// the arithmetic.
    present_cover: Vec<i64>,
    // Current collision run.
    coll_active: bool,
    coll_fp: u64,
    coll_ts: i64,
    coll: Vec<CollMember>,
    /// Bytes of body currently staged in `coll` — charged BEFORE each clone
    /// and reset whenever the group is flushed, so the staged buffer is
    /// bounded by `MAX_TS_COLLISION_GROUP_BYTES` by construction (issue #227
    /// review round 3, finding 1).
    coll_bytes: u64,
    /// QUERY-LIFETIME bytes retained by `groups` — each entry's rendered
    /// key + `LabelSet` + map-slot share ([`group_entry_bytes`]), gated by
    /// [`charge_group_bytes`] BEFORE the insertion that retains them and
    /// released as `finish_in_place` consumes each entry (issue #227 review
    /// round 6: these bytes outlive the collision flush that resets
    /// `coll_bytes`, so they need their own counter). Between the flush's
    /// `coll_bytes` reset and each member's charge-or-drop, the group's
    /// staged members are transiently outside both counters — a transient
    /// bounded by [`MAX_TS_COLLISION_GROUP_BYTES`] (one group staged at a
    /// time) and freed within the same flush.
    group_bytes: u64,
    /// Issue #236: RESULT point-slots reserved by this state, through
    /// [`charge_result_points`]. One grid's width per output series, at
    /// the moment the series is created — `O(1)`, never per point. An
    /// ADMISSION counter: it is never discharged, because the points it
    /// reserves are the result.
    result_points: u64,
    /// The innermost vector aggregation, applied AS this state emits
    /// (issue #236 Part B) instead of over its materialised output.
    /// `None` — the state's own construction default — is the
    /// materialising path this leaf has always taken; it is attached by
    /// [`RangeSlideState::attach_fold`] after construction, so the
    /// constructor's shape (and its allocation census) is unchanged.
    fold: Option<VectorAggFold>,
}

impl<'q> RangeSlideState<'q> {
    fn new(
        compiled: &'q super::pipeline::CompiledPipeline,
        meta: &HashMap<u64, StreamMetaRow>,
        client: &'q ClientAgg,
        window: ClientWindow,
        rate_window_ns: Option<u64>,
        caps: AggCaps,
    ) -> Result<Self, ReadError> {
        // Destructuring the `Range` variant is what makes the range
        // GUARANTEED present and validated (issue #227 review round 4,
        // finding 2): there is no zero/absent range to receive.
        let ClientWindow::Range {
            grid_start_ns: grid_start,
            end_ns,
            step_ns,
            range_ns,
        } = window
        else {
            unreachable!("RangeSlideState is constructed only for a Range window")
        };
        let step = step_ns.as_u64();
        // Loki-exact resolution rule (review round 7, finding 1): reject on
        // `intervals > 11000`, NOT on the point count — the inclusive grid
        // of an exactly-at-the-limit request holds 11_001 points and the
        // reference serves it.
        let count = ensure_grid_resolution(grid_start, end_ns, step)?;
        // `count == kmax + 1` (0 ⇒ empty grid, kmax = -1).
        let kmax = count as i64 - 1;
        let mut base_labels: HashMap<u64, LabelSet> = HashMap::new();
        let mut hashes: HashMap<u64, u64> = HashMap::new();
        for (fp, m) in meta {
            let labels = series_labels(m);
            hashes.insert(*fp, stream_hash(&labels));
            base_labels.insert(*fp, labels);
        }
        let op = client.range_op;
        let class = reducer_class(op, client.value);
        let needs_body_order = matches!(class, ReducerClass::CanonicalFold)
            || matches!(op, RangeAggOp::FirstOverTime | RangeAggOp::LastOverTime);
        let is_absent = matches!(op, RangeAggOp::AbsentOverTime);
        Ok(RangeSlideState {
            compiled,
            op,
            class,
            value_kind: client.value,
            param: client.param,
            rate_window_ns,
            grid_start,
            step,
            // No narrowing: `range_ns` is already a boundary-validated,
            // in-domain `i64` (issue #227 review round 2, finding 2).
            range: range_ns.get(),
            kmax,
            fan_out: compiled.metric_mutates_labels(),
            is_absent,
            needs_body_order,
            absent_labels: client.absent_labels.clone(),
            base_labels,
            hashes,
            retained: 0,
            per_sample: retention_points_per_sample(op),
            caps,
            cur: None,
            cur_fp: 0,
            series_out: Vec::new(),
            groups: HashMap::new(),
            // `kmax + 2` slots (`kmax + 1` grid points plus the exclusive
            // upper delta index). `kmax` passed the 11k grid guard above, so
            // this allocation is bounded before any row is read — and it is
            // made ONLY for `absent_over_time` (issue #221): `present_cover`
            // is written only under `is_absent` and read only in
            // `finish_absent`, reached under the same flag, so every other
            // reducer keeps an empty (allocation-free) vec. The gate is the
            // branch-free `* (is_absent as usize)` multiplier (a zero-length
            // `vec![0; 0]` never allocates) so this constructor's censused
            // branch shape is unchanged.
            present_cover: vec![0; (kmax.max(-1) + 2) as usize * (is_absent as usize)],
            coll_active: false,
            coll_fp: 0,
            coll_ts: 0,
            coll: Vec::new(),
            coll_bytes: 0,
            group_bytes: 0,
            result_points: 0,
            fold: None,
        })
    }

    /// The grid this state emits on — the fold indexes its dense slots by
    /// the same triple, so a slot and a grid point are two views of one
    /// value.
    fn grid(&self) -> FoldGrid {
        FoldGrid {
            start: self.grid_start,
            step: self.step,
            kmax: self.kmax,
        }
    }

    /// Hands the INNERMOST vector aggregation to the leaf (issue #236
    /// Part B). A no-op for the specs the leaf cannot own
    /// ([`VectorAggFold::new`] returns `None`), which is why
    /// [`RangeSlideState::folded_aggs`] — not the caller's intent —
    /// decides how many specs the caller must still apply.
    ///
    /// Attached AFTER `new` rather than taken as a constructor parameter:
    /// the grid is only known once `ensure_grid_resolution` has run, and
    /// keeping it out of `new` leaves that constructor's branch/allocation
    /// census (issue #221 `logql_variants_alloc`) untouched.
    fn attach_fold(&mut self, spec: &plan::VectorAggSpec) {
        self.fold = VectorAggFold::new(spec, self.grid(), self.caps.result_points);
    }

    /// How many trailing (innermost) specs this leaf has taken over: 0 or
    /// 1. The caller applies the remaining prefix.
    fn folded_aggs(&self) -> usize {
        usize::from(self.fold.is_some())
    }

    /// Folds one batch of (physical-key-ordered) rows: runs the pipeline,
    /// fails on a surviving `__error__`, and buffers same-`(fingerprint,
    /// timestamp_ns)` runs for full-body ranking before loading them.
    ///
    /// `base_labels` is moved to a LOCAL for the duration so the batch-reused
    /// `scratch` (whose `Cow`s borrow it) does not tie a `self` borrow across
    /// the mid-loop `&mut self` `flush_collision` — that keeps the fold's
    /// per-row allocation at ZERO (the alloc-gate discipline), one shared
    /// scratch across the whole batch.
    fn push_rows(&mut self, rows: &[MetricScanRow]) -> Result<(), ReadError> {
        let base_labels = std::mem::take(&mut self.base_labels);
        let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
        let mut result = Ok(());
        for row in rows {
            // `base_labels` is a LOCAL, so the reused `scratch` (whose `Cow`s
            // borrow it) does not tie a `self` borrow across this `&mut self`
            // flush — the fold stays zero-alloc-per-row with one shared
            // scratch across the batch.
            if self.coll_active
                && (self.coll_fp != row.fingerprint || self.coll_ts != row.timestamp_ns)
                && let Err(e) = self.flush_collision(&base_labels)
            {
                result = Err(e);
                break;
            }
            let Some(base) = base_labels.get(&row.fingerprint) else {
                continue;
            };
            let (line, value) =
                match self
                    .compiled
                    .run_metric_into(&row.body, base, row.timestamp_ns, &mut scratch)
                {
                    Ok(MetricRun::Dropped) => continue,
                    Ok(MetricRun::Kept { line, value }) => (line, value),
                    Err(e) => {
                        result = Err(e.into());
                        break;
                    }
                };
            if let Err(e) = check_surviving_error(&scratch) {
                result = Err(e);
                break;
            }
            let v = match self.value_kind {
                ClientValue::Count => 1.0,
                ClientValue::Bytes => line.len() as f64,
                ClientValue::Unwrap => match value {
                    Some(v) => v,
                    None => continue,
                },
            };
            self.coll_active = true;
            self.coll_fp = row.fingerprint;
            self.coll_ts = row.timestamp_ns;
            // THE SINGLE STAGING FUNNEL (issue #227 review round 4, finding
            // 1): every per-member allocation — body clone, rendered label
            // JSON, cloned `LabelSet` — is sized, capped, THEN allocated in
            // one place. No reducer class has a path that stages anything
            // outside it.
            if let Err(e) = self.stage_member(row, v, &mut scratch) {
                result = Err(e);
                break;
            }
        }
        self.base_labels = base_labels;
        result
    }

    /// **The single staging funnel** — the ONLY place a per-member
    /// allocation enters `coll` (issue #227 review round 4, finding 1).
    ///
    /// Order is load-bearing: (i) size every allocation this member will
    /// make, (ii) check BOTH caps, (iii) only then allocate. Because the
    /// sizing happens first, the allocation that would breach a cap is never
    /// performed, for **every** reducer class — the previous code sized only
    /// the body (and only when `needs_body_order`), leaving the fan-out
    /// `key`/`LabelSet` clones uncharged.
    ///
    /// The charge is an UPPER BOUND on the heap bytes about to be allocated
    /// (see [`Self::member_stage_bytes`]), so `coll_bytes >= actual staged
    /// heap bytes` always, and `coll_bytes <= MAX_TS_COLLISION_GROUP_BYTES`
    /// is enforced — hence actual staged bytes are capped too.
    fn stage_member(
        &mut self,
        row: &MetricScanRow,
        value: f64,
        scratch: &mut Vec<(Cow<'_, str>, Cow<'_, str>)>,
    ) -> Result<(), ReadError> {
        let stages_out = self.fan_out && !self.is_absent;
        if stages_out {
            // In-place sort, no allocation — safe to do before the caps.
            scratch.sort_unstable();
        }
        // (i) size EVERY allocation this member will make.
        let bytes = self.member_stage_bytes(row, scratch, stages_out);
        // (ii) both caps, BEFORE any allocation. `saturating_add` cannot mask
        // a breach (saturation only grows the sum) but does keep a
        // pathological charge from overflowing the comparison itself.
        let next_count = self.coll.len() as u64 + 1;
        let next_bytes = self.coll_bytes.saturating_add(bytes);
        if next_count > self.caps.collision_members || next_bytes > self.caps.collision_bytes {
            return Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup {
                count: next_count,
                cap: self.caps.collision_members,
                bytes: next_bytes,
                bytes_cap: self.caps.collision_bytes,
            }));
        }
        // (iii) now — and only now — allocate.
        let body = if self.needs_body_order {
            row.body.clone()
        } else {
            String::new()
        };
        let out = if stages_out {
            let key = render_labels_json_sorted(scratch);
            let labels: LabelSet = scratch
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            Some((key, labels))
        } else {
            None
        };
        self.coll_bytes = next_bytes;
        self.coll.push(CollMember { body, value, out });
        Ok(())
    }

    /// A provable UPPER BOUND on the heap bytes [`Self::stage_member`] is
    /// about to allocate for one member. Charging an over-estimate is what
    /// makes the cap sound: `coll_bytes >= actual`, so bounding `coll_bytes`
    /// bounds the real footprint.
    ///
    /// **Per allocation** (issue #227 review round 5, finding 1 — the
    /// round-4 version counted the JSON content ONCE and charged a flat 32
    /// bytes per container slot, both of which an adversarial input beats):
    /// - **rendered label JSON** ([`render_labels_json_sorted`]) — sized by
    ///   [`rendered_labels_json_len`], which walks the SAME escape arms as
    ///   [`push_json_string`] and so yields the rendered length EXACTLY,
    ///   including the `\u00xx` six-bytes-for-one expansion of a C0 control
    ///   byte that the previous charge missed. Sizing (rather than assuming a
    ///   6× worst case) also means an ordinary multi-hundred-KiB label value
    ///   is not rejected six times too early. The `String` is pre-sized with
    ///   an estimate that assumes no escaping, so escaping-heavy input forces
    ///   geometric growth — charged through [`grown_alloc_bytes`].
    /// - **cloned `LabelSet`** — one exactly-reserved `String` per key and
    ///   per value, plus one exactly-reserved element buffer of `pairs ×
    ///   size_of::<(String, String)>()` (the `collect` runs from a
    ///   `TrustedLen` iterator). Each is charged through
    ///   [`alloc_block_bytes`] — allocator size-class rounding covered,
    ///   floored at the allocator's minimum block — so a label set of many
    ///   one-byte values cannot cost more than its charge.
    /// - **body clone** — `String::clone` reserves exactly `row.body.len()`
    ///   in one allocation; charged through [`alloc_block_bytes`], only when
    ///   `needs_body_order`.
    /// - **`CollMember` slot in `coll`** — a `Vec` push can DOUBLE capacity
    ///   with the old buffer still live during the realloc, so for `n`
    ///   members the live request is `≤ 2n` slots plus an old buffer of
    ///   `≤ n` slots; with each block retained at ≤ 2× its request (the
    ///   [`alloc_block_bytes`] model, review round 7 finding 2) the peak is
    ///   `≤ 6n × size_of::<CollMember>()`, and the initial
    ///   `min_non_zero_cap = 4` block is `≤ 8 × size_of::<CollMember>()`.
    ///   Charging `8 × size_of::<CollMember>()` per member dominates BOTH
    ///   for every `n ≥ 1`. The `String`/`Vec` headers of the pieces above
    ///   live INSIDE that slot, so they are already covered.
    ///
    /// Saturating arithmetic throughout, so a hostile label value cannot
    /// wrap the charge round to a small number — saturation can only make
    /// the charge larger, which is the safe direction.
    fn member_stage_bytes(
        &self,
        row: &MetricScanRow,
        scratch: &[(Cow<'_, str>, Cow<'_, str>)],
        stages_out: bool,
    ) -> u64 {
        /// See the `CollMember`-slot bullet above: 8× per element dominates
        /// the ≤ 6n-slot realloc peak (2n live + n old, each ≤ 2×-rounded)
        /// and the first member's `min_non_zero_cap = 4` block.
        const VEC_GROWTH_FACTOR: u64 = 8;

        let mut bytes = (size_of::<CollMember>() as u64).saturating_mul(VEC_GROWTH_FACTOR);

        if self.needs_body_order {
            bytes = bytes.saturating_add(alloc_block_bytes(row.body.len() as u64));
        }

        if stages_out {
            // The rendered JSON, sized exactly, then grown.
            bytes = bytes.saturating_add(grown_alloc_bytes(rendered_labels_json_len(scratch)));
            // The cloned `LabelSet`: one owned `String` per key and per
            // value, each floored at the allocator's minimum block.
            for (k, v) in scratch {
                bytes = bytes
                    .saturating_add(alloc_block_bytes(k.len() as u64))
                    .saturating_add(alloc_block_bytes(v.len() as u64));
            }
            // ...plus its exactly-reserved element buffer.
            let elems = (scratch.len() as u64).saturating_mul(size_of::<(String, String)>() as u64);
            bytes = bytes.saturating_add(alloc_block_bytes(elems));
        }

        bytes
    }

    /// Ranks and dispatches the buffered collision group (full-body order ⇒
    /// deterministic `tie_rank`), then releases the bodies.
    fn flush_collision(&mut self, base_labels: &HashMap<u64, LabelSet>) -> Result<(), ReadError> {
        if self.coll.is_empty() {
            self.coll_active = false;
            return Ok(());
        }
        let fp = self.coll_fp;
        let ts = self.coll_ts;
        self.coll_active = false;
        // The staged bodies are released with this group (every exit path
        // below either clears or takes `coll`), so the byte charge resets
        // here — one group is staged at a time, which is what makes
        // `MAX_TS_COLLISION_GROUP_BYTES` a true bound on the staged buffer.
        // The fan-out `key`/`LabelSet` that SURVIVE the flush into the
        // query-lifetime `groups` map are re-charged against
        // `MAX_CLIENT_AGG_GROUP_BYTES` before that insertion
        // (`fan_out_sample`, issue #227 review round 6), so nothing escapes
        // both counters — the only uncounted state is this flush's own
        // staged members, bounded by the collision cap and freed before it
        // returns.
        self.coll_bytes = 0;
        // A true total order over the group (order-dependent reducers only):
        // distinct bodies always differ; byte-identical bodies are genuinely
        // interchangeable. Skipped entirely for the common order-independent
        // reducers (which never retained a body).
        if self.needs_body_order {
            self.coll
                .sort_by(|a, b| a.body.as_bytes().cmp(b.body.as_bytes()));
        }

        if self.is_absent {
            // Issue #227 review finding 1: presence is recorded as O(GRID)
            // COVERAGE, never an O(scan) timestamp vector. One surviving
            // sample at `ts` makes every grid point in `[k_lo, k_hi]`
            // non-empty; a difference array records that in O(1) per group
            // and is prefix-summed once at finish. Memory is bounded by the
            // grid (already capped at `MAX_CLIENT_AGG_BUCKETS`) regardless of
            // scan density — nothing per-sample is retained, so no
            // client-supplied range/step/cardinality can grow it.
            if !self.coll.is_empty() {
                let (k_lo, k_hi) = self.covering_k(ts);
                if k_lo <= k_hi {
                    self.present_cover[k_lo as usize] += 1;
                    self.present_cover[k_hi as usize + 1] -= 1;
                }
            }
            self.coll.clear();
            return Ok(());
        }

        let stream_hash = self.hashes.get(&fp).copied().unwrap_or(0);
        if self.fan_out {
            // The fan-out path moves `out` out of each member, so it consumes
            // the buffer (the mutating path is the already-expensive class);
            // `coll` re-grows on the next group.
            let members = std::mem::take(&mut self.coll);
            for (rank, m) in members.into_iter().enumerate() {
                let (key, labels) = m.out.expect("mutating member carries its output set");
                self.fan_out_sample(key, labels, ts, stream_hash, rank as u32, m.value)?;
            }
            return Ok(());
        }

        // Non-mutating: one slider per fingerprint (fingerprints contiguous).
        if self.cur.is_none() || self.cur_fp != fp {
            if let Some(prev) = self.cur.take() {
                self.rotate_slider(prev);
            }
            // Issue #236 P2 — the premise fix on the non-mutating RANGE
            // path. Part A deleted the mid-scan group-count rejection
            // that used to stand here, and the `labels` clone
            // below plus the eventual `series_out` element were otherwise
            // uncharged. Charged BEFORE the clone, so a refusal never
            // allocates; discharged in the slider rotation's `None` arm
            // (a series that yields no points) and at the end of
            // `finish_in_place`'s non-mutating arm.
            let src = base_labels.get(&fp);
            charge_group_bytes(
                &mut self.group_bytes,
                group_entry_bytes("", src.unwrap_or(&EMPTY_LABEL_SET), SERIES_OUT_SLOT),
                self.caps.group_bytes,
            )?;
            // Issue #236: this slider will emit at most one point per
            // grid point, so the grid's width is reserved once, here,
            // before the slider exists — `O(1)`, and in the same units
            // `MAX_METRIC_RESULT_POINTS` is derived in.
            charge_result_points(
                &mut self.result_points,
                grid_slot_count(self.kmax),
                self.caps.result_points,
            )?;
            let labels = src.cloned().unwrap_or_default();
            self.cur = Some(FpSlide {
                stream_hash,
                labels,
                op: self.op,
                class: self.class,
                param: self.param,
                rate_window_ns: self.rate_window_ns,
                grid_start: self.grid_start,
                step: self.step,
                range: self.range,
                kmax: self.kmax,
                next_k: 0,
                win: VecDeque::new(),
                run_int: 0,
                per_sample: self.per_sample,
                points: Vec::new(),
            });
            self.cur_fp = fp;
        }
        // Disjoint field borrows (`cur`, `coll`, `retained`) — the slider
        // reads values straight from the buffer, no intermediate `Vec`; the
        // buffer's capacity is reused (cleared, never reallocated per group).
        let cur = self.cur.as_mut().expect("slider just set");
        cur.load_group(
            ts,
            &self.coll,
            &mut self.retained,
            self.caps.retention_points,
        )?;
        self.coll.clear();
        Ok(())
    }

    /// Retires the finished non-mutating slider — issue #236 P2's discharge
    /// leg. A slider that produced points hands its `MatrixSeries` to
    /// `series_out` and KEEPS its charge (the bytes are still live) until
    /// `finish_in_place` releases the whole vector; a slider that produced
    /// NONE is dropped here, so its charge is released here. Sized over the
    /// same labels P2 charged (`FpSlide::labels` is `base_labels`' value,
    /// cloned and never mutated), which is what makes `group_bytes == 0` at
    /// finish an exact identity rather than an inequality.
    fn rotate_slider(&mut self, prev: FpSlide) {
        let entry_bytes = group_entry_bytes("", &prev.labels, SERIES_OUT_SLOT);
        match prev.finish(&mut self.retained) {
            Some(series) => self.series_out.push(series),
            None => discharge_group_bytes(&mut self.group_bytes, entry_bytes),
        }
    }

    /// Fans one ranked sample into every grid cell whose window `(t-range,
    /// t]` covers `ts` (the covering-set identity `t ∈ [ts, ts+range)`).
    fn fan_out_sample(
        &mut self,
        key: String,
        labels: LabelSet,
        ts: i64,
        stream_hash: u64,
        tie_rank: u32,
        value: f64,
    ) -> Result<(), ReadError> {
        let (k_lo, k_hi) = self.covering_k(ts);
        if k_lo > k_hi {
            return Ok(());
        }
        let is_new = !self.groups.contains_key(&key);
        if is_new {
            // Issue #227 review round 6: the key + `LabelSet` MOVE into the
            // QUERY-LIFETIME `groups` map below and outlive the collision
            // flush (whose `coll_bytes` charge has already been reset), so
            // their bytes are charged to the query-lifetime counter BEFORE
            // the insertion that retains them — refused means never
            // inserted, for any label size and any group count.
            charge_group_bytes(
                &mut self.group_bytes,
                group_entry_bytes(&key, &labels, MUT_GROUP_SLOT),
                self.caps.group_bytes,
            )?;
            // Issue #236: same reservation as the non-mutating slider —
            // one grid width per output group, once, before the group
            // exists.
            charge_result_points(
                &mut self.result_points,
                grid_slot_count(self.kmax),
                self.caps.result_points,
            )?;
        }
        let per_sample = self.per_sample;
        let cap = self.caps.retention_points;
        let empty_cells = mut_cells_for(self.class, self.range, self.step, self.kmax);
        let retained = &mut self.retained;
        let group = self.groups.entry(key).or_insert_with(|| MutGroup {
            labels,
            cells: empty_cells,
        });
        // Review finding 3: a newly-CREATED entry is charged to the same
        // concurrent-retention counter as a retained point, so the
        // mutating fan-out obeys the documented invariant instead of
        // relying on an implicit `groups × grid` product — which issue
        // #236 has since deleted outright (there is no group-count cap
        // left to form one). Updating an EXISTING entry is O(1) and
        // charges nothing. Size → check the cap → allocate, through the
        // ONE gate (issue #227 review round 5, finding 2): an insert may
        // grow the map, so the cap must refuse before it, not after.
        //
        // ONE point per entry on the class-A arms: an entry is a pair of
        // integers, narrower than the `WinSample` the unit is defined by,
        // and class A never re-reduces over a scratch (`per_sample == 1`
        // for every integer op anyway).
        match &mut group.cells {
            // Issue #236 Part C, C1: two touches per SAMPLE — the deltas
            // at `k_lo` and at the exclusive `k_hi + 1` — instead of one
            // per covered cell. `k_hi + 1` may be `kmax + 1`, which is a
            // delta index only and is never emitted.
            MutCells::IntDeltas(cells) => {
                for (k, dv, dc) in [(k_lo, value as i64, 1i64), (k_hi + 1, -(value as i64), -1)] {
                    match cells.entry(k) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            let d = e.get_mut();
                            d.dvalue += dv;
                            d.dcount += dc;
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            charge_retention(retained, 1, cap)?;
                            e.insert(IntDelta {
                                dvalue: dv,
                                dcount: dc,
                            });
                        }
                    }
                }
            }
            MutCells::IntExpanded(cells) => {
                for k in k_lo..=k_hi {
                    match cells.entry(k) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            *e.get_mut() += value as u64;
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            charge_retention(retained, 1, cap)?;
                            e.insert(value as u64);
                        }
                    }
                }
            }
            // Issue #236 Part C, C2: classes B/C retain each surviving
            // sample ONCE. The covering cells are recovered at finish by
            // a two-pointer sweep, so retention no longer scales with
            // `ceil(range/step)`.
            MutCells::Samples(samples) => {
                charge_retention(retained, per_sample, cap)?;
                samples.push(WinSample {
                    ts,
                    stream_hash,
                    tie_rank,
                    value,
                });
            }
        }
        Ok(())
    }

    /// The grid-index range `[k_lo, k_hi]` (clamped to `[0, kmax]`) of grid
    /// points whose window `(grid_t-range, grid_t]` covers `ts`:
    /// `grid_t-range < ts ≤ grid_t` ⟺ `ts ≤ grid_t < ts+range`.
    fn covering_k(&self, ts: i64) -> (i64, i64) {
        covering_k_of(ts, self.grid_start, self.step, self.range, self.kmax)
    }

    fn grid_point(&self, k: i64) -> i64 {
        // i128 intermediate + a SATURATING narrowing (issue #227 arithmetic
        // sweep): `k <= kmax` derives from `grid_point_count`, so every grid
        // point is in `[grid_start, end]` and the clamp never fires on a
        // real query — but a plain `as i64` would silently wrap rather than
        // saturate if that invariant were ever broken.
        clamp_bucket(self.grid_start as i128 + k as i128 * self.step as i128)
    }

    /// Finalizes into the query result, asserting the concurrent-retention
    /// invariant closed out (every charge discharged).
    fn finish(mut self) -> Result<QueryResult, ReadError> {
        let out = self.finish_in_place()?;
        debug_assert_eq!(
            self.retained, 0,
            "every retention charge must be discharged at finish"
        );
        debug_assert_eq!(
            self.group_bytes, 0,
            "every group-label byte charge must be discharged at finish"
        );
        Ok(out)
    }

    /// The finish body, in place so the post-condition (`retained == 0`) is
    /// OBSERVABLE — `finish` consumes `self`, which made the release leg of
    /// the AC8 test unfalsifiable (issue #227 review round 3, finding 3).
    fn finish_in_place(&mut self) -> Result<QueryResult, ReadError> {
        // `base_labels` moved to a local (as in `push_rows`) so the final
        // `&mut self` flush does not clash with a `self.base_labels` borrow.
        let base_labels = std::mem::take(&mut self.base_labels);
        self.flush_collision(&base_labels)?;
        if let Some(prev) = self.cur.take() {
            self.rotate_slider(prev);
        }
        if self.is_absent {
            let series = self.finish_absent()?;
            return self.emit(series);
        }
        if self.fan_out {
            // **The reduction-order pin, extended to the fold** (issue
            // #236, task-manager ruling "Option B"). `groups` is a
            // `HashMap` under a per-process randomly-seeded hasher, so
            // draining it directly would hand the fold a member order
            // that varies run to run — and Welford, and float addition,
            // are order-SENSITIVE. Sorting by label set here is the same
            // total order `pin_reduction_order` applies immediately
            // before grouping on the materialising path, so the folded
            // and materialised values are the same bits, and both are
            // reproducible. One sort per stage, never per group.
            //
            // Applied whether or not a fold is attached — unlike
            // `emit`'s, which is inside the folding arm. A fan-out result
            // came out in `HashMap` walk order before, which no test
            // could assert and no user could rely on; making it
            // label-ascending is the same order the wire already carries
            // everywhere else.
            let mut groups: Vec<(String, MutGroup)> =
                std::mem::take(&mut self.groups).into_iter().collect();
            groups.sort_by(|(_, a), (_, b)| a.labels.cmp(&b.labels));
            let mut out: Vec<MatrixSeries> = Vec::new();
            for (key, group) in groups {
                // Round 6 symmetry: the discharge is sized over the SAME
                // (unmodified) key + labels the insertion charged — the key
                // is immutable as the map key and `MutGroup.labels` is never
                // touched after creation — so finish returns `group_bytes`
                // to exactly 0. Taken AFTER the entry is consumed below (key
                // dropped, labels moved into the output series), mirroring
                // the cell discharges: a charge is never released while the
                // memory it paid for is still owned by this state.
                let entry_bytes = group_entry_bytes(&key, &group.labels, MUT_GROUP_SLOT);
                drop(key);
                let (labels, points) = self.drain_group(group);
                if !points.is_empty() {
                    // Issue #236 Part B: the fold consumes each group AS
                    // it is built, so the `scanned groups x grid points`
                    // materialisation that `out` used to accumulate never
                    // exists — that is the whole point-axis win.
                    match self.fold.as_mut() {
                        Some(fold) => fold.push_series(&labels, &points)?,
                        None => out.push(MatrixSeries { labels, points }),
                    }
                }
                discharge_group_bytes(&mut self.group_bytes, entry_bytes);
            }
            return Ok(QueryResult::Matrix(match self.fold.take() {
                Some(fold) => fold.finish(),
                None => out,
            }));
        }
        // Issue #236 P2's other discharge leg: every series that reached
        // `series_out` still holds its charge, released as the vector
        // leaves the state. Sized over the same (never-mutated) labels the
        // charge used, so `group_bytes` returns to exactly 0.
        let out = std::mem::take(&mut self.series_out);
        for s in &out {
            discharge_group_bytes(
                &mut self.group_bytes,
                group_entry_bytes("", &s.labels, SERIES_OUT_SLOT),
            );
        }
        self.emit(out)
    }

    /// Drains ONE mutating output group's retained cells into its grid
    /// points, discharging the retention they were charged for (issue
    /// #236 Part C).
    ///
    /// A frame of its own rather than an inline block in
    /// `finish_in_place`: it holds all three [`MutCells`] arms, and the
    /// per-variant allocation census (`logql_variants_alloc`) pins a
    /// FUNCTION's body — folding it into the caller would have made one
    /// 24-branch frame, and hiding it behind an un-censused helper would
    /// have made its allocations invisible to a window that genuinely
    /// executes them (a `variants(...)` sub-state whose pipeline mutates
    /// labels takes exactly this path).
    fn drain_group(&mut self, group: MutGroup) -> (LabelSet, Vec<(i64, f64)>) {
        let mut points: Vec<(i64, f64)> = Vec::new();
        // Every arm drains the group's cells into a `Vec` keyed by
        // grid index so the points come out ordered. The drain is
        // bounded by the cells' own charge (one point per created
        // cell / retained sample) and each element is narrower than
        // the `WinSample` that charge is denominated in — the `Vec`
        // moves, it does not copy. The discharge happens AFTER the
        // cells are consumed, so a charge is never released while
        // the memory it paid for is still live (issue #227 review
        // round 5, finding 2).
        match group.cells {
            MutCells::IntExpanded(cells) => {
                let mut cells: Vec<(i64, u64)> = cells.into_iter().collect();
                let staged = cells.len() as u64;
                cells.sort_by_key(|(k, _)| *k);
                for (k, run_int) in cells {
                    points.push((
                        self.grid_point(k),
                        reduce_int_cell(self.op, self.rate_window_ns, run_int),
                    ));
                }
                discharge_retention(&mut self.retained, staged);
            }
            // Issue #236 Part C, C1: prefix-sum the difference
            // array ascending. Between two consecutive delta
            // indices the running pair is CONSTANT, so a covered
            // run emits one point per cell and an uncovered run is
            // skipped in O(1) — the emitted set and its values are
            // exactly the expanded form's (`covering_k` is the
            // half-open interval each `(+1, -1)` pair spans).
            // `run_count > 0` is the coverage test, which is why
            // it cannot fold into `run_value`: a covered cell of
            // value 0 emits `0`, an uncovered one emits nothing.
            MutCells::IntDeltas(cells) => {
                let mut cells: Vec<(i64, IntDelta)> = cells.into_iter().collect();
                let staged = cells.len() as u64;
                cells.sort_by_key(|(k, _)| *k);
                let (mut run_value, mut run_count) = (0i64, 0i64);
                for i in 0..cells.len() {
                    let (k, d) = cells[i];
                    run_value += d.dvalue;
                    run_count += d.dcount;
                    if run_count <= 0 {
                        continue;
                    }
                    // Constant until the next delta index, and
                    // never past the last grid point.
                    let end = cells
                        .get(i + 1)
                        .map_or(self.kmax + 1, |(next, _)| *next)
                        .min(self.kmax + 1);
                    let run_int = u64::try_from(run_value).unwrap_or(0);
                    let v = reduce_int_cell(self.op, self.rate_window_ns, run_int);
                    for cell in k.max(0)..end {
                        points.push((self.grid_point(cell), v));
                    }
                }
                discharge_retention(&mut self.retained, staged);
            }
            // Issue #236 Part C, C2: one sort per GROUP, then a
            // two-pointer sweep over ascending grid indices. The
            // slice handed to `reduce_window` is the identical
            // element sequence the per-cell form produced —
            // `win_order` is `ts`-major and total on a group's
            // samples, and the window `(t-range, t]` is exactly
            // `covering_k`'s inverse. Cells are visited only where
            // some sample covers them (the merged covering
            // intervals), so an uncovered stretch of grid costs
            // nothing, as it did before.
            MutCells::Samples(mut samples) => {
                let staged = samples.len() as u64;
                samples.sort_by(win_order);
                // `covering_k` is monotone in `ts` and the samples
                // are now `ts`-ascending, so the covering
                // intervals arrive sorted and merging them is ONE
                // pass. Merging is what keeps an uncovered stretch
                // of grid free, exactly as the per-cell map made
                // it free.
                let mut covered: Vec<(i64, i64)> = Vec::new();
                for s in &samples {
                    let (a, b) = self.covering_k(s.ts);
                    if a > b {
                        continue;
                    }
                    match covered.last_mut() {
                        Some(last) if a <= last.1 + 1 => last.1 = last.1.max(b),
                        _ => covered.push((a, b)),
                    }
                }
                let (mut lo, mut hi) = (0usize, 0usize);
                for (a, b) in covered {
                    for cell in a..=b {
                        let t = self.grid_point(cell);
                        while hi < samples.len() && samples[hi].ts <= t {
                            hi += 1;
                        }
                        // `checked_sub` for the same reason
                        // `FpSlide::emit_at` uses it: an eviction
                        // bound below the representable domain
                        // evicts nothing.
                        if let Some(bound) = t.checked_sub(self.range) {
                            while lo < hi && samples[lo].ts <= bound {
                                lo += 1;
                            }
                        }
                        if let Some(v) = reduce_window(
                            self.op,
                            self.class,
                            self.param,
                            self.rate_window_ns,
                            0,
                            &samples[lo..hi],
                        ) {
                            points.push((t, v));
                        }
                    }
                }
                discharge_retention(&mut self.retained, staged * self.per_sample);
            }
        }
        (group.labels, points)
    }

    /// Routes an already-materialised batch of leaf series through the
    /// attached fold (issue #236 Part B), or hands it back unchanged when
    /// nothing folded.
    ///
    /// **The reduction-order pin, extended to the fold.** The batch is put
    /// in label-set order before folding — the same total order
    /// `pin_reduction_order` applies immediately before grouping on the
    /// materialising path — so the folded value is the materialised value
    /// bit for bit, and both are reproducible. The sort is inside the
    /// folding arm so a query that does NOT fold keeps its existing output
    /// order byte for byte.
    ///
    /// Series are consumed one at a time, so a series' points are freed as
    /// they are folded rather than all being held until finish.
    ///
    /// **Residual, recorded so it is not rediscovered as a surprise**
    /// (ledger entry `#236 (c)`). The non-mutating caller has already
    /// materialised `series_out` by the time it gets here, so this arm
    /// does NOT collapse that vector the way the fan-out arm collapses
    /// its groups. Folding at each slider's close instead would, but
    /// sliders complete in FINGERPRINT order — deterministic, and not the
    /// label-set order this pins — so the folded value would stop being
    /// the materialised value. The consequence is deferred, not absent:
    /// once emitted points are charged
    /// ([`MAX_METRIC_RESULT_POINTS`], not yet levied) a non-mutating range
    /// leaf whose `streams x grid points` exceeds the charge is refused
    /// where the reference serves it. The fix is a step-ordered
    /// evaluator (issue #250), not a larger constant.
    fn emit(&mut self, mut series: Vec<MatrixSeries>) -> Result<QueryResult, ReadError> {
        match self.fold.take() {
            None => Ok(QueryResult::Matrix(series)),
            Some(mut fold) => {
                pin_reduction_order(&mut series, |s| &s.labels);
                for s in series {
                    fold.push_series(&s.labels, &s.points)?;
                }
                Ok(QueryResult::Matrix(fold.finish()))
            }
        }
    }

    /// `absent_over_time`: emit `1.0` at every grid point whose selector-wide
    /// window `(t-range, t]` is EMPTY (Loki's one emit-on-empty reducer).
    /// Prefix-sums the O(grid) coverage difference array (review finding 1):
    /// a running sum > 0 means at least one sample covers that grid point.
    ///
    /// Returns the series rather than a [`QueryResult`] so the caller can
    /// route them through the issue #236 Part B fold like every other
    /// emit path; the 0-or-1 shape is unchanged.
    fn finish_absent(&mut self) -> Result<Vec<MatrixSeries>, ReadError> {
        // Issue #236: `absent_over_time` emits at most ONE series, whose
        // points are the grid's empty windows — reserved before the
        // vector is built, like every other emitted series.
        charge_result_points(
            &mut self.result_points,
            grid_slot_count(self.kmax),
            self.caps.result_points,
        )?;
        let mut points: Vec<(i64, f64)> = Vec::new();
        // Same width as the deltas (see `present_cover`'s proof): the running
        // sum is bounded by the total collision-group count, itself bounded by
        // the byte-scan budget — it cannot overflow `i64`.
        let mut running: i64 = 0;
        for k in 0..=self.kmax {
            running += self.present_cover[k as usize];
            if running == 0 {
                points.push((self.grid_point(k), 1.0));
            }
        }
        Ok(if points.is_empty() {
            Vec::new()
        } else {
            vec![MatrixSeries {
                labels: std::mem::take(&mut self.absent_labels),
                points,
            }]
        })
    }
}

/// The pure client-aggregated evaluation (issue M6-10): the slice-driven
/// wrapper over [`ClientAggState`] the hermetic golden/allocation suites
/// pin (the engine streams rows into the same state chunk-wise instead
/// of buffering the scan — review round 1, finding 1).
///
/// Vector aggregations are NOT applied here — the caller finishes them
/// (`apply_vector_aggs`), mirroring the SQL path. Callers that want the
/// engine's ACTUAL sequence, with the innermost aggregation folded at the
/// leaf (issue #236 Part B), use [`run_client_agg_rows_folded`].
pub fn run_client_agg_rows(
    rows: &[MetricScanRow],
    compiled: &super::pipeline::CompiledPipeline,
    meta: &HashMap<u64, StreamMetaRow>,
    client: &ClientAgg,
    window: ClientWindow,
    rate_window_ns: Option<u64>,
) -> Result<QueryResult, ReadError> {
    run_client_agg_rows_folded(rows, compiled, meta, client, window, rate_window_ns, &[])
}

/// [`run_client_agg_rows`] plus the whole vector-aggregation chain, run
/// the way [`LogQlEngine::run_metric_client`] runs it (issue #236 Part
/// B): on a RANGE query the innermost spec is handed to the leaf, which
/// folds it AS it emits, and only the remaining prefix is materialised
/// through [`apply_vector_aggs`]. On an instant query, and for the specs
/// the leaf cannot own (`sort`/`sort_desc`/`approx_topk`), every spec goes
/// to `apply_vector_aggs` — i.e. exactly today's path.
///
/// `pub` so the hermetic suites and the conformance runner drive the same
/// sequence the engine does, rather than a materialising approximation of
/// it: the fold's equivalence with `apply_vector_aggs` is a property that
/// has to be EXERCISED, not asserted.
pub fn run_client_agg_rows_folded(
    rows: &[MetricScanRow],
    compiled: &super::pipeline::CompiledPipeline,
    meta: &HashMap<u64, StreamMetaRow>,
    client: &ClientAgg,
    window: ClientWindow,
    rate_window_ns: Option<u64>,
    aggs: &[plan::VectorAggSpec],
) -> Result<QueryResult, ReadError> {
    if matches!(window, ClientWindow::Range { .. }) {
        // Issue #227: a range query evaluates Loki's sliding windows, which
        // assume fingerprint-contiguous, ascending-ts input (the live
        // engine's `metric_raw_samples_sliding` PK scan guarantees it). The
        // live path already streams in that order; only re-sort (cloning) the
        // pure-slice entry if it is NOT already ordered — so a pre-ordered
        // scan folds with zero per-row allocation.
        let mut state = RangeSlideState::new(
            compiled,
            meta,
            client,
            window,
            rate_window_ns,
            AggCaps::DEFAULT,
        )?;
        if let Some(spec) = aggs.last() {
            state.attach_fold(spec);
        }
        let folded = state.folded_aggs();
        let ordered = rows.windows(2).all(|w| {
            (w[0].fingerprint, w[0].timestamp_ns) <= (w[1].fingerprint, w[1].timestamp_ns)
        });
        if ordered {
            state.push_rows(rows)?;
        } else {
            let mut sorted: Vec<MetricScanRow> = rows.to_vec();
            sorted.sort_by(|a, b| {
                a.fingerprint
                    .cmp(&b.fingerprint)
                    .then(a.timestamp_ns.cmp(&b.timestamp_ns))
            });
            state.push_rows(&sorted)?;
        }
        let result = state.finish()?;
        return Ok(apply_vector_aggs(result, &aggs[..aggs.len() - folded]));
    }
    // The `Range` arm returned above, so this narrowing cannot fail; it
    // is a narrowing rather than an assertion so the instant state cannot
    // be built for a stepped window at all (issue #236 Part D).
    let instant = window
        .as_instant()
        .ok_or_else(|| ReadError::PipelineInvalid {
            reason: "internal: a stepped window reached the instant aggregation state".to_string(),
        })?;
    let mut state = ClientAggState::new(
        compiled,
        meta,
        client,
        instant,
        rate_window_ns,
        AggCaps::DEFAULT,
    )?;
    state.push_rows(rows)?;
    Ok(apply_vector_aggs(state.finish(), aggs))
}

/// The live engine's per-fold metric-aggregation state: the instant
/// single-window [`ClientAggState`] or the issue #227 range sliding
/// evaluator, both driven chunk-wise off the raw scan stream.
#[derive(Debug)]
enum MetricAggState<'q> {
    Instant(Box<ClientAggState<'q>>),
    Range(Box<RangeSlideState<'q>>),
}

impl MetricAggState<'_> {
    fn push_rows(&mut self, rows: &[MetricScanRow]) -> Result<(), ReadError> {
        match self {
            MetricAggState::Instant(s) => s.push_rows(rows),
            MetricAggState::Range(s) => s.push_rows(rows),
        }
    }

    fn finish(self) -> Result<QueryResult, ReadError> {
        match self {
            MetricAggState::Instant(s) => Ok(s.finish()),
            MetricAggState::Range(s) => s.finish(),
        }
    }
}

// =====================================================================
// Issue #221: `variants(<metricExpr>, …) of (<logRangeExpr>)`.
//
// ONE scan with N reducers, never N queries: the common log range plans
// the single raw scan (byte-identical SQL to the equivalent
// single-extractor query, independent of N), and every chunk of that one
// row stream is fanned out in memory to N ordinary sub-states
// (`ClientAggState`/`RangeSlideState`), each governed by
// `AggCaps::DEFAULT.divided(n)` so the per-field TOTAL over sub-states is
// exactly today's single-query bound. Every construction-time allocation
// the fan-out ADDS beyond one ordinary extractor state is charged against
// `MAX_VARIANT_FANOUT_STATE_BYTES` BEFORE it happens (charge-before-
// allocate, #227 discipline) and released as `finish` consumes the state.
// =====================================================================

/// The reference's variant-index label (`__variant__`), set to the plain
/// decimal `index.to_string()` — no padding.
pub const VARIANT_LABEL: &str = "__variant__";

/// The variants fan-out state budget — DERIVED, not chosen: one extra
/// query-lifetime group-bytes budget ([`AggCaps::DEFAULT`]`.group_bytes`
/// == [`MAX_CLIENT_AGG_GROUP_BYTES`]). It moves with that cap. One
/// counter spans plan-time spec state and exec-time arena/sub-state
/// state — the budget is never doubled.
pub const MAX_VARIANT_FANOUT_STATE_BYTES: u64 = AggCaps::DEFAULT.group_bytes;

/// A CLONE of a source stage list, per SOURCE byte `S`
/// ([`stage_source_bytes`]): content ≤ 2S ([`alloc_block_bytes`]'s
/// size-class model) + per-allocation floor ≤ 32S ([`MIN_ALLOC_BYTES`];
/// each allocation needs ≥ 1 source byte — an empty-string clone
/// allocates nothing) + inner element slots
/// ≤ 2 × size_of::<(String, String)>() × S = 96S ⇒ ≤ 130S. Over-charging
/// is the safe direction: a breach is a clean 422, never an OOM.
const STAGE_CLONE_BYTES_PER_SOURCE_BYTE: u64 = 130;

/// A cloned/compiled stage list's retained heap per SOURCE byte — the
/// clone factor above plus the `Vec<CompiledStage>` slot term (the widest
/// arm is `Regexp(regex::Regex)`, so the slot width is DERIVED from the
/// real private enum via [`super::pipeline::COMPILED_STAGE_SLOT_BYTES`],
/// never hard-coded: a flat literal silently under-charges if the enum
/// widens). ≤ 2 slots per source byte covers the exactly-reserved buffer
/// at the [`alloc_block_bytes`] 2× rounding.
const COMPILED_STAGE_BYTES_PER_SOURCE_BYTE: u64 =
    STAGE_CLONE_BYTES_PER_SOURCE_BYTE + 2 * super::pipeline::COMPILED_STAGE_SLOT_BYTES as u64;

/// One `regex::Regex` CLONE's own retained heap: a fresh, lazily
/// populated cache pool — the lazy-DFA cache capacity (2 MiB, the regex
/// crate's default `hybrid_cache_capacity`) plus the meta engine's other
/// per-`Cache` structures (one-pass DFA, backtracker visited set,
/// PikeVM). The compiled PROGRAM is shared through the `Arc`
/// ([`CompiledPipeline::extended_with`]) and is NOT charged again.
const REGEX_CACHE_STATE_BYTES: u64 = 4 * 1024 * 1024;

/// A provable UPPER BOUND on the heap ONE **exactly reserved**
/// (`Vec::with_capacity(n)`) buffer of `n` `T` slots retains. The payload
/// behind any pointer inside `T` is charged SEPARATELY (the C/H split) —
/// this is the container line only.
///
/// No growth term is needed *because* every N-scaled buffer this path
/// introduces is `with_capacity`-reserved before the first push, with `n`
/// known up front; there is no realloc peak. A buffer whose count is NOT
/// known up front must use [`grown_alloc_bytes`] instead
/// (`Grouping::labels`, which `__variant__` is pushed into, does).
pub(crate) fn vec_buffer_bytes<T>(n: u64) -> u64 {
    alloc_block_bytes(n.saturating_mul(size_of::<T>() as u64))
}

/// The four driver-owned N-scaled BUFFERS, charged together as one term
/// BEFORE the first of them is allocated: [`VariantArena`]'s
/// `pipelines`/`slot` and [`VariantsAggState`]'s `subs`/`sub_charged`.
/// Charged in one place because `VariantsAggState` inherits the arena's
/// charge as its floor (`base`), so a single charge at the top of
/// [`VariantArena::build`] provably precedes every one of the four
/// allocations.
fn variant_driver_buffer_bytes(n: u64) -> u64 {
    vec_buffer_bytes::<usize>(n)
        .saturating_add(vec_buffer_bytes::<CompiledPipeline>(n.saturating_add(1)))
        .saturating_add(vec_buffer_bytes::<MetricAggState<'_>>(n))
        .saturating_add(vec_buffer_bytes::<u64>(n))
}

/// Total OWNED-STRING bytes in a SOURCE stage list — the `S` the clone
/// factors above multiply. An exhaustive `match` with **no `_` arm**: a
/// new `Stage` variant is a compile error here, forcing the sizing walk
/// to be re-run for it before it can ship.
pub(crate) fn stage_source_bytes(stages: &[Stage]) -> u64 {
    fn matcher_bytes(m: &Matcher) -> u64 {
        (m.name.len() as u64).saturating_add(m.value.len() as u64)
    }
    fn label_filter_bytes(e: &LabelFilterExpr) -> u64 {
        match e {
            LabelFilterExpr::Match(m) => matcher_bytes(m),
            LabelFilterExpr::Compare { name, rhs, .. } => {
                let rhs_len = match rhs {
                    NumericLiteral::Number(raw) | NumericLiteral::DurationOrBytes(raw) => {
                        raw.len() as u64
                    }
                };
                (name.len() as u64).saturating_add(rhs_len)
            }
            LabelFilterExpr::Ip { name, value, .. } => {
                (name.len() as u64).saturating_add(value.len() as u64)
            }
            LabelFilterExpr::And(a, b) | LabelFilterExpr::Or(a, b) => {
                label_filter_bytes(a).saturating_add(label_filter_bytes(b))
            }
        }
    }
    let mut bytes: u64 = 0;
    for stage in stages {
        let stage_bytes = match stage {
            Stage::LineFilter(lf) => lf
                .alternatives()
                .fold(0u64, |acc, (v, _)| acc.saturating_add(v.len() as u64)),
            Stage::Parser(p) => match p {
                ParserStage::Json { extractions } => extractions.iter().fold(0u64, |acc, e| {
                    acc.saturating_add(e.label.len() as u64)
                        .saturating_add(e.expression.len() as u64)
                }),
                ParserStage::Logfmt { extractions, .. } => {
                    extractions.iter().fold(0u64, |acc, e| {
                        acc.saturating_add(e.label.len() as u64)
                            .saturating_add(e.expression.len() as u64)
                    })
                }
                ParserStage::Regexp(raw) | ParserStage::Pattern(raw) => raw.len() as u64,
            },
            Stage::LabelFilter(expr) => label_filter_bytes(expr),
            Stage::LineFormat(tmpl) => tmpl.len() as u64,
            Stage::LabelFormat(fmts) => fmts.iter().fold(0u64, |acc, f| match f {
                LabelFmt::Rename { dst, src } => acc
                    .saturating_add(dst.len() as u64)
                    .saturating_add(src.len() as u64),
                LabelFmt::Template { dst, tmpl } => acc
                    .saturating_add(dst.len() as u64)
                    .saturating_add(tmpl.len() as u64),
            }),
            Stage::Unwrap(u) => (u.label.len() as u64)
                .saturating_add(u.conversion.as_deref().map_or(0, |c| c.len() as u64)),
            Stage::Unpack | Stage::Decolorize => 0,
            Stage::Drop(elems) | Stage::Keep(elems) => elems.iter().fold(0u64, |acc, e| {
                acc.saturating_add(e.label.len() as u64)
                    .saturating_add(e.matcher.as_ref().map_or(0, |m| m.value.len() as u64))
            }),
        };
        bytes = bytes.saturating_add(stage_bytes);
    }
    bytes
}

/// Regex-compiling stage forms in a SOURCE list (the compiled internals
/// are private to `pipeline.rs`), enumerated from the five
/// `compile_regex`/`compile_anchored_regex` call sites: `LineFilter`
/// alternatives under `|~`/`!~` (non-`ip` heads and `or` alternatives),
/// `| regexp`, label-filter `Match` nodes with `=~`/`!~` (recursing
/// through `and`/`or`), `| decolorize`, and `drop`/`keep` elements with a
/// `=~`/`!~` matcher.
pub(crate) fn regex_stage_count(stages: &[Stage]) -> u64 {
    fn label_filter_regexes(e: &LabelFilterExpr) -> u64 {
        match e {
            LabelFilterExpr::Match(m) => u64::from(matches!(m.op, MatchOp::Re | MatchOp::Nre)),
            LabelFilterExpr::Compare { .. } | LabelFilterExpr::Ip { .. } => 0,
            LabelFilterExpr::And(a, b) | LabelFilterExpr::Or(a, b) => {
                label_filter_regexes(a).saturating_add(label_filter_regexes(b))
            }
        }
    }
    let mut count: u64 = 0;
    for stage in stages {
        let stage_count = match stage {
            Stage::LineFilter(lf)
                if matches!(lf.op, LineFilterOp::Regex | LineFilterOp::NotRegex) =>
            {
                lf.alternatives().fold(0u64, |acc, (_, is_ip)| {
                    acc.saturating_add(u64::from(!is_ip))
                })
            }
            Stage::LineFilter(_) => 0,
            Stage::Parser(ParserStage::Regexp(_)) => 1,
            Stage::Parser(_) => 0,
            Stage::LabelFilter(expr) => label_filter_regexes(expr),
            Stage::Decolorize => 1,
            Stage::Drop(elems) | Stage::Keep(elems) => elems.iter().fold(0u64, |acc, e| {
                acc.saturating_add(u64::from(
                    e.matcher
                        .as_ref()
                        .is_some_and(|m| matches!(m.op, MatchOp::Re | MatchOp::Nre)),
                ))
            }),
            Stage::LineFormat(_) | Stage::LabelFormat(_) | Stage::Unwrap(_) | Stage::Unpack => 0,
        };
        count = count.saturating_add(stage_count);
    }
    count
}

/// The charge for ONE additional [`VariantArena`] entry (a distinct
/// non-empty unwrap tail): the cloned common stages + compiled tail
/// (source-byte factors) plus one fresh regex cache pool per regex in
/// EITHER list — the compiled programs themselves are `Arc`-shared by
/// [`CompiledPipeline::extended_with`] and never recompiled.
pub(crate) fn variant_pipeline_entry_bytes(common: &[Stage], tail: &[Stage]) -> u64 {
    stage_source_bytes(common)
        .saturating_add(stage_source_bytes(tail))
        .saturating_mul(COMPILED_STAGE_BYTES_PER_SOURCE_BYTE)
        .saturating_add(
            regex_stage_count(common)
                .saturating_add(regex_stage_count(tail))
                .saturating_mul(REGEX_CACHE_STATE_BYTES),
        )
}

/// EXACT bytes one variants sub-state's construction-time `meta` snapshot
/// retains, with the C/H split explicit — per map, the TABLE share and
/// the element payload are separate terms:
///
/// - `base_labels`: `entries × map_entry_bytes(size_of::<(u64, LabelSet)>())`
///   (C) + `Σ label_set_bytes(labels)` (H);
/// - `hashes` (the sliding kind only): `entries ×
///   map_entry_bytes(size_of::<(u64, u64)>())` (C; the payload is `Copy`).
///
/// Walked over the FIRST sub-state's ALREADY-BUILT maps, so the sizing
/// pass is one O(streams) traversal with no re-parse and no allocation,
/// and runs only when a query declares ≥ 2 variants.
fn variant_meta_snapshot_bytes(base_labels: &HashMap<u64, LabelSet>, with_hashes: bool) -> u64 {
    let mut bytes: u64 = 0;
    for labels in base_labels.values() {
        bytes = bytes
            .saturating_add(map_entry_bytes(size_of::<(u64, LabelSet)>()))
            .saturating_add(label_set_bytes(labels));
    }
    if with_hashes {
        bytes = bytes.saturating_add(
            (base_labels.len() as u64).saturating_mul(map_entry_bytes(size_of::<(u64, u64)>())),
        );
    }
    bytes
}

/// The per-sub-state charge, for sub-state index ≥ 1 (index 0 costs
/// exactly what the equivalent single-extractor query already allocates
/// and is charged ZERO — so a 1-variant query is admitted exactly when
/// the plain query is): the boxed state slot (the kind actually built),
/// the `meta` snapshot, the range kind's construction-time
/// `absent_labels` clone (zero when the list is empty — an empty `Vec`
/// clone allocates nothing), and `present_cover` for an
/// `absent_over_time` range sub-state (structurally absent otherwise).
fn variant_state_bytes(
    is_range: bool,
    is_absent: bool,
    absent_labels: &LabelSet,
    meta_bytes: u64,
    kmax: i64,
) -> u64 {
    let slot = if is_range {
        alloc_block_bytes(size_of::<RangeSlideState<'_>>() as u64)
    } else {
        alloc_block_bytes(size_of::<ClientAggState<'_>>() as u64)
    };
    let mut bytes = slot.saturating_add(meta_bytes);
    if is_range && !absent_labels.is_empty() {
        bytes = bytes.saturating_add(label_set_bytes(absent_labels));
    }
    if is_range && is_absent {
        bytes = bytes.saturating_add(alloc_block_bytes(8 * (kmax.max(-1) + 2) as u64));
    }
    bytes
}

/// Every byte one [`plan::VariantSpec`] retains, sized from BORROWED AST
/// pieces the planner already holds — no container is constructed to
/// compute this bound, and the whole charge precedes the first clone
/// (issue #221 review rounds 4–5).
///
/// **The walk that produced this bound (W-MEM, verbatim so it is
/// re-runnable).** The accounting domain is the OWNED-TYPE CLOSURE of the
/// per-variant roots (`VariantSpec`, `VariantArena`, `VariantsAggState`,
/// `ClientAggState`, `RangeSlideState`) **plus their construction path**
/// (every function executed en route to producing them, walked in call
/// order, each allocating statement classified). Buckets: (S) `Copy`
/// scalar in the slot; (B) borrowed; (C) container buffer/table — charged
/// via [`vec_buffer_bytes`]/[`map_entry_bytes`]/[`grown_alloc_bytes`];
/// (H) element payload — charged before allocation; (G) grows during the
/// scan — governed by a **divided** [`AggCaps`] field. Residues (an
/// explicit byte bound, or N-neutrality, never a bare count): **R1** the
/// once-per-query common-range plan products (one synthetic
/// `MetricExpr::Range` + `metric_plan`'s usual allocations — zero slope
/// in N, so outside a per-variant boundary); **R2** per-fingerprint
/// transients inside the sub-state constructors (`series_labels`
/// scratch) — sub-states are constructed sequentially, so the peak is
/// N-independent and the retained result is charged by
/// [`variant_meta_snapshot_bytes`]; **R4** the three no-count-band
/// branches (meta hydration, a distinct-tail arena compile, an
/// aggregation-bearing `apply_vector_aggs`), named with their
/// compensating controls in `tests/logql_variants_alloc.rs`.
///
/// Terms, in table order (each isolated by a one-axis fixture pair in
/// the I-series tests):
/// - the tail buffer (C) + its `Stage` payload (H, the clone factor);
/// - the `absent_labels` buffer + strings (C+H, `absent_over_time` only —
///   Δ2: sourced from the VARIANT's own selector);
/// - the `vector_aggs` buffer (C — [`grown_alloc_bytes`], because
///   `Result<Vec<_>>`-collect grows by pushes, never one reservation);
/// - per vector layer, grouping present OR CREATED (member M3):
///   the `Grouping::labels` buffer ([`grown_alloc_bytes`] over `len + 1` —
///   `__variant__` is PUSHED into it, the one N-scaled buffer that
///   reallocs) + each cloned label + the injected `__variant__` string.
pub(crate) fn variant_spec_bytes(
    tail: &[Stage],
    selector: &StreamSelector,
    is_absent: bool,
    agg_chain: &MetricExpr,
) -> u64 {
    let mut bytes: u64 = 0;
    if !tail.is_empty() {
        bytes = bytes
            .saturating_add(vec_buffer_bytes::<Stage>(tail.len() as u64))
            .saturating_add(
                stage_source_bytes(tail).saturating_mul(STAGE_CLONE_BYTES_PER_SOURCE_BYTE),
            );
    }
    if is_absent {
        let mut eq_count: u64 = 0;
        for m in selector.matchers.iter().filter(|m| m.op == MatchOp::Eq) {
            eq_count += 1;
            bytes = bytes
                .saturating_add(alloc_block_bytes(m.name.len() as u64))
                .saturating_add(alloc_block_bytes(m.value.len() as u64));
        }
        if eq_count > 0 {
            bytes = bytes.saturating_add(vec_buffer_bytes::<(String, String)>(eq_count));
        }
    }
    let mut layers: u64 = 0;
    let mut cur = agg_chain;
    while let MetricExpr::Vector {
        grouping, inner, ..
    } = cur
    {
        layers += 1;
        let declared = grouping.as_ref().map_or(0, |g| g.labels.len() as u64);
        bytes = bytes.saturating_add(grown_alloc_bytes(
            (declared + 1).saturating_mul(size_of::<String>() as u64),
        ));
        if let Some(g) = grouping {
            for l in &g.labels {
                bytes = bytes.saturating_add(alloc_block_bytes(l.len() as u64));
            }
        }
        bytes = bytes.saturating_add(alloc_block_bytes(VARIANT_LABEL.len() as u64));
        cur = inner;
    }
    if layers > 0 {
        bytes = bytes.saturating_add(grown_alloc_bytes(
            layers.saturating_mul(size_of::<plan::VectorAggSpec>() as u64),
        ));
    }
    bytes
}

/// The one variants fan-out gate: every charged allocation is sized,
/// checked against the cap, and accounted HERE — before the allocation it
/// pays for. Mirrors [`charge_group_bytes`] exactly (same shape, same
/// `saturating_add` rationale, same post-condition); returns the
/// `(next, cap)` pair on breach so each caller wraps it in ITS reason
/// ([`TooBroadReason::VariantSpecBytes`] at plan time,
/// [`TooBroadReason::VariantStateBytes`] at exec time) without a second
/// implementation.
pub(crate) fn charge_fanout_bytes(
    charged: &mut u64,
    bytes: u64,
    cap: u64,
) -> Result<(), (u64, u64)> {
    let next = charged.saturating_add(bytes);
    if next > cap {
        return Err((next, cap));
    }
    *charged = next;
    Ok(())
}

/// Releases a [`charge_fanout_bytes`] charge as `finish` consumes the
/// state it paid for. Saturating for panic-proofing only — the pairing is
/// exact, which `finish`'s `charged == base` post-condition asserts.
fn discharge_fanout_bytes(charged: &mut u64, bytes: u64) {
    *charged = charged.saturating_sub(bytes);
}

fn variant_state_breach((bytes, cap): (u64, u64)) -> ReadError {
    ReadError::QueryTooBroad(TooBroadReason::VariantStateBytes { bytes, cap })
}

/// The compiled-pipeline arena for ONE variants query (issue #221).
/// Entry 0 is the COMMON pipeline; each further entry is
/// `entry0.extended_with(tail)` for one DISTINCT non-empty unwrap tail.
/// Built and OWNED by the driver BEFORE [`VariantsAggState`], which only
/// BORROWS from it — the arena and its borrowers are never owned by the
/// same struct, so there is no self-reference.
#[derive(Debug)]
pub struct VariantArena {
    pipelines: Vec<CompiledPipeline>,
    slot: Vec<usize>,
    charged: u64,
}

impl VariantArena {
    /// ORDER (normative — every allocation is preceded by its charge):
    /// 1. count gate (defense in depth — the planner already gated it;
    ///    `build` is reachable from tests and the pure
    ///    [`run_variants_rows`] seam);
    /// 2. ONE [`variant_driver_buffer_bytes`] charge covering all four
    ///    driver buffers, levied before any of them exists
    ///    (`base_charged` = the planner's `spec_bytes`, so plan-time and
    ///    exec-time share one counter and one cap);
    /// 3. the two exact reservations;
    /// 4. entry 0 (`compile(common)`), charged ZERO — a single-extractor
    ///    query pays it too;
    /// 5. per variant with a NON-EMPTY tail, in index order: dedup by
    ///    backward scan over the TAIL slices already processed (no
    ///    auxiliary container, and `common ++ tail` is NEVER materialized
    ///    to compare — that would be one allocation per variant); on a
    ///    miss, charge [`variant_pipeline_entry_bytes`] BEFORE
    ///    `extended_with`.
    pub fn build(
        common: &[Stage],
        variants: &[plan::VariantSpec],
        cap: u64,
        base_charged: u64,
    ) -> Result<Self, ReadError> {
        let n = variants.len() as u64;
        if n > plan::MAX_VARIANT_SUB_STATES {
            return Err(ReadError::QueryTooBroad(TooBroadReason::VariantSubStates {
                count: n,
                cap: plan::MAX_VARIANT_SUB_STATES,
            }));
        }
        let mut charged = base_charged;
        // AC 14: at N = 1 the arena charge is exactly 0 — a 1-variant
        // query is admitted exactly when the equivalent single-extractor
        // query is; the driver buffers are charged only when the fan-out
        // is real (N >= 2).
        if n >= 2 {
            charge_fanout_bytes(&mut charged, variant_driver_buffer_bytes(n), cap)
                .map_err(variant_state_breach)?;
        }
        let mut pipelines: Vec<CompiledPipeline> = Vec::with_capacity(variants.len() + 1);
        let mut slot: Vec<usize> = Vec::with_capacity(variants.len());
        pipelines.push(CompiledPipeline::compile(common)?);
        for (i, spec) in variants.iter().enumerate() {
            let tail = &spec.client().pipeline;
            if tail.is_empty() {
                slot.push(0);
                continue;
            }
            // Dedup key is the variant's TAIL slice alone: entry_i ==
            // entry0.extended_with(tail_i), so equal tails give equal
            // entries (the common prefix is fixed for the query).
            let mut shared = None;
            for j in 0..i {
                if variants[j].client().pipeline == *tail {
                    shared = Some(slot[j]);
                    break;
                }
            }
            if let Some(s) = shared {
                slot.push(s);
                continue;
            }
            charge_fanout_bytes(
                &mut charged,
                variant_pipeline_entry_bytes(common, tail),
                cap,
            )
            .map_err(variant_state_breach)?;
            let extended = pipelines[0].extended_with(tail)?;
            pipelines.push(extended);
            slot.push(pipelines.len() - 1);
        }
        Ok(VariantArena {
            pipelines,
            slot,
            charged,
        })
    }

    /// The compiled pipeline variant `variant_index` runs (`common ++
    /// tail`, shared for empty/duplicate tails).
    fn get(&self, variant_index: usize) -> &CompiledPipeline {
        &self.pipelines[self.slot.get(variant_index).copied().unwrap_or(0)]
    }

    pub fn charged_bytes(&self) -> u64 {
        self.charged
    }

    pub fn len(&self) -> usize {
        self.pipelines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pipelines.is_empty()
    }
}

/// The variants driver: ONE scan, N sub-states (issue #221). Owns the
/// per-sub-state charges and releases them as `finish` consumes each
/// sub-state.
#[derive(Debug)]
pub struct VariantsAggState<'q> {
    arena: &'q VariantArena,
    variants: &'q [plan::VariantSpec],
    subs: Vec<MetricAggState<'q>>,
    sub_charged: Vec<u64>,
    /// Running fan-out charge; starts at (and returns to) `base`.
    charged: u64,
    /// The arena's charge (which already includes the planner's
    /// `spec_bytes` and the driver buffers) — the floor `finish` returns
    /// to.
    base: u64,
    cap: u64,
}

impl<'q> VariantsAggState<'q> {
    /// Builds sub-state 0 uncharged (a 1-variant query is admitted
    /// exactly when the equivalent single-extractor query is), sizes the
    /// `meta` snapshot from ITS already-built maps, then charges
    /// [`variant_state_bytes`] BEFORE constructing each further
    /// sub-state. The state kind comes from each spec's window (all
    /// variants share the grid, so all are the same kind); every
    /// sub-state gets `AggCaps::DEFAULT.divided(n)`, so the per-field SUM
    /// over sub-states is exactly the single-query bound.
    pub fn new(
        arena: &'q VariantArena,
        variants: &'q [plan::VariantSpec],
        meta: &HashMap<u64, StreamMetaRow>,
        cap: u64,
    ) -> Result<Self, ReadError> {
        let n = variants.len() as u64;
        let caps = AggCaps::DEFAULT.divided(n.max(1));
        let base = arena.charged_bytes();
        let mut charged = base;
        let mut subs: Vec<MetricAggState<'q>> = Vec::with_capacity(variants.len());
        let mut sub_charged: Vec<u64> = Vec::with_capacity(variants.len());
        let mut meta_bytes: u64 = 0;
        for (i, spec) in variants.iter().enumerate() {
            let compiled = arena.get(i);
            let is_range = matches!(spec.window(), ClientWindow::Range { .. });
            let is_absent = matches!(spec.client().range_op, RangeAggOp::AbsentOverTime);
            if i > 0 {
                let kmax = match spec.window() {
                    ClientWindow::Range {
                        grid_start_ns,
                        end_ns,
                        step_ns,
                        ..
                    } => grid_point_count(grid_start_ns, end_ns, step_ns.as_u64()) as i64 - 1,
                    ClientWindow::Instant { .. } => -1,
                };
                let bytes = variant_state_bytes(
                    is_range,
                    is_absent,
                    &spec.client().absent_labels,
                    meta_bytes,
                    kmax,
                );
                charge_fanout_bytes(&mut charged, bytes, cap).map_err(variant_state_breach)?;
                sub_charged.push(bytes);
            } else {
                sub_charged.push(0);
            }
            let state = if is_range {
                MetricAggState::Range(Box::new(RangeSlideState::new(
                    compiled,
                    meta,
                    spec.client(),
                    spec.window(),
                    spec.rate_window_ns(),
                    caps,
                )?))
            } else {
                let instant =
                    spec.window()
                        .as_instant()
                        .ok_or_else(|| ReadError::PipelineInvalid {
                            reason: "internal: a stepped variant window reached the instant \
                                     aggregation state"
                                .to_string(),
                        })?;
                MetricAggState::Instant(Box::new(ClientAggState::new(
                    compiled,
                    meta,
                    spec.client(),
                    instant,
                    spec.rate_window_ns(),
                    caps,
                )?))
            };
            if i == 0 {
                // Sized from the FIRST sub-state's already-built maps —
                // one O(streams) walk, no re-parse, no allocation; the
                // hashes half exists only on the sliding kind.
                meta_bytes = match &state {
                    MetricAggState::Range(s) => variant_meta_snapshot_bytes(&s.base_labels, true),
                    MetricAggState::Instant(s) => {
                        variant_meta_snapshot_bytes(&s.base_labels, false)
                    }
                };
            }
            subs.push(state);
        }
        Ok(VariantsAggState {
            arena,
            variants,
            subs,
            sub_charged,
            charged,
            base,
            cap,
        })
    }

    /// Test/gate accessor for the measured-vs-charged slope gates.
    pub fn charged_bytes(&self) -> u64 {
        self.charged
    }

    /// Forwards one chunk to every sub-state in INDEX ORDER (so a
    /// surviving-`__error__` failure is raised by the lowest-indexed
    /// variant — deterministic). A Range sub-state receives the whole
    /// slice (its own sliding `(t-range, t]` windows apply the variant's
    /// `[range]`); an Instant sub-state receives maximal in-window runs
    /// of the SAME slice (no per-variant buffer): the scan is bounded by
    /// the COMMON range only, so a variant with a shorter `[range]` must
    /// exclude the older rows here.
    pub fn push_rows(&mut self, rows: &[MetricScanRow]) -> Result<(), ReadError> {
        for (i, sub) in self.subs.iter_mut().enumerate() {
            match sub {
                MetricAggState::Range(s) => s.push_rows(rows)?,
                MetricAggState::Instant(s) => {
                    let spec = &self.variants[i];
                    let mut k = 0usize;
                    while k < rows.len() {
                        if !spec.admits_instant(rows[k].timestamp_ns) {
                            k += 1;
                            continue;
                        }
                        let start = k;
                        while k < rows.len() && spec.admits_instant(rows[k].timestamp_ns) {
                            k += 1;
                        }
                        s.push_rows(&rows[start..k])?;
                    }
                }
            }
        }
        Ok(())
    }

    /// The finish body, in place so the `charged == base` post-condition
    /// is OBSERVABLE (the `RangeSlideState::finish_in_place` precedent).
    /// Order is NORMATIVE (issue #221): per variant, in index order —
    /// (1) reduce; (2) [`append_variant_label`] on every series
    /// (overriding any common-pipeline `__variant__`); (3) that variant's
    /// (already `__variant__`-injected) vector aggregations — SKIPPED
    /// when it carries none (`apply_vector_aggs` would round-trip every
    /// matrix series' points through a `BTreeMap` for an identity
    /// transform); (4) concatenate in index order into one pre-sized
    /// output; (5) **range only**: drop every series whose label set has
    /// no `__variant__` (reference `engine.go:485-487`); instant keeps
    /// them (`engine.go:620-634`) — the reference's instant/range
    /// asymmetry.
    fn finish_in_place(&mut self) -> Result<QueryResult, ReadError> {
        let is_range = matches!(
            self.variants.first().map(plan::VariantSpec::window),
            Some(ClientWindow::Range { .. })
        );
        let subs = std::mem::take(&mut self.subs);
        let mut per_variant: Vec<QueryResult> = Vec::with_capacity(subs.len());
        for (i, sub) in subs.into_iter().enumerate() {
            let spec = &self.variants[i];
            let mut out = sub.finish()?;
            // Adjudicated correction (issue #221, capture-governed): the
            // reference attaches `__variant__` per EXTRACTED SAMPLE (the
            // consolidated extractor), so an `absent_over_time` variant's
            // SYNTHETIC series never carries it — index-less (and kept)
            // at instant, dropped by the range filter below, grouped as
            // `{}` under an outer aggregation. Container-captured
            // (`b13_variants.test` absent cases); the approved plan's
            // append-to-every-series order was wrong here.
            if !matches!(spec.client().range_op, RangeAggOp::AbsentOverTime) {
                match &mut out {
                    QueryResult::Vector(items) => {
                        for s in items.iter_mut() {
                            append_variant_label(&mut s.labels, spec.index());
                        }
                    }
                    QueryResult::Matrix(items) => {
                        for s in items.iter_mut() {
                            append_variant_label(&mut s.labels, spec.index());
                        }
                    }
                    _ => {}
                }
            }
            let out = if spec.vector_aggs().is_empty() {
                out
            } else {
                apply_vector_aggs(out, spec.vector_aggs())
            };
            // Issue #236: the result-series cap is applied PER VARIANT,
            // before the concat — matching the reference's own granularity
            // (`engine.go:474-506`, `:609-621` apply `maxSeries` per
            // variant, not to the concatenated whole). Strictly more
            // permissive than capping the concat: a 3-variant query
            // returning 400 series each is served (1 200 result series).
            // The remaining divergence is that the reference SKIPS the
            // breaching variant with a warning where PulsusDB 422s —
            // that needs a `warnings` response envelope and is #277.
            ensure_result_series(&out)?;
            discharge_fanout_bytes(&mut self.charged, self.sub_charged[i]);
            per_variant.push(out);
        }
        if is_range {
            let total: usize = per_variant
                .iter()
                .map(|r| match r {
                    QueryResult::Matrix(items) => items.len(),
                    _ => 0,
                })
                .sum();
            let mut out: Vec<MatrixSeries> = Vec::with_capacity(total);
            for r in per_variant {
                if let QueryResult::Matrix(items) = r {
                    out.extend(
                        items
                            .into_iter()
                            .filter(|s| s.labels.iter().any(|(k, _)| k == VARIANT_LABEL)),
                    );
                }
            }
            Ok(QueryResult::Matrix(out))
        } else {
            let total: usize = per_variant
                .iter()
                .map(|r| match r {
                    QueryResult::Vector(items) => items.len(),
                    _ => 0,
                })
                .sum();
            let mut out: Vec<VectorSample> = Vec::with_capacity(total);
            for r in per_variant {
                if let QueryResult::Vector(items) = r {
                    out.extend(items);
                }
            }
            Ok(QueryResult::Vector(out))
        }
    }

    /// Finalizes, asserting every per-sub-state charge was released.
    pub fn finish(mut self) -> Result<QueryResult, ReadError> {
        let out = self.finish_in_place()?;
        debug_assert_eq!(
            self.charged, self.base,
            "every variants fan-out charge must be discharged at finish"
        );
        let _ = self.cap;
        let _ = self.arena;
        Ok(out)
    }
}

/// Sets `__variant__` to the plain decimal `index`, OVERRIDING any
/// existing `__variant__` the common pipeline produced (the reference
/// appends a DUPLICATE label there and then mis-routes samples by
/// re-parsing the two-valued set — provably wrong output, deliberately
/// not reproduced; ledgered), keeping the vector key-sorted. Writes the
/// value `String` ONCE — `set_label_sorted`'s shape with the `String`
/// moved into whichever arm takes it (that helper allocates
/// `value.to_string()` in both arms and is deliberately left untouched).
pub fn append_variant_label(labels: &mut Vec<(String, String)>, index: usize) {
    let value = index.to_string();
    match labels.binary_search_by(|(k, _)| k.as_str().cmp(VARIANT_LABEL)) {
        Ok(i) => labels[i].1 = value,
        Err(i) => labels.insert(i, (VARIANT_LABEL.to_string(), value)),
    }
}

/// The pure, slice-driven variants evaluation — the twin of
/// [`run_client_agg_rows`] the hermetic corpus runner drives, through the
/// SAME [`VariantArena`] + [`VariantsAggState`], so the corpus exercises
/// the identical charging path. Checks order ONCE and sorts ONCE into at
/// most one local `Vec` pushed into every sub-state — it never delegates
/// to `run_client_agg_rows`, whose per-sub-state re-sort would clone
/// every scanned body N times (issue #221 member Δ6.2.4).
pub fn run_variants_rows(
    rows: &[MetricScanRow],
    meta: &HashMap<u64, StreamMetaRow>,
    common: &[Stage],
    variants: &[plan::VariantSpec],
) -> Result<QueryResult, ReadError> {
    let arena = VariantArena::build(common, variants, MAX_VARIANT_FANOUT_STATE_BYTES, 0)?;
    let mut state = VariantsAggState::new(&arena, variants, meta, MAX_VARIANT_FANOUT_STATE_BYTES)?;
    let is_range = matches!(
        variants.first().map(plan::VariantSpec::window),
        Some(ClientWindow::Range { .. })
    );
    if is_range {
        let ordered = rows.windows(2).all(|w| {
            (w[0].fingerprint, w[0].timestamp_ns) <= (w[1].fingerprint, w[1].timestamp_ns)
        });
        if ordered {
            state.push_rows(rows)?;
        } else {
            let mut sorted: Vec<MetricScanRow> = rows.to_vec();
            sorted.sort_by(|a, b| {
                a.fingerprint
                    .cmp(&b.fingerprint)
                    .then(a.timestamp_ns.cmp(&b.timestamp_ns))
            });
            state.push_rows(&sorted)?;
        }
    } else {
        state.push_rows(rows)?;
    }
    state.finish()
}

/// The high, last-resort ClickHouse `max_memory_usage` net for the sliding
/// range read (issue #227): far above any streamed footprint (the read is
/// scan-buffer-bounded, ~tens of MiB, range-independent) so it never gates a
/// normal query — the per-query memory bound is the Rust-side concurrent
/// retention cap, not this. A documented constant (the `DEFAULT_MAX_STREAMS`
/// precedent); promote to `reader.logql_metric_range_max_memory_bytes` only
/// if a deployment needs it.
pub const RANGE_READ_MAX_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;

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
    // Oracle-pinned, re-probed byte-identical at grafana/loki:3.7.4
    // (issue #240 wave0 capture): the "one" side is the source
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
    // Oracle-pinned, re-probed byte-identical at grafana/loki:3.7.4
    // (issue #240 wave0 capture), byte-exact.
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
#[allow(clippy::too_many_arguments)]
fn eval_structured_metadata_row<'a>(
    compiled: &'a super::pipeline::CompiledPipeline,
    body: &'a str,
    merged: &'a [(String, String)],
    sm: &'a StructuredMetadataCtx,
    label_groups: &mut HashMap<String, FanOutGroup>,
    timestamp_ns: i64,
    service: &str,
    mut scratch: LabelScratch<'a>,
) -> (bool, Result<LabelScratch<'a>, ReadError>) {
    let survived = match compiled.run_into_with_sm(body, merged, timestamp_ns, sm, &mut scratch) {
        Ok(Some(line)) => {
            let line = line.into_owned();
            scratch.sort_unstable();
            push_fanout_entry(label_groups, &scratch, timestamp_ns, line, service);
            true
        }
        Ok(None) => false,
        // Template render-budget breach: the whole query fails (bounded
        // 422 — issue #230 follow-up).
        Err(e) => return (false, Err(e.into())),
    };
    // Drop every borrow of `merged` before the buffer is recycled for reuse.
    scratch.clear();
    (survived, Ok(scratch))
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

/// The slot cap `DetectedRowFeeder::trim` applies to each carried `Vec`
/// scratch, and the byte cap it applies to each carried `String` slot
/// (issue #244): a row wider than this still processes fully — the caps
/// bound only what the feeder CARRIES to the next row.
const MAX_FEEDER_SCRATCH_SLOTS: usize = 4096;
const MAX_FEEDER_SCRATCH_STRING_BYTES: usize = 4096;

/// DERIVED from `size_of`, never calibrated: the five-term bound on the
/// heap [`DetectedRowFeeder`]'s buffers can carry between rows after
/// `trim()` — `merge_buf` + `sm_buf` (each `MAX_FEEDER_SCRATCH_SLOTS`
/// slots of `(String, String)`), `label_scratch`
/// (`MAX_FEEDER_SCRATCH_SLOTS` slots of `(Cow, Cow)`), and the two
/// `sm_ctx` `String`s (`MAX_FEEDER_SCRATCH_STRING_BYTES` each), every
/// term rounded through [`alloc_block_bytes`]. Content carried by the
/// `String`s inside a trimmed `Vec`'s slots is zero — `trim` clears
/// before capping, so a kept spine is empty.
const fn feeder_scratch_bytes() -> u64 {
    let pair = (MAX_FEEDER_SCRATCH_SLOTS * size_of::<(String, String)>()) as u64;
    let cow =
        (MAX_FEEDER_SCRATCH_SLOTS * size_of::<(Cow<'static, str>, Cow<'static, str>)>()) as u64;
    2 * alloc_block_bytes(pair)
        + alloc_block_bytes(cow)
        + 2 * alloc_block_bytes(MAX_FEEDER_SCRATCH_STRING_BYTES as u64)
}

/// The published bound on what the detected-fields feeder carries between
/// rows (issue #244, claim C1) — `1_196_032 B` on 64-bit targets, gated
/// by `detected_fields_witness.rs`.
pub const MAX_FEEDER_SCRATCH_BYTES: u64 = feeder_scratch_bytes();

/// The per-row scratch state both detected-fields read paths stream
/// through (issue #244) — one row is live at a time; the carried
/// capacity is bounded by [`MAX_FEEDER_SCRATCH_BYTES`] via `trim()` on
/// every exit path of `feed_row`.
///
/// Two rules (`&mut Vec<T>` is invariant in `T` — see
/// `eval_structured_metadata_row`'s rationale): **R1** every scratch
/// stored in a struct is `LabelScratch<'static>`; **R2** a scratch
/// crosses a lifetime only by MOVE (`mem::take`, by-value,
/// [`recycle_label_scratch`]).
#[derive(Debug, Default)]
struct DetectedRowFeeder {
    merge_buf: Vec<(String, String)>,
    sm_buf: Vec<(String, String)>,
    sm_ctx: StructuredMetadataCtx,
    /// ONE scratch serves both the pipeline pass and the auto-parse pass
    /// (issue #244) — `run_into_with_sm` returns a `Cow` over
    /// `body`/`self`, never over `labels`.
    label_scratch: LabelScratch<'static>,
}

impl DetectedRowFeeder {
    /// All buffers empty; no allocation.
    fn new() -> Self {
        Self::default()
    }

    /// Clears every reusable buffer and DROPS any whose capacity exceeds
    /// the cap. Called on EVERY exit path of `feed_row` — one return
    /// point, after it. The ONLY place capacity is released.
    fn trim(&mut self) {
        fn trim_vec<T>(v: &mut Vec<T>) {
            v.clear();
            if v.capacity() > MAX_FEEDER_SCRATCH_SLOTS {
                *v = Vec::new();
            }
        }
        fn trim_str(s: &mut String) {
            s.clear();
            if s.capacity() > MAX_FEEDER_SCRATCH_STRING_BYTES {
                *s = String::new();
            }
        }
        trim_vec(&mut self.merge_buf);
        trim_vec(&mut self.sm_buf);
        trim_vec(&mut self.label_scratch);
        trim_str(&mut self.sm_ctx.err);
        trim_str(&mut self.sm_ctx.details);
        self.sm_ctx.has_ordinary = false;
    }

    /// The [`alloc_block_bytes`]-rounded heap the five carried buffers
    /// hold right now — the quantity [`MAX_FEEDER_SCRATCH_BYTES`] bounds
    /// after every `feed_row`.
    fn scratch_capacity_bytes(&self) -> u64 {
        alloc_block_bytes((self.merge_buf.capacity() * size_of::<(String, String)>()) as u64)
            .saturating_add(alloc_block_bytes(
                (self.sm_buf.capacity() * size_of::<(String, String)>()) as u64,
            ))
            .saturating_add(alloc_block_bytes(
                (self.label_scratch.capacity()
                    * size_of::<(Cow<'static, str>, Cow<'static, str>)>()) as u64,
            ))
            .saturating_add(alloc_block_bytes(self.sm_ctx.err.capacity() as u64))
            .saturating_add(alloc_block_bytes(self.sm_ctx.details.capacity() as u64))
    }

    /// Streams ONE sampled row through the pipeline into `acc` and drops
    /// it (issue #244): merge SM labels if present, run the pipeline, and
    /// on survival observe SM pairs (re-parsed into the merge-drained
    /// `sm_buf`), pipeline-extracted pairs, then the auto-parse pass.
    /// `Ok(true)` = the row survived the pipeline (counts toward
    /// `line_limit`); a fingerprint that never hydrated is `Ok(false)`.
    /// An `Err` is the #230 template render-budget breach — the whole
    /// query fails, exactly as before.
    #[allow(clippy::too_many_arguments)]
    fn feed_row(
        &mut self,
        fingerprint: u64,
        timestamp_ns: i64,
        body: &str,
        structured_metadata: &str,
        base_labels: &HashMap<u64, Vec<(String, String)>>,
        compiled: &super::pipeline::CompiledPipeline,
        acc: &mut FieldAccumulator,
    ) -> Result<bool, ReadError> {
        let result: Result<bool, ReadError> = 'row: {
            let Some(base) = base_labels.get(&fingerprint) else {
                break 'row Ok(false);
            };
            let Self {
                merge_buf,
                sm_buf,
                sm_ctx,
                label_scratch,
            } = self;
            let has_sm = !structured_metadata.is_empty();
            if has_sm {
                // Drains `sm_buf` into `merge_buf` — leaving `sm_buf`
                // free for the post-survival SM re-parse below.
                merge_labels_with_structured_metadata(
                    base,
                    structured_metadata,
                    merge_buf,
                    sm_buf,
                    sm_ctx,
                );
            }
            let run_base: &[(String, String)] = if has_sm { &*merge_buf } else { base };
            let sm: &StructuredMetadataCtx = if has_sm {
                &*sm_ctx
            } else {
                &EMPTY_STRUCTURED_METADATA
            };
            let sm_reparse: Option<(&str, &mut Vec<(String, String)>)> = if has_sm {
                Some((structured_metadata, sm_buf))
            } else {
                None
            };
            let scratch = std::mem::take(label_scratch); // MOVE OUT (R2)
            let (survived, used) = observe_detected_row(
                compiled,
                body,
                run_base,
                timestamp_ns,
                sm,
                sm_reparse,
                acc,
                scratch,
            );
            match used {
                Ok(returned) => {
                    *label_scratch = returned;
                    Ok(survived)
                }
                Err(e) => break 'row Err(e),
            }
        };
        self.trim();
        result
    }
}

/// Runs one sampled row through the query pipeline and, when it
/// survives, feeds the [`FieldAccumulator`] (issue #170): structured-
/// metadata pairs first (no parser attribution), then pipeline-extracted
/// keys not present in the merged base (no parser attribution;
/// `__error__`/`__error_details__` excluded inside `observe_pair`), then
/// json-first/logfmt-fallback auto-detection on the POST-pipeline line.
/// `sm: &'a StructuredMetadataCtx` is the #238 signature, preserved.
///
/// Two changes vs the pre-#244 shape; their effect on per-row allocation
/// is measured by AC 13 of the #244 plan and stated only there — see
/// **C2 (Q1+Q2)**, plan §A:
///  * ONE scratch serves both the pipeline pass and the auto-parse pass —
///    `run_into_with_sm` returns a `Cow<'a, str>` over `body`/`self`, NOT
///    over `labels`;
///  * the SM pairs are RE-PARSED into the (merge-drained) `sm_buf`
///    instead of a third owned buffer. Observation ORDER unchanged — SM
///    pairs, then pipeline pairs, then auto-parse — and the re-parse runs
///    only when the row SURVIVED
///    (`detected_fields_matched_count_is_post_pipeline_dropped_rows_do_not_count`).
///    Parse count per SM row: 2 before → 1 + 1-if-surviving.
///
/// `scratch` is taken by value and returned for recycling — same
/// per-row-lifetime rationale as [`eval_structured_metadata_row`].
#[allow(clippy::too_many_arguments)]
fn observe_detected_row<'a>(
    compiled: &'a super::pipeline::CompiledPipeline,
    body: &'a str,
    run_base: &'a [(String, String)],
    ts_ns: i64,
    sm: &'a StructuredMetadataCtx,
    sm_reparse: Option<(&str, &mut Vec<(String, String)>)>,
    acc: &mut FieldAccumulator,
    scratch: LabelScratch<'static>,
) -> (bool, Result<LabelScratch<'static>, ReadError>) {
    let mut scratch: LabelScratch<'a> = scratch; // 'static -> 'a by covariance
    let line = match compiled.run_into_with_sm(body, run_base, ts_ns, sm, &mut scratch) {
        Ok(Some(line)) => line,
        Ok(None) => {
            scratch.clear();
            return (false, Ok(recycle_label_scratch(scratch)));
        }
        // Template render-budget breach fails the sampling query too —
        // the bounded 422 (issue #230 follow-up).
        Err(e) => return (false, Err(e.into())),
    };
    if let Some((sm_json, buf)) = sm_reparse {
        // D1 (explanatory, #244 plan §6): the SM re-parse, on survival
        // only.
        buf.clear();
        parse_flat_labels_into(sm_json, buf);
        for (k, v) in buf.iter() {
            acc.observe_pair(k, v, None);
        }
    }
    for (k, v) in scratch.iter() {
        if run_base.iter().any(|(bk, _)| bk.as_str() == k.as_ref()) {
            continue;
        }
        acc.observe_pair(k.as_ref(), v.as_ref(), None);
    }
    // Drop every borrow of `run_base` before the buffer is recycled.
    scratch.clear();
    let scratch: LabelScratch<'static> = recycle_label_scratch(scratch);
    // D2 (explanatory, #244 plan §6): the auto-parse pass.
    (true, Ok(auto_parse_observe(line.as_ref(), acc, scratch)))
}

/// The auto-parse pass over the post-pipeline line, reusing the SAME
/// (recycled) scratch the pipeline pass used (issue #244).
fn auto_parse_observe<'l>(
    line: &'l str,
    acc: &mut FieldAccumulator,
    scratch: LabelScratch<'l>,
) -> LabelScratch<'static> {
    let mut scratch = scratch;
    if let Some(parser) = detected::auto_parse_into(line, &mut scratch) {
        for (k, v) in scratch.iter() {
            acc.observe_pair(k.as_ref(), v.as_ref(), Some(parser));
        }
    }
    scratch.clear();
    recycle_label_scratch(scratch)
}

/// AC 13's baseline (issue #244): a TEST-ONLY TRANSCRIPTION of the
/// pre-#244 per-row shape — the owned `added` vector (plan-pinned
/// `d145ded` `exec.rs:7159-7163`, whole function `:7148-7173`; identical
/// at merge base `a627a6c` modulo the #230 `ts_ns`/`Result` threading
/// transcribed here from the `a627a6c` text) and
/// [`detected::auto_parse_legacy_shape`]'s owned return
/// (`detected.rs:241-254`) — run over the SAME [`DetectedRowFeeder`]
/// state, so the measured difference is the shape change and nothing
/// else.
///
/// SCOPE — **C2 (Q1+Q2)**, #244 plan §A, both qualifiers: Q1, only the
/// four row shapes AC 13 measures; Q2, a comparison between two helpers
/// in THIS tree, not a whole-program before/after against the shipped old
/// binary. AC 13e enforces that these helpers never reach production; it
/// does NOT enforce that they still match `d145ded`. Their fidelity is
/// human-verified against the cited line ranges at implementation time
/// (AC 13f) and recorded in the implementation notes; #244 plan §6.3
/// states what would make it mechanical.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
fn observe_detected_row_legacy_shape<'a>(
    compiled: &'a super::pipeline::CompiledPipeline,
    body: &'a str,
    run_base: &'a [(String, String)],
    ts_ns: i64,
    sm: &'a StructuredMetadataCtx,
    sm_pairs: &[(String, String)],
    acc: &mut FieldAccumulator,
    mut scratch: LabelScratch<'a>,
) -> (bool, Result<LabelScratch<'a>, ReadError>) {
    let survived = match compiled.run_into_with_sm(body, run_base, ts_ns, sm, &mut scratch) {
        Ok(Some(line)) => {
            acc.observe_structured_metadata(sm_pairs);
            let added: Vec<(String, String)> = scratch
                .iter()
                .filter(|(k, _)| !run_base.iter().any(|(bk, _)| bk.as_str() == k.as_ref()))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            acc.observe_parsed(&added, None);
            if let Some((parser, pairs)) = detected::auto_parse_legacy_shape(line.as_ref()) {
                acc.observe_parsed(&pairs, Some(parser));
            }
            true
        }
        Ok(None) => false,
        Err(e) => return (false, Err(e.into())),
    };
    scratch.clear();
    (survived, Ok(scratch))
}

/// Incremental twin of [`advance_tail_cursor`] for the streaming paged
/// loop (issue #244): observes every drained row — including rows past
/// `line_limit` that are not fed; the cursor advances over the RAW page —
/// and folds into the previous cursor at page end. Equivalence over
/// randomized sequences is pinned by AC 17's tests below.
#[derive(Debug)]
struct TailCursorTracker {
    tuple: Option<(i64, u64, u64)>,
    run: u32,
    rows: u32,
}

impl TailCursorTracker {
    fn new() -> Self {
        Self {
            tuple: None,
            run: 0,
            rows: 0,
        }
    }

    /// Called for EVERY drained row.
    fn observe(&mut self, timestamp_ns: i64, fingerprint: u64, body_hash: u64) {
        self.rows = self.rows.saturating_add(1);
        let bt = (timestamp_ns, fingerprint, body_hash);
        match self.tuple {
            Some(t) if t == bt => self.run = self.run.saturating_add(1),
            _ => {
                self.tuple = Some(bt);
                self.run = 1;
            }
        }
    }

    /// `(next cursor, rows drained)` — an empty page keeps `prev`; a
    /// page ending on `prev`'s tuple carries its `seen` (the `OFFSET`
    /// already skipped those), exactly [`advance_tail_cursor`].
    fn finish(self, prev: Option<TailCursor>) -> (Option<TailCursor>, u32) {
        match self.tuple {
            None => (prev, self.rows),
            Some(bt) => {
                let carry = match prev {
                    Some(c) if c.tuple == bt => c.seen,
                    _ => 0,
                };
                (
                    Some(TailCursor {
                        tuple: bt,
                        seen: self.run.saturating_add(carry),
                    }),
                    self.rows,
                )
            }
        }
    }
}

/// The #90 branch split as a pure truth table (issue #244, AC 16c): a
/// `ScanBudgetBytes` overflow AFTER at least one drained page keeps the
/// accumulated prefix (`Ok(true)` = terminate-PARTIAL); on the FIRST page
/// (`spent == 0`) it is a genuinely too-broad query and propagates —
/// HTTP 422, regardless of how many rows were already delivered (the
/// first-page rule; streaming must NOT turn that 422 into a 200). Every
/// other error propagates.
fn classify_page_error(mapped: ReadError, spent: u64) -> Result<bool, ReadError> {
    if spent > 0
        && matches!(
            mapped,
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { .. })
        )
    {
        return Ok(true);
    }
    Err(mapped)
}

/// The streaming paged-loop state (issue #244): everything
/// `run_detected_fields_paged` carries between pages, with the one-page
/// drain factored into [`DetectedPagedState::absorb_page`] so the whole
/// loop body is hermetically testable over injected streams.
#[derive(Debug)]
struct DetectedPagedState {
    feeder: DetectedRowFeeder,
    cursor: Option<TailCursor>,
    spent: u64,
    matched: u32,
    page_size: u32,
    line_limit: u32,
    budget: u64,
}

impl DetectedPagedState {
    /// Drains ONE already-opened page to completion, streaming each row
    /// through the feeder as it arrives, then returns the loop's
    /// decision: `Ok(None)` continue / `Ok(Some(false))`
    /// terminate-COMPLETE / `Ok(Some(true))` terminate-PARTIAL / `Err`
    /// propagate. The drain stops at the FIRST error — exactly what the
    /// pre-#244 per-row `?` did. `line_limit` stops FEEDING, never
    /// DRAINING: `read_bytes` is meaningful only after a full drain
    /// (`wait_end_of_query = 1`) and `fetched < page_size` is the
    /// window-exhaustion terminal. Generic over the stream AND its error
    /// so the public seam never names `ChError`.
    #[allow(clippy::too_many_arguments)]
    async fn absorb_page<S, E>(
        &mut self,
        stream: &mut S,
        read_bytes: impl FnOnce(&S) -> Option<u64>,
        map_err: impl Fn(E) -> ReadError,
        base_labels: &HashMap<u64, Vec<(String, String)>>,
        compiled: &super::pipeline::CompiledPipeline,
        acc: &mut FieldAccumulator,
    ) -> Result<Option<bool>, ReadError>
    where
        S: Stream<Item = Result<TailSampleRow, E>> + Unpin,
    {
        let mut tracker = TailCursorTracker::new();
        let mut page_err: Option<ReadError> = None;
        while let Some(item) = stream.next().await {
            let row = match item {
                Ok(row) => row,
                Err(e) => {
                    page_err = Some(map_err(e));
                    break;
                }
            };
            // Advance over the RAW page, not survivors — a page entirely
            // dropped by the pipeline must never stall the walk.
            tracker.observe(row.timestamp_ns, row.fingerprint, row.body_hash);
            if self.matched >= self.line_limit {
                continue;
            }
            match self.feeder.feed_row(
                row.fingerprint,
                row.timestamp_ns,
                &row.body,
                &row.structured_metadata,
                base_labels,
                compiled,
                acc,
            ) {
                Ok(true) => self.matched += 1,
                Ok(false) => {}
                Err(e) => {
                    page_err = Some(e);
                    break;
                }
            }
        }
        let (cursor, fetched) = tracker.finish(self.cursor.take());
        self.cursor = cursor;
        if let Some(mapped) = page_err {
            // Mid-page prefix retention (issue #244, pinned by AC 16b):
            // rows delivered before the error are already accumulated;
            // the prefix boundary is NOT required to align with a page
            // boundary.
            return classify_page_error(mapped, self.spent).map(Some);
        }
        let read = read_bytes(stream).unwrap_or_else(|| self.budget.saturating_sub(self.spent));
        self.spent = self.spent.saturating_add(read);
        if self.matched >= self.line_limit {
            // Post-pipeline limit filled — complete, never partial.
            return Ok(Some(false));
        }
        if fetched < self.page_size {
            // Window exhausted — complete over the whole window (this is
            // the branch that finds late-occurring matches).
            return Ok(Some(false));
        }
        Ok(None)
    }
}

/// The public hermetic test seam for the #244 streaming detected-fields
/// machinery — accumulator + feeder + paged state, all private inside.
/// `#[doc(hidden)]`: consumed only by `tests/detected_fields_witness.rs`
/// and the `logqltest` corpus runner, never by callers.
#[doc(hidden)]
#[derive(Debug)]
pub struct DetectedFieldsProbe {
    acc: FieldAccumulator,
    state: DetectedPagedState,
    base_labels: HashMap<u64, Vec<(String, String)>>,
    /// The legacy shape's third owned SM-observation buffer — pre-#244
    /// `feed_detected_rows` carried one across a page's rows; held here
    /// (test-only) so `feed_row_legacy_shape` reproduces that steady
    /// state without adding a buffer to the production feeder.
    legacy_sm_obs: Vec<(String, String)>,
}

#[doc(hidden)]
impl DetectedFieldsProbe {
    pub fn new(line_limit: u32, field_limit: u32) -> Self {
        Self::with_byte_budget(line_limit, field_limit, detected::MAX_DETECTED_FIELD_BYTES)
    }

    pub fn with_byte_budget(line_limit: u32, field_limit: u32, retention_budget: u64) -> Self {
        Self {
            acc: FieldAccumulator::with_byte_budget(field_limit, retention_budget),
            state: DetectedPagedState {
                feeder: DetectedRowFeeder::new(),
                cursor: None,
                spent: 0,
                matched: 0,
                page_size: line_limit.max(1),
                line_limit,
                budget: u64::MAX,
            },
            base_labels: HashMap::new(),
            legacy_sm_obs: Vec::new(),
        }
    }

    pub fn add_stream(&mut self, fingerprint: u64, labels: &[(String, String)]) {
        self.base_labels.insert(fingerprint, labels.to_vec());
    }

    pub fn observe_pair(&mut self, key: &str, value: &str, parser: Option<&'static str>) {
        self.acc.observe_pair(key, value, parser);
    }

    /// One row through the PRODUCTION feeder; applies the
    /// `matched >= line_limit` feeding gate.
    pub fn feed_row(
        &mut self,
        compiled: &super::pipeline::CompiledPipeline,
        fingerprint: u64,
        timestamp_ns: i64,
        body: &str,
        structured_metadata: &str,
    ) -> Result<bool, ReadError> {
        if self.state.matched >= self.state.line_limit {
            return Ok(false);
        }
        let survived = self.state.feeder.feed_row(
            fingerprint,
            timestamp_ns,
            body,
            structured_metadata,
            &self.base_labels,
            compiled,
            &mut self.acc,
        )?;
        if survived {
            self.state.matched += 1;
        }
        Ok(survived)
    }

    /// AC 13's baseline: the SAME row and the SAME feeder state through
    /// the pre-#244 per-row OBSERVE shape (the owned `sm_obs` parse, the
    /// owned `added` vector, [`detected::auto_parse_legacy_shape`]'s
    /// owned return — the transcribed `d145ded` ranges). The wrapper
    /// shares the feeder's carry policy (`trim()` on exit) with the new
    /// path deliberately: AC 13's contract is that "the measured
    /// difference is exactly the shape change", and the transcription's
    /// cited ranges are the observe-level helpers — a baseline that also
    /// reverted the #244 carry policy would fold trim's capacity-drop
    /// cost into a comparison that is meant to isolate the per-row
    /// owned-copy shape. Never a production path (AC 13e).
    pub fn feed_row_legacy_shape(
        &mut self,
        compiled: &super::pipeline::CompiledPipeline,
        fingerprint: u64,
        timestamp_ns: i64,
        body: &str,
        structured_metadata: &str,
    ) -> Result<bool, ReadError> {
        if self.state.matched >= self.state.line_limit {
            return Ok(false);
        }
        let Some(base) = self.base_labels.get(&fingerprint) else {
            return Ok(false);
        };
        let feeder = &mut self.state.feeder;
        let has_sm = !structured_metadata.is_empty();
        if has_sm {
            self.legacy_sm_obs.clear();
            parse_flat_labels_into(structured_metadata, &mut self.legacy_sm_obs);
            merge_labels_with_structured_metadata(
                base,
                structured_metadata,
                &mut feeder.merge_buf,
                &mut feeder.sm_buf,
                &mut feeder.sm_ctx,
            );
        }
        let run_base: &[(String, String)] = if has_sm { &feeder.merge_buf } else { base };
        let sm_pairs: &[(String, String)] = if has_sm { &self.legacy_sm_obs } else { &[] };
        let sm: &StructuredMetadataCtx = if has_sm {
            &feeder.sm_ctx
        } else {
            &EMPTY_STRUCTURED_METADATA
        };
        let scratch = std::mem::take(&mut feeder.label_scratch);
        let (survived, used) = observe_detected_row_legacy_shape(
            compiled,
            body,
            run_base,
            timestamp_ns,
            sm,
            sm_pairs,
            &mut self.acc,
            scratch,
        );
        let result = match used {
            Ok(returned) => {
                feeder.label_scratch = recycle_label_scratch(returned);
                Ok(survived)
            }
            Err(e) => Err(e),
        };
        // The shared carry policy (see the doc above): the baseline trims
        // exactly like the new path, so the measured windows differ only
        // in the observe-level shape. (`legacy_sm_obs` is legacy-only
        // state — its untrimmed reuse IS part of the old shape.)
        self.state.feeder.trim();
        let survived = result?;
        if survived {
            self.state.matched += 1;
        }
        Ok(survived)
    }

    /// One injected page through the REAL paged-loop body.
    pub async fn absorb_page<S>(
        &mut self,
        compiled: &super::pipeline::CompiledPipeline,
        stream: &mut S,
        read_bytes: u64,
    ) -> Result<Option<bool>, ReadError>
    where
        S: Stream<Item = Result<TailSampleRow, ReadError>> + Unpin,
    {
        self.state
            .absorb_page(
                stream,
                |_| Some(read_bytes),
                |e| e,
                &self.base_labels,
                compiled,
                &mut self.acc,
            )
            .await
    }

    pub fn matched(&self) -> u32 {
        self.state.matched
    }

    pub fn charged(&self) -> u64 {
        self.acc.charged()
    }

    pub fn peak_charged(&self) -> u64 {
        self.acc.peak_charged()
    }

    pub fn scratch_capacity_bytes(&self) -> u64 {
        self.state.feeder.scratch_capacity_bytes()
    }

    pub fn finish(self) -> (Vec<DetectedFieldOut>, bool) {
        self.acc.finish()
    }
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
    let mut sm_ctx = StructuredMetadataCtx::default();
    for row in sm_rows {
        let Some(m) = meta.get(&row.fingerprint) else {
            continue;
        };
        let base = base_cache
            .entry(row.fingerprint)
            .or_insert_with(|| parse_flat_labels(&m.labels));
        // Merge base + SM (colliding SM keys renamed `_extracted`, per the
        // oracle — no duplicate keys under any collision pattern), then sort for
        // canonical rendering. NO PIPELINE runs on this path, so the
        // reserved-SM materialisation gate is applied here, by
        // `append_visible` (issue #238): a lone `__error_details__` SM entry
        // must not surface (live-probed — the reference's clean-builder fast
        // path skips it), while an `__error__` SM entry must.
        merge_labels_with_structured_metadata(
            base,
            &row.structured_metadata,
            &mut merge_buf,
            &mut sm_buf,
            &mut sm_ctx,
        );
        sm_ctx.append_visible(&mut merge_buf);
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

/// The `[range]` selector width in seconds, in the reference's EXACT
/// arithmetic (issue #232).
///
/// The reference divides every per-second reducer by `selRange.Seconds()`,
/// which is `float64(d/Second) + float64(d%Second)/1e9` — **two** roundings
/// (the sub-second division, then the addition). The obvious
/// `ns as f64 / 1e9` is **one** rounding and is not the same function: for
/// `1118ms` it yields `1.1180000000000001048` where the reference yields
/// `1.1179999999999998828`, one ULP apart, which propagates straight into
/// the emitted `rate` / `bytes_rate` value (probed against
/// `grafana/loki:3.7.4`: `rate(...[1118ms])` over one line is
/// `0.8944543828264759`, not `0.8944543828264757`).
///
/// Whole-second and sub-second widths are unaffected (`nsec == 0` makes the
/// addition exact; `sec == 0` reduces to the single division), which is why
/// no corpus case caught this before — 788 of the 86.4M millisecond-granular
/// widths up to 24h differ.
fn range_seconds(ns: u64) -> f64 {
    let sec = ns / 1_000_000_000;
    let nsec = ns % 1_000_000_000;
    // Both operands are exact in f64 — `sec <= MAX_DURATION_NS / 1e9`
    // (~9.2e9) and `nsec < 1e9` are both well under 2^53 — so the only
    // roundings are the two the reference performs. The reference's own
    // `Duration` is an int64 nanosecond count with the same ceiling, so the
    // two forms agree over the whole representable domain, not just here.
    sec as f64 + nsec as f64 / 1_000_000_000.0
}

fn apply_rate(n: f64, rate_window_ns: Option<u64>) -> f64 {
    match rate_window_ns {
        Some(window_ns) if window_ns > 0 => n / range_seconds(window_ns),
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

/// `LabelsBuilder.Add`'s routing of ONE row's structured metadata
/// (`pkg/logql/log/labels.go:392-412`; issue #238): the base-collision
/// `_extracted` suffix (`parser.go:25`) is applied FIRST (`labels.go:395-397`);
/// a name that is STILL exactly `__error__`/`__error_details__` is assigned to
/// the OUT-OF-BAND slot and `return`s (`labels.go:399-407`) — it never reaches
/// `Set`, so it does NOT dirty the builder. EMPTY VALUES: `Add` assigns them
/// verbatim, and an empty slot is unset (`HasErr` = `err != ""`), so an empty
/// reserved SM value contributes nothing at all. Live-probed at v3.7.4 across
/// eleven SM shapes (`discover_log_levels: false` — with it on, every stream
/// carries a `detected_level` SM entry and the clean-builder fast paths are
/// unreachable).
#[derive(Debug, Default, Clone)]
pub struct StructuredMetadataCtx {
    /// SM `__error__` (post-suffix), routed to the err slot. "" == absent.
    pub err: String,
    /// SM `__error_details__` (post-suffix), routed to the details slot.
    /// "" == absent.
    pub details: String,
    /// At least one NON-EMPTY ordinary SM entry reached `Set` — the
    /// reference's `hasAdd()`. Empty-valued entries are excluded because the
    /// reference's distributor strips them before they can reach the builder
    /// (`pkg/distributor/distributor.go:698-723` through Prometheus'
    /// `labels.Builder`, which deletes empty-valued base labels —
    /// `labels_slicelabels.go:404-412`); PulsusDB stores them (#259), and
    /// counting only non-empty entries here keeps the details-visibility
    /// gate reference-exact regardless of what ingest stored (live-probed).
    pub has_ordinary: bool,
}

/// The shared no-structured-metadata context. NOT a `const`: `String` has a
/// `Drop` impl, so `&SOME_CONST` would be a temporary and could not satisfy
/// a `&'a` parameter; a `static` gives a `&'static` that coerces to any `'a`.
pub static EMPTY_STRUCTURED_METADATA: StructuredMetadataCtx = StructuredMetadataCtx {
    err: String::new(),
    details: String::new(),
    has_ordinary: false,
};

impl StructuredMetadataCtx {
    /// No reserved entry and no non-empty ordinary entry — the row
    /// contributes nothing to the error slots or the dirty bit.
    pub fn is_empty(&self) -> bool {
        self.err.is_empty() && self.details.is_empty() && !self.has_ordinary
    }

    /// The gated view for the NO-PIPELINE fast path (`fan_out_sm_fast_path`,
    /// which runs no `CompiledPipeline` and must therefore apply the
    /// materialisation gate itself — issue #238): `__error__` iff non-empty;
    /// `__error_details__` iff non-empty AND (`has_ordinary` OR `err`
    /// non-empty) — the same `visible()` rule the pipeline's `ErrorSlots`
    /// applies at emit. Upserts into the already-merged label vector
    /// (`set_label` semantics — the slot value wins on a same-name entry).
    pub fn append_visible(&self, out: &mut Vec<(String, String)>) {
        let upsert = |out: &mut Vec<(String, String)>, key: &str, value: &str| match out
            .iter_mut()
            .find(|(k, _)| k == key)
        {
            Some(slot) => slot.1 = value.to_string(),
            None => out.push((key.to_string(), value.to_string())),
        };
        if !self.err.is_empty() {
            upsert(out, ERROR_LABEL, &self.err);
        }
        if !self.details.is_empty() && (self.has_ordinary || !self.err.is_empty()) {
            upsert(out, ERROR_DETAILS_LABEL, &self.details);
        }
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
///
/// **Reserved-name routing (issue #238):** a pair whose POST-suffix key is
/// exactly `__error__`/`__error_details__` goes into `sm_ctx` (the reference's
/// `Add` routes it to the out-of-band slot and returns — `labels.go:399-407`)
/// and is NEVER pushed into `merge_buf`; every other pair upserts as before
/// and, when non-empty, sets `sm_ctx.has_ordinary`. `sm_ctx` is cleared and
/// refilled per row. The suffix rule runs FIRST, so a base `__error__` +
/// SM `__error__` yields an ordinary `__error___extracted` entry (probed).
fn merge_labels_with_structured_metadata(
    base: &[(String, String)],
    structured_metadata: &str,
    merge_buf: &mut Vec<(String, String)>,
    sm_buf: &mut Vec<(String, String)>,
    sm_ctx: &mut StructuredMetadataCtx,
) {
    sm_ctx.err.clear();
    sm_ctx.details.clear();
    sm_ctx.has_ordinary = false;
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
        if key == ERROR_LABEL {
            // Assign, INCLUDING an empty value (an empty slot is unset).
            sm_ctx.err = value;
            continue;
        }
        if key == ERROR_DETAILS_LABEL {
            sm_ctx.details = value;
            continue;
        }
        sm_ctx.has_ordinary |= !value.is_empty();
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

/// **The reduction-order pin** (issue #236 Part B, task-manager ruling
/// "Option B").
///
/// [`VectorAccum`] transcribes the reference's Welford recurrence, which
/// is **order-sensitive**: a group's `avg`/`stddev`/`stdvar` depends on
/// the order its members are accumulated in. PulsusDB's member order is
/// the order series arrive from the leaf, and the leaf emits by walking a
/// `HashMap` (`ClientAggState::finish` over `label_groups`/`fp_groups`).
/// Rust's default hasher is randomly seeded PER PROCESS, so without this
/// pin the same query returns different bits on different runs —
/// measured at 6 failures in 20 runs of `logqltest_corpus` before the pin
/// existed.
///
/// So the members are put in a total order that is a property of the
/// DATA, not of a hash seed: ascending by label set, which is the series'
/// own identity and the canonical order used throughout this crate.
/// Applied once per stage, immediately before grouping — never inside a
/// group's reduce (that would be `O(k log k)` per group on the read hot
/// path) — and AFTER the selection/reorder early returns, so
/// `select_k_*`/`sort_instant` keep their existing order contracts
/// untouched.
///
/// **Residual, stated because "deterministic" must not be read as
/// "identical".** This makes PulsusDB reproducible; it does not make it
/// order-identical to the reference, which walks a Go map and is itself
/// nondeterministic here (measured 10/2 over 12 runs on identical data).
/// The committed `b4_vector_aggs.test` captures land in a wide majority
/// basin — enumerating all 24 member orders of `{2,4,6,8}`, 20 of them
/// (including ascending) produce exactly the captured
/// `stdvar=5.0`/`stddev=2.23606797749979` — so the green corpus is real
/// evidence that this pin agrees with the reference on that data. It is
/// NOT proof that the sorted order is the reference's: on other data a
/// different member order could differ in the last bit. Plan v14's risk
/// #6 called these values "not capturable"; the accurate statement,
/// established by that enumeration, is **capturable but not
/// order-independent**.
fn pin_reduction_order<T>(series: &mut [T], labels_of: impl Fn(&T) -> &LabelSet) {
    series.sort_by(|a, b| labels_of(a).cmp(labels_of(b)));
}

/// The reference's per-op STREAMING accumulator, transcribed arm for arm
/// from grafana/loki v3.7.4 `pkg/logql/evaluator.go` — seed `:479-486`
/// (plus the `Stddev`/`Stdvar` zeroing at `:491-492`), update `:522-580`,
/// finish `:586-596`.
///
/// Used by BOTH [`reduce`] and the range fold, so instant and range can
/// never disagree on a value. It is simultaneously:
///
/// * **the parity fix.** PulsusDB previously computed `avg` as
///   `sum/len` and `stddev`/`stdvar` through a two-pass
///   `population_variance`; the reference uses Welford's online
///   recurrence, which produces DIFFERENT bits. And an all-NaN `min`/`max`
///   group folded to `±INF` here where the reference yields `NaN` (its
///   `group.value < s.F || IsNaN(group.value)` test seeds from the first
///   member and only ever replaces a NaN accumulator).
/// * **the enabler.** It is O(1) per group, which is what lets the fold
///   retain state proportional to OUTPUT groups rather than scanned ones.
///
/// Order-sensitivity, stated: Welford is not associative, so a group's
/// value depends on member order. The reference iterates a Go map and is
/// therefore order-NONDETERMINISTIC for `avg`/`stddev`/`stdvar` (measured
/// 10/2 over 12 runs on identical data), which is why those ops are
/// proved here by source-cited unit goldens and are NOT capturable from a
/// container. PulsusDB pins a deterministic order — the same treatment
/// the instant `first_over_time`/`last_over_time` tie already gets.
#[derive(Clone, Copy, Debug)]
struct VectorAccum {
    value: f64,
    mean: f64,
    count: u64,
}

impl VectorAccum {
    /// The "no member yet" state of a [`ReduceFold`] slot.
    ///
    /// `count == 0` is a SENTINEL, not a reachable accumulator: [`seed`]
    /// sets `count: 1` for **every** op and [`update`] never decrements,
    /// so a slot that has taken a value can never be mistaken for an
    /// empty one. Pinned by
    /// `vector_accum_seed_always_leaves_a_nonzero_count`, which drives
    /// every reducing op — the sentinel is what lets the fold hold a
    /// dense `Vec<VectorAccum>` (24 B/slot) instead of a
    /// `Vec<Option<VectorAccum>>` (32 B/slot) across a grid of up to
    /// [`MAX_ADMITTED_GRID_POINTS`] slots per group.
    ///
    /// [`seed`]: VectorAccum::seed
    /// [`update`]: VectorAccum::update
    const EMPTY: Self = VectorAccum {
        value: 0.0,
        mean: 0.0,
        count: 0,
    };

    /// Whether this slot has taken a value yet — see [`VectorAccum::EMPTY`].
    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// `evaluator.go:479-486` — the first member seeds `value` AND `mean`
    /// with its own sample and `groupCount` with 1; `:491-492` then zeroes
    /// `value` for `stddev`/`stdvar` (their `value` accumulates M2, not a
    /// running total).
    fn seed(op: VectorAggOp, f: f64) -> Self {
        let value = match op {
            VectorAggOp::Stddev | VectorAggOp::Stdvar => 0.0,
            _ => f,
        };
        VectorAccum {
            value,
            mean: f,
            count: 1,
        }
    }

    /// `evaluator.go:522-580`, arm for arm.
    fn update(&mut self, op: VectorAggOp, f: f64) {
        match op {
            // `:527` group.value += s.F
            VectorAggOp::Sum => self.value += f,
            // `:530-531` groupCount++; mean += (s.F - mean) / groupCount
            VectorAggOp::Avg => {
                self.count += 1;
                self.mean += (f - self.mean) / self.count as f64;
            }
            // `:534-536` if group.value < s.F || IsNaN(group.value)
            VectorAggOp::Max => {
                if self.value < f || self.value.is_nan() {
                    self.value = f;
                }
            }
            // `:539-541` if group.value > s.F || IsNaN(group.value)
            VectorAggOp::Min => {
                if self.value > f || self.value.is_nan() {
                    self.value = f;
                }
            }
            // `:544` groupCount++
            VectorAggOp::Count => self.count += 1,
            // `:547-550` Welford: delta = s.F - mean; mean += delta/n;
            //            value += delta * (s.F - mean)   [the NEW mean]
            VectorAggOp::Stddev | VectorAggOp::Stdvar => {
                self.count += 1;
                let delta = f - self.mean;
                self.mean += delta / self.count as f64;
                self.value += delta * (f - self.mean);
            }
            // Selections and reorders never reach a reducing accumulator —
            // `group_range`/`group_instant` and the fold dispatch them to
            // `select_k_*`/`sort_instant` first. Guarded by
            // `is_reduction`, whose exhaustive match is the single source
            // of truth for this partition.
            VectorAggOp::Topk
            | VectorAggOp::Bottomk
            | VectorAggOp::ApproxTopk
            | VectorAggOp::Sort
            | VectorAggOp::SortDesc => {
                unreachable!("{op:?} is a selection/reorder, dispatched before any accumulator")
            }
        }
    }

    /// `evaluator.go:586-596`.
    fn finish(self, op: VectorAggOp) -> f64 {
        match op {
            // `:588` aggr.value = aggr.mean
            VectorAggOp::Avg => self.mean,
            // `:591` aggr.value = float64(aggr.groupCount)
            VectorAggOp::Count => self.count as f64,
            // `:594` sqrt(value / groupCount)
            VectorAggOp::Stddev => (self.value / self.count as f64).sqrt(),
            // `:596` value / groupCount
            VectorAggOp::Stdvar => self.value / self.count as f64,
            // Sum/Min/Max carry their answer in `value` untouched.
            VectorAggOp::Sum | VectorAggOp::Min | VectorAggOp::Max => self.value,
            VectorAggOp::Topk
            | VectorAggOp::Bottomk
            | VectorAggOp::ApproxTopk
            | VectorAggOp::Sort
            | VectorAggOp::SortDesc => {
                unreachable!("{op:?} is a selection/reorder, dispatched before any accumulator")
            }
        }
    }

    /// The reducing/selecting partition over `VectorAggOp`, as ONE
    /// exhaustive match with no `_` arm — a new operator is a build
    /// failure here until it is dispositioned, rather than silently
    /// landing in whichever branch the caller happened to write.
    fn is_reduction(op: VectorAggOp) -> bool {
        match op {
            VectorAggOp::Sum
            | VectorAggOp::Avg
            | VectorAggOp::Min
            | VectorAggOp::Max
            | VectorAggOp::Count
            | VectorAggOp::Stddev
            | VectorAggOp::Stdvar => true,
            VectorAggOp::Topk
            | VectorAggOp::Bottomk
            | VectorAggOp::ApproxTopk
            | VectorAggOp::Sort
            | VectorAggOp::SortDesc => false,
        }
    }
}

/// Reduces a materialised group through [`VectorAccum`], so the
/// materialising path and the streaming fold compute the SAME bits.
///
/// `vals` is never empty at any call site (a group exists because a
/// member created it), which is what makes the `seed`-then-`update`
/// shape total; the `unwrap_or(f64::NAN)` is defence-in-depth, matching
/// the reference's own behaviour of emitting no group at all rather than
/// a sentinel.
fn reduce(op: VectorAggOp, vals: &[f64]) -> f64 {
    debug_assert!(
        VectorAccum::is_reduction(op),
        "reduce called with the selection/reorder op {op:?}"
    );
    let Some((first, rest)) = vals.split_first() else {
        return f64::NAN;
    };
    let mut acc = VectorAccum::seed(op, *first);
    for v in rest {
        acc.update(op, *v);
    }
    acc.finish(op)
}

/// `sort`/`sort_desc` order an instant result vector by value: ascending
/// (`Sort`) / descending (`SortDesc`). A NaN value ranks LAST in BOTH
/// directions (compared via `is_nan()`, so a NaN's sign never leaks into
/// the order the way `f64::total_cmp` alone would); equal values break by
/// label set ascending — deterministic and hermetically golden-able.
fn sort_instant(mut series: Vec<InstantSeries>, op: VectorAggOp) -> Vec<InstantSeries> {
    let desc = matches!(op, VectorAggOp::SortDesc);
    series.sort_by(|a, b| {
        a.value
            .is_nan()
            .cmp(&b.value.is_nan())
            .then_with(|| {
                if a.value.is_nan() {
                    std::cmp::Ordering::Equal
                } else if desc {
                    b.value.total_cmp(&a.value)
                } else {
                    a.value.total_cmp(&b.value)
                }
            })
            .then_with(|| a.labels.cmp(&b.labels))
    });
    series
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
/// ascending. `labels_of` is an ACCESSOR borrowing the caller's series
/// (issue #221 memory round: the former `Vec<LabelSet>` parameter was a
/// deep clone of every label set, input-scaled and uncharged — the
/// closure reads the identical bytes with zero copies).
fn sort_candidates<'a, F>(candidates: &mut [(usize, f64)], labels_of: F, largest: bool)
where
    F: Fn(usize) -> &'a LabelSet,
{
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
            .then_with(|| labels_of(*ai).cmp(labels_of(*bi)))
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
            sort_candidates(&mut candidates, |i| &series[i].labels, largest);
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
        sort_candidates(&mut candidates, |i| &series[i].labels, largest);
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

// =====================================================================
// Issue #236 Part B — the streaming vector-aggregation fold at the range
// leaf.
//
// `apply_vector_aggs` MATERIALISES: the leaf builds one `MatrixSeries`
// per scanned group and the aggregation collapses that vector afterwards,
// so peak retention is `scanned groups x grid points` even when the
// result is one series. The fold applies the INNERMOST aggregation as the
// leaf emits, so retention is `OUTPUT groups x grid points` — the
// reference's own bound.
//
// **The fold applies NO group-count rejection** (plan v14 §3 Part B, the
// round-13 `[high]`). [`MAX_QUERY_SERIES`] is a FINAL-result cap: an
// outer `sum` over an inner `sum by (id)` collapsing 501+ inner groups to
// ONE series is served by the reference, so rejecting an intermediate
// would reject on a proxy rather than on the resource consumed. Fold
// state is bounded by BYTES and by POINTS — and by nothing else.
//
// **The point half of that bound is NOT YET LEVIED**, and this comment
// says so rather than letting the sentence above be read as enforcement.
// A group's slots are DENSE — `kmax + 1` per output group, whatever the
// data's sparsity — so a fold over `G` output groups holds
// `G x (kmax + 1)` cells. Plan v14 §4's `charge_result_points` charges
// exactly that against [`MAX_METRIC_RESULT_POINTS`] BEFORE the vector is
// allocated; until it lands, the ceiling on a fold's retention is the
// leaf's own group-byte charge and the grid guard, and a query whose
// INTERMEDIATE grouping is very wide over a very fine grid can retain
// more than the finished result would. `MAX_METRIC_RESULT_POINTS` and
// [`MAX_ADMITTED_GRID_POINTS`] are defined but uncharged — do not read
// them as live gates.
// =====================================================================

/// The output grid a [`VectorAggFold`] indexes its dense slots by: the
/// same `(grid_start, step, kmax)` triple `RangeSlideState` emits its
/// points from, so a slot index and a grid point are two views of one
/// value.
#[derive(Clone, Copy, Debug)]
struct FoldGrid {
    start: i64,
    step: u64,
    kmax: i64,
}

impl FoldGrid {
    /// Slots in one group's dense vector: `kmax + 1`, and 0 for the empty
    /// grid (`kmax == -1`, which `grid_point_count == 0` produces).
    fn slots(&self) -> usize {
        usize::try_from(grid_slot_count(self.kmax)).unwrap_or(0)
    }

    /// `grid_start + k*step`, narrowed exactly as `FpSlide::grid_point`
    /// does — one arithmetic, so a folded point and a materialised point
    /// carry the same timestamp bits.
    fn point(&self, k: usize) -> i64 {
        clamp_bucket(self.start as i128 + k as i128 * self.step as i128)
    }

    /// The inverse of [`Self::point`]: `None` when `t` is not one of this
    /// grid's points. Every producer that feeds the fold emits at
    /// `grid_point(k)` for `k in 0..=kmax` (`FpSlide::emit_at`,
    /// `finish_in_place`'s fan-out arm, `finish_absent`), so `None` is an
    /// internal-invariant breach, not a user-reachable input — it is
    /// reported as an error rather than dropped, because a dropped point
    /// is a silently wrong result.
    fn index_of(&self, t: i64) -> Option<usize> {
        let step = i128::from(self.step);
        if step <= 0 {
            return None;
        }
        let delta = i128::from(t) - i128::from(self.start);
        if delta < 0 || delta % step != 0 {
            return None;
        }
        let k = delta / step;
        if k > i128::from(self.kmax) {
            return None;
        }
        usize::try_from(k).ok()
    }
}

/// [`RangeSlideState::covering_k`]'s body as a free function over the
/// grid scalars, so `finish_in_place`'s C2 sweep can compute the same
/// intervals without holding a `&self` borrow across its `&mut self`
/// discharges. ONE implementation, called from both.
fn covering_k_of(ts: i64, grid_start: i64, step: u64, range: i64, kmax: i64) -> (i64, i64) {
    let step = step as i128;
    let gs = grid_start as i128;
    let ts = ts as i128;
    let range = range as i128;
    // ts ≤ grid_start + k·step  ⇒  k ≥ ceil((ts-gs)/step)
    let k_lo = ceil_div_i128(ts - gs, step).max(0);
    // grid_start + k·step < ts+range ⇒ k·step ≤ ts+range-gs-1 ⇒
    // k ≤ floor((ts+range-gs-1)/step)
    let k_hi = (ts + range - gs - 1).div_euclid(step).min(kmax as i128);
    (
        i64::try_from(k_lo).unwrap_or(i64::MAX),
        i64::try_from(k_hi).unwrap_or(i64::MIN),
    )
}

/// The empty [`MutCells`] a query's mutating groups start from — the ONE
/// place the representation is chosen (issue #236 Part C).
///
/// Classes B/C always retain samples. Within class A, the delta form's
/// win is proportional to `ceil(range/step)` — the number of cells one
/// sample covers — so it is taken exactly where that exceeds one, i.e.
/// where the sliding windows OVERLAP. That is the reference's own
/// `selRange >= step` predicate (`pkg/logql/range_vector.go`'s stepped
/// iterator), plus `kmax > 0`: on a one-point grid there is nothing to
/// fan out into and the expanded form is already minimal. Below the
/// predicate the expanded form is strictly better (one charge per sample
/// rather than two, and repeated samples in one cell collapse), so it is
/// kept rather than replaced.
/// The comparison is over `i128`, not `range >= step as i64`: `step` is a
/// `u64` and the narrowing form wraps a wide step to a negative number, so
/// every range would compare `>=` it and take the delta arm. Validated
/// durations keep that out of reach today — the exhaustive predicate test
/// found it anyway, which is the argument for enumerating a small domain
/// rather than sampling it.
fn mut_cells_for(class: ReducerClass, range: i64, step: u64, kmax: i64) -> MutCells {
    if !matches!(class, ReducerClass::InvertInteger) {
        return MutCells::Samples(Vec::new());
    }
    if i128::from(range) >= i128::from(step) && kmax > 0 {
        MutCells::IntDeltas(HashMap::new())
    } else {
        MutCells::IntExpanded(HashMap::new())
    }
}

/// Grid points on a `kmax`-indexed emit grid: `kmax + 1`, and 0 for the
/// empty grid. The unit every [`charge_result_points`] reservation is
/// made in — one series can emit at most this many points, and
/// [`MAX_METRIC_RESULT_POINTS`] is derived as
/// `MAX_QUERY_SERIES * MAX_ADMITTED_GRID_POINTS` in exactly these units.
fn grid_slot_count(kmax: i64) -> u64 {
    u64::try_from(kmax.saturating_add(1)).unwrap_or(0)
}

/// The internal-invariant breach [`FoldGrid::index_of`] reports.
fn fold_off_grid(t: i64) -> ReadError {
    ReadError::PipelineInvalid {
        reason: format!(
            "internal: vector-aggregation fold received a point at {t} off the query grid"
        ),
    }
}

/// One `topk`/`bottomk` candidate holding a grid slot: the sample value
/// and the id of the series it came from. Ids are assigned in PUSH order,
/// which is what makes [`SelectFold`]'s emission order `select_k_range`'s
/// (whose survivors come out in the input vector's order).
#[derive(Clone, Copy, Debug)]
struct Cand {
    value: f64,
    series: u32,
}

/// One grid slot's surviving candidates, best first.
///
/// `Empty`/`One` keep the common cases allocation-free — a slot no series
/// reached, and `topk(1, …)` or a slot only one series reached. `Many` is
/// the only arm that allocates and holds at most `k` elements, so a
/// group's whole selection state is `O(grid x k)` and never `O(scanned
/// series x grid)`.
#[derive(Clone, Debug)]
enum KSel {
    Empty,
    One(Cand),
    Many(Vec<Cand>),
}

impl KSel {
    fn as_slice(&self) -> &[Cand] {
        match self {
            KSel::Empty => &[],
            KSel::One(c) => std::slice::from_ref(c),
            KSel::Many(v) => v,
        }
    }

    /// Inserts `cand`, keeping the slot ordered best-first under `order`
    /// and at most `k` long. Returns the candidate that lost its place —
    /// which is `cand` itself when the slot is full and `cand` is worse
    /// than every survivor.
    ///
    /// Equivalent to `sort_candidates(all).take(k)` because `order` is a
    /// TOTAL order (see [`cand_order`]): the k best elements of a set are
    /// the same set whatever sequence they arrive in.
    fn insert<F>(&mut self, cand: Cand, k: usize, order: &F) -> Option<Cand>
    where
        F: Fn(&Cand, &Cand) -> std::cmp::Ordering,
    {
        match self {
            KSel::Empty => {
                *self = KSel::One(cand);
                None
            }
            KSel::One(cur) => {
                if k == 1 {
                    if order(&cand, cur) == std::cmp::Ordering::Less {
                        let evicted = *cur;
                        *self = KSel::One(cand);
                        Some(evicted)
                    } else {
                        Some(cand)
                    }
                } else {
                    let pair = if order(&cand, cur) == std::cmp::Ordering::Less {
                        vec![cand, *cur]
                    } else {
                        vec![*cur, cand]
                    };
                    *self = KSel::Many(pair);
                    None
                }
            }
            KSel::Many(v) => {
                let pos = v.partition_point(|c| order(c, &cand) == std::cmp::Ordering::Less);
                if pos >= k {
                    // The slot is full (`pos <= v.len() <= k`) and `cand`
                    // sorts after every survivor.
                    return Some(cand);
                }
                v.insert(pos, cand);
                if v.len() > k { v.pop() } else { None }
            }
        }
    }
}

/// A series that currently holds at least one selection slot. Dropped the
/// moment its refcount reaches 0, so `live` is bounded by `output groups
/// x k` rather than by the number of series pushed.
#[derive(Debug)]
struct LiveSeries {
    labels: LabelSet,
    slots: u64,
}

/// [`sort_candidates`]' order, as a comparator over [`Cand`] and extended
/// with the series id ascending as a final tiebreak.
///
/// The tiebreak is not cosmetic and it is not a divergence: `sort_by` is
/// STABLE and `select_k_range` collects its candidates in ascending input
/// index, so two candidates that tie on `(is_nan, value, labels)` already
/// come out in ascending input order there. Naming it makes the fold's
/// order TOTAL, which is what lets an incremental top-k equal a full sort
/// plus `take(k)`. It is reachable: a fingerprint with no hydrated meta
/// gets an EMPTY label set, so two such series tie on labels.
fn cand_order<'a, F>(a: &Cand, b: &Cand, largest: bool, labels_of: &F) -> std::cmp::Ordering
where
    F: Fn(u32) -> &'a LabelSet,
{
    a.value
        .is_nan()
        .cmp(&b.value.is_nan())
        .then_with(|| {
            if a.value.is_nan() {
                std::cmp::Ordering::Equal
            } else if largest {
                b.value.total_cmp(&a.value)
            } else {
                a.value.total_cmp(&b.value)
            }
        })
        .then_with(|| labels_of(a.series).cmp(labels_of(b.series)))
        .then_with(|| a.series.cmp(&b.series))
}

/// The reducing fold (`sum`/`avg`/`min`/`max`/`count`/`stddev`/`stdvar`):
/// one dense `Vec<VectorAccum>` of `kmax + 1` slots per OUTPUT group,
/// each slot the same [`VectorAccum`] `reduce` uses — so a folded value
/// and a materialised one are the same bits by construction, not by
/// coincidence.
#[derive(Debug)]
struct ReduceFold {
    op: VectorAggOp,
    grouping: Option<Grouping>,
    grid: FoldGrid,
    groups: HashMap<LabelSet, Vec<VectorAccum>>,
    /// Reserved point-slots, charged through [`charge_result_points`]
    /// BEFORE the dense vector below is allocated (issue #236).
    slots: u64,
    slot_cap: u64,
}

impl ReduceFold {
    fn push_series(&mut self, labels: &LabelSet, points: &[(i64, f64)]) -> Result<(), ReadError> {
        // A group materialises on the first accumulated VALUE, never on
        // first sight of a member (plan v14 §3 Part B): a series with no
        // points must not create — or charge for — a group.
        if points.is_empty() {
            return Ok(());
        }
        let (op, grid) = (self.op, self.grid);
        // CHARGE, THEN ALLOCATE. `or_insert_with` would allocate inside
        // its closure, which is why the entry is matched explicitly: the
        // dense `kmax + 1` vector is reserved against the cap before it
        // exists, so a breach refuses rather than being observed after
        // the fact.
        let charged = &mut self.slots;
        let cap = self.slot_cap;
        let slots = match self.groups.entry(group_key(labels, self.grouping.as_ref())) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                charge_result_points(charged, grid.slots() as u64, cap)?;
                e.insert(vec![VectorAccum::EMPTY; grid.slots()])
            }
        };
        for &(t, v) in points {
            let k = grid.index_of(t).ok_or_else(|| fold_off_grid(t))?;
            let Some(slot) = slots.get_mut(k) else {
                return Err(fold_off_grid(t));
            };
            if slot.is_empty() {
                *slot = VectorAccum::seed(op, v);
            } else {
                slot.update(op, v);
            }
        }
        Ok(())
    }

    fn finish(self) -> Vec<MatrixSeries> {
        let (op, grid) = (self.op, self.grid);
        self.groups
            .into_iter()
            .filter_map(|(labels, slots)| {
                let points: Vec<(i64, f64)> = slots
                    .into_iter()
                    .enumerate()
                    .filter(|(_, acc)| !acc.is_empty())
                    .map(|(k, acc)| (grid.point(k), acc.finish(op)))
                    .collect();
                (!points.is_empty()).then_some(MatrixSeries { labels, points })
            })
            .collect()
    }
}

/// The selecting fold (`topk`/`bottomk`): one dense `Vec<KSel>` of
/// `kmax + 1` slots per output group, each holding at most `k`
/// candidates, plus the label sets of the series currently holding a
/// slot. `select_k_range` materialises `scanned series x steps` before
/// applying `k`; this never holds more than `output groups x grid x k`.
#[derive(Debug)]
struct SelectFold {
    /// `topk` keeps the largest, `bottomk` the smallest.
    largest: bool,
    k: usize,
    grouping: Option<Grouping>,
    grid: FoldGrid,
    groups: HashMap<LabelSet, Vec<KSel>>,
    /// Refcounted by held slots — see [`LiveSeries`].
    live: HashMap<u32, LiveSeries>,
    /// The next push-order id; `select_k_range`'s input index.
    next_series: u32,
    /// Reserved point-slots (issue #236): the dense per-group vector, and
    /// one more for each candidate a slot retains beyond its first — the
    /// `KSel::Many` heap, which the dense reservation does not cover.
    slots: u64,
    slot_cap: u64,
}

impl SelectFold {
    fn push_series(&mut self, labels: &LabelSet, points: &[(i64, f64)]) -> Result<(), ReadError> {
        let id = self.next_series;
        self.next_series = self.next_series.saturating_add(1);
        if points.is_empty() {
            return Ok(());
        }
        let (grid, k, largest) = (self.grid, self.k, self.largest);
        // A group's slot vector is created on the first push that can
        // fill a slot: `k >= 1` here (`k == 0` is `VectorAggFold::Empty`,
        // which never constructs a `SelectFold`), so the first series to
        // reach a fresh group always wins its slots.
        // CHARGE, THEN ALLOCATE — as `ReduceFold::push_series`.
        let charged = &mut self.slots;
        let cap = self.slot_cap;
        let slots = match self.groups.entry(group_key(labels, self.grouping.as_ref())) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                charge_result_points(charged, grid.slots() as u64, cap)?;
                e.insert(vec![KSel::Empty; grid.slots()])
            }
        };
        let live = &mut self.live;
        for &(t, v) in points {
            let idx = grid.index_of(t).ok_or_else(|| fold_off_grid(t))?;
            let Some(slot) = slots.get_mut(idx) else {
                return Err(fold_off_grid(t));
            };
            // CHARGE, THEN ALLOCATE — the reservation must sit AHEAD of
            // the insertion, not observe it afterwards. The slot's
            // occupancy grows by exactly one iff it is not already full,
            // which is known before `insert` runs: below `k` the new
            // candidate is always accepted, at `k` the insertion is
            // occupancy-neutral (it either evicts one or is rejected).
            if slot.as_slice().len() < k {
                charge_result_points(charged, 1, cap)?;
            }
            let evicted = {
                let seen: &HashMap<u32, LiveSeries> = live;
                // Every candidate already in the slot HOLDS it, so its
                // series is live; the pushing series may not be yet.
                // `EMPTY_LABEL_SET` is the defensive arm, never a
                // reachable one.
                let labels_of = |sid: u32| {
                    if sid == id {
                        labels
                    } else {
                        seen.get(&sid).map_or(&EMPTY_LABEL_SET, |s| &s.labels)
                    }
                };
                let order = |a: &Cand, b: &Cand| cand_order(a, b, largest, &labels_of);
                slot.insert(
                    Cand {
                        value: v,
                        series: id,
                    },
                    k,
                    &order,
                )
            };
            // A series pushes at most one candidate per slot (its points
            // carry distinct timestamps), so an evicted candidate from
            // THIS series can only be the one just offered and rejected.
            if let Some(ev) = evicted
                && ev.series == id
            {
                continue;
            }
            if let Some(ev) = evicted {
                let dropped = match live.get_mut(&ev.series) {
                    Some(held) => {
                        held.slots = held.slots.saturating_sub(1);
                        held.slots == 0
                    }
                    None => false,
                };
                if dropped {
                    live.remove(&ev.series);
                }
            }
            // The label set is cloned on the first slot this series
            // actually wins, never on first sight of it.
            live.entry(id)
                .or_insert_with(|| LiveSeries {
                    labels: labels.clone(),
                    slots: 0,
                })
                .slots += 1;
        }
        Ok(())
    }

    fn finish(self) -> Vec<MatrixSeries> {
        let grid = self.grid;
        // A series belongs to exactly one group (its `group_key` is a
        // function of its labels), and each group's slots are walked in
        // ascending grid index, so a survivor's points come out
        // TIMESTAMP-ASCENDING — the order `select_k_range`'s per-survivor
        // `BTreeMap` yields.
        let mut by_series: HashMap<u32, Vec<(i64, f64)>> = HashMap::new();
        for slots in self.groups.into_values() {
            for (k, sel) in slots.iter().enumerate() {
                let t = grid.point(k);
                for cand in sel.as_slice() {
                    by_series
                        .entry(cand.series)
                        .or_default()
                        .push((t, cand.value));
                }
            }
        }
        // Survivors in ORIGINAL PUSH ORDER — `select_k_range` emits
        // `series.into_iter().zip(keep).filter_map(..)`, i.e. the input
        // vector's order, and ids are the input index.
        let mut survivors: Vec<(u32, LiveSeries)> = self.live.into_iter().collect();
        survivors.sort_by_key(|(id, _)| *id);
        survivors
            .into_iter()
            .filter_map(|(id, held)| {
                by_series
                    .remove(&id)
                    .filter(|points| !points.is_empty())
                    .map(|points| MatrixSeries {
                        labels: held.labels,
                        points,
                    })
            })
            .collect()
    }
}

/// The innermost vector aggregation, applied AS the range leaf emits
/// rather than over its materialised output. See the module-section
/// comment above for the bound this replaces.
#[derive(Debug)]
enum VectorAggFold {
    Reduce(ReduceFold),
    Select(SelectFold),
    /// `topk(0, …)`/`bottomk(0, …)`: the result is empty whatever the
    /// input, so no group is ever constructed and no point is ever
    /// retained. Its reason is purely charge discipline — do not charge
    /// for a group that emits nothing; the group-count rejection whose
    /// premise it used to protect is gone (plan v14 §3 Part B).
    Empty,
}

impl VectorAggFold {
    /// `None` when the leaf cannot own the aggregation:
    ///
    /// * `sort`/`sort_desc` — a range matrix is a PASSTHROUGH at
    ///   `group_range` (there is no single sortable value per series), so
    ///   there is nothing to fold;
    /// * `approx_topk` — instant-only, rejected for a range query at
    ///   plan time (`plan.rs`'s `approx_topk` range check).
    ///
    /// The match is exhaustive with no `_` arm, and
    /// `vector_agg_fold_partitions_every_op_like_is_reduction` pins the
    /// reducing arm against [`VectorAccum::is_reduction`], so a new
    /// operator is a build failure here rather than a silent
    /// misclassification.
    fn new(spec: &plan::VectorAggSpec, grid: FoldGrid, slot_cap: u64) -> Option<Self> {
        let (op, grouping, param) = spec;
        match *op {
            VectorAggOp::Sort | VectorAggOp::SortDesc | VectorAggOp::ApproxTopk => None,
            VectorAggOp::Topk | VectorAggOp::Bottomk => {
                let k = k_of(*param);
                if k == 0 {
                    return Some(VectorAggFold::Empty);
                }
                Some(VectorAggFold::Select(SelectFold {
                    largest: matches!(*op, VectorAggOp::Topk),
                    k,
                    grouping: grouping.clone(),
                    grid,
                    groups: HashMap::new(),
                    live: HashMap::new(),
                    next_series: 0,
                    slots: 0,
                    slot_cap,
                }))
            }
            VectorAggOp::Sum
            | VectorAggOp::Avg
            | VectorAggOp::Min
            | VectorAggOp::Max
            | VectorAggOp::Count
            | VectorAggOp::Stddev
            | VectorAggOp::Stdvar => Some(VectorAggFold::Reduce(ReduceFold {
                op: *op,
                grouping: grouping.clone(),
                grid,
                groups: HashMap::new(),
                slots: 0,
                slot_cap,
            })),
        }
    }

    /// Folds one COMPLETE leaf series. `points` must be this grid's
    /// points, timestamp-ascending — which is what every emit site
    /// produces.
    ///
    /// Fallible today only through [`FoldGrid::index_of`]; the signature
    /// is the seam plan v14 §4's `charge_result_points` charges through,
    /// so it is `Result` from the start rather than widened later across
    /// every call site.
    fn push_series(&mut self, labels: &LabelSet, points: &[(i64, f64)]) -> Result<(), ReadError> {
        match self {
            VectorAggFold::Reduce(f) => f.push_series(labels, points),
            VectorAggFold::Select(f) => f.push_series(labels, points),
            VectorAggFold::Empty => Ok(()),
        }
    }

    fn finish(self) -> Vec<MatrixSeries> {
        match self {
            VectorAggFold::Reduce(f) => f.finish(),
            VectorAggFold::Select(f) => f.finish(),
            VectorAggFold::Empty => Vec::new(),
        }
    }

    /// Retained cells: the quantity plan v14 §4's `charge_result_points`
    /// will charge, and the one AC 8 pins as `output groups x steps`.
    /// Test-only until that counter exists — nothing in the engine reads
    /// it, and exposing it now would suggest it were enforced.
    #[cfg(test)]
    fn cells(&self) -> usize {
        match self {
            VectorAggFold::Reduce(f) => f.groups.values().map(Vec::len).sum(),
            VectorAggFold::Select(f) => f.groups.values().map(Vec::len).sum(),
            VectorAggFold::Empty => 0,
        }
    }

    /// Point-slots reserved so far. Test-only, as [`Self::cells`].
    #[cfg(test)]
    fn reserved_slots(&self) -> u64 {
        match self {
            VectorAggFold::Reduce(f) => f.slots,
            VectorAggFold::Select(f) => f.slots,
            VectorAggFold::Empty => 0,
        }
    }

    /// Output groups currently materialised. Test-only, as [`Self::cells`].
    #[cfg(test)]
    fn groups(&self) -> usize {
        match self {
            VectorAggFold::Reduce(f) => f.groups.len(),
            VectorAggFold::Select(f) => f.groups.len(),
            VectorAggFold::Empty => 0,
        }
    }
}

/// `approx_topk(k, inner)` over an instant result (issue #221) — the
/// reference's `topk(k, CountMinSketchEval(__count_min_sketch__(inner)))`
/// rewrite (pkg/logql/optimize.go), evaluated in one pass:
///
/// 1. canonical order: labels normalized name-sorted in place, then the
///    series sorted by label set ascending (value `total_cmp` tiebreak so
///    the order is total even for a duplicated label set). The
///    reference's own insertion order is a randomized Go map walk
///    (pkg/logql/evaluator.go), i.e. unspecified — PulsusDB pins
///    determinism exactly as instant `first_over_time`/`last_over_time`
///    ties are pinned (docs/features.md §2);
/// 2. for every series: stream its `stableBytes` into the three hashes
///    ([`cms::series_key`]), `add` the SAMPLE VALUE to the sketch, then
///    the retention decision ([`cms::Retention::observe`] — sketch add
///    always precedes retention, per the reference order);
/// 3. at most [`cms::CMS_MAX_LABELS`] label sets are retained (inert
///    below the cap, which is where bit-exactness is claimed);
/// 4. every retained value is replaced by `count(key)` — THE ESTIMATE,
///    never the true value; labels are MOVED out of the input;
/// 5. `select_k_instant(.., Topk, None, param)` — the existing
///    selection, not a second implementation (`grouping` is
///    structurally `None`: rejected at parse time).
///
/// MEMORY (the #227 discipline, satisfied by construction — issue #221
/// plan v4's 13-row accounting, pinned by
/// `approx_topk_accounting_total_is_a_compile_time_constant`): NOTHING
/// on this path allocates proportionally to input. `R = CMS_MAX_LABELS +
/// 1` bounds every input-facing container; using the allocator model
/// `ab = alloc_block_bytes` / `gb = grown_alloc_bytes`:
///
/// | # | allocation | bytes (upper bound) |
/// |---|---|---|
/// | 1 | per-series `labels.sort_unstable()` | 0 (in place — a stable sort would allocate scratch; load-bearing) |
/// | 2 | `series.sort_unstable_by(..)` over all S | 0 (in place — a stable sort would allocate `S/2 x 32` B, input-scaled; load-bearing) |
/// | 3 | key hashing (`cms::series_key` streams `stableBytes`) | 0 |
/// | 4 | CMS counter grid (exact `vec![0.0; W*D]`) | ab(1_522_248) = 3_044_496 |
/// | 5 | retention heap `Vec<(u32, SeriesKey)>` `with_capacity(R)` | ab(24R) = 480_048 |
/// | 6 | `observed: HashSet<u64>` `with_capacity(R)` | ab(147_472) = 294_944 (16_384-bucket hashbrown layout) |
/// | 7 | retained output `Vec<InstantSeries>` `with_capacity(R)` (moved labels, zero new string bytes) | ab(32R) = 640_064 |
/// | 8a | `select_k_instant::groups` table (1 empty-key entry) | 1_024 (generous) |
/// | 8b | `groups`' member `Vec<usize>` (grown by push) | gb(8R) = 480_048 |
/// | 9 | `select_k_instant::keep: Vec<bool>` | ab(R) = 20_002 |
/// | 10 | `select_k_instant::candidates` | ab(16R) = 320_032 |
/// | 11 | `sort_candidates` driftsort scratch (≤ n/2 elements) | ab(16·⌈R/2⌉) = 160_032 |
/// | 12 | `select_k_instant` output (`filter_map` collect — grows) | gb(32R) = 1_920_192 |
///
/// **Peak ≤ 7_360_882 B (7.02 MiB) per `approx_topk` node** — the
/// conservative SUM (every row assumed live simultaneously, no reliance
/// on drop placement), every term a compile-time constant with no
/// dependence on series count, label size, cardinality or density. The
/// input `Vec<InstantSeries>` itself is the allocation this path
/// CONSUMES (built by `apply_vector_aggs` for every vector aggregation,
/// `topk` included) and is not a new charge. Because no term scales
/// with input, nothing here can fail a charge and `apply_vector_aggs`
/// stays infallible. `apply_vector_aggs` applies the agg chain
/// sequentially, so exactly one sketch is live regardless of nesting
/// (parser `MAX_DEPTH` = 64).
fn approx_topk_instant(mut series: Vec<InstantSeries>, param: Option<f64>) -> Vec<InstantSeries> {
    // 1. Canonical, input-order-independent ordering. `sort_unstable*`
    // is load-bearing (rows 1-2 of the accounting table): a stable
    // `sort` here would reintroduce an input-scaled scratch allocation.
    for s in &mut series {
        s.labels.sort_unstable();
    }
    series.sort_unstable_by(|a, b| {
        a.labels
            .cmp(&b.labels)
            .then_with(|| a.value.total_cmp(&b.value))
    });
    // 2-3. One streaming pass: sketch add (ALWAYS, first), then the
    // retention decision — the reference `HeapCountMinSketchVector.Add`
    // order.
    let mut sketch = cms::CountMinSketch::new();
    let mut retention = cms::Retention::new();
    for (idx, s) in series.iter().enumerate() {
        let key = cms::series_key(&s.labels);
        sketch.add(key, s.value);
        retention.observe(idx as u32, key, &sketch, |root| {
            series[root as usize].labels == s.labels
        });
    }
    // 4. Retained series in ascending input (canonical) order, each
    // value replaced by the sketch ESTIMATE; labels moved, never cloned.
    let mut retained = retention.into_entries();
    retained.sort_unstable_by_key(|&(idx, _)| idx);
    let mut out = Vec::with_capacity(retained.len());
    let mut next = retained.iter().peekable();
    for (idx, s) in series.into_iter().enumerate() {
        if let Some(&&(ridx, key)) = next.peek()
            && ridx as usize == idx
        {
            next.next();
            out.push(InstantSeries {
                labels: s.labels,
                value: sketch.count(key),
            });
        }
    }
    // 5. The existing selection — reused, not reimplemented.
    select_k_instant(out, VectorAggOp::Topk, None, param)
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
    // A range result (matrix) has no single sortable value per series;
    // `sort`/`sort_desc` are passthrough here (the reference likewise does
    // not value-order matrices — the wire stays label-canonical).
    if matches!(op, VectorAggOp::Sort | VectorAggOp::SortDesc) {
        return series;
    }
    // Issue #236: the same pin as `group_instant`. `members` is walked in
    // push order at every step below, so pinning the push order pins the
    // per-step accumulation order for every step at once.
    let mut series = series;
    pin_reduction_order(&mut series, |s| &s.labels);
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
    // approx_topk (issue #221): sketch-estimate the values, then the
    // ordinary topk selection. Grouping is rejected at parse time, so
    // `grouping` is structurally `None` here (pinned by
    // `approx_topk_specs_never_carry_a_grouping` in plan.rs).
    if matches!(op, VectorAggOp::ApproxTopk) {
        return approx_topk_instant(series, param);
    }
    if matches!(op, VectorAggOp::Topk | VectorAggOp::Bottomk) {
        return select_k_instant(series, op, grouping, param);
    }
    // `sort`/`sort_desc` reorder the vector by value (no grouping —
    // rejected at plan time), preserving each series unchanged.
    if matches!(op, VectorAggOp::Sort | VectorAggOp::SortDesc) {
        return sort_instant(series, op);
    }
    // Issue #236: pin the reduction order before grouping — see
    // `pin_reduction_order`. Welford is order-sensitive and the incoming
    // order is a hash walk.
    let mut series = series;
    pin_reduction_order(&mut series, |s| &s.labels);
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

// ---------------------------------------------------------------------
// Issue #236 §4/§5 — the post-aggregation byte model.
//
// The coefficients below are MEASURED, not enumerated. Every `W_*`/`B_*`
// is `WITNESS_MARGIN x rate_max` where `rate_max` is the largest secant
// slope observed on that axis by the cohort-attributed allocator witness
// (`crates/pulsus-read/tests/logql_post_agg_witness.rs`). Nothing here
// enumerates containers, element widths or growth factors: the measured
// rate absorbs all of them, which is what makes a forgotten container
// impossible rather than merely unlikely.
//
// Every coefficient below is
//     shipped = ceil(rate_max x WITNESS_MARGIN x 11/10)
// with WITNESS_MARGIN = 2. The extra tenth is NOT a second safety margin
// and is not a hand tightening in either direction: an allocation
// measurement jitters by a few units between runs (hashbrown growth
// order, in-place-collect eligibility), and a gate of the form
// `shipped >= 2 x rate_max_measured_now` would redden on a 1 % drift. The
// tenth is a stated, uniform rounding rule so the CI gate is
// deterministic. There is no upper-bound gate, so rounding up costs
// nothing but tightness, which this design deliberately does not pin.
//
// Read `MAX_POST_AGG_BYTES`' doc for what the resulting bound does and
// does NOT claim.
// ---------------------------------------------------------------------

/// `W_SERIES` — bytes per stage-input series.
///
/// Ladder: `topk(k = N)` over a RANGE operand with no grouping, so every
/// stage retains everything. `N` spans `128 -> 8 192` (64x) with
/// points-per-series and label pairs scaled as `8 192 / N`, so `points`,
/// `label_bytes` and `label_pairs` are constant along the ladder.
/// Measured `rate_max` = **710** B/series (uniform; concentrated 416).
/// Shipped = `ceil(710 x 2 x 11/10)`.
pub const W_SERIES: u64 = 1_562;

/// `W_POINT` — bytes per stage-input point.
///
/// Ladder: a RANGE operand of 64 series with no grouping, so it collapses
/// to a SINGLE output group and one `BTreeSet<i64>` step union holds every
/// point; `steps` spans `4 -> 512` (128x on `points`).
/// Measured `rate_max` = **53** B/point (concentrated; uniform 42).
/// Shipped = `ceil(53 x 2 x 11/10)`.
pub const W_POINT: u64 = 117;

/// `W_LABEL_BYTE` — bytes per raw label content byte.
///
/// Ladder: `without(id00)` over 256 instant series of 4 label pairs — one
/// output group per series, so the retained key mass is maximal; the label
/// VALUE width spans `4 -> 1 024` bytes (128x on `label_bytes`).
/// Measured `rate_max` = **1** B/B on both skews.
/// Shipped = `ceil(1 x 2 x 11/10)`.
pub const W_LABEL_BYTE: u64 = 3;

/// `W_PAIR` — bytes per label pair.
///
/// Ladder: `without(id00)` over 64 instant series, pairs spanning
/// `4 -> 512` (128x) with the per-pair value width scaled as `2 048 /
/// pairs` so the byte total stays near constant.
/// Measured `rate_max` = **103** B/pair (concentrated; uniform 68; the
/// measurement jitters between 102 and 103 between runs, which is what
/// the 11/10 rounding covers).
/// Shipped = `ceil(103 x 2 x 11/10)`.
pub const W_PAIR: u64 = 227;

/// `W_STAGE_SERIES` — bytes per (series x chain stage).
///
/// **MEASURED ZERO, and that is a finding rather than an oversight.**
/// Plan v14 §6.1 predicted that "the previous stage's buffer is live
/// while its successor is collected", so a chain of `L` stages would cost
/// `L` concurrent buffers. It does not:
///
/// * `select_k_instant`'s output is
///   `series.into_iter().zip(keep).filter_map(..).collect()`, and `Zip`
///   and `FilterMap` over `vec::IntoIter` are `SourceIter` +
///   `InPlaceIterable`, so the standard library collects the output **in
///   place, into the input's own buffer** — the second buffer does not
///   exist at all;
/// * every vector-aggregation arm is non-expanding in both series and
///   points (grouping collapses, `topk` selects, `sort` permutes), so a
///   later stage's input is never larger than an earlier stage's, and the
///   peak cannot accumulate down the chain.
///
/// Ladder: nested `topk(k = N)` over 512 instant series at chain lengths
/// 1, 2, 4 and 64 — the peak is **21 204 B at every length**, on both
/// skews, so the rate is 0. Measured further across 8 (shape x grouping x
/// operator) combinations at lengths 1, 2, 4, 8 and 64: flat from length
/// 2 onward everywhere. The term is kept in the model's published form
/// (plan v14 §4) but is inert; what defends the claim is
/// `chain_depth_does_not_multiply_peak_memory` in the witness, which
/// reddens if a future change ever makes depth accumulate.
pub const W_STAGE_SERIES: u64 = 0;

/// `W_GROUPNAME` — bytes per (series x `by`-clause byte).
///
/// Ladder: `by(id00, <q-1 names absent from the data>)` over 256 instant
/// series, `q` spanning `4 -> 256` (64x on `series x
/// group_name_bytes`). §5.4 named an ALL-absent clause as the maximising
/// shape; measurement refutes that — every series then collapses into ONE
/// group, so exactly one key is retained and the peak is flat from `q = 4`
/// to `q = 16` — and §5.4's actual rule, the shape that maximises the
/// axis's rate, selects the one-present-name form.
/// Measured `rate_max` = **11** B per (series x by-byte), both skews.
/// Shipped = `ceil(11 x 2 x 11/10)`.
pub const W_GROUPNAME: u64 = 25;

/// `W_APPROX_TOPK` — the flat count-min sketch plus retention heap
/// (`cms::CMS_DEPTH x cms::CMS_WIDTH` `f64` counters = 7 x 27 183 x 8 B,
/// fixed and input-independent).
///
/// Derived as the measured peak MINUS the model without this term, over
/// the `approx_topk` cells at the SMALLEST inputs (1, 2, 8 and 64
/// series). A flat term is masked at a large fixture — the input-scaled
/// terms already dominate the 1.5 MiB sketch, and the excess reads as 0 —
/// so it is derived where it is visible, which is also where
/// under-bounding would be a real safety hole rather than a cosmetic one.
/// Measured excess = **1 907 298** B at one series.
/// Shipped = `ceil(1 907 298 x 2 x 11/10)`.
pub const W_APPROX_TOPK: u64 = 4_196_056;

/// `B_SERIES` — bytes per binary-operand series.
///
/// Ladder: one-to-one, `matching = None` (so the join signature is the
/// FULL label set and `B_MANY`/`B_INCLUDE` are zero in every rung),
/// instant operands, `N` spanning `64 -> 4 096` per side (64x on
/// `lhs.series + rhs.series`).
/// Measured `rate_max` = **578** B/series (concentrated; uniform 458).
/// Shipped = `ceil(578 x 2 x 11/10)`.
pub const B_SERIES: u64 = 1_272;

/// `B_POINT` — bytes per binary-operand point.
///
/// Ladder: the same matching over MATRIX operands of 16 series, `steps`
/// spanning `4 -> 512` (128x on the point total). Sixteen series, not the
/// usual baseline: `combine_matrices` runs an INDEPENDENT per-step join,
/// so its cost is `steps x series` and the widest rung otherwise
/// dominates the whole binary's wall time.
/// Measured `rate_max` = **37** B/point (concentrated; uniform 33).
/// Shipped = `ceil(37 x 2 x 11/10)`.
pub const B_POINT: u64 = 82;

/// `B_LABEL` — bytes per binary-operand raw label content byte.
///
/// Ladder: the same matching over 256 instant series of 4 pairs, label
/// VALUE width spanning `4 -> 1 024` bytes (128x).
/// Measured `rate_max` = **2** B/B on both skews.
/// Shipped = `ceil(2 x 2 x 11/10)`.
pub const B_LABEL: u64 = 5;

/// `B_PAIR` — bytes per binary-operand label pair.
///
/// Ladder: the same matching over 64 instant series, pairs spanning
/// `4 -> 512` (128x) with the per-pair width scaled as `2 048 / pairs`.
/// `matching = None` is load-bearing here: under `on(id00)` the match
/// signature is a ONE-pair projection, the other pairs are never cloned,
/// and the measured rate is 0 — the plan's pre-commitment to `None` for
/// this row is what makes the axis visible at all.
/// Measured `rate_max` = **107** B/pair (concentrated; uniform 79).
/// Shipped = `ceil(107 x 2 x 11/10)`.
pub const B_PAIR: u64 = 236;

/// `B_MANY` — bytes per many-side series under a group modifier. Zero
/// when there is no group modifier: the one-to-one arm keeps a single
/// `HashSet<MatchSig>` where the grouped arm keeps a `HashMap<MatchSig,
/// HashSet<MatchSig>>`.
///
/// Ladder: `on(id00) group_left()` with an EMPTY include list, many-side
/// width spanning `64 -> 4 096` (64x). This is the per-many-side-item
/// cost of `instant_join`'s `many_matched` map.
/// Measured `rate_max` = **1 319** B/series (concentrated; uniform
/// 1 152). Shipped = `ceil(1 319 x 2 x 11/10)`.
pub const B_MANY: u64 = 2_902;

/// `B_INCLUDE` — bytes per (many-side series x include byte).
///
/// Ladder: `on(id00) group_left(inc_1..inc_q)` over 128 instant series,
/// with the ONE side carrying all 256 include labels in every rung, `q`
/// spanning `4 -> 256` (64x on `many.series x include_bytes`). This is
/// `set_label_sorted`'s insert chain — one `Vec::insert` per include name
/// per many-side series.
/// Measured `rate_max` = **12** B per (series x include byte), both
/// skews. Shipped = `ceil(12 x 2 x 11/10)`.
pub const B_INCLUDE: u64 = 27;

/// **The post-aggregation byte cap** — the smallest power of two at or
/// above `max(X_chain, X_bin)`, where each `X` is the corresponding model
/// maximised over the leaf-gated feasible region **at the non-amplifying
/// corner** (`group_name_bytes = 0`, `include_bytes = 0`, both binary
/// operands at independent leaf budgets).
///
/// **What it buys, exactly:** every client-leaf-sourced stage input with
/// no `by`-name amplification and no `group_left/right` include
/// amplification is admitted. Nothing broader. A query carrying either
/// amplifier may be refused above the thresholds recorded as the O6/O7
/// ledger entries.
///
/// **What it is NOT.** It is not a worst-case proof. It is a bound
/// "measured-and-margined over a compile-enforced construct space, with a
/// clean refusal instead of an OOM at the boundary". Anyone reading it as
/// a worst-case guarantee is reading it wrong: the residual is a
/// distribution adversarial in a dimension no ladder varies, and the 2x
/// margin is what covers it.
///
/// **Deliberately not pinned from above.** No test asserts
/// `MAX_POST_AGG_BYTES < k x max(X)`. A change that REDUCES peak memory
/// (issue #245's Part C deletes two `BTreeMap` indexes and a `BTreeSet`
/// union from `combine_matrices`) must never redden CI; regenerating is
/// one command, `zz_witness_report`.
///
/// # The generator's numbers
///
/// ```text
/// s_min              = 616 bytes        (min over the four leaf entry slots)
/// N_max              = 435 771 series   (MAX_CLIENT_AGG_GROUP_BYTES / s_min)
/// stages             = 64               (min(MAX_DEPTH, MAX_QUERY_BYTES / 4))
/// X_chain            = 2 847 288 941 bytes   (argmax N = 546)
/// X_bin              = 5 970 118 644 bytes   (argmax N = 546)
/// MAX_POST_AGG_BYTES = 8 589 934 592 bytes   (8 GiB)
/// tightness ratio    = 1.4388           (printed, NOT gated)
/// ```
///
/// # O6 — the `by(...)` amplification threshold
///
/// `A_MIN = 597` total `by`-clause bytes, at `N = 435 558`; with
/// `A_NAME_MIN = 2` that is **at least 299 one-character `by` names**.
/// Strictly below `A_MIN`, refusal is impossible at ANY group count.
/// **Reachable**: 597 bytes fits inside `MAX_QUERY_BYTES = 131 072`.
///
/// # O7 — the `group_left/right(include)` amplification threshold
///
/// `AMP_MIN = 97 030 221`, the smallest `many.series x include_bytes`
/// PRODUCT at which the binary funnel can refuse, at `N_many = 546`.
/// **Reachable** within the query-text cap.
///
/// Both are the model-level thresholds; the funnel that turns them into
/// an actual refusal is issue #236's §4 and is not wired yet, so the
/// divergence ledger does not carry O6/O7 rows until it is.
pub const MAX_POST_AGG_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// A stage input's measured shape — the raw counted quantities the byte
/// model multiplies. One `O(series + label pairs)` pass over data that is
/// already materialised (`Vec::len` is `O(1)`, so **no per-point work**).
///
/// Fields are private and every accessor is derived from one exhaustive
/// destructure ([`StageInput::model_inputs`]), so a new axis cannot be
/// added without the paired-fixture isolation gate seeing it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StageInput {
    /// `N` — top-level series in the stage input.
    series: u64,
    /// `Σ labels.len()` over the input's series.
    label_pairs: u64,
    /// `Σ (k.len() + v.len())` — RAW label content bytes.
    label_bytes: u64,
    /// `Σ label_set_bytes(labels)` — the leaf's own charging vocabulary,
    /// so the feasible region's operand and the model's input are the
    /// same quantity.
    label_block_bytes: u64,
    /// The widest single series' label-pair count.
    max_series_pairs: u64,
    /// The longest single label VALUE, in bytes — read only through
    /// [`include_bytes`].
    max_value_bytes: u64,
    /// `P` — total points (1 per series for an instant vector).
    points: u64,
    /// The longest single series, in points.
    max_series_points: u64,
}

impl StageInput {
    /// Every model-relevant input, named, from ONE exhaustive destructure
    /// — adding a field to [`StageInput`] stops this compiling, which is
    /// what keeps §6's "every non-target input is byte-identical" gate
    /// from silently missing a new axis (the `AggCaps` `E0027` precedent).
    pub fn model_inputs(&self) -> [(&'static str, u64); 8] {
        let Self {
            series,
            label_pairs,
            label_bytes,
            label_block_bytes,
            max_series_pairs,
            max_value_bytes,
            points,
            max_series_points,
        } = *self;
        [
            ("series", series),
            ("label_pairs", label_pairs),
            ("label_bytes", label_bytes),
            ("label_block_bytes", label_block_bytes),
            ("max_series_pairs", max_series_pairs),
            ("max_value_bytes", max_value_bytes),
            ("points", points),
            ("max_series_points", max_series_points),
        ]
    }

    /// `N`.
    pub fn series(&self) -> u64 {
        self.series
    }

    /// `P`.
    pub fn points(&self) -> u64 {
        self.points
    }

    /// `Σ label_set_bytes(labels)` — the quantity the leaf's own
    /// group-byte charge is denominated in.
    pub fn label_block_bytes(&self) -> u64 {
        self.label_block_bytes
    }

    /// `Σ (k.len() + v.len())`.
    pub fn label_bytes(&self) -> u64 {
        self.label_bytes
    }

    /// `Σ labels.len()`.
    pub fn label_pairs(&self) -> u64 {
        self.label_pairs
    }

    /// The longest single label VALUE — read only through
    /// [`include_bytes`].
    pub fn max_value_bytes(&self) -> u64 {
        self.max_value_bytes
    }

    /// **Derivation seam, not a measurement.** Builds a [`StageInput`]
    /// from raw counted quantities so the cap derivation can evaluate the
    /// model at the feasible region's corners (`N` up to ~4.4e5 series,
    /// `P` up to 1.2e7 points) without materialising hundreds of MiB of
    /// synthetic series. Every production charge obtains its `StageInput`
    /// from [`measure_matrix`]/[`measure_vector`] — this constructor
    /// measures nothing and must never be used to authorise a charge.
    #[allow(clippy::too_many_arguments)]
    pub fn for_derivation(
        series: u64,
        label_pairs: u64,
        label_bytes: u64,
        label_block_bytes: u64,
        max_series_pairs: u64,
        max_value_bytes: u64,
        points: u64,
        max_series_points: u64,
    ) -> Self {
        Self {
            series,
            label_pairs,
            label_bytes,
            label_block_bytes,
            max_series_pairs,
            max_value_bytes,
            points,
            max_series_points,
        }
    }
}

/// Measures a matrix stage input. `s.points.len()` is `O(1)`, so the pass
/// is `O(series + label pairs)` and adds nothing per point.
pub fn measure_matrix(series: &[MatrixSeries]) -> StageInput {
    let mut m = StageInput {
        series: series.len() as u64,
        ..StageInput::default()
    };
    for s in series {
        measure_labels(&mut m, &s.labels);
        let pts = s.points.len() as u64;
        m.points = m.points.saturating_add(pts);
        m.max_series_points = m.max_series_points.max(pts);
    }
    m
}

/// Measures an instant-vector stage input — one point per series, which
/// is what makes `points == series` here.
pub fn measure_vector(series: &[VectorSample]) -> StageInput {
    let mut m = StageInput {
        series: series.len() as u64,
        points: series.len() as u64,
        max_series_points: u64::from(!series.is_empty()),
        ..StageInput::default()
    };
    for s in series {
        measure_labels(&mut m, &s.labels);
    }
    m
}

/// The label half of a `measure_*` pass, shared so the two entry points
/// cannot drift.
fn measure_labels(m: &mut StageInput, labels: &LabelSet) {
    let pairs = labels.len() as u64;
    m.label_pairs = m.label_pairs.saturating_add(pairs);
    m.max_series_pairs = m.max_series_pairs.max(pairs);
    for (k, v) in labels {
        m.label_bytes = m
            .label_bytes
            .saturating_add(k.len() as u64)
            .saturating_add(v.len() as u64);
        m.max_value_bytes = m.max_value_bytes.max(v.len() as u64);
    }
    m.label_block_bytes = m.label_block_bytes.saturating_add(label_set_bytes(labels));
}

/// `Σ_stages Σ_{name ∈ by(...)} (name.len() + 1)` — the grouping-name
/// amplifier, read off the QUERY TEXT and never off the data.
///
/// Counts **every** `by` name, including ones absent from the data: which
/// names are absent is unknowable before the stage runs, and counting all
/// of them is the conservative direction. `without(...)` contributes
/// nothing — `group_key`'s `Without` arm copies the series' own labels,
/// which the `W_PAIR`/`W_LABEL_BYTE` terms already price.
pub fn group_name_bytes(aggs: &[plan::VectorAggSpec]) -> u64 {
    let mut total: u64 = 0;
    for (_, grouping, _) in aggs {
        let Some(g) = grouping else { continue };
        if g.kind != GroupingKind::By {
            continue;
        }
        for name in &g.labels {
            total = total.saturating_add(name.len() as u64).saturating_add(1);
        }
    }
    total
}

/// `Σ_{ln ∈ include} (ln.len() + one.max_value_bytes + 1)` — the
/// `group_left/right(include)` amplifier, per many-side series.
///
/// Zero for a set operation ([`is_set_op`] returns before `include` is
/// read, `instant_join`'s first statement) and zero for one-to-one
/// matching (`matching.group.is_none()`).
pub fn include_bytes(matching: Option<&VectorMatching>, op: BinOp, one: &StageInput) -> u64 {
    if is_set_op(op) {
        return 0;
    }
    let Some(group) = matching.and_then(|m| m.group.as_ref()) else {
        return 0;
    };
    let include = match group {
        MatchGroup::Left(inc) | MatchGroup::Right(inc) => inc,
    };
    let mut total: u64 = 0;
    for ln in include {
        total = total
            .saturating_add(ln.len() as u64)
            .saturating_add(one.max_value_bytes)
            .saturating_add(1);
    }
    total
}

/// One term of [`post_agg_peak_bytes`]. `None` evaluates the shipped
/// model; every other variant zeroes exactly one coefficient.
///
/// **A test seam** (the `apply_vector_aggs_capped` / `group_bytes_cap`
/// precedent): §6's paired fixtures assert a term is NECESSARY by
/// showing the model WITHOUT it fails to cover the incremental bytes the
/// pair causes. Discriminating on increments is the only comparison that
/// survives independently-margined coefficients.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainTerm {
    /// The shipped model, no term suppressed.
    None,
    Series,
    Point,
    LabelByte,
    Pair,
    StageSeries,
    GroupName,
    ApproxTopk,
}

/// One term of [`binary_peak_bytes`]; see [`ChainTerm`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryTerm {
    /// The shipped model, no term suppressed.
    None,
    Series,
    Point,
    Label,
    Pair,
    Many,
    Include,
}

/// An upper bound on the heap bytes the post-aggregation chain may hold
/// SIMULTANEOUSLY, over and above its input. Contains no container
/// enumeration and no allocator model — every coefficient is measured
/// (§5.4) and margined.
///
/// All arithmetic saturates: `group_name_bytes` is read off unbounded-
/// until-#279 query text and stays large after it, so an amplified query
/// must resolve to `u64::MAX` (⇒ a clean refusal) and never wrap to a
/// small number that would admit an unbounded allocation.
pub fn post_agg_peak_bytes(m: &StageInput, aggs: &[plan::VectorAggSpec]) -> u64 {
    post_agg_peak_bytes_without(m, aggs, ChainTerm::None)
}

/// [`post_agg_peak_bytes`] with one coefficient forced to zero — §6's
/// necessity seam. The `match` is exhaustive with no `_` arm, so a new
/// term must be dispositioned here before it can ship.
pub fn post_agg_peak_bytes_without(
    m: &StageInput,
    aggs: &[plan::VectorAggSpec],
    drop: ChainTerm,
) -> u64 {
    let w = |term: ChainTerm, coeff: u64| if drop == term { 0 } else { coeff };
    let stages = aggs.len() as u64;
    let names = group_name_bytes(aggs);
    let approx = aggs
        .iter()
        .any(|(op, _, _)| matches!(op, VectorAggOp::ApproxTopk));

    let mut total = w(ChainTerm::Series, W_SERIES).saturating_mul(m.series);
    total = total.saturating_add(w(ChainTerm::Point, W_POINT).saturating_mul(m.points));
    total =
        total.saturating_add(w(ChainTerm::LabelByte, W_LABEL_BYTE).saturating_mul(m.label_bytes));
    total = total.saturating_add(w(ChainTerm::Pair, W_PAIR).saturating_mul(m.label_pairs));
    total = total.saturating_add(
        w(ChainTerm::StageSeries, W_STAGE_SERIES)
            .saturating_mul(m.series)
            .saturating_mul(stages),
    );
    total = total.saturating_add(
        w(ChainTerm::GroupName, W_GROUPNAME)
            .saturating_mul(m.series)
            .saturating_mul(names),
    );
    if approx {
        total = total.saturating_add(w(ChainTerm::ApproxTopk, W_APPROX_TOPK));
    }
    total
}

/// The same for a binary combination. `many`/`one` are chosen EXACTLY as
/// [`instant_join`] chooses them (`MatchGroup::Left` and one-to-one ⇒
/// many = lhs, `MatchGroup::Right` ⇒ many = rhs), so the include
/// amplification is never charged against the wrong side.
pub fn binary_peak_bytes(
    op: BinOp,
    matching: Option<&VectorMatching>,
    lhs: &StageInput,
    rhs: &StageInput,
) -> u64 {
    binary_peak_bytes_without(op, matching, lhs, rhs, BinaryTerm::None)
}

/// [`binary_peak_bytes`] with one coefficient forced to zero — §6's
/// necessity seam; see [`post_agg_peak_bytes_without`].
pub fn binary_peak_bytes_without(
    op: BinOp,
    matching: Option<&VectorMatching>,
    lhs: &StageInput,
    rhs: &StageInput,
    drop: BinaryTerm,
) -> u64 {
    let b = |term: BinaryTerm, coeff: u64| if drop == term { 0 } else { coeff };
    // `instant_join`'s own role assignment, transcribed.
    let group = matching.and_then(|m| m.group.as_ref());
    let (many, one) = match group {
        None | Some(MatchGroup::Left(_)) => (lhs, rhs),
        Some(MatchGroup::Right(_)) => (rhs, lhs),
    };
    let inc = include_bytes(matching, op, one);
    // The `B_MANY` term prices `instant_join`'s `many_matched:
    // HashMap<MatchSig, HashSet<MatchSig>>`, which exists ONLY on the
    // grouped arm — the one-to-one arm keeps a single
    // `HashSet<MatchSig>` and is priced by `B_SERIES`. Without this gate
    // the term takes the same value with and without a group modifier and
    // §6.4's difference-of-differences cancels it to zero, which is how
    // the gate found the omission.
    let many_series = if group.is_some() { many.series } else { 0 };

    let mut total =
        b(BinaryTerm::Series, B_SERIES).saturating_mul(lhs.series.saturating_add(rhs.series));
    total = total.saturating_add(
        b(BinaryTerm::Point, B_POINT).saturating_mul(lhs.points.saturating_add(rhs.points)),
    );
    total = total.saturating_add(
        b(BinaryTerm::Label, B_LABEL)
            .saturating_mul(lhs.label_bytes.saturating_add(rhs.label_bytes)),
    );
    total = total.saturating_add(
        b(BinaryTerm::Pair, B_PAIR).saturating_mul(lhs.label_pairs.saturating_add(rhs.label_pairs)),
    );
    total = total.saturating_add(b(BinaryTerm::Many, B_MANY).saturating_mul(many_series));
    total = total.saturating_add(
        b(BinaryTerm::Include, B_INCLUDE)
            .saturating_mul(many.series)
            .saturating_mul(inc),
    );
    total
}

/// `s_min` — the smallest per-entry byte charge any of the four client
/// leaf group paths levies, computed from live `size_of` through the
/// leaf's own [`map_entry_bytes`]/[`grown_alloc_bytes`] vocabulary with
/// the shortest possible key.
///
/// It is the feasible region's series operand: a leaf that admitted `N`
/// groups paid at least `s_min * N` of its
/// [`MAX_CLIENT_AGG_GROUP_BYTES`] budget, so `N <=
/// (MAX_CLIENT_AGG_GROUP_BYTES - L̂) / s_min`. Derived, never chosen —
/// if a slot's layout changes the region moves with it.
pub fn leaf_min_entry_bytes() -> u64 {
    [
        MUT_GROUP_SLOT,
        INSTANT_GROUP_SLOT,
        FP_GROUP_SLOT,
        SERIES_OUT_SLOT,
    ]
    .into_iter()
    .map(|slot| map_entry_bytes(slot).saturating_add(grown_alloc_bytes(0)))
    .min()
    .expect("four leaf entry slots")
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

    // ---- Issue #227: sliding-window range engine ----

    fn slide_meta(fp: u64, labels_json: &str) -> HashMap<u64, StreamMetaRow> {
        let mut m = HashMap::new();
        m.insert(
            fp,
            StreamMetaRow {
                fingerprint: fp,
                service: "svc".to_string(),
                labels: labels_json.to_string(),
            },
        );
        m
    }

    /// The pipeline stages of a log query (for building a `ClientAgg`).
    fn parse_pipeline(query: &str) -> Vec<Stage> {
        let pulsus_logql::Expr::Log(le) = pulsus_logql::parse(query).expect("parse") else {
            panic!("expected a log expression");
        };
        le.pipeline
    }

    /// Builds a RANGE `ClientWindow` through the real validation funnel —
    /// tests cannot fabricate an unvalidated duration either (issue #227
    /// review round 3).
    fn slide_window(start_ns: i64, end_ns: i64, step_ns: u64, range_ns: u64) -> ClientWindow {
        ClientWindow::Range {
            grid_start_ns: start_ns,
            end_ns,
            step_ns: super::super::params::validate_duration_ns(step_ns, "step")
                .expect("valid step"),
            range_ns: super::super::params::validate_duration_ns(range_ns, "range selector")
                .expect("valid range"),
        }
    }

    /// Narrows an instant `ClientWindow` for [`ClientAggState::new`]
    /// (issue #236 Part D). Tests cannot fabricate the witness either —
    /// `InstantWindow`'s field is private to `mod instant_window`, so
    /// `mint` is the only source anywhere in the crate, `mod tests`
    /// included; a test that hands a stepped window here fails at the
    /// `expect`, not silently.
    fn instant_of(window: ClientWindow) -> InstantWindow {
        window.as_instant().expect("an instant window")
    }

    fn slide_rows(fp: u64, samples: &[(i64, &str)]) -> Vec<MetricScanRow> {
        samples
            .iter()
            .map(|(ts, body)| MetricScanRow {
                fingerprint: fp,
                timestamp_ns: *ts,
                body: body.to_string(),
            })
            .collect()
    }

    fn one_series_points(res: QueryResult) -> (LabelSet, Vec<(i64, f64)>) {
        match res {
            QueryResult::Matrix(mut s) => {
                assert_eq!(s.len(), 1, "expected one series, got {s:?}");
                let s = s.remove(0);
                (s.labels, s.points)
            }
            other => panic!("expected a matrix, got {other:?}"),
        }
    }

    /// count_over_time sliding windows, hand-computed. Samples at
    /// 10,20,30,40,50; grid 0..50 step 10, range 25 ⇒ window `(t-25, t]`.
    /// t=0 empty (gap); t=10→1, 20→2, 30→3, 40→{20,30,40}=3, 50→{30,40,50}=3.
    #[test]
    fn sliding_count_over_time_matches_hand_computed_windows() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows = slide_rows(1, &[(10, "x"), (20, "x"), (30, "x"), (40, "x"), (50, "x")]);
        let window = slide_window(0, 50, 10, 25);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).unwrap();
        let (_, points) = one_series_points(res);
        assert_eq!(
            points,
            vec![(10, 1.0), (20, 2.0), (30, 3.0), (40, 3.0), (50, 3.0)],
            "empty first window emits no point; overlapping windows slide"
        );
    }

    /// Issue #227 review round 11 (end-to-end at the domain floor): for
    /// `start = end = i64::MIN` with `[1ns]`, the grid's only window
    /// `(i64::MIN - 1ns, i64::MIN]` has a logical lower bound BELOW the
    /// representable domain — a sample stored at exactly `i64::MIN` is
    /// INSIDE it (the reference's `(t-range, t]` includes it; the bound is
    /// vacuous, not exclusive). The prior saturating eviction clamped the
    /// bound to `i64::MIN` and dropped the sample.
    #[test]
    fn a_sample_at_i64_min_is_inside_an_underflowing_window() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows = slide_rows(1, &[(i64::MIN, "x")]);
        let window = slide_window(i64::MIN, i64::MIN, 1, 1);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).unwrap();
        let (_, points) = one_series_points(res);
        assert_eq!(
            points,
            vec![(i64::MIN, 1.0)],
            "the boundary sample must be counted, matching the reference"
        );
    }

    /// The round-11 negative control: a LEGITIMATELY-computed `i64::MIN`
    /// window bound — `t = i64::MIN + 1`, `[1ns]`, so `t - range` is
    /// exactly `i64::MIN` with NO underflow — keeps its EXCLUSIVE
    /// semantics: the sample at exactly `i64::MIN` stays outside
    /// `(i64::MIN, i64::MIN + 1]` and only the in-window sample counts.
    #[test]
    fn a_legitimate_i64_min_window_bound_still_excludes_the_boundary_sample() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows = slide_rows(1, &[(i64::MIN, "out"), (i64::MIN + 1, "in")]);
        let window = slide_window(i64::MIN + 1, i64::MIN + 1, 1, 1);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).unwrap();
        let (_, points) = one_series_points(res);
        assert_eq!(
            points,
            vec![(i64::MIN + 1, 1.0)],
            "an exactly-representable exclusive bound must still evict the boundary sample"
        );
    }

    /// `rate({}[1m]) ≠ rate({}[10m])`: the `[range]` is live (window width AND
    /// the per-second divisor both track range, not step). Two samples 30s
    /// apart at step 60s.
    #[test]
    fn sliding_rate_depends_on_the_range_selector() {
        let client_for = |range_op| ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op,
            param: None,
            absent_labels: vec![],
        };
        let client = client_for(RangeAggOp::Rate);
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        // Two lines inside a 1m window ending at 60s.
        let rows = slide_rows(1, &[(31_000_000_000, "x"), (59_000_000_000, "x")]);
        let s = 1_000_000_000i64;
        let run = |range_ns: i64| {
            let window = slide_window(60 * s, 60 * s, (60 * s) as u64, range_ns as u64);
            let res = run_client_agg_rows(
                &rows,
                &compiled,
                &meta,
                &client,
                window,
                Some(range_ns as u64),
            )
            .unwrap();
            one_series_points(res).1
        };
        let r1m = run(60 * s); // 2 lines / 60s = 0.0333…
        let r10m = run(600 * s); // 2 lines / 600s = 0.0033…
        assert_ne!(r1m, r10m, "the [range] divisor must be live");
        assert_eq!(r1m, vec![(60 * s, 2.0 / 60.0)]);
        assert_eq!(r10m, vec![(60 * s, 2.0 / 600.0)]);
    }

    /// AC10: a forced same-`(fingerprint, timestamp_ns)` collision of ≥3 rows
    /// with DISTINCT bodies resolves `first_over_time`/`last_over_time` (and
    /// class-C folds) in the fixed full-body `tie_rank` order, byte-stable
    /// across shuffled input.
    #[test]
    fn sliding_collision_group_orders_by_full_body_deterministically() {
        // `last_over_time` over an unwrapped value: value is the byte length,
        // ordered by full body. Three same-ns lines with distinct bodies.
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        // Same ts, distinct bodies — count is order-independent, but the
        // collision path must run without panicking and yield a stable count.
        let mut base = vec![(5i64, "ccc"), (5, "aaa"), (5, "bbb")];
        let r1 = run_client_agg_rows(
            &slide_rows(1, &base),
            &compiled,
            &meta,
            &client,
            window,
            None,
        )
        .unwrap();
        base.reverse();
        let r2 = run_client_agg_rows(
            &slide_rows(1, &base),
            &compiled,
            &meta,
            &client,
            window,
            None,
        )
        .unwrap();
        assert_eq!(r1, r2, "shuffled collision input must be byte-stable");
        assert_eq!(one_series_points(r1).1, vec![(10, 3.0)]);
    }

    /// The collision-group cap trips a clean `TsCollisionGroup` 422, never an
    /// OOM, on a pathological same-`(fingerprint, ts)` run.
    #[test]
    fn sliding_collision_group_over_cap_is_a_named_too_broad_error() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let n = (MAX_TS_COLLISION_GROUP + 1) as usize;
        let rows: Vec<MetricScanRow> = (0..n)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("line-{i}"),
            })
            .collect();
        let window = slide_window(0, 10, 10, 10);
        match run_client_agg_rows(&rows, &compiled, &meta, &client, window, None) {
            Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup { cap, .. })) => {
                assert_eq!(cap, MAX_TS_COLLISION_GROUP);
            }
            other => panic!("expected TsCollisionGroup, got {other:?}"),
        }
    }

    /// AC2: `stream_hash` == Loki's `labels.StableHash`, pinned against
    /// GOLDEN VALUES CAPTURED FROM THE REFERENCE ITSELF — computed by calling
    /// `labels.StableHash` in the pinned `grafana/loki` v3.7.4 checkout
    /// (`vendor/github.com/prometheus/prometheus/model/labels/sharding.go`,
    /// default build tags, the shape Loki release builds use). These are
    /// external constants, NOT a recomputation of our own implementation, so
    /// a change to our byte layout/seed reddens this test.
    #[test]
    fn stream_hash_matches_loki_stable_hash_golden_values() {
        let lbl = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        // (sorted labels, Loki v3.7.4 `labels.StableHash` value)
        let cases: &[(Vec<(String, String)>, u64)] = &[
            (
                lbl(&[("app", "a"), ("env", "prod")]),
                8_934_535_624_278_967_805,
            ),
            (
                lbl(&[("env", "prod"), ("service_name", "checkout")]),
                3_591_138_641_183_557_463,
            ),
            (lbl(&[]), 17_241_709_254_077_376_921),
            (lbl(&[("k", "v")]), 3_592_197_247_305_585_030),
            (
                lbl(&[("__name__", "x"), ("job", "j"), ("zz", "last")]),
                12_310_789_843_392_592_049,
            ),
        ];
        for (labels, want) in cases {
            assert_eq!(
                stream_hash(labels),
                *want,
                "StableHash mismatch for {labels:?}"
            );
        }
    }

    /// AC5(a): a DENSE but Loki-servable window (well within the caps and the
    /// byte budget) streams to a correct result — the cap must not reject
    /// density Loki serves. 50_000 samples in one `[range]`, well under the
    /// 4M cap.
    #[test]
    fn sliding_dense_but_servable_window_streams_without_a_cap_error() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        const N: i64 = 50_000;
        let rows: Vec<MetricScanRow> = (0..N)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i + 1, // 1ns apart, all inside the window
                body: "x".to_string(),
            })
            .collect();
        let window = slide_window(0, N, N as u64, N as u64);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None)
            .expect("a dense-but-servable window must stream, never 422");
        assert_eq!(one_series_points(res).1, vec![(N, N as f64)]);
    }

    /// AC5(b) + AC8: the concurrent-retention cap trips DURING the scan (as
    /// the first oversized window fills), not after it — the charge is
    /// per-load, so a retaining reducer over more than `cap` in-window points
    /// aborts with the named `MetricRetention` error. Uses a tiny synthetic
    /// cap via the retaining (class-B `quantile`) path with a window wide
    /// enough that nothing evicts. `quantile` charges TWO points per sample
    /// (the window entry plus its pre-charged re-reduce copy — review round 5,
    /// finding 2), which is what the trip arithmetic below is expressed in.
    #[test]
    fn sliding_retention_cap_trips_during_the_scan_with_the_named_error() {
        // A retaining reducer (quantile) whose window never evicts: every
        // sample stays concurrently retained, so `retained` climbs to the cap.
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::QuantileOverTime,
            param: Some(0.5),
            absent_labels: vec![],
        };
        let per_sample = retention_points_per_sample(client.range_op);
        assert_eq!(
            per_sample, 2,
            "quantile pre-charges its per-emit value copy alongside the window entry"
        );
        // Drive `FpSlide::load_group` directly with a TINY cap so the test is
        // fast and the trip point is exact (the production cap is 4M).
        let mut slide = FpSlide {
            stream_hash: 7,
            labels: vec![],
            op: client.range_op,
            class: reducer_class(client.range_op, client.value),
            param: client.param,
            rate_window_ns: None,
            grid_start: 0,
            step: 1_000_000,
            range: 1_000_000_000,
            kmax: 10,
            next_k: 0,
            win: VecDeque::new(),
            run_int: 0,
            per_sample,
            points: Vec::new(),
        };
        let mut retained = 0u64;
        const TINY_CAP: u64 = 100;
        let member = |v: f64| CollMember {
            body: String::new(),
            value: v,
            out: None,
        };
        let mut err = None;
        let mut loads = 0u64;
        for i in 0..1_000i64 {
            // All at ts=1 so nothing ever evicts (one collision-free load per
            // call at increasing ts would evict; here the window only grows).
            let group = [member(i as f64)];
            loads += 1;
            if let Err(e) = slide.load_group(1, &group, &mut retained, TINY_CAP) {
                err = Some(e);
                break;
            }
        }
        match err {
            Some(ReadError::QueryTooBroad(TooBroadReason::MetricRetention { count, cap })) => {
                assert_eq!(cap, TINY_CAP);
                assert_eq!(
                    count,
                    TINY_CAP + per_sample,
                    "trips at the first over-cap load"
                );
            }
            other => panic!("expected MetricRetention, got {other:?}"),
        }
        assert_eq!(
            loads,
            TINY_CAP / per_sample + 1,
            "the cap must trip DURING the scan (after ~cap/per-sample loads), not after all 1000"
        );
        // CHARGE BEFORE ALLOCATE (review round 5, finding 2): the breaching
        // sample must never reach the deque. If the charge were applied AFTER
        // `push_back` — as it was before — the window would hold one more
        // sample than the cap allows, i.e. the allocation the cap exists to
        // refuse would already have happened.
        assert_eq!(
            slide.win.len() as u64,
            TINY_CAP / per_sample,
            "the refused sample must not have been pushed"
        );
    }

    /// AC8: the concurrent-retention invariant is symmetric — every charged
    /// point is discharged on eviction, so a long streaming slide over a
    /// narrow window keeps `retained` bounded by the window's contents rather
    /// than growing with the scan.
    #[test]
    fn sliding_retention_charge_and_discharge_are_symmetric() {
        let mut slide = FpSlide {
            stream_hash: 7,
            labels: vec![],
            op: RangeAggOp::QuantileOverTime,
            class: ReducerClass::ReduceIndependent,
            param: Some(0.5),
            rate_window_ns: None,
            grid_start: 0,
            step: 10,
            range: 10,
            kmax: 100,
            next_k: 0,
            win: VecDeque::new(),
            run_int: 0,
            per_sample: retention_points_per_sample(RangeAggOp::QuantileOverTime),
            points: Vec::new(),
        };
        let mut retained = 0u64;
        // 100 samples 10ns apart over a 10ns window: at most a couple are
        // ever concurrently retained, however long the scan runs (times the
        // per-sample charge — 2 for quantile).
        let bound = 3 * retention_points_per_sample(RangeAggOp::QuantileOverTime);
        for i in 1..=100i64 {
            let group = [CollMember {
                body: String::new(),
                value: i as f64,
                out: None,
            }];
            slide
                .load_group(i * 10, &group, &mut retained, MAX_RETAINED_WINDOW_POINTS)
                .expect("under cap");
            assert!(
                retained <= bound,
                "retention must stay window-bounded, saw {retained} after {i} loads"
            );
        }
        // Closing the series discharges everything left in the window.
        slide.finish(&mut retained);
        assert_eq!(retained, 0, "charge/discharge must be symmetric");
    }

    /// AC10: a forced same-`(fingerprint, timestamp_ns)` collision of ≥3 rows
    /// with DISTINCT bodies resolves an ORDER-DEPENDENT reducer through the
    /// full-body `tie_rank` order, byte-stable across shuffled input. Uses
    /// `last_over_time` (positional in the canonical order) plus a class-C
    /// `sum` — the two shapes the review called out as untested.
    #[test]
    fn sliding_same_stream_tie_rank_orders_class_c_and_first_last_deterministically() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        // Three rows at the IDENTICAL `(fingerprint, timestamp_ns)` with
        // DISTINCT bodies that all parse to the same label set (`a` is
        // consumed by the unwrap), chosen so the full-BODY byte order is
        // deliberately DIFFERENT from the value order:
        //   `a="3"` (0x22 after `a=`) < `a=1` (0x31) < `a=2` (0x32)
        //   ⇒ canonical values in order: 3, 1, 2
        // So `first` = 3 and `last` = 2. A value-tiebreak (the old instant
        // rule) would give first=1/last=3, and an arrival-order rule would
        // flap under shuffling — this case discriminates all three.
        let bodies = [r#"a="3""#, "a=1", "a=2"];
        let run = |op: RangeAggOp, order: &[usize]| -> Vec<(i64, f64)> {
            let client = ClientAgg {
                pipeline: parse_pipeline(r#"{x="y"} | logfmt | unwrap a"#),
                value: ClientValue::Unwrap,
                range_op: op,
                param: None,
                absent_labels: vec![],
            };
            let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
            let rows: Vec<MetricScanRow> = order
                .iter()
                .map(|&i| MetricScanRow {
                    fingerprint: 1,
                    timestamp_ns: 5,
                    body: bodies[i].to_string(),
                })
                .collect();
            let res =
                run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).expect("eval");
            one_series_points(res).1
        };
        // Every input permutation must give the SAME, body-order-determined
        // answer (byte-stable across shuffled runs).
        for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2], [1, 2, 0], [2, 0, 1]] {
            assert_eq!(
                run(RangeAggOp::FirstOverTime, &order),
                vec![(10, 3.0)],
                "first = the min-(ts,stream_hash,tie_rank) sample (body order), input {order:?}"
            );
            assert_eq!(
                run(RangeAggOp::LastOverTime, &order),
                vec![(10, 2.0)],
                "last = the max-(ts,stream_hash,tie_rank) sample (body order), input {order:?}"
            );
            // Class C: the fold runs in the same canonical order.
            assert_eq!(run(RangeAggOp::SumOverTime, &order), vec![(10, 6.0)]);
        }
    }

    // -----------------------------------------------------------------
    // Issue #236 §4 — the result-point charge.
    // -----------------------------------------------------------------

    /// The charge is an ADMISSION counter with an exact IDENTITY: one
    /// grid width per output series, whatever the data's density.
    ///
    /// Stated as an identity rather than an inequality because that is
    /// what makes it checkable — a charge that merely stayed under the
    /// cap would pass with the reservation deleted.
    #[test]
    fn the_result_point_charge_is_one_grid_width_per_output_series() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        // step 10 over [0, 100] => kmax = 10 => 11 grid points.
        let window = slide_window(0, 100, 10, 30);
        let grid = 11u64;

        // Non-mutating: one slider per FINGERPRINT.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | line_format "keep""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(!compiled.metric_mutates_labels());
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // Six rows on ONE fingerprint: density must not move the charge.
        state
            .push_rows(&slide_rows(
                1,
                &[
                    (5, "a"),
                    (6, "b"),
                    (17, "c"),
                    (28, "d"),
                    (39, "e"),
                    (95, "f"),
                ],
            ))
            .expect("fold");
        assert_eq!(
            state.result_points, grid,
            "one fingerprint => exactly one grid width, whatever its density"
        );

        // Mutating: one group per distinct OUTPUT LABEL SET.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(compiled.metric_mutates_labels());
        for groups in [1u64, 3, 7] {
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            // Two rows per group, so the count is groups and not rows.
            let mut rows: Vec<MetricScanRow> = Vec::new();
            for g in 0..groups {
                for r in 0..2u64 {
                    rows.push(MetricScanRow {
                        fingerprint: 1,
                        timestamp_ns: (g * 10 + r) as i64,
                        body: format!("id={g}"),
                    });
                }
            }
            state.push_rows(&rows).expect("fold");
            assert_eq!(
                state.result_points,
                groups * grid,
                "{groups} output groups => {groups} grid widths"
            );
        }
    }

    /// The cap REFUSES, and it refuses BEFORE the allocation it is
    /// guarding — the group that would breach is never created.
    #[test]
    fn the_result_point_cap_refuses_before_the_group_exists() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // Room for exactly two groups (11 points each).
        state.caps.result_points = 22;
        // FOUR rows: `push_rows` flushes a collision group only when the
        // next row's `(fingerprint, ts)` differs, so the trailing group
        // stays staged. The fourth row is what makes the third group
        // flush — and breach — inside `push_rows` rather than at finish.
        let rows: Vec<MetricScanRow> = (0..4u64)
            .map(|g| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: g as i64 * 10,
                body: format!("id={g}"),
            })
            .collect();
        match state.push_rows(&rows) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricResultPoints { count, cap })) => {
                assert_eq!(cap, 22);
                assert_eq!(count, 33, "the error names the breaching reservation");
            }
            other => panic!("expected MetricResultPoints, got {other:?}"),
        }
        assert_eq!(
            state.groups.len(),
            2,
            "the breaching group must never be created"
        );
        assert_eq!(state.result_points, 22, "a refused charge must not stick");
    }

    /// The FOLD reserves its dense slots before the vector exists, and
    /// the reservation is the vector's own width.
    #[test]
    fn the_fold_reserves_its_dense_slots_before_allocating_them() {
        let grid = FoldGrid {
            start: 0,
            step: 10,
            kmax: 4,
        };
        let slots = 5u64;
        let by_id = Grouping {
            kind: GroupingKind::By,
            labels: vec!["id".to_string()],
        };
        // Two output groups' worth of room, three groups offered.
        let mut fold = VectorAggFold::new(&(VectorAggOp::Sum, Some(by_id), None), grid, 2 * slots)
            .expect("sum folds");
        for g in 0..2u32 {
            let labels = fold_labels(&[("id", &g.to_string())]);
            fold.push_series(&labels, &[(0, 1.0)]).expect("admitted");
        }
        assert_eq!(fold.groups(), 2);
        assert_eq!(
            fold.cells(),
            2 * slots as usize,
            "dense, kmax + 1 per group"
        );
        let labels = fold_labels(&[("id", "2")]);
        match fold.push_series(&labels, &[(0, 1.0)]) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricResultPoints { count, cap })) => {
                assert_eq!(cap, 2 * slots);
                assert_eq!(count, 3 * slots);
            }
            other => panic!("expected MetricResultPoints, got {other:?}"),
        }
        assert_eq!(
            fold.groups(),
            2,
            "the refused group's dense vector must never be allocated"
        );

        // The selecting fold charges the same dense reservation, plus one
        // for each candidate a slot retains beyond its first.
        let mut fold = VectorAggFold::new(&(VectorAggOp::Topk, None, Some(2.0)), grid, 1_000)
            .expect("topk folds");
        fold.push_series(&fold_labels(&[("h", "a")]), &[(0, 1.0)])
            .expect("admitted");
        // One group's dense vector (5) + one candidate.
        assert_eq!(fold_slots(&fold), slots + 1);
        fold.push_series(&fold_labels(&[("h", "b")]), &[(0, 2.0)])
            .expect("admitted");
        assert_eq!(fold_slots(&fold), slots + 2, "the slot grew to k = 2");
        // The slot is now FULL: a third candidate evicts rather than
        // growing, so it reserves nothing.
        fold.push_series(&fold_labels(&[("h", "c")]), &[(0, 3.0)])
            .expect("admitted");
        assert_eq!(
            fold_slots(&fold),
            slots + 2,
            "an eviction is occupancy-neutral and must charge nothing"
        );
    }

    // -----------------------------------------------------------------
    // Issue #236 Part D — `ClientAggState` is instant-only.
    // -----------------------------------------------------------------

    /// AC 16 — a stepped window cannot reach [`ClientAggState`].
    ///
    /// The domain is the `ClientWindow` enum, which has exactly two
    /// variants, so it is enumerated rather than sampled. The guard is
    /// UNREPRESENTABILITY, not unreachability: `InstantWindow`'s single
    /// field is private to `mod instant_window`, so no line in the crate
    /// — including this one — can write the witness directly, and
    /// `mint` is the only source. A bare unit struct would have been
    /// merely unreachable, since every `ClientAggState::new` call site
    /// lives in this file.
    #[test]
    fn a_stepped_window_cannot_mint_the_instant_witness() {
        let instant = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        assert!(
            instant.as_instant().is_some(),
            "an instant window must narrow"
        );
        // Every stepped shape the constructor could be handed: the
        // narrowest legal grid and a wide one.
        for (start, end, step, range) in [(0i64, 0i64, 1u64, 1u64), (0, 1_000, 10, 45)] {
            let stepped = slide_window(start, end, step, range);
            assert!(
                stepped.as_instant().is_none(),
                "a stepped window must NOT narrow: {stepped:?}"
            );
        }
    }

    /// AC 16 — the state's result is a `Vector`, always.
    ///
    /// The former stepped arms of `finish` (the `bucket_grid` absence
    /// walk and the per-bucket `Matrix` emit) are deleted, so there is no
    /// input that makes this state emit a matrix. Driven over all four
    /// shapes: absent and non-absent, fan-out and non-fan-out.
    #[test]
    fn the_instant_state_can_only_emit_a_vector() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        let rows = slide_rows(1, &[(10, "a=1"), (20, "a=2"), (30, "a=3")]);
        for (op, value, pipeline) in [
            // non-absent, non-fan-out
            (RangeAggOp::CountOverTime, ClientValue::Count, r#"{x="y"}"#),
            // non-absent, fan-out
            (
                RangeAggOp::MaxOverTime,
                ClientValue::Unwrap,
                r#"{x="y"} | logfmt | unwrap a"#,
            ),
            // absent, non-fan-out
            (RangeAggOp::AbsentOverTime, ClientValue::Count, r#"{x="y"}"#),
        ] {
            for rows in [&rows[..], &[][..]] {
                let client = ClientAgg {
                    pipeline: parse_pipeline(pipeline),
                    value,
                    range_op: op,
                    param: None,
                    absent_labels: vec![("app".to_string(), "a".to_string())],
                };
                let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
                let mut state = ClientAggState::new(
                    &compiled,
                    &meta,
                    &client,
                    instant_of(window),
                    None,
                    AggCaps::DEFAULT,
                )
                .unwrap();
                state.push_rows(rows).expect("fold");
                let out = state.finish();
                assert!(
                    matches!(out, QueryResult::Vector(_)),
                    "{op:?} over {} row(s) must emit a vector, got {out:?}",
                    rows.len()
                );
            }
        }
    }

    /// Every row folds into its group's ONE accumulator, on BOTH
    /// grouping arms.
    ///
    /// The collapse of the per-group `BTreeMap` to a single `BucketAcc`
    /// moved the fold into each arm, so each arm now owns a
    /// seed-or-accumulate decision of its own. A mutant that made the
    /// non-fan-out arm REPLACE rather than fold was caught only by six
    /// `variants(...)` goldens — which drive the same state for an
    /// unrelated reason and would stop covering it the moment those
    /// fixtures changed. This pins the property where it lives, on both
    /// arms and over ops whose value depends on every member.
    #[test]
    fn both_grouping_arms_fold_every_row_into_one_accumulator() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        // Four rows, ascending values, all in the one window.
        let rows = slide_rows(1, &[(10, "a=1"), (20, "a=8"), (30, "a=2"), (40, "a=4")]);
        // (op, value kind, non-fan-out pipeline, fan-out pipeline, want)
        // The fan-out pipelines set a CONSTANT label, so both arms
        // produce exactly one group and the only difference between them
        // is which map the accumulator lives in.
        let cases: [(RangeAggOp, ClientValue, &str, &str, f64); 3] = [
            (
                RangeAggOp::CountOverTime,
                ClientValue::Count,
                r#"{x="y"}"#,
                r#"{x="y"} | label_format zone="eu""#,
                4.0,
            ),
            (
                RangeAggOp::BytesOverTime,
                ClientValue::Bytes,
                r#"{x="y"} | line_format "abcde""#,
                r#"{x="y"} | line_format "abcde" | label_format zone="eu""#,
                20.0,
            ),
            (
                RangeAggOp::BytesOverTime,
                ClientValue::Bytes,
                r#"{x="y"}"#,
                r#"{x="y"} | label_format zone="eu""#,
                // "a=1" / "a=8" / "a=2" / "a=4" — 3 bytes each.
                12.0,
            ),
        ];
        for (op, value, plain, mutating, want) in cases {
            for (arm, query) in [("non-fan-out", plain), ("fan-out", mutating)] {
                let client = ClientAgg {
                    pipeline: parse_pipeline(query),
                    value,
                    range_op: op,
                    param: None,
                    absent_labels: vec![],
                };
                let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
                assert_eq!(
                    compiled.metric_mutates_labels(),
                    arm == "fan-out",
                    "{op:?} {arm}: the fixture must take the arm it names"
                );
                let mut state = ClientAggState::new(
                    &compiled,
                    &meta,
                    &client,
                    instant_of(window),
                    None,
                    AggCaps::DEFAULT,
                )
                .unwrap();
                state.push_rows(&rows).expect("fold");
                let QueryResult::Vector(items) = state.finish() else {
                    panic!("instant results are vectors");
                };
                assert_eq!(items.len(), 1, "{op:?} {arm}: one group");
                assert_eq!(
                    items[0].value.to_bits(),
                    want.to_bits(),
                    "{op:?} {arm}: every row must fold into the group's accumulator"
                );
            }
        }
    }

    /// `absent_over_time` instant: the presence FLAG that replaced the
    /// bucket set answers the same question. One surviving line anywhere
    /// suppresses the absence sample; none emits it.
    #[test]
    fn instant_absence_is_a_flag_over_the_whole_selector() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"}"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::AbsentOverTime,
            param: None,
            absent_labels: vec![("app".to_string(), "a".to_string())],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let run = |rows: &[MetricScanRow]| -> usize {
            let mut state = ClientAggState::new(
                &compiled,
                &meta,
                &client,
                instant_of(window),
                None,
                AggCaps::DEFAULT,
            )
            .unwrap();
            state.push_rows(rows).expect("fold");
            let QueryResult::Vector(items) = state.finish() else {
                panic!("instant absence is a vector");
            };
            items.len()
        };
        assert_eq!(run(&[]), 1, "nothing survived => one absence sample");
        assert_eq!(
            run(&slide_rows(1, &[(10, "x")])),
            0,
            "one surviving line anywhere suppresses absence"
        );
        assert_eq!(
            run(&slide_rows(1, &[(10, "x"), (20, "y"), (90, "z")])),
            0,
            "several surviving lines are still just 'present'"
        );
    }

    // -----------------------------------------------------------------
    // Issue #236 Part C — the mutating range path's cell representation.
    // -----------------------------------------------------------------

    /// The three [`MutCells`] arms and the C1 branch predicate, pinned at
    /// the ONE place the representation is chosen.
    ///
    /// The domain is small and enumerated rather than sampled: every
    /// class, and `range` on both sides of `step` plus the exact
    /// boundary, plus the degenerate one-point grid.
    #[test]
    fn the_mutating_cell_representation_is_chosen_by_one_predicate() {
        let arm =
            |class, range: i64, step: u64, kmax: i64| match mut_cells_for(class, range, step, kmax)
            {
                MutCells::IntExpanded(_) => "expanded",
                MutCells::IntDeltas(_) => "deltas",
                MutCells::Samples(_) => "samples",
            };
        // Class A: deltas exactly where the windows overlap AND the grid
        // has more than one point.
        for (range, step, kmax, want) in [
            (9i64, 10u64, 10i64, "expanded"), // range < step
            (10, 10, 10, "deltas"),           // the boundary: range == step
            (11, 10, 10, "deltas"),           // overlapping
            (1_000, 10, 10, "deltas"),        // heavily overlapping
            (1_000, 10, 0, "expanded"),       // one-point grid
            (1_000, 10, -1, "expanded"),      // empty grid
            (1, u64::MAX, 10, "expanded"),    // step wider than any range
        ] {
            assert_eq!(
                arm(ReducerClass::InvertInteger, range, step, kmax),
                want,
                "class A at range={range} step={step} kmax={kmax}"
            );
        }
        // Every other class retains samples, whatever the geometry.
        for class in [ReducerClass::CanonicalFold, ReducerClass::ReduceIndependent] {
            for (range, step, kmax) in [(9i64, 10u64, 10i64), (10, 10, 10), (1_000, 10, 10)] {
                assert_eq!(arm(class, range, step, kmax), "samples", "{class:?}");
            }
        }
    }

    /// AC 12 — **Part C moves no result**, leg 1: the class-A drain
    /// against the UNTOUCHED streaming slider.
    ///
    /// `FpSlide` (the non-mutating path) is not changed by Part C, so it
    /// is the oracle the rewritten mutating drain is measured against:
    /// the same rows through the same reducer, once without a label
    /// mutation (slider) and once with one constant `label_format` label
    /// (fan-out), must emit the identical point SEQUENCE on
    /// `f64::to_bits`. Driven on BOTH C1 branches — `range < step` uses
    /// the expanded arm, `range >= step` the difference array.
    ///
    /// Class A is the only class where both paths exist:
    /// `metric_mutates_labels()` is `mutates_labels || has_unwrap`, so
    /// every unwrap query is a fan-out query by construction. Classes
    /// B/C are covered by the sweep oracle below.
    #[test]
    fn the_class_a_drain_reproduces_the_untouched_streaming_slider() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows: Vec<MetricScanRow> = (1..=9)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i * 7,
                body: format!("body-{i}"),
            })
            .collect();
        for (step, range) in [(10u64, 5u64), (10, 10), (10, 45), (10, 200)] {
            let window = slide_window(0, 100, step, range);
            for (op, value) in [
                (RangeAggOp::CountOverTime, ClientValue::Count),
                (RangeAggOp::BytesOverTime, ClientValue::Bytes),
                (RangeAggOp::BytesRate, ClientValue::Bytes),
                (RangeAggOp::Rate, ClientValue::Count),
            ] {
                let run = |query: &str| -> Vec<(i64, u64)> {
                    let client = ClientAgg {
                        pipeline: parse_pipeline(query),
                        value,
                        range_op: op,
                        param: None,
                        absent_labels: vec![],
                    };
                    let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
                    let res =
                        run_client_agg_rows(&rows, &compiled, &meta, &client, window, Some(range))
                            .unwrap();
                    let QueryResult::Matrix(items) = res else {
                        panic!("expected a matrix");
                    };
                    assert_eq!(items.len(), 1, "{op:?}: one series");
                    items[0]
                        .points
                        .iter()
                        .map(|(t, v)| (*t, v.to_bits()))
                        .collect()
                };
                // BOTH a non-empty and an EMPTY rendered line. The empty
                // one is what makes `dcount` load-bearing: it gives a
                // COVERED cell the value 0, which must still emit `0`
                // and which a coverage test folded into the value would
                // silently drop. (A mutant that folded them passed
                // against the non-empty fixture alone.)
                for line in ["keep", ""] {
                    let sliding = run(&format!(r#"{{x="y"}} | line_format "{line}""#));
                    let fanned = run(&format!(
                        r#"{{x="y"}} | line_format "{line}" | label_format zone="eu""#
                    ));
                    assert!(
                        !sliding.is_empty(),
                        "{op:?}: the fixture must emit (line {line:?})"
                    );
                    assert_eq!(
                        fanned, sliding,
                        "{op:?} at step={step} range={range} line={line:?}: the rewritten \
                         class-A drain must reproduce the streaming slider bit for bit"
                    );
                }
            }
        }
    }

    /// AC 12 — **Part C moves no result**, leg 2: the class-B/C sweep
    /// against the PER-CELL reduction it replaces.
    ///
    /// Classes B/C have no non-mutating path to compare with (every
    /// unwrap query fans out), so the oracle is the OLD ALGORITHM,
    /// reimplemented here over the state's own retained samples: bucket
    /// each sample into every cell `covering_k` gives it, sort each
    /// bucket with `win_order`, reduce. The sweep must produce that
    /// sequence exactly.
    ///
    /// **This is WEAKER evidence than leg 1, and the difference is not
    /// cosmetic.** Leg 1 compares the class-A drain against an
    /// INDEPENDENT implementation — `FpSlide`, written for a different
    /// path and untouched by Part C — so a shared misconception cannot
    /// hide in it. This leg compares the sweep against a
    /// reimplementation written by the same hand, from the same
    /// understanding, in the same sitting: it catches transcription
    /// errors (wrong bucket, missing sort, off-by-one in the pointers —
    /// mutants 21/22/23 all die here) but it CANNOT catch a
    /// misunderstanding of what `covering_k` means, because both sides
    /// would be wrong together. What backs classes B/C against that is
    /// the untouched corpus and goldens (`b9_range_sliding.test`, every
    /// range case in `logql_metric_agg_golden.rs`, the `differential_*`
    /// files), whose values come from the reference. Do not read the two
    /// legs as the same strength.
    #[test]
    fn the_sample_sweep_reproduces_the_per_cell_reduction() {
        // TWO fingerprints collapsing into ONE output group
        // (`label_format app=...` overrides the only distinguishing
        // label), fed in the scan's FINGERPRINT-MAJOR order. That is what
        // makes the group's retained samples arrive out of `win_order`
        // and the finish-time sort load-bearing: a single-fingerprint
        // fixture pushes them already sorted, and a mutant deleting the
        // sort passed against one.
        let mut meta = slide_meta(1, r#"{"app":"a"}"#);
        meta.insert(
            2,
            StreamMetaRow {
                fingerprint: 2,
                service: "svc".to_string(),
                labels: r#"{"app":"b"}"#.to_string(),
            },
        );
        let mut rows: Vec<MetricScanRow> = (1..=9)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i * 7,
                body: format!("a={}", (i * 13) % 7),
            })
            .collect();
        rows.extend((1..=9).map(|i| MetricScanRow {
            fingerprint: 2,
            timestamp_ns: i * 5 + 2,
            body: format!("a={}", (i * 11) % 5),
        }));
        for (step, range) in [(10u64, 5u64), (10, 10), (10, 45), (10, 200)] {
            let window = slide_window(0, 100, step, range);
            for op in [
                RangeAggOp::MaxOverTime,
                RangeAggOp::MinOverTime,
                RangeAggOp::SumOverTime,
                RangeAggOp::AvgOverTime,
                RangeAggOp::FirstOverTime,
                RangeAggOp::LastOverTime,
                RangeAggOp::StddevOverTime,
                RangeAggOp::QuantileOverTime,
                RangeAggOp::RateCounter,
            ] {
                let client = ClientAgg {
                    pipeline: parse_pipeline(
                        r#"{x="y"} | logfmt | label_format app="same" | unwrap a"#,
                    ),
                    value: ClientValue::Unwrap,
                    range_op: op,
                    param: matches!(op, RangeAggOp::QuantileOverTime).then_some(0.5),
                    absent_labels: vec![],
                };
                let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
                let mut state = RangeSlideState::new(
                    &compiled,
                    &meta,
                    &client,
                    window,
                    Some(range),
                    AggCaps::DEFAULT,
                )
                .unwrap();
                state.push_rows(&rows).expect("fold");
                // Flush the trailing collision group before reading the
                // retained set: `push_rows` leaves the last
                // `(fingerprint, ts)` run buffered until finish, so an
                // oracle built before the flush would be one sample
                // short (it was, and said so).
                let base_labels = std::mem::take(&mut state.base_labels);
                state.flush_collision(&base_labels).expect("flush");
                state.base_labels = base_labels;
                // The oracle, computed the PRE-CHANGE way from exactly
                // the samples the state retained.
                let mut want: Vec<(LabelSet, Vec<(i64, u64)>)> = Vec::new();
                for group in state.groups.values() {
                    let MutCells::Samples(samples) = &group.cells else {
                        panic!("{op:?} must retain samples");
                    };
                    let mut cells: HashMap<i64, Vec<WinSample>> = HashMap::new();
                    for s in samples {
                        let (lo, hi) = state.covering_k(s.ts);
                        for k in lo..=hi {
                            cells.entry(k).or_default().push(*s);
                        }
                    }
                    let mut keys: Vec<i64> = cells.keys().copied().collect();
                    keys.sort_unstable();
                    let mut points: Vec<(i64, u64)> = Vec::new();
                    for k in keys {
                        let pts = cells.get_mut(&k).expect("key present");
                        pts.sort_by(win_order);
                        if let Some(v) = reduce_window(
                            op,
                            state.class,
                            state.param,
                            state.rate_window_ns,
                            0,
                            pts,
                        ) {
                            points.push((state.grid_point(k), v.to_bits()));
                        }
                    }
                    want.push((group.labels.clone(), points));
                }
                want.sort();
                let QueryResult::Matrix(items) = state.finish_in_place().expect("finish") else {
                    panic!("expected a matrix");
                };
                let mut got: Vec<(LabelSet, Vec<(i64, u64)>)> = items
                    .into_iter()
                    .map(|s| {
                        (
                            s.labels,
                            s.points
                                .into_iter()
                                .map(|(t, v)| (t, v.to_bits()))
                                .collect(),
                        )
                    })
                    .collect();
                got.sort();
                want.retain(|(_, p)| !p.is_empty());
                assert!(!want.is_empty(), "{op:?}: the fixture must emit");
                assert_eq!(
                    got, want,
                    "{op:?} at step={step} range={range}: the two-pointer sweep must \
                     reproduce the per-cell reduction it replaces"
                );
            }
        }
    }

    /// AC 13 — **charged retention is independent of the window width.**
    ///
    /// The SAME data at `W = ceil(range/step) = 1, 5, 40` charges the same
    /// number of retention points, where the pre-change per-cell form
    /// charged one per covering cell — a `~W x` factor. The expanded
    /// count is recomputed in-test from `covering_k` so the win is
    /// measured, not asserted from memory.
    #[test]
    fn mutating_retention_is_independent_of_the_window_width() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let rows: Vec<MetricScanRow> = (1..=6)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i * 13,
                body: format!("a={i}"),
            })
            .collect();
        for (op, value) in [
            (RangeAggOp::CountOverTime, ClientValue::Count),
            (RangeAggOp::MaxOverTime, ClientValue::Unwrap),
        ] {
            let unwrap = matches!(value, ClientValue::Unwrap);
            let query = if unwrap {
                r#"{x="y"} | logfmt | label_format zone="eu" | unwrap a"#
            } else {
                r#"{x="y"} | logfmt | label_format zone="eu""#
            };
            let client = ClientAgg {
                pipeline: parse_pipeline(query),
                value,
                range_op: op,
                param: None,
                absent_labels: vec![],
            };
            let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
            assert!(compiled.metric_mutates_labels());
            let mut charged: Vec<u64> = Vec::new();
            let mut expanded: Vec<u64> = Vec::new();
            for w in [1u64, 5, 40] {
                let step = 10u64;
                let window = slide_window(0, 2_000, step, w * step);
                let mut state =
                    RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                        .unwrap();
                state.push_rows(&rows).expect("fold");
                charged.push(state.retained);
                // What the per-cell form WOULD have charged: one unit per
                // (sample, covering cell), in the same `per_sample` unit.
                let cells: u64 = rows
                    .iter()
                    .map(|r| {
                        let (lo, hi) = state.covering_k(r.timestamp_ns);
                        if lo > hi { 0 } else { (hi - lo + 1) as u64 }
                    })
                    .sum();
                expanded.push(cells * state.per_sample);
                state.finish_in_place().expect("finish");
                assert_eq!(state.retained, 0, "{op:?} W={w}: charge returns to zero");
            }
            assert_eq!(
                charged[0], charged[1],
                "{op:?}: W=1 and W=5 must charge the same, got {charged:?}"
            );
            assert_eq!(
                charged[0], charged[2],
                "{op:?}: W=1 and W=40 must charge the same, got {charged:?}"
            );
            // ...and the per-cell form it replaces grew with W.
            assert!(
                expanded[2] > 10 * charged[2],
                "{op:?}: the fixture must actually exercise a wide window \
                 (per-cell would charge {} vs {} now)",
                expanded[2],
                charged[2]
            );
        }
    }

    /// AC8 (review round 2 gap): the concurrent-retention invariant on the
    /// MUTATING path — every [`MutCells`] arm charges `retained`, and the
    /// whole charge is released when the state is finished/dropped.
    ///
    /// One of the two tests plan v14 permits to change for issue #236 Part
    /// C: it asserts the REPRESENTATION, which C1/C2 replace (`int_cells`
    /// → `MutCells::IntExpanded`/`IntDeltas`, `pt_cells` →
    /// `MutCells::Samples`). The invariant it pins is unchanged.
    #[test]
    fn sliding_mutating_fan_out_charges_and_releases_retention_for_both_cell_kinds() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        // A `label_format` makes the output set differ from the base ⇒ the
        // mutating fan-out path (`fan_out == true`).
        for (op, value, expect_int_cells) in [
            // Class A -> the integer arms.
            (RangeAggOp::CountOverTime, ClientValue::Count, true),
            // Class B -> `MutCells::Samples` (retained points).
            (RangeAggOp::MaxOverTime, ClientValue::Unwrap, false),
            // Class B with a per-emit scratch charge (`per_sample == 2`):
            // the fan-out cells must charge in the SAME unit the streaming
            // slider does, or the discharge at finish cannot balance.
            (RangeAggOp::QuantileOverTime, ClientValue::Unwrap, false),
        ] {
            let pipeline = if matches!(value, ClientValue::Unwrap) {
                parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu" | unwrap a"#)
            } else {
                parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#)
            };
            let client = ClientAgg {
                pipeline,
                value,
                range_op: op,
                param: matches!(op, RangeAggOp::QuantileOverTime).then_some(0.5),
                absent_labels: vec![],
            };
            let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
            assert!(compiled.metric_mutates_labels(), "must be the fan-out path");
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            let rows: Vec<MetricScanRow> = (1..=5)
                .map(|i| MetricScanRow {
                    fingerprint: 1,
                    timestamp_ns: i * 10,
                    body: "a=1".to_string(),
                })
                .collect();
            state.push_rows(&rows).expect("fan-out fold");
            assert!(
                state.retained > 0,
                "{op:?}: the mutating fan-out must CHARGE retention"
            );
            let group = state.groups.values().next().expect("one output group");
            assert!(!group.cells.is_empty(), "{op:?}: cells populated");
            if expect_int_cells {
                // `range (30) >= step (10)` and `kmax > 0`, so class A is
                // on the DELTA arm here (C1's predicate).
                assert!(
                    matches!(group.cells, MutCells::IntDeltas(_)),
                    "{op:?}: overlapping windows take the delta arm"
                );
                assert_eq!(
                    state.retained,
                    group.cells.charged_units(),
                    "{op:?}: every created int cell is charged exactly once"
                );
            } else {
                assert!(
                    matches!(group.cells, MutCells::Samples(_)),
                    "{op:?}: classes B/C retain samples"
                );
                assert_eq!(
                    state.retained,
                    group.cells.charged_units() * retention_points_per_sample(op),
                    "{op:?}: every retained point is charged exactly once, in `per_sample` units"
                );
            }
            // RELEASE leg (review round 3, finding 3): drive the in-place
            // finish so the discharge is OBSERVABLE — the charge must return
            // to exactly zero. Deleting the discharge in `finish_in_place`
            // reddens this assertion (proved by tripping it).
            let out = state.finish_in_place().expect("finish");
            assert!(matches!(out, QueryResult::Matrix(ref m) if !m.is_empty()));
            assert_eq!(
                state.retained, 0,
                "{op:?}: every mutating-path charge must be discharged at finish"
            );
        }
    }

    /// Review round 5, finding 2: `rate_counter` re-reduces over a BORROW of
    /// the live window instead of copying the whole window into an owned
    /// `Vec<(i64, f64)>` at every grid point. The refactor must be
    /// arithmetically INERT — pinned bit-for-bit (`to_bits`, so a low-bit
    /// float drift or a reordered reset walk reddens) against an INDEPENDENT
    /// transcription of the owned-`Vec` form below. Deliberately NOT compared
    /// against `rate_counter_extrapolated`: that now delegates to the very
    /// function under test, so such a comparison would move with the code and
    /// prove nothing.
    #[test]
    fn rate_counter_over_a_borrowed_window_is_bit_identical_to_the_owning_form() {
        /// The pre-refactor owned-`Vec` implementation, transcribed verbatim
        /// and kept independent of the production code path.
        fn reference(mut pts: Vec<(i64, f64)>, rate_window_ns: Option<u64>) -> f64 {
            if pts.len() < 2 {
                return 0.0;
            }
            let Some(rng_ns) = rate_window_ns.filter(|w| *w > 0) else {
                return 0.0;
            };
            pts.sort_by(|(at, _), (bt, _)| at.cmp(bt));
            let first_t = pts[0].0;
            let last_t = pts[pts.len() - 1].0;
            let first_f = pts[0].1;
            let last_f = pts[pts.len() - 1].1;
            let mut result_value = last_f - first_f;
            let mut last_value = 0.0_f64;
            for &(_, f) in &pts {
                if f < last_value {
                    result_value += last_value;
                }
                last_value = f;
            }
            let sel_range_ms: i128 = (rng_ns / 1_000_000) as i128;
            let range_start = first_t as i128 - sel_range_ms;
            let mut duration_to_start = (first_t as i128 - range_start) as f64 / 1000.0;
            let duration_to_end = 0.0_f64;
            let sampled_interval = (last_t as i128 - first_t as i128) as f64 / 1000.0;
            let average_duration = sampled_interval / (pts.len() - 1) as f64;
            if result_value > 0.0 && first_f >= 0.0 {
                let duration_to_zero = sampled_interval * (first_f / result_value);
                if duration_to_zero < duration_to_start {
                    duration_to_start = duration_to_zero;
                }
            }
            let threshold = average_duration * 1.1;
            let mut extrapolate_to = sampled_interval;
            extrapolate_to += if duration_to_start < threshold {
                duration_to_start
            } else {
                average_duration / 2.0
            };
            extrapolate_to += if duration_to_end < threshold {
                duration_to_end
            } else {
                average_duration / 2.0
            };
            result_value *= extrapolate_to / sampled_interval;
            // `selRange.Seconds()`, transcribed inline (issue #232) rather
            // than calling `range_seconds` — this reference form stays
            // independent of the production code it pins.
            result_value / ((rng_ns / 1_000_000_000) as f64 + (rng_ns % 1_000_000_000) as f64 / 1e9)
        }

        let windows: Vec<Vec<(i64, f64)>> = vec![
            vec![],
            vec![(10, 5.0)],
            vec![(10, 1.0), (20, 2.0), (30, 4.5)],
            // Counter resets (the reset-aware accumulation order matters).
            vec![(10, 9.0), (20, 3.0), (30, 11.25), (40, 2.5), (50, 7.125)],
            // Same-timestamp neighbours (canonical order keeps them adjacent).
            vec![(10, 1.5), (10, 2.5), (10, 0.25), (25, 9.75)],
            // Values that exercise the extrapolation-to-zero branch.
            vec![(0, 0.0), (1_000_000, 3.3), (5_000_000, 3.3000000000000003)],
        ];
        for pts in &windows {
            let ordered: Vec<WinSample> = pts
                .iter()
                .enumerate()
                .map(|(i, &(ts, value))| WinSample {
                    ts,
                    stream_hash: 7,
                    tie_rank: i as u32,
                    value,
                })
                .collect();
            // `1_118_000_000` is a NON-whole-second width, where the
            // reference's two-rounding `Seconds()` and the naive
            // `ns as f64 / 1e9` disagree by an ULP (issue #232) — so the
            // divisor is pinned here too, not just the extrapolation.
            for rate_window_ns in [None, Some(0u64), Some(30_000_000_000), Some(1_118_000_000)] {
                let borrowed = reduce_window(
                    RangeAggOp::RateCounter,
                    ReducerClass::CanonicalFold,
                    None,
                    rate_window_ns,
                    0,
                    &ordered,
                );
                let owned = if ordered.is_empty() {
                    None
                } else {
                    Some(reference(pts.clone(), rate_window_ns))
                };
                match (borrowed, owned) {
                    (None, None) => {}
                    (Some(b), Some(o)) => assert_eq!(
                        b.to_bits(),
                        o.to_bits(),
                        "borrowed {b} != owned {o} for {pts:?} / {rate_window_ns:?}"
                    ),
                    (b, o) => panic!("presence differs: {b:?} vs {o:?} for {pts:?}"),
                }
            }
        }
    }

    /// Review round 5, finding 2: on the MUTATING fan-out path the cap gate
    /// runs BEFORE the cell/point insertion, for both cell kinds — the
    /// allocation the cap exists to refuse is never made. Driven through a
    /// tiny `retention_cap` (materializing four million cells would not be a
    /// test); if the charge were applied after the insert, `retained` would
    /// settle at `cap + 1` and the container would hold the refused entry.
    #[test]
    fn fan_out_retention_cap_refuses_the_breaching_cell_before_inserting_it() {
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        // range 30 / step 10 ⇒ each sample fans into up to 3 grid cells.
        let window = slide_window(0, 100, 10, 30);
        const TINY_CAP: u64 = 4;
        for (op, value, integer) in [
            (RangeAggOp::CountOverTime, ClientValue::Count, true),
            (RangeAggOp::QuantileOverTime, ClientValue::Unwrap, false),
        ] {
            let pipeline = if matches!(value, ClientValue::Unwrap) {
                parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu" | unwrap a"#)
            } else {
                parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#)
            };
            let client = ClientAgg {
                pipeline,
                value,
                range_op: op,
                param: matches!(op, RangeAggOp::QuantileOverTime).then_some(0.5),
                absent_labels: vec![],
            };
            let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            state.caps.retention_points = TINY_CAP;
            let rows: Vec<MetricScanRow> = (1..=20)
                .map(|i| MetricScanRow {
                    fingerprint: 1,
                    timestamp_ns: i * 10,
                    body: "a=1".to_string(),
                })
                .collect();
            match state.push_rows(&rows) {
                Err(ReadError::QueryTooBroad(TooBroadReason::MetricRetention { cap, count })) => {
                    assert_eq!(cap, TINY_CAP);
                    assert!(count > TINY_CAP, "{op:?}: the error names the breach");
                }
                other => panic!("{op:?}: expected MetricRetention, got {other:?}"),
            }
            // What the containers ACTUALLY hold, in charge units.
            let held: u64 = state
                .groups
                .values()
                .map(|g| {
                    g.cells.charged_units()
                        * if integer {
                            1
                        } else {
                            retention_points_per_sample(op)
                        }
                })
                .sum();
            assert_eq!(
                state.retained, held,
                "{op:?}: the counter must match what is actually held"
            );
            assert!(
                held <= TINY_CAP,
                "{op:?}: the breaching cell was inserted anyway ({held} held vs cap {TINY_CAP})"
            );
        }
    }

    /// Review round 2 finding 2: a HOSTILE client-controlled duration is
    /// rejected cleanly at the planner boundary — never narrowed/wrapped.
    #[test]
    fn hostile_durations_are_rejected_at_the_planner_boundary() {
        use super::super::params::MAX_DURATION_NS;
        // In-domain values pass.
        assert_eq!(
            super::super::params::validate_duration_ns(60_000_000_000, "step")
                .unwrap()
                .get(),
            60_000_000_000
        );
        assert_eq!(
            super::super::params::validate_duration_ns(MAX_DURATION_NS as u64, "step")
                .unwrap()
                .get(),
            MAX_DURATION_NS
        );
        // Zero and everything above the domain are named 400s, NOT wraps.
        // Round 10: the domain is the reference's full positive int64, so
        // `MAX_DURATION_NS + 1` IS `i64::MAX as u64 + 1` — the first value
        // that would narrow to a NEGATIVE i64.
        for hostile in [0u64, MAX_DURATION_NS as u64 + 1, u64::MAX] {
            match super::super::params::validate_duration_ns(hostile, "range selector") {
                Err(ReadError::DurationOutOfRange { value, max, .. }) => {
                    assert_eq!(value, hostile);
                    assert_eq!(max, MAX_DURATION_NS);
                }
                other => panic!("hostile duration {hostile} must be rejected, got {other:?}"),
            }
        }
    }

    /// Review round 4, finding 2: the instant/range modelling. A `Range`
    /// window structurally REQUIRES two `ValidatedDuration`s (no zero/absent
    /// representation exists), and an `Instant` window structurally has no
    /// step and no range — so `RangeSlideState` can only receive a real,
    /// validated, non-zero range. `ValidatedDuration::NONE` is gone.
    #[test]
    fn a_range_window_cannot_carry_an_absent_or_zero_range() {
        // The only way to obtain the durations a `Range` window needs.
        let step = super::super::params::validate_duration_ns(10, "step").unwrap();
        let range = super::super::params::validate_duration_ns(30, "range selector").unwrap();
        let w = ClientWindow::Range {
            grid_start_ns: 0,
            end_ns: 100,
            step_ns: step,
            range_ns: range,
        };
        assert_eq!(w.step_ns(), Some(step));
        assert!(range.get() > 0, "a validated range is never zero");
        // And zero can never be minted: the validator rejects it, so no
        // `ValidatedDuration` carrying 0 exists to place in the range slot.
        assert!(super::super::params::validate_duration_ns(0, "range selector").is_err());
        // An instant window has neither a step nor a range slot at all.
        let i = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        assert_eq!(i.step_ns(), None);
    }

    /// Review round 2 finding 1: the `absent_over_time` presence counters are
    /// `i64`, whose maximum magnitude is the number of surviving collision
    /// groups — itself bounded by the byte-scan budget ceiling (one byte per
    /// row minimum). This test pins the arithmetic identity the proof rests
    /// on: the counter stays exact well past the old `i32` ceiling.
    #[test]
    fn absent_presence_counter_cannot_overflow_under_the_scan_budget() {
        // The structural bound: rows <= scan-budget bytes < i64::MAX.
        let budget_ceiling: u64 = pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING;
        assert!(
            (budget_ceiling as u128) < i64::MAX as u128,
            "the byte-scan budget ceiling must bound the group count inside i64"
        );
        // And the counter type genuinely holds a value the old `i32` could
        // not: accumulate PAST the i32 ceiling one increment at a time (the
        // exact `+= 1` shape `flush_collision` uses) and stay exact.
        let mut cover: Vec<i64> = vec![0; 2];
        let start = i32::MAX as i64 - 5;
        cover[0] = start;
        for _ in 0..10 {
            cover[0] += 1;
        }
        assert_eq!(
            cover[0],
            start + 10,
            "counter must stay exact past i32::MAX"
        );
        assert!(cover[0] > i32::MAX as i64, "exceeds the old i32 ceiling");
        // The symmetric decrement is equally exact.
        cover[1] -= cover[0];
        assert_eq!(cover[0] + cover[1], 0, "difference array nets to zero");
    }

    /// Review round 3, finding 1: the collision group is bounded in BYTES,
    /// not just member count — a handful of very large same-`(fingerprint,
    /// ts)` bodies trips the clean error long before the 10 000-member cap,
    /// and the staged buffer never exceeds `MAX_TS_COLLISION_GROUP_BYTES`.
    #[test]
    fn collision_group_byte_cap_trips_before_staging_large_bodies() {
        // An order-DEPENDENT reducer, so bodies are staged for `tie_rank`.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | unwrap a"#),
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::SumOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();

        // 1 MiB bodies at the SAME (fingerprint, ts): only ~8 fit under the
        // 8 MiB byte cap — far below the 10 000-member cap, so the byte
        // dimension is what bites.
        let big = "x".repeat(1024 * 1024);
        let rows: Vec<MetricScanRow> = (0..64)
            .map(|_| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("a=1 {big}"),
            })
            .collect();
        match state.push_rows(&rows) {
            Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup {
                count, cap, ..
            })) => {
                assert_eq!(cap, MAX_TS_COLLISION_GROUP);
                assert!(
                    count < MAX_TS_COLLISION_GROUP,
                    "the BYTE cap must trip well before the member cap, tripped at {count}"
                );
            }
            other => panic!("expected TsCollisionGroup from the byte cap, got {other:?}"),
        }
        // Bound proof, observed: the staged buffer never exceeded the cap.
        assert!(
            state.coll_bytes <= MAX_TS_COLLISION_GROUP_BYTES,
            "staged {} bytes exceeds the {MAX_TS_COLLISION_GROUP_BYTES}-byte cap",
            state.coll_bytes
        );
    }

    /// Review round 4, finding 1: the byte cap covers the FAN-OUT staging
    /// (rendered label JSON + cloned `LabelSet`) for a reducer that stages NO
    /// body — the exact hole in the round-3 accounting. Class A
    /// (`count_over_time`, `needs_body_order == false`) with a label-mutating
    /// pipeline extracting huge label values must still trip the byte cap.
    #[test]
    fn collision_group_byte_cap_covers_fan_out_staging_without_bodies() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(compiled.metric_mutates_labels(), "must be the fan-out path");
        let state_class = reducer_class(client.range_op, client.value);
        assert_eq!(
            state_class,
            ReducerClass::InvertInteger,
            "this case must be the class that stages NO body"
        );
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        assert!(
            !state.needs_body_order,
            "the hole was that no body ⇒ nothing was charged"
        );
        // Each line carries a ~1 MiB extracted label VALUE — no body is
        // staged, but the rendered JSON + cloned LabelSet are.
        let big = "v".repeat(1024 * 1024);
        let rows: Vec<MetricScanRow> = (0..64)
            .map(|_| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("big={big}"),
            })
            .collect();
        match state.push_rows(&rows) {
            Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup {
                count,
                bytes,
                bytes_cap,
                ..
            })) => {
                assert!(
                    count < MAX_TS_COLLISION_GROUP,
                    "the BYTE dimension must trip, not the member count ({count})"
                );
                assert!(bytes > bytes_cap, "the error reports the byte breach");
            }
            other => panic!("fan-out staging must be byte-capped, got {other:?}"),
        }
        assert!(
            state.coll_bytes <= MAX_TS_COLLISION_GROUP_BYTES,
            "staged {} bytes exceeds the cap",
            state.coll_bytes
        );
    }

    /// The byte/count caps must ERROR, never silently truncate a group into a
    /// partial (and therefore wrong) `tie_rank` order — including when the
    /// group STRADDLES a fetch-chunk boundary. Pushing the same group in two
    /// batches must accumulate one continuous charge and fail cleanly.
    #[test]
    fn a_capped_collision_group_errors_rather_than_truncating_across_chunks() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | unwrap a"#),
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::SumOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // ~512 KiB bodies: 5 members charge ~5.3 MiB (the round-7 block
        // model charges 2× the body clone) — under the 8 MiB cap; the
        // second chunk's members push the same open group past it.
        let big = "x".repeat(512 * 1024);
        let chunk: Vec<MetricScanRow> = (0..5)
            .map(|_| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("a=1 {big}"),
            })
            .collect();
        // First chunk: under the cap, group left OPEN (straddling).
        state.push_rows(&chunk).expect("first chunk under the cap");
        let after_first = state.coll_bytes;
        assert!(after_first > 0, "the straddling group carried a charge");
        assert_eq!(state.coll.len(), 5, "group stayed open across the chunk");
        // Second chunk continues the SAME (fingerprint, ts) group — the charge
        // must accumulate continuously (no reset, no double count) and trip.
        match state.push_rows(&chunk) {
            Err(ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup { .. })) => {}
            other => panic!("the straddling group must trip the cap cleanly, got {other:?}"),
        }
        assert!(
            state.coll_bytes <= MAX_TS_COLLISION_GROUP_BYTES,
            "the accumulated charge stayed inside the cap"
        );
        // The error is terminal for the query: nothing is emitted from a
        // partially-staged group, so no truncated tie order can escape.
        assert!(
            state.coll_bytes >= after_first,
            "charge accumulated, not reset"
        );
    }

    /// Issue #227 review round 7, finding 2: the per-allocation charge is
    /// a PROVABLE upper bound on the block a real allocator retains, not
    /// just on the request size — a 33-byte request can retain a 48/64-
    /// byte block under size-class rounding. Pins [`alloc_block_bytes`]
    /// against an independent worst-of-mainstream retained-block model at
    /// sizes straddling every class boundary up to 1 MiB:
    /// `max(next_pow2(n), align16(n + 8), 32)` — powers of two are the
    /// coarsest mainstream class grid (jemalloc/mimalloc/tcmalloc are all
    /// finer), and `align16(n + 8)` is glibc's inline-header chunk. The
    /// boundary cases (33, 65, 129, ...) FAIL if the 2× rounding is
    /// removed (a request-size charge of `n.max(32)` sits strictly below
    /// the model there) — asserted explicitly so the test is provably
    /// non-vacuous.
    /// Issue #221 AC 17: the `approx_topk` 13-row accounting is a
    /// compile-time constant — the layout sizes, the capacity constants
    /// and the row arithmetic are all pinned, and the total recomputed
    /// through the codebase's own allocator model
    /// ([`alloc_block_bytes`]/[`grown_alloc_bytes`]) must stay within the
    /// documented 7_360_882-byte ceiling. Asserts the COMPUTED ceiling,
    /// never a measured allocation count (the alloc-bound-test lesson: a
    /// process-global counter flakes on stray allocations).
    #[test]
    fn approx_topk_accounting_total_is_a_compile_time_constant() {
        // Layout pins — a struct-layout change invalidates rows 5/7/12.
        assert_eq!(std::mem::size_of::<cms::SeriesKey>(), 16);
        assert_eq!(std::mem::size_of::<(u32, cms::SeriesKey)>(), 24);
        assert_eq!(std::mem::size_of::<InstantSeries>(), 32);
        // Capacity constants (the `with_capacity(R)` reservations).
        assert_eq!(cms::CMS_WIDTH, 27_183);
        assert_eq!(cms::CMS_DEPTH, 7);
        assert_eq!(cms::CMS_MAX_LABELS, 10_000);
        let r = cms::CMS_MAX_LABELS as u64 + 1; // transient post-push size

        // Row 4: the exact `vec![0.0; W*D]` counter grid.
        let grid = u64::from(cms::CMS_DEPTH) * u64::from(cms::CMS_WIDTH) * 8;
        assert_eq!(grid, 1_522_248);
        // Row 6: hashbrown's `with_capacity(R)` table — `R*8/7` rounded
        // to the next power of two buckets, 8 B per u64 slot + 1 ctrl
        // byte per bucket + 16 trailing ctrl bytes.
        let buckets = (r * 8).div_ceil(7).next_power_of_two();
        let observed = buckets * 8 + buckets + 16;
        assert_eq!(observed, 147_472);

        // The 13 rows (1-3 are the zero-allocation in-place/streaming
        // rows), each pinned to the doc-table figure on
        // `approx_topk_instant`.
        let rows: [(u64, u64); 10] = [
            (alloc_block_bytes(grid), 3_044_496),             // 4: CMS grid
            (alloc_block_bytes(24 * r), 480_048),             // 5: retention heap
            (alloc_block_bytes(observed), 294_944),           // 6: observed set
            (alloc_block_bytes(32 * r), 640_064),             // 7: retained output
            (1_024, 1_024),                                   // 8a: groups table
            (grown_alloc_bytes(8 * r), 480_048),              // 8b: member indices
            (alloc_block_bytes(r), 20_002),                   // 9: keep
            (alloc_block_bytes(16 * r), 320_032),             // 10: candidates
            (alloc_block_bytes(16 * r.div_ceil(2)), 160_032), // 11: sort scratch
            (grown_alloc_bytes(32 * r), 1_920_192),           // 12: selection output
        ];
        let mut total = 0u64;
        for (i, (computed, pinned)) in rows.iter().enumerate() {
            assert_eq!(computed, pinned, "accounting row {i} drifted");
            total += computed;
        }
        assert!(
            total <= 7_360_882,
            "approx_topk peak accounting total {total} exceeds the documented ceiling"
        );
    }

    #[test]
    fn alloc_block_bytes_covers_allocator_size_class_rounding_at_class_boundaries() {
        /// The worst retained block any mainstream allocator keeps for an
        /// `n`-byte request (see the test doc).
        fn worst_mainstream_retained(n: u64) -> u64 {
            let pow2_class = n.max(1).next_power_of_two();
            let glibc_chunk = (n + 8).div_ceil(16) * 16;
            pow2_class.max(glibc_chunk).max(32)
        }

        let sizes: &[u64] = &[
            1,
            8,
            16,
            17,
            24,
            31,
            32,
            33,
            47,
            48,
            63,
            64,
            65,
            96,
            127,
            128,
            129,
            192,
            255,
            256,
            257,
            511,
            512,
            513,
            1023,
            1024,
            1025,
            4095,
            4096,
            4097,
            65_535,
            65_536,
            65_537,
            1 << 20,
            (1 << 20) + 1,
        ];
        for &n in sizes {
            let charge = alloc_block_bytes(n);
            let worst = worst_mainstream_retained(n);
            assert!(
                charge >= worst,
                "alloc_block_bytes({n}) = {charge} under-counts a retained \
                 block of {worst}"
            );
            // The realloc peak: the grown buffer's request is ≤ 2n and the
            // old ≤ n-byte buffer is still mapped — BOTH size-class-rounded.
            let grown = grown_alloc_bytes(n);
            let peak = worst_mainstream_retained(2 * n) + worst;
            assert!(
                grown >= peak,
                "grown_alloc_bytes({n}) = {grown} under-counts the realloc \
                 peak of {peak}"
            );
        }
        // Non-vacuous: at the class-boundary sizes the pre-round-7
        // request-size charge (`n.max(32)`) sits BELOW the retained block,
        // so removing the rounding turns the asserts above red.
        for &n in &[33u64, 65, 129, 257, 1025, 4097] {
            assert!(
                worst_mainstream_retained(n) > n.max(MIN_ALLOC_BYTES),
                "size {n} must straddle a class boundary for this test to \
                 catch a removed rounding"
            );
        }
    }

    /// Review round 5, finding 1: the charge sizes the rendered JSON by
    /// walking the SAME escape arms the renderer uses, so the two cannot
    /// drift. Covers every arm: the two-byte escapes, the `\u00xx` six-byte
    /// C0 expansion (the one the round-4 charge counted as ONE byte),
    /// verbatim ASCII, multi-byte UTF-8, empty strings and the empty set.
    #[test]
    fn rendered_labels_json_len_matches_the_renderer_byte_for_byte() {
        let cases: Vec<Vec<(Cow<'_, str>, Cow<'_, str>)>> = vec![
            vec![],
            vec![(Cow::Borrowed(""), Cow::Borrowed(""))],
            vec![(Cow::Borrowed("app"), Cow::Borrowed("checkout"))],
            // Every short escape.
            vec![(Cow::Borrowed("q"), Cow::Borrowed("\"\\\n\r\t\u{08}\u{0C}"))],
            // Every C0 byte that has no short escape ⇒ six bytes each.
            vec![(
                Cow::Borrowed("c0"),
                Cow::Owned((0u8..0x20).map(|b| b as char).collect::<String>()),
            )],
            // Multi-byte UTF-8 is copied verbatim at its own width.
            vec![
                (Cow::Borrowed("é"), Cow::Borrowed("日本語")),
                (Cow::Borrowed("emoji"), Cow::Borrowed("🚀🚀")),
            ],
            // A dense mixture across several pairs (comma punctuation).
            vec![
                (Cow::Borrowed("a"), Cow::Borrowed("1")),
                (Cow::Borrowed("b"), Cow::Owned("\u{1}x\"y".repeat(7))),
                (Cow::Borrowed("c"), Cow::Borrowed("plain")),
            ],
        ];
        for pairs in &cases {
            assert_eq!(
                rendered_labels_json_len(pairs),
                render_labels_json_sorted(pairs).len() as u64,
                "sized length must equal the rendered length for {pairs:?}"
            );
        }
    }

    /// Review round 5, finding 1: `member_stage_bytes` must be a true UPPER
    /// BOUND on what `stage_member` ACTUALLY allocates. Non-tautological by
    /// construction — the fixtures drive the real `stage_member` and the
    /// assertion measures the live `capacity()` of every buffer it created
    /// (`coll`'s element buffer, the body clone, the rendered JSON, the
    /// cloned `LabelSet` and each of its owned strings), then compares that
    /// to the charge the caps were checked against.
    ///
    /// The fixtures are the inputs that beat the round-4 accounting: an
    /// all-control-character label value (six rendered bytes per input byte,
    /// counted once before), many one-byte labels (whose real allocations
    /// cannot go below the allocator's minimum block), and enough members to
    /// force `coll`'s capacity to double repeatedly.
    #[test]
    fn member_stage_bytes_upper_bounds_every_allocation_stage_member_makes() {
        /// The live heap footprint of everything currently staged in `coll`.
        fn staged_capacity(state: &RangeSlideState<'_>) -> u64 {
            let mut n = (state.coll.capacity() * size_of::<CollMember>()) as u64;
            for m in &state.coll {
                n += m.body.capacity() as u64;
                if let Some((key, labels)) = &m.out {
                    n += key.capacity() as u64;
                    n += (labels.capacity() * size_of::<(String, String)>()) as u64;
                    for (k, v) in labels {
                        n += (k.capacity() + v.capacity()) as u64;
                    }
                }
            }
            n
        }

        // Class C (`sum_over_time` over `unwrap`) ⇒ bodies staged for
        // `tie_rank`; `label_format` ⇒ the fan-out `key`/`LabelSet` staged
        // too. Both legs of the charge are exercised at once.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu" | unwrap a"#),
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::SumOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);

        let control: String = std::iter::repeat_n('\u{1}', 512).collect();
        struct Fixture {
            name: &'static str,
            body: String,
            labels: Vec<(String, String)>,
        }
        let fixtures = vec![
            // Worst-case escaping: every value byte renders as six.
            Fixture {
                name: "all-control-character values",
                body: "a=1".to_string(),
                labels: vec![
                    ("k".to_string(), control.clone()),
                    ("kk".to_string(), control.clone()),
                ],
            },
            // Minimum-block worst case: 64 one-byte keys and values.
            Fixture {
                name: "many one-byte labels",
                body: "a=1".to_string(),
                labels: (0..64u32)
                    .map(|i| {
                        (
                            char::from(b'a' + (i % 26) as u8).to_string(),
                            "x".to_string(),
                        )
                    })
                    .collect(),
            },
            // No labels at all (charge must still cover the slot + body).
            Fixture {
                name: "no labels",
                body: "a=1 ".repeat(300),
                labels: Vec::new(),
            },
            // Mixed escapes and multi-byte UTF-8.
            Fixture {
                name: "mixed escapes and multi-byte",
                body: "a=1".to_string(),
                labels: vec![
                    ("q\"k".to_string(), "v\\\n\t\u{7}".to_string()),
                    ("日本".to_string(), "🚀".repeat(40)),
                ],
            },
        ];

        for Fixture { name, body, labels } in &fixtures {
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            assert!(state.needs_body_order, "{name}: bodies must be staged");
            assert!(state.fan_out, "{name}: label output must be staged");
            // Enough members to force `coll` to grow through several
            // capacity doublings (4 → 8 → 16 → 32).
            for _ in 0..20 {
                let row = MetricScanRow {
                    fingerprint: 1,
                    timestamp_ns: 5,
                    body: body.clone(),
                };
                let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = labels
                    .iter()
                    .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str())))
                    .collect();
                state.stage_member(&row, 1.0, &mut scratch).expect("staged");
                assert!(
                    state.coll_bytes >= staged_capacity(&state),
                    "{name}: charge {} under-counts the {} bytes actually allocated \
                     after {} members",
                    state.coll_bytes,
                    staged_capacity(&state),
                    state.coll.len()
                );
            }
        }
    }

    /// Issue #227 review round 7, finding 2 (the container-slot leg of the
    /// same allocator-rounding model): the per-member `CollMember` slot
    /// charge must cover the `coll` buffer's SIZE-CLASS-ROUNDED realloc
    /// peak — live capacity (doubling growth, initial `min_non_zero_cap`
    /// of 4) plus the old buffer still mapped during the realloc, each
    /// retained at up to the worst mainstream block for its request.
    /// Drives the REAL `member_stage_bytes` (a class-A state stages
    /// nothing but the slot, so the returned charge IS the per-member
    /// slot charge) and replays `Vec`'s exact growth schedule; reverting
    /// the slot factor to the pre-round-7 `4×` fails at `n = 1` and at
    /// every realloc point.
    #[test]
    fn coll_member_slot_charge_covers_the_size_class_rounded_realloc_peak() {
        fn worst_mainstream_retained(n: u64) -> u64 {
            let pow2_class = n.max(1).next_power_of_two();
            let glibc_chunk = (n + 8).div_ceil(16) * 16;
            pow2_class.max(glibc_chunk).max(32)
        }

        // A class-A reducer with no fan-out: no body staged, no labels
        // staged — `member_stage_bytes` returns exactly the slot charge.
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"}"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        let state = RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
            .unwrap();
        assert!(!state.needs_body_order && !state.fan_out);
        let row = MetricScanRow {
            fingerprint: 1,
            timestamp_ns: 5,
            body: "x".to_string(),
        };
        let slot_charge = state.member_stage_bytes(&row, &[], false);

        let slot = size_of::<CollMember>() as u64;
        let mut cap: u64 = 0;
        for n in 1..=1_000u64 {
            // `RawVec`: first push reserves `min_non_zero_cap = 4`; a full
            // buffer doubles, with the old buffer live during the realloc.
            let old = if n == 1 {
                cap = 4;
                0
            } else if n > cap {
                let old = cap;
                cap *= 2;
                old
            } else {
                0
            };
            let peak = worst_mainstream_retained(cap * slot)
                + if old > 0 {
                    worst_mainstream_retained(old * slot)
                } else {
                    0
                };
            let charged = slot_charge * n;
            assert!(
                charged >= peak,
                "after {n} members the cumulative slot charge {charged} \
                 under-counts the rounded realloc peak {peak}"
            );
        }
    }

    /// Review round 6: distinct mutating-label groups' rendered keys +
    /// `LabelSet`s are QUERY-LIFETIME (they live in `groups` until finish,
    /// outliving the per-flush `coll_bytes` reset), so they are charged
    /// against the group-byte cap BEFORE the map insertion — a refused
    /// group is never inserted and the counter never records it.
    #[test]
    fn query_lifetime_group_label_bytes_are_capped_before_the_map_insertion() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        state.caps.group_bytes = 1;
        // Distinct extracted `u` values ⇒ distinct groups; distinct ts ⇒
        // singleton collision groups (the collision caps stay silent).
        let rows: Vec<MetricScanRow> = (1..=3)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: i * 10,
                body: format!("u=v{i}"),
            })
            .collect();
        match state.push_rows(&rows) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { bytes, cap })) => {
                assert_eq!(cap, 1);
                assert!(bytes > cap, "the error names the byte breach");
            }
            other => panic!("expected MetricGroupLabelBytes, got {other:?}"),
        }
        // Charge-before-insert, observed: the refused group was never
        // inserted and the counter never recorded it.
        assert!(
            state.groups.is_empty(),
            "the breaching group was inserted anyway"
        );
        assert_eq!(state.group_bytes, 0, "a refused charge must not stick");
    }

    /// Review round 6 symmetry: every insertion into the query-lifetime
    /// `groups` map passes the charge (the counter equals the sum of
    /// [`group_entry_bytes`] over the LIVE map — no bypassing path, no
    /// double charge for a repeated group), and `finish_in_place` releases
    /// every charge back to exactly zero. Deleting either the
    /// `fan_out_sample` charge or the finish discharge fails this test.
    #[test]
    fn group_label_byte_charges_match_the_live_map_and_release_at_finish() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 30);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // u=a twice (one group, charged ONCE), u=b once; the final u=c row
        // stays staged in `coll` until finish flushes — proving the
        // finish-time flush passes the same charge.
        let bodies = ["u=a", "u=b", "u=a", "u=c"];
        let rows: Vec<MetricScanRow> = bodies
            .iter()
            .enumerate()
            .map(|(i, b)| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: (i as i64 + 1) * 10,
                body: (*b).to_string(),
            })
            .collect();
        state.push_rows(&rows).expect("under every cap");
        assert_eq!(state.groups.len(), 2, "u=a (deduped) and u=b are flushed");
        let live: u64 = state
            .groups
            .iter()
            .map(|(k, g)| group_entry_bytes(k, &g.labels, MUT_GROUP_SLOT))
            .sum();
        assert!(live > 0, "the fixture must exercise a real charge");
        assert_eq!(
            state.group_bytes, live,
            "the counter must equal the live map's sized entries — every \
             insertion charged, repeated groups charged once"
        );
        let out = state.finish_in_place().expect("finish");
        assert_eq!(
            state.group_bytes, 0,
            "every group-label byte charge must be discharged at finish"
        );
        assert_eq!(state.retained, 0, "retention symmetry is undisturbed");
        match out {
            QueryResult::Matrix(series) => {
                assert_eq!(series.len(), 3, "u=a, u=b and the finish-flushed u=c");
            }
            other => panic!("expected a matrix, got {other:?}"),
        }
    }

    /// Review round 6: [`group_entry_bytes`] is a true UPPER BOUND on the
    /// heap bytes a retained group-map entry actually holds.
    /// Non-tautological — the charge is sized from LENGTHS while the
    /// assertion measures the live `capacity()` of the real entry produced
    /// by the real staging/flush path (the rendered key's capacity exceeds
    /// its length whenever escaping forces growth past the renderer's
    /// raw-byte pre-size, so an exact-length key charge fails here).
    #[test]
    fn group_entry_bytes_upper_bounds_the_retained_map_entry() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);

        let control: String = std::iter::repeat_n('\u{1}', 512).collect();
        let fixtures: Vec<(&str, Vec<(String, String)>)> = vec![
            // Worst-case escaping: the rendered key grows geometrically
            // past its raw-byte pre-size (six rendered bytes per input
            // byte), leaving capacity well above length.
            (
                "all-control-character values",
                vec![
                    ("k".to_string(), control.clone()),
                    ("kk".to_string(), control.clone()),
                ],
            ),
            // Minimum-block worst case: many one-byte strings.
            (
                "many one-byte labels",
                (0..64u32)
                    .map(|i| {
                        (
                            char::from(b'a' + (i % 26) as u8).to_string(),
                            "x".to_string(),
                        )
                    })
                    .collect(),
            ),
            // One huge extracted value (the adversarial shape the round-6
            // finding names: multi-MiB label sets × the series cap).
            (
                "one huge value",
                vec![("big".to_string(), "v".repeat(1_000_000))],
            ),
            // No labels at all (charge still covers key + slot).
            ("no labels", Vec::new()),
            // Mixed escapes and multi-byte UTF-8.
            (
                "mixed escapes and multi-byte",
                vec![
                    ("q\"k".to_string(), "v\\\n\t\u{7}".to_string()),
                    ("日本".to_string(), "🚀".repeat(40)),
                ],
            ),
        ];

        for (name, labels) in &fixtures {
            let mut state =
                RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                    .unwrap();
            assert!(state.fan_out, "{name}: must be the fan-out path");
            let row = MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: "a=1".to_string(),
            };
            let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = labels
                .iter()
                .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str())))
                .collect();
            state.coll_active = true;
            state.coll_fp = 1;
            state.coll_ts = 5;
            state.stage_member(&row, 1.0, &mut scratch).expect("staged");
            state.flush_collision(&HashMap::new()).expect("flushed");
            assert_eq!(state.groups.len(), 1, "{name}: one retained group");
            for (key, g) in &state.groups {
                // Everything measurable the entry retains (the map-table
                // share is an unmeasurable documented margin, like
                // `MIN_ALLOC_BYTES`).
                let measured = key.capacity() as u64
                    + (g.labels.capacity() * size_of::<(String, String)>()) as u64
                    + g.labels
                        .iter()
                        .map(|(k, v)| (k.capacity() + v.capacity()) as u64)
                        .sum::<u64>();
                let charge = group_entry_bytes(key, &g.labels, MUT_GROUP_SLOT);
                assert!(
                    charge >= measured,
                    "{name}: charge {charge} under-counts the {measured} bytes \
                     actually retained"
                );
                assert_eq!(
                    state.group_bytes, charge,
                    "{name}: the counter carries exactly this entry's charge"
                );
            }
        }
    }

    /// Issue #236 P1's charge, gated BEHAVIOURALLY.
    ///
    /// The non-mutating instant arm (`fp_groups`) was count-gated only
    /// before #236; Part A deleted the count cap and P1 put a byte charge
    /// in its place. Found by a mutant: deleting that charge outright was
    /// caught by NOTHING except the `logql_variants_alloc` frame census —
    /// a structural gate that notices the callee set changed, not that
    /// the bound is gone. `discharge_group_bytes` saturates, so the
    /// finish-time `group_bytes == 0` post-condition still holds with the
    /// charge removed, and both existing group-byte tests drive the two
    /// FAN-OUT arms. Plan v14 AC 14(b) asks for exactly this case; it did
    /// not land with Part A.
    #[test]
    fn instant_fp_groups_are_byte_charged_capped_and_released_at_finish() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | line_format "keep""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(
            !compiled.metric_mutates_labels(),
            "must be the NON-fan-out (fp_groups) path"
        );
        // Two fingerprints with real label bytes to charge for.
        let mut meta = slide_meta(1, r#"{"app":"a","region":"eu-west-1"}"#);
        meta.insert(
            2,
            StreamMetaRow {
                fingerprint: 2,
                service: "svc".to_string(),
                labels: r#"{"app":"b","region":"eu-west-2"}"#.to_string(),
            },
        );
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };
        let row = |fp: u64, ts: i64| MetricScanRow {
            fingerprint: fp,
            timestamp_ns: ts,
            body: "hello".to_string(),
        };

        // Trip leg: a tiny cap refuses the FIRST group BEFORE insertion.
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        state.caps.group_bytes = 1;
        match state.push_rows(&[row(1, 5)]) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { bytes, cap })) => {
                assert_eq!(cap, 1);
                assert!(bytes > cap, "the error names the byte breach");
            }
            other => panic!("expected MetricGroupLabelBytes, got {other:?}"),
        }
        assert!(
            state.fp_groups.is_empty(),
            "the breaching group was inserted anyway"
        );
        assert_eq!(state.group_bytes, 0, "a refused charge must not stick");

        // Charge/release leg: the counter equals the live map (a repeated
        // fingerprint charged once), then finish releases every charge —
        // its `debug_assert_eq!(group_bytes, 0)` is the release gate.
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        state
            .push_rows(&[row(1, 5), row(2, 6), row(1, 7)])
            .expect("under every cap");
        assert_eq!(state.fp_groups.len(), 2);
        let live: u64 = state
            .fp_groups
            .keys()
            .map(|fp| {
                group_entry_bytes(
                    "",
                    state.base_labels.get(fp).expect("hydrated"),
                    FP_GROUP_SLOT,
                )
            })
            .sum();
        assert!(live > 0, "the fixture must exercise a real charge");
        assert_eq!(
            state.group_bytes, live,
            "every fingerprint charged; the repeated one charged once"
        );
        match state.finish() {
            QueryResult::Vector(samples) => assert_eq!(samples.len(), 2),
            other => panic!("expected a vector, got {other:?}"),
        }
    }

    /// Review round 6, class completion: the INSTANT fan-out path retains
    /// the same query-lifetime key/`LabelSet` state in `label_groups`
    /// (count-capped, bytes previously uncharged) — same charge, same
    /// helper, same symmetric release at finish (whose
    /// `debug_assert_eq!(group_bytes, 0)` fails this test if the discharge
    /// is deleted).
    #[test]
    fn instant_fan_out_groups_are_byte_charged_capped_and_released_at_finish() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | label_format region="eu""#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(compiled.metric_mutates_labels(), "must be the fan-out path");
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 100,
        };

        // Trip leg: a tiny cap refuses the FIRST group before insertion.
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        state.caps.group_bytes = 1;
        let row = |ts: i64, body: &str| MetricScanRow {
            fingerprint: 1,
            timestamp_ns: ts,
            body: body.to_string(),
        };
        match state.push_rows(&[row(5, "u=a")]) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { bytes, cap })) => {
                assert_eq!(cap, 1);
                assert!(bytes > cap, "the error names the byte breach");
            }
            other => panic!("expected MetricGroupLabelBytes, got {other:?}"),
        }
        assert!(
            state.label_groups.is_empty(),
            "the breaching group was inserted anyway"
        );
        assert_eq!(state.group_bytes, 0, "a refused charge must not stick");

        // Charge/release leg: the counter equals the live map (repeated
        // group charged once), then finish releases every charge.
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        state
            .push_rows(&[row(5, "u=a"), row(6, "u=b"), row(7, "u=a")])
            .expect("under every cap");
        assert_eq!(state.label_groups.len(), 2);
        let live: u64 = state
            .label_groups
            .iter()
            .map(|(k, (labels, _))| group_entry_bytes(k, labels, INSTANT_GROUP_SLOT))
            .sum();
        assert!(live > 0, "the fixture must exercise a real charge");
        assert_eq!(
            state.group_bytes, live,
            "every insertion charged; the repeated group charged once"
        );
        match state.finish() {
            QueryResult::Vector(samples) => assert_eq!(samples.len(), 2),
            other => panic!("expected a vector, got {other:?}"),
        }
    }

    /// Review round 5, finding 3: EXPLAIN and execution share ONE SQL
    /// builder, so the reported `metric_read` query IS the query that runs —
    /// a range plan yields the PK-ordered sliding scan, an instant plan the
    /// total-order scan. (Previously EXPLAIN reported `metric_raw_samples`
    /// while a range query executed the sliding one, making the
    /// `explain_indexes` gates validate a query we never issue.)
    #[test]
    fn explain_and_execution_share_one_client_read_sql() {
        let ctx = PlanCtx {
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 1 << 40,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        };
        let window = super::super::sql::TimeWindow {
            start_ns: 0,
            end_ns: 60_000_000_000,
        };
        let svc = ["'checkout'".to_string()];
        let mk = |spec| {
            let expr = pulsus_logql::parse(r#"count_over_time({env="prod"} | logfmt [5m])"#)
                .expect("parse");
            let params = QueryParams {
                spec,
                limit: 100,
                direction: Direction::Backward,
            };
            match plan::plan(&expr, &params, &ctx).expect("plan") {
                plan::Plan::Metric(mp) => mp,
                other => panic!("expected a metric plan, got {other:?}"),
            }
        };
        // RANGE -> the PK-ordered sliding scan.
        let range_mp = mk(QuerySpec::Range {
            start_ns: 0,
            end_ns: 60_000_000_000,
            step_ns: 15_000_000_000,
        });
        let range_sql = client_metric_read_sql(&range_mp, &svc, &[1], window);
        assert!(
            range_sql.contains("ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC"),
            "range EXPLAIN/exec must report the sliding scan: {range_sql}"
        );
        // INSTANT -> the total-timestamp-order scan.
        let instant_mp = mk(QuerySpec::Instant {
            at_ns: 60_000_000_000,
        });
        let instant_sql = client_metric_read_sql(&instant_mp, &svc, &[1], window);
        assert!(
            instant_sql.contains("ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC"),
            "instant must keep its total order: {instant_sql}"
        );
    }

    /// A group of ordinary-sized bodies is unaffected by the byte cap (it
    /// must not reject anything real).
    #[test]
    fn collision_group_byte_cap_does_not_reject_ordinary_bodies() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{x="y"} | logfmt | unwrap a"#),
            value: ClientValue::Unwrap,
            range_op: RangeAggOp::SumOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 10, 10, 10);
        // Bodies differ in BYTES (trailing padding) but parse to the same
        // label set, so they form ONE 500-member collision group — the shape
        // the byte cap must not reject.
        let rows: Vec<MetricScanRow> = (0..500)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 5,
                body: format!("a=1{}", " ".repeat(i % 7)),
            })
            .collect();
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None)
            .expect("500 ordinary same-ns bodies must be served");
        assert_eq!(one_series_points(res).1, vec![(10, 500.0)]);
    }

    /// AC10: the retention cap's over-cap error is `MetricRetention` and the
    /// collision-group cap's is `TsCollisionGroup` — two DISTINCT named 422
    /// reasons, never conflated.
    #[test]
    fn sliding_over_cap_errors_are_distinct_named_reasons() {
        let retention = ReadError::QueryTooBroad(TooBroadReason::MetricRetention {
            count: MAX_RETAINED_WINDOW_POINTS + 1,
            cap: MAX_RETAINED_WINDOW_POINTS,
        })
        .to_string();
        let collision = ReadError::QueryTooBroad(TooBroadReason::TsCollisionGroup {
            count: MAX_TS_COLLISION_GROUP + 1,
            cap: MAX_TS_COLLISION_GROUP,
            bytes: 1,
            bytes_cap: MAX_TS_COLLISION_GROUP_BYTES,
        })
        .to_string();
        assert!(retention.contains("concurrent sample"), "{retention}");
        assert!(collision.contains("same-nanosecond"), "{collision}");
        assert_ne!(retention, collision);
    }

    /// Issue #227 arithmetic sweep: `rate_counter` over EXTREME timestamps
    /// (near `i64::MIN`/`i64::MAX`) must not panic in debug or wrap in
    /// release — the spans are computed in i128 (review finding 2).
    #[test]
    fn rate_counter_extreme_timestamps_do_not_overflow() {
        // A first sample near i64::MIN underflows `first_t - sel_range_ms`.
        let v = rate_counter_extrapolated(
            vec![(i64::MIN + 1, 1.0), (i64::MIN + 1_000, 2.0)],
            Some(60_000_000_000),
        );
        assert!(v.is_finite(), "near-i64::MIN must not overflow, got {v}");
        // A window spanning near-MIN to near-MAX overflows `last_t - first_t`.
        let v = rate_counter_extrapolated(
            vec![(i64::MIN + 1, 1.0), (i64::MAX - 1, 2.0)],
            Some(60_000_000_000),
        );
        assert!(v.is_finite(), "full-i64-span must not overflow, got {v}");
    }

    /// Issue #227 review finding 1: `absent_over_time` retains NOTHING
    /// per-sample — its presence state is the O(grid) coverage array, so a
    /// dense selector cannot grow memory with the scan. Drives a dense scan
    /// and asserts both the correct gaps and a bounded presence footprint.
    #[test]
    fn sliding_absent_presence_is_grid_bounded_not_scan_bounded() {
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::AbsentOverTime,
            param: None,
            absent_labels: vec![("app".to_string(), "a".to_string())],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 100, 10, 10);
        let mut state =
            RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                .unwrap();
        // 20_000 dense samples covering only the second half of the grid.
        let rows: Vec<MetricScanRow> = (0..20_000)
            .map(|i| MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 51 + (i % 50),
                body: "x".to_string(),
            })
            .collect();
        state.push_rows(&rows).expect("dense absent scan");
        // The presence state is exactly the grid-sized array — never 20_000.
        assert_eq!(
            state.present_cover.len(),
            (window.end_ns() / 10 + 2) as usize,
            "presence state must be grid-sized, independent of scan density"
        );
        assert_eq!(state.retained, 0, "absent retains no per-sample points");
        let QueryResult::Matrix(series) = state.finish().expect("finish") else {
            panic!("expected a matrix");
        };
        // Grid {0,10,...,100}; samples occupy (50,100] ⇒ grid points 60..=100
        // are covered, 0..=50 are absent.
        let pts: Vec<i64> = series[0].points.iter().map(|(t, _)| *t).collect();
        assert_eq!(pts, vec![0, 10, 20, 30, 40, 50]);
    }

    /// The `/stats` pushdown-only gate (M8-LQ2 Delta 1): a pushable literal
    /// line filter is accepted, but a non-pushable `ip()`/mixed-`or` line
    /// filter — and any beyond-line-filter stage — is rejected, so `stats`
    /// never silently over-counts a filter it cannot push down.
    #[test]
    fn stats_gate_rejects_non_pushable_line_filters() {
        fn pipeline(query: &str) -> Vec<Stage> {
            let pulsus_logql::Expr::Log(le) = pulsus_logql::parse(query).expect("parse") else {
                panic!("log expr");
            };
            le.pipeline
        }
        // Pushable: plain literal line filter(s).
        assert!(stats_pipeline_is_pushdown_only(&pipeline(r#"{app="x"}"#)));
        assert!(stats_pipeline_is_pushdown_only(&pipeline(
            r#"{app="x"} |= "err""#
        )));
        assert!(stats_pipeline_is_pushdown_only(&pipeline(
            r#"{app="x"} |= "a" or "b""#
        )));
        // Non-pushable: ip() line filter and a mixed-or with an ip alternative.
        assert!(!stats_pipeline_is_pushdown_only(&pipeline(
            r#"{app="x"} |= ip("10.0.0.0/8")"#
        )));
        assert!(!stats_pipeline_is_pushdown_only(&pipeline(
            r#"{app="x"} |= "a" or ip("10.0.0.0/8")"#
        )));
        // Beyond-line-filter stages are still rejected.
        assert!(!stats_pipeline_is_pushdown_only(&pipeline(
            r#"{app="x"} | json"#
        )));
    }

    /// The house deterministic-seed PRNG (the xtask bench dataset
    /// pattern) — no rand dependency, reproducible failures.
    fn splitmix64(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// The pre-#244 `feed_detected_rows` page shape, rebuilt over the
    /// streaming [`DetectedRowFeeder`] so the plan-v2 contract tests below
    /// drive the SHIPPED per-row path.
    fn feed_rows_via_feeder(
        rows: &[SampleRow],
        base_labels: &HashMap<u64, Vec<(String, String)>>,
        compiled: &super::super::pipeline::CompiledPipeline,
        acc: &mut FieldAccumulator,
        matched: &mut u32,
        line_limit: u32,
    ) -> Result<(), ReadError> {
        let mut feeder = DetectedRowFeeder::new();
        for row in rows {
            if *matched >= line_limit {
                break;
            }
            if feeder.feed_row(
                row.fingerprint,
                row.timestamp_ns,
                &row.body,
                &row.structured_metadata,
                base_labels,
                compiled,
                acc,
            )? {
                *matched += 1;
            }
        }
        Ok(())
    }

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
        feed_rows_via_feeder(&rows, &base_labels, &compiled, &mut acc, &mut matched, 100)
            .expect("no budget breach");
        assert_eq!(
            matched, 1,
            "only the post-pipeline surviving row counts toward line_limit"
        );
        let (fields, _) = acc.finish();
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
        feed_rows_via_feeder(&rows, &base_labels, &compiled, &mut acc, &mut matched, 2)
            .expect("no budget breach");
        assert_eq!(matched, 2);
        let (fields, _) = acc.finish();
        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields[0].cardinality, 2,
            "rows past the line_limit are never sampled"
        );
    }

    // -- Issue #244: the streaming page-loop contract (AC 16) and the
    //    incremental cursor (AC 17), hermetic over injected pages. -------

    fn detected_compiled(query: &str) -> super::super::pipeline::CompiledPipeline {
        let expr = pulsus_logql::parse(query).expect("parse");
        let pulsus_logql::Expr::Log(le) = expr else {
            panic!("log expr");
        };
        super::super::pipeline::CompiledPipeline::compile(&le.pipeline).expect("compile")
    }

    fn detected_base_labels() -> HashMap<u64, Vec<(String, String)>> {
        let mut base_labels = HashMap::new();
        base_labels.insert(1, vec![("app".to_string(), "x".to_string())]);
        base_labels
    }

    /// Distinct field name per row (the AC 16 fixture rule: a shared name
    /// would let a wrong drain produce the right field set).
    fn detected_tail_row(i: u64) -> TailSampleRow {
        TailSampleRow {
            fingerprint: 1,
            timestamp_ns: 1_000 - i as i64, // newest-first, all distinct
            body: format!(r#"{{"f{i}":{i}}}"#),
            body_hash: 0x9000 + i,
            structured_metadata: String::new(),
        }
    }

    fn scan_budget_err() -> ReadError {
        ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
            budget_bytes: 1024,
            estimate: None,
        })
    }

    fn detected_paged_state(page_size: u32, line_limit: u32) -> DetectedPagedState {
        DetectedPagedState {
            feeder: DetectedRowFeeder::new(),
            cursor: None,
            spent: 0,
            matched: 0,
            page_size,
            line_limit,
            budget: u64::MAX,
        }
    }

    fn absorb(
        st: &mut DetectedPagedState,
        acc: &mut FieldAccumulator,
        compiled: &super::super::pipeline::CompiledPipeline,
        base_labels: &HashMap<u64, Vec<(String, String)>>,
        items: Vec<Result<TailSampleRow, ReadError>>,
        read_bytes: u64,
    ) -> Result<Option<bool>, ReadError> {
        let mut stream = futures::stream::iter(items);
        futures::executor::block_on(st.absorb_page(
            &mut stream,
            |_| Some(read_bytes),
            |e| e,
            base_labels,
            compiled,
            acc,
        ))
    }

    /// The error-free field set over `rows`, for the AC 16b comparisons.
    fn detected_fields_over(rows: &[TailSampleRow]) -> Vec<DetectedFieldOut> {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(u32::MAX, 100);
        let items: Vec<Result<TailSampleRow, ReadError>> = rows.iter().cloned().map(Ok).collect();
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 1);
        assert!(matches!(out, Ok(Some(false))), "error-free run terminates");
        acc.finish().0
    }

    /// AC 16a — the first-page rule: a `ScanBudgetBytes` error while
    /// draining the FIRST page (`spent == 0`) is `QueryTooBroad` (the
    /// end-to-end 422), REGARDLESS of how many rows were already
    /// delivered; the prefix is discarded with the request. Streaming
    /// must not turn that 422 into a 200.
    #[test]
    fn detected_paged_first_page_budget_error_stays_query_too_broad_despite_delivered_rows() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(10, 100);
        let items = vec![
            Ok(detected_tail_row(0)),
            Ok(detected_tail_row(1)),
            Ok(detected_tail_row(2)),
            Err(scan_budget_err()),
            Ok(detected_tail_row(3)),
            Ok(detected_tail_row(4)),
        ];
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
        match out {
            Err(ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { .. })) => {}
            other => panic!("first-page budget error must propagate, got {other:?}"),
        }
    }

    /// AC 16b — the later-page rule: with `spent > 0`, a mid-page
    /// `ScanBudgetBytes` terminates PARTIAL with the accumulated prefix
    /// retained — provably the fields of `[r0, r1, r2]` (the delivered
    /// prefix), NOT of the last complete page alone (M6's page-atomic
    /// rewrite) and NOT of all five rows (M7's continue-past-error
    /// rewrite) — and the cursor covers exactly the three drained rows.
    #[test]
    fn detected_paged_later_page_budget_error_returns_partial_prefix_mid_page() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let rows: Vec<TailSampleRow> = (0..5).map(detected_tail_row).collect();
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(2, 100);
        // Page 1: two rows, no error -> continue.
        let page1 = vec![Ok(rows[0].clone()), Ok(rows[1].clone())];
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, page1, 64);
        assert!(matches!(out, Ok(None)), "page 1 continues: {out:?}");
        assert!(st.spent > 0, "page 1's read_bytes must be accounted");
        // Page 2: one row, then the budget error.
        let page2 = vec![
            Ok(rows[2].clone()),
            Err(scan_budget_err()),
            Ok(rows[3].clone()),
            Ok(rows[4].clone()),
        ];
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, page2, 64);
        assert!(
            matches!(out, Ok(Some(true))),
            "later-page budget error terminates PARTIAL: {out:?}"
        );
        let got = acc.finish().0;
        assert_eq!(
            got,
            detected_fields_over(&rows[..3]),
            "(i) the retained prefix is exactly [r0, r1, r2]"
        );
        assert_ne!(
            got,
            detected_fields_over(&rows[..2]),
            "(ii) not the last complete page alone (M6)"
        );
        assert_ne!(
            got,
            detected_fields_over(&rows),
            "(iii) not all five rows (M7)"
        );
        // (iv) the cursor covers the three drained rows: pages [r0, r1]
        // then [r2].
        let want = advance_tail_cursor(
            advance_tail_cursor(None, &rows[..2]),
            std::slice::from_ref(&rows[2]),
        );
        assert_eq!(st.cursor, want, "cursor equals r2's tuple, 3 rows drained");
        assert_eq!(st.cursor.expect("cursor").tuple, {
            let r = &rows[2];
            (r.timestamp_ns, r.fingerprint, r.body_hash)
        });
    }

    /// AC 16c — the two COMPLETE terminals, plus `classify_page_error`'s
    /// truth table.
    #[test]
    fn detected_paged_terminal_branches_and_error_classification() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();

        // line_limit filled -> Ok(Some(false)), and the page still fully
        // drains (the cursor reaches the last raw row).
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(3, 1);
        let items = (0..3).map(|i| Ok(detected_tail_row(i))).collect();
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
        assert!(matches!(out, Ok(Some(false))), "{out:?}");
        assert_eq!(st.matched, 1);
        let r2 = detected_tail_row(2);
        assert_eq!(
            st.cursor.expect("cursor").tuple,
            (r2.timestamp_ns, r2.fingerprint, r2.body_hash),
            "line_limit stops FEEDING, never DRAINING"
        );

        // fetched < page_size (window exhausted) -> Ok(Some(false)).
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(10, 100);
        let items = (0..3).map(|i| Ok(detected_tail_row(i))).collect();
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
        assert!(matches!(out, Ok(Some(false))), "{out:?}");

        // classify_page_error's four cells.
        assert!(matches!(
            classify_page_error(scan_budget_err(), 1),
            Ok(true)
        ));
        assert!(classify_page_error(scan_budget_err(), 0).is_err());
        let other = || ReadError::PipelineInvalid {
            reason: "x".to_string(),
        };
        assert!(classify_page_error(other(), 1).is_err());
        assert!(classify_page_error(other(), 0).is_err());
    }

    /// AC 16d — the subset property: over >= 100 seeded sequences, an
    /// error-cut partial accumulation's label set is a SUBSET of the
    /// error-free run's, with at least one STRICT subset exercised.
    #[test]
    fn detected_paged_partial_prefix_is_always_a_subset_of_the_complete_answer() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let mut state: u64 = 0x0024_4bad_c0de;
        let mut strict_subsets = 0usize;
        for _ in 0..128 {
            let n = 1 + (splitmix64(&mut state) % 8) as usize;
            let cut = (splitmix64(&mut state) % n as u64) as usize;
            let rows: Vec<TailSampleRow> = (0..n as u64).map(detected_tail_row).collect();
            let full: std::collections::BTreeSet<String> = detected_fields_over(&rows)
                .into_iter()
                .map(|f| f.label)
                .collect();
            let mut acc = FieldAccumulator::new(1000);
            let mut st = detected_paged_state(u32::MAX, 100);
            st.spent = 1; // a later page, so the cut is PARTIAL not 422
            let mut items: Vec<Result<TailSampleRow, ReadError>> =
                rows.iter().take(cut).cloned().map(Ok).collect();
            items.push(Err(scan_budget_err()));
            items.extend(rows.iter().skip(cut).cloned().map(Ok));
            let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
            assert!(matches!(out, Ok(Some(true))), "{out:?}");
            let partial: std::collections::BTreeSet<String> =
                acc.finish().0.into_iter().map(|f| f.label).collect();
            assert!(
                partial.is_subset(&full),
                "partial {partial:?} must be a subset of {full:?}"
            );
            if partial.len() < full.len() {
                strict_subsets += 1;
            }
        }
        assert!(
            strict_subsets > 0,
            "at least one strict subset must be exercised"
        );
    }

    /// AC 17(a) — `TailCursorTracker` is EXACTLY `advance_tail_cursor`
    /// over randomized page sequences including empty pages, all-equal
    /// tuples and carry-from-`prev`; the drained count equals the page's
    /// row count.
    #[test]
    fn tail_cursor_tracker_matches_advance_tail_cursor_over_randomized_sequences() {
        let mut state: u64 = 0x1755;
        let mut prev: Option<TailCursor> = None;
        for round in 0..500 {
            let n = (splitmix64(&mut state) % 7) as usize; // 0 = empty page
            let all_equal = splitmix64(&mut state).is_multiple_of(4);
            let rows: Vec<TailSampleRow> = (0..n)
                .map(|i| {
                    // A tiny alphabet forces tie runs; `all_equal` pages
                    // force whole-page runs (the carry case).
                    let (ts, fp, h) = if all_equal {
                        (7, 7, 7)
                    } else {
                        (
                            (splitmix64(&mut state) % 3) as i64,
                            splitmix64(&mut state) % 2,
                            splitmix64(&mut state) % 2,
                        )
                    };
                    TailSampleRow {
                        fingerprint: fp,
                        timestamp_ns: ts,
                        body: format!("b{i}"),
                        body_hash: h,
                        structured_metadata: String::new(),
                    }
                })
                .collect();
            let want = advance_tail_cursor(prev, &rows);
            let mut tracker = TailCursorTracker::new();
            for r in &rows {
                tracker.observe(r.timestamp_ns, r.fingerprint, r.body_hash);
            }
            let (got, drained) = tracker.finish(prev);
            assert_eq!(got, want, "round {round}: rows {rows:?}");
            assert_eq!(drained as usize, rows.len(), "round {round}");
            prev = got;
        }
    }

    /// AC 17(b) — a page is drained PAST `line_limit`: feeding stops at
    /// the limit but the cursor still advances over the whole raw page.
    #[test]
    fn detected_paged_page_is_drained_past_the_line_limit() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let base_labels = detected_base_labels();
        let mut acc = FieldAccumulator::new(1000);
        let mut st = detected_paged_state(5, 1);
        let rows: Vec<TailSampleRow> = (0..5).map(detected_tail_row).collect();
        let items = rows.iter().cloned().map(Ok).collect();
        let out = absorb(&mut st, &mut acc, &compiled, &base_labels, items, 64);
        assert!(matches!(out, Ok(Some(false))), "{out:?}");
        assert_eq!(st.matched, 1, "feeding stopped at line_limit");
        assert_eq!(
            st.cursor,
            advance_tail_cursor(None, &rows),
            "the raw page is fully drained"
        );
        let fields = acc.finish().0;
        assert_eq!(fields.len(), 1, "only the fed row's field: {fields:?}");
    }

    // -- Issue #244 AC 12(c)–(e): the feeder's carried-capacity bound. ----

    /// AC 12(c) — after a row wider than `MAX_FEEDER_SCRATCH_SLOTS`
    /// pairs, every carried buffer is empty and capacity-capped, on the
    /// fed path AND the fingerprint-miss path.
    #[test]
    fn feeder_trim_caps_carried_capacity_after_a_wide_row_and_on_fingerprint_miss() {
        let compiled = detected_compiled(r#"{app="x"}"#);
        let base_labels = detected_base_labels();
        let mut acc = FieldAccumulator::new(8);
        let mut feeder = DetectedRowFeeder::new();
        // A wide SM row: > MAX_FEEDER_SCRATCH_SLOTS pairs.
        let wide: usize = MAX_FEEDER_SCRATCH_SLOTS + 1000;
        let mut sm = String::from("{");
        for i in 0..wide {
            if i > 0 {
                sm.push(',');
            }
            sm.push_str(&format!(r#""k{i:05}":"v""#));
        }
        sm.push('}');
        let survived = feeder
            .feed_row(1, 1, "body", &sm, &base_labels, &compiled, &mut acc)
            .expect("no error");
        assert!(survived);
        let check = |feeder: &DetectedRowFeeder, ctx: &str| {
            assert_eq!(feeder.merge_buf.len(), 0, "{ctx}");
            assert_eq!(feeder.sm_buf.len(), 0, "{ctx}");
            assert_eq!(feeder.label_scratch.len(), 0, "{ctx}");
            assert!(
                feeder.merge_buf.capacity() <= MAX_FEEDER_SCRATCH_SLOTS,
                "{ctx}"
            );
            assert!(
                feeder.sm_buf.capacity() <= MAX_FEEDER_SCRATCH_SLOTS,
                "{ctx}"
            );
            assert!(
                feeder.label_scratch.capacity() <= MAX_FEEDER_SCRATCH_SLOTS,
                "{ctx}"
            );
            assert!(
                feeder.sm_ctx.err.capacity() <= MAX_FEEDER_SCRATCH_STRING_BYTES,
                "{ctx}"
            );
            assert!(
                feeder.sm_ctx.details.capacity() <= MAX_FEEDER_SCRATCH_STRING_BYTES,
                "{ctx}"
            );
            assert!(
                feeder.scratch_capacity_bytes() <= MAX_FEEDER_SCRATCH_BYTES,
                "{ctx}"
            );
        };
        check(&feeder, "after the wide fed row");
        // The fingerprint-miss path trims too: pre-grow a buffer past the
        // cap, then feed a row whose fingerprint never hydrated.
        feeder.merge_buf = Vec::with_capacity(3 * MAX_FEEDER_SCRATCH_SLOTS);
        let survived = feeder
            .feed_row(999, 1, "body", "", &base_labels, &compiled, &mut acc)
            .expect("no error");
        assert!(!survived, "unknown fingerprint is skipped");
        check(&feeder, "after the fingerprint-miss row");
    }

    /// AC 12(d) — the carried-bound constant derives from `size_of` over
    /// exactly FIVE terms (a sixth feeder buffer — M10 — breaks this), and
    /// equals the plan-derived 64-bit figure.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn feeder_scratch_bound_is_the_five_term_size_of_derivation() {
        assert_eq!(MAX_FEEDER_SCRATCH_BYTES, 1_196_032);
        let pair = (MAX_FEEDER_SCRATCH_SLOTS * size_of::<(String, String)>()) as u64;
        let cow =
            (MAX_FEEDER_SCRATCH_SLOTS * size_of::<(Cow<'static, str>, Cow<'static, str>)>()) as u64;
        let five_terms = alloc_block_bytes(pair)
            + alloc_block_bytes(pair)
            + alloc_block_bytes(cow)
            + alloc_block_bytes(MAX_FEEDER_SCRATCH_STRING_BYTES as u64)
            + alloc_block_bytes(MAX_FEEDER_SCRATCH_STRING_BYTES as u64);
        assert_eq!(MAX_FEEDER_SCRATCH_BYTES, five_terms);
    }

    /// AC 12(e) — `recycle_label_scratch` preserves the allocation (the
    /// in-place `collect` specialization; M8's `Vec::new` replacement
    /// loses it).
    #[test]
    fn recycle_label_scratch_preserves_capacity() {
        let scratch: LabelScratch<'static> = Vec::with_capacity(1024);
        let recycled = recycle_label_scratch(scratch);
        assert_eq!(recycled.capacity(), 1024, "the allocation must be reused");
    }

    /// AC 13(e)'s compile half + a leak guard: the legacy helpers exist
    /// for the witness only. (The `git grep` reference audit is recorded
    /// in the #244 implementation notes; this test pins that the probe
    /// seam still routes production `feed_row` through the NEW shape by
    /// asserting the two paths agree on a smoke row.)
    #[test]
    fn probe_feed_row_and_legacy_shape_agree_on_a_smoke_row() {
        let compiled = detected_compiled(r#"{app="x"} | json"#);
        let mut new_probe = DetectedFieldsProbe::new(100, 1000);
        new_probe.add_stream(1, &[("app".to_string(), "x".to_string())]);
        let mut legacy_probe = DetectedFieldsProbe::new(100, 1000);
        legacy_probe.add_stream(1, &[("app".to_string(), "x".to_string())]);
        let body = r#"{"level":"info","code":7}"#;
        let sm = r#"{"trace_id":"abc"}"#;
        assert!(new_probe.feed_row(&compiled, 1, 5, body, sm).expect("ok"));
        assert!(
            legacy_probe
                .feed_row_legacy_shape(&compiled, 1, 5, body, sm)
                .expect("ok")
        );
        assert_eq!(new_probe.finish(), legacy_probe.finish());
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
        let mut tail_out = run_pipeline_rows(rows(), &tail_compiled, &meta, 100)
            .expect("no template budget breach");
        let mut query_out = run_pipeline_rows(rows(), &query_compiled, &meta, 100)
            .expect("no template budget breach");
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
        let results =
            run_pipeline_rows(rows, &compiled, &meta, 100).expect("no template budget breach");
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
        let results =
            run_pipeline_rows(rows, &compiled, &meta, 100).expect("no template budget breach");
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
        let results =
            run_pipeline_rows(rows, &compiled, &meta, 100).expect("no template budget breach");
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
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 3)
            .expect("no template budget breach");
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
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 3)
            .expect("no template budget breach");
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
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 100)
            .expect("no template budget breach");
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
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 2)
            .expect("no template budget breach");
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
                .expect("no budget breach")
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
            let filled = acc
                .feed(&page(base_ts), &compiled)
                .expect("no budget breach");
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
        assert!(
            !acc.feed(&rows(0), &compiled).expect("no budget breach"),
            "2 < 3, not filled"
        );
        assert!(
            acc.feed(&rows(10), &compiled).expect("no budget breach"),
            "2 + 2 ⇒ filled at 3"
        );
        // A further page must not push the total past the limit.
        acc.feed(&rows(20), &compiled).expect("no budget breach");
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
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 100)
            .expect("no template budget breach");
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

    /// Review round 2, finding 1 (carried to the round-7 shared guard):
    /// the grid count uses i128/saturating arithmetic — the
    /// extreme/degenerate shapes below must saturate (and REJECT through
    /// [`ensure_grid_resolution`]) or zero out, never panic or wrap past
    /// the cap.
    #[test]
    fn grid_resolution_guard_is_overflow_safe_at_extreme_bounds() {
        // Full i64 range at step 1: ~2^64 points, saturates cleanly — and
        // the guard rejects, reporting the reference's own saturated
        // interval count (`maxDuration / 1ns`, round 8).
        assert_eq!(grid_point_count(i64::MIN, i64::MAX, 1), u64::MAX);
        assert!(matches!(
            ensure_grid_resolution(i64::MIN, i64::MAX, 1),
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets {
                // The Sub-saturated span (i64::MAX ns) at a 1ns step.
                buckets: 9_223_372_036_854_775_807, // i64::MAX as u64
                cap: MAX_CLIENT_AGG_BUCKETS,
            }))
        ));
        // Inverted/empty windows are zero points and zero fence intervals,
        // never an underflow — 0 points admits.
        assert_eq!(grid_point_count(i64::MAX, i64::MIN, 1), 0);
        assert_eq!(ensure_grid_resolution(i64::MAX, i64::MIN, 1).unwrap(), 0);
        // A single-point window (start == end) is one grid point.
        assert_eq!(grid_point_count(0, 0, 1), 1);
        // A zero step (structurally `InvalidStep` upstream) and a step
        // wider than i64 (never produced by `parse_step`) both saturate
        // so the guard rejects them by name instead of the evaluator
        // ever dividing by a degenerate step.
        assert_eq!(grid_point_count(0, 1_000, 0), u64::MAX);
        assert_eq!(grid_point_count(0, 1_000, u64::MAX), u64::MAX);
        assert!(ensure_grid_resolution(0, 1_000, 0).is_err());
        // Ordinary shapes stay exact (the 11k fence has its own golden):
        // [0, 120] at step 60 holds grid points 0, 60, 120.
        assert_eq!(grid_point_count(0, 120, 60), 3);
    }

    /// Issue #227 review round 7, finding 1: the resolution fence is
    /// Loki's `(end - start) / step > 11000` (TRUNCATING interval count,
    /// `loghttp/query.go` `errStepTooSmall`) — exactly-at-the-limit is
    /// SERVED with its full 11_001-point inclusive grid; one interval
    /// over is rejected. Both engine grid guards (`RangeSlideState::new`
    /// and `materialize_vector_lit`) funnel through
    /// [`ensure_grid_resolution`], so this pins the fence for the whole
    /// engine against the HTTP guard's identical formula.
    #[test]
    fn grid_resolution_fence_serves_11000_intervals_and_rejects_11001() {
        const S: u64 = 1_000_000_000; // 1s step
        let step = S as i64;

        // Exactly at the limit, step-aligned: 11_000 intervals → admitted,
        // 11_001 inclusive grid points.
        assert_eq!(ensure_grid_resolution(0, 11_000 * step, S).unwrap(), 11_001);
        // One under: admitted, 11_000 points.
        assert_eq!(ensure_grid_resolution(0, 10_999 * step, S).unwrap(), 11_000);
        // One interval over: rejected, reporting the INTERVAL count.
        assert!(matches!(
            ensure_grid_resolution(0, 11_001 * step, S),
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets {
                buckets: 11_001,
                cap: MAX_CLIENT_AGG_BUCKETS,
            }))
        ));
        // Step not dividing the span (truncating division, like Loki's
        // integer `Duration / Duration`): 11_000 intervals + half a step
        // still truncates to 11_000 → admitted, 11_001 points.
        assert_eq!(
            ensure_grid_resolution(0, 11_000 * step + step / 2, S).unwrap(),
            11_001
        );
        // ... and one nanosecond past the 11_001st interval boundary is
        // 11_001 truncated intervals → rejected.
        assert!(ensure_grid_resolution(0, 11_001 * step + 1, S).is_err());
        // Unaligned start (same span, shifted): the fence depends only on
        // `end - start`, exactly like the reference.
        let off = 123_456_789;
        assert_eq!(
            ensure_grid_resolution(off, off + 11_000 * step, S).unwrap(),
            11_001
        );
        assert!(ensure_grid_resolution(off, off + 11_001 * step, S).is_err());
    }

    /// Issue #227 review round 7, finding 1 (the end-to-end shape): a
    /// range request of exactly 11_000 intervals must be SERVED — the
    /// request guard admits it and the engine's grid guard must not then
    /// 422 it. Drives the REAL evaluators at the limit: the sliding state
    /// accepts and emits over the full 11_001-point grid, and the
    /// `vector(n)` grid materializes all 11_001 points.
    #[test]
    fn a_range_query_of_exactly_11000_intervals_is_served_not_422() {
        const S: i64 = 1_000_000_000;
        let step = super::super::params::validate_duration_ns(S as u64, "step").unwrap();

        // The leafless vector(n) path: full inclusive grid, both endpoints.
        let window = GridWindow {
            start_ns: 0,
            end_ns: 11_000 * S,
            step_ns: Some(step),
        };
        let res = materialize_vector_lit(1.0, &window)
            .expect("exactly 11_000 intervals must be served (the reference serves it)");
        let QueryResult::Matrix(series) = res else {
            panic!("expected a matrix, got {res:?}");
        };
        assert_eq!(series[0].points.len(), 11_001);
        assert_eq!(series[0].points.first().unwrap().0, 0);
        assert_eq!(series[0].points.last().unwrap().0, 11_000 * S);

        // The sliding data path: RangeSlideState::new admits the same
        // window (kmax = 11_000) instead of tripping MetricBuckets.
        let client = ClientAgg {
            range_op: RangeAggOp::CountOverTime,
            value: ClientValue::Count,
            param: None,
            pipeline: vec![],
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 11_000 * S, S as u64, S as u64);
        let state = RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
            .expect("the engine must admit every request the HTTP guard admits");
        assert_eq!(state.kmax, 11_000);

        // One interval over IS the engine's 422 (the backstop still holds
        // for anything that bypasses the HTTP boundary).
        let window = slide_window(0, 11_001 * S, S as u64, S as u64);
        match RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets { buckets, cap })) => {
                assert_eq!(buckets, 11_001);
                assert_eq!(cap, MAX_CLIENT_AGG_BUCKETS);
            }
            Err(other) => panic!("expected MetricBuckets, got {other:?}"),
            Ok(_) => panic!("one interval over the fence must be rejected"),
        }
    }

    /// Issue #227 review round 8: the fence's span subtraction SATURATES
    /// exactly like the reference's — Go's `time.Time.Sub` clamps an
    /// out-of-range difference to the int64-ns `Duration` bound
    /// (`maxDuration = 1<<63-1`) instead of widening, and the division
    /// then truncates. A full-domain span at a huge step is therefore
    /// SERVED (`i64::MAX / step ≤ 11_000`) where the previous exact-i128
    /// fence wrongly rejected it.
    #[test]
    fn grid_resolution_fence_saturates_the_span_like_the_reference() {
        // 1_000_000s step over the whole i64 domain: the saturated fence
        // counts i64::MAX/step = 9_223 intervals (admit); the TRUE span
        // (2^64-1 ns) holds 18_446 — an exact fence would reject a request
        // the reference serves.
        const STEP: u64 = 1_000_000_000_000_000; // 1_000_000s in ns
        assert_eq!(fence_intervals(i64::MIN, i64::MAX, STEP), 9_223);
        assert_eq!(
            ensure_grid_resolution(i64::MIN, i64::MAX, STEP).unwrap(),
            // The emitted grid still covers the TRUE span: the exact
            // inclusive point count, not the saturated one.
            18_447
        );

        // The saturated-fence boundary: floor(i64::MAX / 11_001) is the
        // largest step counting 11_001 saturated intervals (reject); one
        // nanosecond of step more counts 11_000 (admit), whose true grid —
        // 22_002 points — is the guard's hard ceiling.
        let reject_step = (i64::MAX / 11_001) as u64;
        assert!(matches!(
            ensure_grid_resolution(i64::MIN, i64::MAX, reject_step),
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricBuckets {
                buckets: 11_001,
                cap: MAX_CLIENT_AGG_BUCKETS,
            }))
        ));
        assert_eq!(
            ensure_grid_resolution(i64::MIN, i64::MAX, reject_step + 1).unwrap(),
            22_002
        );

        // An UNSATURATED span stays exact — a span of exactly i64::MAX ns
        // is the saturation onset, byte-identical either way, and the
        // ordinary domain is untouched.
        assert_eq!(fence_intervals(-1, i64::MAX - 1, STEP), 9_223);
        assert_eq!(
            fence_intervals(0, 11_000 * 1_000_000_000, 1_000_000_000),
            11_000
        );
    }

    /// Issue #227 review round 8 (the end-to-end shape at the domain
    /// edge): a full-domain range request at a 1_000_000s step must be
    /// SERVED — the saturating fence admits it and the REAL evaluators
    /// emit the exact 18_447-point start-anchored grid over the true
    /// span, every point in `[start, end]`, no wrap.
    #[test]
    fn a_full_domain_range_query_under_the_saturated_fence_is_served() {
        const STEP: u64 = 1_000_000_000_000_000; // 1_000_000s in ns
        let step = super::super::params::validate_duration_ns(STEP, "step").unwrap();
        // i64::MIN + 18_446·step, in i128 (the product alone overflows i64).
        let last_point = (i64::MIN as i128 + 18_446 * STEP as i128) as i64;

        // The leafless vector(n) path: the exact inclusive true-span grid.
        let window = GridWindow {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
            step_ns: Some(step),
        };
        let res = materialize_vector_lit(1.0, &window)
            .expect("the saturating fence must admit the full-domain span");
        let QueryResult::Matrix(series) = res else {
            panic!("expected a matrix, got {res:?}");
        };
        assert_eq!(series[0].points.len(), 18_447);
        assert_eq!(series[0].points.first().unwrap().0, i64::MIN);
        assert_eq!(series[0].points.last().unwrap().0, last_point);
        assert!(last_point < i64::MAX); // in-domain, short of `end`

        // The sliding data path: RangeSlideState::new admits the same
        // window with the same exact grid (kmax = 18_446).
        let client = ClientAgg {
            range_op: RangeAggOp::CountOverTime,
            value: ClientValue::Count,
            param: None,
            pipeline: vec![],
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(i64::MIN, i64::MAX, STEP, STEP);
        let state = RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
            .expect("the engine must admit every request the HTTP guard admits");
        assert_eq!(state.kmax, 18_446);
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
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 60_000_000_000,
        };
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
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

    /// The `rate_counter` retention state (`BucketAcc::Counter`, code
    /// review round 2): builds a client-aggregated `rate_counter` instant
    /// state and its shared inputs.
    fn rate_counter_state_inputs() -> (
        super::super::pipeline::CompiledPipeline,
        plan::ClientAgg,
        HashMap<u64, StreamMetaRow>,
        ClientWindow,
    ) {
        let stages = match pulsus_logql::parse(r#"rate_counter({a="b"} | logfmt | unwrap c [1m])"#)
            .expect("parse")
        {
            Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) => range.selector.pipeline,
            other => panic!("unexpected expr: {other:?}"),
        };
        let compiled = super::super::pipeline::CompiledPipeline::compile(&stages).expect("compile");
        let client = plan::ClientAgg {
            pipeline: stages,
            value: plan::ClientValue::Unwrap,
            range_op: RangeAggOp::RateCounter,
            param: None,
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
        let window = ClientWindow::Instant {
            start_ns: 0,
            end_ns: 60_000_000_000,
        };
        (compiled, client, meta, window)
    }

    /// Code review round 2 (M8-LQ3, finding 1): the `rate_counter`
    /// per-bucket point retention is bounded by [`MAX_COUNTER_VALUES`],
    /// mirroring the quantile bound. Below the cap the reset-aware rate is
    /// unaffected by the guard (four samples 10,30,5,12 by ts → increase 32,
    /// span 30e9 ns, scaled by the ns/ms extrapolation factor and divided by
    /// the 60s window = 0.5333344); charged to the boundary the next sample
    /// trips the named `CounterValues` too-broad error — complete-or-error,
    /// never a silently truncated increase.
    #[test]
    fn rate_counter_point_retention_is_capped_without_changing_values_below_it() {
        let (compiled, client, meta, window) = rate_counter_state_inputs();
        // Below the cap: the reset-aware value is exactly 32/60, unchanged
        // by the retention guard.
        let rows = [
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 10_000_000_000,
                body: "c=10".to_string(),
            },
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 20_000_000_000,
                body: "c=30".to_string(),
            },
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 30_000_000_000,
                body: "c=5".to_string(), // reset: 5 < 30
            },
            MetricScanRow {
                fingerprint: 1,
                timestamp_ns: 40_000_000_000,
                body: "c=12".to_string(),
            },
        ];
        let result = run_client_agg_rows(
            &rows,
            &compiled,
            &meta,
            &client,
            window,
            Some(60_000_000_000),
        )
        .expect("below the cap the query succeeds");
        let QueryResult::Vector(items) = result else {
            panic!("expected a vector, got {result:?}");
        };
        assert_eq!(items.len(), 1, "one series expected: {items:?}");
        assert_eq!(items[0].value, 0.5333344);

        // At the boundary: the next retained point trips the named error,
        // exactly as the quantile bound does — driven through the real
        // `push_rows` fold with the counter pre-charged (a 4M-row fixture
        // would be pure waste).
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            Some(60_000_000_000),
            AggCaps::DEFAULT,
        )
        .unwrap();
        state.counter_values = MAX_COUNTER_VALUES - 1;
        let err = state.push_rows(&rows).unwrap_err();
        match err {
            ReadError::QueryTooBroad(TooBroadReason::CounterValues { count, cap }) => {
                assert_eq!(cap, MAX_COUNTER_VALUES);
                assert_eq!(count, MAX_COUNTER_VALUES + 1);
            }
            other => panic!("expected QueryTooBroad(CounterValues), got {other:?}"),
        }
    }

    /// Code review round 3 (M8-LQ3, finding 1 — settled EMPIRICALLY): the
    /// concern was that `counter_values += 1` charges an INPUT ROW before
    /// the accumulator is derived, so a row copied into MULTIPLE
    /// accumulators would consume one quota unit while retaining several
    /// points — leaving retention unbounded.
    ///
    /// **Rewritten by issue #236 Part D, and the reason matters.** The
    /// original drove this state with a RANGE window at `step < range`
    /// and asserted the charge held across THREE step buckets. That
    /// configuration is now unrepresentable: `ClientAggState` takes an
    /// [`InstantWindow`] witness, and a range `rate_counter` has always
    /// gone to [`RangeSlideState`] in production — so the old test pinned
    /// a property of a state the engine never built. The identity it
    /// existed for survives in its instant form, where it is exact rather
    /// than merely bounded: every scanned row is retained EXACTLY once,
    /// so `counter_values` equals the retained point count, so a per-row
    /// charge is a true bound. The range half of the same invariant is
    /// pinned on the path that actually serves it, by
    /// `sliding_mutating_fan_out_charges_and_releases_retention_for_both_cell_kinds`
    /// and `mutating_retention_is_independent_of_the_window_width`.
    #[test]
    fn rate_counter_cap_bounds_total_retained_points() {
        let (compiled, client, meta, window) = rate_counter_state_inputs();
        let rows = [
            (10_000_000_000i64, "c=10"),
            (15_000_000_000, "c=30"),
            (25_000_000_000, "c=5"),
            (35_000_000_000, "c=12"),
            (45_000_000_000, "c=7"),
            (55_000_000_000, "c=20"),
        ]
        .into_iter()
        .map(|(timestamp_ns, body)| MetricScanRow {
            fingerprint: 1,
            timestamp_ns,
            body: body.to_string(),
        })
        .collect::<Vec<_>>();

        // Push through the real fold, then introspect the retained state
        // BEFORE finishing (finish consumes it).
        let mut state = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            Some(60_000_000_000),
            AggCaps::DEFAULT,
        )
        .unwrap();
        state.push_rows(&rows).unwrap();

        // `unwrap` sets the metric fan-out gate, so grouping is by final
        // label set — one group here (the unwrapped `c` is removed, base
        // labels are shared), and ONE accumulator in it.
        assert_eq!(state.label_groups.len(), 1, "one series expected");
        let total_retained: usize = state
            .label_groups
            .values()
            .map(|(_, acc)| match acc {
                BucketAcc::Counter(pts) => pts.len(),
                other => panic!("expected a Counter accumulator, got {other:?}"),
            })
            .sum();
        assert_eq!(
            total_retained,
            rows.len(),
            "every scanned row is retained exactly once"
        );
        assert_eq!(
            state.counter_values,
            rows.len() as u64,
            "the per-row charge equals total retained points (a true bound)"
        );

        // Values below the cap are unaffected by the retention guard.
        let QueryResult::Vector(items) = state.finish() else {
            panic!("an instant rate_counter query yields a vector");
        };
        assert_eq!(items.len(), 1, "one series: {items:?}");

        // The cap bounds the retention: pre-charged to `MAX - 1`, the
        // second scanned row trips the named error at `count = MAX + 1`.
        let mut capped = ClientAggState::new(
            &compiled,
            &meta,
            &client,
            instant_of(window),
            Some(60_000_000_000),
            AggCaps::DEFAULT,
        )
        .unwrap();
        capped.counter_values = MAX_COUNTER_VALUES - 1;
        match capped.push_rows(&rows).unwrap_err() {
            ReadError::QueryTooBroad(TooBroadReason::CounterValues { count, cap }) => {
                assert_eq!(cap, MAX_COUNTER_VALUES);
                assert_eq!(count, MAX_COUNTER_VALUES + 1);
            }
            other => panic!("expected QueryTooBroad(CounterValues), got {other:?}"),
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

    /// Issue #232: `range_seconds` is the reference's `Duration.Seconds()`
    /// (`float64(d/Second) + float64(d%Second)/1e9`), NOT `ns as f64 / 1e9`.
    /// The two disagree by an ULP on widths that are neither whole-second nor
    /// sub-second; the expected bit patterns below are the reference form's,
    /// and the naive form is asserted to be a DIFFERENT value so the test
    /// cannot pass against it.
    #[test]
    fn range_seconds_uses_the_references_two_rounding_form() {
        for ns in [
            1_118_000_000u64,
            1_122_000_000,
            1_235_000_000,
            1_247_000_000,
        ] {
            let naive = ns as f64 / 1_000_000_000.0;
            let two_rounding =
                (ns / 1_000_000_000) as f64 + (ns % 1_000_000_000) as f64 / 1_000_000_000.0;
            assert_ne!(
                naive.to_bits(),
                two_rounding.to_bits(),
                "{ns} ns must be a case where the two forms disagree"
            );
            assert_eq!(
                range_seconds(ns).to_bits(),
                two_rounding.to_bits(),
                "{ns} ns must round the reference's way"
            );
        }
    }

    /// Whole-second widths (`nsec == 0`) and sub-second widths (`sec == 0`)
    /// are identical under both forms — the #232 change is inert for them,
    /// which is why no corpus case caught the divergence before.
    #[test]
    fn range_seconds_matches_the_naive_form_on_whole_and_sub_second_widths() {
        let mut widths: Vec<u64> = (0..=600).map(|s| s * 1_000_000_000).collect();
        widths.extend((0..1_000).map(|ms| ms * 1_000_000));
        widths.push(1);
        widths.push(999_999_999);
        for ns in widths {
            assert_eq!(
                range_seconds(ns).to_bits(),
                (ns as f64 / 1_000_000_000.0).to_bits(),
                "{ns} ns must be identical under both forms"
            );
        }
    }

    /// The captured reference value for `rate` over one line in a `[1118ms]`
    /// window (`grafana/loki:3.7.4`, 2026-07-26) — the emitted-value proof
    /// that the divisor's rounding is observable, not merely internal.
    #[test]
    fn apply_rate_reproduces_the_captured_reference_value_for_a_sub_ulp_window() {
        assert_eq!(
            apply_rate(1.0, Some(1_118_000_000)).to_bits(),
            0.8944543828264759_f64.to_bits()
        );
        assert_eq!(
            apply_rate(3.0, Some(1_118_000_000)).to_bits(),
            2.683363148479428_f64.to_bits()
        );
        assert_eq!(
            apply_rate(1.0, Some(1_128_000_000)).to_bits(),
            0.8865248226950354_f64.to_bits()
        );
    }

    // ---- Issue #238: reserved structured-metadata routing (`Add`,
    // `labels.go:392-412`) and the no-pipeline fast-path gate. Rows carry
    // their Delta C''.3 ids; every expected set is a literal reference
    // capture (grafana/loki:3.7.4, `discover_log_levels: false`). The
    // pipeline-path C-rows live in `pipeline.rs`'s tests against these
    // exact (merged base, ctx) pairs. ----

    fn owned_pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        out.sort();
        out
    }

    /// Runs one row's SM through the real merge/routing; returns the SORTED
    /// merged (ordinary) set and the routing outcome.
    fn route_sm(
        base: &[(&str, &str)],
        sm_json: &str,
    ) -> (Vec<(String, String)>, StructuredMetadataCtx) {
        let base: Vec<(String, String)> = base
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let mut merge_buf = Vec::new();
        let mut sm_buf = Vec::new();
        let mut ctx = StructuredMetadataCtx::default();
        merge_labels_with_structured_metadata(
            &base,
            sm_json,
            &mut merge_buf,
            &mut sm_buf,
            &mut ctx,
        );
        merge_buf.sort();
        (merge_buf, ctx)
    }

    /// The no-pipeline fast-path label set: merge + `append_visible` + sort
    /// — exactly what `fan_out_sm_fast_path` renders per row.
    fn fast_path_labels(base: &[(&str, &str)], sm_json: &str) -> Vec<(String, String)> {
        let base: Vec<(String, String)> = base
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let mut merge_buf = Vec::new();
        let mut sm_buf = Vec::new();
        let mut ctx = StructuredMetadataCtx::default();
        merge_labels_with_structured_metadata(
            &base,
            sm_json,
            &mut merge_buf,
            &mut sm_buf,
            &mut ctx,
        );
        ctx.append_visible(&mut merge_buf);
        merge_buf.sort();
        merge_buf
    }

    /// D2/D3 (kills W1, W8): reserved SM names route to the ctx — never
    /// into the merged set, never counting as ordinary.
    #[test]
    fn reserved_sm_names_route_to_the_ctx_not_the_merged_set() {
        // v2: reserved err only.
        let (merged, ctx) = route_sm(&[("service_name", "v2")], r#"{"__error__":"boom"}"#);
        assert_eq!(merged, owned_pairs(&[("service_name", "v2")]));
        assert_eq!(ctx.err, "boom");
        assert_eq!(ctx.details, "");
        assert!(!ctx.has_ordinary);
        // v3: reserved details only.
        let (merged, ctx) = route_sm(&[("service_name", "v3")], r#"{"__error_details__":"bdet"}"#);
        assert_eq!(merged, owned_pairs(&[("service_name", "v3")]));
        assert_eq!(ctx.details, "bdet");
        assert!(!ctx.has_ordinary && ctx.err.is_empty());
        // v9: mixed reserved err + ordinary.
        let (merged, ctx) = route_sm(
            &[("service_name", "v9")],
            r#"{"__error__":"boom","trace_id":"abc"}"#,
        );
        assert_eq!(
            merged,
            owned_pairs(&[("service_name", "v9"), ("trace_id", "abc")])
        );
        assert_eq!(ctx.err, "boom");
        assert!(ctx.has_ordinary);
        // v1: ordinary only.
        let (merged, ctx) = route_sm(&[("service_name", "v1")], r#"{"trace_id":"abc"}"#);
        assert_eq!(
            merged,
            owned_pairs(&[("service_name", "v1"), ("trace_id", "abc")])
        );
        assert!(ctx.has_ordinary && ctx.err.is_empty() && ctx.details.is_empty());
    }

    /// D5 (kills W2 at the routing seam): EMPTY reserved values are
    /// assigned verbatim — and an empty slot is unset (`is_empty`), so the
    /// v4/v5/v12 shapes contribute nothing at all.
    #[test]
    fn empty_reserved_sm_values_leave_the_ctx_empty() {
        for (id, sm_json) in [
            ("v4", r#"{"__error__":""}"#),
            ("v5", r#"{"__error_details__":""}"#),
            ("v12", r#"{"__error__":"","__error_details__":""}"#),
        ] {
            let (merged, ctx) = route_sm(&[("service_name", id)], sm_json);
            assert_eq!(merged, owned_pairs(&[("service_name", id)]), "{id}");
            assert!(ctx.is_empty(), "{id}: {ctx:?}");
        }
    }

    /// D4 (kills W7): `has_ordinary` counts NON-EMPTY ordinary entries only
    /// — the reference's distributor strips empty-valued SM before it can
    /// reach the builder (#259 tracks PulsusDB's ingest divergence; the
    /// stray `trace_id=""` in the merged set is that issue, not this one).
    #[test]
    fn empty_ordinary_sm_values_do_not_set_has_ordinary() {
        // w1.
        let (merged, ctx) = route_sm(&[("service_name", "w1")], r#"{"trace_id":""}"#);
        assert_eq!(
            merged,
            owned_pairs(&[("service_name", "w1"), ("trace_id", "")])
        );
        assert!(!ctx.has_ordinary);
        // w2: an empty ordinary + a reserved details.
        let (merged, ctx) = route_sm(
            &[("service_name", "w2")],
            r#"{"trace_id":"","__error_details__":"bdet"}"#,
        );
        assert_eq!(
            merged,
            owned_pairs(&[("service_name", "w2"), ("trace_id", "")])
        );
        assert_eq!(ctx.details, "bdet");
        assert!(!ctx.has_ordinary);
    }

    /// D1 (kills W3a — C7's routing seam): the `_extracted` base-collision
    /// suffix runs BEFORE the reserved-name test, so a base `__error__` +
    /// SM `__error__` yields an ORDINARY `__error___extracted` entry and an
    /// empty ctx.
    #[test]
    fn the_extracted_suffix_preempts_the_reserved_err_check() {
        let (merged, ctx) = route_sm(
            &[("__error__", "streamerr"), ("service_name", "v10")],
            r#"{"__error__":"boom"}"#,
        );
        assert_eq!(
            merged,
            owned_pairs(&[
                ("__error__", "streamerr"),
                ("__error___extracted", "boom"),
                ("service_name", "v10"),
            ])
        );
        assert!(ctx.err.is_empty());
        assert!(ctx.has_ordinary);
    }

    /// D1 (kills W3b — C25/C26's routing seam): same for the details branch
    /// (`v13`, the round-4 test gap).
    #[test]
    fn the_extracted_suffix_preempts_the_reserved_details_check() {
        let (merged, ctx) = route_sm(
            &[("__error_details__", "streamdet"), ("service_name", "v13")],
            r#"{"__error_details__":"smdet"}"#,
        );
        assert_eq!(
            merged,
            owned_pairs(&[
                ("__error_details__", "streamdet"),
                ("__error_details___extracted", "smdet"),
                ("service_name", "v13"),
            ])
        );
        assert!(ctx.details.is_empty());
        assert!(ctx.has_ordinary);
    }

    /// The bare-selector (no-pipeline) fast-path rows: C3, C5, C7, C10,
    /// C13, C14, C19, C25, C29 through `append_visible`'s gate — the same
    /// `visible()` rule the pipeline applies at emit, owned by this path
    /// because no `CompiledPipeline` runs here (kills W15, and W1/W2/W3/W7
    /// on this path).
    #[test]
    fn fast_path_bare_selector_rows_apply_the_materialisation_gate() {
        // C3: reserved err emits on a clean builder.
        assert_eq!(
            fast_path_labels(&[("service_name", "v2")], r#"{"__error__":"boom"}"#),
            owned_pairs(&[("__error__", "boom"), ("service_name", "v2")])
        );
        // C5: a lone details slot is INVISIBLE.
        assert_eq!(
            fast_path_labels(&[("service_name", "v3")], r#"{"__error_details__":"bdet"}"#),
            owned_pairs(&[("service_name", "v3")])
        );
        // C7: suffix-before-reserved (err branch).
        assert_eq!(
            fast_path_labels(
                &[("__error__", "streamerr"), ("service_name", "v10")],
                r#"{"__error__":"boom"}"#
            ),
            owned_pairs(&[
                ("__error__", "streamerr"),
                ("__error___extracted", "boom"),
                ("service_name", "v10"),
            ])
        );
        // C10/C14: empty reserved values contribute nothing.
        assert_eq!(
            fast_path_labels(&[("service_name", "v4")], r#"{"__error__":""}"#),
            owned_pairs(&[("service_name", "v4")])
        );
        assert_eq!(
            fast_path_labels(
                &[("service_name", "v12")],
                r#"{"__error__":"","__error_details__":""}"#
            ),
            owned_pairs(&[("service_name", "v12")])
        );
        // C13: empty err + non-empty details, clean -> nothing.
        assert_eq!(
            fast_path_labels(
                &[("service_name", "v6")],
                r#"{"__error__":"","__error_details__":"bdet"}"#
            ),
            owned_pairs(&[("service_name", "v6")])
        );
        // C19: details + ordinary dirt -> visible.
        assert_eq!(
            fast_path_labels(
                &[("service_name", "v11")],
                r#"{"__error_details__":"bdet","trace_id":"abc"}"#
            ),
            owned_pairs(&[
                ("__error_details__", "bdet"),
                ("service_name", "v11"),
                ("trace_id", "abc"),
            ])
        );
        // C25: suffix-before-reserved (details branch) — both entries stay.
        assert_eq!(
            fast_path_labels(
                &[("__error_details__", "streamdet"), ("service_name", "v13")],
                r#"{"__error_details__":"smdet"}"#
            ),
            owned_pairs(&[
                ("__error_details__", "streamdet"),
                ("__error_details___extracted", "smdet"),
                ("service_name", "v13"),
            ])
        );
        // C29 (kills W7 on this path): an empty ordinary entry does not
        // open the details gate (`trace_id=""` itself is #259).
        assert_eq!(
            fast_path_labels(
                &[("service_name", "w2")],
                r#"{"trace_id":"","__error_details__":"bdet"}"#
            ),
            owned_pairs(&[("service_name", "w2"), ("trace_id", "")])
        );
    }

    /// `fan_out_sm_fast_path` itself applies the gate (binding
    /// `append_visible` into the real path, not just the helper): the
    /// reserved-err row surfaces `__error__`, the reserved-details row
    /// surfaces nothing.
    #[test]
    fn fan_out_sm_fast_path_applies_the_reserved_sm_gate() {
        let mut meta = HashMap::new();
        meta.insert(
            1u64,
            StreamMetaRow {
                fingerprint: 1,
                service: "v2".to_string(),
                labels: r#"{"service_name":"v2"}"#.to_string(),
            },
        );
        meta.insert(
            2u64,
            StreamMetaRow {
                fingerprint: 2,
                service: "v3".to_string(),
                labels: r#"{"service_name":"v3"}"#.to_string(),
            },
        );
        let rows = vec![
            SampleRow {
                fingerprint: 1,
                timestamp_ns: 1,
                body: "a=Hello b=World".to_string(),
                structured_metadata: r#"{"__error__":"boom"}"#.to_string(),
            },
            SampleRow {
                fingerprint: 2,
                timestamp_ns: 2,
                body: "a=Hello b=World".to_string(),
                structured_metadata: r#"{"__error_details__":"bdet"}"#.to_string(),
            },
        ];
        let mut got: Vec<String> = fan_out_sm_fast_path(&rows, &meta)
            .into_iter()
            .map(|s| s.labels_json)
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                r#"{"__error__":"boom","service_name":"v2"}"#.to_string(),
                r#"{"service_name":"v3"}"#.to_string(),
            ]
        );
    }

    /// C18 (kills W11 on the metric path): a pipeline-set error on a CLEAN
    /// builder still fails the metric query — the emit gate opens on
    /// `HasErr` alone. C17 (kills W5 on the metric path): after the bare
    /// drop the series carries NO orphaned details. Both rows use
    /// pipeline-set errors on the SM-free `v0` shape because
    /// `MetricScanRow` carries no structured metadata (#249) — SM-borne
    /// slots cannot reach `run_metric_into`.
    #[test]
    fn c17_c18_metric_path_pipeline_set_errors() {
        let base = vec![("service_name".to_string(), "v0".to_string())];
        // C18: `count_over_time({v0} | json [5m])` -> pipeline error.
        let compiled =
            CompiledPipeline::compile(&parse_pipeline(r#"{x="y"} | json"#)).expect("compile");
        let mut labels = Vec::new();
        let MetricRun::Kept { .. } = compiled
            .run_metric_into("a=Hello b=World", &base, 0, &mut labels)
            .expect("no budget breach")
        else {
            panic!("an errored line is kept for the surviving-error check");
        };
        let err = check_surviving_error(&labels).expect_err("surviving __error__ fails");
        assert!(
            matches!(&err, ReadError::MetricPipelineError { error_type, .. }
                if error_type == "JSONParserErr"),
            "{err:?}"
        );
        // C17: `count_over_time({v0} | json | drop __error__ [5m])` -> ok,
        // series `{service_name="v0"}` with NO details.
        let compiled =
            CompiledPipeline::compile(&parse_pipeline(r#"{x="y"} | json | drop __error__"#))
                .expect("compile");
        let mut labels = Vec::new();
        let MetricRun::Kept { .. } = compiled
            .run_metric_into("a=Hello b=World", &base, 0, &mut labels)
            .expect("no budget breach")
        else {
            panic!("kept");
        };
        check_surviving_error(&labels).expect("no surviving error");
        let got: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(got, vec![("service_name".to_string(), "v0".to_string())]);
    }

    /// D8's metric-failure discriminator, composed hermetically (the metric
    /// scan itself carries no SM — #249): a reserved-err SM row would fail
    /// `check_surviving_error` while a reserved-details row would not
    /// (reference: `count_over_time({v2}[5m])` -> 400 'boom',
    /// `count_over_time({v3}[5m])` -> 200).
    #[test]
    fn reserved_err_sm_fails_the_surviving_error_check_details_do_not() {
        let compiled = CompiledPipeline::compile(&parse_pipeline(r#"{x="y"}"#)).expect("compile");
        let base = vec![("service_name".to_string(), "s".to_string())];
        let err_ctx = StructuredMetadataCtx {
            err: "boom".to_string(),
            details: String::new(),
            has_ordinary: false,
        };
        let mut labels = Vec::new();
        compiled
            .run_into_with_sm("line", &base, 0, &err_ctx, &mut labels)
            .expect("kept");
        assert!(check_surviving_error(&labels).is_err());
        let details_ctx = StructuredMetadataCtx {
            err: String::new(),
            details: "bdet".to_string(),
            has_ordinary: false,
        };
        let mut labels = Vec::new();
        compiled
            .run_into_with_sm("line", &base, 0, &details_ctx, &mut labels)
            .expect("kept");
        check_surviving_error(&labels).expect("a lone details slot never fails a query");
    }
    // -----------------------------------------------------------------
    // Issue #221: variants fan-out — caps, charges, arena, guards.
    // -----------------------------------------------------------------

    fn variants_ctx() -> PlanCtx<'static> {
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

    const VSEC: i64 = 1_000_000_000;

    fn variants_range_spec() -> QuerySpec {
        QuerySpec::Range {
            start_ns: 0,
            end_ns: 60 * VSEC,
            step_ns: 60 * VSEC as u64 as i64 as u64,
        }
    }

    /// Plans a variants query and returns `(scan, specs, spec_bytes)`.
    fn variants_fixture(query: &str, spec: QuerySpec) -> (MetricPlan, Vec<plan::VariantSpec>, u64) {
        let expr = pulsus_logql::parse(query).expect("parse");
        let params = QueryParams {
            spec,
            limit: 100,
            direction: Direction::Backward,
        };
        match plan::plan(&expr, &params, &variants_ctx()).expect("plan") {
            Plan::MetricBinary(MetricNode::Variants {
                scan,
                variants,
                spec_bytes,
            }) => (*scan, variants, spec_bytes),
            other => panic!("expected a variants plan, got {other:?}"),
        }
    }

    /// `variants(<v>, <v>, …xN) of (<common>)`.
    fn n_variant_query(n: usize, variant: &str, common: &str) -> String {
        let list = vec![variant; n].join(", ");
        format!("variants({list}) of ({common})")
    }

    // -----------------------------------------------------------------
    // Issue #236 Part B — `VectorAccum`, the reference's streaming
    // accumulator.
    // -----------------------------------------------------------------

    /// AC 6 — every reducing arm reproduces grafana/loki v3.7.4
    /// `pkg/logql/evaluator.go` at the EXACT BITS, with each assertion
    /// carrying its line cite.
    ///
    /// The three datasets are chosen so each op DISCRIMINATES against the
    /// pre-#236 formula (`sum/len` for avg, a two-pass
    /// `population_variance` for stddev/stdvar): reverting any one arm
    /// changes the asserted bits. Compared through `to_bits`, never `==`
    /// — one float rendering is a prefix of another and `assert_eq!` on
    /// `f64` hides a last-bit difference in the printed output.
    #[test]
    fn vector_accum_reproduces_the_reference_recurrence_bit_for_bit() {
        fn run(op: VectorAggOp, vals: &[f64]) -> f64 {
            let mut acc = VectorAccum::seed(op, vals[0]);
            for v in &vals[1..] {
                acc.update(op, *v);
            }
            acc.finish(op)
        }

        // `:530-531` avg — Welford mean, NOT `sum/len`. Two-pass gives
        // 4610184818551597739; the recurrence gives one ULP below.
        assert_eq!(
            run(VectorAggOp::Avg, &[1.0, 1.0, 3.0]).to_bits(),
            4610184818551597738u64,
            "avg must be the Welford mean (evaluator.go:530-531, :588)"
        );

        // `:547-550` + `:596` stdvar — M2/n. Two-pass gives
        // 4597174419628082972.
        assert_eq!(
            run(VectorAggOp::Stdvar, &[1.0, 1.0, 2.0]).to_bits(),
            4597174419628082973u64,
            "stdvar must be Welford M2/n (evaluator.go:547-550, :596)"
        );

        // `:547-550` + `:594` stddev — sqrt(M2/n). Two-pass gives
        // 4612489961860455552.
        assert_eq!(
            run(VectorAggOp::Stddev, &[1.0, 1.0, 6.0]).to_bits(),
            4612489961860455551u64,
            "stddev must be sqrt(Welford M2/n) (evaluator.go:547-550, :594)"
        );

        // `:527` sum, `:544`+`:591` count — unchanged by the port.
        assert_eq!(run(VectorAggOp::Sum, &[1.0, 2.0, 4.0]), 7.0);
        assert_eq!(run(VectorAggOp::Count, &[9.0, 9.0, 9.0]), 3.0);

        // `:534-541` min/max over an ALL-NaN group is NaN, not ±INF.
        //
        // **Where the behavioural difference actually lives — measured,
        // not assumed.** The defect was the SEED, not the comparison:
        // PulsusDB's old `fold(f64::INFINITY, f64::min)` started from an
        // infinity that no member could displace, because Rust's
        // `f64::min` prefers the non-NaN operand. Seeding from the first
        // member (as `evaluator.go:481` does) is what fixes it.
        //
        // The `|| IsNaN(group.value)` disjunct is transcription fidelity
        // rather than a second behavioural fix: substituting
        // `self.value.min(f)` for the whole arm leaves every assertion
        // below passing, because Rust's `f64::min`/`max` already agree
        // with the reference's comparison on all three NaN placements.
        // Recorded so nobody reads these cases as pinning the disjunct —
        // they pin the seed.
        let nans = [f64::NAN, f64::NAN, f64::NAN];
        assert!(
            run(VectorAggOp::Min, &nans).is_nan(),
            "all-NaN min must be NaN (evaluator.go:539-541)"
        );
        assert!(
            run(VectorAggOp::Max, &nans).is_nan(),
            "all-NaN max must be NaN (evaluator.go:534-536)"
        );

        // ...but a NaN seed followed by a real sample takes the real one
        // (the `IsNaN(group.value)` disjunct), and a real seed followed by
        // NaN keeps the real one (every comparison with NaN is false).
        assert_eq!(run(VectorAggOp::Max, &[f64::NAN, 5.0]), 5.0);
        assert_eq!(run(VectorAggOp::Min, &[f64::NAN, 5.0]), 5.0);
        assert_eq!(run(VectorAggOp::Max, &[5.0, f64::NAN]), 5.0);
        assert_eq!(run(VectorAggOp::Min, &[5.0, f64::NAN]), 5.0);
    }

    /// Issue #236 Part B — the reduction-order pin is a GATE, not a
    /// convention.
    ///
    /// Welford is order-sensitive, so a hash-walk member order makes
    /// `avg`/`stddev`/`stdvar` vary between runs. This drives
    /// `group_instant`/`group_range` over EVERY permutation of a dataset
    /// known to discriminate (`{2,4,6,8}`: 20 of its 24 orders give
    /// `stdvar` exactly `5.0`, the other 4 give `4.999999999999999`) and
    /// asserts one single output value across all of them.
    ///
    /// Without the pin this fails on 4 of the 24 inputs; the assertion
    /// therefore cannot pass vacuously, and the unit is PERMUTATIONS OF
    /// THE INPUT SERIES VECTOR, not runs of the process — which makes it
    /// deterministic in CI rather than a 1-in-6 flake.
    #[test]
    fn the_reduction_order_pin_makes_welford_input_order_independent() {
        /// Every permutation of `[0, 1, 2, 3]` (Heap's algorithm,
        /// iterative). Written out rather than pulling in a combinatorics
        /// dependency the plan does not specify.
        fn permutations_of_four() -> Vec<[usize; 4]> {
            let mut out = Vec::with_capacity(24);
            let mut a = [0usize, 1, 2, 3];
            let mut c = [0usize; 4];
            out.push(a);
            let mut i = 1;
            while i < 4 {
                if c[i] < i {
                    if i % 2 == 0 {
                        a.swap(0, i);
                    } else {
                        a.swap(c[i], i);
                    }
                    out.push(a);
                    c[i] += 1;
                    i = 1;
                } else {
                    c[i] = 0;
                    i += 1;
                }
            }
            out
        }

        let vals = [2.0f64, 4.0, 6.0, 8.0];
        let perms = permutations_of_four();
        assert_eq!(
            perms.len(),
            24,
            "the unit is PERMUTATIONS of the input vector"
        );
        // Distinct label sets, so the pin has a total order to impose and
        // every permutation is a genuinely different input vector.
        let labels_for =
            |v: f64| -> LabelSet { vec![("host".to_string(), format!("h{}", v as u64))] };

        for op in [VectorAggOp::Stdvar, VectorAggOp::Stddev, VectorAggOp::Avg] {
            let mut instant_seen: BTreeSet<u64> = BTreeSet::new();
            let mut range_seen: BTreeSet<u64> = BTreeSet::new();

            for perm in &perms {
                let instant: Vec<InstantSeries> = perm
                    .iter()
                    .map(|&i| InstantSeries {
                        labels: labels_for(vals[i]),
                        value: vals[i],
                    })
                    .collect();
                // Bare aggregation (`grouping: None`) collapses all four
                // into ONE group, which is what exposes member order.
                let out = group_instant(instant, op, None, None);
                assert_eq!(out.len(), 1);
                instant_seen.insert(out[0].value.to_bits());

                let range: Vec<RangeSeries> = perm
                    .iter()
                    .map(|&i| RangeSeries {
                        labels: labels_for(vals[i]),
                        points: BTreeMap::from([(0i64, vals[i])]),
                    })
                    .collect();
                let out = group_range(range, op, None, None);
                assert_eq!(out.len(), 1);
                range_seen.insert(out[0].points[&0].to_bits());
            }

            assert_eq!(
                instant_seen.len(),
                1,
                "group_instant {op:?} produced {} distinct values across the 24 \
                 member orders — the reduction-order pin is not holding: {instant_seen:?}",
                instant_seen.len()
            );
            assert_eq!(
                range_seen.len(),
                1,
                "group_range {op:?} produced {} distinct values across the 24 \
                 member orders — the reduction-order pin is not holding: {range_seen:?}",
                range_seen.len()
            );
            // Instant and range must also agree with EACH OTHER — the
            // whole point of both routing through `VectorAccum`.
            assert_eq!(instant_seen, range_seen, "{op:?} instant/range disagree");
        }

        // ...and the pinned value is the one the committed corpus
        // captured from the reference (the 20-of-24 majority basin).
        let instant: Vec<InstantSeries> = vals
            .iter()
            .map(|v| InstantSeries {
                labels: labels_for(*v),
                value: *v,
            })
            .collect();
        let out = group_instant(instant, VectorAggOp::Stdvar, None, None);
        assert_eq!(
            out[0].value.to_bits(),
            5.0f64.to_bits(),
            "the pin must reproduce b4_vector_aggs.test's captured stdvar"
        );
    }

    /// `reduce` and the accumulator are the SAME computation — the
    /// property that lets the range fold and the materialising instant
    /// path never disagree. Asserted on bits over every reducing op.
    #[test]
    fn reduce_routes_through_the_accumulator_for_every_reducing_op() {
        const OPS: [VectorAggOp; 7] = [
            VectorAggOp::Sum,
            VectorAggOp::Avg,
            VectorAggOp::Min,
            VectorAggOp::Max,
            VectorAggOp::Count,
            VectorAggOp::Stddev,
            VectorAggOp::Stdvar,
        ];
        let vals = [1.0, 1.0, 3.0, -2.5, 7.25];
        for op in OPS {
            assert!(VectorAccum::is_reduction(op), "{op:?}");
            let mut acc = VectorAccum::seed(op, vals[0]);
            for v in &vals[1..] {
                acc.update(op, *v);
            }
            assert_eq!(
                reduce(op, &vals).to_bits(),
                acc.finish(op).to_bits(),
                "reduce and VectorAccum must agree bit-for-bit on {op:?}"
            );
        }
        // The partition is total and the selecting side is disjoint.
        for op in [
            VectorAggOp::Topk,
            VectorAggOp::Bottomk,
            VectorAggOp::ApproxTopk,
            VectorAggOp::Sort,
            VectorAggOp::SortDesc,
        ] {
            assert!(!VectorAccum::is_reduction(op), "{op:?}");
        }
    }

    // -----------------------------------------------------------------
    // Issue #236 Part B — the streaming fold.
    // -----------------------------------------------------------------

    const REDUCING_OPS: [VectorAggOp; 7] = [
        VectorAggOp::Sum,
        VectorAggOp::Avg,
        VectorAggOp::Min,
        VectorAggOp::Max,
        VectorAggOp::Count,
        VectorAggOp::Stddev,
        VectorAggOp::Stdvar,
    ];

    const SELECTING_OPS: [VectorAggOp; 5] = [
        VectorAggOp::Topk,
        VectorAggOp::Bottomk,
        VectorAggOp::ApproxTopk,
        VectorAggOp::Sort,
        VectorAggOp::SortDesc,
    ];

    /// The dense `Vec<VectorAccum>` a `ReduceFold` slot lives in uses
    /// `count == 0` as its "no member yet" sentinel. That is only sound
    /// because a SEEDED accumulator can never have `count == 0` — for
    /// EVERY reducing op, including the four that never touch `count`
    /// again. Break the seed's `count: 1` and this fails.
    #[test]
    fn vector_accum_seed_always_leaves_a_nonzero_count() {
        assert!(
            VectorAccum::EMPTY.is_empty(),
            "the sentinel must read as empty"
        );
        for op in REDUCING_OPS {
            for v in [0.0f64, -0.0, 1.5, f64::NAN, f64::INFINITY] {
                let mut acc = VectorAccum::seed(op, v);
                assert!(
                    !acc.is_empty(),
                    "{op:?} seeded from {v} must not read as the EMPTY sentinel"
                );
                acc.update(op, 2.0);
                assert!(!acc.is_empty(), "{op:?} after update");
            }
        }
    }

    /// The fold's op partition is the SAME partition
    /// [`VectorAccum::is_reduction`] states, plus the two the leaf cannot
    /// own. Both matches are exhaustive with no `_` arm, so a new
    /// operator is a build failure; this pins that they agree with each
    /// other rather than each being separately exhaustive and wrong.
    #[test]
    fn vector_agg_fold_partitions_every_op_like_is_reduction() {
        let grid = FoldGrid {
            start: 0,
            step: 1,
            kmax: 3,
        };
        for op in REDUCING_OPS {
            let fold = VectorAggFold::new(&(op, None, None), grid, MAX_METRIC_RESULT_POINTS)
                .unwrap_or_else(|| panic!("{op:?} is reducing and must fold"));
            assert!(
                matches!(fold, VectorAggFold::Reduce(_)),
                "{op:?} must be a ReduceFold"
            );
            assert!(VectorAccum::is_reduction(op));
        }
        for op in SELECTING_OPS {
            assert!(!VectorAccum::is_reduction(op));
        }
        // `sort`/`sort_desc` are a matrix PASSTHROUGH at `group_range`,
        // and `approx_topk` is rejected for a range query at plan time —
        // the leaf declines all three and the caller materialises.
        for op in [
            VectorAggOp::Sort,
            VectorAggOp::SortDesc,
            VectorAggOp::ApproxTopk,
        ] {
            assert!(
                VectorAggFold::new(&(op, None, Some(3.0)), grid, MAX_METRIC_RESULT_POINTS)
                    .is_none(),
                "{op:?} must be declined by the leaf"
            );
        }
        for op in [VectorAggOp::Topk, VectorAggOp::Bottomk] {
            assert!(matches!(
                VectorAggFold::new(&(op, None, Some(2.0)), grid, MAX_METRIC_RESULT_POINTS),
                Some(VectorAggFold::Select(_))
            ));
            assert!(matches!(
                VectorAggFold::new(&(op, None, Some(0.0)), grid, MAX_METRIC_RESULT_POINTS),
                Some(VectorAggFold::Empty)
            ));
        }
    }

    /// A leaf series as the fold receives it: labels plus grid-aligned
    /// `(timestamp, value)` points.
    type FoldInput = (LabelSet, Vec<(i64, f64)>);

    fn fold_slots(fold: &VectorAggFold) -> u64 {
        fold.reserved_slots()
    }

    fn fold_labels(pairs: &[(&str, &str)]) -> LabelSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Drives `input` through a fold, in the given order.
    fn drive_fold(
        spec: &plan::VectorAggSpec,
        grid: FoldGrid,
        input: &[FoldInput],
    ) -> Vec<MatrixSeries> {
        let mut fold =
            VectorAggFold::new(spec, grid, MAX_METRIC_RESULT_POINTS).expect("this spec folds");
        for (labels, points) in input {
            fold.push_series(labels, points).expect("on-grid points");
        }
        fold.finish()
    }

    /// Drives the same input through `select_k_range`, the materialising
    /// implementation the fold must reproduce.
    fn drive_select_k_range(
        op: VectorAggOp,
        grouping: Option<&Grouping>,
        param: Option<f64>,
        input: &[FoldInput],
    ) -> Vec<(LabelSet, Vec<(i64, f64)>)> {
        let series: Vec<RangeSeries> = input
            .iter()
            .map(|(labels, points)| RangeSeries {
                labels: labels.clone(),
                points: points.iter().copied().collect(),
            })
            .collect();
        select_k_range(series, op, grouping, param)
            .into_iter()
            .map(|s| (s.labels, s.points.into_iter().collect()))
            .collect()
    }

    fn as_pairs(series: Vec<MatrixSeries>) -> Vec<(LabelSet, Vec<(i64, f64)>)> {
        series.into_iter().map(|s| (s.labels, s.points)).collect()
    }

    /// AC 9 — the SELECTION ORDER, not merely the selected set.
    ///
    /// `SelectFold`'s output SEQUENCE (survivors in original push order,
    /// each survivor's points ascending) and its values must be identical
    /// to `select_k_range` over the same explicit input vector, including
    /// the four adversarial shapes: equal values across series, a group
    /// where every value is equal, two series with identical EMPTY label
    /// sets (which is what makes the series-id tiebreak reachable), and
    /// NaN candidates.
    #[test]
    fn select_fold_reproduces_select_k_range_sequence_and_values() {
        let grid = FoldGrid {
            start: 0,
            step: 10,
            kmax: 2,
        };
        let pts = |vals: [f64; 3]| vec![(0i64, vals[0]), (10, vals[1]), (20, vals[2])];
        let cases: Vec<(&str, Vec<FoldInput>)> = vec![
            (
                "distinct values",
                vec![
                    (fold_labels(&[("h", "a")]), pts([1.0, 5.0, 3.0])),
                    (fold_labels(&[("h", "b")]), pts([4.0, 2.0, 9.0])),
                    (fold_labels(&[("h", "c")]), pts([7.0, 8.0, 0.5])),
                ],
            ),
            (
                "equal values ACROSS series (label tiebreak)",
                vec![
                    (fold_labels(&[("h", "c")]), pts([1.0, 1.0, 1.0])),
                    (fold_labels(&[("h", "a")]), pts([1.0, 1.0, 1.0])),
                    (fold_labels(&[("h", "b")]), pts([1.0, 1.0, 1.0])),
                ],
            ),
            (
                "an all-equal group of two",
                vec![
                    (fold_labels(&[("h", "a")]), pts([2.0, 2.0, 2.0])),
                    (fold_labels(&[("h", "b")]), pts([2.0, 2.0, 2.0])),
                ],
            ),
            (
                "two series with IDENTICAL EMPTY label sets (series-id tiebreak)",
                vec![
                    (Vec::new(), pts([3.0, 1.0, 4.0])),
                    (Vec::new(), pts([1.0, 5.0, 9.0])),
                    (fold_labels(&[("h", "z")]), pts([2.0, 6.0, 5.0])),
                ],
            ),
            (
                // THE discriminating shape for the series-id tiebreak.
                // Two series that tie need no tiebreak (both the fold and
                // the stable sort keep the earlier one), and three that
                // tie at EVERY step are indistinguishable in the output.
                // It takes THREE series with identical labels tying at
                // ONE step and differing at another for the choice to be
                // observable: at step 0 all three hold 2.0 with `k = 2`,
                // and which two survive decides who owns that point.
                "three IDENTICAL-label series tying at one step only",
                vec![
                    (Vec::new(), pts([2.0, 1.0, 1.0])),
                    (Vec::new(), pts([2.0, 2.0, 2.0])),
                    (Vec::new(), pts([2.0, 3.0, 3.0])),
                ],
            ),
            (
                "NaN candidates rank last in BOTH directions",
                vec![
                    (fold_labels(&[("h", "a")]), pts([f64::NAN, 5.0, 1.0])),
                    (fold_labels(&[("h", "b")]), pts([5.0, f64::NAN, 2.0])),
                    (fold_labels(&[("h", "c")]), pts([1.0, 2.0, f64::NAN])),
                ],
            ),
            (
                "every candidate NaN",
                vec![
                    (fold_labels(&[("h", "a")]), pts([f64::NAN; 3])),
                    (fold_labels(&[("h", "b")]), pts([f64::NAN; 3])),
                ],
            ),
        ];

        let bits = |v: Vec<(LabelSet, Vec<(i64, f64)>)>| -> Vec<(LabelSet, Vec<(i64, u64)>)> {
            v.into_iter()
                .map(|(l, p)| (l, p.into_iter().map(|(t, x)| (t, x.to_bits())).collect()))
                .collect()
        };

        for (name, input) in &cases {
            for op in [VectorAggOp::Topk, VectorAggOp::Bottomk] {
                for k in [1.0f64, 2.0, 3.0, 9.0] {
                    let folded = bits(as_pairs(drive_fold(&(op, None, Some(k)), grid, input)));
                    let materialised = bits(drive_select_k_range(op, None, Some(k), input));
                    assert_eq!(
                        folded, materialised,
                        "{op:?}({k}) over `{name}`: the fold must reproduce \
                         select_k_range's SEQUENCE and values"
                    );
                }
            }
            // ...and with a grouping, so the group key is exercised too.
            let grouping = Grouping {
                kind: GroupingKind::By,
                labels: vec!["h".to_string()],
            };
            let folded = bits(as_pairs(drive_fold(
                &(VectorAggOp::Topk, Some(grouping.clone()), Some(1.0)),
                grid,
                input,
            )));
            let materialised = bits(drive_select_k_range(
                VectorAggOp::Topk,
                Some(&grouping),
                Some(1.0),
                input,
            ));
            assert_eq!(folded, materialised, "topk(1) by (h) over `{name}`");
        }
    }

    /// AC 8 — the fold's state is bounded by the OUTPUT, not by the scan.
    ///
    /// A range query over `N` leaf groups collapsing to `G` output groups
    /// over `S` steps retains exactly `G x S` cells, and running at `N`
    /// and `10N` retains the IDENTICAL number. Under the materialising
    /// path the same input holds `N x S` points before the aggregation
    /// runs, which is the quantity this replaces.
    #[test]
    fn the_fold_retains_output_groups_times_steps_whatever_the_scan_width() {
        const STEPS: usize = 7;
        let grid = FoldGrid {
            start: 0,
            step: 10,
            kmax: STEPS as i64 - 1,
        };
        // Two output groups (`by (tier)`), `n` leaf series feeding them.
        let grouping = Grouping {
            kind: GroupingKind::By,
            labels: vec!["tier".to_string()],
        };
        let leaf = |n: usize| -> Vec<FoldInput> {
            (0..n)
                .map(|i| {
                    (
                        fold_labels(&[
                            ("tier", if i % 2 == 0 { "hot" } else { "cold" }),
                            ("id", &i.to_string()),
                        ]),
                        (0..STEPS)
                            .map(|k| ((k as i64) * 10, (i + k) as f64))
                            .collect(),
                    )
                })
                .collect()
        };
        let cells_at = |n: usize| -> (usize, usize, usize) {
            let mut fold = VectorAggFold::new(
                &(VectorAggOp::Sum, Some(grouping.clone()), None),
                grid,
                MAX_METRIC_RESULT_POINTS,
            )
            .expect("sum folds");
            for (labels, points) in leaf(n) {
                fold.push_series(&labels, &points).expect("on-grid");
            }
            let cells = fold.cells();
            let groups = fold.groups();
            let out = fold.finish().len();
            (cells, groups, out)
        };
        let small = cells_at(20);
        let wide = cells_at(200);
        assert_eq!(small, (2 * STEPS, 2, 2), "G x S cells, G output series");
        assert_eq!(
            small, wide,
            "10x the leaf groups must retain the IDENTICAL cell count — the \
             fold is bounded by the OUTPUT, not by the scan"
        );
    }

    /// AC 10 — `topk(0, …)`/`bottomk(0, …)`: no group is ever
    /// constructed and no cell is ever retained, however wide the scan.
    /// A fold that counted groups before consulting `k` would build 501
    /// of them here.
    #[test]
    fn zero_k_fold_constructs_no_group_and_retains_no_cell() {
        let grid = FoldGrid {
            start: 0,
            step: 1,
            kmax: 100,
        };
        // Bare AND `by (id)`: the grouped shape is the one that would
        // build 501 groups if `k` were consulted after the group.
        let by_id = Grouping {
            kind: GroupingKind::By,
            labels: vec!["id".to_string()],
        };
        let shapes = [None, Some(by_id)];
        for op in [VectorAggOp::Topk, VectorAggOp::Bottomk] {
            for grouping in &shapes {
                let mut fold = VectorAggFold::new(
                    &(op, grouping.clone(), Some(0.0)),
                    grid,
                    MAX_METRIC_RESULT_POINTS,
                )
                .expect("k == 0 still folds");
                for i in 0..501u32 {
                    let labels = fold_labels(&[("id", &i.to_string())]);
                    let points: Vec<(i64, f64)> =
                        (0..101).map(|k| (k, (i + k as u32) as f64)).collect();
                    fold.push_series(&labels, &points).expect("no-op push");
                }
                // The RESOURCE claim first, so a mutant that keeps a real
                // selection state at `k == 0` fails on what actually matters
                // rather than on the enum's shape.
                assert_eq!(fold.groups(), 0, "{op:?}: no group may be constructed");
                assert_eq!(fold.cells(), 0, "{op:?}: no cell may be retained");
                assert!(
                    matches!(fold, VectorAggFold::Empty),
                    "{op:?}: k == 0 is the structurally-empty fold"
                );
                assert!(fold.finish().is_empty(), "{op:?}: the result is empty");
            }
        }
    }

    /// A point that is not on the query grid is an internal-invariant
    /// breach and is REPORTED, never silently dropped — a dropped point
    /// is a silently wrong result.
    #[test]
    fn a_point_off_the_query_grid_is_an_error_not_a_dropped_point() {
        let grid = FoldGrid {
            start: 100,
            step: 10,
            kmax: 4,
        };
        assert_eq!(grid.index_of(100), Some(0));
        assert_eq!(grid.index_of(140), Some(4));
        assert_eq!(grid.index_of(150), None, "past kmax");
        assert_eq!(grid.index_of(105), None, "between grid points");
        assert_eq!(grid.index_of(90), None, "before the grid");
        for k in 0..=4usize {
            assert_eq!(grid.index_of(grid.point(k)), Some(k), "point/index inverse");
        }
        let mut fold = VectorAggFold::new(
            &(VectorAggOp::Sum, None, None),
            grid,
            MAX_METRIC_RESULT_POINTS,
        )
        .expect("sum folds");
        match fold.push_series(&Vec::new(), &[(105, 1.0)]) {
            Err(ReadError::PipelineInvalid { reason }) => {
                assert!(reason.contains("off the query grid"), "{reason}");
            }
            other => panic!("expected an off-grid error, got {other:?}"),
        }
    }

    /// Issue #236 Part B — **the reduction-order pin extends to the
    /// fold**, and the gate is exhaustive rather than sampled.
    ///
    /// `RangeSlideState::emit` puts the leaf's series in label-set order
    /// before folding, which is the same total order `pin_reduction_order`
    /// imposes on the materialising path. This drives `emit` over ALL 24
    /// permutations of the discriminating `{2,4,6,8}` dataset (unit:
    /// permutations of the emitted series vector, not runs of the
    /// process) and asserts one single value — and that it is the value
    /// `group_range` produces over the same data.
    ///
    /// Emptying the sort makes this fail on 4 of the 24 inputs, every
    /// run.
    #[test]
    fn the_reduction_order_pin_extends_to_the_fold() {
        fn permutations_of_four() -> Vec<[usize; 4]> {
            let mut out = Vec::with_capacity(24);
            let mut a = [0usize, 1, 2, 3];
            let mut c = [0usize; 4];
            out.push(a);
            let mut i = 1;
            while i < 4 {
                if c[i] < i {
                    if i % 2 == 0 {
                        a.swap(0, i);
                    } else {
                        a.swap(c[i], i);
                    }
                    out.push(a);
                    c[i] += 1;
                    i = 1;
                } else {
                    c[i] = 0;
                    i += 1;
                }
            }
            out
        }

        let vals = [2.0f64, 4.0, 6.0, 8.0];
        let perms = permutations_of_four();
        assert_eq!(
            perms.len(),
            24,
            "the unit is PERMUTATIONS of the emitted series vector"
        );
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 0, 10, 10);

        for op in [VectorAggOp::Stdvar, VectorAggOp::Stddev, VectorAggOp::Avg] {
            let mut seen: BTreeSet<u64> = BTreeSet::new();
            for perm in &perms {
                let mut state =
                    RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                        .expect("state");
                state.attach_fold(&(op, None, None));
                assert_eq!(state.folded_aggs(), 1);
                let emitted: Vec<MatrixSeries> = perm
                    .iter()
                    .map(|&i| MatrixSeries {
                        labels: vec![("host".to_string(), format!("h{}", vals[i] as u64))],
                        points: vec![(0i64, vals[i])],
                    })
                    .collect();
                let QueryResult::Matrix(out) = state.emit(emitted).expect("emit") else {
                    panic!("a range leaf emits a matrix");
                };
                assert_eq!(out.len(), 1, "a bare aggregation collapses to one series");
                seen.insert(out[0].points[0].1.to_bits());
            }
            assert_eq!(
                seen.len(),
                1,
                "the fold produced {} distinct {op:?} values across the 24 emission \
                 orders — the reduction-order pin is not holding in the fold: {seen:?}",
                seen.len()
            );
            // ...and it is the SAME value the materialising path gives.
            let materialised = group_range(
                vals.iter()
                    .map(|v| RangeSeries {
                        labels: vec![("host".to_string(), format!("h{}", *v as u64))],
                        points: BTreeMap::from([(0i64, *v)]),
                    })
                    .collect(),
                op,
                None,
                None,
            );
            assert_eq!(
                seen.iter().copied().collect::<Vec<u64>>(),
                vec![materialised[0].points[&0].to_bits()],
                "{op:?}: folded and materialised must be the same bits"
            );
        }
    }

    /// The mutating (fan-out) arm hands the fold its groups in label-set
    /// order, so the fold's member order is a property of the DATA and
    /// not of a per-process hash seed.
    ///
    /// Observable without a fold too: the emitted series come out
    /// label-ascending where they used to come out in `HashMap` walk
    /// order. With 6 distinct groups a walk order that happens to be
    /// sorted has probability 1/720, so removing the sort reddens this on
    /// essentially every run — stated rather than implied.
    #[test]
    fn the_mutating_finish_emits_its_groups_in_label_order() {
        let client = ClientAgg {
            pipeline: parse_pipeline(r#"{app="a"} | logfmt"#),
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        assert!(
            compiled.metric_mutates_labels(),
            "this fixture must take the fan-out arm"
        );
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        // Bodies chosen so the rendered group labels do NOT sort the way
        // the insertion order does.
        let rows = slide_rows(
            1,
            &[
                (10, "id=zeta"),
                (11, "id=mike"),
                (12, "id=alpha"),
                (13, "id=romeo"),
                (14, "id=bravo"),
                (15, "id=xray"),
            ],
        );
        let window = slide_window(0, 20, 10, 20);
        let res = run_client_agg_rows(&rows, &compiled, &meta, &client, window, None).unwrap();
        let QueryResult::Matrix(out) = res else {
            panic!("expected a matrix");
        };
        assert_eq!(out.len(), 6, "one group per distinct id");
        let labels: Vec<LabelSet> = out.iter().map(|s| s.labels.clone()).collect();
        let mut sorted = labels.clone();
        sorted.sort();
        assert_eq!(
            labels, sorted,
            "the fan-out arm must emit label-ascending — that ordering is what \
             pins the fold's member order"
        );
    }

    // -----------------------------------------------------------------
    // Issue #236 — the result-series cap.
    // -----------------------------------------------------------------

    fn n_vector(n: usize) -> QueryResult {
        QueryResult::Vector(
            (0..n)
                .map(|i| VectorSample {
                    labels: vec![("id".to_string(), i.to_string())],
                    value: i as f64,
                })
                .collect(),
        )
    }

    fn n_matrix(n: usize) -> QueryResult {
        QueryResult::Matrix(
            (0..n)
                .map(|i| MatrixSeries {
                    labels: vec![("id".to_string(), i.to_string())],
                    points: vec![(0, i as f64), (1, i as f64)],
                })
                .collect(),
        )
    }

    /// AC 2 — `ensure_result_series` counts TOP-LEVEL SERIES and uses the
    /// reference's own `> cap` test, so exactly `MAX_QUERY_SERIES` is
    /// served and `cap + 1` is refused. Both vector and matrix shapes,
    /// plus the histogram twins and the three pass-through variants.
    #[test]
    fn ensure_result_series_admits_exactly_the_cap_and_refuses_one_more() {
        let cap = MAX_QUERY_SERIES as usize;
        assert_eq!(cap, 500);

        for at in [n_vector(cap), n_matrix(cap)] {
            ensure_result_series(&at).expect("exactly the cap must be served");
        }
        for over in [n_vector(cap + 1), n_matrix(cap + 1)] {
            match ensure_result_series(&over) {
                Err(ReadError::QueryTooBroad(TooBroadReason::MetricSeries { cap: got })) => {
                    assert_eq!(got, MAX_QUERY_SERIES);
                }
                other => panic!("expected MetricSeries, got {other:?}"),
            }
        }

        // A matrix with FEW series but MANY points is not a breach — the
        // reference counts distinct series, never points.
        let deep = QueryResult::Matrix(vec![MatrixSeries {
            labels: vec![],
            points: (0..100_000).map(|k| (k, k as f64)).collect(),
        }]);
        ensure_result_series(&deep).expect("point count is not the series axis");

        // Non-metric shapes pass: log streams are bounded by the entries
        // limit instead, and scalar/string carry no series at all.
        for other in [
            QueryResult::Streams {
                items: Vec::new(),
                partial: false,
            },
            QueryResult::Scalar(1.0),
            QueryResult::String("x".to_string()),
        ] {
            ensure_result_series(&other).expect("non-metric shapes carry no series axis");
        }
    }

    /// AC 11's static companion, as an executable check rather than a
    /// review-time grep: `MAX_QUERY_SERIES` is read by
    /// `ensure_result_series` and by NOTHING else in production source.
    ///
    /// This is the property the plan calls normative — a constant that is
    /// *currently* read once and one that *can only* be read once are
    /// different guarantees, and the second is what stops the next person
    /// reintroducing a mid-scan group cap.
    ///
    /// **Scope, stated because an unscoped conclusion from a scoped
    /// census is worthless:** every file in
    /// `crates/pulsus-read/src/logql/` (the whole tree in which the
    /// symbol is nameable), truncated at each file's `#[cfg(test)]`
    /// marker — i.e. PRODUCTION source only. Test code is deliberately
    /// out of scope: this very test reads the constant, and so does
    /// `ensure_result_series_admits_exactly_the_cap_and_refuses_one_more`.
    /// The counted unit is SOURCE LINES mentioning the identifier outside
    /// a comment or its own definition.
    #[test]
    fn max_query_series_is_read_in_exactly_one_place() {
        const SOURCES: &[(&str, &str)] = &[
            ("exec.rs", include_str!("exec.rs")),
            ("error.rs", include_str!("error.rs")),
            ("plan.rs", include_str!("plan.rs")),
            ("mod.rs", include_str!("mod.rs")),
            ("pipeline.rs", include_str!("pipeline.rs")),
            ("sql.rs", include_str!("sql.rs")),
            ("detected.rs", include_str!("detected.rs")),
            ("cms.rs", include_str!("cms.rs")),
            ("rows.rs", include_str!("rows.rs")),
            ("params.rs", include_str!("params.rs")),
            ("explain.rs", include_str!("explain.rs")),
            ("escape.rs", include_str!("escape.rs")),
            ("ip.rs", include_str!("ip.rs")),
        ];

        // **The file set must not be able to shrink silently.**
        // `include_str!` needs compile-time literals, so `SOURCES` is
        // written out — which means a source file added to `src/logql`
        // (the post-aggregation region is scheduled to move into one of
        // its own after #236 merges) would simply not be searched, and
        // this census would keep passing while covering less. That is the
        // gate-weakening shape this issue has hit repeatedly, so it is
        // closed here rather than noted: the list is compared against the
        // DIRECTORY at run time and a file it does not name is a loud
        // failure naming the file, not a quiet reduction in scope.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql");
        let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
            .expect("the logql source directory")
            .map(|e| e.expect("dir entry").path())
            .filter(|p| p.extension().is_some_and(|x| x == "rs"))
            .map(|p| {
                p.file_name()
                    .expect("file name")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        on_disk.sort();
        let mut named: Vec<String> = SOURCES.iter().map(|(n, _)| (*n).to_string()).collect();
        named.sort();
        assert_eq!(
            named, on_disk,
            "the census file set and `src/logql` have diverged — add the new file to \
             `SOURCES` (and to any other census scoped to this region) rather than \
             letting the search quietly cover less than the module"
        );

        /// Everything above the file's `#[cfg(test)]` marker.
        fn production(src: &str) -> &str {
            match src.find("\n#[cfg(test)]") {
                Some(i) => &src[..i],
                None => src,
            }
        }

        let mut reads: Vec<String> = Vec::new();
        for (name, src) in SOURCES {
            for (i, line) in production(src).lines().enumerate() {
                if !line.contains("MAX_QUERY_SERIES") {
                    continue;
                }
                let t = line.trim();
                // The definition and doc/line comments are not reads.
                if t.starts_with("///") || t.starts_with("//") || t.starts_with("pub const") {
                    continue;
                }
                reads.push(format!("{name}:{}: {t}", i + 1));
            }
        }
        // Exactly two production lines mention it, both inside
        // `ensure_result_series`: the `> cap` test and the error payload.
        assert_eq!(
            reads.len(),
            2,
            "MAX_QUERY_SERIES must be read only by ensure_result_series; found {reads:#?}"
        );
        for r in &reads {
            assert!(r.starts_with("exec.rs:"), "unexpected reader: {r}");
        }
        // ...and the deleted mid-scan group cap must not return, under
        // its own name, anywhere in the tree (tests included). The
        // needles are assembled at runtime so this test's OWN source does
        // not contain them — a literal here would match itself and the
        // assertion would fail for the wrong reason.
        let deleted_cap = format!("MAX_CLIENT_AGG{}SERIES", '_');
        let deleted_field = format!("caps{}series", '.');
        for (name, src) in SOURCES {
            assert!(
                !src.contains(&deleted_cap),
                "{name} still references the deleted mid-scan group cap"
            );
            assert!(
                !src.contains(&deleted_field),
                "{name} still reads a per-state series cap"
            );
        }
    }

    /// AC 30 — the query-text premise #236's derivation rests on is
    /// ENFORCED by this change, not assumed by it. #279 shipped the cap as
    /// an EXCLUSIVE maximum (`limits.rs:40` rejects `len >= cap`), so the
    /// boundary is pinned from BOTH sides: `cap - 1` accepted, `cap` and
    /// `cap + 1` rejected. Plan v14 phrases it as `cap + 1` only; asserting
    /// the accepted side too is what makes an off-by-one visible.
    #[test]
    fn the_query_text_cap_exists_is_finite_and_rejects_at_the_boundary() {
        let cap = pulsus_logql::MAX_QUERY_BYTES;
        assert!(cap > 0 && cap < usize::MAX, "the cap must be finite");
        assert_eq!(cap, 131_072);

        // A selector whose label VALUE is padded to hit an exact byte
        // length — valid LogQL at every length, so the only thing under
        // test is the admission check.
        let pad = |total: usize| {
            let (head, tail) = (r#"{a=""#, r#""}"#);
            format!(
                "{head}{}{tail}",
                "b".repeat(total - head.len() - tail.len())
            )
        };

        let ok = pad(cap - 1);
        assert_eq!(ok.len(), cap - 1);
        pulsus_logql::parse(&ok).expect("cap - 1 bytes must be accepted");

        for len in [cap, cap + 1] {
            let too_long = pad(len);
            assert_eq!(too_long.len(), len);
            match pulsus_logql::parse(&too_long) {
                Err(pulsus_logql::LogQlError::QueryTooLong {
                    len: got, cap: c, ..
                }) => {
                    assert_eq!((got, c), (len, cap));
                }
                other => panic!("{len} bytes must be rejected, got {other:?}"),
            }
        }
    }

    /// AC 30's second half — the feasible region's `aggs.len()` operand is
    /// `min(MAX_DEPTH, Q/4)`, read from BOTH constants rather than a
    /// literal, so neither can drift without this failing.
    ///
    /// `pulsus_logql::MAX_DEPTH` is `pub(crate)`, so the depth is read
    /// back from the typed error the parser actually raises rather than
    /// by widening another crate's API — which also pins the value the
    /// guard ENFORCES, not merely the one it declares.
    #[test]
    fn the_aggregation_depth_operand_reads_both_constants() {
        let q = pulsus_logql::MAX_QUERY_BYTES as u64;

        let too_deep = format!(
            "{}{}{}",
            "sum(".repeat(200),
            r#"count_over_time({a="b"}[1m])"#,
            ")".repeat(200)
        );
        assert!(too_deep.len() < pulsus_logql::MAX_QUERY_BYTES);
        let depth = match pulsus_logql::parse(&too_deep) {
            Err(pulsus_logql::LogQlError::RecursionLimitExceeded { limit, .. }) => limit as u64,
            other => panic!("expected RecursionLimitExceeded, got {other:?}"),
        };
        assert_eq!(depth, 64);

        // Every nesting level costs at least `sum(` — four bytes of text.
        let operand = depth.min(q / 4);
        assert_eq!(operand, depth, "at Q = {q} the parser depth is binding");
    }

    /// B5 / AC 9, re-derived by issue #236 (AC 35) — `AggCaps::DEFAULT` is
    /// the SIX remaining constants verbatim (`series` is deleted: it was
    /// the mid-scan group cap, and #236 moved the 500 to the final result
    /// as `MAX_QUERY_SERIES`); `divided` keeps the per-field sum ≤ the
    /// single-query bound with every field ≥ 1 for all admissible `n`; and
    /// the backstop is still DERIVED, now landing on
    /// `MAX_TS_COLLISION_GROUP == 10_000` instead of 500 — a strictly
    /// PERMISSIVE re-derivation (the reference is unbounded there).
    #[test]
    fn agg_caps_default_is_the_constants_and_divides_soundly() {
        let d = AggCaps::DEFAULT;
        assert_eq!(d.group_bytes, MAX_CLIENT_AGG_GROUP_BYTES);
        assert_eq!(d.retention_points, MAX_RETAINED_WINDOW_POINTS);
        assert_eq!(d.quantile_values, MAX_QUANTILE_VALUES);
        assert_eq!(d.counter_values, MAX_COUNTER_VALUES);
        assert_eq!(d.collision_members, MAX_TS_COLLISION_GROUP);
        assert_eq!(d.collision_bytes, MAX_TS_COLLISION_GROUP_BYTES);
        assert_eq!(d.result_points, MAX_METRIC_RESULT_POINTS);
        assert_eq!(d.divided(1), d, "divided(1) must be byte-identical");
        // The re-derivation, pinned to the SYMBOL and to the VALUE so a
        // future cap change cannot silently move the backstop.
        assert_eq!(d.min_field(), MAX_TS_COLLISION_GROUP);
        assert_eq!(d.min_field(), 10_000);
        assert_eq!(plan::MAX_VARIANT_SUB_STATES, d.min_field());
        for n in 1..=plan::MAX_VARIANT_SUB_STATES {
            let v = d.divided(n);
            for (field, whole) in [
                (v.group_bytes, d.group_bytes),
                (v.retention_points, d.retention_points),
                (v.quantile_values, d.quantile_values),
                (v.counter_values, d.counter_values),
                (v.collision_members, d.collision_members),
                (v.collision_bytes, d.collision_bytes),
                (v.result_points, d.result_points),
            ] {
                assert!(field >= 1, "divided({n}) floored a cap to 0");
                assert!(field * n <= whole, "divided({n}) sum exceeds the whole");
            }
        }
        // B12 — the collision staging caps are divided like every other
        // field (the fourth N-multiplied allocation, #221 Δ3.3).
        assert_eq!(
            d.divided(2).collision_bytes,
            MAX_TS_COLLISION_GROUP_BYTES / 2
        );
        assert_eq!(d.divided(2).collision_members, MAX_TS_COLLISION_GROUP / 2);
        // Issue #236 §4: the result-point cap divides like every other
        // field, so a `variants(...)` query's sub-states SUM to the
        // single-query bound rather than each getting the whole of it.
        assert_eq!(d.divided(2).result_points, MAX_METRIC_RESULT_POINTS / 2);

        // The hand-written field lists above are the census's weak point:
        // a NEW cap added to `AggCaps` would be divided by `divided` and
        // ignored here. Destructuring is what makes that a build failure
        // — add a field and this stops compiling until it is listed.
        let AggCaps {
            group_bytes: _,
            retention_points: _,
            quantile_values: _,
            counter_values: _,
            collision_members: _,
            collision_bytes: _,
            result_points: _,
        } = d;
    }

    /// (S)-leaf pin: the walk's `Copy ⇒ no owned heap` rule holds for
    /// every scalar leaf — a future field type that stops being `Copy` is
    /// a compile error here, which is precisely the P2 leaf test.
    #[test]
    fn every_s_leaf_in_the_walk_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<ClientWindow>();
        assert_copy::<ClientValue>();
        assert_copy::<RangeAggOp>();
        assert_copy::<VectorAggOp>();
        assert_copy::<GroupingKind>();
        assert_copy::<WinSample>();
        assert_copy::<AggCaps>();
    }

    /// I1 — CHARGE: the driver-buffer term. Pair: N = 64 vs 65 tail-free
    /// variants (single axis: N). Deleting the
    /// `variant_driver_buffer_bytes` charge makes the measured delta 0
    /// while the expected formula stays > 0.
    #[test]
    fn i1_driver_buffer_term_is_charged() {
        let common = r#"{app="x"}[5m]"#;
        let arena_charged = |n: usize| {
            let (scan, variants, _) = variants_fixture(
                &n_variant_query(n, r#"count_over_time({app="x"}[5m])"#, common),
                variants_range_spec(),
            );
            let cp = scan.client.expect("client scan");
            VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0)
                .expect("build")
                .charged_bytes()
        };
        let expected = variant_driver_buffer_bytes(65) - variant_driver_buffer_bytes(64);
        assert!(expected > 0);
        assert_eq!(arena_charged(65) - arena_charged(64), expected);
    }

    /// I2 — CHARGE: the arena-entry term. Pair: variant 1's tail
    /// identical to variant 0's vs distinct with EQUAL source bytes
    /// (single axis: tail identity). Deleting the per-entry charge before
    /// `extended_with` zeroes the measured delta.
    #[test]
    fn i2_arena_entry_term_is_charged() {
        let charged = |second: &str| {
            let q = format!(
                r#"variants(sum_over_time({{app="x"}} | unwrap aa [5m]), sum_over_time({{app="x"}} | unwrap {second} [5m])) of ({{app="x"}} | logfmt [5m])"#
            );
            let (scan, variants, _) = variants_fixture(&q, variants_range_spec());
            let cp = scan.client.expect("client scan");
            let tail1 = variants[1].client().pipeline.clone();
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            (arena.charged_bytes(), arena.len(), cp.pipeline, tail1)
        };
        let (same, same_len, _, _) = charged("aa");
        let (distinct, distinct_len, common, tail_bb) = charged("bb");
        assert_eq!(same_len, 2, "identical tails share one entry");
        assert_eq!(distinct_len, 3, "distinct tails add one entry each");
        let expected = variant_pipeline_entry_bytes(&common, &tail_bb);
        assert!(expected > 0);
        assert_eq!(distinct - same, expected);
    }

    /// I3 — CHARGE: the boxed sub-state slot. Pair: N = 2 vs 3, empty
    /// meta, `count_over_time`, no absent labels (single axis: N; every
    /// other `variant_state_bytes` term is 0).
    #[test]
    fn i3_sub_state_slot_term_is_charged() {
        let sub_charges = |n: usize| {
            let (scan, variants, _) = variants_fixture(
                &n_variant_query(n, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
                variants_range_spec(),
            );
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st =
                VariantsAggState::new(&arena, &variants, &HashMap::new(), u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        let expected = alloc_block_bytes(size_of::<RangeSlideState<'_>>() as u64);
        assert!(expected > 0);
        assert_eq!(sub_charges(3) - sub_charges(2), expected);
    }

    fn k_stream_meta(k: u64) -> HashMap<u64, StreamMetaRow> {
        (0..k)
            .map(|i| {
                (
                    i + 1,
                    StreamMetaRow {
                        fingerprint: i + 1,
                        service: format!("svc{i}"),
                        labels: format!(r#"{{"env":"prod","idx":"{i}"}}"#),
                    },
                )
            })
            .collect()
    }

    /// The I4/I5 expected meta term, built INDEPENDENTLY of
    /// `variant_meta_snapshot_bytes` (same inputs, formula spelled out) so
    /// deleting the runtime charge fails the equality.
    fn expected_meta_term(meta: &HashMap<u64, StreamMetaRow>, with_hashes: bool) -> u64 {
        let mut bytes = 0u64;
        for m in meta.values() {
            let labels = series_labels(m);
            bytes += map_entry_bytes(size_of::<(u64, LabelSet)>()) + label_set_bytes(&labels);
        }
        if with_hashes {
            bytes += meta.len() as u64 * map_entry_bytes(size_of::<(u64, u64)>());
        }
        bytes
    }

    /// I4 — CHARGE: the `base_labels` half of the meta snapshot. Pair:
    /// INSTANT kind (state type and constructor fixed), meta K = 4 vs 0.
    #[test]
    fn i4_meta_base_labels_term_is_charged() {
        let sub_charges = |meta: &HashMap<u64, StreamMetaRow>| {
            let (scan, variants, _) = variants_fixture(
                &n_variant_query(2, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
                QuerySpec::Instant { at_ns: 60 * VSEC },
            );
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st = VariantsAggState::new(&arena, &variants, meta, u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        let meta = k_stream_meta(4);
        let expected = expected_meta_term(&meta, false);
        assert!(expected > 0);
        assert_eq!(sub_charges(&meta) - sub_charges(&HashMap::new()), expected);
    }

    /// I5 — CHARGE: the `hashes` TABLE share, isolated from I4: the RANGE
    /// kind over the same meta pair adds exactly the `(u64, u64)` table
    /// term on top of I4's expression — so I5 fails alone when only the
    /// hashes half is deleted (I4 still passing).
    #[test]
    fn i5_meta_hashes_table_share_is_charged() {
        let sub_charges = |meta: &HashMap<u64, StreamMetaRow>| {
            let (scan, variants, _) = variants_fixture(
                &n_variant_query(2, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
                variants_range_spec(),
            );
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st = VariantsAggState::new(&arena, &variants, meta, u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        let meta = k_stream_meta(4);
        let expected = expected_meta_term(&meta, true);
        assert!(expected > expected_meta_term(&meta, false));
        assert_eq!(sub_charges(&meta) - sub_charges(&HashMap::new()), expected);
    }

    /// I6 — CHARGE: the range kind's construction-time `absent_labels`
    /// clone. Pair: same absent op, same (empty) meta, same grid; the
    /// variant's own selector carries 3 Eq matchers vs 1.
    #[test]
    fn i6_absent_labels_term_is_charged() {
        let sub_charges = |selector: &str| {
            let q = format!(
                r#"variants(absent_over_time({selector}[5m]), absent_over_time({selector}[5m])) of ({{app="x"}}[5m])"#
            );
            let (scan, variants, _) = variants_fixture(&q, variants_range_spec());
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st =
                VariantsAggState::new(&arena, &variants, &HashMap::new(), u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        let three = sub_charges(r#"{a="1", b="2", c="3"}"#);
        let one = sub_charges(r#"{a="1"}"#);
        let labels3: LabelSet = vec![
            ("a".into(), "1".into()),
            ("b".into(), "2".into()),
            ("c".into(), "3".into()),
        ];
        let labels1: LabelSet = vec![("a".into(), "1".into())];
        let expected = label_set_bytes(&labels3) - label_set_bytes(&labels1);
        assert!(expected > 0);
        assert_eq!(three - one, expected);
    }

    /// I7 — CHARGE: the `present_cover` term. Pair: same range grid, the
    /// variant selector carries NO Eq matcher (absent labels empty in
    /// both), op `absent_over_time` vs `count_over_time` — the single
    /// moving term is the grid array.
    #[test]
    fn i7_present_cover_term_is_charged() {
        let sub_charges = |op: &str| {
            let q = format!(
                r#"variants({op}({{app=~"x.*"}}[5m]), {op}({{app=~"x.*"}}[5m])) of ({{app="x"}}[5m])"#
            );
            let (scan, variants, _) = variants_fixture(&q, variants_range_spec());
            let cp = scan.client.expect("client scan");
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            let st =
                VariantsAggState::new(&arena, &variants, &HashMap::new(), u64::MAX).expect("state");
            st.charged_bytes() - arena.charged_bytes()
        };
        // Grid 0..60s step 60s ⇒ points {0, 60} ⇒ kmax = 1 ⇒ 8·(kmax+2).
        let expected = alloc_block_bytes(8 * 3);
        assert_eq!(
            sub_charges("absent_over_time") - sub_charges("count_over_time"),
            expected
        );
    }

    /// B10 / AC 17 — the arena shares, never recompiles: tail-free
    /// variants all share entry 0 (no entry charge — at N = 1 the whole
    /// arena charge is exactly 0, AC 14); identical tails share an entry;
    /// distinct tails add one each (pinned by I2's lengths too).
    #[test]
    fn arena_dedups_on_the_tail_slice_alone() {
        let arena_of = |q: &str| {
            let (scan, variants, _) = variants_fixture(q, variants_range_spec());
            let cp = scan.client.expect("client scan");
            let n = variants.len() as u64;
            let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
            (arena.len(), arena.charged_bytes(), n)
        };
        let (len, charged, _) =
            arena_of(r#"variants(count_over_time({app="x"}[5m])) of ({app="x"}[5m])"#);
        assert_eq!((len, charged), (1, 0), "AC 14: a 1-variant arena charges 0");
        let (len, charged, n) = arena_of(&n_variant_query(
            3,
            r#"count_over_time({app="x"}[5m])"#,
            r#"{app="x"}[5m]"#,
        ));
        assert_eq!(len, 1, "tail-free variants all share entry 0");
        assert_eq!(
            charged,
            variant_driver_buffer_bytes(n),
            "no entry term for shared tails"
        );
    }

    /// B6 — charge-before-allocate at the state boundary: a cap that
    /// admits the arena but not the first extra sub-state trips
    /// `VariantStateBytes` BEFORE that sub-state is constructed.
    #[test]
    fn b6_tiny_cap_trips_before_the_extra_sub_state_exists() {
        let (scan, variants, _) = variants_fixture(
            &n_variant_query(2, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
            variants_range_spec(),
        );
        let cp = scan.client.expect("client scan");
        let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
        // Cap == the arena's own charge: sub-state 0 is free, sub-state 1's
        // slot charge breaches.
        let err = VariantsAggState::new(&arena, &variants, &HashMap::new(), arena.charged_bytes())
            .expect_err("the extra sub-state must breach");
        match err {
            ReadError::QueryTooBroad(TooBroadReason::VariantStateBytes { bytes, cap }) => {
                assert!(bytes > cap);
            }
            other => panic!("expected VariantStateBytes, got {other:?}"),
        }
        // And the driver-buffer charge itself trips first under a
        // near-zero cap (B6b) — before any reservation.
        let err = VariantArena::build(&cp.pipeline, &variants, 1, 0)
            .expect_err("the driver buffers must breach");
        assert!(matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::VariantStateBytes { .. })
        ));
    }

    /// AC 24 + the D5.5 field-addition guard: exhaustive destructuring
    /// (no `..`), every container exactly reserved. Adding a field to
    /// either driver struct breaks this test's compilation, forcing the
    /// W-MEM walk to be re-run for it. Buckets: pipelines/slot/subs/
    /// sub_charged (C, charged via `variant_driver_buffer_bytes`),
    /// charged/base/cap (S), arena/variants (B).
    #[test]
    fn every_driver_container_field_is_accounted() {
        let (scan, variants, _) = variants_fixture(
            &n_variant_query(3, r#"count_over_time({app="x"}[5m])"#, r#"{app="x"}[5m]"#),
            variants_range_spec(),
        );
        let cp = scan.client.expect("client scan");
        let arena = VariantArena::build(&cp.pipeline, &variants, u64::MAX, 0).expect("build");
        {
            let mut st =
                VariantsAggState::new(&arena, &variants, &HashMap::new(), u64::MAX).expect("state");
            st.push_rows(&[]).expect("empty push");
            let VariantsAggState {
                arena: _b_arena,     // B
                variants: _b_specs,  // B
                subs,                // C — with_capacity(n)
                sub_charged,         // C — with_capacity(n)
                charged: _s_charged, // S
                base: _s_base,       // S
                cap: _s_cap,         // S
            } = st;
            assert_eq!(subs.capacity(), 3);
            assert_eq!(sub_charged.capacity(), 3);
        }
        let n = variants.len() as u64;
        let VariantArena {
            pipelines,        // C — with_capacity(n + 1)
            slot,             // C — with_capacity(n)
            charged: a_bytes, // S
        } = arena;
        assert_eq!(pipelines.capacity(), 4);
        assert_eq!(slot.capacity(), 3);
        assert!(a_bytes >= variant_driver_buffer_bytes(n));
    }

    /// B9 — `present_cover` costs nothing unless used: empty for a
    /// non-absent sliding state, grid-sized for an absent one.
    #[test]
    fn present_cover_is_allocated_only_for_absent_over_time() {
        let meta = slide_meta(1, r#"{"app":"x"}"#);
        let window = slide_window(
            0,
            50 * VSEC,
            10 * VSEC as u64 as i64 as u64,
            25 * VSEC as u64 as i64 as u64,
        );
        let mk = |op: RangeAggOp| ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: op,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&[]).unwrap();
        let count_client = mk(RangeAggOp::CountOverTime);
        let count = RangeSlideState::new(
            &compiled,
            &meta,
            &count_client,
            window,
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        assert!(count.present_cover.is_empty(), "non-absent pays no grid");
        assert_eq!(count.present_cover.capacity(), 0, "no allocation at all");
        let absent_client = mk(RangeAggOp::AbsentOverTime);
        let absent = RangeSlideState::new(
            &compiled,
            &meta,
            &absent_client,
            window,
            None,
            AggCaps::DEFAULT,
        )
        .unwrap();
        assert_eq!(absent.present_cover.len(), (absent.kmax + 2) as usize);
    }

    /// AC 51 census — `extended_with(` has exactly ONE production call
    /// site (the arena), and `VariantArena::build`'s body materializes no
    /// `common ++ tail` (no `.concat(`/`.to_vec()`/`chain(`): the dedup
    /// key is the tail slice alone. Production text = everything before
    /// the column-0 `#[cfg(test)]`, `//` comment text stripped (the
    /// `search_sql.rs` census precedent).
    #[test]
    fn variants_exec_census() {
        let src = include_str!("exec.rs");
        let production = src
            .split("\n#[cfg(test)]")
            .next()
            .expect("split")
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(
            production.matches("extended_with(").count(),
            1,
            "exactly one extended_with call (the arena)"
        );
        let build_body = {
            let start = production.find("    pub fn build(").expect("build fn");
            let tail = &production[start..];
            let end = tail.find("\n    }").expect("build end");
            &tail[..end]
        };
        for forbidden in [".concat(", ".to_vec()", "chain("] {
            assert!(
                !build_body.contains(forbidden),
                "VariantArena::build must not materialize common ++ tail ({forbidden})"
            );
        }
        // The exec-side charge funnel has exactly 3 call sites: the
        // driver buffers, the arena entry, the sub-state. Counted over a
        // WHITESPACE-STRIPPED copy so rustfmt line wrapping cannot move
        // the census; the raw token also matches
        // `discharge_fanout_bytes(&mut` as a substring, so the release
        // sites are subtracted.
        let compact: String = production.chars().filter(|c| !c.is_whitespace()).collect();
        let charges = compact.matches("charge_fanout_bytes(&mut").count()
            - compact.matches("discharge_fanout_bytes(&mut").count();
        assert_eq!(charges, 3, "exec charge-site census");
    }
}
