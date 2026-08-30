//! `LogQlEngine` — executes a [`super::plan::Plan`] against ClickHouse via
//! `ChClient`, injects the scan budget, maps overflow codes to
//! [`ReadError::QueryTooBroad`], and finishes vector aggregations in Rust
//! (docs/schemas.md §3.2: "the engine maps fingerprints to `service` and
//! finishes the `sum by`"). Deliberately **not** snapshot-tested — SQL
//! generation itself is `plan`/`sql`'s job and is tested there without a
//! database; this module's own test coverage is the error-mapping unit
//! tests (architect plan amendment §4).

use super::detected::{
    self, DetectedFieldValues, DetectedFields, DetectedLabelOut, FieldAccumulator,
};
use super::error::{ReadError, TooBroadReason};
use super::explain::PlanExplain;
use super::params::{Direction, PlanCtx, QueryParams, QuerySpec, ResponseOptions, TimeBounds};
use super::pipeline::CompiledPipeline;
use super::plan::{self, ClientAgg, MetricNode, MetricPlan, Plan, StreamsPlan};
use super::predicate::CheckedLiteral;
use super::rows::{
    DetectedLabelRow, LabelNameRow, LabelValueRow, LogStatsRow, MetricInstantRow, MetricScanRow,
    PatternFetchRow, SampleRow, StreamMetaRow, StreamRow, TailSampleRow, VolumeRow,
};
use futures::Stream;
use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, ChRow, ChRowStream, QuerySettings};
use pulsus_logql::{Expr, LogExpr, MatchOp, Matcher, RangeAggOp, Stage, StreamSelector};

use super::charge::{
    MAX_STREAMS_RESULT_BYTES, PUSHDOWN_INSTANT_SLOT, StreamsResultBudget, charge_group_bytes,
    group_entry_bytes,
};
use super::client_agg::check_surviving_error;
use super::labels::render_series_labels;
use super::sql::ScanProjection;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::agg::{InstantSeries, LabelSet};
use super::charge::{AggCaps, ensure_result_series};
use super::client_agg::{
    CLIENT_AGG_CHUNK_ROWS, ClientAggState, MetricAggState, RangeSlideState, run_client_agg_rows,
};
use super::detected_probe::{
    DetectedPagedState, DetectedRowFeeder, FanOutGroup, GroupKey, LabelScratch,
    SmFanOutAccumulator, TailCursorTracker, classify_page_error, eval_structured_metadata_row,
    push_fanout_entry, recycle_label_scratch, split_categories, split_merged_categories,
};

use super::labels::{
    EMPTY_STRUCTURED_METADATA, StructuredMetadataCtx, fnv1a64,
    merge_labels_with_structured_metadata, parse_flat_labels, render_labels_json_sorted,
    series_labels,
};
use super::post_agg::{
    MAX_POST_AGG_BYTES, apply_label_replace, apply_vector_aggs, charged_instant_chain,
    combine_binary,
};
use super::variants::{MAX_VARIANT_FANOUT_STATE_BYTES, VariantArena, VariantsAggState};
use super::warnings::Warnings;
use super::window::{ClientWindow, materialize_vector_lit};

/// ClickHouse server exception code for `TOO_MANY_BYTES` — the
/// `max_bytes_to_read` overflow this module sets from
/// `reader.logql_scan_budget_bytes`. One of exactly **two** server codes
/// [`map_read_error`] maps to [`ReadError::QueryTooBroad`] (issue #398
/// added the second, [`CODE_MEMORY_LIMIT_EXCEEDED`]; before it, this was
/// the only one). `max_rows_to_read` is never set on **LogQL** read paths
/// (the traces scan budget sets it deliberately on its generator queries,
/// where code 158 maps to `TooBroadReason::TraceScanBudgetRows` via
/// `traces::exec`'s own mapper — issue #57), so on the LogQL path code
/// 158 (`TOO_MANY_ROWS`) can never masquerade as the byte budget
/// (architect plan amendment §4).
const CODE_TOO_MANY_BYTES: i32 = 307;

/// ClickHouse server exception code for `MEMORY_LIMIT_EXCEEDED` — the
/// `max_memory_usage` overflow issue #398 sets on **every** LogQL read
/// from `reader.logql_read_max_memory_bytes`. Before #398 no LogQL read
/// set a memory limit at all, so this code fell through to
/// [`ReadError::Clickhouse`] and the client saw a `500` carrying the raw
/// server exception — the same "we could not afford this query" condition
/// the byte budget already reported as `422`.
const CODE_MEMORY_LIMIT_EXCEEDED: i32 = 241;

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
    /// Issue #398: `reader.logql_read_max_memory_bytes` — the
    /// `max_memory_usage` ceiling (throw-not-spill) every read this engine
    /// issues carries, applied in [`read_query_settings`] so no dispatch
    /// site can be missed. Breach → server code 241 →
    /// [`TooBroadReason::LogqlReadMemory`] → `422`.
    pub read_max_memory_bytes: u64,
    pub max_streams: usize,
    /// `reader.logql_pipeline_scan_factor` (issue M6-09) — see
    /// [`PlanCtx::pipeline_scan_factor`].
    pub pipeline_scan_factor: u32,
    /// Clustered mode (`PULSUS_CLUSTER` set). Gates
    /// `distributed_product_mode='local'` on the three label-discovery
    /// queries that carry an `IN (subquery)` after issue #399; mirrors
    /// `MetricsConfig::distributed` and `TraceReadConfig::distributed`.
    /// See [`LogQlEngine::activity_settings`].
    pub distributed: bool,
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
    /// With `categorize_labels` (issue #463) this holds the
    /// STREAM-category labels only; without it, the full final label set
    /// exactly as before.
    pub labels_json: String,
    /// `(timestamp_ns, body)`, in the plan's requested direction.
    pub entries: Vec<(i64, String)>,
    /// Parallel to [`Self::entries`] when non-empty (issue #463).
    ///
    /// **Empty on every non-categorised response**, which is why that
    /// path allocates nothing new per entry and stays byte-identical: a
    /// `StreamResult` grows one empty `Vec` per STREAM (24 B), never per
    /// entry.
    pub categories: Vec<EntryCategories>,
}

/// One entry's non-stream labels (issue #463), split the way
/// `LabelsBuilder.LabelsResult` splits them
/// (`pkg/logql/log/labels.go:606-626 @ grafana/loki v3.7.4 b318f282`).
/// Both vectors are sorted by key and neither may contain a name the
/// other does.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EntryCategories {
    /// Rendered under the entry's `structuredMetadata` key.
    pub structured_metadata: Vec<(String, String)>,
    /// Rendered under the entry's `parsed` key. Includes `__error__` /
    /// `__error_details__` — the reference files both under `parsed`
    /// unconditionally (`labels.go:610-614`), including when they
    /// arrived as structured metadata.
    pub parsed: Vec<(String, String)>,
}

/// Which `values` element shape a stream can serve (issue #463). The
/// categorised wire shape is all-or-nothing: a three-element entry in a
/// body that does not advertise `categorize-labels`, or a two-element
/// entry in one that does, is a hard parse failure in the datasource's
/// streaming decoder — so the decision is taken once, from the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireArity {
    Two,
    Three,
}

impl StreamResult {
    /// `Three` iff [`Self::categories`] is exactly parallel to
    /// [`Self::entries`].
    ///
    /// A stream whose vectors disagree reports `Two`, so a construction
    /// bug DOWNGRADES the whole response to the two-element shape instead
    /// of emitting a body no client can parse.
    pub fn wire_arity(&self) -> WireArity {
        if !self.entries.is_empty() && self.categories.len() == self.entries.len() {
            WireArity::Three
        } else {
            WireArity::Two
        }
    }
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

/// Which statement [`LogQlEngine::run_discovery`] renders at stage 2, and
/// under which `X-Pulsus-Explain` stage name. This is the ONLY thing a
/// caller says about the stage-2 SQL. There is no parameter, closure
/// argument or return value through which a caller receives a fingerprint
/// list, so a caller has no expression in which to ignore, replace or
/// fabricate one (issue #482).
#[derive(Debug)]
enum DiscoveryQuery<'a> {
    /// `/labels` -> [`super::sql::label_names`].
    LabelNames,
    /// `/label/{name}/values` -> [`super::sql::label_values`]. `name` is
    /// minted into a `CheckedLiteral` by `super::predicate::literal`
    /// INSIDE the helper; the caller passes the raw name.
    LabelValues { name: &'a str },
    /// `/detected_labels` -> [`super::sql::detected_labels`].
    DetectedLabels,
}

impl DiscoveryQuery<'_> {
    /// The stage-2 explain stage name — the three literals those three
    /// paths pushed before issue #482 folded them into one helper.
    fn stage_name(&self) -> &'static str {
        match self {
            DiscoveryQuery::LabelNames => "label_names",
            DiscoveryQuery::LabelValues { .. } => "label_values",
            DiscoveryQuery::DetectedLabels => "detected_labels",
        }
    }
}

pub struct LogQlEngine {
    client: ChClient,
    config: EngineConfig,
}

impl LogQlEngine {
    pub fn new(client: ChClient, config: EngineConfig) -> Self {
        Self { client, config }
    }

    /// Returns the result alongside the [`Warnings`] the evaluation
    /// accumulated (issue #277) — empty for every query but a
    /// `variants(...)` one that skipped a variant, and an empty
    /// accumulator renders zero bytes on the wire.
    pub async fn query(
        &self,
        expr: &Expr,
        params: &QueryParams,
    ) -> Result<(QueryResult, Warnings), ReadError> {
        self.query_with(expr, params, ResponseOptions::default())
            .await
    }

    /// [`Self::query`] with the response-shape options the request asked
    /// for (issue #463). `ResponseOptions::default()` is the pre-#463
    /// behaviour exactly.
    pub async fn query_with(
        &self,
        expr: &Expr,
        params: &QueryParams,
        opts: ResponseOptions,
    ) -> Result<(QueryResult, Warnings), ReadError> {
        let ctx = self.config.plan_ctx();
        let mut warnings = Warnings::new();
        match plan::plan(expr, params, &ctx)? {
            Plan::Streams(sp) => self
                .run_streams_inner(&sp, None, opts)
                .await
                .map(|(items, partial)| (QueryResult::Streams { items, partial }, warnings)),
            // Issue #236: [`MAX_QUERY_SERIES`] is a FINAL-RESULT cap, so it
            // is applied here — on the whole expression's output — and
            // never inside `run_metric_inner`/`run_metric_node`/
            // `apply_vector_aggs`, where it would reject on scanned or
            // intermediate groups the reference never counts.
            Plan::Metric(mp) => {
                let result = self.run_metric_inner(&mp, None).await?;
                ensure_result_series(&result)?;
                Ok((result, warnings))
            }
            Plan::MetricBinary(node) => {
                let result = self.run_metric_node(&node, None, &mut warnings).await?;
                if final_series_gate_applies(&node) {
                    ensure_result_series(&result)?;
                }
                Ok((result, warnings))
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
    ) -> Result<(QueryResult, Warnings, PlanExplain), ReadError> {
        self.query_explained_with(expr, params, ResponseOptions::default())
            .await
    }

    /// [`Self::query_explained`] with the response-shape options the
    /// request asked for (issue #463).
    pub async fn query_explained_with(
        &self,
        expr: &Expr,
        params: &QueryParams,
        opts: ResponseOptions,
    ) -> Result<(QueryResult, Warnings, PlanExplain), ReadError> {
        let ctx = self.config.plan_ctx();
        let mut warnings = Warnings::new();
        match plan::plan(expr, params, &ctx)? {
            Plan::Streams(sp) => {
                let mut explain = PlanExplain::new("streams");
                let (items, partial) = self
                    .run_streams_inner(&sp, Some(&mut explain), opts)
                    .await?;
                Ok((QueryResult::Streams { items, partial }, warnings, explain))
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
                Ok((result, warnings, explain))
            }
            Plan::MetricBinary(node) => {
                let mut explain = PlanExplain::new(binary_result_type(&node, params));
                let result = self
                    .run_metric_node(&node, Some(&mut explain), &mut warnings)
                    .await?;
                if final_series_gate_applies(&node) {
                    ensure_result_series(&result)?;
                }
                Ok((result, warnings, explain))
            }
        }
    }

    /// Labels discovery (#13 `GET|POST /api/logs/v1/labels`): distinct
    /// `log_streams_idx` keys within `b`'s months. Budget-capped like
    /// every other index scan in this module.
    ///
    /// **Issue #482:** `selector` is `query=`'s matchers-only narrowing —
    /// `None` is the unscoped form (byte-identical to what this endpoint
    /// answered before), `Some` resolves the selector to a fingerprint
    /// set first and reads only those streams' keys.
    pub async fn label_names(
        &self,
        selector: Option<&Expr>,
        b: TimeBounds,
    ) -> Result<Vec<String>, ReadError> {
        self.label_names_inner(selector, b, None).await
    }

    /// [`LogQlEngine::label_names`] plus its `X-Pulsus-Explain` trace, in
    /// the same single pass (no second scan).
    pub async fn label_names_explained(
        &self,
        selector: Option<&Expr>,
        b: TimeBounds,
    ) -> Result<(Vec<String>, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("labels");
        let names = self
            .label_names_inner(selector, b, Some(&mut explain))
            .await?;
        Ok((names, explain))
    }

    async fn label_names_inner(
        &self,
        selector: Option<&Expr>,
        b: TimeBounds,
        explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<String>, ReadError> {
        self.run_discovery::<LabelNameRow, String>(
            DiscoveryQuery::LabelNames,
            selector,
            b,
            explain,
            "labels requires a log stream selector (matchers only)",
            |r| Some(r.name),
        )
        .await
    }

    /// Label-values discovery (#13 `GET /api/logs/v1/label/{name}/values`):
    /// distinct values of `name` within `b`'s months.
    ///
    /// **Issue #482:** `selector` narrows exactly as it does for
    /// [`LogQlEngine::label_names`] — the key filter is applied on top of
    /// the narrowed stream set, so a key that exists on some other stream
    /// contributes nothing.
    pub async fn label_values(
        &self,
        name: &str,
        selector: Option<&Expr>,
        b: TimeBounds,
    ) -> Result<Vec<String>, ReadError> {
        self.label_values_inner(name, selector, b, None).await
    }

    /// [`LogQlEngine::label_values`] plus its `X-Pulsus-Explain` trace, in
    /// the same single pass (no second scan).
    pub async fn label_values_explained(
        &self,
        name: &str,
        selector: Option<&Expr>,
        b: TimeBounds,
    ) -> Result<(Vec<String>, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("label_values");
        let values = self
            .label_values_inner(name, selector, b, Some(&mut explain))
            .await?;
        Ok((values, explain))
    }

    async fn label_values_inner(
        &self,
        name: &str,
        selector: Option<&Expr>,
        b: TimeBounds,
        explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<String>, ReadError> {
        self.run_discovery::<LabelValueRow, String>(
            DiscoveryQuery::LabelValues { name },
            selector,
            b,
            explain,
            "label values requires a log stream selector (matchers only)",
            |r| Some(r.value),
        )
        .await
    }

    /// Runs a matchers-only discovery request end to end — the ONE
    /// stage-1 block for `/labels`, `/label/{name}/values` and
    /// `/detected_labels` (issue #482).
    ///
    /// **The no-stream case is owned here.** When `selector` is `Some`
    /// and stage 1 resolves to nothing, this returns `Ok(Vec::new())`
    /// from inside the helper, before any stage-2 statement is built or
    /// issued. The fingerprint list is a local of this function and is
    /// passed only to the [`super::sql`] builder selected by `query`.
    ///
    /// **No caller receives a fingerprint list in any form** — not as a
    /// parameter, not as a closure argument, not as a return value — so a
    /// caller has no expression in which to ignore, replace or fabricate
    /// one. Because the empty check precedes the build, the
    /// `Option<&[u64]>` reaching [`super::sql::active_fingerprints`] from
    /// these three paths is `None` or a NON-EMPTY slice, never
    /// `Some(&[])`.
    ///
    /// Two things it does not prevent, stated rather than implied:
    /// `R` is not bound to the [`DiscoveryQuery`] variant (a mismatch is
    /// a row-decode error, not a wrong answer — the projections have
    /// disjoint column names), and a future fourth discovery path could
    /// bypass the helper and render a builder itself.
    ///
    /// `metric_rejection` is the endpoint's own `PipelineInvalid` reason:
    /// unreachable through the API layer (all three parse `query` with
    /// `parse_selector`), kept as a structured rejection, never a panic.
    async fn run_discovery<R: ChRow, T>(
        &self,
        query: DiscoveryQuery<'_>,
        selector: Option<&Expr>,
        b: TimeBounds,
        mut explain: Option<&mut PlanExplain>,
        metric_rejection: &str,
        mut keep_row: impl FnMut(R) -> Option<T>,
    ) -> Result<Vec<T>, ReadError> {
        // Always >= 1 literal (`months_overlapping` never returns empty),
        // so the stage-2 month IN-list has no empty-IN hazard.
        let months = plan::months_overlapping(b.start_ns, b.end_ns);
        let fingerprints: Option<Vec<u64>> = match selector {
            None => None,
            Some(expr) => {
                let ctx = self.config.plan_ctx();
                // `limit`/`direction`/`step` are unused placeholders —
                // none of these three endpoints reads samples (the stats
                // idiom).
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
                            reason: metric_rejection.to_string(),
                        });
                    }
                };
                if let Some(e) = explain.as_mut() {
                    e.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
                }
                let fps = self.resolve_fingerprints(&sp.stage1_sql).await?;
                if fps.is_empty() {
                    // No matching streams — return before stage 2 is
                    // built, so no statement is issued and no empty
                    // fingerprint IN-list can render.
                    return Ok(Vec::new());
                }
                Some(fps)
            }
        };
        let fps = fingerprints.as_deref();
        let window = self.activity_window(b);
        let sql = match query {
            DiscoveryQuery::LabelNames => super::sql::label_names(
                &self.config.streams_idx,
                &months,
                fps,
                &self.config.rollup_table,
                window,
                self.config.rollup_res_ns,
            ),
            DiscoveryQuery::LabelValues { name } => super::sql::label_values(
                &self.config.streams_idx,
                &months,
                &super::predicate::literal(name),
                fps,
                &self.config.rollup_table,
                window,
                self.config.rollup_res_ns,
            ),
            DiscoveryQuery::DetectedLabels => super::sql::detected_labels(
                &self.config.streams_idx,
                &months,
                fps,
                &self.config.rollup_table,
                window,
                self.config.rollup_res_ns,
            ),
        };
        if let Some(e) = explain.as_mut() {
            e.push(query.stage_name(), sql.clone(), None);
        }
        let mut out = Vec::new();
        let mut stream = self
            .query_stream::<R>(&sql, &self.activity_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })?;
            if let Some(kept) = keep_row(row) {
                out.push(kept);
            }
        }
        Ok(out)
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
    ///
    /// **An EMPTY `selectors` is legal and means "every series active in
    /// the window"** (issue #406 Part A): the reference's
    /// `MatchForSeriesRequest(nil)` returns no error
    /// (`pkg/logql/matchers.go:13-26` @ grafana/loki v3.7.4 `b318f282`),
    /// so `/series` with no `match[]` — and `/series?match[]={}` — is a
    /// `200` over every series there. That branch skips stage 1 entirely
    /// and reads [`LogQlEngine::all_active_fingerprints`], bounded by the
    /// same `max_streams` cap and the same scan budget as the matched
    /// path, over two statements instead of three.
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
        // Issue #399: stage 1 is month-scoped (`sql::stage1`, shared with
        // every other LogQL path — see `LogQlEngine::active_fingerprints`
        // for why it must stay that way), so the request's own window is
        // applied as its own statement.
        let window = self.activity_window(b);
        if selectors.is_empty() {
            // Issue #406 Part A: no `match[]` (or a lone `{}`) means
            // "every series active in the window". There is no selector to
            // resolve, so there is NO `stage1_stream_resolution` here —
            // the activity scan is the whole first stage, exactly as
            // `/labels` and `/detected_labels` already render it.
            if let Some(e) = explain.as_mut() {
                e.push(
                    "series_activity_filter",
                    super::sql::active_fingerprints(
                        &self.config.rollup_table,
                        None,
                        window,
                        self.config.rollup_res_ns,
                    ),
                    None,
                );
            }
            // Capped inside the streaming loop, so `check_stream_cap` must
            // NOT be repeated on the result here.
            fingerprints = self.all_active_fingerprints(window).await?;
        } else {
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
            // Each selector's own `resolve_fingerprints` call already caps
            // that *individual* selector at `max_streams`
            // (`check_stream_cap` inside it), but says nothing about the
            // deduped union across selectors — N disjoint `match[]` values
            // can each stay under the cap individually while their union
            // blows well past it, building an oversized stage-2
            // `fingerprint IN (...)` hydration query (round-1 code review
            // finding 1). Re-check the cap on the union before proceeding.
            check_stream_cap(fingerprints.len(), self.config.max_streams)?;
            if fingerprints.is_empty() {
                return Ok(Vec::new());
            }
            // Ordering is load-bearing in both directions: filtering first
            // would silently relax `max_streams` to the post-window
            // survivors
            // (`series_stream_cap.rs::series_cap_counts_the_pre_window_union`).
            if let Some(e) = explain.as_mut() {
                e.push(
                    "series_activity_filter",
                    super::sql::active_fingerprints(
                        &self.config.rollup_table,
                        Some(&fingerprints),
                        window,
                        self.config.rollup_res_ns,
                    ),
                    None,
                );
            }
            fingerprints = self.active_fingerprints(&fingerprints, window).await?;
        }
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
            .map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })
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
            let row = row.map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })?;
            fingerprints.push(row.fingerprint);
            check_stream_cap(fingerprints.len(), self.config.max_streams)?;
        }
        Ok(fingerprints)
    }

    /// Every fingerprint ACTIVE in `window`, with no selector to narrow it
    /// — the unscoped arm of [`super::sql::active_fingerprints`], which
    /// `label_names`/`label_values`/`detected_labels` already render today
    /// (issue #399). Issue #406 Part A: this is what `/series` with no
    /// `match[]` resolves against.
    ///
    /// Caps INSIDE the streaming loop, exactly as
    /// [`LogQlEngine::resolve_fingerprints`] does, so an oversized store is
    /// refused after `max_streams + 1` rows rather than after all of them —
    /// and so the caller must not re-check the cap on the result.
    ///
    /// Sorted before returning so the stage-2 `fingerprint IN (...)` text
    /// (and therefore the explain trace) is deterministic, matching the
    /// matched path's sorted union. `DISTINCT fingerprint` already makes
    /// the rows unique; the `dedup` is a cheap restatement of that
    /// invariant, not a second pass over duplicates.
    async fn all_active_fingerprints(
        &self,
        window: super::sql::TimeWindow,
    ) -> Result<Vec<u64>, ReadError> {
        let sql = super::sql::active_fingerprints(
            &self.config.rollup_table,
            None,
            window,
            self.config.rollup_res_ns,
        );
        let mut fingerprints: Vec<u64> = Vec::new();
        // Scoped so the pooled connection's lease drops before the caller
        // issues stage 2 (the `ChRowStream` lease contract).
        {
            let mut stream = self
                .query_stream::<StreamRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                fingerprints.push(row.fingerprint);
                check_stream_cap(fingerprints.len(), self.config.max_streams)?;
            }
        }
        fingerprints.sort_unstable();
        fingerprints.dedup();
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
            let row = row.map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })?;
            // ReplacingMergeTree without FINAL may yield duplicate rows per
            // fingerprint; labels/service are identical per fingerprint, so
            // keeping any one row is safe (docs/schemas.md §3.2 edge cases).
            out.entry(row.fingerprint).or_insert(row);
        }
        Ok(out)
    }

    fn budget_settings(&self) -> QuerySettings {
        read_query_settings(
            self.config.scan_budget_bytes,
            self.config.read_max_memory_bytes,
        )
    }

    /// The request's own bounds, as the activity semi-join's window
    /// (issue #399). A plain re-wrap: the discovery endpoints take client
    /// bounds verbatim and never widen them, so there is nothing to
    /// derive — the ONE quantization is
    /// [`super::sql::activity_lower_bucket_ns`], applied inside the
    /// builder where the rollup resolution lives.
    fn activity_window(&self, b: TimeBounds) -> super::sql::TimeWindow {
        super::sql::TimeWindow {
            start_ns: b.start_ns,
            end_ns: b.end_ns,
        }
    }

    /// [`LogQlEngine::budget_settings`] plus, when clustered,
    /// `distributed_product_mode='local'` — the settings the three
    /// label-discovery scans carrying issue #399's activity semi-join
    /// dispatch with.
    ///
    /// `FROM log_streams_idx_dist … WHERE fingerprint IN (SELECT … FROM
    /// log_metrics_5s_dist …)` is a double-distributed `IN`, rejected at
    /// analysis time under ClickHouse's default `deny` (Code 288,
    /// `DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED`) — deterministic 500s on a
    /// clustered deployment. `local` is EXACT here, not merely permissive:
    /// both tables are Logs-family and shard on `fingerprint`
    /// (docs/schemas.md §7), so a stream's index rows and its rollup rows
    /// are always on the same shard and shard-local `IN` decides
    /// identically to global `IN` — the `metrics::exec::
    /// fallback_fetch_settings` (issue #136) and `traces::exec::
    /// metrics_settings` (issue #59) precedent.
    ///
    /// Applied ONLY to these three dispatches. Stage-1 resolution, stage-2
    /// hydration and `/series`' own activity query keep
    /// [`LogQlEngine::budget_settings`]: none of them nests a distributed
    /// table, and a blanket client-wide default would let a future
    /// non-co-sharded subquery silently return wrong shard-local results
    /// instead of failing loud.
    fn activity_settings(&self) -> QuerySettings {
        activity_query_settings(
            self.config.scan_budget_bytes,
            self.config.read_max_memory_bytes,
            self.config.distributed,
        )
    }

    /// Narrows a resolved fingerprint set to those with log lines in
    /// `window` (issue #399). `/series` is the one endpoint of the four
    /// that cannot embed [`super::sql::active_fingerprints`] as a
    /// subquery: its month-scoped scan is [`super::sql::stage1`], shared
    /// with every other LogQL path, where the predicate would be
    /// redundant (those paths bound the window on their own
    /// sample/rollup/patterns read) and a straight read-path regression.
    ///
    /// Runs AFTER `check_stream_cap` so the cap keeps its current meaning
    /// — the deduped pre-window union — and the cap test keeps failing for
    /// the reason it was written for (`series_stream_cap.rs`). PK-pruned:
    /// the caller's list is the `(fingerprint, bucket_ns)` prefix
    /// (measured, issue #399: 73,728 rows vs 29,080,654 unscoped on the
    /// cost fixture). Dispatched with [`LogQlEngine::budget_settings`],
    /// deliberately NOT [`LogQlEngine::activity_settings`] — there is no
    /// nested distributed table here, so widening the shard-locality
    /// assumption to it would be unearned.
    ///
    /// Returns the caller's order filtered by membership, so
    /// `series_inner`'s sort/dedup contract is untouched.
    async fn active_fingerprints(
        &self,
        fingerprints: &[u64],
        window: super::sql::TimeWindow,
    ) -> Result<Vec<u64>, ReadError> {
        let sql = super::sql::active_fingerprints(
            &self.config.rollup_table,
            Some(fingerprints),
            window,
            self.config.rollup_res_ns,
        );
        let mut active: BTreeSet<u64> = BTreeSet::new();
        // Scoped so the pooled connection's lease drops before the caller
        // issues stage 2 (the `ChRowStream` lease contract).
        {
            let mut stream = self
                .query_stream::<StreamRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                active.insert(row.fingerprint);
            }
        }
        Ok(fingerprints
            .iter()
            .copied()
            .filter(|fp| active.contains(fp))
            .collect())
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
    ///
    /// Issue #398: `remaining` is the DECREMENTED byte budget and lands in
    /// `max_bytes_to_read` only. The memory ceiling is per query and is
    /// never decremented — every page carries the same
    /// `reader.logql_read_max_memory_bytes`.
    pub fn paging_settings(&self, remaining: u64) -> QuerySettings {
        read_query_settings(remaining, self.config.read_max_memory_bytes)
            .set("wait_end_of_query", 1)
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
        opts: ResponseOptions,
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
            // Zero-structured-metadata rows stay on the by-fingerprint
            // path (AC-8 byte-identity); rows carrying structured
            // metadata (issue #97) fan out into their own
            // merged-label-set streams.
            //
            // **Streamed, not staged (issue #312).** There is no
            // `sm_rows` vector: an SM row fans out as it arrives, so its
            // body is live once — charged — rather than twice. A row
            // whose fingerprint is absent from `meta` is skipped here
            // rather than at drain, which is byte-identical (the
            // pre-#312 `filter_map` dropped exactly those) and is what
            // makes the charge equal the returned footprint exactly.
            let mut budget = StreamsResultBudget::new();
            let mut groups = FastPathGroups::new_with(opts.categorize_labels);
            let mut stream = self
                .query_stream::<SampleRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                groups.push_row(row, &meta, &mut budget)?;
            }
            return Ok((groups.into_streams(), false));
        }

        // Dropping sub-case (issue #90): a label filter, or a line filter
        // after `line_format`, drops lines in-engine — a single oversampled
        // `LIMIT` scan could under-return. Keyset-page until the limit
        // fills, the window exhausts, or the budget is spent.
        if sp.fetch_until_limit {
            return self
                .run_streams_paged(sp, &compiled, &meta, &services, &fingerprints, opts)
                .await;
        }

        // Non-dropping transform/fan-out path: rows are consumed in
        // arrival order (stage 3 orders globally by timestamp in the
        // requested direction, so arrival order IS the response order —
        // the global `result_limit` truncation depends on it). A single
        // `stage3` `LIMIT = result_limit` scan, byte-identical to today.
        //
        // **Chunked, not staged (issue #312).** The whole-response
        // `Vec<SampleRow>` this used to build was bounded only by the SQL
        // LIMIT — 5 000 rows x the 64 MiB ingest cap = 320 GiB. Rows now
        // flow through the accumulator's byte-denominated chunk, charged
        // on push and released on flush.
        let mut acc = StreamAccumulator::with_opts(&meta, sp.result_limit, opts);
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule).
            let mut stream = self
                .query_stream::<SampleRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                acc.push_row(row, &compiled)?;
            }
        }
        acc.flush_chunk(&compiled)?;

        Ok((acc.into_streams(), false))
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
    /// ([`TailCursorTracker`] over the raw page, not survivors — so a page
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
    ///
    /// **Streaming (issue #312), and refusal versus partial.** Each page's
    /// rows are charged and chunked through the accumulator as they
    /// arrive — no page-sized `Vec<TailSampleRow>` and no `SampleRow`
    /// re-materialisation into a second page-sized vector; the two used to
    /// hold 50 000 rows x the 64 MiB ingest cap = 3.1 TiB. A
    /// `ScanBudgetBytes` breach mid-page therefore returns the survivors
    /// INCLUDING that page's prefix, which is #244's ruled precedent (the
    /// prefix boundary is not required to align with a page boundary) and
    /// is more complete, never less. The scan-budget partial is decided at
    /// the top of the loop and inside `absorb_page`; the RESULT-budget
    /// refusal is decided inside `push_row`. A query near both boundaries
    /// resolves to whichever trips first, which is page-timing dependent —
    /// but complete-or-error wins where the ledger trips: a
    /// [`TooBroadReason::StreamsResultBytes`] refusal is never downgraded
    /// to a partial.
    async fn run_streams_paged(
        &self,
        sp: &StreamsPlan,
        compiled: &super::pipeline::CompiledPipeline,
        meta: &HashMap<u64, StreamMetaRow>,
        services: &[CheckedLiteral],
        fingerprints: &[u64],
        opts: ResponseOptions,
    ) -> Result<(Vec<StreamResult>, bool), ReadError> {
        let budget = self.config.scan_budget_bytes;
        let window = super::sql::TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        };
        let mut acc = StreamAccumulator::with_opts(meta, sp.result_limit, opts);
        let mut st = StreamsPagedState {
            cursor: None,
            spent: 0,
            // First-page size = the oversample hint; subsequent pages
            // reuse it.
            page_size: sp.scan_limit.max(1),
            budget,
        };

        loop {
            // Terminate before issuing: `max_bytes_to_read = 0` is
            // ClickHouse's *unlimited* sentinel, so a zero cap must never be
            // issued. Once the budget is spent, return the survivors so far
            // as a partial result (a later page's positive-cap overflow is
            // handled below; the first-page `spent == 0` case never reaches
            // here). This makes `page_cap` always > 0.
            if scan_budget_spent(st.spent, budget) {
                return Ok((acc.into_streams(), true));
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
            // stream's pooled-connection lease releases before the next
            // page.
            //
            // Issue #35: `query_stream` returns `Result<_, ReadError>`
            // directly (already mapped through `map_read_error` for a
            // dispatch-time failure); per-row errors are raw `ChError`
            // from `ChRowStream::next()` and are mapped inside
            // `absorb_page` with the SAME `map_read_error(_, budget)` —
            // so both routes reach `classify_page_error` as an
            // already-mapped `ReadError`, preserving the
            // first-page-vs-later-page branch split unchanged.
            let decision = {
                let mut stream = match self
                    .query_stream::<TailSampleRow>(&sql, &self.paging_settings(page_cap))
                    .await
                {
                    Ok(stream) => stream,
                    Err(mapped) => {
                        let partial = classify_page_error(mapped, st.spent)?;
                        return Ok((acc.into_streams(), partial));
                    }
                };
                st.absorb_page(
                    &mut stream,
                    |s| s.read_bytes(),
                    |e| map_read_error(e, budget, self.config.read_max_memory_bytes),
                    &mut acc,
                    compiled,
                )
                .await
            };
            if let Some(terminal) = decision? {
                return Ok((acc.into_streams(), terminal));
            }
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
        let is_instant = mp.step_ns.is_none();
        // Issue #343: the offset shift left the representable timestamp
        // axis, so the query answers empty. Returned BEFORE
        // `resolve_fingerprints` — zero ClickHouse round trips, where the
        // saturating predecessor issued a domain-wide scan from 1977 — and
        // BEFORE the `absent_over_time` zero-fingerprint arm below, whose
        // synthetic `1` would otherwise be reported for a window that does
        // not exist. The pipeline is compiled above this, so a bad regex
        // stays a 400 either way. The plan's bounds are ALSO degenerate
        // (`end_ns < grid_start_ns`), so this is the round-trip saving,
        // not the correctness mechanism.
        if mp.empty_domain {
            return Ok(if is_instant {
                QueryResult::Vector(Vec::new())
            } else {
                QueryResult::Matrix(Vec::new())
            });
        }
        let fingerprints = self.resolve_fingerprints(&mp.stage1_sql).await?;
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
        let source = metric_source(mp);

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
                // Issue #249: this arm runs only when `is_instant`, and an
                // instant plan is always `RouteChoice::Raw` over
                // `log_samples` (the rollup decision requires
                // `QuerySpec::Range`) — so the metadata column always
                // exists here, and the merge needs it.
                ScanProjection::WithStructuredMetadata,
            );
            if let Some(e) = explain.as_mut() {
                e.push("metric_read", sql.clone(), Some(mp.routing.reason.clone()));
            }
            let mut stream = self
                .query_stream::<MetricInstantRow>(&sql, &self.budget_settings())
                .await?;
            // Issue #249: the server groups by `(fingerprint,
            // structured_metadata)`, so one fingerprint can return N rows.
            // The client merges each row's metadata into that stream's label
            // set and RE-groups by the merged final set, summing `n` BEFORE
            // `apply_rate` — exact, because every op that can reach this
            // path is a linear sum (`count()` / `sum(length(body))`).
            let mut groups = PushdownInstantGroups::new(&meta, AggCaps::DEFAULT);
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                groups.push_row(&row)?;
            }
            let series = groups.finish(mp.rate_window_ns);
            let series = charged_instant_chain(series, &mp.vector_aggs, MAX_POST_AGG_BYTES)?;
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
            // **Structurally unreachable, and removed rather than charged
            // (issues #241 / #257).** Reaching here needs `client.is_none()`
            // (the arm above returned otherwise) AND `step_ns.is_some()`.
            // `metric_plan` forces `client = Some(..)` for every
            // `QuerySpec::Range` (`plan.rs`'s `metric_plan`, the `|| is_range`
            // disjunct) and derives `step_ns = None` for exactly
            // `QuerySpec::Instant` (the same function's `match p.spec` binding
            // `step_ns`; both `Range` arms yield `Some(step)`), so
            // the conjunction is unsatisfiable — and it already was on the day
            // #257 was filed against the SQL-aggregated range arm that stood
            // here (`git show 8d1f4519:…/exec.rs`, lines 1004-1021).
            //
            // #257's remedy is therefore removal: there is nothing to charge
            // because the code cannot run. `sql::metric_range` is untouched —
            // it is `pub`, `docs/schemas.md` §3.2 documents its shape, and
            // `tests/rollup_differential.rs` drives it live.
            //
            // A refusal rather than a deletion because the planner could
            // reintroduce the state; the tripwire that would catch that
            // BEFORE a request does is
            // `logql_plan_build_differential.rs::
            // every_planned_range_leaf_is_client_aggregated`.
            Err(ReadError::PipelineInvalid {
                reason: "internal: a range metric plan reached the SQL-aggregated path; every \
                         QuerySpec::Range forces client aggregation (plan.rs)"
                    .to_string(),
            })
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
        // Issue #398: the hard-coded 8 GiB `max_memory_usage` override
        // that used to sit here is gone — every
        // LogQL read now carries the ceiling from
        // `reader.logql_read_max_memory_bytes` via `read_query_settings`,
        // whose default is that same 8 GiB. Keeping the override would
        // have inverted the carve-out: the one path that already had a
        // bound would be the one path the operator's knob cannot reach.
        let settings = if is_range {
            self.budget_settings().set("optimize_read_in_order", 1)
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
                chunk.push(row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?);
                if chunk.len() >= CLIENT_AGG_CHUNK_ROWS {
                    state.push_rows(&chunk)?;
                    chunk.clear();
                }
            }
        }
        state.push_rows(&chunk)?;
        let result = state.finish()?;
        apply_vector_aggs(result, &mp.vector_aggs[..mp.vector_aggs.len() - folded])
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
        warnings: &mut Warnings,
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
            return state.finish(warnings);
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
        // Issue #398: the hard-coded 8 GiB `max_memory_usage` override
        // that used to sit here is gone — every
        // LogQL read now carries the ceiling from
        // `reader.logql_read_max_memory_bytes` via `read_query_settings`,
        // whose default is that same 8 GiB. Keeping the override would
        // have inverted the carve-out: the one path that already had a
        // bound would be the one path the operator's knob cannot reach.
        let settings = if is_range {
            self.budget_settings().set("optimize_read_in_order", 1)
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
                chunk.push(row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?);
                if chunk.len() >= CLIENT_AGG_CHUNK_ROWS {
                    state.push_rows(&chunk)?;
                    chunk.clear();
                }
            }
        }
        state.push_rows(&chunk)?;
        state.finish(warnings)
    }

    /// Evaluates a [`MetricNode`] tree (issue M6-10): leaves execute the
    /// ordinary metric path; `Binary`/`Scalar`/`VectorAgg` combine the
    /// results in-engine.
    ///
    /// **Issue #272: iterative.** The tree is flattened to a post-order
    /// node list on the heap and folded over a value stack reserved once
    /// at the exact high-water mark, so a wide `a or b or c …` chain — a
    /// LEFT-DEEP tree at parse depth 1 — no longer costs a machine frame
    /// per term. The `Pin<Box<dyn Future>>` return type is deleted with
    /// the recursion it existed for.
    ///
    /// Evaluation order is unchanged: post-order left-to-right visits
    /// `lhs`'s whole subtree before `rhs`'s, the first error
    /// short-circuits, and `explain` accumulates leaves left-to-right.
    async fn run_metric_node(
        &self,
        node: &MetricNode,
        explain: Option<&mut PlanExplain>,
        warnings: &mut Warnings,
    ) -> Result<QueryResult, ReadError> {
        // Issue #272, memory L5 Leg B: EVERY allocating step is charged
        // BEFORE it happens, against the same derived cap Leg A uses —
        // the post-order walk's own work stack and node vector inside
        // `metric_node_postorder_charged`, then the value stack here. A
        // failed charge does not mutate the counter and stops the walk
        // before the allocation it refused; the breach is a clean
        // `QueryTooBroad(WalkTransientBytes)` -> 422, never an abort.
        let mut budget = super::walkbound::WalkBudget::new();
        let (nodes, max_value_stack) = plan::metric_node_postorder_charged(node, &mut budget)?;
        budget.charge(max_value_stack.saturating_mul(size_of::<QueryResult>()))?;
        let mut explain = explain;
        let mut vals: Vec<QueryResult> = Vec::with_capacity(max_value_stack);
        for n in nodes {
            let v = match n {
                MetricNode::Leaf(mp) => self.run_metric_inner(mp, explain.as_deref_mut()).await?,
                MetricNode::Scalar(v) => QueryResult::Scalar(*v),
                MetricNode::VectorLit { value, window } => materialize_vector_lit(*value, window)?,
                MetricNode::VectorAgg { aggs, .. } => {
                    let inner = pop_value(&mut vals);
                    apply_vector_aggs(inner, aggs)?
                }
                MetricNode::LabelReplace { spec, .. } => {
                    let inner = pop_value(&mut vals);
                    apply_label_replace(inner, spec)?
                }
                MetricNode::Variants {
                    scan,
                    variants,
                    spec_bytes,
                } => {
                    self.run_variants(
                        scan,
                        variants,
                        *spec_bytes,
                        explain.as_deref_mut(),
                        warnings,
                    )
                    .await?
                }
                MetricNode::Binary {
                    op,
                    return_bool,
                    matching,
                    ..
                } => {
                    // Post-order leaves [.., lhs, rhs] on the tail.
                    let r = pop_value(&mut vals);
                    let l = pop_value(&mut vals);
                    combine_binary(*op, *return_bool, matching.as_ref(), l, r)?
                }
            };
            vals.push(v);
        }
        match vals.pop() {
            Some(v) if vals.is_empty() => Ok(v),
            // Unreachable: a post-order fold consumes exactly `arity(n)`
            // values per node and pushes one, so exactly one survives.
            _ => unreachable!("a post-order metric fold leaves exactly one root value"),
        }
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
            let source = metric_source(mp);
            match mp.step_ns {
                // The execution twin's unreachable arm, refused for the same
                // reason and by the same condition (issues #241 / #257): an
                // EXPLAIN of a plan the engine would refuse must refuse too,
                // or the explain payload would name SQL that can never run.
                // See `run_metric_inner`'s `else` arm for the derivation.
                Some(_) => {
                    return Err(ReadError::PipelineInvalid {
                        reason: "internal: a range metric plan reached the SQL-aggregated \
                                 path; every QuerySpec::Range forces client aggregation \
                                 (plan.rs)"
                            .to_string(),
                    });
                }
                None => super::sql::metric_instant(
                    source,
                    &services,
                    &fingerprints,
                    window,
                    mp.scan_lower,
                    &mp.extra_predicates,
                    // The EXPLAIN twin of the execution site above, and
                    // reached under the same `step_ns.is_none()` instant
                    // condition — so it reports the query that runs.
                    ScanProjection::WithStructuredMetadata,
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
    /// The response-shape options the connection was opened with (issue
    /// #463) — the tail's whole lifetime, since the header is read once
    /// at upgrade and a WebSocket has no per-frame request.
    opts: ResponseOptions,
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
            let row = row.map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })?;
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
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
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
                rows.push(row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?);
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
        explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<DetectedLabelOut>, ReadError> {
        self.run_discovery::<DetectedLabelRow, DetectedLabelOut>(
            DiscoveryQuery::DetectedLabels,
            selector,
            b,
            explain,
            "detected_labels requires a log stream selector (matchers only)",
            |row| {
                // The reference keep rule: static labels always; anything
                // else only when NOT every value is float-or-UUID.
                (detected::is_static_detected_label(&row.key) || row.non_id_values > 0).then_some(
                    DetectedLabelOut {
                        label: row.key,
                        cardinality: row.cardinality,
                    },
                )
            },
        )
        .await
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
        explain: Option<&mut PlanExplain>,
    ) -> Result<DetectedFields, ReadError> {
        let mut acc = FieldAccumulator::new(field_limit);
        let Some(truncated) = self
            .detected_fields_scan(expr, b, line_limit, &mut acc, explain)
            .await?
        else {
            return Ok(DetectedFields {
                fields: Vec::new(),
                truncated: false,
                retention_capped: false,
            });
        };
        let (fields, retention_capped) = acc.finish();
        Ok(DetectedFields {
            fields,
            truncated,
            retention_capped,
        })
    }

    /// `/api/logs/v1/detected_field/{name}/values` (issue #482): the
    /// `/detected_fields` sampling with the accumulator restricted to ONE
    /// field name and its value set capped at `value_limit`, checked
    /// BETWEEN entries — everything one sampled entry contributes is
    /// added before the cap is consulted again, so a single entry can
    /// carry the total past `value_limit`.
    pub async fn detected_field_values(
        &self,
        field: &str,
        expr: &Expr,
        b: TimeBounds,
        line_limit: u32,
        value_limit: u32,
    ) -> Result<DetectedFieldValues, ReadError> {
        self.detected_field_values_inner(field, expr, b, line_limit, value_limit, None)
            .await
    }

    /// [`LogQlEngine::detected_field_values`] plus its
    /// `X-Pulsus-Explain` trace, in the same single pass (no second scan)
    /// — the `query_explained` contract.
    pub async fn detected_field_values_explained(
        &self,
        field: &str,
        expr: &Expr,
        b: TimeBounds,
        line_limit: u32,
        value_limit: u32,
    ) -> Result<(DetectedFieldValues, PlanExplain), ReadError> {
        let mut explain = PlanExplain::new("detected_field_values");
        let out = self
            .detected_field_values_inner(
                field,
                expr,
                b,
                line_limit,
                value_limit,
                Some(&mut explain),
            )
            .await?;
        Ok((out, explain))
    }

    async fn detected_field_values_inner(
        &self,
        field: &str,
        expr: &Expr,
        b: TimeBounds,
        line_limit: u32,
        value_limit: u32,
        explain: Option<&mut PlanExplain>,
    ) -> Result<DetectedFieldValues, ReadError> {
        let mut acc = FieldAccumulator::for_field_values(field, value_limit);
        let Some(truncated) = self
            .detected_fields_scan(expr, b, line_limit, &mut acc, explain)
            .await?
        else {
            return Ok(DetectedFieldValues {
                values: Vec::new(),
                truncated: false,
                retention_capped: false,
            });
        };
        let (values, retention_capped) = acc.into_field_values();
        Ok(DetectedFieldValues {
            values,
            truncated,
            retention_capped,
        })
    }

    /// The stage-1/2/3 sampling half shared by `/detected_fields` and
    /// `/detected_field/{name}/values` (issue #482): identical plan,
    /// identical statements, identical paging — the two routes differ
    /// only in which [`FieldAccumulator`] they feed and in how they
    /// project it afterwards.
    ///
    /// `Ok(None)` = stage 1 matched no stream, so nothing was scanned and
    /// the caller returns its own empty result; `Ok(Some(truncated))`
    /// otherwise, with `truncated` carrying the paged loop's
    /// budget-exhaustion terminal.
    async fn detected_fields_scan(
        &self,
        expr: &Expr,
        b: TimeBounds,
        line_limit: u32,
        acc: &mut FieldAccumulator,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Option<bool>, ReadError> {
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
            return Ok(None);
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
                    let row = row.map_err(|e| {
                        map_read_error(
                            e,
                            self.config.scan_budget_bytes,
                            self.config.read_max_memory_bytes,
                        )
                    })?;
                    if matched >= line_limit || acc.value_cap_reached() {
                        // `line_limit` stops FEEDING, never DRAINING.
                        // Issue #482's value cap is the second feeding
                        // stop and is checked here, between entries —
                        // inert on `/detected_fields`, whose accumulator
                        // leaves both `name_filter` and `value_limit`
                        // `None`.
                        continue;
                    }
                    if feeder.feed_row(
                        row.fingerprint,
                        row.timestamp_ns,
                        &row.body,
                        &row.structured_metadata,
                        &base_labels,
                        &compiled,
                        acc,
                    )? {
                        matched += 1;
                    }
                }
            }
            return Ok(Some(false));
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
                acc,
            )
            .await?;
        Ok(Some(truncated))
    }

    /// The detected_fields fetch-until-limit paging loop (issue #170 plan
    /// v2) — a structural sibling of [`LogQlEngine::run_streams_paged`]
    /// feeding a [`FieldAccumulator`] + a post-pipeline matched-entry
    /// counter instead of a `StreamAccumulator`. Shares the #90 pieces
    /// verbatim: [`super::sql::stage3_keyset`] pages (PK-pruned,
    /// skip-index-prunable predicates, keyset total order), [`advance_tail_cursor`]
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
        services: &[CheckedLiteral],
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
                    |e| map_read_error(e, budget, self.config.read_max_memory_bytes),
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
        self.tail_setup_with(expr, params, ResponseOptions::default())
    }

    /// [`Self::tail_setup`] with the response-shape options the upgrade
    /// request asked for (issue #463).
    pub fn tail_setup_with(
        &self,
        expr: &Expr,
        params: &QueryParams,
        opts: ResponseOptions,
    ) -> Result<TailSetup, ReadError> {
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
                    opts,
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

        // Streamed, not staged (issue #312): a poll used to build a
        // page-sized `Vec<TailSampleRow>` and re-materialise it into a
        // second `Vec<SampleRow>`, both bounded only by
        // `tail_max_fetch_limit` rows x the 64 MiB ingest cap. Rows are
        // now charged and chunked as they arrive, and the cursor walks
        // the raw page incrementally through the same
        // [`TailCursorTracker`] the paged loops use.
        let mut acc = StreamAccumulator::with_opts(&meta, fetch_limit, setup.opts);
        let mut tracker = TailCursorTracker::new();
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule).
            let mut stream = self
                .query_stream::<TailSampleRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                tracker.observe(row.timestamp_ns, row.fingerprint, row.body_hash);
                acc.push_row(
                    SampleRow {
                        fingerprint: row.fingerprint,
                        timestamp_ns: row.timestamp_ns,
                        body: row.body,
                        structured_metadata: row.structured_metadata,
                    },
                    &setup.compiled,
                )?;
            }
        }
        acc.flush_chunk(&setup.compiled)?;
        let (next, fetched) = tracker.finish(prev);
        let streams = acc.into_streams();
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
///
/// **No production caller since issue #312** — every paged drain now
/// walks its page incrementally through
/// [`TailCursorTracker`](super::detected_probe::TailCursorTracker). This
/// survives as that tracker's INDEPENDENT ORACLE: the already-committed
/// `tail_cursor_tracker_matches_advance_tail_cursor_over_randomized_sequences`
/// (`detected_probe.rs`) is what proves the tracker computes the same
/// function, so deleting this as "dead code" would take the tracker's
/// only independent check with it.
#[cfg(test)]
pub(in crate::logql) fn advance_tail_cursor(
    prev: Option<TailCursor>,
    rows: &[TailSampleRow],
) -> Option<TailCursor> {
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

/// Issue #277: does the FINAL-result series cap apply to this root?
///
/// **No, for a root `variants(...)`, and only there.** Measured against
/// the pinned reference: it dispatches on the ROOT expression
/// (`pkg/logql/engine.go:321-322 @ grafana/loki v3.7.4
/// b318f2829f0ae2094ab3a1e90780450e9e4b03be` — "A VariantsExpr is a
/// specific type of SampleExpr, so make sure this case is evaluated
/// first") into `evalVariants` → `JoinMultiVariantSampleVector`
/// (`:608-660`), which caps PER VARIANT and never the concatenation. Every
/// other root — including `variants(...) + 1` — goes to `JoinSampleVector`
/// (`:520-607`) and its plain cap at `:538`/`:588`.
///
/// Captured 2026-08-12 on that container: a root variants query returning
/// **501 total** series across two variants is `200` with no warning,
/// while the identical expression `+ 1` is `400` with
/// `maximum number of series (500) reached for a single query; …`.
///
/// Without this exemption the per-variant gate #236 landed is SHADOWED:
/// `exec.rs` capped the concatenation unconditionally, so a three-variant
/// query returning 400 series each — every variant under the cap — was a
/// 422.
/// `pub` so the conformance runner (`tests/logqltest/runner.rs`) applies
/// the IDENTICAL root test on its own `Plan::MetricBinary` arm rather
/// than a hand-copied twin of it — the same reason
/// [`ensure_result_series`](super::charge::ensure_result_series) is
/// exported.
pub fn final_series_gate_applies(node: &MetricNode) -> bool {
    !matches!(node, MetricNode::Variants { .. })
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

/// Issue #230 follow-up / issue #287: a per-row output-budget breach is
/// the bounded 422 (the same complete-or-error class as every other
/// `QueryTooBroad` reason — never a truncation, never an OOM). Each
/// ledger keeps its OWN reason, so the 422 body names the counter that
/// actually refused.
impl From<super::pipeline::RowBudgetExceeded> for ReadError {
    fn from(e: super::pipeline::RowBudgetExceeded) -> Self {
        let budget_bytes = e.budget_bytes;
        ReadError::QueryTooBroad(match e.budget {
            super::pipeline::RowBudget::TemplateRender => {
                TooBroadReason::TemplateOutputBytes { budget_bytes }
            }
            super::pipeline::RowBudget::JsonFlattenKeys => {
                TooBroadReason::JsonFlattenKeyBytes { budget_bytes }
            }
        })
    }
}

/// The streams paging loop's per-page state (issue #312) — a structural
/// sibling of [`DetectedPagedState`] (issue #244), so the two loops share
/// one drain discipline instead of carrying two hand-written twins.
#[derive(Debug)]
pub(in crate::logql) struct StreamsPagedState {
    pub(in crate::logql) cursor: Option<TailCursor>,
    pub(in crate::logql) spent: u64,
    pub(in crate::logql) page_size: u32,
    pub(in crate::logql) budget: u64,
}

impl StreamsPagedState {
    /// Drains ONE already-opened page to completion, charging and
    /// chunking each row through `acc` as it arrives, then returns the
    /// loop's decision: `Ok(None)` continue / `Ok(Some(false))`
    /// terminate-COMPLETE / `Ok(Some(true))` terminate-PARTIAL / `Err`
    /// propagate. The drain stops at the FIRST error — exactly what the
    /// pre-#312 per-row `?` did. Generic over the stream AND its error so
    /// the seam never names `ChError`.
    ///
    /// The staged chunk is flushed BEFORE the page's fate is decided, so
    /// a mid-page `ScanBudgetBytes` breach keeps the rows that already
    /// arrived; a result-budget breach raised by that flush propagates
    /// rather than being downgraded to a partial.
    pub(in crate::logql) async fn absorb_page<S, E>(
        &mut self,
        stream: &mut S,
        read_bytes: impl FnOnce(&S) -> Option<u64>,
        map_err: impl Fn(E) -> ReadError,
        acc: &mut StreamAccumulator<'_>,
        compiled: &super::pipeline::CompiledPipeline,
    ) -> Result<Option<bool>, ReadError>
    where
        S: Stream<Item = Result<TailSampleRow, E>> + Unpin,
    {
        let mut tracker = TailCursorTracker::new();
        let mut page_err: Option<ReadError> = None;
        let mut filled = false;
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
            match acc.push_row(
                SampleRow {
                    fingerprint: row.fingerprint,
                    timestamp_ns: row.timestamp_ns,
                    body: row.body,
                    structured_metadata: row.structured_metadata,
                },
                compiled,
            ) {
                Ok(true) => filled = true,
                Ok(false) => {}
                Err(e) => {
                    page_err = Some(e);
                    break;
                }
            }
        }
        let (cursor, fetched) = tracker.finish(self.cursor.take());
        self.cursor = cursor;
        // Feed and RELEASE whatever the drain staged before the page's
        // fate is decided: a mid-page error keeps its prefix, and a
        // result-budget refusal raised here is a hard 422 that
        // `classify_page_error` must never see.
        filled |= acc.flush_chunk(compiled)?;
        if let Some(mapped) = page_err {
            return classify_page_error(mapped, self.spent).map(Some);
        }
        let read = read_bytes(stream).unwrap_or_else(|| self.budget.saturating_sub(self.spent));
        self.spent = self.spent.saturating_add(read);
        if filled {
            // Result limit filled — a complete result, never partial.
            return Ok(Some(false));
        }
        if fetched < self.page_size {
            // Fewer rows than asked ⇒ the window is exhausted — a
            // complete result over the whole window, never partial.
            return Ok(Some(false));
        }
        Ok(None)
    }
}

/// The line-filter-only fast path's RETENTION, factored out of
/// [`LogQlEngine::run_streams_inner`] (issue #312) so its hermetic probe
/// drives the shipped body rather than a transcription of it.
///
/// Zero-structured-metadata rows group by source fingerprint with
/// `labels_json` verbatim; structured-metadata-bearing rows (issue #97)
/// fan out through [`SmFanOutAccumulator`]. Rows are absorbed as they
/// arrive — there is no staging on this path at all — and a row whose
/// fingerprint is absent from `meta` is skipped, which is what the
/// pre-#312 drain's `filter_map` did at the other end.
#[derive(Debug, Default)]
pub(in crate::logql) struct FastPathGroups {
    by_fp: HashMap<u64, StreamResult>,
    sm: SmFanOutAccumulator,
    /// Issue #463. With `categorize-labels` the response groups by the
    /// STREAM-category label set, not by fingerprint-or-merged-set — so a
    /// structured-metadata row rejoins the stream it came from instead of
    /// fanning out (reference-captured: seven differently-flagged entries
    /// collapse from seven stream objects to one). Both kinds of row
    /// therefore land in ONE label-keyed map here, and `by_fp`/`sm` stay
    /// empty.
    categorize: bool,
    cat_groups: HashMap<String, FanOutGroup>,
    /// Reused per row on the categorised metadata path only — the same
    /// clear-and-refill discipline `SmFanOutAccumulator` uses.
    cat_base: HashMap<u64, Vec<(String, String)>>,
    cat_merge_buf: Vec<(String, String)>,
    cat_sm_buf: Vec<(String, String)>,
    cat_sm_ctx: StructuredMetadataCtx,
}

impl FastPathGroups {
    pub(in crate::logql) fn new() -> Self {
        Self::default()
    }

    /// [`Self::new`] with the issue #463 wire-shape decision stated.
    pub(in crate::logql) fn new_with(categorize: bool) -> Self {
        Self {
            categorize,
            ..Self::default()
        }
    }

    /// Charges the group on FIRST SIGHT of the fingerprint and the entry
    /// before the body is retained, so neither the map entry nor the
    /// entry exists when the cap refuses.
    pub(in crate::logql) fn push_row(
        &mut self,
        row: SampleRow,
        meta: &HashMap<u64, StreamMetaRow>,
        budget: &mut StreamsResultBudget,
    ) -> Result<(), ReadError> {
        let Some(m) = meta.get(&row.fingerprint) else {
            return Ok(());
        };
        if self.categorize {
            return self.push_row_categorized(row, m, budget);
        }
        if !row.structured_metadata.is_empty() {
            return self.sm.push_row(&row, meta, budget);
        }
        match self.by_fp.entry(row.fingerprint) {
            std::collections::hash_map::Entry::Occupied(e) => {
                budget.charge_entry(row.body.len(), 0)?;
                e.into_mut().entries.push((row.timestamp_ns, row.body));
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                budget.charge_group(&m.labels, &m.service)?;
                budget.charge_entry(row.body.len(), 0)?;
                e.insert(StreamResult {
                    fingerprint: row.fingerprint,
                    service: m.service.clone(),
                    labels_json: m.labels.clone(),
                    entries: vec![(row.timestamp_ns, row.body)],
                    categories: Vec::new(),
                });
            }
        }
        Ok(())
    }

    /// The issue #463 categorised branch. No pipeline runs on this path,
    /// so the stream category is the hydrated `labels_json` VERBATIM
    /// unless a metadata value took over one of its slots — which is the
    /// only case that re-renders, and is what makes the common row cost
    /// exactly one hash probe more than the plain path.
    fn push_row_categorized(
        &mut self,
        row: SampleRow,
        m: &StreamMetaRow,
        budget: &mut StreamsResultBudget,
    ) -> Result<(), ReadError> {
        if row.structured_metadata.is_empty() {
            return push_fanout_entry(
                &mut self.cat_groups,
                budget,
                GroupKey::Rendered {
                    labels_json: m.labels.clone(),
                    fingerprint: row.fingerprint,
                },
                row.timestamp_ns,
                Cow::Owned(row.body),
                &m.service,
                Some(EntryCategories::default()),
            );
        }
        let base = self
            .cat_base
            .entry(row.fingerprint)
            .or_insert_with(|| parse_flat_labels(&m.labels));
        merge_labels_with_structured_metadata(
            base,
            &row.structured_metadata,
            &mut self.cat_merge_buf,
            &mut self.cat_sm_buf,
            &mut self.cat_sm_ctx,
        );
        let categories = split_merged_categories(&mut self.cat_merge_buf, &self.cat_sm_ctx);
        // The stream half is unchanged from the hydrated set unless a
        // metadata value won a stream slot, so the canonical JSON is
        // already in hand for every ordinary row.
        let (labels_json, fingerprint) = if self.cat_sm_ctx.sm_over_stream.is_empty() {
            (m.labels.clone(), row.fingerprint)
        } else {
            let sorted: Vec<(Cow<'_, str>, Cow<'_, str>)> = self
                .cat_merge_buf
                .iter()
                .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str())))
                .collect();
            let json = render_labels_json_sorted(&sorted);
            let fp = fnv1a64(json.as_bytes());
            (json, fp)
        };
        push_fanout_entry(
            &mut self.cat_groups,
            budget,
            GroupKey::Rendered {
                labels_json,
                fingerprint,
            },
            row.timestamp_ns,
            Cow::Owned(row.body),
            &m.service,
            Some(categories),
        )
    }

    pub(in crate::logql) fn into_streams(self) -> Vec<StreamResult> {
        let mut streams: Vec<StreamResult> = self.by_fp.into_values().collect();
        streams.extend(self.sm.into_streams());
        streams.extend(
            self.cat_groups
                .into_iter()
                .map(|(labels_json, g)| StreamResult {
                    fingerprint: g.fingerprint,
                    service: g.service,
                    labels_json,
                    entries: g.entries,
                    categories: g.categories,
                }),
        );
        streams
    }
}

/// The hermetic test seam for the issue #312 line-filter-only fast path —
/// the [`DetectedFieldsProbe`](super::detected_probe::DetectedFieldsProbe)
/// pattern. `#[doc(hidden)]`: consumed only by
/// `tests/logql_streams_result_budget.rs`, never by callers.
#[doc(hidden)]
#[derive(Debug)]
pub struct StreamsFastPathProbe {
    groups: FastPathGroups,
    budget: StreamsResultBudget,
}

impl StreamsFastPathProbe {
    pub fn with_cap(cap: u64) -> Self {
        Self {
            groups: FastPathGroups::new(),
            budget: StreamsResultBudget::with_cap(cap),
        }
    }

    /// [`Self::with_cap`] with the issue #463 categorised wire shape, so
    /// a gate on the categorised charge drives the shipped accumulator
    /// rather than a formula written beside it.
    pub fn with_cap_categorized(cap: u64) -> Self {
        Self {
            groups: FastPathGroups::new_with(true),
            budget: StreamsResultBudget::with_cap(cap),
        }
    }

    /// One row through the REAL fast-path body.
    pub fn push_row(
        &mut self,
        row: SampleRow,
        meta: &HashMap<u64, StreamMetaRow>,
    ) -> Result<(), ReadError> {
        self.groups.push_row(row, meta, &mut self.budget)
    }

    pub fn charged(&self) -> u64 {
        self.budget.charged()
    }

    pub fn staged(&self) -> u64 {
        self.budget.staged()
    }

    pub fn into_streams(self) -> Vec<StreamResult> {
        self.groups.into_streams()
    }
}

/// The hermetic test seam for the issue #312 streaming paged loop — the
/// [`DetectedFieldsProbe`](super::detected_probe::DetectedFieldsProbe)
/// pattern, so the loop body under test is the SHIPPED one rather than a
/// transcription of it. `#[doc(hidden)]`: consumed only by
/// `tests/logql_streams_result_budget.rs`, never by callers.
#[doc(hidden)]
#[derive(Debug)]
pub struct StreamsPagedProbe {
    state: StreamsPagedState,
}

impl StreamsPagedProbe {
    pub fn new(page_size: u32, budget: u64) -> Self {
        Self {
            state: StreamsPagedState {
                cursor: None,
                spent: 0,
                page_size,
                budget,
            },
        }
    }

    /// Pretends `spent` bytes were already scanned — the "later page"
    /// half of the #90 branch split.
    pub fn with_spent(mut self, spent: u64) -> Self {
        self.state.spent = spent;
        self
    }

    pub fn spent(&self) -> u64 {
        self.state.spent
    }

    pub fn cursor(&self) -> Option<TailCursor> {
        self.state.cursor
    }

    /// One injected page through the REAL paged-loop body.
    pub async fn absorb_page<S>(
        &mut self,
        stream: &mut S,
        read_bytes: u64,
        acc: &mut StreamAccumulator<'_>,
        compiled: &super::pipeline::CompiledPipeline,
    ) -> Result<Option<bool>, ReadError>
    where
        S: Stream<Item = Result<TailSampleRow, ReadError>> + Unpin,
    {
        self.state
            .absorb_page(stream, |_| Some(read_bytes), |e| e, acc, compiled)
            .await
    }
}

/// The BYTES staged before one [`StreamAccumulator::feed`] — byte-
/// denominated on purpose: a row count is exactly the proxy issue #312
/// exists to reject. Peak staged retention is this plus at most one row,
/// whatever the stream's length or the rows' size.
pub const STREAM_FEED_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

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
    /// The issue #312 peak-retention ledger — result charges plus the
    /// outstanding staged chunk.
    budget: StreamsResultBudget,
    /// Rows staged between `feed` calls, bounded in BYTES by
    /// [`STREAM_FEED_CHUNK_BYTES`] rather than by a row count — a row
    /// count is exactly the proxy issue #312 exists to reject.
    chunk: Vec<SampleRow>,
    /// Issue #463. With `categorize-labels` EVERY surviving row — plain,
    /// transform and fan-out alike — is keyed by its STREAM-category
    /// label set, so all three land in `label_groups` and `fp_groups`
    /// stays empty. That single map is what makes a stream's
    /// differently-flagged entries collapse back into one object, which
    /// is what the reference returns under the flag.
    categorize: bool,
}

impl<'m> StreamAccumulator<'m> {
    pub fn new(meta: &'m HashMap<u64, StreamMetaRow>, result_limit: u32) -> Self {
        Self::with_cap(meta, result_limit, MAX_STREAMS_RESULT_BYTES)
    }

    /// [`Self::new`] with the issue #463 wire-shape decision stated.
    pub fn with_opts(
        meta: &'m HashMap<u64, StreamMetaRow>,
        result_limit: u32,
        opts: super::params::ResponseOptions,
    ) -> Self {
        let mut acc = Self::with_cap(meta, result_limit, MAX_STREAMS_RESULT_BYTES);
        acc.categorize = opts.categorize_labels;
        acc
    }

    /// A test-visible ceiling; production sites call [`Self::new`].
    pub fn with_cap(meta: &'m HashMap<u64, StreamMetaRow>, result_limit: u32, cap: u64) -> Self {
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
            budget: StreamsResultBudget::with_cap(cap),
            chunk: Vec::new(),
            categorize: false,
        }
    }

    /// Result bytes charged so far (issue #312).
    pub fn charged(&self) -> u64 {
        self.budget.charged()
    }

    /// Staged bytes outstanding right now (issue #312).
    pub fn staged(&self) -> u64 {
        self.budget.staged()
    }

    /// The shipped ceiling this accumulator was built with.
    pub fn cap(&self) -> u64 {
        self.budget.cap()
    }

    /// Stages ONE streamed row — charged IN FULL (body, structured
    /// metadata and slot) before it is pushed — and feeds the chunk once
    /// its staged bytes reach [`STREAM_FEED_CHUNK_BYTES`]. `true` ==
    /// the result limit filled, so the caller can stop paging.
    ///
    /// This replaces the response-sized `Vec<SampleRow>` the streams
    /// paths used to build (issue #312): peak staged bytes are bounded by
    /// the chunk plus at most one row, never by the stream.
    pub fn push_row(
        &mut self,
        row: SampleRow,
        compiled: &super::pipeline::CompiledPipeline,
    ) -> Result<bool, ReadError> {
        self.budget
            .charge_staged_row(row.body.len(), row.structured_metadata.len())?;
        self.chunk.push(row);
        if self.budget.staged() >= STREAM_FEED_CHUNK_BYTES {
            return self.flush_chunk(compiled);
        }
        Ok(self.survivors >= self.result_limit)
    }

    /// Feeds and RELEASES the staged chunk. Must be called after the
    /// drain — the staged charge is only sound if it is discharged when
    /// the rows it paid for are dropped.
    pub fn flush_chunk(
        &mut self,
        compiled: &super::pipeline::CompiledPipeline,
    ) -> Result<bool, ReadError> {
        if self.chunk.is_empty() {
            self.budget.discharge_staged();
            return Ok(self.survivors >= self.result_limit);
        }
        // Taken out so `feed`'s `&[SampleRow]` does not alias `self`;
        // the buffer's capacity is handed back afterwards.
        let mut chunk = std::mem::take(&mut self.chunk);
        let filled = self.feed(&chunk, compiled);
        chunk.clear();
        self.chunk = chunk;
        self.budget.discharge_staged();
        filled
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
            budget,
            // The staged chunk is `take`n by `flush_chunk` before it calls
            // here, so `feed` never sees it. Named rather than elided so a
            // future field is a build failure, not a silent omission.
            chunk: _,
            categorize,
        } = self;
        let categorize = *categorize;
        // Categorised mode groups by the stream-category subset, which is
        // a rendered label set for every row — so it takes the fan-out
        // shape whatever the pipeline does.
        let fan_out = compiled.mutates_labels() || categorize;
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
        // Issue #463: the per-label category vector the categorised
        // pipeline fills, reused across rows exactly like the label
        // scratch beside it. Never touched when the flag is off.
        let mut cat_scratch: Vec<super::pipeline::LabelCategory> = Vec::new();
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
                let Some(line) = (if categorize {
                    compiled.run_into_with_sm_categorized(
                        &row.body,
                        base,
                        row.timestamp_ns,
                        &EMPTY_STRUCTURED_METADATA,
                        &mut scratch,
                        &mut cat_scratch,
                    )?
                } else {
                    compiled.run_into(&row.body, base, row.timestamp_ns, &mut scratch)?
                }) else {
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
                    // Issue #463: split BEFORE the sort — the category
                    // vector is parallel to the label vector in the
                    // order the pipeline left it.
                    let categories =
                        categorize.then(|| split_categories(&mut scratch, &cat_scratch));
                    scratch.sort_unstable();
                    push_fanout_entry(
                        label_groups,
                        budget,
                        GroupKey::Sorted(&scratch),
                        row.timestamp_ns,
                        line,
                        &m.service,
                        categories,
                    )?;
                } else {
                    // Issue #312: the group is charged on FIRST SIGHT of
                    // the fingerprint and the entry before
                    // `line.into_owned()`, so neither the map entry nor
                    // the owned line exists when the cap refuses.
                    match fp_groups.entry(row.fingerprint) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            budget.charge_entry(line.len(), 0)?;
                            e.into_mut()
                                .entries
                                .push((row.timestamp_ns, line.into_owned()));
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            budget.charge_group(&m.labels, &m.service)?;
                            budget.charge_entry(line.len(), 0)?;
                            e.insert(StreamResult {
                                fingerprint: row.fingerprint,
                                service: m.service.clone(),
                                labels_json: m.labels.clone(),
                                entries: vec![(row.timestamp_ns, line.into_owned())],
                                categories: Vec::new(),
                            });
                        }
                    }
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
                    budget,
                    row.timestamp_ns,
                    &m.service,
                    sm_scratch,
                    categorize.then_some(&mut cat_scratch),
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
                        categories: g.categories,
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

/// The SQL-pushdown instant path's client-side re-grouping (issue #249).
///
/// `metric_instant` groups server-side by `(fingerprint,
/// structured_metadata)`; this folds those partial counts into the OUTPUT
/// series, which — since the metric path merges structured metadata into the
/// label set — is keyed by the merged final label set, not by fingerprint.
///
/// **Why summing the partials is exact.** Every op that can reach the
/// pushdown path is a linear sum: `client == None` requires no pipeline past
/// a pushed line filter and a non-unwrap reducer, which leaves `agg_expr` as
/// exactly `count()` or `sum(length(body))` (`plan.rs`). Summing per-metadata
/// partial counts and applying `apply_rate` ONCE afterwards therefore yields
/// the same bits as the client path's single accumulator — whereas applying
/// the rate divisor per partial would not (`a/s + b/s != (a+b)/s` in
/// IEEE-754).
///
/// **Bounded before it allocates.** Each distinct group is charged through
/// the same [`group_entry_bytes`]/[`charge_group_bytes`] helpers and against
/// the same `AggCaps::group_bytes` counter the client paths use, BEFORE the
/// map retains it — so a query refuses with the identical named 422 whichever
/// way it routed. The output vector is built only from groups that were
/// charged, so neither container ever holds a refused group.
#[derive(Debug)]
pub(in crate::logql) struct PushdownInstantGroups {
    /// Each resolved stream's base label set, snapshotted ONCE — the
    /// pre-#249 code called `series_labels` per returned row, and
    /// `(fingerprint, structured_metadata)` returns strictly more rows.
    base_labels: HashMap<u64, LabelSet>,
    /// Rendered final label set -> `(labels, summed count)`.
    groups: HashMap<String, (LabelSet, u64)>,
    /// Query-lifetime group bytes, never discharged: the groups ARE the
    /// result (the `apply_vector_aggs` precedent).
    charged: u64,
    caps: AggCaps,
    merge_buf: Vec<(String, String)>,
    sm_buf: Vec<(String, String)>,
    sm_ctx: StructuredMetadataCtx,
}

impl PushdownInstantGroups {
    pub(in crate::logql) fn new(meta: &HashMap<u64, StreamMetaRow>, caps: AggCaps) -> Self {
        PushdownInstantGroups {
            base_labels: meta.iter().map(|(fp, m)| (*fp, series_labels(m))).collect(),
            groups: HashMap::new(),
            charged: 0,
            caps,
            merge_buf: Vec::new(),
            sm_buf: Vec::new(),
            sm_ctx: StructuredMetadataCtx::default(),
        }
    }

    /// Folds one returned row. A row whose fingerprint did not hydrate is
    /// skipped, exactly as before.
    pub(in crate::logql) fn push_row(&mut self, row: &MetricInstantRow) -> Result<(), ReadError> {
        let Some(base) = self.base_labels.get(&row.fingerprint) else {
            return Ok(());
        };
        // No pipeline runs on this path by construction (`client == None`),
        // so the merge IS the whole label computation — the reference's
        // `NoopStage` short-circuit, which sits AFTER
        // `builder.Add(StructuredMetadataLabel, …)`
        // (`pkg/logql/log/metrics_extraction.go:102-108 @ v3.7.4`), so even
        // a stage-free query merges. The out-of-band error slots are then
        // materialised by the same `visible()` rule the pipeline applies at
        // emit (issue #238).
        let labels: LabelSet = if row.structured_metadata.is_empty() {
            base.clone()
        } else {
            merge_labels_with_structured_metadata(
                base,
                &row.structured_metadata,
                &mut self.merge_buf,
                &mut self.sm_buf,
                &mut self.sm_ctx,
            );
            let mut merged = std::mem::take(&mut self.merge_buf);
            self.sm_ctx.append_visible(&mut merged);
            merged.sort();
            merged
        };
        // A surviving `__error__` fails the whole query here too — measured
        // on the reference at v3.7.4: `count_over_time({…}[5m])` with NO
        // pipeline over an entry whose metadata carries `__error__` answers
        // 400, not a filtered result.
        check_surviving_error(&labels)?;
        let key = render_series_labels(&labels);
        match self.groups.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                e.get_mut().1 = e.get().1.saturating_add(row.n);
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                charge_group_bytes(
                    &mut self.charged,
                    group_entry_bytes(e.key(), &labels, PUSHDOWN_INSTANT_SLOT),
                    self.caps.group_bytes,
                )?;
                e.insert((labels, row.n));
            }
        }
        Ok(())
    }

    /// How many bytes this state has charged — the seam AC-7's "the map
    /// never held the refused group" assertion reads.
    #[cfg(test)]
    pub(in crate::logql) fn charged_bytes(&self) -> u64 {
        self.charged
    }

    #[cfg(test)]
    pub(in crate::logql) fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// The rate divisor is applied ONCE, to the summed count. Emitted in
    /// rendered-label order so the value a downstream `sum` accumulates is
    /// reproducible run to run (a `HashMap` drain is not).
    pub(in crate::logql) fn finish(self, rate_window_ns: Option<u64>) -> Vec<InstantSeries> {
        let mut out: Vec<(String, LabelSet, u64)> = self
            .groups
            .into_iter()
            .map(|(key, (labels, n))| (key, labels, n))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out.into_iter()
            .map(|(_, labels, n)| InstantSeries {
                labels,
                value: apply_rate(n as f64, rate_window_ns),
            })
            .collect()
    }
}

/// The client-aggregated raw fetch SQL for a planned metric leaf — the ONE
/// implementation shared by execution (`run_metric_client`) and EXPLAIN
/// (`explain_metric`), so the reported query is by construction the query
/// that runs (issue #227 review round 5, finding 3). A RANGE query reads in
/// physical-key order for the streaming slide; an instant query keeps the
/// total-timestamp order its reducers pin.
///
/// The [`ScanProjection`] is derived HERE, from `mp.op`, rather than being
/// passed in — one site, so execution and EXPLAIN cannot disagree about
/// whether `structured_metadata` is read (issue #249).
fn client_metric_read_sql(
    mp: &MetricPlan,
    services: &[CheckedLiteral],
    fingerprints: &[u64],
    window: super::sql::TimeWindow,
) -> String {
    // `absent_over_time` is the ONLY reducer whose label set is provably
    // metadata-independent (`syntax/extractor.go:46-47` forces
    // `noLabels = true`, and `labels.go:667-668` then returns
    // `EmptyLabelsResult` — v3.7.4), so it is the only one that keeps the
    // lean projection on this unbounded scan.
    let projection = if matches!(mp.op, RangeAggOp::AbsentOverTime) {
        ScanProjection::Lean
    } else {
        ScanProjection::WithStructuredMetadata
    };
    if mp.step_ns.is_some() {
        super::sql::metric_raw_samples_sliding(
            &mp.table,
            services,
            fingerprints,
            window,
            mp.scan_lower,
            &mp.extra_predicates,
            projection,
        )
    } else {
        super::sql::metric_raw_samples(
            &mp.table,
            services,
            fingerprints,
            window,
            mp.scan_lower,
            &mp.extra_predicates,
            projection,
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
            // Issue #343: the plan's bounds are already offset-shifted;
            // this carries the shift the evaluator adds back on emit.
            offset_ns: mp.offset_ns,
        },
        None => ClientWindow::Instant {
            start_ns: mp.grid_start_ns,
            end_ns: mp.end_ns,
        },
    }
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
///
/// Issue #398 adds the per-query memory ceiling —
/// `max_memory_usage = read_max_memory_bytes` (from
/// `reader.logql_read_max_memory_bytes`) plus
/// `max_bytes_before_external_group_by = 0`, i.e. **throw, not spill**: a
/// spilled aggregation is the silently-slow outcome this repo rejects, and
/// the shipped traces precedent (`traces::exec::generator_settings`, issue
/// #57) sets exactly the same pair. Because every LogQL settings object is
/// built here, one edit reaches all eighteen dispatch sites including the
/// shared stage-1 stream resolution several endpoints run — a breach is
/// server code 241 → [`TooBroadReason::LogqlReadMemory`] → `422`, never
/// the raw-exception `500` it was before.
pub fn read_query_settings(scan_budget_bytes: u64, read_max_memory_bytes: u64) -> QuerySettings {
    QuerySettings::new()
        .set("max_bytes_to_read", scan_budget_bytes)
        .set("read_overflow_mode", "throw")
        .set("max_query_size", crate::querytext::MAX_QUERY_TEXT_BYTES)
        .set("max_memory_usage", read_max_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
}

/// [`read_query_settings`] plus, when `distributed`,
/// `distributed_product_mode='local'` — see
/// [`LogQlEngine::activity_settings`], whose whole body this is. Split out
/// as a free function for the same reason [`read_query_settings`] is one:
/// the settings decision is provable without a ClickHouse connection.
fn activity_query_settings(
    scan_budget_bytes: u64,
    read_max_memory_bytes: u64,
    distributed: bool,
) -> QuerySettings {
    let base = read_query_settings(scan_budget_bytes, read_max_memory_bytes);
    if distributed {
        base.set("distributed_product_mode", "local")
    } else {
        base
    }
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
/// overflow code and (issue #398) the memory-ceiling overflow code to
/// structured [`ReadError::QueryTooBroad`] reasons, and leaving every
/// other server code (including 158 `TOO_MANY_ROWS`, which the LogQL path
/// never triggers because it never sets `max_rows_to_read` — the traces
/// search path sets that budget deliberately and maps 158 in its **own**
/// mapper, `traces::exec::map_trace_read_error`, issue #57) as a generic
/// [`ReadError::Clickhouse`] passthrough — never reinterpreted as a
/// timeout or vice versa.
///
/// **The #412 rule.** `ChError::Server.code` used to be parsed out of the
/// exception TEXT on the streaming path, and tenant bytes reach that text, so
/// the code was spoofable. Issue #412 closed that on any server that declares
/// `X-ClickHouse-Exception-Tag` — 26.3, our floor — by never searching result
/// bytes there and slicing the exception out of its tagged frame by declared
/// length (`vendor/clickhouse/PATCHES.md` §2). The search survives only for an
/// **untagged** server (pre-25.11, or a header-stripping proxy), where it is
/// the only signal there is; that arm is documented as forgeable.
///
/// This mapper needed no edit for that, and the reasoning below is why — it is
/// designed so a wrong code can never make anything worse:
///
/// 1. The BOUND is not the parse — `max_memory_usage` is enforced by
///    ClickHouse whether or not we read the code correctly, so a spoofed
///    code only changes the LABEL on a query that was stopped regardless.
/// 2. Fail-open: a MISSED 241 falls through the unchanged
///    [`ReadError::Clickhouse`] arm to the pre-#398 `500`.
/// 3. A FALSE 241 relabels an already-failing query `422`; no data is
///    served and no body content is exposed that the `500` did not.
/// 4. The code drives nothing but status and message — read dispatches
///    never retry (`ChClient::query_stream` has no retry loop; only
///    `execute` retries, and only for idempotent DDL), so a spoofed
///    *retryable* code cannot re-execute a memory-exhausting read.
/// 5. This function is a pure function of the already-parsed `code` field
///    and never re-inspects `message`, so it inherited #412's fix with no
///    edit here.
fn map_read_error(e: ChError, budget_bytes: u64, read_max_memory_bytes: u64) -> ReadError {
    if let ChError::Server { code, .. } = &e {
        if *code == CODE_TOO_MANY_BYTES {
            return ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
                budget_bytes,
                estimate: None,
            });
        }
        if *code == CODE_MEMORY_LIMIT_EXCEEDED {
            return ReadError::QueryTooBroad(TooBroadReason::LogqlReadMemory {
                budget_bytes: read_max_memory_bytes,
            });
        }
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
pub(in crate::logql) fn range_seconds(ns: u64) -> f64 {
    let sec = ns / 1_000_000_000;
    let nsec = ns % 1_000_000_000;
    // Both operands are exact in f64 — `sec <= MAX_DURATION_NS / 1e9`
    // (~9.2e9) and `nsec < 1e9` are both well under 2^53 — so the only
    // roundings are the two the reference performs. The reference's own
    // `Duration` is an int64 nanosecond count with the same ceiling, so the
    // two forms agree over the whole representable domain, not just here.
    sec as f64 + nsec as f64 / 1_000_000_000.0
}

pub(in crate::logql) fn apply_rate(n: f64, rate_window_ns: Option<u64>) -> f64 {
    match rate_window_ns {
        Some(window_ns) if window_ns > 0 => n / range_seconds(window_ns),
        _ => n,
    }
}

/// The sealed [`super::sql::MetricSource`] for a planned metric read
/// (issue #286).
///
/// The `.expect` is an in-crate construction invariant, the pattern this
/// module already uses. `plan::metric_plan` writes
/// `MetricPlan::{bucket_col, agg_expr}` out of
/// [`super::sql::MetricShape`], so
/// [`super::sql::MetricShape::from_columns`] is total for every plan this
/// crate builds; a `MetricPlan` assembled by hand out-of-crate with a
/// foreign column pair fails closed here instead of interpolating its text
/// into SQL.
fn metric_source(mp: &MetricPlan) -> super::sql::MetricSource<'_> {
    super::sql::MetricSource::new(
        &mp.table,
        mp.source_shape().expect(
            "invariant violated: plan::metric_plan writes MetricPlan::{bucket_col, agg_expr} \
             out of MetricShape (plan.rs), so MetricShape::from_columns is total for every \
             plan this crate builds — this MetricPlan carries a column pair no MetricShape \
             renders, so it was not built by metric_plan",
        ),
    )
}

/// Issue #286: returns [`CheckedLiteral`]s. The mint IS
/// [`super::escape::ch_string`], so "every service literal reaching a
/// `logql::sql` builder was escaped" stops being a fact about this function
/// and becomes one rustc holds up at the parameter.
fn distinct_escaped_services(meta: &HashMap<u64, StreamMetaRow>) -> Vec<CheckedLiteral> {
    let mut services: Vec<&str> = meta.values().map(|m| m.service.as_str()).collect();
    services.sort_unstable();
    services.dedup();
    services
        .into_iter()
        .map(super::predicate::literal)
        .collect()
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

/// Pops one operand off `run_metric_node`'s value stack.
#[inline]
fn pop_value(vals: &mut Vec<QueryResult>) -> QueryResult {
    match vals.pop() {
        Some(v) => v,
        // Unreachable: post-order guarantees a node's children have
        // already pushed their values.
        None => unreachable!("a post-order metric fold has its children's values on the stack"),
    }
}

#[cfg(test)]
mod tests {
    use super::super::charge::{AggCaps, PUSHDOWN_INSTANT_SLOT, group_entry_bytes};
    use super::super::labels::fnv1a64;
    use super::super::plan::{ClientAgg, ClientValue};
    use super::*;
    use crate::logql::testkit::*;

    /// Issue #398: a distinctive, non-default `reader.logql_read_max_memory_bytes`
    /// for the settings/mapper tests — deliberately unequal to any scan
    /// budget any test passes, so a mix-up between the two budgets shows up
    /// as a value mismatch rather than an accidental pass.
    const TEST_READ_MEM: u64 = 7_654_321;

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
        let svc = [super::super::predicate::literal("checkout")];
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

    #[test]
    fn code_307_maps_to_scan_budget_bytes() {
        let e = ChError::Server {
            code: 307,
            message: "Code: 307. DB::Exception: Limit for bytes to read exceeded".to_string(),
        };
        let err = map_read_error(e, 1024, TEST_READ_MEM);
        match err {
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes { budget_bytes, .. }) => {
                assert_eq!(budget_bytes, 1024);
            }
            other => panic!("expected QueryTooBroad(ScanBudgetBytes), got {other:?}"),
        }
    }

    /// Issue #382. `code_307_maps_to_scan_budget_bytes` above hands the
    /// mapper a `ChError::Server` it built itself, so it cannot see how the
    /// code is READ off the wire — and that is where the defect was. This
    /// starts from the `BadResponse` the `clickhouse` crate hands us when the
    /// server raised 307 AFTER it had already written output: a verbatim
    /// capture from 26.3.17.110 (`SELECT toString(number) AS v FROM
    /// numbers(100000000)` with `max_bytes_to_read=1100000`), whose
    /// `RowBinaryWithNamesAndTypes` column header precedes the exception.
    ///
    /// The `Code: N` prefix is the one the ADR 0007 vendored patch
    /// (`vendor/clickhouse/PATCHES.md`) takes from
    /// `X-ClickHouse-Exception-Code` and puts at byte 0. Without it the result
    /// bytes cannot be parsed for a code soundly, so the client gets 500
    /// `internal` instead of the 422 `query_too_broad` that tells them to
    /// narrow the query.
    #[test]
    fn a_307_raised_after_output_was_written_still_maps_to_scan_budget_bytes() {
        let body = "\u{1}\u{1}v\u{6}StringCode: 307. DB::Exception: Limit for rows or bytes to \
                    read exceeded, max bytes: 1.05 MiB, current bytes: 1.50 MiB: While executing \
                    NumbersRange. (TOO_MANY_BYTES) (version 26.3.17.110 (official build))";
        assert_eq!(body.len(), 209, "verbatim capture, pinned against an edit");
        let raw = ChError::from(clickhouse::error::Error::BadResponse(body.to_string()));
        assert!(matches!(
            map_read_error(raw, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));

        let patched = ChError::from(clickhouse::error::Error::BadResponse(format!(
            "Code: 307\n{body}"
        )));
        match map_read_error(patched, 1024, TEST_READ_MEM) {
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
        let err = map_read_error(e, 1024, TEST_READ_MEM);
        assert!(matches!(err, ReadError::Clickhouse(_)));
    }

    /// Issue #398 AC L3: server code 241 becomes the named LogQL read
    /// memory refusal (`422`), carrying the CONFIGURED ceiling — not the
    /// scan budget, which is a different number for a different resource.
    /// 307 and 158 keep their pre-#398 outcomes, so the new arm cannot
    /// have widened.
    #[test]
    fn map_read_error_maps_code_241_to_the_logql_read_memory_reason() {
        let e = ChError::Server {
            code: 241,
            message: "Code: 241. DB::Exception: Memory limit (for query) exceeded: would use \
                      5.02 MiB"
                .to_string(),
        };
        match map_read_error(e, 1024, TEST_READ_MEM) {
            ReadError::QueryTooBroad(TooBroadReason::LogqlReadMemory { budget_bytes }) => {
                assert_eq!(budget_bytes, TEST_READ_MEM);
                assert_ne!(
                    budget_bytes, 1024,
                    "the memory ceiling must never be reported as the byte scan budget"
                );
            }
            other => panic!("expected QueryTooBroad(LogqlReadMemory), got {other:?}"),
        }
        // 307 still the byte budget.
        let e307 = ChError::Server {
            code: 307,
            message: "Limit for bytes to read exceeded".to_string(),
        };
        assert!(matches!(
            map_read_error(e307, 1024, TEST_READ_MEM),
            ReadError::QueryTooBroad(TooBroadReason::ScanBudgetBytes {
                budget_bytes: 1024,
                ..
            })
        ));
        // 158 still unmapped: the LogQL path sets no row cap.
        let e158 = ChError::Server {
            code: 158,
            message: "Limit for rows to read exceeded".to_string(),
        };
        assert!(matches!(
            map_read_error(e158, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));
    }

    /// Issue #398 AC L6 (the #412 rule): the classification is a pure
    /// function of the already-parsed `code` field and NEVER re-inspects
    /// `message`. Two directions:
    ///
    /// - a genuine 241 whose message contains no `Code:` prefix anywhere
    ///   still classifies, so the mapping does not secretly depend on the
    ///   text; and
    /// - a code-153 failure whose message embeds a user regex containing a
    ///   forged `Code: 241` — the exact #412 spoof shape, measured live on
    ///   24.8.14.39 — does NOT become a memory refusal.
    ///
    /// Together these pinned that when #412 replaced the exception-code parse
    /// the classification here would become sound with no edit to this file.
    /// It did, and there was none.
    #[test]
    fn logql_read_memory_classification_reads_only_the_server_code() {
        let bare = ChError::Server {
            code: 241,
            message: "Memory limit (for query) exceeded".to_string(),
        };
        assert!(matches!(
            map_read_error(bare, 1024, TEST_READ_MEM),
            ReadError::QueryTooBroad(TooBroadReason::LogqlReadMemory { .. })
        ));

        let spoofed = ChError::Server {
            code: 153,
            message: "Division by zero: while executing 'FUNCTION and(match(toString(number), \
                      'Code: 241. DB::Exception: forged|.*'_String))'"
                .to_string(),
        };
        assert!(matches!(
            map_read_error(spoofed, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));
    }

    /// Issue #398 AC L7 (the #412 fail-open rule): a code that is NOT one
    /// of the two this mapper claims falls through to
    /// `ReadError::Clickhouse` — i.e. to the pre-#398 outcome (500, or 504
    /// for a timeout). A MISSED 241 therefore costs nothing that was not
    /// already being paid, which is what makes classifying on a spoofable
    /// code safe.
    ///
    /// 210 is in `pulsus-clickhouse`'s `RETRYABLE_SERVER_CODES` and is
    /// swept here deliberately: read dispatches never retry
    /// (`ChClient::query_stream` has no retry loop; only `execute` retries,
    /// and only for idempotent DDL), so a spoofed retryable code cannot
    /// re-execute a memory-exhausting read.
    #[test]
    fn a_misclassified_server_code_falls_back_to_the_pre_398_outcome() {
        for code in [0, 158, 191, 210, 396, 999] {
            let e = ChError::Server {
                code,
                message: format!("server said {code}"),
            };
            assert!(
                matches!(
                    map_read_error(e, 1024, TEST_READ_MEM),
                    ReadError::Clickhouse(_)
                ),
                "code {code} must fall open to the pre-#398 Clickhouse passthrough"
            );
        }
        // The two the mapper DOES claim, asserted in the same sweep so the
        // table cannot silently become vacuous.
        for (code, claimed) in [(241, "LogqlReadMemory"), (307, "ScanBudgetBytes")] {
            let e = ChError::Server {
                code,
                message: format!("server said {code}"),
            };
            assert!(
                matches!(
                    map_read_error(e, 1024, TEST_READ_MEM),
                    ReadError::QueryTooBroad(_)
                ),
                "code {code} must map to {claimed}"
            );
        }
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
        let s = read_query_settings(1024, TEST_READ_MEM);
        assert_eq!(s.get("max_bytes_to_read"), Some("1024"));
        assert_eq!(s.get("read_overflow_mode"), Some("throw"));
        assert_eq!(
            s.get("max_query_size"),
            Some(crate::querytext::MAX_QUERY_TEXT_BYTES.to_string().as_str())
        );
    }

    /// Issue #399 AC16: the three label-discovery scans carrying the
    /// activity semi-join gate `distributed_product_mode='local'` on
    /// clustered mode alone, and carry the byte budget through unchanged
    /// in both states. Mirrors
    /// `metrics::exec::fallback_fetch_settings_*` (#136) and
    /// `traces::exec::metrics_settings_carry_the_set_limits_and_gate_the_local_product_mode`
    /// (#59).
    ///
    /// The complement is asserted here too, and it is the half that
    /// matters: `/series`' own activity query dispatches with
    /// `budget_settings`, NOT this — it nests no distributed table, so it
    /// must not carry the setting.
    #[test]
    fn activity_settings_gate_the_local_product_mode() {
        let unclustered = activity_query_settings(4096, TEST_READ_MEM, false);
        assert_eq!(unclustered.get("max_bytes_to_read"), Some("4096"));
        assert_eq!(unclustered.get("read_overflow_mode"), Some("throw"));
        assert_eq!(
            unclustered.get("distributed_product_mode"),
            None,
            "the local-product rewrite is clustered-only"
        );

        let clustered = activity_query_settings(4096, TEST_READ_MEM, true);
        assert_eq!(clustered.get("max_bytes_to_read"), Some("4096"));
        assert_eq!(clustered.get("read_overflow_mode"), Some("throw"));
        assert_eq!(clustered.get("distributed_product_mode"), Some("local"));
        assert_eq!(
            clustered.get("max_query_size"),
            Some(crate::querytext::MAX_QUERY_TEXT_BYTES.to_string().as_str()),
            "the raised query-text cap survives the clustered branch"
        );

        // `/series` dispatches its activity query with the plain budget
        // settings; those must never grow the setting by accident.
        assert_eq!(
            read_query_settings(4096, TEST_READ_MEM).get("distributed_product_mode"),
            None,
            "the /series activity query must not carry distributed_product_mode"
        );
    }

    /// Issue #398 AC L1: every LogQL read carries the per-query memory
    /// ceiling and refuses to SPILL. `max_bytes_before_external_group_by =
    /// 0` is the throw-not-spill half — with spilling enabled a wide
    /// aggregation would grind through disk instead of failing loud, which
    /// is the silently-slow outcome this repo rejects (the shipped
    /// `traces::exec::generator_settings` precedent, issue #57, sets the
    /// identical pair).
    ///
    /// The three pre-existing keys must survive untouched: the whole point
    /// of putting the ceiling in this one function is that it ADDS to the
    /// budget every dispatch site already carried.
    #[test]
    fn read_query_settings_carry_the_memory_ceiling_and_throw_not_spill() {
        let s = read_query_settings(1024, 4096);
        assert_eq!(s.get("max_memory_usage"), Some("4096"));
        assert_eq!(s.get("max_bytes_before_external_group_by"), Some("0"));
        // The pre-#398 trio, unchanged.
        assert_eq!(s.get("max_bytes_to_read"), Some("1024"));
        assert_eq!(s.get("read_overflow_mode"), Some("throw"));
        assert_eq!(
            s.get("max_query_size"),
            Some(crate::querytext::MAX_QUERY_TEXT_BYTES.to_string().as_str())
        );
    }

    /// Issue #398 AC L2: EVERY settings constructor on this path carries
    /// the ceiling, and the byte budget never leaks into it.
    ///
    /// The three engine methods (`budget_settings`, `activity_settings`,
    /// `paging_settings`) are verbatim delegations to the two free
    /// functions asserted here — `budget_settings` is
    /// `read_query_settings(scan_budget, read_max_memory)`,
    /// `activity_settings` is `activity_query_settings(…)`, and
    /// `paging_settings` is `read_query_settings(remaining, …)` plus
    /// `wait_end_of_query`. They take `&self` and so need a live
    /// `ChClient`; the free functions exist precisely so the settings
    /// decision is provable without one (the same reason
    /// `activity_query_settings` and `scan_budget_spent` were split out).
    /// The engine-level completeness claim — that no dispatch site
    /// bypasses these constructors — is proved live instead, by
    /// `every_logql_engine_query_carries_the_memory_ceiling` in
    /// `tests/query_log_gates.rs`.
    #[test]
    fn every_logql_settings_constructor_carries_the_memory_ceiling() {
        const MEM: u64 = 4096;
        const BUDGET: u64 = 1_000_000;
        // `budget_settings`' body.
        let budget = read_query_settings(BUDGET, MEM);
        // `activity_settings`' body, both branches.
        let activity_local = activity_query_settings(BUDGET, MEM, false);
        let activity_dist = activity_query_settings(BUDGET, MEM, true);
        for (name, s) in [
            ("budget_settings", &budget),
            ("activity_settings(unclustered)", &activity_local),
            ("activity_settings(clustered)", &activity_dist),
        ] {
            assert_eq!(
                s.get("max_memory_usage"),
                Some(MEM.to_string().as_str()),
                "{name} must carry the memory ceiling"
            );
            assert_eq!(
                s.get("max_bytes_before_external_group_by"),
                Some("0"),
                "{name} must throw rather than spill"
            );
        }

        // `paging_settings(remaining)`' body: the DECREMENTED budget lands
        // in `max_bytes_to_read` only. The memory ceiling is per query and
        // is never decremented, so a late page carries the same ceiling as
        // the first — if the decrement ever leaked into it, page N would
        // silently run with a smaller memory bound than page 1.
        const REMAINING: u64 = 777;
        let paging = read_query_settings(REMAINING, MEM).set("wait_end_of_query", 1);
        assert_eq!(paging.get("max_bytes_to_read"), Some("777"));
        assert_eq!(
            paging.get("max_memory_usage"),
            Some(MEM.to_string().as_str())
        );
        assert_eq!(paging.get("max_bytes_before_external_group_by"), Some("0"));
        assert_eq!(paging.get("wait_end_of_query"), Some("1"));
    }

    /// Issue #133: the read settings carry the byte scan budget VERBATIM
    /// at the accepted minimum (1) and at the maximum config-accepted
    /// `reader.logql_scan_budget_bytes` — never ClickHouse's `0`
    /// (unlimited) sentinel.
    #[test]
    fn read_query_settings_carry_the_budget_verbatim_at_the_accepted_min_and_ceiling() {
        assert_eq!(
            read_query_settings(1, TEST_READ_MEM).get("max_bytes_to_read"),
            Some("1")
        );
        let cap = pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING;
        let s = read_query_settings(cap, TEST_READ_MEM);
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
    fn worst_case_envelope() -> (
        Vec<u64>,
        Vec<CheckedLiteral>,
        Vec<super::super::predicate::CheckedFragment>,
    ) {
        let fps: Vec<u64> =
            std::iter::repeat_n(u64::MAX, super::super::params::DEFAULT_MAX_STREAMS).collect();
        // 64-byte literals (`'` + 62 chars + `'`). Issue #286: minted, not
        // hand-written — `predicate::literal` IS `ch_string`, and a
        // 62-digit value escapes to nothing, so the rendered bytes are the
        // same 64 as before.
        let services: Vec<CheckedLiteral> = (0..10_000)
            .map(|i| super::super::predicate::literal(&format!("{i:062}")))
            .collect();
        // 16 × 64 KiB pre-rendered predicates ≈ 1 MiB, a generous multiple
        // of any realistic compiled line-filter chain.
        //
        // Issue #286: a raw 65,536-byte string is no longer constructible
        // as a fragment, so the envelope is MINTED at exactly that size and
        // the size is asserted rather than assumed. `-` is neither `%`, `_`
        // nor `\`, so neither `ch_like_contains` nor `ch_string` escapes
        // anything: a `|=` filter over N dashes renders
        // `body LIKE '%<N dashes>%'`, which is `10 + 2 + N + 2` bytes
        // (issue #450). N = 65,522 gives exactly 65,536.
        let filter = super::super::predicate::line_filter(&pulsus_logql::LineFilter {
            op: pulsus_logql::LineFilterOp::Contains,
            value: "-".repeat(65_522),
            value_is_ip: false,
            or_matches: Vec::new(),
        })
        .expect("a Contains filter compiles no regex");
        assert_eq!(
            filter.as_sql().len(),
            65_536,
            "the envelope's per-filter size is the pre-#286 figure, exactly"
        );
        let line_filters: Vec<super::super::predicate::CheckedFragment> =
            std::iter::repeat_n(filter, 16).collect();
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
                    opts: ResponseOptions::default(),
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
        assert!(matches!(
            map_read_error(e, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));
    }

    #[test]
    fn a_timeout_is_never_reinterpreted_as_a_budget_error() {
        let e = ChError::Timeout("deadline".to_string());
        assert!(matches!(
            map_read_error(e, 1024, TEST_READ_MEM),
            ReadError::Clickhouse(_)
        ));
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
    // -----------------------------------------------------------------
    // Issue #249 — the SQL-pushdown instant path's structured-metadata
    // re-grouping.
    //
    // Fixture: the b25 fixture, captured from grafana/loki:3.7.4
    // (`{"version":"3.7.4","revision":"b318f282","branch":"release-3.7.x"}`)
    // with `limits_config.discover_log_levels: false`, base timestamp
    // 1786440799000000000. Stream A `{app="x", fp="1", service_name="sm"}`
    // holds `alpha`+sm{n=10,trace=a} at 10s, `beta`+sm{n=20,trace=bb} at
    // 20s and `gamma` with no metadata at 30s; stream B
    // `{app="y", fp="2", service_name="sm"}` holds `delta` with
    // sm{app=SMVAL,trace=a} at 40s.
    // -----------------------------------------------------------------

    /// The two streams of the b25 fixture, as this crate's hydrated meta.
    fn sm_meta() -> HashMap<u64, StreamMetaRow> {
        let mut m = HashMap::new();
        m.insert(
            1,
            StreamMetaRow {
                fingerprint: 1,
                service: "sm".to_string(),
                labels: r#"{"app":"x","fp":"1","service_name":"sm"}"#.to_string(),
            },
        );
        m.insert(
            2,
            StreamMetaRow {
                fingerprint: 2,
                service: "sm".to_string(),
                labels: r#"{"app":"y","fp":"2","service_name":"sm"}"#.to_string(),
            },
        );
        m
    }

    /// The fixture's four entries as raw scan rows.
    fn sm_scan_rows() -> Vec<MetricScanRow> {
        let row = |fp, ts, body: &str, sm: &str| MetricScanRow {
            fingerprint: fp,
            timestamp_ns: ts,
            body: body.to_string(),
            structured_metadata: sm.to_string(),
        };
        vec![
            row(1, 10_000_000_000, "alpha", r#"{"n":"10","trace":"a"}"#),
            row(1, 20_000_000_000, "beta", r#"{"n":"20","trace":"bb"}"#),
            row(1, 30_000_000_000, "gamma", ""),
            row(2, 40_000_000_000, "delta", r#"{"app":"SMVAL","trace":"a"}"#),
        ]
    }

    /// The SAME entries as `metric_instant` would return them — one row
    /// per distinct `(fingerprint, structured_metadata)`, with `n` the
    /// server-side `count()`.
    fn sm_instant_rows() -> Vec<MetricInstantRow> {
        let row = |fp, n, sm: &str| MetricInstantRow {
            fingerprint: fp,
            n,
            structured_metadata: sm.to_string(),
        };
        vec![
            row(1, 1, r#"{"n":"10","trace":"a"}"#),
            row(1, 1, r#"{"n":"20","trace":"bb"}"#),
            row(1, 1, ""),
            row(2, 1, r#"{"app":"SMVAL","trace":"a"}"#),
        ]
    }

    /// `bytes_over_time`'s server-side `sum(length(body))` over the same
    /// groups: `alpha`/`gamma`/`delta` are 5 bytes, `beta` is 4.
    fn sm_instant_rows_bytes() -> Vec<MetricInstantRow> {
        let row = |fp, n, sm: &str| MetricInstantRow {
            fingerprint: fp,
            n,
            structured_metadata: sm.to_string(),
        };
        vec![
            row(1, 5, r#"{"n":"10","trace":"a"}"#),
            row(1, 4, r#"{"n":"20","trace":"bb"}"#),
            row(1, 5, ""),
            row(2, 5, r#"{"app":"SMVAL","trace":"a"}"#),
        ]
    }

    fn pushdown_series(
        rows: &[MetricInstantRow],
        rate_window_ns: Option<u64>,
    ) -> Vec<(Vec<(String, String)>, u64)> {
        let meta = sm_meta();
        let mut g = PushdownInstantGroups::new(&meta, AggCaps::DEFAULT);
        for r in rows {
            g.push_row(r).expect("under the cap");
        }
        let mut out: Vec<(Vec<(String, String)>, u64)> = g
            .finish(rate_window_ns)
            .into_iter()
            .map(|s| {
                let mut l = s.labels;
                l.sort();
                (l, s.value.to_bits())
            })
            .collect();
        out.sort();
        out
    }

    fn client_series(
        query: &str,
        value: ClientValue,
        op: RangeAggOp,
        rate_window_ns: Option<u64>,
    ) -> Vec<(Vec<(String, String)>, u64)> {
        let client = ClientAgg {
            pipeline: parse_log_pipeline(query),
            value,
            range_op: op,
            param: None,
            absent_labels: vec![],
            grouping: None,
        };
        let compiled =
            super::super::pipeline::CompiledPipeline::compile(&client.pipeline).expect("compiles");
        let meta = sm_meta();
        // The instant window the pushdown query evaluates: `(t-5m, t]` at
        // t = 60s, which covers all four entries.
        let window = crate::logql::window::ClientWindow::Instant {
            start_ns: 60_000_000_000 - 300_000_000_000,
            end_ns: 60_000_000_000,
        };
        let res = super::super::client_agg::run_client_agg_rows(
            &sm_scan_rows(),
            &compiled,
            &meta,
            &client,
            window,
            rate_window_ns,
        )
        .expect("served");
        let QueryResult::Vector(items) = res else {
            panic!("an instant metric query yields a vector");
        };
        let mut out: Vec<(Vec<(String, String)>, u64)> = items
            .into_iter()
            .map(|s| {
                let mut l = s.labels;
                l.sort();
                (l, s.value.to_bits())
            })
            .collect();
        out.sort();
        out
    }

    fn parse_log_pipeline(query: &str) -> Vec<Stage> {
        let Expr::Log(le) = pulsus_logql::parse(query).expect("parses") else {
            panic!("expected a log expression");
        };
        le.pipeline
    }

    fn lbl(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, x)| (k.to_string(), x.to_string()))
            .collect();
        v.sort();
        v
    }

    /// Issue #249 AC-6 — **the two instant paths cannot disagree, and both
    /// match the reference.**
    ///
    /// The same fixture is evaluated through (a) the pushdown re-grouping
    /// and (b) `run_client_agg_rows`, and the sorted `(labels, value)`
    /// lists are compared at `f64::to_bits`. Joint wrongness cannot pass:
    /// arm (a) is ALSO compared against the container's captured answer.
    #[test]
    fn pushdown_and_client_agree_with_the_reference_on_metadata_series() {
        // --- count_over_time, captured `ac6-count-instant` -------------
        let want_count = vec![
            (
                lbl(&[
                    ("app", "x"),
                    ("fp", "1"),
                    ("n", "10"),
                    ("service_name", "sm"),
                    ("trace", "a"),
                ]),
                1.0f64.to_bits(),
            ),
            (
                lbl(&[
                    ("app", "x"),
                    ("fp", "1"),
                    ("n", "20"),
                    ("service_name", "sm"),
                    ("trace", "bb"),
                ]),
                1.0f64.to_bits(),
            ),
            (
                lbl(&[("app", "x"), ("fp", "1"), ("service_name", "sm")]),
                1.0f64.to_bits(),
            ),
            (
                lbl(&[
                    ("app", "y"),
                    ("app_extracted", "SMVAL"),
                    ("fp", "2"),
                    ("service_name", "sm"),
                    ("trace", "a"),
                ]),
                1.0f64.to_bits(),
            ),
        ];
        let mut want_count_sorted = want_count.clone();
        want_count_sorted.sort();
        let a = pushdown_series(&sm_instant_rows(), None);
        assert_eq!(a, want_count_sorted, "pushdown must match the container");
        let b = client_series(
            r#"{service_name="sm"}"#,
            ClientValue::Count,
            RangeAggOp::CountOverTime,
            None,
        );
        assert_eq!(a, b, "the two instant paths must agree, bit for bit");

        // --- rate, captured `ac6-rate-instant` -------------------------
        // One line per series over a 300s window; the container answered
        // 0.0033333333333333335 for every one of the four.
        let rate = 0.0033333333333333335f64.to_bits();
        let a = pushdown_series(&sm_instant_rows(), Some(300_000_000_000));
        assert!(
            a.iter().all(|(_, v)| *v == rate),
            "every rate series is the container's 0.0033333333333333335: {a:?}"
        );
        assert_eq!(
            a.iter().map(|(l, _)| l.clone()).collect::<Vec<_>>(),
            want_count_sorted
                .iter()
                .map(|(l, _)| l.clone())
                .collect::<Vec<_>>(),
        );
        let b = client_series(
            r#"{service_name="sm"}"#,
            ClientValue::Count,
            RangeAggOp::Rate,
            Some(300_000_000_000),
        );
        assert_eq!(a, b, "the two instant paths must agree on rate");

        // --- bytes_over_time, captured `ac6-bytes-instant` -------------
        let want_bytes = {
            let mut v = vec![
                (
                    lbl(&[
                        ("app", "x"),
                        ("fp", "1"),
                        ("n", "10"),
                        ("service_name", "sm"),
                        ("trace", "a"),
                    ]),
                    5.0f64.to_bits(),
                ),
                (
                    lbl(&[
                        ("app", "x"),
                        ("fp", "1"),
                        ("n", "20"),
                        ("service_name", "sm"),
                        ("trace", "bb"),
                    ]),
                    4.0f64.to_bits(),
                ),
                (
                    lbl(&[("app", "x"), ("fp", "1"), ("service_name", "sm")]),
                    5.0f64.to_bits(),
                ),
                (
                    lbl(&[
                        ("app", "y"),
                        ("app_extracted", "SMVAL"),
                        ("fp", "2"),
                        ("service_name", "sm"),
                        ("trace", "a"),
                    ]),
                    5.0f64.to_bits(),
                ),
            ];
            v.sort();
            v
        };
        let a = pushdown_series(&sm_instant_rows_bytes(), None);
        assert_eq!(a, want_bytes, "pushdown bytes must match the container");
        let b = client_series(
            r#"{service_name="sm"}"#,
            ClientValue::Bytes,
            RangeAggOp::BytesOverTime,
            None,
        );
        assert_eq!(a, b, "the two instant paths must agree on bytes");
    }

    /// Issue #249 — the pushdown path sums the server's PARTIAL counts
    /// before applying the rate divisor, which is what makes it exact.
    /// Two rows of the same fingerprint whose metadata merges to the SAME
    /// final label set (here: metadata that is entirely reserved-name and
    /// therefore inert) must produce ONE series carrying their sum.
    #[test]
    fn pushdown_sums_partials_before_the_rate_divisor() {
        // `1` and `8` over a 300s window: chosen because `1/300 + 8/300`
        // and `9/300` are DIFFERENT doubles (0.030000000000000002 vs
        // 0.03), so this fixture can tell the two orders apart. Most
        // small pairs cannot — which is exactly why the pair is picked
        // rather than assumed.
        let rows = vec![
            MetricInstantRow {
                fingerprint: 1,
                n: 1,
                structured_metadata: String::new(),
            },
            MetricInstantRow {
                fingerprint: 1,
                n: 8,
                structured_metadata: r#"{"__error_details__":"det"}"#.to_string(),
            },
        ];
        assert_ne!(
            (1.0f64 / 300.0 + 8.0f64 / 300.0).to_bits(),
            (9.0f64 / 300.0).to_bits(),
            "the fixture must be able to distinguish the two orders at all"
        );
        let a = pushdown_series(&rows, Some(300_000_000_000));
        assert_eq!(
            a.len(),
            1,
            "an inert merge is the same output series: {a:?}"
        );
        // `9/300` — NOT `1/300 + 8/300`, which is a different double.
        assert_eq!(a[0].1, (9.0f64 / 300.0).to_bits());
        assert_ne!(
            a[0].1,
            (1.0f64 / 300.0 + 8.0f64 / 300.0).to_bits(),
            "per-partial division would be a DIFFERENT double — that is why \
             the sum happens first"
        );
    }

    /// Issue #249 — a structured-metadata `__error__` fails the pushdown
    /// query, with the same named error the client paths raise. Captured:
    /// the container answers 400 to `count_over_time({service_name="sm3"}[5m])`
    /// over an entry whose metadata carries `__error__`, with NO pipeline
    /// in the query.
    #[test]
    fn a_metadata_error_fails_the_pushdown_query_too() {
        let meta = sm_meta();
        let mut g = PushdownInstantGroups::new(&meta, AggCaps::DEFAULT);
        let err = g
            .push_row(&MetricInstantRow {
                fingerprint: 1,
                n: 1,
                structured_metadata: r#"{"__error__":"boom"}"#.to_string(),
            })
            .expect_err("a surviving __error__ fails the query");
        assert!(
            matches!(err, ReadError::MetricPipelineError { ref error_type, .. } if error_type == "boom"),
            "{err:?}"
        );
    }

    /// Issue #249 AC-7 — **the pushdown path is bounded BEFORE it
    /// allocates, and both parts are sized.**
    ///
    /// A fixture exceeding `AggCaps::group_bytes` refuses with the same
    /// named `QueryTooBroad` the client paths raise — never an OOM, never a
    /// truncation — and neither the map nor the output vector ever held the
    /// refused group.
    #[test]
    fn the_pushdown_regrouping_refuses_before_it_retains_the_breaching_group() {
        let meta = sm_meta();
        let tiny = AggCaps {
            group_bytes: 4096,
            ..AggCaps::DEFAULT
        };
        let mut g = PushdownInstantGroups::new(&meta, tiny);
        let fat = "v".repeat(64 * 1024);
        let err = g
            .push_row(&MetricInstantRow {
                fingerprint: 1,
                n: 1,
                structured_metadata: format!(r#"{{"big":"{fat}"}}"#),
            })
            .expect_err("the group must be refused");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { .. })
            ),
            "the pushdown path must raise the client paths' named 422, got {err:?}"
        );
        // The charge PRECEDES the retention, so nothing was kept.
        assert_eq!(g.group_count(), 0, "the map never held the refused group");
        assert_eq!(g.charged_bytes(), 0, "a refused charge is not accumulated");
        assert!(
            g.finish(None).is_empty(),
            "and the output vector never held it either"
        );
    }

    /// Issue #249 AC-7's second half — `PUSHDOWN_INSTANT_SLOT` prices BOTH
    /// live containers, and `group_entry_bytes` is strictly monotone in
    /// each of the two parts the criterion names.
    #[test]
    fn the_pushdown_slot_prices_both_containers_and_each_part_is_observable() {
        assert_eq!(
            PUSHDOWN_INSTANT_SLOT,
            size_of::<(String, InstantSeries)>() + size_of::<InstantSeries>(),
            "the slot must price the map entry AND the vector element"
        );
        let base = vec![("a".to_string(), "1".to_string())];
        let plus_pair = vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ];
        let k = "{a=\"1\"}";
        let longer_k = "{aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=\"1\"}";
        assert!(
            group_entry_bytes(k, &plus_pair, PUSHDOWN_INSTANT_SLOT)
                > group_entry_bytes(k, &base, PUSHDOWN_INSTANT_SLOT),
            "adding a label pair must raise the charge"
        );
        assert!(
            group_entry_bytes(longer_k, &base, PUSHDOWN_INSTANT_SLOT)
                > group_entry_bytes(k, &base, PUSHDOWN_INSTANT_SLOT),
            "lengthening the group key must raise the charge"
        );
    }
}
