//! `LogQlEngine` — executes a [`super::plan::Plan`] against ClickHouse via
//! `ChClient`, injects the scan budget, maps overflow codes to
//! [`ReadError::QueryTooBroad`], and finishes vector aggregations in Rust
//! (docs/schemas.md §3.2: "the engine maps fingerprints to `service` and
//! finishes the `sum by`"). Deliberately **not** snapshot-tested — SQL
//! generation itself is `plan`/`sql`'s job and is tested there without a
//! database; this module's own test coverage is the error-mapping unit
//! tests (architect plan amendment §4).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, ChRow, ChRowStream, QuerySettings};
use pulsus_logql::{BinOp, Expr, Grouping, GroupingKind, RangeAggOp, VectorAggOp};

use super::error::{ReadError, TooBroadReason};
use super::explain::PlanExplain;
use super::params::{Direction, PlanCtx, QueryParams, QuerySpec, TimeBounds};
use super::pipeline::{CompiledPipeline, ERROR_LABEL, MetricRun};
use super::plan::{self, ClientAgg, ClientValue, MetricNode, MetricPlan, Plan, StreamsPlan};
use super::rows::{
    LabelNameRow, LabelValueRow, MetricBucketRow, MetricInstantRow, SampleRow, StreamMetaRow,
    StreamRow,
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

/// The engine's raw result — #13 encodes this into the query-API JSON
/// envelope (out of scope here per the architect plan). `Scalar` is issue
/// #31's addition (`pulsus_promql::QueryValue::Scalar` — a bare-number
/// PromQL expression, e.g. `1 + 1`, evaluated with no series involved);
/// LogQL never produces it.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Streams(Vec<StreamResult>),
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
                .map(QueryResult::Streams),
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
                let result = self.run_streams_inner(&sp, Some(&mut explain)).await?;
                Ok((QueryResult::Streams(result), explain))
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
            .await
            .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
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
            .await
            .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
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
    async fn query_stream<'a, R: ChRow>(
        &'a self,
        sql: &str,
        settings: &QuerySettings,
    ) -> Result<ChRowStream<'a, R>, ChError> {
        let sql = escape_query_placeholders(sql);
        self.client.query_stream::<R>(&sql, settings).await
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
            .await
            .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
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
            .await
            .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
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
        QuerySettings::new()
            .set("max_bytes_to_read", self.config.scan_budget_bytes)
            .set("read_overflow_mode", "throw")
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
    async fn run_streams_inner(
        &self,
        sp: &StreamsPlan,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<StreamResult>, ReadError> {
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
            let mut by_fp: HashMap<u64, Vec<(i64, String)>> = HashMap::new();
            let mut stream = self
                .query_stream::<SampleRow>(&sql, &self.budget_settings())
                .await
                .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
                by_fp
                    .entry(row.fingerprint)
                    .or_default()
                    .push((row.timestamp_ns, row.body));
            }

            return Ok(by_fp
                .into_iter()
                .filter_map(|(fp, entries)| {
                    meta.get(&fp).map(|m| StreamResult {
                        fingerprint: fp,
                        service: m.service.clone(),
                        labels_json: m.labels.clone(),
                        entries,
                    })
                })
                .collect());
        }

        // Transform/fan-out paths: collect rows in arrival order (stage 3
        // orders globally by timestamp in the requested direction, so
        // arrival order IS the response order — the global `result_limit`
        // truncation below depends on it). Bounded by `scan_limit`.
        let mut rows: Vec<SampleRow> = Vec::new();
        let mut stream = self
            .query_stream::<SampleRow>(&sql, &self.budget_settings())
            .await
            .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
        while let Some(row) = stream.next().await {
            rows.push(row.map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?);
        }

        Ok(run_pipeline_rows(rows, &compiled, &meta, sp.result_limit))
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
                .await
                .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
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
                .await
                .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
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
        let mut chunk: Vec<SampleRow> = Vec::with_capacity(CLIENT_AGG_CHUNK_ROWS);
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule) — no other query
            // runs inside this block, and the lease ends at the brace.
            let mut stream = self
                .query_stream::<SampleRow>(&sql, &self.budget_settings())
                .await
                .map_err(|e| map_read_error(e, self.config.scan_budget_bytes))?;
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
                    lhs,
                    rhs,
                } => {
                    let l = self.run_metric_node(lhs, explain.as_deref_mut()).await?;
                    let r = self.run_metric_node(rhs, explain).await?;
                    combine_binary(*op, *return_bool, l, r)
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
    // Base labels parsed once per fingerprint, not per row.
    let mut base_labels: HashMap<u64, Vec<(String, String)>> = HashMap::new();
    for (fp, m) in meta {
        base_labels.insert(*fp, parse_flat_labels(&m.labels));
    }

    let fan_out = compiled.mutates_labels();
    let mut survivors = 0u32;
    // Transform path groups by source fingerprint; fan-out groups by the
    // canonical rendered labels JSON (sorted keys — it doubles as the
    // equality key). Two maps instead of a shared key enum so the
    // fan-out entry API can reuse its own `String` key without a
    // per-row clone (review round 2, finding 1); the fan-out value holds
    // only the per-group accumulator, and the map-owned key MOVES into
    // `StreamResult.labels_json` at final collection — never cloned out
    // of the entry, so high-cardinality fan-out (every row a new group)
    // pays no per-group key duplication either (review round 3).
    let mut fp_groups: HashMap<u64, StreamResult> = HashMap::new();
    let mut label_groups: HashMap<String, FanOutGroup> = HashMap::new();
    // One label scratch reused across every row (issue #72 review round
    // 1, finding 3): all rows share the loop-invariant lifetime of
    // `rows`/`base_labels`/`compiled`, so `run_into` clears and refills
    // the same vector — zero per-row label-vector allocations on the
    // dropped-row path.
    let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();

    for row in &rows {
        if survivors >= result_limit {
            break;
        }
        let Some(m) = meta.get(&row.fingerprint) else {
            continue;
        };
        let base = &base_labels[&row.fingerprint];
        let Some(line) = compiled.run_into(&row.body, base, &mut scratch) else {
            continue;
        };
        survivors += 1;

        if fan_out {
            // Render the canonical JSON DIRECTLY from the sorted borrowed
            // scratch (round-2 finding 1: no owned intermediate label
            // vector, no second clone at render time). Per surviving row
            // this costs exactly the `labels_json` string (needed as the
            // group key either way) + the owned output line; the
            // `StreamResult` fields materialize once per NEW group only.
            scratch.sort_unstable();
            let labels_json = render_labels_json_sorted(&scratch);
            let entry = (row.timestamp_ns, line.into_owned());
            match label_groups.entry(labels_json) {
                std::collections::hash_map::Entry::Occupied(e) => {
                    e.into_mut().entries.push(entry);
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    let service = scratch
                        .iter()
                        .find(|(k, _)| k == "service_name")
                        .map(|(_, v)| v.to_string())
                        .unwrap_or_else(|| m.service.clone());
                    let fingerprint = fnv1a64(e.key().as_bytes());
                    e.insert(FanOutGroup {
                        fingerprint,
                        service,
                        entries: vec![entry],
                    });
                }
            }
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
    }

    fp_groups
        .into_values()
        .chain(
            label_groups
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
    fn push_rows(&mut self, rows: &[SampleRow]) -> Result<(), ReadError> {
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
                match self.label_groups.entry(key) {
                    std::collections::hash_map::Entry::Occupied(e) => &mut e.into_mut().1,
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let labels: LabelSet = scratch
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();
                        &mut e.insert((labels, BTreeMap::new())).1
                    }
                }
            } else {
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
    rows: &[SampleRow],
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

/// Combines two evaluated metric results (issue M6-10). Scope per the
/// adjudication: vector⊗scalar in BOTH orientations, identical-full-
/// label-set one-to-one vector⊗vector, `bool`, and the `and`/`or`/
/// `unless` set operations; matrices align per shared step. `pub` for
/// the hermetic golden suite.
pub fn combine_binary(
    op: BinOp,
    return_bool: bool,
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
        (QueryResult::Vector(l), QueryResult::Vector(r)) => {
            Ok(QueryResult::Vector(combine_vectors(op, return_bool, l, r)))
        }
        (QueryResult::Matrix(l), QueryResult::Matrix(r)) => {
            Ok(QueryResult::Matrix(combine_matrices(op, return_bool, l, r)))
        }
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

/// Identical-full-label-set one-to-one vector matching (the adjudicated
/// M6-10 scope; `on`/`ignoring` grouping is the deferred follow-up).
fn combine_vectors(
    op: BinOp,
    return_bool: bool,
    lhs: Vec<VectorSample>,
    rhs: Vec<VectorSample>,
) -> Vec<VectorSample> {
    let rhs_by_labels: HashMap<LabelSet, f64> =
        rhs.iter().map(|s| (s.labels.clone(), s.value)).collect();
    if is_set_op(op) {
        return match op {
            BinOp::And => lhs
                .into_iter()
                .filter(|s| rhs_by_labels.contains_key(&s.labels))
                .collect(),
            BinOp::Unless => lhs
                .into_iter()
                .filter(|s| !rhs_by_labels.contains_key(&s.labels))
                .collect(),
            BinOp::Or => {
                let lhs_labels: std::collections::HashSet<LabelSet> =
                    lhs.iter().map(|s| s.labels.clone()).collect();
                let mut out = lhs;
                out.extend(rhs.into_iter().filter(|s| !lhs_labels.contains(&s.labels)));
                out
            }
            _ => unreachable!("is_set_op gates the arm"),
        };
    }
    lhs.into_iter()
        .filter_map(|s| {
            let r = *rhs_by_labels.get(&s.labels)?;
            let value = if op.is_comparison() {
                let hit = compare(op, s.value, r);
                if return_bool {
                    if hit { 1.0 } else { 0.0 }
                } else if hit {
                    s.value
                } else {
                    return None;
                }
            } else {
                arith(op, s.value, r)
            };
            Some(VectorSample {
                labels: s.labels,
                value,
            })
        })
        .collect()
}

/// Matrix⊗matrix: the vector semantics applied per shared step (plan v1:
/// "Range (matrix) binary ops align per shared step").
fn combine_matrices(
    op: BinOp,
    return_bool: bool,
    lhs: Vec<MatrixSeries>,
    rhs: Vec<MatrixSeries>,
) -> Vec<MatrixSeries> {
    let rhs_by_labels: HashMap<LabelSet, BTreeMap<i64, f64>> = rhs
        .iter()
        .map(|s| (s.labels.clone(), s.points.iter().copied().collect()))
        .collect();
    if is_set_op(op) {
        return match op {
            // `a and b`: an lhs point survives iff rhs has a same-labels
            // point at the same step.
            BinOp::And => lhs
                .into_iter()
                .filter_map(|s| {
                    let r = rhs_by_labels.get(&s.labels)?;
                    let points: Vec<(i64, f64)> = s
                        .points
                        .into_iter()
                        .filter(|(ts, _)| r.contains_key(ts))
                        .collect();
                    (!points.is_empty()).then_some(MatrixSeries {
                        labels: s.labels,
                        points,
                    })
                })
                .collect(),
            // `a unless b`: an lhs point survives iff rhs has NO
            // same-labels point at that step.
            BinOp::Unless => lhs
                .into_iter()
                .filter_map(|s| {
                    let points: Vec<(i64, f64)> = match rhs_by_labels.get(&s.labels) {
                        None => s.points,
                        Some(r) => s
                            .points
                            .into_iter()
                            .filter(|(ts, _)| !r.contains_key(ts))
                            .collect(),
                    };
                    (!points.is_empty()).then_some(MatrixSeries {
                        labels: s.labels,
                        points,
                    })
                })
                .collect(),
            // `a or b`: all lhs points, plus rhs points at steps where
            // no same-labels lhs point exists.
            BinOp::Or => {
                let lhs_by_labels: HashMap<LabelSet, BTreeSet<i64>> = lhs
                    .iter()
                    .map(|s| {
                        (
                            s.labels.clone(),
                            s.points.iter().map(|(ts, _)| *ts).collect(),
                        )
                    })
                    .collect();
                let mut out = lhs;
                for s in rhs {
                    let extra: Vec<(i64, f64)> = match lhs_by_labels.get(&s.labels) {
                        None => s.points,
                        Some(l) => s
                            .points
                            .into_iter()
                            .filter(|(ts, _)| !l.contains(ts))
                            .collect(),
                    };
                    if extra.is_empty() {
                        continue;
                    }
                    if let Some(existing) = out.iter_mut().find(|o| o.labels == s.labels) {
                        existing.points.extend(extra);
                        existing.points.sort_by_key(|(ts, _)| *ts);
                    } else {
                        out.push(MatrixSeries {
                            labels: s.labels,
                            points: extra,
                        });
                    }
                }
                out
            }
            _ => unreachable!("is_set_op gates the arm"),
        };
    }
    lhs.into_iter()
        .filter_map(|s| {
            let r = rhs_by_labels.get(&s.labels)?;
            let points: Vec<(i64, f64)> = s
                .points
                .into_iter()
                .filter_map(|(ts, lv)| {
                    let rv = *r.get(&ts)?;
                    if op.is_comparison() {
                        let hit = compare(op, lv, rv);
                        if return_bool {
                            Some((ts, if hit { 1.0 } else { 0.0 }))
                        } else {
                            hit.then_some((ts, lv))
                        }
                    } else {
                        Some((ts, arith(op, lv, rv)))
                    }
                })
                .collect();
            (!points.is_empty()).then_some(MatrixSeries {
                labels: s.labels,
                points,
            })
        })
        .collect()
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

/// Parses PulsusDB's canonical flat label JSON (`{"key":"value", ...}`,
/// sorted keys, no nesting — docs/architecture.md §2.3) without a JSON
/// crate dependency (not part of this module's declared dependency set).
/// Malformed input — which should never occur, this only ever reads back
/// what the writer produced — yields whatever pairs were parsed so far
/// rather than panicking.
fn parse_flat_labels(json: &str) -> Vec<(String, String)> {
    let mut chars = json.chars().peekable();
    let mut out = Vec::new();
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
    out
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
        }
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
            SampleRow {
                fingerprint: 1,
                timestamp_ns: 1,
                body: "v=1".to_string(),
            },
            SampleRow {
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
