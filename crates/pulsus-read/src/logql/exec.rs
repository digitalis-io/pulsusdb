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
use pulsus_logql::{Expr, Grouping, GroupingKind, VectorAggOp};

use super::error::{ReadError, TooBroadReason};
use super::explain::PlanExplain;
use super::params::{Direction, PlanCtx, QueryParams, QuerySpec, TimeBounds};
use super::plan::{self, MetricPlan, Plan, StreamsPlan};
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
            let (stage1_sql, table) = match plan::plan(expr, &qp, &ctx)? {
                Plan::Streams(sp) => (sp.stage1_sql, sp.streams_table),
                Plan::Metric(mp) => (mp.stage1_sql, mp.streams_table),
            };
            if let Some(e) = explain.as_mut() {
                e.push("stage1_stream_resolution", stage1_sql.clone(), None);
            }
            let fps = self.resolve_fingerprints(&stage1_sql).await?;
            fingerprints.extend(fps);
            streams_table = table;
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
    async fn run_streams_inner(
        &self,
        sp: &StreamsPlan,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<Vec<StreamResult>, ReadError> {
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
            sp.limit,
        );
        if let Some(e) = explain.as_mut() {
            e.push("stage3_samples", sql.clone(), None);
        }

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

        Ok(by_fp
            .into_iter()
            .filter_map(|(fp, entries)| {
                meta.get(&fp).map(|m| StreamResult {
                    fingerprint: fp,
                    service: m.service.clone(),
                    labels_json: m.labels.clone(),
                    entries,
                })
            })
            .collect())
    }

    /// Executes a [`MetricPlan`] end to end. Same single-pass explain
    /// contract as [`LogQlEngine::run_streams_inner`].
    async fn run_metric_inner(
        &self,
        mp: &MetricPlan,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
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
            return Ok(if is_instant {
                QueryResult::Vector(Vec::new())
            } else {
                QueryResult::Matrix(Vec::new())
            });
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
            for (op, grouping) in mp.vector_aggs.iter().rev() {
                series = group_instant(series, *op, grouping.as_ref());
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
            for (op, grouping) in mp.vector_aggs.iter().rev() {
                series = group_range(series, *op, grouping.as_ref());
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
            sp.limit,
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
            return Ok(explain);
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
        let source = super::sql::MetricSource {
            table: &mp.table,
            bucket_col: mp.bucket_col,
            agg_expr: mp.agg_expr,
        };
        let metric_sql = match mp.step_ns {
            Some(step_ns) => super::sql::metric_range(
                source,
                &services,
                &fingerprints,
                super::sql::TimeWindow {
                    start_ns: mp.start_ns,
                    end_ns: mp.end_ns,
                },
                step_ns,
                &mp.extra_predicates,
            ),
            None => super::sql::metric_instant(
                source,
                &services,
                &fingerprints,
                super::sql::TimeWindow {
                    start_ns: mp.start_ns,
                    end_ns: mp.end_ns,
                },
                &mp.extra_predicates,
            ),
        };
        explain.push("metric_read", metric_sql, Some(mp.routing.reason.clone()));
        Ok(explain)
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

fn reduce(op: VectorAggOp, vals: &[f64]) -> f64 {
    match op {
        VectorAggOp::Sum => vals.iter().sum(),
        VectorAggOp::Avg => vals.iter().sum::<f64>() / vals.len() as f64,
        VectorAggOp::Min => vals.iter().cloned().fold(f64::INFINITY, f64::min),
        VectorAggOp::Max => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        VectorAggOp::Count => vals.len() as f64,
    }
}

fn group_range(
    series: Vec<RangeSeries>,
    op: VectorAggOp,
    grouping: Option<&Grouping>,
) -> Vec<RangeSeries> {
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
) -> Vec<InstantSeries> {
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
        let grouped = group_range(series, VectorAggOp::Sum, Some(&grouping));
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].points.get(&0), Some(&4.0));
        assert_eq!(grouped[0].points.get(&60), Some(&2.0));
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
