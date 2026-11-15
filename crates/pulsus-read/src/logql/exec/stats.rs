use super::*;

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
/// (a zero only where the reference's chunk fill puts one, issue #605). The engine preserves ClickHouse's
/// total desc, pattern asc, level asc order (the pushed-down `ORDER BY`), so
/// the top-1000 presentation IS the contract — the response encoder must not
/// re-sort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternSeries {
    pub pattern: String,
    /// The pattern's level (issue #605): one series per `(pattern, level)`.
    pub level: String,
    pub samples: Vec<(i64, u64)>,
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
        let ctx = self.config.plan_ctx(&self.tenant);
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
        let (sql, routing) = if matches!(expr, Expr::Log(le) if le.pipeline.is_empty()) {
            (
                super::sql::log_stats_rollup(
                    &self.tenant,
                    &self.config.rollup_table,
                    &sql_literals(&fingerprints),
                    window,
                ),
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
                    super::sql::stage2(
                        &self.tenant,
                        &sp.streams_table,
                        &sql_literals(&fingerprints),
                    ),
                    None,
                );
            }
            let meta = self.hydrate(&sp.streams_table, &fingerprints).await?;
            let services = distinct_escaped_services(&meta);
            (
                super::sql::log_stats_raw(
                    &self.tenant,
                    &sp.samples_table,
                    &services,
                    &sql_literals(&fingerprints),
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
        let ctx = self.config.plan_ctx(&self.tenant);
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
            &self.tenant,
            &self.config.patterns_table,
            &sql_literals(&fingerprints),
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
                // ClickHouse already ordered by (total desc, pattern, level) and
                // arraySorted the samples ascending by ts_ns; each ts_ns is a
                // multiple of step_ns (≥ 10s), so ns→unix-seconds is exact.
                let samples = row
                    .samples
                    .into_iter()
                    .map(|(ts_ns, count)| (ts_ns / 1_000_000_000, count))
                    .collect();
                series.push(PatternSeries {
                    pattern: row.pattern,
                    level: row.level,
                    samples,
                });
            }
        }
        Ok(series)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
