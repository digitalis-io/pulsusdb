use super::*;

/// Which statement [`LogQlEngine::run_discovery`] renders at stage 2, and
/// under which `X-Pulsus-Explain` stage name. This is the ONLY thing a
/// caller says about the stage-2 SQL. There is no parameter, closure
/// argument or return value through which a caller receives a fingerprint
/// list, so a caller has no expression in which to ignore, replace or
/// fabricate one (issue #482).
#[derive(Debug)]
pub(super) enum DiscoveryQuery<'a> {
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

impl LogQlEngine {
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
    ) -> Result<DiscoveryAnswer<String>, ReadError> {
        self.label_names_inner(selector, b, None).await
    }

    /// [`LogQlEngine::label_names`] plus its `X-Pulsus-Explain` trace, in
    /// the same single pass (no second scan).
    pub async fn label_names_explained(
        &self,
        selector: Option<&Expr>,
        b: TimeBounds,
    ) -> Result<(DiscoveryAnswer<String>, PlanExplain), ReadError> {
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
    ) -> Result<DiscoveryAnswer<String>, ReadError> {
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
    ) -> Result<DiscoveryAnswer<String>, ReadError> {
        self.label_values_inner(name, selector, b, None).await
    }

    /// [`LogQlEngine::label_values`] plus its `X-Pulsus-Explain` trace, in
    /// the same single pass (no second scan).
    pub async fn label_values_explained(
        &self,
        name: &str,
        selector: Option<&Expr>,
        b: TimeBounds,
    ) -> Result<(DiscoveryAnswer<String>, PlanExplain), ReadError> {
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
    ) -> Result<DiscoveryAnswer<String>, ReadError> {
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
    /// dispatch for `/labels`, `/label/{name}/values` and
    /// `/detected_labels` (issues #482 and #399).
    ///
    /// One statement; no stream cap; an empty answer is zero rows. The
    /// selector's stage-1 statement, when there is one, is nested inside the
    /// activity read, which is nested inside the index scan (issue #399,
    /// D1–D3). The answer is bounded at [`Self::discovery_max_rows`] rows:
    /// the statement carries `LIMIT bound + 1`, and a row past the bound
    /// marks the answer truncated (D4).
    ///
    /// Memory: at most `bound + 1` rows are decoded and `bound` retained.
    /// The writer caps a label value at 2,048 bytes and a name at 1,024, so a
    /// full answer stays under the 1 GiB the streams answer already allows
    /// (`super::charge::MAX_STREAMS_RESULT_BYTES`).
    ///
    /// `R` is not bound to the [`DiscoveryQuery`] variant: a mismatch is a
    /// row-decode error, not a wrong answer — the projections have disjoint
    /// column names.
    ///
    /// `metric_rejection` is the endpoint's own `PipelineInvalid` reason:
    /// unreachable through the API layer (all three parse `query` with
    /// `parse_selector`), kept as a structured rejection, never a panic.
    pub(super) async fn run_discovery<R: ChRow, T>(
        &self,
        query: DiscoveryQuery<'_>,
        selector: Option<&Expr>,
        b: TimeBounds,
        mut explain: Option<&mut PlanExplain>,
        metric_rejection: &str,
        mut keep_row: impl FnMut(R) -> Option<T>,
    ) -> Result<DiscoveryAnswer<T>, ReadError> {
        // Always >= 1 literal (`months_overlapping` never returns empty),
        // so the month IN-list has no empty-IN hazard.
        let months = plan::months_overlapping(b.start_ns, b.end_ns);
        let stage1_sql: Option<String> = match selector {
            None => None,
            Some(expr) => {
                let ctx = self.config.plan_ctx(&self.tenant);
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
                match plan::plan(expr, &qp, &ctx)? {
                    Plan::Streams(sp) => Some(sp.stage1_sql),
                    // Unreachable via the API layer (it parses `query`
                    // with `parse_selector`) — kept as a structured
                    // rejection, never a panic.
                    Plan::Metric(_) | Plan::MetricBinary(_) => {
                        return Err(ReadError::PipelineInvalid {
                            reason: metric_rejection.to_string(),
                        });
                    }
                }
            }
        };
        let scope = super::sql::DiscoveryScope {
            rollup_table: &self.config.rollup_table,
            window: self.activity_window(b),
            rollup_res_ns: self.config.rollup_res_ns,
            stage1_sql: stage1_sql.as_deref(),
        };
        let cap = self.discovery_max_rows;
        let limit = cap.saturating_add(1);
        let sql = match query {
            DiscoveryQuery::LabelNames => super::sql::label_names(
                &self.tenant,
                &self.config.streams_idx,
                &months,
                &scope,
                limit,
            ),
            DiscoveryQuery::LabelValues { name } => super::sql::label_values(
                &self.tenant,
                &self.config.streams_idx,
                &months,
                &super::predicate::literal(name),
                &scope,
                limit,
            ),
            DiscoveryQuery::DetectedLabels => super::sql::detected_labels(
                &self.tenant,
                &self.config.streams_idx,
                &months,
                &scope,
                limit,
            ),
        };
        if let Some(e) = explain.as_mut() {
            e.push(query.stage_name(), sql.clone(), None);
        }
        let mut out = Vec::new();
        let mut rows_read: u64 = 0;
        let mut truncated = false;
        let mut stream = self
            .query_stream::<R>(&sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })?;
            rows_read += 1;
            if rows_read > cap {
                // The row past the bound: the answer is cut here, and
                // this row is dropped.
                truncated = true;
                break;
            }
            if let Some(kept) = keep_row(row) {
                out.push(kept);
            }
        }
        Ok(DiscoveryAnswer {
            items: out,
            truncated,
        })
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
        let ctx = self.config.plan_ctx(&self.tenant);
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
        let mut fingerprints: Vec<Fingerprint> = Vec::new();
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
                        &self.tenant,
                        &self.config.rollup_table,
                        super::sql::ActivityScope::All,
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
                        &self.tenant,
                        &self.config.rollup_table,
                        super::sql::ActivityScope::Listed(&sql_literals(&fingerprints)),
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
                super::sql::stage2(&self.tenant, &streams_table, &sql_literals(&fingerprints)),
                None,
            );
        }
        let meta = self.hydrate(&streams_table, &fingerprints).await?;
        let mut labels: Vec<String> = meta.into_values().map(|m| m.labels).collect();
        labels.sort();
        labels.dedup();
        Ok(labels)
    }
}
