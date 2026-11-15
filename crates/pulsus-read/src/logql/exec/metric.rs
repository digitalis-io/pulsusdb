use super::*;

impl LogQlEngine {
    /// The one statement of [`is_one_statement_sum`]'s shape (issue #839):
    /// no row is the empty vector, one row is one sample with no labels.
    /// The 100,000-stream cap does not apply, as no fingerprint list is held
    /// here; the scan budget applies to the whole statement.
    async fn run_metric_one_statement(
        &self,
        mp: &MetricPlan,
        explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
        let sql = super::sql::metric_instant_sum_one_statement(
            &self.tenant,
            metric_source(mp),
            &mp.table,
            &mp.streams_table,
            &mp.stage1_sql,
            super::sql::TimeWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
            },
            mp.scan_lower,
            &mp.extra_predicates,
        );
        if let Some(e) = explain {
            e.push("metric_read", sql.clone(), Some(mp.routing.reason.clone()));
        }
        let mut stream = self
            .query_stream::<MetricSumRow>(&sql, &self.budget_settings())
            .await?;
        let mut samples = Vec::new();
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })?;
            samples.push(VectorSample {
                labels: Vec::new(),
                value: row.n as f64,
            });
        }
        Ok(QueryResult::Vector(samples))
    }

    /// Executes a [`MetricPlan`] end to end. Same single-pass explain
    /// contract as [`LogQlEngine::run_streams_inner`]. A plan carrying
    /// [`MetricPlan::client`] takes the client-aggregated path (issue
    /// M6-10): a full-window `metric_raw_samples` scan (no `LIMIT`,
    /// budget-abort only) evaluated per line in-engine.
    pub(super) async fn run_metric_inner(
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
        // Issue #624, part 2: a staged plan's stages are today's route's,
        // compiled here for the same reason — a bad pattern is a 400 before
        // any read.
        let staged_compiled = match &mp.value {
            super::sql::MetricValue::Staged(s) => {
                Some(CompiledPipeline::compile(&s.todays_route.pipeline)?)
            }
            // Issue #624, part 4: likewise a reduced plan's, today's route's.
            super::sql::MetricValue::Reduced(r) => {
                Some(CompiledPipeline::compile(&r.todays_route.pipeline)?)
            }
            _ => None,
        };
        // Issue #839: the shape answered by one statement, which resolves
        // the streams inside it.
        let one_statement = is_one_statement_sum(mp);
        if let Some(e) = explain.as_mut() {
            e.set_routing(mp.routing.clone());
            if !one_statement {
                e.push("stage1_stream_resolution", mp.stage1_sql.clone(), None);
                for probe in &mp.probes {
                    e.push(
                        "selectivity_probe",
                        probe.sql.clone(),
                        Some(format!("key = {}", probe.key)),
                    );
                }
            }
        }
        let is_instant = mp.step_ns.is_none();
        // Issue #624, part 5: an instant query lowered at its one grid point
        // still answers a vector.
        let answers_instant = mp.answers_instant();
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
            return Ok(if answers_instant {
                QueryResult::Vector(Vec::new())
            } else {
                QueryResult::Matrix(Vec::new())
            });
        }
        if one_statement {
            return self.run_metric_one_statement(mp, explain).await;
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
            // A staged `absent_over_time` (issue #624, part 2) reports it the
            // same way, through the aggregation it carries.
            if let (super::sql::MetricValue::Staged(s), Some(compiled)) =
                (&mp.value, &staged_compiled)
                && matches!(s.todays_route.range_op, RangeAggOp::AbsentOverTime)
            {
                return run_client_agg_rows(
                    &[],
                    compiled,
                    &HashMap::new(),
                    &s.todays_route,
                    metric_plan_window(&todays_route_plan(mp)),
                    mp.rate_window_ns,
                );
            }
            return Ok(if answers_instant {
                QueryResult::Vector(Vec::new())
            } else {
                QueryResult::Matrix(Vec::new())
            });
        }
        // **Hydration happens BEFORE the client branch** (issue #544 code
        // review round 1). It used to sit below it, and
        // `run_metric_client` hydrated again for itself — which meant the
        // client-aggregated route returned before any metadata fragment
        // could be rendered, because rendering one needs the stream label
        // sets stage 2 resolves. Two things followed, and the second is a
        // wrong answer rather than a wide scan:
        //
        // * a client-aggregated range carried no fragment at all, against
        //   what `docs/features.md` §2 and `docs/schemas.md` §3.2 state.
        //   Its client pipeline re-filters, so the ANSWER was right and
        //   the bytes were not;
        // * the bucketed grid's capability fallback builds an EMPTY
        //   pipeline, whose own doc says that is sound because the
        //   fragment is already in the scan. With the fragment omitted
        //   there is nothing to re-filter, so that route would have
        //   counted every row in the window.
        //
        // One hydration, one render, one predicate vector, passed to every
        // route below.
        if let Some(e) = explain.as_mut() {
            e.push(
                "stage2_hydration",
                super::sql::stage2(
                    &self.tenant,
                    &mp.streams_table,
                    &sql_literals(&fingerprints),
                ),
                None,
            );
        }
        let meta = self.hydrate(&mp.streams_table, &fingerprints).await?;
        // Issue #614, part 2: the rollup plan, or the raw one it falls to.
        let effective = super::rollup::rollup_or_raw(mp, &meta, &self.config.samples);
        let mp: &MetricPlan = &effective;
        if let Some(e) = explain.as_mut() {
            e.set_routing(mp.routing.clone());
        }
        // Rollup table has no `service` column (`ORDER BY (fingerprint,
        // bucket_ns)`); the raw fallback needs it re-injected to keep
        // `log_samples`'s `(service, fingerprint, timestamp_ns)` primary-key
        // prefix engaged (fix-plan amendment §3).
        let services = if mp.rollup {
            Vec::new()
        } else {
            distinct_escaped_services(&meta)
        };

        // Issue #544: the structured-metadata fragments, rendered from the
        // stream label sets stage 2 has just resolved. The two routes that
        // let the DATABASE compute the aggregate cannot simply omit a
        // fragment that does not fit its budget — the answer would move —
        // so the plan carries the client stage it would have had and this
        // is where it is swapped back in. On the client-aggregated route
        // an omission costs bytes and not the answer, because the client
        // pipeline re-runs the filter.
        let lowered = metric_predicates(mp, &fingerprints, &meta, self.metadata_budget());
        if let (Some(client), Some(compiled)) = (&mp.client, &compiled) {
            return self
                .run_metric_client(
                    mp,
                    client,
                    compiled,
                    &fingerprints,
                    &meta,
                    &services,
                    &lowered.predicates,
                    explain,
                )
                .await;
        }
        // Test-only (issue #624, parts 2 and 4): a staged or reduced plan on
        // today's route.
        if self.key_route_test.todays_route_only
            && matches!(
                mp.value,
                super::sql::MetricValue::Staged(_) | super::sql::MetricValue::Reduced(_)
            )
        {
            return self
                .run_bucketed_fallback(
                    mp,
                    &fingerprints,
                    &meta,
                    &services,
                    &lowered.predicates,
                    explain,
                )
                .await;
        }
        // Issue #624, part 4 (§5): a reduced plan whose metadata filter did
        // not fit the statement runs today's route, which re-runs the filter.
        if !lowered.metadata_lowered && matches!(mp.value, super::sql::MetricValue::Reduced(_)) {
            return self
                .run_bucketed_fallback(
                    mp,
                    &fingerprints,
                    &meta,
                    &services,
                    &lowered.predicates,
                    explain,
                )
                .await;
        }
        if !lowered.metadata_lowered {
            let restored = mp
                .metadata_lowering
                .as_ref()
                .and_then(|m| m.client_without_lowering.as_ref());
            if let Some(client) = restored {
                let compiled = CompiledPipeline::compile(&client.pipeline)?;
                // Issue #624, part 5: an instant lowered at one grid point
                // runs as the instant it is.
                return self
                    .run_metric_client(
                        &todays_route_plan(mp),
                        client,
                        &compiled,
                        &fingerprints,
                        &meta,
                        &services,
                        // `lowered.predicates` carries no metadata
                        // fragment here: this arm runs exactly because the
                        // render did not fit its budget.
                        &lowered.predicates,
                        explain,
                    )
                    .await;
            }
        }

        if is_instant {
            // Issue #507: the shape is read INSIDE the arms that need
            // one. A reduced plan has no sealed column pair, so
            // `metric_source`'s `.expect` would be reachable by an
            // ordinary query if this stayed above the branch.
            let sql = super::sql::metric_instant(
                &self.tenant,
                metric_source(mp),
                &services,
                &sql_literals(&fingerprints),
                super::sql::TimeWindow {
                    start_ns: mp.start_ns,
                    end_ns: mp.end_ns,
                },
                mp.scan_lower,
                &lowered.predicates,
                // Issue #249: this arm runs only when `is_instant`, and an
                // instant plan is always `RouteChoice::Raw` over
                // `log_samples` (the rollup decision requires
                // `QuerySpec::Range`) — so the metadata column always
                // exists here. Issue #624, part 8: it is read unless no
                // line's metadata can reach the answer.
                super::rollup::counting_projection(mp, &meta),
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
            let mut groups = PushdownInstantGroups::new(&meta, AggCaps::DEFAULT)
                .with_parent_sum(parent_sum_of(mp));
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
            let series = groups.finish(step_rate_window(mp));
            let series = match rate_after_sum(mp) {
                None => charged_instant_chain(series, &mp.vector_aggs, MAX_POST_AGG_BYTES)?,
                Some(window) => {
                    let (sum, outer) = mp
                        .vector_aggs
                        .split_last()
                        .expect("rate_after_sum holds only under a parent sum");
                    let mut summed = charged_instant_chain(
                        series,
                        std::slice::from_ref(sum),
                        MAX_POST_AGG_BYTES,
                    )?;
                    for s in &mut summed {
                        s.value = apply_rate(s.value, Some(window));
                    }
                    charged_instant_chain(summed, outer, MAX_POST_AGG_BYTES)?
                }
            };
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
            // **The bucketed range read** (issue #507, W2). Reaching here
            // needs `client.is_none()` (the arm above returned otherwise)
            // AND `step_ns.is_some()`, which is exactly the shape
            // `metric_plan`'s `bucketed_range` admits: one of the four
            // counting reducers and nothing in the pipeline beyond a
            // pushable line filter, at any range and step (issue #624).
            //
            // Until #507 this state was unreachable and the arm refused,
            // because `metric_plan` forced `client = Some(..)` for every
            // `QuerySpec::Range` (issues #241 / #257). The tripwire that
            // named the refusal —
            // `logql_plan_build_differential.rs::
            // every_planned_range_leaf_is_client_aggregated` — now states
            // the relaxed property instead.
            //
            // **The capability join.** The grid expression can refuse for
            // three reasons, all of them decided by the request's own time
            // bounds, and each is a fall back to the client path rather
            // than an error: the query is valid and the aggregation simply
            // does not lower. `metric_plan` asserts the chain-decidable
            // half, so none of the three is reachable through the planner;
            // the arm is what makes that a property of the code rather
            // than of the reasoning.
            // **The grid-resolution guard, which no other call on this
            // path applies.** The client path reaches it through
            // `ClientWindow::Range`; this path builds no `ClientWindow`,
            // so without this line an over-cap grid would be answered
            // instead of refused, and a query that 422s today would start
            // returning a matrix. Same constant, same named refusal, no
            // round trip: it runs before the statement is issued.
            let grid_points = match mp.step_ns {
                Some(step) => super::window::ensure_grid_resolution(
                    mp.grid_start_ns,
                    mp.end_ns,
                    step.as_u64(),
                )?,
                None => 0,
            };
            // Issue #624, part 4: the `| unwrap` reducers' own statement,
            // row type and fold.
            if let super::sql::MetricValue::Reduced(r) = &mp.value {
                return self
                    .run_reduced_range(
                        mp,
                        r,
                        &fingerprints,
                        &meta,
                        &services,
                        &lowered.predicates,
                        grid_points,
                        explain,
                    )
                    .await;
            }
            match bucketed_range_sql(
                &self.tenant,
                mp,
                &services,
                &fingerprints,
                &lowered.predicates,
                self.config.rollup_res_ns,
                super::rollup::counting_projection(mp, &meta),
            ) {
                Ok(sql) => {
                    let sql = match self.key_route_test.bucketed_statement_max_memory_bytes {
                        Some(n) => format!("{sql}\nSETTINGS max_memory_usage = {n}"),
                        None => sql,
                    };
                    if let Some(e) = explain.as_mut() {
                        e.push("metric_read", sql.clone(), Some(mp.routing.reason.clone()));
                    }
                    let groups = PushdownRangeGroups::new(
                        &meta,
                        self.lowered_caps(),
                        mp.grid_start_ns,
                        mp.end_ns,
                        mp.step_ns.map_or(0, |s| s.get()),
                        grid_points,
                    );
                    let groups = match &mp.value {
                        // Issue #624, part 2: the label stages run once per
                        // returned group, charged as today's route charges
                        // each series — dense at every range and step. The
                        // statement applied the line filters already.
                        super::sql::MetricValue::Staged(s) => {
                            let stages: Vec<Stage> = s
                                .todays_route
                                .pipeline
                                .iter()
                                .filter(|stage| !matches!(stage, Stage::LineFilter(_)))
                                .cloned()
                                .collect();
                            let step = super::plan::range_step_rules(
                                s.todays_route.range_op,
                                &s.todays_route.pipeline,
                                s.todays_route.grouping.as_deref(),
                                &mp.vector_aggs,
                            );
                            groups.with_stages(
                                CompiledPipeline::compile(&stages)?,
                                &s.todays_route,
                                step,
                                s.regexp.as_ref().map(|r| r.groups.clone()),
                                s.json
                                    .as_ref()
                                    .map(|j| j.keys.iter().map(|k| k.source.clone()).collect()),
                                s.logfmt.is_some(),
                            )
                        }
                        // #507's statement (range equal to step) keeps the
                        // sparse charge it had; the sliding statement
                        // reserves a grid per series, as today's sliding
                        // route does.
                        _ => {
                            let groups = groups.with_parent_sum(parent_sum_of(mp));
                            if mp.step_ns.is_some_and(|s| s.get() == mp.range_ns.get()) {
                                groups.with_sparse_points()
                            } else {
                                groups
                            }
                        }
                    };
                    // Scoped: the row stream holds its pooled connection
                    // until dropped (the `ChRowStream` lease rule), and
                    // today's route issues another query after it.
                    let outcome = {
                        let sql = escape_query_placeholders(&sql);
                        if let Err(reason) = crate::querytext::ensure_query_text_fits(&sql) {
                            return Err(ReadError::QueryTooBroad(reason));
                        }
                        // Issue #624, part 3a: a `regexp` count's rows carry
                        // its two group-key columns.
                        let regexp = matches!(
                            &mp.value,
                            super::sql::MetricValue::Staged(s) if s.regexp.is_some()
                        );
                        let json = matches!(
                            &mp.value,
                            super::sql::MetricValue::Staged(s) if s.json.is_some()
                        );
                        let logfmt = matches!(
                            &mp.value,
                            super::sql::MetricValue::Staged(s) if s.logfmt.is_some()
                        );
                        if logfmt {
                            match self
                                .client
                                .query_stream::<MetricRangeLogfmtRow>(&sql, &self.budget_settings())
                                .await
                                .map(RxRows::<MetricRangeBucketRow, _>::Rx)
                            {
                                Ok(stream) => fold_bucketed_rows(stream, groups).await?,
                                Err(e) if bucketed_statement_failure_goes_to_todays_route(&e) => {
                                    BucketedRows::TodaysRoute
                                }
                                Err(e) => BucketedRows::Failed(e),
                            }
                        } else if json {
                            match self
                                .client
                                .query_stream::<MetricRangeJsonRow>(&sql, &self.budget_settings())
                                .await
                                .map(RxRows::<MetricRangeBucketRow, _>::Rx)
                            {
                                Ok(stream) => fold_bucketed_rows(stream, groups).await?,
                                Err(e) if bucketed_statement_failure_goes_to_todays_route(&e) => {
                                    BucketedRows::TodaysRoute
                                }
                                Err(e) => BucketedRows::Failed(e),
                            }
                        } else {
                            let opened = if regexp {
                                self.client
                                    .query_stream::<MetricRangeRegexpRow>(
                                        &sql,
                                        &self.budget_settings(),
                                    )
                                    .await
                                    .map(RxRows::<MetricRangeBucketRow, _>::Rx)
                            } else {
                                self.client
                                    .query_stream::<MetricRangeBucketRow>(
                                        &sql,
                                        &self.budget_settings(),
                                    )
                                    .await
                                    .map(RxRows::<_, MetricRangeRegexpRow>::Plain)
                            };
                            match opened {
                                Ok(stream) => fold_bucketed_rows(stream, groups).await?,
                                Err(e) if bucketed_statement_failure_goes_to_todays_route(&e) => {
                                    BucketedRows::TodaysRoute
                                }
                                Err(e) => BucketedRows::Failed(e),
                            }
                        }
                    };
                    match outcome {
                        BucketedRows::Folded(groups) => {
                            let series = groups.finish_checked(step_rate_window(mp))?;
                            // The emitted points are on the SHIFTED grid;
                            // this puts them back on the caller's, the one
                            // place the offset is added back on this path
                            // (issue #343).
                            let result =
                                shift_emitted_points(QueryResult::Matrix(series), mp.offset_ns);
                            apply_leaf_aggs(mp, one_point_answer(mp, result), 0)
                        }
                        // Issue #624, D4: the statement ran out of memory.
                        // Today's route streams the same query in bounded
                        // memory, so it answers instead, once. The fold's
                        // partial state is already dropped: the variant
                        // carries none.
                        BucketedRows::TodaysRoute => {
                            self.run_lowered_fallback(
                                super::rollup::RawCause::Memory,
                                mp,
                                &fingerprints,
                                &meta,
                                &services,
                                &lowered.predicates,
                                explain,
                            )
                            .await
                        }
                        BucketedRows::Failed(e) => Err(map_read_error(
                            e,
                            self.config.scan_budget_bytes,
                            self.config.read_max_memory_bytes,
                        )),
                    }
                }
                Err(
                    BucketGridRefusal::StepNotPositive
                    | BucketGridRefusal::AnchorAboveScanStart
                    | BucketGridRefusal::WouldOverflow
                    | BucketGridRefusal::RangeNotPositive
                    | BucketGridRefusal::RollupSource
                    | BucketGridRefusal::OffTheRollupGrid,
                ) => {
                    // No `_` arm: a further refusal reason fails to compile
                    // here rather than falling through to a behaviour
                    // nobody chose for it.
                    self.run_lowered_fallback(
                        super::rollup::RawCause::Refused,
                        mp,
                        &fingerprints,
                        &meta,
                        &services,
                        &lowered.predicates,
                        explain,
                    )
                    .await
                }
            }
        }
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

/// The divisor of a `rate`/`bytes_rate` directly under a `sum` (issue
/// #624, part 3b): the reference hands the sum's grouping to the range step
/// (`canInjectVectorGrouping`, `pkg/logql/syntax/ast.go:1616-1640 @
/// v3.7.4`), so each sum group is one series whose count is divided once.
/// `Some(range)` then; the four reducers take no grouping of their own
/// (`RangeAggOp::allows_grouping`).
fn rate_after_sum(mp: &MetricPlan) -> Option<u64> {
    if !matches!(mp.op, RangeAggOp::Rate | RangeAggOp::BytesRate) {
        return None;
    }
    super::plan::parent_sum_grouping(mp.op, false, &mp.vector_aggs)?;
    mp.rate_window_ns
}

/// The divisor the range step applies per series: none when the parent
/// sum divides instead ([`rate_after_sum`]).
pub(super) fn step_rate_window(mp: &MetricPlan) -> Option<u64> {
    match rate_after_sum(mp) {
        Some(_) => None,
        None => mp.rate_window_ns,
    }
}

/// `mp`'s vector aggregations over a range step's result, the innermost
/// `folded` of them already applied by the fold; under [`rate_after_sum`]
/// the division runs between the parent sum and the rest.
pub(super) fn apply_leaf_aggs(
    mp: &MetricPlan,
    result: QueryResult,
    folded: usize,
) -> Result<QueryResult, ReadError> {
    let Some(window) = rate_after_sum(mp) else {
        return apply_vector_aggs(result, &mp.vector_aggs[..mp.vector_aggs.len() - folded]);
    };
    let (sum, outer) = mp
        .vector_aggs
        .split_last()
        .expect("rate_after_sum holds only under a parent sum");
    let summed = if folded == 0 {
        apply_vector_aggs(result, std::slice::from_ref(sum))?
    } else {
        result
    };
    let divided = match summed {
        QueryResult::Matrix(series) => QueryResult::Matrix(
            series
                .into_iter()
                .map(|mut s| {
                    for p in &mut s.points {
                        p.1 = apply_rate(p.1, Some(window));
                    }
                    s
                })
                .collect(),
        ),
        QueryResult::Vector(samples) => QueryResult::Vector(
            samples
                .into_iter()
                .map(|mut s| {
                    s.value = apply_rate(s.value, Some(window));
                    s
                })
                .collect(),
        ),
        other => other,
    };
    apply_vector_aggs(divided, outer)
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
/// Whether `mp` is answered by one statement (issue #839, D4): an instant
/// `sum(count_over_time(…))` or `sum(bytes_over_time(…))`, with no grouping
/// and no stage but pushable line filters, over a domain on the timestamp
/// axis. Every field read is the plan's own.
fn is_one_statement_sum(mp: &MetricPlan) -> bool {
    use super::sql::{MetricShape, MetricValue};
    mp.step_ns.is_none()
        && !mp.empty_domain
        && matches!(
            mp.value,
            MetricValue::Shaped(MetricShape::RawCount | MetricShape::RawBytes)
        )
        && mp.client.is_none()
        && mp.metadata_lowering.is_none()
        && matches!(
            mp.vector_aggs.as_slice(),
            [(pulsus_logql::VectorAggOp::Sum, None, None)]
        )
        && matches!(mp.op, RangeAggOp::CountOverTime | RangeAggOp::BytesOverTime)
}

pub(super) fn metric_source(mp: &MetricPlan) -> super::sql::MetricSource<'_> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
