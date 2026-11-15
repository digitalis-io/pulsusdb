use super::*;

impl LogQlEngine {
    /// The fallback of either lowered range statement (issue #614, part 2):
    /// a rollup plan becomes the raw plan, reported with `cause` as its
    /// route, and reads the lines with the hydrated streams' services; any
    /// other plan is [`Self::run_bucketed_fallback`]'s unchanged.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_lowered_fallback(
        &self,
        cause: super::rollup::RawCause,
        mp: &MetricPlan,
        fingerprints: &[Fingerprint],
        meta: &HashMap<Fingerprint, StreamMetaRow>,
        services: &[CheckedLiteral],
        predicates: &[CheckedFragment],
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
        let raw = super::rollup::raw_route_of(mp, &self.config.samples, cause);
        if mp.rollup
            && let Some(e) = explain.as_mut()
        {
            e.set_routing(raw.routing.clone());
        }
        let services = raw_services(mp, services, meta);
        self.run_bucketed_fallback(&raw, fingerprints, meta, &services, predicates, explain)
            .await
    }

    /// Today's route for a bucketed range plan (issues #507 and #624): the
    /// client aggregation the lowered chain is equivalent to. Run when the
    /// statement cannot be rendered, and once when it fails for memory.
    pub(super) async fn run_bucketed_fallback(
        &self,
        mp: &MetricPlan,
        fingerprints: &[Fingerprint],
        meta: &HashMap<Fingerprint, StreamMetaRow>,
        services: &[CheckedLiteral],
        predicates: &[CheckedFragment],
        explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
        // Issue #614, part 2: a rollup plan arrives through
        // `run_lowered_fallback`, which made it the raw plan.
        debug_assert!(!mp.rollup, "a rollup plan's fallback is the raw plan");
        // Issue #624, part 5: an instant query lowered at its one grid point
        // runs today's instant route.
        let plan = todays_route_plan(mp);
        let mp: &MetricPlan = &plan;
        let client = bucketed_fallback_client_agg(mp);
        let compiled = CompiledPipeline::compile(&client.pipeline)?;
        // **The lowered predicates, and the metadata fragment in them is
        // load-bearing** (issue #544 code review round 1). This fallback's
        // pipeline is EMPTY, and its own doc says that is sound because the
        // predicates are already in the scan. Passing `mp.extra_predicates`
        // here would leave nothing to apply the metadata filter at all, and
        // the aggregate would count every row in the window.
        self.run_metric_client(
            mp,
            &client,
            &compiled,
            fingerprints,
            meta,
            services,
            predicates,
            explain,
        )
        .await
    }

    /// The lowered `| unwrap` reducers (issue #624, part 4): the reduced
    /// statement and its fold, or today's route once (D8).
    ///
    /// ```text
    /// a selected stream labelled x with a parser in drop mode (part 4b, D7),
    /// a grid or reader the statement cannot render,
    /// the text over the cap, 241, 395, a fold stop     -> today's route, once
    /// 159 / the stream deadline                        -> the timeout
    /// 307                                              -> the scan budget's 422
    /// a cap in the fold                                -> its 422
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_reduced_range(
        &self,
        mp: &MetricPlan,
        value: &super::sql::ReducedUnwrap,
        fingerprints: &[Fingerprint],
        meta: &HashMap<Fingerprint, StreamMetaRow>,
        services: &[CheckedLiteral],
        predicates: &[CheckedFragment],
        grid_points: u64,
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
        // Part 4b, D7: a stream label `x` is read by the statement, except
        // beside a parser in drop mode, where the parser's `x` becomes
        // `x_extracted`, part of the series, which `pk` does not carry.
        let refused = super::reduce::labelled_x_refuses(value, meta);
        let statement = match (refused, mp.step_ns) {
            (false, Some(step)) => {
                let resolved = super::reduce::resolve_reduced(value, meta);
                super::sql::metric_range_reduced(
                    &self.tenant,
                    &mp.table,
                    value,
                    &resolved.columns,
                    services,
                    &sql_literals(&resolved.fingerprints),
                    super::sql::SlidingScan {
                        window: super::sql::TimeWindow {
                            start_ns: mp.start_ns,
                            end_ns: mp.end_ns,
                        },
                        lower: mp.scan_lower,
                        grid_start_ns: mp.grid_start_ns,
                        step_ns: step.get(),
                        range_ns: mp.range_ns.get(),
                    },
                    predicates,
                    self.key_route_test.bucketed_statement_max_memory_bytes,
                )
                .ok()
                .map(|sql| (sql, resolved.class_labels))
            }
            _ => None,
        };
        let Some((sql, class_labels)) = statement else {
            return self
                .run_bucketed_fallback(mp, fingerprints, meta, services, predicates, explain)
                .await;
        };
        if let Some(e) = explain.as_mut() {
            e.push("metric_read", sql.clone(), Some(mp.routing.reason.clone()));
        }
        let sql = escape_query_placeholders(&sql);
        if crate::querytext::ensure_query_text_fits(&sql).is_err() {
            return self
                .run_bucketed_fallback(mp, fingerprints, meta, services, predicates, explain)
                .await;
        }
        let mut fold = super::reduce::ReducedFold::new(
            value,
            class_labels,
            self.lowered_caps(),
            super::reduce::ReducedGrid {
                grid_start_ns: mp.grid_start_ns,
                end_ns: mp.end_ns,
                step_ns: mp.step_ns.map_or(0, |s| s.get()),
                grid_points,
                range_ns: mp.range_ns.get(),
                rate_window_ns: step_rate_window(mp),
            },
            &mp.vector_aggs,
        )?;
        // Scoped: the row stream holds its pooled connection until dropped,
        // and today's route issues another query after it.
        let todays_route = {
            let mut stream = match self
                .client
                .query_stream::<MetricReducedRow>(&sql, &self.budget_settings())
                .await
            {
                Ok(stream) => stream,
                Err(e) if bucketed_statement_failure_goes_to_todays_route(&e) => {
                    return self
                        .run_bucketed_fallback(
                            mp,
                            fingerprints,
                            meta,
                            services,
                            predicates,
                            explain,
                        )
                        .await;
                }
                Err(e) => {
                    return Err(map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    ));
                }
            };
            let mut todays_route = false;
            while let Some(row) = stream.next().await {
                match row {
                    Ok(row) => {
                        if fold.push(&row)? == super::reduce::ReducedPush::Stop {
                            todays_route = true;
                            break;
                        }
                    }
                    Err(e) if bucketed_statement_failure_goes_to_todays_route(&e) => {
                        todays_route = true;
                        break;
                    }
                    Err(e) => {
                        return Err(map_read_error(
                            e,
                            self.config.scan_budget_bytes,
                            self.config.read_max_memory_bytes,
                        ));
                    }
                }
            }
            todays_route
        };
        if todays_route {
            // The fold's state is dropped before today's route reads.
            drop(fold);
            return self
                .run_bucketed_fallback(mp, fingerprints, meta, services, predicates, explain)
                .await;
        }
        let result = shift_emitted_points(QueryResult::Matrix(fold.finish()), mp.offset_ns);
        apply_leaf_aggs(mp, one_point_answer(mp, result), 0)
    }

    /// The client-aggregated metric path (issue M6-10): fetch every
    /// matching `(fingerprint, timestamp_ns, body)` row in the window —
    /// **no `LIMIT`**; the scan is complete or aborts on the byte budget
    /// (`QueryTooBroad`), never silently truncated — then run the
    /// compiled pipeline per line, bucket by step in-engine, reduce per
    /// `(final-label-set, bucket)`, and finish the vector aggregations.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_metric_client(
        &self,
        mp: &MetricPlan,
        client: &ClientAgg,
        compiled: &CompiledPipeline,
        fingerprints: &[Fingerprint],
        meta: &HashMap<Fingerprint, StreamMetaRow>,
        services: &[CheckedLiteral],
        predicates: &[CheckedFragment],
        mut explain: Option<&mut PlanExplain>,
    ) -> Result<QueryResult, ReadError> {
        // **The hydrated state is passed in, not fetched here** (issue
        // #544 code review round 1): the metadata fragments are rendered
        // from the stream label sets, so a route that hydrated for itself
        // was a route that could not carry one. `run_metric_inner` does
        // the one hydration and the one render, and every route below it
        // takes the same `predicates`.
        let is_range = mp.step_ns.is_some();
        let time_window = super::sql::TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        };
        // Issue #227: a range query reads in physical-key order
        // (`optimize_read_in_order`, no server sort) for the streaming slide;
        // an instant query keeps the total-timestamp order its reducers pin.
        let rx = rx_column_for(compiled, client_needs_no_labels(mp, client))?;
        let sql = client_metric_read_sql(
            &self.tenant,
            mp,
            services,
            fingerprints,
            time_window,
            predicates,
            rx.as_ref(),
        );
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
        // Issue #507: the range step's rules, derived from the plan.
        let step = super::plan::range_step_rules(
            client.range_op,
            &client.pipeline,
            client.grouping.as_deref(),
            &mp.vector_aggs,
        );
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
                meta,
                client,
                window,
                step_rate_window(mp),
                self.todays_route_caps(),
            )?
            .with_range_step(step);
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
                MetricAggState::Instant(Box::new(
                    ClientAggState::new(
                        compiled,
                        meta,
                        client,
                        instant,
                        step_rate_window(mp),
                        self.todays_route_caps(),
                    )?
                    .with_range_step(step),
                )),
                0,
            )
        };
        let mut chunk: Vec<MetricScanRow> = Vec::with_capacity(CLIENT_AGG_CHUNK_ROWS);
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule) — no other query
            // runs inside this block, and the lease ends at the brace.
            let mut stream = self
                .query_rows::<MetricScanRow, MetricScanRxRow>(&sql, &settings, rx.is_some())
                .await?;
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
        apply_leaf_aggs(mp, result, folded)
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
    pub(super) async fn run_variants(
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
                super::sql::stage2(
                    &self.tenant,
                    &scan.streams_table,
                    &sql_literals(&fingerprints),
                ),
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
        // Issue #544: the variants scan hydrates for itself, so it
        // renders its own fragments from the same function. A variants
        // chain is `force_client`, so its metadata filter — if the common
        // pipeline carries one — is re-applied per variant by the arena;
        // the fragment narrows the scan and cannot change the answer.
        let lowered = metric_predicates(scan, &fingerprints, &meta, self.metadata_budget());
        // Issue #624, part 3a (D6): one `rx` column serves the common
        // pipeline and every distinct tail the arena built.
        let rx = if arena.rx_patterns().is_empty() {
            None
        } else {
            Some(super::predicate::regexp_captures_column_skipping(
                arena.rx_patterns(),
                &arena.rx_skipped(variants),
            )?)
        };
        let sql = client_metric_read_sql(
            &self.tenant,
            scan,
            &services,
            &fingerprints,
            time_window,
            &lowered.predicates,
            rx.as_ref(),
        );
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
            let mut stream = self
                .query_rows::<MetricScanRow, MetricScanRxRow>(&sql, &settings, rx.is_some())
                .await?;
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
}
