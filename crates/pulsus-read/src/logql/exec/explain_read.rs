use super::*;

impl LogQlEngine {
    pub async fn explain(
        &self,
        expr: &Expr,
        params: &QueryParams,
    ) -> Result<PlanExplain, ReadError> {
        let ctx = self.config.plan_ctx(&self.tenant);
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
        let stage2_sql = super::sql::stage2(
            &self.tenant,
            &sp.streams_table,
            &sql_literals(&fingerprints),
        );
        explain.push("stage2_hydration", stage2_sql.clone(), None);
        let meta = self.hydrate(&sp.streams_table, &fingerprints).await?;
        let services = distinct_escaped_services(&meta);
        let lowered = stage3_predicates(sp, &fingerprints, &meta, self.metadata_budget());
        let fetch_until_limit = sp.fetch_until_limit || !lowered.metadata_lowered;
        let rx = rx_column(&CompiledPipeline::compile(&sp.pipeline)?)?;
        let stage3_sql = super::sql::stage3_with_rx(
            &self.tenant,
            &sp.samples_table,
            &services,
            &sql_literals(&fingerprints),
            super::sql::TimeWindow {
                start_ns: sp.start_ns,
                end_ns: sp.end_ns,
            },
            &lowered.predicates,
            sp.direction,
            self.effective_scan_limit(sp, fetch_until_limit),
            rx.as_ref(),
        );
        explain.push("stage3_samples", stage3_sql, None);
        Ok(explain)
    }

    async fn explain_metric(&self, mp: &MetricPlan) -> Result<PlanExplain, ReadError> {
        let result_type = if mp.answers_instant() {
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
            super::sql::stage2(
                &self.tenant,
                &mp.streams_table,
                &sql_literals(&fingerprints),
            ),
            None,
        );
        let meta = self.hydrate(&mp.streams_table, &fingerprints).await?;
        let effective = super::rollup::rollup_or_raw(mp, &meta, &self.config.samples);
        let mp: &MetricPlan = &effective;
        explain.set_routing(mp.routing.clone());
        let mut note = mp.routing.reason.clone();
        let services = if mp.rollup {
            Vec::new()
        } else {
            distinct_escaped_services(&meta)
        };
        let window = super::sql::TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        };
        // Issue #544: the EXPLAIN twin renders the same fragments the
        // execution site does, from the same function, so the reported
        // statement is the statement that runs.
        let lowered = metric_predicates(mp, &fingerprints, &meta, self.metadata_budget());
        let metric_sql = if mp.client.is_some()
            || (!lowered.metadata_lowered
                && mp
                    .metadata_lowering
                    .as_ref()
                    .is_some_and(|m| m.client_without_lowering.is_some()))
        {
            // Client-aggregated (issue M6-10): the raw full-window fetch,
            // not a SQL aggregate. Issue #227 review round 5, finding 3:
            // EXPLAIN must report the query that ACTUALLY executes — a range
            // query runs the PK-ordered sliding scan (`run_metric_client`),
            // so reporting `metric_raw_samples` here made the
            // `explain_indexes` gates validate a query we never issue.
            let pipeline = match (&mp.client, &mp.metadata_lowering) {
                (Some(client), _) => &client.pipeline,
                (None, Some(m)) => {
                    &m.client_without_lowering
                        .as_ref()
                        .expect("the restored aggregation")
                        .pipeline
                }
                (None, None) => unreachable!("one of the two conditions above holds"),
            };
            let restored = match (&mp.client, &mp.metadata_lowering) {
                (Some(client), _) => client,
                (None, Some(m)) => m
                    .client_without_lowering
                    .as_ref()
                    .expect("the restored aggregation"),
                (None, None) => unreachable!("one of the two conditions above holds"),
            };
            let rx = rx_column_for(
                &CompiledPipeline::compile(pipeline)?,
                client_needs_no_labels(mp, restored),
            )?;
            client_metric_read_sql(
                &self.tenant,
                mp,
                &services,
                &fingerprints,
                window,
                &lowered.predicates,
                rx.as_ref(),
            )
        } else {
            match mp.step_ns {
                // The execution twin's range arm (issue #507, the
                // bucketed range read). It
                // refused with the engine while the state was unreachable;
                // now that the engine serves it, an EXPLAIN that still
                // refused would be a wrong answer on a route a user can
                // reach. Both the statement and the fallback come from the
                // same two functions the engine calls, so the payload is
                // the query that runs — including the capability join,
                // whose three refusals report the client path's scan
                // because that is what would execute.
                // Issue #624, part 4: the reduced shape has its own
                // statement, so the twin matches the plan's `value` here
                // exactly as the reader does. The fallback text is the
                // client scan in both arms, because that is what would
                // execute.
                Some(_) => {
                    let rendered = match &mp.value {
                        super::sql::MetricValue::Shaped(_) | super::sql::MetricValue::Staged(_) => {
                            bucketed_range_sql(
                                &self.tenant,
                                mp,
                                &services,
                                &fingerprints,
                                &lowered.predicates,
                                self.config.rollup_res_ns,
                                super::rollup::counting_projection(mp, &meta),
                            )
                        }
                        // Issue #624, part 4: the reduced statement, unless
                        // a selected stream labelled `x` meets a parser in
                        // drop mode (part 4b, D7) or the metadata filter did
                        // not fit (§5).
                        super::sql::MetricValue::Reduced(r) => {
                            let refused = super::reduce::labelled_x_refuses(r, &meta);
                            match (refused || !lowered.metadata_lowered, mp.step_ns) {
                                (false, Some(step)) => {
                                    let resolved = super::reduce::resolve_reduced(r, &meta);
                                    super::sql::metric_range_reduced(
                                        &self.tenant,
                                        &mp.table,
                                        r,
                                        &resolved.columns,
                                        &services,
                                        &sql_literals(&resolved.fingerprints),
                                        super::sql::SlidingScan {
                                            window,
                                            lower: mp.scan_lower,
                                            grid_start_ns: mp.grid_start_ns,
                                            step_ns: step.get(),
                                            range_ns: mp.range_ns.get(),
                                        },
                                        &lowered.predicates,
                                        self.key_route_test.bucketed_statement_max_memory_bytes,
                                    )
                                    .map_err(|_| BucketGridRefusal::StepNotPositive)
                                }
                                _ => Err(BucketGridRefusal::StepNotPositive),
                            }
                        }
                    };
                    match rendered {
                        Ok(sql) => sql,
                        Err(
                            BucketGridRefusal::StepNotPositive
                            | BucketGridRefusal::AnchorAboveScanStart
                            | BucketGridRefusal::WouldOverflow
                            | BucketGridRefusal::RangeNotPositive
                            | BucketGridRefusal::RollupSource
                            | BucketGridRefusal::OffTheRollupGrid,
                        ) => {
                            // Issue #614, part 2: the route that would run.
                            let raw = super::rollup::raw_route_of(
                                mp,
                                &self.config.samples,
                                super::rollup::RawCause::Refused,
                            );
                            note = raw.routing.reason.clone();
                            explain.set_routing(raw.routing.clone());
                            client_metric_read_sql(
                                &self.tenant,
                                &raw,
                                &raw_services(mp, &services, &meta),
                                &fingerprints,
                                window,
                                &lowered.predicates,
                                rx_column_for(
                                    &CompiledPipeline::compile(
                                        &bucketed_fallback_client_agg(mp).pipeline,
                                    )?,
                                    client_needs_no_labels(mp, &bucketed_fallback_client_agg(mp)),
                                )?
                                .as_ref(),
                            )
                        }
                    }
                }
                // The shape is read here rather than above the match,
                // for the reason `run_metric_inner`'s instant arm gives.
                None => super::sql::metric_instant(
                    &self.tenant,
                    metric_source(mp),
                    &services,
                    &sql_literals(&fingerprints),
                    window,
                    mp.scan_lower,
                    &lowered.predicates,
                    // The EXPLAIN twin of the execution site above, and
                    // reached under the same `step_ns.is_none()` instant
                    // condition — so it reports the query that runs.
                    super::rollup::counting_projection(mp, &meta),
                ),
            }
        };
        explain.push("metric_read", metric_sql, Some(note));
        Ok(())
    }
}
