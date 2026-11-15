use super::*;

impl LogQlEngine {
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
    ) -> Result<DiscoveryAnswer<DetectedLabelOut>, ReadError> {
        self.detected_labels_inner(selector, b, None).await
    }

    /// [`LogQlEngine::detected_labels`] plus its `X-Pulsus-Explain`
    /// trace, in the same single pass (no second scan) — the
    /// `query_explained` contract.
    pub async fn detected_labels_explained(
        &self,
        selector: Option<&Expr>,
        b: TimeBounds,
    ) -> Result<(DiscoveryAnswer<DetectedLabelOut>, PlanExplain), ReadError> {
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
    ) -> Result<DiscoveryAnswer<DetectedLabelOut>, ReadError> {
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
        let ctx = self.config.plan_ctx(&self.tenant);
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
        // Base labels parsed once per fingerprint, not per row (the
        // `StreamAccumulator` idiom).
        let base_labels: HashMap<Fingerprint, Vec<(String, String)>> = meta
            .iter()
            .map(|(fp, m)| (*fp, parse_canonical_labels(&m.labels)))
            .collect();
        let window = super::sql::TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        };
        // Issue #624, part 3a (D6): the database runs the `regexp` stages
        // over the stored line.
        let rx = rx_column(&compiled)?;
        if !sp.fetch_until_limit {
            // Fast path — provably complete, not just fast: with no
            // unpushed dropping stage the pipeline cannot drop a line the
            // SQL didn't already filter exactly (line-filter pushdown
            // carries the exact predicate), so this single scan's
            // `LIMIT line_limit` rows ARE the newest `line_limit`
            // post-pipeline matches (`scan_limit == line_limit` by
            // construction). Never partial.
            let sql = super::sql::stage3_with_rx(
                &self.tenant,
                &sp.samples_table,
                &services,
                &sql_literals(&fingerprints),
                window,
                &sp.line_filters,
                sp.direction,
                sp.scan_limit,
                rx.as_ref(),
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
                    .query_rows::<SampleRow, SampleRxRow>(
                        &sql,
                        &self.budget_settings(),
                        rx.is_some(),
                    )
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
                        &row.rx,
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
            let first_page_sql = super::sql::stage3_keyset_with_rx(
                &self.tenant,
                &sp.samples_table,
                &services,
                &sql_literals(&fingerprints),
                window,
                super::sql::KeysetLower::First,
                sp.direction,
                &sp.line_filters,
                sp.scan_limit.max(1),
                rx.as_ref(),
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
                rx.as_ref(),
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
        base_labels: &HashMap<Fingerprint, Vec<(String, String)>>,
        services: &[CheckedLiteral],
        fingerprints: &[Fingerprint],
        line_limit: u32,
        acc: &mut FieldAccumulator,
        rx: Option<&CheckedFragment>,
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
                    // Minted here: the cursor enters a tuple comparison,
                    // where a bare decimal above `2^64` re-delivers the
                    // boundary row walking forward and skips it walking
                    // back (issue #498).
                    tuple: (c.tuple.0, c.tuple.1.sql_literal(), c.tuple.2),
                    offset: c.seen,
                },
            };
            let sql = super::sql::stage3_keyset_with_rx(
                &self.tenant,
                &sp.samples_table,
                services,
                &sql_literals(fingerprints),
                window,
                ks_lower,
                sp.direction,
                &sp.line_filters,
                st.page_size,
                rx,
            );

            // Open, then stream-drain one page; `read_bytes` is meaningful
            // only after the drain (wait_end_of_query=1). Scoped so the
            // stream's pooled-connection lease — now held across the
            // per-row detection CPU work, bounded to this one page —
            // releases before the next page opens.
            let decision = {
                let mut stream = match self
                    .query_rows::<TailSampleRow, TailSampleRxRow>(
                        &sql,
                        &self.paging_settings(page_cap),
                        rx.is_some(),
                    )
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
}
