use super::*;

/// The bucketed range read's statement for a planned metric leaf (issues
/// #507 and #624) — the ONE implementation shared by execution and EXPLAIN,
/// the [`client_metric_read_sql`] precedent, so an EXPLAIN cannot report a
/// statement the engine would not issue.
///
/// Two statements: [`super::sql::metric_range_bucketed`] when the range
/// equals the step, [`super::sql::metric_range_sliding`] at every other
/// range (issue #624).
///
/// On the first, `lo_ns` is `grid_start_ns - step_ns`: the emit grid one
/// step below its first point, and never `mp.start_ns`, which is the SCAN
/// start and has been widened backwards by the range selector. They are
/// equal exactly when the range equals the step, which is the only case that
/// statement is chosen for, so a defect here would be invisible from this
/// path alone — `super::predicate::bucket_expr` holds the derivation and
/// `sql.rs`'s `the_bucket_anchor_is_the_emit_grid_not_the_widened_scan_start`
/// is the test that separates them.
///
/// `Err` is a fall back to the client path, never a failed query — see the
/// call site's capability join.
/// A raw read's rows, decoded as the base row or as its twin with the
/// `rx` column (issue #624, part 3a, D6), either way yielding the base row.
pub(in crate::logql) enum RxRows<'a, B, T> {
    Plain(ChRowStream<'a, B>),
    Rx(ChRowStream<'a, T>),
}

impl<B, T> RxRows<'_, B, T> {
    /// [`ChRowStream::read_bytes`] of whichever stream this is.
    pub(in crate::logql) fn read_bytes(&self) -> Option<u64> {
        match self {
            RxRows::Plain(s) => s.read_bytes(),
            RxRows::Rx(s) => s.read_bytes(),
        }
    }
}

impl<B: ChRow, T: ChRow + Into<B>> Stream for RxRows<'_, B, T> {
    type Item = Result<B, ChError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut() {
            RxRows::Plain(s) => std::pin::Pin::new(s).poll_next(cx),
            RxRows::Rx(s) => std::pin::Pin::new(s)
                .poll_next(cx)
                .map(|item| item.map(|row| row.map(Into::into))),
        }
    }
}

/// The `rx` column for `compiled`'s database `regexp` stages (issue #624,
/// part 3a, D6), or `None` when it has none.
pub(super) fn rx_column(compiled: &CompiledPipeline) -> Result<Option<CheckedFragment>, ReadError> {
    rx_column_for(compiled, false)
}

/// [`rx_column`] for a metric read (issue #624, part 3c, D7): when the
/// range step's hints say the query needs no labels every `pattern` stage
/// extracts nothing, so its element is an empty array.
pub(super) fn rx_column_for(
    compiled: &CompiledPipeline,
    no_labels: bool,
) -> Result<Option<CheckedFragment>, ReadError> {
    if compiled.rx_patterns().is_empty() {
        return Ok(None);
    }
    let skipped = vec![no_labels; compiled.rx_patterns().len()];
    Ok(Some(super::predicate::regexp_captures_column_skipping(
        compiled.rx_patterns(),
        &skipped,
    )?))
}

/// Whether `client`'s range step needs no labels (issue #624, part 3c, D7).
pub(super) fn client_needs_no_labels(mp: &MetricPlan, client: &ClientAgg) -> bool {
    super::plan::range_step_rules(
        client.range_op,
        &client.pipeline,
        client.grouping.as_deref(),
        &mp.vector_aggs,
    )
    .hints
    .no_labels
}

pub(super) fn bucketed_range_sql(
    tenant: &Tenant,
    mp: &MetricPlan,
    services: &[CheckedLiteral],
    fingerprints: &[Fingerprint],
    predicates: &[CheckedFragment],
    rollup_res_ns: u64,
    projection: ScanProjection,
) -> Result<String, BucketGridRefusal> {
    // A plan with no step has no grid at all. Expressed as a refusal
    // rather than an assertion so the statement cannot be built for one.
    let Some(step) = mp.step_ns else {
        return Err(BucketGridRefusal::StepNotPositive);
    };
    let step_ns = step.get();
    let window = super::sql::TimeWindow {
        start_ns: mp.start_ns,
        end_ns: mp.end_ns,
    };
    // Issue #614, part 2: the rollup statement, at any range.
    if mp.rollup {
        return super::sql::metric_range_rollup(
            tenant,
            metric_source(mp),
            &sql_literals(fingerprints),
            super::sql::SlidingScan {
                window,
                lower: mp.scan_lower,
                grid_start_ns: mp.grid_start_ns,
                step_ns,
                range_ns: mp.range_ns.get(),
            },
            i64::try_from(rollup_res_ns).map_err(|_| BucketGridRefusal::OffTheRollupGrid)?,
        );
    }
    // Issue #624, part 3a (D2): a `regexp` count's captures join the group
    // key. The planner rendered the pattern before it chose this route.
    let regexp = match &mp.value {
        super::sql::MetricValue::Staged(s) => s.regexp.as_ref().map(|r| {
            let indexes: Vec<usize> = r.groups.iter().map(|(_, i)| *i).collect();
            match r.parser {
                super::sql::CaptureParser::Regexp => {
                    super::predicate::regexp_group_columns(&r.pattern, &indexes)
                }
                super::sql::CaptureParser::Pattern => {
                    super::predicate::pattern_group_columns(&r.pattern, &indexes)
                }
            }
            .expect("the planner rendered this pattern (issue #624, parts 3a and 3c)")
        }),
        _ => None,
    };
    // Issue #624, part 3b: a `| json` count's key labels join the group key.
    let json = match &mp.value {
        super::sql::MetricValue::Staged(s) => match &s.json {
            Some(j) => Some(
                super::sql::json_count_columns(j.form, &j.keys)
                    .map_err(|_| BucketGridRefusal::RollupSource)?,
            ),
            None => None,
        },
        _ => None,
    };
    // Issue #624, part 3d: a `| logfmt` count's relevant fields join the
    // group key.
    let logfmt = match &mp.value {
        super::sql::MetricValue::Staged(s) => s
            .logfmt
            .as_ref()
            .map(|l| super::sql::logfmt_count_columns(&l.names)),
        _ => None,
    };
    let extraction = match (&regexp, &json, &logfmt) {
        (Some(r), _, _) => Some(super::sql::GroupExtraction::Regexp(r)),
        (None, Some(j), _) => Some(super::sql::GroupExtraction::Json(j)),
        (None, None, Some(l)) => Some(super::sql::GroupExtraction::Logfmt(l)),
        (None, None, None) => None,
    };
    // Issue #624: #507's statement when the range equals the step, which is
    // smaller and faster there; the sliding statement at every other range.
    if mp.range_ns.get() != step_ns {
        return super::sql::metric_range_sliding_projected(
            tenant,
            metric_source(mp),
            services,
            &sql_literals(fingerprints),
            super::sql::SlidingScan {
                window,
                lower: mp.scan_lower,
                grid_start_ns: mp.grid_start_ns,
                step_ns,
                range_ns: mp.range_ns.get(),
            },
            predicates,
            projection,
            extraction,
        );
    }
    let lo_ns = mp
        .grid_start_ns
        .checked_sub(step_ns)
        .ok_or(BucketGridRefusal::WouldOverflow)?;
    super::sql::metric_range_bucketed_with_regexp(
        tenant,
        metric_source(mp),
        services,
        &sql_literals(fingerprints),
        super::sql::BucketedScan {
            window,
            lower: mp.scan_lower,
            lo_ns,
            step_ns,
        },
        predicates,
        // Issue #624, part 8: `counting_projection`'s answer, from the
        // caller, which holds the hydrated streams.
        projection,
        extraction,
    )
}

/// The grouping of a `sum` directly above a lowered range aggregation
/// (issue #507). A lowered counting plan has no grouping of its own and no
/// stage the parser hints read, so the parent sum is its whole rule.
pub(super) fn parent_sum_of(mp: &MetricPlan) -> Option<ParentSum> {
    super::plan::parent_sum_rules(mp.op, false, &mp.vector_aggs, None)
}

/// A row with a structured-metadata row shape (issue #507).
pub(in crate::logql) trait CarriesMetadata {
    fn metadata_text(&self) -> &str;
}
impl CarriesMetadata for MetricInstantRow {
    fn metadata_text(&self) -> &str {
        &self.structured_metadata
    }
}
impl CarriesMetadata for MetricRangeBucketRow {
    fn metadata_text(&self) -> &str {
        &self.structured_metadata
    }
}

/// A lowered fold's half of `RangeStepRules::parent_sum` (issue #507): the
/// reserved labels a parent `sum` does not keep are removed, unless this
/// row's metadata filled the error slot, where the reference keeps the
/// ungrouped labels (`pkg/logql/log/labels.go:664-668 @ v3.7.4`). `sm_ctx`
/// is only this row's when the row carries metadata, which is why the row is
/// read first.
pub(super) fn remove_unkept_reserved<R: CarriesMetadata>(
    labels: &mut LabelSet,
    parent_sum: Option<ParentSum>,
    row: &R,
    sm_ctx: &StructuredMetadataCtx,
) {
    let Some(p) = parent_sum else {
        return;
    };
    let slot_err = !row.metadata_text().is_empty() && !sm_ctx.err.is_empty();
    if !slot_err {
        p.remove_unkept_reserved(labels);
    }
}

/// Which failures of either bucketed range statement hand the query to
/// today's route (issue #624, D4): the server's memory code, 241, and the
/// `throwIf` code, 395, which a lowered `| json` count raises on a row the
/// database does not decide (part 3b, D4). Today's route streams the same
/// query in bounded memory and parses every line itself, so it answers
/// where the statement could not. The scan budget (307) is its `422`, a
/// timeout (159) is the timeout response, and a row that does not decode is
/// a defect in the row type — none of them is a reason to read the lines.
pub(super) fn bucketed_statement_failure_goes_to_todays_route(e: &ChError) -> bool {
    matches!(
        e,
        ChError::Server {
            code: CODE_MEMORY_LIMIT_EXCEEDED | CODE_THROW_IF,
            ..
        }
    )
}

/// How a bucketed range statement's rows ended (issue #624).
///
/// One value per query, moved once and never stored in a collection, so the
/// fold is held inline rather than boxed.
#[allow(clippy::large_enum_variant)]
pub(in crate::logql) enum BucketedRows {
    /// Every row folded.
    Folded(PushdownRangeGroups),
    /// The statement failed in a way that hands the query to today's
    /// route. Carries no fold, so no row folded before the failure can
    /// reach an answer.
    TodaysRoute,
    /// The statement failed in a way today's error mapping answers.
    Failed(ChError),
}

/// Folds a bucketed range statement's rows (issue #624). A failure of the
/// statement — before its first row or after some rows folded — is
/// classified by [`bucketed_statement_failure_goes_to_todays_route`], and
/// the partial fold is dropped either way. `Err` is the fold's own refusal
/// (a cap, a surviving `__error__`), which is the query's answer.
pub(in crate::logql) async fn fold_bucketed_rows(
    mut rows: impl futures::Stream<Item = Result<MetricRangeBucketRow, ChError>> + Unpin,
    mut groups: PushdownRangeGroups,
) -> Result<BucketedRows, ReadError> {
    while let Some(row) = rows.next().await {
        match row {
            Ok(row) => groups.push_row(&row)?,
            Err(e) if bucketed_statement_failure_goes_to_todays_route(&e) => {
                return Ok(BucketedRows::TodaysRoute);
            }
            Err(e) => return Ok(BucketedRows::Failed(e)),
        }
    }
    Ok(BucketedRows::Folded(groups))
}

/// The document a `| json` count's group runs its stages over (issue #624,
/// part 3b): each present key's source with its decided text as a JSON
/// string, in key order; an absent key is omitted.
pub(super) fn json_group_document(sources: &[String], keys: &[(u8, String)]) -> String {
    let mut doc = String::from("{");
    let mut first = true;
    for (source, (present, text)) in sources.iter().zip(keys) {
        if *present == 0 {
            continue;
        }
        if !first {
            doc.push(',');
        }
        first = false;
        super::labels::push_json_string(&mut doc, source);
        doc.push(':');
        super::labels::push_json_string(&mut doc, text);
    }
    doc.push('}');
    doc
}

/// ClickHouse server exception code for `FUNCTION_THROW_IF_VALUE_IS_NON_ZERO`:
/// a lowered statement's `throwIf` on an undecided row (issues #507, #624).
const CODE_THROW_IF: i32 = 395;

/// Today's route's plan for `mp` (issue #624, part 5): an instant query
/// lowered at its one grid point is the instant query it answers — no step,
/// and the grid starting at the scan start, as an instant plan's does; any
/// other plan is itself.
pub(super) fn todays_route_plan(mp: &MetricPlan) -> Cow<'_, MetricPlan> {
    if mp.value.instant() {
        let mut copy = mp.clone();
        copy.step_ns = None;
        copy.grid_start_ns = copy.start_ns;
        Cow::Owned(copy)
    } else {
        Cow::Borrowed(mp)
    }
}

/// The services a raw read of `mp` re-injects: `services` as computed for
/// `mp`, or, for a rollup plan (which computed none), the hydrated streams'
/// (issue #614, part 2).
pub(super) fn raw_services<'a>(
    mp: &MetricPlan,
    services: &'a [CheckedLiteral],
    meta: &HashMap<Fingerprint, StreamMetaRow>,
) -> Cow<'a, [CheckedLiteral]> {
    if mp.rollup {
        Cow::Owned(distinct_escaped_services(meta))
    } else {
        Cow::Borrowed(services)
    }
}

/// A lowered plan's one-point matrix as the vector an instant query answers
/// (issue #624, part 5), one sample per series; any other result unchanged.
/// Converted before the vector aggregations, which order and select a
/// vector by value and a matrix by nothing.
pub(super) fn one_point_answer(mp: &MetricPlan, result: QueryResult) -> QueryResult {
    if !mp.value.instant() {
        return result;
    }
    match result {
        QueryResult::Matrix(series) => QueryResult::Vector(
            series
                .into_iter()
                .filter_map(|s| {
                    s.points.last().map(|(_, value)| VectorSample {
                        labels: s.labels,
                        value: *value,
                    })
                })
                .collect(),
        ),
        other => other,
    }
}

/// The client aggregation a clean bucketed chain is equivalent to (issue
/// #507, W2) — what the capability join falls back to when the grid cannot
/// be rendered.
///
/// **The pipeline is empty, and that is the whole chain, not a truncation
/// of it.** `metric_plan` lowers only a chain whose every stage is a
/// PUSHABLE line filter, and those are already compiled into
/// `mp.extra_predicates`, which [`client_metric_read_sql`] pushes into the
/// raw scan too. So the fallback re-reads the same rows and counts them
/// in-engine, which is what this query did before #507.
///
/// No grouping and no `absent_labels`: a range aggregation's `by`/`without`
/// is refused by the parser on all four lowered reducers, and
/// `absent_over_time` is not one of them.
///
/// `pub` because the hermetic corpus runner needs the same object: it has
/// no database, so it cannot issue the statement, and it evaluates a
/// lowered plan down this same fallback. Two expressions of "what a
/// lowered chain is equivalent to" would be free to drift.
pub fn bucketed_fallback_client_agg(mp: &MetricPlan) -> ClientAgg {
    // Issue #624, part 2: a staged plan carries the aggregation today's
    // route runs, its stages included.
    if let super::sql::MetricValue::Staged(s) = &mp.value {
        return s.todays_route.clone();
    }
    // Issue #624, part 4: so does a reduced one.
    if let super::sql::MetricValue::Reduced(r) = &mp.value {
        return r.todays_route.clone();
    }
    ClientAgg {
        pipeline: Vec::new(),
        value: if matches!(mp.op, RangeAggOp::BytesOverTime | RangeAggOp::BytesRate) {
            ClientValue::Bytes
        } else {
            ClientValue::Count
        },
        range_op: mp.op,
        param: None,
        absent_labels: Vec::new(),
        grouping: None,
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
pub(super) fn client_metric_read_sql(
    tenant: &Tenant,
    mp: &MetricPlan,
    services: &[CheckedLiteral],
    fingerprints: &[Fingerprint],
    window: super::sql::TimeWindow,
    predicates: &[CheckedFragment],
    rx: Option<&CheckedFragment>,
) -> String {
    // Issue #624, part 2: every reducer reads `structured_metadata`, the
    // column `MetricScanRow` decodes. `absent_over_time` read the lean
    // projection here, and the row type then refused the three-column
    // result: the query failed with a decode error wherever the selector
    // resolved a stream.
    let projection = ScanProjection::WithStructuredMetadata;
    // Issue #624, part 3a (D6): `rx` carries the captures of the `regexp`
    // stages the database runs over the stored line. Issue #624, part 5: an
    // instant query lowered at one grid point reads as the instant it is.
    if !mp.answers_instant() {
        super::sql::metric_raw_samples_sliding_with_rx(
            tenant,
            &mp.table,
            services,
            &sql_literals(fingerprints),
            window,
            mp.scan_lower,
            predicates,
            projection,
            rx,
        )
    } else {
        super::sql::metric_raw_samples_with_rx(
            tenant,
            &mp.table,
            services,
            &sql_literals(fingerprints),
            window,
            mp.scan_lower,
            predicates,
            projection,
            rx,
        )
    }
}

/// Builds the evaluation window for a planned metric leaf. The planner has
/// already validated both durations, so a `Some(step)` plan yields the
/// `Range` variant carrying two [`ValidatedDuration`]s and an instant plan
/// yields `Instant` — an inconsistent window is unrepresentable.
pub(super) fn metric_plan_window(mp: &MetricPlan) -> ClientWindow {
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

#[cfg(test)]
mod tests {
    use super::super::plan::ClientValue;
    use super::*;

    /// Review round 5, finding 3: EXPLAIN and execution share ONE SQL
    /// builder, so the reported `metric_read` query IS the query that runs —
    /// a range plan yields the PK-ordered sliding scan, an instant plan the
    /// total-order scan. (Previously EXPLAIN reported `metric_raw_samples`
    /// while a range query executed the sliding one, making the
    /// `explain_indexes` gates validate a query we never issue.)
    #[test]
    fn explain_and_execution_share_one_client_read_sql() {
        let ctx = PlanCtx {
            tenant: Box::leak(Box::new(
                pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            )),
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
        let range_sql = client_metric_read_sql(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            &range_mp,
            &svc,
            &[Fingerprint::from_raw(1)],
            window,
            &[],
            None,
        );
        assert!(
            range_sql.contains("ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC"),
            "range EXPLAIN/exec must report the sliding scan: {range_sql}"
        );
        // INSTANT -> the total-timestamp-order scan.
        let instant_mp = mk(QuerySpec::Instant {
            at_ns: 60_000_000_000,
        });
        let instant_sql = client_metric_read_sql(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            &instant_mp,
            &svc,
            &[Fingerprint::from_raw(1)],
            window,
            &[],
            None,
        );
        assert!(
            instant_sql.contains("ORDER BY timestamp_ns ASC, fingerprint ASC, body ASC"),
            "instant must keep its total order: {instant_sql}"
        );
    }

    /// Issue #507, W2 — **the capability join, one case per refusal
    /// reason.**
    ///
    /// The grid expression can refuse for three reasons and the arm falls
    /// back to the client path on each, never to an error. None of the
    /// three is reachable through the planner — `metric_plan` asserts the
    /// chain-decidable half and the request boundary caps the span — so
    /// the plans here are BUILT: a real planned plan with one field moved,
    /// which is the only way to reach the arm at all.
    ///
    /// What this pins is the join's two halves: that each reason is
    /// produced, and that the fallback object the arm hands to the client
    /// path is the aggregation the lowered chain is equivalent to. The
    /// round trip itself — that the request then answers 200 — needs a
    /// database and a seam that takes a hand-built plan; there is no such
    /// seam, and this test says so rather than implying it covers it.
    #[test]
    fn each_bucket_grid_refusal_falls_back_to_the_client_path() {
        let planned = |query: &str| {
            let expr = pulsus_logql::parse(query).expect("parse");
            let params = QueryParams {
                spec: QuerySpec::Range {
                    start_ns: 600_000_000_000,
                    end_ns: 1_200_000_000_000,
                    step_ns: 60_000_000_000,
                },
                limit: 100,
                direction: Direction::Backward,
            };
            let ctx = PlanCtx {
                tenant: Box::leak(Box::new(
                    pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                )),
                db: "pulsus",
                streams_idx: "log_streams_idx",
                streams: "log_streams",
                samples: "log_samples",
                rollup_table: "log_metrics_5s",
                rollup_res_ns: 5_000_000_000,
                scan_budget_bytes: 1024,
                max_streams: 100_000,
                pipeline_scan_factor: 10,
            };
            match plan::plan(&expr, &params, &ctx).expect("plan") {
                Plan::Metric(mp) => mp,
                _ => panic!("expected a metric plan"),
            }
        };
        let services: Vec<CheckedLiteral> = Vec::new();
        let fingerprints = [Fingerprint::from_raw(1)];

        // The control: the planned shape renders.
        let base = planned(r#"count_over_time({a="b"}[1m])"#);
        assert!(base.client.is_none(), "the fixture must be a lowered plan");
        assert!(
            bucketed_range_sql(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                &base,
                &services,
                &fingerprints,
                &[],
                5_000_000_000,
                ScanProjection::WithStructuredMetadata,
            )
            .is_ok()
        );

        // No step at all: no grid.
        let mut mp = base.clone();
        mp.step_ns = None;
        assert_eq!(
            bucketed_range_sql(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                &mp,
                &services,
                &fingerprints,
                &[],
                5_000_000_000,
                ScanProjection::WithStructuredMetadata,
            ),
            Err(BucketGridRefusal::StepNotPositive)
        );

        // The scan starts BELOW the anchor, so the early rows would
        // bucket below the grid's first point.
        let mut mp = base.clone();
        mp.start_ns = base.grid_start_ns - base.step_ns.expect("a step").get() - 1;
        assert_eq!(
            bucketed_range_sql(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                &mp,
                &services,
                &fingerprints,
                &[],
                5_000_000_000,
                ScanProjection::WithStructuredMetadata,
            ),
            Err(BucketGridRefusal::AnchorAboveScanStart)
        );

        // The widest numerator the statement could evaluate is not
        // representable.
        let mut mp = base.clone();
        mp.grid_start_ns = i64::MIN + base.step_ns.expect("a step").get();
        mp.start_ns = i64::MIN;
        mp.end_ns = i64::MAX;
        assert_eq!(
            bucketed_range_sql(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                &mp,
                &services,
                &fingerprints,
                &[],
                5_000_000_000,
                ScanProjection::WithStructuredMetadata,
            ),
            Err(BucketGridRefusal::WouldOverflow)
        );

        // The ANCHOR itself is not representable — `grid_start - step` is
        // computed here rather than by the grid expression, so it is this
        // function's own arithmetic that has to refuse. A wrapping
        // subtraction would put the anchor at the TOP of the axis and
        // report the wrong reason, which is what this row separates.
        let mut mp = base.clone();
        mp.grid_start_ns = i64::MIN;
        mp.start_ns = i64::MIN;
        assert_eq!(
            bucketed_range_sql(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
                &mp,
                &services,
                &fingerprints,
                &[],
                5_000_000_000,
                ScanProjection::WithStructuredMetadata,
            ),
            Err(BucketGridRefusal::WouldOverflow)
        );

        // The fallback the arm hands to the client path: the aggregation
        // this chain is equivalent to, with an EMPTY pipeline, because
        // every stage a lowered chain may carry is a pushable line filter
        // and those are already in `extra_predicates`.
        let fallback = bucketed_fallback_client_agg(&base);
        assert!(fallback.pipeline.is_empty());
        assert!(fallback.grouping.is_none());
        assert!(fallback.absent_labels.is_empty());
        assert_eq!(fallback.range_op, RangeAggOp::CountOverTime);
        assert!(matches!(fallback.value, ClientValue::Count));
        assert!(
            CompiledPipeline::compile(&fallback.pipeline).is_ok(),
            "the arm compiles this before running it"
        );
        // The bytes family takes the other value, and it is read off the
        // plan's own `op` rather than re-derived from the query text.
        let bytes = bucketed_fallback_client_agg(&planned(r#"bytes_rate({a="b"}[1m])"#));
        assert!(matches!(bytes.value, ClientValue::Bytes));
        assert_eq!(bytes.range_op, RangeAggOp::BytesRate);
    }
}
