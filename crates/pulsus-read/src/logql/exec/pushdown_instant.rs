use super::*;

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
    base_labels: HashMap<Fingerprint, LabelSet>,
    /// Rendered final label set -> `(labels, summed count)`.
    groups: HashMap<String, (LabelSet, u64)>,
    /// Query-lifetime group bytes, never discharged: the groups ARE the
    /// result (the `apply_vector_aggs` precedent).
    charged: u64,
    caps: AggCaps,
    merge_buf: Vec<(String, String)>,
    sm_buf: Vec<(String, String)>,
    sm_ctx: StructuredMetadataCtx,
    /// A parent `sum`'s grouping (issue #507, `RangeStepRules::parent_sum`).
    parent_sum: Option<ParentSum>,
}

impl PushdownInstantGroups {
    pub(in crate::logql) fn new(meta: &HashMap<Fingerprint, StreamMetaRow>, caps: AggCaps) -> Self {
        PushdownInstantGroups {
            base_labels: meta.iter().map(|(fp, m)| (*fp, series_labels(m))).collect(),
            groups: HashMap::new(),
            charged: 0,
            caps,
            merge_buf: Vec::new(),
            sm_buf: Vec::new(),
            sm_ctx: StructuredMetadataCtx::default(),
            parent_sum: None,
        }
    }

    /// Applies a parent `sum`'s grouping at the range step (issue #507).
    pub(in crate::logql) fn with_parent_sum(mut self, parent_sum: Option<ParentSum>) -> Self {
        self.parent_sum = parent_sum;
        self
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
        let mut labels: LabelSet = if row.structured_metadata.is_empty() {
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
        remove_unkept_reserved(&mut labels, self.parent_sum, row, &self.sm_ctx);
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

#[cfg(test)]
mod tests {
    use super::super::charge::{AggCaps, PUSHDOWN_INSTANT_SLOT, group_entry_bytes};
    use super::super::plan::{ClientAgg, ClientValue};
    use super::*;

    /// Issue #507: on the three lowered folds, a parent `sum` that does not
    /// keep `__error__` removes a stream label of that name before the check;
    /// an error slot filled by the row's metadata still fails the query.
    #[test]
    fn a_parent_sum_removes_a_stream_error_label_on_the_lowered_folds() {
        use crate::logql::pipeline::ParentSum;
        let mut meta = HashMap::new();
        meta.insert(
            Fingerprint::from_raw(1),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(1),
                service: "s".to_string(),
                labels: r#"{"__error__":"s","service_name":"s"}"#.to_string(),
            },
        );
        // A stream with no `__error__` label, so a metadata `__error__` is not
        // renamed `__error___extracted` and fills the slot.
        meta.insert(
            Fingerprint::from_raw(2),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(2),
                service: "s".to_string(),
                labels: r#"{"service_name":"s"}"#.to_string(),
            },
        );
        let by_service = Some(ParentSum {
            keeps_error: false,
            keeps_preserve: false,
            keeps_unwrapped: false,
        });
        let instant = |sm: &str| MetricInstantRow {
            fingerprint: Fingerprint::from_raw(1),
            n: 1,
            structured_metadata: sm.to_string(),
        };
        let instant_fp2 = |sm: &str| MetricInstantRow {
            fingerprint: Fingerprint::from_raw(2),
            n: 1,
            structured_metadata: sm.to_string(),
        };
        assert!(
            PushdownInstantGroups::new(&meta, AggCaps::DEFAULT)
                .push_row(&instant(""))
                .is_err()
        );
        PushdownInstantGroups::new(&meta, AggCaps::DEFAULT)
            .with_parent_sum(by_service)
            .push_row(&instant(""))
            .expect("instant: removed by the parent sum");
        assert!(
            PushdownInstantGroups::new(&meta, AggCaps::DEFAULT)
                .with_parent_sum(by_service)
                .push_row(&instant_fp2(r#"{"__error__":"boom"}"#))
                .is_err(),
            "instant: a metadata error fills the slot"
        );
        let bucket = |sm: &str| MetricRangeBucketRow {
            fingerprint: Fingerprint::from_raw(1),
            bucket_ns: 60_000_000_000,
            n: 1,
            structured_metadata: sm.to_string(),
            matched: 0,
            caps: Vec::new(),
            keys: Vec::new(),
            fields: String::new(),
        };
        assert!(
            PushdownRangeGroups::new(
                &meta,
                AggCaps::DEFAULT,
                60_000_000_000,
                300_000_000_000,
                60_000_000_000,
                5
            )
            .push_row(&bucket(""))
            .is_err()
        );
        PushdownRangeGroups::new(
            &meta,
            AggCaps::DEFAULT,
            60_000_000_000,
            300_000_000_000,
            60_000_000_000,
            5,
        )
        .with_parent_sum(by_service)
        .push_row(&bucket(""))
        .expect("range: removed by the parent sum");
        // The reduced fold runs each key under the query's own rules,
        // `RangeStepRules::parent_sum` included (issue #624, part 4).
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
    fn sm_meta() -> HashMap<Fingerprint, StreamMetaRow> {
        let mut m = HashMap::new();
        m.insert(
            Fingerprint::from_raw(1),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(1),
                service: "sm".to_string(),
                labels: r#"{"app":"x","fp":"1","service_name":"sm"}"#.to_string(),
            },
        );
        m.insert(
            Fingerprint::from_raw(2),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(2),
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
            rx: Vec::new(),
        };
        vec![
            row(
                Fingerprint::from_raw(1),
                10_000_000_000,
                "alpha",
                r#"{"n":"10","trace":"a"}"#,
            ),
            row(
                Fingerprint::from_raw(1),
                20_000_000_000,
                "beta",
                r#"{"n":"20","trace":"bb"}"#,
            ),
            row(Fingerprint::from_raw(1), 30_000_000_000, "gamma", ""),
            row(
                Fingerprint::from_raw(2),
                40_000_000_000,
                "delta",
                r#"{"app":"SMVAL","trace":"a"}"#,
            ),
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
            row(Fingerprint::from_raw(1), 1, r#"{"n":"10","trace":"a"}"#),
            row(Fingerprint::from_raw(1), 1, r#"{"n":"20","trace":"bb"}"#),
            row(Fingerprint::from_raw(1), 1, ""),
            row(
                Fingerprint::from_raw(2),
                1,
                r#"{"app":"SMVAL","trace":"a"}"#,
            ),
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
            row(Fingerprint::from_raw(1), 5, r#"{"n":"10","trace":"a"}"#),
            row(Fingerprint::from_raw(1), 4, r#"{"n":"20","trace":"bb"}"#),
            row(Fingerprint::from_raw(1), 5, ""),
            row(
                Fingerprint::from_raw(2),
                5,
                r#"{"app":"SMVAL","trace":"a"}"#,
            ),
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
                fingerprint: Fingerprint::from_raw(1),
                n: 1,
                structured_metadata: String::new(),
            },
            MetricInstantRow {
                fingerprint: Fingerprint::from_raw(1),
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

    /// Issue #507, condition 3 — **the rate divisor is applied once per
    /// series, before the outer vector aggregation consumes the series**,
    /// and the two orders give different answers.
    ///
    /// The reader's order over pre-aggregated rows ends
    ///
    /// ```text
    /// 5  sum the integer partials for the series
    /// 6  apply the rate divisor ONCE, per series      <- here
    /// 7  the outer vector aggregation consumes the divided series
    /// ```
    ///
    /// Step 6 before step 7 is a choice with an observable consequence,
    /// because floating-point addition is not associative. With counts 1
    /// and 4 over a 3-second window:
    ///
    /// ```text
    /// divide first   1/3 + 4/3  =  1.6666666666666665   bits 0x3FFAAAAAAAAAAAAA
    /// divide after   (1 + 4)/3  =  1.6666666666666667   bits 0x3FFAAAAAAAAAAAAB
    /// ```
    ///
    /// The fixture is asserted to distinguish the two before either answer
    /// is checked, so the test cannot pass by both sides collapsing
    /// together.
    ///
    /// **What this reaches, and what it does not.** It drives the shipped
    /// `PushdownInstantGroups::finish` and the shipped instant
    /// vector-aggregation chain, so removing the divisor from `finish`, or
    /// changing what `finish` divides, reddens it. It does **not** reach
    /// the two statements in `run_metric_instant_pushdown` that put those
    /// two functions in that order — reaching those needs a database
    /// stream, and no hermetic test reaches them.
    #[test]
    fn ac32_divisor_precedes_the_vector_aggregation() {
        const DIVIDE_FIRST_BITS: u64 = 0x3FFA_AAAA_AAAA_AAAA;
        const DIVIDE_AFTER_BITS: u64 = 0x3FFA_AAAA_AAAA_AAAB;
        let window_ns = Some(3_000_000_000u64);

        // The fixture can tell the two orders apart at all.
        assert_eq!((1.0f64 / 3.0 + 4.0f64 / 3.0).to_bits(), DIVIDE_FIRST_BITS);
        assert_eq!((5.0f64 / 3.0).to_bits(), DIVIDE_AFTER_BITS);
        assert_ne!(
            DIVIDE_FIRST_BITS, DIVIDE_AFTER_BITS,
            "1/3 + 4/3 and 5/3 must be different doubles, or this fixture \
             asserts nothing about the order"
        );

        // Two streams, so the outer aggregation has two series to fold.
        let rows = vec![
            MetricInstantRow {
                fingerprint: Fingerprint::from_raw(1),
                n: 1,
                structured_metadata: String::new(),
            },
            MetricInstantRow {
                fingerprint: Fingerprint::from_raw(2),
                n: 4,
                structured_metadata: String::new(),
            },
        ];
        let fold = |rate_window_ns: Option<u64>| -> Vec<InstantSeries> {
            let meta = sm_meta();
            let mut g = PushdownInstantGroups::new(&meta, AggCaps::DEFAULT);
            for r in &rows {
                g.push_row(r).expect("under the cap");
            }
            g.finish(rate_window_ns)
        };
        let sum_chain = |series: Vec<InstantSeries>| -> Vec<InstantSeries> {
            charged_instant_chain(
                series,
                &[(pulsus_logql::VectorAggOp::Sum, None, None)],
                MAX_POST_AGG_BYTES,
            )
            .expect("under the cap")
        };

        // Step 6: `finish` hands back series that are ALREADY divided.
        let divided = fold(window_ns);
        assert_eq!(divided.len(), 2, "two streams must give two series");
        let mut per_series: Vec<u64> = divided.iter().map(|s| s.value.to_bits()).collect();
        per_series.sort_unstable();
        assert_eq!(
            per_series,
            vec![(1.0f64 / 3.0).to_bits(), (4.0f64 / 3.0).to_bits()],
            "the divisor must already have been applied per series"
        );

        // Step 7 over the divided series is the shipped answer.
        let shipped = sum_chain(divided);
        assert_eq!(shipped.len(), 1, "an ungrouped sum is one series");
        assert_eq!(
            shipped[0].value.to_bits(),
            DIVIDE_FIRST_BITS,
            "the shipped order divides each series before summing them"
        );
        assert_ne!(
            shipped[0].value.to_bits(),
            DIVIDE_AFTER_BITS,
            "dividing after the vector aggregation would be a different double"
        );

        // The other order, built from the same two functions: sum the
        // undivided counts, then divide. It is the value the shipped order
        // must NOT produce.
        let after = sum_chain(fold(None));
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].value.to_bits(), 5.0f64.to_bits());
        assert_eq!(
            apply_rate(after[0].value, window_ns).to_bits(),
            DIVIDE_AFTER_BITS,
            "the reversed order is the other double, so the assertion above \
             is about the order and not about the arithmetic"
        );
    }

    /// Issue #507, W3, arm 3 — **the proviso is checked, and a stream
    /// label of the same name suppresses the fragment.**
    ///
    /// The parsed-name fragment reads a key of the line, which is the
    /// label the evaluator resolved only while no selected stream carries
    /// a label of that name. Measured through the shipped pipeline: over
    /// `{"s":"linevalue","x":1}` with a stream label `s="streamvalue"`,
    /// `| json` yields `s = streamvalue` and `s_extracted = linevalue` —
    /// the stream label wins and the line's key is renamed — so a
    /// fragment comparing `JSONExtractString(body,'s')` would drop a row
    /// the evaluator keeps.
    ///
    /// The two halves of this test differ ONLY in whether a resolved
    /// stream carries the name, so neither passes for another reason.
    #[test]
    fn a_stream_label_of_the_same_name_suppresses_the_parsed_fragment() {
        let query = r#"{service_name="sm"} | json | lvl="error""#;
        let expr = pulsus_logql::parse(query).expect("parse");
        let params = QueryParams {
            spec: QuerySpec::Instant {
                at_ns: 60_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let ctx = PlanCtx {
            tenant: Box::leak(Box::new(
                pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            )),
            db: "d",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 1 << 30,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        };
        let Plan::Streams(sp) = plan::plan(&expr, &params, &ctx).expect("plan") else {
            panic!("expected a streams plan")
        };
        assert_eq!(
            super::super::plan::compile_parsed_label_filters(&sp.pipeline).len(),
            1,
            "the control: this chain compiles a fragment at plan time"
        );

        // No resolved stream carries `lvl`, so the proviso holds.
        let meta0 = sm_meta();
        let fps0: Vec<Fingerprint> = meta0.keys().copied().collect();
        let without = stage3_predicates(
            &sp,
            &fps0,
            &meta0,
            super::super::predicate::MAX_METADATA_FRAGMENT_BYTES,
        )
        .predicates;
        assert_eq!(
            without.len(),
            sp.line_filters.len() + 1,
            "the fragment is included when no stream carries the name"
        );
        assert!(
            without
                .last()
                .expect("a fragment")
                .as_sql()
                .contains("'lvl'")
        );

        // One resolved stream carries `lvl` as a label, so it does not.
        let mut meta = sm_meta();
        meta.insert(
            Fingerprint::from_raw(3),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(3),
                service: "sm".to_string(),
                labels: r#"{"lvl":"warn","service_name":"sm"}"#.to_string(),
            },
        );
        let fps: Vec<Fingerprint> = meta.keys().copied().collect();
        let with = stage3_predicates(
            &sp,
            &fps,
            &meta,
            super::super::predicate::MAX_METADATA_FRAGMENT_BYTES,
        )
        .predicates;
        assert_eq!(
            with.len(),
            sp.line_filters.len(),
            "one stream carrying the name is enough to suppress it"
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
                fingerprint: Fingerprint::from_raw(1),
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
                fingerprint: Fingerprint::from_raw(1),
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
