use super::*;

/// One poll's reads, held until [`commit_tail_attempt`] keeps them (issue
/// #804).
#[derive(Debug)]
pub struct TailAttempt {
    pub(super) page: TailPage,
    /// Resolved, not yet in `setup.resolved`; sorted.
    pub(super) added: Vec<Fingerprint>,
    /// Read by this poll's stage 2.
    pub(super) labels: HashMap<Fingerprint, StreamMetaRow>,
    /// `labels`' keys, sorted.
    pub(super) joined: Vec<Fingerprint>,
    /// The lower bound is the setup start.
    pub(super) first_window: bool,
    pub(super) full_scan: bool,
    pub(super) lower_ns: i64,
}

/// Keeps what an accepted [`TailAttempt`] read (issue #804) and returns its
/// page. On the first window the joined streams are admitted with it;
/// otherwise they are reported in [`TailPage::joined`] for a catch-up.
///
/// Only an accepted attempt is kept (issue #803): an `Err` attempt, or an
/// `Ok` one that met a server failure, writes nothing, and the latter
/// becomes the failure's error, so no page reaches the producer and the
/// poll is repeated at the same lower bound.
pub fn commit_tail_attempt(
    setup: &mut TailSetup,
    attempt: Result<TailAttempt, ReadError>,
    failure: Option<ServerFailure>,
) -> Result<TailPage, ReadError> {
    let attempt = attempt?;
    if let Some(f) = failure {
        return Err(ReadError::Clickhouse(f.into()));
    }
    merge_resolved(&mut setup.resolved, &attempt.added);
    setup.hydrated.extend(attempt.labels);
    if attempt.full_scan {
        setup.last_full_scan_lower_ns = setup.last_full_scan_lower_ns.max(attempt.lower_ns);
    }
    let mut page = attempt.page;
    if attempt.first_window {
        merge_resolved(&mut setup.deliverable, &attempt.joined);
    } else {
        page.joined = attempt.joined;
    }
    Ok(page)
}

/// A tail planned to a metric query: the setup refuses it, and the
/// full-span stage 1 (issue #804) replans the same expression, so it
/// cannot meet one.
pub(super) fn tail_needs_a_log_query() -> ReadError {
    ReadError::PipelineInvalid {
        reason: "tail requires a log stream query (metric queries cannot be tailed)".to_string(),
    }
}

/// The `start` bound of a [`QuerySpec`] (the `at` instant for an instant
/// query) — the initial covered-window lower anchor for [`TailSetup`]
/// (issue #94).
pub(super) fn spec_start_ns(spec: &QuerySpec) -> i64 {
    match *spec {
        QuerySpec::Range { start_ns, .. } => start_ns,
        QuerySpec::Instant { at_ns } => at_ns,
    }
}

/// The `end` bound of a [`QuerySpec`] (the `at` instant for an instant
/// query) — the initial covered-window upper anchor for [`TailSetup`]
/// (issue #94).
pub(super) fn spec_end_ns(spec: &QuerySpec) -> i64 {
    match *spec {
        QuerySpec::Range { end_ns, .. } => end_ns,
        QuerySpec::Instant { at_ns } => at_ns,
    }
}

/// Client-free core of [`LogQlEngine::tail_refresh_months`] (issue #94
/// v6-v8, scan-gated phase split), unit-testable with a [`PlanCtx`]
/// literal.
///
/// `narrow == true` is the producer's certification that a COMPLETED
/// full-span live-edge poll has already dwelt at least
/// [`TAIL_REGISTRATION_GRACE_NS`] (the scan gate — see
/// `logs_api/tail.rs::producer_loop`); ONLY then does `scan_floor_ns`
/// advance, monotonically, to `max(scan_floor_ns, lower_ns - GRACE)`.
/// Otherwise (catch-up, fall-behind, or still inside the hold) the floor
/// stays frozen and stage-1 re-plans (reusing [`plan::plan`] — **no
/// ClickHouse I/O**, a pure `stage1_sql` rebuild) to
/// `months_overlapping(scan_floor_ns, upper_ns)` whenever that window's
/// covered `(lo_month, hi_month)` pair differs from
/// `setup.covered_months` — widening upper-only, never narrowing, so a
/// connection that never reaches the live edge keeps the full-span
/// (request-bounded, not lifetime-growing) behaviour. Narrowing past a
/// fingerprint's registration month is safe ONLY because
/// [`TailSetup::resolved`] remembers it — this function alone does not
/// decide correctness. A non-`Streams` plan (unreachable — the setup expr
/// already planned as `Streams`) or a re-plan error leaves `setup`
/// untouched, so the caller can swallow the result and keep tailing on
/// the prior month set.
pub(super) fn refresh_tail_months(
    ctx: &PlanCtx<'_>,
    setup: &mut TailSetup,
    lower_ns: i64,
    upper_ns: i64,
    narrow: bool,
) -> Result<(), ReadError> {
    if narrow {
        // The ONLY place the floor advances: `narrow` certifies a
        // completed full-span scan already dwelt >= GRACE at the live
        // edge (the scan-gate rule), so this can never skip a boundary in
        // compressed (catch-up) time.
        setup.scan_floor_ns = setup
            .scan_floor_ns
            .max(lower_ns.saturating_sub(TAIL_REGISTRATION_GRACE_NS));
    } // else: catch-up, fall-behind, or in-hold — floor frozen, set widens upper-only.
    let hi = upper_ns.max(lower_ns);
    let want = (plan::year_month(setup.scan_floor_ns), plan::year_month(hi));
    if want == setup.covered_months {
        return Ok(());
    }
    let mut qp = setup.base_params; // QueryParams: Copy
    if let QuerySpec::Range {
        start_ns, end_ns, ..
    } = &mut qp.spec
    {
        *start_ns = setup.scan_floor_ns;
        *end_ns = hi;
    }
    if let Plan::Streams(sp) = plan::plan(&setup.expr, &qp, ctx)? {
        setup.plan = sp;
        setup.covered_months = want;
    }
    Ok(())
}

/// Unions `new` into the cumulative resolved-fingerprint cache, sorted
/// and deduped (issue #94: the orphan-cache mechanism — a fingerprint
/// present in an earlier batch survives a later batch that no longer
/// resolves it, because its stage-1 month scrolled out of the current
/// poll window).
pub(super) fn merge_resolved(cache: &mut Vec<Fingerprint>, new: &[Fingerprint]) {
    if new.is_empty() {
        return;
    }
    cache.extend_from_slice(new);
    cache.sort_unstable();
    cache.dedup();
}

/// The occurrence-count cursor update (round-4 adjudication #1): the new
/// tuple is the LAST raw row's; `seen` counts this page's trailing run
/// of rows equal to it, plus the previous `seen` when the tuple did not
/// change (the `OFFSET` already skipped those). Equal-tuple rows are
/// adjacent under the total `ORDER BY` (raw `body` tiebreaker), so the
/// trailing-run count is deterministic even under hash collisions. An
/// empty page leaves the cursor unchanged.
///
/// **No production caller since issue #312** — every paged drain now
/// walks its page incrementally through
/// [`TailCursorTracker`](super::detected_probe::TailCursorTracker). This
/// survives as that tracker's INDEPENDENT ORACLE: the already-committed
/// `tail_cursor_tracker_matches_advance_tail_cursor_over_randomized_sequences`
/// (`detected_probe.rs`) is what proves the tracker computes the same
/// function, so deleting this as "dead code" would take the tracker's
/// only independent check with it.
#[cfg(test)]
pub(in crate::logql) fn advance_tail_cursor(
    prev: Option<TailCursor>,
    rows: &[TailSampleRow],
) -> Option<TailCursor> {
    let last = match rows.last() {
        Some(last) => last,
        None => return prev,
    };
    let bt = (last.timestamp_ns, last.fingerprint, last.body_hash);
    let run = rows
        .iter()
        .rev()
        .take_while(|r| (r.timestamp_ns, r.fingerprint, r.body_hash) == bt)
        .count() as u32;
    let carry = match prev {
        Some(c) if c.tuple == bt => c.seen,
        _ => 0,
    };
    Some(TailCursor {
        tuple: bt,
        seen: run.saturating_add(carry),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- tail month-boundary refresh (issue #94 item 2) -----------------

    const DAY_NS: i64 = 86_400_000_000_000;

    fn tail_test_ctx() -> PlanCtx<'static> {
        PlanCtx {
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
        }
    }

    /// Builds a `TailSetup` client-free (no engine/DB) — the shape
    /// `LogQlEngine::tail_setup` produces, so `refresh_tail_months` can be
    /// exercised against a `PlanCtx` literal.
    fn build_tail_setup(ctx: &PlanCtx<'_>, query: &str, start_ns: i64, end_ns: i64) -> TailSetup {
        let expr = pulsus_logql::parse(query).expect("parse");
        let params = QueryParams {
            spec: QuerySpec::Range {
                start_ns,
                end_ns,
                step_ns: 1_000_000_000,
            },
            limit: 100,
            direction: Direction::Forward,
        };
        match plan::plan(&expr, &params, ctx).expect("plan") {
            Plan::Streams(sp) => {
                let compiled = CompiledPipeline::compile(&sp.pipeline).expect("compile");
                TailSetup {
                    plan: sp,
                    compiled,
                    expr,
                    base_params: params,
                    scan_floor_ns: start_ns,
                    covered_months: (plan::year_month(start_ns), plan::year_month(end_ns)),
                    resolved: Vec::new(),
                    opts: ResponseOptions::default(),
                    hydrated: HashMap::new(),
                    deliverable: Vec::new(),
                    last_full_scan_lower_ns: start_ns,
                }
            }
            _ => panic!("stream selector must plan to Plan::Streams"),
        }
    }

    /// Issue #803 (T7, with #804's state): a tail poll whose server failed
    /// keeps nothing it read — not the resolved union, the labels, the
    /// admitted streams or the full-scan mark — and its page never reaches
    /// the producer; an accepted poll keeps all of them.
    #[test]
    fn a_failed_tail_attempt_leaves_resolved_as_it_was() {
        let fp = Fingerprint::from_raw;
        let ctx = tail_test_ctx();
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, 0, DAY_NS);
        setup.resolved = vec![fp(1)];
        let before_mark = setup.last_full_scan_lower_ns;
        let attempt = || TailAttempt {
            page: TailPage {
                streams: Vec::new(),
                next: None,
                fetched: 7,
                joined: Vec::new(),
            },
            added: vec![fp(2)],
            labels: [(fp(2), meta_row(2))].into_iter().collect(),
            joined: vec![fp(2)],
            first_window: true,
            full_scan: true,
            lower_ns: 500,
        };
        let failure = ServerFailure {
            endpoint: 0,
            message: "reset".to_string(),
        };
        let unchanged = |setup: &TailSetup, what: &str| {
            assert_eq!(setup.resolved, vec![fp(1)], "resolved after {what}");
            assert!(setup.hydrated.is_empty(), "hydrated after {what}");
            assert!(setup.deliverable.is_empty(), "deliverable after {what}");
            assert_eq!(
                setup.last_full_scan_lower_ns, before_mark,
                "full-scan mark after {what}"
            );
        };

        let r = commit_tail_attempt(&mut setup, Ok(attempt()), Some(failure));
        assert!(
            r.is_err(),
            "an Ok attempt that met a server failure is an error"
        );
        unchanged(&setup, "a failed Ok attempt");

        let r = commit_tail_attempt(
            &mut setup,
            Err(ReadError::PipelineInvalid {
                reason: "boom".to_string(),
            }),
            None,
        );
        assert!(r.is_err());
        unchanged(&setup, "an Err attempt");

        let r = commit_tail_attempt(&mut setup, Ok(attempt()), None);
        assert_eq!(r.expect("an accepted attempt").fetched, 7);
        assert_eq!(setup.resolved, vec![fp(1), fp(2)]);
        assert!(setup.hydrated.contains_key(&fp(2)));
        assert_eq!(setup.deliverable, vec![fp(2)]);
        assert_eq!(setup.last_full_scan_lower_ns, before_mark.max(500));
    }

    fn month_literal(ts_ns: i64) -> String {
        let (y, m) = plan::year_month(ts_ns);
        format!("'{y:04}-{m:02}-01'")
    }

    /// Counts occurrences of a quoted ClickHouse `Date` literal
    /// (`'YYYY-MM-01'`) in a SQL string — the exact shape
    /// `months_overlapping` emits (`plan.rs`).
    fn count_month_literals(sql: &str) -> usize {
        let bytes = sql.as_bytes();
        let mut count = 0;
        let mut i = 0;
        while i + 12 <= bytes.len() {
            let is_literal = bytes[i] == b'\''
                && bytes[i + 1..i + 5].iter().all(u8::is_ascii_digit)
                && bytes[i + 5] == b'-'
                && bytes[i + 6..i + 8].iter().all(u8::is_ascii_digit)
                && bytes[i + 8] == b'-'
                && bytes[i + 9] == b'0'
                && bytes[i + 10] == b'1'
                && bytes[i + 11] == b'\'';
            if is_literal {
                count += 1;
                i += 12;
            } else {
                i += 1;
            }
        }
        count
    }

    /// Dec 1 2023 00:00:00 UTC, in ns — a fixed month-boundary instant
    /// reused across the U1-U8 scan-gate tests.
    const DEC_1_2023_NS: i64 = 1_701_388_800_000_000_000;
    /// Nov 1 2023 00:00:00 UTC, in ns.
    const NOV_1_2023_NS: i64 = 1_698_796_800_000_000_000;

    // -- issue #804: the catch-up ceiling and the poll commit ---------------

    /// Issue #804 U1: a catch-up's ceiling admits exactly the positions the
    /// main scan has passed, on both lower-bound modes.
    #[test]
    fn tail_ceiling_admits_exactly_the_positions_the_main_scan_passed() {
        let fp = Fingerprint::from_raw;
        assert_eq!(
            TailCeiling::of(TailLower::Start { start_ns: 100 }),
            TailCeiling::AtOrBelow(100)
        );
        let c = TailCursor {
            tuple: (100, fp(5), 7),
            seen: 1,
        };
        assert_eq!(
            TailCeiling::of(TailLower::After(c)),
            TailCeiling::Before(100, fp(5))
        );
        assert!(TailCeiling::AtOrBelow(100).admits(100, fp(9)));
        assert!(!TailCeiling::AtOrBelow(100).admits(101, fp(1)));
        assert!(TailCeiling::Before(100, fp(5)).admits(100, fp(3)));
        assert!(!TailCeiling::Before(100, fp(5)).admits(100, fp(9)));
        assert!(TailCeiling::Before(100, fp(5)).admits(99, fp(9)));
        assert_eq!(TailCeiling::AtOrBelow(100).ts(), 100);
        assert_eq!(TailCeiling::Before(100, fp(5)).ts(), 100);
    }

    fn meta_row(fp: u128) -> StreamMetaRow {
        StreamMetaRow {
            fingerprint: Fingerprint::from_raw(fp),
            service: "checkout".to_string(),
            labels: format!(r#"{{"pod":"{fp}"}}"#),
        }
    }

    fn empty_page() -> TailPage {
        TailPage {
            streams: Vec::new(),
            next: None,
            fetched: 0,
            joined: Vec::new(),
        }
    }

    /// Issue #804 U3: an accepted attempt keeps what it read. On the first
    /// window its joined streams are admitted; after it they are reported
    /// for a catch-up and not admitted.
    #[test]
    fn commit_tail_attempt_keeps_what_the_attempt_read() {
        let fp = Fingerprint::from_raw;
        let ctx = tail_test_ctx();
        // A setup start of 0, below both attempts' lower bounds, so the
        // full-scan mark is the attempt's.
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, 0, 1_000);
        setup.resolved = vec![fp(1)];

        let a = TailAttempt {
            page: empty_page(),
            added: vec![fp(2)],
            labels: [(fp(1), meta_row(1)), (fp(2), meta_row(2))]
                .into_iter()
                .collect(),
            joined: vec![fp(1), fp(2)],
            first_window: true,
            full_scan: true,
            lower_ns: 500,
        };
        let page = commit_tail_attempt(&mut setup, Ok(a), None).expect("accepted");
        assert_eq!(setup.resolved, vec![fp(1), fp(2)]);
        let mut keys: Vec<Fingerprint> = setup.hydrated.keys().copied().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![fp(1), fp(2)]);
        assert_eq!(setup.deliverable, vec![fp(1), fp(2)]);
        assert_eq!(setup.last_full_scan_lower_ns, 500);
        assert_eq!(page.joined, Vec::<Fingerprint>::new());

        let b = TailAttempt {
            page: empty_page(),
            added: Vec::new(),
            labels: [(fp(3), meta_row(3))].into_iter().collect(),
            joined: vec![fp(3)],
            first_window: false,
            full_scan: false,
            lower_ns: 900,
        };
        let page = commit_tail_attempt(&mut setup, Ok(b), None).expect("accepted");
        let mut keys: Vec<Fingerprint> = setup.hydrated.keys().copied().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![fp(1), fp(2), fp(3)]);
        assert_eq!(setup.deliverable, vec![fp(1), fp(2)]);
        assert_eq!(setup.last_full_scan_lower_ns, 500);
        assert_eq!(page.joined, vec![fp(3)]);
    }

    /// AC3(c) (catch-up phase, `narrow=false`): a refresh whose
    /// `[scan_floor_ns, upper_ns]` covers the SAME `(lo_month, hi_month)`
    /// pair the plan already covers leaves `stage1_sql` byte-identical (no
    /// re-plan, no fire).
    #[test]
    fn tail_refresh_months_is_a_noop_when_the_covered_window_is_unchanged() {
        let ctx = tail_test_ctx();
        // 2023-11-14T22:13:20Z — comfortably mid-month.
        let setup_end = 1_700_000_000_000_000_000i64;
        let setup_start = setup_end - DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);
        let sql0 = setup.plan.stage1_sql.clone();
        let covered0 = setup.covered_months;

        let same_month_upper = setup_end + 3_600_000_000_000; // +1h, still November
        assert_eq!(
            (
                plan::year_month(setup_start),
                plan::year_month(same_month_upper)
            ),
            covered0,
            "lower/upper stay within the setup's covered months"
        );
        refresh_tail_months(&ctx, &mut setup, setup_start, same_month_upper, false)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.plan.stage1_sql, sql0,
            "no-op keeps stage1_sql byte-identical"
        );
        assert_eq!(setup.covered_months, covered0);
    }

    /// AC3(a)+(b)+(c), adapted to the v6-v8 scan-gated phase split: (a) a
    /// catch-up (`narrow=false`) window straddling a month boundary keeps
    /// BOTH month literals (full-span from the frozen floor); (b) only
    /// once the scan gate certifies narrowing (`narrow=true`) at a window
    /// wholly in the later month is the STALE month dropped (the growth
    /// bound); (c) a repeat refresh over the same covered window is a
    /// byte-identical no-op.
    #[test]
    fn tail_refresh_months_straddles_then_narrows_dropping_the_stale_month() {
        let ctx = tail_test_ctx();
        let setup_start = 1_700_000_000_000_000_000i64; // November 2023
        let setup_end = setup_start + 3_600_000_000_000; // +1h, same month
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);
        let month_a_lit = month_literal(setup_start);

        // (a) straddle, still catch-up (narrow=false): lower stays in
        // November, upper crosses into December — both months must
        // resolve or a prior-month stream vanishes mid-straddle.
        let straddle_upper = setup_start + 40 * DAY_NS;
        let month_b_lit = month_literal(straddle_upper);
        assert_ne!(month_a_lit, month_b_lit, "the test crosses a month");
        refresh_tail_months(&ctx, &mut setup, setup_start, straddle_upper, false)
            .expect("no I/O, cannot fail");
        assert!(
            setup.plan.stage1_sql.contains(&month_a_lit)
                && setup.plan.stage1_sql.contains(&month_b_lit),
            "straddling catch-up window covers both months: {}",
            setup.plan.stage1_sql
        );

        // (b) narrow (scan gate open, `narrow=true`): the poll window
        // advances wholly into December, well past GRACE — the stale
        // November month must be DROPPED (the growth bound).
        let narrowed_lower = straddle_upper;
        let narrowed_upper = straddle_upper + 3_600_000_000_000;
        assert_eq!(
            plan::year_month(narrowed_lower),
            plan::year_month(narrowed_upper),
            "the narrowed window stays within December"
        );
        refresh_tail_months(&ctx, &mut setup, narrowed_lower, narrowed_upper, true)
            .expect("no I/O, cannot fail");
        assert!(
            setup.plan.stage1_sql.contains(&month_b_lit),
            "narrowed stage1_sql still covers December: {}",
            setup.plan.stage1_sql
        );
        assert!(
            !setup.plan.stage1_sql.contains(&month_a_lit),
            "narrowed stage1_sql must DROP the stale November month: {}",
            setup.plan.stage1_sql
        );
        assert_eq!(
            setup.covered_months,
            (
                plan::year_month(narrowed_lower),
                plan::year_month(narrowed_upper)
            )
        );

        // (c) a repeat call over the same covered window is a no-op.
        let sql_after = setup.plan.stage1_sql.clone();
        refresh_tail_months(&ctx, &mut setup, narrowed_lower, narrowed_upper, true)
            .expect("no I/O");
        assert_eq!(
            setup.plan.stage1_sql, sql_after,
            "no double-fire over an unchanged covered window"
        );
    }

    /// U1 (issue #94 v6-v8): during catch-up (`narrow=false`) the scan set
    /// stays FULL-SPAN from the frozen `scan_floor_ns` no matter how many
    /// month boundaries the poll window's upper edge crosses — the
    /// pre-#94-v6 behaviour, request-bounded. Fails under a
    /// floor-always-advances mutation (refresh ignoring `narrow`).
    #[test]
    fn tail_refresh_months_u1_catchup_stays_full_span_with_the_floor_frozen() {
        let ctx = tail_test_ctx();
        let setup_start = NOV_1_2023_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);
        let setup_month_lit = month_literal(setup_start);

        // Lowers jump forward 20 days/step, narrow=false throughout —
        // crosses at least 3 month boundaries (Nov->Dec->Jan->Feb).
        let mut lower = setup_start;
        for step in 0..6 {
            let upper = lower + 20 * DAY_NS;
            refresh_tail_months(&ctx, &mut setup, lower, upper, false)
                .expect("no I/O, cannot fail");
            assert!(
                setup.plan.stage1_sql.contains(&setup_month_lit),
                "step {step}: catch-up (narrow=false) must stay full-span from the setup \
                 floor: {}",
                setup.plan.stage1_sql
            );
            let expected = plan::months_overlapping(setup_start, upper).len();
            assert_eq!(
                count_month_literals(&setup.plan.stage1_sql),
                expected,
                "step {step}: catch-up's full-span set == months_overlapping(setup_start, \
                 upper)"
            );
            assert_eq!(
                setup.scan_floor_ns, setup_start,
                "step {step}: floor stays frozen throughout catch-up"
            );
            lower = upper;
        }
    }

    /// U2 (issue #94 v6-v8): at the scan gate (`narrow=true`), a `lower`
    /// within GRACE of a month start keeps the PREVIOUS month in the scan
    /// set (the registration-lag band); once `lower` passes GRACE, the
    /// previous month is dropped. A same-window repeat is a byte-identical
    /// no-op. Fails under `TAIL_REGISTRATION_GRACE_NS = 0`.
    #[test]
    fn tail_refresh_months_u2_grace_band_keeps_the_previous_month_within_grace() {
        let ctx = tail_test_ctx();
        // The clamp arm (scan_floor_ns starts here) sits well over a year
        // before December, so every narrow=true call below binds on the
        // `lower - GRACE` arm, never the clamp.
        let setup_start = DEC_1_2023_NS - 400 * DAY_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);

        // Within GRACE: lower is 30min past the December boundary (<
        // GRACE=1h) ⇒ lower-GRACE lands in November ⇒ both months.
        let lower_in_band = DEC_1_2023_NS + 30 * 60_000_000_000; // +30min
        let upper = lower_in_band + 60_000_000_000; // +60s
        refresh_tail_months(&ctx, &mut setup, lower_in_band, upper, true)
            .expect("no I/O, cannot fail");
        assert_eq!(
            count_month_literals(&setup.plan.stage1_sql),
            2,
            "within GRACE: the previous month is retained: {}",
            setup.plan.stage1_sql
        );

        // A same-window repeat is a byte-identical no-op.
        let sql_after_band = setup.plan.stage1_sql.clone();
        refresh_tail_months(&ctx, &mut setup, lower_in_band, upper, true)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.plan.stage1_sql, sql_after_band,
            "byte-identical no-op over an unchanged covered window"
        );

        // Past GRACE: lower advances beyond the boundary + GRACE ⇒ the
        // previous month is dropped.
        let lower_past_band = DEC_1_2023_NS + TAIL_REGISTRATION_GRACE_NS + 60_000_000_000;
        let upper2 = lower_past_band + 60_000_000_000;
        refresh_tail_months(&ctx, &mut setup, lower_past_band, upper2, true)
            .expect("no I/O, cannot fail");
        assert_eq!(
            count_month_literals(&setup.plan.stage1_sql),
            1,
            "past GRACE: the previous month is dropped: {}",
            setup.plan.stage1_sql
        );
    }

    /// U3 (issue #94 v6-v8): a live-advanced floor stays FROZEN through a
    /// fall-behind episode (`narrow=false`, upper crossing a month — the
    /// set widens upper-only, never narrows) and resumes advancing (stale
    /// months dropping again) once the connection re-enters the scan gate
    /// (`narrow=true`).
    #[test]
    fn tail_refresh_months_u3_fall_behind_freezes_the_floor_then_resumes_on_reentry() {
        let ctx = tail_test_ctx();
        let setup_start = NOV_1_2023_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);

        // Live-advance the floor deep into December.
        let live_lower = DEC_1_2023_NS + 2 * 3_600_000_000_000; // Dec 1 + 2h
        refresh_tail_months(
            &ctx,
            &mut setup,
            live_lower,
            live_lower + 60_000_000_000,
            true,
        )
        .expect("no I/O, cannot fail");
        let floor_after_live = setup.scan_floor_ns;
        assert!(
            floor_after_live > setup_start,
            "floor advanced off the setup start"
        );
        assert_eq!(
            count_month_literals(&setup.plan.stage1_sql),
            1,
            "narrowed to December alone: {}",
            setup.plan.stage1_sql
        );

        // Fall behind (narrow=false): upper crosses into January — the
        // floor's month must be RETAINED (never reset to setup_start), the
        // set widens upper-only.
        let jan_1 = DEC_1_2023_NS + 31 * DAY_NS;
        refresh_tail_months(&ctx, &mut setup, live_lower, jan_1 + DAY_NS, false)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.scan_floor_ns, floor_after_live,
            "floor frozen while fallen behind"
        );
        assert!(
            setup
                .plan
                .stage1_sql
                .contains(&month_literal(DEC_1_2023_NS))
                && setup.plan.stage1_sql.contains(&month_literal(jan_1)),
            "widened upper-only: keeps December AND adds January: {}",
            setup.plan.stage1_sql
        );

        // Re-entry (narrow=true): the floor resumes advancing — stale
        // December drops.
        let live_lower2 = jan_1 + 2 * 3_600_000_000_000;
        refresh_tail_months(
            &ctx,
            &mut setup,
            live_lower2,
            live_lower2 + 60_000_000_000,
            true,
        )
        .expect("no I/O, cannot fail");
        assert!(
            setup.scan_floor_ns > floor_after_live,
            "floor resumed advancing on re-entry"
        );
        assert!(
            !setup
                .plan
                .stage1_sql
                .contains(&month_literal(DEC_1_2023_NS)),
            "stale December dropped once the floor resumes: {}",
            setup.plan.stage1_sql
        );
    }

    /// U4 (issue #94 AC2, updated for the v6-v8 scan-gated phase split,
    /// "bound the tail month IN-list growth"): once the scan gate has
    /// opened (`narrow=true`), the LIVE poll window's own width (not the
    /// connection's elapsed lifetime) determines `stage1_sql`'s month
    /// literal count. 36 steps span ~3 elapsed years; the count never
    /// grows.
    #[test]
    fn tail_refresh_months_stays_bounded_over_a_long_lived_connection() {
        let ctx = tail_test_ctx();
        let setup_start = NOV_1_2023_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);

        const STEP_NS: i64 = 30 * DAY_NS; // the connection's elapsed lifetime, per step
        const WINDOW_NS: i64 = 60_000_000_000; // the live poll window itself (default slice)
        const STEPS: u32 = 36;
        let mut first_count = None;
        let mut last_count = 0;
        for step in 0..STEPS {
            // Past the clamp arm + GRACE from step 0 on, so every step
            // narrows on the `lower - GRACE` arm — a genuinely live poll.
            let lower = setup_start
                + i64::from(step) * STEP_NS
                + TAIL_REGISTRATION_GRACE_NS
                + 60_000_000_000;
            let upper = lower + WINDOW_NS;
            refresh_tail_months(&ctx, &mut setup, lower, upper, true).expect("no I/O, cannot fail");
            let expected =
                plan::months_overlapping(lower - TAIL_REGISTRATION_GRACE_NS, upper).len();
            let count = count_month_literals(&setup.plan.stage1_sql);
            assert_eq!(
                count, expected,
                "step {step}: stage1_sql's literal count must equal \
                 months_overlapping(lower - GRACE, upper), never the connection's elapsed \
                 month count"
            );
            assert!(
                count <= 2,
                "step {step}: bounded by the live poll window's width (60s window + GRACE \
                 stays within 2 calendar months), got {count}"
            );
            if step == 0 {
                first_count = Some(count);
            }
            last_count = count;
        }
        assert_eq!(
            Some(last_count),
            first_count,
            "no growth across {STEPS} elapsed months — the connection-lifetime blow-up is fixed"
        );
    }

    /// U5 (issue #94 v6-v8, codex test-gap 2): during catch-up
    /// (`narrow=false`), the rebuilt scan set is a PURE function of the
    /// `[scan_floor_ns, upper_ns]` window — 1000 refreshes at an identical
    /// window are byte-identical after the first (poll count/elapsed
    /// lifetime cannot grow it); advancing `upper` across exactly one
    /// month boundary adds exactly one literal.
    #[test]
    fn tail_refresh_months_u5_identical_window_refreshes_are_pure_no_accretion() {
        let ctx = tail_test_ctx();
        let setup_start = NOV_1_2023_NS;
        let setup_end = setup_start + DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, setup_start, setup_end);

        let lower = setup_start + 5 * DAY_NS;
        let upper = lower + DAY_NS;
        refresh_tail_months(&ctx, &mut setup, lower, upper, false).expect("no I/O, cannot fail");
        let sql_after_first = setup.plan.stage1_sql.clone();
        let covered_after_first = setup.covered_months;
        let floor_after_first = setup.scan_floor_ns;

        for call in 0..1_000u32 {
            refresh_tail_months(&ctx, &mut setup, lower, upper, false)
                .expect("no I/O, cannot fail");
            assert_eq!(
                setup.plan.stage1_sql, sql_after_first,
                "call {call}: byte-identical over 1000 same-window refreshes"
            );
            assert_eq!(setup.covered_months, covered_after_first);
            assert_eq!(
                setup.scan_floor_ns, floor_after_first,
                "call {call}: floor unmoved by identical-window catch-up refreshes"
            );
        }

        // Advance upper across exactly one month boundary: exactly one
        // literal added.
        let count_before = count_month_literals(&setup.plan.stage1_sql);
        let crossed_upper = setup_start + 35 * DAY_NS; // crosses into December
        refresh_tail_months(&ctx, &mut setup, lower, crossed_upper, false)
            .expect("no I/O, cannot fail");
        let count_after = count_month_literals(&setup.plan.stage1_sql);
        assert_eq!(
            count_after,
            count_before + 1,
            "exactly one literal added on the month crossing"
        );
        assert_eq!(
            count_after,
            plan::months_overlapping(setup.scan_floor_ns, crossed_upper).len()
        );
        assert_eq!(
            setup.scan_floor_ns, floor_after_first,
            "floor still unmoved (narrow=false throughout)"
        );
    }

    /// U8 (issue #94 v8, "clamp-qualified dichotomy"): the codex
    /// counterexample (a clamped start minutes after a month boundary),
    /// committed as the DOCUMENTED class-(i) residual (issue #134) — the
    /// prior month is never scanned by this connection, at setup, through
    /// catch-up, or after the scan gate narrows. The floor's clamp arm can
    /// only ever equal `s`, never fall below it — identical to the landed
    /// pre-#94-v6 code (`refresh_tail_months` fixed `start_ns` at the
    /// setup floor).
    #[test]
    fn tail_refresh_months_u8_boundary_start_never_scans_the_prior_month() {
        let ctx = tail_test_ctx();
        let s = DEC_1_2023_NS + 5 * 60_000_000_000; // 5 minutes past the boundary
        // Setup spans into January so BOTH refresh calls below actually
        // trigger a re-plan (`want != covered_months`) — a same-month-only
        // construction would make every call a no-op and never exercise
        // the clamp arm's `qp.spec.start_ns` assignment at all (the drill
        // must actually rebuild `stage1_sql` to be non-vacuous).
        let setup_end = s + 40 * DAY_NS;
        let mut setup = build_tail_setup(&ctx, r#"{app="x"}"#, s, setup_end);
        let dec_lit = month_literal(s);
        let nov_lit = month_literal(DEC_1_2023_NS - DAY_NS);
        assert_ne!(
            dec_lit, nov_lit,
            "construction sanity: s sits in a different month"
        );

        // (setup) at construction: December present, November absent.
        assert!(setup.plan.stage1_sql.contains(&dec_lit));
        assert!(!setup.plan.stage1_sql.contains(&nov_lit));

        // (catch-up, narrow=false), upper wholly within December (a
        // genuine re-plan: covered_months starts at (Dec,Jan)): the
        // clamp arm floor == s exactly, never below — November stays out
        // of the scan universe.
        refresh_tail_months(&ctx, &mut setup, s, s + 10 * DAY_NS, false)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.scan_floor_ns, s,
            "clamp arm floor == s exactly, never below"
        );
        assert!(setup.plan.stage1_sql.contains(&dec_lit));
        assert!(!setup.plan.stage1_sql.contains(&nov_lit));

        // (post-gate, narrow=true) with `lower` still within GRACE of `s`
        // but `upper` reaching into January (another genuine re-plan): the
        // clamp arm still wins on the LOWER side (`lower - GRACE < s`),
        // floor stays `s` — November must stay excluded even though the
        // window's upper edge has moved on.
        let narrow_lower = s + 30 * 60_000_000_000; // s + 30min, < s + GRACE
        let narrow_upper = narrow_lower + 35 * DAY_NS; // reaches January
        assert_ne!(
            month_literal(narrow_upper),
            dec_lit,
            "construction sanity: narrow_upper reaches a later month"
        );
        refresh_tail_months(&ctx, &mut setup, narrow_lower, narrow_upper, true)
            .expect("no I/O, cannot fail");
        assert_eq!(
            setup.scan_floor_ns, s,
            "still pinned at s: lower - GRACE has not advanced past s"
        );
        assert!(setup.plan.stage1_sql.contains(&dec_lit));
        assert!(
            !setup.plan.stage1_sql.contains(&nov_lit),
            "the prior month is never scanned by this connection, per the clamp-qualified \
             dichotomy (issue #134 class (i)): {}",
            setup.plan.stage1_sql
        );
    }

    /// Issue #94 AC4 first bullet — the orphan-cache mechanism: a
    /// fingerprint present in an EARLIER merge survives a LATER merge that
    /// no longer includes it (its registration month scrolled out of the
    /// narrowed stage-1 window, but the connection still remembers it).
    #[test]
    fn merge_resolved_preserves_a_fingerprint_absent_from_a_later_batch() {
        let mut cache: Vec<Fingerprint> = Vec::new();
        merge_resolved(
            &mut cache,
            &[
                Fingerprint::from_raw(5),
                Fingerprint::from_raw(1),
                Fingerprint::from_raw(3),
            ],
        );
        assert_eq!(
            cache,
            [1, 3, 5].map(Fingerprint::from_raw),
            "sorted + deduped after the first batch"
        );

        // The second (later, narrowed-window) batch no longer resolves
        // fingerprint 1, repeats 3, and adds a new fingerprint 7.
        merge_resolved(
            &mut cache,
            &[Fingerprint::from_raw(7), Fingerprint::from_raw(3)],
        );
        assert_eq!(
            cache,
            [1, 3, 5, 7].map(Fingerprint::from_raw),
            "fingerprint 1 (absent from the second batch) survives; 3 dedups; 7 is added"
        );

        merge_resolved(&mut cache, &[]);
        assert_eq!(
            cache,
            [1, 3, 5, 7].map(Fingerprint::from_raw),
            "an empty batch changes nothing"
        );
    }

    fn tail_row(ts: i64, fp: Fingerprint, hash: u64) -> TailSampleRow {
        TailSampleRow {
            fingerprint: fp,
            timestamp_ns: ts,
            body: format!("b{hash}"),
            body_hash: hash,
            structured_metadata: String::new(),
            rx: Vec::new(),
        }
    }

    /// Issue #74: an empty page never moves the boundary cursor (the
    /// scan watermark, owned by the caller, advances instead — round-4
    /// adjudication #2).
    #[test]
    fn advance_tail_cursor_keeps_the_previous_cursor_on_an_empty_page() {
        let prev = Some(TailCursor {
            tuple: (10, Fingerprint::from_raw(1), 5),
            seen: 2,
        });
        assert_eq!(advance_tail_cursor(prev, &[]), prev);
        assert_eq!(advance_tail_cursor(None, &[]), None);
    }

    /// Issue #74 (round-4 adjudication #1): `seen` counts exactly the
    /// trailing run of rows equal to the last row's tuple.
    #[test]
    fn advance_tail_cursor_counts_the_trailing_tie_run() {
        let rows = [
            tail_row(10, Fingerprint::from_raw(1), 1),
            tail_row(10, Fingerprint::from_raw(2), 7),
            tail_row(10, Fingerprint::from_raw(2), 7),
            tail_row(10, Fingerprint::from_raw(2), 7),
        ];
        let next = advance_tail_cursor(None, &rows).expect("non-empty page");
        assert_eq!(next.tuple, (10, Fingerprint::from_raw(2), 7));
        assert_eq!(next.seen, 3);
    }

    /// Issue #74: when a tie group is split across pages (`OFFSET` skipped
    /// the prior page's rows), the unchanged tuple carries `seen` forward;
    /// a changed tuple resets it.
    #[test]
    fn advance_tail_cursor_carries_seen_for_an_unchanged_tuple_and_resets_on_change() {
        let prev = Some(TailCursor {
            tuple: (10, Fingerprint::from_raw(2), 7),
            seen: 3,
        });
        // Page 2 of the same tie group: every row still equals the tuple.
        let same = [
            tail_row(10, Fingerprint::from_raw(2), 7),
            tail_row(10, Fingerprint::from_raw(2), 7),
        ];
        let next = advance_tail_cursor(prev, &same).expect("non-empty page");
        assert_eq!(next.tuple, (10, Fingerprint::from_raw(2), 7));
        assert_eq!(next.seen, 5, "3 already delivered + 2 new");

        // The cursor tuple changed: the count restarts at the new run.
        let moved = [
            tail_row(10, Fingerprint::from_raw(2), 7),
            tail_row(11, Fingerprint::from_raw(1), 4),
        ];
        let next = advance_tail_cursor(prev, &moved).expect("non-empty page");
        assert_eq!(next.tuple, (11, Fingerprint::from_raw(1), 4));
        assert_eq!(next.seen, 1);
    }

    /// Issue #74 v4 AC2 collision seam (review round 1): two DISTINCT
    /// bodies sharing one `body_hash` — a genuine CityHash collision is
    /// impractical to construct, so the equal-hash pair is injected at
    /// the comparator seam. The cursor treats them as one tuple run
    /// (the SQL side keeps them adjacent and stably ordered via the raw
    /// `body` tiebreaker), so the occurrence count paginates each
    /// exactly once: a `LIMIT`-split collision pair carries `seen`
    /// across pages instead of re-delivering or skipping the second
    /// body.
    #[test]
    fn advance_tail_cursor_paginates_a_hash_collision_pair_exactly_once() {
        // Page 1 fetched only the first colliding body (LIMIT split the
        // pair mid-run).
        let first = TailSampleRow {
            fingerprint: Fingerprint::from_raw(7),
            timestamp_ns: 10,
            body: "alpha".to_string(),
            body_hash: 42,
            structured_metadata: String::new(),
            rx: Vec::new(),
        };
        let second = TailSampleRow {
            fingerprint: Fingerprint::from_raw(7),
            timestamp_ns: 10,
            body: "beta".to_string(),
            body_hash: 42, // injected collision: distinct body, same hash
            structured_metadata: String::new(),
            rx: Vec::new(),
        };
        let c1 = advance_tail_cursor(None, std::slice::from_ref(&first)).expect("cursor");
        assert_eq!(c1.tuple, (10, Fingerprint::from_raw(7), 42));
        assert_eq!(
            c1.seen, 1,
            "one occurrence of the colliding tuple delivered"
        );

        // Page 2 (SQL: `>= tuple OFFSET 1`) fetches exactly the second
        // colliding body; the unchanged tuple carries the count forward.
        let c2 = advance_tail_cursor(Some(c1), std::slice::from_ref(&second)).expect("cursor");
        assert_eq!(c2.tuple, (10, Fingerprint::from_raw(7), 42));
        assert_eq!(
            c2.seen, 2,
            "both distinct bodies of the collision counted — the next OFFSET skips exactly both"
        );

        // Both bodies in ONE page: the trailing run spans the whole
        // equal-hash group regardless of the differing bodies.
        let both = [first, second];
        let c = advance_tail_cursor(None, &both).expect("cursor");
        assert_eq!(c.seen, 2);
    }

    /// Issue #74 AC1 (v1, still standing): a pipeline'd tail poll and the
    /// query path evaluate lines through the SAME `CompiledPipeline`
    /// compiled from the SAME `StreamsPlan` — identical per-line output
    /// on identical rows (tail is the query path, not a parallel engine).
    #[test]
    fn tail_pipeline_output_is_identical_to_the_query_paths_on_the_same_rows() {
        let expr = pulsus_logql::parse(r#"{app="x"} |= "keep" | logfmt | y="z""#).expect("parse");
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
            scan_budget_bytes: 1,
            max_streams: 100,
            pipeline_scan_factor: 10,
        };
        let qp = QueryParams {
            spec: QuerySpec::Range {
                start_ns: 0,
                end_ns: 1_000_000_000,
                step_ns: 1_000_000_000,
            },
            limit: 100,
            direction: Direction::Forward,
        };
        let Ok(Plan::Streams(sp)) = plan::plan(&expr, &qp, &ctx) else {
            panic!("stream selector must plan to Plan::Streams");
        };
        // The two compile sites (tail_setup and run_streams_inner) both
        // compile `sp.pipeline` — prove the outputs coincide per line.
        let tail_compiled = CompiledPipeline::compile(&sp.pipeline).expect("compile");
        let query_compiled = CompiledPipeline::compile(&sp.pipeline).expect("compile");

        let mut meta = HashMap::new();
        meta.insert(
            Fingerprint::from_raw(1),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(1),
                service: "checkout".to_string(),
                labels: r#"{"app":"x","service_name":"checkout"}"#.to_string(),
            },
        );
        // Rows model the post-SQL fetch: the `|= "keep"` prefix is pushed
        // down into stage-3/keyset SQL on BOTH paths (never re-evaluated
        // in-engine), so every synthetic row already contains it; the
        // in-engine `logfmt | y="z"` label filter is what drops row 11.
        let rows = || {
            vec![
                SampleRow {
                    fingerprint: Fingerprint::from_raw(1),
                    timestamp_ns: 10,
                    body: "keep y=z msg=a".to_string(),
                    structured_metadata: String::new(),
                    rx: Vec::new(),
                },
                SampleRow {
                    fingerprint: Fingerprint::from_raw(1),
                    timestamp_ns: 11,
                    body: "keep y=other".to_string(),
                    structured_metadata: String::new(),
                    rx: Vec::new(),
                },
            ]
        };
        let mut tail_out = run_pipeline_rows(rows(), &tail_compiled, &meta, 100)
            .expect("no template budget breach");
        let mut query_out = run_pipeline_rows(rows(), &query_compiled, &meta, 100)
            .expect("no template budget breach");
        tail_out.sort_by(|a, b| a.labels_json.cmp(&b.labels_json));
        query_out.sort_by(|a, b| a.labels_json.cmp(&b.labels_json));
        assert_eq!(tail_out, query_out);
        // And the pipeline genuinely evaluated: only the `y="z"` +
        // `|= "keep"` survivor remains.
        let entries: Vec<_> = tail_out.iter().flat_map(|s| s.entries.clone()).collect();
        assert_eq!(entries, vec![(10, "keep y=z msg=a".to_string())]);
    }
}
