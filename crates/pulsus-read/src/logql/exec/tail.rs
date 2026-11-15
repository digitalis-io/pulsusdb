use super::*;

/// The occurrence-count keyset cursor (issue #74 plan v4 D2 + the round-4
/// adjudication): `tuple` is the last fetched row's
/// `(timestamp_ns, fingerprint, cityHash64(body))`; `seen` counts how
/// many rows equal to `tuple` have already been delivered (the SQL
/// `OFFSET` of the next page), resetting to 0 whenever the tuple
/// changes. Split tie groups are re-fetched via the inclusive `>=`
/// predicate and skipped server-side by `OFFSET seen` — every row of a
/// tie group is delivered exactly once even when `LIMIT` splits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailCursor {
    /// `(timestamp_ns, fingerprint, cityHash64(body))`.
    pub tuple: (i64, Fingerprint, u64),
    /// Rows equal to `tuple` already delivered — the next page's `OFFSET`.
    pub seen: u32,
}

/// One tail poll's lower bound (issue #74 plan v4 D1): the two predicate
/// modes are distinct and never conflated — the first page carries the
/// API `start` bound (`timestamp_ns > start_ns`, the repo stage-3
/// convention); every later page carries the keyset term instead (the
/// cursor dominates `start`).
#[derive(Debug, Clone, Copy)]
pub enum TailLower {
    Start { start_ns: i64 },
    After(TailCursor),
}

/// The positions a catch-up may deliver: exactly those the main scan has passed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailCeiling {
    /// Main scan at `Start { start_ns: t }`: rows with `timestamp_ns <= t`.
    AtOrBelow(i64),
    /// Main scan at `After(c)`: rows with `(timestamp_ns, fingerprint) < (c.tuple.0, c.tuple.1)`.
    Before(i64, Fingerprint),
}

impl TailCeiling {
    pub fn of(lower: TailLower) -> Self {
        match lower {
            TailLower::Start { start_ns } => Self::AtOrBelow(start_ns),
            TailLower::After(c) => Self::Before(c.tuple.0, c.tuple.1),
        }
    }

    pub fn ts(self) -> i64 {
        match self {
            Self::AtOrBelow(t) | Self::Before(t, _) => t,
        }
    }

    pub fn admits(self, ts: i64, fp: Fingerprint) -> bool {
        match self {
            Self::AtOrBelow(t) => ts <= t,
            Self::Before(t, f) => (ts, fp) < (t, f),
        }
    }
}

/// One catch-up page, and the distinct fingerprints of every row it fetched
/// (before the ceiling or the pipeline dropped any), sorted.
#[derive(Debug)]
pub struct CatchUpPage {
    pub page: TailPage,
    pub fetched_fps: Vec<Fingerprint>,
}

/// One tail poll's result. `next` is the advanced boundary cursor
/// (unchanged when the page fetched no rows); `fetched` is the RAW
/// fetched-row count — `fetched == fetch_limit` means the slice may hold
/// more rows and the caller must re-poll from `next` before advancing
/// its scan watermark (see `logs_api/tail.rs`'s producer loop).
#[derive(Debug)]
pub struct TailPage {
    pub streams: Vec<StreamResult>,
    pub next: Option<TailCursor>,
    pub fetched: u32,
    /// The streams whose labels this poll read for the first time and
    /// which are not in the stage-3 list, sorted (issue #804). Empty on
    /// the first window, where those streams go straight into the list.
    pub joined: Vec<Fingerprint>,
}

/// Registration-visibility grace at the live edge (issue #94 v6-v8): the
/// hold/scan-gate keeps `TailSetup::scan_floor_ns` frozen (full-span
/// stage-1) until the producer certifies a COMPLETED full-span poll whose
/// start dwell at the live edge was >= this grace, and thereafter bounds
/// how far the narrowed floor trails the live cursor
/// (`lower_ns - TAIL_REGISTRATION_GRACE_NS`). POLICY, not a derived
/// ceiling: covers the default `batch_ms` flush (200 ms), a ~1.5s pre-send
/// retry tail, and generous headroom for distributed-send backlogs;
/// visibility later than GRACE is issue #134's ingest-durability scope,
/// not a read-path constant to inflate. Documented constant (same precedent as
/// the writer's retry constants) — deliberately not an env/config/request
/// knob (a knob would invite masking #134-class failures with unbounded
/// read-side rescans; reconnect is the existing, no-knob remedy).
/// Once stage 1 is narrowed, a full-span stage 1 also runs once per GRACE of
/// cursor time, so a registration readable later than GRACE is still found
/// (issue #804).
pub const TAIL_REGISTRATION_GRACE_NS: i64 = 3_600_000_000_000;

/// How long, on a monotonic clock from the return of its joining poll, a
/// stream whose labels are readable may be read for lines the tail has
/// passed before it is admitted without the rest (issue #804). A documented
/// constant, not a knob, for the same reason as `TAIL_REGISTRATION_GRACE_NS`.
pub const TAIL_CATCH_UP_RECHECK_NS: i64 = 60_000_000_000;

/// The per-connection tail setup (issue #74): the streams plan and the
/// compiled pipeline, built ONCE before the WebSocket upgrade (a bad
/// regex/template is a 400 rejection, never an upgraded-then-closed
/// socket). Every poll reuses both — tail runs the identical stage-1/2/3
/// plan machinery and the SAME `CompiledPipeline` as `query()`
/// (semantics-drift-free; the task-manager-ratified invariant), only the
/// fetch ordering/cursor differ.
///
/// Beyond the (public) `plan`/`compiled`, the setup carries crate-private
/// state for the bounded, atomicity-safe, scan-gated month refresh (issue
/// #94 v2/v3 + v6-v8's phase split): the original `expr` and
/// `base_params` are replanned — with **no DB I/O** — whenever the scan
/// window `[scan_floor_ns, upper_ns]` covers a different (lo_month,
/// hi_month) pair than `covered_months`.
///
/// `scan_floor_ns` is monotone and starts at the connection's
/// (retention-)clamped setup start `s`. During catch-up/fall-behind (the
/// producer's `narrow == false`) it stays FROZEN, so stage-1 scans the
/// FULL span `[scan_floor_ns, upper_ns]` — identical to the pre-#94-v6
/// behaviour, request-bounded, never lifetime-growing. Only once the
/// producer certifies a COMPLETED full-span live-edge poll whose start
/// dwell was `>= TAIL_REGISTRATION_GRACE_NS` (`narrow == true`) does the
/// floor advance, to `max(scan_floor_ns, lower_ns - GRACE)` — bounding the
/// per-poll month scan to the live poll window's width instead of the
/// connection's lifetime.
///
/// **Clamp-qualified dichotomy (issue #94 v8; the connection's scan
/// universe `U = { M : M >= month(s) }`, identical to the landed
/// pre-#94-v6 code):**
/// - `M ∉ U` (a month strictly before the clamped start): never scanned
///   by ANY scan of this connection, full-span included — pre-existing
///   (issue #134 residual class (i)); the read path cannot recover it
///   (reconnect with an earlier start, retention-permitting, or an
///   ingest-side atomicity/backfill remedy).
/// - `M ∈ U`: every registration into `M` visible by
///   `end(M) + delay + GRACE` is caught by some full-span or in-band scan
///   and cached permanently; a registration visible later is found by the
///   full-span stage 1 that a narrowed connection runs once per GRACE of
///   cursor time, and its stream is then caught up (issue #804). The floor's clamp
///   arm (`scan_floor_ns` never drops below `s`) never excludes a
///   universe month — only the `lower_ns - GRACE` arm can, and only once
///   it has advanced past `s`.
///
/// Because narrowing the lower edge can prune a month whose
/// `log_streams`/`log_streams_idx` registration is the ONLY record of a
/// fingerprint whose sample now falls in a later, in-window month (writes
/// to `log_samples` and the stream tables are non-atomic —
/// `crates/pulsus-write/src/writer/mod.rs`), `resolved` is a cumulative,
/// deduped cache of every fingerprint stage-1 has ever resolved on this
/// connection: stage-2/3 read the cached union, not just the current
/// poll's narrow result, so a fingerprint resolved once (in its
/// registration month) stays resolvable for the rest of the connection.
/// The cache is capped by the same `max_streams` ceiling stage-1 already
/// obeys (reject, not silently truncate). These fields are only ever
/// constructed by [`LogQlEngine::tail_setup`], so `TailSetup`'s public
/// re-export surface is unchanged.
///
/// Stage 3 reads only the streams admitted to it (issue #804): `hydrated`
/// holds the labels of every stream kept for this connection, never removed
/// and never re-read; `deliverable` is the sorted list of admitted streams,
/// stage 3's `IN` list; `last_full_scan_lower_ns` is the lower bound of the
/// last committed poll whose stage 1 covered every month from `start`.
/// [`TailSetup::admit`] is the producer's, after the catch-up read that
/// justifies it.
#[derive(Debug)]
pub struct TailSetup {
    pub plan: StreamsPlan,
    pub compiled: CompiledPipeline,
    /// The original tail expression, replanned on a covered-window change.
    pub(super) expr: Expr,
    /// The setup `QueryParams` (`Copy`); both `start_ns` and `end_ns` are
    /// overridden to the scan window on refresh.
    pub(super) base_params: QueryParams,
    /// The monotone scan-set lower anchor (issue #94 v6-v8): frozen
    /// during catch-up/fall-behind, advances to `max(self, lower_ns -
    /// GRACE)` only on a scan-gated live-edge refresh. Starts at the
    /// connection's clamped setup start.
    pub(super) scan_floor_ns: i64,
    /// `(year_month(scan_floor_ns), year_month(upper))` the current
    /// `plan` covers.
    pub(super) covered_months: ((i64, u32), (i64, u32)),
    /// Cumulative, sorted+deduped union of every fingerprint stage-1 has
    /// resolved on this connection — the orphan-cache that keeps a
    /// partial-failure (older-month-registered) stream resolvable after
    /// the stage-1 month window narrows past its registration month.
    pub(super) resolved: Vec<Fingerprint>,
    /// The response-shape options the connection was opened with (issue
    /// #463) — the tail's whole lifetime, since the header is read once
    /// at upgrade and a WebSocket has no per-frame request.
    pub(super) opts: ResponseOptions,
    /// Labels of every stream kept for this connection (issue #804).
    pub(super) hydrated: HashMap<Fingerprint, StreamMetaRow>,
    /// Admitted streams, sorted: stage 3's `IN` list (issue #804).
    pub(super) deliverable: Vec<Fingerprint>,
    /// Lower bound of the last committed poll whose stage 1 covered every
    /// month from `start` (issue #804).
    pub(super) last_full_scan_lower_ns: i64,
}

impl TailSetup {
    /// The producer admits these streams to stage 3 (their catch-up, if any, is done).
    pub fn admit(&mut self, fps: &[Fingerprint]) {
        merge_resolved(&mut self.deliverable, fps);
    }
}

impl LogQlEngine {
    /// Builds a tail connection's [`TailSetup`] — plan + compiled
    /// pipeline — once, BEFORE the WebSocket upgrade. A metric
    /// expression or an uncompilable pipeline is a 400-class rejection
    /// here, never a wasted upgrade.
    pub fn tail_setup(&self, expr: &Expr, params: &QueryParams) -> Result<TailSetup, ReadError> {
        self.tail_setup_with(expr, params, ResponseOptions::default())
    }

    /// [`Self::tail_setup`] with the response-shape options the upgrade
    /// request asked for (issue #463).
    pub fn tail_setup_with(
        &self,
        expr: &Expr,
        params: &QueryParams,
        opts: ResponseOptions,
    ) -> Result<TailSetup, ReadError> {
        let ctx = self.config.plan_ctx(&self.tenant);
        match plan::plan(expr, params, &ctx)? {
            Plan::Streams(sp) => {
                let compiled = CompiledPipeline::compile(&sp.pipeline)?;
                Ok(TailSetup {
                    plan: sp,
                    compiled,
                    expr: expr.clone(),
                    base_params: *params,
                    scan_floor_ns: spec_start_ns(&params.spec),
                    covered_months: (
                        plan::year_month(spec_start_ns(&params.spec)),
                        plan::year_month(spec_end_ns(&params.spec)),
                    ),
                    resolved: Vec::new(),
                    opts,
                    hydrated: HashMap::new(),
                    deliverable: Vec::new(),
                    last_full_scan_lower_ns: spec_start_ns(&params.spec),
                })
            }
            Plan::Metric(_) | Plan::MetricBinary(_) => Err(tail_needs_a_log_query()),
        }
    }

    /// Best-effort stage-1 month refresh (issue #94 v6-v8, scan-gated
    /// phase split): the stage-1 month IN-list is anchored to
    /// `[setup.scan_floor_ns, upper_ns]`, re-planning (reusing
    /// [`plan::plan`] — **no ClickHouse I/O**, a pure `stage1_sql` string
    /// rebuild) whenever that window's covered `(lo_month, hi_month)`
    /// differs from what `setup.plan` already covers.
    ///
    /// `narrow` is the producer's certification (see
    /// `logs_api/tail.rs::producer_loop`'s scan-gate rule, computed ONCE
    /// per iteration from a single clock read — recomputing it downstream
    /// from a fresh clock is a documented trap: it would misclassify
    /// steady-state live polls as catch-up and silently reintroduce
    /// lifetime-unbounded growth) that a COMPLETED full-span poll at the
    /// live edge has already dwelt >= [`TAIL_REGISTRATION_GRACE_NS`]. Only
    /// then does `scan_floor_ns` advance (monotonically, to
    /// `max(scan_floor_ns, lower_ns - GRACE)`); otherwise (catch-up,
    /// fall-behind, or still inside the hold) the floor stays frozen and
    /// the scan set widens upper-only — full-span, request-bounded, never
    /// lifetime-growing. See [`TailSetup`]'s doc for the full
    /// clamp-qualified coverage argument and residual (issue #134).
    ///
    /// The caller invokes this best-effort (`let _ = …`): on a re-plan
    /// error `setup` is left untouched and the tail keeps running on the
    /// PRIOR month set — it degrades to pre-#94 behaviour (new-month
    /// streams surface on the next successful refresh or a reconnect) and
    /// never errors the connection.
    pub fn tail_refresh_months(
        &self,
        setup: &mut TailSetup,
        lower_ns: i64,
        upper_ns: i64,
        narrow: bool,
    ) -> Result<(), ReadError> {
        let ctx = self.config.plan_ctx(&self.tenant);
        refresh_tail_months(&ctx, setup, lower_ns, upper_ns, narrow)
    }

    /// One live-tail poll (issue #74; issue #94 resolve-and-remember
    /// revision): re-resolves stage-1 over the (now month-narrowed)
    /// `setup.plan`, MERGES the result into `setup.resolved` (the
    /// cumulative cache — see [`TailSetup`]'s doc for why this is
    /// data-loss-free despite the narrowed month window), hydrates the
    /// streams whose labels are not yet held, and fetches one keyset page
    /// over the admitted streams, running the SAME `CompiledPipeline` the
    /// query path runs. The cursor advances past every *fetched* row
    /// (pipeline-dropped lines never re-fetch). Stage 3 reads admitted
    /// streams; everything the poll learns is kept only when it succeeds
    /// (`commit_tail_attempt`).
    pub async fn tail_poll(
        &self,
        setup: &mut TailSetup,
        lower: TailLower,
        upper_ns: i64,
        fetch_limit: u32,
    ) -> Result<TailPage, ReadError> {
        let attempt = self
            .tail_attempt(&*setup, lower, upper_ns, fetch_limit)
            .await;
        commit_tail_attempt(setup, attempt, self.server_failure())
    }

    /// One poll's reads, against committed state only (issue #804): the
    /// result is kept by [`commit_tail_attempt`], so a poll that fails
    /// part-way keeps nothing it learned.
    pub async fn tail_attempt(
        &self,
        setup: &TailSetup,
        lower: TailLower,
        upper_ns: i64,
        fetch_limit: u32,
    ) -> Result<TailAttempt, ReadError> {
        let prev = match lower {
            TailLower::After(c) => Some(c),
            TailLower::Start { .. } => None,
        };
        let lower_ns = match lower {
            TailLower::Start { start_ns } => start_ns,
            TailLower::After(c) => c.tuple.0,
        };
        let start = spec_start_ns(&setup.base_params.spec);
        let first_window = matches!(lower, TailLower::Start { start_ns } if start_ns == start);
        let plan_full = setup.covered_months.0 == plan::year_month(start);
        let sweep = !plan_full
            && lower_ns.saturating_sub(setup.last_full_scan_lower_ns) >= TAIL_REGISTRATION_GRACE_NS;
        let mut found = self.resolve_fingerprints(&setup.plan.stage1_sql).await?;
        if sweep {
            let sql = self.full_span_stage1(setup, start, upper_ns.max(lower_ns))?;
            found.extend(self.resolve_fingerprints(&sql).await?);
            found.sort_unstable();
            found.dedup();
        }
        let added: Vec<Fingerprint> = found
            .into_iter()
            .filter(|fp| setup.resolved.binary_search(fp).is_err())
            .collect();
        check_stream_cap(setup.resolved.len() + added.len(), self.config.max_streams)?;
        let mut pending: Vec<Fingerprint> = setup
            .resolved
            .iter()
            .chain(&added)
            .filter(|fp| !setup.hydrated.contains_key(fp))
            .copied()
            .collect();
        pending.sort_unstable();
        let labels = if pending.is_empty() {
            HashMap::new()
        } else {
            self.hydrate(&setup.plan.streams_table, &pending).await?
        };
        let mut joined: Vec<Fingerprint> = labels.keys().copied().collect();
        joined.sort_unstable();
        // First window: nothing has been passed, so the new streams need no
        // catch-up and are read here. `setup.hydrated` is empty then (no
        // poll has committed), so `labels` is the map.
        let (meta, fps): (&HashMap<Fingerprint, StreamMetaRow>, &[Fingerprint]) =
            if first_window && !joined.is_empty() {
                debug_assert!(setup.deliverable.is_empty());
                (&labels, &joined)
            } else {
                (&setup.hydrated, &setup.deliverable)
            };
        let page = if fps.is_empty() {
            TailPage {
                streams: Vec::new(),
                next: prev,
                fetched: 0,
                joined: Vec::new(),
            }
        } else {
            self.tail_fetch(setup, meta, fps, lower, upper_ns, fetch_limit, None)
                .await?
                .page
        };
        Ok(TailAttempt {
            page,
            added,
            labels,
            joined,
            first_window,
            full_scan: plan_full || sweep,
            lower_ns,
        })
    }

    /// Stage 1 over every month from `start` to `hi`, built by
    /// [`plan::plan`] as `refresh_tail_months` builds it — no I/O.
    fn full_span_stage1(
        &self,
        setup: &TailSetup,
        start: i64,
        hi: i64,
    ) -> Result<String, ReadError> {
        let mut qp = setup.base_params;
        if let QuerySpec::Range {
            start_ns, end_ns, ..
        } = &mut qp.spec
        {
            *start_ns = start;
            *end_ns = hi;
        }
        match plan::plan(&setup.expr, &qp, &self.config.plan_ctx(&self.tenant))? {
            Plan::Streams(sp) => Ok(sp.stage1_sql),
            Plan::Metric(_) | Plan::MetricBinary(_) => Err(tail_needs_a_log_query()),
        }
    }

    /// S5 (issue #804): one catch-up step over `fps`, `lower`..`upper_ns`,
    /// rows outside `ceiling` dropped.
    pub async fn tail_catch_up(
        &self,
        setup: &TailSetup,
        fps: &[Fingerprint],
        lower: TailLower,
        upper_ns: i64,
        ceiling: TailCeiling,
        fetch_limit: u32,
    ) -> Result<CatchUpPage, ReadError> {
        self.tail_fetch(
            setup,
            &setup.hydrated,
            fps,
            lower,
            upper_ns,
            fetch_limit,
            Some(ceiling),
        )
        .await
    }

    /// S4 (issue #804): each of `fps` that has a line in
    /// `(after_ns, ceiling.ts()]`, with its earliest one, sorted.
    pub async fn tail_first_lines(
        &self,
        setup: &TailSetup,
        fps: &[Fingerprint],
        after_ns: i64,
        ceiling: TailCeiling,
    ) -> Result<Vec<(Fingerprint, i64)>, ReadError> {
        let sql = super::sql::tail_first_lines(
            &self.tenant,
            &setup.plan.samples_table,
            &distinct_escaped_services(&setup.hydrated),
            &sql_literals(fps),
            super::sql::TimeWindow {
                start_ns: after_ns,
                end_ns: ceiling.ts(),
            },
        );
        let mut out = Vec::new();
        let mut stream = self
            .query_stream::<FirstLineRow>(&sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })?;
            out.push((row.fingerprint, row.first_ns));
        }
        out.sort_unstable();
        Ok(out)
    }

    /// One keyset page over `fps` with the labels in `meta` (issue #74;
    /// issue #804 made the list and the map arguments). `ceiling`, when
    /// set, drops every row the main scan has not passed.
    #[allow(clippy::too_many_arguments)]
    async fn tail_fetch(
        &self,
        setup: &TailSetup,
        meta: &HashMap<Fingerprint, StreamMetaRow>,
        fingerprints: &[Fingerprint],
        lower: TailLower,
        upper_ns: i64,
        fetch_limit: u32,
        ceiling: Option<TailCeiling>,
    ) -> Result<CatchUpPage, ReadError> {
        let prev = match lower {
            TailLower::After(c) => Some(c),
            TailLower::Start { .. } => None,
        };
        let services = distinct_escaped_services(meta);

        // Tail is forward-only (oldest→newest); `KeysetLower::First`
        // carries the API `start` bound in the window, later pages carry
        // the keyset (window `start_ns` is then unused by the Forward
        // After rendering).
        let start_ns = match lower {
            TailLower::Start { start_ns } => start_ns,
            TailLower::After(_) => 0,
        };
        let window = super::sql::TimeWindow {
            start_ns,
            end_ns: upper_ns,
        };
        let ks_lower = match lower {
            TailLower::Start { .. } => super::sql::KeysetLower::First,
            TailLower::After(c) => super::sql::KeysetLower::After {
                // The cursor's fingerprint is minted here: it enters a
                // tuple comparison, where a bare decimal above `2^64`
                // re-delivers the boundary row walking forward and skips
                // it walking back (issue #498).
                tuple: (c.tuple.0, c.tuple.1.sql_literal(), c.tuple.2),
                offset: c.seen,
            },
        };
        // Issue #624, part 3a (D6): the database runs the `regexp` stages
        // over the stored line.
        let rx = rx_column(&setup.compiled)?;
        let sql = super::sql::stage3_keyset_with_rx(
            &self.tenant,
            &setup.plan.samples_table,
            &services,
            &sql_literals(fingerprints),
            window,
            ks_lower,
            Direction::Forward,
            &setup.plan.line_filters,
            fetch_limit,
            rx.as_ref(),
        );

        // Streamed, not staged (issue #312): a poll used to build a
        // page-sized `Vec<TailSampleRow>` and re-materialise it into a
        // second `Vec<SampleRow>`, both bounded only by
        // `tail_max_fetch_limit` rows x the 64 MiB ingest cap. Rows are
        // now charged and chunked as they arrive, and the cursor walks
        // the raw page incrementally through the same
        // [`TailCursorTracker`] the paged loops use.
        let mut acc = StreamAccumulator::with_opts(meta, fetch_limit, setup.opts);
        let mut tracker = TailCursorTracker::new();
        let mut fetched_fps: BTreeSet<Fingerprint> = BTreeSet::new();
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule).
            let mut stream = self
                .query_rows::<TailSampleRow, TailSampleRxRow>(
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
                tracker.observe(row.timestamp_ns, row.fingerprint, row.body_hash);
                fetched_fps.insert(row.fingerprint);
                if ceiling.is_some_and(|c| !c.admits(row.timestamp_ns, row.fingerprint)) {
                    continue; // past the main scan's lower bound: the main scan delivers it
                }
                acc.push_row(
                    SampleRow {
                        fingerprint: row.fingerprint,
                        timestamp_ns: row.timestamp_ns,
                        body: row.body,
                        structured_metadata: row.structured_metadata,
                        rx: row.rx,
                    },
                    &setup.compiled,
                )?;
            }
        }
        acc.flush_chunk(&setup.compiled)?;
        let (next, fetched) = tracker.finish(prev);
        let streams = acc.into_streams();
        Ok(CatchUpPage {
            page: TailPage {
                streams,
                next,
                fetched,
                joined: Vec::new(),
            },
            fetched_fps: fetched_fps.into_iter().collect(),
        })
    }
}
