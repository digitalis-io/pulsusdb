use super::*;

impl LogQlEngine {
    /// Returns `(streams, partial)`: `partial` is set only on the
    /// fetch-until-limit dropping path when the byte scan budget is
    /// exhausted mid-paging (issue #90's signaled partial — surfaced as
    /// `stats.pulsus_partial`); the fast/non-dropping paths and genuine
    /// exhaustion always return `partial == false`.
    pub(super) async fn run_streams_inner(
        &self,
        sp: &StreamsPlan,
        mut explain: Option<&mut PlanExplain>,
        opts: ResponseOptions,
    ) -> Result<(Vec<StreamResult>, bool), ReadError> {
        // Compile before any I/O: a bad regex/template is a 400-class
        // rejection, never a wasted scan.
        let compiled = super::pipeline::CompiledPipeline::compile(&sp.pipeline)?;

        if let Some(e) = explain.as_mut() {
            e.push("stage1_stream_resolution", sp.stage1_sql.clone(), None);
            for probe in &sp.probes {
                e.push(
                    "selectivity_probe",
                    probe.sql.clone(),
                    Some(format!("key = {}", probe.key)),
                );
            }
        }
        let fingerprints = self.resolve_fingerprints(&sp.stage1_sql).await?;
        if fingerprints.is_empty() {
            return Ok((Vec::new(), false));
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

        // Issue #544: the metadata fragments are rendered here, where the
        // stream label sets exist, and the paging decision is re-taken on
        // the result. `sp.fetch_until_limit` is the PLAN's answer, which
        // assumed the fragments would fit; `fetch_until_limit` below is
        // the read's.
        let lowered = stage3_predicates(sp, &fingerprints, &meta, self.metadata_budget());
        let fetch_until_limit = sp.fetch_until_limit || !lowered.metadata_lowered;
        let scan_limit = self.effective_scan_limit(sp, fetch_until_limit);
        // Issue #624, part 3a (D6): the database runs the `regexp` stages
        // over the stored line.
        let rx = rx_column(&compiled)?;
        let sql = super::sql::stage3_with_rx(
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
            scan_limit,
            rx.as_ref(),
        );
        if let Some(e) = explain.as_mut() {
            e.push("stage3_samples", sql.clone(), None);
        }

        if compiled.is_line_filter_only() {
            // Fast path: today's per-fingerprint shape, `labels_json`
            // verbatim (`scan_limit == result_limit` by construction).
            // Zero-structured-metadata rows stay on the by-fingerprint
            // path (AC-8 byte-identity); rows carrying structured
            // metadata (issue #97) fan out into their own
            // merged-label-set streams.
            //
            // **Streamed, not staged (issue #312).** There is no
            // `sm_rows` vector: an SM row fans out as it arrives, so its
            // body is live once — charged — rather than twice. A row
            // whose fingerprint is absent from `meta` is skipped here
            // rather than at drain, which is byte-identical (the
            // pre-#312 `filter_map` dropped exactly those) and is what
            // makes the charge equal the returned footprint exactly.
            let mut budget = StreamsResultBudget::new();
            let mut groups = FastPathGroups::new_with(opts.categorize_labels);
            let mut stream = self
                .query_stream::<SampleRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                groups.push_row(row, &meta, &mut budget)?;
            }
            return Ok((groups.into_streams(), false));
        }

        // Dropping sub-case (issue #90): a label filter, or a line filter
        // after `line_format`, drops lines in-engine — a single oversampled
        // `LIMIT` scan could under-return. Keyset-page until the limit
        // fills, the window exhausts, or the budget is spent.
        if fetch_until_limit {
            return self
                .run_streams_paged(
                    sp,
                    &compiled,
                    &meta,
                    &services,
                    &fingerprints,
                    scan_limit,
                    opts,
                    rx.as_ref(),
                )
                .await;
        }

        // Non-dropping transform/fan-out path: rows are consumed in
        // arrival order (stage 3 orders globally by timestamp in the
        // requested direction, so arrival order IS the response order —
        // the global `result_limit` truncation depends on it). A single
        // `stage3` `LIMIT = result_limit` scan, byte-identical to today.
        //
        // **Chunked, not staged (issue #312).** The whole-response
        // `Vec<SampleRow>` this used to build was bounded only by the SQL
        // LIMIT — 5 000 rows x the 64 MiB ingest cap = 320 GiB. Rows now
        // flow through the accumulator's byte-denominated chunk, charged
        // on push and released on flush.
        let mut acc = StreamAccumulator::with_opts(&meta, sp.result_limit, opts);
        {
            // Scoped: the row stream holds its pooled connection until
            // dropped (the `ChRowStream` lease rule).
            let mut stream = self
                .query_rows::<SampleRow, SampleRxRow>(&sql, &self.budget_settings(), rx.is_some())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                acc.push_row(row, &compiled)?;
            }
        }
        acc.flush_chunk(&compiled)?;

        Ok((acc.into_streams(), false))
    }

    /// The fetch-until-limit paging loop (issue #90 — the dropping
    /// sub-case). Keyset-pages PK-pruned pages **in the plan's direction**
    /// through one shared [`StreamAccumulator`] until `result_limit`
    /// survivors are collected, the window is exhausted, or the byte scan
    /// budget is spent, returning `(streams, partial)`.
    ///
    /// **Approximate best-effort scan guard — NOT a hard byte ceiling.**
    /// Each page is issued with a decrementing `max_bytes_to_read = budget −
    /// (bytes already scanned by prior pages)` and `read_overflow_mode =
    /// throw`. If the **first** page alone exceeds the budget the query
    /// fails with `QueryTooBroad` (a genuinely too-broad query), exactly as
    /// the pre-paging single-scan path did. Once at least one page has
    /// returned, the loop is best-effort: before issuing each further page
    /// the top-of-loop guard returns the survivors so far with
    /// `pulsus_partial = true` if the budget is already spent (it never
    /// issues a zero cap, which ClickHouse would treat as *unlimited*), and
    /// likewise a later page whose scan trips its positive cap returns the
    /// partial survivors. The loop always terminates and never scans
    /// `pages × window` unbounded. Because ClickHouse enforces the cap per
    /// read block per concurrent reader (per thread, and per shard on a
    /// cluster), the actual bytes scanned can exceed the budget by an
    /// amount that grows with query parallelism and shard count — the
    /// budget bounds runaway paging, not exact bytes. `wait_end_of_query =
    /// 1` (see [`LogQlEngine::paging_settings`]) makes each page's
    /// `read_bytes` the FINAL scanned total (the clickhouse 0.15.1 crate
    /// otherwise captures an understated header-time value), so `spent`
    /// tracks scan progress soundly; an unknown (`None`) read_bytes charges
    /// the full cap (conservative).
    ///
    /// **Termination.** The cursor advances past every *fetched* row
    /// ([`TailCursorTracker`] over the raw page, not survivors — so a page
    /// entirely filtered out by the pipeline never stalls the loop), with
    /// occurrence-count `OFFSET` handling tie-runs larger than a page
    /// (carried from #74). Over a finite window the cursor advances
    /// monotonically, so the loop must eventually fetch `< page_size`
    /// (window exhausted) or spend the budget.
    ///
    /// **Terminal branches.** limit filled / window exhausted → `partial =
    /// false`; budget spent before issuing a later page → `partial = true`
    /// (top-of-loop guard); first-page budget overflow (`spent == 0`) →
    /// propagate `QueryTooBroad` (a genuinely too-broad query, preserving
    /// today's error); a later page (`spent > 0`) tripping its positive cap
    /// → signaled partial.
    ///
    /// **Streaming (issue #312), and refusal versus partial.** Each page's
    /// rows are charged and chunked through the accumulator as they
    /// arrive — no page-sized `Vec<TailSampleRow>` and no `SampleRow`
    /// re-materialisation into a second page-sized vector; the two used to
    /// hold 50 000 rows x the 64 MiB ingest cap = 3.1 TiB. A
    /// `ScanBudgetBytes` breach mid-page therefore returns the survivors
    /// INCLUDING that page's prefix, which is #244's ruled precedent (the
    /// prefix boundary is not required to align with a page boundary) and
    /// is more complete, never less. The scan-budget partial is decided at
    /// the top of the loop and inside `absorb_page`; the RESULT-budget
    /// refusal is decided inside `push_row`. A query near both boundaries
    /// resolves to whichever trips first, which is page-timing dependent —
    /// but complete-or-error wins where the ledger trips: a
    /// [`TooBroadReason::StreamsResultBytes`] refusal is never downgraded
    /// to a partial.
    #[allow(clippy::too_many_arguments)]
    async fn run_streams_paged(
        &self,
        sp: &StreamsPlan,
        compiled: &super::pipeline::CompiledPipeline,
        meta: &HashMap<Fingerprint, StreamMetaRow>,
        services: &[CheckedLiteral],
        fingerprints: &[Fingerprint],
        scan_limit: u32,
        opts: ResponseOptions,
        rx: Option<&CheckedFragment>,
    ) -> Result<(Vec<StreamResult>, bool), ReadError> {
        let budget = self.config.scan_budget_bytes;
        let window = super::sql::TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        };
        let mut acc = StreamAccumulator::with_opts(meta, sp.result_limit, opts);
        let mut st = StreamsPagedState {
            cursor: None,
            spent: 0,
            // First-page size = the oversample hint; subsequent pages
            // reuse it.
            page_size: scan_limit.max(1),
            budget,
        };
        // Issue #544: rendered once for the whole loop, not per page — the
        // fingerprints and the label sets do not move between pages.
        let lowered = stage3_predicates(sp, fingerprints, meta, self.metadata_budget());

        loop {
            // Terminate before issuing: `max_bytes_to_read = 0` is
            // ClickHouse's *unlimited* sentinel, so a zero cap must never be
            // issued. Once the budget is spent, return the survivors so far
            // as a partial result (a later page's positive-cap overflow is
            // handled below; the first-page `spent == 0` case never reaches
            // here). This makes `page_cap` always > 0.
            if scan_budget_spent(st.spent, budget) {
                return Ok((acc.into_streams(), true));
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
                &lowered.predicates,
                st.page_size,
                rx,
            );

            // Open, then stream-drain one page; `read_bytes` is meaningful
            // only after the drain (wait_end_of_query=1). Scoped so the
            // stream's pooled-connection lease releases before the next
            // page.
            //
            // Issue #35: `query_stream` returns `Result<_, ReadError>`
            // directly (already mapped through `map_read_error` for a
            // dispatch-time failure); per-row errors are raw `ChError`
            // from `ChRowStream::next()` and are mapped inside
            // `absorb_page` with the SAME `map_read_error(_, budget)` —
            // so both routes reach `classify_page_error` as an
            // already-mapped `ReadError`, preserving the
            // first-page-vs-later-page branch split unchanged.
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
                    Err(mapped) => {
                        let partial = classify_page_error(mapped, st.spent)?;
                        return Ok((acc.into_streams(), partial));
                    }
                };
                st.absorb_page(
                    &mut stream,
                    |s| s.read_bytes(),
                    |e| map_read_error(e, budget, self.config.read_max_memory_bytes),
                    &mut acc,
                    compiled,
                )
                .await
            };
            if let Some(terminal) = decision? {
                return Ok((acc.into_streams(), terminal));
            }
        }
    }
}

/// The pure transform/fan-out assembly (issue M6-09): runs already-
/// fetched stage-3 rows — **in arrival order**, which stage 3's global
/// `ORDER BY timestamp_ns` makes the requested direction's order —
/// through the compiled pipeline, truncates survivors at `result_limit`
/// **globally across streams** (AC9: never per-stream, and never
/// over-returning), then groups:
/// - transform path (`!mutates_labels`): by source fingerprint,
///   `labels_json` verbatim from hydration;
/// - fan-out path: by final label set, with a canonical re-rendered
///   `labels_json` and a deterministic content-hash fingerprint.
///
/// `pub` (not `pub(crate)`) deliberately: this is the ChClient-free pure
/// half of the streams pipeline path, and the allocation-regression
/// suite (`tests/logql_pipeline_alloc.rs`, review round 2) pins its
/// per-row allocation bounds from outside the crate — the same hermetic
/// surface the in-module unit tests use.
pub fn run_pipeline_rows(
    rows: Vec<SampleRow>,
    compiled: &super::pipeline::CompiledPipeline,
    meta: &HashMap<Fingerprint, StreamMetaRow>,
    result_limit: u32,
) -> Result<Vec<StreamResult>, ReadError> {
    // A one-shot feed over the whole slice — byte-identical output and
    // per-row allocation profile to the pre-#90 monolithic function (the
    // `logql_pipeline_alloc`/`logql_pipeline_golden` suites pin both).
    let mut acc = StreamAccumulator::new(meta, result_limit);
    acc.feed(&rows, compiled)?;
    Ok(acc.into_streams())
}

/// Issue #230 follow-up / issue #287: a per-row output-budget breach is
/// the bounded 422 (the same complete-or-error class as every other
/// `QueryTooBroad` reason — never a truncation, never an OOM). Each
/// ledger keeps its OWN reason, so the 422 body names the counter that
/// actually refused.
impl From<super::pipeline::RowBudgetExceeded> for ReadError {
    fn from(e: super::pipeline::RowBudgetExceeded) -> Self {
        let budget_bytes = e.budget_bytes;
        ReadError::QueryTooBroad(match e.budget {
            super::pipeline::RowBudget::TemplateRender => {
                TooBroadReason::TemplateOutputBytes { budget_bytes }
            }
            super::pipeline::RowBudget::JsonFlattenKeys => {
                TooBroadReason::JsonFlattenKeyBytes { budget_bytes }
            }
        })
    }
}

/// The streams paging loop's per-page state (issue #312) — a structural
/// sibling of [`DetectedPagedState`] (issue #244), so the two loops share
/// one drain discipline instead of carrying two hand-written twins.
#[derive(Debug)]
pub(in crate::logql) struct StreamsPagedState {
    pub(in crate::logql) cursor: Option<TailCursor>,
    pub(in crate::logql) spent: u64,
    pub(in crate::logql) page_size: u32,
    pub(in crate::logql) budget: u64,
}

impl StreamsPagedState {
    /// Drains ONE already-opened page to completion, charging and
    /// chunking each row through `acc` as it arrives, then returns the
    /// loop's decision: `Ok(None)` continue / `Ok(Some(false))`
    /// terminate-COMPLETE / `Ok(Some(true))` terminate-PARTIAL / `Err`
    /// propagate. The drain stops at the FIRST error — exactly what the
    /// pre-#312 per-row `?` did. Generic over the stream AND its error so
    /// the seam never names `ChError`.
    ///
    /// The staged chunk is flushed BEFORE the page's fate is decided, so
    /// a mid-page `ScanBudgetBytes` breach keeps the rows that already
    /// arrived; a result-budget breach raised by that flush propagates
    /// rather than being downgraded to a partial.
    pub(in crate::logql) async fn absorb_page<S, E>(
        &mut self,
        stream: &mut S,
        read_bytes: impl FnOnce(&S) -> Option<u64>,
        map_err: impl Fn(E) -> ReadError,
        acc: &mut StreamAccumulator<'_>,
        compiled: &super::pipeline::CompiledPipeline,
    ) -> Result<Option<bool>, ReadError>
    where
        S: Stream<Item = Result<TailSampleRow, E>> + Unpin,
    {
        let mut tracker = TailCursorTracker::new();
        let mut page_err: Option<ReadError> = None;
        let mut filled = false;
        while let Some(item) = stream.next().await {
            let row = match item {
                Ok(row) => row,
                Err(e) => {
                    page_err = Some(map_err(e));
                    break;
                }
            };
            // Advance over the RAW page, not survivors — a page entirely
            // dropped by the pipeline must never stall the walk.
            tracker.observe(row.timestamp_ns, row.fingerprint, row.body_hash);
            match acc.push_row(
                SampleRow {
                    fingerprint: row.fingerprint,
                    timestamp_ns: row.timestamp_ns,
                    body: row.body,
                    structured_metadata: row.structured_metadata,
                    rx: row.rx,
                },
                compiled,
            ) {
                Ok(true) => filled = true,
                Ok(false) => {}
                Err(e) => {
                    page_err = Some(e);
                    break;
                }
            }
        }
        let (cursor, fetched) = tracker.finish(self.cursor.take());
        self.cursor = cursor;
        // Feed and RELEASE whatever the drain staged before the page's
        // fate is decided: a mid-page error keeps its prefix, and a
        // result-budget refusal raised here is a hard 422 that
        // `classify_page_error` must never see.
        filled |= acc.flush_chunk(compiled)?;
        if let Some(mapped) = page_err {
            return classify_page_error(mapped, self.spent).map(Some);
        }
        let read = read_bytes(stream).unwrap_or_else(|| self.budget.saturating_sub(self.spent));
        self.spent = self.spent.saturating_add(read);
        if filled {
            // Result limit filled — a complete result, never partial.
            return Ok(Some(false));
        }
        if fetched < self.page_size {
            // Fewer rows than asked ⇒ the window is exhausted — a
            // complete result over the whole window, never partial.
            return Ok(Some(false));
        }
        Ok(None)
    }
}

/// The line-filter-only fast path's RETENTION, factored out of
/// [`LogQlEngine::run_streams_inner`] (issue #312) so its hermetic probe
/// drives the shipped body rather than a transcription of it.
///
/// Zero-structured-metadata rows group by source fingerprint with
/// `labels_json` verbatim; structured-metadata-bearing rows (issue #97)
/// fan out through [`SmFanOutAccumulator`]. Rows are absorbed as they
/// arrive — there is no staging on this path at all — and a row whose
/// fingerprint is absent from `meta` is skipped, which is what the
/// pre-#312 drain's `filter_map` did at the other end.
#[derive(Debug, Default)]
pub(in crate::logql) struct FastPathGroups {
    by_fp: HashMap<Fingerprint, StreamResult>,
    sm: SmFanOutAccumulator,
    /// Issue #463. With `categorize-labels` the response groups by the
    /// STREAM-category label set, not by fingerprint-or-merged-set — so a
    /// structured-metadata row rejoins the stream it came from instead of
    /// fanning out (reference-captured: seven differently-flagged entries
    /// collapse from seven stream objects to one). Both kinds of row
    /// therefore land in ONE label-keyed map here, and `by_fp`/`sm` stay
    /// empty.
    categorize: bool,
    cat_groups: HashMap<String, FanOutGroup>,
    /// Reused per row on the categorised metadata path only — the same
    /// clear-and-refill discipline `SmFanOutAccumulator` uses.
    cat_base: HashMap<Fingerprint, Vec<(String, String)>>,
    cat_merge_buf: Vec<(String, String)>,
    cat_sm_buf: Vec<(String, String)>,
    cat_sm_ctx: StructuredMetadataCtx,
}

impl FastPathGroups {
    pub(in crate::logql) fn new() -> Self {
        Self::default()
    }

    /// [`Self::new`] with the issue #463 wire-shape decision stated.
    pub(in crate::logql) fn new_with(categorize: bool) -> Self {
        Self {
            categorize,
            ..Self::default()
        }
    }

    /// Charges the group on FIRST SIGHT of the fingerprint and the entry
    /// before the body is retained, so neither the map entry nor the
    /// entry exists when the cap refuses.
    pub(in crate::logql) fn push_row(
        &mut self,
        row: SampleRow,
        meta: &HashMap<Fingerprint, StreamMetaRow>,
        budget: &mut StreamsResultBudget,
    ) -> Result<(), ReadError> {
        let Some(m) = meta.get(&row.fingerprint) else {
            return Ok(());
        };
        if self.categorize {
            return self.push_row_categorized(row, m, budget);
        }
        if !row.structured_metadata.is_empty() {
            return self.sm.push_row(&row, meta, budget);
        }
        match self.by_fp.entry(row.fingerprint) {
            std::collections::hash_map::Entry::Occupied(e) => {
                budget.charge_entry(row.body.len(), 0)?;
                e.into_mut().entries.push((row.timestamp_ns, row.body));
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                budget.charge_group(&m.labels, &m.service)?;
                budget.charge_entry(row.body.len(), 0)?;
                e.insert(StreamResult {
                    fingerprint: row.fingerprint,
                    service: m.service.clone(),
                    labels_json: m.labels.clone(),
                    entries: vec![(row.timestamp_ns, row.body)],
                    categories: Vec::new(),
                });
            }
        }
        Ok(())
    }

    /// The issue #463 categorised branch. No pipeline runs on this path,
    /// so the stream category is the hydrated `labels_json` VERBATIM
    /// unless a metadata value took over one of its slots — which is the
    /// only case that re-renders, and is what makes the common row cost
    /// exactly one hash probe more than the plain path.
    fn push_row_categorized(
        &mut self,
        row: SampleRow,
        m: &StreamMetaRow,
        budget: &mut StreamsResultBudget,
    ) -> Result<(), ReadError> {
        if row.structured_metadata.is_empty() {
            return push_fanout_entry(
                &mut self.cat_groups,
                budget,
                GroupKey::Rendered {
                    labels_json: m.labels.clone(),
                    fingerprint: row.fingerprint,
                },
                row.timestamp_ns,
                Cow::Owned(row.body),
                &m.service,
                Some(EntryCategories::default()),
            );
        }
        let base = self
            .cat_base
            .entry(row.fingerprint)
            .or_insert_with(|| parse_canonical_labels(&m.labels));
        merge_labels_with_structured_metadata(
            base,
            &row.structured_metadata,
            &mut self.cat_merge_buf,
            &mut self.cat_sm_buf,
            &mut self.cat_sm_ctx,
        );
        let categories = split_merged_categories(&mut self.cat_merge_buf, &self.cat_sm_ctx);
        // The stream half is unchanged from the hydrated set unless a
        // metadata value won a stream slot, so the canonical JSON is
        // already in hand for every ordinary row.
        let (labels_json, fingerprint) = if self.cat_sm_ctx.sm_over_stream.is_empty() {
            (m.labels.clone(), row.fingerprint)
        } else {
            let sorted: Vec<(Cow<'_, str>, Cow<'_, str>)> = self
                .cat_merge_buf
                .iter()
                .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str())))
                .collect();
            let json = render_labels_json_sorted(&sorted);
            let fp = derived_stream_fingerprint(json.as_bytes());
            (json, fp)
        };
        push_fanout_entry(
            &mut self.cat_groups,
            budget,
            GroupKey::Rendered {
                labels_json,
                fingerprint,
            },
            row.timestamp_ns,
            Cow::Owned(row.body),
            &m.service,
            Some(categories),
        )
    }

    pub(in crate::logql) fn into_streams(self) -> Vec<StreamResult> {
        let mut streams: Vec<StreamResult> = self.by_fp.into_values().collect();
        streams.extend(self.sm.into_streams());
        streams.extend(
            self.cat_groups
                .into_iter()
                .map(|(labels_json, g)| StreamResult {
                    fingerprint: g.fingerprint,
                    service: g.service,
                    labels_json,
                    entries: g.entries,
                    categories: g.categories,
                }),
        );
        streams
    }
}

/// The hermetic test seam for the issue #312 line-filter-only fast path —
/// the [`DetectedFieldsProbe`](super::detected_probe::DetectedFieldsProbe)
/// pattern. `#[doc(hidden)]`: consumed only by
/// `tests/logql_streams_result_budget.rs`, never by callers.
#[doc(hidden)]
#[derive(Debug)]
pub struct StreamsFastPathProbe {
    groups: FastPathGroups,
    budget: StreamsResultBudget,
}

impl StreamsFastPathProbe {
    pub fn with_cap(cap: u64) -> Self {
        Self {
            groups: FastPathGroups::new(),
            budget: StreamsResultBudget::with_cap(cap),
        }
    }

    /// [`Self::with_cap`] with the issue #463 categorised wire shape, so
    /// a gate on the categorised charge drives the shipped accumulator
    /// rather than a formula written beside it.
    pub fn with_cap_categorized(cap: u64) -> Self {
        Self {
            groups: FastPathGroups::new_with(true),
            budget: StreamsResultBudget::with_cap(cap),
        }
    }

    /// One row through the REAL fast-path body.
    pub fn push_row(
        &mut self,
        row: SampleRow,
        meta: &HashMap<Fingerprint, StreamMetaRow>,
    ) -> Result<(), ReadError> {
        self.groups.push_row(row, meta, &mut self.budget)
    }

    pub fn charged(&self) -> u64 {
        self.budget.charged()
    }

    pub fn staged(&self) -> u64 {
        self.budget.staged()
    }

    pub fn into_streams(self) -> Vec<StreamResult> {
        self.groups.into_streams()
    }
}

/// The hermetic test seam for the issue #312 streaming paged loop — the
/// [`DetectedFieldsProbe`](super::detected_probe::DetectedFieldsProbe)
/// pattern, so the loop body under test is the SHIPPED one rather than a
/// transcription of it. `#[doc(hidden)]`: consumed only by
/// `tests/logql_streams_result_budget.rs`, never by callers.
#[doc(hidden)]
#[derive(Debug)]
pub struct StreamsPagedProbe {
    state: StreamsPagedState,
}

impl StreamsPagedProbe {
    pub fn new(page_size: u32, budget: u64) -> Self {
        Self {
            state: StreamsPagedState {
                cursor: None,
                spent: 0,
                page_size,
                budget,
            },
        }
    }

    /// Pretends `spent` bytes were already scanned — the "later page"
    /// half of the #90 branch split.
    pub fn with_spent(mut self, spent: u64) -> Self {
        self.state.spent = spent;
        self
    }

    pub fn spent(&self) -> u64 {
        self.state.spent
    }

    pub fn cursor(&self) -> Option<TailCursor> {
        self.state.cursor
    }

    /// One injected page through the REAL paged-loop body.
    pub async fn absorb_page<S>(
        &mut self,
        stream: &mut S,
        read_bytes: u64,
        acc: &mut StreamAccumulator<'_>,
        compiled: &super::pipeline::CompiledPipeline,
    ) -> Result<Option<bool>, ReadError>
    where
        S: Stream<Item = Result<TailSampleRow, ReadError>> + Unpin,
    {
        self.state
            .absorb_page(stream, |_| Some(read_bytes), |e| e, acc, compiled)
            .await
    }
}

/// The BYTES staged before one [`StreamAccumulator::feed`] — byte-
/// denominated on purpose: a row count is exactly the proxy issue #312
/// exists to reject. Peak staged retention is this plus at most one row,
/// whatever the stream's length or the rows' size.
pub const STREAM_FEED_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

/// The stateful grouping/counting core of [`run_pipeline_rows`], extracted
/// (issue #90) so the fetch-until-limit paging loop can stream multiple
/// keyset pages through ONE accumulator: fan-out/transform grouping and
/// the *global* `result_limit` truncation must span pages (a per-page
/// `run_pipeline_rows` + concat would regroup and re-truncate wrongly).
/// Owns the fp/label group maps + parsed base labels + the survivor
/// counter across [`StreamAccumulator::feed`] calls; the per-row label
/// scratch is re-created per `feed` (a page's borrows cannot outlive the
/// call) but reused across every row within the page, preserving the
/// zero-per-row-alloc dropped-row path.
pub struct StreamAccumulator<'m> {
    meta: &'m HashMap<Fingerprint, StreamMetaRow>,
    result_limit: u32,
    // Base labels parsed once per fingerprint, not per row.
    base_labels: HashMap<Fingerprint, Vec<(String, String)>>,
    // Transform path groups by source fingerprint; fan-out groups by the
    // canonical rendered labels JSON (sorted keys — it doubles as the
    // equality key). Two maps instead of a shared key enum so the fan-out
    // entry API can reuse its own `String` key without a per-row clone
    // (review round 2, finding 1); the fan-out value holds only the
    // per-group accumulator, and the map-owned key MOVES into
    // `StreamResult.labels_json` at final collection — never cloned out of
    // the entry, so high-cardinality fan-out (every row a new group) pays
    // no per-group key duplication either (review round 3).
    fp_groups: HashMap<Fingerprint, StreamResult>,
    label_groups: HashMap<String, FanOutGroup>,
    survivors: u32,
    /// The issue #312 peak-retention ledger — result charges plus the
    /// outstanding staged chunk.
    budget: StreamsResultBudget,
    /// Rows staged between `feed` calls, bounded in BYTES by
    /// [`STREAM_FEED_CHUNK_BYTES`] rather than by a row count — a row
    /// count is exactly the proxy issue #312 exists to reject.
    chunk: Vec<SampleRow>,
    /// Issue #463. With `categorize-labels` EVERY surviving row — plain,
    /// transform and fan-out alike — is keyed by its STREAM-category
    /// label set, so all three land in `label_groups` and `fp_groups`
    /// stays empty. That single map is what makes a stream's
    /// differently-flagged entries collapse back into one object, which
    /// is what the reference returns under the flag.
    categorize: bool,
}

impl<'m> StreamAccumulator<'m> {
    pub fn new(meta: &'m HashMap<Fingerprint, StreamMetaRow>, result_limit: u32) -> Self {
        Self::with_cap(meta, result_limit, MAX_STREAMS_RESULT_BYTES)
    }

    /// [`Self::new`] with the issue #463 wire-shape decision stated.
    pub fn with_opts(
        meta: &'m HashMap<Fingerprint, StreamMetaRow>,
        result_limit: u32,
        opts: super::params::ResponseOptions,
    ) -> Self {
        let mut acc = Self::with_cap(meta, result_limit, MAX_STREAMS_RESULT_BYTES);
        acc.categorize = opts.categorize_labels;
        acc
    }

    /// A test-visible ceiling; production sites call [`Self::new`].
    pub fn with_cap(
        meta: &'m HashMap<Fingerprint, StreamMetaRow>,
        result_limit: u32,
        cap: u64,
    ) -> Self {
        let mut base_labels: HashMap<Fingerprint, Vec<(String, String)>> = HashMap::new();
        for (fp, m) in meta {
            base_labels.insert(*fp, parse_canonical_labels(&m.labels));
        }
        Self {
            meta,
            result_limit,
            base_labels,
            fp_groups: HashMap::new(),
            label_groups: HashMap::new(),
            survivors: 0,
            budget: StreamsResultBudget::with_cap(cap),
            chunk: Vec::new(),
            categorize: false,
        }
    }

    /// Result bytes charged so far (issue #312).
    pub fn charged(&self) -> u64 {
        self.budget.charged()
    }

    /// Staged bytes outstanding right now (issue #312).
    pub fn staged(&self) -> u64 {
        self.budget.staged()
    }

    /// The shipped ceiling this accumulator was built with.
    pub fn cap(&self) -> u64 {
        self.budget.cap()
    }

    /// Stages ONE streamed row — charged IN FULL (body, structured
    /// metadata and slot) before it is pushed — and feeds the chunk once
    /// its staged bytes reach [`STREAM_FEED_CHUNK_BYTES`]. `true` ==
    /// the result limit filled, so the caller can stop paging.
    ///
    /// This replaces the response-sized `Vec<SampleRow>` the streams
    /// paths used to build (issue #312): peak staged bytes are bounded by
    /// the chunk plus at most one row, never by the stream.
    pub fn push_row(
        &mut self,
        row: SampleRow,
        compiled: &super::pipeline::CompiledPipeline,
    ) -> Result<bool, ReadError> {
        self.budget
            .charge_staged_row(row.body.len(), row.structured_metadata.len())?;
        self.chunk.push(row);
        if self.budget.staged() >= STREAM_FEED_CHUNK_BYTES {
            return self.flush_chunk(compiled);
        }
        Ok(self.survivors >= self.result_limit)
    }

    /// Feeds and RELEASES the staged chunk. Must be called after the
    /// drain — the staged charge is only sound if it is discharged when
    /// the rows it paid for are dropped.
    pub fn flush_chunk(
        &mut self,
        compiled: &super::pipeline::CompiledPipeline,
    ) -> Result<bool, ReadError> {
        if self.chunk.is_empty() {
            self.budget.discharge_staged();
            return Ok(self.survivors >= self.result_limit);
        }
        // Taken out so `feed`'s `&[SampleRow]` does not alias `self`;
        // the buffer's capacity is handed back afterwards.
        let mut chunk = std::mem::take(&mut self.chunk);
        let filled = self.feed(&chunk, compiled);
        chunk.clear();
        self.chunk = chunk;
        self.budget.discharge_staged();
        filled
    }

    /// Feeds one page of rows in arrival (direction) order — arrival order
    /// IS the response order, so the global `result_limit` truncation
    /// below is correct across pages. Returns `true` once `survivors ==
    /// result_limit` (the caller stops paging).
    pub fn feed(
        &mut self,
        rows: &[SampleRow],
        compiled: &super::pipeline::CompiledPipeline,
    ) -> Result<bool, ReadError> {
        let Self {
            meta,
            result_limit,
            base_labels,
            fp_groups,
            label_groups,
            survivors,
            budget,
            // The staged chunk is `take`n by `flush_chunk` before it calls
            // here, so `feed` never sees it. Named rather than elided so a
            // future field is a build failure, not a silent omission.
            chunk: _,
            categorize,
        } = self;
        let categorize = *categorize;
        // Categorised mode groups by the stream-category subset, which is
        // a rendered label set for every row — so it takes the fan-out
        // shape whatever the pipeline does.
        let fan_out = compiled.mutates_labels() || categorize;
        // One label scratch reused across every row of this page (issue
        // #72 review round 1, finding 3): `run_into` clears and refills the
        // same vector — zero per-row label-vector allocations on the
        // dropped-row (zero-structured-metadata) path.
        let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
        // The structured-metadata merge buffers (issue #97): reused across SM-
        // bearing rows (clear + refill, capacity-amortized), never a fresh
        // per-row allocation of the label vector itself. `merge_buf` holds the
        // merged result; `sm_buf` is the SM-pair parse scratch. Only SM-bearing
        // rows touch them — the empty-SM path never allocates or clears them.
        let mut merge_buf: Vec<(String, String)> = Vec::new();
        let mut sm_buf: Vec<(String, String)> = Vec::new();
        // The Cow label scratch `run_into` fills for SM-bearing rows. Held
        // `'static`-tagged (always empty) between rows and re-tagged per row so
        // its allocation is reused across the page — never a fresh per-row
        // allocation (issue #97 review round 1, finding 2 / AC-12). See
        // `eval_structured_metadata_row` for why the reuse goes through a
        // by-value helper rather than a hoisted `&mut` binding.
        let mut sm_scratch: LabelScratch<'static> = Vec::new();
        // Issue #463: the per-label category vector the categorised
        // pipeline fills, reused across rows exactly like the label
        // scratch beside it. Never touched when the flag is off.
        let mut cat_scratch: Vec<super::pipeline::LabelCategory> = Vec::new();
        // The per-row reserved-SM routing outcome (issue #238), cleared and
        // refilled by the merge — reused across rows like the buffers above.
        let mut sm_ctx = StructuredMetadataCtx::default();

        for row in rows {
            if *survivors >= *result_limit {
                break;
            }
            let Some(m) = meta.get(&row.fingerprint) else {
                continue;
            };
            let base = &base_labels[&row.fingerprint];

            if row.structured_metadata.is_empty() {
                // Zero-structured-metadata fast path — UNCHANGED (the
                // `logql_pipeline_alloc` golden pins its zero-per-row
                // profile; AC-8 byte-identity for pre-#97 data).
                // Issue #624, part 3a (D6): the `regexp` captures the
                // database ran, when the read sent them.
                let captures = super::pipeline::RegexpCaptures::Rows(&row.rx);
                let Some(line) = (if categorize {
                    compiled.run_into_with_sm_categorized_captured(
                        &row.body,
                        base,
                        row.timestamp_ns,
                        &EMPTY_STRUCTURED_METADATA,
                        captures,
                        &mut scratch,
                        &mut cat_scratch,
                    )?
                } else {
                    compiled.run_into_with_sm_captured(
                        &row.body,
                        base,
                        row.timestamp_ns,
                        &EMPTY_STRUCTURED_METADATA,
                        captures,
                        &mut scratch,
                    )?
                }) else {
                    continue;
                };
                *survivors += 1;
                if fan_out {
                    // Render the canonical JSON DIRECTLY from the sorted
                    // borrowed scratch (round-2 finding 1: no owned
                    // intermediate label vector, no second clone at render
                    // time). Per surviving row this costs exactly the
                    // `labels_json` string (needed as the group key either
                    // way) + the owned output line; the `StreamResult` fields
                    // materialize once per NEW group only.
                    // Issue #463: split BEFORE the sort — the category
                    // vector is parallel to the label vector in the
                    // order the pipeline left it.
                    let categories =
                        categorize.then(|| split_categories(&mut scratch, &cat_scratch));
                    scratch.sort_unstable();
                    push_fanout_entry(
                        label_groups,
                        budget,
                        GroupKey::Sorted(&scratch),
                        row.timestamp_ns,
                        line,
                        &m.service,
                        categories,
                    )?;
                } else {
                    // Issue #312: the group is charged on FIRST SIGHT of
                    // the fingerprint and the entry before
                    // `line.into_owned()`, so neither the map entry nor
                    // the owned line exists when the cap refuses.
                    match fp_groups.entry(row.fingerprint) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            budget.charge_entry(line.len(), 0)?;
                            e.into_mut()
                                .entries
                                .push((row.timestamp_ns, line.into_owned()));
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            budget.charge_group(&m.labels, &m.service)?;
                            budget.charge_entry(line.len(), 0)?;
                            e.insert(StreamResult {
                                fingerprint: row.fingerprint,
                                service: m.service.clone(),
                                labels_json: m.labels.clone(),
                                entries: vec![(row.timestamp_ns, line.into_owned())],
                                categories: Vec::new(),
                            });
                        }
                    }
                }
            } else {
                // Structured-metadata-bearing row (issue #97): merge the
                // cached base labels + parsed SM into the reused owned buffer
                // (colliding SM keys renamed `_extracted`, per the oracle),
                // then run the pipeline over that contiguous base. SM changes
                // the label set, so these rows ALWAYS fan out (matching Loki's
                // per-entry SM fan-out). Only SM-bearing rows pay this cost.
                merge_labels_with_structured_metadata(
                    base,
                    &row.structured_metadata,
                    &mut merge_buf,
                    &mut sm_buf,
                    &mut sm_ctx,
                );
                // Reuse `sm_scratch`'s allocation across rows: the helper takes
                // it by value (fresh per-row lifetime for the `merge_buf`
                // borrow), `recycle_label_scratch` returns the same allocation.
                let (survived, used) = eval_structured_metadata_row(
                    compiled,
                    &row.body,
                    &row.rx,
                    &merge_buf,
                    &sm_ctx,
                    label_groups,
                    budget,
                    row.timestamp_ns,
                    &m.service,
                    sm_scratch,
                    categorize.then_some(&mut cat_scratch),
                );
                sm_scratch = recycle_label_scratch(used?);
                if survived {
                    *survivors += 1;
                }
            }
        }

        Ok(*survivors >= *result_limit)
    }

    pub fn into_streams(self) -> Vec<StreamResult> {
        self.fp_groups
            .into_values()
            .chain(
                self.label_groups
                    .into_iter()
                    .map(|(labels_json, g)| StreamResult {
                        fingerprint: g.fingerprint,
                        service: g.service,
                        labels_json,
                        entries: g.entries,
                        categories: g.categories,
                    }),
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::testkit::*;

    // -----------------------------------------------------------------
    // Issue M6-09 AC9(ii): the true limit applies globally after
    // in-engine filtering — both directions, fan-out, post-line_format
    // line filters. Hermetic over `run_pipeline_rows`, the exact function
    // `run_streams_inner` hands fetched rows to.
    // -----------------------------------------------------------------

    fn pipeline_of(query: &str) -> super::super::pipeline::CompiledPipeline {
        let expr = pulsus_logql::parse(query).expect("parse");
        let pulsus_logql::Expr::Log(log) = expr else {
            panic!("expected a log expr");
        };
        super::super::pipeline::CompiledPipeline::compile(&log.pipeline).expect("compile")
    }

    fn meta_two_streams() -> HashMap<Fingerprint, StreamMetaRow> {
        HashMap::from([
            (
                Fingerprint::from_raw(1),
                StreamMetaRow {
                    fingerprint: Fingerprint::from_raw(1),
                    service: "checkout".to_string(),
                    labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
                },
            ),
            (
                Fingerprint::from_raw(2),
                StreamMetaRow {
                    fingerprint: Fingerprint::from_raw(2),
                    service: "billing".to_string(),
                    labels: r#"{"env":"staging","service_name":"billing"}"#.to_string(),
                },
            ),
        ])
    }

    /// Issue #97 review round 1, finding 3 (+ oracle probe against
    /// grafana/loki:3.4.2's default query response): a structured-metadata key
    /// that collides with a stream label key is renamed `<key>_extracted`; the
    /// stream label keeps the original key/value, both appear exactly once (no
    /// duplicate key entries), and the non-colliding SM key merges verbatim.
    /// Same `_extracted` precedence the `| json` parser already uses for
    /// parsed-label collisions.
    #[test]
    fn structured_metadata_key_colliding_with_base_label_lands_under_extracted_suffix() {
        // fp 1 base labels: env=prod, service_name=checkout.
        let meta = meta_two_streams();
        let compiled = pipeline_of(r#"{a="b"}"#);
        let rows = vec![SampleRow {
            fingerprint: Fingerprint::from_raw(1),
            timestamp_ns: 10,
            body: "line".to_string(),
            structured_metadata: r#"{"env":"SMVAL","trace_id":"abc"}"#.to_string(),
            rx: Vec::new(),
        }];
        let results =
            run_pipeline_rows(rows, &compiled, &meta, 100).expect("no template budget breach");
        assert_eq!(results.len(), 1);
        // Canonical sorted JSON: the stream `env` keeps "prod"; the colliding
        // SM `env` surfaces as `env_extracted`; `trace_id` merges as-is.
        assert_eq!(
            results[0].labels_json,
            r#"{"env":"prod","env_extracted":"SMVAL","service_name":"checkout","trace_id":"abc"}"#
        );
    }

    /// Issue #97 review round 2, finding 1 (+ grafana/loki:3.4.2 oracle probe):
    /// a DOUBLE collision must still not emit a duplicate label entry. Base
    /// labels already carry both `env` AND `env_extracted`; the SM `env` renames
    /// to `env_extracted`, which ALSO exists — so it overwrites that slot
    /// (last-write-wins) rather than producing two `env_extracted` entries.
    /// Probed against grafana/loki:3.4.2's default query response: base
    /// `env=prod`+`env_extracted=baseval` + SM `env=smval` renders exactly one
    /// `env_extracted`, and the SM value wins it (no `env_extracted_extracted`,
    /// no numeric suffix, no drop).
    #[test]
    fn structured_metadata_double_collision_overwrites_the_extracted_slot_once() {
        // A stream whose base labels include both `env` and `env_extracted`.
        let meta: HashMap<Fingerprint, StreamMetaRow> = [(
            Fingerprint::from_raw(7),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(7),
                service: "checkout".to_string(),
                labels: r#"{"env":"prod","env_extracted":"baseval","service_name":"checkout"}"#
                    .to_string(),
            },
        )]
        .into_iter()
        .collect();
        let compiled = pipeline_of(r#"{a="b"}"#);
        let rows = vec![SampleRow {
            fingerprint: Fingerprint::from_raw(7),
            timestamp_ns: 10,
            body: "line".to_string(),
            structured_metadata: r#"{"env":"smval"}"#.to_string(),
            rx: Vec::new(),
        }];
        let results =
            run_pipeline_rows(rows, &compiled, &meta, 100).expect("no template budget breach");
        assert_eq!(results.len(), 1);
        // Exactly one `env_extracted`, carrying the SM value (last-write-wins);
        // the stream `env` keeps "prod"; no duplicate key entries.
        assert_eq!(
            results[0].labels_json,
            r#"{"env":"prod","env_extracted":"smval","service_name":"checkout"}"#
        );
    }

    /// Companion double-collision case: the SM object ITSELF supplies both the
    /// colliding key and its `_extracted` form. Base `env=prod`; SM
    /// `env=smval`,`env_extracted=smextra`. The renamed `env` and the literal
    /// `env_extracted` land in the same slot — last-write-wins, one entry.
    /// Matches the grafana/loki:3.4.2 oracle probe (`env_extracted=smextra`).
    #[test]
    fn structured_metadata_supplying_its_own_extracted_key_collapses_to_one_entry() {
        let meta: HashMap<Fingerprint, StreamMetaRow> = [(
            Fingerprint::from_raw(7),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(7),
                service: "checkout".to_string(),
                labels: r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
            },
        )]
        .into_iter()
        .collect();
        let compiled = pipeline_of(r#"{a="b"}"#);
        let rows = vec![SampleRow {
            fingerprint: Fingerprint::from_raw(7),
            timestamp_ns: 10,
            body: "line".to_string(),
            structured_metadata: r#"{"env":"smval","env_extracted":"smextra"}"#.to_string(),
            rx: Vec::new(),
        }];
        let results =
            run_pipeline_rows(rows, &compiled, &meta, 100).expect("no template budget breach");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels_json,
            r#"{"env":"prod","env_extracted":"smextra","service_name":"checkout"}"#
        );
    }

    /// Backward-direction arrival order (newest first), interleaved
    /// across two streams; every row survives the filter — the global
    /// truncation must keep the first `limit` in ARRIVAL order, not
    /// `limit` per stream.
    #[test]
    fn result_limit_applies_globally_across_streams_in_backward_order() {
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let rows = vec![
            sample(1, 40, r#"{"status":"500","m":"d"}"#),
            sample(2, 30, r#"{"status":"500","m":"c"}"#),
            sample(1, 20, r#"{"status":"500","m":"b"}"#),
            sample(2, 10, r#"{"status":"500","m":"a"}"#),
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 3)
            .expect("no template budget breach");
        let total: usize = results.iter().map(|r| r.entries.len()).sum();
        assert_eq!(total, 3, "global cap, not per-stream");
        let mut kept: Vec<i64> = results
            .iter()
            .flat_map(|r| r.entries.iter().map(|(ts, _)| *ts))
            .collect();
        kept.sort_unstable();
        assert_eq!(kept, vec![20, 30, 40], "newest three in backward order");
    }

    #[test]
    fn result_limit_applies_globally_in_forward_order_too() {
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let rows = vec![
            sample(2, 10, r#"{"status":"500","m":"a"}"#),
            sample(1, 20, r#"{"status":"500","m":"b"}"#),
            sample(2, 30, r#"{"status":"500","m":"c"}"#),
            sample(1, 40, r#"{"status":"500","m":"d"}"#),
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 3)
            .expect("no template budget breach");
        let mut kept: Vec<i64> = results
            .iter()
            .flat_map(|r| r.entries.iter().map(|(ts, _)| *ts))
            .collect();
        kept.sort_unstable();
        assert_eq!(kept, vec![10, 20, 30], "oldest three in forward order");
    }

    /// The fan-out path splits one source stream by parsed label set and
    /// still respects the global limit; dropped lines don't count toward
    /// it.
    #[test]
    fn fan_out_regroups_by_final_label_set_with_canonical_labels_json() {
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let rows = vec![
            sample(1, 10, r#"{"status":"500","method":"GET"}"#),
            sample(1, 20, r#"{"status":"200","method":"GET"}"#), // dropped
            sample(1, 30, r#"{"status":"500","method":"PUT"}"#),
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 100)
            .expect("no template budget breach");
        assert_eq!(results.len(), 2, "one result stream per final label set");
        let total: usize = results.iter().map(|r| r.entries.len()).sum();
        assert_eq!(total, 2);
        for r in &results {
            assert!(
                r.labels_json.contains(r#""env":"prod""#)
                    && r.labels_json.contains(r#""status":"500""#),
                "canonical labels_json must carry base + parsed labels: {}",
                r.labels_json
            );
            // Canonical rendering: sorted keys.
            assert!(
                r.labels_json.find("\"env\"").unwrap() < r.labels_json.find("\"method\"").unwrap()
            );
            assert_eq!(
                r.fingerprint,
                derived_stream_fingerprint(r.labels_json.as_bytes())
            );
            assert_eq!(r.service, "checkout");
        }
    }

    /// A post-`line_format` line filter evaluates in-engine over the
    /// REWRITTEN line, drops non-matching entries, and the survivors
    /// respect the global limit.
    #[test]
    fn a_post_line_format_line_filter_drops_in_engine_and_respects_the_limit() {
        let compiled =
            pipeline_of(r#"{a="b"} | json | line_format "{{.method}} {{.status}}" |= "500""#);
        let rows = vec![
            sample(1, 10, r#"{"status":"500","method":"GET"}"#),
            sample(1, 20, r#"{"status":"200","method":"GET"}"#), // rewritten line lacks "500"
            sample(1, 30, r#"{"status":"500","method":"PUT"}"#),
            sample(1, 40, r#"{"status":"500","method":"DELETE"}"#),
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 2)
            .expect("no template budget breach");
        let mut entries: Vec<(i64, String)> = results
            .iter()
            .flat_map(|r| r.entries.iter().cloned())
            .collect();
        entries.sort();
        assert_eq!(
            entries,
            vec![(10, "GET 500".to_string()), (30, "PUT 500".to_string())],
            "rewritten survivors only, capped globally at 2"
        );
    }

    /// Issue #90 AC1 (exact fill, hermetic): a heavily-dropping pipeline
    /// fed page-by-page through ONE `StreamAccumulator` fills to exactly
    /// `result_limit`, whereas the pre-#90 single oversampled scan (one
    /// `run_pipeline_rows` over just the first page) under-returned. The
    /// accumulator's grouping and global truncation span pages.
    #[test]
    fn stream_accumulator_fills_exactly_to_the_limit_across_pages() {
        // Only every 4th line matches `status = "500"` — sparse survivors.
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let statuses = ["200", "404", "500", "503"];
        let page = |base_ts: i64| -> Vec<SampleRow> {
            (0..4)
                .map(|i| {
                    let ts = base_ts + i;
                    sample(
                        1,
                        ts,
                        &format!(r#"{{"status":"{}","m":"{ts}"}}"#, statuses[i as usize]),
                    )
                })
                .collect()
        };
        let meta = meta_two_streams();

        // The pre-#90 behaviour: a single page of 4 rows yields only ONE
        // survivor — an under-return against a limit of 3.
        assert_eq!(
            run_pipeline_rows(page(0), &compiled, &meta, 3)
                .expect("no budget breach")
                .iter()
                .map(|r| r.entries.len())
                .sum::<usize>(),
            1,
            "one page under-returns (1 < limit 3) — the old divergence",
        );

        // Fetch-until-limit: feed successive pages until the accumulator
        // reports the limit is filled.
        let mut acc = StreamAccumulator::new(&meta, 3);
        let mut pages = 0;
        let mut base_ts = 0;
        loop {
            let filled = acc
                .feed(&page(base_ts), &compiled)
                .expect("no budget breach");
            pages += 1;
            base_ts += 4;
            if filled {
                break;
            }
            assert!(pages < 100, "must terminate");
        }
        let total: usize = acc.into_streams().iter().map(|r| r.entries.len()).sum();
        assert_eq!(total, 3, "exact fill to the limit across pages");
        assert_eq!(
            pages, 3,
            "one survivor per 4-row page ⇒ 3 pages fill limit 3"
        );
    }

    /// Issue #90: the accumulator never over-fills — once the limit is
    /// reached, a further `feed` adds nothing and keeps reporting filled.
    #[test]
    fn stream_accumulator_never_over_returns_on_a_later_page() {
        let compiled = pipeline_of(r#"{a="b"} | json | status = "500""#);
        let rows = |ts: i64| {
            vec![
                sample(1, ts, r#"{"status":"500","m":"x"}"#),
                sample(1, ts + 1, r#"{"status":"500","m":"y"}"#),
            ]
        };
        let meta = meta_two_streams();
        let mut acc = StreamAccumulator::new(&meta, 3);
        assert!(
            !acc.feed(&rows(0), &compiled).expect("no budget breach"),
            "2 < 3, not filled"
        );
        assert!(
            acc.feed(&rows(10), &compiled).expect("no budget breach"),
            "2 + 2 ⇒ filled at 3"
        );
        // A further page must not push the total past the limit.
        acc.feed(&rows(20), &compiled).expect("no budget breach");
        let total: usize = acc.into_streams().iter().map(|r| r.entries.len()).sum();
        assert_eq!(
            total, 3,
            "global cap holds across pages, never over-returns"
        );
    }

    /// The transform path (drops/rewrites but never touches labels) keeps
    /// the hydrated `labels_json` verbatim and the source fingerprint.
    #[test]
    fn transform_path_keeps_labels_json_verbatim() {
        let compiled = pipeline_of(r#"{a="b"} | line_format "L={{.env}}" |= "L=prod""#);
        let rows = vec![
            sample(1, 10, "anything"),
            sample(2, 20, "anything"), // env=staging -> rewritten "L=staging" -> dropped
        ];
        let results = run_pipeline_rows(rows, &compiled, &meta_two_streams(), 100)
            .expect("no template budget breach");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].fingerprint, Fingerprint::from_raw(1));
        assert_eq!(
            results[0].labels_json, r#"{"env":"prod","service_name":"checkout"}"#,
            "transform path must splice hydration labels verbatim"
        );
        assert_eq!(results[0].entries, vec![(10, "L=prod".to_string())]);
    }
}
