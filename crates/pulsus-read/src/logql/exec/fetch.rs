use super::*;

impl LogQlEngine {
    /// Wraps [`ChClient::query_stream`] with the placeholder-escaping fix
    /// (see [`escape_query_placeholders`]) every call site in this module
    /// must apply — centralized here so no future call site can forget it.
    /// Issue #35: also the guard choke point — [`ensure_query_text_fits`]
    /// runs against the FINAL text (after doubling, so a `?`-heavy regex
    /// predicate is never undercounted) before the query ever reaches
    /// ClickHouse, and a dispatch-time `ChError` is mapped through
    /// [`map_read_error`] here so call sites no longer need their own
    /// outer `map_err` (per-row mapping inside the streaming loop is
    /// unchanged — a `ChRowStream` yields raw `ChError` per row, not
    /// through this wrapper).
    pub(super) async fn query_stream<'a, R: ChRow>(
        &'a self,
        sql: &str,
        settings: &QuerySettings,
    ) -> Result<ChRowStream<'a, R>, ReadError> {
        let sql = escape_query_placeholders(sql);
        if let Err(reason) = crate::querytext::ensure_query_text_fits(&sql) {
            return Err(ReadError::QueryTooBroad(reason));
        }
        self.client
            .query_stream::<R>(&sql, settings)
            .await
            .map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })
    }

    /// [`LogQlEngine::query_stream`] for a read that sends the `rx` column
    /// when `rx` is true (issue #624, part 3a, D6): the twin `T` is decoded
    /// and turned into the base row `B`, which carries the captures.
    pub(super) async fn query_rows<'a, B: ChRow, T: ChRow + Into<B>>(
        &'a self,
        sql: &str,
        settings: &QuerySettings,
        rx: bool,
    ) -> Result<RxRows<'a, B, T>, ReadError> {
        Ok(if rx {
            RxRows::Rx(self.query_stream::<T>(sql, settings).await?)
        } else {
            RxRows::Plain(self.query_stream::<B>(sql, settings).await?)
        })
    }

    /// Stage 1 — stream resolution. **Budget-capped** (fix-plan amendment
    /// §1, code review finding "Stage 1 bypasses the scan budget"):
    /// docs/schemas.md §3.2 line 305 ties the "aborts with 'query too
    /// broad'" guarantee to the stage-1 index scan itself, not just
    /// stage 3/metric reads — a broad `log_streams_idx` scan must never run
    /// uncapped.
    pub(super) async fn resolve_fingerprints(
        &self,
        stage1_sql: &str,
    ) -> Result<Vec<Fingerprint>, ReadError> {
        let mut fingerprints = Vec::new();
        let mut stream = self
            .query_stream::<StreamRow>(stage1_sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })?;
            fingerprints.push(row.fingerprint);
            check_stream_cap(fingerprints.len(), self.config.max_streams)?;
        }
        // The server returns the rows in no fixed order; sorted, every
        // statement built from the list has one text, and so does the
        // explain trace (issue #624, part 3d), as the unscoped read below.
        fingerprints.sort_unstable();
        Ok(fingerprints)
    }

    /// Every fingerprint ACTIVE in `window`, with no selector to narrow it
    /// — the unscoped arm of [`super::sql::active_fingerprints`], which
    /// `label_names`/`label_values`/`detected_labels` already render today
    /// (issue #399). Issue #406 Part A: this is what `/series` with no
    /// `match[]` resolves against.
    ///
    /// Caps INSIDE the streaming loop, exactly as
    /// [`LogQlEngine::resolve_fingerprints`] does, so an oversized store is
    /// refused after `max_streams + 1` rows rather than after all of them —
    /// and so the caller must not re-check the cap on the result.
    ///
    /// Sorted before returning so the stage-2 `fingerprint IN (...)` text
    /// (and therefore the explain trace) is deterministic, matching the
    /// matched path's sorted union. `DISTINCT fingerprint` already makes
    /// the rows unique; the `dedup` is a cheap restatement of that
    /// invariant, not a second pass over duplicates.
    pub(super) async fn all_active_fingerprints(
        &self,
        window: super::sql::TimeWindow,
    ) -> Result<Vec<Fingerprint>, ReadError> {
        let sql = super::sql::active_fingerprints(
            &self.tenant,
            &self.config.rollup_table,
            super::sql::ActivityScope::All,
            window,
            self.config.rollup_res_ns,
        );
        self.run_activity_scan(&sql).await
    }

    /// Runs one already-rendered [`super::sql::active_fingerprints`] statement
    /// and returns its sorted, deduplicated result.
    ///
    /// **The cap is applied INSIDE the streaming loop**, so an oversized store
    /// is refused after `max_streams + 1` rows rather than after all of them —
    /// and the caller must not re-check it on the result. That refusal is the
    /// shipped `422 query_too_broad` carrying `TooBroadReason::StreamCap`, the
    /// same one stage-1 resolution has always raised. Since issue #399 only
    /// `/series` reads its activity set this way; label discovery nests the
    /// read inside its one statement.
    ///
    /// Sorted before returning so the rendered `fingerprint IN (...)` text —
    /// and therefore the explain trace — is deterministic. `DISTINCT
    /// fingerprint` already makes the rows unique; the `dedup` is a cheap
    /// restatement of that invariant, not a second pass over duplicates.
    async fn run_activity_scan(&self, sql: &str) -> Result<Vec<Fingerprint>, ReadError> {
        let mut fingerprints: Vec<Fingerprint> = Vec::new();
        // Scoped so the pooled connection's lease drops before the caller
        // issues its next statement (the `ChRowStream` lease contract).
        {
            let mut stream = self
                .query_stream::<StreamRow>(sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                fingerprints.push(row.fingerprint);
                check_stream_cap(fingerprints.len(), self.config.max_streams)?;
            }
        }
        fingerprints.sort_unstable();
        fingerprints.dedup();
        Ok(fingerprints)
    }

    /// Stage 2 — hydration. **Budget-capped** for the same reason as stage 1
    /// (fix-plan amendment §1): the scan budget is a per-query cap
    /// (docs/configuration.md §6), not a stage-3-only concern.
    pub(super) async fn hydrate(
        &self,
        streams_table: &str,
        fingerprints: &[Fingerprint],
    ) -> Result<HashMap<Fingerprint, StreamMetaRow>, ReadError> {
        let mut out = HashMap::with_capacity(fingerprints.len());
        if fingerprints.is_empty() {
            return Ok(out);
        }
        let sql = super::sql::stage2(&self.tenant, streams_table, &sql_literals(fingerprints));
        let mut stream = self
            .query_stream::<StreamMetaRow>(&sql, &self.budget_settings())
            .await?;
        while let Some(row) = stream.next().await {
            let row = row.map_err(|e| {
                map_read_error(
                    e,
                    self.config.scan_budget_bytes,
                    self.config.read_max_memory_bytes,
                )
            })?;
            // ReplacingMergeTree without FINAL may yield duplicate rows per
            // fingerprint; labels/service are identical per fingerprint, so
            // keeping any one row is safe (docs/schemas.md §3.2 edge cases).
            out.entry(row.fingerprint).or_insert(row);
        }
        Ok(out)
    }

    pub(super) fn budget_settings(&self) -> QuerySettings {
        self.tagged(read_query_settings(
            self.config.scan_budget_bytes,
            self.config.read_max_memory_bytes,
            self.config.skip_unavailable_shards,
        ))
    }

    /// Applies the `log_comment` tag, when one is set (issues #544 and
    /// #614).
    ///
    /// **The one place it is applied**: every LogQL settings object is
    /// built through it, so no read path can be missed. Without a tag this
    /// is the identity — `system.query_log` records a setting only when
    /// the client sent it, so an untagged read's `log_comment` stays empty
    /// and nothing about the statements themselves moves.
    fn tagged(&self, settings: QuerySettings) -> QuerySettings {
        match &self.query_log_comment {
            Some(tag) => settings.set("log_comment", tag.as_str()),
            None => settings,
        }
    }

    /// The request's own bounds, as the activity semi-join's window
    /// (issue #399). A plain re-wrap: the discovery endpoints take client
    /// bounds verbatim and never widen them, so there is nothing to
    /// derive — the ONE quantization is
    /// [`super::sql::activity_lower_bucket_ns`], applied inside the
    /// builder where the rollup resolution lives.
    pub(super) fn activity_window(&self, b: TimeBounds) -> super::sql::TimeWindow {
        super::sql::TimeWindow {
            start_ns: b.start_ns,
            end_ns: b.end_ns,
        }
    }

    /// Narrows a resolved fingerprint set to those with log lines in
    /// `window` (issue #399). Its month-scoped scan is
    /// [`super::sql::stage1`], shared with every other LogQL path, where an
    /// activity predicate would be redundant — those paths bound the window on
    /// their own sample/rollup/patterns read.
    ///
    /// `/series` dispatches the activity scan as its own statement, as it
    /// always has; the three label-discovery endpoints nest it inside their
    /// one statement instead (issue #399).
    ///
    /// Runs AFTER `check_stream_cap` so the cap keeps its current meaning
    /// — the deduped pre-window union — and the cap test keeps failing for
    /// the reason it was written for (`series_stream_cap.rs`). PK-pruned:
    /// the caller's list is the `(fingerprint, bucket_ns)` prefix
    /// (measured, issue #399: 73,728 rows vs 29,080,654 unscoped on the
    /// cost fixture). Dispatched with [`LogQlEngine::budget_settings`], as
    /// every other statement on this path now is.
    ///
    /// Returns the caller's order filtered by membership, so
    /// `series_inner`'s sort/dedup contract is untouched.
    pub(super) async fn active_fingerprints(
        &self,
        fingerprints: &[Fingerprint],
        window: super::sql::TimeWindow,
    ) -> Result<Vec<Fingerprint>, ReadError> {
        let sql = super::sql::active_fingerprints(
            &self.tenant,
            &self.config.rollup_table,
            super::sql::ActivityScope::Listed(&sql_literals(fingerprints)),
            window,
            self.config.rollup_res_ns,
        );
        let mut active: BTreeSet<Fingerprint> = BTreeSet::new();
        // Scoped so the pooled connection's lease drops before the caller
        // issues stage 2 (the `ChRowStream` lease contract).
        {
            let mut stream = self
                .query_stream::<StreamRow>(&sql, &self.budget_settings())
                .await?;
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| {
                    map_read_error(
                        e,
                        self.config.scan_budget_bytes,
                        self.config.read_max_memory_bytes,
                    )
                })?;
                active.insert(row.fingerprint);
            }
        }
        Ok(fingerprints
            .iter()
            .copied()
            .filter(|fp| active.contains(fp))
            .collect())
    }

    /// Per-page settings for the fetch-until-limit paging loop (issue
    /// #90). `remaining` is the decrementing `budget − spent` cap, an
    /// approximate best-effort scan guard (NOT a hard byte ceiling) that
    /// bounds runaway paging (see [`LogQlEngine::run_streams_paged`]): if
    /// the FIRST page alone overflows this cap the query fails
    /// `QueryTooBroad`, but once a page has returned a later page tripping
    /// its positive cap returns partial survivors instead. Because
    /// ClickHouse enforces the cap per read block per concurrent reader
    /// (per thread, and per shard on a cluster), actual bytes can exceed
    /// the budget, growing with parallelism and shard count.
    /// `wait_end_of_query = 1` forces ClickHouse to emit the FINAL
    /// `read_bytes` in the summary — the clickhouse 0.15.1 crate captures
    /// the summary from the initial response header and never updates it,
    /// so without this the per-page `read_bytes` used to decrement the
    /// remaining cap would be understated and the guard would leak scan.
    /// Each page is `LIMIT page_size`-bounded, so `wait_end_of_query`
    /// buffers only the (small) result, not the scan.
    ///
    /// `pub` for introspection: the AC5 gate asserts `wait_end_of_query = 1`
    /// is present here. That guard cannot live on `system.query_log` —
    /// `wait_end_of_query` is an HTTP-interface-only parameter (absent from
    /// `system.settings`, never recorded in `query_log.Settings`), and the
    /// summed server-side `read_bytes` is identical with or without it
    /// (the setting only affects the CLIENT-side per-page `read_bytes` this
    /// method's caller uses for budget accounting), so the wiring is only
    /// observable here, at the settings object (issue #90).
    ///
    /// Issue #398: `remaining` is the DECREMENTED byte budget and lands in
    /// `max_bytes_to_read` only. The memory ceiling is per query and is
    /// never decremented — every page carries the same
    /// `reader.logql_read_max_memory_bytes`.
    pub fn paging_settings(&self, remaining: u64) -> QuerySettings {
        self.tagged(
            read_query_settings(
                remaining,
                self.config.read_max_memory_bytes,
                self.config.skip_unavailable_shards,
            )
            .set("wait_end_of_query", 1),
        )
    }

    /// Executes a [`StreamsPlan`] end to end. When `explain` is `Some`,
    /// every stage's already-computed SQL is pushed into it in the same
    /// single pass that executes it — no second run (architect plan
    /// amendment §3; see [`LogQlEngine::query_explained`]).
    ///
    /// Three response paths (issue M6-09):
    /// - **fast** — line-filter-only pipeline (everything pushed down):
    ///   the M1 shape, byte-identical (`labels_json` verbatim, SQL `LIMIT
    ///   == limit`, zero new per-row work);
    /// - **transform** — the pipeline drops/rewrites lines but never
    ///   changes the label set: per-fingerprint grouping, `labels_json`
    ///   verbatim, entries filtered/rewritten;
    /// - **fan-out** — a parser/`label_format` (or an `__error__`-adding
    ///   numeric filter) can change the label set: surviving entries
    ///   regroup by final label set, one `StreamResult` per set with a
    ///   canonically re-rendered `labels_json`.
    ///
    /// The stage-3 row bound for THIS read (issue #544).
    ///
    /// [`StreamsPlan::scan_limit`] is the plan's, taken from the plan's
    /// own `fetch_until_limit`. When a metadata fragment does not fit its
    /// budget the read pages after all, and the first page's size is the
    /// oversample the plan would have carried.
    pub(super) fn effective_scan_limit(&self, sp: &StreamsPlan, fetch_until_limit: bool) -> u32 {
        if fetch_until_limit == sp.fetch_until_limit {
            return sp.scan_limit;
        }
        if fetch_until_limit {
            sp.result_limit
                .saturating_mul(self.config.pipeline_scan_factor)
        } else {
            sp.result_limit
        }
    }
}
