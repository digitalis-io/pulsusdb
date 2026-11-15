use super::*;

/// One output series' points in the bucketed fold.
pub(in crate::logql) enum SeriesPoints {
    /// One slot per grid point, `None` where no row arrived (issue #624):
    /// the sliding statement, at a range unequal to the step.
    Dense(Vec<Option<u64>>),
    /// Only the grid points a row arrived at, each charged as it arrives
    /// (issue #507): #507's statement, at a range equal to the step.
    Sparse(HashMap<i64, u64>),
}

/// One output series of the bucketed fold: its labels and its points.
pub(in crate::logql) type DenseSeries = (LabelSet, SeriesPoints);

/// The bucketed range read's client-side fold (issue #507, W2).
///
/// ```text
/// row (fingerprint, bucket_ns, n, structured_metadata)
///   -> the base label set for that fingerprint   stage 2, snapshotted once
///   -> merge the row's structured metadata in    the `PushdownInstantGroups` rule
///   -> group by (final label set, bucket_ns), summing `n`
///   -> apply_rate once per point, over the summed count
/// ```
///
/// **Fold first, then emit.** A grid point is emitted for a folded series
/// exactly where at least one of its contributing rows exists. Two
/// fingerprints whose final label sets are equal — one stream's metadata
/// value making its set equal to another stream's — are ONE series, and
/// that series has a point wherever either of them had a row. Emitting per
/// fingerprint and folding afterwards gives the same values at a different
/// set of points, which is a wrong answer at every gap.
///
/// **A grid point with no contributing row is not emitted at all** — not a
/// zero, not a NaN, no point. The statement returns no row for an empty
/// `(fingerprint, bucket_ns, metadata)` group, so this state's obligation
/// is not to fill; the reference deletes a series from its window map when
/// its sample list empties and emits one sample per series still in it
/// (`pkg/logql/range_vector.go` @ `v3.7.4`).
///
/// Summing `u64` partials is exact, which is what lets the four counting
/// reducers claim `Fidelity::Equivalent`: see
/// `super::compile::RangeAggLower::fidelity`.
///
/// **Each series is charged as the route the query took before charged it**
/// (issue #624, code review round 2):
///
/// - At a range unequal to the step (the sliding statement), one dense slot
///   vector per output series, one slot per grid point, `None` where no row
///   arrived. A series is charged ONCE, when it is created: its label bytes
///   against the label-byte cap, and its grid width against the result-point
///   cap — what today's sliding route reserves per series.
/// - At a range equal to the step (#507's statement, [`Self::with_sparse_points`]),
///   only the points that arrive, each charged against the label-byte cap as
///   it arrives — what this fold charged before. A reducing aggregation over
///   many sparse series then fits as it did, where a grid per series would
///   not.
pub(in crate::logql) struct PushdownRangeGroups {
    /// Each resolved stream's base label set, snapshotted ONCE — one
    /// fingerprint returns up to (grid points x metadata variants) rows.
    base_labels: HashMap<Fingerprint, LabelSet>,
    /// Rendered final label set -> `(labels, one slot per grid point)`.
    groups: HashMap<String, DenseSeries>,
    /// The emit grid's first point and the query's end. A row can arrive
    /// outside them and must not become a point — see [`Self::push_row`].
    grid_start_ns: i64,
    end_ns: i64,
    /// The grid's step and its point count — the width of every series'
    /// slot vector.
    step_ns: i64,
    grid_points: u64,
    /// Query-lifetime bytes, never discharged: the groups ARE the result
    /// (the `PushdownInstantGroups` precedent).
    charged: u64,
    /// Result points reserved: `grid_points` per dense series (issue #624).
    points: u64,
    /// Whether series keep their points sparsely (range equal to step).
    sparse: bool,
    caps: AggCaps,
    merge_buf: Vec<(String, String)>,
    sm_buf: Vec<(String, String)>,
    sm_ctx: StructuredMetadataCtx,
    /// A parent `sum`'s grouping (issue #507, `RangeStepRules::parent_sum`).
    parent_sum: Option<ParentSum>,
    /// The label stages of a staged plan, run once per returned group
    /// (issue #624, part 2). `None` on a clean plan.
    staged: Option<Box<StagedFold>>,
}

/// The staged mode of [`PushdownRangeGroups`] (issue #624, part 2).
///
/// **Today's route, once per group.** Every row of the statement is one
/// `(fingerprint, structured_metadata)` group at one grid point, and the
/// stages read only labels, so the pipeline today's route runs per line
/// gives every line of a group the same answer. It runs here once per
/// group — memoised for the last group, since the statement returns a
/// group's grid points together — with an empty line and timestamp
/// (`pipeline.rs`'s `admitted_stages_read_no_line_and_no_time`).
///
/// **Charged by today's route's rule** (`client_agg.rs`'s `row_route`): a
/// group whose final labels are its slider-safe fingerprint's own, in a
/// state that does not fan out, is a per-fingerprint series charged
/// `group_entry_bytes("", base, SERIES_OUT_SLOT)` and one grid; any other
/// is a per-label-set series charged `group_entry_bytes(key, labels,
/// MUT_GROUP_SLOT)` and one grid — the amounts today's route charges the
/// same series. A group no window holds never reaches this fold.
struct StagedFold {
    /// The plan's stages without its line filters, which the statement
    /// already applied to the rows it counted.
    compiled: CompiledPipeline,
    step: super::pipeline::RangeStepRules,
    fan_out: bool,
    slider_safe: HashSet<Fingerprint>,
    /// Fingerprint-routed series: no rendered key, the base labels.
    fp_series: HashMap<Fingerprint, Vec<Option<u64>>>,
    /// `absent_over_time`: its labels and which grid points a kept group
    /// covers. `None` for the counting reducers.
    absent: Option<(LabelSet, Vec<bool>)>,
    /// A `regexp` count's sent groups, `(name, 1-based index)` (issue #624,
    /// part 3a): the stage reads each row's `matched` and `caps` (D3).
    regexp: Option<Vec<(String, usize)>>,
    /// A `| json` count's key sources (issue #624, part 3b): each group's
    /// keys become the document its stages run over.
    json: Option<Vec<String>>,
    /// A `| logfmt` count (issue #624, part 3d): each group's `fields` are
    /// the line its stages run over.
    logfmt: bool,
    /// The last group's outcome, keyed by the whole group key.
    memo: Option<StagedMemo>,
}

/// [`StagedFold`]'s memo: the group key — `(fingerprint,
/// structured_metadata, matched, caps, keys, fields)` — and that group's
/// outcome.
type StagedMemo = (
    Fingerprint,
    String,
    u8,
    Vec<String>,
    Vec<(u8, String)>,
    String,
    StagedOutcome,
);

/// What the stages make of one group.
#[derive(Clone)]
enum StagedOutcome {
    /// A stage dropped it.
    Dropped,
    /// Kept, under `absent_over_time`: it covers its grid point.
    Present,
    /// Kept, and its fingerprint is its series.
    Fingerprint,
    /// Kept, under these final labels and their rendered key.
    Labels(String, LabelSet),
}

impl PushdownRangeGroups {
    pub(in crate::logql) fn new(
        meta: &HashMap<Fingerprint, StreamMetaRow>,
        caps: AggCaps,
        grid_start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        grid_points: u64,
    ) -> Self {
        PushdownRangeGroups {
            base_labels: meta.iter().map(|(fp, m)| (*fp, series_labels(m))).collect(),
            groups: HashMap::new(),
            grid_start_ns,
            end_ns,
            step_ns,
            grid_points,
            charged: 0,
            points: 0,
            sparse: false,
            caps,
            merge_buf: Vec::new(),
            sm_buf: Vec::new(),
            sm_ctx: StructuredMetadataCtx::default(),
            parent_sum: None,
            staged: None,
        }
    }

    /// Applies a parent `sum`'s grouping at the range step (issue #507).
    pub(in crate::logql) fn with_parent_sum(mut self, parent_sum: Option<ParentSum>) -> Self {
        self.parent_sum = parent_sum;
        self
    }

    /// Keeps each series' points sparsely and charges each as it arrives
    /// (issue #624, code review round 2) — for #507's statement, at a range
    /// equal to the step, where that is how the fold charged before.
    pub(in crate::logql) fn with_sparse_points(mut self) -> Self {
        self.sparse = true;
        self
    }

    /// Runs a staged plan's label stages once per returned group, and
    /// charges each output series as today's route charges it (issue #624,
    /// part 2) — see [`StagedFold`]. `compiled` is the plan's pipeline less
    /// its line filters; `todays_route` the aggregation today's route runs;
    /// `step` today's route's step rules for the plan.
    pub(in crate::logql) fn with_stages(
        mut self,
        compiled: CompiledPipeline,
        todays_route: &ClientAgg,
        step: super::pipeline::RangeStepRules,
        regexp: Option<Vec<(String, usize)>>,
        json: Option<Vec<String>>,
        logfmt: bool,
    ) -> Self {
        let fan_out = compiled.metric_mutates_labels() || todays_route.grouping.is_some();
        let width = usize::try_from(self.grid_points).unwrap_or(0);
        let absent = matches!(todays_route.range_op, RangeAggOp::AbsentOverTime)
            .then(|| (todays_route.absent_labels.clone(), vec![false; width]));
        self.staged = Some(Box::new(StagedFold {
            compiled,
            step,
            fan_out,
            slider_safe: super::client_agg::slider_safe_fingerprints(&self.base_labels),
            fp_series: HashMap::new(),
            absent,
            regexp,
            json,
            logfmt,
            memo: None,
        }));
        self
    }

    /// [`Self::finish`], with the one charge a staged `absent_over_time`
    /// makes when it emits — one grid, as today's route reserves it at
    /// finish (issue #624, part 2).
    pub(in crate::logql) fn finish_checked(
        mut self,
        rate_window_ns: Option<u64>,
    ) -> Result<Vec<MatrixSeries>, ReadError> {
        if self.staged.as_ref().is_some_and(|s| s.absent.is_some()) {
            charge_result_points(&mut self.points, self.grid_points, self.caps.result_points)?;
        }
        Ok(self.finish(rate_window_ns))
    }

    /// What the staged plan's stages make of `row`'s group, memoised for
    /// the last group (issue #624, part 2).
    fn staged_outcome(&mut self, row: &MetricRangeBucketRow) -> Result<StagedOutcome, ReadError> {
        let PushdownRangeGroups {
            base_labels,
            merge_buf,
            sm_buf,
            sm_ctx,
            staged,
            ..
        } = self;
        let staged = staged.as_mut().expect("the staged mode");
        if let Some((fp, sm, matched, caps, keys, fields, outcome)) = &staged.memo
            && *fp == row.fingerprint
            && *sm == row.structured_metadata
            && *matched == row.matched
            && *caps == row.caps
            && *keys == row.keys
            && *fields == row.fields
        {
            return Ok(outcome.clone());
        }
        let base = &base_labels[&row.fingerprint];
        let (pipeline_base, sm): (&[(String, String)], &StructuredMetadataCtx) =
            if row.structured_metadata.is_empty() {
                (base, &super::labels::EMPTY_STRUCTURED_METADATA)
            } else {
                merge_labels_with_structured_metadata(
                    base,
                    &row.structured_metadata,
                    merge_buf,
                    sm_buf,
                    sm_ctx,
                );
                (merge_buf, sm_ctx)
            };
        let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
        // Issue #624, part 3a (D3): the `regexp` stage takes the group's
        // captures; every line of the group has them.
        let captures = match &staged.regexp {
            Some(groups) => super::pipeline::RegexpCaptures::Group {
                matched: row.matched == 1,
                caps: &row.caps,
                groups,
            },
            None => super::pipeline::RegexpCaptures::None,
        };
        // Issue #624, part 3b: the group's key labels as the document its
        // `| json` stage reads; every line of the group gives that stage the
        // same labels.
        // Issue #624, part 3d: a `| logfmt` count's group runs its stages
        // over the group's relevant fields, which are a decided line.
        let document = match &staged.json {
            Some(sources) => std::borrow::Cow::Owned(json_group_document(sources, &row.keys)),
            None if staged.logfmt => std::borrow::Cow::Borrowed(row.fields.as_str()),
            None => std::borrow::Cow::Borrowed(""),
        };
        let run = staged.compiled.run_metric_step_into_with_captures(
            &document,
            pipeline_base,
            0,
            sm,
            None,
            &staged.step,
            captures,
            &mut scratch,
        )?;
        let outcome = match run {
            super::pipeline::MetricRun::Dropped => StagedOutcome::Dropped,
            super::pipeline::MetricRun::Kept { .. } => {
                check_surviving_error(&scratch)?;
                if staged.absent.is_some() {
                    StagedOutcome::Present
                } else {
                    let route = super::client_agg::row_route(
                        staged.fan_out,
                        staged.slider_safe.contains(&row.fingerprint),
                        &scratch,
                        base,
                    );
                    match route {
                        super::client_agg::RowRoute::Fingerprint => StagedOutcome::Fingerprint,
                        super::client_agg::RowRoute::Labels => {
                            scratch.sort_unstable();
                            let key = super::labels::render_labels_json_sorted(&scratch);
                            let labels: LabelSet = scratch
                                .iter()
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect();
                            StagedOutcome::Labels(key, labels)
                        }
                    }
                }
            }
        };
        staged.memo = Some((
            row.fingerprint,
            row.structured_metadata.clone(),
            row.matched,
            row.caps.clone(),
            row.keys.clone(),
            row.fields.clone(),
            outcome.clone(),
        ));
        Ok(outcome)
    }

    /// Folds one row of a staged plan (issue #624, part 2).
    fn push_staged(&mut self, row: &MetricRangeBucketRow, slot: usize) -> Result<(), ReadError> {
        if !self.base_labels.contains_key(&row.fingerprint) {
            return Ok(());
        }
        let outcome = self.staged_outcome(row)?;
        let width =
            usize::try_from(self.grid_points).map_err(|_| range_fold_off_grid(row.bucket_ns))?;
        let cell = match outcome {
            StagedOutcome::Dropped => return Ok(()),
            StagedOutcome::Present => {
                let staged = self.staged.as_mut().expect("the staged mode");
                let (_, present) = staged.absent.as_mut().expect("absent_over_time");
                let at = present
                    .get_mut(slot)
                    .ok_or_else(|| range_fold_off_grid(row.bucket_ns))?;
                *at = true;
                return Ok(());
            }
            StagedOutcome::Fingerprint => {
                let fresh = !self
                    .staged
                    .as_ref()
                    .expect("the staged mode")
                    .fp_series
                    .contains_key(&row.fingerprint);
                if fresh {
                    // Today's route's per-fingerprint charge, before the
                    // allocation, so a refused series is never retained.
                    charge_group_bytes(
                        &mut self.charged,
                        group_entry_bytes("", &self.base_labels[&row.fingerprint], SERIES_OUT_SLOT),
                        self.caps.group_bytes,
                    )?;
                    charge_result_points(
                        &mut self.points,
                        self.grid_points,
                        self.caps.result_points,
                    )?;
                }
                let staged = self.staged.as_mut().expect("the staged mode");
                staged
                    .fp_series
                    .entry(row.fingerprint)
                    .or_insert_with(|| vec![None; width])
                    .get_mut(slot)
            }
            StagedOutcome::Labels(key, labels) => {
                if !self.groups.contains_key(&key) {
                    // Today's route's per-label-set charge.
                    charge_group_bytes(
                        &mut self.charged,
                        group_entry_bytes(&key, &labels, MUT_GROUP_SLOT),
                        self.caps.group_bytes,
                    )?;
                    charge_result_points(
                        &mut self.points,
                        self.grid_points,
                        self.caps.result_points,
                    )?;
                }
                match &mut self
                    .groups
                    .entry(key)
                    .or_insert_with(|| (labels, SeriesPoints::Dense(vec![None; width])))
                    .1
                {
                    SeriesPoints::Dense(slots) => slots.get_mut(slot),
                    SeriesPoints::Sparse(_) => return Err(range_fold_off_grid(row.bucket_ns)),
                }
            }
        };
        let cell = cell.ok_or_else(|| range_fold_off_grid(row.bucket_ns))?;
        *cell = Some(cell.unwrap_or(0).saturating_add(row.n));
        Ok(())
    }

    /// The staged plan's series (issue #624, part 2): the fingerprint- and
    /// label-routed series in label order, as today's route's finish puts
    /// them, or `absent_over_time`'s one series of 1s at every grid point no
    /// kept group covers.
    fn finish_staged(self, staged: StagedFold, rate_window_ns: Option<u64>) -> Vec<MatrixSeries> {
        let (grid_start_ns, step_ns) = (self.grid_start_ns, self.step_ns);
        let at = |k: usize| grid_start_ns.saturating_add((k as i64).saturating_mul(step_ns));
        if let Some((labels, present)) = staged.absent {
            let points: Vec<(i64, f64)> = present
                .iter()
                .enumerate()
                .filter(|(_, covered)| !**covered)
                .map(|(k, _)| (at(k), 1.0))
                .collect();
            return if points.is_empty() {
                Vec::new()
            } else {
                vec![MatrixSeries { labels, points }]
            };
        }
        let mut series: Vec<(LabelSet, Vec<Option<u64>>)> = staged
            .fp_series
            .into_iter()
            .map(|(fp, slots)| (self.base_labels[&fp].clone(), slots))
            .collect();
        series.extend(
            self.groups
                .into_values()
                .filter_map(|(labels, points)| match points {
                    SeriesPoints::Dense(slots) => Some((labels, slots)),
                    SeriesPoints::Sparse(_) => None,
                }),
        );
        series.sort_by(|a, b| a.0.cmp(&b.0));
        series
            .into_iter()
            .map(|(labels, slots)| MatrixSeries {
                labels,
                points: slots
                    .into_iter()
                    .enumerate()
                    .filter_map(|(k, n)| Some((at(k), apply_rate(n? as f64, rate_window_ns))))
                    .collect(),
            })
            .collect()
    }

    /// Folds one returned row. A row whose fingerprint did not hydrate is
    /// skipped, as on the instant path.
    ///
    /// **Rows outside the emit grid are dropped here, and they are rows
    /// the reference never counts.** The scan is bounded by the query's
    /// `end`, which need not sit on the grid; a row in
    /// `(last grid point, end]` buckets to `last + step`, a point the
    /// reference does not evaluate, so its rows are in no window:
    ///
    /// ```text
    /// grid   ... g_last |          end
    /// rows              |  x  x    |   <- bucket to g_last + step, no window
    /// ```
    ///
    /// The lower test is the mirror image, and reachable only when the
    /// scan's lower bound is INCLUSIVE (`widen_scan_start` saturated at
    /// `i64::MIN`): a row at exactly `grid_start - step` buckets below the
    /// grid's first point, where the first window `(grid_start - range,
    /// grid_start]` is open below and excludes it.
    pub(in crate::logql) fn push_row(
        &mut self,
        row: &MetricRangeBucketRow,
    ) -> Result<(), ReadError> {
        if row.bucket_ns < self.grid_start_ns || row.bucket_ns > self.end_ns {
            return Ok(());
        }
        if self.staged.is_some() {
            let slot = self.slot_of(row.bucket_ns)?;
            return self.push_staged(row, slot);
        }
        let Some(base) = self.base_labels.get(&row.fingerprint) else {
            return Ok(());
        };
        // No pipeline runs on this path by construction (`client == None`
        // and every lowered stage is a pushed line filter), so the merge
        // IS the whole label computation — the instant path's derivation,
        // and the same `visible()` rule for the out-of-band error slots.
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
        // A surviving `__error__` fails the whole query, exactly as on the
        // instant pushdown path.
        check_surviving_error(&labels)?;
        let key = render_series_labels(&labels);
        let slot = self.slot_of(row.bucket_ns)?;
        if self.sparse {
            return self.push_sparse(key, labels, row);
        }
        let slots = match self.groups.entry(key) {
            std::collections::hash_map::Entry::Occupied(e) => match &mut e.into_mut().1 {
                SeriesPoints::Dense(slots) => slots,
                SeriesPoints::Sparse(_) => return Err(range_fold_off_grid(row.bucket_ns)),
            },
            std::collections::hash_map::Entry::Vacant(e) => {
                // Both charges before the allocation, so a refused series
                // is never retained.
                charge_group_bytes(
                    &mut self.charged,
                    group_entry_bytes(e.key(), &labels, PUSHDOWN_RANGE_DENSE_SLOT),
                    self.caps.group_bytes,
                )?;
                charge_result_points(&mut self.points, self.grid_points, self.caps.result_points)?;
                let width = usize::try_from(self.grid_points)
                    .map_err(|_| range_fold_off_grid(row.bucket_ns))?;
                match &mut e.insert((labels, SeriesPoints::Dense(vec![None; width]))).1 {
                    SeriesPoints::Dense(slots) => slots,
                    SeriesPoints::Sparse(_) => return Err(range_fold_off_grid(row.bucket_ns)),
                }
            }
        };
        let cell = slots
            .get_mut(slot)
            .ok_or_else(|| range_fold_off_grid(row.bucket_ns))?;
        *cell = Some(cell.unwrap_or(0).saturating_add(row.n));
        Ok(())
    }

    /// The sparse fold (issue #507): a series is charged its entry when it is
    /// created, and each grid point its map entry when it first arrives,
    /// both against the label-byte cap and before the insertion.
    fn push_sparse(
        &mut self,
        key: String,
        labels: LabelSet,
        row: &MetricRangeBucketRow,
    ) -> Result<(), ReadError> {
        match self.groups.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let SeriesPoints::Sparse(points) = &mut e.get_mut().1 else {
                    return Err(range_fold_off_grid(row.bucket_ns));
                };
                match points.entry(row.bucket_ns) {
                    std::collections::hash_map::Entry::Occupied(mut p) => {
                        *p.get_mut() = p.get().saturating_add(row.n);
                    }
                    std::collections::hash_map::Entry::Vacant(p) => {
                        charge_group_bytes(
                            &mut self.charged,
                            map_entry_bytes(PUSHDOWN_RANGE_POINT_SLOT),
                            self.caps.group_bytes,
                        )?;
                        p.insert(row.n);
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                let cost = group_entry_bytes(e.key(), &labels, PUSHDOWN_RANGE_SLOT)
                    .saturating_add(map_entry_bytes(PUSHDOWN_RANGE_POINT_SLOT));
                charge_group_bytes(&mut self.charged, cost, self.caps.group_bytes)?;
                let mut points = HashMap::new();
                points.insert(row.bucket_ns, row.n);
                e.insert((labels, SeriesPoints::Sparse(points)));
            }
        }
        Ok(())
    }

    /// The slot a row inside `[grid_start, end]` folds into. A row there
    /// that is not on the grid is an internal defect of the statement, never
    /// a point.
    fn slot_of(&self, bucket_ns: i64) -> Result<usize, ReadError> {
        let offset = i128::from(bucket_ns) - i128::from(self.grid_start_ns);
        let step = i128::from(self.step_ns);
        if step <= 0 || offset % step != 0 {
            return Err(range_fold_off_grid(bucket_ns));
        }
        usize::try_from(offset / step).map_err(|_| range_fold_off_grid(bucket_ns))
    }

    /// How many bytes this state has charged.
    #[cfg(test)]
    pub(in crate::logql) fn charged_bytes(&self) -> u64 {
        self.charged
    }

    /// How many result points this state has reserved (issue #624).
    #[cfg(test)]
    pub(in crate::logql) fn charged_points(&self) -> u64 {
        self.points
    }

    /// The rate divisor is applied ONCE per point, to the summed count —
    /// one division of an exact integer sum, never a sum of divided
    /// values. Emitted in rendered-label order, and each series' points
    /// ascending by grid point, so the value a downstream `sum`
    /// accumulates is reproducible run to run (a `HashMap` drain is not).
    pub(in crate::logql) fn finish(mut self, rate_window_ns: Option<u64>) -> Vec<MatrixSeries> {
        if let Some(staged) = self.staged.take() {
            return self.finish_staged(*staged, rate_window_ns);
        }
        let (grid_start_ns, step_ns) = (self.grid_start_ns, self.step_ns);
        let mut out: Vec<(String, DenseSeries)> = self.groups.into_iter().collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        // Each series' slots are consumed by its own iteration, so they are
        // freed before the next series' points are built.
        out.into_iter()
            .map(|(_, (labels, points))| {
                let points = match points {
                    SeriesPoints::Dense(slots) => slots
                        .into_iter()
                        .enumerate()
                        .filter_map(|(k, n)| {
                            let n = n?;
                            let ts =
                                grid_start_ns.saturating_add((k as i64).saturating_mul(step_ns));
                            Some((ts, apply_rate(n as f64, rate_window_ns)))
                        })
                        .collect(),
                    SeriesPoints::Sparse(points) => {
                        let mut points: Vec<(i64, f64)> = points
                            .into_iter()
                            .map(|(ts, n)| (ts, apply_rate(n as f64, rate_window_ns)))
                            .collect();
                        points.sort_by_key(|(ts, _)| *ts);
                        points
                    }
                };
                MatrixSeries { labels, points }
            })
            .collect()
    }
}

/// The internal-invariant breach the bucketed fold reports for a row inside
/// the emit grid's span but off its points (issue #624), as `fold.rs` does
/// for its own grid.
pub(in crate::logql) fn range_fold_off_grid(t: i64) -> ReadError {
    ReadError::PipelineInvalid {
        reason: format!(
            "internal: the bucketed range fold received a row at {t} off the query grid"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::super::charge::{AggCaps, group_entry_bytes};
    use super::*;

    // -----------------------------------------------------------------
    // Issue #507, W2 — the bucketed range read's fold.
    // -----------------------------------------------------------------

    /// Two streams, and the SECOND one's base label set is what the
    /// FIRST one's rows fold into once their metadata is merged:
    ///
    /// ```text
    /// fp 10   {app=x,            service_name=r}   + metadata lvl=info
    /// fp 11   {app=x, lvl=info,  service_name=r}
    /// ```
    ///
    /// So a row of fp 10 carrying `lvl=info` and a row of fp 11 carrying
    /// none are the SAME output series — which is the only shape that can
    /// tell fold-then-emit from emit-then-fold.
    fn range_meta() -> HashMap<Fingerprint, StreamMetaRow> {
        let mut m = HashMap::new();
        m.insert(
            Fingerprint::from_raw(10),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(10),
                service: "r".to_string(),
                labels: r#"{"app":"x","service_name":"r"}"#.to_string(),
            },
        );
        m.insert(
            Fingerprint::from_raw(11),
            StreamMetaRow {
                fingerprint: Fingerprint::from_raw(11),
                service: "r".to_string(),
                labels: r#"{"app":"x","lvl":"info","service_name":"r"}"#.to_string(),
            },
        );
        m
    }

    const RANGE_GRID_START_NS: i64 = 60_000_000_000;
    const RANGE_END_NS: i64 = 300_000_000_000;
    const RANGE_STEP_NS: i64 = 60_000_000_000;

    fn bucket_row(fp: Fingerprint, bucket_ns: i64, n: u64, sm: &str) -> MetricRangeBucketRow {
        MetricRangeBucketRow {
            fingerprint: fp,
            bucket_ns,
            n,
            structured_metadata: sm.to_string(),
            matched: 0,
            caps: Vec::new(),
            keys: Vec::new(),
            fields: String::new(),
        }
    }

    /// One emitted series: its sorted labels, and its points as
    /// `(grid point, value bits)` so a float comparison is never a float
    /// comparison.
    type RangeSeries = (Vec<(String, String)>, Vec<(i64, u64)>);

    fn range_series(
        rows: &[MetricRangeBucketRow],
        rate_window_ns: Option<u64>,
    ) -> Vec<RangeSeries> {
        let meta = range_meta();
        let mut g = PushdownRangeGroups::new(
            &meta,
            AggCaps::DEFAULT,
            RANGE_GRID_START_NS,
            RANGE_END_NS,
            RANGE_STEP_NS,
            5,
        );
        for r in rows {
            g.push_row(r).expect("under the cap");
        }
        g.finish(rate_window_ns)
            .into_iter()
            .map(|s| {
                let mut l = s.labels;
                l.sort();
                (
                    l,
                    s.points
                        .into_iter()
                        .map(|(t, v)| (t, v.to_bits()))
                        .collect(),
                )
            })
            .collect()
    }

    /// **The fold happens before the emission, and the gap is the
    /// evidence.**
    ///
    /// Two fingerprints fold into one series. Only fp 11 has a row at the
    /// second grid point and only fp 10 has one at the first and third, so
    /// the folded series carries a point wherever EITHER contributed —
    /// and no point at all at the two grid points where neither did.
    ///
    /// ```text
    /// grid           60    120   180   240   300
    /// fp 10 rows      2      -     1     -     -
    /// fp 11 rows      -      3     -     -     -
    /// folded series   2      3     1    (none)(none)
    /// ```
    ///
    /// Emitting per fingerprint and folding afterwards would answer the
    /// same three values; it is the ABSENT points this pins.
    #[test]
    fn a_folded_series_has_a_point_wherever_either_fingerprint_had_a_row() {
        let series = range_series(
            &[
                bucket_row(
                    Fingerprint::from_raw(10),
                    60_000_000_000,
                    2,
                    r#"{"lvl":"info"}"#,
                ),
                bucket_row(Fingerprint::from_raw(11), 120_000_000_000, 3, ""),
                bucket_row(
                    Fingerprint::from_raw(10),
                    180_000_000_000,
                    1,
                    r#"{"lvl":"info"}"#,
                ),
            ],
            None,
        );
        assert_eq!(series.len(), 1, "one folded series: {series:?}");
        assert_eq!(
            series[0].0,
            vec![
                ("app".to_string(), "x".to_string()),
                ("lvl".to_string(), "info".to_string()),
                ("service_name".to_string(), "r".to_string()),
            ]
        );
        assert_eq!(
            series[0].1,
            vec![
                (60_000_000_000, 2.0f64.to_bits()),
                (120_000_000_000, 3.0f64.to_bits()),
                (180_000_000_000, 1.0f64.to_bits()),
            ],
            "a grid point with no contributing row is not emitted at all"
        );
    }

    /// Two metadata variants at one grid point are one point of the summed
    /// count — the addition that makes `Fidelity::Equivalent` true.
    #[test]
    fn two_metadata_variants_at_one_grid_point_sum_into_one_point() {
        let series = range_series(
            &[
                bucket_row(
                    Fingerprint::from_raw(10),
                    60_000_000_000,
                    2,
                    r#"{"lvl":"info"}"#,
                ),
                bucket_row(Fingerprint::from_raw(11), 60_000_000_000, 5, ""),
                // A THIRD variant that is a different series, so the sum
                // above is not merely "everything at this grid point".
                bucket_row(
                    Fingerprint::from_raw(10),
                    60_000_000_000,
                    4,
                    r#"{"lvl":"warn"}"#,
                ),
            ],
            None,
        );
        assert_eq!(series.len(), 2, "{series:?}");
        let info = series
            .iter()
            .find(|(l, _)| l.contains(&("lvl".to_string(), "info".to_string())))
            .expect("the info series");
        assert_eq!(info.1, vec![(60_000_000_000, 7.0f64.to_bits())]);
        let warn = series
            .iter()
            .find(|(l, _)| l.contains(&("lvl".to_string(), "warn".to_string())))
            .expect("the warn series");
        assert_eq!(warn.1, vec![(60_000_000_000, 4.0f64.to_bits())]);
    }

    /// A row whose grid point is outside the emit grid is dropped, and it
    /// is a row the evaluator never counts: the scan runs to the query's
    /// `end`, which need not sit on the grid, so rows after the last grid
    /// point bucket one step PAST it, where no window is evaluated.
    #[test]
    fn a_row_bucketed_outside_the_emit_grid_is_not_a_point() {
        let series = range_series(
            &[
                bucket_row(
                    Fingerprint::from_raw(10),
                    RANGE_GRID_START_NS - RANGE_STEP_NS,
                    9,
                    "",
                ),
                bucket_row(Fingerprint::from_raw(10), 60_000_000_000, 2, ""),
                bucket_row(
                    Fingerprint::from_raw(10),
                    RANGE_END_NS + RANGE_STEP_NS,
                    9,
                    "",
                ),
            ],
            None,
        );
        assert_eq!(series.len(), 1, "{series:?}");
        assert_eq!(
            series[0].1,
            vec![(60_000_000_000, 2.0f64.to_bits())],
            "only the in-grid row becomes a point"
        );
    }

    /// The rate divisor is applied ONCE per point, to the summed count —
    /// `(2 + 5) / 60`, never `2/60 + 5/60`. They agree here to the bit,
    /// and the point is that the code divides once: the `Equivalent`
    /// claim is about the order, and this is the order.
    #[test]
    fn the_rate_divisor_is_applied_to_the_summed_count() {
        let series = range_series(
            &[
                bucket_row(
                    Fingerprint::from_raw(10),
                    60_000_000_000,
                    2,
                    r#"{"lvl":"info"}"#,
                ),
                bucket_row(Fingerprint::from_raw(11), 60_000_000_000, 5, ""),
            ],
            Some(60_000_000_000),
        );
        assert_eq!(series.len(), 1);
        assert_eq!(
            series[0].1,
            vec![(60_000_000_000, (7.0f64 / 60.0).to_bits())]
        );
    }

    /// A surviving `__error__` in the metadata fails the bucketed query
    /// too, with the same named error the instant pushdown and the client
    /// paths raise.
    #[test]
    fn a_metadata_error_fails_the_bucketed_query_too() {
        let meta = range_meta();
        let mut g = PushdownRangeGroups::new(
            &meta,
            AggCaps::DEFAULT,
            RANGE_GRID_START_NS,
            RANGE_END_NS,
            RANGE_STEP_NS,
            5,
        );
        let err = g
            .push_row(&bucket_row(
                Fingerprint::from_raw(10),
                60_000_000_000,
                1,
                r#"{"__error__":"boom"}"#,
            ))
            .expect_err("a surviving __error__ fails the query");
        assert!(
            matches!(err, ReadError::MetricPipelineError { ref error_type, .. } if error_type == "boom"),
            "{err:?}"
        );
    }

    /// Issue #624: a grid of 61 points from 60 s, one step 60 s.
    const DENSE_POINTS: u64 = 61;
    const DENSE_END_NS: i64 = RANGE_GRID_START_NS + 60 * RANGE_STEP_NS;

    /// The `group_entry_bytes` one series of fp 11 with no metadata costs
    /// on the dense fold.
    fn dense_series_bytes() -> u64 {
        let meta = range_meta();
        let labels = series_labels(&meta[&Fingerprint::from_raw(11)]);
        group_entry_bytes(
            &render_series_labels(&labels),
            &labels,
            super::super::charge::PUSHDOWN_RANGE_DENSE_SLOT,
        )
    }

    /// N5 (issue #624): a series' points are charged once, against the
    /// result-point cap, and never against the label-byte cap. With the
    /// label-byte cap set to exactly one series' entry, a series with a
    /// point at every one of the 61 grid points folds.
    #[test]
    fn a_series_points_are_charged_once_against_the_result_point_cap() {
        let meta = range_meta();
        let caps = AggCaps {
            group_bytes: dense_series_bytes(),
            ..AggCaps::DEFAULT
        };
        let mut g = PushdownRangeGroups::new(
            &meta,
            caps,
            RANGE_GRID_START_NS,
            DENSE_END_NS,
            RANGE_STEP_NS,
            DENSE_POINTS,
        );
        for k in 0..DENSE_POINTS as i64 {
            g.push_row(&bucket_row(
                Fingerprint::from_raw(11),
                RANGE_GRID_START_NS + k * RANGE_STEP_NS,
                1,
                "",
            ))
            .unwrap_or_else(|e| panic!("row {k} must fold: {e:?}"));
        }
        assert_eq!(g.charged_points(), DENSE_POINTS);
        let series = g.finish(None);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].points.len(), DENSE_POINTS as usize);
    }

    /// N6 (issue #624): a second series past the result-point cap is the
    /// named refusal, and nothing of it is retained.
    #[test]
    fn a_series_past_the_result_point_cap_is_refused() {
        let meta = range_meta();
        let caps = AggCaps {
            result_points: DENSE_POINTS,
            ..AggCaps::DEFAULT
        };
        let mut g = PushdownRangeGroups::new(
            &meta,
            caps,
            RANGE_GRID_START_NS,
            DENSE_END_NS,
            RANGE_STEP_NS,
            DENSE_POINTS,
        );
        g.push_row(&bucket_row(
            Fingerprint::from_raw(11),
            RANGE_GRID_START_NS,
            1,
            "",
        ))
        .expect("the first series fits");
        let err = g
            .push_row(&bucket_row(
                Fingerprint::from_raw(10),
                RANGE_GRID_START_NS,
                1,
                r#"{"lvl":"warn"}"#,
            ))
            .expect_err("the second series is past the cap");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(TooBroadReason::MetricResultPoints { .. })
            ),
            "got {err:?}"
        );
        assert_eq!(
            g.finish(None).len(),
            1,
            "the refused series is not retained"
        );
    }

    /// Issue #624, code review round 2: when the range equals the step the
    /// fold keeps each series' points sparsely and charges them one by one,
    /// as the bucketed fold did before (#507). 1,091 one-point series on an
    /// 11,001-point grid are 1,091 points; reserving a grid per series would
    /// be 12,002,091 slots, past `MAX_METRIC_RESULT_POINTS`.
    #[test]
    fn sparse_points_are_charged_per_point_not_per_grid() {
        const POINTS: u64 = 11_001;
        let series = super::super::charge::MAX_METRIC_RESULT_POINTS / POINTS + 1;
        let mut meta = HashMap::new();
        for i in 0..series {
            let fp = Fingerprint::from_raw(u128::from(1_000 + i));
            meta.insert(
                fp,
                StreamMetaRow {
                    fingerprint: fp,
                    service: "r".to_string(),
                    labels: format!(r#"{{"pod":"p{i}","service_name":"r"}}"#),
                },
            );
        }
        let end = RANGE_GRID_START_NS + (POINTS as i64 - 1) * RANGE_STEP_NS;
        let mut g = PushdownRangeGroups::new(
            &meta,
            AggCaps::DEFAULT,
            RANGE_GRID_START_NS,
            end,
            RANGE_STEP_NS,
            POINTS,
        )
        .with_sparse_points();
        for i in 0..series {
            g.push_row(&bucket_row(
                Fingerprint::from_raw(u128::from(1_000 + i)),
                RANGE_GRID_START_NS + 100 * RANGE_STEP_NS,
                1,
                "",
            ))
            .unwrap_or_else(|e| panic!("series {i} must fold: {e:?}"));
        }
        let out = g.finish(None);
        assert_eq!(out.len() as u64, series);
        assert!(out.iter().all(|s| s.points.len() == 1));
    }

    /// T7 (issue #624, part 2): the staged fold charges each output series
    /// what today's route charges it, by the route the row takes there.
    #[test]
    fn staged_fold_charges_by_route() {
        use super::super::charge::{MUT_GROUP_SLOT, SERIES_OUT_SLOT};
        use super::super::labels::render_labels_json_sorted;
        use super::super::pipeline::RangeStepRules;
        use super::super::plan::{ClientAgg, ClientValue};

        fn pipeline_of(query: &str) -> Vec<pulsus_logql::Stage> {
            match &pulsus_logql::parse(query).expect("parse") {
                pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) => {
                    range.selector.pipeline.clone()
                }
                other => panic!("expected a range aggregation, got {other:?}"),
            }
        }
        fn todays(op: RangeAggOp, query: &str, absent: Vec<(String, String)>) -> ClientAgg {
            ClientAgg {
                pipeline: pipeline_of(query),
                value: ClientValue::Count,
                range_op: op,
                param: None,
                absent_labels: absent,
                grouping: None,
            }
        }
        fn staged(
            meta: &HashMap<Fingerprint, StreamMetaRow>,
            c: &ClientAgg,
            caps: AggCaps,
        ) -> PushdownRangeGroups {
            PushdownRangeGroups::new(
                meta,
                caps,
                RANGE_GRID_START_NS,
                DENSE_END_NS,
                RANGE_STEP_NS,
                DENSE_POINTS,
            )
            .with_stages(
                CompiledPipeline::compile(&c.pipeline).expect("compiles"),
                c,
                RangeStepRules::PLAIN,
                None,
                None,
                false,
            )
        }
        let pairs = |p: &[(&str, &str)]| -> Vec<(String, String)> {
            p.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let meta = range_meta();
        let base10 = series_labels(&meta[&Fingerprint::from_raw(10)]);

        // (a) A stage that changes no label, over a slider-safe fingerprint
        // with no metadata: the fingerprint route.
        let filter = todays(
            RangeAggOp::CountOverTime,
            r#"count_over_time({a="b"} | app="x" [1m])"#,
            vec![],
        );
        let mut g = staged(&meta, &filter, AggCaps::DEFAULT);
        g.push_row(&bucket_row(
            Fingerprint::from_raw(10),
            RANGE_GRID_START_NS,
            1,
            "",
        ))
        .expect("folds");
        assert_eq!(
            g.charged_bytes(),
            group_entry_bytes("", &base10, SERIES_OUT_SLOT),
            "(a)"
        );
        assert_eq!(g.charged_points(), DENSE_POINTS, "(a)");

        // (b) The same stage over a row with metadata: the label route.
        let mut g = staged(&meta, &filter, AggCaps::DEFAULT);
        g.push_row(&bucket_row(
            Fingerprint::from_raw(10),
            RANGE_GRID_START_NS,
            1,
            r#"{"lvl":"info"}"#,
        ))
        .expect("folds");
        let labels = pairs(&[("app", "x"), ("lvl", "info"), ("service_name", "r")]);
        assert_eq!(
            g.charged_bytes(),
            group_entry_bytes(&render_labels_json_sorted(&labels), &labels, MUT_GROUP_SLOT),
            "(b)"
        );
        assert_eq!(g.charged_points(), DENSE_POINTS, "(b)");

        // (c) `drop` merges two fingerprints into one series: charged once.
        let drop = todays(
            RangeAggOp::CountOverTime,
            r#"count_over_time({a="b"} | drop lvl [1m])"#,
            vec![],
        );
        let mut g = staged(&meta, &drop, AggCaps::DEFAULT);
        g.push_row(&bucket_row(
            Fingerprint::from_raw(10),
            RANGE_GRID_START_NS,
            1,
            r#"{"lvl":"info"}"#,
        ))
        .expect("folds");
        g.push_row(&bucket_row(
            Fingerprint::from_raw(11),
            RANGE_GRID_START_NS,
            2,
            "",
        ))
        .expect("folds");
        let merged = pairs(&[("app", "x"), ("service_name", "r")]);
        assert_eq!(
            g.charged_bytes(),
            group_entry_bytes(&render_labels_json_sorted(&merged), &merged, MUT_GROUP_SLOT),
            "(c)"
        );
        assert_eq!(g.charged_points(), DENSE_POINTS, "(c)");
        let series = g.finish_checked(None).expect("answers");
        assert_eq!(series.len(), 1, "(c)");
        assert_eq!(series[0].points, vec![(RANGE_GRID_START_NS, 3.0)], "(c)");

        // (d) `absent_over_time` charges one grid, once, when it emits.
        let absent = todays(
            RangeAggOp::AbsentOverTime,
            r#"absent_over_time({a="b"}[1m])"#,
            pairs(&[("app", "x")]),
        );
        let caps = |points: u64| AggCaps {
            result_points: points,
            ..AggCaps::DEFAULT
        };
        let mut g = staged(&meta, &absent, caps(DENSE_POINTS - 1));
        g.push_row(&bucket_row(
            Fingerprint::from_raw(10),
            RANGE_GRID_START_NS,
            1,
            "",
        ))
        .expect("folds");
        assert!(
            matches!(
                g.finish_checked(None),
                Err(ReadError::QueryTooBroad(
                    TooBroadReason::MetricResultPoints { .. }
                ))
            ),
            "(d): one grid is past a cap one point short of it"
        );
        let mut g = staged(&meta, &absent, caps(DENSE_POINTS));
        g.push_row(&bucket_row(
            Fingerprint::from_raw(10),
            RANGE_GRID_START_NS,
            1,
            "",
        ))
        .expect("folds");
        g.push_row(&bucket_row(
            Fingerprint::from_raw(11),
            RANGE_GRID_START_NS + RANGE_STEP_NS,
            1,
            "",
        ))
        .expect("folds");
        let series = g.finish_checked(None).expect("one grid fits");
        assert_eq!(series.len(), 1, "(d)");
        assert_eq!(series[0].labels, pairs(&[("app", "x")]), "(d)");
        assert_eq!(series[0].points.len() as u64, DENSE_POINTS - 2, "(d)");
        assert!(
            series[0].points.iter().all(|(t, v)| *v == 1.0
                && *t != RANGE_GRID_START_NS
                && *t != RANGE_GRID_START_NS + RANGE_STEP_NS),
            "(d): 1 at every point no row covers"
        );
    }

    /// N7 (issue #624) and T6 (part 3b): the server's memory code and the
    /// `| json` count's `throwIf` code hand a bucketed statement's failure
    /// to today's route; nothing else does.
    #[test]
    fn the_memory_and_throw_codes_hand_a_bucketed_statement_to_todays_route() {
        let server = |code: i32| ChError::Server {
            code,
            message: format!("Code: {code}. DB::Exception: x"),
        };
        for (e, want) in [
            (server(241), true),
            (server(307), false),
            (server(159), false),
            (server(395), true),
            (ChError::Decode("bad row".to_string()), false),
        ] {
            assert_eq!(
                super::bucketed_statement_failure_goes_to_todays_route(&e),
                want,
                "{e:?}"
            );
        }
    }

    fn dense_groups(meta: &HashMap<Fingerprint, StreamMetaRow>) -> PushdownRangeGroups {
        PushdownRangeGroups::new(
            meta,
            AggCaps::DEFAULT,
            RANGE_GRID_START_NS,
            DENSE_END_NS,
            RANGE_STEP_NS,
            DENSE_POINTS,
        )
    }

    /// N13 (issue #624): how a bucketed statement's row stream ends.
    #[tokio::test]
    async fn a_bucketed_statement_failure_after_a_row_is_classified_by_its_code() {
        let meta = range_meta();
        let row = |k: i64| {
            Ok(bucket_row(
                Fingerprint::from_raw(11),
                RANGE_GRID_START_NS + k * RANGE_STEP_NS,
                1,
                "",
            ))
        };
        let server = |code: i32| {
            Err(ChError::Server {
                code,
                message: format!("Code: {code}. DB::Exception: x"),
            })
        };
        // (a) a row, then the memory code: today's route, with no fold.
        let got = super::fold_bucketed_rows(
            futures::stream::iter(vec![row(0), server(241)]),
            dense_groups(&meta),
        )
        .await
        .expect("no fold refusal");
        assert!(matches!(got, BucketedRows::TodaysRoute), "(a)");
        // (b) a row, then the scan budget: today's error mapping.
        let got = super::fold_bucketed_rows(
            futures::stream::iter(vec![row(0), server(307)]),
            dense_groups(&meta),
        )
        .await
        .expect("no fold refusal");
        assert!(
            matches!(got, BucketedRows::Failed(ChError::Server { code: 307, .. })),
            "(b)"
        );
        // (c) two rows: both folded.
        let got = super::fold_bucketed_rows(
            futures::stream::iter(vec![row(0), row(1)]),
            dense_groups(&meta),
        )
        .await
        .expect("no fold refusal");
        let BucketedRows::Folded(groups) = got else {
            panic!("(c): expected the folded rows");
        };
        let series = groups.finish(None);
        assert_eq!(series.len(), 1, "(c)");
        assert_eq!(
            series[0].points,
            vec![
                (RANGE_GRID_START_NS, 1.0),
                (RANGE_GRID_START_NS + RANGE_STEP_NS, 1.0)
            ],
            "(c)"
        );
    }

    /// The bucketed fold is bounded BEFORE it allocates, with the client
    /// paths' named 422 — the `PushdownInstantGroups` arrangement, and
    /// nothing is retained when the charge refuses.
    #[test]
    fn the_bucketed_regrouping_refuses_before_it_retains_the_breaching_group() {
        let meta = range_meta();
        let tiny = AggCaps {
            group_bytes: 4096,
            ..AggCaps::DEFAULT
        };
        let mut g = PushdownRangeGroups::new(
            &meta,
            tiny,
            RANGE_GRID_START_NS,
            RANGE_END_NS,
            RANGE_STEP_NS,
            5,
        );
        let fat = "v".repeat(64 * 1024);
        let err = g
            .push_row(&bucket_row(
                Fingerprint::from_raw(10),
                60_000_000_000,
                1,
                &format!(r#"{{"big":"{fat}"}}"#),
            ))
            .expect_err("the group must be refused");
        assert!(
            matches!(
                err,
                ReadError::QueryTooBroad(TooBroadReason::MetricGroupLabelBytes { .. })
            ),
            "got {err:?}"
        );
        assert_eq!(g.charged_bytes(), 0, "a refused charge is not accumulated");
        assert!(
            g.finish(None).is_empty(),
            "and the output vector never held it"
        );
    }
}
