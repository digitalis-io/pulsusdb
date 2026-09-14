//! The extracted-field group key read's reader half (issue #507): the
//! classes and blanked key labels a resolved stream set gives
//! [`super::sql::metric_range_unwrapped`], the group document each returned
//! group is run as, and the fold that merges S1's groups and L's rows into
//! the answer.
//!
//! ```text
//! S1 row  (class, grid point, keys, metadata, v, n_value, n_missing, n_undecided)
//!   -> labels  = the class's stream labels merged with the row's metadata
//!   -> document {"<key source>":"<text>", …, <unwrap path>: 0}
//!   -> our pipeline over the document, under the query's grouping and rules
//!        kept, no error, value 0   -> partial (labels, grid point) += (v, n_value)
//!        dropped                    -> nothing
//!        anything else              -> today's route
//! L row, undecided -> our pipeline over the body with the stream's labels and
//!                     the stored metadata: kept -> += (value, 1); an error is
//!                     the client path's pipeline error
//! ```
//!
//! The document holds exactly the keys the answer can depend on: a decided
//! row's value is the database's reading of the token our parser reads, each
//! key label's text is the one our parser renders, and a blanked key is one
//! our parser renames out of the answer. So running our own pipeline over it
//! gives our label names, collision renames, filter outcomes and grouping
//! (docs/query-to-sql.md, the extracted-field group key).

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use super::agg::LabelSet;
use super::charge::{
    AggCaps, PUSHDOWN_RANGE_POINT_SLOT, PUSHDOWN_RANGE_SLOT, charge_group_bytes, group_entry_bytes,
    map_entry_bytes,
};
use super::client_agg::check_surviving_error;
use super::error::ReadError;
use super::labels::{
    EMPTY_STRUCTURED_METADATA, StructuredMetadataCtx, merge_labels_with_structured_metadata,
    merge_labels_with_structured_metadata_pairs, push_json_string, render_series_labels,
    series_labels,
};
use super::pipeline::{CompiledPipeline, ERROR_LABEL, MetricRun};
use super::rows::{MetricRangeUnwrappedRow, StreamMetaRow, UnwrappedLaneRow};
use super::sql::{
    ClassNames, GroupKeyColumns, MetadataSent, UnwrapForm, UnwrapKeyLabel, UnwrapReducer,
    UnwrappedValue,
};

/// One answer series as the fold emits it: sorted labels, ascending points.
pub(in crate::logql) type FoldedSeries = (LabelSet, Vec<(i64, f64)>);

/// A resolved stream set's key columns and class labels (issue #507).
#[derive(Debug)]
pub(in crate::logql) struct ResolvedGroupKey {
    pub columns: GroupKeyColumns,
    /// Class id -> the stream labels every fingerprint of that class shares
    /// on the names that can reach the answer.
    pub class_labels: HashMap<u64, LabelSet>,
    /// Fingerprint -> its full stream labels, for L's undecided rows.
    pub stream_labels: HashMap<u64, LabelSet>,
    /// The hydrated fingerprints, ascending: the statements read these only,
    /// so a row's class is always one of `class_labels`.
    pub fingerprints: Vec<u64>,
}

/// Groups the hydrated streams into classes and decides which key labels
/// the statement reads (issue #507).
///
/// A bare key label that every selected stream carries is not read: our
/// parser renames the body's value of that name on every row, so no body
/// can change the answer through it. A key label some streams carry is read
/// and blanked on those streams.
pub(in crate::logql) fn resolve(
    value: &UnwrappedValue,
    meta: &HashMap<u64, StreamMetaRow>,
) -> ResolvedGroupKey {
    let mut fingerprints: Vec<u64> = meta.keys().copied().collect();
    fingerprints.sort_unstable();
    let stream_labels: HashMap<u64, LabelSet> =
        meta.iter().map(|(fp, m)| (*fp, series_labels(m))).collect();
    let keys: Vec<(UnwrapKeyLabel, Vec<u64>)> = value
        .keys
        .iter()
        .filter_map(|key| match value.form {
            UnwrapForm::Targeted => Some((key.clone(), Vec::new())),
            UnwrapForm::Bare => {
                let blank: Vec<u64> = fingerprints
                    .iter()
                    .copied()
                    .filter(|fp| {
                        stream_labels[fp]
                            .iter()
                            .any(|(k, _)| k.as_str() == key.label)
                    })
                    .collect();
                if !fingerprints.is_empty() && blank.len() == fingerprints.len() {
                    None
                } else {
                    Some((key.clone(), blank))
                }
            }
        })
        .collect();
    let (classes, class_labels) = match &value.classes {
        ClassNames::PerFingerprint => (None, stream_labels.clone()),
        ClassNames::Projected(names) => group_into_classes(&fingerprints, &stream_labels, |k| {
            names.iter().any(|n| n == k)
        }),
        ClassNames::Without(names) => group_into_classes(&fingerprints, &stream_labels, |k| {
            !names.iter().any(|n| n == k)
        }),
    };
    ResolvedGroupKey {
        columns: GroupKeyColumns { keys, classes },
        class_labels,
        stream_labels,
        fingerprints,
    }
}

type Classes = (Option<Vec<Vec<u64>>>, HashMap<u64, LabelSet>);

fn group_into_classes(
    fingerprints: &[u64],
    stream_labels: &HashMap<u64, LabelSet>,
    keeps: impl Fn(&str) -> bool,
) -> Classes {
    // Ordered by the projected labels, so a class id is stable run to run.
    let mut by_labels: BTreeMap<LabelSet, Vec<u64>> = BTreeMap::new();
    for fp in fingerprints {
        let projected: LabelSet = stream_labels[fp]
            .iter()
            .filter(|(k, _)| keeps(k))
            .cloned()
            .collect();
        by_labels.entry(projected).or_default().push(*fp);
    }
    let mut classes = Vec::with_capacity(by_labels.len());
    let mut labels = HashMap::with_capacity(by_labels.len());
    for (id, (projected, fps)) in by_labels.into_iter().enumerate() {
        labels.insert(id as u64, projected);
        classes.push(fps);
    }
    (Some(classes), labels)
}

/// The group document of one decided group (issue #507): each present key
/// label's source key with its text as a JSON string, then the unwrapped
/// path holding the placeholder `0`. A blanked or absent key is omitted.
pub(in crate::logql) fn group_document(
    value: &UnwrappedValue,
    keys: &[(UnwrapKeyLabel, Vec<u64>)],
    row_keys: &[(u8, String)],
) -> String {
    let mut doc = String::from("{");
    for ((key, _), (present, text)) in keys.iter().zip(row_keys) {
        if *present == 0 {
            continue;
        }
        push_json_string(&mut doc, &key.source);
        doc.push(':');
        push_json_string(&mut doc, text);
        doc.push(',');
    }
    let mut leaf = String::new();
    for (i, seg) in value.path.iter().enumerate() {
        push_json_string(&mut leaf, seg);
        leaf.push(':');
        if i + 1 < value.path.len() {
            leaf.push('{');
        }
    }
    leaf.push('0');
    for _ in 1..value.path.len() {
        leaf.push('}');
    }
    doc.push_str(&leaf);
    doc.push('}');
    doc
}

/// What one group document, or one undecided body, contributes (issue #507).
#[derive(Debug, Clone, PartialEq)]
pub(in crate::logql) enum GroupOutcome {
    /// The answer labels (sorted) and the sample value.
    Keep(LabelSet, f64),
    /// The pipeline drops the line.
    Drop,
    /// The key route cannot answer this group; the reason names why.
    TodaysRoute(&'static str),
}

/// Runs our pipeline over a group document with a group's labels (issue
/// #507). Anything but a kept, error-free line whose value is the
/// placeholder is today's route.
pub(in crate::logql) fn run_group(
    compiled: &CompiledPipeline,
    value: &UnwrappedValue,
    doc: &str,
    base: &[(String, String)],
    sm: &StructuredMetadataCtx,
) -> GroupOutcome {
    let mut labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
    match compiled.run_metric_step_into(
        doc,
        base,
        0,
        sm,
        value.grouping.as_ref(),
        &value.rules,
        &mut labels,
    ) {
        Err(_) => GroupOutcome::TodaysRoute("the group document exceeded a per-line budget"),
        Ok(MetricRun::Dropped) => GroupOutcome::Drop,
        Ok(MetricRun::Kept { value: v, .. }) => {
            if labels
                .iter()
                .any(|(k, v)| k.as_ref() == ERROR_LABEL && !v.is_empty())
            {
                return GroupOutcome::TodaysRoute("the group document carries an error");
            }
            match v {
                Some(v) if v.to_bits() == 0f64.to_bits() => {
                    let mut out: LabelSet = labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                    out.sort();
                    GroupOutcome::Keep(out, 0.0)
                }
                _ => GroupOutcome::TodaysRoute("the group document's value is not its placeholder"),
            }
        }
    }
}

/// Runs our pipeline over an undecided row's body in L, with its stream's
/// labels and its stored metadata (issue #507). An error series is the
/// client path's pipeline error; a per-line budget breach is its `422`.
pub(in crate::logql) fn run_lane_body(
    compiled: &CompiledPipeline,
    value: &UnwrappedValue,
    body: &str,
    base: &[(String, String)],
    sm: &StructuredMetadataCtx,
) -> Result<GroupOutcome, ReadError> {
    let mut labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
    let run = compiled
        .run_metric_step_into(
            body,
            base,
            0,
            sm,
            value.grouping.as_ref(),
            &value.rules,
            &mut labels,
        )
        .map_err(ReadError::from)?;
    match run {
        MetricRun::Dropped => Ok(GroupOutcome::Drop),
        MetricRun::Kept { value: v, .. } => {
            check_surviving_error(&labels)?;
            let mut out: LabelSet = labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            out.sort();
            // A failed conversion that passed the check is the converter's
            // zero, as on the client path.
            Ok(GroupOutcome::Keep(out, v.unwrap_or(0.0)))
        }
    }
}

/// One answer label set's accumulated sum and sample count at one grid point.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Partial {
    sum: f64,
    n: u64,
}

/// The fold over S1's groups or L's rows (issue #507).
pub(in crate::logql) struct KeyRouteFold<'q> {
    value: &'q UnwrappedValue,
    compiled: &'q CompiledPipeline,
    resolved: &'q ResolvedGroupKey,
    /// The innermost vector aggregation's grouping when it is a `sum` over
    /// `sum_over_time`: partials are keyed by its projection, which the
    /// aggregation applies next anyway, so answer label sets it merges do
    /// not accumulate separately.
    project: Option<Option<pulsus_logql::Grouping>>,
    grid_start_ns: i64,
    end_ns: i64,
    partials: HashMap<String, (LabelSet, BTreeMap<i64, Partial>)>,
    charged: u64,
    caps: AggCaps,
    merge_buf: Vec<(String, String)>,
    sm_buf: Vec<(String, String)>,
    sm_ctx: StructuredMetadataCtx,
}

/// Why a fold cannot answer: today's route, or a refusal that is the answer.
#[derive(Debug)]
pub(in crate::logql) enum FoldStop {
    TodaysRoute(&'static str),
    Refusal(ReadError),
}

impl From<ReadError> for FoldStop {
    fn from(e: ReadError) -> Self {
        FoldStop::Refusal(e)
    }
}

impl<'q> KeyRouteFold<'q> {
    pub(in crate::logql) fn new(
        value: &'q UnwrappedValue,
        compiled: &'q CompiledPipeline,
        resolved: &'q ResolvedGroupKey,
        vector_aggs: &[super::plan::VectorAggSpec],
        grid_start_ns: i64,
        end_ns: i64,
        caps: AggCaps,
    ) -> Self {
        let project = match (value.reducer, vector_aggs.last()) {
            (UnwrapReducer::Sum, Some((pulsus_logql::VectorAggOp::Sum, grouping, _))) => {
                Some(grouping.clone())
            }
            _ => None,
        };
        KeyRouteFold {
            value,
            compiled,
            resolved,
            project,
            grid_start_ns,
            end_ns,
            partials: HashMap::new(),
            charged: 0,
            caps,
            merge_buf: Vec::new(),
            sm_buf: Vec::new(),
            sm_ctx: StructuredMetadataCtx::default(),
        }
    }

    /// The labels a group's document runs with: the class's stream labels
    /// merged with the metadata the statement sent. A presence name or the
    /// unwrapped label among them is today's route.
    fn group_base(
        &mut self,
        class: u64,
        sm_text: &str,
        sm_kept: &[(String, String)],
    ) -> Result<bool, FoldStop> {
        let Some(base) = self.resolved.class_labels.get(&class) else {
            return Ok(false);
        };
        match &self.value.metadata {
            MetadataSent::Text if !sm_text.is_empty() => {
                merge_labels_with_structured_metadata(
                    base,
                    sm_text,
                    &mut self.merge_buf,
                    &mut self.sm_buf,
                    &mut self.sm_ctx,
                );
            }
            MetadataSent::Projected { .. } if !sm_kept.is_empty() => {
                merge_labels_with_structured_metadata_pairs(
                    base,
                    sm_kept,
                    &mut self.merge_buf,
                    &mut self.sm_ctx,
                );
            }
            _ => {
                self.merge_buf.clear();
                self.merge_buf.extend(base.iter().cloned());
                self.sm_ctx = StructuredMetadataCtx::default();
                self.sm_ctx.stream_label_count = None;
            }
        }
        if !self.sm_ctx.err.is_empty() || !self.sm_ctx.details.is_empty() {
            return Err(FoldStop::TodaysRoute(
                "a row's metadata carries __error__ or __error_details__",
            ));
        }
        if self
            .merge_buf
            .iter()
            .any(|(k, _)| k.as_str() == self.value.label)
        {
            return Err(FoldStop::TodaysRoute(
                "a row's stream labels or metadata carry the unwrapped name",
            ));
        }
        Ok(true)
    }

    fn in_window(&self, bucket_ns: i64) -> bool {
        bucket_ns >= self.grid_start_ns && bucket_ns <= self.end_ns
    }

    /// Folds one S1 row.
    pub(in crate::logql) fn push_group_row(
        &mut self,
        row: &MetricRangeUnwrappedRow,
    ) -> Result<(), FoldStop> {
        if row.n_undecided > 0 {
            return Err(FoldStop::TodaysRoute("a group holds an undecided row"));
        }
        if !self.in_window(row.bucket_ns) || row.n_value == 0 {
            return Ok(());
        }
        self.push_decided(
            row.class,
            row.bucket_ns,
            &row.keys,
            &row.sm_text,
            &row.sm_kept,
            row.v,
            row.n_value,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn push_decided(
        &mut self,
        class: u64,
        bucket_ns: i64,
        keys: &[(u8, String)],
        sm_text: &str,
        sm_kept: &[(String, String)],
        v: f64,
        n: u64,
    ) -> Result<(), FoldStop> {
        if !self.group_base(class, sm_text, sm_kept)? {
            return Ok(());
        }
        let doc = group_document(self.value, &self.resolved.columns.keys, keys);
        let sm = if self.sm_ctx.stream_label_count.is_some() {
            &self.sm_ctx
        } else {
            &EMPTY_STRUCTURED_METADATA
        };
        match run_group(self.compiled, self.value, &doc, &self.merge_buf, sm) {
            GroupOutcome::Keep(labels, _) => self.add(labels, bucket_ns, v, n),
            GroupOutcome::Drop => Ok(()),
            GroupOutcome::TodaysRoute(why) => Err(FoldStop::TodaysRoute(why)),
        }
    }

    /// Folds one L row.
    pub(in crate::logql) fn push_lane_row(
        &mut self,
        row: &UnwrappedLaneRow,
    ) -> Result<(), FoldStop> {
        if !self.in_window(row.bucket_ns) {
            return Ok(());
        }
        if row.decided == 1 {
            return self.push_decided(
                row.class,
                row.bucket_ns,
                &row.keys,
                &row.sm_text,
                &row.sm_kept,
                row.v,
                1,
            );
        }
        let Some(base) = self.resolved.stream_labels.get(&row.fingerprint) else {
            return Ok(());
        };
        let outcome = if row.sm_text.is_empty() {
            run_lane_body(
                self.compiled,
                self.value,
                &row.body,
                base,
                &EMPTY_STRUCTURED_METADATA,
            )?
        } else {
            merge_labels_with_structured_metadata(
                base,
                &row.sm_text,
                &mut self.merge_buf,
                &mut self.sm_buf,
                &mut self.sm_ctx,
            );
            run_lane_body(
                self.compiled,
                self.value,
                &row.body,
                &self.merge_buf,
                &self.sm_ctx,
            )?
        };
        match outcome {
            GroupOutcome::Keep(labels, x) => self.add(labels, row.bucket_ns, x, 1),
            GroupOutcome::Drop => Ok(()),
            GroupOutcome::TodaysRoute(why) => Err(FoldStop::TodaysRoute(why)),
        }
    }

    fn add(
        &mut self,
        mut labels: LabelSet,
        bucket_ns: i64,
        v: f64,
        n: u64,
    ) -> Result<(), FoldStop> {
        // A row whose error slot is set keeps its ungrouped labels, as the
        // range step keeps them (`RangeStepRules::parent_sum`); only a
        // preserved error reaches here, and its series is today's route's.
        let errored = labels
            .iter()
            .any(|(k, v)| k.as_str() == ERROR_LABEL && !v.is_empty());
        if let Some(grouping) = self.project.as_ref().filter(|_| !errored) {
            match grouping {
                None => labels.clear(),
                Some(g) => match g.kind {
                    pulsus_logql::GroupingKind::By => {
                        labels.retain(|(k, _)| g.labels.iter().any(|l| l == k))
                    }
                    pulsus_logql::GroupingKind::Without => {
                        labels.retain(|(k, _)| !g.labels.iter().any(|l| l == k))
                    }
                },
            }
        }
        let key = render_series_labels(&labels);
        match self.partials.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let (_, points) = e.get_mut();
                match points.entry(bucket_ns) {
                    std::collections::btree_map::Entry::Occupied(mut p) => {
                        let p = p.get_mut();
                        p.sum += v;
                        p.n = p.n.saturating_add(n);
                    }
                    std::collections::btree_map::Entry::Vacant(p) => {
                        charge_group_bytes(
                            &mut self.charged,
                            map_entry_bytes(PUSHDOWN_RANGE_POINT_SLOT),
                            self.caps.group_bytes,
                        )?;
                        p.insert(Partial { sum: 0.0 + v, n });
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                let cost = group_entry_bytes(e.key(), &labels, PUSHDOWN_RANGE_SLOT)
                    .saturating_add(map_entry_bytes(PUSHDOWN_RANGE_POINT_SLOT));
                charge_group_bytes(&mut self.charged, cost, self.caps.group_bytes)?;
                let mut points = BTreeMap::new();
                points.insert(bucket_ns, Partial { sum: 0.0 + v, n });
                e.insert((labels, points));
            }
        }
        Ok(())
    }

    /// The answer, in rendered-label order with ascending points. A merged
    /// value that is not finite is today's route: a sum over finite samples
    /// can overflow where the client path's incremental mean does not.
    pub(in crate::logql) fn finish(self) -> Result<Vec<FoldedSeries>, FoldStop> {
        let reducer = self.value.reducer;
        let mut out: Vec<(String, LabelSet, BTreeMap<i64, Partial>)> = self
            .partials
            .into_iter()
            .map(|(k, (l, p))| (k, l, p))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        let mut series = Vec::with_capacity(out.len());
        for (_, labels, points) in out {
            let mut pts = Vec::with_capacity(points.len());
            for (ts, p) in points {
                let v = merged_value(reducer, p);
                if !v.is_finite() {
                    return Err(FoldStop::TodaysRoute(
                        "a merged value is not finite, which a sum over finite samples can be \
                         where the client path's incremental form is not",
                    ));
                }
                pts.push((ts, v));
            }
            series.push((labels, pts));
        }
        Ok(series)
    }
}

fn merged_value(reducer: UnwrapReducer, p: Partial) -> f64 {
    match reducer {
        UnwrapReducer::Sum => p.sum,
        UnwrapReducer::Avg => p.sum / p.n as f64,
    }
}

/// Test-only access to the key route's resolution and fold (issue #507),
/// for the live agreement measurement: it folds rows the one read returned
/// exactly as the reader does, one row at a time. Production never uses it.
#[doc(hidden)]
pub mod probe {
    use std::collections::HashMap;

    use super::{FoldStop, KeyRouteFold, ResolvedGroupKey, resolve};
    use crate::logql::charge::AggCaps;
    use crate::logql::error::ReadError;
    use crate::logql::pipeline::CompiledPipeline;
    use crate::logql::plan::{MetricPlan, VectorAggSpec};
    use crate::logql::rows::{StreamMetaRow, UnwrappedLaneRow};
    use crate::logql::sql::{GroupKeyColumns, MetricValue, UnwrappedValue};

    /// A plan's group key read, resolved over a stream set.
    pub struct GroupKeyProbe {
        value: UnwrappedValue,
        compiled: CompiledPipeline,
        resolved: ResolvedGroupKey,
        vector_aggs: Vec<VectorAggSpec>,
        grid_start_ns: i64,
        end_ns: i64,
    }

    /// One folded answer: each series' sorted labels and its ascending
    /// `(grid point, value)` points.
    pub type ProbeSeries = Vec<(Vec<(String, String)>, Vec<(i64, f64)>)>;

    /// What folding one or more rows gives.
    #[derive(Debug)]
    pub enum ProbeOutcome {
        /// The answer's series: sorted labels, ascending points.
        Answer(ProbeSeries),
        /// The fold sends the query to today's route.
        TodaysRoute(&'static str),
        /// A refusal that is the query's answer.
        Refusal(ReadError),
    }

    impl GroupKeyProbe {
        /// `None` when the plan is not the group key read.
        pub fn new(mp: &MetricPlan, meta: &HashMap<u64, StreamMetaRow>) -> Option<Self> {
            let MetricValue::Unwrapped(u) = &mp.value else {
                return None;
            };
            let compiled = CompiledPipeline::compile(&u.stages).ok()?;
            Some(GroupKeyProbe {
                resolved: resolve(u, meta),
                value: (**u).clone(),
                compiled,
                vector_aggs: mp.vector_aggs.clone(),
                grid_start_ns: mp.grid_start_ns,
                end_ns: mp.end_ns,
            })
        }

        /// The key columns and classes the statements are rendered with.
        pub fn columns(&self) -> &GroupKeyColumns {
            &self.resolved.columns
        }

        /// The fingerprints the statements read, ascending.
        pub fn fingerprints(&self) -> &[u64] {
            &self.resolved.fingerprints
        }

        /// Folds `rows` of the one read, in order, as the reader does.
        pub fn fold_lane_rows(&self, rows: &[UnwrappedLaneRow]) -> ProbeOutcome {
            let mut fold = KeyRouteFold::new(
                &self.value,
                &self.compiled,
                &self.resolved,
                &self.vector_aggs,
                self.grid_start_ns,
                self.end_ns,
                AggCaps::DEFAULT,
            );
            for row in rows {
                match fold.push_lane_row(row) {
                    Ok(()) => {}
                    Err(FoldStop::TodaysRoute(why)) => return ProbeOutcome::TodaysRoute(why),
                    Err(FoldStop::Refusal(e)) => return ProbeOutcome::Refusal(e),
                }
            }
            match fold.finish() {
                Ok(series) => ProbeOutcome::Answer(series),
                Err(FoldStop::TodaysRoute(why)) => ProbeOutcome::TodaysRoute(why),
                Err(FoldStop::Refusal(e)) => ProbeOutcome::Refusal(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::params::{Direction, PlanCtx, QueryParams, QuerySpec};
    use crate::logql::plan::{Plan, plan};
    use crate::logql::sql::MetricValue;

    /// The group key read a range query plans to, with a one-minute step.
    fn value_of(query: &str) -> UnwrappedValue {
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
        let expr = pulsus_logql::parse(query).expect("parse");
        match plan(&expr, &params, &ctx).expect("plan") {
            Plan::Metric(mp) => match mp.value {
                MetricValue::Unwrapped(u) => *u,
                other => panic!("{query}: expected the group key read, got {other:?}"),
            },
            _ => panic!("{query}: expected a metric plan"),
        }
    }

    fn stream(fp: u64, labels: &str) -> (u64, StreamMetaRow) {
        (
            fp,
            StreamMetaRow {
                fingerprint: fp,
                service: "s".to_string(),
                labels: labels.to_string(),
            },
        )
    }

    /// Criterion 5: the group document holds each present key label's source
    /// key with its text, then the unwrapped path's placeholder; an absent key
    /// is omitted.
    #[test]
    fn the_group_document_holds_the_present_keys_and_the_placeholder() {
        let v = value_of(
            r#"sum_over_time({a="b"} | json c="code", lat="latency", m="missing" | unwrap lat [1m])"#,
        );
        let resolved = resolve(&v, &HashMap::from([stream(1, r#"{"a":"b"}"#)]));
        let doc = group_document(
            &v,
            &resolved.columns.keys,
            &[(1, "a".to_string()), (0, String::new())],
        );
        assert_eq!(doc, r#"{"code":"a","latency":0}"#);
        // A nested unwrapped path nests the placeholder.
        let p = value_of(r#"sum_over_time({a="b"} | json lat="req.latency" | unwrap lat [1m])"#);
        assert_eq!(group_document(&p, &[], &[]), r#"{"req":{"latency":0}}"#);
        // A key text is written as a JSON string, escapes included.
        let doc = group_document(
            &v,
            &resolved.columns.keys,
            &[(1, "a\"b".to_string()), (1, "z".to_string())],
        );
        assert_eq!(doc, r#"{"code":"a\"b","missing":"z","latency":0}"#);
    }

    /// Criterion 5: streams whose labels agree on the class names are one
    /// class, whatever their other labels say.
    #[test]
    fn classes_split_streams_only_on_the_class_names() {
        let v = value_of(r#"sum by (pod) (sum_over_time({a="b"} | json | unwrap latency [1m]))"#);
        let meta = HashMap::from([
            stream(1, r#"{"a":"b","pod":"p1","zone":"z1"}"#),
            stream(2, r#"{"a":"b","pod":"p1","zone":"z2"}"#),
            stream(3, r#"{"a":"b","pod":"p2","zone":"z1"}"#),
        ]);
        let resolved = resolve(&v, &meta);
        assert_eq!(resolved.columns.classes, Some(vec![vec![1, 2], vec![3]]));
        assert_eq!(
            resolved.class_labels[&0],
            vec![("pod".to_string(), "p1".to_string())]
        );
        assert_eq!(
            resolved.class_labels[&1],
            vec![("pod".to_string(), "p2".to_string())]
        );
    }

    /// Criterion 5: a bare key label some streams carry is read and blanked
    /// on those streams; one every stream carries is not read at all; a
    /// blanked key is omitted from the document.
    #[test]
    fn a_blank_key_is_omitted_from_the_document() {
        let v = value_of(r#"sum by (code) (sum_over_time({a="b"} | json | unwrap latency [1m]))"#);
        let some = resolve(
            &v,
            &HashMap::from([
                stream(1, r#"{"a":"b","code":"s"}"#),
                stream(2, r#"{"a":"b"}"#),
            ]),
        );
        assert_eq!(some.columns.keys.len(), 1);
        assert_eq!(some.columns.keys[0].1, vec![1]);
        assert_eq!(
            group_document(&v, &some.columns.keys, &[(0, String::new())]),
            r#"{"latency":0}"#
        );
        let all = resolve(
            &v,
            &HashMap::from([
                stream(1, r#"{"a":"b","code":"s"}"#),
                stream(2, r#"{"a":"b","code":"t"}"#),
            ]),
        );
        assert!(all.columns.keys.is_empty(), "{:?}", all.columns.keys);
    }
}
