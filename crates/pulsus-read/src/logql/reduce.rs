//! The lowered `| unwrap` reducers' fold (issue #624, part 4): the rows of
//! [`super::sql::metric_range_reduced`], one per `(key, grid point)`
//! window, become the answer's series.
//!
//! ```text
//! row (c, kv, pk, e, bucket_ns, n, r, t1, t2, v1)
//!   -> key labels  the class's labels, the key's metadata, x = "0" (or "x" when e = 1)
//!                  with no parser; with one, the parser's input rebuilt from pk with
//!                  x at the placeholder (issue #624, part 4b)
//!   -> the stages  once per key, today's route's pipeline less the filters the
//!                  statement applied, under its grouping and step rules
//!        dropped                         -> the key's rows are skipped
//!        kept with an error, or e = 1    -> today's route
//!        kept, labels another key has    -> today's route
//!        kept                            -> the point (bucket_ns, r), finished per reducer
//! ```

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use pulsus_logql::RangeAggOp;
use pulsus_model::{Fingerprint, FpLiteral};

use super::agg::LabelSet;
use super::charge::{
    AggCaps, MUT_GROUP_SLOT, SERIES_OUT_SLOT, charge_group_bytes, charge_result_points,
    group_entry_bytes,
};
use super::client_agg::{RowRoute, check_surviving_error, rate_counter_extrapolate, row_route};
use super::error::ReadError;
use super::exec::{MatrixSeries, apply_rate, range_fold_off_grid};
use super::labels::{
    StructuredMetadataCtx, merge_labels_with_structured_metadata_pairs, push_json_string,
    render_labels_json_sorted, series_labels, stream_hash,
};
use super::pipeline::RegexpCaptures;
use super::pipeline::{CompiledPipeline, MetricRun, RangeGrouping, RangeStepRules};
use super::rows::{MetricReducedRow, StreamMetaRow};
use super::sql::{KeyMode, ReducedKeyColumns, ReducedParser, ReducedUnwrap};

/// The grid and divisors one reduced fold emits on (issue #624, part 4).
#[derive(Debug, Clone, Copy)]
pub(in crate::logql) struct ReducedGrid {
    pub grid_start_ns: i64,
    pub end_ns: i64,
    pub step_ns: i64,
    pub grid_points: u64,
    /// The `[range]` width, which `rate_counter` extrapolates over.
    pub range_ns: i64,
    /// The divisor `rate` takes at the range step: `None` under a parent
    /// `sum`, which divides after the sum.
    pub rate_window_ns: Option<u64>,
}

/// What one row did to the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::logql) enum ReducedPush {
    /// Folded, or skipped.
    Folded,
    /// The fold cannot answer: today's route runs.
    Stop,
}

/// A resolved stream set's key columns, and the labels each class's keys
/// start from (issue #624, part 4, §3.2).
#[derive(Debug)]
pub(in crate::logql) struct ResolvedReduced {
    pub columns: ReducedKeyColumns,
    /// Class id (a fingerprint when every stream is its own class) -> the
    /// labels a key of that class starts from: in keep mode the class's
    /// labels on `L`; in drop mode one member's full stream labels.
    pub class_labels: HashMap<Fingerprint, LabelSet>,
    /// The hydrated fingerprints, ascending: the statement reads these only.
    pub fingerprints: Vec<Fingerprint>,
}

/// Groups the hydrated streams into classes and decides what the statement
/// reads of each (issue #624, part 4, §3.2).
///
/// - Keep mode (`by (L)`): one class per set of stream labels on `L`; a
///   metadata name in `L` is blanked on the streams carrying it as a stream
///   label, whose value the answer reads instead.
/// - Drop mode (no grouping, or `without (L)`, `E` = `x` and `L`): each
///   fingerprint is its own class unless a stream carries a name in `E`;
///   then streams are one class when they agree on every label outside `E`
///   and carry the same names of `E`. A metadata name in `E` is kept on the
///   streams carrying it as a stream label, where it becomes
///   `<name>_extracted`, which the grouping does not remove.
pub(in crate::logql) fn resolve_reduced(
    value: &ReducedUnwrap,
    meta: &HashMap<Fingerprint, StreamMetaRow>,
) -> ResolvedReduced {
    let mut fingerprints: Vec<Fingerprint> = meta.keys().copied().collect();
    fingerprints.sort_unstable();
    let stream_labels: HashMap<Fingerprint, LabelSet> =
        meta.iter().map(|(fp, m)| (*fp, series_labels(m))).collect();
    let carries = |fp: &Fingerprint, name: &str| stream_labels[fp].iter().any(|(k, _)| k == name);
    let names: &[String] = match &value.mode {
        KeyMode::Keep(l) | KeyMode::Drop(l) => l,
    };
    let blank: Vec<(String, Vec<FpLiteral>)> = names
        .iter()
        .map(|n| {
            let fps = fingerprints
                .iter()
                .filter(|fp| carries(fp, n))
                .copied()
                .map(Fingerprint::sql_literal)
                .collect();
            (n.clone(), fps)
        })
        .collect();
    let (classes, class_labels) = match &value.mode {
        KeyMode::Keep(l) => {
            group_into_classes(&fingerprints, &stream_labels, |k| l.iter().any(|n| n == k))
        }
        KeyMode::Drop(e)
            if !fingerprints
                .iter()
                .any(|fp| e.iter().any(|n| carries(fp, n))) =>
        {
            (None, stream_labels.clone())
        }
        KeyMode::Drop(e) => {
            // The labels outside `E`, and a marker for each name of `E` the
            // stream carries: a NUL cannot begin a label name, so a marker
            // never meets a label.
            let keyed: HashMap<Fingerprint, LabelSet> = stream_labels
                .iter()
                .map(|(fp, labels)| {
                    let mut k: LabelSet = labels
                        .iter()
                        .filter(|(n, _)| !e.contains(n))
                        .cloned()
                        .collect();
                    k.extend(
                        labels
                            .iter()
                            .filter(|(n, _)| e.contains(n))
                            .map(|(n, _)| (format!("\0{n}"), String::new())),
                    );
                    k.sort();
                    (*fp, k)
                })
                .collect();
            let (classes, by_class) = group_into_classes(&fingerprints, &keyed, |_| true);
            let mut first: BTreeMap<&LabelSet, Fingerprint> = BTreeMap::new();
            for fp in &fingerprints {
                first.entry(&keyed[fp]).or_insert(*fp);
            }
            let class_labels = by_class
                .iter()
                .map(|(id, k)| (*id, stream_labels[&first[k]].clone()))
                .collect();
            (classes, class_labels)
        }
    };
    // Part 4b, D3: the statement reads `x` from a stream label first.
    let x_values: Vec<(FpLiteral, String)> = fingerprints
        .iter()
        .filter_map(|fp| {
            stream_labels[fp]
                .iter()
                .find(|(k, _)| *k == value.label)
                .map(|(_, v)| (fp.sql_literal(), v.clone()))
        })
        .collect();
    // Part 4b: in keep mode a name of `pk` a stream carries is that
    // stream's label, and the parser's value of it is left out of `pk`.
    let pk_blank: Vec<(String, Vec<FpLiteral>)> = match &value.mode {
        KeyMode::Keep(_) => pk_labels(value)
            .into_iter()
            .map(|n| {
                let fps = fingerprints
                    .iter()
                    .filter(|fp| carries(fp, &n))
                    .copied()
                    .map(Fingerprint::sql_literal)
                    .collect();
                (n, fps)
            })
            .collect(),
        KeyMode::Drop(_) => Vec::new(),
    };
    let hashes = if classes
        .as_ref()
        .is_some_and(|c| c.iter().any(|members| members.len() > 1))
    {
        fingerprints
            .iter()
            .map(|fp| (fp.sql_literal(), stream_hash(&stream_labels[fp])))
            .collect()
    } else {
        Vec::new()
    };
    ResolvedReduced {
        columns: ReducedKeyColumns {
            classes,
            hashes,
            blank,
            x_values,
            pk_blank,
        },
        class_labels,
        fingerprints,
    }
}

/// A resolved stream set's classes: `None` when each fingerprint is its
/// own class, otherwise the fingerprints of each class by id; and each
/// class id's labels.
type Classes = (Option<Vec<Vec<FpLiteral>>>, HashMap<Fingerprint, LabelSet>);

/// Groups `fingerprints` into classes by their labels `keeps` holds
/// (issue #507, moved here by issue #624, part 4b).
fn group_into_classes(
    fingerprints: &[Fingerprint],
    stream_labels: &HashMap<Fingerprint, LabelSet>,
    keeps: impl Fn(&str) -> bool,
) -> Classes {
    // Ordered by the projected labels, so a class id is stable run to run.
    let mut by_labels: BTreeMap<LabelSet, Vec<Fingerprint>> = BTreeMap::new();
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
        // A class id and a fingerprint share one column: the statement
        // renders `fingerprint` itself when there is no grouping, and a
        // `transform(...)` into small ids when there is (issue #507's
        // `unwrapped_class_expr`). They are therefore one key space, which
        // is why an id is carried as a `Fingerprint` rather than beside
        // one.
        labels.insert(Fingerprint::from_raw(id as u128), projected);
        classes.push(fps.iter().copied().map(Fingerprint::sql_literal).collect());
    }
    (Some(classes), labels)
}

/// The label names `pk` can hold (issue #624, part 4b): a `| json` key's
/// destination, or a `| regexp`/`| pattern` group's sanitised name. A
/// `| logfmt` key holds its fields' text, and none is left out.
fn pk_labels(value: &ReducedUnwrap) -> Vec<String> {
    match &value.parser {
        Some(ReducedParser::Json { keys, .. }) => keys.iter().map(|k| k.label.clone()).collect(),
        Some(ReducedParser::Capture { groups, .. }) => groups
            .iter()
            .map(|(name, _)| {
                if super::pipeline::key_needs_sanitizing(name) {
                    super::pipeline::sanitize_label_key(name)
                } else {
                    name.clone()
                }
            })
            .collect(),
        Some(ReducedParser::Logfmt { .. }) | None => Vec::new(),
    }
}

/// Issue #624, part 4b, D7: a selected stream labelled `x` sends the query
/// to today's route when a parser runs in drop mode, where the parser's `x`
/// becomes `x_extracted`, part of the series, which `pk` does not carry.
pub(in crate::logql) fn labelled_x_refuses(
    value: &ReducedUnwrap,
    meta: &HashMap<Fingerprint, StreamMetaRow>,
) -> bool {
    value.parser.is_some()
        && matches!(value.mode, KeyMode::Drop(_))
        && meta
            .values()
            .any(|m| series_labels(m).iter().any(|(k, _)| *k == value.label))
}

/// The document a `| json` key-run parses (issue #624, part 4b): each of
/// `pk`'s `(source, text)` pairs as a JSON string, then `x`'s path holding
/// the placeholder as a JSON string.
pub(in crate::logql) fn group_document(
    pairs: &[(String, String)],
    path: &[String],
    placeholder: &str,
) -> String {
    let mut doc = String::from("{");
    for (source, text) in pairs {
        push_json_string(&mut doc, source);
        doc.push(':');
        push_json_string(&mut doc, text);
        doc.push(',');
    }
    for (i, seg) in path.iter().enumerate() {
        push_json_string(&mut doc, seg);
        doc.push(':');
        if i + 1 < path.len() {
            doc.push('{');
        }
    }
    push_json_string(&mut doc, placeholder);
    for _ in 1..path.len() {
        doc.push('}');
    }
    doc.push('}');
    doc
}

/// What the stages made of the last key: its rows are skipped, or they are
/// points of the series under this rendered key.
enum Memo {
    Dropped,
    Series(String),
}

/// The last key `(c, kv, pk, e)` and its outcome.
type KeyMemo = (u128, Vec<(String, String)>, Vec<(String, String)>, u8, Memo);

/// The lowered `| unwrap` fold (issue #624, part 4, §4).
///
/// **Today's route, once per key.** Every line of a key `(c, kv, e)` has the
/// key's labels, so the stages today's route runs per line give each line
/// of the key the same answer; they run here once, over the key's labels,
/// the unwrapped label set to `"0"` (or to `"x"`, which fails every
/// conversion, when the key's values failed). The rows of one key arrive
/// together, so only the last key's outcome is remembered.
///
/// **Charged by today's route's rule** for the same outcome
/// (`client_agg.rs`'s `row_route`): each new output series is charged its
/// label bytes and one grid of points before it is created.
pub(in crate::logql) struct ReducedFold {
    compiled: CompiledPipeline,
    grouping: Option<RangeGrouping>,
    step: RangeStepRules,
    fan_out: bool,
    reducer: RangeAggOp,
    quantile: Option<f64>,
    label: String,
    parser: Option<ReducedParser>,
    class_labels: HashMap<Fingerprint, LabelSet>,
    grid: ReducedGrid,
    caps: AggCaps,
    charged: u64,
    points: u64,
    series: HashMap<String, (LabelSet, Vec<Option<f64>>)>,
    memo: Option<KeyMemo>,
    merge_buf: Vec<(String, String)>,
    pairs: Vec<(String, String)>,
    sm_ctx: StructuredMetadataCtx,
}

impl ReducedFold {
    /// `class_labels` maps each class id (a fingerprint when every stream is
    /// its own class) to the labels its keys start from.
    pub(in crate::logql) fn new(
        value: &ReducedUnwrap,
        class_labels: HashMap<Fingerprint, LabelSet>,
        caps: AggCaps,
        grid: ReducedGrid,
        vector_aggs: &[super::plan::VectorAggSpec],
    ) -> Result<Self, ReadError> {
        let compiled = CompiledPipeline::compile(&value.key_stages)?;
        let grouping = value.todays_route.grouping.as_deref().cloned();
        let step = super::plan::range_step_rules(
            value.todays_route.range_op,
            &value.todays_route.pipeline,
            grouping.as_ref(),
            vector_aggs,
        );
        let fan_out = compiled.metric_mutates_labels() || grouping.is_some();
        Ok(ReducedFold {
            compiled,
            grouping,
            step,
            fan_out,
            reducer: value.reducer,
            quantile: value.quantile,
            label: value.label.clone(),
            parser: value.parser.clone(),
            class_labels,
            grid,
            caps,
            charged: 0,
            points: 0,
            series: HashMap::new(),
            memo: None,
            merge_buf: Vec::new(),
            pairs: Vec::new(),
            sm_ctx: StructuredMetadataCtx::default(),
        })
    }

    /// Folds one row. A row outside the emit grid is no point (the
    /// statement's scan ends at the query's end, which need not be on the
    /// grid); a cap is today's route's refusal at the same cap.
    pub(in crate::logql) fn push(
        &mut self,
        row: &MetricReducedRow,
    ) -> Result<ReducedPush, ReadError> {
        if row.bucket_ns < self.grid.grid_start_ns || row.bucket_ns > self.grid.end_ns {
            return Ok(ReducedPush::Folded);
        }
        let fresh = !matches!(&self.memo, Some((c, kv, pk, e, _))
            if *c == row.c && *kv == row.kv && *pk == row.pk && *e == row.e);
        if fresh {
            let outcome = match self.key_outcome(row)? {
                Some(outcome) => outcome,
                None => return Ok(ReducedPush::Stop),
            };
            self.memo = Some((row.c, row.kv.clone(), row.pk.clone(), row.e, outcome));
        }
        let Some((_, _, _, _, Memo::Series(key))) = &self.memo else {
            return Ok(ReducedPush::Folded);
        };
        let slot = reduced_slot(&self.grid, row.bucket_ns)?;
        let value = self.point_value(row);
        let cell = self
            .series
            .get_mut(key)
            .and_then(|(_, slots)| slots.get_mut(slot))
            .ok_or_else(|| range_fold_off_grid(row.bucket_ns))?;
        *cell = Some(value);
        Ok(ReducedPush::Folded)
    }

    /// What the stages make of a new key: `Some(Dropped)`, `Some(Series)`
    /// with the series created and charged, or `None` to stop.
    fn key_outcome(&mut self, row: &MetricReducedRow) -> Result<Option<Memo>, ReadError> {
        let ReducedFold {
            compiled,
            grouping,
            step,
            fan_out,
            label,
            parser,
            class_labels,
            merge_buf,
            pairs,
            sm_ctx,
            ..
        } = self;
        let Some(base) = class_labels.get(&Fingerprint::from_raw(row.c)) else {
            return Ok(Some(Memo::Dropped));
        };
        let placeholder = if row.e == 1 { "x" } else { "0" };
        pairs.clear();
        pairs.extend(row.kv.iter().filter(|(_, v)| !v.is_empty()).cloned());
        // Part 4b (S): a class labelled `x` (drop mode, no parser) holds the
        // placeholder in place of its member's value, and no metadata `x`.
        let relabelled: Option<LabelSet> =
            (parser.is_none() && base.iter().any(|(k, _)| k == label.as_str())).then(|| {
                base.iter()
                    .map(|(k, v)| {
                        if k == label.as_str() {
                            (k.clone(), placeholder.to_string())
                        } else {
                            (k.clone(), v.clone())
                        }
                    })
                    .collect()
            });
        if parser.is_none() && relabelled.is_none() {
            pairs.push((label.clone(), placeholder.to_string()));
        }
        merge_labels_with_structured_metadata_pairs(
            relabelled.as_ref().unwrap_or(base),
            pairs,
            merge_buf,
            sm_ctx,
        );
        // Part 4b, §4: the parser's input rebuilt from `pk`, `x` at the
        // placeholder.
        let mut caps: Vec<String> = Vec::new();
        let mut groups: Vec<(String, usize)> = Vec::new();
        let (document, captures): (String, bool) = match parser.as_ref() {
            None => (String::new(), false),
            Some(ReducedParser::Json { x_path, .. }) => {
                (group_document(&row.pk, x_path, placeholder), false)
            }
            Some(ReducedParser::Logfmt { x_source, .. }) => {
                let fields = row.pk.first().map_or("", |(_, f)| f.as_str());
                (format!("{fields} {x_source}={placeholder}"), false)
            }
            Some(ReducedParser::Capture {
                groups: sent,
                x_index,
                ..
            }) => {
                for (name, text) in &row.pk {
                    let index = sent.iter().find(|(n, _)| n == name).map_or(0, |(_, i)| *i);
                    groups.push((name.clone(), index));
                    caps.push(text.clone());
                }
                groups.push((label.clone(), *x_index));
                caps.push(placeholder.to_string());
                (String::new(), true)
            }
        };
        let captures = if captures {
            RegexpCaptures::Group {
                matched: true,
                caps: &caps,
                groups: &groups,
            }
        } else {
            RegexpCaptures::None
        };
        let mut scratch: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
        let run = compiled.run_metric_step_into_with_captures(
            &document,
            merge_buf,
            0,
            sm_ctx,
            grouping.as_ref(),
            step,
            captures,
            &mut scratch,
        )?;
        if matches!(run, MetricRun::Dropped) {
            return Ok(Some(Memo::Dropped));
        }
        if row.e == 1 || check_surviving_error(&scratch).is_err() {
            return Ok(None);
        }
        let slot = match row_route(*fan_out, false, &scratch, base) {
            RowRoute::Fingerprint => SERIES_OUT_SLOT,
            RowRoute::Labels => MUT_GROUP_SLOT,
        };
        scratch.sort_unstable();
        let key = render_labels_json_sorted(&scratch);
        if self.series.contains_key(&key) {
            // Another key's samples belong to this series' windows.
            return Ok(None);
        }
        let labels: LabelSet = scratch
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        charge_group_bytes(
            &mut self.charged,
            group_entry_bytes(&key, &labels, slot),
            self.caps.group_bytes,
        )?;
        charge_result_points(
            &mut self.points,
            self.grid.grid_points,
            self.caps.result_points,
        )?;
        let width = usize::try_from(self.grid.grid_points)
            .map_err(|_| range_fold_off_grid(row.bucket_ns))?;
        self.series.insert(key.clone(), (labels, vec![None; width]));
        Ok(Some(Memo::Series(key)))
    }

    /// The point a window's row gives, finished per reducer.
    fn point_value(&self, row: &MetricReducedRow) -> f64 {
        match self.reducer {
            RangeAggOp::Rate => apply_rate(row.r, self.grid.rate_window_ns),
            RangeAggOp::StddevOverTime => row.r.sqrt(),
            RangeAggOp::QuantileOverTime => match self.quantile {
                Some(q) if q < 0.0 => f64::NEG_INFINITY,
                Some(q) if q > 1.0 => f64::INFINITY,
                _ => row.r,
            },
            RangeAggOp::RateCounter => rate_counter_extrapolate(
                row.n,
                row.t1,
                row.v1,
                row.t2,
                row.r,
                row.bucket_ns,
                u64::try_from(self.grid.range_ns).ok(),
            ),
            _ => row.r,
        }
    }

    /// The series, in label order, each its points ascending.
    pub(in crate::logql) fn finish(self) -> Vec<MatrixSeries> {
        let (grid_start_ns, step_ns) = (self.grid.grid_start_ns, self.grid.step_ns);
        let mut series: Vec<(LabelSet, Vec<Option<f64>>)> = self.series.into_values().collect();
        series.sort_by(|a, b| a.0.cmp(&b.0));
        series
            .into_iter()
            .map(|(labels, slots)| MatrixSeries {
                labels,
                points: slots
                    .into_iter()
                    .enumerate()
                    .filter_map(|(k, v)| {
                        Some((
                            grid_start_ns.saturating_add((k as i64).saturating_mul(step_ns)),
                            v?,
                        ))
                    })
                    .collect(),
            })
            .collect()
    }
}

/// The slot a row inside `[grid_start, end]` folds into; a row there that is
/// not on the grid is an internal defect of the statement.
fn reduced_slot(grid: &ReducedGrid, bucket_ns: i64) -> Result<usize, ReadError> {
    let offset = i128::from(bucket_ns) - i128::from(grid.grid_start_ns);
    let step = i128::from(grid.step_ns);
    if step <= 0 || offset % step != 0 {
        return Err(range_fold_off_grid(bucket_ns));
    }
    usize::try_from(offset / step).map_err(|_| range_fold_off_grid(bucket_ns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::pipeline::RangeGrouping;
    use crate::logql::plan::{ClientAgg, ClientValue};
    use crate::logql::sql::{KeyMode, UnwrapConversion};
    use pulsus_logql::{RangeAggOp, Stage};

    fn stages(query: &str) -> Vec<Stage> {
        let pulsus_logql::Expr::Log(le) = pulsus_logql::parse(query).expect("parse") else {
            panic!("expected a log expression");
        };
        le.pipeline
    }

    fn value(mode: KeyMode, grouping: Option<RangeGrouping>, query: &str) -> ReducedUnwrap {
        let pipeline = stages(query);
        ReducedUnwrap {
            reducer: RangeAggOp::AvgOverTime,
            quantile: None,
            label: "v".to_string(),
            conversion: UnwrapConversion::Number,
            mode,
            key_stages: pipeline.clone(),
            todays_route: ClientAgg {
                pipeline,
                value: ClientValue::Unwrap,
                range_op: RangeAggOp::AvgOverTime,
                param: None,
                absent_labels: Vec::new(),
                grouping: grouping.map(Box::new),
            },
            instant: false,
            parser: None,
        }
    }

    const GRID: ReducedGrid = ReducedGrid {
        grid_start_ns: 60,
        end_ns: 120,
        step_ns: 60,
        grid_points: 2,
        range_ns: 60,
        rate_window_ns: Some(60),
    };

    fn row(c: u128, kv: &[(&str, &str)], e: u8) -> MetricReducedRow {
        MetricReducedRow {
            c,
            kv: kv
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            pk: Vec::new(),
            e,
            bucket_ns: 60,
            n: 1,
            r: 1.0,
            t1: 0,
            t2: 0,
            v1: 0.0,
        }
    }

    fn fold(value: &ReducedUnwrap) -> ReducedFold {
        ReducedFold::new(
            value,
            HashMap::from([(Fingerprint::from_raw(0), LabelSet::new())]),
            AggCaps::DEFAULT,
            GRID,
            &[],
        )
        .expect("a fold")
    }

    /// **T9 (issue #624, part 4): the fold hands the query to today's route
    /// when two keys reach one label set, or a key whose values failed to
    /// convert is kept; a filter that drops the failed key is no stop.**
    #[test]
    fn the_fold_hands_back_on_a_collision_or_a_kept_error() {
        // Two keys, one answer series: their samples would be one window.
        let keyed = value(
            KeyMode::Keep(vec!["f".to_string(), "pod".to_string()]),
            Some(RangeGrouping::By(vec!["pod".to_string()])),
            r#"{a="b"} | unwrap v"#,
        );
        let mut f = fold(&keyed);
        assert_eq!(
            f.push(&row(0, &[("f", "2"), ("pod", "a")], 0))
                .expect("push"),
            ReducedPush::Folded
        );
        assert_eq!(
            f.push(&row(0, &[("f", "3"), ("pod", "a")], 0))
                .expect("push"),
            ReducedPush::Stop,
            "a second key with the first key's labels"
        );

        // A failed key, kept: its error is today's route's to report.
        let bare = value(
            KeyMode::Drop(vec!["v".to_string()]),
            None,
            r#"{a="b"} | unwrap v"#,
        );
        let mut f = fold(&bare);
        assert_eq!(
            f.push(&row(0, &[], 1)).expect("push"),
            ReducedPush::Stop,
            "a kept e = 1 key"
        );

        // The same key under `| __error__=""`: dropped, and the fold goes on.
        let filtered = value(
            KeyMode::Drop(vec!["v".to_string()]),
            None,
            r#"{a="b"} | unwrap v | __error__="""#,
        );
        let mut f = fold(&filtered);
        assert_eq!(
            f.push(&row(0, &[], 1)).expect("push"),
            ReducedPush::Folded,
            "a dropped e = 1 key"
        );
        assert_eq!(f.push(&row(0, &[], 0)).expect("push"), ReducedPush::Folded);
        let series = f.finish();
        assert_eq!(series.len(), 1, "the e = 0 key's series: {series:?}");
        assert_eq!(series[0].points, vec![(60, 1.0)]);
    }

    /// **T8 (issue #624, part 4b): the key-run parses the key with the
    /// placeholder.** For each parser, a row's `pk` is what the parser
    /// reads: under `| unwrap x` an `e = 0` key is a series whose labels
    /// hold `pk`'s names (a name left out of `pk` is absent) and not `x`,
    /// and an `e = 1` key is kept with its error, so today's route answers;
    /// under `| unwrap x | __error__=""` the `e = 1` key is dropped.
    #[test]
    fn the_key_run_parses_the_key_with_the_placeholder() {
        use crate::logql::sql::{CaptureParser, ReducedParser, UnwrapForm, UnwrapKeyLabel};
        let names = |n: &[&str]| -> Vec<String> { n.iter().map(|s| s.to_string()).collect() };
        let keys = |n: &[&str]| -> Vec<UnwrapKeyLabel> {
            n.iter()
                .map(|s| UnwrapKeyLabel {
                    label: s.to_string(),
                    source: s.to_string(),
                })
                .collect()
        };
        let keep = || {
            (
                KeyMode::Keep(names(&["route", "status"])),
                Some(RangeGrouping::By(names(&["route", "status"]))),
            )
        };
        // (pipeline, unwrapped label, keep mode, parser, pk)
        type Case<'a> = (
            &'a str,
            &'a str,
            bool,
            ReducedParser,
            Vec<(&'a str, &'a str)>,
        );
        let cases: Vec<Case<'_>> = vec![
            (
                r#"{a="b"} | json | unwrap x"#,
                "x",
                true,
                ReducedParser::Json {
                    form: UnwrapForm::Bare,
                    keys: keys(&["route", "status"]),
                    x_path: names(&["x"]),
                },
                vec![("route", "r1")],
            ),
            (
                r#"{a="b"} | json x="x", route="route" | unwrap x"#,
                "x",
                false,
                ReducedParser::Json {
                    form: UnwrapForm::Targeted,
                    keys: keys(&["route"]),
                    x_path: names(&["x"]),
                },
                vec![("route", "r1")],
            ),
            (
                r#"{a="b"} | logfmt | unwrap x"#,
                "x",
                true,
                ReducedParser::Logfmt {
                    names: names(&["route", "status"]),
                    x_source: "x".to_string(),
                    x_destination: None,
                },
                vec![("", "route=r1")],
            ),
            (
                r#"{a="b"} | logfmt y="x", route="route" | unwrap y"#,
                "y",
                false,
                ReducedParser::Logfmt {
                    names: names(&["route"]),
                    x_source: "x".to_string(),
                    x_destination: Some("y".to_string()),
                },
                vec![("", "route=r1")],
            ),
            (
                r#"{a="b"} | regexp "route=(?P<route>\\S+) x=(?P<x>\\S+)" | unwrap x"#,
                "x",
                false,
                ReducedParser::Capture {
                    parser: CaptureParser::Regexp,
                    pattern: r"route=(?P<route>\S+) x=(?P<x>\S+)".to_string(),
                    groups: vec![("route".to_string(), 1)],
                    x_index: 2,
                },
                vec![("route", "r1")],
            ),
            (
                r#"{a="b"} | pattern "<_> route=<route> x=<x>" | unwrap x"#,
                "x",
                false,
                ReducedParser::Capture {
                    parser: CaptureParser::Pattern,
                    pattern: "<_> route=<route> x=<x>".to_string(),
                    groups: vec![("route".to_string(), 1)],
                    x_index: 2,
                },
                vec![("route", "r1")],
            ),
        ];
        for (query, label, kept, parser, pk) in cases {
            for filtered in [false, true] {
                let query = if filtered {
                    format!(r#"{query} | __error__="""#)
                } else {
                    query.to_string()
                };
                let (mode, grouping) = if kept {
                    keep()
                } else {
                    (KeyMode::Drop(names(&[label])), None)
                };
                let mut v = value(mode, grouping, &query);
                v.label = label.to_string();
                v.parser = Some(parser.clone());
                let mut r = row(0, &[], 0);
                r.pk = pk
                    .iter()
                    .map(|(k, t)| (k.to_string(), t.to_string()))
                    .collect();
                if !filtered {
                    let mut f = fold(&v);
                    let outcome = f.key_outcome(&r).expect("a key outcome");
                    let Some(Memo::Series(key)) = outcome else {
                        panic!("{query}: an e = 0 key is a series");
                    };
                    let labels = &f.series[&key].0;
                    assert!(
                        labels.iter().any(|(k, v)| k == "route" && v == "r1"),
                        "{query}: the labels hold pk's route: {labels:?}"
                    );
                    assert!(
                        !labels
                            .iter()
                            .any(|(k, _)| k == "status" || k == "x" || k == "y"),
                        "{query}: no name pk does not hold, and not x: {labels:?}"
                    );
                }
                r.e = 1;
                let mut f = fold(&v);
                let outcome = f.key_outcome(&r).expect("a key outcome");
                if filtered {
                    assert!(
                        matches!(outcome, Some(Memo::Dropped)),
                        "{query}: a failed key under __error__=\"\" is dropped"
                    );
                } else {
                    assert!(
                        outcome.is_none(),
                        "{query}: a failed key is kept with its error: today's route"
                    );
                }
            }
        }
    }
}
