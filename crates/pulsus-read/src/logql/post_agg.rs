//! Post-aggregation — everything downstream of a completed scan, and the
//! byte ledger that bounds it.
//!
//! `mod ledger`'s [`Ledger`] is the acquire-before-allocate token every
//! stage of this funnel takes, which is why the whole closure lives in one
//! module: `Ledger` is `pub(super)` to `mod ledger`, so a stage left
//! outside this file cannot name it. The `W_*`/`B_*` cost model prices a
//! stage before it runs, and [`apply_vector_aggs`] is the funnel's
//! entry point.

use super::cms;
use super::error::{ReadError, TooBroadReason};
use super::plan::{self};
use pulsus_logql::{BinOp, Grouping, GroupingKind, MatchGroup, VectorAggOp, VectorMatching};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::agg::{
    InstantSeries, LabelSet, RangeSeries, instant_payload_cmp, k_of, pin_reduction_order,
    range_payload_cmp, reduce,
};
use super::charge::{
    FP_GROUP_SLOT, INSTANT_GROUP_SLOT, MUT_GROUP_SLOT, SERIES_OUT_SLOT, grown_alloc_bytes,
    label_set_bytes, map_entry_bytes,
};
use super::exec::{MatrixSeries, QueryResult, VectorSample};
use super::fold::group_key;

/// Applies one binary operator to a pair of numbers, operand order
/// preserved (noncommutative ops are never reordered — plan v2 D4).
fn arith(op: BinOp, l: f64, r: f64) -> f64 {
    match op {
        BinOp::Add => l + r,
        BinOp::Sub => l - r,
        BinOp::Mul => l * r,
        BinOp::Div => l / r,
        BinOp::Mod => l % r,
        BinOp::Pow => l.powf(r),
        // Comparisons/set ops never reach `arith` (dispatched below).
        _ => unreachable!("arith called with a non-arithmetic operator"),
    }
}

fn compare(op: BinOp, l: f64, r: f64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Neq => l != r,
        BinOp::Gt => l > r,
        BinOp::Gte => l >= r,
        BinOp::Lt => l < r,
        BinOp::Lte => l <= r,
        _ => unreachable!("compare called with a non-comparison operator"),
    }
}

fn is_set_op(op: BinOp) -> bool {
    matches!(op, BinOp::And | BinOp::Or | BinOp::Unless)
}

/// One scalar-side application preserving orientation:
/// `scalar_on_left = false` → `vector_value OP scalar`;
/// `true` → `scalar OP vector_value`. For comparisons the VECTOR value
/// is kept on a filter match (oracle-probed: `5 < vec(10)` keeps `10`);
/// under `bool` every sample stays with value 0/1.
fn scalar_apply(
    op: BinOp,
    return_bool: bool,
    scalar: f64,
    v: f64,
    scalar_on_left: bool,
) -> Option<f64> {
    let (l, r) = if scalar_on_left {
        (scalar, v)
    } else {
        (v, scalar)
    };
    if op.is_comparison() {
        let hit = compare(op, l, r);
        if return_bool {
            Some(if hit { 1.0 } else { 0.0 })
        } else {
            hit.then_some(v)
        }
    } else {
        Some(arith(op, l, r))
    }
}

/// Combines two evaluated metric results (issue M6-10, extended by #91).
/// Scope: vector⊗scalar in BOTH orientations, vector⊗vector and
/// matrix⊗matrix with one-to-one AND `group_left`/`group_right` vector
/// matching (`on`/`ignoring` signatures), `bool`, and the `and`/`or`/
/// `unless` set operations. Matrix binops are an INDEPENDENT per-step
/// instant join (Prometheus/Loki re-evaluate the instant join per
/// timestamp — see [`combine_matrices`]). `matching` is the parsed
/// clause, `None` for default full-label one-to-one. `pub` for the
/// hermetic golden suite.
pub fn combine_binary(
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: QueryResult,
    rhs: QueryResult,
) -> Result<QueryResult, ReadError> {
    let mut charged = 0u64;
    combine_binary_capped(
        &mut charged,
        op,
        return_bool,
        matching,
        lhs,
        rhs,
        MAX_POST_AGG_BYTES,
    )
}

/// The binary cap seam; see [`apply_vector_aggs_capped`]. The charge is
/// levied on the measured operands BEFORE the join builds its one-side
/// index, its match signatures or its output.
#[allow(clippy::too_many_arguments)]
pub fn combine_binary_capped(
    charged: &mut u64,
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: QueryResult,
    rhs: QueryResult,
    cap: u64,
) -> Result<QueryResult, ReadError> {
    let (lm, rm) = (measure_operand(&lhs), measure_operand(&rhs));
    let bytes = binary_peak_bytes(op, matching, &lm, &rm);
    let l = Ledger::acquire_binary(charged, &lm, &rm, bytes, cap)?;
    match (lhs, rhs) {
        (QueryResult::Scalar(l), QueryResult::Scalar(r)) => {
            if is_set_op(op) {
                // Oracle-probed: a set operation against a scalar is a
                // named 400 ("unexpected literal for ... logical/set
                // binary operation").
                return Err(set_op_scalar_error(op));
            }
            // Oracle-probed: scalar⊗scalar comparison yields 0/1 with or
            // without `bool`.
            let v = if op.is_comparison() {
                if compare(op, l, r) { 1.0 } else { 0.0 }
            } else {
                arith(op, l, r)
            };
            Ok(QueryResult::Scalar(v))
        }
        (
            QueryResult::Scalar(s),
            vector_side @ (QueryResult::Vector(_) | QueryResult::Matrix(_)),
        ) => {
            if is_set_op(op) {
                return Err(set_op_scalar_error(op));
            }
            map_samples(
                vector_side,
                |v| scalar_apply(op, return_bool, s, v, true),
                &l,
            )
        }
        (
            vector_side @ (QueryResult::Vector(_) | QueryResult::Matrix(_)),
            QueryResult::Scalar(s),
        ) => {
            if is_set_op(op) {
                return Err(set_op_scalar_error(op));
            }
            map_samples(
                vector_side,
                |v| scalar_apply(op, return_bool, s, v, false),
                &l,
            )
        }
        (QueryResult::Vector(a), QueryResult::Vector(b)) => Ok(QueryResult::Vector(
            combine_vectors(op, return_bool, matching, a, b, &l)?,
        )),
        (QueryResult::Matrix(a), QueryResult::Matrix(b)) => Ok(QueryResult::Matrix(
            combine_matrices(op, return_bool, matching, a, b, &l)?,
        )),
        // Both operands evaluate under the same QuerySpec, so a
        // vector/matrix mix (or a streams/string operand) is structurally
        // impossible — defensive named error, never a panic.
        _ => Err(ReadError::PipelineInvalid {
            reason: "binary operation over incompatible result types".to_string(),
        }),
    }
}

fn set_op_scalar_error(op: BinOp) -> ReadError {
    ReadError::PipelineInvalid {
        reason: format!(
            "unexpected literal for a leg of logical/set binary operation ({op}): set \
             operations are defined between vectors only"
        ),
    }
}

/// Maps every sample of a vector/matrix result through `f` (`None`
/// drops the sample — the comparison-filter path), dropping series left
/// empty.
fn map_samples(
    result: QueryResult,
    f: impl Fn(f64) -> Option<f64>,
    l: &Ledger<'_>,
) -> Result<QueryResult, ReadError> {
    match &result {
        QueryResult::Vector(items) => l.admit(items.len() as u64, items.len() as u64)?,
        QueryResult::Matrix(items) => {
            let points = items
                .iter()
                .fold(0u64, |a, s| a.saturating_add(s.points.len() as u64));
            l.admit(items.len() as u64, points)?;
        }
        _ => {}
    }
    Ok(match result {
        QueryResult::Vector(items) => QueryResult::Vector(
            items
                .into_iter()
                .filter_map(|s| {
                    f(s.value).map(|value| VectorSample {
                        labels: s.labels,
                        value,
                    })
                })
                .collect(),
        ),
        QueryResult::Matrix(items) => QueryResult::Matrix(
            items
                .into_iter()
                .filter_map(|s| {
                    let points: Vec<(i64, f64)> = s
                        .points
                        .into_iter()
                        .filter_map(|(ts, v)| f(v).map(|nv| (ts, nv)))
                        .collect();
                    (!points.is_empty()).then_some(MatrixSeries {
                        labels: s.labels,
                        points,
                    })
                })
                .collect(),
        ),
        other => other,
    })
}

/// A reduced match signature — the `on`/`ignoring` projection of a
/// series' (already key-sorted) labels.
type MatchSig = Vec<(String, String)>;

/// Per-matrix-series timestamp index for the per-step join: each series'
/// borrowed labels paired with its `timestamp → value` map.
type StepIndex<'a> = Vec<(&'a [(String, String)], BTreeMap<i64, f64>)>;

/// One instant-vector element for the shared join core — labels borrowed
/// from the caller's operand (a [`VectorSample`] or a per-step projection
/// of a [`MatrixSeries`]) plus the sample value.
struct JoinItem<'a> {
    labels: &'a [(String, String)],
    value: f64,
}

/// Projects a series' labels onto its match signature: `on(l)` keeps only
/// the listed keys, `ignoring(l)` drops them, `None` keeps the full set
/// (byte-identical to the pre-#91 full-`LabelSet` key). Input is
/// key-sorted (aggregation sorts labels), so the output stays sorted.
fn match_signature(
    labels: &[(String, String)],
    matching: Option<&VectorMatching>,
    _l: &Ledger<'_>,
) -> MatchSig {
    match matching {
        None => labels.to_vec(),
        Some(vm) if vm.on => labels
            .iter()
            .filter(|(k, _)| vm.labels.iter().any(|l| l == k))
            .cloned()
            .collect(),
        Some(vm) => labels
            .iter()
            .filter(|(k, _)| !vm.labels.iter().any(|l| l == k))
            .cloned()
            .collect(),
    }
}

/// Sets `key`=`value` in a key-sorted label vector, replacing an existing
/// entry or inserting in sorted position (keeps the vector sorted so
/// downstream identity/equality stays canonical).
fn set_label_sorted(labels: &mut Vec<(String, String)>, key: &str, value: &str, _l: &Ledger<'_>) {
    match labels.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
        Ok(i) => labels[i].1 = value.to_string(),
        Err(i) => labels.insert(i, (key.to_string(), value.to_string())),
    }
}

/// Removes `key` from a key-sorted label vector (no-op if absent).
fn remove_label_sorted(labels: &mut Vec<(String, String)>, key: &str) {
    if let Ok(i) = labels.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
        labels.remove(i);
    }
}

fn duplicate_one_side_error(swapped: bool) -> ReadError {
    // Oracle-pinned, re-probed byte-identical at grafana/loki:3.7.4
    // (issue #240 wave0 capture): the "one" side is the source
    // rhs normally, the source lhs under `group_right`.
    let side = if swapped { "left" } else { "right" };
    ReadError::PipelineInvalid {
        reason: format!(
            "found duplicate series on the {side} hand-side;many-to-many matching not allowed: \
             matching labels must be unique on one side"
        ),
    }
}

fn multiple_matches_error() -> ReadError {
    // Oracle-pinned, re-probed byte-identical at grafana/loki:3.7.4
    // (issue #240 wave0 capture), byte-exact.
    ReadError::PipelineInvalid {
        reason: "multiple matches for labels: many-to-one matching must be explicit \
                 (group_left/group_right)"
            .to_string(),
    }
}

fn grouping_unique_error() -> ReadError {
    // Prometheus/Loki wording for a duplicate grouped output identity;
    // unreachable with distinct many-side series, kept for completeness.
    ReadError::PipelineInvalid {
        reason: "multiple matches for labels: grouping labels must ensure unique matches"
            .to_string(),
    }
}

/// The shared instant-join core (issue #91). BOTH the vector path
/// ([`combine_vectors`], one virtual step) and the matrix path
/// ([`combine_matrices`], looped over shared timestamps) call this, so the
/// two can never diverge. Fresh per-call state ⇒ duplicate detection is
/// per-step-scoped for matrices.
///
/// Semantics verified against `pulsus_promql::eval::binop` and pinned
/// against `grafana/loki:3.4.2`:
/// - one-to-one output labels = the reduced signature; the many side
///   passes through whole under `group_left`/`group_right`, include labels
///   copied from the one side (empty value ⇒ label absent).
/// - the one-side signature map is built UNCONDITIONALLY first, so a
///   duplicate one-side signature errors for every cardinality.
/// - the empty-operand short-circuit is scoped to arithmetic/comparison
///   ONLY (adjudicated); set ops get their own empty handling in
///   [`set_op_join`].
fn instant_join(
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: &[JoinItem<'_>],
    rhs: &[JoinItem<'_>],
    led: &Ledger<'_>,
) -> Result<Vec<VectorSample>, ReadError> {
    admit_join(lhs, rhs, led)?;
    if is_set_op(op) {
        return set_op_join(op, matching, lhs, rhs, led);
    }

    // Arithmetic/comparison empty-operand short-circuit — BEFORE the
    // one-side map is built, so an unpairable duplicate never surfaces a
    // spurious error (mirrors binop.rs). Scoped to arithmetic/comparison
    // ONLY; set ops handled above.
    if lhs.is_empty() || rhs.is_empty() {
        return Ok(Vec::new());
    }

    // Operand roles: `group_right` swaps sides so the loop always sees
    // `many` = the many side and `one` = the one side; the value
    // computation swaps back below.
    let (many, one, include, swapped) = match matching.and_then(|m| m.group.as_ref()) {
        None => (lhs, rhs, None, false),
        Some(MatchGroup::Left(inc)) => (lhs, rhs, Some(inc.as_slice()), false),
        Some(MatchGroup::Right(inc)) => (rhs, lhs, Some(inc.as_slice()), true),
    };
    let one_to_one = include.is_none();

    // The one side, hashed by match signature — a duplicate here is
    // many-to-many, an error for every cardinality.
    let mut one_by_key: HashMap<MatchSig, &JoinItem<'_>> = HashMap::with_capacity(one.len());
    for r in one {
        let key = match_signature(r.labels, matching, led);
        if one_by_key.insert(key, r).is_some() {
            return Err(duplicate_one_side_error(swapped));
        }
    }

    let mut one_to_one_matched: HashSet<MatchSig> = HashSet::new();
    let mut many_matched: HashMap<MatchSig, HashSet<MatchSig>> = HashMap::new();
    let mut out: Vec<VectorSample> = Vec::new();
    for l in many {
        let key = match_signature(l.labels, matching, led);
        let Some(r) = one_by_key.get(&key) else {
            continue;
        };
        // Restore source operand order for the value (upstream swap-back).
        let (vl, vr) = if swapped {
            (r.value, l.value)
        } else {
            (l.value, r.value)
        };
        let (value, keep) = if op.is_comparison() {
            let hit = compare(op, vl, vr);
            if return_bool {
                (if hit { 1.0 } else { 0.0 }, true)
            } else {
                (vl, hit)
            }
        } else {
            (arith(op, vl, vr), true)
        };

        let labels: MatchSig = if one_to_one {
            key.clone()
        } else {
            // Many side passes through whole; include labels copied from
            // the one side (empty value ⇒ absent, per binop.rs).
            let mut labels = l.labels.to_vec();
            if let Some(inc) = include {
                for ln in inc {
                    match r.labels.iter().find(|(k, _)| k == ln) {
                        Some((_, v)) if !v.is_empty() => set_label_sorted(&mut labels, ln, v, led),
                        _ => remove_label_sorted(&mut labels, ln),
                    }
                }
            }
            labels
        };

        // Duplicate detection — BEFORE the keep filter (a filtered-out
        // comparison still consumes its signature, upstream-exact).
        if one_to_one {
            if !one_to_one_matched.insert(key.clone()) {
                return Err(multiple_matches_error());
            }
        } else if !many_matched
            .entry(key.clone())
            .or_default()
            .insert(labels.clone())
        {
            return Err(grouping_unique_error());
        }

        if keep {
            out.push(VectorSample { labels, value });
        }
    }
    Ok(out)
}

/// The `and`/`or`/`unless` set operators keyed on the match signature
/// (issue #70 semantics, extended by #91 to reduced signatures under an
/// `on`/`ignoring` clause; a `group_left`/`group_right` on a set op is a
/// no-op, per the grafana/loki:3.4.2 probe). No empty-operand
/// short-circuit — each operator keeps its own empty handling
/// (`lhs and ∅`→∅; `lhs or ∅`→lhs, `∅ or rhs`→rhs; `lhs unless ∅`→lhs,
/// `∅ unless rhs`→∅), which per-step covers the matrix path.
fn set_op_join(
    op: BinOp,
    matching: Option<&VectorMatching>,
    lhs: &[JoinItem<'_>],
    rhs: &[JoinItem<'_>],
    l: &Ledger<'_>,
) -> Result<Vec<VectorSample>, ReadError> {
    admit_join(lhs, rhs, l)?;
    let own = |it: &JoinItem<'_>| VectorSample {
        labels: it.labels.to_vec(),
        value: it.value,
    };
    Ok(match op {
        BinOp::And => {
            let rhs_sigs: HashSet<MatchSig> = rhs
                .iter()
                .map(|s| match_signature(s.labels, matching, l))
                .collect();
            lhs.iter()
                .filter(|it| rhs_sigs.contains(&match_signature(it.labels, matching, l)))
                .map(own)
                .collect()
        }
        BinOp::Unless => {
            let rhs_sigs: HashSet<MatchSig> = rhs
                .iter()
                .map(|s| match_signature(s.labels, matching, l))
                .collect();
            lhs.iter()
                .filter(|it| !rhs_sigs.contains(&match_signature(it.labels, matching, l)))
                .map(own)
                .collect()
        }
        BinOp::Or => {
            let lhs_sigs: HashSet<MatchSig> = lhs
                .iter()
                .map(|s| match_signature(s.labels, matching, l))
                .collect();
            let mut out: Vec<VectorSample> = lhs.iter().map(own).collect();
            out.extend(
                rhs.iter()
                    .filter(|r| !lhs_sigs.contains(&match_signature(r.labels, matching, l)))
                    .map(own),
            );
            out
        }
        _ => unreachable!("is_set_op gates the arm"),
    })
}

/// Vector⊗vector: the [`instant_join`] core over one virtual step.
fn combine_vectors(
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: Vec<VectorSample>,
    rhs: Vec<VectorSample>,
    l: &Ledger<'_>,
) -> Result<Vec<VectorSample>, ReadError> {
    l.admit(
        (lhs.len() as u64).saturating_add(rhs.len() as u64),
        (lhs.len() as u64).saturating_add(rhs.len() as u64),
    )?;
    let lhs_items: Vec<JoinItem<'_>> = lhs
        .iter()
        .map(|s| JoinItem {
            labels: &s.labels,
            value: s.value,
        })
        .collect();
    let rhs_items: Vec<JoinItem<'_>> = rhs
        .iter()
        .map(|s| JoinItem {
            labels: &s.labels,
            value: s.value,
        })
        .collect();
    instant_join(op, return_bool, matching, &lhs_items, &rhs_items, l)
}

/// Matrix⊗matrix: an INDEPENDENT per-step instant join (issue #91 delta
/// 1). Prometheus/Loki re-evaluate the instant join at every timestamp;
/// two same-signature series whose points never share a step therefore
/// never collide, while a same-timestamp ambiguity errors. The per-step
/// core is [`instant_join`] — the exact function the vector path uses.
fn combine_matrices(
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: Vec<MatrixSeries>,
    rhs: Vec<MatrixSeries>,
    l: &Ledger<'_>,
) -> Result<Vec<MatrixSeries>, ReadError> {
    let series = (lhs.len() as u64).saturating_add(rhs.len() as u64);
    let points = lhs
        .iter()
        .chain(rhs.iter())
        .fold(0u64, |a, s| a.saturating_add(s.points.len() as u64));
    l.admit(series, points)?;
    // Index each side's points by timestamp once (labels stay borrowable
    // from the owned operands for the whole loop).
    let lhs_maps: StepIndex<'_> = lhs
        .iter()
        .map(|s| (s.labels.as_slice(), s.points.iter().copied().collect()))
        .collect();
    let rhs_maps: StepIndex<'_> = rhs
        .iter()
        .map(|s| (s.labels.as_slice(), s.points.iter().copied().collect()))
        .collect();

    // The union of every timestamp on either side (ascending) — set ops
    // need lhs-only / rhs-only steps too (`or`/`unless`).
    let mut timestamps: BTreeSet<i64> = BTreeSet::new();
    for (_, m) in lhs_maps.iter().chain(rhs_maps.iter()) {
        timestamps.extend(m.keys().copied());
    }

    // Output series keyed by output labels, first-seen order preserved.
    let mut order: Vec<MatchSig> = Vec::new();
    let mut out: HashMap<MatchSig, Vec<(i64, f64)>> = HashMap::new();
    // Reused per-step scratch (allocation discipline).
    let mut lhs_items: Vec<JoinItem<'_>> = Vec::new();
    let mut rhs_items: Vec<JoinItem<'_>> = Vec::new();
    for &t in &timestamps {
        lhs_items.clear();
        rhs_items.clear();
        for (labels, m) in &lhs_maps {
            if let Some(v) = m.get(&t) {
                lhs_items.push(JoinItem { labels, value: *v });
            }
        }
        for (labels, m) in &rhs_maps {
            if let Some(v) = m.get(&t) {
                rhs_items.push(JoinItem { labels, value: *v });
            }
        }
        for sample in instant_join(op, return_bool, matching, &lhs_items, &rhs_items, l)? {
            match out.get_mut(&sample.labels) {
                Some(points) => points.push((t, sample.value)),
                None => {
                    order.push(sample.labels.clone());
                    out.insert(sample.labels, vec![(t, sample.value)]);
                }
            }
        }
    }

    Ok(order
        .into_iter()
        .map(|labels| {
            let points = out.remove(&labels).expect("every ordered key was inserted");
            MatrixSeries { labels, points }
        })
        .collect())
}

/// `sort`/`sort_desc` order an instant result vector by value: ascending
/// (`Sort`) / descending (`SortDesc`). A NaN value ranks LAST in BOTH
/// directions (compared via `is_nan()`, so a NaN's sign never leaks into
/// the order the way `f64::total_cmp` alone would); equal values break by
/// label set ascending — deterministic and hermetically golden-able.
fn sort_instant(
    mut series: Vec<InstantSeries>,
    op: VectorAggOp,
    l: &Ledger<'_>,
) -> Result<Vec<InstantSeries>, ReadError> {
    admit_instant(&series, l)?;
    let desc = matches!(op, VectorAggOp::SortDesc);
    series.sort_by(|a, b| {
        a.value
            .is_nan()
            .cmp(&b.value.is_nan())
            .then_with(|| {
                if a.value.is_nan() {
                    std::cmp::Ordering::Equal
                } else if desc {
                    b.value.total_cmp(&a.value)
                } else {
                    a.value.total_cmp(&b.value)
                }
            })
            .then_with(|| a.labels.cmp(&b.labels))
    });
    Ok(series)
}

/// Deterministic candidate ordering for `topk`/`bottomk` (pinned by
/// golden, plan edge case 7): NaN candidates rank LAST for BOTH
/// directions (oracle-probed: `topk(2)` over `{NaN, 5, 1}` selects
/// `{5, 1}` and `bottomk(2)` selects `{1, 5}` — a NaN is never
/// preferred over a finite value); among non-NaN values, descending for
/// topk / ascending for bottomk; ties broken by the series' label set
/// ascending. `labels_of` is an ACCESSOR borrowing the caller's series
/// (issue #221 memory round: the former `Vec<LabelSet>` parameter was a
/// deep clone of every label set, input-scaled and uncharged — the
/// closure reads the identical bytes with zero copies).
pub(in crate::logql) fn sort_candidates<'a, F>(
    candidates: &mut [(usize, f64)],
    labels_of: F,
    largest: bool,
    _l: &Ledger<'_>,
) where
    F: Fn(usize) -> &'a LabelSet,
{
    candidates.sort_by(|(ai, av), (bi, bv)| {
        av.is_nan()
            .cmp(&bv.is_nan())
            .then_with(|| {
                if av.is_nan() {
                    // Both NaN: value order is meaningless; fall through
                    // to the label tie-break.
                    std::cmp::Ordering::Equal
                } else if largest {
                    bv.total_cmp(av)
                } else {
                    av.total_cmp(bv)
                }
            })
            .then_with(|| labels_of(*ai).cmp(labels_of(*bi)))
    });
}

/// `topk`/`bottomk` over a range result: within each group, at each step,
/// keep the k highest/lowest samples — preserving each survivor's
/// ORIGINAL series labels (selection, not reduction).
fn select_k_range(
    series: Vec<RangeSeries>,
    op: VectorAggOp,
    grouping: Option<&Grouping>,
    param: Option<f64>,
    l: &Ledger<'_>,
) -> Result<Vec<RangeSeries>, ReadError> {
    admit_range(&series, l)?;
    let k = k_of(param);
    if k == 0 {
        return Ok(Vec::new());
    }
    let largest = matches!(op, VectorAggOp::Topk);
    let mut groups: HashMap<LabelSet, Vec<usize>> = HashMap::new();
    for (idx, s) in series.iter().enumerate() {
        groups
            .entry(group_key(&s.labels, grouping))
            .or_default()
            .push(idx);
    }
    let mut keep: Vec<BTreeMap<i64, f64>> = series.iter().map(|_| BTreeMap::new()).collect();
    for members in groups.values() {
        let steps: BTreeSet<i64> = members
            .iter()
            .flat_map(|&i| series[i].points.keys().copied())
            .collect();
        for step in steps {
            let mut candidates: Vec<(usize, f64)> = members
                .iter()
                .filter_map(|&i| series[i].points.get(&step).map(|v| (i, *v)))
                .collect();
            sort_candidates(&mut candidates, |i| &series[i].labels, largest, l);
            for (idx, v) in candidates.into_iter().take(k) {
                keep[idx].insert(step, v);
            }
        }
    }
    Ok(series
        .into_iter()
        .zip(keep)
        .filter_map(|(s, points)| {
            (!points.is_empty()).then_some(RangeSeries {
                labels: s.labels,
                points,
            })
        })
        .collect())
}

/// `topk`/`bottomk` over an instant result: keep the k highest/lowest
/// samples per group, original labels preserved.
fn select_k_instant(
    series: Vec<InstantSeries>,
    op: VectorAggOp,
    grouping: Option<&Grouping>,
    param: Option<f64>,
    l: &Ledger<'_>,
) -> Result<Vec<InstantSeries>, ReadError> {
    admit_instant(&series, l)?;
    let k = k_of(param);
    if k == 0 {
        return Ok(Vec::new());
    }
    let largest = matches!(op, VectorAggOp::Topk);
    let mut groups: HashMap<LabelSet, Vec<usize>> = HashMap::new();
    for (idx, s) in series.iter().enumerate() {
        groups
            .entry(group_key(&s.labels, grouping))
            .or_default()
            .push(idx);
    }
    let mut keep: Vec<bool> = vec![false; series.len()];
    for members in groups.values() {
        let mut candidates: Vec<(usize, f64)> =
            members.iter().map(|&i| (i, series[i].value)).collect();
        sort_candidates(&mut candidates, |i| &series[i].labels, largest, l);
        for (idx, _) in candidates.into_iter().take(k) {
            keep[idx] = true;
        }
    }
    Ok(series
        .into_iter()
        .zip(keep)
        .filter_map(|(s, kept)| kept.then_some(s))
        .collect())
}

// =====================================================================
// Issue #236 Part B — the streaming vector-aggregation fold at the range
// leaf.
//
// `apply_vector_aggs` MATERIALISES: the leaf builds one `MatrixSeries`
// per scanned group and the aggregation collapses that vector afterwards,
// so peak retention is `scanned groups x grid points` even when the
// result is one series. The fold applies the INNERMOST aggregation as the
// leaf emits, so retention is `OUTPUT groups x grid points` — the
// reference's own bound.
//
// **The fold applies NO group-count rejection** (plan v14 §3 Part B, the
// round-13 `[high]`). [`MAX_QUERY_SERIES`] is a FINAL-result cap: an
// outer `sum` over an inner `sum by (id)` collapsing 501+ inner groups to
// ONE series is served by the reference, so rejecting an intermediate
// would reject on a proxy rather than on the resource consumed. Fold
// state is bounded by BYTES and by POINTS — and by nothing else.
//
// **The point half of that bound is NOT YET LEVIED**, and this comment
// says so rather than letting the sentence above be read as enforcement.
// A group's slots are DENSE — `kmax + 1` per output group, whatever the
// data's sparsity — so a fold over `G` output groups holds
// `G x (kmax + 1)` cells. Plan v14 §4's `charge_result_points` charges
// exactly that against [`MAX_METRIC_RESULT_POINTS`] BEFORE the vector is
// allocated; until it lands, the ceiling on a fold's retention is the
// leaf's own group-byte charge and the grid guard, and a query whose
// INTERMEDIATE grouping is very wide over a very fine grid can retain
// more than the finished result would. `MAX_METRIC_RESULT_POINTS` and
// [`MAX_ADMITTED_GRID_POINTS`] are defined but uncharged — do not read
// them as live gates.
// =====================================================================

/// `approx_topk(k, inner)` over an instant result (issue #221) — the
/// reference's `topk(k, CountMinSketchEval(__count_min_sketch__(inner)))`
/// rewrite (pkg/logql/optimize.go), evaluated in one pass:
///
/// 1. canonical order: labels normalized name-sorted in place, then the
///    series sorted by label set ascending (value `total_cmp` tiebreak so
///    the order is total even for a duplicated label set). The
///    reference's own insertion order is a randomized Go map walk
///    (pkg/logql/evaluator.go), i.e. unspecified — PulsusDB pins
///    determinism exactly as instant `first_over_time`/`last_over_time`
///    ties are pinned (docs/features.md §2);
/// 2. for every series: stream its `stableBytes` into the three hashes
///    ([`cms::series_key`]), `add` the SAMPLE VALUE to the sketch, then
///    the retention decision ([`cms::Retention::observe`] — sketch add
///    always precedes retention, per the reference order);
/// 3. at most [`cms::CMS_MAX_LABELS`] label sets are retained (inert
///    below the cap, which is where bit-exactness is claimed);
/// 4. every retained value is replaced by `count(key)` — THE ESTIMATE,
///    never the true value; labels are MOVED out of the input;
/// 5. `select_k_instant(.., Topk, None, param)` — the existing
///    selection, not a second implementation (`grouping` is
///    structurally `None`: rejected at parse time).
///
/// MEMORY (the #227 discipline, satisfied by construction — issue #221
/// plan v4's 13-row accounting, pinned by
/// `approx_topk_accounting_total_is_a_compile_time_constant`): NOTHING
/// on this path allocates proportionally to input. `R = CMS_MAX_LABELS +
/// 1` bounds every input-facing container; using the allocator model
/// `ab = alloc_block_bytes` / `gb = grown_alloc_bytes`:
///
/// | # | allocation | bytes (upper bound) |
/// |---|---|---|
/// | 1 | per-series `labels.sort_unstable()` | 0 (in place — a stable sort would allocate scratch; load-bearing) |
/// | 2 | `series.sort_unstable_by(..)` over all S | 0 (in place — a stable sort would allocate `S/2 x 32` B, input-scaled; load-bearing) |
/// | 3 | key hashing (`cms::series_key` streams `stableBytes`) | 0 |
/// | 4 | CMS counter grid (exact `vec![0.0; W*D]`) | ab(1_522_248) = 3_044_496 |
/// | 5 | retention heap `Vec<(u32, SeriesKey)>` `with_capacity(R)` | ab(24R) = 480_048 |
/// | 6 | `observed: HashSet<u64>` `with_capacity(R)` | ab(147_472) = 294_944 (16_384-bucket hashbrown layout) |
/// | 7 | retained output `Vec<InstantSeries>` `with_capacity(R)` (moved labels, zero new string bytes) | ab(32R) = 640_064 |
/// | 8a | `select_k_instant::groups` table (1 empty-key entry) | 1_024 (generous) |
/// | 8b | `groups`' member `Vec<usize>` (grown by push) | gb(8R) = 480_048 |
/// | 9 | `select_k_instant::keep: Vec<bool>` | ab(R) = 20_002 |
/// | 10 | `select_k_instant::candidates` | ab(16R) = 320_032 |
/// | 11 | `sort_candidates` driftsort scratch (≤ n/2 elements) | ab(16·⌈R/2⌉) = 160_032 |
/// | 12 | `select_k_instant` output (`filter_map` collect — grows) | gb(32R) = 1_920_192 |
///
/// **Peak ≤ 7_360_882 B (7.02 MiB) per `approx_topk` node** — the
/// conservative SUM (every row assumed live simultaneously, no reliance
/// on drop placement), every term a compile-time constant with no
/// dependence on series count, label size, cardinality or density. The
/// input `Vec<InstantSeries>` itself is the allocation this path
/// CONSUMES (built by `apply_vector_aggs` for every vector aggregation,
/// `topk` included) and is not a new charge. Because no term scales
/// with input, nothing here can fail a charge and `apply_vector_aggs`
/// stays infallible. `apply_vector_aggs` applies the agg chain
/// sequentially, so exactly one sketch is live regardless of nesting
/// (parser `MAX_DEPTH` = 64).
fn approx_topk_instant(
    mut series: Vec<InstantSeries>,
    param: Option<f64>,
    l: &Ledger<'_>,
) -> Result<Vec<InstantSeries>, ReadError> {
    admit_instant(&series, l)?;
    // 1. Canonical, input-order-independent ordering. `sort_unstable*`
    // is load-bearing (rows 1-2 of the accounting table): a stable
    // `sort` here would reintroduce an input-scaled scratch allocation.
    for s in &mut series {
        s.labels.sort_unstable();
    }
    series.sort_unstable_by(|a, b| {
        a.labels
            .cmp(&b.labels)
            .then_with(|| a.value.total_cmp(&b.value))
    });
    // 2-3. One streaming pass: sketch add (ALWAYS, first), then the
    // retention decision — the reference `HeapCountMinSketchVector.Add`
    // order.
    let mut sketch = cms::CountMinSketch::new();
    let mut retention = cms::Retention::new();
    for (idx, s) in series.iter().enumerate() {
        let key = cms::series_key(&s.labels);
        sketch.add(key, s.value);
        retention.observe(idx as u32, key, &sketch, |root| {
            series[root as usize].labels == s.labels
        });
    }
    // 4. Retained series in ascending input (canonical) order, each
    // value replaced by the sketch ESTIMATE; labels moved, never cloned.
    let mut retained = retention.into_entries();
    retained.sort_unstable_by_key(|&(idx, _)| idx);
    let mut out = Vec::with_capacity(retained.len());
    let mut next = retained.iter().peekable();
    for (idx, s) in series.into_iter().enumerate() {
        if let Some(&&(ridx, key)) = next.peek()
            && ridx as usize == idx
        {
            next.next();
            out.push(InstantSeries {
                labels: s.labels,
                value: sketch.count(key),
            });
        }
    }
    // 5. The existing selection — reused, not reimplemented.
    select_k_instant(out, VectorAggOp::Topk, None, param, l)
}

fn group_range(
    series: Vec<RangeSeries>,
    op: VectorAggOp,
    grouping: Option<&Grouping>,
    param: Option<f64>,
    l: &Ledger<'_>,
) -> Result<Vec<RangeSeries>, ReadError> {
    admit_range(&series, l)?;
    if matches!(op, VectorAggOp::Topk | VectorAggOp::Bottomk) {
        return select_k_range(series, op, grouping, param, l);
    }
    // A range result (matrix) has no single sortable value per series;
    // `sort`/`sort_desc` are passthrough here (the reference likewise does
    // not value-order matrices — the wire stays label-canonical).
    if matches!(op, VectorAggOp::Sort | VectorAggOp::SortDesc) {
        return Ok(series);
    }
    // Issue #236: the same pin as `group_instant`. `members` is walked in
    // push order at every step below, so pinning the push order pins the
    // per-step accumulation order for every step at once.
    let mut series = series;
    pin_reduction_order(
        &mut series,
        |s| &s.labels,
        |a, b| {
            range_payload_cmp(
                a.points.iter().map(|(t, v)| (*t, *v)),
                b.points.iter().map(|(t, v)| (*t, *v)),
            )
        },
    );
    let mut groups: HashMap<LabelSet, Vec<BTreeMap<i64, f64>>> = HashMap::new();
    for s in series {
        groups
            .entry(group_key(&s.labels, grouping))
            .or_default()
            .push(s.points);
    }
    Ok(groups
        .into_iter()
        .map(|(labels, members)| {
            let steps: BTreeSet<i64> = members.iter().flat_map(|m| m.keys().copied()).collect();
            let points = steps
                .into_iter()
                .filter_map(|step| {
                    let vals: Vec<f64> = members
                        .iter()
                        .filter_map(|m| m.get(&step).copied())
                        .collect();
                    if vals.is_empty() {
                        None
                    } else {
                        Some((step, reduce(op, &vals)))
                    }
                })
                .collect();
            RangeSeries { labels, points }
        })
        .collect())
}

fn group_instant(
    series: Vec<InstantSeries>,
    op: VectorAggOp,
    grouping: Option<&Grouping>,
    param: Option<f64>,
    l: &Ledger<'_>,
) -> Result<Vec<InstantSeries>, ReadError> {
    admit_instant(&series, l)?;
    // approx_topk (issue #221): sketch-estimate the values, then the
    // ordinary topk selection. Grouping is rejected at parse time, so
    // `grouping` is structurally `None` here (pinned by
    // `approx_topk_specs_never_carry_a_grouping` in plan.rs).
    if matches!(op, VectorAggOp::ApproxTopk) {
        return approx_topk_instant(series, param, l);
    }
    if matches!(op, VectorAggOp::Topk | VectorAggOp::Bottomk) {
        return select_k_instant(series, op, grouping, param, l);
    }
    // `sort`/`sort_desc` reorder the vector by value (no grouping —
    // rejected at plan time), preserving each series unchanged.
    if matches!(op, VectorAggOp::Sort | VectorAggOp::SortDesc) {
        return sort_instant(series, op, l);
    }
    // Issue #236: pin the reduction order before grouping — see
    // `pin_reduction_order`. Welford is order-sensitive and the incoming
    // order is a hash walk.
    let mut series = series;
    pin_reduction_order(&mut series, |s| &s.labels, instant_payload_cmp);
    let mut groups: HashMap<LabelSet, Vec<f64>> = HashMap::new();
    for s in series {
        groups
            .entry(group_key(&s.labels, grouping))
            .or_default()
            .push(s.value);
    }
    Ok(groups
        .into_iter()
        .map(|(labels, vals)| InstantSeries {
            labels,
            value: reduce(op, &vals),
        })
        .collect())
}

// ---------------------------------------------------------------------
// Issue #236 §4/§5 — the post-aggregation byte model.
//
// The coefficients below are MEASURED, not enumerated. Every `W_*`/`B_*`
// is `WITNESS_MARGIN x rate_max` where `rate_max` is the largest secant
// slope observed on that axis by the cohort-attributed allocator witness
// (`crates/pulsus-read/tests/logql_post_agg_witness.rs`). Nothing here
// enumerates containers, element widths or growth factors: the measured
// rate absorbs all of them, which is what makes a forgotten container
// impossible rather than merely unlikely.
//
// Every coefficient below is
//     shipped = ceil(rate_max x WITNESS_MARGIN x 11/10)
// with WITNESS_MARGIN = 2. The extra tenth is NOT a second safety margin
// and is not a hand tightening in either direction: an allocation
// measurement jitters by a few units between runs (hashbrown growth
// order, in-place-collect eligibility), and a gate of the form
// `shipped >= 2 x rate_max_measured_now` would redden on a 1 % drift. The
// tenth is a stated, uniform rounding rule so the CI gate is
// deterministic. There is no upper-bound gate, so rounding up costs
// nothing but tightness, which this design deliberately does not pin.
//
// Read `MAX_POST_AGG_BYTES`' doc for what the resulting bound does and
// does NOT claim.
// ---------------------------------------------------------------------

/// `W_SERIES` — bytes per stage-input series.
///
/// Ladder: `topk(k = N)` over a RANGE operand with no grouping, so every
/// stage retains everything. `N` spans `128 -> 8 192` (64x) with
/// points-per-series and label pairs scaled as `8 192 / N`, so `points`,
/// `label_bytes` and `label_pairs` are constant along the ladder.
/// Measured `rate_max` = **710** B/series (uniform; concentrated 416).
/// Shipped = `ceil(710 x 2 x 11/10)`.
pub const W_SERIES: u64 = 1_562;

/// `W_POINT` — bytes per stage-input point.
///
/// Ladder: a RANGE operand of 64 series with no grouping, so it collapses
/// to a SINGLE output group and one `BTreeSet<i64>` step union holds every
/// point; `steps` spans `4 -> 512` (128x on `points`).
/// Measured `rate_max` = **53** B/point (concentrated; uniform 42).
/// Shipped = `ceil(53 x 2 x 11/10)`.
pub const W_POINT: u64 = 117;

/// `W_LABEL_BYTE` — bytes per raw label content byte.
///
/// Ladder: `without(id00)` over 256 instant series of 4 label pairs — one
/// output group per series, so the retained key mass is maximal; the label
/// VALUE width spans `4 -> 1 024` bytes (128x on `label_bytes`).
/// Measured `rate_max` = **1** B/B on both skews.
/// Shipped = `ceil(1 x 2 x 11/10)`.
pub const W_LABEL_BYTE: u64 = 3;

/// `W_PAIR` — bytes per label pair.
///
/// Ladder: `without(id00)` over 64 instant series, pairs spanning
/// `4 -> 512` (128x) with the per-pair value width scaled as `2 048 /
/// pairs` so the byte total stays near constant.
/// Measured `rate_max` = **103** B/pair (concentrated; uniform 68; the
/// measurement jitters between 102 and 103 between runs, which is what
/// the 11/10 rounding covers).
/// Shipped = `ceil(103 x 2 x 11/10)`.
pub const W_PAIR: u64 = 227;

/// `W_STAGE_SERIES` — bytes per (series x chain stage).
///
/// **MEASURED ZERO, and that is a finding rather than an oversight.**
/// Plan v14 §6.1 predicted that "the previous stage's buffer is live
/// while its successor is collected", so a chain of `L` stages would cost
/// `L` concurrent buffers. It does not:
///
/// * `select_k_instant`'s output is
///   `series.into_iter().zip(keep).filter_map(..).collect()`, and `Zip`
///   and `FilterMap` over `vec::IntoIter` are `SourceIter` +
///   `InPlaceIterable`, so the standard library collects the output **in
///   place, into the input's own buffer** — the second buffer does not
///   exist at all;
/// * every vector-aggregation arm is non-expanding in both series and
///   points (grouping collapses, `topk` selects, `sort` permutes), so a
///   later stage's input is never larger than an earlier stage's, and the
///   peak cannot accumulate down the chain.
///
/// Ladder: nested `topk(k = N)` over 512 instant series at chain lengths
/// 1, 2, 4 and 64 — the peak is **21 204 B at every length**, on both
/// skews, so the rate is 0. Measured further across 8 (shape x grouping x
/// operator) combinations at lengths 1, 2, 4, 8 and 64: flat from length
/// 2 onward everywhere.
///
/// # DO NOT DELETE THIS TERM BECAUSE IT IS ZERO
///
/// **The zero is contingent on a COMPILER SPECIALISATION, not on the
/// nature of the computation.** Same-size in-place collect is an
/// optimisation the standard library is free to apply or not: it holds
/// because `InstantSeries` and `VectorSample` have identical layout and
/// because `Zip`/`FilterMap` over `vec::IntoIter` happen to implement
/// `SourceIter` + `InPlaceIterable` today. Insert an expanding
/// aggregation arm, change a collect's source shape, or add a stage whose
/// output type differs in layout from its input, and the second buffer
/// reappears — at which point a DELETED term would silently under-bound
/// the model and [`MAX_POST_AGG_BYTES`] would stop covering the real
/// peak. A bound that is too small is worse than no bound, because it is
/// trusted.
///
/// So the term stays in the model's published form (plan v14 §4), inert,
/// with `chain_depth_does_not_multiply_peak_memory` in
/// `tests/logql_post_agg_witness.rs` as the guard: it asserts that depth
/// beyond TWO stages adds nothing and that at most two stage buffers are
/// ever concurrent, over 8 shapes, and reddens the moment either stops
/// being true. Re-derive the coefficient then; do not delete the axis.
///
/// (Second occurrence in a week of `vec::IntoIter`'s in-place
/// specialisation falsifying a stated premise — issue #272's §8.6
/// correction was the first. It is a genuinely surprising optimisation.)
pub const W_STAGE_SERIES: u64 = 0;

/// `W_GROUPNAME` — bytes per (series x `by`-clause byte).
///
/// Ladder: `by(id00, <q-1 names absent from the data>)` over 256 instant
/// series, `q` spanning `4 -> 256` (64x on `series x
/// group_name_bytes`). §5.4 named an ALL-absent clause as the maximising
/// shape; measurement refutes that — every series then collapses into ONE
/// group, so exactly one key is retained and the peak is flat from `q = 4`
/// to `q = 16` — and §5.4's actual rule, the shape that maximises the
/// axis's rate, selects the one-present-name form.
/// Measured `rate_max` = **11** B per (series x by-byte), both skews.
/// Shipped = `ceil(11 x 2 x 11/10)`.
pub const W_GROUPNAME: u64 = 25;

/// `W_APPROX_TOPK` — the flat count-min sketch plus retention heap
/// (`cms::CMS_DEPTH x cms::CMS_WIDTH` `f64` counters = 7 x 27 183 x 8 B,
/// fixed and input-independent).
///
/// Derived as the measured peak MINUS the model without this term, over
/// the `approx_topk` cells at the SMALLEST inputs (1, 2, 8 and 64
/// series). A flat term is masked at a large fixture — the input-scaled
/// terms already dominate the 1.5 MiB sketch, and the excess reads as 0 —
/// so it is derived where it is visible, which is also where
/// under-bounding would be a real safety hole rather than a cosmetic one.
/// Measured excess = **1 907 298** B at one series.
/// Shipped = `ceil(1 907 298 x 2 x 11/10)`.
pub const W_APPROX_TOPK: u64 = 4_196_056;

/// `B_SERIES` — bytes per binary-operand series.
///
/// Ladder: one-to-one, `matching = None` (so the join signature is the
/// FULL label set and `B_MANY`/`B_INCLUDE` are zero in every rung),
/// instant operands, `N` spanning `64 -> 4 096` per side (64x on
/// `lhs.series + rhs.series`).
/// Measured `rate_max` = **578** B/series (concentrated; uniform 458).
/// Shipped = `ceil(578 x 2 x 11/10)`.
pub const B_SERIES: u64 = 1_272;

/// `B_POINT` — bytes per binary-operand point.
///
/// Ladder: the same matching over MATRIX operands of 16 series, `steps`
/// spanning `4 -> 512` (128x on the point total). Sixteen series, not the
/// usual baseline: `combine_matrices` runs an INDEPENDENT per-step join,
/// so its cost is `steps x series` and the widest rung otherwise
/// dominates the whole binary's wall time.
/// Measured `rate_max` = **37** B/point (concentrated; uniform 33).
/// Shipped = `ceil(37 x 2 x 11/10)`.
pub const B_POINT: u64 = 82;

/// `B_LABEL` — bytes per binary-operand raw label content byte.
///
/// Ladder: the same matching over 256 instant series of 4 pairs, label
/// VALUE width spanning `4 -> 1 024` bytes (128x).
/// Measured `rate_max` = **2** B/B on both skews.
/// Shipped = `ceil(2 x 2 x 11/10)`.
pub const B_LABEL: u64 = 5;

/// `B_PAIR` — bytes per binary-operand label pair.
///
/// Ladder: the same matching over 64 instant series, pairs spanning
/// `4 -> 512` (128x) with the per-pair width scaled as `2 048 / pairs`.
/// `matching = None` is load-bearing here: under `on(id00)` the match
/// signature is a ONE-pair projection, the other pairs are never cloned,
/// and the measured rate is 0 — the plan's pre-commitment to `None` for
/// this row is what makes the axis visible at all.
/// Measured `rate_max` = **107** B/pair (concentrated; uniform 79).
/// Shipped = `ceil(107 x 2 x 11/10)`.
pub const B_PAIR: u64 = 236;

/// `B_MANY` — bytes per many-side series under a group modifier. Zero
/// when there is no group modifier: the one-to-one arm keeps a single
/// `HashSet<MatchSig>` where the grouped arm keeps a `HashMap<MatchSig,
/// HashSet<MatchSig>>`.
///
/// Ladder: `on(id00) group_left()` with an EMPTY include list, many-side
/// width spanning `64 -> 4 096` (64x). This is the per-many-side-item
/// cost of `instant_join`'s `many_matched` map.
/// Measured `rate_max` = **1 319** B/series (concentrated; uniform
/// 1 152). Shipped = `ceil(1 319 x 2 x 11/10)`.
pub const B_MANY: u64 = 2_902;

/// `B_INCLUDE` — bytes per (many-side series x include byte).
///
/// Ladder: `on(id00) group_left(inc_1..inc_q)` over 128 instant series,
/// with the ONE side carrying all 256 include labels in every rung, `q`
/// spanning `4 -> 256` (64x on `many.series x include_bytes`). This is
/// `set_label_sorted`'s insert chain — one `Vec::insert` per include name
/// per many-side series.
/// Measured `rate_max` = **12** B per (series x include byte), both
/// skews. Shipped = `ceil(12 x 2 x 11/10)`.
pub const B_INCLUDE: u64 = 27;

/// **The post-aggregation byte cap** — the smallest power of two at or
/// above `max(X_chain, X_bin)`, where each `X` is the corresponding model
/// maximised over the leaf-gated feasible region **at the non-amplifying
/// corner** (`group_name_bytes = 0`, `include_bytes = 0`, both binary
/// operands at independent leaf budgets).
///
/// **What it buys, exactly:** every client-leaf-sourced stage input with
/// no `by`-name amplification and no `group_left/right` include
/// amplification is admitted. Nothing broader. A query carrying either
/// amplifier may be refused above the thresholds recorded as the O6/O7
/// ledger entries.
///
/// **What it is NOT.** It is not a worst-case proof. It is a bound
/// "measured-and-margined over a compile-enforced construct space, with a
/// clean refusal instead of an OOM at the boundary". Anyone reading it as
/// a worst-case guarantee is reading it wrong: the residual is a
/// distribution adversarial in a dimension no ladder varies, and the 2x
/// margin is what covers it.
///
/// **Deliberately not pinned from above.** No test asserts
/// `MAX_POST_AGG_BYTES < k x max(X)`. A change that REDUCES peak memory
/// (issue #245's Part C deletes two `BTreeMap` indexes and a `BTreeSet`
/// union from `combine_matrices`) must never redden CI; regenerating is
/// one command, `zz_witness_report`.
///
/// # The generator's numbers
///
/// ```text
/// s_min              = 616 bytes        (min over the four leaf entry slots)
/// N_max              = 435 771 series   (MAX_CLIENT_AGG_GROUP_BYTES / s_min)
/// stages             = 64               (min(MAX_DEPTH, MAX_QUERY_BYTES / 4))
/// X_chain            = 2 847 288 941 bytes   (argmax N = 546)
/// X_bin              = 5 970 118 644 bytes   (argmax N = 546)
/// MAX_POST_AGG_BYTES = 8 589 934 592 bytes   (8 GiB)
/// tightness ratio    = 1.4388           (printed, NOT gated)
/// ```
///
/// # O6 — the `by(...)` amplification threshold
///
/// `A_MIN = 597` total `by`-clause bytes, at `N = 435 558`; with
/// `A_NAME_MIN = 2` that is **at least 299 one-character `by` names**.
/// Strictly below `A_MIN`, refusal is impossible at ANY group count.
/// **Reachable**: 597 bytes fits inside `MAX_QUERY_BYTES = 131 072`.
///
/// # O7 — the `group_left/right(include)` amplification threshold
///
/// `AMP_MIN = 97 030 221`, the smallest `many.series x include_bytes`
/// PRODUCT at which the binary funnel can refuse, at `N_many = 546`.
/// **Reachable** within the query-text cap.
///
/// Both are the model-level thresholds; the funnel that turns them into
/// an actual refusal is issue #236's §4 and is not wired yet, so the
/// divergence ledger does not carry O6/O7 rows until it is.
pub const MAX_POST_AGG_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// A stage input's measured shape — the raw counted quantities the byte
/// model multiplies. One `O(series + label pairs)` pass over data that is
/// already materialised (`Vec::len` is `O(1)`, so **no per-point work**).
///
/// Fields are private and every accessor is derived from one exhaustive
/// destructure ([`StageInput::model_inputs`]), so a new axis cannot be
/// added without the paired-fixture isolation gate seeing it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StageInput {
    /// `N` — top-level series in the stage input.
    series: u64,
    /// `Σ labels.len()` over the input's series.
    label_pairs: u64,
    /// `Σ (k.len() + v.len())` — RAW label content bytes.
    label_bytes: u64,
    /// `Σ label_set_bytes(labels)` — the leaf's own charging vocabulary,
    /// so the feasible region's operand and the model's input are the
    /// same quantity.
    label_block_bytes: u64,
    /// The widest single series' label-pair count.
    max_series_pairs: u64,
    /// The longest single label VALUE, in bytes — read only through
    /// [`include_bytes`].
    max_value_bytes: u64,
    /// `P` — total points (1 per series for an instant vector).
    points: u64,
    /// The longest single series, in points.
    max_series_points: u64,
}

impl StageInput {
    /// Every model-relevant input, named, from ONE exhaustive destructure
    /// — adding a field to [`StageInput`] stops this compiling, which is
    /// what keeps §6's "every non-target input is byte-identical" gate
    /// from silently missing a new axis (the `AggCaps` `E0027` precedent).
    pub fn model_inputs(&self) -> [(&'static str, u64); 8] {
        let Self {
            series,
            label_pairs,
            label_bytes,
            label_block_bytes,
            max_series_pairs,
            max_value_bytes,
            points,
            max_series_points,
        } = *self;
        [
            ("series", series),
            ("label_pairs", label_pairs),
            ("label_bytes", label_bytes),
            ("label_block_bytes", label_block_bytes),
            ("max_series_pairs", max_series_pairs),
            ("max_value_bytes", max_value_bytes),
            ("points", points),
            ("max_series_points", max_series_points),
        ]
    }

    /// `N`.
    pub fn series(&self) -> u64 {
        self.series
    }

    /// `P`.
    pub fn points(&self) -> u64 {
        self.points
    }

    /// `Σ label_set_bytes(labels)` — the quantity the leaf's own
    /// group-byte charge is denominated in.
    pub fn label_block_bytes(&self) -> u64 {
        self.label_block_bytes
    }

    /// `Σ (k.len() + v.len())`.
    pub fn label_bytes(&self) -> u64 {
        self.label_bytes
    }

    /// `Σ labels.len()`.
    pub fn label_pairs(&self) -> u64 {
        self.label_pairs
    }

    /// The longest single label VALUE — read only through
    /// [`include_bytes`].
    pub fn max_value_bytes(&self) -> u64 {
        self.max_value_bytes
    }

    /// **Derivation seam, not a measurement.** Builds a [`StageInput`]
    /// from raw counted quantities so the cap derivation can evaluate the
    /// model at the feasible region's corners (`N` up to ~4.4e5 series,
    /// `P` up to 1.2e7 points) without materialising hundreds of MiB of
    /// synthetic series. Every production charge obtains its `StageInput`
    /// from [`measure_matrix`]/[`measure_vector`] — this constructor
    /// measures nothing and must never be used to authorise a charge.
    #[allow(clippy::too_many_arguments)]
    pub fn for_derivation(
        series: u64,
        label_pairs: u64,
        label_bytes: u64,
        label_block_bytes: u64,
        max_series_pairs: u64,
        max_value_bytes: u64,
        points: u64,
        max_series_points: u64,
    ) -> Self {
        Self {
            series,
            label_pairs,
            label_bytes,
            label_block_bytes,
            max_series_pairs,
            max_value_bytes,
            points,
            max_series_points,
        }
    }
}

/// Measures a matrix stage input. `s.points.len()` is `O(1)`, so the pass
/// is `O(series + label pairs)` and adds nothing per point.
pub fn measure_matrix(series: &[MatrixSeries]) -> StageInput {
    let mut m = StageInput {
        series: series.len() as u64,
        ..StageInput::default()
    };
    for s in series {
        measure_labels(&mut m, &s.labels);
        let pts = s.points.len() as u64;
        m.points = m.points.saturating_add(pts);
        m.max_series_points = m.max_series_points.max(pts);
    }
    m
}

/// Measures an instant-vector stage input — one point per series, which
/// is what makes `points == series` here.
pub fn measure_vector(series: &[VectorSample]) -> StageInput {
    let mut m = StageInput {
        series: series.len() as u64,
        points: series.len() as u64,
        max_series_points: u64::from(!series.is_empty()),
        ..StageInput::default()
    };
    for s in series {
        measure_labels(&mut m, &s.labels);
    }
    m
}

/// The label half of a `measure_*` pass, shared so the two entry points
/// cannot drift.
fn measure_labels(m: &mut StageInput, labels: &LabelSet) {
    let pairs = labels.len() as u64;
    m.label_pairs = m.label_pairs.saturating_add(pairs);
    m.max_series_pairs = m.max_series_pairs.max(pairs);
    for (k, v) in labels {
        m.label_bytes = m
            .label_bytes
            .saturating_add(k.len() as u64)
            .saturating_add(v.len() as u64);
        m.max_value_bytes = m.max_value_bytes.max(v.len() as u64);
    }
    m.label_block_bytes = m.label_block_bytes.saturating_add(label_set_bytes(labels));
}

/// `Σ_stages Σ_{name ∈ by(...)} (name.len() + 1)` — the grouping-name
/// amplifier, read off the QUERY TEXT and never off the data.
///
/// Counts **every** `by` name, including ones absent from the data: which
/// names are absent is unknowable before the stage runs, and counting all
/// of them is the conservative direction. `without(...)` contributes
/// nothing — `group_key`'s `Without` arm copies the series' own labels,
/// which the `W_PAIR`/`W_LABEL_BYTE` terms already price.
pub fn group_name_bytes(aggs: &[plan::VectorAggSpec]) -> u64 {
    let mut total: u64 = 0;
    for (_, grouping, _) in aggs {
        let Some(g) = grouping else { continue };
        if g.kind != GroupingKind::By {
            continue;
        }
        for name in &g.labels {
            total = total.saturating_add(name.len() as u64).saturating_add(1);
        }
    }
    total
}

/// `Σ_{ln ∈ include} (ln.len() + one.max_value_bytes + 1)` — the
/// `group_left/right(include)` amplifier, per many-side series.
///
/// Zero for a set operation ([`is_set_op`] returns before `include` is
/// read, `instant_join`'s first statement) and zero for one-to-one
/// matching (`matching.group.is_none()`).
pub fn include_bytes(matching: Option<&VectorMatching>, op: BinOp, one: &StageInput) -> u64 {
    if is_set_op(op) {
        return 0;
    }
    let Some(group) = matching.and_then(|m| m.group.as_ref()) else {
        return 0;
    };
    let include = match group {
        MatchGroup::Left(inc) | MatchGroup::Right(inc) => inc,
    };
    let mut total: u64 = 0;
    for ln in include {
        total = total
            .saturating_add(ln.len() as u64)
            .saturating_add(one.max_value_bytes)
            .saturating_add(1);
    }
    total
}

/// One term of [`post_agg_peak_bytes`]. `None` evaluates the shipped
/// model; every other variant zeroes exactly one coefficient.
///
/// **A test seam** (the `apply_vector_aggs_capped` / `group_bytes_cap`
/// precedent): §6's paired fixtures assert a term is NECESSARY by
/// showing the model WITHOUT it fails to cover the incremental bytes the
/// pair causes. Discriminating on increments is the only comparison that
/// survives independently-margined coefficients.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainTerm {
    /// The shipped model, no term suppressed.
    None,
    Series,
    Point,
    LabelByte,
    Pair,
    StageSeries,
    GroupName,
    ApproxTopk,
}

/// One term of [`binary_peak_bytes`]; see [`ChainTerm`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryTerm {
    /// The shipped model, no term suppressed.
    None,
    Series,
    Point,
    Label,
    Pair,
    Many,
    Include,
}

/// An upper bound on the heap bytes the post-aggregation chain may hold
/// SIMULTANEOUSLY, over and above its input. Contains no container
/// enumeration and no allocator model — every coefficient is measured
/// (§5.4) and margined.
///
/// All arithmetic saturates: `group_name_bytes` is read off unbounded-
/// until-#279 query text and stays large after it, so an amplified query
/// must resolve to `u64::MAX` (⇒ a clean refusal) and never wrap to a
/// small number that would admit an unbounded allocation.
pub fn post_agg_peak_bytes(m: &StageInput, aggs: &[plan::VectorAggSpec]) -> u64 {
    post_agg_peak_bytes_without(m, aggs, ChainTerm::None)
}

/// [`post_agg_peak_bytes`] with one coefficient forced to zero — §6's
/// necessity seam. The `match` is exhaustive with no `_` arm, so a new
/// term must be dispositioned here before it can ship.
pub fn post_agg_peak_bytes_without(
    m: &StageInput,
    aggs: &[plan::VectorAggSpec],
    drop: ChainTerm,
) -> u64 {
    let w = |term: ChainTerm, coeff: u64| if drop == term { 0 } else { coeff };
    let stages = aggs.len() as u64;
    let names = group_name_bytes(aggs);
    let approx = aggs
        .iter()
        .any(|(op, _, _)| matches!(op, VectorAggOp::ApproxTopk));

    let mut total = w(ChainTerm::Series, W_SERIES).saturating_mul(m.series);
    total = total.saturating_add(w(ChainTerm::Point, W_POINT).saturating_mul(m.points));
    total =
        total.saturating_add(w(ChainTerm::LabelByte, W_LABEL_BYTE).saturating_mul(m.label_bytes));
    total = total.saturating_add(w(ChainTerm::Pair, W_PAIR).saturating_mul(m.label_pairs));
    total = total.saturating_add(
        w(ChainTerm::StageSeries, W_STAGE_SERIES)
            .saturating_mul(m.series)
            .saturating_mul(stages),
    );
    total = total.saturating_add(
        w(ChainTerm::GroupName, W_GROUPNAME)
            .saturating_mul(m.series)
            .saturating_mul(names),
    );
    if approx {
        total = total.saturating_add(w(ChainTerm::ApproxTopk, W_APPROX_TOPK));
    }
    total
}

/// The same for a binary combination. `many`/`one` are chosen EXACTLY as
/// [`instant_join`] chooses them (`MatchGroup::Left` and one-to-one ⇒
/// many = lhs, `MatchGroup::Right` ⇒ many = rhs), so the include
/// amplification is never charged against the wrong side.
pub fn binary_peak_bytes(
    op: BinOp,
    matching: Option<&VectorMatching>,
    lhs: &StageInput,
    rhs: &StageInput,
) -> u64 {
    binary_peak_bytes_without(op, matching, lhs, rhs, BinaryTerm::None)
}

/// [`binary_peak_bytes`] with one coefficient forced to zero — §6's
/// necessity seam; see [`post_agg_peak_bytes_without`].
pub fn binary_peak_bytes_without(
    op: BinOp,
    matching: Option<&VectorMatching>,
    lhs: &StageInput,
    rhs: &StageInput,
    drop: BinaryTerm,
) -> u64 {
    let b = |term: BinaryTerm, coeff: u64| if drop == term { 0 } else { coeff };
    // `instant_join`'s own role assignment, transcribed.
    let group = matching.and_then(|m| m.group.as_ref());
    let (many, one) = match group {
        None | Some(MatchGroup::Left(_)) => (lhs, rhs),
        Some(MatchGroup::Right(_)) => (rhs, lhs),
    };
    let inc = include_bytes(matching, op, one);
    // The `B_MANY` term prices `instant_join`'s `many_matched:
    // HashMap<MatchSig, HashSet<MatchSig>>`, which exists ONLY on the
    // grouped arm — the one-to-one arm keeps a single
    // `HashSet<MatchSig>` and is priced by `B_SERIES`. Without this gate
    // the term takes the same value with and without a group modifier and
    // §6.4's difference-of-differences cancels it to zero, which is how
    // the gate found the omission.
    let many_series = if group.is_some() { many.series } else { 0 };

    let mut total =
        b(BinaryTerm::Series, B_SERIES).saturating_mul(lhs.series.saturating_add(rhs.series));
    total = total.saturating_add(
        b(BinaryTerm::Point, B_POINT).saturating_mul(lhs.points.saturating_add(rhs.points)),
    );
    total = total.saturating_add(
        b(BinaryTerm::Label, B_LABEL)
            .saturating_mul(lhs.label_bytes.saturating_add(rhs.label_bytes)),
    );
    total = total.saturating_add(
        b(BinaryTerm::Pair, B_PAIR).saturating_mul(lhs.label_pairs.saturating_add(rhs.label_pairs)),
    );
    total = total.saturating_add(b(BinaryTerm::Many, B_MANY).saturating_mul(many_series));
    total = total.saturating_add(
        b(BinaryTerm::Include, B_INCLUDE)
            .saturating_mul(many.series)
            .saturating_mul(inc),
    );
    total
}

/// Measures a range stage input — the chain's shape once the
/// `QueryResult` has been converted. `points.len()` on a `BTreeMap` is
/// `O(1)`, so the pass stays `O(series + label pairs)`.
fn measure_range(series: &[RangeSeries]) -> StageInput {
    let mut m = StageInput {
        series: series.len() as u64,
        ..StageInput::default()
    };
    for s in series {
        measure_labels(&mut m, &s.labels);
        let pts = s.points.len() as u64;
        m.points = m.points.saturating_add(pts);
        m.max_series_points = m.max_series_points.max(pts);
    }
    m
}

/// Measures an instant stage input — one point per series.
fn measure_instant(series: &[InstantSeries]) -> StageInput {
    let mut m = StageInput {
        series: series.len() as u64,
        points: series.len() as u64,
        max_series_points: u64::from(!series.is_empty()),
        ..StageInput::default()
    };
    for s in series {
        measure_labels(&mut m, &s.labels);
    }
    m
}

/// The funnel's proof token (issue #236 §4.1).
///
/// Everything in this module is private to it: [`Ledger`]'s fields, its
/// only constructors and the charge they perform. A `&Ledger` in a
/// signature is therefore PROOF that the stage's modelled bytes were
/// charged against the cap before that function could be reached — an
/// uncapped call site does not compile, in release exactly as in debug.
mod ledger {
    #[cfg(test)]
    use super::MAX_POST_AGG_BYTES;
    use super::{ReadError, StageInput, TooBroadReason};

    /// Proof that a stage's modelled bytes were charged, AND the budget
    /// that charge covers.
    ///
    /// The discharge is [`Drop`], so charge/discharge symmetry is
    /// structural on every return path (`?`, early return, unwind) — a
    /// leak is not expressible rather than merely tested for.
    #[derive(Debug)]
    pub(in crate::logql) struct Ledger<'a> {
        charged: &'a mut u64,
        bytes: u64,
        cap: u64,
        admitted_series: u64,
        admitted_points: u64,
    }

    impl<'a> Ledger<'a> {
        /// The chain funnel's charge: atomic check-then-add, and a FAILED
        /// charge does not mutate the counter (the `charge_group_bytes` /
        /// `traces::exec::ByteBudget::charge` precedent).
        pub(super) fn acquire(
            charged: &'a mut u64,
            m: &StageInput,
            bytes: u64,
            cap: u64,
        ) -> Result<Self, ReadError> {
            Self::acquire_for(charged, m.series(), m.points(), bytes, cap)
        }

        /// The binary funnel's charge. Both operands are live at once and
        /// both enter the join, so the admitted envelope is their SUM —
        /// the same quantity `binary_peak_bytes` is evaluated over.
        pub(super) fn acquire_binary(
            charged: &'a mut u64,
            lhs: &StageInput,
            rhs: &StageInput,
            bytes: u64,
            cap: u64,
        ) -> Result<Self, ReadError> {
            Self::acquire_for(
                charged,
                lhs.series().saturating_add(rhs.series()),
                lhs.points().saturating_add(rhs.points()),
                bytes,
                cap,
            )
        }

        fn acquire_for(
            charged: &'a mut u64,
            admitted_series: u64,
            admitted_points: u64,
            bytes: u64,
            cap: u64,
        ) -> Result<Self, ReadError> {
            let next = charged.saturating_add(bytes);
            if next > cap {
                return Err(ReadError::QueryTooBroad(
                    TooBroadReason::MetricPostAggBytes { bytes: next, cap },
                ));
            }
            *charged = next;
            Ok(Self {
                charged,
                bytes,
                cap,
                admitted_series,
                admitted_points,
            })
        }

        /// **UNCONDITIONAL** — no `debug_assert`, no `cfg`. Every
        /// classified ENTRY function calls this on the collection it is
        /// about to process; a shortfall is a clean refusal in release
        /// exactly as in debug, because a `debug_assert` is not an
        /// enforcement mechanism, it is a comment that runs in CI.
        ///
        /// `series` is a `Vec::len` (`O(1)`); `points` is `O(series)` for
        /// the two matrix entries and `O(1)` for the vector/join entries.
        /// Nothing per point is added anywhere.
        pub(super) fn admit(&self, series: u64, points: u64) -> Result<(), ReadError> {
            if series > self.admitted_series || points > self.admitted_points {
                return Err(ReadError::QueryTooBroad(
                    TooBroadReason::MetricPostAggBytes {
                        bytes: self.bytes,
                        cap: self.cap,
                    },
                ));
            }
            Ok(())
        }

        /// A token whose budget is the whole cap, for the in-module tests
        /// that drive one region function directly. It charges like any
        /// other acquisition — there is no unchecked constructor.
        #[cfg(test)]
        pub(super) fn for_test(charged: &'a mut u64) -> Self {
            Self::acquire_for(charged, u64::MAX, u64::MAX, 0, MAX_POST_AGG_BYTES)
                .expect("a zero-byte charge is always admitted")
        }
    }

    impl Drop for Ledger<'_> {
        fn drop(&mut self) {
            *self.charged = self.charged.saturating_sub(self.bytes);
        }
    }
}

use ledger::Ledger;

/// One binary operand's measured shape; a scalar/string/streams operand
/// carries no series and measures empty.
fn measure_operand(r: &QueryResult) -> StageInput {
    match r {
        QueryResult::Vector(items) => measure_vector(items),
        QueryResult::Matrix(items) => measure_matrix(items),
        _ => StageInput::default(),
    }
}

/// A range collection's admission: `Vec::len` plus one `O(series)` sum of
/// `BTreeMap::len`. Nothing per point.
fn admit_range(series: &[RangeSeries], l: &Ledger<'_>) -> Result<(), ReadError> {
    let points = series
        .iter()
        .fold(0u64, |a, s| a.saturating_add(s.points.len() as u64));
    l.admit(series.len() as u64, points)
}

/// An instant collection's admission — one point per series, so `O(1)`.
fn admit_instant(series: &[InstantSeries], l: &Ledger<'_>) -> Result<(), ReadError> {
    l.admit(series.len() as u64, series.len() as u64)
}

/// A join operand pair's admission — `O(1)`, run once per step on the
/// matrix path.
fn admit_join(lhs: &[JoinItem<'_>], rhs: &[JoinItem<'_>], l: &Ledger<'_>) -> Result<(), ReadError> {
    let n = (lhs.len() as u64).saturating_add(rhs.len() as u64);
    l.admit(n, n)
}

/// `s_min` — the smallest per-entry byte charge any of the four client
/// leaf group paths levies, computed from live `size_of` through the
/// leaf's own [`map_entry_bytes`]/[`grown_alloc_bytes`] vocabulary with
/// the shortest possible key.
///
/// It is the feasible region's series operand: a leaf that admitted `N`
/// groups paid at least `s_min * N` of its
/// [`MAX_CLIENT_AGG_GROUP_BYTES`] budget, so `N <=
/// (MAX_CLIENT_AGG_GROUP_BYTES - L̂) / s_min`. Derived, never chosen —
/// if a slot's layout changes the region moves with it.
pub fn leaf_min_entry_bytes() -> u64 {
    [
        MUT_GROUP_SLOT,
        INSTANT_GROUP_SLOT,
        FP_GROUP_SLOT,
        SERIES_OUT_SLOT,
    ]
    .into_iter()
    .map(|slot| map_entry_bytes(slot).saturating_add(grown_alloc_bytes(0)))
    .min()
    .expect("four leaf entry slots")
}

/// The chain over an already-converted RANGE vector. Entry-class: it
/// admits the collection it is handed, then applies the stages
/// innermost-first (the `.rev()` matching `MetricPlan.vector_aggs`'
/// outer-first order).
fn run_range_chain(
    mut series: Vec<RangeSeries>,
    aggs: &[plan::VectorAggSpec],
    l: &Ledger<'_>,
) -> Result<Vec<RangeSeries>, ReadError> {
    admit_range(&series, l)?;
    for (op, grouping, param) in aggs.iter().rev() {
        series = group_range(series, *op, grouping.as_ref(), *param, l)?;
    }
    Ok(series)
}

/// The chain over an already-converted INSTANT vector; see
/// [`run_range_chain`].
fn run_instant_chain(
    mut series: Vec<InstantSeries>,
    aggs: &[plan::VectorAggSpec],
    l: &Ledger<'_>,
) -> Result<Vec<InstantSeries>, ReadError> {
    admit_instant(&series, l)?;
    for (op, grouping, param) in aggs.iter().rev() {
        series = group_instant(series, *op, grouping.as_ref(), *param, l)?;
    }
    Ok(series)
}

/// Charges, then runs the chain over an already-converted range vector.
///
/// The SQL metric path holds `Vec<RangeSeries>` directly and reaching
/// [`apply_vector_aggs`] from there would cost a `BTreeMap -> Vec ->
/// BTreeMap` round trip per point on the commonest metric shape. This is
/// the same funnel — measure, charge, run — entered one conversion
/// earlier.
pub(in crate::logql) fn charged_range_chain(
    series: Vec<RangeSeries>,
    aggs: &[plan::VectorAggSpec],
    cap: u64,
) -> Result<Vec<RangeSeries>, ReadError> {
    if aggs.is_empty() {
        return Ok(series);
    }
    let m = measure_range(&series);
    let bytes = post_agg_peak_bytes(&m, aggs);
    let mut charged = 0u64;
    let l = Ledger::acquire(&mut charged, &m, bytes, cap)?;
    run_range_chain(series, aggs, &l)
}

/// [`charged_range_chain`] for an instant vector.
pub(in crate::logql) fn charged_instant_chain(
    series: Vec<InstantSeries>,
    aggs: &[plan::VectorAggSpec],
    cap: u64,
) -> Result<Vec<InstantSeries>, ReadError> {
    if aggs.is_empty() {
        return Ok(series);
    }
    let m = measure_instant(&series);
    let bytes = post_agg_peak_bytes(&m, aggs);
    let mut charged = 0u64;
    let l = Ledger::acquire(&mut charged, &m, bytes, cap)?;
    run_instant_chain(series, aggs, &l)
}

/// Applies an outer-to-inner vector-aggregation chain to a metric result,
/// charged against [`MAX_POST_AGG_BYTES`] before it allocates.
///
/// `pub` like [`run_pipeline_rows`]: the hermetic golden suite
/// (`tests/logql_metric_agg_golden.rs`) pins the reducer/selection
/// semantics from outside the crate.
pub fn apply_vector_aggs(
    result: QueryResult,
    aggs: &[plan::VectorAggSpec],
) -> Result<QueryResult, ReadError> {
    let mut charged = 0u64;
    apply_vector_aggs_capped(&mut charged, result, aggs, MAX_POST_AGG_BYTES)
}

/// The cap seam (the `group_bytes_cap`/`retention_cap` precedent): exists
/// ONLY so tests can drive the boundary, and takes the counter by
/// reference so the charge/discharge symmetry is observable.
///
/// **The order is the contract.** (1) an empty chain returns its input
/// before any measurement and before the conversion — which also deletes
/// an `O(points)` `Vec -> BTreeMap -> Vec` round trip from every
/// no-aggregation result; (2) measurement happens on the
/// `MatrixSeries`/`VectorSample` shape, so the conversion is INSIDE its
/// own charge; (3) a refused charge means nothing is converted, nothing
/// is grouped and no token exists, so the chain cannot run; (4) every
/// Entry function admits its own input unconditionally; (5) the token
/// drops and discharges.
pub fn apply_vector_aggs_capped(
    charged: &mut u64,
    result: QueryResult,
    aggs: &[plan::VectorAggSpec],
    cap: u64,
) -> Result<QueryResult, ReadError> {
    if aggs.is_empty() {
        return Ok(result);
    }
    match result {
        QueryResult::Matrix(items) => {
            let m = measure_matrix(&items);
            let bytes = post_agg_peak_bytes(&m, aggs);
            let l = Ledger::acquire(charged, &m, bytes, cap)?;
            let series: Vec<RangeSeries> = items
                .into_iter()
                .map(|s| RangeSeries {
                    labels: s.labels,
                    points: s.points.into_iter().collect(),
                })
                .collect();
            let series = run_range_chain(series, aggs, &l)?;
            Ok(QueryResult::Matrix(
                series
                    .into_iter()
                    .map(|s| MatrixSeries {
                        labels: s.labels,
                        points: s.points.into_iter().collect(),
                    })
                    .collect(),
            ))
        }
        QueryResult::Vector(items) => {
            let m = measure_vector(&items);
            let bytes = post_agg_peak_bytes(&m, aggs);
            let l = Ledger::acquire(charged, &m, bytes, cap)?;
            let series: Vec<InstantSeries> = items
                .into_iter()
                .map(|s| InstantSeries {
                    labels: s.labels,
                    value: s.value,
                })
                .collect();
            let series = run_instant_chain(series, aggs, &l)?;
            Ok(QueryResult::Vector(
                series
                    .into_iter()
                    .map(|s| VectorSample {
                        labels: s.labels,
                        value: s.value,
                    })
                    .collect(),
            ))
        }
        // A vector aggregation over a scalar is rejected at plan time
        // (`build_metric_node`); passthrough is defensive only.
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::super::charge::AggCaps;
    use super::super::client_agg::RangeSlideState;
    use super::super::fold::{FoldGrid, VectorAggFold};
    use super::super::pipeline::CompiledPipeline;
    use super::super::plan::{ClientAgg, ClientValue};
    use super::*;
    use crate::logql::MAX_METRIC_RESULT_POINTS;
    use crate::logql::testkit::*;
    use pulsus_logql::RangeAggOp;

    #[test]
    fn group_range_sum_by_reduces_matching_steps() {
        let mut a = BTreeMap::new();
        a.insert(0i64, 1.0);
        a.insert(60, 2.0);
        let mut b = BTreeMap::new();
        b.insert(0i64, 3.0);
        let series = vec![
            RangeSeries {
                labels: vec![("service_name".to_string(), "checkout".to_string())],
                points: a,
            },
            RangeSeries {
                labels: vec![("service_name".to_string(), "checkout".to_string())],
                points: b,
            },
        ];
        let grouping = Grouping {
            kind: GroupingKind::By,
            labels: vec!["service_name".to_string()],
        };
        let mut charged = 0u64;
        let led = Ledger::for_test(&mut charged);
        let grouped =
            group_range(series, VectorAggOp::Sum, Some(&grouping), None, &led).expect("grouped");
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].points.get(&0), Some(&4.0));
        assert_eq!(grouped[0].points.get(&60), Some(&2.0));
    }

    /// Issue #236 Part B — the reduction-order pin is a GATE, not a
    /// convention.
    ///
    /// Welford is order-sensitive, so a hash-walk member order makes
    /// `avg`/`stddev`/`stdvar` vary between runs. This drives
    /// `group_instant`/`group_range` over EVERY permutation of a dataset
    /// known to discriminate (`{2,4,6,8}`: 20 of its 24 orders give
    /// `stdvar` exactly `5.0`, the other 4 give `4.999999999999999`) and
    /// asserts one single output value across all of them.
    ///
    /// Without the pin this fails on 4 of the 24 inputs; the assertion
    /// therefore cannot pass vacuously, and the unit is PERMUTATIONS OF
    /// THE INPUT SERIES VECTOR, not runs of the process — which makes it
    /// deterministic in CI rather than a 1-in-6 flake.
    #[test]
    fn the_reduction_order_pin_makes_welford_input_order_independent() {
        /// Every permutation of `[0, 1, 2, 3]` (Heap's algorithm,
        /// iterative). Written out rather than pulling in a combinatorics
        /// dependency the plan does not specify.
        fn permutations_of_four() -> Vec<[usize; 4]> {
            let mut out = Vec::with_capacity(24);
            let mut a = [0usize, 1, 2, 3];
            let mut c = [0usize; 4];
            out.push(a);
            let mut i = 1;
            while i < 4 {
                if c[i] < i {
                    if i % 2 == 0 {
                        a.swap(0, i);
                    } else {
                        a.swap(c[i], i);
                    }
                    out.push(a);
                    c[i] += 1;
                    i = 1;
                } else {
                    c[i] = 0;
                    i += 1;
                }
            }
            out
        }

        let vals = [2.0f64, 4.0, 6.0, 8.0];
        let perms = permutations_of_four();
        assert_eq!(
            perms.len(),
            24,
            "the unit is PERMUTATIONS of the input vector"
        );
        // Distinct label sets, so the pin has a total order to impose and
        // every permutation is a genuinely different input vector.
        //
        // **Two label models, and the second is the one that matters**
        // (review round 1 `[medium]`). With DISTINCT label sets the sort
        // key alone is total, so the sweep could not see a pin that
        // ordered only by labels — and equal label sets are exactly where
        // Welford stays order-dependent, because `LabelSet::cmp` returns
        // `Equal` and a stable sort then keeps the INPUT order. `Shared`
        // is the case the sweep exists for, and it was the case the
        // sweep excluded.
        let distinct =
            |v: f64| -> LabelSet { vec![("host".to_string(), format!("h{}", v as u64))] };
        let identical = |_: f64| -> LabelSet { vec![("host".to_string(), "same".to_string())] };
        let models: [(&str, &dyn Fn(f64) -> LabelSet); 2] = [
            ("distinct label sets", &distinct),
            ("EQUAL label sets", &identical),
        ];

        let mut charged = 0u64;
        let led = Ledger::for_test(&mut charged);
        for (model, labels_for) in models {
            for op in [VectorAggOp::Stdvar, VectorAggOp::Stddev, VectorAggOp::Avg] {
                let mut instant_seen: BTreeSet<u64> = BTreeSet::new();
                let mut range_seen: BTreeSet<u64> = BTreeSet::new();

                for perm in &perms {
                    let instant: Vec<InstantSeries> = perm
                        .iter()
                        .map(|&i| InstantSeries {
                            labels: labels_for(vals[i]),
                            value: vals[i],
                        })
                        .collect();
                    // Bare aggregation (`grouping: None`) collapses all four
                    // into ONE group, which is what exposes member order.
                    let out = group_instant(instant, op, None, None, &led).expect("grouped");
                    assert_eq!(out.len(), 1);
                    instant_seen.insert(out[0].value.to_bits());

                    let range: Vec<RangeSeries> = perm
                        .iter()
                        .map(|&i| RangeSeries {
                            labels: labels_for(vals[i]),
                            points: BTreeMap::from([(0i64, vals[i])]),
                        })
                        .collect();
                    let out = group_range(range, op, None, None, &led).expect("grouped");
                    assert_eq!(out.len(), 1);
                    range_seen.insert(out[0].points[&0].to_bits());
                }

                assert_eq!(
                    instant_seen.len(),
                    1,
                    "group_instant {op:?} over {model} produced {} distinct values across the 24 \
                 member orders — the reduction-order pin is not holding: {instant_seen:?}",
                    instant_seen.len()
                );
                assert_eq!(
                    range_seen.len(),
                    1,
                    "group_range {op:?} over {model} produced {} distinct values across the 24 \
                 member orders — the reduction-order pin is not holding: {range_seen:?}",
                    range_seen.len()
                );
                // Instant and range must also agree with EACH OTHER — the
                // whole point of both routing through `VectorAccum`.
                assert_eq!(
                    instant_seen, range_seen,
                    "{op:?} over {model}: instant/range disagree"
                );
            }
        }

        // ...and the pinned value is the one the committed corpus
        // captured from the reference (the 20-of-24 majority basin).
        let instant: Vec<InstantSeries> = vals
            .iter()
            .map(|v| InstantSeries {
                labels: distinct(*v),
                value: *v,
            })
            .collect();
        let out = group_instant(instant, VectorAggOp::Stdvar, None, None, &led).expect("grouped");
        assert_eq!(
            out[0].value.to_bits(),
            5.0f64.to_bits(),
            "the pin must reproduce b4_vector_aggs.test's captured stdvar"
        );
    }

    /// Drives `input` through a fold, in the given order.
    fn drive_fold(
        spec: &plan::VectorAggSpec,
        grid: FoldGrid,
        input: &[FoldInput],
    ) -> Vec<MatrixSeries> {
        let mut fold = VectorAggFold::new(spec, grid, MAX_METRIC_RESULT_POINTS, u64::MAX)
            .expect("this spec folds");
        for (labels, points) in input {
            fold.push_series(labels, points).expect("on-grid points");
        }
        fold.finish()
    }

    /// Drives the same input through `select_k_range`, the materialising
    /// implementation the fold must reproduce.
    fn drive_select_k_range(
        op: VectorAggOp,
        grouping: Option<&Grouping>,
        param: Option<f64>,
        input: &[FoldInput],
    ) -> Vec<(LabelSet, Vec<(i64, f64)>)> {
        let series: Vec<RangeSeries> = input
            .iter()
            .map(|(labels, points)| RangeSeries {
                labels: labels.clone(),
                points: points.iter().copied().collect(),
            })
            .collect();
        let mut charged = 0u64;
        let led = Ledger::for_test(&mut charged);
        select_k_range(series, op, grouping, param, &led)
            .expect("selected")
            .into_iter()
            .map(|s| (s.labels, s.points.into_iter().collect()))
            .collect()
    }

    fn as_pairs(series: Vec<MatrixSeries>) -> Vec<(LabelSet, Vec<(i64, f64)>)> {
        series.into_iter().map(|s| (s.labels, s.points)).collect()
    }

    /// AC 9 — the SELECTION ORDER, not merely the selected set.
    ///
    /// `SelectFold`'s output SEQUENCE (survivors in original push order,
    /// each survivor's points ascending) and its values must be identical
    /// to `select_k_range` over the same explicit input vector, including
    /// the four adversarial shapes: equal values across series, a group
    /// where every value is equal, two series with identical EMPTY label
    /// sets (which is what makes the series-id tiebreak reachable), and
    /// NaN candidates.
    #[test]
    fn select_fold_reproduces_select_k_range_sequence_and_values() {
        let grid = FoldGrid {
            start: 0,
            step: 10,
            kmax: 2,
        };
        let pts = |vals: [f64; 3]| vec![(0i64, vals[0]), (10, vals[1]), (20, vals[2])];
        let cases: Vec<(&str, Vec<FoldInput>)> = vec![
            (
                "distinct values",
                vec![
                    (fold_labels(&[("h", "a")]), pts([1.0, 5.0, 3.0])),
                    (fold_labels(&[("h", "b")]), pts([4.0, 2.0, 9.0])),
                    (fold_labels(&[("h", "c")]), pts([7.0, 8.0, 0.5])),
                ],
            ),
            (
                "equal values ACROSS series (label tiebreak)",
                vec![
                    (fold_labels(&[("h", "c")]), pts([1.0, 1.0, 1.0])),
                    (fold_labels(&[("h", "a")]), pts([1.0, 1.0, 1.0])),
                    (fold_labels(&[("h", "b")]), pts([1.0, 1.0, 1.0])),
                ],
            ),
            (
                "an all-equal group of two",
                vec![
                    (fold_labels(&[("h", "a")]), pts([2.0, 2.0, 2.0])),
                    (fold_labels(&[("h", "b")]), pts([2.0, 2.0, 2.0])),
                ],
            ),
            (
                "two series with IDENTICAL EMPTY label sets (series-id tiebreak)",
                vec![
                    (Vec::new(), pts([3.0, 1.0, 4.0])),
                    (Vec::new(), pts([1.0, 5.0, 9.0])),
                    (fold_labels(&[("h", "z")]), pts([2.0, 6.0, 5.0])),
                ],
            ),
            (
                // THE discriminating shape for the series-id tiebreak.
                // Two series that tie need no tiebreak (both the fold and
                // the stable sort keep the earlier one), and three that
                // tie at EVERY step are indistinguishable in the output.
                // It takes THREE series with identical labels tying at
                // ONE step and differing at another for the choice to be
                // observable: at step 0 all three hold 2.0 with `k = 2`,
                // and which two survive decides who owns that point.
                "three IDENTICAL-label series tying at one step only",
                vec![
                    (Vec::new(), pts([2.0, 1.0, 1.0])),
                    (Vec::new(), pts([2.0, 2.0, 2.0])),
                    (Vec::new(), pts([2.0, 3.0, 3.0])),
                ],
            ),
            (
                "NaN candidates rank last in BOTH directions",
                vec![
                    (fold_labels(&[("h", "a")]), pts([f64::NAN, 5.0, 1.0])),
                    (fold_labels(&[("h", "b")]), pts([5.0, f64::NAN, 2.0])),
                    (fold_labels(&[("h", "c")]), pts([1.0, 2.0, f64::NAN])),
                ],
            ),
            (
                "every candidate NaN",
                vec![
                    (fold_labels(&[("h", "a")]), pts([f64::NAN; 3])),
                    (fold_labels(&[("h", "b")]), pts([f64::NAN; 3])),
                ],
            ),
        ];

        let bits = |v: Vec<(LabelSet, Vec<(i64, f64)>)>| -> Vec<(LabelSet, Vec<(i64, u64)>)> {
            v.into_iter()
                .map(|(l, p)| (l, p.into_iter().map(|(t, x)| (t, x.to_bits())).collect()))
                .collect()
        };

        for (name, input) in &cases {
            for op in [VectorAggOp::Topk, VectorAggOp::Bottomk] {
                for k in [1.0f64, 2.0, 3.0, 9.0] {
                    let folded = bits(as_pairs(drive_fold(&(op, None, Some(k)), grid, input)));
                    let materialised = bits(drive_select_k_range(op, None, Some(k), input));
                    assert_eq!(
                        folded, materialised,
                        "{op:?}({k}) over `{name}`: the fold must reproduce \
                         select_k_range's SEQUENCE and values"
                    );
                }
            }
            // ...and with a grouping, so the group key is exercised too.
            let grouping = Grouping {
                kind: GroupingKind::By,
                labels: vec!["h".to_string()],
            };
            let folded = bits(as_pairs(drive_fold(
                &(VectorAggOp::Topk, Some(grouping.clone()), Some(1.0)),
                grid,
                input,
            )));
            let materialised = bits(drive_select_k_range(
                VectorAggOp::Topk,
                Some(&grouping),
                Some(1.0),
                input,
            ));
            assert_eq!(folded, materialised, "topk(1) by (h) over `{name}`");
        }
    }

    /// Issue #236 Part B — **the reduction-order pin extends to the
    /// fold**, and the gate is exhaustive rather than sampled.
    ///
    /// `RangeSlideState::emit` puts the leaf's series in label-set order
    /// before folding, which is the same total order `pin_reduction_order`
    /// imposes on the materialising path. This drives `emit` over ALL 24
    /// permutations of the discriminating `{2,4,6,8}` dataset (unit:
    /// permutations of the emitted series vector, not runs of the
    /// process) and asserts one single value — and that it is the value
    /// `group_range` produces over the same data.
    ///
    /// Emptying the sort makes this fail on 4 of the 24 inputs, every
    /// run.
    #[test]
    fn the_reduction_order_pin_extends_to_the_fold() {
        fn permutations_of_four() -> Vec<[usize; 4]> {
            let mut out = Vec::with_capacity(24);
            let mut a = [0usize, 1, 2, 3];
            let mut c = [0usize; 4];
            out.push(a);
            let mut i = 1;
            while i < 4 {
                if c[i] < i {
                    if i % 2 == 0 {
                        a.swap(0, i);
                    } else {
                        a.swap(c[i], i);
                    }
                    out.push(a);
                    c[i] += 1;
                    i = 1;
                } else {
                    c[i] = 0;
                    i += 1;
                }
            }
            out
        }

        let vals = [2.0f64, 4.0, 6.0, 8.0];
        let perms = permutations_of_four();
        assert_eq!(
            perms.len(),
            24,
            "the unit is PERMUTATIONS of the emitted series vector"
        );
        let client = ClientAgg {
            pipeline: vec![],
            value: ClientValue::Count,
            range_op: RangeAggOp::CountOverTime,
            param: None,
            absent_labels: vec![],
        };
        let compiled = CompiledPipeline::compile(&client.pipeline).unwrap();
        let meta = slide_meta(1, r#"{"app":"a"}"#);
        let window = slide_window(0, 0, 10, 10);

        for op in [VectorAggOp::Stdvar, VectorAggOp::Stddev, VectorAggOp::Avg] {
            let mut seen: BTreeSet<u64> = BTreeSet::new();
            for perm in &perms {
                let mut state =
                    RangeSlideState::new(&compiled, &meta, &client, window, None, AggCaps::DEFAULT)
                        .expect("state");
                state.attach_fold(&(op, None, None));
                assert_eq!(state.folded_aggs(), 1);
                let emitted: Vec<MatrixSeries> = perm
                    .iter()
                    .map(|&i| MatrixSeries {
                        labels: vec![("host".to_string(), format!("h{}", vals[i] as u64))],
                        points: vec![(0i64, vals[i])],
                    })
                    .collect();
                let QueryResult::Matrix(out) = state.emit(emitted).expect("emit") else {
                    panic!("a range leaf emits a matrix");
                };
                assert_eq!(out.len(), 1, "a bare aggregation collapses to one series");
                seen.insert(out[0].points[0].1.to_bits());
            }
            assert_eq!(
                seen.len(),
                1,
                "the fold produced {} distinct {op:?} values across the 24 emission \
                 orders — the reduction-order pin is not holding in the fold: {seen:?}",
                seen.len()
            );
            // ...and it is the SAME value the materialising path gives.
            let mut charged = 0u64;
            let led = Ledger::for_test(&mut charged);
            let materialised = group_range(
                vals.iter()
                    .map(|v| RangeSeries {
                        labels: vec![("host".to_string(), format!("h{}", *v as u64))],
                        points: BTreeMap::from([(0i64, *v)]),
                    })
                    .collect(),
                op,
                None,
                None,
                &led,
            )
            .expect("materialised");
            assert_eq!(
                seen.iter().copied().collect::<Vec<u64>>(),
                vec![materialised[0].points[&0].to_bits()],
                "{op:?}: folded and materialised must be the same bits"
            );
        }
    }
}
