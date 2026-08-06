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
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};

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
/// index, its match signatures or its output — and **after** every
/// class-(P) semantic refusal that this charge could preempt has been
/// decided (issue #290).
///
/// The order is structural, not a convention. [`decide_binary`] MOVES
/// both operands into a `BinaryDecided` — together with the `op` and the
/// `matching` the decision was made under — and `Ledger::acquire_binary`
/// consumes that in turn, handing back nothing loose: its whole result
/// is one `BinaryCharged`, which owns the charge, the operands and the
/// context, has private fields, and is what [`join_decided`] must be
/// given BY VALUE. So an operand cannot reach the join except through a
/// charge the decision preceded, the decision cannot be made over a
/// different pair (there is no second pair to name), and THIS CALL
/// cannot join under a different matcher or against a different budget,
/// because there is no parameter left to pass either through. That last
/// one is a bound on the call and not on the module — see
/// [`join_decided`] for what that does and does not reach. See
/// docs/architecture.md §5.6.
///
/// **What "that this charge could preempt" excludes.** The (P1) join
/// refusals are decided only when this seam's charge would actually
/// refuse — see [`decide_binary`]'s guard. When the charge admits there
/// is no budget rejection for anything to be preempted by: the join runs
/// and the caller gets the join's own answer, which is the answer the
/// preflight exists to let through. The (P0) shape refusals are decided
/// unconditionally.
pub fn combine_binary_capped(
    charged: &mut u64,
    op: BinOp,
    return_bool: bool,
    matching: Option<&VectorMatching>,
    lhs: QueryResult,
    rhs: QueryResult,
    cap: u64,
) -> Result<QueryResult, ReadError> {
    let decided = decide_binary(op, matching, lhs, rhs, *charged, cap)?;
    join_decided(Ledger::acquire_binary(charged, decided, cap)?, return_bool)
}

/// The join itself, over a charge, operands and a decision context that
/// arrive as ONE value out of [`Ledger::acquire_binary`] and could not
/// have come from anywhere else.
///
/// **Why this is a separate function** (issue #290, review round 2's
/// `[medium]`): (P1)'s answer is a function of `op` and `matching`, so
/// joining under a context the refusals were not decided under is the
/// same defect as joining operands that were not decided at all. Keeping
/// the join in [`combine_binary_capped`] left that context nameable —
/// the caller's own parameters were still in scope, and rebinding them
/// over the decision's would have compiled and passed.
///
/// **Why it takes one moved value** (review round 3's `[medium]`):
/// splitting the function was not enough on its own. `op` and `matching`
/// are `Copy`, so while `join_decided` still had parameters of those
/// types, a caller that renamed the destructured bindings could pass its
/// own alongside the decision's operands — and that mutant compiled.
/// `Copy` was never the obstacle; carrying the context in loose
/// arguments was. It now arrives inside [`BinaryCharged`], whose fields
/// are private to `mod ledger` and whose only constructor is the charge.
/// The three caller-side ways to substitute a context are each a compile
/// error, measured on this tree: handing it in beside the decision is
/// `E0061` (this function takes 2 arguments), overwriting the decision's
/// with `charged.matching = ..` is `E0616`, and rebuilding the value
/// around the caller's is `E0451`.
///
/// The charge travels in the same value for the same reason — a join
/// against a ledger the operands were not charged on is context
/// substitution too, one layer down. It WAS expressible before: at
/// `9695900` a `Ledger::acquire` over a private counter and an unlimited
/// cap could be passed to this function's `&Ledger` parameter and it
/// compiled. There is no such parameter now.
///
/// **What this bounds** (review rounds 4 and 6's `[medium]`). The bound
/// is on the CALL, and it is checkable: *no caller can substitute the
/// context by calling this function* — every argument route into it is
/// a compile error (`E0061`/`E0616`/`E0451`, above), and `return_bool`
/// is the only parameter left.
///
/// It is not a bound on the module around it. [`BinaryCharged`]'s
/// fields are private to `mod ledger` and its consuming exit
/// [`BinaryCharged::into_join_context`] is `pub(super)`, so code
/// elsewhere in this file can unpack a carrier and join under its own
/// context without calling this function at all. No tighter visibility
/// is available where the carrier stands, and the reason is mechanical
/// rather than a missing annotation: a visibility may name only an
/// ANCESTOR of the item's own module, so an exit defined in `mod ledger`
/// can be restricted to `ledger` itself or to
/// `post_agg`/`logql`/`crate`, and to nothing else. Both alternatives
/// were measured on this tree: naming a sibling module
/// (`pub(in crate::logql::post_agg::preflight)`) is `E0742`
/// ("visibilities can only be restricted to ancestor modules"), and
/// making the exit private is `E0624` at this function's own
/// destructuring, since this function is not inside `mod ledger`.
/// Moving this dispatch into `mod ledger` so the exit could be private
/// is not taken: it would put result-shape query logic inside the module
/// that is the budget and its proof tokens.
///
/// `return_bool` is still a parameter, and it is NOT a piece of the
/// decision context that was left outside the carrier: nothing is
/// decided under it. [`decide_binary`] takes the operands, the
/// operator, the matcher and the budget, and `return_bool` is not among
/// them; neither is it among [`binary_peak_bytes`]' arguments. So there
/// is no `return_bool` the refusals were decided or the charge priced
/// under, for this call to be able to disagree with. What it selects is
/// what a comparison EMITS: under `bool` every matched pair yields a
/// sample valued 0/1, and without it only the pairs the comparison holds
/// for survive, carrying the value their own arm keeps — the source left
/// operand's in [`instant_join`], and in [`scalar_apply`] the VECTOR
/// side's whichever side that is (`5 < vec(10)` keeps `10`).
fn join_decided(
    charged: BinaryCharged<'_, '_>,
    return_bool: bool,
) -> Result<QueryResult, ReadError> {
    let (ledger, op, matching, lhs, rhs) = charged.into_join_context();
    let led = &ledger;
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
                led,
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
                led,
            )
        }
        (QueryResult::Vector(a), QueryResult::Vector(b)) => Ok(QueryResult::Vector(
            combine_vectors(op, return_bool, matching, a, b, led)?,
        )),
        (QueryResult::Matrix(a), QueryResult::Matrix(b)) => Ok(QueryResult::Matrix(
            combine_matrices(op, return_bool, matching, a, b, led)?,
        )),
        // Both operands evaluate under the same QuerySpec, so a
        // vector/matrix mix (or a streams/string operand) is structurally
        // impossible — defensive named error, never a panic.
        _ => Err(incompatible_types_error()),
    }
}

/// The operand-shape mismatch refusal, in ONE constructor so the arm
/// [`combine_binary_capped`] keeps as defence in depth and the class-(P)
/// preflight that now decides it above the charge cannot drift apart
/// (issue #290).
fn incompatible_types_error() -> ReadError {
    ReadError::PipelineInvalid {
        reason: "binary operation over incompatible result types".to_string(),
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
    // Prometheus/Loki wording for a duplicate grouped output identity.
    //
    // REACHABLE with distinct many-side series (issue #290 corrects the
    // former "unreachable" claim here): when the include copy DROPS a
    // label the many-side series differed on, two distinct many series
    // collapse onto one output identity. Witness, shipped and green:
    // `group_left_include_collapsing_distinct_many_labels_is_grouping_unique_error`
    // (crates/pulsus-read/tests/logql_metric_agg_golden.rs), oracle-
    // pinned byte-identical against `grafana/loki:3.4.2`.
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
/// no `by`-name amplification, no `group_left/right` include
/// amplification, and no `label_replace` template amplification is
/// admitted. Nothing broader. A query carrying one of the three
/// amplifiers may be refused above its threshold — O6/O7/O8 below.
///
/// **What it is NOT.** It is not a worst-case proof. It is a bound
/// "measured-and-margined over a compile-enforced construct space, with a
/// clean refusal instead of an OOM at the boundary". Anyone reading it as
/// a worst-case guarantee is reading it wrong: the residual is a
/// distribution adversarial in a dimension no ladder varies, and the 2x
/// margin is what covers it.
///
/// **Deliberately not pinned from above.** No test asserts
/// `MAX_POST_AGG_BYTES < k x max(X)` — a TIGHTNESS bound would redden a
/// change that REDUCES peak memory (issue #245's Part C deletes two
/// `BTreeMap` indexes and a `BTreeSet` union from `combine_matrices`),
/// and such a change must never redden CI for being an improvement.
/// The published FIGURES below are a different matter (issue #276 fix
/// round 3): every one of them is pinned to the derivation by
/// `the_published_figures_are_pinned_to_the_derivation`, which reads
/// them out of THIS comment (and O8's ledger row) — equality, not
/// tightness. A coefficient change now moves prose and derivation in
/// the same commit; regeneration stays one command,
/// `zz_witness_report`, made mandatory instead of optional.
///
/// # The generator's numbers
///
/// ```text
/// s_min              = 616 bytes        (min over the four leaf entry slots)
/// N_max              = 435 771 series   (MAX_CLIENT_AGG_GROUP_BYTES / s_min)
/// stages             = 64               (min(MAX_DEPTH, MAX_QUERY_BYTES / 4))
/// X_chain            = 2 847 288 941 bytes   (argmax N = 546)
/// X_bin              = 5 970 118 644 bytes   (argmax N = 546)
/// X_lr (L = 0)       = 2 843 340 769 bytes   (argmax N = 546)
/// MAX_POST_AGG_BYTES = 8 589 934 592 bytes   (8 GiB)
/// tightness ratio    = 1.4388           (value pinned; no BOUND gated)
/// ```
///
/// # O6 — the `by(...)` amplification threshold
///
/// `A_MIN = 597` total `by`-clause bytes, at `N = 435 558`; with
/// `A_NAME_MIN = 2` that is **at least 299 one-character `by` names**.
/// Strictly below `A_MIN`, refusal is impossible at ANY group count.
/// **Reachable**: `A_MIN` fits inside `MAX_QUERY_BYTES = 131 072` (the
/// figure is stated ONCE above — a second copy would drift eventually,
/// fix round 4's `[high]`).
///
/// # O7 — the `group_left/right(include)` amplification threshold
///
/// `AMP_MIN = 97 030 221`, the smallest `many.series x include_bytes`
/// PRODUCT at which the binary funnel can refuse, at `N_many = 546`.
/// **Reachable** within the query-text cap.
///
/// # O8 — the `label_replace` template amplification threshold
///
/// [`label_replace_peak_bytes`] charges `2 x W_LABEL_BYTE x L` bytes
/// per series, `L = dst.len() + replacement.len() + #'$' x
/// max_value_bytes` (each `$` in the template can expand to the widest
/// label value in the input). `L` is a quantity this ceiling's
/// generator never varies, so the cap deliberately does NOT absorb it:
/// `replacement.len()` is bounded only by `MAX_QUERY_BYTES`, and an
/// absorbing ceiling would sit in the tens of gigabytes and protect
/// nothing (issue #276 fix round 2 ruling — refusing a query that asks
/// to materialise gigabytes of labels is correct; the reference has no
/// cap on this path and exhausts memory instead).
///
/// **Where refusal begins, concretely** (gated by
/// `o8_the_label_replace_template_threshold_bounds_where_refusal_is_possible`,
/// regenerated by `zz_witness_report`): `L_MIN = 2 413` template bytes,
/// at `N = 435 645` — strictly below, refusal is impossible at ANY
/// series count. The amplifying term ALONE crosses the cap at `L x
/// series > MAX_POST_AGG_BYTES / (2 x W_LABEL_BYTE) = 1 431 655 765`
/// byte-series (at `N_max = 435 771` that is a `$`-free replacement of
/// 3 286 bytes); between the two, the input's own envelope terms decide,
/// and only LOWER the point of refusal. **Reachable**: `L_MIN` fits
/// inside `MAX_QUERY_BYTES = 131 072` (stated once above, on purpose).
///
/// All three funnels ARE wired: [`apply_vector_aggs`] carries O6,
/// [`combine_binary`] carries O7 and [`apply_label_replace`] carries O8,
/// each refusing live with a clean 422 at [`MAX_POST_AGG_BYTES`], and
/// `both_amplifiers_are_refused_end_to_end_from_query_text` drives O6
/// and O7 from real query text. The divergence ledger carries a row for
/// each, in docs/benchmarks/logs-differential-ledger.md: rows (d) and
/// (e) under "Issue #236 — high-cardinality aggregations" for O6/O7, and
/// `label-replace-template-amplification` for O8.
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

/// Bytes charged per operand series for the class-(P) preflight's OWN
/// scratch (issue #290).
///
/// Priced the way every other `Vec::with_capacity` charge in this crate
/// is priced (`variants::vec_buffer_bytes`,
/// `detected_probe::scratch_capacity_bytes`):
/// `alloc_block_bytes(n * size_of::<T>())`. No `grown_alloc_bytes` term —
/// every one of the six buffers is reserved at a count known up front and
/// never pushed past capacity, which the runtime capacity gate
/// (`the_preflight_scratch_stays_inside_its_charge`) is what proves.
///
/// **Slope derivation.** `alloc_block_bytes(x) = max(2x, 32) <= 2x + 32`,
/// and the six buffers hold, per series of `S = lhs.series + rhs.series`:
/// `sides_l` + `sides_r` (their capacities SUM to `S`) at `2·W` where
/// `W = size_of::<SideSeries>()`; `order`, `groups` and `cursors` at
/// `3 · 2 · 4 = 24`; the `heads` heap at `2 · 16 = 32`. Six buffers
/// contribute six floors, which is [`PREFLIGHT_FLAT_BYTES`].
pub const PREFLIGHT_BYTES_PER_SERIES: u64 =
    2 * size_of::<preflight::SideSeries<'static>>() as u64 + 56;

/// The six per-allocation floors of [`PREFLIGHT_BYTES_PER_SERIES`]'
/// derivation — `alloc_block_bytes`' [`super::charge::MIN_ALLOC_BYTES`] applied
/// once per buffer, so the charge covers the smallest inputs where the
/// floor and not the product dominates.
pub const PREFLIGHT_FLAT_BYTES: u64 = 6 * super::charge::MIN_ALLOC_BYTES;

/// **Domination.** For every `S >= 1`,
/// `slope·S + flat <= (slope + flat)·S <= B_SERIES·S <= binary_peak_bytes`
/// — [`binary_peak_bytes_without`] always starts with
/// `B_SERIES · (lhs.series + rhs.series)` and every other term is
/// non-negative. A preflight whose scratch is NOT dominated by the stage
/// charge it precedes could hold memory the stage's own model does not,
/// so the inequality is asserted at compile time rather than believed.
const _: () = assert!(PREFLIGHT_BYTES_PER_SERIES + PREFLIGHT_FLAT_BYTES <= B_SERIES);

/// The scratch ceiling above which the class-(P) preflight is SKIPPED.
///
/// Derived, not chosen, so that every input the stage charge can admit at
/// the production cap is preflighted: the stage admits only if
/// `B_SERIES·S <= MAX_POST_AGG_BYTES`, i.e. `S <= MAX_POST_AGG_BYTES /
/// B_SERIES`, and the preflight's closed form at that `S` is this figure.
///
/// A skip is **not** a refusal — it is not an `Err`, there is nothing to
/// swallow, and the caller falls through to the unchanged stage path, so
/// the three join refusals behave exactly as they did before issue #290.
pub const MAX_BINARY_PREFLIGHT_BYTES: u64 =
    PREFLIGHT_BYTES_PER_SERIES * (MAX_POST_AGG_BYTES / B_SERIES) + PREFLIGHT_FLAT_BYTES;

/// The class-(P) preflight's charge, as ONE closed form so there is no
/// per-buffer twin to drift from the derivation above.
///
/// `S == 0` charges 0: [`preflight::decide_binary_refusals`] returns
/// before any buffer exists when either operand carries no series (no
/// step can then have both sides non-empty), so the six floors are not
/// owed.
pub fn preflight_scratch_bytes(lhs: &StageInput, rhs: &StageInput) -> u64 {
    let s = lhs.series().saturating_add(rhs.series());
    if s == 0 {
        return 0;
    }
    PREFLIGHT_BYTES_PER_SERIES
        .saturating_mul(s)
        .saturating_add(PREFLIGHT_FLAT_BYTES)
}

/// The ceiling [`ledger::PreflightCharge::acquire`] tests against. Test
/// builds can force it, because the skip branch is unreachable below
/// ~6.75 million combined series and would otherwise never be exercised
/// in CI (issue #290 AC 21).
fn preflight_ceiling() -> u64 {
    // ONE definition, with the `cfg` inside the body: a `#[cfg(test)]` /
    // `#[cfg(not(test))]` pair would be two free functions of the same
    // name, which `region_census::free_fns` refuses because the
    // call-graph closure could not resolve which body it walked.
    #[cfg(test)]
    if let Some(forced) = PREFLIGHT_CEILING_OVERRIDE.with(|c| c.get()) {
        return forced;
    }
    MAX_BINARY_PREFLIGHT_BYTES
}

#[cfg(test)]
thread_local! {
    /// Forces [`preflight_ceiling`]; `None` = the shipped constant.
    static PREFLIGHT_CEILING_OVERRIDE: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
    /// Every timestamp the preflight reads, counted at the ONE accessor
    /// that can read one ([`preflight::points::Points::at`]). A
    /// `thread_local`, not an atomic: `cargo test` runs these in threads
    /// of one process and a process-global counter cross-contaminates.
    static PREFLIGHT_POINTS_READ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// `Σ alloc_block_bytes(buf.capacity() · size_of::<T>())` over the
    /// six scratch buffers, recorded at
    /// [`preflight::decide_binary_refusals`]' single exit.
    static PREFLIGHT_SCRATCH_CAP: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The class-(P) preflight: the semantic refusals the binary funnel can
/// raise, decided from the operands' already-materialised labels and
/// timestamps **before** the stage charge exists (issue #290).
///
/// The classifying test this module satisfies, stated in
/// docs/architecture.md §5.6: a refusal is class **(P)** iff it is
/// decidable reading only the inputs — already materialised, already
/// owned by the caller — with auxiliary scratch whose size is a function
/// of INPUT COUNTS alone and never of the output's shape, and without
/// constructing any part of the charged output. All five of this funnel's
/// semantic refusals pass that test, so this funnel has no class-(A)
/// member left.
///
/// **Where the "before the charge" claim stops.** (P1) is conditional.
/// What holds however it is deferred is that no refusal is LOST, only
/// moved: `instant_join` still performs all three of its own checks, so
/// a (P1) refusal the preflight did not decide is raised there instead,
/// below `Ledger::acquire_binary`. The two conditions `ledger::decide_binary`
/// spells for itself are the conjuncts of its own `if`, and they differ
/// in what a deferral there costs:
///
/// 1. **The guard**, the first conjunct. (P1) runs only when the stage
///    charge about to be levied would refuse. When it would not, the
///    join's refusals are raised below the charge harmlessly, because
///    that charge admitted and so preempts nothing.
/// 2. **The skip**, the second. When the scratch would exceed
///    [`MAX_BINARY_PREFLIGHT_BYTES`], `PreflightCharge::acquire` returns
///    `None` and (P1) does not run at all. Behaviour then falls back
///    byte-for-byte to the pre-#290 ordering, budget-first — here the
///    budget DOES answer first, which
///    `the_join_refusals_are_preempted_by_the_budget_when_the_preflight_is_skipped`
///    keeps reproducible. The skip is unreachable below ~6.75 million
///    combined series.
///
/// Deferring a refusal does not reclassify it: (P) is a property of the
/// refusal — decidable from the inputs in input-sized scratch — not of
/// where it happens to be decided, and
/// `every_semantic_refusal_under_the_binary_seam_is_decided_above_the_charge`
/// derives the funnel's non-budget refusal constructors from the call
/// graph and pins them against a published answer, within the scope that
/// test states. (P0) is subject to neither condition: `decide_shape` runs
/// unconditionally, above every charge.
///
/// **The scratch is charged before it is allocated.** `decide_shape` is
/// (P0) — a match on operand discriminants that needs no input-scaled
/// scratch and therefore runs above every charge. (It is not literally
/// allocation-free: on its two refusing paths it calls an error
/// constructor that builds the message `String` the client receives, as
/// every semantic refusal in this crate does under no charge at all.
/// What it never causes is an allocation whose size an operand can
/// influence.) [`decide_binary_refusals`] is (P1): it takes a
/// `&PreflightCharge`, which is proof that its six buffers were priced
/// from `Vec::len()` counts and admitted against
/// [`MAX_BINARY_PREFLIGHT_BYTES`] first.
///
/// **This module causes NO allocation outside those six buffers and the
/// messages its error constructors build.** Two independent mechanisms
/// bound that: a source rule in `tests/logql_post_agg_witness.rs`
/// (`the_preflight_module_allocates_only_its_six_reserved_buffers`),
/// which sees direct calls only, and the measured allocator gate in
/// `tests/logql_preflight_alloc_gate.rs`, which is closed over the callee
/// closure by construction and is the enforcement of the byte bound.
mod preflight {
    use super::ledger::PreflightCharge;
    use super::{
        BinOp, MatchGroup, QueryResult, ReadError, VectorMatching, duplicate_one_side_error,
        grouping_unique_error, incompatible_types_error, is_set_op, multiple_matches_error,
        set_op_scalar_error,
    };
    use std::cmp::{Ordering, Reverse};
    use std::collections::BinaryHeap;

    /// The only route by which the preflight can read a timestamp.
    ///
    /// The field is private to THIS module, so a traversal that bypasses
    /// [`Points::at`] is one that cannot name the data — which is what
    /// makes the read counter a COMPLETE record of point reads rather
    /// than an advisory one, and the numeric bound in
    /// `the_preflight_reads_no_point_without_a_collision` meaningful.
    /// In non-test builds the increment compiles out entirely.
    pub(super) mod points {
        /// `None` is an INSTANT operand — one virtual step at `t = 0`.
        /// (An `enum { Instant, Steps(&[..]) }` is the same shape; the
        /// `Option` spelling keeps the payload behind a private field.)
        pub(super) struct Points<'a> {
            steps: Option<&'a [(i64, f64)]>,
        }

        impl<'a> Points<'a> {
            pub(super) fn instant() -> Self {
                Self { steps: None }
            }

            pub(super) fn steps(pts: &'a [(i64, f64)]) -> Self {
                Self { steps: Some(pts) }
            }

            /// `O(1)`, and UNCOUNTED: a length is not a point.
            pub(super) fn len(&self) -> usize {
                match self.steps {
                    None => 1,
                    Some(p) => p.len(),
                }
            }

            /// The ONLY read.
            pub(super) fn at(&self, i: usize) -> i64 {
                #[cfg(test)]
                super::super::PREFLIGHT_POINTS_READ.with(|c| c.set(c.get().saturating_add(1)));
                match self.steps {
                    None => 0,
                    Some(p) => p[i].0,
                }
            }
        }
    }
    use points::Points;

    /// One operand series, borrowed: its already-materialised labels and
    /// the timestamps at which it is present. Never owns anything.
    pub(super) struct SideSeries<'a> {
        labels: &'a [(String, String)],
        points: Points<'a>,
    }

    /// A many-side element's per-step output identity — the quantity its
    /// refusal is keyed on. `Grouped` carries no owned data: it is
    /// compared through a filtered walk of the BORROWED label slice, and
    /// no output label vector is ever materialised.
    ///
    /// The two modes are mutually exclusive — `instant_join`'s
    /// `let one_to_one = include.is_none();` — so exactly one collision
    /// error is possible per step.
    #[derive(Clone, Copy)]
    enum Identity<'a> {
        /// Identity is the match signature; a repeat is
        /// [`multiple_matches_error`].
        OneToOne,
        /// Identity is `(signature, labels minus the include names)`; a
        /// repeat is [`grouping_unique_error`].
        ///
        /// Why the include names alone: within one step two many-side
        /// elements are only ever compared when they share the match
        /// signature `key`, and a shared `key` means
        /// `one_by_key.get(&key)` returned the SAME one-side element `r`
        /// (`instant_join`'s `let Some(r) = one_by_key.get(&key) else`).
        /// The include overlay is therefore identical for both — a fixed
        /// set of `(name, value)` pairs to insert and a fixed set of
        /// names to delete (the `else` arm of `instant_join`'s
        /// `let labels: MatchSig = if one_to_one`, where the include
        /// names drive `set_label_sorted`/`remove_label_sorted`) — and
        /// the two parts have disjoint key sets, so
        /// `final_labels(l1) == final_labels(l2)` iff
        /// `filter(l1, k ∉ include) == filter(l2, k ∉ include)`.
        Grouped(&'a [String]),
    }

    /// (P0) — the operand-shape dispatch, reproducing the arm
    /// [`super::combine_binary_capped`]'s `match` selects over ALL SEVEN
    /// [`QueryResult`] variants. Needs no input-scaled scratch — the only
    /// thing it allocates is the refusal message itself, on the two arms
    /// that refuse — so it runs above even the preflight's own charge and
    /// can never be preempted, at any cap, with any counter.
    pub(super) fn decide_shape(
        op: BinOp,
        lhs: &QueryResult,
        rhs: &QueryResult,
    ) -> Result<(), ReadError> {
        match (lhs, rhs) {
            (QueryResult::Scalar(_), QueryResult::Scalar(_))
            | (QueryResult::Scalar(_), QueryResult::Vector(_) | QueryResult::Matrix(_))
            | (QueryResult::Vector(_) | QueryResult::Matrix(_), QueryResult::Scalar(_)) => {
                if is_set_op(op) {
                    return Err(set_op_scalar_error(op));
                }
                Ok(())
            }
            (QueryResult::Vector(_), QueryResult::Vector(_))
            | (QueryResult::Matrix(_), QueryResult::Matrix(_)) => Ok(()),
            _ => Err(incompatible_types_error()),
        }
    }

    /// A side's series count — `Vec::len`, `O(1)`, no traversal.
    fn operand_series(r: &QueryResult) -> usize {
        match r {
            QueryResult::Vector(items) => items.len(),
            QueryResult::Matrix(items) => items.len(),
            _ => 0,
        }
    }

    /// The counts the preflight's own charge was sized from, read back
    /// off the operands so [`PreflightCharge::admit`] can refuse a body
    /// that measured one pair and decided over another. `O(series)`, no
    /// per-point work.
    pub(super) fn operand_counts(lhs: &QueryResult, rhs: &QueryResult) -> (u64, u64) {
        let count = |r: &QueryResult| -> (u64, u64) {
            match r {
                QueryResult::Vector(items) => (items.len() as u64, items.len() as u64),
                QueryResult::Matrix(items) => (
                    items.len() as u64,
                    items
                        .iter()
                        .fold(0u64, |a, s| a.saturating_add(s.points.len() as u64)),
                ),
                _ => (0, 0),
            }
        };
        let (ls, lp) = count(lhs);
        let (rs, rp) = count(rhs);
        (ls.saturating_add(rs), lp.saturating_add(rp))
    }

    /// The preflight's admission — the [`super::admit_join`] of its own
    /// regime.
    fn admit_operands(series: u64, points: u64, pc: &PreflightCharge) -> Result<(), ReadError> {
        pc.admit(series, points)
    }

    /// (P1) — the three join refusals, decided over the WHOLE join.
    ///
    /// `duplicate_one_side_error`, `multiple_matches_error` and
    /// `grouping_unique_error` — the three `instant_join` raises — are
    /// all functions of the operands' labels and of which series are
    /// present at which timestamp. This reproduces `combine_matrices`'
    /// ascending step order — its `for &t in &timestamps` over the
    /// timestamp union — and `instant_join`'s within-step order — the
    /// one-side index before the many loop — so the error it returns is
    /// the error the join would have returned, at the same step.
    pub(super) fn decide_binary_refusals<'a>(
        op: BinOp,
        matching: Option<&VectorMatching>,
        lhs: &'a QueryResult,
        rhs: &'a QueryResult,
        pc: &PreflightCharge,
    ) -> Result<(), ReadError> {
        // `set_op_join` constructs no semantic refusal, and every operand
        // shape other than V⊗V / M⊗M either errors in `decide_shape` or
        // goes to `map_samples`, which has no join.
        if is_set_op(op)
            || !matches!(
                (lhs, rhs),
                (QueryResult::Vector(_), QueryResult::Vector(_))
                    | (QueryResult::Matrix(_), QueryResult::Matrix(_))
            )
        {
            return Ok(());
        }
        let (n_l, n_r) = (operand_series(lhs), operand_series(rhs));
        // `instant_join`'s `if lhs.is_empty() || rhs.is_empty()`
        // short-circuit: a side with no series is empty at every step, so
        // no step can reach the one-side index.
        if n_l == 0 || n_r == 0 {
            return Ok(());
        }
        let (series, points) = operand_counts(lhs, rhs);
        admit_operands(series, points, pc)?;

        let total = n_l + n_r;
        let mut sides_l: Vec<SideSeries<'a>> = Vec::with_capacity(n_l);
        let mut sides_r: Vec<SideSeries<'a>> = Vec::with_capacity(n_r);
        let mut order: Vec<u32> = Vec::with_capacity(total);
        let mut groups: Vec<u32> = Vec::with_capacity(total);
        let mut cursors: Vec<u32> = Vec::with_capacity(total);
        let mut heads: BinaryHeap<Reverse<(i64, u32)>> = BinaryHeap::with_capacity(total);

        // ONE exit, so the capacity observation below sees every return
        // path of the stages (issue #290 AC 19).
        let out = (|| -> Result<(), ReadError> {
            project_side(lhs, &mut sides_l, pc);
            project_side(rhs, &mut sides_r, pc);
            // `instant_join`'s own role assignment, transcribed — its
            // `let (many, one, include, swapped)` match on
            // `matching.and_then(|m| m.group.as_ref())`.
            let (many, one, ident, swapped) = match matching.and_then(|m| m.group.as_ref()) {
                None => (&sides_l[..], &sides_r[..], Identity::OneToOne, false),
                Some(MatchGroup::Left(inc)) => (
                    &sides_l[..],
                    &sides_r[..],
                    Identity::Grouped(inc.as_slice()),
                    false,
                ),
                Some(MatchGroup::Right(inc)) => (
                    &sides_r[..],
                    &sides_l[..],
                    Identity::Grouped(inc.as_slice()),
                    true,
                ),
            };
            // No two series on the same side share the identity that
            // side's refusal is keyed on ⇒ no step can refuse, and NO
            // POINT HAS BEEN READ.
            if !collision_groups(one, many, matching, ident, &mut order, &mut groups, pc) {
                return Ok(());
            }
            match earliest_offending_step(
                one,
                many,
                ident,
                &order,
                &groups,
                &mut heads,
                &mut cursors,
                swapped,
                pc,
            ) {
                Some(err) => Err(err),
                None => Ok(()),
            }
        })();

        #[cfg(test)]
        record_scratch_capacity(&sides_l, &sides_r, &order, &groups, &cursors, &heads);
        out
    }

    /// Borrows one operand's series into the scratch view. Pushes exactly
    /// `operand_series(operand)` entries into a buffer the caller
    /// reserved at that count, so the `alloc_block_bytes` model's
    /// "one exactly-reserved request" precondition holds.
    fn project_side<'a>(
        operand: &'a QueryResult,
        out: &mut Vec<SideSeries<'a>>,
        _pc: &PreflightCharge,
    ) {
        match operand {
            QueryResult::Vector(items) => {
                for s in items {
                    out.push(SideSeries {
                        labels: s.labels.as_slice(),
                        points: Points::instant(),
                    });
                }
            }
            QueryResult::Matrix(items) => {
                for s in items {
                    out.push(SideSeries {
                        labels: s.labels.as_slice(),
                        points: Points::steps(s.points.as_slice()),
                    });
                }
            }
            _ => {}
        }
    }

    /// The union index `gi` addresses the one side first, then the many
    /// side; the bool is "this is a many-side series".
    fn series_at<'s, 'a>(
        one: &'s [SideSeries<'a>],
        many: &'s [SideSeries<'a>],
        gi: u32,
    ) -> (&'s SideSeries<'a>, bool) {
        let i = gi as usize;
        if i < one.len() {
            (&one[i], false)
        } else {
            (&many[i - one.len()], true)
        }
    }

    /// Stage 3 — sorts the union by `(match signature, side, identity)`
    /// and assigns each sorted position its KEY GROUP (a run of equal
    /// match signature). Returns whether any two series on the SAME side
    /// share the identity that side's refusal is keyed on; `false` means
    /// no step can refuse and the caller returns `Ok` having read no
    /// point.
    ///
    /// `sort_unstable_by` is in-place (pattern-defeating quicksort) —
    /// `sort_by` would allocate `n/2` and break the six-buffer model.
    ///
    /// Cost `O(S log S · L)` comparator work, `O(S)` auxiliary, ZERO
    /// point reads.
    #[allow(clippy::too_many_arguments)]
    fn collision_groups(
        one: &[SideSeries<'_>],
        many: &[SideSeries<'_>],
        matching: Option<&VectorMatching>,
        ident: Identity<'_>,
        order: &mut Vec<u32>,
        groups: &mut Vec<u32>,
        _pc: &PreflightCharge,
    ) -> bool {
        let total = one.len() + many.len();
        for gi in 0..total {
            order.push(gi as u32);
        }
        order.sort_unstable_by(|&a, &b| {
            let (sa, ma) = series_at(one, many, a);
            let (sb, mb) = series_at(one, many, b);
            sig_cmp(sa.labels, sb.labels, matching)
                // One-side members sort BEFORE many-side members inside a
                // key group, which is what lets the per-step scan see a
                // group's one-side presence before its many-side members.
                .then_with(|| ma.cmp(&mb))
                .then_with(|| {
                    if ma && mb {
                        grouped_ident_cmp(sa.labels, sb.labels, ident)
                    } else {
                        Ordering::Equal
                    }
                })
        });

        let mut kg = 0u32;
        for pos in 0..total {
            if pos > 0 {
                let (sp, _) = series_at(one, many, order[pos - 1]);
                let (sc, _) = series_at(one, many, order[pos]);
                if sig_cmp(sp.labels, sc.labels, matching) != Ordering::Equal {
                    kg += 1;
                }
            }
            groups.push(kg);
        }

        for pos in 1..total {
            if groups[pos] != groups[pos - 1] {
                continue;
            }
            let (sp, mp) = series_at(one, many, order[pos - 1]);
            let (sc, mc) = series_at(one, many, order[pos]);
            if !mp && !mc {
                return true; // two one-side series share a match signature
            }
            if mp && mc && grouped_ident_cmp(sp.labels, sc.labels, ident) == Ordering::Equal {
                return true; // two many-side series share an output identity
            }
        }
        false
    }

    /// Stage 4 — the earliest step at which the join would refuse, and
    /// with which error.
    ///
    /// A `(timestamp, sorted position)` min-heap, so within one step the
    /// pops arrive in sorted-position order: key groups are contiguous,
    /// and inside a key group the one-side members arrive before the
    /// many-side members, which in turn arrive in identity order. Two
    /// present series that share an identity are therefore ADJACENT among
    /// the pops, and the whole per-step state is three scalars.
    ///
    /// Precedence transcribes `instant_join`: the one-side index is built
    /// before the many loop, so a duplicate one-side signature wins over
    /// a many-side repeat at the SAME step; and `combine_matrices`'
    /// ascending union (its `for &t in &timestamps`) makes the earliest
    /// step win across steps. Both are gated by `instant_join`'s
    /// `if lhs.is_empty() || rhs.is_empty()` short-circuit — a step where
    /// either side is empty refuses nothing.
    ///
    /// Each point is read exactly once, when it is pushed: `O(P log S)`
    /// time, `O(S)` auxiliary, never more than `P` reads.
    #[allow(clippy::too_many_arguments)]
    fn earliest_offending_step(
        one: &[SideSeries<'_>],
        many: &[SideSeries<'_>],
        ident: Identity<'_>,
        order: &[u32],
        groups: &[u32],
        heads: &mut BinaryHeap<Reverse<(i64, u32)>>,
        cursors: &mut Vec<u32>,
        swapped: bool,
        _pc: &PreflightCharge,
    ) -> Option<ReadError> {
        for (pos, &gi) in order.iter().enumerate() {
            let (s, _) = series_at(one, many, gi);
            cursors.push(0);
            if s.points.len() > 0 {
                heads.push(Reverse((s.points.at(0), pos as u32)));
            }
        }

        while let Some(&Reverse((t, _))) = heads.peek() {
            let mut one_present = false;
            let mut many_present = false;
            let mut dup_one = false;
            let mut ident_repeat = false;
            let mut cur_kg = u32::MAX;
            let mut kg_one_seen = false;
            let mut prev_many: Option<u32> = None;

            while let Some(&Reverse((tt, pos))) = heads.peek() {
                if tt != t {
                    break;
                }
                heads.pop();
                let (s, is_many) = series_at(one, many, order[pos as usize]);
                let kg = groups[pos as usize];
                if kg != cur_kg {
                    cur_kg = kg;
                    kg_one_seen = false;
                    prev_many = None;
                }
                if is_many {
                    many_present = true;
                    if let Some(pp) = prev_many
                        && kg_one_seen
                    {
                        let (sp, _) = series_at(one, many, order[pp as usize]);
                        if grouped_ident_cmp(sp.labels, s.labels, ident) == Ordering::Equal {
                            ident_repeat = true;
                        }
                    }
                    prev_many = Some(pos);
                } else {
                    one_present = true;
                    if kg_one_seen {
                        dup_one = true;
                    }
                    kg_one_seen = true;
                }

                let next = cursors[pos as usize] as usize + 1;
                if next < s.points.len() {
                    let nt = s.points.at(next);
                    // The operands are ascending by construction —
                    // `MatrixSeries::points` in `logql/exec.rs` is
                    // declared "`(step_ns, value)`, ascending by step".
                    // If one is not, the preflight DECIDES NOTHING and
                    // the join decides as it always did — an incomplete
                    // preflight is safe, an unsound one is not.
                    if nt <= t {
                        return None;
                    }
                    heads.push(Reverse((nt, pos)));
                }
                cursors[pos as usize] = next as u32;
            }

            if one_present && many_present {
                if dup_one {
                    return Some(duplicate_one_side_error(swapped));
                }
                if ident_repeat {
                    return Some(match ident {
                        Identity::OneToOne => multiple_matches_error(),
                        Identity::Grouped(_) => grouping_unique_error(),
                    });
                }
            }
        }
        None
    }

    /// `match_signature`'s `on`/`ignoring` filter, as a predicate rather
    /// than a projection — the preflight must not materialise a
    /// `MatchSig`, which `match_signature` materialises with `.cloned()`.
    fn keep_label(k: &str, matching: Option<&VectorMatching>) -> bool {
        match matching {
            None => true,
            Some(vm) if vm.on => vm.labels.iter().any(|l| l == k),
            Some(vm) => !vm.labels.iter().any(|l| l == k),
        }
    }

    /// Orders two series by their match signature, comparing the filtered
    /// label sequences in place. Equality here is exactly `MatchSig`
    /// equality, because a `MatchSig` IS that sequence.
    fn sig_cmp(
        a: &[(String, String)],
        b: &[(String, String)],
        matching: Option<&VectorMatching>,
    ) -> Ordering {
        a.iter()
            .filter(|(k, _)| keep_label(k, matching))
            .cmp(b.iter().filter(|(k, _)| keep_label(k, matching)))
    }

    /// Orders two many-side series by their per-step output identity; see
    /// [`Identity`] for why the include names are all that is filtered.
    fn grouped_ident_cmp(
        a: &[(String, String)],
        b: &[(String, String)],
        ident: Identity<'_>,
    ) -> Ordering {
        match ident {
            Identity::OneToOne => Ordering::Equal,
            Identity::Grouped(inc) => a
                .iter()
                .filter(|(k, _)| !inc.iter().any(|n| n == k))
                .cmp(b.iter().filter(|(k, _)| !inc.iter().any(|n| n == k))),
        }
    }

    /// Records what the six buffers actually hold, so the charge is
    /// checked against an OBSERVATION on every differential case rather
    /// than against its own arithmetic.
    #[cfg(test)]
    // `&Vec<T>`, not `&[T]`: a slice cannot report `capacity()`, and
    // capacity is the whole quantity here — a `push` past the reserved
    // count is exactly the model violation this observes.
    #[allow(clippy::ptr_arg)]
    fn record_scratch_capacity(
        sides_l: &Vec<SideSeries<'_>>,
        sides_r: &Vec<SideSeries<'_>>,
        order: &Vec<u32>,
        groups: &Vec<u32>,
        cursors: &Vec<u32>,
        heads: &BinaryHeap<Reverse<(i64, u32)>>,
    ) {
        let blk = crate::logql::charge::alloc_block_bytes;
        let w = size_of::<SideSeries<'_>>() as u64;
        let total = blk(sides_l.capacity() as u64 * w)
            + blk(sides_r.capacity() as u64 * w)
            + blk(order.capacity() as u64 * 4)
            + blk(groups.capacity() as u64 * 4)
            + blk(cursors.capacity() as u64 * 4)
            + blk(heads.capacity() as u64 * size_of::<Reverse<(i64, u32)>>() as u64);
        super::PREFLIGHT_SCRATCH_CAP.with(|c| c.set(total));
    }
}

/// The seam `tests/logql_preflight_alloc_gate.rs` and
/// `tests/logql_preflight_guard_gate.rs` bracket with their own counting
/// global allocators (issue #290 §3).
///
/// `#[doc(hidden)]`, and it exists for those gates: the preflight's
/// allocation bound has to be MEASURED, because the source rule that
/// would otherwise carry it sees direct calls only and a helper defined
/// outside the scanned module defeats it. An integration-test binary
/// cannot bracket a private function, and this is the smallest seam that
/// lets it.
///
/// **The body is one call to [`decide_binary`] and nothing else**, so
/// what the gates measure is the shipped decision path — its (P0) tier,
/// its measurement, its guard and its (P1) tier — and not a
/// re-implementation of it that a mutant could leave green. The returned
/// `BinaryDecided` is dropped rather than charged: dropping frees the
/// operands, which requests nothing. Precedent:
/// `tests/logql_json_key_alloc_gate.rs`.
#[doc(hidden)]
pub fn preflight_alloc_probe(
    op: BinOp,
    matching: Option<&VectorMatching>,
    lhs: QueryResult,
    rhs: QueryResult,
    stage_charged: u64,
    stage_cap: u64,
) -> Result<(), ReadError> {
    decide_binary(op, matching, lhs, rhs, stage_charged, stage_cap).map(|_| ())
}

/// The funnel's proof token (issue #236 §4.1).
///
/// Everything in this module is private to it: [`Ledger`]'s fields, its
/// only constructors and the charge they perform. A `&Ledger` in a
/// signature is therefore PROOF that the stage's modelled bytes were
/// charged against the cap before that function could be reached — an
/// uncapped call site does not compile, in release exactly as in debug.
///
/// Issue #290 adds a SECOND proof token to the same module,
/// [`PreflightCharge`], for the class-(P) preflight's own scratch, and
/// two values that are not tokens but their dual — values that OWN what
/// a caller would otherwise pass loose, with no accessor but a consuming
/// one: [`BinaryDecided`], the operands whose refusals have been decided
/// (only [`Ledger::acquire_binary`] gets them back), and
/// [`BinaryCharged`], those operands plus the charge and the decision
/// context, which only that same function mints and which is consumed
/// by a single move (by `join_decided` in this tree; the exit is
/// `pub(super)`, so what is bounded to that one function is CALLING it,
/// not consuming a carrier — see its "what this bounds").
mod ledger {
    #[cfg(test)]
    use super::MAX_POST_AGG_BYTES;
    use super::{
        BinOp, MAX_BINARY_PREFLIGHT_BYTES, QueryResult, ReadError, StageInput, TooBroadReason,
        VectorMatching,
    };

    /// The class-(P) preflight's proof token (issue #290).
    ///
    /// It OWNS its charge. There is no `&mut u64` parameter, so a caller
    /// cannot hand it a precharge it cannot honour, and `charged`/`cap`
    /// are not in scope for its decision — the preflight's admission is
    /// never a function of an unrelated caller's spending. A
    /// `&PreflightCharge` in a signature is proof that the scratch was
    /// charged, exactly as `&Ledger` is for the stage.
    #[derive(Debug)]
    pub(in crate::logql) struct PreflightCharge {
        bytes: u64,
        admitted_series: u64,
        admitted_points: u64,
    }

    impl PreflightCharge {
        /// `None` = the scratch would exceed
        /// [`MAX_BINARY_PREFLIGHT_BYTES`], so the preflight is SKIPPED.
        /// A skip is not a refusal: it is not an `Err`, there is nothing
        /// to swallow, and the caller falls through to the unchanged
        /// stage path.
        pub(super) fn acquire(lhs: &StageInput, rhs: &StageInput) -> Option<Self> {
            let bytes = super::preflight_scratch_bytes(lhs, rhs);
            if bytes > super::preflight_ceiling() {
                return None;
            }
            Some(Self {
                bytes,
                admitted_series: lhs.series().saturating_add(rhs.series()),
                admitted_points: lhs.points().saturating_add(rhs.points()),
            })
        }

        /// Unchanged in spirit from [`Ledger::admit`]: the charge was
        /// sized from these counts, so a body that measured one pair and
        /// decides over another is refused.
        pub(super) fn admit(&self, series: u64, points: u64) -> Result<(), ReadError> {
            if series > self.admitted_series || points > self.admitted_points {
                return Err(ReadError::QueryTooBroad(
                    TooBroadReason::MetricPostAggBytes {
                        bytes: self.bytes,
                        cap: MAX_BINARY_PREFLIGHT_BYTES,
                    },
                ));
            }
            Ok(())
        }

        #[cfg(test)]
        pub(super) fn bytes(&self) -> u64 {
            self.bytes
        }
    }

    /// Operands whose every class-(P) refusal has been decided, together
    /// with the measurement taken FROM THEM **and the decision context
    /// they were decided under** (issue #290).
    ///
    /// The fields are private to this module: the only constructor is
    /// [`decide_binary`], and the only exit is [`Ledger::acquire_binary`],
    /// which charges. There is no accessor, no `into_operands`, no
    /// `Deref`. That is what makes "decide before you charge" structural
    /// rather than conventional — an operand cannot reach the join except
    /// through a charge the decision preceded, and there is no second
    /// pair for a caller to decide one of and charge the other.
    ///
    /// **`op` and `matching` live in here for the same reason the
    /// operands do.** (P1)'s whole answer is a function of the matcher —
    /// which side is "one", what the match signature projects, whether
    /// the identity is the signature or the include-stripped label set —
    /// and `bytes` is priced under that same matcher. Were the context
    /// left outside, parent-module code could `mem::swap` or
    /// `mem::replace` the operand halves of two decisions and then charge
    /// and join a decided pair under a matcher it was never decided
    /// under. It cannot: the halves are private and inseparable, so a
    /// swap moves the whole decision or does not compile.
    #[derive(Debug)]
    pub(super) struct BinaryDecided<'m> {
        op: BinOp,
        matching: Option<&'m VectorMatching>,
        lhs: QueryResult,
        rhs: QueryResult,
        lm: StageInput,
        rm: StageInput,
        bytes: u64,
    }

    /// Decides every class-(P) refusal of the binary funnel that the
    /// stage charge could preempt, then measures and prices the operands
    /// it decided over.
    ///
    /// The tiers are (P0) `decide_shape`, unconditional and above every
    /// charge because it needs no input-scaled scratch; then the
    /// measurement, which is saturating arithmetic over already
    /// materialised data; then (P1) `decide_binary_refusals`, under its
    /// own charge, SKIPPED rather than refused when the scratch would
    /// exceed the ceiling.
    ///
    /// # The guard
    ///
    /// (P1) runs only when [`stage_charge_would_refuse`] — the SAME
    /// expression [`Ledger::acquire_for`] rejects on, so the two cannot
    /// drift — says the charge about to be levied would refuse.
    ///
    /// That is the whole condition under which the preflight can change
    /// an answer. Issue #290 exists because a budget rejection preempted
    /// a decidable semantic error; where the charge admits there is no
    /// budget rejection, the join runs, and the caller gets the join's
    /// own answer — the same answer a preflight would have produced, and
    /// the same one it produced before #290 existed.
    /// `a_skipped_preflight_leaves_behaviour_unchanged` asserts that
    /// equality over 81 930 cases at the production cap.
    ///
    /// So an admitted query pays nothing: no scratch is reserved, no
    /// signature is sorted and no point is read.
    /// `tests/logql_preflight_guard_gate.rs` measures the bytes at zero;
    /// `the_guard_reads_no_point_when_the_charge_cannot_refuse` counts
    /// the point reads; `the_guard_turns_over_at_the_exact_byte_the_charge_refuses_on`
    /// pins the boundary.
    ///
    /// **This is not v2's `include_delta > 0` gate.** That one keyed the
    /// preflight on a property of the QUERY while leaving an acquisition
    /// that could still refuse underneath it, so a low cap or a nonzero
    /// caller counter reopened the defect (plan v2 review, `[blocking]`).
    /// This one keys on the acquisition itself: it skips exactly when
    /// that acquisition cannot refuse, at any cap, with any counter, so
    /// the refusal it protects against does not exist on the skipped
    /// path. `PreflightCharge::acquire` still reads neither — the
    /// preflight's own SCRATCH admission is never a function of a
    /// caller's spending (plan v5 review, `[high]`); only whether there
    /// is anything to protect is.
    pub(super) fn decide_binary<'m>(
        op: BinOp,
        matching: Option<&'m VectorMatching>,
        lhs: QueryResult,
        rhs: QueryResult,
        stage_charged: u64,
        stage_cap: u64,
    ) -> Result<BinaryDecided<'m>, ReadError> {
        super::preflight::decide_shape(op, &lhs, &rhs)?;
        let (lm, rm) = (super::measure_operand(&lhs), super::measure_operand(&rhs));
        let bytes = super::binary_peak_bytes(op, matching, &lm, &rm);
        if stage_charge_would_refuse(stage_charged, bytes, stage_cap)
            && let Some(pc) = PreflightCharge::acquire(&lm, &rm)
        {
            super::preflight::decide_binary_refusals(op, matching, &lhs, &rhs, &pc)?;
        }
        Ok(BinaryDecided {
            op,
            matching,
            lhs,
            rhs,
            lm,
            rm,
            bytes,
        })
    }

    /// **The one definition of "this charge refuses".**
    ///
    /// [`Ledger::acquire_for`] rejects on it and [`decide_binary`]'s
    /// guard runs the (P1) preflight on it. One expression, so a change
    /// to the refusal condition moves the guard with it and a mutant that
    /// loosens one loosens both.
    fn stage_charge_would_refuse(charged: u64, bytes: u64, cap: u64) -> bool {
        charged.saturating_add(bytes) > cap
    }

    /// A charged decision: the [`Ledger`] the operands were charged on,
    /// the operands themselves, and the `op`/`matching` they were
    /// decided under — inseparable until the join takes them (issue
    /// #290, review round 3's `[medium]`).
    ///
    /// **Why the context is not returned loose.** `op` and `matching`
    /// are `Copy`, so a `join_decided(&led, op, matching, lhs, rhs)`
    /// leaves the caller free to pass ITS OWN `op`/`matching` — the
    /// decision's operands joined under a context nothing was decided
    /// under. Shadowing the caller's bindings only hides that; renaming
    /// the destructured ones brings it straight back, and it compiled.
    /// `Copy` is not the constraint. What closes it is that the only
    /// thing carrying the context into the join is a value that must be
    /// MOVED in and cannot be built: the fields are private to this
    /// module (a struct literal outside it is E0451), the only
    /// constructor is [`Ledger::acquire_binary`], and the only exit is
    /// [`Self::into_join_context`], which consumes — so a detached
    /// context cannot be reattached, because there is no way back to a
    /// `BinaryCharged`.
    ///
    /// **What that bounds** (review rounds 4 and 6's `[medium]`). It
    /// bounds CALLING the join: no argument route into `join_decided`
    /// can supply a context (`E0061`/`E0616`/`E0451`, measured, on that
    /// function). It does not bound consuming a carrier — these fields
    /// are private to this module and [`Self::into_join_context`] is
    /// `pub(super)`, so code elsewhere in this file can unpack one
    /// without going through the join.
    ///
    /// The ledger travels here for the same reason. A join against a
    /// budget the operands were not charged on is context substitution
    /// one layer down, and `&Ledger` as a join parameter left it
    /// expressible: at `9695900`, `Ledger::acquire` over a local counter
    /// with `cap = u64::MAX` passed to `join_decided` compiled.
    #[derive(Debug)]
    pub(super) struct BinaryCharged<'a, 'm> {
        led: Ledger<'a>,
        op: BinOp,
        matching: Option<&'m VectorMatching>,
        lhs: QueryResult,
        rhs: QueryResult,
    }

    impl<'a, 'm> BinaryCharged<'a, 'm> {
        /// The ONLY exit, and it CONSUMES: what comes out is what went
        /// in, once. The charge stays live in the returned [`Ledger`],
        /// which discharges on drop as always.
        pub(super) fn into_join_context(
            self,
        ) -> (
            Ledger<'a>,
            BinOp,
            Option<&'m VectorMatching>,
            QueryResult,
            QueryResult,
        ) {
            (self.led, self.op, self.matching, self.lhs, self.rhs)
        }
    }

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
        ///
        /// Consumes the decision, levies the charge derived from it, and
        /// hands back **one value and nothing loose**: a
        /// [`BinaryCharged`] owning the charge, the operands it was
        /// decided over and the `op` and `matching` they were decided
        /// under. Callers cannot name any of those by another route
        /// (issue #290): there is no `&StageInput` parameter left to
        /// supply a measurement that was not taken from them, no way to
        /// obtain a [`BinaryDecided`] except from [`decide_binary`], and
        /// no way to pair one decision's operands with another's matcher
        /// or another's ledger, because all of them are private fields
        /// of one value that only this function can mint.
        pub(super) fn acquire_binary<'m>(
            charged: &'a mut u64,
            decided: BinaryDecided<'m>,
            cap: u64,
        ) -> Result<BinaryCharged<'a, 'm>, ReadError> {
            let BinaryDecided {
                op,
                matching,
                lhs,
                rhs,
                lm,
                rm,
                bytes,
            } = decided;
            let led = Self::acquire_for(
                charged,
                lm.series().saturating_add(rm.series()),
                lm.points().saturating_add(rm.points()),
                bytes,
                cap,
            )?;
            Ok(BinaryCharged {
                led,
                op,
                matching,
                lhs,
                rhs,
            })
        }

        fn acquire_for(
            charged: &'a mut u64,
            admitted_series: u64,
            admitted_points: u64,
            bytes: u64,
            cap: u64,
        ) -> Result<Self, ReadError> {
            let next = charged.saturating_add(bytes);
            if stage_charge_would_refuse(*charged, bytes, cap) {
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
// Issue #290's additions, each on its OWN line and AFTER the line
// above: `the_enforcement_path_contains_no_debug_assert` delimits its
// scan region with the literal `"\nuse ledger::Ledger;"`, so folding
// these into a brace group would silence that gate rather than fail it.
// `PreflightCharge` is named at this level only by the in-crate tests
// that drive the preflight's own admission directly: since the probe
// routes through `decide_binary`, production code reaches the token
// through `mod ledger` and `mod preflight` alone.
use ledger::BinaryCharged;
#[cfg(test)]
use ledger::PreflightCharge;
use ledger::decide_binary;

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

// ---------------------------------------------------------------------
// `label_replace(...)` (issue #276)
// ---------------------------------------------------------------------

/// Applies a compiled `label_replace` transform to an evaluated metric
/// result, charged against [`MAX_POST_AGG_BYTES`] before it allocates.
///
/// Reference semantics (grafana/loki v3.7.4 `pkg/logql/evaluator.go`
/// `LabelReplaceEvaluator::Next`, every edge live-probed on the pinned
/// container — issue #276):
/// * the source value is the series' `src` label, `""` when absent;
/// * the anchored regex (`^(?:…)$`, no dot-all) either matches the WHOLE
///   value or the series passes through untouched;
/// * on a match, `dst` is deleted and re-set to the Go
///   `Regexp.ExpandString` expansion of `replacement` (`$1`/`${name}`,
///   out-of-range or unmatched group → empty, `$$` → literal `$`, a
///   trailing lone `$` → literal); an EMPTY expansion leaves `dst`
///   deleted;
/// * `dst` is NOT validated: the reference sets any name — `"0bad"`,
///   `"__name__"`, even `""` — and emits it on the wire verbatim
///   (probed; Prometheus's `label_replace` validity check has no Loki
///   counterpart), so no validation happens here either;
/// * output label sets may COLLIDE. At instant the reference returns the
///   duplicate sample entries as-is (probed: four identical `{src="same"}`
///   elements) — mirrored here by relabeling in place. At range the
///   reference's engine accumulates per-step vectors into one series per
///   label set, so colliding series MERGE and same-timestamp points
///   repeat inside the merged series (probed) — see
///   [`merge_matrix_collisions`].
pub fn apply_label_replace(
    result: QueryResult,
    spec: &plan::LabelReplaceSpec,
) -> Result<QueryResult, ReadError> {
    let mut charged = 0u64;
    apply_label_replace_capped(&mut charged, result, spec, MAX_POST_AGG_BYTES)
}

/// The cap seam (the [`apply_vector_aggs_capped`] precedent): measure,
/// charge, then transform — a refused charge mutates nothing.
pub fn apply_label_replace_capped(
    charged: &mut u64,
    result: QueryResult,
    spec: &plan::LabelReplaceSpec,
    cap: u64,
) -> Result<QueryResult, ReadError> {
    match result {
        QueryResult::Vector(mut items) => {
            let m = measure_vector(&items);
            let bytes = label_replace_peak_bytes(&m, spec);
            let l = Ledger::acquire(charged, &m, bytes, cap)?;
            for s in &mut items {
                relabel(&mut s.labels, spec, &l);
            }
            Ok(QueryResult::Vector(items))
        }
        QueryResult::Matrix(mut items) => {
            let m = measure_matrix(&items);
            let bytes = label_replace_peak_bytes(&m, spec);
            let l = Ledger::acquire(charged, &m, bytes, cap)?;
            for s in &mut items {
                relabel(&mut s.labels, spec, &l);
            }
            Ok(QueryResult::Matrix(merge_matrix_collisions(items, &l)?))
        }
        // A `label_replace` over a scalar-typed tree is rejected at plan
        // time (`fold_plan_ops`); passthrough is defensive only.
        other => Ok(other),
    }
}

/// An upper bound on the transform's own allocations, in the
/// [`post_agg_peak_bytes`] style over the same measured [`StageInput`].
/// Every term below prices a NAMED allocation site in the transform —
/// the sentence and the mechanism are the same object (fix round 1's
/// `[high]`: the first version omitted the collision merge's label-set
/// clone, so the merge allocated bytes nothing charged):
/// * one expansion `String` per series (`relabel`) — each `$` in the
///   template starts at most one group reference expanding to at most
///   the longest label value, so `replacement.len() + #'$' ×
///   max_value_bytes` bounds it;
/// * one re-inserted `(dst, expansion)` pair per series (`relabel`'s
///   `labels.insert`): `W_PAIR + W_LABEL_BYTE × (dst.len() +
///   expansion_bound)` — `per_series`, charged TWICE because the merge's
///   clone (next bullet) copies that same pair a second time;
/// * the collision merge's slot-key clone (`merge_matrix_collisions`,
///   `s.labels.clone()`): one copy of each DISTINCT post-rewrite label
///   set, bounded by cloning EVERY series' set — the input's own
///   `W_PAIR × label_pairs + W_LABEL_BYTE × label_bytes` for the
///   pre-existing pairs, plus the second `per_series` for the inserted
///   one. Charged on the instant arm too, where no merge runs — an
///   over-bound in the refusing direction, kept so both arms charge one
///   expression;
/// * the merge's rebuilt containers — group vectors, merged point
///   vectors (allocated before the old ones are freed), and the
///   [`merge_point_lists`] heap (at most one entry per colliding list,
///   so ≤ one per input series) — bounded by the input's own
///   series/point envelope (`W_SERIES`/`W_POINT`).
pub fn label_replace_peak_bytes(m: &StageInput, spec: &plan::LabelReplaceSpec) -> u64 {
    let dollar_refs = spec.replacement.bytes().filter(|b| *b == b'$').count() as u64;
    let expansion_bound = (spec.replacement.len() as u64)
        .saturating_add(dollar_refs.saturating_mul(m.max_value_bytes));
    let per_series = W_PAIR.saturating_add(
        W_LABEL_BYTE.saturating_mul((spec.dst.len() as u64).saturating_add(expansion_bound)),
    );
    let merge_clone = W_PAIR
        .saturating_mul(m.label_pairs)
        .saturating_add(W_LABEL_BYTE.saturating_mul(m.label_bytes));
    W_SERIES
        .saturating_mul(m.series)
        .saturating_add(W_POINT.saturating_mul(m.points))
        .saturating_add(per_series.saturating_mul(2).saturating_mul(m.series))
        .saturating_add(merge_clone)
}

/// One series' in-place rewrite (an Element of the funnel's census: the
/// `&Ledger` is the proof its expansion/insert allocations were priced
/// by `label_replace_peak_bytes` before this could be reached). The
/// label vector is kept sorted by name (the invariant every metric-path
/// producer maintains — `labels::series_labels` sorts, group keys are
/// built sorted), so the deleted `dst` is found and the new pair
/// re-inserted by binary search.
fn relabel(labels: &mut LabelSet, spec: &plan::LabelReplaceSpec, _l: &Ledger<'_>) {
    let src_value = labels
        .iter()
        .find(|(k, _)| *k == spec.src)
        .map(|(_, v)| v.as_str())
        .unwrap_or("");
    let expanded = spec.re().captures(src_value).map(|caps| {
        let mut out = String::new();
        caps.expand(&spec.replacement, &mut out);
        out
    });
    // No match → no replacement takes place (the reference's rule).
    let Some(expanded) = expanded else { return };
    labels.retain(|(k, _)| *k != spec.dst);
    if !expanded.is_empty() {
        let at = labels.partition_point(|(k, _)| k.as_str() < spec.dst.as_str());
        labels.insert(at, (spec.dst.clone(), expanded));
    }
}

/// Merges matrix series whose label sets collided after the rewrite,
/// preserving first-seen series order.
///
/// The reference's range engine joins each step's vector into one series
/// per label set, appending samples step by step — so a merged series
/// carries every colliding point, INCLUDING duplicate timestamps
/// (probed: `[[t1,"1"],[t2,"1"],[t2,"1"],[t2,"1"],[t2,"1"]]` on the
/// wire). Points are ordered by timestamp; the same-timestamp intra-order
/// is the reference's per-step vector order, which for an aggregated
/// operand is a Go map walk — not reproducible even by the reference
/// itself — so PulsusDB pins the deterministic input-series order instead
/// (the ratified `approx_topk`/instant-tie precedent; ledgered under
/// `label-replace-collision-tie-order`).
fn merge_matrix_collisions(
    items: Vec<MatrixSeries>,
    l: &Ledger<'_>,
) -> Result<Vec<MatrixSeries>, ReadError> {
    // Entry-class admission (the funnel contract): the collection is the
    // relabeled operand, so its envelope is exactly the measured input's.
    let points: usize = items.iter().map(|s| s.points.len()).sum();
    l.admit(items.len() as u64, points as u64)?;
    let mut index: HashMap<&[(String, String)], usize> = HashMap::new();
    let mut any_collision = false;
    for s in &items {
        let n = index.len();
        if index.entry(s.labels.as_slice()).or_insert(n) != &n {
            any_collision = true;
        }
    }
    if !any_collision {
        return Ok(items);
    }
    // Group colliding series' point lists in input order, then k-way
    // stable-merge each group by timestamp (every input list is already
    // ascending; ties keep input-order precedence).
    let mut groups: Vec<(LabelSet, PointLists)> = Vec::with_capacity(index.len());
    let mut slot: HashMap<LabelSet, usize> = HashMap::new();
    for s in items {
        match slot.get(&s.labels) {
            Some(&i) => groups[i].1.push(s.points),
            None => {
                // This clone of each distinct post-rewrite label set is
                // priced by `label_replace_peak_bytes`' `merge_clone` +
                // second `per_series` terms (fix round 1's `[high]`).
                slot.insert(s.labels.clone(), groups.len());
                groups.push((s.labels, vec![s.points]));
            }
        }
    }
    Ok(groups
        .into_iter()
        .map(|(labels, lists)| MatrixSeries {
            labels,
            points: merge_point_lists(lists),
        })
        .collect())
}

/// The colliding point lists of one merged output series, in input
/// order.
type PointLists = Vec<Vec<(i64, f64)>>;

/// Stable k-way merge by timestamp through a min-heap keyed
/// `(timestamp, list index)`.
///
/// `k` — the colliding lists in ONE merge — is bounded only by the
/// operand's series count: `label_replace(..., "app", "same", "app",
/// ".*")` rewrites EVERY series to one label set, so an adversarial
/// query drives `k` to the full admitted series envelope. A linear
/// min-scan would be `O(points × k)` — billions of probes at permitted
/// result sizes (fix round 1's `[med]`) — where the heap is
/// `O(points × log k)`, holding at most one entry per list (≤ one per
/// input series, inside the `W_SERIES` envelope the charge admits).
///
/// Order contract (unchanged): ascending timestamp; a tie resolves to
/// the EARLIER input list, every time it recurs (the `(ts, index)` heap
/// key); duplicate timestamps are KEPT (the reference emits them); a
/// list's internal order survives because only its head is ever in the
/// heap.
fn merge_point_lists(lists: PointLists) -> Vec<(i64, f64)> {
    if lists.len() == 1 {
        return lists.into_iter().next().unwrap_or_default();
    }
    let total = lists.iter().map(Vec::len).sum();
    let mut cursors: Vec<std::iter::Peekable<std::vec::IntoIter<(i64, f64)>>> = lists
        .into_iter()
        .map(|l| l.into_iter().peekable())
        .collect();
    let mut heap: BinaryHeap<Reverse<(i64, usize)>> = BinaryHeap::with_capacity(cursors.len());
    for (i, c) in cursors.iter_mut().enumerate() {
        if let Some(&(ts, _)) = c.peek() {
            heap.push(Reverse((ts, i)));
        }
    }
    let mut out = Vec::with_capacity(total);
    while let Some(Reverse((_, i))) = heap.pop() {
        if let Some(p) = cursors[i].next() {
            out.push(p);
        }
        if let Some(&(ts, _)) = cursors[i].peek() {
            heap.push(Reverse((ts, i)));
        }
    }
    out
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
            grouping: None,
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

    // -----------------------------------------------------------------
    // `label_replace(...)` (issue #276) — reference semantics pinned per
    // the b16 container captures; edges the corpus DSL cannot express
    // (empty destination name, the cap seam, the RE2 rewrite) live here.
    // -----------------------------------------------------------------

    fn lr_spec(dst: &str, replacement: &str, src: &str, regex: &str) -> plan::LabelReplaceSpec {
        plan::LabelReplaceSpec::compile(dst, replacement, src, regex).expect("compile")
    }

    fn lr_labels(pairs: &[(&str, &str)]) -> LabelSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn lr_vector(items: Vec<(LabelSet, f64)>) -> QueryResult {
        QueryResult::Vector(
            items
                .into_iter()
                .map(|(labels, value)| VectorSample { labels, value })
                .collect(),
        )
    }

    fn lr_apply_vector(result: QueryResult, spec: &plan::LabelReplaceSpec) -> Vec<VectorSample> {
        match apply_label_replace(result, spec).expect("apply") {
            QueryResult::Vector(items) => items,
            other => panic!("expected a vector, got {other:?}"),
        }
    }

    /// The new pair lands at its SORTED position (the metric-path label
    /// invariant), and an existing `dst` is deleted before the re-set.
    #[test]
    fn label_replace_inserts_the_destination_sorted_and_replaces_an_existing_one() {
        let spec = lr_spec("m", "v-$1", "s", "x-(.*)");
        let items = lr_apply_vector(
            lr_vector(vec![(
                lr_labels(&[("a", "1"), ("m", "old"), ("s", "x-7")]),
                1.0,
            )]),
            &spec,
        );
        assert_eq!(
            items[0].labels,
            lr_labels(&[("a", "1"), ("m", "v-7"), ("s", "x-7")])
        );
    }

    /// A non-matching source leaves the series untouched — including a
    /// pre-existing destination value.
    #[test]
    fn label_replace_no_match_leaves_the_series_untouched() {
        let spec = lr_spec("m", "v", "s", "nope");
        let labels = lr_labels(&[("m", "old"), ("s", "x-7")]);
        let items = lr_apply_vector(lr_vector(vec![(labels.clone(), 1.0)]), &spec);
        assert_eq!(items[0].labels, labels);
    }

    /// An empty expansion DELETES the destination (the reference's
    /// `Del` + conditional `Set`).
    #[test]
    fn label_replace_empty_expansion_deletes_the_destination() {
        let spec = lr_spec("m", "$2", "s", "(x)-(?:.*)");
        let items = lr_apply_vector(
            lr_vector(vec![(lr_labels(&[("m", "old"), ("s", "x-7")]), 1.0)]),
            &spec,
        );
        assert_eq!(items[0].labels, lr_labels(&[("s", "x-7")]));
    }

    /// The reference performs NO destination-name validation: an EMPTY
    /// name (probed: the container emits `{"": "x", …}` on the wire) is
    /// set like any other, and sorts first.
    #[test]
    fn label_replace_sets_an_empty_destination_name_verbatim() {
        let spec = lr_spec("", "x", "s", ".*");
        let items = lr_apply_vector(lr_vector(vec![(lr_labels(&[("s", "v")]), 1.0)]), &spec);
        assert_eq!(items[0].labels, lr_labels(&[("", "x"), ("s", "v")]));
    }

    /// Instant collisions are returned as duplicate samples in input
    /// order — never merged (the container returns all four).
    #[test]
    fn label_replace_instant_collisions_are_kept_as_duplicates() {
        let spec = lr_spec("s", "same", "s", "(.*)");
        let items = lr_apply_vector(
            lr_vector(vec![
                (lr_labels(&[("s", "a")]), 1.0),
                (lr_labels(&[("s", "b")]), 2.0),
            ]),
            &spec,
        );
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].labels, lr_labels(&[("s", "same")]));
        assert_eq!(items[0].value, 1.0);
        assert_eq!(items[1].labels, lr_labels(&[("s", "same")]));
        assert_eq!(items[1].value, 2.0);
    }

    /// Range collisions MERGE into one series: points ordered by
    /// timestamp, duplicate timestamps KEPT, same-timestamp intra-order =
    /// input-series order (the pinned deterministic tie — the reference's
    /// own per-step order is a Go map walk it cannot reproduce), and
    /// first-seen series order preserved for the non-colliding rest.
    #[test]
    fn label_replace_range_collisions_merge_with_input_order_ties() {
        let spec = lr_spec("s", "same", "s", "[ab]");
        let matrix = QueryResult::Matrix(vec![
            MatrixSeries {
                labels: lr_labels(&[("s", "a")]),
                points: vec![(30, 1.0), (60, 2.0)],
            },
            MatrixSeries {
                labels: lr_labels(&[("s", "keep")]),
                points: vec![(30, 9.0)],
            },
            MatrixSeries {
                labels: lr_labels(&[("s", "b")]),
                points: vec![(0, 3.0), (60, 4.0)],
            },
        ]);
        let out = match apply_label_replace(matrix, &spec).expect("apply") {
            QueryResult::Matrix(items) => items,
            other => panic!("expected a matrix, got {other:?}"),
        };
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].labels, lr_labels(&[("s", "same")]));
        // ts-ascending; the duplicate 60 keeps input order (a's 2.0
        // before b's 4.0 — series a precedes series b in the input).
        assert_eq!(
            out[0].points,
            vec![(0, 3.0), (30, 1.0), (60, 2.0), (60, 4.0)]
        );
        assert_eq!(out[1].labels, lr_labels(&[("s", "keep")]));
        assert_eq!(out[1].points, vec![(30, 9.0)]);
    }

    /// A collision-free matrix passes through with every series and point
    /// untouched (the merge is a no-op fast path).
    #[test]
    fn label_replace_collision_free_matrix_is_untouched() {
        let spec = lr_spec("d", "v", "s", ".*");
        let matrix = QueryResult::Matrix(vec![MatrixSeries {
            labels: lr_labels(&[("s", "a")]),
            points: vec![(0, 1.0)],
        }]);
        let out = match apply_label_replace(matrix, &spec).expect("apply") {
            QueryResult::Matrix(items) => items,
            other => panic!("expected a matrix, got {other:?}"),
        };
        assert_eq!(out[0].labels, lr_labels(&[("d", "v"), ("s", "a")]));
        assert_eq!(out[0].points, vec![(0, 1.0)]);
    }

    /// The regex goes through the #317 RE2→Rust rewrite: `\d` is RE2's
    /// ASCII class, so an Arabic-Indic digit does NOT match — the bare
    /// rust-regex compile (Unicode `\d`) would. This is the mutant that
    /// reddens if the rewrite is dropped from the compile path.
    #[test]
    fn label_replace_regex_keeps_re2_ascii_class_semantics() {
        let spec = lr_spec("d", "hit", "s", r"\d");
        let items = lr_apply_vector(
            lr_vector(vec![
                (lr_labels(&[("s", "5")]), 1.0),
                (lr_labels(&[("s", "٥")]), 2.0),
            ]),
            &spec,
        );
        assert_eq!(items[0].labels, lr_labels(&[("d", "hit"), ("s", "5")]));
        assert_eq!(
            items[1].labels,
            lr_labels(&[("s", "٥")]),
            "an Arabic-Indic digit must NOT match RE2's ASCII \\d"
        );
    }

    /// Fix round 1's `[high]` witness: the collision merge CLONES each
    /// distinct post-rewrite label set (`slot.insert(s.labels.clone(),
    /// …)`), so the budget must price the input's EXISTING label bytes.
    /// Two colliding series carry a 64 KiB label the replacement never
    /// touches (no `$`, so the expansion bound prices none of it); every
    /// other model term totals under 5 KiB, so a 32 KiB cap admits the
    /// call under the old expression and then the merge allocates two
    /// 64 KiB clones nothing charged. The `merge_clone` term is the fix,
    /// and this test is its mutant: delete that term and the charge
    /// falls back under the cap, the apply succeeds, and the refusal
    /// assertion reddens (tripped for real during fix round 1).
    #[test]
    fn label_replace_charges_the_collision_merge_clone_before_it_allocates() {
        let spec = lr_spec("s", "same", "s", "[ab]");
        let wide = "x".repeat(64 * 1024);
        let series = |tag: &str| MatrixSeries {
            labels: lr_labels(&[("blob", wide.as_str()), ("s", tag)]),
            points: vec![(0, 1.0)],
        };
        let matrix = || QueryResult::Matrix(vec![series("a"), series("b")]);
        let mut charged = 0u64;
        match apply_label_replace_capped(&mut charged, matrix(), &spec, 32 * 1024) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes { .. })) => {}
            other => panic!("expected the byte-cap refusal, got {other:?}"),
        }
        assert_eq!(charged, 0, "a refused charge must not mutate the counter");
        // The same input under the real cap: the merge is paid for, runs,
        // and discharges.
        let mut charged = 0u64;
        let out = apply_label_replace_capped(&mut charged, matrix(), &spec, MAX_POST_AGG_BYTES)
            .expect("within cap");
        assert_eq!(charged, 0, "the ledger must discharge on success");
        match out {
            QueryResult::Matrix(items) => assert_eq!(items.len(), 1, "the collision merged"),
            other => panic!("expected a matrix, got {other:?}"),
        }
    }

    /// The heap merge's stability contract, driven directly (fix round
    /// 1's `[med]` rewrite): within-list duplicate timestamps keep their
    /// order (only a list's head is ever in the heap), and a cross-list
    /// tie resolves to the earlier list EVERY time it recurs — including
    /// the three-way tie at 30.
    #[test]
    fn merge_point_lists_is_a_stable_merge_under_duplicate_timestamps() {
        let merged = merge_point_lists(vec![
            vec![(10, 1.0), (10, 2.0), (30, 3.0)],
            vec![(10, 4.0), (20, 5.0), (30, 6.0)],
            vec![(5, 7.0), (30, 8.0)],
        ]);
        assert_eq!(
            merged,
            vec![
                (5, 7.0),
                (10, 1.0),
                (10, 2.0),
                (10, 4.0),
                (20, 5.0),
                (30, 3.0),
                (30, 6.0),
                (30, 8.0)
            ]
        );
    }

    /// The cap seam: a refused charge transforms nothing, and the
    /// charge/discharge symmetry holds on both paths.
    #[test]
    fn label_replace_cap_seam_refuses_cleanly_and_discharges() {
        let spec = lr_spec("d", "v", "s", ".*");
        let input = || lr_vector(vec![(lr_labels(&[("s", "a")]), 1.0)]);
        let mut charged = 0u64;
        match apply_label_replace_capped(&mut charged, input(), &spec, 1) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes { .. })) => {}
            other => panic!("expected the byte-cap refusal, got {other:?}"),
        }
        assert_eq!(charged, 0, "a refused charge must not mutate the counter");
        let mut charged = 0u64;
        apply_label_replace_capped(&mut charged, input(), &spec, MAX_POST_AGG_BYTES)
            .expect("within cap");
        assert_eq!(charged, 0, "the ledger must discharge on success");
    }

    // =================================================================
    // Issue #290 — the class-(P) preflight: the differential, the
    // read counter, the scratch charge and the skip path.
    // =================================================================

    /// **The oracle: the join with NO preflight and an unrefusable
    /// budget.** A faithful copy of [`combine_binary_capped`]'s
    /// post-charge `match`, driven by `Ledger::for_test`. It is what the
    /// preflight's answers are differentiated against, so the two cannot
    /// drift silently — 491 520 + 98 cases pin them equal.
    fn join_only(
        op: BinOp,
        return_bool: bool,
        matching: Option<&VectorMatching>,
        lhs: QueryResult,
        rhs: QueryResult,
    ) -> Result<QueryResult, ReadError> {
        let mut charged = 0u64;
        let l = Ledger::for_test(&mut charged);
        match (lhs, rhs) {
            (QueryResult::Scalar(a), QueryResult::Scalar(b)) => {
                if is_set_op(op) {
                    return Err(set_op_scalar_error(op));
                }
                let v = if op.is_comparison() {
                    if compare(op, a, b) { 1.0 } else { 0.0 }
                } else {
                    arith(op, a, b)
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
            _ => Err(incompatible_types_error()),
        }
    }

    /// The comparable identity of a refusal: the reason text for the
    /// semantic errors, the debug form for anything else. Comparing the
    /// TEXT is what makes "the preflight returns the error the join would
    /// have returned" a claim about what a client sees.
    fn err_key(e: &ReadError) -> String {
        match e {
            ReadError::PipelineInvalid { reason } => reason.clone(),
            other => format!("{other:?}"),
        }
    }

    fn err_of<T>(r: Result<T, ReadError>) -> Option<String> {
        r.err().map(|e| err_key(&e))
    }

    /// The pre-committed 5-series multiset over `{x, y}`, key-sorted.
    /// It contains one EXACT duplicate (index 2 repeats index 0), one
    /// signature-colliding pair under `on(x)`/`ignoring(y)`, and one
    /// series carrying no `y` — which as the ONE side is the include-copy
    /// collapse that reaches `grouping_unique_error`.
    const DIFF_SERIES: [&[(&str, &str)]; 5] = [
        &[("x", "1"), ("y", "p")],
        &[("x", "1"), ("y", "q")],
        &[("x", "1"), ("y", "p")],
        &[("x", "2"), ("y", "p")],
        &[("x", "1")],
    ];

    fn diff_labels(i: usize) -> Vec<(String, String)> {
        DIFF_SERIES[i]
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    /// The three pre-committed timestamp assignments. `T0` puts every
    /// series on every step; `T1` gives colliding pairs steps they never
    /// share (the admit-side control, and the source of steps where one
    /// side is empty and the other is not — `instant_join`'s
    /// `if lhs.is_empty() || rhs.is_empty()` short-circuit); `T2`
    /// overlaps partially, so "earliest step wins" is exercised.
    fn diff_stamps(pattern: usize, i: usize) -> &'static [i64] {
        match pattern {
            0 => &[1, 2, 3],
            1 => match i % 3 {
                0 => &[1],
                1 => &[2],
                _ => &[3],
            },
            _ => {
                if i.is_multiple_of(2) {
                    &[1, 2]
                } else {
                    &[2, 3]
                }
            }
        }
    }

    /// `shape == 0` is the instant (one-virtual-step) form; `1..=3`
    /// select `T0`/`T1`/`T2` over matrix operands.
    fn diff_side(subset: u32, shape: usize) -> QueryResult {
        let members = (0..5usize).filter(|i| (subset >> i) & 1 == 1);
        if shape == 0 {
            QueryResult::Vector(
                members
                    .map(|i| VectorSample {
                        labels: diff_labels(i),
                        value: (i + 1) as f64,
                    })
                    .collect(),
            )
        } else {
            QueryResult::Matrix(
                members
                    .map(|i| MatrixSeries {
                        labels: diff_labels(i),
                        points: diff_stamps(shape - 1, i)
                            .iter()
                            .map(|&t| (t, (i + 1) as f64))
                            .collect(),
                    })
                    .collect(),
            )
        }
    }

    /// Axis A — `(op, return_bool)`, six entries.
    ///
    /// Six and not ten, and the reason is scoped to what THESE runs
    /// drive. `diff_side` builds vector and matrix operands only, so the
    /// reader of `return_bool` they reach is `instant_join`'s
    /// `if op.is_comparison()` block. `scalar_apply` reads it too, on the
    /// scalar arms this axis never produces; `set_op_join` does not read
    /// it at all. In that block the
    /// effect is on `value`/`keep`, and `keep` gates `out.push` AFTER the
    /// duplicate checks (`instant_join`'s "Duplicate detection — BEFORE
    /// the keep filter"). `(Gt, true)` is kept as the control that says
    /// so, and pairing `true` with the non-comparison ops adds no branch
    /// these runs would take.
    const AXIS_A: [(BinOp, bool); 6] = [
        (BinOp::Div, false),
        (BinOp::Gt, false),
        (BinOp::Gt, true),
        (BinOp::And, false),
        (BinOp::Or, false),
        (BinOp::Unless, false),
    ];

    /// Axis B — the eight matching clauses.
    fn axis_b() -> Vec<Option<VectorMatching>> {
        let x = || vec!["x".to_string()];
        let y = || vec!["y".to_string()];
        let inc_y = || vec!["y".to_string()];
        vec![
            None,
            Some(VectorMatching {
                on: true,
                labels: x(),
                group: None,
            }),
            Some(VectorMatching {
                on: false,
                labels: y(),
                group: None,
            }),
            Some(VectorMatching {
                on: true,
                labels: x(),
                group: Some(MatchGroup::Left(Vec::new())),
            }),
            Some(VectorMatching {
                on: true,
                labels: x(),
                group: Some(MatchGroup::Left(inc_y())),
            }),
            Some(VectorMatching {
                on: false,
                labels: y(),
                group: Some(MatchGroup::Left(inc_y())),
            }),
            Some(VectorMatching {
                on: true,
                labels: x(),
                group: Some(MatchGroup::Right(Vec::new())),
            }),
            Some(VectorMatching {
                on: true,
                labels: x(),
                group: Some(MatchGroup::Right(inc_y())),
            }),
        ]
    }

    /// Axis D — one instant shape plus the nine independent
    /// `(T_lhs, T_rhs)` matrix pairs.
    fn axis_d() -> Vec<(usize, usize)> {
        let mut out = vec![(0usize, 0usize)];
        for l in 1..=3 {
            for r in 1..=3 {
                out.push((l, r));
            }
        }
        out
    }

    /// A `(stage_charged, stage_cap)` pair whose charge refuses for EVERY
    /// operand pair — `1 + bytes > 0` holds however small `bytes` is, so
    /// [`decide_binary`]'s guard never skips. The differential, the shape
    /// pin and the read counter all drive it: their subject is the
    /// preflight itself, and a guard that skipped would make them measure
    /// nothing. What the guard does on the other side of that condition
    /// is the subject of its own tests.
    const ALWAYS_REFUSING: (u64, u64) = (1, 0);

    /// The three-way relation, for one case. Soundness (the preflight
    /// invents no rejection), completeness (it misses none of the three
    /// join refusals) and message identity are ONE assertion now that all
    /// three are decided; the two byte assertions ride along because the
    /// scratch model has to hold on every input the differential drives,
    /// not on the four fixtures someone chose.
    fn diff_case(
        op: BinOp,
        return_bool: bool,
        matching: Option<&VectorMatching>,
        sl: u32,
        sr: u32,
        dl: usize,
        dr: usize,
    ) {
        let lhs = diff_side(sl, dl);
        let rhs = diff_side(sr, dr);
        let (lm, rm) = (measure_operand(&lhs), measure_operand(&rhs));
        let charge = preflight_scratch_bytes(&lm, &rm);
        PREFLIGHT_SCRATCH_CAP.with(|c| c.set(0));
        let decided = err_of(decide_binary(
            op,
            matching,
            lhs.clone(),
            rhs.clone(),
            ALWAYS_REFUSING.0,
            ALWAYS_REFUSING.1,
        ));
        let oracle = err_of(join_only(op, return_bool, matching, lhs, rhs));
        assert_eq!(
            decided, oracle,
            "op = {op:?} bool = {return_bool} matching = {matching:?} subsets = ({sl:#07b}, \
             {sr:#07b}) shapes = ({dl}, {dr})"
        );
        let observed = PREFLIGHT_SCRATCH_CAP.with(|c| c.get());
        assert!(
            observed <= charge,
            "the preflight held {observed} B of scratch against a {charge} B charge"
        );
        let modelled = binary_peak_bytes(op, matching, &lm, &rm);
        assert!(
            charge <= modelled,
            "the preflight charge {charge} must be dominated by the stage charge {modelled}"
        );
        let s = lm.series() + rm.series();
        let want = if s == 0 {
            0
        } else {
            PREFLIGHT_BYTES_PER_SERIES * s + PREFLIGHT_FLAT_BYTES
        };
        assert_eq!(
            charge, want,
            "the charge must be the closed form at S = {s}"
        );
    }

    /// One axis-A partition of the differential — 8 × 10 × 1024 cases.
    fn run_diff_partition(a: usize) -> usize {
        let (op, return_bool) = AXIS_A[a];
        let matchings = axis_b();
        let shapes = axis_d();
        let mut cases = 0usize;
        for m in &matchings {
            for &(dl, dr) in &shapes {
                for sl in 0..32u32 {
                    for sr in 0..32u32 {
                        diff_case(op, return_bool, m.as_ref(), sl, sr, dl, dr);
                        cases += 1;
                    }
                }
            }
        }
        assert_eq!(cases, 81_920, "partition {a} lost cases");
        cases
    }

    macro_rules! diff_partition {
        ($name:ident, $idx:expr) => {
            #[test]
            fn $name() {
                let started = std::time::Instant::now();
                let cases = run_diff_partition($idx);
                println!(
                    "#290 differential partition {} ({:?}): {cases} cases in {:?}",
                    $idx,
                    AXIS_A[$idx],
                    started.elapsed()
                );
            }
        };
    }

    diff_partition!(the_preflight_agrees_with_the_join_over_partition_0_div, 0);
    diff_partition!(the_preflight_agrees_with_the_join_over_partition_1_gt, 1);
    diff_partition!(
        the_preflight_agrees_with_the_join_over_partition_2_gt_bool,
        2
    );
    diff_partition!(the_preflight_agrees_with_the_join_over_partition_3_and, 3);
    diff_partition!(the_preflight_agrees_with_the_join_over_partition_4_or, 4);
    diff_partition!(
        the_preflight_agrees_with_the_join_over_partition_5_unless,
        5
    );

    /// A dropped partition fails by ARITHMETIC, not by someone noticing:
    /// the six partitions must cover the whole pre-committed space.
    #[test]
    fn the_differential_partitions_cover_the_whole_space() {
        assert_eq!(AXIS_A.len(), 6);
        assert_eq!(axis_b().len(), 8);
        assert_eq!(axis_d().len(), 10);
        let per_partition = axis_b().len() * axis_d().len() * 32 * 32;
        assert_eq!(per_partition, 81_920);
        assert_eq!(AXIS_A.len() * per_partition, 491_520);
    }

    /// Every [`QueryResult`] variant against every other, at two ops —
    /// the exhaustive pin of (P0) against the arm the real `match`
    /// selects.
    #[test]
    fn the_shape_dispatch_is_pinned_against_every_result_variant_pair() {
        let variants = || -> Vec<QueryResult> {
            vec![
                QueryResult::Streams {
                    items: Vec::new(),
                    partial: false,
                },
                QueryResult::Vector(Vec::new()),
                QueryResult::Matrix(Vec::new()),
                QueryResult::Scalar(1.0),
                QueryResult::String(String::new()),
                QueryResult::VectorHist(Vec::new()),
                QueryResult::MatrixHist(Vec::new()),
            ]
        };
        let mut cases = 0usize;
        for op in [BinOp::Div, BinOp::And] {
            for (li, _) in variants().into_iter().enumerate() {
                for (ri, _) in variants().into_iter().enumerate() {
                    let lhs = variants().remove(li);
                    let rhs = variants().remove(ri);
                    let decided = err_of(decide_binary(
                        op,
                        None,
                        lhs.clone(),
                        rhs.clone(),
                        ALWAYS_REFUSING.0,
                        ALWAYS_REFUSING.1,
                    ));
                    let oracle = err_of(join_only(op, false, None, lhs, rhs));
                    assert_eq!(decided, oracle, "op = {op:?} variants = ({li}, {ri})");
                    cases += 1;
                }
            }
        }
        assert_eq!(cases, 98);
    }

    // ---- the read counter -------------------------------------------

    /// One fixture series: `(labels, first timestamp, stride)`. A stride
    /// of 2 on two colliding series is what makes them never present at
    /// the same step.
    type ReadFixtureSeries<'a> = (&'a [(&'a str, &'a str)], i64, i64);

    fn matrix_side(series: &[ReadFixtureSeries<'_>], points_per_series: usize) -> QueryResult {
        QueryResult::Matrix(
            series
                .iter()
                .map(|(labels, offset, stride)| MatrixSeries {
                    labels: labels
                        .iter()
                        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                        .collect(),
                    points: (0..points_per_series)
                        .map(|t| (offset + t as i64 * stride, 1.0))
                        .collect(),
                })
                .collect(),
        )
    }

    fn points_read_for(
        matching: Option<&VectorMatching>,
        lhs: QueryResult,
        rhs: QueryResult,
    ) -> u64 {
        points_read_and_scratch_at(ALWAYS_REFUSING, matching, lhs, rhs).0
    }

    /// `(points read, scratch capacity observed)` for one decision at a
    /// given `(stage_charged, stage_cap)`.
    ///
    /// The scratch figure is `u64::MAX` when
    /// [`preflight::decide_binary_refusals`] did not reach its single
    /// exit — the sentinel is written BEFORE the call and only that exit
    /// overwrites it, so "still the sentinel" means the six
    /// `Vec::with_capacity` reservations below it never ran.
    ///
    /// That reading is exact only for the fixture class the callers use:
    /// an arithmetic vector⊗vector or matrix⊗matrix pair with series on
    /// BOTH sides, where neither of `decide_binary_refusals`' two early
    /// returns (set op or non-join shape; an empty side) applies, so
    /// entering the function means reserving the buffers. For any other
    /// shape the sentinel says only "the buffers were not reserved",
    /// which is weaker than "the function was not entered". The measured
    /// statement, closed over the whole callee closure, is
    /// `tests/logql_preflight_guard_gate.rs`.
    fn points_read_and_scratch_at(
        (stage_charged, stage_cap): (u64, u64),
        matching: Option<&VectorMatching>,
        lhs: QueryResult,
        rhs: QueryResult,
    ) -> (u64, u64) {
        PREFLIGHT_POINTS_READ.with(|c| c.set(0));
        PREFLIGHT_SCRATCH_CAP.with(|c| c.set(u64::MAX));
        let _ = decide_binary(BinOp::Div, matching, lhs, rhs, stage_charged, stage_cap);
        (
            PREFLIGHT_POINTS_READ.with(|c| c.get()),
            PREFLIGHT_SCRATCH_CAP.with(|c| c.get()),
        )
    }

    fn on_x_group_left_empty() -> VectorMatching {
        VectorMatching {
            on: true,
            labels: vec!["x".to_string()],
            group: Some(MatchGroup::Left(Vec::new())),
        }
    }

    /// The five collision shapes the counter is asserted over. Each is
    /// `(name, matching, lhs, rhs)` built at `n` points per series. Index
    /// 0 is the no-collision control; 1–3 refuse at their first step; 4
    /// sweeps every step and admits, which is where a quadratic
    /// traversal becomes visible.
    fn read_fixtures(
        n: usize,
    ) -> Vec<(
        &'static str,
        Option<VectorMatching>,
        QueryResult,
        QueryResult,
    )> {
        let distinct_l: &[ReadFixtureSeries<'_>] = &[
            (&[("x", "1")], 0, 1),
            (&[("x", "2")], 0, 1),
            (&[("x", "3")], 0, 1),
            (&[("x", "4")], 0, 1),
        ];
        let distinct_r: &[ReadFixtureSeries<'_>] = &[
            (&[("x", "1")], 0, 1),
            (&[("x", "2")], 0, 1),
            (&[("x", "3")], 0, 1),
            (&[("x", "4")], 0, 1),
        ];
        // Two ONE-side series sharing the `on(x)` signature.
        let dup_one: &[ReadFixtureSeries<'_>] = &[
            (&[("x", "1"), ("z", "a")], 0, 1),
            (&[("x", "1"), ("z", "b")], 0, 1),
            (&[("x", "2")], 0, 1),
            (&[("x", "3")], 0, 1),
        ];
        // The SAME collision on interleaved steps, so the two colliding
        // series are never present together. Stage 3 cannot rule it out
        // — they do share a signature — so Stage 4 sweeps every step and
        // returns `Ok`. This is the fixture that measures the SWEEP: the
        // three refusing ones below stop at their first step, where a
        // quadratic traversal never gets the chance to show itself.
        let dup_one_disjoint: &[ReadFixtureSeries<'_>] = &[
            (&[("x", "1"), ("z", "a")], 0, 2),
            (&[("x", "1"), ("z", "b")], 1, 2),
            (&[("x", "2")], 0, 1),
            (&[("x", "3")], 0, 1),
        ];
        // Two MANY-side series sharing the full signature (one-to-one).
        let dup_many: &[ReadFixtureSeries<'_>] = &[
            (&[("x", "1")], 0, 1),
            (&[("x", "1")], 0, 1),
            (&[("x", "2")], 0, 1),
            (&[("x", "3")], 0, 1),
        ];
        // The grouped collapse: two many series differing only in `y`,
        // with `y` copied from a one side that has none.
        let collapse_many: &[ReadFixtureSeries<'_>] = &[
            (&[("x", "1"), ("y", "p")], 0, 1),
            (&[("x", "1"), ("y", "q")], 0, 1),
        ];
        let collapse_one: &[ReadFixtureSeries<'_>] = &[(&[("x", "1")], 0, 1)];
        vec![
            (
                "no-collision",
                Some(on_x_group_left_empty()),
                matrix_side(distinct_l, n),
                matrix_side(distinct_r, n),
            ),
            (
                "one-side collision",
                Some(on_x_group_left_empty()),
                matrix_side(distinct_l, n),
                matrix_side(dup_one, n),
            ),
            (
                "many-side collision, one-to-one",
                None,
                matrix_side(dup_many, n),
                matrix_side(distinct_r, n),
            ),
            (
                "many-side collision, grouped",
                Some(VectorMatching {
                    on: false,
                    labels: vec!["y".to_string()],
                    group: Some(MatchGroup::Left(vec!["y".to_string()])),
                }),
                matrix_side(collapse_many, n),
                matrix_side(collapse_one, n),
            ),
            (
                "one-side collision that never co-occurs — a FULL sweep, admitted",
                Some(on_x_group_left_empty()),
                matrix_side(distinct_l, n),
                matrix_side(dup_one_disjoint, n),
            ),
        ]
    }

    fn total_points(r: &QueryResult) -> u64 {
        match r {
            QueryResult::Matrix(items) => items
                .iter()
                .fold(0u64, |a, s| a.saturating_add(s.points.len() as u64)),
            QueryResult::Vector(items) => items.len() as u64,
            _ => 0,
        }
    }

    /// **No point is read unless two series on the SAME side share the
    /// identity that side's refusal is keyed on.** An identity at ten
    /// times the points, not a threshold — a threshold would pass for a
    /// traversal that reads a constant fraction.
    #[test]
    fn the_preflight_reads_no_point_without_a_collision() {
        for n in [4usize, 40] {
            let mut fx = read_fixtures(n);
            let (name, m, lhs, rhs) = fx.remove(0);
            assert_eq!(name, "no-collision");
            assert_eq!(
                points_read_for(m.as_ref(), lhs, rhs),
                0,
                "the no-collision fixture read a point at {n} points per series"
            );
        }
    }

    /// On a collision the reads stay inside the operands' own point
    /// totals, and grow LINEARLY with them — a nested sweep fails the
    /// ratio even though it would pass a ceiling chosen for the small
    /// fixture.
    #[test]
    fn the_preflight_reads_stay_linear_in_the_points_it_must_examine() {
        for i in 1..5usize {
            let mut small = read_fixtures(4);
            let (name, m, lhs, rhs) = small.remove(i);
            let bound = 2 * (total_points(&lhs) + total_points(&rhs));
            let reads_small = points_read_for(m.as_ref(), lhs, rhs);
            assert!(
                reads_small > 0,
                "{name}: the collision fixture must reach the step sweep"
            );
            assert!(
                reads_small <= bound,
                "{name}: read {reads_small} points against a {bound}-point bound"
            );

            let mut big = read_fixtures(40);
            let (_, m, lhs, rhs) = big.remove(i);
            let bound = 2 * (total_points(&lhs) + total_points(&rhs));
            let reads_big = points_read_for(m.as_ref(), lhs, rhs);
            assert!(
                reads_big <= bound,
                "{name}: read {reads_big} points against a {bound}-point bound at 10x"
            );
            assert!(
                reads_big <= reads_small * 11,
                "{name}: reads grew {reads_small} -> {reads_big} for a 10x point count — that \
                 is superlinear"
            );
        }
    }

    // ---- the guard --------------------------------------------------

    /// **A query the charge admits pays NOTHING for the preflight** —
    /// no point read, and not one of the six scratch buffers reserved.
    ///
    /// This is the ordinary-traffic path: these fixtures model a few
    /// kilobytes against an 8 GiB cap, so [`decide_binary`]'s guard
    /// short-circuits before `PreflightCharge::acquire`. Every collision
    /// shape is driven, INCLUDING the one-to-one fixture with no include
    /// list, because a guard that only covered grouped joins would be the
    /// claim this test exists to stop being false.
    ///
    /// The second half is what makes the first half evidence: the same
    /// five fixtures at a refusing charge must reserve buffers, and the
    /// four with a collision must read points. Without it the test would
    /// pass on a preflight that had been deleted.
    #[test]
    fn the_guard_reads_no_point_when_the_charge_cannot_refuse() {
        const ADMITS: (u64, u64) = (0, MAX_POST_AGG_BYTES);
        for n in [4usize, 40] {
            for (name, m, lhs, rhs) in read_fixtures(n) {
                let (reads, scratch) = points_read_and_scratch_at(ADMITS, m.as_ref(), lhs, rhs);
                assert_eq!(
                    reads, 0,
                    "{name} ({n} points/series): the admitted path read {reads} points — the \
                     guard is not short-circuiting"
                );
                assert_eq!(
                    scratch,
                    u64::MAX,
                    "{name} ({n} points/series): the admitted path reserved scratch — the guard \
                     is not short-circuiting before the six buffers"
                );
            }
        }

        // Non-vacuity: at a refusing charge the same fixtures DO the
        // work. Fixture 0 has no collision, so it reserves its buffers
        // and reads no point; 1..5 reach the step sweep.
        for (i, (name, m, lhs, rhs)) in read_fixtures(4).into_iter().enumerate() {
            let (reads, scratch) =
                points_read_and_scratch_at(ALWAYS_REFUSING, m.as_ref(), lhs, rhs);
            assert_ne!(
                scratch,
                u64::MAX,
                "{name}: a refusing charge must run the preflight, or the guard test above \
                 compares nothing"
            );
            if i > 0 {
                assert!(
                    reads > 0,
                    "{name}: a refusing charge must reach the step sweep"
                );
            }
        }
    }

    /// **The guard skips exactly when the charge admits** — the boundary,
    /// at the one byte where the answer changes.
    ///
    /// At `cap = T` the stage charge admits and the preflight is skipped;
    /// at `cap = T - 1` it refuses and the preflight runs. Both are
    /// asserted on the SAME fixture, so the difference is the guard and
    /// nothing else, and both are asserted on the scratch sentinel rather
    /// than on the returned error — the answer is the semantic one either
    /// way at `T - 1`, and at `T` the join produces it a moment later.
    #[test]
    fn the_guard_turns_over_at_the_exact_byte_the_charge_refuses_on() {
        let matching = VectorMatching {
            on: true,
            labels: vec!["x".to_string()],
            group: None,
        };
        let pair = |k: &str, v: &str| (k.to_string(), v.to_string());
        let many = QueryResult::Vector(vec![
            VectorSample {
                labels: vec![pair("a", "1"), pair("x", "1")],
                value: 1.0,
            },
            VectorSample {
                labels: vec![pair("a", "2"), pair("x", "1")],
                value: 2.0,
            },
        ]);
        let one = QueryResult::Vector(vec![VectorSample {
            labels: vec![pair("x", "1")],
            value: 3.0,
        }]);
        let (lm, rm) = (measure_operand(&many), measure_operand(&one));
        let t = binary_peak_bytes(BinOp::Div, Some(&matching), &lm, &rm);
        assert!(t > 0, "the fixture must model a nonzero envelope");

        let (_, admitted) =
            points_read_and_scratch_at((0, t), Some(&matching), many.clone(), one.clone());
        assert_eq!(
            admitted,
            u64::MAX,
            "at cap = T the charge admits, so the preflight must not run"
        );
        let (_, refused) = points_read_and_scratch_at((0, t - 1), Some(&matching), many, one);
        assert_ne!(
            refused,
            u64::MAX,
            "at cap = T - 1 the charge refuses, so the preflight MUST run — this is the byte the \
             guard turns over on"
        );
    }

    // ---- the scratch model ------------------------------------------

    /// The published figures, by EQUALITY. They are derived
    /// (`2 · size_of::<SideSeries>() + 56`, `6 · MIN_ALLOC_BYTES`), so a
    /// wider view type moves them and this is what says so.
    #[test]
    fn the_preflight_constants_are_the_published_figures() {
        assert_eq!(PREFLIGHT_BYTES_PER_SERIES, 120);
        assert_eq!(PREFLIGHT_FLAT_BYTES, 192);
        assert_eq!(MAX_POST_AGG_BYTES / B_SERIES, 6_753_093);
        assert_eq!(MAX_BINARY_PREFLIGHT_BYTES, 810_371_352);
        assert_eq!(
            MAX_BINARY_PREFLIGHT_BYTES,
            PREFLIGHT_BYTES_PER_SERIES * (MAX_POST_AGG_BYTES / B_SERIES) + PREFLIGHT_FLAT_BYTES
        );
    }

    /// The six buffers at their real reserved counts fit inside the
    /// charge, and the charge is dominated by the stage model, across a
    /// series sweep that includes the small inputs where the 32-byte
    /// allocator floor and not the product dominates.
    #[test]
    fn the_scratch_model_covers_the_six_buffers_and_stays_under_the_stage_charge() {
        let blk = crate::logql::charge::alloc_block_bytes;
        let w = 2 * size_of::<preflight::SideSeries<'static>>() as u64 / 2;
        for &nl in &[0u64, 1, 2, 3, 7, 64, 4096] {
            for &nr in &[0u64, 1, 2, 3, 7, 64, 4096] {
                let lm = StageInput::for_derivation(nl, 0, 0, 0, 0, 0, nl, 1);
                let rm = StageInput::for_derivation(nr, 0, 0, 0, 0, 0, nr, 1);
                let charge = preflight_scratch_bytes(&lm, &rm);
                let s = nl + nr;
                if s == 0 {
                    assert_eq!(charge, 0);
                    continue;
                }
                let buffers =
                    blk(nl * w) + blk(nr * w) + blk(s * 4) + blk(s * 4) + blk(s * 4) + blk(s * 16);
                assert!(
                    buffers <= charge,
                    "S = {s}: the six buffers price {buffers} B against a {charge} B charge"
                );
                assert!(
                    charge <= B_SERIES * s,
                    "S = {s}: the preflight charge {charge} must be dominated by B_SERIES · S"
                );
            }
        }
    }

    /// The preflight's own admission is a real check, not a formality:
    /// a charge sized from one pair refuses a body handed another.
    #[test]
    fn the_preflight_charge_refuses_a_wider_pair_than_it_was_sized_for() {
        let one = QueryResult::Vector(vec![VectorSample {
            labels: diff_labels(0),
            value: 1.0,
        }]);
        let narrow = measure_operand(&one);
        let pc = PreflightCharge::acquire(&narrow, &narrow).expect("well inside the ceiling");
        assert_eq!(
            pc.bytes(),
            PREFLIGHT_BYTES_PER_SERIES * 2 + PREFLIGHT_FLAT_BYTES
        );
        let wide = QueryResult::Vector(
            (0..64)
                .map(|i| VectorSample {
                    labels: vec![("x".to_string(), format!("{i:08}"))],
                    value: 1.0,
                })
                .collect(),
        );
        match preflight::decide_binary_refusals(BinOp::Div, None, &wide, &wide, &pc) {
            Err(ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes { bytes, cap })) => {
                assert_eq!(bytes, pc.bytes());
                assert_eq!(cap, MAX_BINARY_PREFLIGHT_BYTES);
            }
            other => panic!("expected the preflight's own admission to refuse, got {other:?}"),
        }
    }

    // ---- the skip path ----------------------------------------------

    /// Runs `f` with the preflight ceiling forced, so the skip branch —
    /// unreachable below ~6.75 million combined series, and therefore
    /// untested in CI by construction — is exercised.
    fn with_preflight_ceiling<T>(ceiling: u64, f: impl FnOnce() -> T) -> T {
        PREFLIGHT_CEILING_OVERRIDE.with(|c| c.set(Some(ceiling)));
        let out = f();
        PREFLIGHT_CEILING_OVERRIDE.with(|c| c.set(None));
        out
    }

    /// **A skipped (P1) is behaviour-preserving, by BOTH routes that skip
    /// it.** Every case's answer is "pre-change behaviour with the (P0)
    /// corrections applied": the shape refusals still win above the
    /// charge, and everything else is the join's own answer.
    ///
    /// The two routes are asserted on the same case, in the same loop:
    ///
    /// * **the guard** — the run at `cap = MAX_POST_AGG_BYTES` with a
    ///   clean counter, where the charge admits and [`decide_binary`]
    ///   never enters (P1). This is the ordinary-traffic path, and these
    ///   81 930 cases are what say the guard changes no answer.
    /// * **the ceiling** — the same call with the preflight ceiling
    ///   forced to 0, i.e. the scratch skip.
    ///
    /// The ceiling route also has a discriminating test of its own at a
    /// REFUSING cap, where the guard does not skip and the two come
    /// apart: `the_join_refusals_are_preempted_by_the_budget_when_the_preflight_is_skipped`.
    ///
    /// The P0-case count is asserted non-zero, because a run in which no
    /// case can distinguish the two oracles proves neither.
    #[test]
    fn a_skipped_preflight_leaves_behaviour_unchanged() {
        let (op, return_bool) = AXIS_A[0];
        let matchings = axis_b();
        let shapes = axis_d();
        let mut cases = 0usize;
        let mut p0_cases = 0usize;

        /// One case, both skip routes. `guard` is the run at the
        /// production cap with the ceiling left alone — the charge admits
        /// and (P1) is guarded off; `ceiling` is the same call with the
        /// scratch ceiling forced to 0.
        fn both_skips(
            op: BinOp,
            return_bool: bool,
            m: Option<&VectorMatching>,
            lhs: &QueryResult,
            rhs: &QueryResult,
        ) -> (Option<String>, Option<String>) {
            let mut charged = 0u64;
            let guard = err_of(combine_binary_capped(
                &mut charged,
                op,
                return_bool,
                m,
                lhs.clone(),
                rhs.clone(),
                MAX_POST_AGG_BYTES,
            ));
            let mut charged = 0u64;
            let ceiling = with_preflight_ceiling(0, || {
                err_of(combine_binary_capped(
                    &mut charged,
                    op,
                    return_bool,
                    m,
                    lhs.clone(),
                    rhs.clone(),
                    MAX_POST_AGG_BYTES,
                ))
            });
            (guard, ceiling)
        }

        for m in &matchings {
            for &(dl, dr) in &shapes {
                for sl in 0..32u32 {
                    for sr in 0..32u32 {
                        let lhs = diff_side(sl, dl);
                        let rhs = diff_side(sr, dr);
                        let (guard, ceiling) = both_skips(op, return_bool, m.as_ref(), &lhs, &rhs);
                        let shape = err_of(preflight::decide_shape(op, &lhs, &rhs));
                        if shape.is_some() {
                            p0_cases += 1;
                        }
                        let want = match shape {
                            Some(e) => Some(e),
                            None => err_of(join_only(op, return_bool, m.as_ref(), lhs, rhs)),
                        };
                        assert_eq!(
                            guard, want,
                            "the guarded skip diverged at ({sl:#07b}, {sr:#07b})"
                        );
                        assert_eq!(
                            ceiling, want,
                            "the ceiling skip diverged at ({sl:#07b}, {sr:#07b})"
                        );
                        cases += 1;
                    }
                }
            }
        }
        // The axis-A partition carries no scalar or mixed-shape operand,
        // so the P0 half would be vacuous on it alone; these
        // pre-committed pairs are what make the distinction real.
        for op in [BinOp::Div, BinOp::And] {
            for (lhs, rhs) in [
                (QueryResult::Scalar(1.0), QueryResult::Scalar(2.0)),
                (QueryResult::Scalar(1.0), QueryResult::Vector(Vec::new())),
                (QueryResult::Vector(Vec::new()), QueryResult::Scalar(2.0)),
                (
                    QueryResult::Vector(Vec::new()),
                    QueryResult::Matrix(Vec::new()),
                ),
                (
                    QueryResult::String(String::new()),
                    QueryResult::Vector(Vec::new()),
                ),
            ] {
                let (guard, ceiling) = both_skips(op, false, None, &lhs, &rhs);
                let shape = err_of(preflight::decide_shape(op, &lhs, &rhs));
                if shape.is_some() {
                    p0_cases += 1;
                }
                let want = match shape {
                    Some(e) => Some(e),
                    None => err_of(join_only(op, false, None, lhs, rhs)),
                };
                assert_eq!(guard, want, "the guarded skip diverged on a P0 pair");
                assert_eq!(ceiling, want, "the ceiling skip diverged on a P0 pair");
                cases += 1;
            }
        }
        assert_eq!(cases, 81_930);
        assert!(
            p0_cases > 0,
            "no case in the skip-mode run could distinguish the two oracles"
        );
        println!("#290 skip-mode run: {cases} cases, {p0_cases} of them (P0)");
    }

    /// **The defect, kept reproducible.** With the preflight skipped —
    /// which is byte-for-byte the pre-#290 ordering for the three join
    /// refusals — the same six fixtures that
    /// `tests/logql_semantics_before_budget.rs` asserts return a 400 at
    /// `cap = 0` come back as `MetricPostAggBytes`, i.e. a 422
    /// `query_too_broad` telling the client their query is too large when
    /// it is ambiguous.
    ///
    /// This is the discriminating half. Without it the runtime rows would
    /// pass on code that never had the bug, and "shown failing on today's
    /// code" would be a claim about a mutant nobody can re-run.
    #[test]
    fn the_join_refusals_are_preempted_by_the_budget_when_the_preflight_is_skipped() {
        let on_x_gl = VectorMatching {
            on: true,
            labels: vec!["x".to_string()],
            group: Some(MatchGroup::Left(Vec::new())),
        };
        let on_x = VectorMatching {
            on: true,
            labels: vec!["x".to_string()],
            group: None,
        };
        let ign_y_gl_y = VectorMatching {
            on: false,
            labels: vec!["y".to_string()],
            group: Some(MatchGroup::Left(vec!["y".to_string()])),
        };
        let s = |pairs: &[(&str, &str)], v: f64| VectorSample {
            labels: pairs
                .iter()
                .map(|(k, val)| ((*k).to_string(), (*val).to_string()))
                .collect(),
            value: v,
        };
        let m = |pairs: &[(&str, &str)], ts: &[i64]| MatrixSeries {
            labels: pairs
                .iter()
                .map(|(k, val)| ((*k).to_string(), (*val).to_string()))
                .collect(),
            points: ts.iter().map(|&t| (t, 1.0)).collect(),
        };
        let rows: Vec<(&str, &VectorMatching, QueryResult, QueryResult)> = vec![
            (
                "matrix, duplicate at a later step",
                &on_x_gl,
                QueryResult::Matrix(vec![m(&[("m", "a"), ("x", "1")], &[1, 2])]),
                QueryResult::Matrix(vec![
                    m(&[("x", "1"), ("z", "p")], &[1, 2]),
                    m(&[("x", "1"), ("z", "q")], &[2]),
                ]),
            ),
            (
                "matrix, empty leading steps on one side",
                &on_x_gl,
                QueryResult::Matrix(vec![m(&[("m", "a"), ("x", "1")], &[1, 2])]),
                QueryResult::Matrix(vec![
                    m(&[("x", "1"), ("z", "p")], &[2]),
                    m(&[("x", "1"), ("z", "q")], &[2]),
                ]),
            ),
            (
                "vector, duplicate one side",
                &on_x_gl,
                QueryResult::Vector(vec![s(&[("m", "a"), ("x", "1")], 10.0)]),
                QueryResult::Vector(vec![
                    s(&[("x", "1"), ("z", "p")], 2.0),
                    s(&[("x", "1"), ("z", "q")], 3.0),
                ]),
            ),
            (
                "grouped join with an empty include list",
                &on_x_gl,
                QueryResult::Vector(vec![
                    s(&[("x", "1"), ("y", "p")], 10.0),
                    s(&[("x", "1"), ("y", "p")], 20.0),
                ]),
                QueryResult::Vector(vec![s(&[("x", "1")], 2.0)]),
            ),
            (
                "one-to-one, multiple matches",
                &on_x,
                QueryResult::Vector(vec![
                    s(&[("a", "1"), ("x", "1")], 10.0),
                    s(&[("a", "2"), ("x", "1")], 20.0),
                ]),
                QueryResult::Vector(vec![s(&[("x", "1")], 2.0)]),
            ),
            (
                "grouped join, include copy collapses two distinct many series",
                &ign_y_gl_y,
                QueryResult::Vector(vec![
                    s(&[("x", "1"), ("y", "p")], 10.0),
                    s(&[("x", "1"), ("y", "q")], 20.0),
                ]),
                QueryResult::Vector(vec![s(&[("x", "1")], 2.0)]),
            ),
        ];
        for (name, matching, lhs, rhs) in rows {
            // With the preflight ON, the semantic error wins at cap = 0.
            let mut charged = 0u64;
            let decided = combine_binary_capped(
                &mut charged,
                BinOp::Div,
                false,
                Some(matching),
                lhs.clone(),
                rhs.clone(),
                0,
            );
            assert!(
                matches!(decided, Err(ReadError::PipelineInvalid { .. })),
                "{name}: expected the semantic 400, got {decided:?}"
            );

            // With it SKIPPED — the pre-#290 ordering — the budget
            // answers first, which is the defect.
            let mut charged = 0u64;
            let preempted = with_preflight_ceiling(0, || {
                combine_binary_capped(&mut charged, BinOp::Div, false, Some(matching), lhs, rhs, 0)
            });
            match preempted {
                Err(ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes { .. })) => {}
                other => panic!(
                    "{name}: the un-preflighted path must still be preempted by the budget — \
                     if it is not, this row no longer discriminates the defect: {other:?}"
                ),
            }
        }
    }
}
