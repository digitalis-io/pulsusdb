//! The aggregation vocabulary shared by the sliding state, the fold and
//! the post-aggregation funnel.
//!
//! [`LabelSet`] and the [`RangeSeries`]/[`InstantSeries`] result shapes,
//! the deterministic orderings ([`pin_reduction_order`],
//! [`instant_payload_cmp`], [`range_payload_cmp`]) and the reference-exact
//! [`VectorAccum`] recurrence every reducing op folds through.

use pulsus_logql::VectorAggOp;
use std::collections::BTreeMap;

/// The label set a fingerprint with no hydrated meta stands for — issue
/// #236 P2 sizes its charge over this so the charge/discharge pair stays
/// exact on a fingerprint `base_labels` never saw (`cloned().unwrap_or_
/// default()` yields exactly this).
pub(in crate::logql) static EMPTY_LABEL_SET: LabelSet = Vec::new();

pub(in crate::logql) type LabelSet = Vec<(String, String)>;

pub(in crate::logql) struct RangeSeries {
    pub(in crate::logql) labels: LabelSet,
    pub(in crate::logql) points: BTreeMap<i64, f64>,
}

pub(in crate::logql) struct InstantSeries {
    pub(in crate::logql) labels: LabelSet,
    pub(in crate::logql) value: f64,
}

/// **The reduction-order pin** (issue #236 Part B, task-manager ruling
/// "Option B").
///
/// [`VectorAccum`] transcribes the reference's Welford recurrence, which
/// is **order-sensitive**: a group's `avg`/`stddev`/`stdvar` depends on
/// the order its members are accumulated in. PulsusDB's member order is
/// the order series arrive from the leaf, and the leaf emits by walking a
/// `HashMap` (`ClientAggState::finish` over `label_groups`/`fp_groups`).
/// Rust's default hasher is randomly seeded PER PROCESS, so without this
/// pin the same query returns different bits on different runs —
/// measured at 6 failures in 20 runs of `logqltest_corpus` before the pin
/// existed.
///
/// So the members are put in a total order that is a property of the
/// DATA, not of a hash seed: ascending by label set, which is the series'
/// own identity and the canonical order used throughout this crate.
/// Applied once per stage, immediately before grouping — never inside a
/// group's reduce (that would be `O(k log k)` per group on the read hot
/// path) — and AFTER the selection/reorder early returns, so
/// `select_k_*`/`sort_instant` keep their existing order contracts
/// untouched.
///
/// **Residual, stated because "deterministic" must not be read as
/// "identical".** This makes PulsusDB reproducible; it does not make it
/// order-identical to the reference, which walks a Go map and is itself
/// nondeterministic here (measured 10/2 over 12 runs on identical data).
/// The committed `b4_vector_aggs.test` captures land in a wide majority
/// basin — enumerating all 24 member orders of `{2,4,6,8}`, 20 of them
/// (including ascending) produce exactly the captured
/// `stdvar=5.0`/`stddev=2.23606797749979` — so the green corpus is real
/// evidence that this pin agrees with the reference on that data. It is
/// NOT proof that the sorted order is the reference's: on other data a
/// different member order could differ in the last bit. Plan v14's risk
/// #6 called these values "not capturable"; the accurate statement,
/// established by that enumeration, is **capturable but not
/// order-independent**.
pub(in crate::logql) fn pin_reduction_order<T>(
    series: &mut [T],
    labels_of: impl Fn(&T) -> &LabelSet,
    payload_cmp: impl Fn(&T, &T) -> std::cmp::Ordering,
) {
    series.sort_by(|a, b| {
        labels_of(a)
            .cmp(labels_of(b))
            .then_with(|| payload_cmp(a, b))
    });
}

/// A total order on an instant series' payload — its value's BITS, so
/// `NaN` orders too (`total_cmp`, never `partial_cmp`).
pub(in crate::logql) fn instant_payload_cmp(
    a: &InstantSeries,
    b: &InstantSeries,
) -> std::cmp::Ordering {
    a.value.total_cmp(&b.value)
}

/// A total order on a range series' payload: the `(timestamp, value)`
/// sequence, lexicographically, values by BITS. Allocates nothing.
pub(in crate::logql) fn range_payload_cmp(
    a: impl Iterator<Item = (i64, f64)>,
    b: impl Iterator<Item = (i64, f64)>,
) -> std::cmp::Ordering {
    let mut a = a;
    let mut b = b;
    loop {
        match (a.next(), b.next()) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (Some((ta, va)), Some((tb, vb))) => {
                let o = ta.cmp(&tb).then_with(|| va.total_cmp(&vb));
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
        }
    }
}

/// The reference's per-op STREAMING accumulator, transcribed arm for arm
/// from grafana/loki v3.7.4 `pkg/logql/evaluator.go` — seed `:479-486`
/// (plus the `Stddev`/`Stdvar` zeroing at `:491-492`), update `:522-580`,
/// finish `:586-596`.
///
/// Used by BOTH [`reduce`] and the range fold, so instant and range can
/// never disagree on a value. It is simultaneously:
///
/// * **the parity fix.** PulsusDB previously computed `avg` as
///   `sum/len` and `stddev`/`stdvar` through a two-pass
///   `population_variance`; the reference uses Welford's online
///   recurrence, which produces DIFFERENT bits. And an all-NaN `min`/`max`
///   group folded to `±INF` here where the reference yields `NaN` (its
///   `group.value < s.F || IsNaN(group.value)` test seeds from the first
///   member and only ever replaces a NaN accumulator).
/// * **the enabler.** It is O(1) per group, which is what lets the fold
///   retain state proportional to OUTPUT groups rather than scanned ones.
///
/// Order-sensitivity, stated: Welford is not associative, so a group's
/// value depends on member order. The reference iterates a Go map and is
/// therefore order-NONDETERMINISTIC for `avg`/`stddev`/`stdvar` (measured
/// 10/2 over 12 runs on identical data), which is why those ops are
/// proved here by source-cited unit goldens and are NOT capturable from a
/// container. PulsusDB pins a deterministic order — the same treatment
/// the instant `first_over_time`/`last_over_time` tie already gets.
#[derive(Clone, Copy, Debug)]
pub(in crate::logql) struct VectorAccum {
    value: f64,
    mean: f64,
    count: u64,
}

impl VectorAccum {
    /// The "no member yet" state of a [`ReduceFold`] slot.
    ///
    /// `count == 0` is a SENTINEL, not a reachable accumulator: [`seed`]
    /// sets `count: 1` for **every** op and [`update`] never decrements,
    /// so a slot that has taken a value can never be mistaken for an
    /// empty one. Pinned by
    /// `vector_accum_seed_always_leaves_a_nonzero_count`, which drives
    /// every reducing op — the sentinel is what lets the fold hold a
    /// dense `Vec<VectorAccum>` (24 B/slot) instead of a
    /// `Vec<Option<VectorAccum>>` (32 B/slot) across a grid of up to
    /// [`MAX_ADMITTED_GRID_POINTS`] slots per group.
    ///
    /// [`seed`]: VectorAccum::seed
    /// [`update`]: VectorAccum::update
    pub(in crate::logql) const EMPTY: Self = VectorAccum {
        value: 0.0,
        mean: 0.0,
        count: 0,
    };

    /// Whether this slot has taken a value yet — see [`VectorAccum::EMPTY`].
    pub(in crate::logql) fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// `evaluator.go:479-486` — the first member seeds `value` AND `mean`
    /// with its own sample and `groupCount` with 1; `:491-492` then zeroes
    /// `value` for `stddev`/`stdvar` (their `value` accumulates M2, not a
    /// running total).
    pub(in crate::logql) fn seed(op: VectorAggOp, f: f64) -> Self {
        let value = match op {
            VectorAggOp::Stddev | VectorAggOp::Stdvar => 0.0,
            _ => f,
        };
        VectorAccum {
            value,
            mean: f,
            count: 1,
        }
    }

    /// `evaluator.go:522-580`, arm for arm.
    pub(in crate::logql) fn update(&mut self, op: VectorAggOp, f: f64) {
        match op {
            // `:527` group.value += s.F
            VectorAggOp::Sum => self.value += f,
            // `:530-531` groupCount++; mean += (s.F - mean) / groupCount
            VectorAggOp::Avg => {
                self.count += 1;
                self.mean += (f - self.mean) / self.count as f64;
            }
            // `:534-536` if group.value < s.F || IsNaN(group.value)
            VectorAggOp::Max => {
                if self.value < f || self.value.is_nan() {
                    self.value = f;
                }
            }
            // `:539-541` if group.value > s.F || IsNaN(group.value)
            VectorAggOp::Min => {
                if self.value > f || self.value.is_nan() {
                    self.value = f;
                }
            }
            // `:544` groupCount++
            VectorAggOp::Count => self.count += 1,
            // `:547-550` Welford: delta = s.F - mean; mean += delta/n;
            //            value += delta * (s.F - mean)   [the NEW mean]
            VectorAggOp::Stddev | VectorAggOp::Stdvar => {
                self.count += 1;
                let delta = f - self.mean;
                self.mean += delta / self.count as f64;
                self.value += delta * (f - self.mean);
            }
            // Selections and reorders never reach a reducing accumulator —
            // `group_range`/`group_instant` and the fold dispatch them to
            // `select_k_*`/`sort_instant` first. Guarded by
            // `is_reduction`, whose exhaustive match is the single source
            // of truth for this partition.
            VectorAggOp::Topk
            | VectorAggOp::Bottomk
            | VectorAggOp::ApproxTopk
            | VectorAggOp::Sort
            | VectorAggOp::SortDesc => {
                unreachable!("{op:?} is a selection/reorder, dispatched before any accumulator")
            }
        }
    }

    /// `evaluator.go:586-596`.
    pub(in crate::logql) fn finish(self, op: VectorAggOp) -> f64 {
        match op {
            // `:588` aggr.value = aggr.mean
            VectorAggOp::Avg => self.mean,
            // `:591` aggr.value = float64(aggr.groupCount)
            VectorAggOp::Count => self.count as f64,
            // `:594` sqrt(value / groupCount)
            VectorAggOp::Stddev => (self.value / self.count as f64).sqrt(),
            // `:596` value / groupCount
            VectorAggOp::Stdvar => self.value / self.count as f64,
            // Sum/Min/Max carry their answer in `value` untouched.
            VectorAggOp::Sum | VectorAggOp::Min | VectorAggOp::Max => self.value,
            VectorAggOp::Topk
            | VectorAggOp::Bottomk
            | VectorAggOp::ApproxTopk
            | VectorAggOp::Sort
            | VectorAggOp::SortDesc => {
                unreachable!("{op:?} is a selection/reorder, dispatched before any accumulator")
            }
        }
    }

    /// The reducing/selecting partition over `VectorAggOp`, as ONE
    /// exhaustive match with no `_` arm — a new operator is a build
    /// failure here until it is dispositioned, rather than silently
    /// landing in whichever branch the caller happened to write.
    pub(in crate::logql) fn is_reduction(op: VectorAggOp) -> bool {
        match op {
            VectorAggOp::Sum
            | VectorAggOp::Avg
            | VectorAggOp::Min
            | VectorAggOp::Max
            | VectorAggOp::Count
            | VectorAggOp::Stddev
            | VectorAggOp::Stdvar => true,
            VectorAggOp::Topk
            | VectorAggOp::Bottomk
            | VectorAggOp::ApproxTopk
            | VectorAggOp::Sort
            | VectorAggOp::SortDesc => false,
        }
    }
}

/// Reduces a materialised group through [`VectorAccum`], so the
/// materialising path and the streaming fold compute the SAME bits.
///
/// `vals` is never empty at any call site (a group exists because a
/// member created it), which is what makes the `seed`-then-`update`
/// shape total; the `unwrap_or(f64::NAN)` is defence-in-depth, matching
/// the reference's own behaviour of emitting no group at all rather than
/// a sentinel.
pub(in crate::logql) fn reduce(op: VectorAggOp, vals: &[f64]) -> f64 {
    debug_assert!(
        VectorAccum::is_reduction(op),
        "reduce called with the selection/reorder op {op:?}"
    );
    let Some((first, rest)) = vals.split_first() else {
        return f64::NAN;
    };
    let mut acc = VectorAccum::seed(op, *first);
    for v in rest {
        acc.update(op, *v);
    }
    acc.finish(op)
}

/// The `topk`/`bottomk` `k`: the parameter floored to a count; a missing
/// or non-positive parameter selects nothing (the planner already
/// rejects a missing `k` — defensive here).
pub(in crate::logql) fn k_of(param: Option<f64>) -> usize {
    match param {
        Some(p) if p >= 1.0 => p.floor() as usize,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::testkit::*;

    // -----------------------------------------------------------------
    // Issue #236 Part B — `VectorAccum`, the reference's streaming
    // accumulator.
    // -----------------------------------------------------------------

    /// AC 6 — every reducing arm reproduces grafana/loki v3.7.4
    /// `pkg/logql/evaluator.go` at the EXACT BITS, with each assertion
    /// carrying its line cite.
    ///
    /// The three datasets are chosen so each op DISCRIMINATES against the
    /// pre-#236 formula (`sum/len` for avg, a two-pass
    /// `population_variance` for stddev/stdvar): reverting any one arm
    /// changes the asserted bits. Compared through `to_bits`, never `==`
    /// — one float rendering is a prefix of another and `assert_eq!` on
    /// `f64` hides a last-bit difference in the printed output.
    #[test]
    fn vector_accum_reproduces_the_reference_recurrence_bit_for_bit() {
        fn run(op: VectorAggOp, vals: &[f64]) -> f64 {
            let mut acc = VectorAccum::seed(op, vals[0]);
            for v in &vals[1..] {
                acc.update(op, *v);
            }
            acc.finish(op)
        }

        // `:530-531` avg — Welford mean, NOT `sum/len`. Two-pass gives
        // 4610184818551597739; the recurrence gives one ULP below.
        assert_eq!(
            run(VectorAggOp::Avg, &[1.0, 1.0, 3.0]).to_bits(),
            4610184818551597738u64,
            "avg must be the Welford mean (evaluator.go:530-531, :588)"
        );

        // `:547-550` + `:596` stdvar — M2/n. Two-pass gives
        // 4597174419628082972.
        assert_eq!(
            run(VectorAggOp::Stdvar, &[1.0, 1.0, 2.0]).to_bits(),
            4597174419628082973u64,
            "stdvar must be Welford M2/n (evaluator.go:547-550, :596)"
        );

        // `:547-550` + `:594` stddev — sqrt(M2/n). Two-pass gives
        // 4612489961860455552.
        assert_eq!(
            run(VectorAggOp::Stddev, &[1.0, 1.0, 6.0]).to_bits(),
            4612489961860455551u64,
            "stddev must be sqrt(Welford M2/n) (evaluator.go:547-550, :594)"
        );

        // `:527` sum, `:544`+`:591` count — unchanged by the port.
        assert_eq!(run(VectorAggOp::Sum, &[1.0, 2.0, 4.0]), 7.0);
        assert_eq!(run(VectorAggOp::Count, &[9.0, 9.0, 9.0]), 3.0);

        // `:534-541` min/max over an ALL-NaN group is NaN, not ±INF.
        //
        // **Where the behavioural difference actually lives — measured,
        // not assumed.** The defect was the SEED, not the comparison:
        // PulsusDB's old `fold(f64::INFINITY, f64::min)` started from an
        // infinity that no member could displace, because Rust's
        // `f64::min` prefers the non-NaN operand. Seeding from the first
        // member (as `evaluator.go:481` does) is what fixes it.
        //
        // The `|| IsNaN(group.value)` disjunct is transcription fidelity
        // rather than a second behavioural fix: substituting
        // `self.value.min(f)` for the whole arm leaves every assertion
        // below passing, because Rust's `f64::min`/`max` already agree
        // with the reference's comparison on all three NaN placements.
        // Recorded so nobody reads these cases as pinning the disjunct —
        // they pin the seed.
        let nans = [f64::NAN, f64::NAN, f64::NAN];
        assert!(
            run(VectorAggOp::Min, &nans).is_nan(),
            "all-NaN min must be NaN (evaluator.go:539-541)"
        );
        assert!(
            run(VectorAggOp::Max, &nans).is_nan(),
            "all-NaN max must be NaN (evaluator.go:534-536)"
        );

        // ...but a NaN seed followed by a real sample takes the real one
        // (the `IsNaN(group.value)` disjunct), and a real seed followed by
        // NaN keeps the real one (every comparison with NaN is false).
        assert_eq!(run(VectorAggOp::Max, &[f64::NAN, 5.0]), 5.0);
        assert_eq!(run(VectorAggOp::Min, &[f64::NAN, 5.0]), 5.0);
        assert_eq!(run(VectorAggOp::Max, &[5.0, f64::NAN]), 5.0);
        assert_eq!(run(VectorAggOp::Min, &[5.0, f64::NAN]), 5.0);
    }

    /// `reduce` and the accumulator are the SAME computation — the
    /// property that lets the range fold and the materialising instant
    /// path never disagree. Asserted on bits over every reducing op.
    #[test]
    fn reduce_routes_through_the_accumulator_for_every_reducing_op() {
        const OPS: [VectorAggOp; 7] = [
            VectorAggOp::Sum,
            VectorAggOp::Avg,
            VectorAggOp::Min,
            VectorAggOp::Max,
            VectorAggOp::Count,
            VectorAggOp::Stddev,
            VectorAggOp::Stdvar,
        ];
        let vals = [1.0, 1.0, 3.0, -2.5, 7.25];
        for op in OPS {
            assert!(VectorAccum::is_reduction(op), "{op:?}");
            let mut acc = VectorAccum::seed(op, vals[0]);
            for v in &vals[1..] {
                acc.update(op, *v);
            }
            assert_eq!(
                reduce(op, &vals).to_bits(),
                acc.finish(op).to_bits(),
                "reduce and VectorAccum must agree bit-for-bit on {op:?}"
            );
        }
        // The partition is total and the selecting side is disjoint.
        for op in [
            VectorAggOp::Topk,
            VectorAggOp::Bottomk,
            VectorAggOp::ApproxTopk,
            VectorAggOp::Sort,
            VectorAggOp::SortDesc,
        ] {
            assert!(!VectorAccum::is_reduction(op), "{op:?}");
        }
    }

    /// The dense `Vec<VectorAccum>` a `ReduceFold` slot lives in uses
    /// `count == 0` as its "no member yet" sentinel. That is only sound
    /// because a SEEDED accumulator can never have `count == 0` — for
    /// EVERY reducing op, including the four that never touch `count`
    /// again. Break the seed's `count: 1` and this fails.
    #[test]
    fn vector_accum_seed_always_leaves_a_nonzero_count() {
        assert!(
            VectorAccum::EMPTY.is_empty(),
            "the sentinel must read as empty"
        );
        for op in REDUCING_OPS {
            for v in [0.0f64, -0.0, 1.5, f64::NAN, f64::INFINITY] {
                let mut acc = VectorAccum::seed(op, v);
                assert!(
                    !acc.is_empty(),
                    "{op:?} seeded from {v} must not read as the EMPTY sentinel"
                );
                acc.update(op, 2.0);
                assert!(!acc.is_empty(), "{op:?} after update");
            }
        }
    }
}
