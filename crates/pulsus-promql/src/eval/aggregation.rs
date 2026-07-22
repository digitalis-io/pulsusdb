//! The aggregation operators: `sum/avg/min/max/count/group/stddev/stdvar`
//! (reductions), `quantile` (reduction with a φ parameter),
//! `topk/bottomk/limitk/limit_ratio` (verbatim series selection), and
//! `count_values` (value-label injection + count), all with `by`/
//! `without` grouping. `sum`/`avg` use [`KahanSum`] (Neumaier-compensated
//! summation) and `stddev`/`stdvar` Welford's recurrence, **accumulation
//! order pinned to the input vector's own order** — which is itself
//! pinned to ascending-fingerprint order all the way back to the fetch
//! layer's `ORDER BY fingerprint, unix_milli` (docs/schemas.md §2.3) and
//! never reshuffled by a `HashMap` in between (every grouping step here
//! accumulates into per-group state in the same relative order the input
//! vector arrives in, regardless of the `HashMap`'s own bucket iteration
//! order). Exact last-ULP parity with Prometheus's own series-storage
//! accumulation order is a #33 differential concern (architect plan Open
//! Q1), not assumed here.

use std::cmp::Ordering;
use std::collections::HashMap;

use pulsus_model::{FloatHistogram, FloatHistogramOpError};
use xxhash_rust::xxh64::xxh64;

use crate::annotations::{Annotations, go_float, messages};
use crate::error::PromqlError;
use crate::eval::functions::quantile_of;
use crate::eval::labels::full_labels;
use crate::math::KahanSum;
use crate::plan::{AggOp, Grouping};
use crate::value::{InstantSample, Labels};

/// One aggregation group's identity (issue #69, M6-06, plan v2 Δ1): the
/// metric-name channel travels NEXT TO the non-name label set, mirroring
/// [`InstantSample`]'s own split-name invariant (docs/architecture.md
/// §2.3 — `Labels` never contains `__name__`). `name` is `Some` only
/// under `by (…, __name__, …)` over name-carrying inputs, so
/// `sum by(__name__)(bare_selector)` discriminates and preserves metric
/// names, while `without` and ungrouped aggregation drop the name as
/// before; `count_values("__name__", v)` writes its formatted value into
/// this channel too (never a `Labels` entry).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    name: Option<String>,
    labels: Labels,
}

impl GroupKey {
    /// Deterministic iteration/output order: non-name labels first, the
    /// name channel as tie-break — the same `(labels, metric_name)`
    /// convention as the range accumulator's sort (`eval/mod.rs`).
    fn output_cmp(&self, other: &Self) -> Ordering {
        (&self.labels, &self.name).cmp(&(&other.labels, &other.name))
    }
}

/// Computes the sample's [`GroupKey`] under `grouping`:
/// - `by`: keep only the named labels; `__name__` in the `by` set reads
///   the sample's `metric_name` into the name channel (virtual-name
///   injection — `by(__name__)` discriminates metric names);
/// - `without`: drop the named labels; the name channel is ALWAYS `None`
///   (upstream deletes `__name__` unconditionally in its without branch);
/// - no grouping: the single anonymous group.
///
/// **Issue #86 (M6-08d) resolves the #69-recorded divergence:** under the
/// delayed model, `metric_name` is the RETAINED name even for
/// drop-marked inputs, so `by(__name__)` over `rate(…)` now partitions on
/// the retained strings exactly like upstream — multiple such series
/// yield distinct drop-marked groups whose post-drop labelsets collide at
/// the terminal cleanup (`name_label_dropping.test:84`'s `expect fail`).
/// The group's own verdict is the member OR ([`Acc::drop_name`]).
fn group_key(s: &InstantSample, grouping: Option<&Grouping>) -> GroupKey {
    match grouping {
        None => GroupKey {
            name: None,
            labels: Labels::default(),
        },
        Some(g) if g.without => GroupKey {
            name: None,
            labels: s.labels.without(&g.labels),
        },
        Some(g) => GroupKey {
            name: if g.labels.iter().any(|l| l == "__name__") {
                s.metric_name.clone()
            } else {
                None
            },
            labels: s.labels.only(&g.labels),
        },
    }
}

/// Every `AggOp` aggregation. `param` is `topk`/`bottomk`/`limitk`'s `k`,
/// `quantile`'s φ, or `limit_ratio`'s `r` (already evaluated to a scalar
/// by the caller); `count_values` does not route through here (its
/// parameter is a string — see [`count_values`]). `annos` collects the
/// M7-A5b-iii native-histogram info/warning annotations (`engine.go`'s
/// `aggregation`/`aggregationK`) — a no-op sink for a float-only vector.
pub fn aggregate(
    op: AggOp,
    vector: &[InstantSample],
    grouping: Option<&Grouping>,
    param: Option<f64>,
    annos: &mut Annotations,
) -> Result<Vec<InstantSample>, PromqlError> {
    match op {
        AggOp::Topk | AggOp::Bottomk => aggregate_topk(op, vector, grouping, param, annos),
        AggOp::LimitK | AggOp::LimitRatio => aggregate_limit(op, vector, grouping, param),
        AggOp::Quantile => aggregate_quantile(vector, grouping, param, annos),
        _ => Ok(aggregate_reduce(op, vector, grouping, annos)),
    }
}

/// The aggregation-op name upstream's `NewHistogramIgnoredInAggregationInfo`
/// embeds (`engine.go`'s literal string arguments — `"min"`, `"max"`,
/// `"stddev"`, `"stdvar"`, `"quantile"`, `"topk"`, `"bottomk"`).
fn ignored_in_aggregation_name(op: AggOp) -> &'static str {
    match op {
        AggOp::Min => "min",
        AggOp::Max => "max",
        AggOp::Stddev => "stddev",
        AggOp::Stdvar => "stdvar",
        AggOp::Quantile => "quantile",
        AggOp::Topk => "topk",
        AggOp::Bottomk => "bottomk",
        _ => unreachable!("only called for the histogram-ignoring aggregation ops"),
    }
}

struct Acc {
    kahan: KahanSum,
    min: f64,
    max: f64,
    count: f64,
    /// Issue #86 (M6-08d): the group's delayed name-removal verdict — the
    /// OR of every member's `drop_name` (upstream `groupedAggregation.
    /// dropName`, "True if any sample in this group has DropName set",
    /// engine.go:3579; seeded at group creation :3608, OR-folded :3665,
    /// emitted :3945). Meaningful when `by(__name__)` retained the name
    /// in the group key: `sum by(__name__)(m or rate(n[5m]))` keeps the
    /// retained name mid-tree but drops it terminally iff any member was
    /// drop-marked (`name_label_dropping.test`'s OR-propagation cases).
    drop_name: bool,
    /// Welford running mean/M2 for `stddev`/`stdvar` (issue #69, M6-06 —
    /// upstream `aggregation()`'s own recurrence, run for EVERY sample
    /// including the first; that exact form is load-bearing for the
    /// vendored edge cases: single finite → `m2 = v·(v−v) = 0`, single
    /// `±Inf` → `m2 = Inf·(Inf−Inf) = NaN`, all-equal → exactly `0.0`).
    mean: f64,
    m2: f64,
    t_ms: i64,

    // --- M7-A5b-iii: native-histogram `sum`/`avg`/`min`/`max`/`stddev`/
    // `stdvar` state — mirrors `groupedAggregation`'s histogram fields
    // (`engine.go:3559-3583`). `seen` mirrors `group.seen`: `false` means
    // this group has not yet stabilized on a valid (non-histogram-only)
    // member for an op that skips histograms — the group produces NO
    // output (upstream's `if !aggr.seen { continue }`, `:3866`) unless a
    // later float sample re-stabilizes it.
    seen: bool,
    has_float: bool,
    has_histogram: bool,
    incompatible_histograms: bool,
    /// The running `sum`/`avg` histogram accumulator (upstream
    /// `histogramValue`). For `avg` this holds the running SUM until
    /// output-time division, exactly like the pin.
    hist_value: Option<FloatHistogram>,
    /// `avg`'s incremental-mean accumulator, populated only after a
    /// `HasOverflow` switch (upstream `histogramMean`).
    hist_mean: Option<FloatHistogram>,
    hist_incremental_mean: bool,
    /// The running FULL compensation histogram for `hist_value`/
    /// `hist_mean` (upstream `histogramKahanC` — `None` until the first
    /// `KahanAdd`, mirroring the pin's `nil`; every scalar AND bucket
    /// carries its own Neumaier remainder, `float_histogram_kahan.rs`).
    hist_kahan_c: Option<FloatHistogram>,
    /// Issue #154: `avg`'s FLOAT channel — the pin's `floatValue`/
    /// `floatMean`/`floatKahanC`/`floatIncrementalMean`
    /// (engine.go:3783-3800 at 40af9c2, finalized :3893-3897): direct
    /// mean (running Kahan sum seeded with the FIRST member's raw value,
    /// group init :3604) as long as the running sum stays finite; on
    /// overflow, switch to the incremental-mean recurrence for the rest
    /// of the group (`aggregators.test:651-657`'s ±`big` cases — the old
    /// shared `KahanSum` direct mean returned ±Inf there). `Sum` keeps
    /// the pre-existing `kahan` field (a zero-seeded sum is the pin's
    /// own SUM shape).
    avg_sum: f64,
    avg_mean: f64,
    avg_kahan_c: f64,
    avg_incremental_mean: bool,
    /// Issue #125: upstream's `counterResetSeen`/`notCounterResetSeen`
    /// (`groupedAggregation`, `engine.go:3577-3578`) — tracked over INPUT
    /// sample hints (group init `:3619-3621`, SUM fold `:3681-3683`, AVG
    /// fold `:3736-3738`; only when the group's first member was a
    /// histogram, mirroring the pin's `group.histogramValue != nil`
    /// guard); both-seen ⇒ `HistogramCounterResetCollisionWarning` at
    /// group finalization (`:3939-3941`).
    counter_reset_seen: bool,
    not_counter_reset_seen: bool,
}

impl Acc {
    fn fresh(t_ms: i64) -> Self {
        Acc {
            kahan: KahanSum::new(),
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            count: 0.0,
            drop_name: false,
            mean: 0.0,
            m2: 0.0,
            t_ms,
            seen: true,
            has_float: false,
            has_histogram: false,
            incompatible_histograms: false,
            hist_value: None,
            hist_mean: None,
            hist_incremental_mean: false,
            hist_kahan_c: None,
            avg_sum: 0.0,
            avg_mean: 0.0,
            avg_kahan_c: 0.0,
            avg_incremental_mean: false,
            counter_reset_seen: false,
            not_counter_reset_seen: false,
        }
    }
}

/// Issue #125: folds one INPUT sample's counter-reset hint into the
/// group's collision-tracking booleans — the pin's `switch
/// h.CounterResetHint` at the three tracked sites (`engine.go:3619-3621,
/// 3681-3683,3736-3738`).
fn track_counter_reset(acc: &mut Acc, h: &FloatHistogram) {
    match h.counter_reset_hint {
        pulsus_model::CounterResetHint::CounterReset => acc.counter_reset_seen = true,
        pulsus_model::CounterResetHint::NotCounterReset => acc.not_counter_reset_seen = true,
        _ => {}
    }
}

/// The pin's final compensation flush (`aggr.histogramValue.Add(aggr.
/// histogramKahanC)`, `engine.go:3877-3899` — the returned error is
/// asserted impossible upstream: "Add can theoretically return
/// ErrHistogramsIncompatibleSchema, but at this stage errors should not
/// occur if earlier KahanAdd calls succeeded"). Mirrored: the
/// compensation always shares the value's schema family, so `Err` is a
/// broken invariant — debug-asserted, falling back to the uncompensated
/// value rather than panicking.
fn flush_compensation(value: FloatHistogram, comp: &FloatHistogram) -> FloatHistogram {
    match value.add(comp) {
        Ok(outcome) => outcome.result,
        Err(_) => {
            debug_assert!(
                false,
                "the compensation histogram always shares the running sum's schema family"
            );
            value
        }
    }
}

/// Folds ONE histogram sample `h` into `acc` for `Sum`/`Avg` — the pin's
/// `AVG`/`SUM` step-case histogram arm (`engine.go:3673-3703,3719-3762`),
/// reached only when `acc.hist_value.is_some()` (i.e. this group's FIRST
/// member was itself a histogram — a float-first group's histogram
/// members are counted via `has_histogram` at the call site but never
/// folded here, matching `group.histogramValue != nil`). `annos`/`op`
/// only distinguish the two error/info sites' text (none differ by op
/// today — both use `HistogramOperation::Agg` — but kept explicit for the
/// reviewer).
fn fold_histogram_into_sum(
    acc: &mut Acc,
    sample_metric_name: &str,
    h: &FloatHistogram,
    annos: &mut Annotations,
) {
    let Some(current) = acc.hist_value.clone() else {
        return;
    };
    match current.kahan_add(h, acc.hist_kahan_c.as_ref()) {
        Ok(outcome) => {
            if outcome.nhcb_bounds_reconciled {
                annos.info(messages::mismatched_custom_buckets_histograms_info(
                    messages::HistogramOperation::Agg,
                ));
            }
            acc.hist_value = Some(outcome.result);
            acc.hist_kahan_c = Some(outcome.compensation);
        }
        Err(FloatHistogramOpError::IncompatibleSchema) => {
            annos.warning(messages::mixed_exponential_custom_histograms_warning(
                sample_metric_name,
            ));
            acc.incompatible_histograms = true;
        }
    }
}

/// `avg`'s histogram step (upstream's `AVG` case's histogram arm,
/// `engine.go:3719-3762`): direct-mean Kahan accumulation with a switch to
/// incremental-mean once the running sum overflows (`HasOverflow`), and
/// the incremental-mean update every step thereafter. `acc.count` is the
/// group's `groupCount` — the caller increments it BEFORE calling this
/// (upstream's `group.groupCount++` precedes the `h != nil` branch,
/// unconditionally, for every avg sample regardless of type).
fn fold_histogram_into_avg(
    acc: &mut Acc,
    sample_metric_name: &str,
    h: &FloatHistogram,
    annos: &mut Annotations,
) {
    let Some(current) = acc.hist_value.clone() else {
        return;
    };
    if !acc.hist_incremental_mean {
        let outcome = match current.kahan_add(h, acc.hist_kahan_c.as_ref()) {
            Ok(o) => o,
            Err(FloatHistogramOpError::IncompatibleSchema) => {
                annos.warning(messages::mixed_exponential_custom_histograms_warning(
                    sample_metric_name,
                ));
                acc.incompatible_histograms = true;
                return;
            }
        };
        if outcome.nhcb_bounds_reconciled {
            annos.info(messages::mismatched_custom_buckets_histograms_info(
                messages::HistogramOperation::Agg,
            ));
        }
        if !outcome.result.has_overflow() {
            acc.hist_value = Some(outcome.result);
            acc.hist_kahan_c = Some(outcome.compensation);
            return;
        }
        // Overflow: switch to incremental mean, seeded from the
        // PRE-overflow running sum/compensation (`group.histogramValue`/
        // `group.histogramKahanC` — never mutated by the failed attempt
        // above, mirroring the pin's `v := group.histogramValue.Copy()`
        // local-copy discipline). The compensation is scaled as a WHOLE
        // histogram (`group.histogramKahanC.Div(group.groupCount - 1)`,
        // `engine.go:3746-3748` — full `Div`, buckets included).
        acc.hist_incremental_mean = true;
        let mut mean = current;
        mean.div(acc.count - 1.0);
        acc.hist_mean = Some(mean);
        if let Some(c) = acc.hist_kahan_c.as_mut() {
            c.div(acc.count - 1.0);
        }
    }
    // Incremental-mean update (both freshly-switched and already-
    // incremental paths share this tail — upstream has no `break` between
    // them, `engine.go:3743-3762`). `kahanC.Mul(q)` is a full-histogram
    // scale, guarded on presence like the pin's `!= nil`.
    let q = (acc.count - 1.0) / acc.count;
    if let Some(c) = acc.hist_kahan_c.as_mut() {
        c.mul(q);
    }
    let mut to_add = h.clone();
    to_add.div(acc.count);
    let mut scaled_mean = acc
        .hist_mean
        .clone()
        .expect("hist_incremental_mean implies hist_mean is Some");
    scaled_mean.mul(q);
    match scaled_mean.kahan_add(&to_add, acc.hist_kahan_c.as_ref()) {
        Ok(outcome) => {
            if outcome.nhcb_bounds_reconciled {
                annos.info(messages::mismatched_custom_buckets_histograms_info(
                    messages::HistogramOperation::Agg,
                ));
            }
            acc.hist_mean = Some(outcome.result);
            acc.hist_kahan_c = Some(outcome.compensation);
        }
        Err(FloatHistogramOpError::IncompatibleSchema) => {
            annos.warning(messages::mixed_exponential_custom_histograms_warning(
                sample_metric_name,
            ));
            acc.incompatible_histograms = true;
        }
    }
}

fn aggregate_reduce(
    op: AggOp,
    vector: &[InstantSample],
    grouping: Option<&Grouping>,
    annos: &mut Annotations,
) -> Vec<InstantSample> {
    let mut groups: HashMap<GroupKey, Acc> = HashMap::new();
    for s in vector {
        let key = group_key(s, grouping);
        let is_new = !groups.contains_key(&key);
        let acc = groups.entry(key).or_insert_with(|| Acc::fresh(s.t_ms));
        // Upstream's `if !group.seen { *group = groupedAggregation{...};
        // <op-specific init>; continue }` (`engine.go:3598-3660`): a group
        // that has never stabilized (every member so far was a histogram,
        // for an op that skips them) re-runs the SAME "first sample" logic
        // on each new arrival.
        let is_first = is_new || !acc.seen;
        if is_first && !is_new {
            *acc = Acc::fresh(s.t_ms);
        }
        // Upstream's `if group.incompatibleHistograms { continue }`
        // (`engine.go:3663-3665`), checked right after the not-seen/reinit
        // branch: once a group has hit an incompatible-schema fold, every
        // further member is skipped outright (no further annotations, no
        // further accumulation attempts).
        if !is_first && acc.incompatible_histograms {
            continue;
        }
        acc.drop_name |= s.drop_name;

        match (op, &s.h) {
            (AggOp::Sum, Some(h)) => {
                acc.has_histogram = true;
                if is_first {
                    acc.hist_value = Some((**h).clone());
                    track_counter_reset(acc, h);
                } else {
                    // Hint tracking only for a histogram-first group — the
                    // pin guards on `group.histogramValue != nil`
                    // (`engine.go:3679-3683`); a float-first group's
                    // histogram members are never folded OR tracked.
                    if acc.hist_value.is_some() {
                        track_counter_reset(acc, h);
                    }
                    fold_histogram_into_sum(acc, s.metric_name.as_deref().unwrap_or(""), h, annos);
                }
            }
            (AggOp::Avg, Some(h)) => {
                acc.count += 1.0;
                acc.has_histogram = true;
                if is_first {
                    acc.hist_value = Some((**h).clone());
                    track_counter_reset(acc, h);
                } else {
                    if acc.hist_value.is_some() {
                        track_counter_reset(acc, h);
                    }
                    fold_histogram_into_avg(acc, s.metric_name.as_deref().unwrap_or(""), h, annos);
                }
            }
            (AggOp::Sum, None) => {
                acc.has_float = true;
                acc.kahan.add(s.v);
            }
            // Issue #154: the pin's AVG float step (engine.go:3783-3800
            // at 40af9c2). Group init seeds `floatValue` with the first
            // member's RAW value (:3604, no compensation); later members
            // fold via `kahansum.Inc` while the running sum stays finite;
            // the first overflow switches this group to the
            // incremental-mean recurrence (`q = (n-1)/n`), which then
            // absorbs the current member too (upstream falls through
            // after the switch — no skip).
            (AggOp::Avg, None) => {
                acc.has_float = true;
                acc.count += 1.0;
                if is_first {
                    acc.avg_sum = s.v;
                } else {
                    if !acc.avg_incremental_mean {
                        let (new_sum, new_c) =
                            crate::math::kahan_inc(s.v, acc.avg_sum, acc.avg_kahan_c);
                        if !new_sum.is_infinite() {
                            acc.avg_sum = new_sum;
                            acc.avg_kahan_c = new_c;
                        } else {
                            acc.avg_incremental_mean = true;
                            acc.avg_mean = acc.avg_sum / (acc.count - 1.0);
                            acc.avg_kahan_c /= acc.count - 1.0;
                        }
                    }
                    if acc.avg_incremental_mean {
                        let q = (acc.count - 1.0) / acc.count;
                        let (mean, c) = crate::math::kahan_inc(
                            s.v / acc.count,
                            q * acc.avg_mean,
                            q * acc.avg_kahan_c,
                        );
                        acc.avg_mean = mean;
                        acc.avg_kahan_c = c;
                    }
                }
            }
            (AggOp::Min | AggOp::Max, Some(_)) => {
                annos.info(messages::histogram_ignored_in_aggregation_info(
                    ignored_in_aggregation_name(op),
                ));
                if is_first {
                    acc.seen = false;
                }
                // Else: the group already stabilized on an earlier float
                // member — this histogram is simply skipped.
            }
            (AggOp::Min | AggOp::Max, None) => {
                acc.min = acc.min.min(s.v);
                acc.max = acc.max.max(s.v);
                acc.count += 1.0;
            }
            (AggOp::Stddev | AggOp::Stdvar, Some(_)) => {
                annos.info(messages::histogram_ignored_in_aggregation_info(
                    ignored_in_aggregation_name(op),
                ));
                if is_first {
                    acc.seen = false;
                }
            }
            (AggOp::Stddev | AggOp::Stdvar, None) => {
                acc.count += 1.0;
                // Welford: count is incremented BEFORE the mean update
                // divides by it (the recurrence's own definition).
                let d = s.v - acc.mean;
                acc.mean += d / acc.count;
                acc.m2 += d * (s.v - acc.mean);
            }
            (AggOp::Count, _) => {
                acc.count += 1.0;
            }
            (AggOp::Group, _) => {}
            _ => unreachable!("handled by aggregate_topk/aggregate_limit/aggregate_quantile"),
        }
    }

    let mut out: Vec<InstantSample> = groups
        .into_iter()
        .filter(|(_, acc)| acc.seen)
        .filter_map(|(key, acc)| {
            // M7-A5b-iii: mixed float+histogram poisons the WHOLE group
            // (`engine.go:3862-3865,3907-3910`) — checked before the
            // incompatible-schema drop, matching the pin's order.
            if (op == AggOp::Sum || op == AggOp::Avg) && acc.has_float && acc.has_histogram {
                annos.warning(messages::mixed_floats_histograms_agg_warning());
                return None;
            }
            if acc.incompatible_histograms {
                return None;
            }
            // Issue #125: the group-finalization collision warning
            // (`engine.go:3939-3941`) — AFTER the mixed/incompatible
            // drops, exactly like the pin (a dropped group `continue`s
            // before its check runs). Only sum/avg ever set the booleans.
            if acc.counter_reset_seen && acc.not_counter_reset_seen {
                annos.warning(messages::histogram_counter_reset_collision_warning(
                    messages::HistogramOperation::Agg,
                ));
            }
            let (v, h) = match op {
                // The pin's SUM output arm (`engine.go:3893-3901`): flush
                // the FULL compensation histogram via `Add` (skipped when
                // it never materialized — the `!= nil` guard), then
                // `Compact(0)`.
                AggOp::Sum if acc.has_histogram => {
                    let mut result = acc
                        .hist_value
                        .expect("has_histogram implies hist_value is Some for a stabilized group");
                    if let Some(c) = &acc.hist_kahan_c {
                        result = flush_compensation(result, c);
                    }
                    result.compact();
                    (0.0, Some(Box::new(result)))
                }
                AggOp::Sum => (acc.kahan.value(), None),
                // The pin's AVG output arm (`engine.go:3869-3891`):
                // incremental → `histogramMean.Add(kahanC)`; direct →
                // `histogramValue.Div(groupCount)` then
                // `.Add(kahanC.Div(groupCount))` (the compensation scaled
                // as a WHOLE histogram); then `Compact(0)`.
                AggOp::Avg if acc.has_histogram => {
                    let mut result = if acc.hist_incremental_mean {
                        let result = acc
                            .hist_mean
                            .expect("hist_incremental_mean implies hist_mean is Some");
                        match acc.hist_kahan_c {
                            Some(ref c) => flush_compensation(result, c),
                            None => result,
                        }
                    } else {
                        let mut result = acc.hist_value.expect(
                            "has_histogram implies hist_value is Some for a stabilized group",
                        );
                        result.div(acc.count);
                        match acc.hist_kahan_c {
                            Some(mut c) => {
                                c.div(acc.count);
                                flush_compensation(result, &c)
                            }
                            None => result,
                        }
                    };
                    result.compact();
                    (0.0, Some(Box::new(result)))
                }
                // Issue #154: the pin's AVG float readout (engine.go:
                // 3893-3897): incremental → `floatMean + floatKahanC`;
                // direct → sum and compensation divided SEPARATELY
                // (`floatValue/groupCount + floatKahanC/groupCount`,
                // never `(sum+c)/n` — the sum alone may sit at the edge
                // of the float range).
                AggOp::Avg if acc.avg_incremental_mean => (acc.avg_mean + acc.avg_kahan_c, None),
                AggOp::Avg => (acc.avg_sum / acc.count + acc.avg_kahan_c / acc.count, None),
                AggOp::Min => (acc.min, None),
                AggOp::Max => (acc.max, None),
                AggOp::Count => (acc.count, None),
                AggOp::Group => (1.0, None),
                // Population variance (upstream divides by count, not
                // count−1): a single sample yields exactly 0 (or NaN via
                // the Inf/NaN m2 edge, see `Acc::mean`'s doc).
                AggOp::Stddev => ((acc.m2 / acc.count).sqrt(), None),
                AggOp::Stdvar => (acc.m2 / acc.count, None),
                AggOp::Topk | AggOp::Bottomk => unreachable!("handled by aggregate_topk"),
                AggOp::LimitK | AggOp::LimitRatio => unreachable!("handled by aggregate_limit"),
                AggOp::Quantile => unreachable!("handled by aggregate_quantile"),
            };
            // Issue #37: every `aggregate_reduce` op **computes** a new
            // value (a sum/avg/min/max/count/group/stddev/stdvar over a
            // whole group) — Prometheus drops `__name__` here (captured:
            // `query.name_aggregation_drops_get.json`), EXCEPT under
            // `by(__name__)`, where the name is part of the group key and
            // is preserved on the output (issue #69 plan v2 Δ1;
            // `name_label_dropping.test:79,107`). `aggregate_topk`/
            // `aggregate_limit` (below) are the aggregation ops that do
            // *not* go through this fn — they clone the original, matched
            // `InstantSample` verbatim, so `metric_name` survives there
            // unmodified (they select existing series, never compute a
            // value — captured/verified: `topk(1, up)` keeps `__name__`).
            Some(InstantSample {
                labels: key.labels,
                metric_name: key.name,
                drop_name: acc.drop_name,
                t_ms: acc.t_ms,
                v,
                h,
            })
        })
        .collect();
    // Deterministic output order (HashMap iteration order is not stable) —
    // not a correctness requirement, but keeps callers/tests from having
    // to sort themselves.
    out.sort_by(|a, b| (&a.labels, &a.metric_name).cmp(&(&b.labels, &b.metric_name)));
    out
}

fn aggregate_topk(
    op: AggOp,
    vector: &[InstantSample],
    grouping: Option<&Grouping>,
    param: Option<f64>,
    annos: &mut Annotations,
) -> Result<Vec<InstantSample>, PromqlError> {
    let k = param.ok_or_else(|| PromqlError::BadMatching {
        detail: "topk/bottomk require a k parameter".to_string(),
    })?;
    // Issue #154: the pin's parameter gauntlet IN ORDER
    // (engine.go:1632-1645 at 40af9c2): `params.Max() < 1` returns empty
    // FIRST (Go `NaN < 1` is false, so NaN falls through), then NaN
    // errors ("Parameter value is NaN", pinned by `aggregators.test:422`),
    // then the int64 under/overflow errors. The `< 1` early return makes
    // the underflow arm unreachable here (single-extremum instant/step
    // shape) — kept for the message parity anyway.
    if k < 1.0 {
        return Ok(Vec::new());
    }
    if k.is_nan() {
        return Err(PromqlError::InvalidParameter {
            detail: "Parameter value is NaN".to_string(),
        });
    }
    if k >= i64::MAX as f64 {
        return Err(PromqlError::InvalidParameter {
            detail: format!(
                "Scalar value {} overflows int64",
                crate::annotations::go_float::format_g(k)
            ),
        });
    }
    let k = k as usize;

    // M7-A5b-iii: a histogram member is skipped + `HistogramIgnoredIn
    // AggregationInfo` (`aggregationK`'s `TOPK`/`BOTTOMK` cases,
    // `engine.go:4032-4083`) — it never enters the heap; a group whose
    // members are ALL histograms therefore never appears in `groups`
    // (equivalent to upstream's `!aggr.seen` output skip).
    let mut groups: HashMap<GroupKey, Vec<InstantSample>> = HashMap::new();
    for s in vector {
        if s.h.is_some() {
            annos.info(messages::histogram_ignored_in_aggregation_info(
                ignored_in_aggregation_name(op),
            ));
            continue;
        }
        let key = group_key(s, grouping);
        groups.entry(key).or_default().push(s.clone());
    }

    let mut group_keys: Vec<GroupKey> = groups.keys().cloned().collect();
    group_keys.sort_by(GroupKey::output_cmp);

    // Issue #154: NaN is the LEAST element for topk and the GREATEST for
    // bottomk — the pin's `vectorByValueHeap.Less`/
    // `vectorByReverseValueHeap.Less` both return `true` when `vi` is NaN
    // (functions.go:2690-2729 at 40af9c2), so a NaN member is displaced
    // from the heap by any non-NaN candidate and, when it survives (fewer
    // than k non-NaN members), sorts LAST in the reversed output — for
    // BOTH directions. The old `partial_cmp(..).unwrap_or(Equal)`
    // comparator was NON-TRANSITIVE in the presence of NaN (garbage order
    // AND garbage selection — `aggregators.test:287-378`).
    fn nan_last(a: f64, b: f64, descending: bool) -> Ordering {
        match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => {
                let ord = a.partial_cmp(&b).expect("both values are non-NaN");
                if descending { ord.reverse() } else { ord }
            }
        }
    }
    let mut out = Vec::new();
    for key in group_keys {
        let mut members = groups.remove(&key).expect("key came from groups.keys()");
        match op {
            AggOp::Topk => members.sort_by(|a, b| nan_last(a.v, b.v, true)),
            AggOp::Bottomk => members.sort_by(|a, b| nan_last(a.v, b.v, false)),
            _ => unreachable!("only called for Topk/Bottomk"),
        }
        out.extend(members.into_iter().take(k));
    }
    Ok(out)
}

/// `quantile(φ, v)` (issue #69, M6-06): per group, [`quantile_of`] over
/// the members' values collected in input order — upstream's shared
/// `quantile()` (NaN sorts smallest, `rank = φ·(n−1)` linear
/// interpolation, out-of-range φ clamps to `±Inf`/`NaN` rather than
/// erroring, the #67 convention). Computes a new value ⇒ output identity
/// is the group key (name kept only under `by(__name__)`).
fn aggregate_quantile(
    vector: &[InstantSample],
    grouping: Option<&Grouping>,
    param: Option<f64>,
    annos: &mut Annotations,
) -> Result<Vec<InstantSample>, PromqlError> {
    let phi = param.ok_or_else(|| PromqlError::BadMatching {
        detail: "quantile requires a quantile parameter".to_string(),
    })?;

    // Issue #154: the pin's QUANTILE parameter warning (engine.go:
    // 1664-1674 at 40af9c2, pre-step-loop over the param extrema): NaN,
    // > 1, and < 0 each add `NewInvalidQuantileWarning` — emitted here
    // per step and deduped on the message text (an instant/constant-φ
    // query is outcome-identical to the pin's once-per-query emission).
    // Pinned by `aggregators.test:544` (`quantile without(point)(NaN,
    // data)` + `expect warn`).
    // NaN is outside the range too (`contains` is false for NaN) — the
    // pin's three checks collapse into one.
    if !(0.0..=1.0).contains(&phi) {
        annos.warning(messages::invalid_quantile_warning(phi));
    }

    // M7-A5b-iii: a histogram member is skipped + `HistogramIgnoredIn
    // AggregationInfo` (`engine.go:3648-3652,3860-3863`) — never pushed to
    // the group's heap; a group whose members are ALL histograms never
    // appears in `groups` (equivalent to upstream's `!aggr.seen` skip).
    //
    // `(values, group drop_name OR — issue #86, the `Acc::drop_name`
    // rule, t_ms)` per group.
    let mut groups: HashMap<GroupKey, (Vec<f64>, bool, i64)> = HashMap::new();
    for s in vector {
        if s.h.is_some() {
            annos.info(messages::histogram_ignored_in_aggregation_info("quantile"));
            continue;
        }
        let key = group_key(s, grouping);
        let entry = groups
            .entry(key)
            .or_insert_with(|| (Vec::new(), false, s.t_ms));
        entry.0.push(s.v);
        entry.1 |= s.drop_name;
    }

    let mut group_keys: Vec<GroupKey> = groups.keys().cloned().collect();
    group_keys.sort_by(GroupKey::output_cmp);

    let mut out = Vec::with_capacity(group_keys.len());
    for key in group_keys {
        let (mut values, drop_name, t_ms) =
            groups.remove(&key).expect("key came from groups.keys()");
        let v = quantile_of(phi, &mut values);
        out.push(InstantSample {
            labels: key.labels,
            metric_name: key.name,
            drop_name,
            t_ms,
            v,
            h: None,
        });
    }
    Ok(out)
}

/// `limitk(k, v)` / `limit_ratio(r, v)` (issue #69, M6-06, experimental —
/// planner-gated): both select existing series **verbatim** (`__name__`
/// kept, like `topk`).
///
/// A NaN parameter is a query error with upstream's exact message
/// (`Parameter value is NaN` / `Ratio value is NaN` — the vendored
/// `aggregators.test:425-429` expect-fail cases, which error even over an
/// empty selection, so the check runs before anything else; issue #69
/// coordinator adjudication overriding plan v2 Δ4's empty-result
/// inheritance).
///
/// - `limitk` keeps the first `min(k, |group|)` members per group in
///   input (ascending-fingerprint) order. The remaining k-parameter guard
///   is inherited from the reviewed `topk` guard (plan v2 Δ4): ±Inf and
///   `k < 1` ⇒ empty, fractional `k` truncates (the vendored corpus pins
///   nothing for ±Inf — revisable by a later differential). WHICH k
///   series is PulsusDB-deterministic but not upstream-defined
///   (upstream's own `limit.test` only asserts count/subset/boundary
///   invariants).
/// - `limit_ratio` includes each series iff
///   [`ratio_includes`]`(r, `[`series_offset`]`(s))` — upstream
///   `AddRatioSample`'s exact predicate; `r` caps to `[-1, 1]` first. The
///   cap warn annotation is NOT emitted here (issue #130): upstream warns
///   once per query from the evaluation-wide param extrema
///   (`engine.go:1655-1660` at the pin), so emission lives in
///   `eval::flush_ratio_warnings` over the per-node accumulator the
///   `Aggregate` arm folds each step's raw param into. Membership is
///   per-series, so `by`/`without` grouping cannot change the selected
///   set — the input is filtered directly in input order.
fn aggregate_limit(
    op: AggOp,
    vector: &[InstantSample],
    grouping: Option<&Grouping>,
    param: Option<f64>,
) -> Result<Vec<InstantSample>, PromqlError> {
    let param = param.ok_or_else(|| PromqlError::BadMatching {
        detail: "limitk/limit_ratio require a parameter".to_string(),
    })?;
    if param.is_nan() {
        return Err(PromqlError::InvalidParameter {
            detail: match op {
                AggOp::LimitK => "Parameter value is NaN".to_string(),
                _ => "Ratio value is NaN".to_string(),
            },
        });
    }
    match op {
        AggOp::LimitK => {
            if !param.is_finite() || param < 1.0 {
                return Ok(Vec::new());
            }
            let k = param as usize;
            // Streaming first-k per group: preserves input order and only
            // clones the selected members.
            let mut taken: HashMap<GroupKey, usize> = HashMap::new();
            let mut out = Vec::new();
            for s in vector {
                let n = taken.entry(group_key(s, grouping)).or_insert(0);
                if *n < k {
                    *n += 1;
                    out.push(s.clone());
                }
            }
            Ok(out)
        }
        AggOp::LimitRatio => {
            let r = param.clamp(-1.0, 1.0);
            Ok(vector
                .iter()
                .filter(|s| ratio_includes(r, series_offset(s)))
                .cloned()
                .collect())
        }
        _ => unreachable!("only called for LimitK/LimitRatio"),
    }
}

/// `count_values(label, v)` (issue #69, M6-06): per sample, the group key
/// under `by`/`without` is augmented with `label = format(value)` —
/// overwriting an existing entry on either channel (the vendored
/// `aggregators.test:467-479` "Overwrite label with output" cases) — and
/// the output counts the members per augmented key. `label == "__name__"`
/// writes the **metric-name channel**, never a `Labels` entry (the
/// split-name invariant, docs/architecture.md §2.3 — the
/// `eval::labels::set_or_delete` precedent), overwriting even a
/// `by(__name__)`-injected name. Label-name validity was checked at plan
/// time. M7-A5b-iii: a histogram sample's value-label text is its
/// `String()` rendering (`engine.go:4188` — `aggregationCountValues`'s
/// `s.H.String()` arm), via [`histogram_display_string`].
pub fn count_values(
    vector: &[InstantSample],
    label: &str,
    grouping: Option<&Grouping>,
) -> Vec<InstantSample> {
    let mut groups: HashMap<GroupKey, (f64, i64)> = HashMap::new();
    for s in vector {
        let mut key = group_key(s, grouping);
        let formatted = match &s.h {
            Some(h) => histogram_display_string(h),
            None => format_count_values_value(s.v),
        };
        if label == "__name__" {
            key.name = Some(formatted);
        } else {
            key.labels.set(label.to_string(), formatted);
        }
        groups.entry(key).or_insert((0.0, s.t_ms)).0 += 1.0;
    }
    let mut out: Vec<InstantSample> = groups
        .into_iter()
        .map(|(key, (count, t_ms))| InstantSample {
            labels: key.labels,
            metric_name: key.name,
            // A counted output is a fresh sample, never drop-marked
            // (upstream `countValues` constructs a default `Sample`) —
            // its `__name__` channel, when written, was written
            // explicitly.
            drop_name: false,
            t_ms,
            v: count,
            h: None,
        })
        .collect();
    out.sort_by(|a, b| (&a.labels, &a.metric_name).cmp(&(&b.labels, &b.metric_name)));
    out
}

/// M7-A5b-iii. Upstream `FloatHistogram.String()` (`float_histogram.go:
/// 176-203`): `{count:<g>, sum:<g>, <bucket>, <bucket>, …}` in ascending
/// numeric order (negative buckets, the zero bucket, positive buckets),
/// zero-count buckets omitted, each bucket rendered `[`/`(`+`<g>,<g>`+
/// `]`/`)`+`:<g>` per its inclusivity (`generic.go`'s `Bucket.String`,
/// `:148-159`). Reuses [`FloatHistogram::all_buckets`] — that method's own
/// doc proves its sequence (negative-descending, zero, positive-ascending)
/// is exactly the one `String()`'s reversed-negative-iterator walk
/// produces, so no separate walk is needed here.
fn histogram_display_string(h: &FloatHistogram) -> String {
    let mut s = format!(
        "{{count:{}, sum:{}",
        go_float::format_g(h.count),
        go_float::format_g(h.sum)
    );
    for b in h.all_buckets() {
        if b.count == 0.0 {
            continue;
        }
        s.push_str(&format!(
            ", {}{},{}{}:{}",
            if b.lower_inclusive { '[' } else { '(' },
            go_float::format_g(b.lower),
            go_float::format_g(b.upper),
            if b.upper_inclusive { ']' } else { ')' },
            go_float::format_g(b.count),
        ));
    }
    s.push('}');
    s
}

/// Go `strconv.FormatFloat(f, 'f', -1, 64)` — the value-label text
/// `count_values` stamps. For finite values Rust's `Display` is the same
/// shortest-round-trip positional decimal (never scientific notation,
/// `-0.0` prints `-0`) — pinned by goldens below; the non-finite
/// spellings differ (`+Inf`/`-Inf` vs Rust's `inf`) and are special-cased
/// (`NaN` is written explicitly too, for the golden's sake, though Rust
/// agrees there).
fn format_count_values_value(v: f64) -> String {
    if v == f64::INFINITY {
        "+Inf".to_string()
    } else if v == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else if v.is_nan() {
        "NaN".to_string()
    } else {
        format!("{v}")
    }
}

/// The series' deterministic `limit_ratio` inclusion offset — upstream
/// `AddRatioSample`'s `float64(labels.Hash()) / float64(math.MaxUint64)`,
/// reproduced with the same canonical buffer byte layout (sorted
/// `key 0xFF value 0xFF` runs over the FULL identity, `__name__` spliced
/// at its lexical key position — [`full_labels`]) and hash primitive
/// (xxh64, seed 0 — the `pulsus_model::metric_fingerprint` layout).
/// **Not claimed bit-identical to upstream `labels.Hash()`** (that layout
/// is unverifiable from this repo and fragile across upstream build tags;
/// recorded as a documented divergence in the coverage manifest's
/// `rationale`, issue #69 adjudication 3) — but stable across steps and
/// processes, which is what the partition/stability/complement invariants
/// require of ANY fixed offset.
fn series_offset(s: &InstantSample) -> f64 {
    let mut buf = Vec::new();
    for (k, v) in full_labels(s) {
        buf.extend_from_slice(k.as_bytes());
        buf.push(0xFF);
        buf.extend_from_slice(v.as_bytes());
        buf.push(0xFF);
    }
    offset_from_hash(xxh64(&buf, 0))
}

/// `u64::MAX as f64` rounds to 2^64, and top-band hashes also round to
/// 2^64 — so the offset CAN equal exactly `1.0`, faithfully reproducing
/// upstream's non-guarantee at `r = 1.0` (a series whose offset rounds to
/// 1.0 is excluded even by `limit_ratio(1.0, …)`; only `r = -1.0`
/// guarantees all series — issue #69 plan v2 Δ2).
fn offset_from_hash(h: u64) -> f64 {
    h as f64 / (u64::MAX as f64)
}

/// Upstream `AddRatioSample`'s inclusion predicate, verbatim:
/// `(r >= 0 && offset < r) || (r < 0 && offset >= 1.0 + r)`. A NaN `r`
/// would fail both branches (every comparison with NaN is false) — kept
/// total, though [`aggregate_limit`] errors on a NaN parameter before
/// this predicate is ever reached.
fn ratio_includes(r: f64, offset: f64) -> bool {
    if r >= 0.0 {
        offset < r
    } else {
        offset >= 1.0 + r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(labels: &[(&str, &str)], v: f64) -> InstantSample {
        InstantSample {
            labels: Labels::new(labels.iter().map(|(k, v)| (k.to_string(), v.to_string()))),
            metric_name: Some("test_metric".to_string()),
            drop_name: false,
            t_ms: 0,
            v,
            h: None,
        }
    }

    fn hist_sample(labels: &[(&str, &str)], h: FloatHistogram) -> InstantSample {
        InstantSample {
            labels: Labels::new(labels.iter().map(|(k, v)| (k.to_string(), v.to_string()))),
            metric_name: Some("metric".to_string()),
            drop_name: false,
            t_ms: 0,
            v: 0.0,
            h: Some(Box::new(h)),
        }
    }

    fn exp_hist(count: f64, sum: f64, buckets: Vec<f64>) -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![pulsus_model::Span {
                offset: 0,
                length: buckets.len() as u32,
            }],
            negative_spans: vec![],
            positive_buckets: buckets,
            negative_buckets: vec![],
            custom_values: vec![],
        }
    }

    fn nhcb_hist(
        count: f64,
        sum: f64,
        custom_values: Vec<f64>,
        buckets: Vec<f64>,
    ) -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: pulsus_model::CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![pulsus_model::Span {
                offset: 0,
                length: buckets.len() as u32,
            }],
            negative_spans: vec![],
            positive_buckets: buckets,
            negative_buckets: vec![],
            custom_values,
        }
    }

    fn grouping(without: bool, labels: &[&str]) -> Grouping {
        Grouping {
            without,
            labels: labels.iter().map(|s| s.to_string()).collect(),
        }
    }

    // --- M7-A5b-iii: native-histogram sum/avg, corpus-cited
    // (`native_histograms.test`, "Test mixing exponential and custom
    // buckets" / "Test mismatched custom bucket boundaries") ---

    /// `native_histograms.test:1201-1213` T=0 column (exponential-only):
    /// `sum(metric)` over `exponential{sum:4,count:3,buckets:[1,2,1]}` +
    /// `other-exponential{sum:3,count:2,buckets:[1,1,1]}` ->
    /// `{{sum:7 count:5 buckets:[2 3 2]}}`, no warning.
    #[test]
    fn sum_over_two_exponential_histograms_matches_the_pinned_corpus_value() {
        let vector = vec![
            hist_sample(
                &[("series", "exponential")],
                exp_hist(3.0, 4.0, vec![1.0, 2.0, 1.0]),
            ),
            hist_sample(
                &[("series", "other-exponential")],
                exp_hist(2.0, 3.0, vec![1.0, 1.0, 1.0]),
            ),
        ];
        let mut annos = Annotations::new();
        let out = aggregate(AggOp::Sum, &vector, None, None, &mut annos).unwrap();
        assert_eq!(out.len(), 1);
        let h = out[0].h.as_ref().unwrap();
        assert!(h.bits_eq(&exp_hist(5.0, 7.0, vec![2.0, 3.0, 2.0])));
        assert!(annos.as_strings(0, 0).0.is_empty(), "no warning expected");
    }

    /// `native_histograms.test:1214-1216` T=12 column: the same group ALSO
    /// receiving `custom`/`other-custom` NHCB samples (mismatched SCHEMA,
    /// not just bounds) — `expect warn`, result `_` (the whole group is
    /// dropped, `MixedExponentialCustomHistogramsWarning`, not the
    /// binop-only `IncompatibleBucketLayoutInBinOpWarning`).
    #[test]
    fn sum_over_mixed_exponential_and_custom_bucket_histograms_warns_and_drops() {
        let vector = vec![
            hist_sample(
                &[("series", "exponential")],
                exp_hist(3.0, 4.0, vec![1.0, 2.0, 1.0]),
            ),
            hist_sample(
                &[("series", "other-exponential")],
                exp_hist(2.0, 3.0, vec![1.0, 1.0, 1.0]),
            ),
            hist_sample(
                &[("series", "custom")],
                nhcb_hist(1.0, 1.0, vec![5.0, 10.0], vec![1.0]),
            ),
            hist_sample(
                &[("series", "other-custom")],
                nhcb_hist(2.0, 15.0, vec![5.0, 10.0], vec![0.0, 2.0]),
            ),
        ];
        let mut annos = Annotations::new();
        let out = aggregate(AggOp::Sum, &vector, None, None, &mut annos).unwrap();
        assert!(out.is_empty(), "the mixed-schema group is dropped: {out:?}");
        let (warnings, _) = annos.as_strings(0, 0);
        assert_eq!(
            warnings,
            vec![messages::mixed_exponential_custom_histograms_warning(
                "metric"
            )]
        );
    }

    /// `native_histograms.test:1222-1228` T=0 column ("Test mismatched
    /// custom bucket boundaries"): `series="2"` (`custom_values:[10]
    /// buckets:[1]`) + `series="3"` (`custom_values:[2,10] buckets:[1]`,
    /// i.e. bucket 0 = `(-Inf,2]`) reconcile to the intersected bound
    /// `[10]` -> both land in target index 0 ->
    /// `{{schema:-53 count:2 sum:2 custom_values:[10] buckets:[2]}}`, no
    /// warning (an info instead — reconciliation, not a hard mismatch).
    #[test]
    fn sum_over_nhcb_histograms_with_mismatched_bounds_reconciles_matching_the_pinned_corpus_value()
    {
        let vector = vec![
            hist_sample(
                &[("series", "2")],
                nhcb_hist(1.0, 1.0, vec![10.0], vec![1.0]),
            ),
            hist_sample(
                &[("series", "3")],
                nhcb_hist(1.0, 1.0, vec![2.0, 10.0], vec![1.0]),
            ),
        ];
        let mut annos = Annotations::new();
        let out = aggregate(AggOp::Sum, &vector, None, None, &mut annos).unwrap();
        assert_eq!(out.len(), 1);
        let h = out[0].h.as_ref().unwrap();
        assert!(h.bits_eq(&nhcb_hist(2.0, 2.0, vec![10.0], vec![2.0])));
        assert!(annos.as_strings(0, 0).0.is_empty(), "no warning expected");
        assert_eq!(
            annos.as_strings(0, 0).1,
            vec![messages::mismatched_custom_buckets_histograms_info(
                messages::HistogramOperation::Agg
            )]
        );
    }

    /// `count`/`group` are type-agnostic (`native_histograms.test:1234-
    /// 1237`'s `count(metric)`/`group(metric)` rows) — a histogram sample
    /// counts exactly like a float one.
    #[test]
    fn count_and_group_over_histogram_samples_are_unaffected() {
        let vector = vec![
            hist_sample(&[("series", "1")], exp_hist(1.0, 1.0, vec![1.0])),
            hist_sample(&[("series", "2")], exp_hist(1.0, 1.0, vec![1.0])),
        ];
        let mut annos = Annotations::new();
        assert_eq!(
            aggregate(AggOp::Count, &vector, None, None, &mut annos).unwrap()[0].v,
            2.0
        );
        assert_eq!(
            aggregate(AggOp::Group, &vector, None, None, &mut annos).unwrap()[0].v,
            1.0
        );
    }

    /// `min`/`max`/`stddev`/`stdvar` skip a histogram sample and info once
    /// (`engine.go`'s `HistogramIgnoredInAggregationInfo`); a PURE-
    /// histogram group produces NO output series at all (never stabilizes
    /// — `!aggr.seen`).
    #[test]
    fn min_over_a_pure_histogram_group_produces_no_output_and_infos() {
        let vector = vec![hist_sample(&[("s", "1")], exp_hist(1.0, 1.0, vec![1.0]))];
        let mut annos = Annotations::new();
        let out = aggregate(AggOp::Min, &vector, None, None, &mut annos).unwrap();
        assert!(out.is_empty());
        assert_eq!(
            annos.as_strings(0, 0).1,
            vec![messages::histogram_ignored_in_aggregation_info("min")]
        );
    }

    /// A group with an EARLIER float baseline stays valid; a LATER
    /// histogram member is skipped (info) but does not poison the group.
    #[test]
    fn max_over_a_float_then_histogram_group_keeps_the_float_result() {
        let vector = vec![
            sample(&[("s", "1")], 5.0),
            hist_sample(&[("s", "1")], exp_hist(1.0, 1.0, vec![1.0])),
        ];
        let mut annos = Annotations::new();
        let out = aggregate(AggOp::Max, &vector, None, None, &mut annos).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 5.0);
        assert!(out[0].h.is_none());
        assert_eq!(
            annos.as_strings(0, 0).1,
            vec![messages::histogram_ignored_in_aggregation_info("max")]
        );
    }

    /// `topk`/`bottomk` skip a histogram member + info (`aggregationK`'s
    /// `TOPK`/`BOTTOMK` cases) rather than sorting on the fabricated
    /// `v: 0.0`.
    #[test]
    fn topk_skips_a_histogram_member_and_infos() {
        let vector = vec![
            sample(&[("s", "1")], 5.0),
            hist_sample(&[("s", "2")], exp_hist(1.0, 1.0, vec![1.0])),
        ];
        let mut annos = Annotations::new();
        let out = aggregate(AggOp::Topk, &vector, None, Some(2.0), &mut annos).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 5.0);
        assert_eq!(
            annos.as_strings(0, 0).1,
            vec![messages::histogram_ignored_in_aggregation_info("topk")]
        );
    }

    /// `count_values` over a histogram sample stamps its `String()`
    /// rendering as the value label (`engine.go:4188`).
    #[test]
    fn count_values_over_a_histogram_sample_stamps_its_display_string() {
        let vector = vec![hist_sample(
            &[("s", "1")],
            exp_hist(4.0, 5.0, vec![1.0, 2.0, 1.0]),
        )];
        let out = count_values(&vector, "v", None);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].labels.get("v"),
            Some("{count:4, sum:5, (0.5,1]:1, (1,2]:2, (2,4]:1}")
        );
    }

    /// ADVERSARIAL (codex round-1 [high], end-to-end through `sum()`):
    /// the fold's BUCKET-level Kahan compensation recovers `+1.0`
    /// contributions plain accumulation loses above 2^53 — the output
    /// bucket (and count/sum/zero_count) is `2^53 + 2`, provably not the
    /// plain-add plateau `2^53`.
    #[test]
    fn sum_aggregation_bucket_compensation_recovers_lost_low_order_adds() {
        const BIG: f64 = 9007199254740992.0; // 2^53
        const BIG_PLUS_2: f64 = 9007199254740994.0;
        let mk = |label: &str, bucket: f64| {
            let mut h = exp_hist(bucket, bucket, vec![bucket]);
            h.zero_threshold = 0.001;
            h.zero_count = bucket;
            hist_sample(&[("s", label)], h)
        };
        let vector = vec![mk("1", BIG), mk("2", 1.0), mk("3", 1.0)];
        let mut annos = Annotations::new();
        let out = aggregate(AggOp::Sum, &vector, None, None, &mut annos).unwrap();
        assert_eq!(out.len(), 1);
        let h = out[0].h.as_ref().unwrap();
        assert_eq!(h.positive_buckets, vec![BIG_PLUS_2]);
        assert_ne!(
            h.positive_buckets,
            vec![BIG],
            "plain bucket accumulation would plateau at 2^53"
        );
        assert_eq!(h.zero_count, BIG_PLUS_2);
        assert_eq!(h.count, BIG_PLUS_2);
        assert_eq!(h.sum, BIG_PLUS_2);
        assert!(annos.is_empty());
    }

    /// ADVERSARIAL through `avg()` (direct mean): the flushed compensated
    /// mean is `(2^53 + 2) / 3` computed as `(2^53)/3 + 2/3` via the
    /// pin's `Div(count)` + `Add(kahanC.Div(count))` arithmetic — a plain
    /// fold would yield exactly `2^53 / 3`.
    #[test]
    fn avg_aggregation_bucket_compensation_survives_the_mean_division() {
        const BIG: f64 = 9007199254740992.0;
        let vector = vec![
            hist_sample(&[("s", "1")], exp_hist(BIG, BIG, vec![BIG])),
            hist_sample(&[("s", "2")], exp_hist(1.0, 1.0, vec![1.0])),
            hist_sample(&[("s", "3")], exp_hist(1.0, 1.0, vec![1.0])),
        ];
        let mut annos = Annotations::new();
        let out = aggregate(AggOp::Avg, &vector, None, None, &mut annos).unwrap();
        assert_eq!(out.len(), 1);
        let h = out[0].h.as_ref().unwrap();
        // The pin's exact arithmetic: value/3 + comp/3 (both compensations
        // are exactly 2.0 here since the two +1.0s were lost whole).
        let expected = BIG / 3.0 + 2.0 / 3.0;
        assert_eq!(h.positive_buckets, vec![expected]);
        assert_ne!(
            h.positive_buckets,
            vec![BIG / 3.0],
            "compensation must do work"
        );
        assert_eq!(h.count, expected);
    }

    #[test]
    fn sum_with_no_grouping_reduces_to_one_series() {
        let vector = vec![sample(&[("job", "a")], 1.0), sample(&[("job", "b")], 2.0)];
        let out = aggregate(AggOp::Sum, &vector, None, None, &mut Annotations::new()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 3.0);
        assert!(out[0].labels.is_empty());
    }

    #[test]
    fn sum_by_groups_and_sums_per_group() {
        let vector = vec![
            sample(&[("job", "a"), ("inst", "1")], 1.0),
            sample(&[("job", "a"), ("inst", "2")], 2.0),
            sample(&[("job", "b"), ("inst", "1")], 5.0),
        ];
        let g = grouping(false, &["job"]);
        let out = aggregate(AggOp::Sum, &vector, Some(&g), None, &mut Annotations::new()).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].labels.get("job"), Some("a"));
        assert_eq!(out[0].v, 3.0);
        assert_eq!(out[1].labels.get("job"), Some("b"));
        assert_eq!(out[1].v, 5.0);
    }

    #[test]
    fn sum_uses_kahan_summation_on_an_ordering_a_naive_sum_gets_wrong() {
        let vector = vec![
            sample(&[("s", "1")], 1e100),
            sample(&[("s", "2")], 1.0),
            sample(&[("s", "3")], -1e100),
        ];
        let out = aggregate(AggOp::Sum, &vector, None, None, &mut Annotations::new()).unwrap();
        assert_eq!(out[0].v, 1.0);
    }

    #[test]
    fn without_excludes_the_named_labels_from_the_group_key() {
        let vector = vec![
            sample(&[("job", "a"), ("inst", "1")], 1.0),
            sample(&[("job", "a"), ("inst", "2")], 2.0),
        ];
        let g = grouping(true, &["inst"]);
        let out = aggregate(AggOp::Sum, &vector, Some(&g), None, &mut Annotations::new()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 3.0);
    }

    #[test]
    fn avg_divides_by_group_member_count() {
        let vector = vec![sample(&[("job", "a")], 2.0), sample(&[("job", "a")], 4.0)];
        let out = aggregate(AggOp::Avg, &vector, None, None, &mut Annotations::new()).unwrap();
        assert_eq!(out[0].v, 3.0);
    }

    #[test]
    fn min_and_max() {
        let vector = vec![
            sample(&[("job", "a")], 5.0),
            sample(&[("job", "a")], 1.0),
            sample(&[("job", "a")], 3.0),
        ];
        assert_eq!(
            aggregate(AggOp::Min, &vector, None, None, &mut Annotations::new()).unwrap()[0].v,
            1.0
        );
        assert_eq!(
            aggregate(AggOp::Max, &vector, None, None, &mut Annotations::new()).unwrap()[0].v,
            5.0
        );
    }

    #[test]
    fn count_counts_group_members() {
        let vector = vec![
            sample(&[("job", "a")], 1.0),
            sample(&[("job", "a")], 1.0),
            sample(&[("job", "b")], 1.0),
        ];
        let g = grouping(false, &["job"]);
        let out = aggregate(
            AggOp::Count,
            &vector,
            Some(&g),
            None,
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out[0].v, 2.0);
        assert_eq!(out[1].v, 1.0);
    }

    #[test]
    fn group_always_yields_one() {
        let vector = vec![sample(&[("job", "a")], 42.0)];
        let out = aggregate(AggOp::Group, &vector, None, None, &mut Annotations::new()).unwrap();
        assert_eq!(out[0].v, 1.0);
    }

    #[test]
    fn topk_keeps_the_largest_k_values_per_group() {
        let vector = vec![
            sample(&[("s", "1")], 5.0),
            sample(&[("s", "2")], 1.0),
            sample(&[("s", "3")], 3.0),
        ];
        let out = aggregate(
            AggOp::Topk,
            &vector,
            None,
            Some(2.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].v, 5.0);
        assert_eq!(out[1].v, 3.0);
    }

    #[test]
    fn bottomk_keeps_the_smallest_k_values_per_group() {
        let vector = vec![
            sample(&[("s", "1")], 5.0),
            sample(&[("s", "2")], 1.0),
            sample(&[("s", "3")], 3.0),
        ];
        let out = aggregate(
            AggOp::Bottomk,
            &vector,
            None,
            Some(2.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].v, 1.0);
        assert_eq!(out[1].v, 3.0);
    }

    #[test]
    fn topk_retains_full_original_labels_not_the_grouping_key() {
        let vector = vec![sample(&[("job", "a"), ("inst", "1")], 5.0)];
        let out = aggregate(
            AggOp::Topk,
            &vector,
            None,
            Some(1.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out[0].labels.get("inst"), Some("1"));
    }

    // --- issue #37: `__name__` keep/drop rule ---

    #[test]
    fn sum_drops_metric_name() {
        let vector = vec![sample(&[("job", "a")], 1.0), sample(&[("job", "b")], 2.0)];
        let out = aggregate(AggOp::Sum, &vector, None, None, &mut Annotations::new()).unwrap();
        assert_eq!(out[0].metric_name, None);
    }

    #[test]
    fn avg_min_max_count_group_stddev_stdvar_all_drop_metric_name() {
        let vector = vec![sample(&[("job", "a")], 1.0)];
        for op in [
            AggOp::Avg,
            AggOp::Min,
            AggOp::Max,
            AggOp::Count,
            AggOp::Group,
            AggOp::Stddev,
            AggOp::Stdvar,
        ] {
            let out = aggregate(op, &vector, None, None, &mut Annotations::new()).unwrap();
            assert_eq!(out[0].metric_name, None, "{op:?} must drop __name__");
        }
    }

    /// `topk`/`bottomk` select existing series verbatim — they never
    /// compute a new value, so `__name__` is kept (captured/verified:
    /// `topk(1, up)`, see PROVENANCE.md).
    #[test]
    fn topk_and_bottomk_keep_metric_name() {
        let vector = vec![sample(&[("s", "1")], 5.0), sample(&[("s", "2")], 1.0)];
        let topk = aggregate(
            AggOp::Topk,
            &vector,
            None,
            Some(1.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(topk[0].metric_name.as_deref(), Some("test_metric"));
        let bottomk = aggregate(
            AggOp::Bottomk,
            &vector,
            None,
            Some(1.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(bottomk[0].metric_name.as_deref(), Some("test_metric"));
    }

    #[test]
    fn topk_without_a_k_parameter_is_bad_matching() {
        let vector = vec![sample(&[("s", "1")], 1.0)];
        let err = aggregate(AggOp::Topk, &vector, None, None, &mut Annotations::new()).unwrap_err();
        assert!(matches!(err, PromqlError::BadMatching { .. }));
    }

    #[test]
    fn topk_by_groups_independently() {
        let vector = vec![
            sample(&[("job", "a")], 1.0),
            sample(&[("job", "a")], 2.0),
            sample(&[("job", "b")], 9.0),
        ];
        let g = grouping(false, &["job"]);
        let out = aggregate(
            AggOp::Topk,
            &vector,
            Some(&g),
            Some(1.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        let vals: Vec<f64> = out.iter().map(|s| s.v).collect();
        assert!(vals.contains(&2.0));
        assert!(vals.contains(&9.0));
    }

    #[test]
    fn an_empty_vector_aggregates_to_an_empty_result() {
        assert!(
            aggregate(AggOp::Sum, &[], None, None, &mut Annotations::new())
                .unwrap()
                .is_empty()
        );
    }

    // --- issue #69 (M6-06): stddev/stdvar (Welford, population) ---

    fn named_sample(name: Option<&str>, labels: &[(&str, &str)], v: f64) -> InstantSample {
        InstantSample {
            labels: Labels::new(labels.iter().map(|(k, v)| (k.to_string(), v.to_string()))),
            metric_name: name.map(str::to_string),
            drop_name: false,
            t_ms: 0,
            v,
            h: None,
        }
    }

    /// `aggregators.test:799-830` (histogram row excluded, #22): the
    /// `{1, 2}` group — stdvar 0.25, stddev 0.5, EXACT.
    #[test]
    fn stddev_and_stdvar_of_one_and_two_are_exactly_half_and_quarter() {
        let vector = vec![
            sample(&[("label", "a")], 1.0),
            sample(&[("label", "b")], 2.0),
        ];
        assert_eq!(
            aggregate(AggOp::Stdvar, &vector, None, None, &mut Annotations::new()).unwrap()[0].v,
            0.25
        );
        assert_eq!(
            aggregate(AggOp::Stddev, &vector, None, None, &mut Annotations::new()).unwrap()[0].v,
            0.5
        );
    }

    /// A single finite sample has population variance exactly 0
    /// (`aggregators.test:820-830`'s per-label rows).
    #[test]
    fn stddev_and_stdvar_of_a_single_finite_sample_are_exactly_zero() {
        let vector = vec![sample(&[("label", "a")], 42.5)];
        assert_eq!(
            aggregate(AggOp::Stdvar, &vector, None, None, &mut Annotations::new()).unwrap()[0].v,
            0.0
        );
        assert_eq!(
            aggregate(AggOp::Stddev, &vector, None, None, &mut Annotations::new()).unwrap()[0].v,
            0.0
        );
    }

    /// `aggregators.test:960-967`: a single `Inf` sample is `NaN` (the
    /// Welford recurrence's `m2 += Inf · (Inf − Inf)` edge), NOT 0 — this
    /// is exactly why the recurrence must run for the first sample too.
    #[test]
    fn stddev_and_stdvar_of_a_single_inf_sample_are_nan() {
        for v in [f64::INFINITY, f64::NEG_INFINITY] {
            let vector = vec![sample(&[("label", "a")], v)];
            assert!(
                aggregate(AggOp::Stdvar, &vector, None, None, &mut Annotations::new()).unwrap()[0]
                    .v
                    .is_nan()
            );
            assert!(
                aggregate(AggOp::Stddev, &vector, None, None, &mut Annotations::new()).unwrap()[0]
                    .v
                    .is_nan()
            );
        }
    }

    /// `aggregators.test:903-910`: a single `NaN` sample is `NaN`.
    #[test]
    fn stddev_and_stdvar_of_a_single_nan_sample_are_nan() {
        let vector = vec![sample(&[("label", "a")], f64::NAN)];
        assert!(
            aggregate(AggOp::Stdvar, &vector, None, None, &mut Annotations::new()).unwrap()[0]
                .v
                .is_nan()
        );
        assert!(
            aggregate(AggOp::Stddev, &vector, None, None, &mut Annotations::new()).unwrap()[0]
                .v
                .is_nan()
        );
    }

    /// All-equal values yield EXACTLY `0.0` (every `d` after the first is
    /// exactly 0 — no accumulated rounding), matching upstream's own
    /// Welford choice.
    #[test]
    fn stddev_and_stdvar_of_all_equal_values_are_exactly_zero() {
        let vector: Vec<InstantSample> = (0..5)
            .map(|i| sample(&[("s", &i.to_string())], 0.1 + 0.2))
            .collect();
        assert_eq!(
            aggregate(AggOp::Stdvar, &vector, None, None, &mut Annotations::new()).unwrap()[0].v,
            0.0
        );
        assert_eq!(
            aggregate(AggOp::Stddev, &vector, None, None, &mut Annotations::new()).unwrap()[0].v,
            0.0
        );
    }

    /// `aggregators.test:857-900`: a NaN member poisons only ITS group —
    /// ungrouped → `{} NaN`; `by (label)` → 0, 0, NaN.
    #[test]
    fn stddev_by_isolates_a_nan_member_to_its_own_group() {
        let vector = vec![
            sample(&[("label", "a")], 1.0),
            sample(&[("label", "b")], 2.0),
            sample(&[("label", "c")], f64::NAN),
        ];
        assert!(
            aggregate(AggOp::Stddev, &vector, None, None, &mut Annotations::new()).unwrap()[0]
                .v
                .is_nan()
        );
        let g = grouping(false, &["label"]);
        let out = aggregate(
            AggOp::Stddev,
            &vector,
            Some(&g),
            None,
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].v, 0.0);
        assert_eq!(out[1].v, 0.0);
        assert!(out[2].v.is_nan());
    }

    // --- issue #69 (M6-06): metric-name channel in the group key ---

    /// `by (__name__)` discriminates metric names via virtual-name
    /// injection (plan v2 Δ1; `name_label_dropping.test:79,107`): two
    /// metrics with identical non-name labels form TWO groups, each
    /// output preserving its name.
    #[test]
    fn sum_by_dunder_name_discriminates_and_preserves_metric_names() {
        let vector = vec![
            named_sample(Some("metric_a"), &[("env", "1")], 10.0),
            named_sample(Some("metric_b"), &[("env", "1")], 32.0),
        ];
        let g = grouping(false, &["__name__"]);
        let out = aggregate(AggOp::Sum, &vector, Some(&g), None, &mut Annotations::new()).unwrap();
        assert_eq!(out.len(), 2, "two names, two groups: {out:?}");
        assert_eq!(out[0].metric_name.as_deref(), Some("metric_a"));
        assert_eq!(out[0].v, 10.0);
        assert_eq!(out[1].metric_name.as_deref(), Some("metric_b"));
        assert_eq!(out[1].v, 32.0);
        assert!(out.iter().all(|s| s.labels.is_empty()));
    }

    /// These inputs are genuinely nameless (`metric_name: None,
    /// drop_name: false`), not name-DROPPED (e.g. `rate(…)` output, which
    /// retains `metric_name` with `drop_name: true` under the delayed-name-
    /// removal model). Two genuinely-nameless series with distinct
    /// non-name labels group under one absent-`__name__` key and merge
    /// into a single anonymous group here, matching upstream: with no
    /// retained name there is no `DropName` collision at cleanup
    /// (`name_label_dropping.test:107-112`). The name-DROPPED case (e.g.
    /// `sum by (__name__) (rate(...))` over distinct metrics) is a
    /// different input shape and is pinned separately as the upstream
    /// duplicate-labelset error — see
    /// `sum_by_dunder_name_over_rate_of_distinct_names_is_the_duplicate_labelset_error`
    /// in `eval/mod.rs`.
    #[test]
    fn sum_by_dunder_name_over_genuinely_nameless_inputs_merges_like_upstream() {
        let vector = vec![
            named_sample(None, &[("env", "1")], 0.2),
            named_sample(None, &[("env", "2")], 0.2),
        ];
        let g = grouping(false, &["__name__"]);
        let out = aggregate(AggOp::Sum, &vector, Some(&g), None, &mut Annotations::new()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, None);
        assert_eq!(out[0].v, 0.4);
    }

    /// Regression pin: `without (…)` ALWAYS drops the name (upstream
    /// deletes `__name__` unconditionally in its without branch), even
    /// `without (job)` with distinct metric names — one merged group.
    #[test]
    fn sum_without_always_drops_the_metric_name_channel() {
        let vector = vec![
            named_sample(Some("metric_a"), &[("job", "a")], 1.0),
            named_sample(Some("metric_b"), &[("job", "b")], 2.0),
        ];
        let g = grouping(true, &["job"]);
        let out = aggregate(AggOp::Sum, &vector, Some(&g), None, &mut Annotations::new()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, None);
        assert_eq!(out[0].v, 3.0);
    }

    /// `topk by (__name__)` groups per name but still emits members
    /// verbatim (selection, never computation).
    #[test]
    fn topk_by_dunder_name_groups_per_name_and_stays_verbatim() {
        let vector = vec![
            named_sample(Some("metric_a"), &[("i", "1")], 1.0),
            named_sample(Some("metric_a"), &[("i", "2")], 9.0),
            named_sample(Some("metric_b"), &[("i", "1")], 5.0),
        ];
        let g = grouping(false, &["__name__"]);
        let out = aggregate(
            AggOp::Topk,
            &vector,
            Some(&g),
            Some(1.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        let vals: Vec<(Option<&str>, f64)> = out
            .iter()
            .map(|s| (s.metric_name.as_deref(), s.v))
            .collect();
        assert!(vals.contains(&(Some("metric_a"), 9.0)));
        assert!(vals.contains(&(Some("metric_b"), 5.0)));
    }

    // --- issue #69 (M6-06): count_values ---

    /// `aggregators.test:447-451` (histogram rows excluded): counts per
    /// formatted value, injects the value label, drops `__name__`.
    #[test]
    fn count_values_counts_per_formatted_value_and_drops_the_name() {
        let vector = vec![
            sample(&[("i", "0")], 6.0),
            sample(&[("i", "1")], 6.0),
            sample(&[("i", "2")], 7.0),
        ];
        let out = count_values(&vector, "version", None);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].labels.get("version"), Some("6"));
        assert_eq!(out[0].v, 2.0);
        assert_eq!(out[1].labels.get("version"), Some("7"));
        assert_eq!(out[1].v, 1.0);
        assert!(out.iter().all(|s| s.metric_name.is_none()));
    }

    /// `aggregators.test:467-479` ("Overwrite label with output. Don't do
    /// this."): a destination label already in the group key is
    /// OVERWRITTEN, merging groups that shared a value across distinct
    /// original destinations.
    #[test]
    fn count_values_overwrites_an_existing_group_key_label() {
        // job=api/app both carry value 6 → one merged {job="6"} group.
        let vector = vec![
            sample(&[("job", "api"), ("g", "p")], 6.0),
            sample(&[("job", "app"), ("g", "p")], 6.0),
            sample(&[("job", "app"), ("g", "c")], 7.0),
        ];
        let g = grouping(false, &["job", "g"]);
        let out = count_values(&vector, "job", Some(&g));
        assert_eq!(out.len(), 2, "{out:?}");
        let six = out
            .iter()
            .find(|s| s.labels.get("job") == Some("6"))
            .unwrap();
        assert_eq!(six.labels.get("g"), Some("p"));
        assert_eq!(six.v, 2.0);
        let seven = out
            .iter()
            .find(|s| s.labels.get("job") == Some("7"))
            .unwrap();
        assert_eq!(seven.labels.get("g"), Some("c"));
        assert_eq!(seven.v, 1.0);
    }

    /// Plan v2 Δ1/Δ4: `count_values("__name__", v)` writes the
    /// metric-name CHANNEL — never a `Labels` entry — and the output
    /// still evaluates downstream (a further `count` over it).
    #[test]
    fn count_values_of_dunder_name_writes_the_metric_name_channel() {
        let vector = vec![
            sample(&[("i", "0")], 6.0),
            sample(&[("i", "1")], 6.0),
            sample(&[("i", "2")], 7.0),
        ];
        let out = count_values(&vector, "__name__", None);
        assert_eq!(out.len(), 2);
        assert!(
            out.iter().all(|s| s.labels.get("__name__").is_none()),
            "__name__ must never appear as a Labels entry: {out:?}"
        );
        let six = out
            .iter()
            .find(|s| s.metric_name.as_deref() == Some("6"))
            .unwrap();
        assert_eq!(six.v, 2.0);
        let seven = out
            .iter()
            .find(|s| s.metric_name.as_deref() == Some("7"))
            .unwrap();
        assert_eq!(seven.v, 1.0);
        // Downstream evaluation: count(count_values("__name__", v)) — the
        // synthesized names group away again without tripping anything.
        let downstream =
            aggregate(AggOp::Count, &out, None, None, &mut Annotations::new()).unwrap();
        assert_eq!(downstream.len(), 1);
        assert_eq!(downstream[0].v, 2.0);
    }

    /// Collision golden on the name channel: a `by (__name__)`-injected
    /// name is overwritten by the formatted value, merging two distinct
    /// input metrics that share a value.
    #[test]
    fn count_values_of_dunder_name_overwrites_a_by_name_group_name() {
        let vector = vec![
            named_sample(Some("metric_a"), &[], 6.0),
            named_sample(Some("metric_b"), &[], 6.0),
        ];
        let g = grouping(false, &["__name__"]);
        let out = count_values(&vector, "__name__", Some(&g));
        assert_eq!(out.len(), 1, "shared value 6 merges both names: {out:?}");
        assert_eq!(out[0].metric_name.as_deref(), Some("6"));
        assert_eq!(out[0].v, 2.0);
    }

    /// `format_count_values_value` goldens — Go
    /// `strconv.FormatFloat(f, 'f', -1, 64)` parity for the
    /// formatting-safe classes plus the special-cased non-finite
    /// spellings (plan v2 Δ4 / AC5).
    #[test]
    fn count_values_formatting_matches_go_format_float() {
        for (v, want) in [
            (0.0, "0"),
            (-0.0, "-0"),
            (6.0, "6"),
            (0.5, "0.5"),
            (-2.25, "-2.25"),
            // 'f' never switches to scientific notation, and neither does
            // Rust's Display.
            (1e21, "1000000000000000000000"),
            (1e-7, "0.0000001"),
            (f64::INFINITY, "+Inf"),
            (f64::NEG_INFINITY, "-Inf"),
            (f64::NAN, "NaN"),
        ] {
            assert_eq!(format_count_values_value(v), want, "value {v}");
        }
    }

    // --- issue #69 (M6-06): quantile ---

    /// `aggregators.test:487-556` (φ = 0.8/0.2 over the two/three/uneven/
    /// NaN-sample groups; NaN sorts smallest, interpolation through a NaN
    /// neighbour is NaN).
    #[test]
    fn quantile_matches_the_oracle_groups() {
        let data = |test: &str, vals: &[f64]| -> Vec<InstantSample> {
            vals.iter()
                .enumerate()
                .map(|(i, v)| sample(&[("test", test), ("point", &i.to_string())], *v))
                .collect()
        };
        let mut vector = data("two", &[0.0, 1.0]);
        vector.extend(data("three", &[0.0, 1.0, 2.0]));
        vector.extend(data("uneven", &[0.0, 1.0, 4.0]));
        vector.extend(data("nan", &[0.0, 1.0, f64::NAN]));
        let g = grouping(true, &["point"]);

        let by_test = |out: &[InstantSample], test: &str| -> f64 {
            out.iter()
                .find(|s| s.labels.get("test") == Some(test))
                .unwrap()
                .v
        };
        let p80 = aggregate(
            AggOp::Quantile,
            &vector,
            Some(&g),
            Some(0.8),
            &mut Annotations::new(),
        )
        .unwrap();
        assert!((by_test(&p80, "two") - 0.8).abs() < 1e-12);
        assert!((by_test(&p80, "three") - 1.6).abs() < 1e-12);
        assert!((by_test(&p80, "uneven") - 2.8).abs() < 1e-12);
        // NaN is the smallest sample: rank 1.6 interpolates 0..1 → 0.6.
        assert!((by_test(&p80, "nan") - 0.6).abs() < 1e-12);
        assert!(p80.iter().all(|s| s.metric_name.is_none()));

        let p20 = aggregate(
            AggOp::Quantile,
            &vector,
            Some(&g),
            Some(0.2),
            &mut Annotations::new(),
        )
        .unwrap();
        assert!((by_test(&p20, "two") - 0.2).abs() < 1e-12);
        assert!((by_test(&p20, "three") - 0.4).abs() < 1e-12);
        assert!((by_test(&p20, "uneven") - 0.4).abs() < 1e-12);
        // rank 0.4 interpolates NaN..0 → NaN.
        assert!(by_test(&p20, "nan").is_nan());
    }

    /// Out-of-range/NaN φ clamps per upstream `quantile()` (the #67
    /// convention): φ < 0 → -Inf, φ > 1 → +Inf, φ = NaN → NaN — never an
    /// error.
    #[test]
    fn quantile_phi_out_of_range_clamps_and_never_errors() {
        let vector = vec![sample(&[("s", "1")], 1.0), sample(&[("s", "2")], 2.0)];
        let low = aggregate(
            AggOp::Quantile,
            &vector,
            None,
            Some(-0.5),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(low[0].v, f64::NEG_INFINITY);
        let high = aggregate(
            AggOp::Quantile,
            &vector,
            None,
            Some(1.5),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(high[0].v, f64::INFINITY);
        let nan = aggregate(
            AggOp::Quantile,
            &vector,
            None,
            Some(f64::NAN),
            &mut Annotations::new(),
        )
        .unwrap();
        assert!(nan[0].v.is_nan());
    }

    #[test]
    fn quantile_by_dunder_name_preserves_the_group_name() {
        let vector = vec![
            named_sample(Some("metric_a"), &[("i", "1")], 1.0),
            named_sample(Some("metric_a"), &[("i", "2")], 3.0),
        ];
        let g = grouping(false, &["__name__"]);
        let out = aggregate(
            AggOp::Quantile,
            &vector,
            Some(&g),
            Some(0.5),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name.as_deref(), Some("metric_a"));
        assert_eq!(out[0].v, 2.0);
    }

    #[test]
    fn quantile_without_a_parameter_is_bad_matching() {
        let vector = vec![sample(&[("s", "1")], 1.0)];
        let err = aggregate(
            AggOp::Quantile,
            &vector,
            None,
            None,
            &mut Annotations::new(),
        )
        .unwrap_err();
        assert!(matches!(err, PromqlError::BadMatching { .. }));
    }

    // --- issue #69 (M6-06): limitk (hash/order-independent invariants) ---

    fn six_series() -> Vec<InstantSample> {
        (0..6)
            .map(|i| {
                let group = if i < 2 { "production" } else { "canary" };
                sample(&[("instance", &i.to_string()), ("group", group)], i as f64)
            })
            .collect()
    }

    /// `count(limitk(k, v)) == min(k, N)` per group, in input order (the
    /// vendored `limit.test:17-45` methodology, minus its `and`-based
    /// subset assertion — subset is asserted structurally below).
    #[test]
    fn limitk_count_is_min_of_k_and_group_size() {
        let vector = six_series();
        for (k, want) in [(1.0, 1), (2.0, 2), (5.0, 5), (6.0, 6), (100.0, 6)] {
            let out = aggregate(
                AggOp::LimitK,
                &vector,
                None,
                Some(k),
                &mut Annotations::new(),
            )
            .unwrap();
            assert_eq!(out.len(), want, "k={k}");
        }
        let g = grouping(false, &["group"]);
        // production has 2 members, canary 4: min(3,2)+min(3,4) = 5.
        let out = aggregate(
            AggOp::LimitK,
            &vector,
            Some(&g),
            Some(3.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out.len(), 5);
    }

    /// Subset + verbatim: every selected series IS one of the inputs,
    /// byte-identical (`__name__` kept — selection never computes).
    #[test]
    fn limitk_selects_a_verbatim_subset_of_the_input() {
        let vector = six_series();
        let out = aggregate(
            AggOp::LimitK,
            &vector,
            None,
            Some(3.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out.len(), 3);
        for s in &out {
            assert!(vector.contains(s), "not a verbatim input member: {s:?}");
            assert_eq!(s.metric_name.as_deref(), Some("test_metric"));
        }
    }

    /// The inherited topk k-guard (plan v2 Δ4, deliberately pinned for
    /// the corpus-unpinned cases): `k < 1` and ±Inf yield empty;
    /// fractional k truncates. NaN is NOT here — it errors (below).
    #[test]
    fn limitk_parameter_boundaries_follow_the_topk_guard() {
        let vector = six_series();
        for k in [0.0, -1.0, 0.9, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                aggregate(
                    AggOp::LimitK,
                    &vector,
                    None,
                    Some(k),
                    &mut Annotations::new()
                )
                .unwrap()
                .is_empty(),
                "k={k} must select nothing"
            );
        }
        let out = aggregate(
            AggOp::LimitK,
            &vector,
            None,
            Some(2.9),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(out.len(), 2, "fractional k truncates");
    }

    /// `aggregators.test:425-426`: a NaN k is a query error with
    /// upstream's exact message — even over an empty selection.
    #[test]
    fn limitk_nan_parameter_is_a_query_error() {
        for vector in [six_series(), Vec::new()] {
            let err = aggregate(
                AggOp::LimitK,
                &vector,
                None,
                Some(f64::NAN),
                &mut Annotations::new(),
            )
            .unwrap_err();
            match err {
                PromqlError::InvalidParameter { ref detail } => {
                    assert_eq!(detail, "Parameter value is NaN")
                }
                other => panic!("expected InvalidParameter, got {other:?}"),
            }
            assert!(err.to_string().contains("Parameter value is NaN"));
        }
    }

    #[test]
    fn limitk_without_a_parameter_is_bad_matching() {
        let err = aggregate(
            AggOp::LimitK,
            &six_series(),
            None,
            None,
            &mut Annotations::new(),
        )
        .unwrap_err();
        assert!(matches!(err, PromqlError::BadMatching { .. }));
    }

    /// Cross-step stability: the same identities select the same subset
    /// regardless of sample timestamps/values (selection is identity-
    /// keyed input-order truncation, nothing else).
    #[test]
    fn limitk_selection_is_stable_across_steps() {
        let vector = six_series();
        let later: Vec<InstantSample> = vector
            .iter()
            .map(|s| InstantSample {
                t_ms: s.t_ms + 60_000,
                v: s.v + 100.0,
                ..s.clone()
            })
            .collect();
        let a: Vec<Labels> = aggregate(
            AggOp::LimitK,
            &vector,
            None,
            Some(3.0),
            &mut Annotations::new(),
        )
        .unwrap()
        .into_iter()
        .map(|s| s.labels)
        .collect();
        let b: Vec<Labels> = aggregate(
            AggOp::LimitK,
            &later,
            None,
            Some(3.0),
            &mut Annotations::new(),
        )
        .unwrap()
        .into_iter()
        .map(|s| s.labels)
        .collect();
        assert_eq!(a, b);
    }

    // --- issue #69 (M6-06): limit_ratio ---

    /// Boundary sets (plan v2 Δ2, upstream-faithful): `r = 0` → empty;
    /// `r = -1` → ALL (offset ≥ 0 always — the only GUARANTEED-all
    /// boundary); out-of-range r caps to ±1.
    #[test]
    fn limit_ratio_boundaries() {
        let vector = six_series();
        assert!(
            aggregate(
                AggOp::LimitRatio,
                &vector,
                None,
                Some(0.0),
                &mut Annotations::new()
            )
            .unwrap()
            .is_empty()
        );
        let all = aggregate(
            AggOp::LimitRatio,
            &vector,
            None,
            Some(-1.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(all, vector, "r = -1 selects everything, verbatim");
        // Caps: 1.1 → 1.0 and -1.1 → -1.0 (cap warn annotation deferred
        // to M6-08).
        let capped_pos = aggregate(
            AggOp::LimitRatio,
            &vector,
            None,
            Some(1.1),
            &mut Annotations::new(),
        )
        .unwrap();
        let at_one = aggregate(
            AggOp::LimitRatio,
            &vector,
            None,
            Some(1.0),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(capped_pos, at_one);
        let capped_neg = aggregate(
            AggOp::LimitRatio,
            &vector,
            None,
            Some(-1.1),
            &mut Annotations::new(),
        )
        .unwrap();
        assert_eq!(capped_neg, vector);
    }

    /// `aggregators.test:428-429`: a NaN r is a query error with
    /// upstream's exact message — even over an empty selection.
    #[test]
    fn limit_ratio_nan_parameter_is_a_query_error() {
        for vector in [six_series(), Vec::new()] {
            let err = aggregate(
                AggOp::LimitRatio,
                &vector,
                None,
                Some(f64::NAN),
                &mut Annotations::new(),
            )
            .unwrap_err();
            match err {
                PromqlError::InvalidParameter { ref detail } => {
                    assert_eq!(detail, "Ratio value is NaN")
                }
                other => panic!("expected InvalidParameter, got {other:?}"),
            }
            assert!(err.to_string().contains("Ratio value is NaN"));
        }
    }

    #[test]
    fn limit_ratio_without_a_parameter_is_bad_matching() {
        let err = aggregate(
            AggOp::LimitRatio,
            &six_series(),
            None,
            None,
            &mut Annotations::new(),
        )
        .unwrap_err();
        assert!(matches!(err, PromqlError::BadMatching { .. }));
    }

    /// Complement partition (`limit.test:127-158`'s `or`/`and` cases,
    /// asserted structurally without set ops): for any r ∈ (0, 1),
    /// `limit_ratio(r, v)` and `limit_ratio(-(1-r), v)` are disjoint and
    /// their union is the whole input — true for ANY fixed offset, which
    /// is exactly why this invariant (not the specific subset) is the
    /// gated surface.
    #[test]
    fn limit_ratio_complements_partition_the_input() {
        let vector = six_series();
        for r in [0.2, 0.5, 0.8] {
            let selected = aggregate(
                AggOp::LimitRatio,
                &vector,
                None,
                Some(r),
                &mut Annotations::new(),
            )
            .unwrap();
            let complement = aggregate(
                AggOp::LimitRatio,
                &vector,
                None,
                Some(-(1.0 - r)),
                &mut Annotations::new(),
            )
            .unwrap();
            assert_eq!(
                selected.len() + complement.len(),
                vector.len(),
                "r={r}: union must be everything"
            );
            for s in &selected {
                assert!(
                    !complement.contains(s),
                    "r={r}: {s:?} in both sides — not disjoint"
                );
            }
            // Union == input (both sides preserve input order, so a merge
            // check suffices).
            let mut union: Vec<&InstantSample> = selected.iter().chain(&complement).collect();
            union.sort_by(|a, b| a.labels.cmp(&b.labels));
            let mut want: Vec<&InstantSample> = vector.iter().collect();
            want.sort_by(|a, b| a.labels.cmp(&b.labels));
            assert_eq!(union, want, "r={r}");
        }
    }

    /// Cross-step stability: offsets hash the series identity only —
    /// timestamps/values never change the selection.
    #[test]
    fn limit_ratio_selection_is_stable_across_steps() {
        let vector = six_series();
        let later: Vec<InstantSample> = vector
            .iter()
            .map(|s| InstantSample {
                t_ms: s.t_ms + 60_000,
                v: s.v + 100.0,
                ..s.clone()
            })
            .collect();
        let a: Vec<Labels> = aggregate(
            AggOp::LimitRatio,
            &vector,
            None,
            Some(0.5),
            &mut Annotations::new(),
        )
        .unwrap()
        .into_iter()
        .map(|s| s.labels)
        .collect();
        let b: Vec<Labels> = aggregate(
            AggOp::LimitRatio,
            &later,
            None,
            Some(0.5),
            &mut Annotations::new(),
        )
        .unwrap()
        .into_iter()
        .map(|s| s.labels)
        .collect();
        assert_eq!(a, b);
    }

    /// The Δ2 boundary golden: `u64::MAX as f64` rounds to 2^64, so a
    /// top-band hash yields an offset of exactly `1.0` — which
    /// `ratio_includes(1.0, ·)` EXCLUDES (`offset < r` is strict),
    /// reproducing upstream's `AddRatioSample` non-guarantee at r = 1.0.
    #[test]
    fn offset_can_round_to_one_and_is_excluded_at_ratio_one() {
        assert_eq!(offset_from_hash(u64::MAX), 1.0);
        assert!(!ratio_includes(1.0, 1.0));
        // …while r = -1.0 includes every representable offset, 1.0
        // included: offset >= 1.0 + (-1.0) = 0.0 always holds.
        assert!(ratio_includes(-1.0, 0.0));
        assert!(ratio_includes(-1.0, 1.0));
        assert_eq!(offset_from_hash(0), 0.0);
        assert!(!ratio_includes(0.0, 0.0));
    }

    /// The offset hashes the FULL identity — `__name__` included at its
    /// lexical position — so two series differing only by metric name get
    /// independent offsets.
    #[test]
    fn series_offset_depends_on_the_metric_name_channel() {
        let a = named_sample(Some("metric_a"), &[("job", "x")], 1.0);
        let b = named_sample(Some("metric_b"), &[("job", "x")], 1.0);
        assert_ne!(series_offset(&a), series_offset(&b));
        // And is a pure function of identity.
        assert_eq!(series_offset(&a), series_offset(&a.clone()));
        let o = series_offset(&a);
        assert!((0.0..=1.0).contains(&o));
    }
}
