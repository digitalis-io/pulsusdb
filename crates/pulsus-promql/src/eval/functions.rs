//! Range-vector functions (`rate`/`irate`/`increase`/`delta`),
//! `*_over_time` aggregations, and `histogram_quantile` — ported from
//! Prometheus v3.13's `promql/functions.go` (`extrapolatedRate`,
//! `instantValue`) and `promql/quantile.go` (`bucketQuantile`), not
//! re-derived.

use crate::error::PromqlError;
use crate::math::{KahanSum, kahan_inc};
use crate::plan::{OverTimeFn, OverTimeParamFn, RangeFn};
use crate::value::Sample;

/// `rate`/`increase`/`delta` + `irate`'s shared entry point. `samples` must
/// already be windowed to the step's range-vector window (left-open
/// right-closed, with any stale-NaN-marked sample already excluded by the
/// caller — [`crate::eval`]'s windowing helper) and sorted ascending.
/// `range_start_ms`/`range_end_ms` are the *nominal* window edges (`t −
/// offset − range`, `t − offset`) used only for the extrapolation distance
/// calculation, not for filtering (filtering already happened).
pub fn eval_range_fn(
    func: RangeFn,
    samples: &[Sample],
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
) -> Option<f64> {
    match func {
        RangeFn::Irate => eval_irate(samples),
        RangeFn::Rate => {
            eval_extrapolated(samples, range_ms, range_start_ms, range_end_ms, true, true)
        }
        RangeFn::Increase => {
            eval_extrapolated(samples, range_ms, range_start_ms, range_end_ms, true, false)
        }
        RangeFn::Delta => eval_extrapolated(
            samples,
            range_ms,
            range_start_ms,
            range_end_ms,
            false,
            false,
        ),
    }
}

/// `irate` — Prometheus's `instantValue(vals, samples, isRate=true)`: uses
/// only the last two samples, no extrapolation. A drop between them is
/// treated as a counter reset (the result is simply the last value, not
/// `last - previous`).
///
/// Issue #39 audit: re-verified operation-for-operation against
/// `promql/functions.go` (v3.13.0, lines 829-834, 836-840, 874-880) —
/// `sampledInterval := ss[1].T - ss[0].T` (a single `i64` subtraction,
/// matching `interval_ms` here), the reset-vs-diff branch (`ss[1].F -
/// ss[0].F`, matching `last.v - prev.v`), and a single final division
/// `resultSample.F /= float64(sampledInterval) / 1000` (matching `result /=
/// interval_ms as f64 / 1000.0`) — already bit-exact, unlike
/// `eval_extrapolated` below; no change needed here.
fn eval_irate(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let last = &samples[samples.len() - 1];
    let prev = &samples[samples.len() - 2];
    let interval_ms = last.t_ms - prev.t_ms;
    if interval_ms == 0 {
        return None;
    }
    let mut result = if last.v < prev.v {
        last.v
    } else {
        last.v - prev.v
    };
    result /= interval_ms as f64 / 1000.0;
    Some(result)
}

/// `rate`/`increase`/`delta` — Prometheus's `extrapolatedRate`
/// (`promql/functions.go`, v3.13.0, lines 471-591), ported
/// operation-for-operation, not just formula-for-formula (issue #39: a
/// prior version of this port computed the right *values* via a
/// differently-*ordered* sequence of floating-point operations, which
/// silently produced 1-2 ULP-divergent results from real Prometheus on
/// real inputs — see the two numbered notes below for the two spots that
/// actually mattered).
///
/// Requires at least 2 samples in the window (the extrapolation heuristic
/// needs at least one interval to average).
fn eval_extrapolated(
    samples: &[Sample],
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
    is_counter: bool,
    is_rate: bool,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let first = &samples[0];
    let last = &samples[samples.len() - 1];
    // Line 510: `resultFloat = samples.Floats[numSamplesMinusOne].F -
    // samples.Floats[0].F` (last - first).
    let mut result_value = last.v - first.v;

    // Lines 519-524: counter-reset correction. Go's loop walks
    // `samples.Floats[1:]` (never comparing the very first sample against
    // anything), pairing each element with its immediate predecessor.
    // Starting `last_value` at `0.0` and looping over every sample
    // (including the first) is equivalent for a counter — the first
    // comparison (`first.v < 0.0`) can never fire for a genuine
    // non-negative counter reading — without needing a second slice index.
    if is_counter {
        let mut last_value = 0.0_f64;
        for s in samples {
            if s.v < last_value {
                result_value += last_value;
            }
            last_value = s.v;
        }
    }

    // Lines 531-535.
    let mut duration_to_start = (first.t_ms - range_start_ms) as f64 / 1000.0;
    let mut duration_to_end = (range_end_ms - last.t_ms) as f64 / 1000.0;
    let sampled_interval = (last.t_ms - first.t_ms) as f64 / 1000.0;
    let average_duration_between_samples = sampled_interval / (samples.len() - 1) as f64;
    let extrapolation_threshold = average_duration_between_samples * 1.1;

    // issue #39 note 1 — ORDER matters here, not just the two formulas in
    // isolation: upstream clamps `durationToStart` to the threshold FIRST
    // (lines 550-552), and only *then* (lines 553-574) applies the
    // counter-cannot-go-negative zero-point override, comparing
    // `durationToZero` against the *already-clamped* `duration_to_start` —
    // never the raw value. Computing the zero-point override against the
    // raw `duration_to_start` (as a prior version of this port did) and
    // applying the threshold clamp as a separate, later decision is a
    // different sequence of comparisons, which can select a different
    // final `duration_to_start` (not merely round differently).
    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration_between_samples / 2.0;
    }
    if is_counter {
        // Lines 560-573: `durationToZero := durationToStart` is the
        // fallback when the zero-crossing isn't computable — mirrored
        // here by pre-seeding `duration_to_zero` with the (already
        // threshold-clamped) `duration_to_start` so the final `if
        // duration_to_zero < duration_to_start` comparison is a genuine
        // no-op in that case, exactly as upstream's is.
        let mut duration_to_zero = duration_to_start;
        if result_value > 0.0 && first.v >= 0.0 {
            duration_to_zero = sampled_interval * (first.v / result_value);
        }
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    // Lines 576-578: `duration_to_end`'s own threshold clamp — independent
    // of the counter zero-point logic above, which only ever touches
    // `duration_to_start`.
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    // issue #39 note 2 — the actual root cause of the observed ULP
    // divergence: lines 580-585 fully reduce `factor` (including the `/
    // ms.Range.Seconds()` division when `is_rate`) into ONE value, THEN
    // multiply `resultFloat` by it exactly once. `(a * b) / c` and `a *
    // (b / c)` round differently in IEEE 754 even though they're
    // mathematically equal — a prior version of this port did
    // `result_value *= (extrapolate_to_interval / sampled_interval)`
    // followed by a *separate* `result_value /= range_seconds`, which is
    // the `(a*b)/c` shape upstream never performs.
    let mut factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    if is_rate {
        factor /= range_ms as f64 / 1000.0;
    }
    result_value *= factor;
    Some(result_value)
}

/// `avg/min/max/sum/count_over_time`. `None` for an empty window (series
/// absent at this step) — never a wrong `0`/`NaN` standing in for absence.
///
/// `sum_over_time` uses [`KahanSum`] (upstream `funcSumOverTime`,
/// `promql/functions.go` v3.13.0: `sum, c := 0., 0.; for _, f := range
/// s.Floats { sum, c = kahansum.Inc(f.F, sum, c) }; return sum + c` — seeds
/// at `0.0` and Kahan-adds *every* sample, exactly what [`KahanSum::new`]
/// + [`KahanSum::add`]-per-sample + [`KahanSum::value`] does).
///
/// `avg_over_time` (issue #39 audit finding) is **not** `sum_over_time /
/// count` — upstream's `funcAvgOverTime` uses a materially different
/// accumulation (see that function's own doc comment below) that this
/// port now replicates operation-for-operation instead of reusing
/// [`KahanSum`] the same way `sum_over_time` does.
///
/// `min`/`max_over_time` (issue #39) are now bit-faithful, including
/// `NaN`/signed-zero tie-breaking — see [`eval_extremum_over_time`], ported
/// from upstream's `compareOverTime` (`promql/functions.go` v3.13.0,
/// lines 1467-1494). `count_over_time` is a plain length and carries no
/// ULP or `NaN` risk at all.
pub fn eval_over_time(func: OverTimeFn, samples: &[Sample]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    match func {
        OverTimeFn::Count => Some(samples.len() as f64),
        OverTimeFn::Sum => {
            let mut k = KahanSum::new();
            for s in samples {
                k.add(s.v);
            }
            Some(k.value())
        }
        OverTimeFn::Avg => Some(eval_avg_over_time(samples)),
        OverTimeFn::Min => Some(eval_extremum_over_time(samples, WhichExtremum::Min)),
        OverTimeFn::Max => Some(eval_extremum_over_time(samples, WhichExtremum::Max)),
        // Issue #67 (M6-04) — the rest of the parameterless range-window
        // surface, each ported from `promql/functions.go` v3.13.0.
        OverTimeFn::Stddev => Some(eval_stdvar_over_time(samples).sqrt()),
        OverTimeFn::Stdvar => Some(eval_stdvar_over_time(samples)),
        // `funcLastOverTime`/`funcFirstOverTime`: the positional
        // last/first float sample's value (`el.Floats[len-1]` /
        // `el.Floats[0]` — floats-only here, #22 owns native histograms).
        OverTimeFn::Last => samples.last().map(|s| s.v),
        OverTimeFn::First => samples.first().map(|s| s.v),
        // `funcPresentOverTime`: `1` for any series with a sample in the
        // window (the empty case already returned `None` above).
        OverTimeFn::Present => Some(1.0),
        OverTimeFn::Idelta => eval_idelta(samples),
        OverTimeFn::Resets => Some(eval_resets(samples)),
        OverTimeFn::Changes => Some(eval_changes(samples)),
        OverTimeFn::Deriv => eval_deriv(samples),
        OverTimeFn::Mad => Some(eval_mad_over_time(samples)),
        OverTimeFn::TsOfMin => Some(eval_ts_of_extremum(samples, TsExtremum::Min)),
        OverTimeFn::TsOfMax => Some(eval_ts_of_extremum(samples, TsExtremum::Max)),
        // `funcTsOfFirstOverTime`/`funcTsOfLastOverTime`: positional.
        OverTimeFn::TsOfFirst => samples.first().map(|s| s.t_ms as f64 / 1000.0),
        OverTimeFn::TsOfLast => samples.last().map(|s| s.t_ms as f64 / 1000.0),
    }
}

/// The parameterized range-window functions (issue #67, M6-04):
/// `quantile_over_time(φ, m[r])`, `predict_linear(m[r], t)`,
/// `double_exponential_smoothing(m[r], sf, tf)`. `args` carries the
/// already-evaluated scalar parameter(s) in registry order (the planner
/// guarantees the count; re-checked structurally — a descriptive error,
/// never a panic). `eval_t_ms` is the evaluation step time —
/// `predict_linear`'s intercept timestamp (upstream `enh.Ts`; the #67
/// adjudication pins step time, NOT the offset-adjusted window edge —
/// the offset golden is M6-08's obligation).
///
/// Only `DoubleExpSmoothing` can error ([`PromqlError::InvalidParameter`]
/// on an out-of-range factor — upstream panics there); `Quantile`'s
/// out-of-range φ yields `±Inf`/`NaN` per upstream `quantile`, never an
/// error. The evaluator only calls this for a series with ≥ 1 windowed
/// sample (upstream's engine-level invocation guard — see
/// [`eval_double_exponential_smoothing`]'s validation-ordering rule), so
/// the empty-`samples` paths here are defensive totality, not reachable
/// through `evaluate`.
pub fn eval_over_time_param(
    func: OverTimeParamFn,
    samples: &[Sample],
    args: &[f64],
    eval_t_ms: i64,
) -> Result<Option<f64>, PromqlError> {
    match (func, args) {
        // `funcQuantileOverTime` (v3.13.0): an empty window yields no
        // sample *before* φ is even ranged-checked; otherwise the shared
        // `quantile` interpolation (out-of-range φ included — a warn
        // annotation upstream, never an error).
        (OverTimeParamFn::Quantile, &[phi]) => {
            if samples.is_empty() {
                return Ok(None);
            }
            let mut values: Vec<f64> = samples.iter().map(|s| s.v).collect();
            Ok(Some(quantile_of(phi, &mut values)))
        }
        // `funcPredictLinear` (v3.13.0): `slope*duration + intercept`,
        // regression intercept at `enh.Ts`; `< 2` samples yields nothing.
        (OverTimeParamFn::PredictLinear, &[duration_s]) => {
            if samples.len() < 2 {
                return Ok(None);
            }
            let (slope, intercept) = linear_regression(samples, eval_t_ms);
            Ok(Some(slope * duration_s + intercept))
        }
        (OverTimeParamFn::DoubleExpSmoothing, &[sf, tf]) => {
            eval_double_exponential_smoothing(samples, sf, tf)
        }
        (func, _) => Err(PromqlError::Unsupported {
            construct: format!(
                "{func:?} with {} scalar argument(s) — plan() guarantees the per-function \
                 count; this plan was not built by plan()",
                args.len()
            ),
        }),
    }
}

/// Simple linear regression — ported operation-for-operation from
/// `promql/functions.go` v3.13.0's `linearRegression(samples,
/// interceptTime)`: **plain uncompensated `+=` accumulation** for
/// `sumX/sumY/sumXY/sumX2` (the #67 review round-1 finding + adjudication:
/// upstream does NOT Kahan-compensate here, and compensating would itself
/// be a divergence — the #39 lesson is "match the reference's arithmetic",
/// not "improve" it), plus the `constY` short-circuit: a constant series
/// returns `(0, y)` before any division, or `(NaN, NaN)` when the constant
/// is `±Inf`. `x` is `(t_ms - intercept_time_ms) / 1e3` seconds, exactly
/// upstream's `float64(sample.T-interceptTime) / 1e3`.
///
/// Callers guarantee `samples.len() >= 2` (`deriv`/`predict_linear` both
/// return `None` below that); `samples[0]` indexing is therefore safe, and
/// kept total anyway via the non-empty debug assert.
fn linear_regression(samples: &[Sample], intercept_time_ms: i64) -> (f64, f64) {
    debug_assert!(samples.len() >= 2, "callers check < 2 samples first");
    let init_y = samples[0].v;
    let mut const_y = true;
    let mut n = 0.0_f64;
    let mut sum_x = 0.0_f64;
    let mut sum_y = 0.0_f64;
    let mut sum_xy = 0.0_f64;
    let mut sum_x2 = 0.0_f64;
    for (i, s) in samples.iter().enumerate() {
        if const_y && i > 0 && s.v != init_y {
            const_y = false;
        }
        n += 1.0;
        let x = (s.t_ms - intercept_time_ms) as f64 / 1e3;
        sum_x += x;
        sum_y += s.v;
        sum_xy += x * s.v;
        sum_x2 += x * x;
    }
    if const_y {
        if init_y.is_infinite() {
            return (f64::NAN, f64::NAN);
        }
        return (0.0, init_y);
    }
    let cov_xy = sum_xy - sum_x * sum_y / n;
    let var_x = sum_x2 - sum_x * sum_x / n;
    let slope = cov_xy / var_x;
    let intercept = sum_y / n - slope * sum_x / n;
    (slope, intercept)
}

/// `deriv` — `funcDeriv` (v3.13.0): the regression slope with the
/// intercept timestamp anchored at the window's **first sample**
/// (`samples.Floats[0].T` — upstream's own comment: an arbitrary
/// timestamp near the values, avoiding float accuracy issues). `< 2`
/// samples yields nothing.
fn eval_deriv(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let (slope, _) = linear_regression(samples, samples[0].t_ms);
    Some(slope)
}

/// `idelta` — upstream's shared `instantValue(vals, samples, isRate=false)`
/// rule set, mirroring [`eval_irate`] (v3.13.0 lines 829-880): `< 2`
/// samples → no sample, **equal last-two timestamps → no sample** (the
/// zero-interval drop applies to both rate and non-rate modes — #67 review
/// round-1 Δ2), otherwise the last two floats' plain difference (no
/// counter-reset branch and no per-second division in non-rate mode).
fn eval_idelta(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let last = &samples[samples.len() - 1];
    let prev = &samples[samples.len() - 2];
    if last.t_ms - prev.t_ms == 0 {
        return None;
    }
    Some(last.v - prev.v)
}

/// `resets` — `funcResets` (v3.13.0, floats-only path): counts strict
/// drops (`cur < prev`) between consecutive samples. IEEE `<` makes any
/// NaN-adjacent pair a non-reset (both `NaN < x` and `x < NaN` are false).
fn eval_resets(samples: &[Sample]) -> f64 {
    let mut resets = 0.0_f64;
    for w in samples.windows(2) {
        if w[1].v < w[0].v {
            resets += 1.0;
        }
    }
    resets
}

/// `changes` — `funcChanges` (v3.13.0, floats-only path): counts
/// consecutive pairs where `cur != prev`, except the both-NaN pair
/// (upstream's explicit `!(IsNaN(cur) && IsNaN(prev))`) — a one-sided NaN
/// transition (`0 → NaN`, `NaN → 0`) still counts.
fn eval_changes(samples: &[Sample]) -> f64 {
    let mut changes = 0.0_f64;
    for w in samples.windows(2) {
        let (prev, cur) = (w[0].v, w[1].v);
        if cur != prev && !(cur.is_nan() && prev.is_nan()) {
            changes += 1.0;
        }
    }
    changes
}

/// `stdvar_over_time` (`stddev_over_time` = its square root) —
/// `funcStdvarOverTime`/`funcStddevOverTime` (v3.13.0): Welford's online
/// recurrence with **Kahan-compensated** mean and aux accumulators
/// (`kahanSumInc` on both, exactly upstream — the #67 adjudication keeps
/// this function family's own Kahan convention; only `linearRegression`
/// is plain). Callers guarantee non-empty (the shared `eval_over_time`
/// empty check).
fn eval_stdvar_over_time(samples: &[Sample]) -> f64 {
    let mut count = 0.0_f64;
    let mut mean = 0.0_f64;
    let mut c_mean = 0.0_f64;
    let mut aux = 0.0_f64;
    let mut c_aux = 0.0_f64;
    for s in samples {
        count += 1.0;
        let delta = s.v - (mean + c_mean);
        let (new_mean, new_c_mean) = kahan_inc(delta / count, mean, c_mean);
        mean = new_mean;
        c_mean = new_c_mean;
        let (new_aux, new_c_aux) = kahan_inc(delta * (s.v - (mean + c_mean)), aux, c_aux);
        aux = new_aux;
        c_aux = new_c_aux;
    }
    (aux + c_aux) / count
}

/// `mad_over_time` — `funcMadOverTime` (v3.13.0): the median of absolute
/// deviations from the median, both medians via the shared [`quantile_of`]
/// interpolation at φ = 0.5.
fn eval_mad_over_time(samples: &[Sample]) -> f64 {
    let mut values: Vec<f64> = samples.iter().map(|s| s.v).collect();
    let median = quantile_of(0.5, &mut values);
    let mut deviations: Vec<f64> = samples.iter().map(|s| (s.v - median).abs()).collect();
    quantile_of(0.5, &mut deviations)
}

/// Which extremum [`eval_ts_of_extremum`] tracks (an enum, not a boolean
/// parameter, per house style).
#[derive(Debug, Clone, Copy)]
enum TsExtremum {
    Min,
    Max,
}

/// `ts_of_min/max_over_time` — `funcTsOfMinOverTime`/`funcTsOfMaxOverTime`
/// (v3.13.0), comparison rules pinned per the #67 review round-1 Δ4:
/// strict `<`/`>` keeps the **first** occurrence of an equal finite
/// extremum; the `|| IsNaN(extremum)` disjunct **replaces a leading NaN**
/// with the first non-NaN sample (and keeps replacing while the tracked
/// extremum is NaN), so an **all-NaN window selects the last sample**.
/// Result is the selected sample's timestamp in seconds. Callers
/// guarantee non-empty.
fn eval_ts_of_extremum(samples: &[Sample], which: TsExtremum) -> f64 {
    debug_assert!(!samples.is_empty(), "caller already checked non-empty");
    let mut extremum = samples[0].v;
    let mut ts_ms = samples[0].t_ms;
    for s in samples {
        let better = match which {
            TsExtremum::Min => s.v < extremum,
            TsExtremum::Max => s.v > extremum,
        };
        if better || extremum.is_nan() {
            extremum = s.v;
            ts_ms = s.t_ms;
        }
    }
    ts_ms as f64 / 1000.0
}

/// The scalar quantile over raw values — ported from `promql/quantile.go`
/// v3.13.0's `quantile(q, values)`: NaN/out-of-range φ **clamps, never
/// errors** (`NaN → NaN`, `φ < 0 → -Inf`, `φ > 1 → +Inf` — the same rule
/// [`histogram_quantile`] above already applies, #67 plan v2 Δ5), then
/// sorts and linearly interpolates between the two straddling ranks.
/// Sorting places NaN values **first** (upstream's `vectorByValueHeap.Less`
/// returns true whenever the left value is NaN), materialized here as a
/// total order so `sort_by`'s contract holds: NaN < non-NaN, NaN == NaN.
/// Sorts `values` in place. Shared by `quantile_over_time`,
/// `mad_over_time`, and (issue #69, M6-06) the `quantile` aggregation
/// operator — upstream's own `quantile()` is likewise the single shared
/// implementation for all three.
pub(crate) fn quantile_of(phi: f64, values: &mut [f64]) -> f64 {
    if values.is_empty() || phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    values.sort_by(|a, b| match (a.is_nan(), b.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        // Total by construction: both sides are non-NaN here.
        (false, false) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
    });

    let n = values.len() as f64;
    let rank = phi * (n - 1.0);
    let lower_index = rank.floor().max(0.0);
    let upper_index = (lower_index + 1.0).min(n - 1.0);
    let weight = rank - rank.floor();
    values[lower_index as usize] * (1.0 - weight) + values[upper_index as usize] * weight
}

/// `double_exponential_smoothing` — `funcDoubleExponentialSmoothing`
/// (v3.13.0, experimental): Holt's linear-trend smoothing. Factor
/// validation mirrors upstream's `sf <= 0 || sf >= 1` panics (here a
/// [`PromqlError::InvalidParameter`] query error naming the parameter and
/// bounds — the #67 adjudication).
///
/// **Validation-ordering rule** (#67 code review finding 2, pinned
/// empirically against prom/prometheus:v3.13.0): *within an invocation*,
/// validation runs **before** the `< 2` samples check, exactly like
/// upstream — a one-sample window with a bad factor is an error, not an
/// empty result — but the **engine only invokes a matrix function for a
/// series with ≥ 1 point in the window** (the `PlanExpr::OverTimeParam`
/// arm's empty-window skip, mirroring engine.go's
/// `len(ss.Floats)+len(ss.Histograms) > 0` guard), so a selector matching
/// nothing (or only empty windows) succeeds with an empty result even
/// when the factors are invalid. A NaN factor passes both comparisons
/// (IEEE: `NaN <= 0` and `NaN >= 1` are both false), exactly as
/// upstream's Go comparisons do — no extra NaN rejection is added.
fn eval_double_exponential_smoothing(
    samples: &[Sample],
    sf: f64,
    tf: f64,
) -> Result<Option<f64>, PromqlError> {
    if sf <= 0.0 || sf >= 1.0 {
        return Err(PromqlError::InvalidParameter {
            detail: format!("invalid smoothing factor: expected 0 < sf < 1, got {sf}"),
        });
    }
    if tf <= 0.0 || tf >= 1.0 {
        return Err(PromqlError::InvalidParameter {
            detail: format!("invalid trend factor: expected 0 < tf < 1, got {tf}"),
        });
    }
    if samples.len() < 2 {
        return Ok(None);
    }

    let mut s0 = 0.0_f64;
    let mut s1 = samples[0].v;
    let mut b = samples[1].v - samples[0].v;
    for (i, sample) in samples.iter().enumerate().skip(1) {
        // Scale the raw value against the smoothing factor.
        let x = sf * sample.v;
        // Scale the last smoothed value with the trend at this point.
        b = calc_trend_value(i - 1, tf, s0, s1, b);
        let y = (1.0 - sf) * (s1 + b);
        s0 = s1;
        s1 = x + y;
    }
    Ok(Some(s1))
}

/// `calcTrendValue` (v3.13.0): the trend recurrence
/// `tf*(s1-s0) + (1-tf)*b`, with the seed trend `b` passed through
/// unchanged on the first iteration (`i == 0`).
fn calc_trend_value(i: usize, tf: f64, s0: f64, s1: f64, b: f64) -> f64 {
    if i == 0 {
        return b;
    }
    let x = tf * (s1 - s0);
    let y = (1.0 - tf) * b;
    x + y
}

/// Which extremum [`eval_extremum_over_time`] tracks (an enum, not a
/// boolean parameter, per house style).
#[derive(Debug, Clone, Copy)]
enum WhichExtremum {
    Min,
    Max,
}

/// `min_over_time`/`max_over_time` float path — upstream `compareOverTime`
/// (`promql/functions.go` v3.13.0, lines 1467-1494) ported
/// operation-for-operation: seed with the first sample, then for every
/// sample replace when `(cur </> extremum) || extremum.is_nan()`.
/// Strict `</>` makes `-0 == +0` a non-replacement, so the first sample
/// wins on a signed-zero tie; the `is_nan()` disjunct keeps replacing while
/// the accumulator is NaN, so a leading NaN is overwritten by the first
/// non-NaN sample and an all-NaN window returns NaN. Structurally the same
/// comparison shape as [`eval_ts_of_extremum`] below. Callers guarantee
/// non-empty.
fn eval_extremum_over_time(samples: &[Sample], which: WhichExtremum) -> f64 {
    debug_assert!(!samples.is_empty(), "caller already checked non-empty");
    let mut extremum = samples[0].v;
    for s in samples {
        let replace = match which {
            WhichExtremum::Min => s.v < extremum,
            WhichExtremum::Max => s.v > extremum,
        } || extremum.is_nan();
        if replace {
            extremum = s.v;
        }
    }
    extremum
}

/// `avg_over_time` — upstream's `funcAvgOverTime` float path
/// (`promql/functions.go` v3.13.0, lines 1267-1297), ported
/// operation-for-operation (issue #39 audit finding: this is *not*
/// `sum_over_time(...) / count(...)`, and the original port here computed
/// it that way — a genuinely different accumulation, not just a
/// differently-rounded path to the same formula):
///
/// - `sum` is **seeded with the first sample's raw value directly** — no
///   Kahan compensation is applied to it — and only the *second* sample
///   onward is folded in via [`kahan_inc`]. Contrast `sum_over_time`
///   (above), which seeds at `0.0` and Kahan-adds *every* sample including
///   the first.
/// - The final combination is `sum/count + kahanC/count` — **two**
///   separate divisions, then added — never `(sum + kahanC) / count`.
/// - If the running sum ever overflows to `±Inf`, upstream falls back to
///   an *incremental mean* recurrence (`mean`/`kahanC` updated per-sample
///   via `q := (count-1)/count`) for the remainder of the series. Ported
///   here too for full fidelity even though no fixture/corpus value in
///   this codebase currently reaches it (avg_over_time's inputs are all
///   well within `f64` range) — this function must not go quietly wrong
///   the day one does.
fn eval_avg_over_time(samples: &[Sample]) -> f64 {
    debug_assert!(!samples.is_empty(), "caller already checked non-empty");
    let mut sum = samples[0].v;
    let mut mean = 0.0_f64;
    let mut kahan_c = 0.0_f64;
    let mut incremental_mean = false;
    let mut count = 1.0_f64;

    for (i, s) in samples[1..].iter().enumerate() {
        count = (i + 2) as f64;
        if !incremental_mean {
            let (new_sum, new_c) = kahan_inc(s.v, sum, kahan_c);
            if !new_sum.is_infinite() {
                sum = new_sum;
                kahan_c = new_c;
                continue;
            }
            // Switch to the incremental-mean recurrence, seeded from the
            // (pre-overflow) running sum's own mean so far.
            incremental_mean = true;
            mean = sum / (count - 1.0);
            kahan_c /= count - 1.0;
        }
        let q = (count - 1.0) / count;
        let (new_mean, new_c) = kahan_inc(s.v / count, q * mean, q * kahan_c);
        mean = new_mean;
        kahan_c = new_c;
    }

    if incremental_mean {
        mean + kahan_c
    } else {
        sum / count + kahan_c / count
    }
}

/// One classic-histogram bucket: `(le, cumulative_count)`. Grouping (by
/// every label except `le`) happens in [`crate::eval`]; this function
/// receives exactly one group's buckets.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    pub le: f64,
    pub count: f64,
}

/// `smallDeltaTolerance` (`quantile.go:45`) — the relative-delta threshold
/// below which a difference between successive cumulative bucket counts is
/// treated as a floating-point artifact (silently equalized, never
/// reported as forced monotonicity).
const SMALL_DELTA_TOLERANCE: f64 = 1e-12;

/// The smallest positive normal f64 — upstream `almost.minNormal`
/// (`util/almost/almost.go:22`, `math.Float64frombits(0x0010000000000000)`).
const MIN_NORMAL: f64 = f64::MIN_POSITIVE;

/// Upstream `almost.Equal` (`util/almost/almost.go`, pinned `40af9c2`),
/// ported for [`ensure_monotonic_and_ignore_small_deltas`]'s tolerance
/// check. The stale-NaN/NaN special cases are ported for fidelity even
/// though cumulative bucket counts are never NaN in practice.
fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    let a_stale = a.to_bits() == pulsus_model::STALE_NAN_BITS;
    let b_stale = b.to_bits() == pulsus_model::STALE_NAN_BITS;
    if a_stale || b_stale {
        return a_stale && b_stale;
    }
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < MIN_NORMAL {
        return diff < epsilon * MIN_NORMAL;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

/// `ensureMonotonicAndIgnoreSmallDeltas`'s non-`fixedPrecision` returns
/// (M7-A5b-i #124 review finding 2a): the forced-monotonicity flag plus
/// the bucket-bound/diff detail `NewHistogramQuantileForcedMonotonicityInfo`
/// renders (`histogramQuantileForcedMonotonicityErr.Error()`,
/// `annotations.go:333-341`). `min_bucket`/`max_bucket` start at
/// `+Inf`/`-Inf` (upstream's zero-value-via-`math.Inf` convention,
/// `quantile.go:678-679`) and only move when a genuine decrease is
/// clamped; meaningless (and never rendered) when `forced` is `false`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonotonicityReport {
    pub forced: bool,
    pub min_bucket: f64,
    pub max_bucket: f64,
    pub max_diff: f64,
}

impl MonotonicityReport {
    /// The "nothing forced" report every early-return path before
    /// `ensure_monotonic_and_ignore_small_deltas` runs uses (upstream:
    /// the zero value of `forcedMonotonic, minBucket, maxBucket, maxDiff`
    /// at those same early returns, `quantile.go:107-146`).
    const NONE: Self = Self {
        forced: false,
        min_bucket: 0.0,
        max_bucket: 0.0,
        max_diff: 0.0,
    };
}

/// Upstream `ensureMonotonicAndIgnoreSmallDeltas` (`quantile.go`, pinned
/// `40af9c2`), the count-mutating core: numerically-insignificant
/// differences between successive cumulative counts (relative delta below
/// `tolerance`, EITHER direction) are silently equalized to the previous
/// count; genuine decreases are clamped up and reported via
/// [`MonotonicityReport`] — the trigger for
/// `NewHistogramQuantileForcedMonotonicityInfo`. (The pin also returns
/// `fixedPrecision`; nothing in A5b-i's annotation text needs it, so it
/// is not ported.)
fn ensure_monotonic_and_ignore_small_deltas(
    buckets: &mut [Bucket],
    tolerance: f64,
) -> MonotonicityReport {
    let mut forced_monotonic = false;
    let mut min_bucket = f64::INFINITY;
    let mut max_bucket = f64::NEG_INFINITY;
    let mut max_diff = 0.0f64;
    let mut prev = buckets[0].count;
    for b in &mut buckets[1..] {
        let curr = b.count;
        if curr == prev {
            continue;
        }
        if almost_equal(prev, curr, tolerance) {
            // Silently correct numerically insignificant differences from
            // floating-point precision errors, regardless of direction.
            // `prev` is NOT updated (the difference is ignored).
            b.count = prev;
            continue;
        }
        if curr < prev {
            // Force monotonicity by removing any decreases regardless of
            // magnitude. `prev` is NOT updated (the decrease is ignored).
            b.count = prev;
            forced_monotonic = true;
            if b.le < min_bucket {
                min_bucket = b.le;
            }
            if b.le > max_bucket {
                max_bucket = b.le;
            }
            let diff = prev - curr;
            if diff > max_diff {
                max_diff = diff;
            }
            continue;
        }
        prev = curr;
    }
    MonotonicityReport {
        forced: forced_monotonic,
        min_bucket,
        max_bucket,
        max_diff,
    }
}

/// `histogram_quantile` — Prometheus's `bucketQuantile`, ported: sorts by
/// `le`, coalesces duplicate bounds, forces cumulative monotonicity
/// (independent scrapes can produce non-monotonic buckets), requires a
/// `+Inf` bucket, then linearly interpolates within the bucket the
/// requested quantile's rank falls into. The float-value verdicts are
/// [`histogram_quantile_with_monotonicity_report`]'s (all pre-M7 tests
/// unchanged); this thin wrapper drops the forced-monotonicity report for
/// callers that don't report the info annotation.
pub fn histogram_quantile(quantile: f64, buckets: Vec<Bucket>) -> Result<f64, PromqlError> {
    histogram_quantile_with_monotonicity_report(quantile, buckets).map(|(q, _report)| q)
}

/// [`histogram_quantile`] plus upstream `BucketQuantile`'s
/// `forcedMonotonic`/`minBucket`/`maxBucket`/`maxDiff` returns
/// (M7-A5b-i): the [`MonotonicityReport`] a genuine (beyond
/// `smallDeltaTolerance`) cumulative-count decrease produces — the
/// trigger for `NewHistogramQuantileForcedMonotonicityInfo`
/// (`funcHistogramQuantile`, `functions.go:2111-2117`).
pub fn histogram_quantile_with_monotonicity_report(
    quantile: f64,
    mut buckets: Vec<Bucket>,
) -> Result<(f64, MonotonicityReport), PromqlError> {
    if quantile.is_nan() {
        return Ok((f64::NAN, MonotonicityReport::NONE));
    }
    if quantile < 0.0 {
        return Ok((f64::NEG_INFINITY, MonotonicityReport::NONE));
    }
    if quantile > 1.0 {
        return Ok((f64::INFINITY, MonotonicityReport::NONE));
    }
    if buckets.is_empty() {
        return Err(PromqlError::HistogramBucket {
            detail: "no buckets in the series group".to_string(),
        });
    }

    buckets.sort_by(|a, b| a.le.partial_cmp(&b.le).unwrap_or(std::cmp::Ordering::Equal));

    // Upstream `coalesceBuckets` (M7-A5b-i, landed with the monotonicity
    // report): duplicate `le` bounds (two series whose `le` strings parse
    // to the same f64, e.g. "1" and "1.0") merge by adding counts —
    // BEFORE the monotonicity pass, so a duplicate can never masquerade
    // as a forced-monotonicity decrease.
    let mut buckets = coalesce_buckets(buckets);

    // Ported from Prometheus's own `bucketQuantile`: fewer than 2 buckets
    // (e.g. a lone `+Inf` bucket, no finite boundary to interpolate
    // against) cannot produce an interpolated quantile.
    if buckets.len() < 2 {
        return Ok((f64::NAN, MonotonicityReport::NONE));
    }

    // Force cumulative monotonicity (edge case 5): independent scrapes can
    // produce a bucket whose count is lower than a smaller-`le` bucket's;
    // clamp it up rather than silently produce a wrong quantile. Tolerance
    // + reporting per the pin (`ensureMonotonicAndIgnoreSmallDeltas`).
    let report = ensure_monotonic_and_ignore_small_deltas(&mut buckets, SMALL_DELTA_TOLERANCE);

    let last = *buckets.last().expect("checked non-empty above");
    if last.le.is_finite() {
        return Err(PromqlError::HistogramBucket {
            detail: "no +Inf bucket found".to_string(),
        });
    }

    let total = last.count;
    if total == 0.0 {
        return Ok((f64::NAN, report));
    }

    let rank = quantile * total;
    let b_idx = buckets
        .iter()
        .position(|b| b.count >= rank)
        .unwrap_or(buckets.len() - 1);

    if b_idx == buckets.len() - 1 {
        // The rank falls in the +Inf bucket itself — Prometheus reports
        // the previous (highest finite) bucket boundary rather than +Inf.
        return Ok((buckets[buckets.len() - 2].le, report));
    }
    if b_idx == 0 {
        return Ok((buckets[0].le.max(0.0), report));
    }

    let bucket_start = buckets[b_idx - 1].le.max(0.0);
    let bucket_end = buckets[b_idx].le;
    let count = buckets[b_idx].count - buckets[b_idx - 1].count;
    let rank_in_bucket = rank - buckets[b_idx - 1].count;
    if count <= 0.0 {
        return Ok((bucket_end, report));
    }
    Ok((
        bucket_start + (bucket_end - bucket_start) * (rank_in_bucket / count),
        report,
    ))
}

/// Merges buckets sharing the same `le` — mirrors upstream `coalesceBuckets`
/// (`quantile.go`). `buckets` must already be sorted by `le`.
fn coalesce_buckets(buckets: Vec<Bucket>) -> Vec<Bucket> {
    let mut out: Vec<Bucket> = Vec::with_capacity(buckets.len());
    for b in buckets {
        match out.last_mut() {
            Some(last) if last.le == b.le => last.count += b.count,
            _ => out.push(b),
        }
    }
    out
}

/// `interpolateLinearly` (`quantile.go`, `BucketFraction`'s inner closure):
/// the -Inf lower bound special case returns the bucket's own cumulative
/// count (no contribution beyond it — the same "skip the infinite-width
/// bucket" trick native `histogram_fraction`'s counterpart uses).
fn interpolate_bucket_linearly(
    lower_bound: f64,
    upper_bound: f64,
    count: f64,
    rank: f64,
    v: f64,
) -> f64 {
    if lower_bound == f64::NEG_INFINITY {
        count
    } else {
        rank + (count - rank) * (v - lower_bound) / (upper_bound - lower_bound)
    }
}

/// `histogram_fraction`'s classic-`le`-bucket counterpart — Prometheus's
/// `BucketFraction` (`quantile.go`), ported: the fraction of observations
/// between `lower` and `upper` over one group's classic (`_bucket`/`le`)
/// buckets. Mirrors [`histogram_quantile`]'s own sort/`+Inf`-requirement/
/// coalesce contract.
pub fn bucket_fraction(lower: f64, upper: f64, mut buckets: Vec<Bucket>) -> f64 {
    if buckets.is_empty() {
        return f64::NAN;
    }
    buckets.sort_by(|a, b| a.le.partial_cmp(&b.le).unwrap_or(std::cmp::Ordering::Equal));
    if !buckets
        .last()
        .expect("checked non-empty above")
        .le
        .is_infinite()
    {
        return f64::NAN;
    }
    let buckets = coalesce_buckets(buckets);

    let count = buckets.last().expect("checked non-empty above").count;
    if count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let mut rank = 0.0f64;
    let mut lower_rank = 0.0f64;
    let mut upper_rank = 0.0f64;
    let mut lower_set = false;
    let mut upper_set = false;
    let mut lower_bound = if buckets[0].le <= 0.0 {
        f64::NEG_INFINITY
    } else {
        0.0
    };

    for (i, b) in buckets.iter().enumerate() {
        if i > 0 {
            lower_bound = buckets[i - 1].le;
        }
        let upper_bound = b.le;

        if !lower_set && lower_bound >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && lower_bound >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && lower_bound < lower && upper_bound > lower {
            lower_rank =
                interpolate_bucket_linearly(lower_bound, upper_bound, b.count, rank, lower);
            lower_set = true;
        }
        if !upper_set && lower_bound < upper && upper_bound > upper {
            upper_rank =
                interpolate_bucket_linearly(lower_bound, upper_bound, b.count, rank, upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank = b.count;
    }
    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }
    (upper_rank - lower_rank) / count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(t_ms: i64, v: f64) -> Sample {
        Sample::float(t_ms, v)
    }

    // --- rate / increase / delta: edge case 2 (AC) ---

    #[test]
    fn rate_divides_increase_by_the_range_width_in_seconds() {
        // 2 samples exactly at the window edges: no extrapolation needed.
        let samples = vec![s(0, 0.0), s(60_000, 60.0)];
        let v = eval_range_fn(RangeFn::Rate, &samples, 60_000, 0, 60_000).unwrap();
        assert!((v - 1.0).abs() < 1e-9, "got {v}");
    }

    #[test]
    fn increase_does_not_divide_by_the_range_width() {
        let samples = vec![s(0, 0.0), s(60_000, 60.0)];
        let v = eval_range_fn(RangeFn::Increase, &samples, 60_000, 0, 60_000).unwrap();
        assert!((v - 60.0).abs() < 1e-9, "got {v}");
    }

    #[test]
    fn delta_does_not_apply_counter_reset_correction() {
        // A "drop" in a delta (gauge) series is a real negative delta, not
        // a reset to correct for.
        let samples = vec![s(0, 10.0), s(60_000, 4.0)];
        let v = eval_range_fn(RangeFn::Delta, &samples, 60_000, 0, 60_000).unwrap();
        assert!((v - (-6.0)).abs() < 1e-9, "got {v}");
    }

    #[test]
    fn a_mid_window_counter_reset_adds_back_the_pre_drop_value() {
        // Counter goes 0 -> 100 -> 10 (reset) -> 40 over a 3-minute window
        // sampled every minute. Corrected total increase = 100 + (40-0) =
        // 140 (the drop from 100 to 10 adds the pre-drop value 100 back).
        let samples = vec![
            s(0, 0.0),
            s(60_000, 100.0),
            s(120_000, 10.0),
            s(180_000, 40.0),
        ];
        let v = eval_range_fn(RangeFn::Increase, &samples, 180_000, 0, 180_000).unwrap();
        assert!((v - 140.0).abs() < 1e-9, "got {v}");
    }

    #[test]
    fn a_sample_near_the_edge_extrapolates_further_than_a_sample_far_from_it() {
        // `delta` (not `rate`/`increase`) deliberately: `is_counter` is
        // false, so the counter-reset "assume the series started near
        // zero" duration_to_start shortcut (edge case 2's other branch)
        // never fires here, isolating exactly the near-vs-far-edge
        // extrapolation behavior this test targets.
        //
        // 4 evenly-spaced samples every 60s; a window whose nominal edges
        // are only 10s beyond the observed samples on both ends
        // (duration_to_start/end = 10s, well under the 1.1x * 60s = 66s
        // threshold) -> both edges fully extrapolate by their small gap.
        let samples = vec![
            s(30_000, 0.0),
            s(90_000, 10.0),
            s(150_000, 20.0),
            s(210_000, 30.0),
        ];
        let v = eval_range_fn(RangeFn::Delta, &samples, 200_000, 20_000, 220_000).unwrap();
        // sampled_interval = 180s, extrapolate_to = 180 + 10 + 10 = 200s
        // raw delta = 30, scale = 200/180 -> 100/3.
        assert!((v - 100.0 / 3.0).abs() < 1e-6, "got {v}");
    }

    #[test]
    fn a_sample_far_from_the_edge_extrapolates_only_half_the_average_interval() {
        // Same series and same `delta` rationale as the "near the edge"
        // case above, but a much wider nominal window (duration_to_start/
        // end now far exceed the 1.1x average-interval threshold on both
        // sides), so extrapolation is capped at half the average interval
        // per edge instead of the full observed gap — a materially
        // different result from the "near the edge" case (100/3 there vs.
        // 40 here), demonstrating the AC's near-vs-far distinction.
        let samples = vec![
            s(30_000, 0.0),
            s(90_000, 10.0),
            s(150_000, 20.0),
            s(210_000, 30.0),
        ];
        // range_start = -1_000_000 (duration_to_start huge), range_end =
        // 1_000_000 (duration_to_end huge): average interval = 60s,
        // extrapolate_to = 180 + 30 + 30 = 240s (half-interval each side).
        let v = eval_range_fn(RangeFn::Delta, &samples, 2_000_000, -1_000_000, 1_000_000).unwrap();
        assert!((v - 40.0).abs() < 1e-6, "got {v}");
    }

    #[test]
    fn a_two_sample_series_still_extrapolates() {
        let samples = vec![s(10_000, 5.0), s(50_000, 25.0)];
        let v = eval_range_fn(RangeFn::Increase, &samples, 60_000, 0, 60_000).unwrap();
        // sampled_interval = 40s, avg interval = 40s, threshold = 44s.
        // duration_to_start = 10s (< threshold) -> full extrapolation;
        // duration_to_end = 10s (< threshold) -> full extrapolation.
        // extrapolate_to = 40 + 10 + 10 = 60s; raw increase = 20; scale =
        // 60/40 -> 30.
        assert!((v - 30.0).abs() < 1e-9, "got {v}");
    }

    #[test]
    fn fewer_than_two_samples_yields_no_result() {
        assert_eq!(
            eval_range_fn(RangeFn::Rate, &[s(0, 1.0)], 60_000, 0, 60_000),
            None
        );
        assert_eq!(eval_range_fn(RangeFn::Rate, &[], 60_000, 0, 60_000), None);
    }

    /// Issue #39 hand-derived golden: the exact #33 differential-harness
    /// repro (`target/e2e-artifacts/metrics-diff/single/mismatch-
    /// 1784061239969673907.json`, not committed — gitignored under
    /// `target/`), reconstructed from the corpus generator's own
    /// deterministic algorithm (`e2e/src/corpus.rs`'s `splitmix64`/`mix`/
    /// `counter_increment`, `seed=424242` from
    /// `test/fixtures/metrics/differential.json`, `service_idx=1`
    /// ("svc-1"), `instance_idx=0` ("inst-000"), no counter reset on this
    /// particular series — `flat=68`, `68 % COUNTER_RESET_MODULUS(5) ==
    /// 3`) — not re-derived from first principles, so this test's raw
    /// sample values are independently verifiable against that generator.
    ///
    /// Query: `rate(requests_total{...}[2m])`, instant time
    /// `1784061189208` ms (the corpus's own last sample). Real Prometheus
    /// v3.13.0 (same pinned image `#32`'s goldens use) reported
    /// `134.55238095238093` for this exact input; this engine reported
    /// `134.55238095238096` (bit `...1b` vs. `...1a` in the low mantissa
    /// byte) before the `eval_extrapolated` operation-order fix above.
    /// Asserts the exact bit pattern (`to_bits`), not an epsilon — an
    /// epsilon comparison would have silently passed on the very bug this
    /// golden exists to catch.
    #[test]
    fn issue_39_rate_extrapolation_matches_prometheus_bit_exactly() {
        // 8 raw `requests_total` samples inside the `(range_start,
        // range_end]` window (step 15s, samples at ts_idx 32..=39 of the
        // corpus's 40; every value below matches `counter_value(seed=
        // 424242, service_idx=1, instance_idx=0, ts_idx)` computed
        // independently in Python against the corpus module's own
        // algorithm during this fix's investigation).
        let samples = vec![
            s(1_784_061_084_208, 66_846.0),
            s(1_784_061_099_208, 68_855.0),
            s(1_784_061_114_208, 70_858.0),
            s(1_784_061_129_208, 72_866.0),
            s(1_784_061_144_208, 74_905.0),
            s(1_784_061_159_208, 76_939.0),
            s(1_784_061_174_208, 78_951.0),
            s(1_784_061_189_208, 80_974.0),
        ];
        let range_ms = 120_000; // `[2m]`
        let range_end_ms = 1_784_061_189_208; // the corpus's last sample ts.
        let range_start_ms = range_end_ms - range_ms;

        let v = eval_range_fn(
            RangeFn::Rate,
            &samples,
            range_ms,
            range_start_ms,
            range_end_ms,
        )
        .expect("8 samples in window");

        let expected = 134.552_380_952_380_93_f64;
        assert_eq!(
            v.to_bits(),
            expected.to_bits(),
            "got {v:?} (bits {:x}), want {expected:?} (bits {:x}) — real Prometheus's own \
             reported value for this exact input",
            v.to_bits(),
            expected.to_bits()
        );
    }

    // --- irate ---

    #[test]
    fn irate_uses_only_the_last_two_samples() {
        let samples = vec![s(0, 0.0), s(60_000, 100.0), s(120_000, 130.0)];
        let v = eval_range_fn(RangeFn::Irate, &samples, 120_000, 0, 120_000).unwrap();
        // (130 - 100) / 60s = 0.5/s
        assert!((v - 0.5).abs() < 1e-9, "got {v}");
    }

    #[test]
    fn irate_treats_a_drop_as_a_reset_using_the_last_value() {
        let samples = vec![s(0, 100.0), s(60_000, 10.0)];
        let v = eval_range_fn(RangeFn::Irate, &samples, 60_000, 0, 60_000).unwrap();
        assert!((v - (10.0 / 60.0)).abs() < 1e-9, "got {v}");
    }

    // --- over_time ---

    #[test]
    fn sum_over_time_uses_kahan_summation() {
        let samples = vec![s(0, 1e100), s(60_000, 1.0), s(120_000, -1e100)];
        let v = eval_over_time(OverTimeFn::Sum, &samples).unwrap();
        assert_eq!(v, 1.0);
    }

    #[test]
    fn avg_over_time_divides_by_the_sample_count() {
        let samples = vec![s(0, 2.0), s(60_000, 4.0)];
        assert_eq!(eval_over_time(OverTimeFn::Avg, &samples), Some(3.0));
    }

    /// Issue #39 audit finding: `avg_over_time` is genuinely a different
    /// accumulation from `sum_over_time(...) / count(...)`, not just a
    /// differently-rounded path to the same formula — pinned here with a
    /// case where the two approaches provably diverge at the last bit
    /// (found by randomized search against a Python replica of both
    /// algorithms during this fix's investigation, then hand-verified).
    /// Bit-exact (`to_bits`), not an epsilon comparison, for the same
    /// reason as the rate-family golden above.
    #[test]
    fn avg_over_time_matches_upstreams_accumulation_not_sum_over_time_divided_by_count() {
        let values = [
            577_446.702_271,
            -812_280.826_452,
            -943_305.046_956,
            671_530.207_84,
            -134_465.864_19,
            524_560.164_916,
            -995_787.893_298,
        ];
        let samples: Vec<Sample> = values
            .iter()
            .enumerate()
            .map(|(i, &v)| s(i as i64 * 60_000, v))
            .collect();

        let avg = eval_over_time(OverTimeFn::Avg, &samples).unwrap();

        let mut k = KahanSum::new();
        for v in values {
            k.add(v);
        }
        let naive_sum_then_divide = k.value() / values.len() as f64;

        assert_ne!(
            avg.to_bits(),
            naive_sum_then_divide.to_bits(),
            "this input was specifically chosen because the two accumulations diverge — if \
             they now match, either the input no longer exercises the difference or \
             avg_over_time regressed to the naive shape"
        );
        let expected: f64 = -158_900.365_124_142_85;
        assert_eq!(avg.to_bits(), expected.to_bits(), "got {avg:?}");
    }

    /// Issue #39: `avg_over_time`'s upstream incremental-mean overflow
    /// fallback must produce a finite, sane result rather than `NaN`/`Inf`
    /// garbage once the running sum overflows `f64::MAX`.
    #[test]
    fn avg_over_time_falls_back_to_incremental_mean_on_overflow() {
        let samples = vec![
            s(0, f64::MAX),
            s(60_000, f64::MAX),
            s(120_000, 1.0),
            s(180_000, 2.0),
        ];
        let avg = eval_over_time(OverTimeFn::Avg, &samples).unwrap();
        assert!(avg.is_finite(), "got {avg:?}");
        // Roughly `f64::MAX / 2` (the two `f64::MAX` terms dominate) —
        // sanity, not bit-exact (this path exists to avoid NaN/Inf, not to
        // be pinned to a captured Prometheus value).
        assert!(avg > 1e307, "got {avg:?}");
    }

    #[test]
    fn min_max_count_over_time() {
        let samples = vec![s(0, 3.0), s(60_000, 1.0), s(120_000, 2.0)];
        assert_eq!(eval_over_time(OverTimeFn::Min, &samples), Some(1.0));
        assert_eq!(eval_over_time(OverTimeFn::Max, &samples), Some(3.0));
        assert_eq!(eval_over_time(OverTimeFn::Count, &samples), Some(3.0));
    }

    /// Issue #39: an all-`NaN` window must yield `NaN` (upstream's
    /// `compareOverTime` seeds from `Floats[0]` and its `|| IsNaN(extremum)`
    /// disjunct keeps replacing while the accumulator is `NaN`, so it never
    /// escapes `NaN`) — not `+Inf`, which is what the old `f64::min` fold
    /// returned (`f64::min` ignores `NaN` operands entirely).
    #[test]
    fn min_over_time_all_nan_window_is_nan() {
        let two = vec![s(0, f64::NAN), s(60_000, f64::NAN)];
        let three = vec![s(0, f64::NAN), s(60_000, f64::NAN), s(120_000, f64::NAN)];
        for samples in [two, three] {
            let v = eval_over_time(OverTimeFn::Min, &samples).unwrap();
            assert!(v.is_nan(), "got {v:?}");
        }
    }

    /// Issue #39: same all-`NaN` case for `max_over_time` (old
    /// `f64::max`-fold returned `-Inf`).
    #[test]
    fn max_over_time_all_nan_window_is_nan() {
        let two = vec![s(0, f64::NAN), s(60_000, f64::NAN)];
        let three = vec![s(0, f64::NAN), s(60_000, f64::NAN), s(120_000, f64::NAN)];
        for samples in [two, three] {
            let v = eval_over_time(OverTimeFn::Max, &samples).unwrap();
            assert!(v.is_nan(), "got {v:?}");
        }
    }

    /// Issue #39: upstream's strict `<` never fires on an exact tie, so
    /// `-0 == +0` never replaces and the first sample wins — `min([+0,
    /// -0]) == +0`. The old `f64::min` fold returned `-0` here.
    /// `to_bits()` (not `==`, which treats `+0.0 == -0.0`) is required to
    /// observe the sign.
    #[test]
    fn min_over_time_signed_zero_tie_keeps_first() {
        let samples = vec![s(0, 0.0_f64), s(60_000, -0.0_f64)];
        let v = eval_over_time(OverTimeFn::Min, &samples).unwrap();
        assert_eq!(
            v.to_bits(),
            0.0_f64.to_bits(),
            "got {v:?} (bits {:x})",
            v.to_bits()
        );
    }

    /// Issue #39: same tie rule for `max_over_time` — `max([-0, +0]) ==
    /// -0`, first sample wins. The old `f64::max` fold returned `+0` here.
    #[test]
    fn max_over_time_signed_zero_tie_keeps_first() {
        let samples = vec![s(0, -0.0_f64), s(60_000, 0.0_f64)];
        let v = eval_over_time(OverTimeFn::Max, &samples).unwrap();
        assert_eq!(
            v.to_bits(),
            (-0.0_f64).to_bits(),
            "got {v:?} (bits {:x})",
            v.to_bits()
        );
    }

    /// Issue #39 regression guard (passes on both old and new code): an
    /// interior `NaN` alongside finite samples must not poison the result —
    /// the `is_nan()` disjunct only keeps replacing while the *accumulator*
    /// is `NaN`, so once a finite sample has been seen it sticks.
    #[test]
    fn min_max_over_time_skip_interior_nan() {
        let samples = vec![s(0, 5.0), s(60_000, f64::NAN), s(120_000, 3.0)];
        assert_eq!(eval_over_time(OverTimeFn::Min, &samples), Some(3.0));
        assert_eq!(eval_over_time(OverTimeFn::Max, &samples), Some(5.0));
    }

    /// Issue #39: a *leading* `NaN` seed must be replaced by the first finite
    /// sample and not poison the window — this exercises the accumulator-`NaN`
    /// disjunct on the seed itself (`compareOverTime` seeds from `samples[0]`,
    /// so a `NaN` seed relies on `|| extremum.is_nan()` to let a later finite
    /// value win). Distinct from the interior-`NaN` guard above.
    #[test]
    fn min_max_over_time_leading_nan_is_replaced_by_first_finite() {
        let samples = vec![s(0, f64::NAN), s(60_000, 5.0), s(120_000, 3.0)];
        assert_eq!(eval_over_time(OverTimeFn::Min, &samples), Some(3.0));
        assert_eq!(eval_over_time(OverTimeFn::Max, &samples), Some(5.0));
    }

    #[test]
    fn an_empty_window_is_absent_not_zero() {
        assert_eq!(eval_over_time(OverTimeFn::Sum, &[]), None);
        assert_eq!(eval_over_time(OverTimeFn::Count, &[]), None);
    }

    // --- histogram_quantile: edge case 5 (AC) ---

    fn buckets(pairs: &[(f64, f64)]) -> Vec<Bucket> {
        pairs
            .iter()
            .map(|&(le, count)| Bucket { le, count })
            .collect()
    }

    #[test]
    fn histogram_quantile_basic_interpolation() {
        // Classic textbook example: buckets 0.1/0.2/0.5/1/+Inf with counts
        // 1/2/5/10/10 (cumulative). p50 falls in the (0.2, 0.5] bucket.
        let bs = buckets(&[
            (0.1, 1.0),
            (0.2, 2.0),
            (0.5, 5.0),
            (1.0, 10.0),
            (f64::INFINITY, 10.0),
        ]);
        let q = histogram_quantile(0.5, bs).unwrap();
        // rank = 5.0 -> exactly at the (0.5, 5.0) bucket boundary count;
        // b_idx finds the bucket where count >= rank, i.e. index 2 (le=0.5,
        // count=5.0). bucket_start = buckets[1].le = 0.2, count in bucket =
        // 5-2=3, rank_in_bucket = 5-2=3 -> interpolated = 0.2 + (0.5-0.2)*
        // (3/3) = 0.5.
        assert!((q - 0.5).abs() < 1e-9, "got {q}");
    }

    #[test]
    fn histogram_quantile_p90_interpolates_within_the_last_finite_bucket() {
        let bs = buckets(&[
            (0.1, 1.0),
            (0.2, 2.0),
            (0.5, 5.0),
            (1.0, 10.0),
            (f64::INFINITY, 10.0),
        ]);
        let q = histogram_quantile(0.9, bs).unwrap();
        // rank = 9.0 -> falls in bucket le=1.0 (count 10 >= 9). bucket_start
        // = 0.5, count = 10-5=5, rank_in_bucket = 9-5=4 -> 0.5 + 0.5*(4/5)
        // = 0.9.
        assert!((q - 0.9).abs() < 1e-9, "got {q}");
    }

    #[test]
    fn histogram_quantile_forces_monotonicity_before_interpolating() {
        // A non-monotonic bucket (le=0.5 count dips below le=0.2's count)
        // must be clamped up, not used as-is.
        let bs = buckets(&[
            (0.1, 1.0),
            (0.2, 5.0),
            (0.5, 3.0),
            (1.0, 10.0),
            (f64::INFINITY, 10.0),
        ]);
        let q = histogram_quantile(0.5, bs).unwrap();
        // After forcing monotonicity: le=0.5 count becomes 5 (clamped up
        // from 3). rank = 5.0 falls exactly at le=0.2 or le=0.5 (both count
        // 5) -> b_idx finds the first with count >= rank, index 1 (le=0.2).
        // bucket_start = buckets[0].le = 0.1, count = 5-1=4, rank_in_bucket
        // = 5-1=4 -> 0.1 + 0.1*(4/4) = 0.2.
        assert!((q - 0.2).abs() < 1e-9, "got {q}");
    }

    #[test]
    fn histogram_quantile_missing_inf_bucket_is_an_error() {
        let bs = buckets(&[(0.1, 1.0), (0.5, 5.0)]);
        let err = histogram_quantile(0.5, bs).unwrap_err();
        assert!(matches!(err, PromqlError::HistogramBucket { .. }));
    }

    #[test]
    fn histogram_quantile_single_bucket_is_nan_no_finite_boundary_to_interpolate() {
        // Only one bucket (+Inf itself, ported straight from Prometheus's
        // own `len(buckets) < 2` guard): no finite boundary exists to
        // interpolate against, so the result is NaN — never a fabricated
        // wrong quantile.
        let bs = buckets(&[(f64::INFINITY, 10.0)]);
        let q = histogram_quantile(0.5, bs).unwrap();
        assert!(q.is_nan());
    }

    #[test]
    fn histogram_quantile_of_zero_total_observations_is_nan() {
        let bs = buckets(&[(0.1, 0.0), (f64::INFINITY, 0.0)]);
        let q = histogram_quantile(0.5, bs).unwrap();
        assert!(q.is_nan());
    }

    #[test]
    fn histogram_quantile_clamps_out_of_range_quantiles() {
        let bs = buckets(&[(0.1, 1.0), (f64::INFINITY, 1.0)]);
        assert_eq!(
            histogram_quantile(-1.0, bs.clone()).unwrap(),
            f64::NEG_INFINITY
        );
        assert_eq!(histogram_quantile(1.5, bs).unwrap(), f64::INFINITY);
    }

    // --- ensure_monotonic_and_ignore_small_deltas (M7-A5b-i: the
    //     forced-monotonicity report + smallDeltaTolerance port) ---

    #[test]
    fn a_genuine_decrease_is_clamped_and_reported_as_forced() {
        let mut bs = buckets(&[(0.1, 5.0), (0.5, 3.0), (f64::INFINITY, 10.0)]);
        let report = ensure_monotonic_and_ignore_small_deltas(&mut bs, SMALL_DELTA_TOLERANCE);
        assert!(report.forced);
        assert_eq!(bs[1].count, 5.0, "the decrease is clamped up to prev");
        // The clamped bucket (le=0.5) is both the min and max forced bound
        // for a single decrease; the diff is the clamped-away delta (2.0).
        assert_eq!(report.min_bucket, 0.5);
        assert_eq!(report.max_bucket, 0.5);
        assert_eq!(report.max_diff, 2.0);
        // The clamped quantile verdict is unchanged from the pre-A5b port.
        let (q, report) = histogram_quantile_with_monotonicity_report(
            0.5,
            buckets(&[(0.1, 5.0), (0.5, 3.0), (f64::INFINITY, 10.0)]),
        )
        .unwrap();
        assert!(report.forced);
        assert!(q.is_finite());
    }

    #[test]
    fn a_small_delta_below_tolerance_is_equalized_but_never_reported_as_forced() {
        // A relative delta of ~1e-16 (far below 1e-12) in EITHER direction
        // is a floating-point artifact: equalized to prev, no forced flag
        // (upstream `fixedPrecision`, quantile.go).
        let big = 1e15;
        let tiny_down = big - 0.1; // relative delta ~1e-16
        let mut bs = buckets(&[(0.1, big), (0.5, tiny_down), (f64::INFINITY, big)]);
        let report = ensure_monotonic_and_ignore_small_deltas(&mut bs, SMALL_DELTA_TOLERANCE);
        assert!(!report.forced, "a tolerance-level decrease is not forced");
        assert_eq!(bs[1].count, big, "equalized to the previous count");

        let tiny_up = big + 0.1;
        let mut bs = buckets(&[(0.1, big), (0.5, tiny_up), (f64::INFINITY, tiny_up)]);
        let report = ensure_monotonic_and_ignore_small_deltas(&mut bs, SMALL_DELTA_TOLERANCE);
        assert!(!report.forced);
        assert_eq!(
            bs[1].count, big,
            "a tolerance-level INCREASE is also equalized (upstream: 'regardless of direction')"
        );
    }

    #[test]
    fn a_monotone_input_is_untouched_and_unreported() {
        let mut bs = buckets(&[(0.1, 1.0), (0.5, 5.0), (f64::INFINITY, 10.0)]);
        let report = ensure_monotonic_and_ignore_small_deltas(&mut bs, SMALL_DELTA_TOLERANCE);
        assert!(!report.forced);
        assert_eq!(bs[1].count, 5.0);
    }

    #[test]
    fn almost_equal_matches_the_pinned_semantics() {
        // Exact equality and the zero/subnormal branch.
        assert!(almost_equal(1.0, 1.0, 1e-12));
        assert!(almost_equal(0.0, 0.0, 1e-12));
        assert!(!almost_equal(0.0, 1e-300, 1e-12)); // diff >= eps*minNormal
        // Relative branch.
        assert!(almost_equal(1e15, 1e15 - 0.1, 1e-12));
        assert!(!almost_equal(1.0, 2.0, 1e-12));
        // NaN pairs equal; stale-NaN only equals stale-NaN.
        assert!(almost_equal(f64::NAN, f64::NAN, 1e-12));
        let stale = f64::from_bits(pulsus_model::STALE_NAN_BITS);
        assert!(almost_equal(stale, stale, 1e-12));
        assert!(!almost_equal(stale, f64::NAN, 1e-12));
    }

    #[test]
    fn histogram_quantile_coalesces_duplicate_le_bounds_before_monotonicity() {
        // Two buckets at le=1.0 (counts 3 and 2) coalesce to one (count 5)
        // BEFORE the monotonicity pass — never reported as forced.
        let bs = vec![
            Bucket {
                le: 1.0,
                count: 3.0,
            },
            Bucket {
                le: 1.0,
                count: 2.0,
            },
            Bucket {
                le: f64::INFINITY,
                count: 5.0,
            },
        ];
        let (q, report) = histogram_quantile_with_monotonicity_report(0.5, bs).unwrap();
        assert!(!report.forced, "a coalesced duplicate is not a decrease");
        assert!(q.is_finite());
    }

    // --- bucket_fraction: classic-le histogram_fraction counterpart ---

    #[test]
    fn bucket_fraction_empty_is_nan() {
        assert!(bucket_fraction(0.0, 1.0, Vec::new()).is_nan());
    }

    #[test]
    fn bucket_fraction_missing_inf_bucket_is_nan() {
        let bs = buckets(&[(0.1, 1.0), (0.5, 5.0)]);
        assert!(bucket_fraction(0.0, 1.0, bs).is_nan());
    }

    #[test]
    fn bucket_fraction_lower_ge_upper_is_zero() {
        let bs = buckets(&[(1.0, 5.0), (f64::INFINITY, 10.0)]);
        assert_eq!(bucket_fraction(1.0, 1.0, bs.clone()), 0.0);
        assert_eq!(bucket_fraction(2.0, 1.0, bs), 0.0);
    }

    #[test]
    fn bucket_fraction_full_range_is_one() {
        let bs = buckets(&[(1.0, 5.0), (f64::INFINITY, 10.0)]);
        let f = bucket_fraction(f64::NEG_INFINITY, f64::INFINITY, bs);
        assert!((f - 1.0).abs() < 1e-12, "got {f}");
    }

    #[test]
    fn bucket_fraction_is_inverse_of_histogram_quantile_at_the_same_point() {
        let bs = buckets(&[(1.0, 5.0), (2.0, 8.0), (f64::INFINITY, 10.0)]);
        let q = histogram_quantile(0.5, bs.clone()).unwrap();
        let f = bucket_fraction(f64::NEG_INFINITY, q, bs);
        assert!((f - 0.5).abs() < 1e-9, "quantile={q} fraction={f}");
    }

    #[test]
    fn bucket_fraction_coalesces_duplicate_upper_bounds() {
        // Two buckets sharing le=1.0 (independent scrapes / a spurious
        // duplicate) coalesce to one before ranking.
        let bs = vec![
            Bucket {
                le: 1.0,
                count: 3.0,
            },
            Bucket {
                le: 1.0,
                count: 2.0,
            },
            Bucket {
                le: f64::INFINITY,
                count: 5.0,
            },
        ];
        // All 5 observations are at/below 1.0 (coalesced le=1.0 count=5),
        // so the full range [−Inf, +Inf) fraction is 1.0.
        let f = bucket_fraction(f64::NEG_INFINITY, f64::INFINITY, bs);
        assert!((f - 1.0).abs() < 1e-12, "got {f}");
    }

    // =======================================================================
    // Issue #67 (M6-04) hand-derived goldens — AC6. `to_bits` where the
    // computation is IEEE-exact or pinned to an independently-computed
    // (Python IEEE-754 double replica of the upstream operation order)
    // reference; 1e-9 tolerance where the value is hand-derived
    // arithmetically.
    // =======================================================================

    // --- linear_regression / deriv / predict_linear ---

    /// The constY short-circuit: a constant finite series has slope
    /// exactly `0` and intercept exactly the constant — `(0, y)` before
    /// any division (upstream linearRegression).
    #[test]
    fn deriv_and_predict_linear_of_a_constant_series_short_circuit_to_zero_slope() {
        let samples = vec![s(0, 5.0), s(60_000, 5.0), s(120_000, 5.0)];
        assert_eq!(eval_over_time(OverTimeFn::Deriv, &samples), Some(0.0));
        // predict = 0 * t + 5 exactly, regardless of duration/intercept time.
        assert_eq!(
            eval_over_time_param(OverTimeParamFn::PredictLinear, &samples, &[600.0], 120_000)
                .unwrap(),
            Some(5.0)
        );
    }

    /// The constY short-circuit's `±Inf` branch: a constant-infinite
    /// series yields `(NaN, NaN)`.
    #[test]
    fn deriv_and_predict_linear_of_a_constant_infinite_series_are_nan() {
        for inf in [f64::INFINITY, f64::NEG_INFINITY] {
            let samples = vec![s(0, inf), s(60_000, inf), s(120_000, inf)];
            let d = eval_over_time(OverTimeFn::Deriv, &samples).unwrap();
            assert!(d.is_nan(), "constant {inf} deriv must be NaN, got {d}");
            let p =
                eval_over_time_param(OverTimeParamFn::PredictLinear, &samples, &[600.0], 120_000)
                    .unwrap()
                    .unwrap();
            assert!(p.is_nan(), "constant {inf} predict must be NaN, got {p}");
        }
    }

    /// The upstream worked example (`functions.test`'s own comment block,
    /// derivable by hand: covXY = 105000, varX = 9900000): 11 samples at
    /// 0..3000s, values 0,10,20,30,40,0,10,20,30,40,50 — slope
    /// 105000/9900000. Bit-exact against the plain-accumulation reference.
    #[test]
    fn deriv_matches_the_upstream_worked_example_bit_exactly() {
        let vals = [
            0.0, 10.0, 20.0, 30.0, 40.0, 0.0, 10.0, 20.0, 30.0, 40.0, 50.0,
        ];
        let samples: Vec<Sample> = vals
            .iter()
            .enumerate()
            .map(|(i, &v)| s(i as i64 * 300_000, v))
            .collect();
        let d = eval_over_time(OverTimeFn::Deriv, &samples).unwrap();
        let expected = 0.010_606_060_606_060_607_f64;
        assert_eq!(d.to_bits(), expected.to_bits(), "got {d:?}");
    }

    /// The upstream `predict_linear(testcounter_reset_middle_total[50m],
    /// 3600)` case: 10 samples at 300..3000s (the left-open window drops
    /// t=0), intercept at the step time 3000s — hand-derivable to exactly
    /// 70 (covXY = 67500, varX = 7425000, slope*3600 + intercept = 70).
    /// Pins both the value and the intercept-at-step-time convention (an
    /// intercept at samples[0].t would give a different result).
    #[test]
    fn predict_linear_matches_the_upstream_worked_example_bit_exactly() {
        let vals = [10.0, 20.0, 30.0, 40.0, 0.0, 10.0, 20.0, 30.0, 40.0, 50.0];
        let samples: Vec<Sample> = vals
            .iter()
            .enumerate()
            .map(|(i, &v)| s((i as i64 + 1) * 300_000, v))
            .collect();
        let p = eval_over_time_param(
            OverTimeParamFn::PredictLinear,
            &samples,
            &[3600.0],
            3_000_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(p.to_bits(), 70.0_f64.to_bits(), "got {p:?}");
    }

    /// The intercept-timestamp convention isolated on a 2-sample series
    /// (exact dyadic arithmetic): samples (1s, 1), (2s, 3), slope 2.
    /// Intercept at the step time 2s ⇒ intercept 3 ⇒ predict(10s) = 23;
    /// anchoring at samples[0] (1s) would instead give 21 — pinned apart.
    #[test]
    fn predict_linear_anchors_the_intercept_at_the_step_time_not_the_first_sample() {
        let samples = vec![s(1_000, 1.0), s(2_000, 3.0)];
        let at_step =
            eval_over_time_param(OverTimeParamFn::PredictLinear, &samples, &[10.0], 2_000)
                .unwrap()
                .unwrap();
        assert_eq!(at_step, 23.0);
        let at_first_sample =
            eval_over_time_param(OverTimeParamFn::PredictLinear, &samples, &[10.0], 1_000)
                .unwrap()
                .unwrap();
        assert_eq!(
            at_first_sample, 21.0,
            "sanity: the two anchor conventions genuinely differ on this input"
        );
    }

    /// The plain-vs-Kahan discriminator (#67 review round-1 Δ1 test gap):
    /// an ill-conditioned series (values oscillating between ~1e9 and
    /// ~1e5 magnitudes with non-dyadic decimals) where upstream's plain
    /// `+=` accumulation and a Kahan-compensated one produce **different
    /// bits** for both slope and intercept. Pins the plain-accumulation
    /// bits (computed independently in Python replicating upstream's
    /// exact operation order); the Kahan-compensated slope on the same
    /// input is 0xc13b410ced203bcb — a regression to Kahan here flips
    /// this assert.
    #[test]
    fn linear_regression_uses_plain_accumulation_not_kahan_pinned_by_bits() {
        let vals = [
            1_000_465_370.550_9,
            710_276.683_7,
            999_540_489.682,
            -676_624.352_1,
            1_000_748_348.674_7,
            124_156.976_2,
            223_538.645_4,
        ];
        let samples: Vec<Sample> = vals
            .iter()
            .enumerate()
            .map(|(i, &v)| s(i as i64 * 60_000, v))
            .collect();
        // deriv = the regression slope anchored at samples[0].
        let slope = eval_over_time(OverTimeFn::Deriv, &samples).unwrap();
        assert_eq!(
            slope.to_bits(),
            0xc13b_410c_ed20_3bce,
            "plain-accumulation slope; Kahan would give 0xc13b410ced203bcb — got {slope:?}"
        );
        // predict_linear with duration 0 at intercept time 0 exposes the
        // intercept: slope * 0.0 is -0.0 here and x + -0.0 == x bit-exactly
        // for the non-zero intercept.
        let intercept = eval_over_time_param(OverTimeParamFn::PredictLinear, &samples, &[0.0], 0)
            .unwrap()
            .unwrap();
        assert_eq!(
            intercept.to_bits(),
            0x41c6_5bd8_f4da_c96a,
            "plain-accumulation intercept; Kahan would give 0x41c65bd8f4dac968 — got {intercept:?}"
        );
    }

    #[test]
    fn deriv_and_predict_linear_need_at_least_two_samples() {
        assert_eq!(eval_over_time(OverTimeFn::Deriv, &[s(0, 1.0)]), None);
        assert_eq!(
            eval_over_time_param(OverTimeParamFn::PredictLinear, &[s(0, 1.0)], &[60.0], 0).unwrap(),
            None
        );
    }

    // --- idelta ---

    #[test]
    fn idelta_is_the_last_two_samples_plain_difference() {
        // The upstream fixture: 0 50 100 50 -> idelta = -50 (no
        // counter-reset branch in non-rate mode, no per-second division).
        let samples = vec![
            s(0, 0.0),
            s(300_000, 50.0),
            s(600_000, 100.0),
            s(900_000, 50.0),
        ];
        let v = eval_over_time(OverTimeFn::Idelta, &samples).unwrap();
        assert_eq!(v.to_bits(), (-50.0_f64).to_bits());
    }

    #[test]
    fn idelta_with_fewer_than_two_samples_yields_no_result() {
        assert_eq!(eval_over_time(OverTimeFn::Idelta, &[]), None);
        assert_eq!(eval_over_time(OverTimeFn::Idelta, &[s(0, 1.0)]), None);
    }

    /// The shared `instantValue` zero-interval drop (#67 review round-1
    /// Δ2): equal final-two timestamps yield no sample even in non-rate
    /// mode — never a `v2 - v1` over a zero interval.
    #[test]
    fn idelta_with_equal_final_two_timestamps_yields_no_result() {
        let samples = vec![s(0, 1.0), s(60_000, 2.0), s(60_000, 5.0)];
        assert_eq!(eval_over_time(OverTimeFn::Idelta, &samples), None);
    }

    // --- resets / changes ---

    #[test]
    fn resets_counts_strict_drops_between_consecutive_samples() {
        // The upstream fixture rows ([50m] window): 1 2 3 0 1 0 0 1 2 0 ->
        // 3 resets; 1 2 3 4 5 1 2 3 4 5 -> 1; 0 0 0 0 0 1 1 1 1 1 -> 0.
        for (vals, want) in [
            (vec![1.0, 2.0, 3.0, 0.0, 1.0, 0.0, 0.0, 1.0, 2.0, 0.0], 3.0),
            (vec![1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 2.0, 3.0, 4.0, 5.0], 1.0),
            (vec![0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0], 0.0),
        ] {
            let samples: Vec<Sample> = vals
                .iter()
                .enumerate()
                .map(|(i, &v)| s(i as i64 * 300_000, v))
                .collect();
            assert_eq!(
                eval_over_time(OverTimeFn::Resets, &samples),
                Some(want),
                "{vals:?}"
            );
        }
    }

    #[test]
    fn resets_never_counts_nan_adjacent_pairs() {
        // IEEE `<` is false for any NaN operand, so 0 -> NaN -> 0 has no
        // reset; a single sample has 0 resets (not absent).
        let samples = vec![s(0, 0.0), s(300_000, f64::NAN), s(600_000, 0.0)];
        assert_eq!(eval_over_time(OverTimeFn::Resets, &samples), Some(0.0));
        assert_eq!(eval_over_time(OverTimeFn::Resets, &[s(0, 7.0)]), Some(0.0));
    }

    #[test]
    fn changes_counts_value_changes_including_one_sided_nan_transitions() {
        // The upstream NaN fixture: NaN NaN NaN -> 0 (NaN <-> NaN never
        // counts); 0 NaN 0 -> 2 (each one-sided NaN transition counts).
        let all_nan: Vec<Sample> = (0..3).map(|i| s(i * 300_000, f64::NAN)).collect();
        assert_eq!(eval_over_time(OverTimeFn::Changes, &all_nan), Some(0.0));
        let one_sided = vec![s(0, 0.0), s(300_000, f64::NAN), s(600_000, 0.0)];
        assert_eq!(eval_over_time(OverTimeFn::Changes, &one_sided), Some(2.0));
        // The counter fixture row, all 10 samples: 1 2 3 0 1 0 0 1 2 0 ->
        // 8 changes (the 0 -> 0 pair at indices 5,6 is the only
        // non-change). Upstream's `[50m]` figure of 7 is over the
        // left-open 9-sample window that drops the t=0 sample — the proof
        // corpus case pins that windowed form end-to-end.
        let counter: Vec<Sample> = [1.0, 2.0, 3.0, 0.0, 1.0, 0.0, 0.0, 1.0, 2.0, 0.0]
            .iter()
            .enumerate()
            .map(|(i, &v)| s(i as i64 * 300_000, v))
            .collect();
        assert_eq!(eval_over_time(OverTimeFn::Changes, &counter), Some(8.0));
        assert_eq!(eval_over_time(OverTimeFn::Changes, &[s(0, 7.0)]), Some(0.0));
    }

    // --- stddev_over_time / stdvar_over_time ---

    /// The upstream fixture `metric 0 8 8 2 3` -> stdvar 10.56 (mean 4.2,
    /// Σ(dev²) = 52.8, /5). Pinned bit-exactly to the Kahan-Welford
    /// reference value (independently computed with upstream's operation
    /// order); `10.559999999999999` is that exact double, one ULP below
    /// the decimal literal `10.56`'s nearest double.
    #[test]
    fn stdvar_and_stddev_over_time_match_the_kahan_welford_reference() {
        let samples: Vec<Sample> = [0.0, 8.0, 8.0, 2.0, 3.0]
            .iter()
            .enumerate()
            .map(|(i, &v)| s(i as i64 * 10_000, v))
            .collect();
        let var = eval_over_time(OverTimeFn::Stdvar, &samples).unwrap();
        assert_eq!(
            var.to_bits(),
            10.559_999_999_999_999_f64.to_bits(),
            "got {var:?}"
        );
        let dev = eval_over_time(OverTimeFn::Stddev, &samples).unwrap();
        assert_eq!(dev.to_bits(), var.sqrt().to_bits());
        assert!((dev - 3.249_615_361_854_384).abs() < 1e-9);
    }

    /// The upstream #4927 regression fixture: a constant series has
    /// exactly zero variance (Welford's `delta` is exactly 0 from the
    /// second sample on) — no catastrophic-cancellation drift.
    #[test]
    fn stdvar_over_time_of_a_constant_series_is_exactly_zero() {
        let samples: Vec<Sample> = (0..3)
            .map(|i| s(i * 10_000, 1.599_050_563_727_786_8))
            .collect();
        assert_eq!(eval_over_time(OverTimeFn::Stdvar, &samples), Some(0.0));
        assert_eq!(eval_over_time(OverTimeFn::Stddev, &samples), Some(0.0));
        // A single sample is variance 0, not absent.
        assert_eq!(eval_over_time(OverTimeFn::Stdvar, &[s(0, 9.0)]), Some(0.0));
    }

    // --- quantile_over_time / mad_over_time ---

    fn quantile_ot(phi: f64, vals: &[f64]) -> Option<f64> {
        let samples: Vec<Sample> = vals
            .iter()
            .enumerate()
            .map(|(i, &v)| s(i as i64 * 10_000, v))
            .collect();
        eval_over_time_param(OverTimeParamFn::Quantile, &samples, &[phi], 0)
            .expect("quantile_over_time never errors")
    }

    #[test]
    fn quantile_over_time_interpolates_including_even_cardinality() {
        // The upstream fixture (odd cardinality): 0 1 2 at φ=0.75 -> 1.5.
        assert_eq!(quantile_ot(0.75, &[0.0, 1.0, 2.0]), Some(1.5));
        // Even cardinality: 4 values, φ=0.5 -> rank 1.5, midway between
        // the 2nd and 3rd sorted values.
        assert_eq!(quantile_ot(0.5, &[1.0, 3.0, 2.0, 4.0]), Some(2.5));
        // Two samples 0 1 at φ=0.8 -> 0.8 (upstream fixture).
        let v = quantile_ot(0.8, &[0.0, 1.0]).unwrap();
        assert!((v - 0.8).abs() < 1e-12, "got {v}");
    }

    /// NaN samples sort FIRST (upstream vectorByValueHeap's Less), so
    /// they occupy the low ranks: [NaN, 0, 1] has median 0 and φ=0 -> NaN.
    #[test]
    fn quantile_over_time_sorts_nan_samples_first() {
        assert_eq!(quantile_ot(0.5, &[f64::NAN, 0.0, 1.0]), Some(0.0));
        assert_eq!(quantile_ot(0.5, &[0.0, f64::NAN, 1.0]), Some(0.0));
        let v = quantile_ot(0.0, &[0.0, f64::NAN, 1.0]).unwrap();
        assert!(v.is_nan(), "φ=0 selects the NaN rank, got {v}");
    }

    /// Out-of-range/NaN φ clamps — NEVER an error (#67 plan v2 Δ5, the
    /// histogram_quantile rule): φ<0 -> -Inf, φ>1 -> +Inf, NaN -> NaN.
    #[test]
    fn quantile_over_time_clamps_out_of_range_phi_without_error() {
        assert_eq!(quantile_ot(-1.0, &[0.0, 1.0]), Some(f64::NEG_INFINITY));
        assert_eq!(quantile_ot(2.0, &[0.0, 1.0]), Some(f64::INFINITY));
        let v = quantile_ot(f64::NAN, &[0.0, 1.0]).unwrap();
        assert!(v.is_nan(), "got {v}");
        // An empty window is absent regardless of φ (upstream returns
        // before ranging φ).
        assert_eq!(quantile_ot(2.0, &[]), None);
    }

    #[test]
    fn mad_over_time_is_the_median_absolute_deviation() {
        // The upstream fixture's [70s] window: 6 2 1 999 1 2 (even
        // cardinality) -> median 2, |dev| = 4 0 1 997 1 0 -> median 1.
        let samples: Vec<Sample> = [6.0, 2.0, 1.0, 999.0, 1.0, 2.0]
            .iter()
            .enumerate()
            .map(|(i, &v)| s(i as i64 * 10_000, v))
            .collect();
        assert_eq!(eval_over_time(OverTimeFn::Mad, &samples), Some(1.0));
    }

    // --- last / first / present / ts_of_* ---

    #[test]
    fn last_first_present_over_time_are_positional() {
        let samples = vec![s(0, 5.0), s(10_000, 7.0), s(20_000, 3.0)];
        assert_eq!(eval_over_time(OverTimeFn::Last, &samples), Some(3.0));
        assert_eq!(eval_over_time(OverTimeFn::First, &samples), Some(5.0));
        assert_eq!(eval_over_time(OverTimeFn::Present, &samples), Some(1.0));
        for func in [OverTimeFn::Last, OverTimeFn::First, OverTimeFn::Present] {
            assert_eq!(eval_over_time(func, &[]), None, "{func:?} of empty");
        }
    }

    #[test]
    fn ts_of_first_and_last_over_time_are_positional_timestamps_in_seconds() {
        let samples = vec![s(10_500, 5.0), s(20_500, 7.0), s(30_500, 3.0)];
        assert_eq!(eval_over_time(OverTimeFn::TsOfFirst, &samples), Some(10.5));
        assert_eq!(eval_over_time(OverTimeFn::TsOfLast, &samples), Some(30.5));
    }

    /// Repeated extrema: strict `<`/`>` keeps the FIRST occurrence (#67
    /// review round-1 Δ4).
    #[test]
    fn ts_of_min_max_over_time_keep_the_first_equal_extremum() {
        let samples = vec![s(0, 1.0), s(10_000, 3.0), s(20_000, 3.0), s(30_000, 1.0)];
        assert_eq!(eval_over_time(OverTimeFn::TsOfMax, &samples), Some(10.0));
        assert_eq!(eval_over_time(OverTimeFn::TsOfMin, &samples), Some(0.0));
    }

    /// A leading NaN is replaced by the first non-NaN sample (the
    /// `|| IsNaN(extremum)` disjunct), then ordinary comparison resumes.
    #[test]
    fn ts_of_min_max_over_time_replace_a_leading_nan() {
        let samples = vec![s(0, f64::NAN), s(10_000, 5.0), s(20_000, 2.0)];
        assert_eq!(eval_over_time(OverTimeFn::TsOfMax, &samples), Some(10.0));
        assert_eq!(eval_over_time(OverTimeFn::TsOfMin, &samples), Some(20.0));
    }

    /// An all-NaN window keeps replacing the (still-NaN) extremum, so the
    /// LAST sample's timestamp wins.
    #[test]
    fn ts_of_min_max_over_time_of_an_all_nan_window_select_the_last_sample() {
        let samples: Vec<Sample> = (0..3).map(|i| s(i * 10_000, f64::NAN)).collect();
        assert_eq!(eval_over_time(OverTimeFn::TsOfMax, &samples), Some(20.0));
        assert_eq!(eval_over_time(OverTimeFn::TsOfMin, &samples), Some(20.0));
    }

    // --- double_exponential_smoothing ---

    /// Hand-derived (exact dyadic arithmetic, sf = tf = 0.5, values
    /// 1,4,9,16,25): s1 walks 4 -> 8 -> 13.75 -> 21.6875. Bit-exact.
    #[test]
    fn double_exponential_smoothing_matches_the_hand_derived_recurrence() {
        let samples: Vec<Sample> = [1.0, 4.0, 9.0, 16.0, 25.0]
            .iter()
            .enumerate()
            .map(|(i, &v)| s(i as i64 * 10_000, v))
            .collect();
        let v = eval_over_time_param(
            OverTimeParamFn::DoubleExpSmoothing,
            &samples,
            &[0.5, 0.5],
            0,
        )
        .unwrap()
        .unwrap();
        assert_eq!(v.to_bits(), 21.6875_f64.to_bits(), "got {v:?}");
    }

    #[test]
    fn double_exponential_smoothing_needs_at_least_two_samples() {
        let one = [s(0, 1.0)];
        assert_eq!(
            eval_over_time_param(OverTimeParamFn::DoubleExpSmoothing, &one, &[0.5, 0.5], 0)
                .unwrap(),
            None
        );
    }

    /// Out-of-range factors are `InvalidParameter` errors naming the
    /// parameter and bounds — and validation runs BEFORE the sample-count
    /// check (upstream panics before its `l < 2` guard), so even an
    /// empty/short window rejects an invalid factor.
    #[test]
    fn double_exponential_smoothing_rejects_out_of_range_factors() {
        let samples = vec![s(0, 1.0), s(10_000, 2.0)];
        for bad_sf in [0.0, 1.0, 2.0, -0.5] {
            let err = eval_over_time_param(
                OverTimeParamFn::DoubleExpSmoothing,
                &samples,
                &[bad_sf, 0.5],
                0,
            )
            .unwrap_err();
            match err {
                PromqlError::InvalidParameter { detail } => assert!(
                    detail.contains("smoothing factor") && detail.contains("0 < sf < 1"),
                    "sf={bad_sf}: got {detail:?}"
                ),
                other => panic!("sf={bad_sf}: expected InvalidParameter, got {other:?}"),
            }
        }
        let err = eval_over_time_param(
            OverTimeParamFn::DoubleExpSmoothing,
            &samples,
            &[0.5, 1.5],
            0,
        )
        .unwrap_err();
        match err {
            PromqlError::InvalidParameter { detail } => assert!(
                detail.contains("trend factor") && detail.contains("0 < tf < 1"),
                "got {detail:?}"
            ),
            other => panic!("expected InvalidParameter, got {other:?}"),
        }
        // Validation-before-count: a single-sample window still errors.
        let err = eval_over_time_param(
            OverTimeParamFn::DoubleExpSmoothing,
            &[s(0, 1.0)],
            &[2.0, 0.5],
            0,
        )
        .unwrap_err();
        assert!(matches!(err, PromqlError::InvalidParameter { .. }));
    }
}
