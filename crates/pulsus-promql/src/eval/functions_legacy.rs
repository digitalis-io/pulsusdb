// Verbatim copies of the window functions at 9ba05f05, before issue #18
// part 3 moved them onto `window.rs`'s summaries. Included by
// `functions.rs`'s `legacy` test module and by
// `crates/pulsus-read/tests/live_metrics_window_algebra.rs`: each test
// compares the refactored functions with these, as bits. The includer
// brings `Sample` into scope. The bodies are verbatim; each item is made
// `pub` so the includer can call it. Nothing here is compiled outside a
// test.

/// `isStartTimestampReset` (`promql/functions.go:703-733`, v3.13.0
/// `40af9c2`), ported arm-for-arm: whether the current sample's start
/// timestamp marks a counter reset relative to the previous sample.
///
/// - `curr_st == 0` (unset) or `curr_st > curr_t` (clearly invalid) →
///   no reset (`:704-708`);
/// - `curr_st < prev_t` → no reset (`:710-712`);
/// - `curr_st > prev_t` → reset (`:714-716`);
/// - `curr_st == prev_t` (the OTel delta-vs-cumulative-with-unknown-start
///   disambiguation, `:718-732`): a previous start timestamp greater than
///   the previous sample timestamp is invalid — treated as unknown to
///   avoid a spurious reset (`:728-730`); otherwise a reset iff
///   `prev_st != 0` (`:732` — a KNOWN previous start means this is a
///   delta datapoint).
pub fn is_start_timestamp_reset(prev_st: i64, prev_t: i64, curr_st: i64, curr_t: i64) -> bool {
    if curr_st == 0 || curr_st > curr_t {
        return false;
    }
    if curr_st < prev_t {
        return false;
    }
    if curr_st > prev_t {
        return true;
    }
    if prev_st > prev_t {
        return false;
    }
    prev_st != 0
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
///
/// Issue #155: with a `Some` start-timestamp channel, the reset condition
/// becomes `ss[1].F < ss[0].F || isStartTimestampReset(ss[0].ST, ss[0].T,
/// ss[1].ST, ss[1].T)` (`functions.go:837` at the pin); `None` keeps the
/// value comparison alone (the pre-change path — upstream's ST fields are
/// zero-valued when the engine option is off, and `is_start_timestamp_
/// reset` with `curr_st == 0` is always false).
pub fn eval_irate(samples: &[Sample], start_ts: Option<&[i64]>) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let last = &samples[samples.len() - 1];
    let prev = &samples[samples.len() - 2];
    let interval_ms = last.t_ms - prev.t_ms;
    if interval_ms == 0 {
        return None;
    }
    let is_reset = match start_ts {
        None => last.v < prev.v,
        Some(st) => {
            last.v < prev.v
                || is_start_timestamp_reset(
                    st[samples.len() - 2],
                    prev.t_ms,
                    st[samples.len() - 1],
                    last.t_ms,
                )
        }
    };
    let mut result = if is_reset { last.v } else { last.v - prev.v };
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
pub fn eval_extrapolated(
    samples: &[Sample],
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
    is_counter: bool,
    is_rate: bool,
    start_ts: Option<&[i64]>,
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
    //
    // Issue #155: one branch per WINDOW on the ST channel — the `None`
    // arm is the pre-change loop verbatim; the `Some` arm mirrors the
    // pin's `:521` condition `currPoint.F < prevPoint.F || (i+1 <
    // len(startTimestamps) && isStartTimestampReset(startTimestamps[i],
    // prevPoint.T, startTimestamps[i+1], currPoint.T))`.
    if is_counter {
        match start_ts {
            None => {
                let mut last_value = 0.0_f64;
                for s in samples {
                    if s.v < last_value {
                        result_value += last_value;
                    }
                    last_value = s.v;
                }
            }
            Some(st) => {
                for i in 1..samples.len() {
                    let prev = &samples[i - 1];
                    let curr = &samples[i];
                    if curr.v < prev.v
                        || (i < st.len()
                            && is_start_timestamp_reset(st[i - 1], prev.t_ms, st[i], curr.t_ms))
                    {
                        result_value += prev.v;
                    }
                }
            }
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

/// `idelta` — upstream's shared `instantValue(vals, samples, isRate=false)`
/// rule set, mirroring [`eval_irate`] (v3.13.0 lines 829-880): `< 2`
/// samples → no sample, **equal last-two timestamps → no sample** (the
/// zero-interval drop applies to both rate and non-rate modes — #67 review
/// round-1 Δ2), otherwise the last two floats' plain difference (no
/// counter-reset branch and no per-second division in non-rate mode).
pub fn eval_idelta(samples: &[Sample]) -> Option<f64> {
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
pub fn eval_resets(samples: &[Sample]) -> f64 {
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
pub fn eval_changes(samples: &[Sample]) -> f64 {
    let mut changes = 0.0_f64;
    for w in samples.windows(2) {
        let (prev, cur) = (w[0].v, w[1].v);
        if cur != prev && !(cur.is_nan() && prev.is_nan()) {
            changes += 1.0;
        }
    }
    changes
}

/// Which extremum [`eval_extremum_over_time`] tracks (an enum, not a
/// boolean parameter, per house style).
#[derive(Debug, Clone, Copy)]
pub enum WhichExtremum {
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
pub fn eval_extremum_over_time(samples: &[Sample], which: WhichExtremum) -> f64 {
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
