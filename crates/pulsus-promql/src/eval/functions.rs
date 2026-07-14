//! Range-vector functions (`rate`/`irate`/`increase`/`delta`),
//! `*_over_time` aggregations, and `histogram_quantile` — ported from
//! Prometheus v3.13's `promql/functions.go` (`extrapolatedRate`,
//! `instantValue`) and `promql/quantile.go` (`bucketQuantile`), not
//! re-derived.

use crate::error::PromqlError;
use crate::math::{KahanSum, kahan_inc};
use crate::plan::{OverTimeFn, RangeFn};
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
    let last = samples[samples.len() - 1];
    let prev = samples[samples.len() - 2];
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
    let first = samples[0];
    let last = samples[samples.len() - 1];
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
/// `min`/`max`/`count_over_time` (issue #39 audit) carry no ULP risk at
/// all — upstream's `compareOverTime` (`funcMinOverTime`/
/// `funcMaxOverTime`) does nothing but direct `>`/`<` value comparisons
/// (no arithmetic, so no rounding to diverge on) and `funcCountOverTime`
/// is a plain length; both already match here bit-for-bit by construction.
/// (Their `NaN`-vs-leading-value tie-breaking rule does differ subtly from
/// this port's `f64::max`/`f64::min` fold — out of scope for #39, which is
/// specifically about floating-point *accumulation order*, not `NaN`
/// handling; flagged as a distinct follow-up.)
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
        OverTimeFn::Min => Some(samples.iter().map(|s| s.v).fold(f64::INFINITY, f64::min)),
        OverTimeFn::Max => Some(
            samples
                .iter()
                .map(|s| s.v)
                .fold(f64::NEG_INFINITY, f64::max),
        ),
    }
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

/// `histogram_quantile` — Prometheus's `bucketQuantile`, ported: sorts by
/// `le`, forces cumulative monotonicity (independent scrapes can produce
/// non-monotonic buckets), requires a `+Inf` bucket, then linearly
/// interpolates within the bucket the requested quantile's rank falls
/// into.
pub fn histogram_quantile(quantile: f64, mut buckets: Vec<Bucket>) -> Result<f64, PromqlError> {
    if quantile.is_nan() {
        return Ok(f64::NAN);
    }
    if quantile < 0.0 {
        return Ok(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Ok(f64::INFINITY);
    }
    if buckets.is_empty() {
        return Err(PromqlError::HistogramBucket {
            detail: "no buckets in the series group".to_string(),
        });
    }

    buckets.sort_by(|a, b| a.le.partial_cmp(&b.le).unwrap_or(std::cmp::Ordering::Equal));

    // Ported from Prometheus's own `bucketQuantile`: fewer than 2 buckets
    // (e.g. a lone `+Inf` bucket, no finite boundary to interpolate
    // against) cannot produce an interpolated quantile.
    if buckets.len() < 2 {
        return Ok(f64::NAN);
    }

    // Force cumulative monotonicity (edge case 5): independent scrapes can
    // produce a bucket whose count is lower than a smaller-`le` bucket's;
    // clamp it up rather than silently produce a wrong quantile.
    let mut max_count = f64::NEG_INFINITY;
    for b in &mut buckets {
        if b.count < max_count {
            b.count = max_count;
        } else {
            max_count = b.count;
        }
    }

    let last = *buckets.last().expect("checked non-empty above");
    if last.le.is_finite() {
        return Err(PromqlError::HistogramBucket {
            detail: "no +Inf bucket found".to_string(),
        });
    }

    let total = last.count;
    if total == 0.0 {
        return Ok(f64::NAN);
    }

    let rank = quantile * total;
    let b_idx = buckets
        .iter()
        .position(|b| b.count >= rank)
        .unwrap_or(buckets.len() - 1);

    if b_idx == buckets.len() - 1 {
        // The rank falls in the +Inf bucket itself — Prometheus reports
        // the previous (highest finite) bucket boundary rather than +Inf.
        return Ok(buckets[buckets.len() - 2].le);
    }
    if b_idx == 0 {
        return Ok(buckets[0].le.max(0.0));
    }

    let bucket_start = buckets[b_idx - 1].le.max(0.0);
    let bucket_end = buckets[b_idx].le;
    let count = buckets[b_idx].count - buckets[b_idx - 1].count;
    let rank_in_bucket = rank - buckets[b_idx - 1].count;
    if count <= 0.0 {
        return Ok(bucket_end);
    }
    Ok(bucket_start + (bucket_end - bucket_start) * (rank_in_bucket / count))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(t_ms: i64, v: f64) -> Sample {
        Sample { t_ms, v }
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
}
