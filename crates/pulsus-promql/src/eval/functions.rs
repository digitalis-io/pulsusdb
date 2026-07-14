//! Range-vector functions (`rate`/`irate`/`increase`/`delta`),
//! `*_over_time` aggregations, and `histogram_quantile` — ported from
//! Prometheus v3.13's `promql/functions.go` (`extrapolatedRate`,
//! `instantValue`) and `promql/quantile.go` (`bucketQuantile`), not
//! re-derived.

use crate::error::PromqlError;
use crate::math::KahanSum;
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

/// `rate`/`increase`/`delta` — Prometheus's `extrapolatedRate`, ported
/// verbatim (counter-reset correction when `is_counter`, then
/// 1.1x-average-interval edge extrapolation, then divide by `range_ms`
/// when `is_rate`). Requires at least 2 samples in the window (the
/// extrapolation heuristic needs at least one interval to average).
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
    let mut result_value = last.v - first.v;

    if is_counter {
        let mut last_value = 0.0_f64;
        for s in samples {
            if s.v < last_value {
                result_value += last_value;
            }
            last_value = s.v;
        }
    }

    let mut duration_to_start = (first.t_ms - range_start_ms) as f64 / 1000.0;
    let duration_to_end = (range_end_ms - last.t_ms) as f64 / 1000.0;
    let sampled_interval = (last.t_ms - first.t_ms) as f64 / 1000.0;
    let average_duration_between_samples = sampled_interval / (samples.len() - 1) as f64;

    if is_counter && result_value > 0.0 && first.v >= 0.0 {
        let duration_to_zero = sampled_interval * (first.v / result_value);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut extrapolate_to_interval = sampled_interval;

    if duration_to_start < extrapolation_threshold {
        extrapolate_to_interval += duration_to_start;
    } else {
        extrapolate_to_interval += average_duration_between_samples / 2.0;
    }
    if duration_to_end < extrapolation_threshold {
        extrapolate_to_interval += duration_to_end;
    } else {
        extrapolate_to_interval += average_duration_between_samples / 2.0;
    }
    result_value *= extrapolate_to_interval / sampled_interval;
    if is_rate {
        result_value /= range_ms as f64 / 1000.0;
    }
    Some(result_value)
}

/// `avg/min/max/sum/count_over_time`. `sum`/`avg` use [`KahanSum`],
/// matching Prometheus's own compensated-summation `*_over_time`
/// implementation. `None` for an empty window (series absent at this
/// step) — never a wrong `0`/`NaN` standing in for absence.
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
        OverTimeFn::Avg => {
            let mut k = KahanSum::new();
            for s in samples {
                k.add(s.v);
            }
            Some(k.value() / samples.len() as f64)
        }
        OverTimeFn::Min => Some(samples.iter().map(|s| s.v).fold(f64::INFINITY, f64::min)),
        OverTimeFn::Max => Some(
            samples
                .iter()
                .map(|s| s.v)
                .fold(f64::NEG_INFINITY, f64::max),
        ),
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
