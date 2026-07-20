//! Native-histogram function set (M7-A5b-i): `histogram_quantile`,
//! `histogram_fraction`, and the six accessors
//! (`histogram_count/sum/avg/stddev/stdvar`) over native
//! [`pulsus_model::FloatHistogram`] buckets — ported from the pinned
//! Prometheus `promql/quantile.go` (`HistogramQuantile`/`HistogramFraction`)
//! and `promql/functions.go` (`simpleHistogramFunc`/`histogramVariance`),
//! v3.13.0 @ `40af9c2`, not re-derived. The classic `le`-bucketed forms
//! (`functions::histogram_quantile`, [`bucket_fraction`]) are the
//! counterpart for float samples carrying a `le` label — this module is
//! native-histogram-only.

use pulsus_model::{FloatBucket, FloatHistogram};

use crate::annotations::{Annotations, messages};
use crate::math::kahan_inc;

/// `histogram_quantile(q, h)` over a native histogram — ports
/// `quantile.go` `HistogramQuantile` (`:222`) operation-for-operation.
/// `metric_name` (empty if none) feeds the two NaN-observation info
/// annotations' `maybeAddMetricName` suffix (`messages::
/// native_histogram_quantile_nan_result_info`/`_skew_info`).
pub fn histogram_quantile(
    q: f64,
    h: &FloatHistogram,
    metric_name: &str,
    annos: &mut Annotations,
) -> f64 {
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    if h.count == 0.0 || q.is_nan() {
        return f64::NAN;
    }

    // If there are NaN observations (h.Sum is NaN) or q < 0.5, use the
    // forward iterator; otherwise the reverse iterator (`quantile.go:242-250`).
    let use_forward = h.sum.is_nan() || q < 0.5;
    let (all, mut rank) = if use_forward {
        (h.all_buckets(), q * h.count)
    } else {
        (h.all_buckets_reverse(), (1.0 - q) * h.count)
    };

    // `bucket = it.At()` is assigned UNCONDITIONALLY every iteration in the
    // pin (`quantile.go:253-261`) before the `Count == 0` skip — mirrored
    // here via an `Option` seeded on the first visited bucket regardless of
    // its count, not only the first *non-empty* one.
    let mut count = 0.0f64;
    let mut idx = 0usize;
    let mut bucket: Option<FloatBucket> = None;
    while idx < all.len() {
        let b = all[idx];
        idx += 1;
        bucket = Some(b);
        if b.count == 0.0 {
            continue;
        }
        count += b.count;
        if count >= rank {
            break;
        }
    }
    let Some(mut bucket) = bucket else {
        return f64::NAN;
    };

    if !h.uses_custom_buckets() && bucket.lower < 0.0 && bucket.upper > 0.0 {
        if h.negative_buckets.is_empty() && !h.positive_buckets.is_empty() {
            // The result is in the zero bucket and the histogram has only
            // positive buckets: consider 0 the lower bound.
            bucket.lower = 0.0;
        } else if h.positive_buckets.is_empty() && !h.negative_buckets.is_empty() {
            // Only negative buckets: consider 0 the upper bound.
            bucket.upper = 0.0;
        }
    } else if h.uses_custom_buckets() {
        if bucket.lower == f64::NEG_INFINITY {
            // First bucket, lower bound -Inf.
            if bucket.upper <= 0.0 {
                return bucket.upper;
            }
            bucket.lower = 0.0;
        } else if bucket.upper == f64::INFINITY {
            // Last bucket, upper bound +Inf.
            return bucket.lower;
        }
    }

    // Numerical inaccuracies must never push count above h.Count.
    if count > h.count {
        count = h.count;
    }
    // Hit the highest bucket without reaching rank — only possible with NaN
    // observations (Sum is then also NaN).
    if count < rank {
        if h.sum.is_nan() {
            annos.info(messages::native_histogram_quantile_nan_result_info(
                metric_name,
            ));
            return f64::NAN;
        }
        return bucket.upper;
    }

    // NaN observations increase h.Count but not the buckets' total, so the
    // forward iterator is used to find percentiles when Sum is NaN.
    if h.sum.is_nan() || q < 0.5 {
        rank -= count - bucket.count;
    } else {
        rank = count - rank;
    }
    // Detect if h.Count is greater than the sum of buckets (NaN skew).
    if h.sum.is_nan() {
        for b in &all[idx..] {
            count += b.count;
        }
        if count < h.count {
            annos.info(messages::native_histogram_quantile_nan_skew_info(
                metric_name,
            ));
        }
    }

    let fraction = rank / bucket.count;

    // Linear interpolation for custom buckets and for quantiles landing in
    // the zero bucket.
    if h.uses_custom_buckets() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return bucket.lower + (bucket.upper - bucket.lower) * fraction;
    }

    // Exponential (logarithmic-scale) interpolation otherwise.
    let log_lower = bucket.lower.abs().log2();
    let log_upper = bucket.upper.abs().log2();
    if bucket.lower > 0.0 {
        (log_lower + (log_upper - log_lower) * fraction).exp2()
    } else {
        -((log_upper + (log_lower - log_upper) * (1.0 - fraction)).exp2())
    }
}

/// `interpolateLinearly` — shared by [`histogram_fraction`] (used for
/// custom-bucket histograms and the zero bucket); a free function (not a
/// closure) because [`FloatBucket`] is `Copy` and this avoids the
/// simultaneous-borrow conflict a closure capturing `b`/`rank` by
/// reference would hit against the loop's own `rank += b.count` mutation
/// (`quantile.go:417-427`).
fn interpolate_linearly(b: FloatBucket, rank: f64, v: f64) -> f64 {
    if b.lower == f64::NEG_INFINITY {
        b.count
    } else {
        rank + b.count * (v - b.lower) / (b.upper - b.lower)
    }
}

/// `interpolateExponentially` — the logarithmic-scale counterpart used for
/// standard exponential buckets (`quantile.go:430-441`).
fn interpolate_exponentially(b: FloatBucket, rank: f64, v: f64) -> f64 {
    let log_lower = b.lower.abs().log2();
    let log_upper = b.upper.abs().log2();
    let log_v = v.abs().log2();
    let fraction = if v > 0.0 {
        (log_v - log_lower) / (log_upper - log_lower)
    } else {
        1.0 - ((log_v - log_upper) / (log_lower - log_upper))
    };
    rank + b.count * fraction
}

/// `histogram_fraction(lower, upper, h)` over a native histogram — ports
/// `quantile.go` `HistogramFraction` (`:394`) operation-for-operation,
/// including the NaN-observations info annotation (plan v4 residual B:
/// range-independent — the trigger is `h.Sum.is_nan() && total bucket
/// count after draining every remaining bucket < h.Count`, not a
/// `lower==-Inf && upper==+Inf` gate).
pub fn histogram_fraction(
    lower: f64,
    upper: f64,
    h: &FloatHistogram,
    metric_name: &str,
    annos: &mut Annotations,
) -> f64 {
    if h.count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let all = h.all_buckets();
    let mut count = 0.0f64;
    let mut rank = 0.0f64;
    let mut lower_rank = 0.0f64;
    let mut upper_rank = 0.0f64;
    let mut lower_set = false;
    let mut upper_set = false;
    let mut idx = 0usize;

    while idx < all.len() {
        let mut b = all[idx];
        idx += 1;
        count += b.count;
        let mut zero_bucket = false;

        if b.lower <= 0.0 && b.upper >= 0.0 {
            zero_bucket = true;
            if h.negative_buckets.is_empty() && !h.positive_buckets.is_empty() {
                b.lower = 0.0;
            } else if h.positive_buckets.is_empty() && !h.negative_buckets.is_empty() {
                b.upper = 0.0;
            }
        }

        if !lower_set && b.lower >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && b.lower >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && b.lower < lower && b.upper > lower {
            lower_rank = if h.uses_custom_buckets() || zero_bucket {
                interpolate_linearly(b, rank, lower)
            } else {
                interpolate_exponentially(b, rank, lower)
            };
            lower_set = true;
        }
        if !upper_set && b.lower < upper && b.upper > upper {
            upper_rank = if h.uses_custom_buckets() || zero_bucket {
                interpolate_linearly(b, rank, upper)
            } else {
                interpolate_exponentially(b, rank, upper)
            };
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank += b.count;
    }

    if h.sum.is_nan() {
        // Possible NaN observations: adjust `count` to include only
        // non-NaN observations by draining the rest of the buckets.
        for b in &all[idx..] {
            count += b.count;
        }
        if count < h.count {
            annos.info(messages::native_histogram_fraction_nans_info(metric_name));
        }
    } else {
        count = h.count;
    }

    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }

    (upper_rank - lower_rank) / h.count
}

/// `histogram_count(h)` — `functions.go` `funcHistogramCount` via
/// `simpleHistogramFunc` (`:2017`).
pub fn histogram_count(h: &FloatHistogram) -> f64 {
    h.count
}

/// `histogram_sum(h)` — `functions.go` `funcHistogramSum`.
pub fn histogram_sum(h: &FloatHistogram) -> f64 {
    h.sum
}

/// `histogram_avg(h)` — `functions.go` `funcHistogramAvg`: `h.Sum / h.Count`
/// verbatim (never `histogram_sum`/`histogram_count`'s separately-rounded
/// composition).
pub fn histogram_avg(h: &FloatHistogram) -> f64 {
    h.sum / h.count
}

/// `histogramVariance` (`functions.go:1978-2016`): per-bucket
/// geometric-mean (exponential buckets) / arithmetic-mean (custom buckets,
/// or the zero bucket) squared-deviation, Kahan-compensated
/// (`util/kahansum.Inc`, [`kahan_inc`]). Shared by [`histogram_stddev`]
/// (`sqrt`) and [`histogram_stdvar`] (identity).
fn histogram_variance(h: &FloatHistogram) -> f64 {
    let mean = h.sum / h.count;
    let mut variance = 0.0f64;
    let mut c_variance = 0.0f64;
    for b in h.all_buckets() {
        if b.count == 0.0 {
            continue;
        }
        let val = if h.uses_custom_buckets() {
            (b.upper + b.lower) / 2.0
        } else if b.lower <= 0.0 && b.upper >= 0.0 {
            0.0
        } else {
            let v = (b.upper * b.lower).sqrt();
            if b.upper < 0.0 { -v } else { v }
        };
        let delta = val - mean;
        let (new_variance, new_c) = kahan_inc(b.count * delta * delta, variance, c_variance);
        variance = new_variance;
        c_variance = new_c;
    }
    variance += c_variance;
    variance / h.count
}

/// `histogram_stddev(h)` — `functions.go` `funcHistogramStdDev`.
pub fn histogram_stddev(h: &FloatHistogram) -> f64 {
    histogram_variance(h).sqrt()
}

/// `histogram_stdvar(h)` — `functions.go` `funcHistogramStdVar`.
pub fn histogram_stdvar(h: &FloatHistogram) -> f64 {
    histogram_variance(h)
}

#[cfg(test)]
mod tests {
    use pulsus_model::{NativeHistogram, Span};

    use super::*;

    /// `single_histogram {{schema:0 sum:5 count:4 buckets:[1 2 1]}}`
    /// (`native_histograms.test:34`): schema-0 exponential, positive
    /// buckets (0.5,1]:1, (1,2]:2, (2,4]:1.
    fn single_histogram() -> FloatHistogram {
        NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, 1, -1],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float()
    }

    /// `custom_buckets_histogram {{schema:-53 sum:5 count:4
    /// custom_values:[5 10] buckets:[1 2 1]}}` (`native_histograms.test:1078`):
    /// NHCB, buckets (-Inf,5]:1, (5,10]:2, (10,+Inf]:1.
    fn custom_buckets_histogram() -> FloatHistogram {
        NativeHistogram {
            schema: pulsus_model::CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, 1, -1],
            negative_buckets: vec![],
            custom_values: vec![5.0, 10.0],
        }
        .to_float()
    }

    // -- AC i-2: native histogram_quantile clamping/NaN/empty --

    #[test]
    fn quantile_below_zero_is_negative_infinity() {
        let mut annos = Annotations::new();
        assert_eq!(
            histogram_quantile(-0.1, &single_histogram(), "", &mut annos),
            f64::NEG_INFINITY
        );
        assert!(annos.is_empty(), "clamping alone adds no annotation here");
    }

    #[test]
    fn quantile_above_one_is_positive_infinity() {
        let mut annos = Annotations::new();
        assert_eq!(
            histogram_quantile(1.1, &single_histogram(), "", &mut annos),
            f64::INFINITY
        );
    }

    #[test]
    fn quantile_of_nan_is_nan() {
        let mut annos = Annotations::new();
        assert!(histogram_quantile(f64::NAN, &single_histogram(), "", &mut annos).is_nan());
    }

    #[test]
    fn quantile_of_a_zero_count_histogram_is_nan() {
        let mut h = single_histogram();
        h.count = 0.0;
        let mut annos = Annotations::new();
        assert!(histogram_quantile(0.5, &h, "", &mut annos).is_nan());
    }

    #[test]
    fn quantile_interior_exponential_interpolation() {
        // rank = 0.5*4 = 2.0 with q>=0.5 -> reverse iterator, rank=(1-0.5)*4=2.
        // Reverse order: (2,4]:1 (count=1, <2) -> (1,2]:2 (count=3>=2) ->
        // bucket=(1,2], rank = count-rank = 3-2 = 1, fraction=1/2=0.5.
        // Positive bucket exponential interp: 2^(log2(1)+(log2(2)-log2(1))*0.5)
        // = 2^0.5.
        let mut annos = Annotations::new();
        let v = histogram_quantile(0.5, &single_histogram(), "", &mut annos);
        assert!((v - std::f64::consts::SQRT_2).abs() < 1e-12, "got {v}");
        assert!(annos.is_empty());
    }

    #[test]
    fn quantile_nhcb_interpolates_linearly() {
        // rank = 0.5*4=2 (q<0.5 forward, or q==0.5 -> reverse: rank=2 either
        // way here by symmetry). Forward: (-Inf,5]:1 (1<2) -> (5,10]:2
        // (count=3>=2) bucket=(5,10], rank-=count-bucket.count=3-2=1... use
        // reverse (q>=0.5): reverse order (10,+Inf]:1(1<2) -> (5,10]:2
        // (count=3>=2), bucket=(5,10], rank=count-rank=3-2=1, fraction=0.5,
        // linear: 5 + (10-5)*0.5 = 7.5.
        let mut annos = Annotations::new();
        let v = histogram_quantile(0.5, &custom_buckets_histogram(), "req", &mut annos);
        assert!((v - 7.5).abs() < 1e-12, "got {v}");
    }

    #[test]
    fn quantile_zero_bucket_clamps_to_positive_only_lower_bound() {
        // A histogram whose POSITIVE side has at least one (zero-count)
        // bucket entry and whose NEGATIVE side has none — upstream's clamp
        // check is on array *presence* (`len(h.PositiveBuckets) > 0`), not
        // on any bucket's count — so the zero bucket's Lower clamps to 0
        // rather than -zero_threshold.
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.5,
            zero_count: 10,
            count: 10,
            sum: 0.0,
            positive_spans: vec![Span {
                offset: 5,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![0],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let mut annos = Annotations::new();
        // Force the forward iterator deterministically (q < 0.5):
        // rank = 0.1*10 = 1.
        let v = histogram_quantile(0.1, &h, "", &mut annos);
        // Only the zero bucket has observations; fraction = rank/count =
        // 1/10 = 0.1; clamped lower=0, upper=0.5: 0 + 0.5*0.1 = 0.05.
        assert!((v - 0.05).abs() < 1e-12, "got {v}");
    }

    // -- AC i-4: histogram_fraction --

    #[test]
    fn fraction_of_a_zero_count_histogram_is_nan() {
        let mut h = single_histogram();
        h.count = 0.0;
        let mut annos = Annotations::new();
        assert!(histogram_fraction(0.0, 1.0, &h, "", &mut annos).is_nan());
    }

    #[test]
    fn fraction_of_nan_bounds_is_nan() {
        let mut annos = Annotations::new();
        assert!(histogram_fraction(f64::NAN, 1.0, &single_histogram(), "", &mut annos).is_nan());
        assert!(histogram_fraction(0.0, f64::NAN, &single_histogram(), "", &mut annos).is_nan());
    }

    #[test]
    fn fraction_lower_greater_or_equal_upper_is_zero() {
        let mut annos = Annotations::new();
        assert_eq!(
            histogram_fraction(1.0, 1.0, &single_histogram(), "", &mut annos),
            0.0
        );
        assert_eq!(
            histogram_fraction(2.0, 1.0, &single_histogram(), "", &mut annos),
            0.0
        );
    }

    #[test]
    fn fraction_full_range_is_one() {
        let mut annos = Annotations::new();
        let v = histogram_fraction(
            f64::NEG_INFINITY,
            f64::INFINITY,
            &single_histogram(),
            "",
            &mut annos,
        );
        assert!((v - 1.0).abs() < 1e-12, "got {v}");
    }

    #[test]
    fn fraction_is_inverse_of_quantile_at_the_same_point() {
        let h = single_histogram();
        let mut annos = Annotations::new();
        let q = histogram_quantile(0.5, &h, "", &mut annos);
        let f = histogram_fraction(f64::NEG_INFINITY, q, &h, "", &mut annos);
        assert!((f - 0.5).abs() < 1e-9, "got quantile={q} fraction={f}");
    }

    #[test]
    fn fraction_nans_info_fires_when_nan_observations_are_excluded() {
        // NaN sum with fewer bucket observations than h.Count signals NaN
        // observations excluded from every fraction (residual B: range-
        // independent trigger).
        let mut h = single_histogram();
        h.sum = f64::NAN;
        h.count = 5.0; // buckets sum to 4 < 5 -> NaN observations present.
        let mut annos = Annotations::new();
        let _ = histogram_fraction(f64::NEG_INFINITY, f64::INFINITY, &h, "m", &mut annos);
        let (_, infos) = annos.as_strings(0, 0);
        assert_eq!(infos.len(), 1);
        assert!(infos[0].contains("histogram_fraction") && infos[0].contains("NaN"));
    }

    #[test]
    fn fraction_nans_info_does_not_fire_when_bucket_total_matches_count() {
        let mut h = single_histogram();
        h.sum = f64::NAN;
        // count already equals the bucket total (4) -- no NaN observations.
        let mut annos = Annotations::new();
        let _ = histogram_fraction(f64::NEG_INFINITY, f64::INFINITY, &h, "m", &mut annos);
        assert!(annos.is_empty());
    }

    // -- AC i-3: the six accessors --

    #[test]
    fn accessors_read_count_sum_avg_directly() {
        let h = single_histogram();
        assert_eq!(histogram_count(&h), 4.0);
        assert_eq!(histogram_sum(&h), 5.0);
        assert_eq!(histogram_avg(&h), 1.25);
    }

    #[test]
    fn stdvar_is_stddev_squared() {
        let h = single_histogram();
        let stddev = histogram_stddev(&h);
        let stdvar = histogram_stdvar(&h);
        assert!(
            (stddev * stddev - stdvar).abs() < 1e-9,
            "{stddev} vs {stdvar}"
        );
        assert!(stdvar > 0.0, "a dispersed histogram has positive variance");
    }

    #[test]
    fn variance_uses_arithmetic_mean_for_custom_buckets() {
        // NHCB uses (upper+lower)/2, never the geometric mean — a faithful
        // consequence (matching the pin, not a bug here) is that the
        // open-ended first/last NHCB buckets ((-Inf,5], (10,+Inf]) produce
        // a non-finite representative value, same as upstream's own
        // `histogramVariance` would for this fixture.
        let h = custom_buckets_histogram();
        let v = histogram_stdvar(&h);
        assert!(
            !v.is_finite(),
            "open-ended NHCB buckets make variance non-finite, got {v}"
        );
    }

    #[test]
    fn variance_zero_bucket_uses_zero_as_the_representative_value() {
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.5,
            zero_count: 10,
            count: 10,
            sum: 0.0,
            positive_spans: vec![],
            negative_spans: vec![],
            positive_buckets: vec![],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        // Every observation falls in the zero bucket, representative value
        // 0; mean = 0/10 = 0, so variance is exactly 0.
        assert_eq!(histogram_stdvar(&h), 0.0);
    }
}
