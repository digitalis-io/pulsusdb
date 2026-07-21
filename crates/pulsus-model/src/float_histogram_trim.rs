// M7-#129: `FloatHistogram::trim_buckets` — the native-histogram TRIM
// operators' (`</` TRIM_UPPER, `>/` TRIM_LOWER) bucket-level primitive,
// ported operation-for-operation from the pinned `model/histogram/
// float_histogram.go` (v3.13.0, `40af9c2`, via `git show
// 40af9c2:model/histogram/float_histogram.go`), lines 2122-2454:
// `TrimBuckets`, `handleInfinityBuckets`, `computeSplit`,
// `computeZeroBucketTrim`, `computeBucketTrim`, `computeMidpoint`.
//
// **Iteration order.** Upstream's `PositiveBucketIterator`/
// `NegativeBucketIterator` both walk in ascending STORAGE order (the
// `floatBucketIterator(_, 0, h.Schema)` fast path — "starting next to the
// zero bucket and going up" for both sides, i.e. dense array index 0..len,
// NOT `AllBucketIterator`'s reversed-negative walk). This is exactly
// [`natural_side_buckets`]'s own order (`float_histogram.rs`'s ordering
// proof), so each side is decoded once via `natural_side_buckets(..)` and
// walked with `enumerate()` — the enumeration index IS
// `trimmedHist.PositiveBuckets[i]`/`NegativeBuckets[i]`'s dense index.
// Deliberately NOT [`FloatHistogram::all_buckets`] — its zero-threshold
// boundary clamp is `AllBucketIterator`-only and would corrupt the bucket
// bounds `computeSplit`/`computeMidpoint` interpolate over.
//
// **Changed-only totals (`trimmedBuckets` flag).** `Count`/`Sum` are
// recomputed from the trimmed buckets' midpoints ONLY when at least one
// bucket's stored value actually changed value — a no-op trim (every
// bucket already entirely on the kept side) leaves `Count`/`Sum` at their
// ORIGINAL bit-exact values, not a midpoint-approximated recomputation.
// This is load-bearing: `h_test >/ -Inf` (every bucket entirely above
// `-Inf`, hence unchanged) must return the original `sum:123.75`, not a
// geometric-midpoint approximation of it.
//
// **No NaN special-casing.** The comparisons below are a literal port of
// Go's `<`/`<=`/`>`/`>=`/`==` structure; Rust's `f64` comparison operators
// have the same IEEE-754 semantics as Go's, so NaN propagates through
// exactly as upstream (every comparison involving NaN is `false`) with no
// added handling.

/// One side's [`FloatHistogram::trim_buckets`] pass outcome: whether any
/// bucket on this side had a nonzero count (`hasPositive`/`hasNegative`,
/// used by [`compute_zero_bucket_trim`]'s bias) and whether any bucket's
/// stored value actually changed (`trimmedBuckets`).
struct TrimSideResult {
    has_any: bool,
    trimmed: bool,
}

/// The single running count/sum accumulator pair `TrimBuckets` threads
/// serially through BOTH side walks and the zero bucket (`updatedCount`/
/// `updatedSum` in the pin) — one accumulator, upstream's exact traversal
/// order (every positive bucket, then every negative bucket, then zero).
/// Deliberately NOT per-side subtotals combined afterwards: IEEE-754
/// addition is not associative, so pre-summing a multi-bucket negative
/// side and adding it as one term can round differently in the last ULP
/// (the #129 codex code-review [medium] finding; pinned by
/// `trim_recomputed_totals_accumulate_serially_in_upstream_traversal_order`).
struct TrimTotals {
    count: f64,
    sum: f64,
}

/// One side's trim pass (`TrimBuckets`'s per-side `for i, iter :=
/// ...PositiveBucketIterator(); ...` / `...NegativeBucketIterator()...`
/// loop, both `isUpperTrim` arms): `buckets` is the dense array mutated in
/// place (`trimmedHist.PositiveBuckets`/`NegativeBuckets`); `decoded` is
/// that side's bounds, in the same dense order (`natural_side_buckets`);
/// `totals` is the SHARED serial accumulator (see [`TrimTotals`]) this
/// pass continues, `+=`-ing per bucket exactly where the pin does.
fn trim_side(
    buckets: &mut [f64],
    decoded: &[Bucket],
    is_positive: bool,
    is_upper_trim: bool,
    rhs: f64,
    is_custom_bucket: bool,
    totals: &mut TrimTotals,
) -> TrimSideResult {
    let mut has_any = false;
    let mut trimmed = false;
    for (i, bucket) in decoded.iter().enumerate() {
        if bucket.count == 0.0 {
            continue;
        }
        has_any = true;
        let entirely_kept = if is_upper_trim {
            bucket.upper <= rhs
        } else {
            bucket.lower >= rhs
        };
        let contains_trim_point = if is_upper_trim {
            bucket.lower < rhs
        } else {
            bucket.upper > rhs
        };
        if entirely_kept {
            totals.count += bucket.count;
            totals.sum +=
                compute_midpoint(bucket.lower, bucket.upper, is_positive, is_custom_bucket)
                    * bucket.count;
        } else if contains_trim_point {
            let (keep_count, midpoint) =
                compute_bucket_trim(bucket, rhs, is_upper_trim, is_positive, is_custom_bucket);
            totals.count += keep_count;
            totals.sum += midpoint * keep_count;
            if buckets[i] != keep_count {
                buckets[i] = keep_count;
                trimmed = true;
            }
        } else {
            buckets[i] = 0.0;
            trimmed = true;
        }
    }
    TrimSideResult { has_any, trimmed }
}

impl FloatHistogram {
    /// `TrimBuckets` (`float_histogram.go:2122-2179`): trims the histogram
    /// at `rhs`, keeping observations below it (`is_upper_trim` — `</`,
    /// TRIM_UPPER) or above it (`>/`, TRIM_LOWER). Returns a new,
    /// independent histogram (`h.Copy()` in the pin); `self` is untouched.
    pub fn trim_buckets(&self, rhs: f64, is_upper_trim: bool) -> FloatHistogram {
        let mut trimmed = self.clone();
        let is_custom_bucket = trimmed.uses_custom_buckets();

        // ONE serial count/sum accumulator threaded through both side
        // walks and the zero bucket, in the pin's exact traversal order
        // (positive, then negative, then zero) — see [`TrimTotals`].
        let mut totals = TrimTotals {
            count: 0.0,
            sum: 0.0,
        };

        let positive_decoded = natural_side_buckets(
            &trimmed.positive_spans,
            &trimmed.positive_buckets,
            true,
            trimmed.schema,
            &trimmed.custom_values,
        );
        let pos = trim_side(
            &mut trimmed.positive_buckets,
            &positive_decoded,
            true,
            is_upper_trim,
            rhs,
            is_custom_bucket,
            &mut totals,
        );

        let negative_decoded = natural_side_buckets(
            &trimmed.negative_spans,
            &trimmed.negative_buckets,
            false,
            trimmed.schema,
            &trimmed.custom_values,
        );
        let neg = trim_side(
            &mut trimmed.negative_buckets,
            &negative_decoded,
            false,
            is_upper_trim,
            rhs,
            is_custom_bucket,
            &mut totals,
        );

        let mut trimmed_buckets = pos.trimmed || neg.trimmed;

        // Handle the zero count bucket.
        if trimmed.zero_count > 0.0 {
            let zero_bucket = trimmed.zero_bucket();
            let (keep_count, midpoint) = compute_zero_bucket_trim(
                &zero_bucket,
                rhs,
                neg.has_any,
                pos.has_any,
                is_upper_trim,
            );
            if trimmed.zero_count != keep_count {
                trimmed.zero_count = keep_count;
                trimmed_buckets = true;
            }
            totals.sum += midpoint * keep_count;
            totals.count += keep_count;
        }

        if trimmed_buckets {
            // Only update the totals in case some bucket(s) were fully (or
            // partially) trimmed — see this file's module doc.
            trimmed.count = totals.count;
            trimmed.sum = totals.sum;
            trimmed.compact();
        }

        trimmed
    }
}

/// `computeSplit` (`float_histogram.go:2358-2382`): the portion of the
/// bucket's count at or below `rhs`.
fn compute_split(b: &Bucket, rhs: f64, is_positive: bool, is_linear: bool) -> f64 {
    if rhs <= b.lower {
        return 0.0;
    }
    if rhs >= b.upper {
        return b.count;
    }
    let fraction = if is_linear {
        (rhs - b.lower) / (b.upper - b.lower)
    } else {
        let log_lower = b.lower.abs().log2();
        let log_upper = b.upper.abs().log2();
        let log_v = rhs.abs().log2();
        if is_positive {
            (log_v - log_lower) / (log_upper - log_lower)
        } else {
            1.0 - ((log_v - log_upper) / (log_lower - log_upper))
        }
    };
    b.count * fraction
}

/// `computeZeroBucketTrim` (`float_histogram.go:2384-2409`): the zero
/// bucket's kept count and midpoint, biased to `[0, upper]`/`[lower, 0]`
/// when only one non-zero side is populated (`has_positive`/
/// `has_negative`, set by [`trim_side`]'s non-zero-count buckets only).
fn compute_zero_bucket_trim(
    zero_bucket: &Bucket,
    rhs: f64,
    has_negative: bool,
    has_positive: bool,
    is_upper_trim: bool,
) -> (f64, f64) {
    let mut lower = zero_bucket.lower;
    let mut upper = zero_bucket.upper;
    if has_negative && !has_positive {
        upper = 0.0;
    }
    if has_positive && !has_negative {
        lower = 0.0;
    }

    if is_upper_trim {
        if rhs <= lower {
            return (0.0, 0.0);
        }
        if rhs >= upper {
            return (zero_bucket.count, (lower + upper) / 2.0);
        }
        let fraction = (rhs - lower) / (upper - lower);
        let midpoint = (lower + rhs) / 2.0;
        (zero_bucket.count * fraction, midpoint)
    } else {
        if rhs <= lower {
            return (zero_bucket.count, (lower + upper) / 2.0);
        }
        if rhs >= upper {
            return (0.0, 0.0);
        }
        let fraction = (upper - rhs) / (upper - lower);
        let midpoint = (rhs + upper) / 2.0;
        (zero_bucket.count * fraction, midpoint)
    }
}

/// `computeBucketTrim` (`float_histogram.go:2411-2422`): the kept count
/// and midpoint for a bucket straddling `rhs`. Delegates to
/// [`handle_infinity_buckets`] when either bound is infinite (the pin's
/// `math.IsInf(b.Lower, -1) || math.IsInf(b.Upper, 1)` guard).
fn compute_bucket_trim(
    b: &Bucket,
    rhs: f64,
    is_upper_trim: bool,
    is_positive: bool,
    is_custom_bucket: bool,
) -> (f64, f64) {
    if b.lower == f64::NEG_INFINITY || b.upper == f64::INFINITY {
        return handle_infinity_buckets(is_upper_trim, b, rhs);
    }
    let under_count = compute_split(b, rhs, is_positive, is_custom_bucket);
    if is_upper_trim {
        (
            under_count,
            compute_midpoint(b.lower, rhs, is_positive, is_custom_bucket),
        )
    } else {
        (
            b.count - under_count,
            compute_midpoint(rhs, b.upper, is_positive, is_custom_bucket),
        )
    }
}

/// `computeMidpoint` (`float_histogram.go:2445-2454`): the representative
/// value for a surviving `[lower, upper]` interval — geometric mean
/// (signed by `is_positive`) for exponential schemas, arithmetic mean
/// (`is_linear`) for NHCB, with the pin's own infinite-bound
/// special-casing.
fn compute_midpoint(lower: f64, upper: f64, is_positive: bool, is_linear: bool) -> f64 {
    if lower.is_infinite() {
        if upper.is_infinite() {
            return 0.0;
        }
        if upper > 0.0 {
            return upper / 2.0;
        }
        return upper;
    } else if upper.is_infinite() {
        return lower;
    }

    if is_linear {
        return (lower + upper) / 2.0;
    }

    let geo_mean = (lower * upper).abs().sqrt();
    if is_positive { geo_mean } else { -geo_mean }
}

/// `handleInfinityBuckets` (`float_histogram.go:2299-2356`): the kept
/// count and midpoint for a bucket with an infinite lower or upper bound
/// — conservative discard when the trim point falls inside the infinite
/// span's unknown distribution (see the pin's own per-branch comments,
/// reproduced below), otherwise exact (the whole bucket is kept, or a
/// finite-NHCB-upper bucket linearly interpolates treating `-Inf` as `0`).
fn handle_infinity_buckets(is_upper_trim: bool, b: &Bucket, rhs: f64) -> (f64, f64) {
    fn zero_if_inf(x: f64) -> f64 {
        if x.is_infinite() { 0.0 } else { x }
    }

    // Case 1: bucket with lower bound -Inf.
    if b.lower == f64::NEG_INFINITY {
        if is_upper_trim {
            // TRIM_UPPER (`</`) - remove values greater than rhs.
            if rhs >= b.upper {
                // rhs is greater than the upper bound: keep the entire bucket.
                return (b.count, 0.0);
            }
            if rhs > 0.0 && b.upper > 0.0 && b.upper != f64::INFINITY {
                // Upper is finite and positive: treat lower as 0 (despite
                // it de facto being -Inf). Only possible with NHCB, so
                // linear interpolation is always valid here.
                return (b.count * rhs / b.upper, rhs / 2.0);
            }
            if b.upper <= 0.0 {
                return (b.count, rhs);
            }
            // A valid trim, but the exact distribution inside an infinite
            // bucket is unknown: remove the entire bucket.
            return (0.0, zero_if_inf(b.upper));
        }
        // TRIM_LOWER (`>/`) - remove values less than rhs.
        if rhs <= b.lower {
            // Impossible to happen because the lower bound is -Inf.
            // Returning the entire current bucket.
            return (b.count, 0.0);
        }
        if rhs >= 0.0 && b.upper > rhs && b.upper != f64::INFINITY {
            // Upper is finite and positive: treat lower as 0. Only
            // possible with NHCB, so linear interpolation is always valid.
            return (b.count * (1.0 - rhs / b.upper), (rhs + b.upper) / 2.0);
        }
        return (0.0, zero_if_inf(b.upper));
    }

    // Case 2: bucket with upper bound +Inf.
    if b.upper == f64::INFINITY {
        if is_upper_trim {
            // TRIM_UPPER (`</`) - remove values greater than rhs. Lower
            // doesn't matter: whether rhs >= lower (some values in this
            // +Inf-extending bucket could exceed rhs) or rhs < lower (every
            // value in the bucket is >= lower > rhs), the entire bucket is
            // removed either way.
            return (0.0, zero_if_inf(b.lower));
        }
        // TRIM_LOWER (`>/`) - remove values less than rhs.
        if rhs >= b.lower {
            return (b.count, rhs);
        }
        // lower < rhs: inside the infinity bucket, but the exact
        // distribution is unknown: conservatively remove the entire bucket.
        return (0.0, zero_if_inf(b.lower));
    }

    unreachable!("one of the bounds must be infinite for handle_infinity_buckets, got {b:?}");
}

#[cfg(test)]
mod trim_tests {
    use super::*;
    use crate::histogram::CUSTOM_BUCKETS_SCHEMA;

    /// Schema-0 exponential, positive buckets (0.5,1]:a, (1,2]:b, (2,4]:c,
    /// no zero bucket, no negative side.
    fn exp_hist(count: u64, sum: f64, abs_buckets: [i64; 3]) -> FloatHistogram {
        let deltas = [
            abs_buckets[0],
            abs_buckets[1] - abs_buckets[0],
            abs_buckets[2] - abs_buckets[1],
        ];
        NativeHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count,
            sum,
            positive_spans: vec![Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_buckets: deltas.to_vec(),
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float()
    }

    /// `h_test` (`native_histograms.test`): schema 0, buckets
    /// `[1 2 1]` (absolute), count 4, sum 123.75 — the corpus's own
    /// sum-preservation pin fixture (`h_test >/ -Inf`).
    fn h_test() -> FloatHistogram {
        exp_hist(4, 123.75, [1, 2, 1])
    }

    /// NHCB with the given custom bounds; `abs_buckets` are absolute
    /// counts (span offset 0).
    fn nhcb_hist(count: u64, sum: f64, bounds: Vec<f64>, abs_buckets: Vec<i64>) -> FloatHistogram {
        let mut deltas = vec![abs_buckets[0]];
        for w in abs_buckets.windows(2) {
            deltas.push(w[1] - w[0]);
        }
        NativeHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            zero_count: 0,
            count,
            sum,
            positive_spans: vec![Span {
                offset: 0,
                length: abs_buckets.len() as u32,
            }],
            negative_spans: vec![],
            positive_buckets: deltas,
            negative_buckets: vec![],
            custom_values: bounds,
        }
        .to_float()
    }

    // -- No-op trim: sum/count preservation (the `trimmedBuckets` gate) --

    #[test]
    fn trim_lower_at_neg_inf_keeps_original_sum_and_count_bit_exact() {
        // Every bucket is entirely above -Inf (no bucket straddles it), so
        // no bucket's stored value changes: Count/Sum stay at their
        // ORIGINAL values, not a midpoint recomputation.
        let h = h_test();
        let trimmed = h.trim_buckets(f64::NEG_INFINITY, false);
        assert_eq!(trimmed.sum, 123.75);
        assert_eq!(trimmed.count, 4.0);
        assert_eq!(trimmed.positive_buckets, vec![1.0, 2.0, 1.0]);
    }

    #[test]
    fn trim_upper_at_pos_inf_keeps_original_sum_and_count_bit_exact() {
        let h = h_test();
        let trimmed = h.trim_buckets(f64::INFINITY, true);
        assert_eq!(trimmed.sum, 123.75);
        assert_eq!(trimmed.count, 4.0);
        assert_eq!(trimmed.positive_buckets, vec![1.0, 2.0, 1.0]);
    }

    // -- Exponential interpolation --

    #[test]
    fn trim_upper_drops_buckets_entirely_above_rhs_and_recomputes_totals() {
        // rhs = 1.0 lands exactly on the (0.5,1] bucket's upper edge -
        // entirely kept; (1,2] and (2,4] are entirely above - dropped.
        let h = exp_hist(4, 5.0, [1, 2, 1]);
        let trimmed = h.trim_buckets(1.0, true);
        assert_eq!(trimmed.count, 1.0);
        // Kept bucket's midpoint: geometric mean of 0.5 and 1.0.
        let expected_sum = (0.5f64 * 1.0).sqrt() * 1.0;
        assert!((trimmed.sum - expected_sum).abs() < 1e-12);
        assert_eq!(trimmed.positive_buckets, vec![1.0]);
    }

    #[test]
    fn trim_lower_drops_buckets_entirely_below_rhs_and_recomputes_totals() {
        let h = exp_hist(4, 5.0, [1, 2, 1]);
        let trimmed = h.trim_buckets(1.0, false);
        // (0.5,1] is entirely below (upper == rhs, not > rhs) - dropped.
        // (1,2] and (2,4] are entirely at/above - kept.
        assert_eq!(trimmed.count, 3.0);
        assert_eq!(trimmed.positive_buckets, vec![2.0, 1.0]);
    }

    #[test]
    fn trim_upper_interpolates_exponentially_within_a_straddled_bucket() {
        // rhs = 1.5 lands inside (1,2]: log2 interpolation. Bucket (0.5,1]
        // is already empty and bucket (2,4] is discarded entirely, so
        // after `Compact(0)` only the straddled bucket survives, at
        // index 0.
        let h = exp_hist(2, 3.0, [0, 1, 1]);
        let trimmed = h.trim_buckets(1.5, true);
        let log_lower = 1.0f64.log2();
        let log_upper = 2.0f64.log2();
        let log_v = 1.5f64.log2();
        let fraction = (log_v - log_lower) / (log_upper - log_lower);
        assert_eq!(trimmed.positive_buckets.len(), 1);
        assert!((trimmed.positive_buckets[0] - fraction).abs() < 1e-12);
    }

    // -- NHCB: linear interpolation, exact boundary --

    #[test]
    fn trim_upper_nhcb_uses_linear_interpolation_within_a_straddled_bucket() {
        // Bounds [5, 10]: buckets (-Inf,5]:1, (5,10]:2, (10,+Inf):3. rhs=7.5
        // lands inside (5,10]: linear fraction (7.5-5)/(10-5) = 0.5 of its
        // absolute count 2 is kept; the (-Inf,5] bucket is entirely kept
        // (index 0 survives unchanged), (10,+Inf) is entirely discarded.
        let h = nhcb_hist(3, 3.0, vec![5.0, 10.0], vec![1, 2, 3]);
        let trimmed = h.trim_buckets(7.5, true);
        assert_eq!(trimmed.positive_buckets.len(), 2);
        assert_eq!(trimmed.positive_buckets[0], 1.0);
        assert!((trimmed.positive_buckets[1] - 1.0).abs() < 1e-12);
    }

    #[test]
    fn trim_upper_at_exact_bucket_boundary_needs_no_interpolation() {
        // rhs exactly on a bucket's upper edge is "entirely kept", not
        // "straddled" (upstream's `bucket.Upper <= rhs` branch) - value
        // unchanged; the discarded (2,4] bucket drops out on `Compact(0)`.
        let h = exp_hist(4, 5.0, [1, 2, 1]);
        let trimmed = h.trim_buckets(2.0, true);
        assert_eq!(trimmed.positive_buckets, vec![1.0, 2.0]);
    }

    // -- Zero-bucket bias --

    #[test]
    fn zero_bucket_biases_to_positive_only_when_no_negative_side_populated() {
        let h = NativeHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 1.0,
            zero_count: 4,
            count: 5,
            sum: 0.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        // TRIM_LOWER at rhs=0.5: zero bucket biased to [0,1] (has_positive,
        // no has_negative) - fraction (1-0.5)/(1-0) = 0.5.
        let trimmed = h.trim_buckets(0.5, false);
        assert!((trimmed.zero_count - 2.0).abs() < 1e-12);
    }

    #[test]
    fn zero_bucket_biases_to_negative_only_when_no_positive_side_populated() {
        let h = NativeHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 1.0,
            zero_count: 4,
            count: 5,
            sum: 0.0,
            positive_spans: vec![],
            negative_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            positive_buckets: vec![],
            negative_buckets: vec![1],
            custom_values: vec![],
        }
        .to_float();
        // TRIM_UPPER at rhs=-0.5: zero bucket biased to [-1,0]
        // (has_negative, no has_positive) - fraction (-0.5-(-1))/(0-(-1))
        // = 0.5.
        let trimmed = h.trim_buckets(-0.5, true);
        assert!((trimmed.zero_count - 2.0).abs() < 1e-12);
    }

    // -- +/-Inf rhs behavior already exercised by the sum-preservation
    //    tests above; below: a bucket straddling +/-Inf itself (NHCB). --

    #[test]
    fn trim_lower_nhcb_first_bucket_neg_inf_interpolates_when_upper_finite_positive() {
        // NHCB first bucket is (-Inf, 5]; rhs=2.5, upper=5 finite/positive:
        // linear interpolation treating lower as 0.
        let h = nhcb_hist(1, 1.0, vec![5.0], vec![1]);
        let trimmed = h.trim_buckets(2.5, false);
        assert!((trimmed.positive_buckets[0] - 0.5).abs() < 1e-12);
    }

    /// #129 codex code-review [medium] regression: the recomputed totals
    /// must accumulate SERIALLY in upstream's traversal order (every
    /// positive bucket, then every negative bucket, then zero — one
    /// running accumulator, `TrimBuckets`'s `updatedCount`/`updatedSum`),
    /// NOT per-side subtotals combined afterwards. IEEE-754 addition is
    /// not associative: with the sum accumulator at `sqrt(2)·2^53`
    /// (half-ULP = 1.0) and two negative-bucket contributions of
    /// ≈ −0.7071 each, serial addition rounds back to the accumulator
    /// twice (each |c| < half-ULP), while the pre-fix grouping pre-sums
    /// them to ≈ −1.4142 (> half-ULP) and lands one ULP (2.0) lower.
    #[test]
    fn trim_recomputed_totals_accumulate_serially_in_upstream_traversal_order() {
        const BIG: f64 = 9007199254740992.0; // 2^53
        let h = FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.5,
            zero_count: 3.0,
            count: BIG + 4.5,
            sum: 999.0,
            // Positive indices 1 and 3: (1,2] with the huge count, and
            // (4,8], entirely above rhs=4 — discarded, which is what
            // forces the totals recompute (`trimmedBuckets`).
            positive_spans: vec![
                Span {
                    offset: 1,
                    length: 1,
                },
                Span {
                    offset: 1,
                    length: 1,
                },
            ],
            negative_spans: vec![Span {
                offset: 0,
                length: 2,
            }],
            positive_buckets: vec![BIG, 1.0],
            // Negative indices 0 and 1: (-1,-0.5] count 1 (contribution
            // -sqrt(0.5)) and (-2,-1] count 0.5 (contribution
            // -sqrt(2)*0.5) — two populated negative buckets, each
            // contribution ≈ -0.7071.
            negative_buckets: vec![1.0, 0.5],
            custom_values: vec![],
        };
        let trimmed = h.trim_buckets(4.0, true);

        // Reference values, replicating the implementation's per-bucket
        // arithmetic exactly (geometric-mean midpoint × count), then
        // accumulated in the two candidate orders.
        let p = (1.0f64 * 2.0).abs().sqrt() * BIG; // (1,2] contribution
        let n1 = -((-1.0f64 * -0.5).abs().sqrt()) * 1.0; // (-1,-0.5]
        let n2 = -((-2.0f64 * -1.0).abs().sqrt()) * 0.5; // (-2,-1]
        // Zero bucket: no bias (both sides populated), rhs >= upper keeps
        // all 3.0 at midpoint (−0.5+0.5)/2 = 0.0 — count-only.
        let serial_sum = (((0.0 + p) + n1) + n2) + 0.0 * 3.0;
        let grouped_sum = p + (n1 + n2) + 0.0 * 3.0;
        let serial_count = (((0.0 + BIG) + 1.0) + 0.5) + 3.0;

        assert_ne!(
            serial_sum.to_bits(),
            grouped_sum.to_bits(),
            "fixture must discriminate the two accumulation orders \
             (serial {serial_sum} vs grouped {grouped_sum})"
        );
        assert_eq!(
            trimmed.sum.to_bits(),
            serial_sum.to_bits(),
            "sum must be the serial-order accumulation \
             (got {}, serial {serial_sum}, grouped {grouped_sum})",
            trimmed.sum
        );
        assert_eq!(trimmed.count.to_bits(), serial_count.to_bits());
    }

    #[test]
    fn trim_upper_nhcb_last_bucket_pos_inf_is_conservatively_discarded_when_straddled() {
        // NHCB last bucket is (10, +Inf); rhs=15 falls inside it (lower=10
        // < rhs) but the distribution is unknown - entire bucket removed
        // (and drops out on `Compact(0)`); the other two buckets are
        // entirely below rhs and stay unchanged.
        let h = nhcb_hist(3, 3.0, vec![5.0, 10.0], vec![1, 2, 3]);
        let trimmed = h.trim_buckets(15.0, true);
        assert_eq!(trimmed.positive_buckets, vec![1.0, 2.0]);
    }
}
