//! Query-time float-bucket histogram (M7-A5b-i foundation) — ports the
//! pinned Prometheus `model/histogram/float_histogram.go` (v3.13.0,
//! `40af9c2`, via `git show 40af9c2:model/histogram/float_histogram.go`).
//! Upstream evaluates PromQL exclusively on `*FloatHistogram` (never the
//! integer `Histogram`): every eval entrypoint this crate's evaluator
//! calls — `HistogramQuantile` (`promql/quantile.go:222`),
//! `HistogramFraction` (`:394`), the accessors (`promql/functions.go`
//! `simpleHistogramFunc`) — takes a `*FloatHistogram`. [`NativeHistogram`]
//! (the storage/merge integer form, [`crate::histogram`]) converts to this
//! type exactly once, at the read boundary, via [`NativeHistogram::
//! to_float`] (`histogram.go:365` `Histogram.ToFloat`).
//!
//! **Scope note (A5b-i):** only the substrate A5b-i's function set needs is
//! ported — `to_float`, the bucket-boundary/iteration machinery
//! (`AllBucketIterator`/`AllReverseBucketIterator`), `ZeroBucket`,
//! `UsesCustomBuckets`, and [`FloatHistogram::bits_eq`] (the value-model
//! equality primitive). `Add`/`Sub`/`KahanAdd`/`Mul`/`Div`/`CopyToSchema`/
//! `Compact`/`DetectReset`/`Equals` are **not** ported here — those are
//! A5b-ii (range functions, counter resets) and A5b-iii (aggregation,
//! binops) territory, per the locked 3-way split (issue #124 plan v2).
//! `CounterResetHint` is likewise not modeled: A3 stores no
//! `counter_reset_hint` column (`histogram.rs` doc), so every decoded
//! histogram is upstream's `UnknownCounterReset` — a documented, adjudicated
//! gap (OQ2), not something this port can or should paper over.
//!
//! The bucket iterators here are a **semantically-equivalent
//! simplification** of upstream's zero-alloc streaming iterators: rather
//! than porting the generic `baseBucketIterator`/`floatBucketIterator`/
//! `reverseFloatBucketIterator` state machines (built for lazy, allocation-
//! free traversal across a cross-schema merge this crate's A5b-i scope
//! never performs — `targetSchema` always equals `h.Schema` here), this
//! module decodes each side's spans directly into an owned, ascending-index
//! `Vec<Bucket>` (mirroring exactly the running-index walk
//! `floatBucketIterator.Next()`'s fast path performs) and derives
//! [`FloatHistogram::all_buckets`]/[`FloatHistogram::all_buckets_reverse`]
//! from that — see the two functions' docs for the ordering proof.

use crate::histogram::{NativeHistogram, Span, is_custom_buckets_schema};

include!("float_histogram_bounds.rs");

/// A decoded histogram bucket — mirrors upstream `histogram.Bucket[float64]`
/// (`model/histogram/generic.go:128`): absolute (not cumulative) `count`,
/// bounds, and their inclusivity. `index` is the bucket's schema-relative
/// index (irrelevant for the zero bucket, mirroring upstream's own
/// "Irrelevant for the zero bucket" doc).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    pub lower: f64,
    pub upper: f64,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub count: f64,
    pub index: i32,
}

/// Query-time float-bucket histogram — see the module doc. Bucket counts
/// are absolute (not delta-encoded, unlike [`NativeHistogram`]).
#[derive(Debug, Clone)]
pub struct FloatHistogram {
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub positive_spans: Vec<Span>,
    pub negative_spans: Vec<Span>,
    pub positive_buckets: Vec<f64>,
    pub negative_buckets: Vec<f64>,
    pub custom_values: Vec<f64>,
}

impl FloatHistogram {
    /// Mirrors upstream `UsesCustomBuckets` (`float_histogram.go:64`).
    pub fn uses_custom_buckets(&self) -> bool {
        is_custom_buckets_schema(self.schema)
    }

    /// The zero bucket (`[-zero_threshold, zero_threshold]`, both bounds
    /// inclusive) — mirrors upstream `ZeroBucket` (`float_histogram.go:270`).
    /// Meaningless for a custom-bounds (NHCB) histogram (upstream panics;
    /// this port simply never calls it there — [`Self::all_buckets`] only
    /// visits the zero bucket when `zero_count > 0`, and NHCB always
    /// carries `zero_count == 0`, `histogram.rs`'s `validate`).
    pub fn zero_bucket(&self) -> Bucket {
        Bucket {
            lower: -self.zero_threshold,
            upper: self.zero_threshold,
            lower_inclusive: true,
            upper_inclusive: true,
            count: self.zero_count,
            index: 0,
        }
    }

    /// Every bucket (negative, zero, positive) in ascending numeric order —
    /// mirrors upstream `AllBucketIterator` (`float_histogram.go:904`),
    /// including its zero-threshold boundary clamp (a bucket adjacent to
    /// the zero bucket has its zero-facing edge pulled in to
    /// `±zero_threshold` so the two never overlap).
    ///
    /// **Ordering proof (why this needs no separate reverse-iterator
    /// port):** upstream's negative side is walked by a *reverse* iterator
    /// (from the highest schema-index bucket — the most-negative value —
    /// down to the one nearest zero), which is exactly
    /// `natural_side_buckets(negative, ..).reverse()` here, since
    /// `natural_side_buckets` decodes in ascending schema-index order and
    /// index ascends from near-zero outward on the negative side (`Lower =
    /// -getBound(idx)`, and `getBound` is monotonically increasing in
    /// `idx`, so ascending `idx` ⇒ descending — i.e. more negative —
    /// `Lower`). The positive side's forward iterator is already
    /// `natural_side_buckets(positive, ..)` unchanged (ascending `idx` ⇒
    /// ascending `Upper`).
    pub fn all_buckets(&self) -> Vec<Bucket> {
        let mut out =
            Vec::with_capacity(self.negative_buckets.len() + self.positive_buckets.len() + 1);
        let mut negative = natural_side_buckets(
            &self.negative_spans,
            &self.negative_buckets,
            false,
            self.schema,
            &self.custom_values,
        );
        negative.reverse();
        for mut b in negative {
            if b.upper < 0.0 && b.upper > -self.zero_threshold {
                b.upper = -self.zero_threshold;
            } else if b.lower > 0.0 && b.lower < self.zero_threshold {
                b.lower = self.zero_threshold;
            }
            out.push(b);
        }
        if self.zero_count > 0.0 {
            out.push(self.zero_bucket());
        }
        let positive = natural_side_buckets(
            &self.positive_spans,
            &self.positive_buckets,
            true,
            self.schema,
            &self.custom_values,
        );
        for mut b in positive {
            if b.lower > 0.0 && b.lower < self.zero_threshold {
                b.lower = self.zero_threshold;
            } else if b.upper < 0.0 && b.upper > -self.zero_threshold {
                b.upper = -self.zero_threshold;
            }
            out.push(b);
        }
        out
    }

    /// Every bucket in descending numeric order — mirrors upstream
    /// `AllReverseBucketIterator` (`float_histogram.go:918`). Provably the
    /// exact reverse of [`Self::all_buckets`]'s sequence: both describe the
    /// same total order over the same bucket set (list reversal is
    /// associative — `reverse(reverse(neg) ++ zero ++ pos) == reverse(pos)
    /// ++ zero ++ neg`, which is precisely upstream's reverse-positive +
    /// zero + forward-negative construction), so no separate walk is
    /// ported.
    pub fn all_buckets_reverse(&self) -> Vec<Bucket> {
        let mut v = self.all_buckets();
        v.reverse();
        v
    }

    /// Bitwise equality: `count`/`sum`/`zero_threshold` and every bucket
    /// (positive/negative/`custom_values`) by [`f64::to_bits`] (NaN-safe,
    /// deterministic); `schema` by value. The value-model equality
    /// primitive `pulsus-promql`'s hand-written `PartialEq` compares
    /// through (mirrors [`NativeHistogram::bits_eq`]). **Distinct from**
    /// upstream `FloatHistogram.Equals` (`float_histogram.go:606`), which
    /// is compaction-sensitive *semantic* equality for the `h==h` binop
    /// (A5b-iii) — not ported here.
    pub fn bits_eq(&self, other: &FloatHistogram) -> bool {
        fn bits_eq_slice(a: &[f64], b: &[f64]) -> bool {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
        }
        self.schema == other.schema
            && self.zero_threshold.to_bits() == other.zero_threshold.to_bits()
            && self.zero_count.to_bits() == other.zero_count.to_bits()
            && self.count.to_bits() == other.count.to_bits()
            && self.sum.to_bits() == other.sum.to_bits()
            && self.positive_spans == other.positive_spans
            && self.negative_spans == other.negative_spans
            && bits_eq_slice(&self.positive_buckets, &other.positive_buckets)
            && bits_eq_slice(&self.negative_buckets, &other.negative_buckets)
            && bits_eq_slice(&self.custom_values, &other.custom_values)
    }
}

impl NativeHistogram {
    /// Converts the stored integer histogram to its query-time float form
    /// — a lossless (within f64 precision) port of upstream
    /// `Histogram.ToFloat` (`histogram.go:365`): widens `zero_count`/
    /// `count` to `f64`, copies `sum` verbatim (including a `STALE_NAN`
    /// bit pattern — the eval-boundary staleness marker,
    /// `Sample::is_stale`), and delta-decodes each side's buckets into
    /// absolute (not cumulative) `f64` counts. NHCB (custom-bounds,
    /// `is_custom_buckets`) clears the zero/negative fields and interns
    /// `custom_values`; exponential clears `custom_values` and copies
    /// `zero_threshold`/`negative_*` as-is.
    ///
    /// Delta-decode uses `wrapping_add` (never panics on corrupt storage —
    /// the `check_histogram_buckets` convention this module already
    /// follows); a validated histogram (A4 ingest validates before write)
    /// never overflows in practice.
    pub fn to_float(&self) -> FloatHistogram {
        let positive_buckets = decode_deltas(&self.positive_buckets);
        if self.is_custom_buckets() {
            FloatHistogram {
                schema: self.schema,
                zero_threshold: 0.0,
                zero_count: 0.0,
                count: self.count as f64,
                sum: self.sum,
                positive_spans: self.positive_spans.clone(),
                negative_spans: Vec::new(),
                positive_buckets,
                negative_buckets: Vec::new(),
                custom_values: self.custom_values.clone(),
            }
        } else {
            FloatHistogram {
                schema: self.schema,
                zero_threshold: self.zero_threshold,
                zero_count: self.zero_count as f64,
                count: self.count as f64,
                sum: self.sum,
                positive_spans: self.positive_spans.clone(),
                negative_spans: self.negative_spans.clone(),
                positive_buckets,
                negative_buckets: decode_deltas(&self.negative_buckets),
                custom_values: Vec::new(),
            }
        }
    }
}

/// Delta-decode a `*_buckets` column (first element absolute, the rest
/// deltas — `NativeHistogram`'s doc) into absolute `f64` counts, mirroring
/// upstream's `ToFloat` accumulation loop EXACTLY (`histogram.go:397-400`:
/// `var currentPositive float64; currentPositive += float64(b)` — each
/// i64 delta is widened to f64 FIRST and accumulated in f64, so per-step
/// f64 rounding above 2^53 matches upstream bit-for-bit; an i64
/// accumulator that casts at the end would round differently there and
/// could wrap on overflow, which f64 accumulation never does).
fn decode_deltas(deltas: &[i64]) -> Vec<f64> {
    let mut running: f64 = 0.0;
    deltas
        .iter()
        .map(|&d| {
            running += d as f64;
            running
        })
        .collect()
}

/// Decodes `spans`/`buckets` into a `Vec<Bucket>` in ascending schema-index
/// order (the order the arrays themselves are stored in) — a direct,
/// allocation-owning port of upstream `floatBucketIterator.Next()`'s fast
/// path (`schema == targetSchema`, `float_histogram.go:1218-1245`; A5b-i
/// never merges across schemas, so the cross-schema merge branch is not
/// ported). `positive` selects the bound formula
/// ([`Self::bucket_at`]-equivalent, `at()` in the pin,
/// `generic.go:200-222`).
fn natural_side_buckets(
    spans: &[Span],
    buckets: &[f64],
    positive: bool,
    schema: i32,
    custom_values: &[f64],
) -> Vec<Bucket> {
    let mut out = Vec::with_capacity(buckets.len());
    if spans.is_empty() || buckets.is_empty() {
        return out;
    }
    let mut span_idx = 0usize;
    let mut idx_in_span: u32 = 0;
    let mut curr_idx: i32 = spans[0].offset;
    for (bucket_idx, &count) in buckets.iter().enumerate() {
        if bucket_idx != 0 {
            curr_idx += 1;
        }
        while idx_in_span >= spans[span_idx].length {
            idx_in_span = 0;
            span_idx += 1;
            if span_idx >= spans.len() {
                // Defensive: more buckets than the spans cover (only
                // reachable for a corrupt/invalid histogram — A4 validates
                // at ingest). Stop rather than index out of range, mirroring
                // upstream's own `bucketsIdx >= len(buckets)` guard intent.
                return out;
            }
            curr_idx += spans[span_idx].offset;
        }
        out.push(bucket_at(curr_idx, schema, custom_values, positive, count));
        idx_in_span += 1;
    }
    out
}

/// One bucket's bounds/inclusivity/count at schema-index `idx` — mirrors
/// upstream `baseBucketIterator.at` (`generic.go:200-222`).
fn bucket_at(idx: i32, schema: i32, custom_values: &[f64], positive: bool, count: f64) -> Bucket {
    let (lower, upper) = if positive {
        (
            get_bound(idx - 1, schema, custom_values),
            get_bound(idx, schema, custom_values),
        )
    } else {
        (
            -get_bound(idx, schema, custom_values),
            -get_bound(idx - 1, schema, custom_values),
        )
    };
    let (lower_inclusive, upper_inclusive) = if is_custom_buckets_schema(schema) {
        (idx == 0, true)
    } else {
        (lower < 0.0, upper > 0.0)
    };
    Bucket {
        lower,
        upper,
        lower_inclusive,
        upper_inclusive,
        count,
        index: idx,
    }
}

/// The bucket boundary at schema-relative index `idx` — mirrors upstream
/// `getBound` (`generic.go:549`). For NHCB (`schema == CUSTOM_BUCKETS_SCHEMA`),
/// `idx == -1` is `-Inf`, `idx == len(custom_values)` is `+Inf`, otherwise
/// `custom_values[idx]`. Otherwise delegates to the exponential formula.
fn get_bound(idx: i32, schema: i32, custom_values: &[f64]) -> f64 {
    if is_custom_buckets_schema(schema) {
        let length = custom_values.len() as i32;
        if idx == length {
            return f64::INFINITY;
        }
        if idx == -1 {
            return f64::NEG_INFINITY;
        }
        if idx < -1 || idx > length {
            // Unreachable for a validated histogram (A4 ingest validates
            // custom bounds cover every span); never panic on untrusted
            // data — NaN propagates harmlessly through downstream math.
            debug_assert!(false, "custom bucket index {idx} out of range 0..{length}");
            return f64::NAN;
        }
        return custom_values[idx as usize];
    }
    get_bound_exponential(idx, schema)
}

/// Mirrors upstream `getBoundExponential` (`generic.go:566`) — see the
/// pin's own extensive comment for why the last regular bucket's upper
/// bound is clamped to `f64::MAX` rather than the formula's natural `+Inf`
/// (so `+Inf` observations land in a distinct "inf bucket" one index
/// higher).
fn get_bound_exponential(idx: i32, schema: i32) -> f64 {
    if schema < 0 {
        let exp = (idx as i64) << (-schema) as u32;
        if exp == 1024 {
            return f64::MAX;
        }
        return ldexp(1.0, exp);
    }
    let frac_idx = (idx & ((1i32 << schema) - 1)) as usize;
    let frac = EXPONENTIAL_BOUNDS[schema as usize][frac_idx];
    let exp = (idx >> schema) as i64 + 1;
    if frac == 0.5 && exp == 1025 {
        return f64::MAX;
    }
    ldexp(frac, exp)
}

/// `frac * 2^exp`, IEEE-754 exact scale (Go's `math.Ldexp`) — built via a
/// direct exponent-field bit shift rather than a `powi` multiply, so it is
/// bit-exact for every normal-range result (every bucket boundary this
/// crate's schemas −4..=8 / NHCB actually produce). Our callers only ever
/// pass a normal, non-zero, finite `frac` (an `EXPONENTIAL_BOUNDS` entry in
/// `[0.5, 1)`, or the literal `1.0`), so `math.Ldexp`'s zero/subnormal/Inf/
/// NaN `frac` special-casing is unreachable here and not ported; an
/// underflowing `exp` (also unreachable for any realistic bucket schema)
/// falls back to an exact-for-the-normal-range multiply rather than
/// panicking.
fn ldexp(frac: f64, exp: i64) -> f64 {
    let bits = frac.to_bits();
    let biased_exp = ((bits >> 52) & 0x7ff) as i64;
    let new_biased_exp = biased_exp + exp;
    if new_biased_exp >= 0x7ff {
        return f64::INFINITY;
    }
    if new_biased_exp <= 0 {
        return frac * 2f64.powi(exp.clamp(i32::MIN as i64, i32::MAX as i64) as i32);
    }
    let new_bits = (bits & !(0x7ffu64 << 52)) | ((new_biased_exp as u64) << 52);
    f64::from_bits(new_bits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::histogram::CUSTOM_BUCKETS_SCHEMA;

    /// `single_histogram {{schema:0 sum:5 count:4 buckets:[1 2 1]}}`
    /// (`native_histograms.test:34`), the A3 corpus fixture also used by
    /// `histogram.rs`'s own round-trip tests. Absolute buckets `[1 2 1]`
    /// delta-encode to `[1 1 -1]`.
    fn single_histogram() -> NativeHistogram {
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
    }

    /// `custom_buckets_histogram {{schema:-53 sum:5 count:4
    /// custom_values:[5 10] buckets:[1 2 1]}}`
    /// (`native_histograms.test:1078`).
    fn custom_buckets_histogram() -> NativeHistogram {
        NativeHistogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
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
    }

    // -- AC1: to_float + bucket-iterator correctness --

    #[test]
    fn to_float_widens_scalars_and_decodes_deltas_to_absolute_counts() {
        let fh = single_histogram().to_float();
        assert_eq!(fh.schema, 0);
        assert_eq!(fh.count, 4.0);
        assert_eq!(fh.sum, 5.0);
        assert_eq!(fh.zero_threshold, 0.0);
        assert_eq!(fh.zero_count, 0.0);
        // Deltas [1, 1, -1] -> absolute [1, 2, 1].
        assert_eq!(fh.positive_buckets, vec![1.0, 2.0, 1.0]);
        assert!(fh.negative_buckets.is_empty());
        assert!(fh.custom_values.is_empty());
    }

    #[test]
    fn to_float_is_lossless_on_sum_including_a_nan_bit_pattern() {
        let mut h = single_histogram();
        h.sum = f64::from_bits(crate::STALE_NAN_BITS);
        let fh = h.to_float();
        assert_eq!(fh.sum.to_bits(), crate::STALE_NAN_BITS);
    }

    #[test]
    fn to_float_nhcb_clears_zero_and_negative_fields_and_interns_custom_values() {
        let fh = custom_buckets_histogram().to_float();
        assert_eq!(fh.schema, CUSTOM_BUCKETS_SCHEMA);
        assert_eq!(fh.zero_threshold, 0.0);
        assert_eq!(fh.zero_count, 0.0);
        assert!(fh.negative_buckets.is_empty());
        assert_eq!(fh.custom_values, vec![5.0, 10.0]);
        assert_eq!(fh.positive_buckets, vec![1.0, 2.0, 1.0]);
    }

    #[test]
    fn all_buckets_single_histogram_yields_schema_0_boundaries_ascending() {
        let fh = single_histogram().to_float();
        let buckets = fh.all_buckets();
        assert_eq!(buckets.len(), 3);
        // Schema 0: bucket boundaries are powers of 2 (get_bound(idx) =
        // 2^idx); `positive_spans: [{offset:0, length:3}]` starts at
        // schema-index 0, so the three buckets are (0.5,1], (1,2], (2,4].
        assert_eq!(buckets[0].lower, 0.5);
        assert_eq!(buckets[0].upper, 1.0);
        assert_eq!(buckets[0].count, 1.0);
        assert_eq!(buckets[1].lower, 1.0);
        assert_eq!(buckets[1].upper, 2.0);
        assert_eq!(buckets[1].count, 2.0);
        assert_eq!(buckets[2].lower, 2.0);
        assert_eq!(buckets[2].upper, 4.0);
        assert_eq!(buckets[2].count, 1.0);
        for b in &buckets {
            assert!(
                !b.lower_inclusive,
                "positive exponential lower bound is open"
            );
            assert!(
                b.upper_inclusive,
                "positive exponential upper bound is closed"
            );
        }
    }

    #[test]
    fn all_buckets_reverse_is_the_exact_reverse_of_all_buckets() {
        let fh = single_histogram().to_float();
        let mut forward = fh.all_buckets();
        let reverse = fh.all_buckets_reverse();
        forward.reverse();
        assert_eq!(forward.len(), reverse.len());
        for (a, b) in forward.iter().zip(&reverse) {
            assert_eq!(a.lower, b.lower);
            assert_eq!(a.upper, b.upper);
            assert_eq!(a.count, b.count);
        }
    }

    #[test]
    fn all_buckets_nhcb_uses_custom_bounds_with_closed_right_open_left() {
        let fh = custom_buckets_histogram().to_float();
        let buckets = fh.all_buckets();
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].lower, f64::NEG_INFINITY);
        assert_eq!(buckets[0].upper, 5.0);
        assert!(
            buckets[0].lower_inclusive,
            "first NHCB bucket is [-Inf, ...]"
        );
        assert!(buckets[0].upper_inclusive);
        assert_eq!(buckets[1].lower, 5.0);
        assert_eq!(buckets[1].upper, 10.0);
        assert!(!buckets[1].lower_inclusive);
        assert_eq!(buckets[2].lower, 10.0);
        assert_eq!(buckets[2].upper, f64::INFINITY);
    }

    #[test]
    fn all_buckets_negative_and_zero_bucket_interleave_ascending() {
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.5,
            zero_count: 3,
            count: 5,
            sum: 0.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            positive_buckets: vec![1],
            negative_buckets: vec![1],
            custom_values: vec![],
        };
        let fh = h.to_float();
        let buckets = fh.all_buckets();
        // negative bucket (schema-index 0: (-1,-0.5]) -> zero bucket
        // ((-0.5,0.5], neither edge close enough to the zero-adjacent
        // bucket bounds to clamp) -> positive bucket (index 0: (0.5,1]);
        // ascending numeric order throughout.
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].lower, -1.0);
        assert_eq!(buckets[0].upper, -0.5);
        assert_eq!(buckets[1].lower, -0.5);
        assert_eq!(buckets[1].upper, 0.5);
        assert_eq!(buckets[1].count, 3.0);
        assert_eq!(buckets[2].lower, 0.5);
        assert_eq!(buckets[2].upper, 1.0);
    }

    #[test]
    fn zero_count_zero_skips_the_zero_bucket_entirely() {
        let mut h = single_histogram();
        h.zero_count = 0;
        let fh = h.to_float();
        assert_eq!(fh.all_buckets().len(), 3, "no zero bucket emitted");
    }

    /// Regression for the review finding at `float_histogram.rs:253`
    /// (`#124` codex review): `decode_deltas` must accumulate each `i64`
    /// delta as `f64` on every step — matching `Histogram.ToFloat`'s
    /// `currentPositive += float64(b)` (`histogram.go:397-400`) — not
    /// accumulate exactly in `i64` and cast per-step (or at the end). Once
    /// the running cumulative count exceeds `2^53` (the largest integer
    /// exactly representable in `f64`), the two strategies diverge: the
    /// per-delta-`f64` accumulator's rounding error compounds forward
    /// (every subsequent `+1` delta is silently absorbed once the running
    /// total is no longer representable exactly), whereas an exact `i64`
    /// accumulator re-rounds a fresh exact value at every step and does
    /// NOT plateau the same way. Deltas chosen so the two strategies
    /// produce different bucket values from the third bucket onward
    /// (verified independently in Python against both strategies).
    #[test]
    fn to_float_accumulates_deltas_in_f64_matching_upstream_above_two_pow_53() {
        let deltas = vec![1i64 << 53, 1, 1, 1, 1, 1];
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: deltas.len() as u64,
            sum: 0.0,
            positive_spans: vec![Span {
                offset: 0,
                length: deltas.len() as u32,
            }],
            negative_spans: vec![],
            positive_buckets: deltas,
            negative_buckets: vec![],
            custom_values: vec![],
        };
        let fh = h.to_float();
        // Correct: per-delta f64 accumulation. The running total saturates
        // at 2^53 once it stops being exactly representable, so every
        // bucket from the second one onward reads the same plateaued value.
        let expected = [
            9007199254740992.0,
            9007199254740992.0,
            9007199254740992.0,
            9007199254740992.0,
            9007199254740992.0,
            9007199254740992.0,
        ];
        assert_eq!(fh.positive_buckets, expected);
        // Wrong (guarded against): an exact i64 running accumulator cast
        // to f64 at each step keeps climbing past the plateau instead.
        let wrong_i64_then_cast = [
            9007199254740992.0,
            9007199254740992.0,
            9007199254740994.0,
            9007199254740996.0,
            9007199254740996.0,
            9007199254740996.0,
        ];
        assert_ne!(fh.positive_buckets, wrong_i64_then_cast);
    }

    // -- FloatHistogram::bits_eq (value-model equality primitive) --

    #[test]
    fn bits_eq_true_for_bit_identical_float_histograms() {
        let a = single_histogram().to_float();
        let b = single_histogram().to_float();
        assert!(a.bits_eq(&b));
    }

    #[test]
    fn bits_eq_false_for_one_bucket_difference() {
        let a = single_histogram().to_float();
        let mut b = single_histogram().to_float();
        b.positive_buckets[1] = 999.0;
        assert!(!a.bits_eq(&b));
    }

    #[test]
    fn bits_eq_true_for_two_identical_stale_nan_sums() {
        let mut h1 = single_histogram();
        h1.sum = f64::from_bits(crate::STALE_NAN_BITS);
        let mut h2 = single_histogram();
        h2.sum = f64::from_bits(crate::STALE_NAN_BITS);
        assert!(h1.to_float().bits_eq(&h2.to_float()));
    }

    #[test]
    fn bits_eq_false_for_nan_sum_vs_finite_sum() {
        let mut h = single_histogram();
        h.sum = f64::from_bits(crate::STALE_NAN_BITS);
        assert!(!h.to_float().bits_eq(&single_histogram().to_float()));
    }

    #[test]
    fn bits_eq_false_for_differing_custom_values_bits() {
        let a = custom_buckets_histogram().to_float();
        let mut b = custom_buckets_histogram().to_float();
        b.custom_values[1] = 10.000000000000002;
        assert!(!a.bits_eq(&b));
    }

    // -- get_bound_exponential / ldexp: schema-base boundary spot checks --

    #[test]
    fn get_bound_exponential_schema_0_doubles_each_step() {
        assert_eq!(get_bound_exponential(0, 0), 1.0);
        assert_eq!(get_bound_exponential(1, 0), 2.0);
        assert_eq!(get_bound_exponential(-1, 0), 0.5);
    }

    #[test]
    fn get_bound_exponential_schema_2_matches_the_precalculated_table() {
        // Schema 2 has 4 sub-buckets per octave: bounds at
        // 0.5, 0.5946..., 0.7071..., 0.8409... within [0.5, 1).
        assert_eq!(get_bound_exponential(-4, 2), 0.5);
        assert!((get_bound_exponential(-3, 2) - 0.5946035575013605).abs() < 1e-15);
        assert_eq!(get_bound_exponential(0, 2), 1.0);
    }

    #[test]
    fn ldexp_matches_manual_power_of_two_scaling() {
        assert_eq!(ldexp(1.0, 10), 1024.0);
        assert_eq!(ldexp(0.5, 1), 1.0);
        assert_eq!(ldexp(1.0, -1), 0.5);
    }
}
