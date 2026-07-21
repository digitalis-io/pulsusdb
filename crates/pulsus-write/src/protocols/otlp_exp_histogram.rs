//! OTLP `ExponentialHistogramDataPoint` → A3 [`NativeHistogram`] conversion
//! (M7-A4, issue #120). A pure, panic-free, isolated module: it turns the
//! dense OTLP bucket representation (a per-side `{offset, bucket_counts}`
//! contiguous run) into the Prometheus integer sparse form A3 stores
//! (schema + spans + delta-encoded buckets), including the `scale > 8`
//! downscale-to-schema-8 merge. Everything is checked arithmetic: a
//! malformed/overflowing wire data point is **rejected** into partial
//! success ([`ExpReject`]), never wrapped or panicked. `validate()` runs at
//! the ingest seam (`otlp_metrics::emit_native_exponential_histogram`)
//! after this conversion; the aggregate count-equality cross-check lives
//! here so the whole "is this a storable native histogram" decision is one
//! unit-tested unit.
//!
//! Prometheus/OTLP are named interop targets only — nothing is linked or
//! imported from them; the semantics are re-derived against this repo's own
//! classic exponential-histogram flatten (`otlp_metrics.rs`) and the A3
//! value model.

use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets;
use opentelemetry_proto::tonic::metrics::v1::{DataPointFlags, ExponentialHistogramDataPoint};
use pulsus_model::{
    EXPONENTIAL_SCHEMA_MAX, EXPONENTIAL_SCHEMA_MIN, NativeHistogram, STALE_NAN_BITS, Span,
};

/// Why an OTLP exponential-histogram data point cannot be stored as a native
/// histogram. Each variant is rejected into partial success (the whole data
/// point emits no sample) — never a wrap/panic. The wording is turned into
/// the `ParsedMetrics::rejected_message` at the ingest seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExpReject {
    /// `scale < -4` (below Prometheus's minimum exponential schema).
    ScaleTooLow(i32),
    /// A Prometheus bucket index / span offset / span length does not fit
    /// its `i32`/`u32`/`usize` storage width.
    OffsetOverflow,
    /// A merged absolute bucket count, or a `u64`→`i64` bucket count, does
    /// not fit `i64` (delta encoding is `i64`).
    CountOverflow,
    /// A delta between two adjacent absolute counts does not fit `i64`.
    DeltaOverflow,
    /// The summed source bucket counts (positive + negative + zero) do not
    /// equal the reported `count`.
    CountMismatch {
        /// The data point's reported `count`.
        expected: u64,
        /// The checked sum of all bucket counts + `zero_count`.
        actual: u64,
    },
}

impl std::fmt::Display for ExpReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpReject::ScaleTooLow(scale) => write!(
                f,
                "exponential histogram scale {scale} is below the minimum schema \
                 {EXPONENTIAL_SCHEMA_MIN}"
            ),
            ExpReject::OffsetOverflow => {
                write!(
                    f,
                    "exponential histogram bucket index/offset overflows its storage width"
                )
            }
            ExpReject::CountOverflow => {
                write!(
                    f,
                    "exponential histogram bucket count overflows i64 during conversion"
                )
            }
            ExpReject::DeltaOverflow => {
                write!(
                    f,
                    "exponential histogram bucket delta overflows i64 during encoding"
                )
            }
            ExpReject::CountMismatch { expected, actual } => write!(
                f,
                "exponential histogram bucket counts sum to {actual} but count={expected}"
            ),
        }
    }
}

/// `true` when `flags` carries the OTLP `NoRecordedValue` staleness marker.
fn is_stale(flags: u32) -> bool {
    flags & DataPointFlags::NoRecordedValueMask as u32 != 0
}

/// The whole-histogram stale marker: an empty exponential histogram carrying
/// the canonical stale-NaN bit pattern as its `sum`. `validate()` accepts it
/// (NaN sum, `sum_of_buckets(0) <= count(0)`), and the aggregate count check
/// trivially passes (`0 == 0`).
fn stale_marker() -> NativeHistogram {
    NativeHistogram {
        counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
        schema: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        count: 0,
        sum: f64::from_bits(STALE_NAN_BITS),
        positive_spans: Vec::new(),
        negative_spans: Vec::new(),
        positive_buckets: Vec::new(),
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

/// Maps OTLP `scale` to the stored A3 `schema` plus a downscale amount:
/// `scale ∈ [-4, 8]` stores verbatim (`scale_down = 0`); `scale > 8`
/// downscales to schema 8 by merging `2^(scale-8)` source buckets; `scale <
/// -4` rejects (Prometheus's minimum exponential schema is -4).
fn schema_and_scale_down(scale: i32) -> Result<(i32, u32), ExpReject> {
    if scale < EXPONENTIAL_SCHEMA_MIN {
        return Err(ExpReject::ScaleTooLow(scale));
    }
    if scale <= EXPONENTIAL_SCHEMA_MAX {
        Ok((scale, 0))
    } else {
        // `scale > 8`, so `scale - 8` is positive and fits `u32`
        // (`scale <= i32::MAX`).
        Ok((
            EXPONENTIAL_SCHEMA_MAX,
            (scale - EXPONENTIAL_SCHEMA_MAX) as u32,
        ))
    }
}

/// The absolute exponential bucket index of the `j`-th entry of a side with
/// `offset` — `offset + j`, widened to `i64` with checked arithmetic (a
/// crafted `offset`/`j` near the type bounds rejects rather than wraps).
fn abs_index(offset: i32, j: usize) -> Result<i64, ExpReject> {
    let j = i64::try_from(j).map_err(|_| ExpReject::OffsetOverflow)?;
    i64::from(offset)
        .checked_add(j)
        .ok_or(ExpReject::OffsetOverflow)
}

/// The merged (downscaled) index of absolute index `i`: arithmetic floor
/// `i >> scale_down`. `i64::checked_shr` returns `None` for `scale_down >=
/// 64`, which collapses every value into the sign bucket (`-1` for negative
/// `i`, `0` otherwise) — the exact mathematical limit of `floor(i /
/// 2^scale_down)`, with no shift-overflow UB or panic.
fn merged_index(i: i64, scale_down: u32) -> i64 {
    i.checked_shr(scale_down)
        .unwrap_or(if i < 0 { -1 } else { 0 })
}

/// Converts one side's dense OTLP `{offset, bucket_counts}` into a single
/// Prometheus span plus delta-encoded buckets, applying the `scale_down`
/// merge. Returns empty vectors for an absent/empty side or one that trims
/// to all-zero. Internal zero buckets (after merge) stay inside the one
/// dense span, encoded as their true adjacent-count deltas (a `0` count is
/// `-prev` then `+next`, never a literal `0` delta).
fn convert_side(
    buckets: Option<&Buckets>,
    scale_down: u32,
) -> Result<(Vec<Span>, Vec<i64>), ExpReject> {
    let Some(buckets) = buckets else {
        return Ok((Vec::new(), Vec::new()));
    };
    let counts = &buckets.bucket_counts;
    if counts.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let offset = buckets.offset;

    // A contiguous i-run maps to a contiguous m-run (floor is monotone), so
    // the merged accumulation is a dense vector from `m_first..=m_last`.
    let m_first = merged_index(abs_index(offset, 0)?, scale_down);
    let m_last = merged_index(abs_index(offset, counts.len() - 1)?, scale_down);
    let run = m_last
        .checked_sub(m_first)
        .and_then(|d| d.checked_add(1))
        .ok_or(ExpReject::OffsetOverflow)?;
    let run = usize::try_from(run).map_err(|_| ExpReject::OffsetOverflow)?;

    let mut merged: Vec<u64> = vec![0; run];
    for (j, &count) in counts.iter().enumerate() {
        let m = merged_index(abs_index(offset, j)?, scale_down);
        // `m` is monotone non-decreasing in `j` and bounded by `m_last`, so
        // `m - m_first` is a valid `0..run` slot.
        let slot = (m - m_first) as usize;
        merged[slot] = merged[slot]
            .checked_add(count)
            .ok_or(ExpReject::CountOverflow)?;
    }

    // Trim leading/trailing zero merged buckets; an all-zero side stores no
    // span at all.
    let Some(start) = merged.iter().position(|&c| c != 0) else {
        return Ok((Vec::new(), Vec::new()));
    };
    let end = merged
        .iter()
        .rposition(|&c| c != 0)
        .expect("start exists, so a non-zero bucket exists");
    let trimmed = &merged[start..=end];

    // The stored Prometheus index of the first trimmed bucket is
    // `(m_first + start) + 1` (the +1 index convention: Prometheus index =
    // merged absolute index + 1, symmetric on both sides).
    let first_prom = m_first
        .checked_add(start as i64)
        .and_then(|x| x.checked_add(1))
        .ok_or(ExpReject::OffsetOverflow)?;
    let span_offset = i32::try_from(first_prom).map_err(|_| ExpReject::OffsetOverflow)?;
    let span_length = u32::try_from(trimmed.len()).map_err(|_| ExpReject::OffsetOverflow)?;
    let spans = vec![Span {
        offset: span_offset,
        length: span_length,
    }];

    // Delta-encode the trimmed absolute counts: first absolute, the rest
    // successive differences (A3 `check_histogram_buckets` reconstructs the
    // running sum).
    let mut deltas = Vec::with_capacity(trimmed.len());
    let mut prev: i64 = 0;
    for &count in trimmed {
        let ci = i64::try_from(count).map_err(|_| ExpReject::CountOverflow)?;
        deltas.push(ci.checked_sub(prev).ok_or(ExpReject::DeltaOverflow)?);
        prev = ci;
    }

    Ok((spans, deltas))
}

/// The checked sum of every source bucket count (positive + negative) plus
/// `zero_count`. Rejects `CountOverflow` on `u64` overflow (mirrors the
/// classic path's `checked_sum`). The merge preserves totals, so summing the
/// raw OTLP `bucket_counts` equals summing the merged absolute counts.
fn aggregate_count(dp: &ExponentialHistogramDataPoint) -> Result<u64, ExpReject> {
    let mut agg = dp.zero_count;
    if let Some(positive) = &dp.positive {
        for &count in &positive.bucket_counts {
            agg = agg.checked_add(count).ok_or(ExpReject::CountOverflow)?;
        }
    }
    if let Some(negative) = &dp.negative {
        for &count in &negative.bucket_counts {
            agg = agg.checked_add(count).ok_or(ExpReject::CountOverflow)?;
        }
    }
    Ok(agg)
}

/// Converts an OTLP `ExponentialHistogramDataPoint` into an A3
/// [`NativeHistogram`]. Precedence and mapping (normative, issue #120):
///
/// - **stale** (`flags & NoRecordedValue`) → an empty stale marker
///   (`sum = STALE_NAN_BITS`), before any other mapping.
/// - `scale` → `schema`/`scale_down` ([`schema_and_scale_down`];
///   `scale < -4` rejects `ScaleTooLow`).
/// - `sum` **present** → verbatim (bit-preserved); `sum` **absent** →
///   canonical quiet `f64::NAN` (distinct bits from `STALE_NAN_BITS`), never
///   `0.0`.
/// - `zero_threshold`/`zero_count`/`count` verbatim; `min`/`max` ignored (no
///   column); `custom_values` always empty (NHCB is not an OTLP source).
/// - positive/negative `Buckets` → one span + delta buckets per side
///   ([`convert_side`]).
///
/// The aggregate count-equality cross-check runs last (before the seam's
/// `validate()`): the summed source counts must equal `count`, else
/// `CountMismatch`. For a stale marker this is `0 == 0`.
pub(crate) fn to_native_histogram(
    dp: &ExponentialHistogramDataPoint,
) -> Result<NativeHistogram, ExpReject> {
    if is_stale(dp.flags) {
        return Ok(stale_marker());
    }

    let (schema, scale_down) = schema_and_scale_down(dp.scale)?;
    let sum = dp.sum.unwrap_or(f64::NAN);
    let (positive_spans, positive_buckets) = convert_side(dp.positive.as_ref(), scale_down)?;
    let (negative_spans, negative_buckets) = convert_side(dp.negative.as_ref(), scale_down)?;

    let hist = NativeHistogram {
        // Issue #125: every accepted point writes hint 0 (Unknown). OTLP
        // `ExponentialHistogramDataPoint` carries no monotonicity flag and
        // delta temporality is rejected wholesale at the dispatch seam
        // (`otlp_metrics.rs`'s `is_delta` gate), so `Gauge` (3) is
        // unproducible at ingest today — a gauge-capable ingest surface
        // (e.g. remote-write receive) is issue #140.
        counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
        schema,
        zero_threshold: dp.zero_threshold,
        zero_count: dp.zero_count,
        count: dp.count,
        sum,
        positive_spans,
        negative_spans,
        positive_buckets,
        negative_buckets,
        custom_values: Vec::new(),
    };

    let agg = aggregate_count(dp)?;
    if agg != hist.count {
        return Err(ExpReject::CountMismatch {
            expected: hist.count,
            actual: agg,
        });
    }

    Ok(hist)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn side(offset: i32, counts: Vec<u64>) -> Buckets {
        Buckets {
            offset,
            bucket_counts: counts,
        }
    }

    /// Builds a cumulative exp-histogram data point with `zero_threshold =
    /// 0.0` and `flags = 0`; the two tests that need a non-zero
    /// `zero_threshold`/stale flag set those fields directly.
    fn dp(
        scale: i32,
        zero_count: u64,
        count: u64,
        sum: Option<f64>,
        positive: Option<Buckets>,
        negative: Option<Buckets>,
    ) -> ExponentialHistogramDataPoint {
        ExponentialHistogramDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: 0,
            time_unix_nano: 1_700_000_000_000_000_000,
            count,
            sum,
            scale,
            zero_count,
            positive,
            negative,
            flags: 0,
            exemplars: Vec::new(),
            min: None,
            max: None,
            zero_threshold: 0.0,
        }
    }

    /// Reconstructs a side's absolute per-bucket counts from delta-encoded
    /// buckets (the A3 decode: running sum of deltas).
    fn abs_counts(deltas: &[i64]) -> Vec<i64> {
        let mut out = Vec::with_capacity(deltas.len());
        let mut running = 0i64;
        for &d in deltas {
            running += d;
            out.push(running);
        }
        out
    }

    #[test]
    fn scale_zero_positive_negative_and_zero_bucket() {
        // scale 0: positive [1,2] at offset 0 (abs idx 0,1 -> prom 1,2),
        // negative [3] at offset 0 (prom 1), zero_count 4. count=10.
        let mut d = dp(
            0,
            4,
            10,
            Some(2.5),
            Some(side(0, vec![1, 2])),
            Some(side(0, vec![3])),
        );
        d.zero_threshold = 1e-9;
        let h = to_native_histogram(&d).expect("valid");
        assert_eq!(h.schema, 0);
        assert_eq!(h.zero_count, 4);
        assert_eq!(h.count, 10);
        assert_eq!(h.zero_threshold.to_bits(), 1e-9f64.to_bits());
        assert_eq!(h.sum.to_bits(), 2.5f64.to_bits());
        assert_eq!(
            h.positive_spans,
            vec![Span {
                offset: 1,
                length: 2
            }]
        );
        assert_eq!(abs_counts(&h.positive_buckets), vec![1, 2]);
        assert_eq!(
            h.negative_spans,
            vec![Span {
                offset: 1,
                length: 1
            }]
        );
        assert_eq!(abs_counts(&h.negative_buckets), vec![3]);
        h.validate().expect("validates");
    }

    #[test]
    fn internal_zero_bucket_encodes_and_decodes_faithfully() {
        // positive [5,0,3] at offset 0 -> one span length 3, deltas
        // [5,-5,3] decoding to abs [5,0,3]. count = 8.
        let d = dp(0, 0, 8, Some(1.0), Some(side(0, vec![5, 0, 3])), None);
        let h = to_native_histogram(&d).expect("valid");
        assert_eq!(
            h.positive_spans,
            vec![Span {
                offset: 1,
                length: 3
            }]
        );
        assert_eq!(h.positive_buckets, vec![5, -5, 3]);
        assert_eq!(abs_counts(&h.positive_buckets), vec![5, 0, 3]);
        h.validate().expect("validates");
    }

    #[test]
    fn scale_above_eight_downscales_and_merges() {
        // scale 10 -> schema 8, scale_down 2 (merge groups of 4). offset 0,
        // counts [1,1,1,1, 2,2] (abs idx 0..5). merged m = idx>>2:
        // idx 0..3 -> m0 (sum 4), idx 4..5 -> m1 (sum 4). prom index m+1.
        let d = dp(
            10,
            0,
            8,
            Some(3.0),
            Some(side(0, vec![1, 1, 1, 1, 2, 2])),
            None,
        );
        let h = to_native_histogram(&d).expect("valid");
        assert_eq!(h.schema, 8);
        assert_eq!(
            h.positive_spans,
            vec![Span {
                offset: 1,
                length: 2
            }]
        );
        assert_eq!(abs_counts(&h.positive_buckets), vec![4, 4]);
        h.validate().expect("validates");
    }

    #[test]
    fn scale_below_minus_four_rejects() {
        let d = dp(-5, 0, 0, Some(0.0), None, None);
        assert_eq!(
            to_native_histogram(&d).unwrap_err(),
            ExpReject::ScaleTooLow(-5)
        );
    }

    #[test]
    fn stale_flag_builds_marker() {
        let mut d = dp(3, 9, 99, Some(42.0), Some(side(0, vec![1, 2])), None);
        d.zero_threshold = 1.0;
        d.flags = DataPointFlags::NoRecordedValueMask as u32;
        let h = to_native_histogram(&d).expect("stale marker");
        assert_eq!(h.count, 0);
        assert_eq!(h.zero_count, 0);
        assert_eq!(h.zero_threshold, 0.0);
        assert!(h.positive_spans.is_empty());
        assert!(h.positive_buckets.is_empty());
        assert_eq!(h.sum.to_bits(), STALE_NAN_BITS);
        h.validate().expect("stale marker validates");
    }

    #[test]
    fn absent_sum_maps_to_quiet_nan_distinct_from_stale() {
        let d = dp(0, 0, 3, None, Some(side(0, vec![1, 2])), None);
        let h = to_native_histogram(&d).expect("valid");
        assert_eq!(h.sum.to_bits(), f64::NAN.to_bits());
        assert_ne!(h.sum.to_bits(), STALE_NAN_BITS);
        h.validate()
            .expect("absent-sum validates via the NaN branch");
    }

    #[test]
    fn present_sum_is_bit_preserved() {
        let d = dp(0, 0, 1, Some(-0.0), Some(side(0, vec![1])), None);
        let h = to_native_histogram(&d).expect("valid");
        assert_eq!(h.sum.to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn count_mismatch_with_absent_sum_rejects() {
        // buckets sum to 3 (1+2) but count says 99, sum absent.
        let d = dp(0, 0, 99, None, Some(side(0, vec![1, 2])), None);
        assert_eq!(
            to_native_histogram(&d).unwrap_err(),
            ExpReject::CountMismatch {
                expected: 99,
                actual: 3
            }
        );
    }

    #[test]
    fn aggregate_count_overflow_rejects() {
        let d = dp(0, 1, 0, Some(0.0), Some(side(0, vec![u64::MAX])), None);
        assert_eq!(
            to_native_histogram(&d).unwrap_err(),
            ExpReject::CountOverflow
        );
    }

    #[test]
    fn i32_span_offset_overflow_rejects() {
        // offset = i32::MAX, one bucket -> prom index i32::MAX + 1 overflows
        // i32.
        let d = dp(0, 0, 1, Some(0.0), Some(side(i32::MAX, vec![1])), None);
        assert_eq!(
            to_native_histogram(&d).unwrap_err(),
            ExpReject::OffsetOverflow
        );
    }

    #[test]
    fn empty_sides_produce_empty_histogram() {
        let d = dp(0, 0, 0, Some(0.0), None, None);
        let h = to_native_histogram(&d).expect("valid empty");
        assert!(h.positive_spans.is_empty());
        assert!(h.negative_spans.is_empty());
        assert_eq!(h.count, 0);
        h.validate().expect("empty validates");
    }

    #[test]
    fn all_zero_side_trims_to_no_span() {
        // positive counts all zero -> no span; zero_count carries the count.
        let d = dp(0, 5, 5, Some(0.0), Some(side(0, vec![0, 0])), None);
        let h = to_native_histogram(&d).expect("valid");
        assert!(h.positive_spans.is_empty());
        assert!(h.positive_buckets.is_empty());
        assert_eq!(h.zero_count, 5);
        h.validate().expect("validates");
    }

    #[test]
    fn huge_scale_down_collapses_to_sign_bucket_without_panic() {
        // scale huge -> scale_down >= 64 -> checked_shr None -> all positive
        // idx collapse to m=0 (prom 1). counts sum into one bucket.
        let d = dp(
            i32::MAX,
            0,
            6,
            Some(1.0),
            Some(side(0, vec![1, 2, 3])),
            None,
        );
        let h = to_native_histogram(&d).expect("valid");
        assert_eq!(h.schema, 8);
        assert_eq!(
            h.positive_spans,
            vec![Span {
                offset: 1,
                length: 1
            }]
        );
        assert_eq!(abs_counts(&h.positive_buckets), vec![6]);
        h.validate().expect("validates");
    }
}
