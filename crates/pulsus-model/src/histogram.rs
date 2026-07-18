//! Native (sparse) histogram value model shared across ingestion and query
//! engines (docs/architecture.md §2). A metric sample is either a float or a
//! native histogram; this module carries the histogram half plus the lossless
//! split/join to the `metric_hist_samples` value columns.
//!
//! The stored [`NativeHistogram`] mirrors, field-for-field, the pinned
//! Prometheus integer `histogram.Histogram` (schema, zero bucket, spans,
//! delta-encoded buckets, custom bounds) so both exponential (schema −4..=8)
//! and NHCB (schema −53) samples represent exactly. `CounterResetHint` is
//! intentionally not modeled: no storage column exists for it and it is a
//! query-time derivation. Prometheus is a semantics reference only — nothing is
//! linked or imported. This module adds only new types; the float path
//! ([`crate::MetricSample`]) is untouched.

/// Minimum exponential (base-2) bucket schema. Mirrors Prometheus
/// `generic.go` `ExponentialSchemaMin`.
pub const EXPONENTIAL_SCHEMA_MIN: i32 = -4;
/// Maximum exponential (base-2) bucket schema. Mirrors `ExponentialSchemaMax`.
pub const EXPONENTIAL_SCHEMA_MAX: i32 = 8;
/// Custom-bounds (NHCB) schema sentinel. Mirrors `CustomBucketsSchema`.
pub const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

/// Whether `schema` selects NHCB (custom bounds). Mirrors
/// `IsCustomBucketsSchema`.
pub fn is_custom_buckets_schema(schema: i32) -> bool {
    schema == CUSTOM_BUCKETS_SCHEMA
}

/// Whether `schema` is a valid exponential schema. Mirrors
/// `IsExponentialSchema` (`EXPONENTIAL_SCHEMA_MIN..=EXPONENTIAL_SCHEMA_MAX`).
fn is_exponential_schema(schema: i32) -> bool {
    (EXPONENTIAL_SCHEMA_MIN..=EXPONENTIAL_SCHEMA_MAX).contains(&schema)
}

/// A contiguous run of buckets. Mirrors `histogram.Span`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// Gap to the previous span (always positive), or the starting index for
    /// the first span (which may be negative for exponential schemas).
    pub offset: i32,
    /// Number of buckets in the span.
    pub length: u32,
}

/// Integer sparse native histogram — the STORED wire form (mirrors
/// `histogram.Histogram`, minus `CounterResetHint`). `*_buckets` are
/// delta-encoded (first element absolute, the rest deltas relative to the
/// previous) exactly as upstream; they map to the `*_bucket_deltas` columns.
/// NHCB (schema −53) populates `custom_values` and leaves the negative/zero
/// fields empty.
///
/// No `PartialEq` derive: `sum` (and, as a stale marker, other f64s) may be
/// NaN — follow the `MetricSampleRow` precedent and compare bit patterns
/// explicitly (`f64::to_bits`) where equality is needed.
#[derive(Debug, Clone)]
pub struct NativeHistogram {
    /// Bucket schema: `EXPONENTIAL_SCHEMA_MIN..=EXPONENTIAL_SCHEMA_MAX` for
    /// exponential buckets, or `CUSTOM_BUCKETS_SCHEMA` for NHCB.
    pub schema: i32,
    /// Width of the zero bucket (unused for NHCB, must be 0).
    pub zero_threshold: f64,
    /// Observations falling into the zero bucket (unused for NHCB, must be 0).
    pub zero_count: u64,
    /// Total number of observations.
    pub count: u64,
    /// Sum of observations; also the stale marker (may be NaN).
    pub sum: f64,
    /// Spans for positive buckets.
    pub positive_spans: Vec<Span>,
    /// Spans for negative buckets (empty for NHCB).
    pub negative_spans: Vec<Span>,
    /// Delta-encoded positive bucket counts -> `pos_bucket_deltas`.
    pub positive_buckets: Vec<i64>,
    /// Delta-encoded negative bucket counts -> `neg_bucket_deltas` (empty for
    /// NHCB).
    pub negative_buckets: Vec<i64>,
    /// Custom (usually upper) bounds; used only for NHCB (schema −53). Empty
    /// means absent — there is no present-but-empty state in this model.
    pub custom_values: Vec<f64>,
}

/// The `metric_hist_samples` VALUE columns, 1:1 with the catalog CREATE
/// (identity triplet `metric_name`/`fingerprint`/`unix_milli` excluded — those
/// live on the write/read `#[derive(Row)]` wrapper). Plain POD, no Row/serde
/// derive here (keeps `pulsus-model` clickhouse-free; the Row boundary stays in
/// the write/read crates). `schema` is `i8` — the physical column width.
#[derive(Debug, Clone)]
pub struct HistogramColumns {
    /// `schema` narrowed to the physical `Int8` column width.
    pub schema: i8,
    /// `zero_threshold` column.
    pub zero_threshold: f64,
    /// `zero_count` column.
    pub zero_count: u64,
    /// `count` column.
    pub count: u64,
    /// `sum` column.
    pub sum: f64,
    /// `pos_span_offsets` column (parallel to `pos_span_lengths`).
    pub pos_span_offsets: Vec<i32>,
    /// `pos_span_lengths` column (parallel to `pos_span_offsets`).
    pub pos_span_lengths: Vec<u32>,
    /// `pos_bucket_deltas` column.
    pub pos_bucket_deltas: Vec<i64>,
    /// `neg_span_offsets` column (parallel to `neg_span_lengths`).
    pub neg_span_offsets: Vec<i32>,
    /// `neg_span_lengths` column (parallel to `neg_span_offsets`).
    pub neg_span_lengths: Vec<u32>,
    /// `neg_bucket_deltas` column.
    pub neg_bucket_deltas: Vec<i64>,
    /// `custom_values` column.
    pub custom_values: Vec<f64>,
}

/// Errors from histogram conversion and validation. One variant per upstream
/// error class; the message wording is local to this crate.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HistogramError {
    /// `schema` does not fit the `Int8` storage column (defensive; never
    /// silently truncates).
    #[error("schema {0} does not fit the Int8 storage column")]
    SchemaOutOfRange(i32),
    /// A column pair that must be parallel differs in length (decode side).
    #[error("{side} span offsets ({offsets}) and lengths ({lengths}) differ in count")]
    SpanArrayLengthMismatch {
        /// `"positive"` or `"negative"`.
        side: &'static str,
        /// Number of offsets.
        offsets: usize,
        /// Number of lengths.
        lengths: usize,
    },
    /// `schema` is neither a valid exponential schema nor NHCB.
    #[error(
        "invalid schema {0}: must be {EXPONENTIAL_SCHEMA_MIN}..={EXPONENTIAL_SCHEMA_MAX} or {CUSTOM_BUCKETS_SCHEMA}"
    )]
    InvalidSchema(i32),
    /// Sum of span lengths on a side does not match its bucket count.
    #[error("{side} span lengths sum ({expected}) != bucket count ({actual})")]
    SpanBucketCountMismatch {
        /// `"positive"` or `"negative"`.
        side: &'static str,
        /// Sum of span lengths.
        expected: u64,
        /// Number of buckets provided.
        actual: usize,
    },
    /// A span offset is negative where upstream forbids it (any span for NHCB;
    /// a subsequent span for exponential).
    #[error("{side} span {span_index} has a negative offset")]
    SpanNegativeOffset {
        /// `"positive"` or `"negative"`.
        side: &'static str,
        /// Zero-based index of the offending span.
        span_index: usize,
    },
    /// A delta-decoded running bucket count went negative.
    #[error("{side} bucket {bucket_index} has a negative observation count")]
    NegativeBucketCount {
        /// `"positive"` or `"negative"`.
        side: &'static str,
        /// Zero-based index of the offending bucket.
        bucket_index: usize,
    },
    /// An NHCB custom bound is NaN.
    #[error("custom bounds must not be NaN")]
    CustomBoundsNaN,
    /// NHCB custom bounds are not strictly increasing.
    #[error("custom bounds must be strictly increasing")]
    CustomBoundsNotIncreasing,
    /// The last NHCB custom bound is explicitly +Inf.
    #[error("last custom bound must not be explicitly +Inf")]
    CustomBoundsInfinite,
    /// Too few NHCB custom bounds to cover the spans.
    #[error("custom bounds too few: have {have}, need {need}")]
    CustomBoundsTooFew {
        /// Number of bounds present.
        have: usize,
        /// Total span length that must be covered.
        need: usize,
    },
    /// NHCB has a non-zero `zero_count`.
    #[error("custom buckets must have a zero count of 0")]
    CustomBucketsZeroCount,
    /// NHCB has a non-zero `zero_threshold`.
    #[error("custom buckets must have a zero threshold of 0")]
    CustomBucketsZeroThreshold,
    /// NHCB has negative spans.
    #[error("custom buckets must not have negative spans")]
    CustomBucketsNegativeSpans,
    /// NHCB has negative buckets.
    #[error("custom buckets must not have negative buckets")]
    CustomBucketsNegativeBuckets,
    /// An exponential-schema histogram carries custom bounds.
    #[error("exponential schema must not have custom bounds")]
    ExponentialSchemaCustomBounds,
    /// Finite `sum`, but bucket observations do not equal `count`.
    #[error("bucket observations ({buckets}) != count ({count})")]
    CountMismatch {
        /// Observations found in buckets.
        buckets: u64,
        /// The `count` field.
        count: u64,
    },
    /// NaN `sum`, but bucket observations exceed `count`.
    #[error("bucket observations ({buckets}) exceed count ({count})")]
    CountNotBigEnough {
        /// Observations found in buckets.
        buckets: u64,
        /// The `count` field.
        count: u64,
    },
}

/// Split `Vec<Span>` into the parallel offset/length arrays the columns store.
fn split_spans(spans: &[Span]) -> (Vec<i32>, Vec<u32>) {
    let mut offsets = Vec::with_capacity(spans.len());
    let mut lengths = Vec::with_capacity(spans.len());
    for span in spans {
        offsets.push(span.offset);
        lengths.push(span.length);
    }
    (offsets, lengths)
}

/// Join parallel offset/length arrays back into `Vec<Span>`. The only decode
/// side structural failure: the arrays cannot rebuild spans if they differ in
/// length.
fn join_spans(
    side: &'static str,
    offsets: &[i32],
    lengths: &[u32],
) -> Result<Vec<Span>, HistogramError> {
    if offsets.len() != lengths.len() {
        return Err(HistogramError::SpanArrayLengthMismatch {
            side,
            offsets: offsets.len(),
            lengths: lengths.len(),
        });
    }
    Ok(offsets
        .iter()
        .zip(lengths)
        .map(|(&offset, &length)| Span { offset, length })
        .collect())
}

/// Port of upstream `checkHistogramSpans`: a subsequent span (index > 0) must
/// have a non-negative offset (the first may be negative), and the span-length
/// sum must equal the bucket count.
fn check_exponential_spans(
    spans: &[Span],
    num_buckets: usize,
    side: &'static str,
) -> Result<(), HistogramError> {
    let mut span_buckets: u64 = 0;
    for (n, span) in spans.iter().enumerate() {
        if n > 0 && span.offset < 0 {
            return Err(HistogramError::SpanNegativeOffset {
                side,
                span_index: n,
            });
        }
        span_buckets = span_buckets.wrapping_add(u64::from(span.length));
    }
    if span_buckets != num_buckets as u64 {
        return Err(HistogramError::SpanBucketCountMismatch {
            side,
            expected: span_buckets,
            actual: num_buckets,
        });
    }
    Ok(())
}

/// Port of upstream `checkHistogramBuckets` with `deltas=true`: delta-decode
/// the running bucket count, rejecting a negative running total and
/// accumulating the observation count.
fn check_histogram_buckets(
    buckets: &[i64],
    count: &mut u64,
    side: &'static str,
) -> Result<(), HistogramError> {
    let mut last: i64 = 0;
    for (i, &delta) in buckets.iter().enumerate() {
        // Wrapping to mirror upstream Go's int64/uint64 arithmetic: untrusted
        // deltas must not panic (validate runs on ingest input). An overflow
        // that wraps the running count negative is still rejected below.
        let c = last.wrapping_add(delta);
        if c < 0 {
            return Err(HistogramError::NegativeBucketCount {
                side,
                bucket_index: i,
            });
        }
        last = c;
        *count = count.wrapping_add(c as u64);
    }
    Ok(())
}

/// Port of upstream `checkHistogramCustomBounds`: bounds are non-NaN, strictly
/// increasing, and not trailed by an explicit +Inf; every span offset is
/// non-negative (all spans, including the first); span lengths sum to the
/// bucket count; and there are enough bounds to cover the spans.
fn check_histogram_custom_bounds(
    bounds: &[f64],
    spans: &[Span],
    num_buckets: usize,
) -> Result<(), HistogramError> {
    let mut prev = f64::NEG_INFINITY;
    for (i, &curr) in bounds.iter().enumerate() {
        if curr.is_nan() {
            return Err(HistogramError::CustomBoundsNaN);
        }
        if i > 0 && curr <= prev {
            return Err(HistogramError::CustomBoundsNotIncreasing);
        }
        prev = curr;
    }
    if prev == f64::INFINITY {
        return Err(HistogramError::CustomBoundsInfinite);
    }

    let mut span_buckets: u64 = 0;
    let mut total_span_length: i64 = 0;
    for (n, span) in spans.iter().enumerate() {
        if span.offset < 0 {
            return Err(HistogramError::SpanNegativeOffset {
                side: "positive",
                span_index: n,
            });
        }
        span_buckets = span_buckets.wrapping_add(u64::from(span.length));
        total_span_length =
            total_span_length.wrapping_add(i64::from(span.length) + i64::from(span.offset));
    }
    if span_buckets != num_buckets as u64 {
        return Err(HistogramError::SpanBucketCountMismatch {
            side: "positive",
            expected: span_buckets,
            actual: num_buckets,
        });
    }
    if (bounds.len() as i64 + 1) < total_span_length {
        return Err(HistogramError::CustomBoundsTooFew {
            have: bounds.len(),
            need: total_span_length as usize,
        });
    }
    Ok(())
}

impl NativeHistogram {
    /// Lossless encode to the value columns. Splits each `Vec<Span>` into the
    /// parallel offset/length arrays. Fails only if `schema` exceeds `Int8`
    /// (unreachable for valid data; defensive, never silently truncates).
    pub fn to_columns(&self) -> Result<HistogramColumns, HistogramError> {
        let schema =
            i8::try_from(self.schema).map_err(|_| HistogramError::SchemaOutOfRange(self.schema))?;
        let (pos_span_offsets, pos_span_lengths) = split_spans(&self.positive_spans);
        let (neg_span_offsets, neg_span_lengths) = split_spans(&self.negative_spans);
        Ok(HistogramColumns {
            schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            count: self.count,
            sum: self.sum,
            pos_span_offsets,
            pos_span_lengths,
            pos_bucket_deltas: self.positive_buckets.clone(),
            neg_span_offsets,
            neg_span_lengths,
            neg_bucket_deltas: self.negative_buckets.clone(),
            custom_values: self.custom_values.clone(),
        })
    }

    /// Lossless decode from the value columns (pure structural inverse of
    /// [`Self::to_columns`]: joins parallel arrays back into `Vec<Span>`,
    /// widens `schema` i8->i32). Fails only when a side's offsets/lengths
    /// differ in length. Does NOT run semantic validation — call
    /// [`Self::validate`] for that (trusted-storage decode).
    pub fn from_columns(cols: &HistogramColumns) -> Result<Self, HistogramError> {
        let positive_spans =
            join_spans("positive", &cols.pos_span_offsets, &cols.pos_span_lengths)?;
        let negative_spans =
            join_spans("negative", &cols.neg_span_offsets, &cols.neg_span_lengths)?;
        Ok(Self {
            schema: i32::from(cols.schema),
            zero_threshold: cols.zero_threshold,
            zero_count: cols.zero_count,
            count: cols.count,
            sum: cols.sum,
            positive_spans,
            negative_spans,
            positive_buckets: cols.pos_bucket_deltas.clone(),
            negative_buckets: cols.neg_bucket_deltas.clone(),
            custom_values: cols.custom_values.clone(),
        })
    }

    /// Semantic validity — a faithful port of upstream `Histogram.Validate`,
    /// check-for-check and in the same short-circuit order. Called at the A4
    /// ingest seam before [`Self::to_columns`]; kept separate from
    /// [`Self::from_columns`] so the storage round-trip identity stays provable
    /// on hand-built columns. Returns the first violated invariant.
    pub fn validate(&self) -> Result<(), HistogramError> {
        let mut n_count: u64 = 0;
        let mut p_count: u64 = 0;

        if is_custom_buckets_schema(self.schema) {
            check_histogram_custom_bounds(
                &self.custom_values,
                &self.positive_spans,
                self.positive_buckets.len(),
            )?;
            if self.zero_count != 0 {
                return Err(HistogramError::CustomBucketsZeroCount);
            }
            if self.zero_threshold != 0.0 {
                return Err(HistogramError::CustomBucketsZeroThreshold);
            }
            if !self.negative_spans.is_empty() {
                return Err(HistogramError::CustomBucketsNegativeSpans);
            }
            if !self.negative_buckets.is_empty() {
                return Err(HistogramError::CustomBucketsNegativeBuckets);
            }
        } else if is_exponential_schema(self.schema) {
            check_exponential_spans(
                &self.positive_spans,
                self.positive_buckets.len(),
                "positive",
            )?;
            check_exponential_spans(
                &self.negative_spans,
                self.negative_buckets.len(),
                "negative",
            )?;
            check_histogram_buckets(&self.negative_buckets, &mut n_count, "negative")?;
            if !self.custom_values.is_empty() {
                return Err(HistogramError::ExponentialSchemaCustomBounds);
            }
        } else {
            return Err(HistogramError::InvalidSchema(self.schema));
        }

        check_histogram_buckets(&self.positive_buckets, &mut p_count, "positive")?;

        let sum_of_buckets = n_count.wrapping_add(p_count).wrapping_add(self.zero_count);
        if self.sum.is_nan() {
            if sum_of_buckets > self.count {
                return Err(HistogramError::CountNotBigEnough {
                    buckets: sum_of_buckets,
                    count: self.count,
                });
            }
        } else if sum_of_buckets != self.count {
            return Err(HistogramError::CountMismatch {
                buckets: sum_of_buckets,
                count: self.count,
            });
        }
        Ok(())
    }

    /// Whether this histogram uses custom buckets (NHCB, schema −53).
    pub fn is_custom_buckets(&self) -> bool {
        is_custom_buckets_schema(self.schema)
    }
}

/// `metric_series.value_type` discriminator: 0 = float, 1 = histogram. The
/// per-series "mixed" state is an ingest-time rollup over these per-value
/// types and is inert to read routing — out of scope here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    /// A float sample.
    Float = 0,
    /// A native histogram sample.
    Histogram = 1,
}

impl ValueType {
    /// The stored `UInt8` discriminant.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Decode a stored discriminant; `None` for undefined codes.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Float),
            1 => Some(Self::Histogram),
            _ => None,
        }
    }
}

/// A per-sample value: float or native histogram. The float variant carries
/// the raw f64 verbatim (stale-NaN safe, same guarantee as `MetricSample`).
#[derive(Debug, Clone)]
pub enum SampleValue {
    /// A float value.
    Float(f64),
    /// A native histogram value.
    Histogram(NativeHistogram),
}

impl SampleValue {
    /// The discriminator this value stores in `metric_series.value_type`.
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Float(_) => ValueType::Float,
            Self::Histogram(_) => ValueType::Histogram,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::STALE_NAN_BITS;

    /// Compare two histograms with f64 fields by bit pattern (NaN-safe) and
    /// everything else by value.
    fn assert_hist_bits_eq(a: &NativeHistogram, b: &NativeHistogram) {
        assert_eq!(a.schema, b.schema, "schema");
        assert_eq!(
            a.zero_threshold.to_bits(),
            b.zero_threshold.to_bits(),
            "zero_threshold"
        );
        assert_eq!(a.zero_count, b.zero_count, "zero_count");
        assert_eq!(a.count, b.count, "count");
        assert_eq!(a.sum.to_bits(), b.sum.to_bits(), "sum");
        assert_eq!(a.positive_spans, b.positive_spans, "positive_spans");
        assert_eq!(a.negative_spans, b.negative_spans, "negative_spans");
        assert_eq!(a.positive_buckets, b.positive_buckets, "positive_buckets");
        assert_eq!(a.negative_buckets, b.negative_buckets, "negative_buckets");
        assert_eq!(
            a.custom_values.len(),
            b.custom_values.len(),
            "custom_values len"
        );
        for (i, (x, y)) in a.custom_values.iter().zip(&b.custom_values).enumerate() {
            assert_eq!(x.to_bits(), y.to_bits(), "custom_values[{i}]");
        }
    }

    fn assert_round_trip(h: &NativeHistogram) {
        let cols = h.to_columns().expect("to_columns");
        let back = NativeHistogram::from_columns(&cols).expect("from_columns");
        assert_hist_bits_eq(h, &back);
    }

    /// `single_histogram {{schema:0 sum:5 count:4 buckets:[1 2 1]}}`
    /// (`native_histograms.test:34`). Absolute buckets `[1 2 1]` delta-encode
    /// to `[1 1 -1]`.
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

    /// `custom_buckets_histogram {{schema:-53 sum:5 count:4 custom_values:[5 10]
    /// buckets:[1 2 1]}}` (`native_histograms.test:1078`).
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

    /// `empty_histogram{h="exp"} {{}}` (`native_histograms.test:3`).
    fn empty_exponential() -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            positive_spans: vec![],
            negative_spans: vec![],
            positive_buckets: vec![],
            negative_buckets: vec![],
            custom_values: vec![],
        }
    }

    /// `empty_histogram{h="cbh"} {{schema:-53 custom_values:[-2 3]}}`
    /// (`native_histograms.test:4`).
    fn empty_nhcb() -> NativeHistogram {
        NativeHistogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            positive_spans: vec![],
            negative_spans: vec![],
            positive_buckets: vec![],
            negative_buckets: vec![],
            custom_values: vec![-2.0, 3.0],
        }
    }

    // -- AC 2: exponential round-trip (corpus-referenced) --

    #[test]
    fn exponential_to_columns_projects_parallel_arrays() {
        let cols = single_histogram().to_columns().expect("to_columns");
        assert_eq!(cols.schema, 0i8);
        assert_eq!(cols.pos_span_offsets, vec![0]);
        assert_eq!(cols.pos_span_lengths, vec![3]);
        assert_eq!(cols.pos_bucket_deltas, vec![1, 1, -1]);
        assert!(cols.neg_span_offsets.is_empty());
        assert!(cols.neg_span_lengths.is_empty());
        assert!(cols.neg_bucket_deltas.is_empty());
        assert!(cols.custom_values.is_empty());
        assert_eq!(cols.count, 4);
        assert_eq!(cols.sum.to_bits(), 5.0f64.to_bits());
    }

    #[test]
    fn exponential_round_trips_bit_for_bit() {
        assert_round_trip(&single_histogram());
    }

    // -- AC 3: NHCB round-trip (corpus-referenced) --

    #[test]
    fn nhcb_to_columns_narrows_schema_and_keeps_bounds() {
        let cols = custom_buckets_histogram().to_columns().expect("to_columns");
        assert_eq!(cols.schema, -53i8);
        assert_eq!(cols.pos_span_offsets, vec![0]);
        assert_eq!(cols.pos_span_lengths, vec![3]);
        assert_eq!(cols.pos_bucket_deltas, vec![1, 1, -1]);
        assert_eq!(cols.custom_values, vec![5.0, 10.0]);
        assert!(cols.neg_span_offsets.is_empty());
        assert!(cols.neg_bucket_deltas.is_empty());
    }

    #[test]
    fn nhcb_round_trips_bit_for_bit() {
        assert_round_trip(&custom_buckets_histogram());
    }

    // -- AC 4: empty + negative-span coverage --

    #[test]
    fn empty_exponential_round_trips_to_empty_columns() {
        let cols = empty_exponential().to_columns().expect("to_columns");
        assert!(cols.pos_span_offsets.is_empty());
        assert!(cols.pos_span_lengths.is_empty());
        assert!(cols.pos_bucket_deltas.is_empty());
        assert!(cols.custom_values.is_empty());
        assert_round_trip(&empty_exponential());
    }

    #[test]
    fn empty_nhcb_round_trips_bit_for_bit() {
        assert_round_trip(&empty_nhcb());
    }

    #[test]
    fn negative_span_populated_round_trips_bit_for_bit() {
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 1e-128,
            zero_count: 7,
            count: 20,
            sum: -3.5,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![
                Span {
                    offset: -2,
                    length: 2,
                },
                Span {
                    offset: 1,
                    length: 1,
                },
            ],
            positive_buckets: vec![3],
            negative_buckets: vec![2, -1, 4],
            custom_values: vec![],
        };
        let cols = h.to_columns().expect("to_columns");
        assert_eq!(cols.neg_span_offsets, vec![-2, 1]);
        assert_eq!(cols.neg_span_lengths, vec![2, 1]);
        assert_eq!(cols.neg_bucket_deltas, vec![2, -1, 4]);
        assert_round_trip(&h);
    }

    #[test]
    fn stale_nan_sum_survives_round_trip_by_bits() {
        let mut h = single_histogram();
        h.sum = f64::from_bits(STALE_NAN_BITS);
        let cols = h.to_columns().expect("to_columns");
        assert_eq!(cols.sum.to_bits(), STALE_NAN_BITS);
        let back = NativeHistogram::from_columns(&cols).expect("from_columns");
        assert_eq!(back.sum.to_bits(), STALE_NAN_BITS);
        assert_hist_bits_eq(&h, &back);
    }

    // -- AC 5: validation fidelity — valid cases --

    #[test]
    fn corpus_samples_validate_ok() {
        single_histogram()
            .validate()
            .expect("single_histogram valid");
        custom_buckets_histogram()
            .validate()
            .expect("custom_buckets_histogram valid");
        empty_exponential()
            .validate()
            .expect("empty_exponential valid");
        empty_nhcb().validate().expect("empty_nhcb valid");
    }

    #[test]
    fn is_custom_buckets_schema_only_true_for_minus_53() {
        assert!(is_custom_buckets_schema(CUSTOM_BUCKETS_SCHEMA));
        assert!(custom_buckets_histogram().is_custom_buckets());
        assert!(!is_custom_buckets_schema(0));
        assert!(!single_histogram().is_custom_buckets());
    }

    // -- AC 5: validation fidelity — invalid, one per class --

    #[test]
    fn invalid_schema_above_and_below_range() {
        let mut h = single_histogram();
        h.schema = 9;
        assert_eq!(h.validate(), Err(HistogramError::InvalidSchema(9)));
        h.schema = -10;
        assert_eq!(h.validate(), Err(HistogramError::InvalidSchema(-10)));
    }

    #[test]
    fn nhcb_nonzero_zero_count_rejected() {
        let mut h = custom_buckets_histogram();
        h.zero_count = 1;
        assert_eq!(h.validate(), Err(HistogramError::CustomBucketsZeroCount));
    }

    #[test]
    fn nhcb_nonzero_zero_threshold_rejected() {
        let mut h = custom_buckets_histogram();
        h.zero_threshold = 1.0;
        assert_eq!(
            h.validate(),
            Err(HistogramError::CustomBucketsZeroThreshold)
        );
    }

    #[test]
    fn nhcb_negative_spans_rejected() {
        let mut h = custom_buckets_histogram();
        h.negative_spans = vec![Span {
            offset: 0,
            length: 1,
        }];
        assert_eq!(
            h.validate(),
            Err(HistogramError::CustomBucketsNegativeSpans)
        );
    }

    #[test]
    fn nhcb_negative_buckets_rejected() {
        let mut h = custom_buckets_histogram();
        h.negative_buckets = vec![1];
        assert_eq!(
            h.validate(),
            Err(HistogramError::CustomBucketsNegativeBuckets)
        );
    }

    #[test]
    fn custom_bounds_not_increasing_rejected() {
        let mut h = empty_nhcb();
        h.custom_values = vec![10.0, 5.0];
        assert_eq!(h.validate(), Err(HistogramError::CustomBoundsNotIncreasing));
        h.custom_values = vec![5.0, 5.0];
        assert_eq!(h.validate(), Err(HistogramError::CustomBoundsNotIncreasing));
    }

    #[test]
    fn custom_bounds_nan_rejected() {
        let mut h = empty_nhcb();
        h.custom_values = vec![f64::NAN];
        assert_eq!(h.validate(), Err(HistogramError::CustomBoundsNaN));
    }

    #[test]
    fn custom_bounds_trailing_infinite_rejected() {
        let mut h = empty_nhcb();
        h.custom_values = vec![5.0, f64::INFINITY];
        assert_eq!(h.validate(), Err(HistogramError::CustomBoundsInfinite));
    }

    #[test]
    fn custom_bounds_too_few_rejected() {
        let mut h = custom_buckets_histogram();
        h.custom_values = vec![5.0];
        assert_eq!(
            h.validate(),
            Err(HistogramError::CustomBoundsTooFew { have: 1, need: 3 })
        );
    }

    #[test]
    fn exponential_with_custom_bounds_rejected() {
        let mut h = single_histogram();
        h.custom_values = vec![5.0];
        assert_eq!(
            h.validate(),
            Err(HistogramError::ExponentialSchemaCustomBounds)
        );
    }

    #[test]
    fn empty_custom_values_on_exponential_accepted() {
        let mut h = single_histogram();
        h.custom_values = vec![];
        h.validate()
            .expect("empty custom_values == absent, accepted");
    }

    #[test]
    fn exponential_subsequent_span_negative_offset_rejected() {
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 2,
            sum: 2.0,
            positive_spans: vec![
                Span {
                    offset: 0,
                    length: 1,
                },
                Span {
                    offset: -1,
                    length: 1,
                },
            ],
            negative_spans: vec![],
            positive_buckets: vec![1, 0],
            negative_buckets: vec![],
            custom_values: vec![],
        };
        assert_eq!(
            h.validate(),
            Err(HistogramError::SpanNegativeOffset {
                side: "positive",
                span_index: 1,
            })
        );
    }

    #[test]
    fn nhcb_first_span_negative_offset_rejected() {
        let h = NativeHistogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 1,
            sum: 1.0,
            positive_spans: vec![Span {
                offset: -1,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1],
            negative_buckets: vec![],
            custom_values: vec![5.0],
        };
        assert_eq!(
            h.validate(),
            Err(HistogramError::SpanNegativeOffset {
                side: "positive",
                span_index: 0,
            })
        );
    }

    #[test]
    fn span_bucket_count_mismatch_rejected() {
        let mut h = single_histogram();
        h.positive_buckets = vec![1, 1];
        assert_eq!(
            h.validate(),
            Err(HistogramError::SpanBucketCountMismatch {
                side: "positive",
                expected: 3,
                actual: 2,
            })
        );
    }

    #[test]
    fn positive_delta_reconstruction_negative_rejected() {
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 2,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, -2],
            negative_buckets: vec![],
            custom_values: vec![],
        };
        assert_eq!(
            h.validate(),
            Err(HistogramError::NegativeBucketCount {
                side: "positive",
                bucket_index: 1,
            })
        );
    }

    #[test]
    fn negative_delta_reconstruction_negative_rejected() {
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            positive_spans: vec![],
            negative_spans: vec![Span {
                offset: 0,
                length: 2,
            }],
            positive_buckets: vec![],
            negative_buckets: vec![1, -2],
            custom_values: vec![],
        };
        assert_eq!(
            h.validate(),
            Err(HistogramError::NegativeBucketCount {
                side: "negative",
                bucket_index: 1,
            })
        );
    }

    #[test]
    fn positive_delta_overflow_does_not_panic() {
        // Untrusted deltas that overflow i64 during reconstruction must wrap
        // (mirroring Go) and be rejected via the negative-count check, not panic.
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 2,
            }],
            negative_spans: vec![],
            positive_buckets: vec![i64::MAX, 1],
            negative_buckets: vec![],
            custom_values: vec![],
        };
        assert_eq!(
            h.validate(),
            Err(HistogramError::NegativeBucketCount {
                side: "positive",
                bucket_index: 1,
            })
        );
    }

    #[test]
    fn negative_delta_overflow_does_not_panic() {
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            positive_spans: vec![],
            negative_spans: vec![Span {
                offset: 0,
                length: 2,
            }],
            positive_buckets: vec![],
            negative_buckets: vec![i64::MAX, 1],
            custom_values: vec![],
        };
        assert_eq!(
            h.validate(),
            Err(HistogramError::NegativeBucketCount {
                side: "negative",
                bucket_index: 1,
            })
        );
    }

    #[test]
    fn count_accumulation_overflow_does_not_panic() {
        // Three max-count buckets overflow the u64 observation accumulator;
        // wrapping arithmetic must return a verdict rather than panic.
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_buckets: vec![i64::MAX, 0, 0],
            negative_buckets: vec![],
            custom_values: vec![],
        };
        assert!(matches!(
            h.validate(),
            Err(HistogramError::CountMismatch { .. })
        ));
    }

    #[test]
    fn count_mismatch_when_sum_finite() {
        let mut h = single_histogram();
        h.count = 99;
        assert_eq!(
            h.validate(),
            Err(HistogramError::CountMismatch {
                buckets: 4,
                count: 99,
            })
        );
    }

    #[test]
    fn count_not_big_enough_when_sum_nan() {
        let mut h = single_histogram();
        h.sum = f64::NAN;
        h.count = 2;
        assert_eq!(
            h.validate(),
            Err(HistogramError::CountNotBigEnough {
                buckets: 4,
                count: 2,
            })
        );
    }

    // -- AC 5: exponential-branch precedence (v3) --

    #[test]
    fn negative_reconstruction_precedes_exp_custom_bounds() {
        // Both violated: negative-side delta-decode (step 3) runs BEFORE the
        // exponential-no-custom-bounds check (step 4).
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            positive_spans: vec![],
            negative_spans: vec![Span {
                offset: 0,
                length: 2,
            }],
            positive_buckets: vec![],
            negative_buckets: vec![1, -2],
            custom_values: vec![5.0],
        };
        assert_eq!(
            h.validate(),
            Err(HistogramError::NegativeBucketCount {
                side: "negative",
                bucket_index: 1,
            })
        );
    }

    #[test]
    fn exp_custom_bounds_precedes_positive_reconstruction() {
        // Both violated on the positive side: the exp-custom-bounds check
        // (step 4) precedes the post-switch positive delta-decode (step 5).
        let h = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 2,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, -2],
            negative_buckets: vec![],
            custom_values: vec![5.0],
        };
        assert_eq!(
            h.validate(),
            Err(HistogramError::ExponentialSchemaCustomBounds)
        );
    }

    // -- AC 5: from_columns structural failure --

    #[test]
    fn from_columns_rejects_unequal_span_arrays() {
        let cols = HistogramColumns {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 0,
            sum: 0.0,
            pos_span_offsets: vec![0],
            pos_span_lengths: vec![],
            pos_bucket_deltas: vec![],
            neg_span_offsets: vec![],
            neg_span_lengths: vec![],
            neg_bucket_deltas: vec![],
            custom_values: vec![],
        };
        assert_eq!(
            NativeHistogram::from_columns(&cols).unwrap_err(),
            HistogramError::SpanArrayLengthMismatch {
                side: "positive",
                offsets: 1,
                lengths: 0,
            }
        );
    }

    #[test]
    fn to_columns_rejects_schema_outside_int8() {
        let mut h = single_histogram();
        h.schema = 200;
        assert_eq!(
            h.to_columns().unwrap_err(),
            HistogramError::SchemaOutOfRange(200)
        );
    }

    // -- AC 6: value_type mapping --

    #[test]
    fn sample_value_maps_to_value_type() {
        assert_eq!(SampleValue::Float(1.5).value_type(), ValueType::Float);
        assert_eq!(SampleValue::Float(1.5).value_type().as_u8(), 0);
        assert_eq!(
            SampleValue::Histogram(single_histogram()).value_type(),
            ValueType::Histogram
        );
        assert_eq!(
            SampleValue::Histogram(single_histogram())
                .value_type()
                .as_u8(),
            1
        );
    }

    #[test]
    fn value_type_from_u8_round_trips_and_rejects_undefined() {
        assert_eq!(ValueType::from_u8(0), Some(ValueType::Float));
        assert_eq!(ValueType::from_u8(1), Some(ValueType::Histogram));
        assert_eq!(ValueType::from_u8(2), None);
        assert_eq!(ValueType::Float.as_u8(), 0);
        assert_eq!(ValueType::Histogram.as_u8(), 1);
    }
}
