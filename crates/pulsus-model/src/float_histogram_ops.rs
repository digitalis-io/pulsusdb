// M7-A5b-ii: FloatHistogram binary/counter-reset primitives — `Mul`/`Div`/
// `Compact`/`CopyToSchema`/`Add`/`Sub`/`DetectReset`/`Equals`, ported from
// the pinned `model/histogram/float_histogram.go` (v3.13.0, `40af9c2`, via
// `git show 40af9c2:model/histogram/float_histogram.go`) and
// `model/histogram/generic.go`. `KahanAdd` (the compensated Add with a
// caller-held compensation histogram — `sum`/`avg` aggregation and
// `sum_over_time`/`avg_over_time`'s fold, plan v2 OQ3) lives in the
// sibling `float_histogram_kahan.rs` (M7-A5b-iii), reusing this module's
// helpers; `rate`/`increase`/`delta`/`irate`/`idelta` and the binop arms
// use this module's plain `Add`/`Sub` — `functions.go:663,680`,
// `engine.go:3518,3532`.
// `CounterResetHint` is never modeled (`float_histogram.rs`'s own module
// doc, OQ2): every decoded histogram is upstream's `UnknownCounterReset`,
// so `adjustCounterReset`/the two `CounterReset`/`NotCounterReset`
// `DetectReset` shortcuts are dead code upstream would fall through
// anyway — this port skips them rather than modeling an unreachable state.
//
// **Pin-conformance notes (the #124 A5b-ii codex round-1 findings):**
// - **Kahan structure in the zero-bucket paths.** The pin's
//   `zeroCountForLargerThreshold` (`float_histogram.go:992-1059`)
//   accumulates via `kahansum.Inc` even in the plain `Add`/`Sub` flow
//   (compensation `c == nil` there — `reconcileZeroBuckets`,
//   `float_histogram.go:1109-1127`, passes `nil` and discards the
//   returned compensation term at `Add`/`Sub`'s own call sites,
//   `:358,543`). [`kahan_inc`] is ported and threaded identically,
//   including the pin's redo-loop quirk (the compensation term is NOT
//   reset on the `continue outer` retry, while the primary count is —
//   `:1005-1007`). Note `kahansum.Inc`'s primary sum is literally
//   `sum + inc` (`util/kahansum/kahansum.go`), so the *returned* zero
//   count in the compensation-discarded plain flow is arithmetically the
//   plain-accumulation value — the structure is ported for exactness
//   anyway, and becomes load-bearing when A5b-iii's `KahanAdd` lands.
// - **No eager zero-bucket stripping in `Add`/`Sub`.** Upstream's
//   `addBuckets` (`float_histogram.go:1420`) preserves zero-count buckets
//   (the result "might have buckets with a population of zero", `Add`'s
//   own doc) and defers normalization to an explicit `Compact` —
//   mirrored: [`FloatHistogram::combine`] keeps the full union layout
//   (explicit zeros included) and only [`FloatHistogram::compact`]
//   strips. The one in-`Add` compaction the pin performs —
//   `trimBucketsInZeroBucket` (`:1065-1100`), which zeroes-then-
//   `Compact(0)`s the RECEIVER when its zero threshold grows during
//   reconciliation — is mirrored at the same point.
// - **NHCB (schema −53) mismatched-custom-bounds reconciliation is
//   PORTED** (round-1 shipped a documented error-out simplification;
//   codex established the path is reachable — any NHCB row in
//   `metric_hist_samples` decodes through `from_columns` → `to_float`
//   straight into the evaluator; the storage schema and A3 model fully
//   support NHCB even though the currently-shipped OTLP ingest never
//   emits it — and numerically load-bearing: `native_histograms.test`'s
//   `nhcb_metric` block expects `resets(...) == 0` across a bounds
//   change and reconciled `rate`/`increase` results with NO warning).
//   `Add`/`Sub` reconcile mismatched custom bounds to their intersection
//   (`intersectCustomBucketBounds` + `addCustomBucketsWithMismatches`,
//   `float_histogram.go:1779-1902`, Kahan-compensated exactly as pinned)
//   and report `nhcb_bounds_reconciled = true` (callers emit the pinned
//   `NewMismatchedCustomBucketsHistogramsInfo`); `DetectReset` compares
//   rolled-up buckets over the common bounds
//   (`detectResetWithMismatchedCustomBounds`, `:1703-1776`).
//
// **Implementation strategy — index-keyed merge, not the pinned
// array-splicing state machines.** Mirrors this module's own
// `all_buckets`/`natural_side_buckets` precedent: every operation decodes
// each side into `(schema-index, count)` pairs, combines/reduces by index
// (each shared index gets exactly ONE addition, `a + sign*b`, and each
// unshared index is inserted verbatim — the same per-index arithmetic, in
// the same ascending-origin order, that `addBuckets`/`reduceResolution`
// perform, so VALUES are bit-identical), and rebuilds the span encoding
// of the resulting index set — zero-count entries included. The rebuilt
// encoding is the minimal one (no zero-length spans, no zero-offset
// adjacent spans); the pin's in-place splicing can express the SAME
// (index, value) sequence with a non-minimal encoding, a transient state
// its own docs describe as `Compact`-normalized. Both canonicalize
// identically under [`FloatHistogram::compact`], and every A5b-ii
// consumer compacts before any comparison/output (`histogramRate`/
// `instantValue` both end in `.Compact(0)` — `functions.go:700,868`), so
// the encoding difference is unobservable; the (index, value) content —
// what [`FloatHistogram::equals`] compares and the wire encodes — is
// bit-identical.

use std::collections::BTreeMap;

/// Errors from [`FloatHistogram::combine`] — mirrors upstream's
/// `ErrHistogramsIncompatibleSchema` (`checkSchemaAndBounds`,
/// `float_histogram.go:2058`): one operand exponential, the other NHCB.
/// (Mismatched NHCB custom bounds are NOT an error — they reconcile; see
/// the module doc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FloatHistogramOpError {
    /// One operand is exponential-schema, the other NHCB (custom buckets).
    #[error(
        "cannot combine an exponential-schema histogram with an NHCB (custom-buckets) histogram"
    )]
    IncompatibleSchema,
}

/// `Add` vs `Sub` — an enum (not a boolean parameter) selecting
/// [`FloatHistogram::combine`]'s sign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineOp {
    Add,
    Sub,
}

/// The result of [`FloatHistogram::combine`]: the combined histogram plus
/// whether NHCB custom bounds needed reconciling to their intersection —
/// mirrors upstream `Add`/`Sub`'s `nhcbBoundsReconciled` return, which
/// callers surface as `NewMismatchedCustomBucketsHistogramsInfo`
/// (`functions.go:672-674,689-691,863-865`).
#[derive(Debug, Clone)]
pub struct CombineOutcome {
    pub result: FloatHistogram,
    pub nhcb_bounds_reconciled: bool,
}

impl FloatHistogram {
    /// Semantic equality — mirrors upstream `Equals` (`float_histogram.go:
    /// 606`): same schema/count/sum (bitwise), same custom bounds (NHCB),
    /// same zero threshold/count, and the same bucket layout on each side
    /// ((index, value) sequence — differing raw span encodings of the
    /// identical layout still match, matching upstream's
    /// `spansMatch`+`floatBucketsMatch`, which treat zero-length spans as
    /// transparent). Distinct from [`Self::bits_eq`] (the value-model
    /// equality primitive, byte-identical arrays) — this is the `h==h`/
    /// `changes()` semantic primitive (M7-A5b-ii).
    pub fn equals(&self, other: &FloatHistogram) -> bool {
        if self.schema != other.schema
            || self.count.to_bits() != other.count.to_bits()
            || self.sum.to_bits() != other.sum.to_bits()
        {
            return false;
        }
        if self.uses_custom_buckets()
            && !custom_bucket_bounds_match(&self.custom_values, &other.custom_values)
        {
            return false;
        }
        if self.zero_threshold != other.zero_threshold
            || self.zero_count.to_bits() != other.zero_count.to_bits()
        {
            return false;
        }
        indexed_side_equals(
            &self.negative_spans,
            &self.negative_buckets,
            &other.negative_spans,
            &other.negative_buckets,
        ) && indexed_side_equals(
            &self.positive_spans,
            &self.positive_buckets,
            &other.positive_spans,
            &other.positive_buckets,
        )
    }

    /// Scales every bucket count, `zero_count`, `count`, and `sum` by
    /// `factor` in place — mirrors upstream `Mul` (`float_histogram.go:
    /// 290`). `CounterResetHint` is not modeled (module doc), so the
    /// pin's `factor < 0 -> GaugeType` side effect has no observable
    /// counterpart here.
    pub fn mul(&mut self, factor: f64) {
        self.zero_count *= factor;
        self.count *= factor;
        self.sum *= factor;
        for b in &mut self.positive_buckets {
            *b *= factor;
        }
        for b in &mut self.negative_buckets {
            *b *= factor;
        }
    }

    /// Divides every bucket count, `zero_count`, `count`, and `sum` by
    /// `scalar` in place — mirrors upstream `Div` (`float_histogram.go:
    /// 309`), including the "divide by zero clears every bucket" rule.
    pub fn div(&mut self, scalar: f64) {
        self.zero_count /= scalar;
        self.count /= scalar;
        self.sum /= scalar;
        if scalar == 0.0 {
            self.positive_buckets.clear();
            self.negative_buckets.clear();
            self.positive_spans.clear();
            self.negative_spans.clear();
            return;
        }
        for b in &mut self.positive_buckets {
            *b /= scalar;
        }
        for b in &mut self.negative_buckets {
            *b /= scalar;
        }
    }

    /// `Compact(0)` — mirrors upstream `Compact` (`float_histogram.go:
    /// 699`) called with `maxEmptyBuckets == 0` (every A5b-ii consumer
    /// calls it that way — `functions.go:700,868`): drops every
    /// zero-valued bucket and re-encodes the survivors minimally (no
    /// zero-length spans, no zero-offset adjacencies — exactly what the
    /// pin's `compactBuckets` leaves for `maxEmptyBuckets == 0`).
    pub fn compact(&mut self) {
        let (spans, buckets) = compact_side(&self.positive_spans, &self.positive_buckets);
        self.positive_spans = spans;
        self.positive_buckets = buckets;
        let (spans, buckets) = compact_side(&self.negative_spans, &self.negative_buckets);
        self.negative_spans = spans;
        self.negative_buckets = buckets;
    }

    /// Reduces exponential-schema resolution to `target_schema` (must be
    /// `<= self.schema`; NHCB has no resolution to reduce) — mirrors
    /// upstream `CopyToSchema` (`float_histogram.go:147`) via
    /// `reduceResolution` (`generic.go:782`, `deltaBuckets=false`):
    /// consecutive origin buckets mapping to the same target index
    /// accumulate via a single running `+=` in ascending origin order;
    /// zero-count origin buckets still materialize their target bucket
    /// (upstream appends them verbatim — no zero-stripping here).
    pub fn copy_to_schema(&self, target_schema: i32) -> FloatHistogram {
        if target_schema == self.schema {
            return self.clone();
        }
        debug_assert!(
            !self.uses_custom_buckets(),
            "copy_to_schema is exponential-only (mirrors the pin's own panic)"
        );
        debug_assert!(
            target_schema <= self.schema,
            "copy_to_schema only ever reduces resolution"
        );
        let (positive_spans, positive_buckets) = reduce_resolution_side(
            &self.positive_spans,
            &self.positive_buckets,
            self.schema,
            target_schema,
        );
        let (negative_spans, negative_buckets) = reduce_resolution_side(
            &self.negative_spans,
            &self.negative_buckets,
            self.schema,
            target_schema,
        );
        FloatHistogram {
            schema: target_schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            count: self.count,
            sum: self.sum,
            positive_spans,
            negative_spans,
            positive_buckets,
            negative_buckets,
            custom_values: Vec::new(),
        }
    }

    /// `Add`/`Sub` — mirrors upstream `Add`/`Sub` (`float_histogram.go:
    /// 352`/`537`) operation-for-operation and in the pin's own ORDER:
    /// zero-bucket reconciliation FIRST, at each operand's NATIVE schema
    /// (`reconcileZeroBuckets` runs before the schema `switch` in the
    /// pin — the bucket bounds the threshold walk sees are the
    /// full-resolution ones, which matters when a growing threshold lands
    /// on a straddled bucket's upper edge); then `count`/`sum`; then
    /// resolution reduction to the common (minimum) schema; then the
    /// bucket merge with the other side's fully-in-zero-bucket buckets
    /// skipped (`addBuckets`'s `getBoundExponential(indexB, schema) <=
    /// threshold` prefix skip — exponential only) and zero-count buckets
    /// PRESERVED (no eager compaction; module doc).
    ///
    /// NHCB: matching custom bounds merge index-wise (no threshold skip —
    /// the pin's `IsExponentialSchema` gate); MISMATCHED bounds reconcile
    /// to the intersection (`addCustomBucketsWithMismatches` port) and
    /// set `nhcb_bounds_reconciled`.
    pub fn combine(
        &self,
        other: &FloatHistogram,
        op: CombineOp,
    ) -> Result<CombineOutcome, FloatHistogramOpError> {
        if self.uses_custom_buckets() != other.uses_custom_buckets() {
            return Err(FloatHistogramOpError::IncompatibleSchema);
        }
        let sign = match op {
            CombineOp::Add => 1.0,
            CombineOp::Sub => -1.0,
        };
        let count = self.count + sign * other.count;
        let sum = self.sum + sign * other.sum;

        if self.uses_custom_buckets() {
            if custom_bucket_bounds_match(&self.custom_values, &other.custom_values) {
                let (positive_spans, positive_buckets) = merge_indexed_union(
                    &indexed_pairs(&self.positive_spans, &self.positive_buckets),
                    &indexed_pairs(&other.positive_spans, &other.positive_buckets),
                    sign,
                    |_idx| false,
                );
                return Ok(CombineOutcome {
                    result: FloatHistogram {
                        schema: self.schema,
                        zero_threshold: 0.0,
                        zero_count: 0.0,
                        count,
                        sum,
                        positive_spans,
                        negative_spans: Vec::new(),
                        positive_buckets,
                        negative_buckets: Vec::new(),
                        custom_values: self.custom_values.clone(),
                    },
                    nhcb_bounds_reconciled: false,
                });
            }
            // Mismatched custom bounds: reconcile to the intersection
            // (`float_histogram.go:374-385`/`559-570` via
            // `addCustomBucketsWithMismatches`, `:1812-1902`). No incoming
            // compensation (the pin's `nil` `bucketsC` in the plain
            // `Add`/`Sub` flow); the compensation output is discarded at
            // the pin's own discard point (`:379,564`).
            let intersected =
                intersect_custom_bucket_bounds(&self.custom_values, &other.custom_values);
            let (positive_spans, positive_buckets, _comps) = add_custom_buckets_with_mismatches(
                sign,
                &indexed_pairs(&self.positive_spans, &self.positive_buckets),
                None,
                &self.custom_values,
                &indexed_pairs(&other.positive_spans, &other.positive_buckets),
                &other.custom_values,
                &intersected,
            );
            return Ok(CombineOutcome {
                result: FloatHistogram {
                    schema: self.schema,
                    zero_threshold: 0.0,
                    zero_count: 0.0,
                    count,
                    sum,
                    positive_spans,
                    negative_spans: Vec::new(),
                    positive_buckets,
                    negative_buckets: Vec::new(),
                    custom_values: intersected,
                },
                nhcb_bounds_reconciled: true,
            });
        }

        // Exponential. Step 1 (pin order): reconcile the zero buckets at
        // NATIVE schemas — may grow the common threshold and trim self's
        // in-zero buckets (`reconcileZeroBuckets` +
        // `trimBucketsInZeroBucket`).
        let reconciled = reconcile_zero_buckets(
            SelfZeroState {
                threshold: self.zero_threshold,
                zero_count: self.zero_count,
                positive: natural_side_buckets(
                    &self.positive_spans,
                    &self.positive_buckets,
                    true,
                    self.schema,
                    &[],
                ),
                negative: natural_side_buckets(
                    &self.negative_spans,
                    &self.negative_buckets,
                    false,
                    self.schema,
                    &[],
                ),
            },
            other.zero_threshold,
            other.zero_count,
            &natural_side_buckets(
                &other.positive_spans,
                &other.positive_buckets,
                true,
                other.schema,
                &[],
            ),
            &natural_side_buckets(
                &other.negative_spans,
                &other.negative_buckets,
                false,
                other.schema,
                &[],
            ),
        );
        let zero_threshold = reconciled.threshold;
        let zero_count = reconciled.self_zero_count + sign * reconciled.other_zero_count;

        // Step 2: resolution reduction to the common minimum schema (the
        // pin's schema `switch` via `mustReduceResolution`), on self's
        // possibly-trimmed layout.
        let target_schema = self.schema.min(other.schema);
        let self_pos = reduce_pairs(
            &bucket_pairs(&reconciled.self_positive),
            self.schema,
            target_schema,
        );
        let self_neg = reduce_pairs(
            &bucket_pairs(&reconciled.self_negative),
            self.schema,
            target_schema,
        );
        let other_pos = reduce_pairs(
            &indexed_pairs(&other.positive_spans, &other.positive_buckets),
            other.schema,
            target_schema,
        );
        let other_neg = reduce_pairs(
            &indexed_pairs(&other.negative_spans, &other.negative_buckets),
            other.schema,
            target_schema,
        );

        // Step 3: the bucket merge (`addBuckets`): the other side's
        // buckets lying entirely inside the zero bucket are skipped —
        // `getBoundExponential(indexB, schema) <= threshold`, which is
        // the upper edge for a positive bucket and |lower| for a negative
        // one (both sides share the index→bound formula).
        let (positive_spans, positive_buckets) =
            merge_indexed_union(&self_pos, &other_pos, sign, |idx| {
                get_bound_exponential(idx, target_schema) <= zero_threshold
            });
        let (negative_spans, negative_buckets) =
            merge_indexed_union(&self_neg, &other_neg, sign, |idx| {
                get_bound_exponential(idx, target_schema) <= zero_threshold
            });

        Ok(CombineOutcome {
            result: FloatHistogram {
                schema: target_schema,
                zero_threshold,
                zero_count,
                count,
                sum,
                positive_spans,
                negative_spans,
                positive_buckets,
                negative_buckets,
                custom_values: Vec::new(),
            },
            nhcb_bounds_reconciled: false,
        })
    }

    /// Add — see [`Self::combine`].
    pub fn add(&self, other: &FloatHistogram) -> Result<CombineOutcome, FloatHistogramOpError> {
        self.combine(other, CombineOp::Add)
    }

    /// Sub — see [`Self::combine`].
    pub fn sub(&self, other: &FloatHistogram) -> Result<CombineOutcome, FloatHistogramOpError> {
        self.combine(other, CombineOp::Sub)
    }

    /// `DetectReset` — mirrors upstream `DetectReset` (`float_histogram.go:
    /// 750`), minus the `CounterResetHint` shortcuts (module doc above).
    /// NHCB with mismatched custom bounds compares rolled-up buckets over
    /// the common bounds (`detectResetWithMismatchedCustomBounds`,
    /// `:1703-1776`) — a bounds CHANGE alone is not a reset
    /// (`native_histograms.test`'s `resets(nhcb_metric[13m]) == 0`).
    pub fn detect_reset(&self, previous: &FloatHistogram) -> bool {
        if self.count < previous.count {
            return true;
        }
        if self.uses_custom_buckets() {
            if !previous.uses_custom_buckets() {
                return true;
            }
            if !custom_bucket_bounds_match(&self.custom_values, &previous.custom_values) {
                return self.detect_reset_with_mismatched_custom_bounds(previous);
            }
        }
        // An exponential `self` against an NHCB `previous` falls through
        // to this schema comparison exactly like the pin (any exponential
        // schema > −53): a schema increase is a reset.
        if self.schema > previous.schema {
            return true;
        }
        if self.zero_threshold < previous.zero_threshold {
            return true;
        }
        if !self.uses_custom_buckets() {
            let prev_pos = natural_side_buckets(
                &previous.positive_spans,
                &previous.positive_buckets,
                true,
                previous.schema,
                &[],
            );
            let prev_neg = natural_side_buckets(
                &previous.negative_spans,
                &previous.negative_buckets,
                false,
                previous.schema,
                &[],
            );
            let (prev_zero_count, new_threshold, _c) = zero_count_for_larger_threshold(
                previous.zero_count,
                0.0,
                &prev_pos,
                &prev_neg,
                self.zero_threshold,
                previous.zero_threshold,
            );
            if new_threshold != self.zero_threshold {
                return true;
            }
            if self.zero_count < prev_zero_count {
                return true;
            }
        }

        // Both sides, restricted (exponential only — the pin's
        // `absoluteStartValue` skip is `IsExponentialSchema`-gated,
        // `float_histogram.go:1314`) to buckets NOT entirely inside the
        // zero bucket, with `previous` reduced to `self`'s schema —
        // mirrors `h.floatBucketIterator(_, h.ZeroThreshold, h.Schema)`
        // for BOTH operands (`:798-805`).
        let self_pos = filtered_at_schema(
            &self.positive_spans,
            &self.positive_buckets,
            true,
            self.schema,
            self.schema,
            self.zero_threshold,
            &self.custom_values,
        );
        let self_neg = filtered_at_schema(
            &self.negative_spans,
            &self.negative_buckets,
            false,
            self.schema,
            self.schema,
            self.zero_threshold,
            &self.custom_values,
        );
        let prev_pos = filtered_at_schema(
            &previous.positive_spans,
            &previous.positive_buckets,
            true,
            previous.schema,
            self.schema,
            self.zero_threshold,
            &self.custom_values,
        );
        let prev_neg = filtered_at_schema(
            &previous.negative_spans,
            &previous.negative_buckets,
            false,
            previous.schema,
            self.schema,
            self.zero_threshold,
            &self.custom_values,
        );

        if detect_reset_side(&self_pos, &prev_pos) {
            return true;
        }
        detect_reset_side(&self_neg, &prev_neg)
    }

    /// `detectResetWithMismatchedCustomBounds` (`float_histogram.go:
    /// 1703-1776`): walks the two custom-bound lists in lockstep; at each
    /// COMMON bound (and the trailing +Inf/+Inf pair), rolls up each
    /// side's not-yet-consumed buckets with `upper <= bound` (plain `+=`,
    /// matching the pin's `rollupSumForBound`) and reports a reset iff
    /// the current side's rollup is smaller. Bounds present on only one
    /// side never trigger a comparison — a pure bounds change with
    /// consistent rolled-up counts is NOT a reset.
    fn detect_reset_with_mismatched_custom_bounds(&self, previous: &FloatHistogram) -> bool {
        let curr_buckets = natural_side_buckets(
            &self.positive_spans,
            &self.positive_buckets,
            true,
            self.schema,
            &self.custom_values,
        );
        let prev_buckets = natural_side_buckets(
            &previous.positive_spans,
            &previous.positive_buckets,
            true,
            previous.schema,
            &previous.custom_values,
        );
        let curr_bounds = &self.custom_values;
        let prev_bounds = &previous.custom_values;

        fn rollup(buckets: &[Bucket], cursor: &mut usize, bound: f64) -> f64 {
            let mut sum = 0.0;
            while *cursor < buckets.len() && buckets[*cursor].upper <= bound {
                sum += buckets[*cursor].count;
                *cursor += 1;
            }
            sum
        }

        let (mut curr_bound_idx, mut prev_bound_idx) = (0usize, 0usize);
        let (mut curr_cursor, mut prev_cursor) = (0usize, 0usize);
        while curr_bound_idx <= curr_bounds.len() && prev_bound_idx <= prev_bounds.len() {
            let curr_bound = curr_bounds
                .get(curr_bound_idx)
                .copied()
                .unwrap_or(f64::INFINITY);
            let prev_bound = prev_bounds
                .get(prev_bound_idx)
                .copied()
                .unwrap_or(f64::INFINITY);
            if curr_bound == prev_bound {
                let curr_rollup = rollup(&curr_buckets, &mut curr_cursor, curr_bound);
                let prev_rollup = rollup(&prev_buckets, &mut prev_cursor, curr_bound);
                if curr_rollup < prev_rollup {
                    return true;
                }
                curr_bound_idx += 1;
                prev_bound_idx += 1;
            } else if curr_bound < prev_bound {
                curr_bound_idx += 1;
            } else {
                prev_bound_idx += 1;
            }
        }
        false
    }
}

/// One Neumaier-compensated increment — ported operation-for-operation
/// from the pin's `util/kahansum.Inc` (`kahansum.go`): `t := sum + inc`,
/// then fold the rounding error into `c` (reset to exactly `0.0` on
/// overflow to ±Inf; never accumulate from Inf-tainted arithmetic). The
/// primary sum is literally `sum + inc` — bit-identical to a plain `+=` —
/// so in the compensation-discarded plain-`Add`/`Sub` flow (module doc)
/// the returned totals match plain accumulation exactly; the structure is
/// ported for pin-exactness and for A5b-iii's compensated paths. The
/// extra parens are load-bearing: Go evaluates `(sum-t)+inc` as one value
/// before adding it to `c` (the same note as `pulsus-promql`'s own
/// `kahan_inc` — duplicated here because `pulsus-model` cannot depend on
/// `pulsus-promql`).
fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;
    let new_c = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };
    (t, new_c)
}

/// `CustomBucketBoundsMatch` (`generic.go:92`): identical length, identical
/// values in order.
fn custom_bucket_bounds_match(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x == y)
}

/// `intersectCustomBucketBounds` (`float_histogram.go:1779-1807`): the
/// sorted intersection of two custom-bound lists (both strictly
/// increasing by the NHCB invariant).
fn intersect_custom_bucket_bounds(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            out.push(a[i]);
            i += 1;
            j += 1;
        } else if a[i] < b[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

/// `addCustomBucketsWithMismatches` (`float_histogram.go:1812-1902`): maps
/// BOTH operands' buckets onto the intersected bound layout — a source
/// bucket with bound `b` lands in the first intersected bound `>= b`
/// (else the trailing +Inf bucket) — accumulating with the pin's exact
/// Kahan pairs (`kahansum.Inc`/`Dec` per contribution; A's buckets first,
/// then B's, each in storage order, `Dec(x) == Inc(-x)`), then excludes
/// buckets whose primary AND compensation are both exactly zero
/// (`:1873`) and re-encodes minimally (the pin's own span construction
/// here IS the minimal encoding — unlike `addBuckets`, this path strips
/// its both-zero buckets itself).
///
/// `a_comps` is A's incoming compensation-bucket array (parallel to
/// `a_pairs`, the pin's `bucketsC` — `KahanAdd`'s NHCB-mismatch arm feeds
/// its running compensation through, `withCompensation=true` for the A
/// pass only, `:1849-1851`); pass `None` for the plain `Add`/`Sub` flow
/// (the pin's `nil`). The third return is the resulting compensation
/// array — the plain callers discard it (`:379,564`, the `_` third
/// return), `kahan_add` keeps it.
fn add_custom_buckets_with_mismatches(
    sign: f64,
    a_pairs: &[(i32, f64)],
    a_comps: Option<&[f64]>,
    a_bounds: &[f64],
    b_pairs: &[(i32, f64)],
    b_bounds: &[f64],
    intersected: &[f64],
) -> (Vec<Span>, Vec<f64>, Vec<f64>) {
    let n = intersected.len() + 1;
    let mut target = vec![0.0f64; n];
    let mut c_target = vec![0.0f64; n];

    let mut map_side =
        |pairs: &[(i32, f64)], comps: Option<&[f64]>, bounds: &[f64], side_sign: f64| {
            // `intersectIdx` persists across buckets within one side's pass
            // (both bound lists ascend) and resets between the two passes —
            // the pin's `mapBuckets`-local variable.
            let mut intersect_idx = 0usize;
            for (pos, &(src_idx, value)) in pairs.iter().enumerate() {
                let mut target_idx = n - 1; // Default: the trailing +Inf bucket.
                if src_idx >= 0 && (src_idx as usize) < bounds.len() {
                    let src_bound = bounds[src_idx as usize];
                    while intersect_idx < intersected.len() {
                        if intersected[intersect_idx] >= src_bound {
                            target_idx = intersect_idx;
                            break;
                        }
                        intersect_idx += 1;
                    }
                }
                let (t, c) = kahan_inc(side_sign * value, target[target_idx], c_target[target_idx]);
                target[target_idx] = t;
                c_target[target_idx] = c;
                // The pin's `withCompensation` arm (`:1849-1851`): A's own
                // compensation bucket folds in right after A's value.
                if let Some(comps) = comps {
                    let (t, c) = kahan_inc(
                        comps.get(pos).copied().unwrap_or(0.0),
                        target[target_idx],
                        c_target[target_idx],
                    );
                    target[target_idx] = t;
                    c_target[target_idx] = c;
                }
            }
        };
    map_side(a_pairs, a_comps, a_bounds, 1.0);
    map_side(b_pairs, None, b_bounds, sign);

    let mut spans: Vec<Span> = Vec::new();
    let mut buckets: Vec<f64> = Vec::new();
    let mut comps: Vec<f64> = Vec::new();
    let mut last_idx: Option<i32> = None;
    for (i, (&t, &c)) in target.iter().zip(&c_target).enumerate() {
        if t == 0.0 && c == 0.0 {
            continue;
        }
        let idx = i as i32;
        match last_idx {
            Some(last) if idx == last + 1 => {
                spans
                    .last_mut()
                    .expect("last_idx is only Some once a span has been pushed")
                    .length += 1;
            }
            Some(last) => spans.push(Span {
                offset: idx - last - 1,
                length: 1,
            }),
            None => spans.push(Span {
                offset: idx,
                length: 1,
            }),
        }
        buckets.push(t);
        comps.push(c);
        last_idx = Some(idx);
    }
    (spans, buckets, comps)
}

/// `spansMatch`+`floatBucketsMatch` (`histogram.go:287`,
/// `float_histogram.go:1686`) folded into one index-keyed comparison:
/// decode both sides to `(index, value)` (zero-LENGTH spans are
/// transparent, exactly `spansMatch`'s tolerance) and require the exact
/// same index sequence with bit-identical values (explicit zero-count
/// buckets included — upstream compares the raw bucket arrays).
fn indexed_side_equals(
    spans_a: &[Span],
    buckets_a: &[f64],
    spans_b: &[Span],
    buckets_b: &[f64],
) -> bool {
    let a = indexed_pairs(spans_a, buckets_a);
    let b = indexed_pairs(spans_b, buckets_b);
    a.len() == b.len()
        && a.iter()
            .zip(&b)
            .all(|((ia, va), (ib, vb))| ia == ib && va.to_bits() == vb.to_bits())
}

/// `(index, value)` pairs in ascending storage order.
fn indexed_pairs(spans: &[Span], buckets: &[f64]) -> Vec<(i32, f64)> {
    span_indices(spans, buckets.len())
        .into_iter()
        .zip(buckets.iter().copied())
        .collect()
}

/// A decoded side's `(index, count)` pairs (drops the bounds).
fn bucket_pairs(buckets: &[Bucket]) -> Vec<(i32, f64)> {
    buckets.iter().map(|b| (b.index, b.count)).collect()
}

/// Yields each bucket's schema-relative index, in storage (ascending
/// span-then-position) order — the index-only half of the sibling
/// `natural_side_buckets` walk (`float_histogram.rs`), factored out here
/// so the ops in this file don't need a schema/`custom_values` just to
/// know which index a bucket count belongs to.
fn span_indices(spans: &[Span], n_buckets: usize) -> Vec<i32> {
    let mut out = Vec::with_capacity(n_buckets);
    if spans.is_empty() {
        return out;
    }
    let mut span_idx = 0usize;
    let mut idx_in_span: u32 = 0;
    let mut curr_idx: i32 = spans[0].offset;
    for bucket_idx in 0..n_buckets {
        if bucket_idx != 0 {
            curr_idx += 1;
        }
        while idx_in_span >= spans[span_idx].length {
            idx_in_span = 0;
            span_idx += 1;
            if span_idx >= spans.len() {
                return out;
            }
            curr_idx += spans[span_idx].offset;
        }
        out.push(curr_idx);
        idx_in_span += 1;
    }
    out
}

/// Rebuilds the minimal span encoding for the given ascending
/// `(index, value)` pairs — every pair is kept verbatim, zero values
/// included (callers that must strip zeros — [`compact_side`], the NHCB
/// reconcile's both-zero exclusion — filter BEFORE calling).
fn rebuild_spans(items: impl Iterator<Item = (i32, f64)>) -> (Vec<Span>, Vec<f64>) {
    let mut spans: Vec<Span> = Vec::new();
    let mut buckets: Vec<f64> = Vec::new();
    let mut last_idx: Option<i32> = None;
    for (idx, v) in items {
        match last_idx {
            Some(last) if idx == last + 1 => {
                spans
                    .last_mut()
                    .expect("last_idx is only Some once a span has been pushed")
                    .length += 1;
            }
            Some(last) => spans.push(Span {
                offset: idx - last - 1,
                length: 1,
            }),
            None => spans.push(Span {
                offset: idx,
                length: 1,
            }),
        }
        buckets.push(v);
        last_idx = Some(idx);
    }
    (spans, buckets)
}

/// `Compact(0)` for one side — see [`FloatHistogram::compact`]'s doc.
fn compact_side(spans: &[Span], buckets: &[f64]) -> (Vec<Span>, Vec<f64>) {
    let indices = span_indices(spans, buckets.len());
    rebuild_spans(
        indices
            .into_iter()
            .zip(buckets.iter().copied())
            .filter(|(_, v)| *v != 0.0),
    )
}

/// `mustReduceResolution`/`reduceResolution` (`generic.go:782`,
/// `deltaBuckets=false`) for one side — see
/// [`FloatHistogram::copy_to_schema`]'s doc.
fn reduce_resolution_side(
    spans: &[Span],
    buckets: &[f64],
    origin_schema: i32,
    target_schema: i32,
) -> (Vec<Span>, Vec<f64>) {
    let pairs = reduce_pairs(&indexed_pairs(spans, buckets), origin_schema, target_schema);
    rebuild_spans(pairs.into_iter())
}

/// [`reduce_resolution_side`] on already-decoded pairs; a no-op copy when
/// the schemas match. Zero-count target buckets are KEPT (a target bucket
/// exists iff at least one origin bucket maps to it — upstream appends
/// them verbatim, no zero-stripping).
fn reduce_pairs(pairs: &[(i32, f64)], origin_schema: i32, target_schema: i32) -> Vec<(i32, f64)> {
    if origin_schema == target_schema {
        return pairs.to_vec();
    }
    let mut target: BTreeMap<i32, f64> = BTreeMap::new();
    for &(idx, v) in pairs {
        let t = target_idx(idx, origin_schema, target_schema);
        target.entry(t).and_modify(|x| *x += v).or_insert(v);
    }
    target.into_iter().collect()
}

/// `targetIdx` (`generic.go:1409`): the schema-`target_schema` index a
/// `origin_schema`-index bucket rolls up into when reducing resolution.
fn target_idx(idx: i32, origin_schema: i32, target_schema: i32) -> i32 {
    ((idx - 1) >> (origin_schema - target_schema)) + 1
}

/// `addBuckets` (`float_histogram.go:1420`) on the index-keyed
/// representation: `a`'s pairs verbatim (zero values included), then
/// `b`'s pairs folded in storage order — one `x += sign*v` per shared
/// index, `sign*v` inserted verbatim for a new index — skipping `b`'s
/// buckets for which `skip_b(index)` holds (the fully-inside-the-zero-
/// bucket skip; pass `|_| false` for NHCB, mirroring the pin's
/// `IsExponentialSchema` gate). Zero-count buckets from EITHER side
/// survive into the result — normalization is [`FloatHistogram::compact`]'s
/// job, exactly like the pin.
fn merge_indexed_union(
    a: &[(i32, f64)],
    b: &[(i32, f64)],
    sign: f64,
    skip_b: impl Fn(i32) -> bool,
) -> (Vec<Span>, Vec<f64>) {
    let mut merged: BTreeMap<i32, f64> = BTreeMap::new();
    for &(idx, v) in a {
        merged.insert(idx, v);
    }
    for &(idx, v) in b {
        if skip_b(idx) {
            continue;
        }
        merged
            .entry(idx)
            .and_modify(|x| *x += sign * v)
            .or_insert(sign * v);
    }
    rebuild_spans(merged.into_iter())
}

/// The self side's evolving state during [`reconcile_zero_buckets`] — the
/// pin mutates the receiving histogram in place (`h.ZeroCount`/
/// `h.ZeroThreshold`/`trimBucketsInZeroBucket`); this port evolves a
/// working decode instead (never mutating the operand).
struct SelfZeroState {
    threshold: f64,
    zero_count: f64,
    positive: Vec<Bucket>,
    negative: Vec<Bucket>,
}

/// [`reconcile_zero_buckets`]'s outcome: the common threshold, each
/// side's zero count at that threshold, and self's possibly-trimmed
/// native-schema decode.
struct ReconciledZero {
    threshold: f64,
    self_zero_count: f64,
    other_zero_count: f64,
    self_positive: Vec<Bucket>,
    self_negative: Vec<Bucket>,
}

/// `reconcileZeroBuckets` (`float_histogram.go:1109-1127`), plain-flow
/// (`c == nil`): ping-pongs between the sides until both share one zero
/// threshold, growing whichever is smaller. When SELF's threshold grows,
/// its buckets now inside the zero bucket are trimmed —
/// `trimBucketsInZeroBucket` (`:1065-1100`) zeroes them (positive:
/// `lower < threshold`; negative: `upper > -threshold`) and then
/// `Compact(0)`s the whole side (the pin "abuses Compact", dropping ALL
/// zero-count buckets of self, pre-existing ones included — mirrored by
/// the combined retain below). OTHER is never mutated — its adjusted zero
/// count is recomputed fresh from its original decode each pass, and its
/// fully-in-zero buckets are excluded later by the merge's threshold
/// skip. All zero-count accumulation runs through [`kahan_inc`] with the
/// compensation discarded exactly where the pin discards it
/// (`reconcileZeroBuckets`'s `nil` compensation arguments and the
/// `_`-dropped `otherCZeroCount` at `Add`/`Sub`'s call sites,
/// `float_histogram.go:358,543`).
fn reconcile_zero_buckets(
    mut own: SelfZeroState,
    other_threshold: f64,
    other_zero_count: f64,
    other_positive: &[Bucket],
    other_negative: &[Bucket],
) -> ReconciledZero {
    let mut other_thr = other_threshold;
    let mut other_zc = other_zero_count;
    while other_thr != own.threshold {
        if own.threshold > other_thr {
            let (zc, thr, _c) = zero_count_for_larger_threshold(
                other_zero_count,
                0.0,
                other_positive,
                other_negative,
                own.threshold,
                other_threshold,
            );
            other_zc = zc;
            other_thr = thr;
        }
        if other_thr > own.threshold {
            let (zc, thr, _c) = zero_count_for_larger_threshold(
                own.zero_count,
                0.0,
                &own.positive,
                &own.negative,
                other_thr,
                own.threshold,
            );
            own.zero_count = zc;
            own.threshold = thr;
            // trimBucketsInZeroBucket: zero the in-zero buckets, then the
            // full-Compact(0) side effect (drop every zero-count bucket).
            own.positive.retain(|b| b.lower >= thr && b.count != 0.0);
            own.negative.retain(|b| b.upper <= -thr && b.count != 0.0);
        }
    }
    ReconciledZero {
        threshold: own.threshold,
        self_zero_count: own.zero_count,
        other_zero_count: other_zc,
        self_positive: own.positive,
        self_negative: own.negative,
    }
}

/// `zeroCountForLargerThreshold` (`float_histogram.go:992-1059`),
/// plain-flow (`c == nil`, so the per-bucket compensation-bucket adds are
/// absent): the zero count (and possibly-further-adjusted threshold) a
/// histogram would have at `larger_threshold`, given its native-schema
/// decode. Accumulates via [`kahan_inc`] exactly like the pin — including
/// the redo-loop quirk that the compensation term persists across the
/// `continue outer` retry while the primary count resets (`:1005-1007`).
/// Returns `(zero_count, threshold, compensation)`; every current caller
/// discards the compensation at the pin's own discard points.
fn zero_count_for_larger_threshold(
    zero_count: f64,
    c_zero_count: f64,
    positive: &[Bucket],
    negative: &[Bucket],
    larger_threshold: f64,
    from_threshold: f64,
) -> (f64, f64, f64) {
    if larger_threshold == from_threshold {
        return (zero_count, larger_threshold, c_zero_count);
    }
    let mut threshold = larger_threshold;
    let mut c = c_zero_count;
    'outer: loop {
        let mut zc = zero_count;
        for b in positive {
            if b.lower >= threshold {
                break;
            }
            let (t, nc) = kahan_inc(b.count, zc, c);
            zc = t;
            c = nc;
            if b.upper > threshold {
                // New threshold ended up within a bucket; if it's
                // populated, adjust before we are done here.
                if b.count != 0.0 {
                    threshold = b.upper;
                }
                break;
            }
        }
        for b in negative {
            if b.upper <= -threshold {
                break;
            }
            let (t, nc) = kahan_inc(b.count, zc, c);
            zc = t;
            c = nc;
            if b.lower < -threshold {
                // New threshold within a negative bucket: if it's
                // populated, adjust and redo the whole thing (the
                // positive-side treatment is invalid now).
                if b.count != 0.0 {
                    threshold = -b.lower;
                    continue 'outer;
                }
                break;
            }
        }
        return (zc, threshold, c);
    }
}

/// One side, reduced from `origin_schema` to `target_schema` (a no-op
/// when equal) and — for exponential schemas ONLY (the pin's
/// `absoluteStartValue` skip is `IsExponentialSchema`-gated,
/// `float_histogram.go:1314`; NHCB bounds may be negative, so an
/// unconditional filter would wrongly drop real buckets) — restricted to
/// buckets not entirely inside `[-zero_threshold, zero_threshold]`: keep
/// positive iff `upper > threshold`, negative iff `|lower| > threshold`
/// (the pin skips while `getBoundExponential(currIdx) <=
/// absoluteStartValue`, and that bound is the upper edge for a positive
/// bucket / |lower| for a negative one). The exact substrate
/// `DetectReset`'s `floatBucketIterator(positive, h.ZeroThreshold,
/// h.Schema)` walks.
#[allow(clippy::too_many_arguments)]
fn filtered_at_schema(
    spans: &[Span],
    buckets: &[f64],
    positive: bool,
    origin_schema: i32,
    target_schema: i32,
    zero_threshold: f64,
    custom_values: &[f64],
) -> std::collections::HashMap<i32, f64> {
    let (spans, buckets) = if origin_schema == target_schema {
        (spans.to_vec(), buckets.to_vec())
    } else {
        reduce_resolution_side(spans, buckets, origin_schema, target_schema)
    };
    let decoded = natural_side_buckets(&spans, &buckets, positive, target_schema, custom_values);
    let exponential = !is_custom_buckets_schema(target_schema);
    decoded
        .into_iter()
        .filter(|b| {
            if !exponential {
                return true;
            }
            if positive {
                b.upper > zero_threshold
            } else {
                b.lower < -zero_threshold
            }
        })
        .map(|b| (b.index, b.count))
        .collect()
}

/// `detectReset` (`float_histogram.go:808`): a reset iff any bucket index
/// populated in `prev` is missing (implicit `0.0`) or smaller in `curr` —
/// upstream's two-iterator walk never flags a bucket that exists only in
/// `curr`, so a single pass over `prev`'s indices suffices.
fn detect_reset_side(
    curr: &std::collections::HashMap<i32, f64>,
    prev: &std::collections::HashMap<i32, f64>,
) -> bool {
    prev.iter()
        .any(|(idx, &prev_count)| curr.get(idx).copied().unwrap_or(0.0) < prev_count)
}

#[cfg(test)]
mod ops_tests {
    use super::*;
    use crate::histogram::CUSTOM_BUCKETS_SCHEMA;

    /// Schema-0 exponential, positive buckets (0.5,1]:a, (1,2]:b, (2,4]:c.
    fn exp_hist(count: u64, sum: f64, abs_buckets: [i64; 3]) -> FloatHistogram {
        let deltas = [
            abs_buckets[0],
            abs_buckets[1] - abs_buckets[0],
            abs_buckets[2] - abs_buckets[1],
        ];
        NativeHistogram {
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

    /// NHCB with the given custom bounds; `abs_buckets` are the absolute
    /// counts of the leading buckets (span offset 0).
    fn nhcb_hist(count: u64, sum: f64, bounds: Vec<f64>, abs_buckets: Vec<i64>) -> FloatHistogram {
        let mut deltas = vec![abs_buckets[0]];
        for w in abs_buckets.windows(2) {
            deltas.push(w[1] - w[0]);
        }
        NativeHistogram {
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

    #[test]
    fn mul_scales_count_sum_and_every_bucket() {
        let mut h = exp_hist(4, 5.0, [1, 2, 1]);
        h.mul(2.0);
        assert_eq!(h.count, 8.0);
        assert_eq!(h.sum, 10.0);
        assert_eq!(h.positive_buckets, vec![2.0, 4.0, 2.0]);
    }

    #[test]
    fn div_scales_count_sum_and_every_bucket() {
        let mut h = exp_hist(4, 5.0, [1, 2, 1]);
        h.div(2.0);
        assert_eq!(h.count, 2.0);
        assert_eq!(h.sum, 2.5);
        assert_eq!(h.positive_buckets, vec![0.5, 1.0, 0.5]);
    }

    #[test]
    fn div_by_zero_clears_every_bucket() {
        let mut h = exp_hist(4, 5.0, [1, 2, 1]);
        h.div(0.0);
        assert!(h.positive_buckets.is_empty());
        assert!(h.positive_spans.is_empty());
        assert!(h.count.is_infinite());
    }

    #[test]
    fn compact_drops_zero_valued_buckets_and_rebuilds_minimal_spans() {
        let mut h = exp_hist(2, 2.0, [1, 0, 1]);
        h.compact();
        // Absolute [1, 0, 1] -> the middle bucket (index 1) drops; two
        // surviving buckets at indices 0 and 2, split into two spans
        // (offset 0 length 1, then offset 1 length 1).
        assert_eq!(h.positive_buckets, vec![1.0, 1.0]);
        assert_eq!(h.positive_spans.len(), 2);
        assert_eq!(
            h.positive_spans[0],
            Span {
                offset: 0,
                length: 1
            }
        );
        assert_eq!(
            h.positive_spans[1],
            Span {
                offset: 1,
                length: 1
            }
        );
    }

    #[test]
    fn copy_to_schema_reduces_resolution_merging_adjacent_buckets() {
        // Schema 1 has 2 sub-buckets per octave; indices 1,2 both map to
        // schema-0 index 1 (`targetIdx(idx,1,0) = ((idx-1)>>1)+1`: idx=1
        // -> 1, idx=2 -> 1), so their counts (1, 2) merge into one target
        // bucket.
        let h = NativeHistogram {
            schema: 1,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 3,
            sum: 3.0,
            positive_spans: vec![Span {
                offset: 1,
                length: 2,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, 1], // absolute [1, 2]
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let reduced = h.copy_to_schema(0);
        assert_eq!(reduced.schema, 0);
        assert_eq!(reduced.count, 3.0);
        assert_eq!(reduced.positive_buckets, vec![3.0]);
    }

    #[test]
    fn copy_to_schema_same_schema_is_a_cheap_clone() {
        let h = exp_hist(4, 5.0, [1, 2, 1]);
        let same = h.copy_to_schema(0);
        assert!(h.bits_eq(&same));
    }

    /// `reduceResolution` keeps zero-count target buckets (the pin
    /// appends them verbatim; a target bucket exists iff at least one
    /// origin bucket maps to it) — no zero-stripping during reduction.
    #[test]
    fn copy_to_schema_keeps_zero_count_target_buckets() {
        let h = NativeHistogram {
            schema: 1,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 1,
            sum: 1.0,
            positive_spans: vec![Span {
                offset: 1,
                length: 4,
            }],
            negative_spans: vec![],
            // Absolute [0, 0, 1, 0]: schema-1 indices 1..=4; indices 1,2
            // -> target 1 (both zero), 3,4 -> target 2 (1 + 0).
            positive_buckets: vec![0, 0, 1, -1],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let reduced = h.copy_to_schema(0);
        assert_eq!(reduced.positive_buckets, vec![0.0, 1.0]);
        assert_eq!(
            reduced.positive_spans,
            vec![Span {
                offset: 1,
                length: 2
            }]
        );
    }

    #[test]
    fn combine_add_same_schema_sums_matching_buckets() {
        let a = exp_hist(4, 5.0, [1, 2, 1]);
        let b = exp_hist(4, 5.0, [1, 2, 1]);
        let outcome = a.combine(&b, CombineOp::Add).unwrap();
        assert_eq!(outcome.result.count, 8.0);
        assert_eq!(outcome.result.sum, 10.0);
        assert_eq!(outcome.result.positive_buckets, vec![2.0, 4.0, 2.0]);
        assert!(!outcome.nhcb_bounds_reconciled);
    }

    /// The #124 A5b-ii codex round-1 finding 1(b): `Sub` must NOT strip
    /// the zero-count buckets it produces — upstream defers to a lazy
    /// `Compact` (`Add`'s own doc: the result "might have buckets with a
    /// population of zero"). Identical-minus-identical keeps the full
    /// three-bucket layout at value 0; only `compact()` strips it.
    #[test]
    fn combine_sub_preserves_zero_buckets_until_compact() {
        let a = exp_hist(4, 5.0, [1, 2, 1]);
        let b = exp_hist(4, 5.0, [1, 2, 1]);
        let mut result = a.combine(&b, CombineOp::Sub).unwrap().result;
        assert_eq!(result.count, 0.0);
        assert_eq!(result.sum, 0.0);
        assert_eq!(
            result.positive_buckets,
            vec![0.0, 0.0, 0.0],
            "zero buckets survive Sub (lazy Compact, like the pin)"
        );
        assert_eq!(
            result.positive_spans,
            vec![Span {
                offset: 0,
                length: 3
            }]
        );
        result.compact();
        assert!(result.positive_buckets.is_empty());
        assert!(result.positive_spans.is_empty());
    }

    /// A zero-count bucket present in only ONE operand also survives the
    /// merge (union layout, no eager strip).
    #[test]
    fn combine_add_preserves_an_explicit_zero_bucket_from_one_operand() {
        let a = exp_hist(2, 2.0, [1, 0, 1]); // explicit zero at index 1
        let b = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 1,
            sum: 1.0,
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
        let outcome = a.combine(&b, CombineOp::Add).unwrap();
        assert_eq!(outcome.result.positive_buckets, vec![2.0, 0.0, 1.0]);
    }

    #[test]
    fn combine_add_reduces_to_the_common_minimum_schema() {
        // Schema-1 histogram (2 buckets, indices 1,2 -> abs [1,1]) added
        // to a schema-0 histogram (1 bucket, index 1 -> abs [5]).
        let hi_res = NativeHistogram {
            schema: 1,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 2,
            sum: 2.0,
            positive_spans: vec![Span {
                offset: 1,
                length: 2,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, 0], // absolute [1, 1]
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let lo_res = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 5,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![5],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let outcome = hi_res.combine(&lo_res, CombineOp::Add).unwrap();
        assert_eq!(outcome.result.schema, 0);
        assert_eq!(outcome.result.count, 7.0);
        // Both hi_res buckets (indices 1,2 -> schema-0 index 1) plus
        // lo_res's own index-1 bucket land in the same target bucket:
        // 1+1+5 = 7.
        assert_eq!(outcome.result.positive_buckets, vec![7.0]);
    }

    #[test]
    fn combine_add_reconciles_a_smaller_zero_threshold_into_the_larger() {
        // `a`'s zero bucket is [-1,1]; `b` has NO zero bucket but a
        // populated bucket entirely inside [-1,1] (schema-0 bucket index
        // 0, (0.5,1]). Adding must roll that bucket into the zero count
        // (b's own layout is untouched; the merge's threshold skip
        // excludes the in-zero bucket).
        let a = NativeHistogram {
            schema: 0,
            zero_threshold: 1.0,
            zero_count: 3,
            count: 3,
            sum: 0.0,
            positive_spans: vec![],
            negative_spans: vec![],
            positive_buckets: vec![],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let b = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 2,
            sum: 1.5,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![2],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let outcome = a.combine(&b, CombineOp::Add).unwrap();
        assert_eq!(outcome.result.zero_threshold, 1.0);
        assert_eq!(outcome.result.zero_count, 5.0, "3 + b's 2 rolled in");
        assert!(
            outcome.result.positive_buckets.is_empty(),
            "b's only bucket is fully inside the zero region"
        );
    }

    /// The #124 A5b-ii codex round-1 finding 1(a) regression, pin order:
    /// zero-bucket reconciliation runs BEFORE resolution reduction, at
    /// each operand's NATIVE schema (`reconcileZeroBuckets` precedes the
    /// schema `switch` in `Add`). `self` is schema 1 with a populated
    /// bucket (1, 2^0.5]; `other`'s larger threshold (1.3) lands inside
    /// it, so the common threshold grows to that bucket's NATIVE upper
    /// bound 2^0.5 = 1.4142135623730951 — NOT to 2.0, which is what
    /// reconciling after reduction to the common schema 0 (bucket (1,2])
    /// would produce.
    #[test]
    fn combine_reconciles_zero_threshold_at_native_schema_before_reduction() {
        let a = NativeHistogram {
            schema: 1,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 2,
            sum: 3.0,
            positive_spans: vec![Span {
                offset: 1,
                length: 4,
            }],
            negative_spans: vec![],
            // Absolute [1, 0, 0, 1]: populated at schema-1 indices 1
            // ((1, 1.414]) and 4 ((2, 2.828]).
            positive_buckets: vec![1, -1, 0, 1],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let b = NativeHistogram {
            schema: 0,
            zero_threshold: 1.3,
            zero_count: 5,
            count: 5,
            sum: 0.0,
            positive_spans: vec![],
            negative_spans: vec![],
            positive_buckets: vec![],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let outcome = a.combine(&b, CombineOp::Add).unwrap();
        // The native schema-1 bucket upper — the pin's own
        // `exponentialBounds` table value 0.7071067811865475 * 2, one ULP
        // BELOW `f64::consts::SQRT_2` (hence the bit-level derivation
        // here rather than a decimal literal clippy would flag as an
        // approximated constant) — not the reduced schema-0 bucket
        // upper 2.0.
        let table_sqrt2 = 0.7071067811865475f64 * 2.0;
        assert!(table_sqrt2 < std::f64::consts::SQRT_2);
        assert_eq!(outcome.result.zero_threshold, table_sqrt2);
        // a's in-zero bucket (count 1) rolled into the zero count: 1 + 5.
        assert_eq!(outcome.result.zero_count, 6.0);
        assert_eq!(outcome.result.schema, 0);
        // a's surviving bucket: schema-1 index 4 -> schema-0 index 2,
        // count 1.
        assert_eq!(outcome.result.positive_buckets, vec![1.0]);
        assert_eq!(
            outcome.result.positive_spans,
            vec![Span {
                offset: 2,
                length: 1
            }]
        );
    }

    /// The negative-side redo loop of `zeroCountForLargerThreshold`
    /// (`continue outer`): a populated negative bucket straddling the
    /// growing threshold pushes the threshold to |lower| and restarts.
    #[test]
    fn combine_zero_threshold_growth_redoes_on_a_straddling_negative_bucket() {
        // self: only a negative bucket (-2,-1]:3 (schema 0, index 1),
        // zero threshold 0. other: threshold 1.3, zero count 10.
        let a = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 3,
            sum: -4.0,
            positive_spans: vec![],
            negative_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            positive_buckets: vec![],
            negative_buckets: vec![3],
            custom_values: vec![],
        }
        .to_float();
        let b = NativeHistogram {
            schema: 0,
            zero_threshold: 1.3,
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
        let outcome = a.combine(&b, CombineOp::Add).unwrap();
        // (-2,-1] straddles 1.3 -> threshold grows to |-2| = 2, redo
        // confirms, bucket absorbed into the zero count.
        assert_eq!(outcome.result.zero_threshold, 2.0);
        assert_eq!(outcome.result.zero_count, 13.0);
        assert!(outcome.result.negative_buckets.is_empty());
    }

    #[test]
    fn combine_nhcb_matching_bounds_sums_per_bound() {
        let a = nhcb_hist(4, 5.0, vec![5.0, 10.0], vec![1, 2, 1]);
        let b = nhcb_hist(4, 5.0, vec![5.0, 10.0], vec![1, 2, 1]);
        let outcome = a.combine(&b, CombineOp::Add).unwrap();
        assert_eq!(outcome.result.count, 8.0);
        assert_eq!(outcome.result.positive_buckets, vec![2.0, 4.0, 2.0]);
        assert_eq!(outcome.result.custom_values, vec![5.0, 10.0]);
        assert!(!outcome.nhcb_bounds_reconciled);
    }

    /// The `native_histograms.test` `nhcb_metric` shape (`:1291`,
    /// bounds [5,10] minus bounds [5]): `Sub` reconciles to the
    /// intersection [5]; both operands' single (-Inf,5] bucket cancels
    /// exactly, leaving no buckets — the corpus's pinned
    /// `increase(nhcb_metric[13m]) == {{schema:-53 custom_values:[5]}}`
    /// substrate.
    #[test]
    fn combine_nhcb_mismatched_bounds_reconciles_to_the_intersection() {
        let last = nhcb_hist(1, 1.0, vec![5.0, 10.0], vec![1]);
        let first = nhcb_hist(1, 1.0, vec![5.0], vec![1]);
        let outcome = last.combine(&first, CombineOp::Sub).unwrap();
        assert!(outcome.nhcb_bounds_reconciled);
        assert_eq!(outcome.result.custom_values, vec![5.0]);
        assert_eq!(outcome.result.count, 0.0);
        assert_eq!(outcome.result.sum, 0.0);
        assert!(
            outcome.result.positive_buckets.is_empty(),
            "the (-Inf,5] contributions cancel exactly (both primary and compensation zero)"
        );
    }

    /// Add across mismatched bounds: contributions map onto the
    /// intersected layout — a bucket bounded above the last common bound
    /// rolls into the trailing +Inf bucket.
    #[test]
    fn combine_nhcb_mismatched_bounds_add_maps_onto_the_intersection() {
        // a: bounds [5], buckets (-Inf,5]:2 -- count 2.
        // b: bounds [5,10], buckets (-Inf,5]:1, (5,10]:3 -- count 4.
        let a = nhcb_hist(2, 4.0, vec![5.0], vec![2]);
        let b = nhcb_hist(4, 20.0, vec![5.0, 10.0], vec![1, 3]);
        let outcome = a.combine(&b, CombineOp::Add).unwrap();
        assert!(outcome.nhcb_bounds_reconciled);
        assert_eq!(outcome.result.custom_values, vec![5.0]);
        // Target layout over [5]: [(-Inf,5], (5,+Inf]]. a: 2 -> idx 0.
        // b: 1 -> idx 0; 3 (bound 10 > 5) -> the +Inf bucket idx 1.
        assert_eq!(outcome.result.positive_buckets, vec![3.0, 3.0]);
        assert_eq!(outcome.result.count, 6.0);
    }

    #[test]
    fn combine_exponential_and_nhcb_is_incompatible() {
        let a = exp_hist(4, 5.0, [1, 2, 1]);
        let b = nhcb_hist(4, 5.0, vec![5.0, 10.0], vec![1, 2, 1]);
        assert_eq!(
            a.combine(&b, CombineOp::Sub).unwrap_err(),
            FloatHistogramOpError::IncompatibleSchema
        );
    }

    #[test]
    fn detect_reset_false_for_monotonic_growth() {
        let prev = exp_hist(4, 5.0, [1, 2, 1]);
        let curr = exp_hist(6, 7.0, [1, 3, 2]);
        assert!(!curr.detect_reset(&prev));
    }

    #[test]
    fn detect_reset_true_for_a_single_bucket_decrease() {
        // native_histograms.test's reset_in_bucket case: count/sum both
        // rose, but bucket index 1 dropped 2 -> 1.
        let prev = exp_hist(4, 5.0, [1, 2, 1]);
        let curr = exp_hist(5, 6.0, [1, 1, 3]);
        assert!(curr.detect_reset(&prev));
    }

    #[test]
    fn detect_reset_true_when_a_populated_bucket_disappears() {
        let prev = exp_hist(4, 5.0, [1, 2, 1]);
        let curr = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![4],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        assert!(curr.detect_reset(&prev));
    }

    #[test]
    fn detect_reset_true_for_a_schema_increase() {
        let prev = exp_hist(4, 5.0, [1, 2, 1]);
        let curr = NativeHistogram {
            schema: 1,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 2,
            }],
            negative_spans: vec![],
            positive_buckets: vec![4, 0],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        assert!(curr.detect_reset(&prev));
    }

    #[test]
    fn detect_reset_false_for_a_schema_decrease_that_reduces_previous_cleanly() {
        // `prev` at schema 1 (fine resolution, indices 1,2 -> abs [1,1]),
        // `curr` at schema 0 (coarser) with the merged, non-decreased
        // total (2) in the corresponding bucket.
        let prev = NativeHistogram {
            schema: 1,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 2,
            sum: 2.0,
            positive_spans: vec![Span {
                offset: 1,
                length: 2,
            }],
            negative_spans: vec![],
            positive_buckets: vec![1, 0],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        let curr = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 2,
            sum: 2.0,
            positive_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![2],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        assert!(!curr.detect_reset(&prev));
    }

    /// The `native_histograms.test` `nhcb_metric` differential
    /// (`resets(nhcb_metric[13m]) == 0`, `:1334`): a custom-bounds CHANGE
    /// ([5] -> [5,10]) with consistent rolled-up counts is NOT a reset —
    /// the round-1 conservative-`true` would have counted 1 here.
    #[test]
    fn detect_reset_nhcb_bounds_change_with_consistent_rollups_is_not_a_reset() {
        let prev = nhcb_hist(1, 1.0, vec![5.0], vec![1]);
        let curr = nhcb_hist(1, 1.0, vec![5.0, 10.0], vec![1]);
        assert!(!curr.detect_reset(&prev));
    }

    /// A genuine rolled-up decrease at a COMMON bound IS a reset.
    #[test]
    fn detect_reset_nhcb_mismatched_bounds_rolled_up_decrease_is_a_reset() {
        // prev: bounds [5], (-Inf,5]:2. curr: bounds [5,10],
        // (-Inf,5]:1, (5,10]:1 -- same total count (no count decrease),
        // but the rollup at the common bound 5 dropped 2 -> 1.
        let prev = nhcb_hist(2, 4.0, vec![5.0], vec![2]);
        let curr = nhcb_hist(2, 8.0, vec![5.0, 10.0], vec![1, 1]);
        assert!(curr.detect_reset(&prev));
    }

    /// Redistribution WITHIN a common-bound segment does not trip the
    /// rollup comparison (only common bounds are compared).
    #[test]
    fn detect_reset_nhcb_redistribution_within_a_segment_is_not_a_reset() {
        // prev: bounds [5,10], buckets 1,1. curr: bounds [10], bucket 2.
        // Common bounds: [10]. Rollup at 10: prev 1+1=2, curr 2 -> equal.
        let prev = nhcb_hist(2, 6.0, vec![5.0, 10.0], vec![1, 1]);
        let curr = nhcb_hist(2, 6.0, vec![10.0], vec![2]);
        assert!(!curr.detect_reset(&prev));
    }

    #[test]
    fn equals_true_for_bit_identical_histograms() {
        let a = exp_hist(4, 5.0, [1, 2, 1]);
        let b = exp_hist(4, 5.0, [1, 2, 1]);
        assert!(a.equals(&b));
    }

    #[test]
    fn equals_tolerates_differing_span_encodings_of_the_same_layout() {
        // Same populated (index, value) layout as `exp_hist`'s
        // [1,2,1]@offset0 (indices 0,1,2), but split into two real spans
        // separated by a (structurally meaningless, zero-offset)
        // zero-length span in between -- `spansMatch`'s own documented
        // tolerance (`histogram.go:287`).
        let a = exp_hist(4, 5.0, [1, 2, 1]);
        let b = NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![
                Span {
                    offset: 0,
                    length: 1,
                },
                Span {
                    offset: 0,
                    length: 0,
                },
                Span {
                    offset: 0,
                    length: 2,
                },
            ],
            negative_spans: vec![],
            positive_buckets: vec![1, 1, -1],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        assert!(a.equals(&b));
        // But NOT bits_eq -- the raw span arrays genuinely differ.
        assert!(!a.bits_eq(&b));
    }

    #[test]
    fn equals_false_for_a_differing_bucket_value() {
        let a = exp_hist(4, 5.0, [1, 2, 1]);
        let b = exp_hist(4, 5.0, [1, 3, 0]);
        assert!(!a.equals(&b));
    }

    #[test]
    fn equals_false_for_differing_schema() {
        let a = exp_hist(4, 5.0, [1, 2, 1]);
        let b = NativeHistogram {
            schema: 1,
            zero_threshold: 0.0,
            zero_count: 0,
            count: 4,
            sum: 5.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            positive_buckets: vec![4],
            negative_buckets: vec![],
            custom_values: vec![],
        }
        .to_float();
        assert!(!a.equals(&b));
    }

    #[test]
    fn equals_false_for_differing_nhcb_bounds() {
        let a = nhcb_hist(1, 1.0, vec![5.0], vec![1]);
        let b = nhcb_hist(1, 1.0, vec![5.0, 10.0], vec![1]);
        assert!(!a.equals(&b));
    }
}
