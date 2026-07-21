// M7-A5b-iii: `FloatHistogram::kahan_add` — the FULL-histogram compensated
// summation `sum`/`avg` aggregation and `sum_over_time`/`avg_over_time`
// fold through (issue #124 plan v2 OQ3: `KahanAdd` is the only histogram
// op that carries compensation; every other consumer — rate/increase/
// binops — uses the plain `Add`/`Sub` `float_histogram_ops.rs` ports).
// Ported from the pinned `model/histogram/float_histogram.go` (v3.13.0,
// `40af9c2`, via `git show 40af9c2:model/histogram/float_histogram.go`):
// `KahanAdd` (`:417-531`), `kahanAddBuckets` (`:1541-1670`),
// `kahanReduceResolution` (`:1945-2033`), `zeroCountForLargerThreshold`'s
// `c != nil` flow (`:992-1059`), `trimBucketsInZeroBucket` + `kahanCompact`
// (`:1065-1100`, `:710-720`), and `addCustomBucketsWithMismatches`'
// `withCompensation` arm (`:1812-1902`, shared with
// `float_histogram_ops.rs`).
//
// **Everything is compensated, exactly like the pin** (the #124 A5b-iii
// codex round-1 [high] finding — a prior draft compensated only the
// `count`/`sum` scalars): the compensation is a full parallel
// [`FloatHistogram`] (upstream `newCompensationHistogram`, `:2040-2054`)
// whose `count`/`sum`/`zero_count` and EVERY bucket carry the running
// `kahansum.Inc` low-order remainder, threaded through zero-bucket
// reconciliation, schema reduction, the bucket merge, and NHCB
// mismatched-bounds mapping. The caller performs the pin's final flush
// (`sum.Add(comp)` — `engine.go:3877-3899`, `functions.go:1547-1551`)
// when the fold ends.
//
// **Structural invariant (upstream's own):** the returned compensation
// shares the result's spans with bucket arrays parallel position-for-
// position (`c.PositiveSpans = h.PositiveSpans`, `:513-515`); a
// compensation passed back INTO `kahan_add` must be the one returned for
// the same running histogram (or `None` for the first fold step —
// upstream's `nil`). Violations are a caller bug (debug-asserted;
// out-of-range positions read as `0.0` rather than panicking on
// untrusted data, the crate's convention).
//
// Implementation strategy: the same index-keyed merge as
// `float_histogram_ops.rs` (see that module's doc for the bit-identity
// argument), extended to `(index, value, compensation)` triples — each
// shared index performs the pin's exact `Inc(value)`-then-`Inc(comp)`
// pair in the pin's order, each unshared index inserts `(value, comp)`
// verbatim (`kahanAddBuckets`' insert arms copy `compensationBucketB`
// alongside `bucketB`).

/// `(schema index, bucket value, compensation)` — the working triple for
/// the Kahan bucket walks.
type KTriple = (i32, f64, f64);

/// The result of [`FloatHistogram::kahan_add`]: the folded histogram, the
/// updated full compensation histogram to pass into the NEXT fold step
/// (upstream `KahanAdd`'s `updatedC` return), and whether NHCB custom
/// bounds needed reconciling (callers emit
/// `NewMismatchedCustomBucketsHistogramsInfo` with
/// `HistogramOperation::Agg`, `annotations.go:463`).
#[derive(Debug, Clone)]
pub struct KahanAddOutcome {
    pub result: FloatHistogram,
    pub compensation: FloatHistogram,
    pub nhcb_bounds_reconciled: bool,
    /// `CounterReset` met `NotCounterReset` (issue #125) — upstream
    /// `KahanAdd`'s `counterResetCollision` return via `adjustCounterReset`
    /// (`float_histogram.go:421`, `:2070-2094`). The aggregation callers
    /// discard it (the pin's `_` — they track input-hint collisions
    /// themselves); it is carried for pin-parity of the op itself.
    pub counter_reset_collision: bool,
}

impl FloatHistogram {
    /// `KahanAdd` (`float_histogram.go:417-531`) — `self + other` with
    /// full Neumaier compensation. `c` is the fold's running compensation
    /// (`None` for the first addition — upstream's `nil`, which allocates
    /// a zero `newCompensationHistogram`), structurally parallel to
    /// `self` (see the module doc's invariant).
    pub fn kahan_add(
        &self,
        other: &FloatHistogram,
        c: Option<&FloatHistogram>,
    ) -> Result<KahanAddOutcome, FloatHistogramOpError> {
        // `checkSchemaAndBounds` (`:2058-2064`).
        if self.uses_custom_buckets() != other.uses_custom_buckets() {
            return Err(FloatHistogramOpError::IncompatibleSchema);
        }
        // `adjustCounterReset` (`:421`) — right after the schema check,
        // exactly like the plain `Add` (issue #125).
        let (counter_reset_hint, counter_reset_collision) =
            adjust_counter_reset(self.counter_reset_hint, other.counter_reset_hint);
        // The compensation's hint (issue #125, load-bearing for the
        // callers' final flush `Add`): the pin's `newCompensationHistogram`
        // copies the receiver's ALREADY-ADJUSTED hint at creation
        // (`float_histogram.go:2035-2043`, called AFTER `adjustCounterReset`
        // at `:421-426`), and later KahanAdds never touch the passed-in
        // `c`'s hint — so a fold of all-NotCounterReset inputs flushes
        // `adjust(NCR, NCR) = NCR`, not `adjust(NCR, Unknown) = Unknown`.
        let compensation_hint = match c {
            Some(ch) => ch.counter_reset_hint,
            None => counter_reset_hint,
        };
        // `if c == nil { c = h.newCompensationHistogram() }` (`:424-426`):
        // zero-valued arrays parallel to self's.
        let (c_pos, c_neg, c_zero_in, c_count_in, c_sum_in) = match c {
            Some(ch) => {
                debug_assert_eq!(
                    ch.positive_buckets.len(),
                    self.positive_buckets.len(),
                    "compensation must be structurally parallel to the running sum"
                );
                debug_assert_eq!(
                    ch.negative_buckets.len(),
                    self.negative_buckets.len(),
                    "compensation must be structurally parallel to the running sum"
                );
                (
                    ch.positive_buckets.clone(),
                    ch.negative_buckets.clone(),
                    ch.zero_count,
                    ch.count,
                    ch.sum,
                )
            }
            None => (
                vec![0.0; self.positive_buckets.len()],
                vec![0.0; self.negative_buckets.len()],
                0.0,
                0.0,
                0.0,
            ),
        };
        // `h.Count/Sum Inc` (`:432-433`).
        let (count, c_count) = kahan_inc(other.count, self.count, c_count_in);
        let (sum, c_sum) = kahan_inc(other.sum, self.sum, c_sum_in);

        if self.uses_custom_buckets() {
            let a_pairs = indexed_pairs(&self.positive_spans, &self.positive_buckets);
            let b_pairs = indexed_pairs(&other.positive_spans, &other.positive_buckets);
            let (positive_spans, positive_buckets, positive_comps, custom_values, reconciled) =
                if custom_bucket_bounds_match(&self.custom_values, &other.custom_values) {
                    // Matching bounds: `kahanAddBuckets` union merge, no
                    // threshold skip (the `IsExponentialSchema` gate), the
                    // other side carrying no compensation (`nil` B comps,
                    // `:449`).
                    let a = attach_comps(&a_pairs, &c_pos);
                    let b = zero_comps(&b_pairs);
                    let (spans, buckets, comps) = kahan_merge_indexed_union(&a, &b, |_| false);
                    (spans, buckets, comps, self.custom_values.clone(), false)
                } else {
                    // Mismatched bounds: map both onto the intersection,
                    // A's compensation participating (`withCompensation`,
                    // `:1849-1851`); the compensation output is KEPT here
                    // (unlike the plain `Add`/`Sub` flow).
                    let intersected =
                        intersect_custom_bucket_bounds(&self.custom_values, &other.custom_values);
                    let (spans, buckets, comps) = add_custom_buckets_with_mismatches(
                        1.0,
                        &a_pairs,
                        Some(&c_pos),
                        &self.custom_values,
                        &b_pairs,
                        &other.custom_values,
                        &intersected,
                    );
                    (spans, buckets, comps, intersected, true)
                };
            return Ok(KahanAddOutcome {
                result: FloatHistogram {
                    counter_reset_hint,
                    schema: self.schema,
                    zero_threshold: 0.0,
                    zero_count: 0.0,
                    count,
                    sum,
                    positive_spans: positive_spans.clone(),
                    negative_spans: Vec::new(),
                    positive_buckets,
                    negative_buckets: Vec::new(),
                    custom_values: custom_values.clone(),
                },
                // `c.PositiveSpans = h.PositiveSpans`; the mismatch arm
                // also sets `c.CustomValues = intersectedBounds`
                // (`:464-466`).
                compensation: FloatHistogram {
                    counter_reset_hint: compensation_hint,
                    schema: self.schema,
                    zero_threshold: 0.0,
                    zero_count: 0.0,
                    count: c_count,
                    sum: c_sum,
                    positive_spans,
                    negative_spans: Vec::new(),
                    positive_buckets: positive_comps,
                    negative_buckets: Vec::new(),
                    custom_values,
                },
                nhcb_bounds_reconciled: reconciled,
                counter_reset_collision,
            });
        }

        // Exponential. Step 1 (pin order, `:427-431`): reconcile the zero
        // buckets at NATIVE schemas, compensation threaded through self's
        // side (`reconcileZeroBuckets(other, c)`).
        let self_pos = attach_bucket_comps(
            natural_side_buckets(
                &self.positive_spans,
                &self.positive_buckets,
                true,
                self.schema,
                &[],
            ),
            &c_pos,
        );
        let self_neg = attach_bucket_comps(
            natural_side_buckets(
                &self.negative_spans,
                &self.negative_buckets,
                false,
                self.schema,
                &[],
            ),
            &c_neg,
        );
        let other_pos_dec = natural_side_buckets(
            &other.positive_spans,
            &other.positive_buckets,
            true,
            other.schema,
            &[],
        );
        let other_neg_dec = natural_side_buckets(
            &other.negative_spans,
            &other.negative_buckets,
            false,
            other.schema,
            &[],
        );
        let rec = kahan_reconcile_zero_buckets(
            KahanSelfZeroState {
                threshold: self.zero_threshold,
                zero_count: self.zero_count,
                c_zero_count: c_zero_in,
                positive: self_pos,
                negative: self_neg,
            },
            other.zero_threshold,
            other.zero_count,
            &other_pos_dec,
            &other_neg_dec,
        );
        // `h.ZeroCount, c.ZeroCount = Inc(otherZeroCount, …)` (`:429`).
        // The pin's second `Inc(otherCZeroCount, …)` (`:430`) is skipped:
        // `otherCZeroCount` is ALWAYS exactly `0.0` (`reconcileZeroBuckets`
        // passes `nil` compensation for the other side, `:1116`), and
        // `Inc(0, s, c)` is a bitwise no-op (`t = s + 0 = s`;
        // `(s - t) + 0 = 0`).
        let (zero_count, c_zero) = kahan_inc(
            rec.other_zero_count,
            rec.self_zero_count,
            rec.self_c_zero_count,
        );

        // Step 2: resolution reduction to the common minimum schema —
        // `kahanReduceResolution` for whichever side reduces (`:479-510`);
        // the other side's reduction STARTS with zero compensation
        // (`otherCPositiveBuckets = make(...)`, `:495-497`) and may
        // generate one from the reduction's own rounding.
        let target_schema = self.schema.min(other.schema);
        let self_pos_t = kahan_reduce_triples(
            bucket_comp_triples(&rec.self_positive),
            self.schema,
            target_schema,
        );
        let self_neg_t = kahan_reduce_triples(
            bucket_comp_triples(&rec.self_negative),
            self.schema,
            target_schema,
        );
        let other_pos_t = kahan_reduce_triples(
            zero_comps(&indexed_pairs(
                &other.positive_spans,
                &other.positive_buckets,
            )),
            other.schema,
            target_schema,
        );
        let other_neg_t = kahan_reduce_triples(
            zero_comps(&indexed_pairs(
                &other.negative_spans,
                &other.negative_buckets,
            )),
            other.schema,
            target_schema,
        );

        // Step 3: the bucket merge (`kahanAddBuckets`), skipping the other
        // side's fully-in-zero-bucket prefix (`getBoundExponential(indexB,
        // schema) <= threshold`, `:1567`).
        let threshold = rec.threshold;
        let (positive_spans, positive_buckets, positive_comps) =
            kahan_merge_indexed_union(&self_pos_t, &other_pos_t, |idx| {
                get_bound_exponential(idx, target_schema) <= threshold
            });
        let (negative_spans, negative_buckets, negative_comps) =
            kahan_merge_indexed_union(&self_neg_t, &other_neg_t, |idx| {
                get_bound_exponential(idx, target_schema) <= threshold
            });

        Ok(KahanAddOutcome {
            result: FloatHistogram {
                counter_reset_hint,
                schema: target_schema,
                zero_threshold: threshold,
                zero_count,
                count,
                sum,
                positive_spans: positive_spans.clone(),
                negative_spans: negative_spans.clone(),
                positive_buckets,
                negative_buckets,
                custom_values: Vec::new(),
            },
            // `c.Schema = h.Schema; c.ZeroThreshold = h.ZeroThreshold;
            // c.PositiveSpans/NegativeSpans = h.…` (`:525-529`).
            compensation: FloatHistogram {
                counter_reset_hint: compensation_hint,
                schema: target_schema,
                zero_threshold: threshold,
                zero_count: c_zero,
                count: c_count,
                sum: c_sum,
                positive_spans,
                negative_spans,
                positive_buckets: positive_comps,
                negative_buckets: negative_comps,
                custom_values: Vec::new(),
            },
            nhcb_bounds_reconciled: false,
            counter_reset_collision,
        })
    }

    /// `HasOverflow` (`float_histogram.go:2098-2117`): any scalar or
    /// bucket field is `±Inf` — reachable when aggregating enough
    /// histograms to exceed `f64`'s range. `avg`'s direct-mean
    /// calculation switches to an incremental mean once this fires
    /// (`engine.go`'s `AVG` arm, `functions.go`'s `funcAvgOverTime`).
    pub fn has_overflow(&self) -> bool {
        self.zero_count.is_infinite()
            || self.count.is_infinite()
            || self.sum.is_infinite()
            || self.positive_buckets.iter().any(|v| v.is_infinite())
            || self.negative_buckets.iter().any(|v| v.is_infinite())
            || self.custom_values.iter().any(|v| v.is_infinite())
    }
}

/// Zips `(index, value)` pairs with a parallel compensation array
/// (positions beyond the array — a caller-invariant violation — read as
/// `0.0`, never a panic).
fn attach_comps(pairs: &[(i32, f64)], comps: &[f64]) -> Vec<KTriple> {
    pairs
        .iter()
        .enumerate()
        .map(|(i, &(idx, v))| (idx, v, comps.get(i).copied().unwrap_or(0.0)))
        .collect()
}

/// `(index, value)` pairs as zero-compensation triples (the other
/// operand's side — upstream's `nil` compensation).
fn zero_comps(pairs: &[(i32, f64)]) -> Vec<KTriple> {
    pairs.iter().map(|&(idx, v)| (idx, v, 0.0)).collect()
}

/// Zips a decoded bucket side with its parallel compensation array.
fn attach_bucket_comps(buckets: Vec<Bucket>, comps: &[f64]) -> Vec<(Bucket, f64)> {
    buckets
        .into_iter()
        .enumerate()
        .map(|(i, b)| (b, comps.get(i).copied().unwrap_or(0.0)))
        .collect()
}

fn bucket_comp_triples(side: &[(Bucket, f64)]) -> Vec<KTriple> {
    side.iter().map(|(b, c)| (b.index, b.count, *c)).collect()
}

/// Rebuilds the minimal span encoding plus the two parallel bucket
/// arrays (values, compensations) for ascending triples — the Kahan
/// counterpart of `rebuild_spans` (`float_histogram_ops.rs`).
fn kahan_rebuild_spans(items: impl Iterator<Item = KTriple>) -> (Vec<Span>, Vec<f64>, Vec<f64>) {
    let mut spans: Vec<Span> = Vec::new();
    let mut buckets: Vec<f64> = Vec::new();
    let mut comps: Vec<f64> = Vec::new();
    let mut last_idx: Option<i32> = None;
    for (idx, v, c) in items {
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
        comps.push(c);
        last_idx = Some(idx);
    }
    (spans, buckets, comps)
}

/// `kahanAddBuckets` (`float_histogram.go:1541-1670`) on the index-keyed
/// representation (see the module doc): `a`'s triples verbatim; `b`'s
/// folded in ascending order — a shared index performs `Inc(valueB)` then
/// (iff `compB != 0`, the pin's own gate at `:1608,:1625`) `Inc(compB)`;
/// an unshared index inserts `(valueB, compB)` verbatim (the pin's insert
/// arms copy `compensationBucketB` alongside `bucketB`, `:1587-1591,
/// :1634-1640`). `skip_b` is the fully-inside-the-zero-bucket prefix skip.
fn kahan_merge_indexed_union(
    a: &[KTriple],
    b: &[KTriple],
    skip_b: impl Fn(i32) -> bool,
) -> (Vec<Span>, Vec<f64>, Vec<f64>) {
    let mut merged: BTreeMap<i32, (f64, f64)> = a.iter().map(|&(i, v, c)| (i, (v, c))).collect();
    for &(idx, v, c) in b {
        if skip_b(idx) {
            continue;
        }
        match merged.entry(idx) {
            std::collections::btree_map::Entry::Occupied(mut e) => {
                let (av, ac) = *e.get();
                let (nv, nc) = kahan_inc(v, av, ac);
                let (nv, nc) = if c != 0.0 {
                    kahan_inc(c, nv, nc)
                } else {
                    (nv, nc)
                };
                e.insert((nv, nc));
            }
            std::collections::btree_map::Entry::Vacant(e) => {
                e.insert((v, c));
            }
        }
    }
    kahan_rebuild_spans(merged.into_iter().map(|(i, (v, c))| (i, v, c)))
}

/// `kahanReduceResolution` (`float_histogram.go:1945-2033`): the FIRST
/// origin bucket mapping to a target index seeds `(value, comp)` verbatim
/// (`:1985-1988`); every subsequent origin bucket mapping to the SAME
/// target performs `Inc(value)` then `Inc(comp)` (`:1992-2003`) — the
/// reduction itself can therefore GENERATE compensation from its own
/// rounding, which the merge's `compB != 0` arm later folds in. A no-op
/// pass-through when the schemas match.
fn kahan_reduce_triples(
    triples: Vec<KTriple>,
    origin_schema: i32,
    target_schema: i32,
) -> Vec<KTriple> {
    if origin_schema == target_schema {
        return triples;
    }
    let mut out: Vec<KTriple> = Vec::new();
    for (idx, v, c) in triples {
        let t = target_idx(idx, origin_schema, target_schema);
        match out.last_mut() {
            Some((last_t, tv, tc)) if *last_t == t => {
                let (nv, nc) = kahan_inc(v, *tv, *tc);
                let (nv, nc) = kahan_inc(c, nv, nc);
                *tv = nv;
                *tc = nc;
            }
            _ => out.push((t, v, c)),
        }
    }
    out
}

/// The self side's evolving state during [`kahan_reconcile_zero_buckets`]
/// — the compensated counterpart of `float_histogram_ops.rs`'s
/// `SelfZeroState`.
struct KahanSelfZeroState {
    threshold: f64,
    zero_count: f64,
    c_zero_count: f64,
    positive: Vec<(Bucket, f64)>,
    negative: Vec<(Bucket, f64)>,
}

struct KahanReconciledZero {
    threshold: f64,
    self_zero_count: f64,
    self_c_zero_count: f64,
    other_zero_count: f64,
    self_positive: Vec<(Bucket, f64)>,
    self_negative: Vec<(Bucket, f64)>,
}

/// `reconcileZeroBuckets(other, c)` (`float_histogram.go:1109-1127`) with
/// a live compensation: when SELF's threshold grows, its in-zero buckets'
/// values AND compensations fold into the (compensated) zero count
/// (`zeroCountForLargerThreshold`'s `c != nil` arm, `:1016-1018,
/// :1040-1042`), then `trimBucketsInZeroBucket(c)` zeroes both arrays and
/// `kahanCompact(0)`s — dropping every zero-VALUE bucket and its
/// compensation along with it (`compactBuckets` keys emptiness on the
/// PRIMARY buckets only, `generic.go:237-265` — a zero-value bucket's
/// residual compensation is discarded, exactly like the pin). The other
/// side is never mutated and never carries compensation (its
/// `zeroCountForLargerThreshold` call passes `nil`, `:1116`).
fn kahan_reconcile_zero_buckets(
    mut own: KahanSelfZeroState,
    other_threshold: f64,
    other_zero_count: f64,
    other_positive: &[Bucket],
    other_negative: &[Bucket],
) -> KahanReconciledZero {
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
            let (zc, thr, c_zc) = kahan_zero_count_for_larger_threshold(
                own.zero_count,
                own.c_zero_count,
                &own.positive,
                &own.negative,
                other_thr,
                own.threshold,
            );
            own.zero_count = zc;
            own.c_zero_count = c_zc;
            own.threshold = thr;
            // trimBucketsInZeroBucket + kahanCompact(0): zero the in-zero
            // (value, comp) pairs, then drop every zero-VALUE bucket
            // (comp discarded along — primary-keyed, see the fn doc).
            own.positive
                .retain(|(b, _)| b.lower >= thr && b.count != 0.0);
            own.negative
                .retain(|(b, _)| b.upper <= -thr && b.count != 0.0);
        }
    }
    KahanReconciledZero {
        threshold: own.threshold,
        self_zero_count: own.zero_count,
        self_c_zero_count: own.c_zero_count,
        other_zero_count: other_zc,
        self_positive: own.positive,
        self_negative: own.negative,
    }
}

/// `zeroCountForLargerThreshold`'s `c != nil` flow (`float_histogram.go:
/// 992-1059`): per merged-into-zero bucket, `Inc(b.Count)` THEN
/// `Inc(c.Buckets[i])` (`:1016-1018`), with the pin's redo-loop quirk —
/// the compensation persists across the `continue outer` retry while the
/// primary count resets (`:1005-1007`). Returns `(zero_count, threshold,
/// c_zero_count)`.
fn kahan_zero_count_for_larger_threshold(
    zero_count: f64,
    c_zero_count: f64,
    positive: &[(Bucket, f64)],
    negative: &[(Bucket, f64)],
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
        for (b, bc) in positive {
            if b.lower >= threshold {
                break;
            }
            let (t, nc) = kahan_inc(b.count, zc, c);
            zc = t;
            c = nc;
            let (t, nc) = kahan_inc(*bc, zc, c);
            zc = t;
            c = nc;
            if b.upper > threshold {
                if b.count != 0.0 {
                    threshold = b.upper;
                }
                break;
            }
        }
        for (b, bc) in negative {
            if b.upper <= -threshold {
                break;
            }
            let (t, nc) = kahan_inc(b.count, zc, c);
            zc = t;
            c = nc;
            let (t, nc) = kahan_inc(*bc, zc, c);
            zc = t;
            c = nc;
            if b.lower < -threshold {
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

#[cfg(test)]
mod kahan_tests {
    use super::*;
    use crate::histogram::{CUSTOM_BUCKETS_SCHEMA, NativeHistogram, Span};

    /// 2^53 — the largest f64 magnitude range with ulp 1; `2^53 + 1.0`
    /// rounds back to `2^53` (ties-to-even), so `+1.0` folds are EXACTLY
    /// the adversarial case where plain accumulation loses the addend and
    /// Kahan compensation recovers it: two lost `+1.0`s flush to the
    /// exactly representable `2^53 + 2`.
    const BIG: f64 = 9007199254740992.0;
    const BIG_PLUS_2: f64 = 9007199254740994.0;

    fn exp_hist(count: f64, sum: f64, buckets: Vec<f64>) -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![Span {
                offset: 0,
                length: buckets.len() as u32,
            }],
            negative_spans: vec![],
            positive_buckets: buckets,
            negative_buckets: vec![],
            custom_values: vec![],
        }
    }

    fn nhcb_hist(count: f64, sum: f64, bounds: Vec<f64>, buckets: Vec<f64>) -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![Span {
                offset: 0,
                length: buckets.len() as u32,
            }],
            negative_spans: vec![],
            positive_buckets: buckets,
            negative_buckets: vec![],
            custom_values: bounds,
        }
    }

    /// The fold + final flush every caller performs (`sum = h0;
    /// sum, c = sum.KahanAdd(h1, c); …; sum.Add(c)`).
    fn fold_and_flush(hists: &[FloatHistogram]) -> FloatHistogram {
        let mut sum = hists[0].clone();
        let mut comp: Option<FloatHistogram> = None;
        for h in &hists[1..] {
            let outcome = sum.kahan_add(h, comp.as_ref()).unwrap();
            sum = outcome.result;
            comp = Some(outcome.compensation);
        }
        match comp {
            Some(c) => sum.add(&c).unwrap().result,
            None => sum,
        }
    }

    #[test]
    fn kahan_add_sums_count_sum_and_buckets_like_plain_add_on_exact_values() {
        let a = exp_hist(4.0, 5.0, vec![1.0, 2.0, 1.0]);
        let outcome = a.kahan_add(&a, None).unwrap();
        assert_eq!(outcome.result.count, 8.0);
        assert_eq!(outcome.result.sum, 10.0);
        assert_eq!(outcome.result.positive_buckets, vec![2.0, 4.0, 2.0]);
        assert!(!outcome.nhcb_bounds_reconciled);
        // Exact-value folds leave zero compensation everywhere.
        assert_eq!(outcome.compensation.count, 0.0);
        assert_eq!(outcome.compensation.sum, 0.0);
        assert_eq!(outcome.compensation.positive_buckets, vec![0.0, 0.0, 0.0]);
        // Structural invariant: compensation parallel to the result.
        assert_eq!(
            outcome.compensation.positive_spans,
            outcome.result.positive_spans
        );
    }

    /// ADVERSARIAL (the codex round-1 [high] finding): bucket-level
    /// compensation must recover `+1.0` contributions a plain `+=` loses
    /// above 2^53 — the flushed bucket is `2^53 + 2`, NOT the plain
    /// result `2^53`. Also proves `count`/`sum` compensation on the same
    /// values.
    #[test]
    fn bucket_level_compensation_recovers_lost_low_order_adds() {
        let hists = vec![
            exp_hist(BIG, BIG, vec![BIG]),
            exp_hist(1.0, 1.0, vec![1.0]),
            exp_hist(1.0, 1.0, vec![1.0]),
        ];
        let flushed = fold_and_flush(&hists);
        assert_eq!(flushed.positive_buckets, vec![BIG_PLUS_2]);
        assert_ne!(
            flushed.positive_buckets,
            vec![BIG],
            "plain accumulation would plateau at 2^53 — the compensation must do work"
        );
        assert_eq!(flushed.count, BIG_PLUS_2);
        assert_eq!(flushed.sum, BIG_PLUS_2);
    }

    /// The same fold through the PLAIN `add` (no compensation) provably
    /// plateaus — the differential contrast proving `kahan_add`'s
    /// compensation is doing the work.
    #[test]
    fn plain_add_fold_loses_the_low_order_adds_kahan_recovers() {
        let one = exp_hist(1.0, 1.0, vec![1.0]);
        let mut plain = exp_hist(BIG, BIG, vec![BIG]);
        plain = plain.add(&one).unwrap().result;
        plain = plain.add(&one).unwrap().result;
        assert_eq!(plain.positive_buckets, vec![BIG]);
        assert_eq!(plain.count, BIG);
    }

    /// ADVERSARIAL: the ZERO COUNT is compensated too (`h.ZeroCount,
    /// c.ZeroCount = kahansum.Inc(…)`, `:429`).
    #[test]
    fn zero_count_compensation_recovers_lost_low_order_adds() {
        let mk = |zc: f64| FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            zero_threshold: 1.0,
            zero_count: zc,
            ..exp_hist(zc, 0.0, vec![])
        };
        let flushed = fold_and_flush(&[mk(BIG), mk(1.0), mk(1.0)]);
        assert_eq!(flushed.zero_count, BIG_PLUS_2);
        assert_ne!(flushed.zero_count, BIG);
    }

    /// ADVERSARIAL: NHCB (matching custom bounds) bucket compensation.
    #[test]
    fn nhcb_matching_bounds_bucket_compensation_recovers_lost_adds() {
        let flushed = fold_and_flush(&[
            nhcb_hist(BIG, BIG, vec![5.0], vec![BIG]),
            nhcb_hist(1.0, 1.0, vec![5.0], vec![1.0]),
            nhcb_hist(1.0, 1.0, vec![5.0], vec![1.0]),
        ]);
        assert_eq!(flushed.positive_buckets, vec![BIG_PLUS_2]);
        assert_eq!(flushed.custom_values, vec![5.0]);
    }

    /// ADVERSARIAL: NHCB MISMATCHED bounds — the accumulated compensation
    /// survives the intersection remap (`addCustomBucketsWithMismatches`'
    /// `withCompensation` arm folds A's comp bucket right after A's
    /// value). Fold [b=2^53 cv=[5]], [b=1 cv=[5]] (comp now 1), then a
    /// mismatched [b=1 cv=[5,10]] → intersect [5]; flush = 2^53 + 2.
    #[test]
    fn nhcb_mismatched_bounds_remap_preserves_the_compensation() {
        let mut sum = nhcb_hist(BIG, BIG, vec![5.0], vec![BIG]);
        let mut comp: Option<FloatHistogram> = None;
        let o1 = sum
            .kahan_add(&nhcb_hist(1.0, 1.0, vec![5.0], vec![1.0]), comp.as_ref())
            .unwrap();
        sum = o1.result;
        comp = Some(o1.compensation);
        assert!(!o1.nhcb_bounds_reconciled);
        assert_eq!(
            comp.as_ref().unwrap().positive_buckets,
            vec![1.0],
            "the lost +1.0 sits in the compensation bucket"
        );
        let o2 = sum
            .kahan_add(
                &nhcb_hist(1.0, 1.0, vec![5.0, 10.0], vec![1.0]),
                comp.as_ref(),
            )
            .unwrap();
        assert!(o2.nhcb_bounds_reconciled);
        assert_eq!(o2.result.custom_values, vec![5.0]);
        assert_eq!(o2.compensation.custom_values, vec![5.0]);
        let flushed = o2.result.add(&o2.compensation).unwrap().result;
        assert_eq!(flushed.positive_buckets, vec![BIG_PLUS_2]);
    }

    /// `kahanReduceResolution` (self side): reducing schema-1 buckets
    /// [2^53, 1] (indices 1,2 → one schema-0 target bucket) generates the
    /// compensation `1.0` the plain reduction would lose; merging the
    /// other side's `1.0` at the same index accumulates it to 2.
    #[test]
    fn self_side_kahan_schema_reduction_generates_compensation() {
        let hi_res = FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 1,
            positive_spans: vec![Span {
                offset: 1,
                length: 2,
            }],
            positive_buckets: vec![BIG, 1.0],
            ..exp_hist(BIG + 2.0, 0.0, vec![])
        };
        let lo_res = FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 0,
            positive_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            positive_buckets: vec![1.0],
            ..exp_hist(1.0, 0.0, vec![])
        };
        let outcome = hi_res.kahan_add(&lo_res, None).unwrap();
        assert_eq!(outcome.result.schema, 0);
        assert_eq!(outcome.result.positive_buckets, vec![BIG]);
        assert_eq!(
            outcome.compensation.positive_buckets,
            vec![2.0],
            "reduction rounding (1.0) + merge rounding (1.0) both land in the compensation"
        );
        let flushed = outcome.result.add(&outcome.compensation).unwrap().result;
        assert_eq!(flushed.positive_buckets, vec![BIG_PLUS_2]);
    }

    /// `kahanReduceResolution` (other side): reducing the OTHER operand
    /// creates a fresh compensation (`otherCPositiveBuckets`) which the
    /// merge's `compB != 0` arm folds in (`:1608`).
    #[test]
    fn other_side_reduction_compensation_flows_through_the_merge() {
        let lo_res = FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 0,
            positive_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            positive_buckets: vec![1.0],
            ..exp_hist(1.0, 0.0, vec![])
        };
        let hi_res = FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 1,
            positive_spans: vec![Span {
                offset: 1,
                length: 2,
            }],
            positive_buckets: vec![BIG, 1.0],
            ..exp_hist(BIG + 2.0, 0.0, vec![])
        };
        let outcome = lo_res.kahan_add(&hi_res, None).unwrap();
        assert_eq!(outcome.result.positive_buckets, vec![BIG]);
        assert_eq!(outcome.compensation.positive_buckets, vec![2.0]);
        let flushed = outcome.result.add(&outcome.compensation).unwrap().result;
        assert_eq!(flushed.positive_buckets, vec![BIG_PLUS_2]);
    }

    /// A growing zero threshold folds the trimmed bucket's VALUE AND its
    /// accumulated COMPENSATION into the compensated zero count
    /// (`zeroCountForLargerThreshold`'s `c != nil` arm).
    #[test]
    fn zero_threshold_growth_folds_bucket_compensation_into_the_zero_count() {
        // Running sum: bucket (0.5,1] = 2^53 carrying compensation 1.0
        // (constructed directly for a focused unit test — the state a
        // prior lossy fold would leave).
        let sum = FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.5,
            zero_count: 0.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            positive_buckets: vec![BIG],
            ..exp_hist(BIG, 0.0, vec![])
        };
        let comp = FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            positive_buckets: vec![1.0],
            count: 0.0,
            ..sum.clone()
        };
        // Other: zero bucket [-1,1] (threshold 1.0 > 0.5) with one more
        // observation — forces self's (0.5,1] bucket into the zero bucket.
        let other = FloatHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 1.0,
            zero_count: 1.0,
            ..exp_hist(1.0, 0.0, vec![])
        };
        let outcome = sum.kahan_add(&other, Some(&comp)).unwrap();
        assert_eq!(outcome.result.zero_threshold, 1.0);
        assert!(outcome.result.positive_buckets.is_empty());
        // zero count: Inc(2^53 value) then Inc(1.0 comp) then Inc(1.0
        // other zero count) — the value plateaus at 2^53 while the
        // compensation holds the two lost 1.0s.
        assert_eq!(outcome.result.zero_count, BIG);
        assert_eq!(outcome.compensation.zero_count, 2.0);
        let flushed = outcome.result.add(&outcome.compensation).unwrap().result;
        assert_eq!(flushed.zero_count, BIG_PLUS_2);
    }

    #[test]
    fn kahan_add_propagates_incompatible_schema() {
        let exp = exp_hist(1.0, 1.0, vec![1.0]);
        let nhcb = nhcb_hist(1.0, 1.0, vec![5.0], vec![1.0]);
        assert_eq!(
            exp.kahan_add(&nhcb, None).unwrap_err(),
            FloatHistogramOpError::IncompatibleSchema
        );
    }

    /// Folding three converted `NativeHistogram`s stays exact for
    /// integer-valued fixtures (the corpus's own value class).
    #[test]
    fn kahan_add_folds_three_converted_native_histograms_exactly() {
        let h = NativeHistogram {
            counter_reset_hint: crate::CounterResetHint::Unknown,
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
        .to_float();
        let flushed = fold_and_flush(&[h.clone(), h.clone(), h]);
        assert_eq!(flushed.count, 12.0);
        assert_eq!(flushed.sum, 15.0);
        assert_eq!(flushed.positive_buckets, vec![3.0, 6.0, 3.0]);
    }

    #[test]
    fn has_overflow_detects_an_infinite_count_and_bucket() {
        let mut h = exp_hist(1.0, 1.0, vec![1.0]);
        assert!(!h.has_overflow());
        h.count = f64::INFINITY;
        assert!(h.has_overflow());
        let mut h2 = exp_hist(1.0, 1.0, vec![1.0]);
        h2.positive_buckets[0] = f64::INFINITY;
        assert!(h2.has_overflow());
    }
}
