//! The window algebra (issue #18 part 3): a run of samples summarized as a
//! [`Segment`], adjacent segments merged in time order, and a window's
//! [`WindowSummary`] finished by the same arithmetic the raw functions
//! use.
//!
//! **One arithmetic per function.** Each raw window function in
//! [`super::functions`] is a narrow summarizer here plus a finisher here.
//! A narrow summarizer fills only the fields its finisher reads, so the
//! raw route does no work it did not do before. [`window`] fills the same
//! fields from segments, and the finishers do not know which produced
//! them.
//!
//! **Segments compose exactly.** For a sample sequence cut into pieces,
//! [`merge`] of the pieces' segments equals the segment of the whole, in
//! every field, as bits: the boundary pair between two pieces is counted
//! as the pair inside one piece would be, and each piece's pre-drop
//! values follow its boundary drop, so the corrections reach the finisher
//! in time order. `eval_extrapolated` adds them one at a time, and the
//! order is part of its answer: `1 − 1e16` rounds to `−1e16`, so adding
//! `1e16` then `1` gives `1` while adding `1` first loses it.
//!
//! **Tiers cap the stored drop values.** A stored segment keeps its
//! pre-drop values only up to a tier's cap ([`cap`]); past it the list is
//! emptied and the count kept. Such a segment is **overflowed**
//! (`resets.len() < n_resets`), and a counter finisher refuses a window
//! holding one. Every other field is exact under any cap.

use crate::value::Sample;

/// One run of consecutive non-stale float samples, in canonical order
/// (time, then value bits), or one stored bucket summary.
#[derive(Debug, Clone)]
pub struct Segment {
    pub n: u32,
    pub first: (i64, f64),
    pub last: (i64, f64),
    /// The second-last sample; `None` when `n < 2`.
    pub prev: Option<(i64, f64)>,
    /// `fold(m = first; x: if x < m or isNaN(m) then x else m)` over the
    /// run, the fold `min_over_time` uses.
    pub min: f64,
    /// The same fold with `>`.
    pub max: f64,
    /// Consecutive pairs with `y != p` and not both NaN.
    pub changes: u32,
    /// Consecutive pairs with `y < p`.
    pub n_resets: u32,
    /// Each such `p`, in time order; empty when overflowed.
    pub resets: Vec<f64>,
}

impl Segment {
    /// Some pre-drop values were dropped by a tier's cap. An overflowed
    /// segment's `resets` is always empty.
    pub fn overflowed(&self) -> bool {
        self.resets.len() < self.n_resets as usize
    }

    fn of_one(t_ms: i64, v: f64) -> Segment {
        Segment {
            n: 1,
            first: (t_ms, v),
            last: (t_ms, v),
            prev: None,
            min: v,
            max: v,
            changes: 0,
            n_resets: 0,
            resets: Vec::new(),
        }
    }

    /// Appends one sample after `last`.
    fn push(&mut self, t_ms: i64, v: f64) {
        let p = self.last.1;
        if changed(p, v) {
            self.changes += 1;
        }
        if v < p {
            self.n_resets += 1;
            self.resets.push(p);
        }
        self.min = fold_min(self.min, v);
        self.max = fold_max(self.max, v);
        self.prev = Some(self.last);
        self.last = (t_ms, v);
        self.n += 1;
    }
}

/// What a window's finishers read.
#[derive(Debug, Clone)]
pub struct WindowSummary {
    pub n: usize,
    pub first: (i64, f64),
    pub last: (i64, f64),
    pub prev: Option<(i64, f64)>,
    /// The addends of `eval_extrapolated`'s correction loop, in order:
    /// `0.0` when the first value is negative, then each pre-drop value.
    pub corrections: Vec<f64>,
    /// Some segment's pre-drop values were dropped by a tier's cap.
    pub overflow: bool,
    pub min: f64,
    pub max: f64,
    pub changes: u32,
    pub resets: u32,
}

impl WindowSummary {
    /// A summary with only `n`, `first` and `last` filled; the narrow
    /// summarizers fill the rest they need.
    fn empty(n: usize, first: (i64, f64), last: (i64, f64)) -> WindowSummary {
        WindowSummary {
            n,
            first,
            last,
            prev: None,
            corrections: Vec::new(),
            overflow: false,
            min: 0.0,
            max: 0.0,
            changes: 0,
            resets: 0,
        }
    }
}

/// `changes`'s pair rule (`eval_changes`): unequal, and not both NaN.
fn changed(p: f64, y: f64) -> bool {
    y != p && !(y.is_nan() && p.is_nan())
}

/// `min_over_time`'s fold step (`eval_extremum_over_time`).
fn fold_min(m: f64, x: f64) -> f64 {
    if x < m || m.is_nan() { x } else { m }
}

/// `max_over_time`'s fold step.
fn fold_max(m: f64, x: f64) -> f64 {
    if x > m || m.is_nan() { x } else { m }
}

/// The segment of `samples`' non-stale samples, uncapped; `None` when
/// every sample is a stale marker or there is none. Stale markers are
/// skipped, as a range window skips them.
pub fn segment_of(samples: &[Sample]) -> Option<Segment> {
    let mut live = samples.iter().filter(|s| !s.is_stale());
    let head = live.next()?;
    let mut seg = Segment::of_one(head.t_ms, head.v);
    for s in live {
        seg.push(s.t_ms, s.v);
    }
    Some(seg)
}

/// Adjacent segments, in time order, as one; uncapped. `None` when they
/// hold no sample.
///
/// `n_resets` and `changes` add the boundary pairs `(last_{i−1},
/// first_i)`. `resets` is, for each segment in order, its boundary drop
/// and then its own `resets`; it is empty when any input is overflowed.
/// Min and max continue the fold pairwise, which is exact for it,
/// including signed zeros and NaN payloads.
pub fn merge(segments: &[Segment]) -> Option<Segment> {
    let mut live = segments.iter().filter(|g| g.n > 0);
    let mut m = live.next()?.clone();
    let mut overflow = m.overflowed();
    for g in live {
        let p = m.last.1;
        let y = g.first.1;
        if changed(p, y) {
            m.changes += 1;
        }
        m.changes += g.changes;
        if y < p {
            m.n_resets += 1;
            m.resets.push(p);
        }
        m.n_resets += g.n_resets;
        overflow |= g.overflowed();
        m.resets.extend_from_slice(&g.resets);
        m.min = fold_min(m.min, g.min);
        m.max = fold_max(m.max, g.max);
        m.prev = if g.n >= 2 { g.prev } else { Some(m.last) };
        m.last = g.last;
        m.n += g.n;
    }
    if overflow {
        m.resets.clear();
    }
    Some(m)
}

/// A segment as a tier stores it: its pre-drop values are dropped when
/// there are more than `reset_cap` of them, and every other field is
/// unchanged. A 5m row is `cap(segment_of(bucket), 4)`; a 1h row is
/// `cap(merge(the hour's 5m rows), 16)`.
pub fn cap(mut seg: Segment, reset_cap: u32) -> Segment {
    if seg.n_resets > reset_cap {
        seg.resets = Vec::new();
    }
    seg
}

/// The window over `segments`, adjacent and in time order: their
/// [`merge`], with the leading `0.0` correction `eval_extrapolated`'s
/// loop adds when the first value is negative.
pub fn window(segments: &[Segment]) -> Option<WindowSummary> {
    let m = merge(segments)?;
    let overflow = m.overflowed();
    let mut corrections = Vec::with_capacity(m.resets.len() + 1);
    if m.first.1 < 0.0 {
        corrections.push(0.0);
    }
    corrections.extend_from_slice(&m.resets);
    Some(WindowSummary {
        n: m.n as usize,
        first: m.first,
        last: m.last,
        prev: m.prev,
        corrections,
        overflow,
        min: m.min,
        max: m.max,
        changes: m.changes,
        resets: m.n_resets,
    })
}

/// Every field of the window over `samples`, from one segment.
pub fn summarize(samples: &[Sample]) -> Option<WindowSummary> {
    window(&[segment_of(samples)?])
}

// ---------------------------------------------------------------------
// Narrow summarizers, for the raw route. Each takes a window's samples as
// the raw functions receive them — sorted, stale markers already removed
// — and fills only the fields its finisher reads.
// ---------------------------------------------------------------------

/// `n`, `first` and `last`: what `delta` reads.
pub(crate) fn summarize_ends(samples: &[Sample]) -> Option<WindowSummary> {
    let first = samples.first()?;
    let last = samples.last()?;
    Some(WindowSummary::empty(
        samples.len(),
        (first.t_ms, first.v),
        (last.t_ms, last.v),
    ))
}

/// `n`, `first`, `last` and `corrections`: what `rate` and `increase`
/// read. The loop is `eval_extrapolated`'s, starting from `0.0`.
pub fn summarize_counter(samples: &[Sample]) -> Option<WindowSummary> {
    let mut w = summarize_ends(samples)?;
    let mut last_value = 0.0_f64;
    for s in samples {
        if s.v < last_value {
            w.corrections.push(last_value);
        }
        last_value = s.v;
    }
    Some(w)
}

/// `n`, `last` and `prev`: what `irate` and `idelta` read.
pub fn summarize_last_two(samples: &[Sample]) -> Option<WindowSummary> {
    let mut w = summarize_ends(samples)?;
    if samples.len() >= 2 {
        let prev = &samples[samples.len() - 2];
        w.prev = Some((prev.t_ms, prev.v));
    }
    Some(w)
}

/// `n`, `min` and `max`: what `min_over_time` and `max_over_time` read.
pub fn summarize_extremum(samples: &[Sample]) -> Option<WindowSummary> {
    let mut w = summarize_ends(samples)?;
    let mut min = samples[0].v;
    let mut max = samples[0].v;
    for s in samples {
        min = fold_min(min, s.v);
        max = fold_max(max, s.v);
    }
    w.min = min;
    w.max = max;
    Some(w)
}

/// `n`, `changes` and `resets`: what `changes` and `resets` read.
pub fn summarize_steps(samples: &[Sample]) -> Option<WindowSummary> {
    let mut w = summarize_ends(samples)?;
    for pair in samples.windows(2) {
        let (p, y) = (pair[0].v, pair[1].v);
        if changed(p, y) {
            w.changes += 1;
        }
        if y < p {
            w.resets += 1;
        }
    }
    Some(w)
}

// ---------------------------------------------------------------------
// Finishers
// ---------------------------------------------------------------------

/// `rate`/`increase`/`delta` from a window summary: exactly
/// `eval_extrapolated`'s operations, in its order — `last − first`, then
/// each correction added in order, then the duration and factor
/// arithmetic. A counter finisher returns `None` on an overflowed
/// summary, whose corrections are incomplete. The line numbers below are
/// the ones `eval_extrapolated` cites.
pub fn finish_extrapolated(
    w: &WindowSummary,
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
    is_counter: bool,
    is_rate: bool,
) -> Option<f64> {
    if w.n < 2 {
        return None;
    }
    if is_counter && w.overflow {
        return None;
    }
    let (first_t, first_v) = w.first;
    let (last_t, last_v) = w.last;
    // Line 510: `resultFloat = samples.Floats[numSamplesMinusOne].F -
    // samples.Floats[0].F` (last - first).
    let mut result_value = last_v - first_v;
    // Lines 519-524: the counter-reset corrections, one addition each,
    // in time order — never pre-summed, which rounds differently.
    if is_counter {
        for c in &w.corrections {
            result_value += *c;
        }
    }

    // Lines 531-535.
    let mut duration_to_start = (first_t - range_start_ms) as f64 / 1000.0;
    let mut duration_to_end = (range_end_ms - last_t) as f64 / 1000.0;
    let sampled_interval = (last_t - first_t) as f64 / 1000.0;
    let average_duration_between_samples = sampled_interval / (w.n - 1) as f64;
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
        if result_value > 0.0 && first_v >= 0.0 {
            duration_to_zero = sampled_interval * (first_v / result_value);
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

/// `irate` from the last two samples: a drop between them is a reset,
/// and the result is then the last value.
pub fn finish_irate(w: &WindowSummary) -> Option<f64> {
    if w.n < 2 {
        return None;
    }
    let last = w.last;
    let prev = w.prev?;
    let interval_ms = last.0 - prev.0;
    if interval_ms == 0 {
        return None;
    }
    let mut result = if last.1 < prev.1 {
        last.1
    } else {
        last.1 - prev.1
    };
    result /= interval_ms as f64 / 1000.0;
    Some(result)
}

/// `idelta` from the last two samples: their plain difference, and no
/// sample when they share a millisecond.
pub fn finish_idelta(w: &WindowSummary) -> Option<f64> {
    if w.n < 2 {
        return None;
    }
    let last = w.last;
    let prev = w.prev?;
    if last.0 - prev.0 == 0 {
        return None;
    }
    Some(last.1 - prev.1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_model::STALE_NAN_BITS;

    #[allow(dead_code)]
    mod legacy {
        use crate::value::Sample;
        include!("functions_legacy.rs");
    }

    const STALE: f64 = f64::from_bits(STALE_NAN_BITS);

    /// Samples one second apart from `t = 0`.
    fn seq(vals: &[f64]) -> Vec<Sample> {
        vals.iter()
            .enumerate()
            .map(|(i, &v)| Sample::float(i as i64 * 1000, v))
            .collect()
    }

    type Pair = (i64, u64);

    fn pair(p: (i64, f64)) -> Pair {
        (p.0, p.1.to_bits())
    }

    /// Every field of a segment, floats as bits.
    #[allow(clippy::type_complexity)]
    fn seg_key(s: &Segment) -> (u32, Pair, Pair, Option<Pair>, u64, u64, u32, u32, Vec<u64>) {
        (
            s.n,
            pair(s.first),
            pair(s.last),
            s.prev.map(pair),
            s.min.to_bits(),
            s.max.to_bits(),
            s.changes,
            s.n_resets,
            s.resets.iter().map(|v| v.to_bits()).collect(),
        )
    }

    /// Every field of a window summary, floats as bits.
    #[allow(clippy::type_complexity)]
    fn summary_key(
        w: &WindowSummary,
    ) -> (
        usize,
        Pair,
        Pair,
        Option<Pair>,
        Vec<u64>,
        bool,
        u64,
        u64,
        u32,
        u32,
    ) {
        (
            w.n,
            pair(w.first),
            pair(w.last),
            w.prev.map(pair),
            w.corrections.iter().map(|v| v.to_bits()).collect(),
            w.overflow,
            w.min.to_bits(),
            w.max.to_bits(),
            w.changes,
            w.resets,
        )
    }

    fn bits(v: Option<f64>) -> Option<u64> {
        v.map(f64::to_bits)
    }

    /// 3-a and 3-b's domain: every sequence of length 1 to 6 over seven
    /// values, cut into 1 to 3 non-empty pieces every way. Calls `f(all,
    /// pieces)` and returns the number of cuts.
    fn for_each_cut(mut f: impl FnMut(&[Sample], &[&[Sample]])) -> usize {
        const VALUES: [f64; 7] = [-1.0, -0.0, 0.0, 1.0, 2.0, f64::NAN, STALE];
        let mut cuts = 0;
        for len in 1..=6usize {
            let total = VALUES.len().pow(len as u32);
            for code in 0..total {
                let mut c = code;
                let vals: Vec<f64> = (0..len)
                    .map(|_| {
                        let v = VALUES[c % VALUES.len()];
                        c /= VALUES.len();
                        v
                    })
                    .collect();
                let all = seq(&vals);
                // No cut, one cut at `a`, two cuts at `a < b`.
                f(&all, &[&all]);
                cuts += 1;
                for a in 1..len {
                    f(&all, &[&all[..a], &all[a..]]);
                    cuts += 1;
                    for b in a + 1..len {
                        f(&all, &[&all[..a], &all[a..b], &all[b..]]);
                        cuts += 1;
                    }
                }
            }
        }
        cuts
    }

    fn segments(pieces: &[&[Sample]]) -> Vec<Segment> {
        pieces.iter().filter_map(|p| segment_of(p)).collect()
    }

    /// 3-a: the pieces' segments merge to the segment of the whole, in
    /// every field as bits, stale markers excluded; so the window of the
    /// pieces is the summary of the whole.
    #[test]
    fn pieces_merge_to_the_segment_of_the_whole() {
        let mut checked = 0usize;
        let cuts = for_each_cut(|all, pieces| {
            let live = all.iter().filter(|s| !s.is_stale()).count();
            let whole = segment_of(all);
            assert_eq!(
                whole.as_ref().map(|s| s.n as usize),
                (live > 0).then_some(live),
                "segment_of counts the non-stale samples of {all:?}"
            );
            let want = whole.and_then(|s| merge(&[s]));
            let got = merge(&segments(pieces));
            assert_eq!(
                got.as_ref().map(seg_key),
                want.as_ref().map(seg_key),
                "merge of {pieces:?}"
            );
            let got = window(&segments(pieces));
            let want = summarize(all);
            assert_eq!(
                got.as_ref().map(summary_key),
                want.as_ref().map(summary_key),
                "window of {pieces:?}"
            );
            checked += 1;
        });
        // 7^L sequences of each length L, each cut 1 + (L-1) + C(L-1, 2)
        // ways: 7 + 98 + 1,372 + 16,807 + 184,877 + 1,882,384.
        assert_eq!(cuts, 2_085_545);
        assert_eq!(checked, cuts);
    }

    /// The window `(-1 s, last sample]` over samples one second apart.
    fn finishes(w: Option<&WindowSummary>, len: usize) -> Vec<Option<u64>> {
        let start = -1000;
        let end = (len as i64 - 1) * 1000;
        let range = end - start;
        let Some(w) = w else {
            return vec![None; 5];
        };
        vec![
            bits(finish_extrapolated(w, range, start, end, true, true)),
            bits(finish_extrapolated(w, range, start, end, true, false)),
            bits(finish_extrapolated(w, range, start, end, false, false)),
            bits(finish_irate(w)),
            bits(finish_idelta(w)),
        ]
    }

    /// The order the corrections are added in decides this answer:
    /// `1 − 1e16` rounds to `−1e16`, so adding `1e16` then `1` gives `1`,
    /// and adding `1` first loses it.
    fn cancellation_fixture() -> Vec<Sample> {
        seq(&[1e16, 0.0, 1.0, 0.0, 1.0])
    }

    /// 3-b: every finisher over the window of the pieces equals the same
    /// finisher over the summary of the whole, as bits; and the
    /// cancellation fixture, cut after its first sample, gives `increase`
    /// over `(-1 s, 4 s]` of exactly 1.25.
    #[test]
    fn a_window_from_pieces_finishes_as_the_whole() {
        let cuts = for_each_cut(|all, pieces| {
            let got = window(&segments(pieces));
            let want = summarize(all);
            assert_eq!(
                finishes(got.as_ref(), all.len()),
                finishes(want.as_ref(), all.len()),
                "finishers over {pieces:?}"
            );
        });
        assert_eq!(cuts, 2_085_545);

        let all = cancellation_fixture();
        let whole = summarize(&all);
        let cut = window(&segments(&[&all[..1], &all[1..]]));
        let increase = |w: Option<&WindowSummary>| {
            bits(w.and_then(|w| finish_extrapolated(w, 5000, -1000, 4000, true, false)))
        };
        assert_eq!(increase(whole.as_ref()), Some(0x3ff4_0000_0000_0000));
        assert_eq!(increase(cut.as_ref()), Some(0x3ff4_0000_0000_0000));
        assert_eq!(
            finishes(cut.as_ref(), all.len()),
            finishes(whole.as_ref(), all.len())
        );
    }

    /// 240 samples 15 s apart, rising by 1 from 100, and `0` at each
    /// index in `drops`.
    fn hour_with_drops(drops: &[usize]) -> Vec<Sample> {
        let mut v = 100.0;
        (0..240)
            .map(|i| {
                v = if drops.contains(&i) { 0.0 } else { v + 1.0 };
                Sample::float(i as i64 * 15_000, v)
            })
            .collect()
    }

    /// The hour's twelve 5m rows (cap 4) and its 1h row (cap 16).
    fn tiers(samples: &[Sample]) -> (Vec<Segment>, Segment) {
        let b: Vec<Segment> = samples
            .chunks(20)
            .map(|bucket| cap(segment_of(bucket).expect("a bucket holds samples"), 4))
            .collect();
        let h = cap(merge(&b).expect("an hour holds samples"), 16);
        (b, h)
    }

    /// 3-h: the tier caps. Sixteen drops in an hour are stored; seventeen
    /// are not; a 5m row past its cap overflows its hour however few the
    /// hour's drops are, and the hour is otherwise exact.
    #[test]
    fn the_tier_caps_keep_or_drop_the_reset_values() {
        let inner: Vec<usize> = (0..12).map(|i| i * 20 + 10).collect();
        let mut h16_drops = inner.clone();
        h16_drops.extend([20, 40, 60, 80]);
        let mut h17_drops = h16_drops.clone();
        h17_drops.push(100);

        let (_, h) = tiers(&hour_with_drops(&h16_drops));
        assert_eq!(h.n_resets, 16);
        assert_eq!(h.resets.len(), 16, "the 16-drop hour stores its values");

        let (_, h) = tiers(&hour_with_drops(&h17_drops));
        assert_eq!(h.n_resets, 17);
        assert!(h.resets.is_empty(), "the 17-drop hour stores none");

        let h6 = hour_with_drops(&[2, 4, 6, 8, 10, 50]);
        let (b, h) = tiers(&h6);
        assert_eq!(b[0].n_resets, 5);
        assert!(b[0].resets.is_empty(), "the 5-drop 5m row is overflowed");
        assert_eq!(h.n_resets, 6);
        assert!(h.resets.is_empty(), "its hour inherits the overflow");
        let uncapped = merge(&[segment_of(&h6).expect("samples")]).expect("samples");
        let mut capped_key = seg_key(&h);
        let mut uncapped_key = seg_key(&uncapped);
        capped_key.8.clear();
        uncapped_key.8.clear();
        assert_eq!(
            capped_key, uncapped_key,
            "every other field of the hour is exact"
        );

        let w = window(&b).expect("samples");
        assert!(w.overflow);
        let (range, start, end) = (3_600_000, -1, 3_600_000);
        assert_eq!(finish_extrapolated(&w, range, start, end, true, true), None);
        assert_eq!(
            finish_extrapolated(&w, range, start, end, true, false),
            None
        );
        assert_eq!(
            bits(finish_extrapolated(&w, range, start, end, false, false)),
            bits(legacy::eval_extrapolated(
                &h6, range, start, end, false, false, None
            )),
            "delta reads no corrections"
        );
        assert!(
            finish_extrapolated(&w, range, start, end, false, false).is_some(),
            "the delta compared is a value"
        );
    }

    /// 3-i: NaN payloads. The fold replaces a NaN accumulator, so the
    /// last NaN wins, across segments as within one.
    #[test]
    fn merged_min_and_max_keep_the_last_nan_payload() {
        let samples: Vec<Sample> = (1..=4u64)
            .map(|k| {
                Sample::float(
                    (k as i64 - 1) * 1000,
                    f64::from_bits(0x7ff8_0000_0000_0000 | k),
                )
            })
            .collect();
        let segs = segments(&[&samples[..2], &samples[2..3], &samples[3..]]);
        let m = merge(&segs).expect("samples");
        assert_eq!(m.min.to_bits(), 0x7ff8_0000_0000_0004);
        assert_eq!(m.max.to_bits(), 0x7ff8_0000_0000_0004);
        assert_eq!(
            m.min.to_bits(),
            legacy::eval_extremum_over_time(&samples, legacy::WhichExtremum::Min).to_bits()
        );
        assert_eq!(
            m.max.to_bits(),
            legacy::eval_extremum_over_time(&samples, legacy::WhichExtremum::Max).to_bits()
        );
    }
}
