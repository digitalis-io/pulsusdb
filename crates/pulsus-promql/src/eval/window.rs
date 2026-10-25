//! The window algebra (issue #18 part 3). Stubs for the tests-first
//! commit; the implementation follows.

#![allow(unused_variables, clippy::needless_pass_by_value)]

use crate::value::Sample;

#[derive(Debug, Clone)]
pub struct Segment {
    pub n: u32,
    pub first: (i64, f64),
    pub last: (i64, f64),
    pub prev: Option<(i64, f64)>,
    pub min: f64,
    pub max: f64,
    pub changes: u32,
    pub n_resets: u32,
    pub resets: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct WindowSummary {
    pub n: usize,
    pub first: (i64, f64),
    pub last: (i64, f64),
    pub prev: Option<(i64, f64)>,
    pub corrections: Vec<f64>,
    pub overflow: bool,
    pub min: f64,
    pub max: f64,
    pub changes: u32,
    pub resets: u32,
}

fn stub_segment() -> Segment {
    Segment {
        n: 0,
        first: (0, 0.0),
        last: (0, 0.0),
        prev: None,
        min: 0.0,
        max: 0.0,
        changes: 0,
        n_resets: 0,
        resets: Vec::new(),
    }
}

pub fn segment_of(samples: &[Sample]) -> Option<Segment> {
    Some(stub_segment())
}

pub fn merge(segments: &[Segment]) -> Option<Segment> {
    segments.first().cloned()
}

pub fn cap(seg: Segment, reset_cap: u32) -> Segment {
    seg
}

pub fn window(segments: &[Segment]) -> Option<WindowSummary> {
    None
}

pub fn summarize(samples: &[Sample]) -> Option<WindowSummary> {
    None
}

pub fn summarize_counter(samples: &[Sample]) -> Option<WindowSummary> {
    None
}

pub fn summarize_last_two(samples: &[Sample]) -> Option<WindowSummary> {
    None
}

pub fn summarize_extremum(samples: &[Sample]) -> Option<WindowSummary> {
    None
}

pub fn summarize_steps(samples: &[Sample]) -> Option<WindowSummary> {
    None
}

pub fn finish_extrapolated(
    w: &WindowSummary,
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
    is_counter: bool,
    is_rate: bool,
) -> Option<f64> {
    None
}

pub fn finish_irate(w: &WindowSummary) -> Option<f64> {
    None
}

pub fn finish_idelta(w: &WindowSummary) -> Option<f64> {
    None
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
