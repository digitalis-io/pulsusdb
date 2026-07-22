//! Oracle-cited unit tests for the extended range-selector ports
//! (issue #150, AC6). Each asserts a specific pinned algorithm behaviour
//! (Prometheus v3.13.0 @ 40af9c2).

use super::*;
use pulsus_model::{CounterResetHint, FloatHistogram};

fn f(t_ms: i64, v: f64) -> Sample {
    Sample::float(t_ms, v)
}

/// A minimal single-bucket exponential histogram with `count`/`sum` set and
/// the given counter-reset hint.
fn h_sample(t_ms: i64, count: f64, hint: CounterResetHint) -> Sample {
    Sample::hist(
        t_ms,
        FloatHistogram {
            counter_reset_hint: hint,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum: count,
            positive_spans: vec![pulsus_model::Span {
                offset: 1,
                length: 1,
            }],
            negative_spans: Vec::new(),
            positive_buckets: vec![count],
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        },
    )
}

#[test]
fn interpolate_models_counter_reset_from_zero() {
    // is_counter with y2 < y1: y1 is forced to 0 (functions.go:96-98).
    let p1 = f(0, 100.0);
    let p2 = f(10, 10.0);
    // Non-counter: straight linear interpolation at t=5 → 55.
    assert_eq!(interpolate(&p1, &p2, 5, false), 55.0);
    // Counter reset: y1=0, so 0 + 10*(5/10) = 5.
    assert_eq!(interpolate(&p1, &p2, 5, true), 5.0);
}

#[test]
fn correct_for_counter_resets_uses_boundary_anchors() {
    // left=1, points ascending except a drop, right compared against last.
    // points: 2,1,3; prev starts at left=1: 2>=1, 1<2 → +2, 3>=1; right(3) vs
    // prev(3): no. correction = 2.
    let points = [f(0, 2.0), f(10, 1.0), f(20, 3.0)];
    assert_eq!(correct_for_counter_resets(1.0, 3.0, &points), 2.0);
}

#[test]
fn extended_rate_single_sample_anchored_increase_is_zero() {
    // A single-sample anchored window yields increase = 0 (corpus line 32,
    // `increase(metric[1m] anchored)` at 5s).
    let samples = [f(0, 1.0)];
    let range_ms = 60_000;
    let range_start = 5_000 - 60_000; // eff_t - range
    let range_end = 5_000;
    let out = extended_rate(
        &samples,
        range_ms,
        range_start,
        range_end,
        false,
        true,
        false,
    );
    assert_eq!(out, Some(0.0));
}

#[test]
fn extended_rate_single_sample_at_range_end_anchored_rate_is_zero() {
    // Regression for the `last_sample_index -= 1` usize underflow: a lone
    // anchored counter sample sitting EXACTLY at `range_end_ms`. The pre-fix
    // inclusive-slice port panicked "attempt to subtract with overflow" here;
    // upstream (functions.go:333-345) decrements `lastSampleIndex` to -1, slices
    // `f[0:0]` (empty), and emits a rate point of 0.0. Match the oracle exactly.
    let samples = [f(5_000, 42.0)];
    let range_ms = 60_000;
    let range_end = 5_000;
    let range_start = range_end - range_ms; // -55_000
    let out = extended_rate(
        &samples,
        range_ms,
        range_start,
        range_end,
        false, // anchored
        true,  // is_counter (rate)
        true,  // is_rate
    );
    assert_eq!(out, Some(0.0));
}

#[test]
fn extended_rate_single_sample_at_range_end_smoothed_rate_is_zero() {
    // The SMOOTHED twin of the underflow regression above: with one sample
    // exactly at `range_end_ms`, the smoothed `lastSampleIndex` recompute
    // (`sort.Search(..., T >= rangeEnd)`) lands on index 0 and the same
    // boundary-exclusion decrement fires. Oracle-verified at the pin
    // (promqltest harness, `load 5m / metric _ 5`,
    // `rate(metric[1m] smoothed)` at 5m → `{} 0`).
    let samples = [f(5_000, 42.0)];
    let range_ms = 60_000;
    let range_end = 5_000;
    let range_start = range_end - range_ms; // -55_000
    let out = extended_rate(
        &samples,
        range_ms,
        range_start,
        range_end,
        true, // smoothed
        true, // is_counter (rate)
        true, // is_rate
    );
    assert_eq!(out, Some(0.0));
}

#[test]
fn extended_rate_anchor_at_start_sample_at_end_increase_is_delta() {
    // Review test gap: anchor exactly at `range_start` plus one sample exactly
    // at `range_end`. Both boundaries excluded from the interior correction →
    // an empty interior slice; increase is just right-left with no reset.
    let range_ms = 60_000;
    let range_end = 5_000;
    let range_start = range_end - range_ms; // -55_000
    let samples = [f(range_start, 1.0), f(range_end, 3.0)];
    let out = extended_rate(
        &samples,
        range_ms,
        range_start,
        range_end,
        false, // anchored
        true,  // is_counter (increase)
        false, // is_rate
    );
    assert_eq!(out, Some(2.0));
}

#[test]
fn extended_rate_smoothed_interpolates_right_boundary() {
    // corpus line 59: `increase(metric[1m] smoothed)` at 5s → 0.333….
    // metric 1+1x… (t=0..). Window widened; boundaries at rangeStart=-55s,
    // rangeEnd=5s.
    let samples = [f(0, 1.0), f(15_000, 2.0), f(30_000, 3.0)];
    let out = extended_rate(&samples, 60_000, -55_000, 5_000, true, true, false).unwrap();
    assert!((out - 0.333_333_333).abs() < 1e-6, "got {out}");
}

#[test]
fn anchor_trim_no_anchor_keeps_all() {
    // Every sample after range start ⇒ idx==0 ⇒ whole slice.
    let samples = [f(10, 1.0), f(20, 2.0)];
    let got = anchor_trim(&samples, 5).unwrap();
    assert_eq!(got.len(), 2);
}

#[test]
fn anchor_trim_prepends_the_anchor() {
    // Anchor = last sample at or before range start; earlier dropped.
    let samples = [f(0, 1.0), f(10, 2.0), f(30, 3.0), f(40, 4.0)];
    // range_start = 30: anchor is the t=30 sample, drop t=0/t=10.
    let got = anchor_trim(&samples, 30).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].t_ms, 30);
    assert_eq!(got[1].t_ms, 40);
}

#[test]
fn anchor_trim_nothing_after_start_is_none() {
    // No sample strictly after range start ⇒ None (found=false).
    let samples = [f(0, 1.0), f(10, 2.0)];
    assert!(anchor_trim(&samples, 20).is_none());
}

/// The float value of a `smoothed_instant` result, asserting `h: None`.
fn float_of(out: Option<(f64, Option<Box<FloatHistogram>>)>) -> f64 {
    let (v, h) = out.expect("expected a value");
    assert!(h.is_none(), "expected a float result");
    v
}

#[test]
fn smoothed_instant_exact_interpolate_carry_forward() {
    let mut annos = Annotations::default();
    let samples = [f(0, 1.0), f(10, 2.0), f(20, 3.0)];
    // Exact match at t=10 → 2.
    assert_eq!(
        float_of(smoothed_instant(&samples, 10, 300_000, "m", 0, &mut annos)),
        2.0
    );
    // Interpolate at t=5 between (0,1) and (10,2) → 1.5.
    assert_eq!(
        float_of(smoothed_instant(&samples, 5, 300_000, "m", 0, &mut annos)),
        1.5
    );
    // Carry forward past the last sample.
    assert_eq!(
        float_of(smoothed_instant(&samples, 25, 300_000, "m", 0, &mut annos)),
        3.0
    );
    // Only-future data ⇒ skip.
    assert!(smoothed_instant(&samples, 25, 5, "m", 0, &mut annos).is_none());
}

#[test]
fn smoothed_instant_mixed_window_warns_and_skips() {
    let mut annos = Annotations::default();
    let samples = [f(0, 1.0), h_sample(10, 3.0, CounterResetHint::Unknown)];
    assert!(smoothed_instant(&samples, 5, 300_000, "mixed", 0, &mut annos).is_none());
    let (warnings, _) = annos.as_strings("", 64, 64);
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("mix of histograms and floats")),
        "expected a mixed-type warning, got {warnings:?}"
    );
}

#[test]
fn extended_histogram_rate_result_is_gauge() {
    // Two monotonic histogram counter samples, anchored increase.
    let samples = [
        h_sample(0, 1.0, CounterResetHint::Unknown),
        h_sample(45_000, 4.0, CounterResetHint::Unknown),
    ];
    let mut annos = Annotations::default();
    let out = extended_histogram_rate(
        &samples, 60_000, 0, 50_000, false, true, false, "h", 0, &mut annos,
    );
    match out {
        Some(RangeValue::Histogram(h)) => {
            assert_eq!(h.counter_reset_hint, CounterResetHint::Gauge);
            assert!((h.count - 3.0).abs() < 1e-9, "count {}", h.count);
        }
        other => panic!("expected a histogram result, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// `extend_floats` (issue #166, engine.go:4841-4877 @ 40af9c2). Zero corpus
// rows pin the bare anchored/smoothed root (extended_vectors.test is
// entirely function-wrapped), so each case below cites its derivation from
// the pinned algorithm with hand-computed expectations.
// ---------------------------------------------------------------------------

/// The `(t_ms, v)` view of an extension, asserting every point is a float.
fn points_of(out: &[Sample]) -> Vec<(i64, f64)> {
    out.iter()
        .map(|s| {
            assert!(s.h.is_none(), "extend_floats output is all-float");
            (s.t_ms, s.v)
        })
        .collect()
}

#[test]
fn extend_floats_anchored_picks_the_pre_mint_anchor_at_the_left_boundary() {
    // (a) Anchored: firstSampleIndex = max(0, Search(T > mint) - 1) lands
    // on the pre-mint anchor (-10, 5); pickOrInterpolateLeft with
    // smoothed=false PICKS f[first].v (functions.go:72-79, no
    // interpolation). The anchor itself is filtered (T <= mint,
    // engine.go:4861-4863) in favour of the {mint, left} boundary point;
    // the in-window samples follow; right picks f[last].v.
    let f_in = [f(-10, 5.0), f(10, 6.0), f(15, 6.5)];
    let out = extend_floats(&f_in, 0, 20, false);
    assert_eq!(
        points_of(&out),
        vec![(0, 5.0), (10, 6.0), (15, 6.5), (20, 6.5)]
    );
}

#[test]
fn extend_floats_smoothed_interpolates_both_boundaries() {
    // (b) Smoothed: left interpolates (-10,0)→(10,20) at t=0 →
    // 0 + 20·(10/20) = 10 exactly (functions.go:89-101, is_counter=false);
    // lastSampleIndex recomputes to Search(T >= maxt) = 2, and right
    // interpolates (10,20)→(30,40) at t=20 → 20 + 20·(10/20) = 30 exactly.
    // Both straddling samples are filtered to their boundary points.
    let f_in = [f(-10, 0.0), f(10, 20.0), f(30, 40.0)];
    let out = extend_floats(&f_in, 0, 20, true);
    assert_eq!(points_of(&out), vec![(0, 10.0), (10, 20.0), (20, 30.0)]);
}

#[test]
fn extend_floats_filters_samples_exactly_at_the_boundaries() {
    // (c) Samples exactly AT mint and AT maxt: both are excluded from the
    // interior (engine.go:4860-4866, `<= mint` / `>= maxt`) and replaced
    // by boundary points of equal value — left picks f[first].v (the
    // t=mint sample), right picks f[last].v (the t=maxt sample).
    let f_in = [f(0, 1.0), f(10, 2.0), f(20, 3.0)];
    let out = extend_floats(&f_in, 0, 20, false);
    assert_eq!(points_of(&out), vec![(0, 1.0), (10, 2.0), (20, 3.0)]);
}

#[test]
fn extend_floats_all_samples_at_or_before_mint_is_empty() {
    // (d) `floats[lastSampleIndex].T <= mint` → `[]` (engine.go:4851-4853):
    // every sample at/before the range start extends to nothing and the
    // caller omits the series (totalSize == 0, engine.go:2864-2868).
    let f_in = [f(-20, 1.0), f(-10, 2.0)];
    assert!(extend_floats(&f_in, 0, 20, false).is_empty());
    // A lone sample exactly at mint hits the same arm.
    let f_in = [f(0, 1.0)];
    assert!(extend_floats(&f_in, 0, 20, false).is_empty());
    assert!(extend_floats(&f_in, 0, 20, true).is_empty());
}

#[test]
fn extend_floats_empty_input_is_empty() {
    // (e) Documented divergence from the pin: upstream indexes
    // `floats[len-1]` on an empty slice (Go `sort.Search(n<=0)` returns 0)
    // — a runtime panic through `ev.recover`, not a defined result. The
    // port guards to an empty extension (`extended_rate`'s empty-window
    // precedent) so the series is omitted.
    assert!(extend_floats(&[], 0, 20, false).is_empty());
    assert!(extend_floats(&[], 0, 20, true).is_empty());
}

#[test]
fn extend_floats_smoothed_all_future_carries_the_first_value_to_both_boundaries() {
    // (f) The pinned smoothed all-future quirk: samples only in
    // (maxt, maxt+lb]. Search(T > mint) over f[..len-1] = 0 → first = 0;
    // Search(T >= maxt) = 0 → last = 0; f[0].T > mint passes the empty
    // check; left falls through to f[0].v (f[0].T ≮ mint); right needs
    // `last > 0` to interpolate (functions.go:81-88) so it also picks
    // f[0].v; the interior is empty (f[0].T >= maxt). Result: the first
    // FUTURE value carried to both boundary points.
    let f_in = [f(30, 7.0), f(40, 9.0)];
    let out = extend_floats(&f_in, 0, 20, true);
    assert_eq!(points_of(&out), vec![(0, 7.0), (20, 7.0)]);
}

#[test]
fn extend_floats_single_in_window_sample_yields_three_points() {
    // (g) One strictly-interior sample: first = last = 0, both boundary
    // picks carry f[0].v, and the sample survives the interior filter —
    // {mint, v}, the sample, {maxt, v}.
    let f_in = [f(10, 5.0)];
    let out = extend_floats(&f_in, 0, 20, false);
    assert_eq!(points_of(&out), vec![(0, 5.0), (10, 5.0), (20, 5.0)]);
    // Anchored lone sample exactly at maxt (risk 3): upstream's
    // `lastSampleIndex--` goes transiently to -1; the exclusive-end port
    // yields the two boundary points, both carrying the sample's value.
    let f_in = [f(20, 5.0)];
    let out = extend_floats(&f_in, 0, 20, false);
    assert_eq!(points_of(&out), vec![(0, 5.0), (20, 5.0)]);
}

#[test]
fn extended_histogram_rate_gauge_hint_warns_not_a_counter() {
    // A mid gauge-hinted sample under a counter function warns (issue #125
    // live arm; corpus lines 588/592).
    let samples = [
        h_sample(0, 1.0, CounterResetHint::Unknown),
        h_sample(30_000, 2.0, CounterResetHint::Gauge),
        h_sample(60_000, 3.0, CounterResetHint::Unknown),
    ];
    let mut annos = Annotations::default();
    let _ = extended_histogram_rate(
        &samples, 90_000, 0, 90_000, false, true, false, "g", 0, &mut annos,
    );
    let (warnings, _) = annos.as_strings("", 64, 64);
    assert!(
        warnings.iter().any(|w| w.contains("not a counter")),
        "expected not-a-counter warning, got {warnings:?}"
    );
}
