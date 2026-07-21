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
        float_of(smoothed_instant(&samples, 10, 300_000, "m", &mut annos)),
        2.0
    );
    // Interpolate at t=5 between (0,1) and (10,2) → 1.5.
    assert_eq!(
        float_of(smoothed_instant(&samples, 5, 300_000, "m", &mut annos)),
        1.5
    );
    // Carry forward past the last sample.
    assert_eq!(
        float_of(smoothed_instant(&samples, 25, 300_000, "m", &mut annos)),
        3.0
    );
    // Only-future data ⇒ skip.
    assert!(smoothed_instant(&samples, 25, 5, "m", &mut annos).is_none());
}

#[test]
fn smoothed_instant_mixed_window_warns_and_skips() {
    let mut annos = Annotations::default();
    let samples = [f(0, 1.0), h_sample(10, 3.0, CounterResetHint::Unknown)];
    assert!(smoothed_instant(&samples, 5, 300_000, "mixed", &mut annos).is_none());
    let (warnings, _) = annos.as_strings(64, 64);
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
        &samples, 60_000, 0, 50_000, false, true, false, "h", &mut annos,
    );
    match out {
        Some(RangeValue::Histogram(h)) => {
            assert_eq!(h.counter_reset_hint, CounterResetHint::Gauge);
            assert!((h.count - 3.0).abs() < 1e-9, "count {}", h.count);
        }
        other => panic!("expected a histogram result, got {other:?}"),
    }
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
        &samples, 90_000, 0, 90_000, false, true, false, "g", &mut annos,
    );
    let (warnings, _) = annos.as_strings(64, 64);
    assert!(
        warnings.iter().any(|w| w.contains("not a counter")),
        "expected not-a-counter warning, got {warnings:?}"
    );
}
