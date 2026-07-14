//! Sample selection for an **instant vector** at one evaluation step: the
//! most recent sample within the 5-minute lookback `(t − lookback, t]`,
//! excluding the Prometheus stale-NaN bit pattern. Two structurally
//! distinct exclusion rules (architect plan edge case 3), both load-
//! bearing and tested independently:
//!
//! 1. **Lookback exclusion** — a sample older than `t − lookback_ms` makes
//!    the series absent at `t`, even if it is the only sample the series
//!    ever had.
//! 2. **Stale-NaN exclusion** — a sample whose bits equal
//!    [`pulsus_model::STALE_NAN_BITS`] makes the series absent at `t`,
//!    *even if it is the most recent sample within the lookback window*.
//!    An ordinary `NaN` (any other bit pattern) is **not** stale — only
//!    this exact bit pattern is, matching Prometheus's own
//!    `value.IsStaleNaN` (`.to_bits()` comparison, never `.is_nan()`,
//!    since `NaN != NaN` and a bare `is_nan()` check would not distinguish
//!    a genuinely stale marker from any other NaN payload).

use pulsus_model::STALE_NAN_BITS;

use crate::value::Sample;

/// Returns the most recent sample in `(t_ms − lookback_ms, t_ms]`, or
/// `None` if no such sample exists or the most recent one within that
/// window is the stale-NaN marker. `samples` must be sorted ascending by
/// `t_ms` (the fetch layer's own `ORDER BY` contract).
pub fn instant_value(samples: &[Sample], t_ms: i64, lookback_ms: i64) -> Option<Sample> {
    let lower_excl = t_ms - lookback_ms;
    // Greatest index whose t_ms <= t_ms (samples sorted ascending).
    let idx = samples.partition_point(|s| s.t_ms <= t_ms);
    if idx == 0 {
        return None;
    }
    let candidate = samples[idx - 1];
    if candidate.t_ms <= lower_excl {
        return None;
    }
    if candidate.v.to_bits() == STALE_NAN_BITS {
        return None;
    }
    Some(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(t_ms: i64, v: f64) -> Sample {
        Sample { t_ms, v }
    }

    #[test]
    fn picks_the_most_recent_sample_within_the_lookback_window() {
        let samples = vec![s(0, 1.0), s(100, 2.0), s(200, 3.0)];
        assert_eq!(instant_value(&samples, 250, 300_000), Some(s(200, 3.0)));
    }

    #[test]
    fn a_sample_exactly_at_the_eval_time_is_included_right_closed() {
        let samples = vec![s(100, 1.0)];
        assert_eq!(instant_value(&samples, 100, 300_000), Some(s(100, 1.0)));
    }

    #[test]
    fn a_sample_exactly_at_the_lookback_lower_bound_is_excluded_left_open() {
        // window is (t - lookback, t]; a sample exactly at t - lookback is
        // excluded (AC: left-open right-closed window boundaries).
        let samples = vec![s(0, 1.0)];
        assert_eq!(instant_value(&samples, 300_000, 300_000), None);
    }

    #[test]
    fn a_sample_one_ms_inside_the_lookback_lower_bound_is_included() {
        let samples = vec![s(1, 1.0)];
        assert_eq!(instant_value(&samples, 300_000, 300_000), Some(s(1, 1.0)));
    }

    #[test]
    fn a_sample_older_than_the_lookback_window_makes_the_series_absent() {
        let samples = vec![s(0, 1.0)];
        assert_eq!(instant_value(&samples, 300_001, 300_000), None);
    }

    #[test]
    fn a_future_sample_past_the_eval_time_is_never_selected() {
        let samples = vec![s(0, 1.0), s(1000, 2.0)];
        assert_eq!(instant_value(&samples, 500, 300_000), Some(s(0, 1.0)));
    }

    #[test]
    fn a_stale_nan_marker_sample_makes_the_series_absent_even_if_recent() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let samples = vec![s(0, 1.0), s(100, stale)];
        assert_eq!(instant_value(&samples, 100, 300_000), None);
    }

    #[test]
    fn an_ordinary_nan_that_is_not_the_stale_marker_is_not_treated_as_stale() {
        // Only the exact STALE_NAN_BITS pattern is stale — a bare `is_nan()`
        // check would wrongly exclude this.
        let ordinary_nan = f64::from_bits(0x7FF8_0000_0000_0001);
        assert!(ordinary_nan.is_nan());
        assert_ne!(ordinary_nan.to_bits(), STALE_NAN_BITS);
        let samples = vec![s(100, ordinary_nan)];
        let got = instant_value(&samples, 100, 300_000).unwrap();
        assert!(got.v.is_nan());
    }

    #[test]
    fn an_empty_sample_set_is_absent() {
        assert_eq!(instant_value(&[], 100, 300_000), None);
    }

    #[test]
    fn falls_back_past_a_stale_marker_only_when_it_is_not_the_most_recent_sample() {
        // The stale marker here is *not* the most-recent sample within the
        // window, so the lookup should never even reach it — it should
        // simply select the later, non-stale sample.
        let stale = f64::from_bits(STALE_NAN_BITS);
        let samples = vec![s(0, stale), s(100, 5.0)];
        assert_eq!(instant_value(&samples, 100, 300_000), Some(s(100, 5.0)));
    }
}
