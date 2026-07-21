//! Extended range-vector selector semantics (issue #150): the sample-
//! selection, interpolation, and counter-reset algorithms behind the
//! `anchored`/`smoothed` modifiers. Ported from Prometheus v3.13.0 at the
//! pinned conformance SHA `40af9c2` — `promql/functions.go` (`interpolate`,
//! `pickOrInterpolate{Left,Right}{,Histogram}`, `correctForCounterResets`
//! (`Histogram`), `validateHistogramRange`, `extendedRate`,
//! `extendedHistogramRate`, `pickFirstSampleIndices`) and
//! `promql/engine.go` (`smoothSeries`).
//!
//! Window contract: the caller ([`super::windowed_range_source`]) widens
//! the fetched sample slice (anchored: `(lower−lb, upper]`; smoothed:
//! `(lower−lb, upper+lb]`) but keeps `range_start_ms`/`range_end_ms` at the
//! ORIGINAL selector bounds, which these ports need for the boundary math
//! and the `isRate` divisor.

use pulsus_model::{CounterResetHint, FloatHistogram, FloatHistogramOpError};

use super::hist_range_fns::RangeValue;
use crate::annotations::{Annotations, messages};
use crate::value::Sample;

use CounterResetHint::Gauge;

/// `sort.Search(n, pred)` over the FIRST `n` samples: the smallest index in
/// `[0, n]` at which `pred` first holds (`pred` false below, true at/above).
/// Upstream deliberately bounds these searches by `lastSampleIndex` (=
/// `len-1`), NOT `len` — the trailing sample is excluded from the search so
/// the boundary picks stay off-by-one exact with the pin.
fn search_first<F: Fn(&Sample) -> bool>(samples: &[Sample], n: usize, pred: F) -> usize {
    samples[..n].partition_point(|s| !pred(s))
}

// ---------------------------------------------------------------------------
// Float boundary/interpolation helpers (functions.go:72-180).
// ---------------------------------------------------------------------------

/// `interpolate` (functions.go:89-101): linear interpolation between two
/// float points. When `is_counter` and `p2 < p1`, the counter is modelled
/// as restarting from zero (`y1 = 0`) so the interpolation spans the reset.
fn interpolate(p1: &Sample, p2: &Sample, t: i64, is_counter: bool) -> f64 {
    let y1 = if is_counter && p2.v < p1.v { 0.0 } else { p1.v };
    let y2 = p2.v;
    y1 + (y2 - y1) * (t - p1.t_ms) as f64 / (p2.t_ms - p1.t_ms) as f64
}

/// `pickOrInterpolateLeft` (functions.go:72-79).
fn pick_or_interpolate_left(
    f: &[Sample],
    first: usize,
    range_start: i64,
    smoothed: bool,
    is_counter: bool,
) -> f64 {
    if smoothed && f[first].t_ms < range_start {
        interpolate(&f[first], &f[first + 1], range_start, is_counter)
    } else {
        f[first].v
    }
}

/// `pickOrInterpolateRight` (functions.go:81-88).
fn pick_or_interpolate_right(
    f: &[Sample],
    last: usize,
    range_end: i64,
    smoothed: bool,
    is_counter: bool,
) -> f64 {
    if smoothed && last > 0 && f[last].t_ms > range_end {
        interpolate(&f[last - 1], &f[last], range_end, is_counter)
    } else {
        f[last].v
    }
}

/// `correctForCounterResets` (functions.go:164-180): the accumulated
/// correction over the interior float samples, using `left`/`right` as the
/// boundary comparison anchors.
fn correct_for_counter_resets(left: f64, right: f64, points: &[Sample]) -> f64 {
    let mut correction = 0.0;
    let mut prev = left;
    for p in points {
        if p.v < prev {
            correction += prev;
        }
        prev = p.v;
    }
    if right < prev {
        correction += prev;
    }
    correction
}

/// `extendedRate` (functions.go:305-357): anchored/smoothed
/// rate/increase/delta over an all-float window. `range_start_ms`/
/// `range_end_ms` are the ORIGINAL selector bounds; `samples` is the widened
/// window slice. Returns `None` (empty result) when no sample falls in range.
#[allow(clippy::too_many_arguments)]
pub(crate) fn extended_rate(
    samples: &[Sample],
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
    smoothed: bool,
    is_counter: bool,
    is_rate: bool,
) -> Option<f64> {
    let f = samples;
    if f.is_empty() {
        return None;
    }
    let orig_last = f.len() - 1;
    let mut first_sample_index =
        search_first(f, orig_last, |s| s.t_ms > range_start_ms).saturating_sub(1);
    let last_sample_index = if smoothed {
        search_first(f, orig_last, |s| s.t_ms >= range_end_ms)
    } else {
        orig_last
    };

    if f[last_sample_index].t_ms <= range_start_ms {
        return None;
    }
    if smoothed && f[first_sample_index].t_ms > range_end_ms {
        return None;
    }

    let left =
        pick_or_interpolate_left(f, first_sample_index, range_start_ms, smoothed, is_counter);
    let right = pick_or_interpolate_right(f, last_sample_index, range_end_ms, smoothed, is_counter);

    let mut result = right - left;

    if is_counter {
        // Boundary samples are already accounted for by pick/interpolate;
        // exclude them from the interior reset correction. Upstream slices
        // `f[firstSampleIndex : lastSampleIndex+1]` and does `lastSampleIndex--`
        // when the last sample sits at/after `range_end` — for a lone boundary
        // sample that yields an EMPTY interior slice (Go tolerates the transient
        // `-1` because it only ever uses `lastSampleIndex+1` as the exclusive
        // upper bound). Compute that exclusive upper bound directly so the
        // `usize` cannot underflow (functions.go:333-345). The invariant
        // `first_sample_index <= interior_end` holds: a single sample cannot be
        // both `<= range_start` and `>= range_end` (`range_ms > 0`), so the
        // half-open slice is always valid and `correct_for_counter_resets` still
        // runs its `right < left` boundary check over the empty slice.
        if f[first_sample_index].t_ms <= range_start_ms {
            first_sample_index += 1;
        }
        let interior_end = if f[last_sample_index].t_ms >= range_end_ms {
            last_sample_index
        } else {
            last_sample_index + 1
        };
        result += correct_for_counter_resets(left, right, &f[first_sample_index..interior_end]);
    }
    if is_rate {
        result /= range_ms as f64 / 1000.0;
    }
    Some(result)
}

// ---------------------------------------------------------------------------
// Histogram boundary/interpolation helpers (functions.go:104-303).
// ---------------------------------------------------------------------------

/// `interpolateHistograms` (functions.go:104-149). NHCB reconciliation infos
/// are pushed onto `annos`; `Err` is the incompatible-schema case.
fn interpolate_histograms(
    h1: &FloatHistogram,
    t1: i64,
    h2: &FloatHistogram,
    t2: i64,
    t: i64,
    is_counter: bool,
    annos: &mut Annotations,
) -> Result<FloatHistogram, FloatHistogramOpError> {
    if t == t1 {
        return Ok(h1.clone());
    }
    if t == t2 {
        return Ok(h2.clone());
    }
    let fraction = (t - t1) as f64 / (t2 - t1) as f64;

    if is_counter && h2.detect_reset(h1) {
        // Counter reset: model as restarting from zero (h2 scaled by the
        // fraction; the copy inherits h2's CounterResetHint).
        let mut r = h2.clone();
        r.mul(fraction);
        return Ok(r);
    }

    // result = h1 + (h2 - h1) * fraction.
    let mut result = h2.clone();
    let outcome = result.sub(h1)?;
    result = outcome.result;
    if outcome.nhcb_bounds_reconciled {
        annos.info(messages::mismatched_custom_buckets_histograms_info(
            messages::HistogramOperation::Sub,
        ));
    }
    result.mul(fraction);
    let outcome = result.add(h1)?;
    result = outcome.result;
    if outcome.nhcb_bounds_reconciled {
        annos.info(messages::mismatched_custom_buckets_histograms_info(
            messages::HistogramOperation::Add,
        ));
    }
    Ok(result)
}

/// `pickOrInterpolateLeftHistogram` (functions.go:151-158).
fn pick_or_interpolate_left_hist(
    h: &[Sample],
    first: usize,
    range_start: i64,
    smoothed: bool,
    is_counter: bool,
    annos: &mut Annotations,
) -> Result<FloatHistogram, FloatHistogramOpError> {
    if smoothed && h[first].t_ms < range_start {
        interpolate_histograms(
            hist(&h[first]),
            h[first].t_ms,
            hist(&h[first + 1]),
            h[first + 1].t_ms,
            range_start,
            is_counter,
            annos,
        )
    } else {
        Ok(hist(&h[first]).clone())
    }
}

/// `pickOrInterpolateRightHistogram` (functions.go:160-167).
fn pick_or_interpolate_right_hist(
    h: &[Sample],
    last: usize,
    range_end: i64,
    smoothed: bool,
    is_counter: bool,
    annos: &mut Annotations,
) -> Result<FloatHistogram, FloatHistogramOpError> {
    if smoothed && last > 0 && h[last].t_ms > range_end {
        interpolate_histograms(
            hist(&h[last - 1]),
            h[last - 1].t_ms,
            hist(&h[last]),
            h[last].t_ms,
            range_end,
            is_counter,
            annos,
        )
    } else {
        Ok(hist(&h[last]).clone())
    }
}

/// `validateHistogramRange` (functions.go:227-244): schema consistency plus
/// the counter-hint check. Returns `false` (with a warning) when exponential
/// and custom buckets are mixed; adds a not-a-counter warning per gauge-
/// hinted sample when `is_counter` (issue #125 — the live gauge-hint arm).
fn validate_histogram_range(
    h: &[Sample],
    is_counter: bool,
    annos: &mut Annotations,
    metric_name: &str,
) -> bool {
    let using_custom = hist(&h[0]).uses_custom_buckets();
    for p in h {
        let ph = hist(p);
        if ph.uses_custom_buckets() != using_custom {
            annos.warning(messages::mixed_exponential_custom_histograms_warning(
                metric_name,
            ));
            return false;
        }
        if is_counter && ph.counter_reset_hint == Gauge {
            annos.warning(messages::native_histogram_not_counter_warning(metric_name));
        }
    }
    true
}

/// `addHistogramWithAnnotations` (functions.go:187-201): in-place `base +=
/// other`, translating NHCB reconciliation to an info and incompatibility to
/// a warning + `false`.
fn add_hist_annos(
    base: &mut FloatHistogram,
    other: &FloatHistogram,
    annos: &mut Annotations,
    metric_name: &str,
) -> bool {
    match base.add(other) {
        Ok(outcome) => {
            *base = outcome.result;
            if outcome.nhcb_bounds_reconciled {
                annos.info(messages::mismatched_custom_buckets_histograms_info(
                    messages::HistogramOperation::Add,
                ));
            }
            true
        }
        Err(FloatHistogramOpError::IncompatibleSchema) => {
            annos.warning(messages::mixed_exponential_custom_histograms_warning(
                metric_name,
            ));
            false
        }
    }
}

/// `subHistogramWithAnnotations` (functions.go:204-218): in-place `base -=
/// other` with the same annotation translation as [`add_hist_annos`].
fn sub_hist_annos(
    base: &mut FloatHistogram,
    other: &FloatHistogram,
    annos: &mut Annotations,
    metric_name: &str,
) -> bool {
    match base.sub(other) {
        Ok(outcome) => {
            *base = outcome.result;
            if outcome.nhcb_bounds_reconciled {
                annos.info(messages::mismatched_custom_buckets_histograms_info(
                    messages::HistogramOperation::Sub,
                ));
            }
            true
        }
        Err(FloatHistogramOpError::IncompatibleSchema) => {
            annos.warning(messages::mixed_exponential_custom_histograms_warning(
                metric_name,
            ));
            false
        }
    }
}

/// `annosFromInterpolationError` (functions.go:181-185): the only classified
/// error is an incompatible schema → the mixed exp/custom warning.
fn annos_from_interpolation_error(annos: &mut Annotations, metric_name: &str) {
    annos.warning(messages::mixed_exponential_custom_histograms_warning(
        metric_name,
    ));
}

/// `correctForCounterResetsHistogram` (functions.go:247-303): the histogram
/// analogue of [`correct_for_counter_resets`]. `Ok(Some(_))` = a correction
/// histogram, `Ok(None)` = no correction, `Err(())` = a combine failure
/// (annotation already pushed). `left`/`right` are the boundary values;
/// `right` MUST be the un-mutated boundary so its `detect_reset` doesn't
/// self-detect.
#[allow(clippy::too_many_arguments)]
fn correct_for_counter_resets_hist(
    h: &[Sample],
    first_sample_index: usize,
    last_sample_index: usize,
    left: &FloatHistogram,
    right: &FloatHistogram,
    range_start: i64,
    smoothed: bool,
    annos: &mut Annotations,
    metric_name: &str,
) -> Result<Option<FloatHistogram>, ()> {
    // firstSampleIndex is represented by left, so the loop starts one beyond.
    let mut first = first_sample_index + 1;
    let mut prev: &FloatHistogram = left;
    if smoothed
        && h[first_sample_index].t_ms < range_start
        && hist(&h[first_sample_index + 1]).detect_reset(hist(&h[first_sample_index]))
    {
        // The left interpolation already spanned this reset; skip
        // h[firstSampleIndex+1] and use it as the comparison anchor.
        prev = hist(&h[first_sample_index + 1]);
        first += 1;
    }
    // lastSampleIndex is always excluded (right is a copy/interpolation that
    // inherits its hint). `first > last+1` == `first > lastSampleIndex`.
    if first > last_sample_index {
        return Ok(None);
    }

    let mut correction: Option<FloatHistogram> = None;
    for p in &h[first..last_sample_index] {
        let curr = hist(p);
        if curr.detect_reset(prev) {
            match &mut correction {
                None => correction = Some(prev.clone()),
                Some(c) => {
                    if !add_hist_annos(c, prev, annos, metric_name) {
                        return Err(());
                    }
                }
            }
        }
        prev = curr;
    }
    if right.detect_reset(prev) {
        match &mut correction {
            None => correction = Some(prev.clone()),
            Some(c) => {
                if !add_hist_annos(c, prev, annos, metric_name) {
                    return Err(());
                }
            }
        }
    }
    Ok(correction)
}

/// `extendedHistogramRate` (functions.go:359-448): the all-histogram analogue
/// of [`extended_rate`]. The result carries a `Gauge` hint (functions.go:441).
#[allow(clippy::too_many_arguments)]
pub(crate) fn extended_histogram_rate(
    samples: &[Sample],
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
    smoothed: bool,
    is_counter: bool,
    is_rate: bool,
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<RangeValue> {
    let h = samples;
    if h.is_empty() {
        return None;
    }
    let orig_last = h.len() - 1;
    let first_sample_index =
        search_first(h, orig_last, |s| s.t_ms > range_start_ms).saturating_sub(1);
    let last_sample_index = if smoothed {
        search_first(h, orig_last, |s| s.t_ms >= range_end_ms)
    } else {
        orig_last
    };

    if h[last_sample_index].t_ms <= range_start_ms {
        return None;
    }
    if smoothed && h[first_sample_index].t_ms > range_end_ms {
        return None;
    }

    if !validate_histogram_range(
        &h[first_sample_index..=last_sample_index],
        is_counter,
        annos,
        metric_name,
    ) {
        return None;
    }

    let left = match pick_or_interpolate_left_hist(
        h,
        first_sample_index,
        range_start_ms,
        smoothed,
        is_counter,
        annos,
    ) {
        Ok(v) => v,
        Err(_) => {
            annos_from_interpolation_error(annos, metric_name);
            return None;
        }
    };
    let right = match pick_or_interpolate_right_hist(
        h,
        last_sample_index,
        range_end_ms,
        smoothed,
        is_counter,
        annos,
    ) {
        Ok(v) => v,
        Err(_) => {
            annos_from_interpolation_error(annos, metric_name);
            return None;
        }
    };

    if !is_counter && (left.counter_reset_hint != Gauge || right.counter_reset_hint != Gauge) {
        annos.warning(messages::native_histogram_not_gauge_warning(metric_name));
    }

    // Copy right so correctForCounterResetsHistogram can still call
    // right.detect_reset without observing the subtraction's mutation.
    let mut result = right.clone();
    if !sub_hist_annos(&mut result, &left, annos, metric_name) {
        return None;
    }

    if is_counter {
        match correct_for_counter_resets_hist(
            h,
            first_sample_index,
            last_sample_index,
            &left,
            &right,
            range_start_ms,
            smoothed,
            annos,
            metric_name,
        ) {
            Err(()) => return None,
            Ok(Some(correction)) => {
                if !add_hist_annos(&mut result, &correction, annos, metric_name) {
                    return None;
                }
            }
            Ok(None) => {}
        }
    }
    if is_rate {
        result.div(range_ms as f64 / 1000.0);
    }
    result.counter_reset_hint = Gauge;
    result.compact();
    Some(RangeValue::Histogram(result))
}

// ---------------------------------------------------------------------------
// Anchored trim (functions.go:2217-2256) and smoothed instant
// (engine.go:1729-1832).
// ---------------------------------------------------------------------------

/// Merged-stream `pickFirstSampleIndices` (functions.go:2224-2256): the
/// anchor is the single most recent sample at or before `range_start_ms`,
/// retained as the baseline together with every later sample. `None` = no
/// sample lies strictly after the range start (nothing to measure).
pub(crate) fn anchor_trim(samples: &[Sample], range_start_ms: i64) -> Option<&[Sample]> {
    // idx = number of samples at or before the range start = index of the
    // first sample strictly after it.
    let idx = samples.partition_point(|s| s.t_ms <= range_start_ms);
    if idx >= samples.len() {
        // No sample strictly after the range start.
        return None;
    }
    if idx == 0 {
        // No anchor; every sample is after the range start.
        return Some(samples);
    }
    // Prepend the anchor (samples[idx-1]); drop earlier samples.
    Some(&samples[idx - 1..])
}

/// `smoothSeries`' per-step body (engine.go:1751-1832): the smoothed instant
/// value at `eff_t` over the `(eff_t−lb, eff_t+lb]` window. Returns the
/// interpolated/carried value as `(v, h)` — `h: None` for a float. `None`
/// skips the step (only-future data, an empty window, or a mixed-type
/// window, the last also emitting a warning).
pub(crate) fn smoothed_instant(
    samples: &[Sample],
    eff_t: i64,
    lookback_ms: i64,
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<(f64, Option<Box<FloatHistogram>>)> {
    let start = samples.partition_point(|s| s.t_ms <= eff_t - lookback_ms);
    let end = samples.partition_point(|s| s.t_ms <= eff_t + lookback_ms);
    let window: Vec<&Sample> = samples[start..end]
        .iter()
        .filter(|s| !s.is_stale())
        .collect();
    if window.is_empty() {
        return None;
    }
    let has_hist = window.iter().any(|s| s.h.is_some());
    let has_float = window.iter().any(|s| s.h.is_none());
    if has_hist && has_float {
        annos.warning(messages::mixed_floats_histograms_warning(metric_name));
        return None;
    }

    // First index with T >= eff_t (dataTS).
    let i = window.partition_point(|s| s.t_ms < eff_t);

    if has_hist {
        if i < window.len() && window[i].t_ms == eff_t {
            // Exact match.
            return Some((0.0, Some(Box::new(hist(window[i]).clone()))));
        }
        if i > 0 && i < window.len() {
            let prev = window[i - 1];
            let next = window[i];
            let ph = hist(prev);
            let nh = hist(next);
            if ph.uses_custom_buckets() != nh.uses_custom_buckets() {
                annos.warning(messages::mixed_exponential_custom_histograms_warning(
                    metric_name,
                ));
                return None;
            }
            // Treat as counter unless BOTH neighbours carry the gauge hint
            // (engine.go:1786).
            let is_counter = ph.counter_reset_hint != Gauge || nh.counter_reset_hint != Gauge;
            match interpolate_histograms(ph, prev.t_ms, nh, next.t_ms, eff_t, is_counter, annos) {
                Ok(h) => return Some((0.0, Some(Box::new(h)))),
                Err(_) => {
                    annos_from_interpolation_error(annos, metric_name);
                    return None;
                }
            }
        }
        if i > 0 {
            // No next point; carry forward, resetting the hint to Unknown
            // (engine.go:1796-1801 — the hint describes a pair, not a value).
            let mut h = hist(window[i - 1]).clone();
            h.counter_reset_hint = CounterResetHint::Unknown;
            return Some((0.0, Some(Box::new(h))));
        }
        // i == 0: only-future data; skip.
        None
    } else {
        if i < window.len() && window[i].t_ms == eff_t {
            return Some((window[i].v, None));
        }
        if i > 0 && i < window.len() {
            // Float interpolation is always non-counter (engine.go:1822 TODO).
            let val = interpolate(window[i - 1], window[i], eff_t, false);
            return Some((val, None));
        }
        if i > 0 {
            // Carry forward the previous value.
            return Some((window[i - 1].v, None));
        }
        None
    }
}

/// A window sample's histogram channel — all callers here have already
/// established the sample is a histogram (the dispatch/validation upstream).
fn hist(s: &Sample) -> &FloatHistogram {
    s.h.as_ref()
        .expect("extended histogram path only runs on all-histogram windows")
}

#[cfg(test)]
mod tests;
