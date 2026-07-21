//! Histogram-aware range-vector function set (M7-A5b-ii): `rate`/
//! `increase`/`delta`/`irate`/`idelta` over native histograms — ports
//! `extrapolatedRate`'s histogram branch + `histogramRate`
//! (`promql/functions.go:452-591,597-701`, pinned `40af9c2`, v3.13.0) and
//! `instantValue`'s histogram branch (`:760-884`). Plain `Add`/`Sub` (not
//! `KahanAdd`) throughout — plan v2 OQ3: only `sum`/`avg`(`_over_time`)
//! aggregation is compensated.
//!
//! A window with NO histogram sample dispatches to the byte-unchanged
//! float-only path ([`super::functions::eval_range_fn`]); a window mixing
//! floats and histograms warns and drops
//! ([`crate::annotations::messages::mixed_floats_histograms_warning`]) —
//! `windowed_range_source` (`eval/mod.rs`) no longer rejects a histogram
//! outright, so this module is the new chokepoint for these five
//! functions specifically.

use pulsus_model::{FloatHistogram, FloatHistogramOpError};

use crate::annotations::{Annotations, messages};
use crate::plan::{OverTimeFn, RangeFn};
use crate::value::Sample;

/// A range-function result: a float (the ordinary case, or a histogram
/// window's error/mixed disposition) or a histogram (a native-histogram
/// counter's rate/increase/delta/irate).
#[derive(Debug, Clone)]
pub enum RangeValue {
    Float(f64),
    Histogram(FloatHistogram),
}

/// `rate`/`increase`/`delta`/`irate` over a window that may contain
/// histogram samples. `metric_name` feeds the mixed/incompatible-schema
/// warnings (empty if none — same convention as [`super::histogram_fns`]).
pub fn eval_range_fn_hist(
    func: RangeFn,
    samples: &[Sample],
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<RangeValue> {
    // `irate` (`instantValue`) only ever looks at the last TWO samples in
    // the window — its own mixed-type check is a per-pair check, not a
    // whole-window one (`functions.go:869-872`'s `default:` arm), unlike
    // `rate`/`increase`/`delta` below. `instant_value_hist` is therefore
    // self-contained and bypasses the whole-window gate entirely.
    if func == RangeFn::Irate {
        return instant_value_hist(samples, true, metric_name, annos);
    }
    let hist_count = samples.iter().filter(|s| s.h.is_some()).count();
    if hist_count == 0 {
        return super::functions::eval_range_fn(
            func,
            samples,
            range_ms,
            range_start_ms,
            range_end_ms,
        )
        .map(RangeValue::Float);
    }
    if hist_count != samples.len() {
        annos.warning(messages::mixed_floats_histograms_warning(metric_name));
        return None;
    }
    match func {
        RangeFn::Irate => unreachable!("handled above"),
        RangeFn::Rate => extrapolated_rate_hist(
            samples,
            range_ms,
            range_start_ms,
            range_end_ms,
            true,
            true,
            metric_name,
            annos,
        ),
        RangeFn::Increase => extrapolated_rate_hist(
            samples,
            range_ms,
            range_start_ms,
            range_end_ms,
            true,
            false,
            metric_name,
            annos,
        ),
        RangeFn::Delta => extrapolated_rate_hist(
            samples,
            range_ms,
            range_start_ms,
            range_end_ms,
            false,
            false,
            metric_name,
            annos,
        ),
    }
}

/// `irate`/`idelta`'s shared last-two-samples rule — ports `instantValue`
/// (`functions.go:760-884`) IN FULL (not just its histogram arm): looks
/// only at the two chronologically-latest samples in `samples` (which,
/// since the merged `Sample` stream is timestamp-interleaved with at most
/// one value — float XOR histogram — per timestamp, `samples[len-2..]`
/// IS exactly upstream's merged `ss[0]`/`ss[1]`), and dispatches on their
/// types: both float → the byte-unchanged
/// [`super::functions::eval_irate`]/[`super::functions::eval_idelta`];
/// both histogram → the port below; one of each → `default:`'s
/// `NewMixedFloatsHistogramsWarning`. `is_rate` selects per-second
/// division (`irate`) vs the raw difference (`idelta`); it also gates
/// whether a detected counter reset skips the subtraction (`irate`
/// returns the LAST value verbatim on a reset — matching float `irate`'s
/// own reset rule — while `idelta` always subtracts, matching upstream's
/// `!isRate ||` short-circuit).
pub fn instant_value_hist(
    samples: &[Sample],
    is_rate: bool,
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<RangeValue> {
    if samples.len() < 2 {
        return None;
    }
    let last = &samples[samples.len() - 1];
    let prev = &samples[samples.len() - 2];
    let interval_ms = last.t_ms - prev.t_ms;
    if interval_ms == 0 {
        return None;
    }
    let (last_h, prev_h) = match (&last.h, &prev.h) {
        (None, None) => {
            let f = if is_rate {
                super::functions::eval_range_fn(RangeFn::Irate, samples, 0, 0, 0)
            } else {
                super::functions::eval_over_time(OverTimeFn::Idelta, samples)
            };
            return f.map(RangeValue::Float);
        }
        (Some(l), Some(p)) => (l, p),
        _ => {
            annos.warning(messages::mixed_floats_histograms_warning(metric_name));
            return None;
        }
    };

    let mut result = last_h.as_ref().clone();
    // Issue #125 (hints now stored/propagated): the pin's exact hint
    // conditions (`functions.go:846-853`). "irate should only be applied
    // to counters" — warn when EITHER sample IS gauge-hinted; "idelta
    // should only be applied to gauges" — warn when EITHER sample ISN'T.
    // Both fire BEFORE the subtraction (the warning also accompanies a
    // later incompatible-schema warning, matching the pin's ordering).
    use pulsus_model::CounterResetHint::Gauge;
    if is_rate && (last_h.counter_reset_hint == Gauge || prev_h.counter_reset_hint == Gauge) {
        annos.warning(messages::native_histogram_not_counter_warning(metric_name));
    }
    if !is_rate && (last_h.counter_reset_hint != Gauge || prev_h.counter_reset_hint != Gauge) {
        annos.warning(messages::native_histogram_not_gauge_warning(metric_name));
    }
    let should_subtract = !is_rate || !last_h.detect_reset(prev_h);
    if should_subtract {
        match result.sub(prev_h) {
            Ok(outcome) => {
                result = outcome.result;
                // `instantValue`'s reconcile info (`functions.go:863-865`).
                if outcome.nhcb_bounds_reconciled {
                    annos.info(messages::mismatched_custom_buckets_histograms_info(
                        messages::HistogramOperation::Sub,
                    ));
                }
            }
            Err(FloatHistogramOpError::IncompatibleSchema) => {
                annos.warning(messages::mixed_exponential_custom_histograms_warning(
                    metric_name,
                ));
                return None;
            }
        }
    }
    // Else: a reset was detected for `irate` — `resultSample` stays the
    // last sample's own copy, matching upstream's "leave resultSample at
    // its current value" comment (`functions.go:842-843`).
    //
    // The result is a computed difference/rate, never a counter — the
    // pin marks it gauge unconditionally (`functions.go:867`).
    result.counter_reset_hint = Gauge;
    result.compact();
    if is_rate {
        result.div(interval_ms as f64 / 1000.0);
    }
    Some(RangeValue::Histogram(result))
}

/// `histogramRate` (`functions.go:597-701`): the counter-reset-corrected
/// delta between the FIRST and LAST histogram sample in `points`, reduced
/// to the minimum schema seen across the window. `points` must be
/// all-histogram with at least 2 elements.
fn histogram_rate(
    points: &[Sample],
    is_counter: bool,
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<FloatHistogram> {
    let last = points[points.len() - 1].h.as_ref()?.as_ref().clone();
    let mut prev = points[0].h.as_ref()?.as_ref().clone();

    // Issue #125: "this native histogram metric is not a counter" — the
    // pin checks the FIRST and LAST points' hints up front
    // (`functions.go:615-620`; the mid-loop below covers the rest), BEFORE
    // the reset null-out (which replaces `prev` with an empty histogram).
    use pulsus_model::CounterResetHint::Gauge;
    if is_counter && (prev.counter_reset_hint == Gauge || last.counter_reset_hint == Gauge) {
        annos.warning(messages::native_histogram_not_counter_warning(metric_name));
    }

    // Null out the 1st sample if there's a counter reset between it and
    // the 2nd — we then don't need the 1st sample's (possibly
    // incompatible) bucket layout at all.
    if is_counter && points.len() > 1 {
        let second = points[1].h.as_ref()?;
        if second.detect_reset(&prev) {
            prev = FloatHistogram {
                counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                schema: second.schema,
                zero_threshold: 0.0,
                zero_count: 0.0,
                count: 0.0,
                sum: 0.0,
                positive_spans: Vec::new(),
                negative_spans: Vec::new(),
                positive_buckets: Vec::new(),
                negative_buckets: Vec::new(),
                custom_values: second.custom_values.clone(),
            };
        }
    }

    if last.uses_custom_buckets() != prev.uses_custom_buckets() {
        annos.warning(messages::mixed_exponential_custom_histograms_warning(
            metric_name,
        ));
        return None;
    }

    let mut min_schema = last.schema.min(prev.schema);
    if is_counter {
        for p in &points[1..points.len() - 1] {
            let curr = p.h.as_ref()?;
            // Mid-window gauge-hinted sample under a counter function —
            // the pin's per-sample check (`functions.go:649-651`; dedup
            // collapses repeats to one warning).
            if curr.counter_reset_hint == Gauge {
                annos.warning(messages::native_histogram_not_counter_warning(metric_name));
            }
            if curr.schema < min_schema {
                min_schema = curr.schema;
            }
            if curr.uses_custom_buckets() != prev.uses_custom_buckets() {
                annos.warning(messages::mixed_exponential_custom_histograms_warning(
                    metric_name,
                ));
                return None;
            }
        }
    }

    let mut h = if min_schema == last.schema {
        last.clone()
    } else {
        last.copy_to_schema(min_schema)
    };
    // `Sub`'s own internal schema reduction handles `prev` regardless of
    // its (>= min_schema) schema — see `float_histogram_ops.rs`'s doc.
    match h.sub(&prev) {
        Ok(outcome) => {
            h = outcome.result;
            // `histogramRate`'s Sub reconcile info (`functions.go:672-674`).
            if outcome.nhcb_bounds_reconciled {
                annos.info(messages::mismatched_custom_buckets_histograms_info(
                    messages::HistogramOperation::Sub,
                ));
            }
        }
        Err(FloatHistogramOpError::IncompatibleSchema) => {
            annos.warning(messages::mixed_exponential_custom_histograms_warning(
                metric_name,
            ));
            return None;
        }
    }

    if is_counter {
        let mut prev_iter = prev;
        for p in &points[1..] {
            let curr = p.h.as_ref()?;
            if curr.detect_reset(&prev_iter) {
                match h.add(&prev_iter) {
                    Ok(outcome) => {
                        h = outcome.result;
                        // The counter-reset-loop Add reconcile info
                        // (`functions.go:689-691`).
                        if outcome.nhcb_bounds_reconciled {
                            annos.info(messages::mismatched_custom_buckets_histograms_info(
                                messages::HistogramOperation::Add,
                            ));
                        }
                    }
                    Err(FloatHistogramOpError::IncompatibleSchema) => {
                        annos.warning(messages::mixed_exponential_custom_histograms_warning(
                            metric_name,
                        ));
                        return None;
                    }
                }
            }
            prev_iter = curr.as_ref().clone();
        }
    } else if points[0].h.as_ref()?.counter_reset_hint != Gauge
        || points[points.len() - 1].h.as_ref()?.counter_reset_hint != Gauge
    {
        // `delta` "should only be applied to gauges" — the pin's `else if`
        // (`functions.go:695-697`) warns when the FIRST or LAST point's
        // `CounterResetHint != GaugeType` (the ORIGINAL first point, not
        // the possibly-nulled `prev`), once per series, after a successful
        // Sub (an incompatible-schema Sub returned above, before this
        // point, exactly like the pin's early return). Hint-conditional
        // since issue #125.
        annos.warning(messages::native_histogram_not_gauge_warning(metric_name));
    }
    // The rate/increase/delta result is a computed difference, never a
    // counter — marked gauge unconditionally (`functions.go:699`).
    h.counter_reset_hint = Gauge;
    h.compact();
    Some(h)
}

/// `extrapolatedRate`'s histogram branch (`functions.go:489-590`):
/// `histogram_rate` plus the shared extrapolation-factor computation
/// (duration-to-start/end, the counter zero-point override, the 1.1x
/// threshold), applied via [`FloatHistogram::mul`] instead of the float
/// path's `*=`. Self-contained (not shared with
/// [`super::functions::eval_range_fn`]'s float `eval_extrapolated`) —
/// deliberately duplicated rather than refactoring already-reviewed,
/// heavily-tested float code (risk-minimization convention already used
/// elsewhere in this crate).
#[allow(clippy::too_many_arguments)]
fn extrapolated_rate_hist(
    samples: &[Sample],
    range_ms: i64,
    range_start_ms: i64,
    range_end_ms: i64,
    is_counter: bool,
    is_rate: bool,
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<RangeValue> {
    if samples.len() < 2 {
        return None;
    }
    let mut result = histogram_rate(samples, is_counter, metric_name, annos)?;

    let first_t = samples[0].t_ms;
    let last_t = samples[samples.len() - 1].t_ms;
    let num_samples_minus_one = (samples.len() - 1) as f64;

    let mut duration_to_start = (first_t - range_start_ms) as f64 / 1000.0;
    let mut duration_to_end = (range_end_ms - last_t) as f64 / 1000.0;
    let sampled_interval = (last_t - first_t) as f64 / 1000.0;
    let average_duration_between_samples = sampled_interval / num_samples_minus_one;
    let extrapolation_threshold = average_duration_between_samples * 1.1;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration_between_samples / 2.0;
    }
    if is_counter {
        let mut duration_to_zero = duration_to_start;
        if result.count > 0.0 {
            // The ORIGINAL first sample's count (not the possibly-nulled
            // `prev` inside `histogram_rate`) — `functions.go:568`.
            let first_count = samples[0].h.as_ref()?.count;
            if first_count >= 0.0 {
                duration_to_zero = sampled_interval * (first_count / result.count);
            }
        }
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    let mut factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    if is_rate {
        factor /= range_ms as f64 / 1000.0;
    }
    result.mul(factor);
    Some(RangeValue::Histogram(result))
}

/// The `OverTimeFn` (`*_over_time`/`idelta`/`resets`/`changes`) histogram-
/// aware result: a float (the ordinary case) or a preserved histogram
/// (`last_over_time`/`first_over_time`).
#[derive(Debug, Clone)]
pub enum OverTimeValue {
    Float(f64),
    Histogram(FloatHistogram),
}

impl From<RangeValue> for OverTimeValue {
    fn from(v: RangeValue) -> Self {
        match v {
            RangeValue::Float(f) => OverTimeValue::Float(f),
            RangeValue::Histogram(h) => OverTimeValue::Histogram(h),
        }
    }
}

/// The `OverTimeFn` disposition map (plan v3/v4, issue #124) — complete
/// since M7-A5b-iii: `Sum`/`Avg` dispatch to the KahanAdd fold below
/// ([`eval_sum_avg_over_time_hist`]), every other variant per A5b-ii.
///
/// **IMPLEMENT / preserve / count** (`functions.go`, cited per arm below).
/// **DROP + info-only-on-a-MIXED-window** (`min`/`max`/`stddev`/`stdvar`/
/// `mad`/`deriv`/`ts_of_min`/`ts_of_max`_over_time — `compareOverTime`
/// `:1455-1481`, `varianceOverTime` `:1602-1630`, `funcMadOverTime`
/// `:1368-1392`, `funcDeriv` `:1892-1916`): a histogram-only window is
/// SILENT (plan v4 residual A — the float-count check returns before any
/// annotation), a MIXED window drops the histograms and fires
/// `HistogramIgnoredInMixedRangeInfo` once.
pub fn eval_over_time_hist(
    func: OverTimeFn,
    samples: &[Sample],
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<OverTimeValue> {
    match func {
        // `funcLastOverTime`/`funcFirstOverTime` (`:1308-1365`): preserve
        // whichever sample (float or histogram) is positionally last/first
        // — already exactly `samples.last()`/`samples.first()` since the
        // merged `Sample` stream is timestamp-interleaved by construction
        // (the A5a dual-read merge), unlike upstream's separate
        // Floats/Histograms arrays.
        OverTimeFn::Last => samples.last().map(sample_to_over_time_value),
        OverTimeFn::First => samples.first().map(sample_to_over_time_value),
        // `funcIdelta` (`:756-758`) shares `instantValue` with `irate`.
        OverTimeFn::Idelta => {
            instant_value_hist(samples, false, metric_name, annos).map(OverTimeValue::from)
        }
        // `funcResets`/`funcChanges` (`:2258-2327`/`:2330-2379`): an EMPTY
        // `(t-r, t]` window drops the series — upstream never invokes the
        // function for a pointless series, so emitting `Float(0.0)` here
        // manufactured a phantom `0` series (issue #130,
        // extended_vectors.test:314/:336). Every other arm already returns
        // `None` on empty.
        OverTimeFn::Resets => {
            (!samples.is_empty()).then(|| OverTimeValue::Float(eval_resets_hist(samples)))
        }
        OverTimeFn::Changes => {
            (!samples.is_empty()).then(|| OverTimeValue::Float(eval_changes_hist(samples)))
        }
        // `funcCountOverTime` (`len(Floats)+len(Histograms)`),
        // `funcPresentOverTime` (type-agnostic `1`),
        // `funcTsOfFirstOverTime`/`funcTsOfLastOverTime` (positional
        // timestamp) — already type-agnostic in the pre-A5b-ii
        // `functions::eval_over_time` (none of the four read `.v`), so no
        // new logic: this arm just removes the old blanket reject.
        OverTimeFn::Count | OverTimeFn::Present | OverTimeFn::TsOfFirst | OverTimeFn::TsOfLast => {
            super::functions::eval_over_time(func, samples).map(OverTimeValue::Float)
        }
        OverTimeFn::Min
        | OverTimeFn::Max
        | OverTimeFn::Stddev
        | OverTimeFn::Stdvar
        | OverTimeFn::Mad
        | OverTimeFn::Deriv
        | OverTimeFn::TsOfMin
        | OverTimeFn::TsOfMax => eval_drop_set_over_time(func, samples, metric_name, annos),
        // M7-A5b-iii: `funcSumOverTime`/`funcAvgOverTime`'s histogram
        // (KahanAdd) path — deferred out of A5b-ii, landed here.
        OverTimeFn::Sum | OverTimeFn::Avg => {
            eval_sum_avg_over_time_hist(func, samples, metric_name, annos)
        }
    }
}

/// `funcSumOverTime`/`funcAvgOverTime`'s histogram-aware path
/// (`functions.go:1148-1163` avg / `1498-1513` sum — the `len(Floats)>0 &&
/// len(Histograms)>0` mixed-window warning; `aggrHistOverTime` + the
/// `sum`/`avg` full-histogram KahanAdd fold otherwise — `avg`'s
/// direct-mean-with-overflow-switch mirrors the pin verbatim, per
/// `aggregation.rs`'s `fold_histogram_into_avg` (deliberately duplicated
/// here rather than sharing an `Acc`-shaped type across the two call
/// shapes — flagged as a candidate follow-up refactor, not attempted
/// under this item's scope)). A pure-float window delegates to the
/// byte-unchanged [`super::functions::eval_over_time`].
///
/// Pin-exact tails: NO `Compact` (neither `funcSumOverTime` nor
/// `funcAvgOverTime` compacts — unlike the `engine.go` aggregation arms);
/// `sum`'s final flush captures the flush-`Add`'s own
/// `nhcbBoundsReconciled` (`functions.go:1547-1554`) while `avg`'s two
/// flush arms discard it (`:1249-1256`, the `_, _, _, err :=` pattern);
/// an `ErrHistogramsIncompatibleSchema` anywhere — mid-fold or in the
/// final flush — yields the `MixedExponentialCustomHistogramsWarning` and
/// an EMPTY result with every accumulated fold annotation DISCARDED
/// (`funcSumOverTime`'s error arm returns a FRESH annotation set,
/// `:1558-1562`, dropping the deferred `nhcbBoundsReconciledSeen` info) —
/// hence the info here is buffered in `nhcb_seen` and emitted only on
/// success.
fn eval_sum_avg_over_time_hist(
    func: OverTimeFn,
    samples: &[Sample],
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<OverTimeValue> {
    let has_float = samples.iter().any(|s| s.h.is_none());
    let has_hist = samples.iter().any(|s| s.h.is_some());
    if samples.is_empty() {
        return None;
    }
    if has_float && has_hist {
        annos.warning(messages::mixed_floats_histograms_warning(metric_name));
        return None;
    }
    if !has_hist {
        return super::functions::eval_over_time(func, samples).map(OverTimeValue::Float);
    }

    let mut hists = samples.iter().filter_map(|s| s.h.as_deref());
    let mut sum = hists
        .next()
        .expect("has_hist implies at least one histogram sample")
        .clone();
    // The running FULL compensation histogram (upstream `comp`/`kahanC`,
    // `nil` until the first KahanAdd).
    let mut c: Option<FloatHistogram> = None;
    let mut count = 1.0_f64;
    let mut incremental_mean = false;
    let mut mean: Option<FloatHistogram> = None;
    // The pin's `nhcbBoundsReconciledSeen` — added once, in the deferred
    // block, only when the fold SUCCEEDS (see the fn doc).
    let mut nhcb_seen = false;
    // Issue #125: the pin's `counterResetSeen`/`notCounterResetSeen`
    // tracking over INPUT sample hints (`functions.go:1178-1196` and the
    // avg twin) — `trackCounterReset` runs on the first sample and each
    // subsequent one; both-seen ⇒ the collision warning, emitted with the
    // success tail below (the pin's deferred add is discarded on the
    // error path, which returns a fresh annotation set).
    let mut cr_seen = false;
    let mut ncr_seen = false;
    let mut track_counter_reset = |h: &FloatHistogram| match h.counter_reset_hint {
        pulsus_model::CounterResetHint::CounterReset => cr_seen = true,
        pulsus_model::CounterResetHint::NotCounterReset => ncr_seen = true,
        _ => {}
    };
    track_counter_reset(&sum);

    macro_rules! incompatible_schema {
        () => {{
            annos.warning(messages::mixed_exponential_custom_histograms_warning(
                metric_name,
            ));
            return None;
        }};
    }

    for h in hists {
        track_counter_reset(h);
        count += 1.0;
        match func {
            OverTimeFn::Sum => match sum.kahan_add(h, c.as_ref()) {
                Ok(outcome) => {
                    nhcb_seen |= outcome.nhcb_bounds_reconciled;
                    sum = outcome.result;
                    c = Some(outcome.compensation);
                }
                Err(FloatHistogramOpError::IncompatibleSchema) => incompatible_schema!(),
            },
            OverTimeFn::Avg => {
                if !incremental_mean {
                    let outcome = match sum.kahan_add(h, c.as_ref()) {
                        Ok(o) => o,
                        Err(FloatHistogramOpError::IncompatibleSchema) => incompatible_schema!(),
                    };
                    nhcb_seen |= outcome.nhcb_bounds_reconciled;
                    if !outcome.result.has_overflow() {
                        sum = outcome.result;
                        c = Some(outcome.compensation);
                        continue;
                    }
                    // Overflow: switch to incremental mean, seeded from
                    // the pre-overflow sum/compensation, the compensation
                    // scaled as a WHOLE histogram (`kahanC.Div(count-1)`,
                    // `functions.go:1229-1232`).
                    incremental_mean = true;
                    let mut m = sum.clone();
                    m.div(count - 1.0);
                    mean = Some(m);
                    if let Some(c) = c.as_mut() {
                        c.div(count - 1.0);
                    }
                }
                let q = (count - 1.0) / count;
                if let Some(c) = c.as_mut() {
                    c.mul(q);
                }
                let mut to_add = h.clone();
                to_add.div(count);
                let mut scaled_mean = mean.clone().expect("incremental_mean implies mean is Some");
                scaled_mean.mul(q);
                match scaled_mean.kahan_add(&to_add, c.as_ref()) {
                    Ok(outcome) => {
                        nhcb_seen |= outcome.nhcb_bounds_reconciled;
                        mean = Some(outcome.result);
                        c = Some(outcome.compensation);
                    }
                    Err(FloatHistogramOpError::IncompatibleSchema) => incompatible_schema!(),
                }
            }
            _ => unreachable!("only called for Sum/Avg"),
        }
    }

    // Final compensation flush — a full-histogram `Add`, guarded on
    // presence like the pin's `!= nil` checks, error → the same
    // incompatible-schema disposition as a mid-fold error (fn doc).
    let result = match (func, incremental_mean) {
        // `funcSumOverTime` (`:1547-1554`): `sum.Add(comp)` — the flush's
        // own `nhcbBoundsReconciled` counts toward the deferred info.
        (OverTimeFn::Sum, _) => match c {
            Some(comp) => match sum.add(&comp) {
                Ok(outcome) => {
                    nhcb_seen |= outcome.nhcb_bounds_reconciled;
                    outcome.result
                }
                Err(FloatHistogramOpError::IncompatibleSchema) => incompatible_schema!(),
            },
            None => sum,
        },
        // `funcAvgOverTime` incremental tail (`:1247-1252`):
        // `mean.Add(kahanC)`, nhcb flag discarded (`_, _, _, err :=`).
        (OverTimeFn::Avg, true) => {
            let mean = mean.expect("incremental_mean implies mean is Some");
            match c {
                Some(comp) => match mean.add(&comp) {
                    Ok(outcome) => outcome.result,
                    Err(FloatHistogramOpError::IncompatibleSchema) => incompatible_schema!(),
                },
                None => mean,
            }
        }
        // `funcAvgOverTime` direct tail (`:1253-1258`):
        // `sum.Div(count).Add(kahanC.Div(count))` — the compensation
        // scaled as a whole histogram; nhcb flag discarded.
        (OverTimeFn::Avg, false) => {
            sum.div(count);
            match c {
                Some(mut comp) => {
                    comp.div(count);
                    match sum.add(&comp) {
                        Ok(outcome) => outcome.result,
                        Err(FloatHistogramOpError::IncompatibleSchema) => incompatible_schema!(),
                    }
                }
                None => sum,
            }
        }
        _ => unreachable!("only called for Sum/Avg"),
    };
    // The deferred block's order (`functions.go:1188-1195`): collision
    // warning first, then the NHCB info.
    if cr_seen && ncr_seen {
        annos.warning(messages::histogram_counter_reset_collision_warning(
            messages::HistogramOperation::Agg,
        ));
    }
    if nhcb_seen {
        annos.info(messages::mismatched_custom_buckets_histograms_info(
            messages::HistogramOperation::Agg,
        ));
    }
    // NO Compact — neither pinned function compacts its result (unlike
    // the engine aggregation arms' `Compact(0)`).
    Some(OverTimeValue::Histogram(result))
}

fn sample_to_over_time_value(s: &Sample) -> OverTimeValue {
    match &s.h {
        Some(h) => OverTimeValue::Histogram(h.as_ref().clone()),
        None => OverTimeValue::Float(s.v),
    }
}

/// `funcResets`'s histogram-aware path (`functions.go:2258-2327`): a reset
/// is a strict value drop (float→float, unchanged from
/// [`super::functions::eval_resets`]'s doc), ANY type transition
/// (float↔histogram), or `curr.DetectReset(prev)` for a histogram→
/// histogram pair.
fn eval_resets_hist(samples: &[Sample]) -> f64 {
    let mut resets = 0.0_f64;
    for w in samples.windows(2) {
        let is_reset = match (&w[0].h, &w[1].h) {
            (None, None) => w[1].v < w[0].v,
            (Some(prev), Some(curr)) => curr.detect_reset(prev),
            _ => true, // a float<->histogram transition is always a reset.
        };
        if is_reset {
            resets += 1.0;
        }
    }
    resets
}

/// `funcChanges`'s histogram-aware path (`functions.go:2330-2379`): a
/// change is a value inequality (float→float, both-NaN excepted,
/// unchanged from [`super::functions::eval_changes`]'s doc), ANY type
/// transition, or `!curr.Equals(prev)` (semantic equality — compaction-
/// sensitive, distinct from [`pulsus_model::FloatHistogram::bits_eq`]) for
/// a histogram→histogram pair.
fn eval_changes_hist(samples: &[Sample]) -> f64 {
    let mut changes = 0.0_f64;
    for w in samples.windows(2) {
        let is_change = match (&w[0].h, &w[1].h) {
            (None, None) => {
                let (prev, cur) = (w[0].v, w[1].v);
                cur != prev && !(cur.is_nan() && prev.is_nan())
            }
            (Some(prev), Some(curr)) => !curr.equals(prev),
            _ => true,
        };
        if is_change {
            changes += 1.0;
        }
    }
    changes
}

/// The shared DROP-set disposition (module doc above): float-only
/// samples feed the byte-unchanged [`super::functions::eval_over_time`];
/// a histogram present alongside at least one float fires
/// `HistogramIgnoredInMixedRangeInfo`. An all-histogram (zero-float)
/// window is silent — `floats.is_empty()` short-circuits before any
/// annotation, mirroring every cited function's own `len(Floats) == 0`
/// early return.
fn eval_drop_set_over_time(
    func: OverTimeFn,
    samples: &[Sample],
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<OverTimeValue> {
    let floats: Vec<Sample> = samples.iter().filter(|s| s.h.is_none()).cloned().collect();
    if floats.is_empty() {
        return None;
    }
    let hist_present = floats.len() != samples.len();
    let result = super::functions::eval_over_time(func, &floats);
    if hist_present {
        annos.info(messages::histogram_ignored_in_mixed_range_info(metric_name));
    }
    result.map(OverTimeValue::Float)
}

/// `funcQuantileOverTime`'s histogram-aware path (`functions.go:1578-
/// 1600`): the same DROP-set disposition as [`eval_drop_set_over_time`]
/// (silent on a histogram-only window, `HistogramIgnoredInMixedRangeInfo`
/// on a mixed one), applied on top of the existing (pre-A5b, unchanged)
/// float `quantile_over_time` — an out-of-range φ's
/// `NewInvalidQuantileWarning` is a separate, pre-existing gap in that
/// path (never wired for `quantile_over_time`, only for
/// `histogram_quantile`'s φ per plan v2 OQ1(c)) and out of this item's
/// scope to add.
pub fn eval_quantile_over_time_hist(
    phi: f64,
    samples: &[Sample],
    metric_name: &str,
    annos: &mut Annotations,
) -> Option<f64> {
    let floats: Vec<Sample> = samples.iter().filter(|s| s.h.is_none()).cloned().collect();
    if floats.is_empty() {
        return None;
    }
    let hist_present = floats.len() != samples.len();
    let result = super::functions::eval_over_time_param(
        crate::plan::OverTimeParamFn::Quantile,
        &floats,
        &[phi],
        0,
    )
    .ok()
    .flatten();
    if hist_present {
        annos.info(messages::histogram_ignored_in_mixed_range_info(metric_name));
    }
    result
}

/// M7-A5b-iii (the codex round-1 [medium] finding — these two were the
/// LAST 422-guarded histogram windows): `predict_linear`/
/// `double_exponential_smoothing`'s histogram disposition, ported from
/// `funcPredictLinear` (`functions.go:1919-1943`) and
/// `funcDoubleExponentialSmoothing` (`:911-964`) — both share the exact
/// same shape:
/// - parameter validation FIRST (`double_exponential_smoothing`'s
///   sf/tf panic precedes the float-count check, `:923-928` — a
///   histogram-only window with bad factors still errors; the existing
///   [`super::functions::eval_over_time_param`] already validates before
///   its own `< 2` check, the #67 validation-ordering rule);
/// - the computation runs over the FLOAT SUBSET only (`samples.Floats`);
/// - fewer than 2 floats → EMPTY (no sample), with
///   `NewHistogramIgnoredInMixedRangeInfo` iff exactly 1 float coexists
///   with histograms (`:1928-1934`, `:932-940`) — a histogram-ONLY window
///   is silent;
/// - ≥ 2 floats with histograms present → the float result + the same
///   info (`:1936-1939`, `:959-962`).
///
/// Net: the info fires iff `floats >= 1 && histograms > 0`; the result
/// exists iff `floats >= 2` — never a hard error for histogram presence.
pub fn eval_over_time_param_hist(
    func: crate::plan::OverTimeParamFn,
    samples: &[Sample],
    scalars: &[f64],
    eval_t_ms: i64,
    metric_name: &str,
    annos: &mut Annotations,
) -> Result<Option<f64>, crate::error::PromqlError> {
    let floats: Vec<Sample> = samples.iter().filter(|s| s.h.is_none()).cloned().collect();
    let hist_present = floats.len() != samples.len();
    // Validation (inside `eval_over_time_param`, before its float-count
    // check) runs regardless of the float subset's size — an invalid
    // factor errors BEFORE any annotation, exactly like the pin's panic.
    let result = super::functions::eval_over_time_param(func, &floats, scalars, eval_t_ms)?;
    if hist_present && !floats.is_empty() {
        annos.info(messages::histogram_ignored_in_mixed_range_info(metric_name));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use pulsus_model::{NativeHistogram, Span};

    use super::*;

    fn hist_sample(t_ms: i64, count: u64, sum: f64, buckets: Vec<i64>) -> Sample {
        let h = NativeHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
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
        .to_float();
        Sample::hist(t_ms, h)
    }

    fn assert_hist_eq(v: Option<RangeValue>, count: f64, sum: f64, buckets: &[f64]) {
        match v {
            Some(RangeValue::Histogram(h)) => {
                assert!((h.count - count).abs() < 1e-9, "count: got {}", h.count);
                assert!((h.sum - sum).abs() < 1e-9, "sum: got {}", h.sum);
                assert_eq!(h.positive_buckets.len(), buckets.len());
                for (a, b) in h.positive_buckets.iter().zip(buckets) {
                    assert!((a - b).abs() < 1e-9, "bucket: got {a} want {b}");
                }
            }
            other => panic!("expected a histogram, got {other:?}"),
        }
    }

    /// `native_histograms.test:1059-1062` (`reset_in_bucket`), pinned
    /// v3.13.0 `40af9c2`: a counter reset visible ONLY in one bucket
    /// (count/sum both still increase) between t=0 and t=5m, corrected by
    /// `increase(reset_in_bucket[15m])` at t=10m. Hand-derived and
    /// independently re-verified against the corpus's own expected value
    /// (`{{count:9 sum:10.5 buckets:[1.5 3 4.5]}}`).
    #[test]
    fn increase_over_reset_in_bucket_matches_the_pinned_corpus_value() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]), // absolute [1,2,1]
            hist_sample(300_000, 5, 6.0, vec![1, 0, 2]), // absolute [1,1,3] -- reset at bucket 1 (2->1)
            hist_sample(600_000, 6, 7.0, vec![1, 1, 1]), // absolute [1,2,3]
        ];
        let mut annos = Annotations::new();
        let v = eval_range_fn_hist(
            RangeFn::Increase,
            &samples,
            900_000,
            -300_000,
            600_000,
            "reset_in_bucket",
            &mut annos,
        );
        assert_hist_eq(v, 9.0, 10.5, &[1.5, 3.0, 4.5]);
        assert!(annos.is_empty(), "no_warn expected per the corpus case");
    }

    /// `native_histograms.test:135-157` (`incr_histogram`): a monotone
    /// counter histogram, +1 bucket-1 observation and +2 sum every 5m for
    /// 10 steps. `rate(incr_histogram[10m])` at t=50m over the 3 samples
    /// at t=40m/45m/50m (n=8/9/10 increments) matches the corpus's pinned
    /// `{{count:0.0033333333333333335 sum:0.006666666666666667 offset:1
    /// buckets:[0.0033333333333333335]}}` (a single populated bucket at
    /// schema-index 1 — this test only asserts the populated bucket's
    /// value, matching the corpus shape).
    #[test]
    fn rate_over_incr_histogram_matches_the_pinned_corpus_value() {
        // Absolute buckets [1, 2+n, 1] delta-encode to [1, 1+n, -1-n].
        let at = |n: i64, t_ms: i64| {
            hist_sample(
                t_ms,
                4 + n as u64,
                4.0 + 2.0 * n as f64,
                vec![1, 1 + n, -1 - n],
            )
        };
        let samples = vec![at(8, 2_400_000), at(9, 2_700_000), at(10, 3_000_000)];
        let mut annos = Annotations::new();
        let v = eval_range_fn_hist(
            RangeFn::Rate,
            &samples,
            600_000,
            2_400_000,
            3_000_000,
            "incr_histogram",
            &mut annos,
        );
        match v {
            Some(RangeValue::Histogram(h)) => {
                assert!(
                    (h.count - 0.0033333333333333335).abs() < 1e-12,
                    "count {}",
                    h.count
                );
                assert!(
                    (h.sum - 0.006666666666666667).abs() < 1e-12,
                    "sum {}",
                    h.sum
                );
                assert_eq!(h.positive_buckets.len(), 1);
                assert!(
                    (h.positive_buckets[0] - 0.0033333333333333335).abs() < 1e-12,
                    "bucket {}",
                    h.positive_buckets[0]
                );
            }
            other => panic!("expected a histogram, got {other:?}"),
        }
        assert!(annos.is_empty(), "no_warn expected per the corpus case");
    }

    #[test]
    fn mixed_float_and_histogram_window_warns_and_drops() {
        let mut samples = vec![hist_sample(0, 4, 5.0, vec![1, 1, -1])];
        samples.push(Sample::float(300_000, 1.0));
        let mut annos = Annotations::new();
        let v = eval_range_fn_hist(
            RangeFn::Rate,
            &samples,
            300_000,
            0,
            300_000,
            "m",
            &mut annos,
        );
        assert!(v.is_none());
        let (warnings, _) = annos.as_strings(0, 0);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("mix of histograms and floats"));
    }

    #[test]
    fn float_only_window_delegates_to_the_byte_unchanged_float_path() {
        let samples = vec![Sample::float(0, 1.0), Sample::float(60_000, 3.0)];
        let mut annos = Annotations::new();
        let v = eval_range_fn_hist(RangeFn::Delta, &samples, 60_000, 0, 60_000, "m", &mut annos);
        match v {
            Some(RangeValue::Float(f)) => assert!((f - 2.0).abs() < 1e-9),
            other => panic!("expected a float, got {other:?}"),
        }
        assert!(annos.is_empty());
    }

    /// `irate` on a genuine counter reset (last < prev) returns the LAST
    /// value verbatim (no subtraction) — mirrors the float `irate`
    /// convention (`eval_irate`'s own doc) via `instantValue`'s
    /// `!isRate ||` gate.
    #[test]
    fn irate_on_a_counter_reset_returns_the_last_sample_verbatim() {
        let samples = vec![
            hist_sample(0, 10, 10.0, vec![10]),
            hist_sample(60_000, 3, 3.0, vec![3]), // reset: count/bucket both dropped
        ];
        let mut annos = Annotations::new();
        let v = eval_range_fn_hist(RangeFn::Irate, &samples, 60_000, 0, 60_000, "m", &mut annos);
        match v {
            Some(RangeValue::Histogram(h)) => {
                // irate divides by interval seconds even on a reset
                // (`functions.go:874-880` runs unconditionally): the last
                // sample's own count (3), divided by the 60s interval.
                assert!((h.count - 0.05).abs() < 1e-9, "count {}", h.count);
            }
            other => panic!("expected a histogram, got {other:?}"),
        }
        assert!(annos.is_empty());
    }

    /// `idelta` always subtracts, even across what would be a counter
    /// reset for `irate` — `functions.go:837`'s `!isRate ||` short-circuit.
    /// Being gauge-expecting, it also fires the not-a-gauge warning
    /// (`instantValue`'s `!isRate` arm, `functions.go:850-853` —
    /// unconditional under hint-less storage, `Unknown != GaugeType`);
    /// `irate` (`is_rate`) never does (its `NotCounter` counterpart needs
    /// a `GaugeType` hint — the remaining genuine carve-out).
    #[test]
    fn idelta_always_subtracts_even_across_a_reset_and_warns_not_gauge() {
        let samples = vec![
            hist_sample(0, 10, 10.0, vec![10]),
            hist_sample(60_000, 3, 3.0, vec![3]),
        ];
        let mut annos = Annotations::new();
        let v = instant_value_hist(&samples, false, "m", &mut annos);
        match v {
            Some(RangeValue::Histogram(h)) => {
                assert!((h.count - (-7.0)).abs() < 1e-9, "count {}", h.count);
            }
            other => panic!("expected a histogram, got {other:?}"),
        }
        let (warnings, _) = annos.as_strings(0, 0);
        assert_eq!(
            warnings,
            vec!["PromQL warning: this native histogram metric is not a gauge: \"m\"".to_string()],
        );
    }

    // -- eval_over_time_hist: the OverTimeFn disposition map --

    #[test]
    fn last_over_time_preserves_a_trailing_histogram_sample() {
        let samples = vec![
            Sample::float(0, 1.0),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Last, &samples, "m", &mut annos);
        match v {
            Some(OverTimeValue::Histogram(h)) => assert_eq!(h.count, 4.0),
            other => panic!("expected a preserved histogram, got {other:?}"),
        }
        assert!(annos.is_empty());
    }

    #[test]
    fn first_over_time_preserves_a_leading_histogram_sample() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            Sample::float(60_000, 1.0),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::First, &samples, "m", &mut annos);
        match v {
            Some(OverTimeValue::Histogram(h)) => assert_eq!(h.count, 4.0),
            other => panic!("expected a preserved histogram, got {other:?}"),
        }
    }

    #[test]
    fn count_over_time_counts_float_and_histogram_samples_together() {
        let samples = vec![
            Sample::float(0, 1.0),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
            hist_sample(120_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Count, &samples, "m", &mut annos);
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if f == 3.0));
    }

    #[test]
    fn resets_counts_a_histogram_bucket_decrease() {
        // native_histograms.test reset_in_bucket: a reset visible only in
        // one bucket between sample 0 and 1, none between 1 and 2.
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            hist_sample(300_000, 5, 6.0, vec![1, 0, 2]),
            hist_sample(600_000, 6, 7.0, vec![1, 1, 1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Resets, &samples, "m", &mut annos);
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if f == 1.0));
    }

    #[test]
    fn resets_counts_a_float_to_histogram_type_transition() {
        let samples = vec![
            Sample::float(0, 1.0),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Resets, &samples, "m", &mut annos);
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if f == 1.0));
    }

    /// Issue #130: an EMPTY window drops the series (`None`), never a
    /// phantom `Float(0.0)` — upstream never invokes `funcResets` for a
    /// pointless series (extended_vectors.test:336, `resets(metric[1m])`
    /// at 3m over a series with no samples in the window).
    #[test]
    fn resets_over_an_empty_window_returns_none() {
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Resets, &[], "m", &mut annos);
        assert!(v.is_none(), "empty window must drop the series, got {v:?}");
        assert!(annos.is_empty());
    }

    #[test]
    fn changes_counts_a_semantically_unequal_histogram_pair() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            hist_sample(60_000, 5, 6.0, vec![1, 1, 0]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Changes, &samples, "m", &mut annos);
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if f == 1.0));
    }

    #[test]
    fn changes_is_zero_for_bit_identical_consecutive_histograms() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Changes, &samples, "m", &mut annos);
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if f == 0.0));
    }

    /// Issue #130: the `changes` twin of
    /// [`resets_over_an_empty_window_returns_none`]
    /// (extended_vectors.test:314, `changes(metric[1m])` at 3m).
    #[test]
    fn changes_over_an_empty_window_returns_none() {
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Changes, &[], "m", &mut annos);
        assert!(v.is_none(), "empty window must drop the series, got {v:?}");
        assert!(annos.is_empty());
    }

    /// Plan v4 residual A: a histogram-ONLY window over a DROP-set
    /// function (`min_over_time` etc.) is SILENT — no value, no
    /// annotation (`functions.go:1461` returns before `:1464-1465`'s
    /// info add).
    #[test]
    fn drop_set_over_a_histogram_only_window_is_silent() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Min, &samples, "m", &mut annos);
        assert!(v.is_none());
        assert!(annos.is_empty(), "no annotation on a histogram-only window");
    }

    /// The mixed-window pairing: a DROP-set function over BOTH floats and
    /// histograms computes on the floats and fires
    /// `HistogramIgnoredInMixedRangeInfo` exactly once.
    #[test]
    fn drop_set_over_a_mixed_window_computes_on_floats_and_annotates() {
        let samples = vec![
            Sample::float(0, 3.0),
            Sample::float(60_000, 1.0),
            hist_sample(120_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Min, &samples, "m", &mut annos);
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if f == 1.0));
        let (_, infos) = annos.as_strings(0, 0);
        assert_eq!(infos.len(), 1);
        assert!(infos[0].contains("ignored histograms in a range"));
    }

    #[test]
    fn drop_set_ts_of_min_over_a_mixed_window_annotates() {
        let samples = vec![
            Sample::float(0, 3.0),
            Sample::float(60_000, 1.0),
            hist_sample(120_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::TsOfMin, &samples, "m", &mut annos);
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if (f - 60.0).abs() < 1e-9));
        assert!(!annos.is_empty());
    }

    #[test]
    fn quantile_over_time_histogram_only_window_is_silent() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_quantile_over_time_hist(0.5, &samples, "m", &mut annos);
        assert!(v.is_none());
        assert!(annos.is_empty());
    }

    #[test]
    fn quantile_over_time_mixed_window_computes_on_floats_and_annotates() {
        let samples = vec![
            Sample::float(0, 1.0),
            Sample::float(60_000, 3.0),
            hist_sample(120_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_quantile_over_time_hist(1.0, &samples, "m", &mut annos);
        assert!(matches!(v, Some(f) if (f - 3.0).abs() < 1e-9));
        assert!(!annos.is_empty());
    }

    #[test]
    fn idelta_over_a_float_only_window_matches_the_byte_unchanged_float_path() {
        let samples = vec![Sample::float(0, 1.0), Sample::float(60_000, 4.0)];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Idelta, &samples, "m", &mut annos);
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if (f - 3.0).abs() < 1e-9));
        assert!(annos.is_empty());
    }

    // -- native_histograms.test `nhcb_metric` differentials (`:1291-1382`,
    //    pinned 40af9c2): load 6m, samples {cv:[5] b:[1]} x2 then
    //    {cv:[5 10] b:[1]} x2, each count 1 / sum 1. The corpus pins the
    //    NHCB mismatched-bounds reconciliation semantics this port now
    //    implements (#124 A5b-ii codex round-1 finding 2). --

    fn nhcb_sample(t_ms: i64, bounds: Vec<f64>, buckets: Vec<i64>) -> Sample {
        let count = buckets.iter().sum::<i64>() as u64;
        let mut deltas = vec![buckets[0]];
        for w in buckets.windows(2) {
            deltas.push(w[1] - w[0]);
        }
        let h = NativeHistogram {
            counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
            schema: pulsus_model::CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            zero_count: 0,
            count,
            sum: 1.0,
            positive_spans: vec![Span {
                offset: 0,
                length: buckets.len() as u32,
            }],
            negative_spans: vec![],
            positive_buckets: deltas,
            negative_buckets: vec![],
            custom_values: bounds,
        }
        .to_float();
        Sample::hist(t_ms, h)
    }

    /// The 12m-window slice of the corpus fixture: samples at 0/6m/12m,
    /// bounds change at the third sample.
    fn nhcb_metric_window_at_12m() -> Vec<Sample> {
        vec![
            nhcb_sample(0, vec![5.0], vec![1]),
            nhcb_sample(360_000, vec![5.0], vec![1]),
            nhcb_sample(720_000, vec![5.0, 10.0], vec![1]),
        ]
    }

    /// `increase(nhcb_metric[13m])` at 12m == `{{schema:-53
    /// custom_values:[5]}}` with `expect no_warn` (`:1326-1328`): the
    /// bounds mismatch RECONCILES (intersection [5], contributions
    /// cancel) — never a warning/drop.
    #[test]
    fn increase_over_nhcb_bounds_change_matches_the_pinned_corpus_value() {
        let mut annos = Annotations::new();
        let v = eval_range_fn_hist(
            RangeFn::Increase,
            &nhcb_metric_window_at_12m(),
            780_000,
            -60_000,
            720_000,
            "nhcb_metric",
            &mut annos,
        );
        match v {
            Some(RangeValue::Histogram(h)) => {
                assert_eq!(h.schema, pulsus_model::CUSTOM_BUCKETS_SCHEMA);
                assert_eq!(h.custom_values, vec![5.0]);
                assert_eq!(h.count, 0.0);
                assert_eq!(h.sum, 0.0);
                assert!(h.positive_buckets.is_empty());
            }
            other => panic!("expected a reconciled histogram, got {other:?}"),
        }
        let (warnings, infos) = annos.as_strings(0, 0);
        assert!(
            warnings.is_empty(),
            "expect no_warn per the corpus: {warnings:?}"
        );
        assert_eq!(
            infos,
            vec![
                "PromQL info: mismatched custom buckets were reconciled during subtraction"
                    .to_string()
            ],
        );
    }

    /// `rate(nhcb_metric[13m])` at 12m — same reconciled shape
    /// (`:1330-1332`), `expect no_warn`.
    #[test]
    fn rate_over_nhcb_bounds_change_matches_the_pinned_corpus_value() {
        let mut annos = Annotations::new();
        let v = eval_range_fn_hist(
            RangeFn::Rate,
            &nhcb_metric_window_at_12m(),
            780_000,
            -60_000,
            720_000,
            "nhcb_metric",
            &mut annos,
        );
        match v {
            Some(RangeValue::Histogram(h)) => {
                assert_eq!(h.custom_values, vec![5.0]);
                assert_eq!(h.count, 0.0);
                assert!(h.positive_buckets.is_empty());
            }
            other => panic!("expected a reconciled histogram, got {other:?}"),
        }
        let (warnings, _) = annos.as_strings(0, 0);
        assert!(warnings.is_empty(), "expect no_warn per the corpus");
    }

    /// `delta(nhcb_metric[13m])` at 18m — the corpus pins BOTH
    /// annotations (`:1365-1369`): `expect warn msg: PromQL warning: this
    /// native histogram metric is not a gauge: "nhcb_metric"` (the
    /// `!isCounter` gauge-expectation warning, `functions.go:695-697` —
    /// reproducible under hint-less storage because every decoded hint is
    /// `Unknown != GaugeType`) and `expect info msg: PromQL info:
    /// mismatched custom buckets were reconciled during subtraction`.
    /// Result `{{schema:-53 custom_values:[5]}}`.
    #[test]
    fn delta_over_nhcb_bounds_change_emits_the_pinned_warning_and_reconcile_info() {
        // The 18m window: samples at 6m/12m/18m -- first {cv:[5]}, then
        // two {cv:[5,10]}.
        let samples = vec![
            nhcb_sample(360_000, vec![5.0], vec![1]),
            nhcb_sample(720_000, vec![5.0, 10.0], vec![1]),
            nhcb_sample(1_080_000, vec![5.0, 10.0], vec![1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_range_fn_hist(
            RangeFn::Delta,
            &samples,
            780_000,
            300_000,
            1_080_000,
            "nhcb_metric",
            &mut annos,
        );
        match v {
            Some(RangeValue::Histogram(h)) => {
                assert_eq!(h.custom_values, vec![5.0]);
                assert_eq!(h.count, 0.0);
                assert!(h.positive_buckets.is_empty());
            }
            other => panic!("expected a reconciled histogram, got {other:?}"),
        }
        let (warnings, infos) = annos.as_strings(0, 0);
        assert_eq!(
            warnings,
            vec![
                "PromQL warning: this native histogram metric is not a gauge: \"nhcb_metric\""
                    .to_string()
            ],
        );
        assert_eq!(
            infos,
            vec![
                "PromQL info: mismatched custom buckets were reconciled during subtraction"
                    .to_string()
            ],
        );
    }

    /// `delta(nhcb_metric[13m])` at 12m (`:1322-1324`): the corpus pins
    /// the not-a-gauge warning there too (`expect warn msg: …not a
    /// gauge: "nhcb_metric"`), result `{{schema:-53 custom_values:[5]}}`.
    #[test]
    fn delta_at_12m_emits_the_pinned_not_gauge_warning() {
        let mut annos = Annotations::new();
        let v = eval_range_fn_hist(
            RangeFn::Delta,
            &nhcb_metric_window_at_12m(),
            780_000,
            -60_000,
            720_000,
            "nhcb_metric",
            &mut annos,
        );
        match v {
            Some(RangeValue::Histogram(h)) => {
                assert_eq!(h.custom_values, vec![5.0]);
                assert_eq!(h.count, 0.0);
            }
            other => panic!("expected a reconciled histogram, got {other:?}"),
        }
        let (warnings, _) = annos.as_strings(0, 0);
        assert_eq!(
            warnings,
            vec![
                "PromQL warning: this native histogram metric is not a gauge: \"nhcb_metric\""
                    .to_string()
            ],
        );
    }

    /// `resets(nhcb_metric[13m])` at 12m == 0 (`:1334-1336`): the custom-
    /// bounds CHANGE with consistent rolled-up counts is NOT a reset —
    /// `DetectReset` compares rollups over the common bounds instead of
    /// conservatively flagging the mismatch.
    #[test]
    fn resets_over_nhcb_bounds_change_matches_the_pinned_corpus_zero() {
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(
            OverTimeFn::Resets,
            &nhcb_metric_window_at_12m(),
            "nhcb_metric",
            &mut annos,
        );
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if f == 0.0));
        assert!(annos.is_empty());
    }

    /// `changes(nhcb_metric[13m])` at 12m == 1 (`:1318-1320`): the bounds
    /// change makes the third sample semantically unequal to the second
    /// (`Equals` compares custom bounds), the identical first pair does
    /// not count.
    #[test]
    fn changes_over_nhcb_bounds_change_matches_the_pinned_corpus_one() {
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(
            OverTimeFn::Changes,
            &nhcb_metric_window_at_12m(),
            "nhcb_metric",
            &mut annos,
        );
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if f == 1.0));
    }

    #[test]
    fn idelta_over_a_mixed_last_two_warns() {
        let samples = vec![
            Sample::float(0, 1.0),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Idelta, &samples, "m", &mut annos);
        assert!(v.is_none());
        let (warnings, _) = annos.as_strings(0, 0);
        assert_eq!(warnings.len(), 1);
    }

    // -- M7-A5b-iii: `sum_over_time`/`avg_over_time`'s histogram KahanAdd
    //    path — the same `nhcb_metric` 12m-window fixture (`:1296-1304`). --

    /// `sum_over_time(nhcb_metric[13m])` at 12m == `{{schema:-53 count:3
    /// sum:3 custom_values:[5] buckets:[3]}}`, `expect no_warn` +
    /// `expect info msg: … reconciled during aggregation` (`:1296-1299`).
    #[test]
    fn sum_over_time_over_nhcb_bounds_change_matches_the_pinned_corpus_value() {
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(
            OverTimeFn::Sum,
            &nhcb_metric_window_at_12m(),
            "nhcb_metric",
            &mut annos,
        );
        match v {
            Some(OverTimeValue::Histogram(h)) => {
                assert_eq!(h.custom_values, vec![5.0]);
                assert_eq!(h.count, 3.0);
                assert_eq!(h.sum, 3.0);
                assert_eq!(h.positive_buckets, vec![3.0]);
            }
            other => panic!("expected a reconciled histogram, got {other:?}"),
        }
        let (warnings, infos) = annos.as_strings(0, 0);
        assert!(warnings.is_empty(), "expect no_warn per the corpus");
        assert_eq!(
            infos,
            vec![
                "PromQL info: mismatched custom buckets were reconciled during aggregation"
                    .to_string()
            ],
        );
    }

    /// `avg_over_time(nhcb_metric[13m])` at 12m == `{{schema:-53 count:1
    /// sum:1 custom_values:[5] buckets:[1]}}` (`:1301-1304`) — the direct-
    /// mean path (no `HasOverflow` switch for these small values).
    #[test]
    fn avg_over_time_over_nhcb_bounds_change_matches_the_pinned_corpus_value() {
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(
            OverTimeFn::Avg,
            &nhcb_metric_window_at_12m(),
            "nhcb_metric",
            &mut annos,
        );
        match v {
            Some(OverTimeValue::Histogram(h)) => {
                assert_eq!(h.custom_values, vec![5.0]);
                assert_eq!(h.count, 1.0);
                assert_eq!(h.sum, 1.0);
                assert_eq!(h.positive_buckets, vec![1.0]);
            }
            other => panic!("expected a reconciled histogram, got {other:?}"),
        }
        let (warnings, _) = annos.as_strings(0, 0);
        assert!(warnings.is_empty(), "expect no_warn per the corpus");
    }

    /// `sum_over_time` over a MIXED float+histogram window drops the
    /// WHOLE window with `NewMixedFloatsHistogramsWarning`
    /// (`functions.go:1148-1153,1498-1503` — a whole-window guard, unlike
    /// the DROP-set functions' per-histogram-sample skip).
    #[test]
    fn sum_over_time_over_a_mixed_window_warns_and_drops_the_whole_window() {
        let samples = vec![
            Sample::float(0, 1.0),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Sum, &samples, "m", &mut annos);
        assert!(v.is_none());
        let (warnings, _) = annos.as_strings(0, 0);
        assert_eq!(
            warnings,
            vec![messages::mixed_floats_histograms_warning("m")]
        );
    }

    /// A pure-float window delegates to the byte-unchanged float
    /// `sum_over_time`/`avg_over_time` (no histogram machinery touched).
    #[test]
    fn sum_over_time_over_a_pure_float_window_is_unaffected() {
        let samples = vec![Sample::float(0, 1.0), Sample::float(60_000, 2.0)];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Sum, &samples, "m", &mut annos);
        assert!(matches!(v, Some(OverTimeValue::Float(f)) if f == 3.0));
        assert!(annos.is_empty());
    }

    /// `sum_over_time` folding THREE same-schema exponential histograms —
    /// the common (non-NHCB) fold path, fully Kahan-compensated.
    #[test]
    fn sum_over_time_folds_three_same_schema_histograms() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
            hist_sample(120_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Sum, &samples, "m", &mut annos);
        match v {
            Some(OverTimeValue::Histogram(h)) => {
                assert_eq!(h.count, 12.0);
                assert_eq!(h.sum, 15.0);
                assert_eq!(h.positive_buckets, vec![3.0, 6.0, 3.0]);
            }
            other => panic!("expected a folded histogram, got {other:?}"),
        }
        assert!(annos.is_empty());
    }

    /// A pre-built `Sample` carrying an f64-valued (non-integer-encodable)
    /// histogram — the adversarial fold fixtures below can't route
    /// through `NativeHistogram` (integer bucket columns).
    fn float_hist_sample(t_ms: i64, bucket: f64) -> Sample {
        Sample::hist(
            t_ms,
            FloatHistogram {
                counter_reset_hint: pulsus_model::CounterResetHint::Unknown,
                schema: 0,
                zero_threshold: 0.0,
                zero_count: 0.0,
                count: bucket,
                sum: bucket,
                positive_spans: vec![Span {
                    offset: 0,
                    length: 1,
                }],
                negative_spans: vec![],
                positive_buckets: vec![bucket],
                negative_buckets: vec![],
                custom_values: vec![],
            },
        )
    }

    /// ADVERSARIAL (codex round-1 [high], end-to-end through
    /// `sum_over_time`): bucket-level Kahan compensation recovers `+1.0`
    /// contributions plain accumulation loses above 2^53.
    #[test]
    fn sum_over_time_bucket_compensation_recovers_lost_low_order_adds() {
        const BIG: f64 = 9007199254740992.0; // 2^53
        const BIG_PLUS_2: f64 = 9007199254740994.0;
        let samples = vec![
            float_hist_sample(0, BIG),
            float_hist_sample(60_000, 1.0),
            float_hist_sample(120_000, 1.0),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Sum, &samples, "m", &mut annos);
        match v {
            Some(OverTimeValue::Histogram(h)) => {
                assert_eq!(h.positive_buckets, vec![BIG_PLUS_2]);
                assert_ne!(
                    h.positive_buckets,
                    vec![BIG],
                    "plain bucket accumulation would plateau at 2^53"
                );
                assert_eq!(h.count, BIG_PLUS_2);
                assert_eq!(h.sum, BIG_PLUS_2);
            }
            other => panic!("expected a folded histogram, got {other:?}"),
        }
        assert!(annos.is_empty());
    }

    /// ADVERSARIAL through `avg_over_time` (direct mean): the flushed
    /// compensated mean is `2^53/3 + 2/3` via the pin's `Div(count)` +
    /// `Add(kahanC.Div(count))` arithmetic — a plain fold would yield
    /// exactly `2^53 / 3`.
    #[test]
    fn avg_over_time_bucket_compensation_survives_the_mean_division() {
        const BIG: f64 = 9007199254740992.0;
        let samples = vec![
            float_hist_sample(0, BIG),
            float_hist_sample(60_000, 1.0),
            float_hist_sample(120_000, 1.0),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_hist(OverTimeFn::Avg, &samples, "m", &mut annos);
        match v {
            Some(OverTimeValue::Histogram(h)) => {
                let expected = BIG / 3.0 + 2.0 / 3.0;
                assert_eq!(h.positive_buckets, vec![expected]);
                assert_ne!(
                    h.positive_buckets,
                    vec![BIG / 3.0],
                    "the compensation must survive the mean division"
                );
                assert_eq!(h.count, expected);
            }
            other => panic!("expected a folded histogram, got {other:?}"),
        }
    }

    // -- M7-A5b-iii (codex round-1 [medium]): predict_linear /
    //    double_exponential_smoothing float-subset dispositions
    //    (`eval_over_time_param_hist`). Full-pipeline coverage lives in
    //    `eval::tests`; this is the per-condition unit matrix. --

    #[test]
    fn predict_linear_histogram_only_window_is_silently_empty() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_param_hist(
            crate::plan::OverTimeParamFn::PredictLinear,
            &samples,
            &[10.0],
            120_000,
            "m",
            &mut annos,
        )
        .unwrap();
        assert!(v.is_none());
        assert!(annos.is_empty(), "histogram-only window is silent");
    }

    #[test]
    fn predict_linear_mixed_window_computes_on_the_float_subset_and_annotates() {
        let samples = vec![
            Sample::float(0, 0.0),
            hist_sample(30_000, 4, 5.0, vec![1, 1, -1]),
            Sample::float(60_000, 60.0),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_param_hist(
            crate::plan::OverTimeParamFn::PredictLinear,
            &samples,
            &[0.0],
            60_000,
            "m",
            &mut annos,
        )
        .unwrap();
        // Floats regress to slope 1/s, intercept 60 at t=60s; the
        // interleaved histogram must not perturb the regression.
        assert_eq!(v, Some(60.0));
        let (warnings, infos) = annos.as_strings(0, 0);
        assert!(warnings.is_empty());
        assert_eq!(
            infos,
            vec![messages::histogram_ignored_in_mixed_range_info("m")]
        );
    }

    /// One float + histograms: too few float points → EMPTY, but the
    /// mixed info still fires (`functions.go:1928-1932`).
    #[test]
    fn predict_linear_one_float_in_a_mixed_window_is_empty_with_info() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            Sample::float(60_000, 1.0),
        ];
        let mut annos = Annotations::new();
        let v = eval_over_time_param_hist(
            crate::plan::OverTimeParamFn::PredictLinear,
            &samples,
            &[10.0],
            60_000,
            "m",
            &mut annos,
        )
        .unwrap();
        assert!(v.is_none());
        assert_eq!(
            annos.as_strings(0, 0).1,
            vec![messages::histogram_ignored_in_mixed_range_info("m")]
        );
    }

    /// Float-only windows are byte-identical to the direct float path
    /// (no annotation, same value).
    #[test]
    fn predict_linear_float_only_window_is_unchanged() {
        let samples = vec![Sample::float(0, 0.0), Sample::float(60_000, 60.0)];
        let mut annos = Annotations::new();
        let v = eval_over_time_param_hist(
            crate::plan::OverTimeParamFn::PredictLinear,
            &samples,
            &[0.0],
            60_000,
            "m",
            &mut annos,
        )
        .unwrap();
        let direct = super::super::functions::eval_over_time_param(
            crate::plan::OverTimeParamFn::PredictLinear,
            &samples,
            &[0.0],
            60_000,
        )
        .unwrap();
        assert_eq!(v, direct);
        assert!(annos.is_empty());
    }

    /// `double_exponential_smoothing`'s sf/tf validation precedes the
    /// float-count check (`functions.go:923-928`): a histogram-only
    /// window with an invalid factor errors; with valid factors it is
    /// silently empty.
    #[test]
    fn double_exponential_smoothing_validates_factors_before_the_float_count_check() {
        let samples = vec![
            hist_sample(0, 4, 5.0, vec![1, 1, -1]),
            hist_sample(60_000, 4, 5.0, vec![1, 1, -1]),
        ];
        let mut annos = Annotations::new();
        // Invalid sf → error even though the float subset is empty.
        assert!(
            eval_over_time_param_hist(
                crate::plan::OverTimeParamFn::DoubleExpSmoothing,
                &samples,
                &[2.0, 0.5],
                0,
                "m",
                &mut annos,
            )
            .is_err()
        );
        // Valid factors → silent empty.
        let v = eval_over_time_param_hist(
            crate::plan::OverTimeParamFn::DoubleExpSmoothing,
            &samples,
            &[0.5, 0.5],
            0,
            "m",
            &mut annos,
        )
        .unwrap();
        assert!(v.is_none());
        assert!(annos.is_empty());
    }
}
