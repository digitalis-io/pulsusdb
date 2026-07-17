//! OTLP metrics parser (issue #27 architect plan + amendment, docs/
//! architecture.md §4): a pure `bytes -> ExportMetricsServiceRequest ->
//! ParsedMetrics` pipeline with no I/O. Resource + scope attributes flatten
//! through the same canonical label model the OTLP logs receiver uses
//! (`pulsus_model::LabelSet::from_normalized` -> `metric_fingerprint`, issue
//! #4/#8's precedent) — fingerprints derive *only* via `pulsus-model`, never
//! re-derived here. `__name__` is never placed in a [`LabelSet`]: the metric
//! name travels only as `MetricPoint`/`SeriesRef`'s first-class
//! `metric_name` column (docs/architecture.md §2.3), and
//! `metric_fingerprint` excludes it anyway.
//!
//! Gauge/Sum flatten to one series per data point; Histogram/
//! ExponentialHistogram flatten to cumulative `<name>_bucket{le}`/`_sum`/
//! `_count` series (a `+Inf` bucket always present, always equal to
//! `_count`); Summary flattens to `<name>{quantile}`/`_sum`/`_count`. See
//! each `emit_*` function's doc comment for the per-type mapping pinned by
//! the architect plan.

use std::collections::HashSet;
use std::sync::Arc;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, DataPointFlags, ExponentialHistogramDataPoint, HistogramDataPoint,
    Metric, NumberDataPoint, ScopeMetrics, SummaryDataPoint, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use pulsus_model::{Fingerprint, LabelSet, STALE_NAN_BITS, metric_fingerprint};

use crate::error::LogsIngestError;
use crate::ingest::metrics::{MetricMetadata, MetricPoint, ParsedMetrics, SeriesRef};

/// Decodes a (decompressed) OTLP `/v1/metrics` request body. The sole
/// decode boundary: a malformed/truncated protobuf is a whole-request,
/// atomic failure (mirrors `otlp_logs::decode`) — never partially applied.
pub fn decode(body: &[u8]) -> Result<ExportMetricsServiceRequest, LogsIngestError> {
    Ok(ExportMetricsServiceRequest::decode(body)?)
}

/// Decodes a (decompressed) OTLP/JSON (proto3-JSON) `/v1/metrics` request body
/// — the `Content-Type: application/json` sibling of [`decode`] (issue #76),
/// feeding the same [`parse`] as protobuf. Non-finite doubles (`"NaN"`/
/// `"Infinity"`/`"-Infinity"`) on any data-point field decode correctly via
/// the vendored+patched `opentelemetry-proto` special-double serde
/// (docs/decisions/0004); a malformed body maps to 400/code 3 via
/// [`LogsIngestError::DecodeJson`].
pub fn decode_json(body: &[u8]) -> Result<ExportMetricsServiceRequest, LogsIngestError> {
    Ok(serde_json::from_slice(body)?)
}

/// Parses a decoded `ExportMetricsServiceRequest` into normalized rows.
/// Pure: a function of `req` and `now_ns` only, no I/O, no clock reads —
/// the caller (the ingest handler) is the only clock/IO boundary. `now_ns`
/// becomes every metadata row's `updated_ns` (the `ReplacingMergeTree`
/// version column, issue #26 amendment).
pub fn parse(req: &ExportMetricsServiceRequest, now_ns: i64) -> ParsedMetrics {
    let mut out = ParsedMetrics::default();
    // Dedups `SeriesRef` registration within this request by `(metric_name,
    // fingerprint)` (architect plan: "a labels carrier, not a per-sample
    // registration").
    let mut seen_series: HashSet<(Arc<str>, Fingerprint)> = HashSet::new();
    // Dedups `MetricMetadata` within this request by base family name
    // (architect plan: "one MetricMetadata ... per Metric descriptor,
    // deduped within-request by base name").
    let mut seen_metadata: HashSet<Arc<str>> = HashSet::new();

    for resource_metrics in &req.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            // Base label pairs (resource ⊕ scope identity ⊕ scope attrs),
            // computed once per `ScopeMetrics` (architect plan) and reused,
            // unresolved, across every data point in it — the actual
            // `LabelSet`/collision count is only ever produced once the
            // final per-data-point pair set (base ⊕ dp attrs ⊕ synthetic
            // le/quantile) is known, in `emit_sample`.
            let base_pairs = build_scope_pairs(resource_metrics.resource.as_ref(), scope_metrics);

            for metric in &scope_metrics.metrics {
                parse_metric(
                    &mut out,
                    &mut seen_series,
                    &mut seen_metadata,
                    metric,
                    &base_pairs,
                    now_ns,
                );
            }
        }
    }

    out
}

/// Dispatches one `Metric` descriptor to its type-specific handler
/// (architect plan's per-type mapping table), after registering its
/// (deduped, base-named) [`MetricMetadata`] row. A `Metric` with no `data`
/// oneof set carries no data points and is silently skipped — the OTLP
/// wire format allows this at the message-shape level even though it is
/// not a meaningful export.
fn parse_metric(
    out: &mut ParsedMetrics,
    seen_series: &mut HashSet<(Arc<str>, Fingerprint)>,
    seen_metadata: &mut HashSet<Arc<str>>,
    metric: &Metric,
    base_pairs: &[(String, String)],
    now_ns: i64,
) {
    let Some(data) = metric.data.as_ref() else {
        return;
    };
    let name: Arc<str> = Arc::from(metric.name.as_str());

    let metric_type = match data {
        metric::Data::Gauge(_) => "gauge",
        metric::Data::Sum(sum) => {
            if sum.is_monotonic {
                "counter"
            } else {
                "gauge"
            }
        }
        metric::Data::Histogram(_) | metric::Data::ExponentialHistogram(_) => "histogram",
        metric::Data::Summary(_) => "summary",
    };

    if seen_metadata.insert(Arc::clone(&name)) {
        out.metadata.push(MetricMetadata {
            metric_name: Arc::clone(&name),
            metric_type: metric_type.to_string(),
            help: metric.description.clone(),
            unit: metric.unit.clone(),
            updated_ns: now_ns,
        });
    }

    match data {
        metric::Data::Gauge(gauge) => {
            for dp in &gauge.data_points {
                emit_number_point(out, seen_series, Arc::clone(&name), base_pairs, dp);
            }
        }
        metric::Data::Sum(sum) => {
            if is_delta(sum.aggregation_temporality) {
                reject_whole_metric(out, &metric.name, sum.data_points.len());
                return;
            }
            for dp in &sum.data_points {
                emit_number_point(out, seen_series, Arc::clone(&name), base_pairs, dp);
            }
        }
        metric::Data::Histogram(hist) => {
            if is_delta(hist.aggregation_temporality) {
                reject_whole_metric(out, &metric.name, hist.data_points.len());
                return;
            }
            for dp in &hist.data_points {
                emit_histogram_point(out, seen_series, &name, base_pairs, dp);
            }
        }
        metric::Data::ExponentialHistogram(exp) => {
            if is_delta(exp.aggregation_temporality) {
                reject_whole_metric(out, &metric.name, exp.data_points.len());
                return;
            }
            for dp in &exp.data_points {
                emit_exponential_histogram_point(out, seen_series, &name, base_pairs, dp);
            }
        }
        metric::Data::Summary(summary) => {
            for dp in &summary.data_points {
                emit_summary_point(out, seen_series, &name, base_pairs, dp);
            }
        }
    }
}

/// `true` when `temporality` is `AGGREGATION_TEMPORALITY_DELTA` (architect
/// plan: delta Sum/Histogram/ExponentialHistogram is rejected wholesale —
/// delta->cumulative conversion is stateful and deferred to M7).
/// `UNSPECIFIED`/`CUMULATIVE` (and any unrecognized enum value) are treated
/// as not-delta, i.e. stored as-is.
fn is_delta(temporality: i32) -> bool {
    temporality == AggregationTemporality::Delta as i32
}

/// Rejects every data point of a whole metric (delta temporality) into
/// partial success, naming the metric — never a per-bucket count (architect
/// plan: "increments `rejected`" once per data point, not once per sample).
fn reject_whole_metric(out: &mut ParsedMetrics, metric_name: &str, data_point_count: usize) {
    out.rejected += data_point_count as u64;
    if out.rejected_message.is_none() {
        out.rejected_message = Some(format!(
            "metric {metric_name}: delta temporality unsupported until M7"
        ));
    }
}

/// Rejects a single data point into partial success, naming `metric_name`.
fn reject_point(out: &mut ParsedMetrics, message: String) {
    out.rejected += 1;
    if out.rejected_message.is_none() {
        out.rejected_message = Some(message);
    }
}

/// The per-data-point context every `emit_sample` call within one data
/// point's handler shares: the scope's base label pairs, this data point's
/// own attributes, and its resolved timestamp — bundled to keep
/// `emit_sample`'s own argument count within clippy's default threshold
/// rather than re-threading three unchanging arguments through every call
/// site in a histogram/exponential-histogram/summary data point.
struct DataPointContext<'a> {
    base_pairs: &'a [(String, String)],
    dp_attributes: &'a [KeyValue],
    unix_milli: i64,
}

/// Gauge/Sum mapping (architect plan table): one sample named `{name}`,
/// labeled by the data point's own attributes (chained onto the scope's
/// base pairs) — no derived series.
fn emit_number_point(
    out: &mut ParsedMetrics,
    seen_series: &mut HashSet<(Arc<str>, Fingerprint)>,
    name: Arc<str>,
    base_pairs: &[(String, String)],
    dp: &NumberDataPoint,
) {
    let unix_milli = match resolve_timestamp_ms(dp.time_unix_nano) {
        Ok(ms) => ms,
        Err(message) => {
            reject_point(out, format!("metric {name}: {message}"));
            return;
        }
    };
    let Some(raw_value) = resolve_number_value(dp.value.as_ref()) else {
        reject_point(
            out,
            format!("metric {name}: data point has no recognized value field"),
        );
        return;
    };
    let value = stale_or(dp.flags, raw_value);
    let ctx = DataPointContext {
        base_pairs,
        dp_attributes: &dp.attributes,
        unix_milli,
    };
    emit_sample(out, seen_series, name, &ctx, None, value);
}

/// Histogram mapping (architect plan + amendment): `{name}_bucket{le}` per
/// cumulative bucket + a `+Inf` bucket, `{name}_sum` (if present),
/// `{name}_count`. **Invariant**: `derived_count = sum(bucket_counts)` is
/// cross-checked against the reported `count`; on mismatch the whole data
/// point is rejected into partial success and emits no samples at all — a
/// self-inconsistent histogram must never produce a silently inconsistent
/// `_bucket`/`_count` series (amendment, Finding 2). The sum is computed
/// with [`checked_sum`]: an attacker-controlled payload whose bucket
/// counts would overflow `u64` when summed is rejected the same way as a
/// reported-count mismatch, rather than panicking (debug builds) or
/// silently wrapping to an under-count (release builds) — review finding
/// 1.
///
/// A data point with no bucket distribution (`bucket_counts` empty, legal
/// per the OTLP wire format — "count and sum are known" only) has no
/// invariant to check, but the AC's `_bucket{le="+Inf"} == _count`
/// invariant still holds unconditionally (review finding 2): the `+Inf`
/// bucket is emitted directly from the reported `count`.
fn emit_histogram_point(
    out: &mut ParsedMetrics,
    seen_series: &mut HashSet<(Arc<str>, Fingerprint)>,
    name: &Arc<str>,
    base_pairs: &[(String, String)],
    dp: &HistogramDataPoint,
) {
    let unix_milli = match resolve_timestamp_ms(dp.time_unix_nano) {
        Ok(ms) => ms,
        Err(message) => {
            reject_point(out, format!("metric {name}: {message}"));
            return;
        }
    };
    let ctx = DataPointContext {
        base_pairs,
        dp_attributes: &dp.attributes,
        unix_milli,
    };
    let bucket_name: Arc<str> = Arc::from(format!("{name}_bucket").as_str());

    if dp.bucket_counts.is_empty() {
        // No distribution to cross-check, but `_bucket{le="+Inf"}` must
        // still equal `_count` (review finding 2, AC).
        emit_sample(
            out,
            seen_series,
            Arc::clone(&bucket_name),
            &ctx,
            Some(("le", "+Inf".to_string())),
            stale_or(dp.flags, dp.count as f64),
        );
    } else {
        let Some(derived_count) = checked_sum(dp.bucket_counts.iter().copied()) else {
            reject_point(
                out,
                format!(
                    "histogram {name}: bucket counts overflow u64 while summing (rejected \
                     rather than silently wrapping/panicking)"
                ),
            );
            return;
        };
        if derived_count != dp.count {
            reject_point(
                out,
                format!(
                    "histogram {name}: bucket counts sum to {derived_count} but count={reported}",
                    reported = dp.count
                ),
            );
            return;
        }

        let mut running: u64 = 0;
        for (i, &count) in dp.bucket_counts.iter().enumerate() {
            // Infallible: `derived_count` above is the checked total of
            // every entry in `bucket_counts`, so a prefix sum of the same
            // sequence can never itself overflow.
            running = running
                .checked_add(count)
                .expect("prefix sum is bounded by the already-checked total derived_count");
            let le = dp.explicit_bounds.get(i).copied().unwrap_or(f64::INFINITY);
            let value = stale_or(dp.flags, running as f64);
            emit_sample(
                out,
                seen_series,
                Arc::clone(&bucket_name),
                &ctx,
                Some(("le", render_bound(le))),
                value,
            );
        }
    }

    let count_name: Arc<str> = Arc::from(format!("{name}_count").as_str());
    emit_sample(
        out,
        seen_series,
        count_name,
        &ctx,
        None,
        stale_or(dp.flags, dp.count as f64),
    );
    if let Some(sum) = dp.sum {
        let sum_name: Arc<str> = Arc::from(format!("{name}_sum").as_str());
        emit_sample(
            out,
            seen_series,
            sum_name,
            &ctx,
            None,
            stale_or(dp.flags, sum),
        );
    }
}

/// Sums `counts`, checked: an overflowing sum (only reachable via a
/// pathological/malicious payload — legitimate bucket counts are bounded
/// by real observation counts, never adversarially chosen near-`u64::MAX`
/// values) returns `None` rather than panicking (debug builds, where
/// `u64::sum()`'s internal `+` has overflow checks on) or silently
/// wrapping to an under-count (release builds, where it would not).
/// Shared by the classic- and exponential-histogram count-invariant checks
/// (review finding 1).
fn checked_sum(counts: impl IntoIterator<Item = u64>) -> Option<u64> {
    counts
        .into_iter()
        .try_fold(0u64, |acc, c| acc.checked_add(c))
}

/// Exponential-histogram flattening (architect plan amendment, pinned; full
/// accuracy corpus deferred to #33). For scale `s`, a positive bucket at
/// absolute index `k = positive.offset + j` has upper bound
/// `2f64.powf((k+1) as f64 * 2f64.powi(-s))`; a negative bucket at
/// `k = negative.offset + j` has upper bound
/// `-2f64.powf(k as f64 * 2f64.powi(-s))` (observations are `<= -base^k`);
/// the zero bucket's bound is `zero_threshold` (`0.0` if unset). Any
/// non-finite computed bound (overflow at coarse scale/high index) folds
/// into the `+Inf` overflow bucket rather than causing a rejection.
///
/// Same count-invariant cross-check as [`emit_histogram_point`]:
/// `derived_count = sum(bucket_counts) + zero_count`, cross-checked against
/// the reported `count` — mismatch rejects the whole data point, no
/// samples emitted.
fn emit_exponential_histogram_point(
    out: &mut ParsedMetrics,
    seen_series: &mut HashSet<(Arc<str>, Fingerprint)>,
    name: &Arc<str>,
    base_pairs: &[(String, String)],
    dp: &ExponentialHistogramDataPoint,
) {
    let unix_milli = match resolve_timestamp_ms(dp.time_unix_nano) {
        Ok(ms) => ms,
        Err(message) => {
            reject_point(out, format!("metric {name}: {message}"));
            return;
        }
    };
    let ctx = DataPointContext {
        base_pairs,
        dp_attributes: &dp.attributes,
        unix_milli,
    };

    let mut pairs = exponential_bucket_pairs(dp);
    let Some(derived_count) = checked_sum(pairs.iter().map(|(_, count)| *count)) else {
        reject_point(
            out,
            format!(
                "histogram {name}: bucket counts overflow u64 while summing (rejected rather \
                 than silently wrapping/panicking)"
            ),
        );
        return;
    };
    if derived_count != dp.count {
        reject_point(
            out,
            format!(
                "histogram {name}: bucket counts sum to {derived_count} but count={reported}",
                reported = dp.count
            ),
        );
        return;
    }

    // Non-finite bounds (scale/index overflow) fold into the `+Inf`
    // overflow bucket (architect plan) rather than being treated as a
    // distinct, unrenderable label.
    for (le, _) in pairs.iter_mut() {
        if !le.is_finite() {
            *le = f64::INFINITY;
        }
    }
    // Sorted ascending by bound, `total_cmp` rather than `partial_cmp`
    // (infallible: no NaN survives the non-finite fold above, but
    // `total_cmp` avoids ever panicking on a partial order regardless).
    pairs.sort_by(|a, b| a.0.total_cmp(&b.0));

    // Merge buckets whose *rendered* `le` collides (architect plan risk 3:
    // float rounding can map two distinct bucket bounds to the same
    // displayed label, which would otherwise emit two samples at the same
    // `(metric_name, fingerprint, timestamp)`).
    let mut merged: Vec<(String, u64)> = Vec::with_capacity(pairs.len());
    for (le, count) in pairs {
        let label = render_bound(le);
        match merged.last_mut() {
            // Infallible: merging only regroups the same already-checked-
            // sum sequence (`derived_count` above), so this partial total
            // can never itself overflow `u64`.
            Some(last) if last.0 == label => {
                last.1 = last
                    .1
                    .checked_add(count)
                    .expect("merged total is bounded by the already-checked derived_count")
            }
            _ => merged.push((label, count)),
        }
    }

    let bucket_name: Arc<str> = Arc::from(format!("{name}_bucket").as_str());
    let mut running: u64 = 0;
    let mut emitted_inf = false;
    for (label, count) in &merged {
        // Infallible for the same reason as the merge step above.
        running = running
            .checked_add(*count)
            .expect("prefix sum is bounded by the already-checked total derived_count");
        let value = stale_or(dp.flags, running as f64);
        emitted_inf = label == "+Inf";
        emit_sample(
            out,
            seen_series,
            Arc::clone(&bucket_name),
            &ctx,
            Some(("le", label.clone())),
            value,
        );
    }
    if !emitted_inf {
        // `running` already equals `derived_count` (every positive/
        // negative/zero bucket has been folded in above) — the `+Inf`
        // bucket is always present and always equals `_count` by
        // construction (architect plan invariant).
        let value = stale_or(dp.flags, running as f64);
        emit_sample(
            out,
            seen_series,
            Arc::clone(&bucket_name),
            &ctx,
            Some(("le", "+Inf".to_string())),
            value,
        );
    }

    let count_name: Arc<str> = Arc::from(format!("{name}_count").as_str());
    emit_sample(
        out,
        seen_series,
        count_name,
        &ctx,
        None,
        stale_or(dp.flags, dp.count as f64),
    );
    if let Some(sum) = dp.sum {
        let sum_name: Arc<str> = Arc::from(format!("{name}_sum").as_str());
        emit_sample(
            out,
            seen_series,
            sum_name,
            &ctx,
            None,
            stale_or(dp.flags, sum),
        );
    }
}

/// Builds the raw `(bound, count)` pairs for one exponential-histogram data
/// point — positive buckets, negative buckets, then the zero bucket
/// (unconditionally, even when `zero_count == 0`: it contributes nothing to
/// the cumulative sum in that case, keeping the invariant check simple and
/// total). Bounds are *not yet* folded to `+Inf` or sorted — that is
/// [`emit_exponential_histogram_point`]'s job, shared with the sum used for
/// the count-invariant check.
fn exponential_bucket_pairs(dp: &ExponentialHistogramDataPoint) -> Vec<(f64, u64)> {
    let mut pairs = Vec::new();
    if let Some(positive) = &dp.positive {
        for (j, &count) in positive.bucket_counts.iter().enumerate() {
            let k = bucket_index(positive.offset, j);
            pairs.push((exponential_upper_bound(k, dp.scale), count));
        }
    }
    if let Some(negative) = &dp.negative {
        for (j, &count) in negative.bucket_counts.iter().enumerate() {
            let k = bucket_index(negative.offset, j);
            pairs.push((exponential_lower_bound_negated(k, dp.scale), count));
        }
    }
    pairs.push((dp.zero_threshold, dp.zero_count));
    pairs
}

/// Widens `offset + j` (a bucket array index) to `i64` via checked/
/// saturating arithmetic — review finding 3: `j` never remotely approaches
/// `i64::MAX` in practice (bucket arrays are bounded by the 64 MiB
/// decompressed-body cap), but an adversarial/pathological `offset` near
/// `i32::MAX`/`MIN` combined with a crafted `j` must saturate rather than
/// wrap or panic. A saturated `k` still folds correctly: it feeds
/// [`exponential_upper_bound`]/[`exponential_lower_bound_negated`], whose
/// own overflow guard folds an extreme `k` to the `+Inf` bucket exactly
/// like a legitimate coarse-scale overflow.
fn bucket_index(offset: i32, j: usize) -> i64 {
    let j = i64::try_from(j).unwrap_or(i64::MAX);
    i64::from(offset).saturating_add(j)
}

/// `2f64.powi(-scale)` — the `2^-scale` factor inside the exponential-
/// histogram base formula (`base = 2^(2^-scale)`, both bound fns below).
/// `scale.checked_neg()` guards the one case unary negation on an `i32`
/// can overflow: `scale == i32::MIN` (legal on the wire — `scale` is a
/// `sint32`, no range restriction per the OTLP spec). That case is
/// mathematically `2^(2^31)` — astronomically large — so it maps directly
/// to `f64::INFINITY` rather than panicking on the negation (review
/// finding 3: no panic on any wire `scale` value). A resulting `0.0 *
/// f64::INFINITY` multiplication downstream (when `k`/`k+1` is exactly
/// `0`) produces `NaN`, not a panic — `NaN.is_finite()` is `false`, so the
/// caller's existing non-finite fold still routes it to the `+Inf`
/// overflow bucket correctly.
fn scale_factor(scale: i32) -> f64 {
    match scale.checked_neg() {
        Some(neg_scale) => 2f64.powi(neg_scale),
        None => f64::INFINITY,
    }
}

/// Positive-side upper bound of absolute bucket index `k` at `scale`:
/// `2f64.powf((k+1) as f64 * 2f64.powi(-scale))` (architect plan, pinned).
/// `k.checked_add(1)` guards the one integer operation in this expression
/// that could overflow (review finding 3); the `f64` arithmetic that
/// follows never panics — `powf`/`powi` saturate to `f64::INFINITY`/`0.0`
/// on overflow/underflow — so an overflowing `k + 1` folds directly to the
/// same `+Inf` outcome a legitimate coarse-scale overflow already produces
/// (this fn's caller's non-finite fold), rather than panicking.
fn exponential_upper_bound(k: i64, scale: i32) -> f64 {
    match k.checked_add(1) {
        Some(k_plus_one) => 2f64.powf(k_plus_one as f64 * scale_factor(scale)),
        None => f64::INFINITY,
    }
}

/// Negative-side bound of absolute bucket index `k` at `scale`, mirrored
/// and negated: `-2f64.powf(k as f64 * 2f64.powi(-scale))` — observations
/// in this bucket are `<= -base^k` (architect plan, pinned). No integer
/// addition here (unlike the positive-side `k + 1`), so the only overflow
/// guard needed is [`scale_factor`]'s — `k as f64` and the `f64`
/// operations that follow cannot panic.
fn exponential_lower_bound_negated(k: i64, scale: i32) -> f64 {
    -(2f64.powf(k as f64 * scale_factor(scale)))
}

/// Summary mapping (architect plan table): one `{name}` sample per
/// quantile (labeled with the synthetic `quantile` pair), plus `{name}_sum`
/// and `{name}_count` (both unconditional — `SummaryDataPoint::sum`/
/// `count` are non-optional on the wire, unlike a histogram's `sum`).
fn emit_summary_point(
    out: &mut ParsedMetrics,
    seen_series: &mut HashSet<(Arc<str>, Fingerprint)>,
    name: &Arc<str>,
    base_pairs: &[(String, String)],
    dp: &SummaryDataPoint,
) {
    let unix_milli = match resolve_timestamp_ms(dp.time_unix_nano) {
        Ok(ms) => ms,
        Err(message) => {
            reject_point(out, format!("metric {name}: {message}"));
            return;
        }
    };
    let ctx = DataPointContext {
        base_pairs,
        dp_attributes: &dp.attributes,
        unix_milli,
    };

    for qv in &dp.quantile_values {
        emit_sample(
            out,
            seen_series,
            Arc::clone(name),
            &ctx,
            Some(("quantile", render_bound(qv.quantile))),
            stale_or(dp.flags, qv.value),
        );
    }

    let sum_name: Arc<str> = Arc::from(format!("{name}_sum").as_str());
    emit_sample(
        out,
        seen_series,
        sum_name,
        &ctx,
        None,
        stale_or(dp.flags, dp.sum),
    );
    let count_name: Arc<str> = Arc::from(format!("{name}_count").as_str());
    emit_sample(
        out,
        seen_series,
        count_name,
        &ctx,
        None,
        stale_or(dp.flags, dp.count as f64),
    );
}

/// Builds one sample's final `LabelSet` (base pairs ⊕ this data point's
/// attributes ⊕ an optional synthetic `le`/`quantile` pair, all fed as ONE
/// iterator to [`LabelSet::from_normalized`] — architect plan: "no source
/// precedence override"), fingerprints it, registers the `(metric_name,
/// fingerprint)` series (deduped) and pushes the sample.
fn emit_sample(
    out: &mut ParsedMetrics,
    seen_series: &mut HashSet<(Arc<str>, Fingerprint)>,
    metric_name: Arc<str>,
    ctx: &DataPointContext<'_>,
    extra: Option<(&str, String)>,
    value: f64,
) {
    let pairs = ctx
        .base_pairs
        .iter()
        .cloned()
        .chain(attr_pairs(ctx.dp_attributes))
        .chain(extra.into_iter().map(|(k, v)| (k.to_string(), v)));
    let (labels, collisions) = LabelSet::from_normalized(pairs);
    out.collisions += collisions as u64;
    let fingerprint = metric_fingerprint(&labels);

    if seen_series.insert((Arc::clone(&metric_name), fingerprint)) {
        out.series.push(SeriesRef {
            metric_name: Arc::clone(&metric_name),
            fingerprint,
            labels,
        });
    }
    out.samples.push(MetricPoint {
        metric_name,
        fingerprint,
        unix_milli: ctx.unix_milli,
        value,
    });
}

/// Flattens `resource.attributes ⊕ otel_scope_name/version ⊕
/// scope.attributes` into raw `(key, value)` pairs — the base set every
/// data point in this `ScopeMetrics` chains its own attributes onto before
/// calling [`LabelSet::from_normalized`] (mirrors `otlp_logs`'s
/// `build_scope_labels`, but returns unresolved pairs rather than an
/// already-built `LabelSet`+fingerprint, since metrics resolve the final
/// label set per data point, not once per scope). `otel_scope_name`/
/// `otel_scope_version` are emitted only when `scope_metrics.scope` is
/// present (same rule as logs, issue #8 task-manager resolution).
fn build_scope_pairs(
    resource: Option<&Resource>,
    scope_metrics: &ScopeMetrics,
) -> Vec<(String, String)> {
    let resource_attrs = resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]);
    let scope = scope_metrics.scope.as_ref();
    let scope_identity = scope.into_iter().flat_map(|s| {
        [
            ("otel_scope_name".to_string(), s.name.clone()),
            ("otel_scope_version".to_string(), s.version.clone()),
        ]
    });
    let scope_attrs = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);

    attr_pairs(resource_attrs)
        .chain(scope_identity)
        .chain(attr_pairs(scope_attrs))
        .collect()
}

/// Renders a `KeyValue` list to `(key, value)` label pairs — value
/// rendering mirrors `otlp_logs::any_value_to_string` byte-for-byte
/// (duplicated here rather than shared: `otlp_logs.rs` is a frozen,
/// out-of-scope file for this issue, the same precedent
/// `otlp_logs::base64_encode`'s own doc comment already establishes for
/// this crate).
fn attr_pairs(attrs: &[KeyValue]) -> impl Iterator<Item = (String, String)> + '_ {
    attrs
        .iter()
        .map(|kv| (kv.key.clone(), any_value_to_string(kv.value.as_ref())))
}

/// Renders an OTLP attribute's `AnyValue` to its stored label-value form:
/// a string value verbatim; a scalar (bool/int/double) via `Display`; an
/// array/kvlist via `serde_json`; bytes as base64. Absent (`None`) or an
/// entirely unspecified `AnyValue` both render as `""`. See this module's
/// doc comment on [`attr_pairs`] for why this duplicates
/// `otlp_logs::any_value_to_string`.
fn any_value_to_string(value: Option<&AnyValue>) -> String {
    let Some(value) = value.and_then(|v| v.value.as_ref()) else {
        return String::new();
    };
    match value {
        Value::StringValue(s) => s.clone(),
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => d.to_string(),
        Value::ArrayValue(_) | Value::KvlistValue(_) => {
            serde_json::to_string(&any_value_to_json(value)).expect(
                "a JSON value tree built only from strings/numbers/bools/arrays/objects \
                 cannot fail to serialize",
            )
        }
        Value::BytesValue(bytes) => base64_encode(bytes),
        // Profiling-signal-only reference; non-profiling receivers treat
        // its presence as a non-fatal issue and process the value as
        // absent/empty (mirrors `otlp_logs::any_value_to_string`).
        Value::StringValueStrindex(_) => String::new(),
    }
}

/// Recursively renders an `AnyValue`'s `value` oneof to a `serde_json`
/// tree, used for the array/kvlist branch of [`any_value_to_string`].
fn any_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::StringValue(s) => serde_json::Value::String(s.clone()),
        Value::BoolValue(b) => serde_json::Value::Bool(*b),
        Value::IntValue(i) => serde_json::Value::Number((*i).into()),
        Value::DoubleValue(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::ArrayValue(array) => serde_json::Value::Array(
            array
                .values
                .iter()
                .map(|v| {
                    v.value
                        .as_ref()
                        .map(any_value_to_json)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        Value::KvlistValue(kvlist) => {
            let mut map = serde_json::Map::with_capacity(kvlist.values.len());
            for entry in &kvlist.values {
                let rendered = entry
                    .value
                    .as_ref()
                    .and_then(|v| v.value.as_ref())
                    .map(any_value_to_json)
                    .unwrap_or(serde_json::Value::Null);
                map.insert(entry.key.clone(), rendered);
            }
            serde_json::Value::Object(map)
        }
        Value::BytesValue(bytes) => serde_json::Value::String(base64_encode(bytes)),
        Value::StringValueStrindex(_) => serde_json::Value::Null,
    }
}

/// Minimal RFC 4648 standard base64 encoder (with padding), duplicated from
/// `otlp_logs::base64_encode` for the same reason (see that fn's doc
/// comment): `pulsus-write` does not depend on `pulsus-server`, and
/// `otlp_logs.rs` is out of scope for this issue.
fn base64_encode(input: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied();
        let b2 = chunk.get(2).copied();
        let n =
            (u32::from(b0) << 16) | (u32::from(b1.unwrap_or(0)) << 8) | u32::from(b2.unwrap_or(0));
        out.push(CHARS[((n >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((n >> 12) & 0x3F) as usize] as char);
        out.push(if b1.is_some() {
            CHARS[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        out.push(if b2.is_some() {
            CHARS[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Resolves a data point's `time_unix_nano` into `unix_milli` (truncating
/// integer division — ns is non-negative on the wire, docs/schemas.md
/// "verbatim at millisecond precision"). `Err` when `time_unix_nano == 0`
/// (task-manager resolution: a metric sample with no timestamp is
/// malformed, unlike a log record's `now_ns` fallback) — a per-point
/// rejection (partial success), not a whole-request failure.
///
/// The division-then-cast is infallible: `u64::MAX / 1_000_000 <
/// i64::MAX`, so any `u64` nanosecond value converts without truncation or
/// overflow once divided down to milliseconds.
fn resolve_timestamp_ms(time_unix_nano: u64) -> Result<i64, String> {
    if time_unix_nano == 0 {
        return Err("data point has time_unix_nano == 0".to_string());
    }
    let millis = time_unix_nano / 1_000_000;
    Ok(
        i64::try_from(millis)
            .expect("u64::MAX / 1_000_000 fits in i64 (see this fn's doc comment)"),
    )
}

/// `NumberDataPoint::value`'s `AsDouble`/`AsInt` union: `AsDouble` verbatim,
/// `AsInt` cast to `f64` (architect plan: precision loss beyond 2^53
/// accepted, matches Prometheus's own `float64` sample model). `None`
/// (neither oneof variant set) is an invalid data point per the OTLP spec's
/// own doc comment ("considered invalid when one of the recognized value
/// fields is not present") — the caller rejects it.
fn resolve_number_value(value: Option<&number_data_point::Value>) -> Option<f64> {
    match value {
        Some(number_data_point::Value::AsDouble(d)) => Some(*d),
        // `AsInt` beyond +/-2^53 loses integer exactness on this cast —
        // documented, accepted (architect plan).
        Some(number_data_point::Value::AsInt(i)) => Some(*i as f64),
        None => None,
    }
}

/// `true` when `flags` carries `DataPointFlags::NoRecordedValueMask`
/// (Prometheus's staleness marker equivalent).
fn is_stale(flags: u32) -> bool {
    flags & DataPointFlags::NoRecordedValueMask as u32 != 0
}

/// `value` unless `flags` marks the data point stale, in which case the
/// canonical stale-NaN bit pattern ([`STALE_NAN_BITS`]) is substituted
/// (architect plan: "for histograms: `_sum`, `_count`, and every bucket" —
/// every sample this fn backs, on any stale-flagged data point).
fn stale_or(flags: u32, value: f64) -> f64 {
    if is_stale(flags) {
        f64::from_bits(STALE_NAN_BITS)
    } else {
        value
    }
}

/// Renders a bound (`le` or `quantile`) via Rust's `f64` `Display`
/// (`format!("{value}")`) — the shortest round-trip decimal, never
/// scientific notation, which is bit-for-bit what Go's
/// `strconv.FormatFloat(v, 'f', -1, 64)` produces (the OpenTelemetry
/// collector's `prometheusremotewrite` exporter's own rendering rule).
/// Fingerprint-identity critical (architect plan risk 1): any drift here
/// silently corrupts label resolution against a value written by a real
/// collector. Any non-finite value (the exponential-histogram overflow
/// fold) renders as the literal string `"+Inf"`, matching Prometheus's own
/// bucket label.
fn render_bound(value: f64) -> String {
    if value.is_finite() {
        format!("{value}")
    } else {
        "+Inf".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets;
    use opentelemetry_proto::tonic::metrics::v1::{
        ExponentialHistogram, Gauge, Histogram, ResourceMetrics, Sum, Summary,
        summary_data_point::ValueAtQuantile,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
            key_strindex: 0,
        }
    }

    fn request(resource_metrics: Vec<ResourceMetrics>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest { resource_metrics }
    }

    fn scope_metrics(metrics: Vec<Metric>) -> ScopeMetrics {
        ScopeMetrics {
            scope: None,
            metrics,
            schema_url: String::new(),
        }
    }

    fn one_metric_request(
        resource: Option<Resource>,
        metric: Metric,
    ) -> ExportMetricsServiceRequest {
        request(vec![ResourceMetrics {
            resource,
            scope_metrics: vec![scope_metrics(vec![metric])],
            schema_url: String::new(),
        }])
    }

    fn gauge_metric(name: &str, dp: NumberDataPoint) -> Metric {
        Metric {
            name: name.to_string(),
            description: "a gauge".to_string(),
            unit: "1".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![dp],
            })),
        }
    }

    fn number_dp(time_unix_nano: u64, value: f64, attributes: Vec<KeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            attributes,
            start_time_unix_nano: 0,
            time_unix_nano,
            exemplars: vec![],
            flags: 0,
            value: Some(number_data_point::Value::AsDouble(value)),
        }
    }

    // -- parse: empty / pure -------------------------------------------

    #[test]
    fn parse_of_empty_request_returns_empty_output() {
        let out = parse(&request(vec![]), 1_000);
        assert_eq!(out, ParsedMetrics::default());
    }

    #[test]
    fn parse_is_a_pure_function_of_its_arguments() {
        let req = one_metric_request(
            None,
            gauge_metric("up", number_dp(1_700_000_000_000_000_000, 1.0, vec![])),
        );
        let a = parse(&req, 42);
        let b = parse(&req, 42);
        assert_eq!(a, b);
    }

    // -- decode -----------------------------------------------------------

    #[test]
    fn decode_rejects_malformed_bytes() {
        let err = decode(b"\xFF\xFF\xFF not a protobuf message").unwrap_err();
        assert!(matches!(err, LogsIngestError::Decode(_)));
    }

    #[test]
    fn decode_round_trips_an_encoded_request() {
        let req = one_metric_request(None, gauge_metric("up", number_dp(1, 1.0, vec![])));
        let bytes = req.encode_to_vec();
        let decoded = decode(&bytes).expect("valid protobuf decodes");
        assert_eq!(decoded, req);
    }

    // -- gauge / sum --------------------------------------------------

    #[test]
    fn gauge_data_point_flattens_to_one_sample_named_verbatim() {
        let req = one_metric_request(
            None,
            gauge_metric(
                "cpu_usage",
                number_dp(
                    1_700_000_000_000_000_000,
                    0.5,
                    vec![kv("host", Value::StringValue("a".into()))],
                ),
            ),
        );
        let out = parse(&req, 0);
        assert_eq!(out.samples.len(), 1);
        assert_eq!(&*out.samples[0].metric_name, "cpu_usage");
        assert_eq!(out.samples[0].value, 0.5);
        assert_eq!(out.samples[0].unix_milli, 1_700_000_000_000);
        assert_eq!(out.series.len(), 1);
        assert_eq!(out.series[0].labels.get("host"), Some("a"));
        assert_eq!(out.metadata.len(), 1);
        assert_eq!(&*out.metadata[0].metric_name, "cpu_usage");
        assert_eq!(out.metadata[0].metric_type, "gauge");
        assert_eq!(out.metadata[0].updated_ns, 0);
    }

    #[test]
    fn sum_monotonic_metadata_type_is_counter() {
        let metric = Metric {
            name: "requests_total".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_dp(1, 1.0, vec![])],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
        };
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.metadata[0].metric_type, "counter");
    }

    #[test]
    fn sum_non_monotonic_metadata_type_is_gauge() {
        let metric = Metric {
            name: "queue_size".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_dp(1, 1.0, vec![])],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: false,
            })),
        };
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.metadata[0].metric_type, "gauge");
    }

    #[test]
    fn sum_delta_temporality_rejects_every_data_point() {
        let metric = Metric {
            name: "requests_total".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_dp(1, 1.0, vec![]), number_dp(2, 2.0, vec![])],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: true,
            })),
        };
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.rejected, 2);
        assert!(
            out.rejected_message
                .as_ref()
                .unwrap()
                .contains("requests_total")
        );
        assert!(out.samples.is_empty());
        // Metadata is still registered (type is knowable independent of
        // temporality support).
        assert_eq!(out.metadata.len(), 1);
    }

    #[test]
    fn number_data_point_with_zero_timestamp_is_rejected_as_partial_success() {
        let req = one_metric_request(None, gauge_metric("up", number_dp(0, 1.0, vec![])));
        let out = parse(&req, 0);
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
    }

    #[test]
    fn number_data_point_with_no_value_field_is_rejected() {
        let dp = NumberDataPoint {
            attributes: vec![],
            start_time_unix_nano: 0,
            time_unix_nano: 1,
            exemplars: vec![],
            flags: 0,
            value: None,
        };
        let req = one_metric_request(None, gauge_metric("up", dp));
        let out = parse(&req, 0);
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
    }

    #[test]
    fn as_int_value_casts_to_f64() {
        let dp = NumberDataPoint {
            attributes: vec![],
            start_time_unix_nano: 0,
            time_unix_nano: 1,
            exemplars: vec![],
            flags: 0,
            value: Some(number_data_point::Value::AsInt(42)),
        };
        let out = parse(&one_metric_request(None, gauge_metric("up", dp)), 0);
        assert_eq!(out.samples[0].value, 42.0);
    }

    #[test]
    fn no_recorded_value_flag_emits_the_stale_nan_bit_pattern() {
        let dp = NumberDataPoint {
            attributes: vec![],
            start_time_unix_nano: 0,
            time_unix_nano: 1,
            exemplars: vec![],
            flags: DataPointFlags::NoRecordedValueMask as u32,
            value: Some(number_data_point::Value::AsDouble(1.0)),
        };
        let out = parse(&one_metric_request(None, gauge_metric("up", dp)), 0);
        assert_eq!(out.samples[0].value.to_bits(), STALE_NAN_BITS);
    }

    // -- label normalization / fingerprint identity --------------------

    #[test]
    fn dotted_and_underscored_service_name_fingerprint_identically() {
        let resource_dot = Resource {
            attributes: vec![kv("service.name", Value::StringValue("checkout".into()))],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let resource_underscore = Resource {
            attributes: vec![kv("service_name", Value::StringValue("checkout".into()))],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let out_dot = parse(
            &one_metric_request(
                Some(resource_dot),
                gauge_metric("up", number_dp(1, 1.0, vec![])),
            ),
            0,
        );
        let out_underscore = parse(
            &one_metric_request(
                Some(resource_underscore),
                gauge_metric("up", number_dp(1, 1.0, vec![])),
            ),
            0,
        );
        assert_eq!(
            out_dot.samples[0].fingerprint,
            out_underscore.samples[0].fingerprint
        );
    }

    #[test]
    fn metric_name_never_enters_the_label_set() {
        let out = parse(
            &one_metric_request(
                None,
                gauge_metric("http_requests_total", number_dp(1, 1.0, vec![])),
            ),
            0,
        );
        assert_eq!(out.series[0].labels.get("__name__"), None);
    }

    #[test]
    fn scope_identity_labels_present_only_when_scope_is_present() {
        let with_scope = ScopeMetrics {
            scope: Some(InstrumentationScope {
                name: "my-scope".to_string(),
                version: "1.0.0".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            metrics: vec![gauge_metric("up", number_dp(1, 1.0, vec![]))],
            schema_url: String::new(),
        };
        let out = parse(
            &request(vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![with_scope],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(
            out.series[0].labels.get("otel_scope_name"),
            Some("my-scope")
        );
        assert_eq!(
            out.series[0].labels.get("otel_scope_version"),
            Some("1.0.0")
        );
    }

    // -- histogram ------------------------------------------------------

    fn histogram_dp(
        time_unix_nano: u64,
        count: u64,
        sum: Option<f64>,
        bucket_counts: Vec<u64>,
        explicit_bounds: Vec<f64>,
    ) -> HistogramDataPoint {
        HistogramDataPoint {
            attributes: vec![],
            start_time_unix_nano: 0,
            time_unix_nano,
            count,
            sum,
            bucket_counts,
            explicit_bounds,
            exemplars: vec![],
            flags: 0,
            min: None,
            max: None,
        }
    }

    fn histogram_metric(
        name: &str,
        temporality: AggregationTemporality,
        dp: HistogramDataPoint,
    ) -> Metric {
        Metric {
            name: name.to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![dp],
                aggregation_temporality: temporality as i32,
            })),
        }
    }

    #[test]
    fn histogram_flattens_to_cumulative_buckets_and_inf_equals_count() {
        // bounds [1.0, 2.5], bucket_counts [2, 3, 5] -> cumulative [2, 5, 10]
        let dp = histogram_dp(
            1_700_000_000_000_000_000,
            10,
            Some(42.0),
            vec![2, 3, 5],
            vec![1.0, 2.5],
        );
        let metric = histogram_metric("latency", AggregationTemporality::Cumulative, dp);
        let out = parse(&one_metric_request(None, metric), 0);

        let buckets: Vec<_> = out
            .samples
            .iter()
            .filter(|s| &*s.metric_name == "latency_bucket")
            .collect();
        assert_eq!(buckets.len(), 3);

        let le = |s: &MetricPoint| {
            out.series
                .iter()
                .find(|r| r.fingerprint == s.fingerprint)
                .unwrap()
                .labels
                .get("le")
                .unwrap()
                .to_string()
        };
        let mut rendered: Vec<(String, f64)> = buckets.iter().map(|s| (le(s), s.value)).collect();
        rendered.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        assert_eq!(
            rendered,
            vec![
                ("1".to_string(), 2.0),
                ("2.5".to_string(), 5.0),
                ("+Inf".to_string(), 10.0),
            ]
        );

        let count = out
            .samples
            .iter()
            .find(|s| &*s.metric_name == "latency_count")
            .unwrap();
        assert_eq!(count.value, 10.0);
        let sum = out
            .samples
            .iter()
            .find(|s| &*s.metric_name == "latency_sum")
            .unwrap();
        assert_eq!(sum.value, 42.0);
    }

    #[test]
    fn histogram_count_mismatch_rejects_the_whole_data_point_with_no_samples() {
        // bucket_counts sum to 10, but reported count is 99 -> mismatch.
        let dp = histogram_dp(1, 99, Some(1.0), vec![2, 3, 5], vec![1.0, 2.5]);
        let metric = histogram_metric("latency", AggregationTemporality::Cumulative, dp);
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.rejected, 1);
        assert!(out.rejected_message.as_ref().unwrap().contains("latency"));
        assert!(out.samples.is_empty());
    }

    /// Review finding 2: a bucketless histogram (legal OTLP shape — "count
    /// and sum are known" only) still emits `_bucket{le="+Inf"} == _count`
    /// unconditionally, alongside `_sum`/`_count` — three samples, not two.
    #[test]
    fn histogram_with_no_bucket_distribution_still_emits_inf_bucket_equal_to_count() {
        let dp = histogram_dp(1, 5, Some(12.5), vec![], vec![]);
        let metric = histogram_metric("latency", AggregationTemporality::Cumulative, dp);
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.samples.len(), 3);

        let bucket = out
            .samples
            .iter()
            .find(|s| &*s.metric_name == "latency_bucket")
            .expect("+Inf bucket is emitted even with no explicit distribution");
        assert_eq!(bucket.value, 5.0);
        let bucket_series = out
            .series
            .iter()
            .find(|r| r.fingerprint == bucket.fingerprint)
            .unwrap();
        assert_eq!(bucket_series.labels.get("le"), Some("+Inf"));

        let count = out
            .samples
            .iter()
            .find(|s| &*s.metric_name == "latency_count")
            .unwrap();
        assert_eq!(count.value, 5.0);
        assert_eq!(bucket.value, count.value);
    }

    #[test]
    fn histogram_delta_temporality_rejects_the_whole_metric() {
        let dp = histogram_dp(1, 10, Some(1.0), vec![10], vec![]);
        let metric = histogram_metric("latency", AggregationTemporality::Delta, dp);
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
    }

    // -- exponential histogram -------------------------------------------

    /// A base fixture (`time_unix_nano = 1`, scale 0, no buckets/sum) each
    /// test overrides via struct-update syntax — sidesteps an 8-argument
    /// helper function (`ExponentialHistogramDataPoint` derives `Default`
    /// via `prost::Message`, same convention `otlp_logs.rs`'s tests use for
    /// `LogRecord { ..Default::default() }`).
    fn exp_histogram_dp() -> ExponentialHistogramDataPoint {
        ExponentialHistogramDataPoint {
            time_unix_nano: 1,
            ..Default::default()
        }
    }

    fn exp_histogram_metric(name: &str, dp: ExponentialHistogramDataPoint) -> Metric {
        Metric {
            name: name.to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![dp],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
        }
    }

    #[test]
    fn exponential_histogram_negative_zero_positive_buckets_sum_to_inf_equals_count() {
        // scale 0 (base = 2): positive offset 0, counts [1, 1] -> buckets
        // (0,1]->1 obs, (1,2]->1 obs; negative offset 0, counts [1] -> bucket
        // observations <= -1; zero bucket count 1. total = 1+1+1+1 = 4.
        let dp = ExponentialHistogramDataPoint {
            time_unix_nano: 1_700_000_000_000_000_000,
            count: 4,
            sum: Some(-3.5),
            zero_count: 1,
            positive: Some(Buckets {
                offset: 0,
                bucket_counts: vec![1, 1],
            }),
            negative: Some(Buckets {
                offset: 0,
                bucket_counts: vec![1],
            }),
            ..exp_histogram_dp()
        };
        let metric = exp_histogram_metric("size", dp);
        let out = parse(&one_metric_request(None, metric), 0);

        let count = out
            .samples
            .iter()
            .find(|s| &*s.metric_name == "size_count")
            .unwrap();
        assert_eq!(count.value, 4.0);

        // Find the +Inf-labeled bucket sample by cross-referencing its
        // series' labels, and assert it equals `_count` (the invariant).
        let inf_sample = out
            .samples
            .iter()
            .filter(|s| &*s.metric_name == "size_bucket")
            .find(|s| {
                out.series
                    .iter()
                    .any(|r| r.fingerprint == s.fingerprint && r.labels.get("le") == Some("+Inf"))
            })
            .expect("a +Inf bucket is always present");
        assert_eq!(inf_sample.value, 4.0);
    }

    #[test]
    fn exponential_histogram_count_mismatch_rejects_the_whole_data_point() {
        let dp = ExponentialHistogramDataPoint {
            count: 99, // wrong: actual bucket total is 2
            positive: Some(Buckets {
                offset: 0,
                bucket_counts: vec![2],
            }),
            ..exp_histogram_dp()
        };
        let metric = exp_histogram_metric("size", dp);
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
    }

    #[test]
    fn exponential_histogram_delta_temporality_rejects_the_whole_metric() {
        let dp = ExponentialHistogramDataPoint {
            count: 1,
            zero_count: 1,
            ..exp_histogram_dp()
        };
        let metric = Metric {
            name: "size".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![dp],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
        };
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
    }

    #[test]
    fn exponential_upper_bound_matches_the_pinned_formula() {
        // scale 0: base = 2, bucket index k=0 upper bound = 2^(0+1) = 2.0
        assert_eq!(exponential_upper_bound(0, 0), 2.0);
        // k=1 -> 2^2 = 4.0
        assert_eq!(exponential_upper_bound(1, 0), 4.0);
    }

    #[test]
    fn exponential_bound_overflow_folds_to_positive_infinity() {
        // A very coarse negative scale drives the base itself to overflow.
        let huge = exponential_upper_bound(i64::MAX / 2, -10);
        assert!(!huge.is_finite());
    }

    // -- review finding 1: u64 overflow guards -----------------------------

    #[test]
    fn checked_sum_rejects_an_overflowing_total() {
        assert_eq!(checked_sum([u64::MAX, 1]), None);
        assert_eq!(checked_sum([1, 2, 3]), Some(6));
        assert_eq!(checked_sum(std::iter::empty()), Some(0));
    }

    #[test]
    fn histogram_bucket_counts_overflowing_u64_rejects_the_data_point_without_panicking() {
        // Two buckets whose sum overflows `u64` — a payload no legitimate
        // collector would ever produce, but an adversarial/corrupted one
        // could. Must reject via partial success, never panic (debug
        // builds) or silently wrap to an under-count (release builds).
        let dp = histogram_dp(1, 5, None, vec![u64::MAX, 1], vec![1.0]);
        let metric = histogram_metric("latency", AggregationTemporality::Cumulative, dp);
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
        assert!(out.rejected_message.as_ref().unwrap().contains("overflow"));
    }

    #[test]
    fn exponential_histogram_bucket_counts_overflowing_u64_rejects_the_data_point_without_panicking()
     {
        let dp = ExponentialHistogramDataPoint {
            count: 5,
            positive: Some(Buckets {
                offset: 0,
                bucket_counts: vec![u64::MAX, 1],
            }),
            ..exp_histogram_dp()
        };
        let metric = exp_histogram_metric("size", dp);
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
        assert!(out.rejected_message.as_ref().unwrap().contains("overflow"));
    }

    // -- review finding 3: extreme scale/offset never panics --------------

    #[test]
    fn extreme_positive_offset_and_index_fold_to_positive_infinity_without_panicking() {
        // `offset` at the very top of its `i32` range plus a further
        // index: `bucket_index` must saturate rather than overflow/panic,
        // and the resulting extreme `k` must still fold to a non-finite
        // (eventually `+Inf`) bound rather than panic.
        let k = bucket_index(i32::MAX, usize::MAX);
        let bound = exponential_upper_bound(k, 0);
        assert!(!bound.is_finite());
    }

    #[test]
    fn extreme_negative_offset_folds_without_panicking() {
        // At scale 0, `k = i32::MIN` drives the exponent to a huge
        // negative number, which underflows `2f64.powf` toward `0.0`
        // (not `+Inf` — the negative-bound formula has no `+1`, so this
        // extreme does not overflow the way the positive side's `k + 1`
        // can). The point under test is that computing it never panics;
        // the resulting `-0.0` is asserted as a concrete, non-panicking
        // outcome rather than a tautological "finite or not" check.
        let k = bucket_index(i32::MIN, 0);
        let bound = exponential_lower_bound_negated(k, 0);
        assert_eq!(bound, 0.0);
    }

    #[test]
    fn scale_i32_min_never_panics_on_negation() {
        // `scale.checked_neg()`'s one guarded case: `i32::MIN` has no
        // positive `i32` counterpart.
        assert!(scale_factor(i32::MIN).is_infinite());
        let bound = exponential_upper_bound(0, i32::MIN);
        // `0.0 * f64::INFINITY == NaN`, which is non-finite (never a
        // panic) and folds to `+Inf` at the call site exactly like any
        // other non-finite bound.
        assert!(!bound.is_finite());
    }

    /// A cheap, non-cryptographic, fixed-seed xorshift64 PRNG (same
    /// pattern as `pulsus-write::writer::table::XorShift64` and
    /// `pulsus-logql`'s `tests/fuzz_smoke.rs`) — deterministic so this
    /// test is reproducible in CI.
    struct XorShift64(u64);

    impl XorShift64 {
        fn new(seed: u64) -> Self {
            XorShift64(seed | 1)
        }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }

    /// Fuzz-smoke (review finding 4, "in the spirit of the fuzz-smoke
    /// precedent"): random `scale`/`offset`/`index` combinations,
    /// including the extremes, fed through every exponential-histogram
    /// bound/index helper. Only a panic fails this test — any `f64`
    /// output (finite or not) is an acceptable outcome.
    #[test]
    fn exponential_bound_helpers_never_panic_across_random_scale_offset_index() {
        let mut rng = XorShift64::new(0xD1CE_F00D);
        let extremes = [i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX];
        for _ in 0..2000 {
            let scale = extremes[(rng.next_u64() as usize) % extremes.len()]
                .wrapping_add((rng.next_u64() % 21).wrapping_sub(10) as i32);
            let offset = extremes[(rng.next_u64() as usize) % extremes.len()];
            let index = (rng.next_u64() % 1_000_003) as usize;

            let k = bucket_index(offset, index);
            let _ = exponential_upper_bound(k, scale);
            let _ = exponential_lower_bound_negated(k, scale);
            let _ = scale_factor(scale);
        }
        // Reaching here (no panic across 2000 pseudo-random combinations
        // spanning the full `i32` extremes) is the assertion.
    }

    // -- summary ----------------------------------------------------------

    fn summary_dp(
        time_unix_nano: u64,
        count: u64,
        sum: f64,
        quantiles: Vec<(f64, f64)>,
    ) -> SummaryDataPoint {
        SummaryDataPoint {
            attributes: vec![],
            start_time_unix_nano: 0,
            time_unix_nano,
            count,
            sum,
            quantile_values: quantiles
                .into_iter()
                .map(|(quantile, value)| ValueAtQuantile { quantile, value })
                .collect(),
            flags: 0,
        }
    }

    #[test]
    fn summary_flattens_to_quantile_sum_and_count_series() {
        let dp = summary_dp(
            1_700_000_000_000_000_000,
            3,
            6.0,
            vec![(0.5, 1.5), (0.99, 2.5)],
        );
        let metric = Metric {
            name: "req_duration".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(metric::Data::Summary(Summary {
                data_points: vec![dp],
            })),
        };
        let out = parse(&one_metric_request(None, metric), 0);
        assert_eq!(out.samples.len(), 4); // 2 quantiles + sum + count
        assert_eq!(out.metadata[0].metric_type, "summary");

        let q = |name_quantile: &str| {
            out.samples
                .iter()
                .find(|s| {
                    &*s.metric_name == "req_duration"
                        && out.series.iter().any(|r| {
                            r.fingerprint == s.fingerprint
                                && r.labels.get("quantile") == Some(name_quantile)
                        })
                })
                .unwrap()
        };
        assert_eq!(q("0.5").value, 1.5);
        assert_eq!(q("0.99").value, 2.5);

        let sum = out
            .samples
            .iter()
            .find(|s| &*s.metric_name == "req_duration_sum")
            .unwrap();
        assert_eq!(sum.value, 6.0);
        let count = out
            .samples
            .iter()
            .find(|s| &*s.metric_name == "req_duration_count")
            .unwrap();
        assert_eq!(count.value, 3.0);
    }

    // -- render_bound golden ------------------------------------------

    #[test]
    fn render_bound_matches_the_go_strconv_format_float_f_neg1_64_convention() {
        for (value, expected) in [
            (0.005, "0.005"),
            (0.01, "0.01"),
            (2.5, "2.5"),
            (1e-9, "0.000000001"),
            (100.0, "100"),
        ] {
            assert_eq!(render_bound(value), expected, "value {value}");
        }
        assert_eq!(render_bound(f64::INFINITY), "+Inf");
        assert_eq!(render_bound(f64::NEG_INFINITY), "+Inf");
        assert_eq!(render_bound(f64::NAN), "+Inf");
    }

    // -- metadata dedup -------------------------------------------------

    #[test]
    fn metadata_is_deduped_within_a_request_by_base_name() {
        let metric_a = gauge_metric(
            "up",
            number_dp(1, 1.0, vec![kv("a", Value::StringValue("1".into()))]),
        );
        let metric_b = gauge_metric(
            "up",
            number_dp(2, 2.0, vec![kv("b", Value::StringValue("2".into()))]),
        );
        let out = parse(
            &request(vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![scope_metrics(vec![metric_a, metric_b])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.metadata.len(), 1);
        assert_eq!(out.samples.len(), 2);
    }

    #[test]
    fn label_collisions_are_counted_from_resource_and_scope_and_datapoint_attrs() {
        let resource = Resource {
            attributes: vec![kv("env", Value::StringValue("from_resource".into()))],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let dp = number_dp(
            1,
            1.0,
            vec![kv("env", Value::StringValue("from_dp".into()))],
        );
        let out = parse(
            &one_metric_request(Some(resource), gauge_metric("up", dp)),
            0,
        );
        assert_eq!(out.collisions, 1);
    }
}
