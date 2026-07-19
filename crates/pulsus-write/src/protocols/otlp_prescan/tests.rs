//! Hermetic proving tests for the OTLP protobuf wire pre-scan (issue #115,
//! track 5). Each repeated field / fan-out vector in the plan's exhaustive
//! checklist gets a non-vacuous pair: a payload at the cap pre-scan-PASSES, a
//! payload one past the cap is REJECTED with the field-named
//! [`LogsIngestError::OversizeMessage`] — proving the reject is attributable to
//! the guard alone (the two payloads differ by exactly one element). Payloads
//! are built directly as protobuf WIRE bytes (empty sub-messages are two bytes
//! each), so an over-cap fan-out is cheap to construct here yet — the point —
//! would allocate the amplified structure only if it reached `decode`; the
//! pre-scan rejects it first.

use prost::Message;

use super::*;
use crate::error::LogsIngestError;

// ---------------------------------------------------------------------------
// Wire builders
// ---------------------------------------------------------------------------

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn tag(field: u32, wire: u8) -> u64 {
    (u64::from(field) << 3) | u64::from(wire)
}

/// A length-delimited (`wire type 2`) field: tag, length, `payload`. Used for
/// sub-messages, `string`/`bytes` scalars, and packed scalar blobs alike.
fn ld(field: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 8);
    put_varint(&mut out, tag(field, 2));
    put_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(payload);
    out
}

/// `n` occurrences of an empty length-delimited `field` (each two bytes) — the
/// cheapest way to drive a repeated sub-message field's count.
fn empty_repeated(field: u32, n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n * 2);
    let t = tag(field, 2);
    for _ in 0..n {
        put_varint(&mut out, t);
        out.push(0); // length 0
    }
    out
}

// ---------------------------------------------------------------------------
// Field numbers (verified against vendor/opentelemetry-proto/.../*.rs)
// ---------------------------------------------------------------------------

const F_REQ_ROOT: u32 = 1; // resource_spans / resource_metrics / resource_logs
const F_RS_SCOPE_SPANS: u32 = 2;
const F_SS_SPANS: u32 = 2;
const F_SPAN_ATTRS: u32 = 9;
const F_SPAN_EVENTS: u32 = 11;
const F_SPAN_LINKS: u32 = 13;
const F_EVENT_ATTRS: u32 = 3;
const F_LINK_ATTRS: u32 = 4;

const F_RM_SCOPE_METRICS: u32 = 2;
const F_SM_METRICS: u32 = 2;
const F_METRIC_GAUGE: u32 = 5;
const F_METRIC_SUM: u32 = 7;
const F_METRIC_HISTOGRAM: u32 = 9;
const F_METRIC_EXP_HISTOGRAM: u32 = 10;
const F_METRIC_SUMMARY: u32 = 11;
const F_METRIC_METADATA: u32 = 12;
const F_DATA_POINTS: u32 = 1;
const F_NDP_ATTRS: u32 = 7;
const F_NDP_EXEMPLARS: u32 = 5;
const F_HDP_ATTRS: u32 = 9;
const F_HDP_BUCKET_COUNTS: u32 = 6;
const F_HDP_EXPLICIT_BOUNDS: u32 = 7;
const F_HDP_EXEMPLARS: u32 = 8;
const F_EHDP_ATTRS: u32 = 1;
const F_EHDP_POSITIVE: u32 = 8;
const F_EHDP_EXEMPLARS: u32 = 11;
const F_EH_BUCKETS_COUNTS: u32 = 2;
const F_SDP_ATTRS: u32 = 7;
const F_SDP_QUANTILES: u32 = 6;
const F_EXEMPLAR_ATTRS: u32 = 7;

const F_RL_SCOPE_LOGS: u32 = 2;
const F_SL_LOG_RECORDS: u32 = 2;
const F_LR_BODY: u32 = 5;
const F_LR_ATTRS: u32 = 6;

const F_RESOURCE: u32 = 1; // ResourceSpans/Metrics/Logs.resource
const F_SCOPE: u32 = 1; // ScopeSpans/Metrics/Logs.scope
const F_RESOURCE_ATTRS: u32 = 1;
const F_RESOURCE_ENTITY_REFS: u32 = 3;
const F_SCOPE_ATTRS: u32 = 3;
const F_ENTITYREF_ID_KEYS: u32 = 3;
const F_ENTITYREF_DESC_KEYS: u32 = 4;

const F_KV_VALUE: u32 = 2;
const F_KV_KEY: u32 = 1;
const F_AV_STRING: u32 = 1;
const F_AV_ARRAY: u32 = 5;
const F_AV_KVLIST: u32 = 6;
const F_ARRAY_VALUES: u32 = 1;
const F_KVLIST_VALUES: u32 = 1;

// ---------------------------------------------------------------------------
// Structural wrappers: place a leaf field payload under a full request root
// ---------------------------------------------------------------------------

/// Wraps an already-built `spans_area` (a concat of `Span` field entries) in
/// one `ResourceSpans` / one `ScopeSpans` under the traces request root.
fn traces_with_spans(spans_area: &[u8]) -> Vec<u8> {
    let resource_spans = ld(F_RS_SCOPE_SPANS, spans_area); // one ScopeSpans(payload=spans_area)
    ld(F_REQ_ROOT, &resource_spans) // one ResourceSpans
}

/// Wraps a single `Span` payload as one span under the traces root.
fn traces_with_one_span(span_payload: &[u8]) -> Vec<u8> {
    traces_with_spans(&ld(F_SS_SPANS, span_payload))
}

/// Wraps a `metrics_area` (concat of `Metric` field entries) under the metrics
/// request root, in one `ResourceMetrics` / one `ScopeMetrics`.
fn metrics_with_metrics(metrics_area: &[u8]) -> Vec<u8> {
    let resource_metrics = ld(F_RM_SCOPE_METRICS, metrics_area);
    ld(F_REQ_ROOT, &resource_metrics)
}

/// Wraps a `Metric` payload as one `ScopeMetrics.metrics` (field 2) entry.
fn one_metric(metric_payload: &[u8]) -> Vec<u8> {
    ld(F_SM_METRICS, metric_payload)
}

/// Wraps a single data-point-container payload for a given `Metric.data` oneof
/// arm, under one metric / scope / resource.
fn metrics_with_one_datapoint_container(oneof_field: u32, container_payload: &[u8]) -> Vec<u8> {
    let metric = ld(oneof_field, container_payload);
    metrics_with_metrics(&one_metric(&metric))
}

/// Wraps a data-point payload (one data point) under the given oneof arm.
fn metrics_with_one_datapoint(oneof_field: u32, dp_payload: &[u8]) -> Vec<u8> {
    metrics_with_one_datapoint_container(oneof_field, &ld(F_DATA_POINTS, dp_payload))
}

/// Wraps a `log_area` (concat of `LogRecord` field entries) under the logs
/// request root, in one `ResourceLogs` / one `ScopeLogs`.
fn logs_with_records(records_area: &[u8]) -> Vec<u8> {
    let resource_logs = ld(F_RL_SCOPE_LOGS, records_area);
    ld(F_REQ_ROOT, &resource_logs)
}

/// One `KeyValue` attribute occurrence carrying `key`.
fn attribute(field: u32, key: &[u8]) -> Vec<u8> {
    ld(field, &ld(F_KV_KEY, key))
}

/// `n` attribute occurrences under `field`, all sharing the identical key
/// bytes (so a dedup-collapsing counter would wrongly see one key).
fn attributes(field: u32, n: usize, key: &[u8]) -> Vec<u8> {
    let one = attribute(field, key);
    one.repeat(n)
}

// ---------------------------------------------------------------------------
// Generic non-vacuous cap assertion
// ---------------------------------------------------------------------------

/// Asserts `build(cap)` pre-scan-passes and `build(cap + 1)` is rejected with
/// `OversizeMessage { field }`.
fn assert_cap<F>(
    prescan: fn(&[u8]) -> Result<(), LogsIngestError>,
    cap: usize,
    field: &str,
    build: F,
) where
    F: Fn(usize) -> Vec<u8>,
{
    let at_cap = build(cap);
    prescan(&at_cap)
        .unwrap_or_else(|err| panic!("{field}: exactly {cap} must pass the pre-scan, got {err:?}"));
    let over = build(cap + 1);
    match prescan(&over) {
        Err(LogsIngestError::OversizeMessage {
            field: got,
            limit,
            actual,
        }) => {
            assert_eq!(got, field, "{field}: wrong OversizeMessage field");
            assert!(
                actual > limit,
                "{field}: actual {actual} must exceed limit {limit}"
            );
        }
        other => panic!("{field}: {} must be rejected, got {other:?}", cap + 1),
    }
}

// ===========================================================================
// Traces — per-level caps
// ===========================================================================

#[test]
fn resource_spans_per_level_cap() {
    assert_cap(prescan_traces, MAX_RESOURCE_SPANS, "resource_spans", |n| {
        empty_repeated(F_REQ_ROOT, n)
    });
}

#[test]
fn scope_spans_per_level_cap() {
    assert_cap(prescan_traces, MAX_SCOPE_SPANS, "scope_spans", |n| {
        ld(F_REQ_ROOT, &empty_repeated(F_RS_SCOPE_SPANS, n))
    });
}

#[test]
fn spans_per_level_cap() {
    assert_cap(prescan_traces, MAX_SPANS, "spans", |n| {
        traces_with_spans(&empty_repeated(F_SS_SPANS, n))
    });
}

#[test]
fn span_attributes_per_level_cap() {
    assert_cap(
        prescan_traces,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| traces_with_one_span(&attributes(F_SPAN_ATTRS, n, b"k")),
    );
}

#[test]
fn span_events_per_level_cap() {
    assert_cap(prescan_traces, MAX_EVENTS_PER_SPAN, "events", |n| {
        traces_with_one_span(&empty_repeated(F_SPAN_EVENTS, n))
    });
}

#[test]
fn span_links_per_level_cap() {
    assert_cap(prescan_traces, MAX_LINKS_PER_SPAN, "links", |n| {
        traces_with_one_span(&empty_repeated(F_SPAN_LINKS, n))
    });
}

#[test]
fn span_event_attributes_per_level_cap() {
    assert_cap(
        prescan_traces,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| {
            let event = attributes(F_EVENT_ATTRS, n, b"k");
            traces_with_one_span(&ld(F_SPAN_EVENTS, &event))
        },
    );
}

#[test]
fn span_link_attributes_per_level_cap() {
    assert_cap(
        prescan_traces,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| {
            let link = attributes(F_LINK_ATTRS, n, b"k");
            traces_with_one_span(&ld(F_SPAN_LINKS, &link))
        },
    );
}

// ===========================================================================
// Metrics — per-level caps
// ===========================================================================

#[test]
fn resource_metrics_per_level_cap() {
    assert_cap(
        prescan_metrics,
        MAX_RESOURCE_METRICS,
        "resource_metrics",
        |n| empty_repeated(F_REQ_ROOT, n),
    );
}

#[test]
fn scope_metrics_per_level_cap() {
    assert_cap(prescan_metrics, MAX_SCOPE_METRICS, "scope_metrics", |n| {
        ld(F_REQ_ROOT, &empty_repeated(F_RM_SCOPE_METRICS, n))
    });
}

#[test]
fn metrics_per_level_cap() {
    assert_cap(prescan_metrics, MAX_METRICS, "metrics", |n| {
        metrics_with_metrics(&empty_repeated(F_SM_METRICS, n))
    });
}

#[test]
fn metric_metadata_per_level_cap() {
    assert_cap(
        prescan_metrics,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| metrics_with_metrics(&one_metric(&attributes(F_METRIC_METADATA, n, b"k"))),
    );
}

#[test]
fn gauge_data_points_per_level_cap() {
    assert_cap(prescan_metrics, MAX_DATA_POINTS, "data_points", |n| {
        metrics_with_one_datapoint_container(F_METRIC_GAUGE, &empty_repeated(F_DATA_POINTS, n))
    });
}

#[test]
fn sum_data_points_per_level_cap() {
    assert_cap(prescan_metrics, MAX_DATA_POINTS, "data_points", |n| {
        metrics_with_one_datapoint_container(F_METRIC_SUM, &empty_repeated(F_DATA_POINTS, n))
    });
}

#[test]
fn histogram_data_points_per_level_cap() {
    assert_cap(prescan_metrics, MAX_DATA_POINTS, "data_points", |n| {
        metrics_with_one_datapoint_container(F_METRIC_HISTOGRAM, &empty_repeated(F_DATA_POINTS, n))
    });
}

#[test]
fn exponential_histogram_data_points_per_level_cap() {
    assert_cap(prescan_metrics, MAX_DATA_POINTS, "data_points", |n| {
        metrics_with_one_datapoint_container(
            F_METRIC_EXP_HISTOGRAM,
            &empty_repeated(F_DATA_POINTS, n),
        )
    });
}

#[test]
fn summary_data_points_per_level_cap() {
    assert_cap(prescan_metrics, MAX_DATA_POINTS, "data_points", |n| {
        metrics_with_one_datapoint_container(F_METRIC_SUMMARY, &empty_repeated(F_DATA_POINTS, n))
    });
}

#[test]
fn number_data_point_attributes_per_level_cap() {
    assert_cap(
        prescan_metrics,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| metrics_with_one_datapoint(F_METRIC_GAUGE, &attributes(F_NDP_ATTRS, n, b"k")),
    );
}

#[test]
fn number_data_point_exemplars_per_level_cap() {
    assert_cap(prescan_metrics, MAX_EXEMPLARS, "exemplars", |n| {
        metrics_with_one_datapoint(F_METRIC_GAUGE, &empty_repeated(F_NDP_EXEMPLARS, n))
    });
}

#[test]
fn histogram_data_point_attributes_per_level_cap() {
    assert_cap(
        prescan_metrics,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| metrics_with_one_datapoint(F_METRIC_HISTOGRAM, &attributes(F_HDP_ATTRS, n, b"k")),
    );
}

#[test]
fn histogram_data_point_exemplars_per_level_cap() {
    assert_cap(prescan_metrics, MAX_EXEMPLARS, "exemplars", |n| {
        metrics_with_one_datapoint(F_METRIC_HISTOGRAM, &empty_repeated(F_HDP_EXEMPLARS, n))
    });
}

#[test]
fn exponential_histogram_data_point_attributes_per_level_cap() {
    assert_cap(
        prescan_metrics,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| metrics_with_one_datapoint(F_METRIC_EXP_HISTOGRAM, &attributes(F_EHDP_ATTRS, n, b"k")),
    );
}

#[test]
fn exponential_histogram_data_point_exemplars_per_level_cap() {
    assert_cap(prescan_metrics, MAX_EXEMPLARS, "exemplars", |n| {
        metrics_with_one_datapoint(F_METRIC_EXP_HISTOGRAM, &empty_repeated(F_EHDP_EXEMPLARS, n))
    });
}

#[test]
fn summary_data_point_attributes_per_level_cap() {
    assert_cap(
        prescan_metrics,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| metrics_with_one_datapoint(F_METRIC_SUMMARY, &attributes(F_SDP_ATTRS, n, b"k")),
    );
}

#[test]
fn exemplar_filtered_attributes_per_level_cap() {
    assert_cap(
        prescan_metrics,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| {
            let exemplar = attributes(F_EXEMPLAR_ATTRS, n, b"k");
            metrics_with_one_datapoint(F_METRIC_GAUGE, &ld(F_NDP_EXEMPLARS, &exemplar))
        },
    );
}

// ---- packed scalar fan-out vectors ----

/// A packed `fixed64` (`double`/`fixed64`) blob with `elements` slots.
fn packed_fixed64(field: u32, elements: usize) -> Vec<u8> {
    ld(field, &vec![0u8; elements * 8])
}

/// A packed `varint` blob with `elements` single-byte (`0`) varints.
fn packed_varint(field: u32, elements: usize) -> Vec<u8> {
    ld(field, &vec![0u8; elements])
}

#[test]
fn histogram_bucket_counts_packed_cap() {
    assert_cap(prescan_metrics, MAX_BUCKETS, "bucket_counts", |n| {
        metrics_with_one_datapoint(F_METRIC_HISTOGRAM, &packed_fixed64(F_HDP_BUCKET_COUNTS, n))
    });
}

#[test]
fn histogram_explicit_bounds_packed_cap() {
    assert_cap(prescan_metrics, MAX_BUCKETS, "explicit_bounds", |n| {
        metrics_with_one_datapoint(
            F_METRIC_HISTOGRAM,
            &packed_fixed64(F_HDP_EXPLICIT_BOUNDS, n),
        )
    });
}

#[test]
fn exponential_histogram_bucket_counts_packed_varint_cap() {
    assert_cap(prescan_metrics, MAX_BUCKETS, "bucket_counts", |n| {
        let buckets = packed_varint(F_EH_BUCKETS_COUNTS, n);
        let dp = ld(F_EHDP_POSITIVE, &buckets);
        metrics_with_one_datapoint(F_METRIC_EXP_HISTOGRAM, &dp)
    });
}

// ---- singular mergeable messages accumulate across occurrences (finding 1) ----

/// Builds an `ExponentialHistogramDataPoint` with the `positive` (field 8)
/// singular `Buckets` repeated `occurrences` times, each carrying `per`
/// packed-varint `bucket_counts`, wrapped as one exponential-histogram metric.
fn exp_histogram_positive_split(occurrences: usize, per: usize) -> Vec<u8> {
    let one = ld(F_EHDP_POSITIVE, &packed_varint(F_EH_BUCKETS_COUNTS, per));
    metrics_with_one_datapoint(F_METRIC_EXP_HISTOGRAM, &one.repeat(occurrences))
}

#[test]
fn exponential_histogram_positive_buckets_merge_across_occurrences_rejected() {
    // `positive` is a SINGULAR `Buckets`; prost merges duplicate occurrences,
    // CONCATENATING their `bucket_counts`. Two occurrences each just over half
    // the cap are individually under it but sum past it — the pre-scan must
    // accumulate across occurrences (finding 1) and reject.
    let half = MAX_BUCKETS / 2 + 1;

    // Non-vacuity: a SINGLE occurrence of `half` counts is under the cap and
    // pre-scan-PASSES — so the reject below is attributable to the
    // cross-occurrence accumulation, not to either occurrence alone.
    prescan_metrics(&exp_histogram_positive_split(1, half))
        .expect("a single positive Buckets under the cap passes");

    // Two occurrences: 2*half = MAX_BUCKETS + 2 concatenated counts > cap.
    let body = exp_histogram_positive_split(2, half);
    match prescan_metrics(&body) {
        Err(LogsIngestError::OversizeMessage {
            field: "bucket_counts",
            limit,
            actual,
        }) => {
            assert_eq!(limit, MAX_BUCKETS);
            assert!(actual > MAX_BUCKETS, "accumulated {actual} must exceed cap");
        }
        other => panic!("split positive Buckets must be rejected, got {other:?}"),
    }

    // Proof the reject is non-vacuous: the SAME bytes DECODE cleanly (they are
    // well-formed), and prost MERGES the two `positive` occurrences into one
    // `Buckets` with 2*half > MAX_BUCKETS counts — the amplified vector the
    // pre-scan stops before `decode` materializes it.
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    let decoded = ExportMetricsServiceRequest::decode(body.as_slice()).expect("well-formed");
    let metric = &decoded.resource_metrics[0].scope_metrics[0].metrics[0];
    let Some(Data::ExponentialHistogram(eh)) = &metric.data else {
        panic!("expected exponential histogram");
    };
    let merged = eh.data_points[0]
        .positive
        .as_ref()
        .expect("positive merged");
    assert_eq!(
        merged.bucket_counts.len(),
        2 * half,
        "prost concatenates the split bucket_counts on merge"
    );
}

#[test]
fn exponential_histogram_positive_and_negative_accumulate_independently() {
    // `positive` (8) and `negative` (9) merge into DISTINCT `Buckets` structs,
    // so their counts must accumulate in disjoint per-parent slots: each at the
    // full cap in the same data point is in-bounds and pre-scan-PASSES (a shared
    // accumulator would wrongly sum them to 2*cap and reject).
    let positive = ld(
        F_EHDP_POSITIVE,
        &packed_varint(F_EH_BUCKETS_COUNTS, MAX_BUCKETS),
    );
    let negative = ld(9, &packed_varint(F_EH_BUCKETS_COUNTS, MAX_BUCKETS));
    let mut dp = positive;
    dp.extend_from_slice(&negative);
    let body = metrics_with_one_datapoint(F_METRIC_EXP_HISTOGRAM, &dp);
    prescan_metrics(&body).expect("positive and negative each at the cap accumulate independently");
}

#[test]
fn summary_quantile_values_cap() {
    assert_cap(prescan_metrics, MAX_QUANTILES, "quantile_values", |n| {
        metrics_with_one_datapoint(F_METRIC_SUMMARY, &empty_repeated(F_SDP_QUANTILES, n))
    });
}

#[test]
fn packed_bucket_counts_split_across_occurrences_still_rejected() {
    // A packed field may be split into several length-delimited blobs; the
    // per-level counter accumulates across them, so the split cannot evade the
    // cap (proto anti-evasion, packed analog of AC-9).
    let half = MAX_BUCKETS / 2 + 1;
    let mut dp = packed_fixed64(F_HDP_BUCKET_COUNTS, half);
    dp.extend_from_slice(&packed_fixed64(F_HDP_BUCKET_COUNTS, half)); // total > MAX_BUCKETS
    let body = metrics_with_one_datapoint(F_METRIC_HISTOGRAM, &dp);
    assert!(matches!(
        prescan_metrics(&body),
        Err(LogsIngestError::OversizeMessage {
            field: "bucket_counts",
            ..
        })
    ));
}

#[test]
fn unpacked_bucket_counts_are_counted_as_elements() {
    // The same numeric field encoded UNPACKED (one wire field per element)
    // must count identically — an attacker can't split a packed vector into
    // individual fixed64 fields to dodge the cap.
    let mut dp = Vec::new();
    let over = MAX_BUCKETS + 1;
    for _ in 0..over {
        put_varint(&mut dp, tag(F_HDP_BUCKET_COUNTS, 1)); // wire type 1 = fixed64
        dp.extend_from_slice(&0u64.to_le_bytes());
    }
    let body = metrics_with_one_datapoint(F_METRIC_HISTOGRAM, &dp);
    assert!(matches!(
        prescan_metrics(&body),
        Err(LogsIngestError::OversizeMessage {
            field: "bucket_counts",
            ..
        })
    ));
}

// ===========================================================================
// Logs — per-level caps
// ===========================================================================

#[test]
fn resource_logs_per_level_cap() {
    assert_cap(prescan_logs, MAX_RESOURCE_LOGS, "resource_logs", |n| {
        empty_repeated(F_REQ_ROOT, n)
    });
}

#[test]
fn scope_logs_per_level_cap() {
    assert_cap(prescan_logs, MAX_SCOPE_LOGS, "scope_logs", |n| {
        ld(F_REQ_ROOT, &empty_repeated(F_RL_SCOPE_LOGS, n))
    });
}

#[test]
fn log_records_per_level_cap() {
    assert_cap(prescan_logs, MAX_LOG_RECORDS, "log_records", |n| {
        logs_with_records(&empty_repeated(F_SL_LOG_RECORDS, n))
    });
}

#[test]
fn log_record_attributes_per_level_cap() {
    assert_cap(
        prescan_logs,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| {
            let record = attributes(F_LR_ATTRS, n, b"k");
            logs_with_records(&ld(F_SL_LOG_RECORDS, &record))
        },
    );
}

// ===========================================================================
// Shared leaf types
// ===========================================================================

#[test]
fn resource_attributes_per_level_cap() {
    // Resource is reachable via ResourceSpans.resource (field 1).
    assert_cap(
        prescan_traces,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| {
            let resource = attributes(F_RESOURCE_ATTRS, n, b"k");
            let rs = ld(F_RESOURCE, &resource);
            ld(F_REQ_ROOT, &rs)
        },
    );
}

#[test]
fn resource_entity_refs_per_level_cap() {
    assert_cap(prescan_traces, MAX_ENTITY_REFS, "entity_refs", |n| {
        let resource = empty_repeated(F_RESOURCE_ENTITY_REFS, n);
        ld(F_REQ_ROOT, &ld(F_RESOURCE, &resource))
    });
}

#[test]
fn resource_entity_refs_merge_across_occurrences_rejected() {
    // `Resource` is SINGULAR under `ResourceSpans.resource`; prost merges
    // duplicate occurrences, concatenating their `entity_refs`. `entity_refs`
    // has NO cross-request aggregate, so cross-occurrence accumulation (finding
    // 1) is the ONLY thing bounding a split — two occurrences each just over
    // half the cap sum past it.
    let half = MAX_ENTITY_REFS / 2 + 1;
    let resource = empty_repeated(F_RESOURCE_ENTITY_REFS, half);
    let one_resource = ld(F_RESOURCE, &resource);

    // Non-vacuity: one resource with `half` entity_refs is under the cap.
    prescan_traces(&ld(F_REQ_ROOT, &one_resource)).expect("a single resource under the cap passes");

    // Two `resource` occurrences within one ResourceSpans: 2*half > cap.
    let body = ld(F_REQ_ROOT, &one_resource.repeat(2));
    match prescan_traces(&body) {
        Err(LogsIngestError::OversizeMessage {
            field: "entity_refs",
            limit,
            actual,
        }) => {
            assert_eq!(limit, MAX_ENTITY_REFS);
            assert!(actual > MAX_ENTITY_REFS);
        }
        other => panic!("split resource entity_refs must be rejected, got {other:?}"),
    }
}

#[test]
fn resource_attributes_merge_across_occurrences_rejected() {
    // Duplicate `resource` occurrences also merge their `attributes`; the
    // per-level cap must see the accumulated total, not each occurrence alone.
    let half = MAX_ATTRIBUTES_PER_ELEMENT / 2 + 1;
    let one_resource = ld(F_RESOURCE, &attributes(F_RESOURCE_ATTRS, half, b"k"));

    prescan_traces(&ld(F_REQ_ROOT, &one_resource))
        .expect("a single resource under the attribute cap passes");

    let body = ld(F_REQ_ROOT, &one_resource.repeat(2));
    match prescan_traces(&body) {
        Err(LogsIngestError::OversizeMessage {
            field: "attributes",
            limit,
            actual,
        }) => {
            assert_eq!(limit, MAX_ATTRIBUTES_PER_ELEMENT);
            assert!(actual > MAX_ATTRIBUTES_PER_ELEMENT);
        }
        other => panic!("split resource attributes must be rejected, got {other:?}"),
    }
}

#[test]
fn entity_ref_id_keys_per_level_cap() {
    assert_cap(
        prescan_traces,
        MAX_ENTITY_REF_KEYS,
        "entity_ref_keys",
        |n| {
            // one EntityRef with n id_keys (repeated string)
            let entity_ref = empty_repeated(F_ENTITYREF_ID_KEYS, n);
            let resource = ld(F_RESOURCE_ENTITY_REFS, &entity_ref);
            ld(F_REQ_ROOT, &ld(F_RESOURCE, &resource))
        },
    );
}

#[test]
fn entity_ref_description_keys_per_level_cap() {
    assert_cap(
        prescan_traces,
        MAX_ENTITY_REF_KEYS,
        "entity_ref_keys",
        |n| {
            let entity_ref = empty_repeated(F_ENTITYREF_DESC_KEYS, n);
            let resource = ld(F_RESOURCE_ENTITY_REFS, &entity_ref);
            ld(F_REQ_ROOT, &ld(F_RESOURCE, &resource))
        },
    );
}

#[test]
fn instrumentation_scope_attributes_per_level_cap() {
    // InstrumentationScope reachable via ScopeSpans.scope (field 1).
    assert_cap(
        prescan_traces,
        MAX_ATTRIBUTES_PER_ELEMENT,
        "attributes",
        |n| {
            let scope = attributes(F_SCOPE_ATTRS, n, b"k");
            let scope_spans = ld(F_SCOPE, &scope);
            let rs = ld(F_RS_SCOPE_SPANS, &scope_spans);
            ld(F_REQ_ROOT, &rs)
        },
    );
}

// ===========================================================================
// AnyValue nesting depth (wire pre-scan's own depth bound)
// ===========================================================================

/// An `AnyValue` wire message nested `levels` deep: a scalar leaf wrapped in
/// `levels - 1` `ArrayValue` containers. `levels == 1` is a bare scalar.
fn nested_array_av(levels: usize) -> Vec<u8> {
    let mut av = ld(F_AV_STRING, b"x"); // scalar AnyValue, one node
    for _ in 1..levels {
        let array = ld(F_ARRAY_VALUES, &av); // ArrayValue { values: [av] }
        av = ld(F_AV_ARRAY, &array); // AnyValue { array_value: array }
    }
    av
}

/// The kvlist analog: a scalar wrapped in `levels - 1` `KvlistValue`
/// containers (each via a `KeyValue.value`).
fn nested_kvlist_av(levels: usize) -> Vec<u8> {
    let mut av = ld(F_AV_STRING, b"x");
    for _ in 1..levels {
        let kv = ld(F_KV_VALUE, &av); // KeyValue { value: av }
        let kvlist = ld(F_KVLIST_VALUES, &kv); // KeyValueList { values: [kv] }
        av = ld(F_AV_KVLIST, &kvlist); // AnyValue { kvlist_value: kvlist }
    }
    av
}

/// Places an `AnyValue` as a span attribute's value.
fn traces_with_attr_value(av: &[u8]) -> Vec<u8> {
    let kv = ld(F_KV_VALUE, av); // KeyValue.value = av
    let span = ld(F_SPAN_ATTRS, &kv);
    traces_with_one_span(&span)
}

#[test]
fn anyvalue_array_nesting_at_the_cap_passes() {
    let body = traces_with_attr_value(&nested_array_av(MAX_ANYVALUE_DEPTH));
    prescan_traces(&body).expect("exactly MAX_ANYVALUE_DEPTH nesting is accepted");
}

#[test]
fn anyvalue_array_nesting_one_past_the_cap_rejected() {
    let body = traces_with_attr_value(&nested_array_av(MAX_ANYVALUE_DEPTH + 1));
    match prescan_traces(&body) {
        Err(LogsIngestError::OversizeMessage {
            field: "AnyValue nesting depth",
            limit,
            actual,
        }) => {
            assert_eq!(limit, MAX_ANYVALUE_DEPTH);
            assert_eq!(actual, MAX_ANYVALUE_DEPTH + 1);
        }
        other => panic!("expected depth reject, got {other:?}"),
    }
}

#[test]
fn anyvalue_kvlist_nesting_one_past_the_cap_rejected() {
    let body = traces_with_attr_value(&nested_kvlist_av(MAX_ANYVALUE_DEPTH + 1));
    assert!(matches!(
        prescan_traces(&body),
        Err(LogsIngestError::OversizeMessage {
            field: "AnyValue nesting depth",
            ..
        })
    ));
}

#[test]
fn log_record_body_deep_nesting_rejected() {
    // Depth guard on the LogRecord.body path (singular AnyValue, not an
    // attribute).
    let body_av = nested_array_av(MAX_ANYVALUE_DEPTH + 1);
    let record = ld(F_LR_BODY, &body_av);
    let request = logs_with_records(&ld(F_SL_LOG_RECORDS, &record));
    assert!(matches!(
        prescan_logs(&request),
        Err(LogsIngestError::OversizeMessage {
            field: "AnyValue nesting depth",
            ..
        })
    ));
}

#[test]
fn a_very_deep_wire_chain_is_rejected_without_stack_overflow() {
    // Far deeper than the walk's frame stack; the iterative walker rejects it
    // (depth or the MAX_WIRE_DEPTH backstop) without recursing.
    let body = traces_with_attr_value(&nested_array_av(10_000));
    assert!(matches!(
        prescan_traces(&body),
        Err(LogsIngestError::OversizeMessage {
            field: "AnyValue nesting depth",
            ..
        })
    ));
}

// ===========================================================================
// Aggregate (cross-request shared, monotonic) caps
// ===========================================================================

#[test]
fn total_spans_aggregate_cap_across_scopes() {
    // Each ScopeSpans stays at MAX_SPANS (per-level OK), but their sum exceeds
    // MAX_TOTAL_SPANS — only the shared aggregate counter catches it.
    let full_scope = ld(F_RS_SCOPE_SPANS, &empty_repeated(F_SS_SPANS, MAX_SPANS));
    let scopes = MAX_TOTAL_SPANS / MAX_SPANS + 1;
    let resource_spans = full_scope.repeat(scopes);
    let request = ld(F_REQ_ROOT, &resource_spans);
    assert!(matches!(
        prescan_traces(&request),
        Err(LogsIngestError::OversizeMessage {
            field: "total spans",
            ..
        })
    ));
}

#[test]
fn total_attributes_aggregate_cap_across_spans() {
    // Each span carries MAX_ATTRIBUTES_PER_ELEMENT attrs (per-level OK); their
    // sum over enough spans exceeds MAX_TOTAL_ATTRIBUTES.
    let span = ld(
        F_SS_SPANS,
        &attributes(F_SPAN_ATTRS, MAX_ATTRIBUTES_PER_ELEMENT, b"k"),
    );
    let spans = MAX_TOTAL_ATTRIBUTES / MAX_ATTRIBUTES_PER_ELEMENT + 1;
    let request = traces_with_spans(&span.repeat(spans));
    assert!(matches!(
        prescan_traces(&request),
        Err(LogsIngestError::OversizeMessage {
            field: "total attributes",
            ..
        })
    ));
}

#[test]
fn total_data_points_aggregate_cap_across_metrics() {
    let metric = ld(
        F_SM_METRICS,
        &ld(
            F_METRIC_GAUGE,
            &empty_repeated(F_DATA_POINTS, MAX_DATA_POINTS),
        ),
    );
    let metrics = MAX_TOTAL_DATA_POINTS / MAX_DATA_POINTS + 1;
    let request = metrics_with_metrics(&metric.repeat(metrics));
    assert!(matches!(
        prescan_metrics(&request),
        Err(LogsIngestError::OversizeMessage {
            field: "total data points",
            ..
        })
    ));
}

#[test]
fn total_anyvalue_elements_aggregate_cap() {
    // ArrayValue.values has NO per-level cap — the aggregate is its ONLY count
    // bound (depth is the other). A single wide array past MAX_ANYVALUE_ELEMENTS
    // is rejected by the aggregate.
    let elements = empty_repeated(F_ARRAY_VALUES, MAX_ANYVALUE_ELEMENTS + 1);
    let array = ld(F_AV_ARRAY, &elements);
    let request = traces_with_attr_value(&array);
    assert!(matches!(
        prescan_traces(&request),
        Err(LogsIngestError::OversizeMessage {
            field: "total AnyValue elements",
            ..
        })
    ));
}

#[test]
fn aggregate_accepts_cross_parent_under_cap() {
    // Two scopes each with a handful of spans — well under every per-level and
    // aggregate cap — pre-scan-passes, proving the aggregate reject above is
    // specific to exceeding the cap, not to any multi-parent shape.
    let scope = ld(F_RS_SCOPE_SPANS, &empty_repeated(F_SS_SPANS, 8));
    let request = ld(F_REQ_ROOT, &scope.repeat(2));
    prescan_traces(&request).expect("small cross-parent payload is within caps");
}

// ===========================================================================
// Duplicate-key anti-evasion (counts raw occurrences, not distinct keys)
// ===========================================================================

#[test]
fn duplicate_key_attributes_still_rejected() {
    // MAX_ATTRIBUTES_PER_ELEMENT + 1 attributes ALL with the identical key —
    // a dedup-collapsing counter would see one key and wrongly accept.
    let body = traces_with_one_span(&attributes(
        F_SPAN_ATTRS,
        MAX_ATTRIBUTES_PER_ELEMENT + 1,
        b"same-key",
    ));
    assert!(matches!(
        prescan_traces(&body),
        Err(LogsIngestError::OversizeMessage {
            field: "attributes",
            ..
        })
    ));
}

// ===========================================================================
// Malformed wire is deferred to prost (not an oversize reject)
// ===========================================================================

#[test]
fn malformed_wire_is_deferred_not_rejected() {
    // A truncated length-delimited field: the pre-scan bails to Ok, leaving the
    // canonical prost DecodeError to the subsequent decode.
    let mut bad = Vec::new();
    put_varint(&mut bad, tag(F_REQ_ROOT, 2));
    put_varint(&mut bad, 100); // claims 100 bytes, provides none
    assert!(prescan_traces(&bad).is_ok());
    assert!(prescan_metrics(&bad).is_ok());
    assert!(prescan_logs(&bad).is_ok());
}

#[test]
fn overcap_prefix_then_malformed_tail_defers_to_prost() {
    // Round-2 finding (#115): malformed-wire classification takes PRECEDENCE
    // over an over-cap prefix. The pre-scan records the cap violation but keeps
    // walking the (allocation-free, depth-bounded) structure to the end; a
    // malformed field after the over-cap prefix makes it DEFER to prost's
    // canonical decode error rather than surface OversizeMessage — and it does
    // so WITHOUT materializing the over-cap structure (decode stays skipped).
    let mut body = empty_repeated(F_REQ_ROOT, MAX_RESOURCE_SPANS + 1); // valid, over cap
    // Append a truncated resource_spans field the continued walk now reaches.
    put_varint(&mut body, tag(F_REQ_ROOT, 2));
    put_varint(&mut body, 100); // claims 100 bytes, provides none (malformed)

    // Non-vacuous: under the OLD immediate-Reject control flow this returned
    // OversizeMessage; the record-and-continue fix defers to prost instead.
    assert!(
        prescan_traces(&body).is_ok(),
        "over-cap prefix + malformed tail must defer to prost, not reclassify as oversize"
    );

    // And prost does reject the same bytes (canonical decode error) — the
    // classification the pre-scan correctly leaves to decode.
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    assert!(ExportTraceServiceRequest::decode(body.as_slice()).is_err());
}

#[test]
fn overcap_prefix_wellformed_to_end_still_rejected() {
    // The counterpart to malformed precedence: a body that is well-formed all
    // the way to the end still surfaces the recorded cap violation. Extra valid
    // resource_spans AFTER the over-cap point prove the reject survives the
    // continued record-and-continue walk (it is not lost by walking past the
    // first violation).
    let mut body = empty_repeated(F_REQ_ROOT, MAX_RESOURCE_SPANS + 1); // over cap
    body.extend_from_slice(&empty_repeated(F_REQ_ROOT, 4)); // well-formed tail
    match prescan_traces(&body) {
        Err(LogsIngestError::OversizeMessage {
            field: "resource_spans",
            limit,
            actual,
        }) => {
            assert_eq!(limit, MAX_RESOURCE_SPANS);
            assert!(actual > MAX_RESOURCE_SPANS);
        }
        other => panic!("well-formed over-cap body must reject as oversize, got {other:?}"),
    }
}

#[test]
fn wellformed_in_bounds_body_passes() {
    // Control for the two cases above: a well-formed, in-bounds body neither
    // records a reject nor meets malformed wire, so the record-and-continue walk
    // completes and returns Ok.
    let body = empty_repeated(F_REQ_ROOT, 4);
    prescan_traces(&body).expect("well-formed in-bounds body passes");
}

#[test]
fn malformed_before_any_overcap_is_deferred() {
    // The contract's other half: a malformed field encountered BEFORE any cap
    // is exceeded bails to Ok (deferred to prost) — the pre-scan only ever ADDS
    // an oversize reject, never reclassifies an otherwise-in-bounds malformed
    // body. Here a single (in-bounds) resource_spans is followed by a truncated
    // field; no cap is tripped, so the scan defers.
    let mut body = empty_repeated(F_REQ_ROOT, 1); // one valid resource_spans, under cap
    put_varint(&mut body, tag(F_REQ_ROOT, 2));
    put_varint(&mut body, 100); // truncated
    assert!(
        prescan_traces(&body).is_ok(),
        "malformed-before-overcap defers"
    );
}

/// A single `KeyValue` attribute (field `F_SPAN_ATTRS`) whose inner wire is
/// TRUNCATED: the outer length-delimited frame is valid (so the parent reaches
/// it and, once its counter is blown, must descend), but the `KeyValue` payload
/// declares a 100-byte field with no bytes — malformed wire the descent meets.
fn malformed_inner_attribute() -> Vec<u8> {
    let mut bad_kv = Vec::new();
    put_varint(&mut bad_kv, tag(F_KV_KEY, 2)); // field 1, wire type 2
    put_varint(&mut bad_kv, 100); // claims 100 bytes, provides none (truncated)
    ld(F_SPAN_ATTRS, &bad_kv)
}

#[test]
fn malformed_wire_inside_triggering_overcap_child_defers_to_prost() {
    // Round-3 finding (#115): the over-cap element that TRIPS the per-level cap
    // itself carries malformed inner wire. Under the round-2 skip-descent flow
    // `charge` rejected BEFORE `descend`, so this malformed child was never
    // scanned and the pre-scan wrongly surfaced OversizeMessage. Decoupling cap
    // accounting from the structural scan descends into the over-cap child,
    // meets the malformed wire, and DEFERS to prost.
    let mut attrs = empty_repeated(F_SPAN_ATTRS, MAX_ATTRIBUTES_PER_ELEMENT); // at cap
    attrs.extend_from_slice(&malformed_inner_attribute()); // the +1 that trips the cap
    let body = traces_with_one_span(&attrs);

    // Non-vacuous: under the OLD skip-descent flow this returned OversizeMessage;
    // the decouple-cap-from-scan fix defers to prost instead.
    assert!(
        prescan_traces(&body).is_ok(),
        "malformed wire inside the triggering over-cap child must defer to prost"
    );

    // prost does reject the same bytes (canonical decode error) — the
    // classification the pre-scan correctly leaves to decode.
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    assert!(ExportTraceServiceRequest::decode(body.as_slice()).is_err());
}

#[test]
fn malformed_wire_inside_subsequent_overcap_child_defers_to_prost() {
    // The counter is ALREADY frozen past the cap by the time the malformed child
    // is reached. Under skip-descent, every same-field child after the first
    // violation was skipped too — so malformed wire in a SUBSEQUENT over-cap
    // child was never seen. The decoupled scan descends into it regardless of
    // the frozen counter and defers to prost.
    let mut attrs = empty_repeated(F_SPAN_ATTRS, MAX_ATTRIBUTES_PER_ELEMENT + 1); // over cap; counter frozen
    attrs.extend_from_slice(&malformed_inner_attribute()); // a further, already-over-cap child
    let body = traces_with_one_span(&attrs);

    assert!(
        prescan_traces(&body).is_ok(),
        "malformed wire inside a subsequent over-cap child must defer to prost"
    );

    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    assert!(ExportTraceServiceRequest::decode(body.as_slice()).is_err());
}

#[test]
fn overcap_children_wellformed_to_end_still_rejected() {
    // The counterpart to the two malformed cases: an over-cap repeated field
    // whose children are all well-formed to the very end (each `KeyValue`
    // carries a real key) still surfaces the recorded cap violation. The
    // decoupled scan now DESCENDS into every over-cap child, finds them clean,
    // and completes — proving the descent does not drop the recorded reject.
    let attrs = attributes(F_SPAN_ATTRS, MAX_ATTRIBUTES_PER_ELEMENT + 1, b"k"); // over cap, all well-formed
    let body = traces_with_one_span(&attrs);
    match prescan_traces(&body) {
        Err(LogsIngestError::OversizeMessage {
            field: "attributes",
            limit,
            actual,
        }) => {
            assert_eq!(limit, MAX_ATTRIBUTES_PER_ELEMENT);
            assert!(actual > MAX_ATTRIBUTES_PER_ELEMENT);
        }
        other => panic!("well-formed over-cap children must reject as oversize, got {other:?}"),
    }
}

/// A packed-varint `bucket_counts` blob of `valid_elements` well-formed
/// single-byte varints followed by a TRUNCATED varint (a lone `0x80`
/// continuation byte with nothing after it) — malformed wire the pre-scan meets
/// only if it walks the packed blob to the end.
fn packed_varint_malformed_tail(field: u32, valid_elements: usize) -> Vec<u8> {
    let mut blob = vec![0u8; valid_elements]; // `valid_elements` zero varints
    blob.push(0x80); // continuation bit set, no following byte → truncated
    ld(field, &blob)
}

#[test]
fn overcap_packed_varint_bucket_counts_with_malformed_tail_defers_to_prost() {
    // Round-4 finding (#115): the packed-varint charge path. An over-cap
    // `bucket_counts` blob whose trailing varint is TRUNCATED must defer to
    // prost, not surface OversizeMessage. Under the round-3 short-circuit
    // `count_packed_varints` stopped at the cap and never saw the malformed tail,
    // so the pre-scan wrongly returned OversizeMessage. Walking the whole blob
    // meets the truncated varint and DEFERS.
    let buckets = packed_varint_malformed_tail(F_EH_BUCKETS_COUNTS, MAX_BUCKETS + 1);
    let dp = ld(F_EHDP_POSITIVE, &buckets);
    let body = metrics_with_one_datapoint(F_METRIC_EXP_HISTOGRAM, &dp);

    // Non-vacuous: under the OLD short-circuit this returned OversizeMessage; the
    // walk-to-end fix defers to prost instead.
    assert!(
        prescan_metrics(&body).is_ok(),
        "over-cap packed bucket_counts with a malformed trailing varint must defer to prost"
    );

    // prost does reject the same bytes (canonical decode error) — the truncated
    // packed varint the pre-scan correctly leaves to decode.
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    assert!(ExportMetricsServiceRequest::decode(body.as_slice()).is_err());
}

#[test]
fn overcap_packed_varint_bucket_counts_wellformed_still_rejected() {
    // The counterpart: an over-cap packed `bucket_counts` blob that is
    // well-formed to its end still surfaces the recorded cap violation (the
    // walk-to-end fix must not drop the reject for a clean over-cap blob).
    let buckets = packed_varint(F_EH_BUCKETS_COUNTS, MAX_BUCKETS + 1);
    let dp = ld(F_EHDP_POSITIVE, &buckets);
    let body = metrics_with_one_datapoint(F_METRIC_EXP_HISTOGRAM, &dp);
    match prescan_metrics(&body) {
        Err(LogsIngestError::OversizeMessage {
            field: "bucket_counts",
            limit,
            actual,
        }) => {
            assert_eq!(limit, MAX_BUCKETS);
            assert!(actual > MAX_BUCKETS);
        }
        other => panic!("well-formed over-cap packed bucket_counts must reject, got {other:?}"),
    }
}

/// A single `SummaryDataPoint.quantile_values` (`ValueAtQuantile`) occurrence
/// whose inner wire is TRUNCATED: the outer length-delimited frame is valid, but
/// the inner `ValueAtQuantile` declares a 100-byte length-delimited field with
/// no bytes — malformed wire the CountedMessage descent meets.
fn malformed_inner_quantile() -> Vec<u8> {
    let mut bad = Vec::new();
    put_varint(&mut bad, tag(1, 2)); // field 1, wire type 2 (truncated below)
    put_varint(&mut bad, 100); // claims 100 bytes, provides none
    ld(F_SDP_QUANTILES, &bad)
}

#[test]
fn overcap_counted_quantile_values_with_malformed_inner_defers_to_prost() {
    // Round-4 finding (#115): the CountedMessage charge path. An over-cap
    // `quantile_values` whose triggering occurrence carries malformed inner wire
    // must defer to prost. Under the round-3 flow `charge?` rejected BEFORE any
    // descent (CountedMessage never descended), so this malformed child was never
    // scanned and the pre-scan wrongly returned OversizeMessage. Descending into
    // the length-validated child meets the malformed wire and DEFERS.
    let mut quantiles = empty_repeated(F_SDP_QUANTILES, MAX_QUANTILES); // at cap
    quantiles.extend_from_slice(&malformed_inner_quantile()); // the +1 that trips the cap
    let body = metrics_with_one_datapoint(F_METRIC_SUMMARY, &quantiles);

    // Non-vacuous: under the OLD skip-descent flow this returned OversizeMessage;
    // the record-and-descend fix defers to prost instead.
    assert!(
        prescan_metrics(&body).is_ok(),
        "over-cap quantile_values with malformed inner wire must defer to prost"
    );

    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    assert!(ExportMetricsServiceRequest::decode(body.as_slice()).is_err());
}

#[test]
fn overcap_counted_quantile_values_wellformed_still_rejected() {
    // The counterpart: an over-cap `quantile_values` whose occurrences are all
    // well-formed (empty ValueAtQuantile messages) still surfaces the recorded
    // cap violation — the added descent must not drop the reject.
    let quantiles = empty_repeated(F_SDP_QUANTILES, MAX_QUANTILES + 1); // over cap, all valid
    let body = metrics_with_one_datapoint(F_METRIC_SUMMARY, &quantiles);
    match prescan_metrics(&body) {
        Err(LogsIngestError::OversizeMessage {
            field: "quantile_values",
            limit,
            actual,
        }) => {
            assert_eq!(limit, MAX_QUANTILES);
            assert!(actual > MAX_QUANTILES);
        }
        other => panic!("well-formed over-cap quantile_values must reject, got {other:?}"),
    }
}

#[test]
fn empty_body_passes() {
    prescan_traces(&[]).expect("empty");
    prescan_metrics(&[]).expect("empty");
    prescan_logs(&[]).expect("empty");
}

// ===========================================================================
// Positive: a legitimate small request pre-scans AND decodes unchanged
// ===========================================================================

#[test]
fn legitimate_traces_request_passes_and_decodes() {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("svc".to_string())),
                    }),
                    key_strindex: 0,
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    name: "op".to_string(),
                    attributes: vec![KeyValue {
                        key: "http.method".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("GET".to_string())),
                        }),
                        key_strindex: 0,
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let bytes = req.encode_to_vec();
    prescan_traces(&bytes).expect("legitimate request pre-scans clean");
    let decoded = ExportTraceServiceRequest::decode(bytes.as_slice()).expect("decodes");
    assert_eq!(decoded.resource_spans.len(), 1);
}
