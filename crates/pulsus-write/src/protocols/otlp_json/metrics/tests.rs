//! DoS proving tests for the bounded OTLP/JSON metrics decode (issue #115
//! track 6c), mirroring `otlp_json/tests.rs` (6a) / `otlp_json/logs_tests.rs`
//! (6b) at the same per-level / aggregate / depth thresholds. Every reject is
//! proven NON-VACUOUS: the matching in-bounds body parses `Ok`, and the
//! reject fires DURING deserialization (a `serde` error ->
//! [`LogsIngestError::DecodeJson`]). Shared building blocks (`AnyValueSeed`,
//! `ResourceSeed`, `InstrumentationScopeSeed`, `KeyValueSeed`, `AccumSeq`,
//! `buffer_scalar_or_skip`, `finish_via_derive`) are reused verbatim from
//! track 6a and are exhaustively cap/alias/duplicate-key tested there — this
//! file covers only the METRICS-specific graph (`resourceMetrics`/
//! `scopeMetrics`/`metrics`/the 5-way `data` oneof/data-point shapes) plus the
//! `crates/pulsus-write/tests/otlp_json_equivalence.rs` and
//! `otlp_json_vendor_patch.rs` green gates.
//!
//! # Scope of the exact-cap PAIRED tests
//!
//! Every DISTINCT `MAX_*` per-level constant reachable from the metrics graph
//! gets a full at-cap-accepts / cap+1-rejects pair, using a REPRESENTATIVE
//! field for constants shared by multiple structurally-identical call sites
//! (e.g. `dataPoints` on the five `Metric.data` arms all share
//! `MAX_DATA_POINTS` via the identical `accumulate_msgs` call — the gauge arm
//! is the pair's representative; the other four arms get a cap+1-REJECT-only
//! test proving their own call site is wired, since the accept-path logic is
//! identical code already proven by the gauge pair). Every occurrence still
//! gets independent non-vacuous reject coverage.

use crate::error::LogsIngestError;
use crate::protocols::otlp_metrics::decode_json;
use crate::protocols::otlp_prescan::{
    MAX_ANYVALUE_DEPTH, MAX_ANYVALUE_ELEMENTS, MAX_ATTRIBUTES_PER_ELEMENT, MAX_BUCKETS,
    MAX_DATA_POINTS, MAX_EXEMPLARS, MAX_METRICS, MAX_QUANTILES, MAX_RESOURCE_METRICS,
    MAX_SCOPE_METRICS, MAX_TOTAL_DATA_POINTS, MAX_TOTAL_EXEMPLARS,
};

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/// A JSON array literal of `n` copies of `elem` (no trailing comma).
fn arr(elem: &str, n: usize) -> String {
    let mut body = String::with_capacity((elem.len() + 1) * n + 2);
    body.push('[');
    if n > 0 {
        let mut chunk = String::with_capacity(elem.len() + 1);
        chunk.push_str(elem);
        chunk.push(',');
        body.push_str(&chunk.repeat(n));
        body.pop(); // drop the trailing comma
    }
    body.push(']');
    body
}

/// Wrap a `metrics` array literal into one resourceMetrics/scopeMetrics
/// envelope.
fn one_scope(metrics_json: &str) -> String {
    format!(r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":{metrics_json}}}]}}]}}"#)
}

/// Wrap a single metric object into a full request.
fn one_metric(metric_json: &str) -> String {
    one_scope(&format!("[{metric_json}]"))
}

/// A minimal Gauge metric with `n` empty (`{}`) `dataPoints`.
fn gauge_metric_with_points(n: usize) -> String {
    format!(
        r#"{{"name":"m","gauge":{{"dataPoints":{}}}}}"#,
        arr("{}", n)
    )
}

/// A Gauge metric with exactly one data point, itself given by `point_json`.
fn one_gauge_point(point_json: &str) -> String {
    format!(r#"{{"name":"m","gauge":{{"dataPoints":[{point_json}]}}}}"#)
}

fn assert_ok(body: &str) {
    decode_json(body.as_bytes())
        .unwrap_or_else(|e| panic!("expected Ok, got {e:?}\nbody prefix: {:.120}", body));
}

/// Assert the body is rejected during decode as a `DecodeJson` (400 / code 3),
/// returning the message so the caller can prove the reject is the
/// bounded-seed one (non-vacuity vs. an unrelated parse error).
fn reject_message(body: &str) -> String {
    match decode_json(body.as_bytes()).expect_err("expected a bounded-decode reject") {
        LogsIngestError::DecodeJson(e) => e.to_string(),
        other => panic!("expected DecodeJson, got {other:?}"),
    }
}

fn assert_rejects_with(body: &str, needle: &str) {
    let msg = reject_message(body);
    assert!(
        msg.contains(needle),
        "reject message {msg:?} must mention {needle:?} (non-vacuity)"
    );
}

fn nested_array_value(levels: usize) -> String {
    let mut v = r#"{"stringValue":"leaf"}"#.to_string();
    for _ in 0..levels {
        v = format!(r#"{{"arrayValue":{{"values":[{v}]}}}}"#);
    }
    v
}

// --------------------------------------------------------------------------
// Positive: no regression + both container spellings accepted
// --------------------------------------------------------------------------

#[test]
fn in_bounds_metrics_json_parses_with_every_metric_type() {
    let gauge = r#"{
        "name":"temperature","metadata":[{"key":"k","value":{"stringValue":"v"}}],
        "gauge":{"dataPoints":[{"attributes":[{"key":"host","value":{"stringValue":"h1"}}],
            "timeUnixNano":"1","exemplars":[{"filteredAttributes":[],"timeUnixNano":"1",
            "asDouble":"NaN"}],"asDouble":21.5}]}
    }"#;
    let sum = r#"{
        "name":"requests","sum":{"dataPoints":[{"timeUnixNano":"2","asInt":"5"}],
            "aggregationTemporality":2,"isMonotonic":true}
    }"#;
    let histogram = r#"{
        "name":"latency","histogram":{"dataPoints":[{"timeUnixNano":"3","count":"3",
            "sum":"NaN","bucketCounts":["1","2"],"explicitBounds":["Infinity"],
            "exemplars":[{"timeUnixNano":"3","asDouble":"Infinity"}]}],
            "aggregationTemporality":2}
    }"#;
    let exponential_histogram = r#"{
        "name":"exp","exponentialHistogram":{"dataPoints":[{"timeUnixNano":"4","count":"6",
            "scale":2,"zeroCount":"1","positive":{"offset":1,"bucketCounts":["1","2"]},
            "negative":{"offset":-1,"bucketCounts":["3"]}}],"aggregationTemporality":2}
    }"#;
    let summary = r#"{
        "name":"quantiles","summary":{"dataPoints":[{"timeUnixNano":"5",
            "quantileValues":[{"quantile":0.5,"value":1.0}]}]}
    }"#;
    let metrics = format!("[{gauge},{sum},{histogram},{exponential_histogram},{summary}]");
    assert_ok(&one_scope(&metrics));
}

#[test]
fn container_fields_accept_both_camel_and_snake_case_spellings() {
    let snake = r#"{"resource_metrics":[{"scope_metrics":[{"metrics":[
        {"name":"m","exponential_histogram":{"data_points":[{
            "positive":{"bucket_counts":["1"]},
            "negative":{"bucket_counts":["1"]}
        }]}}
    ]}]}]}"#;
    assert_ok(snake);

    let camel = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[
        {"name":"m","exponentialHistogram":{"dataPoints":[{
            "positive":{"bucketCounts":["1"]},
            "negative":{"bucketCounts":["1"]}
        }]}}
    ]}]}]}"#;
    assert_ok(camel);
}

// --------------------------------------------------------------------------
// Per-level cap: over-cap rejects (non-vacuous, one per field)
// --------------------------------------------------------------------------

#[test]
fn resource_metrics_over_per_level_cap_rejects() {
    let body = format!(
        r#"{{"resourceMetrics":{}}}"#,
        arr(r#"{"scopeMetrics":[]}"#, MAX_RESOURCE_METRICS + 1)
    );
    assert_rejects_with(&body, "resourceMetrics");
}

#[test]
fn scope_metrics_over_per_level_cap_rejects() {
    let body = format!(
        r#"{{"resourceMetrics":[{{"scopeMetrics":{}}}]}}"#,
        arr(r#"{"metrics":[]}"#, MAX_SCOPE_METRICS + 1)
    );
    assert_rejects_with(&body, "scopeMetrics");
}

#[test]
fn metrics_over_per_level_cap_rejects() {
    let body = one_scope(&arr(r#"{"name":"m"}"#, MAX_METRICS + 1));
    assert_rejects_with(&body, "metrics");
}

#[test]
fn metric_metadata_over_per_level_cap_rejects() {
    let metric = format!(
        r#"{{"name":"m","metadata":{}}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1)
    );
    assert_rejects_with(&one_metric(&metric), "attributes");
}

#[test]
fn gauge_data_points_over_per_level_cap_rejects() {
    assert_rejects_with(
        &one_metric(&gauge_metric_with_points(MAX_DATA_POINTS + 1)),
        "dataPoints",
    );
}

#[test]
fn sum_data_points_over_per_level_cap_rejects() {
    let metric = format!(
        r#"{{"name":"m","sum":{{"dataPoints":{}}}}}"#,
        arr("{}", MAX_DATA_POINTS + 1)
    );
    assert_rejects_with(&one_metric(&metric), "dataPoints");
}

#[test]
fn histogram_data_points_over_per_level_cap_rejects() {
    let metric = format!(
        r#"{{"name":"m","histogram":{{"dataPoints":{}}}}}"#,
        arr("{}", MAX_DATA_POINTS + 1)
    );
    assert_rejects_with(&one_metric(&metric), "dataPoints");
}

#[test]
fn exponential_histogram_data_points_over_per_level_cap_rejects() {
    let metric = format!(
        r#"{{"name":"m","exponentialHistogram":{{"dataPoints":{}}}}}"#,
        arr("{}", MAX_DATA_POINTS + 1)
    );
    assert_rejects_with(&one_metric(&metric), "dataPoints");
}

#[test]
fn summary_data_points_over_per_level_cap_rejects() {
    let metric = format!(
        r#"{{"name":"m","summary":{{"dataPoints":{}}}}}"#,
        arr("{}", MAX_DATA_POINTS + 1)
    );
    assert_rejects_with(&one_metric(&metric), "dataPoints");
}

#[test]
fn number_data_point_attributes_over_per_level_cap_rejects() {
    let point = format!(
        r#"{{"attributes":{}}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1)
    );
    assert_rejects_with(&one_metric(&one_gauge_point(&point)), "attributes");
}

#[test]
fn histogram_data_point_attributes_over_per_level_cap_rejects() {
    let point = format!(
        r#"{{"attributes":{}}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1)
    );
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "attributes");
}

#[test]
fn exponential_histogram_data_point_attributes_over_per_level_cap_rejects() {
    let point = format!(
        r#"{{"attributes":{}}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1)
    );
    let metric = format!(r#"{{"name":"m","exponentialHistogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "attributes");
}

#[test]
fn summary_data_point_attributes_over_per_level_cap_rejects() {
    let point = format!(
        r#"{{"attributes":{}}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1)
    );
    let metric = format!(r#"{{"name":"m","summary":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "attributes");
}

#[test]
fn number_data_point_exemplars_over_per_level_cap_rejects() {
    let point = format!(r#"{{"exemplars":{}}}"#, arr("{}", MAX_EXEMPLARS + 1));
    assert_rejects_with(&one_metric(&one_gauge_point(&point)), "exemplars");
}

#[test]
fn histogram_data_point_exemplars_over_per_level_cap_rejects() {
    let point = format!(r#"{{"exemplars":{}}}"#, arr("{}", MAX_EXEMPLARS + 1));
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "exemplars");
}

#[test]
fn exponential_histogram_data_point_exemplars_over_per_level_cap_rejects() {
    let point = format!(r#"{{"exemplars":{}}}"#, arr("{}", MAX_EXEMPLARS + 1));
    let metric = format!(r#"{{"name":"m","exponentialHistogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "exemplars");
}

#[test]
fn exemplar_filtered_attributes_over_per_level_cap_rejects() {
    let exemplar = format!(
        r#"{{"filteredAttributes":{}}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1)
    );
    let point = format!(r#"{{"exemplars":[{exemplar}]}}"#);
    assert_rejects_with(&one_metric(&one_gauge_point(&point)), "attributes");
}

#[test]
fn histogram_bucket_counts_over_per_level_cap_rejects() {
    let point = format!(r#"{{"bucketCounts":{}}}"#, arr("1", MAX_BUCKETS + 1));
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "bucketCounts");
}

#[test]
fn histogram_explicit_bounds_over_per_level_cap_rejects() {
    let point = format!(r#"{{"explicitBounds":{}}}"#, arr("1.0", MAX_BUCKETS + 1));
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "explicitBounds");
}

#[test]
fn exponential_histogram_buckets_bucket_counts_over_per_level_cap_rejects() {
    let point = format!(
        r#"{{"positive":{{"bucketCounts":{}}}}}"#,
        arr("1", MAX_BUCKETS + 1)
    );
    let metric = format!(r#"{{"name":"m","exponentialHistogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "bucketCounts");
}

#[test]
fn summary_quantile_values_over_per_level_cap_rejects() {
    let point = format!(r#"{{"quantileValues":{}}}"#, arr("{}", MAX_QUANTILES + 1));
    let metric = format!(r#"{{"name":"m","summary":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "quantileValues");
}

// --------------------------------------------------------------------------
// Scalar-array elements are primitive-only tokens: a nested array/object
// element rejects WITHOUT being materialized (track-6c code review [high]).
// The "nested ... in scalar array element" needle proves the reject came
// from the primitive-only `ScalarToken` visitor BEFORE materialization —
// under the old unrestricted `serde_json::Value` element the nested tree was
// fully built and only rejected later by the vendored per-element codec,
// whose error mentions the expected scalar type instead (non-vacuity).
// --------------------------------------------------------------------------

#[test]
fn histogram_bucket_counts_nested_array_element_rejects_unmaterialized() {
    // A deeply/widely nested element inside bucketCounts: must reject at the
    // token, never build the tree.
    let nested = format!("[{}]", arr("[1,2,3]", 1000));
    let point = format!(r#"{{"bucketCounts":[{nested}]}}"#);
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "nested array in scalar array element");
}

#[test]
fn histogram_explicit_bounds_nested_object_element_rejects_unmaterialized() {
    let point = r#"{"explicitBounds":[{"a":{"b":{"c":[1,2,3]}}}]}"#;
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(
        &one_metric(&metric),
        "nested object in scalar array element",
    );
}

#[test]
fn exponential_histogram_buckets_nested_element_rejects_unmaterialized() {
    // Both the positive and negative Buckets paths route through the same
    // primitive-only element token.
    let positive = r#"{"positive":{"bucketCounts":[[1,2]]}}"#;
    let metric = format!(r#"{{"name":"m","exponentialHistogram":{{"dataPoints":[{positive}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "nested array in scalar array element");

    let negative = r#"{"negative":{"bucketCounts":[{"k":"v"}]}}"#;
    let metric = format!(r#"{{"name":"m","exponentialHistogram":{{"dataPoints":[{negative}]}}}}"#);
    assert_rejects_with(
        &one_metric(&metric),
        "nested object in scalar array element",
    );
}

// --------------------------------------------------------------------------
// Exact-cap PAIRED tests (at-cap accepts / cap+1 rejects), real MAX constants
// --------------------------------------------------------------------------

#[test]
fn resource_metrics_at_cap_accepts() {
    let body = format!(
        r#"{{"resourceMetrics":{}}}"#,
        arr(r#"{"scopeMetrics":[]}"#, MAX_RESOURCE_METRICS)
    );
    assert_ok(&body);
}

#[test]
fn resource_metrics_cap_plus_one_rejects() {
    resource_metrics_over_per_level_cap_rejects();
}

#[test]
fn scope_metrics_at_cap_accepts() {
    let body = format!(
        r#"{{"resourceMetrics":[{{"scopeMetrics":{}}}]}}"#,
        arr(r#"{"metrics":[]}"#, MAX_SCOPE_METRICS)
    );
    assert_ok(&body);
}

#[test]
fn scope_metrics_cap_plus_one_rejects() {
    scope_metrics_over_per_level_cap_rejects();
}

#[test]
fn metrics_at_cap_accepts() {
    assert_ok(&one_scope(&arr(r#"{"name":"m"}"#, MAX_METRICS)));
}

#[test]
fn metrics_cap_plus_one_rejects() {
    metrics_over_per_level_cap_rejects();
}

#[test]
fn metric_metadata_at_cap_accepts() {
    let metric = format!(
        r#"{{"name":"m","metadata":{}}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT)
    );
    assert_ok(&one_metric(&metric));
}

#[test]
fn metric_metadata_cap_plus_one_rejects() {
    metric_metadata_over_per_level_cap_rejects();
}

#[test]
fn gauge_data_points_at_cap_accepts() {
    assert_ok(&one_metric(&gauge_metric_with_points(MAX_DATA_POINTS)));
}

#[test]
fn gauge_data_points_cap_plus_one_rejects() {
    gauge_data_points_over_per_level_cap_rejects();
}

#[test]
fn number_data_point_attributes_at_cap_accepts() {
    let point = format!(
        r#"{{"attributes":{}}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT)
    );
    assert_ok(&one_metric(&one_gauge_point(&point)));
}

#[test]
fn number_data_point_attributes_cap_plus_one_rejects() {
    number_data_point_attributes_over_per_level_cap_rejects();
}

#[test]
fn number_data_point_exemplars_at_cap_accepts() {
    let point = format!(r#"{{"exemplars":{}}}"#, arr("{}", MAX_EXEMPLARS));
    assert_ok(&one_metric(&one_gauge_point(&point)));
}

#[test]
fn number_data_point_exemplars_cap_plus_one_rejects() {
    number_data_point_exemplars_over_per_level_cap_rejects();
}

#[test]
fn histogram_bucket_counts_at_cap_accepts() {
    let point = format!(r#"{{"bucketCounts":{}}}"#, arr("1", MAX_BUCKETS));
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_ok(&one_metric(&metric));
}

#[test]
fn histogram_bucket_counts_cap_plus_one_rejects() {
    histogram_bucket_counts_over_per_level_cap_rejects();
}

#[test]
fn histogram_explicit_bounds_at_cap_accepts() {
    let point = format!(r#"{{"explicitBounds":{}}}"#, arr("1.0", MAX_BUCKETS));
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_ok(&one_metric(&metric));
}

#[test]
fn histogram_explicit_bounds_cap_plus_one_rejects() {
    histogram_explicit_bounds_over_per_level_cap_rejects();
}

#[test]
fn negative_buckets_bucket_counts_at_cap_accepts() {
    // The ExponentialHistogramDataPoint NEGATIVE Buckets path (the positive
    // path's over-cap reject lives above; this pair proves the negative
    // call site is wired to the same MAX_BUCKETS cap).
    let point = format!(
        r#"{{"negative":{{"bucketCounts":{}}}}}"#,
        arr("1", MAX_BUCKETS)
    );
    let metric = format!(r#"{{"name":"m","exponentialHistogram":{{"dataPoints":[{point}]}}}}"#);
    assert_ok(&one_metric(&metric));
}

#[test]
fn negative_buckets_bucket_counts_cap_plus_one_rejects() {
    let point = format!(
        r#"{{"negative":{{"bucketCounts":{}}}}}"#,
        arr("1", MAX_BUCKETS + 1)
    );
    let metric = format!(r#"{{"name":"m","exponentialHistogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "bucketCounts");
}

#[test]
fn summary_quantile_values_at_cap_accepts() {
    let point = format!(r#"{{"quantileValues":{}}}"#, arr("{}", MAX_QUANTILES));
    let metric = format!(r#"{{"name":"m","summary":{{"dataPoints":[{point}]}}}}"#);
    assert_ok(&one_metric(&metric));
}

#[test]
fn summary_quantile_values_cap_plus_one_rejects() {
    summary_quantile_values_over_per_level_cap_rejects();
}

// --------------------------------------------------------------------------
// Cross-request aggregate reject: each under per-level cap, summing over
// --------------------------------------------------------------------------

#[test]
fn data_points_over_aggregate_cap_rejects() {
    // Each metric holds < MAX_DATA_POINTS points; enough metrics sum past
    // MAX_TOTAL_DATA_POINTS.
    let per_metric = MAX_DATA_POINTS - 1;
    let metrics_needed = MAX_TOTAL_DATA_POINTS / per_metric + 1;
    let one = gauge_metric_with_points(per_metric);
    let body = one_scope(&arr(&one, metrics_needed));
    assert_rejects_with(&body, "total data points");
}

#[test]
fn exemplars_over_aggregate_cap_rejects() {
    // Each data point holds MAX_EXEMPLARS exemplars (in-bounds per level);
    // enough data points sum past MAX_TOTAL_EXEMPLARS.
    let per_point = MAX_EXEMPLARS;
    let points_needed = MAX_TOTAL_EXEMPLARS / per_point + 1;
    let point = format!(r#"{{"exemplars":{}}}"#, arr("{}", per_point));
    let metric = format!(
        r#"{{"name":"m","gauge":{{"dataPoints":{}}}}}"#,
        arr(&point, points_needed)
    );
    assert_rejects_with(&one_metric(&metric), "total exemplars");
}

// --------------------------------------------------------------------------
// AnyValue: wired for metric attributes (over-wide / over-depth), non-vacuous
// --------------------------------------------------------------------------

#[test]
fn data_point_attribute_anyvalue_over_wide_kvlist_rejects() {
    let entries = arr(r#"{"key":"k"}"#, MAX_ANYVALUE_ELEMENTS + 1);
    let attr = format!(r#"{{"key":"wide","value":{{"kvlistValue":{{"values":{entries}}}}}}}"#);
    let point = format!(r#"{{"attributes":[{attr}]}}"#);
    assert_rejects_with(&one_metric(&one_gauge_point(&point)), "AnyValue elements");
}

#[test]
fn data_point_attribute_anyvalue_over_depth_rejects() {
    let deep = nested_array_value(MAX_ANYVALUE_DEPTH + 1);
    let attr = format!(r#"{{"key":"deep","value":{deep}}}"#);
    let point = format!(r#"{{"attributes":[{attr}]}}"#);
    assert_rejects_with(
        &one_metric(&one_gauge_point(&point)),
        "AnyValue nesting depth",
    );
}

// --------------------------------------------------------------------------
// Alias-split anti-evasion: camelCase + snake_case route into ONE counter
// --------------------------------------------------------------------------

#[test]
fn alias_split_resource_metrics_cannot_evade_the_per_level_cap() {
    let camel = arr(r#"{"scopeMetrics":[]}"#, MAX_RESOURCE_METRICS);
    let snake = arr(r#"{"scopeMetrics":[]}"#, 1);
    let body = format!(r#"{{"resourceMetrics":{camel},"resource_metrics":{snake}}}"#);
    assert_rejects_with(&body, "resourceMetrics");
}

#[test]
fn alias_split_scope_metrics_cannot_evade_the_per_level_cap() {
    let camel = arr(r#"{"metrics":[]}"#, MAX_SCOPE_METRICS);
    let snake = arr(r#"{"metrics":[]}"#, 1);
    let body =
        format!(r#"{{"resourceMetrics":[{{"scopeMetrics":{camel},"scope_metrics":{snake}}}]}}"#);
    assert_rejects_with(&body, "scopeMetrics");
}

#[test]
fn alias_split_data_points_cannot_evade_the_per_level_cap() {
    let camel = arr("{}", MAX_DATA_POINTS);
    let snake = arr("{}", 1);
    let metric =
        format!(r#"{{"name":"m","gauge":{{"dataPoints":{camel},"data_points":{snake}}}}}"#);
    assert_rejects_with(&one_metric(&metric), "dataPoints");
}

#[test]
fn alias_split_bucket_counts_cannot_evade_the_per_level_cap() {
    let camel = arr("1", MAX_BUCKETS);
    let snake = arr("1", 1);
    let point = format!(r#"{{"bucketCounts":{camel},"bucket_counts":{snake}}}"#);
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "bucketCounts");
}

#[test]
fn alias_split_explicit_bounds_cannot_evade_the_per_level_cap() {
    let camel = arr("1.0", MAX_BUCKETS);
    let snake = arr("1.0", 1);
    let point = format!(r#"{{"explicitBounds":{camel},"explicit_bounds":{snake}}}"#);
    let metric = format!(r#"{{"name":"m","histogram":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "explicitBounds");
}

#[test]
fn alias_split_quantile_values_cannot_evade_the_per_level_cap() {
    let camel = arr("{}", MAX_QUANTILES);
    let snake = arr("{}", 1);
    let point = format!(r#"{{"quantileValues":{camel},"quantile_values":{snake}}}"#);
    let metric = format!(r#"{{"name":"m","summary":{{"dataPoints":[{point}]}}}}"#);
    assert_rejects_with(&one_metric(&metric), "quantileValues");
}

#[test]
fn alias_split_filtered_attributes_cannot_evade_the_per_level_cap() {
    let camel = arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT);
    let snake = arr(r#"{"key":"k"}"#, 1);
    let exemplar = format!(r#"{{"filteredAttributes":{camel},"filtered_attributes":{snake}}}"#);
    let point = format!(r#"{{"exemplars":[{exemplar}]}}"#);
    assert_rejects_with(&one_metric(&one_gauge_point(&point)), "attributes");
}

#[test]
fn alias_split_exponential_histogram_member_still_rejects_as_multiple_members() {
    // camelCase `exponentialHistogram` + snake_case `exponential_histogram`
    // both present is the SAME oneof member set twice — must reject as a
    // multi-member oneof (P6), not silently accumulate.
    let metric = r#"{"name":"m",
        "exponentialHistogram":{"dataPoints":[]},
        "exponential_histogram":{"dataPoints":[]}}"#;
    assert_rejects_with(&one_metric(metric), "multiple metric data oneof members");
}

// --------------------------------------------------------------------------
// Duplicate-key semantics
// --------------------------------------------------------------------------

#[test]
fn duplicate_metrics_key_accumulates_into_one_counter() {
    // Two separate `"metrics"` keys in the same scopeMetrics object each
    // under-cap individually, summing past MAX_METRICS — raw occurrences
    // accumulate into the same counter, they don't reset per key.
    let half = MAX_METRICS / 2 + 1;
    let chunk = arr(r#"{"name":"m"}"#, half);
    let body = format!(
        r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":{chunk},"metrics":{chunk}}}]}}]}}"#
    );
    assert_rejects_with(&body, "metrics");
}

#[test]
fn metric_duplicate_name_rejects_matching_vendored() {
    let metric = r#"{"name":"a","name":"b"}"#;
    assert_rejects_with(&one_metric(metric), "");
}

#[test]
fn resource_metrics_duplicate_resource_rejects_matching_vendored() {
    let body = r#"{"resourceMetrics":[{"resource":{},"resource":{}}]}"#;
    assert!(decode_json(body.as_bytes()).is_err());
}

#[test]
fn scope_metrics_duplicate_scope_rejects_matching_vendored() {
    let body = r#"{"resourceMetrics":[{"scopeMetrics":[{"scope":{},"scope":{}}]}]}"#;
    assert!(decode_json(body.as_bytes()).is_err());
}

#[test]
fn exponential_histogram_data_point_duplicate_positive_rejects_matching_vendored() {
    let point = r#"{"positive":{},"positive":{}}"#;
    let metric = format!(r#"{{"name":"m","exponentialHistogram":{{"dataPoints":[{point}]}}}}"#);
    assert!(decode_json(one_metric(&metric).as_bytes()).is_err());
}

#[test]
fn exponential_histogram_data_point_duplicate_negative_rejects_matching_vendored() {
    let point = r#"{"negative":{},"negative":{}}"#;
    let metric = format!(r#"{{"name":"m","exponentialHistogram":{{"dataPoints":[{point}]}}}}"#);
    assert!(decode_json(one_metric(&metric).as_bytes()).is_err());
}

// --------------------------------------------------------------------------
// P6: the 5-way `Metric.data` oneof rejects >1 member / a malformed member,
// exactly like the vendored derive (see otlp_json_vendor_patch.rs for the
// low-level derive-direct proofs; these prove the SAME behaviour through the
// bounded seed's hand-routing).
// --------------------------------------------------------------------------

#[test]
fn multiple_metric_data_oneof_members_rejects() {
    let metric = r#"{"name":"m","gauge":{"dataPoints":[]},"sum":{"dataPoints":[]}}"#;
    assert_rejects_with(&one_metric(metric), "multiple metric data oneof members");
}

#[test]
fn malformed_metric_data_member_is_a_decode_error_not_a_swallow() {
    // A bad `count` deep inside the data subtree must be a decode error, not
    // silently collapse `Metric.data` to `None` (the P6 swallow this whole
    // family of patches closes).
    let metric = r#"{"name":"m","histogram":{"dataPoints":[{"count":"nope"}]}}"#;
    assert!(decode_json(one_metric(metric).as_bytes()).is_err());
}

#[test]
fn empty_metric_data_oneof_decodes_to_none() {
    // No recognized member key present -> `data: None`, not an error.
    assert_ok(&one_metric(r#"{"name":"m"}"#));
}

#[test]
fn number_data_point_multiple_value_oneof_members_rejects() {
    let point = r#"{"asDouble":1.5,"asInt":"2"}"#;
    assert!(decode_json(one_metric(&one_gauge_point(point)).as_bytes()).is_err());
}

#[test]
fn number_data_point_malformed_value_rejects() {
    let point = r#"{"asInt":"not-a-number"}"#;
    assert!(decode_json(one_metric(&one_gauge_point(point)).as_bytes()).is_err());
}

#[test]
fn exemplar_multiple_value_oneof_members_rejects() {
    let exemplar = r#"{"asDouble":1.5,"asInt":"2"}"#;
    let point = format!(r#"{{"exemplars":[{exemplar}]}}"#);
    assert!(decode_json(one_metric(&one_gauge_point(&point)).as_bytes()).is_err());
}

#[test]
fn exemplar_malformed_value_rejects() {
    let exemplar = r#"{"asInt":{}}"#;
    let point = format!(r#"{{"exemplars":[{exemplar}]}}"#);
    assert!(decode_json(one_metric(&one_gauge_point(&point)).as_bytes()).is_err());
}

// --------------------------------------------------------------------------
// Non-finite doubles (P1) still decode through the bounded seed
// --------------------------------------------------------------------------

#[test]
fn non_finite_doubles_still_decode_through_the_bounded_seed() {
    let gauge_point = r#"{"asDouble":"NaN"}"#;
    assert_ok(&one_metric(&one_gauge_point(gauge_point)));

    let histogram = r#"{"name":"m","histogram":{"dataPoints":[{"sum":"Infinity",
        "explicitBounds":["-Infinity","Infinity"]}]}}"#;
    assert_ok(&one_metric(histogram));

    let exemplar_point = r#"{"exemplars":[{"asDouble":"-Infinity"}]}"#;
    assert_ok(&one_metric(&one_gauge_point(exemplar_point)));
}

// --------------------------------------------------------------------------
// Unknown-key discipline: never materialized (skipped), matching the
// vendored derive's no-deny-unknown-fields behaviour.
// --------------------------------------------------------------------------

#[test]
fn unknown_key_with_wide_value_is_ignored_matching_vendored() {
    let wide = arr("1", 10_000);
    let metric = format!(r#"{{"name":"m","somethingUnknown":{wide}}}"#);
    assert_ok(&one_metric(&metric));
}
