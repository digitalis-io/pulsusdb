//! Bounded proto3-JSON building blocks for OTLP metrics (issue #115, track 6c
//! — the final sub-track of the coordinated ingest-DoS family fix).
//!
//! Mirrors [`super::decode_traces`] (6a) / [`super::decode_logs`] (6b) at the
//! SAME per-level / aggregate / depth thresholds
//! ([`crate::protocols::otlp_prescan`]), reusing every shared building block
//! (`JsonAggregates`, `AnyValueSeed`/`KeyValueSeed`/`ResourceSeed`/
//! `InstrumentationScopeSeed`, `AccumSeq`/`accumulate_msgs`/
//! `accumulate_attributes`, `buffer_scalar_or_skip`/`finish_via_derive`,
//! `OptionSeed`) verbatim from `super`.
//!
//! # The `Metric.data` 5-way oneof (P6) is hand-routed
//!
//! Every OTHER intercepted field in this module is a plain repeated/message
//! container handled by the shared combinators. `Metric.data` is different:
//! it is a `#[serde(flatten)]` oneof whose FIVE possible members
//! (`gauge`/`sum`/`histogram`/`exponentialHistogram`/`summary`) are top-level
//! keys of the `Metric` JSON object, and each member carries a repeated
//! `dataPoints` that must be bounded DURING deserialization — so it cannot be
//! scalar-buffered like `NumberDataPoint.value`/`Exemplar.value` below (see
//! next section). [`MetricSeed`] therefore hand-routes all five member keys
//! (matching the vendored `Flat` struct's field table in
//! `vendor/opentelemetry-proto/src/proto.rs`'s `oneof_metric_data` module)
//! through [`set_metric_data`], which reproduces the vendored `set_oneof`
//! semantics EXACTLY: absent → `None`; exactly one recognized member (any of
//! the five, any accepted spelling) → `Some`; a SECOND member set (whichever
//! combination or spelling) → a decode error (never a silent last-write-win
//! swallow). A malformed member value (e.g. `"gauge":"nope"`) propagates as
//! `Err` through the member's own bounded seed, exactly as the vendored
//! `Option<Gauge>` field would fail to deserialize a non-object value.
//!
//! # `NumberDataPoint.value` / `Exemplar.value` (P6) need NO hand-routing
//!
//! Unlike `Metric.data`, these two flatten oneofs (`asDouble`/`asInt`) are
//! pure SCALARS — no repeated data to bound. Buffering `asDouble`/`asInt`
//! like any other scalar leaf and finishing through [`super::finish_via_derive`]
//! replays the SAME JSON through the SAME vendored `deserialize_with`
//! (`oneof_number_value`/`oneof_exemplar_value`), so P1 (non-finite doubles)
//! and P6 (reject >1 member, reject malformed, accept `asInt`-as-string) come
//! for free, byte-identically — no reimplementation needed or attempted.
//!
//! # Numeric-vector bounding (`bucketCounts`/`explicitBounds`)
//!
//! `HistogramDataPoint.bucket_counts`/`explicit_bounds` and
//! `Buckets.bucket_counts` are repeated SCALAR (u64-as-string / special-double)
//! fields, not messages — the vendored per-element codec
//! (`deserialize_vec_string_to_vec_u64`, the `f64_special_vec` patch) lives in
//! the vendor crate's PRIVATE `serializers` module and cannot be called from
//! here. [`accumulate_bounded_scalar_array`] bounds the ARRAY LENGTH during
//! deserialization (reject-before-materialize, the same discipline
//! [`super::AccumSeq`] uses) while buffering each element as a
//! `serde_json::Value` — the identical buffer-and-delegate discipline every
//! scalar leaf already uses (depth is bounded by `serde_json`'s own recursion
//! limit; total bytes by the per-level element cap × the 64 MiB request body
//! cap already enforced before decode — no NEW amplification vector, the same
//! O(body) argument the v6 scalar-value-disposition plan makes for every
//! other buffered scalar). The buffered array replays through the vendored
//! derive via [`super::finish_via_derive`], so `f64_special_vec` /
//! `deserialize_vec_string_to_vec_u64` (and their non-finite / string-or-number
//! per-element acceptance) apply byte-identically.
//!
//! # Data-point message children are ALWAYS bounded seeds, never scalars
//!
//! `NumberDataPoint`/`HistogramDataPoint`/`ExponentialHistogramDataPoint`/
//! `SummaryDataPoint`/`Exemplar`/`exponential_histogram_data_point::Buckets`
//! each get their own bounded seed; none of their MESSAGE-typed children
//! (`attributes`, `exemplars`, `positive`/`negative` `Buckets`,
//! `quantileValues`) appear in a `*_SCALARS` buffer-and-delegate list — the
//! Span.status-class DoS this whole family fix guards against.
//! `summary_data_point::ValueAtQuantile` is the one exception that needs NO
//! bounded seed at all: it is all-scalar (`quantile`, `value`, both `f64`),
//! matching the protobuf pre-scan's `ValueAtQuantile => Action::Skip` — its
//! elements decode via the vendored `Deserialize` directly
//! (`std::marker::PhantomData<ValueAtQuantile>`, the same pattern
//! [`super::accumulate_strings`] uses for `Vec<String>`).

use std::fmt;

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets;
use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
    SummaryDataPoint, metric,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

use crate::error::LogsIngestError;
use crate::protocols::otlp_prescan::{
    MAX_BUCKETS, MAX_DATA_POINTS, MAX_EXEMPLARS, MAX_METRICS, MAX_QUANTILES, MAX_RESOURCE_METRICS,
    MAX_SCOPE_METRICS, MAX_TOTAL_DATA_POINTS, MAX_TOTAL_EXEMPLARS,
};

use super::{
    AggCharge, InstrumentationScopeSeed, JsonAggregates, OptionSeed, ResourceSeed,
    accumulate_attributes, accumulate_msgs, buffer_scalar_or_skip, finish_via_derive,
};

// ---------------------------------------------------------------------------
// camelCase scalar-leaf field lists (v6 ruling 1: scalar leaves stay
// camelCase-only, delegated to the vendored derive unchanged)
// ---------------------------------------------------------------------------

const RESOURCE_METRICS_SCALARS: &[&str] = &["schemaUrl"];
const SCOPE_METRICS_SCALARS: &[&str] = &["schemaUrl"];
const METRIC_SCALARS: &[&str] = &["name", "description", "unit"];
// `Gauge`/`Summary` carry no scalar leaves — every key that isn't `dataPoints`
// is genuinely unknown and IgnoredAny-skipped.
const GAUGE_SCALARS: &[&str] = &[];
const SUMMARY_SCALARS: &[&str] = &[];
const SUM_SCALARS: &[&str] = &["aggregationTemporality", "isMonotonic"];
const HISTOGRAM_SCALARS: &[&str] = &["aggregationTemporality"];
const EXP_HISTOGRAM_SCALARS: &[&str] = &["aggregationTemporality"];
const NUMBER_DATA_POINT_SCALARS: &[&str] = &[
    "startTimeUnixNano",
    "timeUnixNano",
    "flags",
    "asDouble",
    "asInt",
];
const HISTOGRAM_DATA_POINT_SCALARS: &[&str] = &[
    "startTimeUnixNano",
    "timeUnixNano",
    "count",
    "sum",
    "flags",
    "min",
    "max",
];
const EXP_HISTOGRAM_DATA_POINT_SCALARS: &[&str] = &[
    "startTimeUnixNano",
    "timeUnixNano",
    "count",
    "sum",
    "scale",
    "zeroCount",
    "flags",
    "min",
    "max",
    "zeroThreshold",
];
const BUCKETS_SCALARS: &[&str] = &["offset"];
const SUMMARY_DATA_POINT_SCALARS: &[&str] =
    &["startTimeUnixNano", "timeUnixNano", "count", "sum", "flags"];
const EXEMPLAR_SCALARS: &[&str] = &["timeUnixNano", "spanId", "traceId", "asDouble", "asInt"];

// ---------------------------------------------------------------------------
// Numeric-vector bounding (bucketCounts / explicitBounds)
// ---------------------------------------------------------------------------

/// A single scalar-array element restricted to PRIMITIVE JSON tokens (issue
/// #115 track-6c code review, [high] finding): captures a bool / number /
/// string / null as a `serde_json::Value` scalar for replay through
/// [`super::finish_via_derive`], and REJECTS a nested array/object
/// IMMEDIATELY — `visit_seq`/`visit_map` return `Err` before touching the
/// `SeqAccess`/`MapAccess`, so the nested attacker tree is never drained or
/// materialized (the same never-materialize discipline
/// [`super::buffer_scalar_or_skip`] applies to unknown keys). A plain
/// `next_element::<serde_json::Value>()` would fully build a deeply-nested
/// element before the vendored per-element codec rejected its type — the
/// materialization-DoS class this whole family fix closes.
struct ScalarToken(serde_json::Value);

impl<'de> serde::Deserialize<'de> for ScalarToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ScalarTokenVisitor;

        impl<'de> Visitor<'de> for ScalarTokenVisitor {
            type Value = ScalarToken;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a primitive JSON value (number, string, bool, or null)")
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> Result<ScalarToken, E> {
                Ok(ScalarToken(serde_json::Value::Bool(v)))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<ScalarToken, E> {
                Ok(ScalarToken(serde_json::Value::from(v)))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<ScalarToken, E> {
                Ok(ScalarToken(serde_json::Value::from(v)))
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<ScalarToken, E> {
                // A JSON-text number is always finite, so `from_f64` cannot
                // fail here; guard anyway rather than panic on a hypothetical
                // non-finite from a different Deserializer.
                serde_json::Number::from_f64(v)
                    .map(|n| ScalarToken(serde_json::Value::Number(n)))
                    .ok_or_else(|| E::custom("non-finite JSON number"))
            }

            // `visit_string`/`visit_borrowed_str` forward here by default.
            fn visit_str<E: de::Error>(self, v: &str) -> Result<ScalarToken, E> {
                Ok(ScalarToken(serde_json::Value::String(v.to_owned())))
            }

            fn visit_none<E: de::Error>(self) -> Result<ScalarToken, E> {
                Ok(ScalarToken(serde_json::Value::Null))
            }

            fn visit_unit<E: de::Error>(self) -> Result<ScalarToken, E> {
                Ok(ScalarToken(serde_json::Value::Null))
            }

            fn visit_seq<A>(self, _seq: A) -> Result<ScalarToken, A::Error>
            where
                A: SeqAccess<'de>,
            {
                // Reject WITHOUT consuming `_seq`: returning Err before any
                // `next_element` call propagates immediately, so the nested
                // array is never walked or built.
                Err(de::Error::custom("nested array in scalar array element"))
            }

            fn visit_map<A>(self, _map: A) -> Result<ScalarToken, A::Error>
            where
                A: MapAccess<'de>,
            {
                // Same: never consume `_map`.
                Err(de::Error::custom("nested object in scalar array element"))
            }
        }

        deserializer.deserialize_any(ScalarTokenVisitor)
    }
}

/// Bounds a repeated SCALAR (not message) field's array LENGTH during
/// deserialization — reject-before-materialize, matching
/// [`super::AccumSeq`]'s per-element probe — buffering each in-bounds element
/// as a primitive-only [`ScalarToken`] (a nested array/object element rejects
/// without materializing) for later replay through
/// [`super::finish_via_derive`] (module doc: "Numeric-vector bounding"). Used
/// for `HistogramDataPoint.bucketCounts`/`explicitBounds` and
/// `Buckets.bucketCounts`, whose per-element special-double / u64-as-string
/// codec lives in the vendor crate's private `serializers` module.
fn accumulate_bounded_scalar_array<'de, A>(
    map: &mut A,
    per_level: (usize, &'static str),
) -> Result<serde_json::Value, A::Error>
where
    A: MapAccess<'de>,
{
    struct BoundedArraySeed {
        cap: usize,
        field: &'static str,
    }

    impl<'de> DeserializeSeed<'de> for BoundedArraySeed {
        type Value = serde_json::Value;

        fn deserialize<D>(self, deserializer: D) -> Result<serde_json::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(self)
        }
    }

    impl<'de> Visitor<'de> for BoundedArraySeed {
        type Value = serde_json::Value;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a JSON array")
        }

        fn visit_seq<A2>(self, mut seq: A2) -> Result<serde_json::Value, A2::Error>
        where
            A2: SeqAccess<'de>,
        {
            let mut out: Vec<serde_json::Value> = Vec::new();
            loop {
                if out.len() >= self.cap {
                    if seq.next_element::<IgnoredAny>()?.is_some() {
                        return Err(de::Error::custom(format!(
                            "{} exceeds the per-element bound of {}",
                            self.field, self.cap
                        )));
                    }
                    return Ok(serde_json::Value::Array(out));
                }
                match seq.next_element::<ScalarToken>()? {
                    Some(ScalarToken(v)) => out.push(v),
                    None => return Ok(serde_json::Value::Array(out)),
                }
            }
        }
    }

    map.next_value_seed(BoundedArraySeed {
        cap: per_level.0,
        field: per_level.1,
    })
}

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

/// Decodes a proto3-JSON `ExportMetricsServiceRequest` with every reachable
/// repeated/container field bounded DURING deserialization (issue #115 track
/// 6c), mirroring [`super::decode_traces`]/[`super::decode_logs`] at the SAME
/// thresholds. A cap/depth violation is a whole-request `serde` error →
/// [`LogsIngestError::DecodeJson`] (HTTP 400 / `google.rpc.Status.code = 3`).
pub(crate) fn decode_metrics(body: &[u8]) -> Result<ExportMetricsServiceRequest, LogsIngestError> {
    let agg = JsonAggregates::default();
    let mut de = serde_json::Deserializer::from_slice(body);
    let req = ExportMetricsServiceRequestSeed { agg: &agg }.deserialize(&mut de)?;
    // Reject trailing garbage exactly as `serde_json::from_slice` would.
    de.end()?;
    Ok(req)
}

struct ExportMetricsServiceRequestSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ExportMetricsServiceRequestSeed<'_> {
    type Value = ExportMetricsServiceRequest;

    fn deserialize<D>(self, deserializer: D) -> Result<ExportMetricsServiceRequest, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ExportMetricsServiceRequestSeed<'_> {
    type Value = ExportMetricsServiceRequest;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ExportMetricsServiceRequest object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ExportMetricsServiceRequest, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut resource_metrics: Vec<ResourceMetrics> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "resourceMetrics" | "resource_metrics" => accumulate_msgs(
                    &mut map,
                    &mut resource_metrics,
                    (MAX_RESOURCE_METRICS, "resourceMetrics"),
                    None,
                    || ResourceMetricsSeed { agg },
                )?,
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(ExportMetricsServiceRequest { resource_metrics })
    }
}

struct ResourceMetricsSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ResourceMetricsSeed<'_> {
    type Value = ResourceMetrics;

    fn deserialize<D>(self, deserializer: D) -> Result<ResourceMetrics, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ResourceMetricsSeed<'_> {
    type Value = ResourceMetrics;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ResourceMetrics object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ResourceMetrics, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Same buffer-and-delegate contract as `ResourceSpansSeed`/
        // `ResourceLogsSeed`: intercept the singular `resource` (dup-guarded)
        // and repeated `scopeMetrics` (bounded); BUFFER the scalar
        // `schemaUrl` and finish through the vendored `ResourceMetrics`
        // derive so a duplicate `schemaUrl` rejects exactly as the derive
        // does (issue #115 finding 1).
        let agg = self.agg;
        let mut resource: Option<Resource> = None;
        let mut resource_seen = false;
        let mut scope_metrics: Vec<ScopeMetrics> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "resource" => {
                    if resource_seen {
                        return Err(de::Error::duplicate_field("resource"));
                    }
                    resource_seen = true;
                    resource = map.next_value_seed(OptionSeed(ResourceSeed { agg }))?;
                }
                "scopeMetrics" | "scope_metrics" => accumulate_msgs(
                    &mut map,
                    &mut scope_metrics,
                    (MAX_SCOPE_METRICS, "scopeMetrics"),
                    None,
                    || ScopeMetricsSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, RESOURCE_METRICS_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut resource_metrics: ResourceMetrics = finish_via_derive(&pairs)?;
        resource_metrics.resource = resource;
        resource_metrics.scope_metrics = scope_metrics;
        Ok(resource_metrics)
    }
}

struct ScopeMetricsSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ScopeMetricsSeed<'_> {
    type Value = ScopeMetrics;

    fn deserialize<D>(self, deserializer: D) -> Result<ScopeMetrics, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ScopeMetricsSeed<'_> {
    type Value = ScopeMetrics;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ScopeMetrics object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ScopeMetrics, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut scope: Option<InstrumentationScope> = None;
        let mut scope_seen = false;
        let mut metrics: Vec<Metric> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "scope" => {
                    if scope_seen {
                        return Err(de::Error::duplicate_field("scope"));
                    }
                    scope_seen = true;
                    scope = map.next_value_seed(OptionSeed(InstrumentationScopeSeed { agg }))?;
                }
                "metrics" => accumulate_msgs(
                    &mut map,
                    &mut metrics,
                    (MAX_METRICS, "metrics"),
                    None,
                    || MetricSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, SCOPE_METRICS_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut scope_metrics: ScopeMetrics = finish_via_derive(&pairs)?;
        scope_metrics.scope = scope;
        scope_metrics.metrics = metrics;
        Ok(scope_metrics)
    }
}

// ---------------------------------------------------------------------------
// Metric + the 5-way `data` oneof (P6, hand-routed — module doc)
// ---------------------------------------------------------------------------

/// Sets the `Metric.data` oneof slot, erroring if a member was already set —
/// reproduces the vendored `oneof_metric_data::deserialize`'s `set_oneof` P6
/// semantics (issue #115 track 6c): "at most one member" holds regardless of
/// which of the five arms, or which accepted spelling, supplied the second
/// occurrence.
fn set_metric_data<E: de::Error>(
    slot: &mut Option<metric::Data>,
    value: metric::Data,
) -> Result<(), E> {
    if slot.is_some() {
        return Err(E::custom("multiple metric data oneof members set"));
    }
    *slot = Some(value);
    Ok(())
}

struct MetricSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for MetricSeed<'_> {
    type Value = Metric;

    fn deserialize<D>(self, deserializer: D) -> Result<Metric, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for MetricSeed<'_> {
    type Value = Metric;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Metric object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Metric, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut metadata: Vec<KeyValue> = Vec::new();
        let mut data: Option<metric::Data> = None;
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                // `Metric.metadata` shares the `attributes` cap/aggregate
                // (matches the protobuf pre-scan's `attr_count()` reuse on
                // field 12 — same counter, different JSON key name).
                "metadata" => accumulate_attributes(&mut map, &mut metadata, agg)?,
                // The 5-way `data` oneof (P6): hand-routed because each arm
                // carries a `dataPoints` fan-out that must be bounded before
                // materializing (module doc). `exponentialHistogram` is the
                // only arm whose camelCase/snake_case spellings differ.
                "gauge" => {
                    let g = map.next_value_seed(GaugeSeed { agg })?;
                    set_metric_data(&mut data, metric::Data::Gauge(g))?;
                }
                "sum" => {
                    let s = map.next_value_seed(SumSeed { agg })?;
                    set_metric_data(&mut data, metric::Data::Sum(s))?;
                }
                "histogram" => {
                    let h = map.next_value_seed(HistogramSeed { agg })?;
                    set_metric_data(&mut data, metric::Data::Histogram(h))?;
                }
                "exponentialHistogram" | "exponential_histogram" => {
                    let eh = map.next_value_seed(ExponentialHistogramSeed { agg })?;
                    set_metric_data(&mut data, metric::Data::ExponentialHistogram(eh))?;
                }
                "summary" => {
                    let s = map.next_value_seed(SummarySeed { agg })?;
                    set_metric_data(&mut data, metric::Data::Summary(s))?;
                }
                _ => buffer_scalar_or_skip(key, METRIC_SCALARS, &mut map, &mut pairs)?,
            }
        }
        // Empty scalar buffer -> `Metric::default()` (byte-identical to the
        // `serde(default)` derive on an empty object — common for a
        // name-less synthetic point in tests); a non-empty buffer is
        // finished through the vendored derive so duplicate scalar keys
        // reject.
        let mut metric = if pairs.is_empty() {
            Metric::default()
        } else {
            finish_via_derive(&pairs)?
        };
        metric.metadata = metadata;
        metric.data = data;
        Ok(metric)
    }
}

// ---------------------------------------------------------------------------
// The five `Metric.data` oneof arms — each bounds its `dataPoints` fan-out
// ---------------------------------------------------------------------------

struct GaugeSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for GaugeSeed<'_> {
    type Value = Gauge;

    fn deserialize<D>(self, deserializer: D) -> Result<Gauge, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for GaugeSeed<'_> {
    type Value = Gauge;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Metric.Gauge object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Gauge, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut data_points: Vec<NumberDataPoint> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "dataPoints" | "data_points" => accumulate_msgs(
                    &mut map,
                    &mut data_points,
                    (MAX_DATA_POINTS, "dataPoints"),
                    Some(AggCharge {
                        cell: &agg.data_points,
                        cap: MAX_TOTAL_DATA_POINTS,
                        field: "total data points",
                    }),
                    || NumberDataPointSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, GAUGE_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut gauge = if pairs.is_empty() {
            Gauge::default()
        } else {
            finish_via_derive(&pairs)?
        };
        gauge.data_points = data_points;
        Ok(gauge)
    }
}

struct SumSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for SumSeed<'_> {
    type Value = Sum;

    fn deserialize<D>(self, deserializer: D) -> Result<Sum, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for SumSeed<'_> {
    type Value = Sum;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Metric.Sum object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Sum, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut data_points: Vec<NumberDataPoint> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "dataPoints" | "data_points" => accumulate_msgs(
                    &mut map,
                    &mut data_points,
                    (MAX_DATA_POINTS, "dataPoints"),
                    Some(AggCharge {
                        cell: &agg.data_points,
                        cap: MAX_TOTAL_DATA_POINTS,
                        field: "total data points",
                    }),
                    || NumberDataPointSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, SUM_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut sum = if pairs.is_empty() {
            Sum::default()
        } else {
            finish_via_derive(&pairs)?
        };
        sum.data_points = data_points;
        Ok(sum)
    }
}

struct HistogramSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for HistogramSeed<'_> {
    type Value = Histogram;

    fn deserialize<D>(self, deserializer: D) -> Result<Histogram, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for HistogramSeed<'_> {
    type Value = Histogram;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Metric.Histogram object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Histogram, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut data_points: Vec<HistogramDataPoint> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "dataPoints" | "data_points" => accumulate_msgs(
                    &mut map,
                    &mut data_points,
                    (MAX_DATA_POINTS, "dataPoints"),
                    Some(AggCharge {
                        cell: &agg.data_points,
                        cap: MAX_TOTAL_DATA_POINTS,
                        field: "total data points",
                    }),
                    || HistogramDataPointSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, HISTOGRAM_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut histogram = if pairs.is_empty() {
            Histogram::default()
        } else {
            finish_via_derive(&pairs)?
        };
        histogram.data_points = data_points;
        Ok(histogram)
    }
}

struct ExponentialHistogramSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ExponentialHistogramSeed<'_> {
    type Value = ExponentialHistogram;

    fn deserialize<D>(self, deserializer: D) -> Result<ExponentialHistogram, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ExponentialHistogramSeed<'_> {
    type Value = ExponentialHistogram;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Metric.ExponentialHistogram object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ExponentialHistogram, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut data_points: Vec<ExponentialHistogramDataPoint> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "dataPoints" | "data_points" => accumulate_msgs(
                    &mut map,
                    &mut data_points,
                    (MAX_DATA_POINTS, "dataPoints"),
                    Some(AggCharge {
                        cell: &agg.data_points,
                        cap: MAX_TOTAL_DATA_POINTS,
                        field: "total data points",
                    }),
                    || ExponentialHistogramDataPointSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, EXP_HISTOGRAM_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut eh = if pairs.is_empty() {
            ExponentialHistogram::default()
        } else {
            finish_via_derive(&pairs)?
        };
        eh.data_points = data_points;
        Ok(eh)
    }
}

struct SummarySeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for SummarySeed<'_> {
    type Value = Summary;

    fn deserialize<D>(self, deserializer: D) -> Result<Summary, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for SummarySeed<'_> {
    type Value = Summary;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Metric.Summary object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Summary, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut data_points: Vec<SummaryDataPoint> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "dataPoints" | "data_points" => accumulate_msgs(
                    &mut map,
                    &mut data_points,
                    (MAX_DATA_POINTS, "dataPoints"),
                    Some(AggCharge {
                        cell: &agg.data_points,
                        cap: MAX_TOTAL_DATA_POINTS,
                        field: "total data points",
                    }),
                    || SummaryDataPointSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(key, SUMMARY_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut summary = if pairs.is_empty() {
            Summary::default()
        } else {
            finish_via_derive(&pairs)?
        };
        summary.data_points = data_points;
        Ok(summary)
    }
}

// ---------------------------------------------------------------------------
// Data-point shapes
// ---------------------------------------------------------------------------

struct NumberDataPointSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for NumberDataPointSeed<'_> {
    type Value = NumberDataPoint;

    fn deserialize<D>(self, deserializer: D) -> Result<NumberDataPoint, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for NumberDataPointSeed<'_> {
    type Value = NumberDataPoint;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON NumberDataPoint object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<NumberDataPoint, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut exemplars: Vec<Exemplar> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, agg)?,
                "exemplars" => accumulate_msgs(
                    &mut map,
                    &mut exemplars,
                    (MAX_EXEMPLARS, "exemplars"),
                    Some(AggCharge {
                        cell: &agg.exemplars,
                        cap: MAX_TOTAL_EXEMPLARS,
                        field: "total exemplars",
                    }),
                    || ExemplarSeed { agg },
                )?,
                // `value` (P6, `asDouble`/`asInt`): a pure scalar oneof — NO
                // hand-routing needed (module doc); buffered and finished
                // through the vendored derive like any other scalar leaf.
                _ => buffer_scalar_or_skip(key, NUMBER_DATA_POINT_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut ndp = if pairs.is_empty() {
            NumberDataPoint::default()
        } else {
            finish_via_derive(&pairs)?
        };
        ndp.attributes = attributes;
        ndp.exemplars = exemplars;
        Ok(ndp)
    }
}

struct HistogramDataPointSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for HistogramDataPointSeed<'_> {
    type Value = HistogramDataPoint;

    fn deserialize<D>(self, deserializer: D) -> Result<HistogramDataPoint, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for HistogramDataPointSeed<'_> {
    type Value = HistogramDataPoint;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON HistogramDataPoint object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<HistogramDataPoint, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut exemplars: Vec<Exemplar> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, agg)?,
                "bucketCounts" | "bucket_counts" => {
                    let v =
                        accumulate_bounded_scalar_array(&mut map, (MAX_BUCKETS, "bucketCounts"))?;
                    pairs.push(("bucketCounts".to_string(), v));
                }
                "explicitBounds" | "explicit_bounds" => {
                    let v =
                        accumulate_bounded_scalar_array(&mut map, (MAX_BUCKETS, "explicitBounds"))?;
                    pairs.push(("explicitBounds".to_string(), v));
                }
                "exemplars" => accumulate_msgs(
                    &mut map,
                    &mut exemplars,
                    (MAX_EXEMPLARS, "exemplars"),
                    Some(AggCharge {
                        cell: &agg.exemplars,
                        cap: MAX_TOTAL_EXEMPLARS,
                        field: "total exemplars",
                    }),
                    || ExemplarSeed { agg },
                )?,
                _ => {
                    buffer_scalar_or_skip(key, HISTOGRAM_DATA_POINT_SCALARS, &mut map, &mut pairs)?
                }
            }
        }
        // `bucketCounts`/`explicitBounds` are always buffered into `pairs`
        // above when present, so the `pairs.is_empty()` shortcut only fires
        // for a data point carrying JUST `attributes`/`exemplars` (or
        // nothing) — matches the vendored `serde(default)` derive exactly
        // either way.
        let mut hdp = if pairs.is_empty() {
            HistogramDataPoint::default()
        } else {
            finish_via_derive(&pairs)?
        };
        hdp.attributes = attributes;
        hdp.exemplars = exemplars;
        Ok(hdp)
    }
}

struct ExponentialHistogramDataPointSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ExponentialHistogramDataPointSeed<'_> {
    type Value = ExponentialHistogramDataPoint;

    fn deserialize<D>(self, deserializer: D) -> Result<ExponentialHistogramDataPoint, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ExponentialHistogramDataPointSeed<'_> {
    type Value = ExponentialHistogramDataPoint;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ExponentialHistogramDataPoint object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<ExponentialHistogramDataPoint, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut exemplars: Vec<Exemplar> = Vec::new();
        let mut positive: Option<Buckets> = None;
        let mut positive_seen = false;
        let mut negative: Option<Buckets> = None;
        let mut negative_seen = false;
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, agg)?,
                "positive" => {
                    if positive_seen {
                        return Err(de::Error::duplicate_field("positive"));
                    }
                    positive_seen = true;
                    positive = map.next_value_seed(OptionSeed(BucketsSeed))?;
                }
                "negative" => {
                    if negative_seen {
                        return Err(de::Error::duplicate_field("negative"));
                    }
                    negative_seen = true;
                    negative = map.next_value_seed(OptionSeed(BucketsSeed))?;
                }
                "exemplars" => accumulate_msgs(
                    &mut map,
                    &mut exemplars,
                    (MAX_EXEMPLARS, "exemplars"),
                    Some(AggCharge {
                        cell: &agg.exemplars,
                        cap: MAX_TOTAL_EXEMPLARS,
                        field: "total exemplars",
                    }),
                    || ExemplarSeed { agg },
                )?,
                _ => buffer_scalar_or_skip(
                    key,
                    EXP_HISTOGRAM_DATA_POINT_SCALARS,
                    &mut map,
                    &mut pairs,
                )?,
            }
        }
        let mut ehdp = if pairs.is_empty() {
            ExponentialHistogramDataPoint::default()
        } else {
            finish_via_derive(&pairs)?
        };
        ehdp.attributes = attributes;
        ehdp.exemplars = exemplars;
        ehdp.positive = positive;
        ehdp.negative = negative;
        Ok(ehdp)
    }
}

/// Bounded seed for `exponential_histogram_data_point::Buckets`: `bucketCounts`
/// (repeated uint64-as-string, [`MAX_BUCKETS`], both spellings, NO
/// aggregate — matches the protobuf pre-scan's `ExponentialHistogramBuckets`
/// cap exactly). `offset` is a plain scalar.
struct BucketsSeed;

impl<'de> DeserializeSeed<'de> for BucketsSeed {
    type Value = Buckets;

    fn deserialize<D>(self, deserializer: D) -> Result<Buckets, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for BucketsSeed {
    type Value = Buckets;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON ExponentialHistogramDataPoint.Buckets object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Buckets, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "bucketCounts" | "bucket_counts" => {
                    let v =
                        accumulate_bounded_scalar_array(&mut map, (MAX_BUCKETS, "bucketCounts"))?;
                    pairs.push(("bucketCounts".to_string(), v));
                }
                _ => buffer_scalar_or_skip(key, BUCKETS_SCALARS, &mut map, &mut pairs)?,
            }
        }
        finish_via_derive(&pairs)
    }
}

struct SummaryDataPointSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for SummaryDataPointSeed<'_> {
    type Value = SummaryDataPoint;

    fn deserialize<D>(self, deserializer: D) -> Result<SummaryDataPoint, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for SummaryDataPointSeed<'_> {
    type Value = SummaryDataPoint;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON SummaryDataPoint object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<SummaryDataPoint, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut quantile_values: Vec<ValueAtQuantile> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "attributes" => accumulate_attributes(&mut map, &mut attributes, agg)?,
                // `ValueAtQuantile` is all-scalar (module doc) — its element
                // seed is the vendored `Deserialize` directly, no bounded
                // wrapper needed.
                "quantileValues" | "quantile_values" => accumulate_msgs(
                    &mut map,
                    &mut quantile_values,
                    (MAX_QUANTILES, "quantileValues"),
                    None,
                    || std::marker::PhantomData::<ValueAtQuantile>,
                )?,
                _ => buffer_scalar_or_skip(key, SUMMARY_DATA_POINT_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut sdp = if pairs.is_empty() {
            SummaryDataPoint::default()
        } else {
            finish_via_derive(&pairs)?
        };
        sdp.attributes = attributes;
        sdp.quantile_values = quantile_values;
        Ok(sdp)
    }
}

struct ExemplarSeed<'a> {
    agg: &'a JsonAggregates,
}

impl<'de> DeserializeSeed<'de> for ExemplarSeed<'_> {
    type Value = Exemplar;

    fn deserialize<D>(self, deserializer: D) -> Result<Exemplar, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ExemplarSeed<'_> {
    type Value = Exemplar;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a proto3-JSON Exemplar object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Exemplar, A::Error>
    where
        A: MapAccess<'de>,
    {
        let agg = self.agg;
        let mut filtered_attributes: Vec<KeyValue> = Vec::new();
        let mut pairs: Vec<(String, serde_json::Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                // Shares the `attributes` cap/aggregate (matches the
                // protobuf pre-scan's `attr_count()` reuse on `Exemplar`
                // field 7 — same counter, different JSON key name).
                "filteredAttributes" | "filtered_attributes" => {
                    accumulate_attributes(&mut map, &mut filtered_attributes, agg)?
                }
                // `value` (P6, `asDouble`/`asInt`): pure scalar oneof, no
                // hand-routing needed (module doc).
                _ => buffer_scalar_or_skip(key, EXEMPLAR_SCALARS, &mut map, &mut pairs)?,
            }
        }
        let mut exemplar = if pairs.is_empty() {
            Exemplar::default()
        } else {
            finish_via_derive(&pairs)?
        };
        exemplar.filtered_attributes = filtered_attributes;
        Ok(exemplar)
    }
}

#[cfg(test)]
mod tests;
