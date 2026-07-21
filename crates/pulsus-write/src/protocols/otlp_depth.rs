//! Shared OTLP `AnyValue` recursion-depth guard (finding #54).
//!
//! An OTLP `AnyValue` is a recursive sum type: `ArrayValue` holds a
//! `Vec<AnyValue>` and `KvlistValue` a `Vec<KeyValue>` whose values are
//! themselves `AnyValue`s. Every render path in the OTLP parsers
//! (`any_value_to_string` -> `any_value_to_json`) descends this tree with
//! unbounded *native* recursion, so a maliciously deep — yet tiny on the
//! wire — request can exhaust the stack and abort the process before any
//! per-element or byte budget ever fires.
//!
//! This module bounds nesting to [`MAX_ANYVALUE_DEPTH`] with an **iterative**
//! (frame-stack) pre-pass invoked at the very top of each OTLP `parse`,
//! rejecting the WHOLE request (HTTP 400 / `google.rpc.Status.code = 3`, via
//! [`LogsIngestError::OversizeMessage`]) before a single value is rendered or
//! a single row materialized. The guard itself never recurses, so it cannot
//! overflow while guarding, and it short-circuits on the first over-cap node,
//! so a pathologically deep chain is rejected after descending at most
//! `MAX_ANYVALUE_DEPTH + 1` levels rather than being walked to its full depth.
//!
//! The frame-stack holds one *iterator* per open container level, advanced a
//! single child at a time, so its auxiliary memory is O(nesting depth) —
//! bounded by [`MAX_ANYVALUE_DEPTH`] frames — and NOT O(container width). A
//! deliberately WIDE-but-shallow container (millions of siblings) therefore
//! costs the guard the same fixed handful of frames as a narrow one; it never
//! materializes all of a level's siblings on the heap at once the way a
//! push-all-children work-stack would (an allocation-DoS vector native
//! recursion also never exposes, since it descends one child at a time).
//!
//! Scope is depth only: repeated-field element-count bounds are a separate
//! pre-scan concern, not built here.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{Exemplar, NumberDataPoint, metric};

use crate::error::LogsIngestError;

/// Maximum nesting depth of an OTLP `AnyValue` tree accepted on any ingest
/// path. The outermost `AnyValue` sits at depth 1; each `ArrayValue`/
/// `KvlistValue` container adds one level, so this admits up to 32 stacked
/// containers. Far above any legitimate telemetry attribute (real producers
/// nest a handful of levels at most) yet low enough that the subsequent
/// bounded render recursion cannot approach the stack limit.
pub const MAX_ANYVALUE_DEPTH: usize = 32;

/// The `field` label carried by the [`LogsIngestError::OversizeMessage`] a
/// depth violation raises — the message reads "AnyValue nesting depth count
/// {N} exceeds the documented limit of 32".
const DEPTH_FIELD: &str = "AnyValue nesting depth";

/// A borrowed iterator over one open container level's child `AnyValue`s. An
/// `ArrayValue` yields its elements directly; a `KvlistValue` yields each
/// `KeyValue`'s `value` (skipping entries whose `value` is absent). Each
/// variant is a plain [`slice::Iter`], so a frame holding one is O(1) memory
/// regardless of how many siblings the level has.
enum ContainerIter<'a> {
    Array(std::slice::Iter<'a, AnyValue>),
    Kvlist(std::slice::Iter<'a, KeyValue>),
}

impl<'a> ContainerIter<'a> {
    /// Advances by one child, returning the next nested `AnyValue` or `None`
    /// when the level is exhausted. Kvlist entries with no `value` are skipped
    /// in-place (they contribute no nested node) without buffering siblings.
    fn next_child(&mut self) -> Option<&'a AnyValue> {
        match self {
            ContainerIter::Array(iter) => iter.next(),
            ContainerIter::Kvlist(iter) => {
                for kv in iter.by_ref() {
                    if let Some(child) = kv.value.as_ref() {
                        return Some(child);
                    }
                }
                None
            }
        }
    }
}

/// One open container level: the iterator over its remaining children plus the
/// level's own depth (root container = depth 1).
struct Frame<'a> {
    iter: ContainerIter<'a>,
    depth: usize,
}

/// Opens a child-iterator frame for `value` iff it is a nesting container
/// (`ArrayValue`/`KvlistValue`), the only kinds [`ensure_anyvalue_depth`] must
/// descend into. Scalars, the empty oneof, and the unresolved string-index
/// kind yield `None` — no nested `AnyValue` to walk.
fn container_iter(value: &AnyValue) -> Option<ContainerIter<'_>> {
    match value.value.as_ref() {
        Some(Value::ArrayValue(array)) => Some(ContainerIter::Array(array.values.iter())),
        Some(Value::KvlistValue(kvlist)) => Some(ContainerIter::Kvlist(kvlist.values.iter())),
        _ => None,
    }
}

/// Iteratively verifies that `root`'s `AnyValue` nesting does not exceed
/// [`MAX_ANYVALUE_DEPTH`]. Uses a frame-stack of borrowed child-iterators
/// (never native recursion), advancing one child at a time so its auxiliary
/// memory stays O(nesting depth) — at most `MAX_ANYVALUE_DEPTH` frames — and
/// NEVER O(container width). Returns [`LogsIngestError::OversizeMessage`] on
/// the first node past the cap, so the walk descends at most
/// `MAX_ANYVALUE_DEPTH + 1` levels down any branch before rejecting.
pub fn ensure_anyvalue_depth(root: &AnyValue) -> Result<(), LogsIngestError> {
    // Scalar / empty / string-index root: nothing nests, so return without
    // allocating a frame-stack. This is the overwhelmingly common case — a
    // plain attribute value — so the guard stays allocation-free on the hot
    // decode path; only a genuinely nested container root pays for a heap
    // stack. (Keeps the metrics `parse` allocation budget, issue #62, intact:
    // a request of N scalar-attribute data points adds zero allocations here.)
    let Some(root_iter) = container_iter(root) else {
        return Ok(());
    };
    // Pre-size to the depth cap: the stack can never exceed MAX_ANYVALUE_DEPTH
    // frames (a frame at depth d is only pushed when d <= MAX_ANYVALUE_DEPTH),
    // so this single allocation is the guard's entire auxiliary heap cost —
    // fixed, and independent of any level's width.
    let mut stack: Vec<Frame<'_>> = Vec::with_capacity(MAX_ANYVALUE_DEPTH);
    stack.push(Frame {
        iter: root_iter,
        depth: 1,
    });
    while let Some(frame) = stack.last_mut() {
        let depth = frame.depth;
        match frame.iter.next_child() {
            Some(child) => {
                let child_depth = depth + 1;
                if child_depth > MAX_ANYVALUE_DEPTH {
                    return Err(LogsIngestError::OversizeMessage {
                        field: DEPTH_FIELD,
                        limit: MAX_ANYVALUE_DEPTH,
                        actual: child_depth,
                    });
                }
                // Only nesting containers open a new frame; a scalar child is
                // depth-checked above and then dropped without buffering.
                if let Some(child_iter) = container_iter(child) {
                    stack.push(Frame {
                        iter: child_iter,
                        depth: child_depth,
                    });
                }
            }
            // Level exhausted: pop it and resume its parent's iterator.
            None => {
                stack.pop();
            }
        }
    }
    Ok(())
}

/// Checks every `AnyValue` reachable through a `KeyValue` slice's values.
fn ensure_kvs_depth(kvs: &[KeyValue]) -> Result<(), LogsIngestError> {
    for kv in kvs {
        if let Some(value) = kv.value.as_ref() {
            ensure_anyvalue_depth(value)?;
        }
    }
    Ok(())
}

/// Whole-request `AnyValue` depth guard for `ExportTraceServiceRequest`:
/// resource / scope / span attributes plus every span event and link
/// attribute set. Invoked at the top of `otlp_traces::parse`.
pub fn ensure_trace_anyvalue_depth(req: &ExportTraceServiceRequest) -> Result<(), LogsIngestError> {
    for resource_spans in &req.resource_spans {
        if let Some(resource) = resource_spans.resource.as_ref() {
            ensure_kvs_depth(&resource.attributes)?;
        }
        for scope_spans in &resource_spans.scope_spans {
            if let Some(scope) = scope_spans.scope.as_ref() {
                ensure_kvs_depth(&scope.attributes)?;
            }
            for span in &scope_spans.spans {
                ensure_kvs_depth(&span.attributes)?;
                for event in &span.events {
                    ensure_kvs_depth(&event.attributes)?;
                }
                for link in &span.links {
                    ensure_kvs_depth(&link.attributes)?;
                }
            }
        }
    }
    Ok(())
}

/// Whole-request `AnyValue` depth guard for `ExportMetricsServiceRequest`:
/// resource / scope attributes, per-metric metadata, and every data point's
/// attributes plus exemplar `filtered_attributes` across all five metric
/// oneof kinds. Invoked at the top of `otlp_metrics::parse`.
pub fn ensure_metrics_anyvalue_depth(
    req: &ExportMetricsServiceRequest,
) -> Result<(), LogsIngestError> {
    for resource_metrics in &req.resource_metrics {
        if let Some(resource) = resource_metrics.resource.as_ref() {
            ensure_kvs_depth(&resource.attributes)?;
        }
        for scope_metrics in &resource_metrics.scope_metrics {
            if let Some(scope) = scope_metrics.scope.as_ref() {
                ensure_kvs_depth(&scope.attributes)?;
            }
            for metric in &scope_metrics.metrics {
                ensure_kvs_depth(&metric.metadata)?;
                let Some(data) = metric.data.as_ref() else {
                    continue;
                };
                match data {
                    metric::Data::Gauge(gauge) => ensure_number_points_depth(&gauge.data_points)?,
                    metric::Data::Sum(sum) => ensure_number_points_depth(&sum.data_points)?,
                    metric::Data::Histogram(histogram) => {
                        for dp in &histogram.data_points {
                            ensure_kvs_depth(&dp.attributes)?;
                            ensure_exemplars_depth(&dp.exemplars)?;
                        }
                    }
                    metric::Data::ExponentialHistogram(histogram) => {
                        for dp in &histogram.data_points {
                            ensure_kvs_depth(&dp.attributes)?;
                            ensure_exemplars_depth(&dp.exemplars)?;
                        }
                    }
                    metric::Data::Summary(summary) => {
                        for dp in &summary.data_points {
                            ensure_kvs_depth(&dp.attributes)?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Whole-request `AnyValue` depth guard for `ExportLogsServiceRequest`:
/// resource / scope attributes, and each log record's attributes plus its
/// `body` `AnyValue`. Invoked at the top of `otlp_logs::parse`.
pub fn ensure_logs_anyvalue_depth(req: &ExportLogsServiceRequest) -> Result<(), LogsIngestError> {
    for resource_logs in &req.resource_logs {
        if let Some(resource) = resource_logs.resource.as_ref() {
            ensure_kvs_depth(&resource.attributes)?;
        }
        for scope_logs in &resource_logs.scope_logs {
            if let Some(scope) = scope_logs.scope.as_ref() {
                ensure_kvs_depth(&scope.attributes)?;
            }
            for record in &scope_logs.log_records {
                ensure_kvs_depth(&record.attributes)?;
                if let Some(body) = record.body.as_ref() {
                    ensure_anyvalue_depth(body)?;
                }
            }
        }
    }
    Ok(())
}

/// `NumberDataPoint` (Gauge/Sum) depth walk: attributes + exemplars.
fn ensure_number_points_depth(points: &[NumberDataPoint]) -> Result<(), LogsIngestError> {
    for dp in points {
        ensure_kvs_depth(&dp.attributes)?;
        ensure_exemplars_depth(&dp.exemplars)?;
    }
    Ok(())
}

/// Exemplar `filtered_attributes` depth walk.
fn ensure_exemplars_depth(exemplars: &[Exemplar]) -> Result<(), LogsIngestError> {
    for exemplar in exemplars {
        ensure_kvs_depth(&exemplar.filtered_attributes)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};

    /// Builds an `AnyValue` nested `levels` `AnyValue` nodes deep: a scalar
    /// leaf (level `levels`) wrapped in `levels - 1` `ArrayValue` containers.
    /// `levels == 1` yields a bare scalar. Built iteratively so the builder
    /// itself never risks a deep-recursion overflow.
    fn nested_array(levels: usize) -> AnyValue {
        assert!(levels >= 1);
        let mut value = AnyValue {
            value: Some(Value::StringValue("leaf".to_string())),
        };
        for _ in 1..levels {
            value = AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![value],
                })),
            };
        }
        value
    }

    /// Iteratively dismantles a [`nested_array`] tree so its `Drop` does not
    /// recurse. A very deep tree would otherwise risk a stack overflow *on
    /// drop* (prost's derived `Drop` is recursive) — unrelated to the
    /// iterative code under test, but it would still crash the test process.
    fn drain(mut value: AnyValue) {
        while let Some(Value::ArrayValue(mut array)) = value.value.take() {
            match array.values.pop() {
                Some(child) => value = child,
                None => break,
            }
        }
    }

    /// The kvlist analog of [`nested_array`] — alternates the container kind
    /// so the `KvlistValue` descent arm is exercised too.
    fn nested_kvlist(levels: usize) -> AnyValue {
        assert!(levels >= 1);
        let mut value = AnyValue {
            value: Some(Value::StringValue("leaf".to_string())),
        };
        for _ in 1..levels {
            value = AnyValue {
                value: Some(Value::KvlistValue(KeyValueList {
                    values: vec![KeyValue {
                        key: "k".to_string(),
                        value: Some(value),
                        key_strindex: 0,
                    }],
                })),
            };
        }
        value
    }

    #[test]
    fn scalar_and_absent_values_are_within_depth() {
        ensure_anyvalue_depth(&nested_array(1)).expect("scalar leaf is depth 1");
        ensure_anyvalue_depth(&AnyValue { value: None }).expect("empty oneof descends nowhere");
    }

    #[test]
    fn nesting_at_the_cap_is_accepted() {
        ensure_anyvalue_depth(&nested_array(MAX_ANYVALUE_DEPTH))
            .expect("exactly MAX_ANYVALUE_DEPTH array levels is accepted");
        ensure_anyvalue_depth(&nested_kvlist(MAX_ANYVALUE_DEPTH))
            .expect("exactly MAX_ANYVALUE_DEPTH kvlist levels is accepted");
    }

    #[test]
    fn nesting_one_past_the_cap_is_rejected() {
        // Differs from the accepted case by exactly one container level, so
        // the rejection is attributable to the guard alone: without it this
        // tree parses identically to the `MAX_ANYVALUE_DEPTH` case above.
        let err = ensure_anyvalue_depth(&nested_array(MAX_ANYVALUE_DEPTH + 1))
            .expect_err("one level past the cap is rejected");
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                limit: MAX_ANYVALUE_DEPTH,
                actual,
                ..
            } if actual == MAX_ANYVALUE_DEPTH + 1
        ));
        ensure_anyvalue_depth(&nested_kvlist(MAX_ANYVALUE_DEPTH + 1))
            .expect_err("one kvlist level past the cap is rejected");
    }

    #[test]
    fn a_very_deep_chain_is_rejected_by_bounded_descent() {
        // A chain far deeper than any renderer's native recursion could
        // survive is rejected without the guard itself recursing (it walks at
        // most MAX_ANYVALUE_DEPTH + 1 levels, reporting that as `actual`).
        let deep = nested_array(10_000);
        let err = ensure_anyvalue_depth(&deep).expect_err("a 10k-deep chain is rejected");
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage { actual, .. } if actual == MAX_ANYVALUE_DEPTH + 1
        ));
        drain(deep);
    }
}
