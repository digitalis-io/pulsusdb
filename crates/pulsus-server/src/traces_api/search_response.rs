//! Assembles the documented `GET /api/traces/v1/search` JSON response
//! (docs/api.md §4.2) from `pulsus_read::SearchOutput` — response
//! shaping stays server-side so `pulsus-read` stays format-agnostic
//! (issue #55 layering). 64-bit nanosecond timestamps are emitted as
//! JSON strings (protojson convention, same as the trace-fetch surface);
//! `durationMs` is integer milliseconds.

use serde_json::{Value, json};

use pulsus_read::{GroupValue, SearchOutput, SpanSetGroup, SpanSummary, TraceSearchResult};

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn duration_ms(duration_ns: i64) -> i64 {
    duration_ns / 1_000_000
}

fn span_json(span: &SpanSummary) -> Value {
    let mut obj = json!({
        "spanID": hex(&span.span_id),
        "name": span.name,
        "startTimeUnixNano": span.start_ns.to_string(),
        "durationMs": duration_ms(span.duration_ns),
    });
    if !span.attributes.is_empty() {
        obj["attributes"] = Value::Array(
            span.attributes
                .iter()
                .map(|(key, value)| json!({"key": key, "value": {"stringValue": value}}))
                .collect(),
        );
    }
    obj
}

/// One `by()` group-key value → the reference's typed `value:{…}` object
/// (issue #193). A `Double` renders from its `canonical_double_bits`
/// pattern via `f64::from_bits`; `Nil` renders no value object (the span
/// carried no value for this key).
fn group_value_json(value: &GroupValue) -> Value {
    match value {
        GroupValue::Str(s) => json!({ "stringValue": s }),
        GroupValue::Int(i) => json!({ "intValue": i.to_string() }),
        GroupValue::Double(bits) => json!({ "doubleValue": f64::from_bits(*bits) }),
        GroupValue::Bool(b) => json!({ "boolValue": b }),
        GroupValue::Nil => Value::Null,
    }
}

/// One `by()`-produced spanSet (issue #193): the typed group-key
/// `attributes` plus the per-group `matched` count and `spss`-capped span
/// summaries.
fn group_json(group: &SpanSetGroup) -> Value {
    json!({
        "attributes": group
            .attributes
            .iter()
            .map(|(key, value)| json!({"key": key, "value": group_value_json(value)}))
            .collect::<Vec<_>>(),
        "matched": group.matched,
        "spans": group.spans.iter().map(span_json).collect::<Vec<_>>(),
    })
}

fn trace_json(trace: &TraceSearchResult) -> Value {
    // Issue #193: when a `by()` grouping is active (`groups` is `Some`),
    // emit one spanSet per group carrying typed `attributes`; otherwise
    // the flat single-spanSet path is byte-identical to the pre-#193
    // response.
    let span_sets = match &trace.groups {
        Some(groups) => groups.iter().map(group_json).collect::<Vec<_>>(),
        None => vec![json!({
            "matched": trace.matched,
            "spans": trace.spans.iter().map(span_json).collect::<Vec<_>>(),
        })],
    };
    json!({
        "traceID": hex(&trace.trace_id),
        "rootServiceName": trace.root.service,
        "rootTraceName": trace.root.name,
        "startTimeUnixNano": trace.root.start_ns.to_string(),
        "durationMs": duration_ms(trace.root.duration_ns),
        "spanSets": span_sets,
    })
}

/// The full documented response envelope — `traces` in the engine's
/// public order (max matched-span timestamp DESC, trace id ASC) plus the
/// `metrics.{partial,limit,returned}` partial-results contract.
pub(crate) fn render(output: &SearchOutput) -> Value {
    json!({
        "traces": output.traces.iter().map(trace_json).collect::<Vec<_>>(),
        "metrics": {
            "partial": output.partial,
            "limit": output.limit,
            "returned": output.returned,
        },
    })
}

#[cfg(test)]
mod tests {
    use pulsus_read::RootSummary;

    use super::*;

    fn sample_output() -> SearchOutput {
        SearchOutput {
            traces: vec![TraceSearchResult {
                trace_id: [0xab; 16],
                root: RootSummary {
                    service: "checkout".to_string(),
                    name: "GET /pay".to_string(),
                    start_ns: 1_700_000_000_000_000_000,
                    duration_ns: 2_500_000_000,
                },
                matched: 5,
                spans: vec![SpanSummary {
                    span_id: [0xcd; 8],
                    name: "charge".to_string(),
                    start_ns: 1_700_000_000_100_000_000,
                    duration_ns: 42_000_000,
                    attributes: vec![("span.foo".to_string(), "bar".to_string())],
                }],
                groups: None,
            }],
            partial: true,
            returned: 1,
            limit: 20,
        }
    }

    #[test]
    fn render_emits_the_documented_envelope() {
        let v = render(&sample_output());
        assert_eq!(
            v["traces"][0]["traceID"],
            "abababababababababababababababab"
        );
        assert_eq!(v["traces"][0]["rootServiceName"], "checkout");
        assert_eq!(v["traces"][0]["rootTraceName"], "GET /pay");
        assert_eq!(v["traces"][0]["startTimeUnixNano"], "1700000000000000000");
        assert_eq!(v["traces"][0]["durationMs"], 2500);
        assert_eq!(v["metrics"]["partial"], true);
        assert_eq!(v["metrics"]["limit"], 20);
        assert_eq!(v["metrics"]["returned"], 1);
    }

    #[test]
    fn span_sets_carry_matched_count_and_span_summaries() {
        let v = render(&sample_output());
        let set = &v["traces"][0]["spanSets"][0];
        assert_eq!(set["matched"], 5);
        assert_eq!(set["spans"][0]["spanID"], "cdcdcdcdcdcdcdcd");
        assert_eq!(set["spans"][0]["name"], "charge");
        assert_eq!(set["spans"][0]["durationMs"], 42);
        assert_eq!(
            set["spans"][0]["attributes"][0],
            serde_json::json!({"key": "span.foo", "value": {"stringValue": "bar"}})
        );
    }

    #[test]
    fn an_empty_output_renders_the_documented_empty_envelope() {
        let v = render(&SearchOutput {
            traces: vec![],
            partial: false,
            returned: 0,
            limit: 20,
        });
        assert_eq!(v["traces"], serde_json::json!([]));
        assert_eq!(v["metrics"]["partial"], false);
        assert_eq!(v["metrics"]["returned"], 0);
    }

    #[test]
    fn spans_without_selected_fields_omit_the_attributes_key() {
        let mut output = sample_output();
        output.traces[0].spans[0].attributes.clear();
        let v = render(&output);
        assert!(
            v["traces"][0]["spanSets"][0]["spans"][0]
                .get("attributes")
                .is_none()
        );
    }

    fn group_span(id: u8, name: &str) -> SpanSummary {
        SpanSummary {
            span_id: [id; 8],
            name: name.to_string(),
            start_ns: 1_700_000_000_100_000_000,
            duration_ns: 1_000_000,
            attributes: vec![],
        }
    }

    /// Issue #193: an active `by()` grouping emits ONE spanSet per group,
    /// each carrying typed `attributes`; the flat `matched`/`spans` are not
    /// serialized.
    #[test]
    fn grouped_output_emits_one_span_set_per_group_with_typed_attributes() {
        let mut output = sample_output();
        output.traces[0].groups = Some(vec![
            SpanSetGroup {
                attributes: vec![(
                    "resource.service.name".to_string(),
                    GroupValue::Str("checkout".to_string()),
                )],
                matched: 2,
                spans: vec![group_span(0x01, "a")],
            },
            SpanSetGroup {
                attributes: vec![(
                    "resource.service.name".to_string(),
                    GroupValue::Str("billing".to_string()),
                )],
                matched: 3,
                spans: vec![group_span(0x02, "b"), group_span(0x03, "c")],
            },
        ]);
        let v = render(&output);
        let sets = v["traces"][0]["spanSets"].as_array().expect("array");
        assert_eq!(sets.len(), 2, "one spanSet per group");
        assert_eq!(sets[0]["matched"], 2);
        assert_eq!(
            sets[0]["attributes"][0],
            serde_json::json!({
                "key": "resource.service.name",
                "value": {"stringValue": "checkout"}
            })
        );
        assert_eq!(sets[0]["spans"][0]["spanID"], "0101010101010101");
        assert_eq!(sets[1]["matched"], 3);
        assert_eq!(sets[1]["spans"].as_array().expect("spans").len(), 2);
    }

    /// Issue #193: numeric / double / bool / nil group-key values render
    /// their reference-typed `value:{…}` objects.
    #[test]
    fn grouped_output_renders_each_group_value_type() {
        let mut output = sample_output();
        output.traces[0].groups = Some(vec![SpanSetGroup {
            attributes: vec![
                ("span.count".to_string(), GroupValue::Int(7)),
                (
                    "span.ratio".to_string(),
                    GroupValue::Double(pulsus_read::canonical_double_bits(1.5)),
                ),
                ("span.ok".to_string(), GroupValue::Bool(true)),
                ("span.missing".to_string(), GroupValue::Nil),
            ],
            matched: 1,
            spans: vec![group_span(0x09, "z")],
        }]);
        let v = render(&output);
        let attrs = &v["traces"][0]["spanSets"][0]["attributes"];
        assert_eq!(attrs[0]["value"], serde_json::json!({"intValue": "7"}));
        assert_eq!(attrs[1]["value"], serde_json::json!({"doubleValue": 1.5}));
        assert_eq!(attrs[2]["value"], serde_json::json!({"boolValue": true}));
        assert_eq!(attrs[3]["value"], serde_json::Value::Null);
    }
}
