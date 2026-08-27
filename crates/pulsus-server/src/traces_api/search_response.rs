//! Assembles the documented `GET /api/traces/v1/search` JSON response
//! (docs/api.md §4.2) from `pulsus_read::SearchOutput` — response
//! shaping stays server-side so `pulsus-read` stays format-agnostic
//! (issue #55 layering). 64-bit nanosecond timestamps are emitted as
//! JSON strings (protojson convention, same as the trace-fetch surface).
//!
//! **The two duration fields sit at two different levels and are not the
//! same field** (issue #458). The reference's `TraceSearchMetadata` has
//! `uint32 durationMs` (`pkg/tempopb/tempo.proto:139` @ v3.0.2) — integer
//! MILLISECONDS, emitted here by [`trace_json`]. Its `Span` has
//! `uint64 durationNanos` (`pkg/tempopb/tempo.proto:160` @ v3.0.2), filled
//! from `span.DurationNanos()` (`pkg/traceql/engine.go:311` @ v3.0.2) —
//! NANOSECONDS, and protojson renders a `uint64` as a JSON **string**, so
//! [`span_json`] emits it as one. The string is load-bearing rather than
//! cosmetic: a span of 9007199254740993 ns (2^53 + 1) survives it and
//! would not survive a JSON number.
//!
//! **protojson OMITS a default-valued scalar**, so a zero-width span
//! carries no `durationNanos` key at all — captured, not reasoned:
//! against `grafana/tempo@sha256:aa8df8d0…` on
//! `GET /api/search?q={name="fresh-w0"}` the span object comes back as
//! `{"spanID":"…","name":"fresh-w0","startTimeUnixNano":"…"}` with the
//! field absent, while a 1 ns span carries `"durationNanos":"1"`.
//! [`span_json`] reproduces that. Emitting `"durationNanos":"0"` was the
//! shape this module shipped for one review round, and the tests were
//! green because they pinned OUR output at that width instead of the
//! reference's — which is why every width the unit test asserts is now a
//! captured response fragment.

use serde_json::{Value, json};

use pulsus_read::{GroupValue, SearchOutput, SpanSetGroup, SpanSummary, TraceSearchResult};

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Trace-level milliseconds only — `TraceSearchMetadata.durationMs`
/// (`pkg/tempopb/tempo.proto:139` @ v3.0.2). A span never carries this.
fn duration_ms(duration_ns: i64) -> i64 {
    duration_ns / 1_000_000
}

/// One `SpanSet.spans[]` entry. `durationNanos` is the reference's
/// `uint64` (`pkg/tempopb/tempo.proto:160` @ v3.0.2) rendered the way
/// protojson renders a `uint64`: as a JSON string, and **omitted entirely
/// when it is zero**, because protojson drops a default-valued scalar.
/// Zero is the width where a hand-written encoder and a protojson encoder
/// part company, so it is the one the tests must carry (see the module
/// doc for the captured bytes).
///
/// `duration_ns` is non-negative by ingest construction
/// (`otlp_traces::resolve_duration_ns` clamps `end < start` and
/// `end == 0` to `0`), and deliberately NOT re-clamped here: a negative
/// value must surface a writer regression, not be hidden by the renderer.
/// A negative therefore renders — it is not zero, so it is not omitted.
fn span_json(span: &SpanSummary) -> Value {
    let mut obj = json!({
        "spanID": hex(&span.span_id),
        "name": span.name,
        "startTimeUnixNano": span.start_ns.to_string(),
    });
    if span.duration_ns != 0 {
        obj["durationNanos"] = Value::String(span.duration_ns.to_string());
    }
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

    /// Issue #458 defect A: a span carries `durationNanos` as a protojson
    /// `uint64` — a JSON **string** of NANOSECONDS
    /// (`pkg/tempopb/tempo.proto:160`, `pkg/traceql/engine.go:311` @
    /// v3.0.2) — and carries no `durationMs` at all. The Grafana Tempo
    /// datasource reads exactly this field: `src/types.ts:89` types it
    /// `durationNanos: string` and `src/resultTransformer.ts:942` does
    /// `parseInt(span.durationNanos, 10)` into an `ns`-unit frame column,
    /// so an absent field renders the Duration column as `NaN`.
    #[test]
    fn span_sets_carry_matched_count_and_span_summaries() {
        let v = render(&sample_output());
        let set = &v["traces"][0]["spanSets"][0];
        assert_eq!(set["matched"], 5);
        assert_eq!(set["spans"][0]["spanID"], "cdcdcdcdcdcdcdcd");
        assert_eq!(set["spans"][0]["name"], "charge");
        assert_eq!(
            set["spans"][0]["durationNanos"],
            serde_json::Value::String("42000000".to_string()),
            "protojson renders a uint64 as a JSON string, not a number"
        );
        assert!(
            set["spans"][0].get("durationNanos").is_some(),
            "a non-zero width is present; only zero is omitted"
        );
        assert!(
            set["spans"][0].get("durationMs").is_none(),
            "the reference's Span message has no durationMs field; it belongs to \
             TraceSearchMetadata one level up"
        );
        assert_eq!(
            set["spans"][0]["attributes"][0],
            serde_json::json!({"key": "span.foo", "value": {"stringValue": "bar"}})
        );
    }

    /// Issue #458 defect A: every width is asserted against a span object
    /// **captured from the reference**, not against one written from our
    /// own output.
    ///
    /// That distinction is the whole point of this test. The first cut of
    /// it listed six widths and spelled the expected value for each from
    /// what this module already emitted; at `0` that was
    /// `"durationNanos":"0"` and the reference emits **no key at all**, so
    /// the suite was green while the byte-parity contract it exists to
    /// enforce was false. An expected value taken from the implementation
    /// cannot fail the way the implementation fails.
    ///
    /// **Provenance.** `grafana/tempo@sha256:aa8df8d069f77b82e978464daf551`
    /// `69bb8d135852ad58700aa96880653c3d8f7` — the digest
    /// `.github/workflows/ci.yml` pins — started with this repo's
    /// `ci/tempo/tempo-compare.yaml` unmodified, one OTLP JSON span per
    /// width pushed to `POST /v1/traces`, then
    /// `GET /api/search?q={name="<n>"}&start=<now-300>&end=<now+60>&limit=5`.
    /// The `i64::MAX` row needed a different window: the reference parses
    /// `end` as a `uint32` of seconds and answers
    /// `invalid end: strconv.ParseUint: parsing "9223372037": value out of
    /// range`, and it caps a range at 168h — so that span was anchored at
    /// `startTimeUnixNano = 1` and captured over `start=0&end=604800`.
    ///
    /// **Key ORDER is deliberately not compared.** `serde_json` sorts
    /// object keys and the reference emits declaration order; a JSON
    /// object is unordered and the datasource reads `span.durationNanos`
    /// by key (`src/resultTransformer.ts:942`). What IS compared is the
    /// key SET — which is how the missing-at-zero case is caught — and
    /// every value with its JSON type.
    #[test]
    fn every_span_width_renders_the_reference_captured_span_object() {
        // (duration_ns, the span object the reference returned for it).
        // Verbatim capture: `spanID`, `name` and `startTimeUnixNano` are
        // the fixture's own, so the comparison is total rather than
        // field-by-field.
        let captures: [(i64, &str); 7] = [
            (
                0,
                r#"{"spanID":"4620000000000000","name":"fresh-w0","startTimeUnixNano":"1787855428000000000"}"#,
            ),
            (
                1,
                r#"{"spanID":"4620000000000001","name":"fresh-w1","startTimeUnixNano":"1787855429000000000","durationNanos":"1"}"#,
            ),
            (
                545_000,
                r#"{"spanID":"4620000000000002","name":"fresh-w545000","startTimeUnixNano":"1787855430000000000","durationNanos":"545000"}"#,
            ),
            (
                42_000_000,
                r#"{"spanID":"4620000000000003","name":"fresh-w42000000","startTimeUnixNano":"1787855431000000000","durationNanos":"42000000"}"#,
            ),
            (
                9_007_199_254_740_993,
                r#"{"spanID":"4620000000000004","name":"fresh-w2p53p1","startTimeUnixNano":"1787855432000000000","durationNanos":"9007199254740993"}"#,
            ),
            (
                2_500_000_000,
                r#"{"spanID":"4620000000000005","name":"fresh-wtrace2500","startTimeUnixNano":"1787855433000000000","durationNanos":"2500000000"}"#,
            ),
            (
                i64::MAX,
                r#"{"spanID":"4600000064000000","name":"width-i64max-only","startTimeUnixNano":"1","durationNanos":"9223372036854775807"}"#,
            ),
        ];

        // The capture set must actually contain the discriminating widths;
        // a future edit that drops one of them fails here rather than
        // quietly narrowing what this test covers.
        let widths: Vec<i64> = captures.iter().map(|(ns, _)| *ns).collect();
        for required in [0, 1, 9_007_199_254_740_993, i64::MAX] {
            assert!(
                widths.contains(&required),
                "the captured set must carry the {required} ns width"
            );
        }

        for (duration_ns, captured) in captures {
            let want: serde_json::Value =
                serde_json::from_str(captured).expect("the capture is valid JSON");
            let span_id: Vec<u8> = (0..8)
                .map(|i| {
                    let hex = &want["spanID"].as_str().expect("spanID")[i * 2..i * 2 + 2];
                    u8::from_str_radix(hex, 16).expect("hex byte")
                })
                .collect();
            let got = span_json(&SpanSummary {
                span_id: span_id.try_into().expect("8 bytes"),
                name: want["name"].as_str().expect("name").to_string(),
                start_ns: want["startTimeUnixNano"]
                    .as_str()
                    .expect("startTimeUnixNano")
                    .parse()
                    .expect("i64 nanos"),
                duration_ns,
                attributes: vec![],
            });
            assert_eq!(
                got, want,
                "{duration_ns} ns: our span object must be the reference's, key set and all \
                 (a JSON string is not a JSON number, and an ABSENT key is not a zero one)"
            );
        }
    }

    /// Issue #458: the TRACE level is unchanged and must stay
    /// `durationMs` — integer milliseconds from the root span's duration
    /// (`pkg/tempopb/tempo.proto:139` @ v3.0.2). This is the field the
    /// issue records as already correct.
    #[test]
    fn the_trace_level_keeps_integer_millisecond_duration_ms() {
        let v = render(&sample_output());
        assert_eq!(v["traces"][0]["durationMs"], 2500);
        assert!(
            v["traces"][0].get("durationNanos").is_none(),
            "the trace level has no durationNanos in the reference"
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
                    "by(resource.service.name)".to_string(),
                    GroupValue::Str("checkout".to_string()),
                )],
                matched: 2,
                spans: vec![group_span(0x01, "a")],
            },
            SpanSetGroup {
                attributes: vec![(
                    "by(resource.service.name)".to_string(),
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
                "key": "by(resource.service.name)",
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
