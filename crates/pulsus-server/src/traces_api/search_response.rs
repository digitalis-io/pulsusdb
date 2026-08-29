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
//!
//! **The `metrics` block is `tempopb.SearchMetrics` and nothing else**
//! (issue #464). It used to carry `partial`, `limit` and `returned`, and
//! none of the three is a field of that message
//! (`pkg/tempopb/tempo.proto:164-172` @ v3.0.2). Grafana's Tempo
//! datasource decodes a search response with `jsonpb.Unmarshal`
//! (`pkg/tempo/search.go:95` @ `v13.1.5-11-g3c7375b`), which **rejects**
//! an unknown field and returns the error instead of results — so those
//! keys did not degrade trace search through that client, they disabled
//! it. [`render`] documents where the truncation signal went.
//!
//! Removing them is not sufficient for a strict client, and nothing here
//! claims it is: `durationMs` is `uint32` in the same message and we
//! emit an unwrapped `i64`, so one trace over ~49.7 days fails the same
//! decode and takes the whole response with it. That is one mechanism —
//! an integer emitted outside its proto field's domain — tracked
//! separately and deliberately not fixed field-by-field here.

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
/// (`pkg/tempopb/tempo.proto:139` @ v3.0.2). A span never carries this,
/// and the nanoseconds fed in are the TRACE's envelope width, not the
/// root span's (issue #464).
///
/// **Not truncated to `uint32`.** The reference computes
/// `uint32(spanset.DurationNanos / 1_000_000)` (`engine.go:295`), so any
/// trace longer than ~49.7 days wraps: measured on the pinned container,
/// a 2^53 + 1 ns trace comes back `durationMs: 417264662` and an
/// `i64::MAX` one `2077252342`, against our 9007199254 and 9223372036854.
/// That wrap is the reference being wrong and is deliberately not copied
/// (recorded on issue #458 as out of scope, with both numbers).
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

/// One `traces[]` entry.
///
/// **`startTimeUnixNano` and `durationMs` are TRACE-level and come from
/// the trace's envelope, not from the root span** (issue #464). The
/// reference fills both from the spanset (`pkg/traceql/engine.go:294-295`
/// @ v3.0.2), whose writer computes `traceStart` and
/// `traceEnd - traceStart` over EVERY span of the trace
/// (`tempodb/encoding/vparquet4/schema.go:558-560`). Filling them from the
/// root span answers wrongly whenever a child starts before the root
/// (clock skew), ends after it, or extends the trace past the root's end —
/// measured on an adversarial four-trace corpus, three of four traces.
/// `rootServiceName`/`rootTraceName` stay the root span's, because those
/// are what the reference takes from it.
///
/// `durationMs` follows the SAME protojson
/// default-omission rule as the span level (issue #458, review round 2):
/// it is dropped when it is zero, and it is zero for every SUB-MILLISECOND
/// trace, not only for a zero-width one. Captured against
/// `grafana/tempo@sha256:aa8df8d0…`: `0`, `1` and `545000` ns all come
/// back with **no `durationMs` key**, while `42000000` ns comes back
/// `"durationMs":42`. The unit is what the omission tests, so the test is
/// on `duration_ms(...)`, never on `duration_ns`.
///
/// The one place this can still part company with the reference is a
/// trace whose millisecond count is an exact multiple of 2^32 (~49.7
/// days): the reference's `uint32` wraps that to `0` and omits the field,
/// and we emit the true value. That is a consequence of NOT copying the
/// overflow, and it is the better answer.
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
    let mut obj = json!({
        "traceID": hex(&trace.trace_id),
        "rootServiceName": trace.root.service,
        "rootTraceName": trace.root.name,
        "startTimeUnixNano": trace.trace_start_ns.to_string(),
        "spanSets": span_sets,
    });
    let ms = duration_ms(trace.trace_duration_ns);
    if ms != 0 {
        obj["durationMs"] = Value::from(ms);
    }
    obj
}

/// The full documented response envelope: `traces` in the engine's public
/// order (max matched-span timestamp DESC, trace id ASC), plus `metrics` —
/// which is `tempopb.SearchMetrics` (`pkg/tempopb/tempo.proto:164-172` @
/// v3.0.2) and NOTHING else.
///
/// This block used to carry `partial`, `limit` and `returned`, and all
/// three were invented (issue #464). Grafana's Tempo datasource decodes a
/// search response with `jsonpb.Unmarshal` (`pkg/tempo/search.go:95` @
/// `v13.1.5-11-g3c7375b`), which REJECTS an unknown field and returns the
/// error instead of results — so those keys did not degrade trace search
/// through that client, they disabled it.
///
/// `limit` and `returned` are dropped outright: `limit` is the caller's own
/// request parameter and `returned` is `traces.len()`. `partial` has no
/// like-for-like home — `PartialStatus` (`tempo.proto:383-386`) is on
/// `TraceByIDResponse`, `QueryRangeResponse` and `QueryInstantResponse`,
/// never on `SearchResponse` — so it rides the pair the search route DOES
/// use for incompleteness: `completedJobs < totalJobs`
/// (`modules/frontend/combiner/response_metrics.go:19-38`,
/// `combiner/search.go:126-135`). We run one plan, so `totalJobs` is 1 and
/// `completedJobs` is 1 or 0. **A zero `completedJobs` is OMITTED**, the way
/// protojson omits a default scalar and the way Grafana's own
/// `(metrics.completedJobs || 0) / (metrics.totalJobs || 1)`
/// (`src/streaming.ts:316`) reads it.
pub(crate) fn render(output: &SearchOutput) -> Value {
    let mut metrics = serde_json::Map::new();
    if !output.partial {
        metrics.insert("completedJobs".to_string(), Value::from(1u32));
    }
    metrics.insert("totalJobs".to_string(), Value::from(1u32));
    json!({
        "traces": output.traces.iter().map(trace_json).collect::<Vec<_>>(),
        "metrics": Value::Object(metrics),
    })
}

#[cfg(test)]
mod tests {
    use pulsus_read::RootSummary;

    use super::*;

    /// Every field of `tempopb.SearchMetrics`
    /// (`pkg/tempopb/tempo.proto:164-172` @ grafana/tempo v3.0.2 /
    /// `0c4b926d09234186de39833e9c7ecb5b7614c8b9`), transcribed by hand.
    ///
    /// **This list is a transcription and nothing in CI checks it.**
    /// `tempopb` is not vendored and this workspace has no Go step, so a
    /// field added to the reference will not be noticed here, and a name
    /// mistyped into this list would let an invented key spelled that way
    /// through. It is checkable by a human against the checkout.
    const SEARCH_METRICS_FIELDS: [&str; 7] = [
        "inspectedTraces",
        "inspectedBytes",
        "totalBlocks",
        "completedJobs",
        "totalJobs",
        "totalBlockBytes",
        "inspectedSpans",
    ];

    /// The values of `SearchOutput::partial` — the ONLY field
    /// [`render`] branches on when it builds the `metrics` block, and so
    /// the exact enumeration the documented metrics shape is derived over
    /// (issue #464).
    ///
    /// **This is not "every branch of `render`".** `render` reaches at
    /// least five other branch points, all of them inside the `traces`
    /// array and none of them able to move a `metrics` key:
    /// `trace_json`'s `groups` split (`:137`) and its zero-`durationMs`
    /// omission (`:152`), `span_json`'s zero-`durationNanos` omission
    /// (`:75`) and its empty-`attributes` omission (`:78`), and
    /// `group_value_json`'s type match (`:94`). The constant covers the
    /// metrics projection only.
    const METRICS_PARTIALITY_BRANCHES: [bool; 2] = [false, true];

    fn sample_output() -> SearchOutput {
        SearchOutput {
            traces: vec![TraceSearchResult {
                trace_id: [0xab; 16],
                // Issue #464: the root span's window is deliberately NOT
                // the trace's — it starts 1 s in and is 1 ms wide, while
                // the trace starts at the base instant and runs 2500 ms.
                // A fixture where the two coincide cannot tell the
                // trace-level rule from the root-span rule, which is how
                // the root-span reading survived until now.
                root: RootSummary {
                    service: "checkout".to_string(),
                    name: "GET /pay".to_string(),
                    start_ns: 1_700_000_001_000_000_000,
                    duration_ns: 1_000_000,
                },
                trace_start_ns: 1_700_000_000_000_000_000,
                trace_duration_ns: 2_500_000_000,
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
        // Issue #464: both are TRACE-level. The fixture's root span
        // starts 1 s later and is 1 ms wide, so neither assertion below
        // can be satisfied by the root span's own window.
        let root = &sample_output().traces[0].root;
        assert_ne!(root.start_ns, 1_700_000_000_000_000_000);
        assert_ne!(root.duration_ns, 2_500_000_000);
        assert_eq!(v["traces"][0]["startTimeUnixNano"], "1700000000000000000");
        assert_eq!(v["traces"][0]["durationMs"], 2500);
        // Issue #464: the whole block, not three keys of it — the
        // retired `partial`/`limit`/`returned` were invented and a
        // per-key assertion cannot see a fourth key reappearing.
        assert_eq!(
            v["metrics"],
            serde_json::json!({"totalJobs": 1}),
            "the sample output is truncated: completedJobs is zero and protojson omits a zero"
        );
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
    /// **Capturing these is fiddlier than it looks, and none of it is
    /// about this repo.** Two things bit, and both cost time:
    ///
    /// * A search over a RECENT window can come back `{"traces":[]}` on a
    ///   freshly written block while `inspectedBytes` climbs — data
    ///   demonstrably read, nothing matched — so an empty answer is not
    ///   evidence of an empty store. Poll it; it starts answering.
    /// * Pushing the epoch-anchored `i64::MAX` fixture in the SAME batch
    ///   as the recent widths made every recent width unsearchable for
    ///   minutes. Two pushes into two blocks, not one payload.
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

    /// Issue #458 review round 2: the TRACE level follows the **same**
    /// protojson default-omission rule as the span level, and every width
    /// here is a trace object **captured from the reference**.
    ///
    /// This test replaces one that asserted `durationMs == 2500` and
    /// nothing else. That assertion was true and useless: it was chosen
    /// because the issue recorded the trace level as correct, and the
    /// widths that would have shown otherwise — every SUB-MILLISECOND one
    /// — were never tried. The reference omits `durationMs` for `0`, `1`
    /// and `545000` ns alike, because the field is MILLISECONDS and all
    /// three of those round to zero. The unit is what the omission tests.
    ///
    /// **Provenance.** Same container and route as
    /// [`every_span_width_renders_the_reference_captured_span_object`].
    /// `spanSet` (the deprecated singular) and `serviceStats` are removed
    /// from each capture before comparison: we do not emit either, which
    /// is a separately recorded gap and not this issue's to close.
    /// `spanSets` is removed from BOTH sides — its contents are span
    /// objects, and those are the other test's subject.
    ///
    /// **Where we deliberately differ, and it is pinned rather than
    /// skipped.** The reference truncates to `uint32`
    /// (`engine.go:295`), so a trace over ~49.7 days wraps. Two captured
    /// widths wrap and their values are asserted on BOTH sides, so the
    /// divergence cannot drift unnoticed:
    /// 2^53 + 1 ns is `417264662` there and `9007199254` here, and
    /// `i64::MAX` is `2077252342` there and `9223372036854` here.
    #[test]
    fn every_trace_width_renders_the_reference_captured_trace_object() {
        let captures: [(i64, &str); 7] = [
            (
                0,
                r#"{"traceID":"46300000000000000000000000000000","rootServiceName":"rev458c-w0","rootTraceName":"tl-w0","startTimeUnixNano":"1787856404000000000"}"#,
            ),
            (
                1,
                r#"{"traceID":"46300000000000000000000000000001","rootServiceName":"rev458c-w1","rootTraceName":"tl-w1","startTimeUnixNano":"1787856405000000000"}"#,
            ),
            (
                545_000,
                r#"{"traceID":"46300000000000000000000000000002","rootServiceName":"rev458c-w545000","rootTraceName":"tl-w545000","startTimeUnixNano":"1787856406000000000"}"#,
            ),
            (
                42_000_000,
                r#"{"traceID":"46300000000000000000000000000003","rootServiceName":"rev458c-w42000000","rootTraceName":"tl-w42000000","startTimeUnixNano":"1787856407000000000","durationMs":42}"#,
            ),
            (
                9_007_199_254_740_993,
                r#"{"traceID":"46300000000000000000000000000004","rootServiceName":"rev458c-w2p53p1","rootTraceName":"tl-w2p53p1","startTimeUnixNano":"1787856408000000000","durationMs":417264662}"#,
            ),
            (
                2_500_000_000,
                r#"{"traceID":"46300000000000000000000000000005","rootServiceName":"rev458c-w2500ms","rootTraceName":"tl-w2500ms","startTimeUnixNano":"1787856409000000000","durationMs":2500}"#,
            ),
            (
                9_223_372_036_854_775_807,
                r#"{"traceID":"46300000000000000000000064000000","rootServiceName":"rev458c-i64max","rootTraceName":"tl-i64max","startTimeUnixNano":"1","durationMs":2077252342}"#,
            ),
        ];
        // The set must keep the widths that discriminate: a zero, a
        // sub-millisecond non-zero (the case that makes this about
        // MILLISECONDS rather than nanoseconds), and a whole-millisecond
        // one.
        let widths: Vec<i64> = captures.iter().map(|(ns, _)| *ns).collect();
        for required in [0, 545_000, 42_000_000, i64::MAX] {
            assert!(
                widths.contains(&required),
                "the captured set must carry the {required} ns width"
            );
        }
        // DO NOT DELETE either of the two over-`u32::MAX` rows. They are
        // the only captures whose reference value differs from ours, and
        // therefore the only rows that can tell a truncating `durationMs`
        // from a non-truncating one; the rest agree under both rules and
        // would stay green if we started copying the overflow. Issue #464
        // makes this field carry the TRACE's width rather than the root
        // span's, which can only ENLARGE the set of stores that reach
        // them. If a later change makes these two rows behave
        // differently, make them pass — never remove them.
        let over_u32: Vec<i64> = widths
            .iter()
            .copied()
            .filter(|ns| ns / 1_000_000 > i64::from(u32::MAX))
            .collect();
        assert_eq!(
            over_u32.len(),
            2,
            "exactly the two widths above u32::MAX milliseconds must remain — they are the only rows that can tell a truncating durationMs from a non-truncating one, got {over_u32:?}"
        );

        for (duration_ns, captured) in captures {
            let want: serde_json::Value =
                serde_json::from_str(captured).expect("the capture is valid JSON");
            let trace_id: Vec<u8> = (0..16)
                .map(|i| {
                    let hex = &want["traceID"].as_str().expect("traceID")[i * 2..i * 2 + 2];
                    u8::from_str_radix(hex, 16).expect("hex byte")
                })
                .collect();
            let trace_start_ns: i64 = want["startTimeUnixNano"]
                .as_str()
                .expect("startTimeUnixNano")
                .parse()
                .expect("i64 nanos");
            let mut got = trace_json(&TraceSearchResult {
                trace_id: trace_id.try_into().expect("16 bytes"),
                // Issue #464: the widths under test are the TRACE's, so
                // the root span is given a deliberately different window —
                // 7 s later, 1 ns wide. If `trace_json` ever reads the
                // root span again, every row here fails.
                root: RootSummary {
                    service: want["rootServiceName"].as_str().expect("svc").to_string(),
                    name: want["rootTraceName"].as_str().expect("name").to_string(),
                    start_ns: trace_start_ns.saturating_add(7_000_000_000),
                    duration_ns: 1,
                },
                trace_start_ns,
                trace_duration_ns: duration_ns,
                matched: 1,
                spans: vec![],
                groups: None,
            });
            got.as_object_mut().expect("object").remove("spanSets");

            let ours_ms = duration_ns / 1_000_000;
            let wraps = ours_ms > i64::from(u32::MAX);
            if wraps {
                // The `uint32` overflow: the VALUES differ by design, so
                // assert both, plus that the key is present on both sides.
                let theirs = want["durationMs"].as_i64().expect("reference durationMs");
                assert_eq!(
                    theirs,
                    ours_ms % (i64::from(u32::MAX) + 1), // 2^32
                    "{duration_ns} ns: the reference's value must be our value wrapped to uint32 \
                     — if it is not, the divergence is no longer the overflow we recorded"
                );
                assert_eq!(
                    got["durationMs"].as_i64(),
                    Some(ours_ms),
                    "{duration_ns} ns: we emit the UNWRAPPED value on purpose"
                );
            } else {
                assert_eq!(
                    got, want,
                    "{duration_ns} ns: our trace object must be the reference's, key set and all \
                     — durationMs is MILLISECONDS, so every sub-millisecond width omits it"
                );
            }
        }
    }

    /// Issue #464: the trace level's `startTimeUnixNano` and `durationMs`
    /// are the TRACE's envelope — the spanset's `StartTimeUnixNanos` and
    /// `DurationNanos` (`pkg/traceql/engine.go:294-295` @ v3.0.2,
    /// computed over every span at
    /// `tempodb/encoding/vparquet4/schema.go:558-560`) — and not the root
    /// span's own window.
    ///
    /// **Provenance.** One identical OTLP/JSON push to the pinned
    /// reference container
    /// (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7`,
    /// this repo's `ci/tempo/tempo-compare.yaml` unmodified) and to
    /// PulsusDB, service `rev464b`, then
    /// `GET /api/search?q={resource.service.name="rev464b"}&start=<base-300>&end=<base+300>&limit=20`.
    /// The four trace objects below are the reference's answers verbatim,
    /// with `spanSet`/`spanSets`/`serviceStats` removed (we emit none of
    /// the three; separately recorded).
    ///
    /// **The corpus is adversarial by construction, and that is asserted
    /// before anything is compared.** Three of the four traces answer
    /// differently under the two rules — a root that starts after its
    /// child, a later child that extends the trace, and a child that ends
    /// after the root — and the fourth is a single-span control where the
    /// root IS the trace. A corpus whose root windows coincide with its
    /// traces' cannot tell the two rules apart, which is exactly how the
    /// root-span reading survived issue #458.
    #[test]
    fn the_trace_envelope_renders_the_reference_captured_start_and_duration() {
        // (root start_ns, root duration_ns, trace start_ns, trace
        //  duration_ns, the trace object the reference returned).
        const BASE: i64 = 1_787_922_982_000_000_000;
        let captures: [(i64, i64, i64, i64, &str); 4] = [
            (
                // root +5 s, 42 ms — it starts AFTER its own child.
                BASE + 5_000_000_000,
                42_000_000,
                BASE,
                5_042_000_000,
                r#"{"traceID":"a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1","rootServiceName":"rev464b","rootTraceName":"root-skew","startTimeUnixNano":"1787922982000000000","durationMs":5042}"#,
            ),
            (
                // root +0, 42 ms; a child 1 s later extends the trace.
                BASE,
                42_000_000,
                BASE,
                1_000_000_000,
                r#"{"traceID":"b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1","rootServiceName":"rev464b","rootTraceName":"root-short","startTimeUnixNano":"1787922982000000000","durationMs":1000}"#,
            ),
            (
                // root +0, 10 ms; a child starts inside it and ends after.
                BASE,
                10_000_000,
                BASE,
                35_000_000,
                r#"{"traceID":"d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1","rootServiceName":"rev464b","rootTraceName":"root-overrun","startTimeUnixNano":"1787922982000000000","durationMs":35}"#,
            ),
            (
                // The control: one span, so the root IS the trace and
                // both rules agree.
                BASE,
                2_500_000_000,
                BASE,
                2_500_000_000,
                r#"{"traceID":"c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1","rootServiceName":"rev464b","rootTraceName":"root-only","startTimeUnixNano":"1787922982000000000","durationMs":2500}"#,
            ),
        ];

        // The corpus relation, asserted BEFORE any value comparison: a
        // narrowed corpus fails here rather than passing under both rules.
        assert!(
            captures
                .iter()
                .any(|(root_start, _, trace_start, _, _)| root_start > trace_start),
            "the corpus must carry a trace whose ROOT starts after the trace does"
        );
        assert!(
            captures
                .iter()
                .filter(|(_, root_dur, _, trace_dur, _)| root_dur != trace_dur)
                .count()
                >= 2,
            "the corpus must carry at least two traces whose ROOT width differs from the trace's"
        );

        for (root_start_ns, root_duration_ns, trace_start_ns, trace_duration_ns, captured) in
            captures
        {
            let want: serde_json::Value =
                serde_json::from_str(captured).expect("the capture is valid JSON");
            let trace_id: Vec<u8> = (0..16)
                .map(|i| {
                    let hex = &want["traceID"].as_str().expect("traceID")[i * 2..i * 2 + 2];
                    u8::from_str_radix(hex, 16).expect("hex byte")
                })
                .collect();
            let mut got = trace_json(&TraceSearchResult {
                trace_id: trace_id.try_into().expect("16 bytes"),
                root: RootSummary {
                    service: want["rootServiceName"].as_str().expect("svc").to_string(),
                    name: want["rootTraceName"].as_str().expect("name").to_string(),
                    start_ns: root_start_ns,
                    duration_ns: root_duration_ns,
                },
                trace_start_ns,
                trace_duration_ns,
                matched: 1,
                spans: vec![],
                groups: None,
            });
            got.as_object_mut().expect("object").remove("spanSets");
            assert_eq!(
                got, want,
                "trace envelope ({trace_start_ns}, {trace_duration_ns} ns) against root span \
                 ({root_start_ns}, {root_duration_ns} ns): the rendered object must be the \
                 reference's — startTimeUnixNano and durationMs are TRACE-level"
            );
        }
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
        // Issue #464 AC 3: zero traces is not a truncated result, so
        // `completedJobs` must be PRESENT. This is the case a "just drop
        // the invented keys" fix sails through and a wrong-branch fix
        // does not.
        assert_eq!(
            v["metrics"],
            serde_json::json!({"completedJobs": 1, "totalJobs": 1}),
            "an empty complete search: one job, one completed"
        );
    }

    /// Issue #464 AC 1: the `metrics` block is the reference's own
    /// `completedJobs`/`totalJobs` pair, asserted as a WHOLE OBJECT on
    /// each completeness branch — a per-key assertion is what let three
    /// invented keys ship.
    ///
    /// `completedJobs` is omitted on the truncated branch because
    /// protojson omits a default-valued scalar, and an omitted key is a
    /// zero rather than a missing value: `completedJobs (absent => 0) <
    /// totalJobs (1)` is the incompleteness signal the search route
    /// carries (`modules/frontend/combiner/response_metrics.go:19-38`,
    /// `combiner/search.go:126-135` @ v3.0.2).
    #[test]
    fn the_metrics_block_is_the_reference_jobs_pair_on_both_partiality_branches() {
        let mut complete = sample_output();
        complete.partial = false;
        assert_eq!(
            render(&complete)["metrics"],
            serde_json::json!({"completedJobs": 1, "totalJobs": 1}),
            "a complete search: one job, one completed"
        );

        let mut truncated = sample_output();
        truncated.partial = true;
        assert_eq!(
            render(&truncated)["metrics"],
            serde_json::json!({"totalJobs": 1}),
            "a truncated search: completedJobs is zero, and protojson omits a zero"
        );
    }

    /// Issue #464 AC 2: no key outside `tempopb.SearchMetrics` survives on
    /// either branch. Grafana's Tempo datasource decodes the search
    /// response with unknown fields REJECTED (`pkg/tempo/search.go:95` @
    /// `v13.1.5-11-g3c7375b`), so one invented key costs the whole
    /// response, not one field of it.
    ///
    /// **Scope, stated exactly.** This detects an *invented key* and
    /// nothing else. It does not detect a wrong value, a wrong branch or
    /// a missing key — those belong to
    /// [`the_metrics_block_is_the_reference_jobs_pair_on_both_partiality_branches`],
    /// and every value/branch break leaves this test green.
    #[test]
    fn every_metrics_key_is_a_tempopb_search_metrics_field() {
        for partial in METRICS_PARTIALITY_BRANCHES {
            let mut output = sample_output();
            output.partial = partial;
            let rendered = render(&output);
            let metrics = rendered["metrics"].as_object().expect("metrics object");
            for key in metrics.keys() {
                assert!(
                    SEARCH_METRICS_FIELDS.contains(&key.as_str()),
                    "partial={partial}: {key:?} is not a tempopb.SearchMetrics field"
                );
            }
        }
    }

    /// Issue #464 AC 6b: the metrics shape in docs/api.md §4.2 is
    /// DERIVED from [`render`] over both partiality branches, never
    /// restated. That sentence is the public contract for
    /// `/api/traces/v1/search` and its `/api/search` alias, and the
    /// hermetic docs pin in `crates/pulsus-read/tests/traces_search_sql.rs`
    /// compares the document against a *constant*; this is the half that
    /// compares it against the code.
    ///
    /// **What it couples:** the union of the `metrics` key sets across
    /// [`METRICS_PARTIALITY_BRANCHES`], against the documentation. Both
    /// branches, because a key emitted only when `output.partial` slips
    /// past a single-call version of this test.
    ///
    /// **What it is not.** It is not "every branch of `render`" (see
    /// [`METRICS_PARTIALITY_BRANCHES`] for the five it does not reach),
    /// it does not reach the VALUES, and it does not reach the omission
    /// rule: a key present on one branch and absent on the other is
    /// indistinguishable here from a key present on both. Those are the
    /// subjects of the two tests above and of the empty-envelope test.
    #[test]
    fn the_documented_metrics_shape_is_derived_from_both_partiality_branches() {
        let mut keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for partial in METRICS_PARTIALITY_BRANCHES {
            let mut output = sample_output();
            output.partial = partial;
            let rendered = render(&output);
            let metrics = rendered["metrics"].as_object().expect("metrics object");
            keys.extend(metrics.keys().cloned());
        }
        let shape = format!(
            "{{{}}}",
            keys.iter()
                .map(|key| format!("\"{key}\":<n>"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let api = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/api.md"),
        )
        .expect("read docs/api.md");
        assert!(
            api.contains(&shape),
            "docs/api.md §4.2 must document the metrics-key union over both partiality \
             branches, {shape:?} — derived from render(), never restated"
        );
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
