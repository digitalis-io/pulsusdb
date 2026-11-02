// Issue #764 — corpus N, the four-span corpus of `!= nil` on an intrinsic,
// shared by `traces_query_v2_live.rs` and
// `traces_metrics_filter_differential.rs`. Both decode these bytes; the
// reference receives the same bytes.
//
// Not a top-level `tests/*.rs` file on purpose: Cargo would build it as its
// own integration-test target, which would select no tests and exit green.
//
// | span | trace | parent | name     | status          | kind | scope   | events     | links  |
// |------|-------|--------|----------|-----------------|------|---------|------------|--------|
// | a0   | A     | —      | ""       | —               | 0    | `{}`    | —          | —      |
// | a1   | A     | a0     | `op`     | error, `boom`   | 2    | `s`/`1` | `ev`       | to b1  |
// | a2   | A     | a0     | `op2`    | ok              | 3    | `s`/`1` | one, `""`  | —      |
// | b1   | B     | ff     | `orphan` | —               | 0    | `s`/—   | —          | —      |
//
// b1's parent `00000000000000ff` is absent, so trace B has no root.

use serde_json::{Value, json};

/// Corpus N of issue #764 as one OTLP/JSON body. Offsets from `base_ns`:
/// a0 0, a1 100 ms, a2 200 ms, b1 300 ms; each span 1 ms; each event at
/// its span's start + 10 ns. Each span is its own `resourceSpans` entry
/// with resource `service.name = <service>`.
pub fn body(base_ns: i64, trace_a: &str, trace_b: &str, service: &str) -> Vec<u8> {
    const MS: i64 = 1_000_000;
    let ns = |off: i64| (base_ns + off).to_string();
    let span = |trace: &str, id: &str, start: i64, extra: Value| {
        let mut s = json!({
            "traceId": trace,
            "spanId": id,
            "startTimeUnixNano": ns(start),
            "endTimeUnixNano": ns(start + MS),
        });
        let (Value::Object(base), Value::Object(more)) = (&mut s, extra) else {
            unreachable!("both are objects");
        };
        base.extend(more);
        s
    };
    let event = |start: i64, name: &str| json!([{"name": name, "timeUnixNano": ns(start + 10)}]);
    let a0 = span(
        trace_a,
        "00000000000000a0",
        0,
        json!({"name": "", "kind": 0}),
    );
    let a1 = span(
        trace_a,
        "00000000000000a1",
        100 * MS,
        json!({
            "parentSpanId": "00000000000000a0",
            "name": "op",
            "kind": 2,
            "status": {"code": 2, "message": "boom"},
            "events": event(100 * MS, "ev"),
            "links": [{"traceId": trace_b, "spanId": "00000000000000b1"}],
        }),
    );
    let a2 = span(
        trace_a,
        "00000000000000a2",
        200 * MS,
        json!({
            "parentSpanId": "00000000000000a0",
            "name": "op2",
            "kind": 3,
            "status": {"code": 1},
            "events": event(200 * MS, ""),
        }),
    );
    let b1 = span(
        trace_b,
        "00000000000000b1",
        300 * MS,
        json!({"parentSpanId": "00000000000000ff", "name": "orphan", "kind": 0}),
    );
    let resource = json!({"attributes": [
        {"key": "service.name", "value": {"stringValue": service}},
    ]});
    let entry = |scope: Value, span: Value| {
        json!({
            "resource": resource,
            "scopeSpans": [{"scope": scope, "spans": [span]}],
        })
    };
    serde_json::to_vec(&json!({
        "resourceSpans": [
            entry(json!({}), a0),
            entry(json!({"name": "s", "version": "1"}), a1),
            entry(json!({"name": "s", "version": "1"}), a2),
            entry(json!({"name": "s"}), b1),
        ]
    }))
    .expect("serialise corpus N")
}
