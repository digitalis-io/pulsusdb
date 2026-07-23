//! Live end-to-end tests for `/api/traces/v1/trace/{traceId}[/json]`
//! (issue #55, AC5): spawns the real `pulsusdb` binary against a live
//! ClickHouse (same harness as `prom_api_live.rs`), seeds traces through
//! the *product* ingest path (`POST /v1/traces`, sync), then drives the
//! fetch surface over loopback HTTP — default/`Accept`-negotiated/forced
//! representations, the 406 mapping on a real successful trace, absent/
//! malformed ids, at-least-once dedup, the 16-hex short-id resolution,
//! and byte-identical JSON across permuted insert orders.
//!
//! Issue #61 (T9) adds the seeded byte-identity proof for the eight
//! pure-binding Tempo query aliases
//! (`tempo_query_aliases_are_byte_identical_to_native_on_seeded_data`):
//! alias vs native status + `Content-Type` + body bytes on real traces —
//! negotiated-JSON, protobuf, and forced-JSON trace bodies plus
//! non-empty search/metrics bodies. The spawns set
//! `PULSUS_COMPAT_ENDPOINTS=true` (aliases mounted; native behavior is
//! unaffected — build-time route merging only).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test traces_api_live
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Ports 31130 (fetch suite) and 31135 (alias suite) — distinct from
//! every other live suite's fixed ports (31100-31134).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use prost::Message;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

const PORT: u16 = 31_130;
/// The issue #61 alias byte-identity suite's own spawn (both tests in
/// this binary may run concurrently — distinct ports, distinct
/// throwaway databases).
const ALIAS_PORT: u16 = 31_135;
/// The issue #75 Zipkin shared-span round-trip suite's own spawn.
const ZIPKIN_PORT: u16 = 31_136;

// ---------------------------------------------------------------------
// Bare-`TcpStream` HTTP/1.1 helper (the `api_conformance.rs` idiom,
// trimmed to what this suite needs: arbitrary method/headers/raw body,
// dechunked byte-exact responses; no gzip is ever negotiated here).
// ---------------------------------------------------------------------

struct RawResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl RawResponse {
    fn content_type(&self) -> Option<&str> {
        self.headers.get("content-type").map(String::as_str)
    }

    fn json(&self, ctx: &str) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "{ctx}: invalid JSON body: {e}\nbody: {:?}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

/// Token-matches `accept` in the (comma-joined) `Vary` header — never a
/// substring check, since `accept-encoding` (the compression layer's own
/// `Vary` contribution) contains `accept` as a substring but is a distinct
/// token.
fn has_vary_accept(res: &RawResponse) -> bool {
    res.headers
        .get("vary")
        .map(|v| {
            v.split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("accept"))
        })
        .unwrap_or(false)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = find_subslice(raw, b"\r\n") else {
            break;
        };
        let size_str = String::from_utf8_lossy(&raw[..line_end]);
        let Ok(size) = usize::from_str_radix(size_str.trim(), 16) else {
            break;
        };
        if size == 0 {
            break;
        }
        let data_start = line_end + 2;
        let data_end = data_start + size;
        if data_end > raw.len() {
            break;
        }
        out.extend_from_slice(&raw[data_start..data_end]);
        raw = &raw[(data_end + 2).min(raw.len())..];
    }
    out
}

fn request(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<(&str, &[u8])>,
) -> Option<RawResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();

    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    let body_bytes = match body {
        Some((content_type, bytes)) => {
            head.push_str(&format!("Content-Type: {content_type}\r\n"));
            bytes
        }
        None => &[],
    };
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body_bytes.len()));

    stream.write_all(head.as_bytes()).ok()?;
    stream.write_all(body_bytes).ok()?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;

    let split_at = find_subslice(&buf, b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let raw_body = &buf[split_at + 4..];

    let mut lines = head.lines();
    let status = lines
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse::<u16>()
        .ok()?;
    // Comma-join duplicate field lines (RFC 9110 §5.3) rather than
    // last-wins: the response may legitimately carry two `Vary` lines
    // (the handler's `accept` plus the compression layer's
    // `accept-encoding`), and both must survive for a token-based match.
    let mut headers: HashMap<String, String> = HashMap::new();
    for line in lines {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        let key = k.trim().to_ascii_lowercase();
        let value = v.trim().to_string();
        headers
            .entry(key)
            .and_modify(|existing| {
                existing.push_str(", ");
                existing.push_str(&value);
            })
            .or_insert(value);
    }

    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };

    Some(RawResponse {
        status,
        headers,
        body,
    })
}

fn get(port: u16, path: &str, headers: &[(&str, &str)], ctx: &str) -> RawResponse {
    request(port, "GET", path, headers, None)
        .unwrap_or_else(|| panic!("{ctx}: request must be reachable (transport failure)"))
}

// ---------------------------------------------------------------------
// Process lifecycle + OTLP seeding through the product ingest path.
// ---------------------------------------------------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_ready(port: u16, db: &str) -> ChildGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env(
            "CLICKHOUSE_SERVER",
            std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        )
        .env(
            "CLICKHOUSE_HTTP_PORT",
            std::env::var("PULSUS_TEST_CH_HTTP_PORT").unwrap_or_else(|_| "19123".to_string()),
        )
        .env("CLICKHOUSE_DB", db)
        // Issue #61 (T9): mount the Tempo compat aliases — needed by the
        // alias byte-identity suite; a no-op for the native assertions
        // (router-build-time merging only, no per-request behavior).
        .env("PULSUS_COMPAT_ENDPOINTS", "true")
        .spawn()
        .expect("spawn pulsusdb");
    let guard = ChildGuard(child);

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if request(port, "GET", "/ready", &[], None).is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s");
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn span(trace_id: [u8; 16], span_id: [u8; 8], name: &str, start_ns: u64) -> Span {
    Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        name: name.to_string(),
        start_time_unix_nano: start_ns,
        end_time_unix_nano: start_ns + 1_000_000,
        ..Default::default()
    }
}

/// Seeds `spans` through `POST /v1/traces` (sync — no `X-Pulsus-Async`
/// header, so a `200` means the rows are flushed and read-visible), with
/// the fixed resource (`service.name=checkout`) and scope (`live-scope`)
/// context every fetch assertion below checks for.
fn ingest(port: u16, spans: Vec<Span>, ctx: &str) {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "checkout")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "live-scope".to_string(),
                    version: String::new(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let res = request(
        port,
        "POST",
        "/v1/traces",
        &[],
        Some(("application/x-protobuf", &req.encode_to_vec())),
    )
    .unwrap_or_else(|| panic!("{ctx}: ingest must be reachable"));
    assert_eq!(
        res.status,
        200,
        "{ctx}: sync ingest must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
}

// ---------------------------------------------------------------------
// Fetch-side assertion helpers.
// ---------------------------------------------------------------------

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn fetch_path(hex_id: &str) -> String {
    format!("/api/traces/v1/trace/{hex_id}")
}

fn spans_of(data: &TracesData) -> Vec<&Span> {
    data.resource_spans
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .flat_map(|ss| &ss.spans)
        .collect()
}

/// Every `ResourceSpans` must carry the seeded resource attr and scope
/// name (v2 test-gap closure: full OTLP resource/scope reconstruction,
/// per span, not just span ids).
fn assert_context_preserved(data: &TracesData, ctx: &str) {
    assert!(!data.resource_spans.is_empty(), "{ctx}: no resource spans");
    for rs in &data.resource_spans {
        let resource = rs.resource.as_ref().unwrap_or_else(|| {
            panic!("{ctx}: a ResourceSpans lost its resource");
        });
        assert!(
            resource.attributes.iter().any(|a| a.key == "service.name"
                && a.value
                    == Some(AnyValue {
                        value: Some(Value::StringValue("checkout".to_string()))
                    })),
            "{ctx}: service.name=checkout resource attr must survive per span"
        );
        for ss in &rs.scope_spans {
            let scope = ss
                .scope
                .as_ref()
                .unwrap_or_else(|| panic!("{ctx}: a ScopeSpans lost its scope"));
            assert_eq!(scope.name, "live-scope", "{ctx}: scope name per span");
        }
    }
}

fn assert_error_envelope(res: &RawResponse, status: u16, error_type: &str, ctx: &str) {
    assert_eq!(
        res.status,
        status,
        "{ctx}: status (body: {:?})",
        String::from_utf8_lossy(&res.body)
    );
    assert!(
        res.content_type()
            .is_some_and(|ct| ct.starts_with("application/json")),
        "{ctx}: errors must stay JSON, content-type {:?}",
        res.content_type()
    );
    let json = res.json(ctx);
    assert_eq!(json["status"], "error", "{ctx}");
    assert_eq!(json["errorType"], error_type, "{ctx}: body {json}");
}

// ---------------------------------------------------------------------
// The suite (one spawn, one throwaway database).
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn trace_fetch_serves_negotiated_representations_against_real_clickhouse() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-server/tests/traces_api_live.rs for setup)"
        );
        return;
    }

    let _guard = spawn_ready(PORT, "pulsus_traces_api_it_live");

    // -- Seed trace A: 3 spans, start times chosen so canonical output
    // order (startTimeUnixNano, spanId) differs from insert order.
    let trace_a = [0xaa; 16];
    let a_hex = hex(&trace_a);
    let s1 = span(trace_a, [1; 8], "span-one", 3_000_000_000_000_000_300);
    let s2 = span(trace_a, [2; 8], "span-two", 3_000_000_000_000_000_100);
    let s3 = span(trace_a, [3; 8], "span-three", 3_000_000_000_000_000_200);
    ingest(
        PORT,
        vec![s1.clone(), s2.clone(), s3.clone()],
        "seed trace A",
    );

    // -- Default representation: 200 application/json, protojson decodes,
    // spans in canonical order, context preserved.
    let ctx = "GET trace A (default)";
    let res = get(PORT, &fetch_path(&a_hex), &[], ctx);
    assert_eq!(res.status, 200, "{ctx}");
    assert_eq!(res.content_type(), Some("application/json"), "{ctx}");
    assert!(
        has_vary_accept(&res),
        "{ctx}: negotiating route must Vary: accept"
    );
    let default_json_body = res.body.clone();
    let decoded: TracesData = serde_json::from_slice(&res.body)
        .unwrap_or_else(|e| panic!("{ctx}: protojson must deserialize as TracesData: {e}"));
    let spans = spans_of(&decoded);
    assert_eq!(spans.len(), 3, "{ctx}: span count");
    assert_eq!(
        spans.iter().map(|s| s.span_id.clone()).collect::<Vec<_>>(),
        vec![vec![2u8; 8], vec![3u8; 8], vec![1u8; 8]],
        "{ctx}: canonical (startTimeUnixNano, spanId) order"
    );
    assert_context_preserved(&decoded, ctx);
    // Protojson shape spot-checks (hex ids, camelCase, u64-as-string).
    let json = res.json(ctx);
    let first = &json["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
    assert_eq!(first["traceId"], a_hex.as_str(), "{ctx}: hex traceId");
    assert_eq!(
        first["startTimeUnixNano"], "3000000000000000100",
        "{ctx}: u64-as-string"
    );

    // -- /json suffix: byte-identical to the default JSON.
    let ctx = "GET trace A /json";
    let res = get(PORT, &format!("{}/json", fetch_path(&a_hex)), &[], ctx);
    assert_eq!(res.status, 200, "{ctx}");
    assert_eq!(res.content_type(), Some("application/json"), "{ctx}");
    assert_eq!(res.body, default_json_body, "{ctx}: byte-identical JSON");
    assert!(
        !has_vary_accept(&res),
        "{ctx}: the forcing route never consults Accept, so no Vary: accept"
    );

    // -- /json with a protobuf Accept: still JSON (forcing ignores Accept).
    let ctx = "GET trace A /json with Accept: application/protobuf";
    let res = get(
        PORT,
        &format!("{}/json", fetch_path(&a_hex)),
        &[("accept", "application/protobuf")],
        ctx,
    );
    assert_eq!(res.status, 200, "{ctx}");
    assert_eq!(res.content_type(), Some("application/json"), "{ctx}");
    assert_eq!(res.body, default_json_body, "{ctx}: byte-identical JSON");
    assert!(!has_vary_accept(&res), "{ctx}: still no Vary: accept");

    // -- Accept: application/protobuf and the x-protobuf request alias:
    // 200 application/protobuf, prost-decodes to the same spans.
    for accept in ["application/protobuf", "application/x-protobuf"] {
        let ctx = format!("GET trace A with Accept: {accept}");
        let res = get(PORT, &fetch_path(&a_hex), &[("accept", accept)], &ctx);
        assert_eq!(res.status, 200, "{ctx}");
        assert_eq!(
            res.content_type(),
            Some("application/protobuf"),
            "{ctx}: response content-type is always application/protobuf, never x-protobuf"
        );
        assert!(
            has_vary_accept(&res),
            "{ctx}: negotiating route must Vary: accept"
        );
        let decoded = TracesData::decode(res.body.as_slice())
            .unwrap_or_else(|e| panic!("{ctx}: body must prost-decode as TracesData: {e}"));
        let spans = spans_of(&decoded);
        assert_eq!(spans.len(), 3, "{ctx}: span count");
        assert_eq!(
            spans.iter().map(|s| s.span_id.clone()).collect::<Vec<_>>(),
            vec![vec![2u8; 8], vec![3u8; 8], vec![1u8; 8]],
            "{ctx}: same canonical order as JSON"
        );
        assert_context_preserved(&decoded, &ctx);
    }

    // -- 406 over HTTP on the *seeded* trace (plan v3 §3: the mapping is
    // exercised on the success path, not only error paths).
    let ctx = "GET trace A with Accept: text/plain";
    let res = get(PORT, &fetch_path(&a_hex), &[("accept", "text/plain")], ctx);
    assert_error_envelope(&res, 406, "not_acceptable", ctx);
    assert!(has_vary_accept(&res), "{ctx}: 406 must Vary: accept");

    // -- Absent + malformed ids.
    let ctx = "GET absent trace";
    let res = get(PORT, &fetch_path(&"ee".repeat(16)), &[], ctx);
    assert_error_envelope(&res, 404, "not_found", ctx);
    assert!(has_vary_accept(&res), "{ctx}: 404 must Vary: accept");

    let ctx = "GET malformed trace id";
    let res = get(PORT, &fetch_path("zzzz"), &[], ctx);
    assert_error_envelope(&res, 400, "bad_data", ctx);
    assert!(has_vary_accept(&res), "{ctx}: 400 must Vary: accept");

    // -- Dedup: ingest the same span twice, fetch returns it once.
    let trace_b = [0xbb; 16];
    let b_hex = hex(&trace_b);
    let dup = span(trace_b, [9; 8], "span-dup", 3_000_000_000_000_001_000);
    ingest(PORT, vec![dup.clone()], "seed trace B (first copy)");
    ingest(PORT, vec![dup], "seed trace B (replay)");
    let ctx = "GET trace B after duplicate ingest";
    let res = get(PORT, &fetch_path(&b_hex), &[], ctx);
    assert_eq!(res.status, 200, "{ctx}");
    let decoded: TracesData = serde_json::from_slice(&res.body)
        .unwrap_or_else(|e| panic!("{ctx}: protojson must deserialize: {e}"));
    assert_eq!(
        spans_of(&decoded).len(),
        1,
        "{ctx}: at-least-once replays dedup to one span"
    );

    // -- 16-hex short id: resolves a stored trace whose high 8 bytes are
    // zero (left-padding contract).
    let mut trace_c = [0u8; 16];
    trace_c[8..].copy_from_slice(&[0xcc; 8]);
    ingest(
        PORT,
        vec![span(
            trace_c,
            [7; 8],
            "span-short",
            3_000_000_000_000_002_000,
        )],
        "seed trace C",
    );
    let ctx = "GET trace C by 16-hex short id";
    let res = get(PORT, &fetch_path(&"cc".repeat(8)), &[], ctx);
    assert_eq!(res.status, 200, "{ctx}: short id must resolve");
    let decoded: TracesData = serde_json::from_slice(&res.body)
        .unwrap_or_else(|e| panic!("{ctx}: protojson must deserialize: {e}"));
    let spans = spans_of(&decoded);
    assert_eq!(spans.len(), 1, "{ctx}");
    assert_eq!(spans[0].span_id, vec![7u8; 8], "{ctx}");

    // -- Permuted insert orders produce byte-identical JSON (plan v3 §2):
    // two traces, identical except for their ids, ingested span-by-span in
    // different orders; after substituting the trace-id hex, the JSON
    // renderings must be byte-identical (canonical output ordering).
    let trace_d = [0xd1; 16];
    let trace_e = [0xd2; 16];
    let starts = [
        3_000_000_000_000_003_300u64,
        3_000_000_000_000_003_100,
        3_000_000_000_000_003_200,
    ];
    let ids: [[u8; 8]; 3] = [[0x11; 8], [0x12; 8], [0x13; 8]];
    // Trace D: insert order s1, s2, s3 (separate POSTs — real distinct
    // inserts, not one batch).
    for i in [0usize, 1, 2] {
        ingest(
            PORT,
            vec![span(trace_d, ids[i], &format!("perm-{i}"), starts[i])],
            "seed trace D",
        );
    }
    // Trace E: same spans, reversed insert order.
    for i in [2usize, 1, 0] {
        ingest(
            PORT,
            vec![span(trace_e, ids[i], &format!("perm-{i}"), starts[i])],
            "seed trace E",
        );
    }
    let ctx = "GET traces D/E (insert-order permutation)";
    let d = get(PORT, &fetch_path(&hex(&trace_d)), &[], ctx);
    let e = get(PORT, &fetch_path(&hex(&trace_e)), &[], ctx);
    assert_eq!(d.status, 200, "{ctx}: D");
    assert_eq!(e.status, 200, "{ctx}: E");
    let e_body = String::from_utf8(e.body).expect("JSON is UTF-8");
    let e_as_d = e_body.replace(&hex(&trace_e), &hex(&trace_d));
    assert_eq!(
        String::from_utf8(d.body).expect("JSON is UTF-8"),
        e_as_d,
        "{ctx}: byte-identical JSON across permuted insert orders (modulo the trace id)"
    );
}

// ---------------------------------------------------------------------
// Issue #61 (T9) AC2: the eight pure-binding Tempo aliases are
// byte-identical to their native twins on SEEDED data.
// ---------------------------------------------------------------------

/// One alias/native pair under identical request headers: status,
/// `Content-Type`, and body BYTES must be equal. Returns the alias
/// response so the caller can additionally prove the shared body is a
/// non-trivial success (the empty-DB conformance matrix can only ever
/// compare 404/empty envelopes — this is the seeded proof).
fn assert_alias_native_identity(
    port: u16,
    alias: &str,
    native: &str,
    headers: &[(&str, &str)],
    ctx: &str,
) -> RawResponse {
    let a = get(port, alias, headers, &format!("{ctx}: alias {alias}"));
    let n = get(port, native, headers, &format!("{ctx}: native {native}"));
    assert_eq!(
        a.status, n.status,
        "{ctx}: alias {alias} status must equal native {native}"
    );
    assert_eq!(
        a.content_type(),
        n.content_type(),
        "{ctx}: alias {alias} Content-Type must equal native {native}"
    );
    assert_eq!(
        a.body, n.body,
        "{ctx}: alias {alias} body bytes must be identical to native {native}"
    );
    a
}

#[tokio::test(flavor = "multi_thread")]
async fn tempo_query_aliases_are_byte_identical_to_native_on_seeded_data() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-server/tests/traces_api_live.rs for setup)"
        );
        return;
    }

    let port = ALIAS_PORT;
    let _guard = spawn_ready(port, "pulsus_traces_compat_it_live");

    // Seed one trace: 2 spans, start times chosen so canonical output
    // order differs from insert order (a real, non-trivial body). Window
    // math below is in unix SECONDS (magnitude < 10^12).
    let trace_f = [0xf5; 16];
    let f_hex = hex(&trace_f);
    let s1 = span(trace_f, [1; 8], "alias-one", 3_100_000_000_000_000_200);
    let s2 = span(trace_f, [2; 8], "alias-two", 3_100_000_000_000_000_100);
    ingest(port, vec![s1, s2], "seed trace F");

    let native_fetch = fetch_path(&f_hex);

    // -- Routes 1 + 3 (trace-by-ID, negotiating handler): default Accept
    // (negotiated JSON) and Accept: application/protobuf, both on a real
    // 200 trace body — not the 404 envelope.
    let mut default_json_body = Vec::new();
    for alias_prefix in ["/api/traces", "/tempo/api/traces"] {
        let alias_fetch = format!("{alias_prefix}/{f_hex}");

        let ctx = format!("{alias_fetch} default (negotiated JSON)");
        let res = assert_alias_native_identity(port, &alias_fetch, &native_fetch, &[], &ctx);
        assert_eq!(res.status, 200, "{ctx}");
        assert_eq!(res.content_type(), Some("application/json"), "{ctx}");
        assert!(
            has_vary_accept(&res),
            "{ctx}: negotiating alias must Vary: accept"
        );
        let decoded: TracesData = serde_json::from_slice(&res.body)
            .unwrap_or_else(|e| panic!("{ctx}: protojson must deserialize: {e}"));
        assert_eq!(spans_of(&decoded).len(), 2, "{ctx}: non-empty seeded body");
        default_json_body = res.body;

        let ctx = format!("{alias_fetch} Accept: application/protobuf");
        let res = assert_alias_native_identity(
            port,
            &alias_fetch,
            &native_fetch,
            &[("accept", "application/protobuf")],
            &ctx,
        );
        assert_eq!(res.status, 200, "{ctx}");
        assert_eq!(res.content_type(), Some("application/protobuf"), "{ctx}");
        assert!(
            has_vary_accept(&res),
            "{ctx}: negotiating alias must Vary: accept"
        );
        let decoded = TracesData::decode(res.body.as_slice())
            .unwrap_or_else(|e| panic!("{ctx}: body must prost-decode as TracesData: {e}"));
        assert_eq!(spans_of(&decoded).len(), 2, "{ctx}: non-empty seeded body");
    }

    // -- Route 2 (trace-by-ID /json, forcing handler): sent WITH a
    // protobuf Accept — a miswired alias (bound to the negotiating
    // handler) would return protobuf; the forcing handler ignores Accept
    // and returns the exact default protojson bytes.
    let ctx = "/api/traces/{traceId}/json with Accept: application/protobuf (forcing proof)";
    let res = assert_alias_native_identity(
        port,
        &format!("/api/traces/{f_hex}/json"),
        &format!("{native_fetch}/json"),
        &[("accept", "application/protobuf")],
        ctx,
    );
    assert_eq!(res.status, 200, "{ctx}");
    assert_eq!(
        res.content_type(),
        Some("application/json"),
        "{ctx}: the /json alias must bind the forcing handler, not the negotiating one"
    );
    assert_eq!(
        res.body, default_json_body,
        "{ctx}: forced JSON is the same protojson bytes as the default representation"
    );

    // -- Route 4 (search): a seeded match-all window returning a
    // non-empty `traces` array.
    let window = "start=3099999000&end=3100001000";
    let search_query = format!("?q=%7B%7D&{window}");
    let ctx = "/api/search seeded match-all";
    let res = assert_alias_native_identity(
        port,
        &format!("/api/search{search_query}"),
        &format!("/api/traces/v1/search{search_query}"),
        &[],
        ctx,
    );
    assert_eq!(res.status, 200, "{ctx}");
    let json = res.json(ctx);
    assert_eq!(
        json["traces"].as_array().map(Vec::len),
        Some(1),
        "{ctx}: the seeded trace must be returned, body {json}"
    );

    // -- Routes 7-10 (TraceQL metrics, both prefixes): seeded range +
    // instant windows returning the **Tempo-native** `{series, metrics}`
    // body (issue #182 replaced the Prometheus matrix/vector envelope on
    // the traces metrics endpoints — docs/api.md §4.4; these endpoints are
    // Tempo-datasource-only). The alias-vs-native byte-identity is checked
    // by `assert_alias_native_identity` (shape-agnostic); here we assert
    // the new shape on both sides.
    let metrics_query = format!("?q=%7B%7D%20%7C%20rate()&{window}&step=60");
    for alias_prefix in ["/api/metrics", "/tempo/api/metrics"] {
        let ctx = format!("{alias_prefix}/query_range seeded");
        let res = assert_alias_native_identity(
            port,
            &format!("{alias_prefix}/query_range{metrics_query}"),
            &format!("/api/traces/v1/metrics/query_range{metrics_query}"),
            &[],
            &ctx,
        );
        assert_eq!(res.status, 200, "{ctx}");
        let json = res.json(&ctx);
        assert!(
            json["series"].as_array().is_some_and(|s| !s.is_empty()),
            "{ctx}: the seeded window must produce a non-empty Tempo-native series set, body {json}"
        );

        let ctx = format!("{alias_prefix}/query seeded");
        let res = assert_alias_native_identity(
            port,
            &format!("{alias_prefix}/query{metrics_query}"),
            &format!("/api/traces/v1/metrics/query{metrics_query}"),
            &[],
            &ctx,
        );
        assert_eq!(res.status, 200, "{ctx}");
        let json = res.json(&ctx);
        // Guard non-emptiness BEFORE indexing (issue #61 pattern): the
        // instant form carries one `samples[]` entry per series.
        assert!(
            json["series"].as_array().is_some_and(|s| !s.is_empty()),
            "{ctx}: the seeded window must produce a non-empty instant series, body {json}"
        );
        // The Tempo-native sample `value` is a JSON number (omitted at
        // zero); the seeded window guarantees a strictly positive rate.
        let value = json["series"][0]["samples"][0]["value"]
            .as_f64()
            .unwrap_or_else(|| {
                panic!("{ctx}: the instant sample value must be a number, body {json}")
            });
        assert!(
            value.is_finite() && value > 0.0,
            "{ctx}: the seeded window must count real spans (finite positive rate), got \
             {value}, body {json}"
        );
    }
}

/// Issue #75 (the adjudicated Q1 shared-span correctness gate): a Zipkin v2
/// JSON shared RPC span — the SAME `(traceId, id)` reported from both ends
/// with different `kind` (CLIENT vs SERVER) — round-trips end-to-end
/// through the real product path: `POST /api/v2/spans` (the Zipkin compat
/// receiver) -> adapt to OTLP -> `TraceWriter` -> ClickHouse -> trace-by-ID
/// assembly. The gate is that **both** sides come back: `GET
/// /api/traces/v1/trace/{id}` returns two spans (SERVER + CLIENT), proving
/// the `(span_id, kind)` de-dup key keeps them distinct, and TraceQL search
/// finds the trace.
#[tokio::test]
async fn zipkin_shared_span_trace_by_id_returns_both_the_server_and_client_sides() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-server/tests/traces_api_live.rs for setup)"
        );
        return;
    }

    let _guard = spawn_ready(ZIPKIN_PORT, "pulsus_traces_api_it_zipkin_shared");

    // Recent timestamp so the 7-day delete-TTL never drops the part; micros
    // on the wire, seconds for the search window.
    let now_secs = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs(),
    )
    .expect("fits i64");
    let ts_micros = now_secs * 1_000_000;

    // A 128-bit trace id (verbatim) + one span id, reported from both RPC
    // ends: CLIENT (frontend) and shared SERVER (backend).
    let trace_hex = "0102030405060708090a0b0c0d0e0f10";
    let body = format!(
        r#"[
          {{"traceId":"{trace_hex}","id":"00000000000000aa",
            "name":"rpc","kind":"CLIENT","timestamp":{ts_micros},"duration":2000,
            "localEndpoint":{{"serviceName":"frontend"}}}},
          {{"traceId":"{trace_hex}","id":"00000000000000aa",
            "name":"rpc","kind":"SERVER","timestamp":{ts_micros},"duration":1800,"shared":true,
            "localEndpoint":{{"serviceName":"backend"}}}}
        ]"#
    );

    let ctx = "POST /api/v2/spans (Zipkin shared pair)";
    let res = request(
        ZIPKIN_PORT,
        "POST",
        "/api/v2/spans",
        &[],
        Some(("application/json", body.as_bytes())),
    )
    .unwrap_or_else(|| panic!("{ctx}: must be reachable"));
    assert_eq!(
        res.status,
        202,
        "{ctx}: Zipkin success is 202 Accepted, body {:?}",
        String::from_utf8_lossy(&res.body)
    );

    // Trace-by-ID returns BOTH sides — the correctness gate.
    let ctx = "GET trace-by-ID (shared span)";
    let res = get(ZIPKIN_PORT, &fetch_path(trace_hex), &[], ctx);
    assert_eq!(
        res.status,
        200,
        "{ctx}: body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    let decoded: TracesData = serde_json::from_slice(&res.body)
        .unwrap_or_else(|e| panic!("{ctx}: protojson must deserialize as TracesData: {e}"));
    let spans = spans_of(&decoded);
    assert_eq!(
        spans.len(),
        2,
        "{ctx}: both the SERVER and CLIENT sides of the shared span must be returned"
    );
    // Same span id on both, distinct kind (SERVER=2, CLIENT=3).
    assert_eq!(
        spans[0].span_id, spans[1].span_id,
        "{ctx}: the two sides share one span_id"
    );
    let mut kinds: Vec<i32> = spans.iter().map(|s| s.kind).collect();
    kinds.sort_unstable();
    assert_eq!(
        kinds,
        vec![SpanKind::Server as i32, SpanKind::Client as i32],
        "{ctx}: the two sides are SERVER and CLIENT"
    );

    // TraceQL search sees the trace too.
    let window = format!("start={}&end={}", now_secs - 10, now_secs + 10);
    let ctx = "TraceQL search (shared span)";
    let res = get(
        ZIPKIN_PORT,
        &format!("/api/traces/v1/search?q=%7B%7D&{window}"),
        &[],
        ctx,
    );
    assert_eq!(
        res.status,
        200,
        "{ctx}: body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    let json = res.json(ctx);
    assert!(
        json["traces"]
            .as_array()
            .is_some_and(|t| t.iter().any(|tr| tr["traceID"] == trace_hex)),
        "{ctx}: the shared-span trace must appear in search results, body {json}"
    );
}
