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
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test traces_api_live
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Fixed loopback ports are declared inline per test (`let port = 31_1NN;`).
//! That no two live tests anywhere declare the same one is not a
//! convention re-derived by hand any more — `live_port_uniqueness.rs`
//! (hermetic) enumerates every declaration under `crates/*/tests` and
//! fails if two collide.

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

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

const PORT: u16 = 31_190;
/// The issue #61 alias byte-identity suite's own spawn (both tests in
/// this binary may run concurrently — distinct ports, distinct
/// throwaway databases).
const ALIAS_PORT: u16 = 31_191;
/// The issue #75 Zipkin shared-span round-trip suite's own spawn.
const ZIPKIN_PORT: u16 = 31_192;
/// The issue #237 ns→seconds wire-byte suite's own spawn.
const ULP_PORT: u16 = 31_193;
/// The issue #458 span-duration wire-field suite's own spawn.
const SPAN_DURATION_PORT: u16 = 31_208;
/// The issue #464 trace-envelope wire suite's own spawn.
const TRACE_ENVELOPE_PORT: u16 = 31_209;
/// The issue #473 search wire-domain suite's own spawn.
const WIRE_DOMAIN_PORT: u16 = 31_211;

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

mod wire_literal {
    /// A reference-captured wire rendering (issue #237), bytes flavour.
    /// Nothing textual or numeric leaves this module: every exit is a
    /// predicate or the opaque `WireProbe`. There is no `value()`, no
    /// `tokens()`, and no raw-text accessor, conversion, deref or
    /// formatting impl of any spelling, so a bare
    /// `find_subslice(&body, lit)` does not COMPILE — the captured text
    /// has no accidental landing site. This whole
    /// block is byte-frozen by the `metrics_response.rs` scanner (Rule
    /// C, upward-extended span); it invokes no macro and does not depend
    /// on `find_subslice` (its search is its own, inside the frozen
    /// span).
    pub(crate) struct WireLiteral(&'static str);

    /// Raw HTTP body bytes handed to the single search path. Built only
    /// from a caller body or `WireLiteral::surrounded_by`; no reader.
    pub(crate) struct WireProbe(Vec<u8>);

    impl WireProbe {
        pub(crate) fn body(bytes: &[u8]) -> Self {
            Self(bytes.to_vec())
        }
    }

    impl WireLiteral {
        pub(crate) const fn new(text: &'static str) -> Self {
            Self(text)
        }

        /// True iff the captured text is EXACTLY what the locked encoder
        /// emits for `want`: it parses bit-identically to `want` AND
        /// `serde_json::to_string(&want)` reproduces it.
        pub(crate) fn denotes(&self, want: f64) -> bool {
            let parses = match self.0.parse::<f64>() {
                Ok(v) => v.to_bits() == want.to_bits(),
                Err(_) => false,
            };
            let renders = match serde_json::to_string(&want) {
                Ok(s) => s == self.0,
                Err(_) => false,
            };
            parses && renders
        }

        /// The rendering as the `}`-closed JSON value token (a
        /// `Sample.value` is last in its object).
        fn closed(&self) -> Vec<u8> {
            let mut t = Vec::new();
            t.extend_from_slice(b"\"value\":");
            t.extend_from_slice(self.0.as_bytes());
            t.push(b'}');
            t
        }

        /// The rendering as the `,`-separated JSON value token (an
        /// `Exemplar.value` precedes `timestampMs`).
        fn separated(&self) -> Vec<u8> {
            let mut t = Vec::new();
            t.extend_from_slice(b"\"value\":");
            t.extend_from_slice(self.0.as_bytes());
            t.push(b',');
            t
        }

        fn appears(token: &[u8], body: &[u8]) -> bool {
            if token.is_empty() || body.len() < token.len() {
                return false;
            }
            body.windows(token.len()).any(|w| w == token)
        }

        /// The ONLY search: true iff the rendering appears as a
        /// DELIMITED JSON value token. Never bare — one captured
        /// rendering is a prefix of another (see the #237 table), so a
        /// bare byte check is wrong in BOTH directions.
        pub(crate) fn occurs_in(&self, probe: &WireProbe) -> bool {
            Self::appears(&self.closed(), &probe.0) || Self::appears(&self.separated(), &probe.0)
        }

        /// This rendering wrapped in caller-chosen text, as a probe —
        /// the delimiter-sensitivity control runs through the same
        /// `occurs_in` the body assertions use.
        pub(crate) fn surrounded_by(&self, left: &str, right: &str) -> WireProbe {
            let mut t = Vec::new();
            t.extend_from_slice(left.as_bytes());
            t.extend_from_slice(self.0.as_bytes());
            t.extend_from_slice(right.as_bytes());
            WireProbe(t)
        }
    }
}

use self::wire_literal::{WireLiteral, WireProbe};

/// Reference-captured ns→seconds renderings (issue #237).
/// `grafana/tempo:3.0.2@sha256:cda87c21…`, probed 2026-07-26.
/// `(ns, seconds value, captured rendering, two-rounding rendering)`.
/// One copy per site by design — the `metrics_response.rs` scanner's
/// Rule A cross-checks this copy against its own, cell for cell.
const REFERENCE_DURATION_SECONDS: &[(i64, f64, WireLiteral, WireLiteral)] = &[
    // ≤16-digit group: 1 ULP apart; pinned by the reference's own
    // comparison operator (`>= L` matches, `> L` does not).
    (
        1_118_000_000,
        1.118,
        WireLiteral::new("1.118"),
        WireLiteral::new("1.1179999999999999"),
    ),
    (
        1_122_000_000,
        1.122,
        WireLiteral::new("1.122"),
        WireLiteral::new("1.1219999999999999"),
    ),
    (
        1_128_000_000,
        1.128,
        WireLiteral::new("1.128"),
        WireLiteral::new("1.1280000000000001"),
    ),
    (
        1_235_000_000,
        1.235,
        WireLiteral::new("1.235"),
        WireLiteral::new("1.2349999999999999"),
    ),
    (
        31_952_000_000,
        31.952,
        WireLiteral::new("31.952"),
        WireLiteral::new("31.951999999999998"),
    ),
    (
        1_000_064_438,
        1.000064438,
        WireLiteral::new("1.000064438"),
        WireLiteral::new("1.0000644379999999"),
    ),
    // 17-significant-digit group: the formatter-independent RAW-WIRE
    // discriminators (#237 round 3). `ns > 2^53`, so the `int64->f64`
    // cast is lossy and the two-rounding value is the correctly rounded
    // one — the reference emitting the single-rounding value positively
    // identifies a cast-first form.
    (
        18_014_398_509_482_025,
        18_014_398.509_482_022,
        WireLiteral::new("18014398.509482022"),
        WireLiteral::new("18014398.509482026"),
    ),
    (
        18_014_398_509_482_035,
        18_014_398.509_482_037,
        WireLiteral::new("18014398.509482037"),
        WireLiteral::new("18014398.509482034"),
    ),
    (
        18_014_398_509_482_017,
        18_014_398.509_482_015,
        WireLiteral::new("18014398.509482015"),
        WireLiteral::new("18014398.50948202"),
    ),
    (
        1_088_608_058_291_172_412,
        1_088_608_058.291_172_3,
        WireLiteral::new("1088608058.2911723"),
        WireLiteral::new("1088608058.2911725"),
    ),
    (
        10_000_000_000_000_005,
        10_000_000.000_000_004,
        WireLiteral::new("10000000.000000004"),
        WireLiteral::new("10000000.000000006"),
    ),
    (
        10_000_000_000_000_015,
        10_000_000.000_000_017,
        WireLiteral::new("10000000.000000017"),
        WireLiteral::new("10000000.000000015"),
    ),
];

/// Exactly representable under both rounding forms — gross-scaling
/// controls, asserted at bit level only (their wire text is #263's
/// integral-double question, deliberately not asserted).
const REFERENCE_DURATION_CONTROLS: &[(i64, f64)] = &[
    (500_000_000, 0.5),
    (1_500_000_000, 1.5),
    (2_000_000_000, 2.0),
];

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

/// Seeds `spans` through `POST /v1/traces` with a resource carrying **no
/// attributes at all** — so no `service.name` reaches the store and the
/// trace's `rootServiceName` is the empty string (issue #473). Everything
/// else matches [`ingest`]; the scope context is identical, so the only
/// difference between a trace seeded here and one seeded there is the
/// field under test.
fn ingest_rootless(port: u16, spans: Vec<Span>, ctx: &str) {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![],
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

/// Drops `db` before the server is spawned, so a suite that asserts whole
/// response bodies starts from an empty store.
///
/// `trace_spans` is a plain `MergeTree` with no de-duplication, and this
/// file's throwaway databases are keyed on the suite name rather than the
/// run — so a second run against the same ClickHouse would otherwise see
/// every seeded span twice, and `matched` and the `spans` array of a
/// byte-exact expected body would both move. The other tests in this file
/// select by trace id instead and do not need this.
async fn drop_database(db: &str) {
    let config = pulsus_clickhouse::ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: "default".to_string(),
        proto: pulsus_clickhouse::ChProto::Http,
        pool_size: 1,
        query_timeout: Duration::from_secs(30),
        ..pulsus_clickhouse::ChConnConfig::default()
    };
    let client = pulsus_clickhouse::ChClient::new(config)
        .await
        .expect("connect to ClickHouse to drop the throwaway database");
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &pulsus_clickhouse::QuerySettings::new(),
            pulsus_clickhouse::Idempotency::Idempotent,
        )
        .await
        .expect("drop the throwaway database");
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

/// Issue #384: every §4 error is Tempo's frontend container over the real
/// wire — `text/plain; charset=utf-8`, **no** `X-Content-Type-Options`, no
/// trailing newline, and not JSON — carrying the message and nothing else.
/// Returns the body so the caller can assert the message with a LITERAL
/// needle (the #237 Rule D shape: a derived needle in a substring search
/// over a wire body is exactly what that guard forbids).
///
/// `nosniff`'s absence is the property that separates this container from
/// `logs_api`'s (`pkg/util/server/error.go:49 @ loki v3.7.4` sets it;
/// Tempo's frontend sets no headers at all,
/// `modules/frontend/handler.go:113-116 @ tempo v3.0.2`). Reusing the
/// LogQL responder would pass every other assertion here.
fn assert_error_body(res: &RawResponse, status: u16, ctx: &str) -> String {
    let body = String::from_utf8_lossy(&res.body).into_owned();
    assert_eq!(res.status, status, "{ctx}: status (body: {body:?})");
    assert_eq!(
        res.content_type(),
        Some("text/plain; charset=utf-8"),
        "{ctx}: error content type"
    );
    assert_eq!(
        res.headers.get("x-content-type-options"),
        None,
        "{ctx}: Tempo's frontend emits no nosniff"
    );
    assert!(
        !res.body.ends_with(b"\n"),
        "{ctx}: no trailing newline, got {body:?}"
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&res.body).is_err(),
        "{ctx}: the body must not parse as JSON, got {body:?}"
    );
    body
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

    let _guard = spawn_ready(PORT, &pulsus_testkit::test_db("pulsus_traces_api_it_live"));

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
    let body = assert_error_body(&res, 406, ctx);
    assert!(
        body.contains("no acceptable representation"),
        "{ctx}: {body:?}"
    );
    assert!(has_vary_accept(&res), "{ctx}: 406 must Vary: accept");

    // -- Absent + malformed ids.
    let ctx = "GET absent trace";
    let res = get(PORT, &fetch_path(&"ee".repeat(16)), &[], ctx);
    let body = assert_error_body(&res, 404, ctx);
    assert!(body.contains("trace not found"), "{ctx}: {body:?}");
    assert!(has_vary_accept(&res), "{ctx}: 404 must Vary: accept");

    let ctx = "GET malformed trace id";
    let res = get(PORT, &fetch_path("zzzz"), &[], ctx);
    let body = assert_error_body(&res, 400, ctx);
    assert!(
        body.contains("expected 16 or 32 hex characters"),
        "{ctx}: {body:?}"
    );
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
    let _guard = spawn_ready(
        port,
        &pulsus_testkit::test_db("pulsus_traces_compat_it_live"),
    );

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
        // instant form carries a SCALAR `value` per series and no
        // `samples` array at all (issue #464 wave 2 —
        // `tempopb.InstantSeries`, `pkg/tempopb/tempo.proto:346-355` @
        // v3.0.2).
        assert!(
            json["series"].as_array().is_some_and(|s| !s.is_empty()),
            "{ctx}: the seeded window must produce a non-empty instant series, body {json}"
        );
        assert!(
            json["series"][0].get("samples").is_none(),
            "{ctx}: an instant series carries no samples array — the retired range shape is              rejected outright by the datasource's strict decoder, body {json}"
        );
        // The Tempo-native `value` is a JSON number (omitted at zero); the
        // seeded window guarantees a strictly positive rate.
        let value = json["series"][0]["value"].as_f64().unwrap_or_else(|| {
            panic!("{ctx}: the instant series value must be a number, body {json}")
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

    let _guard = spawn_ready(
        ZIPKIN_PORT,
        &pulsus_testkit::test_db("pulsus_traces_api_it_zipkin_shared"),
    );

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

/// Extracts the instant series' scalar numeric token
/// (`…"labels":[…],"value":<tok>`) from a raw metrics body and parses it
/// with the std (correctly rounded) parser. Deliberately NOT
/// `res.json()[…].as_f64()`: serde_json's default float parse is
/// best-effort (the `float_roundtrip` feature is off workspace-wide) and
/// mis-decodes some 17-significant-digit tokens by 1 ULP — the decoded
/// assertion would then fire on a byte-correct body (issue #237; observed
/// live on the 1_088_608_058_291_172_412 ns width).
///
/// Issue #464 wave 2 moved the anchor from `"samples":[` to the
/// `],"value":` that closes the series' own `labels` array — the instant
/// body has no `samples` key. The anchor is unambiguous: a LABEL's value
/// is `"value":{`, always preceded by a quote or a comma and never by
/// `]`, and the only other `]` followed by a key is the top-level
/// `],"metrics":`.
fn decoded_instant_sample_bits(body: &[u8], ctx: &str) -> u64 {
    let v = find_subslice(body, b"],\"value\":")
        .unwrap_or_else(|| panic!("{ctx}: the instant series carries a scalar value"));
    let rest = &body[v..];
    let tail = &rest[10..];
    let end = tail
        .iter()
        .position(|b| *b == b'}' || *b == b',')
        .unwrap_or_else(|| panic!("{ctx}: the value token terminates"));
    let tok = std::str::from_utf8(&tail[..end]).unwrap_or_else(|e| panic!("{ctx}: utf8: {e}"));
    tok.parse::<f64>()
        .unwrap_or_else(|e| panic!("{ctx}: numeric value token: {e}"))
        .to_bits()
}

/// Issue #237 (Tier-1, scale-invariant): a seeded span's duration reaches
/// the HTTP wire as the exact bytes the reference emits for the same
/// width. Widths are derived-first — `1500ms`/`2s` are exactly
/// representable under both rounding forms and prove nothing; the six
/// 17-significant-digit widths (`ns > 2^53`, up to 12 599 days) are the
/// formatter-independent discriminators captured from the pinned
/// reference container (`grafana/tempo:3.0.2@sha256:cda87c21…`,
/// 2026-07-26). Very long spans are valid fixtures: `duration_ns` is
/// `Int64`, ingest does not clamp below `i64::MAX`, and the metrics
/// window predicate filters on span START only.
#[tokio::test(flavor = "multi_thread")]
async fn duration_seconds_reach_the_wire_exactly_as_the_reference_emits_them() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-server/tests/traces_api_live.rs for setup)"
        );
        return;
    }

    let _guard = spawn_ready(
        ULP_PORT,
        &pulsus_testkit::test_db("pulsus_traces_ulp_it_live"),
    );

    // One single-span service per width (12 discriminating + 3 controls),
    // one `ResourceSpans` per service, one sync protobuf POST /v1/traces.
    // The file's `ingest()`/`span()` helpers hardcode `checkout`/1 ms, so
    // the fixture is built locally. Span start is the file's existing
    // fixture instant; end = start + width.
    let mut widths: Vec<(String, i64, f64)> = Vec::new();
    for (i, (ns, want, _, _)) in REFERENCE_DURATION_SECONDS.iter().enumerate() {
        widths.push((format!("ulp-w{i}"), *ns, *want));
    }
    for (i, (ns, want)) in REFERENCE_DURATION_CONTROLS.iter().enumerate() {
        widths.push((format!("ulp-c{i}"), *ns, *want));
    }
    let start_ns: u64 = 3_100_000_000_000_000_200;
    let resource_spans: Vec<ResourceSpans> = widths
        .iter()
        .enumerate()
        .map(|(i, (svc, ns, _))| {
            let seq = u8::try_from(i + 1).expect("small fixture");
            let mut sp = span([seq; 16], [seq; 8], "ulp-span", start_ns);
            sp.end_time_unix_nano = start_ns + u64::try_from(*ns).expect("positive width");
            ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", svc)],
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
                    spans: vec![sp],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }
        })
        .collect();
    let req = ExportTraceServiceRequest { resource_spans };
    let ctx = "seed ULP widths";
    let res = request(
        ULP_PORT,
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

    // Window math in unix SECONDS (magnitude < 10^12), the file's
    // existing fixture idiom; the instant query returns one series with
    // one sample per service.
    for (i, (svc, ns, want)) in widths.iter().enumerate() {
        let q = format!(
            "%7B%20resource.service.name%20%3D%20%22{svc}%22%20%7D%20%7C%20max_over_time(duration)"
        );
        let path =
            format!("/api/traces/v1/metrics/query?q={q}&start=3099999000&end=3100001000&step=60");
        let ctx = format!("ULP width {ns} ns ({svc})");
        let res = get(ULP_PORT, &path, &[], &ctx);
        assert_eq!(
            res.status,
            200,
            "{ctx}: body {:?}",
            String::from_utf8_lossy(&res.body)
        );
        if i < REFERENCE_DURATION_SECONDS.len() {
            let (_, _, s_lit, t_lit) = &REFERENCE_DURATION_SECONDS[i];
            let probe = WireProbe::body(&res.body);
            assert!(s_lit.denotes(*want), "{ctx}: transcription");
            assert!(s_lit.occurs_in(&probe), "{ctx}: captured bytes on the wire");
            assert!(!t_lit.occurs_in(&probe), "{ctx}: two-rounding bytes");
        }
        // Decoded bit-equality for all 15 widths (controls included),
        // via the correctly rounded std parser on the raw value token.
        assert_eq!(
            decoded_instant_sample_bits(&res.body, &ctx),
            want.to_bits(),
            "{ctx}: decoded bits"
        );
    }
}

/// Issue #237 guard leg (e) for the bytes copy: `occurs_in` is
/// delimiter-sensitive, exercised through the SAME function the live
/// body assertions use, with needles built from the module's own
/// `surrounded_by` (derived text only). Hermetic — needs no ClickHouse
/// and rides the workspace test step on every PR.
#[test]
fn wire_literal_occurs_in_is_delimiter_sensitive() {
    // Hermetic, but it lives in a gated binary: without this the guard
    // would be per-suite-entry, and `--test <suite> <this test>` would
    // still exit 0 in a live CI job with the gate missing (issue #320).
    pulsus_testkit::require_live_gate(pulsus_testkit::CLICKHOUSE_GATE);
    for (ns, want, s_lit, t_lit) in REFERENCE_DURATION_SECONDS {
        assert!(s_lit.denotes(*want), "{ns}: transcription");
        for lit in [s_lit, t_lit] {
            let outside = lit.surrounded_by("prefix", "suffix");
            assert!(!lit.occurs_in(&outside), "{ns}");
            let closed = lit.surrounded_by("{\"value\":", "}");
            assert!(lit.occurs_in(&closed), "{ns}");
            let inside = lit.surrounded_by("{\"value\":", ",\"timestampMs\":\"1\"}");
            assert!(lit.occurs_in(&inside), "{ns}");
        }
    }
}

/// Issue #458 defect A, at the wire: a span summary carries
/// `durationNanos` as the reference's protojson `uint64` — a JSON
/// **string** of nanoseconds (`pkg/tempopb/tempo.proto:160`,
/// `pkg/traceql/engine.go:311` @ v3.0.2) — and carries no `durationMs`
/// anywhere inside `spanSets`. The trace level carries `durationMs`
/// (`pkg/tempopb/tempo.proto:139`) when it is non-zero, so the absence
/// check is scoped to the `spanSets` sub-object rather than the whole
/// body, and the trace level's own disposition is asserted separately per
/// width.
///
/// Three widths, each present for a different reason.
///
/// **`0` is the width that matters most, and it was missing.** protojson
/// omits a default-valued scalar, so a zero-width span comes back from the
/// reference with **no `durationNanos` key at all** — captured against
/// `grafana/tempo@sha256:aa8df8d0…`:
/// `{"spanID":"…","name":"fresh-w0","startTimeUnixNano":"…"}`. A
/// default-valued field is exactly where a hand-written encoder and a
/// protojson encoder part company, because emitting the zero is the
/// natural thing to write and is wrong. This suite shipped for one review
/// round without it, and the unit test beside it pinned our own
/// `"durationNanos":"0"`, so both were green against a false contract.
///
/// `9007199254740993` (2^53 + 1) and `545000` are the widths a
/// `durationMs` field destroys: a JSON number rounds the first to `…992`
/// and a millisecond integer is a different unit entirely — since issue
/// #473 that trace's `durationMs` saturates at the wire field's 32-bit
/// maximum, `4294967295`, because 9007199254 ms is outside it. The
/// second renders `0` as milliseconds.
///
/// Every needle below is a **delimited** byte literal spelled inline —
/// `"durationNanos":"<digits>"`, closing quote included — so
/// `9007199254740992` cannot satisfy the 2^53 + 1 assertion and a longer
/// digit string cannot satisfy the `545000` one. Spelling them inline
/// rather than building them from a variable is also what keeps the
/// issue #237 wire-literal scanner (`metrics_response.rs`, Rule D) able
/// to see that the search is guarded.
#[tokio::test(flavor = "multi_thread")]
async fn span_summaries_carry_duration_nanos_as_a_protojson_string_on_the_wire() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-server/tests/traces_api_live.rs for setup)"
        );
        return;
    }

    let port = SPAN_DURATION_PORT;
    let _guard = spawn_ready(
        port,
        &pulsus_testkit::test_db("pulsus_traces_span_duration_it_live"),
    );

    // The file's fixture instant; window math below is in unix SECONDS.
    let start_ns: u64 = 3_100_000_000_000_000_200;
    // ONE SINGLE-SPAN TRACE PER WIDTH — the shape the reference captures
    // used. Sharing one trace id would make every trace's root the widest
    // span, so the trace-level assertions below would all be about that
    // one width and the sub-millisecond case could not be tested at all.
    //
    // **This corpus is a deliberate CONTROL and must stay single-span**
    // (issue #464). Its subject is the per-width millisecond-omission
    // boundary and the `uint32`-wrap divergence, and a second span would
    // change the width under test. It therefore cannot discriminate the
    // trace's envelope from the root span's window — under a single-span
    // trace the two are the same number. The test that does discriminate
    // them is
    // `the_trace_level_start_and_duration_are_the_trace_envelope_on_the_wire`,
    // below.
    let spans = vec![
        {
            let mut sp = span([0xa1; 16], [0x04; 8], "huge", start_ns);
            sp.end_time_unix_nano = start_ns + 9_007_199_254_740_993;
            sp
        },
        {
            let mut sp = span([0xa2; 16], [0x05; 8], "tiny", start_ns);
            sp.end_time_unix_nano = start_ns + 545_000;
            sp
        },
        {
            // Zero width: `end == start`. Ingest's `resolve_duration_ns`
            // stores `0`, and the reference emits no `durationNanos` for
            // it.
            let mut sp = span([0xa3; 16], [0x06; 8], "instant", start_ns);
            sp.end_time_unix_nano = start_ns;
            sp
        },
    ];
    ingest(port, spans, "seed issue #458 span durations");

    // -- 2^53 + 1 ns: a JSON number would round it, and the trace-level
    // millisecond field is a different unit AND a narrower domain —
    // 9007199254 ms is above the wire `uint32`, so it saturates
    // (issue #473). The span's own nanoseconds are exact regardless.
    let (body, span_sets) =
        span_duration_probe(port, "huge", &"a1".repeat(16), Some(4_294_967_295));
    let sets = span_sets.as_bytes();
    assert!(
        find_subslice(&body, b"\"durationNanos\":\"9007199254740993\"").is_some(),
        "the raw body must carry the exact delimited field token, got {:?}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        find_subslice(sets, b"\"durationNanos\":\"9007199254740993\"").is_some(),
        "the field must sit inside spanSets: {span_sets}"
    );
    assert!(
        find_subslice(sets, b"durationMs").is_none(),
        "no span summary may carry durationMs: {span_sets}"
    );

    // -- 545 000 ns: sub-millisecond. The span keeps its exact nanoseconds
    // and the TRACE level omits `durationMs` entirely, because 545 000 ns
    // is 0 ms and protojson drops a default-valued uint32. This is the
    // width that shows the trace-level rule is about MILLISECONDS, not
    // about a zero-width trace.
    let (body, span_sets) = span_duration_probe(port, "tiny", &"a2".repeat(16), None);
    let sets = span_sets.as_bytes();
    assert!(
        find_subslice(&body, b"\"durationNanos\":\"545000\"").is_some(),
        "the raw body must carry the exact delimited field token, got {:?}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        find_subslice(sets, b"\"durationNanos\":\"545000\"").is_some(),
        "the field must sit inside spanSets: {span_sets}"
    );
    assert!(
        find_subslice(sets, b"durationMs").is_none(),
        "no span summary may carry durationMs: {span_sets}"
    );

    // -- 0 ns: the field is ABSENT, not `"0"`. Asserted three ways so the
    // gate cannot be satisfied by a rename or by a different rendering of
    // the same zero.
    let (_body, span_sets) = span_duration_probe(port, "instant", &"a3".repeat(16), None);
    let sets = span_sets.as_bytes();
    assert!(
        find_subslice(sets, b"durationNanos").is_none(),
        "protojson omits a default-valued uint64: a zero-width span carries NO durationNanos \
         key, which is what the reference returns for it: {span_sets}"
    );
    assert!(
        find_subslice(sets, b"durationMs").is_none(),
        "no span summary may carry durationMs either: {span_sets}"
    );
    assert!(
        find_subslice(sets, b"\"name\":\"instant\"").is_some(),
        "the zero-width span itself must still be returned — an absent FIELD is not an absent \
         SPAN: {span_sets}"
    );
}

/// Searches for one seeded span by name and returns `(raw response bytes,
/// the re-encoded `spanSets` sub-object)`. The trace level legitimately
/// may carry `durationMs`, so the absence check its callers run has to be
/// scoped to `spanSets` rather than to the whole body. `trace_duration_ms`
/// is the trace level's expected disposition: `Some(ms)` when the field
/// must be present with that value, `None` when the millisecond count is
/// zero and the field must be absent.
fn span_duration_probe(
    port: u16,
    name: &str,
    trace_hex: &str,
    trace_duration_ms: Option<i64>,
) -> (Vec<u8>, String) {
    let q = format!("%7B%20name%20%3D%20%22{name}%22%20%7D");
    let path = format!("/api/traces/v1/search?q={q}&start=3099999000&end=3100001000&limit=5");
    let ctx = format!("search {name}");
    let res = get(port, &path, &[], &ctx);
    assert_eq!(
        res.status,
        200,
        "{ctx}: body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    let json = res.json(&ctx);
    let all = json["traces"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: traces array, body {json}"));
    // Selected by TRACE ID, not by position or by count. The throwaway
    // database is keyed on the suite name, so a re-run of this test on the
    // same ClickHouse finds the previous run's traces too; picking by id
    // makes the assertions below deterministic under that accumulation
    // instead of depending on the store being empty.
    let traces: Vec<&serde_json::Value> = all
        .iter()
        .filter(|t| t["traceID"].as_str() == Some(trace_hex))
        .collect();
    assert_eq!(
        traces.len(),
        1,
        "{ctx}: exactly one trace with id {trace_hex}, body {json}"
    );
    // The TRACE level keeps `durationMs`, in MILLISECONDS, under the same
    // protojson omission rule (issue #458 review round 2): present when
    // the millisecond count is non-zero, absent when it is — which
    // includes every SUB-MILLISECOND trace, not only a zero-width one.
    match trace_duration_ms {
        Some(want) => assert_eq!(
            traces[0]["durationMs"].as_i64(),
            Some(want),
            "{ctx}: the trace level must carry durationMs = {want}, body {json}"
        ),
        None => assert!(
            traces[0].get("durationMs").is_none(),
            "{ctx}: a sub-millisecond trace rounds to 0 ms, and the reference omits a \
             default-valued uint32 — the key must be ABSENT, body {json}"
        ),
    }
    let span_sets = serde_json::to_string(&traces[0]["spanSets"])
        .unwrap_or_else(|e| panic!("{ctx}: re-encode spanSets: {e}"));
    (res.body, span_sets)
}

/// Issue #473 at the wire: every integer the search response emits lies
/// inside the domain of the wire field it lands in, and an empty root
/// service renders the reference's own literal marker.
///
/// **Why the whole body, byte for byte.** A strict protobuf-JSON client
/// decodes this body with no per-field recovery: it returns on the first
/// out-of-domain number and the caller gets that error instead of
/// results, so one bad trace discards every trace of the response.
/// Nothing in this repository can run that decoder — there is no Go step
/// and it is not vendored — so what is gated here is the property one
/// layer up: the exact bytes, including the values, the key sets and the
/// keys that must be ABSENT.
///
/// **The four seeded widths are chosen to discriminate, not to be
/// comfortable.**
///
/// * `wd-over` is `4294967296000000` ns — exactly `4294967296` ms, one
///   past the wire field's 32-bit maximum. It is the boundary itself.
/// * `wd-edge` is one nanosecond less and is the control: `4294967295`
///   ms is IN domain and must not move.
/// * `wd-i64max` is a far larger width that must produce the **same**
///   `durationMs` as `wd-over`. Two different inputs, one output, is what
///   saturation means; a wrapping renderer answers `0` and `2148491558`
///   for this pair, and the unclamped `i64` this route used to emit
///   answers `4294967296` and `6123372036854`. Its span's own
///   `durationNanos` stays exact at `9223372036854775807`, because that
///   field is 64-bit unsigned and nothing about it saturates — the case a
///   blanket "clamp everything to 32 bits" fix gets wrong.
/// * `wd-rootless` carries a resource with no attributes, so its
///   `rootServiceName` is empty and renders the marker. The other three
///   bodies carry `"rootServiceName":"checkout"` and so fail together if
///   the substitution is ever made unconditional.
///
/// The NEGATIVE half of the projection has no live coverage and cannot
/// have any: all three mounted write routes funnel through
/// `otlp_traces::resolve_duration_ns`, which clamps `end == 0` and
/// `end < start` to `0`, and the trace envelope is folded with
/// `saturating_add`. It is gated hermetically in
/// `crates/pulsus-server/src/traces_api/search_response.rs`
/// (`negative_widths_and_starts_render_inside_the_wire_domain`), and this
/// sentence is here so nobody reads this suite as covering it.
#[tokio::test(flavor = "multi_thread")]
async fn search_response_integers_stay_inside_their_wire_domain_on_the_wire() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-server/tests/traces_api_live.rs for setup)"
        );
        return;
    }

    let db = pulsus_testkit::test_db("pulsus_traces_wire_domain_it_live");
    drop_database(&db).await;
    let port = WIRE_DOMAIN_PORT;
    let _guard = spawn_ready(port, &db);

    // The file's fixture instant; window math below is in unix SECONDS.
    const START_NS: u64 = 3_100_000_000_000_000_200;
    // One single-span trace per width: sharing a trace id would make every
    // trace's envelope the widest span's, and three of the four widths
    // could not be tested at all.
    let mut spans = Vec::new();
    for (trace_byte, span_byte, name, end_ns) in [
        (0xa4u8, 0x11u8, "wd-over", 3_104_294_967_296_000_200u64),
        (0xa5, 0x12, "wd-edge", 3_104_294_967_296_000_199),
        // `end - start` overflows an `i64`, so ingest saturates the stored
        // width at `i64::MAX` — which is what makes this the far-larger
        // second input of the saturation pair.
        (0xa6, 0x13, "wd-i64max", 15_000_000_000_000_000_000),
    ] {
        let mut sp = span([trace_byte; 16], [span_byte; 8], name, START_NS);
        sp.end_time_unix_nano = end_ns;
        spans.push(sp);
    }
    ingest(port, spans, "seed issue #473 wire domains");

    // Its own push: the resource carries NO attributes, which is what
    // leaves `rootServiceName` empty.
    let mut rootless = span([0xa7; 16], [0x14; 8], "wd-rootless", START_NS);
    rootless.end_time_unix_nano = 3_100_000_000_042_000_200;
    ingest_rootless(port, vec![rootless], "seed issue #473 rootless trace");

    // (span name, the whole response body). Key order is `serde_json`'s:
    // the workspace does not enable `preserve_order`, so object keys come
    // out sorted.
    let expected: [(&str, &str); 4] = [
        (
            "wd-over",
            r#"{"metrics":{"completedJobs":1,"totalJobs":1},"traces":[{"durationMs":4294967295,"rootServiceName":"checkout","rootTraceName":"wd-over","spanSets":[{"matched":1,"spans":[{"durationNanos":"4294967296000000","name":"wd-over","spanID":"1111111111111111","startTimeUnixNano":"3100000000000000200"}]}],"startTimeUnixNano":"3100000000000000200","traceID":"a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4"}]}"#,
        ),
        (
            "wd-edge",
            r#"{"metrics":{"completedJobs":1,"totalJobs":1},"traces":[{"durationMs":4294967295,"rootServiceName":"checkout","rootTraceName":"wd-edge","spanSets":[{"matched":1,"spans":[{"durationNanos":"4294967295999999","name":"wd-edge","spanID":"1212121212121212","startTimeUnixNano":"3100000000000000200"}]}],"startTimeUnixNano":"3100000000000000200","traceID":"a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5"}]}"#,
        ),
        (
            "wd-i64max",
            r#"{"metrics":{"completedJobs":1,"totalJobs":1},"traces":[{"durationMs":4294967295,"rootServiceName":"checkout","rootTraceName":"wd-i64max","spanSets":[{"matched":1,"spans":[{"durationNanos":"9223372036854775807","name":"wd-i64max","spanID":"1313131313131313","startTimeUnixNano":"3100000000000000200"}]}],"startTimeUnixNano":"3100000000000000200","traceID":"a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6"}]}"#,
        ),
        (
            "wd-rootless",
            r#"{"metrics":{"completedJobs":1,"totalJobs":1},"traces":[{"durationMs":42,"rootServiceName":"<root span not yet received>","rootTraceName":"wd-rootless","spanSets":[{"matched":1,"spans":[{"durationNanos":"42000000","name":"wd-rootless","spanID":"1414141414141414","startTimeUnixNano":"3100000000000000200"}]}],"startTimeUnixNano":"3100000000000000200","traceID":"a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7"}]}"#,
        ),
    ];

    let mut bodies: HashMap<&str, Vec<u8>> = HashMap::new();
    for (name, want) in expected {
        let q = format!("%7B%20name%20%3D%20%22{name}%22%20%7D");
        let path = format!("/api/traces/v1/search?q={q}&start=3099999000&end=3100001000&limit=5");
        let ctx = format!("wire domain {name}");
        let res = get(port, &path, &[], &ctx);
        assert_eq!(
            res.status,
            200,
            "{ctx}: body {:?}",
            String::from_utf8_lossy(&res.body)
        );
        assert_eq!(
            String::from_utf8_lossy(&res.body),
            want,
            "{ctx}: the whole response body, byte for byte"
        );
        bodies.insert(name, res.body);
    }

    // The discriminating tokens again, each spelled INLINE as a delimited
    // byte string, so a failure names the property that moved rather than
    // only reporting that two long bodies differ — and so the issue #237
    // wire-literal scanner (`metrics_response.rs`, Rule D) can see that
    // every raw-byte search in this file is guarded by a literal needle.
    // Delimited on both sides: `4294967295` is a prefix of
    // `4294967295999999`, and a bare digit needle could not tell the
    // saturated trace field from the exact span one.
    let over = bodies.get("wd-over").expect("wd-over body");
    assert!(
        find_subslice(over, b"\"durationMs\":4294967295").is_some(),
        "the boundary width must SATURATE at the wire uint32 maximum, got {:?}",
        String::from_utf8_lossy(over)
    );
    let edge = bodies.get("wd-edge").expect("wd-edge body");
    assert!(
        find_subslice(edge, b"\"durationMs\":4294967295").is_some(),
        "one nanosecond below the boundary is IN domain and must not move, got {:?}",
        String::from_utf8_lossy(edge)
    );
    let i64max = bodies.get("wd-i64max").expect("wd-i64max body");
    assert!(
        find_subslice(i64max, b"\"durationMs\":4294967295").is_some(),
        "a far larger width must saturate to the SAME number as the boundary one — two \
         different inputs, one output, is what saturation means, got {:?}",
        String::from_utf8_lossy(i64max)
    );
    assert!(
        find_subslice(i64max, b"\"durationNanos\":\"9223372036854775807\"").is_some(),
        "the span's own width is 64-bit unsigned on the wire and nothing about it saturates, \
         got {:?}",
        String::from_utf8_lossy(i64max)
    );
    // Short binding name on purpose: the issue #237 scanner (Rule D)
    // reads one masked line at a time and treats a call whose needle
    // rustfmt wrapped onto the next line as unguarded, so the needle and
    // its haystack have to fit rustfmt's 60-column call-argument budget.
    let rs = bodies.get("wd-rootless").expect("wd-rootless body");
    assert!(
        find_subslice(rs, b"\"rootServiceName\":\"<root span not yet received>\"").is_some(),
        "an empty root service renders the reference's literal marker, unescaped, got {:?}",
        String::from_utf8_lossy(rs)
    );

    // The marker must NOT appear on the three traces that have a service.
    // Asserted here rather than only through the three bodies above so the
    // failure names the rule rather than a diff.
    for name in ["wd-over", "wd-edge", "wd-i64max"] {
        let q = format!("%7B%20name%20%3D%20%22{name}%22%20%7D");
        let path = format!("/api/traces/v1/search?q={q}&start=3099999000&end=3100001000&limit=5");
        let ctx = format!("wire domain marker scope {name}");
        let res = get(port, &path, &[], &ctx);
        assert!(
            find_subslice(&res.body, b"\"rootServiceName\":\"checkout\"").is_some(),
            "{ctx}: a present root service is never substituted, got {:?}",
            String::from_utf8_lossy(&res.body)
        );
    }
}

/// Issue #464 at the wire: a trace's `startTimeUnixNano` and `durationMs`
/// are the TRACE's envelope — the earliest span start of the whole trace,
/// and `max(span end) - min(span start)` in integer milliseconds — not the
/// root span's own window. The reference fills both from the spanset
/// (`pkg/traceql/engine.go:294-295` @ v3.0.2), whose writer computes
/// `traceStart` and `traceEnd - traceStart` over every span
/// (`tempodb/encoding/vparquet4/schema.go:558-560`).
///
/// The corpus is adversarial **by construction, and that is asserted
/// before any value is compared**: a root that starts five seconds after
/// its own child, a later child that extends the trace, a child that
/// starts inside the root and ends after it, and a single-span control
/// where the root IS the trace. Three of the four answer differently under
/// the two rules; the fourth agrees under both and is kept as the control.
///
/// This is the end-to-end leg: the corpus goes in through the product
/// ingest path (`POST /v1/traces`, sync) and comes back out of the real
/// search route, so it covers the engine's fold, the retained context and
/// the renderer together rather than any one of them alone.
#[tokio::test(flavor = "multi_thread")]
async fn the_trace_level_start_and_duration_are_the_trace_envelope_on_the_wire() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-server/tests/traces_api_live.rs for setup)"
        );
        return;
    }

    let port = TRACE_ENVELOPE_PORT;
    let _guard = spawn_ready(
        port,
        &pulsus_testkit::test_db("pulsus_traces_envelope_it_live"),
    );

    // The file's fixture instant; window math below is in unix SECONDS.
    const BASE_NS: u64 = 3_100_000_000_000_000_000;

    // (tag / root span name, trace id byte, root start offset, root width,
    //  optional child (start offset, width), expected trace start offset,
    //  expected durationMs).
    struct Case {
        tag: &'static str,
        id: u8,
        root_start_off: u64,
        root_dur: u64,
        child: Option<(u64, u64)>,
        trace_start_off: u64,
        duration_ms: i64,
    }
    let cases = [
        Case {
            // The root starts AFTER its own child (clock skew).
            tag: "env-skew",
            id: 0xe1,
            root_start_off: 5_000_000_000,
            root_dur: 42_000_000,
            child: Some((0, 545_000)),
            trace_start_off: 0,
            duration_ms: 5042,
        },
        Case {
            // A later child extends the trace past the root's end.
            tag: "env-late-child",
            id: 0xe2,
            root_start_off: 0,
            root_dur: 42_000_000,
            child: Some((1_000_000_000, 545_000)),
            trace_start_off: 0,
            duration_ms: 1000,
        },
        Case {
            // A child starts inside the root and ends after it.
            tag: "env-overrun",
            id: 0xe3,
            root_start_off: 0,
            root_dur: 10_000_000,
            child: Some((5_000_000, 30_000_000)),
            trace_start_off: 0,
            duration_ms: 35,
        },
        Case {
            // The control: one span, so the root IS the trace and both
            // rules agree. Kept deliberately — it pins that the envelope
            // rule does not move the ordinary single-span answer.
            tag: "env-only",
            id: 0xe4,
            root_start_off: 0,
            root_dur: 2_500_000_000,
            child: None,
            trace_start_off: 0,
            duration_ms: 2500,
        },
    ];

    // The corpus relation, asserted BEFORE anything is seeded or
    // compared: a narrowed corpus fails here, on the relation, rather
    // than passing under both rules.
    assert!(
        cases.iter().any(|c| c.root_start_off > c.trace_start_off),
        "the corpus must carry a trace whose ROOT starts after the trace does"
    );
    assert!(
        cases
            .iter()
            .filter(|c| c.root_dur != (c.duration_ms as u64) * 1_000_000)
            .count()
            >= 2,
        "the corpus must carry at least two traces whose ROOT width differs from the trace's"
    );

    for case in &cases {
        let trace_id = [case.id; 16];
        let mut spans = Vec::new();
        let root_start = BASE_NS + case.root_start_off;
        let mut root = span(trace_id, [0x01; 8], case.tag, root_start);
        root.end_time_unix_nano = root_start + case.root_dur;
        spans.push(root);
        if let Some((child_off, child_dur)) = case.child {
            let child_start = BASE_NS + child_off;
            let mut child = span(
                trace_id,
                [0x02; 8],
                &format!("{}-child", case.tag),
                child_start,
            );
            child.parent_span_id = vec![0x01; 8];
            child.end_time_unix_nano = child_start + child_dur;
            spans.push(child);
        }
        ingest(port, spans, "seed issue #464 trace envelope");
    }

    for case in &cases {
        let ctx = format!("envelope {}", case.tag);
        let trace_hex = hex(&[case.id; 16]);
        let q = format!("%7B%20name%20%3D%20%22{}%22%20%7D", case.tag);
        let path = format!("/api/traces/v1/search?q={q}&start=3099999000&end=3100001000&limit=5");
        let res = get(port, &path, &[], &ctx);
        assert_eq!(
            res.status,
            200,
            "{ctx}: body {:?}",
            String::from_utf8_lossy(&res.body)
        );
        let json = res.json(&ctx);
        let all = json["traces"]
            .as_array()
            .unwrap_or_else(|| panic!("{ctx}: traces array, body {json}"));
        // Selected by TRACE ID, not by position: the throwaway database
        // is keyed on the suite name, so a re-run on the same ClickHouse
        // finds the previous run's traces too.
        let traces: Vec<&serde_json::Value> = all
            .iter()
            .filter(|t| t["traceID"].as_str() == Some(trace_hex.as_str()))
            .collect();
        assert_eq!(
            traces.len(),
            1,
            "{ctx}: exactly one trace with id {trace_hex}, body {json}"
        );
        let trace = traces[0];

        // The ROOT's metadata is still the root span's — the envelope
        // rule moves the two time fields and nothing else.
        assert_eq!(
            trace["rootTraceName"].as_str(),
            Some(case.tag),
            "{ctx}: rootTraceName stays the ROOT SPAN's, body {json}"
        );
        assert_eq!(
            trace["startTimeUnixNano"].as_str(),
            Some((BASE_NS + case.trace_start_off).to_string().as_str()),
            "{ctx}: startTimeUnixNano is the EARLIEST span of the trace, not the root's start, \
             body {json}"
        );
        assert_eq!(
            trace["durationMs"].as_i64(),
            Some(case.duration_ms),
            "{ctx}: durationMs is max(span end) - min(span start) in MILLISECONDS, not the root \
             span's width, body {json}"
        );
    }
}
