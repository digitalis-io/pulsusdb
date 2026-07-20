//! Issue #36: the live exhaustive API conformance matrix. Expands
//! `support::manifest::route_manifest()` (every `Mounted` `RouteSpec`) ×
//! case-classes into HTTP requests against five real `pulsusdb` spawns —
//! one per mode/auth/compat permutation the plan pins (v2 finding 3) —
//! over loopback (bare `TcpStream` HTTP/1.1, the same idiom
//! `live_server.rs`/`prom_api_live.rs`/`logs_api_live.rs` already use — no
//! new HTTP-client dependency).
//!
//! **No seeded data.** Every assertion here is status/envelope/headers-only
//! (architect plan: "no PromQL/LogQL correctness assertions — conformance
//! asserts status/envelope/headers only"), and every read handler this
//! matrix drives returns a well-formed `200` with an empty result set
//! against a freshly-`--mode init`-reconciled, empty database — `/ready`
//! reaching `200` already implies the pool (and, in reader-enabled modes,
//! the label cache **and** the writer/metric-writer slots — `serve.rs`
//! constructs the writer(s) *before* publishing the pool slot) are live,
//! so no cache-warm poll or ClickHouse seed is needed anywhere below.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, same podman/docker harness as
//! the other live suites:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test api_conformance
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Runtime budget: 6 process spawns (each dominated by ClickHouse schema
//! reconcile, a handful of seconds) plus a few hundred cheap loopback
//! requests (the sixth spawn also seeds two tiny rows directly) — comfortably
//! inside the single-digit-minute budget the architect plan documents.
//! Ports 31120-31125, distinct from every other live suite's fixed ports
//! (31100-31117).
//!
//! **`query_too_broad` (422) live coverage** (code-review round-1 finding):
//! `TooBroadReason` has two independent triggers (`logql/error.rs`) —
//! `StreamCap` (`DEFAULT_MAX_STREAMS = 100_000`, hard-coded, **no** config
//! knob exists to lower it — `PULSUS_LOGQL_MAX_STREAMS` is a doc-comment
//! aspiration in `pulsus-read`, never wired into `pulsus-config`/`env.rs`)
//! and `ScanBudgetBytes` (`reader.logql_scan_budget_bytes`, **is**
//! configurable via `PULSUS_LOGQL_SCAN_BUDGET_BYTES`, default 50 GiB). The
//! sixth spawn below sets that budget to `1` byte and seeds two minimal
//! rows — any real read exceeds 1 byte, tripping ClickHouse's `code 307
//! TOO_MANY_BYTES` -> `ReadError::QueryTooBroad(ScanBudgetBytes)` -> the
//! same documented `422 query_too_broad` taxonomy row `StreamCap` would
//! have produced (this matrix asserts status/envelope only, never internal
//! correctness — either `TooBroadReason` variant proves the same live
//! contract). The `StreamCap` trigger specifically stays unit-level-only
//! (`logql/exec.rs::exceeding_the_stream_cap_maps_to_stream_cap_not_scan_budget_bytes`),
//! since it has no config knob to reach cheaply.

#[path = "support/manifest.rs"]
mod manifest;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use flate2::read::GzDecoder;
use prost::Message;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

use manifest::{
    CaseClass, ExpectedError, Gate, Method, RouteSpec, RouteStatus, Surface, route_manifest,
};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

fn ch_host() -> String {
    std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string())
}

fn ch_http_port() -> String {
    std::env::var("PULSUS_TEST_CH_HTTP_PORT").unwrap_or_else(|_| "19123".to_string())
}

fn ch_http_port_num() -> u16 {
    ch_http_port()
        .parse()
        .expect("PULSUS_TEST_CH_HTTP_PORT is a valid u16")
}

// ---------------------------------------------------------------------
// Bare-`TcpStream` HTTP/1.1 helper (extends `logs_api_live.rs`'s own
// idiom to arbitrary methods/headers/raw bodies + gzip request/response).
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

    /// `ctx` is the caller's full matrix-cell identifier (round-10
    /// finding: a malformed-envelope failure must name the exact cell,
    /// not just say "invalid JSON body").
    fn json(&self, ctx: &str) -> serde_json::Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("{ctx}: invalid JSON body: {e}\nbody: {:?}", self.body))
    }

    /// Token-matches `accept` in the (comma-joined) `Vary` header — never a
    /// substring check, since `accept-encoding` (the compression layer's
    /// own `Vary` contribution) contains `accept` as a substring but is a
    /// distinct token.
    fn has_vary_accept(&self) -> bool {
        self.headers
            .get("vary")
            .map(|v| {
                v.split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("accept"))
            })
            .unwrap_or(false)
    }
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

/// Issues one raw HTTP/1.1 request over loopback for `req` (`manifest::Req`
/// — method/path/query/headers/content-type/body already fully built by
/// the caller/`CaseClass::build`) and returns the exact response bytes,
/// dechunked and gzip-decoded when the server negotiated it.
fn raw_request(port: u16, req: &manifest::Req) -> Option<RawResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();

    let target = if req.query.is_empty() {
        req.path.clone()
    } else {
        format!("{}?{}", req.path, req.query)
    };
    let mut head = format!(
        "{} {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n",
        req.method
    );
    if let Some(ct) = req.content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    for (name, value) in &req.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", req.body.len()));

    stream.write_all(head.as_bytes()).ok()?;
    stream.write_all(&req.body).ok()?;

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
    // last-wins: a negotiating-route response may carry two `Vary` lines
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

    let dechunked = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };

    let body = if headers.get("content-encoding").is_some_and(|v| v == "gzip") {
        let mut decoded = Vec::new();
        GzDecoder::new(&dechunked[..])
            .read_to_end(&mut decoded)
            .ok()?;
        decoded
    } else {
        dechunked
    };

    Some(RawResponse {
        status,
        headers,
        body,
    })
}

/// `ctx` is the caller's full matrix-cell identifier (round-11 finding: a
/// transport failure — connection refused, read timeout — must name the
/// exact cell, not just the path).
fn get(port: u16, path: &str, ctx: &str) -> RawResponse {
    raw_request(port, &manifest::Req::new("GET", path.to_string()))
        .unwrap_or_else(|| panic!("{ctx}: request must be reachable (transport failure)"))
}

// ---------------------------------------------------------------------
// Process lifecycle
// ---------------------------------------------------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_ready(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port())
        .env("CLICKHOUSE_DB", db);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command.spawn().expect("spawn pulsusdb");
    let guard = ChildGuard(child);

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if get_maybe(port, "/ready").is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port}, db {db})");
}

fn get_maybe(port: u16, path: &str) -> Option<RawResponse> {
    raw_request(port, &manifest::Req::new("GET", path.to_string()))
}

// ---------------------------------------------------------------------
// Ingest body builders (mirror `pulsus-write/src/ingest/http.rs`'s own
// test fixtures — no lib-exported "valid request" helper exists there, so
// this crate builds its own minimal ones from the public OTLP/
// `pulsus_write::WriteRequest` types).
// ---------------------------------------------------------------------

fn valid_otlp_logs_body() -> Vec<u8> {
    let record = LogRecord {
        time_unix_nano: 1_700_000_000_000_000_000,
        body: Some(AnyValue {
            value: Some(Value::StringValue("conformance".to_string())),
        }),
        ..Default::default()
    };
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![record],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

fn valid_otlp_metrics_body() -> Vec<u8> {
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "conformance_metric".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: 1_700_000_000_000_000_000,
                            exemplars: vec![],
                            flags: 0,
                            value: Some(number_data_point::Value::AsDouble(1.0)),
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

fn valid_otlp_traces_body() -> Vec<u8> {
    let span = Span {
        trace_id: vec![1; 16],
        span_id: vec![2; 8],
        name: "conformance-span".to_string(),
        start_time_unix_nano: 1_700_000_000_000_000_000,
        end_time_unix_nano: 1_700_000_001_000_000_000,
        ..Default::default()
    };
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

// OTLP/JSON (proto3-JSON) bodies for the content-negotiation cells (issue
// #76). The SAME logical `Export*ServiceRequest` as the protobuf builders
// above, serialized through `opentelemetry-proto`'s `with-serde` impls (hex
// IDs, camelCase, string timestamps) — sent with `Content-Type:
// application/json`, they select the JSON decode fork in `ingest/http.rs`.
fn valid_otlp_logs_json_body() -> Vec<u8> {
    let record = LogRecord {
        time_unix_nano: 1_700_000_000_000_000_000,
        body: Some(AnyValue {
            value: Some(Value::StringValue("conformance".to_string())),
        }),
        ..Default::default()
    };
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![record],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    serde_json::to_vec(&req).expect("serialize OTLP/JSON logs body")
}

fn valid_otlp_metrics_json_body() -> Vec<u8> {
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "conformance_metric".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: 1_700_000_000_000_000_000,
                            exemplars: vec![],
                            flags: 0,
                            value: Some(number_data_point::Value::AsDouble(1.0)),
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    serde_json::to_vec(&req).expect("serialize OTLP/JSON metrics body")
}

fn valid_otlp_traces_json_body() -> Vec<u8> {
    let span = Span {
        trace_id: vec![1; 16],
        span_id: vec![2; 8],
        name: "conformance-span".to_string(),
        start_time_unix_nano: 1_700_000_000_000_000_000,
        end_time_unix_nano: 1_700_000_001_000_000_000,
        ..Default::default()
    };
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    serde_json::to_vec(&req).expect("serialize OTLP/JSON traces body")
}

fn valid_remote_write_body() -> Vec<u8> {
    use pulsus_write::protocols::remote_write::{Label, Sample, TimeSeries, WriteRequest};
    let req = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![Label {
                name: "__name__".to_string(),
                value: "conformance_up".to_string(),
            }],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1_700_000_000_000,
            }],
        }],
        metadata: vec![],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress a valid WriteRequest")
}

/// A valid snappy-protobuf Loki `PushRequest` body (issue #77): one stream
/// `{service_name="checkout"}` with one entry — the agent-default wire form
/// (`Content-Type: application/x-protobuf`, implicit snappy, no
/// `Content-Encoding`).
fn valid_loki_push_body() -> Vec<u8> {
    use pulsus_write::protocols::loki_push::{EntryAdapter, PushRequest, StreamAdapter, Timestamp};
    let req = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{service_name="checkout"}"#.to_string(),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                line: "conformance".to_string(),
                structured_metadata: Vec::new(),
            }],
        }],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress a valid Loki PushRequest")
}

/// A valid Zipkin v2 JSON span array (issue #75): one span with a 64-bit
/// trace id and micros timing — the wire form a Zipkin agent POSTs to
/// `/api/v2/spans`. Deliberately carries NO `localEndpoint`/`tags` (so it
/// indexes zero attributes) and uses the same 2023 era as
/// [`valid_otlp_traces_body`] — the empty-DB read assertions in the full
/// conformance matrix ingest this on the success path and must still see an
/// empty tags catalog / empty search window afterwards (the OTLP traces
/// body is attribute-less for the same reason).
fn valid_zipkin_body() -> Vec<u8> {
    br#"[{"traceId":"0000000000000001","id":"0000000000000002","name":"conformance","timestamp":1700000000000000,"duration":1000}]"#
        .to_vec()
}

/// The ingest handlers' hand-rolled `google.rpc.Status { code, message }`
/// protobuf (mirrors `pulsus-write/src/ingest/http.rs`'s private `Status`
/// type — not exported, so this test binary defines its own decode-only
/// copy of the exact same wire shape).
#[derive(Clone, PartialEq, ::prost::Message)]
struct Status {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

// ---------------------------------------------------------------------
// Path resolution + generic request construction
// ---------------------------------------------------------------------

/// Resolves an axum route template's `{param}` placeholders to a concrete
/// value — content-agnostic (no seeded data assumed), just enough to make
/// the path syntactically dispatchable.
fn resolve_path(path: &str) -> String {
    // `{traceId}` must resolve to a *valid, absent* 32-hex id (issue #55
    // verification finding: the old 8-char `abcd1234` placeholder is
    // invalid under the 16-or-32-hex rule and would 400 on the mounted
    // route instead of exercising the absent-trace 404 oracle).
    path.replace("{name}", "env")
        .replace("{traceId}", manifest::ABSENT_TRACE_ID)
        .replace("{tag}", "service.name")
        .replace("{kind}", "logs")
}

fn undocumented_method(spec: &RouteSpec) -> &'static str {
    // No `RouteSpec` in the manifest documents `DELETE` — a safe universal
    // choice for the "one undocumented method -> 405" case-class.
    debug_assert!(!spec.methods.contains(&Method::Delete));
    "DELETE"
}

/// Builds the "valid" request for `spec`'s success-status assertion:
/// `GET` with `base_query` as the query string, `POST` with `base_query`
/// as an `application/x-www-form-urlencoded` body — `Surface::Ingest`
/// routes are never routed through this (see `assert_ingest_family`).
fn valid_request(spec: &RouteSpec, method: Method) -> manifest::Req {
    assert_ne!(
        spec.surface,
        Surface::Ingest,
        "ingest routes build their own bodies"
    );
    let mut req = manifest::Req::new(method.as_str(), resolve_path(spec.path));
    match method {
        Method::Get => req.query = spec.base_query.to_string(),
        Method::Post => {
            req.content_type = Some("application/x-www-form-urlencoded");
            req.body = spec.base_query.as_bytes().to_vec();
        }
        _ => panic!("unexpected documented method {method:?} on {}", spec.path),
    }
    req
}

/// The concrete JSON type each query endpoint's success `data` field
/// carries (docs/api.md §2/§3's own response shapes): `query`/`query_range`
/// wrap an object (`{"resultType","result",...}`), the discovery routes
/// return bare arrays, `/metadata` and every `/status/*` route return
/// objects. Round-7 finding (medium): `data: null` (or the wrong JSON
/// type) must fail the cell, not merely "data key present".
fn expected_data_is_array(path: &str) -> bool {
    let last = path.rsplit('/').next().unwrap_or(path);
    matches!(last, "labels" | "values" | "series" | "query_exemplars")
}

fn assert_success_envelope(spec: &RouteSpec, res: &RawResponse, ctx: &str) {
    match (spec.surface, spec.path) {
        (Surface::OpsPublic, "/metrics") => {
            // Content-type is unconditional (`ops::metrics_handler` sets it
            // regardless of what `state.metrics.render()` produces); body
            // *content* is not asserted here beyond that — writer-only mode
            // never bridges the label cache's counters in (nothing else is
            // recorded either yet), so an empty body is a genuine, correct
            // `/metrics` response in that mode, not an envelope defect.
            assert_eq!(
                res.content_type(),
                Some("text/plain; version=0.0.4"),
                "{ctx}: /metrics content-type"
            );
        }
        (Surface::OpsPublic, _) => {
            // "/ready": a 200 is a bare `StatusCode::OK.into_response()`
            // (`ops.rs`) — empty body, no Content-Type (round-7 finding:
            // explicit body/header assertion, not status-only).
            assert_eq!(res.status, 200, "{ctx}: /ready status");
            assert!(
                res.body.is_empty(),
                "{ctx}: a 200 /ready body must be empty, got {:?}",
                String::from_utf8_lossy(&res.body)
            );
            assert!(
                res.content_type().is_none(),
                "{ctx}: a 200 /ready must carry no Content-Type header, got {:?}",
                res.content_type()
            );
        }
        (Surface::OpsAuthed, "/config") => {
            assert_eq!(
                res.content_type(),
                Some("text/plain; charset=utf-8"),
                "{ctx}: /config content-type (docs/api.md correction: not YAML media type)"
            );
            assert!(
                !res.body.is_empty(),
                "{ctx}: /config body must be non-empty"
            );
        }
        (Surface::OpsAuthed, _) => {
            // "/buildinfo": every documented field must be a populated
            // (non-null, non-empty) string (round-7 finding: presence
            // alone accepted `null`).
            assert!(
                res.content_type()
                    .is_some_and(|ct| ct.starts_with("application/json")),
                "{ctx}: /buildinfo content-type"
            );
            let json = res.json(ctx);
            for field in ["version", "revision", "builtAt", "rustc"] {
                assert!(
                    json.get(field)
                        .and_then(|v| v.as_str())
                        .is_some_and(|s| !s.is_empty()),
                    "{ctx}: {field:?} must be a non-empty string, body {json}"
                );
            }
        }
        (Surface::LogsQuery, _) | (Surface::PromApi, _) => {
            assert!(
                res.content_type()
                    .is_some_and(|ct| ct.starts_with("application/json")),
                "{ctx}: query envelope content-type"
            );
            let json = res.json(ctx);
            assert_eq!(json["status"], "success", "{ctx}: envelope status field");
            let data = json
                .get("data")
                .unwrap_or_else(|| panic!("{ctx}: envelope must carry data, body {json}"));
            // Round-7 finding (medium): `data` must be the endpoint's
            // actual documented JSON type — `null` (or a mismatched type)
            // fails the cell.
            if expected_data_is_array(spec.path) {
                assert!(
                    data.is_array(),
                    "{ctx}: data must be a JSON array, got {data}"
                );
            } else {
                assert!(
                    data.is_object(),
                    "{ctx}: data must be a JSON object, got {data}"
                );
            }
        }
        (Surface::LogsTail, _) => {
            // The pinned bare-GET `WebSocketUpgrade` rejection (issue
            // #74; `Surface::LogsTail`'s doc comment — pinned empirically
            // by `logs_api/tail.rs`'s own unit test): exact plain-text
            // body. The mounting oracle: an unmounted path is an EMPTY
            // 404 instead.
            assert_eq!(
                res.content_type(),
                Some("text/plain; charset=utf-8"),
                "{ctx}: tail bare-GET rejection content-type"
            );
            assert_eq!(
                res.body,
                b"Connection header did not include 'upgrade'",
                "{ctx}: tail bare-GET rejection body, got {:?}",
                String::from_utf8_lossy(&res.body)
            );
        }
        (Surface::LogsStats, _) => {
            // Issue #74, docs/api.md §2.5: the bare stats object — no
            // status/data envelope. Against this suite's empty databases
            // every counter is exactly 0 (the mounting oracle).
            assert!(
                res.content_type()
                    .is_some_and(|ct| ct.starts_with("application/json")),
                "{ctx}: stats content-type"
            );
            let json = res.json(ctx);
            for field in ["streams", "chunks", "entries", "bytes"] {
                assert_eq!(
                    json[field], 0,
                    "{ctx}: empty-DB stats must be zeroed, body {json}"
                );
            }
            assert!(
                json.get("status").is_none(),
                "{ctx}: stats is the bare object, never the status envelope, body {json}"
            );
        }
        (Surface::TracesFetch, _) => {
            // Against this suite's empty databases the documented outcome
            // of a well-formed fetch is the mounted-but-absent `404
            // not_found` JSON envelope — the mounting oracle
            // (`Surface::TracesFetch`'s doc comment): an unmounted path
            // would return axum's *empty* 404 instead, so this arm fails
            // on a silently un-mounted route. Errors on this surface are
            // always JSON, never protobuf.
            assert_case_envelope(
                res,
                &ExpectedError::Json {
                    error_type: "not_found",
                    has_position: false,
                },
                ctx,
            );
        }
        (Surface::TracesSearch, _) => {
            // Issue #57: success is the documented docs/api.md §4.2
            // envelope, not the `{"status","data"}` query envelope.
            // Against this suite's empty databases the well-formed
            // match-all search returns the empty envelope — the mounting
            // oracle (an unmounted path would 404 instead of 200).
            assert!(
                res.content_type()
                    .is_some_and(|ct| ct.starts_with("application/json")),
                "{ctx}: search envelope content-type"
            );
            let json = res.json(ctx);
            assert_eq!(
                json["traces"],
                serde_json::json!([]),
                "{ctx}: empty DB must return an empty traces array, body {json}"
            );
            assert_eq!(json["metrics"]["partial"], false, "{ctx}: body {json}");
            assert_eq!(json["metrics"]["returned"], 0, "{ctx}: body {json}");
            assert!(
                json["metrics"]["limit"].is_u64(),
                "{ctx}: metrics.limit must be an integer, body {json}"
            );
        }
        (Surface::TracesMetrics, path) => {
            // Issue #59: success is the shared Prometheus query envelope
            // (`prom_api::encode::query_response`). Against this suite's
            // empty databases the well-formed match-all metrics request
            // is the mounting oracle: `query_range` → an empty matrix;
            // `query` → exactly one label-less `"0"` vector sample (a
            // `uniqExact` with no `GROUP BY` always returns one row).
            assert!(
                res.content_type()
                    .is_some_and(|ct| ct.starts_with("application/json")),
                "{ctx}: metrics envelope content-type"
            );
            let json = res.json(ctx);
            assert_eq!(json["status"], "success", "{ctx}: body {json}");
            if path.ends_with("/query_range") {
                assert_eq!(json["data"]["resultType"], "matrix", "{ctx}: body {json}");
                assert_eq!(
                    json["data"]["result"],
                    serde_json::json!([]),
                    "{ctx}: empty DB must return an empty matrix, body {json}"
                );
            } else {
                assert_eq!(json["data"]["resultType"], "vector", "{ctx}: body {json}");
                let result = json["data"]["result"]
                    .as_array()
                    .unwrap_or_else(|| panic!("{ctx}: vector result must be an array: {json}"));
                assert_eq!(
                    result.len(),
                    1,
                    "{ctx}: an instant metrics query always returns one sample, body {json}"
                );
                assert_eq!(
                    result[0]["metric"],
                    serde_json::json!({}),
                    "{ctx}: single-series M4 output is label-less, body {json}"
                );
                assert_eq!(
                    result[0]["value"][1], "0",
                    "{ctx}: empty DB instant value is the quoted \"0\", body {json}"
                );
            }
        }
        (Surface::TracesTags, path) => {
            // Issue #58: success is the documented docs/api.md §4.3
            // native envelope. Against this suite's empty databases both
            // routes return the empty envelope 200 — the mounting oracle
            // (an unmounted path would 404 instead). For the values
            // route this cell runs with the manifest's NON-TRIVIAL
            // `base_query` `q=` — proving accept-and-ignore (a 400 here
            // is the adjudicated-away rejection behavior).
            assert!(
                res.content_type()
                    .is_some_and(|ct| ct.starts_with("application/json")),
                "{ctx}: tags envelope content-type"
            );
            let json = res.json(ctx);
            if path.ends_with("/values") {
                assert_eq!(
                    json["tagValues"],
                    serde_json::json!([]),
                    "{ctx}: empty DB must return an empty tagValues array, body {json}"
                );
            } else {
                assert_eq!(
                    json["scopes"],
                    serde_json::json!([]),
                    "{ctx}: empty DB must return an empty scopes array, body {json}"
                );
            }
            assert_eq!(
                json["truncated"], false,
                "{ctx}: an empty result set is never truncated, body {json}"
            );
        }
        (Surface::TracesTagsV2, path) => {
            // Issue #61: the v2 aliases are the native §4.3 shapes MINUS
            // the PulsusDB-only `truncated` field (Tempo v2 wire-shape
            // conformance). Empty-DB empty envelope 200 is the mounting
            // oracle; the `truncated` ABSENCE is the reshaping oracle (a
            // pure binding onto the native handler would carry it). The
            // seeded non-empty shape proof lives in `traces_tags_live.rs`.
            assert!(
                res.content_type()
                    .is_some_and(|ct| ct.starts_with("application/json")),
                "{ctx}: v2 tags alias envelope content-type"
            );
            let json = res.json(ctx);
            if path.ends_with("/values") {
                assert_eq!(
                    json["tagValues"],
                    serde_json::json!([]),
                    "{ctx}: empty DB must return an empty tagValues array, body {json}"
                );
            } else {
                assert_eq!(
                    json["scopes"],
                    serde_json::json!([]),
                    "{ctx}: empty DB must return an empty scopes array, body {json}"
                );
            }
            assert!(
                json.get("truncated").is_none(),
                "{ctx}: the v2 alias must drop the native `truncated` field, body {json}"
            );
        }
        (Surface::TracesTagsV1, path) => {
            // Issue #61: Tempo's legacy v1 FLAT shapes — bare-string
            // arrays, no scopes, no types, no `truncated`. Empty-DB flat
            // empty envelope 200 is the mounting oracle. Seeded
            // non-empty flat-vs-typed proof in `traces_tags_live.rs`.
            assert!(
                res.content_type()
                    .is_some_and(|ct| ct.starts_with("application/json")),
                "{ctx}: v1 tags alias envelope content-type"
            );
            let json = res.json(ctx);
            if path.ends_with("/values") {
                assert_eq!(
                    json["tagValues"],
                    serde_json::json!([]),
                    "{ctx}: empty DB must return an empty flat tagValues array, body {json}"
                );
            } else {
                assert_eq!(
                    json["tagNames"],
                    serde_json::json!([]),
                    "{ctx}: empty DB must return an empty flat tagNames array, body {json}"
                );
                assert!(
                    json.get("scopes").is_none(),
                    "{ctx}: the v1 flat shape has no scopes key, body {json}"
                );
            }
            assert!(
                json.get("truncated").is_none(),
                "{ctx}: the v1 flat shape has no truncated key, body {json}"
            );
        }
        (Surface::Echo, _) => {
            // Issue #61: the constant Tempo echo — exact body, exact
            // content-type (axum's `&'static str` response).
            assert_eq!(
                res.content_type(),
                Some("text/plain; charset=utf-8"),
                "{ctx}: echo content-type"
            );
            assert_eq!(
                res.body,
                b"echo",
                "{ctx}: echo body must be the exact constant, got {:?}",
                String::from_utf8_lossy(&res.body)
            );
        }
        (Surface::Ingest, _) => {
            unreachable!("{ctx}: ingest routes assert via assert_ingest_family")
        }
    }
}

fn assert_case_envelope(res: &RawResponse, expect: &ExpectedError, ctx: &str) {
    match expect {
        ExpectedError::Json {
            error_type,
            has_position,
        } => {
            assert!(
                res.content_type()
                    .is_some_and(|ct| ct.starts_with("application/json")),
                "{ctx}: expected a JSON error envelope, content-type was {:?}",
                res.content_type()
            );
            let json = res.json(ctx);
            assert_eq!(json["status"], "error", "{ctx}");
            assert_eq!(json["errorType"], *error_type, "{ctx}: body {json}");
            assert!(
                json.get("error")
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| !s.is_empty()),
                "{ctx}: `error` message must be a non-empty string, body {json}"
            );
            assert_eq!(
                json.get("position").is_some(),
                *has_position,
                "{ctx}: `position` field presence, body {json}"
            );
        }
        ExpectedError::Otlp { code } => {
            assert_eq!(
                res.content_type(),
                Some("application/x-protobuf"),
                "{ctx}: OTLP error content-type"
            );
            let status = Status::decode(res.body.as_slice())
                .unwrap_or_else(|e| panic!("{ctx}: invalid google.rpc.Status protobuf: {e}"));
            assert_eq!(status.code, *code, "{ctx}: google.rpc.Status.code");
            assert!(
                !status.message.is_empty(),
                "{ctx}: message must be non-empty"
            );
        }
        ExpectedError::PlainText => {
            assert_eq!(
                res.content_type(),
                Some("text/plain; charset=utf-8"),
                "{ctx}: remote-write error content-type"
            );
            assert!(
                !res.body.is_empty(),
                "{ctx}: plain-text error body must be non-empty"
            );
        }
    }
}

/// Review round-1 finding (high): every 404 cell pins axum's actual
/// not-found response shape — empty body, no `Content-Type` — not just the
/// status code (verified empirically: `curl` against a real spawn shows
/// `content-length: 0` and no `content-type` header on every unmatched
/// path). Round-2 finding (medium): the `Content-Type` *absence* is now
/// asserted explicitly, not just implied by the body being empty.
fn assert_404_empty(res: &RawResponse, ctx: &str) {
    assert_eq!(res.status, 404, "{ctx}: status");
    assert!(
        res.body.is_empty(),
        "{ctx}: axum's not-found body must be empty, got {:?}",
        res.body
    );
    assert!(
        res.content_type().is_none(),
        "{ctx}: a 404 response must carry no Content-Type header, got {:?}",
        res.content_type()
    );
}

/// Axum's `MethodRouter` `Allow` header value for a route mounted on
/// exactly `methods` — verified empirically (`GET,HEAD` for a GET-only
/// route, `GET,HEAD,POST` for GET+POST, bare `POST` for POST-only; `HEAD`
/// is always synthesized alongside `GET`, never alongside any other
/// method). Every `RouteSpec.methods` in this manifest is one of exactly
/// those three shapes.
fn expected_allow(methods: &[Method]) -> String {
    let mut parts: Vec<&str> = Vec::new();
    if methods.contains(&Method::Get) {
        parts.push("GET");
        parts.push("HEAD");
    }
    for m in [Method::Post, Method::Put, Method::Delete, Method::Patch] {
        if methods.contains(&m) {
            parts.push(m.as_str());
        }
    }
    parts.join(",")
}

/// Review round-1 finding (high): every 405 cell pins the `Allow` header
/// axum's `MethodRouter` sets, plus the same empty-body shape as 404.
/// Round-2 finding (medium): pins the *exact* expected method set derived
/// from the manifest (`expected_allow`), not just "non-empty", and asserts
/// `Content-Type` absence too (verified empirically alongside the other
/// two).
fn assert_405_with_allow(res: &RawResponse, ctx: &str, methods: &[Method]) {
    assert_eq!(res.status, 405, "{ctx}: status");
    assert!(
        res.body.is_empty(),
        "{ctx}: a 405 body must be empty, got {:?}",
        res.body
    );
    assert!(
        res.content_type().is_none(),
        "{ctx}: a 405 response must carry no Content-Type header, got {:?}",
        res.content_type()
    );
    let expected = expected_allow(methods);
    assert_eq!(
        res.headers.get("allow").map(String::as_str),
        Some(expected.as_str()),
        "{ctx}: Allow header must name exactly the documented methods"
    );
}

/// Review round-1 finding (high): the auth perimeter's failure envelope —
/// pinned exactly (`middleware::BasicAuth::validate`'s own response:
/// `Body::from("unauthorized")`, `WWW-Authenticate: Basic`, no
/// `Content-Type`), not just the `401` status. Round-2 finding (medium):
/// the `Content-Type` absence is now asserted explicitly too.
fn assert_401_unauthorized(res: &RawResponse, ctx: &str) {
    assert_eq!(res.status, 401, "{ctx}: status");
    assert_eq!(
        res.headers.get("www-authenticate").map(String::as_str),
        Some("Basic"),
        "{ctx}: WWW-Authenticate header"
    );
    assert!(
        res.content_type().is_none(),
        "{ctx}: a 401 response must carry no Content-Type header, got {:?}",
        res.content_type()
    );
    assert_eq!(
        res.body,
        b"unauthorized",
        "{ctx}: unauthenticated body must be the exact pinned text, got {:?}",
        String::from_utf8_lossy(&res.body)
    );
}

/// Method conformance + routing + invalid-param cases + success envelope
/// for one `Surface::{OpsPublic,OpsAuthed,LogsQuery,PromApi}` `RouteSpec`
/// (every documented method reaches the exact documented success status;
/// one undocumented method is `405`; a sibling nonexistent path is `404`;
/// every `CaseClass` in `spec.cases` reaches its exact `(status, envelope)`).
/// `spawn` names the server permutation (AC/round-9 finding: every cell
/// failure names the exact `[spawn] METHOD path case=...` cell).
fn assert_full_route_matrix(port: u16, spec: &RouteSpec, spawn: &str) {
    for &method in spec.methods {
        let ctx = format!(
            "[{spawn}] {} {} case=documented-method-success",
            method.as_str(),
            spec.path
        );
        let req = valid_request(spec, method);
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        assert_eq!(
            res.status,
            spec.success_status,
            "{ctx}: status (body: {:?})",
            String::from_utf8_lossy(&res.body)
        );
        assert_success_envelope(spec, &res, &ctx);
    }

    let ctx = format!(
        "[{spawn}] {} {} case=undocumented-method-405",
        undocumented_method(spec),
        spec.path
    );
    let undocumented = manifest::Req::new(undocumented_method(spec), resolve_path(spec.path));
    let res =
        raw_request(port, &undocumented).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
    assert_405_with_allow(&res, &ctx, spec.methods);

    let ctx = format!("[{spawn}] GET {} case=sibling-nonexistent-404", spec.path);
    let sibling = format!("{}-conformance-nonexistent", resolve_path(spec.path));
    let res = get(port, &sibling, &ctx);
    assert_404_empty(&res, &ctx);

    for case in spec.cases {
        run_case(port, spec, case, spawn);
    }
}

/// The dedicated trace-fetch matrix (issue #55; `Surface::TracesFetch` is
/// special-cased at the dispatch site exactly as `Surface::Ingest` is):
/// the generic [`assert_full_route_matrix`] is incompatible with a
/// trailing-`{param}` route — its sibling-404 check appends a suffix to
/// the resolved path, which here merely *mutates the param* and hits the
/// same route as a 400, not axum's empty 404. The sibling here is proven
/// by appending an extra `/segment` instead, making the path genuinely
/// unrouted. Cells (plan v2/v3): absent 32-hex → 404 `not_found` JSON
/// envelope; 16-hex short id → 404 (accepted, not 400); protobuf /
/// x-protobuf `Accept` on absent → still the 404 JSON envelope (errors
/// never switch to protobuf; for the `/json` route this also proves the
/// suffix ignores `Accept`); `POST` → 405 `Allow: GET,HEAD`; extra-segment
/// sibling → empty 404; plus the manifest's `CaseClass`es (malformed hex →
/// 400).
fn assert_traces_fetch_route(port: u16, spec: &RouteSpec, spawn: &str) {
    let absent_404 = ExpectedError::Json {
        error_type: "not_found",
        has_position: false,
    };
    let path = resolve_path(spec.path);

    // The negotiating route (`trace_by_id`) carries `Vary: accept` on
    // every response it returns (issue #55 follow-up); the `/json` route
    // (`trace_by_id_json`) never consults `Accept`, so it never does.
    let negotiating = !spec.path.ends_with("/json");
    let assert_vary = |res: &RawResponse, ctx: &str| {
        if negotiating {
            assert!(res.has_vary_accept(), "{ctx}: must Vary: accept");
        } else {
            assert!(!res.has_vary_accept(), "{ctx}: /json never Vary: accept");
        }
    };

    for &method in spec.methods {
        let ctx = format!(
            "[{spawn}] {} {} case=documented-method-absent-404",
            method.as_str(),
            spec.path
        );
        let req = manifest::Req::new(method.as_str(), path.clone());
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        assert_eq!(
            res.status,
            spec.success_status,
            "{ctx}: status (body: {:?})",
            String::from_utf8_lossy(&res.body)
        );
        assert_success_envelope(spec, &res, &ctx);
        assert_vary(&res, &ctx);
    }

    // A 16-hex short id is *accepted* (left-padded to 32) and absent — 404
    // with the envelope, never a 400.
    let ctx = format!("[{spawn}] GET {} case=short-16-hex-absent-404", spec.path);
    let short_path = path.replace(manifest::ABSENT_TRACE_ID, "feedfacefeedface");
    let res = get(port, &short_path, &ctx);
    assert_eq!(
        res.status,
        404,
        "{ctx}: a 16-hex id must be accepted and resolve to absent, got {} (body: {:?})",
        res.status,
        String::from_utf8_lossy(&res.body)
    );
    assert_case_envelope(&res, &absent_404, &ctx);
    assert_vary(&res, &ctx);

    // Errors never switch representation: a protobuf-flavoured `Accept`
    // (both the canonical name and the x- request-side alias) still gets
    // the 404 JSON envelope. On the `/json` route this doubles as the
    // "/json ignores Accept" error-path cell.
    for accept in ["application/protobuf", "application/x-protobuf"] {
        let ctx = format!(
            "[{spawn}] GET {} case=absent-404-stays-json accept={accept}",
            spec.path
        );
        let mut req = manifest::Req::new("GET", path.clone());
        req.headers.push(("accept", accept.to_string()));
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        assert_eq!(res.status, 404, "{ctx}: status");
        assert_case_envelope(&res, &absent_404, &ctx);
        assert_vary(&res, &ctx);
    }

    // Inline malformed-hex 400 cell (issue #55 follow-up; deliberately not
    // routed through the manifest's shared `malformed_hex` `CaseClass` /
    // `run_case` — those are shared with routes this fix does not touch).
    let ctx = format!("[{spawn}] GET {} case=malformed-hex-400-vary", spec.path);
    let malformed_path = path.replace(
        manifest::ABSENT_TRACE_ID,
        "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
    );
    let res = get(port, &malformed_path, &ctx);
    assert_eq!(res.status, 400, "{ctx}: status");
    assert_vary(&res, &ctx);

    let ctx = format!("[{spawn}] POST {} case=undocumented-method-405", spec.path);
    let post = manifest::Req::new("POST", path.clone());
    let res = raw_request(port, &post).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
    assert_405_with_allow(&res, &ctx, spec.methods);
    assert!(
        !res.has_vary_accept(),
        "{ctx}: axum-generated 405 must not carry the handler's Vary"
    );

    let ctx = format!("[{spawn}] GET {} case=extra-segment-sibling-404", spec.path);
    let sibling = format!("{path}/conformance-nonexistent");
    let res = get(port, &sibling, &ctx);
    assert_404_empty(&res, &ctx);
    assert!(
        !res.has_vary_accept(),
        "{ctx}: axum-generated sibling-404 must not carry the handler's Vary"
    );

    for case in spec.cases {
        run_case(port, spec, case, spawn);
    }
}

/// Attempts a WebSocket handshake (`Connection: Upgrade` — the one shape
/// [`raw_request`]'s hardcoded `Connection: close` cannot express) and
/// returns the HTTP response. On `101` the body is empty and the still-
/// open stream is returned (dropping it closes the connection); on any
/// other status the (content-length-framed) error body is read fully.
fn ws_attempt(port: u16, target: &str, ctx: &str) -> (RawResponse, Option<TcpStream>) {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .unwrap_or_else(|e| panic!("{ctx}: connect failed: {e}"));
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");
    let head = format!(
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    stream
        .write_all(head.as_bytes())
        .unwrap_or_else(|e| panic!("{ctx}: write failed: {e}"));

    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    while find_subslice(&buf, b"\r\n\r\n").is_none() {
        let n = stream.read(&mut chunk).unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let split_at = find_subslice(&buf, b"\r\n\r\n")
        .unwrap_or_else(|| panic!("{ctx}: no response-header terminator"));
    let head_text = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let mut lines = head_text.lines();
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or_else(|| panic!("{ctx}: unparsable status line"));
    let headers: HashMap<String, String> = lines
        .filter_map(|line| {
            let (k, v) = line.split_once(':')?;
            Some((k.trim().to_ascii_lowercase(), v.trim().to_string()))
        })
        .collect();

    if status == 101 {
        return (
            RawResponse {
                status,
                headers,
                body: Vec::new(),
            },
            Some(stream),
        );
    }
    let mut body = buf[split_at + 4..].to_vec();
    if let Some(len) = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
    {
        while body.len() < len {
            let n = stream.read(&mut chunk).unwrap_or(0);
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
        body.truncate(len);
    }
    (
        RawResponse {
            status,
            headers,
            body,
        },
        None,
    )
}

/// The dedicated live-tail WebSocket matrix (issue #74;
/// `Surface::LogsTail` is special-cased at the dispatch site exactly as
/// `Surface::Ingest`/`TracesFetch` are — the generic matrix's
/// `Connection: close` requests cannot perform an upgrade handshake):
/// bare GET → the empirically-pinned 400 rejection (mounting oracle);
/// real handshake + valid selector → `101`; handshake + missing/metric
/// query → the 400 JSON envelope BEFORE any upgrade; `POST` → 405
/// `Allow: GET,HEAD`; sibling nonexistent path → empty 404. Streaming
/// content is `logs_tail_live.rs`'s job; slot exhaustion (429) gets its
/// own spawn below.
fn assert_tail_route(port: u16, spec: &RouteSpec, spawn: &str) {
    let path = resolve_path(spec.path);

    // Bare GET (no upgrade headers): the pinned rejection.
    let ctx = format!("[{spawn}] GET {} case=bare-get-pinned-rejection", spec.path);
    let mut req = manifest::Req::new("GET", path.clone());
    req.query = spec.base_query.to_string();
    let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
    assert_eq!(
        res.status,
        spec.success_status,
        "{ctx}: status (body: {:?})",
        String::from_utf8_lossy(&res.body)
    );
    assert_success_envelope(spec, &res, &ctx);

    // Real handshake with a valid selector: 101 Switching Protocols.
    let ctx = format!("[{spawn}] GET {} case=handshake-101", spec.path);
    let (res, stream) = ws_attempt(port, &format!("{path}?{}", spec.base_query), &ctx);
    assert_eq!(res.status, 101, "{ctx}: status");
    assert!(stream.is_some(), "{ctx}: upgraded stream returned");
    drop(stream); // closing the TCP stream ends the tail connection

    // Handshake with a MISSING query: rejected 400 (JSON envelope)
    // before any upgrade — the response is plain HTTP, never a 101.
    let ctx = format!(
        "[{spawn}] GET {} case=handshake-missing-query-400",
        spec.path
    );
    let (res, stream) = ws_attempt(port, &path, &ctx);
    assert!(stream.is_none(), "{ctx}: must not upgrade");
    assert_eq!(res.status, 400, "{ctx}: status");
    assert_case_envelope(
        &res,
        &ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
        &ctx,
    );

    // Handshake with a METRIC query: same pre-upgrade 400.
    let ctx = format!(
        "[{spawn}] GET {} case=handshake-metric-query-400",
        spec.path
    );
    let metric_q = "query=count_over_time(%7Bservice_name%3D%22checkout%22%7D%5B1h%5D)";
    let (res, stream) = ws_attempt(port, &format!("{path}?{metric_q}"), &ctx);
    assert!(stream.is_none(), "{ctx}: must not upgrade");
    assert_eq!(res.status, 400, "{ctx}: status");
    assert_case_envelope(
        &res,
        &ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
        &ctx,
    );

    let ctx = format!("[{spawn}] POST {} case=undocumented-method-405", spec.path);
    let post = manifest::Req::new("POST", path.clone());
    let res = raw_request(port, &post).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
    assert_405_with_allow(&res, &ctx, spec.methods);

    let ctx = format!("[{spawn}] GET {} case=sibling-nonexistent-404", spec.path);
    let sibling = format!("{path}-conformance-nonexistent");
    let res = get(port, &sibling, &ctx);
    assert_404_empty(&res, &ctx);
}

fn run_case(port: u16, spec: &RouteSpec, case: &CaseClass, spawn: &str) {
    let mut req = manifest::Req::new(spec.methods[0].as_str(), resolve_path(spec.path));
    (case.build)(&mut req);
    let ctx = format!("[{spawn}] {} {} case={}", req.method, spec.path, case.name);
    let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
    assert_eq!(
        res.status,
        case.expect_status,
        "{ctx}: status (body: {:?})",
        String::from_utf8_lossy(&res.body)
    );
    assert_case_envelope(&res, &case.expect, &ctx);
}

/// The success-outcome envelope for one of the three `Surface::Ingest`
/// routes (v3/v4 per-outcome split: remote-write's empty-body/no-
/// `Content-Type` success shape is structurally distinct from OTLP's
/// `Export*ServiceResponse` protobuf) — shared by [`assert_ingest_route`]'s
/// full async-header matrix and [`all_mode_auth_on_perimeter`]'s
/// valid-credentials cell. `ctx` is the caller's full cell identifier
/// (round-9 finding: failures name the exact cell, including header/
/// credential variant, not just the path).
fn assert_ingest_success_envelope(spec: &RouteSpec, res: &RawResponse, ctx: &str) {
    // The empty-body family (remote-write `/api/v1/write` + the Loki push
    // receiver `/loki/api/v1/push` at 204, issue #77; the Zipkin v2 JSON
    // receiver `/api/v2/spans` + `/tempo/spans` at 202, issue #75): the sync
    // success carries no Content-Type and an empty body, whatever its exact
    // 2xx status.
    if matches!(spec.success_status, 202 | 204) {
        assert!(
            res.content_type().is_none(),
            "{ctx}: an empty-body ingest success must carry no Content-Type header"
        );
        assert!(
            res.body.is_empty(),
            "{ctx}: an empty-body ingest success must have an empty body"
        );
        return;
    }
    assert_eq!(
        res.content_type(),
        Some("application/x-protobuf"),
        "{ctx}: OTLP success content-type"
    );
    if spec.path == "/v1/logs" {
        ExportLogsServiceResponse::decode(res.body.as_slice()).unwrap_or_else(|e| {
            panic!("{ctx}: success body must decode as ExportLogsServiceResponse: {e}")
        });
    } else if spec.path == "/v1/traces" {
        ExportTraceServiceResponse::decode(res.body.as_slice()).unwrap_or_else(|e| {
            panic!("{ctx}: success body must decode as ExportTraceServiceResponse: {e}")
        });
    } else {
        ExportMetricsServiceResponse::decode(res.body.as_slice()).unwrap_or_else(|e| {
            panic!("{ctx}: success body must decode as ExportMetricsServiceResponse: {e}")
        });
    }
}

/// The three `Surface::Ingest` routes' full matrix: `X-Pulsus-Async` ∈
/// {absent, `0`, `1`} against the documented exact sync/async status per
/// route (plan v2 finding 2, restored/pinned), the per-outcome envelope
/// split (plan v3/v4: OTLP success = `Export*ServiceResponse` protobuf,
/// OTLP error = `google.rpc.Status` protobuf; remote-write success = empty
/// body with **no** `Content-Type` header, remote-write error =
/// `text/plain; charset=utf-8`), a sibling-404, an undocumented-method-405,
/// and every `CaseClass` in the manifest.
fn assert_ingest_route(port: u16, spec: &RouteSpec, spawn: &str) {
    let is_remote_write = spec.path == "/api/v1/write";
    // The empty-body family: remote-write and the Loki push receiver (204,
    // issue #77) plus the Zipkin v2 JSON receiver (202, issue #75) — all
    // distinguished from the OTLP routes' `200` + `Export*ServiceResponse`
    // protobuf by an empty-body success at their documented 2xx status
    // (`spec.success_status` drives the sync cells; async always 202).
    let is_empty_body = matches!(spec.success_status, 202 | 204);
    let body_for = |path: &str| -> Vec<u8> {
        match path {
            "/v1/logs" => valid_otlp_logs_body(),
            "/v1/metrics" => valid_otlp_metrics_body(),
            "/v1/traces" => valid_otlp_traces_body(),
            "/api/v1/write" => valid_remote_write_body(),
            "/loki/api/v1/push" => valid_loki_push_body(),
            "/api/v2/spans" | "/tempo/spans" => valid_zipkin_body(),
            other => panic!("no valid-body builder registered for ingest route {other}"),
        }
    };

    let sync = spec.success_status;
    let async_cases: &[(Option<&str>, u16)] = if is_empty_body {
        &[(None, sync), (Some("0"), sync), (Some("1"), 202)]
    } else {
        &[(None, 200), (Some("0"), 200), (Some("1"), 202)]
    };
    for (header, expect_status) in async_cases {
        let ctx = format!(
            "[{spawn}] POST {} case=success x-pulsus-async={}",
            spec.path,
            header.unwrap_or("<absent>")
        );
        let mut req = manifest::Req::new("POST", spec.path);
        if let Some(v) = header {
            req.headers.push(("x-pulsus-async", v.to_string()));
        }
        // Review round-1 finding (medium): send the documented request
        // headers (docs/api.md §1.1-1.2) on the success path, not just an
        // unlabeled body — `Content-Type: application/x-protobuf` for OTLP,
        // plus `Content-Encoding: snappy` for remote-write.
        req.content_type = Some("application/x-protobuf");
        if is_remote_write {
            req.headers.push(("content-encoding", "snappy".to_string()));
        }
        req.body = body_for(spec.path);
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        assert_eq!(res.status, *expect_status, "{ctx}: status");
        assert_ingest_success_envelope(spec, &res, &ctx);
    }

    // OTLP content-negotiation, OTLP-only (issue #76 — the conformance FLIP
    // of #36's deferred "reject-or-ignore under application/json" question).
    // `ingest/http.rs` now forks on `Content-Type`: `application/json` selects
    // the OTLP/JSON decode path, everything else stays protobuf. Two pinned
    // cells replace the prior "Content-Type ignored, protobuf-under-json → 200"
    // assertion the #36 adjudication explicitly deferred here:
    //   (a) a valid OTLP/JSON body + `application/json` → the ordinary success
    //       envelope (JSON is a first-class second encoding);
    //   (b) a protobuf body + `application/json` → 400 / google.rpc.Status.code
    //       == 3 (now undecodable as JSON — negotiation is real, not ignored).
    //
    // Skipped for the empty-body family: remote-write ignores Content-Type
    // entirely (its own always-snappy path), the Loki push receiver (issue
    // #77) negotiates on it with its own JSON grammar, and the Zipkin v2 JSON
    // receiver (issue #75) always decodes JSON (Content-Type is not a fork
    // discriminator) — all pinned by the `ingest/http.rs` handler tests +
    // `LOKI_PUSH_CASES`/`ZIPKIN_CASES` instead.
    if !is_empty_body {
        let json_body_for = |path: &str| -> Vec<u8> {
            match path {
                "/v1/logs" => valid_otlp_logs_json_body(),
                "/v1/metrics" => valid_otlp_metrics_json_body(),
                "/v1/traces" => valid_otlp_traces_json_body(),
                other => panic!("no OTLP/JSON body builder registered for ingest route {other}"),
            }
        };

        // (a) valid OTLP/JSON body under application/json → success.
        let ctx = format!(
            "[{spawn}] POST {} case=otlp-json-body-negotiated-success",
            spec.path
        );
        let mut req = manifest::Req::new("POST", spec.path);
        req.content_type = Some("application/json");
        req.body = json_body_for(spec.path);
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        assert_eq!(
            res.status, spec.success_status,
            "{ctx}: a valid OTLP/JSON body under Content-Type: application/json must succeed"
        );
        assert_ingest_success_envelope(spec, &res, &ctx);

        // (b) protobuf body under application/json → 400 / code 3 (the flip:
        // content negotiation now routes it to the JSON decoder, which rejects
        // it — this exact body used to be pinned as a 200).
        let ctx = format!(
            "[{spawn}] POST {} case=protobuf-body-under-json-now-400",
            spec.path
        );
        let mut req = manifest::Req::new("POST", spec.path);
        req.content_type = Some("application/json");
        req.body = body_for(spec.path);
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        assert_eq!(
            res.status, 400,
            "{ctx}: a protobuf body under Content-Type: application/json is now undecodable-as-JSON \
             (the #76 conformance flip)"
        );
        assert_eq!(
            res.content_type(),
            Some("application/x-protobuf"),
            "{ctx}: OTLP error content-type"
        );
        let status = Status::decode(res.body.as_slice())
            .unwrap_or_else(|e| panic!("{ctx}: invalid google.rpc.Status protobuf: {e}"));
        assert_eq!(
            status.code, 3,
            "{ctx}: google.rpc.Status.code (INVALID_ARGUMENT)"
        );
    }

    let ctx = format!(
        "[{spawn}] {} {} case=undocumented-method-405",
        undocumented_method(spec),
        spec.path
    );
    let undocumented = manifest::Req::new(undocumented_method(spec), spec.path.to_string());
    let res =
        raw_request(port, &undocumented).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
    assert_405_with_allow(&res, &ctx, spec.methods);

    let ctx = format!("[{spawn}] GET {} case=sibling-nonexistent-404", spec.path);
    let sibling = format!("{}-conformance-nonexistent", spec.path);
    let res = get(port, &sibling, &ctx);
    assert_404_empty(&res, &ctx);

    for case in spec.cases {
        run_case(port, spec, case, spawn);
    }
}

/// `Accept-Encoding: identity` vs `gzip` byte-identity (edge case 4): the
/// global `CompressionLayer` must never alter decoded bytes. Run against a
/// representative subset (one JSON ops route, `/metrics`, one
/// `LogsQuery` route, one `PromApi` route) — every mounted route's body is
/// encoded through the exact same global layer, so this is not a per-route
/// property; the plan calls for "at least one JSON and one `/metrics`
/// body".
fn assert_gzip_identity(port: u16, path: &str, query: &str, spawn: &str) {
    // Round-10 finding (medium): the full `[spawn] METHOD path case
    // variant` cell identifier, per leg.
    let ctx_id = format!("[{spawn}] GET {path} case=gzip-identity leg=identity");
    let ctx_gz = format!("[{spawn}] GET {path} case=gzip-identity leg=gzip");
    let mut req = manifest::Req::new("GET", path.to_string());
    req.query = query.to_string();
    // Round-8 finding (medium): the identity leg requests `identity`
    // explicitly and asserts no Content-Encoding came back — an
    // always-gzip server (unsolicited compression) now fails this leg
    // rather than being transparently decoded and passing.
    req.headers
        .push(("accept-encoding", "identity".to_string()));
    let identity = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx_id}: must be reachable"));
    assert!(
        !identity.headers.contains_key("content-encoding"),
        "{ctx_id}: must carry no Content-Encoding header, got {:?}",
        identity.headers.get("content-encoding")
    );
    req.headers.pop();
    req.headers.push(("accept-encoding", "gzip".to_string()));
    let gz = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx_gz}: must be reachable"));
    assert_eq!(
        gz.headers.get("content-encoding").map(String::as_str),
        Some("gzip"),
        "{ctx_gz}: must actually negotiate gzip for this assertion to be meaningful"
    );
    // Round-7 finding (medium): status + Content-Type equality, not just
    // body identity — gzip negotiation must never change either.
    assert_eq!(identity.status, 200, "{ctx_id}: status");
    assert_eq!(
        gz.status, identity.status,
        "{ctx_gz}: status must equal the identity leg's"
    );
    assert_eq!(
        gz.content_type(),
        identity.content_type(),
        "{ctx_gz}: Content-Type must equal the identity leg's"
    );
    assert_eq!(
        gz.body, identity.body,
        "{ctx_gz}: gzip-decoded body must be byte-identical to the identity response"
    );
}

/// `/metrics`'s own gzip-identity leg: unlike every other route this
/// matrix drives, `ops::metrics_handler` bridges the label cache's *live*
/// counters/gauges into the response on every single scrape (`ops.rs`'s
/// own doc comment) — `pulsus_label_cache_age_ms`'s value (and, per the
/// underlying exporter's label-set registration order, the ordering of
/// the labelled `pulsus_label_cache_misses_total{reason=...}` series)
/// genuinely differ between two back-to-back requests, by design, not by
/// a gzip-layer defect. Normalizes both bodies the same way
/// (`explain_indexes.rs`'s established "collapse the volatile part before
/// comparing" idiom, applied to whole lines here rather than digits) so
/// this still proves gzip decode fidelity — corruption in the compression
/// layer would show up as a genuine content mismatch even after
/// normalizing away the one known-live gauge and the label-order
/// nondeterminism.
fn assert_gzip_identity_metrics(port: u16, spawn: &str) {
    fn normalize(body: &[u8]) -> Vec<String> {
        let mut lines: Vec<String> = String::from_utf8_lossy(body)
            .lines()
            .filter(|line| !line.contains("pulsus_label_cache_age_ms"))
            .map(str::to_string)
            .collect();
        lines.sort();
        lines
    }

    // Round-10 finding (medium): full cell identifiers, per leg.
    let ctx_id = format!("[{spawn}] GET /metrics case=gzip-identity leg=identity");
    let ctx_gz = format!("[{spawn}] GET /metrics case=gzip-identity leg=gzip");
    let mut req = manifest::Req::new("GET", "/metrics".to_string());
    // Round-8 finding (medium): explicit `identity` + Content-Encoding
    // absence, same as `assert_gzip_identity`.
    req.headers
        .push(("accept-encoding", "identity".to_string()));
    let identity = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx_id}: must be reachable"));
    assert!(
        !identity.headers.contains_key("content-encoding"),
        "{ctx_id}: must carry no Content-Encoding header, got {:?}",
        identity.headers.get("content-encoding")
    );
    req.headers.pop();
    req.headers.push(("accept-encoding", "gzip".to_string()));
    let gz = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx_gz}: must be reachable"));
    assert_eq!(
        gz.headers.get("content-encoding").map(String::as_str),
        Some("gzip"),
        "{ctx_gz}: must actually negotiate gzip for this assertion to be meaningful"
    );
    // Round-7 finding (medium): status + Content-Type equality too.
    assert_eq!(identity.status, 200, "{ctx_id}: status");
    assert_eq!(
        gz.status, identity.status,
        "{ctx_gz}: status must equal the identity leg's"
    );
    assert_eq!(
        gz.content_type(),
        identity.content_type(),
        "{ctx_gz}: Content-Type must equal the identity leg's"
    );
    assert_eq!(
        normalize(&gz.body),
        normalize(&identity.body),
        "{ctx_gz}: gzip-decoded body must match the identity response once the one known-live \
         gauge line and label-set ordering are normalized away"
    );
}

fn base64_encode(input: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied();
        let b2 = chunk.get(2).copied();
        let n =
            (u32::from(b0) << 16) | (u32::from(b1.unwrap_or(0)) << 8) | u32::from(b2.unwrap_or(0));
        out.push(CHARS[((n >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((n >> 12) & 0x3F) as usize] as char);
        out.push(if b1.is_some() {
            CHARS[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        out.push(if b2.is_some() {
            CHARS[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

// ---------------------------------------------------------------------
// Spawn 1: mode=all, auth off, compat on — the full mounted surface.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn all_mode_auth_off_compat_on_full_matrix() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_120;
    let db = "pulsus_api_conformance_it_full";
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "true")]);

    for spec in route_manifest() {
        if spec.status != RouteStatus::Mounted {
            continue;
        }
        match spec.surface {
            Surface::Ingest => assert_ingest_route(port, spec, "spawn=all,auth=off,compat=on"),
            Surface::TracesFetch => {
                assert_traces_fetch_route(port, spec, "spawn=all,auth=off,compat=on")
            }
            Surface::LogsTail => assert_tail_route(port, spec, "spawn=all,auth=off,compat=on"),
            _ => assert_full_route_matrix(port, spec, "spawn=all,auth=off,compat=on"),
        }
    }

    let spawn = "spawn=all,auth=off,compat=on";
    assert_gzip_identity(port, "/buildinfo", "", spawn);
    assert_gzip_identity(
        port,
        "/api/logs/v1/query_range",
        "query=%7Bservice_name%3D%22checkout%22%7D",
        spawn,
    );
    assert_gzip_identity(port, "/api/v1/query", "query=up", spawn);
    assert_gzip_identity_metrics(port, spawn);
}

// ---------------------------------------------------------------------
// Spawn 2: mode=all, auth on — the 401 perimeter.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn all_mode_auth_on_perimeter() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_121;
    let db = "pulsus_api_conformance_it_auth";
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_COMPAT_ENDPOINTS", "true"),
            ("PULSUS_AUTH_USER", "alice"),
            ("PULSUS_AUTH_PASSWORD", "hunter2"),
        ],
    );

    let valid = format!("Basic {}", base64_encode(b"alice:hunter2"));
    let invalid = format!("Basic {}", base64_encode(b"alice:wrong"));

    for spec in route_manifest() {
        if spec.status != RouteStatus::Mounted {
            continue;
        }
        let method = spec.methods[0];
        let is_ingest = spec.surface == Surface::Ingest;
        let mut req = if is_ingest {
            let mut r = manifest::Req::new("POST", spec.path);
            r.content_type = Some("application/x-protobuf");
            if spec.path == "/api/v1/write" {
                r.headers.push(("content-encoding", "snappy".to_string()));
            }
            r.body = match spec.path {
                "/v1/logs" => valid_otlp_logs_body(),
                "/v1/metrics" => valid_otlp_metrics_body(),
                "/v1/traces" => valid_otlp_traces_body(),
                "/api/v1/write" => valid_remote_write_body(),
                "/loki/api/v1/push" => valid_loki_push_body(),
                "/api/v2/spans" | "/tempo/spans" => valid_zipkin_body(),
                other => panic!("no valid-body builder for {other}"),
            };
            r
        } else {
            valid_request(spec, method)
        };

        // No credentials: `/ready`/`/metrics` (OpsPublic) never gate on
        // auth (exact documented success status, not just "not 401");
        // every other mounted route (including data-plane routes that are
        // otherwise unreachable under other spawns) is exactly `401` with
        // the pinned auth-failure envelope — the perimeter never leaks
        // path existence.
        let spawn = "spawn=all,auth=on,compat=on";
        let ctx = format!(
            "[{spawn}] {} {} case=auth creds=none",
            req.method, spec.path
        );
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        if spec.surface == Surface::OpsPublic {
            assert_eq!(
                res.status, spec.success_status,
                "{ctx}: ops-public must never require auth"
            );
            assert_success_envelope(spec, &res, &ctx);
            continue;
        }
        assert_401_unauthorized(&res, &ctx);

        let ctx = format!(
            "[{spawn}] {} {} case=auth creds=wrong",
            req.method, spec.path
        );
        req.headers.push(("authorization", invalid.clone()));
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        assert_401_unauthorized(&res, &ctx);

        let ctx = format!(
            "[{spawn}] {} {} case=auth creds=valid",
            req.method, spec.path
        );
        req.headers.pop();
        req.headers.push(("authorization", valid.clone()));
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        assert_eq!(
            res.status,
            spec.success_status,
            "{ctx}: valid credentials must reach the exact documented success status (body: {:?})",
            String::from_utf8_lossy(&res.body)
        );
        if is_ingest {
            assert_ingest_success_envelope(spec, &res, &ctx);
        } else {
            assert_success_envelope(spec, &res, &ctx);
        }
    }

    // Perimeter uniformity (edge case 2): an unauthenticated request to a
    // totally nonexistent path is indistinguishable from one to a real
    // (but not-yet-authenticated) path — both carry the exact same `401`
    // envelope, never a path-existence oracle via 404-before-401.
    let ctx = "[spawn=all,auth=on,compat=on] GET /totally-bogus-conformance-path case=auth \
               creds=none (perimeter uniformity)";
    let res = get(port, "/totally-bogus-conformance-path", ctx);
    assert_401_unauthorized(&res, ctx);
}

// ---------------------------------------------------------------------
// Spawn 3: mode=all, compat off — `/loki/*` 404s.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn all_mode_compat_off_alias_404() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_122;
    let db = "pulsus_api_conformance_it_compat_off";
    let _guard = spawn_ready(port, db, &[]); // PULSUS_COMPAT_ENDPOINTS unset => false.

    for spec in route_manifest() {
        if spec.status != RouteStatus::Mounted
            || !matches!(spec.gate, Gate::CompatAndReader | Gate::CompatAndWriter)
        {
            continue;
        }
        // The push receiver (issue #77) is POST-only: probe it with POST so
        // its 404 proves the route is genuinely absent (flag off), not a
        // method mismatch on a mounted route.
        if spec.surface == Surface::Ingest {
            let ctx = format!(
                "[spawn=all,auth=off,compat=off] POST {} case=compat-flag-off-404",
                spec.path
            );
            let mut req = manifest::Req::new("POST", spec.path);
            req.content_type = Some("application/x-protobuf");
            req.body = valid_loki_push_body();
            let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
            assert_404_empty(&res, &ctx);
            continue;
        }
        let ctx = format!(
            "[spawn=all,auth=off,compat=off] GET {} case=compat-flag-off-404",
            spec.path
        );
        let res = get(port, &resolve_path(spec.path), &ctx);
        assert_404_empty(&res, &ctx);
    }
}

// ---------------------------------------------------------------------
// Spawn 4: mode=writer, compat flag ON — every ReaderMode/CompatAndReader
// route still 404s (pins that the compat flag alone never mounts aliases
// without Reader mode); ops + ingest stay live.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn writer_only_mode_reader_routes_404() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_123;
    let db = "pulsus_api_conformance_it_writer_only";
    // Review round-3 finding (medium): `PULSUS_COMPAT_ENDPOINTS=true` here
    // too, not just mode=writer — pins the actual gating interaction
    // (verified by reading `compat.rs::apply_aliases`: it checks
    // `modes::mounted(cfg).contains(&Subsystem::Reader)` unconditionally,
    // *in addition to* the flag, so the alias never mounts writer-side
    // regardless of the flag) rather than only ever exercising the
    // flag-off case, which would miss a regression that mounted aliases
    // whenever the flag is true, independent of mode.
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_MODE", "writer"),
            ("PULSUS_COMPAT_ENDPOINTS", "true"),
        ],
    );

    for spec in route_manifest() {
        if spec.status != RouteStatus::Mounted {
            continue;
        }
        let spawn = "spawn=writer-only,compat=on";
        match spec.gate {
            Gate::ReaderMode | Gate::CompatAndReader => {
                let ctx = format!("[{spawn}] GET {} case=out-of-mode-404", spec.path);
                let res = get(port, &resolve_path(spec.path), &ctx);
                assert_404_empty(&res, &ctx);
            }
            Gate::Always => {
                let ctx = format!("[{spawn}] GET {} case=ops-stays-live", spec.path);
                let res = get(port, &resolve_path(spec.path), &ctx);
                assert_eq!(
                    res.status, spec.success_status,
                    "{ctx}: ops routes must stay live at their exact documented status in \
                     writer-only mode"
                );
                assert_success_envelope(spec, &res, &ctx);
            }
            Gate::WriterMode => assert_ingest_route(port, spec, spawn),
            // Issue #77: writer role + compat flag ON is exactly the
            // condition that mounts the Loki push receiver — the POSITIVE
            // assertion that distinguishes `CompatAndWriter` from
            // `CompatAndReader` (which stays 404 here, no Reader role).
            Gate::CompatAndWriter => assert_ingest_route(port, spec, spawn),
        }
    }
}

// ---------------------------------------------------------------------
// Spawn 5: mode=reader — every WriterMode (ingest) route 404s; ops +
// reader routes stay live.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reader_only_mode_writer_routes_404() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_124;
    let db = "pulsus_api_conformance_it_reader_only";
    let _guard = spawn_ready(port, db, &[("PULSUS_MODE", "reader")]);

    for spec in route_manifest() {
        if spec.status != RouteStatus::Mounted {
            continue;
        }
        let spawn = "spawn=reader-only";
        match spec.gate {
            Gate::WriterMode => {
                let ctx = format!("[{spawn}] POST {} case=out-of-mode-404", spec.path);
                let mut req = manifest::Req::new("POST", spec.path);
                req.content_type = Some("application/x-protobuf");
                if spec.path == "/api/v1/write" {
                    req.headers.push(("content-encoding", "snappy".to_string()));
                }
                req.body = match spec.path {
                    "/v1/logs" => valid_otlp_logs_body(),
                    "/v1/metrics" => valid_otlp_metrics_body(),
                    "/v1/traces" => valid_otlp_traces_body(),
                    "/api/v1/write" => valid_remote_write_body(),
                    other => panic!("no valid-body builder for {other}"),
                };
                let res =
                    raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
                assert_404_empty(&res, &ctx);
            }
            Gate::Always => {
                let ctx = format!("[{spawn}] GET {} case=ops-stays-live", spec.path);
                let res = get(port, &resolve_path(spec.path), &ctx);
                assert_eq!(
                    res.status, spec.success_status,
                    "{ctx}: ops routes must stay live at their exact documented status in \
                     reader-only mode"
                );
                assert_success_envelope(spec, &res, &ctx);
            }
            // `CompatAndReader` needs `PULSUS_COMPAT_ENDPOINTS=true` too
            // (unset here, matches `Config::default()`) — covered by
            // `all_mode_compat_off_alias_404`'s flag-only isolation and
            // `writer_only_mode_reader_routes_404`'s mode-only isolation;
            // skip here to avoid asserting two conflated preconditions in
            // one spawn.
            Gate::CompatAndReader => {}
            // Likewise `CompatAndWriter` (issue #77): compat is off in this
            // spawn AND the Writer role is absent, so both preconditions
            // fail at once — the isolated proofs live in
            // `reader_only_mode_compat_on_writer_compat_route_404` (flag on,
            // Writer role still absent) and spawn 3 (flag off).
            Gate::CompatAndWriter => {}
            Gate::ReaderMode => {
                let method = spec.methods[0];
                let ctx = format!(
                    "[{spawn}] {} {} case=in-mode-success",
                    method.as_str(),
                    spec.path
                );
                let req = valid_request(spec, method);
                let res =
                    raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
                assert_eq!(
                    res.status, spec.success_status,
                    "{ctx}: must reach its documented success status in reader-only mode"
                );
                assert_success_envelope(spec, &res, &ctx);
            }
        }
    }
}

// ---------------------------------------------------------------------
// Spawn 5b (issue #77 delta 3): mode=reader + compat flag ON — the Loki
// push receiver (`CompatAndWriter`) STILL 404s. The dedicated negative that
// isolates the writer-role requirement: unlike spawn 5 (flag off) and spawn
// 3 (flag off, all-mode), here the flag IS on, so a 404 proves the flag
// alone never mounts the writer-side push route without the Writer role —
// completing the "mounted iff (Writer role AND compat flag)" matrix
// alongside spawn 4's positive mount.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reader_only_mode_compat_on_writer_compat_route_404() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_128;
    let db = "pulsus_api_conformance_it_reader_compat_on";
    let _guard = spawn_ready(
        port,
        db,
        &[
            ("PULSUS_MODE", "reader"),
            ("PULSUS_COMPAT_ENDPOINTS", "true"),
        ],
    );

    let mut any = false;
    for spec in route_manifest() {
        if spec.status != RouteStatus::Mounted || spec.gate != Gate::CompatAndWriter {
            continue;
        }
        any = true;
        let ctx = format!(
            "[spawn=reader-only,compat=on] POST {} case=writer-role-absent-404",
            spec.path
        );
        let mut req = manifest::Req::new("POST", spec.path);
        req.content_type = Some("application/x-protobuf");
        req.body = valid_loki_push_body();
        let res = raw_request(port, &req).unwrap_or_else(|| panic!("{ctx}: must be reachable"));
        assert_404_empty(&res, &ctx);
    }
    assert!(
        any,
        "manifest must carry at least one Gate::CompatAndWriter route for this spawn to isolate"
    );
}

// ---------------------------------------------------------------------
// Spawn 6: PULSUS_LOGQL_SCAN_BUDGET_BYTES=1 — the live `422
// query_too_broad` case (code-review round-1 finding: `scan_budget_bytes`
// *is* configurable, unlike the hard-coded `DEFAULT_MAX_STREAMS` — see
// this module's doc comment for the full rationale).
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn logql_scan_budget_query_too_broad_live_case() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_125;
    let db = "pulsus_api_conformance_it_query_too_broad";
    let _guard = spawn_ready(port, db, &[("PULSUS_LOGQL_SCAN_BUDGET_BYTES", "1")]);

    // Minimal seed (one stream, one sample — mirrors `logs_api_live.rs`'s
    // own direct-`ChClient`-insert idiom, trimmed to the smallest amount
    // that still exercises a real ClickHouse read): any actual row read
    // exceeds the 1-byte budget above.
    let client = ChClient::new(ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port_num(),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    })
    .await
    .expect("connect data client");

    let now_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("now fits in i64 nanoseconds");

    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({now_ns}))), 1, \
                 'checkout', '{{\"service_name\":\"checkout\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, \
                 body) VALUES ('checkout', 1, {now_ns}, 0, 'hello')"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");

    let res = get(
        port,
        "/api/logs/v1/query_range?query=%7Bservice_name%3D%22checkout%22%7D",
        "[spawn=all,scan-budget=1B] GET /api/logs/v1/query_range case=query_too_broad-422",
    );
    assert_eq!(
        res.status,
        422,
        "[spawn=all,scan-budget=1B] GET /api/logs/v1/query_range case=query_too_broad-422: status (body: {:?})",
        String::from_utf8_lossy(&res.body)
    );
    assert_case_envelope(
        &res,
        &ExpectedError::Json {
            error_type: "query_too_broad",
            has_position: false,
        },
        "[spawn=all,scan-budget=1B] GET /api/logs/v1/query_range case=query_too_broad-422",
    );
}

// ---------------------------------------------------------------------
// Issue #74 (M6-11): tail slot exhaustion — with
// PULSUS_TAIL_MAX_CONNECTIONS=1, one held tail connection makes the next
// handshake a pre-upgrade `429 too_many_requests`, and releasing the
// slot (closing the first connection) restores a `101`.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tail_slot_exhaustion_returns_429_before_the_upgrade() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_127;
    let db = "pulsus_api_conformance_it_tail_slots";
    let _guard = spawn_ready(port, db, &[("PULSUS_TAIL_MAX_CONNECTIONS", "1")]);

    let target = "/api/logs/v1/tail?query=%7Bservice_name%3D%22checkout%22%7D";
    let spawn = "spawn=all,tail-slots=1";

    let ctx = format!("[{spawn}] GET /api/logs/v1/tail case=first-connection-101");
    let (res, held) = ws_attempt(port, target, &ctx);
    assert_eq!(res.status, 101, "{ctx}: status");
    let held = held.unwrap_or_else(|| panic!("{ctx}: upgraded stream"));

    let ctx = format!("[{spawn}] GET /api/logs/v1/tail case=slot-exhausted-429");
    let (res, stream) = ws_attempt(port, target, &ctx);
    assert!(stream.is_none(), "{ctx}: must not upgrade");
    assert_eq!(
        res.status,
        429,
        "{ctx}: status (body: {:?})",
        String::from_utf8_lossy(&res.body)
    );
    assert_case_envelope(
        &res,
        &ExpectedError::Json {
            error_type: "too_many_requests",
            has_position: false,
        },
        &ctx,
    );

    // Releasing the held connection frees its owned permit: the next
    // handshake upgrades again (bounded retry — permit release runs
    // asynchronously after the socket drops).
    drop(held);
    let ctx = format!("[{spawn}] GET /api/logs/v1/tail case=slot-released-101");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (res, stream) = ws_attempt(port, target, &ctx);
        if res.status == 101 {
            drop(stream);
            break;
        }
        assert_eq!(
            res.status, 429,
            "{ctx}: only 429 is expected while the slot drains"
        );
        assert!(
            Instant::now() < deadline,
            "{ctx}: the slot must be released once the connection closes"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

// ---------------------------------------------------------------------
// Issue #86 (M6-08d, plan v2 Δ5): the `resultType:"string"` matrix-audit
// row — a top-level PromQL string-literal query on the LIVE /api/v1/query
// route renders the Prometheus string envelope byte-exactly (the request's
// own `time` param stamped as the result timestamp). Zero seed data
// needed: a string query plans with no selectors and touches no tables.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn prom_query_string_literal_renders_result_type_string_live_case() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_126;
    let db = "pulsus_api_conformance_it_string_result";
    let _guard = spawn_ready(port, db, &[]);

    let ctx = "[spawn=all] GET /api/v1/query case=string-literal-200";
    let res = get(port, "/api/v1/query?query=%22conformance%22&time=123", ctx);
    assert_eq!(
        res.status,
        200,
        "{ctx}: status (body: {:?})",
        String::from_utf8_lossy(&res.body)
    );
    assert!(
        res.content_type()
            .is_some_and(|ct| ct.starts_with("application/json")),
        "{ctx}: content-type, got {:?}",
        res.content_type()
    );
    assert_eq!(
        String::from_utf8_lossy(&res.body),
        r#"{"status":"success","data":{"resultType":"string","result":[123,"conformance"]}}"#,
        "{ctx}: byte-exact string envelope"
    );
}
