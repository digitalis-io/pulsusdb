//! Issue #173 (M7-E1) AC9: the end-to-end service-graph HTTP gate — OTLP
//! ingest through the *product* path (`POST /v1/traces`, sync) → the edge
//! MV populates `trace_edges` → `GET /api/traces/v1/service_graph` returns
//! the exact documented JSON edges. This is the full-path gate the no-split
//! rationale rests on (the read-path SQL/pruning is separately gated in
//! `pulsus-read`'s `traces_graph_explain.rs`).
//!
//! Proves, over HTTP against a real spawned `pulsusdb` + ClickHouse 24.8:
//! - client fan-out: one CLIENT parenting two SERVER children in different
//!   services yields two edges, `calls=1` each (fails the collapsed-key
//!   design by construction);
//! - a PRODUCER→CONSUMER pair yields a `messaging` edge;
//! - a cross-kind CLIENT→CONSUMER decoy yields no edge;
//! - an error-status pair reports `failed=1`;
//! - replay idempotence: re-POSTing the byte-identical body leaves the
//!   response unchanged;
//! - merge invariance: the edge set/counts are unchanged after
//!   `OPTIMIZE TABLE trace_edges FINAL`;
//! - Zipkin shared-span correctness (AC7f): a shared RPC (both sides under
//!   the same span id, carrying `zipkin.shared="true"`) produces exactly its
//!   `client → server` edge, and a shared server half whose `parent_id`
//!   coincidentally collides with a real client span produces NO false edge.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Ports 31140-31141 (distinct from
//! every other live suite's fixed ports). Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test traces_graph_live
//! podman rm -f pulsus-ch-test
//! ```

use std::collections::{BTreeMap, HashMap};
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
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

// -- bare-TcpStream HTTP helper (the traces_search_live.rs idiom) --------

struct RawResponse {
    status: u16,
    body: Vec<u8>,
}

impl RawResponse {
    fn json(&self, ctx: &str) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "{ctx}: invalid JSON body: {e}\nbody: {:?}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
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
    body: Option<(&str, &[u8])>,
) -> Option<RawResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();

    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
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
    let headers: HashMap<String, String> = lines
        .filter_map(|line| {
            let (k, v) = line.split_once(':')?;
            Some((k.trim().to_ascii_lowercase(), v.trim().to_string()))
        })
        .collect();

    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    Some(RawResponse { status, body })
}

fn get(port: u16, path: &str, ctx: &str) -> RawResponse {
    request(port, "GET", path, None)
        .unwrap_or_else(|| panic!("{ctx}: request must be reachable (transport failure)"))
}

// -- process lifecycle + throwaway database -----------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn ch_config(db: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

async fn drop_db(db: &str) {
    let client = ChClient::new(ch_config("default"))
        .await
        .expect("connect for drop");
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

async fn optimize_edges_final(db: &str) {
    let client = ChClient::new(ch_config(db))
        .await
        .expect("connect for optimize");
    client
        .execute(
            &format!("OPTIMIZE TABLE {db}.trace_edges FINAL"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("optimize trace_edges");
}

fn spawn_ready(port: u16, db: &str) -> ChildGuard {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    cmd.env("PULSUS_HOST", "127.0.0.1")
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
        // Mount the Zipkin v2 receiver (`/api/v2/spans`, CompatAndWriter) so
        // AC7(f) can drive the genuine Zipkin ingest → conversion → shared
        // path. Native routes stay mounted regardless.
        .env("PULSUS_COMPAT_ENDPOINTS", "true");
    let guard = ChildGuard(cmd.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if request(port, "GET", "/ready", None).is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s");
}

// -- OTLP seeding through the product ingest path -----------------------

fn kv_str(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

/// One span with an explicit `kind` and optional error status.
fn span(
    trace: [u8; 16],
    span_id: [u8; 8],
    parent: [u8; 8],
    kind: SpanKind,
    error: bool,
    attrs: Vec<KeyValue>,
    start_ns: u64,
) -> Span {
    Span {
        trace_id: trace.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: parent.to_vec(),
        name: "op".to_string(),
        kind: kind as i32,
        start_time_unix_nano: start_ns,
        end_time_unix_nano: start_ns + 5_000_000,
        attributes: attrs,
        status: error.then(|| Status {
            code: StatusCode::Error as i32,
            message: String::new(),
        }),
        ..Default::default()
    }
}

/// Seeds `spans` under one resource `service.name = service` through
/// `POST /v1/traces` (sync — a `200` means the rows are flushed and
/// read-visible, and the edge MV has fired). Returns the encoded body so a
/// caller can replay the byte-identical request.
fn ingest(port: u16, service: &str, spans: Vec<Span>, ctx: &str) -> Vec<u8> {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv_str("service.name", service)],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "graph-live".to_string(),
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
    let body = req.encode_to_vec();
    let res = request(
        port,
        "POST",
        "/v1/traces",
        Some(("application/x-protobuf", &body)),
    )
    .unwrap_or_else(|| panic!("{ctx}: ingest must be reachable"));
    assert_eq!(
        res.status,
        200,
        "{ctx}: sync ingest must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    body
}

/// Seeds a Zipkin v2 JSON span array through the REAL mounted Zipkin
/// receiver (`POST /api/v2/spans`, CompatAndWriter — sync, `202` on
/// success), exercising the genuine Zipkin decode → OTLP conversion →
/// `shared`-column → edge path (issue #173 AC7f). Returns nothing; the
/// caller reads the resulting edges back through the service-graph API.
fn zipkin_ingest(port: u16, json: &str, ctx: &str) {
    let res = request(
        port,
        "POST",
        "/api/v2/spans",
        Some(("application/json", json.as_bytes())),
    )
    .unwrap_or_else(|| panic!("{ctx}: zipkin ingest must be reachable"));
    assert_eq!(
        res.status,
        202,
        "{ctx}: zipkin ingest must succeed (202), body {:?}",
        String::from_utf8_lossy(&res.body)
    );
}

fn replay(port: u16, body: &[u8], ctx: &str) {
    let res = request(
        port,
        "POST",
        "/v1/traces",
        Some(("application/x-protobuf", body)),
    )
    .unwrap_or_else(|| panic!("{ctx}: replay must be reachable"));
    assert_eq!(res.status, 200, "{ctx}: replay ingest must succeed");
}

// -- graph read -------------------------------------------------------

/// `(client, server, connectionType) -> (calls, failed)`.
type EdgeMap = BTreeMap<(String, String, String), (u64, u64)>;

fn read_graph(port: u16, start_s: i64, end_s: i64, ctx: &str) -> (EdgeMap, bool) {
    let path = format!("/api/traces/v1/service_graph?start={start_s}&end={end_s}");
    let res = get(port, &path, ctx);
    assert_eq!(
        res.status,
        200,
        "{ctx}: service_graph must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    let json = res.json(ctx);
    let truncated = json["truncated"].as_bool().expect("truncated bool");
    let mut edges = EdgeMap::new();
    for e in json["edges"].as_array().expect("edges array") {
        let key = (
            e["client"].as_str().expect("client").to_string(),
            e["server"].as_str().expect("server").to_string(),
            e["connectionType"]
                .as_str()
                .expect("connectionType")
                .to_string(),
        );
        let calls = e["calls"].as_u64().expect("calls");
        let failed = e["failed"].as_u64().expect("failed");
        // Every edge carries three finite ns quantiles (f64, no f32).
        for q in ["p50Ns", "p95Ns", "p99Ns"] {
            assert!(
                e[q].as_f64().is_some_and(f64::is_finite),
                "{ctx}: {q} must be a finite f64"
            );
        }
        edges.insert(key, (calls, failed));
    }
    (edges, truncated)
}

fn tid(n: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[15] = n;
    id
}
fn sid(n: u8) -> [u8; 8] {
    let mut id = [0u8; 8];
    id[7] = n;
    id
}
const ROOT: [u8; 8] = [0u8; 8];

#[tokio::test(flavor = "multi_thread")]
async fn service_graph_end_to_end_over_http() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_140;
    let db = "pulsus_traces_graph_live_it";
    drop_db(db).await;
    let _guard = spawn_ready(port, db);
    let ctx = "graph-live";

    // A RECENT timestamp: the trace tables carry a 7-day delete-TTL with
    // `ttl_only_drop_parts = 1`, so a fixed historical timestamp lands in an
    // already-expired part and is dropped right after insert (the live-ingest
    // TTL hazard the sibling suites document). Snap to a whole second.
    let base_s = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs(),
    )
    .expect("fits i64");
    let base_ns: u64 = (base_s as u64) * 1_000_000_000;

    // Fan-out: web (CLIENT, span 1) parents two SERVER children in different
    // services — auth (span 10) and cart (span 11). Two edges, calls=1 each.
    let web_client_body = ingest(
        port,
        "web",
        vec![
            span(
                tid(1),
                sid(1),
                ROOT,
                SpanKind::Client,
                false,
                vec![],
                base_ns,
            ),
            // A second CLIENT span under `web` for the error edge below.
            span(
                tid(4),
                sid(4),
                ROOT,
                SpanKind::Client,
                false,
                vec![],
                base_ns,
            ),
        ],
        ctx,
    );
    let auth_body = ingest(
        port,
        "auth",
        vec![span(
            tid(1),
            sid(10),
            sid(1),
            SpanKind::Server,
            false,
            vec![],
            base_ns,
        )],
        ctx,
    );
    ingest(
        port,
        "cart",
        vec![span(
            tid(1),
            sid(11),
            sid(1),
            SpanKind::Server,
            false,
            vec![],
            base_ns,
        )],
        ctx,
    );
    // Error edge: web -> ledger with an error-status SERVER span.
    ingest(
        port,
        "ledger",
        vec![span(
            tid(4),
            sid(12),
            sid(4),
            SpanKind::Server,
            true,
            vec![],
            base_ns,
        )],
        ctx,
    );
    // Messaging: orders (PRODUCER, span 2) -> shipping (CONSUMER, span 20).
    ingest(
        port,
        "orders",
        vec![span(
            tid(2),
            sid(2),
            ROOT,
            SpanKind::Producer,
            false,
            vec![],
            base_ns,
        )],
        ctx,
    );
    ingest(
        port,
        "shipping",
        vec![span(
            tid(2),
            sid(20),
            sid(2),
            SpanKind::Consumer,
            false,
            vec![],
            base_ns,
        )],
        ctx,
    );
    // Cross-kind decoy: xsvc (CLIENT) -> ysvc (CONSUMER): conn_type mismatch
    // (rpc vs messaging), so it must NOT pair.
    ingest(
        port,
        "xsvc",
        vec![span(
            tid(3),
            sid(3),
            ROOT,
            SpanKind::Client,
            false,
            vec![],
            base_ns,
        )],
        ctx,
    );
    ingest(
        port,
        "ysvc",
        vec![span(
            tid(3),
            sid(30),
            sid(3),
            SpanKind::Consumer,
            false,
            vec![],
            base_ns,
        )],
        ctx,
    );

    let (edges, truncated) = read_graph(port, base_s - 3600, base_s + 3600, ctx);
    assert!(!truncated, "the tiny fixture is never truncated");
    let expected: EdgeMap = [
        (("web".into(), "auth".into(), "rpc".into()), (1, 0)),
        (("web".into(), "cart".into(), "rpc".into()), (1, 0)),
        (("web".into(), "ledger".into(), "rpc".into()), (1, 1)),
        (
            ("orders".into(), "shipping".into(), "messaging".into()),
            (1, 0),
        ),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        edges, expected,
        "fan-out yields two web edges (calls=1 each); the error edge reports failed=1; the \
         messaging pair yields a messaging edge; the cross-kind decoy yields none"
    );

    // Replay idempotence: re-POST byte-identical bodies → response unchanged.
    replay(port, &web_client_body, ctx);
    replay(port, &auth_body, ctx);
    let (after_replay, _) = read_graph(port, base_s - 3600, base_s + 3600, ctx);
    assert_eq!(
        after_replay, expected,
        "a byte-identical replay must not change the graph"
    );

    // Merge invariance: unchanged after OPTIMIZE ... FINAL.
    optimize_edges_final(db).await;
    let (after_final, _) = read_graph(port, base_s - 3600, base_s + 3600, ctx);
    assert_eq!(
        after_final, expected,
        "the edge set + counts must be unchanged after OPTIMIZE ... FINAL"
    );

    // Zipkin shared span (AC7f): both RPC sides under the SAME span id
    // (span 40), the server side carrying zipkin.shared="true". The shared
    // server half keys by its own span id, so it pairs with — and only with —
    // its same-id client twin → edge front -> back.
    ingest(
        port,
        "front",
        vec![span(
            tid(5),
            sid(40),
            ROOT,
            SpanKind::Client,
            false,
            vec![],
            base_ns,
        )],
        ctx,
    );
    ingest(
        port,
        "back",
        vec![span(
            tid(5),
            sid(40),
            ROOT,
            SpanKind::Server,
            false,
            vec![kv_str("zipkin.shared", "true")],
            base_ns,
        )],
        ctx,
    );
    // No-false-edge: a shared SERVER half (span 41) whose parent_id (span 40)
    // coincidentally collides with the real `front` client span. Because it is
    // shared, it keys by its OWN id (41), which no client carries → NO edge
    // (a non-shared half here would falsely pair front -> ghost).
    ingest(
        port,
        "ghost",
        vec![span(
            tid(5),
            sid(41),
            sid(40),
            SpanKind::Server,
            false,
            vec![kv_str("zipkin.shared", "true")],
            base_ns,
        )],
        ctx,
    );

    let (shared_edges, _) = read_graph(port, base_s - 3600, base_s + 3600, ctx);
    assert_eq!(
        shared_edges
            .get(&("front".into(), "back".into(), "rpc".into()))
            .copied(),
        Some((1, 0)),
        "the shared RPC must produce exactly its front -> back edge, body {shared_edges:?}"
    );
    assert!(
        !shared_edges
            .keys()
            .any(|(c, s, _)| c == "front" && s == "ghost"),
        "a shared server half must never fabricate an edge from a coincidental parent_id \
         collision, body {shared_edges:?}"
    );

    // AC7(f): the GENUINE Zipkin-receiver path. A real Zipkin v2 shared RPC —
    // CLIENT and SERVER spans stored under the SAME `id` (the Zipkin shared
    // model), the server carrying `"shared": true` exactly as a real Zipkin
    // client emits it — POSTed to the mounted `/api/v2/spans` receiver, which
    // decodes → converts to OTLP (emitting `zipkin.shared="true"`) → the
    // `shared` column → the edge MV. Distinct trace + service names from the
    // OTLP scenarios above so the assertion is unambiguous.
    let base_micros = base_s * 1_000_000;
    let zipkin_body = format!(
        "[{{\"traceId\":\"00000000000000a6\",\"id\":\"0000000000000060\",\
           \"parentId\":\"0000000000000070\",\"kind\":\"CLIENT\",\
           \"timestamp\":{base_micros},\"duration\":5000,\
           \"localEndpoint\":{{\"serviceName\":\"front-z\"}}}},\
          {{\"traceId\":\"00000000000000a6\",\"id\":\"0000000000000060\",\
           \"parentId\":\"0000000000000070\",\"kind\":\"SERVER\",\"shared\":true,\
           \"timestamp\":{base_micros},\"duration\":5000,\
           \"localEndpoint\":{{\"serviceName\":\"back-z\"}}}}]"
    );
    zipkin_ingest(port, &zipkin_body, ctx);

    let (zipkin_edges, _) = read_graph(port, base_s - 3600, base_s + 3600, ctx);
    assert_eq!(
        zipkin_edges
            .get(&("front-z".into(), "back-z".into(), "rpc".into()))
            .copied(),
        Some((1, 0)),
        "the Zipkin-receiver shared RPC must promote through conversion → shared column → \
         edge, yielding exactly front-z -> back-z, body {zipkin_edges:?}"
    );

    drop_db(db).await;
}
