//! Issue #57 AC3/AC5 live end-to-end tests for `GET /api/traces/v1/search`:
//! spawns the real `pulsusdb` binary against a live ClickHouse (same
//! harness as `traces_api_live.rs`), seeds a deterministic corpus through
//! the *product* ingest path (`POST /v1/traces`, sync), then asserts the
//! exact returned trace-ID **sets** for the pre-committed `q=` cases —
//! string attrs, `val_num` ranges, cross-spanset `&&`/`||`, repeated-key
//! conjunctions, unscoped dual-scope resolution, the ratified `!=`/`!~`
//! negation semantics, pipeline aggregates, `select()` projection,
//! functional `spss`, the legacy-params equivalence, trace-wide root
//! hydration, and the public ordering contract (max matched-span
//! timestamp DESC, trace id ASC — deterministic under ties). Two more
//! spawns pin AC5's cap/partial contract (`PULSUS_TRACEQL_MAX_CANDIDATES`)
//! and the scan-budget `422` (`PULSUS_TRACEQL_SCAN_BUDGET_ROWS`).
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test traces_search_live
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Ports 31131-31133 — distinct from every other live suite's fixed
//! ports (31100-31125, 31130).

use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use prost::Message;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// Bare-TcpStream HTTP helper (the traces_api_live.rs idiom, with the
// port as a parameter — this suite runs three spawns).
// ---------------------------------------------------------------------

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

// ---------------------------------------------------------------------
// Process lifecycle + throwaway database.
// ---------------------------------------------------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn drop_db(db: &str) {
    let cfg = ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    };
    let client = ChClient::new(cfg).await.expect("connect for drop");
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

fn spawn_ready(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
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
        .env("CLICKHOUSE_DB", db);
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
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

// ---------------------------------------------------------------------
// OTLP seeding through the product ingest path.
// ---------------------------------------------------------------------

fn kv_str(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn kv_bool(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::BoolValue(value)),
        }),
        key_strindex: 0,
    }
}

fn kv_int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
        key_strindex: 0,
    }
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[allow(clippy::too_many_arguments)]
fn span(
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_id: Option<[u8; 8]>,
    name: &str,
    start_ns: u64,
    duration_ns: u64,
    attrs: Vec<KeyValue>,
) -> Span {
    Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: parent_id.map(|p| p.to_vec()).unwrap_or_default(),
        name: name.to_string(),
        start_time_unix_nano: start_ns,
        end_time_unix_nano: start_ns + duration_ns,
        attributes: attrs,
        ..Default::default()
    }
}

/// Seeds `spans` through `POST /v1/traces` (sync — a `200` means the
/// rows are flushed and read-visible) with the given resource attrs
/// (always including `service.name=checkout` unless overridden).
fn ingest(port: u16, spans: Vec<Span>, resource_attrs: Vec<KeyValue>, ctx: &str) {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: resource_attrs,
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

fn checkout_resource() -> Vec<KeyValue> {
    vec![kv_str("service.name", "checkout")]
}

// ---------------------------------------------------------------------
// Search-side helpers.
// ---------------------------------------------------------------------

/// Minimal percent-encoding for `q=` values.
fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn search(port: u16, q: &str, start_s: i64, end_s: i64, extra: &str, ctx: &str) -> RawResponse {
    let path = format!(
        "/api/traces/v1/search?q={}&start={start_s}&end={end_s}{extra}",
        enc(q)
    );
    let res = get(port, &path, ctx);
    assert_eq!(
        res.status,
        200,
        "{ctx}: search must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    res
}

/// The exact returned trace-ID set.
fn trace_set(json: &serde_json::Value) -> BTreeSet<String> {
    json["traces"]
        .as_array()
        .expect("traces array")
        .iter()
        .map(|t| t["traceID"].as_str().expect("traceID").to_string())
        .collect()
}

/// The returned trace IDs in response order (the public ordering
/// contract).
fn trace_order(json: &serde_json::Value) -> Vec<String> {
    json["traces"]
        .as_array()
        .expect("traces array")
        .iter()
        .map(|t| t["traceID"].as_str().expect("traceID").to_string())
        .collect()
}

fn ids(ns: &[u8]) -> BTreeSet<String> {
    ns.iter().map(|n| hex(&tid(*n))).collect()
}

fn assert_set(port: u16, q: &str, start_s: i64, end_s: i64, expected: &[u8], ctx: &str) {
    let res = search(port, q, start_s, end_s, "", ctx);
    let json = res.json(ctx);
    assert_eq!(
        trace_set(&json),
        ids(expected),
        "{ctx}: exact trace-ID set for {q}\nbody: {json}"
    );
    assert_eq!(json["metrics"]["partial"], false, "{ctx}: not partial");
}

fn now_s() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs(),
    )
    .expect("fits i64")
}

/// Nanoseconds for "BASE + off seconds", offset half a second so window
/// boundaries in whole seconds never sit exactly on a span timestamp.
fn ts(base_s: i64, off_s: i64) -> u64 {
    ((base_s + off_s) as u64) * 1_000_000_000 + 500_000_000
}

const MS: u64 = 1_000_000;

// ---------------------------------------------------------------------
// Spawn A: full semantics (AC3).
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn search_semantics_against_real_clickhouse() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_131;
    let db = "pulsus_traces_search_it_a";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[]);

    let base = now_s() - 3_600;
    let (w0, w1) = (base, base + 600);

    // -- Seed corpus (one ingest per trace; sync => read-visible). ------
    ingest(
        port,
        vec![span(
            tid(1),
            sid(1),
            None,
            "a-prod",
            ts(base, 10),
            50 * MS,
            vec![kv_str("env", "prod")],
        )],
        checkout_resource(),
        "seed T1",
    );
    ingest(
        port,
        vec![span(
            tid(2),
            sid(1),
            None,
            "a-staging",
            ts(base, 11),
            50 * MS,
            vec![kv_str("env", "staging")],
        )],
        checkout_resource(),
        "seed T2",
    );
    ingest(
        port,
        vec![span(
            tid(3),
            sid(1),
            None,
            "a-absent",
            ts(base, 12),
            50 * MS,
            vec![],
        )],
        checkout_resource(),
        "seed T3",
    );
    ingest(
        port,
        vec![span(
            tid(4),
            sid(1),
            None,
            "slow",
            ts(base, 13),
            3_000 * MS,
            vec![kv_int("http.status_code", 500)],
        )],
        checkout_resource(),
        "seed T4",
    );
    ingest(
        port,
        vec![span(
            tid(5),
            sid(1),
            None,
            "fast",
            ts(base, 14),
            10 * MS,
            vec![kv_int("http.status_code", 200)],
        )],
        checkout_resource(),
        "seed T5",
    );
    ingest(
        port,
        vec![
            span(
                tid(6),
                sid(1),
                None,
                "c1",
                ts(base, 15),
                10 * MS,
                vec![kv_str("a", "1")],
            ),
            span(
                tid(6),
                sid(2),
                None,
                "c2",
                ts(base, 16),
                10 * MS,
                vec![kv_str("b", "2")],
            ),
        ],
        checkout_resource(),
        "seed T6",
    );
    ingest(
        port,
        vec![span(
            tid(7),
            sid(1),
            None,
            "c1",
            ts(base, 17),
            10 * MS,
            vec![kv_str("a", "1")],
        )],
        checkout_resource(),
        "seed T7",
    );
    ingest(
        port,
        vec![span(
            tid(8),
            sid(1),
            None,
            "c2",
            ts(base, 18),
            10 * MS,
            vec![kv_str("b", "2")],
        )],
        checkout_resource(),
        "seed T8",
    );
    ingest(
        port,
        vec![
            span(tid(9), sid(1), None, "hot", ts(base, 20), 200 * MS, vec![]),
            span(tid(9), sid(2), None, "hot", ts(base, 21), 200 * MS, vec![]),
            span(tid(9), sid(3), None, "hot", ts(base, 22), 200 * MS, vec![]),
        ],
        checkout_resource(),
        "seed T9",
    );
    ingest(
        port,
        vec![span(
            tid(10),
            sid(1),
            None,
            "hot",
            ts(base, 23),
            10 * MS,
            vec![],
        )],
        checkout_resource(),
        "seed T10",
    );
    ingest(
        port,
        vec![
            span(
                tid(11),
                sid(1),
                None,
                "agg",
                ts(base, 24),
                10 * MS,
                vec![kv_int("retries", 3)],
            ),
            span(
                tid(11),
                sid(2),
                None,
                "agg",
                ts(base, 24),
                10 * MS,
                vec![kv_int("retries", 1)],
            ),
        ],
        checkout_resource(),
        "seed T11",
    );
    ingest(
        port,
        vec![span(
            tid(12),
            sid(1),
            None,
            "agg",
            ts(base, 25),
            10 * MS,
            vec![kv_int("retries", 0)],
        )],
        checkout_resource(),
        "seed T12",
    );
    ingest(
        port,
        vec![span(
            tid(13),
            sid(1),
            None,
            "sel",
            ts(base, 26),
            10 * MS,
            vec![kv_str("foo", "x")],
        )],
        checkout_resource(),
        "seed T13",
    );
    ingest(
        port,
        vec![span(
            tid(14),
            sid(1),
            None,
            "res-env",
            ts(base, 27),
            10 * MS,
            vec![],
        )],
        vec![kv_str("service.name", "checkout"), kv_str("env", "prod")],
        "seed T14",
    );
    ingest(
        port,
        (1..=5)
            .map(|n| {
                span(
                    tid(15),
                    sid(n),
                    None,
                    "many",
                    ts(base, 29 + n as i64),
                    10 * MS,
                    vec![],
                )
            })
            .collect(),
        checkout_resource(),
        "seed T15",
    );
    // T16: the actual root predates the search window; an in-window
    // child matches.
    ingest(
        port,
        vec![
            span(
                tid(16),
                sid(1),
                None,
                "root-op",
                ts(base, -300),
                400_000 * MS,
                vec![],
            ),
            span(
                tid(16),
                sid(2),
                Some(sid(1)),
                "child",
                ts(base, 35),
                10 * MS,
                vec![],
            ),
        ],
        checkout_resource(),
        "seed T16",
    );
    // T17/T18: identical matched-span timestamps — the tiebreak fixture.
    ingest(
        port,
        vec![span(
            tid(17),
            sid(1),
            None,
            "tie",
            ts(base, 40),
            10 * MS,
            vec![],
        )],
        checkout_resource(),
        "seed T17",
    );
    ingest(
        port,
        vec![span(
            tid(18),
            sid(1),
            None,
            "tie",
            ts(base, 40),
            10 * MS,
            vec![],
        )],
        checkout_resource(),
        "seed T18",
    );

    // -- (a) string attr, unscoped: both scopes match. -------------------
    assert_set(port, r#"{ .env = "prod" }"#, w0, w1, &[1, 14], "case a");
    // scoped forms split by scope (adjudication 5 + §4.1 scope column).
    assert_set(
        port,
        r#"{ span.env = "prod" }"#,
        w0,
        w1,
        &[1],
        "case a-span",
    );
    assert_set(
        port,
        r#"{ resource.env = "prod" }"#,
        w0,
        w1,
        &[14],
        "case a-resource",
    );

    // -- (b) val_num range. ----------------------------------------------
    assert_set(
        port,
        "{ span.http.status_code >= 500 }",
        w0,
        w1,
        &[4],
        "case b",
    );

    // -- (c) cross-spanset && = trace-level intersection. -----------------
    assert_set(
        port,
        r#"{ span.a = "1" } && { span.b = "2" }"#,
        w0,
        w1,
        &[6],
        "case c",
    );
    // Cross-spanset || = union.
    assert_set(
        port,
        r#"{ span.a = "1" } || { span.b = "2" }"#,
        w0,
        w1,
        &[6, 7, 8],
        "case c-or",
    );

    // -- (d) mixed-table OR strictly larger than the same operands' &&. --
    assert_set(
        port,
        r#"{ duration > 2s || span.foo = "x" }"#,
        w0,
        w1,
        &[4, 13],
        "case d-or",
    );
    assert_set(
        port,
        r#"{ duration > 2s && span.foo = "x" }"#,
        w0,
        w1,
        &[],
        "case d-and",
    );

    // -- (e) repeated-key conjunction: two independent probes. ------------
    assert_set(
        port,
        r#"{ span.a = "1" }"#,
        w0,
        w1,
        &[6, 7],
        "case e-single",
    );
    assert_set(
        port,
        r#"{ span.a = "1" && span.a = "2" }"#,
        w0,
        w1,
        &[],
        "case e-both",
    );

    // -- (g) negation: absent + different match, equal does not (narrow
    // window isolating T1/T2/T3). ----------------------------------------
    assert_set(
        port,
        r#"{ .env != "prod" }"#,
        base + 9,
        base + 13,
        &[2, 3],
        "case g-neq",
    );
    assert_set(
        port,
        r#"{ .env !~ "pro.*" }"#,
        base + 9,
        base + 13,
        &[2, 3],
        "case g-nre",
    );
    // Dual-scope: a resource-scoped env=prod also blocks the unscoped
    // negation (narrow window isolating T14).
    assert_set(
        port,
        r#"{ .env != "prod" }"#,
        base + 27,
        base + 28,
        &[],
        "case g-dual-scope",
    );

    // -- (h) pipeline aggregates. -----------------------------------------
    assert_set(
        port,
        r#"{ name = "hot" } | count() > 1"#,
        w0,
        w1,
        &[9],
        "case h-count",
    );
    assert_set(
        port,
        r#"{ name = "hot" } | avg(duration) > 100ms"#,
        w0,
        w1,
        &[9],
        "case h-avg-duration",
    );
    assert_set(
        port,
        r#"{ name = "agg" } | avg(span.retries) > 1"#,
        w0,
        w1,
        &[11],
        "case h-avg-attr",
    );

    // -- (j) cross-spanset followed by a pipeline: membership is the
    // union of the operands' matched spans. -------------------------------
    assert_set(
        port,
        r#"{ span.a = "1" } && { span.b = "2" } | count() > 1"#,
        w0,
        w1,
        &[6],
        "case j",
    );
    let res = search(
        port,
        r#"{ span.a = "1" } && { span.b = "2" }"#,
        w0,
        w1,
        "",
        "case j membership",
    );
    let json = res.json("case j membership");
    assert_eq!(
        json["traces"][0]["spanSets"][0]["matched"], 2,
        "the composed spanset carries BOTH operands' matched spans, body {json}"
    );

    // -- (i) select() projects the field into the response spanset. -------
    let res = search(
        port,
        r#"{ span.foo = "x" } | select(span.foo)"#,
        w0,
        w1,
        "",
        "case i",
    );
    let json = res.json("case i");
    assert_eq!(trace_set(&json), ids(&[13]), "case i set");
    assert_eq!(
        json["traces"][0]["spanSets"][0]["spans"][0]["attributes"][0],
        serde_json::json!({"key": "span.foo", "value": {"stringValue": "x"}}),
        "case i: selected field present, body {json}"
    );

    // -- (k) functional spss: summaries capped, matched reports total. ----
    let res = search(port, r#"{ name = "many" }"#, w0, w1, "&spss=2", "case k");
    let json = res.json("case k");
    assert_eq!(trace_set(&json), ids(&[15]), "case k set");
    let set = &json["traces"][0]["spanSets"][0];
    assert_eq!(set["matched"], 5, "case k matched total, body {json}");
    assert_eq!(
        set["spans"].as_array().expect("spans").len(),
        2,
        "case k spss cap, body {json}"
    );

    // -- (k2) issue #193: `| by(span.retries)` regroups a trace's matched
    // spans into ONE spanSet per distinct key value, each carrying the
    // typed group `attributes`. Trace 11's two "agg" spans differ in
    // span.retries (3, 1) → two spanSets; the flat matched/spans view is
    // replaced by the grouped array. -------------------------------------
    let res = search(
        port,
        r#"{ name = "agg" } | by(span.retries)"#,
        w0,
        w1,
        "",
        "case k2 by",
    );
    let json = res.json("case k2 by");
    let t11 = json["traces"]
        .as_array()
        .expect("traces")
        .iter()
        .find(|t| t["traceID"] == hex(&tid(11)))
        .expect("trace 11 in the by() response");
    let sets = t11["spanSets"].as_array().expect("spanSets array");
    assert_eq!(
        sets.len(),
        2,
        "one spanSet per distinct span.retries, body {json}"
    );
    let mut group_values: Vec<f64> = sets
        .iter()
        .map(|s| {
            assert_eq!(s["matched"], 1, "each retries group has one matched span");
            s["attributes"][0]["value"]["doubleValue"]
                .as_f64()
                .unwrap_or_else(|| panic!("group attr doubleValue, body {json}"))
        })
        .collect();
    group_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(group_values, vec![1.0, 3.0], "the two group key values");
    for set in sets {
        assert_eq!(
            set["attributes"][0]["key"], "by(span.retries)",
            "group attribute carries Tempo's by(<expr>) key spelling"
        );
    }

    // -- (k3) issue #193: `| by(span.retries) | coalesce()` collapses the
    // groups back into the flat single spanSet (no per-spanSet
    // attributes), in pipeline order. -----------------------------------
    let res = search(
        port,
        r#"{ name = "agg" } | by(span.retries) | coalesce()"#,
        w0,
        w1,
        "",
        "case k3 coalesce",
    );
    let json = res.json("case k3 coalesce");
    let t11 = json["traces"]
        .as_array()
        .expect("traces")
        .iter()
        .find(|t| t["traceID"] == hex(&tid(11)))
        .expect("trace 11 in the coalesce response");
    let sets = t11["spanSets"].as_array().expect("spanSets array");
    assert_eq!(
        sets.len(),
        1,
        "coalesce() collapses to one spanSet, body {json}"
    );
    assert_eq!(
        sets[0]["matched"], 2,
        "the collapsed spanSet unions both spans"
    );
    assert!(
        sets[0].get("attributes").is_none(),
        "the collapsed flat spanSet carries no group attributes, body {json}"
    );

    // -- (l) legacy params equal their q= equivalent. ----------------------
    let legacy_path = format!(
        "/api/traces/v1/search?tags={}&minDuration=1ms&start={w0}&end={w1}",
        enc("env=prod")
    );
    let legacy = get(port, &legacy_path, "case l legacy");
    assert_eq!(legacy.status, 200, "case l legacy status");
    let q_res = search(
        port,
        r#"{ .env = "prod" && duration >= 1ms }"#,
        w0,
        w1,
        "",
        "case l q",
    );
    assert_eq!(
        trace_set(&legacy.json("case l legacy")),
        trace_set(&q_res.json("case l q")),
        "legacy tags+minDuration must return the same set as its q= equivalent"
    );
    assert_eq!(trace_set(&legacy.json("case l legacy")), ids(&[1, 14]));

    // -- (n) root hydration is trace-wide: the true root predates start. --
    let res = search(port, r#"{ name = "child" }"#, w0, w1, "", "case n");
    let json = res.json("case n");
    assert_eq!(trace_set(&json), ids(&[16]), "case n set");
    let trace = &json["traces"][0];
    assert_eq!(
        trace["rootTraceName"], "root-op",
        "the out-of-window root supplies the metadata, body {json}"
    );
    assert_eq!(
        trace["startTimeUnixNano"],
        ts(base, -300).to_string(),
        "root start comes from the full trace, body {json}"
    );

    // -- (o) public ordering: max matched ts DESC, trace_id ASC; ties are
    // deterministic across repeated identical requests. --------------------
    let res = search(port, r#"{ name = "hot" }"#, w0, w1, "", "case o order");
    let json = res.json("case o order");
    assert_eq!(
        trace_order(&json),
        vec![hex(&tid(10)), hex(&tid(9))],
        "newest matched span wins, body {json}"
    );
    let first = search(port, r#"{ name = "tie" }"#, w0, w1, "", "case o tie 1");
    let second = search(port, r#"{ name = "tie" }"#, w0, w1, "", "case o tie 2");
    assert_eq!(
        trace_order(&first.json("case o tie 1")),
        vec![hex(&tid(17)), hex(&tid(18))],
        "tied timestamps break ascending by trace id"
    );
    assert_eq!(
        first.body, second.body,
        "identical requests must be byte-identical under ties"
    );

    // -- match-all over an empty sub-window: documented empty envelope. ---
    let res = search(port, "{}", w1 + 100, w1 + 200, "", "empty window");
    let json = res.json("empty window");
    assert_eq!(json["traces"], serde_json::json!([]));
    assert_eq!(json["metrics"]["partial"], false);
    assert_eq!(json["metrics"]["returned"], 0);
}

// ---------------------------------------------------------------------
// Spawn B: AC5 — cap, partial flag, exact global top-K, boundary.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn candidate_cap_partial_and_boundary_semantics() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_132;
    let db = "pulsus_traces_search_it_b";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_TRACEQL_MAX_CANDIDATES", "3")]);

    let base = now_s() - 3_600;
    for n in 1..=5u8 {
        ingest(
            port,
            vec![span(
                tid(100 + n),
                sid(1),
                None,
                "cap",
                ts(base, n as i64),
                10 * MS,
                vec![],
            )],
            checkout_resource(),
            "seed cap trace",
        );
    }
    // A newer FALSE POSITIVE for the conjunction case: name matches, the
    // attr does not.
    ingest(
        port,
        vec![span(
            tid(110),
            sid(1),
            None,
            "cap",
            ts(base, 8),
            10 * MS,
            vec![],
        )],
        checkout_resource(),
        "seed false positive",
    );
    for n in 1..=5u8 {
        // Give the five true traces the attr the false positive lacks.
        ingest(
            port,
            vec![span(
                tid(100 + n),
                sid(2),
                None,
                "cap",
                ts(base, n as i64),
                10 * MS,
                vec![kv_str("marked", "yes")],
            )],
            checkout_resource(),
            "seed marked span",
        );
    }

    // Over-cap: 6 name-matching traces against a cap of 3 → partial, and
    // the returned set is the exact newest-3 by the public order.
    let ctx = "over-cap";
    let res = search(port, r#"{ name = "cap" }"#, base, base + 60, "", ctx);
    let json = res.json(ctx);
    assert_eq!(json["metrics"]["partial"], true, "{ctx}: body {json}");
    assert_eq!(json["metrics"]["returned"], 3, "{ctx}: body {json}");
    assert_eq!(
        trace_order(&json),
        vec![hex(&tid(110)), hex(&tid(105)), hex(&tid(104))],
        "{ctx}: exact global-recency membership, body {json}"
    );

    // A newer false positive must not evict true matches. The
    // cross-spanset form keeps BOTH generators (superset union), so the
    // ranked candidates start with 110 — which fails the exact `&&`
    // evaluation (no marked span) — followed by the true matches 105 and
    // 104; the exact true top-2 is returned, short page + partial.
    let ctx = "false-positive-newer";
    let res = search(
        port,
        r#"{ name = "cap" } && { span.marked = "yes" }"#,
        base,
        base + 60,
        "",
        ctx,
    );
    let json = res.json(ctx);
    assert_eq!(json["metrics"]["partial"], true, "{ctx}: body {json}");
    assert_eq!(
        trace_order(&json),
        vec![hex(&tid(105)), hex(&tid(104))],
        "{ctx}: the false positive consumes budget but never displaces a true match"
    );

    // Sub-cap: a window holding 2 matching traces → complete.
    let ctx = "sub-cap";
    let res = search(port, r#"{ name = "cap" }"#, base + 4, base + 6, "", ctx);
    let json = res.json(ctx);
    assert_eq!(json["metrics"]["partial"], false, "{ctx}: body {json}");
    assert_eq!(trace_set(&json), ids(&[104, 105]), "{ctx}");

    // Exactly-at-cap boundary: a window holding exactly 3 matching
    // traces — the generator returns 3 (< cap+1), the merged stream
    // exhausts at the ceiling with no lookahead row: NOT partial (the
    // round-3 false-positive-partial gap).
    let ctx = "exactly-at-cap";
    let res = search(port, r#"{ name = "cap" }"#, base + 3, base + 6, "", ctx);
    let json = res.json(ctx);
    assert_eq!(
        json["metrics"]["partial"], false,
        "{ctx}: exhausting exactly at the cap is not partial, body {json}"
    );
    assert_eq!(trace_set(&json), ids(&[103, 104, 105]), "{ctx}");

    // Ceiling and threshold engaging in the SAME iteration (code review
    // round 1): a multi-generator union (two attr-eq generators, 2 rows
    // each — NEITHER truncated) merges 4 candidates against the cap of
    // 3, with limit=1 so the heap is already full and the lookahead
    // candidate's bound is below the held sort key. The engaged ceiling
    // must be recorded — partial=true, never masked by threshold
    // termination.
    for (n, key, off) in [
        (120u8, "x", 20i64),
        (121, "x", 21),
        (122, "y", 22),
        (123, "y", 23),
    ] {
        ingest(
            port,
            vec![span(
                tid(n),
                sid(1),
                None,
                "both",
                ts(base, off),
                10 * MS,
                vec![kv_str(key, "1")],
            )],
            checkout_resource(),
            "seed both-engage trace",
        );
    }
    let ctx = "ceiling-and-threshold-same-iteration";
    let res = search(
        port,
        r#"{ .x = "1" || .y = "1" }"#,
        base,
        base + 60,
        "&limit=1",
        ctx,
    );
    let json = res.json(ctx);
    assert_eq!(
        json["metrics"]["partial"], true,
        "{ctx}: the consumption ceiling engaged with a lookahead candidate present — \
         threshold eligibility must not mask it, body {json}"
    );
    assert_eq!(
        trace_order(&json),
        vec![hex(&tid(123))],
        "{ctx}: the newest true match is still the returned page"
    );
}

// ---------------------------------------------------------------------
// Spawn C: the scan budget fails loud (422), never silently slow.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn scan_budget_breach_is_422_query_too_broad() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_133;
    let db = "pulsus_traces_search_it_c";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_TRACEQL_SCAN_BUDGET_ROWS", "50")]);

    let base = now_s() - 3_600;
    ingest(
        port,
        (0..100u8)
            .map(|n| span(tid(n), sid(1), None, "bulk", ts(base, n as i64), MS, vec![]))
            .collect(),
        checkout_resource(),
        "seed bulk",
    );

    // A negated attr forces the time-range fallback generator, which
    // must scan the 100-span window — over the 50-row budget.
    let ctx = "scan-budget-422";
    let path = format!(
        "/api/traces/v1/search?q={}&start={}&end={}",
        enc(r#"{ .env != "prod" }"#),
        base,
        base + 3_600
    );
    let res = get(port, &path, ctx);
    assert_eq!(
        res.status,
        422,
        "{ctx}: status, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    let json = res.json(ctx);
    assert_eq!(json["status"], "error", "{ctx}");
    assert_eq!(json["errorType"], "query_too_broad", "{ctx}: body {json}");
}

// ---------------------------------------------------------------------
// Spawn D: the `!`/truthiness asymmetry against a store that HOLDS a
// string-valued attribute (issue #335 Stage B).
// ---------------------------------------------------------------------

/// The two deliberate behaviour changes of the Stage B grammar collapse,
/// end to end through the real route.
///
/// **Why this needs a live store, and why an empty one will not do.** The
/// whole claim is about what happens when a span carries a NON-boolean
/// value under `!`. Evaluation never touches a span in an empty store, so
/// the error cannot fire and every one of these queries answers a cheerful
/// 200 — which is exactly how the `{ .a }` defect below hid during Stage
/// B's own development, where every probe ran against an empty container.
/// The seed therefore includes a STRING-valued `a` on purpose; it is the
/// load-bearing fixture, not corpus decoration.
///
/// What is pinned:
///
/// 1. `{ .a }` is truthiness and TOLERATES the string: 200, matching only
///    the span whose `a` is `true`. Not a 500, and not the `false` or
///    string spans either. `{ .a }` plans as the plain comparison
///    `.a = true`, so a string simply is not equal to a boolean.
/// 2. `{ !.a = 1 }` FAILS the query, because `!` demands a boolean and the
///    string span reaches the evaluator. No boolean could have satisfied
///    `= 1` anyway (`BoolMatch::Never`), and that is the point: the
///    operand is still resolved, so the failure survives. Answering an
///    empty 200 here would be a silent wrong answer.
/// 3. `{ !.a = .b }` — a field on both sides — is a BOOLEAN-vs-boolean
///    comparison since issue #351, so the `!` type demand reaches it too:
///    the string span fails the whole query with the same message case 2
///    gives. Before #351 this was a plan-time 400 (`a boolean negation is
///    not an arithmetic operand`) and the reference answered it; the
///    replacement pins the demand at the shape's NEW home rather than
///    deleting the row.
/// 4. `{ !.c = !.d }` and `{ !.c = .d }` over spans carrying only
///    booleans: the match sets the reference gives — equal booleans for
///    the double negation, differing booleans for the mixed spelling.
///    Case 3 alone could not tell "the query fails" from "the query is
///    still refused", and neither could tell either from "it matches the
///    wrong spans".
///
/// STATUS NOTE, deliberate divergence: the reference answers case 2 with a
/// **500** (`expression (!.a) expected a boolean, but got TypeString`)
/// because the failure happens mid-scan inside its querier. We answer
/// **400 bad_data**: it is a client's malformed query, not a server fault,
/// and `traces_api::error` has always classified `PipelineInvalid` that
/// way. The matrix scores a reference 500 as INCONCLUSIVE rather than as a
/// rejection (`excluded_inconclusive`), so this does not move the
/// scoreboard either way.
#[tokio::test(flavor = "multi_thread")]
async fn negation_demands_a_boolean_where_truthiness_tolerates_a_string() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_134;
    let db = "pulsus_traces_search_it_d";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[]);

    let base = now_s() - 3_600;
    let (w0, w1) = (base, base + 600);

    // The string-valued `a` is the fixture that makes this test able to
    // fail at all.
    for (n, name, attrs) in [
        (1u8, "a-string", vec![kv_str("a", "hello"), kv_int("b", 1)]),
        (2u8, "a-true", vec![kv_bool("a", true), kv_int("b", 1)]),
        (3u8, "a-false", vec![kv_bool("a", false), kv_int("b", 1)]),
    ] {
        ingest(
            port,
            vec![span(
                tid(n),
                sid(1),
                None,
                name,
                ts(base, n as i64),
                MS,
                attrs,
            )],
            checkout_resource(),
            name,
        );
    }

    // 1. Truthiness tolerates the string: 200, only the `true` span.
    let ctx = "truthiness-tolerates-string";
    let res = search(port, "{ .a }", w0, w1, "", ctx);
    assert_eq!(
        trace_set(&res.json(ctx)),
        BTreeSet::from([hex(&tid(2))]),
        "{ctx}: only a == true matches; the string and false spans do not, \
         and the string must not fail the query"
    );

    // 2. `!` demands a boolean: the string span fails the whole query.
    let ctx = "negation-demands-boolean";
    let path = format!(
        "/api/traces/v1/search?q={}&start={w0}&end={w1}",
        enc("{ !.a = 1 }")
    );
    let res = get(port, &path, ctx);
    assert_eq!(
        res.status,
        400,
        "{ctx}: a present non-boolean under `!` must fail the query, not \
         answer an empty 200, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    let json = res.json(ctx);
    assert_eq!(json["status"], "error", "{ctx}");
    let msg = json["error"].as_str().unwrap_or_default();
    // The reference names the operand too (`expression (!.a) expected a
    // boolean, but got TypeString`). Pinned by EQUALITY, not `contains`:
    // this message read `expression (!the attribute) expected a boolean`
    // until this test was written, which is no use to anyone whose query
    // mentions several attributes.
    assert_eq!(msg, "expression (!.a) expected a boolean", "{ctx}");

    // 3. A field on both sides is a boolean-vs-boolean comparison since
    //    issue #351, so the `!` type demand still fires on the string span
    //    — the query fails, and it fails for the SAME stated reason as
    //    case 2 rather than being refused at plan time.
    let ctx = "negated-operand-vs-field-rhs";
    let res = get(
        port,
        &format!(
            "/api/traces/v1/search?q={}&start={w0}&end={w1}",
            enc("{ !.a = .b }")
        ),
        ctx,
    );
    assert_eq!(
        res.status,
        400,
        "{ctx}: body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    let json = res.json(ctx);
    assert_eq!(json["errorType"], "bad_data", "{ctx}");
    assert_eq!(
        json["error"], "expression (!.a) expected a boolean",
        "{ctx}: the boolean-operand path must name the operand it refused"
    );

    // 4. Issue #351: the match sets, over spans whose booleans are the
    //    only values in play. Seeded here rather than above so cases 1-3
    //    keep the string-`a` fixture that makes them able to fail.
    for (n, name, attrs) in [
        (4u8, "cd-tt", vec![kv_bool("c", true), kv_bool("d", true)]),
        (5u8, "cd-ff", vec![kv_bool("c", false), kv_bool("d", false)]),
        (6u8, "cd-tf", vec![kv_bool("c", true), kv_bool("d", false)]),
        // `c` alone: an absent operand is NO MATCH, never a failure.
        (7u8, "cd-t_", vec![kv_bool("c", true)]),
    ] {
        ingest(
            port,
            vec![span(
                tid(n),
                sid(1),
                None,
                name,
                ts(base, n as i64),
                MS,
                attrs,
            )],
            checkout_resource(),
            name,
        );
    }
    for (q, expected, ctx) in [
        (
            "{ !.c = !.d }",
            BTreeSet::from([hex(&tid(4)), hex(&tid(5))]),
            "double-negation-equal-booleans",
        ),
        (
            "{ !.c = .d }",
            BTreeSet::from([hex(&tid(6))]),
            "mixed-negation-differing-booleans",
        ),
    ] {
        let res = search(port, q, w0, w1, "", ctx);
        assert_eq!(
            trace_set(&res.json(ctx)),
            expected,
            "{ctx}: {q} must match the reference's span set"
        );
    }
}

/// Issue #351 (owner ruling, 2026-08-05): the MULTI-VALUED event/link
/// comparison, end to end through the real server and a live ClickHouse.
///
/// **This is the leg that can fail.** The semantics live in a per-batch
/// value co-load that no hermetic test executes: the read has to run,
/// its rows — ONE PER VALUE — have to accumulate into each span's value
/// list, and the any/all-match rule has to be applied to that list. A
/// plan-level check would pass with the co-load returning nothing at
/// all.
///
/// The fixture is the one the reference was probed with, and the
/// expectation is OURS — which is where the ratified divergence sits
/// (ledger `traceql-event-link-operand-any-match`):
///
/// | span | events | `.a` | reference | PulsusDB |
/// |---|---|---|---|---|
/// | 1 | evX,evY,evZ | `evZ` (LAST) | no | **match** |
/// | 2 | evP,evQ,evR | `evP` (first) | match | **match** |
/// | 3 | ev1,evM,ev2 | `evM` (middle) | no | **match** |
/// | 4 | ev7,ev8 | `evNope` | no | no |
/// | 5 | (no events) | `evX` | no | no |
///
/// Span 4 keeps "any" from degenerating into "always"; span 5 is the
/// empty set, which is the case `!=` turns on.
#[tokio::test(flavor = "multi_thread")]
async fn event_and_link_comparisons_match_any_event_over_real_clickhouse() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_135;
    let db = "pulsus_traces_search_it_e";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[]);

    let base = now_s() - 3_600;
    let (w0, w1) = (base, base + 600);

    for (n, name, a, events) in [
        (1u8, "ev-last", "evZ", vec!["evX", "evY", "evZ"]),
        (2u8, "ev-first", "evP", vec!["evP", "evQ", "evR"]),
        (3u8, "ev-mid", "evM", vec!["ev1", "evM", "ev2"]),
        (4u8, "ev-none", "evNope", vec!["ev7", "ev8"]),
        (5u8, "ev-empty", "evX", vec![]),
    ] {
        let start = ts(base, n as i64);
        let mut s = span(
            tid(n),
            sid(1),
            None,
            name,
            start,
            MS,
            vec![kv_str("a", a), kv_str("linked", &hex(&sid(0xAA)))],
        );
        s.events = events
            .iter()
            .enumerate()
            .map(
                |(i, ev)| opentelemetry_proto::tonic::trace::v1::span::Event {
                    time_unix_nano: start + (i as u64 + 1) * 1_000_000,
                    name: (*ev).to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                },
            )
            .collect();
        // Two links on every span, the SECOND of which is the one `.linked`
        // names — so a first-link reading would return nothing here.
        s.links = [sid(0xBB), sid(0xAA)]
            .iter()
            .map(|target| opentelemetry_proto::tonic::trace::v1::span::Link {
                trace_id: tid(0xEE).to_vec(),
                span_id: target.to_vec(),
                ..Default::default()
            })
            .collect();
        ingest(port, vec![s], checkout_resource(), name);
    }

    for (q, expected, ctx) in [
        // ANY event matches — including the LAST (span 1) and the MIDDLE
        // (span 3), which the reference's field-vs-field path misses.
        (
            "{ .a = event:name }",
            BTreeSet::from([hex(&tid(1)), hex(&tid(2)), hex(&tid(3))]),
            "any-event-eq",
        ),
        // Operand order is symmetric for `=`.
        (
            "{ event:name = .a }",
            BTreeSet::from([hex(&tid(1)), hex(&tid(2)), hex(&tid(3))]),
            "any-event-eq-reversed",
        ),
        // `!=` is ALL-match: only the spans where NO event matches — and
        // the span with no events at all, which satisfies it vacuously
        // (the same absent-key rule the literal form follows).
        (
            "{ .a != event:name }",
            BTreeSet::from([hex(&tid(4)), hex(&tid(5))]),
            "all-event-neq-including-empty-set",
        ),
        // Links behave identically, and `.linked` names the SECOND link.
        (
            "{ .linked = link:spanID }",
            BTreeSet::from([
                hex(&tid(1)),
                hex(&tid(2)),
                hex(&tid(3)),
                hex(&tid(4)),
                hex(&tid(5)),
            ]),
            "any-link-eq",
        ),
    ] {
        let res = search(port, q, w0, w1, "", ctx);
        assert_eq!(trace_set(&res.json(ctx)), expected, "{ctx}: {q}");
    }

    // The literal form is unchanged and still index-served — the control
    // that says the value co-load did not disturb the membership path.
    let ctx = "literal-event-name-unchanged";
    let res = search(port, r#"{ event:name = "evZ" }"#, w0, w1, "", ctx);
    assert_eq!(
        trace_set(&res.json(ctx)),
        BTreeSet::from([hex(&tid(1))]),
        "{ctx}"
    );

    // The `~`/`!~` half of the owner's ruling (review 3). It lives HERE,
    // on the literal path, because a regex against a FIELD operand is a
    // 400 in both systems — `pulsus_traceql::validate` and the reference
    // both answer `invalid type for =~ or !~: event:name`, measured — so
    // the field-vs-field path can never see a regex operator.
    //
    // `=~` is ANY-match: span 1's events are evX/evY/evZ and the pattern
    // matches evZ. `!~` is ALL-match: span 1 is EXCLUDED because one of
    // its events matches, while spans 2-4 (no `ev?` match... spans 2 and
    // 3 do contain `ev`-prefixed names, so the discriminating pattern is
    // anchored on evZ alone) are kept.
    for (q, expected, ctx) in [
        (
            r#"{ event:name =~ "evZ" }"#,
            BTreeSet::from([hex(&tid(1))]),
            "regex-any-match",
        ),
        (
            r#"{ event:name !~ "evZ" }"#,
            BTreeSet::from([hex(&tid(2)), hex(&tid(3)), hex(&tid(4)), hex(&tid(5))]),
            "regex-all-match-excludes-the-span-with-a-matching-event",
        ),
    ] {
        let res = search(port, q, w0, w1, "", ctx);
        assert_eq!(trace_set(&res.json(ctx)), expected, "{ctx}: {q}");
    }
}

// ---------------------------------------------------------------------
// Spawn F: a wide event set is REFUSED, not materialized (issue #351,
// review of the first cut).
// ---------------------------------------------------------------------

/// Issue #351 review: one span carrying many distinct event values must
/// hit the read budget and come back `422 query_too_broad` — never a
/// silently materialized unbounded per-span array.
///
/// **Why this cannot be a hermetic test.** The read happens in the
/// database, and so did the growth this test exists to refuse: the first
/// cut read `groupUniqArray(...) GROUP BY`, whose array was built
/// server-side, so no in-process test could observe either the aggregate
/// state or the size of the row that came back. The read is now one row
/// per value (`search_sql::event_set_sql`), and it is still only against
/// a live ClickHouse that the row budget, the streaming charge and the
/// refusal are real at once.
///
/// **Scale-invariant, per the standing rule:** the budget is lowered
/// rather than the fixture inflated (the `scan_budget_breach_is_422…`
/// precedent above), so this never becomes a wall-time or memory race.
///
/// The pair is what makes it evidence:
///
/// * `{ name != event:name }` — the scalar side is a PHYSICAL intrinsic,
///   so the event value read is the ONLY `trace_attrs_idx` read in the
///   whole query; blowing the row budget can therefore only be that read.
/// * `{ name = "ev-bulk" }` over the same store and the same budget
///   answers `200`. Without this control, a 422 would equally well be
///   explained by an unrelated read, and the test would prove nothing
///   about the event path.
#[tokio::test(flavor = "multi_thread")]
async fn a_wide_event_set_is_refused_by_the_budget_not_materialized() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_136;
    let db = "pulsus_traces_search_it_f";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_TRACEQL_SCAN_BUDGET_ROWS", "50")]);

    let base = now_s() - 3_600;
    let (w0, w1) = (base, base + 600);

    // ONE span carrying 400 distinct event names — the shape the review
    // was about: a single span whose event cardinality, not the trace
    // count, drives the read.
    let start = ts(base, 1);
    let mut s = span(tid(1), sid(1), None, "ev-bulk", start, MS, vec![]);
    s.events = (0..400u32)
        .map(|i| opentelemetry_proto::tonic::trace::v1::span::Event {
            time_unix_nano: start + u64::from(i) + 1,
            name: format!("evt-{i:04}"),
            attributes: vec![],
            dropped_attributes_count: 0,
        })
        .collect();
    ingest(port, vec![s], checkout_resource(), "wide event span");

    // The control FIRST: the same store and budget answer 200 when the
    // query does not read the event values.
    let ctx = "control-no-event-read-is-200";
    let res = search(port, r#"{ name = "ev-bulk" }"#, w0, w1, "", ctx);
    assert_eq!(
        trace_set(&res.json(ctx)),
        BTreeSet::from([hex(&tid(1))]),
        "{ctx}: the budget must not already be exhausted without the event read"
    );

    // The event value read is the only attrs read here, and its rows
    // exceed the 50-row budget: a clean 422, never an unbounded
    // materialization.
    let ctx = "wide-event-set-is-422";
    let wide_query = "{ name != event:name }";
    let path = format!(
        "/api/traces/v1/search?q={}&start={w0}&end={w1}",
        enc(wide_query)
    );
    let res = get(port, &path, ctx);
    assert_eq!(
        res.status,
        422,
        "{ctx}: a wide event set must be refused, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    let json = res.json(ctx);
    assert_eq!(json["status"], "error", "{ctx}");
    assert_eq!(json["errorType"], "query_too_broad", "{ctx}: body {json}");

    // The SUCCESS control, on a second server over the SAME database with
    // a normal budget (review 2's second test gap). Without it, a 422
    // could equally mean the query is permanently broken — and it also
    // exercises the co-load at 400 values through the real route, which
    // is the widest set anything runs against a live ClickHouse.
    //
    // `{ name != event:name }` is ALL-match: none of the 400 event names
    // is `ev-bulk`, so every element satisfies the inequality and the
    // span matches.
    let wide_port = port + 1;
    let _wide_guard = spawn_ready(wide_port, db, &[]);
    let ctx = "wide-event-set-succeeds-under-a-normal-budget";
    let res = search(wide_port, wide_query, w0, w1, "", ctx);
    assert_eq!(
        trace_set(&res.json(ctx)),
        BTreeSet::from([hex(&tid(1))]),
        "{ctx}: the 422 above must be the BUDGET, not a permanently failing query"
    );
}
