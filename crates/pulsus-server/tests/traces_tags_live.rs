//! Issue #58 AC8 (+ the Δ1/Δ3 seeded assertions) live end-to-end tests
//! for `GET /api/traces/v1/tags` and `GET /api/traces/v1/tag/{tag}/values`:
//! spawns the real `pulsusdb` binary against a live ClickHouse (the
//! `traces_search_live.rs` harness), seeds through the *product* ingest
//! path (`POST /v1/traces`, sync — the MV populates `trace_tag_catalog`
//! on insert), then asserts:
//!
//! - the bounded, deduplicated, `(scope, key)`-ordered scoped tag-name
//!   set, with and without `scope=`, with `start`/`end` proven ignored;
//! - the bounded, deduplicated, ordered typed value sets (string / int /
//!   duration / bool inference live, incl. the unscoped dual-scope key
//!   forms);
//! - the adjudicated `q=` superset semantics: a non-trivial `q` returns
//!   the SAME full value set as no `q` (accept-and-ignore, never a 400);
//! - the Δ3 truncation contract on BOTH caps: an over-cap key returns
//!   exactly `TAG_VALUES_MAX` ordered values with `truncated: true`
//!   (under-cap `false`), and an over-cap catalog (> `TAG_NAMES_MAX`
//!   distinct keys) returns exactly `TAG_NAMES_MAX` `(scope, key)` pairs
//!   with `truncated: true`;
//! - the zero-payload proof (epic #19 AC1), identity-based: the run's
//!   nonce'd database is the identity for every server-issued query
//!   (`query_id` cannot be set over HTTP); the discovery Select set —
//!   matched by the byte-frozen SELECT lists, independent of the FROM
//!   table — must count EXACTLY the requests this test made and read
//!   only `trace_tag_catalog`, and zero Selects in the run (any SQL
//!   text) may touch `trace_spans`/`trace_attrs_idx`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test traces_tags_live
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Port 31134 — distinct from every other live suite's fixed ports
//! (31100-31133).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use futures::StreamExt;
use prost::Message;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

/// The read-side response caps (`pulsus_read::TAG_VALUES_MAX` /
/// `TAG_NAMES_MAX`) — pinned numerically here so the live suite fails if
/// the documented contract (docs/api.md §4.3) drifts from the code.
const TAG_VALUES_MAX: usize = 1_000;
const TAG_NAMES_MAX: usize = 10_000;

// ---------------------------------------------------------------------
// Bare-TcpStream HTTP helper (the traces_search_live.rs idiom).
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

fn ch_config() -> ChConnConfig {
    ChConnConfig {
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
    }
}

async fn drop_db(db: &str) {
    let client = ChClient::new(ch_config()).await.expect("connect for drop");
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
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
        .env("CLICKHOUSE_DB", db);
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

fn kv_int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
        key_strindex: 0,
    }
}

fn span(trace_id: u8, span_id: u8, name: &str, start_ns: u64, attrs: Vec<KeyValue>) -> Span {
    let mut tid = [0u8; 16];
    tid[15] = trace_id;
    let mut sid = [0u8; 8];
    sid[6] = trace_id;
    sid[7] = span_id;
    Span {
        trace_id: tid.to_vec(),
        span_id: sid.to_vec(),
        name: name.to_string(),
        start_time_unix_nano: start_ns,
        end_time_unix_nano: start_ns + 1_000_000,
        attributes: attrs,
        ..Default::default()
    }
}

fn ingest(port: u16, spans: Vec<Span>, resource_attrs: Vec<KeyValue>, ctx: &str) {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: resource_attrs,
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
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

// ---------------------------------------------------------------------
// Response helpers.
// ---------------------------------------------------------------------

/// `scopes` as ordered `(name, keys)` pairs.
fn scopes_of(json: &serde_json::Value, ctx: &str) -> Vec<(String, Vec<String>)> {
    json["scopes"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: scopes must be an array, body {json}"))
        .iter()
        .map(|s| {
            (
                s["name"].as_str().expect("scope name").to_string(),
                s["tags"]
                    .as_array()
                    .expect("scope tags")
                    .iter()
                    .map(|t| t.as_str().expect("tag").to_string())
                    .collect(),
            )
        })
        .collect()
}

/// `tagValues` as ordered `(type, value)` pairs.
fn values_of(json: &serde_json::Value, ctx: &str) -> Vec<(String, String)> {
    json["tagValues"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: tagValues must be an array, body {json}"))
        .iter()
        .map(|v| {
            (
                v["type"].as_str().expect("type").to_string(),
                v["value"].as_str().expect("value").to_string(),
            )
        })
        .collect()
}

/// Every successful discovery request goes through here so the test
/// carries an exact count of the ClickHouse discovery queries it caused
/// — the identity the zero-payload query_log proof asserts against (an
/// exact `== discovered` count, never a `>=` threshold a missing or
/// mis-filtered query could hide under).
fn get_json(port: u16, path: &str, ctx: &str, discovered: &mut usize) -> serde_json::Value {
    let res = get(port, path, ctx);
    assert_eq!(
        res.status,
        200,
        "{ctx}: must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    *discovered += 1;
    res.json(ctx)
}

const NAMES_URL: &str = "/api/traces/v1/tags";

fn values_url(tag: &str) -> String {
    format!("/api/traces/v1/tag/{tag}/values")
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TablesRow {
    tables: Vec<String>,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

// ---------------------------------------------------------------------
// The suite: one spawn, seeded once.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tag_discovery_against_real_clickhouse() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_134;
    // Per-run nonce'd database (the traces-read bench's nonce rationale):
    // `system.query_log` outlives databases, so a fixed name would
    // aggregate rows across local re-runs and break the EXACT-count
    // zero-payload proof below — `current_database = <nonce'd db>` is the
    // per-run identity attached to every query this spawn issues (the
    // test cannot set `query_id` on server-issued queries over HTTP).
    // Dropped at the end of the test (a panic leaks one throwaway db;
    // the next run uses a fresh nonce, so assertions stay exact).
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let db = format!("pulsus_traces_tags_live_it_{nonce}");
    let db = db.as_str();
    drop_db(db).await;
    let _guard = spawn_ready(port, db);
    // Discovery-request counter: incremented by `get_json` (every 200
    // discovery response), asserted `==` against query_log at the end.
    let mut discovered = 0usize;

    let base_ns: u64 = 1_700_000_000_000_000_000;

    // -- Seed (sync ingest => the catalog MV rows are read-visible). ----
    // Two spans with IDENTICAL span attrs (dedup fixture) + typed values:
    // int (OTLP IntValue renders '500'), duration-looking ('1.5s'),
    // bool-looking ('true').
    let typed_attrs = || {
        vec![
            kv_int("http.status_code", 500),
            kv_str("latency.bucket", "1.5s"),
            kv_str("cache.hit", "true"),
        ]
    };
    ingest(
        port,
        vec![
            span(1, 1, "op-a", base_ns, typed_attrs()),
            span(1, 2, "op-b", base_ns + 1_000, typed_attrs()),
        ],
        vec![kv_str("service.name", "checkout"), kv_str("env", "prod")],
        "seed T1",
    );
    ingest(
        port,
        vec![span(
            2,
            1,
            "op-c",
            base_ns + 2_000,
            vec![kv_int("http.status_code", 200)],
        )],
        vec![kv_str("service.name", "payments")],
        "seed T2",
    );
    // Over-cap fixture: TAG_VALUES_MAX + 50 DISTINCT values for one span
    // key, zero-padded so the expected capped prefix is the ascending
    // v00000..v00999 run (Δ3: cap, cap+1, ordering, dedup).
    ingest(
        port,
        (0..(TAG_VALUES_MAX + 50))
            .map(|n| {
                span(
                    3,
                    (n % 200) as u8,
                    "bulk",
                    base_ns + 3_000 + n as u64,
                    vec![kv_str("bulk.id", &format!("v{n:05}"))],
                )
            })
            .collect(),
        vec![kv_str("service.name", "checkout")],
        "seed bulk",
    );

    // -- Tags: full scoped shape, deduped, (scope, key)-ordered. ---------
    let ctx = "tags full";
    let json = get_json(port, NAMES_URL, ctx, &mut discovered);
    assert_eq!(
        scopes_of(&json, ctx),
        vec![
            (
                "resource".to_string(),
                vec!["env".to_string(), "service.name".to_string()],
            ),
            (
                "span".to_string(),
                vec![
                    "bulk.id".to_string(),
                    "cache.hit".to_string(),
                    "http.status_code".to_string(),
                    "latency.bucket".to_string(),
                ],
            ),
        ],
        "{ctx}: deduped scoped tag names in (scope, key) order, body {json}"
    );
    assert_eq!(json["truncated"], false, "{ctx}: body {json}");

    // -- scope= filters; start/end are accepted and IGNORED (the catalog
    // is time-less — a window excluding every span changes nothing). -----
    let ctx = "tags scope=resource";
    let json = get_json(
        port,
        &format!("{NAMES_URL}?scope=resource"),
        ctx,
        &mut discovered,
    );
    assert_eq!(
        scopes_of(&json, ctx),
        vec![(
            "resource".to_string(),
            vec!["env".to_string(), "service.name".to_string()],
        )],
        "{ctx}: body {json}"
    );
    let ctx = "tags scope=span start/end ignored";
    let json = get_json(
        port,
        &format!("{NAMES_URL}?scope=span&start=1&end=2"),
        ctx,
        &mut discovered,
    );
    assert_eq!(
        scopes_of(&json, ctx).len(),
        1,
        "{ctx}: span scope only, body {json}"
    );
    assert_eq!(scopes_of(&json, ctx)[0].0, "span", "{ctx}");

    // -- scope=bogus is an explicit 400, never widened. ------------------
    let ctx = "tags scope=bogus";
    let res = get(port, &format!("{NAMES_URL}?scope=bogus"), ctx);
    assert_eq!(res.status, 400, "{ctx}");
    let json = res.json(ctx);
    assert_eq!(json["status"], "error", "{ctx}");
    assert_eq!(json["errorType"], "bad_data", "{ctx}: body {json}");

    // -- Values: typed inference live (string/int/duration/bool). --------
    let ctx = "values resource.service.name";
    let json = get_json(
        port,
        &values_url("resource.service.name"),
        ctx,
        &mut discovered,
    );
    assert_eq!(
        values_of(&json, ctx),
        vec![
            ("string".to_string(), "checkout".to_string()),
            ("string".to_string(), "payments".to_string()),
        ],
        "{ctx}: deduped ordered string values, body {json}"
    );
    assert_eq!(json["truncated"], false, "{ctx}: under-cap key");

    let ctx = "values span.http.status_code";
    let json = get_json(
        port,
        &values_url("span.http.status_code"),
        ctx,
        &mut discovered,
    );
    assert_eq!(
        values_of(&json, ctx),
        vec![
            ("int".to_string(), "200".to_string()),
            ("int".to_string(), "500".to_string()),
        ],
        "{ctx}: body {json}"
    );
    // The unscoped forms (leading dot / bare) resolve the same key across
    // both scopes — identical set here (the key exists only span-side).
    for tag in [".http.status_code", "http.status_code"] {
        let ctx = "values unscoped http.status_code";
        let json = get_json(port, &values_url(tag), ctx, &mut discovered);
        assert_eq!(values_of(&json, ctx).len(), 2, "{ctx} ({tag}): body {json}");
    }

    let ctx = "values span.latency.bucket (duration inference)";
    let json = get_json(
        port,
        &values_url("span.latency.bucket"),
        ctx,
        &mut discovered,
    );
    assert_eq!(
        values_of(&json, ctx),
        vec![("duration".to_string(), "1.5s".to_string())],
        "{ctx}: body {json}"
    );
    let ctx = "values span.cache.hit (bool inference)";
    let json = get_json(port, &values_url("span.cache.hit"), ctx, &mut discovered);
    assert_eq!(
        values_of(&json, ctx),
        vec![("bool".to_string(), "true".to_string())],
        "{ctx}: body {json}"
    );

    // -- Δ1: a NON-TRIVIAL q is accepted and ignored — the seeded
    // superset equivalence (same full set as no q), never a 400. ---------
    let ctx = "values q superset";
    let no_q = get_json(
        port,
        &values_url("resource.service.name"),
        ctx,
        &mut discovered,
    );
    let with_q = get_json(
        port,
        &format!(
            "{}?q=%7Bspan.x%3D%22y%22%7D&start=1&end=2",
            values_url("resource.service.name")
        ),
        ctx,
        &mut discovered,
    );
    assert_eq!(
        values_of(&with_q, ctx),
        values_of(&no_q, ctx),
        "{ctx}: q cannot be evaluated against the catalog — the result is the same \
         (superset) set, body {with_q}"
    );

    // -- Δ3: the over-cap key truncates non-silently: exactly the cap,
    // ordered, deduped, truncated=true. -----------------------------------
    let ctx = "values over-cap";
    let json = get_json(port, &values_url("span.bulk.id"), ctx, &mut discovered);
    let vals = values_of(&json, ctx);
    assert_eq!(vals.len(), TAG_VALUES_MAX, "{ctx}: exactly the cap");
    assert_eq!(
        json["truncated"], true,
        "{ctx}: non-silent, body truncated flag"
    );
    let expected: Vec<(String, String)> = (0..TAG_VALUES_MAX)
        .map(|n| ("string".to_string(), format!("v{n:05}")))
        .collect();
    assert_eq!(
        vals, expected,
        "{ctx}: the ordered, deduplicated ascending prefix"
    );

    // -- The TAG_NAMES_MAX twin of the values cap: seed past 10,000
    // distinct keys (cheap: 11 spans x 1,000 distinct span-attr keys in
    // one ingest request), then prove end-to-end that the capped names
    // response is exactly TAG_NAMES_MAX pairs, still (scope, key)-ordered,
    // with truncated=true. Seeded AFTER the exact-set assertions above
    // (which rely on the small fixture). ----------------------------------
    ingest(
        port,
        (0..11u8)
            .map(|s| {
                span(
                    4,
                    s,
                    "names-bulk",
                    base_ns + 10_000 + s as u64,
                    (0..1_000u32)
                        .map(|k| kv_str(&format!("bulkkey.{:05}", s as u32 * 1_000 + k), "x"))
                        .collect(),
                )
            })
            .collect(),
        vec![kv_str("service.name", "checkout")],
        "seed names bulk",
    );
    let ctx = "tags over-cap";
    let json = get_json(port, NAMES_URL, ctx, &mut discovered);
    assert_eq!(json["truncated"], true, "{ctx}: non-silent, body {json}");
    let scopes = scopes_of(&json, ctx);
    let total_pairs: usize = scopes.iter().map(|(_, keys)| keys.len()).sum();
    assert_eq!(total_pairs, TAG_NAMES_MAX, "{ctx}: exactly the cap");
    // Catalog holds 2 resource + (4 + 11,000) span pairs = 11,006; the
    // (scope, key)-ordered cap keeps resource whole and cuts the span
    // list at pair 10,000: bulk.id + bulkkey.00000..bulkkey.09996.
    assert_eq!(
        scopes[0],
        (
            "resource".to_string(),
            vec!["env".to_string(), "service.name".to_string()],
        ),
        "{ctx}: the resource scope survives the cap whole"
    );
    assert_eq!(scopes[1].0, "span", "{ctx}");
    assert_eq!(scopes[1].1.len(), TAG_NAMES_MAX - 2, "{ctx}");
    assert_eq!(scopes[1].1[0], "bulk.id", "{ctx}: ascending key order");
    assert_eq!(
        scopes[1].1.last().map(String::as_str),
        Some("bulkkey.09996"),
        "{ctx}: the cap cuts at exactly the 10,000th (scope, key) pair"
    );

    // -- Zero-payload proof (epic #19 AC1), identity-based (code review):
    // every server query in THIS run carries the nonce'd db as
    // `current_database`, so the run's Select set is exact. Two layers:
    //
    // (a) the discovery set is matched by the byte-frozen SELECT lists
    //     (`SELECT DISTINCT scope, key` / `SELECT DISTINCT val` — pinned
    //     by tags_sql's golden tests), INDEPENDENT of the FROM table, so
    //     a regression reading a payload table still lands inside the
    //     set; the row count must equal the exact number of discovery
    //     requests made above (no >= threshold to hide under) and every
    //     row's `tables` must be exactly the catalog;
    // (b) a text-independent ban: ZERO Selects in this run's database —
    //     any SQL shape whatsoever — touched trace_spans or
    //     trace_attrs_idx (this test never calls search/fetch, so any
    //     hit is a discovery regression by construction).
    let admin = ChClient::new(ch_config()).await.expect("connect admin");
    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let sql = format!(
        "SELECT arraySort(tables) AS tables FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND current_database = '{db}' \
           AND (query LIKE 'SELECT DISTINCT scope, key%' OR query LIKE 'SELECT DISTINCT val%')"
    );
    let mut stream = admin
        .query_stream::<TablesRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_log read");
    let mut rows = 0usize;
    while let Some(row) = stream.next().await {
        let row = row.expect("decode query_log row");
        rows += 1;
        assert_eq!(
            row.tables,
            vec![format!("{db}.trace_tag_catalog")],
            "a tag-discovery query must read exactly the catalog — no span/attr tables"
        );
    }
    assert_eq!(
        rows, discovered,
        "the discovery query set must be exactly the {discovered} requests this test made \
         (got {rows}) — a missing row means a discovery query escaped the shape filter"
    );
    let ban_sql = format!(
        "SELECT toUInt64(count()) AS n FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND current_database = '{db}' \
           AND (has(tables, '{db}.trace_spans') OR has(tables, '{db}.trace_attrs_idx'))"
    );
    let mut stream = admin
        .query_stream::<CountRow>(&ban_sql, &QuerySettings::new())
        .await
        .expect("query_log ban read");
    let mut banned = None;
    while let Some(row) = stream.next().await {
        banned = Some(row.expect("decode count row").n);
    }
    assert_eq!(
        banned,
        Some(0),
        "no Select in this run — regardless of its SQL text — may touch trace_spans or \
         trace_attrs_idx"
    );

    drop_db(db).await;
}
