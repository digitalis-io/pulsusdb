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
//!   text) may touch `trace_spans`/`trace_attrs_idx`;
//! - the issue #61 (T9) seeded wire-shape proof for the four RESHAPING
//!   Tempo tag aliases, over real HTTP against this non-trivial catalog
//!   (the spawn sets `PULSUS_COMPAT_ENDPOINTS=true`): v1 flat bare-string
//!   `tagNames`/`tagValues`, v2 typed `{"type","value"}` objects, and
//!   the `truncated` field ABSENT from every alias body while PRESENT
//!   (and, on the over-cap key, `true`) on the native twin. The alias
//!   requests ride the same exact-count zero-payload proof: they issue
//!   the identical catalog SELECTs, so `discovered` counts them too.
//!
//! Issue #475 adds a second test here, on a deliberately COLLIDING
//! corpus, for the tag answers that are served from the static intrinsic
//! vocabulary and read nothing. Its zero-delta gate is bounded, and the
//! boundary is written down in full beside the gate itself
//! (`intrinsic_discovery_answers_from_the_vocabulary_and_reads_no_trace_table`,
//! the block introducing steps (1)-(6)): what the predicate covers and
//! why that coverage is exact, the five things it does not cover — one of
//! them a measured escape accepted as a deliberate limit — the reason a
//! query-text exclusion must NOT be used to close it, and the durable
//! provenance-keyed fix, recorded uncosted and unscheduled.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test traces_tags_live
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Fixed loopback ports are declared inline per test (`let port = 31_1NN;`).
//! That no two live tests anywhere declare the same one is not a
//! convention re-derived by hand any more — `live_port_uniqueness.rs`
//! (hermetic) enumerates every declaration under `crates/*/tests` and
//! fails if two collide.

#[path = "support/live_db.rs"]
mod live_db;

use live_db::drop_db;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use futures::StreamExt;
use prost::Message;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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
    /// Kept (rather than dropped after dechunking) so error cases can
    /// assert the issue #384 container, `nosniff`'s ABSENCE included.
    headers: HashMap<String, String>,
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
        res.headers.get("content-type").map(String::as_str),
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

    Some(RawResponse {
        status,
        headers,
        body,
    })
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

fn spawn_ready(port: u16, db: &str) -> ChildGuard {
    spawn_ready_env(port, db, &[])
}

/// [`spawn_ready`] with extra environment on top of the baseline (issue
/// #398: `PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES`).
fn spawn_ready_env(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
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
        // Issue #61 (T9): mount the Tempo compat aliases — the reshaping
        // wire-shape assertions below need them; native behavior is
        // unaffected (router-build-time merging only).
        .env("PULSUS_COMPAT_ENDPOINTS", "true");
    for (key, value) in extra_env {
        cmd.env(key, value);
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
    let port = 31_138;
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
    let db = pulsus_testkit::test_db(&format!("pulsus_traces_tags_live_it_{nonce}"));
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
    let scopes = scopes_of(&json, ctx);
    // Issue #475: the static intrinsic scope leads, then the catalog
    // scopes in `(scope, key)` order. The two writer-reserved intrinsic
    // scopes appear nowhere.
    assert_eq!(scopes[0].0, "intrinsic", "{ctx}: body {json}");
    assert_eq!(scopes[0].1.len(), 25, "{ctx}: the served intrinsic names");
    assert_eq!(
        scopes[1..],
        [
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
    let body = assert_error_body(&res, 400, ctx);
    assert!(body.contains("bogus"), "{ctx}: {body:?}");

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

    // -- Issue #61 (T9): the four RESHAPING Tempo aliases against this
    // non-trivial catalog — exact reference wire shapes over real HTTP.
    // Runs BEFORE the names-bulk seed (the small fixture keeps the
    // expected sets exact); every alias request goes through `get_json`,
    // so the exact-count zero-payload proof below covers the aliases too
    // (they issue the identical catalog SELECTs).

    // Route 5, `/api/v2/search/tags`: the native scoped shape with the
    // `truncated` key ABSENT while the native twin carries it.
    let ctx = "alias v2 tags (scoped, no truncated)";
    let native = get_json(port, NAMES_URL, ctx, &mut discovered);
    let alias = get_json(port, "/api/v2/search/tags", ctx, &mut discovered);
    assert_eq!(
        alias["scopes"], native["scopes"],
        "{ctx}: the v2 alias serves the native scoped shape, body {alias}"
    );
    assert!(
        !scopes_of(&alias, ctx).is_empty(),
        "{ctx}: seeded proof must be non-empty, body {alias}"
    );
    assert!(
        native.get("truncated").is_some(),
        "{ctx}: the native twin carries truncated, body {native}"
    );
    assert!(
        alias.get("truncated").is_none(),
        "{ctx}: the alias must drop truncated, body {alias}"
    );

    // Route 6, `/api/v2/search/tag/{tag}/values`: typed {"type","value"}
    // OBJECTS (not v1's bare strings), no `truncated`.
    let ctx = "alias v2 typed values";
    let alias = get_json(
        port,
        "/api/v2/search/tag/span.http.status_code/values",
        ctx,
        &mut discovered,
    );
    assert_eq!(
        values_of(&alias, ctx),
        vec![
            ("int".to_string(), "200".to_string()),
            ("int".to_string(), "500".to_string()),
        ],
        "{ctx}: typed value objects, body {alias}"
    );
    assert!(
        alias["tagValues"][0].is_object(),
        "{ctx}: v2 entries are objects, never bare strings, body {alias}"
    );
    assert!(
        alias.get("truncated").is_none(),
        "{ctx}: the alias must drop truncated, body {alias}"
    );
    // The truncated-drop is observable, not vacuous: on the over-cap key
    // the native flag is TRUE, and the alias still has no key at all.
    let ctx = "alias v2 values truncated-drop (over-cap key)";
    let native = get_json(port, &values_url("span.bulk.id"), ctx, &mut discovered);
    assert_eq!(native["truncated"], true, "{ctx}: body flag");
    let alias = get_json(
        port,
        "/api/v2/search/tag/span.bulk.id/values",
        ctx,
        &mut discovered,
    );
    assert_eq!(
        alias["tagValues"], native["tagValues"],
        "{ctx}: same capped typed set as native"
    );
    assert!(
        alias.get("truncated").is_none(),
        "{ctx}: truncated dropped even when the native flag is true, body keys {:?}",
        alias.as_object().map(|o| o.keys().collect::<Vec<_>>())
    );

    // Route 11, `/api/search/tags`: Tempo v1 FLAT `{"tagNames":[...]}` —
    // distinct keys in catalog (scope, key) order; no scopes, no
    // truncated.
    let ctx = "alias v1 flat tagNames";
    let alias = get_json(port, "/api/search/tags", ctx, &mut discovered);
    assert_eq!(
        alias["tagNames"],
        serde_json::json!([
            "env",
            "service.name",
            "bulk.id",
            "cache.hit",
            "http.status_code",
            "latency.bucket",
        ]),
        "{ctx}: flat bare-string names in catalog order, body {alias}"
    );
    assert!(
        alias.get("scopes").is_none() && alias.get("truncated").is_none(),
        "{ctx}: the flat shape has neither scopes nor truncated, body {alias}"
    );

    // Route 12, `/api/search/tag/{tag}/values`: v1 flat bare STRINGS —
    // the object-vs-string distinction from route 6 on the same seeded
    // data.
    let ctx = "alias v1 flat tagValues";
    let alias = get_json(
        port,
        "/api/search/tag/resource.service.name/values",
        ctx,
        &mut discovered,
    );
    assert_eq!(
        alias["tagValues"],
        serde_json::json!(["checkout", "payments"]),
        "{ctx}: flat bare-string values, body {alias}"
    );
    assert!(
        alias["tagValues"][0].is_string(),
        "{ctx}: v1 entries are bare strings, never typed objects, body {alias}"
    );
    assert!(
        alias.get("truncated").is_none(),
        "{ctx}: no truncated key, body {alias}"
    );
    // Over-cap flat twin: exactly the cap, all bare strings, still no
    // truncated key (native is true).
    let ctx = "alias v1 flat over-cap";
    let alias = get_json(
        port,
        "/api/search/tag/span.bulk.id/values",
        ctx,
        &mut discovered,
    );
    let flat_vals = alias["tagValues"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: tagValues must be an array, body {alias}"));
    assert_eq!(flat_vals.len(), TAG_VALUES_MAX, "{ctx}: exactly the cap");
    assert!(
        flat_vals.iter().all(serde_json::Value::is_string),
        "{ctx}: every capped entry is a bare string"
    );
    assert!(
        alias.get("truncated").is_none(),
        "{ctx}: truncated dropped even on the capped set"
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
    // The cap counts CATALOG pairs only: the leading intrinsic scope is
    // a static list that no catalog read produced, so it is excluded
    // from the sum (issue #475).
    assert_eq!(scopes[0].0, "intrinsic", "{ctx}: the static scope leads");
    let total_pairs: usize = scopes[1..].iter().map(|(_, keys)| keys.len()).sum();
    assert_eq!(total_pairs, TAG_NAMES_MAX, "{ctx}: exactly the cap");
    // Catalog holds 2 resource + (4 + 11,000) span pairs = 11,006; the
    // (scope, key)-ordered cap keeps resource whole and cuts the span
    // list at pair 10,000: bulk.id + bulkkey.00000..bulkkey.09996.
    assert_eq!(
        scopes[1],
        (
            "resource".to_string(),
            vec!["env".to_string(), "service.name".to_string()],
        ),
        "{ctx}: the resource scope survives the cap whole"
    );
    assert_eq!(scopes[2].0, "span", "{ctx}");
    assert_eq!(scopes[2].1.len(), TAG_NAMES_MAX - 2, "{ctx}");
    assert_eq!(scopes[2].1[0], "bulk.id", "{ctx}: ascending key order");
    assert_eq!(
        scopes[2].1.last().map(String::as_str),
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

// =====================================================================
// Issue #475: the colliding corpus, the static answers, and the
// zero-delta proof that a static answer reads no trace table.
//
// A CLEAN corpus cannot tell "bypass the catalog" from "add the static
// list on top of it" — both give the right bytes. This corpus therefore
// carries a user attribute keyed `status`, a span event named the same
// thing as a span, and a link attribute keyed `spanID` that collides
// with a reserved intrinsic key.
// =====================================================================

/// The 25 names the `intrinsic` scope serves, as a LITERAL typed into
/// this suite — an independent copy from `intrinsics.rs`'s, so a wrong
/// list has to be typed wrong twice.
const INTRINSIC_NAMES: [&str; 25] = [
    "duration",
    "event:name",
    "event:timeSinceStart",
    "instrumentation:name",
    "instrumentation:version",
    "kind",
    "link:spanID",
    "link:traceID",
    "name",
    "rootName",
    "rootServiceName",
    "span:duration",
    "span:id",
    "span:kind",
    "span:name",
    "span:parentID",
    "span:status",
    "span:statusMessage",
    "status",
    "statusMessage",
    "trace:duration",
    "trace:id",
    "trace:rootName",
    "trace:rootService",
    "traceDuration",
];

const KIND_VALUES: [&str; 6] = [
    "unspecified",
    "internal",
    "server",
    "client",
    "producer",
    "consumer",
];

/// A 200-asserting request with NO bookkeeping. This test carries no
/// request counter: nothing it asserts depends on classifying a request
/// as counted or uncounted, which is what made the earlier form of this
/// gate agreeable to a wrong implementation.
fn get_ok_json(port: u16, path: &str, ctx: &str) -> serde_json::Value {
    let res = get(port, path, ctx);
    assert_eq!(
        res.status,
        200,
        "{ctx}: must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    res.json(ctx)
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SettingRow {
    value: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct QueryTablesRow {
    query: String,
    tables: Vec<String>,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CatalogRow {
    scope: String,
    key: String,
    val: String,
}

/// Step (0) of the zero-delta gate: a NAMED PRECONDITION, not a property
/// the gate proves.
///
/// Per-query `log_queries=0` erases the gate's input before any
/// predicate runs, so no predicate can recover it. Asserting the
/// precondition turns a zero delta computed from an empty log into a
/// named failure. What it covers: this connection's effective setting —
/// a container profile, a user profile, a connection-level setting. What
/// it does not: a per-query override the server applies to its own
/// reads, which the positive control catches for the one settings root
/// those reads share, and which nothing here catches for a root used
/// only by the batch-window reads.
async fn assert_query_logging_is_on(admin: &ChClient) {
    let mut stream = admin
        .query_stream::<SettingRow>(
            "SELECT toString(value) AS value FROM system.settings WHERE name = 'log_queries'",
            &QuerySettings::new(),
        )
        .await
        .expect("read log_queries");
    let mut value = None;
    while let Some(row) = stream.next().await {
        value = Some(row.expect("decode setting row").value);
    }
    let value = value.expect("system.settings has a log_queries row");
    assert_eq!(
        value, "1",
        "precondition failed: query logging is off for this session (log_queries = {value}); \
         the zero-delta gate cannot run"
    );
}

/// `SYSTEM FLUSH LOGS`, then the number of finished `Select`s whose
/// `tables` array names any `<db>.trace_*` object.
///
/// The counted set is defined by the table read and by nothing else. The
/// run's identity lives in `db` itself — `pulsus_testkit::test_db`
/// composes a per-checkout prefix with a millisecond nonce — so no
/// `current_database` condition is used or wanted: a completed read
/// issued through a client whose session database is something else must
/// still be counted. (That is not the reasoning the older test's comment
/// gives for its own identity, and it is deliberately different.)
async fn flush_and_count_trace_reads(admin: &ChClient, db: &str) -> u64 {
    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    let sql = format!(
        "SELECT toUInt64(count()) AS n FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND arrayExists(t -> startsWith(t, '{db}.trace_'), arrayMap(x -> toString(x), tables))"
    );
    let mut stream = admin
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("count trace reads");
    let mut n = 0;
    while let Some(row) = stream.next().await {
        n = row.expect("decode count row").n;
    }
    n
}

/// Only used to build a panic message: `query` and `tables` for every
/// row the counting predicate matches.
async fn trace_read_rows(admin: &ChClient, db: &str) -> Vec<(String, Vec<String>)> {
    let sql = format!(
        "SELECT query, arraySort(arrayMap(x -> toString(x), tables)) AS tables \
         FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND arrayExists(t -> startsWith(t, '{db}.trace_'), arrayMap(x -> toString(x), tables))"
    );
    let mut stream = admin
        .query_stream::<QueryTablesRow>(&sql, &QuerySettings::new())
        .await
        .expect("read matching rows");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode row");
        out.push((row.query, row.tables));
    }
    out
}

/// The catalog rows the colliding corpus produced, ascending.
async fn catalog_rows(admin: &ChClient, db: &str) -> Vec<(String, String, String)> {
    let sql = format!(
        "SELECT DISTINCT scope, key, val FROM {db}.trace_tag_catalog ORDER BY scope, key, val"
    );
    let mut stream = admin
        .query_stream::<CatalogRow>(&sql, &QuerySettings::new())
        .await
        .expect("read catalog");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode catalog row");
        out.push((row.scope, row.key, row.val));
    }
    out
}

/// Ingests the colliding corpus over `POST /v1/traces`: one resource,
/// one instrumentation scope, three spans, one span event, one span
/// link.
fn ingest_colliding_corpus(port: u16, base_ns: u64) {
    let mut checkout = span(
        1,
        1,
        "checkout",
        base_ns,
        vec![kv_str("status", "degraded")],
    );
    let mut exception = span(1, 2, "exception", base_ns + 1_000, vec![]);
    exception.events = vec![Event {
        time_unix_nano: base_ns + 1_000 + 5_000_000,
        name: "exception".to_string(),
        attributes: vec![kv_str("kind", "retry")],
        dropped_attributes_count: 0,
    }];
    let mut payment = span(1, 3, "payment", base_ns + 2_000, vec![]);
    payment.links = vec![Link {
        trace_id: {
            let mut t = [0u8; 16];
            t[15] = 0x07;
            t.to_vec()
        },
        span_id: vec![0, 0, 0, 0, 0, 0, 0, 0x63],
        trace_state: String::new(),
        attributes: vec![kv_str("spanID", "from-attribute")],
        dropped_attributes_count: 0,
        flags: 0,
    }];
    checkout.kind = 0;

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    kv_str("service.name", "checkout"),
                    kv_str("name", "resource-name-attr"),
                    kv_str("empty.attr", ""),
                ],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "checkout-lib".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: vec![kv_str("sdk", "rust")],
                    dropped_attributes_count: 0,
                }),
                spans: vec![checkout, exception, payment],
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
    .unwrap_or_else(|| panic!("colliding corpus: ingest must be reachable"));
    assert_eq!(
        res.status,
        200,
        "colliding corpus: sync ingest must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
}

/// Issue #475: the intrinsic scope, the static `status`/`kind` values,
/// and the scope discipline — against a corpus built so that a wrong
/// implementation and a right one give DIFFERENT bytes.
#[tokio::test(flavor = "multi_thread")]
async fn intrinsic_discovery_answers_from_the_vocabulary_and_reads_no_trace_table() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_290;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let db = pulsus_testkit::test_db(&format!("pulsus_traces_intrinsics_live_it_{nonce}"));
    let db = db.as_str();
    drop_db(db).await;
    let _guard = spawn_ready(port, db);

    ingest_colliding_corpus(port, 1_700_000_000_000_000_000);

    let admin = ChClient::new(ch_config()).await.expect("connect admin");

    // -- Step (0): the gate's named precondition. ------------------------
    assert_query_logging_is_on(&admin).await;

    // -- The corpus really produced the colliding rows. ------------------
    // This is what makes every assertion below discriminating rather
    // than vacuous: without the `("span","status","degraded")` row a
    // catalog-reading implementation would answer `status` empty, which
    // is indistinguishable from a bypass on bytes alone.
    let rows = catalog_rows(&admin, db).await;
    assert_eq!(
        rows,
        vec![
            ("event".to_string(), "kind".to_string(), "retry".to_string()),
            (
                "event:intrinsic".to_string(),
                "name".to_string(),
                "exception".to_string()
            ),
            (
                "event:intrinsic".to_string(),
                "timeSinceStart".to_string(),
                "5000000".to_string()
            ),
            (
                "instrumentation".to_string(),
                "sdk".to_string(),
                "rust".to_string()
            ),
            (
                "link".to_string(),
                "spanID".to_string(),
                "from-attribute".to_string()
            ),
            (
                "link:intrinsic".to_string(),
                "spanID".to_string(),
                "0000000000000063".to_string()
            ),
            (
                "link:intrinsic".to_string(),
                "traceID".to_string(),
                "00000000000000000000000000000007".to_string()
            ),
            (
                "resource".to_string(),
                "empty.attr".to_string(),
                "".to_string()
            ),
            (
                "resource".to_string(),
                "name".to_string(),
                "resource-name-attr".to_string()
            ),
            (
                "resource".to_string(),
                "service.name".to_string(),
                "checkout".to_string()
            ),
            (
                "span".to_string(),
                "status".to_string(),
                "degraded".to_string()
            ),
        ],
        "the colliding corpus must produce exactly these catalog rows"
    );

    // -- The catalog-reading answers, which the statics must beat. -------
    let ctx = "scoped attribute lookup still reaches the colliding attribute";
    let json = get_ok_json(port, "/api/v2/search/tag/span.status/values", ctx);
    assert_eq!(
        json,
        serde_json::json!({"tagValues": [{"type": "string", "value": "degraded"}]}),
        "{ctx}"
    );

    let ctx = "a leading dot is an unscoped attribute lookup";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/.status/values", ctx),
        serde_json::json!({"tagValues": [{"type": "string", "value": "degraded"}]}),
        "{ctx}"
    );

    let ctx = "a bare reserved KEY is an attribute lookup, and the reserved scope is excluded";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/spanID/values", ctx),
        serde_json::json!({"tagValues": [{"type": "string", "value": "from-attribute"}]}),
        "{ctx}: the link:intrinsic row 0000000000000063 sorts first and must not appear"
    );

    let ctx = "the dot form of a reserved key reaches the attribute";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/link.spanID/values", ctx),
        serde_json::json!({"tagValues": [{"type": "string", "value": "from-attribute"}]}),
        "{ctx}"
    );

    let ctx = "the event scope prefix resolves";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/event.kind/values", ctx),
        serde_json::json!({"tagValues": [{"type": "string", "value": "retry"}]}),
        "{ctx}"
    );

    let ctx = "the instrumentation scope prefix resolves";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/instrumentation.sdk/values", ctx),
        serde_json::json!({"tagValues": [{"type": "string", "value": "rust"}]}),
        "{ctx}"
    );

    let ctx = "an empty attribute value omits the value key";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/resource.empty.attr/values", ctx),
        serde_json::json!({"tagValues": [{"type": "string"}]}),
        "{ctx}"
    );
    let ctx = "the v1 flat projection still emits the empty string";
    assert_eq!(
        get_ok_json(port, "/api/search/tag/resource.empty.attr/values", ctx),
        serde_json::json!({"tagValues": [""]}),
        "{ctx}"
    );
    let ctx = "the v1 flat values route keeps the attribute-only reading";
    assert_eq!(
        get_ok_json(port, "/api/search/tag/status/values", ctx),
        serde_json::json!({"tagValues": ["degraded"]}),
        "{ctx}: v1 answers `status` from the store, not from the statics"
    );

    let ctx = "the unscoped listing carries the intrinsic scope and no reserved scope";
    let json = get_ok_json(port, "/api/v2/search/tags", ctx);
    let scopes = scopes_of(&json, ctx);
    let names: Vec<&str> = scopes.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "intrinsic",
            "event",
            "instrumentation",
            "link",
            "resource",
            "span"
        ],
        "{ctx}: body {json}"
    );
    assert_eq!(scopes[0].1, INTRINSIC_NAMES, "{ctx}");
    assert_eq!(scopes[1].1, ["kind"], "{ctx}");
    assert_eq!(scopes[2].1, ["sdk"], "{ctx}");
    assert_eq!(scopes[3].1, ["spanID"], "{ctx}");
    assert_eq!(scopes[4].1, ["empty.attr", "name", "service.name"], "{ctx}");
    assert_eq!(scopes[5].1, ["status"], "{ctx}");

    let ctx = "the v1 flat listing carries the catalog keys only";
    assert_eq!(
        get_ok_json(port, "/api/search/tags", ctx),
        serde_json::json!({"tagNames": [
            "kind", "sdk", "spanID", "empty.attr", "name", "service.name", "status"
        ]}),
        "{ctx}: no intrinsic names on the unscoped flat route"
    );

    let ctx = "an attribute-scoped request is not given the intrinsic scope";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tags?scope=span", ctx),
        serde_json::json!({"scopes": [{"name": "span", "tags": ["status"]}]}),
        "{ctx}"
    );

    // -- The rejection surface, which the widened accept list must keep. -
    for value in ["bogus", "Intrinsic", "TRACE", "trace%20"] {
        let ctx = format!("scope={value} rejects");
        let res = get(port, &format!("/api/v2/search/tags?scope={value}"), &ctx);
        assert_error_body(&res, 400, &ctx);
    }
    for path in [
        "/api/traces/v1/tag/resource./values",
        "/api/v2/search/tag/span./values",
        "/api/v2/search/tag/event./values",
    ] {
        let ctx = format!("{path} rejects an empty key");
        let res = get(port, path, &ctx);
        let body = assert_error_body(&res, 400, &ctx);
        assert_eq!(
            body, "invalid tag: the attribute key must be non-empty",
            "{ctx}"
        );
    }

    // =================================================================
    // The zero-delta gate.
    //
    // What it asserts, and nothing more: during the window between two
    // `SYSTEM FLUSH LOGS` calls, `system.query_log` holds no
    // `QueryFinish` row with `query_kind = 'Select'` any of whose
    // `tables` entries begins `<db>.trace_`. Every object the traces
    // schema declares is under that prefix.
    //
    // WHAT THAT COVERS, and why the coverage is exact rather than
    // hopeful. Every tag-discovery read the server can issue is built by
    // `tag_names_sql` or `tag_values_sql`
    // (`crates/pulsus-read/src/traces/tags_sql.rs`). Both open with
    // `FROM {CATALOG_TABLE}` against a PRIVATE module constant, and the
    // only caller-supplied strings they accept land inside `WHERE`. So a
    // builder-emitted query always names the table in a top-level `FROM`,
    // and a top-level `FROM` on a real table always populates `tables` —
    // which is why this predicate sees every read those two functions can
    // produce, including one whose result is discarded.
    //
    // WHAT IT DOES NOT COVER — a DELIBERATE LIMIT, not an oversight, and
    // recorded here so the next reader finds a decision instead of
    // rediscovering a hole:
    //
    //   1. A handler that constructs its OWN `ChClient` and its OWN query
    //      text — calling neither builder, never reaching `engine_for`,
    //      never entering `pulsus-read`. Measured in code review (round 1
    //      on this suite): eight completed `SELECT count() FROM
    //      <db>.trace_tag_catalog` reads issued that way finished with
    //      `current_database = default` and an EMPTY `tables` array, and
    //      this gate passed. That is a real read this predicate cannot
    //      see.
    //   2. A read of a table that is not `<db>.trace_*`.
    //   3. A query that never reached `QueryFinish` — one that errored or
    //      was cancelled. Such a request also fails its body assertion,
    //      so it surfaces there instead.
    //   4. A read whose `tables` array does not name the table (a scalar
    //      subquery folds to `system.one`). Not reachable through either
    //      builder since the catalog name became a module constant.
    //   5. Work that is not a ClickHouse query at all — an in-process
    //      cache, a value computed at startup, a read completed before
    //      the window opens; and anything outside the two windows.
    //
    // WHY LIMIT 1 IS ACCEPTED RATHER THAN CLOSED. Reaching it requires
    // SQL neither builder can emit, which is the case this issue's plan
    // named in advance as recorded-and-reported rather than as another
    // hardening round. The escape that DID matter — a caller passing a
    // query fragment where the table name belonged — was closed in
    // production instead, by deleting the parameter: see `CATALOG_TABLE`.
    //
    // DO NOT close limit 1 by excluding this gate's own reads on their
    // QUERY TEXT. That is the same defect the limit describes: a claim
    // about WHO ISSUED a query, tested against WHAT THE QUERY SAYS. It
    // would have to inherit every spelling of this file's own SQL across
    // the five sites that issue it, it turns into a false positive the
    // moment one of them is reformatted, and the natural repair is to
    // widen the pattern — which then exempts a production read whose text
    // happens to match.
    //
    // THE DURABLE FIX, recorded uncosted and unscheduled for whoever
    // picks it up. The subject of the claim is "a store read happened";
    // the predicate reads a table list, which is a proper subset of that.
    // Keying on PROVENANCE rather than on text would let the predicate
    // state the claim actually being made — every `Select` on this
    // database that this gate did not issue — through a per-connection
    // `log_comment`, `initial_query_id`, or a dedicated ClickHouse user
    // for the admin connection. Any of the three identifies the issuer
    // directly, so no query text enters the check at all.
    // =================================================================

    // (1) baseline.
    let b0 = flush_and_count_trace_reads(&admin, db).await;
    // (2) the positive control: one real catalog read.
    let ctx = "positive control";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/span.status/values", ctx),
        serde_json::json!({"tagValues": [{"type": "string", "value": "degraded"}]}),
        "{ctx}"
    );
    // (3) it moved the count by exactly one.
    let a0 = flush_and_count_trace_reads(&admin, db).await;
    assert_eq!(
        a0,
        b0 + 1,
        "the positive control must move the count by exactly one — if it does not, the \
         predicate is vacuous and the zero below proves nothing. Rows: {:#?}",
        trace_read_rows(&admin, db).await
    );

    // (4) the static window opens. Nothing else runs between here and (6).
    let b1 = flush_and_count_trace_reads(&admin, db).await;

    let intrinsic_scope_body = serde_json::json!({
        "scopes": [{"name": "intrinsic", "tags": INTRINSIC_NAMES}]
    });
    let status_values = serde_json::json!({"tagValues": [
        {"type": "keyword", "value": "ok"},
        {"type": "keyword", "value": "error"},
        {"type": "keyword", "value": "unset"},
    ]});
    let kind_values = serde_json::json!({
        "tagValues": KIND_VALUES
            .iter()
            .map(|v| serde_json::json!({"type": "keyword", "value": v}))
            .collect::<Vec<_>>()
    });
    let empty_values = serde_json::json!({"tagValues": []});

    // B1: the exact request the Search tab's Status field issues,
    // redundant `tag=` query parameter included.
    let ctx = "B1 status values";
    assert_eq!(
        get_ok_json(
            port,
            "/api/v2/search/tag/status/values?limit=5000&tag=status",
            ctx
        ),
        status_values,
        "{ctx}: the closed keyword set, typed keyword — never the colliding `degraded`"
    );
    // B2: the native twin carries `truncated`.
    let ctx = "B2 native status values";
    let mut native_status = status_values.clone();
    native_status["truncated"] = serde_json::json!(false);
    assert_eq!(
        get_ok_json(port, "/api/traces/v1/tag/status/values", ctx),
        native_status,
        "{ctx}"
    );
    // B3-B5: `kind`, and its colon-scoped spelling encoded and not. The
    // corpus holds an event attribute literally keyed `kind` whose value
    // is `retry`; none of these may return it.
    let ctx = "B3 kind values";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/kind/values", ctx),
        kind_values,
        "{ctx}"
    );
    let ctx = "B4 span:kind values";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/span:kind/values", ctx),
        kind_values,
        "{ctx}"
    );
    let ctx = "B5 span%3Akind values";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tag/span%3Akind/values", ctx),
        kind_values,
        "{ctx}: the path extractor percent-decodes before the resolver sees it"
    );
    // B6-B9: the open-valued intrinsics answer EMPTY rather than falling
    // through to the catalog. `name` is the one that used to return span
    // EVENT names; `link:spanID` is one character from `link.spanID`,
    // which answers `from-attribute` above.
    for (ctx, path) in [
        ("B6 name values", "/api/v2/search/tag/name/values"),
        (
            "B7 link:spanID values",
            "/api/v2/search/tag/link:spanID/values",
        ),
        ("B8 duration values", "/api/v2/search/tag/duration/values"),
        (
            "B9 nestedSetLeft values",
            "/api/v2/search/tag/nestedSetLeft/values",
        ),
    ] {
        assert_eq!(get_ok_json(port, path, ctx), empty_values, "{ctx}");
    }
    // B10-B12: `scope=intrinsic` on all three names routes.
    let ctx = "B10 v2 scope=intrinsic";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tags?scope=intrinsic", ctx),
        intrinsic_scope_body,
        "{ctx}"
    );
    let ctx = "B11 native scope=intrinsic";
    let mut native_intrinsic = intrinsic_scope_body.clone();
    native_intrinsic["truncated"] = serde_json::json!(false);
    assert_eq!(
        get_ok_json(port, "/api/traces/v1/tags?scope=intrinsic", ctx),
        native_intrinsic,
        "{ctx}"
    );
    let ctx = "B12 v1 flat scope=intrinsic";
    assert_eq!(
        get_ok_json(port, "/api/search/tags?scope=intrinsic", ctx),
        serde_json::json!({"tagNames": INTRINSIC_NAMES}),
        "{ctx}"
    );
    // B13-B15: `scope=trace` is accepted and answers an empty list on
    // all three names routes. This is the row whose RIGHT BYTES a wrong
    // implementation also produces — appending `trace` to `ATTR_SCOPES`
    // would answer identically while issuing three catalog reads, and
    // only the delta below sees it.
    let ctx = "B13 v2 scope=trace";
    assert_eq!(
        get_ok_json(port, "/api/v2/search/tags?scope=trace", ctx),
        serde_json::json!({"scopes": []}),
        "{ctx}"
    );
    let ctx = "B14 native scope=trace";
    assert_eq!(
        get_ok_json(port, "/api/traces/v1/tags?scope=trace", ctx),
        serde_json::json!({"scopes": [], "truncated": false}),
        "{ctx}"
    );
    let ctx = "B15 v1 flat scope=trace";
    assert_eq!(
        get_ok_json(port, "/api/search/tags?scope=trace", ctx),
        serde_json::json!({"tagNames": []}),
        "{ctx}"
    );

    // (6) the window closes: not one of those fifteen requests read a
    // trace table.
    let a1 = flush_and_count_trace_reads(&admin, db).await;
    assert_eq!(
        a1,
        b1,
        "a static tag-discovery answer must read no trace table; the window moved the count \
         from {b1} to {a1}. Rows: {:#?}",
        trace_read_rows(&admin, db).await
    );

    drop_db(db).await;
}

// ---------------------------------------------------------------------
// Issue #398 — the TraceQL half. Measured at 051fd8a with a ClickHouse
// user profile pinning `max_memory_usage = 1000000`:
// `GET /api/search/tag/{tag}/values` answered **500** with the raw server
// exception in the body — `clickhouse: server [241]: Code: 241.
// DB::Exception: Memory limit (for query) exceeded: would use 9.51 MiB …
// maximum: 976.56 KiB.`
//
// That read comes from `traces::exec::catalog_settings`, which is a
// DELIBERATELY INDEPENDENT settings root from `search_settings` (it omits
// the clustered-reader block on purpose). Adding the ceiling to
// `search_settings` alone — the obvious root — would leave this endpoint
// exactly as it was.
// ---------------------------------------------------------------------

/// Catalog rows seeded for the memory-breach fixture.
///
/// **Sizing, measured through this test at `PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES
/// = 1024` — on the two routes below, not on a SQL statement in isolation.**
/// Both routes answer 200 at 0, 1, 10, 100, 500, 1 000, 5 000, 10 000, 20 000
/// and 30 000 rows, and 422 at 40 000, 50 000, 100 000 and 200 000. The
/// threshold is therefore between 30 000 and 40 000 — an order of magnitude
/// above the LogQL fixture's, because this is one `SELECT DISTINCT` over a
/// single freshly written part with no other read in front of it.
///
/// 200 000 is 5x the measured threshold. The previous value, 50 000, sat only
/// 1.25x above it, which is too thin to leave unstated for a CI gate. It costs
/// nothing to raise: the test runs in 0.77 s at 50 000 and 0.81 s at 200 000,
/// because the seed is one server-side `INSERT … FROM numbers()`.
const MEM_CATALOG_ROWS: u64 = 200_000;

/// Issue #398 AC T2: a TraceQL catalog read that breaches the per-query
/// memory ceiling answers **422**, on the native route and on the Tempo
/// alias that produced the measured 500.
#[tokio::test(flavor = "multi_thread")]
async fn trace_tag_values_memory_breach_is_422() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_129;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let db = pulsus_testkit::test_db(&format!("pulsus_traces_tags_mem_it_{nonce}"));
    let db = db.as_str();
    drop_db(db).await;
    let _guard = spawn_ready_env(
        port,
        db,
        &[("PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES", "1024")],
    );

    // Seed the catalog directly (the MV path is covered by the suite
    // above; this fixture is about the READ's budget, not ingest).
    let mut cfg = ch_config();
    cfg.database = db.to_string();
    let client = ChClient::new(cfg).await.expect("connect data client");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.trace_tag_catalog (scope, key, val) \
                 SELECT 'span', 'bulk.id', concat('v', leftPad(toString(number), 9, '0')) \
                 FROM numbers({MEM_CATALOG_ROWS})"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed trace_tag_catalog");

    let mut wrong: Vec<String> = Vec::new();
    for path in [
        "/api/traces/v1/tag/span.bulk.id/values",
        // The Tempo alias the 500 was measured on.
        "/api/search/tag/span.bulk.id/values",
    ] {
        let res = get(port, path, "memory breach");
        let body = String::from_utf8_lossy(&res.body).into_owned();
        if res.status != 422 {
            wrong.push(format!("{path} -> {} {body}", res.status));
            continue;
        }
        if !body.contains("reader.traceql_read_max_memory_bytes") {
            wrong.push(format!("{path} -> 422 without the knob name: {body}"));
        }
        if body.contains("DB::Exception") || body.contains("official build") {
            wrong.push(format!("{path} -> 422 leaking the exception: {body}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "a catalog memory breach must be 422 with our own message:\n{}",
        wrong.join("\n")
    );

    drop_db(db).await;
}

/// The direction-neutral twin of `trace_tag_values_memory_breach_is_422`:
/// at the shipped 8 GiB default the SAME corpus and the SAME routes answer
/// **200**. Without it, a change that 422'd every catalog read would pass
/// the discriminator above.
#[tokio::test(flavor = "multi_thread")]
async fn the_default_trace_memory_ceiling_does_not_refuse_a_catalog_read() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_130;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let db = pulsus_testkit::test_db(&format!("pulsus_traces_tags_mem_ok_it_{nonce}"));
    let db = db.as_str();
    drop_db(db).await;
    let _guard = spawn_ready(port, db);

    let mut cfg = ch_config();
    cfg.database = db.to_string();
    let client = ChClient::new(cfg).await.expect("connect data client");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.trace_tag_catalog (scope, key, val) \
                 SELECT 'span', 'bulk.id', concat('v', leftPad(toString(number), 9, '0')) \
                 FROM numbers({MEM_CATALOG_ROWS})"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed trace_tag_catalog");

    for path in [
        "/api/traces/v1/tag/span.bulk.id/values",
        "/api/search/tag/span.bulk.id/values",
    ] {
        let res = get(port, path, "default ceiling");
        assert_eq!(
            res.status,
            200,
            "{path}: {}",
            String::from_utf8_lossy(&res.body)
        );
    }

    drop_db(db).await;
}
