//! Issue #478, our leg: what PulsusDB answers for every case in
//! `tests/fixtures/reference-tag-values.json`, end to end over HTTP
//! against a live ClickHouse.
//!
//! The reference's half of the same fixture is replayed by
//! `trace_tag_values_differential.rs`. **Neither leg compares itself to
//! the other**: both compare to the same committed artifact, so a driver
//! that mangled a value fails its own leg rather than agreeing with the
//! opposite one.
//!
//! What this suite asserts, in the order the tests run:
//!
//! - every `q_matrix` case's EXACT ordered value list — our order is
//!   asserted everywhere because we sort, where the reference's v2 order
//!   is not stable and the differential leg compares it as a multiset;
//! - the range contract: seven fault shapes on the values routes, each
//!   `400 text/plain; charset=utf-8` with our exact body, and the two
//!   accepting shapes `200`;
//! - the span-name typing corpus, including the over-cap name;
//! - the window bound, as an occupied-day / empty-day PAIR with a
//!   `system.query_log` assertion that the span read actually ran — an
//!   empty list alone cannot tell a measured absence from a static one;
//! - and, in its own test, the BOUNDARY of the `q` tolerance: which
//!   inputs are rejected below the interpretation layer, and with which
//!   status. The tolerance is not absolute and stating it absolutely
//!   invites a later reader to file two correct rejections as
//!   regressions.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test traces_tag_values_narrow_live
//! ```

#[path = "support/tag_values_corpus.rs"]
mod corpus;
#[path = "support/live_db.rs"]
mod live_db;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use futures::StreamExt;
use prost::Message;
use serde_json::Value;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use serde::{Deserialize, Serialize};

use live_db::drop_db;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// Raw HTTP over loopback — the same idiom the other live suites use.
// ---------------------------------------------------------------------

struct RawResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl RawResponse {
    fn json(&self, ctx: &str) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "{ctx}: body is not JSON ({e}): {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let Some(eol) = find_subslice(raw, b"\r\n") else {
            break;
        };
        let size =
            usize::from_str_radix(String::from_utf8_lossy(&raw[..eol]).trim(), 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        let start = eol + 2;
        let end = start + size;
        if end > raw.len() {
            break;
        }
        out.extend_from_slice(&raw[start..end]);
        raw = &raw[(end + 2).min(raw.len())..];
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
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
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

/// A GET whose request TARGET is raw bytes rather than a `&str`, so a
/// sequence that is not valid UTF-8 can be put on the wire. `request`
/// above cannot express that — its path is a `&str` — and the whole
/// point of the case is what happens before any handler sees it.
///
/// Returns the status line's code alone: these responses have no body.
fn raw_target_status(port: u16, target: &[u8], ctx: &str) -> u16 {
    let mut stream =
        TcpStream::connect(("127.0.0.1", port)).unwrap_or_else(|e| panic!("{ctx}: connect: {e}"));
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    let mut req = b"GET ".to_vec();
    req.extend_from_slice(target);
    req.extend_from_slice(b" HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(&req)
        .unwrap_or_else(|e| panic!("{ctx}: write: {e}"));
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .unwrap_or_else(|e| panic!("{ctx}: read: {e}"));
    let head = String::from_utf8_lossy(&buf);
    head.split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or_else(|| panic!("{ctx}: no status line in {head:?}"))
}

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

/// Spawns the real binary and polls `GET /ready` until it answers `200`.
///
/// **The `503 Service Unavailable` lines this prints are the readiness
/// loop working, not a fault.** Until the schema is applied and the pool
/// is published the server answers every poll `503`, and `tower_http`
/// logs each one at `ERROR` level; a run therefore emits a handful of
/// them and then `clickhouse schema ready; pool established`. A reader
/// meeting that log unprepared reads a broken suite. Requests must still
/// be issued only after this function RETURNS: one issued earlier gets
/// the same `503` without reaching ClickHouse, so its request class is
/// silently never exercised and any query-log count taken over it is
/// short rather than wrong.
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

fn push(
    port: u16,
    req: &opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    ctx: &str,
) {
    let res = request(
        port,
        "POST",
        "/v1/traces",
        Some(("application/x-protobuf", &req.encode_to_vec())),
    )
    .unwrap_or_else(|| panic!("{ctx}: ingest must be reachable"));
    assert_eq!(res.status, 200, "{ctx}: ingest, body {}", res.text());
}

// ---------------------------------------------------------------------
// The fixture.
// ---------------------------------------------------------------------

fn fixture() -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/reference-tag-values.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("fixture is valid JSON")
}

/// The `(type, value)` pairs of a body, IN ORDER — ours is asserted as an
/// order, not a set.
fn entries(body: &Value) -> Vec<(String, String)> {
    body["tagValues"]
        .as_array()
        .unwrap_or_else(|| panic!("no tagValues in {body}"))
        .iter()
        .map(|e| match e {
            Value::String(s) => ("string".to_string(), s.clone()),
            _ => (
                e["type"].as_str().unwrap_or_default().to_string(),
                e["value"].as_str().unwrap_or_default().to_string(),
            ),
        })
        .collect()
}

/// A fixture answer's expected entries.
///
/// Two element shapes, because the two ROUTE shapes differ: a `[type,
/// value]` pair for the typed routes, and a bare string for the v1 flat
/// route, whose body carries bare strings on both sides. `entries` above
/// reads a bare string out of a RESPONSE as `string`-typed; this is the
/// same rule on the expectation side.
fn expected(answer: &Value) -> Vec<(String, String)> {
    answer["values"]
        .as_array()
        .expect("values")
        .iter()
        .map(|p| match p {
            Value::String(v) => ("string".to_string(), v.clone()),
            _ => {
                let a = p.as_array().expect("pair");
                (
                    a[0].as_str().unwrap_or_default().to_string(),
                    a[1].as_str().unwrap_or_default().to_string(),
                )
            }
        })
        .collect()
}

fn urlencode(s: &str) -> String {
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

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct CountRow {
    n: u64,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct TextRow {
    s: String,
}

/// One `String` column, as a `Vec` — used for the issue #509
/// received-literal scan and for reading the server clock.
async fn text_column(admin: &ChClient, sql: &str) -> Vec<String> {
    let mut stream = admin
        .query_stream::<TextRow>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode text row").s);
    }
    out
}

/// How many finished Selects in `db` match `prefix` and read exactly
/// `table`.
async fn selects_reading(admin: &ChClient, db: &str, prefix: &str, table: &str) -> u64 {
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
           AND current_database = '{db}' \
           AND query LIKE '{prefix}%' \
           AND has(arrayMap(x -> toString(x), tables), '{db}.{table}')"
    );
    let mut stream = admin
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_log read");
    let mut n = 0;
    while let Some(row) = stream.next().await {
        n = row.expect("decode count row").n;
    }
    n
}

/// Issue #509: the key literals ClickHouse must have RECEIVED for the
/// four discriminating cases — the even run, the odd run, the `?fields`
/// shape and the key that holds nothing.
const QM_RECEIVED_LITERALS: [&str; 4] = [
    "key = 'a??b'",
    "key = 'a???b'",
    "key = 'a?fields'",
    "key = 'nosuchkey'",
];

/// The three literals the driver produced when the key was not escaped.
/// The first is the even-run collapse — `a??b` asked for, `a?b`
/// received. The other two are the `?fields` rewrite: the driver
/// substitutes the derive-generated column list, so the wrong SQL is
/// VALID SQL and ClickHouse answers it happily with nothing.
const QM_FORBIDDEN_LITERALS: [&str; 3] = [
    "key = 'a?b'",
    "key = 'a`val`,`val_type`'",
    "key = '`val`,`val_type`'",
];

/// The byte-frozen prefix of the span-name read (`tags_sql`'s own golden
/// pins the whole statement).
const SPAN_NAME_SELECT_PREFIX: &str = "SELECT DISTINCT if(length(name)";

#[tokio::test(flavor = "multi_thread")]
async fn our_answers_match_the_committed_fixture() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_217;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let db = pulsus_testkit::test_db(&format!("pulsus_tag_narrow_it_{nonce}"));
    let db = db.as_str();
    drop_db(db).await;
    let _guard = spawn_ready(port, db);

    let base = corpus::base_ns();
    let start = (base / 1_000_000_000) as i64 - 3_600;
    let end = (base / 1_000_000_000) as i64 + 600;
    let window = format!("start={start}&end={end}");
    push(port, &corpus::c10_request(base), "C10");

    let fx = fixture();

    // ---- the q matrix, exact and ordered ----------------------------
    let mut checked = 0usize;
    for (id, case) in fx["q_matrix"].as_object().expect("q_matrix") {
        let route = case["route"].as_str().expect("route");
        let mut path = if case.get("params").is_some() {
            // The zero-width window case, issued at the CORPUS instant
            // rather than at `start` — see
            // `corpus::zero_width_probe_secs`, and the hermetic sweep
            // `the_zero_width_probe_lands_on_the_corpus_day` below.
            let at = corpus::zero_width_probe_secs(base);
            format!("{route}?start={at}&end={at}")
        } else {
            format!("{route}?{window}")
        };
        if let Some(q) = case["q"].as_str() {
            path.push_str(&format!("&q={}", urlencode(q)));
        }
        let res = get(port, &path, id);
        assert_eq!(res.status, 200, "{id}: {path} — body {}", res.text());
        let body = res.json(id);
        assert_eq!(
            entries(&body),
            expected(&case["pulsus"]),
            "{id}: {path} — body {body}"
        );
        if let Some(want) = case["pulsus"]["truncated"].as_bool() {
            assert_eq!(
                body["truncated"].as_bool(),
                Some(want),
                "{id}: the native route carries `truncated`, body {body}"
            );
        } else {
            assert!(
                body.get("truncated").is_none(),
                "{id}: an alias body carries no `truncated` key, body {body}"
            );
        }
        checked += 1;
    }
    assert_eq!(checked, 46, "every fixture q-matrix case must be issued");

    // ---- the range contract -----------------------------------------
    for (id, case) in fx["range_faults"].as_object().expect("range_faults") {
        let route = case["route"].as_str().expect("route");
        let shape = case["shape"].as_str().expect("shape");
        let query = match shape {
            "malformed_start" => format!("start=abc&end={end}"),
            "malformed_end" => format!("start={start}&end=abc"),
            "half_start" => format!("start={start}"),
            "half_end" => format!("end={end}"),
            "zero_start" => format!("start=0&end={end}"),
            "zero_end" => format!("start={start}&end=0"),
            "inverted" => format!("start={end}&end={start}"),
            other => panic!("{id}: unknown shape {other}"),
        };
        let res = get(port, &format!("{route}?{query}"), id);
        assert_eq!(res.status, 400, "{id}: body {}", res.text());
        assert_eq!(
            res.headers.get("content-type").map(String::as_str),
            Some("text/plain; charset=utf-8"),
            "{id}: content type"
        );
        let want = case["pulsus"]["body"]
            .as_str()
            .expect("our body")
            .replace("{end}", &start.to_string())
            .replace("{start}", &end.to_string());
        assert_eq!(res.text(), want, "{id}: exact body");
    }
    for (id, case) in fx["range_accepted"].as_object().expect("range_accepted") {
        let route = case["route"].as_str().expect("route");
        let query = match case["shape"].as_str().expect("shape") {
            "both_zero" => "start=0&end=0".to_string(),
            "zero_width" => format!("start={start}&end={start}"),
            other => panic!("{id}: unknown shape {other}"),
        };
        let res = get(port, &format!("{route}?{query}"), id);
        assert_eq!(res.status, 200, "{id}: body {}", res.text());
    }

    // ---- the window bound, as a PAIR --------------------------------
    //
    // An empty list alone cannot tell a measured absence from a static
    // one, so the occupied day and the empty day are asserted together
    // and the query_log is asked whether the span read actually ran.
    let admin = ChClient::new(ch_config()).await.expect("connect admin");
    let before = selects_reading(&admin, db, SPAN_NAME_SELECT_PREFIX, "trace_spans").await;
    let empty_start = start - 90_000;
    let empty_end = start - 86_400;
    let res = get(
        port,
        &format!("/api/v2/search/tag/name/values?start={empty_start}&end={empty_end}"),
        "empty day",
    );
    assert_eq!(res.status, 200, "empty day: body {}", res.text());
    assert_eq!(entries(&res.json("empty day")), Vec::new(), "empty day");
    let after = selects_reading(&admin, db, SPAN_NAME_SELECT_PREFIX, "trace_spans").await;
    assert_eq!(
        after,
        before + 1,
        "the empty answer must be a MEASURED absence: exactly one more span-name read must \
         have run against {db}.trace_spans"
    );

    // ---- the typing corpus, pushed second ---------------------------
    push(port, &corpus::c4_request(base), "C4");
    let res = get(
        port,
        &format!("/api/v2/search/tag/name/values?{window}"),
        "typing",
    );
    assert_eq!(res.status, 200, "typing: body {}", res.text());
    let got = entries(&res.json("typing"));
    for (ty, val) in expected(&fx["span_names"]["T-TYPES"]["pulsus"]) {
        assert!(
            got.contains(&(ty.clone(), val.clone())),
            "typing: {val:?} must be reported as {ty}, got {got:?}"
        );
    }
    let cap = &fx["span_names"]["T-CAP"];
    let want_len = cap["pulsus"]["value_len"].as_u64().expect("value_len") as usize;
    let ch = cap["repeated_char"].as_str().expect("repeated_char");
    let long = got
        .iter()
        .find(|(_, v)| v.len() >= 1_000 && v.starts_with(ch))
        .unwrap_or_else(|| panic!("typing: no over-cap name in our answer"));
    assert_eq!(
        long.1.chars().count(),
        want_len,
        "typing: the over-cap name must be reported capped"
    );
    assert_eq!(
        long.0, "string",
        "typing: the capped name is still a string"
    );

    // ---- issue #509: attribute keys containing `?` ------------------
    //
    // Pushed LAST, so no earlier assertion in this test can see the two
    // `q509-*` span names.
    //
    // Every case asserts the VALUE SET, never the status alone, and that
    // is the whole design. Of the three shapes this defect had, only one
    // was an error: an even run of `?` collapsed in the driver so a
    // DIFFERENT key was asked for, and `?fields` had the row's column
    // list substituted into the literal. Both answered `200` with an
    // empty list — byte-identical to QM-Z, a key nothing holds — so a
    // status-only check passed two of the three.
    push(port, &corpus::cq_request(base), "CQ");

    let qm = fx["question_mark_keys"]
        .as_object()
        .expect("question_mark_keys");
    assert_eq!(
        qm.len(),
        28,
        "every fixture question_mark_keys case must be issued"
    );
    for (id, case) in qm {
        let route = case["route"].as_str().expect("route");
        let mut path = route.to_string();
        let mut sep = '?';
        if case["window"].as_bool().unwrap_or(false) {
            path.push(sep);
            path.push_str(&window);
            sep = '&';
        }
        if let Some(q) = case["q"].as_str() {
            path.push(sep);
            path.push_str(&format!("q={}", urlencode(q)));
        }
        let res = get(port, &path, id);

        if case["expect"].as_str() == Some("error") {
            // QM-T: an empty attribute key. It was a `400` before this
            // change and must stay one — never a `500`, and never a
            // `200` that pretends the key was readable.
            assert_eq!(
                res.status,
                case["pulsus"]["status"].as_u64().expect("status") as u16,
                "{id}: {path} — body {}",
                res.text()
            );
            assert_eq!(
                res.headers.get("content-type").map(String::as_str),
                case["pulsus"]["content_type"].as_str(),
                "{id}: content type"
            );
            assert_eq!(
                res.text(),
                case["pulsus_body"].as_str().expect("pulsus_body"),
                "{id}: exact body"
            );
            continue;
        }

        assert_eq!(res.status, 200, "{id}: {path} — body {}", res.text());
        let body = res.json(id);
        assert_eq!(
            entries(&body),
            expected(&case["pulsus"]),
            "{id}: {path} — body {body}"
        );
        if let Some(want) = case["pulsus"]["truncated"].as_bool() {
            assert_eq!(
                body["truncated"].as_bool(),
                Some(want),
                "{id}: the native route carries `truncated`, body {body}"
            );
        } else {
            assert!(
                body.get("truncated").is_none(),
                "{id}: an alias body carries no `truncated` key, body {body}"
            );
        }
    }

    // ---- issue #509: the key literal ClickHouse RECEIVED -------------
    //
    // The response alone cannot separate the two silent shapes from a
    // key that is genuinely empty: `span.a?fields` and `span.nosuchkey`
    // returned byte-identical bodies before this change. So the four
    // discriminating cases are re-issued here and the text ClickHouse
    // actually received is read back out of `system.query_log`.
    //
    // **The instant bound below is not optional.** `system.query_log` is
    // NOT dropped with the test database and its rows carry the same
    // `current_database`, so without it the "must not contain"
    // assertions read the PREVIOUS run's rows — on a previous build,
    // which is exactly where the forbidden literals live.
    let since = text_column(&admin, "SELECT toString(now64(6)) AS s")
        .await
        .pop()
        .expect("server clock");
    for path in [
        // QM-D, an even run: the driver collapsed it to `a?b`
        "/api/v2/search/tag/span.a%3F%3Fb/values",
        // QM-U2, an odd run: no statement reached ClickHouse at all
        "/api/v2/search/tag/span.a%3F%3F%3Fb/values",
        // QM-V, `?fields`: the row's column list was substituted
        "/api/v2/search/tag/span.a%3Ffields/values",
        // QM-Z, no `?` at all, and no rows either — the control that
        // makes an empty answer mean something
        "/api/v2/search/tag/span.nosuchkey/values",
    ] {
        let res = get(port, path, "received-literal probe");
        assert_eq!(res.status, 200, "{path}: body {}", res.text());
    }
    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
    // Set membership on the EXTRACTED literals, never a bare-key
    // substring search: on the row whose extracted literal is
    // `key = 'a?b?c'`, searching for the bare key `a?b` hits, while
    // searching for the whole `key = '<k>'` form does not.
    let literals = text_column(
        &admin,
        &format!(
            "SELECT DISTINCT extract(query, 'key = ''[^'']*''') AS s FROM system.query_log \
             WHERE type = 'QueryFinish' AND current_database = '{db}' \
               AND query LIKE '%trace_tag_catalog%' \
               AND event_time_microseconds >= toDateTime64('{since}', 6)"
        ),
    )
    .await;
    for want in QM_RECEIVED_LITERALS {
        assert!(
            literals.iter().any(|l| l == want),
            "ClickHouse never received {want}: it received {literals:?}"
        );
    }
    for forbidden in QM_FORBIDDEN_LITERALS {
        assert!(
            !literals.iter().any(|l| l == forbidden),
            "ClickHouse received {forbidden} — the driver rewrote the key: {literals:?}"
        );
    }

    drop_db(db).await;
}

/// The three §4.3 values routes, as mounted.
const VALUES_ROUTES: [&str; 3] = [
    "/api/traces/v1/tag/service.name/values",
    "/api/v2/search/tag/service.name/values",
    "/api/search/tag/service.name/values",
];

/// Issue #478: **where the `q` tolerance stops**, pinned so the two
/// correct rejections below it are not read as regressions later.
///
/// The property the feature keeps is *a `q` that is well-formed input and
/// does not parse as TraceQL never errors* — not "a `q` never errors",
/// which is false and was measured to be false by code review round 1.
/// Both rejected classes are HTTP-transport faults a client can avoid,
/// and neither is a shape the query editor emits: it percent-encodes, and
/// what it sends is the text a human is typing.
///
/// The percent-encoded row is the discriminator. `q=%80` decodes to the
/// same invalid byte the raw row sends, and it is served `200` — so what
/// the `400` refuses is a malformed request line, not a `q` VALUE. A test
/// that asserted only the `400` would have documented the opposite.
#[tokio::test(flavor = "multi_thread")]
async fn the_q_tolerance_stops_at_input_that_is_not_well_formed() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_219;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let db = pulsus_testkit::test_db(&format!("pulsus_tag_q_bounds_it_{nonce}"));
    let db = db.as_str();
    drop_db(db).await;
    let _guard = spawn_ready(port, db);

    // (1) Half-typed TraceQL — the case the tolerance exists for — is
    //     `200` on every values route. Without this the test below would
    //     be pinning rejections with nothing to contrast them against.
    for route in VALUES_ROUTES {
        for raw_q in [
            "%7Bspan.http.status_code%3D",
            "%7Bresource.service.name%3D%22",
            "garbage",
            "%7D",
        ] {
            let ctx = format!("tolerated {route}?q={raw_q}");
            let res = get(port, &format!("{route}?q={raw_q}"), &ctx);
            assert_eq!(res.status, 200, "{ctx}: body {}", res.text());
        }
    }

    // (2) RAW invalid UTF-8 in the request target: `400`, before any
    //     handler runs. Three shapes — a lone continuation byte, a byte
    //     that can never appear in UTF-8, and a truncated two-byte
    //     sequence.
    for route in VALUES_ROUTES {
        for (label, byte) in [
            ("lone 0x80", 0x80u8),
            ("bare 0xFF", 0xFF),
            ("cut 0xC3", 0xC3),
        ] {
            let mut target = format!("{route}?q=").into_bytes();
            target.push(byte);
            let ctx = format!("raw {label} on {route}");
            assert_eq!(
                raw_target_status(port, &target, &ctx),
                400,
                "{ctx}: raw invalid UTF-8 in the request target is refused by the transport"
            );
        }
    }

    // (3) THE DISCRIMINATOR: the same invalid byte, percent-encoded, is
    //     served. So (2) is about the request line, not about `q`.
    for route in VALUES_ROUTES {
        let ctx = format!("percent-encoded 0x80 on {route}");
        let res = get(port, &format!("{route}?q=%80"), &ctx);
        assert_eq!(res.status, 200, "{ctx}: body {}", res.text());
        let long = format!("{route}?q={}", "%80".repeat(4_096));
        let res = get(port, &long, &ctx);
        assert_eq!(res.status, 200, "{ctx} x4096: body {}", res.text());
    }

    // (4) An enormous `q` is rejected, and the LENGTH boundary is the
    //     stable part. Bisected on this tree: 65,493 bytes is the last
    //     `200` and 65,494 is the first rejection — the 64 KiB
    //     request-target bound.
    //
    //     **The rejection's STATUS CODE is not asserted exactly, and
    //     that is a correction rather than a hedge.** An earlier revision
    //     pinned `414` up to 524,194 and `431` from 524,195, bisected
    //     locally. CI answered `431` at 524,194 — same length, same
    //     binary, different machine. Which of the two the transport
    //     chooses depends on how the request bytes arrive, not on
    //     anything this change controls, so pinning it asserted a
    //     property of a machine. What IS the contract, and is asserted:
    //     past the bound the request is refused by the transport with one
    //     of those two statuses — never served `200`, and never the
    //     handler's `400`, which would mean `q` had been read and
    //     rejected on its content.
    let route = VALUES_ROUTES[1];
    for len in [16_384usize, 65_493] {
        let target = format!("{route}?q={}", "a".repeat(len)).into_bytes();
        let ctx = format!("q of {len} bytes on {route}");
        assert_eq!(
            raw_target_status(port, &target, &ctx),
            200,
            "{ctx}: inside the request-target bound, so it must be served"
        );
    }
    for len in [65_494usize, 524_194, 1_048_576] {
        let target = format!("{route}?q={}", "a".repeat(len)).into_bytes();
        let ctx = format!("q of {len} bytes on {route}");
        let status = raw_target_status(port, &target, &ctx);
        assert!(
            status == 414 || status == 431,
            "{ctx}: past the request-target bound this must be a transport refusal \
             (414 or 431), got {status}"
        );
    }

    drop_db(db).await;
}

/// The zero-width case (`q_matrix.Q-AZ`) must resolve to the corpus's own
/// UTC day at EVERY wall clock, and the corpus must not straddle a UTC
/// midnight. Hermetic: pure arithmetic over the same two functions the
/// live tests above call, swept over a whole day.
///
/// **This test exists because the live suite failed on a clock.** Until
/// this change the zero-width probe was issued at `start` — an hour
/// before the corpus — so between 00:01:00 and 01:01:00 UTC it landed on
/// the previous UTC day and the day-granular read answered `[]` instead
/// of the corpus's ten values. Measured by emulating the whole run at 14
/// virtual wall clocks: 0 values inside that band, 10 outside it. The
/// live suite cannot catch that itself, because it runs at one instant
/// and 23 of every 24 hours are green.
///
/// It is a sweep and not a boundary pair on purpose: the old
/// construction was wrong on a contiguous HOUR, and any single-instant
/// check has 23 chances in 24 of sitting outside it.
#[test]
fn the_zero_width_probe_lands_on_the_corpus_day() {
    // Day 20,699 since the epoch is an arbitrary anchor; only the
    // offsets inside the day matter.
    let day0 = 20_699u64 * corpus::NS_PER_DAY;
    for sec in 0..86_400u64 {
        for frac in [0u64, 1, 500_000_000, 999_999_999] {
            let now_ns = day0 + sec * 1_000_000_000 + frac;
            let base = corpus::base_ns_from(now_ns);

            // The whole corpus block is on one UTC day: its first
            // instant is `base` and its last is inside `CORPUS_BLOCK_NS`.
            let first = corpus::utc_day((base / 1_000_000_000) as i64);
            let last = corpus::utc_day(((base + corpus::CORPUS_BLOCK_NS) / 1_000_000_000) as i64);
            assert_eq!(
                first, last,
                "now_ns={now_ns}: the corpus block straddles a UTC midnight \
                 (base={base}), so a day-granular read answers a partial list"
            );

            // And the zero-width probe resolves to that same day.
            let probe = corpus::zero_width_probe_secs(base);
            assert_eq!(
                corpus::utc_day(probe),
                first,
                "now_ns={now_ns}: the zero-width probe at {probe} resolves to UTC day {} \
                 but the corpus is on day {first}",
                corpus::utc_day(probe)
            );
        }
    }
}
