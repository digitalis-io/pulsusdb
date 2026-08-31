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
//!   empty list alone cannot tell a measured absence from a static one.
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

fn expected(answer: &Value) -> Vec<(String, String)> {
    answer["values"]
        .as_array()
        .expect("values")
        .iter()
        .map(|p| {
            let a = p.as_array().expect("pair");
            (
                a[0].as_str().unwrap_or_default().to_string(),
                a[1].as_str().unwrap_or_default().to_string(),
            )
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
            // The zero-width window case.
            format!("{route}?start={start}&end={start}")
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

    drop_db(db).await;
}
