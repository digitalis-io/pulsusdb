//! Live end-to-end tests for `GET|POST /api/logs/v1/{detected_labels,
//! detected_fields}` (issue #170, docs/api.md §2.6): spawns the real
//! `pulsusdb` binary against a live ClickHouse and asserts:
//! - detected_labels drops ID-only keys (all-UUID, all-numeric), keeps
//!   static (`namespace`) + mixed keys with EXACT cardinalities, and
//!   `query=` scoping narrows to the resolved streams;
//! - detected_fields returns structured-metadata fields (`parsers:[]`),
//!   json/logfmt-detected fields with the pinned `type`s and parser
//!   attribution, respects `limit` (first-seen field cap) and
//!   `line_limit` (sample size);
//! - `X-Pulsus-Explain` shows the single stage-3 scan with skip-index
//!   line-filter prefilters + `LIMIT <line_limit>` (Tier-1 pushdown
//!   evidence at the endpoint level), and the paged keyset route when a
//!   dropping stage is present;
//! - the issue #170 plan-v2 sparse-filter fix: matches occurring only
//!   AFTER the first `line_limit` raw rows ARE found (window-exhausted,
//!   complete — no `pulsus_partial` key), and a budget-truncated spawn
//!   returns 200 with `"pulsus_partial":true`;
//! - the `/loki/api/v1/*` aliases are byte-identical to native.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test logs_detected_live
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Ports 31155-31156, distinct from every other live suite.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

fn ch_host() -> String {
    std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string())
}

fn ch_http_port() -> u16 {
    std::env::var("PULSUS_TEST_CH_HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(19123)
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("now fits in i64")
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
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

struct HttpResponse {
    status: u16,
    body: String,
}

fn http_get(port: u16, path_and_query: &str, explain: bool) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    let mut head =
        format!("GET {path_and_query} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if explain {
        head.push_str("X-Pulsus-Explain: 1\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read");
    let split_at = find_subslice(&buf, b"\r\n\r\n").expect("header terminator");
    let head_text = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let status: u16 = head_text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("status line");
    let headers: HashMap<String, String> = head_text
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (k, v) = line.split_once(':')?;
            Some((k.trim().to_ascii_lowercase(), v.trim().to_string()))
        })
        .collect();
    let raw_body = &buf[split_at + 4..];
    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    HttpResponse {
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

fn spawn_ready(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command.spawn().expect("spawn pulsusdb");
    let guard = ChildGuard(child);
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        let ready = TcpStream::connect(("127.0.0.1", port)).is_ok()
            && http_get(port, "/ready", false).status == 200;
        if ready {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port}, db {db})");
}

async fn admin_client() -> ChClient {
    ChClient::new(ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    })
    .await
    .expect("connect admin client")
}

async fn drop_db(db: &str) {
    admin_client()
        .await
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop db");
}

async fn data_client(db: &str) -> ChClient {
    ChClient::new(ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    })
    .await
    .expect("connect data client")
}

async fn seed_stream(
    client: &ChClient,
    db: &str,
    ts_ns: i64,
    fp: u64,
    service: &str,
    labels: &str,
) {
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({ts_ns}))), {fp}, \
                 '{service}', '{labels}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
}

/// A `log_samples` bulk-insert row (the `query_log_gates.rs` shape plus
/// the per-entry structured-metadata column).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    service: String,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: String,
    structured_metadata: String,
}

fn fields_of(json: &serde_json::Value) -> Vec<(String, String, u64, Vec<String>)> {
    json["fields"]
        .as_array()
        .expect("fields array")
        .iter()
        .map(|f| {
            (
                f["label"].as_str().expect("label").to_string(),
                f["type"].as_str().expect("type").to_string(),
                f["cardinality"].as_u64().expect("cardinality"),
                f["parsers"]
                    .as_array()
                    .expect("parsers array")
                    .iter()
                    .map(|p| p.as_str().expect("parser").to_string())
                    .collect(),
            )
        })
        .collect()
}

const CHECKOUT_SELECTOR: &str = "query=%7Bservice_name%3D%22checkout%22%7D";

#[tokio::test(flavor = "multi_thread")]
async fn detected_labels_and_fields_end_to_end() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_155;
    let db = "pulsus_detected_it_live";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "true")]);
    let client = data_client(db).await;

    let now = now_ns();
    // Streams — the log_streams_idx MV populates the index this endpoint
    // aggregates. Key classes: `env` mixed (kept, card 2), `region`
    // non-ID (kept), `service_name` non-ID (kept, card 2 with the sparse
    // stream below), `req_id` UUID-only (dropped), `shard` numeric-only
    // (dropped), `namespace` UUID-only but STATIC (kept).
    seed_stream(
        &client,
        db,
        now,
        1,
        "checkout",
        r#"{"env":"prod","region":"us-east-1","service_name":"checkout"}"#,
    )
    .await;
    seed_stream(
        &client,
        db,
        now,
        2,
        "checkout",
        r#"{"env":"dev","req_id":"7c39a2de-5f6a-4b8e-9d21-0a1b2c3d4e5f","service_name":"checkout"}"#,
    )
    .await;
    seed_stream(
        &client,
        db,
        now,
        3,
        "checkout",
        r#"{"namespace":"a2b4c6d8-1111-2222-3333-444455556666","shard":"42","service_name":"checkout"}"#,
    )
    .await;
    // The sparse-filter stream (plan v2's reviewer-named gap, below).
    seed_stream(
        &client,
        db,
        now,
        9,
        "sparse-svc",
        r#"{"service_name":"sparse-svc"}"#,
    )
    .await;

    // Samples for detected_fields, all on fp 1 (distinct timestamps —
    // deterministic last-entry-wins detection): a JSON body, a logfmt
    // body carrying structured metadata, and a body neither parser
    // accepts (unterminated logfmt quote).
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, \
                 body, structured_metadata) VALUES \
                 ('checkout', 1, {t1}, 0, '{{\"count\":7,\"ratio\":1.5,\"active\":true,\"took\":\"250ms\",\"size\":\"3MiB\",\"msg\":\"hello\"}}', ''), \
                 ('checkout', 1, {t2}, 0, 'method=GET status_text=slow', '{{\"trace_id\":\"abc123\"}}'), \
                 ('checkout', 1, {t3}, 0, 'plain x=\"unterminated', '')",
                t1 = now - 3_000_000_000,
                t2 = now - 2_000_000_000,
                t3 = now - 1_000_000_000,
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");

    // The sparse-filter corpus: 2,600 rows on fp 9; ONLY the OLDEST 3 are
    // JSON matching `| json | level="rare"`. With the default line_limit
    // (100) and scan factor (10) the paged walk (page size 1,000,
    // newest-first) reaches them only on page 3 — long after the first
    // `line_limit` raw rows.
    let sparse_base = now - 600_000_000_000; // 10 minutes ago
    let sparse_rows: Vec<SeedSampleRow> = (0..2_600)
        .map(|i| SeedSampleRow {
            service: "sparse-svc".to_string(),
            fingerprint: 9,
            timestamp_ns: sparse_base + (i as i64) * 36_000_000,
            severity: 0,
            body: if i < 3 {
                r#"{"level":"rare"}"#.to_string()
            } else {
                format!("sparse routine row {i}")
            },
            structured_metadata: String::new(),
        })
        .collect();
    client
        .insert_block("log_samples", &sparse_rows)
        .await
        .expect("bulk insert sparse corpus");

    let start = now - 3 * 24 * 3_600_000_000_000;
    let end = now + 60_000_000_000;

    // -- detected_labels, unscoped: ID-filtering + static keep + exact
    //    cardinality — and the never-touches-log_samples explain proof --
    let res = http_get(
        port,
        &format!("/api/logs/v1/detected_labels?start={start}&end={end}"),
        true,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("detected_labels JSON");
    assert_eq!(
        json["detectedLabels"],
        serde_json::json!([
            {"label": "env", "cardinality": 2},
            {"label": "namespace", "cardinality": 1},
            {"label": "region", "cardinality": 1},
            {"label": "service_name", "cardinality": 2},
        ]),
        "ID-only keys (req_id/shard) dropped, static namespace kept despite \
         UUID-only values, exact cardinalities: {json}"
    );
    let stages = json["explain"]["stages"].as_array().expect("stages");
    let agg = stages
        .iter()
        .find(|s| s["name"] == "detected_labels")
        .expect("a detected_labels stage");
    assert!(
        agg["sql"]
            .as_str()
            .expect("sql")
            .contains("log_streams_idx"),
        "the aggregation reads the stream index"
    );
    for stage in stages {
        let sql = stage["sql"].as_str().unwrap_or_default();
        assert!(
            !sql.contains("log_samples"),
            "detected_labels must NEVER touch log_samples: {sql}"
        );
    }

    // -- detected_labels, scoped: `query=` narrows to the resolved
    //    streams (fp 1 only) ---------------------------------------------
    let res = http_get(
        port,
        &format!(
            "/api/logs/v1/detected_labels?query=%7Benv%3D%22prod%22%7D&start={start}&end={end}"
        ),
        false,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("scoped JSON");
    assert_eq!(
        json["detectedLabels"],
        serde_json::json!([
            {"label": "env", "cardinality": 1},
            {"label": "region", "cardinality": 1},
            {"label": "service_name", "cardinality": 1},
        ]),
        "scoping must narrow every cardinality to the matched stream: {json}"
    );

    // -- detected_fields: SM field (parsers:[]), json/logfmt fields with
    //    the pinned types + parser attribution ----------------------------
    let res = http_get(
        port,
        &format!("/api/logs/v1/detected_fields?{CHECKOUT_SELECTOR}&start={start}&end={end}"),
        false,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("detected_fields JSON");
    assert_eq!(json["limit"], 1000, "default field limit echoed");
    assert!(
        json.get("pulsus_partial").is_none(),
        "complete responses carry no pulsus_partial key: {json}"
    );
    let owned = |items: &[(&str, &str, u64, &[&str])]| {
        items
            .iter()
            .map(|(l, t, c, p)| {
                (
                    l.to_string(),
                    t.to_string(),
                    *c,
                    p.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        fields_of(&json),
        owned(&[
            ("active", "boolean", 1, &["json"]),
            ("count", "int", 1, &["json"]),
            ("method", "string", 1, &["logfmt"]),
            ("msg", "string", 1, &["json"]),
            ("ratio", "float", 1, &["json"]),
            ("size", "bytes", 1, &["json"]),
            ("status_text", "string", 1, &["logfmt"]),
            ("took", "duration", 1, &["json"]),
            ("trace_id", "string", 1, &[]),
        ]),
        "six-type detection, json/logfmt attribution, SM field with no parser: {json}"
    );

    // -- `limit` (field cap): first-seen field names win ------------------
    // Newest-first sampling: the plain row detects nothing, then the
    // logfmt row observes trace_id (SM) then method/status_text — a
    // limit of 2 admits exactly {trace_id, method}.
    let res = http_get(
        port,
        &format!(
            "/api/logs/v1/detected_fields?{CHECKOUT_SELECTOR}&limit=2&start={start}&end={end}"
        ),
        false,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("limited JSON");
    assert_eq!(json["limit"], 2);
    let labels: Vec<String> = fields_of(&json).into_iter().map(|(l, ..)| l).collect();
    assert_eq!(
        labels,
        vec!["method".to_string(), "trace_id".to_string()],
        "the first 2 distinct field names win: {json}"
    );

    // -- `line_limit` (sample size): only the newest entry sampled -------
    // The newest checkout row is the one neither parser accepts, so a
    // line_limit of 1 detects no fields at all.
    let res = http_get(
        port,
        &format!(
            "/api/logs/v1/detected_fields?{CHECKOUT_SELECTOR}&line_limit=1&start={start}&end={end}"
        ),
        false,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    assert_eq!(res.body, r#"{"fields":[],"limit":1000}"#);

    // -- Explain, fast path: the single stage-3 scan carries the
    //    skip-index line-filter prefilters + LIMIT <line_limit> (Tier-1
    //    pushdown evidence at the endpoint level) -------------------------
    let line_filtered = "query=%7Bservice_name%3D%22checkout%22%7D%20%7C%3D%20%22hello%22";
    let res = http_get(
        port,
        &format!(
            "/api/logs/v1/detected_fields?{line_filtered}&line_limit=2&start={start}&end={end}"
        ),
        true,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("explain JSON");
    let stages = json["explain"]["stages"].as_array().expect("stages");
    let read = stages
        .iter()
        .find(|s| s["name"] == "detected_fields_read")
        .expect("a detected_fields_read stage");
    assert_eq!(read["note"], "single-scan: no unpushed dropping stage");
    let sql = read["sql"].as_str().expect("sql");
    assert!(
        sql.contains("hasToken(body, 'hello')"),
        "line-filter token prefilter must push down: {sql}"
    );
    assert!(
        sql.ends_with("LIMIT 2"),
        "the scan is LIMIT <line_limit>-bounded: {sql}"
    );

    // -- Explain, paged route: a dropping stage switches to the keyset
    //    page shape (plan v2's routing note) ------------------------------
    let dropping =
        "query=%7Bservice_name%3D%22checkout%22%7D%20%7C%20json%20%7C%20msg%3D%22hello%22";
    let res = http_get(
        port,
        &format!("/api/logs/v1/detected_fields?{dropping}&start={start}&end={end}"),
        true,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("paged explain JSON");
    let stages = json["explain"]["stages"].as_array().expect("stages");
    let read = stages
        .iter()
        .find(|s| s["name"] == "detected_fields_read")
        .expect("a detected_fields_read stage");
    assert_eq!(read["note"], "paged: unpushed dropping stage");
    let sql = read["sql"].as_str().expect("sql");
    assert!(
        sql.contains("AS body_hash"),
        "the paged route is the keyset page shape: {sql}"
    );
    assert!(
        sql.ends_with("LIMIT 1000"),
        "page row-bound = line_limit x scan factor (100 x 10): {sql}"
    );
    assert!(
        fields_of(&json).iter().any(|(l, ..)| l == "msg"),
        "the surviving json row's fields are detected: {json}"
    );

    // -- Plan v2's reviewer-named gap, live: matches occurring only after
    //    the first line_limit raw rows ARE found; the complete response
    //    carries NO pulsus_partial key ------------------------------------
    let sparse =
        "query=%7Bservice_name%3D%22sparse-svc%22%7D%20%7C%20json%20%7C%20level%3D%22rare%22";
    let res = http_get(
        port,
        &format!("/api/logs/v1/detected_fields?{sparse}&start={start}&end={end}"),
        false,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("sparse JSON");
    assert!(
        !res.body.contains("pulsus_partial"),
        "window exhaustion is complete — no partial key: {}",
        res.body
    );
    assert_eq!(
        fields_of(&json),
        owned(&[("level", "string", 1, &["json"])]),
        "late-occurring matches (page 3 of the walk) must be detected: {json}"
    );

    // -- Aliases: byte-identical to native --------------------------------
    let native = http_get(
        port,
        &format!("/api/logs/v1/detected_labels?start={start}&end={end}"),
        false,
    );
    let alias = http_get(
        port,
        &format!("/loki/api/v1/detected_labels?start={start}&end={end}"),
        false,
    );
    assert_eq!(alias.status, 200);
    assert_eq!(
        alias.body, native.body,
        "detected_labels alias byte-identity"
    );

    let native = http_get(
        port,
        &format!("/api/logs/v1/detected_fields?{CHECKOUT_SELECTOR}&start={start}&end={end}"),
        false,
    );
    let alias = http_get(
        port,
        &format!("/loki/api/v1/detected_fields?{CHECKOUT_SELECTOR}&start={start}&end={end}"),
        false,
    );
    assert_eq!(alias.status, 200);
    assert_eq!(
        alias.body, native.body,
        "detected_fields alias byte-identity"
    );

    drop_db(db).await;
}

/// Plan v2's budget-truncation spawn: a tiny `PULSUS_LOGQL_SCAN_BUDGET_BYTES`
/// sized so the FIRST keyset page (a whole-window scan — the keyset ORDER
/// BY defeats optimize_read_in_order) fits but a later page's remaining
/// cap trips — the response is a 200 carrying `"pulsus_partial":true`
/// (the additive #90 truncation signal), never an error.
#[tokio::test(flavor = "multi_thread")]
async fn detected_fields_budget_truncation_signals_pulsus_partial() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_156;
    let db = "pulsus_detected_it_budget";
    drop_db(db).await;
    // ~4.2 MiB corpus (10,000 rows x ~420 read bytes each); a 6 MiB
    // budget fits page 1 (whole-window scan) but page 2's remaining cap
    // (~1.8 MiB) is far below its ~3.8 MiB scan — deterministic
    // mid-paging abort (the query_log_gates engine tests' proportions).
    let _guard = spawn_ready(port, db, &[("PULSUS_LOGQL_SCAN_BUDGET_BYTES", "6291456")]);
    let client = data_client(db).await;

    let now = now_ns();
    seed_stream(
        &client,
        db,
        now,
        1,
        "budget-svc",
        r#"{"service_name":"budget-svc"}"#,
    )
    .await;
    let base = now - 600_000_000_000;
    let rows: Vec<SeedSampleRow> = (0..10_000)
        .map(|i| SeedSampleRow {
            service: "budget-svc".to_string(),
            fingerprint: 1,
            timestamp_ns: base + (i as i64) * 36_000_000,
            severity: 0,
            // No row ever matches `| json | level="rare"` — the walk can
            // only end on the budget (page size 1,000 << 10,000 rows).
            body: format!("routine row {i} padding_{}", "x".repeat(380)),
            structured_metadata: String::new(),
        })
        .collect();
    client
        .insert_block("log_samples", &rows)
        .await
        .expect("bulk insert budget corpus");

    let start = now - 3 * 24 * 3_600_000_000_000;
    let end = now + 60_000_000_000;
    let dropping =
        "query=%7Bservice_name%3D%22budget-svc%22%7D%20%7C%20json%20%7C%20level%3D%22rare%22";
    let res = http_get(
        port,
        &format!("/api/logs/v1/detected_fields?{dropping}&start={start}&end={end}"),
        false,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("truncated JSON");
    assert_eq!(
        json["pulsus_partial"], true,
        "budget exhaustion mid-paging must signal the additive partial key: {json}"
    );
    assert_eq!(
        json["fields"],
        serde_json::json!([]),
        "no field ever matched — the truncated sample is empty: {json}"
    );

    drop_db(db).await;
}
