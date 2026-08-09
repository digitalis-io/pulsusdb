//! Live end-to-end tests for `GET|POST /api/logs/v1/{detected_labels,
//! detected_fields}` (issue #170, docs/api.md §2.6): spawns the real
//! `pulsusdb` binary against a live ClickHouse and asserts:
//! - detected_labels drops ID-only keys (all-UUID, all-numeric), keeps
//!   static (`namespace`) + mixed keys with EXACT cardinalities, and
//!   `query=` scoping narrows to the resolved streams;
//! - detected_fields returns structured-metadata fields (`parsers:null`
//!   — issue #258's third shape), json/logfmt-detected fields with the
//!   pinned `type`s, parser attribution and the raw `jsonPath` (issue
//!   #254), respects `limit` (first-seen field cap) and `line_limit`
//!   (sample size), and answers a zero-field sample with the reference's
//!   bare `{}` (issue #258);
//! - `X-Pulsus-Explain` shows the single stage-3 scan with skip-index
//!   line-filter prefilters + `LIMIT <line_limit>` (Tier-1 pushdown
//!   evidence at the endpoint level), and the paged keyset route when a
//!   dropping stage is present;
//! - the issue #170 plan-v2 sparse-filter fix: matches occurring only
//!   AFTER the first `line_limit` raw rows ARE found (window-exhausted,
//!   complete — no `pulsus_partial` key), and a budget-truncated spawn
//!   returns 200 with `"pulsus_partial":true`;
//! - the `/loki/api/v1/*` aliases are byte-identical to native;
//! - issue #261: at the two value counts where the reference's p14
//!   HyperLogLog estimate stops matching the truth, `detected_labels`
//!   answers the EXACT count — the number the reference does not give;
//! - issue #399: `detected_labels` answers the REQUESTED WINDOW, not the
//!   calendar month containing it, with the lower bound floored to the
//!   rollup bucket containing `start`.
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
//! Ports 31155-31156, 31165, 31167 and 31169, distinct from every other
//! live suite. (31157, the next number up, already belongs to
//! `loki_push_live.rs`; 31166 belongs to it too, and 31168 to
//! `logs_api_live.rs`.)

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
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

/// Seeds one `log_metrics_5s` row per fingerprint, at the 5s bucket
/// CONTAINING `ts_ns` — the stream-activity evidence issue #399's window
/// filter reads.
///
/// Inserted directly rather than through `log_samples`: a stream that
/// only exists to exercise `/detected_labels` must not acquire sample
/// rows, which would change what `/detected_fields` samples in the same
/// fixture. Streams that DO have `log_samples` rows get their rollup rows
/// from the shipped `log_metrics_5s_mv` and need no call here.
async fn seed_activity(client: &ChClient, db: &str, ts_ns: i64, fingerprints: &[u64]) {
    let bucket = ts_ns / 5_000_000_000 * 5_000_000_000;
    let values: Vec<String> = fingerprints
        .iter()
        .map(|fp| format!("({fp}, {bucket}, 1, 10)"))
        .collect();
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_metrics_5s (fingerprint, bucket_ns, count, bytes) VALUES {}",
                values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_metrics_5s");
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

/// `(label, type, cardinality, parsers, jsonPath)` per field. `parsers`
/// is `null` (issue #258) for an unattributed field, which maps to an
/// empty vec here; `jsonPath` is ABSENT (issue #254) unless the field was
/// json-flattened, which maps to `None`.
type DetectedField = (String, String, u64, Vec<String>, Option<Vec<String>>);

fn fields_of(json: &serde_json::Value) -> Vec<DetectedField> {
    json["fields"]
        .as_array()
        .expect("fields array")
        .iter()
        .map(|f| {
            let parsers = match &f["parsers"] {
                serde_json::Value::Null => Vec::new(),
                v => v
                    .as_array()
                    .expect("parsers array or null")
                    .iter()
                    .map(|p| p.as_str().expect("parser").to_string())
                    .collect(),
            };
            let json_path = f.get("jsonPath").map(|p| {
                p.as_array()
                    .expect("jsonPath array")
                    .iter()
                    .map(|c| c.as_str().expect("path component").to_string())
                    .collect()
            });
            (
                f["label"].as_str().expect("label").to_string(),
                f["type"].as_str().expect("type").to_string(),
                f["cardinality"].as_u64().expect("cardinality"),
                parsers,
                json_path,
            )
        })
        .collect()
}

const CHECKOUT_SELECTOR: &str = "query=%7Bservice_name%3D%22checkout%22%7D";

/// Issue #253 AC 5, run against the end-to-end test's already-seeded
/// `CHECKOUT_SELECTOR` fixture rather than as a standalone `#[tokio::test]`
/// so it costs one server spawn and one seed less on the `schema-it` leg
/// (the plan's own CI-budget note: "one added request against the existing
/// fixture").
///
/// Each request here was a `400` before #253 and answers `200` on
/// `grafana/loki:3.7.4` (measured 2026-08-07): the fields are exactly the
/// unlimited default's, the `limit` is echoed as given, and the
/// `/loki/api/v1/` alias stays byte-identical to the native prefix.
fn detected_fields_accepts_a_field_limit_far_above_the_entry_cap(port: u16, start: i64, end: i64) {
    let query = |prefix: &str, qs: &str| {
        format!("{prefix}/detected_fields?{CHECKOUT_SELECTOR}{qs}&start={start}&end={end}")
    };
    let native = |qs: &str| query("/api/logs/v1", qs);
    let alias = |qs: &str| query("/loki/api/v1", qs);
    let res = http_get(port, &native(""), false);
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("default JSON");
    let default_fields = fields_of(&json);

    for (qs, echoed) in [
        ("&limit=5001", 5001_u64),
        ("&limit=4294967295", 4_294_967_295),
        // Present-but-empty is ABSENT (#253), so the legacy alias
        // supplies the value; this whole request was a 400 before.
        ("&limit=&field_limit=5001", 5001),
    ] {
        let res = http_get(port, &native(qs), false);
        assert_eq!(res.status, 200, "{qs}: body: {}", res.body);
        let json: serde_json::Value = serde_json::from_str(&res.body).expect("uncapped JSON");
        assert_eq!(
            fields_of(&json),
            default_fields,
            "{qs} must return the unlimited default's fields: {json}"
        );
        assert_eq!(
            json["limit"].as_u64(),
            Some(echoed),
            "{qs}: the limit is echoed as given"
        );
        let aliased = http_get(port, &alias(qs), false);
        assert_eq!(aliased.status, 200, "{qs}: alias body: {}", aliased.body);
        assert_eq!(
            aliased.body, res.body,
            "{qs}: uncapped detected_fields alias byte-identity"
        );
    }
}

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
    // Issue #399: `/detected_labels` is now window-scoped, so a stream
    // with no activity in `[start, end]` is correctly absent. Fps 1 and 9
    // get their rollup rows from the MV over the `log_samples` inserts
    // below; fps 2 and 3 exist only for the label-relevance cases and
    // deliberately have no samples, so their activity is seeded here.
    seed_activity(&client, db, now, &[2, 3]).await;

    // Samples for detected_fields, all on fp 1 (distinct timestamps —
    // deterministic last-entry-wins detection): a JSON body, a logfmt
    // body carrying structured metadata, and a body neither parser
    // accepts (unterminated logfmt quote).
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, \
                 body, structured_metadata) VALUES \
                 ('checkout', 1, {t1}, 0, '{{\"count\":7,\"ratio\":1.5,\"active\":true,\"took\":\"250ms\",\"size\":\"3MiB\",\"msg\":\"hello\",\"user\":{{\"id\":42}}}}', ''), \
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
    let agg_sql = agg["sql"].as_str().expect("sql");
    assert!(
        agg_sql.contains("log_streams_idx"),
        "the aggregation reads the stream index"
    );
    // Issue #399 AC9: the same single scan now also names the configured
    // log rollup — the activity semi-join carrying the request's window.
    assert!(
        agg_sql.contains("log_metrics_5s"),
        "the aggregation must carry the activity semi-join over the configured \
         rollup table: {agg_sql}"
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
    /// One expected field row, in `fields_of` order.
    type ExpectedField<'a> = (&'a str, &'a str, u64, &'a [&'a str], Option<&'a [&'a str]>);
    let owned = |items: &[ExpectedField<'_>]| {
        items
            .iter()
            .map(|(l, t, c, p, jp)| {
                (
                    l.to_string(),
                    t.to_string(),
                    *c,
                    p.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                    jp.map(|path| path.iter().map(|s| s.to_string()).collect::<Vec<_>>()),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        fields_of(&json),
        owned(&[
            ("active", "boolean", 1, &["json"], Some(&["active"][..])),
            ("count", "int", 1, &["json"], Some(&["count"][..])),
            ("method", "string", 1, &["logfmt"], None),
            ("msg", "string", 1, &["json"], Some(&["msg"][..])),
            ("ratio", "float", 1, &["json"], Some(&["ratio"][..])),
            ("size", "bytes", 1, &["json"], Some(&["size"][..])),
            ("status_text", "string", 1, &["logfmt"], None),
            ("took", "duration", 1, &["json"], Some(&["took"][..])),
            ("trace_id", "string", 1, &[], None),
            ("user_id", "int", 1, &["json"], Some(&["user", "id"][..])),
        ]),
        "six-type detection, json/logfmt attribution, the raw jsonPath (#254 — nested \
         `user.id` carries both components, a logfmt/SM field carries none), SM field with \
         no parser: {json}"
    );
    // Issue #258's third shape, on the wire: an unattributed field's
    // `parsers` is JSON `null`, never `[]`; a logfmt field carries no
    // `jsonPath` key at all (#254, `omitempty`).
    let trace_id = json["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .find(|f| f["label"] == "trace_id")
        .expect("trace_id field");
    assert!(trace_id["parsers"].is_null(), "{trace_id}");
    assert!(trace_id.get("jsonPath").is_none(), "{trace_id}");
    assert!(
        res.body
            .contains(r#"{"label":"user_id","type":"int","cardinality":1,"parsers":["json"],"jsonPath":["user","id"]}"#),
        "byte-exact reference field shape (proto field order): {}",
        res.body
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

    // -- Issue #253: the field-name axis has NO ceiling ------------------
    detected_fields_accepts_a_field_limit_far_above_the_entry_cap(port, start, end);

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
    // Issue #258: a zero-field result is the reference's bare `{}` — no
    // `fields`, no `limit`.
    assert_eq!(res.body, "{}");

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
        owned(&[("level", "string", 1, &["json"], Some(&["level"][..]))]),
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
    assert!(
        json.get("fields").is_none() && json.get("limit").is_none(),
        "no field ever matched — the truncated sample is the bare object plus the additive \
         partial key (issue #258): {json}"
    );
    assert_eq!(res.body, r#"{"pulsus_partial":true}"#);

    drop_db(db).await;
}

/// Issue #261 — the exactness claim, gated at the layer the user
/// observes. At the two distinct-value counts where the reference's p14
/// HyperLogLog estimate stops equalling the truth, the HTTP body carries
/// the EXACT count:
///
/// | fixture | true N | this endpoint | `grafana/loki:3.7.4` |
/// |---|---|---|---|
/// | `pod-{0..7707}` | 7708 | **7708** | 7640 |
/// | `svc-{0..4532}` | 4533 | **4533** | 4532 |
///
/// Both reference numbers were measured end-to-end against the container
/// on 2026-08-08 (three reps each) and are recorded in
/// `crates/pulsus-read/tests/golden/detected_labels_cardinality/reference_divergence.tsv`
/// with their capture conditions. The two families diverge at different
/// counts because the agreement threshold is a property of the value
/// strings, not of N — see the `detected-cardinality-exact-not-estimated`
/// ledger entry.
///
/// **This case pins the rule; it does not discriminate the #261 change.**
/// #261 edited no `src/`, so the engine this case drives IS the pre-#261
/// engine: it already answered the exact count, and this assertion could
/// not have failed before the change any more than after it. #261
/// changed documentation, not behaviour.
///
/// **What it catches, what it does not, and why the split is deliberate
/// (issue #261 AC 8, restated to what the two gates actually do).**
/// Swapping `uniqExact(val)` in `sql::detected_labels` for
/// `uniqCombined`, `uniqCombined64` or `uniqHLL12` reddens this
/// assertion at both fixtures, and `uniqTheta` at the `pod-` one
/// (measured on `clickhouse/clickhouse-server:24.8`, 2026-08-08: at
/// 7708 distinct values those four answer 7696 / 7696 / 7733 / 7665, and
/// at 4533 they answer 4534 / 4534 / 4552 / 4533).
///
/// A swap to plain `uniq` is **invisible here, permanently**, and the
/// fixture sizes must NOT be changed to chase it. ClickHouse's `uniq` is
/// exact through 65536 distinct values and first diverges at 65537
/// (measured same day: 65536 → 65536, 65537 → 65359). Discriminating it
/// live would therefore need a fixture 8.5× this one's rows, and 65537
/// is not a point where the REFERENCE's estimate diverges — which is the
/// entire reason 7708 and 4533 were chosen. Buying that one break would
/// cost CI time and sever this case from the two numbers it exists to
/// pin. So it is not bought: `uniq(` is banned by literal in the SQL-text
/// gate `the_detected_labels_aggregate_is_still_an_exact_count`
/// (`crates/pulsus-read/tests/detected_labels_cardinality.rs`), which is
/// why that weaker-looking gate exists. Neither gate is redundant, and
/// between them every estimator ClickHouse offers is covered.
///
/// Seeding is ONE bulk statement per fixture over `numbers()`, fanned out
/// into `log_streams_idx` by the shipped `log_streams_idx_mv`
/// (`crates/pulsus-schema/src/catalog.rs`). `pod` is a static detected
/// label (the reference's `cluster`/`namespace`/`instance`/`pod` set), so
/// the relevance filter admits it unconditionally; `svc` is admitted
/// because its values are neither floats nor UUIDs.
#[tokio::test(flavor = "multi_thread")]
async fn detected_labels_cardinality_is_exact_at_the_reference_divergence_points() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_165;
    let db = "pulsus_detected_it_cardinality";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[("PULSUS_COMPAT_ENDPOINTS", "true")]);
    let client = data_client(db).await;

    let now = now_ns();
    // (service_name, label key, value prefix, distinct value count,
    //  fingerprint base) — the two measured divergence points.
    let fixtures = [
        ("card", "pod", "pod-", 7708u64, 0u64),
        ("svcfix", "svc", "svc-", 4533, 10_000_000),
    ];
    for (service, key, prefix, n, fp_base) in fixtures {
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, \
                     updated_ns) SELECT \
                     toStartOfMonth(fromUnixTimestamp64Nano(toInt64({now}))), \
                     {fp_base} + number, '{service}', \
                     concat('{{\"{key}\":\"{prefix}', toString(number), \
                     '\",\"service_name\":\"{service}\"}}'), 0 FROM numbers({n})"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_streams");
        // Issue #399: this fixture seeds `log_streams` only (the index MV
        // fans it out), so without rollup rows every stream is inactive
        // in the requested window and both cases would answer with an
        // empty label set — the exact-cardinality claim would become
        // vacuous rather than false. Seeded here in one statement over
        // `numbers()`, matching the fixture's own idiom; `n` is 7,708 /
        // 4,533 rows, not a corpus.
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_metrics_5s (fingerprint, bucket_ns, count, bytes) \
                     SELECT {fp_base} + number, intDiv({now}, 5000000000) * 5000000000, 1, 10 \
                     FROM numbers({n})"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_metrics_5s activity");
    }

    let start = now - 3 * 24 * 3_600_000_000_000;
    let end = now + 60_000_000_000;
    for (service, key, _prefix, n, _fp_base) in fixtures {
        // `query=` scopes the aggregation to this fixture's streams, so
        // each response carries exactly its own key plus `service_name`.
        let query = format!("query=%7Bservice_name%3D%22{service}%22%7D");
        let path = format!("/api/logs/v1/detected_labels?{query}&start={start}&end={end}");
        let res = http_get(port, &path, false);
        assert_eq!(res.status, 200, "body: {}", res.body);
        let json: serde_json::Value = serde_json::from_str(&res.body).expect("detected_labels");
        // Label-ascending is the documented order (§2.6.2), and
        // `service_name` sorts AFTER `pod` but BEFORE `svc` — build the
        // expectation rather than hard-coding one fixture's order.
        let mut expected = vec![(key, n), ("service_name", 1)];
        expected.sort_by_key(|(label, _)| *label);
        let expected: Vec<serde_json::Value> = expected
            .into_iter()
            .map(|(label, cardinality)| {
                serde_json::json!({"label": label, "cardinality": cardinality})
            })
            .collect();
        assert_eq!(
            json["detectedLabels"],
            serde_json::Value::Array(expected),
            "the EXACT count is the contract at a reference divergence point \
             ({key}: {n} distinct values): {json}"
        );

        let aliased = http_get(
            port,
            &format!("/loki/api/v1/detected_labels?{query}&start={start}&end={end}"),
            false,
        );
        assert_eq!(aliased.status, 200, "alias body: {}", aliased.body);
        assert_eq!(
            aliased.body, res.body,
            "the /loki/api/v1 alias must be byte-identical to native"
        );
    }

    drop_db(db).await;
}

/// The issue #399 window fixture, shared by the two cases below — each
/// seeds its OWN database so either can be run alone.
///
/// Three streams, one `log_samples` line each. Returns `b`, the 5s bucket
/// containing `now - 3600s`: stream 3's only line sits at `b + 1s`, i.e.
/// INSIDE bucket `b`, which starts BEFORE it. That is the whole point of
/// the fixture — real `log_samples` rows, so the shipped rollup MV (not
/// the test) decides which bucket each line lands in, and the bucket
/// boundary under test is production's.
///
/// | fp | labels | line at |
/// |---|---|---|
/// | 1 | `inwin=yes, job=fix, service_name=fix` | `now − 60s` |
/// | 2 | `job=stale, outwin=yes, service_name=stale` | `now − 6h` |
/// | 3 | `edge=yes, job=fix, service_name=fix` | `b + 1s` |
///
/// These cases make no `/detected_fields` assertions, so real sample rows
/// are safe here (unlike `detected_labels_and_fields_end_to_end`, whose
/// fps 2 and 3 must stay sample-free and get `log_metrics_5s` rows direct).
async fn seed_window_fixture(client: &ChClient, db: &str, now: i64) -> i64 {
    let b = (now - 3_600_000_000_000) / 5_000_000_000 * 5_000_000_000;
    let fixture: [(u64, &str, &str, i64); 3] = [
        (
            1,
            "fix",
            r#"{"inwin":"yes","job":"fix","service_name":"fix"}"#,
            now - 60_000_000_000,
        ),
        (
            2,
            "stale",
            r#"{"job":"stale","outwin":"yes","service_name":"stale"}"#,
            now - 6 * 3_600_000_000_000,
        ),
        (
            3,
            "fix",
            r#"{"edge":"yes","job":"fix","service_name":"fix"}"#,
            b + 1_000_000_000,
        ),
    ];
    for (fp, service, labels, ts) in fixture {
        seed_stream(client, db, ts, fp, service, labels).await;
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, \
                     body, structured_metadata) VALUES ('{service}', {fp}, {ts}, 0, 'line', '')"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_samples");
    }
    b
}

/// Issue #399 AC3 — `/detected_labels` answers the requested window, not
/// the calendar month containing it.
///
/// **Measured on `c7649da` (the pre-fix tree):** the response was a pure
/// function of the months overlapping `[start, end]`. A ten-minute window
/// returned every label in August and reported `job` cardinality 2 where
/// only one `job` value existed inside it.
///
/// **The reference bounds this endpoint by the window too, but coarsely.**
/// `grafana/loki:3.7.4` (`b318f2829f0ae2094ab3a1e90780450e9e4b03be`,
/// tsdb/v13, index period 24h, flushed) answered a `[T−2h, T−1h]` window
/// with a label whose only line sat ~5h outside it, and answered the
/// previous day's window with `[]`: `MultiIndex.LabelNames` filters index
/// FILES by `forMatchingIndices` (`pkg/storage/stores/shipper/indexshipper/tsdb/multi_file_index.go:115-132`
/// @ v3.7.4) while `TSDBIndex.LabelNames` ignores `from`/`through`
/// entirely (`single_file_index.go:304-310` @ v3.7.4 — the signature is
/// literally `_, _ model.Time`). Ours lands at one rollup bucket per edge,
/// strictly tighter; registered as
/// `detected-labels-window-scoped-to-rollup-bucket` in
/// docs/benchmarks/logs-differential-ledger.md.
///
/// The bucket-edge half of #399 is
/// [`detected_labels_keeps_a_sample_in_the_bucket_that_contains_start`],
/// deliberately its own named test rather than a second request block
/// here: it discriminates a different wrong implementation, and a named
/// property is what a regression report can point at.
#[tokio::test(flavor = "multi_thread")]
async fn detected_labels_is_scoped_to_the_requested_window() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_167;
    let db = "pulsus_detected_it_window";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[]);
    let client = data_client(db).await;

    let now = now_ns();
    seed_window_fixture(&client, db, now).await;

    let start = now - 600_000_000_000;
    let res = http_get(
        port,
        &format!("/api/logs/v1/detected_labels?start={start}&end={now}"),
        false,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("detected_labels JSON");
    assert_eq!(
        json["detectedLabels"],
        serde_json::json!([
            {"label": "inwin", "cardinality": 1},
            {"label": "job", "cardinality": 1},
            {"label": "service_name", "cardinality": 1},
        ]),
        "`outwin` and `edge` are outside the window and `job` has ONE value in it; \
         the pre-fix tree returned every August label and job:2: {json}"
    );

    drop_db(db).await;
}

/// Issue #399 AC4 — **THE DISCRIMINATOR.** A sample in the bucket
/// CONTAINING `start` must survive the activity filter.
///
/// The window is `[b + 500ms, b + 4s]`, which holds only stream 3's line
/// (at `b + 1s`). That line's rollup bucket is `b` — at or BEFORE
/// `start` — because the MV stores `bucket_ns = intDiv(timestamp_ns,
/// res) * res`. So the lower bound must floor to the containing bucket,
/// not compare against `start_ns` itself.
///
/// **What it rules out, measured rather than argued.** Copying
/// `sql::log_stats_rollup`'s half-open `bucket_ns > start_ns` — which is
/// correct on the SAMPLE axis and wrong on the BUCKET axis — is the
/// plausible wrong fix. Introduced deliberately against this fixture on
/// `clickhouse/clickhouse-server:24.8.14.39`, it answers
/// `{"detectedLabels":[]}`: it silently loses a label whose line is
/// genuinely inside the window. This case fails under that fix (via
/// `edge` vanishing) and on the pre-#399 tree (via `outwin` appearing),
/// and passes only for the floored bound.
///
/// Kept as its own named test rather than folded into
/// [`detected_labels_is_scoped_to_the_requested_window`]: the property
/// has a name so a future reader can find it and a regression can point
/// at it.
#[tokio::test(flavor = "multi_thread")]
async fn detected_labels_keeps_a_sample_in_the_bucket_that_contains_start() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let port = 31_169;
    let db = "pulsus_detected_it_bucket_edge";
    drop_db(db).await;
    let _guard = spawn_ready(port, db, &[]);
    let client = data_client(db).await;

    let now = now_ns();
    let b = seed_window_fixture(&client, db, now).await;

    let edge_start = b + 500_000_000;
    let edge_end = b + 4_000_000_000;
    let res = http_get(
        port,
        &format!("/api/logs/v1/detected_labels?start={edge_start}&end={edge_end}"),
        false,
    );
    assert_eq!(res.status, 200, "body: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("edge JSON");
    assert_eq!(
        json["detectedLabels"],
        serde_json::json!([
            {"label": "edge", "cardinality": 1},
            {"label": "job", "cardinality": 1},
            {"label": "service_name", "cardinality": 1},
        ]),
        "the lower bound must floor to the bucket CONTAINING start, so `edge` is kept \
         while `inwin`/`outwin` stay out: {json}"
    );

    drop_db(db).await;
}
