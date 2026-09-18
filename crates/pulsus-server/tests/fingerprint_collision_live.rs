//! Issue #498 criterion 7: **the end-to-end answers, with the two recorded
//! colliding label-set pairs ingested through the shipped routes.**
//!
//! The defect this issue removes is not visible in any SQL text. Two label
//! sets whose 64-bit hash collided shared one identity, so the store
//! answered a query about one series with the other series' data — and it
//! did so with a `200` and no warning. The only check that can see that is
//! the answer.
//!
//! **The fixture**, constructed at issue #498's base by Floyd's cycle
//! detection over `f(x) = hash("service_name" 0xFF <16 hex of x> 0xFF)`
//! through the shipped functions:
//!
//! ```text
//!   label set                            64-bit              128-bit (decimal)
//!   {service_name="f47da23e240616e1"}    0xf468babd2484e99c  324875417358418231230721791952787510312
//!   {service_name="1d9f6a817a133b68"}    0xf468babd2484e99c  324875417358418231224418266131907151976
//!   {service_name="daf4c59f97f55535"}    0x698b6328161f1bde  169254982756129762653057753242817993694
//!   {service_name="4714b1c6956770d2"}    0x698b6328161f1bde  290831904718675152733872074466791463902
//! ```
//!
//! The logs pair collides under `cityHash64`, so it shares the **high** 64
//! bits, which lead the sort key; the metrics pair collides under
//! `xxHash64`, so it differs there. A build that widened the column but
//! compared only the leading word passes one and fails the other.
//!
//! **Every request carries explicit time bounds.** Both families default a
//! missing `start` to a one-hour window anchored on `now`
//! (`prom_api/handlers.rs`, `logs_api/handlers.rs`), so a fixed instant in
//! the past ages out of the default window and the suite would report an
//! empty result rather than a wrong answer. Nothing here omits a bound.
//!
//! **The instants are derived from one `T0` taken at the start of the
//! run.** The metric answers depend on the instant: at `T0+1s` only the
//! first sample exists and both a broken and a repaired build answer the
//! same number, so the queries evaluate at `T0+16s`, the first instant at
//! which the merge of the two series is visible.
//!
//! ```text
//!   PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
//!     PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!     cargo test -p pulsus-server --test fingerprint_collision_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::drop_db;

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use prost::Message;
use pulsus_write::protocols::loki_push::{EntryAdapter, PushRequest, StreamAdapter, Timestamp};
use pulsus_write::protocols::remote_write::{Label, Sample, TimeSeries, WriteRequest};

/// The logs pair: one `cityHash64` value, two label sets.
const LOGS_A: &str = "f47da23e240616e1";
const LOGS_B: &str = "1d9f6a817a133b68";
/// The metrics pair: one `xxHash64` value, two label sets.
const METRICS_A: &str = "daf4c59f97f55535";
const METRICS_B: &str = "4714b1c6956770d2";

/// The two fixed listener ports this suite binds, one per test: the
/// suites in this crate run as separate processes, so each test needs its
/// own. Written as integer literals because the uniqueness guard reads
/// them (`live_port_uniqueness.rs`) and a port built by arithmetic is
/// invisible to it.
const ANSWERS_PORT: u16 = 31_620;
const SURFACE_PORT: u16 = 31_621;

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
    .expect("fits i64")
}

struct HttpResponse {
    status: u16,
    body: String,
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
        let Ok(size_str) = std::str::from_utf8(&raw[..line_end]) else {
            break;
        };
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

fn http_request(
    port: u16,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> Option<HttpResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();

    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    if method != "GET" {
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    head.push_str("\r\n");

    let mut request = head.into_bytes();
    request.extend_from_slice(body);
    stream.write_all(&request).ok()?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;
    let split_at = find_subslice(&buf, b"\r\n\r\n")?;
    let head_text = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let raw_body = &buf[split_at + 4..];

    let mut lines = head_text.lines();
    let status = lines
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse::<u16>()
        .ok()?;
    let headers: HashMap<String, String> = lines
        .filter_map(|line| {
            let (k, v) = line.split_once(": ")?;
            Some((k.to_ascii_lowercase(), v.to_string()))
        })
        .collect();
    let dechunked = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v == "chunked")
    {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    Some(HttpResponse {
        status,
        body: String::from_utf8_lossy(&dechunked).into_owned(),
    })
}

fn http_get(port: u16, path: &str) -> HttpResponse {
    http_request(port, "GET", path, None, &[]).expect("request reachable")
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
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

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_ready(port: u16, db: &str) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        // The label cache's default TTL is 60s; a shorter one keeps the
        // metrics half inside this test's own deadline. The first attempt
        // at this measurement returned empty for exactly that reason.
        .env("PULSUS_CACHE_TTL", "1s")
        .env("PULSUS_COMPAT_ENDPOINTS", "true")
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db);
    let guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if http_request(port, "GET", "/ready", None, &[]).is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port}, db {db})");
}

/// One Loki push carrying both colliding streams in ONE request — the
/// shape that used to drop the loser before ClickHouse ever saw it,
/// because the writer's per-request `seen_streams` set keyed on the
/// fingerprint alone.
fn logs_push_body(ts_ns: i64) -> Vec<u8> {
    let entry = |line: &str| EntryAdapter {
        timestamp: Some(Timestamp {
            seconds: ts_ns / 1_000_000_000,
            nanos: (ts_ns % 1_000_000_000) as i32,
        }),
        line: line.to_string(),
        structured_metadata: Vec::new(),
    };
    let req = PushRequest {
        streams: vec![
            StreamAdapter {
                labels: format!(r#"{{service_name="{LOGS_A}"}}"#),
                entries: vec![entry("line from stream A")],
            },
            StreamAdapter {
                labels: format!(r#"{{service_name="{LOGS_B}"}}"#),
                entries: vec![entry("line from stream B")],
            },
        ],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the push")
}

fn remote_write_body(series: &[(&str, f64, i64)]) -> Vec<u8> {
    let req = WriteRequest {
        timeseries: series
            .iter()
            .map(|(service, value, ts_ms)| TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".to_string(),
                        value: "pulsus_probe".to_string(),
                    },
                    Label {
                        name: "service_name".to_string(),
                        value: (*service).to_string(),
                    },
                ],
                samples: vec![Sample {
                    value: *value,
                    timestamp: *ts_ms,
                }],
                histograms: vec![],
            })
            .collect(),
        metadata: vec![],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress the write request")
}

/// Every returned stream's label map paired with its lines, for one
/// `query_range` over an explicit window.
fn query_streams(
    port: u16,
    selector: &str,
    t0_ns: i64,
) -> Vec<(BTreeMap<String, String>, Vec<String>)> {
    let start = t0_ns - 3_600_000_000_000;
    let end = t0_ns + 3_600_000_000_000;
    let path = format!(
        "/api/logs/v1/query_range?query={}&start={start}&end={end}&limit=100",
        urlencode(selector)
    );
    let res = http_get(port, &path);
    assert_eq!(res.status, 200, "query_range status (body: {})", res.body);
    let json: serde_json::Value =
        serde_json::from_str(&res.body).unwrap_or_else(|e| panic!("json: {e}: {}", res.body));
    let empty = Vec::new();
    json["data"]["result"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .map(|stream| {
            let labels = stream["stream"]
                .as_object()
                .expect("stream labels")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect();
            let lines = stream["values"]
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .map(|v| v[1].as_str().unwrap_or_default().to_string())
                .collect();
            (labels, lines)
        })
        .collect()
}

/// The `{label -> value}` of one instant-vector sample, with its value.
fn instant_series(port: u16, query: &str, at_ns: i64) -> Vec<(BTreeMap<String, String>, String)> {
    let time = at_ns / 1_000_000_000;
    let path = format!("/api/v1/query?query={}&time={time}", urlencode(query));
    let res = http_get(port, &path);
    assert_eq!(res.status, 200, "instant query status (body: {})", res.body);
    let json: serde_json::Value =
        serde_json::from_str(&res.body).unwrap_or_else(|e| panic!("json: {e}: {}", res.body));
    let empty = Vec::new();
    let mut out: Vec<(BTreeMap<String, String>, String)> = json["data"]["result"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .map(|s| {
            let labels = s["metric"]
                .as_object()
                .expect("metric labels")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect();
            (
                labels,
                s["value"][1].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

/// **The two colliding pairs are two streams and two series, end to end.**
#[tokio::test(flavor = "multi_thread")]
async fn the_recorded_colliding_pairs_answer_as_two_streams_and_two_series() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_server_it_fp_collision");
    drop_db(&db).await;
    let _guard = spawn_ready(ANSWERS_PORT, &db);

    // `T0` is taken once and every bound below is derived from it.
    let t0_ns = now_ns();
    let t0_ms = t0_ns / 1_000_000;

    // ---- logs -------------------------------------------------------
    let res = http_request(
        ANSWERS_PORT,
        "POST",
        "/loki/api/v1/push",
        Some("application/x-protobuf"),
        &logs_push_body(t0_ns),
    )
    .expect("push reachable");
    assert_eq!(res.status, 204, "push status (body: {})", res.body);

    // Each selector names ONE stream and gets that stream's own line,
    // labelled with its own label set. Before the widening, `{…="f47…"}`
    // returned B's line labelled `1d9…`, and `{…="1d9…"}` returned `[]`.
    for (service, line) in [
        (LOGS_A, "line from stream A"),
        (LOGS_B, "line from stream B"),
    ] {
        let got = query_streams(
            ANSWERS_PORT,
            &format!(r#"{{service_name="{service}"}}"#),
            t0_ns,
        );
        assert_eq!(
            got.len(),
            1,
            "{{service_name=\"{service}\"}} answered {} streams, not one: {got:?}",
            got.len()
        );
        assert_eq!(
            got[0].0.get("service_name").map(String::as_str),
            Some(service),
            "the returned stream carries the other stream's labels: {got:?}"
        );
        assert_eq!(
            got[0].1,
            vec![line.to_string()],
            "the returned lines are not {service}'s own: {got:?}"
        );
    }

    // The regex selector reaches BOTH through `log_streams_idx`'s
    // `HAVING uniqExact(key, val) = 1`, which a shared fingerprint used to
    // fail for both streams at once — not just the loser.
    let both = query_streams(
        ANSWERS_PORT,
        &format!(r#"{{service_name=~"{LOGS_A}|{LOGS_B}"}}"#),
        t0_ns,
    );
    assert_eq!(
        both.len(),
        2,
        "the regex selector answered {} streams, not two: {both:?}",
        both.len()
    );
    for (labels, lines) in &both {
        assert_eq!(lines.len(), 1, "each stream carries one line: {both:?}");
        assert!(
            labels
                .get("service_name")
                .is_some_and(|s| s == LOGS_A || s == LOGS_B),
            "an unexpected stream came back: {both:?}"
        );
    }

    // And the metric form over the same selector: two series, one each.
    let start = t0_ns - 3_600_000_000_000;
    let end = t0_ns + 3_600_000_000_000;
    let agg = format!(
        r#"sum by (service_name) (count_over_time({{service_name=~"{LOGS_A}|{LOGS_B}"}}[1h]))"#
    );
    let path = format!(
        "/api/logs/v1/query_range?query={}&start={start}&end={end}&step=3600",
        urlencode(&agg)
    );
    let res = http_get(ANSWERS_PORT, &path);
    assert_eq!(res.status, 200, "aggregation status (body: {})", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("json");
    let empty = Vec::new();
    let series = json["data"]["result"].as_array().unwrap_or(&empty);
    assert_eq!(
        series.len(),
        2,
        "the aggregation answered {} series, not two: {}",
        series.len(),
        res.body
    );

    // ---- metrics ----------------------------------------------------
    // A at T0, B at T0+15s: at T0+1s only A has a sample and both a broken
    // and a repaired build answer 11, so nothing is asserted before the
    // merge is visible.
    let res = http_request(
        ANSWERS_PORT,
        "POST",
        "/api/v1/write",
        Some("application/x-protobuf"),
        &remote_write_body(&[(METRICS_A, 11.0, t0_ms), (METRICS_B, 22.0, t0_ms + 15_000)]),
    )
    .expect("remote write reachable");
    assert_eq!(res.status, 204, "remote write status (body: {})", res.body);

    let at = t0_ns + 16_000_000_000;

    // The label cache resolves a selector to fingerprints, so the series
    // must be resident before the answer means anything. The TTL above is
    // 1s; poll rather than sleep a fixed amount.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut resident = false;
    while Instant::now() < deadline {
        if instant_series(ANSWERS_PORT, "count(pulsus_probe)", at)
            .first()
            .is_some_and(|(_, v)| v == "2")
        {
            resident = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        resident,
        "`count(pulsus_probe)` never reached 2 within 60s; before the widening it answered 1, \
         because both series shared one identity"
    );

    // **The sharpest single statement of the defect:** the selector names
    // one series and the value used to come from the other.
    for (service, want) in [(METRICS_A, "11"), (METRICS_B, "22")] {
        let got = instant_series(
            ANSWERS_PORT,
            &format!(r#"pulsus_probe{{service_name="{service}"}}"#),
            at,
        );
        assert_eq!(
            got.len(),
            1,
            "pulsus_probe{{service_name=\"{service}\"}} answered {} series, not one: {got:?}",
            got.len()
        );
        assert_eq!(
            got[0].0.get("service_name").map(String::as_str),
            Some(service),
            "the answer carries the other series' labels: {got:?}"
        );
        assert_eq!(
            got[0].1, want,
            "pulsus_probe{{service_name=\"{service}\"}} answered {} rather than {want}",
            got[0].1
        );
    }

    // `min`/`max` over the range: one label set used to carry both 11 and
    // 22, and no series in the fixture has both.
    for (op, a, b) in [("max_over_time", "11", "22"), ("min_over_time", "11", "22")] {
        let got = instant_series(ANSWERS_PORT, &format!("{op}(pulsus_probe[2m])"), at);
        let by_service: BTreeMap<String, String> = got
            .iter()
            .map(|(labels, v)| {
                (
                    labels.get("service_name").cloned().unwrap_or_default(),
                    v.clone(),
                )
            })
            .collect();
        assert_eq!(
            by_service.get(METRICS_A).map(String::as_str),
            Some(a),
            "{op}: {METRICS_A} answered {by_service:?}"
        );
        assert_eq!(
            by_service.get(METRICS_B).map(String::as_str),
            Some(b),
            "{op}: {METRICS_B} answered {by_service:?}"
        );
    }

    // `/api/v1/series` with BOTH bounds: with the start omitted and an end
    // past the fixture it returns `[]`, because the default window is one
    // hour anchored on `now`.
    let start_s = t0_ms / 1_000;
    let end_s = start_s + 16;
    let path = format!(
        "/api/v1/series?match[]={}&start={start_s}&end={end_s}",
        urlencode("pulsus_probe")
    );
    let res = http_get(ANSWERS_PORT, &path);
    assert_eq!(res.status, 200, "series status (body: {})", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("json");
    let listed = json["data"].as_array().unwrap_or(&empty);
    assert_eq!(
        listed.len(),
        2,
        "/api/v1/series listed {} series, not two: {}",
        listed.len(),
        res.body
    );

    drop_db(&db).await;
}

/// **The accept/reject surface does not move.** No route takes a
/// fingerprint as a client input, so widening it cannot change what is
/// accepted; this pins the two cases the issue names, against the same
/// running server.
#[tokio::test(flavor = "multi_thread")]
async fn the_accept_reject_surface_is_unchanged() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }
    let db = pulsus_testkit::test_db("pulsus_server_it_fp_surface");
    drop_db(&db).await;
    let port = SURFACE_PORT;
    let _guard = spawn_ready(port, &db);

    let now_s = now_ns() / 1_000_000_000;
    // A malformed selector is still a 400.
    let res = http_get(
        port,
        &format!(
            "/api/v1/query?query={}&time={now_s}",
            urlencode("pulsus_probe{service_name=")
        ),
    );
    assert_eq!(
        res.status, 400,
        "a malformed selector is a 400: {}",
        res.body
    );

    // An unknown metric name is still a 200 with an empty result.
    let res = http_get(
        port,
        &format!(
            "/api/v1/query?query={}&time={now_s}",
            urlencode("pulsus_absent_metric")
        ),
    );
    assert_eq!(res.status, 200, "an unknown name is a 200: {}", res.body);
    let json: serde_json::Value = serde_json::from_str(&res.body).expect("json");
    assert_eq!(
        json["data"]["result"].as_array().map(Vec::len),
        Some(0),
        "an unknown name answers an empty result: {}",
        res.body
    );

    drop_db(&db).await;
}
