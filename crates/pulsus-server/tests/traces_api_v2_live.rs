//! Issue #583 (R9): the window rule at the boundary, over HTTP against a
//! spawned `pulsusdb` and a live ClickHouse.
//!
//! One rule — `start <= ts < end` — on every **request** window. Five
//! cases pin where that is visible to a client:
//!
//! * `T-B1` a span at exactly `start` is returned;
//! * `T-B2` a span at exactly `end` is not;
//! * `T-B3` a search ending at the next UTC midnight reads one day's
//!   partitions, not two, and returns the trace whose newest span is the
//!   window's first nanosecond;
//! * `T-B6` **guard** — the service-graph window is already `[start,
//!   end)` and must stay so;
//! * `T-B8`/`T-B8t` **guards** — `compare()`'s `start`/`end` arguments
//!   are NOT a request window. They are operands of the query language,
//!   the reference defines them as `(start, end]`, and they do not move.
//!
//! ## The fixture is derived from the clock, not written as literals
//!
//! `trace_spans` carries a delete TTL at `retention_days` (default 7,
//! `crates/pulsus-config/src/model.rs:151`) with `ttl_only_drop_parts =
//! 1`. Fixed calendar timestamps fall outside that window as soon as this
//! file is a week old: the rows are dropped, every case returns empty,
//! and the two cases whose expected answer is "nothing" would pass over an
//! empty table — a green run that tested nothing. So the fixture is
//! anchored on **yesterday's** UTC day start, which keeps every span and
//! every request window in the past, less than two days old, and six days
//! inside the default retention; and it makes this morning's midnight an
//! instant that has already passed, which `T-B3` needs.
//!
//! Both cases whose expected answer is empty carry their own non-vacuity
//! request, so an empty table cannot satisfy them.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test traces_api_v2_live
//! ```
//!
//! Fixed loopback ports are declared inline per test (`let port = 31_2NN;`);
//! `live_port_uniqueness.rs` checks no two suites declare the same one.

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::HashMap;
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
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping
/// when the gate is absent in a live CI job, so a lost `env:` block reddens
/// the build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

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
    extra: &[(&str, &str)],
) -> Option<RawResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();

    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in extra {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
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
    let res = request(port, "GET", path, None, &[])
        .unwrap_or_else(|| panic!("{ctx}: request must be reachable (transport failure)"));
    assert_eq!(
        res.status,
        200,
        "{ctx}: must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
    res
}

// ---------------------------------------------------------------------
// Process lifecycle.
// ---------------------------------------------------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_ready(port: u16, db: &ScopedDb) -> ChildGuard {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    cmd.env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db.name());
    let guard = ChildGuard(cmd.spawn().expect("spawn pulsusdb"));

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if request(port, "GET", "/ready", None, &[]).is_some_and(|r| r.status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s");
}

// ---------------------------------------------------------------------
// Fixture B (functional-requirements.md §8.2), anchored on yesterday.
// ---------------------------------------------------------------------

const NS_PER_DAY: i64 = 86_400_000_000_000;

/// The five spans' offsets from the anchoring UTC day start — §8.2's own
/// arithmetic, with only the anchor moved off a calendar date.
const B1_OFF: i64 = 59_646_486_853_636;
const B2_OFF: i64 = 86_399_999_999_999;
const B3_OFF: i64 = 30_000_000_000;
const B4_OFF: i64 = 3_600_000_000_000;
/// The `gw` client span sits six nanoseconds before b1, so the window
/// `[b1 - 1, b1)` holds no span of the trace at all once b1 is out.
const G0_LEAD_NS: i64 = 6;

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// Yesterday's UTC day start.
fn day_start_ns() -> i64 {
    (now_ns().div_euclid(NS_PER_DAY) - 1) * NS_PER_DAY
}

/// `toDate('YYYY-MM-DD')` for the UTC day a nanosecond falls in, computed
/// with `chrono` rather than with the builder under test.
fn date_literal(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let day = chrono::DateTime::from_timestamp(secs, 0).expect("a representable instant");
    format!("toDate('{}')", day.format("%Y-%m-%d"))
}

const TRACE_ID: [u8; 16] = [0xb0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01];

fn sid(n: u8) -> [u8; 8] {
    let mut id = [0u8; 8];
    id[7] = n;
    id
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn kv_str(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn span(
    span_id: [u8; 8],
    parent_id: Option<[u8; 8]>,
    name: &str,
    kind: SpanKind,
    start_ns: i64,
    attrs: Vec<KeyValue>,
) -> Span {
    Span {
        trace_id: TRACE_ID.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: parent_id.map(|p| p.to_vec()).unwrap_or_default(),
        name: name.to_string(),
        kind: kind as i32,
        start_time_unix_nano: u64::try_from(start_ns).expect("a post-epoch fixture"),
        end_time_unix_nano: u64::try_from(start_ns + 1_000_000).expect("a post-epoch fixture"),
        attributes: attrs,
        ..Default::default()
    }
}

/// One `POST /v1/traces` for one service (sync — a `200` means the rows
/// are flushed and read-visible).
fn ingest(port: u16, service: &str, spans: Vec<Span>, ctx: &str) {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv_str("service.name", service)],
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
        &[],
    )
    .unwrap_or_else(|| panic!("{ctx}: ingest must be reachable"));
    assert_eq!(
        res.status,
        200,
        "{ctx}: sync ingest must succeed, body {:?}",
        String::from_utf8_lossy(&res.body)
    );
}

/// Seeds fixture B: g0 under `gw`, b1-b4 under `checkout`, all in one
/// trace. Returns the day start every offset is measured from.
fn seed_fixture_b(port: u16) -> i64 {
    let day = day_start_ns();
    ingest(
        port,
        "gw",
        vec![span(
            sid(1),
            None,
            "gw.call",
            SpanKind::Client,
            day + B1_OFF - G0_LEAD_NS,
            vec![],
        )],
        "seed g0",
    );
    ingest(
        port,
        "checkout",
        vec![
            span(
                sid(2),
                Some(sid(1)),
                "op.b1",
                SpanKind::Server,
                day + B1_OFF,
                vec![],
            ),
            span(
                sid(3),
                None,
                "op.b2",
                SpanKind::Internal,
                day + B2_OFF,
                vec![],
            ),
            span(
                sid(4),
                None,
                "op.b3",
                SpanKind::Internal,
                day + B3_OFF,
                vec![kv_str("http.route", "/only-before"), kv_str("tenant", "t1")],
            ),
            span(
                sid(5),
                None,
                "op.b4",
                SpanKind::Internal,
                day + B4_OFF,
                vec![kv_str("http.route", "/inside"), kv_str("tenant", "t1")],
            ),
        ],
        "seed b1-b4",
    );
    day
}

// ---------------------------------------------------------------------
// Search-side helpers.
// ---------------------------------------------------------------------

/// Minimal percent-encoding for query-string values.
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

/// The trace ids an empty-selector search over `[start_ns, end_ns)`
/// returns. Nanosecond bounds: the trace surface reads an integer of
/// magnitude >= 1e12 as nanoseconds (`traces_api/params.rs`).
fn search_ids(port: u16, start_ns: i64, end_ns: i64, ctx: &str) -> Vec<String> {
    let path = format!("/api/traces/v1/search?q=%7B%7D&start={start_ns}&end={end_ns}&limit=20");
    let res = get(port, &path, ctx);
    let json = res.json(ctx);
    json["traces"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: a traces array, got {json}"))
        .iter()
        .map(|t| {
            t["traceID"]
                .as_str()
                .unwrap_or_else(|| panic!("{ctx}: a traceID string, got {t}"))
                .to_string()
        })
        .collect()
}

/// The phase-1 generator statement a search ran, read from the same
/// request sent with `X-Pulsus-Explain: 1`: the one stage whose SQL
/// carries `AS bound_ts`.
fn generator_sql(port: u16, start_ns: i64, end_ns: i64, ctx: &str) -> String {
    let path = format!("/api/traces/v1/search?q=%7B%7D&start={start_ns}&end={end_ns}&limit=20");
    let raw = request(port, "GET", &path, None, &[("X-Pulsus-Explain", "1")])
        .unwrap_or_else(|| panic!("{ctx}: explain request must be reachable"));
    let json: serde_json::Value = serde_json::from_slice(&raw.body)
        .unwrap_or_else(|e| panic!("{ctx}: explain body is not JSON: {e}"));
    let sqls: Vec<String> = json["explain"]["stages"]
        .as_array()
        .map(|stages| {
            stages
                .iter()
                .filter_map(|s| s["sql"].as_str())
                .filter(|sql| sql.contains("AS bound_ts"))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    match sqls.as_slice() {
        [one] => one.clone(),
        other => panic!(
            "{ctx}: expected one generator stage, found {}: {json}",
            other.len()
        ),
    }
}

// ---------------------------------------------------------------------
// T-B1 / T-B2 — the two ends of the search window.
// ---------------------------------------------------------------------

/// `T-B1`: a span at exactly `start` is matched, so the trace comes back.
///
/// The window `[b1, b1 + 1)` holds exactly one nanosecond and b1 sits on
/// it. On the unchanged tree the search window renders `ts > start`, so b1
/// is dropped and the answer is empty — and the recency candidate bound
/// renders `ts_max > start`, which drops the trace before phase 2 as well.
#[tokio::test(flavor = "multi_thread")]
async fn a_span_at_exactly_the_window_start_is_returned() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_253;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_start")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let b1 = day + B1_OFF;

    assert_eq!(
        search_ids(port, b1, b1 + 1, "T-B1"),
        vec![hex(&TRACE_ID)],
        "T-B1: a span at exactly start is inside [start, end)"
    );
}

/// `T-B2`: a span at exactly `end` is not matched, so the trace does not
/// come back.
///
/// The window `[b1 - 1, b1)` holds no span of the trace once b1 is
/// excluded: g0 is six nanoseconds earlier and the other three are hours
/// away. On the unchanged tree the window renders `ts <= end`, so b1 is
/// returned.
///
/// **Non-vacuity:** the same seeded fixture answers `T-B1`'s request with
/// the trace, so an empty table cannot satisfy this case.
#[tokio::test(flavor = "multi_thread")]
async fn a_span_at_exactly_the_window_end_is_not_returned() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_254;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_end")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let b1 = day + B1_OFF;

    assert_eq!(
        search_ids(port, b1, b1 + 1, "T-B2 non-vacuity"),
        vec![hex(&TRACE_ID)],
        "T-B2: the fixture is there — [b1, b1 + 1) returns the trace"
    );
    assert_eq!(
        search_ids(port, b1 - 1, b1, "T-B2"),
        Vec::<String>::new(),
        "T-B2: a span at exactly end is outside [start, end)"
    );
}

// ---------------------------------------------------------------------
// T-B3 — the day bound comes from the last included nanosecond.
// ---------------------------------------------------------------------

/// `T-B3`: a search ending at the next UTC midnight reads one day's
/// partitions, not two, and still answers.
///
/// (a) b2 is the trace's newest span and the window opens exactly on it.
/// On the unchanged tree `ts_max > start` drops the trace from the
/// candidate set outright, and `ts > start` would drop b2 from phase 2
/// anyway. (b) the generator's day clause names one day; on the unchanged
/// tree it is rendered from `end_ns` and names the next day as well.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_ending_at_midnight_reads_one_days_partitions() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_255;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_midnight")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let (b2, midnight) = (day + B2_OFF, day + NS_PER_DAY);

    assert_eq!(
        search_ids(port, b2, midnight, "T-B3(a)"),
        vec![hex(&TRACE_ID)],
        "T-B3(a): the trace whose newest span is exactly start is a candidate and an answer"
    );

    let sql = generator_sql(port, b2, midnight, "T-B3(b)");
    let one_day = date_literal(day);
    assert!(
        sql.contains(&format!("date >= {one_day} AND date <= {one_day}")),
        "T-B3(b): the day bound comes from the last included nanosecond, so a window ending \
         exactly at midnight names one day:\n{sql}"
    );
}

// ---------------------------------------------------------------------
// T-B6 — guard: the service-graph window stays half-open.
// ---------------------------------------------------------------------

/// `(client, server, connectionType)` triples the service graph reports.
fn graph_edges(port: u16, start_ns: i64, end_ns: i64, ctx: &str) -> Vec<(String, String, String)> {
    let path = format!("/api/traces/v1/service_graph?start={start_ns}&end={end_ns}");
    let res = get(port, &path, ctx);
    let json = res.json(ctx);
    json["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: an edges array, got {json}"))
        .iter()
        .map(|e| {
            (
                e["client"].as_str().unwrap_or_default().to_string(),
                e["server"].as_str().unwrap_or_default().to_string(),
                e["connectionType"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

/// `T-B6` **guard**: the `gw → checkout` edge is absent when its server
/// half starts at exactly `end`. Passes today and must keep passing.
///
/// It discriminates: g0 is strictly inside the window under both
/// conventions, and b1 sits at exactly `end`, so `[start, end)` excludes
/// b1 and reports no edge while `(start, end]` would report one. An edge
/// is reported only when both halves are in-window (`docs/api.md` §4.5).
///
/// **Non-vacuity:** the same fixture, one nanosecond wider, reports the
/// edge.
#[tokio::test(flavor = "multi_thread")]
async fn the_service_graph_omits_an_edge_whose_server_half_sits_at_end() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_256;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_graph")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let b1 = day + B1_OFF;
    let edge = ("gw".to_string(), "checkout".to_string(), "rpc".to_string());

    assert!(
        graph_edges(port, b1 - G0_LEAD_NS - 1, b1 + 1, "T-B6 non-vacuity").contains(&edge),
        "T-B6: the fixture is there — with b1 inside the window the edge is reported"
    );
    assert!(
        !graph_edges(port, b1 - G0_LEAD_NS - 1, b1, "T-B6").contains(&edge),
        "T-B6: b1 sits at exactly end, so the server half is out of [start, end) and no edge \
         is reported"
    );
}

// ---------------------------------------------------------------------
// T-B8 / T-B8t — guards: compare()'s own window does not move.
// ---------------------------------------------------------------------

/// One string label of a response series, `None` when the series does not
/// carry it. A metrics label is an OTLP protojson `AnyValue`
/// (`{"stringValue": …}`).
fn series_label(series: &serde_json::Value, key: &str) -> Option<String> {
    series["labels"].as_array()?.iter().find_map(|l| {
        if l["key"].as_str()? == key {
            l["value"]["stringValue"].as_str().map(str::to_string)
        } else {
            None
        }
    })
}

/// The summed value of the `compare()` series carrying `__meta_type =
/// <meta>` and the label `<key> = <value>`, over the whole emitted grid.
/// `None` when the answer carries no such series. A sample's `value` is
/// omitted when zero (protojson default omission), which reads as 0.
fn compare_series_total(
    json: &serde_json::Value,
    meta: &str,
    key: &str,
    value: &str,
    ctx: &str,
) -> Option<f64> {
    json["series"]
        .as_array()
        .unwrap_or_else(|| panic!("{ctx}: a series array, got {json}"))
        .iter()
        .find(|s| {
            series_label(s, "__meta_type").as_deref() == Some(meta)
                && series_label(s, key).as_deref() == Some(value)
        })
        .map(|s| {
            s["samples"]
                .as_array()
                .map(|samples| {
                    samples
                        .iter()
                        .map(|p| p["value"].as_f64().unwrap_or(0.0))
                        .sum()
                })
                .unwrap_or(0.0)
        })
}

/// One `compare()` range query over the whole anchoring day, with the
/// selection window written out in nanoseconds.
fn compare_answer(
    port: u16,
    day: i64,
    sel_start: i64,
    sel_end: i64,
    ctx: &str,
) -> serde_json::Value {
    let q = format!(
        r#"{{span.tenant="t1"}} | compare({{span.http.route="/inside"}}, 10, {sel_start}, {sel_end})"#
    );
    let path = format!(
        "/api/traces/v1/metrics/query_range?q={}&start={}&end={}&step=3600s",
        enc(&q),
        day / 1_000_000_000,
        (day + NS_PER_DAY) / 1_000_000_000,
    );
    get(port, &path, ctx).json(ctx)
}

/// `T-B8` **guard**: `compare()`'s `start`/`end` arguments keep the
/// reference's `(start, end]`, at BOTH ends. Passes today and must keep
/// passing.
///
/// (a) b4 sits at exactly the `end` argument and counts in the
/// **selection**; (b) b4 sits at exactly the `start` argument and counts
/// in the **baseline**. A half-conversion — `>=` on the start with `<=`
/// left on the end — fails (b) while (a) stays green.
#[tokio::test(flavor = "multi_thread")]
async fn the_compare_selection_window_keeps_both_of_its_reference_ends() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_257;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_compare")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let (b3, b4) = (day + B3_OFF, day + B4_OFF);
    let route = "span.http.route";

    let a = compare_answer(port, day, b3, b4, "T-B8(a)");
    assert_eq!(
        compare_series_total(&a, "selection", route, "/inside", "T-B8(a)"),
        Some(1.0),
        "T-B8(a): b4 is at exactly the end argument, which the reference INCLUDES in the \
         selection\nbody: {a}"
    );
    assert_eq!(
        compare_series_total(&a, "baseline", route, "/inside", "T-B8(a)"),
        None,
        "T-B8(a): and it is therefore not in the baseline\nbody: {a}"
    );

    let b = compare_answer(port, day, b4, day + NS_PER_DAY, "T-B8(b)");
    assert_eq!(
        compare_series_total(&b, "baseline", route, "/inside", "T-B8(b)"),
        Some(1.0),
        "T-B8(b): b4 is at exactly the start argument, which the reference EXCLUDES from the \
         selection\nbody: {b}"
    );
    assert_eq!(
        compare_series_total(&b, "selection", route, "/inside", "T-B8(b)"),
        None,
        "T-B8(b): and it is therefore not in the selection\nbody: {b}"
    );
}

/// `T-B8t` **guard**: the selection window repartitions, it does not
/// filter, so for one attribute key `baseline_total + selection_total` is
/// the whole population whatever window is asked for. The outer filter
/// admits b3 and b4 only, so the sum is 2 for both of `T-B8`'s requests.
///
/// Neither half of `T-B8` can see a change that turned the window into a
/// filter; this one can. The two `*_total` denominators are per attribute
/// key and carry `key=nil` (`docs/api.md` §4.4), so the key is named.
#[tokio::test(flavor = "multi_thread")]
async fn the_compare_totals_cover_the_population_whatever_the_selection_window() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let port = 31_258;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_traces_api_v2_it_totals")).await;
    let _server = spawn_ready(port, &db);
    let day = seed_fixture_b(port);
    let (b3, b4) = (day + B3_OFF, day + B4_OFF);
    let route = "span.http.route";

    for (label, sel_start, sel_end) in [("T-B8t(a)", b3, b4), ("T-B8t(b)", b4, day + NS_PER_DAY)] {
        let json = compare_answer(port, day, sel_start, sel_end, label);
        let baseline =
            compare_series_total(&json, "baseline_total", route, "nil", label).unwrap_or(0.0);
        let selection =
            compare_series_total(&json, "selection_total", route, "nil", label).unwrap_or(0.0);
        assert_eq!(
            baseline + selection,
            2.0,
            "{label}: the two totals cover the whole population the outer filter admits \
             (b3 and b4)\nbody: {json}"
        );
    }
}
