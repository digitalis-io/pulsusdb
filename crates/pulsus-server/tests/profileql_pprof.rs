//! Issue #828: `SelectMergeProfile`, `SelectMergeStacktraces` with
//! `PROFILE_FORMAT_PPROF` and `GET /api/profiles/v1/export`, against the
//! `pulsusdb` binary and a live ClickHouse.
//!
//! Fixture P (plan §8.1) is pushed through `/api/profiles/v1/ingest` under
//! `t1` and, at 100× the values, under `t2`; an OTLP profile is pushed
//! through `/v1development/profiles`. Every answer is decoded and compared
//! as `NF`, and every request's statements are counted in
//! `system.query_log` by its `X-Pulsus-Query-Id`.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profileql_pprof
//! ```

#[path = "support/live_db.rs"]
mod live_db;
#[path = "../../pulsus-read/tests/profileql_pprof_nf/mod.rs"]
mod nf;

use live_db::ScopedDb;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use nf::{CPU, Nf, P1, P2, P4, PERIOD, UNIT};
use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueKind;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::profiles::v1development as otlp;
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_profiles_wire::google_v1::Profile;
use pulsus_profiles_wire::querier_v1::{
    ProfileFormat, SelectMergeProfileRequest, SelectMergeStacktracesRequest,
    SelectMergeStacktracesResponse,
};
use serde_json::{Value, json};

const PORT: u16 = 31_828;
const MINUTE_MS: i64 = 60_000;
const DAY_MS: i64 = 86_400_000;
const SVC: &str = r#"{service_name="svc.cpu"}"#;

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------

struct Res {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Res {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self, ctx: &str) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "{ctx}: not JSON ({e}): {:?}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }

    fn query_id(&self, ctx: &str) -> String {
        self.header("x-pulsus-query-id")
            .unwrap_or_else(|| panic!("{ctx}: no X-Pulsus-Query-Id"))
            .to_string()
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(nl) = find(raw, b"\r\n") {
        let size = std::str::from_utf8(&raw[..nl])
            .ok()
            .and_then(|s| usize::from_str_radix(s.split(';').next().unwrap_or("").trim(), 16).ok());
        let Some(size) = size else { break };
        raw = &raw[nl + 2..];
        if size == 0 || raw.len() < size {
            break;
        }
        out.extend_from_slice(&raw[..size]);
        raw = raw[size..].strip_prefix(b"\r\n").unwrap_or(&raw[size..]);
    }
    out
}

/// One HTTP/1.1 exchange.
fn call(port: u16, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Res {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to pulsusdb");
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(body).expect("write body");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read response");
    let split = find(&buf, b"\r\n\r\n").expect("a header block");
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("a status line");
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let raw = &buf[split + 4..];
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked")
    });
    let body = if chunked { dechunk(raw) } else { raw.to_vec() };
    Res {
        status,
        headers,
        body,
    }
}

fn encode(v: &str) -> String {
    let mut out = String::new();
    for b in v.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(bytes).expect("gzip");
    gz.finish().expect("gzip")
}

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut out)
        .expect("a gzip body");
    out
}

// ---------------------------------------------------------------------
// The server
// ---------------------------------------------------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn assert_port_free(port: u16) {
    if let Err(e) = TcpListener::bind(("127.0.0.1", port)) {
        panic!(
            "port {port} is already in use ({e}): a previous run's server is probably \
             still listening. Find it with `ss -ltnp`; do not kill by program name."
        );
    }
}

fn spawn_ready(port: u16, db: &str, clickhouse_http: u16, env: &[(&str, &str)]) -> ChildGuard {
    assert_port_free(port);
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", clickhouse_http.to_string())
        .env("CLICKHOUSE_DB", db);
    for (k, v) in env {
        command.env(k, v);
    }
    let mut guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = guard.0.try_wait() {
            panic!("pulsusdb exited before becoming ready ({status}) on port {port}");
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && call(port, "GET", "/ready", &[], &[]).status == 200
        {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port})");
}

// ---------------------------------------------------------------------
// Statement counting
// ---------------------------------------------------------------------

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(
            sql,
            &QuerySettings::new().set("insert_deduplicate", 0u64),
            Idempotency::NonIdempotent,
        )
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// The finished statements `system.query_log` holds for each request id.
async fn statements(client: &ChClient, ids: &[String]) -> BTreeMap<String, u64> {
    exec(client, "SYSTEM FLUSH LOGS").await;
    let list: Vec<String> = ids.iter().map(|id| sql_string(id)).collect();
    let sql = format!(
        "SELECT concat(log_comment, ' ', toString(count())) AS s FROM system.query_log \
         WHERE type != 'QueryStart' AND is_initial_query AND log_comment IN ({}) \
         GROUP BY log_comment",
        list.join(", ")
    );
    let rows = client
        .query_strings(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let mut out: BTreeMap<String, u64> = ids.iter().map(|id| (id.clone(), 0)).collect();
    for row in rows {
        let (id, n) = row.split_once(' ').expect("id and count");
        out.insert(id.to_string(), n.parse().expect("a count"));
    }
    out
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

// ---------------------------------------------------------------------
// The OTLP push (EP8)
// ---------------------------------------------------------------------

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(AnyValueKind::StringValue(value.to_owned())),
        }),
        ..KeyValue::default()
    }
}

/// `otlp-svc`'s profile: mapping 1 (`app`) with two symbolised locations,
/// mapping 2 (`libx.so`) with one address-only location.
fn otlp_request(time_ns: u64) -> ExportProfilesServiceRequest {
    let st = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    let line = |function_index: i32, line: i64| otlp::Line {
        function_index,
        line,
        ..otlp::Line::default()
    };
    ExportProfilesServiceRequest {
        dictionary: Some(otlp::ProfilesDictionary {
            string_table: st(&[
                "",
                "samples",
                "count",
                "cpu",
                "nanoseconds",
                "main",
                "a",
                "main.go",
                "app",
                "libx.so",
            ]),
            mapping_table: vec![
                otlp::Mapping::default(),
                otlp::Mapping {
                    memory_start: 0x40_0000,
                    memory_limit: 0x80_0000,
                    filename_strindex: 8,
                    ..otlp::Mapping::default()
                },
                otlp::Mapping {
                    memory_start: nf::LIBX_START,
                    memory_limit: nf::LIBX_LIMIT,
                    filename_strindex: 9,
                    ..otlp::Mapping::default()
                },
            ],
            function_table: vec![
                otlp::Function::default(),
                otlp::Function {
                    name_strindex: 5,
                    filename_strindex: 7,
                    start_line: 1,
                    ..otlp::Function::default()
                },
                otlp::Function {
                    name_strindex: 6,
                    filename_strindex: 7,
                    start_line: 2,
                    ..otlp::Function::default()
                },
            ],
            location_table: vec![
                otlp::Location::default(),
                otlp::Location {
                    mapping_index: 1,
                    address: 0x40_1000,
                    lines: vec![line(1, 10)],
                    ..otlp::Location::default()
                },
                otlp::Location {
                    mapping_index: 1,
                    address: 0x40_2000,
                    lines: vec![line(2, 20)],
                    ..otlp::Location::default()
                },
                otlp::Location {
                    mapping_index: 2,
                    address: nf::L9_ADDRESS,
                    ..otlp::Location::default()
                },
            ],
            stack_table: vec![
                otlp::Stack::default(),
                otlp::Stack {
                    location_indices: vec![2, 1],
                },
                otlp::Stack {
                    location_indices: vec![3],
                },
            ],
            link_table: vec![otlp::Link::default()],
            attribute_table: vec![otlp::KeyValueAndUnit::default()],
        }),
        resource_profiles: vec![otlp::ResourceProfiles {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "otlp-svc")],
                ..Resource::default()
            }),
            scope_profiles: vec![otlp::ScopeProfiles {
                profiles: vec![otlp::Profile {
                    sample_type: Some(otlp::ValueType {
                        type_strindex: 1,
                        unit_strindex: 2,
                    }),
                    period_type: Some(otlp::ValueType {
                        type_strindex: 3,
                        unit_strindex: 4,
                    }),
                    period: PERIOD,
                    time_unix_nano: time_ns,
                    duration_nano: 10_000_000_000,
                    samples: vec![
                        otlp::Sample {
                            stack_index: 1,
                            values: vec![3],
                            ..otlp::Sample::default()
                        },
                        otlp::Sample {
                            stack_index: 2,
                            values: vec![1],
                            ..otlp::Sample::default()
                        },
                    ],
                    ..otlp::Profile::default()
                }],
                ..otlp::ScopeProfiles::default()
            }],
            ..otlp::ResourceProfiles::default()
        }],
    }
}

/// EP8's expected `NF`: `samples` count × the period, each sample `count`
/// periods of CPU time.
fn ep8_expected(time_nanos: i64) -> Nf {
    let app = ("app".to_string(), String::new(), 0, 0, 0, 0b0111);
    let libx = (
        "libx.so".to_string(),
        String::new(),
        nf::LIBX_START,
        nf::LIBX_LIMIT,
        0,
        0,
    );
    let f = |name: &str, start: i64, line: i64| {
        (
            (
                name.to_string(),
                String::new(),
                "main.go".to_string(),
                start,
            ),
            line,
        )
    };
    let loc = |mapping, address, lines| nf::NfLocation {
        mapping: Some(mapping),
        address,
        is_folded: false,
        lines,
    };
    let mut out = nf::header(PERIOD, time_nanos);
    out.samples.insert(
        (
            vec![
                loc(app.clone(), 0, vec![f("a", 2, 20)]),
                loc(app, 0, vec![f("main", 1, 10)]),
            ],
            vec![3 * PERIOD],
        ),
        1,
    );
    out.samples.insert(
        (vec![loc(libx, nf::L9_ADDRESS, Vec::new())], vec![PERIOD]),
        1,
    );
    out
}

// ---------------------------------------------------------------------
// EP1–EP8
// ---------------------------------------------------------------------

fn native(method: &str) -> String {
    format!("/api/profiles/v1/querier.v1.QuerierService/{method}")
}

fn proto_call(port: u16, method: &str, tenant: &str, body: &[u8]) -> Res {
    call(
        port,
        "POST",
        &native(method),
        &[
            ("Content-Type", "application/proto"),
            ("X-Scope-OrgID", tenant),
        ],
        body,
    )
}

/// A `200` `application/proto` answer decoded as a `Profile`.
fn profile_of(res: &Res, ctx: &str) -> Profile {
    assert_eq!(res.status, 200, "{ctx}: {}", res.text());
    assert_eq!(
        res.header("content-type"),
        Some("application/proto"),
        "{ctx}"
    );
    Profile::decode(&res.body[..]).unwrap_or_else(|e| panic!("{ctx}: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_merged_profile_answers_as_pprof_in_one_statement() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profileql_pprof_it")).await;
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect");
    let _server = spawn_ready(
        PORT,
        db.name(),
        live_db::ch_http_port(),
        &[("PULSUS_COMPAT_ENDPOINTS", "false")],
    );
    let now = chrono::Utc::now().timestamp_millis();
    let pushed_ns = (now - 5 * MINUTE_MS) * 1_000_000;
    for (tenant, scale) in [("t1", 1), ("t2", 100)] {
        let body = gzip(&nf::profile(scale, PERIOD, pushed_ns).encode_to_vec());
        let res = call(
            PORT,
            "POST",
            "/api/profiles/v1/ingest?name=svc.cpu%7B%7D&format=pprof",
            &[("X-Scope-OrgID", tenant)],
            &body,
        );
        assert_eq!(res.status, 200, "push under {tenant}: {}", res.text());
    }
    let res = call(
        PORT,
        "POST",
        "/v1development/profiles",
        &[
            ("Content-Type", "application/x-protobuf"),
            ("X-Scope-OrgID", "t1"),
        ],
        &otlp_request(pushed_ns as u64).encode_to_vec(),
    );
    assert_eq!(res.status, 200, "OTLP push: {}", res.text());

    let (start, end) = (now - 30 * MINUTE_MS, now + 30 * MINUTE_MS);
    let end_ns = end * 1_000_000;
    let want = |samples: &[(&str, i64)], unit: i64| nf::expected(samples, unit, PERIOD, end_ns);
    let request = |selector: &str| SelectMergeProfileRequest {
        profile_type_id: CPU.to_string(),
        label_selector: selector.to_string(),
        start,
        end,
        ..SelectMergeProfileRequest::default()
    };
    let mut counted: Vec<(&str, String, u64)> = Vec::new();

    // EP1: the native path, proto, no `max_nodes`: every stack, mapping
    // flags as pushed, one statement.
    let res = proto_call(
        PORT,
        "SelectMergeProfile",
        "t1",
        &request(SVC).encode_to_vec(),
    );
    let ep1 = nf::nf(&profile_of(&res, "EP1"));
    assert_eq!(ep1, want(&P1, UNIT), "EP1");
    counted.push(("EP1", res.query_id("EP1"), 1));

    // EP2: `GET /export`, the gzip of the same message; the compat path is
    // not mounted.
    let query = encode(&format!("{CPU}{SVC}"));
    let res = call(
        PORT,
        "GET",
        &format!("/api/profiles/v1/export?query={query}&from={start}&until={end}"),
        &[("X-Scope-OrgID", "t1")],
        &[],
    );
    assert_eq!(res.status, 200, "EP2: {}", res.text());
    assert_eq!(
        res.header("content-type"),
        Some("application/octet-stream"),
        "EP2"
    );
    let exported = Profile::decode(&gunzip(&res.body)[..]).expect("EP2: a profile");
    assert_eq!(nf::nf(&exported), ep1, "EP2");
    counted.push(("EP2", res.query_id("EP2"), 1));
    let res = call(
        PORT,
        "POST",
        "/querier.v1.QuerierService/SelectMergeProfile",
        &[
            ("Content-Type", "application/proto"),
            ("X-Scope-OrgID", "t1"),
        ],
        &request(SVC).encode_to_vec(),
    );
    assert_eq!(res.status, 404, "EP2 compat: {}", res.text());

    // EP3: a sixth `delta` part selects the first five; four parts refuse.
    let delta = SelectMergeProfileRequest {
        profile_type_id: format!("{CPU}:delta"),
        ..request(SVC)
    };
    let res = proto_call(PORT, "SelectMergeProfile", "t1", &delta.encode_to_vec());
    assert_eq!(nf::nf(&profile_of(&res, "EP3 delta")), ep1, "EP3 delta");
    counted.push(("EP3 delta", res.query_id("EP3 delta"), 1));
    let four = SelectMergeProfileRequest {
        profile_type_id: "a:b:c:d".to_string(),
        ..request(SVC)
    };
    let res = proto_call(PORT, "SelectMergeProfile", "t1", &four.encode_to_vec());
    assert_eq!(res.status, 400, "EP3 four parts: {}", res.text());
    assert_eq!(
        res.json("EP3 four parts"),
        json!({
            "code": "invalid_argument",
            "message": "profile-type selection must be of the form \
                <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta), \
                got(4): \"a:b:c:d\""
        }),
        "EP3 four parts"
    );
    counted.push(("EP3 four parts", res.query_id("EP3 four parts"), 0));

    // EP4: the exploration app's function-details request, JSON.
    let body = json!({
        "profile_typeID": CPU,
        "label_selector": SVC,
        "start": start,
        "end": end,
        "stackTraceSelector": {"call_site": [{"name": "main"}, {"name": "a"}]},
        "maxNodes": 500,
    })
    .to_string();
    let res = call(
        PORT,
        "POST",
        &native("SelectMergeProfile"),
        &[
            ("Content-Type", "application/json"),
            ("X-Scope-OrgID", "t1"),
        ],
        body.as_bytes(),
    );
    assert_eq!(res.status, 200, "EP4: {}", res.text());
    assert_eq!(res.header("content-type"), Some("application/json"), "EP4");
    let profile: Profile = serde_json::from_slice(&res.body).expect("EP4: a JSON profile");
    assert_eq!(nf::nf(&profile), want(&P4, UNIT), "EP4");
    counted.push(("EP4", res.query_id("EP4"), 1));

    // EP5: `SelectMergeStacktraces` with `PROFILE_FORMAT_PPROF`.
    let smst = SelectMergeStacktracesRequest {
        profile_type_id: CPU.to_string(),
        label_selector: SVC.to_string(),
        start,
        end,
        max_nodes: Some(3),
        format: ProfileFormat::Pprof as i32,
        ..SelectMergeStacktracesRequest::default()
    };
    let res = proto_call(PORT, "SelectMergeStacktraces", "t1", &smst.encode_to_vec());
    assert_eq!(res.status, 200, "EP5: {}", res.text());
    let answer = SelectMergeStacktracesResponse::decode(&res.body[..]).expect("EP5: decode");
    assert!(answer.flamegraph.is_none(), "EP5: a flame graph");
    let profile = answer
        .pprof
        .and_then(|p| p.profile)
        .expect("EP5: pprof.profile");
    assert_eq!(nf::nf(&profile), want(&P2, UNIT), "EP5");
    counted.push(("EP5", res.query_id("EP5"), 1));

    // EP6: the tenant's own samples.
    let res = proto_call(
        PORT,
        "SelectMergeProfile",
        "t2",
        &request(SVC).encode_to_vec(),
    );
    assert_eq!(
        nf::nf(&profile_of(&res, "EP6")),
        want(&P1, 100 * UNIT),
        "EP6"
    );
    counted.push(("EP6", res.query_id("EP6"), 1));

    // EP7: a window ending before the lookback floor is the empty message
    // with no statement; a selector matching nothing is the reference's six
    // bytes, after one statement.
    let old = SelectMergeProfileRequest {
        start: now - 9 * DAY_MS,
        end: now - 8 * DAY_MS,
        ..request(SVC)
    };
    let res = proto_call(PORT, "SelectMergeProfile", "t1", &old.encode_to_vec());
    assert_eq!(res.status, 200, "EP7 old: {}", res.text());
    assert!(res.body.is_empty(), "EP7 old: {:?}", res.body);
    counted.push(("EP7 old", res.query_id("EP7 old"), 0));
    let none = request(r#"{service_name="nothing"}"#);
    let res = proto_call(PORT, "SelectMergeProfile", "t1", &none.encode_to_vec());
    assert_eq!(res.status, 200, "EP7 none: {}", res.text());
    assert_eq!(res.body, [0x0a, 0x00, 0x32, 0x00, 0x5a, 0x00], "EP7 none");
    counted.push(("EP7 none", res.query_id("EP7 none"), 1));

    // EP8: an OTLP push's mapping flags and the bounds and addresses they
    // clear or keep.
    let res = proto_call(
        PORT,
        "SelectMergeProfile",
        "t1",
        &request(r#"{service_name="otlp-svc"}"#).encode_to_vec(),
    );
    assert_eq!(
        nf::nf(&profile_of(&res, "EP8")),
        ep8_expected(end_ns),
        "EP8"
    );
    counted.push(("EP8", res.query_id("EP8"), 1));

    let ids: Vec<String> = counted.iter().map(|(_, id, _)| id.clone()).collect();
    let sent = statements(&client, &ids).await;
    for (ctx, id, want) in &counted {
        assert_eq!(sent[id], *want, "{ctx}: statements for {id}");
    }
}
