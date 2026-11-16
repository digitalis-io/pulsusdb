//! L1: `POST /v1development/profiles` through the `pulsusdb` binary against
//! a live ClickHouse — what one OTLP profiles push stores, and that it sends
//! one insert and no read.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test otlp_profiles_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::profiles::v1development::{
    Function, KeyValueAndUnit, Line, Link, Location, Mapping, Profile, ProfilesDictionary,
    ResourceProfiles, Sample, ScopeProfiles, Stack, ValueType,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};

const PORT: u16 = 31_815;

/// The `log_comment` the test's own clock reads carry, so the statement
/// count leaves them out.
const PROBE: &str = "l5-probe";

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn assert_port_free(port: u16) {
    if let Err(e) = std::net::TcpListener::bind(("127.0.0.1", port)) {
        panic!(
            "port {port} is already in use ({e}): a previous run's server is probably \
             still listening. Find it with `ss -ltnp`; do not kill by program name."
        );
    }
}

/// `(status, body)` of one HTTP/1.1 request.
fn request(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("timeout");
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(body).expect("write body");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read");
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a header end");
    let status = String::from_utf8_lossy(&buf[..split])
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("a status");
    (status, buf[split + 4..].to_vec())
}

/// The binary in `all` mode over `db`, one push per landing insert.
fn spawn_ready(port: u16, db: &ScopedDb) -> ChildGuard {
    assert_port_free(port);
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("PULSUS_MODE", "all")
        .env("PULSUS_LANDING_BATCH_MS", "0")
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db.name());
    let mut guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    live_db::register_push_target(port, db.name());
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = guard.0.try_wait() {
            panic!("pulsusdb exited before becoming ready ({status}) on port {port}");
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && request(port, "GET", "/ready", &[], b"").0 == 200
        {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port})");
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as u64
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_owned())),
        }),
        ..KeyValue::default()
    }
}

fn line(function_index: i32) -> Location {
    Location {
        lines: vec![Line {
            function_index,
            ..Line::default()
        }],
        ..Location::default()
    }
}

/// `R`: the design's example request, timed now.
fn r() -> ExportProfilesServiceRequest {
    ExportProfilesServiceRequest {
        dictionary: Some(ProfilesDictionary {
            string_table: [
                "",
                "samples",
                "count",
                "cpu",
                "nanoseconds",
                "main.work",
                "runtime.mallocgc",
            ]
            .iter()
            .map(|s| (*s).to_owned())
            .collect(),
            mapping_table: vec![Mapping::default()],
            function_table: vec![
                Function::default(),
                Function {
                    name_strindex: 5,
                    ..Function::default()
                },
                Function {
                    name_strindex: 6,
                    ..Function::default()
                },
            ],
            location_table: vec![Location::default(), line(1), line(2)],
            stack_table: vec![
                Stack::default(),
                Stack {
                    location_indices: vec![1],
                },
                Stack {
                    location_indices: vec![2, 1],
                },
            ],
            link_table: vec![
                Link::default(),
                Link {
                    trace_id: vec![0xaa; 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                },
            ],
            attribute_table: vec![KeyValueAndUnit::default()],
        }),
        resource_profiles: vec![ResourceProfiles {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout"),
                    string_kv("host.name", "h1"),
                ],
                ..Resource::default()
            }),
            scope_profiles: vec![ScopeProfiles {
                profiles: vec![Profile {
                    sample_type: Some(ValueType {
                        type_strindex: 1,
                        unit_strindex: 2,
                    }),
                    period_type: Some(ValueType {
                        type_strindex: 3,
                        unit_strindex: 4,
                    }),
                    period: 10_000_000,
                    time_unix_nano: now_ns(),
                    duration_nano: 10_000_000_000,
                    profile_id: vec![0x5a; 16],
                    samples: vec![
                        Sample {
                            stack_index: 1,
                            values: vec![3],
                            ..Sample::default()
                        },
                        Sample {
                            stack_index: 2,
                            values: vec![1],
                            link_index: 1,
                            ..Sample::default()
                        },
                    ],
                    ..Profile::default()
                }],
                ..ScopeProfiles::default()
            }],
            ..ResourceProfiles::default()
        }],
    }
}

async fn strings(client: &ChClient, sql: &str, settings: &QuerySettings) -> Vec<String> {
    client
        .query_strings(sql, settings)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
}

/// The server's clock, read by a statement the count leaves out.
async fn server_now(client: &ChClient) -> String {
    strings(
        client,
        "SELECT toString(now64(6)) AS s",
        &QuerySettings::new().set("log_comment", PROBE),
    )
    .await
    .remove(0)
}

/// **L1.** One OTLP push as tenant `t1` is answered `200`, stores the
/// example's rows under `t1`, and sends exactly one statement: the landing
/// insert.
#[tokio::test(flavor = "multi_thread")]
async fn an_otlp_profiles_push_is_stored_by_one_insert_and_no_read() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_otlp_profiles_it")).await;
    let _server = spawn_ready(PORT, &db);
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect to the live ClickHouse");

    let body = r().encode_to_vec();
    let t0 = server_now(&client).await;
    let (status, answer) = request(
        PORT,
        "POST",
        "/v1development/profiles",
        &[
            ("content-type", "application/x-protobuf"),
            ("x-scope-orgid", "t1"),
        ],
        &body,
    );
    let t1 = server_now(&client).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&answer));

    let none = QuerySettings::new();
    let series = strings(
        &client,
        "SELECT concat(org_id, ' ', type_id, ' ', service) AS s FROM profile_series",
        &none,
    )
    .await;
    assert_eq!(
        series,
        ["t1 process_cpu:cpu:nanoseconds:cpu:nanoseconds checkout"]
    );
    let totals = strings(
        &client,
        "SELECT toString(total) AS s FROM profile_entries WHERE org_id = 't1'",
        &none,
    )
    .await;
    assert_eq!(totals, ["40000000"]);
    let samples = strings(
        &client,
        "SELECT concat(toString(value), ' ', toString(span_id)) AS s FROM profile_samples \
         WHERE org_id = 't1' ORDER BY value",
        &none,
    )
    .await;
    assert_eq!(
        samples,
        [
            format!("10000000 {}", 0x0807_0605_0403_0201_u64),
            "30000000 0".to_owned()
        ]
    );

    client
        .execute("SYSTEM FLUSH LOGS", &none, Idempotency::Idempotent)
        .await
        .expect("flush logs");
    let statements = strings(
        &client,
        &format!(
            "SELECT concat(query_kind, '|', toString(has(tables, '{db}.profile_landing')), '|', query) AS s \
             FROM system.query_log \
             WHERE type != 'QueryStart' AND current_database = '{db}' \
               AND log_comment != '{PROBE}' \
               AND event_time_microseconds BETWEEN toDateTime64('{t0}', 6) AND toDateTime64('{t1}', 6)",
            db = db.name(),
        ),
        &none,
    )
    .await;
    assert_eq!(statements.len(), 1, "{statements:#?}");
    let mut parts = statements[0].splitn(3, '|');
    assert_eq!(parts.next(), Some("Insert"), "{statements:?}");
    assert_eq!(parts.next(), Some("1"), "{statements:?}");
    assert!(
        parts.next().is_some_and(|q| q.starts_with("INSERT INTO")),
        "{statements:?}"
    );
}
