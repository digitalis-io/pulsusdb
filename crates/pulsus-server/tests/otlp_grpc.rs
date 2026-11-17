//! Issue #816: OTLP over gRPC through the `pulsusdb` binary against a live
//! ClickHouse. Each signal's export, sent over gRPC, stores the same landing
//! rows the HTTP route stores for the same body: over h2c, over TLS `h2`,
//! gzip and identity. A gRPC push and an HTTP retry of it are one push, and
//! HTTP/1.1 is still served on the same port.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test otlp_grpc
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::collector::profiles::v1development::profiles_service_client::ProfilesServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::profiles::v1development::{
    Function, KeyValueAndUnit, Line, Link, Location, Mapping, Profile, ProfilesDictionary,
    ResourceProfiles, Sample, ScopeProfiles, Stack, ValueType,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use pulsus_clickhouse::{ChClient, QuerySettings};
use tonic::codec::CompressionEncoding;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

const PORT: u16 = 31_816;
const TLS_PORT: u16 = 31_818;

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
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("a status");
    (status, buf[split + 4..].to_vec())
}

/// The binary in `all` mode over `db`, one push per landing insert, with
/// `extra` environment on top.
fn spawn(port: u16, db: &ScopedDb, extra: &[(&str, &str)]) -> ChildGuard {
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
    for (k, v) in extra {
        command.env(k, v);
    }
    let guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    live_db::register_push_target(port, db.name());
    guard
}

/// Waits for `ready` to report `200`, or fails after 60 s.
fn wait_ready(guard: &mut ChildGuard, port: u16, ready: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = guard.0.try_wait() {
            panic!("pulsusdb exited before becoming ready ({status}) on port {port}");
        }
        if ready() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port})");
}

/// The plaintext server.
fn spawn_ready(port: u16, db: &ScopedDb) -> ChildGuard {
    let mut guard = spawn(port, db, &[]);
    wait_ready(&mut guard, port, || {
        TcpStream::connect(("127.0.0.1", port)).is_ok()
            && request(port, "GET", "/ready", &[], b"").0 == 200
    });
    guard
}

/// Writes `contents` to a uniquely named file under the OS temp directory.
fn write_temp_pem(name: &str, contents: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pulsus-otlp-grpc-{name}-{}-{:?}.pem",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::write(&path, contents).expect("write temp pem");
    path
}

/// The TLS server over `cert`, readiness read over TLS.
fn spawn_tls_ready(port: u16, db: &ScopedDb, cert: &rcgen::CertifiedKey) -> ChildGuard {
    let cert_path = write_temp_pem("cert", &cert.cert.pem());
    let key_path = write_temp_pem("key", &cert.key_pair.serialize_pem());
    let mut guard = spawn(
        port,
        db,
        &[
            ("PULSUS_TLS_CERT", cert_path.to_str().expect("utf-8 path")),
            ("PULSUS_TLS_KEY", key_path.to_str().expect("utf-8 path")),
        ],
    );
    let client = tls_client_config(cert.cert.der());
    wait_ready(&mut guard, port, || {
        tls_get_status(port, &client, "/ready") == Some(200)
    });
    let _ = std::fs::remove_file(cert_path);
    let _ = std::fs::remove_file(key_path);
    guard
}

fn tls_client_config(
    server_cert: &rustls_pki_types::CertificateDer<'static>,
) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(server_cert.clone()).expect("add the server cert");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("safe default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}

/// The status of one HTTP/1.1 `GET` over TLS; `None` before the server binds.
fn tls_get_status(port: u16, config: &Arc<rustls::ClientConfig>, path: &str) -> Option<u16> {
    let sock = TcpStream::connect(("127.0.0.1", port)).ok()?;
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let name = rustls_pki_types::ServerName::try_from("localhost").ok()?;
    let conn = rustls::ClientConnection::new(Arc::clone(config), name).ok()?;
    let mut tls = rustls::StreamOwned::new(conn, sock);
    write!(
        tls,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match tls.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    String::from_utf8_lossy(&buf)
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
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

/// 2,000 log records, `<prefix>-<i>`, timed `now - i` ms, under resource
/// `service.name=grpc`. About 50 KB, so one push crosses several HTTP/2
/// DATA frames.
fn logs(prefix: &str, now: u64) -> ExportLogsServiceRequest {
    let log_records = (0..2_000u64)
        .map(|i| LogRecord {
            time_unix_nano: now - i * 1_000_000,
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!("{prefix}-{i}"))),
            }),
            ..LogRecord::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "grpc")],
                ..Resource::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records,
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    }
}

fn gauge(name: &str) -> Metric {
    Metric {
        name: name.to_string(),
        description: String::new(),
        unit: String::new(),
        metadata: vec![],
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: 1_700_000_000_000_000_000,
                exemplars: vec![],
                flags: 0,
                value: Some(number_data_point::Value::AsDouble(1.0)),
            }],
        })),
    }
}

/// `api_conformance.rs`'s `valid_otlp_metrics_body` request, plus a second
/// gauge.
fn metrics() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![gauge("conformance_metric"), gauge("grpc_metric_b")],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// `api_conformance.rs`'s `valid_otlp_traces_body` request, with three
/// spans.
fn traces() -> ExportTraceServiceRequest {
    let span = |id: u8| Span {
        trace_id: vec![1; 16],
        span_id: vec![id; 8],
        name: "conformance-span".to_string(),
        start_time_unix_nano: 1_700_000_000_000_000_000,
        end_time_unix_nano: 1_700_000_001_000_000_000,
        ..Default::default()
    };
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![span(2), span(3), span(4)],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// `api_conformance.rs`'s `valid_otlp_profiles_request`, timed `now`, with a
/// fixed profile id so every push stores the same rows.
fn profiles(now: u64) -> ExportProfilesServiceRequest {
    let line = |function_index| Location {
        lines: vec![Line {
            function_index,
            ..Line::default()
        }],
        ..Location::default()
    };
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
            .map(String::from)
            .to_vec(),
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
                    time_unix_nano: now,
                    duration_nano: 10_000_000_000,
                    profile_id: vec![7; 16],
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

/// The four signals' requests, built once so every transport sends the
/// same bytes.
struct Bodies {
    logs: ExportLogsServiceRequest,
    metrics: ExportMetricsServiceRequest,
    traces: ExportTraceServiceRequest,
    profiles: ExportProfilesServiceRequest,
}

/// Each landing table, with the columns that vary per push.
const TABLES: [(&str, &str); 4] = [
    ("log_landing", "org_id, received_ms, event_id, updated_ns"),
    ("metric_landing", "org_id, received_ms, updated_ns"),
    ("trace_landing", "org_id, received_ms, event_id"),
    ("profile_landing", "org_id, received_ms"),
];

/// Every row `tenant` has in `table`, less the columns that vary per push,
/// one JSON object per row in a fixed order. One statement.
async fn rows(db: &ScopedDb, table: &str, except: &str, tenant: &str) -> String {
    let sql = format!(
        "SELECT * EXCEPT ({except}) FROM {db}.{table} WHERE org_id = '{tenant}' \
         ORDER BY ALL FORMAT JSONEachRow",
        db = db.name(),
    );
    let url = format!("http://{}:{}/", live_db::ch_host(), live_db::ch_http_port());
    let response = reqwest::Client::new()
        .post(url)
        .body(sql.clone())
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"));
    let status = response.status();
    let text = response.text().await.expect("a response body");
    assert!(status.is_success(), "{status}: {text}\n{sql}");
    text
}

/// `tenant`'s rows equal `control`'s in every landing table, and none is
/// empty.
async fn assert_same_rows(db: &ScopedDb, tenant: &str, control: &str) {
    for (table, except) in TABLES {
        let got = rows(db, table, except, tenant).await;
        let want = rows(db, table, except, control).await;
        assert!(!want.is_empty(), "{table}: {control} stored nothing");
        assert_eq!(got, want, "{table}: {tenant} against {control}");
    }
}

/// Each body over HTTP `POST /v1/...` under `tenant`; every answer `200`.
fn push_http(bodies: &Bodies, tenant: &str) {
    for (path, body) in [
        ("/v1/logs", bodies.logs.encode_to_vec()),
        ("/v1/metrics", bodies.metrics.encode_to_vec()),
        ("/v1/traces", bodies.traces.encode_to_vec()),
        ("/v1development/profiles", bodies.profiles.encode_to_vec()),
    ] {
        let (status, answer) = request(
            PORT,
            "POST",
            path,
            &[
                ("content-type", "application/x-protobuf"),
                ("x-scope-orgid", tenant),
            ],
            &body,
        );
        assert_eq!(
            status,
            200,
            "{path} {tenant}: {}",
            String::from_utf8_lossy(&answer)
        );
    }
}

fn with_tenant<T>(message: T, tenant: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("x-scope-orgid", tenant.parse().expect("ASCII metadata"));
    request
}

/// Each body over gRPC on `channel` under `tenant`; every call `OK` with no
/// partial success.
async fn push_grpc(channel: Channel, bodies: &Bodies, tenant: &str, gzip: bool) {
    let mut logs = LogsServiceClient::new(channel.clone());
    let mut metrics = MetricsServiceClient::new(channel.clone());
    let mut traces = TraceServiceClient::new(channel.clone());
    let mut profiles = ProfilesServiceClient::new(channel);
    if gzip {
        logs = logs.send_compressed(CompressionEncoding::Gzip);
        metrics = metrics.send_compressed(CompressionEncoding::Gzip);
        traces = traces.send_compressed(CompressionEncoding::Gzip);
        profiles = profiles.send_compressed(CompressionEncoding::Gzip);
    }
    let answer = logs
        .export(with_tenant(bodies.logs.clone(), tenant))
        .await
        .unwrap_or_else(|s| panic!("logs {tenant}: {s:?}"));
    assert_eq!(answer.into_inner().partial_success, None, "logs {tenant}");
    let answer = metrics
        .export(with_tenant(bodies.metrics.clone(), tenant))
        .await
        .unwrap_or_else(|s| panic!("metrics {tenant}: {s:?}"));
    assert_eq!(
        answer.into_inner().partial_success,
        None,
        "metrics {tenant}"
    );
    let answer = traces
        .export(with_tenant(bodies.traces.clone(), tenant))
        .await
        .unwrap_or_else(|s| panic!("traces {tenant}: {s:?}"));
    assert_eq!(answer.into_inner().partial_success, None, "traces {tenant}");
    let answer = profiles
        .export(with_tenant(bodies.profiles.clone(), tenant))
        .await
        .unwrap_or_else(|s| panic!("profiles {tenant}: {s:?}"));
    assert_eq!(
        answer.into_inner().partial_success,
        None,
        "profiles {tenant}"
    );
}

async fn count(client: &ChClient, sql: &str) -> u64 {
    client
        .query_strings(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sql}"))
        .remove(0)
        .parse()
        .expect("a count")
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_exports_store_what_the_http_routes_store() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_otlp_grpc_it")).await;
    let _plain = spawn_ready(PORT, &db);
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("rcgen self-signed generation");
    let _tls = spawn_tls_ready(TLS_PORT, &db, &cert);
    let client = ChClient::new(live_db::conn_config(db.name()))
        .await
        .expect("connect to the live ClickHouse");

    let now = now_ns();
    let bodies = Bodies {
        logs: logs("line", now),
        metrics: metrics(),
        traces: traces(),
        profiles: profiles(now),
    };

    // 1 (control): HTTP under two tenants stores the same rows, which is
    // what proves the excluded columns are the ones that vary per push.
    push_http(&bodies, "t2");
    push_http(&bodies, "t5");
    assert_same_rows(&db, "t5", "t2").await;

    // 2: gRPC over h2c, gzip.
    let h2c = Endpoint::from_shared(format!("http://127.0.0.1:{PORT}"))
        .expect("an endpoint")
        .connect_lazy();
    push_grpc(h2c.clone(), &bodies, "t1", true).await;
    assert_same_rows(&db, "t1", "t2").await;
    let lines = count(
        &client,
        &format!(
            "SELECT toString(count()) AS s FROM {}.log_landing WHERE org_id = 't1' AND kind = 0",
            db.name()
        ),
    )
    .await;
    assert_eq!(lines, 2_000);

    // 2b: gRPC over TLS `h2`, gzip.
    let tls = Endpoint::from_shared(format!("https://127.0.0.1:{TLS_PORT}"))
        .expect("an endpoint")
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(cert.cert.pem()))
                .domain_name("localhost"),
        )
        .expect("a TLS config")
        .connect_lazy();
    push_grpc(tls, &bodies, "t6", true).await;
    assert_same_rows(&db, "t6", "t2").await;

    // 3: gRPC without compression.
    push_grpc(h2c.clone(), &bodies, "t4", false).await;
    assert_same_rows(&db, "t4", "t2").await;

    // 4: a gRPC push, then the same body over HTTP: one push.
    let dup = logs("dup", now);
    let mut logs_client = LogsServiceClient::new(h2c);
    logs_client
        .export(with_tenant(dup.clone(), "t3"))
        .await
        .unwrap_or_else(|s| panic!("dup logs: {s:?}"));
    let count_t3 = format!(
        "SELECT toString(count()) AS s FROM {}.log_landing WHERE org_id = 't3'",
        db.name()
    );
    let n1 = count(&client, &count_t3).await;
    assert!(n1 > 0, "the gRPC push stored nothing");
    let (status, answer) = request(
        PORT,
        "POST",
        "/v1/logs",
        &[
            ("content-type", "application/x-protobuf"),
            ("x-scope-orgid", "t3"),
        ],
        &dup.encode_to_vec(),
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&answer));
    assert_eq!(count(&client, &count_t3).await, n1);

    // 5: HTTP/1.1 on the same port, after the gRPC calls.
    assert_eq!(request(PORT, "GET", "/buildinfo", &[], b"").0, 200);
}
