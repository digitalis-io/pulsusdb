//! Issue #822: the profiles differential. One fixture is pushed through
//! every receiver to the `pulsusdb` binary and to the pinned reference build,
//! under two tenants, and every request the datasource and the exploration
//! app send is compared between the two, and against what the fixture says
//! the answer must be.
//!
//! A difference that is not a row of
//! `docs/benchmarks/profileql-differential-ledger.md` fails the run. A
//! ledger case pins both answers, and fails when the two agree.
//!
//! Gated behind `PULSUSDB_PROFILEQL_DIFF_URL` (the reference) and
//! `PULSUS_TEST_CLICKHOUSE=1`. The live test skips only when both are unset
//! outside a live CI job; either one set without the other fails it, and in
//! a live CI job either one missing fails it:
//!
//! ```text
//! PULSUSDB_PROFILEQL_DIFF_URL=http://localhost:<port> \
//!   PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-server --test profileql_differential
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::ScopedDb;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::profiles::v1development::profiles_service_client::ProfilesServiceClient;
use opentelemetry_proto::tonic::collector::profiles::v1development::{
    ExportProfilesServiceRequest, ExportProfilesServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueKind;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::profiles::v1development as otlp;
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use pulsus_profiles_wire::google_v1;
use pulsus_profiles_wire::push_v1::{PushRequest, PushResponse, RawProfileSeries, RawSample};
use pulsus_profiles_wire::querier_v1::{
    FlameGraph, ProfileTypesRequest, ProfileTypesResponse, SelectMergeStacktracesRequest,
    SelectMergeStacktracesResponse, SeriesRequest, SeriesResponse,
};
use pulsus_profiles_wire::types_v1::{
    LabelNamesRequest, LabelNamesResponse, LabelPair, LabelValuesRequest, LabelValuesResponse,
};
use serde_json::{Value, json};

/// Our server's fixed port.
const PORT: u16 = 31_826;

/// The reference build the harness is written against, as its build-info
/// route reports it.
const REFERENCE_VERSION: &str = "2.4.3";
const REFERENCE_REVISION: &str = "860ef8d70";

/// The three profile types the fixture stores.
const TYPE_A: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const TYPE_B: &str = "process_cpu:samples:count:cpu:nanoseconds";
const TYPE_M: &str = "memory:alloc_space:bytes:space:bytes";

const LEDGER: &str = "docs/benchmarks/profileql-differential-ledger.md";
const DEFECTS: &str = "docs/reference-defects-we-do-not-copy.md";
/// The phrase the defects document attributes a ledger row with.
const LEDGER_PHRASE: &str = "the profiles differential's ledger";

const HOUR_MS: i64 = 3_600_000;

/// Every row's `rx` value, sorted.
const RX: [&str; 11] = [
    "ingest-jfr",
    "ingest-multipart",
    "ingest-pprof",
    "only",
    "otlp-grpc",
    "otlp-json",
    "otlp-noservice",
    "otlp-proto",
    "push-json",
    "push-noservice",
    "push-proto",
];

/// The `service_name` values the fixture stores under either tenant.
const SERVICES: [&str; 11] = [
    "svc-ingest-jfr",
    "svc-ingest-multipart.cpu",
    "svc-ingest-pprof.cpu",
    "svc-only",
    "svc-otlp-grpc",
    "svc-otlp-json",
    "svc-otlp-proto",
    "svc-push-json",
    "svc-push-proto",
    "unknown_service",
    "unknown_service:fixture-exe",
];

// ---------------------------------------------------------------------
// The case table
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Proto,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tenancy {
    /// Run under `t1-<run>` and `t2-<run>`.
    Both,
    /// Run once, with no `X-Scope-OrgID`.
    Absent,
}

#[derive(Debug, Clone, Copy)]
enum Window {
    /// `now − 1 h` to `now + 1 h`, the datasource's own default.
    Default,
    /// A fixed window in milliseconds.
    Fixed(i64, i64),
}

#[derive(Debug, Clone, Copy)]
enum Method {
    ProfileTypes(Window),
    LabelNames(&'static [&'static str]),
    LabelValues(&'static str, &'static [&'static str]),
    Series {
        label_names: &'static [&'static str],
        matchers: &'static [&'static str],
    },
}

impl Method {
    fn name(&self) -> &'static str {
        match self {
            Method::ProfileTypes(_) => "ProfileTypes",
            Method::LabelNames(_) => "LabelNames",
            Method::LabelValues(..) => "LabelValues",
            Method::Series { .. } => "Series",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Request {
    method: Method,
    encoding: Encoding,
    /// `allow-utf8-labelnames=true` in `Accept`.
    utf8: bool,
    tenancy: Tenancy,
}

/// What the fixture says the decoded answer is, on both servers.
#[derive(Debug, Clone, Copy)]
enum Fixture {
    /// No value at all.
    Empty,
    /// Non-empty; under `t2` a type `t1` does not list, holding
    /// `alloc_space`.
    TypesT2HoldsAlloc,
    /// No name containing `.`.
    NoDottedName,
    /// Each of these is a member.
    Includes(&'static [&'static str]),
    /// Exactly these values, per tenant.
    Exactly {
        t1: &'static [&'static str],
        t2: &'static [&'static str],
    },
    NonEmpty,
    /// C13's `(service_name, __profile_type__)` sets.
    LandingSets,
    /// C14's four sets of row 5.
    PushProtoSets,
}

/// One pinned answer of a ledger case: its status, and its body when the
/// pin holds one.
#[derive(Debug, Clone, Copy)]
struct Pin {
    status: u16,
    body: Option<&'static [u8]>,
}

#[derive(Debug, Clone, Copy)]
enum Expect {
    Parity(Fixture),
    Ledger {
        row: &'static str,
        ours: Pin,
        theirs: Pin,
    },
}

#[derive(Debug, Clone, Copy)]
struct Case {
    id: &'static str,
    client: &'static str,
    request: Request,
    expect: Expect,
}

const fn proto(method: Method, utf8: bool) -> Request {
    Request {
        method,
        encoding: Encoding::Proto,
        utf8,
        tenancy: Tenancy::Both,
    }
}

const fn json_utf8(method: Method) -> Request {
    Request {
        method,
        encoding: Encoding::Json,
        utf8: true,
        tenancy: Tenancy::Both,
    }
}

static CASES: &[Case] = &[
    Case {
        id: "C0",
        client: "datasource with no tenant configured",
        request: Request {
            method: Method::ProfileTypes(Window::Default),
            encoding: Encoding::Proto,
            utf8: false,
            tenancy: Tenancy::Absent,
        },
        expect: Expect::Ledger {
            row: "tenancy-absent-header-is-the-empty-tenant",
            ours: Pin {
                status: 200,
                body: Some(b""),
            },
            theirs: Pin {
                status: 401,
                body: None,
            },
        },
    },
    Case {
        id: "C1",
        client: "datasource health check",
        request: proto(Method::ProfileTypes(Window::Fixed(1000, 4000)), false),
        expect: Expect::Parity(Fixture::Empty),
    },
    Case {
        id: "C2",
        client: "datasource type picker",
        request: proto(Method::ProfileTypes(Window::Default), false),
        expect: Expect::Parity(Fixture::TypesT2HoldsAlloc),
    },
    Case {
        id: "C3",
        client: "datasource editor",
        request: proto(Method::LabelNames(&["{}"]), false),
        expect: Expect::Parity(Fixture::NoDottedName),
    },
    Case {
        id: "C4",
        client: "datasource editor, capability on",
        request: proto(Method::LabelNames(&["{}"]), true),
        expect: Expect::Parity(Fixture::Includes(&["k8s.pod.name"])),
    },
    Case {
        id: "C5",
        client: "datasource editor, capability on",
        request: proto(
            Method::LabelNames(&[r#"{service_name="svc-otlp-proto"}"#]),
            true,
        ),
        expect: Expect::Parity(Fixture::Includes(&["k8s.pod.name", "rx", "env"])),
    },
    Case {
        id: "C6",
        client: "datasource editor",
        request: proto(Method::LabelValues("service_name", &["{}"]), false),
        expect: Expect::Parity(Fixture::Exactly {
            t1: &SERVICES,
            t2: &SERVICES,
        }),
    },
    Case {
        id: "C7",
        client: "datasource editor",
        request: proto(Method::LabelValues("env", &["{}"]), false),
        expect: Expect::Parity(Fixture::Exactly {
            t1: &["prod", "prod-eu", "t1-only"],
            t2: &["prod", "prod-eu", "t2-only"],
        }),
    },
    Case {
        id: "C8",
        client: "datasource editor",
        request: proto(
            Method::LabelValues("pod", &[r#"{service_name="svc-push-proto"}"#]),
            false,
        ),
        expect: Expect::Parity(Fixture::Exactly {
            t1: &["p0"],
            t2: &["p0"],
        }),
    },
    Case {
        id: "C9",
        client: "datasource editor",
        request: proto(Method::LabelValues("rx", &[r#"{env!="prod"}"#]), false),
        expect: Expect::Parity(Fixture::Exactly {
            t1: &["ingest-multipart", "ingest-pprof", "only", "push-json"],
            t2: &["ingest-multipart", "ingest-pprof", "only", "push-json"],
        }),
    },
    Case {
        id: "C10",
        client: "datasource editor",
        request: proto(Method::LabelValues("rx", &[r#"{env=~"prod"}"#]), false),
        expect: Expect::Parity(Fixture::Exactly {
            t1: &[
                "ingest-jfr",
                "otlp-grpc",
                "otlp-json",
                "otlp-noservice",
                "otlp-proto",
                "push-noservice",
                "push-proto",
            ],
            t2: &[
                "ingest-jfr",
                "otlp-grpc",
                "otlp-json",
                "otlp-noservice",
                "otlp-proto",
                "push-noservice",
                "push-proto",
            ],
        }),
    },
    Case {
        id: "C11",
        client: "exploration app",
        request: json_utf8(Method::LabelValues(
            "service_name",
            &[r#"{__profile_type__="process_cpu:cpu:nanoseconds:cpu:nanoseconds"}"#],
        )),
        expect: Expect::Parity(Fixture::NonEmpty),
    },
    Case {
        id: "C12",
        client: "exploration app",
        request: json_utf8(Method::LabelNames(&[
            r#"{__profile_type__="process_cpu:cpu:nanoseconds:cpu:nanoseconds", service_name="svc-otlp-json"}"#,
        ])),
        expect: Expect::Parity(Fixture::Includes(&["k8s.pod.name"])),
    },
    Case {
        id: "C13",
        client: "exploration app landing",
        request: json_utf8(Method::Series {
            label_names: &["service_name", "__profile_type__"],
            matchers: &[],
        }),
        expect: Expect::Parity(Fixture::LandingSets),
    },
    Case {
        id: "C14",
        client: "two matcher strings",
        request: json_utf8(Method::Series {
            label_names: &[],
            matchers: &[r#"{env="prod"}"#, r#"{rx="push-proto"}"#],
        }),
        expect: Expect::Parity(Fixture::PushProtoSets),
    },
];

// ---------------------------------------------------------------------
// H1: the ledger and the cases name each other
// ---------------------------------------------------------------------

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the repository root")
}

/// The ledger's rows: each heading's id and its **Case** line's text.
fn ledger_rows(text: &str) -> Vec<(String, Option<String>)> {
    let mut rows: Vec<(String, Option<String>)> = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("### `") {
            let id = rest.trim_end().trim_end_matches('`').to_string();
            rows.push((id, None));
        } else if let Some(rest) = line.strip_prefix("- **Case:**")
            && let Some(last) = rows.last_mut()
        {
            last.1 = Some(rest.trim().to_string());
        }
    }
    rows
}

/// The `C<n>` a **Case** line names first, when it names one.
fn named_case(case_line: &str) -> Option<&str> {
    let rest = case_line.strip_prefix('`')?;
    let id = &rest[..rest.find('`')?];
    let digits = id.strip_prefix('C')?;
    (!digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit())).then_some(id)
}

/// Every id the defects document attributes to the ledger: per paragraph,
/// lines joined on whitespace, the backticked ids before the phrase.
fn attributed_ids(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for paragraph in text.split("\n\n") {
        let joined = paragraph.split_whitespace().collect::<Vec<_>>().join(" ");
        let Some(at) = joined.find(LEDGER_PHRASE) else {
            continue;
        };
        let before = &joined[..at];
        for (i, part) in before.split('`').enumerate() {
            if i % 2 == 1 && !part.is_empty() {
                out.insert(part.to_string());
            }
        }
    }
    out
}

/// **H1.** Every ledger case names a ledger row; every row whose case is a
/// `C<n>` names a case pinned to that row; every id the defects document
/// attributes to the ledger is a row.
#[test]
fn the_ledger_and_the_cases_name_each_other() {
    let root = repo_root();
    let ledger_path = root.join(LEDGER);
    assert!(
        ledger_path.is_file(),
        "the ledger {LEDGER} is not there, and the cases and the defects document name its rows"
    );
    let ledger = std::fs::read_to_string(&ledger_path).expect("read the ledger");
    let defects = std::fs::read_to_string(root.join(DEFECTS)).expect("read the defects document");
    let rows = ledger_rows(&ledger);
    let headings: BTreeSet<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    let mut problems: Vec<String> = Vec::new();

    for case in CASES {
        if let Expect::Ledger { row, .. } = case.expect
            && !headings.contains(row)
        {
            problems.push(format!(
                "case {} names ledger row `{row}`, not a heading",
                case.id
            ));
        }
    }
    for (id, case_line) in &rows {
        let Some(case_line) = case_line else {
            problems.push(format!("ledger row `{id}` has no **Case** line"));
            continue;
        };
        let Some(case_id) = named_case(case_line) else {
            continue;
        };
        match CASES.iter().find(|c| c.id == case_id) {
            Some(Case {
                expect: Expect::Ledger { row, .. },
                ..
            }) if row == id => {}
            Some(_) => problems.push(format!(
                "ledger row `{id}` names case {case_id}, which is not pinned to that row"
            )),
            None => problems.push(format!(
                "ledger row `{id}` names case {case_id}, not a case"
            )),
        }
    }
    if !headings.contains(JFR_DELAY_ROW) {
        problems.push(format!(
            "VJ names ledger row `{JFR_DELAY_ROW}`, not a heading"
        ));
    }
    let attributed = attributed_ids(&defects);
    assert!(
        attributed.len() >= 7,
        "only {} ids attributed to the ledger in {DEFECTS}, at least 7 expected: {attributed:?}",
        attributed.len()
    );
    for id in &attributed {
        if !headings.contains(id.as_str()) {
            problems.push(format!(
                "{DEFECTS} attributes `{id}` to the ledger, which has no such row"
            ));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

// ---------------------------------------------------------------------
// H2: an unset reference URL fails the run
// ---------------------------------------------------------------------

/// **H2.** In a live CI job, the live test with the reference URL removed
/// exits non-zero naming the variable.
#[test]
fn an_unset_reference_url_fails_the_run() {
    let exe = std::env::current_exe().expect("current_exe");
    let output = Command::new(exe)
        .args(["profileql_matches_the_reference", "--exact", "--nocapture"])
        .env("GITHUB_JOB", "schema-it-traceql")
        .env("PULSUS_TEST_CLICKHOUSE", "1")
        .env_remove("PULSUSDB_PROFILEQL_DIFF_URL")
        .output()
        .expect("spawn the child test process");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "the live test passed with its reference URL unset in a live CI job\n--- stderr ---\n{stderr}"
    );
    assert!(
        stderr.contains("PULSUSDB_PROFILEQL_DIFF_URL is not set"),
        "the child did not name the variable\n--- stderr ---\n{stderr}"
    );
}

// ---------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------

/// A server: a name for the report, and where it listens.
#[derive(Debug, Clone)]
struct Server {
    name: &'static str,
    host: String,
    port: u16,
}

impl Server {
    fn base(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}

/// `http://host:port[/…]` as a [`Server`].
fn parse_url(name: &'static str, url: &str) -> Server {
    let rest = url
        .trim()
        .strip_prefix("http://")
        .unwrap_or_else(|| panic!("{url:?}: expected http://host:port"));
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = authority
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("{url:?}: expected a port"));
    Server {
        name,
        host: host.to_string(),
        port: port
            .parse()
            .unwrap_or_else(|e| panic!("{url:?}: port: {e}")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Res {
    status: u16,
    content_type: Option<String>,
    body: Vec<u8>,
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

/// One HTTP/1.1 exchange. A connection failure panics naming the request.
fn call(server: &Server, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Res {
    let ctx = format!("{} {method} {path}", server.name);
    let mut stream = TcpStream::connect((server.host.as_str(), server.port))
        .unwrap_or_else(|e| panic!("{ctx}: connect: {e}"));
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\n",
        server.host, server.port
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream
        .write_all(head.as_bytes())
        .unwrap_or_else(|e| panic!("{ctx}: write: {e}"));
    stream
        .write_all(body)
        .unwrap_or_else(|e| panic!("{ctx}: write: {e}"));
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .unwrap_or_else(|e| panic!("{ctx}: read: {e}"));
    let split = find(&buf, b"\r\n\r\n").unwrap_or_else(|| panic!("{ctx}: no header block"));
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{ctx}: no status line"));
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let header = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let raw = &buf[split + 4..];
    let chunked = header("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
    Res {
        status,
        content_type: header("content-type"),
        body: if chunked { dechunk(raw) } else { raw.to_vec() },
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn encode_query(v: &str) -> String {
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

fn base64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | (*b as u32) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(bytes).expect("gzip");
    gz.finish().expect("gzip")
}

// ---------------------------------------------------------------------
// Our server
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

fn spawn_ready(port: u16, db: &str) -> ChildGuard {
    assert_port_free(port);
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", live_db::ch_host())
        .env("CLICKHOUSE_HTTP_PORT", live_db::ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db)
        .env("PULSUS_COMPAT_ENDPOINTS", "true");
    let mut guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let server = Server {
        name: "ours",
        host: "127.0.0.1".to_string(),
        port,
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = guard.0.try_wait() {
            panic!("pulsusdb exited before becoming ready ({status}) on port {port}");
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && call(&server, "GET", "/ready", &[], &[]).status == 200
        {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port})");
}

// ---------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------

/// A string-table index, interning `v`.
fn intern(table: &mut Vec<String>, v: &str) -> i64 {
    if let Some(i) = table.iter().position(|x| x == v) {
        return i as i64;
    }
    table.push(v.to_string());
    (table.len() - 1) as i64
}

/// The three stacks, root first, each with its sample label.
/// A stack, root first, and its sample label.
type Stack = (
    &'static [&'static str],
    Option<(&'static str, &'static str)>,
);

const STACKS: [Stack; 3] = [
    (&["main", "a", "b"], None),
    (&["main", "a", "c"], Some(("thread", "t-a"))),
    (&["main"], Some(("pod", "p1"))),
];

/// The `count` of each stack.
const COUNTS: [i64; 3] = [3, 2, 1];

const CPU_PERIOD: i64 = 10_000_000;

/// Profile P, or row 10's memory profile under `t2`, timed `time_ns`, every
/// value times `scale`.
fn pprof(time_ns: i64, scale: i64, memory: bool) -> google_v1::Profile {
    let mut t = vec![String::new()];
    let types: &[(&str, &str)] = if memory {
        &[("alloc_space", "bytes")]
    } else {
        &[("samples", "count"), ("cpu", "nanoseconds")]
    };
    let sample_type = types
        .iter()
        .map(|(ty, unit)| google_v1::ValueType {
            r#type: intern(&mut t, ty),
            unit: intern(&mut t, unit),
        })
        .collect();
    let period_type = if memory {
        ("space", "bytes")
    } else {
        ("cpu", "nanoseconds")
    };
    let period_type = Some(google_v1::ValueType {
        r#type: intern(&mut t, period_type.0),
        unit: intern(&mut t, period_type.1),
    });
    let names = ["main", "a", "b", "c"];
    let function: Vec<google_v1::Function> = names
        .iter()
        .enumerate()
        .map(|(i, n)| google_v1::Function {
            id: i as u64 + 1,
            name: intern(&mut t, n),
            ..google_v1::Function::default()
        })
        .collect();
    let location: Vec<google_v1::Location> = (1..=names.len() as u64)
        .map(|id| google_v1::Location {
            id,
            line: vec![google_v1::Line {
                function_id: id,
                line: 0,
            }],
            ..google_v1::Location::default()
        })
        .collect();
    let id_of = |n: &str| names.iter().position(|x| *x == n).expect("a function") as u64 + 1;
    let sample = STACKS
        .iter()
        .zip(COUNTS)
        .map(|((stack, label), count)| {
            let value = if memory {
                vec![count * 1000 * scale]
            } else {
                vec![count * scale, count * CPU_PERIOD * scale]
            };
            let label = label
                .map(|(k, v)| google_v1::Label {
                    key: intern(&mut t, k),
                    str: intern(&mut t, v),
                    ..google_v1::Label::default()
                })
                .into_iter()
                .collect();
            google_v1::Sample {
                location_id: stack.iter().rev().map(|n| id_of(n)).collect(),
                value,
                label,
            }
        })
        .collect();
    google_v1::Profile {
        sample_type,
        sample,
        location,
        function,
        string_table: t,
        time_nanos: time_ns,
        duration_nanos: 10_000_000_000,
        period_type,
        period: if memory { 0 } else { CPU_PERIOD },
        ..google_v1::Profile::default()
    }
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(AnyValueKind::StringValue(value.to_owned())),
        }),
        ..KeyValue::default()
    }
}

/// Profile O: the three stacks, their counts times `scale`, timed
/// `time_ns`, under resource attributes `resource`.
fn otlp_profile(
    time_ns: i64,
    scale: i64,
    resource: &[(&str, &str)],
) -> ExportProfilesServiceRequest {
    let strings: Vec<String> = [
        "",
        "samples",
        "count",
        "cpu",
        "nanoseconds",
        "main",
        "a",
        "b",
        "c",
        "thread",
        "pod",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    let line = |function_index: i32| otlp::Location {
        lines: vec![otlp::Line {
            function_index,
            ..otlp::Line::default()
        }],
        ..otlp::Location::default()
    };
    let function = |name_strindex: i32| otlp::Function {
        name_strindex,
        ..otlp::Function::default()
    };
    let attribute = |key_strindex: i32, value: &str| otlp::KeyValueAndUnit {
        key_strindex,
        value: Some(AnyValue {
            value: Some(AnyValueKind::StringValue(value.to_owned())),
        }),
        ..otlp::KeyValueAndUnit::default()
    };
    let dictionary = otlp::ProfilesDictionary {
        string_table: strings,
        mapping_table: vec![otlp::Mapping::default()],
        // main 1, a 2, b 3, c 4.
        function_table: vec![
            otlp::Function::default(),
            function(5),
            function(6),
            function(7),
            function(8),
        ],
        location_table: vec![
            otlp::Location::default(),
            line(1),
            line(2),
            line(3),
            line(4),
        ],
        // Leaf first.
        stack_table: vec![
            otlp::Stack::default(),
            otlp::Stack {
                location_indices: vec![3, 2, 1],
            },
            otlp::Stack {
                location_indices: vec![4, 2, 1],
            },
            otlp::Stack {
                location_indices: vec![1],
            },
        ],
        link_table: vec![otlp::Link::default()],
        attribute_table: vec![
            otlp::KeyValueAndUnit::default(),
            attribute(9, "t-a"),
            attribute(10, "p1"),
        ],
    };
    let samples = vec![
        otlp::Sample {
            stack_index: 1,
            values: vec![COUNTS[0] * scale],
            ..otlp::Sample::default()
        },
        otlp::Sample {
            stack_index: 2,
            values: vec![COUNTS[1] * scale],
            attribute_indices: vec![1],
            ..otlp::Sample::default()
        },
        otlp::Sample {
            stack_index: 3,
            values: vec![COUNTS[2] * scale],
            attribute_indices: vec![2],
            ..otlp::Sample::default()
        },
    ];
    ExportProfilesServiceRequest {
        dictionary: Some(dictionary),
        resource_profiles: vec![otlp::ResourceProfiles {
            resource: Some(Resource {
                attributes: resource.iter().map(|(k, v)| string_kv(k, v)).collect(),
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
                    period: CPU_PERIOD,
                    time_unix_nano: time_ns as u64,
                    duration_nano: 10_000_000_000,
                    samples,
                    ..otlp::Profile::default()
                }],
                ..otlp::ScopeProfiles::default()
            }],
            ..otlp::ResourceProfiles::default()
        }],
    }
}

/// OTLP/JSON of `request`. The serializer writes the empty
/// `originalPayload` as an array, which OTLP/JSON spells as base64; a
/// default is omitted there, so the key is dropped.
fn otlp_json(request: &ExportProfilesServiceRequest) -> Vec<u8> {
    let mut v = serde_json::to_value(request).expect("OTLP/JSON");
    for rp in v["resourceProfiles"].as_array_mut().into_iter().flatten() {
        for sp in rp["scopeProfiles"].as_array_mut().into_iter().flatten() {
            for p in sp["profiles"].as_array_mut().into_iter().flatten() {
                if let Some(o) = p.as_object_mut() {
                    o.remove("originalPayload");
                }
            }
        }
    }
    v.to_string().into_bytes()
}

fn push_request(labels: &[(&str, &str)], raw_profile: Vec<u8>) -> PushRequest {
    PushRequest {
        series: vec![RawProfileSeries {
            labels: labels
                .iter()
                .map(|(n, v)| LabelPair {
                    name: n.to_string(),
                    value: v.to_string(),
                })
                .collect(),
            samples: vec![RawSample {
                raw_profile,
                id: String::new(),
            }],
            annotations: Vec::new(),
        }],
    }
}

/// The push request as proto3 JSON.
fn push_json(labels: &[(&str, &str)], raw_profile: &[u8]) -> Vec<u8> {
    let labels: Vec<Value> = labels
        .iter()
        .map(|(n, v)| json!({"name": n, "value": v}))
        .collect();
    json!({"series": [{"labels": labels, "samples": [{"rawProfile": base64(raw_profile)}]}]})
        .to_string()
        .into_bytes()
}

fn multipart(boundary: &str, profile: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"profile\"; \
             filename=\"profile.pprof\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    out.extend_from_slice(profile);
    out.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    out
}

/// Issue #819: a form whose `jfr` part is `recording`, as the common Java
/// agent sends it.
fn jfr_multipart(boundary: &str, recording: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"jfr\"; \
             filename=\"jfr\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    out.extend_from_slice(recording);
    out.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    out
}

/// One of the JFR library's files vendored for issue #819, as stored.
fn jfr_file(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/../pulsus-write/tests/fixtures/profileql_jfr/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// The sum of each value column of one of the library's expected
/// collapsed outputs.
fn jfr_column_sums(name: &str) -> Vec<i64> {
    let mut text = String::new();
    flate2::read::MultiGzDecoder::new(&jfr_file(name)[..])
        .read_to_string(&mut text)
        .expect("a gzip text file");
    let mut sums: Vec<i64> = Vec::new();
    for line in text.lines() {
        let (_, values) = line.rsplit_once(" [").expect("a collapsed line");
        for (i, v) in values.trim_end_matches(']').split(' ').enumerate() {
            let v: i64 = v.parse().expect("a value");
            if sums.len() <= i {
                sums.push(0);
            }
            sums[i] += v;
        }
    }
    sums
}

/// The ledger row the JFR lock delay's pins name (`VJ`).
const JFR_DELAY_ROW: &str = "ingest-jfr-delay-in-nanoseconds";

/// The type id the JFR lock delay is stored under.
const JFR_DELAY_TYPE: &str = "mutex:delay:nanoseconds:mutex:count";

/// `example.jfr`'s ticks per second, from its chunk header.
const JFR_TICKS_PER_SECOND: i128 = 2_303_974_730;

/// `VJ`'s parity rows: each type `example.jfr` stores but the lock delay,
/// and its total from the library's own expected outputs.
fn jfr_totals() -> Vec<(&'static str, i64)> {
    let cpu = jfr_column_sums("example.cpu.txt.gz");
    let tlab = jfr_column_sums("example.alloc_in.txt.gz");
    let outside = jfr_column_sums("example.alloc_out.txt.gz");
    let mutex = jfr_column_sums("example.mutex.txt.gz");
    vec![
        (TYPE_A, cpu[0]),
        (
            "memory:alloc_in_new_tlab_objects:count:space:bytes",
            tlab[0],
        ),
        ("memory:alloc_in_new_tlab_bytes:bytes:space:bytes", tlab[1]),
        (
            "memory:alloc_outside_tlab_objects:count:space:bytes",
            outside[0],
        ),
        (
            "memory:alloc_outside_tlab_bytes:bytes:space:bytes",
            outside[1],
        ),
        ("mutex:contentions:count:mutex:count", mutex[0]),
        (JFR_DELAY_TYPE, mutex[1]),
    ]
}

/// `VJ`'s ledger pins for the lock delay (defects entry 48): the reference
/// stores the library's tick total, and ours that total in nanoseconds, each
/// of its events truncated, so within one nanosecond per event below it.
fn jfr_delay_pins() -> (i64, std::ops::RangeInclusive<i64>) {
    let mutex = jfr_column_sums("example.mutex.txt.gz");
    let (count, ticks) = (mutex[0], mutex[1]);
    let whole = (i128::from(ticks) * 1_000_000_000 / JFR_TICKS_PER_SECOND) as i64;
    (ticks, whole - count + 1..=whole)
}

/// How a push reaches a server.
enum Transport {
    /// An HTTP request: path, headers beyond the tenant and the key, body.
    Http {
        path: String,
        headers: Vec<(&'static str, String)>,
        body: Vec<u8>,
        answer: PushAnswer,
    },
    /// OTLP over gRPC, h2c.
    Grpc(ExportProfilesServiceRequest),
}

/// How a push's HTTP answer is compared (§5).
#[derive(Debug, Clone, Copy)]
enum PushAnswer {
    Otlp,
    Connect,
    Ingest,
}

struct Push {
    rx: &'static str,
    transport: Transport,
}

/// The tenants' fixture (§3): `scale` 1 under `t1`, 100 under `t2`.
fn pushes(t0_s: i64, run: &str, t1: bool) -> Vec<Push> {
    let scale = if t1 { 1 } else { 100 };
    let at = |i: i64| (t0_s + i) * 1_000_000_000;
    let otlp_version = ("OTLP-Profiles-Development-Version", "1".to_string());
    let otlp_resource = |rx: &'static str, service: Option<&'static str>| {
        let mut r: Vec<(&str, &str)> = Vec::new();
        match service {
            Some(s) => r.push(("service.name", s)),
            None => r.push(("process.executable.name", "fixture-exe")),
        }
        r.push(("rx", rx));
        r.push(("env", "prod"));
        if service.is_some() {
            r.push(("k8s.pod.name", "p0"));
        }
        r
    };
    let p = |i: i64| gzip(&pprof(at(i), scale, false).encode_to_vec());
    let mut out = vec![
        Push {
            rx: "otlp-proto",
            transport: Transport::Http {
                path: "/v1development/profiles".to_string(),
                headers: vec![
                    ("Content-Type", "application/x-protobuf".to_string()),
                    otlp_version.clone(),
                ],
                body: otlp_profile(
                    at(1),
                    scale,
                    &otlp_resource("otlp-proto", Some("svc-otlp-proto")),
                )
                .encode_to_vec(),
                answer: PushAnswer::Otlp,
            },
        },
        Push {
            rx: "otlp-json",
            transport: Transport::Http {
                path: "/v1development/profiles".to_string(),
                headers: vec![
                    ("Content-Type", "application/json".to_string()),
                    otlp_version.clone(),
                ],
                body: otlp_json(&otlp_profile(
                    at(2),
                    scale,
                    &otlp_resource("otlp-json", Some("svc-otlp-json")),
                )),
                answer: PushAnswer::Otlp,
            },
        },
        Push {
            rx: "otlp-grpc",
            transport: Transport::Grpc(otlp_profile(
                at(3),
                scale,
                &otlp_resource("otlp-grpc", Some("svc-otlp-grpc")),
            )),
        },
        Push {
            rx: "otlp-noservice",
            transport: Transport::Http {
                path: "/v1development/profiles".to_string(),
                headers: vec![
                    ("Content-Type", "application/x-protobuf".to_string()),
                    otlp_version.clone(),
                ],
                body: otlp_profile(at(4), scale, &otlp_resource("otlp-noservice", None))
                    .encode_to_vec(),
                answer: PushAnswer::Otlp,
            },
        },
        Push {
            rx: "push-proto",
            transport: Transport::Http {
                path: "/push.v1.PusherService/Push".to_string(),
                headers: vec![("Content-Type", "application/proto".to_string())],
                body: push_request(
                    &[
                        ("__name__", "process_cpu"),
                        ("service_name", "svc-push-proto"),
                        ("rx", "push-proto"),
                        ("env", "prod"),
                        ("pod", "p0"),
                        ("k8s.pod.name", "p0"),
                    ],
                    p(5),
                )
                .encode_to_vec(),
                answer: PushAnswer::Connect,
            },
        },
        Push {
            rx: "push-json",
            transport: Transport::Http {
                path: "/push.v1.PusherService/Push".to_string(),
                headers: vec![("Content-Type", "application/json".to_string())],
                body: push_json(
                    &[
                        ("__name__", "process_cpu"),
                        ("service_name", "svc-push-json"),
                        ("rx", "push-json"),
                        ("env", "prod-eu"),
                        ("pod", "p0"),
                    ],
                    &p(6),
                ),
                answer: PushAnswer::Connect,
            },
        },
        Push {
            rx: "push-noservice",
            transport: Transport::Http {
                path: "/push.v1.PusherService/Push".to_string(),
                headers: vec![("Content-Type", "application/proto".to_string())],
                body: push_request(
                    &[
                        ("__name__", "process_cpu"),
                        ("rx", "push-noservice"),
                        ("env", "prod"),
                    ],
                    p(7),
                )
                .encode_to_vec(),
                answer: PushAnswer::Connect,
            },
        },
        Push {
            rx: "ingest-pprof",
            transport: Transport::Http {
                path: format!(
                    "/ingest?name={}&format=pprof&from={}&until={}",
                    encode_query("svc-ingest-pprof.cpu{rx=ingest-pprof}"),
                    t0_s + 8,
                    t0_s + 18
                ),
                headers: Vec::new(),
                body: p(8),
                answer: PushAnswer::Ingest,
            },
        },
        Push {
            rx: "ingest-multipart",
            transport: Transport::Http {
                path: format!(
                    "/ingest?name={}&spyName=gospy&sampleRate=100&from={}&until={}",
                    encode_query("svc-ingest-multipart.cpu{rx=ingest-multipart}"),
                    t0_s + 9,
                    t0_s + 19
                ),
                headers: vec![(
                    "Content-Type",
                    format!("multipart/form-data; boundary=b822-{run}"),
                )],
                body: multipart(&format!("b822-{run}"), &p(9)),
                answer: PushAnswer::Ingest,
            },
        },
    ];
    out.push(Push {
        rx: "ingest-jfr",
        transport: Transport::Http {
            path: format!(
                "/ingest?name={}&format=jfr&spyName=javaspy&sampleRate=100&from={}&until={}",
                encode_query("svc-ingest-jfr{rx=ingest-jfr,env=prod}"),
                t0_s + 11,
                t0_s + 21
            ),
            headers: vec![(
                "Content-Type",
                format!("multipart/form-data; boundary=b819-{run}"),
            )],
            body: jfr_multipart(&format!("b819-{run}"), &jfr_file("example.jfr.gz")),
            answer: PushAnswer::Ingest,
        },
    });
    let (only_labels, only_profile) = if t1 {
        (
            [
                ("__name__", "process_cpu"),
                ("service_name", "svc-only"),
                ("rx", "only"),
                ("env", "t1-only"),
            ],
            p(10),
        )
    } else {
        (
            [
                ("__name__", "memory"),
                ("service_name", "svc-only"),
                ("rx", "only"),
                ("env", "t2-only"),
            ],
            // Already times 100: 3000, 2000, 1000.
            gzip(&pprof(at(10), 1, true).encode_to_vec()),
        )
    };
    out.push(Push {
        rx: "only",
        transport: Transport::Http {
            path: "/push.v1.PusherService/Push".to_string(),
            headers: vec![("Content-Type", "application/proto".to_string())],
            body: push_request(&only_labels, only_profile).encode_to_vec(),
            answer: PushAnswer::Connect,
        },
    });
    out
}

// ---------------------------------------------------------------------
// Answers, decoded and described
// ---------------------------------------------------------------------

/// A body's text, decoded as `method`'s answer.
fn decoded(method: &str, encoding: Encoding, body: &[u8]) -> String {
    if encoding == Encoding::Json || body.first() == Some(&b'{') {
        return String::from_utf8_lossy(body).into_owned();
    }
    let text = match method {
        "ProfileTypes" => ProfileTypesResponse::decode(body).map(|r| {
            r.profile_types
                .iter()
                .map(|t| t.id.clone())
                .collect::<Vec<_>>()
                .join(", ")
        }),
        "LabelNames" => LabelNamesResponse::decode(body).map(|r| r.names.join(", ")),
        "LabelValues" => LabelValuesResponse::decode(body).map(|r| r.names.join(", ")),
        "SelectMergeStacktraces" => SelectMergeStacktracesResponse::decode(body)
            .map(|r| format!("{:?}", r.flamegraph.as_ref().map(nodes))),
        "Push" => PushResponse::decode(body).map(|r| format!("{r:?}")),
        "Export" => ExportProfilesServiceResponse::decode(body).map(|r| format!("{r:?}")),
        _ => return String::from_utf8_lossy(body).into_owned(),
    };
    text.unwrap_or_else(|e| format!("<not {method}: {e}> {}", String::from_utf8_lossy(body)))
}

fn describe(res: &Res, method: &str, encoding: Encoding) -> String {
    format!(
        "status {}, Content-Type {:?}, body {} bytes\n      hex {}\n      decoded {}",
        res.status,
        res.content_type,
        res.body.len(),
        hex(&res.body),
        decoded(method, encoding, &res.body)
    )
}

/// The decoded values of a catalogue answer: type ids, names or values.
fn values(method: &Method, encoding: Encoding, body: &[u8]) -> Result<Vec<String>, String> {
    let e = |e: &dyn std::fmt::Display| e.to_string();
    match (method, encoding) {
        (Method::ProfileTypes(_), Encoding::Proto) => ProfileTypesResponse::decode(body)
            .map(|r| r.profile_types.into_iter().map(|t| t.id).collect())
            .map_err(|x| e(&x)),
        (Method::ProfileTypes(_), Encoding::Json) => {
            serde_json::from_slice::<ProfileTypesResponse>(body)
                .map(|r| r.profile_types.into_iter().map(|t| t.id).collect())
                .map_err(|x| e(&x))
        }
        (Method::LabelNames(_), Encoding::Proto) => LabelNamesResponse::decode(body)
            .map(|r| r.names)
            .map_err(|x| e(&x)),
        (Method::LabelNames(_), Encoding::Json) => {
            serde_json::from_slice::<LabelNamesResponse>(body)
                .map(|r| r.names)
                .map_err(|x| e(&x))
        }
        (Method::LabelValues(..), Encoding::Proto) => LabelValuesResponse::decode(body)
            .map(|r| r.names)
            .map_err(|x| e(&x)),
        (Method::LabelValues(..), Encoding::Json) => {
            serde_json::from_slice::<LabelValuesResponse>(body)
                .map(|r| r.names)
                .map_err(|x| e(&x))
        }
        (Method::Series { .. }, _) => Err("a Series answer has label sets".to_string()),
    }
}

type LabelSet = BTreeSet<(String, String)>;

/// A `Series` answer's label sets.
fn label_sets(encoding: Encoding, body: &[u8]) -> Result<BTreeSet<LabelSet>, String> {
    let r = match encoding {
        Encoding::Proto => SeriesResponse::decode(body).map_err(|e| e.to_string())?,
        Encoding::Json => {
            serde_json::from_slice::<SeriesResponse>(body).map_err(|e| e.to_string())?
        }
    };
    Ok(r.labels_set
        .into_iter()
        .map(|l| l.labels.into_iter().map(|p| (p.name, p.value)).collect())
        .collect())
}

fn set(pairs: &[(&str, &str)]) -> LabelSet {
    pairs
        .iter()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect()
}

/// C13's expected sets under `t1` (`t1` true) or `t2`.
fn landing_sets(t1: bool) -> BTreeSet<LabelSet> {
    let mut out = BTreeSet::new();
    let pair =
        |service: &str, ty: &str| set(&[("service_name", service), ("__profile_type__", ty)]);
    for s in [
        "svc-otlp-proto",
        "svc-otlp-json",
        "svc-otlp-grpc",
        "unknown_service:fixture-exe",
    ] {
        out.insert(pair(s, TYPE_A));
    }
    for s in [
        "svc-push-proto",
        "svc-push-json",
        "unknown_service",
        "svc-ingest-pprof.cpu",
        "svc-ingest-multipart.cpu",
    ] {
        out.insert(pair(s, TYPE_A));
        out.insert(pair(s, TYPE_B));
    }
    for (ty, _) in jfr_totals() {
        out.insert(pair("svc-ingest-jfr", ty));
    }
    if t1 {
        out.insert(pair("svc-only", TYPE_A));
        out.insert(pair("svc-only", TYPE_B));
    } else {
        out.insert(pair("svc-only", TYPE_M));
    }
    out
}

/// Checks one server's answer to a fixture case, `None` when it holds.
/// Every fixture case is a success: an error status fails here, whatever
/// its body decodes as.
fn fixture_failure(fixture: Fixture, case: &Case, t1: bool, res: &Res) -> Option<String> {
    if res.status != 200 {
        return Some(format!("status {}, want 200", res.status));
    }
    check_fixture(fixture, case, t1, &res.body)
}

/// Checks a decoded answer against `fixture`, `None` when it holds.
fn check_fixture(fixture: Fixture, case: &Case, t1: bool, body: &[u8]) -> Option<String> {
    let method = &case.request.method;
    let encoding = case.request.encoding;
    let want_sets = |sets: Result<BTreeSet<LabelSet>, String>, want: BTreeSet<LabelSet>| match sets
    {
        Ok(got) if got == want => None,
        Ok(got) => Some(format!("label sets {got:?}, want {want:?}")),
        Err(e) => Some(format!("undecodable: {e}")),
    };
    match fixture {
        Fixture::LandingSets => return want_sets(label_sets(encoding, body), landing_sets(t1)),
        Fixture::PushProtoSets => {
            let sets = match label_sets(encoding, body) {
                Ok(s) => s,
                Err(e) => return Some(format!("undecodable: {e}")),
            };
            let all_push_proto = sets
                .iter()
                .all(|s| s.contains(&("rx".to_string(), "push-proto".to_string())));
            return (sets.len() != 4 || !all_push_proto)
                .then(|| format!("want 4 label sets, each with rx=push-proto: {sets:?}"));
        }
        _ => {}
    }
    let got = match values(method, encoding, body) {
        Ok(v) => v,
        Err(e) => return Some(format!("undecodable: {e}")),
    };
    match fixture {
        Fixture::Empty => (!got.is_empty()).then(|| format!("want empty, got {got:?}")),
        // The cross-tenant half is checked once both tenants have run.
        Fixture::TypesT2HoldsAlloc | Fixture::NonEmpty => got
            .is_empty()
            .then(|| "want non-empty, got empty".to_string()),
        Fixture::NoDottedName => {
            let dotted: Vec<&String> = got.iter().filter(|n| n.contains('.')).collect();
            (!dotted.is_empty()).then(|| format!("names with a dot: {dotted:?}"))
        }
        Fixture::Includes(want) => {
            let missing: Vec<&&str> = want
                .iter()
                .filter(|w| !got.iter().any(|g| g == **w))
                .collect();
            (!missing.is_empty()).then(|| format!("missing {missing:?} from {got:?}"))
        }
        Fixture::Exactly { t1: a, t2: b } => {
            let want = if t1 { a } else { b };
            (got != want).then(|| format!("got {got:?}, want {want:?}"))
        }
        Fixture::LandingSets | Fixture::PushProtoSets => None,
    }
}

// ---------------------------------------------------------------------
// The flame graph, as nodes
// ---------------------------------------------------------------------

/// A flame graph's nodes as `path → (total, self)`, the root `total`
/// included. Each level holds `[x delta, total, self, name]` per node; a
/// node's parent is the node one level up whose span holds its offset.
fn nodes(fg: &FlameGraph) -> BTreeMap<String, (i64, i64)> {
    let mut out = BTreeMap::new();
    let mut above: Vec<(i64, i64, String)> = Vec::new();
    for level in &fg.levels {
        let mut here: Vec<(i64, i64, String)> = Vec::new();
        let mut end = 0i64;
        for node in level.values.chunks(4) {
            let [delta, total, self_value, name] = node else {
                continue;
            };
            let x = end + delta;
            end = x + total;
            let name = fg
                .names
                .get(usize::try_from(*name).unwrap_or(usize::MAX))
                .cloned()
                .unwrap_or_else(|| format!("<name {name}>"));
            let path = match above
                .iter()
                .find(|(px, pt, _)| *px <= x && x < px + (*pt).max(1))
            {
                Some((_, _, parent)) => format!("{parent},{name}"),
                None => name,
            };
            out.insert(path.clone(), (*total, *self_value));
            here.push((x, *total, path));
        }
        above = here;
    }
    out
}

/// The fixture's flame graph at value `v` per count.
fn expected_nodes(v: i64) -> BTreeMap<String, (i64, i64)> {
    [
        ("total", (6 * v, 0)),
        ("total,main", (6 * v, v)),
        ("total,main,a", (5 * v, 0)),
        ("total,main,a,b", (3 * v, 3 * v)),
        ("total,main,a,c", (2 * v, 2 * v)),
    ]
    .into_iter()
    .map(|(p, n)| (p.to_string(), n))
    .collect()
}

/// V's `(rx, type, v)` per tenant.
fn value_checks(t1: bool) -> Vec<(&'static str, &'static str, i64)> {
    let (a, b) = if t1 {
        (10_000_000, 1)
    } else {
        (1_000_000_000, 100)
    };
    let mut out = Vec::new();
    for rx in ["otlp-proto", "otlp-json", "otlp-grpc", "otlp-noservice"] {
        out.push((rx, TYPE_A, a));
    }
    for rx in [
        "push-proto",
        "push-json",
        "push-noservice",
        "ingest-pprof",
        "ingest-multipart",
    ] {
        out.push((rx, TYPE_A, a));
        out.push((rx, TYPE_B, b));
    }
    if t1 {
        out.push(("only", TYPE_A, a));
        out.push(("only", TYPE_B, b));
    } else {
        out.push(("only", TYPE_M, 1_000));
    }
    out
}

// ---------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------

/// The request body of `method` in `encoding` over window `w`.
fn request_body(method: &Method, encoding: Encoding, w: (i64, i64)) -> Vec<u8> {
    let strings = |m: &[&str]| m.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    match (method, encoding) {
        (Method::ProfileTypes(window), Encoding::Proto) => {
            let (start, end) = match window {
                Window::Default => w,
                Window::Fixed(s, e) => (*s, *e),
            };
            ProfileTypesRequest { start, end }.encode_to_vec()
        }
        (Method::LabelNames(m), Encoding::Proto) => LabelNamesRequest {
            matchers: strings(m),
            start: w.0,
            end: w.1,
        }
        .encode_to_vec(),
        (Method::LabelValues(name, m), Encoding::Proto) => LabelValuesRequest {
            name: name.to_string(),
            matchers: strings(m),
            start: w.0,
            end: w.1,
        }
        .encode_to_vec(),
        (
            Method::Series {
                label_names,
                matchers,
            },
            Encoding::Proto,
        ) => SeriesRequest {
            matchers: strings(matchers),
            label_names: strings(label_names),
            start: w.0,
            end: w.1,
        }
        .encode_to_vec(),
        (Method::ProfileTypes(window), Encoding::Json) => {
            let (start, end) = match window {
                Window::Default => w,
                Window::Fixed(s, e) => (*s, *e),
            };
            json!({"start": start.to_string(), "end": end.to_string()})
                .to_string()
                .into_bytes()
        }
        (Method::LabelNames(m), Encoding::Json) => {
            json!({"matchers": m, "start": w.0.to_string(), "end": w.1.to_string()})
                .to_string()
                .into_bytes()
        }
        (Method::LabelValues(name, m), Encoding::Json) => json!({
            "name": name, "matchers": m, "start": w.0.to_string(), "end": w.1.to_string()
        })
        .to_string()
        .into_bytes(),
        (
            Method::Series {
                label_names,
                matchers,
            },
            Encoding::Json,
        ) => json!({
            "matchers": matchers, "labelNames": label_names,
            "start": w.0.to_string(), "end": w.1.to_string()
        })
        .to_string()
        .into_bytes(),
    }
}

/// Sends `request` to `server` as `tenant` (`None`: no header).
fn send(server: &Server, request: &Request, tenant: Option<&str>, w: (i64, i64)) -> Res {
    let body = request_body(&request.method, request.encoding, w);
    let (content_type, accept) = match (request.encoding, request.utf8) {
        (Encoding::Proto, false) => ("application/proto", None),
        (Encoding::Proto, true) => ("application/proto", Some("*/*; allow-utf8-labelnames=true")),
        (Encoding::Json, false) => ("application/json", None),
        (Encoding::Json, true) => (
            "application/json",
            Some("application/json; allow-utf8-labelnames=true"),
        ),
    };
    let mut headers = vec![("Content-Type", content_type)];
    if request.encoding == Encoding::Proto {
        headers.push(("Connect-Protocol-Version", "1"));
    }
    if let Some(a) = accept {
        headers.push(("Accept", a));
    }
    if let Some(t) = tenant {
        headers.push(("X-Scope-OrgID", t));
    }
    call(
        server,
        "POST",
        &format!("/querier.v1.QuerierService/{}", request.method.name()),
        &headers,
        &body,
    )
}

/// `LabelValues` proto for `rx` over `w`, as `tenant`.
fn rx_values(server: &Server, tenant: &str, w: (i64, i64)) -> Option<Vec<String>> {
    let request = proto(Method::LabelValues("rx", &["{}"]), false);
    let res = send(server, &request, Some(tenant), w);
    if res.status != 200 {
        return None;
    }
    LabelValuesResponse::decode(&res.body[..])
        .ok()
        .map(|r| r.names)
}

// ---------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------

/// Every difference the run met, reported once at the end.
#[derive(Default)]
struct Mismatches(Vec<String>);

impl Mismatches {
    fn both(
        &mut self,
        what: &str,
        why: &str,
        ours: &Res,
        theirs: &Res,
        method: &str,
        enc: Encoding,
    ) {
        self.0.push(format!(
            "{what}: {why}\n    ours:      {}\n    reference: {}",
            describe(ours, method, enc),
            describe(theirs, method, enc)
        ));
    }

    fn one(&mut self, what: &str, why: &str) {
        self.0.push(format!("{what}: {why}"));
    }
}

fn json_value(body: &[u8]) -> Option<Value> {
    serde_json::from_slice(body).ok()
}

/// `None` when two answers are equal under the case's rule (§5).
fn parity(request: &Request, ours: &Res, theirs: &Res) -> Option<&'static str> {
    if ours.status != theirs.status {
        return Some("status differs");
    }
    match (&request.method, request.encoding) {
        (Method::Series { .. }, Encoding::Json) => {
            let (a, b) = (
                label_sets(Encoding::Json, &ours.body),
                label_sets(Encoding::Json, &theirs.body),
            );
            match (a, b) {
                (Ok(a), Ok(b)) if a == b => None,
                (Ok(_), Ok(_)) => Some("label sets differ"),
                _ if ours.body == theirs.body => None,
                _ => Some("bodies differ"),
            }
        }
        (_, Encoding::Json) => {
            if ours.content_type != theirs.content_type {
                return Some("Content-Type differs");
            }
            match (json_value(&ours.body), json_value(&theirs.body)) {
                (Some(a), Some(b)) if a == b => None,
                (Some(_), Some(_)) => Some("JSON bodies differ"),
                _ if ours.body == theirs.body => None,
                _ => Some("bodies differ"),
            }
        }
        (_, Encoding::Proto) => {
            if ours.content_type != theirs.content_type {
                Some("Content-Type differs")
            } else if ours.body != theirs.body {
                Some("bodies differ")
            } else {
                None
            }
        }
    }
}

fn pin_holds(pin: &Pin, res: &Res) -> bool {
    res.status == pin.status && pin.body.is_none_or(|b| res.body == b)
}

/// A push's answers, compared (§5).
fn compare_push(answer: PushAnswer, ours: &Res, theirs: &Res) -> Option<&'static str> {
    if ours.status != theirs.status {
        return Some("status differs");
    }
    match answer {
        PushAnswer::Ingest => (ours.body != theirs.body).then_some("bodies differ"),
        PushAnswer::Otlp | PushAnswer::Connect => {
            if ours.content_type != theirs.content_type {
                return Some("Content-Type differs");
            }
            let is_json = ours
                .content_type
                .as_deref()
                .is_some_and(|c| c.starts_with("application/json"));
            let equal = if is_json {
                json_value(&ours.body) == json_value(&theirs.body)
            } else {
                match answer {
                    PushAnswer::Otlp => {
                        ExportProfilesServiceResponse::decode(&ours.body[..]).ok()
                            == ExportProfilesServiceResponse::decode(&theirs.body[..]).ok()
                    }
                    _ => {
                        PushResponse::decode(&ours.body[..]).ok()
                            == PushResponse::decode(&theirs.body[..]).ok()
                    }
                }
            };
            (!equal).then_some("decoded bodies differ")
        }
    }
}

/// One push to one server; the gRPC row answers as an HTTP-like [`Res`]:
/// status 0 and the encoded response, or the gRPC code and the message.
async fn send_push(server: &Server, push: &Push, tenant: &str, key: &str) -> Res {
    match &push.transport {
        Transport::Http {
            path,
            headers,
            body,
            ..
        } => {
            let mut h: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
            h.push(("X-Scope-OrgID", tenant));
            h.push(("Idempotency-Key", key));
            call(server, "POST", path, &h, body)
        }
        Transport::Grpc(message) => grpc_export(server, message, tenant, key, GRPC_DEADLINE).await,
    }
}

/// The [`Res`] content type of a gRPC push the deadline cut off.
const GRPC_NO_ANSWER: &str = "grpc no answer";

/// How long a gRPC push may take before it fails.
const GRPC_DEADLINE: Duration = Duration::from_secs(30);

/// One OTLP gRPC export to `server`, as an HTTP-like [`Res`].
async fn grpc_export(
    server: &Server,
    message: &ExportProfilesServiceRequest,
    tenant: &str,
    key: &str,
    deadline: Duration,
) -> Res {
    let export = async {
        let channel = tonic::transport::Endpoint::from_shared(server.base())
            .expect("an endpoint")
            .connect_lazy();
        let mut client = ProfilesServiceClient::new(channel);
        let mut request = tonic::Request::new(message.clone());
        let md = request.metadata_mut();
        md.insert("x-scope-orgid", tenant.parse().expect("ASCII metadata"));
        md.insert("idempotency-key", key.parse().expect("ASCII metadata"));
        md.insert(
            "otlp-profiles-development-version",
            "1".parse().expect("ASCII metadata"),
        );
        match client.export(request).await {
            Ok(answer) => Res {
                status: 0,
                content_type: Some("grpc ok".to_string()),
                body: answer.into_inner().encode_to_vec(),
            },
            Err(status) => Res {
                status: status.code() as u16,
                content_type: Some("grpc error".to_string()),
                body: status.message().as_bytes().to_vec(),
            },
        }
    };
    match tokio::time::timeout(deadline, export).await {
        Ok(res) => res,
        Err(_) => Res {
            status: tonic::Code::DeadlineExceeded as u16,
            content_type: Some(GRPC_NO_ANSWER.to_string()),
            body: format!("{}: no answer within {deadline:?}", server.name).into_bytes(),
        },
    }
}

// ---------------------------------------------------------------------
// L1
// ---------------------------------------------------------------------

/// `<run>`: 8 hex digits from the clock and the process id.
fn run_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as u64;
    let mixed = nanos ^ (u64::from(std::process::id()) << 24);
    format!("{:08x}", (mixed ^ (mixed >> 32)) as u32)
}

/// Polls the reference's `/ready` once a second until `200`; 180 s, then a
/// panic.
fn wait_ready(reference: &Server) {
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut last = String::from("no answer");
    while Instant::now() < deadline {
        if TcpStream::connect((reference.host.as_str(), reference.port)).is_ok() {
            let res = call(reference, "GET", "/ready", &[], &[]);
            if res.status == 200 {
                return;
            }
            last = format!(
                "{} {}",
                res.status,
                String::from_utf8_lossy(&res.body).trim()
            );
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    panic!(
        "the reference at {} was not ready within 180 s (last: {last})",
        reference.base()
    );
}

/// The filler writer (§3): while it lives, a one-line folded push to the
/// reference every 5 s under a tenant no case reads. The reference compacts
/// a tenant's newest blocks only when another block arrives, so without it
/// the fixture never becomes visible there.
struct Filler {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Filler {
    fn start(reference: Server, tenant: String) -> Self {
        use std::sync::atomic::Ordering;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            while !flag.load(Ordering::SeqCst) {
                let res = call(
                    &reference,
                    "POST",
                    "/ingest?name=filler.cpu",
                    &[("X-Scope-OrgID", &tenant)],
                    b"main 1",
                );
                if res.status != 200 {
                    eprintln!("filler push answered {}", res.status);
                }
                for _ in 0..50 {
                    if flag.load(Ordering::SeqCst) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for Filler {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// The reference's identity (§2): anything but the pinned build panics.
fn assert_reference_identity(reference: &Server, tenant: &str) {
    let res = call(
        reference,
        "GET",
        "/api/v1/status/buildinfo",
        &[("X-Scope-OrgID", tenant)],
        &[],
    );
    let text = String::from_utf8_lossy(&res.body).into_owned();
    assert_eq!(res.status, 200, "reference build info: {text}");
    let v: Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("reference build info is not JSON ({e}): {text}"));
    assert_eq!(
        v["data"]["version"],
        REFERENCE_VERSION,
        "the reference at {} is not the pinned build: {text}",
        reference.base()
    );
    assert_eq!(
        v["data"]["revision"],
        REFERENCE_REVISION,
        "the reference at {} is not the pinned build: {text}",
        reference.base()
    );
}

/// **L1.** Pushes the fixture to both servers under both tenants, then
/// compares every case and checks every fixture expectation, the stored
/// values and isolation; every difference is reported at once.
#[tokio::test(flavor = "multi_thread")]
async fn profileql_matches_the_reference() {
    // It skips only when both gates are unset outside a live CI job. In a
    // live CI job either gate missing panics (`pulsus_testkit`); anywhere,
    // one gate set without the other panics.
    let url = pulsus_testkit::live_endpoint("PULSUSDB_PROFILEQL_DIFF_URL");
    let database = pulsus_testkit::live_clickhouse_enabled();
    let url = match (url, database) {
        (Some(url), true) => url,
        (None, false) => {
            eprintln!(
                "skipping: neither PULSUSDB_PROFILEQL_DIFF_URL nor PULSUS_TEST_CLICKHOUSE=1 \
                 is set (see module docs)"
            );
            return;
        }
        (None, true) => panic!(
            "PULSUSDB_PROFILEQL_DIFF_URL is not set, but PULSUS_TEST_CLICKHOUSE=1 is: \
             the differential needs the reference's address as well as the database"
        ),
        (Some(_), false) => panic!(
            "PULSUS_TEST_CLICKHOUSE is not set, but PULSUSDB_PROFILEQL_DIFF_URL is: \
             the differential needs the database as well as the reference"
        ),
    };
    let reference = parse_url("reference", &url);
    let run = run_id();
    let t1 = format!("t1-{run}");
    let t2 = format!("t2-{run}");
    wait_ready(&reference);
    assert_reference_identity(&reference, &t1);

    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_profileql_differential_it")).await;
    let _server = spawn_ready(PORT, db.name());
    let ours = Server {
        name: "ours",
        host: "127.0.0.1".to_string(),
        port: PORT,
    };

    let now_ms = chrono::Utc::now().timestamp_millis();
    let t0_s = (now_ms - 5 * 60_000).div_euclid(1000);
    let w = (now_ms - HOUR_MS, now_ms + HOUR_MS);
    let mut mismatches = Mismatches::default();

    // Push §3 to both servers under both tenants.
    for (tenant, is_t1) in [(&t1, true), (&t2, false)] {
        for push in pushes(t0_s, &run, is_t1) {
            let key = format!("k-{}-{run}", push.rx);
            let a = send_push(&ours, &push, tenant, &key).await;
            let b = send_push(&reference, &push, tenant, &key).await;
            let what = format!("push {} under {tenant}", push.rx);
            let (method, why) = match &push.transport {
                Transport::Http { answer, .. } => {
                    let method = match answer {
                        PushAnswer::Otlp => "Export",
                        PushAnswer::Connect => "Push",
                        PushAnswer::Ingest => "ingest",
                    };
                    (method, compare_push(*answer, &a, &b))
                }
                Transport::Grpc(_) => {
                    let silent = |r: &Res| r.content_type.as_deref() == Some(GRPC_NO_ANSWER);
                    let why = if silent(&a) || silent(&b) {
                        Some("no answer within the gRPC deadline")
                    } else if a.status != b.status {
                        Some("gRPC outcome differs")
                    } else if a.status == 0 && a.body != b.body {
                        Some("decoded responses differ")
                    } else {
                        None
                    };
                    ("Export", why)
                }
            };
            if let Some(why) = why {
                mismatches.both(&what, why, &a, &b, method, Encoding::Proto);
            }
        }
    }
    assert!(
        mismatches.0.is_empty(),
        "{} push comparison(s) failed; no case runs on a half-pushed fixture:\n\n{}",
        mismatches.0.len(),
        mismatches.0.join("\n\n")
    );

    // Visible before compared, with the filler writer running.
    let filler = Filler::start(reference.clone(), format!("filler-{run}"));
    let deadline = Instant::now() + Duration::from_secs(240);
    for tenant in [&t1, &t2] {
        for server in [&ours, &reference] {
            pulsus_testkit::settle_by(
                deadline,
                Duration::from_secs(2),
                &format!("{} rx values under {tenant}", server.name),
                || rx_values(server, tenant, w).filter(|names| names == &RX),
            );
        }
    }
    drop(filler);

    // §4: C0–C14.
    let mut types: BTreeMap<(&str, bool), Vec<String>> = BTreeMap::new();
    for case in CASES {
        let tenants: Vec<(Option<&str>, bool)> = match case.request.tenancy {
            Tenancy::Both => vec![(Some(t1.as_str()), true), (Some(t2.as_str()), false)],
            Tenancy::Absent => vec![(None, true)],
        };
        let method = case.request.method.name();
        let enc = case.request.encoding;
        for (tenant, is_t1) in tenants {
            let what = format!(
                "{} ({}, {method}) under {}",
                case.id,
                case.client,
                tenant.unwrap_or("no tenant")
            );
            let a = send(&ours, &case.request, tenant, w);
            let b = send(&reference, &case.request, tenant, w);
            match case.expect {
                Expect::Ledger {
                    row,
                    ours: po,
                    theirs: pt,
                } => {
                    if !pin_holds(&po, &a) || !pin_holds(&pt, &b) {
                        mismatches.both(
                            &what,
                            &format!("ledger row `{row}`: an answer is not its pin"),
                            &a,
                            &b,
                            method,
                            enc,
                        );
                    } else if parity(&case.request, &a, &b).is_none() {
                        mismatches.both(
                            &what,
                            &format!(
                                "ledger row `{row}`: the two agree, so the reference changed \
                                 and the row is retired"
                            ),
                            &a,
                            &b,
                            method,
                            enc,
                        );
                    }
                }
                Expect::Parity(fixture) => {
                    if let Some(why) = parity(&case.request, &a, &b) {
                        mismatches.both(&what, why, &a, &b, method, enc);
                    }
                    for (res, server) in [(&a, &ours), (&b, &reference)] {
                        if let Some(why) = fixture_failure(fixture, case, is_t1, res) {
                            mismatches.one(
                                &what,
                                &format!("{}: not what the fixture says: {why}", server.name),
                            );
                        }
                        let banned: &[&str] = if is_t1 {
                            &["t2-only", "alloc_space"]
                        } else {
                            &["t1-only"]
                        };
                        for word in banned {
                            if find(&res.body, word.as_bytes()).is_some() {
                                mismatches.one(
                                    &what,
                                    &format!("{}: isolation: the body holds `{word}`", server.name),
                                );
                            }
                        }
                        if matches!(fixture, Fixture::TypesT2HoldsAlloc)
                            && let Ok(v) = values(&case.request.method, enc, &res.body)
                        {
                            types.insert((server.name, is_t1), v);
                        }
                    }
                }
            }
        }
    }
    for server in [&ours, &reference] {
        let t1_types = types.get(&(server.name, true)).cloned().unwrap_or_default();
        let t2_types = types
            .get(&(server.name, false))
            .cloned()
            .unwrap_or_default();
        let holds = t2_types
            .iter()
            .any(|t| t.contains("alloc_space") && !t1_types.contains(t));
        if !holds {
            mismatches.one(
                "C2",
                &format!(
                    "{}: t2 lists no type holding alloc_space that t1 does not: t1 {t1_types:?}, \
                     t2 {t2_types:?}",
                    server.name
                ),
            );
        }
    }

    // V: the stored values, per tenant, server, row and type.
    for (tenant, is_t1) in [(&t1, true), (&t2, false)] {
        for (rx, ty, v) in value_checks(is_t1) {
            let body = SelectMergeStacktracesRequest {
                profile_type_id: ty.to_string(),
                label_selector: format!("{{rx=\"{rx}\"}}"),
                start: w.0,
                end: w.1,
                ..SelectMergeStacktracesRequest::default()
            }
            .encode_to_vec();
            for server in [&ours, &reference] {
                let res = call(
                    server,
                    "POST",
                    "/querier.v1.QuerierService/SelectMergeStacktraces",
                    &[
                        ("Content-Type", "application/proto"),
                        ("Connect-Protocol-Version", "1"),
                        ("X-Scope-OrgID", tenant),
                    ],
                    &body,
                );
                let what = format!("V {rx} {ty} under {tenant}");
                let got = (res.status == 200)
                    .then(|| SelectMergeStacktracesResponse::decode(&res.body[..]).ok())
                    .flatten()
                    .map(|r| r.flamegraph.as_ref().map(nodes).unwrap_or_default());
                let want = expected_nodes(v);
                if got.as_ref() != Some(&want) {
                    mismatches.one(
                        &what,
                        &format!(
                            "{}: flame graph {got:?}, want {want:?} ({})",
                            server.name,
                            describe(&res, "SelectMergeStacktraces", Encoding::Proto)
                        ),
                    );
                }
            }
        }
    }

    // VJ (issue #819): the JFR row's stored totals, per tenant, server and
    // type, against the library's own expected outputs; the lock delay is
    // the ledger's, pinned on both sides.
    let (delay_theirs, delay_ours) = jfr_delay_pins();
    for (tenant, _) in [(&t1, true), (&t2, false)] {
        for (ty, want) in jfr_totals() {
            let body = SelectMergeStacktracesRequest {
                profile_type_id: ty.to_string(),
                label_selector: "{rx=\"ingest-jfr\"}".to_string(),
                start: w.0,
                end: w.1,
                ..SelectMergeStacktracesRequest::default()
            }
            .encode_to_vec();
            let mut totals = Vec::new();
            for server in [&ours, &reference] {
                let res = call(
                    server,
                    "POST",
                    "/querier.v1.QuerierService/SelectMergeStacktraces",
                    &[
                        ("Content-Type", "application/proto"),
                        ("Connect-Protocol-Version", "1"),
                        ("X-Scope-OrgID", tenant),
                    ],
                    &body,
                );
                let total = (res.status == 200)
                    .then(|| SelectMergeStacktracesResponse::decode(&res.body[..]).ok())
                    .flatten()
                    .and_then(|r| r.flamegraph.as_ref().map(|fg| fg.total));
                totals.push((server.name, total, res));
            }
            let what = format!("VJ ingest-jfr {ty} under {tenant}");
            if ty == JFR_DELAY_TYPE {
                let (o, r) = (totals[0].1, totals[1].1);
                let held = o.is_some_and(|o| delay_ours.contains(&o)) && r == Some(delay_theirs);
                if !held {
                    mismatches.one(
                        &what,
                        &format!(
                            "ledger row `{JFR_DELAY_ROW}`: an answer is not its pin: ours {o:?}, \
                             want {delay_ours:?}; reference {r:?}, want {delay_theirs}"
                        ),
                    );
                } else if o == r {
                    mismatches.one(
                        &what,
                        &format!(
                            "ledger row `{JFR_DELAY_ROW}`: the two agree, so the reference \
                             changed and the row is retired"
                        ),
                    );
                }
                continue;
            }
            for (name, total, res) in &totals {
                if *total != Some(want) {
                    mismatches.one(
                        &what,
                        &format!(
                            "{name}: total {total:?}, want {want} ({})",
                            describe(res, "SelectMergeStacktraces", Encoding::Proto)
                        ),
                    );
                }
            }
        }
    }

    assert!(
        mismatches.0.is_empty(),
        "{} difference(s) from the reference or the fixture:\n\n{}",
        mismatches.0.len(),
        mismatches.0.join("\n\n")
    );
}

// ---------------------------------------------------------------------
// Review round 1: the success status, the gRPC deadline, the skip
// ---------------------------------------------------------------------

/// A fixture case answered by an error status fails its fixture check,
/// even when the error body decodes as the expected answer (C1's empty
/// body does).
#[test]
fn a_fixture_case_needs_its_success_status() {
    let c1 = CASES.iter().find(|c| c.id == "C1").expect("C1");
    let Expect::Parity(fixture) = c1.expect else {
        panic!("C1 is a parity case");
    };
    let error = Res {
        status: 500,
        content_type: None,
        body: Vec::new(),
    };
    assert!(
        fixture_failure(fixture, c1, true, &error).is_some(),
        "a 500 with an empty body passed C1's fixture check"
    );
    let ok = Res {
        status: 200,
        content_type: Some("application/proto".to_string()),
        body: Vec::new(),
    };
    assert_eq!(fixture_failure(fixture, c1, true, &ok), None);
}

/// A gRPC push to a server that accepts the connection and never answers
/// fails within its deadline instead of holding the suite.
#[tokio::test(flavor = "multi_thread")]
async fn a_grpc_push_that_never_answers_fails_within_its_deadline() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind a silent listener");
    let addr = listener.local_addr().expect("its address");
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for conn in listener.incoming() {
            held.push(conn);
        }
    });
    let silent = Server {
        name: "silent",
        host: "127.0.0.1".to_string(),
        port: addr.port(),
    };
    let message = otlp_profile(1_000_000_000, 1, &[("service.name", "s")]);
    let answer = tokio::time::timeout(
        Duration::from_secs(10),
        grpc_export(&silent, &message, "t", "k", Duration::from_secs(1)),
    )
    .await;
    let res = answer.expect("the push held for 10 s against a 1 s deadline");
    assert_eq!(
        res.content_type.as_deref(),
        Some(GRPC_NO_ANSWER),
        "a push that was never answered reported {res:?}"
    );
}

/// Runs L1 in a child process with `set` and every other gate removed, and
/// returns whether it succeeded and its stderr.
fn run_l1_with(set: &[(&str, &str)]) -> (bool, String) {
    let exe = std::env::current_exe().expect("current_exe");
    let mut command = Command::new(exe);
    command
        .args(["profileql_matches_the_reference", "--exact", "--nocapture"])
        .env_remove("GITHUB_JOB")
        .env_remove("PULSUS_TEST_CLICKHOUSE")
        .env_remove("PULSUSDB_PROFILEQL_DIFF_URL");
    for (k, v) in set {
        command.env(k, v);
    }
    let output = command.output().expect("spawn the child test process");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// L1 skips only with both gates unset outside a live CI job. With the
/// database gate set and no reference address, or the address set and no
/// database gate, it fails.
#[test]
fn one_gate_without_the_other_fails_the_run() {
    let (ok, stderr) = run_l1_with(&[("PULSUS_TEST_CLICKHOUSE", "1")]);
    assert!(
        !ok && stderr.contains("PULSUSDB_PROFILEQL_DIFF_URL is not set"),
        "a set database with no reference address did not fail\n--- stderr ---\n{stderr}"
    );
    let (ok, stderr) = run_l1_with(&[("PULSUSDB_PROFILEQL_DIFF_URL", "http://127.0.0.1:9")]);
    assert!(
        !ok && stderr.contains("PULSUS_TEST_CLICKHOUSE is not set"),
        "a reference address with no database did not fail\n--- stderr ---\n{stderr}"
    );
    let (ok, stderr) = run_l1_with(&[]);
    assert!(
        ok,
        "with both gates unset outside CI, L1 must skip\n{stderr}"
    );
}
