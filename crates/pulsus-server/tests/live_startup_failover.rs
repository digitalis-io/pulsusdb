//! Issue #803: the startup checks are one read request, on a bootstrap pool
//! kept across the reconnect loop's iterations.
//!
//! * S1 — a server that fails the checks with a server failure is demoted,
//!   and the checks run once more at once on the next server.
//! * S2 — a server that refuses for another reason (a version below the
//!   minimum) backs the loop off as before, and the next iteration's
//!   binding is the next server in the kept pool's rotation.
//!
//! Spawns the `pulsusdb` binary with `CLICKHOUSE_SERVERS` naming a
//! stub ClickHouse first and this job's server second. Gated behind
//! `PULSUS_TEST_CLICKHOUSE=1`; the single-node leg runs it.

#[path = "support/live_db.rs"]
mod live_db;
#[path = "../../pulsus-clickhouse/tests/common/stub_server.rs"]
mod stub_server;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use live_db::ScopedDb;
use stub_server::{Mode, Reply, StubServer};

/// Issues a bare HTTP/1.1 GET over loopback and returns the status.
fn http_status(port: u16, path: &str) -> Option<u16> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut buf = String::new();
    stream.read_to_string(&mut buf).ok()?;
    buf.lines().next()?.split_whitespace().nth(1)?.parse().ok()
}

/// Kills the spawned `pulsusdb` process on drop.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawns `pulsusdb` on `port` over `db`, with the stub listed before this
/// job's server.
fn spawn(port: u16, db: &ScopedDb, stub: &StubServer) -> ChildGuard {
    let servers = format!(
        "127.0.0.1:{},{}:{}",
        stub.addr.port(),
        live_db::ch_host(),
        live_db::ch_http_port()
    );
    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVERS", servers)
        .env("CLICKHOUSE_DB", db.name())
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn pulsusdb");
    ChildGuard(child)
}

/// Whether `/ready` answers `200` within 30 s.
fn ready_within_30s(port: u16) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if http_status(port, "/ready") == Some(200) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_checks_move_off_a_server_that_fails_them() {
    if !pulsus_testkit::live_clickhouse_enabled() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    let port: u16 = 31_250;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_startup_failover_s1_it")).await;
    let stub = StubServer::start(Mode::Code(210));
    let server = spawn(port, &db, &stub);

    let ready = tokio::task::spawn_blocking(move || ready_within_30s(port))
        .await
        .expect("the readiness poll");
    drop(server);
    assert!(ready, "/ready must reach 200 within 30 s");
    assert!(
        stub.answered() >= 1,
        "the stub received a check statement: {:?}",
        stub.requests()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_checks_reach_the_next_server_on_the_next_iteration() {
    if !pulsus_testkit::live_clickhouse_enabled() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }
    let port: u16 = 31_251;
    let db = ScopedDb::fresh(pulsus_testkit::test_db("pulsus_startup_failover_s2_it")).await;
    let stub = StubServer::start(Mode::Route {
        prefix: "SELECT version() AS v",
        reply: Reply::Strings("v", vec!["1.0".to_string()]),
        otherwise: Reply::Code(210),
    });
    let server = spawn(port, &db, &stub);

    let ready = tokio::task::spawn_blocking(move || ready_within_30s(port))
        .await
        .expect("the readiness poll");
    drop(server);
    let version_requests = stub
        .requests()
        .iter()
        .filter(|q| q.starts_with("SELECT version() AS v"))
        .count();
    assert!(ready, "/ready must reach 200 within 30 s");
    assert_eq!(
        version_requests,
        1,
        "the stub answered exactly one version request: {:?}",
        stub.requests()
    );
}
