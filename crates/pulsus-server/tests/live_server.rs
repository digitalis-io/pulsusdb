//! Live end-to-end smoke test against a real ClickHouse
//! (docs/architecture.md §1, docs/api.md §7): spawns the real `pulsusdb`
//! binary and drives it over loopback HTTP, observing the `/ready` 503→200
//! cold-start transition plus `/metrics`/`/config`/`/buildinfo`. Deliberately
//! points `CLICKHOUSE_DB` at a database that does **not** pre-exist in the
//! container, built by `schema/schema.sh`'s own statements before the
//! spawn. **The binary creates no schema**, and the second case here is the
//! other half of that: with the database absent, `/ready` stays 503 and the
//! process says which command builds it.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` so plain `cargo test --workspace`
//! stays hermetic. Shares the podman setup documented in
//! crates/pulsus-clickhouse/tests/live_clickhouse.rs (same single container,
//! no TLS, static config):
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.8.21.10
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test live_server
//! podman rm -f pulsus-ch-test
//! ```
//!
//! No HTTP client dependency is added just for this one file — a bare-bones
//! blocking HTTP/1.1 GET over loopback is enough (KISS: no TLS, no DNS).

#[path = "support/live_db.rs"]
mod live_db;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

/// Issues a bare HTTP/1.1 GET over loopback and returns `(status, body)`.
/// `None` on any connection failure (e.g. the server has not bound yet).
fn http_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut buf = String::new();
    stream.read_to_string(&mut buf).ok()?;
    let mut parts = buf.splitn(2, "\r\n\r\n");
    let head = parts.next()?;
    let body = parts.next().unwrap_or("").to_string();
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, body))
}

/// Kills the spawned `pulsusdb` process on drop (including on test panic),
/// so a failing assertion never leaks a background server process.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn ready_transitions_from_503_to_200_and_ops_endpoints_respond() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-clickhouse/tests/live_clickhouse.rs for setup)"
        );
        return;
    }

    let port: u16 = 31_100;
    let db = pulsus_testkit::test_db("pulsus_server_live_it");
    // The binary creates no schema; build it before the spawn or `/ready`
    // never reaches 200. This is what `schema/schema.sh` does for a
    // deployment, rendering the same file.
    live_db::build_schema_blocking(&db);

    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env(
            "CLICKHOUSE_SERVER",
            std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        )
        .env(
            "CLICKHOUSE_HTTP_PORT",
            std::env::var("PULSUS_TEST_CH_HTTP_PORT").unwrap_or_else(|_| "19123".to_string()),
        )
        // Deliberately NOT `default` (the only database the bare
        // `clickhouse/clickhouse-server` image pre-creates, see
        // live_clickhouse.rs): a serving process reads a database somebody
        // else built, and this proves it reaches `/ready` against one.
        .env("CLICKHOUSE_DB", &db)
        .spawn()
        .expect("spawn pulsusdb");
    let _guard = ChildGuard(child);

    // A longer deadline than a bare pool connect needs: startup now runs the
    // full schema reconcile (`CREATE DATABASE` + migrations + MVs) against
    // `pulsus_server_live_it` before `/ready` can flip to 200.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut saw_503 = false;
    let mut became_ready = false;
    while Instant::now() < deadline {
        if let Some((status, _)) = http_get(port, "/ready") {
            match status {
                503 => saw_503 = true,
                200 => {
                    became_ready = true;
                    break;
                }
                other => panic!("unexpected /ready status {other}"),
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        became_ready,
        "/ready never reached 200 within 60s against a database that exists"
    );
    // **The 503 window is not asserted here any more.** It used to be the
    // time the schema reconcile took, which was long enough to observe
    // every run. With the schema built beforehand all that is left is the
    // pool connect, and the process can pass through it between two polls.
    // The 503 state is pinned by
    // `an_absent_schema_keeps_ready_at_503_and_names_the_script`, where it
    // is the steady state rather than a window.
    let _ = saw_503;

    // `/ready` just reached 200 under the default `Mode::All`, which
    // implies the label cache is warm (issue #30) — its counters/gauges
    // must already be present in this very scrape (`ops::metrics_handler`
    // bridges them through the `metrics` facade on every request, not on a
    // timer), proving the "cache hit/size/age metrics on `/metrics`" AC
    // end to end against a real process.
    let (status, body) = http_get(port, "/metrics").expect("/metrics reachable");
    assert_eq!(status, 200);
    for metric in [
        "pulsus_label_cache_series_count",
        "pulsus_label_cache_age_ms",
        "pulsus_label_cache_oversize",
        "pulsus_label_cache_hits_total",
        "pulsus_label_cache_misses_total",
        "pulsus_label_cache_refreshes_total",
    ] {
        assert!(
            body.contains(metric),
            "missing {metric:?} in /metrics body: {body}"
        );
    }

    let (status, body) = http_get(port, "/config").expect("/config reachable");
    assert_eq!(status, 200);
    assert!(!body.is_empty(), "/config body must not be empty");

    let (status, body) = http_get(port, "/buildinfo").expect("/buildinfo reachable");
    assert_eq!(status, 200);
    for field in ["version", "revision", "builtAt", "rustc"] {
        assert!(body.contains(field), "missing {field:?} in {body}");
    }

    // The process stays healthy once it is up, with no background schema
    // task to crash it.
    std::thread::sleep(Duration::from_secs(5));
    let (status, _) = http_get(port, "/ready").expect("/ready reachable later");
    assert_eq!(status, 200, "server must stay healthy");
}

/// **An absent schema is refused, and the refusal names the command that
/// builds one.** The binary creates no schema, so this is the whole of what
/// a fresh ClickHouse with no database gets: `/ready` stays 503 and the log
/// says `schema/schema.sh`.
///
/// Retried rather than terminal, on purpose — the chart's schema Job and
/// the serving Deployments carry no ordering, so a server that started
/// first must converge once the schema lands.
#[test]
fn an_absent_schema_keeps_ready_at_503_and_names_the_script() {
    if !pulsus_testkit::live_clickhouse_enabled() {
        eprintln!("skipping: PULSUS_TEST_CLICKHOUSE is not set");
        return;
    }

    let port: u16 = 31_231;
    // Composed, dropped, and deliberately NOT built.
    let db = pulsus_testkit::test_db("pulsus_server_live_absent_it");

    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env(
            "CLICKHOUSE_SERVER",
            std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        )
        .env(
            "CLICKHOUSE_HTTP_PORT",
            std::env::var("PULSUS_TEST_CH_HTTP_PORT").unwrap_or_else(|_| "19123".to_string()),
        )
        .env("CLICKHOUSE_DB", &db)
        // `tracing`'s subscriber writes to stdout, not stderr.
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn pulsusdb");
    let mut child = ChildGuard(child);
    let mut out = child.0.stdout.take().expect("piped stdout");

    // The process listens straight away and answers 503 while the startup
    // checks are still failing.
    let mut saw_503 = false;
    for _ in 0..50 {
        if let Some((status, _)) = http_get(port, "/ready")
            && status == 503
        {
            saw_503 = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(saw_503, "/ready must answer 503 while the schema is absent");

    // And it stays 503: nothing creates the database.
    std::thread::sleep(Duration::from_secs(2));
    let (status, _) = http_get(port, "/ready").expect("/ready reachable");
    assert_eq!(
        status, 503,
        "/ready must stay 503 while the schema is absent"
    );

    // The refusal names the command. Read with the child still running: it
    // never exits, so the read is bounded by killing it first.
    drop(child);
    let mut log = String::new();
    let _ = out.read_to_string(&mut log);
    assert!(
        log.contains("does not exist"),
        "the log must say the database does not exist: {log}"
    );
    assert!(
        log.contains("schema/schema.sh"),
        "the refusal must name the script that builds the schema: {log}"
    );
}
