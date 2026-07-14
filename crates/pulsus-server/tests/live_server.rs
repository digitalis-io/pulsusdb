//! Live end-to-end smoke test against a real ClickHouse
//! (docs/architecture.md §1, docs/api.md §7): spawns the real `pulsusdb`
//! binary and drives it over loopback HTTP, observing the `/ready` 503→200
//! cold-start transition plus `/metrics`/`/config`/`/buildinfo`. Deliberately
//! points `CLICKHOUSE_DB` at a database that does **not** pre-exist in the
//! container (`pulsus_live_test`, not `default`): a serving process (not
//! just `--mode init`) must create it before `/ready` can ever reach 200
//! (docs/architecture.md §1's schema-controller mount, review fix on issue
//! #6) — pointing at the pre-existing `default` database would silently
//! mask that path.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` so plain `cargo test --workspace`
//! stays hermetic. Shares the podman setup documented in
//! crates/pulsus-clickhouse/tests/live_clickhouse.rs (same single container,
//! no TLS, static config):
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test live_server
//! podman rm -f pulsus-ch-test
//! ```
//!
//! No HTTP client dependency is added just for this one file — a bare-bones
//! blocking HTTP/1.1 GET over loopback is enough (KISS: no TLS, no DNS).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
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
    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        // Short enough that the recurring TTL-rotation task (issue #6
        // review fix) ticks at least once during this test's remaining
        // assertions, cheaply exercising that code path end-to-end against
        // the real database it just created — without new instrumentation
        // to assert against a specific tick (KISS).
        .env("PULSUS_ROTATION_INTERVAL", "2s")
        .env(
            "CLICKHOUSE_SERVER",
            std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        )
        .env(
            "CLICKHOUSE_HTTP_PORT",
            std::env::var("PULSUS_TEST_CH_HTTP_PORT").unwrap_or_else(|_| "19123".to_string()),
        )
        .env(
            "CLICKHOUSE_DB",
            // Deliberately NOT `default` (the only database the bare
            // `clickhouse/clickhouse-server` image pre-creates, see
            // live_clickhouse.rs) — this database must be created by
            // startup's own schema-reconcile step, which is exactly the
            // behavior this test exists to prove.
            std::env::var("PULSUS_TEST_CH_DATABASE")
                .unwrap_or_else(|_| "pulsus_live_test".to_string()),
        )
        .spawn()
        .expect("spawn pulsusdb");
    let _guard = ChildGuard(child);

    // A longer deadline than a bare pool connect needs: startup now runs the
    // full schema reconcile (`CREATE DATABASE` + migrations + MVs) against
    // `pulsus_live_test` before `/ready` can flip to 200.
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
        "/ready never reached 200 within 60s — startup must create \
         `pulsus_live_test` itself, not just connect to a pre-existing database"
    );
    assert!(
        saw_503,
        "/ready should report 503 at least once before the schema is reconciled and the pool \
         connects (cold-start contract)"
    );

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

    // Cheap end-to-end proof that the TTL-rotation task (issue #6 review
    // fix) is actually wired into serving mode and does not crash the
    // process: wait past two rotation ticks (`PULSUS_ROTATION_INTERVAL=2s`
    // above) and confirm `/ready` is still 200 — a panicking or hung
    // rotation task would either kill the process or, in the same runtime,
    // start starving the reconnect/`/ready` machinery.
    std::thread::sleep(Duration::from_secs(5));
    let (status, _) = http_get(port, "/ready").expect("/ready reachable after rotation ticks");
    assert_eq!(
        status, 200,
        "server must stay healthy across multiple TTL-rotation ticks"
    );
}
