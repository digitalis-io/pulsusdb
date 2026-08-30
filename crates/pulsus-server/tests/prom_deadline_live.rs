//! Issue #471 M2: the request deadline, observed on the wire against a
//! **stalled** ClickHouse.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, same podman setup as
//! `prom_api_live.rs`. The harness is:
//!
//! 1. a TCP proxy on a fixed loopback port that forwards to the real
//!    ClickHouse HTTP port, so the server boots, migrates the schema and
//!    warms its label cache normally;
//! 2. a flag that makes every pump thread stop forwarding — connections
//!    are accepted and held open, nothing is written to the backend and
//!    nothing is read from it, so **no response can ever come back**;
//! 3. a server with `PULSUS_QUERY_TIMEOUT=3s` pointed at the proxy.
//!
//! Under the stall the request cannot complete, so the only question is
//! which deadline fires, and that is decided by construction rather than
//! by timing.
//!
//! ## What the miss-counter brackets establish, and what nothing here does
//!
//! Every stalled witness is bracketed by a `/metrics` scrape either side,
//! asserting `pulsus_label_cache_misses_total{reason="out_of_window"}`
//! increased by at least 1 across that pair. **That establishes the
//! request took the out-of-window fallback branch instead of being
//! answered from the resident snapshot** — the hole that would otherwise
//! make these witnesses vacuous, and it is checked per witness rather than
//! for the set.
//!
//! **It does not establish that a ClickHouse query was dispatched, and
//! nothing in this suite does.** `execute_fetch_plan` awaits a probe seam
//! after resolution and before either dispatch arm, so an implementation
//! that resolves, moves this counter and parks there satisfies every
//! assertion below. That implementation is admitted, not gated against.
//! The step from "took the fallback branch" to "issued the query" is read
//! from source and measured nowhere.
//!
//! The counter is attributable only under the precondition the brackets
//! enforce: the test owns every request to its own port and issues exactly
//! one between each pair of scrapes. An out-of-window request from any
//! client would move it.
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test prom_deadline_live
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::drop_db;

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use prost::Message;

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping
/// when the gate is absent in a live CI job (issue #320).
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

/// One raw HTTP/1.1 exchange, returning the status, the whole header block
/// and the body. The header block is returned rather than a parsed map
/// because the **absence** of `Content-Type` is load-bearing on the bare
/// `408`.
fn request(port: u16, head: &str, body: &[u8]) -> Option<(u16, String, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    stream.write_all(head.as_bytes()).ok()?;
    stream.write_all(body).ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let mut parts = text.splitn(2, "\r\n\r\n");
    let headers = parts.next()?.to_string();
    let body = decode_body(&headers, parts.next().unwrap_or(""));
    let status = headers
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, headers, body))
}

/// Undoes `Transfer-Encoding: chunked` framing — `/metrics` and the
/// `/api/v1/*` encoders both stream.
fn decode_body(head: &str, raw: &str) -> String {
    if !head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return raw.to_string();
    }
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(nl) = rest.find("\r\n") {
        let Ok(size) = usize::from_str_radix(rest[..nl].split(';').next().unwrap_or("").trim(), 16)
        else {
            break;
        };
        rest = &rest[nl + 2..];
        if size == 0 {
            break;
        }
        if rest.len() < size {
            out.push_str(rest);
            break;
        }
        out.push_str(&rest[..size]);
        rest = rest[size..].strip_prefix("\r\n").unwrap_or(&rest[size..]);
    }
    out
}

fn get(port: u16, path: &str) -> Option<(u16, String, String)> {
    request(
        port,
        &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
        &[],
    )
}

fn valid_remote_write_body() -> Vec<u8> {
    use pulsus_write::protocols::remote_write::{Label, Sample, TimeSeries, WriteRequest};
    let req = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![Label {
                name: "__name__".to_string(),
                value: "zz_deadline_write".to_string(),
            }],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1_700_000_000_000,
            }],
            histograms: vec![],
        }],
        metadata: vec![],
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy-compress a valid WriteRequest")
}

/// The value of `pulsus_label_cache_misses_total{reason="out_of_window"}`
/// in a `/metrics` scrape. `/metrics` is mounted in the public ops router
/// and merged **outside** the deadline layer, so it answers during a
/// stall; the handler also re-reads the cache snapshot on every scrape, so
/// a scrape sees the value the request that just finished produced.
fn out_of_window_misses(port: u16) -> u64 {
    let (status, _, body) = get(port, "/metrics").expect("/metrics reachable during the stall");
    assert_eq!(status, 200, "/metrics must answer during the stall");
    for line in body.lines() {
        if line.starts_with("pulsus_label_cache_misses_total")
            && line.contains("reason=\"out_of_window\"")
        {
            return line
                .rsplit(' ')
                .next()
                .and_then(|v| v.trim().parse::<f64>().ok())
                .map(|v| v as u64)
                .unwrap_or_else(|| panic!("unparseable counter line: {line}"));
        }
    }
    panic!("no out_of_window miss counter in the scrape");
}

fn cache_refreshes(port: u16) -> u64 {
    let (_, _, body) = get(port, "/metrics").expect("/metrics reachable");
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("pulsus_label_cache_refreshes_total ") {
            return rest.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// Accepts on `listen_port` and forwards to the `backend` port until `stalled`
/// flips, after which it forwards nothing in either direction and holds
/// every connection open. A connection opened while stalled is accepted
/// and its backend dial skipped.
///
/// The proxy is the **stall** and nothing else: it observes nothing and is
/// claimed for nothing. The evidence that a request left the resident
/// label cache is the server's own counter, read through `/metrics`.
fn spawn_stall_proxy(listen_port: u16, backend: u16, stalled: Arc<AtomicBool>) {
    let listener = TcpListener::bind(("127.0.0.1", listen_port)).expect("bind the stall proxy");
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(client) = conn else { continue };
            let stalled = Arc::clone(&stalled);
            std::thread::spawn(move || {
                if stalled.load(Ordering::SeqCst) {
                    hold_open(client);
                    return;
                }
                let Ok(backend) = TcpStream::connect(("127.0.0.1", backend)) else {
                    return;
                };
                let (client_rx, client_tx) = (
                    client.try_clone().expect("clone client"),
                    client.try_clone().expect("clone client"),
                );
                let (backend_rx, backend_tx) = (
                    backend.try_clone().expect("clone backend"),
                    backend.try_clone().expect("clone backend"),
                );
                let up = Arc::clone(&stalled);
                let handle = std::thread::spawn(move || pump(client_rx, backend_tx, &up));
                pump(backend_rx, client_tx, &stalled);
                let _ = handle.join();
            });
        }
    });
}

fn hold_open(mut client: TcpStream) {
    let mut buf = [0u8; 8192];
    client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .ok();
    loop {
        match client.read(&mut buf) {
            Ok(0) => return,
            Ok(_) => {}
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => return,
        }
    }
}

fn pump(mut from: TcpStream, mut to: TcpStream, stalled: &AtomicBool) {
    from.set_read_timeout(Some(Duration::from_millis(50))).ok();
    let mut buf = [0u8; 16384];
    loop {
        if stalled.load(Ordering::SeqCst) {
            // Forward nothing, ever, in either direction — which is what
            // produces the stall. The connection is left open.
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }
        match from.read(&mut buf) {
            Ok(0) => {
                let _ = to.shutdown(Shutdown::Write);
                return;
            }
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    return;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => return,
        }
    }
}

const SERVER_DEADLINE_BODY: &str = concat!(
    r#"{"status":"error","errorType":"timeout","#,
    r#""error":"request exceeded the server deadline of 3s (PULSUS_QUERY_TIMEOUT)"}"#
);
const REQUESTED_TIMEOUT_BODY: &str = concat!(
    r#"{"status":"error","errorType":"timeout","#,
    r#""error":"query exceeded the requested timeout of 1ms (timeout parameter)"}"#
);

#[tokio::test(flavor = "multi_thread")]
async fn prom_request_deadline_answers_503_with_the_error_envelope() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test");
        return;
    }

    let db = pulsus_testkit::test_db("pulsus_prom_deadline_it");
    let port: u16 = 31_301;
    let proxy_port: u16 = 31_302;
    // Deliberately NOT named `*_port`: `live_port_uniqueness`'s rule 1
    // requires every `port`-named binding to be a plain integer literal in
    // the reserved range, and this one is read from the environment.
    let clickhouse_http: u16 = std::env::var("PULSUS_TEST_CH_HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(19123);

    let stalled = Arc::new(AtomicBool::new(false));
    spawn_stall_proxy(proxy_port, clickhouse_http, Arc::clone(&stalled));

    // The `/api/v1/write` leg below is deliberately interrupted mid-insert,
    // so the writer core spools the batch as *uncertain* into `./spool`
    // relative to the process's working directory — which for a test
    // binary is the crate root. Give the child its own scratch directory
    // so a run never leaves files in the checkout.
    let workdir = std::env::temp_dir().join(format!("pulsus-deadline-live-{}", std::process::id()));
    std::fs::create_dir_all(&workdir).expect("create the child's working directory");

    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .current_dir(&workdir)
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        // The deadline under test. Every other `query_timeout`-fed clock
        // carries the same duration, and the HTTP one starts first — which
        // is exactly what this suite converts from an argument into a
        // measurement: a ClickHouse-side deadline winning would produce a
        // body beginning `clickhouse:`.
        .env("PULSUS_QUERY_TIMEOUT", "3s")
        .env("PULSUS_CACHE_TTL", "2s")
        .env("PULSUS_CH_POOL_SIZE", "16")
        .env("CLICKHOUSE_SERVER", "127.0.0.1")
        .env("CLICKHOUSE_HTTP_PORT", proxy_port.to_string())
        .env("CLICKHOUSE_DB", &db)
        .spawn()
        .expect("spawn pulsusdb");
    let _guard = ChildGuard(child);

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut became_ready = false;
    while Instant::now() < deadline {
        if let Some((200, _, _)) = get(port, "/ready") {
            became_ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(became_ready, "/ready never reached 200 within 60s");

    // The out-of-window branch is only reachable once the cache is WARM
    // (generation != 0); a cold cache takes the `cold` branch and moves a
    // different counter, which would make every bracket below vacuous.
    let deadline = Instant::now() + Duration::from_secs(60);
    while cache_refreshes(port) == 0 {
        assert!(
            Instant::now() < deadline,
            "the label cache never completed a refresh within 60s"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    stalled.store(true, Ordering::SeqCst);

    // -----------------------------------------------------------------
    // 11c-1 / C3 — the requested timeout, on `/query`
    // -----------------------------------------------------------------
    let before = out_of_window_misses(port);
    let started = Instant::now();
    let (status, headers, body) = get(
        port,
        "/api/v1/query?query=zz_deadline_probe_1&time=100&timeout=0.001",
    )
    .expect("stalled /query with a requested timeout");
    let elapsed = started.elapsed();
    let after = out_of_window_misses(port);
    assert_eq!(status, 503, "{headers}\n{body}");
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: application/json"),
        "{headers}"
    );
    // Status alone discriminates nothing here — every deadline answer is
    // `503`/`timeout`, and only the body separates the two producers.
    assert_eq!(body.trim(), REQUESTED_TIMEOUT_BODY);
    assert!(
        elapsed < Duration::from_secs(3),
        "the requested 1ms timeout must fire well inside the 3s server deadline, took {elapsed:?}"
    );
    assert!(
        after > before,
        "11c-1: the request was answered without leaving the resident label cache ({before} -> {after})"
    );

    // -----------------------------------------------------------------
    // 11c-2 / C4 — the same, on `/query_range`. The branch is written
    // twice, so it is witnessed twice: an implementation correct only for
    // `/query` passes every other cell.
    // -----------------------------------------------------------------
    let before = out_of_window_misses(port);
    let started = Instant::now();
    let (status, headers, body) = get(
        port,
        "/api/v1/query_range?query=zz_deadline_probe_2&start=0&end=100&step=1&timeout=0.001",
    )
    .expect("stalled /query_range with a requested timeout");
    let elapsed = started.elapsed();
    let after = out_of_window_misses(port);
    assert_eq!(status, 503, "{headers}\n{body}");
    assert_eq!(body.trim(), REQUESTED_TIMEOUT_BODY);
    assert!(elapsed < Duration::from_secs(3), "took {elapsed:?}");
    assert!(
        after > before,
        "11c-2: `/query_range` answered without leaving the resident label cache ({before} -> {after})"
    );

    // -----------------------------------------------------------------
    // 11c-3 / C5 — no `timeout` parameter: the request-deadline layer
    // answers, with the OTHER literal, at about 3s.
    // -----------------------------------------------------------------
    let before = out_of_window_misses(port);
    let started = Instant::now();
    let (status, headers, body) = get(port, "/api/v1/query?query=zz_deadline_probe_3&time=100")
        .expect("stalled /query with no requested timeout");
    let elapsed = started.elapsed();
    let after = out_of_window_misses(port);
    assert_eq!(status, 503, "{headers}\n{body}");
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: application/json"),
        "{headers}"
    );
    assert_eq!(body.trim(), SERVER_DEADLINE_BODY);
    assert!(
        elapsed >= Duration::from_secs(3),
        "the server deadline must not fire early, took {elapsed:?}"
    );
    assert!(
        after > before,
        "11c-3: the no-timeout query parked without leaving the resident label cache ({before} -> {after})"
    );

    // -----------------------------------------------------------------
    // The negative half: the two kinds of excluded path keep the
    // byte-identical bare `408`. `/api/v1/write` is the one that would
    // catch a bare prefix test, and it is not optional.
    //
    // **Why these assert presence and absence rather than an exact header
    // set**, unlike the hermetic twin in `middleware.rs`: a response off a
    // real server has also passed through the globally-applied CORS and
    // trace layers, so it additionally carries `access-control-allow-
    // origin: *` and a `vary:` line — on **every** route, before and after
    // issue #471 alike, and on the `405`s and `200`s too. Those are not
    // this change's to pin, and pinning them here would make an unrelated
    // CORS default a failure of the deadline suite. What is load-bearing
    // is what the deadline layer itself contributes: the status, an empty
    // body, and the ABSENCE of `Content-Type` — the last being what kept
    // this response out of the set a client parses a body for.
    // -----------------------------------------------------------------
    let rw = valid_remote_write_body();
    let (status, headers, body) = request(
        port,
        &format!(
            "POST /api/v1/write HTTP/1.1\r\nHost: localhost\r\n\
             Content-Type: application/x-protobuf\r\nContent-Encoding: snappy\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n",
            rw.len()
        ),
        &rw,
    )
    .expect("stalled /api/v1/write");
    assert_eq!(status, 408, "{headers}\n{body}");
    assert!(
        headers.to_ascii_lowercase().contains("content-length: 0"),
        "{headers}"
    );
    assert!(
        !headers.to_ascii_lowercase().contains("content-type:"),
        "the bare 408 carries no Content-Type: {headers}"
    );
    assert!(body.is_empty(), "the bare 408 has an empty body: {body:?}");

    let (status, headers, body) =
        get(port, "/api/logs/v1/labels").expect("stalled /api/logs/v1/labels");
    assert_eq!(status, 408, "{headers}\n{body}");
    assert!(
        headers.to_ascii_lowercase().contains("content-length: 0"),
        "{headers}"
    );
    assert!(
        !headers.to_ascii_lowercase().contains("content-type:"),
        "the bare 408 carries no Content-Type: {headers}"
    );
    assert!(body.is_empty(), "the bare 408 has an empty body: {body:?}");

    // Unstall so the drop below can reach ClickHouse.
    stalled.store(false, Ordering::SeqCst);
    drop_db(&db).await;
    let _ = std::fs::remove_dir_all(&workdir);
}
