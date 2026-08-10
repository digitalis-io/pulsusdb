//! Issue #311 — **the wiring seam**: `reader.template_timezone` reaches a
//! rendered query response in a REAL server process.
//!
//! Everything else about this feature is already covered hermetically: the
//! config parses and rejects (`pulsus-config`), the read path honours an
//! installed zone and ignores `$TZ`
//! (`pulsus-read/tests/logql_template_timezone.rs`), and no source reads a
//! host zone (`pulsus-read/tests/no_ambient_timezone_reads.rs`). The one
//! thing none of them can see is the single line in `serve::run` that
//! installs the configured zone at startup. Drop it and every one of those
//! suites still passes, while every deployment silently renders in `UTC`
//! and the setting does nothing — so it gets its own end-to-end gate here,
//! against the binary, through HTTP.
//!
//! **Why `Pacific/Kiritimati` (UTC+14).** A whole-day shift makes the three
//! outcomes textually distinct: configured (`2023-11-15 12:13:20 +1400
//! +14`), not-installed (`2023-11-14 22:13:20 +0000 UTC`, a different DATE
//! and zone name), and off-by-hours (a different time, same date). A
//! one-hour zone would let a not-installed bug and a DST bug produce
//! neighbouring strings.
//!
//! Three servers are booted against the same seeded rows — one configured,
//! one left at the default, one configured again — so the assertion is not
//! just "this zone renders like this" but "the configuration is the ONLY
//! thing that differs, and it is what changed the answer".
//!
//! **Two of them run under a hostile `$TZ`, and that is load-bearing.**
//! A host-timezone read can only change an answer when the host channel it
//! reads is non-empty. `$TZ` is unset on a stock CI runner and on the
//! development host, so with it unset a planted `env::var("TZ")` read in
//! the template path falls through to the configured value and this suite
//! reports GREEN on a real defect — measured, not hypothesised (the
//! round-2 review's probe table: same planted read, green with `$TZ`
//! unset, red under `TZ=Pacific/Apia`). That is the golden-file trap one
//! level up: a suite cannot observe an ambient dependency whose channel is
//! empty. Populating the channel is the fix, so the class stops being
//! invisible instead of being documented as a gap.
//!
//! The other ambient zone channel, `/etc/localtime`, needs no fixture: it
//! is always populated with *something*, and the configured zone here
//! (`Pacific/Kiritimati`) is one no realistic host or runner sits in, so
//! any leak of the host zone is a mismatch. When it is absent it cannot
//! leak either. Verified: a composed-path `/etc/localtime` read planted in
//! the template path reddens this suite (`left: "…+0000 GMT"`, the
//! development host's own zone).
//!
//! **The residual, stated rather than glossed.** A leak whose value
//! happens to equal the expected rendering is invisible — inherent to
//! observing output, and the reason the zones above are chosen to be ones
//! nothing else in the fixture or the environment produces.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`; same podman harness as the
//! sibling live suites. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:24.8
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-server --test template_timezone_live
//! podman rm -f pulsus-ch-test
//! ```

#[path = "support/live_db.rs"]
mod live_db;

use live_db::drop_db;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};

/// `true` when this suite should run. Skips cleanly without a container;
/// **panics** rather than skipping when the gate is absent in a live CI
/// job, so a lost `env:` block reddens the build (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

/// A fixed instant, so the expected renderings below are literals rather
/// than something this test computes (a computed expectation would
/// reimplement the formatting under test). `1_700_000_000` seconds =
/// 2023-11-14 22:13:20 UTC.
const FIXED_TS_NS: i64 = 1_700_000_000_000_000_000;

/// What the fixed instant renders as in the CONFIGURED zone (UTC+14 —
/// note the date rolls forward to the 15th).
const RENDERED_IN_KIRITIMATI: &str = "2023-11-15 12:13:20 +1400 +14";

/// …and what it renders as on a server left at the shipped default.
const RENDERED_IN_UTC: &str = "2023-11-14 22:13:20 +0000 UTC";

/// The hostile `$TZ` the UNCONFIGURED server runs under. UTC+13, so a leak
/// renders `2023-11-15 11:13:20 +1300 +13` — a different date and a
/// non-zero offset, textually unmistakable against `+0000 UTC`.
const HOSTILE_TZ_FOR_DEFAULT: &str = "Pacific/Apia";

/// The hostile `$TZ` a CONFIGURED server runs under — deliberately on the
/// far side of the date line from `Pacific/Kiritimati`, so a leak renders
/// `2023-11-14 13:13:20 -0900 AKST`: opposite offset sign, earlier day,
/// distinct abbreviation. Nothing but a genuine host read produces it.
const HOSTILE_TZ_FOR_CONFIGURED: &str = "America/Anchorage";

const SERVICE: &str = "tzwire";
const FINGERPRINT: u64 = 0x8000_0000_0000_0311;

fn ch_host() -> String {
    std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string())
}

fn ch_http_port() -> u16 {
    std::env::var("PULSUS_TEST_CH_HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(19123)
}

fn conn_config(db: &str) -> ChConnConfig {
    ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

/// A bare HTTP/1.1 GET over loopback — no HTTP client dependency for two
/// requests (KISS, the sibling live suites' rationale).
fn http_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok()?;
    let split_at = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buf[..split_at]).into_owned();
    let raw_body = &buf[split_at + 4..];
    let chunked = head.lines().any(|l| {
        l.to_ascii_lowercase()
            .starts_with("transfer-encoding: chunked")
    });
    let body = if chunked {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, String::from_utf8_lossy(&body).into_owned()))
}

fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(line_end) = raw.windows(2).position(|w| w == b"\r\n") {
        let Ok(size) = usize::from_str_radix(String::from_utf8_lossy(&raw[..line_end]).trim(), 16)
        else {
            break;
        };
        if size == 0 {
            break;
        }
        let (start, end) = (line_end + 2, line_end + 2 + size);
        if end > raw.len() {
            break;
        }
        out.extend_from_slice(&raw[start..end]);
        raw = &raw[(end + 2).min(raw.len())..];
    }
    out
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

/// Spawns the REAL `pulsusdb` binary — so startup runs `main.rs` →
/// `serve::run`, which is the code path under test — and blocks until
/// `/ready` is 200.
fn spawn_ready(port: u16, db: &str, extra_env: &[(&str, &str)]) -> ChildGuard {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pulsusdb"));
    command
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db)
        // The fixture instant is in 2023; the default 7-day TTL would make
        // the seeded rows eligible for deletion on any merge.
        .env("PULSUS_RETENTION_DAYS", "36500");
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let guard = ChildGuard(command.spawn().expect("spawn pulsusdb"));
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if http_get(port, "/ready").is_some_and(|(status, _)| status == 200) {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s (port {port}, db {db})");
}

/// Seeds exactly one stream and one entry, at [`FIXED_TS_NS`].
async fn seed(db: &str) {
    let client = ChClient::new(conn_config(db))
        .await
        .expect("connect data client");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({FIXED_TS_NS}))), \
                 {FINGERPRINT}, '{SERVICE}', \
                 '{{\"service_name\":\"{SERVICE}\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
                 VALUES ('{SERVICE}', {FINGERPRINT}, {FIXED_TS_NS}, 0, 'irrelevant body')"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");
}

/// Runs the one query whose whole output is a template-rendered local time
/// and returns the single rendered line.
fn rendered_line(port: u16) -> String {
    let query = format!(r#"{{service_name="{SERVICE}"}} | line_format "{{{{ __timestamp__ }}}}""#);
    let start = FIXED_TS_NS - 3_600_000_000_000;
    let end = FIXED_TS_NS + 3_600_000_000_000;
    let path = format!(
        "/api/logs/v1/query_range?query={}&start={start}&end={end}&limit=10&direction=forward",
        urlencode(&query)
    );
    let (status, body) = http_get(port, &path).expect("query_range reachable");
    assert_eq!(status, 200, "query_range status (body: {body})");
    let json: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("invalid JSON: {e}\nbody: {body}"));
    let streams = json["data"]["result"]
        .as_array()
        .unwrap_or_else(|| panic!("no result array in {body}"));
    assert_eq!(streams.len(), 1, "expected one stream, got {body}");
    let values = streams[0]["values"]
        .as_array()
        .unwrap_or_else(|| panic!("no values array in {body}"));
    assert_eq!(values.len(), 1, "expected one entry, got {body}");
    values[0][1]
        .as_str()
        .unwrap_or_else(|| panic!("entry line is not a string in {body}"))
        .to_string()
}

#[tokio::test]
async fn a_configured_template_timezone_reaches_the_rendered_query_response() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_template_tz_it");
    let configured_port: u16 = 31_170;
    let default_port: u16 = 31_171;

    drop_db(db).await;
    // Seeded through a server that has already reconciled the schema.
    let configured = spawn_ready(
        configured_port,
        db,
        &[("PULSUS_TEMPLATE_TIMEZONE", "Pacific/Kiritimati")],
    );
    seed(db).await;

    // ---- The seam: a configured zone reaches the wire. ----------------
    let rendered = rendered_line(configured_port);
    assert_eq!(
        rendered, RENDERED_IN_KIRITIMATI,
        "PULSUS_TEMPLATE_TIMEZONE did not reach the rendered response \
         (got {rendered:?}; {RENDERED_IN_UTC:?} means startup never installed the \
         configured zone, any other value means it installed the wrong one)"
    );

    // ---- The control: an identical server, minus that one setting …
    //      and with the $TZ channel POPULATED. -------------------------
    //
    // Same binary, same database, same rows, same query — so the only
    // difference between the two answers is the configuration.
    //
    // `$TZ` is deliberately set here, and this is load-bearing rather than
    // decorative. A read of the host zone can only change an answer when
    // the host channel it reads is non-empty; `$TZ` is unset on this host
    // and on a stock CI runner, so with it unset a planted
    // `env::var("TZ")` read falls through to the configured value and this
    // whole suite ships GREEN on a real defect (measured — the review's
    // round-2 table). Populating the channel is what stops that class
    // being invisible, and it is the same failure shape as a golden that
    // cannot see an ambient value because generator and comparator both
    // read it.
    let default_server = spawn_ready(default_port, db, &[("TZ", HOSTILE_TZ_FOR_DEFAULT)]);
    let rendered_default = rendered_line(default_port);
    assert_eq!(
        rendered_default, RENDERED_IN_UTC,
        "an unconfigured server must render the documented default, UTC, even with \
         $TZ={HOSTILE_TZ_FOR_DEFAULT} in its environment (a leak would render +1300 +13)"
    );
    assert_ne!(
        rendered, rendered_default,
        "the fixture must be zone-sensitive, or the first assertion proves nothing"
    );

    // ---- Two nodes, one configuration, one answer — the second also
    //      under a hostile $TZ, and one that CONTRADICTS its config. ----
    //
    // The zone here is on the other side of the date line from the
    // configured one, so a leak is unmistakable: `-0900 AKST` on the
    // previous day cannot be mistaken for `+1400 +14` by an off-by-hours
    // or DST bug, only by a genuine host read.
    let second_configured_port: u16 = 31_172;
    let second_configured = spawn_ready(
        second_configured_port,
        db,
        &[
            ("PULSUS_TEMPLATE_TIMEZONE", "Pacific/Kiritimati"),
            ("TZ", HOSTILE_TZ_FOR_CONFIGURED),
        ],
    );
    assert_eq!(
        rendered_line(second_configured_port),
        rendered,
        "two nodes carrying the same configuration must render identically — and \
         $TZ={HOSTILE_TZ_FOR_CONFIGURED} must not override it (a leak would render -0900 AKST \
         on the 14th)"
    );

    drop(second_configured);
    drop(default_server);
    drop(configured);
}
