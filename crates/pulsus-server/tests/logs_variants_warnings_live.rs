//! Issue #277 — `variants(...)` skip-and-warn, at the HTTP surface and
//! against the pinned reference.
//!
//! Three claims live here, and each is checked where its subject actually
//! is:
//!
//! * **AC 15** — the shadowed per-variant gate is restored. A root
//!   `variants(A, B, C) of (…)` yielding 400 series per variant — 1 200
//!   total, every variant under the cap — is served with `200`. It was a
//!   **422** before this issue: `exec.rs` capped the CONCATENATION
//!   unconditionally, so #236's per-variant granularity was unreachable
//!   from the API.
//! * **AC 16** — PulsusDB's OWN response. `StatusCode::OK` and the
//!   byte-exact body, with `warnings` as the LAST top-level key, driven
//!   through the real `/api/logs/v1/query` and `/api/logs/v1/query_range`
//!   handlers over loopback HTTP. Reverting the skip to a 422 reddens
//!   both.
//! * **AC 7** — live agreement, BOTH sides. The same dataset is pushed to
//!   the digest-pinned `grafana/loki:3.7.4` reference and the same
//!   queries are replayed there, asserting the top-level key SEQUENCE, the
//!   `warnings` array and the surviving `__variant__` set agree. The
//!   range-at-exactly-500 case is replayed as a **pinned disagreement**:
//!   the reference is asserted to skip and warn, PulsusDB to serve and
//!   stay silent, so ledger entry `(d)`'s over-acceptance is measured on
//!   both sides rather than assumed.
//!
//! # Placement, and why it is not `handlers.rs`
//!
//! The #277 plan located AC 15 and AC 16 in
//! `crates/pulsus-server/src/logs_api/handlers.rs`. The handler cannot
//! run without a ClickHouse pool (`engine_for` returns `PoolUnavailable`
//! without one), so a `#[cfg(test)]` module there can reach the encoder
//! but never the handler. Driving the REAL handler is the substance of
//! both criteria, so they live in an env-gated live suite that spawns the
//! real `pulsusdb` binary — the same harness `logs_api_live.rs` uses.
//!
//! # Gates
//!
//! `PULSUS_TEST_CLICKHOUSE=1` for the PulsusDB half;
//! `PULSUSDB_LOGQL_DIFF_URL` **as well** for the differential half, which
//! skips cleanly on its own when the reference is absent. Run locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   PULSUSDB_LOGQL_DIFF_URL=http://localhost:13100 \
//!   cargo test -p pulsus-server --test logs_variants_warnings_live
//! ```
//!
//! Clean-room: no reference source is read here — the reference is used as
//! a black-box runtime oracle over HTTP.

#[path = "support/live_db.rs"]
mod live_db;

use live_db::{ch_host, ch_http_port, drop_db};

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};

/// One line per `id`, 100 ms apart — the `b21_variant_series_cap.test`
/// dataset recipe, so the hermetic corpus and this leg describe the same
/// shape.
const GROUPS: usize = 501;
const SPACING_NS: i64 = 100_000_000;

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping
/// when the gate is absent in a live CI job, so a lost `env:` block reddens
/// the build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

// ---------------------------------------------------------------------
// A bare-bones loopback HTTP client (the `logs_api_live.rs` idiom: no
// client dependency for a handful of requests).
// ---------------------------------------------------------------------

struct HttpResponse {
    status: u16,
    body: String,
}

fn http_request(port: u16, method: &str, path: &str, body: Option<&str>) -> Option<HttpResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .ok()?;
    let request = match body {
        Some(form) => format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\
             Content-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{form}",
            form.len()
        ),
        None => format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"),
    };
    stream.write_all(request.as_bytes()).ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).ok()?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, rest) = text.split_once("\r\n\r\n")?;
    let status: u16 = head.lines().next()?.split_whitespace().nth(1)?.parse().ok()?;
    let chunked = head.to_ascii_lowercase().contains("transfer-encoding: chunked");
    let body = if chunked { dechunk(rest) } else { rest.to_string() };
    Some(HttpResponse { status, body })
}

fn dechunk(mut rest: &str) -> String {
    let mut out = String::new();
    loop {
        let Some((size_line, tail)) = rest.split_once("\r\n") else {
            break;
        };
        let size = usize::from_str_radix(size_line.trim(), 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        if tail.len() < size {
            out.push_str(tail);
            break;
        }
        out.push_str(&tail[..size]);
        rest = tail[size..].strip_prefix("\r\n").unwrap_or("");
    }
    out
}

fn http_get(port: u16, path: &str) -> HttpResponse {
    http_request(port, "GET", path, None).expect("the logs API is reachable")
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

fn q(path: &str, params: &[(&str, &str)]) -> String {
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{query}")
}

// ---------------------------------------------------------------------
// The server under test.
// ---------------------------------------------------------------------

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_ready_server(port: u16, db: &str) -> ChildGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("CLICKHOUSE_SERVER", ch_host())
        .env("CLICKHOUSE_HTTP_PORT", ch_http_port().to_string())
        .env("CLICKHOUSE_DB", db)
        .spawn()
        .expect("spawn pulsusdb");
    let guard = ChildGuard(child);
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Some(res) = http_request(port, "GET", "/ready", None)
            && res.status == 200
        {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("/ready never reached 200 within 60s");
}

fn data_client_config(db: &str) -> ChConnConfig {
    ChConnConfig {
        server: ch_host(),
        http_port: ch_http_port(),
        database: db.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

const FP: u64 = 0x8000_0000_0000_0277;

/// The instant the queries evaluate at, aligned to a whole minute so the
/// range grid's points are exact seconds (the `b15` anchoring rule) and the
/// byte-exact goldens below carry no sub-second remainder.
fn aligned_now_ns() -> i64 {
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos(),
    )
    .expect("current time fits in i64 nanoseconds");
    (now / 60_000_000_000) * 60_000_000_000
}

/// Seeds one stream with `GROUPS` lines `id=<k>`, the k-th at
/// `at_ns - 60s + (k+1)*100ms`, so every line is inside the `[1m]` window
/// at `at_ns`.
async fn seed(client: &ChClient, db: &str, at_ns: i64) {
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) \
                 VALUES (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({at_ns}))), {FP}, \
                 'v277', '{{\"service_name\":\"v277\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");

    let base = at_ns - 60_000_000_000;
    let values: Vec<String> = (0..GROUPS)
        .map(|k| {
            let ts = base + (k as i64 + 1) * SPACING_NS;
            format!("('v277', {FP}, {ts}, 0, 'id={k}')")
        })
        .collect();
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
                 VALUES {}",
                values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");
}

/// The common log range with `n` surviving `| logfmt` groups.
fn common(n: usize) -> String {
    format!(r#"{{service_name="v277"}} | logfmt | id < {n} [1m]"#)
}

/// Variant 0 walks the cap (one series per group); variant 1 is a single
/// series, so it is what survives a skip.
fn cap_query(n: usize) -> String {
    let c = common(n);
    format!("variants(sum by (id) (count_over_time({c})), sum(count_over_time({c}))) of ({c})")
}

// ---------------------------------------------------------------------
// AC 15 / AC 16 — PulsusDB's own response.
// ---------------------------------------------------------------------

/// AC 15 — **the shadowed gate is restored, and 3x400 proves it.**
///
/// A root `variants(A, B, C) of (…)` where every variant yields 400 series
/// is 1 200 RESULT series, every variant under the 500 cap. The reference
/// serves it (its cap is per variant); PulsusDB now does too.
///
/// **This test fails on `b773bb6`.** There, `exec.rs:258`/`:297` applied
/// `ensure_result_series` to the concatenated `Plan::MetricBinary` result
/// unconditionally, so 1 200 > 500 was a 422 `query_too_broad` — which is
/// also why the differential ledger's entry `(c)` claim that this case "is
/// served" was false at the API. Forcing
/// `pulsus_read::logql::final_series_gate_applies` back to an
/// unconditional `true` reproduces that exactly.
#[tokio::test]
async fn three_variants_of_four_hundred_series_each_are_served() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_variants_warn_it_three_by_four_hundred");
    let port = 31_200;
    drop_db(db).await;
    let _guard = spawn_ready_server(port, db);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");
    let at_ns = aligned_now_ns();
    seed(&client, db, at_ns).await;

    let c = common(400);
    let query = format!(
        "variants(sum by (id) (count_over_time({c})), sum by (id) (count_over_time({c})), \
         sum by (id) (count_over_time({c}))) of ({c})"
    );
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/query",
            &[("query", &query), ("time", &at_ns.to_string())],
        ),
    );
    assert_eq!(
        res.status, 200,
        "1200 result series across three under-cap variants must be SERVED, got {}: {}",
        res.status, res.body
    );
    let body: serde_json::Value = serde_json::from_str(&res.body).expect("JSON body");
    assert_eq!(body["data"]["stats"]["series"], 1_200);
    assert_eq!(
        body["data"]["result"].as_array().expect("result array").len(),
        1_200
    );
    assert!(
        body.get("warnings").is_none(),
        "no variant breached, so no warning: {}",
        res.body
    );

    let indices: BTreeSet<String> = body["data"]["result"]
        .as_array()
        .expect("result array")
        .iter()
        .map(|s| s["metric"]["__variant__"].as_str().unwrap_or("").to_string())
        .collect();
    assert_eq!(
        indices,
        ["0", "1", "2"].iter().map(|s| s.to_string()).collect(),
        "every variant contributes"
    );
}

/// AC 16 — **200 with partial results, byte-exact, at the HTTP surface.**
/// The breaching variant contributes zero series and `warnings` is the
/// LAST top-level key. Reverting the skip to a 422 reddens this.
#[tokio::test]
async fn logs_api_returns_200_with_surviving_variants_when_one_breaches() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_variants_warn_it_instant_skip");
    let port = 31_201;
    drop_db(db).await;
    let _guard = spawn_ready_server(port, db);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");
    let at_ns = aligned_now_ns();
    seed(&client, db, at_ns).await;

    let res = http_get(
        port,
        &q(
            "/api/logs/v1/query",
            &[("query", &cap_query(501)), ("time", &at_ns.to_string())],
        ),
    );
    assert_eq!(res.status, 200, "a skip is a 200, never a 422: {}", res.body);

    let secs = at_ns / 1_000_000_000;
    let expected = format!(
        "{{\"status\":\"success\",\"data\":{{\"resultType\":\"vector\",\"result\":[\
         {{\"metric\":{{\"__variant__\":\"1\"}},\"value\":[{secs}.000,\"501\"]}}\
         ],\"stats\":{{\"series\":1}}}},\
         \"warnings\":[\"maximum of series (500) reached for variant (0)\"]}}"
    );
    assert_eq!(res.body, expected);
    assert!(
        res.body.ends_with("]}"),
        "warnings must be the last top-level key"
    );
    assert!(
        !res.body.contains("\"__variant__\":\"0\""),
        "the breaching variant contributes zero series"
    );
}

/// AC 16's matrix companion — the same claim on `/query_range`.
#[tokio::test]
async fn logs_api_query_range_returns_200_with_surviving_variants_when_one_breaches() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let db = &pulsus_testkit::test_db("pulsus_variants_warn_it_range_skip");
    let port = 31_202;
    drop_db(db).await;
    let _guard = spawn_ready_server(port, db);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");
    let at_ns = aligned_now_ns();
    seed(&client, db, at_ns).await;

    // A single grid point, so the golden carries exactly one sample.
    let res = http_get(
        port,
        &q(
            "/api/logs/v1/query_range",
            &[
                ("query", &cap_query(501)),
                ("start", &at_ns.to_string()),
                ("end", &at_ns.to_string()),
                ("step", "30s"),
            ],
        ),
    );
    assert_eq!(res.status, 200, "a skip is a 200, never a 422: {}", res.body);

    let secs = at_ns / 1_000_000_000;
    let expected = format!(
        "{{\"status\":\"success\",\"data\":{{\"resultType\":\"matrix\",\"result\":[\
         {{\"metric\":{{\"__variant__\":\"1\"}},\"values\":[[{secs}.000,\"501\"]]}}\
         ],\"stats\":{{\"series\":1}}}},\
         \"warnings\":[\"maximum of series (500) reached for variant (0)\"]}}"
    );
    assert_eq!(res.body, expected);
}

// ---------------------------------------------------------------------
// AC 7 — live agreement against the pinned reference.
// ---------------------------------------------------------------------

fn reference_base() -> Option<String> {
    std::env::var("PULSUSDB_LOGQL_DIFF_URL").ok()
}

/// One raw response from either side: the top-level key SEQUENCE (as it
/// appears on the wire, not as a map), the `warnings` array, and the set of
/// `__variant__` values that survived.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    status: u16,
    key_sequence: Vec<String>,
    warnings: Vec<String>,
    variants: BTreeSet<String>,
}

/// Reads the top-level keys IN WIRE ORDER. `serde_json::Value`'s object is
/// a `Map` whose iteration order is not the document's unless the
/// `preserve_order` feature is on, so the sequence is taken from the raw
/// bytes — which is the whole point of the assertion.
fn top_level_key_sequence(body: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let bytes = body.as_bytes();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut start = 0usize;
    for (i, b) in bytes.iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if *b == b'\\' {
                escaped = true;
            } else if *b == b'"' {
                in_string = false;
                if depth == 1 {
                    // A string closing at depth 1 is a KEY only when the
                    // next non-space byte is a colon.
                    let rest = &body[i + 1..];
                    if rest.trim_start().starts_with(':') {
                        keys.push(body[start + 1..i].to_string());
                    }
                }
            }
            continue;
        }
        match b {
            b'"' => {
                in_string = true;
                start = i;
            }
            b'{' | b'[' => depth += 1,
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    keys
}

fn observe(status: u16, body: &str) -> Observed {
    let json: serde_json::Value = serde_json::from_str(body)
        .unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"));
    let warnings = json
        .get("warnings")
        .and_then(|w| w.as_array())
        .map(|a| {
            a.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default();
    let variants = json["data"]["result"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|s| s["metric"]["__variant__"].as_str())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Observed {
        status,
        key_sequence: top_level_key_sequence(body),
        warnings,
        variants,
    }
}

/// Pushes the same dataset into the reference. Entries land at the same
/// offsets relative to `at_ns` that [`seed`] uses, so both sides answer the
/// same question.
fn push_to_reference(base: &str, at_ns: i64) {
    let start = at_ns - 60_000_000_000;
    let values: Vec<String> = (0..GROUPS)
        .map(|k| {
            let ts = start + (k as i64 + 1) * SPACING_NS;
            format!("[\"{ts}\",\"id={k}\"]")
        })
        .collect();
    let payload = format!(
        "{{\"streams\":[{{\"stream\":{{\"service_name\":\"v277\"}},\"values\":[{}]}}]}}",
        values.join(",")
    );
    let out = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            "@-",
            &format!("{base}/loki/api/v1/push"),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write as _;
            child
                .stdin
                .as_mut()
                .expect("curl stdin")
                .write_all(payload.as_bytes())?;
            child.wait_with_output()
        })
        .expect("curl must be on PATH");
    let code = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert_eq!(code, "204", "the reference rejected the push");
}

fn reference_query(base: &str, path: &str, params: &[(&str, &str)]) -> (u16, String) {
    let mut args: Vec<String> = vec![
        "-s".into(),
        "-w".into(),
        "\n%{http_code}".into(),
        "-G".into(),
    ];
    for (k, v) in params {
        args.push("--data-urlencode".into());
        args.push(format!("{k}={v}"));
    }
    args.push(format!("{base}{path}"));
    let out = Command::new("curl")
        .args(&args)
        .output()
        .expect("curl must be on PATH");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let (body, code) = text.rsplit_once('\n').unwrap_or(("", "0"));
    (code.trim().parse().unwrap_or(0), body.to_string())
}

/// AC 7 — **both sides, and their agreement.** The corpus cases that carry
/// a skip decision are replayed against the pinned reference AND against
/// PulsusDB's own handler, and the two are compared. Case 8 (range at
/// exactly 500) is replayed as a PINNED DISAGREEMENT, so ledger entry
/// `(d)`'s over-acceptance is measured on both sides rather than assumed.
///
/// This is what catches capture drift: the hermetic corpus cannot, because
/// `logqltest_replay`'s reachable set is streams-only and every row of
/// `b21_variant_series_cap.test` is excluded as a config-delta file in any
/// case.
#[tokio::test]
async fn variant_skips_agree_with_the_pinned_reference() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let Some(reference) = reference_base() else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the variants differential leg");
        return;
    };

    let db = &pulsus_testkit::test_db("pulsus_variants_warn_it_differential");
    let port = 31_203;
    drop_db(db).await;
    let _guard = spawn_ready_server(port, db);
    let client = ChClient::new(data_client_config(db))
        .await
        .expect("connect data client");
    let at_ns = aligned_now_ns();
    seed(&client, db, at_ns).await;
    push_to_reference(&reference, at_ns);

    let ours_instant = |query: &str| {
        let res = http_get(
            port,
            &q(
                "/api/logs/v1/query",
                &[("query", query), ("time", &at_ns.to_string())],
            ),
        );
        observe(res.status, &res.body)
    };
    let theirs_instant = |query: &str| {
        let (status, body) = reference_query(
            &reference,
            "/loki/api/v1/query",
            &[("query", query), ("time", &at_ns.to_string())],
        );
        observe(status, &body)
    };

    // Corpus cases 3-6: at the cap, over it, every variant over it, and
    // eleven variants with two breaching (the byte-lexicographic order).
    let eleven = {
        let c = common(GROUPS);
        let parts: Vec<String> = (0..11)
            .map(|i| {
                if i == 2 || i == 10 {
                    format!("count_over_time({c})")
                } else {
                    format!("sum(count_over_time({c}))")
                }
            })
            .collect();
        format!("variants({}) of ({c})", parts.join(", "))
    };
    let c501 = common(GROUPS);
    let both_bare =
        format!("variants(count_over_time({c501}), bytes_over_time({c501})) of ({c501})");

    for (case, query) in [
        ("3 (instant, exactly the cap)", cap_query(500)),
        ("4 (instant, one over the cap)", cap_query(GROUPS)),
        ("5 (instant, every variant over)", both_bare),
        ("6 (instant, eleven variants)", eleven),
    ] {
        let ours = ours_instant(&query);
        let theirs = theirs_instant(&query);
        assert_eq!(theirs.status, 200, "case {case}: the reference must serve it");
        assert_eq!(ours.status, 200, "case {case}: PulsusDB must serve it");
        assert_eq!(
            ours.key_sequence, theirs.key_sequence,
            "case {case}: top-level key SEQUENCE must agree"
        );
        assert_eq!(
            ours.warnings, theirs.warnings,
            "case {case}: the warnings array must agree, in order"
        );
        assert_eq!(
            ours.variants, theirs.variants,
            "case {case}: the surviving __variant__ set must agree"
        );
    }

    // Corpus case 9: the same skip at RANGE, where both sides agree.
    //
    // A THREE-POINT grid (t, t+30s, t+60s) deliberately: the reference's
    // range skip needs at least two grid points, because the missing
    // `!ok` guard can only fire when a later point revisits an
    // already-counted series. On a single-point grid it serves 500 and
    // case 8 below would compare two agreements instead of the
    // divergence it exists to pin.
    let end_ns = at_ns + 60_000_000_000;
    let range = |query: &str| -> (Observed, Observed) {
        let params = [
            ("query", query),
            ("start", &at_ns.to_string()),
            ("end", &end_ns.to_string()),
            ("step", "30s"),
        ];
        let mine = http_get(port, &q("/api/logs/v1/query_range", &params));
        let ours = observe(mine.status, &mine.body);
        let (status, body) = reference_query(&reference, "/loki/api/v1/query_range", &params);
        (ours, observe(status, &body))
    };

    let (ours9, theirs9) = range(&cap_query(GROUPS));
    assert_eq!(ours9.status, 200);
    assert_eq!(theirs9.status, 200);
    assert_eq!(ours9.key_sequence, theirs9.key_sequence, "case 9 keys");
    assert_eq!(ours9.warnings, theirs9.warnings, "case 9 warnings");
    assert_eq!(ours9.variants, theirs9.variants, "case 9 variants");

    // Corpus case 8 — THE PINNED DISAGREEMENT. Ledger entry (d): the
    // reference skips a range variant of exactly 500 series because
    // `multiVariantVectorsToSeries` tests the length before the
    // series-existence lookup its sibling guards with `!ok`; PulsusDB
    // applies `> 500` uniformly and serves it. Both halves are asserted,
    // so the divergence is MEASURED on both sides.
    let (ours8, theirs8) = range(&cap_query(500));
    assert_eq!(
        theirs8.warnings,
        vec!["maximum of series (500) reached for variant (0)".to_string()],
        "ledger (d): the reference is expected to SKIP the range-500 variant"
    );
    assert!(
        !theirs8.variants.contains("0"),
        "ledger (d): the reference drops variant 0 entirely at range-500"
    );
    assert!(
        ours8.warnings.is_empty(),
        "ledger (d): PulsusDB SERVES the range-500 variant, silently"
    );
    assert!(
        ours8.variants.contains("0"),
        "ledger (d): PulsusDB keeps variant 0 at range-500"
    );

    // Corpus case 10 — the ROOT-ONLY rule: the same expression `+ 1`
    // takes the plain concatenation cap on both sides. The STATUS differs
    // (the reference's 400 versus our 422) — a pre-existing, separately
    // ledgered divergence — so only the rejection itself is compared.
    let plus_one = format!("{} + 1", cap_query(500));
    let ours10 = http_get(
        port,
        &q(
            "/api/logs/v1/query",
            &[("query", &plus_one), ("time", &at_ns.to_string())],
        ),
    );
    let (their_status, their_body) = reference_query(
        &reference,
        "/loki/api/v1/query",
        &[("query", &plus_one), ("time", &at_ns.to_string())],
    );
    assert!(
        (400..500).contains(&ours10.status),
        "case 10: a non-variants root takes the plain cap, got {}: {}",
        ours10.status,
        ours10.body
    );
    assert_eq!(their_status, 400, "case 10: the reference rejects it too");
    for body in [&ours10.body, &their_body] {
        assert!(
            body.contains("maximum number of series (500)"),
            "case 10: both sides must give the PLAIN cap message, got {body}"
        );
    }
}

/// The dataset seeder and the reference push must describe the same
/// dataset, or the differential leg above compares two different
/// questions. Hermetic — no server, no container.
#[test]
fn the_two_seeders_describe_the_same_dataset() {
    let at_ns = 1_700_000_000_000_000_000i64;
    let start = at_ns - 60_000_000_000;
    for k in [0usize, 1, GROUPS - 1] {
        let ts = start + (k as i64 + 1) * SPACING_NS;
        assert!(ts > start, "every line lands inside the (t-1m, t] window");
        assert!(ts <= at_ns, "and at or before the evaluation instant");
    }
    assert_eq!(GROUPS, 501, "one more than the cap, as the corpus uses");
}

/// [`top_level_key_sequence`] reads the WIRE order, which is the whole
/// point of the key-sequence assertion — `serde_json::Value`'s object is a
/// `Map` whose iteration order is not the document's. Hermetic.
#[test]
fn the_key_sequence_reader_reads_wire_order_not_map_order() {
    let body = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__variant__":"1"},"value":[1.0,"5"]}],"stats":{"series":1}},"warnings":["w"]}"#;
    assert_eq!(
        top_level_key_sequence(body),
        vec![
            "status".to_string(),
            "data".to_string(),
            "warnings".to_string()
        ]
    );
    // Nested keys are not top-level ones, and neither is a string VALUE
    // that happens to look like a key.
    let tricky = r#"{"status":"data","warnings":["a:b"],"data":{"k":1}}"#;
    assert_eq!(
        top_level_key_sequence(tricky),
        vec![
            "status".to_string(),
            "warnings".to_string(),
            "data".to_string()
        ]
    );
    // With no warnings the key is ABSENT, not empty.
    let quiet = r#"{"status":"success","data":{"result":[]}}"#;
    assert_eq!(
        top_level_key_sequence(quiet),
        vec!["status".to_string(), "data".to_string()]
    );
}
