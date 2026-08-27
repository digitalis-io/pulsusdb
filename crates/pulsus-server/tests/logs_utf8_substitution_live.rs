//! Issue #455 — the invalid-UTF-8 substitution, both sides, at the wire.
//!
//! Two changes on one data path, and they compose:
//!
//! 1. the template layer's repair mints one U+FFFD per invalid **byte**
//!    (Go's `utf8.DecodeRune` advance) instead of one per maximal invalid
//!    subsequence (`String::from_utf8_lossy`);
//! 2. the streams response marshaller maps every U+FFFD in a stream
//!    **label** value to one space, which is what
//!    `pkg/util/marshal/query.go:25-32`/`:92-93 @ v3.7.4` does.
//!
//! After both, our `__error_details__` label can never carry a U+FFFD.
//! That is byte-identical to the reference wherever the reference's own
//! slot already held U+FFFD (`bytes`, via `strings.ToLower`), and stays
//! divergent where the reference's slot holds raw invalid bytes
//! (`unixToTime`'s `%v` half) — a type constraint, ledgered under
//! `template-output-budget`.
//!
//! # What is asserted here that nothing else can be
//!
//! The label surface. `logql_template_engine.rs` sees only what the
//! template layer RENDERS; the substitution happens at the encoder, so
//! the value a client reads is visible only over HTTP. Both sides are
//! measured in the same run, off the same seed, and compared.
//!
//! # Gates
//!
//! `PULSUS_TEST_CLICKHOUSE=1` for the PulsusDB half; `PULSUSDB_LOGQL_DIFF_URL`
//! **as well** for the differential half, which skips cleanly on its own
//! when the reference container is absent. Run locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=18123 \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   PULSUSDB_LOGQL_DIFF_URL=http://127.0.0.1:13100 \
//!   cargo test -p pulsus-server --test logs_utf8_substitution_live
//! ```
//!
//! # Where this change does and does not reach, swept rather than assumed
//!
//! **The sweep is over CALLERS, transitively, and it has to be.** An
//! earlier revision of this comment said
//! `space_for_replacement_chars` "has exactly one production call site,
//! `render_stream_item_into`" — true, and the wrong question. That
//! renderer has TWO production callers, and the second is `tail_frame`,
//! so the substitution reached `/api/logs/v1/tail` and its
//! `/loki/api/v1/tail` alias, a route nothing on this issue ever
//! measured. It is now scoped: the renderer takes an explicit
//! `LabelBytes` and the tail frame asks for `Verbatim`, held still by
//! `logs_api::encode`'s `tail_frames_keep_their_stream_label_bytes_verbatim`
//! and recorded in the ledger with the captured frames.
//!
//! ```text
//! space_for_replacement_chars  <- render_stream_item_into (only)
//! render_stream_item_into      <- render_stream_item  -> query_response_warned (substitutes)
//!                              <- tail_frame          -> logs_api::tail        (verbatim)
//! ```
//!
//! The per-byte REPAIR has a wider reach and is deliberately not scoped:
//! `lossy_go{,_len,_into}` is called by `lossy_charged`,
//! `compile_charged_regex`, `UnixToTimeError::{rendered_len,render}` and
//! `Retained::from_engine`, all in the read pipeline before any encoder,
//! so it moves every route that renders a template — measured on tail as
//! well as here, and ledgered.
//!
//! The two neighbouring endpoints were probed against the same container
//! on 2026-08-27 to establish that they are untouched rather than infer
//! it, and both are unreachable for a different reason:
//!
//! * **`/loki/api/v1/detected_labels` with any pipeline stage is
//!   rejected before a label is rendered.** Reference: `400 only label
//!   matchers are supported`; without a stage it answers `200`, so the
//!   probe is not merely hitting a broken endpoint. Ours: `400
//!   unexpected trailing input at byte 12` — a pre-existing message
//!   divergence, not this change's.
//! * **`/loki/api/v1/detected_fields` emits no label VALUE at all**, so
//!   its body is byte-identical before and after. Ours answers `200`
//!   `{"fields":[{"label":"zz","type":"string","cardinality":1,...}]}`
//!   for a `label_format` minting a U+FFFD — the label NAME and nothing
//!   else. The reference answers `500 failed to parse series labels to
//!   categorize labels: 1:62: parse error: invalid UTF-8 rune`, its
//!   Prometheus lexer refusing a well-formed result; we deliberately do
//!   not reproduce that. (The column moves with the query text, so it is
//!   quoted with the query it came from and never alone.)
//!
//! There is no break available through either endpoint, which is the
//! point: nothing here can regress them, and no test claims to guard
//! them.
//!
//! One more measured difference, recorded because it bounds the reach of
//! everything above and is NOT this change's to fix: pushing a stream
//! label containing a U+FFFD is `400 couldn't parse labels: 1:14: parse
//! error: invalid UTF-8 rune` at the reference and `204` here. So the
//! only route to a U+FFFD in a stream label on either side is a query
//! stage that mints one, which is what every case below does.
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

// ---------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------

/// `true` when the PulsusDB half should run. Skips cleanly on a developer
/// machine with no container; **panics** rather than skipping when the
/// gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn reference_base() -> Option<String> {
    std::env::var("PULSUSDB_LOGQL_DIFF_URL").ok()
}

// ---------------------------------------------------------------------
// The committed case inventory (AC-7 mechanism 1)
// ---------------------------------------------------------------------

/// The window a case is sent over: `start = base - h`, `end = base + h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Window {
    /// `h = 15m`.
    M15,
    /// `h = 1h`.
    H1,
}

impl Window {
    fn label(self) -> &'static str {
        match self {
            Window::M15 => "15m",
            Window::H1 => "1h",
        }
    }
    fn half_width_ns(self) -> i64 {
        match self {
            Window::M15 => 900_000_000_000,
            Window::H1 => 3_600_000_000_000,
        }
    }
}

/// Every `(query id, window)` pair the collision/ordering differential is
/// required to execute.
///
/// This is the mechanism, not the prose: the test collects the pairs it
/// ACTUALLY sent and asserts set equality against this list, naming any
/// pair that is missing. Deleting a case from [`ORDERING_CASES`] reports
/// `MISSING CASE <id>/<window>` rather than passing with less coverage —
/// which is exactly the weakening a one-window test slips through.
const REQUIRED_CASES: &[(&str, &str)] = &[
    ("Q9", "15m"),
    ("Q9s", "15m"),
    ("Q10", "15m"),
    ("Q15", "1h"),
    ("Q16", "15m"),
    ("Q16", "1h"),
    ("Q7", "1h"),
];

/// Which pair of streams a case seeds. Fixture `A` is `c1`/`c2`; fixture
/// `B` is `z1`/`a2`, whose names sort the OTHER way round from the one
/// holding three lines — without a second fixture the object order is
/// indistinguishable from insertion order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fixture {
    /// `c1` (three lines) and `c2` (one line).
    A,
    /// `z1` (three lines) and `a2` (one line).
    B,
    /// The case seeds nothing — it is rejected before any data is read.
    None,
}

impl Fixture {
    /// `(three-line stream, one-line stream)`.
    fn streams(self) -> Option<(&'static str, &'static str)> {
        match self {
            Fixture::A => Some(("c1", "c2")),
            Fixture::B => Some(("z1", "a2")),
            Fixture::None => None,
        }
    }
}

struct OrderingCase {
    id: &'static str,
    window: Window,
    query: &'static str,
    fixture: Fixture,
    /// This case's OWN offset from the shared anchor, in nanoseconds.
    ///
    /// Every case derives its base as `anchor + base_offset_ns` and seeds
    /// at that base, so no two cases can share a fixture handle without
    /// the distinct-base count collapsing (AC-7 mechanism 2). 100 ms
    /// apart, so no two cases' entries ever share a timestamp and the
    /// backward order inside an object is forced.
    base_offset_ns: i64,
}

// Every query below embeds the replacement character as the Rust escape
// `\u{FFFD}` rather than as a literal, so this source file stays ASCII: a
// literal U+FFFD pasted into query text was silently flattened twice while
// this issue was being planned, and a flattened query is a different query.

/// `{app=~"c1|c2"}`: `c1` gets the literal space, `c2` the U+FFFD.
const Q9: &str = concat!(
    "{app=~\"c1|c2\"} | label_format k=`{{ if eq .app \"c1\" }} {{ else }}{{ \"",
    "\u{FFFD}",
    "\" }}{{ end }}` | drop app, service_name, detected_level"
);
/// Q9 with the two branches swapped — the check that object order follows
/// the PRE-substitution label set.
const Q9S: &str = concat!(
    "{app=~\"c1|c2\"} | label_format k=`{{ if eq .app \"c1\" }}{{ \"",
    "\u{FFFD}",
    "\" }}{{ else }} {{ end }}` | drop app, service_name, detected_level"
);
/// The control: no U+FFFD anywhere, so the two objects cannot collide.
/// Without it Q9 passes on an implementation that merges everything.
const Q10: &str = "{app=~\"c1|c2\"} | label_format k=`{{ if eq .app \"c1\" }}Q{{ else }} {{ end }}` \
                   | drop app, service_name, detected_level";
/// Q9's shape over the second fixture.
const Q16: &str = concat!(
    "{app=~\"z1|a2\"} | label_format k=`{{ if eq .app \"z1\" }} {{ else }}{{ \"",
    "\u{FFFD}",
    "\" }}{{ end }}` | drop app, service_name, detected_level"
);
/// The opening backtick is never closed. **The selector is load-bearing**:
/// the pinned byte offsets move with the query text — the same shape with
/// `{app="foo"}` gives `byte 26` / `col 27` — so the query and the two
/// bodies are pinned together, never an offset alone.
const Q7: &str = "{app=\"c1\"} | line_format `{{ unixToTime \"\\xe0\" }}";

const ORDERING_CASES: &[OrderingCase] = &[
    OrderingCase {
        id: "Q9",
        window: Window::M15,
        query: Q9,
        fixture: Fixture::A,
        base_offset_ns: 0,
    },
    OrderingCase {
        id: "Q9s",
        window: Window::M15,
        query: Q9S,
        fixture: Fixture::A,
        base_offset_ns: 100_000_000,
    },
    OrderingCase {
        id: "Q10",
        window: Window::M15,
        query: Q10,
        fixture: Fixture::A,
        base_offset_ns: 200_000_000,
    },
    OrderingCase {
        id: "Q15",
        window: Window::H1,
        query: Q9,
        fixture: Fixture::A,
        base_offset_ns: 300_000_000,
    },
    OrderingCase {
        id: "Q16",
        window: Window::M15,
        query: Q16,
        fixture: Fixture::B,
        base_offset_ns: 400_000_000,
    },
    OrderingCase {
        id: "Q16",
        window: Window::H1,
        query: Q16,
        fixture: Fixture::B,
        base_offset_ns: 500_000_000,
    },
    OrderingCase {
        id: "Q7",
        window: Window::H1,
        query: Q7,
        fixture: Fixture::None,
        base_offset_ns: 600_000_000,
    },
];

// ---------------------------------------------------------------------
// A bare-bones loopback HTTP client (the `logs_api_live.rs` idiom).
// ---------------------------------------------------------------------

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

fn http_request(port: u16, method: &str, path: &str, body: Option<&[u8]>) -> Option<HttpResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .ok()?;
    let mut request = match body {
        Some(payload) => format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            payload.len()
        )
        .into_bytes(),
        None => format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .into_bytes(),
    };
    if let Some(payload) = body {
        request.extend_from_slice(payload);
    }
    stream.write_all(&request).ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).ok()?;
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let rest = raw[split + 4..].to_vec();
    let status: u16 = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    let chunked = head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked");
    let body = if chunked { dechunk(&rest) } else { rest };
    Some(HttpResponse { status, body })
}

/// Bodies here can hold bytes that are not valid UTF-8 (the reference's
/// `%v` half), so de-chunking works on BYTES — decoding first would
/// destroy the very thing this suite measures.
fn dechunk(mut rest: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let Some(eol) = rest.windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let Ok(size_line) = std::str::from_utf8(&rest[..eol]) else {
            break;
        };
        let Ok(size) = usize::from_str_radix(size_line.trim(), 16) else {
            break;
        };
        if size == 0 {
            break;
        }
        let body = &rest[eol + 2..];
        if body.len() < size {
            out.extend_from_slice(body);
            break;
        }
        out.extend_from_slice(&body[..size]);
        rest = body[size..].strip_prefix(b"\r\n").unwrap_or(b"");
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

/// `GET /loki/api/v1/query_range` with the pinned parameter set, against
/// either side. The route and the window are the two things a stale
/// capture is missing, so both are built here and nowhere else.
fn query_range_path(query: &str, base_ns: i64, window: Window) -> String {
    let h = window.half_width_ns();
    format!(
        "/loki/api/v1/query_range?query={}&start={}&end={}&direction=backward&limit=100",
        urlencode(query),
        base_ns - h,
        base_ns + h
    )
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
        // `/loki/api/v1/push` and `/loki/api/v1/query_range` are compat
        // aliases, and using them on BOTH sides is what makes the seed
        // and the request byte-identical across the differential.
        .env("PULSUS_COMPAT_ENDPOINTS", "1")
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
    panic!("/ready never reached 200 within 60s (port {port}, db {db})");
}

// ---------------------------------------------------------------------
// Both sides, one shape.
// ---------------------------------------------------------------------

/// Where a request goes. Named rather than a boolean so a call site
/// cannot silently send our query to the reference.
#[derive(Debug, Clone, Copy)]
enum Side<'a> {
    Pulsus { port: u16 },
    Reference { base: &'a str },
}

fn reference_curl(base: &str, path: &str, body: Option<&[u8]>) -> HttpResponse {
    let url = format!("{base}{path}");
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-o", "-", "-w", "\n%{http_code}"]);
    if let Some(payload) = body {
        cmd.args([
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            "@-",
            "-X",
            "POST",
        ]);
        cmd.stdin(std::process::Stdio::piped());
        cmd.arg(&url);
        let mut child = cmd
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("curl must be on PATH");
        child
            .stdin
            .as_mut()
            .expect("curl stdin")
            .write_all(payload)
            .expect("write push body");
        let out = child.wait_with_output().expect("curl finished");
        return split_curl_output(out.stdout);
    }
    cmd.arg(&url);
    let out = cmd.output().expect("curl must be on PATH");
    split_curl_output(out.stdout)
}

/// `curl -w '\n%{http_code}'` appends the status after a newline; the body
/// before it is returned as raw BYTES.
fn split_curl_output(mut raw: Vec<u8>) -> HttpResponse {
    let nl = raw
        .iter()
        .rposition(|b| *b == b'\n')
        .expect("curl always writes the status after a newline");
    let code = String::from_utf8_lossy(&raw[nl + 1..]).trim().to_string();
    raw.truncate(nl);
    HttpResponse {
        status: code.parse().unwrap_or(0),
        body: raw,
    }
}

fn get(side: Side<'_>, path: &str) -> HttpResponse {
    match side {
        Side::Pulsus { port } => {
            http_request(port, "GET", path, None).expect("the logs API is reachable")
        }
        Side::Reference { base } => reference_curl(base, path, None),
    }
}

fn push(side: Side<'_>, payload: &str) {
    let res = match side {
        Side::Pulsus { port } => {
            http_request(port, "POST", "/loki/api/v1/push", Some(payload.as_bytes()))
                .expect("the push endpoint is reachable")
        }
        Side::Reference { base } => {
            reference_curl(base, "/loki/api/v1/push", Some(payload.as_bytes()))
        }
    };
    assert_eq!(
        res.status,
        204,
        "{side:?} rejected the push: {}",
        String::from_utf8_lossy(&res.body)
    );
}

// ---------------------------------------------------------------------
// Byte-level extraction — a body may not be valid UTF-8
// ---------------------------------------------------------------------

fn find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    (from..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

/// The label region of a single-stream response (everything before the
/// first `"values":[`) and the entry region (everything after it).
///
/// The same marker text appears in BOTH the `__error_details__` label and,
/// when the query echoes the pair with `line_format`, in the log line —
/// so an extractor that took "the first match" would silently answer a
/// different question for `Q3` than for `Q1`.
fn label_and_entry_regions(body: &[u8]) -> (&[u8], &[u8]) {
    let at = find(body, b"\"values\":[", 0).expect("a streams response carries a values array");
    (&body[..at], &body[at..])
}

/// The bytes between `after` and the next `until`, searched inside
/// `region`. Panics naming the marker, because a missing marker means the
/// response changed shape and "empty" would read as a value.
fn span(region: &[u8], after: &[u8], until: &[u8], what: &str) -> Vec<u8> {
    let start = find(region, after, 0).unwrap_or_else(|| {
        panic!(
            "{what}: marker {:?} absent from {}",
            String::from_utf8_lossy(after),
            String::from_utf8_lossy(region)
        )
    }) + after.len();
    let end = find(region, until, start).unwrap_or_else(|| {
        panic!(
            "{what}: terminator {:?} absent after the marker",
            String::from_utf8_lossy(until)
        )
    });
    region[start..end].to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The scalar values a span DECODES to, which is what a reader sees.
///
/// `\uXXXX` escapes are resolved (the reference writes the `unixToTime`
/// entry echo that way and we write it as raw UTF-8; the bytes differ and
/// the reading does not), and remaining bytes are decoded with **Go's**
/// per-byte lossy rule so a raw invalid byte counts as one U+FFFD — the
/// same granularity the repair under test uses.
fn decoded(mut raw: &[u8]) -> Vec<char> {
    let mut out = Vec::new();
    while !raw.is_empty() {
        if raw[0] == b'\\' && raw.len() >= 2 {
            match raw[1] {
                b'u' if raw.len() >= 6 => {
                    if let Ok(text) = std::str::from_utf8(&raw[2..6])
                        && let Ok(cp) = u32::from_str_radix(text, 16)
                    {
                        out.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                        raw = &raw[6..];
                        continue;
                    }
                }
                b'n' | b't' | b'r' | b'"' | b'\\' | b'/' | b'b' | b'f' => {
                    out.push(match raw[1] {
                        b'n' => '\n',
                        b't' => '\t',
                        b'r' => '\r',
                        b'b' => '\u{8}',
                        b'f' => '\u{C}',
                        other => other as char,
                    });
                    raw = &raw[2..];
                    continue;
                }
                _ => {}
            }
        }
        match std::str::from_utf8(raw) {
            Ok(text) => {
                out.extend(text.chars());
                break;
            }
            Err(e) => {
                let valid =
                    std::str::from_utf8(&raw[..e.valid_up_to()]).expect("validated by valid_up_to");
                out.extend(valid.chars());
                out.push('\u{FFFD}');
                raw = &raw[e.valid_up_to() + 1..];
            }
        }
    }
    out
}

// ---------------------------------------------------------------------
// Seeds
// ---------------------------------------------------------------------

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos(),
    )
    .expect("current time fits in i64 nanoseconds")
}

/// The shared anchor for the collision/ordering cases: the most recent
/// half-past-the-hour that is safely in the past.
///
/// **Placement, not a boundary constant.** A `+/-15m` window around a
/// half-past mark lies inside ONE wall-clock hour, which is where the
/// reference's shipped `split_queries_by_interval` (`1h`, read from its
/// `/config` and quoted in the `streams-split-merge` ledger entry) puts
/// its split boundaries. This test pins no `h -> splits` mapping — the
/// boundary is absolute-time aligned and such a mapping fails on the
/// clock. It asserts the `splits` the reference REPORTS for each window
/// it actually sent, and if that disagrees the message names the
/// placement as the thing to look at.
fn anchor_ns() -> i64 {
    const HOUR: i64 = 3_600_000_000_000;
    const HALF: i64 = 1_800_000_000_000;
    let now = now_ns();
    let mut hour = (now / HOUR) * HOUR;
    if hour + HALF > now - 60_000_000_000 {
        hour -= HOUR;
    }
    hour + HALF
}

/// A per-run, per-CASE tag mixed into every seeded line.
///
/// Two jobs, and both are load-bearing. The reference container is SHARED
/// across CI legs and outlives a single run, so a selector this suite uses
/// may already carry another run's lines: the RUN half makes every
/// comparison a statement about the data this run seeded, rather than an
/// emptiness precondition that a re-run breaks. And every case seeds its
/// own base inside the same window, so the CASE half is what lets a case
/// assert ITS OWN four lines instead of the union of every case's.
fn case_tag(anchor: i64, case_index: usize) -> String {
    format!("r{}c{case_index}", anchor % 1_000_000_007)
}

/// The four-entry fixture: the three-line stream at `base`, `base+10ns`,
/// `base+11ns`; the one-line stream at `base+1ns`.
fn fixture_payload(fixture: Fixture, base: i64, tag: &str) -> Option<String> {
    let (big, small) = fixture.streams()?;
    Some(format!(
        "{{\"streams\":[\
           {{\"stream\":{{\"app\":\"{big}\"}},\"values\":[\
             [\"{}\",\"line-from-{big}-{tag}\"],\
             [\"{}\",\"{big}-extra-a-{tag}\"],\
             [\"{}\",\"{big}-extra-b-{tag}\"]]}},\
           {{\"stream\":{{\"app\":\"{small}\"}},\"values\":[\
             [\"{}\",\"line-from-{small}-{tag}\"]]}}]}}",
        base,
        base + 10,
        base + 11,
        base + 1
    ))
}

/// Polls until `needle` is visible over `selector` on `side`, so a query
/// never reads a store that has not caught up. Bounded; panics naming the
/// side, because "not yet visible" and "never arrives" look identical to a
/// single-shot read.
fn wait_for_line(side: Side<'_>, selector: &str, base: i64, needle: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let path = query_range_path(selector, base, Window::M15);
    let mut last = 0u16;
    while Instant::now() < deadline {
        let res = get(side, &path);
        last = res.status;
        if res.status == 200 && find(&res.body, needle.as_bytes(), 0).is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("{side:?}: {needle:?} never became visible over {selector} (last status {last})");
}

// ---------------------------------------------------------------------
// Q1-Q8 — the error-detail label, both sides
// ---------------------------------------------------------------------

/// The headline surface: what a client reads in `__error_details__`.
///
/// Every expected value was captured in the SAME run from both sides,
/// which is the defence against the failure this issue kept producing —
/// an expected answer that quietly stopped matching its own fixture.
#[tokio::test(flavor = "multi_thread")]
async fn error_detail_labels_agree_with_the_reference_where_they_can() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let Some(reference) = reference_base() else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the substitution differential");
        return;
    };
    let db = &pulsus_testkit::test_db("pulsus_utf8_subst_it_labels");
    let port = 31_206;
    drop_db(db).await;
    let _guard = spawn_ready_server(port, db);
    let ours = Side::Pulsus { port };
    let theirs = Side::Reference { base: &reference };

    let base = now_ns();
    let seed = format!(
        "{{\"streams\":[{{\"stream\":{{\"app\":\"foo\"}},\"values\":[[\"{base}\",\"hello world\"]]}}]}}"
    );
    push(ours, &seed);
    push(theirs, &seed);
    wait_for_line(ours, "{app=\"foo\"}", base, "hello world");
    wait_for_line(theirs, "{app=\"foo\"}", base, "hello world");

    let ask = |side: Side<'_>, query: &str| -> HttpResponse {
        get(side, &query_range_path(query, base, Window::H1))
    };

    // -- Q1: the reference's own slot already held U+FFFD, so both sides
    // -- answer with two spaces. The headline parity row.
    let q1 = "{app=\"foo\"} | line_format `{{ bytes \"12\\xe0\\xa0\" }}`";
    for (name, res) in [("ours", ask(ours, q1)), ("reference", ask(theirs, q1))] {
        assert_eq!(res.status, 200, "Q1 {name}");
        let (labels, _) = label_and_entry_regions(&res.body);
        assert_eq!(
            hex(&span(labels, b"unhandled size name: ", b"\"", "Q1 label")),
            "2020",
            "Q1 {name}: one space per invalid BYTE, at the label"
        );
    }

    // -- Q2: the reference's slot holds RAW bytes; ours cannot. The
    // -- divergence, at its post-change value.
    let q2 = "{app=\"foo\"} | line_format `{{ unixToTime \"\\xe0\\xa0\" }}`";
    let q2_ours = ask(ours, q2);
    let q2_theirs = ask(theirs, q2);
    assert_eq!((q2_ours.status, q2_theirs.status), (200, 200), "Q2 status");
    let ours_span = span(
        label_and_entry_regions(&q2_ours.body).0,
        b"unable to parse time '",
        b"':",
        "Q2 ours",
    );
    let theirs_span = span(
        label_and_entry_regions(&q2_theirs.body).0,
        b"unable to parse time '",
        b"':",
        "Q2 reference",
    );
    assert_eq!(hex(&ours_span), "2020", "Q2 ours");
    assert_eq!(hex(&theirs_span), "e0a0", "Q2 reference");
    assert_eq!(
        (decoded(&ours_span).len(), decoded(&theirs_span).len()),
        (2, 2),
        "Q2: both sides carry TWO runes — the character count matches even \
         though the bytes do not"
    );
    assert!(
        std::str::from_utf8(&q2_ours.body).is_ok(),
        "Q2: our whole response is valid UTF-8"
    );
    assert!(
        std::str::from_utf8(&q2_theirs.body).is_err(),
        "Q2: the reference's whole response is NOT valid UTF-8 — which is \
         the thing our type cannot reproduce"
    );

    // -- Q3: the shape Loki's own troubleshooting page publishes. The
    // -- ENTRY echo is decoded-equal; the LABEL is the Q2 divergence.
    let q3 = "{app=\"foo\"} | line_format `{{ unixToTime \"\\xe0\\xa0\" }}` \
              | line_format `Error: {{.__error__}} - {{.__error_details__}}`";
    let q3_ours = ask(ours, q3);
    let q3_theirs = ask(theirs, q3);
    assert_eq!((q3_ours.status, q3_theirs.status), (200, 200), "Q3 status");
    let echo = |body: &[u8], what: &str| {
        span(
            label_and_entry_regions(body).1,
            b"unable to parse time '",
            b"':",
            what,
        )
    };
    let e_ours = echo(&q3_ours.body, "Q3 ours entry");
    let e_theirs = echo(&q3_theirs.body, "Q3 reference entry");
    assert_eq!(
        hex(&e_ours),
        "efbfbdefbfbd",
        "Q3 ours: two U+FFFD, one per invalid byte"
    );
    assert_eq!(
        decoded(&e_ours),
        decoded(&e_theirs),
        "Q3: the entry echoes are DECODED-equal ({} against {}) — the \
         reference writes them as ASCII `\\ufffd` escapes and we write raw \
         UTF-8, which is escaping, not a value difference",
        hex(&e_ours),
        hex(&e_theirs)
    );
    assert_eq!(decoded(&e_ours), vec!['\u{FFFD}', '\u{FFFD}'], "Q3 entry");
    assert_eq!(
        hex(&span(
            label_and_entry_regions(&q3_ours.body).0,
            b"unable to parse time '",
            b"':",
            "Q3 ours label"
        )),
        "2020",
        "Q3 ours label"
    );
    assert_eq!(
        hex(&span(
            label_and_entry_regions(&q3_theirs.body).0,
            b"unable to parse time '",
            b"':",
            "Q3 reference label"
        )),
        "e0a0",
        "Q3 reference label"
    );

    // -- Q4: byte-identical to the reference on BOTH surfaces. Reverting
    // -- either half of the change reddens this, and reverting the repair
    // -- rule reddens it twice.
    let q4 = "{app=\"foo\"} | line_format `{{ bytes \"12\\xe0\\xa0\" }}` \
              | line_format `Error: {{.__error__}} - {{.__error_details__}}`";
    for (name, res) in [("ours", ask(ours, q4)), ("reference", ask(theirs, q4))] {
        assert_eq!(res.status, 200, "Q4 {name}");
        let (labels, entries) = label_and_entry_regions(&res.body);
        assert_eq!(
            hex(&span(entries, b"unhandled size name: ", b"\"", "Q4 entry")),
            "efbfbdefbfbd",
            "Q4 {name}: the ENTRY keeps both U+FFFD"
        );
        assert_eq!(
            hex(&span(labels, b"unhandled size name: ", b"\"", "Q4 label")),
            "2020",
            "Q4 {name}: the LABEL carries two spaces"
        );
    }

    // -- Q5: the negative that stops the substitution over-reaching. A fix
    // -- that maps U+FFFD in ENTRIES makes this `78 20 79`.
    let q5 = concat!("{app=\"foo\"} | line_format `x{{ \"", "\u{FFFD}", "\" }}y`");
    for (name, res) in [("ours", ask(ours, q5)), ("reference", ask(theirs, q5))] {
        assert_eq!(res.status, 200, "Q5 {name}");
        let (_, entries) = label_and_entry_regions(&res.body);
        assert!(
            find(entries, b"x\xef\xbf\xbdy", 0).is_some(),
            "Q5 {name}: the log LINE keeps its U+FFFD: {}",
            String::from_utf8_lossy(entries)
        );
        assert!(
            find(entries, b"x y", 0).is_none(),
            "Q5 {name}: entries are NOT substituted"
        );
    }

    // -- Q6: an ORDINARY label. The rule is not special to the error pair.
    let q6 = concat!(
        "{app=\"foo\"} | label_format foo=`{{ \"",
        "\u{FFFD}",
        "\" }}`"
    );
    for (name, res) in [("ours", ask(ours, q6)), ("reference", ask(theirs, q6))] {
        assert_eq!(res.status, 200, "Q6 {name}");
        let (labels, _) = label_and_entry_regions(&res.body);
        assert_eq!(
            hex(&span(labels, b"\"foo\":\"", b"\"", "Q6 label")),
            "20",
            "Q6 {name}: an ordinary label value is substituted too"
        );
    }

    // -- Q8: `ff ff` mints two U+FFFD under BOTH repair rules, so this
    // -- input cannot discriminate the rule — it is here to show the
    // -- substitution fires however the U+FFFD arose.
    let q8 = "{app=\"foo\"} | line_format `{{ unixToTime (b64dec \"//8=\") }}`";
    let q8_ours = ask(ours, q8);
    let q8_theirs = ask(theirs, q8);
    assert_eq!((q8_ours.status, q8_theirs.status), (200, 200), "Q8 status");
    assert_eq!(
        hex(&span(
            label_and_entry_regions(&q8_ours.body).0,
            b"unable to parse time '",
            b"':",
            "Q8 ours"
        )),
        "2020",
        "Q8 ours"
    );
    assert_eq!(
        hex(&span(
            label_and_entry_regions(&q8_theirs.body).0,
            b"unable to parse time '",
            b"':",
            "Q8 reference"
        )),
        "ffff",
        "Q8 reference"
    );

    // The whole-of-surface statement the rows above add up to: after this
    // change no `__error_details__` we serve carries a U+FFFD at all.
    for (name, query) in [("Q1", q1), ("Q2", q2), ("Q3", q3), ("Q4", q4), ("Q8", q8)] {
        let res = ask(ours, query);
        let (labels, _) = label_and_entry_regions(&res.body);
        assert!(
            find(labels, "\u{FFFD}".as_bytes(), 0).is_none(),
            "{name}: no U+FFFD survives in any label we serve: {}",
            String::from_utf8_lossy(labels)
        );
    }
}

// ---------------------------------------------------------------------
// Q9 / Q9s / Q10 / Q15 / Q16 / Q7 — collisions, ordering, and the split
// ---------------------------------------------------------------------

/// One side's answer to one ordering case, reduced to what the claims are
/// about: the objects IN WIRE ORDER, each with its `k` value as hex and
/// its lines, plus the reference's own `splits` accounting.
#[derive(Debug)]
struct Objects {
    objects: Vec<(String, Vec<String>)>,
    splits: Option<u64>,
    carries_splits_key: bool,
}

fn observe_objects(body: &[u8], what: &str) -> Objects {
    let text = std::str::from_utf8(body)
        .unwrap_or_else(|e| panic!("{what}: body is not valid UTF-8 ({e})"));
    let json: serde_json::Value =
        serde_json::from_str(text).unwrap_or_else(|e| panic!("{what}: invalid JSON ({e}): {text}"));
    let objects = json["data"]["result"]
        .as_array()
        .unwrap_or_else(|| panic!("{what}: no result array: {text}"))
        .iter()
        .map(|s| {
            let k = s["stream"]["k"].as_str().unwrap_or_default();
            let lines = s["values"]
                .as_array()
                .map(|vs| {
                    vs.iter()
                        .map(|v| v[1].as_str().unwrap_or_default().to_string())
                        .collect()
                })
                .unwrap_or_default();
            (hex(k.as_bytes()), lines)
        })
        .collect();
    Objects {
        objects,
        splits: json["data"]["stats"]["summary"]["splits"].as_u64(),
        carries_splits_key: text.contains("\"splits\""),
    }
}

/// The lines of `object` that THIS run seeded, in wire order.
fn mine(object: &(String, Vec<String>), tag: &str) -> Vec<String> {
    object
        .1
        .iter()
        .filter(|l| l.ends_with(tag))
        .cloned()
        .collect()
}

/// A collision, its swap, its control, the split divergence, a second
/// ordering fixture, and a rejection that must stay one — every case with
/// its own seed base, and the inventory checked by set equality.
///
/// **What this catches about PLACEMENT, established by breaking it rather
/// than by reading.** Moving the substitution to the GROUPING key
/// (`push_fanout_entry`'s `render_labels_json_sorted`) reddens Q9 here
/// with `left: 1  right: 2` — the two colliding streams merge into one
/// object and the seam interleaves. Moving it merely above
/// `query_response_warned`'s `(labels_json, fingerprint)` sort does
/// **not** redden anything here: a fan-out group's fingerprint is
/// `fnv1a64` of its own pre-substitution `labels_json`
/// (`logql/detected_probe.rs:85`), so the tiebreak still carries what the
/// key lost and Q9/Q9s keep reversing. That break is caught by
/// `logs_api::encode`'s
/// `colliding_stream_labels_order_by_the_pre_substitution_label_set`,
/// which chooses fingerprints that contradict the label order — and
/// nowhere else.
#[tokio::test(flavor = "multi_thread")]
async fn colliding_streams_and_the_split_divergence_agree_with_the_reference() {
    if !should_run() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1 (see module docs)");
        return;
    }
    let Some(reference) = reference_base() else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the substitution differential");
        return;
    };
    let db = &pulsus_testkit::test_db("pulsus_utf8_subst_it_ordering");
    let port = 31_207;
    drop_db(db).await;
    let _guard = spawn_ready_server(port, db);
    let ours = Side::Pulsus { port };
    let theirs = Side::Reference { base: &reference };

    let anchor = anchor_ns();

    // Every case seeds at its OWN base, under its OWN tag. Two cases
    // routed through one fixture handle would share a base, and the
    // assertion at the end of this test names the duplicate.
    let mut bases: Vec<(String, i64)> = Vec::new();
    for (idx, case) in ORDERING_CASES.iter().enumerate() {
        let base = anchor + case.base_offset_ns;
        bases.push((format!("{}/{}", case.id, case.window.label()), base));
        if let Some(payload) = fixture_payload(case.fixture, base, &case_tag(anchor, idx)) {
            push(ours, &payload);
            push(theirs, &payload);
        }
    }
    for (fixture, selector) in [
        (Fixture::A, "{app=~\"c1|c2\"}"),
        (Fixture::B, "{app=~\"z1|a2\"}"),
    ] {
        let (big, _) = fixture.streams().expect("A and B both seed");
        let (last_idx, last_base) = ORDERING_CASES
            .iter()
            .enumerate()
            .filter(|(_, c)| c.fixture == fixture)
            .map(|(i, c)| (i, anchor + c.base_offset_ns))
            .next_back()
            .expect("each fixture has at least one case");
        let needle = format!("{big}-extra-b-{}", case_tag(anchor, last_idx));
        wait_for_line(ours, selector, last_base, &needle);
        wait_for_line(theirs, selector, last_base, &needle);
    }

    let mut executed: BTreeSet<(String, String)> = BTreeSet::new();
    let mut saw_unsplit = false;
    let mut saw_split = false;

    for (idx, case) in ORDERING_CASES.iter().enumerate() {
        let base = anchor + case.base_offset_ns;
        let tag = case_tag(anchor, idx);
        let path = query_range_path(case.query, base, case.window);
        let mine_ours = get(ours, &path);
        let mine_theirs = get(theirs, &path);
        let id = format!("{}/{}", case.id, case.window.label());
        executed.insert((case.id.to_string(), case.window.label().to_string()));

        if case.fixture == Fixture::None {
            // Q7 — a rejection that must stay a rejection. A change to
            // string handling is exactly the kind that quietly turns one
            // into an acceptance. The offsets are pinned WITH the query,
            // never alone: the same shape with `{app="foo"}` gives
            // `byte 26` / `col 27`.
            assert_eq!(mine_ours.status, 400, "{id} ours must reject");
            assert_eq!(
                String::from_utf8_lossy(&mine_ours.body),
                "unterminated string starting at byte 25",
                "{id} ours body"
            );
            assert_eq!(mine_theirs.status, 400, "{id} the reference must reject");
            let theirs_body = String::from_utf8_lossy(&mine_theirs.body).into_owned();
            assert!(
                theirs_body.contains("parse error at line 1, col 26: literal not terminated"),
                "{id} reference body: {theirs_body}"
            );
            continue;
        }

        assert_eq!(mine_ours.status, 200, "{id} ours");
        assert_eq!(mine_theirs.status, 200, "{id} reference");
        let ours_obs = observe_objects(&mine_ours.body, &format!("{id} ours"));
        let theirs_obs = observe_objects(&mine_theirs.body, &format!("{id} reference"));

        // Our structural fact, and the one AC-15 records: we have no
        // split step, so our response reports no `splits` field at all.
        assert!(
            !ours_obs.carries_splits_key,
            "{id}: our read path has no split step and must report no splits field"
        );
        let splits = theirs_obs
            .splits
            .unwrap_or_else(|| panic!("{id}: the reference reports no splits accounting"));

        // Ours never merges, at either window.
        assert_eq!(
            ours_obs.objects.len(),
            2,
            "{id} ours: two objects at every window — {:?}",
            ours_obs.objects
        );

        let (big, small) = case.fixture.streams().expect("seeded");
        let big_first = format!("line-from-{big}-{tag}");
        let small_only = format!("line-from-{small}-{tag}");
        // This case's own three big-stream lines, newest first.
        let big_backward = vec![
            format!("{big}-extra-b-{tag}"),
            format!("{big}-extra-a-{tag}"),
            big_first.clone(),
        ];

        match case.window {
            Window::M15 => {
                assert_eq!(
                    splits, 0,
                    "{id}: this window was placed inside one wall-clock hour and the \
                     reference still split it into {splits} — fix the placement in \
                     `anchor_ns`, not the expectation (the boundary is absolute-time \
                     aligned; see the `streams-split-merge` ledger entry)"
                );
                saw_unsplit = true;
                assert_eq!(
                    theirs_obs.objects.len(),
                    2,
                    "{id} reference: below its split boundary it emits BOTH colliding \
                     objects — {:?}",
                    theirs_obs.objects
                );
                // Both sides, object for object: the same `k` bytes and the
                // same lines from this run. This is the parity claim.
                let reduce = |o: &Objects| -> Vec<(String, Vec<String>)> {
                    o.objects
                        .iter()
                        .map(|obj| (obj.0.clone(), mine(obj, &tag)))
                        .collect()
                };
                assert_eq!(
                    reduce(&ours_obs),
                    reduce(&theirs_obs),
                    "{id}: the two sides must agree object for object"
                );

                // And the absolute expectations, so agreement on a wrong
                // answer cannot pass.
                let ks: Vec<&str> = ours_obs.objects.iter().map(|o| o.0.as_str()).collect();
                let ours_lines: Vec<Vec<String>> =
                    ours_obs.objects.iter().map(|o| mine(o, &tag)).collect();
                if case.id == "Q10" {
                    // The control: no U+FFFD, so no collision. The
                    // literal-space object sorts first, `Q` second.
                    assert_eq!(ks, vec!["20", "51"], "{id}: k values");
                    assert_eq!(
                        ours_lines[0],
                        vec![small_only.clone()],
                        "{id}: first object"
                    );
                    assert_eq!(ours_lines[1], big_backward, "{id}: second object");
                } else {
                    assert_eq!(
                        ks,
                        vec!["20", "20"],
                        "{id}: both objects are labelled with a SPACE after substitution"
                    );
                    // Q9 and Q16 give the space to the three-line stream,
                    // Q9s to the one-line stream. Whichever holds the
                    // literal space sorts FIRST, because the ordering key
                    // is the PRE-substitution label set.
                    let (first, second) = if case.id == "Q9s" {
                        (vec![small_only.clone()], big_backward.clone())
                    } else {
                        (big_backward.clone(), vec![small_only.clone()])
                    };
                    assert_eq!(ours_lines[0], first, "{id}: first object");
                    assert_eq!(ours_lines[1], second, "{id}: second object");
                }
            }
            Window::H1 => {
                assert!(
                    splits > 0,
                    "{id}: a window this wide crosses a split boundary wherever it is \
                     placed; the reference reported splits={splits}"
                );
                saw_split = true;
                // The ledgered divergence: above its boundary the
                // reference MERGES the collision and we do not.
                assert_eq!(
                    theirs_obs.objects.len(),
                    1,
                    "{id} reference: above its split boundary it merges the collision — {:?}",
                    theirs_obs.objects
                );
                assert_eq!(theirs_obs.objects[0].0, "20", "{id} reference: merged k");
                assert!(
                    ours_obs.objects.iter().all(|o| o.0 == "20"),
                    "{id} ours: both objects carry a space — {:?}",
                    ours_obs.objects
                );
                // Nothing is LOST by not merging: the same lines are
                // served, spread over two objects instead of one.
                let theirs_lines: BTreeSet<String> =
                    mine(&theirs_obs.objects[0], &tag).into_iter().collect();
                let ours_union: BTreeSet<String> = ours_obs
                    .objects
                    .iter()
                    .flat_map(|o| mine(o, &tag))
                    .collect();
                assert_eq!(
                    ours_union, theirs_lines,
                    "{id}: the same entries reach the client on both sides"
                );
                let expected: BTreeSet<String> = big_backward
                    .iter()
                    .cloned()
                    .chain([small_only.clone()])
                    .collect();
                assert_eq!(ours_union, expected, "{id}: this case's own four lines");
            }
        }
    }

    // -- AC-7 mechanism 1: the committed inventory, by SET EQUALITY -----
    let required: BTreeSet<(String, String)> = REQUIRED_CASES
        .iter()
        .map(|(q, w)| (q.to_string(), w.to_string()))
        .collect();
    let missing: Vec<String> = required
        .difference(&executed)
        .map(|(q, w)| format!("MISSING CASE {q}/{w}"))
        .collect();
    let extra: Vec<String> = executed
        .difference(&required)
        .map(|(q, w)| format!("UNDECLARED CASE {q}/{w}"))
        .collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "the executed case set must equal the committed inventory:\n{}\n{}",
        missing.join("\n"),
        extra.join("\n")
    );

    // -- AC-7 mechanism 2: every case derived its OWN seed base ---------
    let distinct: BTreeSet<i64> = bases.iter().map(|(_, b)| *b).collect();
    assert_eq!(
        distinct.len(),
        bases.len(),
        "every case must derive its own seed base, or two cases are reading one \
         fixture and neither is checking what it claims — bases: {bases:?}"
    );

    // Both branches of the reference's own inconsistency were exercised.
    // Without this a run in which every window happened to split would
    // assert the divergence twice and never the parity.
    assert!(
        saw_unsplit && saw_split,
        "both the reference's unsplit branch (splits=0, two objects) and its split \
         branch (merged) must be exercised — unsplit={saw_unsplit} split={saw_split}"
    );

    eprintln!(
        "#455 ordering differential: {} cases, anchor={anchor}, bases={bases:?}",
        ORDERING_CASES.len()
    );
}

// ---------------------------------------------------------------------
// The ledger rows, field by field (hermetic)
// ---------------------------------------------------------------------

fn workspace_root() -> std::path::PathBuf {
    // crates/pulsus-server -> crates -> <root>
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

/// The body of one `### \`<key>\`` section of the differential ledger,
/// with every run of whitespace collapsed to a single space so a needle
/// never has to know where the Markdown happened to wrap.
fn ledger_section(key: &str) -> String {
    let ledger = std::fs::read_to_string(
        workspace_root().join("docs/benchmarks/logs-differential-ledger.md"),
    )
    .expect("ledger readable");
    let heading = format!("### `{key}`");
    let start = ledger
        .find(&heading)
        .unwrap_or_else(|| panic!("the ledger has no `{key}` entry"));
    let rest = &ledger[start + heading.len()..];
    let end = rest.find("\n### ").unwrap_or(rest.len());
    rest[..end].split_whitespace().collect::<Vec<_>>().join(" ")
}

/// One of the six conditions a recorded measurement needs before anyone
/// can re-measure it.
///
/// **This enum is the fix for a class, not for a row.** Three times on
/// this issue a ledger row carried a number without what produced it: a
/// missing ROUTE cost three rounds on #294, a missing WINDOW cost a round
/// here, and a missing ANCHOR — the tail row's fixed timestamp and line —
/// cost this one. Each time the row named its values and each time the
/// guard checked exactly those, so it passed. Requiring the *conditions*
/// as their own named category, for every row, is what closes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Condition {
    /// The HTTP route the capture was taken on.
    Route,
    /// The request's time bounds, relative to the row's anchor.
    Window,
    /// How the anchor instant itself is chosen — load-bearing whenever a
    /// captured byte depends on where in wall-clock time it falls.
    Anchor,
    /// What was pushed: the stream, the line, and the sample timestamps.
    Seed,
    /// The complete query text, not the fragment the table's cells show.
    Query,
    /// Which reference build the other side of the comparison was.
    Digest,
}

/// Every condition a row must answer for. A row cannot answer for fewer:
/// the guard requires each of these to appear exactly once in the row's
/// list, so a new row cannot quietly omit the one that happens to be
/// awkward.
const ALL_CONDITIONS: &[Condition] = &[
    Condition::Route,
    Condition::Window,
    Condition::Anchor,
    Condition::Seed,
    Condition::Query,
    Condition::Digest,
];

/// What a row promises about one condition.
///
/// Both arms carry literals the guard looks for **inside the row's own
/// table span**, never anywhere in the section — see [`table_span`].
#[derive(Debug, Clone, Copy)]
enum Given {
    /// The row states it. EVERY literal must be present: a table that
    /// sends two queries names both, and deleting either is a table that
    /// no longer describes what was measured.
    Stated(&'static [&'static str]),
    /// The condition genuinely cannot apply, and the row SAYS SO in the
    /// ledger — this literal is that sentence.
    ///
    /// It is checked twice: it must appear in the table's span, so
    /// editing the prose to say the opposite reddens; and it must be a
    /// reason, so a word like `forgotten` or `n/a` is refused on the
    /// `DELIBERATELY_UNWIRED` precedent. A code-local string alone was
    /// the second false-green here — it let the ledger assert the
    /// OPPOSITE of the exemption while the guard stayed green.
    NotApplicable(&'static str),
}

/// One measurement table in the differential ledger, with the conditions
/// that reproduce it and the values it records.
///
/// A section can hold more than one table — `template-output-budget`
/// holds a `query_range` one and a `/tail` one, taken on different
/// routes, with different anchors and different queries — so `row` names
/// which, and each carries its own conditions.
///
/// **`row` is a LOCATOR, not a label.** The ledger opens each table with
/// ``**Table `<row>`.**`` and the guard searches only from there to the
/// next such marker. An earlier revision of this file kept `row` in the
/// panic text alone and searched the whole section by `key`, which let an
/// invented table name pass and let a literal deleted from its own table
/// be found in a neighbouring one — the same blind spot as the flat list
/// it replaced, one layer along.
struct LedgerRow {
    key: &'static str,
    row: &'static str,
    conditions: &'static [(Condition, Given)],
    values: &'static [(&'static str, &'static str)],
}

/// Reasons that are not reasons.
const INADMISSIBLE: &[&str] = &["forgotten", "n/a", "na", "none", "tbd", "unknown", "-"];

/// **The AC-13 helper, widened to conditions.** Asserts a ledger table
/// carries every condition that reproduces it AND every value it claims,
/// and reports **which** are missing — never a single boolean, because a
/// row containing the key and a couple of numbers satisfies a
/// `contains(key)` check while carrying none of the things that make it
/// re-measurable.
fn assert_ledger_row(r: &LedgerRow) {
    let problems = check_ledger_row(r);
    assert!(
        problems.is_empty(),
        "the `{}` ledger entry's {} table has {} problem(s):\n{}",
        r.key,
        r.row,
        problems.len(),
        problems.join("\n")
    );
}

/// [`assert_ledger_row`]'s detection, split out so the guard's OWN
/// failure modes can be exercised without a panic — a `catch_unwind`
/// around them would print three panics into a green run, and around
/// anything that reaches `require_live_gate` it would re-open the #320
/// silent pass. The one line `assert_ledger_row` adds on top is covered
/// by deleting a condition from the shipped ledger and watching the real
/// test go red.
/// The span of `section` belonging to the table `row` names: from its
/// ``**Table `<row>`.**`` marker to the next `**Table ` marker, or the
/// end of the section.
///
/// `None` when the marker is absent, which is how an invented table name
/// fails instead of silently matching the whole section.
fn table_span<'a>(section: &'a str, row: &str) -> Option<&'a str> {
    let marker = format!("**Table `{row}`.");
    let start = section.find(&marker)?;
    let rest = &section[start + marker.len()..];
    let end = rest.find("**Table `").unwrap_or(rest.len());
    Some(&rest[..end])
}

fn check_ledger_row(r: &LedgerRow) -> Vec<String> {
    let section = ledger_section(r.key);
    let mut problems: Vec<String> = Vec::new();

    // 0. The table has to exist. Everything below searches ITS span, so
    //    without this the guard would answer questions about a table the
    //    ledger does not contain.
    let Some(span) = table_span(&section, r.row) else {
        return vec![format!(
            "  NO SUCH TABLE {:?}: the `{}` entry has no ``**Table `{}`.**`` marker \
             (markers present: {:?})",
            r.row,
            r.key,
            r.row,
            section
                .match_indices("**Table `")
                .map(|(i, _)| section[i + 9..]
                    .split('`')
                    .next()
                    .unwrap_or("<unterminated>")
                    .to_string())
                .collect::<Vec<_>>()
        )];
    };

    // 1. Every condition answered for, exactly once. A row that simply
    //    leaves one out is the defect this whole mechanism exists for, so
    //    it is a failure and not a silent pass.
    for want in ALL_CONDITIONS {
        let n = r.conditions.iter().filter(|(c, _)| c == want).count();
        if n != 1 {
            problems.push(format!(
                "  UNANSWERED CONDITION {want:?}: named {n} times, must be exactly once"
            ));
        }
    }

    // 2. Each stated condition present IN THIS TABLE'S SPAN; each
    //    exemption both stated in the ledger and an actual reason.
    for (c, given) in r.conditions {
        match given {
            Given::Stated(needles) => {
                for needle in *needles {
                    if !span.contains(needle) {
                        problems.push(format!("  MISSING CONDITION {c:?}: expected {needle:?}"));
                    }
                }
            }
            Given::NotApplicable(why) => {
                let w = why.trim().to_ascii_lowercase();
                if w.len() < 20 || INADMISSIBLE.contains(&w.as_str()) {
                    problems.push(format!(
                        "  INADMISSIBLE EXEMPTION {c:?}: {why:?} is not a reason"
                    ));
                }
                if !span.contains(why) {
                    problems.push(format!(
                        "  UNSTATED EXEMPTION {c:?}: the ledger must carry the reason \
                         verbatim, expected {why:?}"
                    ));
                }
            }
        }
    }

    // 3. Each recorded value present IN THIS TABLE'S SPAN.
    for (field, needle) in r.values {
        if !span.contains(needle) {
            problems.push(format!("  MISSING VALUE {field}: expected {needle:?}"));
        }
    }

    problems
}

const LEDGER_ROWS: &[LedgerRow] = &[
    LedgerRow {
        key: "template-output-budget",
        row: "template-output-budget/query_range",
        conditions: &[
            (
                Condition::Route,
                Given::Stated(&["/loki/api/v1/query_range"]),
            ),
            (Condition::Window, Given::Stated(&["start = NS - 1h"])),
            (
                Condition::Anchor,
                // Written down rather than left out: the exemption is the
                // place a defect hides, so the row has to say WHY the
                // anchor cannot matter here, and it does.
                Given::NotApplicable(
                    "`NS` itself is not a condition for this table: it is simply `now`, no \
                     value here depends on where it falls, and no compared span contains it.",
                ),
            ),
            (
                Condition::Seed,
                Given::Stated(&["ONE line `hello world` under `{app=\"foo\"}` at an instant `NS`"]),
            ),
            (
                Condition::Query,
                Given::Stated(&["is the cell's first column"]),
            ),
            (Condition::Digest, Given::Stated(&["sha256:87f0a067"])),
        ],
        values: &[
            ("the reference's unixToTime label", "`e0a0`"),
            ("the label we serve for it", "`2020`"),
            ("the entry echo both sides carry", "efbfbdefbfbd"),
            ("the type constraint", "a type constraint, not a preference"),
            ("the type that cannot hold the byte", "Cow<'a, str>"),
            (
                "the substitution rule's source",
                "pkg/util/marshal/query.go:25-32",
            ),
            ("the granularity measurement", "utf8.DecodeRune"),
        ],
    },
    LedgerRow {
        key: "template-output-budget",
        row: "template-output-budget/tail",
        conditions: &[
            (Condition::Route, Given::Stated(&["/loki/api/v1/tail"])),
            (Condition::Window, Given::Stated(&["start = NS - 60s"])),
            (
                Condition::Anchor,
                Given::Stated(&["the most recent half-past-the-hour at least 30 s in the past"]),
            ),
            (
                Condition::Seed,
                Given::Stated(&[
                    "ONE line `tailprobe` under `{app=\"tf1\"}`, whose sample timestamp is \
                     exactly `NS`",
                ]),
            ),
            (
                Condition::Query,
                // BOTH, because the table has two rows and each sends its
                // own: naming one let the other be deleted while the
                // guard stayed green.
                Given::Stated(&[
                    "{app=\"tf1\"} | label_format k=`{{ \"\\ufffd\" }}`",
                    "{app=\"tf1\"} | line_format `{{ unixToTime \"\\xe0\\xa0\" }}`",
                ]),
            ),
            (Condition::Digest, Given::Stated(&["sha256:87f0a067"])),
        ],
        values: &[
            ("the scoping", "scoped to the QUERY response"),
            (
                "what the byte counts span",
                "The byte counts are the frame prefix up to `],\"dropped_entries\"`",
            ),
            (
                "the label bytes before",
                "`\"k\":\"efbfbd\"`, frame served, 114 B",
            ),
            ("the label bytes now", "**byte-identical**, 114 B"),
            (
                "the reference's own tail refusal",
                "could not write JSON tail response",
            ),
            (
                "the row the repair moves",
                "`parse time 'efbfbdefbfbd'`, 343 B",
            ),
        ],
    },
    LedgerRow {
        key: "streams-split-merge",
        row: "streams-split-merge/collision",
        conditions: &[
            (
                Condition::Route,
                Given::Stated(&["/loki/api/v1/query_range"]),
            ),
            (Condition::Window, Given::Stated(&["start = NS - 15m"])),
            (
                Condition::Anchor,
                Given::Stated(&["the most recent half-past-the-hour at least 60 s in the past"]),
            ),
            (
                Condition::Seed,
                Given::Stated(&["three lines, at `NS`, `NS+10ns` and `NS+11ns`"]),
            ),
            (
                Condition::Query,
                Given::Stated(&["| drop app, service_name, detected_level"]),
            ),
            (Condition::Digest, Given::Stated(&["sha256:87f0a067"])),
        ],
        values: &[
            ("the wide window", "start = NS - 1h"),
            ("the unsplit object count", "**two objects**"),
            ("the split object count", "**one merged object**"),
            (
                "the internal-inconsistency statement",
                "The reference is internally inconsistent here",
            ),
            (
                "the deliberate-match statement",
                "We match its unsplit branch because that is our structural configuration",
            ),
            ("the /config citation", "split_queries_by_interval: 1h"),
            ("the boundary's alignment", "absolute-time aligned"),
            (
                "the rule that no test may encode the mapping",
                "would fail on the clock",
            ),
            (
                "the two readings that cannot be separated",
                "indistinguishable by construction",
            ),
            ("the withdrawn ordering claim", "That is withdrawn"),
        ],
    },
];

/// Every measurement table this change owns, each asserted for the
/// conditions that reproduce it as well as the values it records.
///
/// **What this test cannot do, stated rather than implied:** it checks
/// that a table names a route, a window, an anchor, a seed, a query, a
/// digest and its values — and it **cannot tell whether the recorded
/// numbers are true**. The live differentials above, and the captured
/// frames in the commit that added the tail row, are what measure those.
/// A checker that appeared to validate the tables would be worse than
/// none, because it would convert "nobody has re-measured this" into
/// "something checks this".
///
/// What it CAN now do, and could not before: refuse a table that records
/// a number without the conditions that produced it. That failure mode
/// shipped three times on this issue.
///
/// Hermetic. It calls the live gate directly because it reaches no gate
/// of its own: without that, `--test logs_utf8_substitution_live` in a
/// live CI job with the `env:` block dropped would still exit 0 (#320).
#[test]
fn both_ledger_rows_carry_every_field_that_makes_them_re_measurable() {
    pulsus_testkit::require_live_gate(pulsus_testkit::CLICKHOUSE_GATE);
    for row in LEDGER_ROWS {
        assert_ledger_row(row);
    }
    eprintln!(
        "#455 ledger: {} tables, {} conditions each, {} values total",
        LEDGER_ROWS.len(),
        ALL_CONDITIONS.len(),
        LEDGER_ROWS.iter().map(|r| r.values.len()).sum::<usize>()
    );
}

/// The guard's own failure modes, on fixtures rather than on the shipped
/// ledger — a checker whose reporting has never been seen fail is the
/// shape this issue keeps producing.
///
/// Six cases. The first three are the ones the guard caught when it was
/// written; **the last three are the false-greens it did not**, and each
/// is here because a review demonstrated it passing:
///
/// * an invented table name matched the whole section and passed;
/// * a literal deleted from its own table was still found in a
///   neighbouring one, so the deletion passed;
/// * a `NotApplicable` reason lived only in this file, so the ledger
///   could be edited to assert the OPPOSITE and the guard stayed green.
///
/// Neither `#[should_panic]` nor `catch_unwind`: both are named in
/// `pulsus_testkit`'s docs as the way a suite absorbs `require_live_gate`
/// and turns #320's guarantee back into a silent pass, and a
/// `catch_unwind` here would also print panics into a green run.
/// [`check_ledger_row`] returns the problems instead.
#[test]
fn the_ledger_guard_reports_an_omitted_condition_and_a_non_reason() {
    pulsus_testkit::require_live_gate(pulsus_testkit::CLICKHOUSE_GATE);

    let complain = |r: &LedgerRow| -> String {
        let problems = check_ledger_row(r);
        assert!(
            !problems.is_empty(),
            "the fixture row {:?} must fail",
            r.row
        );
        problems.join("\n")
    };
    // Fixtures locate a REAL table, so what they exercise is the check
    // under test and not the missing-marker path.
    const QR: &str = "template-output-budget/query_range";
    // 1. A row that names its VALUES and simply leaves a condition out —
    //    what all three shipped instances of this defect looked like.
    let omitted = LedgerRow {
        key: "template-output-budget",
        row: QR,
        conditions: &[
            (
                Condition::Route,
                Given::Stated(&["/loki/api/v1/query_range"]),
            ),
            (Condition::Window, Given::Stated(&["start = NS - 1h"])),
            (Condition::Seed, Given::Stated(&["`hello world`"])),
            (Condition::Query, Given::Stated(&["line_format"])),
            (Condition::Digest, Given::Stated(&["sha256:87f0a067"])),
        ],
        values: &[("a value it does record", "`e0a0`")],
    };
    let msg = complain(&omitted);
    assert!(
        msg.contains("UNANSWERED CONDITION Anchor: named 0 times"),
        "an omitted condition must be named: {msg}"
    );

    // 2. Every condition answered, but one exempted with a word instead
    //    of a reason.
    let excused = LedgerRow {
        key: "template-output-budget",
        row: QR,
        conditions: &[
            (
                Condition::Route,
                Given::Stated(&["/loki/api/v1/query_range"]),
            ),
            (Condition::Window, Given::Stated(&["start = NS - 1h"])),
            (Condition::Anchor, Given::NotApplicable("n/a")),
            (Condition::Seed, Given::Stated(&["`hello world`"])),
            (Condition::Query, Given::Stated(&["line_format"])),
            (Condition::Digest, Given::Stated(&["sha256:87f0a067"])),
        ],
        values: &[],
    };
    let msg = complain(&excused);
    assert!(
        msg.contains("INADMISSIBLE EXEMPTION Anchor"),
        "an exemption that is not a reason must be named: {msg}"
    );

    // 3. A condition claimed but absent from the prose.
    let absent = LedgerRow {
        key: "template-output-budget",
        row: QR,
        conditions: &[
            (
                Condition::Route,
                Given::Stated(&["/loki/api/v1/query_range"]),
            ),
            (Condition::Window, Given::Stated(&["start = NS - 1h"])),
            (
                Condition::Anchor,
                Given::Stated(&["an anchor sentence this entry does not contain"]),
            ),
            (Condition::Seed, Given::Stated(&["`hello world`"])),
            (Condition::Query, Given::Stated(&["line_format"])),
            (Condition::Digest, Given::Stated(&["sha256:87f0a067"])),
        ],
        values: &[],
    };
    let msg = complain(&absent);
    assert!(
        msg.contains("MISSING CONDITION Anchor"),
        "a claimed-but-absent condition must be named: {msg}"
    );

    // 4. FALSE-GREEN 1 — an invented table name. This passed: the check
    //    searched the whole section by `key` and `row` was decoration.
    let invented = LedgerRow {
        key: "template-output-budget",
        row: "a table that does not exist in the ledger",
        conditions: LEDGER_ROWS[1].conditions,
        values: LEDGER_ROWS[1].values,
    };
    let msg = complain(&invented);
    assert!(
        msg.contains("NO SUCH TABLE"),
        "a table name the ledger does not carry must be named: {msg}"
    );
    assert!(
        msg.contains("template-output-budget/tail"),
        "and the message must list the markers that DO exist: {msg}"
    );

    // 5. FALSE-GREEN 2 — a literal that lives in a NEIGHBOURING table of
    //    the same section. `start = NS - 60s` is the tail table's window
    //    and appears nowhere in the query_range table, so a query_range
    //    row claiming it must fail. Whole-section matching passed this,
    //    which is why deleting a query from its own table stayed green.
    let leaked = LedgerRow {
        key: "template-output-budget",
        row: QR,
        conditions: &[
            (
                Condition::Route,
                Given::Stated(&["/loki/api/v1/query_range"]),
            ),
            // the tail table's window, claimed by the query_range table
            (Condition::Window, Given::Stated(&["start = NS - 60s"])),
            (
                Condition::Anchor,
                Given::NotApplicable(
                    "`NS` itself is not a condition for this table: it is simply `now`, no \
                     value here depends on where it falls, and no compared span contains it.",
                ),
            ),
            (Condition::Seed, Given::Stated(&["`hello world`"])),
            (Condition::Query, Given::Stated(&["line_format"])),
            (Condition::Digest, Given::Stated(&["sha256:87f0a067"])),
        ],
        values: &[],
    };
    let msg = complain(&leaked);
    assert!(
        msg.contains("MISSING CONDITION Window") && msg.contains("start = NS - 60s"),
        "a literal belonging to a NEIGHBOURING table must not satisfy this one: {msg}"
    );
    // The same literal IS in the section — which is exactly why the old
    // whole-section check passed it. Asserted, so this fixture cannot
    // quietly become a test of a literal that is simply absent.
    assert!(
        ledger_section("template-output-budget").contains("start = NS - 60s"),
        "the fixture is only meaningful while the tail table still states its window"
    );

    // 6. FALSE-GREEN 3 — an exemption whose reason is not in the ledger.
    //    The reason used to be a code-local string, so the ledger could
    //    assert the opposite and the guard stayed green.
    let unstated = LedgerRow {
        key: "template-output-budget",
        row: QR,
        conditions: &[
            (
                Condition::Route,
                Given::Stated(&["/loki/api/v1/query_range"]),
            ),
            (Condition::Window, Given::Stated(&["start = NS - 1h"])),
            (
                Condition::Anchor,
                Given::NotApplicable(
                    "the anchor cannot matter here, and this sentence is nowhere in the ledger",
                ),
            ),
            (Condition::Seed, Given::Stated(&["`hello world`"])),
            (Condition::Query, Given::Stated(&["line_format"])),
            (Condition::Digest, Given::Stated(&["sha256:87f0a067"])),
        ],
        values: &[],
    };
    let msg = complain(&unstated);
    assert!(
        msg.contains("UNSTATED EXEMPTION Anchor"),
        "an exemption the ledger does not carry must be named: {msg}"
    );
}
