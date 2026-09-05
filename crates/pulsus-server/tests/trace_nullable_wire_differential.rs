//! Issue #474, the oracle leg: the committed capture in
//! `tests/fixtures/trace_nullable_wire/capture.json` is what the pinned
//! reference build actually answers.
//!
//! This leg drives the reference and nothing else. The PulsusDB leg lives
//! in `traces_api_live.rs`, where the raw-socket helpers it needs already
//! are; the two share only this artifact, and **neither compares itself to
//! the other** — both compare to the same committed hex, so a driver that
//! mangled bytes fails its own leg rather than agreeing with the opposite
//! one.
//!
//! # What it does
//!
//! Pushes the capture's `push_body_hex` verbatim to the reference's OTLP
//! HTTP receiver, then replays every probe and asserts byte equality
//! against the capture's own strings. Four probes over
//! `GET /api/traces/{id}` with `Accept: application/protobuf`, plus the
//! two absent-trace `GET /api/v2/traces/{id}` bodies and one populated v2
//! fetch.
//!
//! # What is compared, and what deliberately is not
//!
//! **Compared:** the whole v1 protobuf body for each probe; the whole
//! absent-trace v2 protobuf and JSON bodies; and, for the populated v2
//! fetch, field 1's contents only.
//!
//! **Not compared:** the populated v2 response's field 2 (`metrics`). Its
//! counter is not stable between two fetches of the same trace and moves
//! in plateaus rather than per request, so two adjacent fetches agreeing
//! proves nothing about it and there is no value to freeze. Ledgered as
//! `traces-v2-fetch-metrics-not-populated`. Response headers other than
//! the status line are not compared either — the reference's `Vary` is
//! `Accept-Encoding` only, which says nothing about the bytes this issue
//! is about.
//!
//! Gate: skips unless `PULSUSDB_NULLABLE_WIRE_DIFF_URL` (the reference's
//! HTTP API base) and `PULSUSDB_NULLABLE_WIRE_OTLP_URL` (its OTLP HTTP
//! base) are both set; **fail-closed**, so a job that dropped the `env:`
//! block reddens instead of reporting green. No ClickHouse is needed —
//! this leg never touches PulsusDB.
//!
//! ```text
//! podman run -d --name pulsus-tempo-474 -p 13474:3200 -p 14474:4318 \
//!     -v $PWD/ci/tempo/tempo-compare.yaml:/etc/tempo/tempo.yaml:ro \
//!     grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7 \
//!     -config.file=/etc/tempo/tempo.yaml
//! PULSUSDB_NULLABLE_WIRE_DIFF_URL=http://localhost:13474 \
//!   PULSUSDB_NULLABLE_WIRE_OTLP_URL=http://localhost:14474 \
//!   cargo test -p pulsus-server --test trace_nullable_wire_differential -- --nocapture
//! ```
//!
//! Clean-room: no reference source or test corpus is read. The fixture is
//! our own authorship and the reference's answers are read back as
//! black-box runtime output.

#[path = "support/wire_capture.rs"]
mod wire_capture;

use std::io::Write;
use std::process::Command;
use std::time::{Duration, Instant};

/// A raw HTTP response as `curl` reports it: the status code and the body
/// bytes, nothing else.
struct CurlResponse {
    status: u16,
    body: Vec<u8>,
}

/// `curl` the reference. The status is written to a side file with `-w`
/// rather than parsed out of the body, so a body containing digits cannot
/// be mistaken for one.
fn curl(args: &[&str], url: &str, ctx: &str) -> CurlResponse {
    let dir = std::env::temp_dir().join(format!("pulsus474-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let code_path = dir.join("code");
    let body_path = dir.join("body");
    let out = Command::new("curl")
        .args(["-s", "--max-time", "20"])
        .args(args)
        .args(["-o", body_path.to_str().expect("utf8 path")])
        .args(["-w", "%{http_code}"])
        .arg(url)
        .output()
        .expect("curl on PATH");
    std::fs::write(&code_path, &out.stdout).expect("write status");
    let status: u16 = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("{ctx}: curl reported no HTTP status ({e})"));
    let body = std::fs::read(&body_path).unwrap_or_default();
    CurlResponse { status, body }
}

fn push_fixture(otlp_base: &str, body: &[u8]) {
    let dir = std::env::temp_dir().join(format!("pulsus474-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let path = dir.join("push.bin");
    let mut f = std::fs::File::create(&path).expect("create push body");
    f.write_all(body).expect("write push body");
    drop(f);
    let url = format!("{}/v1/traces", otlp_base.trim_end_matches('/'));
    let res = curl(
        &[
            "-X",
            "POST",
            "-H",
            "Content-Type: application/x-protobuf",
            "--data-binary",
            &format!("@{}", path.to_str().expect("utf8 path")),
        ],
        &url,
        "push",
    );
    assert_eq!(
        res.status, 200,
        "OTLP push to {url} failed (http {})",
        res.status
    );
}

/// Fetches `path` until the body is non-empty or the deadline passes — the
/// reference cuts a live-store block a few seconds after a push, so the
/// first fetch after ingest can legitimately be an empty trace.
fn get_when_visible(api_base: &str, path: &str, ctx: &str) -> CurlResponse {
    let url = format!("{}{path}", api_base.trim_end_matches('/'));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let res = curl(&["-H", "Accept: application/protobuf"], &url, ctx);
        if res.status == 200 && !res.body.is_empty() {
            return res;
        }
        assert!(
            Instant::now() < deadline,
            "{ctx}: {url} never returned a non-empty 200 within 60s (last status {})",
            res.status
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[test]
fn the_committed_capture_matches_the_live_reference() {
    // Fail-closed on both endpoint gates (issue #320): with the `env:`
    // block dropped, the `else` arm below would print a skip notice and
    // report GREEN in the very job that exists to run this leg.
    let (Some(api_base), Some(otlp_base)) = (
        pulsus_testkit::live_endpoint("PULSUSDB_NULLABLE_WIRE_DIFF_URL"),
        pulsus_testkit::live_endpoint("PULSUSDB_NULLABLE_WIRE_OTLP_URL"),
    ) else {
        eprintln!(
            "skipping the nullable-submessage oracle leg — set \
             PULSUSDB_NULLABLE_WIRE_DIFF_URL (the reference's HTTP API base) and \
             PULSUSDB_NULLABLE_WIRE_OTLP_URL (its OTLP HTTP base)"
        );
        return;
    };

    let capture = wire_capture::load();
    push_fixture(&otlp_base, &wire_capture::from_hex(&capture.push_body_hex));

    for probe in &capture.probes {
        let ctx = format!("probe {} ({})", probe.name, probe.absent);
        let res = get_when_visible(&api_base, &format!("/api/traces/{}", probe.trace_id), &ctx);
        assert_eq!(res.status, 200, "{ctx}: v1 fetch status");
        assert_eq!(
            wire_capture::to_hex(&res.body),
            probe.v1_protobuf_hex,
            "{ctx}: the reference's v1 protobuf body has drifted from the committed capture"
        );
    }

    // The absent-trace v2 envelope, in both representations. These four
    // and twenty-five bytes are what PulsusDB's own v2 route must emit.
    let absent = format!("/api/v2/traces/{}", capture.absent_trace_id);
    let url = format!("{}{absent}", api_base.trim_end_matches('/'));
    let res = curl(
        &["-H", "Accept: application/protobuf"],
        &url,
        "v2 absent pb",
    );
    assert_eq!(res.status, 200, "v2 absent protobuf: status");
    assert_eq!(
        wire_capture::to_hex(&res.body),
        capture.v2_absent_protobuf_hex,
        "v2 absent protobuf body"
    );
    let res = curl(&["-H", "Accept: application/json"], &url, "v2 absent json");
    assert_eq!(res.status, 200, "v2 absent JSON: status");
    assert_eq!(
        String::from_utf8_lossy(&res.body),
        capture.v2_absent_json,
        "v2 absent JSON body"
    );

    // The populated v2 fetch: field 1's contents only. Field 2 is never
    // compared — see the module doc.
    let probe = capture.probe("T1");
    let expected = probe
        .v2_trace_field_hex
        .as_deref()
        .expect("T1 carries a v2 trace field in the capture");
    let expected_bytes = wire_capture::from_hex(expected);
    let url = format!(
        "{}/api/v2/traces/{}",
        api_base.trim_end_matches('/'),
        probe.trace_id
    );
    let res = curl(
        &["-H", "Accept: application/protobuf"],
        &url,
        "v2 populated",
    );
    assert_eq!(res.status, 200, "v2 populated: status");
    let field1 = v2_trace_field(&res.body);
    assert_eq!(
        wire_capture::to_hex(field1),
        expected,
        "the reference's v2 field-1 contents have drifted from the committed capture"
    );
    assert_eq!(
        field1.len(),
        expected_bytes.len(),
        "v2 field-1 length (a sanity check on the length prefix, not a second comparison)"
    );
}

/// Field 1's contents from a v2 fetch envelope: key `0x0a`, then a varint
/// length, then that many bytes. Deliberately minimal — this is the only
/// protobuf parsing either leg does, and it exists because field 2 must be
/// dropped rather than compared.
fn v2_trace_field(body: &[u8]) -> &[u8] {
    assert_eq!(
        body.first(),
        Some(&0x0au8),
        "a v2 envelope must start with field 1's length-delimited key, got {:?}",
        wire_capture::to_hex(body)
    );
    let mut len = 0usize;
    let mut shift = 0u32;
    let mut i = 1usize;
    loop {
        let b = *body.get(i).expect("truncated field-1 length prefix");
        len |= usize::from(b & 0x7f) << shift;
        i += 1;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    body.get(i..i + len).expect("truncated field-1 contents")
}
