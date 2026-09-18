//! Live re-verification of every committed `cityHash64` vector
//! (`tests/fixtures/fingerprints.json`) against a real ClickHouse server.
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` so plain `cargo test --workspace`
//! stays hermetic (no network/container dependency) — mirrors the gating
//! pattern in `crates/pulsus-clickhouse/tests/live_clickhouse.rs` (issue
//! #3). Asserts against **every** length-class/boundary/non-ASCII vector,
//! not a sampling (issue #4 plan amendment).
//!
//! To run these:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-model --test live_cityhash
//! podman rm -f pulsus-ch-test
//! ```
//!
//! Connection parameters can be overridden via `PULSUS_TEST_CH_HOST` /
//! `PULSUS_TEST_CH_HTTP_PORT` if the default `localhost:19123` does not fit
//! your environment.
//!
//! This file is intentionally excluded from hermetic CI: it only runs when
//! `PULSUS_TEST_CLICKHOUSE=1` is set against a live ClickHouse, which is the
//! case locally (see the `podman run` invocation above) and in the issue #7
//! end-to-end environment.

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, QuerySettings, Row};
use pulsus_model::{Fingerprint, LabelSet, metric_fingerprint, stream_fingerprint};
use serde_json::Value;

const FIXTURES: &str = include_str!("fixtures/fingerprints.json");

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(10),
        ..ChConnConfig::default()
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-model/tests/live_cityhash.rs for setup)"
            );
            return;
        }
    };
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct CityHashRow {
    fingerprint: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct Compose128Row {
    fingerprint: u128,
}

fn hex(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

async fn live_cityhash64(client: &ChClient, buf: &[u8]) -> u64 {
    let sql = format!("SELECT cityHash64(unhex('{}')) AS fingerprint", hex(buf));
    use futures::StreamExt;
    let mut stream = client
        .query_stream::<CityHashRow>(&sql, &QuerySettings::new())
        .await
        .expect("query_stream");
    let row = stream.next().await.expect("one row").expect("row decode");
    row.fingerprint
}

/// The composed 128-bit identity as the server derives it — the expression
/// pinned in `crates/pulsus-model/src/fingerprint.rs`'s module doc.
async fn live_compose128(client: &ChClient, buf: &[u8]) -> u128 {
    let h = hex(buf);
    let sql = format!(
        "SELECT bitShiftLeft(toUInt128(cityHash64(unhex('{h}'))), 64) \
         + toUInt128(xxHash64(unhex('{h}'))) AS fingerprint"
    );
    use futures::StreamExt;
    let mut stream = client
        .query_stream::<Compose128Row>(&sql, &QuerySettings::new())
        .await
        .expect("query_stream");
    let row = stream.next().await.expect("one row").expect("row decode");
    row.fingerprint
}

fn decode_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex digit"))
        .collect()
}

fn fixtures() -> Value {
    serde_json::from_str(FIXTURES).expect("tests/fixtures/fingerprints.json must be valid JSON")
}

/// Cross-checks every `raw_cityhash64_vectors` buffer (the full
/// length-class suite: 0/1/3/4/7/8/15/16/17/31/32/33/63/64/65 bytes + one
/// multi-KB buffer) against a live `SELECT cityHash64(unhex(...))`.
#[tokio::test]
async fn raw_cityhash64_vectors_match_a_live_server() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let fx = fixtures();
    let cases = fx["raw_cityhash64_vectors"].as_array().expect("array");
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().expect("name");
        let buf = decode_hex(case["buffer_hex"].as_str().expect("buffer_hex"));
        let expected: u64 = case["fingerprint"]
            .as_str()
            .expect("fingerprint")
            .parse()
            .expect("u64");
        let got = live_cityhash64(&client, &buf).await;
        assert_eq!(got, expected, "{name}: live ClickHouse mismatch");
    }
}

/// Cross-checks every `stream_fingerprints` buffer (boundary-straddling
/// multi-label buffers and non-ASCII UTF-8 values) against a live
/// `SELECT cityHash64(unhex(...))`.
#[tokio::test]
async fn stream_fingerprint_vectors_match_a_live_server() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let fx = fixtures();
    let cases = fx["stream_fingerprints"].as_array().expect("array");
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().expect("name");
        let buf = decode_hex(case["buffer_hex"].as_str().expect("buffer_hex"));
        let expected: u64 = case["fingerprint"]
            .as_str()
            .expect("fingerprint")
            .parse()
            .expect("u64");
        let got = live_cityhash64(&client, &buf).await;
        assert_eq!(got, expected, "{name}: live ClickHouse mismatch");
    }
}

/// The 128-bit composition, cross-checked against the server (issue #498).
///
/// The widening is only sound if ClickHouse can still derive the identity
/// itself — the writer is the fingerprint authority, but an independent
/// server-side derivation is what makes the label index checkable. Until
/// this test existed the claim rested on one hand-run `SELECT`.
///
/// For every committed `stream_fingerprints` and `metric_fingerprints`
/// buffer it asserts three things agree: the server's
/// `bitShiftLeft(toUInt128(cityHash64(buf)), 64) + toUInt128(xxHash64(buf))`,
/// the committed `fingerprint128` vector, and the value this crate's own
/// function returns for the case's labels. The row is read through a
/// `u128` field against a `UInt128` expression, so a client that could not
/// carry the width would fail here rather than silently truncate.
#[tokio::test]
async fn the_composed_fingerprint_matches_a_live_server() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let fx = fixtures();
    for (key, is_stream) in [
        ("stream_fingerprints", true),
        ("metric_fingerprints", false),
    ] {
        let cases = fx[key].as_array().expect("array");
        assert!(!cases.is_empty());
        for case in cases {
            let name = case["name"].as_str().expect("name");
            let buf = decode_hex(case["buffer_hex"].as_str().expect("buffer_hex"));
            let committed: u128 = case["fingerprint128"]
                .as_str()
                .expect("fingerprint128")
                .parse()
                .expect("u128");

            let got = live_compose128(&client, &buf).await;
            assert_eq!(got, committed, "{key}/{name}: live ClickHouse mismatch");

            let labels = LabelSet::from_verbatim(
                case["labels"]
                    .as_array()
                    .expect("labels")
                    .iter()
                    .map(|pair| {
                        let pair = pair.as_array().expect("label pair is a 2-array");
                        (
                            pair[0].as_str().expect("key").to_string(),
                            pair[1].as_str().expect("value").to_string(),
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            let ours = if is_stream {
                stream_fingerprint(&labels)
            } else {
                metric_fingerprint(&labels)
            };
            assert_eq!(
                ours,
                Fingerprint::from_raw(got),
                "{key}/{name}: this crate disagrees with the server"
            );
        }
    }
}
