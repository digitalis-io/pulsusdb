//! Fixture-driven tests for the OTLP logs receiver (issue #8 acceptance
//! criteria): `tests/fixtures/*.bin` are captured
//! `ExportLogsServiceRequest` protobuf payloads (provenance:
//! `tests/fixtures/README.md`). Each test here only decodes/parses a
//! fixture and asserts on `pulsus_write::protocols::otlp_logs::parse`'s
//! output — the fixture stands alone; nothing here re-derives the wire
//! bytes it reads (except `regenerate_fixtures`, gated `#[ignore]`, which
//! is how the fixtures were produced in the first place).

use std::path::{Path, PathBuf};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

use pulsus_write::protocols::otlp_logs::{decode, parse};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn read_fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixtures_dir().join(name))
        .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"))
}

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

fn string_value(s: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(s.to_string())),
    }
}

// ---------------------------------------------------------------------
// Fixture builders. Shared between `regenerate_fixtures` (which writes
// the committed `.bin` files) and nothing else — every other test in this
// file reads the committed bytes from disk, never these builders, so a
// fixture's on-disk content is what is actually under test.
// ---------------------------------------------------------------------

fn build_attributes_fixture() -> ExportLogsServiceRequest {
    let resource = Resource {
        attributes: vec![
            kv("service.name", Value::StringValue("checkout".to_string())),
            kv("k8s.pod.name", Value::StringValue("pod-7".to_string())),
            kv("env", Value::StringValue("prod".to_string())),
        ],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    };
    let scope = InstrumentationScope {
        name: "my-lib".to_string(),
        version: "2.3.1".to_string(),
        attributes: vec![kv("team", Value::StringValue("payments".to_string()))],
        dropped_attributes_count: 0,
    };
    let record = LogRecord {
        time_unix_nano: 1_700_000_000_123_456_789,
        severity_number: 17, // SEVERITY_NUMBER_ERROR
        body: Some(string_value("payment processed")),
        ..Default::default()
    };
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope),
                log_records: vec![record],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn build_partial_success_fixture() -> ExportLogsServiceRequest {
    let good_a = LogRecord {
        time_unix_nano: 1_700_000_000_000_000_000,
        body: Some(string_value("first")),
        ..Default::default()
    };
    let bad = LogRecord {
        // Top bit set: does not fit in `i64` nanoseconds — a per-record
        // rejection (architect plan: timestamps stored verbatim, never
        // clamped/rounded).
        time_unix_nano: u64::MAX,
        body: Some(string_value("unrepresentable timestamp")),
        ..Default::default()
    };
    let good_b = LogRecord {
        time_unix_nano: 1_700_000_100_000_000_000,
        body: Some(string_value("second")),
        ..Default::default()
    };
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![good_a, bad, good_b],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn build_cross_month_fixture() -> ExportLogsServiceRequest {
    // One stream (identical resource/scope), one record just before and
    // one just after the 2024-01-31/2024-02-01 UTC boundary.
    let resource = Resource {
        attributes: vec![kv(
            "service.name",
            Value::StringValue("billing".to_string()),
        )],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    };
    let january = LogRecord {
        time_unix_nano: 1_706_745_599_000_000_000, // 2024-01-31T23:59:59Z
        body: Some(string_value("end of january")),
        ..Default::default()
    };
    let february = LogRecord {
        time_unix_nano: 1_706_745_601_000_000_000, // 2024-02-01T00:00:01Z
        body: Some(string_value("start of february")),
        ..Default::default()
    };
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![january, february],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn build_backfilled_fixture() -> ExportLogsServiceRequest {
    let record = LogRecord {
        time_unix_nano: 1_577_836_800_000_000_000, // 2020-01-01T00:00:00Z
        body: Some(string_value("archived log, backfilled years later")),
        ..Default::default()
    };
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![record],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Regenerates every `tests/fixtures/*.bin` used by the tests below, plus
/// `malformed.bin` (a deliberately truncated protobuf message — the one
/// fixture with no builder above, since it must *not* decode). Gated
/// `#[ignore]`: this is not part of the normal test run, it is the tool
/// used to produce the committed fixtures in the first place. Run it with
/// `cargo test -p pulsus-write --test otlp_logs -- --ignored
/// regenerate_fixtures` after changing a builder above, then commit the
/// resulting `.bin` diffs (see `tests/fixtures/README.md`).
#[test]
#[ignore = "regenerates the committed fixtures; run explicitly, see doc comment"]
fn regenerate_fixtures() {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).unwrap();

    let write = |name: &str, req: &ExportLogsServiceRequest| {
        std::fs::write(dir.join(name), req.encode_to_vec()).unwrap();
    };
    write(
        "attributes_labels_body_severity_timestamp.bin",
        &build_attributes_fixture(),
    );
    write("partial_success.bin", &build_partial_success_fixture());
    write("cross_month.bin", &build_cross_month_fixture());
    write("backfilled.bin", &build_backfilled_fixture());

    // A well-formed field tag/wire-type prefix (like a real message would
    // start with) immediately cut off mid-value: prost sees a length-
    // delimited field announcing more bytes than are actually present and
    // fails with an unexpected-EOF `DecodeError`.
    let mut truncated = build_attributes_fixture().encode_to_vec();
    truncated.truncate(truncated.len() / 2);
    std::fs::write(dir.join("malformed.bin"), truncated).unwrap();
}

#[test]
fn attributes_flatten_into_normalized_labels_body_severity_and_timestamp_are_preserved() {
    let bytes = read_fixture("attributes_labels_body_severity_timestamp.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportLogsServiceRequest");
    let out = parse(&req, 0);

    assert_eq!(out.rows.len(), 1);
    let row = &out.rows[0];
    assert_eq!(row.service, "checkout");
    assert_eq!(row.severity, 17);
    assert_eq!(row.body, "payment processed");
    assert_eq!(row.timestamp_ns.0, 1_700_000_000_123_456_789);

    assert_eq!(out.streams.len(), 1);
    let labels = &out.streams[0].labels;
    // `service.name` -> `service` column (above) *and* the `service_name`
    // label (issue #4 AC#3: the same normalization chain drives both).
    assert_eq!(labels.get("service_name"), Some("checkout"));
    assert_eq!(labels.get("k8s_pod_name"), Some("pod-7"));
    assert_eq!(labels.get("env"), Some("prod"));
    assert_eq!(labels.get("otel_scope_name"), Some("my-lib"));
    assert_eq!(labels.get("otel_scope_version"), Some("2.3.1"));
    assert_eq!(labels.get("team"), Some("payments"));
    assert_eq!(out.collisions, 0);
    assert_eq!(out.rejected, 0);
}

#[test]
fn malformed_protobuf_is_a_whole_request_decode_error() {
    let bytes = read_fixture("malformed.bin");
    let err = decode(&bytes).expect_err("truncated protobuf must not decode");
    assert!(matches!(err, pulsus_write::LogsIngestError::Decode(_)));
}

#[test]
fn partial_success_drops_only_the_unrepresentable_record() {
    let bytes = read_fixture("partial_success.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportLogsServiceRequest");
    let out = parse(&req, 0);

    assert_eq!(out.rejected, 1);
    let message = out.rejected_message.expect("first rejection is recorded");
    assert!(!message.is_empty());

    assert_eq!(out.rows.len(), 2);
    let bodies: Vec<&str> = out.rows.iter().map(|r| r.body.as_str()).collect();
    assert_eq!(bodies, vec!["first", "second"]);
}

#[test]
fn cross_month_request_registers_one_stream_row_per_month() {
    let bytes = read_fixture("cross_month.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportLogsServiceRequest");
    let out = parse(&req, 0);

    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.streams.len(), 2);
    // Same logical stream (one resource/scope): equal fingerprints, two
    // distinct months.
    assert_eq!(out.streams[0].fingerprint, out.streams[1].fingerprint);
    assert_ne!(out.streams[0].month, out.streams[1].month);

    let mut days: Vec<u16> = out
        .streams
        .iter()
        .map(|s| s.month.days_since_epoch())
        .collect();
    days.sort_unstable();
    // 2024-01-01 and 2024-02-01, days since the Unix epoch.
    assert_eq!(days, vec![19_723, 19_754]);
}

#[test]
fn backfilled_timestamp_registers_its_historical_month_not_the_receive_month() {
    let bytes = read_fixture("backfilled.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportLogsServiceRequest");
    // "Receive time" is much later than the record's own 2020 timestamp.
    let now_ns = 1_700_000_000_000_000_000; // ~2023-11-14
    let out = parse(&req, now_ns);

    assert_eq!(out.streams.len(), 1);
    // 2020-01-01, days since the Unix epoch.
    assert_eq!(out.streams[0].month.days_since_epoch(), 18_262);
    // The `ReplacingMergeTree` version column is still the receive time.
    assert_eq!(out.streams[0].updated_ns, now_ns);
}

#[test]
fn parse_is_pure_repeated_calls_on_the_same_fixture_are_identical() {
    let bytes = read_fixture("attributes_labels_body_severity_timestamp.bin");
    let req = decode(&bytes).expect("fixture is a valid ExportLogsServiceRequest");
    let a = parse(&req, 123);
    let b = parse(&req, 123);
    assert_eq!(a, b);
}
