//! Fixture + cross-transport tests for the Loki push receiver (issue #77
//! acceptance criteria). `tests/fixtures/loki-push/promtail_push.bin` is a
//! **real capture** — a snappy-compressed `logproto.PushRequest` recorded
//! from a live grafana/promtail 3.4.2 agent (provenance:
//! `tests/fixtures/loki-push/README.md`) — proving the hand-rolled
//! `logproto` tags in `protocols/loki_push.rs` decode real wire bytes, not
//! just self-consistent synthetic round-trips through the same structs (a
//! wrong tag decodes without error but silently corrupts every following
//! field).
//!
//! The load-bearing correctness test (AC-3) proves a stream pushed via #77
//! fingerprints IDENTICALLY to the same logical stream ingested via OTLP
//! logs — the exact queryability/tailability gate: pushed logs reach the
//! LogQL read path (#72/#73) and tail (#74) because their fingerprint is,
//! by construction, the one those paths expect.

use std::path::{Path, PathBuf};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

use pulsus_write::ingest::decompress::{Encoding, decompress};
use pulsus_write::protocols::loki_push::{
    EntryAdapter, LabelPairAdapter, PushRequest, StreamAdapter, Timestamp, decode_protobuf,
    parse_json, parse_protobuf,
};
use pulsus_write::protocols::otlp_logs;

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/loki-push")
}

/// Reads and decodes the captured `.bin` fixture: snappy-decompress (the
/// captured bytes are exactly the agent's HTTP body — `Content-Type:
/// application/x-protobuf`, no `Content-Encoding`, implicit snappy) then
/// prost-decode — mirrors what `ingest_loki_push`'s protobuf branch does.
fn decode_fixture(name: &str) -> PushRequest {
    let compressed =
        std::fs::read(fixtures_dir().join(name)).unwrap_or_else(|e| panic!("reading {name}: {e}"));
    let decompressed =
        decompress(Encoding::Snappy, &compressed).expect("fixture is valid block-snappy");
    decode_protobuf(&decompressed).expect("fixture is a valid PushRequest")
}

// ---------------------------------------------------------------------
// AC-2: real-capture fixture golden.
// ---------------------------------------------------------------------

/// The exact (timestamp, line) rows the committed capture decodes to, in
/// the fixture's own entry order. The `.bin` is a frozen artifact, so its
/// promtail ingestion-time stamps are fixed bytes — pinned here as literals
/// rather than merely asserted positive, so a future decode regression that
/// shifts a field (a wrong hand-rolled tag) is caught by an exact mismatch,
/// not waved through by a still-positive garbage value.
const EXPECTED_ROWS: &[(i64, &str)] = &[
    (1_784_302_981_712_882_584, "hello from real producer"),
    (1_784_302_981_712_899_648, "second line from promtail"),
];

/// The pinned `stream_fingerprint` of the decoded stream
/// `{env="prod", filename="/logdir/app.log", service_name="checkout"}`,
/// computed once via the frozen model function and pasted here as a literal.
/// Asserting against a hard-coded constant (not a value recomputed from the
/// same decoded output) is what actually pins the parse: recomputing over
/// the just-decoded labels only proves internal self-consistency and would
/// silently track any decode drift.
const EXPECTED_FINGERPRINT: pulsus_model::Fingerprint = 0x0444_F261_FF1E_744E;

/// The real promtail capture decodes to the exact rows/labels/fingerprint a
/// wrong hand-rolled tag could not reproduce — every value pinned as a
/// literal against the frozen `.bin`.
#[test]
fn promtail_capture_decodes_to_the_pinned_rows_and_labels() {
    let req = decode_fixture("promtail_push.bin");
    let out = parse_protobuf(&req, 0).expect("real capture parses");

    // One stream, two log lines.
    assert_eq!(out.streams.len(), 1, "one promtail stream");
    assert_eq!(out.rows.len(), 2, "two captured log lines");

    // Exact (timestamp, line) rows, in the fixture's entry order.
    let rows: Vec<(i64, &str)> = out
        .rows
        .iter()
        .map(|r| (r.timestamp_ns.0, r.body.as_str()))
        .collect();
    assert_eq!(
        rows, EXPECTED_ROWS,
        "decoded rows must match the pinned golden"
    );

    // promtail promotes the static_config labels + a `filename` label; the
    // `service_name` label drives the `service` column.
    let stream = &out.streams[0];
    assert_eq!(stream.labels.get("service_name"), Some("checkout"));
    assert_eq!(stream.labels.get("env"), Some("prod"));
    assert_eq!(stream.labels.get("filename"), Some("/logdir/app.log"));
    assert_eq!(stream.service, "checkout");
    for row in &out.rows {
        assert_eq!(row.service, "checkout");
        assert_eq!(row.severity, 0);
        assert_eq!(row.fingerprint, stream.fingerprint);
    }

    // The stream fingerprint is pinned against a hard-coded literal — the
    // value was computed once and pasted in; it is NOT recomputed from the
    // decoded labels here (that would only prove self-consistency). A decode
    // regression that shifts the label bytes changes this hash and trips.
    assert_eq!(
        stream.fingerprint, EXPECTED_FINGERPRINT,
        "decoded stream fingerprint must match the pinned golden constant"
    );
}

/// `parse_protobuf` is a pure function of its arguments (the real capture is
/// the input).
#[test]
fn parse_of_the_real_capture_is_pure() {
    let req = decode_fixture("promtail_push.bin");
    assert_eq!(
        parse_protobuf(&req, 123).unwrap(),
        parse_protobuf(&req, 123).unwrap()
    );
}

// ---------------------------------------------------------------------
// AC-3: cross-transport fingerprint identity with OTLP logs.
// ---------------------------------------------------------------------

/// The load-bearing correctness gate (issue #77 delta 2 / adjudication): a
/// Loki stream `{service_name="checkout", env="prod"}` and the equivalent
/// **scope-absent, resource-only** OTLP log payload produce the SAME
/// `fingerprint` and `service`.
///
/// The exact condition matters (delta 2): `otlp_logs::build_scope_labels`
/// injects `otel_scope_name`/`otel_scope_version` whenever `ScopeLogs.scope`
/// is `Some`, which would split the fingerprints. With `scope = None`
/// ("absent scopes emit nothing") and resource attributes whose
/// `canonicalize_label_key` images equal the Loki label keys, both inputs
/// feed `LabelSet::from_normalized` over an identical pair set — so
/// `stream_fingerprint` and `service()` are identical by construction.
#[test]
fn loki_stream_fingerprints_identically_to_the_equivalent_otlp_log_stream() {
    // Loki side: `{service_name="checkout", env="prod"}`.
    let loki = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{service_name="checkout", env="prod"}"#.to_string(),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                line: "hello".to_string(),
                structured_metadata: Vec::new(),
            }],
        }],
    };
    let loki_out = parse_protobuf(&loki, 0).unwrap();

    // OTLP side: scope=None, resource attrs `service.name=checkout`,
    // `env=prod` — `service.name` canonicalizes to `service_name`, matching
    // the Loki key exactly.
    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
            key_strindex: 0,
        }
    }
    let otlp = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "checkout"), kv("env", "prod")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: None, // load-bearing: no otel_scope_* labels injected
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("hello".to_string())),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let otlp_out = otlp_logs::parse(&otlp, 0);

    assert_eq!(
        loki_out.rows[0].fingerprint, otlp_out.rows[0].fingerprint,
        "a Loki-pushed stream must fingerprint identically to the equivalent OTLP log stream, \
         or pushed logs become unqueryable via LogQL/tail"
    );
    assert_eq!(loki_out.rows[0].service, otlp_out.rows[0].service);
    assert_eq!(
        loki_out.streams[0].fingerprint,
        otlp_out.streams[0].fingerprint
    );
    assert_eq!(loki_out.streams[0].labels, otlp_out.streams[0].labels);
}

/// The scope-present caveat delta 2 documents: with `scope = Some`, OTLP
/// injects `otel_scope_*` labels, so the fingerprints legitimately DIFFER —
/// pinned here so the narrow scope-absent condition of the AC above is not
/// mistaken for an unconditional equivalence.
#[test]
fn scope_present_otlp_diverges_from_loki_by_the_injected_otel_scope_labels() {
    let loki = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{service_name="checkout"}"#.to_string(),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                line: "x".to_string(),
                structured_metadata: Vec::new(),
            }],
        }],
    };
    let loki_out = parse_protobuf(&loki, 0).unwrap();

    let otlp = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("checkout".to_string())),
                    }),
                    key_strindex: 0,
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(
                    opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                        name: "my-scope".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    },
                ),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("x".to_string())),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let otlp_out = otlp_logs::parse(&otlp, 0);

    assert_ne!(
        loki_out.streams[0].fingerprint, otlp_out.streams[0].fingerprint,
        "a scope-present OTLP payload carries otel_scope_* labels a Loki stream does not"
    );
}

// ---------------------------------------------------------------------
// AC-1: dual-encoding equivalence, end to end through the public parsers.
// ---------------------------------------------------------------------

#[test]
fn json_and_protobuf_bodies_parse_identically() {
    let json = br#"{"streams":[{"stream":{"service_name":"checkout","env":"prod"},
        "values":[["1700000000000000000","a"],["1700000001000000000","b"]]}]}"#;
    let proto = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{service_name="checkout", env="prod"}"#.to_string(),
            entries: vec![
                EntryAdapter {
                    timestamp: Some(Timestamp {
                        seconds: 1_700_000_000,
                        nanos: 0,
                    }),
                    line: "a".to_string(),
                    structured_metadata: Vec::new(),
                },
                EntryAdapter {
                    timestamp: Some(Timestamp {
                        seconds: 1_700_000_001,
                        nanos: 0,
                    }),
                    line: "b".to_string(),
                    structured_metadata: Vec::new(),
                },
            ],
        }],
    };
    assert_eq!(
        parse_json(json, 5).unwrap(),
        parse_protobuf(&proto, 5).unwrap()
    );
}

/// Issue #97 (AC-4): a protobuf tag-3 body and a JSON third-element body of
/// one logical entry carrying structured metadata parse to byte-identical
/// `ParsedLogs` through the public receiver parsers — the canonical JSON
/// String is stored per entry, and the stream fingerprint is unchanged.
#[test]
fn structured_metadata_parses_identically_across_transports() {
    let json = br#"{"streams":[{"stream":{"service_name":"checkout","env":"prod"},
        "values":[["1700000000000000000","boom",{"user_id":"42","trace_id":"abc"}]]}]}"#;
    let proto = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{service_name="checkout", env="prod"}"#.to_string(),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                line: "boom".to_string(),
                structured_metadata: vec![
                    LabelPairAdapter {
                        name: "user_id".to_string(),
                        value: "42".to_string(),
                    },
                    LabelPairAdapter {
                        name: "trace_id".to_string(),
                        value: "abc".to_string(),
                    },
                ],
            }],
        }],
    };
    let from_json = parse_json(json, 5).unwrap();
    let from_proto = parse_protobuf(&proto, 5).unwrap();
    assert_eq!(from_json, from_proto);
    // Canonical sorted-key JSON String (the log_streams.labels shape).
    assert_eq!(
        from_json.rows[0].structured_metadata,
        r#"{"trace_id":"abc","user_id":"42"}"#
    );
    // A structureless push of the same stream fingerprints identically —
    // structured metadata is per-entry, never in the stream label set.
    let plain = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{service_name="checkout", env="prod"}"#.to_string(),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                line: "boom".to_string(),
                structured_metadata: Vec::new(),
            }],
        }],
    };
    let from_plain = parse_protobuf(&plain, 5).unwrap();
    assert_eq!(from_plain.streams, from_proto.streams);
    assert_eq!(
        from_plain.rows[0].fingerprint,
        from_proto.rows[0].fingerprint
    );
}
