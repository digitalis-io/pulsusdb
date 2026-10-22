//! Hermetic OTLP-traces decode fidelity gate (issue #54 AC1, issue #602):
//! `tests/fixtures/otlp-traces/*.bin` are committed
//! `ExportTraceServiceRequest` protobuf payloads (provenance:
//! `tests/fixtures/otlp-traces/README.md`). The test decodes a fixture with
//! the landing decode and asserts **hand-derived golden rows**: every
//! expected value below is written out from the fixture constants, not
//! recomputed by calling the code under test. The ClickHouse round trip of
//! the same fixture lives in `trace_ingest_roundtrip.rs` (env-gated).

use std::path::{Path, PathBuf};

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link, SpanKind};
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;

use pulsus_clickhouse::json_column::{TraceJson, TraceJsonValue};
use pulsus_write::{decode_traces, parse_trace_landing};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/otlp-traces")
}

fn read_fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixtures_dir().join(name))
        .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"))
}

// ---------------------------------------------------------------------
// Fixture constants — the hand-authored inputs AND the source of the
// hand-derived golden values below. Fixed literals (no clock): this suite
// never touches ClickHouse, so there is no TTL-window hazard.
// ---------------------------------------------------------------------

/// 2023-11-14T22:13:20Z. Day 19675 since the Unix epoch
/// (1_700_000_000 s / 86_400 s = 19675.92... -> floor 19675) — the golden
/// `day` the resource row must carry.
const SPAN_A_START_NS: u64 = 1_700_000_000_000_000_000;
const SPAN_A_END_NS: u64 = SPAN_A_START_NS + 1_000_000_000; // +1s
/// Span A's single event fires 3 ms after the span starts.
const SPAN_A_EVENT_NS: u64 = SPAN_A_START_NS + 3_000_000; // +3ms
/// Span A's single link references another span in another trace.
const LINK_TRACE_ID: [u8; 16] = [
    0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
];
const LINK_SPAN_ID: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
const SPAN_B_START_NS: u64 = SPAN_A_START_NS + 5_000_000; // +5ms
const SPAN_B_END_NS: u64 = SPAN_B_START_NS + 250_000_000; // +250ms
const GOLDEN_DAY: u16 = 19_675;

const TRACE_ID: [u8; 16] = [
    0x4B, 0xF9, 0x2F, 0x35, 0x77, 0xB3, 0x4D, 0xA6, 0xA3, 0xCE, 0x92, 0x9D, 0x0E, 0x0E, 0x47, 0x36,
];
const SPAN_A_ID: [u8; 8] = [0x00, 0xF0, 0x67, 0xAA, 0x0B, 0xA9, 0x02, 0xB7];
const SPAN_B_ID: [u8; 8] = [0x30, 0x22, 0x84, 0x0F, 0x1D, 0x1E, 0x9A, 0x7D];

// ---------------------------------------------------------------------
// Fixture builder. Shared between `regenerate_fixtures` (which writes the
// committed `.bin`) and nothing else — every other test reads the
// committed bytes from disk.
// ---------------------------------------------------------------------

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

fn fixture_resource() -> Resource {
    Resource {
        attributes: vec![
            kv("service.name", Value::StringValue("checkout".to_string())),
            kv(
                "deployment.environment",
                Value::StringValue("prod".to_string()),
            ),
        ],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    }
}

fn fixture_scope() -> InstrumentationScope {
    InstrumentationScope {
        name: "checkout-instrumentation".to_string(),
        version: "1.2.3".to_string(),
        // Deliberately non-empty: scope attributes are stored on the span
        // row and catalogued under the `instrumentation` scope.
        attributes: vec![kv(
            "scope.only.attr",
            Value::StringValue("payload-only".to_string()),
        )],
        dropped_attributes_count: 0,
    }
}

/// Span A: root span (empty parent), Server kind, unset status, and three
/// attributes — `http.status_code` (Int 500, an `int` value),
/// `http.method` ("GET"), and `deployment.environment` ("prod") which
/// ALSO exists at resource scope: the dual-scope same-key golden (issue
/// #54 plan v2 test-gap fix). It also carries one span **event** (issue
/// #192 PR-B): `exception` at +3 ms with one attribute `exception.type`, and
/// one span **link** (issue #192 PR-C) to another trace's span with one
/// attribute `link.relation`.
fn span_a() -> Span {
    Span {
        trace_id: TRACE_ID.to_vec(),
        span_id: SPAN_A_ID.to_vec(),
        parent_span_id: vec![],
        name: "GET /checkout".to_string(),
        kind: SpanKind::Server as i32,
        start_time_unix_nano: SPAN_A_START_NS,
        end_time_unix_nano: SPAN_A_END_NS,
        attributes: vec![
            kv("http.status_code", Value::IntValue(500)),
            kv("http.method", Value::StringValue("GET".to_string())),
            kv(
                "deployment.environment",
                Value::StringValue("prod".to_string()),
            ),
        ],
        events: vec![Event {
            time_unix_nano: SPAN_A_EVENT_NS,
            name: "exception".to_string(),
            attributes: vec![kv(
                "exception.type",
                Value::StringValue("IOError".to_string()),
            )],
            dropped_attributes_count: 0,
        }],
        links: vec![Link {
            trace_id: LINK_TRACE_ID.to_vec(),
            span_id: LINK_SPAN_ID.to_vec(),
            trace_state: String::new(),
            attributes: vec![kv(
                "link.relation",
                Value::StringValue("child_of".to_string()),
            )],
            dropped_attributes_count: 0,
            flags: 0,
        }],
        status: None,
        ..Default::default()
    }
}

/// Span B: child of span A, Client kind, explicit Error status, no span
/// attributes.
fn span_b() -> Span {
    Span {
        trace_id: TRACE_ID.to_vec(),
        span_id: SPAN_B_ID.to_vec(),
        parent_span_id: SPAN_A_ID.to_vec(),
        name: "charge-card".to_string(),
        kind: SpanKind::Client as i32,
        start_time_unix_nano: SPAN_B_START_NS,
        end_time_unix_nano: SPAN_B_END_NS,
        attributes: vec![],
        status: Some(Status {
            message: "card declined".to_string(),
            code: StatusCode::Error as i32,
        }),
        ..Default::default()
    }
}

fn build_two_spans_dual_scope_fixture() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(fixture_resource()),
            scope_spans: vec![ScopeSpans {
                scope: Some(fixture_scope()),
                spans: vec![span_a(), span_b()],
                schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
            }],
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
        }],
    }
}

/// Regenerates the committed fixture bytes. `#[ignore]`-gated: run
/// explicitly after editing a builder, then review and commit the `.bin`
/// diff (see the fixtures README).
#[test]
#[ignore]
fn regenerate_fixtures() {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    std::fs::write(
        dir.join("two_spans_dual_scope.bin"),
        build_two_spans_dual_scope_fixture().encode_to_vec(),
    )
    .expect("write fixture");
}

// ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// AC1: hand-derived golden rows.
// ---------------------------------------------------------------------

/// Each stored path of `j` with its scalar value rendered, in stored order.
fn paths(j: &TraceJson) -> Vec<(String, String)> {
    j.entries()
        .iter()
        .map(|e| match &e.value {
            TraceJsonValue::Scalar(s) => {
                (e.path.clone(), format!("{}:{}", s.val_type(), s.render()))
            }
            other => panic!("fixture carries scalars only, got {other:?}"),
        })
        .collect()
}

fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

/// AC1 (issue #54, re-pointed by issue #602): decode the committed fixture
/// with the landing decode and assert every span, resource and catalog row
/// against values written out from the fixture constants.
#[test]
fn parse_produces_the_hand_derived_golden_rows() {
    let req = decode_traces(&read_fixture("two_spans_dual_scope.bin")).expect("fixture decodes");
    let out = parse_trace_landing(&req).expect("within the expansion budget");

    assert_eq!(out.rejected, 0);
    assert_eq!(out.rejected_message, None);
    assert_eq!(out.spans.len(), 2);
    assert_eq!(out.resources.len(), 1, "one resource, one UTC day");

    let r = &out.resources[0];
    assert_eq!(r.day, GOLDEN_DAY);
    assert_eq!(r.service, "checkout");
    assert_eq!(
        paths(&r.attrs),
        pairs(&[("deployment%2Eenvironment", "string:prod")]),
        "service.name is a column, not a resource attribute"
    );
    assert_eq!(r.schema_url, "https://opentelemetry.io/schemas/1.21.0");

    let a = &out.spans[0];
    assert_eq!(a.trace_id, TRACE_ID);
    assert_eq!(a.span_id, SPAN_A_ID);
    assert_eq!(a.parent_span_id, [0u8; 8], "empty parent -> zero sentinel");
    assert_eq!(a.resource_id, r.resource_id);
    assert_eq!(a.name, "GET /checkout");
    assert_eq!(a.service, "checkout");
    assert_eq!(a.service_type, "string");
    assert_eq!(a.start_ns, 1_700_000_000_000_000_000);
    assert_eq!(a.duration_ns, 1_000_000_000);
    assert_eq!(a.end_ns, SPAN_A_END_NS);
    assert_eq!(a.status_code, 0, "unset status -> code 0");
    assert_eq!(a.kind, 2, "SPAN_KIND_SERVER = 2");
    assert_eq!(a.scope_name, "checkout-instrumentation");
    assert_eq!(a.scope_version, "1.2.3");
    assert_eq!(
        a.scope_schema_url,
        "https://opentelemetry.io/schemas/1.21.0"
    );
    assert_eq!(
        paths(&a.scope_attrs),
        pairs(&[("scope%2Eonly%2Eattr", "string:payload-only")])
    );
    assert_eq!(
        paths(&a.attrs),
        pairs(&[
            ("deployment%2Eenvironment", "string:prod"),
            ("http%2Emethod", "string:GET"),
            ("http%2Estatus_code", "int:500"),
        ])
    );
    assert_eq!(a.events.len(), 1);
    assert_eq!(a.events[0].time_ns, SPAN_A_EVENT_NS);
    assert_eq!(a.events[0].name, "exception");
    assert_eq!(
        paths(&a.events[0].attrs),
        pairs(&[("exception%2Etype", "string:IOError")])
    );
    assert_eq!(a.links.len(), 1);
    assert_eq!(a.links[0].trace_id, LINK_TRACE_ID.to_vec());
    assert_eq!(a.links[0].span_id, LINK_SPAN_ID.to_vec());
    assert_eq!(
        paths(&a.links[0].attrs),
        pairs(&[("link%2Erelation", "string:child_of")])
    );

    let b = &out.spans[1];
    assert_eq!(b.trace_id, TRACE_ID);
    assert_eq!(b.span_id, SPAN_B_ID);
    assert_eq!(b.parent_span_id, SPAN_A_ID);
    assert_eq!(b.resource_id, r.resource_id);
    assert_eq!(b.name, "charge-card");
    assert_eq!(b.start_ns, 1_700_000_000_005_000_000);
    assert_eq!(b.duration_ns, 250_000_000);
    assert_eq!(b.end_ns, SPAN_B_END_NS);
    assert_eq!(b.status_code, 2, "STATUS_CODE_ERROR = 2");
    assert_eq!(b.status_message, "card declined");
    assert_eq!(b.kind, 3, "SPAN_KIND_CLIENT = 3");
    assert!(b.attrs.is_empty() && b.events.is_empty() && b.links.is_empty());

    // The catalogs: one row per distinct (scope, key) and per distinct
    // (scope, key, value, type), keys original rather than escaped. The
    // dual-scope key `deployment.environment` is two rows, one per scope.
    let names: Vec<(&str, &str)> = out
        .tag_names
        .iter()
        .map(|n| (n.scope.as_str(), n.key.as_str()))
        .collect();
    let mut want_names = vec![
        ("resource", "deployment.environment"),
        ("resource", "service.name"),
        ("span", "deployment.environment"),
        ("span", "http.method"),
        ("span", "http.status_code"),
        ("event", "exception.type"),
        ("link", "link.relation"),
        ("instrumentation", "scope.only.attr"),
    ];
    let mut got_names = names.clone();
    got_names.sort();
    want_names.sort();
    assert_eq!(got_names, want_names);

    let mut values: Vec<(&str, &str, &str, &str)> = out
        .tag_values
        .iter()
        .map(|v| {
            (
                v.scope.as_str(),
                v.key.as_str(),
                v.value.as_str(),
                v.val_type,
            )
        })
        .collect();
    values.sort();
    let mut want_values = vec![
        ("resource", "deployment.environment", "prod", "string"),
        ("resource", "service.name", "checkout", "string"),
        ("span", "deployment.environment", "prod", "string"),
        ("span", "http.method", "GET", "string"),
        ("span", "http.status_code", "500", "int"),
        ("event", "exception.type", "IOError", "string"),
        ("link", "link.relation", "child_of", "string"),
        (
            "instrumentation",
            "scope.only.attr",
            "payload-only",
            "string",
        ),
    ];
    want_values.sort();
    assert_eq!(values, want_values);
}

/// The committed fixture bytes match the builder — a drift guard so a
/// builder edit without `regenerate_fixtures` (or a hand-edited `.bin`)
/// fails loudly instead of silently testing stale bytes.
#[test]
fn committed_fixture_matches_the_builder() {
    assert_eq!(
        read_fixture("two_spans_dual_scope.bin"),
        build_two_spans_dual_scope_fixture().encode_to_vec(),
        "run `cargo test -p pulsus-write --test trace_ingest_fidelity -- --ignored \
         regenerate_fixtures` and commit the diff"
    );
}
