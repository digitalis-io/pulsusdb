//! Hermetic OTLP-traces parser fidelity gate (issue #54 AC1/AC2):
//! `tests/fixtures/otlp-traces/*.bin` are committed
//! `ExportTraceServiceRequest` protobuf payloads (provenance:
//! `tests/fixtures/otlp-traces/README.md`, same construction method as the
//! logs/metrics fixture corpora — built programmatically against this
//! crate's own `opentelemetry-proto` dependency, then `prost`-encoded).
//! The tests decode a fixture, run `otlp_traces::parse`, and assert
//! **hand-derived golden rows** — every expected field below is written
//! out from the pinned parse rules (issue #54 plan), not recomputed by
//! calling the code under test.
//!
//! Unlike the logs `ingest_fidelity.rs` precedent there is **no
//! live-`cityHash64`-oracle step and no ClickHouse**: traces carry no
//! fingerprint (`cityHash64(trace_id)` is a server-side Distributed
//! sharding expression, never a stored value), so the golden is fully
//! hand-derivable and this suite is hermetic (rides `cargo test
//! --workspace`). The ClickHouse round-trip lives in
//! `trace_ingest_roundtrip.rs` (env-gated).
//!
//! The one exception to "hand-derived": each `SpanRecord.payload`'s exact
//! bytes are prost-encoder output, infeasible to write by hand — the
//! payload is therefore proven **structurally** (AC2): decode it as
//! `TracesData` and assert the single-`ResourceSpans` shape (this span +
//! its own resource + its own scope + both schema URLs), the pinned T2/T3
//! contract.

use std::path::{Path, PathBuf};

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData};
use prost::Message;

use pulsus_write::{decode_traces, parse_traces};

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
/// `date` every attr row must carry.
const SPAN_A_START_NS: u64 = 1_700_000_000_000_000_000;
const SPAN_A_END_NS: u64 = SPAN_A_START_NS + 1_000_000_000; // +1s
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
        // Deliberately non-empty: scope attributes are indexed under
        // `scope='instrumentation'` (issue #192, superseding the #54
        // adjudication #2 that dropped them) AND kept in the payload.
        attributes: vec![kv(
            "scope.only.attr",
            Value::StringValue("payload-only".to_string()),
        )],
        dropped_attributes_count: 0,
    }
}

/// Span A: root span (empty parent), Server kind, unset status, and three
/// attributes — `http.status_code` (Int 500, exercises `val_num`),
/// `http.method` ("GET"), and `deployment.environment` ("prod") which
/// ALSO exists at resource scope: the dual-scope same-key golden (issue
/// #54 plan v2 test-gap fix).
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
// AC1: hand-derived golden rows.
// ---------------------------------------------------------------------

/// AC1 (issue #54): decode -> parse -> assert the exact `SpanRecord` and
/// `AttrRecord` vectors against hand-derived expectations from the pinned
/// parse rules. Payload bytes are excluded here (proven structurally by
/// the AC2 test below — prost output is not hand-derivable).
#[test]
fn parse_produces_the_hand_derived_golden_rows() {
    let req = decode_traces(&read_fixture("two_spans_dual_scope.bin")).expect("fixture decodes");
    let out = parse_traces(&req, 0).expect("within the expansion budget");

    assert_eq!(out.rejected, 0);
    assert_eq!(out.rejected_message, None);

    // --- spans (all fields except payload, which AC2 proves) ---
    assert_eq!(out.spans.len(), 2);

    let a = &out.spans[0];
    assert_eq!(a.trace_id, TRACE_ID);
    assert_eq!(a.span_id, SPAN_A_ID);
    assert_eq!(a.parent_id, [0u8; 8], "empty parent -> zero sentinel");
    assert_eq!(a.name, "GET /checkout");
    assert_eq!(
        a.service, "checkout",
        "resource service.name promoted verbatim"
    );
    assert_eq!(a.timestamp_ns, 1_700_000_000_000_000_000);
    assert_eq!(a.duration_ns, 1_000_000_000);
    assert_eq!(a.status_code, 0, "unset status -> code 0");
    assert_eq!(a.kind, 2, "SPAN_KIND_SERVER = 2");
    assert_eq!(
        a.scope_name, "checkout-instrumentation",
        "instrumentation scope name promoted (issue #192)"
    );
    assert_eq!(a.scope_version, "1.2.3", "instrumentation scope version");

    let b = &out.spans[1];
    assert_eq!(b.trace_id, TRACE_ID);
    assert_eq!(b.span_id, SPAN_B_ID);
    assert_eq!(b.parent_id, SPAN_A_ID);
    assert_eq!(b.name, "charge-card");
    assert_eq!(b.service, "checkout");
    assert_eq!(b.timestamp_ns, 1_700_000_000_005_000_000);
    assert_eq!(b.timestamp_ns, 1_700_000_000_000_000_000 + 5_000_000);
    assert_eq!(b.duration_ns, 250_000_000);
    assert_eq!(b.status_code, 2, "STATUS_CODE_ERROR = 2");
    assert_eq!(b.kind, 3, "SPAN_KIND_CLIENT = 3");
    assert_eq!(b.scope_name, "checkout-instrumentation");
    assert_eq!(b.scope_version, "1.2.3");

    // --- attrs: keys verbatim (never normalized),
    // resource-then-span-then-instrumentation order per span, scope
    // discriminators, val_num only for the numeric parse, day-floored date;
    // instrumentation-scope attributes are indexed under
    // `scope='instrumentation'` (issue #192).
    /// One attr row's golden-comparable projection (`date`/`trace_id`/
    /// per-span carried columns are asserted separately below).
    type AttrGolden<'a> = (&'a str, &'a str, &'a str, Option<f64>, [u8; 8]);
    let rows: Vec<AttrGolden<'_>> = out
        .attrs
        .iter()
        .map(|r| {
            (
                r.scope.as_str(),
                r.key.as_str(),
                r.val.as_str(),
                r.val_num,
                r.span_id,
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            // span A: resource attrs...
            ("resource", "service.name", "checkout", None, SPAN_A_ID),
            (
                "resource",
                "deployment.environment",
                "prod",
                None,
                SPAN_A_ID
            ),
            // ...then span A's own attrs — note the verbatim dotted keys
            // (`http.status_code`, not `http_status_code`) and the
            // dual-scope `deployment.environment`, distinct from the
            // resource row only by scope.
            ("span", "http.status_code", "500", Some(500.0), SPAN_A_ID),
            ("span", "http.method", "GET", None, SPAN_A_ID),
            ("span", "deployment.environment", "prod", None, SPAN_A_ID),
            // ...then span A's instrumentation-scope attr (issue #192),
            // emitted last in the resource→span→instrumentation order.
            (
                "instrumentation",
                "scope.only.attr",
                "payload-only",
                None,
                SPAN_A_ID
            ),
            // span B: resource attrs (no span attrs) then the scope attr.
            ("resource", "service.name", "checkout", None, SPAN_B_ID),
            (
                "resource",
                "deployment.environment",
                "prod",
                None,
                SPAN_B_ID
            ),
            (
                "instrumentation",
                "scope.only.attr",
                "payload-only",
                None,
                SPAN_B_ID
            ),
        ]
    );
    for attr in &out.attrs {
        assert_eq!(attr.date, GOLDEN_DAY, "per-day floor of the span timestamp");
        assert_eq!(attr.trace_id, TRACE_ID);
    }
    // Per-span carried columns: each attr row carries ITS span's
    // timestamp/duration.
    for attr in out.attrs.iter().filter(|r| r.span_id == SPAN_A_ID) {
        assert_eq!(attr.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(attr.duration_ns, 1_000_000_000);
    }
    for attr in out.attrs.iter().filter(|r| r.span_id == SPAN_B_ID) {
        assert_eq!(attr.timestamp_ns, 1_700_000_000_005_000_000);
        assert_eq!(attr.duration_ns, 250_000_000);
    }
}

/// The dual-scope golden's sharpest form (issue #54 plan v2 test-gap fix):
/// the identical verbatim `(key, val)` pair exists at BOTH scopes for span
/// A, and the two rows differ in nothing but `scope`.
#[test]
fn dual_scope_same_key_rows_differ_only_in_scope() {
    let req = decode_traces(&read_fixture("two_spans_dual_scope.bin")).expect("fixture decodes");
    let out = parse_traces(&req, 0).expect("within the expansion budget");

    let dual: Vec<_> = out
        .attrs
        .iter()
        .filter(|r| r.key == "deployment.environment" && r.val == "prod" && r.span_id == SPAN_A_ID)
        .collect();
    assert_eq!(dual.len(), 2, "one row per scope, no collapse");
    assert_eq!(dual[0].scope, "resource");
    assert_eq!(dual[1].scope, "span");
    // Everything except scope is identical — scope alone separates them.
    let (x, y) = (dual[0], dual[1]);
    assert_eq!(x.date, y.date);
    assert_eq!(x.key, y.key);
    assert_eq!(x.val, y.val);
    assert_eq!(x.val_num, y.val_num);
    assert_eq!(x.timestamp_ns, y.timestamp_ns);
    assert_eq!(x.trace_id, y.trace_id);
    assert_eq!(x.span_id, y.span_id);
    assert_eq!(x.duration_ns, y.duration_ns);
}

// ---------------------------------------------------------------------
// AC2: payload reconstruction (the pinned T2/T3 contract).
// ---------------------------------------------------------------------

/// AC2 (issue #54): each `SpanRecord.payload` decodes as a `TracesData`
/// with exactly one `ResourceSpans` (resource attrs present) -> one
/// `ScopeSpans` (scope present, incl. the payload-only scope attribute) ->
/// one `Span` whose ids/name match the record — self-contained and
/// independently decodable, so T3 can concatenate per-span payloads into a
/// valid `TracesData`.
#[test]
fn each_payload_reconstructs_a_self_contained_traces_data() {
    let req = decode_traces(&read_fixture("two_spans_dual_scope.bin")).expect("fixture decodes");
    let out = parse_traces(&req, 0).expect("within the expansion budget");
    assert_eq!(out.spans.len(), 2);

    for (record, original) in out.spans.iter().zip([span_a(), span_b()]) {
        let payload =
            TracesData::decode(record.payload.as_slice()).expect("payload decodes as TracesData");
        assert_eq!(payload.resource_spans.len(), 1, "single ResourceSpans");
        let rs = &payload.resource_spans[0];
        assert_eq!(
            rs.resource,
            Some(fixture_resource()),
            "the span's own resource travels in its payload"
        );
        assert_eq!(rs.schema_url, "https://opentelemetry.io/schemas/1.21.0");
        assert_eq!(rs.scope_spans.len(), 1, "single ScopeSpans");
        let ss = &rs.scope_spans[0];
        assert_eq!(
            ss.scope,
            Some(fixture_scope()),
            "the span's own scope (incl. its payload-only attributes) travels in its payload"
        );
        assert_eq!(ss.schema_url, "https://opentelemetry.io/schemas/1.21.0");
        assert_eq!(ss.spans.len(), 1, "exactly this one span, never a sibling");
        let span = &ss.spans[0];
        assert_eq!(
            span, &original,
            "the original wire span, byte-identical fields"
        );
        assert_eq!(span.trace_id, record.trace_id.to_vec());
        assert_eq!(span.span_id, record.span_id.to_vec());
        assert_eq!(span.name, record.name);
    }
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
