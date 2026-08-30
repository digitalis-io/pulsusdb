//! Ingest-time `detected_level` (issue #483): the captured case table, and
//! the rule asserted against it on all three log-ingest paths.
//!
//! **Where the expected values come from.** Every level in this file is read
//! out of `tests/fixtures/detected_level/reference_cases.json`, which is a
//! CAPTURE from the pinned reference build — no expectation here is rendered
//! by PulsusDB code. [`the_committed_capture_matches_the_live_reference`]
//! re-pushes every row to a live reference container and fails if the
//! committed answers have drifted, so a fixture row edited to match our code
//! goes red without our code being touched.
//!
//! **What this suite is responsible for, and where that stops.** It covers
//! the rule's answer on the 80 named inputs across the three transports, at
//! the stored-string level, plus the fourteen cap rows. It proves nothing
//! about an input not in the table, compares no part of the stored metadata
//! other than the `detected_level` pair (except the five rows that assert
//! the whole string), and does not reach the read path.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

use pulsus_write::protocols::loki_push::{
    EntryAdapter, LabelPairAdapter, PushRequest, StreamAdapter, Timestamp,
};
use pulsus_write::protocols::otlp_logs;
use pulsus_write::{
    LevelDiscovery, LogIngestSettings, LogsIngestError, ParsedLogs, parse_loki_json,
    parse_loki_protobuf,
};

// ---------------------------------------------------------------------
// The source tables. Ids, inputs and the reason each row is here; the
// ANSWERS live in the committed capture, never here.
// ---------------------------------------------------------------------

/// One push-transport row: stream labels beyond `app`, the client's
/// structured metadata in wire order, and the line.
struct PushCase {
    id: &'static str,
    labels: &'static [(&'static str, &'static str)],
    metadata: &'static [(&'static str, &'static str)],
    line: &'static str,
}

const fn push(
    id: &'static str,
    labels: &'static [(&'static str, &'static str)],
    metadata: &'static [(&'static str, &'static str)],
    line: &'static str,
) -> PushCase {
    PushCase {
        id,
        labels,
        metadata,
        line,
    }
}

/// The 56 push rows. Several exist only to separate two readings of the
/// rule that agree everywhere else:
///
/// - lvl11/lvl12 are the ONLY rows separating separate-list-entries-per-
///   spelling from case-insensitive matching. lvl9 does not: a logfmt key is
///   matched case-insensitively on that path, so `SEVERITY=fatal` answers
///   `fatal` under either reading.
/// - lvl10 and e6 are a level word occurring INSIDE a longer token
///   (`information`, `xerror`).
/// - lvl16 turns on the left boundary excluding `:`; lvl17 and e5 turn on
///   the right boundary including it; lvl37 is a tab as the right boundary;
///   lvl21 is a multi-byte neighbour.
/// - lvl23/lvl24/lvl36 are the same words in different orders, which is what
///   makes earliest-wins distinguishable from list-order-wins.
/// - lvl31 against lvl32/lvl33 separates the logfmt step (list order) from
///   the JSON step (object order); lvl34 and lvl35 pin list order for labels
///   and for metadata.
/// - d1-d5 pin that metadata names are matched AFTER canonicalization;
///   e1/e2 that empty-valued pairs are deleted before the rule runs.
/// - e7 pins that a non-string JSON value does not match.
/// - u1/u4/u5 are the `İ` rows: `str::to_lowercase` answers `unknown` for
///   all three.
fn push_cases() -> Vec<PushCase> {
    vec![
        push("lvl1", &[], &[], "level=info msg=hello"),
        push("lvl2", &[], &[], r#"{"level":"ERROR","msg":"boom"}"#),
        push("lvl3", &[], &[], "WARNING disk almost full"),
        push("lvl4", &[], &[], "lvl=dbug entering loop"),
        push("lvl5", &[("level", "Info")], &[], "plain body no token"),
        push("lvl6", &[], &[], "plain line with nothing special"),
        push(
            "lvl7",
            &[],
            &[("detected_level", "WARN")],
            "level=info msg=x",
        ),
        push("lvl8", &[], &[], r#"{"severity":"critical"}"#),
        push("lvl9", &[], &[], "ts=1 SEVERITY=fatal msg=y"),
        push("lvl10", &[], &[], "the word information appears here"),
        push("lvl11", &[], &[], r#"{"lEvEl":"information"}"#),
        push("lvl12", &[("LeVeL", "warning")], &[], "nothing here"),
        push("lvl13", &[], &[], r#"{"log":{"level":"warn"}}"#),
        push("lvl14", &[], &[], r#"{"a":{"b":{"level":"information"}}}"#),
        push("lvl15", &[], &[("detected_level", "banana")], "plain"),
        push("lvl16", &[], &[], "misc:error happened"),
        push("lvl17", &[], &[], "debug: message here"),
        push("lvl18", &[], &[("severity_text", "Warning")], "plain"),
        push("lvl19", &[], &[], r#"{"log.level":"error"}"#),
        push("lvl20", &[], &[], "msg=hello error=nil"),
        push("lvl21", &[], &[], "ERROR\u{2014}disk"),
        push("lvl22", &[], &[], ""),
        push("lvl23", &[], &[], "debug and error"),
        push("lvl24", &[], &[], "error and debug"),
        push("lvl25", &[], &[("level", "error")], "level=warn"),
        push("lvl26", &[("level", "WARNING")], &[], "level=info"),
        push("lvl27", &[], &[], "trace"),
        push("lvl28", &[], &[], "[err] boom"),
        push("lvl29", &[], &[], "lvl=INF starting"),
        push("lvl30", &[], &[], r#"{"SeverityText":"Fatal"}"#),
        push("lvl31", &[], &[], "severity=critical level=error msg=z"),
        push(
            "lvl32",
            &[],
            &[],
            r#"{"severity":"critical","level":"error"}"#,
        ),
        push(
            "lvl33",
            &[],
            &[],
            r#"{"level":"error","severity":"critical"}"#,
        ),
        push(
            "lvl34",
            &[("severity", "critical"), ("level", "error")],
            &[],
            "plain",
        ),
        push(
            "lvl35",
            &[],
            &[("severity", "critical"), ("level", "error")],
            "plain",
        ),
        push("lvl36", &[], &[], "warn and error and debug"),
        push("lvl37", &[], &[], "critical\tfailure"),
        push("lvl38", &[], &[], "a=1 trace=abc"),
        push("d1", &[], &[("detected.level", "WARN")], "level=info msg=x"),
        push("d2", &[], &[("log.level", "error")], "plain body"),
        push("d3", &[], &[("log_level", "error")], "plain body"),
        push("d4", &[], &[("Severity_Text", "Fatal")], "plain body"),
        push("d5", &[], &[("severity.text", "Fatal")], "plain body"),
        push("e1", &[], &[("detected_level", "")], "level=error msg=x"),
        push("e2", &[], &[("level", "")], "level=error msg=x"),
        push("e3", &[], &[], "level= msg=x"),
        push("e4", &[], &[], r#"{"level":""}"#),
        push("e5", &[], &[], "Error:"),
        push("e6", &[], &[], "xerror error"),
        push("e7", &[], &[], r#"{"level":123}"#),
        push("e8", &[], &[], "LEVEL=WaRn msg=x"),
        push("u1", &[], &[], "\u{130}NFO started"),
        push("u2", &[], &[], "\u{212a}"),
        push("u3", &[], &[], "ERROR"),
        push("u4", &[], &[], "\u{130}NFO"),
        push("u5", &[], &[], "WARN\u{130}NG"),
    ]
}

/// One OTLP row. `severity_number` `0` is the wire's "absent"; `scope_attrs`
/// are the only part of the record's inputs PulsusDB stores.
struct OtlpCase {
    id: &'static str,
    severity_number: i32,
    severity_text: &'static str,
    record_attrs: &'static [(&'static str, &'static str)],
    scope_attrs: &'static [(&'static str, &'static str)],
    body: &'static str,
}

const fn otlp(
    id: &'static str,
    severity_number: i32,
    severity_text: &'static str,
    record_attrs: &'static [(&'static str, &'static str)],
    scope_attrs: &'static [(&'static str, &'static str)],
    body: &'static str,
) -> OtlpCase {
    OtlpCase {
        id,
        severity_number,
        severity_text,
        record_attrs,
        scope_attrs,
        body,
    }
}

/// The 24 OTLP rows. o9 and o13 are why the severity TEXT has to participate
/// at the metadata step — it beats the severity number, and an unmatched
/// text passes straight through. o11 is why the number beats the line. o1
/// against p1/p2 is why the RAW wire value must be carried: absent falls
/// through to the line, 25 and 30 answer `unknown` and never reach it. p3,
/// p4 and p7 are why record attributes must participate even though
/// PulsusDB stores none of them. p8-p11 fix the source ORDER when one name
/// arrives in two of them.
fn otlp_cases() -> Vec<OtlpCase> {
    vec![
        otlp("o1", 0, "", &[], &[], "plain body"),
        otlp("o2", 1, "", &[], &[], "plain body"),
        otlp("o3", 5, "", &[], &[], "plain body"),
        otlp("o4", 9, "", &[], &[], "plain body"),
        otlp("o5", 13, "", &[], &[], "plain body"),
        otlp("o6", 17, "", &[], &[], "plain body"),
        otlp("o7", 21, "", &[], &[], "plain body"),
        otlp("o8", 24, "", &[], &[], "plain body"),
        otlp("o9", 17, "Warning", &[], &[], "plain body"),
        otlp("o10", 0, "Debug", &[], &[], "plain body"),
        otlp("o11", 17, "", &[], &[], "level=info msg=x"),
        otlp("o12", 0, "", &[], &[], "level=trace msg=x"),
        otlp("o13", 17, "banana", &[], &[], "plain body"),
        otlp("p1", 25, "", &[], &[], "plain body"),
        otlp("p2", 30, "", &[], &[], "plain body"),
        otlp("p3", 0, "", &[("level", "warn")], &[], "plain body"),
        otlp("p4", 17, "", &[("level", "warn")], &[], "plain body"),
        otlp("p5", 0, "", &[], &[("level", "critical")], "plain body"),
        otlp(
            "p6",
            0,
            "",
            &[],
            &[("detected_level", "WARN")],
            "plain body",
        ),
        otlp(
            "p7",
            0,
            "",
            &[("detected_level", "WARN")],
            &[],
            "plain body",
        ),
        // p8-p11: the same allowed name in BOTH a scope attribute and a
        // record attribute. Plan v2 chose the scope's and marked the
        // question unmeasured; these four probes measured it against the
        // pinned reference build, the plan's choice was wrong, and the
        // implementation changed to match the answers.
        //
        // OBSERVED, and all that these rows assert: for an ordinary allowed
        // name the reference's answer is the RECORD attribute's value (p8,
        // p9); for `detected_level` its answer is the SCOPE attribute's
        // value, verbatim and un-normalized (p10, p11). p8/p9 are one probe
        // with the values swapped, so the answer follows the record in both
        // assignments and cannot be a coincidence of which value sat where.
        // Any account of the ORDER inside the reference that produces those
        // four answers is an inference — see `log_level.rs` — and no row
        // here depends on it.
        //
        // p11 is the row that separates two implementations that agree
        // everywhere else: one that writes the answer it derived from the
        // record attribute into the stored pair, and one that leaves the
        // stored pair exactly as sent. p10 alone does not separate them,
        // because an implementation reading the scope first happens to
        // answer `banana` there too — which is why both rows are here.
        otlp(
            "p8",
            0,
            "",
            &[("level", "warn")],
            &[("level", "critical")],
            "plain body",
        ),
        otlp(
            "p9",
            0,
            "",
            &[("level", "critical")],
            &[("level", "warn")],
            "plain body",
        ),
        otlp(
            "p10",
            0,
            "",
            &[("detected_level", "WARN")],
            &[("detected_level", "banana")],
            "plain body",
        ),
        otlp(
            "p11",
            0,
            "",
            &[("detected_level", "banana")],
            &[("detected_level", "WARN")],
            "plain body",
        ),
    ]
}

// ---------------------------------------------------------------------
// The committed artifact.
// ---------------------------------------------------------------------

const ARTIFACT_IMAGE: &str = "grafana/loki:3.7.4";
const ARTIFACT_VERSION: &str = "3.7.4";
/// The reference commit this repo's conformance suites are pinned to.
const ARTIFACT_REVISION: &str = "b318f282";

#[derive(Serialize, Deserialize)]
struct Artifact {
    image: String,
    config: String,
    push_endpoint: String,
    otlp_endpoint: String,
    query_endpoint: String,
    read_headers: Vec<String>,
    /// The container's `/loki/api/v1/status/buildinfo` response.
    buildinfo: Json,
    captured_at_unix: u64,
    push_cases: Vec<CapturedLevel>,
    otlp_cases: Vec<CapturedLevel>,
    cap_cases: Vec<CapturedCap>,
}

/// One captured row: what the reference stored under `detected_level` for
/// this case's input.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct CapturedLevel {
    id: String,
    /// The nonce'd selector the case was pushed under.
    selector: String,
    push_status: u16,
    /// The whole `structuredMetadata` object the reference returned, so a
    /// reader can see what else the entry carried.
    structured_metadata: Json,
    /// The `detected_level` value, extracted from the object above.
    detected_level: String,
}

/// One captured cap row: the reference's accept/reject answer, and its error
/// text when it rejects.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct CapturedCap {
    id: String,
    push_status: u16,
    error: String,
}

fn artifact_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/detected_level/reference_cases.json")
}

fn load_artifact() -> Artifact {
    let path = artifact_path();
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

fn expected_push(art: &Artifact) -> BTreeMap<&str, &str> {
    art.push_cases
        .iter()
        .map(|c| (c.id.as_str(), c.detected_level.as_str()))
        .collect()
}

fn expected_otlp(art: &Artifact) -> BTreeMap<&str, &str> {
    art.otlp_cases
        .iter()
        .map(|c| (c.id.as_str(), c.detected_level.as_str()))
        .collect()
}

// ---------------------------------------------------------------------
// Our side: the real receivers, over real wire bodies.
// ---------------------------------------------------------------------

const PUSH_TS_NS: i64 = 1_788_099_000_000_000_000;

/// The structured-metadata JSON object of a push body, assembled as TEXT so
/// a repeated name would survive — a `serde_json::Map` would collapse it.
fn sm_object(sm: &[(&str, &str)]) -> String {
    let inner: Vec<String> = sm
        .iter()
        .map(|(k, v)| {
            format!(
                "{}:{}",
                serde_json::to_string(k).expect("string"),
                serde_json::to_string(v).expect("string")
            )
        })
        .collect();
    format!("{{{}}}", inner.join(","))
}

fn label_object(app: &str, labels: &[(&str, &str)]) -> String {
    let mut inner = vec![format!(
        "\"app\":{}",
        serde_json::to_string(app).expect("s")
    )];
    for (k, v) in labels {
        inner.push(format!(
            "{}:{}",
            serde_json::to_string(k).expect("s"),
            serde_json::to_string(v).expect("s")
        ));
    }
    format!("{{{}}}", inner.join(","))
}

fn push_json_body(app: &str, case: &PushCase, ts_ns: i64) -> String {
    format!(
        r#"{{"streams":[{{"stream":{},"values":[[{},{},{}]]}}]}}"#,
        label_object(app, case.labels),
        serde_json::to_string(&ts_ns.to_string()).expect("s"),
        serde_json::to_string(case.line).expect("s"),
        sm_object(case.metadata),
    )
}

fn push_protobuf_request(app: &str, case: &PushCase, ts_ns: i64) -> PushRequest {
    let mut labels = format!("{{app=\"{app}\"");
    for (k, v) in case.labels {
        labels.push_str(&format!(", {k}=\"{v}\""));
    }
    labels.push('}');
    PushRequest {
        streams: vec![StreamAdapter {
            labels,
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: ts_ns / 1_000_000_000,
                    nanos: (ts_ns % 1_000_000_000) as i32,
                }),
                line: case.line.to_string(),
                structured_metadata: case
                    .metadata
                    .iter()
                    .map(|(k, v)| LabelPairAdapter {
                        name: k.to_string(),
                        value: v.to_string(),
                    })
                    .collect(),
            }],
        }],
    }
}

fn otlp_request(selector: &str, case: &OtlpCase) -> ExportLogsServiceRequest {
    let attr = |k: &str, v: &str| KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(v.to_string())),
        }),
        key_strindex: 0,
    };
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("service.name", selector)],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "sc".to_string(),
                    attributes: case.scope_attrs.iter().map(|(k, v)| attr(k, v)).collect(),
                    ..Default::default()
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: PUSH_TS_NS as u64,
                    severity_number: case.severity_number,
                    severity_text: case.severity_text.to_string(),
                    attributes: case.record_attrs.iter().map(|(k, v)| attr(k, v)).collect(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue(case.body.to_string())),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn settings(discovery: LevelDiscovery) -> LogIngestSettings {
    LogIngestSettings {
        discover_log_levels: discovery == LevelDiscovery::On,
    }
}

/// The stored `log_samples.structured_metadata` string for one push row, on
/// the JSON transport.
fn stored_json(case: &PushCase, discovery: LevelDiscovery) -> String {
    let body = push_json_body("probe", case, PUSH_TS_NS);
    let out = parse_loki_json(body.as_bytes(), 0, discovery)
        .unwrap_or_else(|e| panic!("{}: PulsusDB rejected the push body: {e}", case.id));
    one_row(&out, case.id)
}

/// The same row on the protobuf transport.
fn stored_protobuf(case: &PushCase, discovery: LevelDiscovery) -> String {
    let req = push_protobuf_request("probe", case, PUSH_TS_NS);
    let out = parse_loki_protobuf(&req, 0, discovery)
        .unwrap_or_else(|e| panic!("{}: PulsusDB rejected the protobuf body: {e}", case.id));
    one_row(&out, case.id)
}

/// The stored string for one OTLP row.
fn stored_otlp(case: &OtlpCase, discovery: LevelDiscovery) -> String {
    let req = otlp_request("probe", case);
    let out = otlp_logs::parse(&req, 0, settings(discovery))
        .unwrap_or_else(|e| panic!("{}: PulsusDB rejected the OTLP body: {e}", case.id));
    one_row(&out, case.id)
}

fn one_row(out: &ParsedLogs, id: &str) -> String {
    assert_eq!(out.rows.len(), 1, "{id}: expected exactly one row");
    out.rows[0].structured_metadata.clone()
}

/// The `detected_level` value inside a stored structured-metadata string, or
/// `None` when the string carries no such pair.
fn level_of(stored: &str, id: &str) -> Option<String> {
    if stored.is_empty() {
        return None;
    }
    let value: Json =
        serde_json::from_str(stored).unwrap_or_else(|e| panic!("{id}: stored {stored:?}: {e}"));
    value.get("detected_level").map(|v| {
        v.as_str()
            .unwrap_or_else(|| panic!("{id}: detected_level is not a string"))
            .to_string()
    })
}

// ---------------------------------------------------------------------
// Criterion 1 / 12 — the push table, both encodings, asserted separately.
// ---------------------------------------------------------------------

/// Reports EVERY row that disagrees, not just the first. A permutation of
/// the level tables moves several rows at once, and which ones is the whole
/// evidence that the table is pinned by value rather than by membership.
fn assert_all_rows_match(mismatches: Vec<String>, what: &str) {
    assert!(
        mismatches.is_empty(),
        "{what}: {} row(s) disagree with the captured reference answers:\n  {}",
        mismatches.len(),
        mismatches.join("\n  ")
    );
}

#[test]
fn push_cases_json_transport_matches_the_reference_capture() {
    let art = load_artifact();
    let expected = expected_push(&art);
    let mut mismatches = Vec::new();
    for case in push_cases() {
        let want = expected
            .get(case.id)
            .unwrap_or_else(|| panic!("{}: no captured answer", case.id));
        let stored = stored_json(&case, LevelDiscovery::On);
        let got = level_of(&stored, case.id);
        if got.as_deref() != Some(*want) {
            mismatches.push(format!(
                "{}: want {want:?}, got {got:?} — line {:?}, metadata {:?}, labels {:?}",
                case.id, case.line, case.metadata, case.labels
            ));
        }
    }
    assert_all_rows_match(mismatches, "push table, JSON transport");
}

#[test]
fn push_cases_protobuf_transport_matches_the_reference_capture() {
    let art = load_artifact();
    let expected = expected_push(&art);
    let mut mismatches = Vec::new();
    for case in push_cases() {
        let want = expected
            .get(case.id)
            .unwrap_or_else(|| panic!("{}: no captured answer", case.id));
        let stored = stored_protobuf(&case, LevelDiscovery::On);
        let got = level_of(&stored, case.id);
        if got.as_deref() != Some(*want) {
            mismatches.push(format!("{}: want {want:?}, got {got:?}", case.id));
        }
    }
    assert_all_rows_match(mismatches, "push table, protobuf transport");
}

#[test]
fn the_two_push_encodings_store_byte_identical_strings() {
    for case in push_cases() {
        assert_eq!(
            stored_json(&case, LevelDiscovery::On),
            stored_protobuf(&case, LevelDiscovery::On),
            "{}: the two push encodings must store the same string",
            case.id
        );
    }
}

#[test]
fn otlp_cases_match_the_reference_capture() {
    let art = load_artifact();
    let expected = expected_otlp(&art);
    let mut mismatches = Vec::new();
    for case in otlp_cases() {
        let want = expected
            .get(case.id)
            .unwrap_or_else(|| panic!("{}: no captured answer", case.id));
        let stored = stored_otlp(&case, LevelDiscovery::On);
        let got = level_of(&stored, case.id);
        if got.as_deref() != Some(*want) {
            mismatches.push(format!(
                "{}: want {want:?}, got {got:?} — severity {} text {:?} body {:?}",
                case.id, case.severity_number, case.severity_text, case.body
            ));
        }
    }
    assert_all_rows_match(mismatches, "OTLP table");
}

// ---------------------------------------------------------------------
// Criterion 4 — every entry gets a value.
// ---------------------------------------------------------------------

#[test]
fn always_present_every_row_carries_a_detected_level() {
    for case in push_cases() {
        for stored in [
            stored_json(&case, LevelDiscovery::On),
            stored_protobuf(&case, LevelDiscovery::On),
        ] {
            assert!(
                level_of(&stored, case.id).is_some(),
                "{}: no detected_level in {stored:?}",
                case.id
            );
        }
    }
    for case in otlp_cases() {
        let stored = stored_otlp(&case, LevelDiscovery::On);
        assert!(
            level_of(&stored, case.id).is_some(),
            "{}: no detected_level in {stored:?}",
            case.id
        );
    }
}

// ---------------------------------------------------------------------
// Criterion 5 — a pre-existing pair is normalized IN PLACE.
// ---------------------------------------------------------------------

/// The whole stored string, not just the extracted value: an implementation
/// that APPENDS beside the client's pair rather than rewriting it stores two
/// pairs whose canonical names collide, and the tie-break then silently
/// picks one.
#[test]
fn a_pre_existing_pair_is_normalized_in_place_and_not_appended_beside() {
    let cases = push_cases();
    let by_id: BTreeMap<&str, &PushCase> = cases.iter().map(|c| (c.id, c)).collect();
    for (id, expected) in [
        ("lvl7", r#"{"detected_level":"warn"}"#),
        ("lvl15", r#"{"detected_level":"banana"}"#),
        ("d1", r#"{"detected_level":"warn"}"#),
    ] {
        let case = by_id[id];
        assert_eq!(
            stored_json(case, LevelDiscovery::On),
            expected,
            "{id}: JSON"
        );
        assert_eq!(
            stored_protobuf(case, LevelDiscovery::On),
            expected,
            "{id}: protobuf"
        );
    }
    // p6 carries the pair in the scope's own metadata, so the whole string
    // also holds the scope identity; p7 carries it as a RECORD attribute,
    // which PulsusDB does not store, so the pair is appended there.
    let otlp = otlp_cases();
    let by_id: BTreeMap<&str, &OtlpCase> = otlp.iter().map(|c| (c.id, c)).collect();
    assert_eq!(
        stored_otlp(by_id["p6"], LevelDiscovery::On),
        r#"{"detected_level":"warn","scope_name":"sc"}"#
    );
    assert_eq!(
        stored_otlp(by_id["p7"], LevelDiscovery::On),
        r#"{"detected_level":"warn","scope_name":"sc"}"#
    );
}

// ---------------------------------------------------------------------
// Criterion 6 — the knob turns the whole rule off.
// ---------------------------------------------------------------------

/// With the knob off no entry gains a pair, and a client-supplied one is
/// stored EXACTLY as sent — step 1 is gated too. lvl7 is what separates
/// "gated entirely" from "gated partially": a knob that only skips the
/// append would still normalize `WARN` to `warn` here.
#[test]
fn discovery_off_adds_nothing_and_normalizes_nothing() {
    let client_supplied: BTreeMap<&str, &str> =
        [("lvl7", "WARN"), ("lvl15", "banana"), ("d1", "WARN")]
            .into_iter()
            .collect();
    for case in push_cases() {
        for (transport, stored) in [
            ("json", stored_json(&case, LevelDiscovery::Off)),
            ("protobuf", stored_protobuf(&case, LevelDiscovery::Off)),
        ] {
            assert_eq!(
                level_of(&stored, case.id).as_deref(),
                client_supplied.get(case.id).copied(),
                "{} ({transport}): stored {stored:?}",
                case.id
            );
        }
    }
    // The OTLP rows whose SCOPE metadata carries a `detected_level`: those
    // pairs are stored, and with the knob off they are stored exactly as
    // sent. p7's is a RECORD attribute, which PulsusDB never stores, so it
    // leaves nothing behind either way.
    let scope_supplied: BTreeMap<&str, &str> = [("p6", "WARN"), ("p10", "banana"), ("p11", "WARN")]
        .into_iter()
        .collect();
    for case in otlp_cases() {
        let stored = stored_otlp(&case, LevelDiscovery::Off);
        assert_eq!(
            level_of(&stored, case.id).as_deref(),
            scope_supplied.get(case.id).copied(),
            "{}: stored {stored:?}",
            case.id
        );
    }
}

// ---------------------------------------------------------------------
// Criterion 7 — the lowercasing rule.
// ---------------------------------------------------------------------

/// `str::to_lowercase` applies FULL Unicode lowercasing, under which U+0130
/// expands to two characters and the substring `info` disappears; all three
/// of these lines then answer `unknown`. The reference lowercases rune by
/// rune with the simple mapping and answers `info`, `info`, `warn`.
///
/// This case exists so the first-character rule cannot be quietly reverted
/// by someone tidying the function later.
#[test]
fn lowercasing_uses_the_simple_case_mapping_not_full_unicode() {
    let art = load_artifact();
    let expected = expected_push(&art);
    let cases = push_cases();
    let by_id: BTreeMap<&str, &PushCase> = cases.iter().map(|c| (c.id, c)).collect();
    for (id, line) in [
        ("u1", "\u{130}NFO started"),
        ("u4", "\u{130}NFO"),
        ("u5", "WARN\u{130}NG"),
    ] {
        let case = by_id[id];
        assert_eq!(case.line, line, "{id}: the literal input must not drift");
        assert_eq!(
            level_of(&stored_json(case, LevelDiscovery::On), id).as_deref(),
            expected.get(id).copied(),
            "{id}: {line:?}"
        );
        // And the naive form really is wrong on this input, so the case is
        // not merely restating what any implementation would do.
        assert!(
            !line.to_lowercase().contains("info") || id == "u5",
            "{id}: str::to_lowercase unexpectedly preserves the level word"
        );
    }
}

// ---------------------------------------------------------------------
// Criterion 11 — composition with the other structured-metadata rules,
// with discovery ON. The in-file suites in `loki_push.rs` run with it off.
// ---------------------------------------------------------------------

fn stored_raw(metadata: &[(&str, &str)], line: &str, discovery: LevelDiscovery) -> String {
    let body = format!(
        r#"{{"streams":[{{"stream":{{"app":"probe"}},"values":[[{},{},{}]]}}]}}"#,
        serde_json::to_string(&PUSH_TS_NS.to_string()).expect("s"),
        serde_json::to_string(line).expect("s"),
        sm_object(metadata),
    );
    let out = parse_loki_json(body.as_bytes(), 0, discovery).expect("admissible push body");
    out.rows[0].structured_metadata.clone()
}

#[test]
fn discovery_on_composes_with_the_empty_value_strip() {
    // The injected pair is added AFTER the resolve seam, so the empty-value
    // rule cannot delete it and the entry is left carrying only the level.
    assert_eq!(
        stored_raw(&[("a", "")], "level=warn msg=x", LevelDiscovery::On),
        r#"{"detected_level":"warn"}"#
    );
}

#[test]
fn discovery_on_composes_with_the_rename_collision_resolution() {
    // `a.b` renames onto `a_b` and replaces the base twin (issue #381 row
    // c01); the injected pair sorts after both and does not disturb it.
    assert_eq!(
        stored_raw(
            &[("a.b", "x"), ("a_b", "keep")],
            "level=error msg=x",
            LevelDiscovery::On
        ),
        r#"{"a_b":"x","detected_level":"error"}"#
    );
}

#[test]
fn discovery_on_composes_with_the_replacement_character_rewrite() {
    assert_eq!(
        stored_raw(
            &[("a_b", "p\u{FFFD}q")],
            "level=info msg=x",
            LevelDiscovery::On
        ),
        r#"{"a_b":"p q","detected_level":"info"}"#
    );
}

#[test]
fn discovery_on_composes_with_the_per_stream_label_bounds() {
    // A stream breaching a per-stream label bound is dropped whole, level
    // detection or not — the level is per ENTRY and the bound is per STREAM.
    let long = "v".repeat(2049);
    let body = format!(
        r#"{{"streams":[{{"stream":{{"app":"probe","big":{}}},"values":[["1","level=info"]]}}]}}"#,
        serde_json::to_string(&long).expect("s")
    );
    let out = parse_loki_json(body.as_bytes(), 0, LevelDiscovery::On).expect("stream-local drop");
    assert!(out.rows.is_empty(), "the over-wide stream must be dropped");
    assert_eq!(out.stream_errors.len(), 1);
}

// ---------------------------------------------------------------------
// Criterion 10 — the caps.
// ---------------------------------------------------------------------

/// A cap row: the push body it sends, and what PulsusDB must answer.
struct CapCase {
    id: &'static str,
    sm: Vec<(String, String)>,
    line: &'static str,
}

const BYTE_LIMIT: usize = 64 * 1024;
const COUNT_LIMIT: usize = 256;

fn cap_cases() -> Vec<CapCase> {
    let filler = |n: usize| -> Vec<(String, String)> {
        (0..n)
            .map(|i| (format!("p{i:04}"), "1".to_string()))
            .collect()
    };
    vec![
        CapCase {
            id: "c1",
            sm: vec![("x".to_string(), "v".repeat(BYTE_LIMIT - 1))],
            line: "line",
        },
        CapCase {
            id: "c2",
            sm: vec![("x".to_string(), "v".repeat(BYTE_LIMIT))],
            line: "line",
        },
        CapCase {
            id: "c3",
            sm: vec![("detected_level".to_string(), "v".repeat(200_000 - 14))],
            line: "line",
        },
        CapCase {
            id: "c4",
            sm: vec![("detected.level".to_string(), "v".repeat(200_000 - 14))],
            line: "line",
        },
        CapCase {
            id: "c5",
            sm: vec![(
                "detected_level".to_string(),
                "v".repeat(BYTE_LIMIT + 1 - 14),
            )],
            line: "line",
        },
        CapCase {
            id: "c6",
            sm: vec![("detected.level".to_string(), "warn".to_string())],
            line: "x",
        },
        CapCase {
            id: "c7",
            sm: filler(128),
            line: "line",
        },
        CapCase {
            id: "c8",
            sm: filler(129),
            line: "line",
        },
        CapCase {
            id: "c9",
            sm: {
                let mut v = filler(127);
                v.push(("detected_level".to_string(), "warn".to_string()));
                v
            },
            line: "line",
        },
        CapCase {
            id: "c10",
            sm: {
                let mut v = filler(128);
                v.push(("detected_level".to_string(), "warn".to_string()));
                v
            },
            line: "line",
        },
    ]
}

fn parse_cap(case: &CapCase) -> Result<ParsedLogs, LogsIngestError> {
    let pairs: Vec<(&str, &str)> = case
        .sm
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let body = format!(
        r#"{{"streams":[{{"stream":{{"app":"probe"}},"values":[[{},{},{}]]}}]}}"#,
        serde_json::to_string(&PUSH_TS_NS.to_string()).expect("s"),
        serde_json::to_string(case.line).expect("s"),
        sm_object(&pairs),
    );
    parse_loki_json(body.as_bytes(), 0, LevelDiscovery::On)
}

/// **The two cap properties.**
///
/// (a) The injected pair is charged against NEITHER cap: an entry at exactly
/// either limit is still accepted and comes back carrying one more pair.
/// Charging it would flip an entry at either cap from accepted to rejected,
/// an accept-surface change no other test covers.
///
/// (b) The BYTE cap exempts a pair whose RAW name is `detected_level`, and
/// only the byte cap. c3 accepted against c4 rejected is the discriminating
/// pair: without c4 the check would also pass on an implementation that
/// simply raised the cap.
///
/// **Scope note, which must travel with any quotation of this test:** rows
/// c8 and c10 are where our count cap of 256 and the reference's default of
/// 128 legitimately differ — the reference answers `400` there and we answer
/// accepted. They are PulsusDB no-regression rows and may NOT be cited as
/// reference agreement. The BYTE caps do coincide at 65 536.
#[test]
fn caps_charge_the_clients_pairs_only_and_exempt_the_level_name_from_bytes() {
    let cases = cap_cases();
    let by_id: BTreeMap<&str, &CapCase> = cases.iter().map(|c| (c.id, c)).collect();

    // (a) exactly at the byte cap, and one byte over.
    let at_byte_cap = parse_cap(by_id["c1"]).expect("c1: an entry at the byte cap is accepted");
    assert_eq!(
        level_of(&at_byte_cap.rows[0].structured_metadata, "c1").as_deref(),
        Some("unknown"),
        "c1: the injected pair is not charged"
    );
    assert!(matches!(
        parse_cap(by_id["c2"]),
        Err(LogsIngestError::OversizeMessage {
            field: "structured_metadata_bytes",
            limit: BYTE_LIMIT,
            actual: 65_537,
        })
    ));

    // (b) the byte exemption is by the RAW name.
    parse_cap(by_id["c3"]).expect("c3: a large client detected_level is not charged bytes");
    assert!(
        matches!(
            parse_cap(by_id["c4"]),
            Err(LogsIngestError::OversizeMessage {
                field: "structured_metadata_bytes",
                limit: BYTE_LIMIT,
                actual: 200_000,
            })
        ),
        "c4: the same value under a differently-named key must be rejected — \
         without this half the check also passes on an implementation that simply \
         raised the cap"
    );
    parse_cap(by_id["c5"]).expect("c5: just over the byte cap, but under the exempt name");

    // c6: a client pair canonicalizing onto the name is normalized.
    let c6 = parse_cap(by_id["c6"]).expect("c6 accepted");
    assert_eq!(
        c6.rows[0].structured_metadata,
        r#"{"detected_level":"warn"}"#
    );

    // c7-c10: our count cap is 256, so all four are accepted here.
    for id in ["c7", "c8", "c9", "c10"] {
        parse_cap(by_id[id])
            .unwrap_or_else(|e| panic!("{id}: PulsusDB accepts up to 256 pairs: {e}"));
    }

    // The four PulsusDB-only count rows. The count charge still counts a
    // `detected_level` pair — the exemption is the byte cap's alone.
    let filler = |n: usize| -> Vec<(String, String)> {
        (0..n)
            .map(|i| (format!("p{i:04}"), "1".to_string()))
            .collect()
    };
    let at_count_cap = parse_cap(&CapCase {
        id: "count256",
        sm: filler(COUNT_LIMIT),
        line: "line",
    })
    .expect("256 pairs are accepted");
    let stored: Json = serde_json::from_str(&at_count_cap.rows[0].structured_metadata).expect("j");
    assert_eq!(
        stored.as_object().expect("object").len(),
        COUNT_LIMIT + 1,
        "an entry at the count cap comes back carrying one more pair"
    );
    assert!(matches!(
        parse_cap(&CapCase {
            id: "count257",
            sm: filler(COUNT_LIMIT + 1),
            line: "line",
        }),
        Err(LogsIngestError::OversizeMessage {
            field: "structured_metadata",
            limit: COUNT_LIMIT,
            actual: 257,
        })
    ));
    let mut with_level = filler(COUNT_LIMIT - 1);
    with_level.push(("detected_level".to_string(), "warn".to_string()));
    parse_cap(&CapCase {
        id: "count256_with_level",
        sm: with_level,
        line: "line",
    })
    .expect("256 pairs, one of them a client detected_level, are accepted");
    let mut over_with_level = filler(COUNT_LIMIT);
    over_with_level.push(("detected_level".to_string(), "warn".to_string()));
    assert!(
        matches!(
            parse_cap(&CapCase {
                id: "count257_with_level",
                sm: over_with_level,
                line: "line",
            }),
            Err(LogsIngestError::OversizeMessage {
                field: "structured_metadata",
                limit: COUNT_LIMIT,
                actual: 257,
            })
        ),
        "the COUNT charge still counts a client detected_level"
    );
}

/// The reference's own accept/reject answers for the ten cap rows, read out
/// of the capture rather than restated here. c8 and c10 are the two rows
/// where the two stores legitimately differ.
#[test]
fn the_capture_records_the_references_cap_answers() {
    let art = load_artifact();
    let by_id: BTreeMap<&str, &CapturedCap> =
        art.cap_cases.iter().map(|c| (c.id.as_str(), c)).collect();
    for case in cap_cases() {
        assert!(
            by_id.contains_key(case.id),
            "{}: the capture carries no cap answer",
            case.id
        );
    }
    assert_eq!(
        by_id["c3"].push_status, 204,
        "c3: accepted by the reference"
    );
    assert_eq!(
        by_id["c4"].push_status, 400,
        "c4: rejected by the reference"
    );
    for id in ["c8", "c10"] {
        assert_eq!(
            by_id[id].push_status, 400,
            "{id}: the reference's default count cap is 128 and ours is 256 — this row is \
             a PulsusDB no-regression row, not reference agreement"
        );
    }
}

// ---------------------------------------------------------------------
// Live: the only writer of the artifact, and the drift check.
// ---------------------------------------------------------------------

fn curl(args: &[&str]) -> String {
    let out = Command::new("curl")
        .args(["-s", "--max-time", "30"])
        .args(args)
        .output()
        .expect("curl must be on PATH");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn now_unix() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("clock")
}

fn post_json(base_url: &str, path: &str, body: &str) -> (u16, String) {
    // The body goes through a file, not argv: two cap rows carry a 200 000
    // byte value and `--data-binary <literal>` exceeds the argument-list
    // limit.
    let file = std::env::temp_dir().join(format!(
        "pulsus-483-body-{}-{}.json",
        std::process::id(),
        now_unix().as_nanos()
    ));
    std::fs::write(&file, body).expect("write the push body");
    let raw = curl(&[
        "-w",
        "\n%{http_code}",
        "-H",
        "Content-Type: application/json",
        "-X",
        "POST",
        "--data-binary",
        &format!("@{}", file.display()),
        &format!("{base_url}{path}"),
    ]);
    let _ = std::fs::remove_file(&file);
    let (body, status) = raw.rsplit_once('\n').unwrap_or((raw.as_str(), "0"));
    (status.trim().parse::<u16>().unwrap_or(0), body.to_string())
}

/// Reads one nonce'd stream back, polling until the entry is visible.
fn read_back(base_url: &str, selector: &str, ts_ns: i64) -> Json {
    let start = (ts_ns / 1_000_000_000 - 3600).to_string();
    let end = (ts_ns / 1_000_000_000 + 3600).to_string();
    let mut response = Json::Null;
    for _ in 0..40 {
        let raw = curl(&[
            "-G",
            "-H",
            "X-Loki-Response-Encoding-Flags: categorize-labels",
            "--data-urlencode",
            &format!("query={selector}"),
            "--data-urlencode",
            &format!("start={start}"),
            "--data-urlencode",
            &format!("end={end}"),
            "--data-urlencode",
            "limit=10",
            &format!("{base_url}/loki/api/v1/query_range"),
        ]);
        response = serde_json::from_str(&raw).unwrap_or(Json::Null);
        if response["data"]["result"]
            .as_array()
            .is_some_and(|r| !r.is_empty())
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    response
}

fn captured_from(id: &str, selector: &str, push_status: u16, response: &Json) -> CapturedLevel {
    let sm = response["data"]["result"][0]["values"][0][2]["structuredMetadata"].clone();
    let detected_level = sm["detected_level"]
        .as_str()
        .unwrap_or_else(|| {
            panic!("{id}: the reference returned no detected_level; response {response}")
        })
        .to_string();
    CapturedLevel {
        id: id.to_string(),
        selector: selector.to_string(),
        push_status,
        structured_metadata: sm,
        detected_level,
    }
}

fn capture_push(base_url: &str, nonce: u64, case: &PushCase) -> CapturedLevel {
    let app = format!("dl{nonce}-{}", case.id);
    let ts_ns = now_unix().as_nanos() as i64;
    let (push_status, _) = post_json(
        base_url,
        "/loki/api/v1/push",
        &push_json_body(&app, case, ts_ns),
    );
    assert_eq!(push_status, 204, "{}: push rejected", case.id);
    let selector = format!(r#"{{app="{app}"}}"#);
    let response = read_back(base_url, &selector, ts_ns);
    captured_from(case.id, &selector, push_status, &response)
}

fn otlp_body(selector: &str, case: &OtlpCase, ts_ns: i64) -> String {
    let attrs = |pairs: &[(&str, &str)]| -> Vec<Json> {
        pairs
            .iter()
            .map(|(k, v)| json!({"key": k, "value": {"stringValue": v}}))
            .collect()
    };
    let mut record = json!({
        "timeUnixNano": ts_ns.to_string(),
        "body": {"stringValue": case.body},
        "attributes": attrs(case.record_attrs),
    });
    if case.severity_number != 0 {
        record["severityNumber"] = json!(case.severity_number);
    }
    if !case.severity_text.is_empty() {
        record["severityText"] = json!(case.severity_text);
    }
    json!({"resourceLogs": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": selector}}]},
        "scopeLogs": [{
            "scope": {"name": "sc", "attributes": attrs(case.scope_attrs)},
            "logRecords": [record],
        }],
    }]})
    .to_string()
}

fn capture_otlp(base_url: &str, nonce: u64, case: &OtlpCase) -> CapturedLevel {
    let service = format!("dl{nonce}-{}", case.id);
    let ts_ns = now_unix().as_nanos() as i64;
    let (push_status, body) =
        post_json(base_url, "/otlp/v1/logs", &otlp_body(&service, case, ts_ns));
    assert!(
        (200..300).contains(&push_status),
        "{}: OTLP push rejected {push_status} {body}",
        case.id
    );
    let selector = format!(r#"{{service_name="{service}"}}"#);
    let response = read_back(base_url, &selector, ts_ns);
    captured_from(case.id, &selector, push_status, &response)
}

fn capture_cap(base_url: &str, nonce: u64, case: &CapCase) -> CapturedCap {
    let app = format!("dc{nonce}-{}", case.id);
    let ts_ns = now_unix().as_nanos() as i64;
    let pairs: Vec<(&str, &str)> = case
        .sm
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let body = format!(
        r#"{{"streams":[{{"stream":{{"app":"{app}"}},"values":[[{},{},{}]]}}]}}"#,
        serde_json::to_string(&ts_ns.to_string()).expect("s"),
        serde_json::to_string(case.line).expect("s"),
        sm_object(&pairs),
    );
    let (push_status, error) = post_json(base_url, "/loki/api/v1/push", &body);
    CapturedCap {
        id: case.id.to_string(),
        push_status,
        error: error.trim().to_string(),
    }
}

/// **Criterion 3 — the fixture is a capture, not our output.**
///
/// Drift mode (default): re-pushes every push row, every OTLP row and every
/// cap row to the live reference and asserts the fresh answers equal the
/// committed ones. Regen mode (`PULSUS_REGEN_DETECTED_LEVEL_CAPTURE=1`)
/// rewrites the artifact instead, and refuses any container that does not
/// report the pinned version AND revision.
///
/// Self-skips with `PULSUSDB_LOGQL_DIFF_URL` unset, which is why
/// `harness_positions_are_recorded_and_the_ci_step_exists` asserts the CI
/// step that supplies it exists.
#[test]
fn the_committed_capture_matches_the_live_reference() {
    let Ok(base_url) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the detected_level capture leg");
        return;
    };
    let buildinfo: Json = serde_json::from_str(&curl(&[&format!(
        "{base_url}/loki/api/v1/status/buildinfo"
    )]))
    .expect("buildinfo must parse — is the reference container up?");

    let nonce = now_unix().as_secs();
    let fresh_push: Vec<CapturedLevel> = push_cases()
        .iter()
        .map(|c| capture_push(&base_url, nonce, c))
        .collect();
    let fresh_otlp: Vec<CapturedLevel> = otlp_cases()
        .iter()
        .map(|c| capture_otlp(&base_url, nonce, c))
        .collect();
    let fresh_caps: Vec<CapturedCap> = cap_cases()
        .iter()
        .map(|c| capture_cap(&base_url, nonce, c))
        .collect();

    if std::env::var("PULSUS_REGEN_DETECTED_LEVEL_CAPTURE").as_deref() == Ok("1") {
        assert_eq!(
            buildinfo["version"].as_str(),
            Some(ARTIFACT_VERSION),
            "regeneration requires the pinned reference ({ARTIFACT_IMAGE}); refusing to \
             capture from {buildinfo}"
        );
        assert_eq!(
            buildinfo["revision"].as_str(),
            Some(ARTIFACT_REVISION),
            "regeneration requires the pinned reference revision; refusing to capture from \
             {buildinfo}"
        );
        let artifact = Artifact {
            image: ARTIFACT_IMAGE.to_string(),
            config: "ci/logql/config.yaml".to_string(),
            push_endpoint: "/loki/api/v1/push".to_string(),
            otlp_endpoint: "/otlp/v1/logs".to_string(),
            query_endpoint: "/loki/api/v1/query_range".to_string(),
            read_headers: vec!["X-Loki-Response-Encoding-Flags: categorize-labels".to_string()],
            buildinfo,
            captured_at_unix: nonce,
            push_cases: fresh_push,
            otlp_cases: fresh_otlp,
            cap_cases: fresh_caps,
        };
        let path = artifact_path();
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        let text = serde_json::to_string_pretty(&artifact).expect("serialize") + "\n";
        std::fs::write(&path, text).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
        eprintln!("regenerated {path:?} from {base_url} — review the diff");
        return;
    }

    let committed = load_artifact();
    let want_push: BTreeMap<&str, &str> = expected_push(&committed);
    for fresh in &fresh_push {
        assert_eq!(
            Some(fresh.detected_level.as_str()),
            want_push.get(fresh.id.as_str()).copied(),
            "{}: the live reference answers differently than the committed capture — if the \
             reference genuinely changed, regenerate with \
             PULSUS_REGEN_DETECTED_LEVEL_CAPTURE=1 against {ARTIFACT_IMAGE} and review the diff",
            fresh.id
        );
    }
    let want_otlp: BTreeMap<&str, &str> = expected_otlp(&committed);
    for fresh in &fresh_otlp {
        assert_eq!(
            Some(fresh.detected_level.as_str()),
            want_otlp.get(fresh.id.as_str()).copied(),
            "{}: the live reference's OTLP answer differs from the committed capture",
            fresh.id
        );
    }
    let want_caps: BTreeMap<&str, u16> = committed
        .cap_cases
        .iter()
        .map(|c| (c.id.as_str(), c.push_status))
        .collect();
    for fresh in &fresh_caps {
        assert_eq!(
            Some(fresh.push_status),
            want_caps.get(fresh.id.as_str()).copied(),
            "{}: the live reference's cap answer differs from the committed capture",
            fresh.id
        );
    }
}

// ---------------------------------------------------------------------
// Criterion 14 — the written positions, the harness symmetry, and the
// CI step that makes the live leg above more than a self-skip.
// ---------------------------------------------------------------------

fn repo_file(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

/// The two written positions this change reverses are AMENDED, never
/// deleted: the original sentence stays, and the reversal, its date and the
/// second fact — that the harness setting stays off because that oracle
/// implements an older, materially different rule — are added beside it.
///
/// This is a TEXT assertion. It proves the text is present; it cannot prove
/// the text is accurate, and no test can.
#[test]
fn harness_positions_are_recorded_and_the_ci_step_exists() {
    let loki_yaml = repo_file("deploy/e2e/loki.yaml");
    assert!(
        loki_yaml.contains(
            "otherwise the oracle injects a\n#     `detected_level` structured-metadata field per entry"
        ),
        "deploy/e2e/loki.yaml must keep the original written position verbatim"
    );
    assert!(
        loki_yaml.contains("discover_log_levels: false"),
        "deploy/e2e/loki.yaml must keep the oracle's setting off"
    );
    assert!(
        loki_yaml.contains("2026-08-30"),
        "deploy/e2e/loki.yaml must record the date the position was reversed"
    );

    let sm_corpus = repo_file("e2e/src/logs_sm_corpus.rs");
    assert!(
        sm_corpus.contains("(`allow_structured_metadata: true`, `discover_log_levels: false`)"),
        "e2e/src/logs_sm_corpus.rs must keep the original provenance statement verbatim"
    );
    assert!(
        sm_corpus.contains("2026-08-30"),
        "e2e/src/logs_sm_corpus.rs must record the config change and its date"
    );

    for compose in [
        "deploy/e2e/compose.single.yaml",
        "deploy/e2e/compose.cluster.yaml",
    ] {
        assert!(
            repo_file(compose).contains("PULSUS_DISCOVER_LOG_LEVELS"),
            "{compose}: both sides of the e2e harness must be symmetric"
        );
    }

    let ci = repo_file(".github/workflows/ci.yml");
    assert!(
        ci.contains("detected_level capture drift leg (issue #483)"),
        ".github/workflows/ci.yml must run the live replay leg — an env-gated leg no \
         workflow runs passes by self-skipping"
    );
}
