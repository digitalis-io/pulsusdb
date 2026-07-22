//! DoS proving tests for the bounded OTLP/JSON logs decode (issue #115 track
//! 6b), mirroring `otlp_json/tests.rs` (track 6a, traces) at the same
//! per-level / aggregate / depth thresholds. Every reject is proven
//! NON-VACUOUS: the matching in-bounds body parses `Ok`, and the reject fires
//! DURING deserialization (a `serde` error -> [`LogsIngestError::DecodeJson`]).
//! Shared building blocks (`AnyValueSeed`, `ResourceSeed`,
//! `InstrumentationScopeSeed`, `KeyValueSeed`, `EntityRefSeed`, `AccumSeq`,
//! `buffer_scalar_or_skip`, `finish_via_derive`) are reused verbatim from
//! track 6a and are exhaustively cap/alias/duplicate-key tested there — this
//! file covers only the LOGS-specific graph (`resourceLogs`/`scopeLogs`/
//! `logRecords`/`LogRecord.body`) plus the `crates/pulsus-write/tests/
//! otlp_json_equivalence.rs` and `otlp_json_vendor_patch.rs` green gates.

use crate::error::LogsIngestError;
use crate::protocols::otlp_logs::decode_json;
use crate::protocols::otlp_prescan::{
    MAX_ANYVALUE_DEPTH, MAX_ANYVALUE_ELEMENTS, MAX_ATTRIBUTES_PER_ELEMENT, MAX_DECODED_BYTES,
    MAX_LOG_RECORDS, MAX_RESOURCE_LOGS, MAX_SCOPE_LOGS, MAX_TOTAL_LOG_RECORDS,
};

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/// A JSON array literal of `n` copies of `elem` (no trailing comma).
fn arr(elem: &str, n: usize) -> String {
    let mut body = String::with_capacity((elem.len() + 1) * n + 2);
    body.push('[');
    if n > 0 {
        let mut chunk = String::with_capacity(elem.len() + 1);
        chunk.push_str(elem);
        chunk.push(',');
        body.push_str(&chunk.repeat(n));
        body.pop(); // drop the trailing comma
    }
    body.push(']');
    body
}

/// Wrap a `logRecords` array literal into one resourceLogs/scopeLogs envelope.
fn one_scope(log_records_json: &str) -> String {
    format!(r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":{log_records_json}}}]}}]}}"#)
}

/// Wrap a single log record object into a full request.
fn one_record(record_json: &str) -> String {
    one_scope(&format!("[{record_json}]"))
}

fn assert_ok(body: &str) {
    decode_json(body.as_bytes())
        .unwrap_or_else(|e| panic!("expected Ok, got {e:?}\nbody prefix: {:.120}", body));
}

/// Assert the body is rejected during decode as a `DecodeJson` (400 / code 3),
/// returning the message so the caller can prove the reject is the bounded-seed
/// one (non-vacuity vs. an unrelated parse error).
fn reject_message(body: &str) -> String {
    match decode_json(body.as_bytes()).expect_err("expected a bounded-decode reject") {
        LogsIngestError::DecodeJson(e) => e.to_string(),
        other => panic!("expected DecodeJson, got {other:?}"),
    }
}

fn assert_rejects_with(body: &str, needle: &str) {
    let msg = reject_message(body);
    assert!(
        msg.contains(needle),
        "reject message {msg:?} must mention {needle:?} (non-vacuity)"
    );
}

/// A value nested `levels` deep in `arrayValue` wrappers with a scalar leaf.
fn nested_array_value(levels: usize) -> String {
    let mut v = r#"{"stringValue":"leaf"}"#.to_string();
    for _ in 0..levels {
        v = format!(r#"{{"arrayValue":{{"values":[{v}]}}}}"#);
    }
    v
}

// --------------------------------------------------------------------------
// Positive: no regression + both container spellings accepted
// --------------------------------------------------------------------------

#[test]
fn in_bounds_logs_json_parses_with_every_repeated_field() {
    // Resource attrs + entity_refs + scope attrs + record attributes/body —
    // every bounded field, all in bounds, plus every LogRecord scalar leaf.
    let body = r#"{
      "resourceLogs": [{
        "resource": {
          "attributes": [{"key":"service.name","value":{"stringValue":"checkout"}}],
          "entityRefs": [{"schemaUrl":"https://schema","type":"service","idKeys":["service.name"],"descriptionKeys":["k"]}]
        },
        "scopeLogs": [{
          "scope": {"name":"lib","version":"1","attributes":[{"key":"s","value":{"stringValue":"v"}}]},
          "logRecords": [{
            "timeUnixNano":"1700000000000000123",
            "observedTimeUnixNano":"1700000000000000456",
            "severityNumber":9,
            "severityText":"INFO",
            "droppedAttributesCount":0,
            "flags":0,
            "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
            "spanId":"00f067aa0ba902b7",
            "eventName":"ev",
            "body":{"kvlistValue":{"values":[
              {"key":"a","value":{"arrayValue":{"values":[{"intValue":"1"},{"boolValue":true}]}}}
            ]}},
            "attributes":[
              {"key":"http.method","value":{"stringValue":"GET"}}
            ]
          }]
        }]
      }]
    }"#;
    let req = decode_json(body.as_bytes()).expect("in-bounds request decodes");
    let rl = &req.resource_logs[0];
    let resource = rl.resource.as_ref().expect("resource");
    assert_eq!(resource.attributes.len(), 1);
    assert_eq!(resource.entity_refs.len(), 1);
    assert_eq!(resource.entity_refs[0].id_keys, vec!["service.name"]);
    let scope_logs = &rl.scope_logs[0];
    assert_eq!(scope_logs.scope.as_ref().unwrap().attributes.len(), 1);
    let record = &scope_logs.log_records[0];
    assert_eq!(record.attributes.len(), 1);
    assert!(record.body.is_some());
    assert_eq!(record.trace_id.len(), 16);
    assert_eq!(record.span_id.len(), 8);
    assert_eq!(record.event_name, "ev");
    assert_eq!(record.severity_number, 9);
}

#[test]
fn container_fields_accept_both_camel_and_snake_case_spellings() {
    let camel = r#"{"resourceLogs":[{"resource":{"entityRefs":[{"schemaUrl":"u","type":"service","idKeys":["a"],"descriptionKeys":["b"]}]},
        "scopeLogs":[{"logRecords":[{"attributes":[{"key":"k","value":{"arrayValue":{"values":[{"intValue":"1"}]}}}]}]}]}]}"#;
    let snake = r#"{"resource_logs":[{"resource":{"entity_refs":[{"schemaUrl":"u","type":"service","id_keys":["a"],"description_keys":["b"]}]},
        "scope_logs":[{"log_records":[{"attributes":[{"key":"k","value":{"array_value":{"values":[{"intValue":"1"}]}}}]}]}]}]}"#;
    let by_camel = decode_json(camel.as_bytes()).expect("camelCase decodes");
    let by_snake = decode_json(snake.as_bytes()).expect("snake_case decodes");
    assert_eq!(
        by_camel, by_snake,
        "both spellings of the fan-out/container fields must decode identically"
    );
    // And it actually carried the payload (guards against a vacuous empty-eq).
    assert_eq!(
        by_snake.resource_logs[0]
            .resource
            .as_ref()
            .unwrap()
            .entity_refs
            .len(),
        1
    );
}

// --------------------------------------------------------------------------
// Per-level reject: each logs-specific repeated field, cap + 1
// --------------------------------------------------------------------------

#[test]
fn resource_logs_over_per_level_cap_rejects() {
    let body = format!(r#"{{"resourceLogs":{}}}"#, arr("{}", MAX_RESOURCE_LOGS + 1));
    assert_rejects_with(&body, "resourceLogs");
    // Non-vacuity: exactly at cap parses.
    let ok = format!(r#"{{"resourceLogs":{}}}"#, arr("{}", MAX_RESOURCE_LOGS));
    assert_ok(&ok);
}

#[test]
fn scope_logs_over_per_level_cap_rejects() {
    let body = format!(
        r#"{{"resourceLogs":[{{"scopeLogs":{}}}]}}"#,
        arr("{}", MAX_SCOPE_LOGS + 1)
    );
    assert_rejects_with(&body, "scopeLogs");
    // Non-vacuity / off-by-one: exactly at cap (the REAL MAX_SCOPE_LOGS)
    // parses — the reject above differs by exactly one element.
    let ok = format!(
        r#"{{"resourceLogs":[{{"scopeLogs":{}}}]}}"#,
        arr("{}", MAX_SCOPE_LOGS)
    );
    assert_ok(&ok);
}

#[test]
fn log_records_over_per_level_cap_rejects() {
    let body = one_scope(&arr("{}", MAX_LOG_RECORDS + 1));
    assert_rejects_with(&body, "logRecords");
    // Non-vacuity / off-by-one: exactly at cap (the REAL MAX_LOG_RECORDS,
    // which is below MAX_TOTAL_LOG_RECORDS, so the aggregate stays quiet)
    // parses — the reject above differs by exactly one element.
    let ok = one_scope(&arr("{}", MAX_LOG_RECORDS));
    assert_ok(&ok);
}

#[test]
fn log_record_attributes_over_per_level_cap_rejects() {
    let attrs = arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1);
    let body = one_record(&format!(r#"{{"attributes":{attrs}}}"#));
    assert_rejects_with(&body, "attributes");
    // Non-vacuity / off-by-one: exactly at cap (the REAL
    // MAX_ATTRIBUTES_PER_ELEMENT, below the attributes aggregate) parses —
    // the reject above differs by exactly one element.
    let ok = one_record(&format!(
        r#"{{"attributes":{}}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT)
    ));
    assert_ok(&ok);
}

// --------------------------------------------------------------------------
// Cross-request aggregate reject: log_records, under per-level cap, summing
// over the aggregate.
// --------------------------------------------------------------------------

#[test]
fn log_records_over_aggregate_cap_rejects() {
    // Each scope holds < MAX_LOG_RECORDS records; enough scopes sum past
    // MAX_TOTAL_LOG_RECORDS.
    let per_scope = MAX_LOG_RECORDS - 1; // in-bounds per level
    let scopes = MAX_TOTAL_LOG_RECORDS / per_scope + 1;
    let scope_records = arr("{}", per_scope);
    let scope = format!(r#"{{"logRecords":{scope_records}}}"#);
    let body = format!(
        r#"{{"resourceLogs":[{{"scopeLogs":{}}}]}}"#,
        arr(&scope, scopes)
    );
    // Since issue #127 the decode-time byte budget (`size_of` per element)
    // is strictly tighter than the 5M count aggregate for this element
    // weight, so it is the FIRST bound this fixture crosses; the count
    // aggregate remains a backstop for lighter kinds (see the wide-array
    // AnyValue test, whose 32-byte elements still reach their aggregate).
    assert_rejects_with(&body, "decoded bytes (estimated)");
}

// --------------------------------------------------------------------------
// LogRecord.body: bounded AnyValue (width via the shared aggregate, depth via
// the shared per-node bound) — proves `body` is intercepted as a MESSAGE, not
// left in `LOG_RECORD_SCALARS`.
// --------------------------------------------------------------------------

#[test]
fn log_record_body_anyvalue_over_wide_kvlist_rejects() {
    let entries = arr(r#"{"key":"a"}"#, MAX_ANYVALUE_ELEMENTS + 1);
    let body = one_record(&format!(
        r#"{{"body":{{"kvlistValue":{{"values":{entries}}}}}}}"#
    ));
    // Since issue #127 the byte budget fires first here: kvlist entries are
    // `KeyValue`s (64 bytes each), heavier than `MAX_DECODED_BYTES / 5M`, so
    // the byte estimate crosses before the AnyValue-element count aggregate
    // (the wide-ARRAY twin's 32-byte `AnyValue` elements still reach it).
    assert_rejects_with(&body, "decoded bytes (estimated)");
}

#[test]
fn log_record_body_anyvalue_over_depth_rejects() {
    // `body` is the depth-1 AnyValue; `levels` array wrappers put the leaf at
    // depth `levels + 1`. levels == MAX_ANYVALUE_DEPTH => leaf depth MAX+1 =>
    // reject; levels == MAX-1 => leaf depth MAX => accepted.
    let over = nested_array_value(MAX_ANYVALUE_DEPTH);
    let body = one_record(&format!(r#"{{"body":{over}}}"#));
    assert_rejects_with(&body, "AnyValue nesting depth");

    let at_limit = nested_array_value(MAX_ANYVALUE_DEPTH - 1);
    let ok = one_record(&format!(r#"{{"body":{at_limit}}}"#));
    assert_ok(&ok);
}

// --------------------------------------------------------------------------
// Anti-evasion: alias-split (camel + snake) and duplicate keys
// --------------------------------------------------------------------------

#[test]
fn alias_split_resource_logs_cannot_evade_the_per_level_cap() {
    // Each spelling carries < MAX_RESOURCE_LOGS, but the two accumulate into
    // ONE counter and sum past it — the split must NOT stay under cap.
    let half = MAX_RESOURCE_LOGS / 2 + 1; // 2*half > cap
    let body = format!(
        r#"{{"resourceLogs":{},"resource_logs":{}}}"#,
        arr("{}", half),
        arr("{}", half)
    );
    assert_rejects_with(&body, "resourceLogs");
    // Non-vacuity: one spelling alone at `half` (< cap) still parses.
    let ok = format!(r#"{{"resourceLogs":{}}}"#, arr("{}", half));
    assert_ok(&ok);
}

#[test]
fn alias_split_scope_logs_cannot_evade_the_per_level_cap() {
    let half = MAX_SCOPE_LOGS / 2 + 1;
    let body = format!(
        r#"{{"resourceLogs":[{{"scopeLogs":{},"scope_logs":{}}}]}}"#,
        arr("{}", half),
        arr("{}", half)
    );
    assert_rejects_with(&body, "scopeLogs");
}

#[test]
fn alias_split_log_records_cannot_evade_the_per_level_cap() {
    let half = MAX_LOG_RECORDS / 2 + 1;
    let body = format!(
        r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":{},"log_records":{}}}]}}]}}"#,
        arr("{}", half),
        arr("{}", half)
    );
    assert_rejects_with(&body, "logRecords");
}

#[test]
fn duplicate_log_records_key_accumulates_into_one_counter() {
    // Two `logRecords` keys in one scope: raw occurrences accumulate into one
    // counter, so two just-over-half arrays trip the per-level cap.
    let half = MAX_LOG_RECORDS / 2 + 1;
    let body = format!(
        r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":{},"logRecords":{}}}]}}]}}"#,
        arr("{}", half),
        arr("{}", half)
    );
    assert_rejects_with(&body, "logRecords");
}

// --------------------------------------------------------------------------
// Duplicate-known-scalar / duplicate-singular-message rejects, matching the
// non-`serde(default)`/`serde(default)` vendored derives exactly (issue #115
// track-6a finding 2 / round-2 finding 1).
// --------------------------------------------------------------------------

#[test]
fn log_record_duplicate_event_name_rejects_matching_vendored() {
    let body = one_record(r#"{"eventName":"a","eventName":"b"}"#);
    assert_rejects_with(&body, "eventName");
}

#[test]
fn log_record_duplicate_body_rejects_matching_vendored() {
    // `body` is a MESSAGE (bounded `AnyValueSeed`), dup-guarded like the
    // derive's repeated-singular-message rejection.
    let body = one_record(r#"{"body":{"stringValue":"a"},"body":{"stringValue":"b"}}"#);
    assert_rejects_with(&body, "body");
}

#[test]
fn resource_logs_duplicate_schema_url_rejects_matching_vendored() {
    let body = r#"{"resourceLogs":[{"schemaUrl":"a","schemaUrl":"b"}]}"#;
    assert_rejects_with(body, "schemaUrl");
    // Non-vacuity: a single schemaUrl decodes and is carried through the delegate.
    let req = decode_json(r#"{"resourceLogs":[{"schemaUrl":"u"}]}"#.as_bytes())
        .expect("single schemaUrl decodes");
    assert_eq!(req.resource_logs[0].schema_url, "u");
}

#[test]
fn scope_logs_duplicate_schema_url_rejects_matching_vendored() {
    let body = r#"{"resourceLogs":[{"scopeLogs":[{"schemaUrl":"a","schemaUrl":"b"}]}]}"#;
    assert_rejects_with(body, "schemaUrl");
    // Non-vacuity: a single schemaUrl decodes and is carried through the delegate.
    let req = decode_json(r#"{"resourceLogs":[{"scopeLogs":[{"schemaUrl":"u"}]}]}"#.as_bytes())
        .expect("single schemaUrl decodes");
    assert_eq!(req.resource_logs[0].scope_logs[0].schema_url, "u");
}

#[test]
fn resource_logs_duplicate_resource_rejects_matching_vendored() {
    // The vendored derive rejects a repeated singular `resource`; the bounded
    // seed's dup-guard must too.
    let body = r#"{"resourceLogs":[{"resource":{},"resource":{}}]}"#;
    assert_rejects_with(body, "resource");
}

#[test]
fn scope_logs_duplicate_scope_rejects_matching_vendored() {
    let body = r#"{"resourceLogs":[{"scopeLogs":[{"scope":{},"scope":{}}]}]}"#;
    assert_rejects_with(body, "scope");
}

// --------------------------------------------------------------------------
// Unknown fields are IGNORED (no `deny_unknown_fields` in the vendored
// derives) and skipped via `IgnoredAny` WITHOUT materialization — the shared
// `buffer_scalar_or_skip`/`IgnoredAny` mechanism (track 6a) reused verbatim
// here; `tests/otlp_json_unknown_alloc.rs` proves the non-materialization
// property for the shared code path this reuses.
// --------------------------------------------------------------------------

#[test]
fn unknown_key_with_wide_value_is_ignored_matching_vendored() {
    let wide = arr("1", 4096);
    let body = format!(
        r#"{{"resourceLogs":[{{"unkRl":{wide},"resource":{{"unkR":{wide}}},
           "scopeLogs":[{{"unkSl":{wide},"scope":{{"unkSc":{wide}}},
           "logRecords":[{{"unkLr":{wide}}}]}}]}}]}}"#
    );
    assert_ok(&body);
}

// --------------------------------------------------------------------------
// Decode-time byte budget (issue #127)
// --------------------------------------------------------------------------

/// AC 6 (issue #127), logs signal: an over-budget body (the AC 2b derived
/// sizing — `MAX_DECODED_BYTES / size_of::<LogRecord>() + 1024` empty records,
/// auto-split at 900k per scope so NO count cap fires) rejects as `DecodeJson`
/// whose message names the decode budget.
#[test]
fn over_budget_log_records_reject_names_the_decode_budget() {
    use opentelemetry_proto::tonic::logs::v1::LogRecord;

    const PER_CONTAINER: usize = 900_000;
    let total = MAX_DECODED_BYTES / std::mem::size_of::<LogRecord>() + 1024;
    // Self-asserted preconditions: only the BYTE budget can fire.
    const { assert!(PER_CONTAINER < MAX_LOG_RECORDS) }
    assert!(total < MAX_TOTAL_LOG_RECORDS);
    assert!(total.div_ceil(PER_CONTAINER) < MAX_SCOPE_LOGS);

    let mut scopes = String::from("[");
    let mut remaining = total;
    let mut first = true;
    while remaining > 0 {
        let chunk = remaining.min(PER_CONTAINER);
        remaining -= chunk;
        if !first {
            scopes.push(',');
        }
        first = false;
        scopes.push_str(&format!(r#"{{"logRecords":{}}}"#, arr("{}", chunk)));
    }
    scopes.push(']');
    let body = format!(r#"{{"resourceLogs":[{{"scopeLogs":{scopes}}}]}}"#);
    assert_rejects_with(&body, "decoded bytes (estimated)");
}
