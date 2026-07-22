//! DoS proving tests for the bounded OTLP/JSON traces decode (issue #115 track
//! 6a). Every reject is proven NON-VACUOUS: the matching in-bounds body parses
//! `Ok`, and the reject fires DURING deserialization (a `serde` error →
//! [`LogsIngestError::DecodeJson`]). The green-gate suites
//! `tests/otlp_json_equivalence.rs` and `tests/otlp_json_vendor_patch.rs` cover
//! the no-regression / ADR-0004 preservation side.

use crate::error::LogsIngestError;
use crate::protocols::otlp_prescan::{
    MAX_ANYVALUE_DEPTH, MAX_ANYVALUE_ELEMENTS, MAX_ATTRIBUTES_PER_ELEMENT, MAX_DECODED_BYTES,
    MAX_ENTITY_REF_KEYS, MAX_ENTITY_REFS, MAX_EVENTS_PER_SPAN, MAX_LINKS_PER_SPAN,
    MAX_RESOURCE_SPANS, MAX_SCOPE_SPANS, MAX_SPANS, MAX_TOTAL_ATTRIBUTES, MAX_TOTAL_EVENTS,
    MAX_TOTAL_LINKS, MAX_TOTAL_SPANS,
};
use crate::protocols::otlp_traces::decode_json;

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

/// Wrap a `spans` array literal into one resourceSpans/scopeSpans envelope.
fn one_scope(spans_json: &str) -> String {
    format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":{spans_json}}}]}}]}}"#)
}

/// Wrap a single span object into a full request.
fn one_span(span_json: &str) -> String {
    one_scope(&format!("[{span_json}]"))
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

// --------------------------------------------------------------------------
// Positive: no regression + both container spellings accepted
// --------------------------------------------------------------------------

#[test]
fn in_bounds_traces_json_parses_with_every_repeated_field() {
    // Resource attrs + entity_refs + scope attrs + span attrs/events/links +
    // a nested kvlist/array attribute — every bounded field, all in bounds.
    let body = r#"{
      "resourceSpans": [{
        "resource": {
          "attributes": [{"key":"service.name","value":{"stringValue":"checkout"}}],
          "entityRefs": [{"schemaUrl":"https://schema","type":"service","idKeys":["service.name"],"descriptionKeys":["k"]}]
        },
        "scopeSpans": [{
          "scope": {"name":"lib","version":"1","attributes":[{"key":"s","value":{"stringValue":"v"}}]},
          "spans": [{
            "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
            "spanId":"00f067aa0ba902b7",
            "name":"op","kind":2,
            "startTimeUnixNano":"1700000000000000000",
            "attributes":[
              {"key":"http.method","value":{"stringValue":"GET"}},
              {"key":"nested","value":{"kvlistValue":{"values":[
                {"key":"a","value":{"arrayValue":{"values":[{"intValue":"1"},{"boolValue":true}]}}}
              ]}}}
            ],
            "events":[{"name":"ev","timeUnixNano":"1700000000000000001","attributes":[{"key":"e","value":{"stringValue":"x"}}]}],
            "links":[{"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7","attributes":[{"key":"l","value":{"stringValue":"y"}}]}]
          }]
        }]
      }]
    }"#;
    let req = decode_json(body.as_bytes()).expect("in-bounds request decodes");
    let rs = &req.resource_spans[0];
    let resource = rs.resource.as_ref().expect("resource");
    assert_eq!(resource.attributes.len(), 1);
    assert_eq!(resource.entity_refs.len(), 1);
    assert_eq!(resource.entity_refs[0].id_keys, vec!["service.name"]);
    let span = &rs.scope_spans[0].spans[0];
    assert_eq!(span.trace_id.len(), 16);
    assert_eq!(span.kind, 2);
    assert_eq!(span.attributes.len(), 2);
    assert_eq!(span.events.len(), 1);
    assert_eq!(span.links.len(), 1);
}

#[test]
fn container_fields_accept_both_camel_and_snake_case_spellings() {
    let camel = r#"{"resourceSpans":[{"resource":{"entityRefs":[{"schemaUrl":"u","type":"service","idKeys":["a"],"descriptionKeys":["b"]}]},
        "scopeSpans":[{"spans":[{"attributes":[{"key":"k","value":{"arrayValue":{"values":[{"intValue":"1"}]}}}]}]}]}]}"#;
    let snake = r#"{"resource_spans":[{"resource":{"entity_refs":[{"schemaUrl":"u","type":"service","id_keys":["a"],"description_keys":["b"]}]},
        "scope_spans":[{"spans":[{"attributes":[{"key":"k","value":{"array_value":{"values":[{"intValue":"1"}]}}}]}]}]}]}"#;
    let by_camel = decode_json(camel.as_bytes()).expect("camelCase decodes");
    let by_snake = decode_json(snake.as_bytes()).expect("snake_case decodes");
    assert_eq!(
        by_camel, by_snake,
        "both spellings of the fan-out/container fields must decode identically"
    );
    // And it actually carried the payload (guards against a vacuous empty-eq).
    assert_eq!(
        by_snake.resource_spans[0]
            .resource
            .as_ref()
            .unwrap()
            .entity_refs
            .len(),
        1
    );
}

// --------------------------------------------------------------------------
// Per-level reject: each repeated field, cap + 1
// --------------------------------------------------------------------------

#[test]
fn resource_spans_over_per_level_cap_rejects() {
    let body = format!(
        r#"{{"resourceSpans":{}}}"#,
        arr("{}", MAX_RESOURCE_SPANS + 1)
    );
    assert_rejects_with(&body, "resourceSpans");
    // Non-vacuity: exactly at cap parses.
    let ok = format!(r#"{{"resourceSpans":{}}}"#, arr("{}", MAX_RESOURCE_SPANS));
    assert_ok(&ok);
}

#[test]
fn scope_spans_over_per_level_cap_rejects() {
    let body = format!(
        r#"{{"resourceSpans":[{{"scopeSpans":{}}}]}}"#,
        arr("{}", MAX_SCOPE_SPANS + 1)
    );
    assert_rejects_with(&body, "scopeSpans");
}

#[test]
fn spans_over_per_level_cap_rejects() {
    let body = one_scope(&arr("{}", MAX_SPANS + 1));
    assert_rejects_with(&body, "spans");
}

#[test]
fn span_attributes_over_per_level_cap_rejects() {
    let attrs = arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1);
    let body = one_span(&format!(r#"{{"attributes":{attrs}}}"#));
    assert_rejects_with(&body, "attributes");
}

#[test]
fn span_events_over_per_level_cap_rejects() {
    let events = arr("{}", MAX_EVENTS_PER_SPAN + 1);
    let body = one_span(&format!(r#"{{"events":{events}}}"#));
    assert_rejects_with(&body, "events");
}

#[test]
fn span_links_over_per_level_cap_rejects() {
    let links = arr("{}", MAX_LINKS_PER_SPAN + 1);
    let body = one_span(&format!(r#"{{"links":{links}}}"#));
    assert_rejects_with(&body, "links");
}

#[test]
fn event_attributes_over_per_level_cap_rejects() {
    let attrs = arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1);
    let event = format!(r#"{{"attributes":{attrs}}}"#);
    let body = one_span(&format!(r#"{{"events":[{event}]}}"#));
    assert_rejects_with(&body, "attributes");
}

#[test]
fn link_attributes_over_per_level_cap_rejects() {
    let attrs = arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1);
    let link = format!(r#"{{"attributes":{attrs}}}"#);
    let body = one_span(&format!(r#"{{"links":[{link}]}}"#));
    assert_rejects_with(&body, "attributes");
}

#[test]
fn entity_refs_over_per_level_cap_rejects() {
    // Valid EntityRef elements (schemaUrl + type present) so the per-level cap is
    // what trips, not the required-scalar reject.
    let refs = arr(r#"{"schemaUrl":"u","type":"t"}"#, MAX_ENTITY_REFS + 1);
    let body = format!(r#"{{"resourceSpans":[{{"resource":{{"entityRefs":{refs}}}}}]}}"#);
    assert_rejects_with(&body, "entityRefs");
}

#[test]
fn entity_ref_keys_over_per_level_cap_rejects() {
    let keys = arr(r#""k""#, MAX_ENTITY_REF_KEYS + 1);
    let body =
        format!(r#"{{"resourceSpans":[{{"resource":{{"entityRefs":[{{"idKeys":{keys}}}]}}}}]}}"#);
    assert_rejects_with(&body, "idKeys");
}

// --------------------------------------------------------------------------
// Cross-request aggregate reject: each under per-level cap, summing over
// --------------------------------------------------------------------------

#[test]
fn spans_over_aggregate_cap_rejects() {
    // Each scope holds < MAX_SPANS spans; six scopes sum past MAX_TOTAL_SPANS.
    let per_scope = MAX_SPANS - 1; // in-bounds per level
    let scopes = MAX_TOTAL_SPANS / per_scope + 1;
    let scope_spans = arr("{}", per_scope);
    let scope = format!(r#"{{"spans":{scope_spans}}}"#);
    let body = format!(
        r#"{{"resourceSpans":[{{"scopeSpans":{}}}]}}"#,
        arr(&scope, scopes)
    );
    // Since issue #127 the decode-time byte budget (`size_of` per element)
    // is strictly tighter than the 5M count aggregate for this element
    // weight, so it is the FIRST bound this fixture crosses; the count
    // aggregate remains a backstop for lighter kinds (see the wide-array
    // AnyValue test, whose 32-byte elements still reach their aggregate).
    assert_rejects_with(&body, "decoded bytes (estimated)");
}

#[test]
fn events_over_aggregate_cap_rejects() {
    let per_span = MAX_EVENTS_PER_SPAN; // in-bounds per level
    let spans = MAX_TOTAL_EVENTS / per_span + 1;
    let events = arr("{}", per_span);
    let span = format!(r#"{{"events":{events}}}"#);
    let body = one_scope(&arr(&span, spans));
    // Since issue #127 the decode-time byte budget (`size_of` per element)
    // is strictly tighter than the 5M count aggregate for this element
    // weight, so it is the FIRST bound this fixture crosses; the count
    // aggregate remains a backstop for lighter kinds (see the wide-array
    // AnyValue test, whose 32-byte elements still reach their aggregate).
    assert_rejects_with(&body, "decoded bytes (estimated)");
}

#[test]
fn links_over_aggregate_cap_rejects() {
    let per_span = MAX_LINKS_PER_SPAN;
    let spans = MAX_TOTAL_LINKS / per_span + 1;
    let links = arr("{}", per_span);
    let span = format!(r#"{{"links":{links}}}"#);
    let body = one_scope(&arr(&span, spans));
    // Since issue #127 the decode-time byte budget (`size_of` per element)
    // is strictly tighter than the 5M count aggregate for this element
    // weight, so it is the FIRST bound this fixture crosses; the count
    // aggregate remains a backstop for lighter kinds (see the wide-array
    // AnyValue test, whose 32-byte elements still reach their aggregate).
    assert_rejects_with(&body, "decoded bytes (estimated)");
}

#[test]
fn attributes_over_aggregate_cap_rejects() {
    let per_span = MAX_ATTRIBUTES_PER_ELEMENT;
    let spans = MAX_TOTAL_ATTRIBUTES / per_span + 1;
    let attrs = arr(r#"{"key":"k"}"#, per_span);
    let span = format!(r#"{{"attributes":{attrs}}}"#);
    let body = one_scope(&arr(&span, spans));
    // Since issue #127 the decode-time byte budget (`size_of` per element)
    // is strictly tighter than the 5M count aggregate for this element
    // weight, so it is the FIRST bound this fixture crosses; the count
    // aggregate remains a backstop for lighter kinds (see the wide-array
    // AnyValue test, whose 32-byte elements still reach their aggregate).
    assert_rejects_with(&body, "decoded bytes (estimated)");
}

// --------------------------------------------------------------------------
// AnyValue: over-wide (aggregate element count) and over-depth
// --------------------------------------------------------------------------

#[test]
fn anyvalue_over_wide_kvlist_rejects() {
    // A single kvlist attribute with > MAX_ANYVALUE_ELEMENTS entries: the shared
    // AnyValue-element aggregate trips. Entries carry no value (charged by
    // occurrence), so the reject is width-driven, not depth-driven.
    let entries = arr(r#"{"key":"a"}"#, MAX_ANYVALUE_ELEMENTS + 1);
    let attr = format!(r#"{{"key":"big","value":{{"kvlistValue":{{"values":{entries}}}}}}}"#);
    let body = one_span(&format!(r#"{{"attributes":[{attr}]}}"#));
    // Since issue #127 the byte budget fires first here: kvlist entries are
    // `KeyValue`s (64 bytes each), heavier than `MAX_DECODED_BYTES / 5M`, so
    // the byte estimate crosses before the AnyValue-element count aggregate
    // (the wide-ARRAY twin's 32-byte `AnyValue` elements still reach it).
    assert_rejects_with(&body, "decoded bytes (estimated)");
}

/// A value nested `levels` deep in `arrayValue` wrappers with a scalar leaf.
fn nested_array_value(levels: usize) -> String {
    let mut v = r#"{"stringValue":"leaf"}"#.to_string();
    for _ in 0..levels {
        v = format!(r#"{{"arrayValue":{{"values":[{v}]}}}}"#);
    }
    v
}

#[test]
fn anyvalue_over_depth_rejects() {
    // The attribute value AnyValue is at depth 1; `levels` array wrappers put the
    // leaf at depth `levels + 1`. levels == MAX_ANYVALUE_DEPTH => leaf depth
    // MAX+1 > MAX => reject; levels == MAX-1 => leaf depth MAX => accepted.
    let over = nested_array_value(MAX_ANYVALUE_DEPTH);
    let body = one_span(&format!(
        r#"{{"attributes":[{{"key":"deep","value":{over}}}]}}"#
    ));
    assert_rejects_with(&body, "AnyValue nesting depth");

    let at_limit = nested_array_value(MAX_ANYVALUE_DEPTH - 1);
    let ok = one_span(&format!(
        r#"{{"attributes":[{{"key":"deep","value":{at_limit}}}]}}"#
    ));
    assert_ok(&ok);
}

// --------------------------------------------------------------------------
// Anti-evasion: alias-split (camel + snake) and duplicate keys
// --------------------------------------------------------------------------

#[test]
fn alias_split_resource_spans_cannot_evade_the_per_level_cap() {
    // Each spelling carries < MAX_RESOURCE_SPANS, but the two accumulate into ONE
    // counter and sum past it — the split must NOT stay under cap.
    let half = MAX_RESOURCE_SPANS / 2 + 1; // 2*half > cap
    let body = format!(
        r#"{{"resourceSpans":{},"resource_spans":{}}}"#,
        arr("{}", half),
        arr("{}", half)
    );
    assert_rejects_with(&body, "resourceSpans");
    // Non-vacuity: one spelling alone at `half` (< cap) still parses.
    let ok = format!(r#"{{"resourceSpans":{}}}"#, arr("{}", half));
    assert_ok(&ok);
}

#[test]
fn alias_split_scope_spans_cannot_evade_the_per_level_cap() {
    let half = MAX_SCOPE_SPANS / 2 + 1;
    let body = format!(
        r#"{{"resourceSpans":[{{"scopeSpans":{},"scope_spans":{}}}]}}"#,
        arr("{}", half),
        arr("{}", half)
    );
    assert_rejects_with(&body, "scopeSpans");
}

#[test]
fn alias_split_entity_refs_cannot_evade_the_per_level_cap() {
    let half = MAX_ENTITY_REFS / 2 + 1;
    let elem = r#"{"schemaUrl":"u","type":"t"}"#;
    let body = format!(
        r#"{{"resourceSpans":[{{"resource":{{"entityRefs":{},"entity_refs":{}}}}}]}}"#,
        arr(elem, half),
        arr(elem, half)
    );
    assert_rejects_with(&body, "entityRefs");
}

#[test]
fn alias_split_id_keys_cannot_evade_the_per_level_cap() {
    let half = MAX_ENTITY_REF_KEYS / 2 + 1;
    let body = format!(
        r#"{{"resourceSpans":[{{"resource":{{"entityRefs":[{{"idKeys":{},"id_keys":{}}}]}}}}]}}"#,
        arr(r#""k""#, half),
        arr(r#""k""#, half)
    );
    assert_rejects_with(&body, "idKeys");
}

#[test]
fn duplicate_attribute_key_cannot_evade_the_per_element_cap() {
    // Every attribute carries the IDENTICAL key: a dedup-collapsing counter would
    // see one key and wrongly accept. The raw-occurrence counter rejects.
    let attrs = arr(r#"{"key":"dup"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1);
    let body = one_span(&format!(r#"{{"attributes":{attrs}}}"#));
    assert_rejects_with(&body, "attributes");
}

#[test]
fn duplicate_spans_key_accumulates_into_one_counter() {
    // Two `spans` keys in one scope: raw occurrences accumulate into one counter,
    // so two just-over-half arrays trip the per-level cap.
    let half = MAX_SPANS / 2 + 1;
    let body = format!(
        r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":{},"spans":{}}}]}}]}}"#,
        arr("{}", half),
        arr("{}", half)
    );
    assert_rejects_with(&body, "spans");
}

// --------------------------------------------------------------------------
// Per-level reject: the remaining per-repeated-field matrix (cap + 1) the
// review flagged as missing — Resource.attributes, InstrumentationScope
// .attributes, EntityRef.descriptionKeys, ArrayValue.values (AnyValue width).
// --------------------------------------------------------------------------

#[test]
fn resource_attributes_over_per_level_cap_rejects() {
    let attrs = arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1);
    let body = format!(r#"{{"resourceSpans":[{{"resource":{{"attributes":{attrs}}}}}]}}"#);
    assert_rejects_with(&body, "attributes");
    // Non-vacuity: exactly at cap parses.
    let ok = format!(
        r#"{{"resourceSpans":[{{"resource":{{"attributes":{}}}}}]}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT)
    );
    assert_ok(&ok);
}

#[test]
fn scope_attributes_over_per_level_cap_rejects() {
    let attrs = arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT + 1);
    let body =
        format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"scope":{{"attributes":{attrs}}}}}]}}]}}"#);
    assert_rejects_with(&body, "attributes");
    let ok = format!(
        r#"{{"resourceSpans":[{{"scopeSpans":[{{"scope":{{"attributes":{}}}}}]}}]}}"#,
        arr(r#"{"key":"k"}"#, MAX_ATTRIBUTES_PER_ELEMENT)
    );
    assert_ok(&ok);
}

#[test]
fn entity_ref_description_keys_over_per_level_cap_rejects() {
    // The over-cap reject fires during descriptionKeys accumulation, before the
    // scalar leaves are finished — so no schemaUrl is needed to reach the cap.
    let keys = arr(r#""k""#, MAX_ENTITY_REF_KEYS + 1);
    let body = format!(
        r#"{{"resourceSpans":[{{"resource":{{"entityRefs":[{{"descriptionKeys":{keys}}}]}}}}]}}"#
    );
    assert_rejects_with(&body, "descriptionKeys");
}

#[test]
fn anyvalue_over_wide_array_rejects() {
    // An arrayValue with > MAX_ANYVALUE_ELEMENTS scalar elements: the shared
    // AnyValue-element aggregate trips on the excess element WIDTH.
    let elems = arr(r#"{"intValue":"1"}"#, MAX_ANYVALUE_ELEMENTS + 1);
    let attr = format!(r#"{{"key":"big","value":{{"arrayValue":{{"values":{elems}}}}}}}"#);
    let body = one_span(&format!(r#"{{"attributes":[{attr}]}}"#));
    assert_rejects_with(&body, "AnyValue elements");
}

// --------------------------------------------------------------------------
// Finding 1: AnyValue oneof/required-field semantics match the vendored P2
// visitor while the container width/depth stays bounded.
// --------------------------------------------------------------------------

#[test]
fn anyvalue_scalar_after_container_wins_matching_vendored_oneof() {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as V;
    // The vendored P2 visitor keeps the LAST recognized oneof key: a scalar after
    // a container must win (the finding-1 divergence was "container always wins").
    let attr =
        r#"{"key":"k","value":{"arrayValue":{"values":[{"intValue":"1"}]},"stringValue":"last"}}"#;
    let body = one_span(&format!(r#"{{"attributes":[{attr}]}}"#));
    let req = decode_json(body.as_bytes()).expect("mixed oneof decodes; last key wins");
    let v = req.resource_spans[0].scope_spans[0].spans[0].attributes[0]
        .value
        .as_ref()
        .unwrap();
    match v.value.as_ref().unwrap() {
        V::StringValue(s) => assert_eq!(s, "last"),
        other => panic!("expected the trailing StringValue to win, got {other:?}"),
    }
}

#[test]
fn anyvalue_container_after_scalar_wins_matching_vendored_oneof() {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as V;
    // The mirror: a container after a scalar wins (last recognized key).
    let attr =
        r#"{"key":"k","value":{"stringValue":"first","arrayValue":{"values":[{"intValue":"1"}]}}}"#;
    let body = one_span(&format!(r#"{{"attributes":[{attr}]}}"#));
    let req = decode_json(body.as_bytes()).expect("mixed oneof decodes; last key wins");
    let v = req.resource_spans[0].scope_spans[0].spans[0].attributes[0]
        .value
        .as_ref()
        .unwrap();
    assert!(
        matches!(v.value.as_ref().unwrap(), V::ArrayValue(_)),
        "expected the trailing arrayValue to win"
    );
}

#[test]
fn anyvalue_array_missing_values_rejects_matching_vendored() {
    // `{"arrayValue":{}}` — the vendored ArrayValue derive rejects the missing
    // required `values`; the bounded seed must too (was wrongly accepted).
    let attr = r#"{"key":"k","value":{"arrayValue":{}}}"#;
    let body = one_span(&format!(r#"{{"attributes":[{attr}]}}"#));
    assert_rejects_with(&body, "values");
}

#[test]
fn anyvalue_kvlist_missing_values_rejects_matching_vendored() {
    let attr = r#"{"key":"k","value":{"kvlistValue":{}}}"#;
    let body = one_span(&format!(r#"{{"attributes":[{attr}]}}"#));
    assert_rejects_with(&body, "values");
}

// --------------------------------------------------------------------------
// Finding 2: KeyValue / EntityRef scalar fields match the non-serde(default)
// vendored derive — missing-required and duplicate scalar keys are rejected
// (previously silently defaulted / last-write-win). Audit companions cover the
// serde(default) Resource / InstrumentationScope duplicate-scalar rejection.
// --------------------------------------------------------------------------

#[test]
fn key_value_missing_key_rejects_matching_vendored() {
    let attr = r#"{"value":{"stringValue":"v"}}"#;
    let body = one_span(&format!(r#"{{"attributes":[{attr}]}}"#));
    assert_rejects_with(&body, "key");
}

#[test]
fn key_value_duplicate_key_rejects_matching_vendored() {
    let attr = r#"{"key":"a","key":"b"}"#;
    let body = one_span(&format!(r#"{{"attributes":[{attr}]}}"#));
    assert_rejects_with(&body, "key");
}

#[test]
fn entity_ref_missing_required_scalar_rejects_matching_vendored() {
    let body = r#"{"resourceSpans":[{"resource":{"entityRefs":[{"idKeys":["a"]}]}}]}"#;
    assert_rejects_with(body, "schemaUrl");
}

#[test]
fn entity_ref_duplicate_scalar_key_rejects_matching_vendored() {
    let body = r#"{"resourceSpans":[{"resource":{"entityRefs":[{"schemaUrl":"u","type":"a","type":"b"}]}}]}"#;
    assert_rejects_with(body, "type");
}

#[test]
fn resource_duplicate_dropped_attributes_count_rejects_matching_vendored() {
    let body = r#"{"resourceSpans":[{"resource":{"droppedAttributesCount":1,"droppedAttributesCount":2}}]}"#;
    assert_rejects_with(body, "droppedAttributesCount");
}

#[test]
fn scope_duplicate_name_rejects_matching_vendored() {
    let body = r#"{"resourceSpans":[{"scopeSpans":[{"scope":{"name":"a","name":"b"}}]}]}"#;
    assert_rejects_with(body, "name");
}

// --------------------------------------------------------------------------
// Finding 1 (round 2): the ResourceSpans / ScopeSpans envelopes buffer-and-
// delegate their scalar `schemaUrl` and dup-guard their singular message child,
// so a DUPLICATE known scalar (or a repeated singular message) rejects exactly
// as the vendored `serde(default)` derive does — previously a hand-assign
// silently last-write-won.
// --------------------------------------------------------------------------

#[test]
fn resource_spans_duplicate_schema_url_rejects_matching_vendored() {
    let body = r#"{"resourceSpans":[{"schemaUrl":"a","schemaUrl":"b"}]}"#;
    assert_rejects_with(body, "schemaUrl");
    // Non-vacuity: a single schemaUrl decodes and is carried through the delegate.
    let req = decode_json(r#"{"resourceSpans":[{"schemaUrl":"u"}]}"#.as_bytes())
        .expect("single schemaUrl decodes");
    assert_eq!(req.resource_spans[0].schema_url, "u");
}

#[test]
fn scope_spans_duplicate_schema_url_rejects_matching_vendored() {
    let body = r#"{"resourceSpans":[{"scopeSpans":[{"schemaUrl":"a","schemaUrl":"b"}]}]}"#;
    assert_rejects_with(body, "schemaUrl");
    // Non-vacuity: a single schemaUrl decodes and is carried through the delegate.
    let req = decode_json(r#"{"resourceSpans":[{"scopeSpans":[{"schemaUrl":"u"}]}]}"#.as_bytes())
        .expect("single schemaUrl decodes");
    assert_eq!(req.resource_spans[0].scope_spans[0].schema_url, "u");
}

#[test]
fn resource_spans_duplicate_resource_rejects_matching_vendored() {
    // The vendored derive rejects a repeated singular `resource`; the bounded
    // seed's dup-guard must too (was silently last-write-win).
    let body = r#"{"resourceSpans":[{"resource":{},"resource":{}}]}"#;
    assert_rejects_with(body, "resource");
}

#[test]
fn scope_spans_duplicate_scope_rejects_matching_vendored() {
    let body = r#"{"resourceSpans":[{"scopeSpans":[{"scope":{},"scope":{}}]}]}"#;
    assert_rejects_with(body, "scope");
}

// --------------------------------------------------------------------------
// Finding 2 (round 2): unknown keys are IGNORED (no deny_unknown_fields in the
// vendored derives) and skipped via IgnoredAny WITHOUT materialization. This
// correctness companion proves the IGNORE policy (a deeply nested / wide unknown
// value decodes Ok, not rejected); `tests/otlp_json_unknown_alloc.rs` proves the
// value is not materialized.
// --------------------------------------------------------------------------

#[test]
fn unknown_key_with_deep_value_is_ignored_matching_vendored() {
    // A 64-deep unknown array value (under serde_json's recursion limit): the
    // derive ignores the unknown key, so the request must decode Ok.
    let mut deep = String::from("0");
    for _ in 0..64 {
        deep = format!("[{deep}]");
    }
    let body =
        format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{"__x__":{deep}}}]}}]}}]}}"#);
    assert_ok(&body);
}

#[test]
fn unknown_key_with_wide_value_is_ignored_matching_vendored() {
    // A wide unknown array value at every buffer-and-delegate level: all ignored.
    let wide = arr("1", 4096);
    let body = format!(
        r#"{{"resourceSpans":[{{"unkRs":{wide},"resource":{{"unkR":{wide}}},
           "scopeSpans":[{{"unkSs":{wide},"scope":{{"unkSc":{wide}}},
           "spans":[{{"unkSp":{wide}}}]}}]}}]}}"#
    );
    assert_ok(&body);
}

// --------------------------------------------------------------------------
// Decode-time byte budget (issue #127)
// --------------------------------------------------------------------------

/// AC 2a (JSON twin): exact-boundary identity, driven DIRECTLY at the shared
/// `decoded_bytes` cell / probe seam every bounded sequence charges through.
/// `size_of`-derived charges summing to EXACTLY `MAX_DECODED_BYTES` are
/// admitted; one further byte would reject — pinning the strictly-greater
/// semantics, byte-identical to the protobuf `charge` seam.
#[test]
fn byte_budget_exact_boundary_at_the_shared_cell_seam() {
    use opentelemetry_proto::tonic::trace::v1::Span;

    let agg = super::JsonAggregates::default();
    let budget = agg.byte_budget();
    let span = std::mem::size_of::<Span>();
    let full_spans = MAX_DECODED_BYTES / span;
    let remainder = MAX_DECODED_BYTES - full_spans * span;

    assert!(!budget.would_exceed(full_spans * span));
    budget.commit(full_spans * span);
    assert!(!budget.would_exceed(remainder));
    budget.commit(remainder);
    // Exactly at the budget: admitted; one more byte is strictly greater.
    assert!(!budget.would_exceed(0));
    assert!(
        budget.would_exceed(1),
        "strictly-greater semantics: MAX_DECODED_BYTES + 1 must reject"
    );
}

// Minimal local protobuf wire builders for the cross-encoding parity fixture
// (the full builder set lives in `otlp_prescan/tests.rs`; this module is
// JSON-side, so only the three primitives the parity test needs are mirrored).
fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn wire_ld(field: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 8);
    put_varint(&mut out, (u64::from(field) << 3) | 2);
    put_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(payload);
    out
}

fn wire_empty_repeated(field: u32, n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n * 2);
    for _ in 0..n {
        put_varint(&mut out, (u64::from(field) << 3) | 2);
        out.push(0);
    }
    out
}

/// The AC 2b derived fixture chunks: leaves split so no per-level count cap
/// can fire before the byte budget.
fn parity_chunks(total: usize, per_container: usize) -> Vec<usize> {
    let mut chunks = Vec::with_capacity(total.div_ceil(per_container));
    let mut remaining = total;
    while remaining > 0 {
        let chunk = remaining.min(per_container);
        chunks.push(chunk);
        remaining -= chunk;
    }
    chunks
}

/// AC 7 (issue #127): cross-encoding parity. One logical payload (the AC 2b
/// derived sizing: `MAX_DECODED_BYTES / size_of::<Span>() + 1024` empty spans,
/// auto-split at 900k per scope) rejects on BOTH encodings with each track's
/// byte-budget error — `DecodeJson` naming the budget here, `OversizeMessage
/// { field: "decoded bytes (estimated)" }` on the protobuf pre-scan — and a
/// scaled-down in-budget twin admits identically on both.
#[test]
fn cross_encoding_byte_budget_parity_for_spans() {
    use opentelemetry_proto::tonic::trace::v1::Span;

    const PER_CONTAINER: usize = 900_000;
    let total = MAX_DECODED_BYTES / std::mem::size_of::<Span>() + 1024;
    let chunks = parity_chunks(total, PER_CONTAINER);
    // Self-asserted preconditions: only the BYTE budget can fire.
    const { assert!(PER_CONTAINER < MAX_SPANS) }
    assert!(total < MAX_TOTAL_SPANS);
    assert!(chunks.len() < MAX_SCOPE_SPANS);

    // JSON encoding.
    let mut scopes_json = String::from("[");
    for (i, &chunk) in chunks.iter().enumerate() {
        if i > 0 {
            scopes_json.push(',');
        }
        scopes_json.push_str(&format!(r#"{{"spans":{}}}"#, arr("{}", chunk)));
    }
    scopes_json.push(']');
    let json_body = format!(r#"{{"resourceSpans":[{{"scopeSpans":{scopes_json}}}]}}"#);
    assert_rejects_with(&json_body, "decoded bytes (estimated)");

    // Protobuf encoding of the same logical payload.
    let mut scopes_wire = Vec::new();
    for &chunk in &chunks {
        scopes_wire.extend_from_slice(&wire_ld(2, &wire_empty_repeated(2, chunk)));
    }
    let wire_body = wire_ld(1, &scopes_wire);
    match crate::protocols::otlp_traces::decode(&wire_body) {
        Err(LogsIngestError::OversizeMessage { field, .. }) => {
            assert_eq!(field, "decoded bytes (estimated)");
        }
        other => panic!("protobuf twin must reject at the byte budget, got {other:?}"),
    }

    // Scaled-down in-budget twin admits on both encodings.
    const SMALL: usize = 1_000;
    let small_json = one_scope(&arr("{}", SMALL));
    let by_json = decode_json(small_json.as_bytes()).expect("in-budget JSON twin decodes");
    let small_wire = wire_ld(1, &wire_ld(2, &wire_empty_repeated(2, SMALL)));
    let by_wire =
        crate::protocols::otlp_traces::decode(&small_wire).expect("in-budget wire twin decodes");
    assert_eq!(by_json.resource_spans[0].scope_spans[0].spans.len(), SMALL);
    assert_eq!(
        by_json, by_wire,
        "both encodings decode the twin identically"
    );
}
