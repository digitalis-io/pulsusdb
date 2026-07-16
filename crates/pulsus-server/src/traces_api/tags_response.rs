//! Assembles the documented `GET /api/traces/v1/tags` and
//! `GET /api/traces/v1/tag/{tag}/values` JSON responses (docs/api.md
//! §4.3) from `pulsus_read::{TagNames, TagValues}` — response/type
//! shaping stays server-side so `pulsus-read` stays format-agnostic
//! (issue #55 layering). The native shapes are the Tempo-v2 twins
//! (scoped tag names; typed tag values) plus the additive top-level
//! `truncated` flag (issue #58 plan v2 Δ3 — the non-silent cap
//! indicator; T9's compat mapping simply drops it).
//!
//! T9 (issue #61) adds the four compat projections the §8.1 aliases
//! serve: `render_tag_names_scoped_v2`/`render_tag_values_typed_v2`
//! (the native shapes minus `truncated` — Tempo's v2 wire has no
//! equivalent field) and `render_tag_names_flat`/`render_tag_values_flat`
//! (Tempo's legacy v1 flat shapes — scope, value types, and `truncated`
//! all projected away). Pure in-memory projections over the same
//! already-computed `TagNames`/`TagValues`; no extra query work.
//!
//! **Type inference is best-effort by contract** (task-manager
//! adjudication 2 on issue #58): `trace_tag_catalog` stores `val` as a
//! bare `String` with no type column (the #54 amendment window is
//! closed), so the wire `type` is inferred from the stored text — a
//! numeric- or duration-*looking* string attribute infers as
//! numeric/duration. The `duration` category delegates to
//! `pulsus_traceql::is_duration_literal`, the SINGLE SOURCE OF TRUTH for
//! the normative §4.2 duration grammar (final amendment: no second
//! implementation exists to drift — `.5s` infers as duration, `0.1ns`
//! does not).

use std::collections::HashSet;

use serde_json::{Value, json};

use pulsus_read::{TagNames, TagValues};

/// The shared `scopes` array both scoped renderers emit — rows arrive in
/// the catalog's `(scope, key)` order, so grouping preserves both the
/// scope order and each scope's ascending key order.
fn scopes_json(names: &TagNames) -> Vec<Value> {
    let mut scopes: Vec<(String, Vec<String>)> = Vec::new();
    for (scope, key) in &names.names {
        match scopes.last_mut() {
            Some((current, keys)) if current == scope => keys.push(key.clone()),
            _ => scopes.push((scope.clone(), vec![key.clone()])),
        }
    }
    scopes
        .into_iter()
        .map(|(name, tags)| json!({"name": name, "tags": tags}))
        .collect()
}

/// The shared typed `tagValues` array — values stay strings on the wire
/// (Tempo shape); `type` is the inferred category.
fn typed_values_json(values: &TagValues) -> Vec<Value> {
    values
        .values
        .iter()
        .map(|v| json!({"type": infer_type(v), "value": v}))
        .collect()
}

/// Native: `{"scopes":[{"name":…,"tags":[…]}],"truncated":…}`.
pub(crate) fn render_tag_names(names: &TagNames) -> Value {
    json!({
        "scopes": scopes_json(names),
        "truncated": names.truncated,
    })
}

/// Native: `{"tagValues":[{"type":…,"value":…}],"truncated":…}`.
pub(crate) fn render_tag_values(values: &TagValues) -> Value {
    json!({
        "tagValues": typed_values_json(values),
        "truncated": values.truncated,
    })
}

/// Tempo v2 alias (`/api/v2/search/tags`): the native scoped shape MINUS
/// the PulsusDB-only `truncated` field (issue #61 plan v2 Δ1 — alias
/// consumers lose the truncation signal; documented §8.1 delta).
pub(crate) fn render_tag_names_scoped_v2(names: &TagNames) -> Value {
    json!({"scopes": scopes_json(names)})
}

/// Tempo v2 alias (`/api/v2/search/tag/{tag}/values`): the native typed
/// shape MINUS `truncated`.
pub(crate) fn render_tag_values_typed_v2(values: &TagValues) -> Value {
    json!({"tagValues": typed_values_json(values)})
}

/// Tempo v1 alias (`/api/search/tags`): flat `{"tagNames":[…]}` — the
/// distinct keys in catalog `(scope, key)` order, deduplicated across
/// scopes on first occurrence (a key present in both scopes appears
/// once); scope and `truncated` dropped.
pub(crate) fn render_tag_names_flat(names: &TagNames) -> Value {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut keys: Vec<&str> = Vec::new();
    for (_, key) in &names.names {
        if seen.insert(key.as_str()) {
            keys.push(key.as_str());
        }
    }
    json!({"tagNames": keys})
}

/// Tempo v1 alias (`/api/search/tag/{tag}/values`): flat
/// `{"tagValues":[…]}` — bare value strings; type and `truncated`
/// dropped.
pub(crate) fn render_tag_values_flat(values: &TagValues) -> Value {
    json!({"tagValues": &values.values})
}

/// Deterministic best-effort type inference over the stored string, in
/// this order (issue #58 plan v2 Δ2 as amended):
///
/// 1. exact `true`/`false` (case-sensitive, documented) → `bool`;
/// 2. a valid §4.2 TraceQL duration literal, by the normative parser's
///    own verdict (`pulsus_traceql::is_duration_literal` — single
///    source of truth, no second grammar) → `duration`;
/// 3. all ASCII digits with an optional leading `-` → `int`;
/// 4. `f64`-parseable → `float`;
/// 5. everything else → `string`.
pub(crate) fn infer_type(val: &str) -> &'static str {
    if val == "true" || val == "false" {
        return "bool";
    }
    if pulsus_traceql::is_duration_literal(val) {
        return "duration";
    }
    let digits = val.strip_prefix('-').unwrap_or(val);
    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
        return "int";
    }
    if val.parse::<f64>().is_ok() {
        return "float";
    }
    "string"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_tag_names_groups_by_scope_preserving_catalog_order() {
        let names = TagNames {
            names: vec![
                ("resource".to_string(), "env".to_string()),
                ("resource".to_string(), "service.name".to_string()),
                ("span".to_string(), "http.status_code".to_string()),
            ],
            truncated: false,
        };
        assert_eq!(
            render_tag_names(&names),
            json!({
                "scopes": [
                    {"name": "resource", "tags": ["env", "service.name"]},
                    {"name": "span", "tags": ["http.status_code"]},
                ],
                "truncated": false,
            })
        );
    }

    #[test]
    fn render_tag_names_empty_is_the_documented_empty_envelope() {
        let names = TagNames {
            names: vec![],
            truncated: false,
        };
        assert_eq!(
            render_tag_names(&names),
            json!({"scopes": [], "truncated": false})
        );
    }

    #[test]
    fn render_tag_names_surfaces_the_truncated_flag() {
        let names = TagNames {
            names: vec![("span".to_string(), "k".to_string())],
            truncated: true,
        };
        assert_eq!(render_tag_names(&names)["truncated"], json!(true));
    }

    #[test]
    fn render_tag_values_emits_typed_values_and_the_flag() {
        let values = TagValues {
            values: vec!["checkout".to_string(), "500".to_string()],
            truncated: false,
        };
        assert_eq!(
            render_tag_values(&values),
            json!({
                "tagValues": [
                    {"type": "string", "value": "checkout"},
                    {"type": "int", "value": "500"},
                ],
                "truncated": false,
            })
        );
        let empty = TagValues {
            values: vec![],
            truncated: false,
        };
        assert_eq!(
            render_tag_values(&empty),
            json!({"tagValues": [], "truncated": false})
        );
    }

    // -- T9 (issue #61): the four alias projections, pinned. -------------

    fn truncated_names() -> TagNames {
        TagNames {
            names: vec![
                ("resource".to_string(), "env".to_string()),
                ("resource".to_string(), "service.name".to_string()),
                ("span".to_string(), "env".to_string()),
                ("span".to_string(), "http.status_code".to_string()),
            ],
            truncated: true,
        }
    }

    #[test]
    fn render_tag_names_scoped_v2_is_the_native_scopes_without_a_truncated_key() {
        let names = truncated_names();
        let v2 = render_tag_names_scoped_v2(&names);
        assert_eq!(v2["scopes"], render_tag_names(&names)["scopes"]);
        assert!(
            v2.get("truncated").is_none(),
            "the v2 alias must drop `truncated` even when the native flag is true: {v2}"
        );
        let empty = TagNames {
            names: vec![],
            truncated: false,
        };
        assert_eq!(render_tag_names_scoped_v2(&empty), json!({"scopes": []}));
    }

    #[test]
    fn render_tag_values_typed_v2_is_the_native_typed_values_without_a_truncated_key() {
        let values = TagValues {
            values: vec!["checkout".to_string(), "500".to_string()],
            truncated: true,
        };
        let v2 = render_tag_values_typed_v2(&values);
        assert_eq!(
            v2,
            json!({
                "tagValues": [
                    {"type": "string", "value": "checkout"},
                    {"type": "int", "value": "500"},
                ],
            })
        );
        assert!(
            v2.get("truncated").is_none(),
            "the v2 alias must drop `truncated` even when the native flag is true: {v2}"
        );
        let empty = TagValues {
            values: vec![],
            truncated: false,
        };
        assert_eq!(render_tag_values_typed_v2(&empty), json!({"tagValues": []}));
    }

    #[test]
    fn render_tag_names_flat_dedups_across_scopes_in_catalog_order() {
        // `env` exists in BOTH scopes — the flat projection keeps its
        // first (resource-side) occurrence only.
        let flat = render_tag_names_flat(&truncated_names());
        assert_eq!(
            flat,
            json!({"tagNames": ["env", "service.name", "http.status_code"]})
        );
        assert!(flat.get("truncated").is_none(), "no truncated key: {flat}");
        assert!(flat.get("scopes").is_none(), "no scopes key: {flat}");
    }

    #[test]
    fn render_tag_names_flat_empty_is_the_documented_empty_envelope() {
        let empty = TagNames {
            names: vec![],
            truncated: false,
        };
        assert_eq!(render_tag_names_flat(&empty), json!({"tagNames": []}));
    }

    #[test]
    fn render_tag_values_flat_emits_bare_strings_without_type_or_truncated() {
        let values = TagValues {
            values: vec!["checkout".to_string(), "500".to_string()],
            truncated: true,
        };
        assert_eq!(
            render_tag_values_flat(&values),
            json!({"tagValues": ["checkout", "500"]})
        );
        let empty = TagValues {
            values: vec![],
            truncated: false,
        };
        assert_eq!(render_tag_values_flat(&empty), json!({"tagValues": []}));
    }

    /// AC3 (plan v2 Δ2 as amended): the pinned inference vectors,
    /// including the ambiguous ones.
    #[test]
    fn infer_type_covers_the_pinned_vectors() {
        for (val, expected) in [
            ("123", "int"),
            ("-7", "int"),
            ("1.5", "float"),
            ("-1.5", "float"),
            ("1h", "duration"),
            ("1h30m", "string"), // compound literals are not in the grammar
            ("123ms", "duration"),
            ("1.5s", "duration"),
            ("5m", "duration"),
            ("true", "bool"),
            ("false", "bool"),
            ("TRUE", "string"), // case-sensitive, documented
            ("trueish", "string"),
            ("", "string"),
        ] {
            assert_eq!(infer_type(val), expected, "vector {val:?}");
        }
    }

    /// AC3 (final amendment): the duration category agrees with the
    /// normative parser's verdict on the corpus-adjacent cases — `.5s`
    /// is grammar-valid, `0.1ns` rejects (fractional nanoseconds), `1d`
    /// rejects (unsupported unit), `1h30m` rejects (compound).
    #[test]
    fn duration_inference_agrees_with_the_normative_parser_verdict() {
        for val in [".5s", "0.5s", "1d", "0.1ns", "1h30m", "2s", "500µs"] {
            let parser_says = pulsus_traceql::is_duration_literal(val);
            assert_eq!(
                infer_type(val) == "duration",
                parser_says,
                "inference must agree with the parser on {val:?}"
            );
        }
        assert_eq!(infer_type(".5s"), "duration");
        assert_eq!(infer_type("0.5s"), "duration");
        assert_eq!(infer_type("1d"), "string");
        assert_eq!(infer_type("1h30m"), "string");
        // `0.1ns` is lexically duration-shaped but does not resolve to
        // whole nanoseconds (FractionalNanoseconds reject), and its unit
        // suffix defeats the int/float parses — it is a plain string.
        assert_eq!(infer_type("0.1ns"), "string");
    }
}
