//! Assembles the documented `GET /api/traces/v1/tags` and
//! `GET /api/traces/v1/tag/{tag}/values` JSON responses (docs/api.md
//! §4.3) from `pulsus_read::{TagNames, TagValues}` — response/type
//! shaping stays server-side so `pulsus-read` stays format-agnostic
//! (issue #55 layering). The native shapes are the Tempo-v2 twins
//! (scoped tag names; typed tag values) plus the additive top-level
//! `truncated` flag (issue #58 plan v2 Δ3 — the non-silent cap
//! indicator; T9's compat mapping simply drops it).
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

use serde_json::{Value, json};

use pulsus_read::{TagNames, TagValues};

/// `{"scopes":[{"name":…,"tags":[…]}],"truncated":…}` — rows arrive in
/// the catalog's `(scope, key)` order, so grouping preserves both the
/// scope order and each scope's ascending key order.
pub(crate) fn render_tag_names(names: &TagNames) -> Value {
    let mut scopes: Vec<(String, Vec<String>)> = Vec::new();
    for (scope, key) in &names.names {
        match scopes.last_mut() {
            Some((current, keys)) if current == scope => keys.push(key.clone()),
            _ => scopes.push((scope.clone(), vec![key.clone()])),
        }
    }
    json!({
        "scopes": scopes
            .into_iter()
            .map(|(name, tags)| json!({"name": name, "tags": tags}))
            .collect::<Vec<_>>(),
        "truncated": names.truncated,
    })
}

/// `{"tagValues":[{"type":…,"value":…}],"truncated":…}` — values stay
/// strings on the wire (Tempo shape); `type` is the inferred category.
pub(crate) fn render_tag_values(values: &TagValues) -> Value {
    json!({
        "tagValues": values
            .values
            .iter()
            .map(|v| json!({"type": infer_type(v), "value": v}))
            .collect::<Vec<_>>(),
        "truncated": values.truncated,
    })
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
