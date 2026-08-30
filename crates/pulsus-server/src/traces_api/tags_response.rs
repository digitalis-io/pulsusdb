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

use super::intrinsics::{KEYWORD_TYPE, intrinsic_scope_tags};

/// The name of the scope the static intrinsic vocabulary is served under
/// (issue #475).
const INTRINSIC_SCOPE: &str = "intrinsic";

/// What answers a tag-NAMES request, before rendering (issue #475). A
/// distinct variant per source, rather than an empty `TagNames` threaded
/// through the catalog arm, so "no catalog read happened" is visible in
/// the type instead of inferred from a zero-length vector.
pub(crate) enum TagNamesAnswer<'a> {
    /// `scope=intrinsic` — the static list alone, no catalog read.
    IntrinsicOnly,
    /// `scope=` a member of `params::EMPTY_SCOPES` — a scope that names
    /// no catalog scope, so an empty list and no catalog read.
    NoTags,
    /// A catalog read. `with_intrinsic` prepends the static intrinsic
    /// scope, which the two scoped routes do for an unscoped request and
    /// the v1 flat route never does.
    Catalog {
        names: &'a TagNames,
        with_intrinsic: bool,
    },
}

/// What answers a tag-VALUES request, before rendering (issue #475).
pub(crate) enum TagValuesAnswer<'a> {
    /// The static vocabulary: every value typed `keyword`, never
    /// truncated, never read from the store.
    Static(&'a [&'static str]),
    /// A catalog read: type inferred per value, `truncated` carried.
    Catalog(&'a TagValues),
}

impl TagNamesAnswer<'_> {
    /// `truncated` continues to mean "the catalog read hit its cap" and
    /// nothing else, so a static answer is never truncated.
    fn truncated(&self) -> bool {
        match self {
            Self::IntrinsicOnly | Self::NoTags => false,
            Self::Catalog { names, .. } => names.truncated,
        }
    }
}

impl TagValuesAnswer<'_> {
    fn truncated(&self) -> bool {
        match self {
            Self::Static(_) => false,
            Self::Catalog(values) => values.truncated,
        }
    }
}

/// The static intrinsic scope object, identical on every route that
/// carries it.
fn intrinsic_scope_json() -> Value {
    json!({"name": INTRINSIC_SCOPE, "tags": intrinsic_scope_tags()})
}

/// The shared `scopes` array both scoped renderers emit — rows arrive in
/// the catalog's `(scope, key)` order, so grouping preserves both the
/// scope order and each scope's ascending key order.
fn scopes_json(answer: &TagNamesAnswer<'_>) -> Vec<Value> {
    let (names, with_intrinsic) = match answer {
        TagNamesAnswer::IntrinsicOnly => return vec![intrinsic_scope_json()],
        TagNamesAnswer::NoTags => return Vec::new(),
        TagNamesAnswer::Catalog {
            names,
            with_intrinsic,
        } => (*names, *with_intrinsic),
    };
    let mut out: Vec<Value> = Vec::new();
    if with_intrinsic {
        out.push(intrinsic_scope_json());
    }
    let mut scopes: Vec<(String, Vec<String>)> = Vec::new();
    for (scope, key) in &names.names {
        match scopes.last_mut() {
            Some((current, keys)) if current == scope => keys.push(key.clone()),
            _ => scopes.push((scope.clone(), vec![key.clone()])),
        }
    }
    out.extend(
        scopes
            .into_iter()
            .map(|(name, tags)| json!({"name": name, "tags": tags})),
    );
    out
}

/// The shared typed `tagValues` array — values stay strings on the wire
/// (Tempo shape); `type` is the inferred category, or `keyword` for a
/// static answer (issue #475), which is what makes the datasource emit
/// the value unquoted.
///
/// An EMPTY value omits the `value` key entirely: the canonical protobuf
/// JSON mapping omits a default-valued scalar, so the reference sends
/// `{"type":"string"}` with no `value`. Same omission rule
/// `search_response.rs` already applies to `durationNanos`/`durationMs`.
fn typed_values_json(answer: &TagValuesAnswer<'_>) -> Vec<Value> {
    fn entry(ty: &str, val: &str) -> Value {
        let mut obj = serde_json::Map::new();
        obj.insert("type".to_string(), json!(ty));
        if !val.is_empty() {
            obj.insert("value".to_string(), json!(val));
        }
        Value::Object(obj)
    }
    match answer {
        TagValuesAnswer::Static(values) => values.iter().map(|v| entry(KEYWORD_TYPE, v)).collect(),
        TagValuesAnswer::Catalog(values) => values
            .values
            .iter()
            .map(|v| entry(infer_type(v), v))
            .collect(),
    }
}

/// Native: `{"scopes":[{"name":…,"tags":[…]}],"truncated":…}`.
pub(crate) fn render_tag_names(answer: &TagNamesAnswer<'_>) -> Value {
    json!({
        "scopes": scopes_json(answer),
        "truncated": answer.truncated(),
    })
}

/// Native: `{"tagValues":[{"type":…,"value":…}],"truncated":…}`.
pub(crate) fn render_tag_values(answer: &TagValuesAnswer<'_>) -> Value {
    json!({
        "tagValues": typed_values_json(answer),
        "truncated": answer.truncated(),
    })
}

/// Tempo v2 alias (`/api/v2/search/tags`): the native scoped shape MINUS
/// the PulsusDB-only `truncated` field (issue #61 plan v2 Δ1 — alias
/// consumers lose the truncation signal; documented §8.1 delta).
pub(crate) fn render_tag_names_scoped_v2(answer: &TagNamesAnswer<'_>) -> Value {
    json!({"scopes": scopes_json(answer)})
}

/// Tempo v2 alias (`/api/v2/search/tag/{tag}/values`): the native typed
/// shape MINUS `truncated`.
pub(crate) fn render_tag_values_typed_v2(answer: &TagValuesAnswer<'_>) -> Value {
    json!({"tagValues": typed_values_json(answer)})
}

/// Tempo v1 alias (`/api/search/tags`): flat `{"tagNames":[…]}` — the
/// distinct keys in catalog `(scope, key)` order, deduplicated across
/// scopes on first occurrence (a key present in both scopes appears
/// once); scope and `truncated` dropped.
///
/// The intrinsic vocabulary reaches this route only when it is asked for
/// by name (`scope=intrinsic`): an unscoped v1 flat listing carries the
/// catalog keys alone, matching the reference (issue #475).
pub(crate) fn render_tag_names_flat(answer: &TagNamesAnswer<'_>) -> Value {
    let (names, with_intrinsic) = match answer {
        TagNamesAnswer::IntrinsicOnly => return json!({"tagNames": intrinsic_scope_tags()}),
        TagNamesAnswer::NoTags => return json!({"tagNames": Vec::<&str>::new()}),
        TagNamesAnswer::Catalog {
            names,
            with_intrinsic,
        } => (*names, *with_intrinsic),
    };
    let mut seen: HashSet<&str> = HashSet::new();
    let mut keys: Vec<&str> = Vec::new();
    // `with_intrinsic` is honoured here rather than ignored, so that the
    // flag is the SINGLE control over the injection on all three shapes.
    // The v1 handler passes `false`; if it ever stopped doing so, the
    // empty-database cell in `api_conformance.rs` reddens. A renderer
    // that silently dropped the flag would make that cell unable to see
    // the change.
    if with_intrinsic {
        for tag in intrinsic_scope_tags() {
            if seen.insert(tag) {
                keys.push(tag);
            }
        }
    }
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

    /// A catalog answer that does NOT carry the intrinsic scope — the
    /// shape every scoped-request test wants.
    fn catalog(names: &TagNames) -> TagNamesAnswer<'_> {
        TagNamesAnswer::Catalog {
            names,
            with_intrinsic: false,
        }
    }

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
            render_tag_names(&catalog(&names)),
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
            render_tag_names(&catalog(&names)),
            json!({"scopes": [], "truncated": false})
        );
    }

    #[test]
    fn render_tag_names_surfaces_the_truncated_flag() {
        let names = TagNames {
            names: vec![("span".to_string(), "k".to_string())],
            truncated: true,
        };
        assert_eq!(render_tag_names(&catalog(&names))["truncated"], json!(true));
    }

    #[test]
    fn render_tag_values_emits_typed_values_and_the_flag() {
        let values = TagValues {
            values: vec!["checkout".to_string(), "500".to_string()],
            truncated: false,
        };
        assert_eq!(
            render_tag_values(&TagValuesAnswer::Catalog(&values)),
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
            render_tag_values(&TagValuesAnswer::Catalog(&empty)),
            json!({"tagValues": [], "truncated": false})
        );
    }

    // -- issue #475: the static answers and the intrinsic scope ----------

    /// The intrinsic scope leads the two scoped shapes on an unscoped
    /// listing, and the catalog scopes follow in catalog order.
    #[test]
    fn an_unscoped_scoped_listing_leads_with_the_intrinsic_scope() {
        let names = TagNames {
            names: vec![("span".to_string(), "status".to_string())],
            truncated: false,
        };
        let answer = TagNamesAnswer::Catalog {
            names: &names,
            with_intrinsic: true,
        };
        let rendered = render_tag_names_scoped_v2(&answer);
        let scopes = rendered["scopes"].as_array().expect("scopes");
        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes[0]["name"], json!("intrinsic"));
        assert_eq!(scopes[0]["tags"], json!(intrinsic_scope_tags()));
        assert_eq!(
            scopes[1],
            json!({"name": "span", "tags": ["status"]}),
            "the catalog scopes follow, unchanged"
        );
    }

    /// The v1 FLAT route never gains the intrinsic names on an unscoped
    /// listing — asserted, not assumed, because it is the one route the
    /// reference does not inject them on.
    #[test]
    fn the_flat_route_never_injects_the_intrinsic_names() {
        let names = TagNames {
            names: vec![("span".to_string(), "status".to_string())],
            truncated: false,
        };
        assert_eq!(
            render_tag_names_flat(&TagNamesAnswer::Catalog {
                names: &names,
                with_intrinsic: false,
            }),
            json!({"tagNames": ["status"]})
        );
        // The flag is the single control: the renderer HONOURS it rather
        // than dropping it, so the empty-database conformance cell can
        // see a caller that starts setting it. `status` is deliberately
        // BOTH a catalog key here and one of the 25 static names, so the
        // deduplicated list is 25 long, not 26 — the count is measured,
        // not assumed.
        let injected = render_tag_names_flat(&TagNamesAnswer::Catalog {
            names: &names,
            with_intrinsic: true,
        });
        let injected = injected["tagNames"].as_array().expect("tagNames").clone();
        assert_eq!(injected.len(), 25, "{injected:?}");
        assert_eq!(injected[0], json!("duration"), "the static names lead");
        assert!(
            injected.contains(&json!("status")),
            "the catalog key survives, deduplicated against the static name"
        );
    }

    #[test]
    fn the_intrinsic_only_answer_is_the_static_scope_on_every_shape() {
        let tags = json!(intrinsic_scope_tags());
        assert_eq!(
            render_tag_names(&TagNamesAnswer::IntrinsicOnly),
            json!({"scopes": [{"name": "intrinsic", "tags": tags}], "truncated": false})
        );
        assert_eq!(
            render_tag_names_scoped_v2(&TagNamesAnswer::IntrinsicOnly),
            json!({"scopes": [{"name": "intrinsic", "tags": tags}]})
        );
        assert_eq!(
            render_tag_names_flat(&TagNamesAnswer::IntrinsicOnly),
            json!({"tagNames": tags})
        );
    }

    #[test]
    fn the_no_tags_answer_is_an_empty_list_on_every_shape() {
        assert_eq!(
            render_tag_names(&TagNamesAnswer::NoTags),
            json!({"scopes": [], "truncated": false})
        );
        assert_eq!(
            render_tag_names_scoped_v2(&TagNamesAnswer::NoTags),
            json!({"scopes": []})
        );
        assert_eq!(
            render_tag_names_flat(&TagNamesAnswer::NoTags),
            json!({"tagNames": []})
        );
    }

    #[test]
    fn a_static_value_answer_is_typed_keyword_and_never_truncated() {
        let answer = TagValuesAnswer::Static(&["ok", "error", "unset"]);
        assert_eq!(
            render_tag_values_typed_v2(&answer),
            json!({"tagValues": [
                {"type": "keyword", "value": "ok"},
                {"type": "keyword", "value": "error"},
                {"type": "keyword", "value": "unset"},
            ]})
        );
        assert_eq!(
            render_tag_values(&answer)["truncated"],
            json!(false),
            "a static answer never hit a cap"
        );
    }

    /// An empty value omits the `value` key: the canonical protobuf JSON
    /// mapping omits a default-valued scalar. The type is still emitted.
    #[test]
    fn an_empty_tag_value_omits_the_value_key() {
        let values = TagValues {
            values: vec!["".to_string(), "x".to_string()],
            truncated: false,
        };
        assert_eq!(
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values)),
            json!({"tagValues": [{"type": "string"}, {"type": "string", "value": "x"}]})
        );
        let typed = render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values));
        assert!(
            typed["tagValues"][0].get("value").is_none(),
            "the empty value must carry no `value` key: {typed}"
        );
        // The v1 flat projection is untouched: it still emits the empty
        // string as an element (ledger `traceql-v1-flat-empty-value-dropped`).
        assert_eq!(
            render_tag_values_flat(&values),
            json!({"tagValues": ["", "x"]})
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
        let v2 = render_tag_names_scoped_v2(&catalog(&names));
        assert_eq!(v2["scopes"], render_tag_names(&catalog(&names))["scopes"]);
        assert!(
            v2.get("truncated").is_none(),
            "the v2 alias must drop `truncated` even when the native flag is true: {v2}"
        );
        let empty = TagNames {
            names: vec![],
            truncated: false,
        };
        assert_eq!(
            render_tag_names_scoped_v2(&catalog(&empty)),
            json!({"scopes": []})
        );
    }

    #[test]
    fn render_tag_values_typed_v2_is_the_native_typed_values_without_a_truncated_key() {
        let values = TagValues {
            values: vec!["checkout".to_string(), "500".to_string()],
            truncated: true,
        };
        let v2 = render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values));
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
        assert_eq!(
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&empty)),
            json!({"tagValues": []})
        );
    }

    #[test]
    fn render_tag_names_flat_dedups_across_scopes_in_catalog_order() {
        // `env` exists in BOTH scopes — the flat projection keeps its
        // first (resource-side) occurrence only.
        let names = truncated_names();
        let flat = render_tag_names_flat(&catalog(&names));
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
        assert_eq!(
            render_tag_names_flat(&catalog(&empty)),
            json!({"tagNames": []})
        );
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
