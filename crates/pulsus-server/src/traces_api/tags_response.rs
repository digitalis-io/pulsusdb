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
//! **The wire `type` is the STORED type, never a reading of the value's
//! text** (issue #476). `tag_values.val_type` carries the OTLP kind the
//! sender sent, put there at ingest by the landing decode
//! (`TraceJsonScalar::val_type`). Nothing in this module inspects a character of
//! `val` to decide a type. The text-classifying helper that used to —
//! `bool` for `"true"`, `duration` for anything the duration-literal
//! parser accepted, `int` for digits, `float` for an `f64` parse — is
//! DELETED, not kept for legacy rows, along with the `duration` category,
//! which the catalog can no longer emit and which the reference never
//! emits for an attribute either. Every source of a row types it: the
//! writer from the OTLP kind, every read over `spans`, `resources` and the
//! bucket catalogues by a literal per JSON type (issue #602 part 3c
//! removed the `string` fallback for an untyped row, which no source
//! produces).

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
    /// The static vocabulary: every value typed `keyword`, never read from
    /// the store, and truncated only by the request's `limit`.
    Static(&'a [&'static str]),
    /// A catalog read: the stored type per value, `truncated` carried.
    Catalog(&'a TagValues),
}

impl TagNamesAnswer<'_> {
    /// Whether the catalog read hit its cap; a static answer never does.
    /// The request's `limit` can also cut an answer, which
    /// [`render_tag_names`] adds.
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

/// Cuts `list` to the request's `limit`, saying whether anything was cut.
fn cut<T>(list: &mut Vec<T>, limit: Option<usize>) -> bool {
    match limit {
        Some(n) if list.len() > n => {
            list.truncate(n);
            true
        }
        _ => false,
    }
}

/// The shared `scopes` array both scoped renderers emit — rows arrive in
/// the catalog's `(scope, key)` order, so grouping preserves both the
/// scope order and each scope's ascending key order — and whether the
/// request's `limit` cut any scope's list, the `intrinsic` scope's
/// included. The limit applies per scope, after grouping.
fn scopes_json(answer: &TagNamesAnswer<'_>, limit: Option<usize>) -> (Vec<Value>, bool) {
    let (names, with_intrinsic) = match answer {
        TagNamesAnswer::IntrinsicOnly => (None, true),
        TagNamesAnswer::NoTags => return (Vec::new(), false),
        TagNamesAnswer::Catalog {
            names,
            with_intrinsic,
        } => (Some(*names), *with_intrinsic),
    };
    let mut scopes: Vec<(String, Vec<String>)> = Vec::new();
    if with_intrinsic {
        scopes.push((
            INTRINSIC_SCOPE.to_string(),
            intrinsic_scope_tags()
                .iter()
                .map(|t| t.to_string())
                .collect(),
        ));
    }
    let catalog_from = scopes.len();
    for (scope, key) in names.map(|n| n.names.as_slice()).unwrap_or_default() {
        match scopes[catalog_from..].last_mut() {
            Some((current, keys)) if current == scope => keys.push(key.clone()),
            _ => scopes.push((scope.clone(), vec![key.clone()])),
        }
    }
    let mut was_cut = false;
    let out = scopes
        .into_iter()
        .map(|(name, mut tags)| {
            was_cut |= cut(&mut tags, limit);
            json!({"name": name, "tags": tags})
        })
        .collect();
    (out, was_cut)
}

/// The shared typed `tagValues` array — values stay strings on the wire
/// (Tempo shape); `type` is the STORED type, or `keyword` for a static
/// answer (issue #475), which is what makes the datasource emit the value
/// unquoted.
///
/// An EMPTY value omits the `value` key entirely: the canonical protobuf
/// JSON mapping omits a default-valued scalar, so the reference sends
/// `{"type":"string"}` with no `value`. Same omission rule
/// `search_response.rs` already applies to `durationNanos`/`durationMs`.
///
/// Rows arrive `ORDER BY val, val_type` and distinct, so one value stored
/// at two types is two entries (issue #476).
///
/// One pass over at most `TAG_VALUES_MAX` entries, no extra query, and no
/// character of `val` is read. The request's `limit` cuts the entries, a
/// static list too; the flag says whether it did.
fn typed_values_json(answer: &TagValuesAnswer<'_>, limit: Option<usize>) -> (Vec<Value>, bool) {
    let mut out = typed_values(answer);
    let was_cut = cut(&mut out, limit);
    (out, was_cut)
}

fn typed_values(answer: &TagValuesAnswer<'_>) -> Vec<Value> {
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
            .map(|v| entry(&v.val_type, &v.val))
            .collect(),
    }
}

/// Native: `{"scopes":[{"name":…,"tags":[…]}],"truncated":…}`.
/// `truncated` is the read's cap or the request's `limit` cutting a scope.
pub(crate) fn render_tag_names(answer: &TagNamesAnswer<'_>, limit: Option<usize>) -> Value {
    let (scopes, was_cut) = scopes_json(answer, limit);
    json!({
        "scopes": scopes,
        "truncated": answer.truncated() || was_cut,
    })
}

/// Native: `{"tagValues":[{"type":…,"value":…}],"truncated":…}`.
/// `truncated` is the read's cap or the request's `limit` cutting the
/// list, a static one included.
pub(crate) fn render_tag_values(answer: &TagValuesAnswer<'_>, limit: Option<usize>) -> Value {
    let (values, was_cut) = typed_values_json(answer, limit);
    json!({
        "tagValues": values,
        "truncated": answer.truncated() || was_cut,
    })
}

/// Tempo v2 alias (`/api/v2/search/tags`): the native scoped shape MINUS
/// the PulsusDB-only `truncated` field (issue #61 plan v2 Δ1 — alias
/// consumers lose the truncation signal; documented §8.1 delta).
pub(crate) fn render_tag_names_scoped_v2(
    answer: &TagNamesAnswer<'_>,
    limit: Option<usize>,
) -> Value {
    json!({"scopes": scopes_json(answer, limit).0})
}

/// Tempo v2 alias (`/api/v2/search/tag/{tag}/values`): the native typed
/// shape MINUS `truncated`.
pub(crate) fn render_tag_values_typed_v2(
    answer: &TagValuesAnswer<'_>,
    limit: Option<usize>,
) -> Value {
    json!({"tagValues": typed_values_json(answer, limit).0})
}

/// Tempo v1 alias (`/api/search/tags`): flat `{"tagNames":[…]}` — the
/// distinct keys in catalog `(scope, key)` order, deduplicated across
/// scopes on first occurrence (a key present in both scopes appears
/// once); scope and `truncated` dropped.
///
/// The intrinsic vocabulary reaches this route only when it is asked for
/// by name (`scope=intrinsic`): an unscoped v1 flat listing carries the
/// catalog keys alone, matching the reference (issue #475). The request's
/// `limit` cuts the one flat list, after the deduplication.
pub(crate) fn render_tag_names_flat(answer: &TagNamesAnswer<'_>, limit: Option<usize>) -> Value {
    let (names, with_intrinsic) = match answer {
        TagNamesAnswer::IntrinsicOnly => {
            let mut keys = intrinsic_scope_tags().to_vec();
            cut(&mut keys, limit);
            return json!({"tagNames": keys});
        }
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
    cut(&mut keys, limit);
    json!({"tagNames": keys})
}

/// Tempo v1 alias (`/api/search/tag/{tag}/values`): flat
/// `{"tagValues":[…]}` — bare value strings; type and `truncated`
/// dropped.
///
/// Deduplicates on `val` ALONE (issue #476): the underlying read now
/// returns one row per `(value, type)` pair, so a key holding a string
/// `"8080"` and an int `8080` yields two rows whose flat projection would
/// otherwise be `["8080","8080"]`. Rows sharing a `val` are contiguous by
/// the read's `ORDER BY`, so this is a first-occurrence pass, not a set.
/// The request's `limit` cuts the list after that pass.
pub(crate) fn render_tag_values_flat(values: &TagValues, limit: Option<usize>) -> Value {
    let mut flat: Vec<&str> = Vec::with_capacity(values.values.len());
    for v in &values.values {
        if flat.last() != Some(&v.val.as_str()) {
            flat.push(v.val.as_str());
        }
    }
    cut(&mut flat, limit);
    json!({"tagValues": flat})
}

#[cfg(test)]
mod tests {
    use super::*;

    use pulsus_read::TagValue;

    /// `(value, stored type)` pairs as the engine hands them over —
    /// ALREADY in the read's `ORDER BY val, val_type` order.
    fn tag_values(pairs: &[(&str, &str)], truncated: bool) -> TagValues {
        TagValues {
            values: pairs
                .iter()
                .map(|(val, val_type)| TagValue {
                    val: (*val).to_string(),
                    val_type: (*val_type).to_string(),
                })
                .collect(),
            truncated,
        }
    }

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
            render_tag_names(&catalog(&names), None),
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
            render_tag_names(&catalog(&names), None),
            json!({"scopes": [], "truncated": false})
        );
    }

    #[test]
    fn render_tag_names_surfaces_the_truncated_flag() {
        let names = TagNames {
            names: vec![("span".to_string(), "k".to_string())],
            truncated: true,
        };
        assert_eq!(
            render_tag_names(&catalog(&names), None)["truncated"],
            json!(true)
        );
    }

    #[test]
    fn render_tag_values_emits_typed_values_and_the_flag() {
        let values = tag_values(&[("checkout", "string"), ("500", "int")], false);
        assert_eq!(
            render_tag_values(&TagValuesAnswer::Catalog(&values), None),
            json!({
                "tagValues": [
                    {"type": "string", "value": "checkout"},
                    {"type": "int", "value": "500"},
                ],
                "truncated": false,
            })
        );
        let empty = tag_values(&[], false);
        assert_eq!(
            render_tag_values(&TagValuesAnswer::Catalog(&empty), None),
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
        let rendered = render_tag_names_scoped_v2(&answer, None);
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
            render_tag_names_flat(
                &TagNamesAnswer::Catalog {
                    names: &names,
                    with_intrinsic: false,
                },
                None
            ),
            json!({"tagNames": ["status"]})
        );
        // The flag is the single control: the renderer HONOURS it rather
        // than dropping it, so the empty-database conformance cell can
        // see a caller that starts setting it. `status` is deliberately
        // BOTH a catalog key here and one of the 25 static names, so the
        // deduplicated list is 25 long, not 26 — the count is measured,
        // not assumed.
        let injected = render_tag_names_flat(
            &TagNamesAnswer::Catalog {
                names: &names,
                with_intrinsic: true,
            },
            None,
        );
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
            render_tag_names(&TagNamesAnswer::IntrinsicOnly, None),
            json!({"scopes": [{"name": "intrinsic", "tags": tags}], "truncated": false})
        );
        assert_eq!(
            render_tag_names_scoped_v2(&TagNamesAnswer::IntrinsicOnly, None),
            json!({"scopes": [{"name": "intrinsic", "tags": tags}]})
        );
        assert_eq!(
            render_tag_names_flat(&TagNamesAnswer::IntrinsicOnly, None),
            json!({"tagNames": tags})
        );
    }

    #[test]
    fn the_no_tags_answer_is_an_empty_list_on_every_shape() {
        assert_eq!(
            render_tag_names(&TagNamesAnswer::NoTags, None),
            json!({"scopes": [], "truncated": false})
        );
        assert_eq!(
            render_tag_names_scoped_v2(&TagNamesAnswer::NoTags, None),
            json!({"scopes": []})
        );
        assert_eq!(
            render_tag_names_flat(&TagNamesAnswer::NoTags, None),
            json!({"tagNames": []})
        );
    }

    #[test]
    fn a_static_value_answer_is_typed_keyword_and_never_truncated() {
        let answer = TagValuesAnswer::Static(&["ok", "error", "unset"]);
        assert_eq!(
            render_tag_values_typed_v2(&answer, None),
            json!({"tagValues": [
                {"type": "keyword", "value": "ok"},
                {"type": "keyword", "value": "error"},
                {"type": "keyword", "value": "unset"},
            ]})
        );
        assert_eq!(
            render_tag_values(&answer, None)["truncated"],
            json!(false),
            "a static answer never hit a cap"
        );
    }

    /// An empty value omits the `value` key: the canonical protobuf JSON
    /// mapping omits a default-valued scalar. The type is still emitted.
    #[test]
    fn an_empty_tag_value_omits_the_value_key() {
        let values = tag_values(&[("", "string"), ("x", "string")], false);
        assert_eq!(
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values), None),
            json!({"tagValues": [{"type": "string"}, {"type": "string", "value": "x"}]})
        );
        let typed = render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values), None);
        assert!(
            typed["tagValues"][0].get("value").is_none(),
            "the empty value must carry no `value` key: {typed}"
        );
        // The v1 flat projection is untouched: it still emits the empty
        // string as an element (ledger `traceql-v1-flat-empty-value-dropped`).
        assert_eq!(
            render_tag_values_flat(&values, None),
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
        let v2 = render_tag_names_scoped_v2(&catalog(&names), None);
        assert_eq!(
            v2["scopes"],
            render_tag_names(&catalog(&names), None)["scopes"]
        );
        assert!(
            v2.get("truncated").is_none(),
            "the v2 alias must drop `truncated` even when the native flag is true: {v2}"
        );
        let empty = TagNames {
            names: vec![],
            truncated: false,
        };
        assert_eq!(
            render_tag_names_scoped_v2(&catalog(&empty), None),
            json!({"scopes": []})
        );
    }

    #[test]
    fn render_tag_values_typed_v2_is_the_native_typed_values_without_a_truncated_key() {
        let values = tag_values(&[("checkout", "string"), ("500", "int")], true);
        let v2 = render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values), None);
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
        let empty = tag_values(&[], false);
        assert_eq!(
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&empty), None),
            json!({"tagValues": []})
        );
    }

    #[test]
    fn render_tag_names_flat_dedups_across_scopes_in_catalog_order() {
        // `env` exists in BOTH scopes — the flat projection keeps its
        // first (resource-side) occurrence only.
        let names = truncated_names();
        let flat = render_tag_names_flat(&catalog(&names), None);
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
            render_tag_names_flat(&catalog(&empty), None),
            json!({"tagNames": []})
        );
    }

    #[test]
    fn render_tag_values_flat_emits_bare_strings_without_type_or_truncated() {
        let values = tag_values(&[("checkout", "string"), ("500", "int")], true);
        assert_eq!(
            render_tag_values_flat(&values, None),
            json!({"tagValues": ["checkout", "500"]})
        );
        let empty = tag_values(&[], false);
        assert_eq!(
            render_tag_values_flat(&empty, None),
            json!({"tagValues": []})
        );
    }

    // -- issue #476: the stored type ----

    /// The stored type is reported verbatim for every spelling the writer
    /// can produce — a permutation break, not a presence check: swapping
    /// two arms of `TraceJsonScalar::val_type` moves a value here.
    #[test]
    fn a_typed_row_reports_its_stored_type_verbatim() {
        let values = tag_values(
            &[
                ("1.5", "float"),
                ("12345", "string"),
                ("2s", "string"),
                ("500", "int"),
                ("true", "bool"),
            ],
            false,
        );
        assert_eq!(
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values), None),
            json!({"tagValues": [
                {"type": "float", "value": "1.5"},
                {"type": "string", "value": "12345"},
                {"type": "string", "value": "2s"},
                {"type": "int", "value": "500"},
                {"type": "bool", "value": "true"},
            ]})
        );
    }

    /// AC4's renderer half: one key at two types is TWO entries. The
    /// sorting-key half — that both rows survive the merge at all — is
    /// held by `tag_values`' sorting key, which ends in `val_type`
    /// (`crates/pulsus-read/tests/traces_tags_explain.rs`).
    #[test]
    fn one_value_at_two_types_renders_two_entries() {
        let values = tag_values(&[("8080", "int"), ("8080", "string")], false);
        assert_eq!(
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values), None),
            json!({"tagValues": [
                {"type": "int", "value": "8080"},
                {"type": "string", "value": "8080"},
            ]})
        );
    }

    /// AC5: the v1 flat route collapses that pair back to ONE element.
    #[test]
    fn the_flat_route_deduplicates_a_value_stored_at_two_types() {
        let values = tag_values(&[("8080", "int"), ("8080", "string")], false);
        assert_eq!(
            render_tag_values_flat(&values, None),
            json!({"tagValues": ["8080"]})
        );
    }

    /// Issue #478, criterion 5b (wire half). **Span names render as
    /// `string`, whatever they look like**, and the empty name omits the
    /// `value` key entirely.
    ///
    /// The expected body is the reference's captured answer for these
    /// names, not a rendering of our own rule: it types `500`, `1.5`,
    /// `true`, `1.5s` and `-3` as `string`, where a text-classifying
    /// inference would have said `int`, `float`, `bool`, `duration` and
    /// `int`.
    ///
    /// This is the WIRE half only. It cannot see whether the engine set
    /// the type, which is why `pulsus_read`'s
    /// `traces::exec::tests::a_span_name_value_carries_an_explicit_string_type`
    /// exists beside it.
    #[test]
    fn span_names_render_as_string() {
        // What `list_span_name_values` returns: every name typed `string`.
        let values = tag_values(
            &[
                ("", "string"),
                ("-3", "string"),
                ("1.5", "string"),
                ("1.5s", "string"),
                ("500", "string"),
                ("checkout", "string"),
                ("true", "string"),
            ],
            false,
        );
        assert_eq!(
            render_tag_values(&TagValuesAnswer::Catalog(&values), None),
            json!({
                "tagValues": [
                    {"type": "string"},
                    {"type": "string", "value": "-3"},
                    {"type": "string", "value": "1.5"},
                    {"type": "string", "value": "1.5s"},
                    {"type": "string", "value": "500"},
                    {"type": "string", "value": "checkout"},
                    {"type": "string", "value": "true"},
                ],
                "truncated": false,
            })
        );
    }

    /// The store-backed answer and the static vocabulary answer are
    /// rendered by DIFFERENT arms, and confusing them is visible: a
    /// vocabulary value carries `keyword`, which is what makes the
    /// datasource emit it unquoted, and a span name carries `string`,
    /// which makes it quoted. Asserted as a pair so exchanging the two
    /// type constants fails here.
    #[test]
    fn a_vocabulary_value_is_keyword_and_a_span_name_is_string() {
        let statics: [&'static str; 1] = ["error"];
        assert_eq!(
            render_tag_values(&TagValuesAnswer::Static(&statics), None)["tagValues"],
            json!([{"type": "keyword", "value": "error"}])
        );
        let names = tag_values(&[("error", "string")], false);
        assert_eq!(
            render_tag_values(&TagValuesAnswer::Catalog(&names), None)["tagValues"],
            json!([{"type": "string", "value": "error"}])
        );
    }
}
