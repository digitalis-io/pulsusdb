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
//! text** (issue #476). `trace_tag_catalog.val_type` carries the OTLP kind
//! the sender sent, put there at ingest by
//! `pulsus_write::ingest::traces::AttrValueType` and projected by
//! `trace_tag_catalog_mv`. Nothing in this module inspects a character of
//! `val` to decide a type. The text-classifying helper that used to —
//! `bool` for `"true"`, `duration` for anything the duration-literal
//! parser accepted, `int` for digits, `float` for an `f64` parse — is
//! DELETED, not kept for legacy rows, along with the `duration` category,
//! which the catalog can no longer emit and which the reference never
//! emits for an attribute either. Nothing replaced it: see
//! `pulsus_read::TagValue` for why the stored columns cannot type a
//! legacy row either.
//!
//! **The legacy window, stated where the code is.** A row written before
//! migration 41 has `val_type = ""`. It is reported as `string`, and it
//! CANNOT be corrected from what is stored — a string `"1.5"` and a
//! double `1.5` are byte-identical in the catalog, and the numeric
//! companion column is itself a parse of that same text rather than a
//! record of the sender's type (`pulsus_read::TagValue`). Reporting
//! `string` invents nothing and is what those rows already reported for
//! non-numeric text. The window has no end on its
//! own: `trace_tag_catalog` has NO TTL, so legacy rows never age out. It
//! closes when
//!
//! ```sql
//! SELECT count() FROM trace_tag_catalog WHERE val_type = ''
//! ```
//!
//! returns `0` on every deployment — at which point the empty-`val_type`
//! branch of [`entry_type`] and the run rule's drop of an untyped sibling
//! are dead code. Getting there needs a catalog rebuild-or-clear
//! mechanism, which is deliberately not designed here.

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
    /// A catalog read: the stored type per value, `truncated` carried.
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

/// The wire `type` for one catalog row (issue #476): the stored type, or
/// `string` when the row predates migration 41 and carries none. See the
/// module doc for why no better answer exists for such a row and for the
/// query that decides when this branch is dead.
fn entry_type(val_type: &str) -> &str {
    if val_type.is_empty() {
        STRING_TYPE
    } else {
        val_type
    }
}

/// The wire spelling an untyped legacy row reports.
const STRING_TYPE: &str = "string";

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
/// **The run rule** (issue #476). Rows arrive `ORDER BY val, val_type`, so
/// rows sharing a `val` are contiguous and the empty `val_type` sorts
/// FIRST inside its run. For each run: if any row carries a non-empty
/// `val_type`, emit one entry per distinct non-empty type and DROP the
/// empty-`val_type` row; otherwise emit one `string` entry. Without it a
/// rolling upgrade — an un-upgraded node still writing rows with no
/// `val_type` beside an upgraded node writing typed ones — shows the same
/// value twice, `{"type":"string","value":"500"}` beside
/// `{"type":"int","value":"500"}`, for one attribute. Migration 41 puts
/// `val_type` in the sorting key precisely so both rows SURVIVE the merge,
/// so the store cannot collapse them and this pass is the only thing that
/// can.
///
/// Stated edge: if the `LIMIT cap + 1` probe splits a run, the tail is
/// missing and `truncated` is already `true`; that is not chased further.
///
/// One pass over at most `TAG_VALUES_MAX` entries, no extra query, and no
/// character of `val` is read.
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
        TagValuesAnswer::Catalog(values) => {
            let mut out: Vec<Value> = Vec::with_capacity(values.values.len());
            let mut i = 0;
            while i < values.values.len() {
                let val = values.values[i].val.as_str();
                let mut run_end = i;
                while run_end < values.values.len() && values.values[run_end].val == val {
                    run_end += 1;
                }
                let run = &values.values[i..run_end];
                let mut emitted = 0;
                for v in run {
                    // An untyped row is DROPPED when a typed sibling
                    // shares its value, and answered by the fallback
                    // below when it does not.
                    if v.val_type.is_empty() {
                        continue;
                    }
                    out.push(entry(entry_type(&v.val_type), val));
                    emitted += 1;
                }
                if emitted == 0 {
                    out.push(entry(entry_type(""), val));
                }
                i = run_end;
            }
            out
        }
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
///
/// Deduplicates on `val` ALONE (issue #476): the underlying read now
/// returns one row per `(value, type)` pair, so a key holding a string
/// `"8080"` and an int `8080` yields two rows whose flat projection would
/// otherwise be `["8080","8080"]`. Rows sharing a `val` are contiguous by
/// the read's `ORDER BY`, so this is a first-occurrence pass, not a set.
pub(crate) fn render_tag_values_flat(values: &TagValues) -> Value {
    let mut flat: Vec<&str> = Vec::with_capacity(values.values.len());
    for v in &values.values {
        if flat.last() != Some(&v.val.as_str()) {
            flat.push(v.val.as_str());
        }
    }
    json!({"tagValues": flat})
}

#[cfg(test)]
mod tests {
    use super::*;

    use pulsus_read::TagValue;

    /// `(value, stored type)` pairs as the engine hands them over —
    /// ALREADY in the read's `ORDER BY val, val_type` order, which the run
    /// rule depends on. An empty type is a pre-migration-41 row.
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
        let values = tag_values(&[("checkout", "string"), ("500", "int")], false);
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
        let empty = tag_values(&[], false);
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
        let values = tag_values(&[("", "string"), ("x", "string")], false);
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
        let values = tag_values(&[("checkout", "string"), ("500", "int")], true);
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
        let empty = tag_values(&[], false);
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
        let values = tag_values(&[("checkout", "string"), ("500", "int")], true);
        assert_eq!(
            render_tag_values_flat(&values),
            json!({"tagValues": ["checkout", "500"]})
        );
        let empty = tag_values(&[], false);
        assert_eq!(render_tag_values_flat(&empty), json!({"tagValues": []}));
    }

    // -- issue #476: the stored type, the legacy window, the run rule ----

    /// AC7a's hermetic half. A legacy row whose text READS as a number is
    /// `string`. `1.5` is the whole assertion: a text parse says `float`,
    /// so did the classifier this issue deleted, and so does any rule
    /// sourced from the catalog's numeric companion column, which is
    /// itself that same parse taken at write time. Only "report the stored
    /// type, and `string` when there is none" says `string`.
    #[test]
    fn a_legacy_row_reports_string_however_its_text_reads() {
        for text in ["1.5", "12345", "-7", "true", "2s", "alpha", ""] {
            let values = tag_values(&[(text, "")], false);
            let rendered = render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values));
            let expected = if text.is_empty() {
                json!({"tagValues": [{"type": "string"}]})
            } else {
                json!({"tagValues": [{"type": "string", "value": text}]})
            };
            assert_eq!(rendered, expected, "legacy row {text:?}");
        }
    }

    /// The stored type is reported verbatim for every spelling the writer
    /// can produce — a permutation break, not a presence check: swapping
    /// two arms of `AttrValueType::as_str` moves a value here.
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
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values)),
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
    /// gated live in `crates/pulsus-schema/tests/live_traces.rs`.
    #[test]
    fn one_value_at_two_types_renders_two_entries() {
        let values = tag_values(&[("8080", "int"), ("8080", "string")], false);
        assert_eq!(
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values)),
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
            render_tag_values_flat(&values),
            json!({"tagValues": ["8080"]})
        );
    }

    /// AC19's renderer half: the rolling-upgrade shape. An un-upgraded
    /// node writes `('span','http.status_code','500','')`; an upgraded one
    /// writes the same value as `int`. Both rows survive the merge (the
    /// sorting key keeps them), so only this rule can stop the wire
    /// showing `500` twice.
    ///
    /// The INTEGER fixture is the discriminating one. A pair of legacy
    /// STRING rows renders `[string, string]` without the rule, which any
    /// later output deduplication would also collapse — so that fixture
    /// cannot tell the rule from a dedupe. `[string, int]` is not
    /// collapsible by value, so only the run rule produces one entry.
    #[test]
    fn a_rolling_upgrade_duplicate_collapses_to_the_typed_row() {
        let values = tag_values(&[("500", ""), ("500", "int")], false);
        assert_eq!(
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values)),
            json!({"tagValues": [{"type": "int", "value": "500"}]})
        );
        // ...and the untyped row is dropped, not merely reordered.
        let rendered = render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values));
        assert_eq!(rendered["tagValues"].as_array().expect("array").len(), 1);
    }

    /// The run rule keeps runs apart: a legacy row for one value must not
    /// be silenced by a typed row for a DIFFERENT value.
    #[test]
    fn the_run_rule_does_not_leak_across_values() {
        let values = tag_values(&[("a", ""), ("b", ""), ("b", "int"), ("c", "")], false);
        assert_eq!(
            render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values)),
            json!({"tagValues": [
                {"type": "string", "value": "a"},
                {"type": "int", "value": "b"},
                {"type": "string", "value": "c"},
            ]})
        );
    }

    /// The empty string never reaches the wire as a `type`.
    #[test]
    fn no_rendered_entry_carries_an_empty_type() {
        let values = tag_values(&[("x", ""), ("y", "int"), ("y", "")], false);
        for entry in render_tag_values_typed_v2(&TagValuesAnswer::Catalog(&values))["tagValues"]
            .as_array()
            .expect("array")
        {
            assert_ne!(entry["type"], json!(""), "{entry}");
        }
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
    /// the type or left it empty — `entry_type("")` renders `string` too —
    /// which is why `pulsus_read`'s
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
            render_tag_values(&TagValuesAnswer::Catalog(&values)),
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
            render_tag_values(&TagValuesAnswer::Static(&statics))["tagValues"],
            json!([{"type": "keyword", "value": "error"}])
        );
        let names = tag_values(&[("error", "string")], false);
        assert_eq!(
            render_tag_values(&TagValuesAnswer::Catalog(&names))["tagValues"],
            json!([{"type": "string", "value": "error"}])
        );
    }
}
