//! Pure SQL builders for the §4.3 tag-discovery reads (issue #58) — the
//! byte-frozen golden surface, same convention as [`super::sql`] /
//! [`super::search_sql`]: pre-escaped fragments → `String`, no
//! `ChClient`, no I/O. Both queries target `trace_tag_catalog` ONLY —
//! the `Replication::Global`, un-`_dist` catalog (docs/schemas.md §4.1):
//! discovery never reads `trace_spans`/`trace_attrs_idx`/span payloads.
//!
//! `DISTINCT` collapses the ReplacingMergeTree's not-yet-merged
//! duplicates (never `FINAL`); `ORDER BY` follows the catalog's own
//! `(scope, key, val)` sorting key so scoped reads stay index-ordered.
//! The `LIMIT` is the caller's cap **+ 1** — the truncation probe (the
//! search path's `gen_cap + 1` convention, issue #58 plan v2 Δ3): the
//! engine returns `cap` rows plus `truncated = true` when the probe row
//! appears, never an indistinguishable silent subset.
//!
//! Callers pre-escape `scope_literal`/`key_literal` via
//! [`crate::logql::escape::ch_string`] (quotes included) before they
//! reach these builders — that is the injection boundary, not this
//! module.
//!
//! A read is confined to ONE scope or to [`ATTR_SCOPES`]; there is no
//! form that reads every scope in the table, so the writer-reserved
//! intrinsic scopes ([`RESERVED_INTRINSIC_SCOPES`]) can never answer an
//! attribute lookup or appear in a tag listing (issue #475).
//!
//! The table name is a compile-time constant of this module
//! ([`CATALOG_TABLE`]), not an input (issue #475): the only free strings
//! either builder accepts are the two pre-escaped literal positions
//! inside `WHERE`, so no caller can put a table, an alias or a subquery
//! into the `FROM` clause.

/// The one table both tag-discovery reads target. NEVER `_dist`-suffixed:
/// migration 18 is `Replication::Global, family: None`, so no `_dist`
/// wrapper exists to name and every catalog read is a local-replica
/// primary-key-prefix scan with no coordinator fan-out (docs/schemas.md
/// §4.1/§7). It is a constant of this module rather than a parameter or a
/// config field precisely so that no caller can put anything else — a
/// table, an alias, or a subquery — into the `FROM` clause.
const CATALOG_TABLE: &str = "trace_tag_catalog";

/// The five scopes `trace_attrs_idx` carries for sender-supplied
/// ATTRIBUTES, ascending (issue #475). This constant is the single source
/// of truth for the whole scope surface: the SQL `IN` list below, the
/// `scope=` accept list (`traces_api::params::parse_tags_params`) and the
/// `{tag}` prefix set (`parse_tag_lookup`) all derive from it.
pub const ATTR_SCOPES: [&str; 5] = ["event", "instrumentation", "link", "resource", "span"];

/// The two scopes the writer reserves for INTRINSIC rows
/// (`pulsus-write`'s `otlp_traces`), ascending. Their rows hold intrinsic
/// values under reserved keys (`name`, `timeSinceStart`, `spanID`,
/// `traceID`), so an attribute lookup must never answer out of them —
/// which is what a bare-key read did before issue #475. Declared here,
/// rather than implied by absence, so `trace_scope_vocabulary.rs` can
/// state the writer/reader relation as an equality instead of two
/// presence checks.
pub const RESERVED_INTRINSIC_SCOPES: [&str; 2] = ["event:intrinsic", "link:intrinsic"];

/// The rendered SQL `IN` list for [`ATTR_SCOPES`]. A literal, so the
/// builders stay pure concatenation; pinned against the constant by
/// `attr_scopes_in_list_matches_the_constant`.
const ATTR_SCOPES_IN: &str = "('event', 'instrumentation', 'link', 'resource', 'span')";

/// The `GET /api/traces/v1/tags` read: distinct `(scope, key)` pairs,
/// either confined to one scope (a `(scope)` primary-key-prefix prune) or
/// to [`ATTR_SCOPES`] (an `IN` list on the same leading column, so the
/// unscoped form prunes too and the writer-reserved intrinsic scopes can
/// never be listed as attribute tags — issue #475).
pub fn tag_names_sql(scope_literal: Option<&str>, limit: usize) -> String {
    let mut sql = format!("SELECT DISTINCT scope, key\nFROM {CATALOG_TABLE}\n");
    match scope_literal {
        Some(scope) => sql.push_str(&format!("WHERE scope = {scope}\n")),
        None => sql.push_str(&format!("WHERE scope IN {ATTR_SCOPES_IN}\n")),
    }
    sql.push_str(&format!("ORDER BY scope, key\nLIMIT {limit}"));
    sql
}

/// The `GET /api/traces/v1/tag/{tag}/values` read: distinct
/// `(val, val_type)` PAIRS for one key, either confined to one scope (a
/// `(scope, key)` prefix prune) or to [`ATTR_SCOPES`] (issue #475). A
/// bare-key lookup answers out of the five attribute scopes ONLY: the two
/// reserved intrinsic scopes hold intrinsic values under keys a sender can
/// also use (`name`, `spanID`), and before this predicate existed those
/// rows answered attribute lookups.
///
/// The pair, not the value alone (issue #476): the type is per VALUE, and
/// one key can hold a string `'8080'` and an int `8080`, which the wire
/// reports as two entries. `ORDER BY val, val_type` is the catalog's own
/// sorting order inside a fixed `(scope, key)` prefix, so it adds no sort
/// for the scoped shapes — and it is what makes rows sharing a `val`
/// CONTIGUOUS, which the renderer's run rule depends on. The empty string
/// sorts first inside a run, so a legacy row precedes its typed sibling.
///
/// Consequence for the cap, documented in docs/api.md §4.3: the
/// `LIMIT cap + 1` probe and `truncated` now count `(value, type)` pairs,
/// so a key holding one value at two types spends two of them.
pub fn tag_values_sql(key_literal: &str, scope_literal: Option<&str>, limit: usize) -> String {
    let mut sql =
        format!("SELECT DISTINCT val, val_type\nFROM {CATALOG_TABLE}\nWHERE key = {key_literal}");
    match scope_literal {
        Some(scope) => sql.push_str(&format!(" AND scope = {scope}")),
        None => sql.push_str(&format!(" AND scope IN {ATTR_SCOPES_IN}")),
    }
    sql.push_str(&format!("\nORDER BY val, val_type\nLIMIT {limit}"));
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::logql::escape::ch_string;
    use crate::traces::{TAG_NAMES_MAX, TAG_VALUES_MAX};

    /// AC1: all four documented forms, byte-for-byte (the `LIMIT` is the
    /// cap + 1 truncation probe — plan v2 Δ3).
    #[test]
    fn scoped_tag_names_sql_is_byte_exact() {
        assert_eq!(
            tag_names_sql(Some(&ch_string("resource")), TAG_NAMES_MAX + 1),
            "SELECT DISTINCT scope, key\n\
             FROM trace_tag_catalog\n\
             WHERE scope = 'resource'\n\
             ORDER BY scope, key\n\
             LIMIT 10001"
        );
    }

    #[test]
    fn unscoped_tag_names_sql_is_byte_exact() {
        assert_eq!(
            tag_names_sql(None, TAG_NAMES_MAX + 1),
            "SELECT DISTINCT scope, key\n\
             FROM trace_tag_catalog\n\
             WHERE scope IN ('event', 'instrumentation', 'link', 'resource', 'span')\n\
             ORDER BY scope, key\n\
             LIMIT 10001"
        );
    }

    #[test]
    fn scoped_tag_values_sql_is_byte_exact() {
        assert_eq!(
            tag_values_sql(
                &ch_string("service.name"),
                Some(&ch_string("resource")),
                TAG_VALUES_MAX + 1
            ),
            "SELECT DISTINCT val, val_type\n\
             FROM trace_tag_catalog\n\
             WHERE key = 'service.name' AND scope = 'resource'\n\
             ORDER BY val, val_type\n\
             LIMIT 1001"
        );
    }

    #[test]
    fn unscoped_tag_values_sql_is_byte_exact() {
        assert_eq!(
            tag_values_sql(&ch_string("service.name"), None, TAG_VALUES_MAX + 1),
            "SELECT DISTINCT val, val_type\n\
             FROM trace_tag_catalog\n\
             WHERE key = 'service.name' AND scope IN ('event', 'instrumentation', 'link', \
             'resource', 'span')\n\
             ORDER BY val, val_type\n\
             LIMIT 1001"
        );
    }

    /// The injection boundary holds: a hostile key arrives pre-escaped
    /// and stays inside its string literal.
    #[test]
    fn a_pre_escaped_hostile_key_stays_a_string_literal() {
        let sql = tag_values_sql(&ch_string("k'; DROP TABLE x; --"), None, TAG_VALUES_MAX + 1);
        assert!(
            sql.contains("WHERE key = 'k\\'; DROP TABLE x; --'"),
            "{sql}"
        );
    }

    /// Issue #475 AC17: the rendered `IN` list is the constant, not a
    /// second hand-maintained list. Reordering or editing [`ATTR_SCOPES`]
    /// without editing [`ATTR_SCOPES_IN`] fails here rather than silently
    /// widening or narrowing what a bare-key lookup reads.
    #[test]
    fn attr_scopes_in_list_matches_the_constant() {
        let rendered = ATTR_SCOPES
            .iter()
            .map(|s| ch_string(s))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(ATTR_SCOPES_IN, format!("({rendered})"));
    }

    /// Issue #475: the two reader scope lists are disjoint, so a scope
    /// keyword can never be both an attribute scope and a reserved one.
    #[test]
    fn the_two_scope_lists_are_disjoint() {
        for reserved in RESERVED_INTRINSIC_SCOPES {
            assert!(
                !ATTR_SCOPES.contains(&reserved),
                "{reserved} is in both scope lists"
            );
        }
    }

    /// The property the deleted `chconfig` test used to assert about a
    /// config field, stated about the emitted SQL instead (issue #475):
    /// no argument list makes either builder name a `_dist` table.
    #[test]
    fn neither_builder_can_name_a_dist_table() {
        let scope = ch_string("resource");
        let key = ch_string("service.name");
        for sql in [
            tag_names_sql(Some(&scope), TAG_NAMES_MAX + 1),
            tag_names_sql(None, TAG_NAMES_MAX + 1),
            tag_values_sql(&key, Some(&scope), TAG_VALUES_MAX + 1),
            tag_values_sql(&key, None, TAG_VALUES_MAX + 1),
        ] {
            assert!(!sql.contains("_dist"), "{sql}");
        }
    }
}
