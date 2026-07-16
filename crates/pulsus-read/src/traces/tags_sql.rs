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

/// The `GET /api/traces/v1/tags` read: distinct `(scope, key)` pairs,
/// optionally confined to one scope (a `(scope)` primary-key-prefix
/// prune; the unscoped form is a full — small — catalog scan,
/// docs/schemas.md §4.1).
pub fn tag_names_sql(catalog_table: &str, scope_literal: Option<&str>, limit: usize) -> String {
    let mut sql = format!("SELECT DISTINCT scope, key\nFROM {catalog_table}\n");
    if let Some(scope) = scope_literal {
        sql.push_str(&format!("WHERE scope = {scope}\n"));
    }
    sql.push_str(&format!("ORDER BY scope, key\nLIMIT {limit}"));
    sql
}

/// The `GET /api/traces/v1/tag/{tag}/values` read: distinct `val`s for
/// one key, optionally scope-confined (a `(scope, key)` prefix prune;
/// the unscoped form cannot prune the leading `scope` column and is
/// documented as a full — small — catalog scan, docs/schemas.md §4.1).
pub fn tag_values_sql(
    catalog_table: &str,
    key_literal: &str,
    scope_literal: Option<&str>,
    limit: usize,
) -> String {
    let mut sql = format!("SELECT DISTINCT val\nFROM {catalog_table}\nWHERE key = {key_literal}");
    if let Some(scope) = scope_literal {
        sql.push_str(&format!(" AND scope = {scope}"));
    }
    sql.push_str(&format!("\nORDER BY val\nLIMIT {limit}"));
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
            tag_names_sql(
                "trace_tag_catalog",
                Some(&ch_string("resource")),
                TAG_NAMES_MAX + 1
            ),
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
            tag_names_sql("trace_tag_catalog", None, TAG_NAMES_MAX + 1),
            "SELECT DISTINCT scope, key\n\
             FROM trace_tag_catalog\n\
             ORDER BY scope, key\n\
             LIMIT 10001"
        );
    }

    #[test]
    fn scoped_tag_values_sql_is_byte_exact() {
        assert_eq!(
            tag_values_sql(
                "trace_tag_catalog",
                &ch_string("service.name"),
                Some(&ch_string("resource")),
                TAG_VALUES_MAX + 1
            ),
            "SELECT DISTINCT val\n\
             FROM trace_tag_catalog\n\
             WHERE key = 'service.name' AND scope = 'resource'\n\
             ORDER BY val\n\
             LIMIT 1001"
        );
    }

    #[test]
    fn unscoped_tag_values_sql_is_byte_exact() {
        assert_eq!(
            tag_values_sql(
                "trace_tag_catalog",
                &ch_string("service.name"),
                None,
                TAG_VALUES_MAX + 1
            ),
            "SELECT DISTINCT val\n\
             FROM trace_tag_catalog\n\
             WHERE key = 'service.name'\n\
             ORDER BY val\n\
             LIMIT 1001"
        );
    }

    /// The injection boundary holds: a hostile key arrives pre-escaped
    /// and stays inside its string literal.
    #[test]
    fn a_pre_escaped_hostile_key_stays_a_string_literal() {
        let sql = tag_values_sql(
            "trace_tag_catalog",
            &ch_string("k'; DROP TABLE x; --"),
            None,
            TAG_VALUES_MAX + 1,
        );
        assert!(
            sql.contains("WHERE key = 'k\\'; DROP TABLE x; --'"),
            "{sql}"
        );
    }
}
