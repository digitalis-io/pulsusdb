//! The two catalog reads of tag discovery (issue #598 part 1): tag names
//! from `tag_names`, unnarrowed tag values from `tag_values`.
//!
//! Pure SQL builders: pre-escaped fragments → `String`, no `ChClient`, no
//! I/O. Callers pre-escape `scope_literal`/`key_literal` via
//! [`crate::logql::escape::ch_string`] (quotes included); that is the
//! injection boundary, not this module.
//!
//! The table names are private constants of this module, not inputs, so
//! no caller can put a table, an alias or a subquery into the `FROM`
//! clause. Both catalogs are replicated to every node, so there is no
//! `_dist` table to name. `DISTINCT` collapses a not-yet-merged duplicate
//! (both tables are ordered by their whole row, so the duplicate is
//! byte-identical), never `FINAL`. The `LIMIT` is the caller's cap + 1,
//! the truncation probe.

/// The five scopes the writer catalogues for sender-supplied ATTRIBUTES,
/// ascending (issue #475). This constant is the single source of truth
/// for the whole scope surface: the SQL `IN` list below, the `scope=`
/// accept list (`traces_api::params::parse_tags_params`) and the `{tag}`
/// prefix set (`parse_tag_lookup`) all derive from it.
pub const ATTR_SCOPES: [&str; 5] = ["event", "instrumentation", "link", "resource", "span"];

/// The rendered SQL `IN` list for [`ATTR_SCOPES`]. A literal, so the
/// builders stay pure concatenation; pinned against the constant by
/// `attr_scopes_in_list_matches_the_constant`.
pub(crate) const ATTR_SCOPES_IN: &str = "('event', 'instrumentation', 'link', 'resource', 'span')";

const TAG_NAMES_TABLE: &str = "tag_names";
const TAG_VALUES_TABLE: &str = "tag_values";

/// The `GET /api/traces/v1/tags` read: distinct `(scope, key)` pairs from
/// `tag_names`, confined to one scope (a `(scope)` primary-key prefix) or
/// to [`ATTR_SCOPES`] (an `IN` list on the same leading column).
pub fn tag_names_sql(scope_literal: Option<&str>, limit: usize) -> String {
    let mut sql = format!("SELECT DISTINCT scope, key\nFROM {TAG_NAMES_TABLE}\n");
    match scope_literal {
        Some(scope) => sql.push_str(&format!("WHERE scope = {scope}\n")),
        None => sql.push_str(&format!("WHERE scope IN {ATTR_SCOPES_IN}\n")),
    }
    sql.push_str(&format!("ORDER BY scope, key\nLIMIT {limit}"));
    sql
}

/// The unnarrowed `GET /api/traces/v1/tag/{tag}/values` read: distinct
/// `(value, val_type)` pairs for one key from `tag_values`, confined to
/// one scope (a `(scope, key)` prefix) or to [`ATTR_SCOPES`]. `value` is
/// projected as `val`, the field every caller decodes. The pair, not the
/// value alone (issue #476): one key can hold a string `'8080'` and an
/// int `8080`, which the wire reports as two entries, and the `LIMIT`
/// probe counts pairs.
pub fn tag_values_sql(key_literal: &str, scope_literal: Option<&str>, limit: usize) -> String {
    let mut sql = format!(
        "SELECT DISTINCT value AS val, val_type\nFROM {TAG_VALUES_TABLE}\nWHERE key = {key_literal}"
    );
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

    /// All four documented forms, byte for byte (the `LIMIT` is the cap
    /// + 1 truncation probe).
    #[test]
    fn scoped_tag_names_sql_is_byte_exact() {
        assert_eq!(
            tag_names_sql(Some(&ch_string("resource")), TAG_NAMES_MAX + 1),
            "SELECT DISTINCT scope, key\n\
             FROM tag_names\n\
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
             FROM tag_names\n\
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
            "SELECT DISTINCT value AS val, val_type\n\
             FROM tag_values\n\
             WHERE key = 'service.name' AND scope = 'resource'\n\
             ORDER BY val, val_type\n\
             LIMIT 1001"
        );
    }

    #[test]
    fn unscoped_tag_values_sql_is_byte_exact() {
        assert_eq!(
            tag_values_sql(&ch_string("service.name"), None, TAG_VALUES_MAX + 1),
            "SELECT DISTINCT value AS val, val_type\n\
             FROM tag_values\n\
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
    /// second hand-maintained list.
    #[test]
    fn attr_scopes_in_list_matches_the_constant() {
        let rendered = ATTR_SCOPES
            .iter()
            .map(|s| ch_string(s))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(ATTR_SCOPES_IN, format!("({rendered})"));
    }

    /// No argument list makes either catalog builder name a `_dist`
    /// table: both catalogs are replicated to every node (issue #475).
    #[test]
    fn neither_catalog_builder_can_name_a_dist_table() {
        let scope = ch_string("resource");
        let key = ch_string("service.name");
        for sql in [
            tag_names_sql(Some(&scope), TAG_NAMES_MAX + 1),
            tag_names_sql(None, TAG_NAMES_MAX + 1),
            tag_values_sql(&key, Some(&scope), TAG_VALUES_MAX + 1),
            tag_values_sql(&key, None, TAG_VALUES_MAX + 1),
        ] {
            assert!(!sql.is_empty() && !sql.contains("_dist"), "{sql}");
        }
    }
}
