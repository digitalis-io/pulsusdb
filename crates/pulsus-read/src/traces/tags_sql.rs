//! Pure SQL builders for the §4.3 tag-discovery reads (issues #58 and
//! #478) — the byte-frozen golden surface, same convention as
//! [`super::sql`] / [`super::search_sql`]: pre-escaped fragments →
//! `String`, no `ChClient`, no I/O.
//!
//! **Four builders, two storage stories, and the doc says which is
//! which** (issue #478 — before it, one sentence covered the module and
//! it stopped being true when the store-backed pair arrived):
//!
//! * [`tag_names_sql`] and [`tag_values_sql`] target
//!   [`CATALOG_TABLE`] ONLY — the `Replication::Global`, un-`_dist`
//!   catalog (docs/schemas.md §4.1). The table name is a compile-time
//!   constant of this module, so no caller can put a table, an alias or a
//!   subquery into their `FROM` clause. An UNNARROWED attribute-values
//!   read is still exactly [`tag_values_sql`], byte for byte.
//! * [`span_name_values_sql`] and [`attr_values_narrowed_sql`] read the
//!   SPAN tables, because the answers are not in the catalog: the
//!   catalog's MV projects `trace_attrs_idx` only, so it holds no
//!   span-`name` row at all, and it carries no span identity, so a `q`
//!   has nothing to join to there. They take the same
//!   [`SpanFilterCtx`](super::filter::SpanFilterCtx) the whole search
//!   path takes — the table names come from `TraceReadConfig`, which is
//!   what applies the `_dist` suffix when clustered.
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
//! The catalog table name is a compile-time constant of this module
//! ([`CATALOG_TABLE`]), not an input (issue #475): the only free strings
//! the two catalog builders accept are the pre-escaped literal positions
//! inside `WHERE`, so no caller can put a table, an alias or a subquery
//! into their `FROM` clause.

use super::filter::SpanFilterCtx;
use super::search_sql::{byte_cap_expr, date_literal};
use super::tag_narrow::NarrowTerm;

/// The one table both CATALOG tag-discovery reads target. NEVER `_dist`-suffixed:
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

/// The UTC day span of a request window — `[start_days, end_days]`
/// inclusive, days since epoch, rendered through
/// [`super::search_sql::date_literal`].
///
/// **The window bound is day-granular on both tables, deliberately.** A
/// sub-day `timestamp_ns` predicate on `trace_spans` prunes nothing: the
/// sorting key is `(trace_id, timestamp_ns)`, so with `trace_id`
/// unconstrained the second key column cannot prune — and it would defeat
/// the `span_name_day` projection, which is keyed on the day expression.
/// So the window resolves to the UTC days it touches and both tables
/// carry only the day clause: one rule, one ledger row, and the narrowed
/// and unnarrowed answers stay in a superset relation instead of
/// crossing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaySpan {
    pub start_days: i64,
    pub end_days: i64,
}

impl DaySpan {
    /// The UTC days a `[start_ns, end_ns]` window touches.
    pub fn from_window(start_ns: i64, end_ns: i64) -> Self {
        DaySpan {
            start_days: start_ns.div_euclid(NS_PER_DAY),
            end_days: end_ns.div_euclid(NS_PER_DAY),
        }
    }
}

const NS_PER_DAY: i64 = 86_400_000_000_000;

/// The `trace_spans` day-partition clause — the same expression the table
/// is partitioned by (`catalog.rs`, migration 16), so it prunes
/// partitions and keeps the `span_name_day` projection selectable.
fn spans_day_clause(days: DaySpan) -> String {
    format!(
        "toDate(fromUnixTimestamp64Nano(timestamp_ns)) >= {} \
         AND toDate(fromUnixTimestamp64Nano(timestamp_ns)) <= {}",
        date_literal(days.start_days),
        date_literal(days.end_days)
    )
}

/// The `trace_attrs_idx` day-partition clause on its own `date` column.
fn attrs_day_clause(days: DaySpan) -> String {
    format!(
        "date >= {} AND date <= {}",
        date_literal(days.start_days),
        date_literal(days.end_days)
    )
}

/// Renders the narrowing terms as `AND`-joined clauses over
/// `trace_spans` rows — the ONE rendering both store-backed builders
/// use, which is why they cannot disagree about what a `q` means.
///
/// A [`NarrowTerm::Physical`] is already an escaped `trace_spans` column
/// predicate and renders inline. A [`NarrowTerm::Attr`] renders as the
/// `(trace_id, span_id) IN (SELECT …)` index-served semi-join the search
/// and metrics paths already build, confined to its `(key[, val][,
/// scope])` prefix plus the day clause.
///
/// **The semi-join is a correctness mechanism, not a pruning one, and
/// which side that is structural on differs.** On `trace_attrs_idx` the
/// key is `(key, val, scope, timestamp_ns, trace_id, span_id)`, so with
/// `val` and `timestamp_ns` unconstrained the identifier columns sit
/// behind an open range and cannot prune whatever the set looks like. On
/// `trace_spans` the key is `(trace_id, timestamp_ns)`, so `trace_id`
/// leads and a set CAN exclude granules — measured, a localised 5-trace
/// set read 1/245 granules and a scattered 5-trace set 9/245, while the
/// real 245-granule-wide set read 245/245. The span-side statement is
/// therefore about the sets this feature produces on that corpus (large
/// and scattered), not about the schema.
fn term_clauses(ctx: SpanFilterCtx<'_>, terms: &[NarrowTerm], days: DaySpan) -> Vec<String> {
    terms
        .iter()
        .map(|term| match term {
            NarrowTerm::Physical(sql) => sql.clone(),
            NarrowTerm::Attr(probe) => {
                let mut predicate = probe.key_sql.clone();
                predicate.push_str(&format!(" AND {}", probe.pred_sql));
                if let Some(scope) = &probe.scope_sql {
                    predicate.push_str(&format!(" AND {scope}"));
                }
                format!(
                    "(trace_id, span_id) IN (SELECT trace_id, span_id FROM {} \
                     WHERE {} AND {predicate})",
                    ctx.attrs_table,
                    attrs_day_clause(days)
                )
            }
        })
        .collect()
}

/// `SELECT DISTINCT <byte-capped name> AS val FROM <spans> …` — the
/// span-name value read (issue #478 Part 1).
///
/// **Span names live in `trace_spans`, not in the catalog**: the catalog
/// MV selects from `trace_attrs_idx` alone, so it has no span-`name` row
/// to answer with, and what a bare `name` lookup used to reach was span
/// EVENT names under a reserved intrinsic scope. This read is
/// structurally immune to that collision — `trace_spans.name` holds span
/// names and nothing else.
///
/// With `terms` empty this renders the projection-served form and carries
/// NO `timestamp_ns` predicate: a `timestamp_ns` predicate defeats
/// `span_name_day` (migration 42), whose own key is the day expression.
///
/// The value is byte-capped by the shared [`byte_cap_expr`] helper, so a
/// name over the cap is reported the same way every other string column
/// on this surface reports one (ledger row
/// `traceql-tag-values-span-name-byte-cap`).
pub fn span_name_values_sql(
    ctx: SpanFilterCtx<'_>,
    days: DaySpan,
    terms: &[NarrowTerm],
    limit: usize,
) -> String {
    let mut sql = format!(
        "SELECT DISTINCT {} AS val\nFROM {}\nWHERE {}",
        byte_cap_expr("name"),
        ctx.spans_table,
        spans_day_clause(days)
    );
    for clause in term_clauses(ctx, terms, days) {
        sql.push_str(&format!("\n  AND {clause}"));
    }
    sql.push_str(&format!("\nORDER BY val\nLIMIT {limit}"));
    sql
}

/// `SELECT DISTINCT val, val_type FROM <attrs> …` — the NARROWED
/// attribute-values read (issue #478 Part 2).
///
/// Only called when `terms` is NON-EMPTY: an empty `terms` keeps the
/// existing [`tag_values_sql`] catalog read byte for byte, so a dropdown
/// that narrows nothing costs exactly what it did before.
///
/// The `(val, val_type)` pair and `ORDER BY val, val_type` match
/// [`tag_values_sql`]'s contract (issue #476), which is what makes rows
/// sharing a `val` contiguous for the renderer's run rule.
pub fn attr_values_narrowed_sql(
    ctx: SpanFilterCtx<'_>,
    key_literal: &str,
    scope_literal: Option<&str>,
    days: DaySpan,
    terms: &[NarrowTerm],
    limit: usize,
) -> String {
    let mut sql = format!(
        "SELECT DISTINCT val, val_type\nFROM {}\nWHERE key = {key_literal}",
        ctx.attrs_table
    );
    match scope_literal {
        Some(scope) => sql.push_str(&format!(" AND scope = {scope}")),
        None => sql.push_str(&format!(" AND scope IN {ATTR_SCOPES_IN}")),
    }
    sql.push_str(&format!("\n  AND {}", attrs_day_clause(days)));
    let mut spans = format!(
        "SELECT trace_id, span_id\n    FROM {}\n    WHERE {}",
        ctx.spans_table,
        spans_day_clause(days)
    );
    for clause in term_clauses(ctx, terms, days) {
        spans.push_str(&format!("\n      AND {clause}"));
    }
    sql.push_str(&format!(
        "\n  AND (trace_id, span_id) IN (\n    {spans}\n  )"
    ));
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

    // --- issue #478: the two store-backed builders ------------------

    const CTX: SpanFilterCtx<'static> = SpanFilterCtx {
        spans_table: "trace_spans",
        attrs_table: "trace_attrs_idx",
    };

    /// 1_700_000_000 s = 19_675 days (2023-11-14); the window ends the
    /// next UTC day.
    const DAYS: DaySpan = DaySpan {
        start_days: 19_675,
        end_days: 19_676,
    };

    fn narrow(q: &str) -> Vec<NarrowTerm> {
        crate::traces::tag_narrow::narrowing_from_query(q)
            .terms()
            .to_vec()
    }

    /// The unnarrowed span-name read, byte for byte. NO `timestamp_ns`
    /// predicate: one would defeat the `span_name_day` projection.
    #[test]
    fn unnarrowed_span_name_sql_is_byte_exact() {
        assert_eq!(
            span_name_values_sql(CTX, DAYS, &[], TAG_VALUES_MAX + 1),
            "SELECT DISTINCT if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS val\n\
             FROM trace_spans\n\
             WHERE toDate(fromUnixTimestamp64Nano(timestamp_ns)) >= toDate('2023-11-14') \
             AND toDate(fromUnixTimestamp64Nano(timestamp_ns)) <= toDate('2023-11-15')\n\
             ORDER BY val\n\
             LIMIT 1001"
        );
    }

    /// One physical term and one attribute term, byte for byte.
    #[test]
    fn narrowed_span_name_sql_is_byte_exact() {
        let terms = narrow("{resource.service.name=\"cart\" && span.http.method=\"GET\"}");
        assert_eq!(
            span_name_values_sql(CTX, DAYS, &terms, TAG_VALUES_MAX + 1),
            "SELECT DISTINCT if(length(name) <= 8192, name, substringUTF8(name, 1, 2048)) AS val\n\
             FROM trace_spans\n\
             WHERE toDate(fromUnixTimestamp64Nano(timestamp_ns)) >= toDate('2023-11-14') \
             AND toDate(fromUnixTimestamp64Nano(timestamp_ns)) <= toDate('2023-11-15')\n  \
             AND service = 'cart'\n  \
             AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx \
             WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15') \
             AND key = 'http.method' AND val = 'GET' AND scope = 'span')\n\
             ORDER BY val\n\
             LIMIT 1001"
        );
    }

    /// The narrowed attribute-values read, byte for byte: the `(val,
    /// val_type)` pair of issue #476 over the `(key, scope)` prefix, then
    /// the span-set intersection.
    #[test]
    fn narrowed_attr_values_sql_is_byte_exact() {
        let terms = narrow("{resource.service.name=\"cart\"}");
        assert_eq!(
            attr_values_narrowed_sql(
                CTX,
                &ch_string("http.status_code"),
                None,
                DAYS,
                &terms,
                TAG_VALUES_MAX + 1
            ),
            "SELECT DISTINCT val, val_type\n\
             FROM trace_attrs_idx\n\
             WHERE key = 'http.status_code' AND scope IN ('event', 'instrumentation', 'link', \
             'resource', 'span')\n  \
             AND date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')\n  \
             AND (trace_id, span_id) IN (\n    \
             SELECT trace_id, span_id\n    \
             FROM trace_spans\n    \
             WHERE toDate(fromUnixTimestamp64Nano(timestamp_ns)) >= toDate('2023-11-14') \
             AND toDate(fromUnixTimestamp64Nano(timestamp_ns)) <= toDate('2023-11-15')\n      \
             AND service = 'cart'\n  \
             )\n\
             ORDER BY val, val_type\n\
             LIMIT 1001"
        );
    }

    /// A scoped narrowed read confines to the one scope, the same
    /// `(scope, key)` prefix the catalog builder uses.
    #[test]
    fn a_scoped_narrowed_attr_read_names_its_scope() {
        let terms = narrow("{resource.service.name=\"cart\"}");
        let sql = attr_values_narrowed_sql(
            CTX,
            &ch_string("http.method"),
            Some(&ch_string("span")),
            DAYS,
            &terms,
            TAG_VALUES_MAX + 1,
        );
        assert!(
            sql.contains("WHERE key = 'http.method' AND scope = 'span'"),
            "{sql}"
        );
    }

    /// The store-backed builders name the tables their caller's config
    /// resolved — which is how they pick up the `_dist` suffix when
    /// clustered, where the catalog builders never can.
    #[test]
    fn the_store_backed_builders_name_the_configured_tables() {
        let ctx = SpanFilterCtx {
            spans_table: "trace_spans_dist",
            attrs_table: "trace_attrs_idx_dist",
        };
        let sql = span_name_values_sql(ctx, DAYS, &[], TAG_VALUES_MAX + 1);
        assert!(sql.contains("FROM trace_spans_dist"), "{sql}");
        let terms = narrow("{span.http.method=\"GET\"}");
        let sql =
            attr_values_narrowed_sql(ctx, &ch_string("k"), None, DAYS, &terms, TAG_VALUES_MAX + 1);
        assert!(sql.contains("FROM trace_attrs_idx_dist"), "{sql}");
        assert!(sql.contains("FROM trace_spans_dist"), "{sql}");
    }

    /// A hostile key inside a narrowing term arrives escaped and stays
    /// inside its string literal — the probe's key is escaped HERE, not
    /// by the caller.
    #[test]
    fn a_hostile_narrowing_key_stays_a_string_literal() {
        let terms = narrow("{span.[\"k'; DROP TABLE x; --\"]=\"v\"}");
        let sql = span_name_values_sql(CTX, DAYS, &terms, TAG_VALUES_MAX + 1);
        assert!(sql.contains("key = 'k\\'; DROP TABLE x; --'"), "{sql}");
    }

    /// The day span is the UTC days the window touches, on both sides
    /// of the epoch.
    #[test]
    fn a_day_span_covers_the_windows_utc_days() {
        assert_eq!(
            DaySpan::from_window(1_700_000_000_000_000_000, 1_700_010_800_000_000_000),
            DAYS
        );
        assert_eq!(
            DaySpan::from_window(0, 0),
            DaySpan {
                start_days: 0,
                end_days: 0
            }
        );
    }

    /// The property the deleted `chconfig` test used to assert about a
    /// config field, stated about the emitted SQL instead (issue #475):
    /// no argument list makes either CATALOG builder name a `_dist`
    /// table. The two store-backed builders of issue #478 are outside
    /// this claim by design — their tables come from `TraceReadConfig`,
    /// which is what applies the suffix, and
    /// `the_store_backed_builders_name_the_configured_tables` asserts
    /// that instead.
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
            assert!(!sql.contains("_dist"), "{sql}");
        }
    }
}
