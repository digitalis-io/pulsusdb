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
use super::filter::byte_cap_expr;
use super::tag_narrow::NarrowTerm;
use super::window_sql::date_literal;

pub use super::spans::tags::ATTR_SCOPES;
use super::spans::tags::ATTR_SCOPES_IN;

/// The two scopes the writer reserves for INTRINSIC rows
/// (`pulsus-write`'s `otlp_traces`), ascending. Their rows hold intrinsic
/// values under reserved keys (`name`, `timeSinceStart`, `spanID`,
/// `traceID`), so an attribute lookup must never answer out of them —
/// which is what a bare-key read did before issue #475. Declared here,
/// rather than implied by absence, so `trace_scope_vocabulary.rs` can
/// state the writer/reader relation as an equality instead of two
/// presence checks.
pub const RESERVED_INTRINSIC_SCOPES: [&str; 2] = ["event:intrinsic", "link:intrinsic"];

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
/// set read 1/245 granules and a scattered 5-trace set 5/245, while the
/// real 333k-trace set read 245/245. The span-side statement is
/// therefore about the sets this feature produces (large and scattered),
/// not about the schema.
///
/// **Those three figures are one corpus's, and the scattered one is
/// corpus-shaped by definition** — how many granules five scattered ids
/// touch depends on how the ids fell. The scattered figure is `5/245`
/// as measured independently by plan review round 6 and by code review
/// round 1, each on its own 2,000,000-span corpus; an earlier revision
/// of this comment said `9/245`, which was a third corpus's number
/// carried forward as if it were the property. What is NOT
/// corpus-shaped, and is what this comment is for, is the ORDERING
/// argument above it: `trace_id` leads on one table and sits behind an
/// open range on the other.
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
    use crate::traces::TAG_VALUES_MAX;

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
    ///
    /// The absence is DEFERRED work, owned by issue #598, not an
    /// oversight: the store-backed reads keep the day-granular window
    /// until the statement whose cost is measured replaces this one.
    /// Whoever adds the row bound moves this case rather than editing
    /// the literal.
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

    /// One physical term and one attribute term, byte for byte. NO
    /// `timestamp_ns` predicate, for the reason above.
    ///
    /// The absence is DEFERRED work, owned by issue #598, not an
    /// oversight: the store-backed reads keep the day-granular window
    /// until the statement whose cost is measured replaces this one.
    /// Whoever adds the row bound moves this case rather than editing
    /// the literal.
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
    /// the span-set intersection. NO `timestamp_ns` predicate either.
    ///
    /// The absence is DEFERRED work, owned by issue #598, not an
    /// oversight: the store-backed reads keep the day-granular window
    /// until the statement whose cost is measured replaces this one.
    /// Whoever adds the row bound moves this case rather than editing
    /// the literal.
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
}
