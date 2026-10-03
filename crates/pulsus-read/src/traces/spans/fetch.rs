//! The trace fetch's three pure SQL builders (issue #587):
//! `validated inputs -> String`, no `ChClient`, no I/O — the same
//! convention as [`crate::traces::search_sql`], and the surface
//! `tests`-level snapshots freeze.
//!
//! Callers validate `hex32` before it reaches these builders — that is the
//! injection boundary, not this module. Only `[0-9a-f]{32}` can ever reach
//! an `unhex('...')` literal.
//!
//! ## The three statements
//!
//! | builder | when | projects |
//! |---|---|---|
//! | [`indexed_fetch_sql`] | every fetch, always, first | `index_rows`, `bucket_count`, `spans`, `resources` |
//! | [`wide_fetch_sql`] | `bucket_count >= BUCKET_SET_CAP` — the stored set may have truncated | `spans`, `resources` |
//! | [`fallback_fetch_sql`] | the per-trace table has not indexed the trace and the request supplied a window | `spans`, `resources` |
//!
//! ## Three properties of the indexed statement, each load-bearing
//!
//! **`toUInt32(length(bk))`, not the bare `length`.** `length` over an
//! array returns `UInt64`, so a bare projection would report `UInt64`
//! where the row requires `UInt32` and every indexed fetch would end in a
//! schema mismatch. The cast is in the statement rather than the row's
//! type being widened: the value's domain is `0..=4096`, so `UInt32` is
//! the type that describes it, and the cast is inside the text the
//! byte-freeze case holds, so the projected type cannot drift from the
//! row's silently.
//!
//! **`groupUniqArrayArray(4096)(buckets)` caps the outer union.** Each
//! stored row is capped at 4,096 by the view's expression, by the column's
//! declaration on insert and by the same function on merge — but a trace
//! straddling midnight has several rows whose sets this statement unions,
//! and an uncapped union over them is bounded only by `4096 x rows`.
//! Measured, a truncated row unioned with one further bucket is `4097`.
//! Under the default settings `FINAL` collapses the day rows before the
//! statement's aggregate runs, applying the column's own capped merge, so
//! the outer cap is **insurance rather than mechanism** there; it is
//! load-bearing with `do_not_merge_across_partitions_select_final = 1`,
//! which the engine offers as a performance setting and which nothing here
//! forbids. A read path whose correctness depends on a performance setting
//! staying off is a defect waiting for whoever turns it on.
//!
//! **`AND length(bk) < 4096` suppresses the span array on the truncated
//! route.** `bk` is bound in a `WITH` before the span read, so the
//! conjunct is a constant at analysis time: with a complete set it is
//! `true` and nothing changes; with a set that may have truncated it is
//! `false`, the span read matches no row, and `sp.1` is empty. The
//! complete-predicate statement then carries the whole trace, so the two
//! bodies carry **one** copy of it between them. This avoids sending the
//! same bytes twice; it returns nothing less. Delete the conjunct and the
//! response is byte-identical — only the wire traffic changes.
//!
//! ## What the complete-predicate statement trades
//!
//! It carries **no bucket condition at all**: a stored set that may be
//! incomplete would make the key `IN` exclude rows, and a wrong answer is
//! worse than a slow one. It reads its own bounds off the per-trace table
//! rather than taking them from the indexed statement, which is why the
//! indexed statement projects neither. Its partition clause must be **byte
//! identical** to the span table's `PARTITION BY` expression or the prune
//! is lost, which is why all three of its date conversions carry an
//! explicit `'UTC'`.

use super::rows::FetchWindow;
use crate::traces::window_sql::{WindowSql, resources_day_bound};

/// The stored bucket set's per-row cap, in the per-trace table's column
/// type. A set of this length **may** be truncated, so the reader takes
/// the complete-predicate route at `>=` and not at `==`: an uncapped union
/// of a truncated row with one further bucket is `4097`, measured, so
/// equality is false exactly when truncation has happened.
pub const BUCKET_SET_CAP: u32 = 4096;

/// The five-minute bucket the span table's leading sort-key column
/// divides by.
pub const BUCKET_NS: i64 = 300_000_000_000;

/// The 25 span elements and the 6 resource elements every statement
/// projects, and the two shared blocks they sit in. **One owning text**:
/// three statements render the same span scalar and the same resource
/// subquery, and a second copy is a second place for the projection to
/// drift from the row types.
const SPAN_SCALAR: &str = r"(SELECT (groupArray((span_id, parent_span_id, start_ns, end_ns, service,
                          resource_id, name, kind, status_code, status_message,
                          trace_state, flags, scope_name, scope_version, scope_attrs,
                          attrs, attrs_other, dropped_attrs, events, dropped_events,
                          links, dropped_links,
                          scope_schema_url, scope_dropped_attrs, scope_attrs_other)),
              groupUniqArray((service, resource_id)))
      FROM {spans}
      WHERE {predicate}) AS sp";

/// The grouped resource read, shared by all three statements.
///
/// The group key is `resource_id` alone and not `(service, resource_id)`:
/// equal `resource_id` implies equal attributes implies equal service, so
/// the narrower key cannot merge two services, and the five `any(...)`
/// return *the* value rather than *a* value because every row in a group
/// carries the same five. `(service, resource_id)` is used in the
/// **predicate**, where it is the whole sorting key and prunes.
const RESOURCE_SUBQUERY: &str = r"       (SELECT groupArray((resource_id, attrs, attrs_other, dropped_attrs,
                           schema_url, entity_refs))
        FROM (SELECT resource_id,
                     any(attrs) AS attrs, any(attrs_other) AS attrs_other,
                     any(dropped_attrs) AS dropped_attrs, any(schema_url) AS schema_url,
                     any(entity_refs) AS entity_refs
              FROM {resources}
              WHERE {day}
                AND (service, resource_id) IN (SELECT arrayJoin(sp.2))
              GROUP BY resource_id)) AS resources";

/// `trace_id = toFixedString(unhex('<hex>'), 16)`'s right-hand side — the
/// one place the key literal is rendered.
fn trace_key(hex32: &str) -> String {
    debug_assert!(
        hex32.len() == 32 && hex32.bytes().all(|b| b.is_ascii_hexdigit()),
        "hex32 must be caller-validated 32-char hex, got {hex32:?}"
    );
    format!("toFixedString(unhex('{hex32}'), 16)")
}

fn span_scalar(spans_table: &str, predicate: &str) -> String {
    SPAN_SCALAR
        .replace("{spans}", spans_table)
        .replace("{predicate}", predicate)
}

fn resource_subquery(resources_table: &str, day: &str) -> String {
    RESOURCE_SUBQUERY
        .replace("{resources}", resources_table)
        .replace("{day}", day)
}

/// Statement 1's own text, with the span scalar and the resource
/// subquery substituted in. A `const` at column zero, like the two shared
/// blocks above, so the SQL reads as SQL and no source indentation can
/// leak into it.
const INDEXED: &str = r"WITH (SELECT (count(), min(start_ns), max(last_start_ns),
              groupUniqArrayArray({cap})(buckets))
      FROM {traces}
      WHERE trace_id = {key}) AS ext,
     ifNull(ext.1, 0)  AS idx_rows,
     ifNull(ext.2, 0)  AS ext_lo,
     ifNull(ext.3, 0)  AS ext_hi,
     ifNull(ext.4, []) AS bk,
     {span}
SELECT idx_rows               AS index_rows,
       toUInt32(length(bk))   AS bucket_count,
       sp.1                   AS spans,
{resources}";

/// Statement 1's span predicate. The key condition is the whole prefix of
/// the sort key — the bucket and the trace id — against a tuple `IN` over
/// a subquery that reads **no table**: `arrayJoin(bk)` runs over a
/// materialised array from the extent scalar, so the set is a constant at
/// analysis time and becomes a key condition. One granule range per bucket
/// the trace actually occupies.
const INDEXED_PREDICATE: &str = r"(intDiv(start_ns, {bucket_ns}), trace_id) IN
            (SELECT (k, {key})
             FROM (SELECT arrayJoin(bk) AS k))
        AND length(bk) < {cap}";

/// Statement 1w's own text.
const WIDE: &str = r"WITH (SELECT (min(start_ns), max(last_start_ns)) FROM {traces}
      WHERE trace_id = {key}) AS ext,
     ifNull(ext.1, 0) AS lo,
     ifNull(ext.2, 0) AS hi,
     {span}
SELECT sp.1 AS spans,
{resources}";

/// Statement 1w's span predicate: **no bucket condition at all.**
///
/// A stored set that may be incomplete would make a key `IN` EXCLUDE
/// rows, and a wrong answer is worse than a slow one. Every span of the
/// trace has its start inside the extent by the extent's own definition,
/// and its partition date inside the date range those bounds fall in — so
/// this predicate is complete.
///
/// **The partition clause is byte identical to the span table's own
/// `PARTITION BY` expression.** A bare conversion is not, so the prune
/// would be lost and the read would fall back to every partition in
/// retention or to a refusal.
const WIDE_PREDICATE: &str = r"trace_id = {key}
        AND start_ns >= lo AND start_ns <= hi
        AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')
            BETWEEN toDate(fromUnixTimestamp64Nano(lo), 'UTC')
                AND toDate(fromUnixTimestamp64Nano(hi), 'UTC')";

/// Statement 2's own text.
const FALLBACK: &str = r"WITH {span}
SELECT sp.1 AS spans,
{resources}";

/// Statement 2's span predicate.
///
/// **The bucket clause is the whole pushdown.** The engine does not derive
/// it from a condition on the row's own start: measured on this table, 980
/// of 980 granules without it and 26 of 980 with it.
const FALLBACK_PREDICATE: &str = r"trace_id = {key}
        AND {time}
        AND {bucket}";

/// Statement 1 — the indexed fetch. Issued on every fetch request,
/// always, first.
pub fn indexed_fetch_sql(
    traces_table: &str,
    spans_table: &str,
    resources_table: &str,
    hex32: &str,
) -> String {
    let key = trace_key(hex32);
    let predicate = INDEXED_PREDICATE
        .replace("{bucket_ns}", &BUCKET_NS.to_string())
        .replace("{key}", &key)
        .replace("{cap}", &BUCKET_SET_CAP.to_string());
    INDEXED
        .replace("{cap}", &BUCKET_SET_CAP.to_string())
        .replace("{traces}", traces_table)
        .replace("{key}", &key)
        .replace("{span}", &span_scalar(spans_table, &predicate))
        .replace(
            "{resources}",
            &resource_subquery(resources_table, &resources_day_bound("ext_lo", "ext_hi")),
        )
}

/// Statement 1w — the complete-predicate read, for a stored bucket set
/// that may have truncated. Takes only `hex32`: it reads its own bounds,
/// so the reader carries nothing between the two statements.
pub fn wide_fetch_sql(
    traces_table: &str,
    spans_table: &str,
    resources_table: &str,
    hex32: &str,
) -> String {
    let key = trace_key(hex32);
    let predicate = WIDE_PREDICATE.replace("{key}", &key);
    WIDE.replace("{traces}", traces_table)
        .replace("{key}", &key)
        .replace("{span}", &span_scalar(spans_table, &predicate))
        .replace(
            "{resources}",
            &resource_subquery(resources_table, &resources_day_bound("lo", "hi")),
        )
}

/// Statement 2 — the window fallback, for a trace the per-trace table has
/// not indexed. Every bound is rendered from one [`WindowSql`], so the row
/// bound, the bucket bound and the resource-day bound cannot disagree.
pub fn fallback_fetch_sql(
    spans_table: &str,
    resources_table: &str,
    hex32: &str,
    window: FetchWindow,
) -> String {
    // `[start, end)` — the one rule every request window is built with.
    let w = WindowSql::start_closed_end_open(window.start_ns, window.end_ns);
    let predicate = FALLBACK_PREDICATE
        .replace("{key}", &trace_key(hex32))
        .replace("{time}", &w.span_time_clause())
        .replace("{bucket}", &w.span_bucket_clause());
    FALLBACK
        .replace("{span}", &span_scalar(spans_table, &predicate))
        .replace(
            "{resources}",
            &resource_subquery(resources_table, &w.resources_day_clause()),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The byte-freeze case's own inputs: the 20-span trace of the
    /// measurement corpus, the three base table names, and the window the
    /// window cases share.
    const HEX: &str = "50fb0cd99260ac2a15d0a6f208126742";
    const WINDOW: FetchWindow = FetchWindow {
        start_ns: 1_699_999_999_000_000_000,
        end_ns: 1_700_000_002_000_000_000,
    };

    fn indexed() -> String {
        indexed_fetch_sql("traces", "spans", "resources", HEX)
    }

    fn wide() -> String {
        wide_fetch_sql("traces", "spans", "resources", HEX)
    }

    fn fallback() -> String {
        fallback_fetch_sql("spans", "resources", HEX, WINDOW)
    }

    /// `F-3`: **all three builders' output, byte for byte.**
    ///
    /// The `search_sql` convention. Three and not two: with the
    /// complete-predicate builder unfrozen, deleting its `trace_id = ...`
    /// predicate left every other case green — the literal case asserts
    /// only its zone literals and two absences, and the route case
    /// compares the issued text against the same builder's output — and
    /// the fetch would then have returned other traces' spans from the
    /// selected date range.
    ///
    /// **Two clauses are laid out on one line where the design document
    /// wraps them at its own text width**, and the difference is
    /// formatting and nothing else: the resource table's `day` bound in
    /// all three statements, and the fallback's bucket bound, each of
    /// which that document breaks mid-conjunct. Every token, every
    /// literal and every argument is the document's.
    #[test]
    fn all_three_builders_are_byte_frozen() {
        assert_eq!(indexed(), INDEXED_FROZEN);
        assert_eq!(wide(), WIDE_FROZEN);
        assert_eq!(fallback(), FALLBACK_FROZEN);
    }

    const INDEXED_FROZEN: &str = r"WITH (SELECT (count(), min(start_ns), max(last_start_ns),
              groupUniqArrayArray(4096)(buckets))
      FROM traces
      WHERE trace_id = toFixedString(unhex('50fb0cd99260ac2a15d0a6f208126742'), 16)) AS ext,
     ifNull(ext.1, 0)  AS idx_rows,
     ifNull(ext.2, 0)  AS ext_lo,
     ifNull(ext.3, 0)  AS ext_hi,
     ifNull(ext.4, []) AS bk,
     (SELECT (groupArray((span_id, parent_span_id, start_ns, end_ns, service,
                          resource_id, name, kind, status_code, status_message,
                          trace_state, flags, scope_name, scope_version, scope_attrs,
                          attrs, attrs_other, dropped_attrs, events, dropped_events,
                          links, dropped_links,
                          scope_schema_url, scope_dropped_attrs, scope_attrs_other)),
              groupUniqArray((service, resource_id)))
      FROM spans
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT (k, toFixedString(unhex('50fb0cd99260ac2a15d0a6f208126742'), 16))
             FROM (SELECT arrayJoin(bk) AS k))
        AND length(bk) < 4096) AS sp
SELECT idx_rows               AS index_rows,
       toUInt32(length(bk))   AS bucket_count,
       sp.1                   AS spans,
       (SELECT groupArray((resource_id, attrs, attrs_other, dropped_attrs,
                           schema_url, entity_refs))
        FROM (SELECT resource_id,
                     any(attrs) AS attrs, any(attrs_other) AS attrs_other,
                     any(dropped_attrs) AS dropped_attrs, any(schema_url) AS schema_url,
                     any(entity_refs) AS entity_refs
              FROM resources
              WHERE day >= toDate(fromUnixTimestamp64Nano(ext_lo), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(ext_hi), 'UTC')
                AND (service, resource_id) IN (SELECT arrayJoin(sp.2))
              GROUP BY resource_id)) AS resources";

    const WIDE_FROZEN: &str = r"WITH (SELECT (min(start_ns), max(last_start_ns)) FROM traces
      WHERE trace_id = toFixedString(unhex('50fb0cd99260ac2a15d0a6f208126742'), 16)) AS ext,
     ifNull(ext.1, 0) AS lo,
     ifNull(ext.2, 0) AS hi,
     (SELECT (groupArray((span_id, parent_span_id, start_ns, end_ns, service,
                          resource_id, name, kind, status_code, status_message,
                          trace_state, flags, scope_name, scope_version, scope_attrs,
                          attrs, attrs_other, dropped_attrs, events, dropped_events,
                          links, dropped_links,
                          scope_schema_url, scope_dropped_attrs, scope_attrs_other)),
              groupUniqArray((service, resource_id)))
      FROM spans
      WHERE trace_id = toFixedString(unhex('50fb0cd99260ac2a15d0a6f208126742'), 16)
        AND start_ns >= lo AND start_ns <= hi
        AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')
            BETWEEN toDate(fromUnixTimestamp64Nano(lo), 'UTC')
                AND toDate(fromUnixTimestamp64Nano(hi), 'UTC')) AS sp
SELECT sp.1 AS spans,
       (SELECT groupArray((resource_id, attrs, attrs_other, dropped_attrs,
                           schema_url, entity_refs))
        FROM (SELECT resource_id,
                     any(attrs) AS attrs, any(attrs_other) AS attrs_other,
                     any(dropped_attrs) AS dropped_attrs, any(schema_url) AS schema_url,
                     any(entity_refs) AS entity_refs
              FROM resources
              WHERE day >= toDate(fromUnixTimestamp64Nano(lo), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(hi), 'UTC')
                AND (service, resource_id) IN (SELECT arrayJoin(sp.2))
              GROUP BY resource_id)) AS resources";

    const FALLBACK_FROZEN: &str = r"WITH (SELECT (groupArray((span_id, parent_span_id, start_ns, end_ns, service,
                          resource_id, name, kind, status_code, status_message,
                          trace_state, flags, scope_name, scope_version, scope_attrs,
                          attrs, attrs_other, dropped_attrs, events, dropped_events,
                          links, dropped_links,
                          scope_schema_url, scope_dropped_attrs, scope_attrs_other)),
              groupUniqArray((service, resource_id)))
      FROM spans
      WHERE trace_id = toFixedString(unhex('50fb0cd99260ac2a15d0a6f208126742'), 16)
        AND start_ns >= 1699999999000000000 AND start_ns < 1700000002000000000
        AND intDiv(start_ns, 300000000000) BETWEEN intDiv(1699999999000000000, 300000000000) AND intDiv(1700000001999999999, 300000000000)) AS sp
SELECT sp.1 AS spans,
       (SELECT groupArray((resource_id, attrs, attrs_other, dropped_attrs,
                           schema_url, entity_refs))
        FROM (SELECT resource_id,
                     any(attrs) AS attrs, any(attrs_other) AS attrs_other,
                     any(dropped_attrs) AS dropped_attrs, any(schema_url) AS schema_url,
                     any(entity_refs) AS entity_refs
              FROM resources
              WHERE day >= toDate(fromUnixTimestamp64Nano(1699999999000000000), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1700000001999999999), 'UTC')
                AND (service, resource_id) IN (SELECT arrayJoin(sp.2))
              GROUP BY resource_id)) AS resources";

    /// `F-4`: **the fallback's bucket bound comes from the window's LAST
    /// INCLUDED nanosecond, not from its exclusive end.**
    ///
    /// Three windows. The first and the third differ by one nanosecond and
    /// by one bucket — the pair a `hi` taken from `end_ns` gets wrong,
    /// because it would render the third exactly as it renders the second.
    ///
    /// **The statement divides server-side and the case asserts the
    /// arguments**, because that is what the statement renders: the shared
    /// window module hands out the bound nanoseconds and the engine takes
    /// `intDiv` of them, so the reader never reproduces `intDiv`'s
    /// rounding. The bucket numbers the design document names for these
    /// three windows are asserted beside the arguments, computed here by
    /// integer division — a second source, not the builder's.
    #[test]
    fn the_fallbacks_bucket_bound_is_taken_from_the_last_included_nanosecond() {
        let cases: [(i64, i64, i64, i64, i64, i64); 3] = [
            // start_ns, end_ns, expected lo arg, expected hi arg, lo bucket, hi bucket
            (
                1_699_999_999_000_000_000,
                1_700_000_002_000_000_000,
                1_699_999_999_000_000_000,
                1_700_000_001_999_999_999,
                5_666_666,
                5_666_666,
            ),
            (
                1_699_999_999_000_000_000,
                1_700_000_100_000_000_001,
                1_699_999_999_000_000_000,
                1_700_000_100_000_000_000,
                5_666_666,
                5_666_667,
            ),
            (
                1_699_999_999_000_000_000,
                1_700_000_100_000_000_000,
                1_699_999_999_000_000_000,
                1_700_000_099_999_999_999,
                5_666_666,
                5_666_666,
            ),
        ];
        assert_eq!(
            cases.len(),
            3,
            "three windows, two of them one nanosecond apart"
        );
        let mut rendered = Vec::new();
        for (start_ns, end_ns, lo_arg, hi_arg, lo_bucket, hi_bucket) in cases {
            let sql =
                fallback_fetch_sql("spans", "resources", HEX, FetchWindow { start_ns, end_ns });
            let want = format!(
                "intDiv(start_ns, 300000000000) BETWEEN intDiv({lo_arg}, 300000000000) \
                 AND intDiv({hi_arg}, 300000000000)"
            );
            assert!(
                sql.contains(&want),
                "[{start_ns}, {end_ns}) must render `{want}`:\n{sql}"
            );
            // The bucket pair those two arguments divide to, computed from
            // the arguments rather than from the builder.
            assert_eq!(
                lo_arg / BUCKET_NS,
                lo_bucket,
                "[{start_ns}, {end_ns}) lo bucket"
            );
            assert_eq!(
                hi_arg / BUCKET_NS,
                hi_bucket,
                "[{start_ns}, {end_ns}) hi bucket"
            );
            rendered.push(want);
        }
        assert_ne!(
            rendered[1], rendered[2],
            "the second and third windows end one nanosecond apart and must render \
             different bounds — equal bounds is exactly what a `hi` taken from `end_ns` gives"
        );
    }

    /// `F-5a`: the literals and the absences each statement's correctness
    /// rests on, asserted rather than described.
    ///
    /// The complete-predicate statement renders `, 'UTC')` **five** times —
    /// once on `start_ns`, once on each of the two span-range bounds and
    /// once on each of the two resource-day bounds — with the arguments
    /// being its own `lo`/`hi` bindings rather than literals, since it
    /// reads its own bounds. It contains no `arrayJoin(range(` and no
    /// `intDiv(`, and it does contain `min(start_ns)`, `max(last_start_ns)`
    /// and the key predicate the byte-freeze case above showed nothing
    /// else asserted.
    ///
    /// The indexed statement's half is the three corrections to it,
    /// asserted so none can be dropped without a hermetic failure.
    #[test]
    fn the_statements_carry_their_zone_literals_and_their_absences() {
        let wide = wide();
        assert_eq!(
            wide.matches(", 'UTC')").count(),
            5,
            "five explicit zones in the complete-predicate statement:\n{wide}"
        );
        for needle in [
            "toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')",
            "toDate(fromUnixTimestamp64Nano(lo), 'UTC')",
            "toDate(fromUnixTimestamp64Nano(hi), 'UTC')",
            "min(start_ns)",
            "max(last_start_ns)",
            "trace_id = toFixedString(unhex('50fb0cd99260ac2a15d0a6f208126742'), 16)",
        ] {
            assert!(
                wide.contains(needle),
                "wide must contain `{needle}`:\n{wide}"
            );
        }
        for absent in ["arrayJoin(range(", "intDiv("] {
            assert!(
                !wide.contains(absent),
                "wide must not contain `{absent}`:\n{wide}"
            );
        }

        let indexed = indexed();
        for needle in [
            "toUInt32(length(",
            "groupUniqArrayArray(4096)(",
            "length(bk) < 4096",
        ] {
            assert!(
                indexed.contains(needle),
                "indexed must contain `{needle}`:\n{indexed}"
            );
        }
        for absent in ["range(", "least("] {
            assert!(
                !indexed.contains(absent),
                "indexed must not contain `{absent}`:\n{indexed}"
            );
        }
    }

    /// `F-11`: **per call site, with each builder's own argument form.**
    ///
    /// Comparing totals can balance a missing zone against an unrelated
    /// one, and a digit-only pattern matches none of the indexed
    /// statement's calls because it renders `ext_lo`/`ext_hi` and not
    /// digits. So each builder's `fromUnixTimestamp64Nano(` count is
    /// asserted beside both of its own exact literals.
    #[test]
    fn every_date_conversion_carries_its_zone_at_its_own_call_site() {
        let indexed = indexed();
        assert_eq!(
            indexed.matches("fromUnixTimestamp64Nano(").count(),
            2,
            "the indexed statement converts twice, for the resource-day bound:\n{indexed}"
        );
        for needle in [
            "toDate(fromUnixTimestamp64Nano(ext_lo), 'UTC')",
            "toDate(fromUnixTimestamp64Nano(ext_hi), 'UTC')",
        ] {
            assert!(
                indexed.contains(needle),
                "indexed must contain `{needle}`:\n{indexed}"
            );
        }

        let fallback = fallback();
        assert_eq!(
            fallback.matches("fromUnixTimestamp64Nano(").count(),
            2,
            "the fallback converts twice, both from the window:\n{fallback}"
        );
        for needle in [
            "toDate(fromUnixTimestamp64Nano(1699999999000000000), 'UTC')",
            "toDate(fromUnixTimestamp64Nano(1700000001999999999), 'UTC')",
        ] {
            assert!(
                fallback.contains(needle),
                "fallback must contain `{needle}`:\n{fallback}"
            );
        }
    }

    /// The projection aliases of one statement, in order — the text
    /// between the last top-level `SELECT` and the end, read as
    /// `<expr> AS <alias>` pairs. Used by the projection case below.
    fn tuple_elements(sql: &str, open_marker: &str) -> Vec<String> {
        let start = sql
            .find(open_marker)
            .unwrap_or_else(|| panic!("`{open_marker}` must appear in:\n{sql}"))
            + open_marker.len();
        let bytes = sql.as_bytes();
        let mut depth = 1i32;
        let mut end = start;
        while end < bytes.len() {
            match bytes[end] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            end += 1;
        }
        sql[start..end]
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// `F-14`: **each projection against its own stored-column subset.**
    ///
    /// The expected list is the catalogue's column list for that table
    /// minus the columns this projection omits, with `end_ns` moved to the
    /// index the statement projects it at. Comparing a subset against the
    /// full list would leave correct code red; naming the omissions is
    /// what makes a column added to either table force a decision — it
    /// appears in the derived list and this case fails until the new
    /// column is either projected or named here.
    #[test]
    fn each_projection_is_its_tables_column_list_minus_its_named_omissions() {
        /// The caller supplies the trace id; `duration_ns` is replaced by
        /// the sender's own end.
        const SPAN_PROJECTION_OMITS: [&str; 2] = ["trace_id", "duration_ns"];
        /// `day` is the partition and `service` travels on the span row.
        const RESOURCE_PROJECTION_OMITS: [&str; 2] = ["day", "service"];
        /// `end_ns` is projected at index 3, where `duration_ns` stood,
        /// not appended last where the DDL declares it.
        const SPAN_PROJECTION_MOVES: [(&str, usize); 1] = [("end_ns", 3)];

        fn expected(table: &str, omits: &[&str], moves: &[(&str, usize)]) -> Vec<String> {
            let mut columns: Vec<String> = pulsus_schema::table_column_names(table)
                .unwrap_or_else(|| panic!("{table} must be catalogued"))
                .into_iter()
                .filter(|c| !omits.contains(c))
                .map(str::to_string)
                .collect();
            for (name, index) in moves {
                let at = columns
                    .iter()
                    .position(|c| c == name)
                    .unwrap_or_else(|| panic!("{name} must be in {table}'s column list"));
                let column = columns.remove(at);
                columns.insert(*index, column);
            }
            columns
        }

        let span_expected = expected("spans", &SPAN_PROJECTION_OMITS, &SPAN_PROJECTION_MOVES);
        let resource_expected = expected("resources", &RESOURCE_PROJECTION_OMITS, &[]);
        assert_eq!(span_expected.len(), 25, "the span projection's width");
        assert_eq!(
            resource_expected.len(),
            6,
            "the resource projection's width"
        );

        for (label, sql) in [
            ("indexed", indexed()),
            ("wide", wide()),
            ("fallback", fallback()),
        ] {
            assert_eq!(
                tuple_elements(&sql, "groupArray((span_id,"),
                span_expected.iter().skip(1).cloned().collect::<Vec<_>>(),
                "{label}: the span projection after its first element"
            );
            assert_eq!(
                tuple_elements(&sql, "groupArray((resource_id,"),
                resource_expected
                    .iter()
                    .skip(1)
                    .cloned()
                    .collect::<Vec<_>>(),
                "{label}: the resource projection after its first element"
            );
        }
    }

    /// `F-9`: the module declaration a later task depends on resolves.
    #[test]
    fn the_span_fetch_module_path_resolves() {
        let _ = crate::traces::spans::fetch::BUCKET_SET_CAP;
        assert_eq!(BUCKET_SET_CAP, 4096);
        assert_eq!(BUCKET_NS, 300_000_000_000);
    }
}
