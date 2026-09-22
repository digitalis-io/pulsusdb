//! Pure SQL string builders for the two-phase TraceQL search (issue #57
//! plan v7; docs/schemas.md §4.2) — the byte-frozen golden surface
//! (`tests/traces_search_sql.rs`), same convention as
//! [`crate::logql::sql`]: pre-escaped fragments → `String`, no
//! `ChClient`, no I/O, no randomness. Callers pre-escape every
//! user-controlled fragment via [`crate::traces::filter`] /
//! [`crate::logql::escape`] before it reaches these builders — that is
//! the injection boundary, not this module.
//!
//! Phase 1 renders **one bounded ranked query per generator** (never a
//! `UNION ALL` — plan v7 delta 1): an index-served top-K
//! `GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT gen_cap+1`
//! confined to the leaf's pruned prefix. Phase 2 renders the batched
//! hydration read over explicit candidate `trace_id` lists; since issue
//! #557 the hydration statement also carries one predicate column per
//! attribute condition, so a condition sends no read of its own, and
//! since issue #558 it carries the projected value, the numeric reading
//! and the stored kind of every `select()` field, aggregate argument and
//! `by()` key too — so those send no read of their own either. The one
//! phase-2 attribute statement left is [`event_set_sql`], which expands
//! a span's own event/link array.

use crate::logql::escape;
use crate::logql::sql::TimeWindow;

use super::filter::{GenTable, LeafGenerator, ZERO_PARENT_SQL};
use super::search_plan::HydrationShape;
use super::window_sql::WindowSql;

/// Hard **byte** ceiling on every string value the search response
/// returns (`name`/`service`/`select()`-projected attribute values) —
/// owner-approved response truncation (issue #57 re-audit, comment
/// 5028629688/5028693510). ClickHouse's `length()` counts bytes, so
/// [`byte_capped`]'s `length(col) <= TRACE_STR_COL_CAP` branch is
/// byte-identical passthrough for every string at or under the cap; the
/// fallback branch cuts at [`TRACE_STR_COL_CP_FALLBACK`] UTF-8 code
/// points instead of bytes, but a code point is at most 4 bytes, so the
/// fallback output itself never exceeds this same byte ceiling either —
/// documented in docs/api.md §4.2.
pub const TRACE_STR_COL_CAP: u64 = 8192;

/// The truncation fallback's code-point cut: `TRACE_STR_COL_CAP / 4` — a
/// UTF-8 sequence is at most 4 bytes per code point, so
/// `TRACE_STR_COL_CP_FALLBACK` code points can never exceed
/// `TRACE_STR_COL_CAP` bytes even at the worst-case 4-byte width.
///
/// `pub(crate)` since issue #492 part 5, for one further consumer:
/// [`super::search_plan::generator_exactness`] refuses a
/// `resource.service.name` literal of this many code points or more,
/// because that is exactly where a capped reading and a raw one can
/// first agree on a value they should not. Measured on ClickHouse 26.3:
/// with a stored `service` of `'\u{1D11E}' x 2048 + '0'` (8193 bytes,
/// 2049 code points) and a literal of `'\u{1D11E}' x 2048` (2048 code
/// points), `stored = literal` is `0` and `cap(stored) = literal` is
/// `1` — the generator's `PREWHERE` compares the raw column and the
/// evaluator the capped one, so below 2048 code points they cannot
/// disagree and at 2048 they can.
pub(crate) const TRACE_STR_COL_CP_FALLBACK: u64 = TRACE_STR_COL_CAP / 4;

/// The unaliased, unwrapped byte-bound truncation expression — the ONE
/// definition of the cap (issue #184 plan v4: `byte_capped`,
/// and the [`trace_ctx_sql`] co-load's `argMin` value
/// projections all build on this single helper, so the cap length and
/// fallback can never diverge between the displayed-root path and the
/// trace-context co-load; the in-module AC-Δ1c/Δ1d/Δ1e tests pin the
/// construction). `pub(crate)` for exactly one out-of-module consumer
/// (issue #184 code review): the Phase-1 `statusMessage` generator
/// predicate (`super::filter::physical_sql`) compares this same capped
/// expression, so candidate selection agrees byte-for-byte with the
/// capped `status_message` Phase 2 hydrates and evaluates.
pub(crate) fn byte_cap_expr(col: &str) -> String {
    format!(
        "if(length({col}) <= {TRACE_STR_COL_CAP}, {col}, \
         substringUTF8({col}, 1, {TRACE_STR_COL_CP_FALLBACK}))"
    )
}

/// Renders the byte-bound truncation expression for a plain (non-
/// aggregated) string column read, aliased back to its own name — the
/// [`byte_cap_expr`] render plus `AS col`. Used by
/// [`hydration_sql`]/[`root_sql`] on `service`/`name`/`status_message`.
fn byte_capped(col: &str) -> String {
    format!("{} AS {col}", byte_cap_expr(col))
}

/// Renders `days`-since-epoch as a `toDate('YYYY-MM-DD')` literal —
/// civil-date conversion (proleptic Gregorian), pure integer math.
/// `pub(crate)`: [`super::metrics_sql`] reuses it for the metrics
/// semi-joins' daily-partition pruning.
pub(crate) fn date_literal(days: i64) -> String {
    // Howard Hinnant's days-to-civil algorithm.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("toDate('{y:04}-{m:02}-{d:02}')")
}

/// The search window's bound convention, declared ONCE for this whole
/// module: `ts > start AND ts <= end` (docs/schemas.md §4.2), so
/// `end_ns` is IN the window.
///
/// Both [`date_clause`] and [`time_clause`] render from the value this
/// returns, which is what keeps the day-partition prune agreeing with
/// the row bound. Changing the constructor here to
/// [`WindowSql::start_closed_end_open`] — the convention
/// [`super::graph_sql`] and [`super::metrics_sql`] use, and the obvious
/// "fix" for three files that look inconsistent — changes the row bound
/// too and moves every `golden/traces_search/*.sql`. That is deliberate.
///
/// Were the DAY bound alone to take the right-open rule, it would narrow
/// to one day less than the row bound admits and DROP spans stored at
/// exactly `end_ns` — measured, 499 999 rows returned where 500 001 were
/// correct. That is the loud direction in consequence but the quiet one
/// in appearance: no SQL a golden pins would move. See
/// [`super::window_sql`] for both directions and their figures.
fn bounds(w: TimeWindow) -> WindowSql {
    WindowSql::start_open_end_closed(w.start_ns, w.end_ns)
}

/// The `trace_attrs_idx` daily-partition pruning clause for a window
/// (docs/schemas.md §4.1).
fn date_clause(w: TimeWindow) -> String {
    bounds(w).date_clause()
}

/// The row-level time bound (`ts > start AND ts <= end`,
/// docs/schemas.md §4.2).
fn time_clause(w: TimeWindow) -> String {
    bounds(w).time_clause()
}

/// The `trace_recent` bucket prune for a window (issue #560): the same
/// convention as [`date_clause`] and [`time_clause`], at bucket grain.
fn bucket_clause(w: TimeWindow) -> String {
    bounds(w).bucket_clause()
}

/// The `trace_recent` row bound (issue #560): a stored `(bucket, trace)`
/// row can hold a span in `(start, end]` only if its newest span is after
/// `start` and its oldest is at or before `end`.
///
/// There is no `ts_max <= end` bound, and there cannot be one: a trace
/// with a span inside the window and a later span in the same bucket has
/// `ts_max > end` and IS an answer. `ts_min <= end` is what keeps the
/// traces wholly after `end` in the last bucket out — without it the
/// empty search returns nothing at production trace rates
/// (docs/traceql-schema-migration.md §4, Q0).
fn recent_row_clause(w: TimeWindow) -> String {
    format!(
        "ts_max > {} AND ts_min <= {}",
        w.start_ns,
        bounds(w).last_included_ns()
    )
}

/// Renders a candidate `trace_id` list as `IN (unhex('…'), …)` — hex is
/// engine-produced from stored `[u8; 16]` ids, injection-safe by
/// construction.
fn trace_id_in(trace_ids: &[[u8; 16]]) -> String {
    let items: Vec<String> = trace_ids
        .iter()
        .map(|id| format!("unhex('{}')", hex32(id)))
        .collect();
    format!("trace_id IN ({})", items.join(", "))
}

fn hex32(id: &[u8; 16]) -> String {
    let mut out = String::with_capacity(32);
    for b in id {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Phase 1 — one generator's bounded, index-served ranked top-K (plan v7
/// delta 1, byte-pinned):
///
/// ```text
/// SELECT trace_id, max(timestamp_ns) AS bound_ts
/// FROM <its indexed source>
/// [PREWHERE service = '…']
/// WHERE <date/time pruning> [AND (<leaf predicate>)]
/// GROUP BY trace_id
/// [HAVING <pushed spanset aggregate>]
/// ORDER BY bound_ts DESC, trace_id ASC
/// LIMIT {gen_cap + 1}
/// ```
///
/// `bound_ts` is the newest **leaf-matching** span's timestamp — an upper
/// bound on the trace's final public sort key (docs/api.md §4.2 ordering
/// contract), which licenses the engine's threshold termination. The
/// `+ 1` row is the per-generator truncation probe.
///
/// **The two derived tables (issue #560).** `GenTable::Recent` reads
/// `trace_recent`, one row per (bucket, trace):
///
/// ```text
/// SELECT trace_id, toInt64(max(ts_max)) AS bound_ts
/// FROM trace_recent
/// WHERE <date clause>
///   AND <bucket clause>
///   AND ts_max > <start> AND ts_min <= <end>
/// ```
///
/// There `bound_ts` is the trace's newest span in its overlapping
/// buckets, which is `>=` the newest in-window span — still an upper
/// bound, so threshold termination stops later, never earlier. It is cast
/// because `max` over a `SimpleAggregateFunction(max, Int64)` column
/// keeps the wrapper. `GenTable::ErrorSpans` reads `trace_error_spans`,
/// one row per error span, with the date and row bounds of the span
/// table; neither derived statement carries an `AND (<predicate>)` line —
/// the table IS the predicate. Neither reads `FINAL`: an unmerged table
/// can hold a false candidate, never lose a true one.
///
/// `having` is issue #492 part 4's: the fragment
/// [`super::compile::aggregate_having_sql`] rendered, pre-built from
/// closed enums and one integer, so no user text reaches it — exactly as
/// no user text reaches `generator.predicate` unescaped. It is `None` for
/// every statement no aggregate compiled into, which is every statement
/// the corpus rendered before part 4, and for every statement on a
/// derived table (the renderer refuses those sources).
#[allow(clippy::too_many_arguments)]
pub fn generator_sql(
    generator: &LeafGenerator,
    window: TimeWindow,
    spans_table: &str,
    attrs_table: &str,
    recent_table: &str,
    errors_table: &str,
    gen_cap: u64,
    having: Option<&str>,
) -> String {
    const SPAN_BOUND: &str = "SELECT trace_id, max(timestamp_ns) AS bound_ts\n";
    let mut sql = String::new();
    match generator.table {
        GenTable::Spans => {
            sql.push_str(SPAN_BOUND);
            sql.push_str(&format!("FROM {spans_table}\n"));
            if let Some(prewhere) = &generator.prewhere {
                sql.push_str(&format!("PREWHERE {prewhere}\n"));
            }
            sql.push_str(&format!("WHERE {}", time_clause(window)));
        }
        GenTable::Attrs => {
            sql.push_str(SPAN_BOUND);
            sql.push_str(&format!("FROM {attrs_table}\n"));
            sql.push_str(&format!(
                "WHERE {}\n  AND {}",
                date_clause(window),
                time_clause(window)
            ));
        }
        GenTable::Recent => {
            sql.push_str("SELECT trace_id, toInt64(max(ts_max)) AS bound_ts\n");
            sql.push_str(&format!("FROM {recent_table}\n"));
            sql.push_str(&format!(
                "WHERE {}\n  AND {}\n  AND {}",
                date_clause(window),
                bucket_clause(window),
                recent_row_clause(window)
            ));
        }
        GenTable::ErrorSpans => {
            sql.push_str(SPAN_BOUND);
            sql.push_str(&format!("FROM {errors_table}\n"));
            sql.push_str(&format!(
                "WHERE {}\n  AND {}",
                date_clause(window),
                time_clause(window)
            ));
        }
    }
    if !generator.predicate.is_empty() {
        sql.push_str(&format!("\n  AND ({})", generator.predicate));
    }
    sql.push_str("\nGROUP BY trace_id");
    if let Some(having) = having {
        sql.push_str(&format!("\nHAVING {having}"));
    }
    sql.push_str(&format!(
        "\nORDER BY bound_ts DESC, trace_id ASC\nLIMIT {}",
        gen_cap + 1
    ));
    sql
}

/// One attribute SLOT's span-row columns (issue #557, widened by #558).
///
/// A slot is a CONDITION's probe, a PROJECTED FIELD's locator, or an
/// event/link set's WIDTH. The four rendered arrays share one index
/// space, which [`super::search_plan::SlotLayout`] owns.
///
/// The parts are already-rendered fragments: this module assembles them
/// into a statement and never decides what a value test says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeColumn {
    /// `WITH` items, each already `<expr> AS <alias>`, in render order.
    pub with_items: Vec<String>,
    /// The positive `UInt8` test. Never negated — the reader inverts.
    pub test: String,
    /// `(value, numeric, kind)` when something projects this slot's
    /// value (issue #479 for the fused matched value, #558 for the
    /// projected field and the set width), all three read at the element
    /// `test` landed on.
    pub value: Option<(String, String, String)>,
}

/// `arrayFirstIndex((k, s) -> k = <key> AND s = <scope>, attr_key, attr_scope) AS <alias>`
/// (issue #557) — the position of the element a span's attribute
/// RESOLVES to at one scope, or `0` when the span carries no such
/// element.
///
/// `0` is not an error on a computed subscript: ClickHouse returns the
/// element type's default (`''` for `String`, `NULL` for
/// `Nullable(Float64)`), measured on 26.3.29.7. It IS an error as a
/// constant subscript (`Code: 135 ZERO_ARRAY_OR_TUPLE_INDEX`), which is
/// why every subscript this module renders is an alias.
pub fn locate_item(alias: &str, key_literal: &str, scope_literal: &str) -> String {
    format!(
        "arrayFirstIndex((k, s) -> k = {key_literal} AND s = {scope_literal}, \
         attr_key, attr_scope) AS {alias}"
    )
}

/// The same locate with a pre-rendered value test folded into the lambda
/// — the multi-valued arm (issue #557). It yields the first MATCHING
/// element, so the same alias serves the fused value.
///
/// `test` must have been rendered against the lambda's own parameter
/// names, which are `v` (text) and `n` (numeric); `numeric` says which of
/// the two arrays the lambda takes.
///
/// Inside the lambda a `NULL` result is false and the function still
/// returns `UInt32`, so no `ifNull` wrapper belongs here — the wrapper is
/// the PROJECTED single-valued arm's (measured on 26.3.29.7).
pub fn locate_matching_item(
    alias: &str,
    key_literal: &str,
    scope_literal: &str,
    test: &str,
    numeric: bool,
) -> String {
    let (param, array) = if numeric {
        ("n", "attr_num")
    } else {
        ("v", "attr_val")
    };
    format!(
        "arrayFirstIndex((k, s, {param}) -> k = {key_literal} AND s = {scope_literal} AND {test}, \
         attr_key, attr_scope, {array}) AS {alias}"
    )
}

/// `(<alias> != 0) AND <test>` for a single-valued scope; `<alias> != 0`
/// for a multi-valued one, whose locate already carries the test
/// (issue #557).
///
/// **The `!= 0` conjunct is required for correctness, not to avoid an
/// error.** Element `0` of a `String` array reads `''`, so without it
/// `{ span.k = "" }` would match every span that carries no `k` at all.
pub fn probe_test(alias: &str, test: Option<&str>) -> String {
    match test {
        Some(test) => format!("({alias} != 0) AND {test}"),
        None => format!("{alias} != 0"),
    }
}

/// The nested `if` over `(present, expr)` pairs in chain order, ending in
/// `fallback` — the unscoped form (issue #557).
///
/// Used three times over the SAME arms — the test (fallback `0`), the
/// fused value (fallback `''`) and the stored kind (fallback `''`) — so a
/// value can never be read from a scope the test did not resolve to.
pub fn probe_chain(arms: &[(String, String)], fallback: &str) -> String {
    match arms.split_first() {
        None => fallback.to_string(),
        Some(((present, expr), rest)) => {
            format!("if({present}, {expr}, {})", probe_chain(rest, fallback))
        }
    }
}

/// The byte-capped value, the numeric reading and the stored kind, all
/// three subscripted at `alias` (issue #557; issue #558 adds the middle
/// one). The cap is [`byte_cap_expr`], the same one [`hydration_sql`]
/// applies to `service`/`name`, so a projected attribute value obeys the
/// same 8192-byte source truncation as every other projected string.
///
/// **The number is READ, never derived from the text.** `attr_num` is
/// the reading the writer stored, decided from the whole of
/// `(scope, key, value)` — a link's `spanID` of `0000000000000001`
/// stores `NULL` although the text parses as `1.0`
/// (`crates/pulsus-write/src/protocols/otlp_traces.rs:748`). Computing
/// the number from `attr_val` here would answer `1.0` there.
pub fn probe_value_exprs(alias: &str) -> (String, String, String) {
    (
        byte_cap_expr(&format!("attr_val[{alias}]")),
        format!("attr_num[{alias}]"),
        format!("attr_type[{alias}]"),
    )
}

/// One event/link set's per-span WIDTH (issue #558) — how many values
/// that span will contribute when the value statement expands its array.
///
/// `arrayCount` over `(attr_key, attr_scope)` reads those two arrays and
/// NO other column, in particular not `attr_val` and not `attr_num`, so
/// the pre-expansion bound costs no column read. The reader sums it over
/// the batch's hydrated rows and refuses BEFORE issuing
/// [`event_set_sql`], which is what `max_result_rows` cannot do: that
/// setting is checked per BLOCK and `arrayJoin` emits one span's whole
/// set as one block.
pub fn set_width_expr(key_literal: &str, scope_literal: &str) -> String {
    format!(
        "arrayCount((k, s) -> k = {key_literal} AND s = {scope_literal}, \
         attr_key, attr_scope)"
    )
}

/// Phase 2 — one batch's span hydration by primary-key prefix, with one
/// predicate column per attribute condition (issue #557). The
/// `LIMIT {max_spans_per_trace + 1} BY trace_id` probe distinguishes
/// exactly-`max` from overflow (plan v5 delta 3); ordering by
/// `timestamp_ns` keeps the earliest spans under truncation. Reads only
/// physical summary columns and the span's own attribute arrays — never
/// span payloads (`pulsus-read` stays OTLP-agnostic).
///
/// **`slots` empty renders byte-for-byte what this builder rendered
/// before issue #557**, which is checkable rather than asserted: 27 of
/// the 72 committed goldens plan no slot.
///
/// `shape` is NOT derived here. It is produced by
/// `SearchPlan::hydration_shape()` and passed in, because the decoder
/// calls the same method on the same plan and the two must be one value.
///
/// The slot expressions are PROJECTIONS, never predicates: the `WHERE`
/// clause does not move, so part and granule selection cannot change —
/// gated by `tests/traces_search_explain.rs` against
/// `SearchPlan::hydration_sql_without_probes_for`.
///
/// **The numeric array is always `CAST`** (issue #558). A slot needing no
/// number renders the bare `NULL`, and in the commonest shape —
/// `{ span.k = "x" }`, one probe fusing its matched value, no field, no
/// width — EVERY element is `NULL`, which ClickHouse 26.3.29.7 types as
/// `Array(Nullable(Nothing))`. That does not decode into
/// `Vec<Option<f64>>`.
pub fn hydration_sql(
    spans_table: &str,
    trace_ids: &[[u8; 16]],
    window: TimeWindow,
    max_spans_per_trace: usize,
    shape: HydrationShape,
    slots: &[ProbeColumn],
) -> String {
    let mut with_clause = String::new();
    let mut probe_cols = String::new();
    if shape != HydrationShape::Plain {
        let items: Vec<&str> = slots
            .iter()
            .flat_map(|p| p.with_items.iter().map(String::as_str))
            .collect();
        if !items.is_empty() {
            with_clause = format!("WITH {}\n", items.join(",\n     "));
        }
        let tests: Vec<&str> = slots.iter().map(|p| p.test.as_str()).collect();
        probe_cols = format!(",\n       [{}] AS attr_slot", tests.join(", "));
        if shape == HydrationShape::ProbesAndValues {
            // Every array carries exactly `slots.len()` elements and a
            // slot nothing reads a value from carries the literal `''`
            // (or `NULL` in the numeric array) — so no second index
            // mapping exists to get wrong.
            let values: Vec<&str> = slots
                .iter()
                .map(|p| p.value.as_ref().map_or("''", |(v, _, _)| v.as_str()))
                .collect();
            let kinds: Vec<&str> = slots
                .iter()
                .map(|p| p.value.as_ref().map_or("''", |(_, _, t)| t.as_str()))
                .collect();
            let nums: Vec<&str> = slots
                .iter()
                .map(|p| p.value.as_ref().map_or("NULL", |(_, n, _)| n.as_str()))
                .collect();
            probe_cols.push_str(&format!(
                ",\n       [{}] AS attr_slot_val,\n       [{}] AS attr_slot_type,\n       \
                 CAST([{}] AS Array(Nullable(Float64))) AS attr_slot_num",
                values.join(", "),
                kinds.join(", "),
                nums.join(", ")
            ));
        }
    }
    format!(
        "{with_clause}\
         SELECT trace_id, span_id, parent_id, {}, {}, timestamp_ns, duration_ns, \
         status_code, {}, kind, {}, {}{probe_cols}\n\
         FROM {spans_table}\n\
         WHERE {}\n  AND {}\n\
         ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC\n\
         LIMIT {} BY trace_id",
        byte_capped("service"),
        byte_capped("name"),
        byte_capped("status_message"),
        byte_capped("scope_name"),
        byte_capped("scope_version"),
        trace_id_in(trace_ids),
        time_clause(window),
        max_spans_per_trace + 1
    )
}

/// One attribute leaf's membership read over one batch:
/// `SELECT DISTINCT` dedups the `ReplacingMergeTree`/at-least-once
/// duplicates, the `(key[, val][, scope])` prefix + date/time pruning
/// keep it index-served, and the candidate restriction bounds it.
///
/// **No production caller since issue #557**, which answers the phase-2
/// attribute condition from a predicate column on [`hydration_sql`]'s
/// own statement. This builder, `SearchPlan::membership_sql_for` and
/// `super::rows::MembershipRow` are kept as the reproduction path for
/// the frozen issue #492 lowering evidence
/// (`docs/benchmarks/data/traces-lowering-92.json` carries a
/// `membership` stage row, and `xtask/src/bench/traces_lowering.rs`
/// rebuilds it); deleting any of the three makes that artefact
/// unrebuildable. Everything below describes the statement as it was
/// issued, which is what the frozen rows measured.
/// **`with_value` FUSES the matched value into the same read** (issue
/// #479): a probe whose matched value a projection needs adds
/// `<byte-capped val> AS v` to the SAME statement rather than issuing a
/// second one. The value predicate has already forced the `val` column to
/// be read inside the selected granules, so the projection is
/// granule-neutral — pinned as an identity by
/// `tests/traces_search_explain.rs` (`EXPLAIN indexes = 1`, no pinned
/// granule count, no wall-time). The cap is [`byte_cap_expr`], the same
/// one [`hydration_sql`] applies, so a projected value obeys the same
/// 8192-byte source truncation as every other projected string.
///
/// **Issue #510 adds `val_type AS t` to that arm and to that arm ONLY.**
/// A probe with `with_value == false` — the hot membership read every
/// string-equality condition issues — emits a byte-identical statement.
/// The same granule argument covers the extra column: it is one more
/// `LowCardinality(String)` read inside granules the value predicate has
/// already selected, the `WHERE` clause does not move, and the identity is
/// gated rather than asserted.
///
/// **`SELECT DISTINCT` over one more column can return more rows.** A span
/// carrying the same key at the same TEXT under two different stored types
/// (a string `"8080"` and an int `8080`) now yields two membership rows
/// where it yielded one, so that span could project two entries. That is
/// the case the catalog's `(scope, key, val, val_type)` sorting-key
/// extension exists for, and it is not reachable through our own ingest
/// for one span and one key — an OTLP attribute carries one value of one
/// kind. Stated rather than guarded: a `DISTINCT ON` would add server-side
/// state to a hot read to close a shape ingest cannot produce.
pub fn membership_sql(
    attrs_table: &str,
    predicate: &str,
    trace_ids: &[[u8; 16]],
    window: TimeWindow,
    with_value: bool,
) -> String {
    let projection = if with_value {
        format!(
            "trace_id, span_id, {} AS v, val_type AS t",
            byte_cap_expr("val")
        )
    } else {
        "trace_id, span_id".to_string()
    };
    format!(
        "SELECT DISTINCT {projection}\n\
         FROM {attrs_table}\n\
         WHERE {}\n  AND ({predicate})\n  AND {}\n  AND {}",
        date_clause(window),
        time_clause(window),
        trace_id_in(trace_ids)
    )
}

/// Phase 2 — one MULTI-VALUED event/link intrinsic's per-span values over
/// one batch (issue #351; moved off the attribute index onto the span
/// row by issue #558): the values `{ .a = event:name }` compares against,
/// **one row per value**.
///
/// **It expands a RETAINED-ROW subquery.** The inner statement carries
/// `hydration_sql`'s own `WHERE`, `ORDER BY` and
/// `LIMIT max_spans_per_trace BY trace_id` — without the `+ 1` overflow
/// probe row, which the reader discards — so the rows this expands are
/// exactly the rows the reader evaluated. Expanding the whole window
/// instead returns values for spans the search never evaluated, and the
/// measured consequence is a false refusal: on one trace of 10,002 stored
/// spans the unconstrained shape expanded 10,002 values where the reader
/// evaluated 10,000, and at a 10,000-row result cap it answered code 396
/// where the retained-row shape answered 200 with exactly 10,000 rows.
///
/// That identity is also what makes the pre-expansion bound exact: the
/// sum of [`set_width_expr`] over the batch's hydrated rows IS this
/// statement's physical row count.
///
/// **NO aggregate — deliberately, and this is the memory contract, not a
/// style choice.** The first cut of this read used
/// `groupUniqArray(...) GROUP BY trace_id, span_id`, and it broke the
/// Layer-1 residual bound this module's own contract states: "at most
/// `TRACE_SEARCH_MAX_BLOCK_ROWS` rows × (fixed-width columns + string
/// columns each capped at [`TRACE_STR_COL_CAP`] bytes at the source) —
/// never a-priori row-unbounded" (`traces::exec` module doc,
/// docs/schemas.md §7). An ARRAY column is an unbounded number of capped
/// strings in ONE row, so a single span with enough distinct event names
/// made both the server-side aggregate state and the client's decoded row
/// grow without any of that bound applying — and phase-2 reads carry no
/// `max_memory_usage` (only phase-1 generators do), so a server-side
/// blow-up would have surfaced as a 500 rather than the required 422.
///
/// Row-per-value restores the stated shape exactly: every row is
/// fixed-width columns plus ONE byte-capped string, and the executor
/// charges each value against the retention budget BEFORE retaining it.
///
/// **Duplicate rows need no server-side `DISTINCT`.** At-least-once
/// replays can repeat a value, and repetition is inert under both
/// matching rules: ANY-match is unaffected by a repeat, and ALL-match
/// compares `matchCount == elemCount`, which a duplicated element
/// increments on both sides. `trace_spans` is a plain MergeTree, so a
/// replay duplicates the whole span row and therefore the whole set.
///
/// A span whose filtered array is empty emits no row at all, which is the
/// empty set — the answer an absent index row gave before.
pub fn event_set_sql(
    spans_table: &str,
    set: super::filter::EventSetField,
    trace_ids: &[[u8; 16]],
    window: TimeWindow,
    max_spans_per_trace: usize,
) -> String {
    let key = escape::ch_string(set.key());
    let scope = escape::ch_string(set.scope());
    let (inner_col, value_expr) = if set.is_numeric() {
        (
            "attr_num",
            format!(
                "arrayJoin(arrayFilter((n, k, s) -> k = {key} AND s = {scope} \
                 AND isNotNull(n), attr_num, attr_key, attr_scope)) AS v"
            ),
        )
    } else {
        (
            "attr_val",
            format!(
                "arrayJoin(arrayMap(x -> {}, \
                 arrayFilter((v, k, s) -> k = {key} AND s = {scope}, \
                 attr_val, attr_key, attr_scope))) AS v",
                byte_cap_expr("x")
            ),
        )
    };
    format!(
        "SELECT trace_id, span_id, {value_expr}\n\
         FROM (\n  \
         SELECT trace_id, span_id, attr_key, attr_scope, {inner_col}\n  \
         FROM {spans_table}\n  \
         WHERE {}\n    AND {}\n  \
         ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC\n  \
         LIMIT {max_spans_per_trace} BY trace_id\n\
         )",
        trace_id_in(trace_ids),
        time_clause(window),
    )
}

/// Root/summary hydration for the final winners — a `trace_id` PK read
/// with **no time predicate and no row cap** (plan v4 delta 4 + code
/// review round 1: the actual root may predate the search window OR sit
/// past any per-trace row cap, so the read is genuinely trace-wide; the
/// engine picks the root — `parent_id` all-zero, else
/// timestamp-earliest — order-independently, and the read's cost is
/// bounded by the byte budgets, ≤ `limit` winners × fixed summary
/// columns, never payloads).
pub fn root_sql(spans_table: &str, trace_ids: &[[u8; 16]]) -> String {
    format!(
        "SELECT trace_id, span_id, parent_id, {}, {}, timestamp_ns, duration_ns\n\
         FROM {spans_table}\n\
         WHERE {}",
        byte_capped("service"),
        byte_capped("name"),
        trace_id_in(trace_ids)
    )
}

/// The `pick_roots` ordering tuple rendered in SQL (issue #184 plan v3):
/// `argMin` over `(toUInt8(parent_id != <zero>), timestamp_ns, span_id)`
/// minimizes the exact lexicographic key `exec::pick_roots` minimizes —
/// a true zero-parent root (`0`) beats every non-root (`1`), and within a
/// class the earliest `(timestamp_ns, span_id)` wins — so the co-load's
/// winning span is term-for-term the span `pick_roots` would pick from
/// the same (trace-wide) rows.
///
/// `pub(crate)` for one out-of-module consumer (issue #189): the
/// `compare()` cross-tab's window-free per-trace roots read
/// ([`super::metrics_sql::metrics_compare_sql`]) reuses this exact
/// ordering tuple so its `rootName`/`rootServiceName` selection is
/// byte-identical to this search path's.
pub(crate) fn root_ordering_tuple() -> String {
    format!("(toUInt8(parent_id != {ZERO_PARENT_SQL}), timestamp_ns, span_id)")
}

/// Phase 2 — the per-batch **trace-level context co-load** (issue #184
/// plan v2 Δ1): `traceDuration`/`rootName`/`rootServiceName` are
/// trace-level values, so this read is deliberately **trace-wide** — a
/// `trace_id IN` PK-prefix read with **no time predicate and no row
/// cap** (the `root_sql` precedent) — making the evaluated values
/// full-trace-exact regardless of the search window or the per-trace
/// hydration cap. Both `argMin`s share one ordering tuple (same winning
/// span for name and service), and both VALUE projections go through the
/// shared [`byte_cap_expr`] so they are byte-identical to what the
/// displayed-root path (`root_sql` + `pick_roots`) returns; the ordering
/// tuple itself stays on the RAW columns (the cap must never perturb
/// root selection).
pub fn trace_ctx_sql(spans_table: &str, trace_ids: &[[u8; 16]]) -> String {
    let ordering = root_ordering_tuple();
    format!(
        "SELECT trace_id, min(timestamp_ns) AS trace_start_ns, \
         max(timestamp_ns + duration_ns) AS trace_end_ns, \
         argMin({}, {ordering}) AS root_name, \
         argMin({}, {ordering}) AS root_service\n\
         FROM {spans_table}\n\
         WHERE {}\n\
         GROUP BY trace_id",
        byte_cap_expr("name"),
        byte_cap_expr("service"),
        trace_id_in(trace_ids)
    )
}

/// Phase 2 — the per-batch **direct-child-count co-load** (issue #184
/// plan v2 Δ1): one row per `(trace_id, parent span_id)` with its number
/// of distinct direct children. Trace-wide like [`trace_ctx_sql`] (no
/// time predicate, no row cap), so `span:childCount` is full-trace-exact.
/// `count(DISTINCT span_id)` — not a bare `count()` — dedups
/// at-least-once ingest replays, mirroring the read-time dedup every
/// other Phase-2 read performs (`any() GROUP BY` values, the hydration
/// span-id dedup).
pub fn child_count_sql(spans_table: &str, trace_ids: &[[u8; 16]]) -> String {
    format!(
        "SELECT trace_id, parent_id, count(DISTINCT span_id) AS child_count\n\
         FROM {spans_table}\n\
         WHERE {}\n  AND parent_id != {ZERO_PARENT_SQL}\n\
         GROUP BY trace_id, parent_id",
        trace_id_in(trace_ids)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const W: TimeWindow = TimeWindow {
        start_ns: 1_700_000_000_000_000_000,
        end_ns: 1_700_010_800_000_000_000,
    };

    /// Issue #351: the event/link value read, asserted as its EXACT
    /// rendered text — one whole string per intrinsic, variables
    /// substituted.
    ///
    /// **Why exact, and why nothing weaker** (review 4). This gate has
    /// been rebuilt three times and each rebuild was the same defect one
    /// layer down, because each asserted a PROPERTY of the statement and
    /// lost to a spelling that satisfied the property:
    ///
    /// 1. a denylist of aggregate names — `LIMIT 1 BY trace_id, span_id`
    ///    contains none of them and still needs per-group state;
    /// 2. the denylist plus `LIMIT` — a lowercase `group by` walks past a
    ///    case-sensitive substring ban;
    /// 3. a per-line prefix shape (`WHERE `/`  AND ` continuations) — an
    ///    appended `  AND trace_id IN (SELECT trace_id FROM
    ///    trace_attrs_idx group by trace_id)` satisfies the prefix and
    ///    reintroduces grouping state inside the predicate.
    ///
    /// An exact match has no property left to satisfy: nothing can be
    /// appended, nested, re-cased or re-spaced without changing the
    /// string. That is what terminates the class instead of moving it
    /// down another layer.
    ///
    /// What the exactness buys, all at once and without a separate
    /// assertion for each: no aggregate anywhere; the batch's `trace_id`
    /// restriction and the request window; the byte cap applied to each
    /// expanded element for the three string members and `isNotNull` on
    /// the numeric one; and one row per value, which is the memory
    /// contract (`traces::exec`'s Layer-1 residual bound — an array
    /// column would be row-unbounded).
    ///
    /// **Issue #558 puts a subquery in this statement on purpose, and
    /// the exactness is what makes that visible.** The read moved off
    /// `trace_attrs_idx` onto the span row's own arrays, and the inner
    /// statement is the RETAINED-ROW set: `hydration_sql`'s own
    /// `ORDER BY` and `LIMIT MAX_SPANS_PER_TRACE BY trace_id`, without
    /// the `+ 1` overflow probe row the reader discards. Expanding the
    /// whole window instead returns values for spans the search never
    /// evaluated. The clause to look at is therefore
    /// `LIMIT 10000 BY trace_id` inside the `FROM (…)`: removing it is
    /// what this string refuses.
    ///
    /// The goldens pin two of these four through a planned query; this
    /// pins all four at the builder, including both link intrinsics,
    /// which no golden case reaches.
    #[test]
    fn the_event_value_read_renders_exactly_this_sql() {
        use super::super::filter::EventSetField;
        let cases: [(EventSetField, &str); 4] = [
            (
                EventSetField::EventName,
                "SELECT trace_id, span_id, arrayJoin(arrayMap(x -> if(length(x) <= 8192, x, substringUTF8(x, 1, 2048)), arrayFilter((v, k, s) -> k = 'name' AND s = 'event:intrinsic', attr_val, attr_key, attr_scope))) AS v\n\
                 FROM (\n\
                 \x20\x20SELECT trace_id, span_id, attr_key, attr_scope, attr_val\n\
                 \x20\x20FROM trace_spans\n\
                 \x20\x20WHERE trace_id IN (unhex('07070707070707070707070707070707'))\n\
                 \x20\x20\x20\x20AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000\n\
                 \x20\x20ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC\n\
                 \x20\x20LIMIT 10000 BY trace_id\n\
                 )",
            ),
            (
                EventSetField::EventTimeSinceStart,
                "SELECT trace_id, span_id, arrayJoin(arrayFilter((n, k, s) -> k = 'timeSinceStart' AND s = 'event:intrinsic' AND isNotNull(n), attr_num, attr_key, attr_scope)) AS v\n\
                 FROM (\n\
                 \x20\x20SELECT trace_id, span_id, attr_key, attr_scope, attr_num\n\
                 \x20\x20FROM trace_spans\n\
                 \x20\x20WHERE trace_id IN (unhex('07070707070707070707070707070707'))\n\
                 \x20\x20\x20\x20AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000\n\
                 \x20\x20ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC\n\
                 \x20\x20LIMIT 10000 BY trace_id\n\
                 )",
            ),
            (
                EventSetField::LinkSpanId,
                "SELECT trace_id, span_id, arrayJoin(arrayMap(x -> if(length(x) <= 8192, x, substringUTF8(x, 1, 2048)), arrayFilter((v, k, s) -> k = 'spanID' AND s = 'link:intrinsic', attr_val, attr_key, attr_scope))) AS v\n\
                 FROM (\n\
                 \x20\x20SELECT trace_id, span_id, attr_key, attr_scope, attr_val\n\
                 \x20\x20FROM trace_spans\n\
                 \x20\x20WHERE trace_id IN (unhex('07070707070707070707070707070707'))\n\
                 \x20\x20\x20\x20AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000\n\
                 \x20\x20ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC\n\
                 \x20\x20LIMIT 10000 BY trace_id\n\
                 )",
            ),
            (
                EventSetField::LinkTraceId,
                "SELECT trace_id, span_id, arrayJoin(arrayMap(x -> if(length(x) <= 8192, x, substringUTF8(x, 1, 2048)), arrayFilter((v, k, s) -> k = 'traceID' AND s = 'link:intrinsic', attr_val, attr_key, attr_scope))) AS v\n\
                 FROM (\n\
                 \x20\x20SELECT trace_id, span_id, attr_key, attr_scope, attr_val\n\
                 \x20\x20FROM trace_spans\n\
                 \x20\x20WHERE trace_id IN (unhex('07070707070707070707070707070707'))\n\
                 \x20\x20\x20\x20AND timestamp_ns > 1700000000000000000 AND timestamp_ns <= 1700010800000000000\n\
                 \x20\x20ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC\n\
                 \x20\x20LIMIT 10000 BY trace_id\n\
                 )",
            ),
        ];
        for (set, expected) in cases {
            assert_eq!(
                event_set_sql("trace_spans", set, &[[7u8; 16]], W, 10_000),
                expected,
                "{set:?}: the value read is asserted EXACTLY — any difference, including \
                 an appended clause, a nested subquery, a re-casing or a whitespace \
                 change, is a deliberate act that belongs in the diff"
            );
        }
    }

    #[test]
    fn date_literal_renders_the_unix_epoch_and_a_modern_date() {
        assert_eq!(date_literal(0), "toDate('1970-01-01')");
        // 1_700_000_000s / 86_400 = 19_675 days → 2023-11-14.
        assert_eq!(date_literal(19_675), "toDate('2023-11-14')");
    }

    #[test]
    fn date_clause_spans_the_windows_utc_days() {
        // start = 1,700,000,000s (2023-11-14); end = 1,700,010,800s,
        // which crosses into the next UTC day.
        assert_eq!(
            date_clause(W),
            "date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')"
        );
    }

    /// The search window's end is INCLUDED (`ts <= end`), so a window
    /// ending exactly at midnight still contains one nanosecond of the
    /// next UTC day and must keep that day's partition — the opposite of
    /// the rule `graph_sql`/`metrics_sql` correctly apply to their
    /// right-OPEN windows (issue #525).
    ///
    /// `date_clause_spans_the_windows_utc_days` above cannot see this:
    /// its window ends mid-day, where both conventions agree. A window
    /// ending on a day boundary is the only input that discriminates.
    ///
    /// Giving THIS module the right-open rule narrows the day clause to
    /// one day less than the row bound admits, so a span stored at
    /// exactly `end_ns` sits in a partition the query never reads:
    /// measured, 499 999 rows returned where 500 001 were correct. A
    /// lost answer, not a slower query. The reverse mistake — an
    /// exclusive window given this module's inclusive rule — is the one
    /// that keeps every answer and merely reads an extra partition
    /// ([`super::window_sql`] has both).
    #[test]
    fn date_clause_keeps_the_end_day_because_the_search_end_is_included() {
        let w = TimeWindow {
            start_ns: 1_699_920_000_000_000_000, // 2023-11-14 00:00:00
            end_ns: 1_700_006_400_000_000_000,   // 2023-11-15 00:00:00 (INCLUDED)
        };
        assert_eq!(
            date_clause(w),
            "date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')"
        );
        // And the row bound this day bound has to agree with.
        assert_eq!(
            time_clause(w),
            "timestamp_ns > 1699920000000000000 AND timestamp_ns <= 1700006400000000000"
        );
    }

    #[test]
    fn trace_id_in_renders_unhex_literals() {
        let id = [0u8; 16];
        assert_eq!(
            trace_id_in(&[id]),
            "trace_id IN (unhex('00000000000000000000000000000000'))"
        );
    }

    /// Issue #560: the time-range fallback reads `trace_recent`, with both
    /// row bounds and no predicate line.
    #[test]
    fn generator_sql_for_a_time_range_fallback_has_no_predicate_clause() {
        let sql = generator_sql(
            &super::super::filter::LeafGenerator::time_range(),
            W,
            "trace_spans",
            "trace_attrs_idx",
            "trace_recent",
            "trace_error_spans",
            100,
            None,
        );
        assert!(sql.starts_with("SELECT trace_id, toInt64(max(ts_max)) AS bound_ts\n"));
        assert!(sql.contains("FROM trace_recent\n"));
        assert!(sql.contains("ts_min <= "));
        assert!(!sql.contains("AND ("));
        assert!(sql.ends_with("LIMIT 101"));
    }

    /// Issue #133 AC3: at the maximum config-accepted
    /// `reader.traceql_max_candidates` the rendered truncation probe is
    /// `LIMIT 1000001` — the `gen_cap + 1` arithmetic is overflow-free at
    /// every accepted cap (the load-time ceiling is what makes a
    /// `u64::MAX + 1` wrap to `LIMIT 0` — a silently empty search —
    /// unreachable; no saturating-add masking anywhere).
    #[test]
    fn generator_sql_at_the_max_accepted_candidates_cap_renders_limit_1000001() {
        let sql = generator_sql(
            &super::super::filter::LeafGenerator::time_range(),
            W,
            "trace_spans",
            "trace_attrs_idx",
            "trace_recent",
            "trace_error_spans",
            pulsus_config::TRACEQL_MAX_CANDIDATES_CEILING,
            None,
        );
        assert!(sql.ends_with("LIMIT 1000001"), "got: {sql}");
    }

    #[test]
    fn hydration_sql_carries_the_overflow_probe_limit_by() {
        // (root_sql, by contrast, is deliberately uncapped — see below.)
        let sql = hydration_sql(
            "trace_spans",
            &[[7u8; 16]],
            W,
            10_000,
            HydrationShape::Plain,
            &[],
        );
        assert!(sql.contains("LIMIT 10001 BY trace_id"));
        assert!(sql.contains("ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC"));
        assert!(!sql.contains("payload"), "hydration never reads payloads");
    }

    #[test]
    fn root_sql_is_trace_wide_with_no_time_predicate_and_no_row_cap() {
        let sql = root_sql("trace_spans", &[[7u8; 16]]);
        assert!(!sql.contains("timestamp_ns >"), "root read is trace-wide");
        assert!(
            !sql.contains("LIMIT"),
            "a per-trace row cap could drop the true root (code review round 1)"
        );
    }

    /// Issue #57 re-audit AC-A1: the fallback code-point cut is exactly
    /// one quarter of the byte ceiling — a worst-case 4-byte UTF-8 code
    /// point at that cut still lands exactly at the byte ceiling, never
    /// past it.
    #[test]
    fn cp_fallback_is_exactly_one_quarter_of_the_byte_cap() {
        assert_eq!(TRACE_STR_COL_CP_FALLBACK, TRACE_STR_COL_CAP / 4);
        assert_eq!(TRACE_STR_COL_CAP, 8192);
        assert_eq!(TRACE_STR_COL_CP_FALLBACK, 2048);
    }

    /// Issue #57 re-audit AC-A1 (+ issue #184): the byte-bound truncation
    /// expression appears in every string-returning Phase-2 builder
    /// (hydration/root plain columns — `status_message` included since
    /// issue #184 — the `event_set_sql` string arm, and the trace-context
    /// co-load's root projections) and NOWHERE in the generator/membership
    /// SQL rendered HERE — the cap is a response/evaluation
    /// concern, with exactly one predicate exception living in
    /// `filter::physical_sql` (issue #184 code review): the `statusMessage`
    /// Phase-1 predicate compares the capped column via the shared helper
    /// so candidate selection agrees with the capped Phase-2 evaluation.
    #[test]
    fn the_byte_cap_expression_appears_only_in_the_string_returning_builders() {
        let needle = format!(
            "if(length(service) <= {TRACE_STR_COL_CAP}, service, \
             substringUTF8(service, 1, {TRACE_STR_COL_CP_FALLBACK})) AS service"
        );
        let hydration = hydration_sql(
            "trace_spans",
            &[[7u8; 16]],
            W,
            10_000,
            HydrationShape::Plain,
            &[],
        );
        assert!(hydration.contains(&needle), "{hydration}");
        let status_needle = format!(
            "if(length(status_message) <= {TRACE_STR_COL_CAP}, status_message, \
             substringUTF8(status_message, 1, {TRACE_STR_COL_CP_FALLBACK})) AS status_message"
        );
        assert!(hydration.contains(&status_needle), "{hydration}");
        // Issue #192: the instrumentation-scope columns hydrate byte-capped
        // exactly like `status_message`.
        for col in ["scope_name", "scope_version"] {
            let scope_needle = format!(
                "if(length({col}) <= {TRACE_STR_COL_CAP}, {col}, \
                 substringUTF8({col}, 1, {TRACE_STR_COL_CP_FALLBACK})) AS {col}"
            );
            assert!(hydration.contains(&scope_needle), "{hydration}");
        }
        let root = root_sql("trace_spans", &[[7u8; 16]]);
        assert!(root.contains(&needle), "{root}");

        // Issue #558: the event/link set reads the span row's own array,
        // and caps each expanded element with the SAME helper.
        let val_needle = format!(
            "if(length(x) <= {TRACE_STR_COL_CAP}, x, \
             substringUTF8(x, 1, {TRACE_STR_COL_CP_FALLBACK}))"
        );
        let names = event_set_sql(
            "trace_spans",
            super::super::filter::EventSetField::EventName,
            &[[7u8; 16]],
            W,
            10_000,
        );
        assert!(names.contains(&val_needle), "{names}");
        // The numeric arm is untouched — no cap expression.
        let offsets = event_set_sql(
            "trace_spans",
            super::super::filter::EventSetField::EventTimeSinceStart,
            &[[7u8; 16]],
            W,
            10_000,
        );
        assert!(!offsets.contains("substringUTF8"), "{offsets}");

        // Generator/membership SQL never truncates — never touches
        // strings at all, and carries no `substringUTF8`.
        let generator = generator_sql(
            &super::super::filter::LeafGenerator::time_range(),
            W,
            "trace_spans",
            "trace_attrs_idx",
            "trace_recent",
            "trace_error_spans",
            100,
            None,
        );
        assert!(!generator.contains("substringUTF8"), "{generator}");
        let membership = membership_sql("trace_attrs_idx", "key = 'foo'", &[[7u8; 16]], W, false);
        assert!(!membership.contains("substringUTF8"), "{membership}");
        // The child-count co-load returns no strings — no cap expression.
        let child_counts = child_count_sql("trace_spans", &[[7u8; 16]]);
        assert!(!child_counts.contains("substringUTF8"), "{child_counts}");
    }

    /// Issue #184 plan v4: `byte_capped` is built ON `byte_cap_expr` (its
    /// render is a substring), and the cap literals are unchanged — the
    /// coder verification gate half covering the displayed-root path.
    #[test]
    fn byte_capped_wrappers_derive_from_the_shared_cap_expression() {
        assert!(byte_capped("name").contains(&byte_cap_expr("name")));
        assert_eq!(
            byte_capped("name"),
            format!("{} AS name", byte_cap_expr("name"))
        );
    }

    /// Issue #184 AC-Δ1b: the two trace-wide co-loads carry NO time
    /// predicate and no row cap — a `trace_id IN (…)` PK restriction only
    /// — so their values are window- and cap-independent (full-trace
    /// exact, the `root_sql` contract generalized).
    #[test]
    fn trace_wide_coloads_have_no_time_predicate_and_no_row_cap() {
        for sql in [
            trace_ctx_sql("trace_spans", &[[7u8; 16]]),
            child_count_sql("trace_spans", &[[7u8; 16]]),
        ] {
            assert!(!sql.contains("timestamp_ns >"), "trace-wide read: {sql}");
            assert!(!sql.contains("timestamp_ns <="), "trace-wide read: {sql}");
            assert!(!sql.contains("LIMIT"), "no row cap: {sql}");
            assert!(sql.contains("trace_id IN (unhex("), "PK restriction: {sql}");
            assert!(sql.contains("GROUP BY trace_id"), "per-trace groups: {sql}");
        }
    }

    /// Issue #184 AC-Δ1c: the trace-context co-load's `root_name`/
    /// `root_service` VALUE projections are exactly the shared-helper
    /// render inside `argMin` over the `pick_roots` ordering tuple, and
    /// every cap token in the rendered SQL is accounted for by exactly
    /// the two shared-helper renders — a third inline copy of the cap
    /// logic pushes any count to 3 and fails.
    #[test]
    fn trace_ctx_coload_caps_root_strings_via_the_shared_helper_only() {
        let sql = trace_ctx_sql("trace_spans", &[[0u8; 16]]);
        let name_cap = byte_cap_expr("name");
        let svc_cap = byte_cap_expr("service");

        assert!(
            sql.contains(&format!("argMin({name_cap}, (toUInt8(parent_id != ")),
            "root_name argMin must wrap byte_cap_expr(name): {sql}"
        );
        assert!(
            sql.contains(&format!("argMin({svc_cap}, (toUInt8(parent_id != ")),
            "root_service argMin must wrap byte_cap_expr(service): {sql}"
        );

        assert_eq!(
            sql.matches("substringUTF8").count(),
            2,
            "exactly two capped strings: {sql}"
        );
        assert_eq!(
            sql.matches("8192").count(),
            2,
            "cap literal only from the shared helper: {sql}"
        );
        assert_eq!(
            sql.matches("2048").count(),
            2,
            "fallback literal only from the shared helper: {sql}"
        );
    }

    /// Issue #184 AC-Δ1c (ordering): both `argMin`s share ONE ordering
    /// tuple — the `pick_roots` key on the RAW columns (the cap never
    /// perturbs root selection) — so `root_name` and `root_service`
    /// always come from the same winning span.
    #[test]
    fn trace_ctx_coload_orders_both_argmins_by_the_raw_pick_roots_tuple() {
        let sql = trace_ctx_sql("trace_spans", &[[0u8; 16]]);
        let tuple = root_ordering_tuple();
        assert_eq!(
            sql.matches(&tuple).count(),
            2,
            "both argMins share the pick_roots ordering tuple: {sql}"
        );
        assert!(
            tuple.contains("toFixedString(unhex('0000000000000000'), 8)"),
            "the zero-parent sentinel spelling matches the codebase convention: {tuple}"
        );
        assert!(
            !tuple.contains("substringUTF8") && !tuple.contains("8192"),
            "the ordering tuple stays on raw columns: {tuple}"
        );
    }

    /// Issue #184 AC-Δ1d: the cap expression has a SINGLE source of
    /// truth — `substringUTF8` (the truncation call, the cap's
    /// unmistakable signature) appears in this module's production,
    /// non-comment code exactly once: inside `byte_cap_expr`'s `format!`
    /// body. Any inline duplicate — placeholder template or hand-typed
    /// rendered literal — contains `substringUTF8` and pushes this to 2.
    #[test]
    fn the_cap_expression_has_a_single_source_of_truth() {
        let src = include_str!("search_sql.rs");
        // (a) everything before the test module — excludes this test's
        //     own needles.
        let prod = src.split("#[cfg(test)]").next().unwrap();
        // (b) drop comment lines so doc prose never counts.
        let code_only: String = prod
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(
            code_only.matches("substringUTF8").count(),
            1,
            "cap logic must have one definition (byte_cap_expr) — found an inline duplicate"
        );
    }

    /// Issue #184 AC-Δ1e: the trace-context co-load INVOKES the shared
    /// helper by name for both root projections —
    /// `byte_cap_expr("name")`/`byte_cap_expr("service")` as literal call
    /// tokens exist in production only at the co-load builder (every
    /// other site calls `byte_capped`, not `byte_cap_expr`).
    #[test]
    fn the_trace_ctx_coload_invokes_the_shared_cap_helper_by_name() {
        let src = include_str!("search_sql.rs");
        let prod = src.split("#[cfg(test)]").next().unwrap();
        assert!(
            prod.contains(r#"byte_cap_expr("name")"#),
            "the trace-level co-load must call byte_cap_expr(\"name\") for root_name"
        );
        assert!(
            prod.contains(r#"byte_cap_expr("service")"#),
            "the trace-level co-load must call byte_cap_expr(\"service\") for root_service"
        );
    }
}
