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
//! hydration / membership / value reads over explicit candidate
//! `trace_id` lists.

use crate::logql::sql::TimeWindow;

use super::filter::{GenTable, LeafGenerator, ZERO_PARENT_SQL};

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
const TRACE_STR_COL_CP_FALLBACK: u64 = TRACE_STR_COL_CAP / 4;

/// The unaliased, unwrapped byte-bound truncation expression — the ONE
/// definition of the cap (issue #184 plan v4: `byte_capped`,
/// `byte_capped_agg`, and the [`trace_ctx_sql`] co-load's `argMin` value
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

/// The same byte-bound expression wrapped in `any(...)` for the
/// aggregate `attr_values_sql` string arm (dedup replicas via
/// `GROUP BY (trace_id, span_id)`), unaliased — the caller appends
/// `AS v`.
fn byte_capped_agg(col: &str) -> String {
    format!("any({})", byte_cap_expr(col))
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

const NS_PER_DAY: i64 = 86_400_000_000_000;

/// The `trace_attrs_idx` partition-pruning clause for a window:
/// `date >= toDate('…') AND date <= toDate('…')` (daily partitions,
/// docs/schemas.md §4.1).
fn date_clause(w: TimeWindow) -> String {
    let start_days = w.start_ns.div_euclid(NS_PER_DAY);
    let end_days = w.end_ns.div_euclid(NS_PER_DAY);
    format!(
        "date >= {} AND date <= {}",
        date_literal(start_days),
        date_literal(end_days)
    )
}

/// The shared half-open time bound (`ts > start AND ts <= end`,
/// docs/schemas.md §4.2).
fn time_clause(w: TimeWindow) -> String {
    format!(
        "timestamp_ns > {} AND timestamp_ns <= {}",
        w.start_ns, w.end_ns
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
/// ORDER BY bound_ts DESC, trace_id ASC
/// LIMIT {gen_cap + 1}
/// ```
///
/// `bound_ts` is the newest **leaf-matching** span's timestamp — an upper
/// bound on the trace's final public sort key (docs/api.md §4.2 ordering
/// contract), which licenses the engine's threshold termination. The
/// `+ 1` row is the per-generator truncation probe.
pub fn generator_sql(
    generator: &LeafGenerator,
    window: TimeWindow,
    spans_table: &str,
    attrs_table: &str,
    gen_cap: u64,
) -> String {
    let mut sql = String::from("SELECT trace_id, max(timestamp_ns) AS bound_ts\n");
    match generator.table {
        GenTable::Spans => {
            sql.push_str(&format!("FROM {spans_table}\n"));
            if let Some(prewhere) = &generator.prewhere {
                sql.push_str(&format!("PREWHERE {prewhere}\n"));
            }
            sql.push_str(&format!("WHERE {}", time_clause(window)));
        }
        GenTable::Attrs => {
            sql.push_str(&format!("FROM {attrs_table}\n"));
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
    sql.push_str(&format!(
        "\nGROUP BY trace_id\nORDER BY bound_ts DESC, trace_id ASC\nLIMIT {}",
        gen_cap + 1
    ));
    sql
}

/// Phase 2 — one batch's span hydration by primary-key prefix. The
/// `LIMIT {max_spans_per_trace + 1} BY trace_id` probe distinguishes
/// exactly-`max` from overflow (plan v5 delta 3); ordering by
/// `timestamp_ns` keeps the earliest spans under truncation. Reads only
/// physical summary columns — never span payloads (`pulsus-read` stays
/// OTLP-agnostic).
pub fn hydration_sql(
    spans_table: &str,
    trace_ids: &[[u8; 16]],
    window: TimeWindow,
    max_spans_per_trace: usize,
) -> String {
    format!(
        "SELECT trace_id, span_id, parent_id, {}, {}, timestamp_ns, duration_ns, \
         status_code, {}, kind\n\
         FROM {spans_table}\n\
         WHERE {}\n  AND {}\n\
         ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC\n\
         LIMIT {} BY trace_id",
        byte_capped("service"),
        byte_capped("name"),
        byte_capped("status_message"),
        trace_id_in(trace_ids),
        time_clause(window),
        max_spans_per_trace + 1
    )
}

/// Phase 2 — one attribute leaf's membership read over one batch:
/// `SELECT DISTINCT` dedups the `ReplacingMergeTree`/at-least-once
/// duplicates, the `(key[, val][, scope])` prefix + date/time pruning
/// keep it index-served, and the candidate restriction bounds it.
pub fn membership_sql(
    attrs_table: &str,
    predicate: &str,
    trace_ids: &[[u8; 16]],
    window: TimeWindow,
) -> String {
    format!(
        "SELECT DISTINCT trace_id, span_id\n\
         FROM {attrs_table}\n\
         WHERE {}\n  AND ({predicate})\n  AND {}\n  AND {}",
        date_clause(window),
        time_clause(window),
        trace_id_in(trace_ids)
    )
}

/// Phase 2 — one attribute field's per-span value read over one batch
/// (`avg(.attr)`-style aggregates read `val_num`; `select(.attr)` reads
/// `val`). `any(…)` + `GROUP BY (trace_id, span_id)` dedups replays
/// without `FINAL`.
pub fn attr_values_sql(
    attrs_table: &str,
    key_literal: &str,
    scope_literal: Option<&str>,
    numeric: bool,
    trace_ids: &[[u8; 16]],
    window: TimeWindow,
) -> String {
    let value_col = if numeric {
        "any(val_num) AS v".to_string()
    } else {
        format!("{} AS v", byte_capped_agg("val"))
    };
    let extra = if numeric {
        "\n  AND isNotNull(val_num)"
    } else {
        ""
    };
    let scope_clause = match scope_literal {
        Some(scope) => format!("\n  AND scope = {scope}"),
        None => String::new(),
    };
    format!(
        "SELECT trace_id, span_id, {value_col}\n\
         FROM {attrs_table}\n\
         WHERE {}\n  AND key = {key_literal}{scope_clause}{extra}\n  AND {}\n  AND {}\n\
         GROUP BY trace_id, span_id",
        date_clause(window),
        time_clause(window),
        trace_id_in(trace_ids)
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
/// other Phase-2 read performs (`SELECT DISTINCT` membership,
/// `any() GROUP BY` values, the hydration span-id dedup).
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

    #[test]
    fn trace_id_in_renders_unhex_literals() {
        let id = [0u8; 16];
        assert_eq!(
            trace_id_in(&[id]),
            "trace_id IN (unhex('00000000000000000000000000000000'))"
        );
    }

    #[test]
    fn generator_sql_for_a_time_range_fallback_has_no_predicate_clause() {
        let sql = generator_sql(
            &super::super::filter::LeafGenerator::time_range(),
            W,
            "trace_spans",
            "trace_attrs_idx",
            100,
        );
        assert!(sql.starts_with("SELECT trace_id, max(timestamp_ns) AS bound_ts\n"));
        assert!(sql.contains("FROM trace_spans\n"));
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
            pulsus_config::TRACEQL_MAX_CANDIDATES_CEILING,
        );
        assert!(sql.ends_with("LIMIT 1000001"), "got: {sql}");
    }

    #[test]
    fn hydration_sql_carries_the_overflow_probe_limit_by() {
        // (root_sql, by contrast, is deliberately uncapped — see below.)
        let sql = hydration_sql("trace_spans", &[[7u8; 16]], W, 10_000);
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
    /// issue #184 — the `attr_values_sql` string arm, and the trace-context
    /// co-load's root projections) and NOWHERE in the generator/membership/
    /// numeric-value SQL rendered HERE — the cap is a response/evaluation
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
        let hydration = hydration_sql("trace_spans", &[[7u8; 16]], W, 10_000);
        assert!(hydration.contains(&needle), "{hydration}");
        let status_needle = format!(
            "if(length(status_message) <= {TRACE_STR_COL_CAP}, status_message, \
             substringUTF8(status_message, 1, {TRACE_STR_COL_CP_FALLBACK})) AS status_message"
        );
        assert!(hydration.contains(&status_needle), "{hydration}");
        let root = root_sql("trace_spans", &[[7u8; 16]]);
        assert!(root.contains(&needle), "{root}");

        let val_needle = format!(
            "any(if(length(val) <= {TRACE_STR_COL_CAP}, val, \
             substringUTF8(val, 1, {TRACE_STR_COL_CP_FALLBACK}))) AS v"
        );
        let select_values =
            attr_values_sql("trace_attrs_idx", "'foo'", None, false, &[[7u8; 16]], W);
        assert!(select_values.contains(&val_needle), "{select_values}");
        // The numeric arm is untouched — no cap expression, plain
        // `any(val_num) AS v`.
        let agg_values = attr_values_sql("trace_attrs_idx", "'foo'", None, true, &[[7u8; 16]], W);
        assert!(!agg_values.contains("substringUTF8"), "{agg_values}");
        assert_eq!(agg_values.matches("any(val_num) AS v").count(), 1);

        // Generator/membership SQL never truncates — never touches
        // strings at all, and carries no `substringUTF8`.
        let generator = generator_sql(
            &super::super::filter::LeafGenerator::time_range(),
            W,
            "trace_spans",
            "trace_attrs_idx",
            100,
        );
        assert!(!generator.contains("substringUTF8"), "{generator}");
        let membership = membership_sql("trace_attrs_idx", "key = 'foo'", &[[7u8; 16]], W);
        assert!(!membership.contains("substringUTF8"), "{membership}");
        // The child-count co-load returns no strings — no cap expression.
        let child_counts = child_count_sql("trace_spans", &[[7u8; 16]]);
        assert!(!child_counts.contains("substringUTF8"), "{child_counts}");
    }

    /// Issue #184 plan v4: `byte_capped`/`byte_capped_agg` are built ON
    /// `byte_cap_expr` (its render is a substring of both), and the cap
    /// literals are unchanged — the coder verification gate half covering
    /// the displayed-root path.
    #[test]
    fn byte_capped_wrappers_derive_from_the_shared_cap_expression() {
        assert!(byte_capped("name").contains(&byte_cap_expr("name")));
        assert!(byte_capped_agg("val").contains(&byte_cap_expr("val")));
        assert_eq!(
            byte_capped("name"),
            format!("{} AS name", byte_cap_expr("name"))
        );
        assert_eq!(
            byte_capped_agg("val"),
            format!("any({})", byte_cap_expr("val"))
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
