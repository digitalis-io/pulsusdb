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

use super::filter::{GenTable, LeafGenerator};

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
        "SELECT trace_id, span_id, parent_id, service, name, timestamp_ns, duration_ns, \
         status_code, kind\n\
         FROM {spans_table}\n\
         WHERE {}\n  AND {}\n\
         ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC\n\
         LIMIT {} BY trace_id",
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
    let (value_col, extra) = if numeric {
        ("any(val_num) AS v", "\n  AND isNotNull(val_num)")
    } else {
        ("any(val) AS v", "")
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
        "SELECT trace_id, span_id, parent_id, service, name, timestamp_ns, duration_ns\n\
         FROM {spans_table}\n\
         WHERE {}",
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
}
