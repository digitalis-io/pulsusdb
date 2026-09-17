//! Issue #549: the grouped instant read's statement builders — pure,
//! `data -> String`, no `ChClient` and no I/O, the same contract
//! [`super::sample_sql`] holds for the per-sample fetch.
//!
//! # What the statement returns
//!
//! One row per **run**: a maximal stretch of consecutive grid indices
//! over which one group's answer does not change.
//!
//! ```text
//! grid index   0    1    2    3    4    5    6    7
//! group 0      7    7    7    9    9    9    9    7
//!              \_________/    \____________/    \_/
//!               run 1          run 2            run 3
//!
//! rows returned:   (gid 0, 0..2, 7)  (gid 0, 3..6, 9)  (gid 0, 7..7, 7)
//! ```
//!
//! Three rows where the unpushed route fetches every sample of every
//! member series and reduces them in this process. The encoding is a run
//! rather than a value per grid point because the answer IS a step
//! function: a sample stays the series' most recent one until the next
//! arrives or the lookback expires, so a step finer than the scrape
//! repeats itself.
//!
//! # The layers, bottom to top
//!
//! ```text
//! the union        one row per stored sample, both channels, with a
//!       |          stale flag
//! leadInFrame      cover_end = the first millisecond this sample stops
//!       |          being its series' most recent one: the next sample's
//!       |          timestamp, or ts + lookback
//!       v
//! arrayJoin(range) the grid indices this sample covers. A stale sample
//!       |          still occupies its interval — it blocks the earlier
//!       |          sample — and is then dropped by `WHERE NOT stale`,
//!       v          which is the reference's rule: a stale marker makes
//!                  the series absent and does not fall back to an older
//!                  sample.
//! GROUP BY gi,gid  the reduction, one row per (grid index, group)
//!       v
//! two windows      consecutive rows carrying the same answer collapse
//!  + GROUP BY      into one run row
//! ```
//!
//! # Why the reference's extremum rule is written out
//!
//! Reaching for ClickHouse's own `max` returns a different answer from
//! ours. Measured on 26.3: `max` over `[NaN, 1, 3]` answers `nan` while
//! `[1, NaN, 3]` answers `3`, because a NaN accumulator is never
//! replaced and rows arrive in fingerprint order. The reference replaces
//! its accumulator when the comparison says to **or the accumulator is
//! NaN** (`promql/engine.go:3812-3828` @ `v3.13.0`/`40af9c2`; ours is
//! `crates/pulsus-promql/src/eval/aggregation.rs`'s `Min | Max, None`
//! arm), which as a set operation is: the extremum of the non-NaN
//! members, or NaN when there are none. Order-independent. The guarded
//! expression below answers `3` on every ordering and `nan` only when no
//! member is a number.
//!
//! NaN is reachable on this path — the write path preserves a stale
//! marker as a NaN payload — so this is not a hypothetical.
//!
//! **The NaN a group answers is the payload its members carried**, not a
//! manufactured one: the last NaN member in fold order, which is the
//! highest fingerprint. `argMaxIf(v, fingerprint, NOT is_hist)`
//! reproduces that, and it is `argMax` under **both** `min` and `max` —
//! the rule selects the last member, not the extremum, so there is no
//! `argMinIf` variant.
//!
//! # The group key never enters SQL
//!
//! `gid` is assigned in our process by
//! [`pulsus_promql::group_key_of`] over the label sets the resolver
//! already returned, and reaches the statement as a parallel array
//! (`fps`/`gids`) consumed by `transform`. No label name, no label value
//! and no `by`/`without` list is rendered, so the three grouping forms
//! produce **byte-identical text** outside the `gids` array.

use super::grouped::{Grid, GroupedOp};
use super::sample_sql;

/// The `reinterpretAsUInt64` bit pattern of Prometheus's stale marker
/// (`pulsus_model::STALE_NAN_BITS`, `0x7FF0_0000_0000_0002`), as the
/// decimal literal ClickHouse compares against. Asserted equal to the
/// model constant in this module's tests rather than written twice and
/// hoped over.
const STALE_NAN_DECIMAL: u64 = 9_218_868_437_227_405_314;

/// One statement for one fingerprint chunk.
///
/// `fps` and `gids` are parallel: `gids[i]` is the group id of `fps[i]`.
/// The caller has already established that the grid arithmetic cannot
/// overflow ([`super::grouped::shape_of`] owns that guard and is the only
/// owner of it), and this function does not re-check it; it also does not
/// re-check the feature flag.
///
/// **Nine parameters, four of them interchangeable by type.** Transposing
/// `lower_excl_ms` and `upper_incl_ms`, or the two table names, compiles.
/// The signature is the one issue #549's plan specifies and is kept, so
/// what guards against a transposition is a test rather than the type
/// system: `the_max_template_renders_this_statement` below compares the
/// whole statement byte for byte, and
/// `crates/pulsus-read/tests/live_metrics_plan_parts.rs` reads the
/// statement back out of `system.query_log` and compares it against text
/// that test writes out itself. Both were run against a transposition
/// while this comment was written: swapping the two window arguments
/// reddens both.
#[allow(clippy::too_many_arguments)]
pub fn grouped_fetch(
    samples_table: &str,
    hist_samples_table: &str,
    metric_name: &str,
    fps: &[u64],
    gids: &[u32],
    grid: Grid,
    lower_excl_ms: i64,
    upper_incl_ms: i64,
    op: GroupedOp,
) -> String {
    let name = sample_sql::name_predicate(metric_name);
    let window = sample_sql::window_predicate(lower_excl_ms, upper_incl_ms);
    let fp_list = sample_sql::render_fingerprint_list(fps);
    let gid_list = gids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    // The four templates differ in exactly these four fragments. Every
    // other character — the union, the coverage arithmetic, the run
    // windowing — is one text for all four.
    let (outer_tail, carried, agg_projection, value_term) = match op {
        GroupedOp::Min | GroupedOp::Max => {
            let extremum = if matches!(op, GroupedOp::Min) {
                "minIf"
            } else {
                "maxIf"
            };
            (
                ", any(flags) AS flags",
                ", flags",
                format!(
                    "        if(countIf(NOT is_hist AND NOT isNaN(v)) = 0, \
                     argMaxIf(v, fingerprint, NOT is_hist),\n           \
                     {extremum}(v, NOT is_hist AND NOT isNaN(v))) AS agg,\n        \
                     toUInt8(if(countIf(NOT is_hist) > 0, 1, 0) + \
                     if(countIf(is_hist) > 0, 2, 0)) AS flags"
                ),
                // A float compares by its BITS, so a NaN answer that does
                // not change across grid indices stays one run: `nan !=
                // nan` is true in SQL and would start a fresh run at
                // every index.
                "\n              OR reinterpretAsUInt64(agg) != reinterpretAsUInt64(lagInFrame(agg) OVER w)\
                 \n              OR flags != lagInFrame(flags) OVER w",
            )
        }
        // `count` counts a histogram member rather than ignoring it, so
        // there is no `flags` column on these two templates at all and
        // nothing downstream reads one.
        GroupedOp::Count => (
            "",
            "",
            "        toUInt32(count()) AS agg".to_string(),
            "\n              OR agg != lagInFrame(agg) OVER w",
        ),
        GroupedOp::Group => (
            "",
            "",
            "        toUInt32(1) AS agg".to_string(),
            "\n              OR agg != lagInFrame(agg) OVER w",
        ),
    };

    let Grid {
        start_ms,
        step_ms,
        points,
        lookback_ms,
    } = grid;

    format!(
        "WITH {start_ms} AS grid_start, {step_ms} AS grid_step, {points} AS grid_n, \
         {lookback_ms} AS lookback,\n\
         \x20    [{fp_list}] AS fps,\n\
         \x20    CAST([{gid_list}], 'Array(UInt32)') AS gids\n\
         SELECT gid, min(gi) AS gi_start, max(gi) AS gi_end, any(agg) AS agg{outer_tail}\n\
         FROM (\n\
         \x20 SELECT gid, gi, agg{carried},\n\
         \x20   sum(is_new) OVER (PARTITION BY gid ORDER BY gi \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run\n\
         \x20 FROM (\n\
         \x20   SELECT gid, gi, agg{carried},\n\
         \x20     toUInt8(gi != lagInFrame(gi) OVER w + 1{value_term}) AS is_new\n\
         \x20   FROM (\n\
         \x20     SELECT gid, gi,\n\
         {agg_projection}\n\
         \x20     FROM (\n\
         \x20       SELECT gid, fingerprint, v, is_hist,\n\
         \x20         arrayJoin(range(\n\
         \x20           toUInt32(least(toInt64(grid_n),\n\
         \x20             if(ts <= grid_start, 0, \
         intDiv(ts - grid_start + grid_step - 1, grid_step)))),\n\
         \x20           toUInt32(least(toInt64(grid_n),\n\
         \x20             if(cover_end <= grid_start, 0, \
         intDiv(cover_end - grid_start + grid_step - 1, grid_step))))\n\
         \x20         )) AS gi\n\
         \x20       FROM (\n\
         \x20         SELECT transform(fingerprint, fps, gids, CAST(0, 'UInt32')) AS gid, \
         fingerprint,\n\
         \x20           ts, v, is_hist, stale,\n\
         \x20           least(leadInFrame(ts, 1, toInt64({upper_incl_ms}) + lookback + 1) OVER (\n\
         \x20                   PARTITION BY fingerprint ORDER BY ts, is_hist\n\
         \x20                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),\n\
         \x20                 ts + lookback) AS cover_end\n\
         \x20         FROM (\n\
         \x20           SELECT fingerprint, unix_milli AS ts, value AS v, \
         CAST(0, 'UInt8') AS is_hist,\n\
         \x20                  reinterpretAsUInt64(value) = {STALE_NAN_DECIMAL} AS stale\n\
         \x20           FROM {samples_table}\n\
         \x20           PREWHERE {name}\n\
         \x20           WHERE {window} AND fingerprint IN fps\n\
         \x20           UNION ALL\n\
         \x20           SELECT fingerprint, unix_milli AS ts, CAST(0, 'Float64') AS v, \
         CAST(1, 'UInt8') AS is_hist,\n\
         \x20                  reinterpretAsUInt64(sum) = {STALE_NAN_DECIMAL} AS stale\n\
         \x20           FROM {hist_samples_table}\n\
         \x20           PREWHERE {name}\n\
         \x20           WHERE {window} AND fingerprint IN fps\n\
         \x20         )\n\
         \x20       )\n\
         \x20       WHERE NOT stale\n\
         \x20     )\n\
         \x20     GROUP BY gi, gid\n\
         \x20   )\n\
         \x20   WINDOW w AS (PARTITION BY gid ORDER BY gi \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n\
         \x20 )\n\
         )\n\
         GROUP BY gid, run\n\
         ORDER BY gid, gi_start"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grid() -> Grid {
        Grid {
            start_ms: 1_782_907_200_000,
            step_ms: 15_000,
            points: 241,
            lookback_ms: 300_000,
        }
    }

    fn sql(op: GroupedOp) -> String {
        grouped_fetch(
            "metric_samples",
            "metric_hist_samples",
            "http_requests_total",
            &[101, 205, 990],
            &[0, 1, 0],
            grid(),
            1_782_906_900_000,
            1_782_910_800_000,
            op,
        )
    }

    /// The decimal the statement compares against IS the model's stale
    /// marker — written as a literal in SQL, so the two cannot be kept in
    /// step by the compiler.
    #[test]
    fn the_stale_literal_is_the_models_stale_marker() {
        assert_eq!(STALE_NAN_DECIMAL, pulsus_model::STALE_NAN_BITS);
        assert!(sql(GroupedOp::Max).contains("= 9218868437227405314 AS stale"));
    }

    /// The whole statement, byte for byte, once — so a change to any
    /// layer is visible as a text diff rather than as a property that
    /// still holds.
    #[test]
    fn the_max_template_renders_this_statement() {
        assert_eq!(
            sql(GroupedOp::Max),
            "WITH 1782907200000 AS grid_start, 15000 AS grid_step, 241 AS grid_n, \
             300000 AS lookback,\n\
             \x20    [101, 205, 990] AS fps,\n\
             \x20    CAST([0, 1, 0], 'Array(UInt32)') AS gids\n\
             SELECT gid, min(gi) AS gi_start, max(gi) AS gi_end, any(agg) AS agg, \
             any(flags) AS flags\n\
             FROM (\n\
             \x20 SELECT gid, gi, agg, flags,\n\
             \x20   sum(is_new) OVER (PARTITION BY gid ORDER BY gi \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run\n\
             \x20 FROM (\n\
             \x20   SELECT gid, gi, agg, flags,\n\
             \x20     toUInt8(gi != lagInFrame(gi) OVER w + 1\n\
             \x20             OR reinterpretAsUInt64(agg) != \
             reinterpretAsUInt64(lagInFrame(agg) OVER w)\n\
             \x20             OR flags != lagInFrame(flags) OVER w) AS is_new\n\
             \x20   FROM (\n\
             \x20     SELECT gid, gi,\n\
             \x20       if(countIf(NOT is_hist AND NOT isNaN(v)) = 0, \
             argMaxIf(v, fingerprint, NOT is_hist),\n\
             \x20          maxIf(v, NOT is_hist AND NOT isNaN(v))) AS agg,\n\
             \x20       toUInt8(if(countIf(NOT is_hist) > 0, 1, 0) + \
             if(countIf(is_hist) > 0, 2, 0)) AS flags\n\
             \x20     FROM (\n\
             \x20       SELECT gid, fingerprint, v, is_hist,\n\
             \x20         arrayJoin(range(\n\
             \x20           toUInt32(least(toInt64(grid_n),\n\
             \x20             if(ts <= grid_start, 0, \
             intDiv(ts - grid_start + grid_step - 1, grid_step)))),\n\
             \x20           toUInt32(least(toInt64(grid_n),\n\
             \x20             if(cover_end <= grid_start, 0, \
             intDiv(cover_end - grid_start + grid_step - 1, grid_step))))\n\
             \x20         )) AS gi\n\
             \x20       FROM (\n\
             \x20         SELECT transform(fingerprint, fps, gids, CAST(0, 'UInt32')) AS gid, \
             fingerprint,\n\
             \x20           ts, v, is_hist, stale,\n\
             \x20           least(leadInFrame(ts, 1, toInt64(1782910800000) + lookback + 1) OVER (\n\
             \x20                   PARTITION BY fingerprint ORDER BY ts, is_hist\n\
             \x20                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),\n\
             \x20                 ts + lookback) AS cover_end\n\
             \x20         FROM (\n\
             \x20           SELECT fingerprint, unix_milli AS ts, value AS v, \
             CAST(0, 'UInt8') AS is_hist,\n\
             \x20                  reinterpretAsUInt64(value) = 9218868437227405314 AS stale\n\
             \x20           FROM metric_samples\n\
             \x20           PREWHERE metric_name = 'http_requests_total'\n\
             \x20           WHERE unix_milli > 1782906900000 AND unix_milli <= 1782910800000 \
             AND fingerprint IN fps\n\
             \x20           UNION ALL\n\
             \x20           SELECT fingerprint, unix_milli AS ts, CAST(0, 'Float64') AS v, \
             CAST(1, 'UInt8') AS is_hist,\n\
             \x20                  reinterpretAsUInt64(sum) = 9218868437227405314 AS stale\n\
             \x20           FROM metric_hist_samples\n\
             \x20           PREWHERE metric_name = 'http_requests_total'\n\
             \x20           WHERE unix_milli > 1782906900000 AND unix_milli <= 1782910800000 \
             AND fingerprint IN fps\n\
             \x20         )\n\
             \x20       )\n\
             \x20       WHERE NOT stale\n\
             \x20     )\n\
             \x20     GROUP BY gi, gid\n\
             \x20   )\n\
             \x20   WINDOW w AS (PARTITION BY gid ORDER BY gi \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n\
             \x20 )\n\
             )\n\
             GROUP BY gid, run\n\
             ORDER BY gid, gi_start"
        );
    }

    /// `min` differs from `max` in ONE token, and `argMaxIf` is not one
    /// of them: the NaN payload rule selects the last member in fold
    /// order for both operations, never the extremum.
    #[test]
    fn min_differs_from_max_only_in_the_extremum_token() {
        let max = sql(GroupedOp::Max);
        let min = sql(GroupedOp::Min);
        assert_eq!(max.replace("maxIf(v, NOT", "minIf(v, NOT"), min);
        assert!(min.contains("argMaxIf(v, fingerprint, NOT is_hist)"));
        assert!(!min.contains("argMinIf"));
    }

    /// `count`/`group` carry no `flags` column at any level, and compare
    /// their integer answer directly rather than through its bits.
    #[test]
    fn the_counting_templates_carry_no_flags_column() {
        for op in [GroupedOp::Count, GroupedOp::Group] {
            let s = sql(op);
            assert!(!s.contains("flags"), "{op:?} still carries flags");
            assert!(!s.contains("reinterpretAsUInt64(agg)"), "{op:?}");
            assert!(s.contains("OR agg != lagInFrame(agg) OVER w"), "{op:?}");
        }
        assert!(sql(GroupedOp::Count).contains("toUInt32(count()) AS agg"));
        assert!(sql(GroupedOp::Group).contains("toUInt32(1) AS agg"));
    }

    /// The grouping form reaches the statement ONLY through the `gids`
    /// array: `by`, `without` and bare render the same text for the same
    /// gid assignment, and two different assignments differ in that one
    /// line and nowhere else.
    #[test]
    fn only_the_gids_array_carries_the_grouping() {
        let a = grouped_fetch(
            "metric_samples",
            "metric_hist_samples",
            "m",
            &[1, 2],
            &[0, 1],
            grid(),
            0,
            100,
            GroupedOp::Max,
        );
        let b = grouped_fetch(
            "metric_samples",
            "metric_hist_samples",
            "m",
            &[1, 2],
            &[0, 0],
            grid(),
            0,
            100,
            GroupedOp::Max,
        );
        assert_ne!(a, b);
        assert_eq!(
            a.replace("[0, 1], 'Array(UInt32)'", "[0, 0], 'Array(UInt32)'"),
            b
        );
    }

    /// The window predicate and the metric-name literal come from
    /// [`super::sample_sql`]'s own producers, so the grouped statement
    /// and the per-sample statement cannot disagree about what they read.
    #[test]
    fn the_read_predicates_are_the_sample_fetchs_own_fragments() {
        let s = sql(GroupedOp::Max);
        assert!(s.contains(&sample_sql::name_predicate("http_requests_total")));
        assert!(s.contains(&sample_sql::window_predicate(
            1_782_906_900_000,
            1_782_910_800_000
        )));
        // Left-open right-closed, the same as every other metrics read.
        assert!(!s.contains("unix_milli >= 1782906900000"));
    }

    /// An injected metric name stays inside one string literal, exactly
    /// as it does on the per-sample fetch.
    #[test]
    fn metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_samples; --";
        let s = grouped_fetch(
            "metric_samples",
            "metric_hist_samples",
            payload,
            &[1],
            &[0],
            grid(),
            0,
            100,
            GroupedOp::Max,
        );
        assert!(s.contains(&sample_sql::name_predicate(payload)));
        assert!(!s.contains("DROP TABLE metric_samples; --\n"));
    }

    /// ADR 0008 D2, as amended by issue #549: the statement binds no
    /// relational subquery through a common table expression. The
    /// `WITH … AS <alias>` forms here are a scalar and two arrays, which
    /// the rule permits — and which ClickHouse's own parser gives no
    /// binding node, the property `tests/live_sql_corpus_ast.rs` checks
    /// against the server rather than against a regex.
    #[test]
    fn the_with_clause_binds_no_relational_subquery() {
        for op in [
            GroupedOp::Min,
            GroupedOp::Max,
            GroupedOp::Count,
            GroupedOp::Group,
        ] {
            assert!(!sql(op).contains("AS (SELECT"), "{op:?}");
        }
    }
}
