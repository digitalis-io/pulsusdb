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

use pulsus_model::FpLiteral;

use super::grouped::{Grid, GroupedOp, PushedRangeFn, RangeAggOp};
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
/// overflow ([`super::grouped::node_verdicts`] owns that guard and is the only
/// owner of it), and this function does not re-check it; it also does not
/// re-check the feature flag.
///
/// **Eight parameters, four of them interchangeable by type.** Transposing
/// `lower_excl_ms` and `upper_incl_ms`, or the two table names, compiles.
/// The signature is the one issue #549's plan specifies less its metric name (issue #623), so
/// what guards against a transposition is a test rather than the type
/// system — and **which test depends on where the transposition is**,
/// measured by making each one:
///
/// ```text
///   the two window arguments swapped      hermetic golden   live statement
///   INSIDE this builder                   2 red             red
///   at the call site in `exec.rs`         green             red
/// ```
///
/// The hermetic golden (`the_max_template_renders_this_statement` below)
/// calls this function with its own literals, so it cannot see a caller
/// passing the right function the wrong arguments. That is what
/// `crates/pulsus-read/tests/live_metrics_plan_parts.rs` is for: it reads
/// the statement back out of `system.query_log` and compares it against
/// text that test writes out itself.
#[allow(clippy::too_many_arguments)]
pub fn grouped_fetch(
    samples_table: &str,
    hist_samples_table: &str,
    fps: &[FpLiteral],
    gids: &[u32],
    grid: Grid,
    lower_excl_ms: i64,
    upper_incl_ms: i64,
    op: GroupedOp,
) -> String {
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
         \x20           WHERE {window} AND fingerprint IN fps\n\
         \x20           UNION ALL\n\
         \x20           SELECT fingerprint, unix_milli AS ts, CAST(0, 'Float64') AS v, \
         CAST(1, 'UInt8') AS is_hist,\n\
         \x20                  reinterpretAsUInt64(sum) = {STALE_NAN_DECIMAL} AS stale\n\
         \x20           FROM {hist_samples_table}\n\
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

/// Issue #579: one shape-A statement (plan section 3.1) for one time
/// chunk of one node: one row per group per grid index, `SELECT gid, gi,
/// agg`, and one sentinel row whose `gid` is
/// [`super::grouped::HISTOGRAM_SENTINEL_GID`] carrying the count of
/// histogram samples in the window.
///
/// The window is `(grid start - range, grid end]`. The caller has
/// established that the grid arithmetic cannot overflow
/// ([`super::grouped::node_verdicts`] owns that guard).
#[allow(clippy::too_many_arguments)]
pub fn range_aggregate_fetch(
    samples_table: &str,
    hist_samples_table: &str,
    fps: &[FpLiteral],
    gids: &[u32],
    grid: Grid,
    range_ms: i64,
    op: RangeAggOp,
    func: PushedRangeFn,
) -> String {
    let (lower_excl_ms, upper_incl_ms) = range_window(grid, range_ms);
    let window = sample_sql::window_predicate(lower_excl_ms, upper_incl_ms);
    let fp_list = sample_sql::render_fingerprint_list(fps);
    let gid_list = gids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let Grid {
        start_ms,
        step_ms,
        points,
        ..
    } = grid;

    // The value columns and the condition a window must meet. `rate` and
    // `increase` extrapolate from the window's first and last samples and
    // differ only in the division by the range; `irate` reads the last
    // two samples.
    let (value, keep) = match func {
        PushedRangeFn::Rate | PushedRangeFn::Increase => {
            let per_second = if matches!(func, PushedRangeFn::Rate) {
                "((sampled + d_start + d_end) / sampled) / (toFloat64(range_ms) / 1000.) AS factor"
            } else {
                "(sampled + d_start + d_end) / sampled AS factor"
            };
            (
                format!(
                    "\n        i_l - i_f + 1 AS n,\
                     \n        arrayFold((acc, x) -> acc + x.2, arrayFilter(x -> x.1 > i_f, r_l),\
                     \n                  if(y_f < 0., (y_l - y_f) + 0., y_l - y_f)) AS result,\
                     \n        toFloat64(t_f - (grid_start + gi * grid_step - range_ms)) / 1000. AS d_start_raw,\
                     \n        toFloat64((grid_start + gi * grid_step) - t_l) / 1000. AS d_end_raw,\
                     \n        toFloat64(t_l - t_f) / 1000. AS sampled,\
                     \n        sampled / toFloat64(n - 1) AS avg_dur,\
                     \n        avg_dur * 1.1 AS threshold,\
                     \n        if(d_start_raw >= threshold, avg_dur / 2., d_start_raw) AS d_start_1,\
                     \n        if(result > 0. AND y_f >= 0., sampled * (y_f / result), d_start_1) AS d_zero,\
                     \n        if(d_zero < d_start_1, d_zero, d_start_1) AS d_start,\
                     \n        if(d_end_raw >= threshold, avg_dur / 2., d_end_raw) AS d_end,\
                     \n        {per_second},\
                     \n        result * factor AS v"
                ),
                "i_l > i_f",
            )
        }
        PushedRangeFn::Irate => (
            "\n        if(y_l < y_p, y_l, y_l - y_p) / (toFloat64(t_l - t_p) / 1000.) AS v"
                .to_string(),
            "t_p > grid_start + gi * grid_step - range_ms AND t_l != t_p",
        ),
    };
    let agg = range_agg_expression(op);

    format!(
        "WITH {start_ms} AS grid_start, {step_ms} AS grid_step, {points} AS grid_n, \
         {range_ms} AS range_ms,\n\
         \x20    [{fp_list}] AS fps,\n\
         \x20    CAST([{gid_list}], 'Array(UInt32)') AS gids\n\
         SELECT gid, gi, {agg} AS agg\n\
         FROM (\n\
         \x20 SELECT gid, gi, arrayMap(p -> p.2, arraySort(groupArray((fingerprint, v)))) AS vs\n\
         \x20 FROM (\n\
         \x20   SELECT transform(fingerprint, fps, gids, CAST(0, 'UInt32')) AS gid, fingerprint, gi,{value}\n\
         \x20   FROM (\n\
         \x20     SELECT fingerprint, gi, i_f, t_f, y_f, i AS i_l, ts AS t_l, y AS y_l, \
         ts_prev AS t_p, y_prev AS y_p, resets AS r_l\n\
         \x20     FROM (\n\
         \x20       SELECT fingerprint, gi, role, i, ts, y, ts_prev, y_prev, resets,\n\
         \x20         lagInFrame(i) OVER wr AS i_f, lagInFrame(ts) OVER wr AS t_f, \
         lagInFrame(y) OVER wr AS y_f\n\
         \x20       FROM (\n\
         \x20         SELECT fingerprint, i, ts, y, ts_prev, y_prev, resets, role,\n\
         \x20           arrayJoin(range(\n\
         \x20             toUInt32(least(toInt64(grid_n), if(a <= grid_start, 0, \
         intDiv(a - grid_start + grid_step - 1, grid_step)))),\n\
         \x20             toUInt32(least(toInt64(grid_n), if(b <= grid_start, 0, \
         intDiv(b - grid_start + grid_step - 1, grid_step)))))) AS gi\n\
         \x20         FROM (\n\
         \x20           SELECT fingerprint, i, ts, y, ts_prev, y_prev, resets, role,\n\
         \x20             if(role = 0, if(i = 1, ts, greatest(ts, ts_prev + range_ms)), ts) AS a,\n\
         \x20             if(role = 0, ts + range_ms, least(ts_next, ts + range_ms)) AS b\n\
         \x20           FROM (\n\
         \x20             SELECT fingerprint, ts, y, i, ts_prev, y_prev, ts_next,\n\
         \x20               groupArrayIf((i, y_prev), i > 1 AND y < y_prev) OVER w AS resets\n\
         \x20             FROM (\n\
         \x20               SELECT fingerprint, ts, y, yb,\n\
         \x20                 row_number() OVER w AS i,\n\
         \x20                 lagInFrame(ts, 1, toInt64(0)) OVER w AS ts_prev,\n\
         \x20                 lagInFrame(y, 1, 0.) OVER w AS y_prev,\n\
         \x20                 leadInFrame(ts, 1, toInt64(9223372036854775807)) OVER wf AS ts_next\n\
         \x20               FROM (\n\
         \x20                 SELECT fingerprint, unix_milli AS ts, reinterpretAsUInt64(value) AS yb, \
         value AS y,\n\
         \x20                   ts = lagInFrame(unix_milli, 1, toInt64(-1)) OVER w0\n\
         \x20                     AND yb = lagInFrame(reinterpretAsUInt64(value), 1, toUInt64(0)) \
         OVER w0 AS dup\n\
         \x20                 FROM {samples_table}\n\
         \x20                 WHERE {window} AND fingerprint IN fps\n\
         \x20                   AND reinterpretAsUInt64(value) != {STALE_NAN_DECIMAL}\n\
         \x20                 WINDOW w0 AS (PARTITION BY fingerprint ORDER BY unix_milli, \
         reinterpretAsUInt64(value)\n\
         \x20                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n\
         \x20               )\n\
         \x20               WHERE NOT dup\n\
         \x20               WINDOW w AS (PARTITION BY fingerprint ORDER BY ts, yb \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),\n\
         \x20                      wf AS (PARTITION BY fingerprint ORDER BY ts, yb \
         ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)\n\
         \x20             )\n\
         \x20             WINDOW w AS (PARTITION BY fingerprint ORDER BY ts, yb \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n\
         \x20           ) ARRAY JOIN [0, 1] AS role\n\
         \x20         )\n\
         \x20       )\n\
         \x20       WINDOW wr AS (PARTITION BY fingerprint ORDER BY gi, role \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n\
         \x20     )\n\
         \x20     WHERE role = 1\n\
         \x20   )\n\
         \x20   WHERE {keep}\n\
         \x20 )\n\
         \x20 GROUP BY gid, gi\n\
         )\n\
         UNION ALL\n\
         SELECT toUInt32(4294967295) AS gid, toUInt32(0) AS gi,\n\
         \x20 toFloat64((SELECT count() FROM {hist_samples_table}\n\
         \x20            WHERE {window} AND fingerprint IN fps)) AS agg\n\
         ORDER BY gid, gi"
    )
}

/// Issue #579: a shape-A statement's window, `(grid start - range, grid
/// end]` — the oldest sample any of its windows holds, and the last grid
/// point.
pub fn range_window(grid: Grid, range_ms: i64) -> (i64, i64) {
    (
        grid.start_ms - range_ms,
        grid.start_ms + (i64::from(grid.points) - 1) * grid.step_ms,
    )
}

/// `kahan_inc(inc, s, c)` (`pulsus_promql::math::kahan_inc`) inline: the
/// new sum and the new compensation.
fn kahan_inc(inc: &str, s: &str, c: &str) -> (String, String) {
    let t = format!("({s} + {inc})");
    (
        t.clone(),
        format!(
            "if(isInfinite({t}), 0., if(abs({s}) >= abs({inc}), {c} + (({s} - {t}) + {inc}), \
             {c} + (({inc} - {t}) + {s})))"
        ),
    )
}

/// `{AGG}` of plan section 3.1, folded over `vs` — the members' values in
/// ascending fingerprint order, the evaluator's own accumulation order.
fn range_agg_expression(op: RangeAggOp) -> String {
    match op {
        // `KahanSum::add`, read out as `sum + c`.
        RangeAggOp::Sum => "(arrayFold((acc, x) -> (acc.1 + x, if(isInfinite(acc.1 + x), 0., \
             if(abs(acc.1) >= abs(x), acc.2 + ((acc.1 - (acc.1 + x)) + x), \
             acc.2 + ((x - (acc.1 + x)) + acc.1)))), vs, \
             CAST((0., 0.), 'Tuple(Float64, Float64)')) AS k).1 + k.2"
            .to_string(),
        // `aggregate_reduce`'s float `avg`, over state `(n, sum, c,
        // incremental, mean)`: the first member raw; Kahan while the sum
        // stays finite; on the first infinite sum the incremental mean,
        // which absorbs that member and every later one; the split readout.
        RangeAggOp::Avg => {
            let n1 = "(acc.1 + 1.)";
            let (ks, kc) = kahan_inc("x", "acc.2", "acc.3");
            let q = format!("(({n1} - 1.) / {n1})");
            let (im_s, im_c) = kahan_inc(
                &format!("(x / {n1})"),
                &format!("({q} * (acc.2 / ({n1} - 1.)))"),
                &format!("({q} * (acc.3 / ({n1} - 1.)))"),
            );
            let (ii_s, ii_c) = kahan_inc(
                &format!("(x / {n1})"),
                &format!("({q} * acc.5)"),
                &format!("({q} * acc.3)"),
            );
            let lambda = format!(
                "(acc, x) -> if(acc.1 = 0., (1., x, 0., toUInt8(0), 0.), \
                 if(acc.4 = 1, ({n1}, acc.2, {ii_c}, toUInt8(1), {ii_s}), \
                 if(NOT isInfinite({ks}), ({n1}, {ks}, {kc}, toUInt8(0), 0.), \
                 ({n1}, acc.2, {im_c}, toUInt8(1), {im_s}))))"
            );
            let state = format!(
                "arrayFold({lambda}, vs, CAST((0., 0., 0., 0, 0.), \
                 'Tuple(Float64, Float64, Float64, UInt8, Float64)'))"
            );
            format!("if(({state} AS st).4 = 1, st.5 + st.3, st.2 / st.1 + st.3 / st.1)")
        }
        RangeAggOp::Count => "toFloat64(length(vs))".to_string(),
        // The reference's replacement rule: replace when the comparison
        // says to or the accumulator is NaN; the first member wins a tie.
        RangeAggOp::Min => {
            "arrayFold((acc, x) -> if(acc > x OR isNaN(acc), x, acc), vs, nan)".to_string()
        }
        RangeAggOp::Max => {
            "arrayFold((acc, x) -> if(acc < x OR isNaN(acc), x, acc), vs, nan)".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_model::Fingerprint;

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
            &[
                Fingerprint::from_raw(101).sql_literal(),
                Fingerprint::from_raw(205).sql_literal(),
                Fingerprint::from_raw(990).sql_literal(),
            ],
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
             \x20    [toUInt128('101'), toUInt128('205'), toUInt128('990')] AS fps,\n\
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
             \x20           WHERE unix_milli > 1782906900000 AND unix_milli <= 1782910800000 \
             AND fingerprint IN fps\n\
             \x20           UNION ALL\n\
             \x20           SELECT fingerprint, unix_milli AS ts, CAST(0, 'Float64') AS v, \
             CAST(1, 'UInt8') AS is_hist,\n\
             \x20                  reinterpretAsUInt64(sum) = 9218868437227405314 AS stale\n\
             \x20           FROM metric_hist_samples\n\
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
            &[
                Fingerprint::from_raw(1).sql_literal(),
                Fingerprint::from_raw(2).sql_literal(),
            ],
            &[0, 1],
            grid(),
            0,
            100,
            GroupedOp::Max,
        );
        let b = grouped_fetch(
            "metric_samples",
            "metric_hist_samples",
            &[
                Fingerprint::from_raw(1).sql_literal(),
                Fingerprint::from_raw(2).sql_literal(),
            ],
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

    /// The window predicate comes from [`super::sample_sql`]'s own
    /// producer, so the grouped statement and the per-sample statement
    /// cannot disagree about what they read.
    #[test]
    fn the_read_predicates_are_the_sample_fetchs_own_fragments() {
        let s = sql(GroupedOp::Max);
        assert!(s.contains(&sample_sql::window_predicate(
            1_782_906_900_000,
            1_782_910_800_000
        )));
        // Left-open right-closed, the same as every other metrics read.
        assert!(!s.contains("unix_milli >= 1782906900000"));
    }

    /// The sample tables carry no metric name: the series ID holds it, so
    /// the statement reads by ID alone and names no metric anywhere.
    #[test]
    fn the_statement_names_no_metric() {
        for op in [
            GroupedOp::Min,
            GroupedOp::Max,
            GroupedOp::Count,
            GroupedOp::Group,
        ] {
            let s = sql(op);
            assert!(!s.contains("metric_name"), "{op:?}");
            assert!(!s.contains("PREWHERE"), "{op:?}");
            assert!(s.contains("AND fingerprint IN fps"), "{op:?}");
        }
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

    // ------------------------------------------------ issue #579, shape A

    const RANGE_GOLDEN: &str = include_str!("../../tests/golden/range_aggregate_statements.txt");

    fn range_sql(op: RangeAggOp, func: PushedRangeFn) -> String {
        range_aggregate_fetch(
            "metric_samples",
            "metric_hist_samples",
            &[
                Fingerprint::from_raw(101).sql_literal(),
                Fingerprint::from_raw(205).sql_literal(),
                Fingerprint::from_raw(990).sql_literal(),
            ],
            &[0, 1, 0],
            grid(),
            300_000,
            op,
            func,
        )
    }

    /// The golden's sections, keyed by their `<op> <func>` marker.
    fn range_golden() -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut parts = RANGE_GOLDEN.split("-- golden[").skip(1);
        for part in parts.by_ref() {
            let (key, body) = part.split_once("]\n").expect("a marker line");
            out.push((
                key.to_string(),
                body.strip_suffix('\n')
                    .expect("a section ends in a newline")
                    .to_string(),
            ));
        }
        out
    }

    /// Every function under `sum`, and every aggregation under `rate`,
    /// renders the statement the design's generator wrote, byte for byte.
    #[test]
    fn each_range_function_and_aggregation_renders_the_designs_statement() {
        let golden = range_golden();
        assert_eq!(golden.len(), 7, "seven sections in the golden");
        for (key, want) in golden {
            let (op, func) = key.split_once(' ').expect("<op> <func>");
            let op = match op {
                "sum" => RangeAggOp::Sum,
                "avg" => RangeAggOp::Avg,
                "count" => RangeAggOp::Count,
                "min" => RangeAggOp::Min,
                "max" => RangeAggOp::Max,
                other => panic!("unknown op {other}"),
            };
            let func = match func {
                "rate" => PushedRangeFn::Rate,
                "irate" => PushedRangeFn::Irate,
                "increase" => PushedRangeFn::Increase,
                other => panic!("unknown function {other}"),
            };
            assert_eq!(range_sql(op, func), want, "{key}");
        }
    }

    /// The window is `(grid start - range, grid end]` on both tables, and
    /// the stale literal is the model's stale marker.
    #[test]
    fn the_range_statement_reads_its_window_and_drops_stale_markers() {
        let s = range_sql(RangeAggOp::Sum, PushedRangeFn::Rate);
        let window = sample_sql::window_predicate(1_782_906_900_000, 1_782_910_800_000);
        assert_eq!(s.matches(&window).count(), 2, "both tables, one window");
        assert!(s.contains(&format!(
            "reinterpretAsUInt64(value) != {}",
            pulsus_model::STALE_NAN_BITS
        )));
    }

    /// The grouping reaches the statement only through `gids`, and the
    /// sentinel row is in every statement.
    #[test]
    fn the_range_statement_carries_the_grouping_only_in_gids_and_always_a_sentinel() {
        for op in [
            RangeAggOp::Sum,
            RangeAggOp::Avg,
            RangeAggOp::Count,
            RangeAggOp::Min,
            RangeAggOp::Max,
        ] {
            for func in [
                PushedRangeFn::Rate,
                PushedRangeFn::Irate,
                PushedRangeFn::Increase,
            ] {
                let s = range_sql(op, func);
                assert!(
                    s.contains("SELECT toUInt32(4294967295) AS gid, toUInt32(0) AS gi,"),
                    "{op:?} {func:?}"
                );
                assert!(!s.contains("metric_name"), "{op:?} {func:?}");
                assert!(!s.contains("AS (SELECT"), "{op:?} {func:?}");
                let other = range_aggregate_fetch(
                    "metric_samples",
                    "metric_hist_samples",
                    &[
                        Fingerprint::from_raw(101).sql_literal(),
                        Fingerprint::from_raw(205).sql_literal(),
                        Fingerprint::from_raw(990).sql_literal(),
                    ],
                    &[0, 0, 0],
                    grid(),
                    300_000,
                    op,
                    func,
                );
                assert_eq!(
                    s.replace("[0, 1, 0], 'Array(UInt32)'", "[0, 0, 0], 'Array(UInt32)'"),
                    other,
                    "{op:?} {func:?}"
                );
            }
        }
    }

    /// The sentinel id is the one the reader looks for.
    #[test]
    fn the_sentinel_id_is_the_readers() {
        assert_eq!(super::super::grouped::HISTOGRAM_SENTINEL_GID, 4_294_967_295);
    }
}
