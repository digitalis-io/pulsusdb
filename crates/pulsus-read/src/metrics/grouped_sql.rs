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
//! manufactured one: the last NaN member in fold order, which is member
//! order, `(metric_name, fingerprint)` (issue #579 part 2).
//! `argMaxIf(v, <the member's position in fps>, NOT is_hist)` reproduces
//! that, and it is `argMax` under **both** `min` and `max` —
//! the rule selects the last member, not the extremum, so there is no
//! `argMinIf` variant.
//!
//! # The run statement's group key never enters SQL
//!
//! (The shape-A statement, [`range_aggregate_fetch`], computes its group
//! key in the statement instead: issue #579 part 3.)
//!
//! `gid` is assigned in our process by
//! [`pulsus_promql::group_key_of`] over the label sets the resolver
//! already returned, and reaches the statement as a parallel array
//! (`fps`/`gids`) consumed by `transform`. No label name, no label value
//! and no `by`/`without` list is rendered, so the three grouping forms
//! produce **byte-identical text** outside the `gids` array.

use pulsus_model::FpLiteral;

use pulsus_promql::Grouping;

use crate::logql::escape::ch_string;

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
                     argMaxIf(v, transform(fingerprint, fps, arrayEnumerate(fps), toUInt32(0)), NOT is_hist),\n           \
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

/// Issue #579: one shape-A statement for one time chunk of one node: one
/// row per group per grid index, `SELECT gid, gi, agg`, where `gid` is the
/// group's labels, and one sentinel row whose `gid` is
/// [`HISTOGRAM_SENTINEL_KEY`] carrying the count of histogram samples in
/// the window.
///
/// Part 3: the statement reads its series through `ids_sql`, the
/// selector's ID statement, in all three places — the samples, the
/// members and the histogram count — so its size does not grow with the
/// series count. `members_scope` is the selector's metric name when it has
/// one: the members' label rows are read in that name's key range.
/// `grouping` is computed in the statement from each series' stored
/// labels ([`group_key_expression`]).
///
/// Each stage is its own subquery, so no alias is expanded into another
/// stage:
///
/// ```text
/// gather       one row per series: (time, value bits), sorted, stale
///              markers dropped
/// dedup        the same without an exact repeat of the sample before
/// samples      pts: (time, index, value[, previous time, previous value])
///              rs: (index, previous value) of every reset
/// runs         pts_l / pts_f: each sample carries the last / first sample
///              of its equal-time run
/// merge        lasts / firsts: per step, the last sample at or before its
///              end and the first after its start, by one sort of the
///              samples with the step boundaries and a fill
/// windows      sts: (first, last, result, step) for each step with two
///              or more samples in its window
/// series-step  part 1's arithmetic over first, last and result
/// members      rank in (name, ID) order, and the group key
/// groups       the values in rank order, reduced
/// ```
///
/// The window is `(grid start - range, grid end]`. The caller has
/// established that the grid arithmetic cannot overflow
/// ([`super::grouped::node_verdicts`] owns that guard).
#[allow(clippy::too_many_arguments)]
pub fn range_aggregate_fetch(
    samples_table: &str,
    hist_samples_table: &str,
    labels_table: &str,
    ids_sql: &str,
    members_scope: Option<&str>,
    grouping: Option<&Grouping>,
    grid: Grid,
    range_ms: i64,
    op: RangeAggOp,
    func: PushedRangeFn,
) -> String {
    let (lower_excl_ms, upper_incl_ms) = range_window(grid, range_ms);
    let window = sample_sql::window_predicate(lower_excl_ms, upper_incl_ms);
    let Grid {
        start_ms,
        step_ms,
        points,
        ..
    } = grid;

    // `irate` reads the last sample and the one before it, so each sample
    // carries its predecessor's time and value (`0`, `0.` for the first);
    // `rate` and `increase` need neither.
    let irate = matches!(func, PushedRangeFn::Irate);
    let (pts, none, marker_tail) = if irate {
        (
            "arrayMap((t, j, y, tp, yp) -> (t, toUInt32(j), y, tp, yp), ts, arrayEnumerate(ts), ys, \
             arrayPushFront(arrayPopBack(ts), toInt64(0)), arrayPushFront(arrayPopBack(ys), 0.))",
            "(toInt64(-9223372036854775808), toUInt32(0), 0., toInt64(0), 0.)",
            ", toUInt32(0), 0., toInt64(0), 0.)",
        )
    } else {
        (
            "arrayMap((t, j, y) -> (t, toUInt32(j), y), ts, arrayEnumerate(ts), ys)",
            "(toInt64(-9223372036854775808), toUInt32(0), 0.)",
            ", toUInt32(0), 0.)",
        )
    };
    let (value, keep) = match func {
        PushedRangeFn::Rate | PushedRangeFn::Increase => {
            let factor = if matches!(func, PushedRangeFn::Rate) {
                "((sampled + d_start + d_end) / sampled) / (toFloat64(range_ms) / 1000.) AS factor"
            } else {
                "(sampled + d_start + d_end) / sampled AS factor"
            };
            (
                format!(
                    "        i_l - i_f + 1 AS n,\n\
                     \x20       toFloat64(t_f - (grid_start + gi * grid_step - range_ms)) / 1000. AS d_start_raw,\n\
                     \x20       toFloat64((grid_start + gi * grid_step) - t_l) / 1000. AS d_end_raw,\n\
                     \x20       toFloat64(t_l - t_f) / 1000. AS sampled,\n\
                     \x20       sampled / toFloat64(n - 1) AS avg_dur,\n\
                     \x20       avg_dur * 1.1 AS threshold,\n\
                     \x20       if(d_start_raw >= threshold, avg_dur / 2., d_start_raw) AS d_start_1,\n\
                     \x20       if(result > 0. AND y_f >= 0., sampled * (y_f / result), d_start_1) AS d_zero,\n\
                     \x20       if(d_zero < d_start_1, d_zero, d_start_1) AS d_start,\n\
                     \x20       if(d_end_raw >= threshold, avg_dur / 2., d_end_raw) AS d_end,\n\
                     \x20       {factor},\n\
                     \x20       result * factor AS v"
                ),
                String::new(),
            )
        }
        PushedRangeFn::Irate => (
            "        st.2.4 AS t_p, st.2.5 AS y_p,\n\
             \x20       if(y_l < y_p, y_l, y_l - y_p) / (toFloat64(t_l - t_p) / 1000.) AS v"
                .to_string(),
            "\n    WHERE t_p > grid_start + gi * grid_step - range_ms AND t_l != t_p".to_string(),
        ),
    };
    let agg = range_agg_expression(op);
    let gkey = group_key_expression(grouping);
    let members_where = match members_scope {
        Some(name) => format!(
            "WHERE metric_name = {}\n            AND fingerprint IN (",
            ch_string(name)
        ),
        None => "WHERE fingerprint IN (".to_string(),
    };

    format!(
        "WITH {start_ms} AS grid_start, {step_ms} AS grid_step, {points} AS grid_n, {range_ms} AS range_ms
SELECT gid, gi, agg FROM (
SELECT gid, gi,
{agg}
FROM (
  SELECT gid, gi, arraySort((x, r) -> r, groupArray(v), groupArray(rank)) AS vs
  FROM (
    SELECT gid, rank, toUInt32(st.4) AS gi,
        st.1.2 AS i_f, st.1.1 AS t_f, st.1.3 AS y_f, st.2.2 AS i_l, st.2.1 AS t_l, st.2.3 AS y_l, st.3 AS result,
{value}
    FROM (
      SELECT m.gkey AS gid, m.rank AS rank, arrayJoin(s.sts) AS st
      FROM (
        SELECT fingerprint,
          arrayFilter(x -> x.1.2 > 0 AND x.2.2 > x.1.2, arrayZip(firsts, lasts,
            if(empty(rs),
               arrayMap((f, l) -> if(f.3 < 0., (l.3 - f.3) + 0., l.3 - f.3), firsts, lasts),
               arrayMap((f, l) -> arrayCumSum(arrayPushFront(arrayMap(x -> x.2, arrayFilter(x -> x.1 > f.2 AND x.1 <= l.2, rs)),
                                                            if(f.3 < 0., (l.3 - f.3) + 0., l.3 - f.3)))[-1], firsts, lasts)),
            range(grid_n))) AS sts
        FROM (
          SELECT fingerprint, rs,
            arrayFilter((x, b) -> b, arrayFill((x, b) -> NOT b, m_l, arrayMap(x -> x.2 = 0, m_l)), arrayMap(x -> x.2 = 0, m_l)) AS lasts,
            arrayFilter((x, b) -> b, arrayReverseFill((x, b) -> NOT b, m_f, arrayMap(x -> x.2 = 0, m_f)), arrayMap(x -> x.2 = 0, m_f)) AS firsts
          FROM (
            SELECT fingerprint, rs,
              arraySort((x, k) -> k, arrayConcat(pts_l, arrayMap(e -> (e{marker_tail}, ends)),
                        arrayConcat(arrayMap(x -> x.1 * 2, pts), arrayMap(e -> e * 2 + 1, ends))) AS m_l,
              arraySort((x, k) -> k, arrayConcat(pts_f, arrayMap(e -> (e - range_ms{marker_tail}, ends)),
                        arrayConcat(arrayMap(x -> x.1 * 2, pts), arrayMap(e -> (e - range_ms) * 2 + 1, ends))) AS m_f
            FROM (
              SELECT fingerprint, pts, rs,
                arrayMap(g -> grid_start + toInt64(g) * grid_step, range(grid_n)) AS ends,
                arrayReverseFill((x, b) -> b, pts, arrayMap((x, n) -> x.1 != n.1, pts, arrayPushBack(arrayPopFront(pts), {none}))) AS pts_l,
                arrayFill((x, b) -> b, pts, arrayMap((x, p) -> x.1 != p.1, pts, arrayPushFront(arrayPopBack(pts), {none}))) AS pts_f
              FROM (
                SELECT fingerprint,
                  {pts} AS pts,
                  arrayFilter(x -> x.1 > 0, arrayMap((y, yp, j) -> if(j > 1 AND y < yp, (toUInt32(j), yp), (toUInt32(0), 0.)),
                                                      ys, arrayPushFront(arrayPopBack(ys), 0.), arrayEnumerate(ys))) AS rs
                FROM (
                  SELECT fingerprint,
                    arrayFilter((t, k) -> k, arrayMap(x -> x.1, raw), keep) AS ts,
                    arrayMap(b -> reinterpretAsFloat64(b), arrayFilter((b, k) -> k, arrayMap(x -> x.2, raw), keep)) AS ys
                  FROM (
                    SELECT fingerprint, raw,
                      arrayMap((x, p) -> x.1 != p.1 OR x.2 != p.2, raw, arrayPushFront(arrayPopBack(raw), (toInt64(-9223372036854775808), toUInt64(0)))) AS keep
                    FROM (
                      SELECT fingerprint, arraySort(arrayZip(groupArray(unix_milli), arrayMap(v -> reinterpretAsUInt64(v), groupArray(value)))) AS raw
                      FROM {samples_table}
                      WHERE {window} AND fingerprint IN (
{ids_sql}
)
                        AND reinterpretAsUInt64(value) != {STALE_NAN_DECIMAL}
                      GROUP BY fingerprint
                    )
                  )
                )
              )
            )
          )
        )
      ) AS s
      INNER JOIN (
        SELECT fingerprint, row_number() OVER (ORDER BY name, fingerprint) AS rank, {gkey} AS gkey
        FROM (
          SELECT fingerprint, any(metric_name) AS name, any(labels) AS l
          FROM {labels_table}
          {members_where}
{ids_sql}
            )
          GROUP BY fingerprint
        )
      ) AS m USING (fingerprint)
    ){keep}
  )
  GROUP BY gid, gi
)
)
UNION ALL
SELECT {HISTOGRAM_SENTINEL_KEY} AS gid, toUInt32(0) AS gi,
  toFloat64((SELECT count() FROM {hist_samples_table}
             WHERE {window} AND fingerprint IN (
{ids_sql}
))) AS agg
ORDER BY gid, gi"
    )
}

/// Issue #579 part 3: the number of distinct series `ids_sql` selects —
/// the series count a shape-A node is split by time over when the label
/// cache has no list. Distinct, because the ID statement returns one row
/// per activity row: a series active on two days, or not yet merged,
/// appears more than once.
pub fn series_count(ids_sql: &str) -> String {
    format!("SELECT uniqExact(fingerprint) AS n FROM (\n{ids_sql}\n)")
}

/// Issue #579 part 3: the `gid` of the row that carries the count of
/// histogram samples in a shape-A statement's window. No series' group
/// key can take it: no label name is empty.
pub const HISTOGRAM_SENTINEL_KEY: &str = "CAST([('', '')], 'Array(Tuple(String, String))')";

/// Issue #579 part 3: the group key of one series, computed in the
/// statement from its stored label JSON `l` — [`pulsus_promql::group_key_of`]'s
/// rule over the stored label set: no grouping is the one empty key,
/// `by` keeps the named labels a series has, `without` drops them. The
/// stored JSON's keys are sorted, so the key's pairs are in label order.
/// `by (__name__)` over a range function is never pushed (F4), so the
/// metric name never enters the key.
pub fn group_key_expression(grouping: Option<&Grouping>) -> String {
    match grouping {
        None => "CAST([], 'Array(Tuple(String, String))')".to_string(),
        Some(g) => {
            let names = g
                .labels
                .iter()
                .map(|l| ch_string(l))
                .collect::<Vec<_>>()
                .join(", ");
            let not = if g.without { "NOT " } else { "" };
            format!(
                "arrayFilter(kv -> kv.1 {not}IN ({names}), JSONExtractKeysAndValues(l, 'String'))"
            )
        }
    }
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

/// The group stage of a shape-A statement: the projection over `vs` — the
/// members' values in member order, `(metric_name, fingerprint)`, the
/// evaluator's own accumulation order — that ends in `agg`. Each is the
/// value part 1's per-element fold answered, computed element-wise over
/// running sums instead (issue #579 part 3); `live_metrics_grouped.rs`'s
/// `group_stages_equal_their_folds` holds each to its fold, bit for bit.
pub fn range_agg_expression(op: RangeAggOp) -> String {
    match op {
        // `KahanSum::add`, read out as `sum + c`. `t` is the running sum
        // `0, v1, v1+v2, …` — the fold's first accumulator — and each
        // member's compensation term is the fold's own, zeroed up to the
        // last member whose running sum is infinite, where the fold resets
        // it.
        RangeAggOp::Sum => "  arrayCumSum(arrayPushFront(vs, 0.)) AS t,\n\
             \x20 arrayLastIndex(x -> isInfinite(x), t) AS inf_at,\n\
             \x20 arrayMap((p, x, s, j) -> if(j <= inf_at, 0., if(abs(p) >= abs(x), (p - s) + x, (x - s) + p)),\n\
             \x20          arrayPopBack(t), vs, arrayPopFront(t), arrayEnumerate(vs)) AS terms,\n\
             \x20 t[-1] + arrayCumSum(arrayPushFront(terms, 0.))[-1] AS agg"
            .to_string(),
        // `aggregate_reduce`'s float `avg`: the first member raw, then
        // Kahan while the running sum stays finite, read out as
        // `sum / n + c / n`. That is the running sum `t` and the running
        // sum of the compensation terms after the first member. A running
        // sum that turns infinite switches the evaluator to its
        // incremental mean, which only part 1's fold computes, so those
        // rows keep it: the fold runs over their members and over an empty
        // array on every other row.
        //
        // The two answers are picked by index rather than by `if`. Under
        // `if` the fold's readout runs only on the rows that take it, and
        // there two NaN members' sum answered the other member's payload
        // from the evaluator's (measured on ClickHouse 26.3.29.7:
        // `0x7ff8000000000108` against `0x7ff800000000002c`); over the
        // whole column, as part 1 ran it, it answers the evaluator's.
        RangeAggOp::Avg => format!(
            "  arrayCumSum(vs) AS t,\n\
             \x20 arrayExists(x -> isInfinite(x), arrayPopFront(t)) AS inf_sum,\n\
             \x20 arrayMap((p, x, s) -> if(abs(p) >= abs(x), (p - s) + x, (x - s) + p),\n\
             \x20          arrayPopBack(t), arrayPopFront(vs), arrayPopFront(t)) AS terms,\n\
             \x20 toFloat64(length(vs)) AS n,\n\
             \x20 [t[-1] / n + arrayCumSum(arrayPushFront(terms, 0.))[-1] / n,\n\
             \x20  {}][inf_sum + 1] AS agg",
            avg_fold("if(inf_sum, vs, [])", "fs")
        ),
        RangeAggOp::Count => "  toFloat64(length(vs)) AS agg".to_string(),
        // The reference's replacement rule: replace when the comparison
        // says to or the accumulator is NaN, so the answer is the first
        // member holding the extremum of the non-NaN members, or the last
        // member when every member is NaN.
        RangeAggOp::Min | RangeAggOp::Max => {
            let extremum = if matches!(op, RangeAggOp::Min) {
                "arrayMin"
            } else {
                "arrayMax"
            };
            format!(
                "  arrayFilter(x -> NOT isNaN(x), vs) AS nn,\n\
                 \x20 {extremum}(nn) AS extremum,\n\
                 \x20 if(empty(nn), vs[-1], arrayFirst(x -> x = extremum, nn)) AS agg"
            )
        }
    }
}

/// Part 1's `avg` over `values`, over state `(n, sum, c, incremental,
/// mean)` named `state`: the first member raw; Kahan while the sum stays
/// finite; on the first infinite sum the incremental mean, which absorbs
/// that member and every later one; the split readout.
fn avg_fold(values: &str, state: &str) -> String {
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
    let fold = format!(
        "arrayFold({lambda}, {values}, CAST((0., 0., 0., 0, 0.), \
         'Tuple(Float64, Float64, Float64, UInt8, Float64)'))"
    );
    format!(
        "if(({fold} AS {state}).4 = 1, {state}.5 + {state}.3, \
         {state}.2 / {state}.1 + {state}.3 / {state}.1)"
    )
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
             argMaxIf(v, transform(fingerprint, fps, arrayEnumerate(fps), toUInt32(0)), NOT is_hist),\n\
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
        assert!(min.contains(
            "argMaxIf(v, transform(fingerprint, fps, arrayEnumerate(fps), toUInt32(0)), NOT is_hist)"
        ));
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

    // ------------------------------------------ issue #579 part 3, shape A

    const BY_IDS_GOLDEN: &str =
        include_str!("../../tests/golden/range_aggregate_by_ids_statements.txt");

    /// The ID statement of the design's own example (part 3, section 2).
    const PLAN_IDS: &str = "SELECT fingerprint\n\
         FROM metric_series\n\
         WHERE day BETWEEN '2026-10-08' AND '2026-10-08'\n\
         \x20 AND bitAnd(hours, 24) != 0\n\
         \x20 AND fingerprint IN (\n\
         \x20   SELECT fingerprint\n\
         \x20   FROM metric_labels\n\
         \x20   WHERE metric_name = 'cpu_seconds'\n\
         \x20 )";

    /// The design's example grid: 52 steps of 60 s from 03:48 UTC.
    fn plan_grid() -> Grid {
        Grid {
            start_ms: 1_791_431_280_000,
            step_ms: 60_000,
            points: 52,
            lookback_ms: 300_000,
        }
    }

    fn grouping_of(text: &str) -> Option<Grouping> {
        let list = |t: &str| -> Vec<String> {
            t.trim_start_matches('(')
                .trim_end_matches(')')
                .split(", ")
                .map(str::to_string)
                .collect()
        };
        if text == "none" {
            None
        } else if let Some(rest) = text.strip_prefix("by ") {
            Some(Grouping {
                without: false,
                labels: list(rest),
            })
        } else if let Some(rest) = text.strip_prefix("without ") {
            Some(Grouping {
                without: true,
                labels: list(rest),
            })
        } else {
            panic!("unknown grouping {text}")
        }
    }

    fn by_ids_sql(op: RangeAggOp, func: PushedRangeFn, grouping: Option<&Grouping>) -> String {
        range_aggregate_fetch(
            "metric_samples",
            "metric_hist_samples",
            "metric_labels",
            PLAN_IDS,
            Some("cpu_seconds"),
            grouping,
            plan_grid(),
            300_000,
            op,
            func,
        )
    }

    /// The by-ID golden's sections, keyed by `<op> <func> <grouping>`.
    fn by_ids_golden() -> Vec<(String, String)> {
        BY_IDS_GOLDEN
            .split("-- golden[")
            .skip(1)
            .map(|part| {
                let (key, body) = part.split_once("]\n").expect("a marker line");
                (
                    key.to_string(),
                    body.strip_suffix('\n')
                        .expect("a section ends in a newline")
                        .to_string(),
                )
            })
            .collect()
    }

    fn op_of(text: &str) -> RangeAggOp {
        match text {
            "sum" => RangeAggOp::Sum,
            "avg" => RangeAggOp::Avg,
            "count" => RangeAggOp::Count,
            "min" => RangeAggOp::Min,
            "max" => RangeAggOp::Max,
            other => panic!("unknown op {other}"),
        }
    }

    fn func_of(text: &str) -> PushedRangeFn {
        match text {
            "rate" => PushedRangeFn::Rate,
            "irate" => PushedRangeFn::Irate,
            "increase" => PushedRangeFn::Increase,
            other => panic!("unknown function {other}"),
        }
    }

    /// T7: `sum by (mode) (rate(cpu_seconds[5m]))` renders the design's
    /// statement byte for byte — the text written out in part 3's plan,
    /// section 2, before any of this was implemented.
    #[test]
    fn the_designs_by_id_statement_renders_byte_for_byte() {
        let golden = by_ids_golden();
        let (_, want) = golden
            .iter()
            .find(|(k, _)| k == "sum rate by (mode)")
            .expect("the design's section");
        let by_mode = grouping_of("by (mode)");
        assert_eq!(
            &by_ids_sql(RangeAggOp::Sum, PushedRangeFn::Rate, by_mode.as_ref()),
            want
        );
    }

    /// The golden's sections: the design's statement first, then each
    /// other function and aggregation under it, then each other grouping
    /// form. Every combination's structure is
    /// `every_by_id_statement_reads_through_the_id_statement`'s.
    const BY_IDS_SECTIONS: [&str; 9] = [
        "sum rate by (mode)",
        "sum irate by (mode)",
        "sum increase by (mode)",
        "avg rate by (mode)",
        "count rate by (mode)",
        "min rate by (mode)",
        "max rate by (mode)",
        "sum rate none",
        "sum rate without (cpu)",
    ];

    fn render_section(key: &str) -> String {
        let mut parts = key.splitn(3, ' ');
        let op = op_of(parts.next().expect("op"));
        let func = func_of(parts.next().expect("func"));
        let grouping = grouping_of(parts.next().expect("grouping"));
        by_ids_sql(op, func, grouping.as_ref())
    }

    /// Writes the by-ID golden from the builder. `#[ignore]`d: run only to
    /// capture a deliberate change to the statement, and read the diff.
    #[test]
    #[ignore = "regenerates the golden; run only to capture a deliberate change"]
    fn regenerate_the_by_id_golden() {
        let mut out = String::new();
        for key in BY_IDS_SECTIONS {
            out.push_str(&format!("-- golden[{key}]\n{}\n", render_section(key)));
        }
        std::fs::write(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/golden/range_aggregate_by_ids_statements.txt"
            ),
            out,
        )
        .expect("write the golden");
    }

    /// T7: every section of the by-ID golden renders byte for byte.
    #[test]
    fn every_by_id_section_renders_its_golden() {
        let golden = by_ids_golden();
        assert_eq!(
            golden.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            BY_IDS_SECTIONS,
            "the golden's sections"
        );
        for (key, want) in golden {
            assert_eq!(render_section(&key), want, "{key}");
        }
    }

    /// The window is `(grid start - range, grid end]` on both tables, and
    /// the stale literal is the model's stale marker.
    #[test]
    fn the_by_id_statement_reads_its_window_and_drops_stale_markers() {
        let s = by_ids_sql(RangeAggOp::Sum, PushedRangeFn::Rate, None);
        let window = sample_sql::window_predicate(1_791_430_980_000, 1_791_434_340_000);
        assert_eq!(s.matches(&window).count(), 2, "both tables, one window");
        assert!(s.contains(&format!(
            "reinterpretAsUInt64(value) != {}",
            pulsus_model::STALE_NAN_BITS
        )));
    }

    /// T7: the three grouping forms of the group key, as text.
    #[test]
    fn the_group_key_expression_renders_each_grouping_form() {
        assert_eq!(
            group_key_expression(None),
            "CAST([], 'Array(Tuple(String, String))')"
        );
        assert_eq!(
            group_key_expression(grouping_of("by (a, b)").as_ref()),
            "arrayFilter(kv -> kv.1 IN ('a', 'b'), JSONExtractKeysAndValues(l, 'String'))"
        );
        assert_eq!(
            group_key_expression(grouping_of("without (a)").as_ref()),
            "arrayFilter(kv -> kv.1 NOT IN ('a'), JSONExtractKeysAndValues(l, 'String'))"
        );
        // A label name reaches the statement through the string escaper.
        assert_eq!(
            group_key_expression(grouping_of("by (it's)").as_ref()),
            "arrayFilter(kv -> kv.1 IN ('it\\'s'), JSONExtractKeysAndValues(l, 'String'))"
        );
    }

    /// T7: every op, function and grouping form reads its series through
    /// the ID statement in all three places — the samples, the members
    /// and the histogram count — and carries no ID literal; a name-less
    /// selector's members carry no name scope.
    #[test]
    fn every_by_id_statement_reads_through_the_id_statement() {
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
                for grouping in ["none", "by (mode)", "without (cpu)"] {
                    let g = grouping_of(grouping);
                    let s = by_ids_sql(op, func, g.as_ref());
                    let what = format!("{op:?} {func:?} {grouping}");
                    assert_eq!(s.matches(PLAN_IDS).count(), 3, "{what}");
                    assert!(!s.contains("toUInt128("), "{what}");
                    assert!(!s.contains("WINDOW "), "{what}");
                    assert!(s.contains(&group_key_expression(g.as_ref())), "{what}");
                    let tail = format!(
                        "SELECT CAST([('', '')], 'Array(Tuple(String, String))') AS gid, \
                         toUInt32(0) AS gi,\n  toFloat64((SELECT count() FROM metric_hist_samples\n\
                         \x20            WHERE unix_milli > 1791430980000 AND unix_milli <= \
                         1791434340000 AND fingerprint IN (\n{PLAN_IDS}\n))) AS agg\n\
                         ORDER BY gid, gi"
                    );
                    assert!(s.ends_with(&tail), "{what}");
                    let nameless = range_aggregate_fetch(
                        "metric_samples",
                        "metric_hist_samples",
                        "metric_labels",
                        PLAN_IDS,
                        None,
                        g.as_ref(),
                        plan_grid(),
                        300_000,
                        op,
                        func,
                    );
                    assert_eq!(
                        nameless.replace(
                            "WHERE fingerprint IN (",
                            "WHERE metric_name = 'cpu_seconds'\n            AND fingerprint IN ("
                        ),
                        s,
                        "{what}"
                    );
                }
            }
        }
    }
}
