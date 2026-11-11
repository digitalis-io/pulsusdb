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

use pulsus_model::{FpLiteral, Tenant};

use pulsus_promql::{BinOp, Grouping};

use crate::logql::escape::ch_string;

use super::TenantSql;
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
    tenant: &Tenant,
    samples_table: &str,
    hist_samples_table: &str,
    fps: &[FpLiteral],
    gids: &[u32],
    grid: Grid,
    lower_excl_ms: i64,
    upper_incl_ms: i64,
    op: GroupedOp,
) -> String {
    let window = format!(
        "org_id = {} AND {}",
        tenant.sql_literal(),
        sample_sql::window_predicate(lower_excl_ms, upper_incl_ms)
    );
    let fp_array = sample_sql::render_fingerprint_array(fps);
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
         \x20    {fp_array} AS fps,\n\
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
         \x20                   PARTITION BY fingerprint ORDER BY ts, is_hist, reinterpretAsUInt64(v)\n\
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
    tenant: &Tenant,
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
    let window = format!(
        "org_id = {} AND {}",
        tenant.sql_literal(),
        sample_sql::window_predicate(lower_excl_ms, upper_incl_ms)
    );
    let Grid {
        start_ms,
        step_ms,
        points,
        ..
    } = grid;

    let value = value_text(func, "range_ms", None);
    let keep = if matches!(func, PushedRangeFn::Irate) {
        "\n    WHERE t_p > grid_start + gi * grid_step - range_ms AND t_l != t_p"
    } else {
        ""
    };
    let agg = range_agg_expression(op);
    let gkey = group_key_expression(grouping);
    // Issue #635 part 4: the member read is scoped to the tenant like
    // every other metrics table read.
    let members_where = match members_scope {
        Some(name) => format!(
            "WHERE org_id = {} AND metric_name = {}\n            AND fingerprint IN (",
            tenant.sql_literal(),
            ch_string(name)
        ),
        None => format!(
            "WHERE org_id = {} AND fingerprint IN (",
            tenant.sql_literal()
        ),
    };
    let gather = format!(
        "                      SELECT fingerprint, arraySort(arrayZip(groupArray(unix_milli), arrayMap(v -> reinterpretAsUInt64(v), groupArray(value)))) AS raw
                      FROM {samples_table}
                      WHERE {window} AND fingerprint IN (
{ids_sql}
)
                        AND reinterpretAsUInt64(value) != {STALE_NAN_DECIMAL}
                      GROUP BY fingerprint"
    );
    let windows = series_windows(&gather, "", "range_ms", func);

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
{windows}
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

/// Issue #579: the stage that turns each series' gathered points into its
/// steps, `sts`: one `(first, last, result, step)` per grid index with two
/// or more points in its window. `gather` returns `fingerprint[, arm], raw`;
/// `carry` is the columns carried beside `fingerprint` (part 4's ` arm,`);
/// `range` is the window width as SQL. The text is part 3's, so the shape-A
/// statement over raw renders byte for byte as it did.
fn series_windows(gather: &str, carry: &str, range: &str, func: PushedRangeFn) -> String {
    // `irate` reads the last sample and the one before it, so each sample
    // carries its predecessor's time and value (`0`, `0.` for the first);
    // `rate` and `increase` need neither.
    let (pts, none, marker_tail) = if matches!(func, PushedRangeFn::Irate) {
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
    format!(
        "        SELECT fingerprint,{carry}
          arrayFilter(x -> x.1.2 > 0 AND x.2.2 > x.1.2, arrayZip(firsts, lasts,
            if(empty(rs),
               arrayMap((f, l) -> if(f.3 < 0., (l.3 - f.3) + 0., l.3 - f.3), firsts, lasts),
               arrayMap((f, l) -> arrayCumSum(arrayPushFront(arrayMap(x -> x.2, arrayFilter(x -> x.1 > f.2 AND x.1 <= l.2, rs)),
                                                            if(f.3 < 0., (l.3 - f.3) + 0., l.3 - f.3)))[-1], firsts, lasts)),
            range(grid_n))) AS sts
        FROM (
          SELECT fingerprint,{carry} rs,
            arrayFilter((x, b) -> b, arrayFill((x, b) -> NOT b, m_l, arrayMap(x -> x.2 = 0, m_l)), arrayMap(x -> x.2 = 0, m_l)) AS lasts,
            arrayFilter((x, b) -> b, arrayReverseFill((x, b) -> NOT b, m_f, arrayMap(x -> x.2 = 0, m_f)), arrayMap(x -> x.2 = 0, m_f)) AS firsts
          FROM (
            SELECT fingerprint,{carry} rs,
              arraySort((x, k) -> k, arrayConcat(pts_l, arrayMap(e -> (e{marker_tail}, ends)),
                        arrayConcat(arrayMap(x -> x.1 * 2, pts), arrayMap(e -> e * 2 + 1, ends))) AS m_l,
              arraySort((x, k) -> k, arrayConcat(pts_f, arrayMap(e -> (e - {range}{marker_tail}, ends)),
                        arrayConcat(arrayMap(x -> x.1 * 2, pts), arrayMap(e -> (e - {range}) * 2 + 1, ends))) AS m_f
            FROM (
              SELECT fingerprint,{carry} pts, rs,
                arrayMap(g -> grid_start + toInt64(g) * grid_step, range(grid_n)) AS ends,
                arrayReverseFill((x, b) -> b, pts, arrayMap((x, n) -> x.1 != n.1, pts, arrayPushBack(arrayPopFront(pts), {none}))) AS pts_l,
                arrayFill((x, b) -> b, pts, arrayMap((x, p) -> x.1 != p.1, pts, arrayPushFront(arrayPopBack(pts), {none}))) AS pts_f
              FROM (
                SELECT fingerprint,{carry}
                  {pts} AS pts,
                  arrayFilter(x -> x.1 > 0, arrayMap((y, yp, j) -> if(j > 1 AND y < yp, (toUInt32(j), yp), (toUInt32(0), 0.)),
                                                      ys, arrayPushFront(arrayPopBack(ys), 0.), arrayEnumerate(ys))) AS rs
                FROM (
                  SELECT fingerprint,{carry}
                    arrayFilter((t, k) -> k, arrayMap(x -> x.1, raw), keep) AS ts,
                    arrayMap(b -> reinterpretAsFloat64(b), arrayFilter((b, k) -> k, arrayMap(x -> x.2, raw), keep)) AS ys
                  FROM (
                    SELECT fingerprint,{carry} raw,
                      arrayMap((x, p) -> x.1 != p.1 OR x.2 != p.2, raw, arrayPushFront(arrayPopBack(raw), (toInt64(-9223372036854775808), toUInt64(0)))) AS keep
                    FROM (
{gather}
                    )
                  )
                )
              )
            )
          )
        )"
    )
}

/// Issue #579: part 1's arithmetic over a step's first and last point and
/// its result, ending in `v`. `range` is the window width as SQL; `scale`
/// a rounded `increase`'s factor (part 4).
fn value_text(func: PushedRangeFn, range: &str, scale: Option<f64>) -> String {
    match func {
        PushedRangeFn::Rate | PushedRangeFn::Increase => {
            let factor = if matches!(func, PushedRangeFn::Rate) {
                format!(
                    "((sampled + d_start + d_end) / sampled) / (toFloat64({range}) / 1000.) AS factor"
                )
            } else {
                "(sampled + d_start + d_end) / sampled AS factor".to_string()
            };
            // Rust's shortest round-trip text of the factor, which the
            // statement reads back as the same `Float64`.
            let v = match scale {
                Some(f) => format!("result * factor * {f:?} AS v"),
                None => "result * factor AS v".to_string(),
            };
            format!(
                "        i_l - i_f + 1 AS n,\n\
                 \x20       toFloat64(t_f - (grid_start + gi * grid_step - {range})) / 1000. AS d_start_raw,\n\
                 \x20       toFloat64((grid_start + gi * grid_step) - t_l) / 1000. AS d_end_raw,\n\
                 \x20       toFloat64(t_l - t_f) / 1000. AS sampled,\n\
                 \x20       sampled / toFloat64(n - 1) AS avg_dur,\n\
                 \x20       avg_dur * 1.1 AS threshold,\n\
                 \x20       if(d_start_raw >= threshold, avg_dur / 2., d_start_raw) AS d_start_1,\n\
                 \x20       if(result > 0. AND y_f >= 0., sampled * (y_f / result), d_start_1) AS d_zero,\n\
                 \x20       if(d_zero < d_start_1, d_zero, d_start_1) AS d_start,\n\
                 \x20       if(d_end_raw >= threshold, avg_dur / 2., d_end_raw) AS d_end,\n\
                 \x20       {factor},\n\
                 \x20       {v}"
            )
        }
        PushedRangeFn::Irate => "        st.2.4 AS t_p, st.2.5 AS y_p,\n\
             \x20       if(y_l < y_p, y_l, y_l - y_p) / (toFloat64(t_l - t_p) / 1000.) AS v"
            .to_string(),
    }
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

/// Issue #18 part 2a: one level a `Level` side reads — its table, its
/// width in milliseconds, and the intervals it serves.
pub type LevelServed<'a> = (&'a str, i64, &'a [(i64, i64)]);

/// Issue #579 part 4: a `Level` side's reads for one time chunk, as phase 1
/// computed them from its coverage statement (S1).
#[derive(Debug, Clone, Copy)]
pub struct LevelRead<'a> {
    pub clustered: bool,
    pub shards: u32,
    /// The selector's fetch window `(lo, hi]`: S1's run predicate.
    pub window: (i64, i64),
    /// Issue #18 part 2a: each level the side reads, `(table, width ms,
    /// served)`, from the route's level down, its served part cut to `(lo,
    /// chunk end]`; a level that serves nothing is not listed.
    pub levels: &'a [LevelServed<'a>],
    /// Issue #18 part 2a: the window no level covers, cut likewise.
    pub todo: &'a [(i64, i64)],
    /// The raw read around the uncovered part, cut likewise.
    pub read: &'a [(i64, i64)],
    /// The chunk's grid indices whose window reaches the uncovered part,
    /// chunk-relative, `[g0, g1)`; empty unless `overlaid`.
    pub raw_steps: &'a [(u32, u32)],
    /// Issue #18 part 2a: the raw samples are read through the overlay
    /// (a route under 720 s), not folded into the counter.
    pub overlaid: bool,
}

/// Issue #18 part 2c: how a side's statements name its series: the ID
/// statement, or, for a side the label cache answered, the cache's own
/// fingerprints and members as literals.
#[derive(Debug, Clone, Copy)]
pub enum SideSeries<'a> {
    /// Issue #810 (D10): `gated` when the statement counts its own series
    /// and every read of the side is gated on the cap.
    Ids { ids: &'a str, gated: bool },
    Known {
        list: &'a str,
        members: &'a str,
        gated: bool,
    },
}

/// Issue #810 (D9, D10): what a pushed statement counts beside its groups:
/// its series, when a side is not listed, gated on `cap`, and `hist_n`, the
/// query's routed selectors' histogram samples (`{HIST_WHERE}`), when the
/// query has a routed selector.
#[derive(Debug, Clone, Copy, Default)]
pub struct Counts<'a> {
    pub series_n: Option<&'a str>,
    pub cap: usize,
    pub hist_where: Option<&'a str>,
}

/// Issue #18 part 2c: `MEMBERS_KNOWN` — a side's members from the label
/// cache's pairs, in the order given (ascending fingerprint), ranked from 1.
pub fn members_known(pairs: &[(FpLiteral, &pulsus_model::LabelSet)]) -> String {
    let members = pairs
        .iter()
        .enumerate()
        .map(|(i, (fp, labels))| {
            let mut kv: Vec<(&str, &str)> =
                labels.iter().filter(|(k, _)| *k != "__name__").collect();
            kv.sort_unstable();
            let kv = kv
                .iter()
                .map(|(k, v)| format!("({}, {})", ch_string(k), ch_string(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "({}, {}, [{kv}])",
                sample_sql::render_fingerprint_list(std::slice::from_ref(fp)),
                i + 1
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT m.1 AS fingerprint, m.2 AS rank, m.3 AS kv
FROM (SELECT arrayJoin(CAST([{members}], 'Array(Tuple(UInt128, UInt64, Array(Tuple(String, String))))')) AS m)"
    )
}

impl SideSeries<'_> {
    /// The statements' series filter: the literal list for a known side,
    /// else `fingerprint [GLOBAL ]IN (ids)`.
    pub fn filter(&self, global: bool) -> String {
        match self {
            SideSeries::Ids { ids, .. } => {
                let global = if global { "GLOBAL " } else { "" };
                format!("fingerprint {global}IN (\n{ids}\n)")
            }
            SideSeries::Known { list, .. } => format!("fingerprint IN ({list})"),
        }
    }
}

/// Issue #579 part 4: one side of a pushed node, as its statements read it.
#[derive(Debug, Clone, Copy)]
pub struct SideSql<'a> {
    pub tenant: &'a Tenant,
    pub samples_table: &'a str,
    pub labels_table: &'a str,
    /// Issue #18 part 2c: how the statements name the side's series.
    pub series: SideSeries<'a>,
    pub metric_name: &'a str,
    pub func: PushedRangeFn,
    pub range_ms: i64,
    /// A rounded `increase`'s factor, `asked / read`.
    pub scale: Option<f64>,
    /// The selector's fetch window `(lo, hi]`, which the histogram count
    /// reads.
    pub window: (i64, i64),
    /// `None` for a `Raw` arm.
    pub level: Option<LevelRead<'a>>,
}

/// Issue #579 part 4: `LEVEL_ROWS` — the level table's data rows of
/// `served` for the selector's series, one row per series with its rows
/// sorted. Issue #18 part 2c: no run rows and no replica check; a lagging
/// replica is passed over by `max_replica_delay_for_distributed_queries`.
pub fn level_rows(
    tenant: &Tenant,
    series: &SideSeries<'_>,
    table: &str,
    served: &[(i64, i64)],
) -> String {
    format!(
        "SELECT fingerprint, arraySort(arrayMap(x -> (x.1, x.2, x.3, x.4), groupArray((t_end, t_last, last, inc)))) AS rows0
FROM (
  SELECT fingerprint, t_end, t_last, last, inc
  FROM {table}
  WHERE kind = 0 AND org_id = {} AND {} AND {}
)
GROUP BY fingerprint",
        tenant.sql_literal(),
        sample_sql::intervals_predicate("t_end", "t_end", "<=", served),
        series.filter(true),
    )
}

/// Issue #18 part 2a: `COUNTER_GATHER`'s elements — one block per level,
/// its `LEVEL_ROWS` rows as `(t_end, width, t_last, last, inc)`, and, from
/// 720 s, the raw samples no level covers as `(t, 0, t, v, 0.)`.
fn counter_elements(
    tenant: &Tenant,
    samples_table: &str,
    series: &SideSeries<'_>,
    level: &LevelRead<'_>,
) -> Vec<String> {
    let mut out: Vec<String> = level
        .levels
        .iter()
        .map(|&(table, res_ms, served)| {
            format!(
                "SELECT fingerprint, arrayMap(r -> (r.1, toInt64({res_ms}), r.2, r.3, r.4), rows0) AS es
FROM (
{}
)",
                level_rows(tenant, series, table, served)
            )
        })
        .collect();
    if !level.overlaid && !level.todo.is_empty() {
        out.push(format!(
            "SELECT fingerprint, groupArray((unix_milli, toInt64(0), unix_milli, value, 0.)) AS es
FROM {samples_table}
WHERE org_id = {} AND {} AND {}
  AND reinterpretAsUInt64(value) != {STALE_NAN_DECIMAL}
GROUP BY fingerprint",
            tenant.sql_literal(),
            sample_sql::intervals_predicate("unix_milli", "unix_milli", "<=", level.todo),
            series.filter(false),
        ));
    }
    out
}

/// Issue #18 part 2a: `COUNTER_GATHER` — a `Level` arm's points: one
/// counter (`rollup::counter`) folded over the rows of every level the side
/// reads and, from 720 s, the raw samples no level covers, each series'
/// elements in time order. A level row adds its increase; a raw sample its
/// step from the last value, or itself on a drop; the first row or sample
/// with a number starts the counter. From 720 s a level row also gives a
/// start point. Each point carries its element's position and a sub-order
/// (the start point first), so points at one time keep the fold's order.
/// Stale markers are dropped and the groups with no point removed last.
pub fn counter_gather(
    tenant: &Tenant,
    samples_table: &str,
    series: &SideSeries<'_>,
    level: &LevelRead<'_>,
) -> String {
    let elements = counter_elements(tenant, samples_table, series, level).join("\nUNION ALL\n");
    format!(
        "SELECT fingerprint, arm, raw FROM (
SELECT fingerprint, toUInt8(0) AS arm,
  arrayMap(q -> (q.1, q.4), arraySort(arrayFilter(q -> q.4 != {STALE_NAN_DECIMAL}, arrayFlatten(arrayMap((x, i, c) ->
    if(s = 0 OR i < s,
       if(isNaN(x.4) AND (x.2 = 0 OR x.3 > 0), [(x.3, i, 1, reinterpretAsUInt64(x.4))], []),
       multiIf(x.2 > 0 AND x.3 = 0, CAST([], 'Array(Tuple(Int64, UInt64, UInt8, UInt64))'),
               isNaN(x.4), [(x.3, i, 1, reinterpretAsUInt64(x.4))],
               x.2 >= 720000, [(x.1 - x.2 + 1, i, 0, reinterpretAsUInt64(c - x.5)), (x.3, i, 1, reinterpretAsUInt64(c))],
               [(x.3, i, 1, reinterpretAsUInt64(c))])),
    e, arrayEnumerate(e), cs))))) AS raw
FROM (
  SELECT fingerprint, e, s,
    arrayConcat(arrayWithConstant(toUInt64(if(s = 0, length(e), s - 1)), 0.),
                if(s = 0, CAST([], 'Array(Float64)'),
                   arrayCumSum(arrayMap((x, d, i) -> if(i = 1, x.4, d), arraySlice(e, s), arraySlice(ds, s), arrayEnumerate(arraySlice(e, s)))))) AS cs
  FROM (
    SELECT fingerprint, e, s,
      arrayMap((x, pb) -> if(x.2 > 0, x.5, if(isNaN(x.4), 0., if(x.4 >= pb, x.4 - pb, x.4))), e,
               arrayPushFront(arrayPopBack(arrayFill((v, ok) -> ok, arrayMap(x -> x.4, e), sp)), nan)) AS ds
    FROM (
      SELECT fingerprint, e, arrayMap(x -> (x.2 = 0 OR x.3 > 0) AND NOT isNaN(x.4), e) AS sp, arrayFirstIndex(b -> b, sp) AS s
      FROM (
        SELECT fingerprint, arraySort(x -> (x.1, x.2, reinterpretAsUInt64(x.4), x.3, x.5), arrayFlatten(groupArray(es))) AS e
        FROM (
{elements}
        )
        GROUP BY fingerprint
      )
    )
  )
)
)
WHERE notEmpty(raw)"
    )
}

/// Issue #579 part 4: S4, explain only — whether a `Level` side read any
/// level row, today's `served_from_level` for a counter. Issue #18 part
/// 2a: over the `LEVEL_ROWS` of every level the side reads.
pub fn level_served(tenant: &Tenant, series: &SideSeries<'_>, level: &LevelRead<'_>) -> String {
    let rows = level
        .levels
        .iter()
        .map(|&(table, _, served)| level_rows(tenant, series, table, served))
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n");
    format!(
        "SELECT toUInt8(count() > 0) AS served
FROM (
{rows}
)"
    )
}

/// Issue #579 part 4: a side's raw points over the intervals `within`, its
/// `arm` 1: part 3's gather over a list of intervals.
pub fn raw_gather_within(
    tenant: &Tenant,
    samples_table: &str,
    series: &SideSeries<'_>,
    within: &[(i64, i64)],
) -> String {
    let filter = series.filter(false);
    let window = format!(
        "org_id = {} AND {}",
        tenant.sql_literal(),
        sample_sql::intervals_predicate("unix_milli", "unix_milli", "<=", within)
    );
    format!(
        "                      SELECT fingerprint, toUInt8(1) AS arm, arraySort(arrayZip(groupArray(unix_milli), arrayMap(v -> reinterpretAsUInt64(v), groupArray(value)))) AS raw
                      FROM {samples_table}
                      WHERE {window} AND {filter}
                        AND reinterpretAsUInt64(value) != {STALE_NAN_DECIMAL}
                      GROUP BY fingerprint"
    )
}

/// Issue #579 part 4: `SERIES_STEPS` — one row per series and grid index,
/// `fingerprint, gi, v`, each step taken from the arm today's route reads
/// it from: raw where its window reaches time no level covers (the raw
/// steps), the level elsewhere.
pub fn series_steps(side: &SideSql<'_>, grid: Grid) -> String {
    let range = side.range_ms.to_string();
    let (gather, steps) = match &side.level {
        Some(level) => {
            // Issue #18 part 2a: under 720 s the raw samples stay in part 4's
            // raw arm. A chunk in which no level serves and the counter
            // takes no raw sample reads the raw arm alone.
            let counter = !level.levels.is_empty() || (!level.overlaid && !level.todo.is_empty());
            let mut parts = Vec::new();
            if counter {
                parts.push(counter_gather(
                    side.tenant,
                    side.samples_table,
                    &side.series,
                    level,
                ));
            }
            if !counter || (level.overlaid && !level.read.is_empty()) {
                parts.push(raw_gather_within(
                    side.tenant,
                    side.samples_table,
                    &side.series,
                    level.read,
                ));
            }
            let steps = if level.overlaid {
                level.raw_steps.to_vec()
            } else {
                Vec::new()
            };
            (parts.join("\nUNION ALL\n"), steps)
        }
        None => {
            let (lo, hi) = range_window(grid, side.range_ms);
            (
                raw_gather_within(side.tenant, side.samples_table, &side.series, &[(lo, hi)]),
                vec![(0, grid.points)],
            )
        }
    };
    let steps = if steps.is_empty() {
        "CAST([], 'Array(Tuple(UInt32, UInt32))')".to_string()
    } else {
        format!(
            "[{}]",
            steps
                .iter()
                .map(|(a, b)| format!("({a}, {b})"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let keep = if matches!(side.func, PushedRangeFn::Irate) {
        format!(" AND t_p > grid_start + gi * grid_step - {range} AND t_l != t_p")
    } else {
        String::new()
    };
    let value = value_text(side.func, &range, side.scale);
    let windows = series_windows(&gather, " arm,", &range, side.func);
    format!(
        "SELECT fingerprint, toUInt32(st.4) AS gi,
  st.1.2 AS i_f, st.1.1 AS t_f, st.1.3 AS y_f, st.2.2 AS i_l, st.2.1 AS t_l, st.2.3 AS y_l, st.3 AS result,
{value}
FROM (
  SELECT fingerprint, arm, arrayJoin(sts) AS st
  FROM (
{windows}
  )
)
WHERE arm = toUInt8(arrayExists(x -> gi >= x.1 AND gi < x.2, {steps})){keep}"
    )
}

/// Issue #579 part 4: `MEMBERS` — each series' rank in `(name, ID)` order,
/// the order the evaluator folds in, and its stored label pairs.
pub fn members(side: &SideSql<'_>) -> String {
    let ids = match side.series {
        SideSeries::Ids { ids, .. } => ids,
        // Issue #18 part 2c: the label cache's own members.
        SideSeries::Known { members, .. } => return members.to_string(),
    };
    format!(
        "SELECT fingerprint, row_number() OVER (ORDER BY name, fingerprint) AS rank, JSONExtractKeysAndValues(l, 'String') AS kv
FROM (
  SELECT fingerprint, any(metric_name) AS name, any(labels) AS l
  FROM {}
  WHERE org_id = {} AND metric_name = {}
    AND fingerprint IN (
{}
    )
  GROUP BY fingerprint
)",
        side.labels_table,
        side.tenant.sql_literal(),
        ch_string(side.metric_name),
        ids
    )
}

/// Issue #579 part 4: [`group_key_expression`]'s rule over label pairs
/// `kv` instead of a series' stored JSON.
pub fn group_key_over(grouping: Option<&Grouping>, kv: &str) -> String {
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
            format!("arrayFilter(kv -> kv.1 {not}IN ({names}), {kv})")
        }
    }
}

/// The groups stage both part-4 statements share: `inner` gives `gid,
/// rank, gi, v`; the sentinel row carries the histogram count of
/// `sentinel_where`.
fn aggregate_over(
    grid: Grid,
    op: RangeAggOp,
    inner: &str,
    hist_samples_table: &str,
    sentinel_where: &str,
) -> String {
    let Grid {
        start_ms,
        step_ms,
        points,
        ..
    } = grid;
    let agg = range_agg_expression(op);
    format!(
        "WITH {start_ms} AS grid_start, {step_ms} AS grid_step, {points} AS grid_n
SELECT gid, gi, agg FROM (
SELECT gid, gi,
{agg}
FROM (
  SELECT gid, gi, arraySort((x, r) -> r, groupArray(v), groupArray(rank)) AS vs
  FROM (
{inner}
  )
  GROUP BY gid, gi
)
)
UNION ALL
SELECT {HISTOGRAM_SENTINEL_KEY} AS gid, toUInt32(0) AS gi,
  toFloat64((SELECT count() FROM {hist_samples_table} WHERE {sentinel_where})) AS agg
ORDER BY gid, gi"
    )
}

/// `unix_milli > lo AND unix_milli <= hi AND fingerprint IN (ids)`: one
/// side's histogram samples in its fetch window.
fn side_histograms(side: &SideSql<'_>) -> String {
    let (lo, hi) = side.window;
    format!(
        "unix_milli > {lo} AND unix_milli <= {hi} AND {}",
        side.series.filter(false)
    )
}

/// Issue #579 part 4: shape A over a `Level` arm, one time chunk (S3a):
/// one row per group per grid index and the histogram count.
pub fn level_aggregate_fetch(
    side: &SideSql<'_>,
    hist_samples_table: &str,
    grouping: Option<&Grouping>,
    grid: Grid,
    op: RangeAggOp,
    _counts: &Counts<'_>,
) -> String {
    let inner = format!(
        "    SELECT {} AS gid, m.rank AS rank, s.gi AS gi, s.v AS v
    FROM (
{}
    ) AS s
    INNER JOIN (
{}
    ) AS m USING (fingerprint)",
        group_key_over(grouping, "m.kv"),
        series_steps(side, grid),
        members(side)
    );
    let (lo, hi) = side.window;
    let sentinel = format!(
        "org_id = {} AND unix_milli > {lo} AND unix_milli <= {hi}
             AND {}",
        side.tenant.sql_literal(),
        side.series.filter(false)
    );
    aggregate_over(grid, op, &inner, hist_samples_table, &sentinel)
}

/// Issue #579 part 5: the key of S3c's matching sentinel row — a step at
/// which today's evaluator raises a matching error. No series' labels make
/// it, as [`HISTOGRAM_SENTINEL_KEY`].
pub const MATCH_SENTINEL_KEY: &str = "CAST([('', 'match')], 'Array(Tuple(String, String))')";

/// Issue #579 part 5: shape C with `on`, `ignoring` or `group_*`, one time
/// chunk (S3c), its sides in `(many, one)` order: `(lhs, rhs)`, or `(rhs,
/// lhs)` under `group_right`. Both sides' step rows are grouped by
/// `(signature, step)`; a step at which today's evaluator raises a matching
/// error gives the matching sentinel row instead of its outputs (plan
/// section 5, E1-E3), and every output drops its empty-valued labels.
#[allow(clippy::too_many_arguments)]
pub fn match_aggregate_fetch(
    many: &SideSql<'_>,
    one: &SideSql<'_>,
    hist_samples_table: &str,
    grouping: Option<&Grouping>,
    grid: Grid,
    op: RangeAggOp,
    bin: BinOp,
    bool_modifier: bool,
    spec: &super::grouped::MatchSpec,
    _counts: &Counts<'_>,
) -> String {
    use pulsus_promql::Group;
    let list = |labels: &[String]| {
        labels
            .iter()
            .map(|l| ch_string(l))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let labels = list(&spec.labels);
    let sig = |name: &str| {
        if spec.on && spec.labels.iter().any(|l| l == "__name__") {
            format!(
                "arrayConcat(arrayFilter(k -> k.1 IN ({labels}), m.kv), [('__name__', {})])",
                ch_string(name)
            )
        } else if spec.on {
            format!("arrayFilter(k -> k.1 IN ({labels}), m.kv)")
        } else {
            format!("arrayFilter(k -> k.1 NOT IN ({labels}), m.kv)")
        }
    };
    let (right, include) = match &spec.group {
        Group::OneToOne => (false, None),
        Group::Left(i) => (false, Some(i)),
        Group::Right(i) => (true, Some(i)),
    };
    let (l, r) = if right {
        ("one[1].2", "x.2")
    } else {
        ("x.2", "one[1].2")
    };
    let cmp = match bin {
        BinOp::Eq => Some("="),
        BinOp::Ne => Some("!="),
        BinOp::Gt => Some(">"),
        BinOp::Lt => Some("<"),
        BinOp::Ge => Some(">="),
        BinOp::Le => Some("<="),
        _ => None,
    };
    let (val, keep) = match cmp {
        None => {
            let arith = match bin {
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                _ => "/",
            };
            (format!("{l} {arith} {r}"), "1".to_string())
        }
        Some(c) if bool_modifier => (format!("toFloat64({l} {c} {r})"), "1".to_string()),
        Some(c) => (l.to_string(), format!("{l} {c} {r}")),
    };
    let arith = cmp.is_none();
    // Each label list is built as a vector and joined, so a list never
    // renders empty or with a leading comma. An include list holding only
    // `__name__` takes the empty list's form: the name is `{IDNAME}`'s.
    let kept = include.map(|i| {
        i.iter()
            .filter(|l| *l != "__name__")
            .map(|l| ch_string(l))
            .collect::<Vec<_>>()
    });
    let outkv = match &kept {
        None if arith => {
            "arrayFilter(k -> k.1 NOT IN ('__name__', '__type__', '__unit__'), sig)".to_string()
        }
        None => "arrayFilter(k -> k.1 != '__name__', sig)".to_string(),
        Some(inc) if inc.is_empty() && arith => {
            "arrayFilter(k -> k.1 NOT IN ('__type__', '__unit__'), x.3)".to_string()
        }
        Some(inc) if inc.is_empty() => "x.3".to_string(),
        Some(inc) => {
            let mut removed = inc.clone();
            if arith {
                removed.extend(["'__type__'".to_string(), "'__unit__'".to_string()]);
            }
            format!(
                "arraySort(arrayConcat(arrayFilter(k -> k.1 NOT IN ({}), x.3), \
                 arrayFilter(k -> k.1 IN ({}) AND k.2 != '', one[1].3)))",
                removed.join(", "),
                inc.join(", ")
            )
        }
    };
    let outkv = format!("arrayFilter(k -> k.2 != '', {outkv})");
    let e1 = "(length(one) >= 2 AND any_many = 1)";
    let bad = match include {
        None => format!("{e1} OR (length(many) >= 2 AND notEmpty(one))"),
        Some(i) => {
            let idname = if i.iter().any(|l| l == "__name__") {
                format!("[('__name__', {})]", ch_string(one.metric_name))
            } else if !arith {
                format!("[('__name__', {})]", ch_string(many.metric_name))
            } else {
                "CAST([], 'Array(Tuple(String, String))')".to_string()
            };
            let enc = |e: &str| {
                format!(
                    "if(length({e}) < 255, concat(char(length({e})), {e}), concat(char(255), \
                     char(length({e}) % 256), char(intDiv(length({e}), 256) % 256), \
                     char(intDiv(length({e}), 65536) % 256), {e}))"
                )
            };
            let idkey = format!(
                "xxHash64(arrayStringConcat(arrayMap(k -> concat({}, {}), arraySort(arrayConcat({outkv}, {idname})))))",
                enc("k.1"),
                enc("k.2")
            );
            format!(
                "{e1} OR (length(one) = 1 AND length(arrayDistinct(arrayMap(x -> {idkey}, many))) < length(many))"
            )
        }
    };
    let gkey = group_key_over(grouping, &outkv);
    let out = format!(
        "if(length(one) = 1, arrayMap(x -> ({gkey}, x.1, {val}), arrayFilter(x -> {keep}, many)), \
         CAST([], 'Array(Tuple(Array(Tuple(String, String)), UInt64, Float64))'))"
    );
    let inner = format!(
        "    SELECT o.1 AS gid, o.2 AS rank, gi, o.3 AS v
    FROM (
      SELECT gi, sig, many, one, any_many, {bad} AS bad
      FROM (
        SELECT sig, gi, many, one, max(notEmpty(many)) OVER (PARTITION BY gi) AS any_many
        FROM (
          SELECT sig, gi,
            groupArrayIf((rank, v, kv), side = 0) AS many,
            groupArrayIf((rank, v, kv), side = 1) AS one
          FROM (
            SELECT toUInt8(0) AS side, s.gi AS gi, m.rank AS rank, s.v AS v, m.kv AS kv, {} AS sig
            FROM (
{}
            ) AS s
            INNER JOIN (
{}
            ) AS m USING (fingerprint)
            UNION ALL
            SELECT toUInt8(1) AS side, s.gi AS gi, m.rank AS rank, s.v AS v, m.kv AS kv, {} AS sig
            FROM (
{}
            ) AS s
            INNER JOIN (
{}
            ) AS m USING (fingerprint)
          )
          GROUP BY sig, gi
        )
      )
    )
    ARRAY JOIN if(bad, [({MATCH_SENTINEL_KEY}, toUInt64(0), nan)], {out}) AS o",
        sig(many.metric_name),
        series_steps(many, grid),
        members(many),
        sig(one.metric_name),
        series_steps(one, grid),
        members(one),
    );
    // S3b's histogram condition, its sides in written order.
    let (lhs, rhs) = if right { (one, many) } else { (many, one) };
    let sentinel = format!(
        "org_id = {} AND (({}) OR ({}))",
        lhs.tenant.sql_literal(),
        side_histograms(lhs),
        side_histograms(rhs)
    );
    aggregate_over(grid, op, &inner, hist_samples_table, &sentinel)
}

/// Issue #579 part 4: shape C, one time chunk (S3b): both sides' steps,
/// their series paired by label set, the operator, and the aggregate over
/// the left member's rank. Arithmetic drops `__type__`/`__unit__` from the
/// output labels; a comparison keeps them, filtering on its result or,
/// under `bool`, answering it. Issue #579 part 5: every output drops its
/// empty-valued labels, as the reference's result builder does.
#[allow(clippy::too_many_arguments)]
pub fn join_aggregate_fetch(
    lhs: &SideSql<'_>,
    rhs: &SideSql<'_>,
    hist_samples_table: &str,
    grouping: Option<&Grouping>,
    grid: Grid,
    op: RangeAggOp,
    bin: BinOp,
    bool_modifier: bool,
    _counts: &Counts<'_>,
) -> String {
    let cmp = match bin {
        BinOp::Eq => Some("="),
        BinOp::Ne => Some("!="),
        BinOp::Gt => Some(">"),
        BinOp::Lt => Some("<"),
        BinOp::Ge => Some(">="),
        BinOp::Le => Some("<="),
        _ => None,
    };
    let arith = match bin {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        _ => "/",
    };
    let (val, filter, outkv) = match cmp {
        None => (
            format!("l.v {arith} r.v"),
            String::new(),
            "arrayFilter(k -> k.2 != '', arrayFilter(kv -> kv.1 NOT IN ('__type__', '__unit__'), lm.kv))",
        ),
        Some(c) if bool_modifier => (
            format!("toFloat64(l.v {c} r.v)"),
            String::new(),
            "arrayFilter(k -> k.2 != '', lm.kv)",
        ),
        Some(c) => (
            "l.v".to_string(),
            format!("\n    WHERE l.v {c} r.v"),
            "arrayFilter(k -> k.2 != '', lm.kv)",
        ),
    };
    let inner = format!(
        "    SELECT p.gkey AS gid, p.rank AS rank, l.gi AS gi, {val} AS v
    FROM (
{}
    ) AS l
    INNER JOIN (
      SELECT lm.fingerprint AS lfp, rm.fingerprint AS rfp, lm.rank AS rank, {} AS gkey
      FROM (
{}
      ) AS lm
      INNER JOIN (
{}
      ) AS rm ON lm.kv = rm.kv
    ) AS p ON l.fingerprint = p.lfp
    INNER JOIN (
{}
    ) AS r ON r.fingerprint = p.rfp AND r.gi = l.gi{filter}",
        series_steps(lhs, grid),
        group_key_over(grouping, outkv),
        members(lhs),
        members(rhs),
        series_steps(rhs, grid),
    );
    let sentinel = format!(
        "org_id = {} AND (({}) OR ({}))",
        lhs.tenant.sql_literal(),
        side_histograms(lhs),
        side_histograms(rhs)
    );
    aggregate_over(grid, op, &inner, hist_samples_table, &sentinel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_model::Fingerprint;

    /// The single-tenant deployment's tenant: no `X-Scope-OrgID`.
    fn no_tenant() -> Tenant {
        Tenant::from_header(None, false).expect("no header is the empty tenant")
    }

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
            &no_tenant(),
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
             \x20    CAST(['101', '205', '990'], 'Array(UInt128)') AS fps,\n\
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
             \x20                   PARTITION BY fingerprint ORDER BY ts, is_hist, reinterpretAsUInt64(v)\n\
             \x20                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),\n\
             \x20                 ts + lookback) AS cover_end\n\
             \x20         FROM (\n\
             \x20           SELECT fingerprint, unix_milli AS ts, value AS v, \
             CAST(0, 'UInt8') AS is_hist,\n\
             \x20                  reinterpretAsUInt64(value) = 9218868437227405314 AS stale\n\
             \x20           FROM metric_samples\n\
             \x20           WHERE org_id = '' AND unix_milli > 1782906900000 AND unix_milli <= 1782910800000 \
             AND fingerprint IN fps\n\
             \x20           UNION ALL\n\
             \x20           SELECT fingerprint, unix_milli AS ts, CAST(0, 'Float64') AS v, \
             CAST(1, 'UInt8') AS is_hist,\n\
             \x20                  reinterpretAsUInt64(sum) = 9218868437227405314 AS stale\n\
             \x20           FROM metric_hist_samples\n\
             \x20           WHERE org_id = '' AND unix_milli > 1782906900000 AND unix_milli <= 1782910800000 \
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
            &no_tenant(),
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
            &no_tenant(),
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
            &no_tenant(),
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
                    // No ID literal in any spelling (issue #399).
                    assert!(
                        !s.contains("toUInt128(")
                            && !s.contains("fingerprint IN ('")
                            && !s.contains("'Array(UInt128)')"),
                        "{what}"
                    );
                    assert!(!s.contains("WINDOW "), "{what}");
                    assert!(s.contains(&group_key_expression(g.as_ref())), "{what}");
                    let tail = format!(
                        "SELECT CAST([('', '')], 'Array(Tuple(String, String))') AS gid, \
                         toUInt32(0) AS gi,\n  toFloat64((SELECT count() FROM metric_hist_samples\n\
                         \x20            WHERE org_id = '' AND unix_milli > 1791430980000 AND unix_milli <= \
                         1791434340000 AND fingerprint IN (\n{PLAN_IDS}\n))) AS agg\n\
                         ORDER BY gid, gi"
                    );
                    assert!(s.ends_with(&tail), "{what}");
                    let nameless = range_aggregate_fetch(
                        &no_tenant(),
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
                            "WHERE org_id = '' AND fingerprint IN (",
                            "WHERE org_id = '' AND metric_name = 'cpu_seconds'\n            AND fingerprint IN ("
                        ),
                        s,
                        "{what}"
                    );
                }
            }
        }
    }

    // ------------------------------------------ issue #579 part 4, T11

    const JOIN_GOLDEN: &str = include_str!("../../tests/golden/join_aggregate_statements.txt");

    /// T11's ID statement of one metric.
    fn t11_ids(name: &str) -> String {
        format!(
            "SELECT fingerprint\n\
             FROM metric_series\n\
             WHERE day BETWEEN '2026-10-29' AND '2026-10-30'\n\
             \x20 AND bitAnd(hours, 16777215) != 0\n\
             \x20 AND fingerprint IN (\n\
             \x20   SELECT fingerprint\n\
             \x20   FROM metric_labels\n\
             \x20   WHERE metric_name = '{name}'\n\
             \x20 )"
        )
    }

    /// The golden's sections, keyed by their marker.
    fn join_golden() -> Vec<(String, String)> {
        JOIN_GOLDEN
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

    const T11_START: i64 = 1_793_300_400_000;
    const T11_END: i64 = T11_START + 720 * 120_000;
    const T11_WINDOW: (i64, i64) = (T11_START - 600_000, T11_END);
    const T11_SERVED: [(i64, i64); 2] = [
        (T11_WINDOW.0, T11_END - 21_600_000),
        (T11_END - 19_800_000, T11_END - 600_000),
    ];
    const T11_READ: [(i64, i64); 2] = [
        (T11_END - 21_900_000, T11_END - 19_500_000),
        (T11_END - 900_000, T11_END),
    ];
    const T11_STEPS: [(u32, u32); 2] = [(541, 559), (715, 721)];

    const T11_TODO: [(i64, i64); 2] = [
        (T11_END - 21_600_000, T11_END - 19_800_000),
        (T11_END - 600_000, T11_END),
    ];

    fn t11_level() -> LevelRead<'static> {
        const LEVELS: [LevelServed<'static>; 1] = [("metric_samples_60s", 60_000, &T11_SERVED)];
        LevelRead {
            clustered: false,
            shards: 1,
            window: T11_WINDOW,
            read: &T11_READ,
            raw_steps: &T11_STEPS,
            levels: &LEVELS,
            todo: &T11_TODO,
            overlaid: true,
        }
    }

    fn t11_grid() -> Grid {
        Grid {
            start_ms: T11_START,
            step_ms: 120_000,
            points: 721,
            lookback_ms: 300_000,
        }
    }

    /// The section `key` of the golden, which a script rendered from part
    /// 4's plan, section 6, before any builder existed.
    fn join_section(key: &str) -> String {
        join_golden()
            .into_iter()
            .find(|(k, _)| k == key)
            .unwrap_or_else(|| panic!("no section {key}"))
            .1
    }

    /// **U3 (issue #18 part 2a): the counter gather renders the plan's
    /// text byte for byte**: (a) a 720 s side over a 60 s level and raw
    /// samples; (b) a 60 s side over a 5 s level, overlaid, with the raw
    /// arm; (c) a 60 s side at the finest level, part 4's case.
    #[test]
    fn the_counter_gather_renders_the_plans_text() {
        let tenant = no_tenant();
        let ids = t11_ids("p_rx_total");
        let h = 3_600_000;
        let m = 60_000;
        let s720 = vec![
            (T11_WINDOW.0, T11_END - 20 * h),
            (T11_END - 19 * h, T11_END - 12 * m),
        ];
        let s60a = vec![
            (T11_END - 20 * h, T11_END - 19 * h),
            (T11_END - 12 * m, T11_END - 10 * m),
        ];
        let todo_a = [(T11_END - 10 * m, T11_END)];
        let levels_a = [
            ("metric_samples_720s", 720_000, s720.as_slice()),
            ("metric_samples_60s", 60_000, s60a.as_slice()),
        ];
        let a = LevelRead {
            clustered: false,
            shards: 1,
            window: T11_WINDOW,
            levels: &levels_a,
            todo: &todo_a,
            read: &todo_a,
            raw_steps: &[],
            overlaid: false,
        };
        assert_eq!(
            counter_gather(&tenant, "metric_samples", &SideSeries::Ids { ids: &ids, gated: false }, &a),
            join_section("counter_gather 720s over 60s and raw"),
            "(a)"
        );
        let s60b = vec![
            (T11_WINDOW.0, T11_END - 6 * h),
            (T11_END - 5 * h - 30 * m, T11_END - 4 * h),
        ];
        let s5b = vec![(T11_END - 4 * h, T11_END - 10 * m)];
        let levels_b = [
            ("metric_samples_60s", 60_000, s60b.as_slice()),
            ("metric_samples_5s", 5_000, s5b.as_slice()),
        ];
        let b = LevelRead {
            levels: &levels_b,
            ..t11_level()
        };
        let side = SideSql {
            tenant: &tenant,
            samples_table: "metric_samples",
            labels_table: "metric_labels",
            series: SideSeries::Ids { ids: &ids, gated: false },
            metric_name: "p_rx_total",
            func: PushedRangeFn::Rate,
            range_ms: 300_000,
            scale: None,
            window: T11_WINDOW,
            level: Some(b),
        };
        assert_eq!(
            series_steps(&side, t11_grid()),
            join_section("series_steps 60s over 5s"),
            "(b)"
        );
        assert_eq!(
            counter_gather(
                &tenant,
                "metric_samples",
                &SideSeries::Ids { ids: &ids, gated: false },
                &t11_level()
            ),
            join_section("counter_gather 60s"),
            "(c)"
        );
    }

    /// **U1 (issue #18 part 2c): a known side names its series**: the
    /// members from the label cache's pairs, and a 720 s side's steps whose
    /// every series filter is the literal list.
    #[test]
    fn a_known_side_names_its_series() {
        use pulsus_model::{Fingerprint, LabelSet};
        let pair = |pairs: &[(&str, &str)]| {
            LabelSet::from_verbatim(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))
        };
        let (a, b) = (
            pair(&[("instance", "i0"), ("device", "d0")]),
            pair(&[("instance", "i'3"), ("__unit__", "bytes")]),
        );
        let members = members_known(&[
            (Fingerprint::from_raw(101).sql_literal(), &a),
            (Fingerprint::from_raw(205).sql_literal(), &b),
        ]);
        assert_eq!(members, join_section("members_known two series"), "members");
        let tenant = no_tenant();
        let h = 3_600_000;
        let m = 60_000;
        let s720 = vec![
            (T11_WINDOW.0, T11_END - 20 * h),
            (T11_END - 19 * h, T11_END - 12 * m),
        ];
        let s60a = vec![
            (T11_END - 20 * h, T11_END - 19 * h),
            (T11_END - 12 * m, T11_END - 10 * m),
        ];
        let todo_a = [(T11_END - 10 * m, T11_END)];
        let levels_a = [
            ("metric_samples_720s", 720_000, s720.as_slice()),
            ("metric_samples_60s", 60_000, s60a.as_slice()),
        ];
        let level = LevelRead {
            clustered: false,
            shards: 1,
            window: T11_WINDOW,
            levels: &levels_a,
            todo: &todo_a,
            read: &todo_a,
            raw_steps: &[],
            overlaid: false,
        };
        let side = SideSql {
            tenant: &tenant,
            samples_table: "metric_samples",
            labels_table: "metric_labels",
            series: SideSeries::Known {
                list: "'101', '205'",
                members: &members,
                gated: false,
            },
            metric_name: "p_rx_total",
            func: PushedRangeFn::Rate,
            range_ms: 300_000,
            scale: None,
            window: T11_WINDOW,
            level: Some(level),
        };
        assert_eq!(
            series_steps(&side, t11_grid()),
            join_section("series_steps 720s known"),
            "steps"
        );
    }

    /// **T13 (issue #810, D10): every read of a counted statement is gated.**
    /// Shape C with an `Ids` side and a `Known` side of 3 literals, both
    /// gated, its series counted: every series read of either side, the
    /// level rows, the histogram count and the members alike, carries the
    /// gate, so the gates equal the reads (`hist_n`'s own condition, which
    /// is never gated, aside).
    #[test]
    fn every_read_of_a_counted_statement_is_gated() {
        let tenant = no_tenant();
        let ids = "SELECT fingerprint FROM metric_series";
        let labels: Vec<pulsus_model::LabelSet> = (0..3)
            .map(|i| {
                pulsus_model::LabelSet::from_verbatim([("instance".to_string(), format!("i{i}"))])
            })
            .collect();
        let fps: Vec<FpLiteral> = (1..=3u128)
            .map(|i| Fingerprint::from_raw(i).sql_literal())
            .collect();
        let pairs: Vec<(FpLiteral, &pulsus_model::LabelSet)> =
            fps.iter().cloned().zip(labels.iter()).collect();
        let members = members_known(&pairs);
        let list = sample_sql::render_fingerprint_list(&fps);
        let side = |series: SideSeries<'static>, name: &'static str| SideSql {
            tenant: Box::leak(Box::new(no_tenant())),
            samples_table: "metric_samples",
            labels_table: "metric_labels",
            series,
            metric_name: name,
            func: PushedRangeFn::Rate,
            range_ms: 300_000,
            scale: None,
            window: T11_WINDOW,
            level: Some(t11_level()),
        };
        let lhs = side(SideSeries::Ids { ids, gated: true }, "p_rx_total");
        let rhs = side(
            SideSeries::Known {
                list: Box::leak(list.into_boxed_str()),
                members: Box::leak(members.into_boxed_str()),
                gated: true,
            },
            "p_tx_total",
        );
        let hist_where = format!(
            "fingerprint IN ({ids}) AND unix_milli > {} AND unix_milli <= {}",
            T11_WINDOW.0, T11_WINDOW.1
        );
        let series_n = format!("(SELECT uniqExact(fingerprint) FROM ({ids})) + 3");
        let sql = join_aggregate_fetch(
            &lhs,
            &rhs,
            "metric_hist_samples",
            None,
            t11_grid(),
            RangeAggOp::Sum,
            BinOp::Add,
            false,
            &Counts {
                series_n: Some(&series_n),
                cap: 8_192,
                hist_where: Some(&hist_where),
            },
        );
        let _ = &tenant;
        let n = |text: &str, p: &str| text.matches(p).count();
        let gates = n(&sql, " AND series_n * grid_n <= 8192") + n(&sql, "WHERE series_n * grid_n <= 8192");
        let reads = n(&sql, "fingerprint IN (") + n(&sql, "fingerprint GLOBAL IN (")
            + n(&sql, "arrayJoin(CAST([")
            - n(&hist_where, "fingerprint IN (");
        assert!(reads > 0, "{sql}");
        assert_eq!(gates, reads, "every read gated:\n{sql}");
    }

    /// T11: shape C under `+`, both sides at the level, and under `> bool`
    /// with an `irate` side read raw, render the plan's text byte for byte.
    #[test]
    fn the_join_statements_render_the_plans_text() {
        let tenant = no_tenant();
        let (rx_ids, tx_ids) = (t11_ids("p_rx_total"), t11_ids("p_tx_total"));
        fn side<'a>(
            tenant: &'a Tenant,
            name: &'a str,
            ids: &'a str,
            func: PushedRangeFn,
            level: Option<LevelRead<'a>>,
        ) -> SideSql<'a> {
            SideSql {
                tenant,
                samples_table: "metric_samples",
                labels_table: "metric_labels",
                series: SideSeries::Ids { ids, gated: false },
                metric_name: name,
                func,
                range_ms: 300_000,
                scale: None,
                window: T11_WINDOW,
                level,
            }
        }
        let rx = side(
            &tenant,
            "p_rx_total",
            &rx_ids,
            PushedRangeFn::Rate,
            Some(t11_level()),
        );
        let tx = side(
            &tenant,
            "p_tx_total",
            &tx_ids,
            PushedRangeFn::Rate,
            Some(t11_level()),
        );
        let tx_irate = side(&tenant, "p_tx_total", &tx_ids, PushedRangeFn::Irate, None);
        let by_instance = grouping_of("by (instance)");
        let without_device = grouping_of("without (device)");
        assert_eq!(
            join_aggregate_fetch(
                &rx,
                &tx,
                "metric_hist_samples",
                by_instance.as_ref(),
                t11_grid(),
                RangeAggOp::Sum,
                BinOp::Add,
                false,
                &Counts::default(),
            ),
            join_section("sum by (instance) (rate + rate)")
        );
        assert_eq!(
            join_aggregate_fetch(
                &rx,
                &tx_irate,
                "metric_hist_samples",
                without_device.as_ref(),
                t11_grid(),
                RangeAggOp::Count,
                BinOp::Gt,
                true,
                &Counts::default(),
            ),
            join_section("count without (device) (rate > bool irate)")
        );
        // Issue #579 part 5, U6: the filtering comparison's arm.
        assert_eq!(
            join_aggregate_fetch(
                &rx,
                &tx_irate,
                "metric_hist_samples",
                without_device.as_ref(),
                t11_grid(),
                RangeAggOp::Count,
                BinOp::Gt,
                false,
                &Counts::default(),
            ),
            join_section("count without (device) (rate > irate)")
        );
    }

    /// **U9 (issue #579 part 5, after #18 part 2c): a match statement over
    /// two cache-answered sides reads one table per access**, so 2c's lag
    /// guard stays on it; with a side the cache did not answer it does not.
    #[test]
    fn a_known_match_statement_is_one_table() {
        use super::super::grouped::MatchSpec;
        use crate::metrics::dispatch::one_table_per_access;
        use pulsus_model::{Fingerprint, LabelSet};
        use pulsus_promql::Group;
        let labels = LabelSet::from_verbatim(
            [("instance", "i0"), ("role", "")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        let members = members_known(&[
            (Fingerprint::from_raw(101).sql_literal(), &labels),
            (Fingerprint::from_raw(205).sql_literal(), &labels),
        ]);
        let ids = t11_ids("p_tx_total");
        fn side<'a>(name: &'static str, series: SideSeries<'a>) -> SideSql<'a> {
            SideSql {
                tenant: Box::leak(Box::new(no_tenant())),
                samples_table: "metric_samples",
                labels_table: "metric_labels",
                series,
                metric_name: name,
                func: PushedRangeFn::Rate,
                range_ms: 300_000,
                scale: None,
                window: T11_WINDOW,
                level: Some(t11_level()),
            }
        }
        let known = |name: &'static str| {
            side(
                name,
                SideSeries::Known {
                    list: "'101', '205'",
                    members: &members,
                    gated: false,
                },
            )
        };
        let (rx, tx) = (known("p_rx_total"), known("p_tx_total"));
        let tx_ids = side("p_tx_total", SideSeries::Ids { ids: &ids, gated: false });
        let strs = |l: &[&str]| l.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let matches = [
            (
                "* on () group_left (role)",
                BinOp::Mul,
                MatchSpec {
                    on: true,
                    labels: Vec::new(),
                    group: Group::Left(strs(&["role"])),
                },
            ),
            (
                "> ignoring (a, b)",
                BinOp::Gt,
                MatchSpec {
                    on: false,
                    labels: strs(&["a", "b"]),
                    group: Group::OneToOne,
                },
            ),
        ];
        for (what, bin, spec) in &matches {
            for (sides, (many, one), want) in [
                ("both known", (&rx, &tx), true),
                ("one by ID", (&rx, &tx_ids), false),
            ] {
                let sql = match_aggregate_fetch(
                    many,
                    one,
                    "metric_hist_samples",
                    None,
                    t11_grid(),
                    RangeAggOp::Sum,
                    *bin,
                    false,
                    spec,
                    &Counts::default(),
                );
                assert_eq!(one_table_per_access(&sql), want, "{what}, {sides}:\n{sql}");
            }
        }
    }

    /// **U2 (issue #579 part 5): the match statement renders the plan's
    /// text**: section 6's `MATCHED` with each placeholder substituted, its
    /// sides' `SERIES_STEPS` and `MEMBERS` part 4's builders, inside part
    /// 4's aggregate with S3b's histogram condition.
    #[test]
    fn the_match_statement_renders_the_plans_text() {
        use super::super::grouped::MatchSpec;
        use pulsus_promql::Group;
        let tenant = no_tenant();
        let (rx_ids, tx_ids) = (t11_ids("p_rx_total"), t11_ids("p_tx_total"));
        let side =
            |name: &'static str, ids: &'static str, level: Option<LevelRead<'static>>| SideSql {
                tenant: Box::leak(Box::new(no_tenant())),
                samples_table: "metric_samples",
                labels_table: "metric_labels",
                series: SideSeries::Ids { ids, gated: false },
                metric_name: name,
                func: PushedRangeFn::Rate,
                range_ms: 300_000,
                scale: None,
                window: T11_WINDOW,
                level,
            };
        let rx_ids: &'static str = Box::leak(rx_ids.into_boxed_str());
        let tx_ids: &'static str = Box::leak(tx_ids.into_boxed_str());
        let rx = side("p_rx_total", rx_ids, Some(t11_level()));
        let tx = side("p_tx_total", tx_ids, Some(t11_level()));
        let (rx_raw, tx_raw) = (
            side("p_rx_total", rx_ids, None),
            side("p_tx_total", tx_ids, None),
        );
        let strs = |l: &[&str]| l.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let on = |labels: &[&str], group: Group| MatchSpec {
            on: true,
            labels: strs(labels),
            group,
        };
        let cases = [
            (
                "S3c (a) sum by (instance) (rate + on (instance) rate)",
                (&rx, &tx, &rx, &tx),
                grouping_of("by (instance)"),
                RangeAggOp::Sum,
                BinOp::Add,
                false,
                on(&["instance"], Group::OneToOne),
            ),
            (
                "S3c (b) max without (device) (rate > on (instance) group_left (role) rate)",
                (&rx, &tx, &rx, &tx),
                grouping_of("without (device)"),
                RangeAggOp::Max,
                BinOp::Gt,
                false,
                on(&["instance"], Group::Left(strs(&["role"]))),
            ),
            (
                "S3c (c) sum (rate / on (instance) group_right rate), raw",
                (&tx_raw, &rx_raw, &rx_raw, &tx_raw),
                None,
                RangeAggOp::Sum,
                BinOp::Div,
                false,
                on(&["instance"], Group::Right(Vec::new())),
            ),
            (
                "S3c (d) sum by (instance) (rate != on (instance) rate)",
                (&rx, &tx, &rx, &tx),
                grouping_of("by (instance)"),
                RangeAggOp::Sum,
                BinOp::Ne,
                false,
                on(&["instance"], Group::OneToOne),
            ),
            (
                "S3c (e) count (rate >= bool on (__name__, instance) group_left (__name__, role) rate)",
                (&rx, &tx, &rx, &tx),
                None,
                RangeAggOp::Count,
                BinOp::Ge,
                true,
                on(
                    &["__name__", "instance"],
                    Group::Left(strs(&["__name__", "role"])),
                ),
            ),
            // Review r1, finding 1: `__name__` the only include label.
            (
                "S3c (f) sum (rate * on (instance) group_left (__name__) rate)",
                (&rx, &tx, &rx, &tx),
                None,
                RangeAggOp::Sum,
                BinOp::Mul,
                false,
                on(&["instance"], Group::Left(strs(&["__name__"]))),
            ),
            (
                "S3c (g) sum (rate * on (instance) group_right (__name__) rate)",
                (&tx, &rx, &rx, &tx),
                None,
                RangeAggOp::Sum,
                BinOp::Mul,
                false,
                on(&["instance"], Group::Right(strs(&["__name__"]))),
            ),
        ];
        for (key, (many, one, lhs, rhs), grouping, op, bin, bool_modifier, spec) in &cases {
            let matched = join_section(key)
                .replace("{SERIES_STEPS(M)}", &series_steps(many, t11_grid()))
                .replace("{MEMBERS(M)}", &members(many))
                .replace("{SERIES_STEPS(O)}", &series_steps(one, t11_grid()))
                .replace("{MEMBERS(O)}", &members(one));
            let sentinel = format!(
                "org_id = {} AND (({}) OR ({}))",
                tenant.sql_literal(),
                side_histograms(lhs),
                side_histograms(rhs)
            );
            let want = aggregate_over(t11_grid(), *op, &matched, "metric_hist_samples", &sentinel);
            assert_eq!(
                match_aggregate_fetch(
                    many,
                    one,
                    "metric_hist_samples",
                    grouping.as_ref(),
                    t11_grid(),
                    *op,
                    *bin,
                    *bool_modifier,
                    spec,
                    &Counts::default(),
                ),
                want,
                "{key}"
            );
        }
    }
}
