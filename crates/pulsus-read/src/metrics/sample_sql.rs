//! Pure fetch SQL builders for the issue #31 sample fetch — the §2.3 fetch
//! shape. Every function here is `data -> String`: no `ChClient`, no I/O,
//! snapshot-testable without a database (mirrors [`super::sql`]'s own
//! contract for the label-cache fallback subquery).
//!
//! **Left-open right-closed window, always:** `unix_milli > lower_excl_ms
//! AND unix_milli <= upper_incl_ms` — edge case 1 (AC), asserted directly
//! in this module's tests and again end-to-end in
//! `tests/query_log_gates.rs`… no, in the SQL-plan snapshot tests
//! (`tests/metrics_sql_snapshots.rs`).
//!
//! **`metric_name` is the only string literal here** ([`ch_string`]);
//! fingerprints enter as [`FpLiteral`]s and render as
//! `toUInt128('<decimal>')` (no escaping surface — they come from
//! [`super::labels::Resolution::Fingerprints`] or a resolved
//! `SqlFallback`, never from unescaped user text). No function in this
//! module takes a `Fingerprint`: the mint is upstream, which is what makes
//! "every fingerprint in this module's SQL is exact" a property of the
//! signatures rather than of the bodies (issue #498). The `SqlFallback`
//! variant ([`sample_fetch_subquery`]) inlines #30's already-injection-safe
//! sub-query verbatim as `fingerprint IN ( <subquery> )` — the sub-query's
//! own escaping (including the `?`→`??` placeholder-doubling contract for
//! its `match(...)` regex predicates) is [`super::sql`]'s concern, not
//! re-applied here; issue #31's `MetricsEngine` applies the doubling once,
//! at the execution boundary, exactly as `logql::exec` does for its own
//! regex SQL.

use pulsus_model::FpLiteral;

use crate::logql::escape::ch_string;

// ---------------------------------------------------------------------
// The predicate fragments (issue #548)
// ---------------------------------------------------------------------
//
// One producer per fragment, used by the statement builders below AND by
// `super::compile::selector_pred`, which expresses the same read in the
// compile core's predicate lattice. Two producers for one predicate is
// how a plan comes to describe a statement the engine does not send.
//
// **No fragment carries statement layout.** Each renders one clause's
// text and nothing else: the newlines, the `PREWHERE`/`WHERE` keywords
// and the two-space continuation are the builders' business, which is
// what keeps the rendered statements byte-identical to the ones this
// module rendered before the fragments existed (issue #548 criterion 1).

/// `metric_name = 'x'` — the concrete-name `PREWHERE` term.
pub fn name_predicate(metric_name: &str) -> String {
    format!("metric_name = {}", ch_string(metric_name))
}

/// `metric_name IN ('a', 'b')` — the fan-out `PREWHERE` term.
pub fn names_predicate(metric_names: &[String]) -> String {
    let name_list = metric_names
        .iter()
        .map(|n| ch_string(n))
        .collect::<Vec<_>>()
        .join(", ");
    format!("metric_name IN ({name_list})")
}

/// `unix_milli > A AND unix_milli <= B` — left-open right-closed, always.
pub fn window_predicate(lower_excl_ms: i64, upper_incl_ms: i64) -> String {
    format!("unix_milli > {lower_excl_ms} AND unix_milli <= {upper_incl_ms}")
}

/// `fingerprint IN (toUInt128('101'), toUInt128('205'))` — the resolved
/// fingerprint set.
pub fn fingerprints_predicate(fps: &[FpLiteral]) -> String {
    format!("fingerprint IN ({})", render_fingerprint_list(fps))
}

/// `WITH [toUInt128('101'), toUInt128('205')] AS fps` — a resolved
/// fingerprint list, named once for both branches of a fetch over the two
/// sample tables, which test it as `fingerprint IN fps` ([`union_fetch`]).
/// The list is [`fingerprints_predicate`]'s, so the explain surface's leaf
/// and the statement name the same set.
pub fn fingerprints_with(fps: &[FpLiteral]) -> String {
    format!("WITH [{}] AS fps\n", render_fingerprint_list(fps))
}

/// `fingerprint IN (\n<subquery>\n  )` — the degraded-cache path's
/// inlined series selection, layout included because the sub-query is
/// rendered on its own lines.
pub fn subquery_predicate(subquery: &str) -> String {
    format!("fingerprint IN (\n{subquery}\n  )")
}

/// The type of the one column a histogram row's 13 value columns travel in:
/// an array of one tuple, order-locked to `metric_hist_samples`' `CREATE`
/// and [`super::sample_rows::HistColumnsTuple`]. A float row carries the
/// empty array.
const HIST_TYPE: &str = "Array(Tuple(Int8, Float64, UInt64, UInt64, Float64, Array(Int32), \
     Array(UInt32), Array(Int64), Array(Int32), Array(UInt32), Array(Int64), Array(Float64), \
     UInt8))";

/// A histogram row's 13 value columns, as the one tuple [`HIST_TYPE`] holds.
const HIST_TUPLE: &str = "tuple(schema, zero_threshold, zero_count, count, sum, \
     pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
     neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values, \
     counter_reset_hint)";

/// **One statement over both sample tables** (issue #623): the float rows
/// and the histogram rows under one selection, `UNION ALL`, in one order.
/// It replaces a float fetch and a second, complementary histogram fetch
/// that every selector sent and that almost always returned nothing — the
/// grouped read (`super::grouped_sql`) already reads both tables this way.
///
/// Each branch carries the same `PREWHERE` and `WHERE`, so each prunes on
/// its own table's primary key as the two statements did. The caller splits
/// the rows back into the two streams the merge has always taken, so a key
/// present in both tables is answered as before.
///
/// **The histogram columns travel as one column, `hist`, placed before
/// `value`**: an empty array on a float row, one tuple on a histogram row.
/// Measured over 144,000 float samples on 26.3.29.7, compressed as the
/// client receives them: the separate float read 1,414,713 bytes; this form
/// 1,412,651; the 13 columns written out flat, empty on a float row,
/// 1,642,805. Where the extra byte sits matters too: after `value` it cost
/// 1,553,278.
///
/// **A resolved fingerprint list is written once**, as `WITH [...] AS fps`,
/// and each branch tests `fingerprint IN fps` — the grouped read's form.
/// Written into both branches it would double the statement's AST, and a
/// fan-out under the server's `max_ast_elements` of 50,000 would cross it
/// (measured: `TOO_BIG_AST` on 26.3.29.7). The set built from the alias
/// prunes the primary key exactly as the literal list does (measured: the
/// same granules selected, `EXPLAIN indexes = 1`).
fn union_fetch(
    lead: &str,
    samples_table: &str,
    hist_table: &str,
    with: &str,
    prewhere: &str,
    selection: &str,
    order: &str,
) -> String {
    format!(
        "{with}SELECT {lead}unix_milli, hist, value\n\
         FROM (\n\
         \x20 SELECT {lead}unix_milli, CAST([], '{HIST_TYPE}') AS hist, value\n\
         \x20 FROM {samples_table}\n\
         \x20 PREWHERE {prewhere}\n\
         \x20 WHERE {selection}\n\
         \x20 UNION ALL\n\
         \x20 SELECT {lead}unix_milli, CAST([{HIST_TUPLE}], '{HIST_TYPE}') AS hist, \
         CAST(0, 'Float64') AS value\n\
         \x20 FROM {hist_table}\n\
         \x20 PREWHERE {prewhere}\n\
         \x20 WHERE {selection}\n\
         )\n\
         ORDER BY {order}"
    )
}

/// The §2.3 fast-path fetch: an explicit, sorted fingerprint list
/// ([`fingerprints_with`]), over both sample tables ([`union_fetch`]). Callers pre-sort/dedup
/// `fps` (the resolver's own contract); this function renders whatever
/// order it is given, unmodified — snapshot stability is the caller's
/// responsibility, not re-derived here.
pub fn sample_fetch(
    table: &str,
    hist_table: &str,
    metric_name: &str,
    fps: &[FpLiteral],
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let name = name_predicate(metric_name);
    let window = window_predicate(lower_excl_ms, upper_incl_ms);
    union_fetch(
        "fingerprint, ",
        table,
        hist_table,
        &fingerprints_with(fps),
        &name,
        &format!("{window}\n    AND fingerprint IN fps"),
        "fingerprint, unix_milli",
    )
}

/// The over-cap / `SqlFallback` variant: `subquery` (#30's
/// `historical_series_subquery` output, already injection-safe) inlined
/// verbatim as `fingerprint IN ( <subquery> )` in each branch — never a
/// materialized giant `IN` list (edge case 6, AC).
pub fn sample_fetch_subquery(
    table: &str,
    hist_table: &str,
    metric_name: &str,
    subquery: &str,
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let name = name_predicate(metric_name);
    let window = window_predicate(lower_excl_ms, upper_incl_ms);
    let sub = subquery_predicate(subquery);
    union_fetch(
        "fingerprint, ",
        table,
        hist_table,
        "",
        &name,
        &format!("{window}\n    AND {sub}"),
        "fingerprint, unix_milli",
    )
}

/// The issue #85 (M6-08c) multi-metric fan-out fetch: ONE flat query for
/// a name-less/regex-`__name__` selector's whole resolved set —
/// `PREWHERE metric_name IN (<matched names>)` (leading-primary-key
/// granule pruning, the EXPLAIN-gated compound prune's first component)
/// plus `fingerprint IN fps` over the matched list (the second PK
/// component, [`fingerprints_with`]),
/// never a global unfiltered sample scan. Sound without per-pair
/// filtering (plan v3 Δ2, reviewer-verified): label matchers exclude
/// `__name__` and apply uniformly across metrics, so any
/// `(metric_name, fingerprint)` cross-pair naming a real series has
/// matcher-passing labels by construction — the IN×IN cannot over-match.
/// `metric_name` joins the projection so rows group into per-
/// `(metric_name, fingerprint)` series
/// ([`super::sample_rows::MultiUnionSampleRow`]).
pub fn sample_fetch_multi(
    table: &str,
    hist_table: &str,
    metric_names: &[String],
    fps: &[FpLiteral],
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let names = names_predicate(metric_names);
    let window = window_predicate(lower_excl_ms, upper_incl_ms);
    union_fetch(
        "metric_name, fingerprint, ",
        table,
        hist_table,
        &fingerprints_with(fps),
        &names,
        &format!("{window}\n    AND fingerprint IN fps"),
        "metric_name, fingerprint, unix_milli",
    )
}

/// STUB (issue #623, tests first): `main`'s histogram read.
pub fn hist_sample_fetch(
    _table: &str,
    _metric_name: &str,
    _fps: &[FpLiteral],
    _lower_excl_ms: i64,
    _upper_incl_ms: i64,
) -> String {
    String::new()
}

/// The comma-separated `toUInt128('<decimal>')` list an `IN (...)`
/// carries. `pub` because `super::compile`'s `handoff_cost` bound is
/// asserted against what this renders (issue #548 criterion 7), and a
/// bound asserted against a second renderer would bound the wrong text.
///
/// The call form is not optional above `2^64`: ClickHouse reads a bare
/// decimal literal there as `Float64`, exact only to `2^53`, so
/// `fingerprint IN (<bare list>)` prunes every granule and returns nothing
/// (issue #498, measured on ClickHouse 26.3.29.7).
pub fn render_fingerprint_list(fps: &[FpLiteral]) -> String {
    fps.iter()
        .map(FpLiteral::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The fingerprint-chunking threshold (architect plan, edge case 7):
/// fingerprint sets at or above this size split into parallel chunk
/// fetches.
pub const CHUNK_THRESHOLD: usize = 500;

/// Splits `fps` into chunks of at most [`CHUNK_THRESHOLD`] fingerprints,
/// preserving order (never dropping or reordering — edge case 7: chunk-
/// completion order must not affect the evaluator's own fingerprint-order
/// invariant, which the caller re-establishes by merging chunk results
/// back into `SeriesData` keyed by fingerprint, not by chunk-arrival
/// order). A non-empty set smaller than the threshold yields exactly one
/// chunk; an empty set yields zero chunks (`[FpLiteral]::chunks`'s own
/// contract).
///
/// Chunks minted literals rather than fingerprints: this module's
/// signatures may not mention `Fingerprint` (issue #498 criterion 4b), so
/// callers mint before chunking. The cost is one `Vec<FpLiteral>` per
/// query, 16 bytes per resolved fingerprint — 800 KB at the
/// `PULSUS_CACHE_MAX_SERIES` default of 50,000 and 800 MB at the accepted
/// ceiling of 50,000,000 (`crates/pulsus-config/src/validate.rs`,
/// `CACHE_MAX_SERIES_CEILING`).
pub fn chunk_fingerprints(fps: &[FpLiteral], chunk_size: usize) -> Vec<&[FpLiteral]> {
    if chunk_size == 0 {
        return vec![fps];
    }
    fps.chunks(chunk_size).collect()
}

#[cfg(test)]
mod tests {
    use pulsus_model::Fingerprint;

    use super::*;

    /// A minted literal from a small decimal, so these tests keep reading
    /// as `&[fp(101), fp(205)]` after the identity widened (issue #498).
    fn fp(v: u128) -> FpLiteral {
        Fingerprint::from_raw(v).sql_literal()
    }

    /// The four values where a bare decimal and the exact call form first
    /// disagree: `2^64-1` (the last value a bare literal reads exactly),
    /// `2^64` (the first `Float64`, and what `2^64+1` rounds onto),
    /// `2^64+1` (the value a bare literal loses) and its neighbour.
    const BOUNDARY: [u128; 4] = [
        18_446_744_073_709_551_615,
        18_446_744_073_709_551_616,
        18_446_744_073_709_551_617,
        18_446_744_073_709_551_618,
    ];

    /// The exact call form at the four values where a bare decimal and
    /// `toUInt128('<decimal>')` first disagree (issue #498).
    #[test]
    fn render_fingerprint_list_renders_the_exact_call_form_at_the_2_64_boundary() {
        let fps: Vec<FpLiteral> = BOUNDARY.iter().copied().map(fp).collect();
        assert_eq!(
            render_fingerprint_list(&fps),
            "toUInt128('18446744073709551615'), toUInt128('18446744073709551616'), \
             toUInt128('18446744073709551617'), toUInt128('18446744073709551618')"
        );
    }

    #[test]
    fn sample_fetch_window_is_left_open_right_closed() {
        let sql = sample_fetch(
            "metric_samples",
            "metric_hist_samples",
            "up",
            &[Fingerprint::from_raw(1).sql_literal()],
            0,
            100,
        );
        assert!(sql.contains("unix_milli > 0 AND unix_milli <= 100"));
        // Never `>=` on the lower bound — that would include the excluded
        // edge sample (AC: left-open right-closed window boundaries).
        assert!(!sql.contains("unix_milli >= 0"));
    }

    #[test]
    fn sample_fetch_of_an_empty_fingerprint_list_renders_an_empty_list() {
        let sql = sample_fetch("metric_samples", "metric_hist_samples", "up", &[], 0, 100);
        assert!(sql.starts_with("WITH [] AS fps\n"), "{sql}");
    }

    #[test]
    fn sample_fetch_subquery_inlines_the_subquery_verbatim() {
        let subquery = "SELECT fingerprint FROM metric_series WHERE metric_name = 'up'";
        let sql = sample_fetch_subquery(
            "metric_samples",
            "metric_hist_samples",
            "up",
            subquery,
            0,
            100,
        );
        assert!(sql.contains(&format!("fingerprint IN (\n{subquery}\n  )")));
        assert!(!sql.contains("IN (SELECT fingerprint FROM metric_series"));
    }

    #[test]
    fn sample_fetch_subquery_never_materializes_a_giant_in_list() {
        let subquery = "SELECT fingerprint FROM metric_series WHERE metric_name = 'up'";
        let sql = sample_fetch_subquery(
            "metric_samples",
            "metric_hist_samples",
            "up",
            subquery,
            0,
            100,
        );
        // No comma-separated numeric literal list anywhere in this SQL.
        assert!(!sql.contains("IN (1,"));
    }

    #[test]
    fn metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_samples; --";
        let sql = sample_fetch(
            "metric_samples",
            "metric_hist_samples",
            payload,
            &[Fingerprint::from_raw(1).sql_literal()],
            0,
            100,
        );
        assert!(sql.contains(&format!("metric_name = {}", ch_string(payload))));
    }

    #[test]
    fn chunk_fingerprints_splits_at_the_threshold() {
        let fps: Vec<FpLiteral> = (0..1_200).map(fp).collect();
        let chunks = chunk_fingerprints(&fps, 500);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 500);
        assert_eq!(chunks[1].len(), 500);
        assert_eq!(chunks[2].len(), 200);
    }

    #[test]
    fn chunk_fingerprints_preserves_order() {
        let fps: Vec<FpLiteral> = (0..1_000).map(fp).collect();
        let chunks = chunk_fingerprints(&fps, 500);
        let flattened: Vec<FpLiteral> = chunks.into_iter().flatten().copied().collect();
        assert_eq!(flattened, fps);
    }

    #[test]
    fn chunk_fingerprints_of_a_set_under_the_threshold_is_one_chunk() {
        let fps: Vec<FpLiteral> = (0..10).map(fp).collect();
        let chunks = chunk_fingerprints(&fps, 500);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 10);
    }

    #[test]
    fn chunk_fingerprints_of_an_empty_set_is_zero_chunks() {
        let chunks = chunk_fingerprints(&[], 500);
        assert!(chunks.is_empty());
    }

    #[test]
    fn chunk_threshold_matches_the_documented_500_cap() {
        assert_eq!(CHUNK_THRESHOLD, 500);
    }

    // --- sample_fetch_multi (issue #85, M6-08c) ---

    #[test]
    fn sample_fetch_multi_window_is_left_open_right_closed() {
        let sql = sample_fetch_multi(
            "metric_samples",
            "metric_hist_samples",
            &["up".to_string()],
            &[Fingerprint::from_raw(1).sql_literal()],
            0,
            100,
        );
        assert!(sql.contains("unix_milli > 0 AND unix_milli <= 100"));
        assert!(!sql.contains("unix_milli >= 0"));
    }

    #[test]
    fn sample_fetch_multi_metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_samples; --".to_string();
        let sql = sample_fetch_multi(
            "metric_samples",
            "metric_hist_samples",
            std::slice::from_ref(&payload),
            &[Fingerprint::from_raw(1).sql_literal()],
            0,
            100,
        );
        assert!(sql.contains(&format!("metric_name IN ({})", ch_string(&payload))));
    }

    // -- issue #623: one statement reads both sample tables ------------

    /// **The concrete-name fetch is one statement over both tables.** A
    /// float row carries an empty `hist`; a histogram row carries its
    /// columns as the one element of `hist` and a zero `value`. The order
    /// is the one both fetches had.
    #[test]
    fn sample_fetch_reads_both_tables_in_one_statement() {
        let sql = sample_fetch(
            "metric_samples",
            "metric_hist_samples",
            "http_requests_total",
            &[fp(101), fp(205)],
            1_000,
            2_000,
        );
        let hist_type = "Array(Tuple(Int8, Float64, UInt64, UInt64, Float64, Array(Int32), \
                         Array(UInt32), Array(Int64), Array(Int32), Array(UInt32), \
                         Array(Int64), Array(Float64), UInt8))";
        assert_eq!(
            sql,
            format!(
                "WITH [toUInt128('101'), toUInt128('205')] AS fps\n\
                 SELECT fingerprint, unix_milli, hist, value\n\
                 FROM (\n\
                 \x20 SELECT fingerprint, unix_milli, CAST([], '{hist_type}') AS hist, value\n\
                 \x20 FROM metric_samples\n\
                 \x20 PREWHERE metric_name = 'http_requests_total'\n\
                 \x20 WHERE unix_milli > 1000 AND unix_milli <= 2000\n\
                 \x20   AND fingerprint IN fps\n\
                 \x20 UNION ALL\n\
                 \x20 SELECT fingerprint, unix_milli, CAST([tuple(schema, zero_threshold, \
                 zero_count, count, sum, pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
                 neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values, \
                 counter_reset_hint)], '{hist_type}') AS hist, CAST(0, 'Float64') AS value\n\
                 \x20 FROM metric_hist_samples\n\
                 \x20 PREWHERE metric_name = 'http_requests_total'\n\
                 \x20 WHERE unix_milli > 1000 AND unix_milli <= 2000\n\
                 \x20   AND fingerprint IN fps\n\
                 )\n\
                 ORDER BY fingerprint, unix_milli"
            )
        );
    }

    /// The fallback and fan-out fetches take the same union: both tables,
    /// one statement, each branch carrying the same selection.
    #[test]
    fn the_fallback_and_fan_out_fetches_read_both_tables_in_one_statement() {
        let subquery = "SELECT fingerprint FROM metric_series WHERE metric_name = 'up'";
        let fallback = sample_fetch_subquery(
            "metric_samples",
            "metric_hist_samples",
            "up",
            subquery,
            0,
            100,
        );
        let multi = sample_fetch_multi(
            "metric_samples",
            "metric_hist_samples",
            &["a".to_string(), "b".to_string()],
            &[fp(1)],
            0,
            100,
        );
        for (sql, selection, order) in [
            (&fallback, subquery, "\nORDER BY fingerprint, unix_milli"),
            (
                &multi,
                "fingerprint IN fps",
                "\nORDER BY metric_name, fingerprint, unix_milli",
            ),
        ] {
            assert_eq!(sql.matches("UNION ALL").count(), 1, "{sql}");
            assert_eq!(sql.matches("FROM metric_samples\n").count(), 1, "{sql}");
            assert_eq!(
                sql.matches("FROM metric_hist_samples\n").count(),
                1,
                "{sql}"
            );
            assert_eq!(
                sql.matches(selection).count(),
                2,
                "both branches select: {sql}"
            );
            assert!(sql.ends_with(order), "{sql}");
        }
        assert!(
            multi.starts_with(
                "WITH [toUInt128('1')] AS fps\nSELECT metric_name, fingerprint, unix_milli, hist, value\n"
            ),
            "{multi}"
        );
        assert_eq!(
            multi.matches("toUInt128('1')").count(),
            1,
            "the list is written once"
        );
    }

    /// T2 (issue #623): the histogram read is `main`'s own statement again,
    /// sent beside the float read rather than folded into one union.
    #[test]
    fn hist_sample_fetch_renders_the_12_column_shape() {
        let sql = hist_sample_fetch(
            "metric_hist_samples",
            "http_request_duration_seconds",
            &[
                Fingerprint::from_raw(101).sql_literal(),
                Fingerprint::from_raw(205).sql_literal(),
                Fingerprint::from_raw(990).sql_literal(),
            ],
            1_000,
            2_000,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, unix_milli, schema, zero_threshold, zero_count, count, sum, \
             pos_span_offsets, pos_span_lengths, pos_bucket_deltas, neg_span_offsets, \
             neg_span_lengths, neg_bucket_deltas, custom_values, counter_reset_hint\n\
             FROM metric_hist_samples\n\
             PREWHERE metric_name = 'http_request_duration_seconds'\n\
             WHERE unix_milli > 1000 AND unix_milli <= 2000\n\
             \x20 AND fingerprint IN (toUInt128('101'), toUInt128('205'), toUInt128('990'))\n\
             ORDER BY fingerprint, unix_milli"
        );
    }
}
