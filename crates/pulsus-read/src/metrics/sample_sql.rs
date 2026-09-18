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

/// `fingerprint IN (\n<subquery>\n  )` — the degraded-cache path's
/// inlined series selection, layout included because the sub-query is
/// rendered on its own lines.
pub fn subquery_predicate(subquery: &str) -> String {
    format!("fingerprint IN (\n{subquery}\n  )")
}

/// The §2.3 fast-path fetch: an explicit, sorted `fingerprint IN (...)`
/// list. Callers pre-sort/dedup `fps` (the resolver's own contract); this
/// function renders whatever order it is given, unmodified — snapshot
/// stability is the caller's responsibility, not re-derived here.
pub fn sample_fetch(
    table: &str,
    metric_name: &str,
    fps: &[FpLiteral],
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let name = name_predicate(metric_name);
    let window = window_predicate(lower_excl_ms, upper_incl_ms);
    let fps = fingerprints_predicate(fps);
    format!(
        "SELECT fingerprint, unix_milli, value\nFROM {table}\nPREWHERE {name}\nWHERE {window}\n  AND {fps}\nORDER BY fingerprint, unix_milli"
    )
}

/// The over-cap / `SqlFallback` variant: `subquery` (#30's
/// `historical_series_subquery` output, already injection-safe) inlined
/// verbatim as `fingerprint IN ( <subquery> )` — never a materialized
/// giant `IN` list (edge case 6, AC).
pub fn sample_fetch_subquery(
    table: &str,
    metric_name: &str,
    subquery: &str,
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let name = name_predicate(metric_name);
    let window = window_predicate(lower_excl_ms, upper_incl_ms);
    let sub = subquery_predicate(subquery);
    format!(
        "SELECT fingerprint, unix_milli, value\nFROM {table}\nPREWHERE {name}\nWHERE {window}\n  AND {sub}\nORDER BY fingerprint, unix_milli"
    )
}

/// The issue #85 (M6-08c) multi-metric fan-out fetch: ONE flat query for
/// a name-less/regex-`__name__` selector's whole resolved set —
/// `PREWHERE metric_name IN (<matched names>)` (leading-primary-key
/// granule pruning, the EXPLAIN-gated compound prune's first component)
/// plus `fingerprint IN (<matched fps>)` (the second PK component),
/// never a global unfiltered sample scan. Sound without per-pair
/// filtering (plan v3 Δ2, reviewer-verified): label matchers exclude
/// `__name__` and apply uniformly across metrics, so any
/// `(metric_name, fingerprint)` cross-pair naming a real series has
/// matcher-passing labels by construction — the IN×IN cannot over-match.
/// `metric_name` joins the projection so rows group into per-
/// `(metric_name, fingerprint)` series ([`super::sample_rows::MultiSampleRow`]).
pub fn sample_fetch_multi(
    table: &str,
    metric_names: &[String],
    fps: &[FpLiteral],
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let names = names_predicate(metric_names);
    let window = window_predicate(lower_excl_ms, upper_incl_ms);
    let fps = fingerprints_predicate(fps);
    format!(
        "SELECT metric_name, fingerprint, unix_milli, value\nFROM {table}\nPREWHERE {names}\nWHERE {window}\n  AND {fps}\nORDER BY metric_name, fingerprint, unix_milli"
    )
}

/// The 13 `metric_hist_samples` value columns, order-locked to the catalog
/// `CREATE` (id-23, plus the id-27 additive `counter_reset_hint` — LAST,
/// issue #125) and [`super::sample_rows::HistSampleRow`]. Appended
/// after the identity columns in every histogram fetch's SELECT list; the
/// **only** difference from the float builders is this column list and the
/// table name (M7-A5a AC1/AC5 — the PREWHERE/window/IN/ORDER-BY shape is
/// byte-for-byte the float shape). The extra fixed-width `UInt8` column
/// changes no PREWHERE/ORDER-BY shape and adds no per-row bucket work.
const HIST_VALUE_COLUMNS: &str = "schema, zero_threshold, zero_count, count, sum, \
     pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
     neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values, \
     counter_reset_hint";

/// The histogram half of [`sample_fetch`]: the complementary
/// `metric_hist_samples` read for the same `(metric_name, fingerprint set,
/// window)`. Byte-for-byte [`sample_fetch`]'s PREWHERE/window/`IN`/ORDER-BY
/// shape — only the SELECT column list and table name differ (M7-A5a).
pub fn hist_sample_fetch(
    table: &str,
    metric_name: &str,
    fps: &[FpLiteral],
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let name = name_predicate(metric_name);
    let window = window_predicate(lower_excl_ms, upper_incl_ms);
    let fps = fingerprints_predicate(fps);
    format!(
        "SELECT fingerprint, unix_milli, {HIST_VALUE_COLUMNS}\nFROM {table}\nPREWHERE {name}\nWHERE {window}\n  AND {fps}\nORDER BY fingerprint, unix_milli"
    )
}

/// The histogram half of [`sample_fetch_subquery`] — the `SqlFallback`
/// path's complementary read. Byte-for-byte its shape (the injection-safe
/// sub-query inlined verbatim as `fingerprint IN ( <subquery> )`), only the
/// SELECT column list and table name differ (M7-A5a).
pub fn hist_sample_fetch_subquery(
    table: &str,
    metric_name: &str,
    subquery: &str,
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let name = name_predicate(metric_name);
    let window = window_predicate(lower_excl_ms, upper_incl_ms);
    let sub = subquery_predicate(subquery);
    format!(
        "SELECT fingerprint, unix_milli, {HIST_VALUE_COLUMNS}\nFROM {table}\nPREWHERE {name}\nWHERE {window}\n  AND {sub}\nORDER BY fingerprint, unix_milli"
    )
}

/// The histogram half of [`sample_fetch_multi`] — the name-less/regex-
/// `__name__` fan-out's complementary read. Byte-for-byte its flat
/// `PREWHERE metric_name IN (…) … fingerprint IN (…)` shape, only the
/// SELECT column list (leading `metric_name`, then the 13 value columns)
/// and table name differ (M7-A5a).
pub fn hist_sample_fetch_multi(
    table: &str,
    metric_names: &[String],
    fps: &[FpLiteral],
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let names = names_predicate(metric_names);
    let window = window_predicate(lower_excl_ms, upper_incl_ms);
    let fps = fingerprints_predicate(fps);
    format!(
        "SELECT metric_name, fingerprint, unix_milli, {HIST_VALUE_COLUMNS}\nFROM {table}\nPREWHERE {names}\nWHERE {window}\n  AND {fps}\nORDER BY metric_name, fingerprint, unix_milli"
    )
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
    fn sample_fetch_renders_the_schemas_md_2_3_shape() {
        let sql = sample_fetch(
            "metric_samples",
            "http_requests_total",
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
            "SELECT fingerprint, unix_milli, value\n\
             FROM metric_samples\n\
             PREWHERE metric_name = 'http_requests_total'\n\
             WHERE unix_milli > 1000 AND unix_milli <= 2000\n\
             \x20 AND fingerprint IN (toUInt128('101'), toUInt128('205'), toUInt128('990'))\n\
             ORDER BY fingerprint, unix_milli"
        );
    }

    #[test]
    fn sample_fetch_window_is_left_open_right_closed() {
        let sql = sample_fetch(
            "metric_samples",
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
    fn sample_fetch_of_an_empty_fingerprint_list_renders_empty_parens() {
        let sql = sample_fetch("metric_samples", "up", &[], 0, 100);
        assert!(sql.contains("fingerprint IN ()"));
    }

    #[test]
    fn sample_fetch_subquery_inlines_the_subquery_verbatim() {
        let subquery = "SELECT fingerprint FROM metric_series WHERE metric_name = 'up'";
        let sql = sample_fetch_subquery("metric_samples", "up", subquery, 0, 100);
        assert!(sql.contains(&format!("fingerprint IN (\n{subquery}\n  )")));
        assert!(!sql.contains("IN (SELECT fingerprint FROM metric_series"));
    }

    #[test]
    fn sample_fetch_subquery_never_materializes_a_giant_in_list() {
        let subquery = "SELECT fingerprint FROM metric_series WHERE metric_name = 'up'";
        let sql = sample_fetch_subquery("metric_samples", "up", subquery, 0, 100);
        // No comma-separated numeric literal list anywhere in this SQL.
        assert!(!sql.contains("IN (1,"));
    }

    #[test]
    fn metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_samples; --";
        let sql = sample_fetch(
            "metric_samples",
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
    fn sample_fetch_multi_renders_the_flat_in_in_shape() {
        let sql = sample_fetch_multi(
            "metric_samples",
            &["foo_total".to_string(), "bar_total".to_string()],
            &[
                Fingerprint::from_raw(101).sql_literal(),
                Fingerprint::from_raw(205).sql_literal(),
            ],
            1_000,
            2_000,
        );
        assert_eq!(
            sql,
            "SELECT metric_name, fingerprint, unix_milli, value\n\
             FROM metric_samples\n\
             PREWHERE metric_name IN ('foo_total', 'bar_total')\n\
             WHERE unix_milli > 1000 AND unix_milli <= 2000\n\
             \x20 AND fingerprint IN (toUInt128('101'), toUInt128('205'))\n\
             ORDER BY metric_name, fingerprint, unix_milli"
        );
    }

    #[test]
    fn sample_fetch_multi_window_is_left_open_right_closed() {
        let sql = sample_fetch_multi(
            "metric_samples",
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
            std::slice::from_ref(&payload),
            &[Fingerprint::from_raw(1).sql_literal()],
            0,
            100,
        );
        assert!(sql.contains(&format!("metric_name IN ({})", ch_string(&payload))));
    }

    /// The concrete-name fetch SQL is byte-unchanged by #85 — the flat
    /// IN-set shape is a *new* builder alongside it, never a rewrite of
    /// the single-metric fast path (the EXPLAIN-gated PK prune).
    #[test]
    fn sample_fetch_single_name_shape_is_untouched_by_the_multi_builder() {
        let sql = sample_fetch(
            "metric_samples",
            "up",
            &[
                Fingerprint::from_raw(1).sql_literal(),
                Fingerprint::from_raw(2).sql_literal(),
            ],
            0,
            100,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, unix_milli, value\n\
             FROM metric_samples\n\
             PREWHERE metric_name = 'up'\n\
             WHERE unix_milli > 0 AND unix_milli <= 100\n\
             \x20 AND fingerprint IN (toUInt128('1'), toUInt128('2'))\n\
             ORDER BY fingerprint, unix_milli"
        );
    }

    // --- M7-A5a: histogram fetch builders (dual-read complementary half) ---

    const HIST_COLS: &str = "schema, zero_threshold, zero_count, count, sum, \
         pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
         neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values, \
         counter_reset_hint";

    /// AC1: the Chunks-path histogram builder renders the float builder's
    /// exact PREWHERE/window/`IN`/ORDER-BY shape with the 13 histogram
    /// value columns and the `metric_hist_samples` table.
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
            format!(
                "SELECT fingerprint, unix_milli, {HIST_COLS}\n\
                 FROM metric_hist_samples\n\
                 PREWHERE metric_name = 'http_request_duration_seconds'\n\
                 WHERE unix_milli > 1000 AND unix_milli <= 2000\n\
                 \x20 AND fingerprint IN (toUInt128('101'), toUInt128('205'), toUInt128('990'))\n\
                 ORDER BY fingerprint, unix_milli"
            )
        );
    }

    #[test]
    fn hist_sample_fetch_window_is_left_open_right_closed() {
        let sql = hist_sample_fetch(
            "metric_hist_samples",
            "up",
            &[Fingerprint::from_raw(1).sql_literal()],
            0,
            100,
        );
        assert!(sql.contains("unix_milli > 0 AND unix_milli <= 100"));
        assert!(!sql.contains("unix_milli >= 0"));
    }

    #[test]
    fn hist_sample_fetch_subquery_inlines_the_subquery_verbatim() {
        let subquery = "SELECT fingerprint FROM metric_series WHERE metric_name = 'up'";
        let sql = hist_sample_fetch_subquery("metric_hist_samples", "up", subquery, 0, 100);
        assert!(sql.contains(&format!("fingerprint IN (\n{subquery}\n  )")));
        assert!(sql.starts_with(&format!("SELECT fingerprint, unix_milli, {HIST_COLS}")));
    }

    #[test]
    fn hist_sample_fetch_multi_renders_the_flat_in_in_shape() {
        let sql = hist_sample_fetch_multi(
            "metric_hist_samples",
            &["foo_seconds".to_string(), "bar_seconds".to_string()],
            &[
                Fingerprint::from_raw(101).sql_literal(),
                Fingerprint::from_raw(205).sql_literal(),
            ],
            1_000,
            2_000,
        );
        assert_eq!(
            sql,
            format!(
                "SELECT metric_name, fingerprint, unix_milli, {HIST_COLS}\n\
                 FROM metric_hist_samples\n\
                 PREWHERE metric_name IN ('foo_seconds', 'bar_seconds')\n\
                 WHERE unix_milli > 1000 AND unix_milli <= 2000\n\
                 \x20 AND fingerprint IN (toUInt128('101'), toUInt128('205'))\n\
                 ORDER BY metric_name, fingerprint, unix_milli"
            )
        );
    }

    /// Extracts the PREWHERE+WHERE+ORDER-BY tail — everything after the
    /// `FROM <table>` line — so a float/hist pair can be compared for
    /// predicate/window overlap independent of the SELECT list and table.
    fn predicate_tail(sql: &str) -> &str {
        let from = sql.find("\nFROM ").expect("has FROM");
        let after_table = sql[from + 1..].find('\n').expect("table line") + from + 1;
        &sql[after_table..]
    }

    /// AC7a (Chunks): the float read and its complementary histogram read
    /// share the identical PREWHERE metric-name, window literal, fingerprint
    /// predicate, and ORDER BY — only the SELECT list and table differ.
    #[test]
    fn ac7a_chunks_float_and_hist_predicates_are_identical() {
        let float = sample_fetch(
            "metric_samples",
            "up",
            &[
                Fingerprint::from_raw(7).sql_literal(),
                Fingerprint::from_raw(9).sql_literal(),
            ],
            1_000,
            2_000,
        );
        let hist = hist_sample_fetch(
            "metric_hist_samples",
            "up",
            &[
                Fingerprint::from_raw(7).sql_literal(),
                Fingerprint::from_raw(9).sql_literal(),
            ],
            1_000,
            2_000,
        );
        assert_eq!(predicate_tail(&float), predicate_tail(&hist));
        assert!(predicate_tail(&float).contains("unix_milli > 1000 AND unix_milli <= 2000"));
        assert!(predicate_tail(&float).contains("fingerprint IN (toUInt128('7'), toUInt128('9'))"));
        assert!(predicate_tail(&float).contains("PREWHERE metric_name = 'up'"));
    }

    /// AC7a (Fallback): identical inlined `fingerprint IN ( <subquery> )`,
    /// window and metric-name predicate.
    #[test]
    fn ac7a_fallback_float_and_hist_predicates_are_identical() {
        let subquery = "SELECT fingerprint FROM metric_series WHERE metric_name = 'up'";
        let float = sample_fetch_subquery("metric_samples", "up", subquery, 1_000, 2_000);
        let hist = hist_sample_fetch_subquery("metric_hist_samples", "up", subquery, 1_000, 2_000);
        assert_eq!(predicate_tail(&float), predicate_tail(&hist));
    }

    /// AC7a (Multi): identical `metric_name IN (…)`, window and
    /// `fingerprint IN (…)`.
    #[test]
    fn ac7a_multi_float_and_hist_predicates_are_identical() {
        let names = vec!["a_seconds".to_string(), "b_seconds".to_string()];
        let float = sample_fetch_multi(
            "metric_samples",
            &names,
            &[
                Fingerprint::from_raw(7).sql_literal(),
                Fingerprint::from_raw(9).sql_literal(),
            ],
            1_000,
            2_000,
        );
        let hist = hist_sample_fetch_multi(
            "metric_hist_samples",
            &names,
            &[
                Fingerprint::from_raw(7).sql_literal(),
                Fingerprint::from_raw(9).sql_literal(),
            ],
            1_000,
            2_000,
        );
        assert_eq!(predicate_tail(&float), predicate_tail(&hist));
    }
}
