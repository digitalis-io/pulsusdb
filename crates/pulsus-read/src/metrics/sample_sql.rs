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
//! fingerprints are `u64` numeric literals (no escaping surface — they
//! come from [`super::labels::Resolution::Fingerprints`] or a resolved
//! `SqlFallback`, never from unescaped user text). The `SqlFallback`
//! variant ([`sample_fetch_subquery`]) inlines #30's already-injection-safe
//! sub-query verbatim as `fingerprint IN ( <subquery> )` — the sub-query's
//! own escaping (including the `?`→`??` placeholder-doubling contract for
//! its `match(...)` regex predicates) is [`super::sql`]'s concern, not
//! re-applied here; issue #31's `MetricsEngine` applies the doubling once,
//! at the execution boundary, exactly as `logql::exec` does for its own
//! regex SQL.

use crate::logql::escape::ch_string;

/// The §2.3 fast-path fetch: an explicit, sorted `fingerprint IN (...)`
/// list. Callers pre-sort/dedup `fps` (the resolver's own contract); this
/// function renders whatever order it is given, unmodified — snapshot
/// stability is the caller's responsibility, not re-derived here.
pub fn sample_fetch(
    table: &str,
    metric_name: &str,
    fps: &[u64],
    lower_excl_ms: i64,
    upper_incl_ms: i64,
) -> String {
    let fp_list = render_fingerprint_list(fps);
    format!(
        "SELECT fingerprint, unix_milli, value\nFROM {table}\nPREWHERE metric_name = {}\nWHERE unix_milli > {lower_excl_ms} AND unix_milli <= {upper_incl_ms}\n  AND fingerprint IN ({fp_list})\nORDER BY fingerprint, unix_milli",
        ch_string(metric_name)
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
    format!(
        "SELECT fingerprint, unix_milli, value\nFROM {table}\nPREWHERE metric_name = {}\nWHERE unix_milli > {lower_excl_ms} AND unix_milli <= {upper_incl_ms}\n  AND fingerprint IN (\n{subquery}\n  )\nORDER BY fingerprint, unix_milli",
        ch_string(metric_name)
    )
}

fn render_fingerprint_list(fps: &[u64]) -> String {
    fps.iter()
        .map(u64::to_string)
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
/// chunk; an empty set yields zero chunks (`[u64]::chunks`'s own
/// contract).
pub fn chunk_fingerprints(fps: &[u64], chunk_size: usize) -> Vec<&[u64]> {
    if chunk_size == 0 {
        return vec![fps];
    }
    fps.chunks(chunk_size).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_fetch_renders_the_schemas_md_2_3_shape() {
        let sql = sample_fetch(
            "metric_samples",
            "http_requests_total",
            &[101, 205, 990],
            1_000,
            2_000,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, unix_milli, value\n\
             FROM metric_samples\n\
             PREWHERE metric_name = 'http_requests_total'\n\
             WHERE unix_milli > 1000 AND unix_milli <= 2000\n\
             \x20 AND fingerprint IN (101, 205, 990)\n\
             ORDER BY fingerprint, unix_milli"
        );
    }

    #[test]
    fn sample_fetch_window_is_left_open_right_closed() {
        let sql = sample_fetch("metric_samples", "up", &[1], 0, 100);
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
        let sql = sample_fetch("metric_samples", payload, &[1], 0, 100);
        assert!(sql.contains(&format!("metric_name = {}", ch_string(payload))));
    }

    #[test]
    fn chunk_fingerprints_splits_at_the_threshold() {
        let fps: Vec<u64> = (0..1_200).collect();
        let chunks = chunk_fingerprints(&fps, 500);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 500);
        assert_eq!(chunks[1].len(), 500);
        assert_eq!(chunks[2].len(), 200);
    }

    #[test]
    fn chunk_fingerprints_preserves_order() {
        let fps: Vec<u64> = (0..1_000).collect();
        let chunks = chunk_fingerprints(&fps, 500);
        let flattened: Vec<u64> = chunks.into_iter().flatten().copied().collect();
        assert_eq!(flattened, fps);
    }

    #[test]
    fn chunk_fingerprints_of_a_set_under_the_threshold_is_one_chunk() {
        let fps: Vec<u64> = (0..10).collect();
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
}
