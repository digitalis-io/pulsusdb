//! Pure fallback SQL builders — the snapshot-testing surface for issue
//! #30's `metric_series` historical/JOIN fallback (docs/schemas.md §2.1).
//! Every function here is `data -> String`: no `ChClient`, no I/O. Callers
//! ([`super::labels`]) pre-escape every user-controlled fragment before it
//! reaches these builders, via the single injection boundary this crate
//! already has ([`crate::logql::escape`]) — reused rather than duplicated,
//! per the position->primitive table below (architect plan amendment §2).
//!
//! **Escaping position -> primitive** (pinned, unit-tested in this file
//! rather than assumed from `logql`'s own coverage):
//!
//! | SQL position | Rendered as | Primitive |
//! |---|---|---|
//! | `metric_name = '<name>'` | ClickHouse string literal | [`ch_string`] |
//! | label key in `JSONExtractString(labels, '<key>')` | string literal (a *value argument*, never an identifier) | [`ch_string`] |
//! | Eq/Neq value `... = '<val>'` / `!= '<val>'` | string literal | [`ch_string`] |
//! | Re/Nre pattern `match(JSONExtractString(labels,'<key>'), '<pat>')` | fully-anchored `'^(?:pat)$'` literal | [`ch_regex_anchored`] |
//!
//! Absent-label semantics are unchanged from the in-process path:
//! `JSONExtractString` returns `''` for a missing key, matching
//! [`super::labels`]'s `""` rule for a missing [`pulsus_model::LabelSet`]
//! entry (load-bearing for the cache-vs-SQL differential test).
//!
//! **Placeholder-doubling is NOT this module's concern.** `ch_regex_anchored`
//! always emits a literal `?` (the `^(?:...)$` template) — the `clickhouse`
//! crate's `SqlBuilder` treats a bare `?` as an unbound bind placeholder
//! unless doubled. The canonical SQL text this module returns is
//! deliberately un-doubled (snapshot-testable, matches `logql::sql`'s own
//! contract); the doubling is applied once, at the execution boundary
//! (`logql::exec::escape_query_placeholders`'s pattern) — issue #31's
//! engine must apply it before this text reaches `ChClient::query_stream`,
//! exactly as `logql::exec` already does for its own regex SQL.

use crate::logql::escape::{ch_regex_anchored, ch_string};
use pulsus_model::floor_to_activity_bucket;

use super::matcher::{DataWindow, LabelMatcher, MatchOp};

/// `intDiv({ms}, {bucket_ms}) * {bucket_ms}` — the literal bound
/// docs/schemas.md §2.1 renders, computed via the shared
/// [`floor_to_activity_bucket`] (not re-derived here) so the rendered
/// number is byte-identical to what the writer's own registration gate
/// computes (issue #26 precedent; cross-crate pinned by
/// `tests/metrics_bucket_floor.rs`).
fn floored_bound(ms: i64, bucket_ms: i64) -> i64 {
    floor_to_activity_bucket(ms, bucket_ms)
}

/// Renders one matcher as a `JSONExtractString(labels, '<key>')` predicate.
/// The key is always a string literal (see this module's escaping table) —
/// never `ch_ident`, which is reserved for trusted schema identifiers.
fn matcher_predicate(m: &LabelMatcher) -> String {
    let target = format!("JSONExtractString(labels, {})", ch_string(&m.key));
    match m.op {
        MatchOp::Eq => format!("{target} = {}", ch_string(&m.value)),
        MatchOp::Neq => format!("{target} != {}", ch_string(&m.value)),
        MatchOp::Re => format!("match({target}, {})", ch_regex_anchored(&m.value)),
        MatchOp::Nre => format!("NOT match({target}, {})", ch_regex_anchored(&m.value)),
    }
}

fn base_where(series_table: &str, metric_name: &str, window: DataWindow, bucket_ms: i64) -> String {
    let lower = floored_bound(window.start_ms, bucket_ms);
    let upper = floored_bound(window.end_ms, bucket_ms);
    format!(
        "FROM {series_table}\nWHERE metric_name = {}\n  AND unix_milli >= {lower} AND unix_milli <= {upper}",
        ch_string(metric_name)
    )
}

fn append_matchers(sql: &mut String, matchers: &[LabelMatcher]) {
    for m in matchers {
        sql.push_str("\n  AND ");
        sql.push_str(&matcher_predicate(m));
    }
}

/// The injection-safe `metric_series` sub-query issue #31 inlines verbatim
/// as `fingerprint IN ( <this> )` against `metric_samples`, for **every**
/// fallback (task-manager resolution #4 on issue #30: a uniform inline
/// sub-query, one round trip, no materialized-list special case). No
/// `ORDER BY`/`LIMIT 1 BY`: the caller only needs a *set* of fingerprints,
/// and `IN (...)` already ignores duplicates — docs/schemas.md §2.3's
/// fallback shape.
pub fn historical_series_subquery(
    series_table: &str,
    metric_name: &str,
    window: DataWindow,
    bucket_ms: i64,
    matchers: &[LabelMatcher],
) -> String {
    let mut sql = format!(
        "SELECT fingerprint\n{}",
        base_where(series_table, metric_name, window, bucket_ms)
    );
    append_matchers(&mut sql, matchers);
    sql
}

/// The standalone, deduplicated `fingerprint, labels` form — docs/schemas.md
/// §2.1's `LIMIT 1 BY metric_name, fingerprint` lookup SQL. Used by the
/// live differential test (a materialized comparison set against the
/// in-process resolution) and by any caller wanting the historical labels
/// themselves rather than an `IN (...)` sub-query.
pub fn historical_resolution_query(
    series_table: &str,
    metric_name: &str,
    window: DataWindow,
    bucket_ms: i64,
    matchers: &[LabelMatcher],
) -> String {
    let mut sql = format!(
        "SELECT fingerprint, labels\n{}",
        base_where(series_table, metric_name, window, bucket_ms)
    );
    append_matchers(&mut sql, matchers);
    sql.push_str("\nORDER BY unix_milli DESC\nLIMIT 1 BY metric_name, fingerprint");
    sql
}

/// Issue #31's label-hydration query: `fingerprint -> labels` for an
/// already-**resolved, concrete** fingerprint list (mirrors `logql::exec`'s
/// stage-2 hydration precedent). Used only on the `SqlFallback` sample-
/// fetch path ([`super::sample_sql::sample_fetch_subquery`]) — the sample
/// fetch's own nested `fingerprint IN ( <subquery> )` already narrows to
/// exactly the fingerprints that returned samples in-window, so this
/// hydrates *only those*, not the fallback's full (possibly much larger)
/// matcher-matched set. No `window`/`matchers`/bucket-floor predicates
/// here — the fingerprint list is already the answer; this is a pure
/// `fingerprint -> labels` lookup, filtered only by `metric_name` (the
/// schema's metric-scoping invariant) and the explicit `IN (...)` list.
pub fn series_labels_by_fingerprint(series_table: &str, metric_name: &str, fps: &[u64]) -> String {
    let fp_list = fps
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT fingerprint, labels\nFROM {series_table}\nWHERE metric_name = {}\n  AND fingerprint IN ({fp_list})\nORDER BY unix_milli DESC\nLIMIT 1 BY metric_name, fingerprint",
        ch_string(metric_name)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window() -> DataWindow {
        DataWindow {
            start_ms: 1_000,
            end_ms: 3_600_001,
        }
    }

    fn eq(key: &str, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op: MatchOp::Eq,
            value: value.to_string(),
        }
    }

    #[test]
    fn historical_series_subquery_renders_bucket_floored_bounds() {
        let sql = historical_series_subquery(
            "metric_series",
            "http_requests_total",
            window(),
            3_600_000,
            &[],
        );
        assert!(sql.contains("metric_name = 'http_requests_total'"));
        assert!(sql.contains("unix_milli >= 0 AND unix_milli <= 3600000"));
        assert!(sql.starts_with("SELECT fingerprint\nFROM metric_series"));
    }

    #[test]
    fn historical_series_subquery_has_no_order_by_or_limit_1_by() {
        let sql = historical_series_subquery(
            "metric_series",
            "up",
            window(),
            3_600_000,
            &[eq("job", "api")],
        );
        assert!(!sql.contains("ORDER BY"));
        assert!(!sql.contains("LIMIT"));
    }

    #[test]
    fn historical_resolution_query_dedups_with_limit_1_by() {
        let sql = historical_resolution_query("metric_series", "up", window(), 3_600_000, &[]);
        assert!(sql.starts_with("SELECT fingerprint, labels\nFROM metric_series"));
        assert!(sql.ends_with("ORDER BY unix_milli DESC\nLIMIT 1 BY metric_name, fingerprint"));
    }

    #[test]
    fn series_labels_by_fingerprint_renders_an_explicit_fingerprint_list() {
        let sql = series_labels_by_fingerprint("metric_series", "up", &[101, 205, 990]);
        assert_eq!(
            sql,
            "SELECT fingerprint, labels\nFROM metric_series\nWHERE metric_name = 'up'\n  AND fingerprint IN (101, 205, 990)\nORDER BY unix_milli DESC\nLIMIT 1 BY metric_name, fingerprint"
        );
    }

    #[test]
    fn series_labels_by_fingerprint_has_no_window_or_matcher_predicates() {
        let sql = series_labels_by_fingerprint("metric_series", "up", &[1]);
        assert!(!sql.contains("unix_milli >="));
        assert!(!sql.contains("JSONExtractString"));
    }

    #[test]
    fn eq_matcher_renders_json_extract_equality() {
        let sql = historical_series_subquery(
            "metric_series",
            "up",
            window(),
            3_600_000,
            &[eq("job", "api")],
        );
        assert!(sql.contains("JSONExtractString(labels, 'job') = 'api'"));
    }

    #[test]
    fn neq_matcher_renders_json_extract_inequality() {
        let m = LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Neq,
            value: "api".to_string(),
        };
        let sql = historical_series_subquery("metric_series", "up", window(), 3_600_000, &[m]);
        assert!(sql.contains("JSONExtractString(labels, 'job') != 'api'"));
    }

    #[test]
    fn re_matcher_renders_anchored_match() {
        let m = LabelMatcher {
            key: "status".to_string(),
            op: MatchOp::Re,
            value: "5..".to_string(),
        };
        let sql = historical_series_subquery("metric_series", "up", window(), 3_600_000, &[m]);
        assert!(sql.contains("match(JSONExtractString(labels, 'status'), '^(?:5..)$')"));
    }

    #[test]
    fn nre_matcher_renders_negated_anchored_match() {
        let m = LabelMatcher {
            key: "status".to_string(),
            op: MatchOp::Nre,
            value: "5..".to_string(),
        };
        let sql = historical_series_subquery("metric_series", "up", window(), 3_600_000, &[m]);
        assert!(sql.contains("NOT match(JSONExtractString(labels, 'status'), '^(?:5..)$')"));
    }

    #[test]
    fn multiple_matchers_are_all_anded_together() {
        let sql = historical_series_subquery(
            "metric_series",
            "http_requests_total",
            window(),
            3_600_000,
            &[eq("job", "api"), eq("env", "prod")],
        );
        assert!(sql.contains("JSONExtractString(labels, 'job') = 'api'"));
        assert!(sql.contains("JSONExtractString(labels, 'env') = 'prod'"));
        assert_eq!(sql.matches("JSONExtractString(labels,").count(), 2);
    }

    #[test]
    fn floored_bound_matches_the_shared_model_definition() {
        assert_eq!(floored_bound(3_600_001, 3_600_000), 3_600_000);
        assert_eq!(
            floored_bound(3_600_001, 3_600_000),
            floor_to_activity_bucket(3_600_001, 3_600_000)
        );
    }

    // --- injection tests (architect plan amendment §3) ---

    /// Label-KEY injection: a key containing `'`, `\`, and control chars
    /// renders as a single closed `ch_string` literal inside
    /// `JSONExtractString(labels, '...')` — no bare quote escapes the
    /// argument.
    #[test]
    fn label_key_injection_stays_inside_one_literal() {
        let payload = "job'; DROP TABLE metric_series; --\n\t\0";
        let m = eq(payload, "api");
        let sql = historical_series_subquery("metric_series", "up", window(), 3_600_000, &[m]);
        assert!(sql.contains(&format!(
            "JSONExtractString(labels, {})",
            ch_string(payload)
        )));
        assert_no_unescaped_quote(&ch_string(payload));
    }

    /// Regex-literal injection: an `=~` pattern carrying `'`, `\`, and a
    /// metacharacter renders inside one anchored `'^(?:...)$'` literal, and
    /// the whole literal survives the `?`-doubling round trip (only `?`
    /// characters are doubled; the escaped payload contains none extra).
    #[test]
    fn regex_literal_injection_stays_inside_one_anchored_literal() {
        let payload = r#"a'.*)$OR(1=1\b"#;
        let m = LabelMatcher {
            key: "status".to_string(),
            op: MatchOp::Re,
            value: payload.to_string(),
        };
        let sql = historical_series_subquery("metric_series", "up", window(), 3_600_000, &[m]);
        let expected = ch_regex_anchored(payload);
        assert_no_unescaped_quote(&expected);
        assert!(sql.contains(&format!(
            "match(JSONExtractString(labels, 'status'), {expected})"
        )));
        // The `^(?:...)$` template always carries a literal `?` — doubling
        // it must still round-trip to the same count of literal `?`s once
        // unbound (`??` -> `?`), i.e. the doubled form has exactly twice as
        // many `?` characters as the original.
        let doubled = expected.replace('?', "??");
        assert_eq!(
            doubled.matches('?').count(),
            2 * expected.matches('?').count()
        );
    }

    /// `metric_name` and Eq/Neq value injection: same single-literal
    /// neutralization.
    #[test]
    fn metric_name_and_value_injection_stay_inside_one_literal_each() {
        let name_payload = "up'; DROP TABLE metric_series; --";
        let value_payload = "api' OR '1'='1";
        let m = eq("job", value_payload);
        let sql =
            historical_series_subquery("metric_series", name_payload, window(), 3_600_000, &[m]);
        assert!(sql.contains(&format!("metric_name = {}", ch_string(name_payload))));
        assert!(sql.contains(&format!(
            "JSONExtractString(labels, 'job') = {}",
            ch_string(value_payload)
        )));
        assert_no_unescaped_quote(&ch_string(name_payload));
        assert_no_unescaped_quote(&ch_string(value_payload));
    }

    fn assert_no_unescaped_quote(literal: &str) {
        assert!(literal.starts_with('\'') && literal.ends_with('\''));
        let inner = &literal[1..literal.len() - 1];
        let mut chars = inner.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' {
                chars.next();
                continue;
            }
            assert_ne!(c, '\'', "bare unescaped quote in {literal:?}");
        }
    }
}
