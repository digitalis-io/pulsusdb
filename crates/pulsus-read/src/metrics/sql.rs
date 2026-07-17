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

use super::matcher::{DataWindow, DiscoveryFilter, LabelMatcher, MatchOp};

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

/// Issue #32's discovery query: `fingerprint, metric_name, labels` for
/// **all** series matching `filter`, bucket-floored to `window` — used by
/// `MetricsEngine::{label_names,label_values,series}`, which apply their
/// **own** window filtering here rather than trusting the label cache's
/// wider (whole-`PULSUS_CACHE_WINDOW`) resident-superset fast path (#30
/// handoff AC: the cache's bucket-granularity superset must not leak into a
/// discovery response for a narrower request window). `filter.metric_name
/// == None` renders no `metric_name` predicate at all — "every metric",
/// Prometheus's own `/labels`/`/label/{name}/values` semantics when
/// `match[]` is omitted (docs/api.md §3.3). Selects `metric_name` (unlike
/// [`historical_resolution_query`]) because a metric-name-less filter's
/// caller does not otherwise know which metric each returned row belongs
/// to — needed to populate `__name__` per row.
pub fn discovery_query(
    series_table: &str,
    filter: &DiscoveryFilter,
    window: DataWindow,
    bucket_ms: i64,
) -> String {
    let lower = floored_bound(window.start_ms, bucket_ms);
    let upper = floored_bound(window.end_ms, bucket_ms);
    let mut sql = format!("SELECT fingerprint, metric_name, labels\nFROM {series_table}\n");
    match &filter.metric_name {
        Some(name) => {
            sql.push_str(&format!(
                "WHERE metric_name = {}\n  AND unix_milli >= {lower} AND unix_milli <= {upper}",
                ch_string(name)
            ));
        }
        None => {
            sql.push_str(&format!(
                "WHERE unix_milli >= {lower} AND unix_milli <= {upper}"
            ));
        }
    }
    append_matchers(&mut sql, &filter.matchers);
    sql.push_str("\nORDER BY unix_milli DESC\nLIMIT 1 BY metric_name, fingerprint");
    sql
}

/// Issue #89's discovery analog of [`super::sample_sql::sample_fetch_multi`]:
/// ONE flat query for a regex/negated-`__name__` `match[]` selector's whole
/// resolved candidate set — `metric_name IN (<resolved names>)` (the leading
/// primary-key component of `metric_series ORDER BY (metric_name,
/// fingerprint, unix_milli)`) plus `fingerprint IN (<resolved fps>)` (the
/// second component), both EXPLAIN-gated in `explain_indexes.rs`. The
/// request window is re-applied here with the same bucket-floored bounds as
/// [`discovery_query`], so the label cache's wider resident superset (the
/// resolution source for the IN sets) never leaks into a narrower discovery
/// response — the `discovery_series` invariant.
///
/// Sound without per-pair filtering, by `sample_fetch_multi`'s argument: a
/// `metric_name` is in the IN set only if it passed the selector's
/// `name_matchers`, and `metric_fingerprint` excludes `__name__`
/// (docs/schemas.md §2.1) so a fingerprint's label set is name-invariant —
/// every `(metric_name, fingerprint)` cross-pair naming a real series is a
/// genuine match. `LIMIT 1 BY metric_name, fingerprint` dedups to one row
/// per series, as in [`discovery_query`].
pub fn discovery_fetch_multi(
    series_table: &str,
    metric_names: &[String],
    fps: &[u64],
    window: DataWindow,
    bucket_ms: i64,
) -> String {
    let lower = floored_bound(window.start_ms, bucket_ms);
    let upper = floored_bound(window.end_ms, bucket_ms);
    let name_list = metric_names
        .iter()
        .map(|n| ch_string(n))
        .collect::<Vec<_>>()
        .join(", ");
    let fp_list = fps
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT fingerprint, metric_name, labels\nFROM {series_table}\nWHERE metric_name IN ({name_list})\n  AND fingerprint IN ({fp_list})\n  AND unix_milli >= {lower} AND unix_milli <= {upper}\nORDER BY unix_milli DESC\nLIMIT 1 BY metric_name, fingerprint"
    )
}

/// `GET /api/v1/metadata` (issue #32): `metric_metadata` is a
/// `ReplacingMergeTree(updated_ns)` (docs/schemas.md §2.1) whose merges are
/// asynchronous, so a plain `SELECT` can observe more than one row per
/// `metric_name` — `argMax(_, updated_ns)` deterministically collapses to
/// the latest-written value per column without waiting for a merge,
/// grouped by the base family name (schemas.md §2.1's writer contract: a
/// derived series' suffix is never stripped here — callers must already be
/// querying by the base name). `metric` is an optional exact-name filter,
/// `limit` an optional row cap.
pub fn metadata_query(metadata_table: &str, metric: Option<&str>, limit: Option<usize>) -> String {
    let mut sql = format!(
        "SELECT metric_name, argMax(metric_type, updated_ns) AS metric_type, argMax(help, updated_ns) AS help, argMax(unit, updated_ns) AS unit\nFROM {metadata_table}"
    );
    if let Some(name) = metric {
        sql.push_str(&format!("\nWHERE metric_name = {}", ch_string(name)));
    }
    sql.push_str("\nGROUP BY metric_name\nORDER BY metric_name");
    if let Some(n) = limit {
        sql.push_str(&format!("\nLIMIT {n}"));
    }
    sql
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

    // --- discovery_query (issue #32) ---

    #[test]
    fn discovery_query_with_a_metric_name_filters_on_it() {
        let filter = DiscoveryFilter {
            metric_name: Some("up".to_string()),
            name_matchers: vec![],
            matchers: vec![eq("job", "api")],
        };
        let sql = discovery_query("metric_series", &filter, window(), 3_600_000);
        assert!(sql.contains("metric_name = 'up'"));
        assert!(sql.contains("JSONExtractString(labels, 'job') = 'api'"));
        assert!(sql.starts_with("SELECT fingerprint, metric_name, labels\nFROM metric_series"));
        assert!(sql.ends_with("ORDER BY unix_milli DESC\nLIMIT 1 BY metric_name, fingerprint"));
    }

    #[test]
    fn discovery_query_without_a_metric_name_has_no_metric_name_predicate() {
        let filter = DiscoveryFilter::default();
        let sql = discovery_query("metric_series", &filter, window(), 3_600_000);
        assert!(!sql.contains("metric_name ="));
        assert!(sql.contains("unix_milli >= 0 AND unix_milli <= 3600000"));
    }

    #[test]
    fn discovery_query_without_a_metric_name_still_applies_matchers() {
        let filter = DiscoveryFilter {
            metric_name: None,
            name_matchers: vec![],
            matchers: vec![eq("job", "api")],
        };
        let sql = discovery_query("metric_series", &filter, window(), 3_600_000);
        assert!(sql.contains("JSONExtractString(labels, 'job') = 'api'"));
    }

    #[test]
    fn discovery_query_metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_series; --";
        let filter = DiscoveryFilter {
            metric_name: Some(payload.to_string()),
            name_matchers: vec![],
            matchers: vec![],
        };
        let sql = discovery_query("metric_series", &filter, window(), 3_600_000);
        assert!(sql.contains(&format!("metric_name = {}", ch_string(payload))));
        assert_no_unescaped_quote(&ch_string(payload));
    }

    // --- discovery_fetch_multi (issue #89) ---

    #[test]
    fn discovery_fetch_multi_renders_the_flat_in_by_in_shape() {
        let sql = discovery_fetch_multi(
            "metric_series",
            &["up".to_string(), "up_alias".to_string()],
            &[101, 205],
            window(),
            3_600_000,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, metric_name, labels\n\
             FROM metric_series\n\
             WHERE metric_name IN ('up', 'up_alias')\n\
             \x20 AND fingerprint IN (101, 205)\n\
             \x20 AND unix_milli >= 0 AND unix_milli <= 3600000\n\
             ORDER BY unix_milli DESC\n\
             LIMIT 1 BY metric_name, fingerprint"
        );
    }

    #[test]
    fn discovery_fetch_multi_floors_both_window_bounds_to_the_bucket() {
        let sql = discovery_fetch_multi(
            "metric_series",
            &["up".to_string()],
            &[7],
            DataWindow {
                start_ms: 3_600_001,
                end_ms: 7_300_000,
            },
            3_600_000,
        );
        assert!(sql.contains("unix_milli >= 3600000 AND unix_milli <= 7200000"));
    }

    /// The window is re-applied in SQL (not inherited from the label
    /// cache's wider resident superset) — the `discovery_series`
    /// no-residency-leak invariant.
    #[test]
    fn discovery_fetch_multi_always_constrains_the_request_window() {
        let sql = discovery_fetch_multi("metric_series", &["up".to_string()], &[1], window(), 1);
        assert!(sql.contains("AND unix_milli >= "));
        assert!(sql.contains(" AND unix_milli <= "));
    }

    #[test]
    fn discovery_fetch_multi_metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_series; --";
        let sql = discovery_fetch_multi(
            "metric_series",
            &[payload.to_string()],
            &[1],
            window(),
            3_600_000,
        );
        assert!(sql.contains(&format!("metric_name IN ({})", ch_string(payload))));
        assert_no_unescaped_quote(&ch_string(payload));
    }

    // --- metadata_query (issue #32) ---

    #[test]
    fn metadata_query_with_no_filter_or_limit_selects_every_row() {
        let sql = metadata_query("metric_metadata", None, None);
        assert!(sql.starts_with("SELECT metric_name, argMax(metric_type, updated_ns)"));
        assert!(!sql.contains("WHERE"));
        assert!(!sql.contains("LIMIT"));
        assert!(sql.ends_with("GROUP BY metric_name\nORDER BY metric_name"));
    }

    #[test]
    fn metadata_query_filters_on_the_given_metric_name() {
        let sql = metadata_query("metric_metadata", Some("up"), None);
        assert!(sql.contains("WHERE metric_name = 'up'"));
    }

    #[test]
    fn metadata_query_applies_the_given_limit() {
        let sql = metadata_query("metric_metadata", None, Some(10));
        assert!(sql.ends_with("LIMIT 10"));
    }

    #[test]
    fn metadata_query_metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_metadata; --";
        let sql = metadata_query("metric_metadata", Some(payload), None);
        assert!(sql.contains(&format!("WHERE metric_name = {}", ch_string(payload))));
        assert_no_unescaped_quote(&ch_string(payload));
    }
}
