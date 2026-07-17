//! Pure per-stage SQL string builders — the snapshot-testing surface
//! (`tests/sql_snapshots.rs`). Every function here is `AST-derived data →
//! String`: no `ChClient`, no I/O, no randomness. Callers (mainly
//! [`super::plan`]) are responsible for pre-escaping every user-controlled
//! fragment via [`super::escape`] before it reaches these builders — that
//! is the injection boundary, not this module.

use super::params::Direction;

/// A half-open-below/closed-above nanosecond time bound (`ts > start AND ts
/// <= end`, docs/schemas.md §3.2), grouped into one parameter so the stage
/// 3/metric builders below stay under clippy's argument-count lint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Which physical table a metric read targets, and that table's
/// bucket/aggregate column shape — the rollup-vs-raw routing decision
/// [`super::plan::metric_plan`] makes, grouped into one parameter (same
/// clippy argument-count reason as [`TimeWindow`]). Rollup-served reads
/// `log_metrics_<res>` with `bucket_ns`/`sum(count)`|`sum(bytes)`; the raw
/// fallback reads `log_samples` with `timestamp_ns`/`count()`|`sum(length(body))`.
#[derive(Debug, Clone, Copy)]
pub struct MetricSource<'a> {
    pub table: &'a str,
    pub bucket_col: &'a str,
    pub agg_expr: &'a str,
}

/// Stage 1 — single-pass stream resolution over `log_streams_idx`
/// (docs/schemas.md §3.2). `months` are pre-rendered `'YYYY-MM-01'` date
/// literals (at least one); `positive_branches`/`negative_branches` are
/// pre-rendered, already-parenthesized `(key = '...' AND ...)` OR-branches
/// (see [`super::plan::normalize_matchers`]).
///
/// **Pure-positive selectors collapse byte-for-byte to docs/schemas.md
/// §3.2's canonical `HAVING uniqExact(key, val) = n` form** (architect plan
/// amendment §1) — the `negative_branches.is_empty()` branch below is
/// load-bearing for that byte-exact requirement; changing its shape breaks
/// the snapshot contract.
pub fn stage1(
    streams_idx_table: &str,
    months: &[String],
    positive_branches: &[String],
    negative_branches: &[String],
) -> String {
    let month_clause = month_clause(months);

    let mut where_branches: Vec<&str> = positive_branches.iter().map(String::as_str).collect();
    where_branches.extend(negative_branches.iter().map(String::as_str));
    let where_or_list = where_branches.join(" OR ");

    let having = if negative_branches.is_empty() {
        format!("uniqExact(key, val) = {}", positive_branches.len())
    } else {
        let pos_or = positive_branches.join(" OR ");
        let neg_or = negative_branches.join(" OR ");
        format!(
            "uniqExactIf((key, val), {pos_or}) = {}\n   AND countIf({neg_or}) = 0",
            positive_branches.len()
        )
    };

    format!(
        "SELECT fingerprint\nFROM {streams_idx_table}\nWHERE {month_clause}\n  AND ({where_or_list})\nGROUP BY fingerprint\nHAVING {having}"
    )
}

/// A `count()` selectivity probe over one matcher key's index prefix
/// (docs/schemas.md §3.2: "the planner orders matchers by selectivity
/// (cheap `count()` probes on index prefixes)"). Only computed when the
/// selector contains at least one regex matcher — pure-equality selectors
/// are point ranges and skip probes entirely (architect plan: "Selectivity
/// probes").
pub fn probe(streams_idx_table: &str, months: &[String], key_literal: &str) -> String {
    let month_clause = month_clause(months);
    format!(
        "SELECT count() AS n\nFROM {streams_idx_table}\nWHERE {month_clause} AND key = {key_literal}"
    )
}

/// Labels discovery (#13 `GET|POST /api/logs/v1/labels`): every distinct
/// `log_streams_idx` key within `months`, ascending. Budget-capped like
/// every other index scan in this module (`LogQlEngine::budget_settings`).
pub fn label_names(streams_idx_table: &str, months: &[String]) -> String {
    format!(
        "SELECT DISTINCT key AS name\nFROM {streams_idx_table}\nWHERE {}\nORDER BY name",
        month_clause(months)
    )
}

/// Label-values discovery (#13 `GET /api/logs/v1/label/{{name}}/values`):
/// every distinct value of one key within `months`, ascending.
/// `key_literal` is a pre-escaped ClickHouse string literal (see
/// [`super::escape::ch_string`]). **M1 scope:** returns the key's full
/// distinct-value set; `query=`-selector narrowing is deferred to M6
/// parity (docs/api.md §2.3).
pub fn label_values(streams_idx_table: &str, months: &[String], key_literal: &str) -> String {
    format!(
        "SELECT DISTINCT val AS value\nFROM {streams_idx_table}\nWHERE {} AND key = {key_literal}\nORDER BY value",
        month_clause(months)
    )
}

/// The `month = '...'` / `month IN (...)` clause shared by every stage-1-
/// style `log_streams_idx` scan in this module (`months` is at least one
/// pre-rendered `'YYYY-MM-01'` date literal).
fn month_clause(months: &[String]) -> String {
    if months.len() == 1 {
        format!("month = {}", months[0])
    } else {
        format!("month IN ({})", months.join(", "))
    }
}

/// Stage 2 — hydration (docs/schemas.md §3.2 line 307), byte-exact to the
/// canonical shape: `SELECT fingerprint, service, labels FROM log_streams
/// WHERE fingerprint IN (...)`.
pub fn stage2(streams_table: &str, fingerprints: &[u64]) -> String {
    let fp_list = fp_list(fingerprints);
    format!(
        "SELECT fingerprint, service, labels FROM {streams_table} WHERE fingerprint IN ({fp_list})"
    )
}

/// Stage 3 — samples, primary-index + skip-index served (docs/schemas.md
/// §3.2). `services` are pre-escaped string literals; `line_filters` are
/// pre-rendered predicate fragments (see
/// [`super::plan::compile_line_filter`]), one per pipeline `LineFilter`
/// stage, ANDed together.
///
/// **Singleton/`IN` split (architect plan amendment §2, review finding 2):**
/// exactly one service renders the byte-exact §3.2 form `PREWHERE service =
/// 'checkout'`; more than one renders `PREWHERE service IN (...)`.
pub fn stage3(
    samples_table: &str,
    services: &[String],
    fingerprints: &[u64],
    window: TimeWindow,
    line_filters: &[String],
    direction: Direction,
    limit: u32,
) -> String {
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let order = match direction {
        Direction::Backward => "DESC",
        Direction::Forward => "ASC",
    };
    let TimeWindow { start_ns, end_ns } = window;

    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause);
    }
    sql.push_str(&format!("\nORDER BY timestamp_ns {order}\nLIMIT {limit}"));
    sql
}

/// A range metric query bucketed by `step_ns` (`intDiv(bucket_col, step) *
/// step`, docs/schemas.md §3.2). `extra_predicates` carries line-filter
/// pushdown for the (line-filter-forced) raw fallback.
///
/// **`PREWHERE service ...` on the raw fallback only (fix-plan amendment
/// §3, code review finding "Raw metric fallback loses the `log_samples`
/// primary-key prefix"):** when `source.table` is `log_samples`, omitting a
/// service predicate drops the leading column of `ORDER BY (service,
/// fingerprint, timestamp_ns)` — docs/schemas.md §3.2 line 285 mandates
/// injecting it "even a query that never mentions `service`" to keep the
/// primary index engaged, exactly as stage 3 already does. Pass `services =
/// &[]` for the rollup path (`log_metrics_<res>` has no `service` column,
/// `ORDER BY (fingerprint, bucket_ns)`); a non-empty `services` renders the
/// same singleton/`IN` split [`stage3`] uses.
pub fn metric_range(
    source: MetricSource<'_>,
    services: &[String],
    fingerprints: &[u64],
    window: TimeWindow,
    step_ns: u64,
    extra_predicates: &[String],
) -> String {
    let MetricSource {
        table,
        bucket_col,
        agg_expr,
    } = source;
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let prewhere = metric_prewhere(services);
    let mut sql = format!(
        "SELECT fingerprint, intDiv({bucket_col}, {step_ns}) * {step_ns} AS step, {agg_expr} AS n\nFROM {table}\n{prewhere}WHERE fingerprint IN ({fp_list}) AND {bucket_col} > {start_ns} AND {bucket_col} <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str(" AND ");
        sql.push_str(clause);
    }
    sql.push_str("\nGROUP BY fingerprint, step");
    sql
}

/// An instant metric query — a single window, no bucketing
/// ([`super::params::QuerySpec::Instant`]'s structural contract: no
/// `intDiv` expression, no `step` column). See [`metric_range`]'s doc
/// comment for the `services`/`PREWHERE` contract (fix-plan amendment §3).
pub fn metric_instant(
    source: MetricSource<'_>,
    services: &[String],
    fingerprints: &[u64],
    window: TimeWindow,
    extra_predicates: &[String],
) -> String {
    let MetricSource {
        table,
        bucket_col,
        agg_expr,
    } = source;
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let prewhere = metric_prewhere(services);
    let mut sql = format!(
        "SELECT fingerprint, {agg_expr} AS n\nFROM {table}\n{prewhere}WHERE fingerprint IN ({fp_list}) AND {bucket_col} > {start_ns} AND {bucket_col} <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str(" AND ");
        sql.push_str(clause);
    }
    sql.push_str("\nGROUP BY fingerprint");
    sql
}

/// The client-aggregated metric fetch (issue M6-10): a stage-3-shaped raw
/// scan of `(fingerprint, timestamp_ns, body)` over the **full** window,
/// with the line-filter prefix pushed down — and deliberately **no
/// `LIMIT`**: an aggregation must see every matching line or abort on the
/// byte scan budget (`max_bytes_to_read` → `QueryTooBroad`), never
/// silently truncate (complete-or-error, the adjudicated design). The
/// `PREWHERE service ...` contract matches [`stage3`]/[`metric_range`]
/// (the `log_samples` primary-key prefix stays engaged).
///
/// **Stable total order (review round 2, finding 2):** `ORDER BY`
/// carries `fingerprint, body` as secondary keys — the projection's only
/// other columns — so equal-timestamp rows arrive in one reproducible
/// order across runs/merges/replicas (float accumulation order, and
/// therefore bit-level sums, stay stable; the first/last reducers are
/// additionally order-independent via their own value tie-break).
pub fn metric_raw_samples(
    samples_table: &str,
    services: &[String],
    fingerprints: &[u64],
    window: TimeWindow,
    extra_predicates: &[String],
) -> String {
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str("\n  AND ");
        sql.push_str(clause);
    }
    sql.push_str("\nORDER BY timestamp_ns ASC, fingerprint ASC, body ASC");
    sql
}

/// Renders the metric-read `PREWHERE service ...\n` line, or an empty
/// string when `services` is empty (the rollup path — no `service` column
/// to filter on).
fn metric_prewhere(services: &[String]) -> String {
    if services.is_empty() {
        String::new()
    } else {
        format!("PREWHERE {}\n", service_predicate(services))
    }
}

fn fp_list(fingerprints: &[u64]) -> String {
    fingerprints
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The singleton-equality/`IN` split shared by every stage 3 style
/// predicate over a resolved value set (architect plan amendment §2).
fn service_predicate(services: &[String]) -> String {
    if services.len() == 1 {
        format!("service = {}", services[0])
    } else {
        format!("service IN ({})", services.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage2_renders_the_canonical_hydration_shape() {
        assert_eq!(
            stage2("log_streams", &[18374, 99120]),
            "SELECT fingerprint, service, labels FROM log_streams WHERE fingerprint IN (18374, 99120)"
        );
    }

    #[test]
    fn label_names_renders_a_distinct_key_scan_for_one_month() {
        assert_eq!(
            label_names("log_streams_idx", &["'2026-07-01'".to_string()]),
            "SELECT DISTINCT key AS name\nFROM log_streams_idx\nWHERE month = '2026-07-01'\nORDER BY name"
        );
    }

    #[test]
    fn label_names_renders_a_month_in_list_for_a_boundary_spanning_window() {
        let sql = label_names(
            "log_streams_idx",
            &["'2026-07-01'".to_string(), "'2026-08-01'".to_string()],
        );
        assert!(sql.contains("WHERE month IN ('2026-07-01', '2026-08-01')"));
    }

    #[test]
    fn label_values_renders_a_distinct_value_scan_scoped_to_one_key() {
        assert_eq!(
            label_values("log_streams_idx", &["'2026-07-01'".to_string()], "'env'"),
            "SELECT DISTINCT val AS value\nFROM log_streams_idx\nWHERE month = '2026-07-01' AND key = 'env'\nORDER BY value"
        );
    }

    #[test]
    fn service_predicate_is_bare_equality_for_one_service() {
        assert_eq!(
            service_predicate(&["'checkout'".to_string()]),
            "service = 'checkout'"
        );
    }

    #[test]
    fn service_predicate_is_in_list_for_multiple_services() {
        assert_eq!(
            service_predicate(&["'checkout'".to_string(), "'billing'".to_string()]),
            "service IN ('checkout', 'billing')"
        );
    }

    #[test]
    fn fp_list_joins_with_comma_space() {
        assert_eq!(fp_list(&[1, 2, 3]), "1, 2, 3");
    }

    #[test]
    fn metric_range_omits_prewhere_when_services_is_empty_the_rollup_path() {
        let sql = metric_range(
            MetricSource {
                table: "log_metrics_5s",
                bucket_col: "bucket_ns",
                agg_expr: "sum(count)",
            },
            &[],
            &[1, 2],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            60,
            &[],
        );
        assert!(!sql.contains("PREWHERE"));
    }

    #[test]
    fn metric_range_renders_singleton_prewhere_for_the_raw_fallback() {
        let sql = metric_range(
            MetricSource {
                table: "log_samples",
                bucket_col: "timestamp_ns",
                agg_expr: "count()",
            },
            &["'checkout'".to_string()],
            &[1, 2],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            60,
            &[],
        );
        assert!(sql.contains("PREWHERE service = 'checkout'\n"));
    }

    #[test]
    fn metric_range_renders_in_list_prewhere_for_multiple_services() {
        let sql = metric_range(
            MetricSource {
                table: "log_samples",
                bucket_col: "timestamp_ns",
                agg_expr: "count()",
            },
            &["'checkout'".to_string(), "'billing'".to_string()],
            &[1, 2],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            60,
            &[],
        );
        assert!(sql.contains("PREWHERE service IN ('checkout', 'billing')\n"));
    }

    #[test]
    fn metric_instant_renders_the_same_prewhere_contract() {
        let sql = metric_instant(
            MetricSource {
                table: "log_samples",
                bucket_col: "timestamp_ns",
                agg_expr: "count()",
            },
            &["'checkout'".to_string()],
            &[1],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            &[],
        );
        assert!(sql.contains("PREWHERE service = 'checkout'\n"));
        assert!(!sql.contains("intDiv"));
    }
}
