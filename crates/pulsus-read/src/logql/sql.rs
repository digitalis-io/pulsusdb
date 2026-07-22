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

/// The four textual forms Go's `uuid.Parse` accepts (issue #170,
/// `/detected_labels`' ID-likeness reference, grafana/loki:3.4.2
/// `containsAllIDTypes`): plain hyphenated 8-4-4-4-12 (optionally
/// `urn:uuid:`-prefixed), `{hyphenated}` (both braces required), and bare
/// 32-hex. Case-insensitive (`(?i)`), fully anchored — rendered through
/// [`super::escape::ch_string`] into [`detected_labels`]' `match(val, ...)`
/// predicate, the single implementation (SQL only, no Rust twin to drift).
const UUID_RE: &str = r"(?i)^(?:(?:urn:uuid:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\}|[0-9a-f]{32})$";

/// Detected-labels aggregation over the stream index (issue #170,
/// docs/api.md §2.6): one output row per distinct key within `months`,
/// with `uniqExact(val)` as the exact cardinality (documented improvement
/// over the reference's hyperloglog estimate) and `non_id_values` counting
/// values that are neither a float (`toFloat64OrNull`) nor a UUID
/// ([`UUID_RE`]) — the server-side half of the reference's
/// `containsAllIDTypes` relevance filter (the keep rule — static label OR
/// `non_id_values > 0` — applies client-side in `exec`). `fingerprints` =
/// `None` for the unscoped form; `Some` appends the same `fingerprint IN`
/// filter to the identical idx-months scan. **Never touches
/// `log_samples`** — same table and scan class as [`label_names`],
/// month-partition-pruned, server-side aggregated (fan-in is one row per
/// key, never per value).
pub fn detected_labels(
    streams_idx_table: &str,
    months: &[String],
    fingerprints: Option<&[u64]>,
) -> String {
    let month_clause = month_clause(months);
    let uuid_literal = super::escape::ch_string(UUID_RE);
    let mut sql = format!(
        "SELECT key, uniqExact(val) AS cardinality, countIf(toFloat64OrNull(val) IS NULL AND NOT match(val, {uuid_literal})) AS non_id_values\nFROM {streams_idx_table}\nWHERE {month_clause}"
    );
    if let Some(fps) = fingerprints {
        sql.push_str(&format!("\n  AND fingerprint IN ({})", fp_list(fps)));
    }
    sql.push_str("\nGROUP BY key\nORDER BY key");
    sql
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
        "SELECT fingerprint, timestamp_ns, body, structured_metadata\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause);
    }
    sql.push_str(&format!("\nORDER BY timestamp_ns {order}\nLIMIT {limit}"));
    sql
}

/// The lower-bound mode of one [`stage3_keyset`] page (issue #74 live
/// tail, generalized for issue #90 streams paging — plan v4 D1/D2 + the
/// round-4 adjudication).
///
/// - [`KeysetLower::First`] — the first page: the API window's exclusive
///   `start`/inclusive `end` bounds (`timestamp_ns > start_ns AND
///   timestamp_ns <= end_ns`, docs/schemas.md §3.2), carried by the
///   `window` argument. No keyset term. Direction-agnostic.
/// - [`KeysetLower::After`] — every later page: the occurrence-count
///   keyset. The composite predicate is **inclusive** (`>=` the boundary
///   tuple when walking Forward, `<=` when walking Backward) so a tie
///   group split by `LIMIT` is re-fetched rather than skipped, and the
///   server-side `OFFSET` skips exactly the already-delivered rows of the
///   boundary tuple (deterministic under the total `ORDER BY` below). The
///   redundant `timestamp_ns >= ts` (Forward) / `timestamp_ns <= ts`
///   (Backward) term keeps the primary index's time column engaged for
///   granule pruning (the tuple comparison alone does not prune).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeysetLower {
    /// The first page; the `window` argument carries both time bounds.
    First,
    After {
        /// The boundary `(timestamp_ns, fingerprint, cityHash64(body))`.
        tuple: (i64, u64, u64),
        /// How many rows equal to `tuple` were already delivered — the
        /// SQL `OFFSET`.
        offset: u32,
    },
}

/// Stage 3, keyset-pagination mode (issue #74 live tail; issue #90
/// streams fetch-until-limit — deliberately a reusable builder, the
/// foundation for streams pagination, not a tail-only hack). Leaves the
/// frozen [`stage3`] untouched; shares its `PREWHERE
/// service`/`fingerprint IN`/line-filter pushdown contract byte-for-byte.
///
/// The total `ORDER BY timestamp_ns, fingerprint, body_hash, body` (raw
/// `body` as final tiebreaker), all columns following `direction`, makes
/// equal-`(ts, fp, hash)` rows — including genuine CityHash collisions —
/// a stable adjacent run across queries, so [`KeysetLower::After`]'s
/// occurrence-count `OFFSET` is well-defined. `cityHash64(body)` is
/// projected by ClickHouse and captured as `body_hash` (no CH/Rust
/// divergence at the boundary).
///
/// **`direction` mirrors the whole page.** Forward (ASC, tail's only
/// mode — byte-identical to issue #74's rendering) walks oldest→newest
/// with a `>=` composite and the redundant `timestamp_ns >= ts` lower
/// bound; Backward (DESC, the query default's newest-first mode) walks
/// newest→oldest with a `<=` composite and the redundant `timestamp_ns
/// <= ts` upper bound, keeping the API's `start` bound as the fixed
/// lower.
// One arg over clippy's threshold: `window`, `lower`, and `direction`
// are each independent page coordinates that cannot fold without
// obscuring the SQL contract (the sibling `stage3` sits at the 7-arg
// limit for the same reason).
#[allow(clippy::too_many_arguments)]
pub fn stage3_keyset(
    samples_table: &str,
    services: &[String],
    fingerprints: &[u64],
    window: TimeWindow,
    lower: KeysetLower,
    direction: Direction,
    line_filters: &[String],
    limit: u32,
) -> String {
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;

    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})"
    );
    match (direction, lower) {
        (_, KeysetLower::First) => {
            sql.push_str(&format!(
                "\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
            ));
        }
        (
            Direction::Forward,
            KeysetLower::After {
                tuple: (ts, fp, hash),
                ..
            },
        ) => {
            sql.push_str(&format!(
                "\n  AND timestamp_ns >= {ts} AND timestamp_ns <= {end_ns}\n  AND (timestamp_ns, fingerprint, cityHash64(body)) >= ({ts}, {fp}, {hash})"
            ));
        }
        (
            Direction::Backward,
            KeysetLower::After {
                tuple: (ts, fp, hash),
                ..
            },
        ) => {
            sql.push_str(&format!(
                "\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {ts}\n  AND (timestamp_ns, fingerprint, cityHash64(body)) <= ({ts}, {fp}, {hash})"
            ));
        }
    }
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause);
    }
    let ord = match direction {
        Direction::Forward => "ASC",
        Direction::Backward => "DESC",
    };
    sql.push_str(&format!(
        "\nORDER BY timestamp_ns {ord}, fingerprint {ord}, body_hash {ord}, body {ord}"
    ));
    match lower {
        KeysetLower::First => sql.push_str(&format!("\nLIMIT {limit}")),
        KeysetLower::After { offset, .. } => {
            sql.push_str(&format!("\nLIMIT {limit} OFFSET {offset}"))
        }
    }
    sql
}

/// The `/api/logs/v1/stats` rollup-served aggregation (issue #74, no line
/// filter): zero body reads — `sum(count)`/`sum(bytes)` off
/// `log_metrics_<res>` (PK `(fingerprint, bucket_ns)`), `streams` as
/// `uniqExact(fingerprint)`, and `chunks` as the adjudicated
/// selector-scoped partition-count proxy (`uniqExact` of the bucket's
/// date — docs/api.md §2.5). Same half-open bucket predicate as
/// [`metric_range`].
pub fn log_stats_rollup(rollup_table: &str, fingerprints: &[u64], window: TimeWindow) -> String {
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    format!(
        "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(bucket_ns))) AS chunks, sum(count) AS entries, sum(bytes) AS bytes\nFROM {rollup_table}\nWHERE fingerprint IN ({fp_list}) AND bucket_ns > {start_ns} AND bucket_ns <= {end_ns}"
    )
}

/// The `/api/logs/v1/stats` raw fallback (issue #74, line-filtered): the
/// rollup is body-content-blind, so a line filter forces a `log_samples`
/// scan with the identical `PREWHERE service` + skip-index line-filter
/// prefilters [`stage3`] emits (granule-skipped, PK-pruned). Same
/// `streams`/`chunks` shape as [`log_stats_rollup`]; `entries`/`bytes`
/// count matching lines exactly.
pub fn log_stats_raw(
    samples_table: &str,
    services: &[String],
    fingerprints: &[u64],
    window: TimeWindow,
    line_filters: &[String],
) -> String {
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let mut sql = format!(
        "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(timestamp_ns))) AS chunks, count() AS entries, sum(length(body)) AS bytes\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause);
    }
    sql
}

/// The `/api/logs/v1/volume` aggregation (issue #169, docs/api.md §2.6):
/// per-fingerprint byte volume off `log_metrics_<res>` (PK `(fingerprint,
/// bucket_ns)`) — rollup-ONLY, zero body reads (the endpoint accepts a
/// matchers-only selector, so there is no raw fallback at all, unlike
/// [`log_stats_rollup`]'s line-filtered sibling). Same half-open bucket
/// predicate family as [`log_stats_rollup`]/[`metric_range`], so the
/// identical MinMax + `(fingerprint, bucket_ns)` primary-key pruning
/// applies (`tests/explain_indexes.rs`' Tier-1 gate).
pub fn log_volume_rollup(rollup_table: &str, fingerprints: &[u64], window: TimeWindow) -> String {
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    format!(
        "SELECT fingerprint, sum(bytes) AS bytes\nFROM {rollup_table}\nWHERE fingerprint IN ({fp_list}) AND bucket_ns > {start_ns} AND bucket_ns <= {end_ns}\nGROUP BY fingerprint"
    )
}

/// The maximum number of pattern series `/api/logs/v1/patterns` returns — the
/// top-`N`-by-total-count LIMIT pushed into ClickHouse (M7-C3, issue #171).
pub const MAX_PATTERNS: usize = 1000;

/// The `/api/logs/v1/patterns` aggregation (M7-C3, issue #171, docs/schemas.md
/// §3.2): stage-1 fingerprints → ONE pushed-down aggregate over `log_patterns`
/// with no hydration (the response carries no labels). The inner query
/// re-buckets `bucket_ns` to `step_ns` and sums per `(pattern, ts_ns)`; the
/// outer sums per pattern and emits the ascending `(ts_ns, cnt)` samples array,
/// ordered total-count desc then pattern asc, top-[`MAX_PATTERNS`].
///
/// **Pushdown/pruning:** `fingerprint IN` engages the `(fingerprint, bucket_ns,
/// pattern)` primary-key prefix (granule pruning), daily partitions prune the
/// window (`tests/explain_indexes.rs`' Tier-1 gate), and the aggregation +
/// top-K + LIMIT all execute in ClickHouse — the client decodes ≤ 1000
/// already-assembled series. Half-open window `[start, end)` (D4).
pub fn log_patterns_read(
    patterns_table: &str,
    fingerprints: &[u64],
    window: TimeWindow,
    step_ns: u64,
) -> String {
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let limit = MAX_PATTERNS;
    format!(
        "SELECT pattern, sum(cnt) AS total, arraySort(x -> x.1, groupArray((ts_ns, cnt))) AS samples\nFROM (\n  SELECT pattern, intDiv(bucket_ns, {step_ns}) * {step_ns} AS ts_ns, sum(count) AS cnt\n  FROM {patterns_table}\n  WHERE fingerprint IN ({fp_list}) AND bucket_ns >= {start_ns} AND bucket_ns < {end_ns}\n  GROUP BY pattern, ts_ns\n)\nGROUP BY pattern\nORDER BY total DESC, pattern ASC\nLIMIT {limit}"
    )
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
    fn detected_labels_unscoped_renders_one_row_per_key_with_the_id_predicate() {
        let sql = detected_labels("log_streams_idx", &["'2026-07-01'".to_string()], None);
        assert!(sql.starts_with("SELECT key, uniqExact(val) AS cardinality, countIf(toFloat64OrNull(val) IS NULL AND NOT match(val, "));
        assert!(sql.contains("WHERE month = '2026-07-01'"));
        assert!(!sql.contains("fingerprint"));
        assert!(sql.ends_with("GROUP BY key\nORDER BY key"));
    }

    #[test]
    fn detected_labels_scoped_appends_the_fingerprint_filter_to_the_same_scan() {
        let scoped = detected_labels(
            "log_streams_idx",
            &["'2026-07-01'".to_string()],
            Some(&[7, 9]),
        );
        assert!(scoped.contains("WHERE month = '2026-07-01'\n  AND fingerprint IN (7, 9)"));
        let unscoped = detected_labels("log_streams_idx", &["'2026-07-01'".to_string()], None);
        assert_eq!(
            scoped.replace("\n  AND fingerprint IN (7, 9)", ""),
            unscoped,
            "scoped form must be the unscoped scan plus only the fingerprint filter"
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

    /// Issue #74 first-bound AC (hermetic half): the first tail page
    /// carries the explicit exclusive API `start` bound — the repo
    /// stage-3 convention — and NO keyset term, byte-exact.
    #[test]
    fn stage3_keyset_first_page_is_byte_exact_with_the_exclusive_start_bound() {
        let sql = stage3_keyset(
            "log_samples",
            &["'checkout'".to_string()],
            &[18374],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::First,
            Direction::Forward,
            &[],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service = 'checkout'\n\
             WHERE fingerprint IN (18374)\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 2000\n\
             ORDER BY timestamp_ns ASC, fingerprint ASC, body_hash ASC, body ASC\n\
             LIMIT 500"
        );
    }

    /// Issue #74 (round-4 adjudication #1): the keyset page — inclusive
    /// `>=` composite predicate, redundant time lower bound for granule
    /// pruning, total ORDER BY with the raw-`body` tiebreaker, and the
    /// server-side occurrence-count `OFFSET` — byte-exact.
    #[test]
    fn stage3_keyset_later_page_is_byte_exact_with_inclusive_tuple_and_offset() {
        let sql = stage3_keyset(
            "log_samples",
            &["'checkout'".to_string(), "'billing'".to_string()],
            &[1, 2],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::After {
                tuple: (1_500, 7, 42),
                offset: 3,
            },
            Direction::Forward,
            &["positionCaseSensitive(body, 'err') > 0".to_string()],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service IN ('checkout', 'billing')\n\
             WHERE fingerprint IN (1, 2)\n\
             \x20 AND timestamp_ns >= 1500 AND timestamp_ns <= 2000\n\
             \x20 AND (timestamp_ns, fingerprint, cityHash64(body)) >= (1500, 7, 42)\n\
             \x20 AND positionCaseSensitive(body, 'err') > 0\n\
             ORDER BY timestamp_ns ASC, fingerprint ASC, body_hash ASC, body ASC\n\
             LIMIT 500 OFFSET 3"
        );
    }

    /// Issue #90 (backward paging): the query default's newest-first
    /// keyset — DESC on every ORDER column, the exclusive API `start` as
    /// the fixed lower bound, and the mirrored redundant `timestamp_ns <=
    /// end` upper bound so the first page's granule pruning matches the
    /// forward form.
    #[test]
    fn stage3_keyset_backward_first_page_is_byte_exact_desc_with_the_window_bounds() {
        let sql = stage3_keyset(
            "log_samples",
            &["'checkout'".to_string()],
            &[18374],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::First,
            Direction::Backward,
            &[],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service = 'checkout'\n\
             WHERE fingerprint IN (18374)\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 2000\n\
             ORDER BY timestamp_ns DESC, fingerprint DESC, body_hash DESC, body DESC\n\
             LIMIT 500"
        );
    }

    /// Issue #90 (backward paging): a later newest-first page — the
    /// inclusive `<=` composite tuple, the redundant `timestamp_ns <= ts`
    /// upper bound (walking down), the fixed exclusive `timestamp_ns >
    /// start` lower bound, DESC total order, and the occurrence-count
    /// `OFFSET` — byte-exact.
    #[test]
    fn stage3_keyset_backward_later_page_is_byte_exact_with_le_tuple_and_offset() {
        let sql = stage3_keyset(
            "log_samples",
            &["'checkout'".to_string(), "'billing'".to_string()],
            &[1, 2],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::After {
                tuple: (1_500, 7, 42),
                offset: 3,
            },
            Direction::Backward,
            &["positionCaseSensitive(body, 'err') > 0".to_string()],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service IN ('checkout', 'billing')\n\
             WHERE fingerprint IN (1, 2)\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 1500\n\
             \x20 AND (timestamp_ns, fingerprint, cityHash64(body)) <= (1500, 7, 42)\n\
             \x20 AND positionCaseSensitive(body, 'err') > 0\n\
             ORDER BY timestamp_ns DESC, fingerprint DESC, body_hash DESC, body DESC\n\
             LIMIT 500 OFFSET 3"
        );
    }

    /// Issue #74 pre-query clamp AC (hermetic half): the caller-clamped
    /// fetch limit is exactly what lands in the SQL `LIMIT` — nothing in
    /// the builder can widen it back.
    #[test]
    fn stage3_keyset_limit_is_the_callers_clamped_fetch_limit_verbatim() {
        let sql = stage3_keyset(
            "log_samples",
            &["'checkout'".to_string()],
            &[1],
            TimeWindow {
                start_ns: 0,
                end_ns: 10,
            },
            KeysetLower::First,
            Direction::Forward,
            &[],
            5_000,
        );
        assert!(sql.ends_with("LIMIT 5000"), "{sql}");
    }

    /// Issue #74 AC6 (hermetic half): the no-filter stats aggregation is
    /// rollup-served — zero body reads — byte-exact.
    #[test]
    fn log_stats_rollup_is_byte_exact() {
        let sql = log_stats_rollup(
            "log_metrics_5s",
            &[18374, 99120],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
        );
        assert_eq!(
            sql,
            "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(bucket_ns))) AS chunks, sum(count) AS entries, sum(bytes) AS bytes\n\
             FROM log_metrics_5s\n\
             WHERE fingerprint IN (18374, 99120) AND bucket_ns > 1000 AND bucket_ns <= 2000"
        );
        assert!(!sql.contains("body"), "rollup stats must never read body");
    }

    /// Issue #171 (M7-C3): the `/api/logs/v1/patterns` aggregation is
    /// byte-exact — the pushed-down top-1000 with the `fingerprint IN` PK
    /// prefix, half-open `[start, end)` window, `step_ns` re-bucketing, and the
    /// `groupArray((ts_ns, cnt))` samples array. Never reads `body`.
    #[test]
    fn log_patterns_read_is_byte_exact() {
        let sql = log_patterns_read(
            "log_patterns",
            &[18374, 99120],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            10_000_000_000,
        );
        assert_eq!(
            sql,
            "SELECT pattern, sum(cnt) AS total, arraySort(x -> x.1, groupArray((ts_ns, cnt))) AS samples\n\
             FROM (\n  \
               SELECT pattern, intDiv(bucket_ns, 10000000000) * 10000000000 AS ts_ns, sum(count) AS cnt\n  \
               FROM log_patterns\n  \
               WHERE fingerprint IN (18374, 99120) AND bucket_ns >= 1000 AND bucket_ns < 2000\n  \
               GROUP BY pattern, ts_ns\n\
             )\n\
             GROUP BY pattern\n\
             ORDER BY total DESC, pattern ASC\n\
             LIMIT 1000"
        );
        assert!(!sql.contains("body"), "patterns must never read body");
    }

    /// Issue #74 AC6 (hermetic half): the line-filtered stats fallback
    /// scans `log_samples` with stage-3's exact PREWHERE + skip-index
    /// line-filter pushdown — byte-exact.
    #[test]
    fn log_stats_raw_is_byte_exact_with_line_filter_pushdown() {
        let sql = log_stats_raw(
            "log_samples",
            &["'checkout'".to_string()],
            &[18374],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            &["positionCaseSensitive(body, 'err') > 0".to_string()],
        );
        assert_eq!(
            sql,
            "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(timestamp_ns))) AS chunks, count() AS entries, sum(length(body)) AS bytes\n\
             FROM log_samples\n\
             PREWHERE service = 'checkout'\n\
             WHERE fingerprint IN (18374)\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 2000\n\
             \x20 AND positionCaseSensitive(body, 'err') > 0"
        );
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
