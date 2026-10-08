//! Pure series-read SQL builders — the snapshot-testing surface for the
//! lookup and activity reads (issue #623; docs/schemas.md §2.1): every
//! matcher on `metric_labels`, the window on `metric_series`.
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
//! | Re/Nre pattern `match(JSONExtractString(labels,'<key>'), '<pat>')` | fully-anchored `'(?-s)^(?:pat)$'` literal | the issue #240 PromQL RE2-authority exemption, callable ONLY from [`super::series_where`]'s sealed renderer — this file can name the escaper but cannot present the token its signature demands (`E0624`, measured) |
//!
//! The `(?-s)` prefix is issue #324: ClickHouse's `match()` sets RE2's
//! `dot_nl` option, so `.` matches a newline there and does not in RE2
//! itself — the escaper's own doc carries the measurement. Issue #315 adds
//! a constant, analysis-time compile probe alongside every regex
//! predicate, so a pattern RE2 rejects is rejected even when the query
//! window holds no rows for `match()` to run on.
//!
//! Absent-label semantics are unchanged from the in-process path:
//! `JSONExtractString` returns `''` for a missing key, matching
//! [`super::labels`]'s `""` rule for a missing [`pulsus_model::LabelSet`]
//! entry (load-bearing for the cache-vs-SQL differential test).
//!
//! **Placeholder-doubling is NOT this module's concern.** The anchored
//! literal always carries a literal `?` (the `(?-s)^(?:...)$` template) — the `clickhouse`
//! crate's `SqlBuilder` treats a bare `?` as an unbound bind placeholder
//! unless doubled. The canonical SQL text this module returns is
//! deliberately un-doubled (snapshot-testable, matches `logql::sql`'s own
//! contract); the doubling is applied once, at the execution boundary
//! (`logql::exec::escape_query_placeholders`'s pattern) — issue #31's
//! engine must apply it before this text reaches `ChClient::query_stream`,
//! exactly as `logql::exec` already does for its own regex SQL.

use pulsus_model::{FpLiteral, Tenant};

use crate::logql::escape::ch_string;

use super::TenantSql;
use super::matcher::{DataWindow, DiscoveryFilter, LabelMatcher};
use super::series_where::{Lookup, SeriesTables, SeriesWhere};

/// `metric_name = '<name>'`: one metric's scope on the lookup (issue #623).
fn name_scope(metric_name: &str) -> String {
    format!("metric_name = {}", ch_string(metric_name))
}

/// `metric_name IN ('<a>', '<b>')`: several metrics' scope.
fn names_scope(metric_names: &[String]) -> String {
    let name_list = metric_names
        .iter()
        .map(|n| ch_string(n))
        .collect::<Vec<_>>()
        .join(", ");
    format!("metric_name IN ({name_list})")
}

/// `fingerprint IN (<ids>)`: a set of series IDs already resolved.
fn ids_scope(fps: &[FpLiteral]) -> String {
    let fp_list = fps
        .iter()
        .map(FpLiteral::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!("fingerprint IN ({fp_list})")
}

/// One series read (issue #623): the window on the activity table and every
/// matcher on the lookup, rendered by [`SeriesWhere`] — so "a user regex
/// rendered without its probe" is not expressible here from the sanctioned
/// components (see `super::series_where`'s module doc, which also states
/// the one unsealed crossing rustc does not police).
fn series_read(
    tenant: &Tenant,
    window: DataWindow,
    scope: &[String],
    name_matchers: &[LabelMatcher],
    matchers: &[LabelMatcher],
) -> SeriesWhere {
    SeriesWhere::activity(
        tenant,
        window,
        Lookup {
            scope,
            name_matchers,
            matchers,
        },
    )
}

/// The lookup route's tables (issue #635): no label index, so every read
/// renders today's lookup statement.
fn lookup_tables<'a>(series_table: &'a str, labels_table: &'a str) -> SeriesTables<'a> {
    SeriesTables {
        series: series_table,
        labels: labels_table,
        label_index: None,
        label_values: None,
    }
}

/// The sweep's statement (`super::refresh`): statement 2 with no matcher,
/// every series active in the cache window with its own name and labels.
pub fn sweep_query(
    tenant: &Tenant,
    series_table: &str,
    labels_table: &str,
    window: DataWindow,
) -> String {
    series_read(tenant, window, &[], &[], &[])
        .with_labels(lookup_tables(series_table, labels_table))
}

/// Statement 1 (issue #623): the IDs of `metric_name`'s series that
/// `matchers` select on the lookup and the activity table finds in
/// `window`. Inlined verbatim by issue #31's fallback as `fingerprint IN (
/// <this> )` against the sample tables, and bounded by the `info()`
/// cardinality probe. No `ORDER BY`: the caller needs a *set*, and `IN
/// (...)` ignores the repeats unmerged activity rows give.
pub fn historical_series_subquery(
    tenant: &Tenant,
    series_table: &str,
    labels_table: &str,
    metric_name: &str,
    window: DataWindow,
    matchers: &[LabelMatcher],
) -> String {
    format!(
        "SELECT fingerprint\n{}",
        series_read(tenant, window, &[name_scope(metric_name)], &[], matchers)
            .ids_from_where(lookup_tables(series_table, labels_table))
    )
}

/// Issue #82 (retroactive re-review, Finding 1): bounds an `info()`
/// node's DEGRADED-path (cache-miss) series-selection subquery — the
/// exact [`historical_series_subquery`] text
/// `super::labels::LabelledResolution::SqlFallback` already returns —
/// with a `LIMIT cap+1` (the group-B `distinct_metric_names_probe`
/// shape below), so the caller can COUNT the returned rows and reject
/// `> cap` BEFORE the unbounded subquery is ever inlined into the real
/// sample fetch (`sample_fetch_subquery`) — the info-family fetch is
/// bounded before materialization, never a post-fetch backstop.
///
/// **`DISTINCT` (#82 code-review round: the [high] over-count fix):**
/// the activity table holds one row per series per day, and more than one
/// until its parts merge, so one series active across a wide window yields
/// several rows sharing a fingerprint. The inner
/// [`historical_series_subquery`] deliberately does NOT dedup (safe for
/// the sample fetch's `IN (...)` SET semantics — do not change it), so
/// the probe deduplicates HERE: it must count DISTINCT series. The `LIMIT
/// cap+1` applies over the deduplicated set; `ORDER BY fingerprint` makes
/// the probe's own row set deterministic (only the COUNT matters).
pub fn info_series_cardinality_probe(series_subquery_sql: &str, cap: u64) -> String {
    format!(
        "SELECT DISTINCT fingerprint\nFROM (\n{series_subquery_sql}\n)\nORDER BY fingerprint\nLIMIT {}",
        cap.saturating_add(1)
    )
}

/// Statement 2 for one metric (issue #623): `fingerprint, metric_name,
/// labels`, one row per series of `metric_name` that `matchers` select and
/// the activity table finds in `window`. Used by the live differential
/// tests (a materialized comparison set against the in-process resolution)
/// and by any caller wanting the historical labels themselves rather than
/// an `IN (...)` sub-query.
pub fn historical_resolution_query(
    tenant: &Tenant,
    series_table: &str,
    labels_table: &str,
    metric_name: &str,
    window: DataWindow,
    matchers: &[LabelMatcher],
) -> String {
    series_read(tenant, window, &[name_scope(metric_name)], &[], matchers)
        .with_labels(lookup_tables(series_table, labels_table))
}

/// Statement 4 (issue #623): `fingerprint, metric_name, labels` for an
/// already-**resolved** ID list (mirrors `logql::exec`'s stage-2 hydration
/// precedent). Used only to hydrate the `SqlFallback` sample fetch
/// ([`super::sample_sql::sample_fetch_subquery`]), whose IDs are those that
/// returned samples in the request window — so this read needs no window
/// of its own. `metric_names` scopes the lookup to a key range; `any`
/// collapses the copies the table holds until it merges.
pub fn series_labels_by_fingerprint(
    tenant: &Tenant,
    labels_table: &str,
    metric_names: &[String],
    fps: &[FpLiteral],
) -> String {
    format!(
        "SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels\n\
         FROM (\n\
         \x20 SELECT fingerprint, metric_name AS name, labels AS label_text\n\
         \x20 FROM {labels_table}\n\
         \x20 WHERE org_id = {}\n\
         \x20   AND {}\n\
         \x20   AND {}\n\
         )\n\
         GROUP BY fingerprint\n\
         ORDER BY metric_name, fingerprint",
        tenant.sql_literal(),
        names_scope(metric_names),
        ids_scope(fps)
    )
}

/// Issue #32's discovery query, statement 2 (issue #623): `fingerprint,
/// metric_name, labels` for **all** series matching `filter` in `window` —
/// used by `MetricsEngine::{label_names,label_values,series}`, which apply
/// their **own** window here rather than trusting the label cache's wider
/// (whole-`PULSUS_CACHE_WINDOW`) resident superset (#30 handoff AC: the
/// cache's superset must not leak into a discovery response for a narrower
/// request window). `filter.metric_name == None` renders no name scope —
/// "every metric", the reference's own `/labels`/`/label/{name}/values`
/// semantics when `match[]` is omitted (docs/api.md §3.3). Each row
/// carries its own `metric_name`, which a name-less filter's caller needs
/// for `__name__`.
pub fn discovery_query(
    tenant: &Tenant,
    series_table: &str,
    labels_table: &str,
    filter: &DiscoveryFilter,
    window: DataWindow,
) -> String {
    discovery_read(tenant, filter, window).with_labels(lookup_tables(series_table, labels_table))
}

/// [`discovery_query`] over `t` (issue #635): a name-less filter with a
/// positive label matcher reads its IDs from the label index when `t`
/// names it; every other filter renders [`discovery_query`]'s text.
pub(super) fn discovery_series_query(
    tenant: &Tenant,
    t: SeriesTables<'_>,
    filter: &DiscoveryFilter,
    window: DataWindow,
) -> String {
    discovery_read(tenant, filter, window).with_labels(t)
}

/// [`discovery_distinct_names_query`] over `t` (issue #635).
pub(super) fn discovery_names_query(
    tenant: &Tenant,
    t: SeriesTables<'_>,
    filter: &DiscoveryFilter,
    window: DataWindow,
) -> String {
    format!(
        "SELECT DISTINCT metric_name\n{}\nORDER BY metric_name",
        discovery_read(tenant, filter, window).ids_from_where(t)
    )
}

/// Issue #635: `/labels` for one name-less filter, answered from the label
/// index — the distinct keys of the series `filter` selects in `window`.
pub(super) fn discovery_label_names_query(
    tenant: &Tenant,
    t: SeriesTables<'_>,
    filter: &DiscoveryFilter,
    window: DataWindow,
) -> String {
    discovery_read(tenant, filter, window).label_keys(t)
}

/// Issue #635: `/label/{key}/values` for one name-less filter, answered
/// from the label index.
pub(super) fn discovery_label_values_query(
    tenant: &Tenant,
    t: SeriesTables<'_>,
    key: &str,
    filter: &DiscoveryFilter,
    window: DataWindow,
) -> String {
    discovery_read(tenant, filter, window).label_values(key, t)
}

/// Issue #635 part 3, statement R: the names and label sets of the series a
/// selector with no metric name and no `__name__` matcher selects in
/// `window`, through the label index when `t` names it, in `(metric_name,
/// fingerprint)` order and capped at `cap + 1` rows, so a caller can tell
/// a result past `cap` from one at it.
pub(super) fn nameless_resolution_query(
    tenant: &Tenant,
    t: SeriesTables<'_>,
    matchers: &[LabelMatcher],
    window: DataWindow,
    cap: u64,
) -> String {
    format!(
        "{}\nLIMIT {}",
        discovery_series_query(tenant, t, &nameless_filter(matchers), window),
        cap.saturating_add(1)
    )
}

/// Issue #635 part 3, statement 1: the IDs [`nameless_resolution_query`]
/// selects, as the sub-query the sample statements nest.
pub(super) fn nameless_ids_query(
    tenant: &Tenant,
    t: SeriesTables<'_>,
    matchers: &[LabelMatcher],
    window: DataWindow,
) -> String {
    format!(
        "SELECT fingerprint\n{}",
        discovery_read(tenant, &nameless_filter(matchers), window).ids_from_where(t)
    )
}

/// The filter of a selector with no metric name and no `__name__` matcher.
fn nameless_filter(matchers: &[LabelMatcher]) -> DiscoveryFilter {
    DiscoveryFilter {
        metric_name: None,
        name_matchers: Vec::new(),
        matchers: matchers.to_vec(),
    }
}

/// The one series read [`discovery_query`] and
/// [`discovery_distinct_names_query`] share: the same window and the same
/// lookup predicates, so the two cannot disagree about which series
/// `filter` selects. `the_two_discovery_builders_share_one_where_byte_for_byte`
/// below is what would catch a hand-derived second form.
fn discovery_read(tenant: &Tenant, filter: &DiscoveryFilter, window: DataWindow) -> SeriesWhere {
    let scope: Vec<String> = filter.metric_name.iter().map(|n| name_scope(n)).collect();
    series_read(
        tenant,
        window,
        &scope,
        &filter.name_matchers,
        &filter.matchers,
    )
}

/// Issue #472's narrow discovery projection, statement 3 (issue #623): the
/// distinct metric names of exactly the series [`discovery_query`] would
/// have returned, for the SAME `filter` and `window`.
///
/// **Why the answer is unchanged.** Both read the activity rows in the
/// window whose IDs the lookup predicates select (one [`discovery_read`]);
/// the activity row carries the series' name, so the distinct names of
/// that set are the names of [`discovery_query`]'s rows.
///
/// **The lookup is dropped from the statement, which is not the same as
/// never being read.** With no `match[]` — the discovery client's actual
/// first call — there is no lookup predicate, so `metric_labels` is not
/// referenced at all and the statement reads the activity table alone
/// (#472). With matchers it reads the lookup to evaluate them; the win
/// there is transport and parse count (rows collapse from one-per-series to
/// one-per-metric-name, and `parse_canonical_label_set` is not called at
/// all), not bytes read.
///
/// **NO `LIMIT`.** The discovery `limit` stays a response-size cap applied
/// last, in the handler (docs/api.md §3.3).
pub fn discovery_distinct_names_query(
    tenant: &Tenant,
    series_table: &str,
    labels_table: &str,
    filter: &DiscoveryFilter,
    window: DataWindow,
) -> String {
    format!(
        "SELECT DISTINCT metric_name\n{}\nORDER BY metric_name",
        discovery_read(tenant, filter, window)
            .ids_from_where(lookup_tables(series_table, labels_table))
    )
}

/// Issue #89's discovery analog of [`super::sample_sql::sample_fetch_multi`],
/// statement 2 (issue #623): ONE query for a regex/negated-`__name__`
/// `match[]` selector's whole resolved candidate set, scoped on the lookup
/// to `metric_name IN (<resolved names>) AND fingerprint IN (<resolved
/// IDs>)`. The IDs come from the label cache, which holds every series
/// active in its whole window, so the request window is applied here by
/// the activity read — the cache's wider resident superset never leaks into
/// a narrower discovery response (the `discovery_series` invariant).
pub fn discovery_fetch_multi(
    tenant: &Tenant,
    series_table: &str,
    labels_table: &str,
    metric_names: &[String],
    fps: &[FpLiteral],
    window: DataWindow,
) -> String {
    let scope = [names_scope(metric_names), ids_scope(fps)];
    series_read(tenant, window, &scope, &[], &[])
        .with_labels(lookup_tables(series_table, labels_table))
}

/// Issue #96's degraded-cache discovery **probe**, statement 3 with the
/// selector's **name matchers** on the lookup (issue #623): the bounded
/// `SELECT DISTINCT metric_name` that resolves the candidate metric-name
/// set when the resident label cache cannot (cold / stale /
/// out-of-window / regex-cache-full — [`MultiMetricResolution::
/// Unresolvable`](super::labels::MultiMetricResolution::Unresolvable)). The
/// ordinary label matchers apply later, in the [`discovery_fetch_by_names`]
/// fetch — a deliberately cheap names-only probe with a fail-safe superset
/// cap (the #96 adjudication).
///
/// `LIMIT {fanout_cap + 1}` bounds the **returned** row count: the caller
/// aborts to `QueryTooBroad(MetricFanout)` when it sees more than
/// `fanout_cap` rows, never an unbounded `IN` set. The `+1` is computed
/// with [`u64::saturating_add`] as inert defense-in-depth for builder
/// totality; config load caps `fanout_cap` at
/// `pulsus_config::PROMQL_MAX_METRIC_FANOUT_CEILING` (issue #96
/// retroactive re-review), so at runtime `cap + 1` never saturates.
pub fn distinct_metric_names_probe(
    tenant: &Tenant,
    series_table: &str,
    labels_table: &str,
    name_matchers: &[LabelMatcher],
    window: DataWindow,
    fanout_cap: u64,
) -> String {
    format!(
        "SELECT DISTINCT metric_name\n{}\nORDER BY metric_name\nLIMIT {}",
        series_read(tenant, window, &[], name_matchers, &[])
            .ids_from_where(lookup_tables(series_table, labels_table)),
        fanout_cap.saturating_add(1)
    )
}

/// Issue #96's degraded-cache discovery **fetch**, statement 2 (issue
/// #623): the names-only analog of [`discovery_fetch_multi`], scoped on the
/// lookup to `metric_name IN (<probed names>)` with the ordinary **label
/// matchers** applied there and the request window applied by the activity
/// read, so the below-cap result set is the warm
/// [`discovery_fetch_multi`] path's. `metric_names` is the probe's sorted,
/// deduped, non-empty output (caller guarantees non-empty — an empty probe
/// result skips the fetch entirely).
pub fn discovery_fetch_by_names(
    tenant: &Tenant,
    series_table: &str,
    labels_table: &str,
    metric_names: &[String],
    matchers: &[LabelMatcher],
    window: DataWindow,
) -> String {
    series_read(tenant, window, &[names_scope(metric_names)], &[], matchers)
        .with_labels(lookup_tables(series_table, labels_table))
}

/// `GET /api/v1/metadata` (issue #32): `metric_metadata` is a
/// `ReplacingMergeTree(updated_ns)` (docs/schemas.md §2.1) whose merges are
/// asynchronous, so a plain `SELECT` can observe more than one row per
/// `metric_name` — `argMax(_, updated_ns)` deterministically collapses to the
/// latest-written value without waiting for a merge, grouped by the base
/// family name (schemas.md §2.1's writer contract: a derived series' suffix
/// is never stripped here — callers must already be querying by the base
/// name). `metric` is an optional exact-name filter, `limit` an optional row
/// cap.
///
/// **One `argMax` over the whole tuple, not three independent ones** (issue
/// #603). Three separate calls may resolve column by column where two rows
/// for a name carry EQUAL `updated_ns`, and answer a descriptor assembled
/// from both — the type from one row, the help from the other. One aggregate
/// over the tuple makes one of the two rows win whole; which one is
/// unspecified and needs no rule, since both are descriptors a client sent in
/// that nanosecond, and the next push for that name settles it with a larger
/// stamp. The columns are unpacked outside the grouping, so the four the
/// caller decodes are unchanged, in order.
pub fn metadata_query(
    tenant: &Tenant,
    metadata_table: &str,
    metric: Option<&str>,
    limit: Option<usize>,
) -> String {
    let mut sql = String::from(
        "SELECT metric_name, tupleElement(d, 1) AS metric_type, tupleElement(d, 2) AS help, tupleElement(d, 3) AS unit\nFROM (SELECT metric_name, argMax((metric_type, help, unit), updated_ns) AS d",
    );
    sql.push_str(&format!("\nFROM {metadata_table}"));
    sql.push_str(&format!("\nWHERE org_id = {}", tenant.sql_literal()));
    if let Some(name) = metric {
        sql.push_str(&format!("\n  AND metric_name = {}", ch_string(name)));
    }
    sql.push_str("\nGROUP BY metric_name)\nORDER BY metric_name");
    if let Some(n) = limit {
        sql.push_str(&format!("\nLIMIT {n}"));
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The single-tenant deployment's tenant: no `X-Scope-OrgID`.
    fn no_tenant() -> Tenant {
        Tenant::from_header(None, false).expect("no header is the empty tenant")
    }
    use crate::metrics::anchored_re2_literal_for_test;
    use crate::metrics::matcher::MatchOp;
    use pulsus_model::Fingerprint;

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
    fn historical_series_subquery_renders_the_day_window() {
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "http_requests_total",
            window(),
            &[],
        );
        assert!(sql.contains("metric_name = 'http_requests_total'"));
        assert!(sql.contains("\n  AND day BETWEEN '1970-01-01' AND '1970-01-01'\n"));
        assert!(sql.starts_with("SELECT fingerprint\nFROM metric_series"));
    }

    #[test]
    fn historical_series_subquery_has_no_order_by_or_limit_1_by() {
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[eq("job", "api")],
        );
        assert!(!sql.contains("ORDER BY"));
        assert!(!sql.contains("LIMIT"));
    }

    /// #82 code-review round ([high] over-count fix): the probe wraps the
    /// non-deduplicated inner subquery (verbatim, unchanged) in a
    /// `SELECT DISTINCT fingerprint` so one series spanning many activity
    /// buckets consumes exactly ONE cardinality slot, with the
    /// `LIMIT cap+1` applied over the deduplicated set.
    #[test]
    fn info_series_cardinality_probe_dedups_fingerprints_before_the_cap_plus_one_limit() {
        let base = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "target_info",
            window(),
            &[],
        );
        let sql = info_series_cardinality_probe(&base, 999);
        assert_eq!(
            sql,
            format!(
                "SELECT DISTINCT fingerprint\nFROM (\n{base}\n)\nORDER BY fingerprint\nLIMIT 1000"
            )
        );
    }

    /// Boundary value (the `distinct_metric_names_probe` precedent): at
    /// the maximum accepted cap (`u64::MAX`) the `cap + 1` limit must NOT
    /// overflow or panic — `saturating_add(1)` clamps to `u64::MAX`.
    #[test]
    fn info_series_cardinality_probe_does_not_overflow_at_max_cap() {
        let base = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "target_info",
            window(),
            &[],
        );
        let sql = info_series_cardinality_probe(&base, u64::MAX);
        assert!(sql.ends_with(&format!("LIMIT {}", u64::MAX)), "got: {sql}");
    }

    #[test]
    fn series_labels_by_fingerprint_has_no_window_or_matcher_predicates() {
        let sql = series_labels_by_fingerprint(
            &no_tenant(),
            "metric_labels",
            &["up".to_string()],
            &[Fingerprint::from_raw(1).sql_literal()],
        );
        assert!(!sql.contains("day BETWEEN"));
        assert!(!sql.contains("JSONExtractString"));
    }

    #[test]
    fn eq_matcher_renders_json_extract_equality() {
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
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
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[m],
        );
        assert!(sql.contains("JSONExtractString(labels, 'job') != 'api'"));
    }

    #[test]
    fn re_matcher_renders_anchored_match() {
        let m = LabelMatcher {
            key: "status".to_string(),
            op: MatchOp::Re,
            value: "5..".to_string(),
        };
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[m],
        );
        assert!(sql.contains("match(JSONExtractString(labels, 'status'), '(?-s)^(?:5..)$')"));
    }

    #[test]
    fn nre_matcher_renders_negated_anchored_match() {
        let m = LabelMatcher {
            key: "status".to_string(),
            op: MatchOp::Nre,
            value: "5..".to_string(),
        };
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[m],
        );
        assert!(sql.contains("NOT match(JSONExtractString(labels, 'status'), '(?-s)^(?:5..)$')"));
    }

    fn re(key: &str, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op: MatchOp::Re,
            value: value.to_string(),
        }
    }

    /// Issue #324: every pattern this module hands ClickHouse carries RE2's
    /// `(?-s)` flag ahead of the anchor, so `match()`'s `dot_nl` default
    /// cannot make `.` match a newline. Pinned on the rendered SQL, not on
    /// the escaper, because the escaper demands a token only
    /// `series_where` can construct.
    #[test]
    fn every_rendered_pattern_carries_re2s_dot_excludes_newline_flag() {
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[re("status", "5..")],
        );
        assert_eq!(sql.matches("'(?-s)^(?:5..)$'").count(), 2, "got: {sql}");
        assert!(
            !sql.contains("'^(?:"),
            "an unprefixed anchored literal survived: {sql}"
        );
        assert_eq!(anchored_re2_literal_for_test("5.."), "'(?-s)^(?:5..)$'");
    }

    /// Issue #331: a matcher whose pattern carries a no-`i` flag-group
    /// head would silently select no rows (ClickHouse's analyzer leaks
    /// the head into its required substring), so the escaper appends
    /// `-i` to the head — a no-op the analyzer handles — and the
    /// `(?-s)` prefix composes in front unchanged. A head coexisting
    /// with an `i`-carrying one instead gets the never-matching `|$.`
    /// arm. Everything else must render exactly as issue #324 left it
    /// (previous test).
    #[test]
    fn issue_331_affected_matchers_render_the_analyzer_workaround() {
        assert_eq!(
            anchored_re2_literal_for_test("(?s:a.b)"),
            "'(?-s)^(?:(?s-i:a.b))$'"
        );
        assert_eq!(
            anchored_re2_literal_for_test("(?m)up$"),
            "'(?-s)^(?:(?m-i)up$)$'"
        );
        assert_eq!(
            anchored_re2_literal_for_test("(?i)(?s:ab)"),
            "'(?-s)^(?:(?i)(?s:ab))$|$.'"
        );
        // Fix round 3: not literal-leading -> the arm, by measurement.
        assert_eq!(
            anchored_re2_literal_for_test("(?s:.*5..)"),
            "'(?-s)^(?:(?s:.*5..))$|$.'"
        );
        // The workaround changes the row predicate and the issue #315
        // compile probe identically — they share the renderer.
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[re("status", "(?s:5..)")],
        );
        assert_eq!(
            sql.matches("'(?-s)^(?:(?s-i:5..))$'").count(),
            2,
            "got: {sql}"
        );
    }

    /// Issue #315: the compile probe is a **constant** `match()` folded at
    /// analysis time; since issue #623 it is its own line of the activity
    /// read, one per regex.
    #[test]
    fn a_regex_matcher_adds_a_constant_compile_probe_to_the_lower_bound() {
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[re("status", "5..")],
        );
        assert!(
            sql.contains("\n  AND 0 * match('', '(?-s)^(?:5..)$') = 0\n"),
            "got: {sql}"
        );
    }

    /// The probe names EVERY regex matcher, in matcher order — ClickHouse
    /// folds the sum left to right, so the first invalid pattern is the one
    /// reported, matching upstream's own order.
    #[test]
    fn the_compile_probe_names_every_regex_matcher_in_order() {
        let nre = LabelMatcher {
            key: "env".to_string(),
            op: MatchOp::Nre,
            value: "dev".to_string(),
        };
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[re("status", "5.."), eq("job", "api"), nre],
        );
        assert!(
            sql.contains(
                "\n  AND 0 * match('', '(?-s)^(?:5..)$') = 0\n  AND 0 * match('', \
                 '(?-s)^(?:dev)$') = 0\n"
            ),
            "got: {sql}"
        );
    }

    /// The other half, and the one that keeps the probe free: a matcher set
    /// with no regex renders byte-identically to before issue #315, so no
    /// EXPLAIN plan or snapshot moves for the common case.
    #[test]
    fn a_matcher_set_without_a_regex_renders_no_probe_at_all() {
        for matchers in [
            vec![],
            vec![eq("job", "api")],
            vec![LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Neq,
                value: "api".to_string(),
            }],
        ] {
            let sql = historical_series_subquery(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                "up",
                window(),
                &matchers,
            );
            assert!(!sql.contains("0 * "), "got: {sql}");
            assert!(!sql.contains("match("), "got: {sql}");
        }
    }

    /// End-to-end shape check over today's builders. **Not** the guarantee
    /// that probe coverage is complete — this is a hand-maintained list,
    /// and review round 1 correctly rejected it as the primary defence: a
    /// seventh builder could be added without extending it. The guarantee
    /// is structural and lives in `super::series_where`, whose renderer is
    /// the only source of a bound or a `match()` predicate *from the
    /// sanctioned components* — the `_for_test` literal seam stays
    /// splicable by hand, per that module doc's "what rustc does not
    /// enforce" (`a_rendered_regex_and_its_compile_probe_are_inseparable`
    /// states the property; the module doc records the compile errors a
    /// builder gets for recombining the components). What this test still
    /// earns is that each
    /// builder actually *routes through* that renderer and splices the tail
    /// into the right place in its own statement.
    #[test]
    fn every_builder_that_renders_a_user_regex_also_renders_the_probe() {
        let filter = DiscoveryFilter {
            metric_name: Some("up".to_string()),
            name_matchers: vec![],
            matchers: vec![re("status", "5..")],
        };
        let nameless = DiscoveryFilter {
            metric_name: None,
            name_matchers: vec![],
            matchers: vec![re("status", "5..")],
        };
        let name_matcher = LabelMatcher {
            key: "__name__".to_string(),
            op: MatchOp::Re,
            value: "up.*".to_string(),
        };
        let built = [
            historical_series_subquery(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                "up",
                window(),
                &[re("status", "5..")],
            ),
            historical_resolution_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                "up",
                window(),
                &[re("status", "5..")],
            ),
            discovery_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &filter,
                window(),
            ),
            discovery_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &nameless,
                window(),
            ),
            // Issue #472's narrow projection shares `discovery_from_where`
            // with `discovery_query`, so it inherits the same sealed tail —
            // listed here anyway, because the list is what says a builder
            // was considered.
            discovery_distinct_names_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &filter,
                window(),
            ),
            discovery_distinct_names_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &nameless,
                window(),
            ),
            discovery_fetch_by_names(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &["up".to_string()],
                &[re("status", "5..")],
                window(),
            ),
            distinct_metric_names_probe(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &[name_matcher],
                window(),
                10,
            ),
        ];
        for sql in built {
            assert!(sql.contains("match("), "no user regex rendered: {sql}");
            assert!(
                sql.contains("0 * match('', "),
                "a user regex reached SQL with no compile probe: {sql}"
            );
        }
    }

    #[test]
    fn multiple_matchers_are_all_anded_together() {
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "http_requests_total",
            window(),
            &[eq("job", "api"), eq("env", "prod")],
        );
        assert!(sql.contains("JSONExtractString(labels, 'job') = 'api'"));
        assert!(sql.contains("JSONExtractString(labels, 'env') = 'prod'"));
        assert_eq!(sql.matches("JSONExtractString(labels,").count(), 2);
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
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[m],
        );
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
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            "up",
            window(),
            &[m],
        );
        let expected = anchored_re2_literal_for_test(payload);
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
        let sql = historical_series_subquery(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            name_payload,
            window(),
            &[m],
        );
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
        let sql = discovery_query(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &filter,
            window(),
        );
        assert!(sql.contains("metric_name = 'up'"));
        assert!(sql.contains("JSONExtractString(labels, 'job') = 'api'"));
        assert!(sql.starts_with(
            "SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels\nFROM ("
        ));
        assert!(sql.ends_with("ORDER BY metric_name, fingerprint"));
    }

    #[test]
    fn discovery_query_without_a_metric_name_has_no_metric_name_predicate() {
        let filter = DiscoveryFilter::default();
        let sql = discovery_query(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &filter,
            window(),
        );
        assert!(!sql.contains("metric_name ="));
        assert!(sql.contains(" AND day BETWEEN '1970-01-01' AND '1970-01-01'\n"));
    }

    #[test]
    fn discovery_query_without_a_metric_name_still_applies_matchers() {
        let filter = DiscoveryFilter {
            metric_name: None,
            name_matchers: vec![],
            matchers: vec![eq("job", "api")],
        };
        let sql = discovery_query(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &filter,
            window(),
        );
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
        let sql = discovery_query(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &filter,
            window(),
        );
        assert!(sql.contains(&format!("metric_name = {}", ch_string(payload))));
        assert_no_unescaped_quote(&ch_string(payload));
    }

    // --- discovery_distinct_names_query (issue #472) ---

    /// The filter table AC1 and AC3 both range over: no name, a concrete
    /// name, one `Eq` matcher, one `Re` matcher, and a name carrying a
    /// hostile quote. Both statements are rendered from each element, so a
    /// head that drifts in only one of the five shapes is still caught.
    fn discovery_filter_table() -> Vec<(&'static str, DiscoveryFilter)> {
        vec![
            ("unfiltered", DiscoveryFilter::default()),
            (
                "concrete name",
                DiscoveryFilter {
                    metric_name: Some("up".to_string()),
                    name_matchers: vec![],
                    matchers: vec![],
                },
            ),
            (
                "one Eq matcher",
                DiscoveryFilter {
                    metric_name: None,
                    name_matchers: vec![],
                    matchers: vec![eq("job", "api")],
                },
            ),
            (
                "one Re matcher",
                DiscoveryFilter {
                    metric_name: None,
                    name_matchers: vec![],
                    matchers: vec![re("status", "5..")],
                },
            ),
            (
                "hostile-quote name",
                DiscoveryFilter {
                    metric_name: Some("up'; DROP TABLE metric_series; --".to_string()),
                    name_matchers: vec![],
                    matchers: vec![eq("job", "api")],
                },
            ),
        ]
    }

    /// Issue #472 AC3 (issue #623) — the two builders read the same series:
    /// the wide statement nests the narrow one's activity read, line for
    /// line, as the set its lookup rows are drawn from.
    ///
    /// **What it does not prove:** that the read is *shared* rather than
    /// duplicated-and-currently-equal. That is what [`discovery_read`]
    /// gives by construction, and construction is all it gives.
    #[test]
    fn the_two_discovery_builders_share_one_where_byte_for_byte() {
        for (what, filter) in discovery_filter_table() {
            let wide = discovery_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &filter,
                window(),
            );
            let narrow = discovery_distinct_names_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &filter,
                window(),
            );
            let body = narrow
                .strip_prefix("SELECT DISTINCT metric_name\n")
                .and_then(|rest| rest.strip_suffix("\nORDER BY metric_name"))
                .expect("the narrow statement is its projection around the series read");
            let nested: Vec<String> = body.lines().map(|l| format!("      {l}")).collect();
            assert!(
                wide.contains(&nested.join("\n")),
                "{what}: the wide statement nests the narrow one's read\n{wide}\n{narrow}"
            );
        }
    }

    // --- discovery_fetch_multi (issue #89) ---

    #[test]
    fn discovery_fetch_multi_masks_the_windows_hours() {
        let sql = discovery_fetch_multi(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &["up".to_string()],
            &[Fingerprint::from_raw(7).sql_literal()],
            DataWindow {
                start_ms: 3_600_001,
                end_ms: 7_300_000,
            },
        );
        assert!(sql.contains(
            "AND bitAnd(hours, multiIf(day = '1970-01-01' AND day = '1970-01-01', 6, \
             day = '1970-01-01', 16777214, day = '1970-01-01', 7, 16777215)) != 0\n"
        ));
    }

    /// The window is re-applied in SQL (not inherited from the label
    /// cache's wider resident superset) — the `discovery_series`
    /// no-residency-leak invariant.
    #[test]
    fn discovery_fetch_multi_always_constrains_the_request_window() {
        let sql = discovery_fetch_multi(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &["up".to_string()],
            &[Fingerprint::from_raw(1).sql_literal()],
            window(),
        );
        assert!(sql.contains("AND day BETWEEN "));
        assert!(sql.contains("AND bitAnd(hours, "));
    }

    #[test]
    fn discovery_fetch_multi_metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_series; --";
        let sql = discovery_fetch_multi(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &[payload.to_string()],
            &[Fingerprint::from_raw(1).sql_literal()],
            window(),
        );
        assert!(sql.contains(&format!("metric_name IN ({})", ch_string(payload))));
        assert_no_unescaped_quote(&ch_string(payload));
    }

    // --- distinct_metric_names_probe / discovery_fetch_by_names (issue #96) ---

    fn name_re(value: &str) -> LabelMatcher {
        LabelMatcher {
            key: "__name__".to_string(),
            op: MatchOp::Re,
            value: value.to_string(),
        }
    }

    /// The fan-out's names probe is statement 3 with the name matchers on
    /// the lookup (every name matcher runs there), capped at `cap + 1`.
    #[test]
    fn distinct_metric_names_probe_renders_the_capped_name_predicate_shape() {
        let sql = distinct_metric_names_probe(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &[name_re("up.*")],
            window(),
            2,
        );
        assert_eq!(
            sql,
            "SELECT DISTINCT metric_name\n\
             FROM metric_series\n\
             WHERE org_id = ''\n\
             \x20 AND day BETWEEN '1970-01-01' AND '1970-01-01'\n\
             \x20 AND 0 * match('', '(?-s)^(?:up.*)$') = 0\n\
             \x20 AND bitAnd(hours, multiIf(day = '1970-01-01' AND day = '1970-01-01', 3, \
             day = '1970-01-01', 16777215, day = '1970-01-01', 3, 16777215)) != 0\n\
             \x20 AND fingerprint IN (\n\
             \x20   SELECT fingerprint\n\
             \x20   FROM metric_labels\n\
             \x20   WHERE org_id = ''\n\
             \x20     AND match(metric_name, '(?-s)^(?:up.*)$')\n\
             \x20 )\n\
             ORDER BY metric_name\n\
             LIMIT 3"
        );
    }

    #[test]
    fn distinct_metric_names_probe_renders_neq_and_nre_against_the_metric_name_column() {
        let neq = LabelMatcher {
            key: "__name__".to_string(),
            op: MatchOp::Neq,
            value: "up".to_string(),
        };
        let nre = LabelMatcher {
            key: "__name__".to_string(),
            op: MatchOp::Nre,
            value: "down.*".to_string(),
        };
        let sql = distinct_metric_names_probe(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &[neq, nre],
            window(),
            5,
        );
        assert!(sql.contains("      AND metric_name != 'up'\n"), "{sql}");
        assert!(
            sql.contains("      AND NOT match(metric_name, '(?-s)^(?:down.*)$')\n"),
            "{sql}"
        );
        assert!(!sql.contains("JSONExtractString"));
    }

    /// The limit is `cap + 1` (one extra row detects "more than cap distinct
    /// names matched" without an unbounded set).
    #[test]
    fn distinct_metric_names_probe_limits_to_cap_plus_one() {
        let sql = distinct_metric_names_probe(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &[name_re("x")],
            window(),
            999,
        );
        assert!(sql.ends_with("LIMIT 1000"), "got: {sql}");
    }

    /// Boundary value (issue #96 retroactive re-review): at the maximum
    /// config-accepted `promql_max_metric_fanout`
    /// (`pulsus_config::PROMQL_MAX_METRIC_FANOUT_CEILING`) the `cap + 1`
    /// limit is finite — config load now rejects anything above the
    /// ceiling, so `u64::MAX` is no longer a reachable `fanout_cap`.
    #[test]
    fn distinct_metric_names_probe_does_not_overflow_at_max_fanout() {
        let cap = pulsus_config::PROMQL_MAX_METRIC_FANOUT_CEILING;
        let sql = distinct_metric_names_probe(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &[name_re("x")],
            window(),
            cap,
        );
        assert!(sql.ends_with("LIMIT 1000001"), "got: {sql}");
    }

    /// The probe reads the window's hours, 01:00:00.001 to 02:01:40: hours
    /// 1 and 2 of one day.
    #[test]
    fn distinct_metric_names_probe_masks_the_windows_hours() {
        let sql = distinct_metric_names_probe(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &[name_re("x")],
            DataWindow {
                start_ms: 3_600_001,
                end_ms: 7_300_000,
            },
            10,
        );
        assert!(
            sql.contains(
                "  AND bitAnd(hours, multiIf(day = '1970-01-01' AND day = '1970-01-01', 6, \
                 day = '1970-01-01', 16777214, day = '1970-01-01', 7, 16777215)) != 0\n"
            ),
            "{sql}"
        );
        assert!(!sql.contains("unix_milli"), "{sql}");
    }

    #[test]
    fn distinct_metric_names_probe_name_injection_stays_inside_one_anchored_literal() {
        let payload = r#"up'; DROP TABLE metric_series; --"#;
        let sql = distinct_metric_names_probe(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &[name_re(payload)],
            window(),
            2,
        );
        let expected = anchored_re2_literal_for_test(payload);
        assert_no_unescaped_quote(&expected);
        assert!(sql.contains(&format!("match(metric_name, {expected})")));
    }

    #[test]
    fn discovery_fetch_by_names_has_no_fingerprint_in_component() {
        let sql = discovery_fetch_by_names(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &["up".to_string()],
            &[],
            window(),
        );
        assert!(!sql.contains("fingerprint IN (toUInt128"));
        assert!(sql.contains("metric_name IN ('up')"));
        assert!(sql.contains("AND bitAnd(hours, "));
        assert!(sql.ends_with("ORDER BY metric_name, fingerprint"));
    }

    #[test]
    fn discovery_fetch_by_names_metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_series; --";
        let sql = discovery_fetch_by_names(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &[payload.to_string()],
            &[],
            window(),
        );
        assert!(sql.contains(&format!("metric_name IN ({})", ch_string(payload))));
        assert_no_unescaped_quote(&ch_string(payload));
    }

    // --- metadata_query (issue #32) ---

    /// Issue #603: one aggregate over the whole descriptor tuple. Three
    /// independent `argMax` calls can resolve column by column on an
    /// `updated_ns` tie and answer a descriptor assembled from two rows; the
    /// substring count is what catches that, and the whole statement is
    /// written out so the shape cannot drift silently.
    #[test]
    fn metadata_query_aggregates_the_descriptor_as_one_tuple() {
        let sql = metadata_query(&no_tenant(), "metric_metadata", None, None);
        assert_eq!(
            sql,
            "SELECT metric_name, tupleElement(d, 1) AS metric_type, \
             tupleElement(d, 2) AS help, tupleElement(d, 3) AS unit\n\
             FROM (SELECT metric_name, argMax((metric_type, help, unit), updated_ns) AS d\n\
             FROM metric_metadata\n\
             WHERE org_id = ''\n\
             GROUP BY metric_name)\n\
             ORDER BY metric_name"
        );
        assert_eq!(
            sql.matches("argMax(").count(),
            1,
            "one aggregate, so one row wins whole: {sql}"
        );
        assert!(!sql.contains("metric_name ="));
        assert!(!sql.contains("LIMIT"));

        let filtered = metadata_query(&no_tenant(), "metric_metadata", Some("up"), Some(10));
        assert_eq!(
            filtered,
            "SELECT metric_name, tupleElement(d, 1) AS metric_type, \
             tupleElement(d, 2) AS help, tupleElement(d, 3) AS unit\n\
             FROM (SELECT metric_name, argMax((metric_type, help, unit), updated_ns) AS d\n\
             FROM metric_metadata\n\
             WHERE org_id = ''\n\
             \x20 AND metric_name = 'up'\n\
             GROUP BY metric_name)\n\
             ORDER BY metric_name\n\
             LIMIT 10"
        );
        assert_eq!(filtered.matches("argMax(").count(), 1);
    }

    #[test]
    fn metadata_query_filters_on_the_given_metric_name() {
        let sql = metadata_query(&no_tenant(), "metric_metadata", Some("up"), None);
        assert!(sql.contains("\n  AND metric_name = 'up'"));
    }

    #[test]
    fn metadata_query_applies_the_given_limit() {
        let sql = metadata_query(&no_tenant(), "metric_metadata", None, Some(10));
        assert!(sql.ends_with("LIMIT 10"));
    }

    #[test]
    fn metadata_query_metric_name_injection_stays_inside_one_literal() {
        let payload = "up'; DROP TABLE metric_metadata; --";
        let sql = metadata_query(&no_tenant(), "metric_metadata", Some(payload), None);
        assert!(sql.contains(&format!("AND metric_name = {}", ch_string(payload))));
        assert_no_unescaped_quote(&ch_string(payload));
    }

    // -- issue #623: labels come from the label table --------------------

    /// U7 (issue #623): every builder that takes a metric name reads only
    /// that metric's label rows — the line after each `FROM metric_labels`
    /// names `metric_name`.
    #[test]
    fn every_label_reading_builder_names_the_label_table() {
        let filter = DiscoveryFilter {
            metric_name: Some("up".to_string()),
            name_matchers: Vec::new(),
            matchers: vec![eq("job", "api")],
        };
        let fps = [Fingerprint::from_raw(7).sql_literal()];
        for sql in [
            historical_series_subquery(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                "up",
                window(),
                &[eq("job", "api")],
            ),
            historical_resolution_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                "up",
                window(),
                &[eq("job", "api")],
            ),
            discovery_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &filter,
                window(),
            ),
            discovery_distinct_names_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &filter,
                window(),
            ),
            discovery_fetch_multi(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &["up".to_string()],
                &fps,
                window(),
            ),
            discovery_fetch_by_names(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &["up".to_string()],
                &[eq("job", "api")],
                window(),
            ),
            series_labels_by_fingerprint(&no_tenant(), "metric_labels", &["up".to_string()], &fps),
        ] {
            let mut lines = sql.lines();
            let mut reads = 0;
            while let Some(line) = lines.next() {
                if line.trim() == "FROM metric_labels" {
                    reads += 1;
                    // Issue #635 part 4: the tenant, then the name, as the
                    // key orders them.
                    let tenant = lines.next().unwrap_or_default();
                    assert!(tenant.contains("org_id = ''"), "{tenant:?} in {sql}");
                    let next = lines.next().unwrap_or_default();
                    assert!(next.contains("metric_name"), "{next:?} in {sql}");
                }
            }
            assert!(reads > 0, "no label read in {sql}");
        }
    }

    /// The design's §4 window: 2026-09-07 22:30 to 23:30 UTC.
    fn evening() -> DataWindow {
        DataWindow {
            start_ms: 1_788_820_200_000,
            end_ms: 1_788_823_800_000,
        }
    }

    const MASK: &str = "multiIf(day = '2026-09-07' AND day = '2026-09-07', 12582912, \
                        day = '2026-09-07', 12582912, day = '2026-09-07', 16777215, 16777215)";

    fn ids() -> [FpLiteral; 2] {
        [
            Fingerprint::from_raw(101).sql_literal(),
            Fingerprint::from_raw(205).sql_literal(),
        ]
    }

    /// **K2 (issue #623): the four statements, byte for byte.** The
    /// design's selector `up{job="api", status=~"5.."}` over its window:
    /// statement 1 (series IDs), statement 2 (series with labels) through
    /// both builders that render it for one name, statement 3 (names) with
    /// matchers and without, statement 4 (labels for IDs the window already
    /// bounds), and the two `IN`-scoped forms of statement 2. No statement
    /// joins.
    #[test]
    fn the_four_statements_are_byte_exact() {
        let matchers = [eq("job", "api"), re("status", "5..")];
        let s1 = format!(
            "SELECT fingerprint\n\
             FROM metric_series\n\
             WHERE org_id = ''\n\
             \x20 AND day BETWEEN '2026-09-07' AND '2026-09-07'\n\
             \x20 AND 0 * match('', '(?-s)^(?:5..)$') = 0\n\
             \x20 AND bitAnd(hours, {MASK}) != 0\n\
             \x20 AND fingerprint IN (\n\
             \x20   SELECT fingerprint\n\
             \x20   FROM metric_labels\n\
             \x20   WHERE org_id = ''\n\
             \x20     AND metric_name = 'up'\n\
             \x20     AND JSONExtractString(labels, 'job') = 'api'\n\
             \x20     AND match(JSONExtractString(labels, 'status'), '(?-s)^(?:5..)$')\n\
             \x20 )"
        );
        assert_eq!(
            historical_series_subquery(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                "up",
                evening(),
                &matchers
            ),
            s1,
            "statement 1"
        );
        let s2 = format!(
            "SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels\n\
             FROM (\n\
             \x20 SELECT fingerprint, metric_name AS name, labels AS label_text\n\
             \x20 FROM metric_labels\n\
             \x20 WHERE org_id = ''\n\
             \x20   AND metric_name = 'up'\n\
             \x20   AND JSONExtractString(labels, 'job') = 'api'\n\
             \x20   AND match(JSONExtractString(labels, 'status'), '(?-s)^(?:5..)$')\n\
             \x20   AND fingerprint IN (\n\
             \x20     SELECT fingerprint\n\
             \x20     FROM metric_series\n\
             \x20     WHERE org_id = ''\n\
             \x20       AND day BETWEEN '2026-09-07' AND '2026-09-07'\n\
             \x20       AND 0 * match('', '(?-s)^(?:5..)$') = 0\n\
             \x20       AND bitAnd(hours, {MASK}) != 0\n\
             \x20       AND fingerprint IN (\n\
             \x20         SELECT fingerprint\n\
             \x20         FROM metric_labels\n\
             \x20         WHERE org_id = ''\n\
             \x20           AND metric_name = 'up'\n\
             \x20           AND JSONExtractString(labels, 'job') = 'api'\n\
             \x20           AND match(JSONExtractString(labels, 'status'), '(?-s)^(?:5..)$')\n\
             \x20       )\n\
             \x20   )\n\
             )\n\
             GROUP BY fingerprint\n\
             ORDER BY metric_name, fingerprint"
        );
        assert_eq!(
            historical_resolution_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                "up",
                evening(),
                &matchers
            ),
            s2,
            "statement 2, the resolution query"
        );
        let named = DiscoveryFilter {
            metric_name: Some("up".to_string()),
            name_matchers: vec![],
            matchers: matchers.to_vec(),
        };
        assert_eq!(
            discovery_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &named,
                evening()
            ),
            s2,
            "statement 2, named discovery"
        );
        let unnamed = DiscoveryFilter {
            metric_name: None,
            name_matchers: vec![],
            matchers: matchers.to_vec(),
        };
        assert_eq!(
            discovery_distinct_names_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &unnamed,
                evening()
            ),
            format!(
                "SELECT DISTINCT metric_name\n\
                 FROM metric_series\n\
                 WHERE org_id = ''\n\
                 \x20 AND day BETWEEN '2026-09-07' AND '2026-09-07'\n\
                 \x20 AND 0 * match('', '(?-s)^(?:5..)$') = 0\n\
                 \x20 AND bitAnd(hours, {MASK}) != 0\n\
                 \x20 AND fingerprint IN (\n\
                 \x20   SELECT fingerprint\n\
                 \x20   FROM metric_labels\n\
                 \x20   WHERE org_id = ''\n\
                 \x20     AND JSONExtractString(labels, 'job') = 'api'\n\
                 \x20     AND match(JSONExtractString(labels, 'status'), '(?-s)^(?:5..)$')\n\
                 \x20 )\n\
                 ORDER BY metric_name"
            ),
            "statement 3 with matchers"
        );
        assert_eq!(
            discovery_distinct_names_query(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &DiscoveryFilter::default(),
                evening()
            ),
            format!(
                "SELECT DISTINCT metric_name\n\
                 FROM metric_series\n\
                 WHERE org_id = ''\n\
                 \x20 AND day BETWEEN '2026-09-07' AND '2026-09-07'\n\
                 \x20 AND bitAnd(hours, {MASK}) != 0\n\
                 ORDER BY metric_name"
            ),
            "statement 3 with no matcher"
        );
        let two_names = ["up".to_string(), "down".to_string()];
        assert_eq!(
            series_labels_by_fingerprint(&no_tenant(), "metric_labels", &two_names, &ids()),
            "SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels\n\
             FROM (\n\
             \x20 SELECT fingerprint, metric_name AS name, labels AS label_text\n\
             \x20 FROM metric_labels\n\
             \x20 WHERE org_id = ''\n\
             \x20   AND metric_name IN ('up', 'down')\n\
             \x20   AND fingerprint IN (toUInt128('101'), toUInt128('205'))\n\
             )\n\
             GROUP BY fingerprint\n\
             ORDER BY metric_name, fingerprint",
            "statement 4"
        );
        assert_eq!(
            discovery_fetch_multi(
                &no_tenant(),
                "metric_series",
                "metric_labels",
                &two_names,
                &ids(),
                evening()
            ),
            format!(
                "SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels\n\
                 FROM (\n\
                 \x20 SELECT fingerprint, metric_name AS name, labels AS label_text\n\
                 \x20 FROM metric_labels\n\
                 \x20 WHERE org_id = ''\n\
                 \x20   AND metric_name IN ('up', 'down')\n\
                 \x20   AND fingerprint IN (toUInt128('101'), toUInt128('205'))\n\
                 \x20   AND fingerprint IN (\n\
                 \x20     SELECT fingerprint\n\
                 \x20     FROM metric_series\n\
                 \x20     WHERE org_id = ''\n\
                 \x20       AND day BETWEEN '2026-09-07' AND '2026-09-07'\n\
                 \x20       AND bitAnd(hours, {MASK}) != 0\n\
                 \x20       AND fingerprint IN (\n\
                 \x20         SELECT fingerprint\n\
                 \x20         FROM metric_labels\n\
                 \x20         WHERE org_id = ''\n\
                 \x20           AND metric_name IN ('up', 'down')\n\
                 \x20           AND fingerprint IN (toUInt128('101'), toUInt128('205'))\n\
                 \x20       )\n\
                 \x20   )\n\
                 )\n\
                 GROUP BY fingerprint\n\
                 ORDER BY metric_name, fingerprint"
            ),
            "the fan-out: statement 2, scoped to the cache's names and IDs"
        );
        let by_names = discovery_fetch_by_names(
            &no_tenant(),
            "metric_series",
            "metric_labels",
            &two_names,
            &[eq("job", "api")],
            evening(),
        );
        assert!(
            by_names.contains(
                "\n  WHERE org_id = ''\n    AND metric_name IN ('up', 'down')\n    AND JSONExtractString(labels, 'job') = 'api'\n"
            ),
            "{by_names}"
        );
        for sql in [
            s1,
            s2,
            by_names,
            sweep_query(&no_tenant(), "metric_series", "metric_labels", evening()),
        ] {
            assert!(!sql.contains("JOIN"), "{sql}");
        }
    }

    /// Issue #635 part 3, U2: the design's statement R and statement 1 for
    /// `{job="api", status=~"5.."}` over a 15-minute range at 60 s ending
    /// at 1791409260000, lookback 5 minutes.
    #[test]
    fn the_nameless_builders_render_statement_r_and_statement_1() {
        let t = SeriesTables {
            series: "metric_series",
            labels: "metric_labels",
            label_index: Some("metric_label_index"),
            label_values: Some("metric_label_values"),
        };
        let matchers = [
            eq("job", "api"),
            LabelMatcher {
                key: "status".to_string(),
                op: MatchOp::Re,
                value: "5..".to_string(),
            },
        ];
        // Issue #635 part 4: the design's scoped statements, as tenant-q.
        let header = http::HeaderValue::from_static("tenant-q");
        let tenant_q = Tenant::from_header(Some(&header), false).expect("a tenant");
        let window = DataWindow {
            start_ms: 1_791_408_060_000,
            end_ms: 1_791_409_260_000,
        };
        assert_eq!(
            nameless_resolution_query(&tenant_q, t, &matchers, window, 50_000),
            "SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels
FROM (
  SELECT fingerprint, metric_name AS name, labels AS label_text
  FROM metric_labels
  WHERE org_id = 'tenant-q'
    AND fingerprint IN (
      SELECT fingerprint
      FROM metric_series
      WHERE org_id = 'tenant-q'
        AND day BETWEEN '2026-10-07' AND '2026-10-07'
        AND 0 * match('', '(?-s)^(?:5..)$') = 0
        AND bitAnd(hours, multiIf(day = '2026-10-07' AND day = '2026-10-07', 2097152, day = '2026-10-07', 14680064, day = '2026-10-07', 4194303, 16777215)) != 0
        AND fingerprint IN (
          SELECT fingerprint FROM metric_label_index WHERE org_id = 'tenant-q' AND key = 'job' AND value = 'api'
          INTERSECT
          SELECT fingerprint FROM metric_label_index WHERE org_id = 'tenant-q' AND key = 'status' AND value IN (SELECT value FROM metric_label_values WHERE org_id = 'tenant-q' AND key = 'status' AND match(value, '(?-s)^(?:5..)$'))
        )
    )
)
GROUP BY fingerprint
ORDER BY metric_name, fingerprint
LIMIT 50001",
            "statement R"
        );
        assert_eq!(
            nameless_ids_query(&tenant_q, t, &matchers, window),
            "SELECT fingerprint
FROM metric_series
WHERE org_id = 'tenant-q'
  AND day BETWEEN '2026-10-07' AND '2026-10-07'
  AND 0 * match('', '(?-s)^(?:5..)$') = 0
  AND bitAnd(hours, multiIf(day = '2026-10-07' AND day = '2026-10-07', 2097152, day = '2026-10-07', 14680064, day = '2026-10-07', 4194303, 16777215)) != 0
  AND fingerprint IN (
    SELECT fingerprint FROM metric_label_index WHERE org_id = 'tenant-q' AND key = 'job' AND value = 'api'
    INTERSECT
    SELECT fingerprint FROM metric_label_index WHERE org_id = 'tenant-q' AND key = 'status' AND value IN (SELECT value FROM metric_label_values WHERE org_id = 'tenant-q' AND key = 'status' AND match(value, '(?-s)^(?:5..)$'))
  )",
            "statement 1"
        );
    }
}
