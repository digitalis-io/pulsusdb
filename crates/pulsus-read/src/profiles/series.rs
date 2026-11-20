//! Issue #824: `SelectSeries` planned as one statement (S6) — the per-step
//! time series of profile totals, one row per series, bucketed and
//! aggregated in ClickHouse. [`series`] orders the rows as the reference
//! orders its series and applies the limit.
//!
//! Without a call site the statement reads `profile_entries`, joined to the
//! selected series' labels when grouped. With one it reads each profile's
//! total over the stacks beginning with the call site from
//! `profile_samples`.
//!
//! Issue #826: with exemplars, each point also names its largest profile
//! (`argMax`), and a second `UNION ALL` branch of the same statement
//! returns each selected series' labels; [`series_exemplars`] joins them.

use std::collections::HashMap;

use pulsus_model::{LabelMatcher, Tenant};
use serde::{Deserialize, Serialize};

use crate::logql::escape::ch_string;
use crate::metrics::DataWindow;
use crate::traces::tenant_scope::TenantTerm;

use super::engine::{ProfilesReadConfig, ProfilesReadError};
use super::selector::{Compiled, SelectorError, SelectorRequest, compile};
use super::tree::{STACK_DAY_AFTER_MS, STACK_DAY_BEFORE_MS, call_site_predicate, utc_date};

/// What a `SelectSeries` request selects.
#[derive(Debug, Clone, Copy, Default)]
pub struct SeriesRequest<'a> {
    pub profile_type: &'a str,
    pub label_selector: &'a str,
    pub start_ms: i64,
    pub end_ms: i64,
    /// The step in seconds.
    pub step: f64,
    /// The labels each series is grouped by, in request order.
    pub group_by: &'a [String],
    /// `AVERAGE`: each point is the mean of its profiles' totals.
    pub average: bool,
    /// `stack_trace_selector.call_site[].name`, root first.
    pub call_site: &'a [String],
    /// The request's `limit`, `None` when absent.
    pub limit: Option<i64>,
    /// Issue #826: `EXEMPLAR_TYPE_INDIVIDUAL`: each point names its
    /// largest profile.
    pub exemplars: bool,
}

/// One series as S6 returns it: its `group_by` values, the sum of its
/// points, and its points as `(timestamp ms, value)` in time order.
#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
pub struct TimeSeriesRow {
    pub g: Vec<String>,
    pub series_total: f64,
    pub points: Vec<(i64, f64)>,
}

/// Issue #826: one row of S6 with exemplars. `kind` 0 is a series as
/// [`TimeSeriesRow`], each point `(timestamp ms, value, the largest
/// profile's total, its id, its time ms, its fingerprint)`; `kind` 1 is one
/// selected series' `fingerprint` and its stored `labels` less the
/// `group_by` names.
#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
pub struct TimeSeriesExemplarRow {
    pub kind: u8,
    pub g: Vec<String>,
    pub series_total: f64,
    pub points: Vec<(i64, f64, i64, String, i64, u128)>,
    pub fingerprint: u128,
    pub labels: Vec<(String, String)>,
}

/// Issue #826: one profile that contributed to a point or a heatmap cell.
/// `span_id` and `trace_id` are `""` for an `INDIVIDUAL` exemplar,
/// `profile_id` for a `SPAN` one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Exemplar {
    pub timestamp: i64,
    pub profile_id: String,
    pub span_id: String,
    pub trace_id: String,
    pub value: i64,
    pub labels: Vec<(String, String)>,
}

/// One point of a series.
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    pub timestamp: i64,
    pub value: f64,
    /// Issue #826: the point's largest profile, when exemplars are asked
    /// for.
    pub exemplar: Option<Exemplar>,
}

/// One series of the answer: its labels in `group_by` order, a missing one
/// dropped, and its points in time order.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub labels: Vec<(String, String)>,
    pub points: Vec<Point>,
}

/// The step in milliseconds as the reference computes it: refused below
/// 1 ms (and NaN), then the step's nanoseconds truncated, then whole
/// milliseconds.
pub fn step_ms(step: f64) -> Result<i64, ProfilesReadError> {
    if step.is_nan() || step < 0.001 {
        return Err(ProfilesReadError::InvalidArgument(
            "step must be >= 1ms".to_string(),
        ));
    }
    Ok((step * 1e9) as i64 / 1_000_000)
}

/// Plans `req` for `tenant`: `Ok(None)` when its window ends before the
/// lookback floor, so nothing is read. In the reference's order, except
/// that the step is checked after the selector: the window, the type and
/// the selector, then the step.
pub fn plan(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    req: &SeriesRequest<'_>,
    now_ms: i64,
) -> Result<Option<String>, ProfilesReadError> {
    let Some((window, matchers)) = select(
        tenant,
        config,
        req.profile_type,
        req.label_selector,
        req.start_ms,
        req.end_ms,
        now_ms,
    )?
    else {
        return Ok(None);
    };
    let step = step_ms(req.step)?;

    let start = window.start_ms;
    let end = window.end_ms;
    let read_start = start.saturating_sub(step);
    let last = start + (end - start) / step * step;
    let lo_ns = read_start.saturating_mul(1_000_000);
    let hi_ns = end
        .saturating_mul(1_000_000)
        .min(last.saturating_mul(1_000_000).saturating_add(999_999));
    let w = DataWindow {
        start_ms: read_start,
        end_ms: end,
    };
    let series_table = config.series_table.as_str();
    let series = crate::metrics::profile_series(tenant, w, series_table, &matchers);
    let tenant_term = TenantTerm::of(tenant);
    let type_id = matchers
        .iter()
        .rev()
        .find(|m| m.key == "__profile_type__")
        .map(|m| m.value.as_str())
        .unwrap_or_default();
    let type_id = ch_string(type_id);
    let service_terms = crate::metrics::profile_service_terms(&matchers);

    let (g, join) = group(tenant, w, series_table, &matchers, req.group_by);
    let source = if req.call_site.is_empty() {
        format!(
            "FROM {entries} AS e{join}
  WHERE {tenant_term} AND e.type_id = {type_id}{service_terms}
      AND e.fingerprint IN ({series})
      AND e.timestamp_ns >= {lo_ns} AND e.timestamp_ns <= {hi_ns}",
            entries = config.entries_table,
        )
    } else {
        format!(
            "FROM (
    SELECT fingerprint, timestamp_ns, {profile}sum(value) AS total
    FROM {samples}
    WHERE {tenant_term} AND type_id = {type_id}{service_terms}
      AND fingerprint IN ({series})
      AND timestamp_ns >= {lo_ns} AND timestamp_ns <= {hi_ns}
      AND stack_id IN (SELECT stack_id
FROM profile_stacks
WHERE {tenant_term} AND day BETWEEN '{d0}' AND '{d1}'
  AND {call_site})
    GROUP BY fingerprint, timestamp_ns, profile_id
  ) AS e{join}",
            samples = config.samples_table,
            d0 = utc_date(read_start.saturating_sub(STACK_DAY_BEFORE_MS)),
            d1 = utc_date(end.saturating_add(STACK_DAY_AFTER_MS)),
            call_site = call_site_predicate("names", req.call_site),
            profile = if req.exemplars { "profile_id, " } else { "" },
        )
    };
    let v = if req.average {
        "avg(e.total)"
    } else {
        "toFloat64(sum(e.total))"
    };
    let limit = match req.limit {
        Some(n) if n > 0 => format!("\nLIMIT {n}"),
        _ => String::new(),
    };
    if !req.exemplars {
        return Ok(Some(format!(
            "SELECT g, sum(v) AS series_total, arraySort(groupArray((ts, v))) AS points
FROM (
  SELECT {g} AS g,
         {start} + intDiv(intDiv(e.timestamp_ns, 1000000) - {start} + {step_minus_1}, {step}) * {step} AS ts,
         {v} AS v
  {source}
  GROUP BY g, ts
)
GROUP BY g
ORDER BY series_total DESC, g{limit}",
            step_minus_1 = step - 1,
        )));
    }
    let labels = labels_branch(tenant, w, series_table, &matchers, req.group_by, "");
    Ok(Some(format!(
        "SELECT 0 AS kind, g, sum(v) AS series_total, arraySort(groupArray((ts, v, m, toString(x.1), intDiv(x.2, 1000000), x.3))) AS points, toUInt128(0) AS fingerprint, CAST([] AS Array(Tuple(String, String))) AS labels
FROM (
  SELECT {g} AS g,
         {start} + intDiv(intDiv(e.timestamp_ns, 1000000) - {start} + {step_minus_1}, {step}) * {step} AS ts,
         {v} AS v,
         max(e.total) AS m,
         argMax((e.profile_id, e.timestamp_ns, e.fingerprint), e.total) AS x
  {source}
  GROUP BY g, ts
)
GROUP BY g
ORDER BY series_total DESC, g{limit}{labels}",
        step_minus_1 = step - 1,
    )))
}

/// Issue #826: the `UNION ALL` branch that returns each selected series'
/// fingerprint and its stored labels less the `group_by` names, padded to
/// the first branch's columns with `pad` after the empty points.
pub(super) fn labels_branch(
    tenant: &Tenant,
    w: DataWindow,
    series_table: &str,
    matchers: &[LabelMatcher],
    group_by: &[String],
    pad: &str,
) -> String {
    let all = "JSONExtractKeysAndValues(any(labels), 'String')";
    let labels = if group_by.is_empty() {
        all.to_string()
    } else {
        let names: Vec<String> = group_by.iter().map(|l| ch_string(l)).collect();
        format!(
            "arrayFilter(p -> NOT has([{}], p.1), {all})",
            names.join(", ")
        )
    };
    format!(
        "\nUNION ALL\nSELECT 1, CAST([] AS Array(String)), 0, [], {pad}fingerprint, {labels}\n{}\nGROUP BY fingerprint",
        crate::metrics::profile_series_where(tenant, w, series_table, matchers)
    )
}

/// The window and the matchers `profile_type`, `label_selector` and
/// `[start_ms, end_ms]` select for `tenant`, checked in the reference's
/// order (the window, the type, the selector); `None` when the window ends
/// before the lookback floor.
pub(super) fn select(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    profile_type: &str,
    label_selector: &str,
    start_ms: i64,
    end_ms: i64,
    now_ms: i64,
) -> Result<Option<(DataWindow, Vec<LabelMatcher>)>, ProfilesReadError> {
    let selectors = [label_selector];
    let compiled = compile(
        tenant,
        &config.series_table,
        &SelectorRequest {
            selectors: &selectors,
            profile_type: Some(profile_type),
            start_ms,
            end_ms,
        },
        now_ms,
        config.limits,
    )
    .map_err(|SelectorError::InvalidArgument(m)| ProfilesReadError::InvalidArgument(m))?;
    Ok(match compiled {
        Compiled::Empty => None,
        Compiled::Series {
            window, matchers, ..
        } => Some((window, matchers)),
    })
}

/// A grouped read's `g` and its join to the selected series' labels over
/// `w`: without `group_by`, the empty array and no join.
pub(super) fn group(
    tenant: &Tenant,
    w: DataWindow,
    series_table: &str,
    matchers: &[LabelMatcher],
    group_by: &[String],
) -> (String, String) {
    if group_by.is_empty() {
        return ("CAST([] AS Array(String))".to_string(), String::new());
    }
    let labels: Vec<String> = group_by
        .iter()
        .map(|l| format!("JSONExtractString(labels, {})", ch_string(l)))
        .collect();
    let join = format!(
        "\n  INNER JOIN (\n    SELECT fingerprint, any([{}]) AS g\n{}\n    GROUP BY fingerprint\n  ) AS s ON e.fingerprint = s.fingerprint",
        labels.join(", "),
        crate::metrics::profile_series_where(tenant, w, series_table, matchers)
    );
    ("s.g".to_string(), join)
}

/// The rows as the reference answers them: each series labelled by the
/// `group_by` names it holds a value for, in request order; ordered by the
/// sum of its points, descending, then by its labels; cut to `limit` when
/// it is above 0.
pub fn series(rows: Vec<TimeSeriesRow>, group_by: &[String], limit: Option<i64>) -> Vec<Series> {
    order_and_cut(
        rows.into_iter()
            .map(|row| {
                let points = row
                    .points
                    .into_iter()
                    .map(|(timestamp, value)| Point {
                        timestamp,
                        value,
                        exemplar: None,
                    })
                    .collect();
                Series {
                    labels: group_labels(group_by, row.g),
                    points,
                }
            })
            .collect(),
        limit,
    )
}

/// Issue #826: S6's exemplar rows as [`series`] answers them, each point
/// carrying its largest profile, labelled with the `kind` 1 row of its
/// fingerprint.
pub fn series_exemplars(
    rows: Vec<TimeSeriesExemplarRow>,
    group_by: &[String],
    limit: Option<i64>,
) -> Vec<Series> {
    let (rows, labels) = split_labels(rows, |r| r.kind == 0, |r| (r.fingerprint, r.labels));
    order_and_cut(
        rows.into_iter()
            .map(|row| {
                let points = row
                    .points
                    .into_iter()
                    .map(|(timestamp, value, total, profile_id, at, fp)| Point {
                        timestamp,
                        value,
                        exemplar: Some(Exemplar {
                            timestamp: at,
                            profile_id,
                            span_id: String::new(),
                            trace_id: String::new(),
                            value: total,
                            labels: labels.get(&fp).cloned().unwrap_or_default(),
                        }),
                    })
                    .collect();
                Series {
                    labels: group_labels(group_by, row.g),
                    points,
                }
            })
            .collect(),
        limit,
    )
}

/// Issue #826: each selected series' labels by its fingerprint.
pub(super) type LabelsByFingerprint = HashMap<u128, Vec<(String, String)>>;

/// Issue #826: an exemplar statement's rows split into the series rows,
/// those `is_series` holds for, and the map each other row's
/// `(fingerprint, labels)` makes. Each label list takes the reference's
/// order: `__profile_type__` first, `__service_name__` second, the rest by
/// name.
pub(super) fn split_labels<R>(
    rows: Vec<R>,
    is_series: impl Fn(&R) -> bool,
    labels: impl Fn(R) -> (u128, Vec<(String, String)>),
) -> (Vec<R>, LabelsByFingerprint) {
    let (series, other): (Vec<R>, Vec<R>) = rows.into_iter().partition(|r| is_series(r));
    let by_fingerprint = other
        .into_iter()
        .map(|r| {
            let (fingerprint, mut pairs) = labels(r);
            pairs.sort_by(|(a, _), (b, _)| {
                let key = |n: &str| (n != "__profile_type__", n != "__service_name__");
                key(a).cmp(&key(b)).then_with(|| a.cmp(b))
            });
            (fingerprint, pairs)
        })
        .collect();
    (series, by_fingerprint)
}

/// A series' labels: the `group_by` names it holds a value for, in request
/// order.
pub(super) fn group_labels(group_by: &[String], g: Vec<String>) -> Vec<(String, String)> {
    group_by
        .iter()
        .zip(g)
        .filter(|(_, value)| !value.is_empty())
        .map(|(name, value)| (name.clone(), value))
        .collect()
}

/// The series ordered by the sum of their points, descending, then by
/// their labels; cut to `limit` when it is above 0.
fn order_and_cut(series: Vec<Series>, limit: Option<i64>) -> Vec<Series> {
    let mut out: Vec<(f64, Series)> = series
        .into_iter()
        .map(|s| (s.points.iter().fold(0.0, |acc, p| acc + p.value), s))
        .collect();
    out.sort_by(|(a_sum, a), (b_sum, b)| {
        b_sum
            .partial_cmp(a_sum)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| compare_label_pairs(&a.labels, &b.labels))
    });
    let mut out: Vec<Series> = out.into_iter().map(|(_, s)| s).collect();
    if let Some(k) = limit.filter(|k| *k > 0) {
        out.truncate(usize::try_from(k).unwrap_or(usize::MAX));
    }
    out
}

/// Two label sets compared pairwise, name then value, in byte order; when
/// one is a prefix of the other, the shorter first.
pub(super) fn compare_label_pairs(
    a: &[(String, String)],
    b: &[(String, String)],
) -> std::cmp::Ordering {
    for ((an, av), (bn, bv)) in a.iter().zip(b) {
        let c = an
            .as_bytes()
            .cmp(bn.as_bytes())
            .then_with(|| av.as_bytes().cmp(bv.as_bytes()));
        if c.is_ne() {
            return c;
        }
    }
    a.len().cmp(&b.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiles::selector::WindowLimits;
    use crate::profiles::tenant_scope;

    const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    /// 2026-11-16T09:00Z and 10:00Z.
    const START: i64 = 1_794_819_600_000;
    const END: i64 = 1_794_823_200_000;
    const DAY_MS: i64 = 86_400_000;
    const HOUR_MS: i64 = 3_600_000;

    /// Plan §0's statement.
    const EXAMPLE: &str = "SELECT g, sum(v) AS series_total, arraySort(groupArray((ts, v))) AS points
FROM (
  SELECT s.g AS g,
         1794819600000 + intDiv(intDiv(e.timestamp_ns, 1000000) - 1794819600000 + 14999, 15000) * 15000 AS ts,
         toFloat64(sum(e.total)) AS v
  FROM profile_entries AS e
  INNER JOIN (
    SELECT fingerprint, any([JSONExtractString(labels, 'service_name')]) AS g
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1792, day = '2026-11-16', 16776960, day = '2026-11-16', 2047, 16777215)) != 0
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
    GROUP BY fingerprint
  ) AS s ON e.fingerprint = s.fingerprint
  WHERE org_id = 't1' AND e.type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
      AND e.fingerprint IN (SELECT fingerprint
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1792, day = '2026-11-16', 16776960, day = '2026-11-16', 2047, 16777215)) != 0
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds')
      AND e.timestamp_ns >= 1794819585000000000 AND e.timestamp_ns <= 1794823200000000000
  GROUP BY g, ts
)
GROUP BY g
ORDER BY series_total DESC, g";

    /// Plan §0's statement with exemplars (issue #826).
    const EXAMPLE_EXEMPLARS: &str = "SELECT 0 AS kind, g, sum(v) AS series_total, arraySort(groupArray((ts, v, m, toString(x.1), intDiv(x.2, 1000000), x.3))) AS points, toUInt128(0) AS fingerprint, CAST([] AS Array(Tuple(String, String))) AS labels
FROM (
  SELECT s.g AS g,
         1794819600000 + intDiv(intDiv(e.timestamp_ns, 1000000) - 1794819600000 + 14999, 15000) * 15000 AS ts,
         toFloat64(sum(e.total)) AS v,
         max(e.total) AS m,
         argMax((e.profile_id, e.timestamp_ns, e.fingerprint), e.total) AS x
  FROM profile_entries AS e
  INNER JOIN (
    SELECT fingerprint, any([JSONExtractString(labels, 'service_name')]) AS g
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1792, day = '2026-11-16', 16776960, day = '2026-11-16', 2047, 16777215)) != 0
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
    GROUP BY fingerprint
  ) AS s ON e.fingerprint = s.fingerprint
  WHERE org_id = 't1' AND e.type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
      AND e.fingerprint IN (SELECT fingerprint
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1792, day = '2026-11-16', 16776960, day = '2026-11-16', 2047, 16777215)) != 0
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds')
      AND e.timestamp_ns >= 1794819585000000000 AND e.timestamp_ns <= 1794823200000000000
  GROUP BY g, ts
)
GROUP BY g
ORDER BY series_total DESC, g
UNION ALL
SELECT 1, CAST([] AS Array(String)), 0, [], fingerprint, arrayFilter(p -> NOT has(['service_name'], p.1), JSONExtractKeysAndValues(any(labels), 'String'))
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1792, day = '2026-11-16', 16776960, day = '2026-11-16', 2047, 16777215)) != 0
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
GROUP BY fingerprint";

    fn tenant(name: &str) -> Tenant {
        let value = http::HeaderValue::from_str(name).expect("a header value");
        Tenant::from_header(Some(&value), false).expect("a valid tenant")
    }

    fn single() -> ProfilesReadConfig {
        ProfilesReadConfig {
            series_table: "profile_series".to_string(),
            samples_table: "profile_samples".to_string(),
            entries_table: "profile_entries".to_string(),
            dist_suffix: String::new(),
            limits: WindowLimits::default(),
            scan_budget_rows: 50_000_000,
            max_tree_nodes: 4_194_304,
            max_result_bytes: 268_435_456,
            query_memory_bytes: 4_294_967_296,
            distributed: false,
            skip_unavailable_shards: false,
            max_nodes_default: 8_192,
            max_nodes_max: 1_048_576,
        }
    }

    fn clustered() -> ProfilesReadConfig {
        ProfilesReadConfig {
            series_table: "profile_series_dist".to_string(),
            samples_table: "profile_samples_dist".to_string(),
            entries_table: "profile_entries_dist".to_string(),
            dist_suffix: "_dist".to_string(),
            distributed: true,
            ..single()
        }
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn by_service() -> Vec<String> {
        strings(&["service_name"])
    }

    /// Plan §0's request with `group_by`.
    fn example(group_by: &[String]) -> SeriesRequest<'_> {
        SeriesRequest {
            profile_type: CPU,
            label_selector: "{}",
            start_ms: START,
            end_ms: END,
            step: 15.0,
            group_by,
            ..SeriesRequest::default()
        }
    }

    fn planned(config: &ProfilesReadConfig, req: &SeriesRequest<'_>, now: i64) -> String {
        match plan(&tenant("t1"), config, req, now) {
            Ok(Some(sql)) => sql,
            other => panic!("expected a plan, got {other:?}"),
        }
    }

    /// S1: plan §0's request renders plan §0's statement, byte for byte.
    #[test]
    fn s1_the_example_renders_the_example_statement() {
        let g = by_service();
        assert_eq!(planned(&single(), &example(&g), END), EXAMPLE);
    }

    /// S2: clustered, the entries and both series reads name the wrappers.
    #[test]
    fn s2_clustered_reads_the_wrappers() {
        let g = by_service();
        let sql = planned(&clustered(), &example(&g), END);
        assert!(sql.contains("FROM profile_entries_dist AS e"), "{sql}");
        assert_eq!(
            sql.matches("FROM profile_series_dist\n").count(),
            2,
            "{sql}"
        );
    }

    /// S3: the tenant check accepts Form E with and without the join, and
    /// Form C, for two tenants.
    #[test]
    fn s3_the_tenant_check_accepts_every_form() {
        let g = by_service();
        let call_site = strings(&["main", "f"]);
        for name in ["t1", "t2"] {
            let term = TenantTerm::of(&tenant(name));
            for (config, dist) in [(single(), ""), (clustered(), "_dist")] {
                for req in [
                    example(&g),
                    example(&[]),
                    SeriesRequest {
                        call_site: &call_site,
                        ..example(&g)
                    },
                ] {
                    let sql = match plan(&tenant(name), &config, &req, END) {
                        Ok(Some(sql)) => sql,
                        other => panic!("expected a plan, got {other:?}"),
                    };
                    assert_eq!(
                        tenant_scope::check(&sql, &term, &tenant_scope::tables(dist)),
                        Ok(()),
                        "{sql}"
                    );
                }
            }
        }
    }

    /// S4: without `group_by` there is no join and `g` is the empty array.
    #[test]
    fn s4_no_group_by_no_join() {
        let sql = planned(&single(), &example(&[]), END);
        assert!(!sql.contains("INNER JOIN"), "{sql}");
        assert!(
            sql.contains("SELECT CAST([] AS Array(String)) AS g,"),
            "{sql}"
        );
    }

    /// S5: `AVERAGE` is the mean of the step's totals; otherwise their sum.
    #[test]
    fn s5_average_is_avg() {
        let g = by_service();
        let sql = planned(
            &single(),
            &SeriesRequest {
                average: true,
                ..example(&g)
            },
            END,
        );
        assert!(sql.contains("\n         avg(e.total) AS v\n"), "{sql}");
        assert!(!sql.contains("sum(e.total)"), "{sql}");
        let sql = planned(&single(), &example(&g), END);
        assert!(
            sql.contains("\n         toFloat64(sum(e.total)) AS v\n"),
            "{sql}"
        );
    }

    /// S6: a positive limit ends the statement; 0, a negative one or none
    /// adds nothing.
    #[test]
    fn s6_limit() {
        let g = by_service();
        let sql = planned(
            &single(),
            &SeriesRequest {
                limit: Some(3),
                ..example(&g)
            },
            END,
        );
        assert!(
            sql.ends_with("ORDER BY series_total DESC, g\nLIMIT 3"),
            "{sql}"
        );
        for limit in [Some(0), Some(-1), None] {
            let sql = planned(
                &single(),
                &SeriesRequest {
                    limit,
                    ..example(&g)
                },
                END,
            );
            assert!(
                sql.ends_with("ORDER BY series_total DESC, g"),
                "{limit:?}: {sql}"
            );
        }
    }

    /// S7: the step is refused below 1 ms and truncated to whole
    /// milliseconds as the reference truncates it.
    #[test]
    fn s7_step() {
        for step in [0.0009, f64::NAN] {
            assert_eq!(
                step_ms(step),
                Err(ProfilesReadError::InvalidArgument(
                    "step must be >= 1ms".to_string()
                )),
                "{step}"
            );
        }
        for (step, ms) in [(15.0, 15_000), (0.0015, 1), (1.9999, 1_999)] {
            assert_eq!(step_ms(step), Ok(ms), "{step}");
        }
        let g = by_service();
        assert_eq!(
            plan(
                &tenant("t1"),
                &single(),
                &SeriesRequest {
                    step: 0.0009,
                    ..example(&g)
                },
                END
            ),
            Err(ProfilesReadError::InvalidArgument(
                "step must be >= 1ms".to_string()
            ))
        );
    }

    /// S8: the read starts a step before the window and ends at the last
    /// point's millisecond.
    #[test]
    fn s8_bounds() {
        let g = by_service();
        let sql = planned(&single(), &example(&g), END);
        assert!(
            sql.contains(
                "e.timestamp_ns >= 1794819585000000000 AND e.timestamp_ns <= 1794823200000000000\n"
            ),
            "{sql}"
        );
        let sql = planned(
            &single(),
            &SeriesRequest {
                end_ms: END + 7_000,
                ..example(&g)
            },
            END + 7_000,
        );
        assert!(
            sql.contains("AND e.timestamp_ns <= 1794823200000999999\n"),
            "{sql}"
        );
    }

    /// S9: both series reads cover the window widened by a step.
    #[test]
    fn s9_series_reads_take_the_expanded_window() {
        let g = by_service();
        // Day d is 2026-11-16, d0 its midnight.
        let d0 = START - 9 * HOUR_MS;
        let sql = planned(
            &single(),
            &SeriesRequest {
                start_ms: d0 + 10_000,
                end_ms: d0 + 30 * 60_000,
                ..example(&g)
            },
            d0 + 30 * 60_000,
        );
        assert_eq!(
            sql.matches("day BETWEEN '2026-11-15' AND '2026-11-16'\n")
                .count(),
            2,
            "{sql}"
        );
    }

    /// S10: a call site reads each profile's total over the stacks that
    /// begin with it.
    #[test]
    fn s10_call_site_is_form_c() {
        let g = by_service();
        let call_site = strings(&["main", "f"]);
        let sql = planned(
            &single(),
            &SeriesRequest {
                call_site: &call_site,
                ..example(&g)
            },
            END,
        );
        for expected in [
            "\n    FROM profile_samples\n",
            "\n  AND arraySlice(names, 1, 2) = ['main', 'f'])\n",
            "\n    GROUP BY fingerprint, timestamp_ns, profile_id\n",
        ] {
            assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        }
        assert!(!sql.contains("profile_entries"), "{sql}");
    }

    /// S11: a `service_name` matcher is repeated on the entries read.
    #[test]
    fn s11_service_matchers_are_repeated() {
        let g = by_service();
        let sql = planned(
            &single(),
            &SeriesRequest {
                label_selector: r#"{service_name="x"}"#,
                ..example(&g)
            },
            END,
        );
        assert!(
            sql.contains(
                "WHERE org_id = 't1' AND e.type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'\n      \
                 AND service = 'x'\n      AND e.fingerprint IN ("
            ),
            "{sql}"
        );
    }

    /// S12: a window ending before the lookback floor plans nothing.
    #[test]
    fn s12_window_before_the_floor_plans_nothing() {
        let g = by_service();
        let now = END + 8 * DAY_MS;
        assert_eq!(plan(&tenant("t1"), &single(), &example(&g), now), Ok(None));
        // The same request inside the lookback plans a statement.
        assert!(matches!(
            plan(&tenant("t1"), &single(), &example(&g), END),
            Ok(Some(_))
        ));
    }

    fn row(g: &str, points: &[(i64, f64)]) -> TimeSeriesRow {
        TimeSeriesRow {
            g: vec![g.to_string()],
            series_total: points.iter().map(|(_, v)| v).sum(),
            points: points.to_vec(),
        }
    }

    fn labels(s: &[Series]) -> Vec<Vec<(String, String)>> {
        s.iter().map(|s| s.labels.clone()).collect()
    }

    fn pod(v: &str) -> Vec<(String, String)> {
        vec![("pod".to_string(), v.to_string())]
    }

    /// S13: the rows become series ordered by their sums, descending, a
    /// missing label dropped; equal sums by their labels; the limit cuts.
    #[test]
    fn s13_rows_become_ordered_series() {
        let group_by = strings(&["pod"]);
        let rows = vec![
            row("a", &[(1, 1.0), (2, 2.0)]),
            row("b", &[(3, 6.0)]),
            row("", &[(4, 5.0)]),
        ];
        let out = series(rows.clone(), &group_by, None);
        assert_eq!(labels(&out), vec![pod("b"), vec![], pod("a")]);
        assert_eq!(
            out[2].points,
            vec![
                Point {
                    timestamp: 1,
                    value: 1.0,
                    exemplar: None,
                },
                Point {
                    timestamp: 2,
                    value: 2.0,
                    exemplar: None,
                }
            ]
        );
        assert_eq!(labels(&series(rows, &group_by, Some(1))), vec![pod("b")]);
        let tied = vec![row("b", &[(1, 3.0)]), row("a", &[(1, 3.0)])];
        assert_eq!(
            labels(&series(tied, &group_by, None)),
            vec![pod("a"), pod("b")]
        );
    }

    /// S14 (issue #826): plan §0's request with exemplars renders plan §0's
    /// statement, byte for byte.
    #[test]
    fn s14_exemplars_render_the_example_statement() {
        let g = by_service();
        let req = SeriesRequest {
            exemplars: true,
            ..example(&g)
        };
        assert_eq!(planned(&single(), &req, END), EXAMPLE_EXEMPLARS);
        // Off, the statement is #824's.
        assert_eq!(planned(&single(), &example(&g), END), EXAMPLE);
    }

    /// S15 (issue #826): the tenant check accepts Form E with and without
    /// the join, and Form C, with exemplars, for two tenants, single node
    /// and clustered; the labels branch is there and scoped.
    #[test]
    fn s15_the_tenant_check_accepts_the_exemplar_forms() {
        let g = by_service();
        let call_site = strings(&["main", "f"]);
        for name in ["t1", "t2"] {
            let term = TenantTerm::of(&tenant(name));
            for (config, dist) in [(single(), ""), (clustered(), "_dist")] {
                for req in [
                    example(&g),
                    example(&[]),
                    SeriesRequest {
                        call_site: &call_site,
                        ..example(&g)
                    },
                ] {
                    let req = SeriesRequest {
                        exemplars: true,
                        ..req
                    };
                    let sql = match plan(&tenant(name), &config, &req, END) {
                        Ok(Some(sql)) => sql,
                        other => panic!("expected a plan, got {other:?}"),
                    };
                    assert!(
                        sql.contains(
                            "\nUNION ALL\nSELECT 1, CAST([] AS Array(String)), 0, [], fingerprint, "
                        ),
                        "{sql}"
                    );
                    assert_eq!(
                        tenant_scope::check(&sql, &term, &tenant_scope::tables(dist)),
                        Ok(()),
                        "{sql}"
                    );
                }
            }
        }
    }

    /// S16 (issue #826): the exemplar's labels drop the `group_by` names;
    /// without `group_by`, every stored label.
    #[test]
    fn s16_exemplar_labels_drop_the_group_by() {
        let g = strings(&["pod", "service_name"]);
        let sql = planned(
            &single(),
            &SeriesRequest {
                exemplars: true,
                ..example(&g)
            },
            END,
        );
        assert!(
            sql.contains(
                "arrayFilter(p -> NOT has(['pod', 'service_name'], p.1), \
                 JSONExtractKeysAndValues(any(labels), 'String'))"
            ),
            "{sql}"
        );
        let sql = planned(
            &single(),
            &SeriesRequest {
                exemplars: true,
                ..example(&[])
            },
            END,
        );
        assert!(
            sql.contains("0, [], fingerprint, JSONExtractKeysAndValues(any(labels), 'String')\n"),
            "{sql}"
        );
        assert!(!sql.contains("arrayFilter"), "{sql}");
    }

    /// S17 (issue #826): with a call site and exemplars, Form C's subquery
    /// returns each profile's id; off, it is #824's.
    #[test]
    fn s17_form_c_carries_the_profile() {
        let g = by_service();
        let call_site = strings(&["main", "f"]);
        let req = SeriesRequest {
            call_site: &call_site,
            ..example(&g)
        };
        let sql = planned(
            &single(),
            &SeriesRequest {
                exemplars: true,
                ..req
            },
            END,
        );
        assert!(
            sql.contains(
                "\n    SELECT fingerprint, timestamp_ns, profile_id, sum(value) AS total\n"
            ),
            "{sql}"
        );
        let sql = planned(&single(), &req, END);
        assert!(
            sql.contains("\n    SELECT fingerprint, timestamp_ns, sum(value) AS total\n"),
            "{sql}"
        );
    }

    /// S18 (issue #826): the limit binds the series branch, not the labels
    /// branch.
    #[test]
    fn s18_the_limit_binds_the_series_branch() {
        let g = by_service();
        let sql = planned(
            &single(),
            &SeriesRequest {
                exemplars: true,
                limit: Some(3),
                ..example(&g)
            },
            END,
        );
        assert!(
            sql.contains("ORDER BY series_total DESC, g\nLIMIT 3\nUNION ALL\nSELECT 1,"),
            "{sql}"
        );
    }

    fn exemplar_row(g: &str, points: &[(i64, f64)]) -> TimeSeriesExemplarRow {
        TimeSeriesExemplarRow {
            kind: 0,
            g: vec![g.to_string()],
            series_total: points.iter().map(|(_, v)| v).sum(),
            points: points
                .iter()
                .map(|(t, v)| (*t, *v, 1, "p".to_string(), *t, 1))
                .collect(),
            fingerprint: 0,
            labels: Vec::new(),
        }
    }

    /// S19 (issue #826): each point carries its exemplar, labelled from the
    /// labels row of its fingerprint; the series are ordered and cut as
    /// S13's.
    #[test]
    fn s19_rows_become_points_with_exemplars() {
        let x = vec![("x".to_string(), "1".to_string())];
        let rows = vec![
            TimeSeriesExemplarRow {
                kind: 0,
                g: vec!["a".to_string()],
                series_total: 3.0,
                points: vec![(1, 3.0, 3, "p".to_string(), 7, 9)],
                fingerprint: 0,
                labels: Vec::new(),
            },
            TimeSeriesExemplarRow {
                kind: 1,
                g: Vec::new(),
                series_total: 0.0,
                points: Vec::new(),
                fingerprint: 9,
                labels: x.clone(),
            },
        ];
        let out = series_exemplars(rows, &strings(&["pod"]), None);
        assert_eq!(
            out,
            vec![Series {
                labels: pod("a"),
                points: vec![Point {
                    timestamp: 1,
                    value: 3.0,
                    exemplar: Some(Exemplar {
                        timestamp: 7,
                        profile_id: "p".to_string(),
                        span_id: String::new(),
                        trace_id: String::new(),
                        value: 3,
                        labels: x,
                    }),
                }],
            }]
        );
        let group_by = strings(&["pod"]);
        let rows = vec![
            exemplar_row("a", &[(1, 1.0), (2, 2.0)]),
            exemplar_row("b", &[(3, 6.0)]),
            exemplar_row("", &[(4, 5.0)]),
        ];
        let out = series_exemplars(rows.clone(), &group_by, None);
        assert_eq!(labels(&out), vec![pod("b"), vec![], pod("a")]);
        assert_eq!(
            labels(&series_exemplars(rows, &group_by, Some(1))),
            vec![pod("b")]
        );
        let tied = vec![
            exemplar_row("b", &[(1, 3.0)]),
            exemplar_row("a", &[(1, 3.0)]),
        ];
        assert_eq!(
            labels(&series_exemplars(tied, &group_by, None)),
            vec![pod("a"), pod("b")]
        );
    }

    /// S20 (issue #826 part B): an exemplar's labels take the reference's
    /// order, `__profile_type__` first, `__service_name__` second, the rest
    /// by name.
    #[test]
    fn s20_exemplar_labels_take_the_reference_order() {
        let pairs = |p: &[(&str, &str)]| -> Vec<(String, String)> {
            p.iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect()
        };
        let rows = vec![
            TimeSeriesExemplarRow {
                kind: 0,
                g: vec!["a".to_string()],
                series_total: 3.0,
                points: vec![(1, 3.0, 3, "p".to_string(), 7, 9)],
                fingerprint: 0,
                labels: Vec::new(),
            },
            TimeSeriesExemplarRow {
                kind: 1,
                g: Vec::new(),
                series_total: 0.0,
                points: Vec::new(),
                fingerprint: 9,
                labels: pairs(&[
                    ("__name__", "process_cpu"),
                    ("__period_type__", "cpu"),
                    ("__profile_type__", "T"),
                    ("__service_name__", "v"),
                    ("pod", "e"),
                ]),
            },
        ];
        let out = series_exemplars(rows, &strings(&["pod"]), None);
        let got = out[0].points[0]
            .exemplar
            .as_ref()
            .map(|e| e.labels.clone())
            .unwrap_or_default();
        assert_eq!(
            got,
            pairs(&[
                ("__profile_type__", "T"),
                ("__service_name__", "v"),
                ("__name__", "process_cpu"),
                ("__period_type__", "cpu"),
                ("pod", "e"),
            ])
        );
    }
}
