//! Issue #825: `SelectHeatmap` planned as one statement (S8) — per series
//! and slot, how many values fall in each of 20 buckets of the global value
//! range, computed in ClickHouse. [`heatmap`] orders the rows and applies
//! the limit.
//!
//! `INDIVIDUAL` reads each profile's total from `profile_entries`; `SPAN`
//! reads each profile's samples of one span from `profile_samples`, summed.
//!
//! Issue #826: with exemplars, each value carries its ids until the window
//! has the range, each cell's largest value is its exemplar, and a second
//! `UNION ALL` branch returns each selected series' labels;
//! [`heatmap_exemplars`] joins them.

use pulsus_model::Tenant;
use serde::{Deserialize, Serialize};

use crate::logql::escape::ch_string;
use crate::metrics::DataWindow;
use crate::traces::tenant_scope::TenantTerm;

use super::engine::{ProfilesReadConfig, ProfilesReadError};
use super::series::{self, Exemplar};

/// `querier.v1.HeatmapQueryType` `INDIVIDUAL`.
pub const QUERY_TYPE_INDIVIDUAL: i32 = 1;
/// `querier.v1.HeatmapQueryType` `SPAN`.
pub const QUERY_TYPE_SPAN: i32 = 2;

/// What a `SelectHeatmap` request selects.
#[derive(Debug, Clone, Copy, Default)]
pub struct HeatmapRequest<'a> {
    pub profile_type: &'a str,
    pub label_selector: &'a str,
    pub start_ms: i64,
    pub end_ms: i64,
    /// The step in seconds.
    pub step: f64,
    /// The labels each series is grouped by, in request order.
    pub group_by: &'a [String],
    /// `querier.v1.HeatmapQueryType` as a number.
    pub query_type: i32,
    /// The request's `limit`, `None` when absent.
    pub limit: Option<i64>,
    /// Issue #826: exemplars of the query type, one per cell.
    pub exemplars: bool,
}

/// One series as S8 returns it: its `group_by` values, its value count,
/// its slots as `(timestamp ms, 20 counts)` in time order, and the 20
/// bucket starts.
#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
pub struct HeatmapRow {
    pub g: Vec<String>,
    pub n: u64,
    pub slots: Vec<(i64, Vec<u32>)>,
    pub y_min: Vec<f64>,
}

/// Issue #826: one cell's exemplar as S8 returns it: `(total, time ms,
/// profile id, span id, trace id in hex, fingerprint)`.
pub type ExemplarTuple = (i64, i64, String, u64, String, u128);

/// Issue #826: one row of S8 with exemplars. `kind` 0 is a series as
/// [`HeatmapRow`], each slot also carrying its cells' exemplars in bucket
/// order; `kind` 1 is one selected series' `fingerprint` and its
/// stored `labels` less the `group_by` names.
#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
pub struct HeatmapExemplarRow {
    pub kind: u8,
    pub g: Vec<String>,
    pub n: u64,
    pub slots: Vec<(i64, Vec<u32>, Vec<ExemplarTuple>)>,
    pub y_min: Vec<f64>,
    pub fingerprint: u128,
    pub labels: Vec<(String, String)>,
}

/// One slot of a series.
#[derive(Debug, Clone, PartialEq)]
pub struct Slot {
    pub timestamp: i64,
    pub y_min: Vec<f64>,
    pub counts: Vec<i32>,
    /// Issue #826: one per non-empty bucket, lowest bucket first.
    pub exemplars: Vec<Exemplar>,
}

/// One series of the answer: its labels in `group_by` order, a missing one
/// dropped, and its slots in time order.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub labels: Vec<(String, String)>,
    pub slots: Vec<Slot>,
}

/// Whether `query_type` is `SPAN`; refused unless it is `INDIVIDUAL` or
/// `SPAN`, whatever the window (the owner's decision of 2026-11-17).
fn query_type(query_type: i32) -> Result<bool, ProfilesReadError> {
    match query_type {
        QUERY_TYPE_INDIVIDUAL => Ok(false),
        QUERY_TYPE_SPAN => Ok(true),
        _ => Err(ProfilesReadError::InvalidArgument(
            "valid heatmap query type must be specified".to_string(),
        )),
    }
}

/// Plans `req` for `tenant`: `Ok(None)` when its window ends before the
/// lookback floor, so nothing is read. In the reference's order: the step,
/// the limit, the window's order, then the window, the type and the
/// selector, then the query type. The exemplar type is the caller's.
pub fn plan(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    req: &HeatmapRequest<'_>,
    now_ms: i64,
) -> Result<Option<String>, ProfilesReadError> {
    let invalid = |m: String| Err(ProfilesReadError::InvalidArgument(m));
    if req.step.is_nan() || req.step < 0.001 {
        return invalid(format!("step must be >= 1ms, got {:.6}", req.step));
    }
    if let Some(limit) = req.limit.filter(|l| *l < 0) {
        return invalid(format!("limit must be non-negative, got {limit}"));
    }
    if req.start_ms >= req.end_ms {
        return invalid(format!(
            "start time must be before end time, got start={} end={}",
            req.start_ms, req.end_ms
        ));
    }
    let Some((window, matchers)) = series::select(
        tenant,
        config,
        req.profile_type,
        req.label_selector,
        req.start_ms,
        req.end_ms,
        now_ms,
    )?
    else {
        return query_type(req.query_type).map(|_| None);
    };
    let span = query_type(req.query_type)?;
    let step = series::step_ms(req.step)?;

    let start = window.start_ms;
    let end = window.end_ms;
    let read_start = start.saturating_sub(step);
    let lo_ns = read_start.saturating_mul(1_000_000);
    let hi_ns = end.saturating_mul(1_000_000);
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
    let (g, join) = series::group(tenant, w, series_table, &matchers, req.group_by);
    let source = if span {
        format!(
            "FROM (
      SELECT fingerprint, timestamp_ns, sum(value) AS total{ids}
      FROM {samples}
      WHERE {tenant_term} AND type_id = {type_id}{service_terms}
        AND fingerprint IN ({series})
        AND timestamp_ns >= {lo_ns} AND timestamp_ns <= {hi_ns}
        AND span_id != 0
      GROUP BY fingerprint, timestamp_ns, profile_id, trace_id, span_id
    ) AS e{join}",
            samples = config.samples_table,
            ids = if req.exemplars {
                ", span_id, trace_id"
            } else {
                ""
            },
        )
    } else {
        format!(
            "FROM {entries} AS e{join}
    WHERE {tenant_term} AND e.type_id = {type_id}{service_terms}
      AND e.fingerprint IN ({series})
      AND e.timestamp_ns >= {lo_ns} AND e.timestamp_ns <= {hi_ns}",
            entries = config.entries_table,
        )
    };
    // With a limit, the statement's cut ties on the labels as `heatmap`
    // orders them: the non-empty `(name, value)` pairs in request order.
    let (order, limit) = match req.limit {
        Some(n) if n > 0 && !req.group_by.is_empty() => {
            let names: Vec<String> = req.group_by.iter().map(|l| ch_string(l)).collect();
            (
                format!(
                    "arrayFilter(p -> p.2 != '', arrayZip([{}], g))",
                    names.join(", ")
                ),
                format!("\nLIMIT {}", n.saturating_add(1)),
            )
        }
        Some(n) if n > 0 => ("g".to_string(), format!("\nLIMIT {}", n.saturating_add(1))),
        _ => ("g".to_string(), String::new()),
    };
    if req.exemplars {
        let (xs, exemplar) = if span {
            (
                "arraySort(x -> (-x.1, x.2, x.3, x.4), groupArray((e.total, e.timestamp_ns, e.span_id, e.trace_id, e.fingerprint)))",
                "(x.1, intDiv(x.2, 1000000), '', x.3, lower(hex(x.4)), x.5)",
            )
        } else {
            (
                "arraySort(x -> (-x.1, x.2, x.3), groupArray((e.total, e.timestamp_ns, e.profile_id, e.fingerprint)))",
                "(x.1, intDiv(x.2, 1000000), toString(x.3), toUInt64(0), '', x.4)",
            )
        };
        let labels =
            series::labels_branch(tenant, w, series_table, &matchers, req.group_by, "[], ");
        return Ok(Some(format!(
            "SELECT 0 AS kind, g, sum(length(b)) AS n, arraySort(groupArray((ts, arrayMap(k -> toUInt32(countEqual(b, k)), range(20)), arrayReverse(arrayMap((x, c) -> {exemplar}, arrayFilter((x, c, p) -> c != p, xs, b, arrayPopBack(arrayPushFront(b, -1))), arrayFilter((c, p) -> c != p, b, arrayPopBack(arrayPushFront(b, -1)))))))) AS slots,
       any(arrayMap(i -> toFloat64(lo + toInt64(i * w)), range(20))) AS y_min, toUInt128(0) AS fingerprint, CAST([] AS Array(Tuple(String, String))) AS labels
FROM (
  SELECT g, ts, lo, w, xs,
         arrayMap(v -> least(19, toInt64((v - lo + 1) / w)), xs.1) AS b0,
         arrayMap((c, v) -> c - (toInt64(c * w) > v - lo), b0, xs.1) AS b1,
         arrayMap((c, v) -> c + (c < 19 AND toInt64((c + 1) * w) <= v - lo), b1, xs.1) AS b
  FROM (
    SELECT g, ts, xs, lo, toFloat64(if(hi = lo, 1, hi - lo)) / 20 AS w
    FROM (
      SELECT g, ts, xs, min(v_min) OVER () AS lo, greatest(max(v_max) OVER (), 0) AS hi
      FROM (
        SELECT {g} AS g, {start} + intDiv(intDiv(e.timestamp_ns, 1000000) - {start} + {step_minus_1}, {step}) * {step} AS ts, {xs} AS xs, min(e.total) AS v_min, max(e.total) AS v_max
        {source}
        GROUP BY g, ts
      )
    )
  )
)
GROUP BY g
ORDER BY n DESC, {order}{limit}{labels}",
            step_minus_1 = step - 1,
        )));
    }
    Ok(Some(format!(
        "SELECT g, sum(length(b)) AS n, arraySort(groupArray((ts, arrayMap(k -> toUInt32(countEqual(b, k)), range(20))))) AS slots,
       any(arrayMap(i -> toFloat64(lo + toInt64(i * w)), range(20))) AS y_min
FROM (
  SELECT g, ts, lo, w,
         arrayMap(v -> least(19, toInt64((v - lo + 1) / w)), vs) AS b0,
         arrayMap((c, v) -> c - (toInt64(c * w) > v - lo), b0, vs) AS b1,
         arrayMap((c, v) -> c + (c < 19 AND toInt64((c + 1) * w) <= v - lo), b1, vs) AS b
  FROM (
    SELECT g, ts, vs, lo, toFloat64(if(hi = lo, 1, hi - lo)) / 20 AS w
    FROM (
      SELECT g, ts, vs, min(v_min) OVER () AS lo, greatest(max(v_max) OVER (), 0) AS hi
      FROM (
        SELECT {g} AS g, {start} + intDiv(intDiv(e.timestamp_ns, 1000000) - {start} + {step_minus_1}, {step}) * {step} AS ts, groupArray(e.total) AS vs, min(e.total) AS v_min, max(e.total) AS v_max
        {source}
        GROUP BY g, ts
      )
    )
  )
)
GROUP BY g
ORDER BY n DESC, {order}{limit}",
        step_minus_1 = step - 1,
    )))
}

/// The rows as the answer's series: each labelled by the `group_by` names
/// it holds a value for, in request order; every slot carries the row's
/// bucket starts. With more rows than a positive `limit`, ordered by their
/// value counts, descending, then by their labels, and cut to `limit`;
/// otherwise ordered by their labels.
pub fn heatmap(rows: Vec<HeatmapRow>, group_by: &[String], limit: Option<i64>) -> Vec<Series> {
    order_and_cut(
        rows.into_iter()
            .map(|row| {
                let y_min = row.y_min;
                let slots = row
                    .slots
                    .into_iter()
                    .map(|(timestamp, counts)| Slot {
                        timestamp,
                        y_min: y_min.clone(),
                        counts: slot_counts(counts),
                        exemplars: Vec::new(),
                    })
                    .collect();
                let labels = series::group_labels(group_by, row.g);
                (row.n, Series { labels, slots })
            })
            .collect(),
        limit,
    )
}

/// Issue #826: S8's exemplar rows as [`heatmap`] answers them, each slot
/// carrying its cells' exemplars, labelled with the `kind` 1 row of their
/// fingerprint: a span id 0 as `""`, another as 16 hex digits of its
/// bytes little-endian; an all-zero trace id as `""`.
pub fn heatmap_exemplars(
    rows: Vec<HeatmapExemplarRow>,
    group_by: &[String],
    limit: Option<i64>,
) -> Vec<Series> {
    let (rows, labels) = series::split_labels(rows, |r| r.kind == 0, |r| (r.fingerprint, r.labels));
    order_and_cut(
        rows.into_iter()
            .map(|row| {
                let y_min = row.y_min;
                let slots = row
                    .slots
                    .into_iter()
                    .map(|(timestamp, counts, exemplars)| Slot {
                        timestamp,
                        y_min: y_min.clone(),
                        counts: slot_counts(counts),
                        exemplars: exemplars
                            .into_iter()
                            .map(|(total, at, profile_id, span, trace, fp)| Exemplar {
                                timestamp: at,
                                profile_id,
                                span_id: if span == 0 {
                                    String::new()
                                } else {
                                    span.to_le_bytes()
                                        .iter()
                                        .map(|b| format!("{b:02x}"))
                                        .collect()
                                },
                                trace_id: if trace.bytes().all(|b| b == b'0') {
                                    String::new()
                                } else {
                                    trace
                                },
                                value: total,
                                labels: labels.get(&fp).cloned().unwrap_or_default(),
                            })
                            .collect(),
                    })
                    .collect();
                let labels = series::group_labels(group_by, row.g);
                (row.n, Series { labels, slots })
            })
            .collect(),
        limit,
    )
}

fn slot_counts(counts: Vec<u32>) -> Vec<i32> {
    counts
        .into_iter()
        .map(|c| i32::try_from(c).unwrap_or(i32::MAX))
        .collect()
}

/// With more series than a positive `limit`, ordered by their value
/// counts, descending, then by their labels, and cut to `limit`; otherwise
/// ordered by their labels.
fn order_and_cut(mut out: Vec<(u64, Series)>, limit: Option<i64>) -> Vec<Series> {
    let cut = limit
        .filter(|k| *k > 0)
        .map(|k| usize::try_from(k).unwrap_or(usize::MAX))
        .filter(|k| out.len() > *k);
    match cut {
        Some(k) => {
            out.sort_by(|(a_n, a), (b_n, b)| {
                b_n.cmp(a_n)
                    .then_with(|| series::compare_label_pairs(&a.labels, &b.labels))
            });
            out.truncate(k);
        }
        None => out.sort_by(|(_, a), (_, b)| series::compare_label_pairs(&a.labels, &b.labels)),
    }
    out.into_iter().map(|(_, s)| s).collect()
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

    /// Plan §0's statement.
    const EXAMPLE: &str = "SELECT g, sum(length(b)) AS n, arraySort(groupArray((ts, arrayMap(k -> toUInt32(countEqual(b, k)), range(20))))) AS slots,
       any(arrayMap(i -> toFloat64(lo + toInt64(i * w)), range(20))) AS y_min
FROM (
  SELECT g, ts, lo, w,
         arrayMap(v -> least(19, toInt64((v - lo + 1) / w)), vs) AS b0,
         arrayMap((c, v) -> c - (toInt64(c * w) > v - lo), b0, vs) AS b1,
         arrayMap((c, v) -> c + (c < 19 AND toInt64((c + 1) * w) <= v - lo), b1, vs) AS b
  FROM (
    SELECT g, ts, vs, lo, toFloat64(if(hi = lo, 1, hi - lo)) / 20 AS w
    FROM (
      SELECT g, ts, vs, min(v_min) OVER () AS lo, greatest(max(v_max) OVER (), 0) AS hi
      FROM (
        SELECT s.g AS g, 1794819600000 + intDiv(intDiv(e.timestamp_ns, 1000000) - 1794819600000 + 14999, 15000) * 15000 AS ts, groupArray(e.total) AS vs, min(e.total) AS v_min, max(e.total) AS v_max
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
    )
  )
)
GROUP BY g
ORDER BY n DESC, g";

    /// Plan §2.2's statement (issue #826): §0's request with `INDIVIDUAL`
    /// exemplars.
    const EXAMPLE_EXEMPLARS: &str = "SELECT 0 AS kind, g, sum(length(b)) AS n, arraySort(groupArray((ts, arrayMap(k -> toUInt32(countEqual(b, k)), range(20)), arrayReverse(arrayMap((x, c) -> (x.1, intDiv(x.2, 1000000), toString(x.3), toUInt64(0), '', x.4), arrayFilter((x, c, p) -> c != p, xs, b, arrayPopBack(arrayPushFront(b, -1))), arrayFilter((c, p) -> c != p, b, arrayPopBack(arrayPushFront(b, -1)))))))) AS slots,
       any(arrayMap(i -> toFloat64(lo + toInt64(i * w)), range(20))) AS y_min, toUInt128(0) AS fingerprint, CAST([] AS Array(Tuple(String, String))) AS labels
FROM (
  SELECT g, ts, lo, w, xs,
         arrayMap(v -> least(19, toInt64((v - lo + 1) / w)), xs.1) AS b0,
         arrayMap((c, v) -> c - (toInt64(c * w) > v - lo), b0, xs.1) AS b1,
         arrayMap((c, v) -> c + (c < 19 AND toInt64((c + 1) * w) <= v - lo), b1, xs.1) AS b
  FROM (
    SELECT g, ts, xs, lo, toFloat64(if(hi = lo, 1, hi - lo)) / 20 AS w
    FROM (
      SELECT g, ts, xs, min(v_min) OVER () AS lo, greatest(max(v_max) OVER (), 0) AS hi
      FROM (
        SELECT s.g AS g, 1794819600000 + intDiv(intDiv(e.timestamp_ns, 1000000) - 1794819600000 + 14999, 15000) * 15000 AS ts, arraySort(x -> (-x.1, x.2, x.3), groupArray((e.total, e.timestamp_ns, e.profile_id, e.fingerprint))) AS xs, min(e.total) AS v_min, max(e.total) AS v_max
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
    )
  )
)
GROUP BY g
ORDER BY n DESC, g
UNION ALL
SELECT 1, CAST([] AS Array(String)), 0, [], [], fingerprint, arrayFilter(p -> NOT has(['service_name'], p.1), JSONExtractKeysAndValues(any(labels), 'String'))
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
            landing_table: "profile_landing".to_string(),
            adhoc_table: "profile_adhoc".to_string(),
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
            landing_table: "profile_landing_dist".to_string(),
            adhoc_table: "profile_adhoc_dist".to_string(),
            dist_suffix: "_dist".to_string(),
            distributed: true,
            ..single()
        }
    }

    fn by_service() -> Vec<String> {
        vec!["service_name".to_string()]
    }

    /// Plan §0's request with `group_by`.
    fn example(group_by: &[String]) -> HeatmapRequest<'_> {
        HeatmapRequest {
            profile_type: CPU,
            label_selector: "{}",
            start_ms: START,
            end_ms: END,
            step: 15.0,
            group_by,
            query_type: QUERY_TYPE_INDIVIDUAL,
            limit: None,
            exemplars: false,
        }
    }

    fn planned(config: &ProfilesReadConfig, req: &HeatmapRequest<'_>, now: i64) -> String {
        match plan(&tenant("t1"), config, req, now) {
            Ok(Some(sql)) => sql,
            other => panic!("expected a plan, got {other:?}"),
        }
    }

    /// H1: plan §0's request renders plan §0's statement, byte for byte.
    #[test]
    fn h1_the_example_renders_the_example_statement() {
        let g = by_service();
        assert_eq!(planned(&single(), &example(&g), END), EXAMPLE);
    }

    /// H2: clustered, the entries, the samples and both series reads name
    /// the wrappers.
    #[test]
    fn h2_clustered_reads_the_wrappers() {
        let g = by_service();
        let sql = planned(&clustered(), &example(&g), END);
        assert!(sql.contains("FROM profile_entries_dist AS e"), "{sql}");
        assert_eq!(
            sql.matches("FROM profile_series_dist\n").count(),
            2,
            "{sql}"
        );
        let span = HeatmapRequest {
            query_type: QUERY_TYPE_SPAN,
            ..example(&g)
        };
        let sql = planned(&clustered(), &span, END);
        assert!(sql.contains("FROM profile_samples_dist\n"), "{sql}");
    }

    /// H3: the tenant check accepts Form I with and without the join, and
    /// Form S, single node and clustered, for two tenants.
    #[test]
    fn h3_the_tenant_check_accepts_every_form() {
        let g = by_service();
        for name in ["t1", "t2"] {
            let term = TenantTerm::of(&tenant(name));
            for (config, dist) in [(single(), ""), (clustered(), "_dist")] {
                for req in [
                    example(&g),
                    example(&[]),
                    HeatmapRequest {
                        query_type: QUERY_TYPE_SPAN,
                        ..example(&g)
                    },
                ] {
                    let sql = match plan(&tenant(name), &config, &req, END) {
                        Ok(Some(sql)) => sql,
                        other => panic!("expected a plan, got {other:?}"),
                    };
                    assert!(sql.contains(&format!("org_id = '{name}'")), "{sql}");
                    assert_eq!(
                        tenant_scope::check(&sql, &term, &tenant_scope::tables(dist)),
                        Ok(()),
                        "{sql}"
                    );
                }
            }
        }
    }

    /// H4: `SPAN` reads one value per span of a profile, from the samples.
    #[test]
    fn h4_span_reads_one_value_per_span_of_a_profile() {
        let g = by_service();
        let sql = planned(
            &single(),
            &HeatmapRequest {
                query_type: QUERY_TYPE_SPAN,
                ..example(&g)
            },
            END,
        );
        for expected in [
            "\n        AND span_id != 0\n",
            "GROUP BY fingerprint, timestamp_ns, profile_id, trace_id, span_id\n",
            "\n      FROM profile_samples\n",
        ] {
            assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        }
        assert!(!sql.contains("profile_entries"), "{sql}");
    }

    /// H5: the read starts a step before the window and ends at its end,
    /// with no cap at the last grid point.
    #[test]
    fn h5_bounds() {
        let g = by_service();
        let sql = planned(&single(), &example(&g), END);
        assert!(
            sql.contains(">= 1794819585000000000 AND e.timestamp_ns <= 1794823200000000000\n"),
            "{sql}"
        );
        let sql = planned(
            &single(),
            &HeatmapRequest {
                end_ms: END + 7_000,
                ..example(&g)
            },
            END + 7_000,
        );
        assert!(
            sql.contains("AND e.timestamp_ns <= 1794823207000000000\n"),
            "{sql}"
        );
    }

    /// H6: a positive limit asks for one series more; 0 and none add
    /// nothing.
    #[test]
    fn h6_limit_asks_one_more() {
        let g = by_service();
        let sql = planned(
            &single(),
            &HeatmapRequest {
                limit: Some(3),
                ..example(&g)
            },
            END,
        );
        assert!(
            sql.ends_with(
                "ORDER BY n DESC, arrayFilter(p -> p.2 != '', arrayZip(['service_name'], g))\nLIMIT 4"
            ),
            "{sql}"
        );
        for limit in [Some(0), None] {
            let sql = planned(
                &single(),
                &HeatmapRequest {
                    limit,
                    ..example(&g)
                },
                END,
            );
            assert!(sql.ends_with("ORDER BY n DESC, g"), "{limit:?}: {sql}");
        }
    }

    fn invalid(m: &str) -> Result<Option<String>, ProfilesReadError> {
        Err(ProfilesReadError::InvalidArgument(m.to_string()))
    }

    /// H7: the checks, in the reference's order: the step, the limit, the
    /// window's order, the window, type and selector, then the query type.
    #[test]
    fn h7_checks_in_order() {
        let g = by_service();
        let t = tenant("t1");
        let step = invalid("step must be >= 1ms, got 0.000900");
        for selector in ["{}", "{"] {
            let req = HeatmapRequest {
                step: 0.0009,
                label_selector: selector,
                ..example(&g)
            };
            assert_eq!(plan(&t, &single(), &req, END), step, "{selector}");
        }
        let req = HeatmapRequest {
            limit: Some(-1),
            ..example(&g)
        };
        assert_eq!(
            plan(&t, &single(), &req, END),
            invalid("limit must be non-negative, got -1")
        );
        let req = HeatmapRequest {
            end_ms: START,
            ..example(&g)
        };
        assert_eq!(
            plan(&t, &single(), &req, END),
            invalid(
                "start time must be before end time, got start=1794819600000 end=1794819600000"
            )
        );
        for query_type in [0, 3] {
            let req = HeatmapRequest {
                query_type,
                ..example(&g)
            };
            assert_eq!(
                plan(&t, &single(), &req, END),
                invalid("valid heatmap query type must be specified"),
                "{query_type}"
            );
        }
        assert_eq!(
            plan(&t, &single(), &example(&g), END + 8 * DAY_MS),
            Ok(None)
        );
    }

    /// H9: an absent or unknown query type is refused also when the window
    /// ends before the lookback floor.
    #[test]
    fn h9_the_query_type_is_checked_on_every_window() {
        let g = by_service();
        for query_type in [0, 3] {
            let req = HeatmapRequest {
                query_type,
                ..example(&g)
            };
            assert_eq!(
                plan(&tenant("t1"), &single(), &req, END + 8 * DAY_MS),
                invalid("valid heatmap query type must be specified"),
                "{query_type}"
            );
        }
    }

    /// H10: with a limit, the statement's cut ties on the labels, empty
    /// values dropped, so the series it keeps are the ones `heatmap` keeps.
    #[test]
    fn h10_the_limit_ties_on_the_labels() {
        let g = vec!["a".to_string(), "b".to_string()];
        let sql = planned(
            &single(),
            &HeatmapRequest {
                limit: Some(1),
                ..example(&g)
            },
            END,
        );
        assert!(
            sql.ends_with(
                "ORDER BY n DESC, arrayFilter(p -> p.2 != '', arrayZip(['a', 'b'], g))\nLIMIT 2"
            ),
            "{sql}"
        );
    }

    fn row(g: &str, n: u64) -> HeatmapRow {
        HeatmapRow {
            g: vec![g.to_string()],
            n,
            slots: vec![(START, vec![n as u32; 20])],
            y_min: (0..20).map(|i| (i * 10 + n) as f64).collect(),
        }
    }

    fn labels(s: &[Series]) -> Vec<Vec<(String, String)>> {
        s.iter().map(|s| s.labels.clone()).collect()
    }

    fn pod(v: &str) -> Vec<(String, String)> {
        vec![("pod".to_string(), v.to_string())]
    }

    /// H8: the rows become series ordered by their labels; past a positive
    /// limit, by their value counts, descending, cut to the limit.
    #[test]
    fn h8_rows_become_ordered_series() {
        let group_by = vec!["pod".to_string()];
        let rows = vec![row("b", 2), row("a", 3), row("", 1)];
        let out = heatmap(rows.clone(), &group_by, None);
        assert_eq!(labels(&out), vec![vec![], pod("a"), pod("b")]);
        for (series, n) in out.iter().zip([1u64, 3, 2]) {
            assert_eq!(
                series.slots,
                vec![Slot {
                    timestamp: START,
                    y_min: row("", n).y_min,
                    counts: vec![n as i32; 20],
                    exemplars: Vec::new(),
                }]
            );
        }
        assert_eq!(
            labels(&heatmap(rows.clone(), &group_by, Some(1))),
            vec![pod("a")]
        );
        assert_eq!(
            labels(&heatmap(rows, &group_by, Some(3))),
            vec![vec![], pod("a"), pod("b")]
        );
    }

    /// H9 (issue #826): §0's request with `INDIVIDUAL` exemplars renders
    /// plan §2.2's statement, byte for byte; off, #825's.
    #[test]
    fn h9_exemplars_render_the_example_statement() {
        let g = by_service();
        let req = HeatmapRequest {
            exemplars: true,
            ..example(&g)
        };
        assert_eq!(planned(&single(), &req, END), EXAMPLE_EXEMPLARS);
        assert_eq!(planned(&single(), &example(&g), END), EXAMPLE);
    }

    /// H10 (issue #826): `SPAN` exemplars carry the span and the trace.
    #[test]
    fn h10_span_exemplars_carry_span_and_trace() {
        let g = by_service();
        let sql = planned(
            &single(),
            &HeatmapRequest {
                query_type: QUERY_TYPE_SPAN,
                exemplars: true,
                ..example(&g)
            },
            END,
        );
        for expected in [
            "SELECT fingerprint, timestamp_ns, sum(value) AS total, span_id, trace_id\n",
            "arraySort(x -> (-x.1, x.2, x.3, x.4), groupArray((e.total, e.timestamp_ns, e.span_id, e.trace_id, e.fingerprint))) AS xs",
            "(x.1, intDiv(x.2, 1000000), '', x.3, lower(hex(x.4)), x.5)",
        ] {
            assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        }
    }

    /// H11 (issue #826): the tenant check accepts both query types with
    /// exemplars, single node and clustered, for two tenants; the labels
    /// branch is there and scoped.
    #[test]
    fn h11_the_tenant_check_accepts_the_exemplar_forms() {
        let g = by_service();
        for name in ["t1", "t2"] {
            let term = TenantTerm::of(&tenant(name));
            for (config, dist) in [(single(), ""), (clustered(), "_dist")] {
                for query_type in [QUERY_TYPE_INDIVIDUAL, QUERY_TYPE_SPAN] {
                    let req = HeatmapRequest {
                        query_type,
                        exemplars: true,
                        ..example(&g)
                    };
                    let sql = match plan(&tenant(name), &config, &req, END) {
                        Ok(Some(sql)) => sql,
                        other => panic!("expected a plan, got {other:?}"),
                    };
                    assert!(
                        sql.contains(
                            "\nUNION ALL\nSELECT 1, CAST([] AS Array(String)), 0, [], [], fingerprint, "
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

    /// H12 (issue #826): each exemplar tuple becomes an exemplar, the span
    /// as 16 hex digits of its bytes little-endian, an all-zero trace as
    /// `""`, in the row's order, labelled from its fingerprint's row.
    #[test]
    fn h12_rows_become_slots_with_exemplars() {
        let x = vec![("x".to_string(), "1".to_string())];
        let rows = vec![
            HeatmapExemplarRow {
                kind: 0,
                g: vec![String::new()],
                n: 2,
                slots: vec![(
                    START,
                    vec![1; 20],
                    vec![
                        (
                            5,
                            7,
                            String::new(),
                            10,
                            "00000000000000000000000000000002".to_string(),
                            9,
                        ),
                        (
                            4,
                            6,
                            String::new(),
                            11,
                            "00000000000000000000000000000000".to_string(),
                            9,
                        ),
                    ],
                )],
                y_min: vec![0.0; 20],
                fingerprint: 0,
                labels: Vec::new(),
            },
            HeatmapExemplarRow {
                kind: 1,
                g: Vec::new(),
                n: 0,
                slots: Vec::new(),
                y_min: Vec::new(),
                fingerprint: 9,
                labels: x.clone(),
            },
        ];
        let out = heatmap_exemplars(rows, &[], None);
        let exemplar = |value: i64, timestamp: i64, span: &str, trace: &str| Exemplar {
            timestamp,
            profile_id: String::new(),
            span_id: span.to_string(),
            trace_id: trace.to_string(),
            value,
            labels: x.clone(),
        };
        assert_eq!(
            out,
            vec![Series {
                labels: Vec::new(),
                slots: vec![Slot {
                    timestamp: START,
                    y_min: vec![0.0; 20],
                    counts: vec![1; 20],
                    exemplars: vec![
                        exemplar(5, 7, "0a00000000000000", "00000000000000000000000000000002"),
                        exemplar(4, 6, "0b00000000000000", ""),
                    ],
                }],
            }]
        );
    }
}
