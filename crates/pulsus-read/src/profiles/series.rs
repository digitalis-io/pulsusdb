//! Issue #824: `SelectSeries` planned as one statement (S6) — the per-step
//! time series of profile totals, one row per series, bucketed and
//! aggregated in ClickHouse. [`series`] orders the rows as the reference
//! orders its series and applies the limit.
//!
//! Without a call site the statement reads `profile_entries`, joined to the
//! selected series' labels when grouped. With one it reads each profile's
//! total over the stacks beginning with the call site from
//! `profile_samples`.

use pulsus_model::Tenant;
use serde::{Deserialize, Serialize};

use super::engine::{ProfilesReadConfig, ProfilesReadError};

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
}

/// One series as S6 returns it: its `group_by` values, the sum of its
/// points, and its points as `(timestamp ms, value)` in time order.
#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
pub struct TimeSeriesRow {
    pub g: Vec<String>,
    pub series_total: f64,
    pub points: Vec<(i64, f64)>,
}

/// One point of a series.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub timestamp: i64,
    pub value: f64,
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
    let _ = step;
    Ok(0)
}

/// Plans `req` for `tenant`. Not written yet.
pub fn plan(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    req: &SeriesRequest<'_>,
    now_ms: i64,
) -> Result<Option<String>, ProfilesReadError> {
    let _ = (tenant, config, req, now_ms);
    Ok(None)
}

/// The rows as the reference answers them. Not written yet.
pub fn series(rows: Vec<TimeSeriesRow>, group_by: &[String], limit: Option<i64>) -> Vec<Series> {
    let _ = (rows, group_by, limit);
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiles::selector::WindowLimits;
    use crate::profiles::tenant_scope;
    use crate::traces::tenant_scope::TenantTerm;

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
                    value: 1.0
                },
                Point {
                    timestamp: 2,
                    value: 2.0
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
}
