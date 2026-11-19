//! Issue #829: `render` planned as one statement — the merged tree (S5,
//! `super::tree`) and the time series of profile totals (S6,
//! `super::series`) of one selection under `UNION ALL`, each row tagged by
//! a `part` column — and the reference's timeline over the series.

use pulsus_model::Tenant;
use serde::{Deserialize, Serialize};

use super::engine::{ProfilesReadConfig, ProfilesReadError};
use super::series::TimeSeriesRow;
use super::tree::{TreeRequest, TreeRow};

/// What a `render` request selects: the flame graph's selection, the
/// series' `groupBy` labels, and whether each point is the average.
#[derive(Debug, Clone, Copy, Default)]
pub struct RenderRequest<'a> {
    pub tree: TreeRequest<'a>,
    pub group_by: &'a [String],
    pub average: bool,
}

/// A planned request: the statement, the trim's two `max_nodes`, and the
/// timeline's step in seconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub sql: String,
    pub max_nodes: i64,
    pub raw_max_nodes: i64,
    pub step_s: i64,
}

/// One row of the statement: `part` 0 is a [`TreeRow`], `part` 1 a
/// [`TimeSeriesRow`]; the other part's columns are zero.
#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
pub struct RenderRow {
    pub part: u8,
    pub node: u128,
    pub parent: u128,
    pub name: String,
    pub total: i64,
    pub self_value: i64,
    pub g: Vec<String>,
    pub series_total: f64,
    pub points: Vec<(i64, f64)>,
}

/// Plans `req` for `tenant`: `Ok(None)` when its window ends before the
/// lookback floor, so nothing is read.
pub fn plan(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    req: &RenderRequest<'_>,
    now_ms: i64,
) -> Result<Option<Planned>, ProfilesReadError> {
    let _ = (tenant, config, req, now_ms);
    Ok(None)
}

/// The statement's rows split by `part`.
pub fn split(rows: Vec<RenderRow>) -> (Vec<TreeRow>, Vec<TimeSeriesRow>) {
    let _ = rows;
    (Vec::new(), Vec::new())
}

/// The series step in seconds, as the reference's `CalcPointInterval`
/// computes it (`pkg/querier/timeline/calculator.go:18-130`).
pub fn point_interval_s(from_ms: i64, until_ms: i64) -> i64 {
    let _ = (from_ms, until_ms);
    0
}

/// The reference's `timeline.New` (`pkg/querier/timeline/timeline.go:16-98`):
/// the start in seconds, and one sample per step from the start, both
/// ends aligned down to the step. `points` sorted, `(ms, value)`.
pub fn timeline(points: &[(i64, f64)], start_ms: i64, end_ms: i64, step_s: i64) -> (i64, Vec<u64>) {
    let _ = (points, start_ms, end_ms, step_s);
    (0, Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiles::selector::WindowLimits;
    use crate::profiles::series::{self, SeriesRequest};
    use crate::profiles::{tenant_scope, tree};
    use crate::traces::tenant_scope::TenantTerm;

    const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    /// 2026-11-16T09:00Z and 10:00Z.
    const START: i64 = 1_794_819_600_000;
    const END: i64 = 1_794_823_200_000;
    const DAY_MS: i64 = 86_400_000;

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

    /// R1: `CalcPointInterval`: below 15 s is 15 s, else the rounding
    /// table.
    #[test]
    fn r1_point_interval() {
        for (from, until, want) in [
            (0, 3_600_000, 15),
            (0, 86_400_000, 60),
            (0, 604_800_000, 300),
            (0, 2_592_000_000, 1_800),
            (1_000, 0, 15),
        ] {
            assert_eq!(point_interval_s(from, until), want, "{from}..{until}");
        }
    }

    /// R2: both ends aligned down to the step, the gaps backfilled, a
    /// point at the aligned end left out.
    #[test]
    fn r2_timeline() {
        let points = [
            (1_000_007_000, 1.0),
            (1_000_037_000, 2.0),
            (1_000_082_000, 3.0),
            (1_000_097_000, 4.0),
        ];
        assert_eq!(
            timeline(&points, 1_000_007_000, 1_000_097_000, 15),
            (1_000_005, vec![1, 0, 2, 0, 0, 3])
        );
        assert_eq!(
            timeline(&[], 1_000_007_000, 1_000_097_000, 15),
            (1_000_005, vec![0; 6])
        );
        assert_eq!(
            timeline(&points, 1_000_007_000, 1_000_007_000, 15),
            (1_000_005, Vec::new())
        );
    }

    /// R3: one statement holding S5 and S6 verbatim under one `UNION ALL`,
    /// every profile read scoped to the tenant; nothing before the floor.
    #[test]
    fn r3_one_statement() {
        let t1 = tenant("t1");
        let config = single();
        let req = TreeRequest {
            profile_type: CPU,
            label_selector: r#"{service_name="svc"}"#,
            start_ms: START,
            end_ms: END,
            ..TreeRequest::default()
        };
        let render = RenderRequest {
            tree: req,
            group_by: &[],
            average: false,
        };
        let planned = match plan(&t1, &config, &render, END) {
            Ok(Some(p)) => p,
            other => panic!("expected a plan, got {other:?}"),
        };
        let s5 = tree::plan(&t1, &config, &req, END)
            .expect("S5")
            .expect("a plan")
            .sql;
        let s6 = series::plan(
            &t1,
            &config,
            &SeriesRequest {
                profile_type: CPU,
                label_selector: r#"{service_name="svc"}"#,
                start_ms: START,
                end_ms: END,
                step: 15.0,
                ..SeriesRequest::default()
            },
            END,
        )
        .expect("S6")
        .expect("a plan");
        assert!(planned.sql.contains(&s5), "S5 missing:\n{}", planned.sql);
        assert!(planned.sql.contains(&s6), "S6 missing:\n{}", planned.sql);
        assert_eq!(
            planned.sql.matches("UNION ALL").count(),
            1,
            "{}",
            planned.sql
        );
        assert_eq!(planned.step_s, 15);
        assert_eq!(
            tenant_scope::check(
                &planned.sql,
                &TenantTerm::of(&t1),
                &tenant_scope::tables("")
            ),
            Ok(())
        );
        let old = RenderRequest {
            tree: TreeRequest {
                start_ms: END - 8 * DAY_MS - 3_600_000,
                end_ms: END - 8 * DAY_MS,
                ..req
            },
            ..render
        };
        assert_eq!(plan(&t1, &config, &old, END), Ok(None));
    }
}
