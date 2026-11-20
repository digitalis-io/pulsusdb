//! Issue #829: `render` planned as one statement — the merged tree (S5,
//! `super::tree`) and the time series of profile totals (S6,
//! `super::series`) of one selection under `UNION ALL`, each row tagged by
//! a `part` column — and the reference's timeline over the series.

use pulsus_model::Tenant;
use serde::{Deserialize, Serialize};

use super::engine::{ProfilesReadConfig, ProfilesReadError};
use super::series::{self, SeriesRequest, TimeSeriesRow};
use super::tree::{self, TreeRequest, TreeRow};

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
/// lookback floor, so nothing is read. S5 and S6 compile the same type,
/// selector and window, so they agree on that; each is checked as its own
/// method checks it, S5 first.
pub fn plan(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    req: &RenderRequest<'_>,
    now_ms: i64,
) -> Result<Option<Planned>, ProfilesReadError> {
    let Some(tree) = tree::plan(tenant, config, &req.tree, now_ms)? else {
        return Ok(None);
    };
    let step_s = point_interval_s(req.tree.start_ms, req.tree.end_ms);
    let series = SeriesRequest {
        profile_type: req.tree.profile_type,
        label_selector: req.tree.label_selector,
        start_ms: req.tree.start_ms,
        end_ms: req.tree.end_ms,
        step: step_s as f64,
        group_by: req.group_by,
        average: req.average,
        call_site: &[],
        limit: None,
        exemplars: false,
    };
    let Some(series) = series::plan(tenant, config, &series, now_ms)? else {
        return Ok(None);
    };
    let sql = format!(
        "SELECT 0 AS part, node, parent, name, total, self_value,
       CAST([] AS Array(String)) AS g, toFloat64(0) AS series_total,
       CAST([] AS Array(Tuple(Int64, Float64))) AS points
FROM (
{tree}
)
UNION ALL
SELECT 1, toUInt128(0), toUInt128(0), '', toInt64(0), toInt64(0), g, series_total, points
FROM (
{series}
)",
        tree = tree.sql,
    );
    Ok(Some(Planned {
        sql,
        max_nodes: tree.max_nodes,
        raw_max_nodes: tree.raw_max_nodes,
        step_s,
    }))
}

/// The statement's rows split by `part`. The series' order does not
/// survive the union; `super::series::series` orders them.
pub fn split(rows: Vec<RenderRow>) -> (Vec<TreeRow>, Vec<TimeSeriesRow>) {
    let mut tree = Vec::with_capacity(rows.len());
    let mut series = Vec::new();
    for r in rows {
        if r.part == 0 {
            tree.push(TreeRow {
                node: r.node,
                parent: r.parent,
                name: r.name,
                total: r.total,
                self_value: r.self_value,
            });
        } else {
            series.push(TimeSeriesRow {
                g: r.g,
                series_total: r.series_total,
                points: r.points,
            });
        }
    }
    (tree, series)
}

/// The series step in seconds, as the reference's `CalcPointInterval`
/// computes it (`pkg/querier/timeline/calculator.go:18-130`): the window
/// over 1,500 points, below 15 s is 15 s, else the rounding table's step.
/// Every step it can answer is a whole number of seconds.
pub fn point_interval_s(from_ms: i64, until_ms: i64) -> i64 {
    const RESOLUTION: i64 = 1_500;
    const MIN_S: i64 = 15;
    const MS: i64 = 1_000_000;
    /// `roundInterval`'s rows from 15 s up: an interval of at most the
    /// first value, in ms, has the second as its step, in seconds.
    const ROUND: [(i64, i64); 18] = [
        (17_500, 15),
        (25_000, 20),
        (45_000, 30),
        (90_000, 60),
        (210_000, 120),
        (450_000, 300),
        (750_000, 600),
        (1_050_000, 900),
        (1_500_000, 1_200),
        (2_700_000, 1_800),
        (5_400_000, 3_600),
        (9_000_000, 7_200),
        (16_200_000, 10_800),
        (32_400_000, 21_600),
        (86_400_000, 43_200),
        (172_800_000, 86_400),
        (604_800_000, 86_400),
        (1_814_400_000, 604_800),
    ];
    let interval_ns = until_ms.saturating_sub(from_ms).saturating_mul(MS) / RESOLUTION;
    if interval_ns < MIN_S * 1_000 * MS {
        return MIN_S;
    }
    if let Some((_, step)) = ROUND.iter().find(|(max, _)| interval_ns <= max * MS) {
        return *step;
    }
    // The table's last two rows: below 6 weeks is 30 days, else a year.
    if interval_ns < 3_628_800_000 * MS {
        2_592_000
    } else {
        31_536_000
    }
}

/// The reference's `timeline.New` (`pkg/querier/timeline/timeline.go:16-98`):
/// the start in seconds, and one sample per step from the start, both
/// ends aligned down to the step, the gaps and the tail backfilled with 0.
/// `points` sorted, `(ms, value)`; those outside `[start, end)` after the
/// alignment are left out. Go's and Rust's `/` both truncate toward zero.
pub fn timeline(points: &[(i64, f64)], start_ms: i64, end_ms: i64, step_s: i64) -> (i64, Vec<u64>) {
    let d = step_s * 1000;
    let (s, e) = (start_ms / d * d, end_ms / d * d);
    let size = |a: i64, b: i64| (b / 1000 - a / 1000) / step_s;
    if s >= e {
        return (s / 1000, Vec::new());
    }
    let cap = size(s, e).max(0) as usize;
    let mut pts = points.iter().filter(|p| p.0 >= s && p.0 < e).peekable();
    let mut out: Vec<u64> = Vec::with_capacity(cap);
    if let Some(first) = pts.peek() {
        out.resize(size(s, first.0).max(0) as usize, 0);
        let mut prev = first.0;
        for p in pts {
            let n = size(prev, p.0) - 1;
            if n > 0 {
                out.resize(out.len() + n as usize, 0);
            }
            out.push(p.1 as u64);
            prev = p.0;
        }
    }
    if out.len() < cap {
        out.resize(cap, 0);
    }
    (s / 1000, out)
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
