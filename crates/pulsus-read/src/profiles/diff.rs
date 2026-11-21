//! Issue #827: `Diff` planned as one statement (S7) — both selections'
//! merged trees side by side, one row per node of their union, computed in
//! ClickHouse. `super::flamegraph_diff` trims each side with its own
//! `max_nodes` and combines them.
//!
//! Each side is one branch that sums its own samples per stack; the
//! branches are `UNION ALL`-ed and summed per stack into `vl` and `vr`,
//! joined once to the tenant's stack rows for both sides' receive days, and
//! aggregated per node into each side's total and self.

use pulsus_model::Tenant;
use serde::{Deserialize, Serialize};

use crate::logql::escape::ch_string;
use crate::traces::tenant_scope::TenantTerm;

use super::engine::{ProfilesReadConfig, ProfilesReadError};
use super::tree::{self, TreeRequest};

/// A planned `Diff`: S7's text and each side's resolved `max_nodes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub sql: String,
    pub left_max_nodes: i64,
    pub right_max_nodes: i64,
}

/// One node of the union of the two merged trees, as S7 returns it. A
/// side's total is 0 on a node outside its tree.
#[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
pub struct DiffRow {
    pub node: u128,
    /// 0 for a root.
    pub parent: u128,
    pub name: String,
    pub left_total: i64,
    pub left_self: i64,
    pub right_total: i64,
    pub right_self: i64,
}

/// Plans `left` against `right` for `tenant`: `Ok(None)` when both windows
/// end before the lookback floor, so nothing is read. In the order: the
/// left side's `max_nodes`, then the right's, then the left side, then the
/// right ([`tree::side`]). A side whose window ends before the floor has no
/// branch, and its sums are 0.
pub fn plan(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    left: &TreeRequest<'_>,
    right: &TreeRequest<'_>,
    now_ms: i64,
) -> Result<Option<Planned>, ProfilesReadError> {
    let left_max_nodes = tree::resolve_max_nodes(left.max_nodes.unwrap_or(0), config)?;
    let right_max_nodes = tree::resolve_max_nodes(right.max_nodes.unwrap_or(0), config)?;
    let sides = [
        tree::side(tenant, config, left, now_ms)?,
        tree::side(tenant, config, right, now_ms)?,
    ];
    if sides.iter().all(Option::is_none) {
        return Ok(None);
    }
    let tenant_term = TenantTerm::of(tenant);

    let mut ranges: Vec<String> = Vec::new();
    for side in sides.iter().flatten() {
        let (d0, d1) = side.days();
        let range = format!("day BETWEEN '{d0}' AND '{d1}'");
        if !ranges.contains(&range) {
            ranges.push(range);
        }
    }
    let days = if ranges.len() == 1 {
        ranges.remove(0)
    } else {
        format!("({})", ranges.join(" OR "))
    };

    // A present side's call site, tested on the joined stack's frames.
    let call_sites: Vec<Option<String>> = [left.call_site, right.call_site]
        .iter()
        .zip(&sides)
        .map(|(names, side)| {
            (side.is_some() && !names.is_empty())
                .then(|| tree::call_site_predicate("any(st.names)", names))
        })
        .collect();
    let sum = |k: usize, column: &str| match &call_sites[k] {
        Some(cs) => format!("if({cs}, any(agg.{column}), 0)"),
        None => format!("any(agg.{column})"),
    };
    let (vl, vr) = (sum(0, "vl"), sum(1, "vr"));
    let having = if call_sites.iter().any(Option::is_some) {
        "\n  HAVING vl > 0 OR vr > 0"
    } else {
        ""
    };

    let branches: Vec<String> = sides
        .iter()
        .enumerate()
        .filter_map(|(k, side)| side.as_ref().map(|s| (k, s)))
        .map(|(k, side)| {
            format!(
                "      SELECT {k} AS side, stack_id, sum(value) AS v
      FROM {samples}
      WHERE {tenant_term} AND type_id = {type_id}{service_terms}
      AND fingerprint IN ({series})
      AND timestamp_ns >= {start_ns} AND timestamp_ns <= {end_ns}{filters}
      GROUP BY stack_id",
                samples = config.samples_table,
                type_id = ch_string(side.type_id()),
                service_terms = crate::metrics::profile_service_terms(&side.matchers),
                series = side.series,
                start_ns = side.window.start_ms.saturating_mul(1_000_000),
                end_ns = side.window.end_ms.saturating_mul(1_000_000),
                filters = side.filters,
            )
        })
        .collect();
    let sql = format!(
        "SELECT node_ids[i] AS node,
       any(if(i = 1, toUInt128(0), node_ids[i - 1])) AS parent,
       any(names[i]) AS name,
       sum(vl) AS left_total,
       sumIf(vl, i = length(node_ids)) AS left_self,
       sum(vr) AS right_total,
       sumIf(vr, i = length(node_ids)) AS right_self
FROM (
  SELECT st.stack_id AS stack_id, any(st.node_ids) AS node_ids, any(st.names) AS names,
         {vl} AS vl, {vr} AS vr
  FROM (
    SELECT stack_id, node_ids, names
    FROM profile_stacks
    WHERE {tenant_term} AND {days}
  ) AS st
  INNER JOIN (
    SELECT stack_id, sumIf(v, side = 0) AS vl, sumIf(v, side = 1) AS vr
    FROM (
{branches}
    )
    GROUP BY stack_id
  ) AS agg ON st.stack_id = agg.stack_id
  GROUP BY st.stack_id{having}
)
ARRAY JOIN arrayEnumerate(node_ids) AS i
GROUP BY node",
        branches = branches.join("\n      UNION ALL\n"),
    );
    Ok(Some(Planned {
        sql,
        left_max_nodes,
        right_max_nodes,
    }))
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

    /// Plan §0's statement.
    const EXAMPLE: &str = "SELECT node_ids[i] AS node,
       any(if(i = 1, toUInt128(0), node_ids[i - 1])) AS parent,
       any(names[i]) AS name,
       sum(vl) AS left_total,
       sumIf(vl, i = length(node_ids)) AS left_self,
       sum(vr) AS right_total,
       sumIf(vr, i = length(node_ids)) AS right_self
FROM (
  SELECT st.stack_id AS stack_id, any(st.node_ids) AS node_ids, any(st.names) AS names,
         any(agg.vl) AS vl, any(agg.vr) AS vr
  FROM (
    SELECT stack_id, node_ids, names
    FROM profile_stacks
    WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  ) AS st
  INNER JOIN (
    SELECT stack_id, sumIf(v, side = 0) AS vl, sumIf(v, side = 1) AS vr
    FROM (
      SELECT 0 AS side, stack_id, sum(value) AS v
      FROM profile_samples
      WHERE org_id = 't1' AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
      AND service = 'checkout'
      AND fingerprint IN (SELECT fingerprint
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1536, day = '2026-11-16', 16776704, day = '2026-11-16', 2047, 16777215)) != 0
  AND service = 'checkout'
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds')
      AND timestamp_ns >= 1794819600000000000 AND timestamp_ns <= 1794823200000000000
      GROUP BY stack_id
      UNION ALL
      SELECT 1 AS side, stack_id, sum(value) AS v
      FROM profile_samples
      WHERE org_id = 't1' AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
      AND service = 'payment'
      AND fingerprint IN (SELECT fingerprint
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1536, day = '2026-11-16', 16776704, day = '2026-11-16', 2047, 16777215)) != 0
  AND service = 'payment'
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds')
      AND timestamp_ns >= 1794819600000000000 AND timestamp_ns <= 1794823200000000000
      GROUP BY stack_id
    )
    GROUP BY stack_id
  ) AS agg ON st.stack_id = agg.stack_id
  GROUP BY st.stack_id
)
ARRAY JOIN arrayEnumerate(node_ids) AS i
GROUP BY node";

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

    fn side(service: &'static str) -> TreeRequest<'static> {
        TreeRequest {
            profile_type: CPU,
            label_selector: service,
            start_ms: START,
            end_ms: END,
            ..TreeRequest::default()
        }
    }

    fn left() -> TreeRequest<'static> {
        side(r#"{service_name="checkout"}"#)
    }

    fn right() -> TreeRequest<'static> {
        side(r#"{service_name="payment"}"#)
    }

    fn planned(
        config: &ProfilesReadConfig,
        l: &TreeRequest<'_>,
        r: &TreeRequest<'_>,
        now: i64,
    ) -> Planned {
        match plan(&tenant("t1"), config, l, r, now) {
            Ok(Some(p)) => p,
            other => panic!("expected a plan, got {other:?}"),
        }
    }

    fn refused(l: &TreeRequest<'_>, r: &TreeRequest<'_>) -> String {
        match plan(&tenant("t1"), &single(), l, r, END) {
            Err(ProfilesReadError::InvalidArgument(m)) => m,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// D1: plan §0's request renders plan §0's statement, byte for byte.
    #[test]
    fn d1_the_example_renders_the_example_statement() {
        let p = planned(&single(), &left(), &right(), END);
        assert_eq!(p.sql, EXAMPLE);
    }

    /// D2: clustered, each side reads the samples and the series through
    /// their wrappers; the stacks are read once, locally.
    #[test]
    fn d2_clustered_reads_the_samples_wrapper() {
        let sql = planned(&clustered(), &left(), &right(), END).sql;
        for (expected, n) in [
            ("\n      FROM profile_samples_dist\n", 2),
            ("\nFROM profile_series_dist\n", 2),
            ("\n    FROM profile_stacks\n", 1),
        ] {
            assert_eq!(sql.matches(expected).count(), n, "{expected:?} in\n{sql}");
        }
        assert!(!sql.contains("profile_stacks_dist"), "{sql}");
    }

    /// D3: a side whose window ends before the lookback floor has no branch;
    /// both such sides plan nothing.
    #[test]
    fn d3_an_empty_side_has_no_branch() {
        let old = TreeRequest {
            start_ms: START - 8 * DAY_MS,
            end_ms: END - 8 * DAY_MS,
            ..right()
        };
        let sql = planned(&single(), &left(), &old, END).sql;
        assert_eq!(sql.matches("SELECT 0 AS side").count(), 1, "{sql}");
        assert!(!sql.contains("UNION ALL"), "{sql}");
        assert!(!sql.contains("SELECT 1 AS side"), "{sql}");
        let old_left = TreeRequest {
            start_ms: START - 8 * DAY_MS,
            end_ms: END - 8 * DAY_MS,
            ..left()
        };
        assert_eq!(
            plan(&tenant("t1"), &single(), &old_left, &old, END),
            Ok(None)
        );
    }

    /// D4: two windows a week apart read two day ranges of the stacks, not
    /// the week between; equal windows read one.
    #[test]
    fn d4_two_windows_on_two_days_read_two_day_ranges() {
        let week_ago = TreeRequest {
            start_ms: START - 7 * DAY_MS,
            end_ms: END - 7 * DAY_MS,
            ..right()
        };
        let sql = planned(&single(), &left(), &week_ago, END).sql;
        let expected = "\n    FROM profile_stacks\n    WHERE org_id = 't1' AND \
                        (day BETWEEN '2026-11-16' AND '2026-11-16' OR \
                        day BETWEEN '2026-11-09' AND '2026-11-09')\n";
        assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        let sql = planned(&single(), &left(), &right(), END).sql;
        let expected = "\n    FROM profile_stacks\n    WHERE org_id = 't1' AND \
                        day BETWEEN '2026-11-16' AND '2026-11-16'\n";
        assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
    }

    /// D5: a call site keeps its side's sums on the stacks beginning with
    /// it, tested after the join, and the stacks neither side keeps are
    /// dropped before the node aggregation.
    #[test]
    fn d5_a_call_site_applies_to_its_side_only() {
        let call_site = strings(&["main", "a"]);
        let l = TreeRequest {
            call_site: &call_site,
            ..left()
        };
        let sql = planned(&single(), &l, &right(), END).sql;
        for expected in [
            "if(arraySlice(any(st.names), 1, 2) = ['main', 'a'], any(agg.vl), 0) AS vl, \
             any(agg.vr) AS vr",
            "\n  GROUP BY st.stack_id\n  HAVING vl > 0 OR vr > 0\n)",
        ] {
            assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        }
        let sql = planned(&single(), &left(), &right(), END).sql;
        assert!(!sql.contains("HAVING"), "{sql}");
        assert!(!sql.contains("arraySlice"), "{sql}");
    }

    /// D6: each side resolves its own `max_nodes`, as its flame graph does,
    /// the left first; each is refused as the flame graph's.
    #[test]
    fn d6_each_side_resolves_its_own_max_nodes() {
        let with = |l: Option<i64>, r: Option<i64>| {
            plan(
                &tenant("t1"),
                &single(),
                &TreeRequest {
                    max_nodes: l,
                    ..left()
                },
                &TreeRequest {
                    max_nodes: r,
                    ..right()
                },
                END,
            )
            .map(|p| p.map(|p| (p.left_max_nodes, p.right_max_nodes)))
        };
        assert_eq!(with(Some(4), None), Ok(Some((4, 8_192))));
        assert_eq!(with(None, None), Ok(Some((8_192, 8_192))));
        assert_eq!(
            with(Some(1_048_577), Some(1)),
            Err(ProfilesReadError::InvalidArgument(
                "max flamegraph nodes limit 1048577 is greater than allowed 1048576".to_string()
            ))
        );
        assert_eq!(
            with(Some(1), Some(1_048_577)),
            Err(ProfilesReadError::InvalidArgument(
                "max flamegraph nodes limit 1048577 is greater than allowed 1048576".to_string()
            ))
        );
    }

    /// D7: each side's span, trace and profile-id lines stay in its own
    /// branch.
    #[test]
    fn d7_each_side_keeps_its_own_filters() {
        let spans = strings(&["00f0000000000001"]);
        let traces = strings(&["0102030405060708090A0b0c0d0e0f10"]);
        let sql = planned(
            &single(),
            &TreeRequest {
                span_selector: &spans,
                ..left()
            },
            &TreeRequest {
                trace_id_selector: &traces,
                ..right()
            },
            END,
        )
        .sql;
        let (branch0, branch1) = sql.split_once("UNION ALL").expect("two branches");
        let span = "\n      AND span_id IN (72057594037989376)\n";
        let trace = "\n      AND trace_id IN (unhex('0102030405060708090a0b0c0d0e0f10'))\n";
        assert!(branch0.contains(span), "{sql}");
        assert!(!branch0.contains("trace_id IN"), "{sql}");
        assert!(branch1.contains(trace), "{sql}");
        assert!(!branch1.contains("span_id IN"), "{sql}");
        assert_eq!(
            refused(
                &left(),
                &TreeRequest {
                    span_selector: &spans,
                    trace_id_selector: &traces,
                    ..right()
                }
            ),
            "span_selector and trace_id_selector cannot be combined"
        );
    }

    /// D8: the tenant check accepts every rendering.
    #[test]
    fn d8_the_tenant_check_accepts_both() {
        for org in ["t1", "t2"] {
            let term = TenantTerm::of(&tenant(org));
            for (config, suffix) in [(single(), ""), (clustered(), "_dist")] {
                let sql = match plan(&tenant(org), &config, &left(), &right(), END) {
                    Ok(Some(p)) => p.sql,
                    other => panic!("{org}{suffix}: expected a plan, got {other:?}"),
                };
                assert_eq!(
                    tenant_scope::check(&sql, &term, &tenant_scope::tables(suffix)),
                    Ok(()),
                    "{org}{suffix}"
                );
            }
        }
    }
}
