//! Issue #828: the merged profile as a pprof `Profile` — `SelectMergeProfile`,
//! `SelectMergeStacktraces` with `PROFILE_FORMAT_PPROF`, and
//! `GET /api/profiles/v1/export` — planned as one statement (S9).

use pulsus_model::Tenant;
use pulsus_profiles_wire::google_v1::Profile;
use serde::{Deserialize, Serialize};

use super::engine::{ProfilesReadConfig, ProfilesReadError};

/// What a pprof request selects: [`super::tree::TreeRequest`]'s fields,
/// with `go_pgo` as `(keep_locations, aggregate_callees)`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PprofRequest<'a> {
    pub profile_type: &'a str,
    pub label_selector: &'a str,
    pub start_ms: i64,
    pub end_ms: i64,
    /// The request's `max_nodes`, `None` when absent.
    pub max_nodes: Option<i64>,
    /// `stack_trace_selector.call_site[].name`, root first.
    pub call_site: &'a [String],
    pub span_selector: &'a [String],
    pub trace_id_selector: &'a [String],
    pub profile_id_selector: &'a [String],
    pub go_pgo: Option<(u32, bool)>,
}

/// A planned request: S9's text and what the build needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub sql: String,
    pub window_end_ms: i64,
    /// The five-part type id.
    pub profile_type: String,
    /// The request's `max_nodes`, 0 when absent.
    pub max_nodes: i64,
    /// Whether the request carries a call site.
    pub call_site: bool,
    pub go_pgo: Option<(u32, bool)>,
}

/// One row of S9: a merged stack (`kind` 0) or a location it names
/// (`kind` 1).
#[derive(Debug, Clone, Default, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
pub struct PprofRow {
    pub period: i64,
    pub kind: u8,
    pub k: u128,
    pub location_ids: Vec<u128>,
    pub v: i64,
    pub mapping_file: String,
    pub mapping_build_id: String,
    pub mapping_start: u64,
    pub mapping_limit: u64,
    pub mapping_offset: u64,
    pub mapping_flags: u8,
    pub address: u64,
    pub line_functions: Vec<String>,
    pub line_system_names: Vec<String>,
    pub line_files: Vec<String>,
    pub line_start_lines: Vec<i64>,
    pub line_numbers: Vec<i64>,
}

/// Plans `req` for `tenant`: `Ok(None)` when its window ends before the
/// lookback floor, so nothing is read.
pub fn plan(
    _tenant: &Tenant,
    _config: &ProfilesReadConfig,
    _req: &PprofRequest<'_>,
    _now_ms: i64,
) -> Result<Option<Planned>, ProfilesReadError> {
    Ok(None)
}

/// The `Profile` of S9's rows.
pub fn build(_planned: &Planned, _rows: Vec<PprofRow>) -> Profile {
    Profile::default()
}

#[cfg(test)]
#[path = "../../tests/profileql_pprof_nf/mod.rs"]
mod nf;

#[cfg(test)]
mod tests {
    use super::nf::{self, Nf, P1, P2, P4, P6, PERIOD, UNIT};
    use super::*;
    use crate::profiles::selector::WindowLimits;
    use crate::profiles::tenant_scope;
    use crate::traces::tenant_scope::TenantTerm;
    use prost::Message;

    const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    /// 2026-11-16T09:00Z and 10:00Z.
    const START: i64 = 1_794_819_600_000;
    const END: i64 = 1_794_823_200_000;

    /// Plan §0's statement.
    const EXAMPLE: &str = "SELECT max(max(o.pv)) OVER () AS period,
       o.kind AS kind, o.k AS k, any(o.locs) AS location_ids, any(o.val) AS v,
       any(l.mapping_file) AS mapping_file, any(l.mapping_build_id) AS mapping_build_id,
       any(l.mapping_start) AS mapping_start, any(l.mapping_limit) AS mapping_limit,
       any(l.mapping_offset) AS mapping_offset, any(l.mapping_flags) AS mapping_flags,
       any(l.address) AS address, any(l.line_functions) AS line_functions,
       any(l.line_system_names) AS line_system_names, any(l.line_files) AS line_files,
       any(l.line_start_lines) AS line_start_lines, any(l.line_numbers) AS line_numbers
FROM (
  SELECT location_id, mapping_file, mapping_build_id, mapping_start, mapping_limit, mapping_offset,
         mapping_flags, address, line_functions, line_system_names, line_files, line_start_lines, line_numbers
  FROM profile_locations
  WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
) AS l
RIGHT JOIN (
  SELECT tupleElement(t, 1) AS kind, tupleElement(t, 2) AS k, tupleElement(t, 3) AS locs, tupleElement(t, 4) AS val, tupleElement(t, 5) AS pv
  FROM (
    SELECT any(st.location_ids) AS location_ids, any(agg.v) AS v, any(agg.p) AS p
    FROM (
      SELECT stack_id, location_ids
      FROM profile_stacks
      WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
    ) AS st
    INNER JOIN (
      SELECT stack_id, sum(value) AS v, max(period) AS p
      FROM profile_samples
      WHERE org_id = 't1' AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
        AND service = 's0'
        AND fingerprint IN (SELECT fingerprint
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1536, day = '2026-11-16', 16776704, day = '2026-11-16', 2047, 16777215)) != 0
  AND service = 's0'
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds')
        AND timestamp_ns >= 1794819600000000000 AND timestamp_ns <= 1794823200000000000
      GROUP BY stack_id
    ) AS agg ON st.stack_id = agg.stack_id
    GROUP BY st.stack_id
  )
  ARRAY JOIN arrayConcat([(toUInt8(0), reinterpretAsUInt128(sipHash128(location_ids)), location_ids, v, p)],
                         arrayMap(x -> (toUInt8(1), x, CAST([], 'Array(UInt128)'), toInt64(0), toInt64(0)), location_ids)) AS t
) AS o ON l.location_id = o.k
GROUP BY kind, k";

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

    /// Plan §0's request.
    fn example() -> PprofRequest<'static> {
        PprofRequest {
            profile_type: CPU,
            label_selector: r#"{service_name="s0"}"#,
            start_ms: START,
            end_ms: END,
            max_nodes: Some(16_384),
            ..PprofRequest::default()
        }
    }

    fn planned_as(org: &str, config: &ProfilesReadConfig, req: &PprofRequest<'_>) -> Planned {
        match plan(&tenant(org), config, req, END) {
            Ok(Some(p)) => p,
            other => panic!("expected a plan, got {other:?}"),
        }
    }

    fn planned(config: &ProfilesReadConfig, req: &PprofRequest<'_>) -> Planned {
        planned_as("t1", config, req)
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    // -----------------------------------------------------------------
    // The build, on fixture P's rows
    // -----------------------------------------------------------------

    /// A location row of kind 1.
    fn location_row(
        k: u128,
        mapping: (&str, &str, u64, u64, u8),
        address: u64,
        lines: &[(&str, i64, i64)],
    ) -> PprofRow {
        PprofRow {
            period: PERIOD,
            kind: 1,
            k,
            mapping_file: mapping.0.to_string(),
            mapping_build_id: mapping.1.to_string(),
            mapping_start: mapping.2,
            mapping_limit: mapping.3,
            mapping_flags: mapping.4,
            address,
            line_functions: lines.iter().map(|l| l.0.to_string()).collect(),
            line_system_names: lines.iter().map(|l| l.0.to_string()).collect(),
            line_files: lines.iter().map(|_| "main.go".to_string()).collect(),
            line_start_lines: lines.iter().map(|l| l.1).collect(),
            line_numbers: lines.iter().map(|l| l.2).collect(),
            ..PprofRow::default()
        }
    }

    fn stack_row(location_ids: &[u128], v: i64) -> PprofRow {
        PprofRow {
            period: PERIOD,
            kind: 0,
            k: location_ids
                .iter()
                .fold(7u128, |h, x| h.wrapping_mul(31) ^ x),
            location_ids: location_ids.to_vec(),
            v,
            ..PprofRow::default()
        }
    }

    /// P's nine location rows, `k` = 1–9.
    fn p_locations() -> Vec<PprofRow> {
        let mut out: Vec<PprofRow> = nf::LINES
            .iter()
            .enumerate()
            .map(|(i, (name, line))| {
                let start = nf::function(name).3;
                location_row(
                    i as u128 + 1,
                    ("app", "bid", 0, 0, 15),
                    0,
                    &[(name, start, *line)],
                )
            })
            .collect();
        out.push(location_row(
            9,
            ("libx.so", "", nf::LIBX_START, nf::LIBX_LIMIT, 0),
            nf::L9_ADDRESS,
            &[],
        ));
        out
    }

    /// P's rows as S9 returns them for a request with no go_pgo: the
    /// stacks `samples` (P's indices, counts) and the locations they name.
    fn p_rows(samples: &[(&[u128], i64)]) -> Vec<PprofRow> {
        let mut rows: Vec<PprofRow> = samples
            .iter()
            .map(|(ids, count)| stack_row(ids, count * UNIT))
            .collect();
        let named: std::collections::BTreeSet<u128> = samples
            .iter()
            .flat_map(|(ids, _)| ids.iter().copied())
            .collect();
        rows.extend(p_locations().into_iter().filter(|l| named.contains(&l.k)));
        rows
    }

    fn p_all() -> Vec<PprofRow> {
        p_rows(&[
            (&[2, 1], 2),
            (&[3, 1], 3),
            (&[4, 2, 1], 10),
            (&[5, 3, 1], 1),
            (&[7, 6, 1], 4),
            (&[8], 1),
            (&[9], 6),
        ])
    }

    fn planned_for(max_nodes: i64, call_site: bool, go_pgo: Option<(u32, bool)>) -> Planned {
        Planned {
            sql: String::new(),
            window_end_ms: END,
            profile_type: CPU.to_string(),
            max_nodes,
            call_site,
            go_pgo,
        }
    }

    fn built(
        max_nodes: i64,
        call_site: bool,
        go_pgo: Option<(u32, bool)>,
        rows: Vec<PprofRow>,
    ) -> Nf {
        let p = build(&planned_for(max_nodes, call_site, go_pgo), rows);
        let bytes = p.encode_to_vec();
        nf::nf(&Profile::decode(&bytes[..]).expect("the built profile decodes"))
    }

    fn want(samples: &[(&str, i64)]) -> Nf {
        nf::expected(samples, UNIT, PERIOD, END * 1_000_000)
    }

    /// P1: no `max_nodes` → every stack as stored.
    #[test]
    fn p1_full() {
        assert_eq!(built(0, false, None, p_all()), want(&P1));
    }

    /// P2: `max_nodes` 3 → `min` 10: `c`, `d`, `e`, `zz` cut exactly, the
    /// address-only `L9` kept.
    #[test]
    fn p2_cut_three() {
        assert_eq!(built(3, false, None, p_all()), want(&P2));
    }

    /// P3: `max_nodes` 16384 → `min` 0: P1, `L9` kept.
    #[test]
    fn p3_cut_16384() {
        assert_eq!(built(16_384, false, None, p_all()), want(&P1));
    }

    /// P4: the call site `[main, a]`'s rows with `max_nodes` 500.
    #[test]
    fn p4_call_site() {
        let rows = p_rows(&[
            (&[2, 1], 2),
            (&[3, 1], 3),
            (&[4, 2, 1], 10),
            (&[5, 3, 1], 1),
        ]);
        assert_eq!(built(500, true, None, rows), want(&P4));
    }

    /// P6: `go_pgo{keep 1}`: the statement's sliced rows, one sample each.
    #[test]
    fn p6_pgo_keep_one() {
        let rows = p_rows(&[
            (&[2], 2),
            (&[3], 3),
            (&[4], 10),
            (&[5], 1),
            (&[7], 4),
            (&[8], 1),
            (&[9], 6),
        ]);
        assert_eq!(built(0, false, Some((1, false)), rows), want(&P6));
    }

    /// P11: a location's inlined lines are frames of the cut: a location
    /// whose lines are all marked goes whole, one partly marked keeps only
    /// its unmarked lines.
    #[test]
    fn p11_inlined_lines_count_as_frames() {
        let main = location_row(1, ("app", "bid", 0, 0, 15), 0, &[("main", 1, 10)]);
        let x = location_row(20, ("app", "bid", 0, 0, 15), 0, &[("g", 8, 1), ("f", 9, 2)]);
        let h = location_row(21, ("app", "bid", 0, 0, 15), 0, &[("h", 10, 3)]);
        let rows = vec![
            stack_row(&[20, 1], UNIT),
            stack_row(&[21, 1], 10 * UNIT),
            main.clone(),
            x,
            h,
        ];
        let line = |name: &str, start: i64, n: i64| {
            (
                (
                    name.to_string(),
                    name.to_string(),
                    "main.go".to_string(),
                    start,
                ),
                n,
            )
        };
        let loc = |lines: Vec<((String, String, String, i64), i64)>| nf::NfLocation {
            mapping: Some(nf::mapping1()),
            address: 0,
            is_folded: false,
            lines,
        };
        let l1 = loc(vec![line("main", 1, 10)]);
        let mut expect = nf::header(PERIOD, END * 1_000_000);
        expect
            .samples
            .insert((vec![nf::stub(), l1.clone()], vec![UNIT]), 1);
        expect.samples.insert(
            (
                vec![loc(vec![line("h", 10, 3)]), l1.clone()],
                vec![10 * UNIT],
            ),
            1,
        );
        assert_eq!(built(2, false, None, rows), expect, "Lx taken whole");

        let y = location_row(22, ("app", "bid", 0, 0, 15), 0, &[("g", 8, 1), ("f", 9, 2)]);
        let z = location_row(
            23,
            ("app", "bid", 0, 0, 15),
            0,
            &[("h", 10, 3), ("f", 9, 2)],
        );
        let rows = vec![
            stack_row(&[22, 1], UNIT),
            stack_row(&[23, 1], 10 * UNIT),
            main,
            y,
            z,
        ];
        let mut expect = nf::header(PERIOD, END * 1_000_000);
        expect.samples.insert(
            (
                vec![nf::stub(), loc(vec![line("f", 9, 2)]), l1.clone()],
                vec![UNIT],
            ),
            1,
        );
        expect.samples.insert(
            (
                vec![loc(vec![line("h", 10, 3), line("f", 9, 2)]), l1],
                vec![10 * UNIT],
            ),
            1,
        );
        assert_eq!(built(3, false, None, rows), expect, "Ly split");
    }

    /// P8: no rows → the reference's six bytes.
    #[test]
    fn p8_no_rows() {
        let bytes = build(&planned_for(3, false, None), Vec::new()).encode_to_vec();
        assert_eq!(bytes, [0x0a, 0x00, 0x32, 0x00, 0x5a, 0x00]);
    }

    /// P9: the pushed period, absent when 0; `time_nanos` the window's end.
    #[test]
    fn p9_period() {
        let p = build(&planned_for(0, false, None), p_all());
        assert_eq!((p.period, p.time_nanos), (PERIOD, END * 1_000_000));
        let rows: Vec<PprofRow> = p_all()
            .into_iter()
            .map(|r| PprofRow { period: 0, ..r })
            .collect();
        let p = build(&planned_for(0, false, None), rows);
        assert_eq!((p.period, p.time_nanos), (0, END * 1_000_000));
        assert!(!p.sample.is_empty(), "{p:?}");
    }

    /// P10: one request's bytes do not depend on the rows' order.
    #[test]
    fn p10_same_bytes() {
        let a = build(&planned_for(3, false, None), p_all()).encode_to_vec();
        let mut rows = p_all();
        rows.reverse();
        let b = build(&planned_for(3, false, None), rows).encode_to_vec();
        assert!(!a.is_empty());
        assert_eq!(a, b);
    }

    // -----------------------------------------------------------------
    // The statement
    // -----------------------------------------------------------------

    /// S1: plan §0's request renders plan §0's statement, byte for byte.
    #[test]
    fn s1_the_example_renders_the_example_statement() {
        let p = planned(&single(), &example());
        assert_eq!(p.sql, EXAMPLE);
        assert_eq!(
            (p.window_end_ms, p.profile_type.as_str(), p.max_nodes),
            (END, CPU, 16_384)
        );
    }

    /// S2: clustered, the samples and the series through their wrappers; the
    /// stacks and the locations, on every node, read locally.
    #[test]
    fn s2_clustered() {
        let sql = planned(&clustered(), &example()).sql;
        for expected in [
            "FROM profile_samples_dist\n",
            "\nFROM profile_series_dist\n",
            "FROM profile_stacks\n",
            "FROM profile_locations\n",
        ] {
            assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        }
        assert!(!sql.contains("profile_stacks_dist"), "{sql}");
        assert!(!sql.contains("profile_locations_dist"), "{sql}");
    }

    /// S3: `go_pgo` takes the slice and wins over a call site; a call site
    /// alone filters the stacks by name; `aggregate_callees` takes the
    /// second template.
    #[test]
    fn s3_pgo_and_call_site() {
        let call_site = strings(&["main", "a"]);
        let both = planned(
            &single(),
            &PprofRequest {
                call_site: &call_site,
                go_pgo: Some((5, false)),
                ..example()
            },
        );
        assert!(
            both.sql.contains(
                "SELECT arraySlice(location_ids, 1, 5) AS location_ids, sum(v) AS v, max(p) AS p"
            ),
            "{}",
            both.sql
        );
        assert!(!both.sql.contains("arraySlice(names"), "{}", both.sql);
        assert!(!both.sql.contains("GROUPING SETS"), "{}", both.sql);
        assert!(!both.call_site, "go_pgo wins over the call site");

        let site = planned(
            &single(),
            &PprofRequest {
                call_site: &call_site,
                ..example()
            },
        );
        assert!(
            site.sql
                .contains("AND arraySlice(names, 1, 2) = ['main', 'a']"),
            "{}",
            site.sql
        );
        assert!(site.call_site);

        let aggregate = planned(
            &single(),
            &PprofRequest {
                go_pgo: Some((5, true)),
                ..example()
            },
        );
        for expected in [
            "GROUP BY GROUPING SETS ((j.sk), (j.eid))",
            "arrayMap(x -> x.2, arraySort(groupUniqArray((j.pos, j.eid)))) AS path",
            "SELECT arraySlice(location_ids, 1, 5) AS location_ids, sum(v) AS v, max(p) AS p",
            "ARRAY JOIN arrayEnumerate(location_ids) AS pos",
            "SELECT max(max(a.p)) OVER () AS period",
        ] {
            assert!(
                aggregate.sql.contains(expected),
                "{expected:?} missing from\n{}",
                aggregate.sql
            );
        }
        assert_eq!(aggregate.go_pgo, Some((5, true)));
    }

    /// S4: the tenant check accepts every rendering, under each tenant.
    #[test]
    fn s4_tenant_check() {
        for org in ["t1", "t2"] {
            let term = TenantTerm::of(&tenant(org));
            for go_pgo in [None, Some((5, false)), Some((0, true)), Some((5, true))] {
                let req = PprofRequest {
                    go_pgo,
                    ..example()
                };
                let one = planned_as(org, &single(), &req).sql;
                assert_eq!(
                    tenant_scope::check(&one, &term, &tenant_scope::tables("")),
                    Ok(()),
                    "{org} {go_pgo:?}"
                );
                let two = planned_as(org, &clustered(), &req).sql;
                assert_eq!(
                    tenant_scope::check(&two, &term, &tenant_scope::tables("_dist")),
                    Ok(()),
                    "{org} {go_pgo:?} clustered"
                );
            }
        }
    }

    /// S5: `max_nodes` is not validated: negative, 0 and above the flame
    /// graph's largest all plan.
    #[test]
    fn s5_max_nodes_is_not_validated() {
        for n in [-1, 0, 2_000_000] {
            let p = planned(
                &single(),
                &PprofRequest {
                    max_nodes: Some(n),
                    ..example()
                },
            );
            assert_eq!(p.max_nodes, n);
        }
        let p = planned(
            &single(),
            &PprofRequest {
                max_nodes: None,
                ..example()
            },
        );
        assert_eq!(p.max_nodes, 0);
    }

    /// S6: the period is read from the samples the statement sums, and
    /// nothing reads the entries.
    #[test]
    fn s6_period_rides_with_the_samples() {
        let sql = planned(&single(), &example()).sql;
        for expected in [
            "SELECT stack_id, sum(value) AS v, max(period) AS p",
            "SELECT max(max(o.pv)) OVER () AS period",
        ] {
            assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        }
        assert!(!sql.contains("profile_entries"), "{sql}");
    }

    /// A window ending before the lookback floor plans nothing.
    #[test]
    fn a_window_before_the_floor_plans_nothing() {
        let now = END + 8 * 86_400_000;
        assert_eq!(plan(&tenant("t1"), &single(), &example(), now), Ok(None));
    }
}
