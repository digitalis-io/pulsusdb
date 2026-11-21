//! Issue #831: u6–u9 of plan §7.1.

use super::convert::{Converted, Format, convert};
use super::*;
use crate::profiles::selector::WindowLimits;
use crate::traces::tenant_scope::TenantTerm;

use super::fx;

const L: &str = "01900000-0000-7000-8000-000000000001";
const R: &str = "01900000-0000-7000-8000-000000000002";

fn t1() -> Tenant {
    let value = http::HeaderValue::from_static("t1");
    Tenant::from_header(Some(&value), false).expect("a valid tenant")
}

fn config() -> ProfilesReadConfig {
    ProfilesReadConfig {
        series_table: "profile_series".to_string(),
        samples_table: "profile_samples".to_string(),
        entries_table: "profile_entries".to_string(),
        landing_table: "profile_landing".to_string(),
        adhoc_table: "profile_adhoc".to_string(),
        dist_suffix: String::new(),
        limits: WindowLimits::default(),
        scan_budget_rows: 1,
        max_tree_nodes: 1,
        max_result_bytes: 1,
        query_memory_bytes: 1,
        distributed: false,
        skip_unavailable_shards: false,
        max_nodes_default: 8192,
        max_nodes_max: 1_048_576,
    }
}

fn converted(format: Format, name: &str, bytes: &[u8]) -> Converted {
    convert(format, name, bytes).expect("converts")
}

/// A type row of `c`'s type `k`, as S14 answers it.
fn type_row(c: &Converted, k: usize) -> AdhocRow {
    let t = &c.types[k];
    AdhocRow {
        part: 1,
        type_ordinal: k as u16,
        selected: 1,
        format: c.format.as_str().to_string(),
        display_name: t.display_name.clone(),
        spy_name: t.spy_name.clone(),
        sample_rate: t.sample_rate,
        units: t.units.clone(),
        ..AdhocRow::default()
    }
}

fn rendered(c: &Converted, k: usize, m: i64) -> Flamebearer {
    render(&type_row(c, k), &tree_rows(&c.types[k].stacks), m).expect("renders")
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

fn levels(v: &[&[i64]]) -> Vec<Vec<i64>> {
    v.iter().map(|l| l.to_vec()).collect()
}

/// u6: a pprof is drawn by the push path's renderer, with its stored
/// metadata, `spyName` `""` and `groups` `null`.
#[test]
fn u6_pprof_render() {
    let p = converted(Format::Pprof, "p.pb.gz", &fx::f_p());
    let cpu = rendered(&p, 0, 8192);
    assert_eq!(cpu.names, strings(&["total", "main", "b", "a", "1f2e"]));
    assert_eq!(
        cpu.levels,
        levels(&[&[0, 12, 0, 0], &[0, 12, 0, 1], &[0, 2, 2, 4, 0, 10, 10, 3, 0, 0, 0, 2]])
    );
    assert_eq!((cpu.num_ticks, cpu.max_self), (12, 10));
    let text = answer_text(&cpu);
    assert!(
        text.ends_with(r#""metadata":{"format":"single","spyName":"","sampleRate":1000000000,"units":"samples","name":"cpu"},"timeline":null,"groups":null,"heatmap":null}"#),
        "{text}"
    );
    let samples = rendered(&p, 1, 8192);
    assert_eq!(
        samples.levels,
        levels(&[&[0, 5, 0, 0], &[0, 5, 0, 1], &[0, 0, 0, 4, 0, 1, 1, 3, 0, 4, 4, 2]])
    );
    assert_eq!(samples.max_self, 4);
    let text = answer_text(&samples);
    assert!(
        text.contains(r#""metadata":{"format":"single","spyName":"","sampleRate":100,"units":"objects","name":"samples"}"#),
        "{text}"
    );
    let e = converted(Format::Pprof, "empty", &fx::f_e());
    assert_eq!(e.types.len(), 2);
    let empty = rendered(&e, 0, 8192);
    assert_eq!(
        (empty.names.clone(), empty.levels.clone()),
        (strings(&["total"]), levels(&[&[0, 0, 0, 0]]))
    );
    assert!(answer_text(&empty).ends_with(r#""groups":null,"heatmap":null}"#));
    // §0: F-C at 2 nodes, byte for byte.
    let c = converted(Format::Collapsed, "c_1.txt", fx::F_C);
    assert_eq!(
        answer_text(&rendered(&c, 0, 2)),
        r#"{"version":1,"flamebearer":{"names":["total","main","other"],"levels":[[0,11,0,0],[0,11,0,1],[0,11,11,2]],"numTicks":11,"maxSelf":11},"metadata":{"format":"single","spyName":"unknown","sampleRate":100,"units":"","name":"c_1.txt"},"timeline":null,"groups":{},"heatmap":null}"#
    );
}

/// u7: a diff reads each side's flame graph back into a tree (AA1).
#[test]
fn u7_diff() {
    let x = converted(Format::Collapsed, "x.txt", fx::F_X);
    let side = rendered(&x, 0, 8192);
    let d = diff(&side, &side, 8192).expect("diffs");
    assert_eq!(d.names, strings(&["total", "main", "y", "other"]));
    assert_eq!(
        d.levels,
        levels(&[
            &[0, 8, 0, 0, 8, 0, 0],
            &[0, 8, 0, 0, 8, 0, 1],
            &[0, 5, 5, 0, 5, 5, 3, 0, 3, 3, 0, 3, 3, 2]
        ])
    );
    assert_eq!((d.num_ticks, d.left_ticks, d.right_ticks), (16, 8, 8));
    let text = answer_text(&d);
    assert!(
        text.contains(r#""metadata":{"format":"double","spyName":"","sampleRate":100,"units":"","name":"x.txt"},"timeline":null,"groups":null,"heatmap":null,"leftTicks":8,"rightTicks":8}"#),
        "{text}"
    );
    let p = converted(Format::Pprof, "p.pb.gz", &fx::f_p());
    let z = converted(Format::Pprof, "z", &fx::f_z());
    let d = diff(&rendered(&p, 0, 8192), &rendered(&z, 0, 8192), 8192).expect("diffs");
    assert_eq!(d.names, strings(&["total", "main", "a", "1f2e"]));
    assert_eq!(
        d.levels,
        levels(&[
            &[0, 12, 0, 0, 10, 0, 0],
            &[0, 12, 0, 0, 10, 0, 1],
            &[0, 2, 2, 0, 0, 0, 3, 0, 10, 10, 0, 10, 10, 2]
        ])
    );
    assert_eq!(
        (d.num_ticks, d.max_self, d.left_ticks, d.right_ticks),
        (22, 10, 12, 10)
    );
    assert!(
        answer_text(&d).contains(r#""metadata":{"format":"double","spyName":"","sampleRate":1000000000,"units":"samples","name":"cpu"}"#)
    );
}

const ONE: &str = "SELECT 0 AS part, toString(upload_id) AS uid, node_ids[i] AS node,
       any(if(i = 1, toUInt128(0), node_ids[i - 1])) AS parent,
       any(names[i]) AS name,
       sum(v) AS total,
       sumIf(v, i = length(node_ids)) AS self_value,
       toUInt16(0) AS type_ordinal, toUInt8(0) AS selected, '' AS upload_name, toInt64(0) AS uploaded_at,
       '' AS format, '' AS display_name, '' AS spy_name, toUInt32(0) AS sample_rate, '' AS units
FROM (
  SELECT st.upload_id AS upload_id, st.stack_id AS stack_id, any(st.node_ids) AS node_ids, any(st.names) AS names, any(agg.v) AS v
  FROM (
    SELECT upload_id, stack_id, node_ids, names
    FROM profile_adhoc
    WHERE org_id = 't1' AND upload_id IN ('01900000-0000-7000-8000-000000000001') AND row_kind = 2
  ) AS st
  INNER JOIN (
    SELECT upload_id, stack_id, sum(value) AS v
    FROM profile_adhoc
    WHERE org_id = 't1' AND upload_id IN ('01900000-0000-7000-8000-000000000001') AND row_kind = 0
      AND type_ordinal = (SELECT type_ordinal FROM profile_adhoc WHERE org_id = 't1' AND upload_id = '01900000-0000-7000-8000-000000000001' AND row_kind = 1 ORDER BY type_ordinal LIMIT 1)
    GROUP BY upload_id, stack_id
  ) AS agg ON st.upload_id = agg.upload_id AND st.stack_id = agg.stack_id
  GROUP BY st.upload_id, st.stack_id
)
ARRAY JOIN arrayEnumerate(node_ids) AS i
GROUP BY upload_id, node
UNION ALL
SELECT 1, toString(upload_id), toUInt128(0), toUInt128(0), '', toInt64(0), toInt64(0), type_ordinal,
       toUInt8(type_ordinal = (SELECT type_ordinal FROM profile_adhoc WHERE org_id = 't1' AND upload_id = '01900000-0000-7000-8000-000000000001' AND row_kind = 1 ORDER BY type_ordinal LIMIT 1)), upload_name, uploaded_at, format, display_name, spy_name, sample_rate, units
FROM profile_adhoc
WHERE org_id = 't1' AND upload_id IN ('01900000-0000-7000-8000-000000000001') AND row_kind = 1";

const SEL_L: &str = "(SELECT type_ordinal FROM profile_adhoc WHERE org_id = 't1' AND upload_id = '01900000-0000-7000-8000-000000000001' AND row_kind = 1 ORDER BY display_name = 'cpu' DESC, if(display_name = 'cpu', -toInt32(type_ordinal), toInt32(type_ordinal)) LIMIT 1)";
const SEL_R: &str = "(SELECT type_ordinal FROM profile_adhoc WHERE org_id = 't1' AND upload_id = '01900000-0000-7000-8000-000000000002' AND row_kind = 1 ORDER BY display_name = 'cpu' DESC, if(display_name = 'cpu', -toInt32(type_ordinal), toInt32(type_ordinal)) LIMIT 1)";

const LIST: &str = "SELECT toString(upload_id) AS id, any(upload_name) AS name, any(uploaded_at) AS uploaded_at
FROM profile_adhoc
WHERE org_id = 't1' AND row_kind = 1
GROUP BY upload_id
ORDER BY uploaded_at DESC, id DESC";

/// u8: S14's three forms and S15, exactly; each passes the tenant check.
#[test]
fn u8_statements() {
    let c = config();
    let one = trees_sql(&t1(), &c, &[L], None);
    assert_eq!(one, ONE);
    let named = trees_sql(&t1(), &c, &[L], Some("cpu"));
    let by_name = "ORDER BY display_name = 'cpu' DESC, if(display_name = 'cpu', -toInt32(type_ordinal), toInt32(type_ordinal)) LIMIT 1";
    assert_eq!(named, ONE.replace("ORDER BY type_ordinal LIMIT 1", by_name));
    let two = trees_sql(&t1(), &c, &[L, R], Some("cpu"));
    let sel = format!("if(upload_id = '{L}', {SEL_L}, {SEL_R})");
    let want = ONE
        .replace(
            &format!("upload_id IN ('{L}')"),
            &format!("upload_id IN ('{L}', '{R}')"),
        )
        .replace(
            &format!(
                "(SELECT type_ordinal FROM profile_adhoc WHERE org_id = 't1' AND upload_id = '{L}' AND row_kind = 1 ORDER BY type_ordinal LIMIT 1)"
            ),
            &sel,
        );
    assert_eq!(two, want);
    // The same id twice is read once.
    assert_eq!(trees_sql(&t1(), &c, &[L, L], None), ONE);
    assert_eq!(list_sql(&t1(), &c), LIST);
    let term = TenantTerm::of(&t1());
    let tables = crate::profiles::tenant_scope::tables("");
    for sql in [&one, &named, &two, &LIST.to_string()] {
        assert_eq!(
            crate::profiles::tenant_scope::check(sql, &term, &tables),
            Ok(()),
            "{sql}"
        );
    }
}

/// u9: F-P's rows, and `tree_rows` of its `cpu` stacks.
#[test]
fn u9_rows() {
    let p = converted(Format::Pprof, "p.pb.gz", &fx::f_p());
    let rows = rows(&t1(), L, "p.pb.gz", 1_795_000_000_000, &p);
    let of = |kind: u8, is_type: u8| -> Vec<&AdhocLandingRow> {
        rows.iter()
            .filter(|r| r.kind == kind && r.is_type == is_type)
            .collect()
    };
    let types = of(KIND_ROWS, 1);
    let samples = of(KIND_ROWS, 0);
    let stacks = of(KIND_STACKS, 0);
    assert_eq!((types.len(), samples.len(), stacks.len()), (2, 6, 3));
    assert_eq!(rows.len(), 11);
    assert_eq!(samples.iter().filter(|r| r.value == 0).count(), 2);
    assert!(rows.iter().all(|r| r.org_id == "t1"
        && r.uploaded_at == 1_795_000_000_000
        && r.received_ms == 1_795_000_000_000
        && r.upload_id == (0x0190_0000_0000_7000, 0x8000_0000_0000_0001)));
    assert_eq!(
        types
            .iter()
            .map(|r| (r.type_ordinal, r.display_name.as_str(), r.upload_name.as_str(), r.upload_format.as_str()))
            .collect::<Vec<_>>(),
        [(0, "cpu", "p.pb.gz", "pprof"), (1, "samples", "p.pb.gz", "pprof")]
    );
    let tree = tree_rows(&p.types[0].stacks);
    let got: Vec<(&str, i64, i64)> = tree
        .iter()
        .map(|r| (r.name.as_str(), r.total, r.self_value))
        .collect();
    assert_eq!(
        got,
        [("main", 12, 0), ("a", 10, 10), ("b", 0, 0), ("1f2e", 2, 2)]
    );
    // Node ids chain as the push path's, through `compose128`.
    let main = &tree[0];
    let mut buf = 0u128.to_le_bytes().to_vec();
    buf.extend_from_slice(b"main");
    assert_eq!(
        pulsus_model::compose128(&buf).sql_literal().to_string(),
        format!("toUInt128('{}')", main.node)
    );
    assert_eq!((main.parent, tree[1].parent), (0, main.node));
    // Every stack row's stack id is its leaf's node id.
    for s in &stacks {
        assert_eq!(s.stack_id, *s.node_ids.last().expect("frames"));
    }
}

/// The id and name rules of §5.
#[test]
fn ids_and_names() {
    assert_eq!(sanitize_name("c 1.txt"), "c_1.txt");
    assert_eq!(sanitize_name("a/中-b_c.d"), "a__-b_c.d");
    assert!(valid_id("01900000-0000-7000-8000-000000000001"));
    assert!(!valid_id("a/b"));
    assert!(!valid_id(""));
    let id = mint_id(1_795_000_000_000);
    let parsed = uuid::Uuid::parse_str(&id).expect("a UUID");
    assert_eq!(parsed.get_version_num(), 7);
    assert_eq!(id, parsed.hyphenated().to_string());
}
