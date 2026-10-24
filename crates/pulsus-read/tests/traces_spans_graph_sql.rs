//! Issue #599 U1: the hermetic, byte-frozen golden suite for the service
//! graph's statement over the per-minute edge totals and the span table
//! (`spans/graph.rs`, docs/api.md §4.5). Each case renders the one statement
//! and byte-compares it against a committed file under
//! `tests/golden/traces_spans_graph/`. **Do not** edit the committed files
//! by hand — run the `#[ignore]` `regenerate_goldens` test and review the
//! diff (the byte-frozen-artifact rule).

use pulsus_read::{GraphTarget, GraphWindow, SERVICE_GRAPH_MAX_EDGES, spans_graph_sql};

/// The previous graph SQL's suite's window (it went, #602 part 3a):
/// 2023-11-14 23:33 .. +3h, crossing a UTC
/// day, so the bucket bounds and the minute rounding are pinned on both
/// sides of midnight.
const W: GraphWindow = GraphWindow {
    start_ns: 1_699_999_980_000_000_000,
    end_ns: 1_700_010_840_000_000_000,
};

struct Case {
    name: &'static str,
    /// `(cluster, database)` the routing table names; `None` on a single
    /// node.
    target: Option<(&'static str, &'static str)>,
}

const CASES: &[Case] = &[
    // Single node: the current database's bare tables.
    Case {
        name: "single_node",
        target: None,
    },
    // Clustered: one per-shard statement through `cluster(…, view(…))`,
    // both tables qualified by the target's database.
    Case {
        name: "clustered",
        target: Some(("c1", "pulsus")),
    },
];

fn target_of(case: &Case) -> Option<GraphTarget> {
    case.target.map(|(c, d)| GraphTarget {
        cluster: c.to_string(),
        database: d.to_string(),
    })
}

fn sql_of(case: &Case) -> String {
    spans_graph_sql(
        empty_term(),
        W,
        target_of(case).as_ref(),
        SERVICE_GRAPH_MAX_EDGES,
    )
}

fn composite(case: &Case) -> String {
    let target = match case.target {
        None => "none".to_string(),
        Some((c, d)) => format!("cluster {c}, database {d}"),
    };
    format!(
        "-- case: {}\n-- target: {target}\n\n{}\n",
        case.name,
        sql_of(case)
    )
}

fn golden_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_spans_graph")
}

#[test]
fn every_case_matches_its_committed_golden_byte_for_byte() {
    for case in CASES {
        let path = golden_dir().join(format!("{}.sql", case.name));
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "missing golden {path:?} ({e}); run `cargo test -p pulsus-read --test \
                 traces_spans_graph_sql -- --ignored regenerate_goldens` and commit the diff"
            )
        });
        assert_eq!(
            composite(case),
            expected,
            "case {:?} drifted from its committed golden {path:?} — if the change is \
             intentional, regenerate and review the diff",
            case.name
        );
    }
}

/// The properties the goldens carry that a regenerated golden could lose
/// without anyone reading it: the clustered statement runs per shard on the
/// target's own tables and never through a routing wrapper, and both read
/// their markers in one subquery.
#[test]
fn the_clustered_statement_runs_per_shard_on_the_target_tables() {
    let clustered = sql_of(&CASES[1]);
    // Twice: the per-shard statement and, inside it, the fan-out that reads
    // every shard's markers (issue #599 part A2), from which each shard
    // takes its own entry.
    assert_eq!(
        clustered.matches("cluster('c1', view(").count(),
        2,
        "the per-shard statement and its marker fan-out: {clustered}"
    );
    for own in ["shardNum()", "hostName()"] {
        assert!(
            clustered.contains(own),
            "each shard picks its own markers by {own}: {clustered}"
        );
    }
    assert!(clustered.contains("FROM pulsus.service_edges FINAL"));
    assert!(clustered.contains("FROM pulsus.spans\n"));
    assert!(
        !clustered.contains("_dist"),
        "no routing wrapper: {clustered}"
    );
    for case in CASES {
        let sql = sql_of(case);
        assert_eq!(
            sql.matches("WHERE org_id = '' AND conn_type = ''").count(),
            1,
            "{}: the markers are read in one subquery: {sql}",
            case.name
        );
        assert!(
            sql.ends_with(&format!("LIMIT {}", SERVICE_GRAPH_MAX_EDGES + 1)),
            "{}: the truncation probe: {sql}",
            case.name
        );
    }
    let single = sql_of(&CASES[0]);
    assert!(single.contains("FROM service_edges FINAL"));
    assert!(single.contains("FROM spans\n"));
    assert!(
        !single.contains("cluster("),
        "single node reads locally: {single}"
    );
}

/// Regenerates every committed golden. `#[ignore]`d: run explicitly after an
/// intentional statement change, review the diff, and say so in the PR (the
/// byte-frozen-artifact rule).
#[test]
#[ignore = "regenerates the committed goldens; run explicitly, see doc comment"]
fn regenerate_goldens() {
    let dir = golden_dir();
    std::fs::create_dir_all(&dir).expect("create golden dir");
    for case in CASES {
        let path = dir.join(format!("{}.sql", case.name));
        std::fs::write(&path, composite(case)).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
        eprintln!("wrote {path:?}");
    }
}

/// The empty tenant's term (issue #738): the golden is rendered for it.
fn empty_term() -> &'static pulsus_read::traces::tenant_scope::TenantTerm {
    static TERM: std::sync::LazyLock<pulsus_read::traces::tenant_scope::TenantTerm> =
        std::sync::LazyLock::new(|| {
            pulsus_read::traces::tenant_scope::TenantTerm::of(
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            )
        });
    &TERM
}
