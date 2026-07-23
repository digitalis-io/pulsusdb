//! Issue #173 AC6: the hermetic, byte-frozen golden suite for the
//! service-graph SQL (docs/schemas.md §4.2, docs/api.md §4.5). Each case
//! renders the single service-graph query and byte-compares it against a
//! committed file under `tests/golden/traces_graph/`. **Do not** edit the
//! committed files by hand — run the `#[ignore]` `regenerate_goldens` test
//! and review the diff (the byte-frozen-artifact rule).

use pulsus_read::{GraphWindow, SERVICE_GRAPH_MAX_EDGES, service_graph_sql};

/// Fixed window: the metrics suite's snapped 2023-11-14 .. +3h shape
/// (S = 1_699_999_980, E = 1_700_010_840 in seconds), so the goldens pin the
/// daily-partition prune crossing a UTC-day boundary and the left-closed/
/// right-open nanosecond bound.
const W: GraphWindow = GraphWindow {
    start_ns: 1_699_999_980_000_000_000,
    end_ns: 1_700_010_840_000_000_000,
};

struct Case {
    name: &'static str,
    edges_table: &'static str,
}

const CASES: &[Case] = &[
    // Single-node: the base `trace_edges` table on both join sides.
    Case {
        name: "single_node",
        edges_table: "trace_edges",
    },
    // Clustered: the `_dist` table names in the SQL text on both sides; the
    // §7 clustered-reader settings + `distributed_product_mode='local'` ride
    // as HTTP settings (pinned in `traces::exec`'s `graph_settings` unit
    // test), never SQL text — so this golden differs from `single_node` only
    // by the table name, the ratified §4.2/§4.4 semi-join precedent.
    Case {
        name: "clustered_local_join",
        edges_table: "trace_edges_dist",
    },
];

fn composite(case: &Case) -> String {
    format!(
        "-- case: {}\n-- edges_table: {}\n\n{}\n",
        case.name,
        case.edges_table,
        service_graph_sql(W, case.edges_table, SERVICE_GRAPH_MAX_EDGES),
    )
}

fn golden_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_graph")
}

#[test]
fn every_case_matches_its_committed_golden_byte_for_byte() {
    for case in CASES {
        let path = golden_dir().join(format!("{}.sql", case.name));
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "missing golden {path:?} ({e}); run `cargo test -p pulsus-read --test \
                 traces_graph_sql -- --ignored regenerate_goldens` and commit the diff"
            )
        });
        let actual = composite(case);
        assert_eq!(
            actual, expected,
            "case {:?} drifted from its committed golden {path:?} — if the change is \
             intentional, regenerate and review the diff",
            case.name
        );
    }
}

/// Targeted content assertions independent of the composite framing: the
/// two-level pushdown, the within-`conn_type` `pair_id` join, the
/// `Array(Float64)`-pinned quantiles (issue #173 Fix 2), and the `cap + 1`
/// truncation probe.
#[test]
fn single_node_pins_the_documented_fragments() {
    let sql = service_graph_sql(W, "trace_edges", SERVICE_GRAPH_MAX_EDGES);
    assert!(sql.contains("count() AS calls"));
    assert!(sql.contains("countIf(greatest(s.failed, c.failed) = 1) AS failed"));
    assert!(sql.contains(
        "CAST(quantilesTDigest(0.5, 0.95, 0.99)(s.duration_ns) AS Array(Float64)) AS quantiles_ns"
    ));
    assert!(sql.contains("WHERE side = 1 AND"));
    assert!(sql.contains("WHERE side = 0 AND"));
    assert!(sql.contains("GROUP BY trace_id, span_id"));
    assert!(sql.contains("GROUP BY trace_id, pair_id"));
    assert!(sql.contains(
        "ON c.trace_id = s.trace_id AND c.pair_id = s.pair_id AND c.conn_type = s.conn_type"
    ));
    assert!(sql.contains("date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')"));
    assert!(
        sql.contains("timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000")
    );
    assert!(sql.contains("ORDER BY calls DESC, client ASC, server ASC"));
    assert!(sql.ends_with("LIMIT 1001"));
    assert!(
        !sql.contains("Float32"),
        "no f32 anywhere on the decode path"
    );
}

/// Doc-consistency gate (the metrics suite's AC8 pattern): every shipped
/// service-graph SQL shape and committed constant is documented —
/// docs/schemas.md §4.2 (the ledger + query-time pairing) and docs/api.md
/// §4.5 (params, envelope, boundary rule, 400/422 taxonomy).
#[test]
fn shipped_graph_shapes_and_limits_are_documented() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let schemas = std::fs::read_to_string(root.join("docs/schemas.md")).expect("read schemas.md");
    let api = std::fs::read_to_string(root.join("docs/api.md")).expect("read api.md");

    for needle in [
        "trace_edges",
        "ON c.trace_id = s.trace_id AND c.pair_id = s.pair_id AND c.conn_type = s.conn_type",
        "CAST(quantilesTDigest(0.5, 0.95, 0.99)(s.duration_ns) AS Array(Float64))",
    ] {
        assert!(
            schemas.contains(needle),
            "docs/schemas.md §4.2 must document {needle:?}"
        );
    }
    for needle in [
        "/api/traces/v1/service_graph",
        "connectionType",
        "SERVICE_GRAPH_MAX_EDGES",
        "query_too_broad",
    ] {
        assert!(
            api.contains(needle),
            "docs/api.md §4.5 must document {needle:?}"
        );
    }
}

/// Regenerates every committed golden. `#[ignore]`d: run explicitly after an
/// intentional SQL-shape change, review the diff, and say so in the PR (the
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
