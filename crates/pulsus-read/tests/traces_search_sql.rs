//! Issue #57 AC1: the hermetic, byte-frozen golden suite for the
//! two-phase TraceQL search SQL (docs/schemas.md §4.2). Every case
//! renders one deterministic composite — the plan's Phase-1 generator
//! queries plus the Phase-2 batch SQL (hydration / membership / value
//! reads over a fixed sample batch) and the winners' root hydration —
//! and byte-compares it against a committed file under
//! `tests/golden/traces_search/`. These goldens double as T8's hermetic
//! golden-corpus semantic gate; **do not** edit the committed files by
//! hand — run the `#[ignore]` `regenerate_goldens` test and review the
//! diff (the byte-frozen-artifact rule).

use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};
use pulsus_read::{SearchPlan, SpanFilterCtx};

/// Fixed request window: 2023-11-14T22:13:20Z .. +3h (the §4.2 "last 3h"
/// worked-example shape, pinned to absolute values for determinism).
const PARAMS: SearchParams = SearchParams {
    start_ns: 1_700_000_000_000_000_000,
    end_ns: 1_700_010_800_000_000_000,
    limit: 20,
    spss: 3,
};

const MAX_CANDIDATES: u64 = 100_000;

/// The fixed sample candidate batch the Phase-2 builders render against.
const BATCH: [[u8; 16]; 2] = [
    [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ],
    [
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ],
];

/// One returned winner for the root-hydration section.
const WINNERS: [[u8; 16]; 1] = [[
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
]];

struct Case {
    name: &'static str,
    q: &'static str,
    distributed: bool,
}

const CASES: &[Case] = &[
    Case {
        // The §4.2 worked example: the `&&` picks the statically most
        // selective generator (service equality via the `service_time`
        // projection PREWHERE); the numeric attr condition becomes a
        // Phase-2 membership read; `duration > 2s` evaluates engine-side
        // on the hydrated `duration_ns` column.
        name: "worked_example",
        q: r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }"#,
        distributed: false,
    },
    Case {
        // Mixed-table OR: BOTH generators are emitted (a match may
        // satisfy either side) — never an intersection.
        name: "mixed_or",
        q: r#"{ duration > 2s || span.foo = "x" }"#,
        distributed: false,
    },
    Case {
        // Non-service physical predicate: bounded time-window span scan.
        name: "status_only",
        q: "{ status = error }",
        distributed: false,
    },
    Case {
        // Negation: the time-range fallback generator (absence is not
        // indexable) + the positive membership probe Phase 2 inverts.
        name: "negated_attr",
        q: r#"{ .env != "prod" }"#,
        distributed: false,
    },
    Case {
        // Pipeline aggregates are exact and engine-side: no aggregate
        // SQL is emitted (the golden pins its absence).
        name: "count_pipeline",
        q: r#"{ resource.service.name = "checkout" } | count() > 2"#,
        distributed: false,
    },
    Case {
        // Cross-spanset &&: Phase-1 candidates are the superset union of
        // both operands' generators; exactness lives in Phase 2.
        name: "cross_spanset",
        q: r#"{ resource.service.name = "checkout" } && { span.foo = "x" }"#,
        distributed: false,
    },
    Case {
        // Standalone key-only numeric generator (code review round 1):
        // the val_num comparison is BOTH the Phase-1 generator (key-only
        // `(key)` prefix scan) and the Phase-2 membership probe.
        name: "val_num_range",
        q: "{ span.http.status_code >= 500 }",
        distributed: false,
    },
    Case {
        // Positive service regex generates via its indexed attr-index
        // row (key-only prefix + anchored match), not the fallback.
        name: "service_regex",
        q: r#"{ resource.service.name =~ "check.*" }"#,
        distributed: false,
    },
    Case {
        // Nested boolean completeness: (A || B) && (C || D) keeps one
        // complete OR-set of generators; all four leaves still get
        // membership probes for exact evaluation.
        name: "nested_boolean",
        q: r#"{ (.a = "1" || .b = "2") && (.c = "3" || .d = "4") }"#,
        distributed: false,
    },
    Case {
        // Two comparisons on the same key are two independent probes —
        // the round-1 `uniqExact(key)` miscount shape cannot recur.
        name: "repeated_key",
        q: r#"{ span.a = "1" && span.a = "2" }"#,
        distributed: false,
    },
    Case {
        // Unscoped attr: no scope predicate — prunes on the bare
        // (key, val) prefix and matches either scope.
        name: "unscoped_attr",
        q: r#"{ .k = "v" }"#,
        distributed: false,
    },
    Case {
        // Aggregate + select value reads (val_num / val batch reads).
        name: "agg_and_select",
        q: r#"{ resource.service.name = "checkout" } | avg(span.retries) > 1 | select(span.foo)"#,
        distributed: false,
    },
    Case {
        // The clustered form of the worked example: `_dist` tables; the
        // §7 clustered-reader + budget settings ride as HTTP settings,
        // never SQL text (pinned separately in `traces::exec` tests).
        name: "clustered_worked_example",
        q: r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }"#,
        distributed: true,
    },
];

fn plan_for(case: &Case) -> SearchPlan {
    let (spans, attrs) = if case.distributed {
        ("trace_spans_dist", "trace_attrs_idx_dist")
    } else {
        ("trace_spans", "trace_attrs_idx")
    };
    let query = pulsus_traceql::parse(case.q).expect("case query parses");
    plan_search(
        &query,
        &PARAMS,
        &SearchCtx {
            filter: SpanFilterCtx {
                spans_table: spans,
                attrs_table: attrs,
            },
            max_candidates: MAX_CANDIDATES,
            distributed: case.distributed,
        },
    )
    .expect("case query plans")
}

/// The deterministic composite rendering one golden file freezes.
fn composite(case: &Case) -> String {
    let plan = plan_for(case);
    let mut out = String::new();
    out.push_str(&format!("-- case: {}\n-- q: {}\n", case.name, case.q));
    for (i, sql) in plan.generator_sqls.iter().enumerate() {
        out.push_str(&format!("\n== phase1 generator[{i}] ==\n{sql}\n"));
    }
    out.push_str(&format!(
        "\n== phase2 hydration (sample batch) ==\n{}\n",
        plan.hydration_sql_for(&BATCH)
    ));
    for probe_idx in 0..plan.probes_len() {
        out.push_str(&format!(
            "\n== phase2 membership[{probe_idx}] ==\n{}\n",
            plan.membership_sql_for(probe_idx, &BATCH)
        ));
    }
    for field_idx in 0..plan.agg_fields_len() {
        out.push_str(&format!(
            "\n== phase2 aggregate values[{field_idx}] ==\n{}\n",
            plan.agg_values_sql_for(field_idx, &BATCH)
        ));
    }
    for field_idx in 0..plan.select_attrs_len() {
        out.push_str(&format!(
            "\n== phase2 select values[{field_idx}] ==\n{}\n",
            plan.select_values_sql_for(field_idx, &BATCH)
        ));
    }
    out.push_str(&format!(
        "\n== root hydration (sample winners) ==\n{}\n",
        plan.root_sql_for(&WINNERS)
    ));
    out
}

fn golden_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_search")
}

#[test]
fn every_case_matches_its_committed_golden_byte_for_byte() {
    for case in CASES {
        let path = golden_dir().join(format!("{}.sql", case.name));
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "missing golden {path:?} ({e}); run `cargo test -p pulsus-read --test \
                 traces_search_sql -- --ignored regenerate_goldens` and commit the diff"
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

/// AC1 targeted content assertions on the worked example (the plan's
/// pinned fragments), independent of the composite framing.
#[test]
fn worked_example_pins_the_documented_fragments() {
    let plan = plan_for(&CASES[0]);
    assert_eq!(
        plan.generator_sqls.len(),
        1,
        "the && picks exactly one generator"
    );
    let generator = &plan.generator_sqls[0];
    assert!(generator.contains("PREWHERE service = 'checkout'"));
    assert!(generator.contains("timestamp_ns > 1700000000000000000"));
    assert!(generator.contains("timestamp_ns <= 1700010800000000000"));
    assert!(generator.contains("ORDER BY bound_ts DESC, trace_id ASC"));
    assert!(
        generator.ends_with(&format!("LIMIT {}", MAX_CANDIDATES + 1)),
        "the per-generator cap+1 truncation probe"
    );
    let membership = plan.membership_sql_for(0, &BATCH);
    assert!(membership.contains("key = 'http.status_code'"));
    assert!(membership.contains("val_num >= 500"));
    assert!(membership.contains("scope = 'span'"));
    assert!(membership.contains("date >="), "date partition pruning");
    let hydration = plan.hydration_sql_for(&BATCH);
    assert!(
        hydration.contains("LIMIT 10001 BY trace_id"),
        "the per-trace overflow probe (MAX_SPANS_PER_TRACE + 1)"
    );
    assert!(
        !hydration.contains("payload"),
        "search never reads payloads"
    );
}

#[test]
fn clustered_case_targets_the_dist_tables_everywhere() {
    let plan = plan_for(
        CASES
            .iter()
            .find(|c| c.distributed)
            .expect("clustered case"),
    );
    assert!(plan.generator_sqls[0].contains("FROM trace_spans_dist\n"));
    assert!(
        plan.membership_sql_for(0, &BATCH)
            .contains("FROM trace_attrs_idx_dist\n")
    );
    assert!(
        plan.hydration_sql_for(&BATCH)
            .contains("FROM trace_spans_dist\n")
    );
    assert!(
        plan.root_sql_for(&WINNERS)
            .contains("FROM trace_spans_dist\n")
    );
    assert!(plan.distributed());
}

/// AC8 doc-consistency gate: every shipped SQL shape and limit is
/// documented — docs/schemas.md §4.2 (the two-phase design, every
/// generator class with its corrected key-only prefixes, the streaming
/// executor constants, partiality sources) and docs/api.md §4.2 (the
/// ordering + partial-results + 422 contracts). A shape change that
/// skips its doc edit fails here, not in review.
#[test]
fn shipped_shapes_and_limits_are_documented() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let schemas = std::fs::read_to_string(root.join("docs/schemas.md")).expect("read schemas.md");
    let api = std::fs::read_to_string(root.join("docs/api.md")).expect("read api.md");

    // schemas.md §4.2: the per-generator ranked shape + merge + executor.
    for needle in [
        "max(timestamp_ns) AS bound_ts",
        "ORDER BY bound_ts DESC, trace_id ASC",
        "LIMIT {PULSUS_TRACEQL_MAX_CANDIDATES + 1}",
        "**key-only** `(key)` prefix scan",
        "`service_time` projection PREWHERE",
        "`idx_duration` minmax",
        "the time-range generator",
        "`BATCH_TRACES` (32)",
        "LIMIT {MAX_SPANS_PER_TRACE + 1} BY trace_id",
        "`max(bound_ts)` per trace",
        "response summaries only",
        "PULSUS_TRACEQL_SCAN_BUDGET_ROWS",
    ] {
        assert!(
            schemas.contains(needle),
            "docs/schemas.md §4.2 must document {needle:?}"
        );
    }
    // schemas.md §7: the reader-settings/memory contract.
    for needle in [
        "max_result_bytes",
        "result_overflow_mode = 'throw'",
        "block-granular",
        "256 MiB retention counter",
        // Issue #57 re-audit: the hard block/string-byte bound and the
        // generator memory ceiling.
        "max_block_size = TRACE_SEARCH_MAX_BLOCK_ROWS",
        "TRACE_STR_COL_CAP",
        "PULSUS_TRACEQL_GENERATOR_MAX_MEMORY_BYTES",
        "MEMORY_LIMIT_EXCEEDED",
        // Issue #57 re-audit v7: the corrected two-layer framing —
        // max_result_bytes is effective on the wrapped projection (a
        // deliberate hardening), Layer 1 = per-batch, Layer 2 =
        // cross-batch retained accumulation.
        "unwrapped passthrough columns",
        "per-batch",
        "cross-batch retained accumulation",
    ] {
        assert!(
            schemas.contains(needle),
            "docs/schemas.md §7 must document {needle:?}"
        );
    }
    // api.md §4.2: the public contracts.
    for needle in [
        "\"partial\":<bool>,\"limit\":<n>,\"returned\":<n>",
        "**Ordering contract:**",
        "trace_id` ascending as the tiebreak",
        "query_too_broad",
        "mutually exclusive",
        "logfmt",
        // Issue #57 re-audit: the response string-truncation contract.
        "8192-byte",
        "2048 UTF-8 code points",
        "PULSUS_TRACEQL_GENERATOR_MAX_MEMORY_BYTES",
    ] {
        assert!(
            api.contains(needle),
            "docs/api.md §4.2 must document {needle:?}"
        );
    }
}

/// Regenerates every committed golden. `#[ignore]`d: run explicitly
/// after an intentional SQL-shape change, review the diff, and say so in
/// the PR (byte-frozen-artifact rule).
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
