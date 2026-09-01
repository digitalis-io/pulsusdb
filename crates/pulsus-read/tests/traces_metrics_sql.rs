//! Issue #59 AC2: the hermetic, byte-frozen golden suite for the TraceQL
//! metrics SQL (docs/schemas.md §4.2, docs/api.md §4.4). Every case
//! renders one deterministic composite — the plan's range SQL and
//! instant SQL — and byte-compares it against a committed file under
//! `tests/golden/traces_metrics/`. **Do not** edit the committed files
//! by hand — run the `#[ignore]` `regenerate_goldens` test and review
//! the diff (the byte-frozen-artifact rule).

use pulsus_read::traces::metrics_plan::{
    ExemplarSeriesKey, MetricsCtx, MetricsParams, plan_trace_metrics,
};
use pulsus_read::{SpanFilterCtx, TraceMetricsPlan};

/// Fixed request window: the search suite's 2023-11-14T22:13:20Z .. +3h
/// shape. 1_700_000_000 is deliberately NOT a multiple of 60 — the
/// goldens pin the outward epoch snap (S = 1_699_999_980, E =
/// 1_700_010_840 for step 60).
const PARAMS: MetricsParams = MetricsParams {
    start_ns: 1_700_000_000_000_000_000,
    end_ns: 1_700_010_800_000_000_000,
    step_ms: 60_000,
    exemplars: None,
};

struct Case {
    name: &'static str,
    q: &'static str,
    distributed: bool,
}

const CASES: &[Case] = &[
    Case {
        // The docs/schemas.md §4.2 worked example: the root-AND-spine
        // service equality hoists to PREWHERE (service_time projection);
        // the numeric attr leaf is an index-served semi-join; duration
        // renders inline on the physical column. Counting is the
        // replay-deduped uniqExact — never bare count().
        name: "rate_worked_example",
        q: r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s } | rate()"#,
        distributed: false,
    },
    Case {
        // Same filter, count_over_time: the SQL body is IDENTICAL to
        // rate (the function only changes the client-side value math at
        // the encode boundary — plan v2 delta 5).
        name: "count_over_time_worked_example",
        q: r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s } | count_over_time()"#,
        distributed: false,
    },
    Case {
        // `{}` match-all: time-only pushdown, day-pruned then bounded by
        // the Layer-1 budgets.
        name: "match_all_rate",
        q: "{} | rate()",
        distributed: false,
    },
    Case {
        // A lone scoped attr leaf: the whole WHERE is one semi-join.
        name: "attr_semi_join",
        q: "{ span.http.status_code >= 500 } | rate()",
        distributed: false,
    },
    Case {
        // Negated scoped attr: NOT IN around the positive predicate (the
        // ratified absent-key rule).
        name: "negated_attr",
        q: r#"{ span.env != "prod" } | count_over_time()"#,
        distributed: false,
    },
    Case {
        // Unscoped negation: NO scope clause in the subquery — the
        // positive set spans both scopes, so NOT IN counts spans with no
        // positive row in either (plan v2 test-gap closure).
        name: "unscoped_negated_attr",
        q: r#"{ .env != "prod" } | rate()"#,
        distributed: false,
    },
    Case {
        // Nested-OR service equalities: Or is opaque — NO PREWHERE, both
        // service leaves render inline in WHERE (plan v2 delta 4).
        name: "nested_or_service_no_hoist",
        q: r#"{ (resource.service.name = "a" || resource.service.name = "b") && duration > 1s } | rate()"#,
        distributed: false,
    },
    Case {
        // Mixed boolean tree: attr semi-join OR physical leaf, ANDed
        // with a status leaf — pins the deterministic parenthesization.
        name: "mixed_boolean",
        q: r#"{ (span.foo = "x" || duration > 2s) && status = error } | rate()"#,
        distributed: false,
    },
    Case {
        // The clustered worked example: `_dist` tables everywhere; the
        // §7 clustered-reader + set-limit + local-product settings ride
        // as HTTP settings, never SQL text (pinned in traces::exec unit
        // tests).
        name: "clustered_worked_example",
        q: r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s } | rate()"#,
        distributed: true,
    },
    Case {
        // Issue #182: `sum_over_time(duration)` — the replay-dedup inner
        // query (`any(duration_ns)` per (t, trace_id, span_id)) then the
        // outer `toFloat64(sum(val))`.
        name: "sum_over_time_duration",
        q: r#"{ span.http.status_code >= 500 } | sum_over_time(duration)"#,
        distributed: false,
    },
    Case {
        // `avg_over_time(duration)` — same dedup shape, avg aggregate.
        name: "avg_over_time_duration",
        q: "{} | avg_over_time(duration)",
        distributed: false,
    },
    Case {
        // `rate() by(resource.service.name)` — grouped count with the
        // physical `service` group column and the distinct-by-key series
        // cap probe (rendered separately, pinned below).
        name: "rate_by_service",
        q: r#"{ duration > 1s } | rate() by(resource.service.name)"#,
        distributed: false,
    },
    Case {
        // `sum_over_time(duration) by(resource.service.name)` — grouped
        // value aggregation: dedup inner, grouped outer sum.
        name: "sum_over_time_by_service",
        q: r#"{ span.env = "prod" } | sum_over_time(duration) by(resource.service.name)"#,
        distributed: false,
    },
    Case {
        // `quantile_over_time(duration, ...)` — TDigest over the deduped
        // duration, one Array(Float64) per bucket (issue #182 OQ4).
        name: "quantile_over_time_multi",
        q: "{} | quantile_over_time(duration, 0.5, 0.9, 0.99)",
        distributed: false,
    },
    Case {
        // `histogram_over_time(duration)` — issue #252: the reference's
        // `Log2Bucketize` pushed down as a `GROUP BY` on
        // `toUInt64(roundToExp2(val - 1)) * 2`, one plain `count()` row
        // per OCCUPIED `(t, bucket)`, with the sub-2ns drop as the outer
        // `WHERE val >= 2`. No ladder, nothing cumulative.
        name: "histogram_over_time_duration",
        q: r#"{ span.http.status_code >= 500 } | histogram_over_time(duration)"#,
        distributed: false,
    },
    Case {
        // AC14-example (issue #252): the docs/api.md §4.4 worked example
        // for the MATCHED histogram. Its plan is frozen here so the
        // documented query cannot drift into one that no longer plans;
        // the numbers the prose states for it are pinned by
        // `traces_log2_reference.rs` (the reference's) and
        // `traces_metrics_live.rs` (ours).
        name: "docs_histogram_worked_example",
        q: r#"{ resource.service.name = "checkout" } | histogram_over_time(duration)"#,
        distributed: false,
    },
    Case {
        // AC14-example (issue #252): the §4.4 worked example for the
        // DIVERGING percentile — same selector, same corpus,
        // `quantilesTDigest` instead of the reference's bucket walk
        // (ledger `2026-08-05-traceql-quantile-over-time-tdigest`).
        name: "docs_quantile_worked_example",
        q: r#"{ resource.service.name = "checkout" } | quantile_over_time(duration, 0.5, 0.9, 0.99, 1.0)"#,
        distributed: false,
    },
    Case {
        // `with(exemplars=N)` — the bounded per-bucket groupArraySample
        // collection SQL (issue #182 P5), rendered alongside the count
        // range query.
        name: "rate_with_exemplars",
        q: "{} | rate() with(exemplars=3)",
        distributed: false,
    },
    Case {
        // `compare({selection})` — the attribute cross-tab (intrinsic
        // arrayJoin + index-attr join), the baseline/selection totals, and
        // the distinct-(key,value) cap probe (issue #182 P6b).
        name: "compare_status",
        q: r#"{ resource.service.name = "checkout" } | compare({ span.http.status_code = "500" })"#,
        distributed: false,
    },
    Case {
        // Issue #460: the four-argument form Grafana Traces Drilldown's
        // Comparison tab generates once a time selection is dragged. The
        // `(start, end]` window is a conjunct on the `is_sel` SELECT-list
        // expression and appears NOWHERE in `PREWHERE`/`WHERE` — the
        // window repartitions the population into baseline/selection, it
        // does not filter it (`engine_metrics_compare.go:98-110`
        // @ v3.0.2). Diffing this golden against `compare_status.sql`
        // shows the whole change: one conjunct, in one place, and the
        // totals/probe SQL untouched.
        name: "compare_status_window",
        q: r#"{ resource.service.name = "checkout" } | compare({ span.http.status_code = "500" }, 3, 1700000005000000000, 1700000008000000000)"#,
        distributed: false,
    },
    Case {
        // Issue #458: the root-span filter the datasource's root-span rate
        // panel generates. `nestedSetParent < 0` is TRUE for the root
        // sentinel `-1` and constant-FALSE over the whole non-root domain
        // (`[1, ∞)`), so it lowers to the reference's own `IsRoot`
        // identity — one `FixedString(8)` column comparison, no join, no
        // subquery (`nested_set_model.go:11-12,57` @ v3.0.2).
        name: "nested_set_root_rate",
        q: "{ nestedSetParent < 0 } | rate()",
        distributed: false,
    },
    Case {
        // The non-root half of the family: `>= 1` is FALSE for the root
        // sentinel `-1` and constant-TRUE over the whole non-root domain,
        // so it lowers to the negation of the root identity.
        //
        // The spelling is `>= 1` rather than `!= -1` because a NEGATIVE
        // literal is not a literal in this grammar: `-1` parses to
        // `Unary { Neg, Literal(Number("1")) }`, so `{ nestedSetParent !=
        // -1 }` never reaches the nested-set leaf at all — it is refused
        // one arm earlier by the operand-shape check, which is the
        // wave-2 `field-vs-field and arithmetic comparisons` class
        // (issue #458, still open). `>= 1` pins the same rendered
        // outcome through the path wave 1 owns.
        name: "nested_set_nonroot_rate",
        q: "{ nestedSetParent >= 1 } | rate()",
        distributed: false,
    },
    Case {
        // `= 0` is false everywhere in the domain — a constant-`0` filter,
        // the same fold `{ false }` takes.
        name: "nested_set_constant_false",
        q: "{ nestedSetParent = 0 } | rate()",
        distributed: false,
    },
    Case {
        // `!= 0` is true everywhere — the constant-`1` match-all filter.
        name: "nested_set_constant_true",
        q: "{ nestedSetParent != 0 } | rate()",
        distributed: false,
    },
    Case {
        // Issue #458 AC 5/AC 6: the hoist keeps the service equality in
        // PREWHERE (selecting `service_time`) and the root test lands in
        // the residual WHERE. **Placement is a text property and this
        // golden is the only HERMETIC oracle for it** — `EXPLAIN SYNTAX`
        // and `system.query_log.query` also preserve the spelling, but
        // both need a live server.
        name: "service_and_nested_set_root",
        q: r#"{ resource.service.name = "checkout" && nestedSetParent < 0 } | rate()"#,
        distributed: false,
    },
    Case {
        // Issue #458: bare attribute truthiness. `{ .flag }` IS
        // `.flag = true`, so it renders the ordinary index-served
        // attribute semi-join against the stored `'true'` text — the
        // literal `val = 'true'` is the byte an inverted lowering moves.
        name: "bare_attr_truthiness",
        q: "{ .flag } | rate()",
        distributed: false,
    },
];

fn plan_for(case: &Case) -> TraceMetricsPlan {
    let (spans, attrs) = if case.distributed {
        ("trace_spans_dist", "trace_attrs_idx_dist")
    } else {
        ("trace_spans", "trace_attrs_idx")
    };
    let query = pulsus_traceql::parse(case.q).expect("case query parses");
    plan_trace_metrics(
        &query,
        &PARAMS,
        &MetricsCtx {
            filter: SpanFilterCtx {
                spans_table: spans,
                attrs_table: attrs,
            },
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: case.distributed,
            skip_unavailable_shards: false,
        },
    )
    .expect("case query plans")
}

/// The deterministic composite rendering one golden file freezes: both
/// SQL forms of the plan (range → matrix, instant → vector).
fn composite(case: &Case) -> String {
    let plan = plan_for(case);
    // compare() has no range_sql/instant_sql — it serves from its
    // cross-tab/totals SQL, frozen here.
    if let Some((cross, totals)) = plan.compare_range() {
        let mut out = format!(
            "-- case: {}\n-- q: {}\n\n== compare cross-tab (query_range) ==\n{cross}\n\n\
             == compare totals (query_range) ==\n{totals}\n",
            case.name, case.q,
        );
        // The two cap probes, instant first so the frozen one keeps its
        // committed position and its committed bytes (issue #477).
        if let Some(probe) = plan.instant_probe_sql() {
            out.push_str(&format!("\n== compare series probe ==\n{probe}\n"));
        }
        if let Some(probe) = plan.range_probe_sql() {
            out.push_str(&format!("\n== compare range series probe ==\n{probe}\n"));
        }
        push_exemplars(&mut out, &plan);
        return out;
    }
    let mut out = format!(
        "-- case: {}\n-- q: {}\n\n== range (query_range) ==\n{}\n\n== instant (query) ==\n{}\n",
        case.name,
        case.q,
        plan.range_sql(),
        plan.instant_sql()
    );
    // Grouped queries also freeze both distinct-by-key series cap probes.
    if let Some(probe) = plan.instant_probe_sql() {
        out.push_str(&format!("\n== series probe ==\n{probe}\n"));
    }
    if let Some(probe) = plan.range_probe_sql() {
        out.push_str(&format!("\n== range series probe ==\n{probe}\n"));
    }
    push_exemplars(&mut out, &plan);
    out
}

/// Appends the exemplar-collection SQL section.
///
/// Called from BOTH branches, and deliberately so: issue #477 turns
/// exemplars on by default, and the compare branch used to return before
/// this append, so the two comparison goldens would have been the only
/// two of the 26 without an `exemplars` section. A carve-out in a frozen
/// corpus is a place for a rule to stop being uniform.
fn push_exemplars(out: &mut String, plan: &TraceMetricsPlan) {
    if let Some(ex) = plan.exemplar_sql() {
        out.push_str(&format!("\n== exemplars ==\n{ex}\n"));
    }
}

fn golden_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_metrics")
}

#[test]
fn every_case_matches_its_committed_golden_byte_for_byte() {
    for case in CASES {
        let path = golden_dir().join(format!("{}.sql", case.name));
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "missing golden {path:?} ({e}); run `cargo test -p pulsus-read --test \
                 traces_metrics_sql -- --ignored regenerate_goldens` and commit the diff"
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

/// Targeted content assertions on the worked example (the plan's pinned
/// fragments), independent of the composite framing.
#[test]
fn worked_example_pins_the_documented_fragments() {
    let plan = plan_for(&CASES[0]);
    let range = plan.range_sql();
    assert!(range.starts_with(
        "SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns \
         - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t,\n       \
         uniqExact(trace_id, span_id) AS n\n"
    ));
    assert!(range.contains("PREWHERE service = 'checkout'"));
    // The RANGE window's bounds (issue #477): `(aS - step, aE]` spelled
    // as the integer-nanosecond half-open `[aS - step + 1, aE + 1)`.
    // NOT the raw request window, and NOT search's `> start AND <= end`.
    assert!(range.contains(
        "WHERE timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001"
    ));
    // The instant window is unchanged.
    assert!(plan.instant_sql().contains(
        "WHERE timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000"
    ));
    assert!(range.contains(
        "(trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx WHERE \
         date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')"
    ));
    assert!(range.contains("key = 'http.status_code' AND val_num >= 500 AND scope = 'span'"));
    assert!(range.contains("duration_ns > 2000000000"));
    assert!(range.ends_with("GROUP BY t\nORDER BY t ASC"));
    assert!(
        !range.contains("count()"),
        "counting is always the replay-deduped uniqExact"
    );
    assert!(
        !range.contains("/ 60"),
        "the rate division is client-side at the encode boundary, never SQL"
    );
    assert!(
        !range.contains("toUnixTimestamp("),
        "the bucket column is Int64 epoch-milliseconds (toUnixTimestamp64Milli), never the \
         UInt32-overflowing toUnixTimestamp — issue #59 re-audit"
    );
    // The instant form is the same body without bucketing.
    let instant = plan.instant_sql();
    assert!(instant.starts_with("SELECT uniqExact(trace_id, span_id) AS n\n"));
    assert!(!instant.contains("GROUP BY"));
    assert_eq!(plan.snapped_end_ms(), 1_700_010_840_000);
}

/// Issue #458 AC 5: the nested-set root lowering's exact bytes, and the
/// PREWHERE **placement** the golden composite freezes.
///
/// Placement is a text property. `EXPLAIN SYNTAX` and
/// `system.query_log.query` also preserve it, but both need a live
/// server; this is the only **required hermetic** oracle for which
/// conjunct sits inside the `PREWHERE`. The execution counters cannot
/// see it: moving the root test into the `PREWHERE` alongside the
/// service equality leaves the projection, Parts, Granules,
/// `query_log.projections`, `read_rows` and
/// `RowsReadByPrewhereReaders` all identical (measured, issue #458 plan
/// v4 Delta C part 2).
#[test]
fn the_nested_set_root_lowering_pins_its_sql_and_its_prewhere_placement() {
    let case = |name: &str| CASES.iter().find(|c| c.name == name).expect("case exists");
    const ROOT_SQL: &str = "parent_id = toFixedString(unhex('0000000000000000'), 8)";

    // The reference's own `IsRoot` identity: one FixedString(8) column
    // comparison. No join, no subquery, nothing for a cost gate to catch
    // later (`nested_set_model.go:11-12,57` @ v3.0.2).
    let root = plan_for(case("nested_set_root_rate"))
        .range_sql()
        .to_string();
    assert!(root.contains(&format!("AND {ROOT_SQL}")), "{root}");
    assert!(!root.contains("IN (SELECT"), "no semi-join: {root}");
    assert!(!root.contains("JOIN"), "no join: {root}");

    let nonroot = plan_for(case("nested_set_nonroot_rate"))
        .range_sql()
        .to_string();
    assert!(
        nonroot.contains(&format!("AND NOT ({ROOT_SQL})")),
        "{nonroot}"
    );

    // The two constant folds render the same `1`/`0` the `{ }` match-all
    // and `{ false }` render.
    assert!(
        plan_for(case("nested_set_constant_true"))
            .range_sql()
            .contains("\n  AND 1\n"),
        "{}",
        plan_for(case("nested_set_constant_true")).range_sql()
    );
    assert!(
        plan_for(case("nested_set_constant_false"))
            .range_sql()
            .contains("\n  AND 0\n"),
        "{}",
        plan_for(case("nested_set_constant_false")).range_sql()
    );

    // Placement: the service equality is the ONLY thing in the PREWHERE,
    // and the root test is a residual WHERE conjunct on an unindexed
    // column that must not displace it.
    let hoisted = plan_for(case("service_and_nested_set_root"))
        .range_sql()
        .to_string();
    assert!(
        hoisted.contains("PREWHERE service = 'checkout'\nWHERE timestamp_ns"),
        "the PREWHERE carries the service equality and nothing else: {hoisted}"
    );
    assert!(
        hoisted.contains(&format!("AND {ROOT_SQL}\nGROUP BY t")),
        "the root test is the residual WHERE conjunct: {hoisted}"
    );

    // Bare truthiness is plain equality against the stored boolean text.
    let flag = plan_for(case("bare_attr_truthiness"))
        .range_sql()
        .to_string();
    assert!(flag.contains("key = 'flag' AND val = 'true'"), "{flag}");
}

#[test]
fn rate_and_count_over_time_share_one_sql_body() {
    // Plan v2 delta 5: the function changes only the encode-boundary
    // value math — byte-identical SQL keeps the AC4 identities exact.
    assert_eq!(
        plan_for(&CASES[0]).range_sql(),
        plan_for(&CASES[1]).range_sql()
    );
    assert_eq!(
        plan_for(&CASES[0]).instant_sql(),
        plan_for(&CASES[1]).instant_sql()
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
    assert!(plan.range_sql().contains("FROM trace_spans_dist\n"));
    assert!(plan.range_sql().contains("FROM trace_attrs_idx_dist WHERE"));
    assert!(plan.instant_sql().contains("FROM trace_spans_dist\n"));
    assert!(plan.distributed());
}

/// Doc-consistency gate (the search suite's AC8 pattern): every shipped
/// metrics SQL shape and committed constant is documented —
/// docs/schemas.md §4.2 (the pushdown shape, dedup counting, snapping)
/// and docs/api.md §4.4 (function set, step derivation, point cap, 422).
#[test]
fn shipped_metrics_shapes_and_limits_are_documented() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let schemas = std::fs::read_to_string(root.join("docs/schemas.md")).expect("read schemas.md");
    let api = std::fs::read_to_string(root.join("docs/api.md")).expect("read api.md");

    for needle in [
        "toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t",
        // Issue #477 (b): the right-closed reading, and the `- 1` that
        // makes it one, both named where a reader looking at the SQL will
        // find them.
        "label `L` covers the instants `(L − step, L]`",
        "uniqExact(trace_id, span_id) AS n",
        "PREWHERE service = 'checkout'",
        "GROUP BY t",
        "max_rows_in_set",
        "set_overflow_mode = 'throw'",
        "distributed_product_mode = 'local'",
        // Issue #477 (b): §4.2's own statement of the range window and the
        // right-closed label, beside the SQL block that renders them.
        "the instants `(L − step, L]`",
        "`[S − step + 1, E + 1)`",
    ] {
        assert!(
            schemas.contains(needle),
            "docs/schemas.md §4.2 must document {needle:?}"
        );
    }
    for needle in [
        "rate()",
        "count_over_time()",
        "DEFAULT_METRICS_POINTS",
        "MAX_METRICS_POINTS",
        "11000",
        // The §4.4 point-cap taxonomy. Issue #384 removed the `errorType`
        // vocabulary from §4 with the JSON envelope, so the old
        // `query_too_broad` needle would now pass only if the doc still
        // named a field the wire no longer carries. The sentence is the pin.
        "is rejected **statically before execution** with `422`",
        "left-closed",
        // Issue #477 (a): the emitted grid, its extra leading bucket, and
        // the per-function density rule an alert author needs.
        "`intervals + 1` samples",
        "extra **leading** bucket",
        "`(E − S) / step + 1`",
        "The `*_over_time(duration)` value aggregations stay **sparse**",
        // Issue #477 (d): the step grammar and the whole-millisecond bound.
        "any positive **whole number of milliseconds**",
        "traceql-metrics-fractional-ms-step-rejected",
        // Issue #477 (c): AC12 proves a reader can FIND the change — that
        // the sentence names BOTH inputs, says the budget is a total, and
        // states the precedence. The semantics are AC5's.
        "**Both the `with(exemplars=…)` hint and the `exemplars` request parameter are a TOTAL budget for the whole response**",
        "the **hint wins** when present, otherwise the **parameter**, otherwise a default of 100",
        "traceql-metrics-exemplars-total-budget",
        // Issue #464 wave 2 review: §4.4 carried an empty-window sentence
        // that contradicted the instant-body paragraph above it and said
        // the opposite of what the route does, and nothing failed. The
        // measured contract — ungrouped forms return one series whose zero
        // `value` is omitted, grouped forms and `histogram_over_time`
        // return an empty list — is a claim an alert author acts on, so it
        // gets a needle rather than staying unchecked prose.
        "an absent `value` is a numeric zero, never no-data",
        // The summary sentence above is not the claim. Planted: inverting
        // the enumeration to "the ungrouped forms return no series at all"
        // — the exact wrong text the review found — left the summary
        // needle green. These two pin the halves an alert author reads.
        "return exactly one labelled series whose zero `value` is omitted",
        "`histogram_over_time(duration)` return an empty `series` list",
    ] {
        assert!(
            api.contains(needle),
            "docs/api.md §4.4 must document {needle:?}"
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

// ---------------------------------------------------------------------------
// Issue #477 AC7: the goldens move, and they move in exactly one declared way.
//
// Every check below compares against `tests/golden/traces_metrics_base/`, a
// byte-for-byte copy of the 26 committed goldens at `2f78c53`. That copy is a
// SIBLING of the frozen corpus root, so `golden_sql_freeze.rs`'s two-name
// `CORPORA` walk cannot see it and the freeze count stays at 77.
// ---------------------------------------------------------------------------

/// Every golden this corpus holds, by file stem. The set equality below is
/// what makes this a domain and not a sample.
const GOLDEN_SQL: [&str; 26] = [
    "attr_semi_join",
    "avg_over_time_duration",
    "bare_attr_truthiness",
    "clustered_worked_example",
    "compare_status",
    "compare_status_window",
    "count_over_time_worked_example",
    "docs_histogram_worked_example",
    "docs_quantile_worked_example",
    "histogram_over_time_duration",
    "match_all_rate",
    "mixed_boolean",
    "negated_attr",
    "nested_or_service_no_hoist",
    "nested_set_constant_false",
    "nested_set_constant_true",
    "nested_set_nonroot_rate",
    "nested_set_root_rate",
    "quantile_over_time_multi",
    "rate_by_service",
    "rate_with_exemplars",
    "rate_worked_example",
    "service_and_nested_set_root",
    "sum_over_time_by_service",
    "sum_over_time_duration",
    "unscoped_negated_attr",
];

/// The committed pre-#477 copy of the 26 goldens.
fn golden_base_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_metrics_base")
}

/// The `== name ==` sections of one composite, in file order, each body
/// stripped of the blank line that separates it from the next header.
fn golden_sections(text: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut current: Option<(String, Vec<&str>)> = None;
    for line in text.lines() {
        if let Some(name) = line
            .strip_prefix("== ")
            .and_then(|rest| rest.strip_suffix(" =="))
        {
            if let Some((n, body)) = current.take() {
                out.push((n, body.join("\n").trim_matches('\n').to_string()));
            }
            current = Some((name.to_string(), Vec::new()));
        } else if let Some((_, body)) = current.as_mut() {
            body.push(line);
        }
    }
    if let Some((n, body)) = current {
        out.push((n, body.join("\n").trim_matches('\n').to_string()));
    }
    out
}

fn section_names(text: &str) -> Vec<String> {
    golden_sections(text).into_iter().map(|(n, _)| n).collect()
}

fn section_body(text: &str, name: &str) -> Option<String> {
    golden_sections(text)
        .into_iter()
        .find(|(n, _)| n == name)
        .map(|(_, b)| b)
}

/// The declared inverse of issue #477's axis change: three substitutions,
/// and nothing else. Applying it to a moved section must restore that
/// section's committed pre-#477 bytes exactly.
///
/// The three are the three things that moved: the bucket instant shifted
/// back one nanosecond before flooring (and the interval unit with it, to
/// nanoseconds, because a millisecond interval rounds that shift away —
/// see `metrics_sql::range_bucket_expr`), the whole step added back to
/// reach the right edge, and the range window widened by one step on the
/// left and one nanosecond on the right. All 26 cases plan at
/// `step_ms = 60_000` over the suite's fixed window, so the literals are
/// constants here rather than a re-derivation.
fn undo_axis(section: &str) -> String {
    section
        .replace(
            "fromUnixTimestamp64Nano(timestamp_ns - 1)",
            "fromUnixTimestamp64Nano(timestamp_ns)",
        )
        .replace(
            "INTERVAL 60000000000 NANOSECOND)) + 60000",
            "INTERVAL 60000 MILLISECOND))",
        )
        .replace(
            "timestamp_ns >= 1699999920000000001 AND timestamp_ns < 1700010840000000001",
            "timestamp_ns >= 1699999980000000000 AND timestamp_ns < 1700010840000000000",
        )
}

fn read_golden(dir: &std::path::Path, stem: &str) -> String {
    let path = dir.join(format!("{stem}.sql"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

/// The `*.sql` stems actually present in a directory.
fn sql_stems(dir: &std::path::Path) -> std::collections::BTreeSet<String> {
    std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}"))
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .map(|p| p.file_stem().expect("stem").to_string_lossy().into_owned())
        .collect()
}

/// AC7(a): the domain is exactly these 26 names, in both directories.
#[test]
fn the_golden_domain_is_exactly_the_committed_twenty_six() {
    let declared: std::collections::BTreeSet<String> =
        GOLDEN_SQL.iter().map(|s| s.to_string()).collect();
    assert_eq!(declared.len(), 26, "GOLDEN_SQL has a duplicate");
    assert_eq!(
        sql_stems(&golden_dir()),
        declared,
        "the committed corpus is not the declared domain"
    );
    assert_eq!(
        sql_stems(&golden_base_dir()),
        declared,
        "the base copy is not the declared domain"
    );
    // The corpus root holds the 26 plus the one committed capture, and
    // nothing else — the floor that keeps the set equality above from
    // passing over an emptied directory.
    let entries: Vec<String> = std::fs::read_dir(golden_dir())
        .expect("read_dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(entries.len(), 27, "26 .sql + log2_reference_capture.json");
}

/// AC7(b): each file's section-name set is its BASE set plus the declared
/// additions — derived per file from the base file, never from a table, so
/// the four structural shapes are covered by one rule.
#[test]
fn every_golden_section_name_set_is_its_base_set_plus_the_declared_additions() {
    for stem in GOLDEN_SQL {
        let base = read_golden(&golden_base_dir(), stem);
        let new = read_golden(&golden_dir(), stem);
        let base_names = section_names(&base);
        let new_names = section_names(&new);

        let mut expected = base_names.clone();
        if base_names.iter().any(|n| n == "series probe") {
            let at = new_names
                .iter()
                .position(|n| n == "series probe")
                .expect("the instant probe section survives");
            let _ = at;
            expected.push("range series probe".to_string());
        }
        if base_names.iter().any(|n| n == "compare series probe") {
            expected.push("compare range series probe".to_string());
        }
        if !base_names.iter().any(|n| n == "exemplars") {
            expected.push("exemplars".to_string());
        }
        let expected_set: std::collections::BTreeSet<&String> = expected.iter().collect();
        let new_set: std::collections::BTreeSet<&String> = new_names.iter().collect();
        assert_eq!(expected_set, new_set, "{stem}: section-name set");

        // The base sections keep their base relative order.
        let kept: Vec<&String> = new_names
            .iter()
            .filter(|n| base_names.contains(n))
            .collect();
        let base_order: Vec<&String> = base_names.iter().collect();
        assert_eq!(kept, base_order, "{stem}: base sections reordered");

        // Each range probe sits immediately after its instant counterpart,
        // and `exemplars` is last.
        for (instant, range) in [
            ("series probe", "range series probe"),
            ("compare series probe", "compare range series probe"),
        ] {
            if let Some(i) = new_names.iter().position(|n| n == instant) {
                assert_eq!(
                    new_names.get(i + 1).map(String::as_str),
                    Some(range),
                    "{stem}: {range} must follow {instant}"
                );
            }
        }
        assert_eq!(
            new_names.last().map(String::as_str),
            Some("exemplars"),
            "{stem}: the exemplar section is last"
        );
    }
}

/// AC7(c): the inverse, applied per section. Every section that moved must
/// come back to its committed bytes under exactly the three declared
/// substitutions; every section that must NOT move is checked separately by
/// AC7(d), so a failure here names which half moved.
#[test]
fn the_declared_inverse_restores_every_moved_section_to_its_base_bytes() {
    const MOVED: [&str; 3] = [
        "range (query_range)",
        "compare cross-tab (query_range)",
        "compare totals (query_range)",
    ];
    let mut checked = 0usize;
    for stem in GOLDEN_SQL {
        let base = read_golden(&golden_base_dir(), stem);
        let new = read_golden(&golden_dir(), stem);
        for name in MOVED {
            let Some(b) = section_body(&base, name) else {
                continue;
            };
            let n =
                section_body(&new, name).unwrap_or_else(|| panic!("{stem}: {name} disappeared"));
            assert_ne!(n, b, "{stem}: {name} did not move at all");
            assert_eq!(undo_axis(&n), b, "{stem}: {name} does not invert");
            checked += 1;
        }
        // `rate_with_exemplars` is the one file that ALREADY had an
        // exemplars section. Its per-bucket `K` moves too, because the
        // budget became a total spread over the grid — parsed from the
        // case's own `with(exemplars=K)`, a rule rather than a hardcode.
        if let Some(b) = section_body(&base, "exemplars") {
            let case = CASES
                .iter()
                .find(|c| c.name == stem)
                .expect("every golden stem is a case");
            let k: u32 = case
                .q
                .split("with(exemplars=")
                .nth(1)
                .and_then(|rest| rest.split(')').next())
                .expect("the case names its hint")
                .parse()
                .expect("a numeric hint");
            let n = section_body(&new, "exemplars").expect("exemplars survive");
            assert_eq!(
                undo_axis(&n).replace(
                    "groupArraySample(1, 1)",
                    &format!("groupArraySample({k}, 1)")
                ),
                b,
                "{stem}: the pre-existing exemplar section does not invert"
            );
            checked += 1;
        }
    }
    // 24 range + 2 compare cross-tab + 2 compare totals + 1 exemplars.
    assert_eq!(
        checked, 29,
        "the inverse must be applied to every moved section"
    );
}

/// AC7(d): every instant-side section is byte-identical to `2f78c53`.
/// Asserted separately from the inverse so a failure names which half
/// moved. The instant route belongs to #503 and this change moves none of
/// its bytes.
#[test]
fn every_instant_side_section_is_byte_identical_to_base() {
    const FROZEN: [&str; 3] = ["instant (query)", "series probe", "compare series probe"];
    let mut checked = 0usize;
    for stem in GOLDEN_SQL {
        let base = read_golden(&golden_base_dir(), stem);
        let new = read_golden(&golden_dir(), stem);
        for name in FROZEN {
            let Some(b) = section_body(&base, name) else {
                continue;
            };
            let n =
                section_body(&new, name).unwrap_or_else(|| panic!("{stem}: {name} disappeared"));
            assert_eq!(n, b, "{stem}: {name} moved and must not have");
            checked += 1;
        }
    }
    // 24 instant + 2 grouped probes + 2 compare probes.
    assert_eq!(
        checked, 28,
        "the frozen half must cover every instant section"
    );
}

/// AC7(c), added-section half: an added section has no base counterpart,
/// so the check is relational WITHIN the new file.
#[test]
fn every_added_section_matches_its_own_files_range_predicate() {
    for stem in GOLDEN_SQL {
        let new = read_golden(&golden_dir(), stem);
        let range = section_body(&new, "range (query_range)")
            .or_else(|| section_body(&new, "compare cross-tab (query_range)"))
            .unwrap_or_else(|| panic!("{stem}: no range-side section"));
        let range_pred = predicate_text(&range);

        let ex = section_body(&new, "exemplars").expect("every file has an exemplar section");
        assert_eq!(
            predicate_text(&ex),
            range_pred,
            "{stem}: the exemplar SQL must carry the range query's own predicate"
        );
        let case = CASES
            .iter()
            .find(|c| c.name == stem)
            .expect("every golden stem is a case");
        assert!(
            ex.contains(&format!(
                "groupArraySample(1, 1)({}) AS ex",
                exemplar_sample_tuple(plan_for(case).exemplar_key())
            )),
            "{stem}: {ex}"
        );

        for (instant, range_probe) in [
            ("series probe", "range series probe"),
            ("compare series probe", "compare range series probe"),
        ] {
            let Some(added) = section_body(&new, range_probe) else {
                continue;
            };
            assert_eq!(
                predicate_text(&added),
                range_pred,
                "{stem}: {range_probe} must be over the RANGE window"
            );
            let frozen = section_body(&new, instant).expect("the instant probe is there");
            assert_ne!(
                predicate_text(&frozen),
                range_pred,
                "{stem}: the two probes must be over DIFFERENT windows — that is the whole \
                 point of splitting them"
            );
        }
    }
}

/// Issue #477 wave 2: the comparison exemplar statement's key universe is
/// the cross-tab's own. The exemplar statement needs only the KEYS (which
/// `<side>_total` series a span contributes to), so it renders them as a
/// literal list rather than re-deriving the value expressions; this pins
/// that list against the cross-tab text it is a projection of, in both
/// directions, so neither branch can gain or lose an intrinsic alone.
#[test]
fn the_compare_exemplar_keys_are_the_cross_tabs_own_intrinsic_keys() {
    use pulsus_read::traces::metrics_sql::COMPARE_INTRINSIC_KEYS;
    let case = CASES
        .iter()
        .find(|c| c.name == "compare_status")
        .expect("the comparison case is in the corpus");
    let plan = plan_for(case);
    let (cross_tab, _) = plan.compare_range().expect("a comparison plan");
    // The cross-tab's intrinsics branch is one `arrayJoin([...])` of
    // `('<key>', <value expr>)` pairs; take the key literals out of it.
    let branch = cross_tab
        .split("arrayJoin([")
        .nth(1)
        .and_then(|rest| rest.split("]) AS kv").next())
        .expect("the intrinsics branch is an arrayJoin of (key, value) pairs");
    let from_cross_tab: Vec<&str> = branch
        .split("('")
        .skip(1)
        .filter_map(|p| p.split("',").next())
        .collect();
    assert_eq!(
        from_cross_tab,
        COMPARE_INTRINSIC_KEYS.to_vec(),
        "the exemplar statement's key list and the cross-tab's intrinsics branch have drifted"
    );
    // …and every one of them reaches the exemplar statement verbatim.
    let ex = plan.exemplar_sql().expect("exemplars are on by default");
    for key in COMPARE_INTRINSIC_KEYS {
        assert!(
            ex.contains(&format!("'{key}'")),
            "the comparison exemplar statement drops the {key} intrinsic:\n{ex}"
        );
    }
}

/// The sampled tuple each exemplar shape collects. `Single`/`Group` read
/// the span rows directly, so they name the physical `timestamp_ns`; the
/// two duration shapes read the deduped subquery's `ts`, and the quantile
/// shape carries the duration that decides its `p=` series as well.
fn exemplar_sample_tuple(key: &ExemplarSeriesKey) -> &'static str {
    match key {
        ExemplarSeriesKey::Single | ExemplarSeriesKey::Group { .. } => {
            "tuple(trace_id, timestamp_ns)"
        }
        ExemplarSeriesKey::Quantile => "tuple(trace_id, ts, val)",
        ExemplarSeriesKey::HistogramBucket | ExemplarSeriesKey::CompareSide => {
            "tuple(trace_id, ts)"
        }
    }
}

/// Issue #477 wave 2, ruling finding 1 — the HERMETIC half of "an
/// exemplar never lands on a series that did not produce it": every
/// exemplar statement must RETURN the column(s) that identify a series in
/// the shape it was built for, and must group by them.
///
/// This is the criterion that would have caught the wave-2 defect at
/// plan level: quantile, histogram and compare each rendered a statement
/// whose only key was the time bucket, while framing many series per
/// bucket. It cannot pass on a statement that has nothing to join on.
#[test]
fn every_exemplar_statement_returns_its_shapes_series_identity() {
    let mut seen: std::collections::BTreeSet<&'static str> = std::collections::BTreeSet::new();
    for case in CASES {
        let plan = plan_for(case);
        let ex = plan
            .exemplar_sql()
            .unwrap_or_else(|| panic!("{}: exemplars are on by default", case.name));
        // Required fragments per shape: what the row returns, and the
        // grouping that makes the row unique on that identity.
        let (name, required): (&'static str, Vec<String>) = match plan.exemplar_key() {
            ExemplarSeriesKey::Single => ("Single", vec!["GROUP BY t\n".to_string()]),
            ExemplarSeriesKey::Group { .. } => (
                "Group",
                vec![" AS g0,".to_string(), "GROUP BY t, g0\n".to_string()],
            ),
            ExemplarSeriesKey::Quantile => (
                "Quantile",
                vec![
                    "tuple(trace_id, ts, val)".to_string(),
                    "GROUP BY t\n".to_string(),
                    // The wave-3 ruling's domain, pinned where it is
                    // decided: the `p` values a sample is placed against
                    // are the whole window's, merged across the range
                    // partition by a WINDOW function — one statement and
                    // one scan, not a second aggregation over the spans.
                    " OVER () AS Array(Float64)) AS qs".to_string(),
                ],
            ),
            ExemplarSeriesKey::HistogramBucket => (
                "HistogramBucket",
                vec![
                    " AS bucket,".to_string(),
                    "GROUP BY t, bucket\n".to_string(),
                ],
            ),
            ExemplarSeriesKey::CompareSide => (
                "CompareSide",
                vec![
                    "SELECT t, is_sel, akey, ".to_string(),
                    "GROUP BY t, is_sel, akey\n".to_string(),
                ],
            ),
        };
        seen.insert(name);
        for frag in required {
            assert!(
                ex.contains(&frag),
                "{}: the {name} exemplar statement must carry {frag:?} — without it the \
                 engine has nothing to join a sample to and falls back to a first-series \
                 attachment:\n{ex}",
                case.name
            );
        }
    }
    // Anti-vacuity: the corpus must actually reach every shape, or this
    // test proves nothing about the ones it never planned.
    assert_eq!(
        seen,
        [
            "CompareSide",
            "Group",
            "HistogramBucket",
            "Quantile",
            "Single"
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>(),
        "the golden corpus must exercise every exemplar shape"
    );
}

/// The DISTINCT `PREWHERE` fragments and window bounds a section reads
/// over — the text that says which rows a statement touches.
///
/// A set rather than a list, and deliberately: the comparison cross-tab
/// interpolates its `base` subquery four times (once directly, once
/// through the roots CTE, once through the intrinsics branch and once
/// through the index-attribute join), so every one of its predicates
/// occurs several times over. What has to match between two sections is
/// WHICH rows they read, not how many times the same predicate is
/// spelled.
fn predicate_text(section: &str) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    for line in section.lines().map(str::trim) {
        if let Some(rest) = line.strip_prefix("PREWHERE ") {
            out.insert(format!("PREWHERE {}", rest.trim()));
        }
        // Every window bound, wherever it sits — top-level `WHERE`, the
        // attribute semi-join, the compare selection predicate.
        const LO: &str = "timestamp_ns >= ";
        const HI: &str = " AND timestamp_ns < ";
        let mut rest = line;
        while let Some(at) = rest.find(LO) {
            let after_lo = &rest[at + LO.len()..];
            let lo: String = after_lo.chars().take_while(char::is_ascii_digit).collect();
            let after_num = &after_lo[lo.len()..];
            if !lo.is_empty() && after_num.starts_with(HI) {
                let hi: String = after_num[HI.len()..]
                    .chars()
                    .take_while(char::is_ascii_digit)
                    .collect();
                if !hi.is_empty() {
                    out.insert(format!("WINDOW [{lo}, {hi})"));
                }
            }
            rest = after_lo;
        }
    }
    out
}

/// AC7(e): the committed reference capture is byte-identical to
/// `2f78c53`. It is not copied into the base fixture, so a digest is its
/// only witness.
#[test]
fn the_log2_reference_capture_is_byte_identical_to_base() {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(golden_dir().join("log2_reference_capture.json"))
        .expect("read the committed capture");
    let got = format!("{:x}", Sha256::digest(&bytes));
    assert_eq!(
        got, "c4a6b1e930339848898f9a424b490ca98368082dee11eba0c1071e49a1a54c31",
        "the reference capture must not move — it is a black-box capture, not a rendering"
    );
}

/// AC7(f): 26 modified, 0 added, 0 removed — as a source-level invariant
/// rather than a diff stat, so it stays checkable after the commit.
///
/// Strictly stronger than the `git diff --stat` line it replaces: a test
/// process cannot see the working tree's git status, but it can see that
/// every one of the 26 differs from its committed base and that neither
/// directory gained or lost a name.
#[test]
fn the_golden_corpus_differs_from_its_base_copy_in_every_file() {
    for stem in GOLDEN_SQL {
        let base = read_golden(&golden_base_dir(), stem);
        let new = read_golden(&golden_dir(), stem);
        assert_ne!(
            new, base,
            "{stem}: this golden did not move, and issue #477 moves all 26"
        );
    }
    assert_eq!(GOLDEN_SQL.len(), 26);
}

// ---------------------------------------------------------------------------
// Issue #477 AC10(i)/(i-b): the stale-unit name scan.
//
// This is a BEST-EFFORT check and nobody may cite it as proof that no
// stale-unit name exists. It catches exactly two real failures — not
// renaming at all, and renaming the code while leaving the prose — and it
// is defeated by a consistent rename of all five spellings to another
// wrong name. That is accepted and published, not hidden: what enforces
// the MEANING is AC6 (the step grammar), AC7(c) (the goldens' millisecond
// intervals), AC5 (the exemplar budget's unit) and AC13 (the interval cap).
// ---------------------------------------------------------------------------

/// Identifiers that exist at `2f78c53` in the two metrics roots and are
/// FALSIFIED by issue #477. Derived from the tree, not predicted: each is
/// a name whose own sentence stops being true.
///
/// `step_s` names seconds and the field becomes milliseconds; the two
/// per-bucket exemplar constants become one total budget; and the two test
/// names assert grammars this change replaces.
const BANNED: [&str; 5] = [
    "step_s",
    "MAX_EXEMPLARS_PER_BUCKET",
    "DEFAULT_EXEMPLARS_PER_BUCKET",
    "explicit_step_forms_parse_to_whole_seconds",
    "non_positive_or_fractional_second_steps_are_rejected",
];

/// Whole-identifier scan over the RAW text of every `*.rs` under `dir` —
/// comments and string literals included.
///
/// Text-level on purpose, not a Rust lexer. The residue this check exists
/// for is prose: 14 of the 75 base occurrences sit in comments, four of
/// them sentences that say the step is in seconds, and a lexer walk would
/// be blind to every one of them while reporting green.
fn stale_spellings(dir: &std::path::Path) -> (Vec<(String, usize, &'static str)>, usize, usize) {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}")) {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                out.push(path);
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf();
    let mut files = Vec::new();
    walk(dir, &mut files);
    files.sort();
    let mut hits = Vec::new();
    let mut step_ms_occurrences = 0usize;
    for path in &files {
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned();
        for (i, line) in text.lines().enumerate() {
            for token in identifiers(line) {
                if let Some(b) = BANNED.iter().find(|b| **b == token) {
                    hits.push((rel.clone(), i + 1, *b));
                }
                if token == "step_ms" {
                    step_ms_occurrences += 1;
                }
            }
        }
    }
    (hits, files.len(), step_ms_occurrences)
}

/// Every `[A-Za-z_][A-Za-z0-9_]*` run in a line.
fn identifiers(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in line.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            if cur.is_empty() && c.is_ascii_digit() {
                continue;
            }
            cur.push(c);
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn metrics_root(rel: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .join(rel)
}

/// AC10(i)'s verdict over one scanned tree: the empty vector is GREEN and
/// every entry is a reason the tree is RED, in a fixed order.
///
/// Factored out of [`the_metrics_path_carries_no_stale_step_unit_token`]
/// so the staged mutation corpus under
/// `tests/fixtures/issue477/ac10i_trees/` runs THIS rule and not a second
/// copy of it (issue #477 wave 2, Q23). `file_floor` is the anti-vacuity
/// floor, which scales with the tree: 30 for the two real roots, a
/// fixture's own file count for a staged tree.
fn ac10i_reasons(
    hits: &[(String, usize, &'static str)],
    files: usize,
    step_ms: usize,
    file_floor: usize,
) -> Vec<String> {
    let mut out = Vec::new();
    // Anti-vacuity: a scan pointed at nothing must fail, not pass.
    if files < file_floor {
        out.push(format!(
            "only {files} .rs files scanned — the scan read almost nothing"
        ));
    }
    if step_ms < 1 {
        out.push("no `step_ms` anywhere — the scan is not reading the changed code".to_string());
    }
    if !hits.is_empty() {
        out.push(format!(
            "stale pre-#477 spellings survive in the metrics path:\n{}",
            hits.iter()
                .map(|(f, l, t)| format!("  {f}:{l} {t}"))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    out
}

/// AC10(i). Class CHANGE: at `2f78c53` this fails with 75 occurrences over
/// 73 lines in 5 files.
#[test]
fn the_metrics_path_carries_no_stale_step_unit_token() {
    let mut hits = Vec::new();
    let mut files = 0usize;
    let mut step_ms = 0usize;
    for root in [
        "crates/pulsus-read/src/traces",
        "crates/pulsus-server/src/traces_api",
    ] {
        let (h, n, m) = stale_spellings(&metrics_root(root));
        hits.extend(h);
        files += n;
        step_ms += m;
    }
    let reasons = ac10i_reasons(&hits, files, step_ms, 30);
    assert!(reasons.is_empty(), "{}", reasons.join("\n"));
}

/// Issue #477 wave 2, Q23 staged: the same rule over the six committed
/// mutation trees, each with the verdict and the occurrence count it must
/// give. The plan's Q23 named eighteen answers a reviewer had no way to
/// reproduce — the trees were built in a scratch directory and thrown
/// away — so the trees are committed here and the answers are asserted.
///
/// Fixtures are `.txt` and are materialised as `.rs` in a temporary
/// directory: an `.rs` file under `crates/*/tests/` is inside the domain
/// of `live_db_naming.rs` and `live_port_uniqueness.rs`, and a corpus of
/// deliberately-wrong source has no business being in either.
///
/// `e3b` is GREEN on purpose. It is the published defeat of this
/// best-effort scan: a consistent rename of all five spellings to a name
/// that is also wrong passes, because the rule compares spellings and not
/// meanings. What enforces the meaning is AC6, AC7(c), AC5 and AC13.
#[test]
fn the_stale_unit_scan_answers_the_committed_verdict_on_every_staged_tree() {
    /// `(tree, expected verdict is GREEN, banned-spelling occurrences)`.
    const ROWS: [(&str, bool, usize); 6] = [
        ("base", false, 7),
        ("e1", false, 1),
        ("e2", false, 4),
        ("e3", false, 1),
        ("e3b", true, 0),
        ("e4", false, 2),
    ];

    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/issue477/ac10i_trees");
    let scratch = std::env::temp_dir().join(format!("pulsus-i477-ac10i-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);

    let mut table = String::new();
    let mut failures: Vec<String> = Vec::new();
    for (tree, want_green, want_hits) in ROWS {
        let out = scratch.join(tree);
        std::fs::create_dir_all(&out).expect("create the staged tree");
        let mut staged = 0usize;
        for entry in std::fs::read_dir(src.join(tree)).expect("read the fixture tree") {
            let path = entry.expect("fixture entry").path();
            let stem = path
                .file_stem()
                .expect("stem")
                .to_string_lossy()
                .into_owned();
            std::fs::copy(&path, out.join(format!("{stem}.rs"))).expect("stage the fixture");
            staged += 1;
        }
        let (hits, files, step_ms) = stale_spellings(&out);
        // The floor is this tree's own file count, so the anti-vacuity
        // half still fires on an emptied directory without rejecting a
        // fixture for being small.
        let reasons = ac10i_reasons(&hits, files, step_ms, staged);
        let green = reasons.is_empty();
        table.push_str(&format!(
            "  {tree:<5} files={files} occurrences={} step_ms={step_ms} {}\n",
            hits.len(),
            if green { "GREEN" } else { "RED" }
        ));
        if green != want_green {
            failures.push(format!(
                "{tree}: expected {}, got {} ({reasons:?})",
                if want_green { "GREEN" } else { "RED" },
                if green { "GREEN" } else { "RED" }
            ));
        }
        if hits.len() != want_hits {
            failures.push(format!(
                "{tree}: expected {want_hits} banned occurrences, got {} ({hits:?})",
                hits.len()
            ));
        }
    }
    let _ = std::fs::remove_dir_all(&scratch);
    eprintln!("Q23 — AC10(i) over the staged trees:\n{table}");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// AC10(i-b). The complete set of NON-COMPILED residue outside the two
/// roots — occurrences `rustc` cannot see because they are prose. Measured
/// tree-wide at `2f78c53`: exactly three sites.
#[test]
fn no_stale_step_or_exemplar_spelling_survives_outside_the_metrics_roots() {
    let mut hits: Vec<String> = Vec::new();
    for rel in [
        "crates/pulsus-read/tests/traces_alloc_audit.rs",
        "crates/pulsus-read/tests/traces_metrics_live.rs",
        "docs/api.md",
    ] {
        let path = metrics_root(rel);
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        for (i, line) in text.lines().enumerate() {
            for token in identifiers(line) {
                if BANNED.contains(&token.as_str()) {
                    hits.push(format!("  {rel}:{} {token}", i + 1));
                }
            }
        }
    }
    assert!(
        hits.is_empty(),
        "stale pre-#477 spellings survive where the compiler cannot see them:\n{}",
        hits.join("\n")
    );
}
