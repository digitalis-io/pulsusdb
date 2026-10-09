//! Issue #293 — the `build_metric_node` conversion's pre-change oracle.
//!
//! `plan()`'s `MetricBinary` route was a per-node recursion over
//! `MetricExpr` and is now two loops (a `find_preorder` emission pass and
//! a reverse fold). The two must agree on **everything observable**: the
//! planned tree, and — because a plan is largely a rejection surface —
//! the FIRST error every malformed shape produces, which depends on the
//! order a node's own fallible work runs relative to its operands'.
//!
//! **The golden is a differential, not a self-portrait.** It was captured
//! by running [`render_golden`] verbatim against the RECURSIVE
//! implementation at `origin/main` `ae66648` (the commit this branch
//! forked from) and committed unchanged; this suite replays it against
//! the iterative one. A golden written after the change would only prove
//! the new code is self-consistent, which is exactly what a conversion
//! defect also is.
//!
//! Content-hash frozen by `characterization_freeze.rs`.

use std::collections::BTreeSet;

use pulsus_logql::{BinOp, parse};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};

const GOLDEN: &str = include_str!("golden/plan_build_differential.txt");

fn ctx() -> PlanCtx<'static> {
    PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

const START_NS: i64 = 1_782_907_200_000_000_000;
const END_NS: i64 = 1_782_928_800_000_000_000;

fn range_params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

fn instant_params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Instant { at_ns: START_NS },
        limit: 100,
        direction: Direction::Backward,
    }
}

/// Every shape that reaches the converted walk, plus the shapes that must
/// NOT reach it (a `Vector` chain bottoming at `Range` stays on the
/// `Plan::Metric` route). Each runs under both `QuerySpec` kinds, because
/// `is_range` gates `approx_topk` and `window_from` differs.
///
/// Grouped by the arm each row exercises; the error rows are the ones
/// that pin evaluation ORDER, which is the property a linearised program
/// can silently change.
const CORPUS: &[&str] = &[
    // --- leaves ---------------------------------------------------
    "1",
    "vector(1)",
    "vector(2.5)",
    r#"rate({a="b"}[5m]) + 0"#,
    // --- binary trees, left-deep and right-nested -----------------
    "1 + 2",
    "1 + 2 * 3",
    "(1 + 2) * 3",
    "1 + 1 + 1 + 1 + 1",
    "1 + (1 + (1 + (1 + 1)))",
    // The per-operator rows are GENERATED from `BinOp::ALL` (see
    // `generated_bin_op_rows`), not written out here: a hand-written list
    // is a second source that a new variant can silently miss.
    r#"rate({a="b"}[5m]) or rate({c="d"}[5m])"#,
    r#"rate({a="b"}[5m]) or rate({c="d"}[5m]) or rate({e="f"}[5m])"#,
    r#"rate({a="b"}[5m]) and rate({c="d"}[5m])"#,
    r#"rate({a="b"}[5m]) unless rate({c="d"}[5m])"#,
    // The whole `VectorMatching` space: `on`/`ignoring`, an EMPTY label
    // list, and both `MatchGroup` sides with and without include labels.
    // `matching` is cloned into the emitted program, so a conversion that
    // dropped or mis-placed the clone shows up here.
    r#"rate({a="b"}[5m]) / on (x) rate({c="d"}[5m])"#,
    r#"rate({a="b"}[5m]) * on () rate({c="d"}[5m])"#,
    r#"rate({a="b"}[5m]) - ignoring (x, z) rate({c="d"}[5m])"#,
    r#"rate({a="b"}[5m]) / ignoring (x) group_left (y) rate({c="d"}[5m])"#,
    r#"rate({a="b"}[5m]) / on (x) group_right (y) rate({c="d"}[5m])"#,
    r#"rate({a="b"}[5m]) / on (x) group_left rate({c="d"}[5m])"#,
    r#"rate({a="b"}[5m]) / ignoring (x) group_right rate({c="d"}[5m])"#,
    r#"rate({a="b"}[5m]) > bool on (x) rate({c="d"}[5m])"#,
    // --- the vector-aggregation chain -----------------------------
    // Base is `Range`: the WHOLE chain collapses onto one leaf.
    r#"sum(rate({a="b"}[5m])) + 1"#,
    r#"sum(max(rate({a="b"}[5m]))) + 1"#,
    r#"topk(3, sum by (x) (rate({a="b"}[5m]))) + 1"#,
    // Base is `Binary`: one `VectorAgg` node carrying every layer.
    r#"sum(rate({a="b"}[5m]) + rate({c="d"}[5m]))"#,
    r#"sum(max(rate({a="b"}[5m]) + rate({c="d"}[5m])))"#,
    r#"sum by (x) (max without (y) (topk(3, rate({a="b"}[5m]) + 1)))"#,
    r#"sum(rate({a="b"}[5m]) + rate({c="d"}[5m])) / sum(rate({e="f"}[5m]) + 1)"#,
    // Base is `VectorFn` / `Variants`.
    "sum(vector(1))",
    "sum(max(vector(1)))",
    r#"variants(count_over_time({a="b"}[5m])) of ({a="b"}[5m])"#,
    r#"variants(count_over_time({a="b"}[5m]), sum by (lvl) (rate({a="b"}[5m]))) of ({a="b"}[5m])"#,
    r#"variants(count_over_time({a="b"}[5m])) of ({a="b"}[5m]) + 1"#,
    r#"1 + variants(count_over_time({a="b"}[5m])) of ({a="b"}[5m])"#,
    // Two independent chains in one expression (the pending-layer
    // accumulator must not leak across them).
    r#"sum(rate({a="b"}[5m]) + 1) + max(rate({c="d"}[5m]) + 2)"#,
    r#"sum(rate({a="b"}[5m]) + 1) + sum(rate({c="d"}[5m]))"#,
    // --- rejections: the chain's own arms -------------------------
    "sum(1)",
    "sum(max(1))",
    "sum(1) + rate({a=\"b\"}[5m])",
    r#"rate({a="b"}[5m]) + sum(1)"#,
    r#"sort by (x) (rate({a="b"}[5m]) + 1)"#,
    r#"approx_topk(2, rate({a="b"}[5m]) + 1)"#,
    r#"sum(approx_topk(2, rate({a="b"}[5m]) + 1))"#,
    // --- rejections: ORDER between a layer and its base -----------
    // The layer's `approx_topk` rejection must win over the base's
    // bad-regex rejection under a range spec, because the recursion
    // validated the layers before planning the base.
    r#"approx_topk(2, rate({a="b"} |~ "(" [5m]) + 1)"#,
    r#"sum(rate({a="b"} |~ "(" [5m]) + 1)"#,
    // --- rejections: ORDER between lhs and rhs --------------------
    // Both operands are bad; `lhs` must be the one reported.
    r#"sum(1) + sum(2)"#,
    r#"rate({a="b"} |~ "(" [5m]) + rate({c="d"} |~ "[" [5m])"#,
    // --- rejections: leaf arms ------------------------------------
    r#"rate({a="b"} |~ "(" [5m]) + 1"#,
    r#"1 + rate({a="b"} |~ "(" [5m])"#,
    r#"quantile_over_time(0.9, {a="b"} | unwrap d [5m]) + 1"#,
    r#"sum_over_time({a="b"}[5m]) + 1"#,
];

/// One row per `BinOp` variant, plus a second row per COMPARISON
/// variant in its `bool` form — **generated from
/// [`pulsus_logql::BinOp::ALL`]**, which the same macro invocation
/// declares alongside the enum itself.
///
/// This is the difference between a census and a construction: a new
/// operator gets corpus rows because the slice grew, not because someone
/// remembered to add them. Adding a variant makes the rendered corpus
/// differ from the committed golden, so the differential fails until the
/// golden is regenerated against the reference planner.
///
/// Both operands are range aggregations, which is the one operand shape
/// every operator accepts (the set operators reject scalars), so no
/// variant degrades into a parse error and contributes nothing.
fn generated_bin_op_rows() -> Vec<String> {
    let mut out = Vec::new();
    for op in BinOp::ALL {
        out.push(format!(r#"rate({{a="b"}}[5m]) {op} rate({{c="d"}}[5m])"#));
        if op.is_comparison() {
            out.push(format!(
                r#"rate({{a="b"}}[5m]) {op} bool rate({{c="d"}}[5m])"#
            ));
        }
    }
    out
}

/// Every query the golden covers: the curated shapes above, then the
/// generated per-operator rows.
fn all_queries() -> Vec<String> {
    let mut out: Vec<String> = CORPUS.iter().map(|q| (*q).to_string()).collect();
    out.extend(generated_bin_op_rows());
    out
}

/// The whole corpus, rendered. Deliberately dependency-free — no
/// serializer, no formatter beyond `{:#?}` — so it produces the same
/// bytes on either side of the conversion.
fn render_golden() -> String {
    let mut out = String::new();
    out.push_str("# issue #293 — plan() over the MetricBinary route\n");
    out.push_str("# captured from the RECURSIVE build_metric_node; replayed iteratively\n");
    let c = ctx();
    for query in &all_queries() {
        for (label, params) in [("range", range_params()), ("instant", instant_params())] {
            out.push_str("\n=== ");
            out.push_str(label);
            out.push_str(" | ");
            out.push_str(query);
            out.push('\n');
            let expr = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
            match plan(&expr, &params, &c) {
                Ok(p) => out.push_str(&format!("{p:#?}\n")),
                Err(e) => out.push_str(&format!("ERR {e:?}\n")),
            }
        }
    }
    out
}

#[test]
fn the_iterative_planner_reproduces_the_recursive_planners_output_byte_for_byte() {
    assert_eq!(
        render_golden(),
        GOLDEN,
        "the converted `build_metric_node` produced a different plan or a different first \
         error than the recursion it replaced"
    );
}

/// A corpus that only exercised the easy arms would pass vacuously.
#[test]
fn the_corpus_covers_both_outcomes_and_every_planned_node_kind() {
    let body = GOLDEN;
    for marker in [
        "Binary {",
        "VectorAgg {",
        "Scalar(",
        "VectorLit {",
        "Leaf(",
        "Variants {",
        "ERR ",
    ] {
        assert!(
            body.contains(marker),
            "the golden never exercises {marker:?} — the differential is weaker than it looks"
        );
    }
    let errors = body.matches("\nERR ").count();
    let plans = body.matches("\nMetricBinary(").count() + body.matches("\nMetric(").count();
    assert!(
        errors >= 20 && plans >= 40,
        "corpus coverage collapsed: {errors} rejections / {plans} plans"
    );
}

/// Every `BinOp` variant is actually PLANNED — asserted against
/// [`pulsus_logql::BinOp::ALL`], the slice the enum's own macro emits, so
/// this cannot be satisfied by a list that has drifted from the enum.
///
/// The generator above makes the corpus grow with the enum; this checks
/// that each generated row really reached a `Binary` node rather than
/// being rejected on the way.
#[test]
fn every_bin_op_variant_is_planned_in_the_golden() {
    let mut names: BTreeSet<String> = BTreeSet::new();
    for op in BinOp::ALL {
        // `{:?}` is the derive's, which is what the golden renders.
        let rendered = format!("op: {op:?},");
        assert!(
            GOLDEN.contains(&rendered),
            "no golden row plans `BinOp::{op:?}` — byte-equivalence is unestablished for it"
        );
        assert!(
            names.insert(format!("{op:?}")),
            "`BinOp::ALL` repeats {op:?}"
        );
    }
    assert_eq!(
        names.len(),
        BinOp::ALL.len(),
        "`BinOp::ALL` is not a set of distinct variants"
    );
    // The corpus is driven BY that slice, so the two can only agree.
    let generated = generated_bin_op_rows();
    let comparisons = BinOp::ALL.iter().filter(|op| op.is_comparison()).count();
    assert_eq!(
        generated.len(),
        BinOp::ALL.len() + comparisons,
        "the generator stopped covering one row per operator plus one per comparison"
    );
    for q in &generated {
        assert!(
            GOLDEN.contains(&format!("| {q}\n")),
            "the golden predates a generated row ({q}) — regenerate it against the reference \
             planner, never against this branch"
        );
    }
}

/// The `bool` modifier and the whole `VectorMatching` space, same
/// disposition: enumerated from the type, not from the corpus.
#[test]
fn both_return_bool_states_and_every_vector_matching_shape_are_planned() {
    for expected in [
        "return_bool: false",
        "return_bool: true",
        // `on: true` / `on: false`, an empty label list, and both
        // `MatchGroup` sides with and without include labels.
        "on: true",
        "on: false",
        "labels: []",
        "group: None",
        "Left(\n",
        "Right(\n",
    ] {
        assert!(
            GOLDEN.contains(expected),
            "no golden row plans {expected:?} — the binary construct space is only sampled"
        );
    }
}

/// Issues #241, #507 and #624 — **every planned range leaf that is NOT a
/// clean counting chain is client-aggregated, and a clean one is not, at
/// any range and step.**
///
/// #507 lowered one shape — one of the four counting reducers, nothing in
/// the pipeline beyond a pushable line filter — at a range equal to the
/// step; #624 lowers the same shape at every range and step. So the clean
/// case is asserted at three steps against the corpus' `[5m]`:
///
/// ```text
///   step 60s    range > step    the sliding statement (issue #624)
///   step  5m    range == step   #507's statement
///   step 10m    range < step    the sliding statement (issue #624)
/// ```
///
/// The classifier is written here from the QUERY, not imported from the
/// planner, so the two cannot agree by construction.
///
/// **Scope, so nobody reads this as the proof.** The PROOF is a total
/// read of the only `MetricPlan` constructor: `git grep -n "MetricPlan {"
/// -- crates` has one production hit, `plan.rs`'s `metric_plan`. This
/// test covers the corpus below — dozens of queries, not the query
/// language — so it is a regression DETECTOR, not the establishing
/// argument.
///
/// Non-vacuity is asserted by counts, not by the test passing: a corpus
/// that planned nothing, or one where nothing lowered, would satisfy a
/// bare `all()`.
#[test]
fn every_planned_range_leaf_that_is_not_a_clean_bucketed_chain_is_client_aggregated() {
    let c = ctx();
    let params = range_params();
    let mut leaves = 0usize;
    let mut planned = 0usize;
    for query in &all_queries() {
        let expr = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
        let Ok(p) = plan(&expr, &params, &c) else {
            continue;
        };
        planned += 1;
        let mps: Vec<&pulsus_read::logql::MetricPlan> = match &p {
            Plan::Streams(_) => Vec::new(),
            Plan::Metric(mp) => vec![mp],
            Plan::MetricBinary(node) => node.leaves(),
        };
        for mp in mps {
            leaves += 1;
            assert!(
                mp.step_ns.is_some(),
                "`{query}` planned a range query with `step_ns: None` — the instant/range \
                 discriminant `run_metric_inner` branches on has moved"
            );
        }
    }
    assert!(
        planned >= 40 && leaves >= 40,
        "the range corpus collapsed: {planned} plans / {leaves} metric leaves — a vacuous \
         pass here would prove nothing"
    );

    // --- the clean case, asserted positively, at three steps ---------
    //
    // A leaf lowers exactly when its query names one of the four counting
    // reducers with nothing piped after the selector; the classifier is a
    // scan of the query text, so it shares no helper with the planner.
    let clean_bucketed = |query: &str| {
        let reducers = [
            "count_over_time({",
            "bytes_over_time({",
            "rate({",
            "bytes_rate({",
            // Issue #624 part 2: presence, from the same counts.
            "absent_over_time({",
        ];
        // `variants(...)` plans ONE multi-extractor scan with
        // `force_client = true` — its leaf is a scan shared by every
        // variant, not a chain of its own, so it never lowers however
        // clean the extractor looks.
        reducers.iter().any(|r| query.contains(r))
            && !query.contains("variants(")
            && staged_stages(query).is_some()
    };
    for (step_ns, reason) in [
        (
            60_000_000_000u64,
            "raw: sliding range aggregation in the database (issue #624)",
        ),
        (
            300_000_000_000,
            "raw: bucketed range aggregation (issue #507)",
        ),
        (
            600_000_000_000,
            "raw: sliding range aggregation in the database (issue #624)",
        ),
    ] {
        let step_params = QueryParams {
            spec: QuerySpec::Range {
                start_ns: START_NS,
                end_ns: END_NS,
                step_ns,
            },
            limit: 100,
            direction: Direction::Backward,
        };
        let mut lowered = 0usize;
        let mut client = 0usize;
        for query in &all_queries() {
            let expr = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
            let Ok(p) = plan(&expr, &step_params, &c) else {
                continue;
            };
            let mps: Vec<&pulsus_read::logql::MetricPlan> = match &p {
                Plan::Streams(_) => Vec::new(),
                Plan::Metric(mp) => vec![mp],
                Plan::MetricBinary(node) => node.leaves(),
            };
            // A query holding several leaves can mix the two, so the
            // classifier is applied to the WHOLE query only where it holds
            // one.
            if mps.len() != 1 {
                continue;
            }
            for mp in mps {
                if clean_bucketed(query) {
                    lowered += 1;
                    assert!(
                        mp.client.is_none(),
                        "`{query}` at step {step_ns} is a clean counting chain, so its \
                         aggregation must lower into the statement (issues #507 / #624)"
                    );
                    // Issue #624 part 2: label stages, or `absent_over_time`,
                    // run over the statement's rows, and the reason says so.
                    let staged = staged_stages(query).is_some_and(|n| n > 0)
                        || query.contains("absent_over_time(");
                    let want = if staged {
                        "raw: range aggregation in the database, label stages over its rows (issue #624)"
                    } else {
                        reason
                    };
                    assert_eq!(mp.routing.reason, want, "{query} at step {step_ns}");
                } else {
                    client += 1;
                    assert!(
                        mp.client.is_some(),
                        "`{query}` at step {step_ns} is not a clean counting chain, so it \
                         must stay client-aggregated (issues #241 / #507)"
                    );
                }
            }
        }
        assert!(
            lowered >= 4 && client >= 4,
            "step {step_ns}: the positive half is vacuous: {lowered} lowered / {client} \
             client-aggregated"
        );
    }
}

/// The count of label-only stages in a single-reducer query's pipeline, or
/// `None` when it holds any other stage (issue #624, part 2). Read from the
/// query text, so it shares nothing with the planner: a stage is `drop`,
/// `keep`, `decolorize` (not under a bytes reducer), or a filter on a label
/// name with a string matcher; a line filter (`|=`, `|~`, `!=`, `!~`) or
/// anything else is not one.
fn staged_stages(query: &str) -> Option<usize> {
    let open = query.find("({")?;
    let close = query[open..]
        .find('[')
        .map(|i| open + i)
        .unwrap_or(query.len());
    let selector_end = query[open..close].find('}').map(|i| open + i + 1)?;
    let pipeline = &query[selector_end..close];
    let bytes = query.contains("bytes_over_time(") || query.contains("bytes_rate(");
    let mut stages = 0usize;
    for segment in pipeline.split('|').skip(1) {
        let segment = segment.trim();
        let label_filter = segment.split_once(['=', '!']).is_some_and(|(name, _)| {
            let name = name.trim();
            !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        }) && segment.contains('"');
        let admitted = segment.starts_with("drop ")
            || segment.starts_with("keep ")
            || (segment == "decolorize" && !bytes)
            || label_filter;
        if !admitted {
            return None;
        }
        stages += 1;
    }
    Some(stages)
}

/// `cargo test -p pulsus-read --test logql_plan_build_differential -- --ignored zz_regenerate`
///
/// **Run this on the PRE-change tree only.** Regenerating it after a
/// planner change replaces the oracle with the thing it is meant to
/// check.
#[test]
#[ignore = "generator: rewrites the committed golden"]
fn zz_regenerate_golden() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    std::fs::create_dir_all(&dir).expect("golden dir");
    let body = render_golden();
    std::fs::write(dir.join("plan_build_differential.txt"), &body).expect("write golden");
    let digest = <sha2::Sha256 as sha2::Digest>::digest(body.as_bytes());
    std::fs::write(
        dir.join("plan_build_differential.sha256"),
        format!("{digest:x}\n"),
    )
    .expect("write sha256");
}
