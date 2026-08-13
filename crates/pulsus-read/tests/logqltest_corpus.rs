//! Issue #220 (Batch 0): the hermetic `logqltest` corpus entry point — a
//! promqltest-style replayer for LogQL value/streams `.test` files against
//! the pure evaluator in `pulsus-read`. No ClickHouse; the pinned reference
//! container (`grafana/loki:3.7.4`) is touched ONLY to capture a new case's
//! expected value (see `tests/logqltest/PROVENANCE.md`), never at test time.
//!
//! Batch 0 seeds the corpus with the instant-eval subset of today's 39
//! differential cases (`test/fixtures/logs/differential.json`) ported into
//! the DSL; later batches (B1–B6) just drop new `.test` files into the
//! corpus dir — they are discovered from disk, so batches never edit a
//! shared list (parallel-safe) and a file cannot silently drop out.

#[path = "logqltest/mod.rs"]
mod driver;

use driver::runner::{DirectiveCounts, EvalMode, run_file};

/// Every `.test` file in `tests/logqltest/corpus`, discovered from disk (not
/// a hardcoded list) so batches only ever ADD files with no shared edit point
/// — parallel-safe.
///
/// **What discovery does and does not guarantee** (issue #249, correcting
/// the sentence that stood here). Reading the directory makes the replay run
/// whatever is on disk, so a file cannot be silently SKIPPED — but a file
/// that is DELETED simply stops being discovered, and nothing here notices.
/// The floor in [`corpus_dir_is_populated`] is what closes that: it is the
/// current file count, so removing a file fails the build. It is a COUNT,
/// deliberately not a name list — a list would be the shared edit point the
/// per-batch parallelism exists to avoid.
fn corpus_files() -> Vec<String> {
    let mut on_disk: Vec<String> = std::fs::read_dir(driver::corpus_dir())
        .expect("corpus dir exists")
        .map(|e| e.expect("readable dir entry").file_name())
        .filter_map(|n| {
            let n = n.to_string_lossy().to_string();
            n.ends_with(".test").then_some(n)
        })
        .collect();
    on_disk.sort();
    on_disk
}

/// Count `eval*` directives in a `.test` file's raw text, compared per-file
/// against the cases the runner produced — so a parse bug that silently drops
/// an `eval` is caught without needing a fragile global exact count.
fn count_eval_directives(text: &str) -> usize {
    text.lines()
        .filter(|l| {
            let tok = l.split_whitespace().next().unwrap_or("");
            matches!(tok, "eval" | "eval_ordered" | "eval_fail")
        })
        .count()
}

/// The corpus directory is populated (a catastrophic dir/glob failure or an
/// empty checkout is caught) and NO file has been deleted.
///
/// The floor is the count on disk, raised by each batch that adds a file
/// (issue #249: 39 -> 40 with `b25_structured_metadata.test`; issue #278:
/// 40 -> 43, which is 42 files that were already on disk under a floor
/// nobody had raised, plus `b26_line_filter_pushdown.test`). A deletion
/// drops the count below the floor and fails here — which is the half of
/// the anti-drop guarantee that disk discovery cannot give on its own.
#[test]
fn corpus_dir_is_populated() {
    let files = corpus_files();
    assert!(
        files.len() >= 43,
        "expected at least the 43 committed .test files, found {} — a file was \
         deleted, or the batch that added one did not raise this floor: {files:?}",
        files.len()
    );
}

/// The whole corpus replays 100% green, and exercises every executed
/// directive kind (`clear`, `load`, `eval`, `eval_ordered`, `eval_fail`)
/// and every result kind (streams + instant vector).
#[test]
fn corpus_is_fully_green_and_exercises_every_directive() {
    let mut totals = DirectiveCounts::default();
    let mut failures: Vec<String> = Vec::new();
    let mut case_count = 0usize;

    for name in &corpus_files() {
        let path = driver::corpus_dir().join(name);
        let text = driver::read_file(&path);
        let run = run_file(name, &text).unwrap_or_else(|e| panic!("{e}"));
        // Per-file no-drop guard: every `eval*` directive in the file must
        // have produced a case (parallel-safe — no global exact count).
        assert_eq!(
            run.cases.len(),
            count_eval_directives(&text),
            "{name}: {} eval directives but {} cases — a parse bug dropped some",
            count_eval_directives(&text),
            run.cases.len()
        );
        case_count += run.cases.len();
        for case in &run.cases {
            if !case.passed {
                failures.push(format!(
                    "{name}:{} `{}` — {}",
                    case.line, case.query, case.detail
                ));
            }
        }
        totals.clear += run.counts.clear;
        totals.load += run.counts.load;
        totals.eval_value += run.counts.eval_value;
        totals.eval_ordered += run.counts.eval_ordered;
        totals.eval_fail += run.counts.eval_fail;
        totals.streams_cases += run.counts.streams_cases;
        totals.vector_cases += run.counts.vector_cases;
        totals.scalar_cases += run.counts.scalar_cases;
        totals.matrix_cases += run.counts.matrix_cases;
        totals.detected_cases += run.counts.detected_cases;
    }

    assert!(
        failures.is_empty(),
        "the logqltest corpus must be 100% green:\n{}",
        failures.join("\n")
    );

    // A growing floor: Batch 0 seeds 32 instant-eval cases; later batches
    // only add more. The per-file no-drop guard above is what catches
    // silently-dropped cases — this is just a corpus-shrink backstop.
    assert!(
        case_count >= 32,
        "expected at least the 32 seed cases, found {case_count}"
    );
    assert!(totals.clear > 0, "corpus never exercised `clear`");
    assert!(totals.load > 0, "corpus never exercised `load`");
    assert!(totals.eval_value > 0, "corpus never exercised `eval`");
    assert!(
        totals.eval_ordered > 0,
        "corpus never exercised `eval_ordered` (sort/sort_desc)"
    );
    assert!(
        totals.eval_fail > 0,
        "corpus never exercised `eval_fail` (error cases)"
    );
    assert!(
        totals.streams_cases > 0,
        "corpus never produced a streams result"
    );
    assert!(
        totals.vector_cases > 0,
        "corpus never produced a vector result"
    );
    assert!(
        totals.matrix_cases > 0,
        "corpus never produced a matrix (range) result — issue #227 `eval range`"
    );
    assert!(
        totals.detected_cases > 0,
        "corpus never produced a detected-fields result — issue #244 `eval detected`"
    );
}

/// The #218 guard, exercised for real: a deliberately-perturbed expected
/// value must redden the runner (exact-f64, not tolerance). A one-ULP
/// change to a captured `rate_counter` value is caught.
#[test]
fn a_perturbed_expected_value_reddens_the_runner() {
    let dataset = "load\n  {env=\"prod\", service_name=\"checkout\"} service=checkout\n\
                   \t10s  c=10\n\t20s  c=30\n\t30s  c=5\n\t40s  c=12\n\n";
    // Correct capture passes.
    let good = format!(
        "{dataset}eval instant at 60s rate_counter({{env=\"prod\"}} | logfmt | unwrap c [1m])\n\
         \t{{env=\"prod\", service_name=\"checkout\"}} 0.5333344\n"
    );
    let run = run_file("inline/rate_counter_good.test", &good).expect("parse");
    assert!(
        run.cases[0].passed,
        "correct capture: {}",
        run.cases[0].detail
    );

    // A one-ULP perturbation of the expected value must FAIL (exact bits).
    let perturbed_bits = 0.5333344_f64.to_bits() + 1;
    let perturbed = f64::from_bits(perturbed_bits);
    let bad = format!(
        "{dataset}eval instant at 60s rate_counter({{env=\"prod\"}} | logfmt | unwrap c [1m])\n\
         \t{{env=\"prod\", service_name=\"checkout\"}} {perturbed}\n"
    );
    let run = run_file("inline/rate_counter_bad.test", &bad).expect("parse");
    assert!(
        !run.cases[0].passed,
        "a one-ULP perturbation must redden the runner (exact-f64, no tolerance)"
    );
    assert!(
        run.cases[0].detail.contains("value mismatch"),
        "the failure must name a value mismatch: {}",
        run.cases[0].detail
    );
    assert_eq!(run.cases[0].mode, EvalMode::Value);
}

/// Issue #240 AC7(g), the positive direction: the new plan-time regex
/// validation must reject NOTHING the corpus accepts. It walks every
/// `eval`/`eval_ordered` log query in the corpus through the real
/// planner and requires `Ok` — zero false rejections from the
/// pushed-down line-filter/matcher validation.
///
/// It was written because the corpus runner did not route LOG queries
/// through `plan()` at all. Issue #278 closed that: the runner now plans
/// every leg, so a plan-time rejection would surface as a case failure
/// in the green run too. This is kept as the DIRECT statement of the
/// property — it names the planner, asserts a floor on how many queries
/// it reached, and would still fail if a future runner change routed
/// around `plan()` again.
#[test]
fn every_corpus_log_query_still_plans_ok_under_regex_validation() {
    use pulsus_read::logql::{Direction, QueryParams, QuerySpec, plan};
    let mut checked = 0usize;
    for name in &corpus_files() {
        let path = driver::corpus_dir().join(name);
        let text = driver::read_file(&path);
        for (i, line) in text.lines().enumerate() {
            let mut tokens = line.split_whitespace();
            let directive = tokens.next().unwrap_or("");
            if !matches!(directive, "eval" | "eval_ordered") {
                continue;
            }
            // `eval[/_ordered] instant at <T> <query>` — range evals are
            // metric queries by construction (already planned green).
            let rest = line.split_once(" at ").map(|(_, r)| r);
            let Some(rest) = rest else { continue };
            let Some((t_tok, query)) = rest.trim().split_once(' ') else {
                continue;
            };
            let Ok(at_ns) = driver::runner::parse_duration_ns(t_tok) else {
                continue;
            };
            let Ok(expr) = pulsus_logql::parse(query.trim()) else {
                continue;
            };
            if !matches!(expr, pulsus_logql::Expr::Log(_)) {
                continue;
            }
            let params = QueryParams {
                spec: QuerySpec::Instant { at_ns },
                limit: 100,
                direction: Direction::Backward,
            };
            let ctx = pulsus_read::logql::PlanCtx {
                db: "pulsus",
                streams_idx: "log_streams_idx",
                streams: "log_streams",
                samples: "log_samples",
                rollup_table: "log_metrics_5s",
                rollup_res_ns: 5_000_000_000,
                scan_budget_bytes: 50 * 1024 * 1024 * 1024,
                max_streams: 100_000,
                pipeline_scan_factor: 10,
            };
            plan(&expr, &params, &ctx).unwrap_or_else(|e| {
                panic!("{name}:{}: corpus log query must still plan Ok: {e}", i + 1)
            });
            checked += 1;
        }
    }
    assert!(
        checked >= 30,
        "expected to plan a meaningful number of corpus log queries, got {checked}"
    );
}

/// Issue #240 AC3: `msg_exact:` exists, is anchored on the WHOLE produced
/// error, and its grammar is exactly-one-nonempty-assert-line — four
/// negative controls, each proved to redden/reject for real.
#[test]
fn msg_exact_discriminates_and_its_grammar_is_exactly_one_assert_line() {
    let dataset = "load\n  {env=\"prod\", service_name=\"checkout\"} service=checkout\n\
                   \t10s  c=10\n\n";
    let body = "count min sketches are only supported on instant queries";
    let query = "eval_fail range from 0s to 300s step 60s approx_topk(2, count_over_time({env=\"prod\"}[1m]))";

    // Control 0 (green baseline): the exact produced text passes.
    let good = format!("{dataset}{query}\n\tmsg_exact: {body}\n");
    let run = run_file("inline/msg_exact_good.test", &good).expect("parse");
    assert!(
        run.cases[0].passed,
        "byte-exact match: {}",
        run.cases[0].detail
    );

    // (i) a one-byte perturbation reddens.
    let perturbed = format!(
        "{dataset}{query}\n\tmsg_exact: {}\n",
        body.replace('m', "n")
    );
    let run = run_file("inline/msg_exact_perturbed.test", &perturbed).expect("parse");
    assert!(
        !run.cases[0].passed,
        "a one-byte perturbation must redden msg_exact"
    );

    // (ii) a strict SUBSTRING expectation reddens under msg_exact (the
    // same value passes under msg:).
    let substring = "count min sketches";
    let strict = format!("{dataset}{query}\n\tmsg_exact: {substring}\n");
    let run = run_file("inline/msg_exact_substring.test", &strict).expect("parse");
    assert!(
        !run.cases[0].passed,
        "a strict substring must redden msg_exact (it is anchored, not contains)"
    );
    let loose = format!("{dataset}{query}\n\tmsg: {substring}\n");
    let run = run_file("inline/msg_substring.test", &loose).expect("parse");
    assert!(run.cases[0].passed, "msg: stays a substring gate");

    // (iii) two assert lines is a parse-time grammar error naming the line.
    let two = format!("{dataset}{query}\n\tmsg: {substring}\n\tmsg_exact: {body}\n");
    let err = run_file("inline/msg_two_lines.test", &two).expect_err("two assert lines");
    assert!(
        err.contains("more than one assert line"),
        "grammar error: {err}"
    );

    // (iv) an empty value is a parse-time grammar error.
    let empty = format!("{dataset}{query}\n\tmsg_exact:\n");
    let err = run_file("inline/msg_empty.test", &empty).expect_err("empty value");
    assert!(err.contains("empty value"), "grammar error: {err}");

    // And zero assert lines is a grammar error too (the exactly-one rule's
    // other edge; the plan's grammar statement).
    let zero = format!("{dataset}{query}\n");
    let err = run_file("inline/msg_zero_lines.test", &zero).expect_err("no assert line");
    assert!(err.contains("requires exactly one"), "grammar error: {err}");
}

// ---------------------------------------------------------------------
// Issue #278 — the runner executes the line filters the planner pushes
// into SQL, on every leg it can reach, and refuses the one it cannot.
//
// Each test below names the break that reddens it, because the whole
// subject of this issue is a check that could not fail.
// ---------------------------------------------------------------------

/// AC1. A pushed-down line filter is APPLIED on the streams leg.
///
/// *Break:* point the `Expr::Log` arm back at `CompiledPipeline::compile`
/// — the case returns both lines and this goes RED.
#[test]
fn the_streams_leg_applies_a_pushed_down_line_filter() {
    let text = "load\n  {app=\"s\", service_name=\"pd-streams\"}\n\
                \t0s   alpha foo bravo\n\t5s   alpha bar bravo\n\n\
                eval instant at 60s {service_name=\"pd-streams\"} |= \"foo\"\n\
                \t{app=\"s\", service_name=\"pd-streams\"} 0s alpha foo bravo\n";
    let run = run_file("inline/pushdown_streams.test", text).expect("parse");
    assert!(
        run.cases[0].passed,
        "the pushed-down `|= \"foo\"` must exclude the `bar` line: {}",
        run.cases[0].detail
    );
}

/// AC2. And on the metric leg, where the filter changes the COUNT.
///
/// The `| logfmt` is not decoration and not a workaround: a range
/// aggregation with NO pipeline plans server-side (`raw: instant query`,
/// `client` is `None`) and this hermetic runner has always refused those
/// — "only client-aggregated (raw-scan) metric plans". A parser stage
/// is what puts the aggregation on the client path the runner executes,
/// and it does not rewrite the line, so the filter ahead of it is still
/// PUSHED. That is the shape this test needs.
///
/// *Break:* point `eval_leaf` back at `CompiledPipeline::compile` — the
/// sum is the unfiltered `2` and this goes RED.
#[test]
fn the_metric_leg_applies_a_pushed_down_line_filter() {
    let text = "load\n  {app=\"m\", service_name=\"pd-metric\"}\n\
                \t0s   kind=keep n=1\n\t5s   kind=drop n=2\n\n\
                eval instant at 60s sum(count_over_time({service_name=\"pd-metric\"} \
                |= \"keep\" | logfmt [1m]))\n\
                \t{} 1\n";
    let run = run_file("inline/pushdown_metric.test", text).expect("parse");
    assert!(
        run.cases[0].passed,
        "the pushed-down filter must reduce the sum to 1: {}",
        run.cases[0].detail
    );
}

/// AC3. And on the `detected` leg, where the filter removes the only
/// line carrying a field.
///
/// *Break:* the same, in `evaluate_detected` — `b` reappears and `kind`
/// reports a cardinality of two, RED.
#[test]
fn the_detected_leg_applies_a_pushed_down_line_filter() {
    let text = "load\n  {app=\"d\", service_name=\"pd-detected\"}\n\
                \t10s  kind=keep a=1\n\t20s  kind=drop b=2\n\n\
                eval detected at 60s {service_name=\"pd-detected\"} |= \"keep\" | logfmt\n\
                \ta int 1 logfmt\n\tkind string 1 logfmt\n";
    let run = run_file("inline/pushdown_detected.test", text).expect("parse");
    assert!(
        run.cases[0].passed,
        "the pushed-down filter must keep `b` out of the detected set: {}",
        run.cases[0].detail
    );
}

/// AC5. The one leg `compile_for_corpus` cannot reach is REFUSED, not
/// answered wrongly: `VariantArena::build` compiles the variants COMMON
/// pipeline itself, with the pushdown active.
///
/// *Break:* delete the refusal in `eval_node`'s `Variants` arm — the
/// case answers with unfiltered rows, the `eval_fail` verb sees a
/// success, RED.
#[test]
fn a_pushed_down_common_line_filter_in_variants_is_refused() {
    let text = "load\n  {app=\"v\", service_name=\"pd-variants\"}\n\
                \t0s   alpha foo bravo\n\t5s   alpha bar bravo\n\n\
                eval_fail instant at 60s variants(count_over_time({service_name=\"pd-variants\"}[1m])) \
                of ({service_name=\"pd-variants\"} |= \"foo\" [1m])\n\
                \tmsg: issue #278\n";
    let run = run_file("inline/pushdown_variants.test", text).expect("parse");
    assert!(
        run.cases[0].passed,
        "a pushed common-side filter must be refused by name, not answered: {}",
        run.cases[0].detail
    );
}

/// Every corpus `eval*` directive, planned, with the number of line
/// filters the planner PUSHES counted across every plan shape.
fn pushed_filter_count(query: &str) -> usize {
    use pulsus_read::logql::{
        Direction, MetricNode, MetricNodeScc, Plan, PlanCtx, QueryParams, QuerySpec, plan,
    };
    let Ok(expr) = pulsus_logql::parse(query) else {
        return 0;
    };
    let params = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let ctx = PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    };
    match plan(&expr, &params, &ctx) {
        Ok(Plan::Streams(sp)) => sp.line_filters.len(),
        Ok(Plan::Metric(mp)) => mp.extra_predicates.len(),
        Ok(Plan::MetricBinary(node)) => {
            let mut nodes = Vec::new();
            pulsus_logql::walk::postorder_into::<MetricNodeScc>(&node, &mut nodes);
            nodes
                .iter()
                .map(|n| match n {
                    MetricNode::Leaf(mp) => mp.extra_predicates.len(),
                    MetricNode::Variants { scan, .. } => scan.extra_predicates.len(),
                    _ => 0,
                })
                .sum()
        }
        Err(_) => 0,
    }
}

/// Every `eval*` directive in the corpus, with its file and line — read
/// through the corpus's OWN parser, so this cannot drift from what the
/// replay actually runs.
fn corpus_eval_directives() -> Vec<(String, usize, String)> {
    let mut out = Vec::new();
    for name in &corpus_files() {
        let path = driver::corpus_dir().join(name);
        let text = driver::read_file(&path);
        for c in driver::runner::parse_file(name, &text).unwrap_or_else(|e| panic!("{e}")) {
            if let driver::runner::Command::Eval(e) = c {
                out.push((name.clone(), e.line, e.query));
            }
        }
    }
    out
}

/// AC6. **The corpus is no longer blind: it contains rows that take the
/// path.** Without this, reverting the runner fix would be undetectable
/// — a corpus with no pushed-down filter in it passes either way, which
/// is exactly the state this issue found.
///
/// *Break:* delete `b26_line_filter_pushdown.test` — RED here, and RED
/// on `corpus_dir_is_populated`'s floor too.
#[test]
fn the_corpus_exercises_pushed_down_line_filters() {
    let rows: Vec<(String, usize)> = corpus_eval_directives()
        .into_iter()
        .filter_map(|(f, l, q)| (pushed_filter_count(&q) > 0).then_some((f, l)))
        .collect();
    assert!(
        rows.len() >= 8,
        "only {} corpus directive(s) plan to a PUSHED-DOWN line filter, below the eight \
         issue #278 added. The runner's ability to execute that path is only worth having \
         if the corpus takes it: {rows:?}",
        rows.len()
    );
}

/// AC8. **No new row is vacuous.** A row whose filter excludes nothing
/// is precisely the row that passed on the blind path and measured
/// nothing, so it is asserted mechanically rather than left to authoring
/// care.
///
/// **What it compares, exactly:** the row's EXPECTATION against the
/// sample count its `load` has in force. It does not itself run the
/// filter. That is the whole property only in combination with
/// [`corpus_is_fully_green_and_exercises_every_directive`], which pins
/// expectation == what the pipeline actually produces; together they give
/// "the filter drops at least one loaded line". Measured, and the reason
/// the pairing is stated rather than assumed: widening a row's filter to
/// match everything WITHOUT re-capturing it reddens the green run and not
/// this test, while widening it and re-capturing — what an author adding
/// a vacuous row would commit — reddens this one and not the green run.
///
/// *Break:* replace a P-row's filter with one that matches every loaded
/// line and record all three lines as expected — RED, naming the row.
#[test]
fn every_pushdown_corpus_row_excludes_at_least_one_loaded_line() {
    let name = "b26_line_filter_pushdown.test";
    let text = driver::read_file(&driver::corpus_dir().join(name));
    let cmds = driver::runner::parse_file(name, &text).unwrap_or_else(|e| panic!("{e}"));
    let mut loaded = 0usize;
    let mut checked = 0usize;
    for c in &cmds {
        match c {
            driver::runner::Command::Clear => loaded = 0,
            driver::runner::Command::Load(specs) => {
                loaded += specs.iter().map(|s| s.samples.len()).sum::<usize>();
            }
            driver::runner::Command::Eval(e) => {
                assert!(
                    e.expected.len() < loaded,
                    "{name}:{}: `{}` expects {} of {loaded} loaded lines — it excludes none, so \
                     it would pass with the filter dropped entirely and measures nothing",
                    e.line,
                    e.query,
                    e.expected.len()
                );
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 8, "expected the eight P-rows, checked {checked}");
}

/// AC12. **No artefact still claims the runner is pushdown-blind.**
///
/// A tripwire for the EXACT phrase that existed before this issue, over
/// a file set derived by walking `tests/logqltest/` recursively plus the
/// two documents that carried it.
///
/// **What it cannot see:** a paraphrase, and any file outside that walk.
/// It is not a proof that the surrounding prose is now true — the
/// derived sweep in the implementation notes is what did that, and a
/// reviewer read it. This only stops the literal coming back.
///
/// *Break:* restore the phrase in `docs/api.md` — RED.
#[test]
fn no_artefact_claims_the_runner_is_pushdown_blind() {
    // Assembled at run time so THIS file does not contain the needle and
    // need an exemption from its own check.
    let needle = format!("{} {} {}", "pushdown", "blind", "spot");
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {dir:?}: {e}")) {
            let p = e.expect("dir entry").path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push(p);
            }
        }
    }
    walk(&root.join("tests/logqltest"), &mut files);
    let repo = root
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crates/pulsus-read");
    files.push(repo.join("docs/api.md"));
    files.push(repo.join("docs/benchmarks/logs-differential-ledger.md"));
    assert!(
        files.len() > 40,
        "the walk found only {} files — it is looking in the wrong place",
        files.len()
    );
    let offenders: Vec<String> = files
        .iter()
        .filter(|p| std::fs::read_to_string(p).is_ok_and(|t| t.contains(needle.as_str())))
        .map(|p| p.display().to_string())
        .collect();
    assert!(
        offenders.is_empty(),
        "these artefacts still describe the corpus runner as blind to the pushdown path, \
         which issue #278 closed: {offenders:?}"
    );
}

/// AC15. **The "not luck" evidence is in the tree, not only on the
/// issue.** Zero committed rows moved when the runner was fixed, and a
/// reader meeting that figure alone concludes the defect was harmless.
/// It was the opposite: three committed files document authors inserting
/// `| decolorize` or a label filter SPECIFICALLY to defeat the pushdown,
/// and the new corpus file's header names all three.
///
/// **What it cannot see:** whether the sentence around those names is
/// true. It pins that the pointers exist; a reviewer reads the sentence.
///
/// *Break:* delete a file name from the header — RED.
#[test]
fn the_pushdown_corpus_names_the_rows_written_around_the_defect() {
    let text = driver::read_file(&driver::corpus_dir().join("b26_line_filter_pushdown.test"));
    for named in [
        "b24_string_escapes.test",
        "b1_parsers_filters.test",
        "b13_variants.test",
    ] {
        assert!(
            text.contains(named),
            "b26's header must name {named} — the file that documents an author working \
             around the pushdown blindness. Without those pointers the measured `zero rows \
             affected` reads as `this issue was harmless`."
        );
    }
}

// ---------------------------------------------------------------------
// Issue #397 — the corpus finding, as a CHECKED claim rather than prose.
//
// The issue predicted that a committed corpus row would have to move,
// because #221 pinned "the variant pipeline is dead syntax" as the rule.
// It does not move: every wrapped variant committed BEFORE #397 has an
// EMPTY pipeline, so no expected value depends on the rule that changed.
// An untouched corpus therefore means ABSENCE of coverage, not coverage
// — and this test is what keeps that reading from having to be taken on
// trust. It fails if a wrapped-variant-with-a-pipeline query appears in
// any of the three artefacts outside the section that was captured for
// this issue.
// ---------------------------------------------------------------------

/// The three artefacts read for the claim, by name so the next reader
/// can check the list rather than infer it.
const PRE_397_ARTEFACTS: &[&str] = &[
    "tests/logqltest/corpus/b13_variants.test",
    "tests/logql_metric_agg_golden.rs",
    "tests/golden/plan_build_differential.txt",
];

/// Every `variants(...)`-bearing query text in the three artefacts,
/// paired with its artefact. Extraction is deliberately crude — any
/// line holding `variants(` yields the substring from the first
/// `variants(` to the end of its enclosing literal/line — because a
/// missed query can only WEAKEN this test, while the "at least one
/// wrapped-with-pipeline query is found" assertion below proves the
/// extractor still finds anything at all.
fn variants_queries() -> Vec<(&'static str, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for artefact in PRE_397_ARTEFACTS {
        let text = std::fs::read_to_string(root.join(artefact))
            .unwrap_or_else(|e| panic!("read {artefact}: {e}"));
        for line in text.lines() {
            let Some(start) = line.find("variants(") else {
                continue;
            };
            let rest = &line[start..];
            // Trim the Rust raw-string / golden-header tails so the
            // parser sees a query and nothing else.
            let query = rest
                .split("\"#,")
                .next()
                .unwrap_or(rest)
                .trim_end_matches(&['"', '#', ',', ' '][..]);
            out.push((*artefact, query.to_string()));
        }
    }
    out
}

/// Whether `query` plans to a variants node holding at least one variant
/// that is WRAPPED in a vector aggregation AND carries a non-empty
/// pipeline of its own — the exact shape whose meaning issue #397
/// changed. Unparseable or non-variants queries answer `false`.
fn has_a_wrapped_variant_with_a_pipeline(query: &str) -> bool {
    use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};
    let Ok(expr) = pulsus_logql::parse(query) else {
        return false;
    };
    let params = QueryParams {
        spec: QuerySpec::Instant {
            at_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    let ctx = PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    };
    match plan(&expr, &params, &ctx) {
        Ok(Plan::MetricBinary(node)) => {
            let mut nodes = Vec::new();
            pulsus_logql::walk::postorder_into::<pulsus_read::logql::MetricNodeScc>(
                &node, &mut nodes,
            );
            nodes.iter().any(|n| match n {
                pulsus_read::logql::MetricNode::Variants { variants, .. } => variants
                    .iter()
                    .any(|s| !s.vector_aggs().is_empty() && !s.client().pipeline.is_empty()),
                _ => false,
            })
        }
        _ => false,
    }
}

/// Every wrapped-variant-with-a-pipeline query in the three artefacts is
/// one the `W` section of `b13_variants.test` captured for issue #397 —
/// the hermetic goldens added alongside it reuse those exact query
/// strings. So nothing committed BEFORE #397 depended on the rule it
/// changed, and the untouched expected values elsewhere are absence of
/// coverage rather than coverage.
#[test]
fn only_the_397_section_has_a_wrapped_variant_with_a_pipeline() {
    // The W section's queries, taken from the file itself so the two
    // halves cannot drift.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let b13 = std::fs::read_to_string(root.join(PRE_397_ARTEFACTS[0])).expect("read b13");
    let w_section = b13
        .split_once("# --- Issue #397")
        .expect("b13 carries the #397 section")
        .1;

    let mut outside = Vec::new();
    let mut inside = 0usize;
    for (artefact, query) in variants_queries() {
        if !has_a_wrapped_variant_with_a_pipeline(&query) {
            continue;
        }
        if w_section.contains(query.as_str()) {
            inside += 1;
        } else {
            outside.push(format!("{artefact}: {query}"));
        }
    }
    assert!(
        outside.is_empty(),
        "a wrapped variant with a live pipeline appears that issue #397's own corpus \
         section did not capture. Its expected value depends on the rule #397 changed, so \
         it must be captured against the pinned container rather than left standing:\n  {}",
        outside.join("\n  ")
    );
    // Finder validation: the extractor above must actually be finding
    // this shape, or the emptiness of `outside` means nothing.
    assert!(
        inside > 0,
        "the extractor found no wrapped-variant-with-a-pipeline query at all, so the \
         assertion above is vacuous — fix the extraction, not the assertion"
    );
}
