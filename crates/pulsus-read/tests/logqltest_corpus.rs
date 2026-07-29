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
/// — parallel-safe. The #29 F1 anti-drop guarantee holds by construction: the
/// replay runs whatever is on disk, so a file cannot silently drop out.
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
/// empty checkout is caught); at least the Batch 0 seed files are present.
#[test]
fn corpus_dir_is_populated() {
    let files = corpus_files();
    assert!(
        files.len() >= 6,
        "expected at least the 6 seed .test files, found {files:?}"
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
/// validation must reject NOTHING the corpus accepts. The corpus runner
/// never routes LOG queries through `plan()` (its documented pushdown
/// blind spot, #278), so this walks every `eval`/`eval_ordered` log
/// query in the corpus through the real planner and requires `Ok` —
/// zero false rejections from the pushed-down line-filter/matcher
/// validation. (Metric queries already plan inside the green corpus
/// run; `sql_snapshots.rs`/`explain_indexes.rs` prove the emitted SQL
/// is byte-identical.)
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
