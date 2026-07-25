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
