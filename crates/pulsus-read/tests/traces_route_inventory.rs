//! Issue #591 part 3's committed route inventory (section 4 of its
//! design): one row per corpus query under
//! `crates/pulsus-traceql/tests/corpus/accept/` and `grafana/` — its name,
//! the route it takes, the engine that answers it, and the task of
//! `docs/TraceQL/server-implementation.md` that serves it.
//!
//! `task` is committed and derived from the design, not from the code:
//! §3.2's construct a query reaches, mapped to the issue that serves it.
//! `route` and `side` are computed here, from the parser, the planner and
//! the fork's coverage function, and must equal the file. So a corpus
//! query added without a row fails, and so does a query moving sides
//! without its `task` changing.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use pulsus_read::SpanFilterCtx;
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};
use pulsus_read::traces::spans::search::plan_statement;
use pulsus_traceql::PipelineStage;

fn corpus_dir(sub: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../pulsus-traceql/tests/corpus")
        .join(sub)
}

/// Every `.traceql` under `accept/` and `grafana/`: its name and its text.
fn corpus() -> Vec<(String, String)> {
    let mut out = Vec::new();
    for sub in ["accept", "grafana"] {
        let dir = corpus_dir(sub);
        for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("read {dir:?}: {e}")) {
            let path = entry.expect("a directory entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("traceql") {
                continue;
            }
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("a UTF-8 name")
                .to_string();
            let text =
                std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
            out.push((name, text));
        }
    }
    out.sort();
    out
}

/// The committed rows: `(name, route, side, task)`.
fn inventory() -> Vec<(String, String, String, String)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/traces_route_inventory.tsv");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let mut lines = text.lines();
    assert_eq!(lines.next(), Some("name\troute\tside\ttask"), "the header");
    lines
        .map(|line| {
            let cols: Vec<&str> = line.split('\t').collect();
            assert_eq!(cols.len(), 4, "four columns: {line:?}");
            (
                cols[0].to_string(),
                cols[1].to_string(),
                cols[2].to_string(),
                cols[3].to_string(),
            )
        })
        .collect()
}

/// `metrics` when the pipeline holds a metrics stage, else `search`.
fn route_of(query: &pulsus_traceql::Query) -> &'static str {
    let metrics = query.pipeline.iter().any(|stage| {
        matches!(
            stage,
            PipelineStage::Metric(_)
                | PipelineStage::MetricSecondStage(_)
                | PipelineStage::Compare { .. }
        )
    });
    if metrics { "metrics" } else { "search" }
}

/// The engine that answers a search: `refused` when the planner refuses
/// it, `new` when the fork's coverage function gives a statement, else
/// `old`. A metrics query is today's engine's.
fn side_of(query: &pulsus_traceql::Query, route: &str) -> &'static str {
    if route == "metrics" {
        return "old";
    }
    const START: i64 = 1_790_000_000_000_000_000;
    let params = SearchParams {
        start_ns: START,
        end_ns: START + 3_600_000_000_000,
        limit: 20,
        spss: 3,
    };
    let ctx = SearchCtx {
        filter: SpanFilterCtx {
            spans_table: "trace_spans",
            attrs_table: "trace_attrs_idx",
        },
        recent_table: "trace_recent",
        errors_table: "trace_error_spans",
        max_candidates: 100_000,
        max_series: 1_000,
        distributed: false,
    };
    match plan_search(query, &params, &ctx) {
        Err(_) => "refused",
        Ok(plan) => match plan_statement(&plan, "spans", "traces", "resources", 64, &[]) {
            Some(_) => "new",
            None => "old",
        },
    }
}

/// The side a task's queries take: task 9's are the search statement's,
/// tasks 10 to 15's today's engine's.
fn side_of_task(task: &str) -> &'static str {
    match task {
        "9" => "new",
        "10" | "11" | "12" | "14" | "15" => "old",
        "refused" => "refused",
        other => panic!("an unknown task {other:?}"),
    }
}

/// Section 6.1: the inventory names every corpus query once, and each
/// row's route and side are the computed ones and agree with its task.
#[test]
fn the_inventory_is_complete_and_exact() {
    let corpus = corpus();
    let rows = inventory();
    let corpus_names: BTreeSet<&str> = corpus.iter().map(|(n, _)| n.as_str()).collect();
    let row_names: BTreeSet<&str> = rows.iter().map(|(n, ..)| n.as_str()).collect();
    assert_eq!(row_names.len(), rows.len(), "a name appears once");
    assert_eq!(
        row_names, corpus_names,
        "the inventory's names are the corpus files"
    );
    assert_eq!(rows.len(), 141, "141 corpus queries");

    let mut wrong = Vec::new();
    let mut sides = std::collections::BTreeMap::<String, usize>::new();
    for (name, route, side, task) in &rows {
        let text = &corpus
            .iter()
            .find(|(n, _)| n == name)
            .expect("checked above")
            .1;
        let query =
            pulsus_traceql::parse(text).unwrap_or_else(|e| panic!("{name} must parse: {e:?}"));
        let computed_route = route_of(&query);
        let computed_side = side_of(&query, computed_route);
        if route != computed_route {
            wrong.push(format!(
                "{name}: route {route} in the file, {computed_route} computed"
            ));
        }
        if side != computed_side {
            wrong.push(format!(
                "{name}: side {side} in the file, {computed_side} computed"
            ));
        }
        if side != side_of_task(task) {
            wrong.push(format!(
                "{name}: side {side} disagrees with task {task} ({})",
                side_of_task(task)
            ));
        }
        *sides.entry(computed_side.to_string()).or_default() += 1;
    }
    assert!(
        wrong.is_empty(),
        "{} row(s) wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
    eprintln!("computed sides: {sides:?}");
    assert_eq!(
        (
            sides.get("new").copied().unwrap_or(0),
            sides.get("old").copied().unwrap_or(0),
            sides.get("refused").copied().unwrap_or(0)
        ),
        (119, 19, 3),
        "new, old and refused"
    );
}
