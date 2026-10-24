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

use pulsus_read::traces::metrics_plan::{MetricsCtx, MetricsParams};
use pulsus_read::{SearchParams, plan_search_request};
use pulsus_read::{StatementTables, plan_trace_metrics_routed};
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

/// The engine that answers a query: `refused` when the planner refuses
/// it, else `new`, the statement. A metrics query is planned by `plan_trace_metrics_routed` (issue #596
/// part 1) over the search's window, step 60 s.
fn side_of(query: &pulsus_traceql::Query, route: &str) -> &'static str {
    const START: i64 = 1_790_000_000_000_000_000;
    if route == "metrics" {
        let params = MetricsParams {
            start_ns: START,
            end_ns: START + 3_600_000_000_000,
            step_ms: 60_000,
            exemplars: None,
            floor_ns: i64::MIN,
        };
        let ctx = MetricsCtx {
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        };
        let tables = StatementTables {
            spans: "spans",
            error_spans: "error_spans",
            resources: "resources",
            traces: "traces",
            indexed: &[],
            tenant: empty_term(),
        };
        return match plan_trace_metrics_routed(query, &params, &ctx, &tables) {
            Err(_) => "refused",
            Ok(_) => "new",
        };
    }
    let params = SearchParams {
        start_ns: START,
        end_ns: START + 3_600_000_000_000,
        limit: 20,
        spss: 3,
    };
    let tables = StatementTables {
        spans: "spans",
        error_spans: "error_spans",
        resources: "resources",
        traces: "traces",
        indexed: &[],
        tenant: empty_term(),
    };
    match plan_search_request(query, &params, &tables, 64) {
        Err(_) => "refused",
        Ok(_) => "new",
    }
}

/// The side a task's queries take: task 9's are the search statement's,
/// task 14's and 15's the metrics statement's (issue #596 part 1, issue
/// #597).
fn side_of_task(task: &str) -> &'static str {
    match task {
        "9" | "14" | "15" => "new",
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
            sides.get("refused").copied().unwrap_or(0)
        ),
        (139, 2),
        "new and refused"
    );
}

/// The six tables of the previous trace store, each written as two
/// fragments so this file does not name them and match itself. Their
/// views (`<name>_mv`) and wrappers (`<name>_dist`) contain these names.
const OLD_TRACE_TABLES: [&str; 6] = [
    concat!("trace_", "spans"),
    concat!("trace_", "attrs_idx"),
    concat!("trace_", "tag_catalog"),
    concat!("trace_", "edges"),
    concat!("trace_", "recent"),
    concat!("trace_", "error_spans"),
];

/// Issue #602: no tracked file outside `docs/` names a table of the
/// previous trace store. `docs/` is excluded because it carries the
/// history. The allowlist is empty.
#[test]
fn no_code_names_an_old_trace_table() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let out = std::process::Command::new("git")
        .args(["ls-files", "-z", "--", ":!docs/"])
        .current_dir(&root)
        .output()
        .expect("run git ls-files");
    assert!(out.status.success(), "git ls-files failed: {out:?}");
    let files: Vec<String> = out
        .stdout
        .split(|b| *b == 0)
        .filter(|f| !f.is_empty())
        .map(|f| String::from_utf8_lossy(f).into_owned())
        .collect();
    let this = "crates/pulsus-read/tests/traces_route_inventory.rs";
    assert!(
        files.iter().any(|f| f == this),
        "the sweep searched nothing: the file list does not hold {this} ({} files)",
        files.len()
    );
    let mut hits = Vec::new();
    for file in &files {
        let bytes = match std::fs::read(root.join(file)) {
            Ok(b) => b,
            // Tracked, but deleted in the working tree.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => panic!("read {file}: {e}"),
        };
        let text = String::from_utf8_lossy(&bytes);
        for (n, line) in text.lines().enumerate() {
            if OLD_TRACE_TABLES.iter().any(|t| line.contains(t)) {
                hits.push(format!("{file}:{}: {line}", n + 1));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "{} line(s) outside docs/ name an old trace table:\n{}",
        hits.len(),
        hits.join("\n")
    );
}

/// The empty tenant (issue #738): every fixture here is pushed with no
/// `X-Scope-OrgID`.
#[allow(dead_code)]
fn empty_tenant() -> pulsus_model::Tenant {
    pulsus_model::Tenant::from_header(None, false).expect("the empty tenant")
}

/// [`empty_tenant`]'s term, for a statement built outside an engine.
#[allow(dead_code)]
fn empty_term() -> &'static pulsus_read::traces::tenant_scope::TenantTerm {
    static TERM: std::sync::LazyLock<pulsus_read::traces::tenant_scope::TenantTerm> =
        std::sync::LazyLock::new(|| {
            pulsus_read::traces::tenant_scope::TenantTerm::of(&empty_tenant())
        });
    &TERM
}
