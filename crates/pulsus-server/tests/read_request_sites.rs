//! Issue #803 (E1): the source check that stands in for enforcement of
//! "an HTTP read handler builds its engine only through `with_engine`".
//!
//! A read request is one engine's client, bound to one server when it is
//! built; `with_engine` runs the request once more on another server when
//! that one fails, and saves only the accepted run's state. A handler that
//! reached the pool, or built an engine, itself would escape both.
//!
//! What it checks, over every `.rs` file under `src/logs_api`,
//! `src/prom_api` and `src/traces_api` except `engine.rs` and `*_tests.rs`,
//! each read up to its first `#[cfg(test)]` line:
//!
//! * none contains `.pool` followed by a character that cannot continue an
//!   identifier (so `.pool_size` does not match), `from_shared_pool`,
//!   `LogQlEngine::new`, `MetricsEngine::new`, `TraceEngine::new`,
//!   `logql_engine(`, `metrics_engine(` or `trace_engine(`;
//! * each of the three `engine.rs` exists and calls `engine_for(` only
//!   inside `with_engine` and `tail_engine` (the nearest `fn` before each
//!   call);
//! * `tail_engine(` occurs in no file of `src/logs_api` other than
//!   `engine.rs` and `tail.rs`.
//!
//! What it cannot see: an alias of the pool or of a constructor; a closure
//! passed to `with_engine` that returns its `Arc` engine as the result, so
//! that the engine is used after the request; and code outside those
//! directories. It is a substring check, not a parse.

use std::fs;
use std::path::{Path, PathBuf};

const DIRS: [&str; 3] = ["src/logs_api", "src/prom_api", "src/traces_api"];

const FORBIDDEN: [&str; 7] = [
    "from_shared_pool",
    "LogQlEngine::new",
    "MetricsEngine::new",
    "TraceEngine::new",
    "logql_engine(",
    "metrics_engine(",
    "trace_engine(",
];

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rs_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in fs::read_dir(&d).unwrap_or_else(|e| panic!("read {}: {e}", d.display())) {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|x| x == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// The file's text up to its first `#[cfg(test)]` line.
fn before_tests(path: &Path) -> String {
    let src = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut out = String::new();
    for line in src.lines() {
        if line.trim_start().starts_with("#[cfg(test)]") {
            break;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// `(line, what)` for every forbidden spelling in `text`.
fn forbidden_hits(text: &str) -> Vec<(usize, String)> {
    let mut hits = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let mut from = 0;
        while let Some(i) = line[from..].find(".pool") {
            let at = from + i;
            let next = line[at + ".pool".len()..].chars().next();
            if next.is_none_or(|c| !is_ident_char(c)) {
                hits.push((n + 1, ".pool".to_string()));
            }
            from = at + ".pool".len();
        }
        for f in FORBIDDEN {
            if line.contains(f) {
                hits.push((n + 1, f.to_string()));
            }
        }
    }
    hits
}

/// The name of the nearest `fn` before byte `at` of `text`.
fn enclosing_fn(text: &str, at: usize) -> Option<String> {
    let head = &text[..at];
    let i = head.rfind("fn ")?;
    let name: String = head[i + 3..]
        .chars()
        .take_while(|c| is_ident_char(*c))
        .collect();
    Some(name)
}

#[test]
fn api_handlers_build_engines_only_through_with_engine() {
    let root = crate_root();
    let mut problems = Vec::new();

    for dir in DIRS {
        let dir_path = root.join(dir);
        for path in rs_files(&dir_path) {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name == "engine.rs" || name.ends_with("_tests.rs") {
                continue;
            }
            let rel = path
                .strip_prefix(&root)
                .unwrap_or(&path)
                .display()
                .to_string();
            let text = before_tests(&path);
            for (line, what) in forbidden_hits(&text) {
                problems.push(format!("{rel}:{line}: `{what}`"));
            }
            if dir == "src/logs_api" && name != "tail.rs" && text.contains("tail_engine(") {
                problems.push(format!("{rel}: calls `tail_engine(` outside tail.rs"));
            }
        }

        let engine = dir_path.join("engine.rs");
        if !engine.is_file() {
            problems.push(format!("{dir}/engine.rs does not exist"));
            continue;
        }
        let text = before_tests(&engine);
        let mut from = 0;
        while let Some(i) = text[from..].find("engine_for(") {
            let at = from + i;
            from = at + "engine_for(".len();
            if text[..at].ends_with("fn ") {
                continue;
            }
            match enclosing_fn(&text, at).as_deref() {
                Some("with_engine" | "tail_engine") => {}
                other => problems.push(format!(
                    "{dir}/engine.rs calls `engine_for(` inside {other:?}, not with_engine or \
                     tail_engine"
                )),
            }
        }
    }

    assert!(
        problems.is_empty(),
        "read handlers must build engines only through with_engine:\n{}",
        problems.join("\n")
    );
}
