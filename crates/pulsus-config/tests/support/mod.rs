//! Test-only helpers shared by the integration tests in this directory.
//! Not compiled as its own test binary — cargo only treats files placed
//! directly under `tests/` as separate integration-test crates, so a
//! `tests/support/mod.rs` submodule is invisible to the test harness except
//! via `mod support;`.
//!
//! `std::env` is process-global, so tests that set environment variables
//! must serialize with every other test in the same binary that also
//! touches the environment (different `tests/*.rs` files are already
//! separate processes and don't interfere with each other).
//!
//! Each `tests/*.rs` file compiles this module independently (as its own
//! integration-test crate) and uses a different subset of these helpers;
//! `dead_code` is allowed here rather than duplicating helpers per file.
#![allow(dead_code)]

use std::sync::{Mutex, MutexGuard, OnceLock};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Acquires the process-wide environment lock for this test binary. Hold
/// the returned guard for the entire duration of a test that reads or
/// writes environment variables.
pub fn lock_env() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Clears every documented pulsus-config environment variable, so a test
/// starts from a clean slate regardless of what an earlier test (or the
/// host shell) left behind. Caller must hold `lock_env()`.
pub fn clear_all() {
    for var in pulsus_config::ALL_ENV_VARS {
        // SAFETY: `std::env::remove_var` is only unsound if called
        // concurrently with another thread reading/writing the
        // environment; the caller holds `lock_env()`, which every test in
        // this binary that touches the environment also acquires.
        unsafe { std::env::remove_var(var) };
    }
}

/// Sets a single environment variable. Caller must hold `lock_env()`.
pub fn set(var: &str, value: &str) {
    // SAFETY: see `clear_all`.
    unsafe { std::env::set_var(var, value) };
}

/// Writes `contents` to a uniquely-named file under the OS temp directory
/// and returns its path. The caller is responsible for removing it.
pub fn write_temp_yaml(name: &str, contents: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pulsus-config-test-{name}-{}-{:?}.yaml",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::write(&path, contents).expect("write temp yaml fixture");
    path
}

/// Reads a file by its path from the repository root, panicking with that
/// path when it cannot.
pub fn repo_file(rel: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {rel}: {e}"))
}

fn join_path(prefix: &str, key: &serde_norway::Value) -> String {
    let key = key
        .as_str()
        .unwrap_or_else(|| panic!("a non-string key under {prefix:?}"));
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}.{key}")
    }
}

/// Every key path of a YAML value, dotted (`reader.cache_ttl`). A mapping
/// adds its own keys and recurses; anything else, a sequence included, is a
/// leaf.
pub fn key_paths(v: &serde_norway::Value) -> std::collections::BTreeSet<String> {
    fn walk(prefix: &str, v: &serde_norway::Value, out: &mut std::collections::BTreeSet<String>) {
        if let Some(m) = v.as_mapping() {
            for (k, child) in m {
                let path = join_path(prefix, k);
                out.insert(path.clone());
                walk(&path, child, out);
            }
        }
    }
    let mut out = std::collections::BTreeSet::new();
    walk("", v, &mut out);
    out
}

/// The model's key paths: those of `Config::default()` serialised. No field
/// is skipped, flattened or renamed, so this is the field set.
pub fn model_key_paths() -> std::collections::BTreeSet<String> {
    key_paths(
        &serde_norway::to_value(pulsus_config::Config::default()).expect("serialise the defaults"),
    )
}

/// Every leaf of a YAML value with its dotted path. A non-empty mapping
/// recurses; anything else, a sequence or an empty mapping included, is a
/// leaf.
pub fn leaf_values(
    v: &serde_norway::Value,
) -> std::collections::BTreeMap<String, serde_norway::Value> {
    fn walk(
        prefix: &str,
        v: &serde_norway::Value,
        out: &mut std::collections::BTreeMap<String, serde_norway::Value>,
    ) {
        match v.as_mapping() {
            Some(m) if !m.is_empty() => {
                for (k, child) in m {
                    walk(&join_path(prefix, k), child, out);
                }
            }
            _ => {
                out.insert(prefix.to_string(), v.clone());
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    walk("", v, &mut out);
    out
}

/// One assertion that `doc` has exactly the model's key paths. Its message
/// names `what`, then the count and list of model paths `doc` lacks, then
/// the count and list of `doc` paths the model lacks.
pub fn assert_names_every_model_key(what: &str, doc: &serde_norway::Value) {
    let model = model_key_paths();
    let doc = key_paths(doc);
    let missing: Vec<&String> = model.difference(&doc).collect();
    let extra: Vec<&String> = doc.difference(&model).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "{what}: missing {} model keys {missing:?}; {} keys the model does not have {extra:?}",
        missing.len(),
        extra.len()
    );
}

/// The rows of `docs/configuration.md`'s "Grammar boundaries" table (issue
/// #746 part A): (accepted, grammar, values), each value one code span taken
/// exactly, surrounding spaces included.
pub fn grammar_boundaries() -> Vec<(bool, String, Vec<String>)> {
    let text = repo_file("docs/configuration.md");
    let start = text
        .find("### Grammar boundaries")
        .unwrap_or_else(|| panic!("no Grammar boundaries table"));
    let section = &text[start + 1..];
    let section = &section[..section.find("\n#").unwrap_or(section.len())];
    let mut rows = Vec::new();
    for line in section.lines() {
        let Some((accepted, rest)) = line
            .strip_prefix("| accepted | ")
            .map(|r| (true, r))
            .or_else(|| line.strip_prefix("| refused | ").map(|r| (false, r)))
        else {
            continue;
        };
        let (grammar, cell) = rest.split_once(" | ").expect("three cells");
        let mut cell = cell.strip_suffix(" |").expect("the row's end");
        let mut values = Vec::new();
        loop {
            let body = cell
                .strip_prefix('`')
                .unwrap_or_else(|| panic!("a code span: {line}"));
            let end = body.find('`').expect("a closed code span");
            values.push(body[..end].to_string());
            cell = &body[end + 1..];
            if cell.is_empty() {
                break;
            }
            cell = cell
                .strip_prefix(", ")
                .unwrap_or_else(|| panic!("values are separated by ', ': {line}"));
        }
        rows.push((accepted, grammar.to_string(), values));
    }
    let kinds: Vec<(bool, &str)> = rows.iter().map(|(a, g, _)| (*a, g.as_str())).collect();
    assert_eq!(
        kinds,
        [
            (true, "dedup window"),
            (false, "dedup window"),
            (true, "log rollup resolution"),
            (false, "log rollup resolution"),
        ],
        "the table's rows"
    );
    rows
}

/// A log rollup resolution from the table as a `Duration`, worked out here
/// rather than by the parser under test.
pub fn resolution_of(v: &str) -> std::time::Duration {
    use std::time::Duration;
    let digits: String = v.chars().take_while(char::is_ascii_digit).collect();
    let n: u64 = digits.parse().expect("digits");
    match &v[digits.len()..] {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        "d" => Duration::from_secs(n * 86_400),
        "w" => Duration::from_secs(n * 604_800),
        unit => panic!("unit {unit}"),
    }
}
