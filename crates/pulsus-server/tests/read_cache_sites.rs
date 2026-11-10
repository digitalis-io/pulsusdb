//! Issue #778 (#803's rule): the source check that the range handler gives
//! the read cache the request attempt's own engine.
//!
//! `answer_range_on` stores a fill only while the engine it is given has no
//! server failure. That holds for the request only when the engine is the
//! one `with_engine` built for the attempt, so this checks, over every
//! `.rs` file under `src/`, each read up to its first `#[cfg(test)]` line:
//!
//! * `read_cache::answer_range(` (the closure form) occurs nowhere;
//! * `read_cache::answer_range_on(` occurs exactly once, in
//!   `src/prom_api/handlers.rs`, as `answer_range_on(&cache, &engine,`;
//! * the nearest `with_engine(` before it opens a closure `|engine|`, and
//!   no `fn` stands between the two.
//!
//! What it cannot see: a rebinding of `engine` inside the closure, or an
//! alias of the function. It is a substring check, not a parse.

use std::fs;
use std::path::{Path, PathBuf};

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// `path`'s text up to its first `#[cfg(test)]` line.
fn production_text(path: &Path) -> String {
    let text = fs::read_to_string(path).expect("read a source file");
    match text.find("#[cfg(test)]") {
        Some(i) => text[..i].to_string(),
        None => text,
    }
}

#[test]
fn the_range_handler_gives_the_cache_the_attempts_engine() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    files.sort();
    let mut on_calls = Vec::new();
    for f in &files {
        let text = production_text(f);
        assert!(
            !text.contains("read_cache::answer_range("),
            "{}: the closure form of the cache, not answer_range_on",
            f.display()
        );
        for (i, _) in text.match_indices("read_cache::answer_range_on(") {
            on_calls.push((f.clone(), text.clone(), i));
        }
    }
    assert_eq!(
        on_calls.len(),
        1,
        "answer_range_on call sites: {}",
        on_calls.len()
    );
    let (file, text, at) = &on_calls[0];
    assert!(
        file.ends_with("src/prom_api/handlers.rs"),
        "answer_range_on is called from {}",
        file.display()
    );
    assert!(
        text[*at..].starts_with("read_cache::answer_range_on(&cache, &engine,"),
        "answer_range_on is not given `&engine`: {}",
        &text[*at..(*at + 80).min(text.len())]
    );
    let before = &text[..*at];
    let open = before
        .rfind("with_engine(")
        .expect("a with_engine before the call");
    assert!(
        !before[open..].contains("fn "),
        "a fn between with_engine and answer_range_on"
    );
    let head = &before[open..];
    let closure = head.find('|').expect("a closure in with_engine");
    assert!(
        head[closure..].starts_with("|engine|"),
        "the with_engine closure's parameter is not `engine`: {}",
        &head[closure..(closure + 20).min(head.len())]
    );
}
