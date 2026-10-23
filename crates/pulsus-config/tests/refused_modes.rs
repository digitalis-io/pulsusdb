//! Issue #506: no tracked file names a process mode the binary refuses.
//!
//! **The refused names come from the history of the mode parser.** Every
//! spelling `Mode`'s parser has ever accepted is printed by
//!
//! ```text
//! git log -p -G'=> Ok\(Mode::' -- crates/pulsus-config/src/model.rs
//! ```
//!
//! which gives `all`, `init`, `reader` and `writer`. Of those, `init`
//! (removed by #619) is the one refused today. `REMOVED_MODES` is that list,
//! and the test first asserts each member is refused, so a name that comes
//! back fails until it leaves the list.
//!
//! **A mention is any of these forms**, on one line, with `R` a removed name
//! and `A` an accepted one:
//!
//! * after `--mode`, with any quoting (`--mode init`);
//! * after `PULSUS_MODE` (`PULSUS_MODE=init`);
//! * after a `mode` key or parameter (`mode: init`, `"mode" "init"`,
//!   `.mode "init"`);
//! * next to the word `mode` or `modes` (`init mode`, `writer/init modes`);
//! * in a list with an accepted mode, joined by `/`, `,`, `|`, `or` or `and`
//!   (`all, writer, reader, init`);
//! * backticked, on a line that also backticks an accepted mode;
//! * the first cell of a table row (the mode tables).
//!
//! Every tracked file is read except `schema/` (`schema.sh` has its own
//! `--mode single|cluster` flag), `deploy/charts/` (issue #506 part 2 takes
//! the chart's mentions and removes this exclusion), this file, and any file
//! that is not UTF-8.

use std::path::{Path, PathBuf};

use regex::Regex;

/// The mode names the parser accepted once and refuses now.
const REMOVED_MODES: &[&str] = &["init"];

/// The tracked paths this test does not read, by prefix.
const SKIPPED: &[&str] = &["schema/", "deploy/charts/"];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The seven forms of a mention of `removed`, each with its name.
fn mention_forms(removed: &str) -> Vec<(&'static str, Regex)> {
    let r = removed;
    let a = "(?:all|writer|reader)";
    let sep = r"\s*(?:/|,|\||\bor\b|\band\b)\s*";
    [
        ("after --mode", format!(r#"--mode[\s="'`]*{r}\b"#)),
        (
            "after PULSUS_MODE",
            format!(r#"PULSUS_MODE[\s=:"'`]*{r}\b"#),
        ),
        (
            "a mode key or parameter",
            format!(r#"(?i)\bmode["'`]?\s*[:=]?\s*["'`]?{r}\b"#),
        ),
        (
            "next to mode",
            format!(r"(?i)\b{r}[\s-]+modes?\b|\bmodes?\s+{r}\b"),
        ),
        (
            "in a list with an accepted mode",
            format!(r"`?\b{a}\b`?{sep}`?\b{r}\b|\b{r}\b`?{sep}`?\b{a}\b"),
        ),
        ("backticked beside a mode", format!("`{r}`")),
        (
            "a table row's first cell",
            format!(r"^\|\s*`?{r}`?(\s*\(default\))?\s*\|"),
        ),
    ]
    .into_iter()
    .map(|(form, re)| (form, Regex::new(&re).expect("a valid mention pattern")))
    .collect()
}

#[test]
fn no_tracked_file_names_a_mode_the_binary_refuses() {
    for name in REMOVED_MODES {
        assert!(
            name.parse::<pulsus_config::Mode>().is_err(),
            "{name} is accepted again; remove it from REMOVED_MODES"
        );
    }
    let accepted_backticked = Regex::new("`(?:all|writer|reader)`").expect("a valid pattern");
    let forms: Vec<(&str, Regex)> = REMOVED_MODES
        .iter()
        .flat_map(|r| mention_forms(r))
        .collect();

    let listing = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root())
        .args(["ls-files", "-z"])
        .output()
        .expect("git ls-files must run: the tracked tree is this test's domain");
    assert!(listing.status.success(), "git ls-files failed");
    let listing = String::from_utf8(listing.stdout).expect("UTF-8 paths");

    let mut lines_read = 0usize;
    let mut found = Vec::new();
    for file in listing.split('\0') {
        if file.is_empty()
            || SKIPPED.iter().any(|prefix| file.starts_with(prefix))
            || file == "crates/pulsus-config/tests/refused_modes.rs"
        {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(repo_root().join(file)) else {
            continue; // not UTF-8
        };
        for (i, line) in text.lines().enumerate() {
            lines_read += 1;
            for (form, re) in &forms {
                let beside =
                    *form != "backticked beside a mode" || accepted_backticked.is_match(line);
                if beside && re.is_match(line) {
                    found.push(format!("{file}:{}: {form}: {}", i + 1, line.trim()));
                }
            }
        }
    }
    assert!(lines_read > 100_000, "read only {lines_read} lines");
    assert!(
        found.is_empty(),
        "{} mentions of a refused mode: {found:#?}",
        found.len()
    );
}
