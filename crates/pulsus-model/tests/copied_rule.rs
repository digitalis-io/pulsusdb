//! Issue #381 — the structured-metadata `del`-vs-`Set` rule has exactly ONE
//! wording, and the documents that state it COPY it rather than describe it.
//!
//! Three rounds of review on this issue found the same defect three times: a
//! rule restated in the reader's own words drifts from the code, and the
//! drift is invisible because every wording is individually plausible. The
//! response was to declare one sentence canonical. That declaration was still
//! only an intention — this test is what makes it fail.
//!
//! The canonical sentence lives in `pulsus_model::resolve_structured_metadata`'s
//! doc comment, primitive 7, between the markers below. Every file in
//! [`COPIES`] must contain it **byte for byte** after doc-comment prefixes are
//! stripped and whitespace is collapsed — the two normalizations a Markdown
//! renderer applies anyway, so the rendered text is identical, not merely
//! similar. Nothing else is normalized: a changed word, a moved emphasis span,
//! a dropped comma or an em dash swapped for a comma all fail, which is
//! precisely the class of difference that got through by eye.
//!
//! To change the rule: edit it in `labels.rs` and run this test, which names
//! every file that has fallen behind.

use std::path::{Path, PathBuf};

/// The canonical text lives here, in the doc comment of the function that
/// implements the rule.
const SOURCE: &str = "crates/pulsus-model/src/labels.rs";

/// Every document that must carry the canonical sentence verbatim.
///
/// `docs/api.md` §8.2 states it inside a rules TABLE and `docs/schemas.md`
/// §3.1 inside prose; both still carry the identical sentence, because the
/// sentence contains no `|` and a Markdown table cell is otherwise ordinary
/// inline Markdown. If that ever stops being true, the honest move is to drop
/// the file from this list and say in the file that it RESTATES rather than
/// copies — not to loosen the comparison.
const COPIES: &[&str] = &["docs/api.md", "docs/schemas.md"];

const START: &str = "<!-- copied-rule:del-vs-set:start -->";
const END: &str = "<!-- copied-rule:del-vs-set:end -->";

fn repo_root() -> PathBuf {
    // `crates/pulsus-model` -> repo root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the manifest dir is two levels below the repo root")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

/// Strips Rust doc-comment prefixes and collapses every whitespace run to one
/// space — what a Markdown renderer does to inline text either way. Applied to
/// the source and to the docs alike, so neither side gets a normalization the
/// other does not.
fn normalize(text: &str) -> String {
    let stripped: String = text
        .lines()
        .map(|line| {
            let line = line.trim_start();
            line.strip_prefix("///")
                .or_else(|| line.strip_prefix("//!"))
                .unwrap_or(line)
        })
        .collect::<Vec<_>>()
        .join(" ");
    stripped.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The text BETWEEN the markers, trimmed, from an already-normalized
/// haystack. Panics with the file name when a marker is missing or repeated,
/// so a silently-deleted rule fails as loudly as a reworded one.
///
/// Trimmed because the source states the rule on its own doc-comment lines
/// while a table cell states it inline, which puts a space just inside the
/// markers in one and not the other — invisible in both renderings, and the
/// only difference this normalization can hide, since it touches nothing
/// interior.
fn extract(normalized: &str, what: &str) -> String {
    assert_eq!(
        normalized.matches(START).count(),
        1,
        "{what}: expected exactly one {START}"
    );
    assert_eq!(
        normalized.matches(END).count(),
        1,
        "{what}: expected exactly one {END}"
    );
    let start = normalized.find(START).expect("counted above");
    let end = normalized.find(END).expect("counted above");
    assert!(
        start < end,
        "{what}: the end marker precedes the start marker"
    );
    normalized[start + START.len()..end].trim().to_string()
}

#[test]
fn every_document_copies_the_del_vs_set_rule_verbatim() {
    let canonical = extract(&normalize(&read(SOURCE)), SOURCE);
    // Non-vacuity: a marker pair that had lost its contents would otherwise
    // let every file "match" an empty rule.
    assert!(
        canonical.len() > 200,
        "the canonical sentence at {SOURCE} has shrunk to {canonical:?} — a marker pair with \
         (almost) nothing between it would make this test vacuous"
    );
    assert!(
        canonical.contains("`del` drops BASE entries only"),
        "the canonical sentence no longer states the rule this test exists for: {canonical:?}"
    );

    let mut stale = Vec::new();
    for copy in COPIES {
        let found = extract(&normalize(&read(copy)), copy);
        if found != canonical {
            stale.push(format!(
                "{copy}\n  has:      {found}\n  expected: {canonical}"
            ));
        }
    }
    assert!(
        stale.is_empty(),
        "these documents no longer copy {SOURCE}'s primitive 7 — edit the rule THERE and copy it \
         out, do not reword it here:\n{}",
        stale.join("\n")
    );
}

/// The comparison is exact, not "close enough" — demonstrated rather than
/// asserted, by putting the four differences that got through the last review
/// past [`normalize`] and checking each one still reads as different.
///
/// They are the real ones: `An` -> `an`, an emphasis span moved from the
/// qualifier to the whole clause, `— a rename` -> `, and a rename`, and a
/// dropped comma. A normalization that swallowed any of them would let the
/// same defect back in.
#[test]
fn the_comparison_rejects_the_differences_that_got_through_by_eye() {
    let canonical = extract(&normalize(&read(SOURCE)), SOURCE);
    let body = canonical.as_str();

    let mutations = [
        ("case", body.replacen("An empty value", "an empty value", 1)),
        (
            "emphasis moved",
            body.replacen(
                "that the builder did not `Set`",
                "*that the builder did not `Set`*",
                1,
            ),
        ),
        (
            "em dash for comma",
            body.replacen(", and a rename", " — a rename", 1),
        ),
        (
            "comma dropped",
            body.replacen("name in either wire order,", "name in either wire order", 1),
        ),
    ];
    for (label, mutated) in mutations {
        assert_ne!(
            normalize(&mutated),
            normalize(body),
            "{label}: this difference survives normalization, so the test would not catch it"
        );
    }

    // …while the normalizations the test DOES apply are genuinely invisible:
    // a doc-comment prefix and a re-wrap must not count as a difference.
    let rewrapped = format!("///   {}", body.replace(' ', "\n///       "));
    assert_eq!(
        normalize(&rewrapped),
        normalize(body),
        "re-wrapping a doc comment must not read as a changed rule"
    );
}
