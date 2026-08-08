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
//! [`COPIES`] must contain it **byte for byte** after whitespace runs are
//! collapsed — and, on the RUST SOURCE side only, after each line's
//! doc-comment marker is stripped. Both are normalizations a Markdown renderer
//! applies to that input anyway, so the rendered text is identical rather than
//! merely similar. The asymmetry is deliberate: `///` is syntax in the source
//! and literal rendered text in a `.md` file, so stripping it from both sides
//! would let a document that gained a stray `///` prefix compare equal.
//! Nothing else is normalized — a changed word, a moved emphasis span, a
//! dropped comma or an em dash swapped for a comma all fail, which is
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

/// Which side of the comparison a text came from — and therefore whether
/// `///`/`//!` at the start of a line is syntax to strip or literal text to
/// keep.
///
/// It is only syntax in the Rust source. In a Markdown document those three
/// characters render as themselves, so stripping them there would let a doc
/// that gained a stray `///` prefix compare equal to the source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    RustSource,
    MarkdownDoc,
}

/// Collapses every whitespace run to one space, and — for
/// [`Kind::RustSource`] ONLY — strips the leading doc-comment marker from each
/// line. Both are what a Markdown renderer does to the corresponding input, so
/// two texts that survive this comparison render identically rather than
/// merely similarly.
fn normalize(text: &str, kind: Kind) -> String {
    let joined: String = text
        .lines()
        .map(|line| match kind {
            Kind::RustSource => {
                let line = line.trim_start();
                line.strip_prefix("///")
                    .or_else(|| line.strip_prefix("//!"))
                    .unwrap_or(line)
            }
            Kind::MarkdownDoc => line,
        })
        .collect::<Vec<_>>()
        .join(" ");
    joined.split_whitespace().collect::<Vec<_>>().join(" ")
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
    let canonical = extract(&normalize(&read(SOURCE), Kind::RustSource), SOURCE);
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
        let found = extract(&normalize(&read(copy), Kind::MarkdownDoc), copy);
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
///
/// Each mutation is applied in two steps that fail SEPARATELY, because a
/// `replacen` whose needle has vanished silently no-ops and a no-op mutation
/// equals the body — which would trip the "survives normalization" assertion
/// for entirely the wrong reason. So: the needle must be present (or this
/// test is checking nothing), the replacement must change the raw text, and
/// only then must the difference survive [`normalize`].
#[test]
fn the_comparison_rejects_the_differences_that_got_through_by_eye() {
    let canonical = extract(&normalize(&read(SOURCE), Kind::RustSource), SOURCE);
    let body = canonical.as_str();

    let mutations = [
        ("case", "An empty value", "an empty value"),
        (
            "emphasis moved",
            "that the builder did not `Set`",
            "*that the builder did not `Set`*",
        ),
        ("em dash for comma", ", and a rename", " — a rename"),
        (
            "comma dropped",
            "name in either wire order,",
            "name in either wire order",
        ),
    ];
    for (label, needle, replacement) in mutations {
        assert!(
            body.contains(needle),
            "{label}: the mutation's needle {needle:?} is no longer in the canonical \
             sentence, so this row asserts nothing — re-derive it from the sentence as \
             it now reads"
        );
        let mutated = body.replacen(needle, replacement, 1);
        assert_ne!(
            mutated, body,
            "{label}: the mutation did not change the raw text"
        );
        assert_ne!(
            normalize(&mutated, Kind::RustSource),
            normalize(body, Kind::RustSource),
            "{label}: this difference survives normalization, so the test would not catch it"
        );
    }

    // …while the normalizations the test DOES apply are genuinely invisible:
    // a doc-comment prefix and a re-wrap must not count as a difference.
    let rewrapped = format!("///   {}", body.replace(' ', "\n///       "));
    assert_eq!(
        normalize(&rewrapped, Kind::RustSource),
        normalize(body, Kind::RustSource),
        "re-wrapping a doc comment must not read as a changed rule"
    );
}
