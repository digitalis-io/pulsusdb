//! Issue #385 — the two backpressure divergences have exactly ONE wording
//! each, and the documents that state them COPY it rather than describe it.
//!
//! Both endpoints this issue touches answer sink backpressure `429` where
//! their reference does something else, and both divergences are owner
//! rulings rather than measurements, so the reasoning is the artifact. A
//! reasoning restated in the reader's own words drifts from the code, and
//! the drift is invisible because every wording is individually plausible.
//! So one copy is declared canonical and this test is what makes a drifted
//! copy fail.
//!
//! | marker | canonical | copy |
//! |---|---|---|
//! | `copied-rule:rw-backpressure` | `remote_write_backpressure_response`'s doc comment in `src/ingest/http.rs` | `docs/api.md` §1.2 |
//! | `copied-rule:zipkin-backpressure` | the `zipkin-backpressure-429-not-500` entry in `docs/benchmarks/traces-differential-ledger.md` | `docs/api.md` §8.2 |
//!
//! # The comparison is RAW, and that is deliberate
//!
//! `crates/pulsus-model/tests/copied_rule.rs` (issue #381) collapses every
//! whitespace run before comparing. This test does not, and is a second
//! mechanism on purpose: #381's gate keeps its comparator (tightening it
//! would redden `main` today on all three of its bindings), while this one
//! starts strict.
//!
//! The folding comparator was tried here first and two counterexamples
//! sank it, both verified by reproducing its `normalize` and running them
//! through it: a paragraph and the same paragraph with **two trailing
//! spaces** before a newline fold equal but render as a Markdown hard
//! break; a paragraph and the same paragraph with a **blank line plus a
//! four-space indent** fold equal but render as a code block. The deeper
//! problem is that "renders identically as Markdown" is a claim over the
//! whole of Markdown's rendering surface, and any escape list over that
//! surface will be incomplete — those two were the second attempt at one.
//! Joining lines instead of folding them is not a fix either: `intro\n-
//! item` and `intro - item` join identically and render as a list versus a
//! paragraph. **Raw equality has no escape list, so there is nothing to
//! leave off it.**
//!
//! ## What raw equality rejects that folding accepted
//!
//! Do not read this strictness as an oversight and re-introduce folding.
//! Every one of these was silently equal before and is RED now:
//!
//! - **reflow** — rewrapping a paragraph across different line boundaries;
//! - **reindentation**, including changing a doc comment's indent level;
//! - **changed blank-line placement** inside the marker body;
//! - **doubled or collapsed interior spaces**, and tabs for spaces;
//! - **trailing-whitespace changes**, including the hard break above.
//!
//! The friction — editing the document copy means editing the source
//! comment — is the intended behaviour of a copied-rule gate, not a cost
//! to be softened.
//!
//! ## Constraint on future edits: no trailing-space hard breaks
//!
//! A Markdown hard break written as two trailing spaces is not
//! maintainable on the Rust side of a pair: editors and `rustfmt` trim
//! trailing whitespace, which would silently redden this gate. Canonical
//! text that needs a line break must use a blank line or an explicit
//! `<br>`. Neither committed text needs one today.
//!
//! The ONLY transform applied is removing `^[ \t]*///` plus at most one
//! following space, on the Rust side only — lossless syntax removal, since
//! `///` is Rust syntax there and literal rendered text in a `.md` file.
//! It deletes a fixed prefix from each line and so cannot merge two
//! distinct texts.

use std::path::{Path, PathBuf};

/// Which side of a pair a text came from, and therefore whether a leading
/// `///` is syntax to strip or literal text to keep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    RustDoc,
    Markdown,
}

/// One canonical text and every document that must carry it verbatim.
#[derive(Clone, Copy, Debug)]
struct Pair {
    /// e.g. `"copied-rule:rw-backpressure"`; the needles are
    /// `<!-- {marker}:start -->` and `<!-- {marker}:end -->`.
    marker: &'static str,
    canonical: (&'static str, Side),
    copies: &'static [(&'static str, Side)],
    /// Non-vacuity, mirroring `copied_rule.rs`: a marker pair that had
    /// lost its contents would otherwise let every file "match" an empty
    /// rule.
    must_contain: &'static str,
    min_len: usize,
}

const PAIRS: &[Pair] = &[
    Pair {
        marker: "copied-rule:rw-backpressure",
        canonical: ("crates/pulsus-write/src/ingest/http.rs", Side::RustDoc),
        copies: &[("docs/api.md", Side::Markdown)],
        must_contain: "treats `429` as non-recoverable and **drops** the batch",
        min_len: 800,
    },
    Pair {
        marker: "copied-rule:zipkin-backpressure",
        canonical: (
            "docs/benchmarks/traces-differential-ledger.md",
            Side::Markdown,
        ),
        copies: &[("docs/api.md", Side::Markdown)],
        must_contain: "answers its ingestion rate-limit rejection **500**",
        min_len: 400,
    },
];

fn repo_root() -> PathBuf {
    // `crates/pulsus-write` -> repo root.
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

/// The lines strictly between the unique start- and end-marker LINES, with
/// Rust doc syntax removed and **nothing else changed**.
///
/// Extraction is by line index, never by byte offset into a normalized
/// string: a folding comparator normalizes first and so cannot preserve
/// line structure at all, which is the property this test exists to
/// compare.
///
/// Splitting on `'\n'` rather than `str::lines` is deliberate — `lines`
/// silently swallows the `\r` of a CRLF checkout, which would let the
/// comparison change meaning with the checkout's line endings.
///
/// Panics, naming the file (and the line, for a malformed doc line), when
/// a marker is missing or repeated, when the end marker precedes the
/// start, when a `RustDoc` body line lacks `///`, or when the body
/// contains `\r`.
fn body(path: &str, marker: &str, side: Side) -> String {
    let text = read(path);
    let start_needle = format!("<!-- {marker}:start -->");
    let end_needle = format!("<!-- {marker}:end -->");
    let lines: Vec<&str> = text.split('\n').collect();

    let find_unique = |needle: &str| -> usize {
        let hits: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(_, l)| l.contains(needle))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            hits.len(),
            1,
            "{path}: expected exactly one line containing {needle:?}, found {}",
            hits.len()
        );
        hits[0]
    };

    let start = find_unique(&start_needle);
    let end = find_unique(&end_needle);
    assert!(
        start < end,
        "{path}: the {marker} end marker precedes its start marker"
    );

    let mut out: Vec<String> = Vec::with_capacity(end - start);
    for (offset, raw) in lines[start + 1..end].iter().enumerate() {
        let lineno = start + offset + 2; // 1-based, and past the start marker
        assert!(
            !raw.contains('\r'),
            "{path}:{lineno}: carriage return inside a {marker} body — this file must be \
             checked out with LF endings or the comparison changes meaning"
        );
        match side {
            Side::Markdown => out.push((*raw).to_string()),
            Side::RustDoc => {
                let trimmed = raw.trim_start_matches([' ', '\t']);
                let stripped = trimmed.strip_prefix("///").unwrap_or_else(|| {
                    panic!(
                        "{path}:{lineno}: a line inside the {marker} body is not a doc comment: \
                         {raw:?}"
                    )
                });
                out.push(stripped.strip_prefix(' ').unwrap_or(stripped).to_string());
            }
        }
    }
    out.join("\n")
}

/// Every copy is byte-identical to its canonical once Rust doc-comment
/// syntax is removed — no whitespace folding of any kind.
#[test]
fn backpressure_divergence_is_recorded() {
    let mut stale = Vec::new();
    for pair in PAIRS {
        let (canonical_path, canonical_side) = pair.canonical;
        let canonical = body(canonical_path, pair.marker, canonical_side);

        assert!(
            canonical.len() >= pair.min_len,
            "{canonical_path}: the {} canonical text has shrunk to {} bytes ({canonical:?}) — a \
             marker pair with (almost) nothing between it would make this test vacuous",
            pair.marker,
            canonical.len()
        );
        assert!(
            canonical.contains(pair.must_contain),
            "{canonical_path}: the {} canonical text no longer states the rule this test exists \
             for ({:?} is missing): {canonical:?}",
            pair.marker,
            pair.must_contain
        );

        for (copy_path, copy_side) in pair.copies {
            let found = body(copy_path, pair.marker, *copy_side);
            if found != canonical {
                stale.push(format!(
                    "{copy_path} ({})\n  has:      {found:?}\n  expected: {canonical:?}",
                    pair.marker
                ));
            }
        }
    }
    assert!(
        stale.is_empty(),
        "these documents no longer copy their canonical text byte for byte — edit the rule at \
         its canonical site and copy it out, do not reword or rewrap it here:\n{}",
        stale.join("\n")
    );
}

/// Reproduces `crates/pulsus-model/tests/copied_rule.rs`'s `normalize` for
/// Markdown input — join the lines with a space, then collapse every
/// whitespace run — so the row below can show what that comparator accepts
/// and this one does not.
///
/// A deliberate duplicate, not a shared helper: #381's gate is not
/// modified by this issue, and importing its private function would couple
/// the two mechanisms this test's module doc argues should stay separate.
/// If the two ever drift, this function's only job is to make the
/// demonstration below vacuous, which the equality assertions catch.
fn fold(text: &str) -> String {
    text.split('\n')
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// The comparison really is stricter than the folding one — demonstrated
/// rather than asserted, by building the differences that decided the
/// mechanism and showing each is invisible to folding and visible here.
///
/// The four rows are the real ones: the two counterexamples that sank
/// whitespace folding (a trailing-space hard break, and a blank line plus
/// a four-space indent opening a code block), the list-versus-paragraph
/// case that sank the line-joining fallback, and a plain reflow. Every
/// pair is derived from the committed canonical text rather than written
/// out, so a reworded rule cannot leave this test quietly checking a
/// string that no longer exists — the needle assertion fires first.
#[test]
fn folding_accepts_these_differences_and_this_test_does_not() {
    let canonical = body(PAIRS[0].canonical.0, PAIRS[0].marker, PAIRS[0].canonical.1);
    let needle = "body. The reference";
    assert!(
        canonical.contains(needle),
        "the demonstration's needle {needle:?} is no longer in the canonical text, so every \
         row below asserts nothing — re-derive them from the text as it now reads"
    );

    let variant = |replacement: &str| canonical.replacen(needle, replacement, 1);

    let cases = [
        // (label, left, right)
        ("trailing-space hard break", canonical.clone(), {
            let v = variant("body.  \nThe reference");
            assert_ne!(v, canonical, "hard break: the mutation changed nothing");
            v
        }),
        ("blank line + indent (code block)", canonical.clone(), {
            let v = variant("body.\n\n    The reference");
            assert_ne!(v, canonical, "code block: the mutation changed nothing");
            v
        }),
        ("reflow", canonical.clone(), {
            let v = variant("body.\nThe reference");
            assert_ne!(v, canonical, "reflow: the mutation changed nothing");
            v
        }),
        // Not a mutation OF the canonical but a pair of siblings: a list
        // item and an inline dash join identically, which is why a
        // line-joining comparator was rejected too.
        (
            "list item vs inline dash",
            variant("body.\n- The reference"),
            variant("body. - The reference"),
        ),
    ];

    for (label, left, right) in cases {
        assert_eq!(
            fold(&left),
            fold(&right),
            "{label}: this pair is NOT invisible to the folding comparator, so it demonstrates \
             nothing about the difference between the two mechanisms"
        );
        assert_ne!(
            left, right,
            "{label}: the raw comparison would accept this difference, so this test would not \
             catch it"
        );
    }
}
