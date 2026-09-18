//! Issue #498 criterion 10: **the four places that state the migration
//! amendment policy agree on which window is the current one.**
//!
//! The catalog's `CREATE`s are edited in place while the condition the
//! policy names still holds — no tagged release, no persistent
//! deployment, databases created fresh. Four artefacts write that policy
//! down and each names the latest window:
//!
//! ```text
//!   docs/schemas.md                        §6, "Migration amendment policy"
//!   docs/architecture.md                   §3, the DDL-ownership paragraph
//!   crates/pulsus-schema/src/catalog.rs    the module doc
//!   docs/traceql-schema-migration.md       a design record of a decision
//! ```
//!
//! **Nothing read any of them before this file.** Searched at the base of
//! issue #498: no test and no source file referenced the policy sentence,
//! and `doc_verification_markers.rs`'s two `docs/schemas.md` entries are
//! about a shard roster and an omitted response field. An unguarded
//! paragraph in four copies is a paragraph three of which get updated,
//! which is exactly what happened — issue #54's window was named as the
//! last one in all four long after the plan for this issue had listed
//! three of them.
//!
//! **It keys on TEXT, never on a line number.** The fourth passage quotes
//! `docs/schemas.md:933` for a paragraph that now sits at `:999`, so a
//! line-keyed check would have to be corrected by hand every time a
//! document above it grew.
//!
//! The fourth is a **record of a decision** rather than a statement of
//! policy, so it is treated differently: its analysis and its conclusion
//! stand untouched, and it carries a dated note at its head saying the
//! window was reopened. What this file asserts of it is the same thing it
//! asserts of the others — it names the current window, and it does not
//! still claim the earlier one was the last.

/// The window this change opened, named the same way in all four.
const CURRENT_WINDOW: &str = "#498";

/// The window that was the last one before it. A passage still calling
/// this one "the last" is stale, whatever else it says.
const SUPERSEDED_WINDOW: &str = "#54";

/// The sentence fragment that carries the claim, in each artefact's own
/// wording, and **how many times that passage names the current window**.
///
/// The count is the point. Two of these passages name `#498` twice — the
/// catalog's module doc in its statement and again in its attribution, the
/// design record's note in its first sentence and again in its last — and
/// a `contains` check passes when only one of the two is changed. Measured:
/// with the catalog's first occurrence alone rewritten, a presence test
/// reports every file agreeing while the passage says two different things
/// about which window is current. The cardinality is what makes each
/// single-occurrence edit a failure.
const POLICY_PASSAGES: &[(&str, &str, usize)] = &[
    ("docs/schemas.md", "**Migration amendment policy:**", 1),
    (
        "docs/architecture.md",
        "Migrations are idempotent, and append-only from the first tagged release onward",
        1,
    ),
    (
        "crates/pulsus-schema/src/catalog.rs",
        "**Amendment policy:** migrations are append-only",
        2,
    ),
    (
        "docs/traceql-schema-migration.md",
        "> **Historical, 2026-09-17.** The window was reopened by a ruling on issue",
        2,
    ),
];

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(repo_root().join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// The passage: from its opening fragment to the end of the paragraph it
/// starts (the next blank line), which is the unit a person edits.
fn passage(rel: &str, opener: &str) -> String {
    let src = read(rel);
    let at = src.find(opener).unwrap_or_else(|| {
        panic!("{rel} no longer carries the amendment-policy passage opening {opener:?}")
    });
    let rest = &src[at..];
    match rest.find("\n\n") {
        Some(end) => rest[..end].to_string(),
        None => rest.to_string(),
    }
}

/// **All four name the current window, as many times as they are written
/// to.**
///
/// Presence is not enough: a passage that names the window once and the
/// superseded one once is a passage saying two things, and a `contains`
/// check reads it as agreement.
#[test]
fn the_four_amendment_policy_statements_name_one_window() {
    let mut wrong: Vec<String> = Vec::new();
    for (rel, opener, expected) in POLICY_PASSAGES {
        let p = passage(rel, opener);
        let got = p.matches(CURRENT_WINDOW).count();
        if got != *expected {
            wrong.push(format!(
                "{rel}: names {CURRENT_WINDOW} {got} time(s), expected {expected}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "the amendment-policy passage must name the same window in all four places, everywhere \
         it states one:\n  {}",
        wrong.join("\n  ")
    );
}

/// **And none still calls the superseded window the last one.** Naming the
/// current window is not enough on its own: a passage can carry both and
/// then say two incompatible things in the same paragraph.
#[test]
fn no_amendment_policy_statement_still_calls_the_superseded_window_the_last_one() {
    let stale: Vec<&str> = POLICY_PASSAGES
        .iter()
        .filter(|(rel, opener, _)| {
            let p = passage(rel, opener);
            // The design record's dated note names the superseded window
            // deliberately, as the thing that was superseded, so what is
            // checked is the CLAIM rather than the word: "<marker> … last
            // such … window".
            let Some(at) = p.find(SUPERSEDED_WINDOW) else {
                return false;
            };
            let after = &p[at..];
            after.contains("was the last such") || after.contains("was the last window")
        })
        .map(|(rel, _, _)| *rel)
        .collect();
    assert!(
        stale.is_empty(),
        "these passages still call {SUPERSEDED_WINDOW} the last amendment window, which is no \
         longer true: {stale:?}"
    );
}

/// The design record's own three stale sentences are covered by one dated
/// note, and its conclusion — that the span change needed no amendment —
/// is untouched. Asserted so a later edit cannot quietly rewrite the
/// analysis instead of annotating it.
#[test]
fn the_design_record_keeps_its_conclusion_under_the_dated_note() {
    let src = read("docs/traceql-schema-migration.md");
    let note = src
        .find("> **Historical, 2026-09-17.**")
        .expect("the dated note is at the head of the passage");
    let claim = src
        .find("**Amending migration 16 and 18 in place is not currently permitted.**")
        .expect("the record's original claim is still there");
    assert!(
        note < claim,
        "the dated note must stand at the head of the passage it annotates, before the claim it \
         supersedes"
    );
    assert!(
        src.contains("It is expressible entirely as new,\nappend-only migrations"),
        "the record's conclusion — the span change needs no amendment — must not be disturbed"
    );
}
