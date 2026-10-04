//! The live-suite database-name uniqueness guard: no two tests in the
//! workspace compose the same throwaway ClickHouse name.
//!
//! Hermetic — no server, no ClickHouse. Runs under the plain `ci` job's
//! `cargo test --workspace` / `cargo nextest run --workspace`, not behind
//! `PULSUS_TEST_CLICKHOUSE`.
//!
//! ## Why this exists
//!
//! Three pairs of live tests shared a database name (issue #616). Every
//! one of those tests opens with `DROP DATABASE IF EXISTS`, so whichever
//! member of a pair started second destroyed the other's schema while it
//! was still inserting. The symptom does not look like a collision: the
//! second test reports a table *it had itself created* as unknown, which
//! reads as a schema problem in the test that fails. All three predated
//! the change that surfaced one of them; they were latent for as long as
//! the two members happened not to overlap, and a continuous-integration
//! job that had passed on the previous commit went red twice when an
//! unrelated change shifted the ordering.
//!
//! `live_port_uniqueness.rs` does exactly this for listener ports, and
//! `live_db_naming.rs` establishes that every test name comes from
//! [`source_scan::COMPOSER_CALLS`] — but nothing said the names those
//! calls pass are distinct. This does.
//!
//! ## What the two guards establish together
//!
//! `live_db_naming.rs` rules 1 and 2: a reserved `pulsus_…_it…` name may
//! appear only inside a composer's argument list, and every `db`-named
//! binding and `CLICKHOUSE_DB` setting delegates to a composer. So the
//! composer call sites are where the names are. This file enumerates
//! those call sites and requires the **spelling** each one passes to be
//! unique across the workspace.
//!
//! The two guards are kept apart on purpose. `live_db_naming.rs` exempts
//! itself from its own scan, because its fixtures contain the very shapes
//! it rejects — and that file is also where both of this project's
//! known-harmless look-alike names live. A uniqueness check folded into
//! it would inherit the exemption and never see them, which is precisely
//! the distinction this check has to get right.
//!
//! ## A creating call, and text that looks like one
//!
//! The hard part is telling a call that creates a database from the same
//! characters appearing somewhere that creates nothing. Two names on
//! `main` are written more than once and both are harmless:
//!
//! * `pulsus_x_it` — in doc comments, in `pulsus-testkit`'s own unit
//!   tests, and inside fixture source strings.
//! * `pulsus_read_it_s1_single` — once as a call in
//!   `crates/pulsus-read/tests/explain_indexes.rs`, and once inside a raw
//!   string literal in `live_db_naming.rs` that is handed to a source
//!   scanner **as text**. The second one looks exactly like the first.
//!
//! Three mechanisms separate them, none of them a list of file names:
//!
//! 1. **A comment is not code.** The scan runs over the comment-blanked
//!    view, so a composer call quoted in a doc comment is not there to be
//!    found at all.
//! 2. **Text inside a string literal is not code.** The two preprocessed
//!    views disagree on exactly those bytes — see
//!    [`inside_a_string_literal`]. A composer call whose own first byte
//!    sits inside a literal is counted as quoted text and contributes no
//!    name.
//! 3. **Out of the test tree the composer is unreachable.**
//!    [`the_composer_is_reachable_only_from_the_scanned_test_tree`] checks
//!    that no `.rs` file outside the scanned tree makes a qualified
//!    composer call or imports one, so `pulsus-testkit`'s own unit tests
//!    are not an exception that has to be trusted.
//!
//! ## The rules
//!
//! 1. **Every composer argument is readable.** A composer call's argument
//!    list must contain a plain string literal whose fixed part is an
//!    identifier. `test_db(&name)`, `test_db(r"…")` and
//!    `test_db("not a name")` are hard failures — a name the scan cannot
//!    read must stop the build rather than be skipped quietly.
//! 2. **Every composer call is qualified.** A bare `test_db(…)` in a
//!    scanned file is a hard failure. The scan keys on the qualified
//!    spelling, so an unqualified call is a name it would never compare.
//! 3. **Every spelling is composed at exactly one call site**, across the
//!    whole scanned tree and across all three composers.
//!
//! ## Names composed at run time
//!
//! `pulsus_testkit::test_db(&format!("pulsus_read_it_qlg_{n}"))` exists,
//! 37 such sites at the time of writing. They are **covered as
//! templates** — rule 3 compares the template text, so two sites cannot
//! share `pulsus_read_it_qlg_{}` any more than they can share a fixed
//! name — and their *renderings* are **excluded**, because the scan
//! cannot evaluate `n`.
//!
//! What that leaves open, stated rather than implied:
//!
//! * A rendering that happens to equal a fixed name elsewhere. Ruling
//!   this out textually means treating the template's stem as a prefix and
//!   refusing every fixed name under it. On `main` that convicts 32 safe
//!   pairs — 21 of a template against a fixed name, 11 of a template
//!   against another template — because `pulsus_read_it_qlg_{}`
//!   interpolates a nonce, not the word `bodysearch`. A check that loud
//!   would be renamed around rather than kept.
//! * Two renderings of one template inside a single run colliding, which
//!   is a property of the interpolated expression, not of the text.
//!
//! [`MIN_COMPOSED_NAMES`] keeps that class visible: if the composed sites
//! disappear, the floor fires rather than this note quietly describing
//! nothing.
//!
//! ## Known boundary — what this cannot see
//!
//! * **Anything outside `crates/*/tests/**`.** `xtask`'s benchmarks and
//!   `ci/checks/mv_dedup_probe.sh` name a database from a command-line
//!   argument or a shell variable; neither is run by `cargo test` and
//!   neither has a name in source to compare. Rule 2's outside-the-tree
//!   half fails the build if a composer call appears there, so the
//!   boundary cannot be crossed silently.
//! * **A name assembled from non-literal parts.** Rule 1 refuses it, so
//!   it cannot reach the inventory unseen — but it also cannot be
//!   written, which is the trade.
//! * **A name split across literals** — `concat!("pulsus_x", "_it")`.
//!   Rule 1 reads the first literal, so this would be compared as
//!   `pulsus_x` and could collide on a stem that is not the name. No such
//!   site exists; `live_db_naming.rs` records the same boundary.
//! * **A database created by SQL the scan reads as data.** That is
//!   `live_db_naming.rs` rule 1's job, not this file's.
//!
//! The floors below are what keeps a *silent* zero-finding pass from
//! looking like success. Each is shown firing on its own, from a fixture
//! that clears the other four.

#[path = "support/source_scan.rs"]
mod source_scan;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use source_scan::{
    COMPOSER_CALLS, COMPOSER_ITEMS, COMPOSER_QUALIFIER, line_of, preprocess_views, rs_files_under,
    skip_literal_or_comment, skip_quoted, workspace_root,
};

/// Floors. These exist so that a scan which suddenly matches *nothing* —
/// a renamed directory, a composer spelled a new way, a walk that
/// silently returns no files — fails instead of passing green over zero
/// call sites. Set below the counts this scan measures (278 files
/// scanned, 574 creating calls in 77 files — no two of them sharing a
/// spelling — 37 of the 574 composed at run time, and 32 quoted
/// look-alikes) with enough slack that ordinary deletions do not trip
/// them.
///
/// [`check_floors`] evaluates **all five** and reports every breach
/// rather than returning on the first, so an empty tree names all five at
/// once instead of hiding four behind the first.
const MIN_FILES_SCANNED: usize = 200;
const MIN_CREATING_CALLS: usize = 480;
const MIN_CALLING_FILES: usize = 60;
const MIN_COMPOSED_NAMES: usize = 25;
/// At least one composer call in the tree is quoted text rather than a
/// call. Without this the look-alike discriminator could stop working and
/// only the fixtures in [`finder_tests`] would notice. Deliberately 1
/// against a measured 32: this is an existence floor, and the 32 are
/// fixtures in three files, any of which may legitimately be rewritten.
const MIN_QUOTED_LOOKALIKES: usize = 1;

/// Floors for [`the_composer_is_reachable_only_from_the_scanned_test_tree`],
/// which walks the whole repository rather than the test tree: 278 files
/// in the tree and 311 outside it.
const MIN_FILES_OUTSIDE: usize = 250;

/// The composer's own crate, the one place a composer call composes a
/// string without creating anything. Exempted from the
/// outside-the-tree rule — and the exemption is checked rather than
/// asserted by [`testkit_dependencies`]: the crate declares no
/// dependencies at all, so nothing there can reach a server.
const COMPOSER_CRATE: &str = "crates/pulsus-testkit";

/// The five floors, named so a breach can be asserted on individually
/// rather than by matching prose.
///
/// | variant | floor | demonstrated alone by |
/// |---|---|---|
/// | [`Floor::FilesScanned`] | [`MIN_FILES_SCANNED`] | `the_files_scanned_floor_fires_on_its_own` |
/// | [`Floor::CreatingCalls`] | [`MIN_CREATING_CALLS`] | `the_creating_call_floor_fires_on_its_own` |
/// | [`Floor::CallingFiles`] | [`MIN_CALLING_FILES`] | `the_calling_file_floor_fires_on_its_own` |
/// | [`Floor::ComposedNames`] | [`MIN_COMPOSED_NAMES`] | `the_composed_name_floor_fires_on_its_own` |
/// | [`Floor::QuotedLookalikes`] | [`MIN_QUOTED_LOOKALIKES`] | `the_quoted_lookalike_floor_fires_on_its_own` |
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Floor {
    FilesScanned,
    CreatingCalls,
    CallingFiles,
    ComposedNames,
    QuotedLookalikes,
}

/// How a name reached the composer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    /// A plain string literal: `test_db("pulsus_read_it_s2")`. The
    /// spelling is the name.
    Fixed,
    /// A format template: `test_db(&format!("pulsus_read_it_qlg_{n}"))`.
    /// The spelling is the template; what it renders to is not known
    /// here — see the module doc.
    Composed,
}

/// One creating call.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Name {
    /// Ordered first: grouping by spelling is the whole check.
    spelling: String,
    kind: Kind,
    file: String,
    line: usize,
    /// Which of [`COMPOSER_CALLS`] was called, for the error message.
    composer: String,
}

/// What one whole-tree scan found.
#[derive(Debug, Default)]
struct Inventory {
    files_scanned: usize,
    names: Vec<Name>,
    /// Composer spellings that are text rather than calls, as
    /// `(file, line)` — the look-alikes, counted so the discriminator
    /// cannot go quiet unnoticed.
    quoted: Vec<(String, usize)>,
}

/// What one file's scan found.
#[derive(Debug, Default)]
struct FileScan {
    names: Vec<Name>,
    quoted: Vec<usize>,
}

// ---------------------------------------------------------------------
// Lexing helpers
// ---------------------------------------------------------------------

/// Kept private rather than shared with the two sibling guards, which
/// each have their own copy: a one-line character class has no divergent
/// behaviour to protect. [`COMPOSER_CALLS`] does, which is why *it* has a
/// single owning declaration.
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// `true` when byte `at` of the source sits inside a string or char
/// literal.
///
/// Both views come from one lexer pass over the same bytes and are
/// byte-length-identical to the source: `stripped` blanks comments only,
/// `blanked` blanks comments **and** string/char literals (non-newline
/// bytes to spaces). So for a byte the source spells as an ASCII letter:
///
/// | where the byte is | `stripped` | `blanked` |
/// |---|---|---|
/// | live code | the letter | the letter |
/// | inside a string literal | the letter | a space |
/// | inside a comment | a space | a space |
///
/// Only the middle row has the two views disagreeing, which is what makes
/// a single byte comparison decisive. Every [`COMPOSER_CALLS`] entry
/// starts with an ASCII letter — the precondition of that table's left
/// column, asserted at compile time by
/// [`finder_tests::every_composer_spelling_starts_with_an_identifier_byte`].
fn inside_a_string_literal(stripped: &str, blanked: &str, at: usize) -> bool {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = (stripped, blanked, at);
    false
}

/// The byte span of the balanced parenthesised argument list that starts
/// at `open` (the index of `(`), to just past the matching `)`, or to the
/// end of the text when the source is truncated.
///
/// Literal-aware: a `(` or `)` inside a string literal does not move the
/// depth. `live_db_naming.rs` counts bytes without that step, which is
/// sound for the question it asks (does a span *contain* a helper call)
/// and not for this one (where does the span end, so which literal is the
/// first one in it).
fn arg_list_span(bytes: &[u8], open: usize) -> (usize, usize) {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = bytes;
    (open, open)
}

/// The first plain string literal in `stripped[lo..hi]`, as
/// `(content_offset, contents)`.
///
/// `None` when there is none, **and also** when the first literal is a
/// raw or byte string: `r#"…"#` and `b"…"` have delimiters this reader
/// does not decode, and a half-read name compared against the rest of the
/// tree is worse than a refusal. No composer call in the tree is written
/// that way; rule 1 keeps it so.
fn first_string_literal(stripped: &str, lo: usize, hi: usize) -> Option<(usize, &str)> {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = (stripped, lo, hi);
    None
}

/// `true` when `s` is a bare Rust/ClickHouse identifier:
/// `[A-Za-z_][A-Za-z0-9_]*`. A fixed name that is not one means the
/// literal this scan picked is not the name.
fn is_identifier(s: &str) -> bool {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = s;
    false
}

// ---------------------------------------------------------------------
// The scan
// ---------------------------------------------------------------------

/// Scans one source file. `Err` carries every rule-1 and rule-2
/// violation found in it, each already prefixed with `file:line`.
fn scan_source(rel: &str, src: &str) -> Result<FileScan, Vec<String>> {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = (rel, src);
    Ok(FileScan::default())
}

/// Every `.rs` file under `root/crates/*/tests`, sorted. The scanned
/// tree, shared by the whole-tree scan and by the outside-the-tree rule
/// so the two cannot disagree about where the boundary is.
fn domain_files(root: &Path) -> BTreeSet<PathBuf> {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = root;
    BTreeSet::new()
}

/// `path` relative to `root`, with forward slashes.
fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Walks the scanned tree. Returns the inventory, or every rule-1 and
/// rule-2 violation across it.
fn scan_tree(root: &Path) -> Result<Inventory, Vec<String>> {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = root;
    Ok(Inventory::default())
}

/// All five floors, over an already-scanned tree. Every floor is
/// evaluated; the error carries one entry per breach, in [`Floor`] order.
fn check_floors(inv: &Inventory) -> Result<(), Vec<(Floor, String)>> {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = inv;
    Err(Vec::new())
}

/// Rule 3: every spelling is composed at exactly one call site.
fn check_uniqueness(inv: &Inventory) -> Result<(), String> {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = inv;
    Ok(())
}

// ---------------------------------------------------------------------
// The guard
// ---------------------------------------------------------------------

/// Every throwaway ClickHouse name a test under `crates/*/tests`
/// composes is composed at exactly one call site, so no two tests can
/// drop each other's database.
#[test]
fn every_test_database_name_is_composed_at_exactly_one_call_site() {
    let root = workspace_root();
    let inv = match scan_tree(&root) {
        Ok(inv) => inv,
        Err(errors) => panic!(
            "{} composer call(s) the uniqueness guard cannot read:\n{}",
            errors.len(),
            errors.join("\n")
        ),
    };
    if let Err(breaches) = check_floors(&inv) {
        let list = breaches
            .iter()
            .map(|(floor, msg)| format!("  {floor:?}: {msg}"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!(
            "{} of the 5 scan floors were breached — the scan checked far less than it should \
             have:\n{list}",
            breaches.len()
        );
    }
    if let Err(msg) = check_uniqueness(&inv) {
        panic!("{msg}");
    }
}

// ---------------------------------------------------------------------
// Rule 2, outside the tree.
// ---------------------------------------------------------------------

/// Every qualified composer call and every `use` that imports a composer
/// in `.rs` files under `root`, excluding the scanned tree and
/// [`COMPOSER_CRATE`]. `vendor`, `target` and `.git` are not walked.
fn composer_sites_outside_the_tree(root: &Path) -> (usize, Vec<String>) {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = root;
    (0, Vec::new())
}

/// [`rs_files_under`] with `target`, `vendor` and `.git` left unwalked:
/// a build directory holds generated sources and `vendor` holds other
/// people's crates, neither of which can call this project's composer.
fn walk_outside(dir: &Path, out: &mut Vec<PathBuf>) {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = (dir, out);
}

/// The entries in [`COMPOSER_CRATE`]'s `[dependencies]` table.
///
/// This is what keeps that exemption from being an assurance: the crate is
/// exempt *because* it declares no dependencies, so a composer call in it
/// composes a string and can reach no server. Add a dependency and the
/// exemption has to be argued again.
fn testkit_dependencies(root: &Path) -> Vec<String> {
    // Issue #616, tests-first commit: stub. The implementation
    // lands in the next commit; every test above must fail on an
    // assertion here, not on a compile error.
    let _ = root;
    Vec::new()
}

/// The scan's domain is the whole of the composer's reach: no `.rs` file
/// outside `crates/*/tests` calls a composer by its qualified name or
/// imports one, so a name created where the uniqueness scan does not look
/// cannot exist.
#[test]
fn the_composer_is_reachable_only_from_the_scanned_test_tree() {
    let root = workspace_root();

    let deps = testkit_dependencies(&root);
    assert!(
        deps.is_empty(),
        "{COMPOSER_CRATE} now declares dependencies ({deps:?}). It is exempt from this rule \
         because a composer call there can reach no server, and that was true only while it had \
         none."
    );

    let (files_outside, sites) = composer_sites_outside_the_tree(&root);
    assert!(
        sites.is_empty(),
        "these sites compose a test object name outside `crates/*/tests`, where \
         `every_test_database_name_is_composed_at_exactly_one_call_site` does not look, so the \
         name is never compared against any suite's: {sites:#?}. Move the call into the test \
         tree, or widen this guard's domain to cover it."
    );
    assert!(
        files_outside >= MIN_FILES_OUTSIDE,
        "walked only {files_outside} `.rs` files outside the test tree (floor \
         {MIN_FILES_OUTSIDE}) — the walk found almost nothing, so finding no composer call \
         outside the tree means nothing"
    );
    assert!(
        domain_files(&root).len() >= MIN_FILES_SCANNED,
        "the scanned tree has fewer than {MIN_FILES_SCANNED} files, so the two halves of this \
         rule are measured against a tree that is not there"
    );
}

// ---------------------------------------------------------------------
// Validating the finder: each rule is shown rejecting the thing it is
// for, each floor is shown firing on its own, and the two known-harmless
// look-alike shapes are shown staying quiet. A guard that has never been
// observed to fail is indistinguishable from one that cannot.
// ---------------------------------------------------------------------

mod finder_tests {
    use super::*;

    fn errs(src: &str) -> Vec<String> {
        scan_source("t.rs", src).expect_err("expected the scan to reject this source")
    }

    fn accept(src: &str) -> FileScan {
        scan_source("t.rs", src).expect("expected the scan to accept this source")
    }

    fn spellings(src: &str) -> Vec<(String, Kind)> {
        accept(src)
            .names
            .into_iter()
            .map(|n| (n.spelling, n.kind))
            .collect()
    }

    /// The precondition of [`inside_a_string_literal`]'s table: every
    /// composer spelling's first byte is an ASCII letter, so it is never
    /// a space in either view unless a blanking pass put one there.
    #[test]
    fn every_composer_spelling_starts_with_an_identifier_byte() {
        const {
            let mut i = 0;
            while i < COMPOSER_CALLS.len() {
                assert!(COMPOSER_CALLS[i].as_bytes()[0].is_ascii_alphabetic());
                i += 1;
            }
        }
        // And each qualified spelling really is the bare item plus the
        // qualifier, which is what rule 2's skip relies on.
        assert_eq!(COMPOSER_CALLS.len(), COMPOSER_ITEMS.len());
        for (call, item) in COMPOSER_CALLS.iter().zip(COMPOSER_ITEMS) {
            assert_eq!(*call, format!("{COMPOSER_QUALIFIER}{item}"));
        }
    }

    /// Every fixture whose expected value is spelled OUTSIDE a composer
    /// call carries no `it` word, here and below. Not cosmetic: a
    /// `pulsus_…_it…` token that is not inside a composer's argument list
    /// is exactly what `live_db_naming.rs` rule 1 convicts, and this file
    /// is scanned by it. The uniqueness rule does not read the shape, so
    /// nothing is lost — `a_name_outside_the_reserved_shape_is_still_a_name`
    /// is the case for that.
    #[test]
    fn all_three_composer_spellings_are_read_as_creating_calls() {
        let src = r#"
fn f() {
    let db = &pulsus_testkit::test_db("pulsus_read_s1_single");
    let table = &pulsus_testkit::test_ident("pulsus_clickhouse_roundtrip");
}
static DB: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_search");
"#;
        let mut got = spellings(src);
        got.sort();
        assert_eq!(
            got,
            vec![
                ("pulsus_clickhouse_roundtrip".to_string(), Kind::Fixed),
                ("pulsus_read_s1_single".to_string(), Kind::Fixed),
                ("pulsus_traces_search".to_string(), Kind::Fixed),
            ]
        );
    }

    /// A name without the reserved `…_it…` shape is still a name: the
    /// tree has 16 of them, all of them databases a live test creates.
    #[test]
    fn a_name_outside_the_reserved_shape_is_still_a_name() {
        assert_eq!(
            spellings(r#"fn f() { let db = pulsus_testkit::test_db("a494_logs"); }"#),
            vec![("a494_logs".to_string(), Kind::Fixed)]
        );
    }

    #[test]
    fn a_format_template_is_read_as_a_composed_name() {
        assert_eq!(
            spellings(
                r#"fn f() { let db = pulsus_testkit::test_db(&format!("pulsus_read_qlg_{n}")); }"#
            ),
            vec![("pulsus_read_qlg_{n}".to_string(), Kind::Composed)]
        );
    }

    /// The first known-harmless shape: a composer call quoted in a doc
    /// comment. Comments are blanked before the scan, so there is nothing
    /// there to find — not even a quoted look-alike.
    #[test]
    fn a_composer_call_in_a_doc_comment_is_not_a_call() {
        // `crates/pulsus-server/tests/support/live_db.rs`'s shape.
        let scan = accept(
            "/// let db = pulsus_testkit::test_db(\"pulsus_x_it\");\nfn f() { let x = 1; }\n",
        );
        assert!(scan.names.is_empty(), "{:?}", scan.names);
        assert!(scan.quoted.is_empty(), "{:?}", scan.quoted);
    }

    /// The second known-harmless shape, and the one that reads exactly
    /// like a call: `live_db_naming.rs:709` sits inside an `r#"…"#` block
    /// that is fed to a source scanner as text. It composes nothing.
    #[test]
    fn a_composer_call_inside_a_string_literal_is_text_and_not_a_call() {
        let src = r##"
fn fixture() {
    let src = r#"
fn f() {
    let db = &pulsus_testkit::test_db("pulsus_read_it_s1_single");
    let run_db = pulsus_testkit::test_db(&format!("pulsus_read_it_qlg_{n}"));
}
"#;
    accept(src);
}
"##;
        let scan = accept(src);
        assert!(
            scan.names.is_empty(),
            "a call inside a string literal creates nothing: {:?}",
            scan.names
        );
        assert_eq!(scan.quoted.len(), 2, "both are counted as quoted text");
    }

    /// Both directions of the distinction in one fixture: the same name
    /// once as text and once as a call. The call is reported, the text is
    /// not — which is the property a check that simply skipped the file
    /// would not have.
    #[test]
    fn the_same_name_as_text_and_as_a_call_yields_exactly_one_name() {
        let src = r##"
fn fixture() {
    let quoted = r#"let db = pulsus_testkit::test_db("pulsus_read_it_s1_single");"#;
}
async fn real() {
    let db = &pulsus_testkit::test_db("pulsus_read_it_s1_single");
}
"##;
        let scan = accept(src);
        // Asserted by LINE rather than by spelling: the two sites spell
        // the same name, so the line is what says which was read.
        assert_eq!(scan.names.len(), 1, "{:?}", scan.names);
        assert_eq!(scan.names[0].line, 6, "the call, not the text");
        assert_eq!(scan.quoted, vec![3], "the text, not the call");
    }

    /// Rule 2 inside the tree. Deliberately bound to a name that is not
    /// `db`-shaped and given a name outside the reserved shape, so that
    /// this fixture does not also trip `live_db_naming.rs`'s two rules
    /// when *that* guard scans this file.
    #[test]
    fn an_unqualified_composer_call_is_rejected() {
        let e = errs(r#"fn f() { let x = test_db("bare_name"); }"#);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("without its"), "{}", e[0]);
        assert!(e[0].contains("t.rs:1"), "{}", e[0]);
    }

    #[test]
    fn a_qualified_composer_call_does_not_trip_the_unqualified_rule() {
        accept(r#"fn f() { let db = pulsus_testkit::test_ident("pulsus_qualified_it"); }"#);
        accept(r#"fn f() { let db = pulsus_testkit::TestDb::new("pulsus_qualified2_it"); }"#);
    }

    /// Rule 1: a name the scan cannot read stops the build rather than
    /// being skipped. Each of the three unreadable shapes is shown.
    #[test]
    fn a_composer_argument_the_scan_cannot_read_is_rejected() {
        for arg in ["&name", "&self.db_name", "scratch_name()"] {
            let e = errs(&format!(
                r#"fn f() {{ let db = pulsus_testkit::test_db({arg}); }}"#
            ));
            assert_eq!(e.len(), 1, "{arg}: {e:?}");
            assert!(e[0].contains("no readable name"), "{arg}: {}", e[0]);
        }
    }

    #[test]
    fn a_raw_or_byte_string_argument_is_rejected() {
        for arg in [r#"r"abc""#, r##"r#"abc"#"##, r#"b"abc""#] {
            let e = errs(&format!(
                r#"fn f() {{ let db = pulsus_testkit::test_db({arg}); }}"#
            ));
            assert!(!e.is_empty(), "{arg}: accepted");
            assert!(e[0].contains("no readable name"), "{arg}: {}", e[0]);
        }
    }

    #[test]
    fn a_literal_that_is_not_an_identifier_is_rejected() {
        let e = errs(r#"fn f() { let db = pulsus_testkit::test_db("INSERT INTO x"); }"#);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(
            e[0].contains("does not begin with an identifier"),
            "{}",
            e[0]
        );
    }

    /// The argument-span reader skips literals, so a parenthesis written
    /// inside one does not move the depth and end the span early.
    #[test]
    fn a_parenthesis_inside_a_literal_does_not_end_the_argument_span() {
        let src = r#"g(h(")"), 1);"#;
        let (lo, hi) = arg_list_span(src.as_bytes(), 1);
        assert_eq!(&src[lo..hi], r#"(h(")"), 1)"#);
    }

    /// The shape issue #616 is about: one name, two call sites, in two
    /// different files — `pulsus_trace_landing_it_w5` was in
    /// `trace_rows_v2.rs` and `trace_landing.rs`.
    #[test]
    fn a_name_composed_at_two_call_sites_is_rejected() {
        let mut inv = passing_inventory();
        assert!(check_uniqueness(&inv).is_ok(), "the control must pass");
        inv.names[7].spelling = inv.names[3].spelling.clone();
        inv.names[7].file = "crates/pulsus-write/tests/trace_landing.rs".to_string();
        let msg = check_uniqueness(&inv).expect_err("a duplicate must be rejected");
        assert!(msg.contains("more than one call site"), "{msg}");
        assert!(msg.contains(&inv.names[3].spelling), "{msg}");
        assert!(msg.contains(&inv.names[3].file), "{msg}");
        assert!(msg.contains("trace_landing.rs"), "{msg}");
        assert!(msg.contains("DROP DATABASE IF EXISTS"), "{msg}");
    }

    /// One namespace across the three composers: `test_db` and
    /// `test_ident` are the same function, and a table name colliding
    /// with a database name is the same accident.
    #[test]
    fn one_name_passed_to_two_different_composers_is_rejected() {
        let mut inv = passing_inventory();
        inv.names[2].spelling = inv.names[1].spelling.clone();
        inv.names[2].composer = COMPOSER_CALLS[1].to_string();
        let msg = check_uniqueness(&inv).expect_err("two composers, one name: rejected");
        assert!(msg.contains(COMPOSER_CALLS[1]), "{msg}");
    }

    /// Two composed names sharing a template are a duplicate too: rule 3
    /// compares the template text, which is the half of the run-time
    /// class this guard covers.
    #[test]
    fn two_sites_sharing_one_format_template_are_rejected() {
        let mut inv = passing_inventory();
        for i in [4, 5] {
            inv.names[i].spelling = "pulsus_read_qlg_{}".to_string();
            inv.names[i].kind = Kind::Composed;
        }
        let msg = check_uniqueness(&inv).expect_err("one template, two sites: rejected");
        assert!(msg.contains("pulsus_read_qlg_{}"), "{msg}");
        assert!(msg.contains("Composed"), "{msg}");
    }

    /// …and two composed names that merely share a stem are not, because
    /// the interpolated part is a nonce. The exclusion in the module doc,
    /// as a case.
    #[test]
    fn two_templates_sharing_a_stem_are_not_a_duplicate() {
        let mut inv = passing_inventory();
        inv.names[4].spelling = "pulsus_read_qlg_{}".to_string();
        inv.names[4].kind = Kind::Composed;
        inv.names[5].spelling = "pulsus_read_qlg_{stem}_{}".to_string();
        inv.names[5].kind = Kind::Composed;
        assert!(check_uniqueness(&inv).is_ok());
    }

    // -----------------------------------------------------------------
    // The floors, one at a time. Each fixture is a one-field mutation of
    // the control, so it proves something about the field it mutates.
    // -----------------------------------------------------------------

    /// An inventory that clears all five floors.
    fn passing_inventory() -> Inventory {
        Inventory {
            files_scanned: MIN_FILES_SCANNED,
            names: (0..MIN_CREATING_CALLS)
                .map(|i| Name {
                    spelling: format!("pulsus_fixture_{i}"),
                    kind: if i < MIN_COMPOSED_NAMES {
                        Kind::Composed
                    } else {
                        Kind::Fixed
                    },
                    file: format!("crates/c/tests/f{}.rs", i % MIN_CALLING_FILES),
                    line: i + 1,
                    composer: COMPOSER_CALLS[0].to_string(),
                })
                .collect(),
            quoted: (0..MIN_QUOTED_LOOKALIKES)
                .map(|i| ("crates/c/tests/q.rs".to_string(), i + 1))
                .collect(),
        }
    }

    /// Asserts `inv` breaches exactly `expected` and nothing else.
    fn breaches_exactly(inv: &Inventory, expected: Floor, needle: &str) {
        let breaches = check_floors(inv).expect_err("this fixture must breach a floor");
        let floors: Vec<Floor> = breaches.iter().map(|(f, _)| *f).collect();
        assert_eq!(floors, vec![expected], "breaches: {breaches:?}");
        assert!(breaches[0].1.contains(needle), "{}", breaches[0].1);
    }

    #[test]
    fn the_control_inventory_clears_every_floor() {
        assert!(
            check_floors(&passing_inventory()).is_ok(),
            "the per-floor fixtures are one-field mutations of this; if it does not pass, they \
             prove nothing about the field they mutate"
        );
    }

    /// A floor of zero admits everything, so each floor being non-zero is
    /// asserted at compile time rather than assumed.
    #[test]
    fn no_floor_is_set_to_zero() {
        const {
            assert!(MIN_FILES_SCANNED > 0);
            assert!(MIN_CREATING_CALLS > 0);
            assert!(MIN_CALLING_FILES > 0);
            assert!(MIN_COMPOSED_NAMES > 0);
            assert!(MIN_QUOTED_LOOKALIKES > 0);
            assert!(MIN_FILES_OUTSIDE > 0);
        }
    }

    #[test]
    fn the_files_scanned_floor_fires_on_its_own() {
        let inv = Inventory {
            files_scanned: MIN_FILES_SCANNED - 1,
            ..passing_inventory()
        };
        breaches_exactly(
            &inv,
            Floor::FilesScanned,
            &format!("scanned only {} test source files", MIN_FILES_SCANNED - 1),
        );
    }

    #[test]
    fn the_creating_call_floor_fires_on_its_own() {
        let mut inv = passing_inventory();
        // Drop one Fixed name: the composed count and the file spread
        // both stay clear of their floors.
        let at = inv
            .names
            .iter()
            .rposition(|n| n.kind == Kind::Fixed)
            .unwrap();
        inv.names.remove(at);
        breaches_exactly(
            &inv,
            Floor::CreatingCalls,
            &format!("found only {}", MIN_CREATING_CALLS - 1),
        );
    }

    #[test]
    fn the_calling_file_floor_fires_on_its_own() {
        let mut inv = passing_inventory();
        for n in &mut inv.names {
            if n.file == format!("crates/c/tests/f{}.rs", MIN_CALLING_FILES - 1) {
                n.file = "crates/c/tests/f0.rs".to_string();
            }
        }
        breaches_exactly(
            &inv,
            Floor::CallingFiles,
            &format!("only {} files compose", MIN_CALLING_FILES - 1),
        );
    }

    #[test]
    fn the_composed_name_floor_fires_on_its_own() {
        let mut inv = passing_inventory();
        let at = inv
            .names
            .iter()
            .position(|n| n.kind == Kind::Composed)
            .unwrap();
        inv.names[at].kind = Kind::Fixed;
        breaches_exactly(
            &inv,
            Floor::ComposedNames,
            &format!(
                "only {} names are composed at run time",
                MIN_COMPOSED_NAMES - 1
            ),
        );
    }

    #[test]
    fn the_quoted_lookalike_floor_fires_on_its_own() {
        let inv = Inventory {
            quoted: Vec::new(),
            ..passing_inventory()
        };
        breaches_exactly(
            &inv,
            Floor::QuotedLookalikes,
            "found only 0 composer call(s) that are quoted text",
        );
    }

    /// And the whole set together: a directory tree that really is empty
    /// — the exact shape a renamed `tests/` directory or a broken walk
    /// produces — breaches all five, each named.
    #[test]
    fn an_empty_tree_breaches_every_floor_and_names_each_one() {
        let empty = std::env::temp_dir().join(format!(
            "pulsus_db_uniq_empty_{}_{}",
            std::process::id(),
            line!()
        ));
        std::fs::create_dir_all(empty.join("crates")).expect("create empty scan root");
        let inv = scan_tree(&empty).expect("an empty tree has no violations to report");
        assert_eq!(inv.files_scanned, 0);
        assert_eq!(inv.names.len(), 0);
        assert_eq!(inv.quoted.len(), 0);
        let breaches = check_floors(&inv).expect_err("an empty tree must not pass");
        let floors: Vec<Floor> = breaches.iter().map(|(f, _)| *f).collect();
        assert_eq!(
            floors,
            vec![
                Floor::FilesScanned,
                Floor::CreatingCalls,
                Floor::CallingFiles,
                Floor::ComposedNames,
                Floor::QuotedLookalikes,
            ],
            "every floor must report, not just the first: {breaches:?}"
        );
        std::fs::remove_dir_all(&empty).ok();
    }

    /// The outside-the-tree rule's own finder: a composer call in a file
    /// that is not under `crates/*/tests` is reported, and the same text
    /// inside a string literal is not.
    #[test]
    fn the_outside_the_tree_rule_sees_a_call_in_src_and_not_one_in_a_literal() {
        let root = std::env::temp_dir().join(format!(
            "pulsus_db_uniq_outside_{}_{}",
            std::process::id(),
            line!()
        ));
        let src = root.join("crates/pulsus-thing/src");
        std::fs::create_dir_all(&src).expect("create fixture tree");
        std::fs::write(
            src.join("lib.rs"),
            "fn f() { let db = pulsus_testkit::test_db(\"pulsus_outside_it\"); }\n",
        )
        .expect("write call");
        std::fs::write(
            src.join("quoted.rs"),
            "fn f() { let s = \"pulsus_testkit::test_db(x)\"; }\n",
        )
        .expect("write quoted");
        let (files, sites) = composer_sites_outside_the_tree(&root);
        assert_eq!(files, 2, "both files walked");
        assert_eq!(sites.len(), 1, "{sites:?}");
        assert!(sites[0].contains("lib.rs:1"), "{sites:?}");
        std::fs::remove_dir_all(&root).ok();
    }

    /// …and the composer's own crate is the one path it skips.
    #[test]
    fn the_outside_the_tree_rule_skips_the_composer_crate() {
        let root = std::env::temp_dir().join(format!(
            "pulsus_db_uniq_crate_{}_{}",
            std::process::id(),
            line!()
        ));
        let src = root.join(COMPOSER_CRATE).join("src");
        std::fs::create_dir_all(&src).expect("create fixture tree");
        std::fs::write(
            src.join("lib.rs"),
            "fn f() { let db = pulsus_testkit::test_db(\"pulsus_inside_it\"); }\n",
        )
        .expect("write call");
        let (files, sites) = composer_sites_outside_the_tree(&root);
        assert_eq!(files, 0, "the composer crate is not walked for sites");
        assert!(sites.is_empty(), "{sites:?}");
        std::fs::remove_dir_all(&root).ok();
    }

    /// The exemption's own guard: the composer crate is exempt because it
    /// declares no dependencies, and that is read from its manifest.
    #[test]
    fn the_composer_crates_dependency_table_is_read_not_assumed() {
        let root = std::env::temp_dir().join(format!(
            "pulsus_db_uniq_deps_{}_{}",
            std::process::id(),
            line!()
        ));
        let dir = root.join(COMPOSER_CRATE);
        std::fs::create_dir_all(&dir).expect("create fixture tree");
        std::fs::write(
            dir.join("Cargo.toml"),
            "[package]\nname = \"x\"\n\n# a comment\n[dependencies]\n# none yet\n\n[dev-dependencies]\nserde = \"1\"\n",
        )
        .expect("write manifest");
        assert!(
            testkit_dependencies(&root).is_empty(),
            "a commented-out and empty table reads as no dependencies"
        );
        std::fs::write(
            dir.join("Cargo.toml"),
            "[package]\nname = \"x\"\n\n[dependencies]\nreqwest = \"0.12\"\n",
        )
        .expect("rewrite manifest");
        assert_eq!(
            testkit_dependencies(&root),
            vec!["reqwest = \"0.12\"".to_string()],
            "a dependency that could reach a server is reported"
        );
        std::fs::remove_dir_all(&root).ok();
    }
}
