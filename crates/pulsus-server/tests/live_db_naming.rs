//! The live-suite database-naming guard: no test under `crates/*/tests`
//! composes the name of a throwaway ClickHouse database itself.
//!
//! Hermetic — no server, no ClickHouse. Runs under the plain `ci` job's
//! `cargo test --workspace` / `cargo nextest run --workspace`, not behind
//! `PULSUS_TEST_CLICKHOUSE`.
//!
//! ## Why this exists
//!
//! Every live suite creates a throwaway database, seeds it, asserts, and
//! `DROP DATABASE IF EXISTS`es it. The names were hardcoded per *test* —
//! `let db = "pulsus_loki_push_sm_collision_it";` — which is fine while
//! every checkout runs its own ClickHouse container, and wrong the moment
//! two share one server: both runs pick the same database and the first
//! one to finish drops the other's data mid-assertion. The fix is a single
//! composer, [`pulsus_testkit::test_db`], which prefixes the name with
//! `PULSUS_TEST_CH_DATABASE_PREFIX`. A composer helps only if nothing
//! bypasses it, and "nothing bypasses it" is what this file checks.
//!
//! ## The reserved shape
//!
//! A **reserved name** is a `pulsus_`-prefixed snake_case token that has
//! `it` as one of its later words: `pulsus_read_it_s1_single`,
//! `pulsus_traces_tags_live_it_{nonce}`, `pulsus_clickhouse_it_roundtrip`.
//! That is the shape this project already used for every integration-test
//! database, table and `query_id`, and it is now reserved for names the
//! helper composes. It does not collide with anything else the tree
//! spells with a `pulsus_` prefix — crate paths (`pulsus_read::`), metric
//! names (`pulsus_label_cache_hits_total`), wire fields
//! (`pulsus_partial`), cluster names (`pulsus_test_cluster`) — none of
//! which has `it` as a word.
//!
//! ## The two rules, which close each other's holes
//!
//! 1. **Every reserved name is composed by the helper.** A reserved name
//!    appearing anywhere in a scanned file — in a string literal, in an
//!    identifier, inside a SQL fragment — must sit inside the argument
//!    list of a [`HELPER_CALLS`] call. Writing
//!    `let db = "pulsus_x_it";` is a hard failure naming file and line.
//! 2. **Every database binding goes through the helper.** A `let`/
//!    `const`/`static`/field/assignment site whose name's last snake-case
//!    word is `db` or `database`, and every `CLICKHOUSE_DB` environment
//!    setting, must either mention a helper call in its right-hand side or
//!    contain no database-shaped string literal beyond
//!    [`SHARED_DATABASES`] — and creating or dropping one of *those* is
//!    itself a failure, which is what makes them safe to name.
//!
//! Rule 1 alone cannot see a test database named outside the reserved
//! shape — `let db = "scratch";`. Rule 2 alone cannot see a reserved name
//! that never reaches a `db`-named binding — `spawn_ready(port,
//! "pulsus_x_it")`, `format!("INSERT INTO pulsus_x_it.log_samples …")`.
//! Together they leave no ordinary way to introduce a per-test database
//! whose name the prefix does not reach.
//!
//! ## Known boundary — what this cannot see
//!
//! The scan is textual and covers `crates/*/tests/**/*.rs` only.
//!
//! * **A database name outside the reserved shape, passed positionally.**
//!   `spawn_ready(port, "scratch")` binds nothing named `db` and carries
//!   no reserved name. Rule 1 is what makes the reserved shape the only
//!   comfortable way to name a test database, and rule 2 catches the
//!   binding form; a name that is both unreserved *and* never bound is
//!   invisible here.
//! * **A name assembled from non-literal parts** — `let db =
//!   scratch_name();`, or a name threaded in through a function parameter.
//!   Rule 2 sees no string literal and passes.
//! * **A helper call spelled another way.** [`HELPER_CALLS`] are matched
//!   as literal text, fully qualified. `use pulsus_testkit::test_db;`
//!   followed by a bare `test_db(…)` is *rejected*, deliberately: the
//!   qualified spelling is what makes "this really is the shared helper,
//!   not a local function of the same name" checkable without resolving
//!   names.
//! * **Anything outside `crates/*/tests`** — `xtask`'s benchmark
//!   databases, `e2e`'s compose-provisioned `pulsus` database, and any
//!   `#[cfg(test)]` module under `src/**`. Those are not run concurrently
//!   by two checkouts against one server today.
//! * **A name split across literals** — `concat!("pulsus_x", "_it")`.
//!
//! The floors below are what keeps a *silent* zero-finding pass from
//! looking like success. Each one is shown firing **on its own**, from a
//! fixture that clears the other three — `the_files_scanned_floor_fires_on_its_own`
//! and its three siblings — and
//! [`finder_tests::an_empty_tree_breaches_every_floor_and_names_each_one`]
//! shows all four reporting together. See the note on the floor constants
//! for why one empty-tree observation was not enough.

#[path = "support/source_scan.rs"]
mod source_scan;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use source_scan::{line_of, preprocess_views, rs_files_under, workspace_root};

/// The fully-qualified spellings that compose a per-run ClickHouse name.
/// Matched as literal text — see the boundary note on why the qualified
/// form is required.
const HELPER_CALLS: &[&str] = &[
    "pulsus_testkit::test_db(",
    "pulsus_testkit::test_ident(",
    "pulsus_testkit::TestDb::new(",
];

/// Databases a test may name directly, because no test in the tree
/// creates or drops one: `default` and `system` are ClickHouse's own, and
/// `pulsus` is the product's default database name, which the hermetic
/// SQL-rendering suites use as a `PlanCtx` field and never connect to.
///
/// That "no test creates or drops one" is not left as an assurance —
/// [`shared_database_ddl`] fails the build if a scanned file issues
/// `CREATE DATABASE`/`DROP DATABASE` against any of them, which is the
/// only way this exemption could turn into the collision it excuses.
const SHARED_DATABASES: &[&str] = &["default", "system", "pulsus"];

/// Floors. These exist so that a scan which suddenly matches *nothing* —
/// a renamed directory, a helper spelled a new way, a walk that silently
/// returns no files — fails instead of passing green over zero call
/// sites. Set below the real counts at the time of writing (179 files
/// scanned, 261 helper call sites in 53 files, 666 rule-2 naming sites)
/// with enough slack that ordinary deletions do not trip them.
///
/// [`check_inventory`] evaluates **all four** and reports every breach,
/// rather than returning on the first. That is not a nicety: with a
/// short-circuiting check, the only way to watch a floor fire is to point
/// the scan at an empty tree, where the *first* floor fails and the other
/// three never evaluate — so three of the four would be unobservable, and
/// a floor nobody can watch fire is the same shape as the vacuous pass it
/// exists to prevent. [`finder_tests`] additionally drives each floor on
/// its own, from a fixture that satisfies the other three (see
/// [`Floor`]'s variants for which test proves which).
const MIN_FILES_SCANNED: usize = 120;
const MIN_HELPER_CALLS: usize = 200;
const MIN_HELPER_CALL_FILES: usize = 40;
const MIN_BINDING_SITES: usize = 500;

/// The four floors, named so a breach can be asserted on individually
/// rather than by matching prose.
///
/// | variant | floor | demonstrated alone by |
/// |---|---|---|
/// | [`Floor::FilesScanned`] | [`MIN_FILES_SCANNED`] | `the_files_scanned_floor_fires_on_its_own` |
/// | [`Floor::HelperCalls`] | [`MIN_HELPER_CALLS`] | `the_helper_call_floor_fires_on_its_own` |
/// | [`Floor::HelperCallFiles`] | [`MIN_HELPER_CALL_FILES`] | `the_helper_call_file_floor_fires_on_its_own` |
/// | [`Floor::BindingSites`] | [`MIN_BINDING_SITES`] | `the_binding_site_floor_fires_on_its_own` |
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Floor {
    FilesScanned,
    HelperCalls,
    HelperCallFiles,
    BindingSites,
}

/// What one whole-tree scan found.
#[derive(Debug, Default)]
struct Inventory {
    files_scanned: usize,
    /// One entry per helper call site: `(file, line)`.
    helper_calls: Vec<(String, usize)>,
    /// How many `db`/`database` naming sites rule 2 inspected.
    binding_sites: usize,
}

// ---------------------------------------------------------------------
// Lexing helpers
// ---------------------------------------------------------------------

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// A `pulsus_`-prefixed token is a reserved test-object name iff one of
/// its words after the `pulsus` prefix is exactly `it`. Word-grained on
/// purpose: `pulsus_it246` and `pulsus_label_cache_hits_total` are not
/// reserved, `pulsus_read_it_s1` and `pulsus_traces_tags_it` are.
fn is_reserved_name(token: &str) -> bool {
    let Some(rest) = token.strip_prefix("pulsus_") else {
        return false;
    };
    rest.split('_').any(|w| w == "it")
}

/// A string literal is *database-shaped* if it could be a bare ClickHouse
/// database name written by hand: lower-case, at least two characters,
/// `[a-z][a-z0-9_]*`. Upper-case strings (environment variable names,
/// SQL keywords) and anything with a space, dot or quote are not.
fn is_database_shaped(literal: &str) -> bool {
    literal.len() >= 2
        && literal.starts_with(|c: char| c.is_ascii_lowercase())
        && literal
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !SHARED_DATABASES.contains(&literal)
}

/// Every `pulsus_…` token in `text`, as `(byte offset, token)`. Runs over
/// the comment-stripped view, so string literals *are* searched — a name
/// baked into a SQL fragment is exactly the hidden construction rule 1
/// looks for.
fn pulsus_tokens(text: &str) -> Vec<(usize, &str)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while let Some(rel) = text[i..].find("pulsus_") {
        let start = i + rel;
        if start > 0 && is_ident_byte(bytes[start - 1]) {
            i = start + 1;
            continue;
        }
        let mut end = start;
        while bytes.get(end).is_some_and(|&b| is_ident_byte(b)) {
            end += 1;
        }
        out.push((start, &text[start..end]));
        i = end.max(start + 1);
    }
    out
}

/// The byte span of the balanced parenthesised argument list that starts
/// at `open` (the index of `(`). Runs to the matching `)`, or to the end
/// of the text when the source is truncated.
fn arg_list_span(bytes: &[u8], open: usize) -> (usize, usize) {
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return (open, i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    (open, bytes.len())
}

/// Every helper call's argument-list span in `stripped`, plus the byte
/// offset of each call for line reporting.
fn helper_call_spans(stripped: &str) -> Vec<(usize, usize)> {
    let bytes = stripped.as_bytes();
    let mut spans = Vec::new();
    for call in HELPER_CALLS {
        let mut i = 0usize;
        while let Some(rel) = stripped[i..].find(call) {
            let at = i + rel;
            // `x.pulsus_testkit::…` cannot happen, but a longer identifier
            // ending in the same text could; require a non-identifier byte
            // before the match.
            let preceded_ok = at == 0 || !is_ident_byte(bytes[at - 1]);
            if preceded_ok {
                spans.push(arg_list_span(bytes, at + call.len() - 1));
            }
            i = at + call.len();
        }
    }
    spans.sort_unstable();
    spans
}

/// The right-hand side of a naming site: from `from`, up to the first
/// `;` or `,` at nesting depth zero, or to a closing delimiter that would
/// take the depth negative (which is how `fn f(db: &str)` stops at the
/// parameter type instead of swallowing the body).
fn rhs_span(bytes: &[u8], from: usize) -> (usize, usize) {
    let mut depth = 0i32;
    let mut i = from;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                depth -= 1;
                if depth < 0 {
                    return (from, i);
                }
            }
            b';' if depth == 0 => return (from, i),
            b',' if depth == 0 => return (from, i),
            _ => {}
        }
        i += 1;
    }
    (from, bytes.len())
}

/// Every plain (non-raw) string literal inside `stripped[lo..hi]`, as
/// `(offset, contents)`. Escapes are not decoded: a database name never
/// contains one, and a literal that does simply fails
/// [`is_database_shaped`].
fn string_literals(stripped: &str, lo: usize, hi: usize) -> Vec<(usize, &str)> {
    let bytes = stripped.as_bytes();
    let mut out = Vec::new();
    let mut i = lo;
    while i < hi {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        let start = i + 1;
        let mut j = start;
        while j < bytes.len() && bytes[j] != b'"' {
            if bytes[j] == b'\\' {
                j += 1;
            }
            j += 1;
        }
        if j >= hi {
            break;
        }
        out.push((start, &stripped[start..j.min(hi)]));
        i = j + 1;
    }
    out
}

// ---------------------------------------------------------------------
// The scan
// ---------------------------------------------------------------------

/// One site that names a database: a `db`/`database` binding, field or
/// assignment, or a `CLICKHOUSE_DB` environment setting.
#[derive(Debug)]
struct NamingSite {
    line: usize,
    /// What was written, for the error message.
    what: String,
    rhs: (usize, usize),
}

/// The identifier immediately before byte `end` (exclusive), if any.
fn ident_before(stripped: &str, end: usize) -> Option<(usize, &str)> {
    let bytes = stripped.as_bytes();
    let mut j = end;
    while j > 0 && bytes[j - 1].is_ascii_whitespace() {
        j -= 1;
    }
    let hi = j;
    while j > 0 && is_ident_byte(bytes[j - 1]) {
        j -= 1;
    }
    if j == hi {
        None
    } else {
        Some((j, &stripped[j..hi]))
    }
}

/// `true` when `ident`'s last snake-case word is `db` or `database` — the
/// names this project gives a variable holding a database name. Last word
/// rather than any word: `db_config` and `drop_db_if_stale` are not
/// themselves database names, `run_db`, `otlp_db` and `TEST_DB` are.
fn names_a_database(ident: &str) -> bool {
    let last = ident.rsplit('_').next().unwrap_or(ident);
    last.eq_ignore_ascii_case("db") || last.eq_ignore_ascii_case("database")
}

/// Every naming site in `stripped`.
fn naming_sites(stripped: &str) -> Vec<NamingSite> {
    let bytes = stripped.as_bytes();
    let mut out = Vec::new();

    // (a) `NAME = …` / `NAME : …` where NAME's last word is db/database.
    // The `:` form covers both `const DB: &str = …` (the RHS then spans
    // the type *and* the initialiser) and `database: …` struct fields.
    let mut i = 0usize;
    while i < bytes.len() {
        let sep = match bytes[i] {
            b'=' if bytes.get(i + 1) != Some(&b'=')
                && !matches!(
                    bytes.get(i.wrapping_sub(1)),
                    Some(b'<' | b'>' | b'!' | b'=')
                ) =>
            {
                true
            }
            b':' if bytes.get(i + 1) != Some(&b':')
                && !matches!(bytes.get(i.wrapping_sub(1)), Some(b':')) =>
            {
                true
            }
            _ => false,
        };
        if !sep {
            i += 1;
            continue;
        }
        if let Some((start, ident)) = ident_before(stripped, i)
            && names_a_database(ident)
        {
            out.push(NamingSite {
                line: line_of(stripped, start),
                what: ident.to_string(),
                rhs: rhs_span(bytes, i + 1),
            });
        }
        i += 1;
    }

    // (b) `.env("CLICKHOUSE_DB", …)` — the database the child server is
    // pointed at, which no binding in the test need ever hold.
    let needle = "\"CLICKHOUSE_DB\"";
    let mut i = 0usize;
    while let Some(rel) = stripped[i..].find(needle) {
        let at = i + rel;
        out.push(NamingSite {
            line: line_of(stripped, at),
            what: "CLICKHOUSE_DB".to_string(),
            rhs: rhs_span(bytes, at + needle.len() + 1),
        });
        i = at + needle.len();
    }

    out
}

/// Every `CREATE DATABASE`/`DROP DATABASE` in `stripped` whose target is
/// one of [`SHARED_DATABASES`], as `(offset, name)`.
///
/// This is what keeps the [`SHARED_DATABASES`] exemption honest: those
/// names are allowed *because* no test creates or drops them, and this is
/// the check that says so rather than the comment.
fn shared_database_ddl(stripped: &str) -> Vec<(usize, &'static str)> {
    let bytes = stripped.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while let Some(rel) = stripped[i..].find("DATABASE") {
        let at = i + rel;
        i = at + "DATABASE".len();
        // Only `CREATE`/`DROP` targets a database by name here; `ATTACH`,
        // `RENAME` and `SHOW` do not occur in this tree.
        let before = stripped[..at].trim_end();
        if !(before.ends_with("CREATE") || before.ends_with("DROP")) {
            continue;
        }
        let mut j = i;
        // Skip `IF EXISTS` / `IF NOT EXISTS`.
        loop {
            while bytes.get(j).is_some_and(|b| b.is_ascii_whitespace()) {
                j += 1;
            }
            let word_start = j;
            while bytes.get(j).is_some_and(|&b| is_ident_byte(b)) {
                j += 1;
            }
            let word = &stripped[word_start..j];
            if word.eq_ignore_ascii_case("if")
                || word.eq_ignore_ascii_case("not")
                || word.eq_ignore_ascii_case("exists")
            {
                continue;
            }
            if let Some(name) = SHARED_DATABASES.iter().find(|n| **n == word) {
                out.push((word_start, *name));
            }
            break;
        }
    }
    out
}

/// Scans one source file. `Err` carries every violation of rule 1 or rule
/// 2 found in it, each already prefixed with `file:line`.
fn scan_source(rel: &str, src: &str) -> Result<(Vec<usize>, usize), Vec<String>> {
    // `.0`: comments blanked, string/char literals intact. Rule 1 wants
    // string literals intact — a database name baked into a SQL fragment
    // is exactly the hidden construction it looks for — and wants comments
    // gone, because several suites' module docs quote example names.
    let (stripped, _blanked) = preprocess_views(src);

    let mut errors: Vec<String> = Vec::new();
    let spans = helper_call_spans(&stripped);
    let covered = |offset: usize| spans.iter().any(|&(lo, hi)| offset >= lo && offset < hi);

    // Rule 1: every reserved name sits inside a helper call.
    for (offset, token) in pulsus_tokens(&stripped) {
        if !is_reserved_name(token) || covered(offset) {
            continue;
        }
        errors.push(format!(
            "{rel}:{}: `{token}` is a reserved live-test object name but is not composed by \
             `pulsus_testkit::test_db`. Two checkouts sharing one ClickHouse would both use this \
             name and drop each other's data — write it as \
             `pulsus_testkit::test_db(\"{token}\")`.",
            line_of(&stripped, offset),
        ));
    }

    // Rule 2: every database naming site delegates to the helper.
    let sites = naming_sites(&stripped);
    for site in &sites {
        let (lo, hi) = site.rhs;
        if spans.iter().any(|&(slo, _)| slo >= lo && slo < hi) {
            continue;
        }
        for (offset, literal) in string_literals(&stripped, lo, hi) {
            if !is_database_shaped(literal) || covered(offset) {
                continue;
            }
            errors.push(format!(
                "{rel}:{}: `{}` is named a database and is given the literal `{literal}`, which \
                 `pulsus_testkit::test_db` did not compose. Only {SHARED_DATABASES:?} may be \
                 named directly; every throwaway database must carry \
                 `PULSUS_TEST_CH_DATABASE_PREFIX` or two checkouts sharing one ClickHouse \
                 collide.",
                site.line, site.what,
            ));
        }
    }

    // The [`SHARED_DATABASES`] exemption, checked rather than asserted.
    for (offset, name) in shared_database_ddl(&stripped) {
        errors.push(format!(
            "{rel}:{}: this creates or drops `{name}`, one of the databases every test is allowed \
             to name directly *because* no test creates or drops it. Give it a throwaway name \
             from `pulsus_testkit::test_db` instead, or the exemption becomes the collision it \
             was carved out to avoid.",
            line_of(&stripped, offset),
        ));
    }

    if errors.is_empty() {
        Ok((
            spans
                .iter()
                .map(|&(lo, _)| line_of(&stripped, lo))
                .collect(),
            sites.len(),
        ))
    } else {
        Err(errors)
    }
}

/// This file. Excluded from its own scan: its error messages and its
/// `finder_tests` fixtures deliberately contain reserved names in exactly
/// the shapes rule 1 rejects, so scanning itself would report its own
/// examples as violations. The exemption is safe by construction — this
/// file touches no ClickHouse and creates no database — and it cannot go
/// stale silently: rename the file and it stops matching, at which point
/// it *is* scanned and its fixtures fail loudly rather than quietly
/// widening the exemption. [`every_live_test_database_name_comes_from_the_helper`]
/// additionally asserts the path still resolves.
const SELF_PATH: &str = "crates/pulsus-server/tests/live_db_naming.rs";

/// Walks `root/crates/*/tests/**/*.rs`. Returns the inventory, or every
/// rule-1/rule-2 violation across the tree.
fn scan_tree(root: &Path) -> Result<Inventory, Vec<String>> {
    let mut inv = Inventory::default();
    let mut errors = Vec::new();
    let crates = root.join("crates");
    let mut crate_dirs: Vec<PathBuf> = std::fs::read_dir(&crates)
        .map(|rd| {
            rd.flatten()
                .map(|e| e.path())
                .filter(|p| p.is_dir())
                .collect()
        })
        .unwrap_or_default();
    crate_dirs.sort();
    for dir in crate_dirs {
        for file in rs_files_under(&dir.join("tests")) {
            let rel = file
                .strip_prefix(root)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            if rel == SELF_PATH {
                continue;
            }
            let Ok(src) = std::fs::read_to_string(&file) else {
                continue;
            };
            inv.files_scanned += 1;
            match scan_source(&rel, &src) {
                Ok((calls, sites)) => {
                    inv.helper_calls
                        .extend(calls.into_iter().map(|line| (rel.clone(), line)));
                    inv.binding_sites += sites;
                }
                Err(mut e) => errors.append(&mut e),
            }
        }
    }
    if errors.is_empty() {
        Ok(inv)
    } else {
        Err(errors)
    }
}

/// All four floors, over an already-scanned tree.
///
/// Every floor is evaluated; the error carries one entry per breach, in
/// [`Floor`] order. Deliberately not short-circuiting — see the note on
/// the floor constants.
fn check_inventory(inv: &Inventory) -> Result<(), Vec<(Floor, String)>> {
    let mut breaches = Vec::new();
    if inv.files_scanned < MIN_FILES_SCANNED {
        breaches.push((
            Floor::FilesScanned,
            format!(
                "scanned only {} test source files (floor {MIN_FILES_SCANNED}) — the walk found \
                 almost nothing, so a green result here would mean nothing was checked.",
                inv.files_scanned
            ),
        ));
    }
    if inv.helper_calls.len() < MIN_HELPER_CALLS {
        breaches.push((
            Floor::HelperCalls,
            format!(
                "found only {} `pulsus_testkit::test_db` call sites (floor {MIN_HELPER_CALLS}) — \
                 either the live suites were deleted or the helper is being spelled a way this \
                 scan does not match, in which case rule 1 is passing over names it never saw.",
                inv.helper_calls.len()
            ),
        ));
    }
    let files: BTreeSet<&str> = inv.helper_calls.iter().map(|(f, _)| f.as_str()).collect();
    if files.len() < MIN_HELPER_CALL_FILES {
        breaches.push((
            Floor::HelperCallFiles,
            format!(
                "only {} files compose a test database name (floor {MIN_HELPER_CALL_FILES}) — the \
                 scan is matching one file's shape and missing the rest.",
                files.len()
            ),
        ));
    }
    if inv.binding_sites < MIN_BINDING_SITES {
        breaches.push((
            Floor::BindingSites,
            format!(
                "rule 2 inspected only {} database naming sites (floor {MIN_BINDING_SITES}) — the \
                 site recogniser has stopped matching, so rule 2 is passing vacuously.",
                inv.binding_sites
            ),
        ));
    }
    if breaches.is_empty() {
        Ok(())
    } else {
        Err(breaches)
    }
}

// ---------------------------------------------------------------------
// The guard
// ---------------------------------------------------------------------

/// Every throwaway ClickHouse database a test under `crates/*/tests`
/// creates is named by [`pulsus_testkit::test_db`], so that
/// `PULSUS_TEST_CH_DATABASE_PREFIX` reaches all of them.
#[test]
fn every_live_test_database_name_comes_from_the_helper() {
    let root = workspace_root();
    assert!(
        root.join(SELF_PATH).is_file(),
        "{SELF_PATH} does not resolve — the one scan exemption names a file that no longer \
         exists, so it is silently exempting nothing (or, worse, has been widened)"
    );
    let inv = match scan_tree(&root) {
        Ok(inv) => inv,
        Err(errors) => panic!(
            "{} test database name(s) the prefix cannot reach:\n{}",
            errors.len(),
            errors.join("\n")
        ),
    };
    if let Err(breaches) = check_inventory(&inv) {
        let list = breaches
            .iter()
            .map(|(floor, msg)| format!("  {floor:?}: {msg}"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!(
            "{} of the 4 scan floors were breached — the scan checked far less than it should \
             have:\n{list}",
            breaches.len()
        );
    }
}

// ---------------------------------------------------------------------
// Validating the finder: each rule is shown rejecting the thing it is
// for, and the floors are shown firing on an empty tree. A guard that has
// never been observed to fail is indistinguishable from one that cannot.
// ---------------------------------------------------------------------

mod finder_tests {
    use super::*;

    fn errs(src: &str) -> Vec<String> {
        scan_source("t.rs", src).expect_err("expected the scan to reject this source")
    }

    fn accept(src: &str) -> (Vec<usize>, usize) {
        scan_source("t.rs", src).expect("expected the scan to accept this source")
    }

    #[test]
    fn the_helper_call_is_the_one_accepted_way_to_name_a_test_database() {
        let src = r#"
fn f() {
    let db = &pulsus_testkit::test_db("pulsus_read_it_s1_single");
    let run_db = pulsus_testkit::test_db(&format!("pulsus_read_it_qlg_{n}"));
    let table = &pulsus_testkit::test_ident("pulsus_clickhouse_it_roundtrip");
}
static DB: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_traces_search_it");
"#;
        let (calls, sites) = accept(src);
        assert_eq!(calls.len(), 4, "one span per helper call");
        assert!(sites >= 3, "rule 2 saw the db bindings: {sites}");
    }

    /// The shape that was actually shipped, and the reason for all of
    /// this: both rules see it, which is what "they close each other's
    /// holes" means when the two overlap.
    #[test]
    fn a_hardcoded_reserved_database_name_is_rejected() {
        let e = errs(r#"fn f() { let db = "pulsus_loki_push_sm_collision_it"; }"#);
        assert_eq!(e.len(), 2, "{e:?}");
        assert!(
            e.iter()
                .any(|m| m.contains("reserved live-test object name")),
            "{e:?}"
        );
        assert!(e.iter().any(|m| m.contains("named a database")), "{e:?}");
        assert!(e.iter().all(|m| m.contains("t.rs:1")), "{e:?}");
    }

    /// The shape rule 2 exists for: a database binding whose literal is
    /// not reserved-shaped, so rule 1 never sees it.
    #[test]
    fn an_unreserved_database_literal_in_a_db_binding_is_rejected() {
        let e = errs(r#"fn f() { let db = "scratch"; }"#);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("named a database"), "{}", e[0]);
    }

    /// …and the shape rule 1 exists for: a reserved name that never
    /// reaches a `db`-named binding, so rule 2 never sees it.
    #[test]
    fn a_reserved_name_passed_positionally_is_rejected() {
        let e = errs(r#"fn f() { spawn_ready(port, "pulsus_traces_api_it_live"); }"#);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("reserved live-test object name"), "{}", e[0]);
    }

    #[test]
    fn a_reserved_name_hidden_inside_a_sql_fragment_is_rejected() {
        let e = errs(r#"fn f() { exec("INSERT INTO pulsus_read_it_s2.log_samples VALUES"); }"#);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("reserved live-test object name"), "{}", e[0]);
    }

    #[test]
    fn a_clickhouse_db_environment_setting_must_use_the_helper() {
        let e = errs(r#"fn f() { cmd.env("CLICKHOUSE_DB", "scratch"); }"#);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("named a database"), "{}", e[0]);
        let (_, sites) = accept(
            r#"fn f() { cmd.env("CLICKHOUSE_DB", pulsus_testkit::test_db("pulsus_x_it")); }"#,
        );
        assert!(sites >= 1);
    }

    #[test]
    fn a_field_assignment_is_a_naming_site_too() {
        let e = errs(r#"fn f() { cfg.database = "scratch".to_string(); }"#);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("named a database"), "{}", e[0]);
    }

    /// The two databases the server provides itself are not throwaways.
    #[test]
    fn the_shared_databases_may_be_named_directly() {
        let (_, sites) = accept(
            r#"
fn f() {
    let db = "default";
    let cfg = ChConnConfig { database: "system".to_string() };
}
"#,
        );
        assert!(sites >= 2, "{sites}");
    }

    /// The connection database is chosen by the operator through an
    /// environment variable, not composed per test; its uppercase name is
    /// not database-shaped and must not trip rule 2.
    #[test]
    fn the_operator_supplied_connection_database_is_not_a_naming_site_violation() {
        accept(
            r#"
fn f() -> ChConnConfig {
    ChConnConfig {
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
    }
}
"#,
        );
    }

    /// A parameter named `db` is not a naming site: its type is the RHS,
    /// and the scan must stop at the closing paren rather than swallowing
    /// the function body.
    #[test]
    fn a_db_parameter_does_not_swallow_the_function_body() {
        accept(r#"async fn drop_db(db: &str) { let s = "scratch"; }"#);
    }

    /// `pulsus_` tokens that are not reserved names — crate paths, metric
    /// names, wire fields — pass untouched.
    #[test]
    fn a_pulsus_token_without_the_it_word_is_not_a_reserved_name() {
        accept(
            r#"
use pulsus_read::sql;
fn f() {
    let m = "pulsus_label_cache_hits_total";
    let c = "pulsus_test_cluster";
    let j = "pulsus_it246";
    assert!(json.get("pulsus_partial").is_none());
}
"#,
        );
        assert!(is_reserved_name("pulsus_read_it_s1"));
        assert!(is_reserved_name("pulsus_traces_tags_it"));
        assert!(!is_reserved_name("pulsus_it246"));
        assert!(!is_reserved_name("pulsus_label_cache_hits_total"));
        assert!(!is_reserved_name("read_it_s1"));
    }

    /// A reserved name quoted in a module doc is documentation, not a
    /// construction: comments are blanked before rule 1 runs.
    #[test]
    fn a_reserved_name_named_only_in_a_comment_is_not_a_construction() {
        accept("//! Seeds `pulsus_read_it_s2`, then drops it.\nfn f() {}");
    }

    /// A bare `test_db(…)` after `use pulsus_testkit::test_db;` is
    /// rejected on purpose — see the boundary note.
    #[test]
    fn an_unqualified_helper_call_does_not_satisfy_rule_one() {
        let e = errs(r#"fn f() { let db = test_db("pulsus_x_it"); }"#);
        assert!(
            e.iter()
                .any(|m| m.contains("reserved live-test object name")),
            "{e:?}"
        );
    }

    /// The exemption's own guard: `pulsus`, `default` and `system` are
    /// nameable *because* nothing creates or drops them, and that is a
    /// checked property, not a comment.
    #[test]
    fn creating_or_dropping_an_exempted_database_is_rejected() {
        for sql in [
            "CREATE DATABASE pulsus",
            "DROP DATABASE IF EXISTS pulsus",
            "DROP DATABASE default",
            "CREATE DATABASE IF NOT EXISTS system",
        ] {
            let e = errs(&format!(r#"fn f() {{ exec("{sql}"); }}"#));
            assert_eq!(e.len(), 1, "{sql}: {e:?}");
            assert!(e[0].contains("creates or drops"), "{sql}: {}", e[0]);
        }
        // The ordinary form — a name the helper composed — is untouched.
        accept(r#"fn f() { exec(&format!("DROP DATABASE IF EXISTS {db}")); }"#);
    }

    // -----------------------------------------------------------------
    // The floors, one at a time.
    //
    // Review finding (PR #424): pointing the scan at an empty tree used
    // to be the only way to watch a floor fire, and with a
    // short-circuiting check only the FIRST floor was observable that
    // way. Three of the four could have been set to zero and nothing
    // would have said so. Each floor now has a fixture that satisfies the
    // other three and breaches exactly that one, so every floor is
    // demonstrable on its own — and `check_inventory` reports all
    // breaches, so the empty tree names all four at once.
    // -----------------------------------------------------------------

    /// `helper_calls` spread over `files` distinct files, `calls` in
    /// total. `calls >= files` or the file count comes out short.
    fn calls_over_files(calls: usize, files: usize) -> Vec<(String, usize)> {
        assert!(calls >= files, "fixture needs at least one call per file");
        (0..calls)
            .map(|i| (format!("crates/c/tests/f{}.rs", i % files), i + 1))
            .collect()
    }

    /// An inventory that clears all four floors — the control every
    /// per-floor fixture is a one-field mutation of.
    fn passing_inventory() -> Inventory {
        Inventory {
            files_scanned: MIN_FILES_SCANNED,
            helper_calls: calls_over_files(MIN_HELPER_CALLS, MIN_HELPER_CALL_FILES),
            binding_sites: MIN_BINDING_SITES,
        }
    }

    /// Asserts `inv` breaches exactly `expected` and nothing else. The
    /// "and nothing else" half is what makes the fixture a proof about
    /// *that* floor rather than about the check as a whole.
    fn breaches_exactly(inv: &Inventory, expected: Floor, needle: &str) {
        let breaches = check_inventory(inv).expect_err("this fixture must breach a floor");
        let floors: Vec<Floor> = breaches.iter().map(|(f, _)| *f).collect();
        assert_eq!(floors, vec![expected], "breaches: {breaches:?}");
        assert!(breaches[0].1.contains(needle), "{}", breaches[0].1);
    }

    #[test]
    fn the_control_inventory_clears_every_floor() {
        assert!(
            check_inventory(&passing_inventory()).is_ok(),
            "the per-floor fixtures are one-field mutations of this; if it does not pass, they \
             prove nothing about the field they mutate"
        );
    }

    /// A floor of zero admits everything, so each floor being non-zero is
    /// asserted rather than assumed — at compile time, which is where a
    /// constant's value can be settled once and for all.
    #[test]
    fn no_floor_is_set_to_zero() {
        const {
            assert!(MIN_FILES_SCANNED > 0);
            assert!(MIN_HELPER_CALLS > 0);
            assert!(MIN_HELPER_CALL_FILES > 0);
            assert!(MIN_BINDING_SITES > 0);
        }
    }

    /// Floor 1 alone: the walk found almost no files, but everything it
    /// did find is in order.
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

    /// Floor 2 alone: plenty of files, still spread over enough of them,
    /// but one call site short — the shape of the helper being spelled a
    /// way this scan no longer matches.
    #[test]
    fn the_helper_call_floor_fires_on_its_own() {
        let inv = Inventory {
            helper_calls: calls_over_files(MIN_HELPER_CALLS - 1, MIN_HELPER_CALL_FILES),
            ..passing_inventory()
        };
        breaches_exactly(
            &inv,
            Floor::HelperCalls,
            &format!("found only {}", MIN_HELPER_CALLS - 1),
        );
    }

    /// Floor 3 alone: the call count is fine, but they have all collapsed
    /// into too few files — the shape of the scan matching one file's
    /// idiom and missing the rest.
    #[test]
    fn the_helper_call_file_floor_fires_on_its_own() {
        let inv = Inventory {
            helper_calls: calls_over_files(MIN_HELPER_CALLS, MIN_HELPER_CALL_FILES - 1),
            ..passing_inventory()
        };
        breaches_exactly(
            &inv,
            Floor::HelperCallFiles,
            &format!("only {} files compose", MIN_HELPER_CALL_FILES - 1),
        );
    }

    /// Floor 4 alone: rule 1's inventory is untouched, but rule 2's site
    /// recogniser has stopped matching, which is how rule 2 would go
    /// quietly vacuous while rule 1 kept the test green.
    #[test]
    fn the_binding_site_floor_fires_on_its_own() {
        let inv = Inventory {
            binding_sites: MIN_BINDING_SITES - 1,
            ..passing_inventory()
        };
        breaches_exactly(
            &inv,
            Floor::BindingSites,
            &format!("inspected only {}", MIN_BINDING_SITES - 1),
        );
    }

    /// And the whole set together: a directory tree that really is empty
    /// — the exact shape a renamed `tests/` directory or a broken walk
    /// produces — breaches **all four**, each named. Before the check
    /// stopped short-circuiting, this observed one of the four.
    #[test]
    fn an_empty_tree_breaches_every_floor_and_names_each_one() {
        let empty = std::env::temp_dir().join(format!(
            "pulsus_db_guard_empty_{}_{}",
            std::process::id(),
            line!()
        ));
        std::fs::create_dir_all(empty.join("crates")).expect("create empty scan root");
        let inv = scan_tree(&empty).expect("an empty tree has no violations to report");
        assert_eq!(inv.files_scanned, 0);
        assert_eq!(inv.helper_calls.len(), 0);
        assert_eq!(inv.binding_sites, 0);
        let breaches = check_inventory(&inv).expect_err("an empty tree must not pass");
        let floors: Vec<Floor> = breaches.iter().map(|(f, _)| *f).collect();
        assert_eq!(
            floors,
            vec![
                Floor::FilesScanned,
                Floor::HelperCalls,
                Floor::HelperCallFiles,
                Floor::BindingSites,
            ],
            "every floor must report, not just the first: {breaches:?}"
        );
        std::fs::remove_dir_all(&empty).ok();
    }
}

// ---------------------------------------------------------------------
// Issue #523 review round 1 — rule 3: a suite that has adopted the
// drop-on-entry-and-exit guard must not also acquire a name without it.
//
// `crates/pulsus-server/tests/support/live_db.rs`'s `ScopedDb` drops its
// database on entry AND on scope exit, so a test does not have to remember
// the second half. `traces_api_live.rs` takes it, and its `spawn_ready`
// now demands `&ScopedDb`, so a bare `pulsus_testkit::test_db(…)` handed
// to a server spawn does not compile.
//
// That leaves one path the type cannot reach: a name acquired and used
// WITHOUT spawning a server — a direct `ChClient` against the database,
// say. This rule closes it in source.
//
// What none of the three closes is a test that ENDS the guard's life on
// purpose: `std::mem::forget(db)` skips `Drop`, and this rule stays green
// because the acquisition is still written the guarded way. Measured in
// the #523 review round 2 — `23 tests run: 23 passed` here, and a database
// left resident. Left open deliberately; the failure being guarded against
// is a test that does not clean up, not one that arranges not to.
// ---------------------------------------------------------------------

/// The guarded acquisition, matched as literal text for the same reason
/// [`HELPER_CALLS`] are.
const SCOPED_ACQUISITION: &str = "ScopedDb::fresh(";

/// Rule 3, keyed on the FILE'S OWN choice rather than on a list of file
/// names: **a scanned file that uses `ScopedDb` at all must obtain every
/// database name through it.**
///
/// Keying it this way is deliberate. A per-file exemption list would make
/// the rule true by naming which files it applies to, and the next suite
/// to adopt the guard would be outside it by default — the shape where a
/// carve-out becomes the place the defect lives. As written, adopting the
/// guard is what opts a suite in, and the sixteen suites that have not
/// adopted it are not silently declared compliant: they are simply not
/// claimed to be, which is what the notes on this issue say.
///
/// What it establishes and what it does not: every `pulsus_testkit::test_db(`
/// in such a file is immediately preceded by `ScopedDb::fresh(`. A name
/// assembled some other way, or one threaded in through a parameter, is
/// the boundary rule 1 and rule 2 already state for themselves.
#[test]
fn a_suite_that_uses_the_scoped_guard_uses_it_for_every_name() {
    let root = workspace_root();
    let mut offenders: Vec<String> = Vec::new();
    let mut adopting_files = 0usize;
    let mut guarded_sites = 0usize;

    let crates = root.join("crates");
    let mut crate_dirs: Vec<PathBuf> = std::fs::read_dir(&crates)
        .map(|rd| {
            rd.flatten()
                .map(|e| e.path())
                .filter(|p| p.is_dir())
                .collect()
        })
        .unwrap_or_default();
    crate_dirs.sort();
    for dir in crate_dirs {
        for file in rs_files_under(&dir.join("tests")) {
            let rel = file
                .strip_prefix(&root)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            if rel == SELF_PATH || rel.ends_with("support/live_db.rs") {
                continue;
            }
            let Ok(src) = std::fs::read_to_string(&file) else {
                continue;
            };
            // Comments blanked, string literals intact: a doc comment
            // showing the guarded form must neither opt a file in nor
            // satisfy the rule for it.
            let (stripped, _) = preprocess_views(&src);
            if !stripped.contains(SCOPED_ACQUISITION) {
                continue;
            }
            adopting_files += 1;
            let needle = "pulsus_testkit::test_db(";
            let mut from = 0usize;
            while let Some(at) = stripped[from..].find(needle) {
                let at = from + at;
                from = at + needle.len();
                if stripped[..at].trim_end().ends_with(SCOPED_ACQUISITION) {
                    guarded_sites += 1;
                } else {
                    offenders.push(format!("{rel}:{}", line_of(&stripped, at)));
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "these suites use the drop-on-entry-and-exit guard but acquire a database name \
         without it, so that database survives the run and the next run reads doubled rows \
         (issue #523): {offenders:?}. Wrap the name: \
         `ScopedDb::fresh(pulsus_testkit::test_db(\"…\")).await`."
    );

    // Floors. The rule above is satisfied by a tree in which nothing uses
    // the guard, which is the state this issue found and fixed; without
    // these, deleting every guard would pass it green.
    assert!(
        adopting_files >= 1,
        "no scanned file uses {SCOPED_ACQUISITION} any more — the guard this rule exists to \
         enforce has been removed from the tree, and the rule now checks nothing"
    );
    assert!(
        guarded_sites >= 10,
        "only {guarded_sites} guarded acquisitions remain, below the 10 that adopted the \
         guard in issue #523 — a live test has dropped back to an unguarded name"
    );
}
