//! The live-suite fixed-port guard: every listener port a `crates/*/tests`
//! test binds is declared exactly once across the whole workspace.
//!
//! Hermetic — no server, no ClickHouse. Runs under the plain `ci` job's
//! `cargo test --workspace` / `cargo nextest run --workspace`, not behind
//! `PULSUS_TEST_CLICKHOUSE`.
//!
//! ## Why this exists
//!
//! Live suites bind fixed loopback ports rather than ephemeral ones: the
//! test spawns the server, then talks to it by number. Port choice was a
//! hand-maintained convention — each suite's module doc claimed "distinct
//! from every other live suite's fixed ports (31100-31117)" and each new
//! suite re-derived that claim by reading the others. Nothing checked it.
//! Eighteen port values had ended up declared by two or three sites; run
//! in parallel (which is how CI and `cargo nextest` run them, each test in
//! its own process) the second binder gets `EADDRINUSE` and an unrelated
//! pull request goes red. Renumbering alone repairs today's list and the
//! next batch arrives silently, so the check is the deliverable and the
//! renumbering is what makes it pass.
//!
//! ## The two rules, which close each other's holes
//!
//! A scan for one syntactic shape passes vacuously the moment a port is
//! written a different way — as a `const`, by arithmetic, inside a URL
//! string, in hex. All four of those existed here. So the guard enforces
//! a *pair* of rules over [`RESERVED_PORTS`] (31000-31999, reserved for
//! exactly this purpose):
//!
//! 1. **Every port declaration is a literal in the reserved range.** A
//!    binding whose snake-case name contains the word `port` must be
//!    `let`/`const`/`static NAME [: T] = <integer literal>;`, or a bare
//!    alias of another port declaration in the same file. Anything else —
//!    `port + 1`, `base_port()`, `env::var(..)` — is a hard failure naming
//!    the file, line and right-hand side.
//! 2. **Every reserved-range literal is a port declaration.** Any integer
//!    literal whose value lands in 31000-31999 anywhere in a scanned file
//!    — passed straight to a function, embedded in a URL string, spelled
//!    `0x7A6C` — must be the right-hand side of a rule-1 declaration.
//!
//! Rule 1 alone cannot see a port that never gets a `port`-named binding;
//! rule 2 alone cannot see a port declared outside the reserved range.
//! Together they leave no way to introduce a fixed listener port that the
//! uniqueness check does not then compare against every other one.
//!
//! ## Known boundary
//!
//! The scan is textual and covers `crates/*/tests/**/*.rs` only. It does
//! not see: a port assembled at runtime from non-literal parts (rule 1
//! rejects the binding, but a value threaded through a helper *parameter*
//! never appears as a binding at all); a macro that expands to a port; a
//! reserved-range value hidden inside an identifier-prefixed string
//! (`"x31140"`); a listener in `src/**` under `#[cfg(test)]`.
//!
//! `src/**` is deliberately out of scope rather than merely unscanned:
//! pointing the walk at it was tried, and rule 2 immediately fired on
//! `crates/pulsus-read/src/traces/metrics_plan.rs:1065`, where `31952ms`
//! is a *duration* inside a string. Reserving 31000-31999 is a rule the
//! test tree can keep and production source cannot, so the guard stops at
//! `tests/`.
//!
//! The floors below are what keeps a *silent* zero-finding pass from
//! looking like success, and
//! [`the_floors_reject_a_tree_with_nothing_in_it`] proves they fire.

#[path = "support/source_scan.rs"]
mod source_scan;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

use source_scan::{line_of, preprocess_views, rs_files_under, workspace_root};

/// The port range reserved for live-suite fixed listeners. Both rules in
/// the module doc are stated over this range; a fixed test port outside it
/// is refused by [`check_inventory`] precisely because rule 2 could not
/// then see its neighbours.
const RESERVED_PORTS: RangeInclusive<u32> = 31_000..=31_999;

/// Floors. These exist so that a scan which suddenly matches *nothing* —
/// a renamed directory, a declaration spelled a new way, a walk that
/// silently returns no files — fails instead of passing green over zero
/// declarations. They are set below the real counts at the time of
/// writing (174 files scanned, 88 declarations, 15 declaring files) with
/// enough slack that ordinary deletions do not trip them.
const MIN_FILES_SCANNED: usize = 120;
const MIN_DECLARATIONS: usize = 70;
const MIN_DECLARING_FILES: usize = 12;

/// One fixed port declaration: `let port = 31_140;` and friends.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Decl {
    file: String,
    line: usize,
    name: String,
    port: u32,
}

/// What one whole-tree scan found.
#[derive(Debug, Default)]
struct Inventory {
    files_scanned: usize,
    decls: Vec<Decl>,
}

// ---------------------------------------------------------------------
// Lexing helpers
// ---------------------------------------------------------------------

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// A binding name is a port declaration iff one of its snake-case words is
/// `port`. Word-grained rather than substring-grained on purpose: `ported`
/// and `supported` are not ports, `ALIAS_PORT` and `wide_port` are.
fn names_a_port(ident: &str) -> bool {
    ident.split('_').any(|w| w.eq_ignore_ascii_case("port"))
}

/// Parses the integer literal starting at `bytes[i]`, which the caller has
/// already established begins a literal (a digit not preceded by an
/// identifier byte or `.`). Returns `(value, end_offset)`; `None` when the
/// literal does not fit in `u32` (never a port).
///
/// Handles `31_140`, `31140`, `31_140u16`, `0x7A6C`, `0b…`, `0o…` — the
/// last three because rule 2 would otherwise be evadable by spelling the
/// same port in another radix.
fn parse_int_literal(bytes: &[u8], i: usize) -> Option<(u32, usize)> {
    let (radix, mut j) = match (bytes.get(i), bytes.get(i + 1)) {
        (Some(b'0'), Some(b'x' | b'X')) => (16u32, i + 2),
        (Some(b'0'), Some(b'b' | b'B')) => (2, i + 2),
        (Some(b'0'), Some(b'o' | b'O')) => (8, i + 2),
        _ => (10, i),
    };
    let start = j;
    let mut digits = String::new();
    while let Some(&b) = bytes.get(j) {
        if b == b'_' {
            j += 1;
            continue;
        }
        if (b as char).is_digit(radix) {
            digits.push(b as char);
            j += 1;
            continue;
        }
        break;
    }
    if digits.is_empty() {
        // `0x` with no digits, or a bare `_`: not a literal we can read.
        return if radix == 10 { None } else { Some((0, start)) };
    }
    // Consume a type suffix (`u16`, `i32`, …) so the caller resumes past it.
    while bytes.get(j).is_some_and(|&b| is_ident_byte(b)) {
        j += 1;
    }
    u32::from_str_radix(&digits, radix).ok().map(|v| (v, j))
}

/// The right-hand side of a `port`-named binding, classified.
#[derive(Debug)]
enum Rhs {
    /// An integer literal, with the byte offset it starts at (rule 2 uses
    /// the offset to know this literal is accounted for).
    Literal(u32, usize),
    /// A bare identifier — `let port = ALIAS_PORT;`.
    Alias(String),
    /// Anything else: rejected.
    Unrecognised,
}

fn classify_rhs(stripped: &str, span: (usize, usize)) -> Rhs {
    let (lo, hi) = span;
    let raw = &stripped[lo..hi];
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Rhs::Unrecognised;
    }
    let offset = lo + raw.find(trimmed).unwrap_or(0);
    let bytes = trimmed.as_bytes();
    if bytes[0].is_ascii_digit() {
        if let Some((value, end)) = parse_int_literal(bytes, 0)
            && end == trimmed.len()
        {
            return Rhs::Literal(value, offset);
        }
        return Rhs::Unrecognised;
    }
    if (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes.iter().all(|&b| is_ident_byte(b))
    {
        return Rhs::Alias(trimmed.to_string());
    }
    Rhs::Unrecognised
}

// ---------------------------------------------------------------------
// The scan
// ---------------------------------------------------------------------

/// One `let`/`const`/`static` binding site the scanner could parse.
struct Binding {
    name: String,
    /// Byte span of the right-hand side, exclusive of `=` and `;`.
    rhs: (usize, usize),
    /// Byte offset of the keyword, for line reporting.
    at: usize,
}

/// Every simple `let`/`const`/`static NAME [: T] = RHS;` in `stripped`.
///
/// Byte-indexed throughout: `stripped` keeps string literals intact, so it
/// contains multi-byte characters and no index here may be assumed to sit
/// on a `char` boundary.
///
/// Deliberately gives up (rather than guessing) on destructuring patterns
/// and `if let` — `let Some(x) = …`, `let (a, b) = …` — because a binding
/// with no plain name cannot be a port declaration under rule 1, and rule
/// 2 still sees any reserved-range literal inside it.
fn bindings(stripped: &str) -> Vec<Binding> {
    let bytes = stripped.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let kw = ["let", "const", "static"].into_iter().find(|kw| {
            bytes[i..].starts_with(kw.as_bytes())
                && (i == 0 || !is_ident_byte(bytes[i - 1]))
                && !bytes.get(i + kw.len()).is_some_and(|&b| is_ident_byte(b))
        });
        let Some(kw) = kw else {
            i += 1;
            continue;
        };
        let at = i;
        let mut j = i + kw.len();
        let skip_ws = |bytes: &[u8], mut j: usize| {
            while bytes.get(j).is_some_and(|b| b.is_ascii_whitespace()) {
                j += 1;
            }
            j
        };
        j = skip_ws(bytes, j);
        if bytes[j..].starts_with(b"mut") && !bytes.get(j + 3).is_some_and(|&b| is_ident_byte(b)) {
            j = skip_ws(bytes, j + 3);
        }
        let name_start = j;
        while bytes.get(j).is_some_and(|&b| is_ident_byte(b)) {
            j += 1;
        }
        let name = &stripped[name_start..j];
        if name.is_empty() || name.as_bytes()[0].is_ascii_digit() {
            i = at + kw.len();
            continue;
        }
        j = skip_ws(bytes, j);
        // `NAME : TYPE =` or `NAME =`. A `:` type ascription runs to the
        // first `=` that is not part of `==`/`=>`/`<=`/`>=`/`!=`.
        if bytes.get(j) == Some(&b':') {
            j += 1;
            while j < bytes.len() {
                if bytes[j] == b';' || bytes[j] == b'{' {
                    break;
                }
                if bytes[j] == b'='
                    && bytes.get(j + 1) != Some(&b'=')
                    && bytes.get(j + 1) != Some(&b'>')
                    && !matches!(bytes.get(j - 1), Some(b'<' | b'>' | b'!' | b'='))
                {
                    break;
                }
                j += 1;
            }
        }
        if bytes.get(j) != Some(&b'=') || bytes.get(j + 1) == Some(&b'=') {
            i = at + kw.len();
            continue;
        }
        let rhs_start = j + 1;
        let Some(semi) = stripped[rhs_start..].find(';').map(|k| rhs_start + k) else {
            i = at + kw.len();
            continue;
        };
        out.push(Binding {
            name: name.to_string(),
            rhs: (rhs_start, semi),
            at,
        });
        i = semi + 1;
    }
    out
}

/// Scans one source file. `Err` carries every violation of rule 1 or rule
/// 2 found in it, each already prefixed with `file:line`.
fn scan_source(rel: &str, src: &str) -> Result<Vec<Decl>, Vec<String>> {
    // `.0`: comments blanked, string/char literals intact. Rule 2 wants
    // string literals intact — a port baked into a URL is exactly the
    // hidden declaration it is looking for — and wants comments gone,
    // because several suites' module docs enumerate port ranges in prose.
    let (stripped, _blanked) = preprocess_views(src);

    let mut errors: Vec<String> = Vec::new();
    let mut decls: Vec<Decl> = Vec::new();
    let mut aliases: Vec<(String, usize)> = Vec::new();
    // Byte offsets of literals that rule 1 accounted for.
    let mut accounted: BTreeSet<usize> = BTreeSet::new();

    for b in bindings(&stripped) {
        if !names_a_port(&b.name) {
            continue;
        }
        let line = line_of(&stripped, b.at);
        match classify_rhs(&stripped, b.rhs) {
            Rhs::Literal(port, offset) => {
                accounted.insert(offset);
                decls.push(Decl {
                    file: rel.to_string(),
                    line,
                    name: b.name,
                    port,
                });
            }
            Rhs::Alias(target) => aliases.push((target, line)),
            Rhs::Unrecognised => errors.push(format!(
                "{rel}:{line}: `{}` is a port declaration whose value is not an integer literal \
                 (`{}`). A fixed test port must be written `let {} = 31_NNN;` so the uniqueness \
                 guard can see it; a port built by arithmetic or a call is invisible to it.",
                b.name,
                stripped[b.rhs.0..b.rhs.1].trim(),
                b.name,
            )),
        }
    }

    // An alias must point at a port declaration in the same file; otherwise
    // it is a value from somewhere the scan never looked.
    let local: BTreeSet<&str> = decls.iter().map(|d| d.name.as_str()).collect();
    for (target, line) in &aliases {
        if !local.contains(target.as_str()) {
            errors.push(format!(
                "{rel}:{line}: port alias `{target}` does not resolve to a port declaration in \
                 this file — the value it carries is never checked for uniqueness."
            ));
        }
    }

    // Rule 2: no reserved-range literal anywhere except at a declaration.
    let bytes = stripped.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if !bytes[i].is_ascii_digit()
            || (i > 0 && (is_ident_byte(bytes[i - 1]) || bytes[i - 1] == b'.'))
        {
            i += 1;
            continue;
        }
        let Some((value, end)) = parse_int_literal(bytes, i) else {
            i += 1;
            continue;
        };
        if RESERVED_PORTS.contains(&value) && !accounted.contains(&i) {
            errors.push(format!(
                "{rel}:{}: the literal `{}` is in the reserved live-test port range {}-{} but is \
                 not a `let`/`const` declaration whose name contains `port`. Bind it \
                 (`let port = {value};`) so the uniqueness guard compares it against every other \
                 suite's ports.",
                line_of(&stripped, i),
                &stripped[i..end],
                RESERVED_PORTS.start(),
                RESERVED_PORTS.end(),
            ));
        }
        i = end.max(i + 1);
    }

    if errors.is_empty() {
        Ok(decls)
    } else {
        Err(errors)
    }
}

/// This file. Excluded from its own scan: its error messages and its
/// `finder_tests` fixtures deliberately contain reserved-range literals in
/// exactly the shapes rule 2 rejects, so scanning itself would report its
/// own examples as violations. The exemption is safe by construction —
/// this file spawns no server and declares no live port — and it cannot go
/// stale silently: rename the file and it stops matching, at which point
/// it *is* scanned and its fixtures fail loudly rather than quietly
/// widening the exemption. [`every_live_test_fixed_port_is_declared_exactly_once`]
/// additionally asserts the path still resolves.
const SELF_PATH: &str = "crates/pulsus-server/tests/live_port_uniqueness.rs";

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
                Ok(decls) => inv.decls.extend(decls),
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

/// The floors and the uniqueness property, over an already-scanned tree.
fn check_inventory(inv: &Inventory) -> Result<(), String> {
    if inv.files_scanned < MIN_FILES_SCANNED {
        return Err(format!(
            "scanned only {} test source files (floor {MIN_FILES_SCANNED}) — the walk found \
             almost nothing, so a green result here would mean nothing was checked.",
            inv.files_scanned
        ));
    }
    if inv.decls.len() < MIN_DECLARATIONS {
        return Err(format!(
            "found only {} fixed-port declarations (floor {MIN_DECLARATIONS}) — either the live \
             suites were deleted or the declaration shape changed and this scan no longer \
             matches it.",
            inv.decls.len()
        ));
    }
    let declaring: BTreeSet<&str> = inv.decls.iter().map(|d| d.file.as_str()).collect();
    if declaring.len() < MIN_DECLARING_FILES {
        return Err(format!(
            "only {} files declare a fixed port (floor {MIN_DECLARING_FILES}) — the scan is \
             matching one file's shape and missing the rest.",
            declaring.len()
        ));
    }

    let outside: Vec<&Decl> = inv
        .decls
        .iter()
        .filter(|d| !RESERVED_PORTS.contains(&d.port))
        .collect();
    if !outside.is_empty() {
        let list = outside
            .iter()
            .map(|d| format!("  {}:{} `{}` = {}", d.file, d.line, d.name, d.port))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!(
            "these fixed test ports fall outside the reserved range {}-{}:\n{list}\nOutside the \
             reserved range the second rule (every reserved-range literal is a declaration) \
             cannot see a port written any other way, so the guard would have a blind spot.",
            RESERVED_PORTS.start(),
            RESERVED_PORTS.end(),
        ));
    }

    let mut grouped: BTreeMap<u32, Vec<&Decl>> = BTreeMap::new();
    for d in &inv.decls {
        grouped.entry(d.port).or_default().push(d);
    }
    let dupes: Vec<(&u32, &Vec<&Decl>)> = grouped.iter().filter(|(_, v)| v.len() > 1).collect();
    if dupes.is_empty() {
        return Ok(());
    }
    let mut msg = format!(
        "{} port value(s) are declared by more than one live test. Run in parallel — which is \
         how CI and `cargo nextest` run them — the second binder fails with EADDRINUSE and \
         reddens an unrelated change:\n",
        dupes.len()
    );
    for (port, sites) in &dupes {
        msg.push_str(&format!("  {port}:\n"));
        for d in sites.iter() {
            msg.push_str(&format!("    {}:{} `{}`\n", d.file, d.line, d.name));
        }
    }
    // Suggest from the lowest port already in use upwards, not from the
    // bottom of the reserved range: the whole allocation sits in one band
    // and a suggestion 100 below it is not one anybody would take.
    let used: BTreeSet<u32> = grouped.keys().copied().collect();
    let from = used
        .iter()
        .next()
        .copied()
        .unwrap_or(*RESERVED_PORTS.start());
    let free: Vec<String> = RESERVED_PORTS
        .filter(|p| *p >= from && !used.contains(p))
        .take(12)
        .map(|p| format!("{}_{:03}", p / 1000, p % 1000))
        .collect();
    msg.push_str(&format!("free ports, lowest first: {}", free.join(", ")));
    Err(msg)
}

// ---------------------------------------------------------------------
// The guard
// ---------------------------------------------------------------------

/// Every fixed listener port declared by a test under `crates/*/tests`
/// is declared exactly once, and every reserved-range literal in those
/// files is such a declaration.
#[test]
fn every_live_test_fixed_port_is_declared_exactly_once() {
    let root = workspace_root();
    assert!(
        root.join(SELF_PATH).is_file(),
        "{SELF_PATH} does not resolve — the one scan exemption names a file that no longer \
         exists, so it is silently exempting nothing (or, worse, has been widened)"
    );
    let inv = match scan_tree(&root) {
        Ok(inv) => inv,
        Err(errors) => panic!(
            "{} fixed-port declaration(s) the uniqueness guard cannot read:\n{}",
            errors.len(),
            errors.join("\n")
        ),
    };
    if let Err(msg) = check_inventory(&inv) {
        panic!("{msg}");
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

    fn ports(src: &str) -> Vec<u32> {
        scan_source("t.rs", src)
            .expect("expected the scan to accept this source")
            .into_iter()
            .map(|d| d.port)
            .collect()
    }

    #[test]
    fn the_recognised_declaration_forms_are_all_seen() {
        let src = r#"
const PORT: u16 = 31_130;
static ZIPKIN_PORT: u16 = 31_131;
fn f() {
    let port = 31_132;
    let port: u16 = 31_133;
    let mut wide_port = 31_134;
    let second_configured_port: u16 = 31_135;
    let port = PORT;
}
"#;
        assert_eq!(
            ports(src),
            vec![31_130, 31_131, 31_132, 31_133, 31_134, 31_135],
            "one declaration per literal-valued port binding; the alias is not a declaration"
        );
    }

    #[test]
    fn a_binding_whose_name_merely_contains_port_is_not_a_declaration() {
        // `ported` is a real binding in `pulsus-read`'s provenance test.
        let src = "fn f() { let ported = 3; let reported = 4; }";
        assert!(ports(src).is_empty());
    }

    #[test]
    fn an_arithmetic_port_declaration_is_rejected() {
        let e = errs("fn f() { let port = 31_136; let wide_port = port + 1; }");
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("not an integer literal"), "{}", e[0]);
        assert!(e[0].contains("t.rs:1"), "{}", e[0]);
    }

    #[test]
    fn a_computed_port_declaration_is_rejected() {
        let e = errs("fn f() { let port = base_port(); }");
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("not an integer literal"), "{}", e[0]);
    }

    #[test]
    fn a_bare_reserved_literal_passed_as_an_argument_is_rejected() {
        let e = errs("fn f() { spawn_ready(31_140, db); }");
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("reserved live-test port range"), "{}", e[0]);
    }

    #[test]
    fn a_port_hidden_inside_a_url_string_is_rejected() {
        let e = errs(r#"fn f() { get("http://127.0.0.1:31141/ready"); }"#);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("reserved live-test port range"), "{}", e[0]);
    }

    #[test]
    fn a_port_spelled_in_hex_is_rejected() {
        // 0x79A5 == 31141.
        let e = errs("fn f() { spawn_ready(0x79A5, db); }");
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("reserved live-test port range"), "{}", e[0]);
    }

    #[test]
    fn a_port_range_named_only_in_a_comment_is_not_a_declaration() {
        let src = "//! Ports 31140-31147, distinct from every other suite.\nfn f() {}";
        assert!(scan_source("t.rs", src).is_ok());
    }

    #[test]
    fn an_alias_to_a_name_this_file_never_declares_is_rejected() {
        let e = errs("fn f() { let port = SOMEWHERE_ELSE; }");
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("does not resolve"), "{}", e[0]);
    }

    #[test]
    fn a_duplicate_port_is_rejected_wherever_the_two_sites_are() {
        let mut inv = Inventory {
            files_scanned: MIN_FILES_SCANNED,
            decls: Vec::new(),
        };
        for i in 0..MIN_DECLARATIONS as u32 {
            inv.decls.push(Decl {
                file: format!("crates/c/tests/f{}.rs", i % MIN_DECLARING_FILES as u32),
                line: 1,
                name: "port".into(),
                port: 31_100 + i,
            });
        }
        assert!(check_inventory(&inv).is_ok(), "the control must pass");

        // Same value, two different files — the shape that was actually
        // shipped (loki_push_live.rs and tls_live.rs both bound 31160).
        inv.decls[7].port = inv.decls[3].port;
        let msg = check_inventory(&inv).expect_err("a duplicate must be rejected");
        assert!(msg.contains("declared by more than one live test"), "{msg}");
        assert!(msg.contains(&format!("{}", inv.decls[3].port)), "{msg}");
        assert!(msg.contains("free ports, lowest first"), "{msg}");
    }

    #[test]
    fn a_port_outside_the_reserved_range_is_rejected() {
        let mut inv = Inventory {
            files_scanned: MIN_FILES_SCANNED,
            decls: (0..MIN_DECLARATIONS as u32)
                .map(|i| Decl {
                    file: format!("crates/c/tests/f{}.rs", i % MIN_DECLARING_FILES as u32),
                    line: 1,
                    name: "port".into(),
                    port: 31_100 + i,
                })
                .collect(),
        };
        inv.decls[0].port = 18_080;
        let msg = check_inventory(&inv).expect_err("an out-of-range port must be rejected");
        assert!(msg.contains("outside the reserved range"), "{msg}");
    }

    /// The floor half of the guard: a scan that matches nothing must fail,
    /// not pass. Pointed at a directory tree that really is empty — the
    /// exact shape a renamed `tests/` directory or a broken walk produces.
    #[test]
    fn the_floors_reject_a_tree_with_nothing_in_it() {
        let empty = std::env::temp_dir().join(format!(
            "pulsus_port_guard_empty_{}_{}",
            std::process::id(),
            line!()
        ));
        std::fs::create_dir_all(empty.join("crates")).expect("create empty scan root");
        let inv = scan_tree(&empty).expect("an empty tree has no violations to report");
        assert_eq!(inv.files_scanned, 0);
        assert_eq!(inv.decls.len(), 0);
        let msg = check_inventory(&inv).expect_err("an empty tree must not pass");
        assert!(msg.contains("scanned only 0 test source files"), "{msg}");
        std::fs::remove_dir_all(&empty).ok();
    }

    /// …and the declaration floor fires independently of the file floor:
    /// plenty of files, no declarations left in them.
    #[test]
    fn the_declaration_floor_fires_when_the_files_are_there_but_the_ports_are_not() {
        let inv = Inventory {
            files_scanned: MIN_FILES_SCANNED,
            decls: Vec::new(),
        };
        let msg = check_inventory(&inv).expect_err("zero declarations must not pass");
        assert!(
            msg.contains("found only 0 fixed-port declarations"),
            "{msg}"
        );
    }

    /// And the declaring-file floor fires when one file still matches but
    /// the others have drifted out of the scan's sight.
    #[test]
    fn the_declaring_file_floor_fires_when_only_one_file_still_matches() {
        let inv = Inventory {
            files_scanned: MIN_FILES_SCANNED,
            decls: (0..MIN_DECLARATIONS as u32)
                .map(|i| Decl {
                    file: "crates/c/tests/only.rs".into(),
                    line: 1,
                    name: "port".into(),
                    port: 31_100 + i,
                })
                .collect(),
        };
        let msg = check_inventory(&inv).expect_err("one declaring file must not pass");
        assert!(msg.contains("files declare a fixed port"), "{msg}");
    }
}
