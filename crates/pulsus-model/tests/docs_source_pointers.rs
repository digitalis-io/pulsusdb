//! Issue #615 — every pointer from a document into a source file, in
//! every document under `docs/`, read and given a verdict.
//!
//! A design document here cites source by line number — "see
//! `exec.rs:125`". When the source moves the citation goes stale, and a
//! stale citation is read as current. An earlier check covered three
//! documents out of the eleven that carry pointers, and on those three it
//! compared the cited line against a snapshot of itself: a pointer moved
//! to a wrong line, with the snapshot regenerated, passed.
//!
//! # What is different here
//!
//! **The document list is derived.** It is every tracked `.md` and `.svg`
//! under `docs/` except `docs/old/`, from `git ls-files`. Nothing names a
//! document, so no document can be left out of the list.
//!
//! **There is no snapshot of the cited line.** A pointer is checked
//! against the NAMES the citing text prints, which the document controls
//! and the source does not. Regenerating a dataset cannot make a wrong
//! pointer right, because no dataset here holds the cited line's text.
//!
//! **Five written forms are read**, not one. The fifth — a comma list —
//! was found while writing this and is not in the issue's table.
//!
//! # The rule, in full
//!
//! A pointer names a file and a line range. The range PASSES when, for
//! some name the citing text prints,
//!
//!   * the range prints that name, or
//!   * the range lies inside the item that DEFINES that name, where an
//!     item begins at its first doc-comment or attribute line and ends at
//!     the end of its body.
//!
//! The second clause is the explicit decision issue #615 asks for: **a
//! pointer at a symbol's doc comment passes**, because the doc comment is
//! part of the item that defines the symbol. A pointer anywhere else in
//! the file does not.
//!
//! It FAILS when the range anchors on nothing and some name the
//! pointer's own line prints is defined exactly once in that file,
//! somewhere else. That is "the pointer names a symbol defined
//! elsewhere", and it is the state a pointer falls into when the source
//! moves underneath it.
//!
//! Everything else is a pointer this rule cannot check, and each one is
//! enumerated in [`UNANCHORED_TSV`] with the reason. The two sets
//! partition the pointers, asserted in both directions: a pointer in
//! neither set is a hole, and a frozen pointer that starts anchoring has
//! to come out of the frozen set rather than sit there as an exemption.
//!
//! # What it does not check
//!
//! A pointer into a tree this repository does not hold — the reference
//! implementation's Go files and a few others. Nothing here can read
//! those files, so the line number is unknowable. The distinct FILE NAMES
//! of that class are frozen in [`FOREIGN_TSV`] so that a mistyped path to
//! one of our own files cannot hide in it.
//!
//! A pointer that anchors on a name which is also somewhere else in the
//! file. The name is a word the document prints, not a proof, and a short
//! common word can land anywhere. The census below counts how many
//! pointers anchor on a name shorter than eight characters.
//!
//! # The census, and how to re-take it
//!
//! ```text
//! cargo test -p pulsus-model --test docs_source_pointers -- --ignored \
//!     the_census_of_source_pointers
//! ```
//!
//! On `23770e3d`, before any repair:
//!
//! ```text
//! documents scanned                                     45
//! documents carrying at least one pointer               20
//!   qualified path   `crates/pulsus-read/src/x.rs:12`   862
//!   bare filename    `x.rs:12`                          635
//!   continuation     `:12` and `x.rs:12,34`             430
//!   linked path      [`sym`](../crates/.../x.rs) (12)    13
//!   pointers written                                   1,940
//!   distinct (document, pointer) keys                  1,516
//! anchored                                               412
//!   by the range printing the name                       360
//!   by the range lying in the name's definition           52
//! FAILS                                                   32
//!   names a symbol defined elsewhere                      32
//!   beyond the end of the file                              0
//! cannot be checked by this rule                       1,072
//!   not in this repository                               419
//!   pinned to another version, `@ v3.0.2`                224
//!   ambiguous basename                                    68
//!   no name the target file holds                        361
//! ```
//!
//! The forms are counted per written pointer and the verdicts per key: a
//! token written twice in one document names one target both times, so a
//! key gets the better of its occurrences' verdicts. See [`rank`].
//!
//! The issue counted 961 over eleven documents; three earlier sweeps of
//! one change answered 3, then 27, then more. Every one of those numbers
//! came from a pattern anchored on a colon, which cannot see a Markdown
//! link followed by `(line N)`, cannot see a comma list, and cannot
//! attribute a `` `:N` `` whose file is named in the prose above the
//! table it sits in. Twenty documents carry pointers, not eleven.

use std::collections::{BTreeMap, BTreeSet};

const UNANCHORED_TSV: &str = "crates/pulsus-model/tests/docs_source_pointers_unanchored.tsv";
const FOREIGN_TSV: &str = "crates/pulsus-model/tests/docs_source_pointers_foreign_files.tsv";

/// The floor on how many pointers the reader finds. A reader that stops
/// seeing a form is the failure this issue is about, so the floor is
/// stated per form below as well.
const POINTERS_FLOOR: usize = 1_700;

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

/// Every tracked file, by relative path.
fn tracked_files() -> Vec<String> {
    let out = std::process::Command::new("git")
        .args(["ls-files"])
        .current_dir(repo_root())
        .output()
        .expect("git ls-files");
    assert!(out.status.success(), "git ls-files failed");
    String::from_utf8(out.stdout)
        .expect("utf-8")
        .lines()
        .map(str::to_string)
        .collect()
}

/// **The documents, derived.** Every tracked `.md` and `.svg` under
/// `docs/` that is not under `docs/old/`. No list is maintained by hand:
/// a hand-maintained list is how eight documents carrying pointers came
/// to be outside the earlier check.
fn documents(tracked: &[String]) -> Vec<String> {
    tracked
        .iter()
        .filter(|f| f.starts_with("docs/") && !f.starts_with("docs/old/"))
        .filter(|f| f.ends_with(".md") || f.ends_with(".svg"))
        .cloned()
        .collect()
}

/// How a pointer is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Form {
    /// `crates/pulsus-read/src/traces/exec.rs:125` — the path written out.
    Qualified,
    /// `exec.rs:125` — the basename alone.
    Bare,
    /// `` `:2498` `` and the second number of `` `client.rs:104,117` `` —
    /// the file comes from an earlier pointer.
    Continuation,
    /// ``[`filter::collect`](../crates/.../filter.rs) (line 2327)`` — the
    /// file comes from a Markdown link and the line from prose. **No
    /// pattern anchored on a colon can see this one**, which is why three
    /// earlier sweeps of one change answered 3, then 27, then more.
    Linked,
}

impl Form {
    fn word(self) -> &'static str {
        match self {
            Form::Qualified => "qualified",
            Form::Bare => "bare",
            Form::Continuation => "continuation",
            Form::Linked => "linked",
        }
    }
}

/// One pointer, with the text around it that the rule reads.
#[derive(Debug, Clone)]
struct Pointer {
    doc: String,
    doc_line: u32,
    form: Form,
    /// What the document wrote for the file part, before resolution.
    path_text: String,
    first: u32,
    last: u32,
    /// The document wrote `@ <version>` after it, so the line is in
    /// another tree at a stated version. See [`pinned_after`].
    pinned: bool,
    /// The names of the nearest backticked token to the pointer that is
    /// not itself a pointer. **Only these can make a pointer FAIL.**
    ///
    /// Using every name on the line convicted a correct pointer the
    /// first time this was run: one table row in
    /// `docs/TraceQL/functional-requirements.md` carries four pointers
    /// and six backticked names, and `intrinsics.rs:61-63`, which points
    /// exactly at the function the row says produces a list, was
    /// convicted on `KEYWORD_TYPE` — a name belonging to a different
    /// pointer on the same row.
    near: BTreeSet<String>,
    /// Names the enclosing paragraph prints, or the table row for a
    /// pointer in a table. A table is one paragraph and a wide table
    /// prints a hundred names, which would make a coincidental match
    /// likely, so a row is its own context.
    wide: BTreeSet<String>,
}

impl Pointer {
    /// The key the frozen datasets use. The line is part of it: moving a
    /// pointer changes its key, so a frozen row cannot follow it.
    fn token(&self) -> String {
        if self.first == self.last {
            format!("{}:{}", self.path_text, self.first)
        } else {
            format!("{}:{}-{}", self.path_text, self.first, self.last)
        }
    }

    fn key(&self) -> (String, String) {
        (self.doc.clone(), self.token())
    }
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// A pointer's own text, so a pointer is never read as its own anchor.
fn looks_like_a_pointer(tok: &str) -> bool {
    match tok.rsplit_once(':') {
        Some((head, tail)) => {
            looks_like_path(head)
                && tail
                    .chars()
                    .all(|c| c.is_ascii_digit() || c == '-' || c == ',')
                && tail.starts_with(|c: char| c.is_ascii_digit())
        }
        None => false,
    }
}

/// The names one backticked token or link label prints: its identifier
/// segments, and the token itself when it is one word.
fn names_of(tok: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    if looks_like_a_pointer(tok) {
        return out;
    }
    for seg in tok.split(|c: char| !is_ident(c)) {
        if seg.len() >= 4 && seg.chars().any(|c| c.is_ascii_alphabetic()) {
            out.insert(seg.to_string());
        }
    }
    out
}

/// The backticked tokens on one line, each with the offsets of its
/// opening and closing backtick.
fn backticked_at(line: &str) -> Vec<(usize, usize, String)> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while let Some(rel) = line[at..].find('`') {
        let open = at + rel;
        let Some(rel2) = line[open + 1..].find('`') else {
            break;
        };
        let close = open + 1 + rel2;
        let inner = &line[open + 1..close];
        if (2..=160).contains(&inner.chars().count()) {
            out.push((open, close, inner.to_string()));
        }
        at = close + 1;
    }
    out
}

/// The backticked tokens on one line.
fn backticked(line: &str) -> Vec<String> {
    backticked_at(line).into_iter().map(|(_, _, t)| t).collect()
}

/// The names of the nearest backticked token to `at` that is not itself
/// a pointer and that prints at least one name.
///
/// Nearest in either direction, **measured to the token's near edge**: a
/// document writes both "`X` is built at `plan.rs:12`" and "at
/// `plan.rs:12`, `X` is built". Measuring to the opening backtick
/// instead made the token AFTER the pointer win in
/// `` `compile_filter_predicate` (line 202) → `render_expr` (378) →
/// `lower_leaf` (530) ``, where each number belongs to the name
/// immediately before it, so `(378)` was convicted on `lower_leaf`.
fn nearest_names(line: &str, at: usize) -> BTreeSet<String> {
    let mut toks: Vec<(usize, BTreeSet<String>)> = backticked_at(line)
        .into_iter()
        .map(|(open, close, t)| {
            let d = if close < at {
                at - close
            } else {
                open.abs_diff(at)
            };
            (d, names_of(&t))
        })
        .filter(|(_, ns)| !ns.is_empty())
        .collect();
    toks.sort_by_key(|(d, _)| *d);
    toks.into_iter()
        .next()
        .map(|(_, ns)| ns)
        .unwrap_or_default()
}

/// Is this a file path a document would write? The last segment must end
/// in a dot and one to eight letters.
///
/// **The extension list is closed**, and both halves of that are
/// deliberate. Without it `http://127.0.0.1:8123` reads as a pointer
/// into a file called `//127.0.0.1` at line 8123, and
/// `resource.service.name` — an attribute spelling these documents print
/// hundreds of times — reads as a file called `name`, which then becomes
/// the file of the next `` `:N` ``. Both happened while this was being
/// written. What the list cannot do is see a pointer into a file whose
/// extension is new; [`every_extension_the_reader_knows_is_used`] keeps
/// the list from carrying dead entries, and nothing can report the
/// missing one.
fn looks_like_path(s: &str) -> bool {
    let base = s.rsplit('/').next().unwrap_or(s);
    let Some((stem, ext)) = base.rsplit_once('.') else {
        return false;
    };
    !stem.is_empty()
        && POINTER_EXTENSIONS.contains(&ext)
        && s.chars()
            .all(|c| is_ident(c) || c == '.' || c == '/' || c == '-')
}

/// The file extensions a pointer in these documents names.
const POINTER_EXTENSIONS: [&str; 15] = [
    "go", "md", "mod", "proto", "rs", "sh", "sql", "test", "toml", "tpl", "ts", "tsx", "y", "yaml",
    "yml",
];

fn digits_at(s: &str) -> (String, usize) {
    let d: String = s.chars().take_while(char::is_ascii_digit).collect();
    let n = d.len();
    (d, n)
}

/// Every pointer in one document, in document order.
///
/// **A continuation takes the nearest file named before it**, in reading
/// order: the nearest one earlier on its own line, else the last one on
/// an earlier line. A file is "named" by a pointer, by a backticked path
/// on its own, or by a Markdown link to one. All three are needed:
///
///   * ADR 0009 writes `` `crates/.../catalog.rs` ``, with no line
///     number, and then `` `:1648` `` and `` `:1650` ``. With only
///     pointers as antecedents those two took the file from a Markdown
///     link six lines above, in the ADR's `Related:` list, and came out
///     as a citation past the end of an unrelated document;
///   * `docs/traceql-schema-migration.md` names its file in the prose
///     above a table and writes `` `:325` `` in the rows. Resetting the
///     antecedent at each heading left 106 of those read by nothing.
fn pointers_in(doc: &str, text: &str) -> Vec<Pointer> {
    let lines: Vec<&str> = text.lines().collect();
    let (para_of, names_of_para) = paragraphs(&lines);

    let mut out = Vec::new();
    // The last file named on an earlier line.
    let mut running: Option<String> = None;
    for (idx, line) in lines.iter().enumerate() {
        let doc_line = idx as u32 + 1;
        // A table row is its own context; see [`Pointer::wide`].
        let mut wide: BTreeSet<String> =
            backticked(line).iter().flat_map(|t| names_of(t)).collect();
        if !line.trim_start().starts_with('|') && para_of[idx] != usize::MAX {
            wide.extend(names_of_para[para_of[idx]].iter().cloned());
        }
        let mut push =
            |form, path_text: &str, first, last, pinned, at: usize, extra: &BTreeSet<String>| {
                let mut near = nearest_names(line, at);
                near.extend(extra.iter().cloned());
                let mut wide = wide.clone();
                wide.extend(extra.iter().cloned());
                out.push(Pointer {
                    doc: doc.to_string(),
                    doc_line,
                    form,
                    path_text: path_text.to_string(),
                    first,
                    last,
                    pinned,
                    near,
                    wide,
                });
            };

        // Every file this line names, by offset, so a continuation can
        // take the nearest one before it.
        let written = written_out(line);
        let links = markdown_links(line);
        let mut named: Vec<(usize, String, BTreeSet<String>)> = Vec::new();
        named.extend(
            written
                .iter()
                .map(|w| (w.at, w.path.clone(), BTreeSet::new())),
        );
        named.extend(
            links
                .iter()
                .map(|l| (l.at, l.target.clone(), l.label_names.clone())),
        );
        named.extend(
            bare_paths(line)
                .into_iter()
                .map(|(at, p)| (at, p, BTreeSet::new())),
        );
        named.sort_by_key(|(at, _, _)| *at);
        let antecedent = |at: usize| -> Option<(String, BTreeSet<String>)> {
            match named.iter().rev().find(|(o, _, _)| *o < at) {
                Some((_, p, ns)) => Some((p.clone(), ns.clone())),
                None => running.clone().map(|p| (p, BTreeSet::new())),
            }
        };

        // Forms A, B and the comma list.
        for w in &written {
            let form = if w.path.contains('/') {
                Form::Qualified
            } else {
                Form::Bare
            };
            push(
                form,
                &w.path,
                w.first,
                w.last,
                w.pinned,
                w.at,
                &BTreeSet::new(),
            );
            for n in &w.also {
                push(
                    Form::Continuation,
                    &w.path,
                    *n,
                    *n,
                    w.pinned,
                    w.at,
                    &BTreeSet::new(),
                );
            }
        }

        // Form D: a file named in the text, then `(line N)` or `(N)`.
        for (at, first, last) in parenthesised(line) {
            if let Some((path, extra)) = antecedent(at) {
                push(Form::Linked, &path, first, last, false, at, &extra);
            }
        }

        // Form C.
        for (at, first, last, pinned) in continuations(line) {
            match antecedent(at) {
                Some((path, _)) => push(
                    Form::Continuation,
                    &path,
                    first,
                    last,
                    pinned,
                    at,
                    &BTreeSet::new(),
                ),
                // A continuation nothing can attribute. Counted and
                // frozen under its own reason rather than dropped — a
                // dropped pointer is a pointer no check can ever see.
                None => push(
                    Form::Continuation,
                    "",
                    first,
                    last,
                    pinned,
                    at,
                    &BTreeSet::new(),
                ),
            }
        }

        if let Some((_, p, _)) = named.last() {
            running = Some(p.clone());
        }
    }
    out
}

/// Every token on a line that is a bare file path, with its offset.
///
/// A path named with no line number still says which file the `` `:N` ``
/// after it belongs to, and it is not always backticked: the quoted Go
/// blocks in `docs/reference-defects-we-do-not-copy.md` name their file
/// in a `// pkg/traceql/ast_execute.go, v3.0.2` header, and the `` `:741` ``
/// in the prose below one of them belongs to that file.
fn bare_paths(line: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut at = 0usize;
    let bytes = line.as_bytes();
    while at <= line.len() {
        let part_of_path = at < line.len() && {
            let c = bytes[at] as char;
            is_ident(c) || c == '.' || c == '/' || c == '-'
        };
        if !part_of_path {
            if at > start && looks_like_path(&line[start..at]) {
                out.push((start, line[start..at].to_string()));
            }
            start = at + 1;
        }
        at += 1;
    }
    out
}

/// Is the text just after a pointer an `@ <version>` marker?
///
/// These documents write `` `de.rs:1102 @ 1.0.150` `` for a line in
/// another crate at a stated version, and `` `:249-280 @ v3.0.2` `` for
/// the reference. The file is not this tree's, even where a file of the
/// same name is — `de.rs` matched `vendor/clickhouse/src/rowbinary/de.rs`
/// and was reported as a citation past its end.
fn pinned_after(line: &str, j: usize) -> bool {
    line[j.min(line.len())..]
        .trim_start_matches('`')
        .trim_start()
        .starts_with('@')
}

/// `(paragraph index of each line, names each paragraph prints)`. A
/// paragraph is a run of non-blank lines; `usize::MAX` is a blank line.
fn paragraphs(lines: &[&str]) -> (Vec<usize>, Vec<BTreeSet<String>>) {
    let mut para_of = vec![usize::MAX; lines.len()];
    let mut names = Vec::new();
    let mut i = 0usize;
    while i < lines.len() {
        if lines[i].trim().is_empty() {
            i += 1;
            continue;
        }
        let start = i;
        while i < lines.len() && !lines[i].trim().is_empty() {
            i += 1;
        }
        let id = names.len();
        let mut ns = BTreeSet::new();
        for l in &lines[start..i] {
            for t in backticked(l) {
                ns.extend(names_of(&t));
            }
        }
        names.push(ns);
        for slot in para_of[start..i].iter_mut() {
            *slot = id;
        }
    }
    (para_of, names)
}

/// One `<path>:<line>[-<line>][,<line>…]` on a line.
struct Written {
    /// Byte offset of the start of the path, so a continuation on the
    /// same line can be attributed to the nearest pointer before it.
    at: usize,
    path: String,
    first: u32,
    last: u32,
    /// The further lines of a comma list.
    also: Vec<u32>,
    pinned: bool,
}

fn written_out(line: &str) -> Vec<Written> {
    let bytes = line.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while let Some(rel) = line[i..].find(':') {
        let colon = i + rel;
        i = colon + 1;
        let (first, n) = digits_at(&line[colon + 1..]);
        if first.is_empty() {
            continue;
        }
        let mut j = colon + 1 + n;
        let mut last = first.clone();
        if line[j..].starts_with('-') {
            let (second, m) = digits_at(&line[j + 1..]);
            if !second.is_empty() {
                last = second;
                j += 1 + m;
            }
        }
        let mut start = colon;
        while start > 0 {
            let c = bytes[start - 1] as char;
            if is_ident(c) || c == '.' || c == '/' || c == '-' {
                start -= 1;
            } else {
                break;
            }
        }
        let path = &line[start..colon];
        if !looks_like_path(path) {
            continue;
        }
        let mut also = Vec::new();
        while line[j..].starts_with(',') {
            let (more, m) = digits_at(&line[j + 1..]);
            if more.is_empty() {
                break;
            }
            j += 1 + m;
            also.push(more.parse().expect("digits"));
        }
        out.push(Written {
            at: start,
            path: path.to_string(),
            first: first.parse().expect("digits"),
            last: last.parse().expect("digits"),
            also,
            pinned: pinned_after(line, j),
        });
        i = j;
    }
    out
}

struct Link {
    at: usize,
    target: String,
    label_names: BTreeSet<String>,
}

/// Every `[label](path)` on a line whose target is a file path.
fn markdown_links(line: &str) -> Vec<Link> {
    let mut out = Vec::new();
    let mut k = 0usize;
    while let Some(rel) = line[k..].find("](") {
        let at = k + rel;
        k = at + 2;
        let Some(close) = line[k..].find(')') else {
            break;
        };
        let target = &line[k..k + close];
        if looks_like_path(target) {
            let label = line[..at].rfind('[').map(|o| &line[o + 1..at]);
            out.push(Link {
                at,
                target: target.to_string(),
                label_names: label
                    .map(|l| names_of(l.trim_matches('`')))
                    .unwrap_or_default(),
            });
        }
        k += close + 1;
    }
    out
}

/// `(line N)`, `(lines N-M)`, and — straight after a backticked name — a
/// bare `(N)` of two digits or more.
///
/// The bare spelling is how `` `compile_filter_predicate` (line 202) →
/// `render_expr` (378) `` writes its second and third pointers.
fn parenthesised(line: &str) -> Vec<(usize, u32, u32)> {
    let bytes = line.as_bytes();
    let mut out = Vec::new();
    let mut at = 0usize;
    while let Some(rel) = line[at..].find('(') {
        let open = at + rel;
        at = open + 1;
        let rest = &line[open + 1..];
        let (body, explicit) = match rest.strip_prefix("line ").or(rest.strip_prefix("lines ")) {
            Some(r) => (r, true),
            None => (rest, false),
        };
        let (first, n) = digits_at(body);
        // `(08-09)` is a date, not a line range. A line number has no
        // leading zero, and two of those were read as pointers.
        if first.len() < 2 || first.starts_with('0') {
            continue;
        }
        let mut j = open + 1 + (rest.len() - body.len()) + n;
        let mut last = first.clone();
        if line[j..].starts_with('-') {
            let (second, m) = digits_at(&line[j + 1..]);
            if !second.is_empty() {
                last = second;
                j += 1 + m;
            }
        }
        if bytes.get(j) != Some(&b')') {
            continue;
        }
        if !explicit && !line[..open].trim_end().ends_with('`') {
            continue;
        }
        out.push((
            open,
            first.parse().expect("digits"),
            last.parse().expect("digits"),
        ));
    }
    out
}

/// Every `` `:<line>` `` / `` `:<line>-<line>` `` on one line, as
/// `(byte offset of the opening backtick, first, last)`.
///
/// The backticks are part of the form: a bare `:825` in prose is an
/// ordinary colon before a number.
fn continuations(line: &str) -> Vec<(usize, u32, u32, bool)> {
    let bytes = line.as_bytes();
    let mut out = Vec::new();
    let mut at = 0usize;
    while let Some(rel) = line[at..].find("`:") {
        let open = at + rel;
        at = open + 2;
        let (first, n) = digits_at(&line[open + 2..]);
        if first.is_empty() {
            continue;
        }
        let mut j = open + 2 + n;
        let mut last = first.clone();
        if line[j..].starts_with('-') {
            let (second, m) = digits_at(&line[j + 1..]);
            if !second.is_empty() {
                last = second;
                j += 1 + m;
            }
        }
        if bytes.get(j) == Some(&b'`') {
            out.push((
                open,
                first.parse().expect("digits"),
                last.parse().expect("digits"),
                pinned_after(line, j),
            ));
        }
    }
    out
}

// ---------------------------------------------------------------------
// The rule
// ---------------------------------------------------------------------

/// How a pointer anchored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum How {
    /// The cited range prints the name.
    RangePrintsIt,
    /// The cited range lies inside the item that defines the name,
    /// including its doc comment and its attributes.
    InsideItsDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Verdict {
    Anchored {
        path: String,
        name: String,
        how: How,
    },
    /// The cited line is past the end of the file it names.
    BeyondEndOfFile { path: String, lines: usize },
    /// Nothing anchors the range, and a name the pointer's own line
    /// prints is defined exactly once in that file, somewhere else.
    DefinedElsewhere { path: String, names: Vec<String> },
    /// No tracked file carries that name. The reference implementation's
    /// files, and a few others this repository does not hold.
    NotInThisRepository,
    /// Several tracked files end in that basename and nothing separates
    /// them.
    AmbiguousBasename,
    /// One file, and the citing text prints no name the file holds.
    NoNameTheTargetFileHolds,
    /// A `` `:N` `` with no earlier pointer to take a file from.
    ContinuationWithNoAntecedent,
    /// `@ <version>`: a line in another tree at a stated version.
    PinnedToAnotherVersion,
}

impl Verdict {
    /// The word the frozen dataset records, or `None` when the pointer is
    /// checked rather than frozen.
    fn reason(&self) -> Option<&'static str> {
        match self {
            Verdict::Anchored { .. }
            | Verdict::BeyondEndOfFile { .. }
            | Verdict::DefinedElsewhere { .. } => None,
            Verdict::NotInThisRepository => Some("not_in_this_repository"),
            Verdict::AmbiguousBasename => Some("ambiguous_basename"),
            Verdict::NoNameTheTargetFileHolds => Some("no_name_the_target_file_holds"),
            Verdict::ContinuationWithNoAntecedent => Some("continuation_with_no_antecedent"),
            Verdict::PinnedToAnotherVersion => Some("pinned_to_another_version"),
        }
    }
}

/// Every tracked file a written path could mean. A leading `./` or `../`
/// is a document-relative prefix and is dropped; what is left must be a
/// whole path suffix, so `plan.rs` never matches `search_plan.rs`.
fn candidates<'a>(path_text: &str, tracked: &'a [String]) -> Vec<&'a String> {
    let mut t = path_text;
    while let Some(r) = t.strip_prefix("../").or(t.strip_prefix("./")) {
        t = r;
    }
    tracked
        .iter()
        .filter(|f| *f == t || f.ends_with(&format!("/{t}")))
        .collect()
}

/// The source files, and every item definition in each, both cached.
///
/// The definitions are taken in ONE pass per file. Asking per name
/// instead rescanned each file once for every name any document printed
/// near a pointer into it, and the four checks here took ninety seconds
/// between them.
struct Sources {
    by_path: BTreeMap<String, Vec<String>>,
    defs: BTreeMap<String, BTreeMap<String, Vec<(u32, u32)>>>,
}

impl Sources {
    fn new() -> Self {
        Self {
            by_path: BTreeMap::new(),
            defs: BTreeMap::new(),
        }
    }

    fn lines(&mut self, path: &str) -> &[String] {
        self.by_path
            .entry(path.to_string())
            .or_insert_with(|| read(path).lines().map(str::to_string).collect())
    }

    fn spans_of(&mut self, path: &str, name: &str) -> Vec<(u32, u32)> {
        if !self.defs.contains_key(path) {
            let defs = definitions(self.lines(path));
            self.defs.insert(path.to_string(), defs);
        }
        self.defs[path].get(name).cloned().unwrap_or_default()
    }
}

/// Every item these lines define, as `name -> [(first, last)]`, 1-based
/// and inclusive, each span starting at the item's first doc-comment or
/// attribute line.
///
/// This is a reader over item keywords rather than a parser. It answers
/// for `fn`, `struct`, `enum`, `trait`, `union`, `type`, `mod`, `static`,
/// `const` and `macro_rules!`, and for nothing else — an enum variant or
/// a struct field has no definition span here, so a pointer naming one
/// anchors by the range printing it or not at all.
fn definitions(lines: &[String]) -> BTreeMap<String, Vec<(u32, u32)>> {
    const LEAD: [&str; 6] = ["pub", "async", "unsafe", "extern", "default", "move"];
    const ITEM: [&str; 10] = [
        "fn",
        "struct",
        "enum",
        "trait",
        "union",
        "type",
        "mod",
        "static",
        "const",
        "macro_rules!",
    ];
    let mut out: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
    for (i, raw) in lines.iter().enumerate() {
        let t = raw.trim();
        let mut toks: Vec<&str> = t.split_whitespace().collect();
        // Strip the leading words that can precede an item keyword, but
        // never a word that is an item keyword itself: `const` is both,
        // and stripping it first made `const MAX_EXPANDED_BYTES` invisible.
        while let Some(first) = toks.first() {
            let head = first.split('(').next().unwrap_or(first);
            let is_item = ITEM.contains(&head) && toks.len() > 1;
            if !is_item && LEAD.contains(&head) && toks.len() > 1 {
                toks.remove(0);
            } else {
                break;
            }
        }
        let item = toks
            .first()
            .map(|k| k.split('(').next().unwrap_or(k))
            .is_some_and(|k| ITEM.contains(&k));
        let name = match toks.get(1).map(|n| ident_prefix(n)) {
            Some(n) if item && !n.is_empty() => n.to_string(),
            _ => continue,
        };
        let mut start = i;
        while start > 0 {
            let p = lines[start - 1].trim();
            if p.starts_with("//") || p.starts_with("#[") || p.starts_with("#!") {
                start -= 1;
            } else {
                break;
            }
        }
        let mut end = i;
        let (mut depth, mut opened) = (0i64, false);
        for (k, l) in lines.iter().enumerate().skip(i) {
            let code = match l.find("//") {
                Some(at) => &l[..at],
                None => l.as_str(),
            };
            for c in code.chars() {
                match c {
                    '{' => {
                        depth += 1;
                        opened = true;
                    }
                    '}' => depth -= 1,
                    _ => {}
                }
            }
            end = k;
            if (opened && depth <= 0) || (!opened && code.trim_end().ends_with(';')) {
                break;
            }
        }
        out.entry(name)
            .or_default()
            .push((start as u32 + 1, end as u32 + 1));
    }
    out
}

fn ident_prefix(s: &str) -> &str {
    let n = s
        .char_indices()
        .take_while(|(_, c)| is_ident(*c))
        .map(|(i, c)| i + c.len_utf8())
        .last()
        .unwrap_or(0);
    &s[..n]
}

/// Does the range print `name` as a whole word?
fn range_prints(lines: &[String], first: u32, last: u32, name: &str) -> bool {
    let hay = lines[first as usize - 1..(last as usize).min(lines.len())].join("\n");
    let mut at = 0usize;
    while let Some(rel) = hay[at..].find(name) {
        let s = at + rel;
        let e = s + name.len();
        let before = s == 0 || !is_ident(hay[..s].chars().next_back().expect("non-empty"));
        let after = e >= hay.len() || !is_ident(hay[e..].chars().next().expect("non-empty"));
        if before && after {
            return true;
        }
        at = s + 1;
    }
    false
}

/// The verdict for one pointer. **Anchoring wins**: a line that prints
/// several names is anchored by any one of them, and a second name
/// defined elsewhere in the file is not evidence against it.
fn resolve(p: &Pointer, tracked: &[String], src: &mut Sources) -> Verdict {
    if p.pinned {
        return Verdict::PinnedToAnotherVersion;
    }
    if p.path_text.is_empty() {
        return Verdict::ContinuationWithNoAntecedent;
    }
    let cands: Vec<String> = candidates(&p.path_text, tracked)
        .into_iter()
        .cloned()
        .collect();
    if cands.is_empty() {
        return Verdict::NotInThisRepository;
    }
    let mut in_range: Vec<String> = Vec::new();
    let mut shortest: Option<(String, usize)> = None;
    for c in &cands {
        let n = src.lines(c).len();
        if n >= p.last as usize {
            in_range.push(c.clone());
        }
        if shortest.as_ref().is_none_or(|(_, m)| n < *m) {
            shortest = Some((c.clone(), n));
        }
    }
    if in_range.is_empty() {
        let (path, lines) = shortest.expect("at least one candidate");
        return Verdict::BeyondEndOfFile { path, lines };
    }
    let mut anchored: Vec<(String, String, How)> = Vec::new();
    let mut elsewhere: BTreeSet<(String, String, u32)> = BTreeSet::new();
    for cand in &in_range {
        let rust = cand.ends_with(".rs");
        for name in &p.wide {
            if range_prints(src.lines(cand), p.first, p.last, name) {
                anchored.push((cand.clone(), name.clone(), How::RangePrintsIt));
                continue;
            }
            if !rust {
                continue;
            }
            let spans = src.spans_of(cand, name);
            if spans.iter().any(|(s, e)| p.first >= *s && p.last <= *e) {
                anchored.push((cand.clone(), name.clone(), How::InsideItsDefinition));
            } else if spans.len() == 1 && p.near.contains(name) {
                elsewhere.insert((cand.clone(), name.clone(), spans[0].0));
            }
        }
    }
    // Prefer the longest anchoring name: the most specific spelling the
    // document printed is the one a reader would check.
    anchored.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.1.cmp(&b.1)));
    if let Some((path, name, how)) = anchored.first() {
        return Verdict::Anchored {
            path: path.clone(),
            name: name.clone(),
            how: *how,
        };
    }
    if in_range.len() > 1 {
        return Verdict::AmbiguousBasename;
    }
    if !elsewhere.is_empty() {
        return Verdict::DefinedElsewhere {
            path: in_range[0].clone(),
            names: elsewhere
                .iter()
                .map(|(_, n, at)| format!("`{n}` is defined at line {at}"))
                .collect(),
        };
    }
    Verdict::NoNameTheTargetFileHolds
}

/// Every pointer in every document, with its verdict.
fn all_pointers() -> (Vec<String>, Vec<(Pointer, Verdict)>) {
    let tracked = tracked_files();
    let docs = documents(&tracked);
    assert!(
        docs.len() >= 40,
        "only {} documents were discovered under docs/; the walk is broken, not the record",
        docs.len()
    );
    let mut src = Sources::new();
    let mut out = Vec::new();
    for doc in &docs {
        for p in pointers_in(doc, &read(doc)) {
            let v = resolve(&p, &tracked, &mut src);
            out.push((p, v));
        }
    }
    (docs, out)
}

/// How a frozen reason ranks when one `(document, token)` is written in
/// two places that fall into two different unanchored classes. Lower is
/// more specific, and the dataset records one reason per key.
fn frozen_rank(v: &Verdict) -> u8 {
    match v {
        Verdict::PinnedToAnotherVersion => 0,
        Verdict::NotInThisRepository => 1,
        Verdict::ContinuationWithNoAntecedent => 2,
        Verdict::AmbiguousBasename => 3,
        Verdict::NoNameTheTargetFileHolds => 4,
        Verdict::Anchored { .. }
        | Verdict::BeyondEndOfFile { .. }
        | Verdict::DefinedElsewhere { .. } => u8::MAX,
    }
}

/// What one pass over the documents found.
struct Reading {
    docs: Vec<String>,
    /// Every pointer as written, in document order.
    occurrences: Vec<(Pointer, Verdict)>,
    /// The occurrences that fail. **Per occurrence, not per key.** One
    /// token can be right where one sentence writes it and wrong where
    /// another does: `logs-differential-ledger.md` cites
    /// `logql/sql.rs:163-178` twice, and the citation beside
    /// `cardinality` lands on a line that happens to print that word
    /// while the citation beside `sql::detected_labels` is 445 lines off.
    /// Keeping the verdict per key hid the second one behind the first.
    fails: Vec<(Pointer, Verdict)>,
    /// `(document, token) -> (reason, a representative occurrence)` for
    /// the keys where **no** occurrence anchors and none fails. These are
    /// the pointers the rule has no answer for.
    frozen: BTreeMap<(String, String), (&'static str, Pointer)>,
    /// One token anchoring in two different files: the key is then not a
    /// target, and both datasets are keyed on it.
    split: Vec<String>,
}

fn reading() -> Reading {
    let (docs, occurrences) = all_pointers();
    let mut anchored_paths: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    let mut answered: BTreeSet<(String, String)> = BTreeSet::new();
    let mut fails = Vec::new();
    let mut frozen: BTreeMap<(String, String), (&'static str, Pointer)> = BTreeMap::new();
    for (p, v) in &occurrences {
        match v {
            Verdict::Anchored { path, .. } => {
                anchored_paths
                    .entry(p.key())
                    .or_default()
                    .insert(path.clone());
                answered.insert(p.key());
            }
            Verdict::BeyondEndOfFile { .. } | Verdict::DefinedElsewhere { .. } => {
                answered.insert(p.key());
                fails.push((p.clone(), v.clone()));
            }
            other => {
                let reason = other.reason().expect("a frozen verdict has a reason");
                match frozen.get(&p.key()) {
                    Some((had, _)) if rank_of(had) <= frozen_rank(other) => {}
                    _ => {
                        frozen.insert(p.key(), (reason, p.clone()));
                    }
                }
            }
        }
    }
    frozen.retain(|k, _| !answered.contains(k));
    let split: Vec<String> = anchored_paths
        .iter()
        .filter(|(_, paths)| paths.len() > 1)
        .map(|((doc, token), paths)| format!("{doc} cites {token}, which anchors in {paths:?}"))
        .collect();
    Reading {
        docs,
        occurrences,
        fails,
        frozen,
        split,
    }
}

/// The rank of a reason word, so the two spellings cannot drift.
fn rank_of(reason: &str) -> u8 {
    match reason {
        "pinned_to_another_version" => frozen_rank(&Verdict::PinnedToAnotherVersion),
        "not_in_this_repository" => frozen_rank(&Verdict::NotInThisRepository),
        "continuation_with_no_antecedent" => frozen_rank(&Verdict::ContinuationWithNoAntecedent),
        "ambiguous_basename" => frozen_rank(&Verdict::AmbiguousBasename),
        "no_name_the_target_file_holds" => frozen_rank(&Verdict::NoNameTheTargetFileHolds),
        other => panic!("no rank for the reason {other:?}"),
    }
}

// ---------------------------------------------------------------------
// The datasets of what this rule cannot check
// ---------------------------------------------------------------------

/// `(document, token) -> reason`, for the pointers the rule cannot check.
fn frozen_unanchored() -> BTreeMap<(String, String), String> {
    let text = read(UNANCHORED_TSV);
    let mut out = BTreeMap::new();
    for (n, line) in text.lines().enumerate() {
        if n == 0 {
            assert_eq!(line, "doc\ttoken\treason", "{UNANCHORED_TSV} header");
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(f.len(), 3, "{UNANCHORED_TSV}:{}: three columns", n + 1);
        assert!(
            [
                "not_in_this_repository",
                "ambiguous_basename",
                "no_name_the_target_file_holds",
                "continuation_with_no_antecedent",
                "pinned_to_another_version",
            ]
            .contains(&f[2]),
            "{UNANCHORED_TSV}:{}: unknown reason {:?}",
            n + 1,
            f[2]
        );
        let prior = out.insert((f[0].to_string(), f[1].to_string()), f[2].to_string());
        assert!(
            prior.is_none(),
            "{UNANCHORED_TSV}:{}: {} / {} is recorded twice",
            n + 1,
            f[0],
            f[1]
        );
    }
    out
}

/// The distinct file names no tracked file carries.
fn frozen_foreign() -> BTreeSet<String> {
    let text = read(FOREIGN_TSV);
    let mut out = BTreeSet::new();
    for (n, line) in text.lines().enumerate() {
        if n == 0 {
            assert_eq!(line, "file", "{FOREIGN_TSV} header");
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        out.insert(line.to_string());
        let _ = n;
    }
    out
}

// ---------------------------------------------------------------------
// The checks
// ---------------------------------------------------------------------

/// **Every pointer in every document still points at what it names.**
///
/// The two failures are the ones a moved line produces: a cited line past
/// the end of its file, and a pointer that anchors on nothing while a
/// name its own line prints is defined elsewhere in that file.
#[test]
fn every_source_pointer_in_the_documents_points_at_what_it_names() {
    let r = reading();
    let mut problems: Vec<String> = r.split;
    for (p, v) in &r.fails {
        match v {
            Verdict::BeyondEndOfFile { path, lines } => problems.push(format!(
                "{}:{} cites {} ({}), and {path} has {lines} lines",
                p.doc,
                p.doc_line,
                p.token(),
                p.form.word()
            )),
            Verdict::DefinedElsewhere { path, names } => problems.push(format!(
                "{}:{} cites {} ({}), which anchors on nothing the citing line prints; in \
                 {path}, {}. Re-point it at the expression the sentence names, not at \
                 whatever is now on the old line",
                p.doc,
                p.doc_line,
                p.token(),
                p.form.word(),
                names.join(" and "),
            )),
            other => unreachable!("{other:?} is not a failure"),
        }
    }
    assert!(
        problems.is_empty(),
        "{} stale source pointer(s):\n  {}",
        problems.len(),
        problems.join("\n  ")
    );
}

/// **The checked pointers and the frozen ones partition the record, in
/// both directions.**
///
/// A pointer in neither set is a hole. A frozen pointer that has started
/// anchoring is not an exemption to keep — it comes out of the dataset,
/// which is what stops the hole closing unnoticed and staying enumerated.
#[test]
fn the_frozen_datasets_partition_the_pointers() {
    let r = reading();
    let recorded = frozen_unanchored();
    let mut problems: Vec<String> = Vec::new();
    for (key, (want, p)) in &r.frozen {
        match recorded.get(key) {
            None => problems.push(format!(
                "{}:{} cites {} ({}), which this rule cannot check ({want}), and no row of \
                 {UNANCHORED_TSV} says so",
                p.doc,
                p.doc_line,
                p.token(),
                p.form.word()
            )),
            Some(had) if had != want => problems.push(format!(
                "{UNANCHORED_TSV} gives {} / {} the reason {had:?}; it is now {want:?}",
                key.0, key.1
            )),
            Some(_) => {}
        }
    }
    for key in recorded.keys() {
        if !r.frozen.contains_key(key) {
            problems.push(format!(
                "{UNANCHORED_TSV} freezes {} / {}, which the rule now has an answer for, or \
                 which {} no longer cites. Take the row out",
                key.0, key.1, key.0
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "{} partition problem(s):\n  {}",
        problems.len(),
        problems.join("\n  ")
    );
}

/// **Every file name outside this repository is named.**
///
/// A pointer into a tree we do not hold cannot be checked, so the class
/// is a rule rather than a list of pointers — but a path mistyped into
/// one of our own files falls into the same class, and would be invisible
/// there. The distinct names are frozen, so a new one has to be looked at
/// once.
#[test]
fn every_file_name_this_repository_does_not_hold_is_named() {
    let seen: BTreeSet<String> = reading()
        .occurrences
        .iter()
        .filter(|(_, v)| *v == Verdict::NotInThisRepository)
        .map(|(p, _)| p.path_text.clone())
        .collect();
    let frozen = frozen_foreign();
    let new: Vec<&String> = seen.difference(&frozen).collect();
    assert!(
        new.is_empty(),
        "{} file name(s) that no tracked file carries and {FOREIGN_TSV} does not name: {new:?}. \
         If one of them is a path into this repository, it is mistyped",
        new.len()
    );
    let gone: Vec<&String> = frozen.difference(&seen).collect();
    assert!(
        gone.is_empty(),
        "{FOREIGN_TSV} names {gone:?}, which no document cites any more"
    );
}

/// **The reader still reads all five forms.**
///
/// Three sweeps of one change answered 3, then 27, then more, because
/// each searched the forms it knew and was blind to the rest. A floor per
/// form is what makes a form going unread a failure rather than a
/// quietly smaller number.
#[test]
fn the_reader_reads_every_written_form() {
    let (docs, pointers) = all_pointers();
    assert!(
        pointers.len() >= POINTERS_FLOOR,
        "only {} pointers were read out of {} documents",
        pointers.len(),
        docs.len()
    );
    let mut by_form: BTreeMap<Form, usize> = BTreeMap::new();
    for (p, _) in &pointers {
        *by_form.entry(p.form).or_default() += 1;
    }
    for (form, floor) in [
        (Form::Qualified, 800usize),
        (Form::Bare, 600),
        (Form::Continuation, 250),
        (Form::Linked, 8),
    ] {
        let got = by_form.get(&form).copied().unwrap_or(0);
        assert!(
            got >= floor,
            "the {} form reads {got} pointers; the floor is {floor}",
            form.word()
        );
    }
    let carrying: BTreeSet<&String> = pointers.iter().map(|(p, _)| &p.doc).collect();
    assert!(
        carrying.len() >= 20,
        "only {} documents carry pointers; eleven were known before every document was \
         scanned, so a smaller number means the walk narrowed",
        carrying.len()
    );
}

/// **No extension in the reader's list is dead.**
///
/// The list is closed, so it is the one place a pointer can go unread.
/// An entry nothing uses is either a mistake or a pointer that has gone,
/// and either way it should not sit there looking like coverage.
#[test]
fn every_extension_the_reader_knows_is_used() {
    let (_, pointers) = all_pointers();
    let used: BTreeSet<&str> = pointers
        .iter()
        .filter_map(|(p, _)| p.path_text.rsplit_once('.'))
        .map(|(_, ext)| ext)
        .collect();
    let dead: Vec<&&str> = POINTER_EXTENSIONS
        .iter()
        .filter(|e| !used.contains(**e))
        .collect();
    assert!(
        dead.is_empty(),
        "POINTER_EXTENSIONS carries {dead:?}, which no pointer in any document uses"
    );
}

/// Prints the census this file's module doc records. Asserts nothing.
#[test]
#[ignore = "prints a census; asserts nothing"]
fn the_census_of_source_pointers() {
    let r = reading();
    let (docs, pointers) = (&r.docs, &r.occurrences);
    let mut by_form: BTreeMap<&str, usize> = BTreeMap::new();
    let mut by_verdict: BTreeMap<&str, usize> = BTreeMap::new();
    let mut short_anchor = 0usize;
    for (p, _) in pointers {
        *by_form.entry(p.form.word()).or_default() += 1;
    }
    for (_, v) in pointers {
        *by_verdict
            .entry(match v {
                Verdict::Anchored {
                    how: How::RangePrintsIt,
                    ..
                } => "anchored: the range prints the name",
                Verdict::Anchored {
                    how: How::InsideItsDefinition,
                    ..
                } => "anchored: the range is in the name's definition",
                Verdict::BeyondEndOfFile { .. } => "FAIL: beyond the end of the file",
                Verdict::DefinedElsewhere { .. } => "FAIL: names a symbol defined elsewhere",
                other => other.reason().expect("a frozen verdict has a reason"),
            })
            .or_default() += 1;
        if let Verdict::Anchored { name, .. } = v
            && name.len() < 8
        {
            short_anchor += 1;
        }
    }
    let mut carrying: BTreeMap<&String, usize> = BTreeMap::new();
    for (p, _) in pointers {
        *carrying.entry(&p.doc).or_default() += 1;
    }
    println!("documents scanned: {}", docs.len());
    println!(
        "documents carrying at least one pointer: {}",
        carrying.len()
    );
    for (d, n) in &carrying {
        println!("  {d}: {n}");
    }
    for (k, n) in &by_form {
        println!("  form {k}: {n}");
    }
    println!("total pointers: {}", pointers.len());
    println!(
        "distinct (document, pointer) keys: {}",
        pointers
            .iter()
            .map(|(p, _)| p.key())
            .collect::<BTreeSet<_>>()
            .len()
    );
    println!("failing occurrences: {}", r.fails.len());
    println!("frozen keys: {}", r.frozen.len());
    for (k, n) in &by_verdict {
        println!("  {k}: {n}");
    }
    println!("anchored on a name shorter than eight characters: {short_anchor}");
}

/// Rewrites both frozen datasets. Ignored, so it never runs in CI.
///
/// **Running it cannot make a stale pointer pass.** Neither dataset holds
/// anything about a cited line: one holds the pointers this rule has no
/// answer for, the other the file names this repository does not carry.
/// A pointer that fails is not in either, and regenerating writes nothing
/// that would change its verdict.
///
/// ```text
/// cargo test -p pulsus-model --test docs_source_pointers -- --ignored \
///     regenerate_the_frozen_datasets
/// ```
#[test]
#[ignore = "writes the two frozen datasets"]
fn regenerate_the_frozen_datasets() {
    let r = reading();
    let foreign: BTreeSet<String> = r
        .occurrences
        .iter()
        .filter(|(_, v)| *v == Verdict::NotInThisRepository)
        .map(|(p, _)| p.path_text.clone())
        .collect();
    let mut out = String::from("doc\ttoken\treason\n");
    for ((doc, token), (reason, _)) in &r.frozen {
        out.push_str(&format!("{doc}\t{token}\t{reason}\n"));
    }
    std::fs::write(repo_root().join(UNANCHORED_TSV), out).expect("write the unanchored dataset");
    let mut out = String::from("file\n");
    for f in &foreign {
        out.push_str(&format!("{f}\n"));
    }
    std::fs::write(repo_root().join(FOREIGN_TSV), out).expect("write the foreign dataset");
}
