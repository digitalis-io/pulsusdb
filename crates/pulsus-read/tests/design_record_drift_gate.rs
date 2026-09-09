//! Issue #492 part 8: the design record states numbers that are derived
//! from the source, and until this file existed nothing derived them.
//!
//! Two inherited inventories said the same thing twice — hand-maintained
//! derived counts, and citations into source files by line number — and
//! both were ungated. A wrong derived count was invisible: changing
//! `docs/query-lowering.md`'s statement of a variant count and running
//! `cargo nextest run --workspace` exited 0 with no failing test.
//!
//! **The instrument is an anchor pair, not a noun and a window.** An
//! earlier design swept for a noun and took the nearest number within N
//! characters. Probed, that instrument did not discriminate: sweeping for
//! `PipelineStage` at a window of 48 characters found five sites, and
//! four of them were line numbers and unrelated counts rather than the
//! variant count. Widening the window to 64 found the same five. The free
//! string parameters were the hole, so they are gone: a site is the exact
//! text immediately before the number and the exact text immediately
//! after it, and the prefix must occur **exactly once** in the file.
//!
//! **Matching is over a whitespace-normalised copy of the file.** Both
//! records are hard-wrapped, so a prefix and its number routinely sit on
//! different lines; a line-scoped rule would turn a re-wrap into a
//! failure. The recorded `line` is the line the NUMBER lands on, mapped
//! back from the normalised offset, and it is **derived** — the ignored
//! `regenerate_the_count_site_lines` test rewrites it, and editing it by
//! hand is what the check's message forbids.

use std::collections::{BTreeMap, BTreeSet};

const COUNTS_TSV: &str = "crates/pulsus-read/tests/design_record_counts.tsv";

/// The number of recorded count sites, stated here so the dataset cannot
/// silently shrink. Issue #492 part 8's plan made this the coder's
/// enumeration to publish rather than a figure the plan could derive: it
/// required `>= 19`, and the enumeration came to 24 across 12 derived
/// counts.
const RECORDED_COUNT_SITES: usize = 24;

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

/// How a site writes its number.
///
/// The word table is CLOSED: an unmapped word is a failure, never a skip.
/// A rendering the table does not know would otherwise read as "this site
/// states no number", which is the silent direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rendering {
    Digits,
    NumberWord,
}

impl Rendering {
    fn parse(self, text: &str) -> Option<u64> {
        let t = text.trim().trim_matches('*').trim_matches('`').trim();
        match self {
            Rendering::Digits => t.replace(',', "").parse().ok(),
            Rendering::NumberWord => match t {
                "one" => Some(1),
                "two" => Some(2),
                "three" => Some(3),
                "four" => Some(4),
                "five" => Some(5),
                "six" => Some(6),
                "seven" => Some(7),
                "eight" => Some(8),
                "nine" => Some(9),
                "ten" => Some(10),
                "eleven" => Some(11),
                "twelve" => Some(12),
                "thirteen" => Some(13),
                "fourteen" => Some(14),
                "fifteen" => Some(15),
                "twenty" => Some(20),
                "twenty-one" => Some(21),
                "twenty-five" => Some(25),
                "thirty-one" => Some(31),
                _ => None,
            },
        }
    }
}

/// How a count is derived from something that is not this document.
///
/// **No `_` arm** in [`derive`]: a new derived count fails to build until
/// it says where its value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Derivation {
    /// The variants of `pub enum <ident>` in `path`.
    EnumVariants {
        path: &'static str,
        ident: &'static str,
    },
    /// The link rows of a section that state a residual state effect.
    LinkRowsWithEffect {
        heading: &'static str,
        end: &'static str,
    },
    /// The link rows of a section that state none — the identity rows and
    /// the not-in-the-chain rows together.
    LinkRowsWithoutEffect {
        heading: &'static str,
        end: &'static str,
    },
    /// The sum of two other derived counts.
    Sum(&'static str, &'static str),
    /// The row count the shipped residual-effect gate asserts, read from
    /// the call that asserts it.
    ShippedResidualEffectRows,
}

/// Every derived count the record states, and where its value comes from.
/// The `id` column of the dataset resolves through this list.
const DERIVATIONS: [(&str, Derivation); 12] = [
    (
        "traceql_pipeline_stage_variants",
        Derivation::EnumVariants {
            path: "crates/pulsus-traceql/src/ast.rs",
            ident: "PipelineStage",
        },
    ),
    (
        "logql_stage_variants",
        Derivation::EnumVariants {
            path: "crates/pulsus-logql/src/ast.rs",
            ident: "Stage",
        },
    ),
    (
        "tql_link_variants",
        Derivation::EnumVariants {
            path: "crates/pulsus-read/src/traces/compile.rs",
            ident: "TqlLink",
        },
    ),
    (
        "lql_link_variants",
        Derivation::EnumVariants {
            path: "crates/pulsus-read/src/logql/compile.rs",
            ident: "LqlLink",
        },
    ),
    (
        "cut_variants",
        Derivation::EnumVariants {
            path: "crates/pulsus-read/src/compile/plan.rs",
            ident: "Cut",
        },
    ),
    (
        "never_reason_variants",
        Derivation::EnumVariants {
            path: "crates/pulsus-read/src/compile/fold.rs",
            ident: "NeverReason",
        },
    ),
    (
        "section_3_1_effect_rows",
        Derivation::LinkRowsWithEffect {
            heading: S31,
            end: S32,
        },
    ),
    (
        "section_3_1_rows_without_effect",
        Derivation::LinkRowsWithoutEffect {
            heading: S31,
            end: S32,
        },
    ),
    (
        "section_7_1_effect_rows",
        Derivation::LinkRowsWithEffect {
            heading: S71,
            end: S72,
        },
    ),
    (
        "section_7_1_rows_without_effect",
        Derivation::LinkRowsWithoutEffect {
            heading: S71,
            end: S72,
        },
    ),
    (
        "residual_effects_total",
        Derivation::Sum("section_3_1_effect_rows", "section_7_1_effect_rows"),
    ),
    (
        "traceql_residual_effect_gate_rows",
        Derivation::ShippedResidualEffectRows,
    ),
];

const QUERY_LOWERING: &str = "docs/query-lowering.md";
const S31: &str = "### 3.1 The complete TraceQL link set";
const S32: &str = "### 3.2 Group 1 — cannot be lowered";
const S71: &str = "### 7.1 The complete LogQL link set";
const S72: &str = "### 7.2 Groups 1, 2 and 3";

fn derive(id: &str) -> u64 {
    let d = DERIVATIONS
        .iter()
        .find(|(k, _)| *k == id)
        .unwrap_or_else(|| panic!("{COUNTS_TSV} names the count {id:?}, which has no derivation"))
        .1;
    // No `_` arm.
    match d {
        Derivation::EnumVariants { path, ident } => enum_variants(path, ident).len() as u64,
        Derivation::LinkRowsWithEffect { heading, end } => {
            let (with, _) = effect_split(heading, end);
            with
        }
        Derivation::LinkRowsWithoutEffect { heading, end } => {
            let (_, without) = effect_split(heading, end);
            without
        }
        Derivation::Sum(a, b) => derive(a) + derive(b),
        Derivation::ShippedResidualEffectRows => {
            let src = read("crates/pulsus-read/src/traces/compile.rs");
            src.split_once("assert_every_residual_state_effect::<Tql>(&rows, ")
                .expect("traces/compile.rs must assert its residual-effect row count")
                .1
                .split(')')
                .next()
                .expect("the call is closed")
                .trim()
                .parse()
                .expect("the row count is a number")
        }
    }
}

/// The variant identifiers of `pub enum <ident>` in `path`.
fn enum_variants(path: &str, ident: &str) -> Vec<String> {
    let src = read(path);
    let head = format!("pub enum {ident} {{");
    let start = src
        .find(&head)
        .unwrap_or_else(|| panic!("{path} must declare `{head}`"))
        + head.len();
    let mut depth = 1usize;
    let mut end = start;
    for (i, c) in src[start..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(end > start, "{path}: `{head}` is not closed");
    let mut depth = 0usize;
    let mut out = Vec::new();
    for line in src[start..end].lines() {
        let t = line.trim();
        if depth == 0 && !t.starts_with("//") && !t.starts_with('#') {
            let id: String = t
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if id.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
                out.push(id);
            }
        }
        depth = depth + t.matches('{').count() + t.matches('(').count() + t.matches('[').count();
        depth = depth.saturating_sub(
            t.matches('}').count() + t.matches(')').count() + t.matches(']').count(),
        );
    }
    assert!(
        !out.is_empty(),
        "{path}: parsed no variants out of `{head}`"
    );
    out
}

/// `(rows stating an effect, rows stating none)` for one link section.
fn effect_split(heading: &str, end: &str) -> (u64, u64) {
    let md = read(QUERY_LOWERING);
    let start = md
        .find(heading)
        .unwrap_or_else(|| panic!("{QUERY_LOWERING} has no {heading:?}"));
    let rest = &md[start..];
    let slice = &rest[..rest
        .find(end)
        .unwrap_or_else(|| panic!("{heading:?} is not followed by {end:?}"))];
    let (mut with, mut without) = (0u64, 0u64);
    for line in slice.lines() {
        if !line.starts_with("| `") {
            continue;
        }
        let cells: Vec<&str> = line.trim_matches('|').split(" | ").map(str::trim).collect();
        // The link tables carry a `continuation` last column; the
        // rejection tables do not and have four cells.
        if cells.len() < 6 {
            continue;
        }
        let effect = cells[cells.len() - 3].replace("**", "");
        if effect.starts_with("none") || effect.starts_with("n/a") {
            without += 1;
        } else {
            with += 1;
        }
    }
    assert!(with + without > 0, "{heading:?} carries no link rows");
    (with, without)
}

/// One recorded SITE of one derived count.
#[derive(Debug, Clone)]
struct CountSite {
    id: String,
    path: String,
    prefix: String,
    suffix: String,
    rendering: Rendering,
    line: u32,
}

fn count_sites() -> Vec<CountSite> {
    let text = read(COUNTS_TSV);
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        if n == 0 {
            assert_eq!(
                line, "id\tpath\tprefix\tsuffix\trendering\tline",
                "{COUNTS_TSV} header"
            );
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(f.len(), 6, "{COUNTS_TSV}:{}: six columns", n + 1);
        out.push(CountSite {
            id: f[0].to_string(),
            path: f[1].to_string(),
            prefix: f[2].to_string(),
            suffix: f[3].to_string(),
            rendering: match f[4] {
                "digits" => Rendering::Digits,
                "word" => Rendering::NumberWord,
                other => panic!("{COUNTS_TSV}:{}: unknown rendering {other:?}", n + 1),
            },
            line: f[5]
                .parse()
                .unwrap_or_else(|_| panic!("{COUNTS_TSV}:{}: line", n + 1)),
        });
    }
    out
}

/// The file's text with every run of whitespace collapsed to one space,
/// and the original line number of each character.
fn normalised(raw: &str) -> (String, Vec<u32>) {
    let (mut out, mut lines) = (String::new(), Vec::new());
    let (mut ln, mut prev_ws) = (1u32, true);
    for ch in raw.chars() {
        if ch.is_whitespace() {
            if ch == '\n' {
                ln += 1;
            }
            if !prev_ws {
                out.push(' ');
                lines.push(ln);
                prev_ws = true;
            }
        } else {
            out.push(ch);
            lines.push(ln);
            prev_ws = false;
        }
    }
    (out, lines)
}

/// The value a site writes, and the line the number lands on — or `None`
/// when the site does not resolve.
///
/// **Not resolving is not this function's failure to report.** Whether a
/// recorded site still resolves is
/// [`every_recorded_count_site_resolves_exactly_once`]'s question, and
/// keeping the two separate is what makes them distinguishable: a
/// reworded sentence reddens the resolution check and leaves the value
/// check green, so the two failures name two different defects rather
/// than one defect twice.
fn read_site(site: &CountSite) -> Option<(Option<u64>, u32, String)> {
    let raw = read(&site.path);
    let (t, lines) = normalised(&raw);
    if t.matches(&site.prefix).count() != 1 {
        return None;
    }
    let start = t.find(&site.prefix).expect("checked above") + site.prefix.len();
    let tail = &t[start..];
    let stop = tail.find(&site.suffix).unwrap_or_else(|| {
        panic!(
            "{}: suffix {:?} does not follow its prefix in {}",
            site.id, site.suffix, site.path
        )
    });
    let between = tail[..stop].to_string();
    // The number's own line: the first non-space character after the
    // prefix.
    let offset = start + between.len() - between.trim_start().len();
    Some((
        site.rendering.parse(&between),
        lines[offset.min(lines.len() - 1)],
        between,
    ))
}

/// **Every derived count the record states is the one derived.**
///
/// The value between a site's anchors is compared with the value derived
/// from the source — an enum's variant list, a section's own table, or a
/// row count the shipped gate asserts. Neither side can produce the
/// other, which is what makes a stale number visible.
#[test]
fn every_derived_count_the_record_states_is_the_one_derived() {
    let sites = count_sites();
    assert!(!sites.is_empty(), "{COUNTS_TSV} is empty");
    let mut checked = 0usize;
    for site in &sites {
        let want = derive(&site.id);
        let Some((got, _, between)) = read_site(site) else {
            // The site no longer resolves. That is
            // `every_recorded_count_site_resolves_exactly_once`'s
            // failure, in this same binary; reporting it here too would
            // make one defect look like two.
            continue;
        };
        checked += 1;
        let got = got.unwrap_or_else(|| {
            panic!(
                "{}: {} writes {between:?} between its anchors, which is not a number this \
                 rendering knows",
                site.id, site.path
            )
        });
        assert_eq!(
            got, want,
            "{} derives to {want}, but {}:{} writes {between:?}",
            site.id, site.path, site.line
        );
    }
    assert!(
        checked > 0,
        "no recorded site resolved, so this check compared nothing"
    );
}

/// **Every recorded site still resolves, exactly once.**
///
/// This is the clause that stops the sweep silently narrowing: a site
/// whose sentence is reworded no longer has its prefix in the file, and
/// the check names the site rather than quietly covering one place fewer.
///
/// **What it cannot see** is a site that writes the same number somewhere
/// NEW. That was never mechanically checkable — an earlier design claimed
/// a frozen site count would catch it, and a frozen count cannot — and it
/// is stated as a limit rather than gated by something that cannot see
/// it.
#[test]
fn every_recorded_count_site_resolves_exactly_once() {
    let sites = count_sites();
    assert_eq!(
        sites.len(),
        RECORDED_COUNT_SITES,
        "{COUNTS_TSV} holds {} rows; RECORDED_COUNT_SITES says {RECORDED_COUNT_SITES}",
        sites.len()
    );
    const _: () = assert!(
        RECORDED_COUNT_SITES >= 19,
        "the enumeration must cover at least the 19 sites the plan established as a floor"
    );
    let ids: BTreeSet<&str> = sites.iter().map(|s| s.id.as_str()).collect();
    for (id, _) in DERIVATIONS {
        assert!(
            ids.contains(id),
            "the derivation {id:?} has no recorded site, so nothing in the record states it and \
             the derivation is dead"
        );
    }
    for site in &sites {
        let raw = read(&site.path);
        let (t, _) = normalised(&raw);
        let n = t.matches(&site.prefix).count();
        assert_eq!(
            n, 1,
            "{}: prefix {:?} occurs {n} times in {}; recorded sites must resolve exactly once",
            site.id, site.prefix, site.path
        );
        let (_, line, _) = read_site(site).expect("resolves, asserted above");
        assert_eq!(
            line, site.line,
            "{}: the number is at {}:{line}, and {COUNTS_TSV} records {}. That column is DERIVED \
             — run the ignored `regenerate_the_count_site_lines` rather than editing it",
            site.id, site.path, site.line
        );
    }
}

/// Rewrites the derived `line` column of [`COUNTS_TSV`]. Ignored, so it
/// never runs in CI; the pattern seventeen other ignored tests in this
/// workspace already follow.
///
/// ```text
/// cargo test -p pulsus-read --test design_record_drift_gate -- --ignored
/// ```
#[test]
#[ignore = "writes crates/pulsus-read/tests/design_record_counts.tsv"]
fn regenerate_the_count_site_lines() {
    let sites = count_sites();
    let mut out = String::from("id\tpath\tprefix\tsuffix\trendering\tline\n");
    for site in &sites {
        let (_, line, _) = read_site(site).expect("every recorded site resolves");
        let rendering = match site.rendering {
            Rendering::Digits => "digits",
            Rendering::NumberWord => "word",
        };
        out.push_str(&format!(
            "{}\t{}\t{}\t{}\t{rendering}\t{line}\n",
            site.id, site.path, site.prefix, site.suffix
        ));
    }
    std::fs::write(repo_root().join(COUNTS_TSV), out).expect("write the dataset");
}

// ---------------------------------------------------------------------
// Issue #492 part 8 — the citations
//
// The design record cites source files by line number, and nothing
// derived those citations: moving `search_plan.rs:1854` to `:2854` in
// `docs/query-to-sql.md` and running `cargo nextest run --workspace`
// exited 0 with no failing test.
//
// **No count is written here.** How many citations the record makes is a
// property of the tree at a revision — it moved on every commit of this
// part — so it belongs in `docs/query-lowering.md` §12.3 beside the
// revision it was taken at, and nothing in this file depends on it. An
// earlier version of this comment carried three such figures and they
// were all stale within two commits, in the file whose subject is stale
// figures.
//
// **The covered set is what the resolver answers, and the rest is
// enumerated rather than guessed.** Most citations name a bare basename,
// and six of those basenames match more than one tracked file —
// `plan.rs` matches four. [`resolve_citation`] picks the candidate whose
// cited line contains an identifier the citing prose already prints;
// what it cannot answer is frozen in [`UNRESOLVABLE_TSV`] with a reason
// each, and the check below asserts the two sets partition the record's
// citations in every direction.
//
// **What would close the gap** is making the frozen citing lines print
// an identifier the cited line carries — the same rule the resolved ones
// already satisfy. That is a per-site reading of each cited line against
// the claim beside it, and it is recorded as work rather than promised.
// ---------------------------------------------------------------------

const CITATIONS_TSV: &str = "crates/pulsus-read/tests/design_record_citations.tsv";
const UNRESOLVABLE_TSV: &str = "crates/pulsus-read/tests/design_record_unresolvable_citations.tsv";

/// The five design artefacts every citation is read out of.
const DESIGN_ARTEFACTS: [&str; 5] = [
    "docs/query-lowering.md",
    "docs/query-to-sql.md",
    "docs/decisions/0008-sql-composition-for-lowered-pipelines.md",
    "docs/diagrams/query-lowering-hops.svg",
    "docs/diagrams/query-lowering-boundary.svg",
];

/// What a row's anchor is, and therefore what a reader can check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnchorKind {
    /// A token the CITING prose already prints, which the cited line
    /// carries. The claim and its evidence are reviewable side by side.
    Prose,
    /// A snapshot of the cited line itself, because the citing prose
    /// prints no identifier the line carries. It detects drift — the
    /// line moving or changing reddens — but it cannot show the citation
    /// means the right thing, and this row records which kind it is so
    /// that difference is visible.
    Line,
}

/// One citation target: a tracked file, a line range, and the text that
/// range must contain.
#[derive(Debug, Clone)]
struct CitationRow {
    doc: String,
    token: String,
    path: String,
    line: u32,
    end_line: Option<u32>,
    kind: AnchorKind,
    anchor: String,
}

fn citation_rows() -> Vec<CitationRow> {
    let text = read(CITATIONS_TSV);
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        if n == 0 {
            assert_eq!(
                line, "doc\ttoken\tpath\tline\tend_line\tanchor_kind\tanchor",
                "{CITATIONS_TSV} header"
            );
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.splitn(7, '\t').collect();
        assert_eq!(f.len(), 7, "{CITATIONS_TSV}:{}: seven columns", n + 1);
        out.push(CitationRow {
            doc: f[0].to_string(),
            token: f[1].to_string(),
            path: f[2].to_string(),
            line: f[3]
                .parse()
                .unwrap_or_else(|_| panic!("{CITATIONS_TSV}:{}: line", n + 1)),
            end_line: (!f[4].is_empty()).then(|| {
                f[4].parse()
                    .unwrap_or_else(|_| panic!("{CITATIONS_TSV}:{}: end_line", n + 1))
            }),
            kind: match f[5] {
                "prose" => AnchorKind::Prose,
                "line" => AnchorKind::Line,
                other => panic!("{CITATIONS_TSV}:{}: unknown anchor_kind {other:?}", n + 1),
            },
            anchor: f[6].to_string(),
        });
    }
    out
}

/// The frozen set of citations that cannot be resolved to one tracked
/// file, keyed `(document, token)`, with the reason.
fn unresolvable() -> BTreeSet<(String, String, String)> {
    let text = read(UNRESOLVABLE_TSV);
    let mut out = BTreeSet::new();
    for (n, line) in text.lines().enumerate() {
        if n == 0 {
            assert_eq!(line, "doc\ttoken\treason", "{UNRESOLVABLE_TSV} header");
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(f.len(), 3, "{UNRESOLVABLE_TSV}:{}: three columns", n + 1);
        assert!(
            [
                "ambiguous_basename",
                "not_a_tracked_file",
                "blank_target_line",
                "line_beyond_end_of_file",
                "occurrences_disagree"
            ]
            .contains(&f[2]),
            "{UNRESOLVABLE_TSV}:{}: unknown reason {:?}",
            n + 1,
            f[2]
        );
        out.insert((f[0].to_string(), f[1].to_string(), f[2].to_string()));
    }
    out
}

fn ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// One `<file>.rs:<line>[-<line>]` occurrence.
#[derive(Debug, Clone)]
struct Occurrence {
    doc: String,
    /// The 1-based line of the document the citation sits on. The
    /// fallback comparison below needs it to find the enclosing section.
    doc_line: u32,
    token: String,
    first: u32,
    last: u32,
    /// The whole line the citation sits on. **The resolver reads it**:
    /// which of several `plan.rs` files a bare citation means is decided
    /// by whether the cited line carries an identifier this line prints.
    citing_line: String,
}

/// Every `<file>.rs:<line>[-<line>]` occurrence in the five artefacts.
fn citation_occurrences() -> Vec<Occurrence> {
    let mut out = Vec::new();
    for doc in DESIGN_ARTEFACTS {
        let text = read(doc);
        for (doc_line, line) in text.lines().enumerate() {
            let doc_line = doc_line as u32 + 1;
            let bytes = line.as_bytes();
            let mut i = 0usize;
            while let Some(at) = line[i..].find(".rs:") {
                let dot = i + at;
                // The path: back to the first character that cannot be
                // part of one.
                let mut start = dot;
                while start > 0 {
                    let c = bytes[start - 1] as char;
                    if c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '/' || c == '-' {
                        start -= 1;
                    } else {
                        break;
                    }
                }
                let mut j = dot + 4;
                let first: String = line[j..].chars().take_while(char::is_ascii_digit).collect();
                j += first.len();
                let mut last = first.clone();
                if line[j..].starts_with('-') {
                    let second: String = line[j + 1..]
                        .chars()
                        .take_while(char::is_ascii_digit)
                        .collect();
                    if !second.is_empty() {
                        last = second.clone();
                        j += 1 + second.len();
                    }
                }
                if !first.is_empty() && start < dot {
                    out.push(Occurrence {
                        doc: doc.to_string(),
                        doc_line,
                        token: line[start..j].to_string(),
                        first: first.parse().expect("digits"),
                        last: last.parse().expect("digits"),
                        citing_line: line.to_string(),
                    });
                }
                i = (dot + 4).max(j);
            }
        }
    }
    out
}

/// What a citation resolves to, and when it does not, why not.
///
/// **This is the rule the two datasets record the verdict of**, and it
/// runs here rather than in a script beside the repository, so that a
/// frozen entry which starts resolving fails instead of being tolerated.
/// An earlier revision froze the unresolvable set and checked only that
/// the record still cited it; a code review made a frozen citation
/// resolvable and the suite stayed green. That direction is the whole
/// point of freezing a set — a hole that quietly closes and stays
/// enumerated is a hole nobody goes back to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Resolution {
    /// Exactly one tracked file, and the cited range carries text.
    To(String),
    /// No tracked file ends in that name.
    NotTracked,
    /// Tracked files exist, none of them has the cited line.
    BeyondEndOfFile,
    /// It resolves, and the cited range is **empty** — a citation
    /// pointing at a blank line, which nothing can be anchored on.
    BlankTargetLine,
    /// Several candidates, and the citing line prints no identifier that
    /// separates them.
    AmbiguousBasename,
    /// The record cites this token more than once in one document and
    /// the resolver answers **differently** for two of the occurrences.
    ///
    /// An earlier revision assumed this could not happen — "every
    /// occurrence of a token names the same file and the same line, so
    /// one occurrence with evidence settles the others" — and kept the
    /// first answer, discarding the rest. A code review found three
    /// tokens where the answers differ, so the assumption was false and
    /// the loop was not checking the set. Two contradictory answers are
    /// not an answer: the citation is not resolvable by this rule, and
    /// it is frozen with this reason rather than silently taking
    /// whichever occurrence came first.
    OccurrencesDisagree,
}

impl Resolution {
    /// The word the frozen dataset records for a non-resolution.
    fn reason(&self) -> Option<&'static str> {
        match self {
            Resolution::To(_) => None,
            Resolution::NotTracked => Some("not_a_tracked_file"),
            Resolution::BeyondEndOfFile => Some("line_beyond_end_of_file"),
            Resolution::BlankTargetLine => Some("blank_target_line"),
            Resolution::AmbiguousBasename => Some("ambiguous_basename"),
            Resolution::OccurrencesDisagree => Some("occurrences_disagree"),
        }
    }
}

/// The verdict for one `(document, token)` key, over **every** occurrence
/// of it — never over the first one that answers.
///
/// * two occurrences resolving to different files → `OccurrencesDisagree`;
/// * exactly one file across all that resolve → `To(that file)`, and the
///   occurrences that carry no identifier are covered by the ones that
///   do, because a token names one target wherever it is written;
/// * none resolving → the most specific reason any occurrence gave, in
///   the order blank target, not tracked, beyond end of file, ambiguous.
///   The order is a precedence and not a preference: a blank target is a
///   fact about the cited line, an ambiguous basename is the absence of
///   evidence, and the first is the more useful thing to report.
fn key_verdict(resolutions: &[Resolution]) -> Resolution {
    let paths: BTreeSet<&String> = resolutions
        .iter()
        .filter_map(|r| match r {
            Resolution::To(p) => Some(p),
            _ => None,
        })
        .collect();
    match paths.len() {
        n if n > 1 => Resolution::OccurrencesDisagree,
        1 => Resolution::To((*paths.iter().next().expect("one path")).clone()),
        _ => {
            for want in [
                Resolution::BlankTargetLine,
                Resolution::NotTracked,
                Resolution::BeyondEndOfFile,
            ] {
                if resolutions.contains(&want) {
                    return want;
                }
            }
            Resolution::AmbiguousBasename
        }
    }
}

/// Every occurrence's resolution, grouped by `(document, token)`, in
/// document order.
fn resolutions_by_key(
    occurrences: &[Occurrence],
    tracked: &[String],
) -> BTreeMap<(String, String), Vec<(Occurrence, Resolution)>> {
    let mut out: BTreeMap<(String, String), Vec<(Occurrence, Resolution)>> = BTreeMap::new();
    for occ in occurrences {
        let r = resolve_citation(occ, tracked);
        out.entry((occ.doc.clone(), occ.token.clone()))
            .or_default()
            .push((occ.clone(), r));
    }
    out
}

/// Every spelling of one backticked token that could occur at a cited
/// line: the token itself, its leading identifier path, the last segment
/// of that path, and — for `foo()` — the definition `fn foo`.
fn needles(token: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let t = token.trim();
    if t.contains(".rs:") || t.starts_with("http") {
        return out;
    }
    out.insert(t.to_string());
    let head: &str = t
        .split([' ', '{', '(', '<', '['])
        .next()
        .unwrap_or("")
        .trim();
    if head.len() >= 3 {
        out.insert(head.to_string());
        if let Some((_, tail)) = head.rsplit_once("::") {
            out.insert(tail.to_string());
        }
    }
    if t.ends_with("()") && t.len() > 4 {
        out.insert(format!("fn {}", &t[..t.len() - 2]));
    }
    out.retain(|n| n.len() >= 3);
    out
}

/// The backticked tokens on one line.
fn backticked(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some((_, tail)) = rest.split_once('`') {
        match tail.split_once('`') {
            Some((inner, after)) => {
                if (2..=120).contains(&inner.chars().count()) {
                    out.push(inner.to_string());
                }
                rest = after;
            }
            None => break,
        }
    }
    out
}

fn resolve_citation(occ: &Occurrence, tracked: &[String]) -> Resolution {
    let base = occ.token.split(':').next().unwrap_or("");
    let qualified = base.contains('/');
    let candidates: Vec<&String> = tracked
        .iter()
        .filter(|t| *t == base || t.ends_with(&format!("/{base}")))
        .collect();
    if candidates.is_empty() {
        return Resolution::NotTracked;
    }
    let in_range: Vec<&&String> = candidates
        .iter()
        .filter(|t| read(t).lines().count() >= occ.last as usize)
        .collect();
    if in_range.is_empty() {
        return Resolution::BeyondEndOfFile;
    }
    let range_of = |path: &str| -> String {
        read(path)
            .lines()
            .skip(occ.first as usize - 1)
            .take((occ.last - occ.first + 1) as usize)
            .collect::<Vec<_>>()
            .join("\n")
    };
    let mut ns: BTreeSet<String> = BTreeSet::new();
    for tok in backticked(&occ.citing_line) {
        ns.extend(needles(&tok));
    }
    let scored: Vec<(usize, &String)> = in_range
        .iter()
        .map(|t| {
            let body = range_of(t);
            (ns.iter().filter(|n| body.contains(n.as_str())).count(), **t)
        })
        .collect();
    let top = scored.iter().map(|(s, _)| *s).max().unwrap_or(0);
    let best: Vec<&String> = scored
        .iter()
        .filter(|(s, _)| *s == top && top > 0)
        .map(|(_, t)| *t)
        .collect();
    let picked = if qualified && in_range.len() == 1 {
        Some((*in_range[0]).clone())
    } else if best.len() == 1 {
        Some(best[0].clone())
    } else if in_range.len() == 1 {
        Some((*in_range[0]).clone())
    } else {
        None
    };
    match picked {
        None => Resolution::AmbiguousBasename,
        Some(p) => {
            if ws(&range_of(&p)).is_empty() {
                Resolution::BlankTargetLine
            } else {
                Resolution::To(p)
            }
        }
    }
}

/// **Every citation the record makes still points at what it names.**
///
/// Each row's anchor must occur inside the cited line range of the file
/// it names. A citation whose target moves stops containing its anchor
/// and the check names the row.
///
/// **What it cannot see** is a citation whose anchor is right and whose
/// CLAIM is wrong: the anchor says the line carries this text, not that
/// the text means what the prose beside it says. A `prose` anchor is a
/// token the citing prose already prints, so the claim and its evidence
/// are reviewable side by side; a `line` anchor is a snapshot and it is
/// not. How many rows are of each kind is a property of the tree, so it
/// is stated in `docs/query-lowering.md` §12.3's census, which
/// `every_figure_section_12_3_states_is_the_one_the_datasets_hold`
/// derives; the `anchor_kind` column is what makes the difference
/// visible rather than assumed away.
#[test]
fn every_design_record_citation_still_points_at_what_it_names() {
    let rows = citation_rows();
    assert!(!rows.is_empty(), "{CITATIONS_TSV} is empty");
    let (mut prose, mut line) = (0usize, 0usize);
    for row in &rows {
        let src = read(&row.path);
        let lines: Vec<&str> = src.lines().collect();
        let last = row.end_line.unwrap_or(row.line) as usize;
        assert!(
            last <= lines.len(),
            "{} cites {}:{} but that file has {} lines",
            CITATIONS_TSV,
            row.path,
            last,
            lines.len()
        );
        let body = ws(&lines[row.line as usize - 1..last].join(" "));
        assert!(
            body.contains(&row.anchor),
            "{}:{} does not contain its anchor {:?}",
            row.path,
            row.line,
            row.anchor
        );
        match row.kind {
            AnchorKind::Prose => prose += 1,
            AnchorKind::Line => line += 1,
        }
    }
    assert!(
        prose > 0 && line > 0,
        "the dataset must carry both anchor kinds; it carries {prose} prose and {line} line"
    );
}

/// **Every citation in the record has a row, or is named as one that
/// cannot be resolved.**
///
/// The two datasets partition the record's citations, and the partition
/// is asserted in both directions: an occurrence that resolves to no row
/// and is not in the frozen set is a hole, and a frozen entry that has
/// started resolving must be removed rather than left as a standing
/// exemption. **That is what stops the uncovered set widening quietly**,
/// which is the failure mode a narrowed check invites.
#[test]
fn every_citation_in_the_design_record_has_a_row() {
    let rows = citation_rows();
    let frozen = unresolvable();
    let frozen_keys: BTreeSet<(String, String)> = frozen
        .iter()
        .map(|(d, t, _)| (d.clone(), t.clone()))
        .collect();
    // Keyed on `(document, citation token)`: the same basename and line
    // can resolve in one document and be ambiguous in another, so the
    // document is part of the key. A token that resolves anywhere in a
    // document is resolved for every occurrence of it in that document —
    // they all name the same file and the same line.
    let resolved: BTreeSet<(String, String)> = rows
        .iter()
        .map(|r| (r.doc.clone(), r.token.clone()))
        .collect();
    let both: Vec<&(String, String)> = resolved.intersection(&frozen_keys).collect();
    assert!(
        both.is_empty(),
        "{both:?} appear in BOTH datasets. The two must PARTITION the record's citations: a \
         citation covered by both rules is covered by neither"
    );

    let occurrences = citation_occurrences();
    assert!(
        occurrences.len() > 500,
        "only {} citations were found in the five artefacts; the reader is broken, not the record",
        occurrences.len()
    );
    let tracked = tracked_rust_files();
    let by_key = resolutions_by_key(&occurrences, &tracked);

    let mut problems: Vec<String> = Vec::new();
    for ((doc, token), group) in &by_key {
        let key = (doc.clone(), token.clone());
        let verdict = key_verdict(&group.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>());
        match &verdict {
            Resolution::To(path) => {
                // **Every occurrence is compared, not the first one that
                // answered.** An occurrence that carries no identifier
                // resolves to nothing and is covered by the ones that
                // do; an occurrence that resolves ELSEWHERE cannot
                // happen here, because that is `OccurrencesDisagree`.
                for (occ, r) in group {
                    if let Resolution::To(p) = r {
                        assert_eq!(
                            p, path,
                            "{doc}:{} cites {token}, which the key resolves to {path}",
                            occ.doc_line
                        );
                    }
                }
                if frozen_keys.contains(&key) {
                    problems.push(format!(
                        "{UNRESOLVABLE_TSV} freezes {doc} / {token}, which now RESOLVES to \
                         {path}. The frozen set is the enumerated hole, not a standing \
                         exemption: move the row into {CITATIONS_TSV}"
                    ));
                    continue;
                }
                match rows.iter().find(|x| x.doc == *doc && x.token == *token) {
                    None => problems.push(format!(
                        "{doc} cites {token}, which resolves to {path} and has no row in \
                         {CITATIONS_TSV}"
                    )),
                    Some(row) if row.path != *path => problems.push(format!(
                        "{doc} cites {token}, which resolves to {path}; {CITATIONS_TSV} records \
                         {}",
                        row.path
                    )),
                    Some(_) => {}
                }
            }
            other => {
                let want = other.reason().expect("a non-resolution has a reason");
                if resolved.contains(&key) {
                    problems.push(format!(
                        "{CITATIONS_TSV} carries {doc} / {token}, which no longer resolves \
                         ({want})"
                    ));
                    continue;
                }
                match frozen.iter().find(|(d, t, _)| d == doc && t == token) {
                    None => problems.push(format!(
                        "{doc} cites {token}, which resolves to nothing ({want}) and is in \
                         neither dataset"
                    )),
                    Some((_, _, got)) if got != want => problems.push(format!(
                        "{UNRESOLVABLE_TSV} gives {doc} / {token} the reason {got:?}; it is now \
                         {want:?}"
                    )),
                    Some(_) => {}
                }
            }
        }
    }
    assert!(
        problems.is_empty(),
        "{} citation problem(s):\n  {}",
        problems.len(),
        problems.join("\n  ")
    );
}

/// **No citation row is unused.**
///
/// A row nothing cites is a claim about a line that the record no longer
/// makes, and it would otherwise sit in the dataset looking like
/// coverage.
#[test]
fn no_citation_row_is_unused() {
    let rows = citation_rows();
    let cited: BTreeSet<(String, String)> = citation_occurrences()
        .into_iter()
        .map(|o| (o.doc, o.token))
        .collect();
    let unused: Vec<String> = rows
        .iter()
        .filter(|r| !cited.contains(&(r.doc.clone(), r.token.clone())))
        .map(|r| {
            format!(
                "{CITATIONS_TSV} row {}:{} is cited nowhere in {}",
                r.path, r.line, r.doc
            )
        })
        .collect();
    assert!(unused.is_empty(), "{}", unused.join("\n"));
    let stale: Vec<String> = unresolvable()
        .into_iter()
        .filter(|(d, t, _)| !cited.contains(&(d.clone(), t.clone())))
        .map(|(d, t, _)| format!("{UNRESOLVABLE_TSV} names {d} / {t}, which {d} no longer cites"))
        .collect();
    assert!(stale.is_empty(), "{}", stale.join("\n"));
}

// ---------------------------------------------------------------------
// Issue #492 part 8, landing 3 — the gate inventory
//
// The record names 28 `test(=…)` selectors and states, per gate, whether
// it exists. **That state was wrong for almost all of them.** 27 of the
// 28 resolve to a definition in the tree; the one that does not is
// `no_such_test_name_at_all_zzz`, the record's own negative control, so
// the finder discriminates rather than answering "present" to
// everything. The record's `at base` column was a measurement at a named
// commit and stays as one; part 8 added a `today` column beside it,
// which is what this check reads.
//
// **Resolution is by FILE, not by last path segment.** Two of the
// selectors share a final segment —
// `every_residual_state_effect_is_the_one_the_document_states` is
// defined in `logql/compile.rs` and in `traces/compile.rs` — and 308
// integration-test function names in this workspace are defined in more
// than one binary. A check resolving on the last segment would answer
// "present" for a gate that exists somewhere else entirely.
// ---------------------------------------------------------------------

const GATES_TSV: &str = "crates/pulsus-read/tests/design_record_gates.tsv";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateState {
    Exists,
    /// Absent from the tree on purpose. The record carries exactly one:
    /// its own negative control, which is what makes a finder that
    /// reports everything present detectable.
    Absent,
}

#[derive(Debug, Clone)]
struct GateRow {
    selector: String,
    krate: String,
    file: String,
    state: GateState,
}

fn gate_rows() -> Vec<GateRow> {
    let text = read(GATES_TSV);
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        if n == 0 {
            assert_eq!(line, "selector\tkrate\tfile\tstate", "{GATES_TSV} header");
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(f.len(), 4, "{GATES_TSV}:{}: four columns", n + 1);
        out.push(GateRow {
            selector: f[0].to_string(),
            krate: f[1].to_string(),
            file: f[2].to_string(),
            state: match f[3] {
                "exists" => GateState::Exists,
                "absent" => GateState::Absent,
                other => panic!("{GATES_TSV}:{}: unknown state {other:?}", n + 1),
            },
        });
    }
    out
}

/// Every `test(=…)` selector the five artefacts name, with the line it
/// sits on.
fn record_selectors() -> Vec<(String, String)> {
    let mut out = Vec::new();
    for doc in DESIGN_ARTEFACTS {
        for line in read(doc).lines() {
            let mut i = 0usize;
            while let Some(at) = line[i..].find("test(=") {
                let start = i + at + "test(=".len();
                if let Some(end) = line[start..].find(')') {
                    let sel = &line[start..start + end];
                    if !sel.contains('…') && !sel.is_empty() {
                        out.push((sel.to_string(), line.to_string()));
                    }
                    i = start + end;
                } else {
                    break;
                }
            }
        }
    }
    out
}

/// **The gate inventory cannot go stale.**
///
/// Three claims, each with two sides:
///
/// 1. every selector the record names has a row in [`GATES_TSV`], and
///    every row is named by the record — so the dataset cannot carry a
///    gate the record has stopped naming, or miss one it has started to;
/// 2. a row marked `exists` names a file that defines the function
///    **exactly once**, and a row marked `absent` names a function no
///    tracked file defines anywhere;
/// 3. where the record states the gate's state in a table of its own —
///    the `today` column of §11.1 to §11.4 — that word agrees with the
///    tree.
///
/// Claim 3 is the one that caught the record: before part 8 it called 24
/// of its own gates `wave 1` while they existed.
#[test]
fn every_gate_the_record_names_exists_or_is_marked_absent() {
    let rows = gate_rows();
    let named = record_selectors();
    assert!(
        named.len() >= 28,
        "only {} selectors were read out of the five artefacts",
        named.len()
    );
    let dataset: BTreeSet<&str> = rows.iter().map(|r| r.selector.as_str()).collect();
    let recorded: BTreeSet<&str> = named.iter().map(|(s, _)| s.as_str()).collect();
    let missing: Vec<&&str> = recorded.difference(&dataset).collect();
    assert!(
        missing.is_empty(),
        "the record names {missing:?}, which has no row in {GATES_TSV}"
    );
    let unused: Vec<&&str> = dataset.difference(&recorded).collect();
    assert!(
        unused.is_empty(),
        "{GATES_TSV} carries {unused:?}, which the record names nowhere"
    );

    let tracked = tracked_rust_files();
    let mut absent_rows = 0usize;
    for row in &rows {
        let segment = row.selector.rsplit("::").next().expect("a selector");
        let needle = format!("fn {segment}(");
        match row.state {
            GateState::Exists => {
                assert!(
                    row.file.starts_with(&format!("crates/{}/", row.krate)),
                    "{}: the file {:?} is not in the crate {:?} the row names",
                    row.selector,
                    row.file,
                    row.krate
                );
                let hits = read(&row.file).matches(&needle).count();
                assert_eq!(
                    hits, 1,
                    "{GATES_TSV} says {} exists in {}, where `{needle}` occurs {hits} times",
                    row.selector, row.file
                );
            }
            GateState::Absent => {
                absent_rows += 1;
                let defining: Vec<&String> = tracked
                    .iter()
                    .filter(|f| read(f).contains(&needle))
                    .collect();
                assert!(
                    defining.is_empty(),
                    "{GATES_TSV} calls {} absent, but {defining:?} define it",
                    row.selector
                );
            }
        }
    }
    assert_eq!(
        absent_rows, 1,
        "the record carries exactly one deliberately absent gate — its own negative control. \
         Without it, a finder that answered \"present\" to everything would look correct"
    );

    // Claim 3: the document's own state word, where it states one.
    let mut checked = 0usize;
    for (selector, line) in &named {
        if !line.starts_with('|') {
            continue;
        }
        let cells: Vec<&str> = line.trim_matches('|').split(" | ").map(str::trim).collect();
        let Some(today) = cells.last() else { continue };
        if !today.contains("exists") && !today.contains("absent") {
            continue;
        }
        let row = rows
            .iter()
            .find(|r| r.selector == *selector)
            .expect("every named selector has a row, asserted above");
        let says_exists = today.contains("exists");
        assert_eq!(
            says_exists,
            row.state == GateState::Exists,
            "docs/query-lowering.md calls {selector} {today:?}, and the tree says {:?}",
            row.state
        );
        checked += 1;
    }
    assert!(
        checked >= 25,
        "only {checked} of the record's own state cells were checked; §11.1 to §11.4 carry 25 \
         rows with a selector"
    );
}

/// Every tracked `.rs` file, from `git ls-files`.
fn tracked_rust_files() -> Vec<String> {
    let out = std::process::Command::new("git")
        .args(["ls-files", "*.rs"])
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

/// Rewrites both citation datasets from [`resolve_citation`]. Ignored, so
/// it never runs in CI.
///
/// **The resolver is the definition and this is the only producer.** An
/// earlier revision generated the datasets from a script that lived
/// beside the repository and checked them with a reader written here;
/// the two drifted on two citations, which is the two-implementations
/// problem in miniature. There is one implementation now, it ships in
/// this file, and anyone can re-run it.
///
/// **Running it is not a way to make a red check green.** The `line` and
/// `anchor` of a resolved row are what the DOCUMENT claims, so a target
/// that moves means the record's citation is stale and a person has to
/// re-read it; re-running this would rewrite the claim to match whatever
/// the source had become. The diff is the review.
///
/// ```text
/// cargo test -p pulsus-read --test design_record_drift_gate -- --ignored
/// ```
#[test]
#[ignore = "writes the two citation datasets"]
fn regenerate_the_citation_datasets() {
    let tracked = tracked_rust_files();
    let occurrences = citation_occurrences();
    // The SAME key verdict the check uses, over every occurrence.
    let by_key = resolutions_by_key(&occurrences, &tracked);
    let verdict: BTreeMap<(String, String), (Resolution, Occurrence)> = by_key
        .iter()
        .map(|(k, group)| {
            let v = key_verdict(&group.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>());
            // The anchor comes from the occurrence that resolved, so it
            // is a token that citing line actually prints.
            let occ = group
                .iter()
                .find(|(_, r)| matches!((r, &v), (Resolution::To(a), Resolution::To(b)) if a == b))
                .map(|(o, _)| o.clone())
                .unwrap_or_else(|| group[0].0.clone());
            (k.clone(), (v, occ))
        })
        .collect();
    let mut resolved = String::from("doc\ttoken\tpath\tline\tend_line\tanchor_kind\tanchor\n");
    let mut frozen = String::from("doc\ttoken\treason\n");
    for ((doc, token), (r, occ)) in &verdict {
        match r {
            Resolution::To(path) => {
                let body: String = read(path)
                    .lines()
                    .skip(occ.first as usize - 1)
                    .take((occ.last - occ.first + 1) as usize)
                    .collect::<Vec<_>>()
                    .join(" ");
                let body = ws(&body);
                // Prefer an anchor the CITING prose prints: the claim and
                // its evidence are then reviewable side by side. Longest
                // first, so the most specific spelling wins.
                let mut prose: Vec<String> = backticked(&occ.citing_line)
                    .iter()
                    .flat_map(|t| needles(t))
                    .filter(|n| body.contains(n.as_str()) && !n.contains('\t'))
                    .collect();
                prose.sort_by(|a, b| b.len().cmp(&a.len()).then(a.cmp(b)));
                let (kind, anchor) = match prose.first() {
                    Some(a) => ("prose", a.clone()),
                    None => (
                        "line",
                        body.chars()
                            .take(120)
                            .collect::<String>()
                            .trim()
                            .to_string(),
                    ),
                };
                let end = if occ.last == occ.first {
                    String::new()
                } else {
                    occ.last.to_string()
                };
                resolved.push_str(&format!(
                    "{doc}\t{token}\t{path}\t{}\t{end}\t{kind}\t{anchor}\n",
                    occ.first
                ));
            }
            other => frozen.push_str(&format!(
                "{doc}\t{token}\t{}\n",
                other.reason().expect("a non-resolution has a reason")
            )),
        }
    }
    std::fs::write(repo_root().join(CITATIONS_TSV), resolved).expect("write the resolved dataset");
    std::fs::write(repo_root().join(UNRESOLVABLE_TSV), frozen).expect("write the frozen dataset");
}

// ---------------------------------------------------------------------
// The fallback that was rejected, and the nine read cases that rejected
// it. No rate is computed here: an earlier revision judged the fallback
// against `resolve_citation`, which is the other rule under test.
// ---------------------------------------------------------------------

/// The language a document section is about, from the nearest heading
/// above the citation that names one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SectionLanguage {
    LogQl,
    TraceQl,
}

impl SectionLanguage {
    /// The path fragments a candidate file must carry to be preferred.
    fn prefers(self) -> [&'static str; 2] {
        match self {
            SectionLanguage::LogQl => ["logql/", "pulsus-logql/"],
            SectionLanguage::TraceQl => ["traces/", "pulsus-traceql/"],
        }
    }
}

fn section_language(doc: &str, doc_line: u32) -> Option<SectionLanguage> {
    let text = read(doc);
    let mut current = None;
    for (i, line) in text.lines().enumerate() {
        if i as u32 + 1 > doc_line {
            break;
        }
        if line.starts_with('#') {
            let low = line.to_lowercase();
            if low.contains("traceql") {
                current = Some(SectionLanguage::TraceQl);
            } else if low.contains("logql") {
                current = Some(SectionLanguage::LogQl);
            }
        }
    }
    current
}

/// What a person concluded after reading one citation's prose against
/// both candidate files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReviewedVerdict {
    /// The fallback's answer is not the file the citing prose describes.
    FallbackWrong,
    /// The fallback's answer IS the file the citing prose describes, and
    /// [`resolve_citation`] is the one that points elsewhere.
    FallbackRight,
    /// The citing sentence describes both candidates, so neither answer
    /// is wrong and neither is evidence.
    Ambiguous,
}

/// **Every citation where the language fallback and [`resolve_citation`]
/// disagree, with a verdict a person reached by reading the citing prose
/// against both candidate files.**
///
/// The key is `(document, token, nth occurrence of that token in that
/// document among the citations that need a rule at all)` — line numbers
/// move whenever the record is re-wrapped, and two occurrences of one
/// token can be about different things. "Need a rule at all" means a
/// bare basename with more than one candidate file that has the cited
/// line; a path-qualified citation, or one with a single candidate, is
/// not counted, so the index is stable against citations elsewhere in
/// the document gaining or losing a path.
///
/// **This table exists because the earlier measurement judged the
/// fallback against `resolve_citation`, which is the other rule under
/// test.** A rate computed that way says how often two rules differ, not
/// how often either is wrong, and a code review reading the nine cases
/// found three where the resolver was the one pointing at the wrong
/// file. So no rate is published. These are the cases, and the note on
/// each is the reasoning.
const REVIEWED_FALLBACK_DIVERGENCES: [(&str, &str, usize, ReviewedVerdict, &str); 9] = [
    (
        "docs/query-lowering.md",
        "exec.rs:2869",
        0,
        ReviewedVerdict::FallbackWrong,
        "a LogQL section citing the TraceQL search executor's generator settings;          crates/pulsus-read/src/logql/exec.rs has no such thing",
    ),
    (
        "docs/query-lowering.md",
        "exec.rs:2869",
        2,
        ReviewedVerdict::FallbackWrong,
        "the same citation again, in the same section, with the same answer",
    ),
    (
        "docs/query-lowering.md",
        "exec.rs:2830-2836",
        0,
        ReviewedVerdict::FallbackWrong,
        "the search settings block the same section quotes; it is in traces/exec.rs",
    ),
    (
        "docs/query-lowering.md",
        "exec.rs:701",
        0,
        ReviewedVerdict::FallbackWrong,
        "a LogQL section citing a line of the TraceQL executor",
    ),
    (
        "docs/query-to-sql.md",
        "labels.rs:157-189",
        0,
        ReviewedVerdict::FallbackWrong,
        "the sentence describes the label ENCODER; the fallback answers          crates/pulsus-read/src/logql/labels.rs, which is not where it lives",
    ),
    (
        "docs/query-to-sql.md",
        "labels.rs:157-189",
        1,
        ReviewedVerdict::Ambiguous,
        "this sentence describes both the writer and the flat reader; the fallback points at          the flat reader, so neither answer is wrong and the case is evidence for neither rule",
    ),
    (
        "docs/query-to-sql.md",
        "labels.rs:363",
        0,
        ReviewedVerdict::FallbackRight,
        "the citing prose describes merge_labels_with_structured_metadata, which is in          crates/pulsus-read/src/logql/labels.rs — the fallback's answer. The resolver points          at metrics/labels.rs",
    ),
    (
        "docs/query-to-sql.md",
        "sql.rs:996",
        2,
        ReviewedVerdict::FallbackRight,
        "the citing prose describes metric_raw_samples_sliding, in          crates/pulsus-read/src/logql/sql.rs — the fallback's answer",
    ),
    (
        "docs/query-to-sql.md",
        "sql.rs:489",
        3,
        ReviewedVerdict::FallbackRight,
        "the citing prose describes stage2, in crates/pulsus-read/src/logql/sql.rs — the          fallback's answer; metrics/sql.rs:489 is a test literal",
    ),
];

/// **Where the language fallback and the anchor rule disagree, and what a
/// person concluded about each.**
///
/// Most of the record's citations name a bare basename and six of those
/// basenames match more than one tracked file. [`resolve_citation`]
/// answers the ones whose citing line prints an identifier the cited
/// line carries. The obvious next rule for the rest is the enclosing
/// section's language: a `plan.rs` citation in a LogQL section means
/// `logql/plan.rs`. **No count is written here** — how many citations
/// are of each kind moves with the record, so it is derived in §12.3's
/// census rather than frozen in a comment.
///
/// **No rate is published, and an earlier revision of this test published
/// one that was measured against itself.** It called `resolve_citation`
/// the truth and counted how often the fallback differed from it, which
/// measures disagreement between two rules rather than error in either.
/// Read one at a time, three of the nine divergences are cases where the
/// **resolver** points at the wrong file.
///
/// What this test asserts instead: the divergence set is exactly the
/// nine reviewed in [`REVIEWED_FALLBACK_DIVERGENCES`], so a new one
/// cannot appear without a person reading it; and five of the nine are
/// citations where the fallback answers a file the citing prose does not
/// describe. **Five wrong answers out of nine disagreements is why the
/// fallback is not applied** — not a percentage, five cases anyone can
/// read.
#[test]
fn the_language_fallback_disagrees_with_the_anchor_rule_only_where_a_person_has_ruled() {
    let tracked = tracked_rust_files();
    let mut seen: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut found: Vec<(String, String, usize, String, String)> = Vec::new();

    for occ in citation_occurrences() {
        let base = occ.token.split(':').next().unwrap_or("");
        if base.contains('/') {
            continue; // already path-qualified: no rule is needed
        }
        let candidates: Vec<&String> = tracked
            .iter()
            .filter(|t| t.ends_with(&format!("/{base}")))
            .filter(|t| read(t).lines().count() >= occ.last as usize)
            .collect();
        if candidates.len() < 2 {
            continue; // one candidate: no rule is needed to choose
        }
        let nth = {
            let key = (occ.doc.clone(), occ.token.clone());
            let n = seen.entry(key).or_insert(0);
            let v = *n;
            *n += 1;
            v
        };
        let Some(lang) = section_language(&occ.doc, occ.doc_line) else {
            continue; // the fallback declines: not an answer, right or wrong
        };
        let preferred: Vec<&&String> = candidates
            .iter()
            .filter(|t| lang.prefers().iter().any(|p| t.contains(p)))
            .collect();
        if preferred.len() != 1 {
            continue; // the fallback declines
        }
        let Resolution::To(anchor_answer) = resolve_citation(&occ, &tracked) else {
            continue; // nothing to disagree with
        };
        if **preferred[0] != anchor_answer {
            found.push((
                occ.doc.clone(),
                occ.token.clone(),
                nth,
                (**preferred[0]).clone(),
                anchor_answer,
            ));
        }
    }

    let got: BTreeSet<(String, String, usize)> = found
        .iter()
        .map(|(d, t, n, _, _)| (d.clone(), t.clone(), *n))
        .collect();
    let reviewed: BTreeSet<(String, String, usize)> = REVIEWED_FALLBACK_DIVERGENCES
        .iter()
        .map(|(d, t, n, _, _)| (d.to_string(), t.to_string(), *n))
        .collect();
    let unreviewed: Vec<&(String, String, usize)> = got.difference(&reviewed).collect();
    assert!(
        unreviewed.is_empty(),
        "the fallback and the anchor rule now disagree on {unreviewed:?}, which nobody has read.          Read the citing prose against both candidate files and add the case with its verdict —          this table is a record of judgements, and it is not something a rule may fill in"
    );
    let gone: Vec<&(String, String, usize)> = reviewed.difference(&got).collect();
    assert!(
        gone.is_empty(),
        "REVIEWED_FALLBACK_DIVERGENCES carries {gone:?}, where the two rules now agree; remove          the reviewed case rather than leaving a judgement about a citation that no longer          diverges"
    );

    let wrong: Vec<&(&str, &str, usize, ReviewedVerdict, &str)> = REVIEWED_FALLBACK_DIVERGENCES
        .iter()
        .filter(|(_, _, _, v, _)| *v == ReviewedVerdict::FallbackWrong)
        .collect();
    let right = REVIEWED_FALLBACK_DIVERGENCES
        .iter()
        .filter(|(_, _, _, v, _)| *v == ReviewedVerdict::FallbackRight)
        .count();
    let ambiguous = REVIEWED_FALLBACK_DIVERGENCES
        .iter()
        .filter(|(_, _, _, v, _)| *v == ReviewedVerdict::Ambiguous)
        .count();
    eprintln!(
        "fallback divergences: {} reviewed — {} the fallback answers wrongly, {right} where the \
         ANCHOR RULE is the one that is wrong, {ambiguous} where the sentence describes both",
        REVIEWED_FALLBACK_DIVERGENCES.len(),
        wrong.len()
    );
    for (doc, token, nth, _, note) in &wrong {
        eprintln!("  FALLBACK WRONG {doc} / {token} (occurrence {nth}): {note}");
    }
    assert_eq!(
        (wrong.len(), right, ambiguous),
        (5, 3, 1),
        "the reviewed verdicts moved; re-read §12.3's decision against them"
    );
}

const CENSUS_BLOCK_BEGIN: &str = "<!-- generated from the citation datasets -->";
const CENSUS_BLOCK_END: &str = "<!-- end generated -->";

/// **§12.3's whole numeric block — three tables and the sentences that
/// state numbers about them — rendered from the datasets.**
///
/// An earlier revision derived the table cells and left the sentences
/// beside them as prose; a code review changed a prose count and the
/// suite stayed green. Gating prose by pattern is not the fix: numbers
/// in English are unbounded, so a pattern that catches today's sentences
/// misses tomorrow's and looks like coverage. The sentences are
/// generated, so there is nothing for a person to write a number into.
fn census_block() -> String {
    let rows = citation_rows();
    let frozen = unresolvable();
    let resolved_keys: BTreeSet<(String, String)> = rows
        .iter()
        .map(|r| (r.doc.clone(), r.token.clone()))
        .collect();
    let frozen_keys: BTreeSet<(String, String)> = frozen
        .iter()
        .map(|(d, t, _)| (d.clone(), t.clone()))
        .collect();
    let occurrences = citation_occurrences();
    let bare = occurrences
        .iter()
        .filter(|o| !o.token.split(':').next().unwrap_or("").contains('/'))
        .count();
    let covered = |keys: &BTreeSet<(String, String)>| {
        occurrences
            .iter()
            .filter(|o| keys.contains(&(o.doc.clone(), o.token.clone())))
            .count()
    };
    let prose = rows.iter().filter(|r| r.kind == AnchorKind::Prose).count();
    let line = rows.iter().filter(|r| r.kind == AnchorKind::Line).count();
    let resolved_occ = covered(&resolved_keys);
    let frozen_occ = covered(&frozen_keys);

    let mut out = String::from(CENSUS_BLOCK_BEGIN);
    out.push_str("\n\n| quantity | at this revision |\n|---|---|\n");
    for (label, n) in [
        (
            "citation occurrences in the five artefacts",
            occurrences.len(),
        ),
        ("of those, citing a bare basename", bare),
        (
            "`(document, token)` pairs the rule resolves",
            resolved_keys.len(),
        ),
        ("occurrences those resolved pairs cover", resolved_occ),
        (
            "`(document, token)` pairs it cannot resolve",
            frozen_keys.len(),
        ),
        ("occurrences those frozen pairs cover", frozen_occ),
        (
            "resolved rows anchored on a token the citing prose prints",
            prose,
        ),
        (
            "resolved rows anchored on a snapshot of the cited line",
            line,
        ),
    ] {
        out.push_str(&format!("| {label} | {n} |\n"));
    }

    let mut by_reason: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, _, r) in &frozen {
        *by_reason.entry(r.as_str()).or_default() += 1;
    }
    out.push_str("\n| reason it cannot be resolved | pairs | what it means |\n|---|---|---|\n");
    for (reason, n) in &by_reason {
        out.push_str(&format!(
            "| `{reason}` | {n} | {} |\n",
            reason_meaning(reason)
        ));
    }

    out.push_str("\n| the reviewed verdict on a fallback disagreement | cases |\n|---|---|\n");
    for (label, verdict) in VERDICT_LABELS {
        let n = REVIEWED_FALLBACK_DIVERGENCES
            .iter()
            .filter(|(_, _, _, v, _)| *v == verdict)
            .count();
        out.push_str(&format!("| {label} | {n} |\n"));
    }
    out.push_str(&format!(
        "\n| anchor kind | what a row of that kind can show |\n|---|---|\n{}",
        ANCHOR_KIND_MEANINGS
            .iter()
            .map(|(k, m)| format!("| `{k}` | {m} |\n"))
            .collect::<String>()
    ));

    out.push_str(&format!(
        "\nOf the {} citation occurrences the five artefacts make, {bare} name a bare basename. \
         The rule resolves {} `(document, token)` pairs covering {resolved_occ} occurrences, and \
         cannot resolve {} covering {frozen_occ}. Of the resolved rows, {prose} are anchored on a \
         token the citing prose prints and {line} on a snapshot of the cited line.\n\n",
        occurrences.len(),
        resolved_keys.len(),
        frozen_keys.len()
    ));
    let wrong = REVIEWED_FALLBACK_DIVERGENCES
        .iter()
        .filter(|(_, _, _, v, _)| *v == ReviewedVerdict::FallbackWrong)
        .count();
    out.push_str(&format!(
        "The language fallback and the anchor rule disagree on {} citations, all of them read one \
         at a time. {wrong} are citations where the fallback answers a file the citing prose does \
         not describe, which is why it is not applied.\n\n",
        REVIEWED_FALLBACK_DIVERGENCES.len()
    ));
    // **The short enumerations belong in here too.** Listing the
    // blank-target citations, the conflicting ones and the wrong
    // fallback answers in the prose beside this block put derived
    // content one line outside a generated region, which is the same
    // defect the region exists to close.
    for (reason, lead) in [
        (
            "blank_target_line",
            "The citations pointing at an empty line are",
        ),
        (
            "occurrences_disagree",
            "The citations the rule answers differently for two occurrences of are",
        ),
    ] {
        let mut listed: Vec<String> = frozen
            .iter()
            .filter(|(_, _, r)| r == reason)
            .map(|(d, t, _)| {
                let docs = frozen
                    .iter()
                    .filter(|(_, t2, r2)| t2 == t && r2 == reason)
                    .count();
                if docs > 1 {
                    format!("`{t}` (cited from {docs} documents)")
                } else {
                    format!("`{t}` (in `{d}`)")
                }
            })
            .collect();
        listed.sort();
        listed.dedup();
        out.push_str(&format!("{lead} {}.\n\n", listed.join(", ")));
    }
    let mut wrong: Vec<String> = REVIEWED_FALLBACK_DIVERGENCES
        .iter()
        .filter(|(_, _, _, v, _)| *v == ReviewedVerdict::FallbackWrong)
        .map(|(d, t, _, _, _)| format!("`{t}` in `{d}`"))
        .collect();
    wrong.sort();
    wrong.dedup();
    out.push_str(&format!(
        "The citations where the fallback answers a file the citing prose does not describe are \
         {}. Each is named with its reasoning in `REVIEWED_FALLBACK_DIVERGENCES`, and the test \
         prints them when it runs.\n\n",
        wrong.join(", ")
    ));
    out.push_str(CENSUS_BLOCK_END);
    out
}

/// What each frozen reason means. **Generated beside the label**, so the
/// label set the document shows and the label set the dataset holds are
/// one list rather than two. An earlier revision listed the labels in
/// prose beside the table, and a code review renamed one and every suite
/// stayed green: a list of row labels carries the same drift risk as a
/// count of them, and it fell outside a sweep drawn around numbers.
fn reason_meaning(reason: &str) -> &'static str {
    match reason {
        "ambiguous_basename" => {
            "the basename matches several tracked files and the citing line prints no identifier \
             that separates them"
        }
        "blank_target_line" => {
            "the cited line exists and is **empty**, so there is nothing to anchor on"
        }
        "occurrences_disagree" => {
            "the record cites the token more than once in one document and the rule answers \
             differently for two of those occurrences"
        }
        "not_a_tracked_file" => {
            "the citation names a throwaway probe that was never committed, which §10 records \
             deliberately"
        }
        other => panic!(
            "the frozen dataset holds the reason {other:?}, which this renderer cannot explain; a \
             new reason needs its sentence here, not a note beside the table"
        ),
    }
}

/// The verdict rows, label and variant together.
const VERDICT_LABELS: [(&str, ReviewedVerdict); 3] = [
    (
        "the fallback answers a file the citing prose does not describe",
        ReviewedVerdict::FallbackWrong,
    ),
    (
        "the fallback is right and the anchor rule points elsewhere",
        ReviewedVerdict::FallbackRight,
    ),
    (
        "the sentence describes both candidates, so neither answer is wrong",
        ReviewedVerdict::Ambiguous,
    ),
];

/// What each anchor kind can and cannot show.
const ANCHOR_KIND_MEANINGS: [(&str, &str); 2] = [
    (
        "prose",
        "a token the citing prose prints, so the claim and its evidence are reviewable side by \
         side",
    ),
    (
        "line",
        "a snapshot of the cited line, taken because the citing prose prints no such token: it \
         detects the line moving or changing and cannot show the citation means the right thing",
    ),
];

fn census_block_in(md: &str) -> String {
    let a = md
        .find(CENSUS_BLOCK_BEGIN)
        .unwrap_or_else(|| panic!("{QUERY_LOWERING} must carry {CENSUS_BLOCK_BEGIN}"));
    let b = md[a..]
        .find(CENSUS_BLOCK_END)
        .unwrap_or_else(|| panic!("the census block is not closed"));
    md[a..a + b + CENSUS_BLOCK_END.len()].to_string()
}

/// **Every figure §12.3 states is the one the datasets hold** — and the
/// sentences that state them are generated, not merely parsed.
#[test]
fn every_figure_section_12_3_states_is_the_one_the_datasets_hold() {
    let md = read(QUERY_LOWERING);
    assert_eq!(
        census_block_in(&md),
        census_block(),
        "the census block in {QUERY_LOWERING} is not what the citation datasets render. It is \
         GENERATED — run the ignored `regenerate_the_census_block` and read the diff, rather than \
         editing the document"
    );
}

/// Writes the generated census block into `docs/query-lowering.md`.
/// Ignored, so it never runs in CI.
#[test]
#[ignore = "writes the generated census block in docs/query-lowering.md"]
fn regenerate_the_census_block() {
    let md = read(QUERY_LOWERING);
    let old = census_block_in(&md);
    std::fs::write(
        repo_root().join(QUERY_LOWERING),
        md.replace(&old, &census_block()),
    )
    .expect("write the document");
}

/// Every tracked file this workspace treats as a committed dataset:
/// the tabular ones the tests read and the evidence ones the benchmarks
/// write. **Discovered from the tree, not listed here** — an earlier
/// revision held a five-path array and called the result derived, and a
/// code review was right that a hand-listed input makes the whole
/// derivation a hand list.
fn committed_dataset_files() -> Vec<String> {
    let out = std::process::Command::new("git")
        .args([
            "ls-files",
            "crates/pulsus-read/tests/*.tsv",
            "docs/benchmarks/data/*.tsv",
            "docs/benchmarks/data/*.json",
        ])
        .current_dir(repo_root())
        .output()
        .expect("git ls-files");
    assert!(out.status.success(), "git ls-files failed");
    let files: Vec<String> = String::from_utf8(out.stdout)
        .expect("utf-8")
        .lines()
        .map(str::to_string)
        .collect();
    assert!(
        files.len() >= 5,
        "only {} dataset files were discovered; the glob is wrong, not the tree",
        files.len()
    );
    files
}

/// Every string a committed dataset holds — every cell of every tabular
/// one, and every key and every string or number value at every depth of
/// every JSON one.
///
/// An earlier revision took the keys of the FIRST row of one JSON file
/// and nothing else, which is why it rejected `generator` — a stage name
/// the artefact holds on 1,132 rows — as a name no dataset holds. **A
/// check that rejects a true value gets silenced by whoever meets it
/// next**, so the traversal is now total.
fn every_value_a_dataset_holds() -> BTreeSet<String> {
    fn walk(v: &serde_json::Value, out: &mut BTreeSet<String>) {
        match v {
            serde_json::Value::Object(m) => {
                for (k, child) in m {
                    out.insert(k.clone());
                    walk(child, out);
                }
            }
            serde_json::Value::Array(a) => {
                for child in a {
                    walk(child, out);
                }
            }
            serde_json::Value::String(s) => {
                out.insert(s.clone());
            }
            other => {
                out.insert(other.to_string());
            }
        }
    }
    let mut out = BTreeSet::new();
    for f in committed_dataset_files() {
        let text = read(&f);
        if f.ends_with(".json") {
            let v: serde_json::Value =
                serde_json::from_str(&text).unwrap_or_else(|e| panic!("{f} must parse: {e}"));
            walk(&v, &mut out);
        } else {
            for line in text.lines() {
                for cell in line.split('\t') {
                    out.insert(cell.trim().to_string());
                }
            }
        }
    }
    out
}

/// The label SETS a tabular dataset holds: the distinct values of each
/// column whose values are all snake_case.
///
/// The snake_case test is what separates a label column from a free-text
/// one — `reason`, `anchor_kind`, `state`, `rendering` and `scope`
/// qualify; `doc`, `token`, `path`, `anchor` and `provenance` carry
/// slashes, dots and spaces and do not. It is a property of the values
/// rather than a threshold on how many there are.
fn label_sets() -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    for f in committed_dataset_files() {
        if !f.ends_with(".tsv") {
            continue;
        }
        let text = read(&f);
        let mut lines = text.lines();
        let header: Vec<&str> = lines.next().unwrap_or("").split('\t').collect();
        let rows: Vec<Vec<&str>> = lines
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.split('\t').collect())
            .collect();
        for (i, name) in header.iter().enumerate() {
            let mut seen: Vec<String> = Vec::new();
            let mut all_snake = true;
            for r in &rows {
                let Some(cell) = r.get(i) else { continue };
                if !is_snake_case(cell) {
                    all_snake = false;
                    break;
                }
                if !seen.iter().any(|s| s == cell) {
                    seen.push((*cell).to_string());
                }
            }
            if all_snake && seen.len() >= 2 {
                out.push((format!("{f}:{name}"), seen));
            }
        }
    }
    out
}

/// **No complete label set a dataset holds is repeated outside a
/// generated region.**
///
/// This is the duplication sweep, and it is a committed test because an
/// uncommitted one is a description of a sweep rather than a sweep — a
/// code review had to reconstruct the previous one from prose to test it
/// at all, and could then only measure its own reconstruction.
///
/// **How a member counts as named.** A label of two words or more counts
/// wherever the swept text has a word **starting with** each of the
/// label's words, in the label's order, inside a window of
/// [`LABEL_MENTION_SPAN`] words — in any punctuation, any case, and with
/// other words in between. So `ambiguous_basename`,
/// `Ambiguous-Basename`, "ambiguous basenames" and "an ambiguous
/// basename" all count. A label that is a single ordinary word, like the
/// anchor kinds `line` and `prose`, counts only inside backticks,
/// because otherwise every sentence containing the word "line" would
/// name it.
///
/// **Starting with is not the same as being, and that is loose in one
/// direction.** It is what lets an inflected form count, and it also
/// lets an unrelated word count whenever it happens to begin with a
/// label word. Take `not_a_tracked_file`: "nothing" begins with `not`,
/// "and" begins with `a`, "filenames" begins with `file`, and so
/// "... and nothing and tracked filenames" is read as naming that label
/// although not one of those three words is the label's. A code review
/// measured exactly that sentence. Short members are where it bites —
/// `a` is the start of every word beginning with that letter. The effect
/// runs one way only: this makes the check say "duplicated" more often
/// than a reader of the labels would, never less.
///
/// **Three things it does not catch.** Each is a case where the set is
/// there for a reader and absent from the words:
///
/// 1. **A word replaced by a synonym.** One sentence naming all four
///    reasons as "an uncertain basename, a blank target line,
///    occurrences that disagree, and a token that is not a tracked file"
///    leaves this green; changing that one word back to "ambiguous"
///    reddens it. Catching the synonym means a thesaurus, and a check
///    whose verdict depends on one is a check nobody can predict, so the
///    limit is taken rather than closed.
/// 2. **The words out of order, or further apart than
///    [`LABEL_MENTION_SPAN`] words.** "The basename is ambiguous" does
///    not count.
/// 3. **An ordering described without naming its members.** "The reasons
///    are listed commonest first" names none of them, so this sees
///    nothing: a set is repeated when all of it is there, and that is
///    none of it. Reordering members is invisible for the same reason —
///    the rule is about the set, and the generated block holds the order.
#[test]
fn no_label_set_a_dataset_holds_is_duplicated_outside_a_generated_region() {
    let md = read(QUERY_LOWERING);
    let swept = swept_text(&md);
    assert!(
        swept.len() > 40,
        "only {} lines were swept; the region markers moved and the sweep looks at nothing",
        swept.len()
    );
    let flat: String = swept
        .iter()
        .map(|(_, l)| l.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let flat_words = words(&flat);
    let backticked: BTreeSet<String> = {
        let mut out = BTreeSet::new();
        for (_, l) in &swept {
            let mut rest = l.as_str();
            while let Some((_, tail)) = rest.split_once('`') {
                match tail.split_once('`') {
                    Some((tok, after)) => {
                        out.insert(tok.to_string());
                        rest = after;
                    }
                    None => break,
                }
            }
        }
        out
    };

    // A multi-word label — `ambiguous_basename`, or the verdict phrase
    // "the fallback answers" — cannot turn up in English by accident, so
    // it counts wherever the swept text says it, in any punctuation and
    // any case: `Ambiguous-Basename` and the bare words "ambiguous
    // basename" both normalise onto the label. A single ordinary word,
    // like the anchor kinds `line` and `prose`, counts only inside
    // backticks, because otherwise every sentence containing the word
    // "line" would name it.
    let named = |v: &str| -> bool {
        let w = words(v);
        if w.len() > 1 {
            names_label(&flat_words, &w)
        } else {
            backticked.contains(v)
        }
    };

    let mut sets: Vec<(String, Vec<String>)> = label_sets();
    // The verdict rows are a label set too; their members are phrases
    // rather than identifiers, and a phrase cannot occur by accident.
    sets.push((
        "VERDICT_LABELS".to_string(),
        VERDICT_LABELS
            .iter()
            .map(|(l, _)| (*l).to_string())
            .collect(),
    ));
    assert!(
        sets.len() >= 4,
        "only {} label sets were derived from the committed datasets; the column test found \
         nothing and the sweep would pass on an empty domain",
        sets.len()
    );
    if std::env::var_os("PULSUS_PRINT_LABEL_SETS").is_some() {
        for (name, members) in &sets {
            println!("label set {name}: {members:?}");
        }
    }

    let mut duplicated: Vec<String> = Vec::new();
    for (name, members) in &sets {
        let present: Vec<&String> = members.iter().filter(|m| named(m)).collect();
        if present.len() == members.len() {
            duplicated.push(format!(
                "the complete label set {name} ({members:?}) is repeated outside a generated \
                 region; move it inside, or name fewer than all of it"
            ));
        }
    }
    assert!(
        duplicated.is_empty(),
        "{} label set(s) are duplicated outside a generated region:\n  {}",
        duplicated.len(),
        duplicated.join("\n  ")
    );
}

/// **No backticked snake_case name the two reconstructed sections print
/// is one nothing in the tree holds.**
///
/// The generated regions close one class: a *set* of dataset labels
/// repeated beside the table it came from, which is the check above.
/// They do not close the other: a single label named in a sentence —
/// "the `occurrences_disagree` category" — which goes stale the moment
/// the label is renamed, with nothing to say so. Banning the mention
/// makes the section unreadable, so the mention stays and this makes a
/// stale one fail.
///
/// **What this actually checks, stated because an earlier revision
/// described it as derived and it was not.** A name passes if it is
///
/// * any string a committed dataset holds — every cell of every tracked
///   `.tsv`, every key and every value at every depth of every tracked
///   evidence `.json`, over files discovered by `git ls-files` rather
///   than listed here; **or**
/// * matched in a tracked Rust file by the letters `fn` and one space,
///   with a character before them that cannot continue an identifier, in
///   text that has had line comments, non-nested block comments,
///   ordinary and byte string literals, raw strings with no `#`, and
///   character literals cut out of it.
///
/// **The second arm is a text scan, not a parser, and this is where that
/// shows.** Every row below was run through [`function_names`] rather
/// than reasoned about:
///
/// | written in a tracked file | the scan yields |
/// |---|---|
/// | `fn` in a macro body that expands to no such function | the name |
/// | `fn` in an attribute's token tree | the name |
/// | a raw string with a `#` and an embedded `"`, or a raw byte string of the same shape | a name from inside the string |
/// | a nested block comment | the names after the **inner** `*/` |
/// | `fn` behind a disabled `#[cfg]`, or a trait method signature | the name |
/// | a tab, a newline or two spaces after `fn` | nothing |
/// | `fn r#match` | `r` |
///
/// The first five **over-admit**: a name the workspace does not really
/// define could be excused by one of them, so a stale name in the
/// document could survive. The last two **under-admit**: a real
/// definition written that way would be missing from the domain and the
/// document naming it would fail. No tracked file writes a definition in
/// either shape today: `git grep -nP 'fn[\t]|fn[ ]{2}|fn r[#]' -- '*.rs'`
/// matches once, on the `r#match` row of the table just above, and
/// `git grep -nP 'fn$' -- '*.rs'` matches twice, both inside doc
/// comments. All three are comments, and comments are cut out before the
/// scan. (The pattern uses character classes so that it does not match
/// itself.)
///
/// Closing this list means parsing Rust. This check is a membership test
/// and not a derivation for exactly that reason, and the list is here so
/// that the sentence describing it is not wider than the code under it.
#[test]
fn no_backticked_name_in_the_reconstructed_sections_is_one_the_tree_does_not_hold() {
    let md = read(QUERY_LOWERING);
    let swept = swept_text(&md);
    let mut held = every_value_a_dataset_holds();
    for f in tracked_rust_files() {
        for name in function_names(&read(&f)) {
            held.insert(name);
        }
    }

    let mut unknown: Vec<String> = Vec::new();
    for (line_no, line) in &swept {
        let mut rest = line.as_str();
        while let Some((_, tail)) = rest.split_once('`') {
            match tail.split_once('`') {
                Some((tok, after)) => {
                    if is_snake_case(tok) && !held.contains(tok) {
                        unknown.push(format!("{QUERY_LOWERING}:{line_no} names `{tok}`"));
                    }
                    rest = after;
                }
                None => break,
            }
        }
    }
    assert!(
        unknown.is_empty(),
        "{} backticked name(s) in §9.2b's reproducibility passage or §12.3 are held by no \
         committed dataset and defined by no function in the workspace:\n  {}",
        unknown.len(),
        unknown.join("\n  ")
    );
}

/// Source with line comments, block comments, string literals and
/// character literals removed, so a name written in prose beside the
/// code cannot be read as a definition.
///
/// **Nothing in this suite guards the character-literal arm.** Turning
/// it off changes the scanned domain by 340 distinct names, and all ten
/// active tests still pass, because no name the document backticks today
/// rests only on a definition that disappears. The arm is right and it
/// is unwatched: a `'"'` opens a string that swallows the code after it,
/// and real definitions leave the domain silently. The next person to
/// touch it should know there is no test to catch them.
///
/// **That 340 is a net figure, not a subtraction.** A throwaway probe
/// over `tracked_rust_files()` that built both domains and took the two
/// set differences printed `removed=347 added=7 net=340`. The seven
/// additions are `forge`, `from_unchecked_literal`,
/// `from_unchecked_month`, `from_unchecked_sql`, `test_aggregation_expr`,
/// `test_context_function_calls` and `test_function_call`: removing the
/// character-literal step changes where the string-literal step thinks
/// strings begin and end, so some text that was inside a string comes
/// back out and some that was outside goes in. Anyone re-measuring this
/// should take both differences; the single number hides one of them.
fn without_comments_and_strings(src: &str) -> String {
    let b: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0usize;
    while i < b.len() {
        let two: String = b[i..(i + 2).min(b.len())].iter().collect();
        if two == "//" {
            while i < b.len() && b[i] != '\n' {
                i += 1;
            }
        } else if two == "/*" {
            i += 2;
            while i + 1 < b.len() && !(b[i] == '*' && b[i + 1] == '/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
        } else if b[i] == '\''
            && (b.get(i + 2) == Some(&'\'')
                || (b.get(i + 1) == Some(&'\\') && b.get(i + 3) == Some(&'\'')))
        {
            // A character literal, skipped whole so that `'"'` does not
            // open a string that swallows the code after it. A lifetime
            // `'a` has no closing quote and falls through to be copied.
            i += if b.get(i + 1) == Some(&'\\') { 4 } else { 3 };
        } else if b[i] == '"' {
            i += 1;
            while i < b.len() && b[i] != '"' {
                if b[i] == '\\' {
                    i += 1;
                }
                i += 1;
            }
            i = (i + 1).min(b.len());
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    out
}

/// The names `fn <name>` introduces in comment-stripped source.
fn function_names(src: &str) -> Vec<String> {
    let stripped = without_comments_and_strings(src);
    let b = stripped.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 3 <= b.len() {
        if &b[i..i + 3] == b"fn " {
            let before_ok = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
            if before_ok {
                let mut j = i + 3;
                let mut name = String::new();
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                    name.push(char::from(b[j]));
                    j += 1;
                }
                if is_snake_case(&name) {
                    out.push(name);
                }
            }
        }
        i += 1;
    }
    out
}

fn is_snake_case(s: &str) -> bool {
    !s.is_empty()
        && s.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// The two reconstructed sections with the generated regions removed —
/// the same spans the generated-block checks compare, taken from the
/// same marker constants so the sweep and the checks cannot disagree
/// about where the boundary is.
fn swept_text(md: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = md.lines().collect();
    let at = |needle: &str, from: usize| {
        lines[from..]
            .iter()
            .position(|l| l.starts_with(needle))
            .map(|i| i + from)
            .unwrap_or_else(|| panic!("{QUERY_LOWERING} must carry a line starting {needle:?}"))
    };
    let r1a = at("**What is reproducible here, and what is not", 0);
    let r1b = at("**The mechanism is adaptive granularity.**", r1a);
    let r2a = at("### 12.3 The citations", 0);
    let mut out = Vec::new();
    for (a, b) in [(r1a, r1b), (r2a, lines.len())] {
        let mut inside = false;
        for (i, l) in lines.iter().enumerate().take(b).skip(a) {
            if l.starts_with(REBUILD_BLOCK_BEGIN_MARK) {
                inside = true;
                continue;
            }
            if l.starts_with(CENSUS_BLOCK_END) {
                inside = false;
                continue;
            }
            if !inside {
                out.push((i + 1, (*l).to_string()));
            }
        }
    }
    out
}

/// The words of a string: lower case, every character that is not a
/// letter or a digit treated as a separator.
///
/// This is what lets the duplication sweep see `ambiguous_basename`
/// written as `Ambiguous-Basename` or as the bare words "ambiguous
/// basename" — all three give the same two words.
fn words(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(|w| w.to_ascii_lowercase())
        .collect()
}

/// How far apart the words of a label may be and still count as one
/// mention of it. `occurrences_disagree` written "occurrences that
/// disagree" spans three words; the allowance is generous enough for a
/// clause or two of English between them and short enough that two
/// unrelated sentences cannot supply the words between them.
const LABEL_MENTION_SPAN: usize = 8;

/// Whether `haystack` names the label whose words are `label`.
///
/// The label's words must appear in order, each matching the start of a
/// word in the text so that a plural or an inflection still counts
/// ("blank target lines" names `blank_target_line`), and the whole
/// mention must fit inside [`LABEL_MENTION_SPAN`] words so that a
/// paraphrase is caught and two separate sentences are not stitched into
/// one.
fn names_label(haystack: &[String], label: &[String]) -> bool {
    if label.is_empty() {
        return false;
    }
    for start in 0..haystack.len() {
        if !haystack[start].starts_with(&label[0]) {
            continue;
        }
        let mut at = 1usize;
        let end = (start + LABEL_MENTION_SPAN).min(haystack.len());
        for w in &haystack[start + 1..end] {
            if at == label.len() {
                break;
            }
            if w.starts_with(&label[at]) {
                at += 1;
            }
        }
        if at == label.len() {
            return true;
        }
    }
    false
}

/// Both generated regions open with this and close with
/// [`CENSUS_BLOCK_END`]; the sweep keys on the shared prefix so a new
/// region does not need a third constant.
const REBUILD_BLOCK_BEGIN_MARK: &str = "<!-- generated";
