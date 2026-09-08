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

use std::collections::BTreeSet;

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
