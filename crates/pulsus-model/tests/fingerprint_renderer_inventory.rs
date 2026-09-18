//! Issue #498 criterion 4b, the enumerated leg: **the set of places a
//! fingerprint can reach SQL is closed, and it is closed by the TYPE.**
//!
//! The output-checked leg lives beside the builders
//! (`crates/pulsus-read/tests/fingerprint_rendering.rs`) and reads what
//! each of the six sites emits. This file asks the other question: is the
//! set six, or is there a seventh nobody read?
//!
//! **Counting fingerprint-typed signatures cannot answer it.** The five
//! SQL modules hold thirty-two functions carrying a fingerprint list and
//! only six of them render; a count can never separate a renderer from a
//! function that forwards a slice. The predicate that does separate them
//! is about the type rather than the count:
//!
//! > In `logql/sql.rs`, `logql/predicate.rs`, `metrics/sample_sql.rs`,
//! > `metrics/sql.rs` and `metrics/grouped_sql.rs`, no function
//! > parameter's type mentions `Fingerprint`.
//!
//! A raw identity therefore cannot enter those modules at all, so a
//! statement they build carries whatever `FpLiteral`'s one `Display`
//! emits. [`EXEMPTIONS`] is the escape hatch and is **empty**: a parameter
//! that must stay a `Fingerprint` goes on it with the reason it does not
//! render, and a non-empty list is a finding rather than a formality.
//!
//! The second check is the inventory itself: the functions in those five
//! modules that turn a literal into text are exactly the six the issue
//! froze. That is a text scan over the conversion spellings, listed in
//! [`CONVERSIONS`], so a seventh site written any of those ways fails
//! here even if its output happens to be right.
//!
//! This file lives in `pulsus-model` rather than beside the modules it
//! reads because the property is about the identity type's surface, and
//! the third check — the whole public surface of `impl Fingerprint` — is
//! about a type defined here.

use std::collections::BTreeSet;

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

/// The five modules that build fingerprint SQL.
const SQL_MODULES: &[&str] = &[
    "crates/pulsus-read/src/logql/sql.rs",
    "crates/pulsus-read/src/logql/predicate.rs",
    "crates/pulsus-read/src/metrics/sample_sql.rs",
    "crates/pulsus-read/src/metrics/sql.rs",
    "crates/pulsus-read/src/metrics/grouped_sql.rs",
];

/// Parameters that may keep a `Fingerprint` type, each with the reason it
/// does not render one. **Expected empty.**
const EXEMPTIONS: &[(&str, &str, &str)] = &[];

/// The six rendering sites, by enclosing function and module.
const RENDERERS: &[(&str, &str)] = &[
    ("crates/pulsus-read/src/logql/sql.rs", "fp_list"),
    ("crates/pulsus-read/src/logql/sql.rs", "stage3_keyset"),
    (
        "crates/pulsus-read/src/logql/predicate.rs",
        "fingerprint_test",
    ),
    (
        "crates/pulsus-read/src/metrics/sample_sql.rs",
        "render_fingerprint_list",
    ),
    (
        "crates/pulsus-read/src/metrics/sql.rs",
        "series_labels_by_fingerprint",
    ),
    (
        "crates/pulsus-read/src/metrics/sql.rs",
        "discovery_fetch_multi",
    ),
];

/// Every way the shipped code turns an `FpLiteral` into text. A site
/// spelled any other way is invisible here, which is why the
/// output-checked leg exists beside this one.
const CONVERSIONS: &[&str] = &[
    "FpLiteral::to_string",
    "fp.to_string()",
    "{fp}",
    ".sql_literal().to_string()",
];

/// Cuts line comments out so a `Fingerprint` in prose is not a parameter.
fn without_line_comments(src: &str) -> String {
    src.lines()
        .map(|l| match l.find("//") {
            Some(at) => &l[..at],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// `(function name, parameter list, body)` for every `fn` in `src`.
///
/// A text walk, not a parser: it takes each `fn <name>(`, balances
/// parentheses to the end of the parameter list, then balances braces to
/// the end of the body. That is enough for the two questions asked here —
/// what a signature's parameters mention, and whether a body carries one
/// of [`CONVERSIONS`] — and it is stated as a text walk so nobody reads
/// more into it.
fn functions(src: &str) -> Vec<(String, String, String)> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while let Some(at) = src[i..].find("fn ") {
        let at = i + at;
        i = at + 3;
        // `fn` must start a token.
        if at > 0 {
            let prev = b[at - 1];
            if prev.is_ascii_alphanumeric() || prev == b'_' {
                continue;
            }
        }
        let rest = &src[at + 3..];
        let name_len = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        let name = rest[..name_len].to_string();
        let Some(open) = src[at + 3 + name_len..].find('(') else {
            continue;
        };
        let open = at + 3 + name_len + open;
        let mut depth = 0i32;
        let mut close = open;
        for (k, c) in src[open..].char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        close = open + k;
                        break;
                    }
                }
                _ => {}
            }
        }
        let params = src[open + 1..close].to_string();
        // The body: from the next `{` after the parameter list, or nothing
        // when the item ends in `;` (a trait signature).
        let tail = &src[close..];
        let body = match (tail.find('{'), tail.find(';')) {
            (Some(bo), semi) if semi.is_none_or(|s| bo < s) => {
                let bo = close + bo;
                let mut d = 0i32;
                let mut end = bo;
                for (k, c) in src[bo..].char_indices() {
                    match c {
                        '{' => d += 1,
                        '}' => {
                            d -= 1;
                            if d == 0 {
                                end = bo + k;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                src[bo..=end.min(src.len() - 1)].to_string()
            }
            _ => String::new(),
        };
        out.push((name, params, body));
    }
    out
}

/// **No function parameter in the five SQL modules mentions
/// `Fingerprint`.** The mint happens upstream of them, so a raw identity
/// cannot reach a statement builder.
#[test]
fn no_sql_module_takes_a_raw_fingerprint() {
    let mut offenders: Vec<String> = Vec::new();
    for module in SQL_MODULES {
        let src = without_line_comments(&read(module));
        for (name, params, _) in functions(&src) {
            if !params.contains("Fingerprint") {
                continue;
            }
            if EXEMPTIONS.iter().any(|(m, f, _)| m == module && *f == name) {
                continue;
            }
            offenders.push(format!(
                "{module}: fn {name}({})",
                params.split_whitespace().collect::<Vec<_>>().join(" ")
            ));
        }
    }
    assert!(
        offenders.is_empty(),
        "a statement builder takes a raw `Fingerprint`, so what it renders is the caller's \
         choice rather than the type's (issue #498 criterion 4b). Mint at the call site, or \
         add the parameter to EXEMPTIONS with the reason it does not render:\n  {}",
        offenders.join("\n  ")
    );
}

/// The escape hatch is empty, said as an assertion so that filling it is a
/// decision somebody made rather than a line nobody read.
#[test]
fn the_exemption_list_is_empty() {
    assert!(
        EXEMPTIONS.is_empty(),
        "EXEMPTIONS is non-empty: {EXEMPTIONS:?}. Each entry is a parameter that keeps a raw \
         identity; read what it does with the value before accepting it"
    );
}

/// **The set of functions that turn a fingerprint into text is exactly the
/// six.** A seventh site — written any of the ways [`CONVERSIONS`] lists —
/// fails here, which is the break issue #498 specifies for this leg.
#[test]
fn the_rendering_sites_are_the_six_the_inventory_names() {
    let mut found: BTreeSet<(String, String)> = BTreeSet::new();
    for module in SQL_MODULES {
        let src = without_line_comments(&read(module));
        for (name, _, body) in functions(&src) {
            if CONVERSIONS.iter().any(|c| body.contains(c)) {
                found.insert((module.to_string(), name));
            }
        }
    }
    let expected: BTreeSet<(String, String)> = RENDERERS
        .iter()
        .map(|(m, f)| (m.to_string(), f.to_string()))
        .collect();
    assert_eq!(
        found, expected,
        "the set of functions in the five SQL modules that turn a fingerprint into text is not \
         the inventory issue #498 froze. A new one needs its own case in \
         crates/pulsus-read/tests/fingerprint_rendering.rs before it is added here"
    );
}

/// Every named renderer is still in the module the inventory names — the
/// other direction of the same claim, so a renaming cannot empty the set
/// quietly.
#[test]
fn every_named_renderer_still_exists() {
    for (module, name) in RENDERERS {
        let src = without_line_comments(&read(module));
        assert!(
            functions(&src).iter().any(|(n, _, _)| n == name),
            "{module} no longer defines `fn {name}`, which the issue #498 inventory names"
        );
    }
}

/// **`impl Fingerprint`'s whole public surface is `from_raw` and
/// `sql_literal`.**
///
/// Issue #498's plan is explicit that this is a source invariant and not
/// a compiler proof: a doctest fence naming one method cannot say anything
/// about every method, and the review demonstrated it — a fence on `get`
/// stays red while a differently named `expose` compiles. So the claim is
/// asserted over the declaration itself.
#[test]
fn the_fingerprint_type_exposes_only_its_mint_and_its_constructor() {
    let src = read("crates/pulsus-model/src/time.rs");
    let at = src
        .find("impl Fingerprint {")
        .expect("time.rs defines `impl Fingerprint`");
    let block = &src[at..];
    let end = block.find("\n}\n").expect("the impl block ends");
    let names: BTreeSet<String> = without_line_comments(&block[..end])
        .match_indices("pub const fn ")
        .chain(without_line_comments(&block[..end]).match_indices("pub fn "))
        .map(|(i, kw)| {
            let rest = &without_line_comments(&block[..end])[i + kw.len()..];
            let n = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            rest[..n].to_string()
        })
        .collect();
    assert_eq!(
        names,
        ["from_raw", "sql_literal"]
            .iter()
            .map(|s| s.to_string())
            .collect::<BTreeSet<String>>(),
        "`impl Fingerprint`'s public surface moved. A method that hands the inner 128-bit value \
         back re-opens the hole the sealed type closes: every caller that wants text can then \
         render it itself, and the six checked sites stop being the whole set"
    );
}
