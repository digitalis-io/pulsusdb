//! Issue #309: which matcher patterns the **warm label cache** is allowed
//! to evaluate in-process.
//!
//! Upstream Prometheus (the metrics API's reference of record, issue #283)
//! compiles every label-matcher regex with Go's `regexp` — an RE2 port —
//! inside `promql/parser`, so a pattern RE2 rejects is a **400 `bad_data`**
//! there. This engine compiles with the Rust `regex` crate, which accepts a
//! strictly different set: `\p{Alphabetic}` compiles in Rust and has no
//! table in RE2 (`re2/unicode_groups.cc` carries general categories and
//! script names only), while `a{bbb}c` is a literal in RE2 and a malformed
//! repetition in Rust.
//!
//! Issue #280 settled the asymmetry for every path that reaches storage:
//! **ClickHouse's RE2 is the authority**, its `Code: 427
//! CANNOT_COMPILE_REGEXP` is classified into `PromqlError::
//! InvalidRegexMatcher` (400) by [`super::dispatch`], and a pattern the Rust
//! crate cannot compile is deliberately handed to it rather than rejected.
//! A **warm, in-window** cache never asks storage, so that verdict never
//! arrives and an RE2-rejected pattern answered `200` with an empty result —
//! a silently wrong answer, which is worse than the loud `500` #280 fixed.
//!
//! **Why this is a screen and not a validator.** Deciding "RE2 rejects
//! this" in-process needs an RE2-syntax parser (a second engine, or an FFI
//! binding to RE2 itself). Over-rejecting is a worse outcome than the
//! current under-rejecting — it breaks queries the reference accepts — so
//! this module does not decide. It answers a strictly easier, conservative
//! question:
//!
//! > can the Rust `regex` crate's acceptance of this pattern be trusted to
//! > agree with RE2's?
//!
//! When the answer is "no", the pattern is not evaluated in-process; the
//! caller degrades to the storage path (`FallbackReason::RegexUnsupported`
//! — exactly the existing route for a pattern Rust cannot compile) and the
//! real authority returns the verdict. **A false positive costs a fallback,
//! never a rejection**: an RE2-valid pattern that trips the screen is still
//! answered, by ClickHouse, correctly. A false negative preserves today's
//! behaviour and nothing more.
//!
//! **What trips it** — the constructs where the Rust crate's accepted
//! grammar is known to exceed RE2's:
//!
//! | construct | Rust | RE2 / Go `regexp/syntax` |
//! |---|---|---|
//! | `\p{…}`, `\P{…}`, `\pN` | full UCD property vocabulary | fixed general-category + script table |
//! | `\u`, `\U` escapes | accepted | not an escape |
//! | `\<`, `\>` | word-boundary assertions | not an escape |
//! | `\b{…}` | named boundary assertions | not an escape |
//! | `(?…)` group heads other than `(?:` / pure `[imsU-]` flags | `x`, `u`, `R`, `P<…>`, `<…>` | `i`, `m`, `s`, `U` only |
//! | `{n,m}` with a bound above 1000 | bounded by a size budget | `kMaxRepeat = 1000` (re2 `parse.cc`, Go `maxRepeat`) |
//! | a repetition applied to a repetition (`a**`, `a{2}{3}`) | compiles as `(a*)*` | `bad repetition operator` |
//!
//! Deliberately **not** screened, because both engines accept them and the
//! divergence is one of meaning rather than acceptance: `\d`/`\w`/`\s` are
//! Unicode-aware in Rust and ASCII-only in RE2, and character-class set
//! operations (`[a&&b]`, `[a--b]`) are operators in Rust and literals in
//! RE2. Those are value divergences on a path both engines answer — a
//! separate defect class from this issue's status divergence, tracked as
//! follow-ups rather than folded in here (screening them would push
//! `\d`-shaped patterns, which are common, off the cache for no
//! status-correctness gain).

use super::matcher::{LabelMatcher, MatchOp};

/// RE2's repetition ceiling — `kMaxRepeat` in `re2/parse.cc`, `maxRepeat`
/// in Go's `regexp/syntax/parse.go`. The Rust crate has no equivalent
/// count limit (only a compiled-size budget), so `a{1001}` compiles there
/// and is rejected by both reference engines.
const RE2_MAX_REPEAT: u64 = 1000;

/// A narrow, `#[doc(hidden)]` test seam letting the corpus differential
/// binary (`tests/re2_screen_differential.rs`) reach the screen — the same
/// shape as issue #89's `MultiMetricScanProbe`, and for the same reason: an
/// external integration-test binary cannot see this crate's `pub(crate)`
/// items, and a cargo feature would either not compile under the plain
/// `cargo test --workspace` lane or ship the seam anyway.
#[doc(hidden)]
pub fn pattern_requires_re2_authority_for_test(pattern: &str) -> bool {
    pattern_requires_re2_authority(pattern)
}

/// What the byte just consumed was, for the "a repetition operator needs a
/// repeatable operand" rule. The Rust crate accepts a repetition applied to
/// a repetition (`a**` compiles as `(a*)*`); RE2 and Go reject it with
/// `bad repetition operator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Prev {
    /// Nothing repeatable yet — pattern start, or just after `(` or `|`.
    Nothing,
    /// A repeatable atom (literal, class, group, escape, assertion).
    Atom,
    /// A repetition operator (`*`, `+`, `?`, `{n,m}`). A following `?` is
    /// the legal non-greedy marker; a following `*`/`+`/`{n,m}` is not.
    Repeat,
    /// The non-greedy `?` of a repetition. Nothing may repeat it.
    LazyRepeat,
}

/// `true` when this pattern's acceptance cannot be decided in-process and
/// must be left to the storage engine's RE2. Conservative: see the module
/// header — `true` costs a storage round-trip, never a rejection.
///
/// A single left-to-right byte scan; no allocation, no compilation. The
/// scan tracks just enough structure to avoid false positives that would
/// push ordinary patterns off the cache: escaped bytes are skipped (so
/// `\\p{L}` is a literal backslash, not a Unicode class) and a character
/// class is consumed whole (so `[*+]` is a class of two literals, not a
/// doubled repetition operator).
pub(crate) fn pattern_requires_re2_authority(pattern: &str) -> bool {
    let b = pattern.as_bytes();
    let mut i = 0;
    let mut prev = Prev::Nothing;
    while i < b.len() {
        match b[i] {
            b'\\' => {
                if escape_requires_re2_authority(b, i) {
                    return true;
                }
                // Skip the escaped byte: `\\p{L}` is a literal backslash
                // followed by literal `p{L}` in both engines, and `\(` can
                // never open a group.
                i += 2;
                prev = Prev::Atom;
            }
            b'[' => {
                // An unterminated class, or one carrying an escape only RE2
                // can adjudicate (`[\p{Alphabetic}]`), defers.
                let Some(end) = class_end(b, i) else {
                    return true;
                };
                i = end + 1;
                prev = Prev::Atom;
            }
            b'(' => {
                if b.get(i + 1) == Some(&b'?') {
                    // Consume the whole `(?…:` / `(?…)` head, so its flag
                    // bytes are never mistaken for atoms or operators.
                    let Some(head) = re2_portable_group_head_len(&b[i + 2..]) else {
                        return true;
                    };
                    i += 2 + head;
                } else {
                    i += 1;
                }
                prev = Prev::Nothing;
            }
            b'|' => {
                i += 1;
                prev = Prev::Nothing;
            }
            b'*' | b'+' => {
                if matches!(prev, Prev::Repeat | Prev::LazyRepeat) {
                    return true;
                }
                i += 1;
                prev = Prev::Repeat;
            }
            b'?' => {
                match prev {
                    // The non-greedy marker — legal in both engines.
                    Prev::Repeat => prev = Prev::LazyRepeat,
                    Prev::LazyRepeat => return true,
                    _ => prev = Prev::Repeat,
                }
                i += 1;
            }
            b'{' => match parse_repetition(b, i + 1) {
                Some(Repetition { end, over_max }) => {
                    if over_max || matches!(prev, Prev::Repeat | Prev::LazyRepeat) {
                        return true;
                    }
                    i = end + 1;
                    prev = Prev::Repeat;
                }
                // Not a well-formed repetition: a literal brace in RE2 (the
                // `a{bbb}c` asymmetry the storage fallback already serves).
                None => {
                    i += 1;
                    prev = Prev::Atom;
                }
            },
            _ => {
                i += 1;
                prev = Prev::Atom;
            }
        }
    }
    false
}

/// The first `Re`/`Nre` matcher whose pattern trips
/// [`pattern_requires_re2_authority`]. `Eq`/`Neq` values are literals, never
/// compiled, so they are never screened.
pub(crate) fn first_matcher_requiring_re2_authority(
    matchers: &[LabelMatcher],
) -> Option<&LabelMatcher> {
    matchers.iter().find(|m| {
        matches!(m.op, MatchOp::Re | MatchOp::Nre) && pattern_requires_re2_authority(&m.value)
    })
}

/// `bytes` begins immediately after a `(?`. `Some(len)` — the byte length
/// of the head including its terminator — only for the heads both engines
/// read identically: `(?:` and a flag run drawn from RE2's whole flag
/// vocabulary (`i`, `m`, `s`, `U`, and `-`) terminated by `)` or `:`.
/// Everything else — `(?x`, `(?u`, `(?R`, `(?P<…>`, `(?<…>`, `(?#…`, an
/// unterminated head — is `None`, and left to the authority.
fn re2_portable_group_head_len(bytes: &[u8]) -> Option<usize> {
    for (i, &c) in bytes.iter().enumerate() {
        match c {
            b'i' | b'm' | b's' | b'U' | b'-' => {}
            b':' | b')' => return Some(i + 1),
            _ => return None,
        }
    }
    None
}

/// `b[i]` is a backslash: `true` when the escape it introduces is one only
/// the storage engine's RE2 can adjudicate. A trailing backslash is
/// malformed in both engines and can only reach here if the Rust crate
/// accepted it, so it defers too.
fn escape_requires_re2_authority(b: &[u8], i: usize) -> bool {
    match b.get(i + 1) {
        None => true,
        Some(b'p' | b'P' | b'u' | b'U' | b'<' | b'>') => true,
        Some(b'b' | b'B') => b.get(i + 2) == Some(&b'{'),
        Some(_) => false,
    }
}

/// The index of the `]` closing the class opened at `open`, honouring an
/// initial `^` and backslash escapes. `None` — defer — for an unterminated
/// class, or one containing an escape only RE2 can adjudicate: class
/// contents are scanned, not skipped, so `[\p{Alphabetic}]` is caught
/// exactly like the bare `\p{Alphabetic}`.
///
/// A nested `[` is NOT treated as opening a sub-class: RE2 reads it as a
/// literal, and the class-set-operation reading the Rust crate gives it is
/// the separate value divergence named in the module header.
fn class_end(b: &[u8], open: usize) -> Option<usize> {
    let mut i = open + 1;
    if b.get(i) == Some(&b'^') {
        i += 1;
    }
    while i < b.len() {
        match b[i] {
            b'\\' => {
                if escape_requires_re2_authority(b, i) {
                    return None;
                }
                i += 2;
            }
            b']' => return Some(i),
            _ => i += 1,
        }
    }
    None
}

/// One well-formed repetition: where its `}` sits, and whether either bound
/// exceeds [`RE2_MAX_REPEAT`].
#[derive(Debug)]
struct Repetition {
    end: usize,
    over_max: bool,
}

/// Parses the repetition body starting at `start` (immediately after a
/// `{`). `None` when the brace does not open a well-formed `{n}`/`{n,}`/
/// `{n,m}` — a literal brace in RE2, and the `a{bbb}c` asymmetry the
/// storage fallback already exists to serve.
fn parse_repetition(b: &[u8], start: usize) -> Option<Repetition> {
    let mut i = start;
    let mut over_max = false;
    let mut digits = 0usize;
    let mut value: u64 = 0;
    let mut seen_comma = false;
    loop {
        match b.get(i) {
            Some(&c) if c.is_ascii_digit() => {
                digits += 1;
                value = value.saturating_mul(10).saturating_add(u64::from(c - b'0'));
                i += 1;
            }
            Some(b',') if !seen_comma && digits > 0 => {
                over_max |= value > RE2_MAX_REPEAT;
                seen_comma = true;
                digits = 0;
                value = 0;
                i += 1;
            }
            Some(b'}') if digits > 0 => {
                return Some(Repetition {
                    end: i,
                    over_max: over_max || value > RE2_MAX_REPEAT,
                });
            }
            // `{n,}` — no upper bound, so only `n` was checked.
            Some(b'}') if seen_comma => return Some(Repetition { end: i, over_max }),
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The measured #309 case plus the rest of the Rust-accepts/RE2-rejects
    /// vocabulary. Each premise is pinned: the Rust crate must ACCEPT the
    /// anchored form, otherwise the vendored parser rejects at plan time and
    /// the screen would be guarding nothing.
    #[test]
    fn patterns_the_rust_crate_accepts_beyond_re2_are_left_to_the_authority() {
        for pattern in [
            r"\p{Alphabetic}",
            r"\p{Greek}",
            r"\pL",
            r"\P{L}",
            r"[\p{L}0-9]",
            r"\u{263A}",
            r"\U0001F600",
            r"(?x) a b ",
            r"(?i-u:foo)",
            r"(?P<name>a)",
            r"(?<name>a)",
            r"a{1001}",
            r"a{2,1001}",
        ] {
            assert!(
                regex::Regex::new(&format!("^(?:{pattern})$")).is_ok(),
                "premise: the Rust `regex` crate must ACCEPT {pattern:?}"
            );
            assert!(
                pattern_requires_re2_authority(pattern),
                "{pattern:?} must be left to the storage engine's RE2"
            );
        }
    }

    /// Word-boundary escapes the Rust crate grew and RE2 never had. Pinned
    /// separately because `\<`/`\>`/`\b{…}` compile only on recent `regex`
    /// versions; if a future crate version rejects them the screen is
    /// harmlessly redundant, not wrong.
    #[test]
    fn rust_only_boundary_escapes_are_left_to_the_authority() {
        for pattern in [r"\<word\>", r"\b{start}x", r"\B{end}x"] {
            assert!(
                pattern_requires_re2_authority(pattern),
                "{pattern:?} must be left to the storage engine's RE2"
            );
        }
    }

    /// The other half of the invariant, and the one that matters most:
    /// over-rejecting breaks valid queries. Every one of these is read
    /// identically by both engines and must keep being answered from the
    /// warm cache.
    #[test]
    fn portable_patterns_stay_on_the_in_process_path() {
        for pattern in [
            "",
            ".*",
            ".+",
            "foo",
            "foo|bar",
            "(a|b)c",
            "(?:a|b)c",
            "(?i)foo",
            "(?i-s:foo)",
            "(?imsU)foo",
            "[0-9]+",
            "[^a-z]",
            "[[:alpha:]]+",
            r"\d+",
            r"\w+\s*",
            r"prod-.+\.example\.com",
            r"a\{bbb\}c",
            r"\\p{L}",
            "a{2,3}",
            "a{1000}",
            "a{2,1000}",
            "a{,5}",
            "a{bbb}c",
            r"\x{263A}",
            r"\A\bfoo\b\z",
            "10.0.0.1",
        ] {
            assert!(
                !pattern_requires_re2_authority(pattern),
                "{pattern:?} is read identically by both engines and must stay in-process"
            );
        }
    }

    /// `\\p{L}` is a literal backslash followed by a literal `p{L}` — the
    /// screen must skip an escaped byte rather than pattern-matching raw
    /// substrings, or every doubled backslash would be a false positive.
    #[test]
    fn an_escaped_backslash_does_not_hide_or_fabricate_a_unicode_class() {
        assert!(!pattern_requires_re2_authority(r"\\p{L}"));
        assert!(pattern_requires_re2_authority(r"\\\p{L}"));
        assert!(pattern_requires_re2_authority(r"\\\\\p{L}"));
    }

    /// A trailing backslash cannot be classified, so it defers.
    #[test]
    fn a_trailing_backslash_defers() {
        assert!(pattern_requires_re2_authority(r"foo\"));
    }

    /// Found by the ClickHouse-24.8 differential, not by inspection: the
    /// Rust crate compiles a repetition of a repetition (`a**` as `(a*)*`)
    /// and RE2 answers `bad repetition operator`. The non-greedy `?` is
    /// the one legal follower and must NOT trip the screen.
    #[test]
    fn a_repetition_of_a_repetition_defers_but_a_non_greedy_marker_does_not() {
        for pattern in [
            "a**",
            "a*+",
            "a?+",
            "a++",
            "a{2}{3}",
            "(a){2}{3}",
            "a?*",
            "a*??",
        ] {
            assert!(
                pattern_requires_re2_authority(pattern),
                "{pattern:?} is `bad repetition operator` in RE2"
            );
        }
        for pattern in [
            "a*?",
            "a+?",
            "a??",
            "a{2}?",
            "a{2,3}?",
            "(a*)*",
            "(?:a{2}){3}",
        ] {
            assert!(
                !pattern_requires_re2_authority(pattern),
                "{pattern:?} is legal in both engines"
            );
        }
    }

    /// A character class is scanned, not skipped: an escape inside it is
    /// classified exactly like a bare one (this was a real escape in the
    /// first differential round), while `*`/`+`/`{` inside it are literals
    /// and must not read as operators.
    #[test]
    fn class_contents_are_scanned_but_their_operators_are_literals() {
        assert!(pattern_requires_re2_authority(r"[\p{Alphabetic}]"));
        assert!(pattern_requires_re2_authority(r"[a\p{L}]"));
        assert!(pattern_requires_re2_authority("[abc"));
        for pattern in ["[*+]", "[+*/-]", "[{}]", "[?*]", r"[\]]", "[a-]", "[^]a]"] {
            assert!(
                !pattern_requires_re2_authority(pattern),
                "{pattern:?} is a class of literals in both engines"
            );
        }
    }

    #[test]
    fn only_regex_operators_are_screened() {
        let re = |value: &str| LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Re,
            value: value.to_string(),
        };
        let eq = |value: &str| LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Eq,
            value: value.to_string(),
        };
        let nre = |value: &str| LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Nre,
            value: value.to_string(),
        };

        // An `=` matcher's value is a literal, never compiled.
        assert!(first_matcher_requiring_re2_authority(&[eq(r"\p{Alphabetic}")]).is_none());
        assert!(first_matcher_requiring_re2_authority(&[re("api|web"), eq("x")]).is_none());
        assert!(first_matcher_requiring_re2_authority(&[re(r"\p{Alphabetic}")]).is_some());
        assert!(first_matcher_requiring_re2_authority(&[nre(r"\p{Alphabetic}")]).is_some());

        // The FIRST offender is reported, so the fallback reason names a
        // matcher that is genuinely undecidable.
        let ordered = [
            re("api"),
            LabelMatcher {
                key: "instance".to_string(),
                op: MatchOp::Re,
                value: r"\p{Greek}".to_string(),
            },
            re(r"\p{L}"),
        ];
        let found = first_matcher_requiring_re2_authority(&ordered).expect("an offender");
        assert_eq!(found.key, "instance");
    }
}
