//! RE2-compatibility machinery shared by every surface that must agree
//! with an RE2-family engine about regular expressions (issue #328 D1;
//! built by #309/#317 inside `pulsus-read`/`pulsus-promql`, extracted
//! here because a third surface — the TraceQL semantic validator —
//! needed it and the direct dependency edge does not exist to be drawn:
//! `pulsus-read` already depends on `pulsus-traceql`, so
//! `pulsus-traceql → pulsus-read` would be a cargo cycle).
//!
//! Three pieces, each moved verbatim from its #309/#317 home and
//! re-exported there so no call site churned:
//!
//! * [`re2_pattern_to_rust`] (#317) — rewrites a pattern into RE2's
//!   *reading* of it before any in-process compile, so the two engines
//!   agree on **meaning** where they both accept.
//! * [`pattern_requires_re2_authority`] (#309) — the conservative
//!   **acceptance** screen: `true` when the Rust `regex` crate's
//!   acceptance of the pattern cannot be trusted to agree with RE2's and
//!   the verdict must be left to a real RE2 (the storage engine).
//!   Preserved bit-for-bit across the extraction: `pulsus-read`'s
//!   `tests/fixtures/re2_screen/screen_verdicts.txt` baseline (committed
//!   before the extraction) replays the whole 4,000+-pattern corpus over
//!   it hermetically, and the #309 live differential re-proves it
//!   against ClickHouse's RE2.
//! * [`re2_verdict`] (#328, new) — the three-valued acceptance verdict
//!   the TraceQL validator consumes, built on the other two.
//!
//! # Why three values
//!
//! Deciding "RE2 rejects this" in-process needs an RE2-syntax parser —
//! a second engine — which this crate deliberately is not (the root fix
//! is issue #336). What CAN be decided is decided; what cannot is
//! `Unknown`, and a consumer must treat `Unknown` as accept — an
//! over-rejection breaks queries the reference serves, which is the
//! harmful direction.
//!
//! The `Unknown` set is ENUMERABLE FROM THIS FILE, not from probing:
//! [`re2_verdict`] answers `Unknown` at exactly two arms — a scan
//! `Undecidable` and the untrusted compile failures — and the scan
//! returns `Undecidable` at exactly SIX sites, each a closed class
//! family: the bare-escape check (`\p`/`\P` Unicode properties,
//! `\u`/`\U`, the `\<`/`\>`/`\b{…}`/`\B{…}` boundary escapes, a
//! trailing backslash), the same escapes inside a character class **that
//! closes** (issue #336: an unterminated one is a decidable joint
//! rejection whatever it contains, so `[\p{L}` no longer reaches this
//! site — the site itself remains, for `[\p{Alphabetic}]`. Note which
//! quantity that leaves alone: the COUNT of sites and classes is
//! unchanged, while the SET of patterns reaching them shrank by the
//! unterminated-class-carrying-an-escape family), the
//! non-`(?:`/flag group heads (lookarounds, named groups, `(?x`/`(?u`/
//! `(?#`/…), a `*`/`+` applied to a repetition, a `?` applied to an
//! already-lazy repetition, and a `{n,m}` above `kMaxRepeat` or
//! following a repetition; the compile arm adds `\Q…\E`, octal
//! escapes and `CompiledTooBig`. The
//! `every_unknown_return_site_has_a_named_class_representative` test
//! pins a representative that REACHES each site (verified by
//! neutralising the sites one at a time). Which members the REFERENCE
//! then accepts or rejects is measured per class and ledgered
//! (`traceql-validate-re2-unknown-residual`,
//! docs/benchmarks/traces-differential-ledger.md), owned by #336.
//!
//! Stated at its true strength: a BRAND-NEW `Undecidable` site added
//! to the scan is caught by the census only if a row is added with it,
//! and by the wider suite only if it moves a verdict in the frozen
//! `screen_verdicts.txt` corpus — otherwise review of THIS CRATE is
//! the only guard. The mechanism that would close that is #336's
//! RE2-syntax parser, deliberately not built here.

use std::borrow::Cow;

mod re2_syntax;

pub use re2_syntax::re2_pattern_to_rust;
// Issue #331: the ClickHouse `match()` flag-group-head strategy — the
// fourth RE2-compatibility surface, landed alongside the #328
// extraction and living here with the walker helpers it shares.
pub use re2_syntax::{
    ClickhouseMatchStrategy, clickhouse_match_head_rewrite, clickhouse_match_strategy,
};

/// RE2's repetition ceiling — `kMaxRepeat` in `re2/parse.cc`, `maxRepeat`
/// in Go's `regexp/syntax/parse.go`, which is where this 1000 comes from
/// and the only reason it is not a round number of our choosing. The
/// consequence for a user is docs/api.md §9.3's repetition-cap row.
const RE2_MAX_REPEAT: u64 = 1000;

/// The three-valued RE2 acceptance verdict (issue #328 D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Re2Verdict {
    /// Both engines accept the pattern.
    Accepts,
    /// RE2 rejects the pattern, decidably: either the scan proved a
    /// joint rejection, or the Rust crate rejected it inside the region
    /// where its rejections are trusted to be RE2's too.
    Rejects,
    /// Undecidable in-process, in EITHER direction — the Rust crate
    /// accepts beyond RE2 (the [`pattern_requires_re2_authority`]
    /// classes) or rejects within it. A consumer must treat this as
    /// accept, which is why docs/api.md §9.3 can list rows reachable on
    /// no route but trace validation. Membership of either direction is
    /// §9.3 and §9.4; do not re-enumerate it here.
    Unknown,
}

/// The three-valued acceptance verdict for `pattern`, per the #328 v2
/// ruling: **bare** compile (never anchored — `^(?:x)(y)$` would balance
/// the user's stray parenthesis for them), with validity judged through
/// the #317 rewrite so the in-process engine reads the pattern the way
/// RE2 does.
///
/// Decision order (plan v3 D1′):
/// 1. scan `Undecidable` → `Unknown`;
/// 2. scan joint-reject (an unterminated class — measured: `[`, `[a`,
///    and since #336 `[\p{L}`/`[a\` too, are rejected by the Rust crate
///    AND by RE2) → `Rejects`;
/// 3. otherwise compile `re2_pattern_to_rust(pattern)`, bare:
///    `Ok` → `Accepts`; `CompiledTooBig` → `Unknown` (RE2's budget is
///    not ours to guess); any other error → `Unknown` if the pattern
///    contains a construct the Rust crate rejects and RE2 accepts
///    (`\Q`/`\E`, octal `\0`–`\7` — checked by conservative substring
///    containment, so a false hit costs an `Unknown`, never a
///    rejection), else `Rejects`.
pub fn re2_verdict(pattern: &str) -> Re2Verdict {
    match scan(pattern) {
        Scan::Undecidable => Re2Verdict::Unknown,
        Scan::JointReject => Re2Verdict::Rejects,
        Scan::Portable => {
            let rewritten: Cow<'_, str> = re2_pattern_to_rust(pattern);
            match regex::Regex::new(&rewritten) {
                Ok(_) => Re2Verdict::Accepts,
                Err(regex::Error::CompiledTooBig(_)) => Re2Verdict::Unknown,
                Err(_) if rust_rejects_beyond_its_remit(pattern) => Re2Verdict::Unknown,
                Err(_) => Re2Verdict::Rejects,
            }
        }
    }
}

/// `true` when the pattern carries a construct from docs/api.md §9.4's
/// first two rows — the ones the Rust crate rejects and RE2 accepts — so
/// a compile failure here proves nothing about RE2's verdict.
///
/// Substring containment on purpose: an escaped `\\Q` also matches, which
/// only widens `Unknown` (the safe direction; a narrower escape-aware
/// scan could misclassify and over-reject).
fn rust_rejects_beyond_its_remit(pattern: &str) -> bool {
    let b = pattern.as_bytes();
    b.windows(2)
        .any(|w| w[0] == b'\\' && (w[1] == b'Q' || w[1] == b'E' || (b'0'..=b'7').contains(&w[1])))
}

/// A narrow test seam kept for `pulsus-read`'s corpus differential
/// binary, which historically reached the screen through
/// `pulsus_read::metrics::pattern_requires_re2_authority_for_test`. That
/// re-export now delegates here.
#[doc(hidden)]
pub fn pattern_requires_re2_authority_for_test(pattern: &str) -> bool {
    pattern_requires_re2_authority(pattern)
}

/// What the byte just consumed was, for the "a repetition operator needs
/// a repeatable operand" rule. This state exists only because the two
/// engines disagree about a repetition applied to a repetition —
/// docs/api.md §9.3's repetition-of-repetition row — so the scan has to
/// track enough structure to spot one.
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

/// The scan's three-valued outcome (issue #328's class split). The old
/// boolean screen is exactly `!matches!(scan(p), Scan::Portable)` — the
/// split refines the non-portable side only, so the boolean is preserved
/// bit-for-bit (gated by `pulsus-read`'s committed
/// `screen_verdicts.txt` whole-corpus baseline).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scan {
    /// The Rust crate's verdict on this pattern can be trusted to be
    /// RE2's.
    Portable,
    /// Both engines decidably reject (today: an unterminated character
    /// class — measured against both engines).
    JointReject,
    /// Only a real RE2 can adjudicate.
    Undecidable,
}

/// `true` when this pattern's acceptance cannot be decided in-process and
/// must be left to the storage engine's RE2. Conservative: `true` costs a
/// storage round-trip, never a rejection (#309's contract, unchanged by
/// the extraction).
pub fn pattern_requires_re2_authority(pattern: &str) -> bool {
    !matches!(scan(pattern), Scan::Portable)
}

/// A single left-to-right byte scan; no allocation, no compilation. The
/// scan tracks just enough structure to avoid false positives that would
/// push ordinary patterns off the cache: escaped bytes are skipped (so
/// `\\p{L}` is a literal backslash, not a Unicode class) and a character
/// class is consumed whole (so `[*+]` is a class of two literals, not a
/// doubled repetition operator).
fn scan(pattern: &str) -> Scan {
    let b = pattern.as_bytes();
    let mut i = 0;
    let mut prev = Prev::Nothing;
    while i < b.len() {
        match b[i] {
            b'\\' => {
                if escape_requires_re2_authority(b, i) {
                    return Scan::Undecidable;
                }
                // Skip the escaped byte: `\\p{L}` is a literal backslash
                // followed by literal `p{L}` in both engines, and `\(` can
                // never open a group.
                i += 2;
                prev = Prev::Atom;
            }
            b'[' => {
                // An unterminated class is a JOINT rejection (issue #328's
                // class split: the Rust crate and RE2 both reject `[` and
                // `[a`); one carrying an escape only RE2 can adjudicate
                // (`[\p{Alphabetic}]`) defers.
                match class_end(b, i) {
                    ClassEnd::Found(end) => {
                        i = end + 1;
                        prev = Prev::Atom;
                    }
                    ClassEnd::Unterminated => return Scan::JointReject,
                    ClassEnd::Undecidable => return Scan::Undecidable,
                }
            }
            b'(' => {
                if b.get(i + 1) == Some(&b'?') {
                    // Consume the whole `(?…:` / `(?…)` head, so its flag
                    // bytes are never mistaken for atoms or operators.
                    let Some(head) = re2_portable_group_head_len(&b[i + 2..]) else {
                        return Scan::Undecidable;
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
                    return Scan::Undecidable;
                }
                i += 1;
                prev = Prev::Repeat;
            }
            b'?' => {
                match prev {
                    // The non-greedy marker — legal in both engines.
                    Prev::Repeat => prev = Prev::LazyRepeat,
                    Prev::LazyRepeat => return Scan::Undecidable,
                    _ => prev = Prev::Repeat,
                }
                i += 1;
            }
            b'{' => match parse_repetition(b, i + 1) {
                Some(Repetition { end, over_max }) => {
                    if over_max || matches!(prev, Prev::Repeat | Prev::LazyRepeat) {
                        return Scan::Undecidable;
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
    Scan::Portable
}

/// The one-line reason out of a `regex::Error`, whose `Display` is a
/// multi-line diagram ending in `error: <reason>`. Falls back to the whole
/// rendering rather than dropping information if that shape ever changes.
pub fn rust_regex_reason(err: &regex::Error) -> String {
    let rendered = err.to_string();
    rendered
        .lines()
        .rev()
        .find_map(|l| l.trim().strip_prefix("error: "))
        .map_or_else(
            || rendered.split_whitespace().collect::<Vec<_>>().join(" "),
            str::to_string,
        )
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
/// the storage engine's RE2 can adjudicate.
///
/// The part no docs table covers, because it is about the ORDER this code
/// runs in: the scan happens BEFORE any compilation, so a trailing
/// backslash (`b.get(i + 1) == None`) reaches this arm whatever either
/// engine would go on to say about the pattern. It defers rather than
/// deciding. Which engines accept which escapes is docs/api.md §9.3.
fn escape_requires_re2_authority(b: &[u8], i: usize) -> bool {
    match b.get(i + 1) {
        None => true,
        Some(b'p' | b'P' | b'u' | b'U' | b'<' | b'>') => true,
        Some(b'b' | b'B') => b.get(i + 2) == Some(&b'{'),
        Some(_) => false,
    }
}

/// Where the class opened at `open` ends (issue #328's split of the two
/// failure modes the old `Option` conflated: an UNTERMINATED class is a
/// decidable joint rejection, an escape inside that only RE2 can
/// adjudicate is not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassEnd {
    /// The index of the closing `]`.
    Found(usize),
    /// No closing `]` — both engines reject (`[`, `[a`; measured).
    /// Issue #336: this outcome WINS over `Undecidable`, so `[\p{L}` is a
    /// decidable joint rejection and not a deferral.
    Unterminated,
    /// The class CLOSES but carries an escape only RE2 can adjudicate
    /// (`[\p{Alphabetic}]`), classified exactly like the bare escape.
    Undecidable,
}

/// Scans the class opened at `open`, honouring an initial `^` and
/// backslash escapes. Class contents are scanned, not skipped, so
/// `[\p{Alphabetic}]` is caught exactly like the bare `\p{Alphabetic}`.
///
/// An undecidable escape is REMEMBERED, not returned on sight (issue
/// #336): whether the class closes is decided first, because an
/// unterminated class is a joint rejection whatever it contains —
/// `[\p{L}`, `[\p{Alphabetic}`, `[\u{263A}` and `[a\` are all rejected by
/// the Rust crate, by Go's `regexp` (`missing closing ]` /
/// `invalid character class range` / `invalid escape sequence`) and by
/// ClickHouse 24.8's RE2, measured. Deciding the escape first reported
/// them `Unknown`, which is the same conflation #328 split for the bare
/// `[`, one level in.
///
/// A nested `[` is NOT treated as opening a sub-class: RE2 reads it as a
/// literal, and the class-set-operation reading the Rust crate gives it is
/// the separate value divergence handled by [`re2_pattern_to_rust`].
fn class_end(b: &[u8], open: usize) -> ClassEnd {
    let mut i = open + 1;
    if b.get(i) == Some(&b'^') {
        i += 1;
    }
    let mut undecidable = false;
    while i < b.len() {
        match b[i] {
            b'\\' => {
                undecidable |= escape_requires_re2_authority(b, i);
                // A trailing backslash steps past the end; the loop
                // condition then reports the class unterminated, which is
                // what both engines answer for `[a\`.
                i += 2;
            }
            b']' => {
                return if undecidable {
                    ClassEnd::Undecidable
                } else {
                    ClassEnd::Found(i)
                };
            }
            _ => i += 1,
        }
    }
    ClassEnd::Unterminated
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

    /// Patterns the screen defers because the Rust crate's ACCEPTANCE of
    /// them cannot be trusted to be RE2's. The list is MIXED, which is
    /// the point and was why this test used to be called
    /// `…_the_rust_crate_accepts_beyond_re2_…`: false for six of its
    /// thirteen rows. Some are docs/api.md §9.3 rows; the rest —
    /// `\p{Greek}`, `\pL`, `\P{L}`, `[\p{L}0-9]`, `(?P<name>a)` and
    /// `(?<name>a)` — appear in no §9 table because BOTH engines accept
    /// them (measured, issue #336), so deferring them costs a storage
    /// round-trip and nothing a user can see. That is deliberate: the
    /// screen models `\p` and the non-`(?:`/flag group heads WHOLESALE
    /// rather than enumerating RE2's property table and head vocabulary,
    /// which is the conservative direction.
    ///
    /// Each premise is pinned: the Rust crate must ACCEPT the anchored
    /// form, otherwise the vendored parser rejects at plan time and the
    /// screen would be guarding nothing.
    #[test]
    fn patterns_the_screen_cannot_adjudicate_are_left_to_the_authority() {
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

    /// Word-boundary escapes the Rust crate grew and RE2 never had as
    /// assertions. This is a MEANING divergence, not an acceptance one:
    /// measured for issue #336, Go's `regexp` and ClickHouse's RE2 both
    /// ACCEPT `\<word\>` — reading `\<` as a literal `<` — so the deferral
    /// buys the right reading, not a rejection. Pinned separately because
    /// `\<`/`\>`/`\b{…}` compile only on recent `regex` versions; if a
    /// future crate version rejects them the screen is harmlessly
    /// redundant, not wrong.
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

    /// Issue #328's class split, on its own terms: an unterminated class
    /// is a JOINT rejection (both engines measured to reject), an
    /// undecidable escape inside a class stays undecidable, and the split
    /// refines only the non-portable side — the boolean screen is
    /// unchanged for every one of these.
    #[test]
    fn the_class_split_separates_unterminated_from_undecidable() {
        for pattern in ["[", "[a", "[abc", "[^", "[^a"] {
            assert!(
                regex::Regex::new(pattern).is_err(),
                "premise: the Rust crate must REJECT {pattern:?} for the joint claim"
            );
            assert_eq!(scan(pattern), Scan::JointReject, "{pattern:?}");
            assert!(pattern_requires_re2_authority(pattern));
        }
        for pattern in [r"[\p{Alphabetic}]", r"[a\p{L}]"] {
            assert_eq!(scan(pattern), Scan::Undecidable, "{pattern:?}");
            assert!(pattern_requires_re2_authority(pattern));
        }
    }

    /// Issue #336: an unterminated class carrying an undecidable escape
    /// is still a DECIDABLE joint rejection — the missing `]` rejects it
    /// in every engine, so the escape never gets to matter. Measured
    /// three ways for each pattern below: the Rust `regex` crate
    /// (asserted here as the premise), Go's `regexp` at go1.25.5
    /// (`missing closing ]` for `[\p{L}`/`[a\`, `invalid character class
    /// range` for `[\p{Alphabetic}`, `invalid escape sequence` for
    /// `[\u{263A}`), and ClickHouse 24.8.14.39's RE2 (`Code: 427`
    /// `cannot compile re2` on all four). Before the fix each was
    /// `Unknown`.
    #[test]
    fn an_unterminated_class_is_a_joint_rejection_whatever_escape_it_carries() {
        for pattern in [r"[\p{L}", r"[\p{Alphabetic}", r"[\u{263A}", r"[a\"] {
            assert!(
                regex::Regex::new(pattern).is_err(),
                "premise: the Rust crate must REJECT {pattern:?} for the joint claim"
            );
            assert_eq!(scan(pattern), Scan::JointReject, "{pattern:?}");
            assert_eq!(re2_verdict(pattern), Re2Verdict::Rejects, "{pattern:?}");
            // The boolean screen is unmoved: both outcomes are
            // non-portable, which is why the frozen corpus baseline
            // cannot shift.
            assert!(pattern_requires_re2_authority(pattern), "{pattern:?}");
        }
        // The escape still decides when the class DOES close.
        for pattern in [r"[\p{L}]", r"[\p{Alphabetic}]", r"[\u{263A}]"] {
            assert_eq!(scan(pattern), Scan::Undecidable, "{pattern:?}");
            assert_eq!(re2_verdict(pattern), Re2Verdict::Unknown, "{pattern:?}");
        }
    }

    /// AC 13: the `re2_verdict` unit table — the D1′ measured list,
    /// verdict for verdict, every row measured against the rewrite
    /// rather than assumed.
    #[test]
    fn re2_verdict_matches_the_measured_table() {
        for (pattern, want) in [
            // Decidable rejections: the scan's joint class, and compile
            // failures inside the trusted region.
            ("[", Re2Verdict::Rejects),
            ("[a", Re2Verdict::Rejects),
            ("x)(y", Re2Verdict::Rejects),
            ("[a--b]", Re2Verdict::Rejects),
            ("a{2,1}", Re2Verdict::Rejects),
            // Rust rejects, RE2 accepts: the compile failure proves
            // nothing about RE2.
            (r"\Qa*\E", Re2Verdict::Unknown),
            (r"\12", Re2Verdict::Unknown),
            // Rust accepts, RE2 rejects: the screened classes.
            (r"\p{Alphabetic}", Re2Verdict::Unknown),
            ("a{1001}", Re2Verdict::Unknown),
            ("(?P<n>a)", Re2Verdict::Unknown),
            // Both accept — including the shapes only the rewrite makes
            // compilable in-process.
            ("che.*", Re2Verdict::Accepts),
            ("a{bbb}c", Re2Verdict::Accepts),
            ("[]a]", Re2Verdict::Accepts),
        ] {
            assert_eq!(re2_verdict(pattern), want, "{pattern:?}");
        }
    }

    /// The compile is BARE, per the ruling: the anchored form
    /// `^(?:x)(y)$` balances the user's stray parenthesis and compiles,
    /// which is exactly the repair the validator must not perform.
    #[test]
    fn the_verdict_compile_is_bare_not_anchored() {
        assert!(regex::Regex::new("^(?:x)(y)$").is_ok());
        assert_eq!(re2_verdict("x)(y"), Re2Verdict::Rejects);
    }

    /// The `Unknown` class census, derived from the CODE (issue #328 fix
    /// rounds 1–2): one representative per `Unknown` RETURN SITE — the
    /// scan's six `Undecidable` sites plus the two untrusted-compile
    /// arms — each verified to reach ITS OWN site by neutralising the
    /// sites one at a time (fix round 2: `a**` was standing in for the
    /// `?`-after-lazy site, which was therefore uncovered). A
    /// representative that stops being `Unknown` (the screen learned to
    /// decide it — #336's work) reddens loudly.
    #[test]
    fn every_unknown_return_site_has_a_named_class_representative() {
        for (site, class, pattern) in [
            // scan site 1: the bare-escape check
            // (`escape_requires_re2_authority`)
            ("scan/bare-escape", "unicode-property", r"\p{Alphabetic}"),
            ("scan/bare-escape", "rust-only-escape", r"\u{263A}"),
            ("scan/bare-escape", "boundary-escape", r"\<word\>"),
            ("scan/bare-escape", "trailing-backslash", "a\\"),
            // scan site 2: the same escapes inside a class (`class_end`)
            ("scan/class-escape", "unicode-property", r"[\p{Alphabetic}]"),
            // scan site 3: non-portable `(?…` group heads
            ("scan/group-head", "lookaround", "(?=x)"),
            ("scan/group-head", "named-group", "(?P<n>a)"),
            ("scan/group-head", "nonportable-group-head", "(?x)a b"),
            // scan site 4: `*`/`+` applied to a repetition
            ("scan/star-after-repeat", "repetition-of-repetition", "a**"),
            // scan site 5: `?` applied to an already-lazy repetition
            ("scan/lazy-chain", "repetition-of-repetition", "a*??"),
            // scan site 6: `{n,m}` above kMaxRepeat, or after a repetition
            ("scan/repetition-bounds", "over-max-repeat", "a{1001}"),
            (
                "scan/repetition-bounds",
                "repetition-of-repetition",
                "a{2}{3}",
            ),
            // compile arm: Rust rejects within RE2's accept set
            ("compile/untrusted-reject", "literal-quoting", r"\Qa*\E"),
            ("compile/untrusted-reject", "octal-escape", r"\12"),
            // compile arm: the Rust crate's size budget, not RE2's
            (
                "compile/too-big",
                "compiled-too-big",
                "(?:(?:(?:(?:[0-9a-f]{32}){32}){32}){32})",
            ),
        ] {
            assert_eq!(
                re2_verdict(pattern),
                Re2Verdict::Unknown,
                "{site} / {class}: {pattern:?} must be Unknown (if the screen now decides \
                 it, update the ledger's class table and the capture vectors together)"
            );
        }
    }

    /// The old boolean screen is exactly `!Portable` — the class split
    /// refines the non-portable side only.
    #[test]
    fn the_boolean_screen_is_the_scan_projected_to_portability() {
        for pattern in ["", "che.*", "[", "[a", r"\p{L}", "a{1001}", "(?P<n>a)"] {
            assert_eq!(
                pattern_requires_re2_authority(pattern),
                !matches!(scan(pattern), Scan::Portable),
                "{pattern:?}"
            );
        }
    }
}
