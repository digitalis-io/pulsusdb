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
//! * [`compile_user_regex`] (#291) — the ONE entry point every
//!   user-supplied pattern in the workspace compiles through, bounding
//!   what compiling it may allocate before it allocates it. See
//!   `compile_budget`'s module doc.
//! * [`re2_definitely_rejects`] (#400 Stage 2) — the reject-only
//!   pre-check LogQL's compile seams consult BEFORE compiling a pattern,
//!   for the constructs where a successful compile is the wrong answer
//!   rather than a slow one. Deliberately INDEPENDENT of the acceptance
//!   screen: it touches neither [`scan`] nor
//!   [`pattern_requires_re2_authority`], so the frozen
//!   `screen_verdicts.txt` baseline cannot move by construction.
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
//!
//! **Issue #400 Stage 2 changed what the paragraph above enumerates,
//! and the distinction is easy to lose.** The six `Undecidable` SITES
//! and their classes are unchanged — nothing in [`scan`] moved. What
//! changed is that [`re2_verdict`] consults
//! [`re2_definitely_rejects`] FIRST, so eight of the fourteen rows the
//! census used to carry now answer `Rejects` before the scan is
//! reached. **The site census is therefore no longer the `Unknown`
//! set**: it is the set of places the scan defers, of which the ones
//! that still SURFACE as `Unknown` are those no rule decides. Three
//! sites are decided in full and their rows were deleted with the
//! reason written at the deletion
//! (`every_unknown_return_site_has_a_named_class_representative`); two
//! survive with a substituted representative, because deleting a
//! surviving site's row is exactly how a site loses its cover
//! unnoticed.

use std::borrow::Cow;

mod compile_budget;
mod re2_syntax;
mod unicode_property_names;

pub use unicode_property_names::{
    go_unicode_property_name_count, go_unicode_property_names, is_go_unicode_property_name,
};

pub use compile_budget::{
    MAX_REGEX_COMPILE_TRANSIENT_BYTES, REGEX_PROGRAM_SIZE_LIMIT, RegexCompileError,
    class_ranges_for_test, compile_user_regex, compile_user_regex_anchored,
    compile_user_regex_with, per_atom_hir_charge_for_test, regex_compile_transient_bound,
    regex_compile_transient_bound_with,
};
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
    ///
    /// **Since #400 Stage 2 this is NOT the scan's deferral set.**
    /// [`re2_definitely_rejects`] is consulted first, so the eight rule
    /// families it decides answer [`Re2Verdict::Rejects`] however the
    /// scan would have classified them. `Unknown` is what is left: a
    /// deferral no rule reaches. `\p{L}` still lands here (the name IS
    /// in `unicodeTable`) while `\p{Alphabetic}` no longer does.
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
    // Issue #400 Stage 2: the DECIDABLE rejections come first. Eight of
    // this function's `Unknown` answers were "the Rust crate accepts it
    // and only a real RE2 knows" — for the eight rule families below the
    // reference's own parser settles it in-process, so the verdict is
    // taken here rather than deferred.
    if re2_definitely_rejects(pattern) {
        return Re2Verdict::Rejects;
    }
    match scan(pattern) {
        Scan::Undecidable => Re2Verdict::Unknown,
        Scan::JointReject => Re2Verdict::Rejects,
        Scan::Portable => {
            let rewritten: Cow<'_, str> = re2_pattern_to_rust(pattern);
            match compile_user_regex(&rewritten) {
                Ok(_) => Re2Verdict::Accepts,
                // Issue #291: OUR compile budget is not RE2's, exactly as
                // the crate's own `CompiledTooBig` is not. This function
                // is a VALIDATOR, and over-rejection is the harmful
                // direction (see the module doc), so a pattern refused
                // for what compiling it would cost us says nothing about
                // whether RE2 accepts it.
                Err(RegexCompileError::TooLarge { .. }) => Re2Verdict::Unknown,
                Err(RegexCompileError::Engine(regex::Error::CompiledTooBig(_))) => {
                    Re2Verdict::Unknown
                }
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

/// Constructs RE2 decidably REJECTS (issue #400 Stage 2).
///
/// The reason the function exists is the subset RE2 rejects and the Rust
/// `regex` crate ACCEPTS — that is the divergence, and it is the one that
/// serves a query the reference refuses while reading it as a different
/// pattern (`a**` as `(a*)*`, `[[:foo:]]` as a class of `:`/`f`/`o`; the
/// readings are pinned by `tests/re2_reject_classes.rs`'s
/// `the_rust_crate_reads_these_as_a_different_pattern`). But rejecting is
/// **not conditioned on the Rust side**, so a joint rejection the rules
/// also catch (`\p{Word}`, `\p{}`) answers `true` here and merely
/// re-attributes an outcome that was already agreement.
///
/// Independent of the acceptance SCREEN: [`scan`] and
/// [`pattern_requires_re2_authority`] are untouched by this, so
/// `pulsus-read`'s committed `screen_verdicts.txt` baseline cannot move —
/// by construction rather than by assertion. One left-to-right byte scan;
/// no allocation, no compilation.
///
/// **Conservative in ONE direction only.** A `false` costs nothing (the
/// compile decides, as today); a `true` REFUSES a query, so every rule
/// carries a control set of patterns the reference is measured to serve
/// (criterion 10) and the whole rule set is swept over the frozen 4,315-
/// pattern `re2_screen` corpus with every flagged pattern put to the
/// pinned container individually (criterion 21). Where a read is not
/// confident the scan declines rather than guessing.
///
/// # The rules, and where each comes from
///
/// | # | rule | reference |
/// |---|---|---|
/// | 0 | the pattern contains `\Q` → `false`, no opinion | `\Q…\E` is literal, so no rule may fire inside one |
/// | a | a repetition applied to a repetition | `ErrInvalidRepeatOp`, `parse.go:414 @ v3.7.4` |
/// | b | a `{n,m}` bound above [`RE2_MAX_REPEAT`] | `ErrInvalidRepeatSize`, `parse.go:436` |
/// | c | a `(?…` flag run carrying `u`, `x` or `R` | `parsePerlFlags` takes `i m s U - : )` only, `parse.go:1142-1253` |
/// | d | a `\u`/`\U` escape, bare or in a class | `ErrInvalidEscape`, `parse.go:1559` |
/// | e | an unknown POSIX class name inside a class | `posixGroup`, `perl_groups.go:105-134`; `ErrInvalidCharRange`, `parse.go:1610` |
/// | f | a `\p{…}`/`\P{…}`/`\pL` name outside [`is_go_unicode_property_name`] | `unicodeTable`, `parse.go:1646-1658`; raised at `:1707` |
/// | g | inside a class, a `-` in range-operator position immediately followed by an unescaped `-`, where the preceding single char is `> 0x2D` | `ErrInvalidCharRange`, `parse.go:1815` |
/// | h | a `(?P<name>`/`(?<name>` whose name carries a byte outside `[A-Za-z0-9_]` | `isValidCaptureName`, `parse.go:1261-1272`, raised at `:1185` |
///
/// Rule 0 is a conservative BAIL-OUT rather than region tracking, the
/// same shape as [`rust_rejects_beyond_its_remit`]: 27 of the first
/// draft's false rejections were constructs sitting inside a `\Q…\E`
/// region, where the reference reads them as literal text and answers
/// `200`. A false `false` costs nothing, and at the LogQL surface costs
/// nothing at all — the Rust crate refuses `\Q` outright.
///
/// Rule (g) is a RANGE rule and not a double-dash rule. `[!--b]`,
/// `[+--b]`, `[ --a]`, `[--a]`, `[--]`, `[a-z--b]`, `[\w--a]`,
/// `[a\--b]`, `[[:alpha:]--b]` and `[\n--b]` are all `200` at the
/// reference, and a "literal `--` is invalid" reading refuses every one
/// of them.
#[must_use]
pub fn re2_definitely_rejects(pattern: &str) -> bool {
    re2_rejection_construct(pattern).is_some()
}

/// The construct that made [`re2_definitely_rejects`] answer `true`, as a
/// short noun phrase, or `None` when no rule fires.
///
/// It exists for ONE reason: the LogQL seams' refusal message names the
/// construct rather than restating the engine's prose, and a `bool`
/// cannot carry that. No parity is claimed for the text — #246's owner
/// rulings (2026-07-26, 2026-08-08) pin the status and the accept/reject
/// decision only. Same single scan, same absence of allocation.
#[must_use]
pub fn re2_rejection_construct(pattern: &str) -> Option<&'static str> {
    let b = pattern.as_bytes();
    // Rule 0. Substring containment on purpose, so an escaped `\\Q` also
    // bails — that only widens the no-opinion set, which is the safe
    // direction.
    if b.windows(2).any(|w| w[0] == b'\\' && w[1] == b'Q') {
        return None;
    }
    let mut i = 0;
    let mut prev = Prev::Nothing;
    while i < b.len() {
        match b[i] {
            b'\\' => match escape_at(b, i) {
                Escape::Rejects(what) => return Some(what),
                Escape::Consumed(next) => {
                    i = next;
                    prev = Prev::Atom;
                }
            },
            b'[' => match class_at(b, i) {
                ClassScan::Rejects(what) => return Some(what),
                ClassScan::Consumed(next) => {
                    i = next;
                    prev = Prev::Atom;
                }
            },
            b'(' if b.get(i + 1) == Some(&b'?') => match group_head_at(b, i) {
                GroupHead::Rejects(what) => return Some(what),
                GroupHead::Consumed(next) => {
                    i = next;
                    prev = Prev::Nothing;
                }
            },
            b'(' | b'|' => {
                i += 1;
                prev = Prev::Nothing;
            }
            // Rule (a), the `*`/`+` half.
            b'*' | b'+' => {
                if matches!(prev, Prev::Repeat | Prev::LazyRepeat) {
                    return Some(REPEAT_OF_REPEAT);
                }
                i += 1;
                prev = Prev::Repeat;
            }
            b'?' => {
                match prev {
                    // The non-greedy marker — legal in both engines.
                    Prev::Repeat => prev = Prev::LazyRepeat,
                    // Rule (a), the lazy-chain half.
                    Prev::LazyRepeat => return Some(REPEAT_OF_REPEAT),
                    _ => prev = Prev::Repeat,
                }
                i += 1;
            }
            b'{' => match parse_repetition(b, i + 1) {
                Some(Repetition { end, over_max }) => {
                    // Rule (b), then rule (a)'s `{n,m}`-after-repetition
                    // half.
                    if over_max {
                        return Some(OVER_MAX_REPEAT);
                    }
                    if matches!(prev, Prev::Repeat | Prev::LazyRepeat) {
                        return Some(REPEAT_OF_REPEAT);
                    }
                    i = end + 1;
                    prev = Prev::Repeat;
                }
                // Not a well-formed repetition: a literal brace in RE2
                // (`a{bbb}c`, `a{,5}`, `a{}` are all `200` there).
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
    None
}

// The construct names the LogQL seams' refusal message carries. Short
// noun phrases, one per rule; no parity is claimed for the text.
const REPEAT_OF_REPEAT: &str = "a repetition applied to a repetition";
const OVER_MAX_REPEAT: &str = "a repetition bound above RE2's limit of 1000";
const RUST_ONLY_FLAG: &str = "a `(?x`/`(?u`/`(?R` group flag RE2 does not have";
const RUST_ONLY_ESCAPE: &str = "a `\\u`/`\\U` escape RE2 does not have";
const UNKNOWN_POSIX_CLASS: &str = "an unrecognised POSIX character class name";
const UNKNOWN_PROPERTY: &str = "an unrecognised Unicode property name";
const INVERTED_DASH_RANGE: &str = "a character-class range whose end is below its start";
const INVALID_CAPTURE_NAME: &str = "a capture name outside [A-Za-z0-9_]";

/// What reading one escape produced: a decidable rejection, or the index
/// just past it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Escape {
    Rejects(&'static str),
    Consumed(usize),
}

/// Reads the escape at `b[i]` (a backslash) OUTSIDE a character class,
/// applying rules (d) and (f).
///
/// **A brace-form escape is consumed WHOLE** — `\x{…}`, `\p{…}`, `\P{…}`,
/// `\b{…}`, `\B{…}`. This is not a nicety: a naive `i += 2` leaves `{41}`
/// of `\x{41}` to be read as a repetition, so `\x{41}{2}` and `\p{L}{2}`
/// — both `200` at the reference — would fire rule (a). That was the
/// second of the two first-draft false-rejection causes.
fn escape_at(b: &[u8], i: usize) -> Escape {
    match b.get(i + 1) {
        // A trailing backslash: `ErrTrailingBackslash` there and an
        // error in the Rust crate too — already agreement, so no rule
        // needs to claim it.
        None => Escape::Consumed(i + 1),
        // Rule (d). RE2 has no `\u`/`\U` escape in any spelling:
        // `A`, `\u{263A}`, `\U00000041`, `\U0001F600` and
        // `\U{1F600}` are each `invalid escape sequence` there, while the
        // Rust crate compiles all of them.
        Some(b'u' | b'U') => Escape::Rejects(RUST_ONLY_ESCAPE),
        // Rule (f).
        Some(b'p' | b'P') => property_escape_at(b, i),
        // The brace-form escapes that are NOT properties.
        Some(b'x' | b'b' | b'B') if b.get(i + 2) == Some(&b'{') => {
            Escape::Consumed(brace_end(b, i + 2))
        }
        // The backslash plus ONE RUNE, so a multi-byte escaped character
        // cannot leave the scan straddling a code point.
        Some(&c) => Escape::Consumed((i + 1 + utf8_len(c)).min(b.len())),
    }
}

/// The index just past the `}` closing the brace opened at `open`, or the
/// end of the pattern when there is none.
fn brace_end(b: &[u8], open: usize) -> usize {
    match b[open..].iter().position(|&c| c == b'}') {
        Some(off) => open + off + 1,
        None => b.len(),
    }
}

/// Rule (f): `b[i]` is the backslash of a `\p`/`\P` escape.
///
/// Mirrors `parseUnicodeClass` (`parse.go:1663-1708 @ v3.7.4`): a
/// single-letter form `\pL`, or a braced form whose name runs to the
/// FIRST `}` anywhere in the remainder — then a single leading `^` is
/// stripped and the sign flipped (`parse.go:1698-1701`), so `\p{^L}` is
/// `\P{L}` and is SERVED. The name that survives that stripping is what
/// `unicodeTable` is asked about.
fn property_escape_at(b: &[u8], i: usize) -> Escape {
    match b.get(i + 2) {
        // `\p` at the end of the pattern: `ErrInvalidCharRange` there,
        // and the Rust crate rejects it too — agreement, and the rule
        // declines rather than re-attributing a truncation.
        None => Escape::Consumed(b.len()),
        Some(b'{') => {
            let Some(off) = b[i + 3..].iter().position(|&c| c == b'}') else {
                // No `}` at all: `ErrInvalidCharRange` there and an error
                // in the Rust crate — agreement, and the name cannot be
                // read, so decline.
                return Escape::Consumed(b.len());
            };
            let end = i + 3 + off;
            let name = &b[i + 3..end];
            if property_name_rejects(name) {
                Escape::Rejects(UNKNOWN_PROPERTY)
            } else {
                Escape::Consumed(end + 1)
            }
        }
        Some(_) => {
            // Single-letter form: `parseUnicodeClass` takes ONE RUNE, not
            // one byte, so `\pé` is the name `é`.
            let rest = &b[i + 2..];
            let len = utf8_len(rest[0]);
            let end = (i + 2 + len).min(b.len());
            if property_name_rejects(&b[i + 2..end]) {
                Escape::Rejects(UNKNOWN_PROPERTY)
            } else {
                Escape::Consumed(end)
            }
        }
    }
}

/// `true` when `unicodeTable` would answer `nil` for this raw property
/// name — i.e. the reference raises `ErrInvalidCharRange`.
///
/// An empty name (`\p{}`) is a rejection there; so is a bare `^`
/// (`\p{^}`), because stripping it leaves nothing for the table.
fn property_name_rejects(raw: &[u8]) -> bool {
    let name = match raw.first() {
        Some(b'^') => &raw[1..],
        _ => raw,
    };
    match std::str::from_utf8(name) {
        Ok(name) => !is_go_unicode_property_name(name),
        // Not UTF-8: `checkUTF8` refuses it there and the Rust crate
        // cannot hold it either — but this scan is handed a `&str`, so
        // the arm is unreachable in practice and declines rather than
        // guessing.
        Err(_) => false,
    }
}

/// The byte length of the UTF-8 sequence `first` opens. A continuation
/// byte answers 1 so a scan that ever lands mid-rune re-synchronises
/// instead of stepping over the next construct.
fn utf8_len(first: u8) -> usize {
    match first {
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => 1,
    }
}

/// What reading one `(?…` head produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupHead {
    Rejects(&'static str),
    Consumed(usize),
}

/// Rules (c) and (h): `b[i]` is a `(` followed by a `?`.
///
/// `parsePerlFlags` (`parse.go:1142-1253 @ v3.7.4`) tries the three named
/// -capture spellings first and otherwise reads a flag run.
fn group_head_at(b: &[u8], i: usize) -> GroupHead {
    // Go: `startsWithP := len(t) > 4 && t[2] == 'P' && t[3] == '<'` and
    // `startsWithName := len(t) > 3 && t[2] == '<'`, with `t` starting at
    // the `(` — so the shorter forms fall through to the flag run.
    let t = &b[i..];
    let starts_with_p = t.len() > 4 && t[2] == b'P' && t[3] == b'<';
    let starts_with_name = t.len() > 3 && t[2] == b'<';
    if starts_with_p || starts_with_name {
        let name_start = if starts_with_p { 4 } else { 3 };
        // Go takes the FIRST `>` in the whole remainder, not the first
        // inside the head.
        let Some(off) = t.iter().position(|&c| c == b'>') else {
            // `ErrInvalidNamedCapture` there and an error in the Rust
            // crate — agreement; decline.
            return GroupHead::Consumed(b.len());
        };
        let name = &t[name_start..off];
        // Rule (h). An EMPTY name is `(?P<>a)`, which the Rust crate also
        // refuses, so the rule does not claim it.
        if !name.is_empty()
            && name
                .iter()
                .any(|c| !c.is_ascii_alphanumeric() && *c != b'_')
        {
            return GroupHead::Rejects(INVALID_CAPTURE_NAME);
        }
        return GroupHead::Consumed(i + off + 1);
    }
    // Rule (c): the flag run. `{u, x, R}` is exactly the Rust-valid
    // minus RE2-valid flag set, so a run carrying one of them is
    // `ErrInvalidPerlOp` there and compiles here.
    let mut j = i + 2;
    let mut saw_rust_only = false;
    while j < b.len() {
        match b[j] {
            b'u' | b'x' | b'R' => {
                saw_rust_only = true;
                j += 1;
            }
            b'i' | b'm' | b's' | b'U' | b'-' => j += 1,
            b':' | b')' => {
                return if saw_rust_only {
                    GroupHead::Rejects(RUST_ONLY_FLAG)
                } else {
                    GroupHead::Consumed(j + 1)
                };
            }
            // Not a flag run at all — `(?#c)`, `(?'n'…)`, `(?=…)`,
            // `(?P=n)`. Every one of these is `ErrInvalidPerlOp` there
            // AND an error in the Rust crate, so no rule claims them.
            _ => return GroupHead::Consumed(i + 2),
        }
    }
    // An unterminated head: `ErrInvalidPerlOp` there, error here.
    GroupHead::Consumed(b.len())
}

/// What scanning one character class produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassScan {
    Rejects(&'static str),
    Consumed(usize),
}

/// The fourteen POSIX class names `posixGroup` carries
/// (`perl_groups.go:105-134 @ v3.7.4`), each of which also has a `[:^…:]`
/// form. Anything else inside a class is `ErrInvalidCharRange`.
const POSIX_CLASS_NAMES: &[&str] = &[
    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print", "punct",
    "space", "upper", "word", "xdigit",
];

/// Scans the character class opened at `open`, applying rules (d), (e),
/// (f) and (g); returns the index just past the closing `]`.
///
/// Follows `parseClass` (`parse.go:1736-1830 @ v3.7.4`) item by item,
/// because rule (g) is about POSITION: a `-` is a range operator only
/// directly after a SINGLE CHARACTER, and only when what follows it is
/// neither the class's own `]` nor the end of the pattern. After a
/// completed range, after a class-shaped item (`\w`, `[:alpha:]`,
/// `\p{L}`), at class start or after `^`, a `-` is an ordinary member.
///
/// An unterminated class is not this function's business: both engines
/// reject it (issue #328's `Scan::JointReject`), so it declines.
fn class_at(b: &[u8], open: usize) -> ClassScan {
    let mut i = open + 1;
    if b.get(i) == Some(&b'^') {
        i += 1;
    }
    // Go: `first := true // ] and - are okay as first char in class`.
    let mut first = true;
    while i < b.len() {
        if b[i] == b']' && !first {
            return ClassScan::Consumed(i + 1);
        }
        first = false;

        // Rule (e): a POSIX named class. Go requires the closing `:]`;
        // without one the bytes are ordinary members (`[[:foo]` is
        // `200`), so the rule must require it too.
        if b[i] == b'['
            && b.get(i + 1) == Some(&b':')
            && i + 2 < b.len()
            && let Some(off) = find(&b[i + 2..], b":]")
        {
            let name = &b[i + 2..i + 2 + off];
            let bare = match name.first() {
                Some(b'^') => &name[1..],
                _ => name,
            };
            let known = std::str::from_utf8(bare)
                .is_ok_and(|n| POSIX_CLASS_NAMES.binary_search(&n).is_ok());
            if !known {
                return ClassScan::Rejects(UNKNOWN_POSIX_CLASS);
            }
            i = i + 2 + off + 2;
            continue;
        }

        if b[i] == b'\\' {
            match b.get(i + 1) {
                // Rule (d), inside a class.
                Some(b'u' | b'U') => return ClassScan::Rejects(RUST_ONLY_ESCAPE),
                // Rule (f), inside a class.
                Some(b'p' | b'P') => match property_escape_at(b, i) {
                    Escape::Rejects(what) => return ClassScan::Rejects(what),
                    Escape::Consumed(next) => {
                        i = next;
                        continue;
                    }
                },
                // A Perl class escape is a class-shaped item: the `-`
                // after it is a member, not an operator.
                Some(b'd' | b'D' | b's' | b'S' | b'w' | b'W') => {
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }

        // A single character — `parseClassChar`, which is `parseEscape`
        // or one rune.
        let (lo, next) = class_char_at(b, i);
        i = next;

        // Go: `if len(t) >= 2 && t[0] == '-' && t[1] != ']'`. Both
        // clauses are load-bearing: `[a-]` is `(a|-)` there, and so is
        // a `-` that ends the pattern.
        if b.len() - i >= 2 && b[i] == b'-' && b[i + 1] != b']' {
            if b[i + 1] == b'-' {
                // Rule (g). `X--` is the range `X`..`-`, invalid exactly
                // when `X > 0x2D`.
                if lo.is_some_and(|lo| lo > 0x2D) {
                    return ClassScan::Rejects(INVERTED_DASH_RANGE);
                }
                i += 2;
            } else {
                let (_, next) = class_char_at(b, i + 1);
                i = next;
            }
        }
    }
    // Unterminated.
    ClassScan::Consumed(b.len())
}

/// One `parseClassChar`: the code point it contributes (`None` when the
/// escape is one this scan will not decode, which makes rule (g) decline
/// rather than guess) and the index just past it.
fn class_char_at(b: &[u8], i: usize) -> (Option<u32>, usize) {
    if b[i] != b'\\' {
        let len = utf8_len(b[i]).min(b.len() - i);
        let cp = std::str::from_utf8(&b[i..i + len])
            .ok()
            .and_then(|s| s.chars().next())
            .map(u32::from);
        return (cp, i + len);
    }
    match b.get(i + 1) {
        None => (None, b.len()),
        Some(b'x') => {
            if b.get(i + 2) == Some(&b'{') {
                let end = brace_end(b, i + 2);
                // `brace_end` answers `b.len()` when there is no `}` at
                // all, in which case there is no value to read.
                let closed = b.get(end - 1) == Some(&b'}');
                let hex = if closed { &b[i + 3..end - 1] } else { &b[..0] };
                (hex_value(hex), end)
            } else {
                let end = (i + 4).min(b.len());
                (hex_value(&b[i + 2..end]), end)
            }
        }
        Some(c @ (b'a' | b'f' | b'n' | b'r' | b't' | b'v')) => {
            let cp = match c {
                b'a' => 0x07,
                b'f' => 0x0C,
                b'n' => 0x0A,
                b'r' => 0x0D,
                b't' => 0x09,
                _ => 0x0B,
            };
            (Some(cp), i + 2)
        }
        // An octal escape (`\0`-`\7`, up to three digits) and every
        // punctuation escape (`\-`, `\\`, `\]`) decode to a code point,
        // but only the punctuation forms matter to rule (g) and only via
        // the `<= 0x2D` half, so both decline: `None` can never make the
        // rule fire.
        Some(&c) if c.is_ascii_punctuation() => (Some(u32::from(c)), i + 2),
        // The backslash plus ONE RUNE, so the scan cannot straddle a code
        // point; `None` can never make rule (g) fire.
        Some(&c) => (None, (i + 1 + utf8_len(c)).min(b.len())),
    }
}

/// The value of a run of hex digits, or `None` if it is empty or carries
/// a non-hex byte.
fn hex_value(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() {
        return None;
    }
    let mut v: u32 = 0;
    for &c in bytes {
        let d = (c as char).to_digit(16)?;
        v = v.checked_mul(16)?.checked_add(d)?;
    }
    Some(v)
}

/// The index of the first occurrence of `needle` in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
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
        // The escape still decides when the class DOES close — and the
        // SCAN is unmoved for all three, which is the property #400
        // Stage 2 had to preserve: `re2_definitely_rejects` is a
        // separate mechanism consulted ahead of the scan, so the frozen
        // `screen_verdicts.txt` baseline cannot shift.
        for pattern in [r"[\p{L}]", r"[\p{Alphabetic}]", r"[\u{263A}]"] {
            assert_eq!(scan(pattern), Scan::Undecidable, "{pattern:?}");
        }
        // The VERDICT moved for two of the three, and only where a rule
        // reads the reference's own table: `Alphabetic` is outside
        // `unicodeTable` (rule (f)) and `\u` is no escape RE2 has (rule
        // (d)), while `L` is in the table and stays undecided.
        assert_eq!(re2_verdict(r"[\p{L}]"), Re2Verdict::Unknown);
        assert_eq!(re2_verdict(r"[\p{Alphabetic}]"), Re2Verdict::Rejects);
        assert_eq!(re2_verdict(r"[\u{263A}]"), Re2Verdict::Rejects);
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
            // Rust accepts, RE2 rejects. **Issue #400 Stage 2 moved the
            // first two from `Unknown` to `Rejects`** — rules (f) and
            // (b) decide them from the reference's own parser, so they
            // no longer need a real RE2. `(?P<n>a)` stays `Unknown`
            // because BOTH engines accept it: no rule claims a valid
            // capture name, and the screen defers group heads wholesale.
            (r"\p{Alphabetic}", Re2Verdict::Rejects),
            ("a{1001}", Re2Verdict::Rejects),
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
            //
            // Issue #400 Stage 2 re-representatived this site. It used to
            // be `\p{Alphabetic}`, which rule (f) now DECIDES — the name
            // is outside `unicodeTable`'s 202 — so it answers `Rejects`
            // and would have reddened this row for the right reason.
            // `\p{L}` reaches the same site and stays `Unknown`: the name
            // IS in the table, so no rule fires and only the scan speaks.
            ("scan/bare-escape", "unicode-property", r"\p{L}"),
            // ...and `rust-only-escape` was DELETED here: rule (d)
            // refuses every `\u`/`\U` spelling there is, so the class has
            // no `Unknown` member left to represent.
            ("scan/bare-escape", "boundary-escape", r"\<word\>"),
            ("scan/bare-escape", "trailing-backslash", "a\\"),
            // scan site 2: the same escapes inside a class (`class_end`).
            // Re-representatived for the same reason as site 1.
            ("scan/class-escape", "unicode-property", r"[\p{L}]"),
            // scan site 3: non-portable `(?…` group heads. `(?x)a b` now
            // answers `Rejects` (rule (c)), so the class is represented
            // by `(?#c)a` — a head rule (c) deliberately does not claim,
            // because `#` is not one of the `{u, x, R}` flags and the
            // reference's own refusal of it is matched by the Rust
            // crate's.
            ("scan/group-head", "lookaround", "(?=x)"),
            ("scan/group-head", "named-group", "(?P<n>a)"),
            ("scan/group-head", "nonportable-group-head", "(?#c)a"),
            // **Three sites were DELETED by #400 Stage 2, and deleting a
            // row is only correct when the rules decide the site IN
            // FULL** — otherwise the site loses its cover silently, which
            // is the failure mode this census exists to prevent. Each of
            // these three is decided by a rule with no residue:
            //
            // * `scan/star-after-repeat` (`a**`, `a*+`) — rule (a) fires
            //   for every `*`/`+` whose operand is a repetition.
            // * `scan/lazy-chain` (`a*??`) — rule (a)'s other half fires
            //   for every `?` after a lazy repetition.
            // * `scan/repetition-bounds` (`a{1001}`, `a{2}{3}`) — the
            //   site returns `Undecidable` on exactly two conditions,
            //   `over_max` and a repetition operand, and rules (b) and
            //   (a) take one each.
            //
            // compile arm: Rust rejects within RE2's accept set. Both
            // survive: rule 0 bails out of any pattern containing `\Q`,
            // and no rule looks at octal escapes.
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
