//! Issue #317: rewriting a user regex so the Rust `regex` crate reads it
//! the way **RE2** does.
//!
//! Upstream Prometheus (the metrics API's reference of record, issue #283)
//! compiles every label-matcher regex with Go's `regexp` — an RE2 port —
//! and this engine's storage path hands the same pattern to ClickHouse's
//! RE2 (issue #280, which made RE2 the authority on *acceptance*). The
//! in-process paths — the warm label cache, `concrete_name_matches`,
//! `info()`'s ignore-set matchers, `label_replace` — compile with the Rust
//! `regex` crate instead, whose grammar is a **superset**: several
//! constructs are accepted by both engines and mean different things.
//! Those are value divergences, not status ones — the query succeeds and
//! returns the wrong rows, with nothing to indicate it:
//!
//! | construct | RE2 / Go `regexp` | Rust `regex` |
//! |---|---|---|
//! | `\d` `\w` `\s` (+ negations) | ASCII (`[0-9]`, `[0-9A-Za-z_]`, `[\t\n\f\r ]`) | Unicode (`\p{Nd}`, …) |
//! | `\b` `\B` | ASCII word boundary | Unicode word boundary |
//! | `[a&&b]` | class of `a`, `&`, `b` | intersection — matches nothing |
//! | `[a~~b]` | class of `a`, `~`, `b` | symmetric difference |
//! | `[a--b]` | range `a`–`-`, i.e. **rejected** | difference — matches `a` |
//! | `[a[b]]` | class of `a`, `[`, `b`, then a literal `]` | nested class (union) |
//! | `[]a]` | class of `]`, `a` (leading `]` is a literal) | *(differs)* |
//! | `a{bbb}c`, `a{,5}` | literal braces | malformed repetition — rejected |
//!
//! [`re2_pattern_to_rust`] rewrites those constructs into Rust syntax with
//! RE2's meaning, and leaves everything else byte-identical. The rewrite is
//! applied **only to the Rust side**: the pattern that reaches ClickHouse
//! is still the user's, because RE2 already reads it correctly and
//! rewriting the SQL predicate could only add risk. The differential
//! (`pulsus-read/tests/re2_screen_differential.rs`) is the evidence: for
//! every corpus pattern both engines accept, `regex` over the rewrite and
//! RE2 over the original agree on every probe subject.
//!
//! **Not** in scope here — acceptance divergences, where one engine
//! rejects what the other compiles (`\p{Alphabetic}`, `a{1001}`, `\Q…\E`,
//! `(?P<n>…)`). Those are `metrics::re2_authority`'s job: it screens them
//! off the in-process path so RE2 returns the verdict. A rewrite whose
//! output the Rust crate rejects therefore costs a storage round-trip and
//! never a wrong answer.

use std::borrow::Cow;

/// RE2's ASCII definitions of the Perl classes (`re2/parse.cc`
/// `kPerlDigit`/`kPerlSpace`/`kPerlWord`; Go
/// `regexp/syntax/perl_groups.go`). Note `\s` has **no** vertical tab —
/// it is `[\t\n\f\r ]`, not POSIX `space`.
fn ascii_perl_class(escape: char) -> Option<&'static str> {
    Some(match escape {
        'd' => "[0-9]",
        'D' => "[^0-9]",
        'w' => "[0-9A-Za-z_]",
        'W' => "[^0-9A-Za-z_]",
        's' => r"[\t\n\f\r ]",
        'S' => r"[^\t\n\f\r ]",
        _ => return None,
    })
}

/// Rewrites `pattern` so the Rust `regex` crate reads it the way RE2 does.
///
/// Borrowed unchanged unless the pattern carries a backslash, a character
/// class or a brace — the only three bytes any rewrite rule keys off — so
/// an ordinary matcher (`api|web`, `prod-.+`) costs one scan and no
/// allocation. Never fails: a pattern neither engine can compile is passed
/// through so the compiler, not this pass, produces the error.
pub fn re2_pattern_to_rust(pattern: &str) -> Cow<'_, str> {
    if !pattern
        .as_bytes()
        .iter()
        .any(|b| matches!(b, b'\\' | b'[' | b'{' | b'}'))
    {
        return Cow::Borrowed(pattern);
    }
    // Indexed `char`s rather than bytes: a class item may be a multi-byte
    // literal and the `-`/`]` lookahead has to land on character
    // boundaries. One allocation per rewritten pattern, on a path that is
    // about to compile a regex (µs) and, at every call site, memoizes the
    // result.
    let cs: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len() + 16);
    let mut i = 0;
    while i < cs.len() {
        i = match cs[i] {
            '\\' => push_escape(&cs, i, &mut out),
            '[' => push_class(&cs, i, &mut out),
            '{' => push_brace(&cs, i, &mut out),
            // A `}` that did not close a repetition: a literal in RE2, and
            // `push_brace` has already escaped its opener.
            '}' => {
                out.push_str(r"\}");
                i + 1
            }
            c => {
                out.push(c);
                i + 1
            }
        };
    }
    Cow::Owned(out)
}

/// One escape sequence **outside** a character class, starting at the
/// backslash `cs[i]`. Returns the index just past it.
fn push_escape(cs: &[char], i: usize, out: &mut String) -> usize {
    // `\p{…}`/`\pN` first: its braces must never reach `push_brace`.
    if let Some(end) = unicode_class_end(cs, i) {
        out.extend(&cs[i..=end]);
        return end + 1;
    }
    let Some(&next) = cs.get(i + 1) else {
        // A trailing lone backslash — malformed in both engines; passed
        // through so the compiler says so.
        out.push('\\');
        return i + 1;
    };
    if let Some(ascii) = ascii_perl_class(next) {
        out.push_str(ascii);
        return i + 2;
    }
    // The crate's ASCII-boundary syntax, which is exactly RE2's `\b`.
    if next == 'b' || next == 'B' {
        out.push_str(if next == 'b' {
            r"(?-u:\b)"
        } else {
            r"(?-u:\B)"
        });
        return i + 2;
    }
    let end = escape_span_end(cs, i);
    out.extend(&cs[i..end]);
    end
}

/// A `{` outside a character class. RE2 reads a brace that does not open a
/// well-formed `{n}`/`{n,}`/`{n,m}` as a **literal** (`a{bbb}c`, `a{,5}`);
/// the Rust crate rejects it as a malformed repetition, so those patterns
/// could only ever be answered by storage. Escaping the literal case makes
/// both engines read the same thing.
fn push_brace(cs: &[char], i: usize, out: &mut String) -> usize {
    match repetition_end(cs, i) {
        // A real repetition is copied whole, so its `}` never reaches the
        // stray-brace rule.
        Some(end) => {
            out.extend(&cs[i..=end]);
            end + 1
        }
        None => {
            out.push_str(r"\{");
            i + 1
        }
    }
}

/// The index of the `}` closing a well-formed repetition opened at `cs[i]`
/// (`{n}`, `{n,}`, `{n,m}` — digits required before any comma, exactly Go's
/// `parseRepeat`). `None` when the brace is a literal in RE2.
fn repetition_end(cs: &[char], open: usize) -> Option<usize> {
    let mut i = open + 1;
    let mut digits = 0usize;
    let mut seen_comma = false;
    loop {
        match cs.get(i) {
            Some(c) if c.is_ascii_digit() => {
                digits += 1;
                i += 1;
            }
            Some(',') if !seen_comma && digits > 0 => {
                seen_comma = true;
                digits = 0;
                i += 1;
            }
            // `{n,}` needs no upper bound; every other form needs digits.
            Some('}') if digits > 0 || seen_comma => return Some(i),
            _ => return None,
        }
    }
}

/// Rewrites the character class opened at `cs[open]`, returning the index
/// just past its `]` (or past the end, for an unterminated class both
/// engines reject).
///
/// Walks items exactly as RE2 does (`re2/parse.cc` `ParseCharClass`, Go
/// `regexp/syntax` `parseClass`) and re-emits each one so the Rust crate
/// cannot read it as something else:
///
/// * a leading `]` is a literal, not the terminator (`first`);
/// * `-` is a range operator **only** when a character follows it and that
///   character is not `]` — every other `-` is a literal and is escaped, so
///   `--` can never reach the Rust crate as its difference operator;
/// * `&`, `~`, `[` and `^` are literals and are escaped, so `&&`/`~~` are
///   not set operators and `[` does not open a nested class.
fn push_class(cs: &[char], open: usize, out: &mut String) -> usize {
    out.push('[');
    let mut i = open + 1;
    if cs.get(i) == Some(&'^') {
        out.push('^');
        i += 1;
    }
    let mut first = true;
    loop {
        let Some(&c) = cs.get(i) else {
            // Unterminated: "missing closing ]" in RE2, and the Rust crate
            // rejects it too. Nothing more to emit.
            return i;
        };
        if c == ']' && !first {
            out.push(']');
            return i + 1;
        }
        first = false;

        // `[:alpha:]` and `\p{…}` mean the same in both engines; copied
        // whole so their punctuation is never mistaken for an item.
        if let Some(end) = posix_class_end(cs, i).or_else(|| unicode_class_end(cs, i)) {
            out.extend(&cs[i..=end]);
            i = end + 1;
            continue;
        }
        // `\b`/`\B` are deliberately NOT rewritten here: inside a class
        // they are not boundaries, and — unlike Perl — RE2 does not read
        // them as BACKSPACE either. It rejects them (`invalid escape
        // sequence: \b`, measured on ClickHouse 24.8), exactly as the Rust
        // crate does, so copying them through keeps the two engines'
        // rejections aligned. Issue #68 mapped them to `\x08` on Perl's
        // reading; the issue #317 differential caught that as a pattern
        // this engine would answer and RE2 would refuse.
        if c == '\\'
            && let Some(&next) = cs.get(i + 1)
            && let Some(ascii) = ascii_perl_class(next)
        {
            out.push_str(ascii);
            i += 2;
            continue;
        }

        let lo_end = escape_span_end(cs, i);
        let is_range =
            cs.get(lo_end) == Some(&'-') && matches!(cs.get(lo_end + 1), Some(&n) if n != ']');
        push_class_literal(&cs[i..lo_end], out);
        if is_range {
            out.push('-');
            let hi_end = escape_span_end(cs, lo_end + 1);
            push_class_literal(&cs[lo_end + 1..hi_end], out);
            i = hi_end;
        } else {
            i = lo_end;
        }
    }
}

/// One class item that RE2 reads as a literal character. An already-escaped
/// item is copied verbatim; a bare one is escaped when the Rust crate would
/// otherwise read it as class syntax.
fn push_class_literal(item: &[char], out: &mut String) {
    if item.len() != 1 {
        out.extend(item);
        return;
    }
    let c = item[0];
    if matches!(c, '\\' | ']' | '[' | '^' | '-' | '&' | '~') {
        out.push('\\');
    }
    out.push(c);
}

/// The index just past the single character (or escape sequence) starting
/// at `cs[i]` — `\x41` and `\x{263A}` are one character, not two tokens, so
/// the `-` after them is classified against the right position.
fn escape_span_end(cs: &[char], i: usize) -> usize {
    if cs.get(i) != Some(&'\\') {
        return i + 1;
    }
    match cs.get(i + 1) {
        None => i + 1,
        Some('x') => match cs.get(i + 2) {
            Some('{') => match cs[i + 3..].iter().position(|&c| c == '}') {
                Some(offset) => i + 3 + offset + 1,
                None => cs.len(),
            },
            _ => (i + 4).min(cs.len()),
        },
        Some(_) => i + 2,
    }
}

/// The index of the `]` closing a POSIX class `[:name:]` opened at `cs[i]`.
fn posix_class_end(cs: &[char], i: usize) -> Option<usize> {
    if cs.get(i) != Some(&'[') || cs.get(i + 1) != Some(&':') {
        return None;
    }
    let mut j = i + 2;
    while j + 1 < cs.len() {
        if cs[j] == ':' && cs[j + 1] == ']' {
            return Some(j + 1);
        }
        j += 1;
    }
    None
}

/// The index of the last character of a Unicode class `\p{…}`/`\P{…}`/
/// `\pN` starting at `cs[i]`.
fn unicode_class_end(cs: &[char], i: usize) -> Option<usize> {
    if cs.get(i) != Some(&'\\') || !matches!(cs.get(i + 1), Some('p' | 'P')) {
        return None;
    }
    match cs.get(i + 2) {
        Some('{') => cs[i + 3..]
            .iter()
            .position(|&c| c == '}')
            .map(|offset| i + 3 + offset),
        Some(_) => Some(i + 2),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rewrite must be a no-op for ordinary matchers — both for
    /// correctness and because a `Cow::Borrowed` is the whole cost story.
    #[test]
    fn ordinary_patterns_are_borrowed_unchanged() {
        for pattern in [
            "", ".*", "foo", "api|web", "(a|b)c", "(?i)foo", "prod-.+", "10.0.0.1", "a*?",
        ] {
            assert!(
                matches!(re2_pattern_to_rust(pattern), Cow::Borrowed(p) if p == pattern),
                "{pattern:?} must be borrowed unchanged"
            );
        }
    }

    #[test]
    fn perl_classes_become_their_ascii_definitions() {
        assert_eq!(re2_pattern_to_rust(r"\d+"), "[0-9]+");
        assert_eq!(re2_pattern_to_rust(r"\D"), "[^0-9]");
        assert_eq!(re2_pattern_to_rust(r"\w"), "[0-9A-Za-z_]");
        assert_eq!(re2_pattern_to_rust(r"\W"), "[^0-9A-Za-z_]");
        assert_eq!(re2_pattern_to_rust(r"\s"), r"[\t\n\f\r ]");
        assert_eq!(re2_pattern_to_rust(r"\S"), r"[^\t\n\f\r ]");
        assert_eq!(re2_pattern_to_rust(r"\d[\w]\\S"), r"[0-9][[0-9A-Za-z_]]\\S");
    }

    #[test]
    fn word_boundaries_become_the_ascii_form() {
        assert_eq!(re2_pattern_to_rust(r"\bx\B"), r"(?-u:\b)x(?-u:\B)");
    }

    /// Inside a class, `\b`/`\B` are not boundaries and — unlike Perl —
    /// not BACKSPACE either: RE2 rejects them, so they must stay
    /// uncompilable here rather than being rewritten into something the
    /// Rust crate would accept.
    #[test]
    fn class_interior_boundaries_stay_rejected_like_re2() {
        assert_eq!(re2_pattern_to_rust(r"[a\b]x\b"), r"[a\b]x(?-u:\b)");
        for pattern in [r"[\b]", r"[a\b]", r"[\B]"] {
            let rewritten = re2_pattern_to_rust(pattern);
            assert!(
                regex::Regex::new(&format!("^(?:{rewritten})$")).is_err(),
                "{pattern:?} -> {rewritten:?} must stay uncompilable"
            );
        }
    }

    /// An escaped backslash is a literal, so the letter after it is not an
    /// escape — the pass must never rewrite it.
    #[test]
    fn an_escaped_backslash_hides_the_class_that_follows() {
        assert_eq!(re2_pattern_to_rust(r"\\d"), r"\\d");
        assert_eq!(re2_pattern_to_rust(r"\\\d"), r"\\[0-9]");
        assert_eq!(re2_pattern_to_rust(r"\\b"), r"\\b");
    }

    /// Multi-character escapes are single tokens: splitting `\x{263A}`
    /// would hand `{263A}` to the brace rule and corrupt the pattern.
    #[test]
    fn multi_character_escapes_are_copied_as_units() {
        for pattern in [
            r"\x41",
            r"\x{263A}",
            r"\p{L}",
            r"\p{Greek}",
            r"\pN",
            r"\P{Nd}",
        ] {
            assert_eq!(re2_pattern_to_rust(pattern), pattern, "{pattern:?}");
        }
        assert_eq!(re2_pattern_to_rust(r"[\x41-\x5A]"), r"[\x41-\x5A]");
    }

    /// The class set operators the Rust crate has and RE2 does not.
    #[test]
    fn class_set_operators_are_escaped_into_literals() {
        assert_eq!(re2_pattern_to_rust("[a&&b]"), r"[a\&\&b]");
        assert_eq!(re2_pattern_to_rust("[a~~b]"), r"[a\~\~b]");
        assert_eq!(re2_pattern_to_rust("[a[b]]"), r"[a\[b]]");
        assert_eq!(re2_pattern_to_rust("[[a]]"), r"[\[a]]");
    }

    /// RE2 reads `-` as a range operator whenever a character other than
    /// `]` follows it, and as a literal otherwise. Reproducing that rule
    /// exactly is what stops `--` reaching the Rust crate as its difference
    /// operator — including the case where the resulting range is
    /// backwards, which both engines must then reject.
    #[test]
    fn dashes_keep_re2s_range_reading() {
        assert_eq!(re2_pattern_to_rust("[a-z]"), "[a-z]");
        assert_eq!(re2_pattern_to_rust("[a-]"), r"[a\-]");
        assert_eq!(re2_pattern_to_rust("[-a]"), r"[\-a]");
        assert_eq!(re2_pattern_to_rust("[--a]"), r"[\--a]");
        // `a-` then hi `-`: a backwards range, rejected by both engines.
        assert_eq!(re2_pattern_to_rust("[a--b]"), r"[a-\-b]");
        // Built via `format!` so `clippy::invalid_regex` does not reject
        // the deliberately-uncompilable literal at lint time (the #68
        // precedent in `eval::labels`' own tests).
        assert!(regex::Regex::new(&format!("[a-{}-b]", '\\')).is_err());
        // …but after a completed range the `-` is a fresh literal item and
        // the NEXT `-` opens a range, exactly as RE2 reads it.
        assert_eq!(re2_pattern_to_rust("[0-9--4]"), r"[0-9\--4]");
    }

    /// RE2 lets a class start with a literal `]`; the Rust crate does not
    /// read it that way.
    #[test]
    fn a_leading_close_bracket_is_a_literal() {
        assert_eq!(re2_pattern_to_rust("[]a]"), r"[\]a]");
        assert_eq!(re2_pattern_to_rust("[^]a]"), r"[^\]a]");
    }

    #[test]
    fn posix_classes_pass_through() {
        assert_eq!(re2_pattern_to_rust("[[:alpha:]]"), "[[:alpha:]]");
        assert_eq!(re2_pattern_to_rust("[^[:alpha:]0-9]"), "[^[:alpha:]0-9]");
    }

    /// A brace that does not open a repetition is a literal in RE2 and a
    /// syntax error in the Rust crate.
    #[test]
    fn literal_braces_are_escaped_and_real_repetitions_are_not() {
        assert_eq!(re2_pattern_to_rust("a{bbb}c"), r"a\{bbb\}c");
        assert_eq!(re2_pattern_to_rust("a{,5}"), r"a\{,5\}");
        assert_eq!(re2_pattern_to_rust("a{2}"), "a{2}");
        assert_eq!(re2_pattern_to_rust("a{2,}"), "a{2,}");
        assert_eq!(re2_pattern_to_rust("a{2,3}b{x}"), r"a{2,3}b\{x\}");
        assert_eq!(re2_pattern_to_rust("a{1001}"), "a{1001}");
        // Inside a class a brace is a literal in both engines already.
        assert_eq!(re2_pattern_to_rust("[{}]"), "[{}]");
        for pattern in ["a{bbb}c", "a{,5}", "a}b"] {
            let rewritten = re2_pattern_to_rust(pattern);
            assert!(
                regex::Regex::new(&format!("^(?:{rewritten})$")).is_ok(),
                "{pattern:?} -> {rewritten:?} must compile"
            );
        }
    }

    /// An unterminated class is rejected by both engines; the pass must not
    /// turn it into something that compiles.
    #[test]
    fn unterminated_constructs_are_left_uncompilable() {
        for pattern in ["[abc", "[", "[^", r"foo\", "[a-"] {
            let rewritten = re2_pattern_to_rust(pattern);
            assert!(
                regex::Regex::new(&format!("^(?:{rewritten})$")).is_err(),
                "{pattern:?} -> {rewritten:?} must stay uncompilable"
            );
        }
    }

    /// Everything the two engines already agree on must survive
    /// byte-for-byte.
    #[test]
    fn agreed_syntax_passes_through_verbatim() {
        for pattern in [
            r"a\.b(?i)ünïcode.*\n\x7f$1",
            r"\A\afoo\z",
            r"(?:a|b)c",
            r"[^a-z0-9_]",
            r"[a\]b]",
            r"\Qa*\E",
        ] {
            assert_eq!(re2_pattern_to_rust(pattern), pattern, "{pattern:?}");
        }
    }
}
