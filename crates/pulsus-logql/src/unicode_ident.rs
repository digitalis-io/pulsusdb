//! The identifier rune predicate, and the keyword fold that rides with
//! it (issue #392).
//!
//! **The rule comes from the reference, not from a Rust std predicate.**
//! grafana/loki v3.7.4 builds its LogQL scanner on a vendored copy of Go
//! `text/scanner` and never assigns `IsIdentRune`
//! (`pkg/logql/syntax/query_scanner.go:157` declares the field;
//! `:339-340` is its only use; `git grep IsIdentRune pkg/` @ v3.7.4
//! returns nothing else), so the **default** rule applies verbatim:
//!
//! ```text
//! // pkg/logql/syntax/query_scanner.go:338-343 @ v3.7.4
//! func (s *Scanner) isIdentRune(ch rune, i int) bool {
//!     if s.IsIdentRune != nil { return ch != EOF && s.IsIdentRune(ch, i) }
//!     return ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch) && i > 0
//! }
//! ```
//!
//! The leading rune goes through the same predicate at `i == 0`
//! (`query_scanner.go:675`, `case s.isIdentRune(ch, 0):`), which is why
//! a decimal digit may not lead: `IsDigit(ch) && i > 0` is false there,
//! and the `case isDecimal(ch)` arm below it takes the token instead.
//!
//! **This is strictly narrower than "allow non-ASCII".** Go's
//! `unicode.IsLetter` is general category **L** and `unicode.IsDigit` is
//! general category **Nd** — nothing else. So combining marks (Mn/Mc),
//! letter-numbers (Nl) and other-numbers (No) are NOT identifier runes.
//! Measured against the pinned v3.7.4 container, `| drop का`
//! (U+0915 + U+093E, Mc), `| drop xⅧ` (Nl), `| drop x½` and `| drop x³`
//! (No) are all 400 while `| drop x٣` (Nd) is 200.
//!
//! **No Rust std predicate is this rule**, which is why the tables are
//! committed rather than a call to `char::is_alphabetic`:
//!
//! | predicate | code points | equals Go? |
//! |---|---|---|
//! | Rust `char::is_alphabetic` | 147,421 | no — true for U+093E (Mc) and U+2167 (Nl) |
//! | Rust `char::is_numeric` | 1,924 | no — true for U+00BD (No) |
//! | Go `unicode.IsLetter` (L) | 136,104 | — |
//! | Go `unicode.IsDigit` (Nd) | 680 | — |
//!
//! (Go counts measured with Go 1.25.5, `unicode.Version = 15.0.0`.)
//!
//! [`crate::unicode_ident_tables`] therefore carries general-category L
//! and Nd, generated from `regex-syntax`'s `\p{L}` / `\p{Nd}` — see that
//! module's header for the Unicode-version skew and the test that pins
//! it.

use crate::unicode_ident_tables::{DECIMAL_NUMBER, LETTER};

/// The reference's `isIdentRune(ch, 0)`: `_` or general category **L**.
/// A decimal digit may NOT lead an identifier.
///
/// ASCII takes a single `is_ascii()` branch and never touches the
/// tables, so an ordinary query pays nothing for this;
/// `tests::the_ascii_fast_path_agrees_with_the_tables_on_every_ascii_char`
/// proves the carve-out cannot disagree with the slow path.
pub(crate) fn is_ident_start(c: char) -> bool {
    if c.is_ascii() {
        return c.is_ascii_alphabetic() || c == '_';
    }
    in_ranges(LETTER, c)
}

/// The reference's `isIdentRune(ch, i)` for `i > 0`: `_`, general
/// category **L**, or general category **Nd**.
///
/// Deliberately NOT `char::is_alphanumeric()` — that is L ∪ Nl ∪ No ∪ M
/// ∪ Nd and would accept `Ⅷ`, `½` and `³`, which the reference refuses.
pub(crate) fn is_ident_continue(c: char) -> bool {
    if c.is_ascii() {
        return c.is_ascii_alphanumeric() || c == '_';
    }
    in_ranges(LETTER, c) || in_ranges(DECIMAL_NUMBER, c)
}

/// Go `strings.ToLower` — the **simple** case mapping — restricted to
/// what a LogQL keyword comparison can observe.
///
/// The reference resolves a keyword by lowercasing the scanned token
/// text at lex time (`pkg/logql/syntax/lex.go:226`,
/// `strings.ToLower(l.TokenText())`), so once an identifier may contain
/// non-ASCII runes the fold must be Go's, not ASCII-only. Exactly two
/// non-ASCII identifier runes lower to an ASCII letter — enumerated over
/// the whole code space rather than assumed, by
/// `tests::the_two_non_ascii_runes_that_fold_to_ascii_are_exactly_u0130_and_u212a`
/// here and independently with Go 1.25.5 while this was written:
///
/// * `U+0130` LATIN CAPITAL LETTER I WITH DOT ABOVE → `i`
/// * `U+212A` KELVIN SIGN → `k`
///
/// Both are live at the wire: `| <U+212A>EEP ax` is 200 and
/// `| drop <U+212A>EEP` is `400 unexpected keep`; `sum by (İGNORING)` is
/// `400 unexpected ignoring` and `| logfmt | addr = İP("1.2.3.4")` is
/// 200 (pinned v3.7.4 container).
///
/// `char::to_lowercase` cannot be used: it is the **full** mapping and
/// yields `"i\u{307}"` (two chars) for U+0130, which is not what the
/// reference compares. That fact is pinned by the same test rather than
/// left as a remembered reason.
pub(crate) fn fold_keyword_char(c: char) -> char {
    match c {
        '\u{130}' => 'i',
        '\u{212A}' => 'k',
        _ => c.to_ascii_lowercase(),
    }
}

/// Membership in a sorted, non-overlapping, non-adjacent inclusive-range
/// table. `regex-syntax` emits ranges in that form and
/// `the_committed_unicode_tables_are_regex_syntax_general_category`
/// re-derives them, so the ordering this search relies on is checked,
/// not assumed.
fn in_ranges(table: &[(char, char)], c: char) -> bool {
    table
        .binary_search_by(|(lo, hi)| {
            if c < *lo {
                core::cmp::Ordering::Greater
            } else if c > *hi {
                core::cmp::Ordering::Less
            } else {
                core::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ASCII fast path is a carve-out, and a carve-out is where the
    /// bug lives — so it is proved against the tables rather than argued
    /// from inspection. Deliberate break: make `is_ident_start`'s fast
    /// path `c.is_ascii_alphanumeric() || c == '_'` and this fails on
    /// `'0'..='9'`.
    #[test]
    fn the_ascii_fast_path_agrees_with_the_tables_on_every_ascii_char() {
        for b in 0u8..=0x7F {
            let c = b as char;
            let table_start = in_ranges(LETTER, c) || c == '_';
            assert_eq!(
                is_ident_start(c),
                table_start,
                "is_ident_start fast path disagrees with the tables at {c:?} (U+{b:04X})"
            );
            let table_continue = in_ranges(LETTER, c) || in_ranges(DECIMAL_NUMBER, c) || c == '_';
            assert_eq!(
                is_ident_continue(c),
                table_continue,
                "is_ident_continue fast path disagrees with the tables at {c:?} (U+{b:04X})"
            );
        }
    }

    /// The two-rune fold is ENUMERATED over the whole code space, not
    /// assumed from the two runes everybody remembers. Deliberate break:
    /// drop the `'\u{130}'` arm of [`fold_keyword_char`] and this fails.
    #[test]
    fn the_two_non_ascii_runes_that_fold_to_ascii_are_exactly_u0130_and_u212a() {
        let found: Vec<char> = (0u32..=0x10FFFF)
            .filter_map(char::from_u32)
            .filter(|c| {
                !c.is_ascii()
                    && (is_ident_start(*c) || is_ident_continue(*c))
                    && fold_keyword_char(*c).is_ascii_alphabetic()
            })
            .collect();
        assert_eq!(
            found,
            vec!['\u{130}', '\u{212A}'],
            "the set of non-ASCII identifier runes folding to an ASCII letter changed — \
             re-derive it against Go's simple case mapping before touching fold_keyword_char"
        );
    }

    /// Why `char::to_lowercase` cannot be the fold: it is the FULL
    /// mapping. Pinned as a fact rather than kept as a comment, because
    /// a future reader will otherwise try the obvious thing.
    #[test]
    fn rust_full_lowercasing_of_u0130_is_two_chars_and_so_cannot_be_the_fold() {
        assert_eq!('\u{130}'.to_lowercase().collect::<String>(), "i\u{307}");
        assert_eq!(fold_keyword_char('\u{130}'), 'i');
    }

    /// The discriminating cases, at the predicate level. The wire-level
    /// twins live in `tests/identifier_charset.rs`.
    #[test]
    fn the_predicates_reject_marks_letter_numbers_and_other_numbers() {
        // Mc (U+093E DEVANAGARI VOWEL SIGN AA) and Mn (U+0301 COMBINING
        // ACUTE ACCENT): `char::is_alphabetic` is true for the first.
        assert!(!is_ident_continue('\u{93e}'));
        assert!(!is_ident_continue('\u{301}'));
        // Nl (U+2167 ROMAN NUMERAL EIGHT): `char::is_alphabetic` is true.
        assert!(!is_ident_continue('\u{2167}'));
        // No (U+00BD VULGAR FRACTION ONE HALF, U+00B3 SUPERSCRIPT THREE):
        // `char::is_numeric` is true for both.
        assert!(!is_ident_continue('\u{bd}'));
        assert!(!is_ident_continue('\u{b3}'));
        // So / Cf are neither.
        assert!(!is_ident_continue('\u{1f642}'));
        assert!(!is_ident_continue('\u{200d}'));
        // Nd (U+0663 ARABIC-INDIC DIGIT THREE) continues but cannot lead.
        assert!(is_ident_continue('\u{663}'));
        assert!(!is_ident_start('\u{663}'));
        // L leads and continues.
        assert!(is_ident_start('é'));
        assert!(is_ident_continue('é'));
    }

    /// `is_alphabetic`/`is_numeric` are the fixes this table exists to
    /// rule out; the disagreement is asserted, not described.
    #[test]
    fn std_char_predicates_are_not_the_reference_rule() {
        assert!('\u{93e}'.is_alphabetic() && !is_ident_continue('\u{93e}'));
        assert!('\u{2167}'.is_alphabetic() && !is_ident_continue('\u{2167}'));
        assert!('\u{bd}'.is_numeric() && !is_ident_continue('\u{bd}'));
    }
}
