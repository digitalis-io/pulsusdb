//! ClickHouse literal/identifier escaping — the injection boundary. No user
//! string (matcher value, line-filter value, regex pattern) ever reaches a
//! generated SQL string without going through one of these functions first
//! (architect plan: "Injection — every matcher/line-filter/regex value
//! flows through escape.rs"). Keys and values are always ClickHouse
//! **string literals**, never identifiers; only fixed schema names (table
//! names, supplied by the trusted `PlanCtx`) ever use [`ch_ident`].
//!
//! Two distinct regex helpers exist because Loki's label matchers
//! (`=~`/`!~`) are **fully anchored** full-value matches, while line
//! filters (`|~`/`!~`) are **unanchored** substring/RE2 searches — using one
//! escaper for both would silently change result sets (architect plan edge
//! case: "Label-regex anchoring vs line-filter anchoring").
//!
//! **The regex half of the invariant (issue #240):** every path that turns
//! a user-supplied regex into ClickHouse SQL validates it first, by
//! compiling exactly the form it will emit. The raw regex escapers are
//! therefore PRIVATE to this module; `logql/` and `traces/` render regexes
//! only through the `_checked` forms below, and the single remaining
//! non-LogQL consumer holds a named capability token carrying its
//! justification (PromQL's SQL path deliberately defers to ClickHouse's
//! RE2 as the regex authority). Issue #282 retired TraceQL's placeholder
//! token by migrating `traces/filter.rs` to `_checked`, so the exemption
//! list shrank rather than ossified.
//!
//! CONSTRAINED MODULE (issue #240). `tests/logqltest_provenance.rs` check D
//! enforces BOTH halves of this, fail-closed:
//!   1. this file contains NO `impl` block, `trait`, `extern`,
//!      `macro_rules!`, `include!`, `derive`, or any attribute other than
//!      `#[cfg(test)]` / `#[test]`, and nothing at top level but `use`,
//!      `fn` and one private `mod tests`; each of those constructs can
//!      expose or inject an item that carries no `pub` on its own line
//!      (measured: a foreign trait implemented here for a type another
//!      module owns is callable from `logql/` with zero `pub` tokens in
//!      this file);
//!   2. every top-level item — the two PRIVATE raw escapers included —
//!      matches the committed `ESCAPE_ITEMS` table verbatim.
//!
//! Together those make the externally-reachable surface of this file
//! exactly the `pub`/`pub(crate)` subset of that table. If you have a real
//! need for a forbidden construct, the gate is meant to fail and start a
//! conversation.

use super::pipeline::PipelineError;

/// Renders `s` as a single-quoted ClickHouse string literal. `\` and `'`
/// are backslash-escaped; control characters use ClickHouse's own escape
/// sequences so the literal round-trips exactly through the server's SQL
/// parser.
pub fn ch_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

/// Renders `s` as a backtick-quoted ClickHouse identifier. Reserved for
/// fixed, trusted schema names (database/table) supplied by [`super::params::PlanCtx`]
/// — matcher keys and values are always string literals via [`ch_string`],
/// never identifiers.
pub fn ch_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('`');
    for c in s.chars() {
        match c {
            '`' => out.push_str("\\`"),
            '\\' => out.push_str("\\\\"),
            other => out.push(other),
        }
    }
    out.push('`');
    out
}

/// A label-matcher regex (`=~`/`!~`), rendered as a **fully anchored**
/// ClickHouse `match()` pattern — Loki requires the whole label value to
/// match, not a substring (architect plan interfaces:
/// `ch_string("^(?:" + pat + ")$")`).
///
/// NOTE: no `pub`. Private to this module, so no alias, re-export or
/// forwarding helper in any OTHER module can reach it (enforced by rustc,
/// not by a grep) — issue #240.
fn ch_regex_anchored(pat: &str) -> String {
    ch_string(&format!("^(?:{pat})$"))
}

/// A line-filter regex (`|~`/`!~`), rendered as an **unanchored**
/// ClickHouse `match()` pattern — Loki's line filters are substring/RE2
/// searches over the whole log body, not full-body matches. Private like
/// [`ch_regex_anchored`].
fn ch_regex_unanchored(pat: &str) -> String {
    ch_string(pat)
}

/// The VALIDATING renderer — the only regex→SQL path available to `logql/`.
/// Compiles `^(?:pat)$`, byte-for-byte the string it escapes, so a pattern
/// that cannot compile is a 400 at plan time instead of a ClickHouse 500
/// mid-query, and the SQL can never disagree with its own validation about
/// anchoring.
pub(crate) fn ch_regex_anchored_checked(pat: &str) -> Result<String, PipelineError> {
    super::pipeline::validate_anchored_regex(pat)?;
    Ok(ch_regex_anchored(pat))
}

/// As above, unanchored (LogQL line filters are RE2 substring searches).
pub(crate) fn ch_regex_unanchored_checked(pat: &str) -> Result<String, PipelineError> {
    super::pipeline::validate_unanchored_regex(pat)?;
    Ok(ch_regex_unanchored(pat))
}

/// THE ONE EXEMPTION — PromQL, and permanently so (the numbering this
/// comment used to carry existed only because TraceQL held the second;
/// #282 retired that one). Its SQL path is by design where a pattern the
/// Rust `regex` crate cannot compile is *sent* (`metrics/labels.rs:496-506`,
/// `:521-526`; `metrics/sql.rs:264-266`). Rust-validating here would reject
/// exactly the queries that fallback exists to serve.
pub(crate) fn ch_regex_anchored_promql_re2(
    _authority: crate::metrics::PromqlRe2Fallback,
    pat: &str,
) -> String {
    ch_regex_anchored(pat)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ch_string_escapes_backslash_and_quote() {
        assert_eq!(ch_string(r#"a'b\c"#), r#"'a\'b\\c'"#);
    }

    #[test]
    fn ch_string_escapes_control_characters() {
        assert_eq!(ch_string("a\nb\tc\rd"), "'a\\nb\\tc\\rd'");
    }

    #[test]
    fn ch_string_leaves_plain_text_untouched() {
        assert_eq!(ch_string("checkout"), "'checkout'");
    }

    #[test]
    fn ch_ident_escapes_backtick_and_backslash() {
        assert_eq!(ch_ident("a`b\\c"), "`a\\`b\\\\c`");
    }

    #[test]
    fn ch_regex_anchored_wraps_and_escapes() {
        assert_eq!(ch_regex_anchored("prod|staging"), "'^(?:prod|staging)$'");
    }

    #[test]
    fn ch_regex_anchored_escapes_embedded_quotes() {
        assert_eq!(ch_regex_anchored("a'b"), "'^(?:a\\'b)$'");
    }

    #[test]
    fn ch_regex_unanchored_does_not_add_anchors() {
        assert_eq!(
            ch_regex_unanchored("connection.*refused"),
            "'connection.*refused'"
        );
    }

    #[test]
    fn injection_attempt_via_single_quote_and_comment_is_neutralized() {
        // A classic SQL-injection payload: closing the string literal and
        // appending a statement. The escaped output must keep the whole
        // payload inside one literal — no unescaped `'` ever appears.
        let payload = "checkout'; DROP TABLE log_samples; --";
        let escaped = ch_string(payload);
        assert_eq!(escaped, r#"'checkout\'; DROP TABLE log_samples; --'"#);
        // The payload's own `'` must be backslash-escaped, not bare — a
        // bare `'` here would close the literal early and let the rest of
        // the payload run as SQL text.
        assert!(escaped.contains(r"\'"));
    }

    #[test]
    fn injection_attempt_via_backslash_quote_pair_is_neutralized() {
        // `\'` naively unescapes to an unescaped quote if the input's own
        // backslash isn't itself escaped first.
        let payload = r#"a\' OR '1'='1"#;
        let escaped = ch_string(payload);
        assert_eq!(escaped, r#"'a\\\' OR \'1\'=\'1'"#);
    }

    // --- the two regex injection property tests, moved from
    // `tests/injection.rs` (issue #240 §4.4): the raw escapers are now
    // module-private, so an external test crate cannot name them. Same
    // payloads (PAYLOAD_QUOTE / PAYLOAD_BACKSLASH_QUOTE / PAYLOAD_COMMENT /
    // PAYLOAD_PAREN — inlined as literals because check D6 restricts this
    // test region to `fn` items), same helper, same assertions.

    /// The four injection payloads, in `injection.rs`'s order: the classic
    /// quote-close, the backslash-quote pair, the comment truncation, and
    /// the unbalanced parenthesis.
    fn injection_payloads() -> [&'static str; 4] {
        [
            "checkout' OR '1'='1",
            r"checkout\' OR 1=1 --",
            "checkout'; DROP TABLE log_samples; --",
            "checkout') OR (1=1",
        ]
    }

    fn assert_no_unescaped_quote_or_backslash(literal: &str) {
        // `literal` is expected to be a well-formed ClickHouse single-quoted
        // string: strip the outer quotes and verify every `'`/`\` inside is
        // itself preceded by a backslash (i.e. escaped), never bare.
        assert!(
            literal.starts_with('\'') && literal.ends_with('\''),
            "{literal}"
        );
        let inner = &literal[1..literal.len() - 1];
        let mut chars = inner.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' {
                // Escaped character: consume it and move on.
                chars.next();
                continue;
            }
            assert_ne!(c, '\'', "bare unescaped quote in {literal:?}");
        }
    }

    #[test]
    fn ch_regex_anchored_never_emits_a_bare_quote_for_any_payload() {
        for payload in injection_payloads() {
            assert_no_unescaped_quote_or_backslash(&ch_regex_anchored(payload));
        }
    }

    #[test]
    fn ch_regex_unanchored_never_emits_a_bare_quote_for_any_payload() {
        for payload in injection_payloads() {
            assert_no_unescaped_quote_or_backslash(&ch_regex_unanchored(payload));
        }
    }
}
