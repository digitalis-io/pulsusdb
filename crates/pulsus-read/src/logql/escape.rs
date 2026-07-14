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

/// A LogQL label matcher regex (`=~`/`!~`), rendered as a **fully anchored**
/// ClickHouse `match()` pattern — Loki requires the whole label value to
/// match, not a substring (architect plan interfaces:
/// `ch_string("^(?:" + pat + ")$")`).
pub fn ch_regex_anchored(pat: &str) -> String {
    ch_string(&format!("^(?:{pat})$"))
}

/// A LogQL line-filter regex (`|~`/`!~`), rendered as an **unanchored**
/// ClickHouse `match()` pattern — Loki's line filters are substring/RE2
/// searches over the whole log body, not full-body matches.
pub fn ch_regex_unanchored(pat: &str) -> String {
    ch_string(pat)
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
}
