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
use pulsus_re2::ClickhouseMatchStrategy;

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

/// The **anchored** `match()` regex text for `pat` — `^(?:pat)$`, with
/// issue #331's ClickHouse-analyzer workaround applied when (and only
/// when) the pattern carries an affected flag-group head. Every pattern
/// [`pulsus_re2::clickhouse_match_strategy`] classifies `Verbatim`
/// renders byte-for-byte as it always did; see that function for the
/// measured defect, the `-i` rewrite's no-op proof obligation, and why
/// the never-matching `|$.` arm sits OUTSIDE the `(?:…)` that encloses
/// the user's pattern (their `(?m)` must not scope into it — with it
/// contained, `$` in the arm is end-of-text and `$.` can never match,
/// measured against 24.8.14.39 and re-measured on 26.3.17.110 — the
/// version we run — including newline-bearing subjects: `match('a\nb',
/// '^(?:z)$|$.')` is `0` on both).
fn anchored_match_regex(pat: &str) -> String {
    // One `^(?:…)$` template occurrence, whatever the strategy — the
    // issue #240 anchoring guard counts this site and keeps the
    // anchored form single-sourced.
    let (body, never_match_arm): (std::borrow::Cow<'_, str>, &str) =
        match pulsus_re2::clickhouse_match_strategy(pat) {
            ClickhouseMatchStrategy::Verbatim => (pat.into(), ""),
            ClickhouseMatchStrategy::RewriteHeads(p) => (p.into(), ""),
            ClickhouseMatchStrategy::NeverMatchArm => (pat.into(), "|$."),
        };
    format!("^(?:{body})${never_match_arm}")
}

/// The **unanchored** `match()` regex text for `pat` (line filters are
/// substring searches). Same issue #331 strategy as
/// [`anchored_match_regex`]; the never-match arm wraps the user's
/// pattern in `(?:…)` itself, both to contain their flags and so a
/// top-level `|` in their pattern cannot rebind against the arm.
fn unanchored_match_regex(pat: &str) -> String {
    match pulsus_re2::clickhouse_match_strategy(pat) {
        ClickhouseMatchStrategy::Verbatim => pat.to_string(),
        ClickhouseMatchStrategy::RewriteHeads(p) => p,
        ClickhouseMatchStrategy::NeverMatchArm => format!("(?:{pat})|$."),
    }
}

/// A label-matcher regex (`=~`/`!~`), rendered as a **fully anchored**
/// ClickHouse `match()` pattern — Loki requires the whole label value to
/// match, not a substring (architect plan interfaces:
/// `ch_string("^(?:" + pat + ")$")`, since issue #331 via
/// [`anchored_match_regex`]).
///
/// NOTE: no `pub`. Private to this module, so no alias, re-export or
/// forwarding helper in any OTHER module can reach it (enforced by rustc,
/// not by a grep) — issue #240.
fn ch_regex_anchored(pat: &str) -> String {
    ch_string(&anchored_match_regex(pat))
}

/// A line-filter regex (`|~`/`!~`), rendered as an **unanchored**
/// ClickHouse `match()` pattern — Loki's line filters are substring/RE2
/// searches over the whole log body, not full-body matches. Private like
/// [`ch_regex_anchored`]; issue #331 via [`unanchored_match_regex`].
fn ch_regex_unanchored(pat: &str) -> String {
    ch_string(&unanchored_match_regex(pat))
}

/// The VALIDATING renderer — the only regex→SQL path available to `logql/`.
/// Compiles `^(?:pat)$` — the string it escapes, byte for byte, except
/// when issue #331's workaround fires, where the emitted text differs
/// from the validated one only by a transform proven
/// compilability-preserving and semantics-preserving (the `-i` no-op or
/// the never-matching arm; `pulsus_re2::clickhouse_match_strategy`'s
/// tests and the re2_screen differential carry that proof). A pattern
/// that cannot compile is a 400 at plan time instead of a ClickHouse 500
/// mid-query, and the SQL can never disagree with its own validation
/// about anchoring.
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
///
/// **Issue #324 — `.` versus newline.** ClickHouse's `match()` compiles its
/// pattern with RE2's `dot_nl` option ON, so `.` matches a newline there,
/// while upstream RE2 — and Go's `regexp`, and therefore Prometheus —
/// leaves it off unless the `s` flag is set. Measured on 24.8.14.39 and
/// **re-measured unchanged on 26.3.17.110**, the version we run (issue
/// #376): `match('\n', '^(?:.)$')` returns `1` while `replaceRegexpOne`,
/// which reaches RE2 without ClickHouse's `OptimizedRegularExpression`
/// wrapper in front of it, reports no match — the server is not
/// self-consistent. This one did NOT get fixed by the upgrade, so the
/// `(?-s)` prefix is still load-bearing. A
/// label value carrying a line break therefore over-matched on this path
/// (they arrive through structured metadata and OTLP attributes). The
/// rendered pattern is prefixed with RE2's own `(?-s)` flag group, which
/// restores the reference's reading; a `(?s)` inside the user's pattern
/// still overrides it from that point on, exactly as upstream.
///
/// The flag goes BEFORE the anchor, not inside the group. On 24.8.14.39
/// ClickHouse's required-substring analysis mis-read a flag-negation group
/// that follows a literal and then answered `0` for every row — measured,
/// `match('abc', '^(?:(?-s)abc)$')` was `0` while RE2 says it matches —
/// and the leading position was the one it analysed correctly (corpus
/// evidence: `pulsus-read/tests/re2_screen_differential.rs`).
///
/// **That server defect is FIXED on 26.3.17.110** (issue #376: the same
/// probe now answers `1`, and the whole 28-entry flag-head registry flips
/// with it). The leading placement is nonetheless **retained pending
/// issue #331**: it is equally correct on the fixed server — `match('abc',
/// '(?-s)^(?:abc)$')` is `1` on both — and unwinding the workaround is
/// #331's decision, not a version bump's. Read this as "the placement no
/// longer has to be here", never as "the server still requires it". Composing
/// [`ch_regex_anchored`]'s own output rather than re-spelling the anchoring
/// template keeps that template single-sourced, which the issue #240
/// anchoring guard also requires.
///
/// **Issue #331** generalises that measurement: the USER's own pattern
/// can carry the same class of flag-group head (`(?s:`, `(?m)`, …) and
/// silently select no rows. [`ch_regex_anchored`] handles it; the
/// prefix surgery below composes with all three renderings (the defeat
/// arm's `|$.` binds after the trailing `$`, and this `(?-s)`'s scope
/// crossing the alternation only clears `s` in an arm that cannot
/// match anything — probed forms in the differential's registry). One
/// consequence, accepted and pinned in `metrics::dispatch`'s tests: a
/// pattern that is BOTH affected and RE2-invalid has ClickHouse echo
/// the transformed text into its 427 body, so that corner's rejection
/// detail quotes the transform rather than the user's spelling.
pub(crate) fn ch_regex_anchored_promql_re2(
    _authority: crate::metrics::PromqlRe2Fallback,
    pat: &str,
) -> String {
    let anchored = ch_regex_anchored(pat);
    // `ch_string` always opens the literal with `'`, so byte 0 is that
    // quote and `[1..]` is the pattern text plus its closing quote.
    format!("'(?-s){}", &anchored[1..])
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

    /// Issue #331: patterns with no affected flag-group head render
    /// byte-for-byte as they always did — pinned against literal
    /// strings, not against the renderer's own construction.
    #[test]
    fn unaffected_patterns_render_byte_identically() {
        for (pat, anchored, unanchored) in [
            ("prod|staging", "'^(?:prod|staging)$'", "'prod|staging'"),
            ("", "'^(?:)$'", "''"),
            ("(?:a|b)c", "'^(?:(?:a|b)c)$'", "'(?:a|b)c'"),
            ("(?i)err", "'^(?:(?i)err)$'", "'(?i)err'"),
            ("(?P<n>ab)", "'^(?:(?P<n>ab))$'", "'(?P<n>ab)'"),
            ("[(?s:]ab", "'^(?:[(?s:]ab)$'", "'[(?s:]ab'"),
            (r"\Q(?m)\E", r"'^(?:\\Q(?m)\\E)$'", r"'\\Q(?m)\\E'"),
        ] {
            assert_eq!(ch_regex_anchored(pat), anchored, "{pat:?}");
            assert_eq!(ch_regex_unanchored(pat), unanchored, "{pat:?}");
        }
    }

    /// The issue #309/#331 differential corpus (curated + generated),
    /// read from the committed fixtures so the crossing below covers
    /// the same 4,300+ patterns the live differential replays.
    fn corpus_patterns() -> Vec<String> {
        let mut out = Vec::new();
        for name in ["curated.txt", "generated.txt"] {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/re2_screen")
                .join(name);
            let text =
                std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
            out.extend(
                text.lines()
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    .map(str::to_string),
            );
        }
        assert!(out.len() > 4_000, "corpus shrank: {}", out.len());
        out
    }

    /// Issue #331 fix round 1: the corpus-wide byte-identity crossing
    /// for BOTH LogQL renderings — every pattern the strategy leaves
    /// alone renders exactly what the pre-#331 escapers produced
    /// (`ch_string("^(?:" + pat + ")$")` and `ch_string(pat)`,
    /// replicated here verbatim). The PromQL rendering's crossing lives
    /// in `tests/re2_screen_differential.rs`, which can reach the
    /// metrics seam; the raw escapers here are module-private by
    /// design, so their crossing sits beside them. Counted on both
    /// sides so neither branch can go vacuous.
    #[test]
    fn unaffected_corpus_patterns_render_byte_identically_in_both_logql_shapes() {
        let mut verbatim = 0usize;
        let mut transformed = 0usize;
        for pat in corpus_patterns() {
            match pulsus_re2::clickhouse_match_strategy(&pat) {
                ClickhouseMatchStrategy::Verbatim => {
                    verbatim += 1;
                    assert_eq!(
                        ch_regex_anchored(&pat),
                        ch_string(&format!("^(?:{pat})$")),
                        "{pat:?}: anchored rendering moved"
                    );
                    assert_eq!(
                        ch_regex_unanchored(&pat),
                        ch_string(&pat),
                        "{pat:?}: unanchored rendering moved"
                    );
                }
                _ => transformed += 1,
            }
        }
        assert!(verbatim > 3_000, "verbatim side went vacuous: {verbatim}");
        assert!(
            transformed > 100,
            "transformed side went vacuous: {transformed}"
        );
    }

    /// Issue #331: an affected head with no `i` anywhere gets the
    /// `-i`-appended rewrite inside the unchanged template.
    #[test]
    fn affected_patterns_render_the_rewritten_head() {
        assert_eq!(ch_regex_anchored("(?s:a.b)"), "'^(?:(?s-i:a.b))$'");
        assert_eq!(ch_regex_anchored("(?m)ab"), "'^(?:(?m-i)ab)$'");
        assert_eq!(ch_regex_unanchored("(?s:err.*)"), "'(?s-i:err.*)'");
        assert_eq!(ch_regex_unanchored("x(?-U)a+"), "'x(?-Ui)a+'");
    }

    /// Issue #331: an affected head coexisting with an `i`-carrying one
    /// gets the never-matching arm, outside the group that contains the
    /// user's pattern (and their flags).
    #[test]
    fn mixed_flag_patterns_render_the_never_match_arm() {
        assert_eq!(ch_regex_anchored("(?i)(?s:ab)"), "'^(?:(?i)(?s:ab))$|$.'");
        assert_eq!(ch_regex_unanchored("(?i)(?s:ab)"), "'(?:(?i)(?s:ab))|$.'");
    }

    /// Issue #331 fix round 3: an affected pattern that is not
    /// literal-leading (`=~".*foo.*"` spellings) also gets the arm —
    /// the artifact measured the rewrite losing its prefilter and
    /// running behind the arm exactly there.
    #[test]
    fn non_literal_leading_affected_patterns_render_the_never_match_arm() {
        assert_eq!(ch_regex_anchored("(?s:.*err.*)"), "'^(?:(?s:.*err.*))$|$.'");
        assert_eq!(ch_regex_unanchored(".*(?s:err)"), "'(?:.*(?s:err))|$.'");
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
