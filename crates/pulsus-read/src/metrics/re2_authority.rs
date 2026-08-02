//! Issue #309: which matcher patterns the **warm label cache** is allowed
//! to evaluate in-process.
//!
//! The screen itself — the pure syntax scan deciding "can the Rust
//! `regex` crate's acceptance of this pattern be trusted to agree with
//! RE2's?" — lives in [`pulsus_re2`] since issue #328's D1 extraction
//! (a third surface, the TraceQL semantic validator, needed it, and
//! `pulsus-traceql → pulsus-read` would be a cargo cycle). Its contract,
//! trip table and rationale are documented there; its behaviour is
//! preserved bit-for-bit across the extraction, gated hermetically by
//! `tests/fixtures/re2_screen/screen_verdicts.txt` (the whole-corpus
//! baseline committed BEFORE the extraction, replayed by
//! `tests/re2_screen_preservation.rs`) and live by
//! `tests/re2_screen_differential.rs` against ClickHouse's real RE2.
//!
//! What stays here is the matcher-facing half — the two functions that
//! name [`super::matcher::LabelMatcher`], which `pulsus-re2` (a pure
//! regex crate) must not know about:
//! [`first_matcher_requiring_re2_authority`] (which `Re`/`Nre` matcher
//! defers to storage) and [`first_invalid_regex_detail`] (issue #316's
//! 400 detail for a pattern the in-process engine cannot compile in
//! RE2's reading of it).

/// Re-exported for the crate-internal call sites (`metrics::labels`) —
/// the #328 extraction's zero-call-site-churn contract.
pub(crate) use pulsus_re2::pattern_requires_re2_authority;
use pulsus_re2::{re2_pattern_to_rust, rust_regex_reason};

use super::matcher::{LabelMatcher, MatchOp};

/// A narrow, `#[doc(hidden)]` test seam letting the corpus differential
/// binary (`tests/re2_screen_differential.rs`) reach the screen — the same
/// shape as issue #89's `MultiMetricScanProbe`, and for the same reason: an
/// external integration-test binary cannot see this crate's `pub(crate)`
/// items, and a cargo feature would either not compile under the plain
/// `cargo test --workspace` lane or ship the seam anyway. Delegates to
/// [`pulsus_re2::pattern_requires_re2_authority`] since the #328
/// extraction.
#[doc(hidden)]
pub fn pattern_requires_re2_authority_for_test(pattern: &str) -> bool {
    pattern_requires_re2_authority(pattern)
}

/// The first `Re`/`Nre` matcher whose pattern trips
/// [`pulsus_re2::pattern_requires_re2_authority`]. `Eq`/`Neq` values are
/// literals, never compiled, so they are never screened.
pub(crate) fn first_matcher_requiring_re2_authority(
    matchers: &[LabelMatcher],
) -> Option<&LabelMatcher> {
    matchers.iter().find(|m| {
        matches!(m.op, MatchOp::Re | MatchOp::Nre) && pattern_requires_re2_authority(&m.value)
    })
}

/// Issue #316: the `detail` for a [`pulsus_promql::PromqlError::
/// InvalidRegexMatcher`] when one of `matcher_sets`' `Re`/`Nre` patterns
/// cannot be compiled at all, in RE2's reading of it
/// ([`re2_pattern_to_rust`]).
///
/// Upstream Prometheus compiles every matcher in `promql/parser`, so an
/// uncompilable pattern is a **400 `bad_data`** there. Every path that
/// reaches storage already returns that status by way of #280's classifier,
/// and the name-less selector path — which has no SQL fallback by design
/// (#85) — is the one that could not: it surfaced the same input as a
/// `422 execution` degraded-cache error, or (when the name matchers matched
/// nothing, so the label matchers were never compiled) as `200` with an
/// empty result.
///
/// The verdict is the *in-process engine's*, deliberately: after the #317
/// rewrite the Rust crate reads the pattern the way RE2 does, so the two
/// disagree only over the acceptance divergences
/// ([`pattern_requires_re2_authority`]'s subject), and only in the
/// direction the screen's docs call harmless. The residual is a pattern
/// the Rust crate rejects and RE2 accepts: with the rewrite that is no
/// longer the `a{bbb}c` family (RE2's literal braces, now rewritten), but
/// shapes such as `\Q…\E`, which upstream would answer and this path
/// rejects. It answered neither before, so nothing that worked stops
/// working — it moves from `422` to `400`.
///
/// Rendered as `^(?:pattern)$, error: reason` — the shape #280's ClickHouse
/// classifier produces, so both routes render one `invalid regexp: …` body.
/// The pattern quoted is the user's, never the rewrite.
pub(crate) fn first_invalid_regex_detail(matcher_sets: &[&[LabelMatcher]]) -> Option<String> {
    matcher_sets
        .iter()
        .flat_map(|ms| ms.iter())
        .filter(|m| matches!(m.op, MatchOp::Re | MatchOp::Nre))
        .find_map(|m| {
            let rewritten = re2_pattern_to_rust(&m.value);
            let err = regex::Regex::new(&format!("^(?:{rewritten})$")).err()?;
            Some(format!(
                "^(?:{})$, error: {}",
                m.value,
                rust_regex_reason(&err)
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #316: the 400 verdict is the in-process engine's, and only
    /// for a pattern it genuinely cannot compile in RE2's reading of it.
    #[test]
    fn only_a_pattern_the_engine_cannot_compile_earns_a_rejection() {
        let re = |value: &str| LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Re,
            value: value.to_string(),
        };

        // A backwards range: RE2 rejects `[a--b]` (`a`..`-`), and so does
        // the rewrite. The detail carries #280's shape, quoting the USER's
        // pattern and not the rewrite.
        let detail = first_invalid_regex_detail(&[&[re("[a--b]")]]).expect("a rejection");
        assert!(
            detail.starts_with(r"^(?:[a--b])$, error: "),
            "got {detail:?}"
        );
        assert!(!detail.contains(r"\-"), "the rewrite leaked: {detail:?}");

        // Nothing else earns one: patterns both engines read the same, a
        // pattern only RE2 can adjudicate (screened, not invalid), and the
        // literal-brace family the rewrite now compiles.
        for pattern in [
            "api|web",
            ".*",
            r"\d+",
            "[a&&b]",
            "[]a]",
            r"\p{Alphabetic}",
            "a{1001}",
            "a{bbb}c",
            "a{,5}",
        ] {
            assert_eq!(
                first_invalid_regex_detail(&[&[re(pattern)]]),
                None,
                "{pattern:?} must not be called invalid"
            );
        }

        // `Eq`/`Neq` values are literals, never compiled.
        assert_eq!(
            first_invalid_regex_detail(&[&[LabelMatcher {
                key: "job".to_string(),
                op: MatchOp::Eq,
                value: "[a--b]".to_string(),
            }]]),
            None
        );

        // Every set is searched, in order, and the FIRST offender wins.
        let detail = first_invalid_regex_detail(&[&[re("api")], &[re("[a--b]"), re("[z-a]")]])
            .expect("a rejection");
        assert!(detail.starts_with(r"^(?:[a--b])$"), "got {detail:?}");
    }

    /// The reason is the compiler's one-line explanation, never its
    /// multi-line diagram (which repeats the pattern and would double it
    /// into the client-facing body).
    #[test]
    fn the_rejection_reason_is_a_single_line() {
        let detail = first_invalid_regex_detail(&[&[LabelMatcher {
            key: "job".to_string(),
            op: MatchOp::Re,
            value: "[a--b]".to_string(),
        }]])
        .expect("a rejection");
        assert!(!detail.contains('\n'), "got {detail:?}");
        assert!(
            detail.ends_with("the start must be <= the end"),
            "{detail:?}"
        );
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
