//! Issue #335 Stage B — **every token legal in both spanset and field
//! position, and what the field grammar does with each.**
//!
//! The pre-collapse grammar was safe here *by accident of structure*:
//! spanset and field expressions had separate parsers, so a token could
//! not leak between the levels however the field grammar changed. The
//! collapse removed that accident — one climb now consumes whatever
//! `field_op_of` admits — so a dual-position token is exactly where the
//! accept surface can silently widen in a direction nobody planned and no
//! probe covers.
//!
//! It already happened twice while this file was being written:
//!
//! - `~` was nearly mapped to the regex operator. It is the structural
//!   SIBLING operator; regex is `=~`/`!~` (`Re`/`Nre`). Caught by a
//!   variant name, which was luck.
//! - `=~`/`!~` with a FIELD right-hand side began parsing AND validating.
//!   The old parser refused a field RHS for a regex operator, so the
//!   validator never needed a rule; the collapse deleted the guard and
//!   nothing replaced it. Measured against the pinned reference —
//!   `{ .a =~ .b }` is a 400 `invalid type for =~ or !~: .b` — and closed
//!   by `ValidateError::InvalidRegexOperand`.
//!
//! That is two silent widenings in one token class, which is why this is
//! an enumeration with a test per row rather than a spot check.

use pulsus_traceql::{parse, validate};

/// Parse ∘ validate, the axis the accept-surface matrix scores.
fn verdict(q: &str) -> Result<(), String> {
    let ast = parse(q).map_err(|e| format!("parse: {e}"))?;
    validate(&ast).map_err(|e| format!("validate: {e}"))
}

fn accepts(q: &str) -> bool {
    verdict(q).is_ok()
}

/// The dual-position tokens, and what the FIELD grammar does with each.
///
/// | token | spanset meaning | field position |
/// |---|---|---|
/// | `~` | sibling | **not a field operator** — rejected |
/// | `>` | child | comparison — accepted |
/// | `<` | parent | comparison — accepted |
/// | `>>` | descendant | **not a field operator** — rejected |
/// | `<<` | ancestor | **not a field operator** — rejected |
/// | `!` | lead of `!>`/`!~` modifiers | boolean NOT prefix — accepted |
/// | `!~` | negated sibling | regex comparison — accepted, **string RHS only** |
/// | `&` | lead of `&>`/`&~` modifiers | **not a field operator** — rejected |
/// | `&&` | boolean AND | boolean AND — accepted |
#[test]
fn structural_only_tokens_are_not_field_operators() {
    // `~`, `>>`, `<<`, `&` have no field meaning. Each must be a parse
    // error INSIDE a filter, and still work BETWEEN spansets.
    for (field_form, spanset_form) in [
        (r#"{ .a ~ .b }"#, r#"{ .a } ~ { .b }"#),
        (r#"{ .a >> .b }"#, r#"{ .a } >> { .b }"#),
        (r#"{ .a << .b }"#, r#"{ .a } << { .b }"#),
        (r#"{ .a & .b }"#, r#"{ .a } && { .b }"#),
    ] {
        assert!(
            parse(field_form).is_err(),
            "{field_form} must not parse — the token has no field meaning"
        );
        assert!(
            parse(spanset_form).is_ok(),
            "{spanset_form} must still parse — the spanset meaning is unaffected"
        );
    }
}

/// The negated and union structural modifiers (`!>`, `!<`, `!>>`, `&>`,
/// `&~`, …) must not be reachable from field position, where `!` is the
/// boolean NOT prefix and `&` is nothing at all.
#[test]
fn structural_modifiers_do_not_leak_into_field_position() {
    for q in [
        r#"{ .a !> .b }"#,
        r#"{ .a !< .b }"#,
        r#"{ .a !>> .b }"#,
        r#"{ .a &> .b }"#,
        r#"{ .a &< .b }"#,
        r#"{ .a &~ .b }"#,
    ] {
        assert!(parse(q).is_err(), "{q} must not parse in field position");
    }
    // …while every one of them works between spansets.
    for q in [
        r#"{ .a } !> { .b }"#,
        r#"{ .a } !< { .b }"#,
        r#"{ .a } &> { .b }"#,
        r#"{ .a } &~ { .b }"#,
    ] {
        assert!(parse(q).is_ok(), "{q} must still parse between spansets");
    }
}

/// `>` `<` `>=` `<=` are genuinely dual: structural between spansets,
/// comparison inside a filter. Position disambiguates, and must keep
/// doing so after the collapse.
#[test]
fn comparison_tokens_keep_both_roles() {
    for q in [
        r#"{ .a > 1 }"#,
        r#"{ .a < 1 }"#,
        r#"{ .a >= 1 }"#,
        r#"{ .a <= 1 }"#,
    ] {
        assert!(accepts(q), "{q} must be a field comparison");
    }
    for q in [r#"{ .a } > { .b }"#, r#"{ .a } < { .b }"#] {
        assert!(parse(q).is_ok(), "{q} must be a structural operator");
    }
}

/// `!` is the boolean NOT prefix in field position and the lead of a
/// negated modifier between spansets. Both must hold at once.
#[test]
fn bang_is_boolean_not_in_field_position() {
    assert!(accepts(r#"{ !.a }"#), "`!` prefixes a field expression");
    assert!(
        accepts(r#"{ !(.a = 1) }"#),
        "`!` prefixes a parenthesized one"
    );
    assert!(
        parse(r#"{ .a } !> { .b }"#).is_ok(),
        "`!>` is still the negated child operator"
    );
}

/// **The widening this enumeration found.** `=~`/`!~` are regex
/// comparisons in field position; their RHS must be a string literal.
///
/// The old parser enforced it by refusing a field RHS, so the validator
/// had no rule. The collapse removed the parser guard — a uniform operand
/// grammar cannot know the operator — and until
/// `ValidateError::InvalidRegexOperand` landed, `{ .a =~ .b }` parsed AND
/// validated. Reference (pinned digest, measured): 400 `invalid type for
/// =~ or !~: .b`, a semantic error carrying no position.
#[test]
fn regex_operators_require_a_string_right_hand_side() {
    for q in [
        r#"{ .a =~ .b }"#,
        r#"{ .a !~ .b }"#,
        r#"{ name =~ .b }"#,
        r#"{ .a =~ name }"#,
    ] {
        let Err(e) = verdict(q) else {
            panic!("{q} must be rejected — a regex RHS must be a string literal");
        };
        assert!(
            e.contains("invalid type for =~ or !~"),
            "{q}: expected the reference's own message, got {e}"
        );
    }
    for q in [r#"{ .a =~ "x" }"#, r#"{ .a !~ "y" }"#, r#"{ name =~ "z" }"#] {
        assert!(accepts(q), "{q} is a valid regex comparison");
    }
}
