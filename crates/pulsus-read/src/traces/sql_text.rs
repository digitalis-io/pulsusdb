//! The SQL text helpers the span-table statements share (issue #602
//! part 3a): the planning error, the comparison operators, the status and
//! kind codes, number rendering, the one regular-expression renderer and
//! the string byte cap. Moved unchanged from the previous span-filter
//! compiler, which went with the previous metrics SQL.
//!
//! Injection boundary: every user-controlled regex flows through
//! [`crate::logql::escape`] before it reaches a SQL fragment.

use pulsus_traceql::{ComparisonOp, SpanKindValue, StatusValue};

use crate::logql::escape;
use crate::logql::pipeline::PipelineError;

/// Planning failure — always a caller error, never an execution failure.
/// [`PlanError::UnsupportedField`]/[`PlanError::TypeMismatch`] map to
/// `400 bad_data` server-side; [`PlanError::MetricsPointCap`] is the one
/// exception — the adjudicated issue #59 bounded-response contract makes
/// a metrics range that resolves more than `MAX_METRICS_POINTS` buckets a
/// static pre-execution `422 query_too_broad`, never a 400 and never a
/// silent truncation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("unsupported field: {0}")]
    UnsupportedField(String),
    #[error("type mismatch: {0}")]
    TypeMismatch(String),
    #[error("metrics range resolves {buckets} buckets, exceeding the {cap}-point cap")]
    MetricsPointCap { buckets: i64, cap: i64 },
}

pub(crate) const OP_SYMBOLS: [(ComparisonOp, &str); 6] = [
    (ComparisonOp::Eq, "="),
    (ComparisonOp::Neq, "!="),
    (ComparisonOp::Gt, ">"),
    (ComparisonOp::Gte, ">="),
    (ComparisonOp::Lt, "<"),
    (ComparisonOp::Lte, "<="),
];

pub(crate) fn sql_op(op: ComparisonOp) -> Option<&'static str> {
    OP_SYMBOLS.iter().find(|(o, _)| *o == op).map(|(_, s)| *s)
}

pub(crate) fn status_code(v: StatusValue) -> i8 {
    match v {
        StatusValue::Unset => 0,
        StatusValue::Ok => 1,
        StatusValue::Error => 2,
    }
}

/// The stored `kind` code is OTLP's `SpanKind`, not the reference's own
/// `Kind` enum — the two order the members differently, and the column
/// this predicate compares against is written from OTLP on ingest. So
/// `unspecified` is 0, as the metrics statement's kind label maps it
/// (`spans/metrics.rs`, issue #335 Stage D1, class D13).
///
/// **This `match` is exhaustive with no wildcard on purpose.** The
/// statements' kind labels are SQL string constants, which the compiler
/// cannot check, so this is the one read-path site that refuses a new
/// variant. Adding a wildcard here would remove it.
pub(crate) fn kind_code(v: SpanKindValue) -> i8 {
    match v {
        SpanKindValue::Unspecified => 0,
        SpanKindValue::Internal => 1,
        SpanKindValue::Server => 2,
        SpanKindValue::Client => 3,
        SpanKindValue::Producer => 4,
        SpanKindValue::Consumer => 5,
    }
}

/// Parses a TraceQL number literal (digit/dot strings by lexer
/// construction) to a finite `f64` for `val_num` comparisons — re-rendered
/// via Rust `Display` so the SQL fragment is deterministic and can never
/// carry raw user text.
pub(crate) fn parse_num(raw: &str) -> Result<f64, PlanError> {
    raw.parse::<f64>()
        .ok()
        .filter(|n| n.is_finite())
        .ok_or_else(|| PlanError::TypeMismatch(format!("not a finite number: {raw:?}")))
}

/// Renders an `f64` as a ClickHouse numeric literal (finite by
/// construction — [`parse_num`] / `Duration::as_nanos` inputs only).
pub(crate) fn render_num(n: f64) -> String {
    // `{}` on a finite f64 never yields exponent-free ambiguity issues for
    // ClickHouse (`500`, `1.5`, `0.95`).
    format!("{n}")
}

/// The ONE regex→SQL renderer on the TraceQL path (issue #282): the
/// checked escaper compiles `^(?:pat)$` — byte-for-byte the string it
/// escapes — so an uncompilable pattern is a plan-time [`PlanError`]
/// (`400`) rather than a mid-query ClickHouse error, and no second
/// validator can disagree with what the SQL actually says.
///
/// The message keeps this module's `invalid regex {pat:?}: …` shape. Only
/// the inner reason moves: `escape::ch_regex_anchored_checked` reports the
/// error of the pattern the CLIENT wrote when that alone fails to compile
/// (the #240 rule), instead of the `^(?:…)$` rewrite's error.
pub(crate) fn anchored_regex_sql(pat: &str) -> Result<String, PlanError> {
    escape::ch_regex_anchored_checked(pat).map_err(|e| {
        let PipelineError::BadRegex(reason) = e else {
            // `ch_regex_anchored_checked` constructs no other variant;
            // rendering defensively keeps this total without a panic.
            return PlanError::TypeMismatch(format!("invalid regex {pat:?}: {e}"));
        };
        PlanError::TypeMismatch(format!("invalid regex {pat:?}: {reason}"))
    })
}

/// Reflects a comparison operator across its operands (`a < b` ⇒ `b > a`)
/// so a folded `<scalar> <op> <attr>` becomes an `<attr>`-first numeric
/// leaf. Equality/inequality are symmetric.
pub(crate) fn flip_comparison(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::Gte => ComparisonOp::Lte,
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::Lte => ComparisonOp::Gte,
        other => other,
    }
}

/// Hard **byte** ceiling on every string value the search response
/// returns (`name`/`service`/`select()`-projected attribute values) —
/// owner-approved response truncation (issue #57 re-audit, comment
/// 5028629688/5028693510). ClickHouse's `length()` counts bytes, so
/// [`byte_capped`]'s `length(col) <= TRACE_STR_COL_CAP` branch is
/// byte-identical passthrough for every string at or under the cap; the
/// fallback branch cuts at [`TRACE_STR_COL_CP_FALLBACK`] UTF-8 code
/// points instead of bytes, but a code point is at most 4 bytes, so the
/// fallback output itself never exceeds this same byte ceiling either —
/// documented in docs/api.md §4.2.
pub(crate) const TRACE_STR_COL_CAP: u64 = 8192;

/// The truncation fallback's code-point cut: `TRACE_STR_COL_CAP / 4` — a
/// UTF-8 sequence is at most 4 bytes per code point, so
/// `TRACE_STR_COL_CP_FALLBACK` code points can never exceed
/// `TRACE_STR_COL_CAP` bytes even at the worst-case 4-byte width.
///
/// `pub(crate)` since issue #492 part 5, for one further consumer:
/// [`super::search_plan::generator_exactness`] refuses a
/// `resource.service.name` literal of this many code points or more,
/// because that is exactly where a capped reading and a raw one can
/// first agree on a value they should not. Measured on ClickHouse 26.3:
/// with a stored `service` of `'\u{1D11E}' x 2048 + '0'` (8193 bytes,
/// 2049 code points) and a literal of `'\u{1D11E}' x 2048` (2048 code
/// points), `stored = literal` is `0` and `cap(stored) = literal` is
/// `1` — the generator's `PREWHERE` compares the raw column and the
/// evaluator the capped one, so below 2048 code points they cannot
/// disagree and at 2048 they can.
pub(crate) const TRACE_STR_COL_CP_FALLBACK: u64 = TRACE_STR_COL_CAP / 4;

/// The unaliased, unwrapped byte-bound truncation expression — the ONE
/// definition of the cap (issue #184 plan v4: `byte_capped`,
/// and the [`trace_ctx_sql`] co-load's `argMin` value
/// projections all build on this single helper, so the cap length and
/// fallback can never diverge between the displayed-root path and the
/// trace-context co-load; the in-module AC-Δ1c/Δ1d/Δ1e tests pin the
/// construction). The span predicate's `statusMessage` leaf and the tag
/// reads compare this same capped expression, so a value read and a value
/// compared agree byte for byte.
pub(crate) fn byte_cap_expr(col: &str) -> String {
    format!(
        "if(length({col}) <= {TRACE_STR_COL_CAP}, {col}, \
         substringUTF8({col}, 1, {TRACE_STR_COL_CP_FALLBACK}))"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pattern the checked escaper accepts renders SQL. The acceptance
    /// SET was unchanged from the `compile_anchored` validator this
    /// replaced (both compile `^(?:pat)$` with the same crate) — **until
    /// issue #400 Stage 2**, which put `pulsus_re2::re2_definitely_rejects`
    /// ahead of that compile at `escape::ch_regex_anchored_checked`. So
    /// the relation is now the compile MINUS the pre-check, and it is
    /// asserted as that rather than as equality, which would have to be
    /// weakened to a subset and stop detecting a rejection the pre-check
    /// did NOT cause.
    ///
    /// **The narrowing is toward parity and is redundant here.** Every
    /// construct the pre-check claims is a reference `400`
    /// (`tests/conformance/validate-vectors.json`, rows rx-u4/u5/u6/u12/
    /// u13/u16/u18/u19/u21/u22), and `pulsus_traceql::validate` already
    /// refuses them one layer above this on every route that validates.
    /// This site is the second gate on the same decision, in the same
    /// direction.
    #[test]
    fn the_checked_escaper_accepts_the_compile_minus_the_re2_precheck() {
        let mut precheck_refused = Vec::new();
        for pat in [
            "che.*",
            "^a$",
            "a|b",
            "(a)(b)",
            r"\d+",
            "[a-z]{2,4}",
            "",
            "a$b",
            "^",
            "$",
            "(?i)x",
            r"\p{Alphabetic}",
            r"\p{Nonsense}",
            "a{2,1}",
            "[",
            "*",
            r"(?P<n>a)",
            // Added with the pre-check, so the list carries a member of
            // each rule family it decides rather than only the property
            // one that happened to be here.
            "a**",
            "a{1001}",
            "(?x)a",
            "[[:foo:]]",
            r"\u{263A}",
            // ...and the CONTROL that keeps this from passing under a
            // pre-check that claimed everything: an in-table property
            // name, which both the crate and the reference accept.
            r"\p{L}",
        ] {
            let via_escaper = anchored_regex_sql(pat).is_ok();
            let compiles = regex::Regex::new(&format!("^(?:{pat})$")).is_ok();
            let refused = pulsus_re2::re2_definitely_rejects(pat);
            if refused {
                precheck_refused.push(pat);
            }
            assert_eq!(
                via_escaper,
                compiles && !refused,
                "{pat:?}: the escaper is the anchored compile minus the RE2 pre-check \
                 (compiles={compiles}, pre-check refuses={refused})"
            );
        }
        assert_eq!(
            precheck_refused.len(),
            7,
            "the pre-check must actually fire on this list, or the relation above is the old \
             equality wearing a new name: {precheck_refused:?}"
        );
    }

    /// Issue #57 re-audit AC-A1: the fallback code-point cut is exactly
    /// one quarter of the byte ceiling — a worst-case 4-byte UTF-8 code
    /// point at that cut still lands exactly at the byte ceiling, never
    /// past it. (Moved here with the constants from the deleted
    /// `search_sql.rs`, #602 part 2c-2b-2.)
    #[test]
    fn cp_fallback_is_exactly_one_quarter_of_the_byte_cap() {
        assert_eq!(TRACE_STR_COL_CP_FALLBACK, TRACE_STR_COL_CAP / 4);
        assert_eq!(TRACE_STR_COL_CAP, 8192);
        assert_eq!(TRACE_STR_COL_CP_FALLBACK, 2048);
    }

    /// Issue #184 AC-Δ1d: the cap expression has a SINGLE source of
    /// truth — `substringUTF8` appears in this module's production,
    /// non-comment code exactly once, inside `byte_cap_expr`. (Moved
    /// with the helper from the deleted `search_sql.rs`.)
    #[test]
    fn the_cap_expression_has_a_single_source_of_truth() {
        let src = include_str!("sql_text.rs");
        let prod = src.split("#[cfg(test)]").next().unwrap();
        let code_only: String = prod
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(
            code_only.matches("substringUTF8").count(),
            1,
            "cap logic must have one definition (byte_cap_expr) — found an inline duplicate"
        );
    }
}
