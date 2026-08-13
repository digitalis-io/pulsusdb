//! Issue #388, Stage A: the accept/reject surface of a `| pattern "…"`
//! argument, measured as a matrix and checked in so it can be re-run.
//!
//! **What this file is.** The reference validates a `| pattern` argument
//! with a grammar of its own under `pkg/logql/log/pattern/`, run from
//! `NewPatternParser` (`pkg/logql/log/parser.go:456-470 @ v3.7.4`).
//! Unlike the `| logfmt` and `| json` expression sub-grammars, that call
//! happens inside `newLabelParserExpr` (`pkg/logql/syntax/ast.go:730-741
//! @ v3.7.4`), which **panics with a `ParseError` during `ParseExpr`** —
//! so a pattern rejection is a 400 in EVERY window, where a `| json`
//! rejection is a `Stage()` error and becomes a 200 once the window's end
//! is older than `query_ingesters_within`. Measured on the pinned
//! container over a window ending 24 h back: `| pattern "<a> <a>"` stays
//! 400 while `| json v="b-c"` and `| logfmt a="b.c"` are 200. **That is
//! why this file and `logql_json_expr_matrix.rs` cannot share a
//! harness**, and it is measured by
//! [`live_the_pattern_rule_is_window_independent`] rather than argued.
//!
//! **The two things this stage closes**, both measured in-process at
//! `5d91ef1` through `parse → plan → compile → run`:
//!
//! 1. `| pattern "<a> <a>"` over `one two three` answered `a="one"` here
//!    — the second capture silently discarded by #334's first-wins rule.
//!    The reference refuses the query.
//! 2. `| pattern "<1a> x"` over `9 x` answered `_1a="9"` here — **a label
//!    the user never wrote**, invented because our capture-name test was
//!    `[A-Za-z0-9_]+` where the reference's lexer is
//!    `'<' (alpha|'_') (alnum|'_')* '>'` (`pattern/lexer.rl:33 @
//!    v3.7.4`). The reference reads `<1a>` as literal text, leaving the
//!    pattern with no capture at all, and refuses it.
//!
//! **The tests, and what each is authority for.**
//!
//! - [`pulsus_agrees_with_the_captured_reference_verdicts`] — hermetic,
//!   the whole matrix, PulsusDB's verdict at the layer a user meets it:
//!   parse → plan → the pipeline compile that runs before any I/O.
//! - [`the_pre_388_rule_disagrees_wherever_the_reference_refuses_a_pattern`]
//!   — hermetic; the rule PulsusDB shipped at `5d91ef1` (`compile_pattern`,
//!   reproduced verbatim in [`pre_388`]) replayed over the same matrix,
//!   with every point classified CLOSED / INTRODUCED / UNMOVED so a
//!   point moving the wrong way names itself.
//! - [`the_rule_table_has_one_row_per_line_of_the_reference_enumeration`]
//!   — the committed `logql_pattern_expr_reference_error_sites.txt` is
//!   the literal output of one `git grep` over the reference; the rule
//!   table has exactly one row per line of it.
//! - [`the_sites_dataset_is_regenerated_not_retyped`] and its
//!   `#[ignore]`d generator — the committed
//!   `logql_pattern_expr_sites.tsv` is produced from the tables and the
//!   sweeps, never hand-edited.
//! - [`live_matrix_against_the_reference`] and
//!   [`live_the_pattern_rule_is_window_independent`] — gated on
//!   `PULSUSDB_LOGQL_DIFF_URL`.
//!
//! **Excluded by name.** `|>` / `!>` pattern line filters carry a
//! DIFFERENT rule set (`pattern.ParseLineFilter`, `pattern.go:36-51 @
//! v3.7.4`: no-capture allowed, named captures refused). Our grammar has
//! no such operator — [`the_line_filter_rule_set_is_out_of_reach`] pins
//! that, and reddens the day someone adds it.

use std::process::Command;

use pulsus_logql::parse;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::plan::{MetricNode, MetricNodeScc, Plan};
use pulsus_read::logql::{Direction, PlanCtx, QueryParams, QuerySpec, plan};

/// What a user sees: the query is served, or it is a 400.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Accept,
    Reject,
}

impl Verdict {
    fn tsv(self) -> &'static str {
        match self {
            Verdict::Accept => "accept",
            Verdict::Reject => "reject",
        }
    }
}

// ---------------------------------------------------------------------
// The reference's rule table. One row per line of
// `logql_pattern_expr_reference_error_sites.txt`, which is the literal
// output of the command that file's header records.
// ---------------------------------------------------------------------

/// How a line of the committed enumeration relates to an error a caller
/// can observe. Derived by READING the line (reading R1 in the
/// non-derivable table in `pattern_expr.rs`'s docs); no check computes it
/// from source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrClass {
    /// Produces an error a caller observes.
    Producer,
    /// Builds the error type; produces nothing on its own.
    Constructor,
    /// A sentinel error VALUE's declaration.
    ErrValueDecl,
}

/// Why a `Producer` row carries no probe. A CLOSED set: the two reasons
/// are different claims and are never collapsed into one excuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WhyNot {
    /// The reference reaches it, but only through a construct our
    /// grammar has no syntax for.
    ConstructAbsentFromOurGrammar,
}

struct RefRule {
    /// `file:line` in `pkg/logql/log/pattern/` at `v3.7.4`, exactly as
    /// the committed enumeration spells it.
    site: &'static str,
    class: ErrClass,
    /// `None` when the row carries probes; `Some(..)` when it carries
    /// none and says why.
    why_not: Option<WhyNot>,
    note: &'static str,
}

const REF_RULES: &[RefRule] = &[
    RefRule {
        site: "ast.go:20",
        class: ErrClass::Producer,
        why_not: None,
        note: "ErrNoCapture: no NAMED capture. `<_>` does not count (captures() skips isUnnamed)",
    },
    RefRule {
        site: "ast.go:30",
        class: ErrClass::Producer,
        why_not: None,
        note: "a repeated NAMED capture",
    },
    RefRule {
        site: "ast.go:44",
        class: ErrClass::Producer,
        why_not: None,
        note: "two ADJACENT capture nodes, named or unnamed",
    },
    RefRule {
        site: "ast.go:54",
        class: ErrClass::Producer,
        why_not: Some(WhyNot::ConstructAbsentFromOurGrammar),
        note: "validateNoNamedCaptures, called only from ParseLineFilter (pattern.go:47), whose \
               sole non-test caller is log/filter.go:859 -- the `|>`/`!>` line filter. Live \
               there: `{a=\"b\"} |> \"<a> foo\"` is 400 `named captures are not allowed: found \
               '<a>'`. Evidence about the reference, not about us",
    },
    RefRule {
        site: "lexer.go:32",
        class: ErrClass::Producer,
        why_not: None,
        note: "the yacc Error callback: the only reachable syntax error is the empty pattern, \
               since every byte lexes as an identifier or a literal rune",
    },
    RefRule {
        site: "parser.go:68",
        class: ErrClass::Constructor,
        why_not: None,
        note: "newParseError builds parseError; it produces nothing on its own",
    },
    RefRule {
        site: "pattern.go:9",
        class: ErrClass::ErrValueDecl,
        why_not: None,
        note: "ErrNoCapture's declaration",
    },
    RefRule {
        site: "pattern.go:10",
        class: ErrClass::ErrValueDecl,
        why_not: None,
        note: "ErrCaptureNotAllowed's declaration",
    },
    RefRule {
        site: "pattern.go:11",
        class: ErrClass::ErrValueDecl,
        why_not: None,
        note: "ErrInvalidExpr's declaration",
    },
];

// ---------------------------------------------------------------------
// Table A -- the pattern ARGUMENT, crossed with every position.
// ---------------------------------------------------------------------

/// Which reference rule refuses a pattern, or that it is served. The
/// value names the `REF_RULES` row, so a probe cannot be recorded
/// against a rule that is not in the enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    Accepted,
    /// `lexer.go:32` — the empty pattern.
    SyntaxEmpty,
    /// `ast.go:20`.
    NoCapture,
    /// `ast.go:44`.
    ConsecutiveCaptures,
    /// `ast.go:30`.
    DuplicateCapture,
}

impl Rule {
    fn site(self) -> &'static str {
        match self {
            Rule::Accepted => "-",
            Rule::SyntaxEmpty => "lexer.go:32",
            Rule::NoCapture => "ast.go:20",
            Rule::ConsecutiveCaptures => "ast.go:44",
            Rule::DuplicateCapture => "ast.go:30",
        }
    }

    fn reference(self) -> Verdict {
        match self {
            Rule::Accepted => Verdict::Accept,
            _ => Verdict::Reject,
        }
    }
}

/// One `| pattern` argument.
///
/// `quoted` is what a user types between the double quotes of
/// `| pattern "…"`; `decoded` is what the reference's `pattern.New`
/// therefore sees, and [`the_fixtures_two_pattern_columns_agree`] derives
/// the second from the first through our own lexer so a fixture typo
/// cannot masquerade as a divergence.
///
/// `pre` is PulsusDB's verdict at `5d91ef1` — the frozen baseline. It was
/// not typed from a reading: the first commit on this branch asserted the
/// real `parse → plan → compile` chain against this column with
/// production code untouched, and [`pre_388`] reproduces it hermetically
/// for ever after.
///
/// `reference_text` is the INNER error text the pinned container
/// returned, i.e. what follows `invalid pattern parser: ` in the response
/// body. Empty for an accepted pattern.
struct Pattern {
    name: &'static str,
    quoted: &'static str,
    decoded: &'static str,
    rule: Rule,
    pre: Verdict,
    reference_text: &'static str,
}

const DUP_A: &str = "duplicate capture name (a): invalid expression";
const NO_CAPTURE: &str = "at least one capture is required";
const SYNTAX_EMPTY: &str =
    "parse error at line 1, col 1: syntax error: unexpected $end, expecting IDENTIFIER or LITERAL";

const PATTERNS: &[Pattern] = &[
    // --- refused: a repeated NAMED capture (ast.go:26-33) -------------
    //     THE ISSUE. All three were served here at `5d91ef1`.
    Pattern {
        name: "dup_adjacent_by_one_literal",
        quoted: "<a> <a>",
        decoded: "<a> <a>",
        rule: Rule::DuplicateCapture,
        pre: Verdict::Accept,
        reference_text: DUP_A,
    },
    Pattern {
        name: "dup_far_apart",
        quoted: "<a> x <a>",
        decoded: "<a> x <a>",
        rule: Rule::DuplicateCapture,
        pre: Verdict::Accept,
        reference_text: DUP_A,
    },
    // `isUnnamed` is `len(c) == 1 && c[0] == '_'` (`ast.go:83-85`), so
    // `<__>` is a NAMED capture and repeating it is a duplicate. A
    // `starts_with('_')` or `trim` spelling of the unnamed test gets this
    // wrong in a way no obvious probe catches.
    Pattern {
        name: "dup_double_underscore",
        quoted: "<__> x <__>",
        decoded: "<__> x <__>",
        rule: Rule::DuplicateCapture,
        pre: Verdict::Accept,
        reference_text: "duplicate capture name (__): invalid expression",
    },
    // --- refused: two ADJACENT captures (ast.go:37-49) ----------------
    Pattern {
        name: "consecutive_two_named",
        quoted: "<a><b>",
        decoded: "<a><b>",
        rule: Rule::ConsecutiveCaptures,
        pre: Verdict::Reject,
        reference_text: "found consecutive capture '<a><b>': invalid expression",
    },
    // RULE ORDER IS OBSERVABLE, and this is the pair that shows it:
    // `<a><a>` is CONSECUTIVE (checked second) and `<_><_>` is
    // NO-CAPTURE (checked first), not the other way round.
    Pattern {
        name: "consecutive_same_name",
        quoted: "<a><a>",
        decoded: "<a><a>",
        rule: Rule::ConsecutiveCaptures,
        pre: Verdict::Reject,
        reference_text: "found consecutive capture '<a><a>': invalid expression",
    },
    // --- refused: no NAMED capture (ast.go:18-21) ---------------------
    Pattern {
        name: "no_capture_two_unnamed_adjacent",
        quoted: "<_><_>",
        decoded: "<_><_>",
        rule: Rule::NoCapture,
        pre: Verdict::Reject,
        reference_text: NO_CAPTURE,
    },
    Pattern {
        name: "no_capture_two_unnamed_separated",
        quoted: "<_> <_>",
        decoded: "<_> <_>",
        rule: Rule::NoCapture,
        pre: Verdict::Reject,
        reference_text: NO_CAPTURE,
    },
    Pattern {
        name: "no_capture_literal_only",
        quoted: "foo",
        decoded: "foo",
        rule: Rule::NoCapture,
        pre: Verdict::Reject,
        reference_text: NO_CAPTURE,
    },
    // --- refused: the IDENTIFIER CHARSET (lexer.rl:33) ----------------
    //     `<1a>` is not an identifier, so it is literal text and the
    //     pattern has no capture. Two of these were served here and
    //     INVENTED a label.
    Pattern {
        name: "charset_digit_leading",
        quoted: "<1a> x",
        decoded: "<1a> x",
        rule: Rule::NoCapture,
        pre: Verdict::Accept,
        reference_text: NO_CAPTURE,
    },
    Pattern {
        name: "charset_digit_only",
        quoted: "<1> x",
        decoded: "<1> x",
        rule: Rule::NoCapture,
        pre: Verdict::Accept,
        reference_text: NO_CAPTURE,
    },
    Pattern {
        name: "charset_non_ascii",
        quoted: "<é> x",
        decoded: "<é> x",
        rule: Rule::NoCapture,
        pre: Verdict::Reject,
        reference_text: NO_CAPTURE,
    },
    Pattern {
        name: "charset_hyphen",
        quoted: "<a-b> x",
        decoded: "<a-b> x",
        rule: Rule::NoCapture,
        pre: Verdict::Reject,
        reference_text: NO_CAPTURE,
    },
    Pattern {
        name: "charset_space",
        quoted: "<a b> x",
        decoded: "<a b> x",
        rule: Rule::NoCapture,
        pre: Verdict::Reject,
        reference_text: NO_CAPTURE,
    },
    Pattern {
        name: "charset_empty_name",
        quoted: "<> x",
        decoded: "<> x",
        rule: Rule::NoCapture,
        pre: Verdict::Reject,
        reference_text: NO_CAPTURE,
    },
    // --- refused: the empty pattern (expr.y:32-35) --------------------
    Pattern {
        name: "empty",
        quoted: "",
        decoded: "",
        rule: Rule::SyntaxEmpty,
        pre: Verdict::Reject,
        reference_text: SYNTAX_EMPTY,
    },
    // --- served -------------------------------------------------------
    Pattern {
        name: "two_distinct_captures",
        quoted: "<a> <b>",
        decoded: "<a> <b>",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
    Pattern {
        name: "underscore_leading_name",
        quoted: "<_a> x",
        decoded: "<_a> x",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
    Pattern {
        name: "digit_inside_name",
        quoted: "<a1> x",
        decoded: "<a1> x",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
    Pattern {
        name: "upper_case_name",
        quoted: "<A> x",
        decoded: "<A> x",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
    Pattern {
        name: "trailing_underscore_name",
        quoted: "<a_> x",
        decoded: "<a_> x",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
    // Served on BOTH sides, and the one row where the charset change
    // moves an extracted LABEL rather than a verdict: `<1b>` is literal
    // text at the reference, so only `a` is extracted, while `5d91ef1`
    // extracted `a` and `_1b`.
    Pattern {
        name: "capture_then_literal_angle_digit",
        quoted: "<a> <1b>",
        decoded: "<a> <1b>",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
    // Capture names are CASE SENSITIVE: `<A>` and `<a>` are distinct.
    Pattern {
        name: "case_distinguishes_names",
        quoted: "<A> x <a>",
        decoded: "<A> x <a>",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
    // An UNNAMED capture never enters the duplicate set, so it may repeat
    // and may sit beside a named one.
    Pattern {
        name: "unnamed_then_named",
        quoted: "<_> x <a>",
        decoded: "<_> x <a>",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
    Pattern {
        name: "named_then_unnamed",
        quoted: "<a> x <_>",
        decoded: "<a> x <_>",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
    // `<__>` (named) beside `<_>` (unnamed): served, where `<__> x <__>`
    // above is not. The pair is what makes `isUnnamed`'s length test
    // observable.
    Pattern {
        name: "double_underscore_then_unnamed",
        quoted: "<__> x <_>",
        decoded: "<__> x <_>",
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: "",
    },
];

// ---------------------------------------------------------------------
// Positions.
// ---------------------------------------------------------------------

/// Where the `| pattern` sits. `{Q}` is the pattern's `quoted` text,
/// dropped into `| pattern "{Q}"`.
///
/// Every position is a `query_range`-shaped query, so between them they
/// drive `exec.rs:612` (streams), `exec.rs:906` (metric, each leaf of a
/// binary plan), `plan.rs`'s variant validation and `variants.rs:509`.
/// They do NOT drive `exec.rs:2290` (`detected_fields`) or `:2576`
/// (`tail`).
///
/// **Unlike the logfmt and json matrices, every position AGREES** —
/// including `variants_common_side`, where a `Stage()` error is swallowed
/// behind `SelectSamples` and answered as a 200. A pattern rejection is
/// raised during `ParseExpr`, before any of that. Measured on the pinned
/// container: `variants(count_over_time({service_name="m"} [5m])) of
/// ({service_name="m"} | pattern "<a> <a>" [5m])` is **400**, where the
/// same shape with `| json v="b-c"` is 200.
struct Position {
    name: &'static str,
    template: &'static str,
}

const POSITIONS: &[Position] = &[
    Position {
        name: "streams",
        template: r#"{service_name="m"} | pattern "{Q}""#,
    },
    Position {
        name: "streams_second_stage",
        template: r#"{service_name="m"} | logfmt | pattern "{Q}""#,
    },
    Position {
        name: "streams_after_line_filter",
        template: r#"{service_name="m"} |= "x" | pattern "{Q}""#,
    },
    Position {
        name: "count_over_time",
        template: r#"count_over_time({service_name="m"} | pattern "{Q}" [5m])"#,
    },
    Position {
        name: "sum_by",
        template: r#"sum by (a) (count_over_time({service_name="m"} | pattern "{Q}" [5m]))"#,
    },
    Position {
        name: "unwrap",
        template: r#"sum_over_time({service_name="m"} | pattern "{Q}" | unwrap x [5m])"#,
    },
    Position {
        name: "topk",
        template: r#"topk(3, count_over_time({service_name="m"} | pattern "{Q}" [5m]))"#,
    },
    Position {
        name: "label_replace",
        template: r#"label_replace(count_over_time({service_name="m"} | pattern "{Q}" [5m]), "d", "$1", "a", "(.*)")"#,
    },
    Position {
        name: "binary_lhs",
        template: r#"count_over_time({service_name="m"} | pattern "{Q}" [5m]) + count_over_time({service_name="m"} [5m])"#,
    },
    Position {
        name: "binary_rhs",
        template: r#"count_over_time({service_name="m"} [5m]) + count_over_time({service_name="m"} | pattern "{Q}" [5m])"#,
    },
    Position {
        name: "variants_variant_side",
        template: r#"variants(count_over_time({service_name="m"} | pattern "{Q}" [5m])) of ({service_name="m"} [5m])"#,
    },
    Position {
        name: "variants_common_side",
        template: r#"variants(count_over_time({service_name="m"} [5m])) of ({service_name="m"} | pattern "{Q}" [5m])"#,
    },
];

/// One matrix point.
struct Point {
    label: String,
    query: String,
    rule: Rule,
    pre: Verdict,
    reference_text: &'static str,
}

impl Point {
    /// What the pinned container answers, from the captured table. A
    /// pattern rejection is a `ParseExpr` error, so it does not vary by
    /// position and does not vary by window.
    fn reference(&self) -> Verdict {
        self.rule.reference()
    }

    /// What PulsusDB answers.
    ///
    /// **BASELINE COMMIT: this is the `5d91ef1` column.** The next commit
    /// lands `pattern_expr.rs` and moves it to `self.rule.reference()`,
    /// so the reviewer sees every verdict that moved in one diff.
    fn pulsus(&self) -> Verdict {
        self.pre
    }
}

fn matrix() -> Vec<Point> {
    let mut out = Vec::new();
    for position in POSITIONS {
        for p in PATTERNS {
            out.push(Point {
                label: format!("{}/{}", position.name, p.name),
                query: position.template.replace("{Q}", p.quoted),
                rule: p.rule,
                pre: p.pre,
                reference_text: p.reference_text,
            });
        }
    }
    out
}

fn ctx() -> PlanCtx<'static> {
    PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

fn params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: 1_782_907_200_000_000_000,
            end_ns: 1_782_928_800_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

/// PulsusDB's verdict **at the layer a user meets it**: parse, then plan,
/// then the pipeline compile `exec` performs before any I/O. Asking the
/// parser alone would be a weaker layer and would report every pattern as
/// accepted, which is exactly the gap this issue closes.
///
/// Mirrors the compile SITES one by one, the same composition
/// `logql_logfmt_expr_matrix.rs:857` uses; that file's
/// `the_compile_sites_are_enumerated_from_the_callers_of_the_compiler`
/// is the authority for the site list and reddens on a new one.
fn pulsus_verdict(query: &str) -> (Verdict, String) {
    let expr = match parse(query) {
        Ok(e) => e,
        Err(e) => return (Verdict::Reject, format!("parse: {e}")),
    };
    let planned = match plan(&expr, &params(), &ctx()) {
        Ok(p) => p,
        Err(e) => return (Verdict::Reject, format!("plan: {e}")),
    };
    let mut pipelines: Vec<Vec<pulsus_logql::Stage>> = Vec::new();
    match &planned {
        Plan::Streams(sp) => pipelines.push(sp.pipeline.clone()),
        Plan::Metric(mp) => pipelines.extend(mp.client.iter().map(|c| c.pipeline.clone())),
        Plan::MetricBinary(node) => {
            for leaf in node.leaves() {
                pipelines.extend(leaf.client.iter().map(|c| c.pipeline.clone()));
            }
            collect_variant_pipelines(node, &mut pipelines);
        }
    }
    for stages in &pipelines {
        if let Err(e) = CompiledPipeline::compile(stages) {
            return (Verdict::Reject, format!("compile: {e}"));
        }
    }
    (Verdict::Accept, String::new())
}

/// Every `common ++ tail` a `MetricNode::Variants` would have
/// `VariantArena::build` compile.
fn collect_variant_pipelines(node: &MetricNode, out: &mut Vec<Vec<pulsus_logql::Stage>>) {
    pulsus_logql::walk::preorder::<MetricNodeScc>(node, |n| {
        if let MetricNode::Variants { scan, variants, .. } = n {
            let common: Vec<pulsus_logql::Stage> = scan
                .client
                .as_ref()
                .map(|c| c.pipeline.clone())
                .unwrap_or_default();
            for spec in variants {
                let mut full = common.clone();
                full.extend(spec.client().pipeline.iter().cloned());
                out.push(full);
            }
        }
    });
}

// ---------------------------------------------------------------------
// The rule PulsusDB shipped at `5d91ef1`, replayed.
// ---------------------------------------------------------------------

/// `compile_pattern` as it stood at `5d91ef1` (`pipeline.rs:2631-2674`),
/// reproduced verbatim apart from returning a [`Verdict`] instead of a
/// `PipelineError`. This is the baseline freeze the issue asked for: not
/// a committed scoreboard file (LogQL has none) but the pre-change rule
/// as a replayable function, the form #247 established.
///
/// It is tied to reality by the branch's FIRST commit, which asserted the
/// real `parse → plan → compile` chain against the `pre` column with
/// production code untouched.
fn pre_388(pattern: &str) -> Verdict {
    let mut tokens: Vec<(u8, String)> = Vec::new(); // (0 literal, 1 capture, 2 discard)
    let mut rest = pattern;
    let mut captures = 0usize;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('<') {
            if let Some(close) = after.find('>') {
                let name = &after[..close];
                let is_capture_name =
                    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                if is_capture_name {
                    let prev_is_capture = matches!(tokens.last(), Some((1 | 2, _)));
                    if prev_is_capture {
                        return Verdict::Reject; // consecutive captures
                    }
                    if name == "_" {
                        tokens.push((2, String::new()));
                    } else {
                        tokens.push((1, name.to_string()));
                        captures += 1;
                    }
                    rest = &after[close + 1..];
                    continue;
                }
            } else {
                return Verdict::Reject; // unclosed '<'
            }
        }
        let next = rest[1..].find('<').map(|off| off + 1).unwrap_or(rest.len());
        let (lit, tail) = rest.split_at(next);
        match tokens.last_mut() {
            Some((0, existing)) => existing.push_str(lit),
            _ => tokens.push((0, lit.to_string())),
        }
        rest = tail;
    }
    if captures == 0 {
        return Verdict::Reject; // at least one named capture is required
    }
    Verdict::Accept
}

// ---------------------------------------------------------------------
// Hermetic tests.
// ---------------------------------------------------------------------

/// **The two pattern columns agree.** `decoded` is what the reference's
/// `pattern.New` sees; this derives it from `quoted` through our own
/// lexer, so a mis-escaped fixture row shows up here rather than as a
/// phantom agreement about the reference.
#[test]
fn the_fixtures_two_pattern_columns_agree() {
    for p in PATTERNS {
        let query = format!(r#"{{service_name="m"}} | pattern "{}""#, p.quoted);
        let expr = parse(&query).unwrap_or_else(|err| panic!("{}: {query}: {err}", p.name));
        let pulsus_logql::Expr::Log(log) = &expr else {
            panic!("{}: not a log query", p.name);
        };
        let mut found = None;
        for stage in &log.pipeline {
            if let pulsus_logql::Stage::Parser(pulsus_logql::ParserStage::Pattern(pat)) = stage {
                found = Some(pat.clone());
            }
        }
        assert_eq!(
            found.as_deref(),
            Some(p.decoded),
            "{}: `| pattern \"{}\"` decodes to {:?}, but the fixture says {:?}",
            p.name,
            p.quoted,
            found,
            p.decoded
        );
    }
}

/// **PulsusDB's verdict is what the table records, point for point.**
///
/// The `pre` column is the frozen baseline and the `rule` column is the
/// reference's; which of the two this asserts against is the whole change
/// this issue makes, so the assertion names both.
#[test]
fn pulsus_agrees_with_the_captured_reference_verdicts() {
    let points = matrix();
    let mut wrong = Vec::new();
    for p in &points {
        let (ours, detail) = pulsus_verdict(&p.query);
        if ours != p.pulsus() {
            wrong.push(format!(
                "{} {}\n    table={:?} chain={:?} {detail}",
                p.label,
                p.query,
                p.pulsus(),
                ours
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} points disagree with the committed table:\n{}",
        wrong.len(),
        points.len(),
        wrong.join("\n")
    );
}

/// **The frozen baseline is re-derivable, not asserted.** Every `pre`
/// value is reproduced by [`pre_388`], the pre-change rule replayed.
#[test]
fn the_pre_388_column_is_reproduced_by_the_replayed_rule() {
    for p in PATTERNS {
        assert_eq!(
            pre_388(p.decoded),
            p.pre,
            "{}: the replayed `5d91ef1` rule disagrees with the frozen `pre` column for {:?}",
            p.name,
            p.decoded
        );
    }
}

/// **The gap this issue closes, enumerated at the baseline.** With
/// `Point::pulsus` still on the `5d91ef1` column, every point where we
/// and the reference disagree is listed by name. The next commit turns
/// this into the direction gate: the same three verdicts, classified into
/// buckets that must partition the matrix, with INTRODUCED asserted
/// empty.
#[test]
fn the_baseline_disagrees_with_the_reference_exactly_where_the_issue_says() {
    let points = matrix();
    let open: Vec<&str> = points
        .iter()
        .filter(|p| p.pulsus() != p.reference())
        .map(|p| p.label.as_str())
        .collect();
    let expected: Vec<&str> = points
        .iter()
        .filter(|p| p.pre != p.rule.reference())
        .map(|p| p.label.as_str())
        .collect();
    assert_eq!(open, expected);
    // Five patterns move, at every position: the three duplicate-capture
    // shapes and the two the identifier charset lets us invent a label
    // for. Computed from the table, never restated as a literal.
    let names: std::collections::BTreeSet<&str> = PATTERNS
        .iter()
        .filter(|p| p.pre != p.rule.reference())
        .map(|p| p.name)
        .collect();
    assert_eq!(
        names.into_iter().collect::<Vec<_>>(),
        vec![
            "charset_digit_leading",
            "charset_digit_only",
            "dup_adjacent_by_one_literal",
            "dup_double_underscore",
            "dup_far_apart",
        ]
    );
    eprintln!(
        "baseline: {} of {} matrix points disagree with the pinned container",
        open.len(),
        points.len()
    );
}

/// **The rule table has exactly one row per line of the committed
/// reference enumeration.** The file is the literal output of one
/// `git grep` whose scope is the non-test files of
/// `pkg/logql/log/pattern/` at `v3.7.4`; its header states what that
/// scope does NOT cover. Deleting a table row, or adding one, reddens.
#[test]
fn the_rule_table_has_one_row_per_line_of_the_reference_enumeration() {
    let sites = reference_error_sites();
    let rows: Vec<&str> = REF_RULES.iter().map(|r| r.site).collect();
    assert_eq!(
        rows, sites,
        "the rule table and the committed reference enumeration have drifted"
    );
}

/// The `file:line` of every non-comment line of the committed
/// enumeration, in file order.
fn reference_error_sites() -> Vec<String> {
    let raw = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/logql_pattern_expr_reference_error_sites.txt"),
    )
    .expect("the committed reference enumeration");
    raw.lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| {
            // `v3.7.4:pkg/logql/log/pattern/ast.go:20:\t...` -> `ast.go:20`
            let mut it = l.splitn(4, ':');
            let _tag = it.next();
            let path = it.next().expect("path");
            let line = it.next().expect("line");
            let file = path.rsplit('/').next().expect("file name");
            format!("{file}:{line}")
        })
        .collect()
}

/// **Every modelled rule is tied to a probe, and every probe names
/// exactly one rule** — the uniqueness property, not a `contains`.
///
/// A `Producer` row with `why_not = None` must be named by AT LEAST ONE
/// rejecting probe; a `Producer` row with `why_not = Some(..)` by
/// EXACTLY ZERO, and it carries its reason. Non-producer rows
/// (`Constructor`, `ErrValueDecl`) carry no probes by construction.
#[test]
fn every_reachable_rule_has_a_probe_and_every_probe_names_one_rule() {
    for r in REF_RULES {
        let probes: Vec<&str> = PATTERNS
            .iter()
            .filter(|p| p.rule.site() == r.site)
            .map(|p| p.name)
            .collect();
        match (r.class, r.why_not) {
            (ErrClass::Producer, None) => assert!(
                !probes.is_empty(),
                "{}: a modelled rule with no probe -- {}",
                r.site,
                r.note
            ),
            (ErrClass::Producer, Some(_)) => assert!(
                probes.is_empty(),
                "{}: a rule declared unprobeable carries probes {probes:?}",
                r.site
            ),
            (ErrClass::Constructor | ErrClass::ErrValueDecl, _) => assert!(
                probes.is_empty(),
                "{}: a non-producer row carries probes {probes:?}",
                r.site
            ),
        }
    }
    for p in PATTERNS {
        let named: Vec<&str> = REF_RULES
            .iter()
            .filter(|r| r.site == p.rule.site())
            .map(|r| r.site)
            .collect();
        match p.rule {
            Rule::Accepted => assert!(
                named.is_empty(),
                "{}: an accepted pattern names rule(s) {named:?}",
                p.name
            ),
            _ => assert_eq!(
                named.len(),
                1,
                "{}: a rejecting probe must name exactly one rule, it names {named:?}",
                p.name
            ),
        }
    }
    // Every variant of the closed `why_not` set appears, asserted over
    // the table's own values rather than over the type's variants.
    let why_nots: Vec<WhyNot> = REF_RULES.iter().filter_map(|r| r.why_not).collect();
    assert_eq!(
        why_nots,
        vec![WhyNot::ConstructAbsentFromOurGrammar],
        "the closed why_not set and the table's values have drifted"
    );
    // And every `Rule` variant is exercised.
    for want in [
        Rule::Accepted,
        Rule::SyntaxEmpty,
        Rule::NoCapture,
        Rule::ConsecutiveCaptures,
        Rule::DuplicateCapture,
    ] {
        assert!(
            PATTERNS.iter().any(|p| p.rule == want),
            "{want:?} is modelled but no probe produces it"
        );
    }
}

/// **`ParseLineFilter`'s rule set is out of reach, and this reddens the
/// day it is not.** `ast.go:54` is the one `Producer` in the pattern
/// package carrying `why_not = ConstructAbsentFromOurGrammar`; the fact
/// that makes it so is that our grammar has no `|>` / `!>` operator.
#[test]
fn the_line_filter_rule_set_is_out_of_reach() {
    let err = parse(r#"{a="b"} |> "<a> foo""#)
        .expect_err("`|>` is not a PulsusDB line-filter operator; `ast.go:54` becomes reachable");
    let msg = err.to_string();
    assert!(
        msg.contains("expected a pipeline stage"),
        "`|>` is refused for an unexpected reason, which may not be the same fact: {msg}"
    );
}

// ---------------------------------------------------------------------
// The ALL dataset (`logql_pattern_expr_sites.tsv`).
// ---------------------------------------------------------------------

/// One tracked-tree site holding a `| pattern "…"` argument.
struct Site {
    /// Which sweep reaches it: `A` a literal argument, `B` one built by
    /// `format!`/`replace`, `C` a built stage KEYWORD.
    sweep: &'static str,
    /// `Literal` a real query argument; `DocProse` a hit inside a doc
    /// comment that never reaches a parser — kept in the dataset with its
    /// verdicts rather than filtered out, so the sweep's output and this
    /// file agree line for line.
    form: &'static str,
    site: &'static str,
    /// The literal argument, or each element of a generator's domain.
    pattern: &'static str,
    /// The verdict at `5d91ef1` and the verdict now. **They are equal on
    /// every row**, which is the "0 of 15 flips" claim in a form that can
    /// go red.
    pre: Verdict,
    post: Verdict,
    note: &'static str,
}

/// **The tracked-tree inventory, from three sweeps with disjoint
/// construction forms.** Committed so a flip cannot be silent, and
/// re-derived by [`sweep_a_still_finds_exactly_the_committed_sites`].
///
/// Today: sweep A finds 15 `| pattern "…"` arguments (`git grep`-style
/// scan over `git ls-files`, keyword case-insensitive because LogQL
/// keywords fold); **sweep B** — `git grep -n -E '(format!|replace)\(.*(\|
/// *json|\| *pattern)' -- crates e2e xtask test` — finds 7 sites, all
/// seven in the `| json` domain and none in this one; **sweep C** —
/// `git grep -n -E 'format!\([^)]*\|\s*\{\}' -- crates e2e xtask test` —
/// finds 2 hits, `ast_walk_characterization.rs:887` (which renders a
/// `LabelFilterExpr`, a TYPE fact, so it cannot be a parser stage) and
/// one in `pulsus-traceql`. **0 of 15 flips.**
const SITES: &[Site] = &[
    Site {
        sweep: "A",
        form: "DocProse",
        site: "crates/pulsus-logql/src/ast.rs:249",
        pattern: "<p>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "`ParserStage::Pattern`'s own doc comment",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-logql/tests/case_folding.rs:277",
        pattern: "<a>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "the `| PATTERN` spelling -- why the scan folds the keyword",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-logql/tests/case_folding.rs:278",
        pattern: "<a>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-logql/tests/snapshots.rs:946",
        pattern: "<method> <path> <_> <status>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "",
    },
    Site {
        sweep: "A",
        form: "DocProse",
        site: "crates/pulsus-read/src/logql/charge.rs:548",
        pattern: ", ",
        pre: Verdict::Reject,
        post: Verdict::Reject,
        note: "prose listing the parser stages; the scan reads the text after the closing \
               backtick as an argument. Not a query -- kept so the sweep's output and this \
               table agree line for line",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/src/logql/plan.rs:4710",
        pattern: "<x> y",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/tests/logql_pipeline_alloc.rs:198",
        pattern: "<method> <path> <status> <took>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "the allocation band's pattern -- its constants must not move",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/tests/logql_pipeline_golden.rs:991",
        pattern: "<method> <_> <status> took <took>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "an UNNAMED capture beside named ones",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/tests/logql_pipeline_golden.rs:1010",
        pattern: "level=<level> msg",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "a literal `=` inside the pattern text",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/tests/logqltest/corpus/b1_parsers_filters.test:253",
        pattern: "<_> <method> <path> <status>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "a LEADING unnamed capture",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/tests/logqltest/corpus/b1_parsers_filters.test:264",
        pattern: "<level> <subsys> <event>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/tests/logqltest/corpus/b1_parsers_filters.test:437",
        pattern: "<a><b>",
        pre: Verdict::Reject,
        post: Verdict::Reject,
        note: "already refused on both sides (consecutive captures)",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/tests/logqltest/corpus/b21_key_collisions.test:190",
        pattern: "<a>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/tests/logqltest/corpus/b2_formatters.test:198",
        pattern: "<method> <path> <status>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "",
    },
    Site {
        sweep: "A",
        form: "Literal",
        site: "crates/pulsus-read/tests/logqltest/corpus/differential_parsers_filters.test:65",
        pattern: "<method> <path> <status> <took>",
        pre: Verdict::Accept,
        post: Verdict::Accept,
        note: "",
    },
];

/// **Every committed site's argument still gets the verdict the dataset
/// records**, replayed through the real `parse → plan → compile` chain
/// rather than through the parser alone — and its `pre` verdict is
/// reproduced by the replayed `5d91ef1` rule, so "nothing flips" is a
/// per-row identity rather than a count.
#[test]
fn every_tracked_pattern_argument_keeps_its_verdict() {
    for s in SITES {
        let query = format!(r#"{{service_name="m"}} | pattern "{}""#, s.pattern);
        let (ours, detail) = pulsus_verdict(&query);
        assert_eq!(ours, s.post, "{}: {:?}: {detail}", s.site, s.pattern);
        assert_eq!(
            pre_388(s.pattern),
            s.pre,
            "{}: the replayed `5d91ef1` rule disagrees with the recorded `pre`",
            s.site
        );
        assert_eq!(
            s.pre, s.post,
            "{}: {:?} FLIPS under #388 -- a committed artefact may not change verdict silently",
            s.site, s.pattern
        );
    }
}

/// **The committed inventory is what the sweep finds today.** A hit the
/// file does not carry reddens, and a file row no sweep produces reddens.
///
/// **What this cannot see, stated:** a pattern argument that is neither a
/// literal, nor built by `format!`/`replace`, nor stored in one of the
/// corpus or fixture files
/// [`every_corpus_pattern_argument_keeps_its_verdict`] walks — one
/// arriving over the network in a suite this file does not touch, for
/// instance. That set is not reachable by any command here and is not
/// claimed empty.
#[test]
fn sweep_a_still_finds_exactly_the_committed_sites() {
    let root = repo_root();
    let mut found: Vec<(String, String)> = Vec::new();
    for file in tracked_files(&root) {
        // The matrix's own tables are the fixture, not a site.
        if file.ends_with("logql_pattern_expr_matrix.rs")
            || file.ends_with("logql_pattern_expr_sites.tsv")
        {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(root.join(&file)) else {
            continue;
        };
        for (i, line) in text.lines().enumerate() {
            for arg in pattern_arguments(line) {
                found.push((format!("{file}:{}", i + 1), arg));
            }
        }
    }
    found.sort();
    let mut want: Vec<(String, String)> = SITES
        .iter()
        .filter(|s| s.sweep == "A")
        .map(|s| (s.site.to_string(), s.pattern.to_string()))
        .collect();
    want.sort();
    assert_eq!(
        found, want,
        "the tracked-tree inventory of `| pattern \"…\"` arguments has moved: add the site to \
         `SITES` and state its verdict"
    );
}

/// Every `| pattern "…"` / `| pattern \`…\`` argument on one line.
///
/// The keyword is matched CASE-INSENSITIVELY, because LogQL keywords fold
/// (`| PATTERN "<a>"` is a valid query and `case_folding.rs:277` is one)
/// — a case-sensitive scan silently loses that site.
fn pattern_arguments(line: &str) -> Vec<String> {
    let lower = line.to_ascii_lowercase();
    let mut out = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    while let Some(off) = lower[i..].find("pattern") {
        let mut j = i + off + "pattern".len();
        // The stage keyword must be preceded by a `|`, with only spaces
        // between: `| pattern`, `|pattern`.
        let before = line[..i + off].trim_end();
        if !before.ends_with('|') {
            i = j;
            continue;
        }
        while j < bytes.len() && bytes[j] == b' ' {
            j += 1;
        }
        let Some(&delim) = bytes.get(j) else { break };
        if delim != b'"' && delim != b'`' {
            i = j;
            continue;
        }
        let mut k = j + 1;
        let mut arg = String::new();
        while k < bytes.len() {
            if bytes[k] == b'\\' && delim == b'"' && k + 1 < bytes.len() {
                arg.push(line[k..].chars().next().expect("char"));
                arg.push(line[k + 1..].chars().next().expect("char"));
                k += 2;
                continue;
            }
            if bytes[k] == delim {
                break;
            }
            let c = line[k..].chars().next().expect("char");
            arg.push(c);
            k += c.len_utf8();
        }
        if k >= bytes.len() {
            break;
        }
        out.push(arg);
        i = k + 1;
    }
    out
}

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn tracked_files(root: &std::path::Path) -> Vec<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files"])
        .output()
        .expect("git ls-files");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::to_string)
        .collect()
}

/// **Sweep D — the queries that live in DATA, not source.** Walks the
/// `logqltest` corpus and the logs fixtures at run time and compiles
/// every `| pattern` argument it finds, because a grep cannot see a query
/// a harness reads from a file.
#[test]
fn every_corpus_pattern_argument_keeps_its_verdict() {
    let root = repo_root();
    let mut checked = 0usize;
    for rel in tracked_files(&root) {
        if !(rel.contains("logqltest/corpus/") || rel.contains("test/fixtures/logs/")) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(root.join(&rel)) else {
            continue;
        };
        for (i, line) in text.lines().enumerate() {
            for arg in pattern_arguments(line) {
                let query = format!(r#"{{service_name="m"}} | pattern "{arg}""#);
                let (ours, detail) = pulsus_verdict(&query);
                // `b1_parsers_filters.test`'s row is an `eval_fail`; every
                // other corpus argument is an `eval`.
                let want = if line.starts_with("eval_fail") {
                    Verdict::Reject
                } else {
                    Verdict::Accept
                };
                assert_eq!(ours, want, "{rel}:{}: {arg:?}: {detail}", i + 1);
                checked += 1;
            }
        }
    }
    assert!(checked > 0, "sweep D found no corpus pattern argument");
    eprintln!("sweep D compiled {checked} corpus/fixture pattern arguments");
}

/// The committed TSV, rendered from the tables above and the sweeps.
fn render_sites_tsv() -> String {
    let mut out = String::new();
    out.push_str(
        "# issue #388 Stage A -- the ONE authoritative dataset for the `| pattern` sub-grammar.\n\
         # GENERATED by `cargo test -p pulsus-read --test logql_pattern_expr_matrix -- \
         --ignored regenerate_the_sites_dataset`; never hand-edited.\n\
         # row=RefRule: one per line of logql_pattern_expr_reference_error_sites.txt.\n\
         #   err_class in {Producer, Constructor, ErrValueDecl}; why_not in \
         {ConstructAbsentFromOurGrammar} on zero-probe Producers only.\n\
         # row=Probe:   one per matrix pattern. `rule` names exactly one RefRule, or `-` when \
         served.\n\
         #   reference_status/reference_text are CAPTURED constants, re-derived only when the \
         live leg runs.\n\
         # row=Site:    one per tracked-tree site holding a `| pattern` argument (sweeps A/B/C).\n\
         row\tid\terr_class\twhy_not\tform\texpression\tpre\tpost\tref_status\tref_text\tnote\n",
    );
    for r in REF_RULES {
        out.push_str(&format!(
            "RefRule\t{}\t{:?}\t{}\t-\t-\t-\t-\t-\t-\t{}\n",
            r.site,
            r.class,
            match r.why_not {
                Some(w) => format!("{w:?}"),
                None => "-".to_string(),
            },
            r.note
        ));
    }
    for p in PATTERNS {
        out.push_str(&format!(
            "Probe\t{}\t-\t-\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            p.rule.site(),
            p.name,
            p.decoded,
            p.pre.tsv(),
            p.rule.reference().tsv(),
            match p.rule {
                Rule::Accepted => "200",
                _ => "400",
            },
            if p.reference_text.is_empty() {
                "-"
            } else {
                p.reference_text
            },
            format_args!("{:?}", p.rule),
        ));
    }
    for s in SITES {
        out.push_str(&format!(
            "Site\t{}\t-\t-\t{}\t{}\t{}\t{}\t-\t-\t{}\n",
            s.site,
            s.form,
            s.pattern,
            s.pre.tsv(),
            s.post.tsv(),
            format_args!("sweep {} -- {}", s.sweep, s.note),
        ));
    }
    out
}

fn sites_tsv_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/logql_pattern_expr_sites.tsv")
}

/// **The dataset is generated, never retyped.** A "verbatim" capture that
/// is typed by hand gets silently paraphrased; this renders the file from
/// the tables and compares byte for byte.
#[test]
fn the_sites_dataset_is_regenerated_not_retyped() {
    let want = render_sites_tsv();
    let got = std::fs::read_to_string(sites_tsv_path()).expect("the committed sites dataset");
    assert_eq!(
        got, want,
        "`logql_pattern_expr_sites.tsv` has drifted from the tables that generate it -- re-run \
         the `regenerate_the_sites_dataset` generator rather than editing the file"
    );
}

#[test]
#[ignore = "regenerates a committed artefact"]
fn regenerate_the_sites_dataset() {
    std::fs::write(sites_tsv_path(), render_sites_tsv()).expect("write");
}

// ---------------------------------------------------------------------
// The live legs (gated on PULSUSDB_LOGQL_DIFF_URL). Status only.
// ---------------------------------------------------------------------

/// `back_s` seconds are subtracted from the window's end, so one function
/// serves both the ordinary leg and the stale-window one.
fn reference_status(base: &str, query: &str, back_s: u64) -> (u32, String) {
    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
        - back_s;
    let out = Command::new("curl")
        .args([
            "-s",
            "--max-time",
            "30",
            "-o",
            "/dev/stderr",
            "-w",
            "%{http_code}",
            "-G",
            &format!("{base}/loki/api/v1/query_range"),
            "--data-urlencode",
            &format!("query={query}"),
            "--data-urlencode",
            &format!("start={}", (now_s - 300) * 1_000_000_000),
            "--data-urlencode",
            &format!("end={}", now_s * 1_000_000_000),
            "--data-urlencode",
            "step=60s",
        ])
        .output()
        .expect("curl");
    let code: u32 = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("http status");
    (
        code,
        String::from_utf8_lossy(&out.stderr).trim().to_string(),
    )
}

/// **Re-measures every committed verdict AND its error text against the
/// pinned container.** The text half is what makes the `reference_text`
/// column a capture rather than a decoration: our own message for a
/// refused pattern is compared against the reference's, byte for byte,
/// after stripping the wrapper the reference puts around it.
#[test]
fn live_matrix_against_the_reference() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the live pattern matrix");
        return;
    };
    let points = matrix();
    let mut disagree = Vec::new();
    for p in &points {
        let (code, body) = reference_status(&base, &p.query, 0);
        let theirs = match code {
            200 => Verdict::Accept,
            400 => Verdict::Reject,
            other => panic!("unexpected status {other} for {:?}: {body}", p.query),
        };
        if theirs != p.reference() {
            disagree.push(format!(
                "{} {}\n    committed={:?} container={:?} {}",
                p.label,
                p.query,
                p.reference(),
                theirs,
                body.chars().take(200).collect::<String>()
            ));
            continue;
        }
        if theirs == Verdict::Reject {
            let want = format!("invalid pattern parser: {}", p.reference_text);
            if !body.contains(&want) {
                disagree.push(format!(
                    "{} {}\n    committed text {want:?} is not in {}",
                    p.label,
                    p.query,
                    body.chars().take(200).collect::<String>()
                ));
            }
        }
    }
    assert!(
        disagree.is_empty(),
        "{} of {} committed verdicts no longer match the pinned container:\n{}",
        disagree.len(),
        points.len(),
        disagree.join("\n")
    );
    eprintln!(
        "re-measured {} matrix points against the reference",
        points.len()
    );
}

/// **The pattern rule is WINDOW-INDEPENDENT, and the json one is not.**
/// This is the fact that forbids one harness for the two stages, so it is
/// measured here rather than asserted in a comment: over a window ending
/// 24 h back, a duplicate-capture pattern is still 400 while the two
/// `Stage()`-layer rejections beside it become 200.
#[test]
fn live_the_pattern_rule_is_window_independent() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the stale-window probe");
        return;
    };
    const DAY: u64 = 24 * 60 * 60;
    for p in PATTERNS {
        let query = format!(r#"{{service_name="m"}} | pattern "{}""#, p.quoted);
        let (code, body) = reference_status(&base, &query, DAY);
        let want = match p.rule.reference() {
            Verdict::Accept => 200,
            Verdict::Reject => 400,
        };
        assert_eq!(
            code,
            want,
            "{}: over a 24 h-stale window the reference answers {code}, not {want}: {}",
            p.name,
            body.chars().take(200).collect::<String>()
        );
    }
    // The control: the two Stage()-layer rules DO go quiet over the same
    // window, which is what makes the assertion above a discrimination
    // rather than a tautology.
    for stage in [r#"| json v="b-c""#, r#"| logfmt a="b.c""#] {
        let query = format!(r#"{{service_name="m"}} {stage}"#);
        let (code, body) = reference_status(&base, &query, DAY);
        assert_eq!(
            code,
            200,
            "{stage} is still refused over a stale window, so this probe no longer discriminates \
             between the ParseExpr and Stage() layers: {}",
            body.chars().take(200).collect::<String>()
        );
    }
}
