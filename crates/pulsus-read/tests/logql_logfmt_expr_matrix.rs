//! Issue #247: the accept/reject surface of a `| logfmt <id>="<expr>"`
//! extraction expression, measured as a matrix and checked in so it can
//! be re-run.
//!
//! **What this file is.** The reference validates a logfmt extraction in
//! TWO layers, and refuses at both. The extraction-**list** shape is a
//! `syntax.y` grammar error, refused by `ParseExpr` in every window
//! (`pkg/logql/syntax/syntax.y:318-321 @ v3.7.4`). The extraction
//! -**expression** body has a grammar of its own under
//! `pkg/logql/log/logfmt/`, run from `NewLogfmtExpressionParser`
//! (`pkg/logql/log/parser.go:506-529 @ v3.7.4`) at `Stage()` time and
//! surfaced as a 400. At `7980344` PulsusDB had no sub-grammar at all,
//! and its extraction list — faithful otherwise — ended silently at a
//! trailing comma instead of refusing it. This is the measurement of
//! both, as a fixture: twelve query positions × the expression shapes
//! that discriminate the sub-grammar, plus the list shapes, with the
//! reference's verdict for each.
//!
//! **The positions are not an enumeration of LogQL, and do not need to
//! be.** What has to be exhaustive is the set of PulsusDB entry points
//! that run a pipeline, since that is where the rule could be skipped;
//! [`POSITIONS`]' doc comment names every one and cites it (`exec.rs:612`,
//! `:906`, `:2290`, `:2576`). The table then holds an expression's
//! verdict CONSTANT across all twelve rather than recording one per
//! position — a claim [`live_matrix_against_the_reference`] puts to the
//! container point by point rather than assuming.
//!
//! - [`pulsus_agrees_with_the_captured_reference_verdicts`] — hermetic,
//!   the whole matrix, PulsusDB's verdict at the layer a user meets it
//!   (parse → plan → the pipeline compile `exec` runs before any I/O:
//!   `exec.rs:612`, `:906`, `:2290`, `:2576`).
//! - [`the_pre_247_rule_disagrees_wherever_the_reference_refuses_an_expression`]
//!   — hermetic, the discrimination check: the rule PulsusDB shipped at
//!   `7980344` (the `| logfmt` compile arm cloned the expression string
//!   verbatim; the extraction list ended silently at a dangling comma)
//!   replayed over the same matrix, with every disagreement enumerated
//!   rather than counted.
//! - [`live_matrix_against_the_reference`] — gated on
//!   `PULSUSDB_LOGQL_DIFF_URL`, re-measures all of it against the pinned
//!   container, and checks the compression this table uses (an
//!   expression's verdict does not depend on WHERE the `| logfmt` sits)
//!   rather than assuming it.
//! - [`live_surface_axis_agrees`] — the same two probes put to every
//!   route, so "the rule is only observable where a pipeline runs" is
//!   measured rather than argued.
//!
//! **The window must end at `now`, and that is a measurement.** A
//! layer-2 rejection is invisible on the reference over a stale window:
//! measured on the pinned container, `{service_name="m247"} | logfmt
//! a="b.c"` is **400** over a window ending now and **200** over one
//! ending 4 h ago, because once the query's end is older than
//! `query_ingesters_within` (3 h) the ingester leaves the path and the
//! store returns `NoopEntryIterator` (`pkg/storage/store.go:492 @
//! v3.7.4`) before `expr.Pipeline()` at `:500`. Ledgered as
//! `malformed-query-refused-in-every-window` (#380). This was checked by
//! breaking it: with the window moved 4 h back,
//! [`live_matrix_against_the_reference`] fails on 205 of its 373 points —
//! exactly the sub-grammar rejections — while the layer-1 list-shape
//! rejections stay 400, because `ParseExpr` runs in every window. A leg
//! captured against a stale window would therefore have recorded the
//! whole sub-grammar as accepted and passed for ever.
//!
//! **Excluded by name.**
//! - **Non-ASCII identifiers — #392.** `| logfmt éx="b"`, `| json
//!   éx="b"`, `| label_format éx="y"`, `| drop éx` and `sum by (éx) (…)`
//!   are never a 400 on the pinned container while our lexer refuses
//!   `éx` in all of them; `{éx="m"}` is 400 on both. Two refinements
//!   measured here: with a matching entry in the window the first two are
//!   a **500** (`could not write JSON response: … unexpected character
//!   inside braces: 'é'`) rather than a 200 — admitted, then failing at
//!   response encoding — and 200-empty when no stream matches. And
//!   `| logfmt éx` (the bare form, which expands to `éx="éx"`) is a 400
//!   there by the sub-grammar's own rule, so that one shape agrees with
//!   us for an unrelated reason. It is a whole-lexer question touching
//!   every identifier position, hence its own issue and its absence from
//!   this matrix.
//! - **The `| json` expression grammar — #394.** `| json a="b c"` and
//!   `| json a="b-c"` are 400 on the reference and accepted here. It uses
//!   a DIFFERENT sub-grammar (`pkg/logql/log/jsonexpr/`), so the logfmt
//!   one must not be reused for it. Only the shared list-shape rule
//!   (a dangling comma) is in this matrix for `| json`.
//! - **The three evaluation divergences — #393.** What a SURVIVING
//!   expression returns is not this issue; only the resolved source key
//!   is adopted here, because it is the sub-grammar's own output.
//!
//! **Unmeasured routes, named with the reason.** `/patterns` is a 404 on
//! the pinned container (its pattern ingester is off in
//! `ci/logql/config.yaml`), so it cannot report a syntax verdict.
//! `/tail` is a WebSocket and this harness speaks HTTP only; its
//! PulsusDB counterpart compiles the same pipeline at `exec.rs:2576`,
//! which the hermetic half already covers.

use std::process::Command;

use pulsus_logql::parse;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::plan::Plan;
use pulsus_read::logql::{Direction, PlanCtx, QueryParams, QuerySpec, plan};

/// What a user sees: the query is served, or it is a 400.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Accept,
    Reject,
}

/// WHICH rule refuses a point on the reference — recorded per case
/// because it is what decides the pre-#247 verdict, and it is the
/// enumeration this issue was built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layer {
    /// The LogQL grammar itself (`ParseExpr`) — refused in every window.
    /// PulsusDB's parser already refused these before #247.
    Grammar,
    /// The extraction-LIST shape PulsusDB's parser used to accept: a
    /// comma with no entry after it (`syntax.y:318-321 @ v3.7.4`).
    DanglingComma,
    /// The extraction-EXPRESSION sub-grammar (`Stage()` → 400), which
    /// PulsusDB had no equivalent of at `7980344`.
    SubGrammar,
    /// Served.
    Accepted,
}

impl Layer {
    fn reference(self) -> Verdict {
        match self {
            Layer::Accepted => Verdict::Accept,
            _ => Verdict::Reject,
        }
    }

    /// The rule PulsusDB shipped at `7980344`. `compile_parser`'s
    /// `ParserStage::Logfmt` arm cloned `e.expression` verbatim
    /// (`pipeline.rs:2393-2404` at that SHA), so nothing in the
    /// sub-grammar could refuse anything; and `parse_extraction_list`
    /// looped `while peek is Ident`, so a comma with nothing after it
    /// simply ended the list. Every other rejection is unchanged.
    fn pre_247(self) -> Verdict {
        match self {
            Layer::Grammar => Verdict::Reject,
            Layer::SubGrammar | Layer::DanglingComma | Layer::Accepted => Verdict::Accept,
        }
    }
}

// ---------------------------------------------------------------------
// Table A — the extraction EXPRESSION, crossed with every position.
// ---------------------------------------------------------------------

/// One extraction expression.
///
/// `quoted` is what a user types between the double quotes of `a="…"`;
/// `decoded` is what the reference's `logfmt.Parse` therefore sees, and
/// [`the_fixtures_two_expression_columns_agree`] checks the two against
/// our own lexer so a fixture typo cannot masquerade as a divergence.
struct Expression {
    name: &'static str,
    quoted: &'static str,
    decoded: &'static str,
    layer: Layer,
}

const EXPRESSIONS: &[Expression] = &[
    // --- accepted: E2 (a bare KEY) ----------------------------------
    Expression {
        name: "key",
        quoted: "b",
        decoded: "b",
        layer: Layer::Accepted,
    },
    Expression {
        name: "key_underscore",
        quoted: "_",
        decoded: "_",
        layer: Layer::Accepted,
    },
    Expression {
        name: "key_upper",
        quoted: "B",
        decoded: "B",
        layer: Layer::Accepted,
    },
    Expression {
        name: "key_digit_tail",
        quoted: "b1",
        decoded: "b1",
        layer: Layer::Accepted,
    },
    Expression {
        name: "key_underscored",
        quoted: "b_c",
        decoded: "b_c",
        layer: Layer::Accepted,
    },
    // --- accepted: E5 (a STRING), including the shapes that then match
    //     nothing on either side.
    Expression {
        name: "string_with_space",
        quoted: r#"\"b c\""#,
        decoded: r#""b c""#,
        layer: Layer::Accepted,
    },
    Expression {
        name: "string_with_dot",
        quoted: r#"\"b.c\""#,
        decoded: r#""b.c""#,
        layer: Layer::Accepted,
    },
    Expression {
        name: "string_empty",
        quoted: r#"\"\""#,
        decoded: r#""""#,
        layer: Layer::Accepted,
    },
    Expression {
        name: "string_unterminated",
        quoted: r#"\"unterminated"#,
        decoded: r#""unterminated"#,
        layer: Layer::Accepted,
    },
    Expression {
        name: "string_plain",
        quoted: r#"\"b\""#,
        decoded: r#""b""#,
        layer: Layer::Accepted,
    },
    // --- accepted: E6 (a KEY or STRING then only STRINGs) -----------
    Expression {
        name: "key_then_string",
        quoted: r#"b \"c\""#,
        decoded: r#"b "c""#,
        layer: Layer::Accepted,
    },
    Expression {
        name: "string_then_string",
        quoted: r#"\"b\" \"c\""#,
        decoded: r#""b" "c""#,
        layer: Layer::Accepted,
    },
    // --- refused: E1 (no token at all) ------------------------------
    Expression {
        name: "empty",
        quoted: "",
        decoded: "",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "blank",
        quoted: " ",
        decoded: " ",
        layer: Layer::SubGrammar,
    },
    // --- refused: E3 (a bad character after a complete expression) --
    Expression {
        name: "dot",
        quoted: "b.c",
        decoded: "b.c",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "dash",
        quoted: "b-c",
        decoded: "b-c",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "slash",
        quoted: "b/c",
        decoded: "b/c",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "colon",
        quoted: "b:c",
        decoded: "b:c",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "equals",
        quoted: "b=c",
        decoded: "b=c",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "comma",
        quoted: "b,c",
        decoded: "b,c",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "bang",
        quoted: "b!",
        decoded: "b!",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "index",
        quoted: "b[0]",
        decoded: "b[0]",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "close_bracket",
        quoted: "b]",
        decoded: "b]",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "backslash",
        quoted: r#"b\\c"#,
        decoded: r#"b\c"#,
        layer: Layer::SubGrammar,
    },
    // --- refused: E4 (a bad FIRST character — the syntax error wins) -
    Expression {
        name: "leading_digit",
        quoted: "0b",
        decoded: "0b",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "non_ascii",
        quoted: "é",
        decoded: "é",
        layer: Layer::SubGrammar,
    },
    // --- refused: E6 (a KEY that is not the first token) ------------
    Expression {
        name: "two_keys",
        quoted: "b c",
        decoded: "b c",
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "string_then_key",
        quoted: r#"\"b\" c"#,
        decoded: r#""b" c"#,
        layer: Layer::SubGrammar,
    },
    Expression {
        name: "bracket_ends_string_then_key",
        quoted: r#"\"a]b\""#,
        decoded: r#""a]b""#,
        layer: Layer::SubGrammar,
    },
];

/// Where the `| logfmt` extraction sits. `{Q}` is the expression's
/// `quoted` text, dropped into `a="{Q}"`.
///
/// Every PulsusDB entry point that runs a pipeline compiles it before any
/// I/O — streams `exec.rs:612`, metric `exec.rs:906` (each leaf of a
/// binary plan through `run_metric_node` → `run_metric_inner`),
/// `detected_fields` `exec.rs:2290`, `tail` `exec.rs:2576` — so no
/// position is exempt, and the table asserts that by holding the verdict
/// CONSTANT across positions rather than recording one per position.
struct Position {
    name: &'static str,
    template: &'static str,
}

const POSITIONS: &[Position] = &[
    Position {
        name: "streams",
        template: r#"{service_name="m"} | logfmt a="{Q}""#,
    },
    Position {
        name: "streams_second_stage",
        template: r#"{service_name="m"} | logfmt | logfmt a="{Q}""#,
    },
    Position {
        name: "streams_after_flag",
        template: r#"{service_name="m"} | logfmt --strict a="{Q}""#,
    },
    Position {
        name: "streams_second_extraction",
        template: r#"{service_name="m"} | logfmt ok="x", a="{Q}""#,
    },
    Position {
        name: "streams_after_line_filter",
        template: r#"{service_name="m"} |= "x" | logfmt a="{Q}""#,
    },
    Position {
        name: "count_over_time",
        template: r#"count_over_time({service_name="m"} | logfmt a="{Q}" [5m])"#,
    },
    Position {
        name: "sum_by",
        template: r#"sum by (a) (count_over_time({service_name="m"} | logfmt a="{Q}" [5m]))"#,
    },
    Position {
        name: "unwrap",
        template: r#"sum_over_time({service_name="m"} | logfmt a="{Q}" | unwrap x [5m])"#,
    },
    Position {
        name: "topk",
        template: r#"topk(3, count_over_time({service_name="m"} | logfmt a="{Q}" [5m]))"#,
    },
    Position {
        name: "label_replace",
        template: r#"label_replace(count_over_time({service_name="m"} | logfmt a="{Q}" [5m]), "d", "$1", "a", "(.*)")"#,
    },
    Position {
        name: "binary_lhs",
        template: r#"count_over_time({service_name="m"} | logfmt a="{Q}" [5m]) + count_over_time({service_name="m"} [5m])"#,
    },
    Position {
        name: "binary_rhs",
        template: r#"count_over_time({service_name="m"} [5m]) + count_over_time({service_name="m"} | logfmt a="{Q}" [5m])"#,
    },
];

// ---------------------------------------------------------------------
// Table B — whole-query shapes that do not factor into position ×
// expression: the extraction LIST, the parser flags, and the raw-string
// form.
// ---------------------------------------------------------------------

struct Shape {
    name: &'static str,
    query: &'static str,
    layer: Layer,
}

const SHAPES: &[Shape] = &[
    // --- the list rule this issue fixes (G4): a comma must be followed
    //     by another entry (`syntax.y:318-321 @ v3.7.4`).
    Shape {
        name: "dangling_comma",
        query: r#"{service_name="m"} | logfmt a="b","#,
        layer: Layer::DanglingComma,
    },
    Shape {
        name: "dangling_comma_then_stage",
        query: r#"{service_name="m"} | logfmt a="b", | json"#,
        layer: Layer::DanglingComma,
    },
    Shape {
        name: "dangling_comma_json",
        query: r#"{service_name="m"} | json a="b","#,
        layer: Layer::DanglingComma,
    },
    Shape {
        name: "dangling_comma_bare_entry",
        query: r#"{service_name="m"} | logfmt a,"#,
        layer: Layer::DanglingComma,
    },
    Shape {
        name: "dangling_comma_in_metric",
        query: r#"count_over_time({service_name="m"} | logfmt a="b", [5m])"#,
        layer: Layer::DanglingComma,
    },
    // --- the raw-string form of an expression: the sub-grammar sees the
    //     same bytes, so the same rule applies (G3 accepts the token,
    //     E3 refuses the body).
    Shape {
        name: "raw_string_expression",
        query: r#"{service_name="m"} | logfmt a=`b.c`"#,
        layer: Layer::SubGrammar,
    },
    Shape {
        name: "raw_string_expression_ok",
        query: r#"{service_name="m"} | logfmt a=`b`"#,
        layer: Layer::Accepted,
    },
    // --- list shapes both sides already refused (G3/G5). They pin the
    //     rule; they discriminate nothing for THIS fix, and
    //     `the_pre_247_rule_disagrees_…` names them as such.
    Shape {
        name: "unquoted_expression",
        query: r#"{service_name="m"} | logfmt a=b"#,
        layer: Layer::Grammar,
    },
    Shape {
        name: "missing_expression",
        query: r#"{service_name="m"} | logfmt a="#,
        layer: Layer::Grammar,
    },
    Shape {
        name: "juxtaposed_entries",
        query: r#"{service_name="m"} | logfmt a="b" c="d""#,
        layer: Layer::Grammar,
    },
    Shape {
        name: "double_comma",
        query: r#"{service_name="m"} | logfmt a="b",,c="d""#,
        layer: Layer::Grammar,
    },
    Shape {
        name: "leading_comma",
        query: r#"{service_name="m"} | logfmt ,a="b""#,
        layer: Layer::Grammar,
    },
    Shape {
        name: "numeric_identifier",
        query: r#"{service_name="m"} | logfmt 1a="b""#,
        layer: Layer::Grammar,
    },
    Shape {
        name: "dashed_identifier",
        query: r#"{service_name="m"} | logfmt a-b="c""#,
        layer: Layer::Grammar,
    },
    Shape {
        name: "flag_after_list",
        query: r#"{service_name="m"} | logfmt a="b" --strict"#,
        layer: Layer::Grammar,
    },
    Shape {
        name: "unknown_flag",
        query: r#"{service_name="m"} | logfmt --nope a="b""#,
        layer: Layer::Grammar,
    },
    // --- accepted list shapes, including the ones the early return in
    //     `parse_extraction_list` must keep working.
    Shape {
        name: "bare_logfmt",
        query: r#"{service_name="m"} | logfmt"#,
        layer: Layer::Accepted,
    },
    Shape {
        name: "bare_logfmt_flags",
        query: r#"{service_name="m"} | logfmt --strict --keep-empty"#,
        layer: Layer::Accepted,
    },
    Shape {
        name: "bare_json",
        query: r#"{service_name="m"} | json"#,
        layer: Layer::Accepted,
    },
    Shape {
        name: "bare_identifier_entry",
        query: r#"{service_name="m"} | logfmt a"#,
        layer: Layer::Accepted,
    },
    Shape {
        name: "bare_then_expression",
        query: r#"{service_name="m"} | logfmt a, b="c""#,
        layer: Layer::Accepted,
    },
    Shape {
        name: "expression_then_bare",
        query: r#"{service_name="m"} | logfmt a="b", c"#,
        layer: Layer::Accepted,
    },
    Shape {
        name: "flags_then_list",
        query: r#"{service_name="m"} | logfmt --strict --keep-empty a="b""#,
        layer: Layer::Accepted,
    },
    Shape {
        name: "repeated_flag",
        query: r#"{service_name="m"} | logfmt --strict --strict a="b""#,
        layer: Layer::Accepted,
    },
    Shape {
        name: "json_expression",
        query: r#"{service_name="m"} | json a="b""#,
        layer: Layer::Accepted,
    },
];

/// One matrix point.
struct Point {
    label: String,
    query: String,
    layer: Layer,
}

fn matrix() -> Vec<Point> {
    let mut out = Vec::new();
    for position in POSITIONS {
        for e in EXPRESSIONS {
            out.push(Point {
                label: format!("{}/{}", position.name, e.name),
                query: position.template.replace("{Q}", e.quoted),
                layer: e.layer,
            });
        }
    }
    for s in SHAPES {
        out.push(Point {
            label: format!("shape/{}", s.name),
            query: s.query.to_string(),
            layer: s.layer,
        });
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

/// A `query_range` request, because that is the endpoint the live leg
/// puts these to — the reference refuses an instant LOG query outright,
/// which would mask the surface under test.
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
/// parser alone would be a weaker layer and would report the whole
/// sub-grammar as accepted, which is exactly the gap this issue closed.
///
/// Every pipeline a plan carries is compiled, including each leaf of a
/// binary plan (`MetricNode::leaves`), because `run_metric_inner` does.
fn pulsus_verdict(query: &str) -> (Verdict, String) {
    let expr = match parse(query) {
        Ok(e) => e,
        Err(e) => return (Verdict::Reject, format!("parse: {e}")),
    };
    let planned = match plan(&expr, &params(), &ctx()) {
        Ok(p) => p,
        Err(e) => return (Verdict::Reject, format!("plan: {e}")),
    };
    let pipelines: Vec<&[pulsus_logql::Stage]> = match &planned {
        Plan::Streams(sp) => vec![&sp.pipeline],
        Plan::Metric(mp) => mp.client.iter().map(|c| c.pipeline.as_slice()).collect(),
        Plan::MetricBinary(node) => node
            .leaves()
            .into_iter()
            .filter_map(|leaf| leaf.client.as_ref())
            .map(|c| c.pipeline.as_slice())
            .collect(),
    };
    for stages in pipelines {
        if let Err(e) = CompiledPipeline::compile(stages) {
            return (Verdict::Reject, format!("compile: {e}"));
        }
    }
    (Verdict::Accept, String::new())
}

/// **The two expression columns agree.** `decoded` is what the
/// reference's sub-grammar sees; this derives it from `quoted` through
/// our own lexer, so a mis-escaped fixture row shows up here rather than
/// as a phantom agreement (or disagreement) about the reference.
#[test]
fn the_fixtures_two_expression_columns_agree() {
    for e in EXPRESSIONS {
        let query = format!(r#"{{service_name="m"}} | logfmt a="{}""#, e.quoted);
        let expr = parse(&query).unwrap_or_else(|err| panic!("{}: {query}: {err}", e.name));
        let pulsus_logql::Expr::Log(log) = &expr else {
            panic!("{}: not a log query", e.name);
        };
        let mut found = None;
        for stage in &log.pipeline {
            if let pulsus_logql::Stage::Parser(pulsus_logql::ParserStage::Logfmt {
                extractions,
                ..
            }) = stage
            {
                found = extractions.first().map(|x| x.expression.clone());
            }
        }
        assert_eq!(
            found.as_deref(),
            Some(e.decoded),
            "{}: `a=\"{}\"` decodes to {:?}, but the fixture says {:?}",
            e.name,
            e.quoted,
            found,
            e.decoded
        );
    }
}

/// **The matrix agrees with the reference, point for point.**
#[test]
fn pulsus_agrees_with_the_captured_reference_verdicts() {
    let points = matrix();
    assert_eq!(
        points.len(),
        POSITIONS.len() * EXPRESSIONS.len() + SHAPES.len(),
        "the matrix must be the full cross product plus the shape table"
    );

    let mut disagree = Vec::new();
    let (mut accepts, mut rejects) = (0usize, 0usize);
    for p in &points {
        let (ours, why) = pulsus_verdict(&p.query);
        match p.layer.reference() {
            Verdict::Accept => accepts += 1,
            Verdict::Reject => rejects += 1,
        }
        if ours != p.layer.reference() {
            disagree.push(format!(
                "{} {}\n    reference={:?} pulsus={:?} {}",
                p.label,
                p.query,
                p.layer.reference(),
                ours,
                why
            ));
            continue;
        }
        // **The LAYER is checked, not assumed.** Agreeing on the verdict
        // is not the same as refusing for the right reason: a
        // sub-grammar point that our PARSER happened to reject would
        // score as agreement while proving nothing about the pipeline
        // compile, and a metric position whose plan carries no client
        // pipeline would never reach the compile at all. So each
        // rejection's reason must name the layer the fixture says.
        match p.layer {
            Layer::SubGrammar => assert!(
                why.starts_with("compile:"),
                "{} {}: the sub-grammar must refuse at the PIPELINE COMPILE (the layer `exec` \
                 reaches before any I/O), but the rejection came from {why:?}",
                p.label,
                p.query
            ),
            Layer::Grammar | Layer::DanglingComma => assert!(
                why.starts_with("parse:"),
                "{} {}: a list-shape rejection must come from the PARSER, but it came from \
                 {why:?}",
                p.label,
                p.query
            ),
            Layer::Accepted => {}
        }
    }
    assert!(
        disagree.is_empty(),
        "{} of {} matrix points disagree with the captured reference verdicts:\n{}",
        disagree.len(),
        points.len(),
        disagree.join("\n")
    );
    // Both dispositions must be represented, or "agrees everywhere"
    // would be a statement about a matrix that only ever accepts.
    assert!(
        accepts > 0 && rejects > 0,
        "the matrix must contain both dispositions ({accepts} accept, {rejects} reject)"
    );
    // And every layer must be exercised, or a whole rule could vanish
    // from the fixture without a test noticing.
    for layer in [
        Layer::Grammar,
        Layer::DanglingComma,
        Layer::SubGrammar,
        Layer::Accepted,
    ] {
        assert!(
            points.iter().any(|p| p.layer == layer),
            "no matrix point exercises {layer:?}"
        );
    }
}

/// **The fixture discriminates.** The rule PulsusDB shipped at `7980344`
/// replayed over the same matrix, with every disagreement enumerated.
///
/// Every one of them is the same direction — PulsusDB accepts a query the
/// reference refuses — and the disagreeing set is exactly the points the
/// two new rules refuse, computed from the table rather than restated as
/// a literal. The `Layer::Grammar` points are the control: they were
/// already 400 on both sides at `7980344`, so they pin the rule and
/// discriminate NOTHING for this fix.
#[test]
fn the_pre_247_rule_disagrees_wherever_the_reference_refuses_an_expression() {
    let points = matrix();
    let mut mismatches = Vec::new();
    for p in &points {
        if p.layer.pre_247() != p.layer.reference() {
            assert_eq!(
                (p.layer.pre_247(), p.layer.reference()),
                (Verdict::Accept, Verdict::Reject),
                "a pre-#247 mismatch in the other direction would be a different bug: {} {}",
                p.label,
                p.query
            );
            mismatches.push(p);
        }
    }
    let expected: usize = points
        .iter()
        .filter(|p| matches!(p.layer, Layer::SubGrammar | Layer::DanglingComma))
        .count();
    assert_eq!(
        mismatches.len(),
        expected,
        "the pre-#247 rule must disagree at every sub-grammar and dangling-comma point, and \
         nowhere else"
    );
    assert!(
        mismatches
            .iter()
            .all(|p| matches!(p.layer, Layer::SubGrammar | Layer::DanglingComma)),
        "a mismatch outside the two rules this issue adds"
    );
    // NAMED, not counted — and split, because the two kinds of
    // non-discriminating point mean different things. A `Grammar` point
    // was already 400 on both trees: it pins a rule and proves nothing
    // about this fix. An `Accepted` point is a CONTROL: it shows the new
    // rule does not over-refuse.
    let already_rejecting: Vec<&str> = points
        .iter()
        .filter(|p| p.layer == Layer::Grammar)
        .map(|p| p.label.as_str())
        .collect();
    let controls = points.iter().filter(|p| p.layer == Layer::Accepted).count();
    assert_eq!(
        already_rejecting.len() + controls + mismatches.len(),
        points.len(),
        "every point must be a discriminator, a control, or an already-rejecting pin"
    );
    eprintln!(
        "pre-#247 rule: {} of {} points disagree with the reference, all 'PulsusDB accepts, \
         reference refuses'.\n{} points pin a rule but DISCRIMINATE NOTHING (400 on both trees \
         at 7980344): {}\n{} points are accept-side controls (200 on both), which is what shows \
         the new rule does not over-refuse.",
        mismatches.len(),
        points.len(),
        already_rejecting.len(),
        already_rejecting.join(", "),
        controls,
    );
}

// ---------------------------------------------------------------------
// The live legs (gated on PULSUSDB_LOGQL_DIFF_URL). Status only: no data
// is pushed, because accept-vs-reject is decided before a line is read.
// ---------------------------------------------------------------------

/// A window ENDING AT `now` — see the module docs; a stale one makes
/// every layer-2 rejection invisible.
fn reference_status(
    base: &str,
    path: &str,
    param: &str,
    query: &str,
    ranged: bool,
) -> (u32, String) {
    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let mut args: Vec<String> = vec![
        "-s".into(),
        "--max-time".into(),
        "30".into(),
        "-o".into(),
        "/dev/stderr".into(),
        "-w".into(),
        "%{http_code}".into(),
        "-G".into(),
        format!("{base}{path}"),
        "--data-urlencode".into(),
        format!("{param}={query}"),
    ];
    if ranged {
        args.push("--data-urlencode".into());
        args.push(format!("start={}", (now_s - 300) * 1_000_000_000));
        args.push("--data-urlencode".into());
        args.push(format!("end={}", now_s * 1_000_000_000));
        args.push("--data-urlencode".into());
        args.push("step=60s".into());
    } else {
        args.push("--data-urlencode".into());
        args.push(format!("time={}", now_s * 1_000_000_000));
    }
    let out = Command::new("curl").args(&args).output().expect("curl");
    let code: u32 = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("http status");
    let body = String::from_utf8_lossy(&out.stderr).trim().to_string();
    (code, body)
}

fn reference_verdict(base: &str, query: &str) -> (Verdict, String) {
    let (code, body) = reference_status(base, "/loki/api/v1/query_range", "query", query, true);
    match code {
        200 => (Verdict::Accept, body),
        400 => (Verdict::Reject, body),
        other => panic!("unexpected status {other} for {query:?}: {body}"),
    }
}

/// **Re-measures the committed verdicts against the pinned container**,
/// and checks the compression they are stored in.
///
/// The table records ONE layer per expression for a matrix of position ×
/// expression. That is a claim — that an expression's verdict does not
/// depend on where the `| logfmt` sits — and this leg puts every point
/// individually, so the claim is measured rather than assumed.
#[test]
fn live_matrix_against_the_reference() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the live logfmt-expression matrix");
        return;
    };
    let points = matrix();
    let mut disagree = Vec::new();
    for p in &points {
        let (theirs, body) = reference_verdict(&base, &p.query);
        if theirs != p.layer.reference() {
            disagree.push(format!(
                "{} {}\n    committed={:?} container={:?} {}",
                p.label,
                p.query,
                p.layer.reference(),
                theirs,
                body.chars().take(160).collect::<String>()
            ));
        }
    }
    assert!(
        disagree.is_empty(),
        "{} of {} committed verdicts no longer match the pinned container — re-capture the \
         table, do not edit it to match one point:\n{}",
        disagree.len(),
        points.len(),
        disagree.join("\n")
    );
    eprintln!(
        "re-measured {} matrix points against the reference; the committed verdicts hold",
        points.len()
    );
}

/// One route, with the status the pinned container answers for a
/// well-formed and for a malformed logfmt expression.
struct Route {
    name: &'static str,
    path: &'static str,
    param: &'static str,
    ranged: bool,
    /// The query, with `{L}` replaced by the `| logfmt` stage.
    template: &'static str,
    well_formed: u32,
    malformed: u32,
}

/// **Where the rule is observable at all.** Captured from the pinned
/// container; re-measured by [`live_surface_axis_agrees`].
///
/// The four matcher-only routes answer `only label matchers are
/// supported` for BOTH probes — the pipeline never reaches `Stage()`
/// there, on either side (PulsusDB refuses a pipeline on those routes
/// too; `logs_api/volume.rs:85-91` is the shape). `/query` is probed with
/// a METRIC query on purpose: a LOG query is refused there on type
/// grounds (`log queries are not supported as an instant query type`)
/// for both probes alike, which would make the route non-discriminating.
/// `/format_query` is 200 for both because `ParseExpr` never calls
/// `Stage()` — it has no PulsusDB counterpart, and is here to show the
/// boundary is `Stage()` and not "any endpoint that parses".
const ROUTES: &[Route] = &[
    Route {
        name: "query_range",
        path: "/loki/api/v1/query_range",
        param: "query",
        ranged: true,
        template: r#"{service_name="m"} {L}"#,
        well_formed: 200,
        malformed: 400,
    },
    Route {
        name: "query_instant_metric",
        path: "/loki/api/v1/query",
        param: "query",
        ranged: false,
        template: r#"count_over_time({service_name="m"} {L} [5m])"#,
        well_formed: 200,
        malformed: 400,
    },
    Route {
        name: "detected_fields",
        path: "/loki/api/v1/detected_fields",
        param: "query",
        ranged: true,
        template: r#"{service_name="m"} {L}"#,
        well_formed: 200,
        malformed: 400,
    },
    Route {
        name: "index_volume",
        path: "/loki/api/v1/index/volume",
        param: "query",
        ranged: true,
        template: r#"{service_name="m"} {L}"#,
        well_formed: 400,
        malformed: 400,
    },
    Route {
        name: "index_stats",
        path: "/loki/api/v1/index/stats",
        param: "query",
        ranged: true,
        template: r#"{service_name="m"} {L}"#,
        well_formed: 400,
        malformed: 400,
    },
    Route {
        name: "series",
        path: "/loki/api/v1/series",
        param: "match[]",
        ranged: true,
        template: r#"{service_name="m"} {L}"#,
        well_formed: 400,
        malformed: 400,
    },
    Route {
        name: "labels",
        path: "/loki/api/v1/labels",
        param: "query",
        ranged: true,
        template: r#"{service_name="m"} {L}"#,
        well_formed: 400,
        malformed: 400,
    },
    Route {
        name: "patterns",
        path: "/loki/api/v1/patterns",
        param: "query",
        ranged: true,
        template: r#"{service_name="m"} {L}"#,
        well_formed: 404,
        malformed: 404,
    },
    Route {
        name: "format_query",
        path: "/loki/api/v1/format_query",
        param: "query",
        ranged: false,
        template: r#"{service_name="m"} {L}"#,
        well_formed: 200,
        malformed: 200,
    },
];

/// **"Only observable where a pipeline runs" is measured, not argued.**
#[test]
fn live_surface_axis_agrees() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the live surface axis");
        return;
    };
    let mut wrong = Vec::new();
    for r in ROUTES {
        for (stage, want) in [
            (r#"| logfmt a="b""#, r.well_formed),
            (r#"| logfmt a="b.c""#, r.malformed),
        ] {
            let query = r.template.replace("{L}", stage);
            let (code, body) = reference_status(&base, r.path, r.param, &query, r.ranged);
            if code != want {
                wrong.push(format!(
                    "{} {stage}\n    committed={want} container={code} {}",
                    r.name,
                    body.chars().take(160).collect::<String>()
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} route verdicts no longer match the pinned container:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
    // The axis has to contain a route where the two probes DIFFER, or it
    // would prove nothing about where the rule is observable.
    assert!(
        ROUTES.iter().any(|r| r.well_formed != r.malformed),
        "no route in the axis discriminates"
    );
    assert!(
        ROUTES.iter().any(|r| r.well_formed == r.malformed),
        "no route in the axis is pipeline-blind"
    );
    eprintln!("re-measured {} routes against the reference", ROUTES.len());
}
