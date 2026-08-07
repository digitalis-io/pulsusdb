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
//! both, as a fixture: every query position × the expression shapes that
//! discriminate the sub-grammar, plus the list shapes, with the
//! reference's verdict AND PulsusDB's for each.
//!
//! **The positions are not an enumeration of LogQL, and do not need to
//! be.** What has to be exhaustive is the set of places PulsusDB compiles
//! a pipeline, since that is where the rule could be skipped. The first
//! version of this file enumerated twelve query POSITIONS instead — "where
//! can a `| logfmt` sit" — and so held no `variants(...)` query at all.
//! That missed **one** of the construct's two pipeline positions, the
//! variant side. The common side was reachable even then: the round-one
//! helper walked `MetricNode::leaves()`, which yields the `scan` plan
//! whose client pipeline IS the common one, and replaying that helper
//! shows it returning a compile-layer rejection for a malformed common
//! pipeline. (An earlier revision of this sentence said "neither", which
//! is false and was checked by reconstructing the helper.) The
//! enumeration is therefore derived from the code that CALLS the
//! compiler, by
//! [`the_compile_sites_are_enumerated_from_the_callers_of_the_compiler`],
//! which reddens on a new call site until someone decides whether this
//! matrix must cover it.
//!
//! **`variants(...)` disagreed with the reference in BOTH directions, and
//! the table says so rather than being tuned until it agrees.** The
//! reference validates the variant's own pipeline and ignores it at
//! evaluation; it runs the common `of (...)` pipeline and does not
//! validate it. PulsusDB was the mirror image. The variant side is now
//! CLOSED — `build_variants_node` compiles a variant's own pipeline
//! purely to validate it, exactly as the reference does — and
//! [`points_disagree_with_the_reference_only_where_the_table_says_so`]
//! asserts that direction is EMPTY. The common side stays open and
//! ledgered — see [`Outcome`] for its mechanism and the measurement
//! behind it.
//!
//! - [`pulsus_agrees_with_the_captured_reference_verdicts`] — hermetic,
//!   the whole matrix, PulsusDB's verdict at the layer a user meets it:
//!   parse → plan → the pipeline compile that runs before any I/O. Every
//!   position is a `query_range`-shaped query, so the sites it actually
//!   drives are `exec.rs:612` (streams), `:906` (metric, incl. every
//!   binary leaf), `plan.rs`'s variant validation and `variants.rs:509`
//!   — NOT `exec.rs:2290`/`:2576`, which are `/detected_fields` and
//!   `/tail` and are covered as described under "unmeasured routes".
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
//! [`live_matrix_against_the_reference`] fails on **205 of its 431**
//! points. That figure is measured by patching the window and running the
//! literal test, never scaled from an earlier one: this sentence said 222
//! for one revision, which was the same measurement taken while the leg
//! still compared against the wrong verdict, and 222 − 17 = 205 is
//! exactly the arithmetic that makes a rescaled number look plausible.
//!
//! **The 205 are not "the sub-grammar rejections", and the exception is
//! the interesting part.** They are the 17 sub-grammar expressions at
//! each of the twelve pipeline-compile positions, plus
//! `shape/raw_string_expression`. The list-shape rejections stay 400
//! because `ParseExpr` runs in every window — and so do all 17
//! `variants_variant_side` points, which the reference raises in
//! `newVariantsEvaluator` in the QUERIER (`evaluator.go:1417 @ v3.7.4`)
//! rather than behind the store's stale-window short circuit. Measured:
//! no `variants_*` point appears among the 205. A leg captured against a
//! stale window would still have recorded most of the sub-grammar as
//! accepted and passed for ever.
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
//! `/tail` is a WebSocket and this harness speaks HTTP only. Its
//! PulsusDB counterpart (`tail_setup`, `exec.rs:2576`) calls the same
//! `CompiledPipeline::compile` on the same `StreamsPlan::pipeline` that
//! `exec.rs:612` does, so the RULE is covered by the streams positions —
//! but that call SITE is not driven by anything in this file, and saying
//! "the hermetic half already covers it" would be a claim about the site
//! rather than the rule. `/detected_fields` (`exec.rs:2290`) is likewise
//! not driven here; it is measured live by
//! [`live_surface_axis_agrees`]'s route table.

use std::process::Command;

use pulsus_logql::parse;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::plan::{MetricNode, MetricNodeScc, Plan};
use pulsus_read::logql::{Direction, PlanCtx, QueryParams, QuerySpec, plan};
use pulsus_read::logql::{MAX_VARIANT_FANOUT_STATE_BYTES, VariantArena};

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
    outcome: Outcome,
    refuses_at: Refusal,
}

/// Whether PulsusDB's verdict at a position matches the reference's, and
/// when it does not, WHICH divergence it is. Recorded per position so a
/// disagreement is stated by the fixture rather than hidden by a table
/// that can only express agreement (issue #247, review round 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// PulsusDB's verdict equals the reference's.
    Agrees,
    /// **Ledgered divergence, deliberate — the only one left.** The
    /// reference answers **200 with an empty result** for a malformed
    /// COMMON `of (...)` pipeline; PulsusDB answers 400. This is not "the
    /// reference serves it": measured on the pinned container, the
    /// ingester returns `rpc error: code = Code(400) desc = error
    /// extracting common pipeline: parse error : stage '| logfmt
    /// a="b.c"' : cannot parse expression [b.c]: unexpected char .` (read
    /// from the container's own log during the request) and the querier
    /// swallows it, handing the user an empty 200 with no indication the
    /// query was broken. The control is the same query with a well-formed
    /// expression, which returns a series. That is the class the owner
    /// ruled on for `malformed-query-refused-in-every-window`: "the user
    /// reads an empty result as 'no logs matched', not 'your query is
    /// broken'". Upheld on review; ledgered, not closed.
    CommonSideRefusedHere,
}

/// Which of PulsusDB's two pre-I/O layers refuses a malformed extraction
/// EXPRESSION at a position. Both run before any ClickHouse call and both
/// surface as a 400, but they are different code and the fixture says
/// which, so "we reject" cannot pass for the wrong reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// `CompiledPipeline::compile` on a pipeline the plan carries — the
    /// ordinary case (`exec.rs:612`, `:906`, `:2290`, `:2576`;
    /// `variants.rs:509` for a variants common pipeline).
    PipelineCompile,
    /// Inside `plan()` itself: `build_variants_node` compiles a variant's
    /// OWN pipeline purely to validate it (issue #247 round 2), because
    /// nothing downstream ever will — `variant_tail` has already
    /// discarded it. Mirrors the reference, which builds the variant's
    /// extractor at `evaluator.go:1417 @ v3.7.4` and keeps only its
    /// length.
    PlanTime,
}

const POSITIONS: &[Position] = &[
    Position {
        name: "streams",
        template: r#"{service_name="m"} | logfmt a="{Q}""#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "streams_second_stage",
        template: r#"{service_name="m"} | logfmt | logfmt a="{Q}""#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "streams_after_flag",
        template: r#"{service_name="m"} | logfmt --strict a="{Q}""#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "streams_second_extraction",
        template: r#"{service_name="m"} | logfmt ok="x", a="{Q}""#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "streams_after_line_filter",
        template: r#"{service_name="m"} |= "x" | logfmt a="{Q}""#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "count_over_time",
        template: r#"count_over_time({service_name="m"} | logfmt a="{Q}" [5m])"#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "sum_by",
        template: r#"sum by (a) (count_over_time({service_name="m"} | logfmt a="{Q}" [5m]))"#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "unwrap",
        template: r#"sum_over_time({service_name="m"} | logfmt a="{Q}" | unwrap x [5m])"#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "topk",
        template: r#"topk(3, count_over_time({service_name="m"} | logfmt a="{Q}" [5m]))"#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "label_replace",
        template: r#"label_replace(count_over_time({service_name="m"} | logfmt a="{Q}" [5m]), "d", "$1", "a", "(.*)")"#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "binary_lhs",
        template: r#"count_over_time({service_name="m"} | logfmt a="{Q}" [5m]) + count_over_time({service_name="m"} [5m])"#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    Position {
        name: "binary_rhs",
        template: r#"count_over_time({service_name="m"} [5m]) + count_over_time({service_name="m"} | logfmt a="{Q}" [5m])"#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PipelineCompile,
    },
    // --- `variants(...) of (...)`: the FIFTH compile site
    //     (`variants.rs:509`), missed by the first version of this
    //     matrix. It has TWO pipeline positions and the reference gives
    //     them OPPOSITE verdicts, so both are here and neither agrees
    //     with us. See [`Outcome`] for the mechanism behind each.
    Position {
        name: "variants_variant_side",
        template: r#"variants(count_over_time({service_name="m"} | logfmt a="{Q}" [5m])) of ({service_name="m"} [5m])"#,
        outcome: Outcome::Agrees,
        refuses_at: Refusal::PlanTime,
    },
    Position {
        name: "variants_common_side",
        template: r#"variants(count_over_time({service_name="m"} [5m])) of ({service_name="m"} | logfmt a="{Q}" [5m])"#,
        outcome: Outcome::CommonSideRefusedHere,
        refuses_at: Refusal::PipelineCompile,
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
    outcome: Outcome,
    refuses_at: Refusal,
}

impl Point {
    /// What the pinned container answers, from the captured table.
    fn reference(&self) -> Verdict {
        match self.outcome {
            // The common `of (...)` pipeline's build error happens behind
            // `SelectSamples` and is swallowed, so the user gets a 200
            // with an empty result. A LogQL GRAMMAR error is still a 400,
            // because `ParseExpr` runs before any of that.
            Outcome::CommonSideRefusedHere => match self.layer {
                Layer::Grammar | Layer::DanglingComma => Verdict::Reject,
                Layer::SubGrammar | Layer::Accepted => Verdict::Accept,
            },
            Outcome::Agrees => self.layer.reference(),
        }
    }

    /// What PulsusDB answers, recorded rather than assumed equal to the
    /// reference — [`points_disagree_with_the_reference_only_where_the_table_says_so`]
    /// pins the disagreeing set exactly.
    fn pulsus(&self) -> Verdict {
        match self.outcome {
            Outcome::Agrees | Outcome::CommonSideRefusedHere => self.layer.reference(),
        }
    }
}

fn matrix() -> Vec<Point> {
    let mut out = Vec::new();
    for position in POSITIONS {
        for e in EXPRESSIONS {
            out.push(Point {
                label: format!("{}/{}", position.name, e.name),
                query: position.template.replace("{Q}", e.quoted),
                layer: e.layer,
                outcome: position.outcome,
                refuses_at: position.refuses_at,
            });
        }
    }
    for s in SHAPES {
        out.push(Point {
            label: format!("shape/{}", s.name),
            query: s.query.to_string(),
            layer: s.layer,
            outcome: Outcome::Agrees,
            // Unused: a shape's layer is never `SubGrammar`.
            refuses_at: Refusal::PipelineCompile,
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
/// **This mirrors the compile SITES, one by one — it does not claim to
/// walk "every pipeline a plan carries".** An earlier version of this
/// comment did claim that, and it was false: `MetricNode::leaves()`
/// (`plan.rs:457`) pushes only `scan` for `MetricNode::Variants` and
/// never the variant specs, so a `variants(...)` query was measured
/// through one of its two pipeline positions and not the other. The
/// authority for this function is
/// [`the_compile_sites_are_enumerated_from_the_callers_of_the_compiler`],
/// which derives the site list from the source rather than from a
/// reading of the plan types.
///
/// The variants arm reproduces `VariantArena::build`
/// (`variants.rs:507-512`) rather than approximating it: the common
/// pipeline is compiled alone, and each variant's tail is compiled as
/// `common ++ tail`, because `VariantSpec::client`'s own doc says
/// "nothing may compile `client.pipeline` on its own".
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
        // `exec.rs:612` (streams), `:2290` (detected_fields), `:2576` (tail).
        Plan::Streams(sp) => pipelines.push(sp.pipeline.clone()),
        // `exec.rs:906` (`run_metric_inner`).
        Plan::Metric(mp) => pipelines.extend(mp.client.iter().map(|c| c.pipeline.clone())),
        Plan::MetricBinary(node) => {
            // `exec.rs:906` again, per binary leaf.
            for leaf in node.leaves() {
                pipelines.extend(leaf.client.iter().map(|c| c.pipeline.clone()));
            }
            // `variants.rs:509` — the fifth site, and the one the twelve
            // -position enumeration missed. `leaves()` already yielded the
            // scan above; add each variant's `common ++ tail`.
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
/// `VariantArena::build` compile. Walks the whole node tree, because a
/// variants node can sit under a binary op or an aggregation.
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

/// **PulsusDB's verdict is what the table records, point for point** —
/// which is the reference's verdict everywhere except the two `variants`
/// positions, whose divergences the table states explicitly. See
/// [`points_disagree_with_the_reference_only_where_the_table_says_so`],
/// which pins that exception set so it cannot quietly grow.
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
        match p.reference() {
            Verdict::Accept => accepts += 1,
            Verdict::Reject => rejects += 1,
        }
        if ours != p.pulsus() {
            disagree.push(format!(
                "{} {}\n    table says pulsus={:?} (reference={:?}), measured {:?} {}",
                p.label,
                p.query,
                p.pulsus(),
                p.reference(),
                ours,
                why
            ));
            continue;
        }
        // A point we are recorded as ACCEPTING carries no rejection
        // reason to check the layer of.
        if p.pulsus() == Verdict::Accept {
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
            Layer::SubGrammar => {
                let want = match p.refuses_at {
                    Refusal::PipelineCompile => "compile:",
                    Refusal::PlanTime => "plan:",
                };
                assert!(
                    why.starts_with(want),
                    "{} {}: the fixture says this sub-grammar rejection surfaces at {:?} \
                     ({want:?}), but it came from {why:?}",
                    p.label,
                    p.query,
                    p.refuses_at
                );
            }
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
        "{} of {} matrix points disagree with the committed PulsusDB verdicts:\n{}",
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
    // And every layer AND every outcome must be exercised, or a whole
    // rule could vanish from the fixture without a test noticing.
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
    for outcome in [Outcome::Agrees, Outcome::CommonSideRefusedHere] {
        assert!(
            points.iter().any(|p| p.outcome == outcome),
            "no matrix point exercises {outcome:?}"
        );
    }
    for refusal in [Refusal::PipelineCompile, Refusal::PlanTime] {
        assert!(
            points
                .iter()
                .any(|p| p.layer == Layer::SubGrammar && p.refuses_at == refusal),
            "no sub-grammar point refuses at {refusal:?}"
        );
    }
}

/// **The exception set is pinned, and it is exactly ONE position.**
/// Without this, a future change could add a divergence by giving a
/// position a non-`Agrees` outcome and everything would still be green.
/// Every disagreement is enumerated by name and its direction asserted.
///
/// The other direction — the reference refuses and we accept — must now
/// be EMPTY. `variants_variant_side` was that bucket until #247 round 2
/// made `build_variants_node` validate a variant's own pipeline, and an
/// empty assertion is what stops it coming back.
#[test]
fn points_disagree_with_the_reference_only_where_the_table_says_so() {
    let points = matrix();
    let mut we_accept_they_refuse = Vec::new();
    let mut we_refuse_they_accept = Vec::new();
    for p in &points {
        if p.pulsus() == p.reference() {
            continue;
        }
        // A disagreement may ONLY come from a position whose outcome
        // declares one, and only for a sub-grammar expression — an
        // `Accepted` expression agrees even at the variants positions,
        // which is why the outcome is a property of the position and the
        // check is on the disagreeing points.
        assert_eq!(
            p.layer,
            Layer::SubGrammar,
            "{}: only a sub-grammar expression may disagree",
            p.label
        );
        match (p.pulsus(), p.reference()) {
            (Verdict::Accept, Verdict::Reject) => we_accept_they_refuse.push(p.label.as_str()),
            (Verdict::Reject, Verdict::Accept) => {
                assert_eq!(p.outcome, Outcome::CommonSideRefusedHere, "{}", p.label);
                we_refuse_they_accept.push(p.label.as_str());
            }
            other => panic!("{}: impossible verdict pair {other:?}", p.label),
        }
    }
    assert!(
        we_accept_they_refuse.is_empty(),
        "PulsusDB accepts {} queries the reference refuses. That is the higher-priority \
         direction and #247 closed the whole of it, so a new one is a regression rather than \
         a divergence to record: {we_accept_they_refuse:?}",
        we_accept_they_refuse.len()
    );
    // The remaining divergence is exactly the sub-grammar expressions at
    // the one position that carries it — computed from the table.
    let sub_grammar = EXPRESSIONS
        .iter()
        .filter(|e| e.layer == Layer::SubGrammar)
        .count();
    assert_eq!(
        we_refuse_they_accept.len(),
        sub_grammar,
        "the common-side divergence must be exactly the sub-grammar expressions: \
         {we_refuse_they_accept:?}"
    );
    eprintln!(
        "LEDGERED divergence — the reference answers 200-empty, we answer 400 ({} points, all \
         `variants_common_side`): {}\nOPEN in the other direction: none.",
        we_refuse_they_accept.len(),
        we_refuse_they_accept.join(", "),
    );
}

/// **The oracle's variants arm, and an honest account of how weak this
/// tie is.** [`pulsus_verdict`]'s variants arm is a hand-written
/// reproduction of `VariantArena::build` (`variants.rs:507-540`): it
/// compiles the common pipeline, then `common ++ tail` per variant. The
/// real code does the second half with `extended_with` instead, so there
/// is no shared code keeping them honest. This test asks the REAL
/// `VariantArena::build` for a verdict on each planned query and asserts
/// it matches the oracle's.
///
/// **It does not currently discriminate, and that was checked rather
/// than assumed.** Two perturbations of the oracle's arm were tried and
/// NEITHER reddens anything in this file:
///
/// - dropping `common` from `common ++ tail` — masked, because the
///   `Plan::MetricBinary` arm already compiles the scan plan's client
///   pipeline through `MetricNode::leaves()`, and for a variants node
///   that IS the common pipeline;
/// - dropping the tail — masked, because `build_variants_node` now
///   compiles each variant's whole own pipeline (tail included) at plan
///   time for #247, so a malformed tail is refused before any arena is
///   built.
///
/// So the arm is redundant with two other paths today, and this test is a
/// TRIPWIRE for when that stops being true rather than a check that
/// currently proves the two agree. It is not a code-sharing refactor on
/// purpose: `extended_with` exists to avoid recompiling the common
/// pipeline's regexes once per tail (`pipeline.rs:1155-1162`), so making
/// the oracle call it would couple this fixture to a performance
/// mechanism instead of to the rule.
///
/// **Carried to #397.** When a variant gets a live pipeline of its own,
/// both masks come off — the arm becomes load-bearing, and whoever does
/// that work must make this test discriminate (or delete the arm in
/// favour of the real one) rather than trusting it as it stands. Tails
/// are already non-empty for `unwrap` variants (measured: `sum_over_time(…
/// | unwrap v | v > 1 …)` plans a two-stage tail), so the assembly this
/// arm performs is real code, not a placeholder.
#[test]
fn the_oracle_agrees_with_the_real_variant_arena() {
    for query in [
        // accepted, no tail
        r#"variants(count_over_time({service_name="m"} [5m])) of ({service_name="m"} [5m])"#,
        // accepted, with an unwrap tail (the shape whose tail is non-empty)
        r#"variants(sum_over_time({service_name="m"} | logfmt | unwrap v [5m])) of ({service_name="m"} | logfmt [5m])"#,
        // accepted, common pipeline carrying an extraction
        r#"variants(count_over_time({service_name="m"} [5m])) of ({service_name="m"} | logfmt a="b" [5m])"#,
        // refused: the common pipeline's extraction expression
        r#"variants(count_over_time({service_name="m"} [5m])) of ({service_name="m"} | logfmt a="b.c" [5m])"#,
        // two variants, one with a tail
        r#"variants(count_over_time({service_name="m"} [5m]), sum_over_time({service_name="m"} | logfmt | unwrap v [5m])) of ({service_name="m"} | logfmt [5m])"#,
    ] {
        let (ours, why) = pulsus_verdict(query);
        // The engine's own answer, from the code the oracle mirrors.
        let expr = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
        let planned = match plan(&expr, &params(), &ctx()) {
            Ok(p) => p,
            Err(_) => {
                // The plan already refused it, so there is no arena to
                // build and both sides agree by construction.
                assert_eq!(ours, Verdict::Reject, "{query}: {why}");
                continue;
            }
        };
        let Plan::MetricBinary(node) = &planned else {
            panic!("{query}: a variants query must plan to a node tree");
        };
        let mut engine = Verdict::Accept;
        pulsus_logql::walk::preorder::<MetricNodeScc>(node, |n| {
            if let MetricNode::Variants { scan, variants, .. } = n {
                let common: Vec<pulsus_logql::Stage> = scan
                    .client
                    .as_ref()
                    .map(|c| c.pipeline.clone())
                    .unwrap_or_default();
                if VariantArena::build(&common, variants, MAX_VARIANT_FANOUT_STATE_BYTES, 0)
                    .is_err()
                {
                    engine = Verdict::Reject;
                }
            }
        });
        assert_eq!(
            ours, engine,
            "{query}: the matrix oracle says {ours:?} ({why}) but `VariantArena::build` says \
             {engine:?}. The oracle's variants arm has drifted from the code it mirrors — see \
             this test's doc comment and issue #397."
        );
    }
}

/// **The compile sites are enumerated from the code that CALLS the
/// compiler, not from a reading of the plan types.** Twelve query
/// positions were enumerated carefully for #247 and still missed
/// `variants.rs:509`, because that enumeration started from "where can a
/// `| logfmt` sit" instead of "who compiles a pipeline".
///
/// **There are TWO ways into the compiler, and both are scanned.**
/// `CompiledPipeline::compile` is the obvious one. The other is
/// `CompiledPipeline::extended_with` (`pipeline.rs:1163`), which appends
/// stages to an already-compiled pipeline through the same
/// `compile_stage` the `compile` loop uses (`pipeline.rs:1180`'s
/// `from_parts` is their shared assembly point) — so it compiles user
/// stages without ever mentioning `compile`. That is precisely the path
/// `VariantArena::build` takes for a variant's unwrap tail, i.e. the
/// path whose omission caused this round: a census scanning only for
/// `compile(` would have reported this file complete while blind to it.
/// `variants.rs`'s own `variants_exec_census` pins `extended_with` at
/// exactly one call site; this one pins WHERE both entry points are,
/// across the whole module.
#[test]
fn the_compile_sites_are_enumerated_from_the_callers_of_the_compiler() {
    /// `(file, compile calls, extended_with calls, why it is or is not a
    /// matrix position)`.
    const SITES: &[(&str, usize, usize, &str)] = &[
        (
            "detected.rs",
            2,
            0,
            "the two process-wide bare `LazyLock` parsers — no user input reaches them, and \
             their extraction lists are empty by construction",
        ),
        (
            "exec.rs",
            4,
            0,
            "streams :612, metric :906 (incl. every binary leaf), detected_fields :2290, tail \
             :2576. `POSITIONS` reaches the first TWO of these and no more: every position is \
             a `query_range`-shaped log or metric query, so none of them runs \
             `detected_fields` or `tail`. `/detected_fields` is covered instead by \
             `live_surface_axis_agrees`'s route table, and `/tail` is named in this file's \
             module docs as unmeasured (WebSocket) with the reason",
        ),
        (
            "plan.rs",
            1,
            0,
            "`build_variants_node` compiles a variant's OWN pipeline purely to validate it \
             (issue #247 round 2, mirroring `evaluator.go:1417 @ v3.7.4`) — the \
             `variants_variant_side` position covers it, and `Refusal::PlanTime` records that \
             it refuses here rather than at a pipeline compile",
        ),
        (
            "variants.rs",
            1,
            1,
            "`VariantArena::build` :509 compiles the COMMON pipeline and :536 extends it with \
             each variant's unwrap tail — the `variants_common_side` position covers the \
             common half; the tails carry no extraction expression of their own because \
             `variant_tail` starts at the `unwrap`",
        ),
    ];

    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql");
    let mut found: Vec<(String, usize, usize)> = Vec::new();
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    files.sort();
    for path in &files {
        let src = std::fs::read_to_string(path).expect("source");
        // Everything above the file's `#[cfg(test)]` marker, the same
        // split `charge.rs`'s censuses use.
        let production = match src.find("\n#[cfg(test)]") {
            Some(i) => &src[..i],
            None => src.as_str(),
        };
        // COMMENT TEXT STRIPPED, the `variants_exec_census` precedent:
        // these files document their own compile sites heavily, and a
        // census that counted its own prose would move whenever someone
        // edited a doc comment.
        let production: String = production
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let production = production.as_str();
        let compiles = production.matches("CompiledPipeline::compile(").count();
        // `extended_with` is scanned on the method name alone, because the
        // receiver is a local (`pipelines[0]`) and naming the type would
        // miss it — which is the whole failure this arm exists for. The
        // DEFINITION lives in `pipeline.rs` and is excluded by matching
        // the call form `.extended_with(`.
        let extends = production.matches(".extended_with(").count();
        if compiles > 0 || extends > 0 {
            found.push((
                path.file_name()
                    .expect("file name")
                    .to_string_lossy()
                    .into_owned(),
                compiles,
                extends,
            ));
        }
    }
    let named: Vec<(String, usize, usize)> = SITES
        .iter()
        .map(|(f, c, e, _)| ((*f).to_string(), *c, *e))
        .collect();
    assert_eq!(
        found, named,
        "the set of pipeline-COMPILER call sites in `src/logql` has changed. Every site is a \
         place the logfmt sub-grammar can be skipped, so add it to `SITES` **and** decide \
         whether `POSITIONS` must cover it. Both entry points count: `compile(` and \
         `.extended_with(`, the latter because it reaches `compile_stage` without naming \
         `compile` — that is how `variants.rs` was missed."
    );
}

/// **The fixture discriminates, and it accounts for the whole effect of
/// this change** — not only for what it closes.
///
/// Each point is classified by comparing three verdicts: the rule
/// PulsusDB shipped at `7980344`, the rule it ships now, and the
/// reference's. That gives four disjoint buckets, and the test asserts
/// they partition the matrix, so no point can fall out of the accounting:
///
/// - **CLOSED** — the pre-#247 tree disagreed with the reference and we
///   now agree. This is what the issue is for.
/// - **STILL OPEN** — both trees disagree with the reference. Asserted
///   EMPTY. It held the `variants_variant_side` points until round 2
///   made `build_variants_node` validate a variant's own pipeline; the
///   assertion is what stops that bucket refilling.
/// - **INTRODUCED** — the pre-#247 tree AGREED with the reference and we
///   now do not. The `variants_common_side` points, where the reference
///   answers 200-empty and we answer 400. Recorded here rather than
///   buried, because a change that fixes N points and breaks M must
///   report M.
/// - **UNMOVED** — the two trees give the same verdict. Split into the
///   points already refused at `7980344` (which pin a rule and
///   discriminate nothing) and the accept-side controls (which show the
///   new rule does not over-refuse).
#[test]
fn the_pre_247_rule_disagrees_wherever_the_reference_refuses_an_expression() {
    let points = matrix();
    let (mut closed, mut still_open, mut introduced) = (Vec::new(), Vec::new(), Vec::new());
    let (mut already_rejecting, mut controls) = (Vec::new(), 0usize);
    for p in &points {
        let (before, now, theirs) = (p.layer.pre_247(), p.pulsus(), p.reference());
        match (before == theirs, now == theirs) {
            (false, true) => {
                assert_eq!(
                    (before, theirs),
                    (Verdict::Accept, Verdict::Reject),
                    "a closed point in the other direction would be a different bug: {} {}",
                    p.label,
                    p.query
                );
                closed.push(p.label.as_str());
            }
            (false, false) => still_open.push(p.label.as_str()),
            (true, false) => introduced.push(p.label.as_str()),
            (true, true) => {
                if theirs == Verdict::Reject {
                    already_rejecting.push(p.label.as_str());
                } else {
                    controls += 1;
                }
            }
        }
    }
    assert_eq!(
        closed.len() + still_open.len() + introduced.len() + already_rejecting.len() + controls,
        points.len(),
        "the four buckets must partition the matrix"
    );
    // CLOSED is exactly the sub-grammar and dangling-comma points at
    // every position whose outcome is `Agrees` — computed from the
    // table, never restated as a literal.
    let expected_closed = points
        .iter()
        .filter(|p| {
            matches!(p.layer, Layer::SubGrammar | Layer::DanglingComma)
                && p.outcome == Outcome::Agrees
        })
        .count();
    assert_eq!(
        closed.len(),
        expected_closed,
        "#247 must close every sub-grammar and dangling-comma point at an agreeing position"
    );
    // And the two exception buckets are exactly their positions'
    // sub-grammar points, both asserted rather than described.
    let sub_grammar = EXPRESSIONS
        .iter()
        .filter(|e| e.layer == Layer::SubGrammar)
        .count();
    // STILL OPEN must be EMPTY: #247 closes every point where the
    // reference refuses and the pre-#247 tree did not, including the
    // `variants_variant_side` ones that round 2 added.
    assert!(
        still_open.is_empty(),
        "#247 leaves {} point(s) where the reference refuses and we do not: {still_open:?}",
        still_open.len()
    );
    assert_eq!(introduced.len(), sub_grammar, "INTRODUCED: {introduced:?}");
    assert!(
        introduced
            .iter()
            .all(|l| l.starts_with("variants_common_side/")),
        "an introduced divergence outside the common side: {introduced:?}"
    );
    eprintln!(
        "of {} matrix points: {} CLOSED by #247; {} STILL OPEN ({}); {} INTRODUCED ({}); {} \
         already refused on both trees at 7980344, which pin a rule and DISCRIMINATE NOTHING \
         ({}); {} accept-side controls.",
        points.len(),
        closed.len(),
        still_open.len(),
        still_open.join(", "),
        introduced.len(),
        introduced.join(", "),
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
        if theirs != p.reference() {
            disagree.push(format!(
                "{} {}\n    committed={:?} container={:?} {}",
                p.label,
                p.query,
                p.reference(),
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
