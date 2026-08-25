//! Issue #388, Stage B (folding in #394): the accept/reject surface of a
//! `| json <id>="<expr>"` extraction expression, measured as a matrix and
//! checked in so it can be re-run.
//!
//! **The issue's title points one way and this half points the other, so
//! read this first.** #388 is filed as "the reference rejects and we
//! accept". For the json sub-grammar the harmful direction is the
//! opposite: at `5d91ef1` PulsusDB **refused four expressions the
//! reference serves** — `arr[ 0 ]`, `arr[0 ]`, `b[ "c" ]` and
//! `[ "b-c" ]`, all 200 there and extracting the right value — and
//! mis-resolved two more (`b . c` and `b.c ` are 200 on both sides,
//! extracting `nested` there and `""` here). A user moving a working
//! query to PulsusDB lost it with no warning. Both directions close with
//! one change, because both come from the same cause: we had a
//! hand-rolled approximation where the reference has a grammar
//! (`pkg/logql/log/jsonexpr/ @ v3.7.4`).
//!
//! **Why this file and `logql_pattern_expr_matrix.rs` cannot share a
//! harness, measured.** A json-expression rejection is a `Stage()` error
//! (`NewJSONExpressionParser`, `pkg/logql/log/parser.go:634-651 @
//! v3.7.4`), so it is **window-dependent**: over a window ending 24 h
//! back the pinned container answers 200 for `| json v="b-c"`. A pattern
//! rejection is raised inside `ParseExpr` and stays 400 in every window.
//! [`live_the_json_rule_is_window_dependent`] measures this half;
//! `logql_pattern_expr_matrix.rs` measures the other. **Every live leg
//! here therefore uses a window ending at `now`**, and a leg captured
//! against a stale window would have recorded the whole sub-grammar as
//! accepted and passed for ever.
//!
//! **The tests, and what each is authority for.**
//!
//! - [`pulsus_agrees_with_the_captured_reference_verdicts`] — hermetic,
//!   the whole matrix, at the layer a user meets it: parse → plan → the
//!   pipeline compile that runs before any I/O.
//! - [`the_pre_388_column_is_reproduced_by_the_replayed_rule`] — the
//!   frozen `5d91ef1` baseline, re-derived by [`pre_388`] rather than
//!   asserted. This is the substitute for a committed baseline file:
//!   LogQL has no scoreboard artefact, so the freeze is the pre-change
//!   rule as a replayable function plus the branch's commit ordering.
//! - [`the_rule_table_has_one_row_per_line_of_the_reference_enumeration`]
//!   — the rule table against the committed literal `git grep` output.
//! - [`the_sites_dataset_is_regenerated_not_retyped`] — the committed
//!   `logql_json_expr_sites.tsv`, rendered from the tables and the tree
//!   sweep, never hand-edited.
//! - [`live_matrix_against_the_reference`] — gated on
//!   `PULSUSDB_LOGQL_DIFF_URL`; every point's status AND the
//!   reference's own inner error text.
//!
//! **The dataset's Site rows are GENERATED from the sweep, where the
//! pattern matrix's are a hand list cross-checked against it.** That
//! difference is a size one and nothing else: sweep A finds 15 pattern
//! arguments and 115 json expressions. A 115-row hand list would be
//! retyped rather than measured, which is the failure mode the generated
//! form exists to prevent.
//!
//! **Excluded by name.** What a SURVIVING expression resolves to is not
//! this file's subject (`b23_json_raw_read.test` and #389 own the read
//! path). The one evaluation change adopted here is the one the grammar
//! produces for free — a whitespace-bearing expression resolves to the
//! reference's path, so `b . c` reads `["b","c"]` — and it is pinned in
//! `logqltest/corpus/b26_json_expr.test`, not here.

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
// The reference's rule table.
// ---------------------------------------------------------------------

/// How a line of the committed enumeration relates to an error a caller
/// can observe. Derived by READING the line; no check computes it from
/// source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrClass {
    /// Produces an error a caller observes.
    Producer,
    /// Re-wraps another producer's error into `sc.err`.
    Relay,
}

/// Why a `Producer` row carries no probe. A CLOSED set; the two reasons
/// are different claims and are never collapsed into one excuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WhyNot {
    /// No input reaches the line at all.
    UnreachableByAnyInput,
}

/// Where a rule row came from. The distinction is load-bearing: the
/// committed enumeration is a **bounded inventory** whose scope is a
/// grep spelling, and `FoundByProbe` is the concrete instance of the
/// residue that bound names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// A line of `logql_json_expr_reference_error_sites.txt`.
    CommandOutput,
    /// NOT in that file, and no command in this repository can put it
    /// there: the error is created inside `strconv`, outside both
    /// packages, and relayed into `sc.err` by `lexer.go:56`.
    FoundByProbe,
}

struct RefRule {
    site: &'static str,
    class: ErrClass,
    source: Source,
    why_not: Option<WhyNot>,
    note: &'static str,
}

const REF_RULES: &[RefRule] = &[
    RefRule {
        site: "lexer.go:26",
        class: ErrClass::Producer,
        source: Source::CommandOutput,
        why_not: None,
        note: "the yacc Error callback. LAST WRITER WINS: a syntax error OVERWRITES a lexer \
               error raised in the same parse, so `b[0` reports the syntax error and not \
               `non-integer value`",
    },
    RefRule {
        site: "lexer.go:56",
        class: ErrClass::Relay,
        source: Source::CommandOutput,
        why_not: None,
        note: "relays scanInt's error into sc.err -- :138, :147 AND strconv.Atoi's range error",
    },
    RefRule {
        site: "lexer.go:80",
        class: ErrClass::Producer,
        source: Source::CommandOutput,
        why_not: None,
        note: "a byte no token can start with: `unexpected char <c>`",
    },
    RefRule {
        site: "lexer.go:114",
        class: ErrClass::Producer,
        source: Source::CommandOutput,
        why_not: Some(WhyNot::UnreachableByAnyInput),
        note: "scanStr's defensive `r != '\"'` branch. scanStr is entered ONLY after unread() of \
               a `\"` (lexer.go:75-76), so no input reaches it. THE ONE ROW IN EITHER TABLE WITH \
               NO EMPIRICAL SUPPORT OF ANY KIND -- it rests on a reading, and is listed as such \
               rather than counted with the rest",
    },
    RefRule {
        site: "lexer.go:138",
        class: ErrClass::Producer,
        source: Source::CommandOutput,
        why_not: None,
        note: "`cannot use float as array index`. Its text SURVIVES whenever the parser can \
               accept what it already shifted -- reasoning said it was always overwritten, and \
               `b 1.5` proved otherwise",
    },
    RefRule {
        site: "lexer.go:147",
        class: ErrClass::Producer,
        source: Source::CommandOutput,
        why_not: None,
        note: "`non-integer value: <c>`. Witnessed by `b 1x`; `b 0` reaches it too but formats \
               the NUL that read() returns at end of input with %c, and the response body \
               genuinely carries that unprintable byte -- verified on the wire, which is why \
               the printable witness is the committed one",
    },
    RefRule {
        site: "strconv/atoi.go:ErrRange",
        class: ErrClass::Producer,
        source: Source::FoundByProbe,
        why_not: None,
        note: "REACHABLE AND OUTSIDE THE ENUMERATION'S SCOPE. scanInt ends with \
               `return strconv.Atoi(...)` (lexer.go:153), whose range error is relayed by \
               :56 and survives when the parser can accept the prefix. Measured: \
               `b 9223372036854775808]` is 400 with that text, and `b[9223372036854775808]` is \
               400 with the syntax error that overwrites it, while `b[9223372036854775807]` is \
               200. So the index bound is Go's `int`, i.e. i64 -- a usize parse would have \
               ACCEPTED 2^63 and introduced a divergence this change was supposed to close. \
               Found by probing, not by the command; recorded here so the count is honest",
    },
];

// ---------------------------------------------------------------------
// Table A -- the extraction EXPRESSION, crossed with every position.
// ---------------------------------------------------------------------

/// Which reference rule refuses an expression, or that it is served. The
/// value names the `REF_RULES` row it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    Accepted,
    /// `lexer.go:26` — the yacc callback, which overwrites any lexer
    /// error raised in the same parse.
    Syntax,
    /// `lexer.go:80`.
    UnexpectedChar,
    /// `lexer.go:138`.
    FloatIndex,
    /// `lexer.go:147`.
    NonIntegerIndex,
    /// `strconv/atoi.go`'s `ErrRange`, relayed by `lexer.go:56`.
    IndexOutOfRange,
}

impl Rule {
    fn site(self) -> &'static str {
        match self {
            Rule::Accepted => "-",
            Rule::Syntax => "lexer.go:26",
            Rule::UnexpectedChar => "lexer.go:80",
            Rule::FloatIndex => "lexer.go:138",
            Rule::NonIntegerIndex => "lexer.go:147",
            Rule::IndexOutOfRange => "strconv/atoi.go:ErrRange",
        }
    }

    fn reference(self) -> Verdict {
        match self {
            Rule::Accepted => Verdict::Accept,
            _ => Verdict::Reject,
        }
    }
}

/// One extraction expression.
///
/// `quoted` is what a user types between the double quotes of
/// `| json v="…"`; `decoded` is what the reference's `jsonexpr.Parse`
/// therefore sees, and [`the_fixtures_two_expression_columns_agree`]
/// derives the second from the first through our own lexer.
///
/// `pre` is PulsusDB's verdict at `5d91ef1`, the frozen baseline;
/// `reference_text` is the pinned container's own inner message, i.e.
/// what follows `cannot parse expression [<expr>]: ` in the body.
///
/// **`reference_text` is compared byte for byte by the live leg for
/// every rule but `IndexOutOfRange`**, whose reference text names a Go
/// standard-library function (`strconv.Atoi: parsing "…": value out of
/// range`). We match the VERDICT there and word the message ourselves;
/// the column keeps the capture so the divergence is visible rather than
/// absent.
struct Expression {
    name: &'static str,
    quoted: &'static str,
    decoded: &'static str,
    rule: Rule,
    pre: Verdict,
    reference_text: &'static str,
}

const EXPRESSIONS: &[Expression] = &[
    Expression {
        name: "e_b_sp_c",
        quoted: r#"b c"#,
        decoded: r#"b c"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected FIELD"#,
    },
    Expression {
        name: "e_b_dash_c",
        quoted: r#"b-c"#,
        decoded: r#"b-c"#,
        rule: Rule::UnexpectedChar,
        pre: Verdict::Accept,
        reference_text: r#"unexpected char -"#,
    },
    Expression {
        name: "empty",
        quoted: r#""#,
        decoded: r#""#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected $end, expecting LSB or FIELD"#,
    },
    Expression {
        name: "e_0b",
        quoted: r#"0b"#,
        decoded: r#"0b"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected $end, expecting LSB or FIELD"#,
    },
    Expression {
        name: "e_b_lsb_c_rsb",
        quoted: r#"b[c]"#,
        decoded: r#"b[c]"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected FIELD, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_dot_dot_c",
        quoted: r#"b..c"#,
        decoded: r#"b..c"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected DOT, expecting FIELD"#,
    },
    Expression {
        name: "e_dot_b",
        quoted: r#".b"#,
        decoded: r#".b"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected DOT, expecting LSB or FIELD"#,
    },
    Expression {
        name: "e_b_dot",
        quoted: r#"b."#,
        decoded: r#"b."#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected $end, expecting FIELD"#,
    },
    Expression {
        name: "e_b_lsb_0",
        quoted: r#"b[0"#,
        decoded: r#"b[0"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected $end, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_rsb",
        quoted: r#"b]"#,
        decoded: r#"b]"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected RSB"#,
    },
    Expression {
        name: "e_b_slash_c",
        quoted: r#"b/c"#,
        decoded: r#"b/c"#,
        rule: Rule::UnexpectedChar,
        pre: Verdict::Accept,
        reference_text: r#"unexpected char /"#,
    },
    Expression {
        name: "e_b_colon_c",
        quoted: r#"b:c"#,
        decoded: r#"b:c"#,
        rule: Rule::UnexpectedChar,
        pre: Verdict::Accept,
        reference_text: r#"unexpected char :"#,
    },
    Expression {
        name: "e_b_eq_c",
        quoted: r#"b=c"#,
        decoded: r#"b=c"#,
        rule: Rule::UnexpectedChar,
        pre: Verdict::Accept,
        reference_text: r#"unexpected char ="#,
    },
    Expression {
        name: "e_b_comma_c",
        quoted: r#"b,c"#,
        decoded: r#"b,c"#,
        rule: Rule::UnexpectedChar,
        pre: Verdict::Accept,
        reference_text: r#"unexpected char ,"#,
    },
    Expression {
        name: "e_b_bang",
        quoted: r#"b!"#,
        decoded: r#"b!"#,
        rule: Rule::UnexpectedChar,
        pre: Verdict::Accept,
        reference_text: r#"unexpected char !"#,
    },
    Expression {
        name: "e_q_b_q",
        quoted: r#"\"b\""#,
        decoded: r#""b""#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected STRING, expecting LSB or FIELD"#,
    },
    Expression {
        name: "e_b_sp_c_sp_d",
        quoted: r#"b c d"#,
        decoded: r#"b c d"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected FIELD"#,
    },
    Expression {
        name: "e_b_lsb_dash_1_rsb",
        quoted: r#"b[-1]"#,
        decoded: r#"b[-1]"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected $end, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_lsb_1_dot_5_rsb",
        quoted: r#"b[1.5]"#,
        decoded: r#"b[1.5]"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected $end, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_lsb_sq_c_sq_rsb",
        quoted: r#"b['c']"#,
        decoded: r#"b['c']"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected $end, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_dot_0",
        quoted: r#"b.0"#,
        decoded: r#"b.0"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected $end, expecting FIELD"#,
    },
    Expression {
        name: "e_eacute",
        quoted: r#"é"#,
        decoded: r#"é"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected $end, expecting LSB or FIELD"#,
    },
    Expression {
        name: "e_b_eacute",
        quoted: r#"bé"#,
        decoded: r#"bé"#,
        rule: Rule::UnexpectedChar,
        pre: Verdict::Accept,
        reference_text: r#"unexpected char é"#,
    },
    Expression {
        name: "e_arr_lsb_sp_0_sp_rsb",
        quoted: r#"arr[ 0 ]"#,
        decoded: r#"arr[ 0 ]"#,
        rule: Rule::Accepted,
        pre: Verdict::Reject,
        reference_text: r#""#,
    },
    Expression {
        name: "e_arr_lsb_0_sp_rsb",
        quoted: r#"arr[0 ]"#,
        decoded: r#"arr[0 ]"#,
        rule: Rule::Accepted,
        pre: Verdict::Reject,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_lsb_sp_q_c_q_sp_rsb",
        quoted: r#"b[ \"c\" ]"#,
        decoded: r#"b[ "c" ]"#,
        rule: Rule::Accepted,
        pre: Verdict::Reject,
        reference_text: r#""#,
    },
    Expression {
        name: "e_lsb_sp_q_b_dash_c_q_sp_rsb",
        quoted: r#"[ \"b-c\" ]"#,
        decoded: r#"[ "b-c" ]"#,
        rule: Rule::Accepted,
        pre: Verdict::Reject,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b",
        quoted: r#"b"#,
        decoded: r#"b"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_dot_c",
        quoted: r#"b.c"#,
        decoded: r#"b.c"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_lsb_0_rsb",
        quoted: r#"b[0]"#,
        decoded: r#"b[0]"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_lsb_q_c_q_rsb",
        quoted: r#"b[\"c\"]"#,
        decoded: r#"b["c"]"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_lsb_0_rsb_lsb_1_rsb",
        quoted: r#"b[0][1]"#,
        decoded: r#"b[0][1]"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b0",
        quoted: r#"b0"#,
        decoded: r#"b0"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_2",
        quoted: r#"_b"#,
        decoded: r#"_b"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_3",
        quoted: r#"B"#,
        decoded: r#"B"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_c",
        quoted: r#"b_c"#,
        decoded: r#"b_c"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_lsb_q_c_sp_d_q_rsb",
        quoted: r#"b[\"c d\"]"#,
        decoded: r#"b["c d"]"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_sp_1_dot_5",
        quoted: r#"b 1.5"#,
        decoded: r#"b 1.5"#,
        rule: Rule::FloatIndex,
        pre: Verdict::Accept,
        reference_text: r#"cannot use float as array index"#,
    },
    Expression {
        name: "e_b_lsb_0_rsb_sp_1_dot_5",
        quoted: r#"b[0] 1.5"#,
        decoded: r#"b[0] 1.5"#,
        rule: Rule::FloatIndex,
        pre: Verdict::Accept,
        reference_text: r#"cannot use float as array index"#,
    },
    Expression {
        name: "e_b_dot_c_sp_1_dot_5",
        quoted: r#"b.c 1.5"#,
        decoded: r#"b.c 1.5"#,
        rule: Rule::FloatIndex,
        pre: Verdict::Accept,
        reference_text: r#"cannot use float as array index"#,
    },
    Expression {
        name: "e_b_sp_1x",
        quoted: r#"b 1x"#,
        decoded: r#"b 1x"#,
        rule: Rule::NonIntegerIndex,
        pre: Verdict::Accept,
        reference_text: r#"non-integer value: x"#,
    },
    Expression {
        name: "e_b_sp_99999999999999999999_sp",
        quoted: r#"b 99999999999999999999 "#,
        decoded: r#"b 99999999999999999999 "#,
        rule: Rule::IndexOutOfRange,
        pre: Verdict::Accept,
        reference_text: r#"strconv.Atoi: parsing "99999999999999999999": value out of range"#,
    },
    Expression {
        name: "e_b_lsb_99999999999999999999_rsb",
        quoted: r#"b[99999999999999999999]"#,
        decoded: r#"b[99999999999999999999]"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected $end, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_sp_9223372036854775807_rsb",
        quoted: r#"b 9223372036854775807]"#,
        decoded: r#"b 9223372036854775807]"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected INDEX"#,
    },
    Expression {
        name: "e_b_sp_9223372036854775808_rsb",
        quoted: r#"b 9223372036854775808]"#,
        decoded: r#"b 9223372036854775808]"#,
        rule: Rule::IndexOutOfRange,
        pre: Verdict::Accept,
        reference_text: r#"strconv.Atoi: parsing "9223372036854775808": value out of range"#,
    },
    Expression {
        name: "e_b_sp_dot_sp_c",
        quoted: r#"b . c"#,
        decoded: r#"b . c"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_dot_c_sp",
        quoted: r#"b.c "#,
        decoded: r#"b.c "#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_arr_lsb_00_rsb",
        quoted: r#"arr[00]"#,
        decoded: r#"arr[00]"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_lsb_0_rsb",
        quoted: r#"[0]"#,
        decoded: r#"[0]"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_lsb_0_rsb_lsb_1_rsb",
        quoted: r#"[0][1]"#,
        decoded: r#"[0][1]"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_lsb_0_rsb_dot_c",
        quoted: r#"b[0].c"#,
        decoded: r#"b[0].c"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_sp_q_c_q",
        quoted: r#"b \"c\""#,
        decoded: r#"b "c""#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected STRING"#,
    },
    Expression {
        name: "e_b_lsb_rsb",
        quoted: r#"b[]"#,
        decoded: r#"b[]"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected RSB, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_lsb_lsb",
        quoted: r#"b[["#,
        decoded: r#"b[["#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected LSB, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_lsb_dot",
        quoted: r#"b[."#,
        decoded: r#"b[."#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected DOT, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_lsb_q_c_q_sp",
        quoted: r#"b[\"c\" "#,
        decoded: r#"b["c" "#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected $end, expecting RSB"#,
    },
    Expression {
        name: "e_b_lsb_0_sp_0_rsb",
        quoted: r#"b[0 0]"#,
        decoded: r#"b[0 0]"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected INDEX, expecting RSB"#,
    },
    Expression {
        name: "e_b_dot_lsb_0_rsb",
        quoted: r#"b.[0]"#,
        decoded: r#"b.[0]"#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected LSB, expecting FIELD"#,
    },
    Expression {
        name: "e_b_dot_rsb",
        quoted: r#"b.]"#,
        decoded: r#"b.]"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected RSB, expecting FIELD"#,
    },
    Expression {
        name: "e_b_dot_q_c_q",
        quoted: r#"b.\"c\""#,
        decoded: r#"b."c""#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected STRING, expecting FIELD"#,
    },
    Expression {
        name: "e_b_lsb_0_rsb_lsb",
        quoted: r#"b[0]["#,
        decoded: r#"b[0]["#,
        rule: Rule::Syntax,
        pre: Verdict::Reject,
        reference_text: r#"syntax error: unexpected $end, expecting STRING or INDEX"#,
    },
    Expression {
        name: "e_b_lsb_0_rsb_rsb",
        quoted: r#"b[0]]"#,
        decoded: r#"b[0]]"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected RSB"#,
    },
    Expression {
        name: "e_rsb",
        quoted: r#"]"#,
        decoded: r#"]"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected RSB, expecting LSB or FIELD"#,
    },
    Expression {
        name: "e_0_sp",
        quoted: r#"0 "#,
        decoded: r#"0 "#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected INDEX, expecting LSB or FIELD"#,
    },
    Expression {
        name: "space_only",
        quoted: r#" "#,
        decoded: r#" "#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected $end, expecting LSB or FIELD"#,
    },
    Expression {
        name: "e_b_lsb_9223372036854775807_rsb",
        quoted: r#"b[9223372036854775807]"#,
        decoded: r#"b[9223372036854775807]"#,
        rule: Rule::Accepted,
        pre: Verdict::Accept,
        reference_text: r#""#,
    },
    Expression {
        name: "e_b_lsb_9223372036854775808_rsb",
        quoted: r#"b[9223372036854775808]"#,
        decoded: r#"b[9223372036854775808]"#,
        rule: Rule::Syntax,
        pre: Verdict::Accept,
        reference_text: r#"syntax error: unexpected $end, expecting STRING or INDEX"#,
    },
];

// ---------------------------------------------------------------------
// Positions.
// ---------------------------------------------------------------------

/// Whether PulsusDB's verdict at a position matches the reference's,
/// and when it does not, WHICH divergence it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// PulsusDB's verdict equals the reference's.
    Agrees,
    /// **Ledgered divergence, deliberate, and inherited from #247 rather
    /// than introduced here.** The reference answers 200 with an empty
    /// result for a malformed COMMON `of (...)` pipeline: the build error
    /// happens behind `SelectSamples` and is swallowed, so the user reads
    /// "no logs matched" where the query is broken. PulsusDB answers 400.
    /// Measured on the pinned container: the same shape with
    /// `| pattern "<a> <a>"` is 400 there, because a pattern rejection is
    /// a `ParseExpr` error and never reaches the swallowing path — which
    /// is the sharpest available demonstration that the two stages sit at
    /// different layers.
    CommonSideRefusedHere,
}

/// Where the `| json` extraction sits. `{Q}` is the expression's
/// `quoted` text, dropped into `v="{Q}"`.
struct Position {
    name: &'static str,
    template: &'static str,
    outcome: Outcome,
}

const POSITIONS: &[Position] = &[
    Position {
        name: "streams",
        template: r#"{service_name="m"} | json v="{Q}""#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "streams_second_stage",
        template: r#"{service_name="m"} | logfmt | json v="{Q}""#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "streams_second_extraction",
        template: r#"{service_name="m"} | json ok="x", v="{Q}""#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "streams_after_line_filter",
        template: r#"{service_name="m"} |= "x" | json v="{Q}""#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "count_over_time",
        template: r#"count_over_time({service_name="m"} | json v="{Q}" [5m])"#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "sum_by",
        template: r#"sum by (v) (count_over_time({service_name="m"} | json v="{Q}" [5m]))"#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "unwrap",
        template: r#"sum_over_time({service_name="m"} | json v="{Q}" | unwrap x [5m])"#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "topk",
        template: r#"topk(3, count_over_time({service_name="m"} | json v="{Q}" [5m]))"#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "label_replace",
        template: r#"label_replace(count_over_time({service_name="m"} | json v="{Q}" [5m]), "d", "$1", "v", "(.*)")"#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "binary_lhs",
        template: r#"count_over_time({service_name="m"} | json v="{Q}" [5m]) + count_over_time({service_name="m"} [5m])"#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "binary_rhs",
        template: r#"count_over_time({service_name="m"} [5m]) + count_over_time({service_name="m"} | json v="{Q}" [5m])"#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "variants_variant_side",
        template: r#"variants(count_over_time({service_name="m"} | json v="{Q}" [5m])) of ({service_name="m"} [5m])"#,
        outcome: Outcome::Agrees,
    },
    Position {
        name: "variants_common_side",
        template: r#"variants(count_over_time({service_name="m"} [5m])) of ({service_name="m"} | json v="{Q}" [5m])"#,
        outcome: Outcome::CommonSideRefusedHere,
    },
];

/// One matrix point.
struct Point {
    label: String,
    query: String,
    rule: Rule,
    outcome: Outcome,
    pre: Verdict,
    reference_text: &'static str,
}

impl Point {
    /// What the pinned container answers, from the captured table.
    fn reference(&self) -> Verdict {
        match self.outcome {
            Outcome::CommonSideRefusedHere => Verdict::Accept,
            Outcome::Agrees => self.rule.reference(),
        }
    }

    /// What PulsusDB answers: the reference's verdict at every point,
    /// INCLUDING the one position where the reference itself answers
    /// 200 — see [`Outcome::CommonSideRefusedHere`], which is the single
    /// recorded divergence and is inherited from #247 rather than
    /// introduced here.
    fn pulsus(&self) -> Verdict {
        self.rule.reference()
    }
}

fn matrix() -> Vec<Point> {
    let mut out = Vec::new();
    for position in POSITIONS {
        for e in EXPRESSIONS {
            out.push(Point {
                label: format!("{}/{}", position.name, e.name),
                query: position.template.replace("{Q}", e.quoted),
                rule: e.rule,
                outcome: position.outcome,
                pre: e.pre,
                reference_text: e.reference_text,
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

/// PulsusDB's verdict **at the layer the accept/reject decision is
/// actually made**: parse, then plan, then the pipeline compile `exec`
/// performs before any I/O.
///
/// **This is NOT the HTTP response, and the rejection texts recorded in
/// this file are CHAIN-DERIVED rather than observed frames.** What runs
/// here stops at the compile error's `Display`. A real `400` body is the
/// far end of a longer chain than that, every link of which is outside
/// this crate:
///
/// `ReadError::PipelineInvalid`'s `Display` (the bare reason, pinned by
/// `logql::error`'s `pipeline_invalid_display_is_the_bare_reason_byte_exact`)
/// -> `logs_api::error::read_error_parts` (maps the variant to `400`)
/// -> `ApiError::into_response` -> `plain_text_error` (which is what sets
/// `text/plain; charset=utf-8` and `nosniff`) -> then the timeout,
/// trace, compression and CORS layers the router wraps around it.
///
/// No test in this crate asserts that frame, and capturing one is
/// deliberately out of scope. Two consequences a reader should not have
/// to rediscover: a claim about the body BYTES or the response HEADERS
/// for one of these queries is not established here, and a server whose
/// ClickHouse pool is not up answers `503` from `ApiError::PoolUnavailable`
/// before pipeline compilation is reached at all — so the naive way of
/// capturing such a frame does not exercise this path.
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

/// `parse_json_path` as it stood at `5d91ef1` (`pipeline.rs:2576-2629`),
/// reproduced verbatim apart from returning a [`Verdict`].
///
/// This is the baseline freeze: not a committed scoreboard file (LogQL
/// has none) but the pre-change rule as a replayable function, the form
/// #247 established, tied to reality by the branch's FIRST commit — which
/// asserted the real `parse → plan → compile` chain against the `pre`
/// column with production code untouched.
fn pre_388(expr: &str) -> Verdict {
    let mut segs = 0usize;
    let bytes = expr.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                if segs == 0 {
                    return Verdict::Reject; // leading '.'
                }
                i += 1;
                if i >= bytes.len() || bytes[i] == b'.' || bytes[i] == b'[' {
                    return Verdict::Reject; // '.' must be followed by a field name
                }
            }
            b'[' => {
                let Some(close) = expr[i..].find(']').map(|off| i + off) else {
                    return Verdict::Reject; // unclosed '['
                };
                let inner = &expr[i + 1..close];
                // A quoted key OR a `usize` index; anything else was
                // "index must be a number or a quoted key". The two
                // accepting arms pushed DIFFERENT segments in the
                // original and both are one segment here, since this
                // replay decides a verdict and not a path.
                let quoted = inner
                    .strip_prefix('"')
                    .and_then(|s| s.strip_suffix('"'))
                    .is_some();
                if !quoted && inner.parse::<usize>().is_err() {
                    return Verdict::Reject;
                }
                segs += 1;
                i = close + 1;
            }
            _ => {
                let end = expr[i..]
                    .find(['.', '['])
                    .map(|off| i + off)
                    .unwrap_or(expr.len());
                if end == i {
                    return Verdict::Reject; // empty path segment
                }
                segs += 1;
                i = end;
            }
        }
    }
    if segs == 0 {
        return Verdict::Reject; // empty expression
    }
    Verdict::Accept
}

// ---------------------------------------------------------------------
// Hermetic tests.
// ---------------------------------------------------------------------

/// **The two expression columns agree.** `decoded` is what the
/// reference's sub-grammar sees; this derives it from `quoted` through
/// our own lexer, so a mis-escaped fixture row shows up here rather than
/// as a phantom agreement about the reference.
#[test]
fn the_fixtures_two_expression_columns_agree() {
    for e in EXPRESSIONS {
        let query = format!(r#"{{service_name="m"}} | json v="{}""#, e.quoted);
        let expr = parse(&query).unwrap_or_else(|err| panic!("{}: {query}: {err}", e.name));
        let pulsus_logql::Expr::Log(log) = &expr else {
            panic!("{}: not a log query", e.name);
        };
        let mut found = None;
        for stage in &log.pipeline {
            if let pulsus_logql::Stage::Parser(pulsus_logql::ParserStage::Json { extractions }) =
                stage
            {
                found = extractions.first().map(|x| x.expression.clone());
            }
        }
        assert_eq!(
            found.as_deref(),
            Some(e.decoded),
            "{}: `v=\"{}\"` decodes to {:?}, but the fixture says {:?}",
            e.name,
            e.quoted,
            found,
            e.decoded
        );
    }
}

/// **PulsusDB's verdict is what the table records, point for point.**
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

/// **The frozen baseline is re-derivable, not asserted.**
#[test]
fn the_pre_388_column_is_reproduced_by_the_replayed_rule() {
    for e in EXPRESSIONS {
        assert_eq!(
            pre_388(e.decoded),
            e.pre,
            "{}: the replayed `5d91ef1` rule disagrees with the frozen `pre` column for {:?}",
            e.name,
            e.decoded
        );
    }
}

/// **The direction gate, per point and re-derived — and it must account
/// for BOTH directions**, because "we over-accept" and "we refuse what
/// they serve" are different bugs and the second is the one #226 calls
/// the real one.
///
/// Four disjoint buckets that must partition the matrix:
///
/// - **CLOSED** — `5d91ef1` disagreed with the reference and we now
///   agree, split by which direction it was.
/// - **INTRODUCED** — `5d91ef1` agreed and we now do not.
/// - **STILL OPEN** — both disagree.
///
/// The last two are the SAME divergence seen from two sides, they live
/// only at `variants_common_side`, and #247 recorded and ledgered it:
/// the reference swallows a `Stage()` build error there and answers
/// 200-empty where we answer 400. Their union is asserted to be exactly
/// the common-side points with a malformed expression, so no OTHER point
/// can move away from the reference.
/// - **UNMOVED** — the two trees agree.
#[test]
fn the_pre_388_rule_disagrees_wherever_the_reference_refuses_an_expression() {
    let points = matrix();
    let (mut closed_over, mut closed_under) = (Vec::new(), Vec::new());
    let (mut introduced, mut still_open) = (Vec::new(), Vec::new());
    let (mut already_rejecting, mut controls) = (Vec::new(), 0usize);
    for p in &points {
        let (before, now, theirs) = (p.pre, p.pulsus(), p.reference());
        match (before == theirs, now == theirs) {
            (false, true) if before == Verdict::Accept => closed_over.push(p.label.as_str()),
            (false, true) => closed_under.push(p.label.as_str()),
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
        closed_over.len()
            + closed_under.len()
            + still_open.len()
            + introduced.len()
            + already_rejecting.len()
            + controls,
        points.len(),
        "the buckets must partition the matrix"
    );
    // **INTRODUCED and STILL OPEN are the SAME ledgered divergence seen
    // from two sides, and both live only at `variants_common_side`.**
    // There the reference swallows a `Stage()` build error behind
    // `SelectSamples` and hands the user a 200 with an empty result; we
    // answer 400 wherever the expression is malformed. Whether a point
    // lands in INTRODUCED or in STILL OPEN depends only on whether
    // `5d91ef1` happened to accept the expression, which is not a
    // property of this change. So the assertion is that their UNION is
    // exactly the common-side points with a malformed expression —
    // computed from the table, never restated as a literal — and that
    // neither reaches any other position.
    let mut diverging: Vec<&str> = introduced
        .iter()
        .chain(still_open.iter())
        .copied()
        .collect();
    diverging.sort_unstable();
    let mut expected: Vec<&str> = points
        .iter()
        .filter(|p| p.outcome == Outcome::CommonSideRefusedHere && p.rule != Rule::Accepted)
        .map(|p| p.label.as_str())
        .collect();
    expected.sort_unstable();
    assert_eq!(
        diverging, expected,
        "a divergence outside the ledgered common-side one: this change may not move any other \
         point away from the reference"
    );
    assert!(
        !introduced.is_empty() && !still_open.is_empty(),
        "the two halves of the common-side divergence are both expected to be non-empty; if one \
         has emptied, the reason is worth knowing before this assertion is relaxed"
    );
    // THE HARMFUL DIRECTION, named rather than counted: the expressions
    // the reference serves and `5d91ef1` refused.
    let served_there_refused_here: std::collections::BTreeSet<&str> = EXPRESSIONS
        .iter()
        .filter(|e| e.pre == Verdict::Reject && e.rule == Rule::Accepted)
        .map(|e| e.decoded)
        .collect();
    assert_eq!(
        served_there_refused_here.into_iter().collect::<Vec<_>>(),
        vec![r#"[ "b-c" ]"#, r#"arr[ 0 ]"#, r#"arr[0 ]"#, r#"b[ "c" ]"#],
        "the set of expressions the reference serves and `5d91ef1` refused has moved"
    );
    assert!(!closed_over.is_empty() && !closed_under.is_empty());
    eprintln!(
        "of {} matrix points: {} CLOSED where we over-accepted, {} CLOSED where we refused what \
         the reference serves, {} INTRODUCED, {} STILL OPEN (the ledgered common-side \
         divergence), {} already refused at 5d91ef1, {} accept-side controls.",
        points.len(),
        closed_over.len(),
        closed_under.len(),
        introduced.len(),
        still_open.len(),
        already_rejecting.len(),
        controls,
    );
}

/// **The rule table has exactly one row per line of the committed
/// reference enumeration — plus the rows the enumeration structurally
/// cannot contain, which are listed separately and counted separately.**
#[test]
fn the_rule_table_has_one_row_per_line_of_the_reference_enumeration() {
    let sites = reference_error_sites();
    let from_command: Vec<&str> = REF_RULES
        .iter()
        .filter(|r| r.source == Source::CommandOutput)
        .map(|r| r.site)
        .collect();
    assert_eq!(
        from_command, sites,
        "the rule table and the committed reference enumeration have drifted"
    );
    // And the residue is explicit rather than folded into the count.
    let probed: Vec<&str> = REF_RULES
        .iter()
        .filter(|r| r.source == Source::FoundByProbe)
        .map(|r| r.site)
        .collect();
    assert_eq!(
        probed,
        vec!["strconv/atoi.go:ErrRange"],
        "a rule outside the enumeration's grep scope must be listed, with the probe that found it"
    );
}

/// The `file:line` of every non-comment line of the committed
/// enumeration, in file order.
fn reference_error_sites() -> Vec<String> {
    let raw = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/logql_json_expr_reference_error_sites.txt"),
    )
    .expect("the committed reference enumeration");
    raw.lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| {
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
#[test]
fn every_reachable_rule_has_a_probe_and_every_probe_names_one_rule() {
    for r in REF_RULES {
        let probes: Vec<&str> = EXPRESSIONS
            .iter()
            .filter(|e| e.rule.site() == r.site)
            .map(|e| e.name)
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
            // The relay carries no probes of its own: every error it
            // relays is attributed to the producer that created it.
            (ErrClass::Relay, _) => assert!(
                probes.is_empty(),
                "{}: the relay row carries probes {probes:?} -- attribute them to the producer",
                r.site
            ),
        }
    }
    for e in EXPRESSIONS {
        let named: Vec<&str> = REF_RULES
            .iter()
            .filter(|r| r.site == e.rule.site())
            .map(|r| r.site)
            .collect();
        match e.rule {
            Rule::Accepted => assert!(
                named.is_empty(),
                "{}: an accepted expression names rule(s) {named:?}",
                e.name
            ),
            _ => assert_eq!(
                named.len(),
                1,
                "{}: a rejecting probe must name exactly one rule, it names {named:?}",
                e.name
            ),
        }
    }
    let why_nots: Vec<WhyNot> = REF_RULES.iter().filter_map(|r| r.why_not).collect();
    assert_eq!(
        why_nots,
        vec![WhyNot::UnreachableByAnyInput],
        "the closed why_not set and the table's values have drifted"
    );
    for want in [
        Rule::Accepted,
        Rule::Syntax,
        Rule::UnexpectedChar,
        Rule::FloatIndex,
        Rule::NonIntegerIndex,
        Rule::IndexOutOfRange,
    ] {
        assert!(
            EXPRESSIONS.iter().any(|e| e.rule == want),
            "{want:?} is modelled but no probe produces it"
        );
    }
}

// ---------------------------------------------------------------------
// The ALL dataset (`logql_json_expr_sites.tsv`).
// ---------------------------------------------------------------------

/// A tracked-tree site whose expression's verdict MOVES under this
/// change, with the resolution. Every flip must appear here and every
/// entry here must flip, so neither list can rot.
struct Flip {
    expression: &'static str,
    resolution: &'static str,
}

/// **Every tracked-tree expression whose verdict moves, hand-resolved.**
///
/// Keyed by the DECODED expression, so one entry covers every site that
/// spells it; [`every_flipped_site_is_resolved_and_every_resolution_flips`]
/// asserts both directions, so neither list can rot. None of these is a
/// query a user or a suite actually runs against the sub-grammar.
const FLIPS: &[Flip] = &[
    Flip {
        expression: "request.headers.User-Agent",
        resolution: "a PARSER-level snapshot (crates/pulsus-logql/tests/snapshots.rs). The                      reference accepts it in ParseExpr too -- a json expression is refused at                      Stage(), one layer later -- and that test never compiles a pipeline, so it                      is unaffected. The expression itself is now correctly refused: `-` is not                      an identifier character (jsonexpr/lexer.go:80)",
    },
    Flip {
        expression: "a.a.a\u{2026}",
        resolution: "a doc-comment ELISION, not an expression",
    },
    Flip {
        expression: "\u{2026}",
        resolution: "a doc-comment elision, not an expression",
    },
    Flip {
        expression: "{key}.k00000",
        resolution: "a `format!` PLACEHOLDER (logql_json_flatten_budget.rs). Its real domain is                      `a`*32761 `.k00000`, FIELD DOT FIELD, which the reference accepts; that                      the generator produces only that is read out of the generator, not run                      (reading R4)",
    },
    Flip {
        expression: "{expr}",
        resolution: "`format!` placeholders (logql_pipeline_golden.rs). One loop's domain is                      `o`, `o.z`, `o[0]`, all valid; the other two are the malformed/valid lists                      this change updates in place",
    },
    Flip {
        expression: "b c",
        resolution: "PROSE and FIXTURE about this expression, never a query that must keep                      working: one of the two #394 glosses that said it was `accepted here`                      (which this change makes false and rewrites), and b26_json_expr.test's own                      eval_fail row, which pins the new verdict",
    },
    Flip {
        expression: "b-c",
        resolution: "the same: the second #394 gloss, this issue's two corpus-file headers, and                      b26_json_expr.test's eval_fail row pinning the new verdict",
    },
    Flip {
        expression: "b/c",
        resolution: "b26_json_expr.test's eval_fail row, which pins the new verdict",
    },
    Flip {
        expression: "b!",
        resolution: "b26_json_expr.test's eval_fail row, which pins the new verdict",
    },
    Flip {
        expression: " ",
        resolution: "b26_json_expr.test's eval_fail row, which pins the new verdict",
    },
    Flip {
        expression: "0b",
        resolution: "b26_json_expr.test's eval_fail row, which pins the new verdict",
    },
    Flip {
        expression: "b.0",
        resolution: "b26_json_expr.test's eval_fail row, which pins the new verdict",
    },
    Flip {
        expression: "b 1.5",
        resolution: "b26_json_expr.test's eval_fail row, which pins the new verdict",
    },
    Flip {
        expression: "b.c 1.5",
        resolution: "b26_json_expr.test's eval_fail row, which pins the new verdict",
    },
    Flip {
        expression: "b 1x",
        resolution: "b26_json_expr.test's eval_fail row, which pins the new verdict",
    },
    Flip {
        expression: "b]",
        resolution: "b26_json_expr.test's eval_fail row -- the `]`-key withdrawal, ledgered as                      `json-expression-bracket-key-unreachable`",
    },
    Flip {
        expression: "b 9223372036854775808]",
        resolution: "b26_json_expr.test's eval_fail row for the index bound, which is Go's                      `int`",
    },
    Flip {
        expression: "b[9223372036854775808]",
        resolution: "the same bound, one bracket in, where the syntax error overwrites the                      range error",
    },
    Flip {
        expression: "arr[ 0 ]",
        resolution: "b26_json_expr.test's eval row -- one of the FOUR the reference serves and                      `5d91ef1` refused. It flips toward the reference, which is the direction                      this half of the issue exists for",
    },
    Flip {
        expression: "arr[0 ]",
        resolution: "the same, second of four",
    },
    Flip {
        expression: "b[ \"c\" ]",
        resolution: "the same, third of four",
    },
    Flip {
        expression: "[ \"b-c\" ]",
        resolution: "the same, fourth of four",
    },
    Flip {
        expression: "[ \"b c\" ]",
        resolution: "the same widening, with a space inside the quoted key",
    },
];

/// Every `| json <id>="…"` extraction expression on one line, as
/// `(identifier, expression)`. The keyword is matched
/// CASE-INSENSITIVELY, because LogQL keywords fold.
fn json_expressions(line: &str) -> Vec<String> {
    let lower = line.to_ascii_lowercase();
    let bytes = line.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(off) = lower[i..].find("json") {
        let start = i + off;
        let mut j = start + "json".len();
        if !line[..start].trim_end().ends_with('|') {
            i = j;
            continue;
        }
        // An extraction list: `ident = "expr"` repeated, separated by
        // commas. Anything else ends the list.
        loop {
            while j < bytes.len() && (bytes[j] == b' ' || bytes[j] == b',') {
                j += 1;
            }
            let id_start = j;
            while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            if j == id_start {
                break;
            }
            let mut k = j;
            while k < bytes.len() && bytes[k] == b' ' {
                k += 1;
            }
            if k >= bytes.len() || bytes[k] != b'=' {
                break;
            }
            k += 1;
            while k < bytes.len() && bytes[k] == b' ' {
                k += 1;
            }
            if k >= bytes.len() || bytes[k] != b'"' {
                break;
            }
            k += 1;
            let mut expr = String::new();
            while k < bytes.len() {
                if bytes[k] == b'\\' && k + 1 < bytes.len() {
                    expr.push('\\');
                    let c = line[k + 1..].chars().next().expect("char");
                    expr.push(c);
                    k += 1 + c.len_utf8();
                    continue;
                }
                if bytes[k] == b'"' {
                    break;
                }
                let c = line[k..].chars().next().expect("char");
                expr.push(c);
                k += c.len_utf8();
            }
            if k >= bytes.len() {
                break;
            }
            out.push(expr);
            j = k + 1;
        }
        i = j.max(start + 4);
    }
    out
}

/// Out of the sweep's scope — see [`sweep_a`].
const EXCLUDED: &[&str] = &[
    "src/logql/pattern_expr.rs",
    "src/logql/json_expr.rs",
    "tests/logql_pattern_expr_matrix.rs",
    "tests/logql_json_expr_matrix.rs",
    "tests/logql_pattern_expr_sites.tsv",
    "tests/logql_json_expr_sites.tsv",
    "tests/logql_pattern_expr_reference_error_sites.txt",
    "tests/logql_json_expr_reference_error_sites.txt",
];

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

/// `(site, expression)` for every tracked-tree hit of sweep A.
fn sweep_a() -> Vec<(String, String)> {
    let root = repo_root();
    let mut found = Vec::new();
    for file in tracked_files(&root) {
        // The files that DEFINE this rule and measure it are not
        // consumers of it — the two sub-grammar modules, the two
        // matrices, the two datasets and the two committed reference
        // enumerations. See the pattern matrix's
        // `sweep_a_still_finds_exactly_the_committed_sites` for the same
        // boundary, stated once.
        if EXCLUDED.iter().any(|x| file.ends_with(x)) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(root.join(&file)) else {
            continue;
        };
        for (i, line) in text.lines().enumerate() {
            for expr in json_expressions(line) {
                found.push((format!("{file}:{}", i + 1), expr));
            }
        }
    }
    found.sort();
    found
}

/// The committed TSV, rendered from the tables above and from the sweep.
///
/// A Site row's two verdict columns are COMPUTED — `pre` by the replayed
/// `5d91ef1` rule, `post` by the real `parse → plan → compile` chain — so
/// a site whose verdict moves shows up as a diff in this file rather than
/// as silence.
fn render_sites_tsv() -> String {
    let mut out = String::new();
    out.push_str(
        "# issue #388 Stage B -- the ONE authoritative dataset for the `| json` extraction \
         sub-grammar.\n\
         # GENERATED by `cargo test -p pulsus-read --test logql_json_expr_matrix -- --ignored \
         regenerate_the_sites_dataset`; never hand-edited.\n\
         # row=RefRule: one per line of logql_json_expr_reference_error_sites.txt, PLUS the rows \
         that file structurally cannot contain (source=FoundByProbe).\n\
         # row=Probe:   one per matrix expression. `rule` names exactly one RefRule, or `-` when \
         served.\n\
         # row=Site:    one per tracked-tree hit of sweep A; `pre`/`post` are computed, not \
         typed. form=LogqlUnparseable marks a hit whose text cannot be a LogQL string body.\n\
         row\tform\tid\terr_class\tsource\twhy_not\texpression\tpre\tpost\tref_status\tref_text\
         \tnote\n",
    );
    for r in REF_RULES {
        out.push_str(&format!(
            "RefRule\t-\t{}\t{:?}\t{:?}\t{}\t-\t-\t-\t-\t-\t{}\n",
            r.site,
            r.class,
            r.source,
            match r.why_not {
                Some(w) => format!("{w:?}"),
                None => "-".to_string(),
            },
            one_line(r.note)
        ));
    }
    for e in EXPRESSIONS {
        out.push_str(&format!(
            "Probe\t-\t{}\t-\t-\t-\t{}\t{}\t{}\t{}\t{}\t{}\n",
            e.rule.site(),
            e.decoded,
            e.pre.tsv(),
            e.rule.reference().tsv(),
            match e.rule {
                Rule::Accepted => "200",
                _ => "400",
            },
            if e.reference_text.is_empty() {
                "-"
            } else {
                e.reference_text
            },
            e.name,
        ));
    }
    for (site, expr) in sweep_a() {
        match resolve_site(&expr) {
            Some((decoded, pre, post)) => {
                let note = FLIPS
                    .iter()
                    .find(|f| f.expression == decoded)
                    .map(|f| f.resolution)
                    .unwrap_or("");
                out.push_str(&format!(
                    "Site\tLiteral\t{site}\t-\t-\t{decoded}\t{}\t{}\t-\t-\t{}\n",
                    pre.tsv(),
                    post.tsv(),
                    one_line(note),
                ));
            }
            None => out.push_str(&format!(
                "Site\tLogqlUnparseable\t{site}\t-\t-\t{expr}\t-\t-\t-\t-\t{}\n",
                one_line(
                    "the sweep's text is not a valid LogQL string body, so no expression reaches \
                     the sub-grammar from this site (an invalid escape, or a `format!` \
                     placeholder). Outside the compile domain; no verdict is recorded rather \
                     than a made-up one"
                ),
            )),
        }
    }
    out
}

/// A sweep hit's `(decoded expression, verdict at 5d91ef1, verdict now)`,
/// or `None` when the written text is not a valid LogQL string body.
///
/// **The sweep yields the expression AS WRITTEN**, so `req.hdr[\"x\"]` in
/// a Rust source arrives with its backslashes on. Decoding is the LogQL
/// parser's job and this asks it to do it, which is also how a text that
/// cannot be a LogQL string at all (`\d`, a `{placeholder}` containing
/// one) is separated out instead of being scored against a rule it never
/// reaches — the escaping artefact this kind of sweep is known for.
fn resolve_site(written: &str) -> Option<(String, Verdict, Verdict)> {
    let query = format!(r#"{{service_name="m"}} | json v="{written}""#);
    let decoded = parse(&query).ok().and_then(first_json_expression)?;
    let (post, _) = pulsus_verdict(&query);
    Some((decoded.clone(), pre_388(&decoded), post))
}

fn first_json_expression(expr: pulsus_logql::Expr) -> Option<String> {
    let pulsus_logql::Expr::Log(log) = expr else {
        return None;
    };
    for stage in &log.pipeline {
        if let pulsus_logql::Stage::Parser(pulsus_logql::ParserStage::Json { extractions }) = stage
        {
            return extractions.first().map(|x| x.expression.clone());
        }
    }
    None
}

/// Rust's string continuation leaves runs of indentation inside a
/// multi-line literal; a TSV cell must not carry them.
fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sites_tsv_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/logql_json_expr_sites.tsv")
}

/// **The dataset is generated, never retyped**, and the generation runs
/// the sweep and the real compiler — so a new site, a moved site or a
/// moved verdict all show up as a byte difference here.
#[test]
fn the_sites_dataset_is_regenerated_not_retyped() {
    let want = render_sites_tsv();
    let got = std::fs::read_to_string(sites_tsv_path()).expect("the committed sites dataset");
    assert_eq!(
        got, want,
        "`logql_json_expr_sites.tsv` has drifted from the tables and the sweep that generate it \
         -- re-run the `regenerate_the_sites_dataset` generator rather than editing the file"
    );
}

/// **Every flip is hand-resolved, and every resolution names a real
/// flip.** The two directions are asserted separately so neither list can
/// rot into the other's shadow.
#[test]
fn every_flipped_site_is_resolved_and_every_resolution_flips() {
    let mut flipped: Vec<String> = Vec::new();
    for (site, written) in sweep_a() {
        let Some((decoded, pre, post)) = resolve_site(&written) else {
            continue;
        };
        if pre != post {
            flipped.push(format!("{site}\t{decoded}"));
        }
    }
    for f in &flipped {
        let decoded = f.split('\t').nth(1).expect("expression");
        assert!(
            FLIPS.iter().any(|x| x.expression == decoded),
            "{f}: a tracked-tree expression whose verdict MOVES with no recorded resolution"
        );
    }
    for f in FLIPS {
        assert!(
            flipped
                .iter()
                .any(|x| x.split('\t').nth(1) == Some(f.expression)),
            "{}: a recorded flip that no longer flips -- delete the entry rather than leaving it",
            f.expression
        );
    }
    eprintln!(
        "{} tracked-tree expressions flip; all are resolved",
        flipped.len()
    );
}

/// **Sweep D — the queries that live in DATA, not source.** Walks the
/// `logqltest` corpus and the logs fixtures at run time and compiles
/// every `| json` extraction expression it finds, because a grep over
/// source cannot see a query a harness reads from a file.
///
/// **The property is per-expression, not per-row.** An earlier revision
/// asserted "an `eval_fail` row's expression must be refused", which is
/// false: `b22_logfmt_expr_reject.test:209` is an `eval_fail` for a
/// DANGLING COMMA and its expression `b` is perfectly well formed. What
/// this checks instead is that no corpus expression's verdict MOVES,
/// which is the property the corpus depends on and needs no reading of
/// why a row fails.
#[test]
fn every_corpus_json_expression_keeps_its_verdict() {
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
            for written in json_expressions(line) {
                let Some((decoded, pre, post)) = resolve_site(&written) else {
                    continue;
                };
                if pre != post {
                    assert!(
                        FLIPS.iter().any(|f| f.expression == decoded),
                        "{rel}:{}: {decoded:?} moves from {pre:?} to {post:?} with no recorded \
                         resolution",
                        i + 1
                    );
                }
                checked += 1;
            }
        }
    }
    assert!(checked > 0, "sweep D found no corpus json expression");
    eprintln!("sweep D compiled {checked} corpus/fixture json expressions");
}

#[test]
#[ignore = "regenerates a committed artefact"]
fn regenerate_the_sites_dataset() {
    std::fs::write(sites_tsv_path(), render_sites_tsv()).expect("write");
}

// ---------------------------------------------------------------------
// The live legs (gated on PULSUSDB_LOGQL_DIFF_URL). Status only.
// ---------------------------------------------------------------------

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
/// pinned container**, over a window ending at `now` — which is
/// load-bearing here and is what
/// [`live_the_json_rule_is_window_dependent`] measures.
#[test]
fn live_matrix_against_the_reference() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the live json-expression matrix");
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
        if theirs == Verdict::Reject && !body.contains(p.reference_text) {
            disagree.push(format!(
                "{} {}\n    committed text {:?} is not in {}",
                p.label,
                p.query,
                p.reference_text,
                body.chars().take(200).collect::<String>()
            ));
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

/// **The json rule is WINDOW-DEPENDENT, and the pattern one is not.**
/// This is the fact that forbids one harness for the two stages. Over a
/// window ending 24 h back the reference answers 200 for expressions it
/// refuses over a fresh one, because the ingester leaves the path before
/// `expr.Pipeline()` ever runs — so every live leg above must use a
/// window ending at `now`.
#[test]
fn live_the_json_rule_is_window_dependent() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the stale-window probe");
        return;
    };
    const DAY: u64 = 24 * 60 * 60;
    let mut still_refused = Vec::new();
    for e in EXPRESSIONS.iter().filter(|e| e.rule != Rule::Accepted) {
        let query = format!(r#"{{service_name="m"}} | json v="{}""#, e.quoted);
        let (code, _) = reference_status(&base, &query, DAY);
        if code != 200 {
            still_refused.push(e.name);
        }
    }
    assert!(
        still_refused.is_empty(),
        "these expressions are refused even over a 24 h-stale window, so they are NOT `Stage()` \
         errors and this file's harness assumption is wrong for them: {still_refused:?}"
    );
    // The control: the PATTERN rule does not go quiet over the same
    // window, which is what makes the assertion above a discrimination
    // rather than a statement about stale windows in general.
    let (code, body) = reference_status(&base, r#"{service_name="m"} | pattern "<a> <a>""#, DAY);
    assert_eq!(
        code,
        400,
        "the pattern rule has become window-dependent too, so the two stages no longer need \
         separate harnesses -- re-read both files' claims before changing either: {}",
        body.chars().take(200).collect::<String>()
    );
}
