// Copyright 2019 The Prometheus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is deriven from generated_parser.y at [1] with the following differences:
//
// - no empty rule
// - no series descriptions rule
//
// [1] https://github.com/prometheus/prometheus/blob/v2.45.0/promql/parser/generated_parser.y
//
// PulsusDB patch (docs/decisions/0003, grammar patch G1 — the first
// grammar-production patch, see vendor/promql-parser/PATCHES.md): the
// duration-expression productions (`number_duration_literal`,
// `duration_expr`, `paren_duration_expr`, `positive_duration_expr`,
// `offset_duration_expr`, `max_of_min_of`, `unary_op`) are ported from
// Prometheus v3.13.0's generated_parser.y at the pinned conformance SHA,
// and `matrix_selector`/`subquery_expr`/`offset_expr` are rewired to
// consume them. `STEP`/`RANGE`/`MAX_OF`/`MIN_OF` join
// `metric_identifier`/`maybe_label` so they stay usable as metric/label
// names, and `function_call` gains the `max_of_min_of function_call_body`
// alternative so the already-implemented `max_of`/`min_of` scalar calls
// keep parsing after keyword-ization. Alternative order within
// `offset_duration_expr` vs `duration_expr` is load-bearing: grmtools
// resolves reduce/reduce conflicts in favour of the earlier production,
// which is what terminates a bare `offset <literal>`/`offset
// <unary-literal>`/`offset step()` before a trailing operator, so
// `foo offset 100 + 2` parses as `(foo offset 100) + 2` (upstream
// semantics, pinned by the eval corpus).
//
// PulsusDB patch (docs/decisions/0003, grammar patch G4 — see
// vendor/promql-parser/PATCHES.md): `function_call` gains the three
// upstream v3.13 query-context arms (`at_modifier_preprocessors
// function_call_body`, `STEP function_call_body`, `RANGE
// function_call_body`) so `start()`/`end()`/`step()`/`range()` parse as
// ordinary zero-arity calls in expression position. The
// `metric_identifier`/`maybe_label` arms and every duration-position use
// of STEP/RANGE are unchanged, as is the `expr AT
// at_modifier_preprocessors LEFT_PAREN RIGHT_PAREN` preprocessor rule
// (`foo @ start()` stays an @-modifier, never a call argument — at_expr
// has no `expr AT expr` production, so the preprocessor rule remains the
// only continuation after AT, exactly as upstream).

%token EQL
BLANK
COLON
COMMA
COMMENT
DURATION
EOF
ERROR
IDENTIFIER
LEFT_BRACE
LEFT_BRACKET
LEFT_PAREN
OPEN_HIST
CLOSE_HIST
METRIC_IDENTIFIER
NUMBER
RIGHT_BRACE
RIGHT_BRACKET
RIGHT_PAREN
SEMICOLON
SPACE
STRING
TIMES

// Operators.
%token OPERATORS_START
ADD
DIV
EQLC
EQL_REGEX
GTE
GTR
// PulsusDB patch (docs/decisions/0003, grammar patch G2 — see
// vendor/promql-parser/PATCHES.md): the native-histogram trim operators
// (`</` TRIM_UPPER, `>/` TRIM_LOWER), ported from Prometheus v3.13.0's
// generated_parser.y at the pinned conformance SHA.
TRIM_UPPER
TRIM_LOWER
LAND
LOR
LSS
LTE
LUNLESS
MOD
MUL
NEQ
NEQ_REGEX
POW
SUB
AT
ATAN2
%token OPERATORS_END

// Aggregators.
%token AGGREGATORS_START
AVG
BOTTOMK
COUNT
COUNT_VALUES
GROUP
MAX
MIN
QUANTILE
STDDEV
STDVAR
SUM
TOPK
LIMITK
LIMIT_RATIO
%token AGGREGATORS_END

// Keywords.
%token KEYWORDS_START
BOOL
BY
GROUP_LEFT
GROUP_RIGHT
FILL
FILL_LEFT
FILL_RIGHT
IGNORING
OFFSET
SMOOTHED
ANCHORED
ON
WITHOUT
%token KEYWORDS_END

// Preprocessors.
%token PREPROCESSOR_START
START
END
STEP
RANGE
MAX_OF
MIN_OF
%token PREPROCESSOR_END

// Start symbols for the generated parser.
%token STARTSYMBOLS_START
START_METRIC
START_SERIES_DESCRIPTION
START_EXPRESSION
START_METRIC_SELECTOR
%token STARTSYMBOLS_END

%expect-unused 'BLANK' 'COMMENT' 'ERROR' 'SEMICOLON' 'SPACE' 'TIMES' 'OPEN_HIST' 'CLOSE_HIST'
%expect-unused 'OPERATORS_START' 'OPERATORS_END' 'AGGREGATORS_START' 'AGGREGATORS_END'
%expect-unused 'KEYWORDS_START' 'KEYWORDS_END' 'PREPROCESSOR_START' 'PREPROCESSOR_END'
%expect-unused 'STARTSYMBOLS_START'
%expect-unused 'START_METRIC' 'START_SERIES_DESCRIPTION' 'START_EXPRESSION' 'START_METRIC_SELECTOR' 'STARTSYMBOLS_END'

// PulsusDB patch (docs/decisions/0003, grammar patch G1): the
// duration-expression productions raise the conflict counts from the
// original `%expect 5` (shift/reduce, zero reduce/reduce). The
// shift/reduce conflicts resolve to shift (standard yacc), and every
// reduce/reduce conflict is the deliberate `offset_duration_expr` /
// `duration_expr` overlap, resolved by grmtools in favour of the
// earlier-defined production — offset_duration_expr's arms, which is the
// upstream precedence behaviour ("foo offset 100 + 2" == "(foo offset
// 100) + 2"), pinned by the proof corpus.
//
// PulsusDB patch (docs/decisions/0003, grammar patch G2): the two new
// `binary_expr` productions for TRIM_UPPER/TRIM_LOWER raise the
// reduce/reduce count further (each new comparison-precedence operator
// overlaps the same offset_duration_expr/duration_expr ambiguity above);
// shift/reduce is unaffected.
//
// PulsusDB patch (docs/decisions/0003, grammar patch G3): the postfix
// `anchored_expr`/`smoothed_expr` productions and the ANCHORED/SMOOTHED
// arms of metric_identifier/maybe_label raise both counts. ANCHORED/
// SMOOTHED carry no precedence (upstream declares none either); the
// shift/reduce conflicts resolve to shift (bind the trailing modifier to
// the preceding expr) and the reduce/reduce conflicts to the
// earlier-defined production — upstream goyacc's default resolution,
// pinned by the parse corpus (`m[1m] anchored`, `anchored{job="test"}`,
// `sum by (smoothed)`) and the crate's own grammar tests.
//
// PulsusDB patch (docs/decisions/0003, grammar patch G4): the three
// query-context `function_call` arms leave BOTH counts unchanged
// (measured: 51 shift/reduce, 243 reduce/reduce — the %expect gate fails
// on any mismatch, so these are build-verified, never guessed). No other
// expression-position production begins with START/END/STEP/RANGE, so a
// keyword followed by `(` at expression start has exactly one
// continuation (the call form); the duration-position and
// metric/label-name uses of the same tokens sit in states G1 already
// accounted for.
%expect 51
%expect-rr 243

%start start

// Operators are listed with increasing precedence.
%left LOR
%left LAND LUNLESS
%left EQLC GTE GTR LSS LTE NEQ TRIM_UPPER TRIM_LOWER
%left ADD SUB
%left MUL DIV MOD ATAN2
%right POW

// Offset and At modifiers do not have associativity.
%nonassoc OFFSET AT GROUP_LEFT GROUP_RIGHT

// This ensures that it is always attempted to parse range or subquery selectors when a left
// bracket is encountered.
%right LEFT_BRACKET

// left_paren has higher precedence than group_left/group_right, to fix the reduce/shift conflict.
// if group_left/group_right is followed by left_paren, the parser will shift instead of reduce
%right LEFT_PAREN

%%
start -> Result<Expr, String>:
                expr { $1 }
        |       expr EOF { $1 }
        |       EOF { Err("no expression found in input".into()) }
;

expr -> Result<Expr, String>:
/* check_ast from bottom to up for nested exprs */
                aggregate_expr { check_ast($1?) }
        |       anchored_expr { check_ast($1?) }
        |       at_expr { check_ast($1?) }
        |       binary_expr { check_ast($1?) }
        |       function_call { check_ast($1?) }
        |       matrix_selector { check_ast($1?) }
        |       number_literal { check_ast($1?) }
        |       offset_expr { check_ast($1?) }
        |       paren_expr { check_ast($1?) }
        |       smoothed_expr { check_ast($1?) }
        |       string_literal { check_ast($1?) }
        |       subquery_expr { check_ast($1?) }
        |       unary_expr  { check_ast($1?) }
        |       vector_selector  { check_ast($1?) }
;

/*
 * Anchored and smoothed modifiers (extended range selectors).
 *
 * PulsusDB patch (docs/decisions/0003, grammar patch G3): ported from
 * upstream v3.13.0's `anchored_expr`/`smoothed_expr` productions
 * (generated_parser.y:613-621). Upstream gates these behind the parser
 * option `EnableExtendedRangeSelectors`; matching the G1 precedent the
 * experimental gate is relocated to plan time (the parser has no options
 * plumbing) — the setter actions here port `setAnchored`/`setSmoothed`
 * (parse.go:1078-1129) minus the gate check.
 */
anchored_expr -> Result<Expr, String>:
                expr ANCHORED { $1?.set_anchored() }
;

smoothed_expr -> Result<Expr, String>:
                expr SMOOTHED { $1?.set_smoothed() }
;

/*
 * Aggregations.
 *
 * PulsusDB patch (issue #128, PATCHES.md AST-metadata class): every
 * action producing one of the seven start-carrying node kinds
 * (AggregateExpr, Call, ParenExpr, UnaryExpr, VectorSelector,
 * NumberLiteral, StringLiteral) records `$span.start()` — the byte
 * offset of the production's first token — via `Expr::with_pos_start`.
 * Postfix productions (offset/@/[range]/anchored/smoothed, matrix and
 * subquery wrapping) never move a node's start, matching upstream
 * Prometheus `PositionRange` semantics, so they need no edits.
 */
aggregate_expr -> Result<Expr, String>:
                aggregate_op aggregate_modifier function_call_body
                {
                        Expr::new_aggregate_expr($1?.id(), Some($2?), $3?).map(|e| e.with_pos_start($span.start()))
                }
        |       aggregate_op function_call_body aggregate_modifier
                {
                        Expr::new_aggregate_expr($1?.id(), Some($3?), $2?).map(|e| e.with_pos_start($span.start()))
                }
        |       aggregate_op function_call_body
                {
                        Expr::new_aggregate_expr($1?.id(), None, $2?).map(|e| e.with_pos_start($span.start()))
                }
;

aggregate_modifier -> Result<LabelModifier, String>:
                BY grouping_labels { Ok(LabelModifier::Include($2?)) }
        |       WITHOUT grouping_labels { Ok(LabelModifier::Exclude($2?)) }
;

/*
 * Binary expressions.
 */
// Operator precedence only works if each of those is listed separately.
binary_expr -> Result<Expr, String>:
                expr ADD       bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr ATAN2   bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr DIV     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr EQLC    bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr GTE     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr GTR     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr LAND    bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr LOR     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr LSS     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr LTE     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr LUNLESS bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr MOD     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr MUL     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr NEQ     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr POW     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr SUB     bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr TRIM_LOWER bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
        |       expr TRIM_UPPER bin_modifier expr { Expr::new_binary_expr($1?, lexeme_to_token($lexer, $2)?.id(), $3?, $4?) }
;

// Using left recursion for the modifier rules, helps to keep the parser stack small and
// reduces allocations
bin_modifier -> Result<Option<BinModifier>, String>:
                fill_modifiers { $1 }
;

bool_modifier -> Result<Option<BinModifier>, String>:
                { Ok(None) }
        |       BOOL
                {
                        let modifier = BinModifier::default().with_return_bool(true);
                        Ok(Some(modifier))
                }
;

on_or_ignoring -> Result<Option<BinModifier>, String>:
                bool_modifier IGNORING grouping_labels
                {
                        Ok(update_optional_matching($1?, Some(LabelModifier::Exclude($3?))))
                }
        |       bool_modifier ON grouping_labels
                {
                        Ok(update_optional_matching($1?, Some(LabelModifier::Include($3?))))
                }
;

group_modifiers -> Result<Option<BinModifier>, String>:
                bool_modifier { $1 }
        |       on_or_ignoring { $1 }
        |       on_or_ignoring GROUP_LEFT grouping_labels
                {
                        Ok(update_optional_card($1?, VectorMatchCardinality::ManyToOne($3?)))
                }
        |       on_or_ignoring GROUP_RIGHT grouping_labels
                {
                        Ok(update_optional_card($1?, VectorMatchCardinality::OneToMany($3?)))
                }
        |       on_or_ignoring GROUP_LEFT
                {
                        Ok(update_optional_card($1?, VectorMatchCardinality::ManyToOne(Labels::new(vec![]))))
                }
        |       on_or_ignoring GROUP_RIGHT
                {
                        Ok(update_optional_card($1?, VectorMatchCardinality::OneToMany(Labels::new(vec![]))))
                }
        |       GROUP_LEFT grouping_labels { Err("unexpected <group_left>".into()) }
        |       GROUP_RIGHT grouping_labels { Err("unexpected <group_right>".into()) }
;

fill_modifiers -> Result<Option<BinModifier>, String>:
                 group_modifiers { $1 }
        |        group_modifiers FILL fill_value
                 {
                         let fill = $3?;
                         Ok(update_optional_fill($1?, VectorMatchFillValues::new(fill, fill)))
                 }
        |        group_modifiers FILL_LEFT fill_value
                 {
                         Ok(update_optional_fill($1?, VectorMatchFillValues::default().with_lhs($3?)))
                 }
        |        group_modifiers FILL_RIGHT fill_value
                 {
                         Ok(update_optional_fill($1?, VectorMatchFillValues::default().with_rhs($3?)))
                 }
        |        group_modifiers FILL_LEFT fill_value FILL_RIGHT fill_value
                 {
                         Ok(update_optional_fill($1?, VectorMatchFillValues::new($3?, $5?)))
                 }
        |        group_modifiers FILL_RIGHT fill_value FILL_LEFT fill_value
                 {
                         Ok(update_optional_fill($1?, VectorMatchFillValues::new($5?, $3?)))
                 }
;

grouping_labels -> Result<Labels, String>:
                LEFT_PAREN grouping_label_list RIGHT_PAREN { $2 }
        |       LEFT_PAREN grouping_label_list COMMA RIGHT_PAREN { $2 }
        |       LEFT_PAREN RIGHT_PAREN { Ok(Labels::new(vec![])) }
;

grouping_label_list -> Result<Labels, String>:
                grouping_label_list COMMA grouping_label { Ok($1?.append($3?.val)) }
        |       grouping_label { Ok(Labels::new(vec![&$1?.val])) }
;

grouping_label -> Result<Token, String>:
                maybe_label
                {
                        let token = $1?;
                        let label = &token.val;
                        if is_label(label) {
                            Ok(token)
                        } else {
                            Err(format!("{label} is not valid label in grouping opts"))
                        }
                }
        |       STRING {
                        let name = unquote_string($lexer.span_str($span))?;
                        Ok(Token::new(T_IDENTIFIER, name))
                }
;

fill_value -> Result<f64, String>:
                LEFT_PAREN NUMBER RIGHT_PAREN
                {
                        if let Ok(tok) = $2 {
                            parse_str_radix($lexer.span_str(tok.span()))
                        } else {
                            Err("Number expected for fill value".to_string())
                        }
                }
        |       LEFT_PAREN ADD NUMBER RIGHT_PAREN
                {
                        if let Ok(tok) = $3 {
                            parse_str_radix($lexer.span_str(tok.span()))
                        } else {
                            Err("Number expected for fill value".to_string())
                        }
                }
        |       LEFT_PAREN SUB NUMBER RIGHT_PAREN
                {
                        if let Ok(tok) = $3 {
                            parse_str_radix($lexer.span_str(tok.span())).map(|v| -v)
                        } else {
                            Err("Number expected for fill value".to_string())
                        }
                }
;

/*
 * Function calls.
 */
function_call -> Result<Expr, String>:
                IDENTIFIER function_call_body
                {
                        let name = lexeme_to_string($lexer, &$1)?;
                        match get_function(&name) {
                            None => Err(format!("unknown function with name '{name}'")),
                            Some(func) => Expr::new_call(func, $2?).map(|e| e.with_pos_start($span.start()))
                        }
                }
        // PulsusDB patch (docs/decisions/0003, grammar patch G1): now that
        // max_of/min_of are keyword tokens (duration expressions), their
        // already-implemented scalar-function call form must keep parsing —
        // upstream v3.13's `max_of_min_of function_call_body` alternative.
        |       max_of_min_of function_call_body
                {
                        let name = $1?.val;
                        match get_function(&name) {
                            None => Err(format!("unknown function with name '{name}'")),
                            Some(func) => Expr::new_call(func, $2?).map(|e| e.with_pos_start($span.start()))
                        }
                }
        // PulsusDB patch (docs/decisions/0003, grammar patch G4 — see
        // vendor/promql-parser/PATCHES.md): the query-context function
        // calls `start()`/`end()`/`step()`/`range()` — upstream v3.13's
        // `at_modifier_preprocessors function_call_body`,
        // `STEP function_call_body`, and `RANGE function_call_body`
        // alternatives (generated_parser.y:477/495/513 at the pinned
        // conformance SHA). Raw-lexeme lookup keeps upstream's case
        // behaviour: keywords lex case-insensitively, but `START()` looks
        // up "START" and stays "unknown function" — both sides agree.
        |       at_modifier_preprocessors function_call_body
                {
                        let name = $1?.val;
                        match get_function(&name) {
                            None => Err(format!("unknown function with name '{name}'")),
                            Some(func) => Expr::new_call(func, $2?).map(|e| e.with_pos_start($span.start()))
                        }
                }
        |       STEP function_call_body
                {
                        let name = lexeme_to_string($lexer, &$1)?;
                        match get_function(&name) {
                            None => Err(format!("unknown function with name '{name}'")),
                            Some(func) => Expr::new_call(func, $2?).map(|e| e.with_pos_start($span.start()))
                        }
                }
        |       RANGE function_call_body
                {
                        let name = lexeme_to_string($lexer, &$1)?;
                        match get_function(&name) {
                            None => Err(format!("unknown function with name '{name}'")),
                            Some(func) => Expr::new_call(func, $2?).map(|e| e.with_pos_start($span.start()))
                        }
                }
;

function_call_body -> Result<FunctionArgs, String>:
                LEFT_PAREN function_call_args RIGHT_PAREN { $2 }
        |       LEFT_PAREN RIGHT_PAREN { Ok(FunctionArgs::empty_args()) }
;

function_call_args -> Result<FunctionArgs, String>:
                function_call_args COMMA expr { Ok($1?.append_args($3?)) }
        |       expr { Ok(FunctionArgs::new_args($1?)) }
        |       function_call_args COMMA { Err("trailing commas not allowed in function call args".into()) }
;

/*
 * Expressions inside parentheses.
 */
paren_expr -> Result<Expr, String>:
                LEFT_PAREN expr RIGHT_PAREN { Expr::new_paren_expr($2?).map(|e| e.with_pos_start($span.start())) }
;

/*
 * Offset modifiers.
 *
 * PulsusDB patch (docs/decisions/0003, grammar patch G1): `offset` takes a
 * duration expression (upstream v3.13's `offset_duration_expr`). A plain
 * (possibly sign-folded) literal resolves to the concrete `Offset` here;
 * everything else is carried as `offset_expr: Some(DurationExpr)`.
 */
offset_expr -> Result<Expr, String>:
                expr OFFSET offset_duration_expr
                {
                        match $3? {
                            DurationExpr::Number(secs) => $1?.offset_expr(offset_from_secs(secs)?),
                            de => $1?.offset_dur_expr(de),
                        }
                }
        |       expr OFFSET EOF { Err("unexpected end of input in offset, expected number, duration, step(), or range()".into()) }
;

// offset_duration_expr is needed to handle expressions like "foo offset -2^2"
// correctly: its single-token/unary/call-like alternatives are defined
// *before* duration_expr's, so the reduce/reduce conflict between "finish
// the offset here" and "keep building a duration expression" resolves (per
// grmtools' earlier-production rule) to finishing the offset — "foo offset
// -2^2" is "(foo offset -2)^2" and "foo offset 100 + 2" is
// "(foo offset 100) + 2", upstream v3.13 semantics.
offset_duration_expr -> Result<DurationExpr, String>:
                number_duration_literal { checked_number_literal($1?) }
        |       unary_op number_duration_literal { apply_unary_op_to_duration_expr($1?, $2?, false) }
        |       STEP LEFT_PAREN RIGHT_PAREN { Ok(DurationExpr::Step) }
        |       RANGE LEFT_PAREN RIGHT_PAREN { Ok(DurationExpr::Range) }
        |       unary_op STEP LEFT_PAREN RIGHT_PAREN { Ok(unary_duration_expr($1?, DurationExpr::Step)) }
        |       unary_op RANGE LEFT_PAREN RIGHT_PAREN { Ok(unary_duration_expr($1?, DurationExpr::Range)) }
        |       max_of_min_of LEFT_PAREN duration_expr COMMA duration_expr RIGHT_PAREN
                { Ok(min_of_max_of_expr($1?, $3?, $5?)) }
        |       unary_op max_of_min_of LEFT_PAREN duration_expr COMMA duration_expr RIGHT_PAREN
                { Ok(unary_duration_expr($1?, min_of_max_of_expr($2?, $4?, $6?))) }
        |       unary_op LEFT_PAREN duration_expr RIGHT_PAREN %prec MUL
                { apply_unary_op_to_duration_expr($1?, $3?, true) }
        |       duration_expr { $1 }
;

max_of_min_of -> Result<Token, String>:
                MAX_OF { lexeme_to_token($lexer, $1) }
        |       MIN_OF { lexeme_to_token($lexer, $1) }
;

unary_op -> Result<Token, String>:
                ADD { lexeme_to_token($lexer, $1) }
        |       SUB { lexeme_to_token($lexer, $1) }
;

duration_expr -> Result<DurationExpr, String>:
                number_duration_literal { checked_number_literal($1?) }
        |       unary_op duration_expr %prec MUL { apply_unary_op_to_duration_expr($1?, $2?, false) }
        |       duration_expr ADD duration_expr { duration_binary_expr(T_ADD, $1?, $3?) }
        |       duration_expr SUB duration_expr { duration_binary_expr(T_SUB, $1?, $3?) }
        |       duration_expr MUL duration_expr { duration_binary_expr(T_MUL, $1?, $3?) }
        |       duration_expr DIV duration_expr { duration_binary_expr(T_DIV, $1?, $3?) }
        |       duration_expr MOD duration_expr { duration_binary_expr(T_MOD, $1?, $3?) }
        |       duration_expr POW duration_expr { duration_binary_expr(T_POW, $1?, $3?) }
        |       STEP LEFT_PAREN RIGHT_PAREN { Ok(DurationExpr::Step) }
        |       RANGE LEFT_PAREN RIGHT_PAREN { Ok(DurationExpr::Range) }
        |       max_of_min_of LEFT_PAREN duration_expr COMMA duration_expr RIGHT_PAREN
                { Ok(min_of_max_of_expr($1?, $3?, $5?)) }
        |       paren_duration_expr { $1 }
;

paren_duration_expr -> Result<DurationExpr, String>:
                LEFT_PAREN duration_expr RIGHT_PAREN
                {
                        // Idempotent wrap: "((1h))" stays one layer of
                        // parens on Display, like upstream's boolean
                        // `Wrapped` flag.
                        match $2? {
                            e @ DurationExpr::Wrapped(_) => Ok(e),
                            e => Ok(DurationExpr::Wrapped(Box::new(e))),
                        }
                }
;

// A duration expression whose *literal* form must be positive — the
// range-selector and subquery range/step positions. Parenthesised and/or
// unary-signed literals count as literals here (upstream keeps them
// `*NumberLiteral`s — `DurationExpr::literal_value`); genuinely computed
// forms are range-checked at resolve time instead (upstream durations.go).
positive_duration_expr -> Result<DurationExpr, String>:
                duration_expr
                {
                        let e = $1?;
                        match e.literal_value() {
                            Some(secs) if secs <= 0.0 =>
                                Err("duration must be greater than 0".into()),
                            _ => Ok(e),
                        }
                }
;

/*
 * @ modifiers.
 *
 * the original name of this production head is step_invariant_expr
 */
at_expr -> Result<Expr, String>:
                expr AT number_literal { $1?.at_expr(AtModifier::try_from($3?)?) }
        |       expr AT ADD number_literal { $1?.at_expr(AtModifier::try_from($4?)?) }
        |       expr AT SUB number_literal
                {
                        let nl = $4.map(|nl| -nl);
                        $1?.at_expr(AtModifier::try_from(nl?)?)
                }
        |       expr AT at_modifier_preprocessors LEFT_PAREN RIGHT_PAREN
                {
                        let at = AtModifier::try_from($3?)?;
                        $1?.at_expr(at)
                }
        |       expr AT EOF
                {
                        Err("unexpected end of input in @, expected timestamp".into())
                }
;

at_modifier_preprocessors -> Result<Token, String>:
                START { lexeme_to_token($lexer, $1) }
        |       END { lexeme_to_token($lexer, $1) }
;

/*
 * Subquery and range selectors.
 *
 * PulsusDB patch (docs/decisions/0003, grammar patch G1): the range and
 * subquery range/step positions take positive duration expressions
 * (upstream v3.13). A literal resolves to the concrete Duration field; a
 * non-literal expression rides the `*_expr` field with a zero placeholder.
 */
matrix_selector -> Result<Expr, String>:
                expr LEFT_BRACKET positive_duration_expr RIGHT_BRACKET
                {
                        match $3? {
                            DurationExpr::Number(secs) =>
                                Expr::new_matrix_selector($1?, duration_from_secs(secs)?, None),
                            de => Expr::new_matrix_selector($1?, Duration::ZERO, Some(de)),
                        }
                }
        |       expr LEFT_BRACKET RIGHT_BRACKET
                {
                        Err("unexpected \"]\" in subquery or range selector, expected number, duration, step(), or range()".into())
                }
;

subquery_expr -> Result<Expr, String>:
                expr LEFT_BRACKET positive_duration_expr COLON positive_duration_expr RIGHT_BRACKET
                {
                        let (range, range_expr) = match $3? {
                            DurationExpr::Number(secs) => (duration_from_secs(secs)?, None),
                            de => (Duration::ZERO, Some(de)),
                        };
                        let (step, step_expr) = match $5? {
                            DurationExpr::Number(secs) => (Some(duration_from_secs(secs)?), None),
                            de => (None, Some(de)),
                        };
                        Expr::new_subquery_expr($1?, range, range_expr, step, step_expr)
                }
        |       expr LEFT_BRACKET positive_duration_expr COLON RIGHT_BRACKET
                {
                        let (range, range_expr) = match $3? {
                            DurationExpr::Number(secs) => (duration_from_secs(secs)?, None),
                            de => (Duration::ZERO, Some(de)),
                        };
                        Expr::new_subquery_expr($1?, range, range_expr, None, None)
                }
;

/*
 * Unary expressions.
 */
unary_expr -> Result<Expr, String>:
                /* PulsusDB patch (issue #128): the result's start is the
                 * sign token, covering upstream's sign-collapse (a parsed
                 * `-1` is a NumberLiteral starting at the `-`). */
                ADD expr %prec MUL { Ok($2?.with_pos_start($span.start())) }
        |       SUB expr %prec MUL { Expr::new_unary_expr($2?).map(|e| e.with_pos_start($span.start())) }
;

/*
 * Vector selectors.
 */
vector_selector -> Result<Expr, String>:
                metric_identifier label_matchers
                {
                        Expr::new_vector_selector(Some($1?.val), $2?).map(|e| e.with_pos_start($span.start()))
                }
        |       metric_identifier
                {
                        Expr::new_vector_selector(Some($1?.val), Matchers::empty()).map(|e| e.with_pos_start($span.start()))
                }
        |       label_matchers
                {
                        Expr::new_vector_selector(None, $1?).map(|e| e.with_pos_start($span.start()))
                }
;

label_matchers -> Result<Matchers, String>:
                LEFT_BRACE label_match_list RIGHT_BRACE { $2 }
        |       LEFT_BRACE label_match_list COMMA RIGHT_BRACE { $2 }
        |       LEFT_BRACE RIGHT_BRACE { Ok(Matchers::empty()) }
        |       LEFT_BRACE COMMA RIGHT_BRACE
                { Err("unexpected ',' in label matching, expected identifier or right_brace".into()) }
;

label_match_list -> Result<Matchers, String>:
                label_match_list COMMA label_matcher { Ok($1?.append($3?)) }
        |       label_match_list LOR label_matcher { Ok($1?.append_or($3?)) }
        |       label_matcher { Ok(Matchers::empty().append($1?)) }
;

label_matcher -> Result<Matcher, String>:
                IDENTIFIER match_op STRING
                {
                        let name = lexeme_to_string($lexer, &$1)?;
                        let value = unquote_string(&lexeme_to_string($lexer, &$3)?)?;
                        Matcher::new_matcher($2?.id(), name, value)
                }
        |       string_identifier match_op STRING
                {
                        let name = $1?;
                        let value = unquote_string(&lexeme_to_string($lexer, &$3)?)?;
                        Matcher::new_matcher($2?.id(), name, value)
                }
        |       string_identifier
                {
                        Matcher::new_metric_name_matcher($1?)
                }
        |       IDENTIFIER match_op match_op
                {
                        let op = $3?.val;
                        Err(format!("unexpected '{op}' in label matching, expected string"))

                }
        |       string_identifier match_op match_op
                {
                        let op = $3?.val;
                        Err(format!("unexpected '{op}' in label matching, expected string"))

                }
        |       IDENTIFIER match_op match_op STRING
                {
                        let op = $3?.val;
                        Err(format!("unexpected '{op}' in label matching, expected string"))

                }
        |       string_identifier match_op match_op STRING
                {
                        let op = $3?.val;
                        Err(format!("unexpected '{op}' in label matching, expected string"))

                }
        |       IDENTIFIER match_op match_op IDENTIFIER
                {
                        let op = $3?.val;
                        Err(format!("unexpected '{op}' in label matching, expected string"))

                }
        |       string_identifier match_op match_op IDENTIFIER
                {
                        let op = $3?.val;
                        Err(format!("unexpected '{op}' in label matching, expected string"))

                }
        |       IDENTIFIER match_op IDENTIFIER
                {
                        let id = lexeme_to_string($lexer, &$3)?;
                        Err(format!("unexpected identifier '{id}' in label matching, expected string"))
                }
        |       string_identifier match_op IDENTIFIER
                {
                        let id = lexeme_to_string($lexer, &$3)?;
                        Err(format!("unexpected identifier '{id}' in label matching, expected string"))
                }
        |       IDENTIFIER
                {
                        let id = lexeme_to_string($lexer, &$1)?;
                        Err(format!("invalid label matcher, expected label matching operator after '{id}'"))
                }
;

/*
 * Metric descriptions.
 */
metric_identifier -> Result<Token, String>:
                AVG { lexeme_to_token($lexer, $1) }
        |       BOTTOMK { lexeme_to_token($lexer, $1) }
        |       BY { lexeme_to_token($lexer, $1) }
        |       COUNT { lexeme_to_token($lexer, $1) }
        |       COUNT_VALUES { lexeme_to_token($lexer, $1) }
        |       FILL { lexeme_to_token($lexer, $1) }
        |       FILL_LEFT { lexeme_to_token($lexer, $1) }
        |       FILL_RIGHT { lexeme_to_token($lexer, $1) }
        |       GROUP { lexeme_to_token($lexer, $1) }
        |       IDENTIFIER { lexeme_to_token($lexer, $1) }
        |       LAND { lexeme_to_token($lexer, $1) }
        |       LOR { lexeme_to_token($lexer, $1) }
        |       LUNLESS { lexeme_to_token($lexer, $1) }
        |       MAX { lexeme_to_token($lexer, $1) }
        |       METRIC_IDENTIFIER { lexeme_to_token($lexer, $1) }
        |       MIN { lexeme_to_token($lexer, $1) }
        |       OFFSET { lexeme_to_token($lexer, $1) }
        |       QUANTILE { lexeme_to_token($lexer, $1) }
        |       STDDEV { lexeme_to_token($lexer, $1) }
        |       STDVAR { lexeme_to_token($lexer, $1) }
        |       SUM { lexeme_to_token($lexer, $1) }
        |       TOPK { lexeme_to_token($lexer, $1) }
        |       LIMITK { lexeme_to_token($lexer, $1) }
        |       LIMIT_RATIO { lexeme_to_token($lexer, $1) }
        |       WITHOUT { lexeme_to_token($lexer, $1) }
        |       START { lexeme_to_token($lexer, $1) }
        |       END { lexeme_to_token($lexer, $1) }
        // PulsusDB patch (docs/decisions/0003, grammar patch G1): the
        // duration-expression keywords stay usable as metric names
        // (upstream v3.13 metric_identifier).
        |       STEP { lexeme_to_token($lexer, $1) }
        |       RANGE { lexeme_to_token($lexer, $1) }
        |       MAX_OF { lexeme_to_token($lexer, $1) }
        |       MIN_OF { lexeme_to_token($lexer, $1) }
        // PulsusDB patch (docs/decisions/0003, grammar patch G3): the
        // extended-range-selector keywords stay usable as metric names
        // (upstream v3.13 metric_identifier:837).
        |       ANCHORED { lexeme_to_token($lexer, $1) }
        |       SMOOTHED { lexeme_to_token($lexer, $1) }
;

/*
 * Series descriptions (only used by unit tests).
 * Note: this is not supported yet.
 */

/*
 * Keyword lists.
 */
aggregate_op -> Result<Token, String>:
                AVG { lexeme_to_token($lexer, $1) }
        |       BOTTOMK { lexeme_to_token($lexer, $1) }
        |       COUNT { lexeme_to_token($lexer, $1) }
        |       COUNT_VALUES { lexeme_to_token($lexer, $1) }
        |       GROUP { lexeme_to_token($lexer, $1) }
        |       MAX { lexeme_to_token($lexer, $1) }
        |       MIN { lexeme_to_token($lexer, $1) }
        |       QUANTILE { lexeme_to_token($lexer, $1) }
        |       STDDEV { lexeme_to_token($lexer, $1) }
        |       STDVAR { lexeme_to_token($lexer, $1) }
        |       SUM { lexeme_to_token($lexer, $1) }
        |       TOPK { lexeme_to_token($lexer, $1) }
        |       LIMITK { lexeme_to_token($lexer, $1) }
        |       LIMIT_RATIO { lexeme_to_token($lexer, $1) }
;

// inside of grouping options label names can be recognized as keywords by the lexer.
// This is a list of keywords that could also be a label name.
maybe_label -> Result<Token, String>:
                AVG { lexeme_to_token($lexer, $1) }
        |       BOOL { lexeme_to_token($lexer, $1) }
        |       BOTTOMK { lexeme_to_token($lexer, $1) }
        |       BY { lexeme_to_token($lexer, $1) }
        |       COUNT { lexeme_to_token($lexer, $1) }
        |       COUNT_VALUES { lexeme_to_token($lexer, $1) }
        |       FILL { lexeme_to_token($lexer, $1) }
        |       FILL_LEFT { lexeme_to_token($lexer, $1) }
        |       FILL_RIGHT { lexeme_to_token($lexer, $1) }
        |       GROUP { lexeme_to_token($lexer, $1) }
        |       GROUP_LEFT { lexeme_to_token($lexer, $1) }
        |       GROUP_RIGHT { lexeme_to_token($lexer, $1) }
        |       IDENTIFIER { lexeme_to_token($lexer, $1) }
        |       IGNORING { lexeme_to_token($lexer, $1) }
        |       LAND { lexeme_to_token($lexer, $1) }
        |       LOR { lexeme_to_token($lexer, $1) }
        |       LUNLESS { lexeme_to_token($lexer, $1) }
        |       MAX { lexeme_to_token($lexer, $1) }
        |       METRIC_IDENTIFIER { lexeme_to_token($lexer, $1) }
        |       MIN { lexeme_to_token($lexer, $1) }
        |       OFFSET { lexeme_to_token($lexer, $1) }
        |       ON { lexeme_to_token($lexer, $1) }
        |       QUANTILE { lexeme_to_token($lexer, $1) }
        |       STDDEV { lexeme_to_token($lexer, $1) }
        |       STDVAR { lexeme_to_token($lexer, $1) }
        |       SUM { lexeme_to_token($lexer, $1) }
        |       TOPK { lexeme_to_token($lexer, $1) }
        |       LIMITK { lexeme_to_token($lexer, $1) }
        |       LIMIT_RATIO { lexeme_to_token($lexer, $1) }
        |       START { lexeme_to_token($lexer, $1) }
        |       END { lexeme_to_token($lexer, $1) }
        |       ATAN2 { lexeme_to_token($lexer, $1) }
        // PulsusDB patch (docs/decisions/0003, grammar patch G1): the
        // duration-expression keywords stay usable as label names
        // (upstream v3.13 maybe_label).
        |       STEP { lexeme_to_token($lexer, $1) }
        |       RANGE { lexeme_to_token($lexer, $1) }
        |       MAX_OF { lexeme_to_token($lexer, $1) }
        |       MIN_OF { lexeme_to_token($lexer, $1) }
        // PulsusDB patch (docs/decisions/0003, grammar patch G3): the
        // extended-range-selector keywords stay usable as label names
        // (upstream v3.13 maybe_label:1095).
        |       ANCHORED { lexeme_to_token($lexer, $1) }
        |       SMOOTHED { lexeme_to_token($lexer, $1) }
;

match_op -> Result<Token, String>:
                EQL { lexeme_to_token($lexer, $1) }
        |       NEQ { lexeme_to_token($lexer, $1) }
        |       EQL_REGEX { lexeme_to_token($lexer, $1) }
        |       NEQ_REGEX { lexeme_to_token($lexer, $1) }
;

/*
 * Literals.
 */
number_literal -> Result<Expr, String>:
                NUMBER
                {
                        let num = parse_str_radix($lexer.span_str($span))?;
                        Ok(Expr::from(num).with_pos_start($span.start()))
                }
        |       DURATION
                {
                        let duration = parse_duration($lexer.span_str($span))?;
                        Ok(Expr::from(duration.as_secs_f64()).with_pos_start($span.start()))
                }
;

string_literal -> Result<Expr, String>:
                STRING { Ok(Expr::from(unquote_string(&span_to_string($lexer, $span))?).with_pos_start($span.start())) }
;

string_identifier -> Result<String, String>:
                STRING {
                        let name = unquote_string(&span_to_string($lexer, $span))?;
                        Ok(name)
                }
;

// PulsusDB patch (docs/decisions/0003, grammar patch G1): upstream
// v3.13's number_duration_literal — a NUMBER is plain seconds, a DURATION
// folds through parse_duration (which carries the leaf overflow-bound
// patch, PATCHES.md fix 3) to seconds. The out-of-range bound for bare
// numbers now lives in the duration-expression literal guards
// (`checked_number_literal`/`apply_unary_op_to_duration_expr`), matching
// upstream's `durationLiteralOutOfRange` placement.
number_duration_literal -> Result<DurationExpr, String>:
                NUMBER
                {
                        parse_str_radix($lexer.span_str($span)).map(DurationExpr::Number)
                }
        |       DURATION
                {
                        parse_duration($lexer.span_str($span)).map(|d| DurationExpr::Number(d.as_secs_f64()))
                }
;

%%

use std::time::Duration;
use crate::label::{Labels, Matcher, Matchers};
use crate::parser::{AtModifier, BinModifier, DurationExpr, Expr, FunctionArgs, LabelModifier, Offset, VectorMatchCardinality, VectorMatchFillValues};
use crate::parser::ast::check_ast;
use crate::parser::function::get_function;
use crate::parser::lex::is_label;
use crate::parser::production::{lexeme_to_string, lexeme_to_token, span_to_string};
use crate::parser::token::{Token, TokenId, T_ADD, T_DIV, T_IDENTIFIER, T_MAX_OF, T_MOD, T_MUL, T_POW, T_SUB};
use crate::util::{parse_duration, parse_str_radix, unquote_string};

// ---------------------------------------------------------------------------
// PulsusDB patch (docs/decisions/0003, grammar patch G1): duration-expression
// action helpers, mirroring upstream v3.13 parse.go/generated_parser.y.
// ---------------------------------------------------------------------------

/// Upstream `durationLiteralOutOfRange`: whether `secs`, as seconds, would
/// overflow Go's `time.Duration` (`i64` nanoseconds).
fn duration_literal_out_of_range(secs: f64) -> bool {
    const MAX: f64 = (1u64 << 63) as f64 / 1e9;
    secs > MAX || secs < -MAX
}

/// Range-checks a literal duration sub-expression (upstream applies
/// `durationLiteralOutOfRange` to every literal alternative of
/// `duration_expr`/`offset_duration_expr`).
fn checked_number_literal(e: DurationExpr) -> Result<DurationExpr, String> {
    match e {
        DurationExpr::Number(secs) if duration_literal_out_of_range(secs) => {
            Err("duration out of range".into())
        }
        e => Ok(e),
    }
}

/// Converts a non-negative literal seconds value (already range- and
/// positivity-checked by the grammar) into a concrete `Duration`.
fn duration_from_secs(secs: f64) -> Result<Duration, String> {
    if !secs.is_finite() || secs < 0.0 {
        // Unreachable via the grammar's own guards; kept total.
        return Err("duration out of range".into());
    }
    Ok(Duration::from_nanos((secs * 1e9).round() as u64))
}

/// Converts a literal offset seconds value into the signed `Offset`.
fn offset_from_secs(secs: f64) -> Result<Offset, String> {
    if secs < 0.0 {
        Ok(Offset::Neg(duration_from_secs(-secs)?))
    } else {
        Ok(Offset::Pos(duration_from_secs(secs)?))
    }
}

/// Upstream `applyUnaryOpToDurationExpr`: a unary op over a literal folds
/// into the (range-checked) literal — parentheses included, so
/// `offset -(5)` is the concrete literal `-5s`; over anything else it
/// builds a `Pos`/`Neg` node (wrapping parenthesised operands first).
fn apply_unary_op_to_duration_expr(
    op: Token,
    e: DurationExpr,
    wrapped: bool,
) -> Result<DurationExpr, String> {
    match e {
        DurationExpr::Number(secs) => {
            let secs = if op.id() == T_SUB { -secs } else { secs };
            if duration_literal_out_of_range(secs) {
                return Err("duration out of range".into());
            }
            Ok(DurationExpr::Number(secs))
        }
        e => {
            let e = if wrapped && !matches!(e, DurationExpr::Wrapped(_)) {
                DurationExpr::Wrapped(Box::new(e))
            } else {
                e
            };
            Ok(unary_duration_expr(op, e))
        }
    }
}

/// Builds the unary node for a non-literal operand.
fn unary_duration_expr(op: Token, e: DurationExpr) -> DurationExpr {
    if op.id() == T_SUB {
        DurationExpr::Neg(Box::new(e))
    } else {
        DurationExpr::Pos(Box::new(e))
    }
}

fn min_of_max_of_expr(op: Token, lhs: DurationExpr, rhs: DurationExpr) -> DurationExpr {
    if op.id() == T_MAX_OF {
        DurationExpr::MaxOf(Box::new(lhs), Box::new(rhs))
    } else {
        DurationExpr::MinOf(Box::new(lhs), Box::new(rhs))
    }
}

/// Builds a binary duration node; division/modulo by a *literal* zero is a
/// parse error (upstream generated_parser.y), by a computed zero a
/// resolve-time error downstream. Parenthesised/unary-signed zero literals
/// (`(0)`, `-(0)`) count as literal zeros — upstream folds them to
/// `*NumberLiteral`s before its `nl.Val == 0` check.
fn duration_binary_expr(
    op: TokenId,
    lhs: DurationExpr,
    rhs: DurationExpr,
) -> Result<DurationExpr, String> {
    let is_literal_zero = rhs.literal_value() == Some(0.0);
    let (lhs, rhs) = (Box::new(lhs), Box::new(rhs));
    match op {
        T_ADD => Ok(DurationExpr::Add(lhs, rhs)),
        T_SUB => Ok(DurationExpr::Sub(lhs, rhs)),
        T_MUL => Ok(DurationExpr::Mul(lhs, rhs)),
        T_DIV if is_literal_zero => Err("division by zero".into()),
        T_DIV => Ok(DurationExpr::Div(lhs, rhs)),
        T_MOD if is_literal_zero => Err("modulo by zero".into()),
        T_MOD => Ok(DurationExpr::Mod(lhs, rhs)),
        T_POW => Ok(DurationExpr::Pow(lhs, rhs)),
        // Unreachable: the grammar only routes the six operators above here.
        _ => Err("unexpected duration expression operator".into()),
    }
}

fn update_optional_matching(
    modifier: Option<BinModifier>,
    matching: Option<LabelModifier>,
) -> Option<BinModifier> {
    let modifier = modifier.unwrap_or_default();
    Some(modifier.with_matching(matching))
}

fn update_optional_card(
    modifier: Option<BinModifier>,
    card: VectorMatchCardinality,
) -> Option<BinModifier> {
    let modifier = modifier.unwrap_or_default();
    Some(modifier.with_card(card))
}

fn update_optional_fill(
    modifier: Option<BinModifier>,
    fill: VectorMatchFillValues,
) -> Option<BinModifier> {
    let modifier = modifier.unwrap_or_default();
    Some(modifier.with_fill_values(fill))
}
