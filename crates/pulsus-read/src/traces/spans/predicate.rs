//! The span-scope predicate compiler, part 1 (issue #588).
//!
//! One TraceQL leaf to one ClickHouse boolean over a `spans` row, and the
//! one place a window and a predicate compose
//! ([`span_membership_sql`]). Nothing here reads or writes: both entry
//! points are pure.
//!
//! **What part 1 serves.** The `span.` attribute scope, and the ten span
//! intrinsics `spans` has a column for — `name`, `kind`, `status`,
//! `statusMessage`, `duration`, `span:id`, `span:parentID`, `trace:id`,
//! `instrumentation:name`, `instrumentation:version`. Every other field,
//! every other attribute scope, arithmetic, a field against a field and
//! the boolean connectives are [`PlanError::UnsupportedField`] naming the
//! construct and issue #589. The arms are exhaustive with no wildcard, so
//! a new `Intrinsic` or `AttrScope` variant fails to compile here rather
//! than falling into the wrong one.
//!
//! **Three rules worth reading before the code.**
//!
//! * A typed `JSON` subcolumn read is `Nullable`: ``attrs.`k`.:Int64`` is
//!   `Nullable(Int64)` for every row, and the value is `NULL` when the key
//!   is absent **or** stored at another type. So the typed read is
//!   coalesced and the negation is applied to the coalesced result —
//!   `NOT (coalesce(… = v, false) OR …)`. Without the `coalesce`,
//!   `false OR NULL` is `NULL`, `NOT NULL` is `NULL`, and a `WHERE` drops
//!   the row: measured on the worked fixture, that rendering answers
//!   nothing at all. With the negation pushed *inside* the arms it answers
//!   one row instead of eight. `!=` matching a span lacking the key is
//!   this product's documented semantics (`docs/api.md` §4.2) and a
//!   deliberate divergence recorded at
//!   `docs/TraceQL/functional-requirements.md`.
//! * **No intrinsic read is coalesced.** Every intrinsic column in
//!   `schema/schema.sql` is non-nullable, so there is no `NULL` to absorb
//!   and a `coalesce` there would be noise that reads as a rule.
//! * **A decimal integer literal does not go through `f64`.** `f64` loses
//!   integer exactness from `2^53` up, and all six comparison operators
//!   then answer differently; `maxInt` through `parse_num`/`render_num` is
//!   `9223372036854776000`, which ClickHouse types `UInt64`. [`render_number`]
//!   is where that is decided.

use pulsus_clickhouse::json_column::escape_json_path;
use pulsus_traceql::{
    AttrScope, ComparisonOp, Duration, Field, FieldExpr, FieldOp, Intrinsic, UnaryOp, Value,
};

use crate::logql::escape;
use crate::traces::filter::{
    PlanError, anchored_regex_sql, flip_comparison, kind_code, parse_num, render_num, sql_op,
    status_code,
};
use crate::traces::window_sql::WindowSql;

/// A ClickHouse boolean over a `spans` row. Private field, no public
/// constructor: [`compile_span_predicate`] and [`compile_span_leaf`] are
/// the only ways to obtain one, so no later task can splice un-escaped
/// query text into a statement. The posture [`WindowSql`] already has.
///
/// `Debug` is derived so a case that expected a refusal can print what it
/// got instead; it is not a second way to read the text.
#[derive(Debug, Clone)]
pub struct SpanPredicate(String);

impl SpanPredicate {
    pub fn sql(&self) -> &str {
        &self.0
    }

    /// STUB (issue #589 part 1, tests first).
    pub fn demand_messages(&self) -> &[String] {
        &[]
    }
}

/// What a predicate needs from the statement it will sit in.
#[derive(Debug, Clone, Copy)]
pub struct PredicateCtx<'a> {
    pub window: WindowSql,
    /// Unqualified in the live suite, `<db>.resources` in production.
    pub resources_table: &'a str,
}

/// STUB (issue #589 part 1, tests first).
pub fn compile_span_predicate_in(
    expr: &FieldExpr,
    _ctx: &PredicateCtx<'_>,
) -> Result<SpanPredicate, PlanError> {
    compile_span_predicate(expr)
}

/// STUB (issue #589 part 1, tests first).
pub fn compile_span_leaf_in(
    field: &Field,
    op: ComparisonOp,
    value: &Value,
    _ctx: &PredicateCtx<'_>,
) -> Result<SpanPredicate, PlanError> {
    compile_span_leaf(field, op, value)
}

/// Compiles one spanset filter body.
///
/// A literal on the LEFT is accepted and the operator mirrored, so
/// `{ 500 <= span.http.response.status_code }` and
/// `{ span.http.response.status_code >= 500 }` are one rendering —
/// [`flip_comparison`] is the same reflection the shipped compiler uses.
pub fn compile_span_predicate(expr: &FieldExpr) -> Result<SpanPredicate, PlanError> {
    Ok(SpanPredicate(render_expr(expr)?))
}

/// The leaf, for a caller that has already normalised to
/// `(field, op, value)` — `filter::compile_leaf`'s shape, so the two
/// compilers read against each other.
pub fn compile_span_leaf(
    field: &Field,
    op: ComparisonOp,
    value: &Value,
) -> Result<SpanPredicate, PlanError> {
    Ok(SpanPredicate(leaf(field, op, value)?))
}

/// The ONE place a window and a predicate compose. The live suite issues
/// it; `tests/traces_compile_v2.rs`'s `T-C4` freezes its text, so the
/// frozen text is what ran.
///
/// Issue it with `final = 1`, as every read on these tables is: `spans` is
/// a `ReplacingMergeTree` and a retried push collapses on the sorting key
/// only after a merge.
///
/// **The returned column is aliased `id`, not `span_id`, and that is not a
/// cosmetic choice.** A select-list alias shadows a column of the same
/// name for the whole statement, the `WHERE` included, so
/// `… AS span_id … WHERE lower(hex(span_id)) < '…'` compares the HEX of
/// the hex text. Measured on 26.3.29.7 over a two-row `FixedString(8)`
/// table: `SELECT lower(hex(span_id)) AS span_id, lower(hex(span_id)) AS
/// twice` returns `0a1b2c3d4e5f60a1` beside
/// `30613162326333643465356636306131`, and the same statement with a
/// `lower(hex(span_id)) < '0a1b2c3d4e5f60a4'` bound answers nothing where
/// the un-aliased spelling answers one row. Any predicate reading
/// `span_id` — the whole `span:id` family — is wrong under that alias.
pub fn span_membership_sql(spans_table: &str, w: WindowSql, p: &SpanPredicate) -> String {
    format!(
        "SELECT lower(hex(span_id)) AS id\n\
         FROM {spans_table}\n\
         WHERE {time}\n\
         \x20 AND {bucket}\n\
         \x20 AND {day}\n\
         \x20 AND ({pred})\n\
         ORDER BY id",
        time = w.span_time_clause(),
        bucket = w.span_bucket_clause(),
        day = w.span_day_clause(),
        pred = p.sql(),
    )
}

// ---------------------------------------------------------------------
// the expression level
// ---------------------------------------------------------------------

/// The refusal every deferred construct takes: it names the construct and
/// the issue that serves it.
fn unsupported(construct: &str) -> PlanError {
    PlanError::UnsupportedField(format!(
        "{construct} is not supported by the span-scope predicate compiler yet (issue #589)"
    ))
}

fn unsupported_scope(scope: AttrScope) -> PlanError {
    unsupported(&format!("the \"{scope}\" attribute scope"))
}

fn unsupported_intrinsic(intrinsic: Intrinsic) -> PlanError {
    unsupported(&format!("{intrinsic}"))
}

/// Whether `expr` is an arithmetic node — a binary arithmetic operator or
/// the unary minus, which is the same construct spelled as a prefix.
fn is_arithmetic(expr: &FieldExpr) -> bool {
    matches!(
        expr,
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            ..
        } | FieldExpr::Unary {
            op: UnaryOp::Neg,
            ..
        }
    )
}

fn render_expr(expr: &FieldExpr) -> Result<String, PlanError> {
    match expr {
        FieldExpr::Binary {
            op: FieldOp::Cmp(op),
            lhs,
            rhs,
        } => {
            if is_arithmetic(lhs) || is_arithmetic(rhs) {
                return Err(unsupported("arithmetic in a span-scope predicate"));
            }
            match (lhs.as_ref(), rhs.as_ref()) {
                (FieldExpr::Field(field), FieldExpr::Literal(value)) => leaf(field, *op, value),
                (FieldExpr::Literal(value), FieldExpr::Field(field)) => {
                    leaf(field, flip_comparison(*op), value)
                }
                (FieldExpr::Field(_), FieldExpr::Field(_)) => {
                    Err(unsupported("a field-against-field comparison"))
                }
                (FieldExpr::Literal(_), FieldExpr::Literal(_)) => {
                    Err(unsupported("a comparison between two literals"))
                }
                _ => Err(unsupported("this comparison's operand shape")),
            }
        }
        FieldExpr::Binary {
            op: FieldOp::Bool(op),
            ..
        } => Err(unsupported(&format!("the {op} operator"))),
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            ..
        } => Err(unsupported("arithmetic in a span-scope predicate")),
        FieldExpr::Unary { op, .. } => Err(unsupported(&format!("the {op} operator"))),
        // `{ span.k }` is TRUTHINESS, not presence: the reference matches a
        // span only where the value IS `true`, so this is plain equality
        // against the boolean literal and inherits the leaf's rendering.
        FieldExpr::Field(field) => leaf(field, ComparisonOp::Eq, &Value::Bool(true)),
        // `{ true }` is exactly `{ }` and `{ false }` matches nothing. A
        // non-boolean static never reaches here from the wire —
        // `pulsus_traceql::validate` rejects it with the reference's own
        // message — and is refused so the AST-constructed path stays total.
        FieldExpr::Literal(Value::Bool(b)) => Ok(if *b { "true" } else { "false" }.to_string()),
        FieldExpr::Literal(_) => Err(unsupported("a non-boolean literal at predicate position")),
        FieldExpr::Exists { field, negated } => exists(field, *negated),
    }
}

/// `{ .k != nil }` (presence) and `{ .k = nil }` (absence).
///
/// `dynamicType` and not a typed read: ``attrs.`k`.:String IS NOT NULL``
/// answers only the rows that store a string, so it misses an integer, a
/// boolean and an array. The two spellings' meanings differ, which is why
/// the AST carries the negation rather than a canonical form.
fn exists(field: &Field, negated: bool) -> Result<String, PlanError> {
    let key = match field {
        Field::Attribute {
            scope: AttrScope::Span,
            key,
        } => key,
        Field::Attribute { scope, .. } => return Err(unsupported_scope(*scope)),
        Field::Intrinsic(_) => {
            return Err(PlanError::TypeMismatch(
                "existence checks are only supported on attributes".to_string(),
            ));
        }
    };
    let op = if negated { "=" } else { "!=" };
    Ok(format!("dynamicType({}) {op} 'None'", attr_path(key)))
}

fn leaf(field: &Field, op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    match field {
        Field::Attribute {
            scope: AttrScope::Span,
            key,
        } => attr_leaf(key, op, value),
        Field::Attribute { scope, .. } => Err(unsupported_scope(*scope)),
        Field::Intrinsic(intrinsic) => intrinsic_leaf(*intrinsic, op, value),
    }
}

// ---------------------------------------------------------------------
// the attribute path
// ---------------------------------------------------------------------

/// `attrs.` then the stored JSON path, backtick-quoted. Two escapes, in
/// this order, and neither is optional.
///
/// 1. [`escape_json_path`] — `%` to `%25`, then `.` to `%2E`. It is the
///    **writer's** function: it is what put the path there, so the reader
///    must not have a second copy. A dot is the server's own nesting
///    separator, so `http.response.status_code` is stored as the single
///    path `http%2Eresponse%2Estatus_code`.
/// 2. [`escape::ch_ident`] — backtick-quote, escaping `` ` `` and `\`.
///    This is the first place in the product where a client-chosen OTLP
///    key becomes a SQL **identifier**; `ch_ident`'s own doc carries the
///    measurement.
fn attr_path(key: &str) -> String {
    format!("attrs.{}", escape::ch_ident(&escape_json_path(key)))
}

// ---------------------------------------------------------------------
// the attribute leaf
// ---------------------------------------------------------------------

/// A string equality: the `:String` arm and the array-membership arm,
/// both, because the compiler has no stored type for a key and
/// `docs/TraceQL/server-implementation.md` gives array membership its own
/// rule with no way to select it. The array arm is inert when the value is
/// not an `Array(Nullable(String))` — measured on the worked fixture, each
/// arm alone answers one of the two rows and neither answers both.
///
/// `=~`, `!~` and the four ordered operators read `:String` only: the
/// design names `= "v"` and nothing else, and an ordered comparison
/// against array elements has no stated meaning.
fn string_eq(path: &str, literal: &str) -> String {
    let lit = escape::ch_string(literal);
    format!(
        "(coalesce({path}.:String = {lit}, false) OR \
         has({path}.:`Array(Nullable(String))`, {lit}))"
    )
}

/// The numeric pair, `Int64` before `Float64` in both directions.
///
/// The order is the one `docs/TraceQL/query-catalogue-accepted.md` carries
/// on all 48 of its coalesced numeric lines; the measurement script
/// disagrees with itself about it, so the catalogue is the authority and
/// the order is fixed once here.
fn numeric_pair(path: &str, op: ComparisonOp, number: &str) -> String {
    let sym = sql_op(op).expect("the caller has already excluded the regex operators");
    format!(
        "(coalesce({path}.:Int64 {sym} {number}, false) OR \
         coalesce({path}.:Float64 {sym} {number}, false))"
    )
}

/// `N` — the numeric literal's SQL text. **Integer first**: `parse_num` /
/// `render_num` are used only for a literal that is not an `i64`, because
/// the `f64` round trip does not preserve one.
///
/// `minInt`/`maxInt` need no arm of their own: the parser turns each into
/// `Value::Number(i64::MIN.to_string())` / `i64::MAX.to_string()`, so they
/// arrive as ordinary decimal literals and take this same path.
fn render_number(raw: &str) -> Result<String, PlanError> {
    if let Ok(i) = raw.parse::<i64>() {
        return Ok(i.to_string());
    }
    Ok(render_num(parse_num(raw)?))
}

/// A duration literal's nanoseconds, which is the number it compares as —
/// not the digits the client wrote, so `> 1ns` and `> 1us` are different
/// comparisons.
fn duration_nanos(d: Duration) -> Result<String, PlanError> {
    i64::try_from(d.as_nanos())
        .map(|n| n.to_string())
        .map_err(|_| PlanError::TypeMismatch("duration literal exceeds the i64 range".to_string()))
}

fn attr_leaf(key: &str, op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let path = attr_path(key);
    let rendered = match (op, value) {
        (ComparisonOp::Eq, Value::String(s)) => string_eq(&path, s),
        (ComparisonOp::Neq, Value::String(s)) => format!("NOT {}", string_eq(&path, s)),
        (
            ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte,
            Value::String(s),
        ) => {
            format!(
                "coalesce({path}.:String {} {}, false)",
                sql_op(op).expect("an ordered operator"),
                escape::ch_string(s)
            )
        }
        (ComparisonOp::Re, Value::String(s)) => {
            format!(
                "coalesce(match({path}.:String, {}), false)",
                anchored_regex_sql(s)?
            )
        }
        (ComparisonOp::Nre, Value::String(s)) => {
            format!(
                "NOT coalesce(match({path}.:String, {}), false)",
                anchored_regex_sql(s)?
            )
        }
        (ComparisonOp::Neq, Value::Number(raw)) => format!(
            "NOT {}",
            numeric_pair(&path, ComparisonOp::Eq, &render_number(raw)?)
        ),
        (op, Value::Number(raw)) if sql_op(op).is_some() => {
            numeric_pair(&path, op, &render_number(raw)?)
        }
        (ComparisonOp::Neq, Value::Duration(d)) => format!(
            "NOT {}",
            numeric_pair(&path, ComparisonOp::Eq, &duration_nanos(*d)?)
        ),
        (op, Value::Duration(d)) if sql_op(op).is_some() => {
            numeric_pair(&path, op, &duration_nanos(*d)?)
        }
        (ComparisonOp::Eq, Value::Bool(b)) => format!("coalesce({path}.:Bool = {b}, false)"),
        (ComparisonOp::Neq, Value::Bool(b)) => {
            format!("NOT (coalesce({path}.:Bool = {b}, false))")
        }
        // The message `filter.rs` produces for the same pair, so
        // `{ span.k = error }` is the same `400` on both compilers.
        _ => {
            return Err(PlanError::TypeMismatch(format!(
                "attribute {key:?} does not support operator {op} on this value type"
            )));
        }
    };
    Ok(rendered)
}

// ---------------------------------------------------------------------
// the intrinsics
// ---------------------------------------------------------------------

/// **No wildcard arm**: a new `Intrinsic` variant fails to compile here
/// rather than falling silently into the wrong half.
fn intrinsic_leaf(
    intrinsic: Intrinsic,
    op: ComparisonOp,
    value: &Value,
) -> Result<String, PlanError> {
    match intrinsic {
        Intrinsic::Name => string_column_leaf(intrinsic, "name", op, value),
        Intrinsic::StatusMessage => string_column_leaf(intrinsic, "status_message", op, value),
        Intrinsic::Kind => kind_leaf(op, value),
        Intrinsic::Status => status_leaf(op, value),
        Intrinsic::Duration => duration_leaf(op, value),
        Intrinsic::SpanId => id_leaf(intrinsic, IdColumn::SPAN_ID, op, value),
        Intrinsic::ParentId => id_leaf(intrinsic, IdColumn::PARENT_SPAN_ID, op, value),
        Intrinsic::TraceId => id_leaf(intrinsic, IdColumn::TRACE_ID, op, value),
        Intrinsic::InstrumentationName => untyped_string_leaf(intrinsic, "scope_name", op, value),
        Intrinsic::InstrumentationVersion => {
            untyped_string_leaf(intrinsic, "scope_version", op, value)
        }
        Intrinsic::NestedSetParent
        | Intrinsic::NestedSetLeft
        | Intrinsic::NestedSetRight
        | Intrinsic::ChildCount
        | Intrinsic::TraceDuration
        | Intrinsic::RootName
        | Intrinsic::RootServiceName
        | Intrinsic::EventName
        | Intrinsic::EventTimeSinceStart
        | Intrinsic::LinkSpanId
        | Intrinsic::LinkTraceId => Err(unsupported_intrinsic(intrinsic)),
    }
}

/// `name` and `statusMessage`: a non-nullable `String`/`LowCardinality`
/// column, so no `coalesce`.
///
/// **All eight operators**, which is where this compiler differs from the
/// shipped one. `{ name > "x" }` is a reference `200` — strings order —
/// and our own validator accepts it
/// (`crates/pulsus-traceql/src/validate.rs`); `filter.rs`'s
/// `string_op_leaf` answers `400` there, and that is strictness this
/// compiler does not inherit.
fn string_column_leaf(
    intrinsic: Intrinsic,
    column: &str,
    op: ComparisonOp,
    value: &Value,
) -> Result<String, PlanError> {
    let Value::String(s) = value else {
        return Err(PlanError::TypeMismatch(format!(
            "{intrinsic} requires a string value"
        )));
    };
    Ok(match op {
        ComparisonOp::Re => format!("match({column}, {})", anchored_regex_sql(s)?),
        ComparisonOp::Nre => format!("NOT match({column}, {})", anchored_regex_sql(s)?),
        other => format!(
            "{column} {} {}",
            sql_op(other).expect("the six non-regex operators"),
            escape::ch_string(s)
        ),
    })
}

/// `instrumentation:name` / `instrumentation:version`, which keep
/// `filter.rs`'s rule unchanged — the cross-type `=`/`!=` fold to `false`
/// included.
///
/// `docs/api.md` names these two fields (with `resource.service.name`) and
/// says of them that a cross-type `=~`/`!~` and **any ordered comparison
/// at those fields stay `400`**. That sentence names the fields and the
/// operators; the field-vs-field paragraph beside it names neither, so it
/// does not reach here.
fn untyped_string_leaf(
    intrinsic: Intrinsic,
    column: &str,
    op: ComparisonOp,
    value: &Value,
) -> Result<String, PlanError> {
    if !matches!(
        op,
        ComparisonOp::Eq | ComparisonOp::Neq | ComparisonOp::Re | ComparisonOp::Nre
    ) {
        return Err(PlanError::TypeMismatch(format!(
            "{intrinsic} supports only = != =~ !~"
        )));
    }
    match value {
        Value::String(s) => Ok(match op {
            ComparisonOp::Re => format!("match({column}, {})", anchored_regex_sql(s)?),
            ComparisonOp::Nre => format!("NOT match({column}, {})", anchored_regex_sql(s)?),
            other => format!(
                "{column} {} {}",
                sql_op(other).expect("= or !="),
                escape::ch_string(s)
            ),
        }),
        // A cross-type `=`/`!=` is a query the reference's own validator
        // accepts and answers, so it matches no span rather than being a
        // plan error. Both spellings fold to `false`, as `filter.rs`'s
        // `never_matching_leaf` does.
        _ if matches!(op, ComparisonOp::Eq | ComparisonOp::Neq) => Ok("false".to_string()),
        _ => Err(PlanError::TypeMismatch(format!(
            "{intrinsic} requires a string value"
        ))),
    }
}

fn kind_leaf(op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let Value::Kind(k) = value else {
        return Err(PlanError::TypeMismatch(
            "kind requires a span-kind keyword".to_string(),
        ));
    };
    if !matches!(op, ComparisonOp::Eq | ComparisonOp::Neq) {
        return Err(PlanError::TypeMismatch(
            "kind supports only = and !=".to_string(),
        ));
    }
    Ok(format!(
        "kind {} {}",
        sql_op(op).expect("= or !="),
        kind_code(*k)
    ))
}

fn status_leaf(op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let Value::Status(s) = value else {
        return Err(PlanError::TypeMismatch(
            "status requires ok|error|unset".to_string(),
        ));
    };
    if !matches!(op, ComparisonOp::Eq | ComparisonOp::Neq) {
        return Err(PlanError::TypeMismatch(
            "status supports only = and !=".to_string(),
        ));
    }
    Ok(format!(
        "status_code {} {}",
        sql_op(op).expect("= or !="),
        status_code(*s)
    ))
}

/// `duration` reads `duration_ns`, the stored column — not
/// `end_ns - start_ns`, which is the same number on the worked fixture and
/// so is not what distinguishes them.
///
/// The operand is checked BEFORE the operator, which is `filter.rs`'s own
/// order: that is what makes `{ duration =~ "x" }` answer "requires a
/// duration literal" on both compilers rather than two different messages
/// for one query.
fn duration_leaf(op: ComparisonOp, value: &Value) -> Result<String, PlanError> {
    let Value::Duration(d) = value else {
        return Err(PlanError::TypeMismatch(
            "duration requires a duration literal".to_string(),
        ));
    };
    let Some(sym) = sql_op(op) else {
        return Err(PlanError::TypeMismatch(
            "duration does not support regex operators".to_string(),
        ));
    };
    Ok(format!("duration_ns {sym} {}", duration_nanos(*d)?))
}

// ---------------------------------------------------------------------
// the three id columns
// ---------------------------------------------------------------------

/// One id column: its name, its stored byte width, and whether the
/// raw-byte literal is wrapped back into the column's own `FixedString`.
///
/// **The wrap is on `trace_id` alone, and it is not cosmetic.**
/// `trace_id` is the sorting key's second column, so the raw-byte
/// comparison there is a key read and giving the right side the column's
/// own type is what keeps it one; `span_id` sits fourth in that key and no
/// key read is claimed for it. `toFixedString` over exactly `width` bytes
/// is the identity, so no answer moves either way —
/// `tests/traces_compile_v2.rs`'s `T-C9` is the only instrument.
#[derive(Debug, Clone, Copy)]
struct IdColumn {
    name: &'static str,
    byte_width: usize,
    wrap_fixed_string: bool,
}

impl IdColumn {
    const SPAN_ID: IdColumn = IdColumn {
        name: "span_id",
        byte_width: 8,
        wrap_fixed_string: false,
    };
    const PARENT_SPAN_ID: IdColumn = IdColumn {
        name: "parent_span_id",
        byte_width: 8,
        wrap_fixed_string: false,
    };
    const TRACE_ID: IdColumn = IdColumn {
        name: "trace_id",
        byte_width: 16,
        wrap_fixed_string: true,
    };
}

/// `span:id`, `span:parentID`, `trace:id`.
///
/// Two rules, both answer-inverting if dropped:
///
/// * **The literal is lowercased for `=`/`!=` and for nothing else.** The
///   stored hex is lowercase, so an uppercase probe must be folded to
///   match it — but a regex may be deliberately case-sensitive, and an
///   ordered comparison is lexicographic on the hex TEXT, where `'0a'`
///   and `'0A'` are different bounds.
/// * **The raw-byte fast path carries `<op>`, not a hard-coded `=`.**
///
/// The width guard is the third: `unhex('')` is zero bytes and a
/// `FixedString` right-pads, so without it `{ span:parentID = "" }` equals
/// every root span instead of nothing.
fn id_leaf(
    intrinsic: Intrinsic,
    column: IdColumn,
    op: ComparisonOp,
    value: &Value,
) -> Result<String, PlanError> {
    let Value::String(raw) = value else {
        return Err(PlanError::TypeMismatch(format!(
            "{intrinsic} requires a string value"
        )));
    };
    let equality = matches!(op, ComparisonOp::Eq | ComparisonOp::Neq);
    let literal = if equality {
        raw.to_lowercase()
    } else {
        raw.clone()
    };
    let full_width_hex = literal.len() == 2 * column.byte_width
        && literal
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if equality && full_width_hex {
        let bytes = format!("unhex({})", escape::ch_string(&literal));
        let right = if column.wrap_fixed_string {
            format!("toFixedString({bytes}, {})", column.byte_width)
        } else {
            bytes
        };
        return Ok(format!(
            "{} {} {right}",
            column.name,
            sql_op(op).expect("= or !=")
        ));
    }
    let hex = format!("lower(hex({}))", column.name);
    Ok(match op {
        ComparisonOp::Re => format!("match({hex}, {})", anchored_regex_sql(&literal)?),
        ComparisonOp::Nre => format!("NOT match({hex}, {})", anchored_regex_sql(&literal)?),
        other => format!(
            "{hex} {} {}",
            sql_op(other).expect("the six non-regex operators"),
            escape::ch_string(&literal)
        ),
    })
}
