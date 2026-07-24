//! The TraceQL AST — the stable contract the T5 planner/SQL generator
//! consumes (docs/architecture.md §5.4). Every type derives `Debug`,
//! `Clone`, `PartialEq`, `Eq`, `Hash` (the last so the AST can key a
//! plan cache; also load-bearing for the `Display` round-trip oracle:
//! `parse(ast.to_string()) == ast`).
//!
//! [`PipelineStage`] is the metrics/aggregate growth point. Issue #59
//! shipped the zero-arity [`PipelineStage::Metric`] set (`rate()`,
//! `count_over_time()`); issue #182 completes the first-stage
//! `*_over_time` family (carried by [`MetricFn`] with its aggregation
//! target), the `by(...)`/`with(...)` clauses ([`MetricStage`]), and the
//! `topk`/`bottomk` [`SecondStage`] operators. The remaining
//! recognized-but-unsupported constructs are reported as `NotYetSupported`
//! (see [`BOUNDARY_CONSTRUCTS`]).

use std::fmt;

/// A parsed TraceQL search query: a spanset expression plus its pipeline
/// of aggregate filters / `select` stages (docs/features.md §4, M4
/// coverage line).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Query {
    pub spanset: SpansetExpr,
    pub pipeline: Vec<PipelineStage>,
    /// Trailing `with(...)` hints on a non-metric query (issue #185 —
    /// `hints.most_recent`): `{ … } with(most_recent=true)`. Empty when
    /// absent. Reuses [`MetricHint`]/[`HintValue`]; `most_recent` is a
    /// recognized key.
    pub hints: Vec<MetricHint>,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.spanset)?;
        for stage in &self.pipeline {
            write!(f, " | {stage}")?;
        }
        if !self.hints.is_empty() {
            write!(f, " with(")?;
            for (i, hint) in self.hints.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{hint}")?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}

/// A spanset expression: a single filter, `&&`/`||` composition *across*
/// spansets (`{...} && {...}`), or a structural relation (`>`/`>>`/`~` —
/// issue #172, the additive M7 variant). Parentheses are structural only
/// — `( expr )` is a valid primary and produces no AST node, so the
/// fully-parenthesized `Display` rendering always reparses to an equal
/// AST.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SpansetExpr {
    Filter(SpansetFilter),
    Binary {
        op: BoolOp,
        lhs: Box<SpansetExpr>,
        rhs: Box<SpansetExpr>,
    },
    /// `{A} op {B}` — a structural relation evaluated over one trace's
    /// span graph (issue #172 shipped `>`/`>>`/`~`; issue #183 completes
    /// the surface with `<`/`<<` and the negated/union modifiers). Binds
    /// tighter than `&&`/`||`, left-associative. The result set depends on
    /// the [`StructuralModifier`]: Plain returns the RHS spans satisfying
    /// the relation, Negated returns the RHS spans NOT satisfying it, and
    /// Union returns both participating sides (docs/api.md §4.2).
    Structural {
        op: StructuralOp,
        modifier: StructuralModifier,
        lhs: Box<SpansetExpr>,
        rhs: Box<SpansetExpr>,
    },
}

impl fmt::Display for SpansetExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpansetExpr::Filter(filter) => write!(f, "{filter}"),
            SpansetExpr::Binary { op, lhs, rhs } => write!(f, "({lhs} {op} {rhs})"),
            SpansetExpr::Structural {
                op,
                modifier,
                lhs,
                rhs,
            } => write!(f, "({lhs} {}{op} {rhs})", modifier.prefix()),
        }
    }
}

/// The direction/kind of a structural relation (issue #172 + #183). The
/// [`StructuralModifier`] is orthogonal: it selects which spans of the
/// relation are returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StructuralOp {
    /// `>` — spans matching the RHS whose direct parent matches the LHS.
    Child,
    /// `>>` — spans matching the RHS with any transitive ancestor
    /// matching the LHS.
    Descendant,
    /// `<` — spans matching the RHS that are the direct parent of an
    /// LHS-matching span (issue #183).
    Parent,
    /// `<<` — spans matching the RHS that are a transitive ancestor of an
    /// LHS-matching span (issue #183).
    Ancestor,
    /// `~` — spans matching the RHS sharing a parent with a *distinct*
    /// span matching the LHS (all-zero `parent_id` roots never match).
    Sibling,
}

impl fmt::Display for StructuralOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            StructuralOp::Child => ">",
            StructuralOp::Descendant => ">>",
            StructuralOp::Parent => "<",
            StructuralOp::Ancestor => "<<",
            StructuralOp::Sibling => "~",
        };
        write!(f, "{s}")
    }
}

/// Which spans of a structural relation are returned (issue #183). The
/// modifier is spelled as a prefix on the operator: none for Plain, `!`
/// for Negated (`!>`, `!~`, …), `&` for Union (`&>`, `&~`, …).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StructuralModifier {
    /// The RHS spans satisfying the relation (the shipped #172 semantics).
    Plain,
    /// The RHS spans NOT satisfying the relation (`!>`, `!>>`, `!<`,
    /// `!<<`, `!~`). With an empty LHS every RHS span is a match.
    Negated,
    /// Both participating sides of the relation (`&>`, `&>>`, `&<`, `&<<`,
    /// `&~`).
    Union,
}

impl StructuralModifier {
    /// The operator prefix this modifier renders with (`Display` oracle).
    fn prefix(self) -> &'static str {
        match self {
            StructuralModifier::Plain => "",
            StructuralModifier::Negated => "!",
            StructuralModifier::Union => "&",
        }
    }
}

/// `{ FieldExpr? }` — `body: None` is the `{}` match-all spanset
/// (time-range-only search, a real Tempo usage T5/T9 must serve —
/// task-manager adjudication 3).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpansetFilter {
    pub body: Option<FieldExpr>,
}

impl fmt::Display for SpansetFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.body {
            None => write!(f, "{{}}"),
            Some(body) => write!(f, "{{ {body} }}"),
        }
    }
}

/// A field-level expression inside a spanset filter: comparisons composed
/// with `&&`/`||` (`&&` binds tighter). Parentheses are structural only,
/// as at the spanset level.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldExpr {
    Comparison {
        field: Field,
        op: ComparisonOp,
        value: Value,
    },
    /// `{lhs} op {rhs}` — a field-vs-field comparison (issue #183,
    /// `comparison.rhs_attribute`): either side an attribute or an
    /// intrinsic, compared per-span. Regex operators are rejected at parse
    /// time (a field RHS never carries a regex).
    FieldCompare {
        lhs: Field,
        op: ComparisonOp,
        rhs: Field,
    },
    /// A bare boolean static (`{ true }` / `{ false }` — issue #183,
    /// `static.bare_boolean`): matches every span or no span.
    BoolStatic(bool),
    /// Attribute existence (issue #185, `existence.*`): the span possesses
    /// the field. The bare-attribute form (`{ .foo }`) and the `!= nil`
    /// spelling (`{ .a != nil }`) both parse to `Exists`; `{ .a = nil }`
    /// parses to `Not(Exists)`. Canonical `Display` is the bare form, so
    /// the round-trip oracle holds.
    Exists(Field),
    /// A comparison with an arithmetic operand on either side (issue #185,
    /// `arith.*`): `{ .a = 1 + 2 }`, `{ .a = -1 }`, `{ duration * 2 > 1s }`.
    /// The parser only routes here when an arithmetic operator (`+ - * / %
    /// ^`) or a unary minus is present, so the frozen literal/field
    /// comparison goldens do not churn. `Display` fully parenthesizes.
    ArithCompare {
        lhs: Operand,
        op: ComparisonOp,
        rhs: Operand,
    },
    /// Unary field negation (`!(.a = 1)`, `!.a` — issue #183,
    /// `logic.not`): the per-span boolean inverse of the inner expression.
    /// `Display` fully parenthesizes the inner so the round-trip oracle
    /// holds.
    Not(Box<FieldExpr>),
    Binary {
        op: BoolOp,
        lhs: Box<FieldExpr>,
        rhs: Box<FieldExpr>,
    },
}

impl fmt::Display for FieldExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldExpr::Comparison { field, op, value } => write!(f, "{field} {op} {value}"),
            FieldExpr::FieldCompare { lhs, op, rhs } => write!(f, "{lhs} {op} {rhs}"),
            FieldExpr::BoolStatic(b) => write!(f, "{b}"),
            FieldExpr::Exists(field) => write!(f, "{field}"),
            FieldExpr::ArithCompare { lhs, op, rhs } => {
                // The comparison operator binds looser than every arithmetic
                // operator, so the outermost operand needs no wrapping parens
                // (which would otherwise reparse as a grouped field
                // expression). Inner operands keep their parens via
                // `Operand`'s own `Display`.
                fmt_operand_bare(f, lhs)?;
                write!(f, " {op} ")?;
                fmt_operand_bare(f, rhs)
            }
            FieldExpr::Not(inner) => write!(f, "!({inner})"),
            FieldExpr::Binary { op, lhs, rhs } => write!(f, "({lhs} {op} {rhs})"),
        }
    }
}

/// `&&` / `||` — used at both the spanset and the field level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BoolOp {
    And,
    Or,
}

impl fmt::Display for BoolOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            BoolOp::And => "&&",
            BoolOp::Or => "||",
        };
        write!(f, "{s}")
    }
}

/// A field-expression arithmetic operator (issue #185, `arith.*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

impl fmt::Display for ArithOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
            ArithOp::Mod => "%",
            ArithOp::Pow => "^",
        };
        write!(f, "{s}")
    }
}

/// One operand of an [`FieldExpr::ArithCompare`] (issue #185): a field, a
/// numeric literal (number or duration), a unary negation, or a binary
/// arithmetic composition. `Display` fully parenthesizes binary nodes and
/// prefixes negation so the round-trip oracle holds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Operand {
    Field(Field),
    Literal(Value),
    Neg(Box<Operand>),
    Arith {
        op: ArithOp,
        lhs: Box<Operand>,
        rhs: Box<Operand>,
    },
}

impl fmt::Display for Operand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operand::Field(field) => write!(f, "{field}"),
            Operand::Literal(value) => write!(f, "{value}"),
            Operand::Neg(inner) => write!(f, "-{inner}"),
            Operand::Arith { op, lhs, rhs } => write!(f, "({lhs} {op} {rhs})"),
        }
    }
}

/// Renders an operand at the outermost position of an
/// [`FieldExpr::ArithCompare`], where a top-level arithmetic node needs no
/// wrapping parens (the comparison operator binds looser). Nested operands
/// still parenthesize via [`Operand`]'s `Display`.
fn fmt_operand_bare(f: &mut fmt::Formatter<'_>, operand: &Operand) -> fmt::Result {
    match operand {
        Operand::Arith { op, lhs, rhs } => write!(f, "{lhs} {op} {rhs}"),
        other => write!(f, "{other}"),
    }
}

/// The left-hand side of a comparison: an intrinsic or a (scoped or
/// unscoped) attribute. `service` is *not* an intrinsic — it is the
/// `resource.service.name` attribute, which the T5 planner maps to the
/// physical `service` column (docs/architecture.md §5.4).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Field {
    Intrinsic(Intrinsic),
    Attribute { scope: AttrScope, key: String },
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Field::Intrinsic(intrinsic) => write!(f, "{intrinsic}"),
            Field::Attribute { scope, key } => write!(f, "{scope}{}", render_attr_key(key)),
        }
    }
}

/// Renders an attribute key back to a TraceQL spelling that reparses to the
/// same key. A "simple" dotted-identifier key (`http.status_code`, `foo`)
/// re-emits verbatim; any key with a segment that is not a bare identifier
/// (spaces, punctuation, empty — the `scope.quoted`/`scope.bracketed`
/// forms, issue #185) re-emits in the canonical bracketed form
/// (`["…"]`), so `parse(ast.to_string()) == ast` holds for every key.
fn render_attr_key(key: &str) -> String {
    let simple = !key.is_empty()
        && key.split('.').all(|seg| {
            !seg.is_empty() && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        });
    if simple {
        key.to_string()
    } else {
        format!("[{}]", quote(key))
    }
}

/// The span intrinsics: the four M4 intrinsics (docs/features.md §4) plus
/// the nested-set structural intrinsics (issue #181) — numeric span
/// properties used in field comparisons (`{ nestedSetParent < 0 }`), not
/// new operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Intrinsic {
    Name,
    Duration,
    Status,
    Kind,
    /// `nestedSetParent` — the nested-set `left` value of a span's parent,
    /// or a negative sentinel for a root span (issue #181).
    NestedSetParent,
    /// `nestedSetLeft` — a span's modified-preorder `left` boundary.
    NestedSetLeft,
    /// `nestedSetRight` — a span's modified-preorder `right` boundary.
    NestedSetRight,
    // -- issue #184: the colon-scoped intrinsic namespace. The bare and
    // `span:`/`trace:` scoped spellings normalize onto one variant each
    // (`span:name` ≡ `name`); the canonical `Display` reparses to the same
    // variant (the round-trip oracle).
    /// `statusMessage` | `span:statusMessage` — the span status message
    /// (string).
    StatusMessage,
    /// `span:childCount` — the number of direct children of a span
    /// (integer; no bare spelling).
    ChildCount,
    /// `span:id` — the span id (hex string; no bare spelling).
    SpanId,
    /// `span:parentID` — the parent span id (hex string; no bare spelling).
    ParentId,
    /// `trace:id` — the trace id (hex string; no bare spelling).
    TraceId,
    /// `traceDuration` | `trace:duration` — the whole trace's duration.
    TraceDuration,
    /// `rootName` | `trace:rootName` — the trace root span's name (string).
    RootName,
    /// `rootServiceName` | `trace:rootService` — the trace root span's
    /// service name (string).
    RootServiceName,
    // -- issue #192: the instrumentation-scope intrinsic namespace. Only a
    // scoped spelling exists (`instrumentation:name`/`instrumentation:version`),
    // resolved through the `instrumentation` scope keyword.
    /// `instrumentation:name` — the OTLP instrumentation scope name (string).
    InstrumentationName,
    /// `instrumentation:version` — the OTLP instrumentation scope version
    /// (string).
    InstrumentationVersion,
    // -- issue #192 (PR-B): the span-event intrinsic namespace. Only a
    // scoped spelling exists (`event:name`/`event:timeSinceStart`), resolved
    // through the `event` scope keyword.
    /// `event:name` — a span event's name (string).
    EventName,
    /// `event:timeSinceStart` — a span event's timestamp relative to its
    /// span's start, as a duration (`event.timeUnixNano − span.startTimeUnixNano`).
    EventTimeSinceStart,
}

impl Intrinsic {
    /// Resolves a bare intrinsic keyword (`name`, `duration`, the legacy
    /// trace-level spellings `statusMessage`/`traceDuration`/`rootName`/
    /// `rootServiceName`, …). Colon-scoped spellings resolve via
    /// [`Intrinsic::from_scoped`]; the intrinsics with only a scoped form
    /// (`childCount`/`id`/`parentID`) are deliberately absent here.
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "name" => Some(Self::Name),
            "duration" => Some(Self::Duration),
            "status" => Some(Self::Status),
            "kind" => Some(Self::Kind),
            "nestedSetParent" => Some(Self::NestedSetParent),
            "nestedSetLeft" => Some(Self::NestedSetLeft),
            "nestedSetRight" => Some(Self::NestedSetRight),
            "statusMessage" => Some(Self::StatusMessage),
            "traceDuration" => Some(Self::TraceDuration),
            "rootName" => Some(Self::RootName),
            "rootServiceName" => Some(Self::RootServiceName),
            _ => None,
        }
    }

    /// Resolves a colon-scoped intrinsic (`span:childCount`, `trace:id`,
    /// …). An unknown scope keyword (`event:`/`link:`/`instrumentation:`)
    /// or an unknown field yields `None`, which the parser surfaces as a
    /// generic error — keeping those constructs' interim-generic
    /// disposition intact (issue #184).
    pub(crate) fn from_scoped(scope: &str, ident: &str) -> Option<Self> {
        match (scope, ident) {
            ("span", "name") => Some(Self::Name),
            ("span", "duration") => Some(Self::Duration),
            ("span", "status") => Some(Self::Status),
            ("span", "kind") => Some(Self::Kind),
            ("span", "statusMessage") => Some(Self::StatusMessage),
            ("span", "childCount") => Some(Self::ChildCount),
            ("span", "id") => Some(Self::SpanId),
            ("span", "parentID") => Some(Self::ParentId),
            ("trace", "id") => Some(Self::TraceId),
            ("trace", "duration") => Some(Self::TraceDuration),
            ("trace", "rootName") => Some(Self::RootName),
            ("trace", "rootService") => Some(Self::RootServiceName),
            ("instrumentation", "name") => Some(Self::InstrumentationName),
            ("instrumentation", "version") => Some(Self::InstrumentationVersion),
            ("event", "name") => Some(Self::EventName),
            ("event", "timeSinceStart") => Some(Self::EventTimeSinceStart),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Intrinsic::Name => "name",
            Intrinsic::Duration => "duration",
            Intrinsic::Status => "status",
            Intrinsic::Kind => "kind",
            Intrinsic::NestedSetParent => "nestedSetParent",
            Intrinsic::NestedSetLeft => "nestedSetLeft",
            Intrinsic::NestedSetRight => "nestedSetRight",
            // Canonical spelling: the bare form where one exists, else the
            // sole scoped form — each reparses to this same variant.
            Intrinsic::StatusMessage => "statusMessage",
            Intrinsic::ChildCount => "span:childCount",
            Intrinsic::SpanId => "span:id",
            Intrinsic::ParentId => "span:parentID",
            Intrinsic::TraceId => "trace:id",
            Intrinsic::TraceDuration => "traceDuration",
            Intrinsic::RootName => "rootName",
            Intrinsic::RootServiceName => "rootServiceName",
            Intrinsic::InstrumentationName => "instrumentation:name",
            Intrinsic::InstrumentationVersion => "instrumentation:version",
            Intrinsic::EventName => "event:name",
            Intrinsic::EventTimeSinceStart => "event:timeSinceStart",
        }
    }
}

impl fmt::Display for Intrinsic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Attribute scope: `span.`, `resource.`, or the leading-`.` unscoped
/// form (searches both scopes — docs/schemas.md §4.1). `parent.` is
/// recognized and rejected as `NotYetSupported` (M7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttrScope {
    Span,
    Resource,
    Unscoped,
    /// `instrumentation.` — the OTLP instrumentation-scope attribute
    /// namespace (issue #192), index-served under `scope='instrumentation'`.
    Instrumentation,
    /// `event.` — the span-event attribute namespace (issue #192 PR-B),
    /// index-served under `scope='event'`.
    Event,
}

impl fmt::Display for AttrScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            AttrScope::Span => "span.",
            AttrScope::Resource => "resource.",
            AttrScope::Unscoped => ".",
            AttrScope::Instrumentation => "instrumentation.",
            AttrScope::Event => "event.",
        };
        write!(f, "{s}")
    }
}

/// The full M4 comparison-operator set. `>`/`>=`/`<`/`<=` are comparisons
/// *inside* a field expression; the same characters between spansets are
/// structural operators (`>` child, `<` parent — issue #172/#183), while
/// `>=`/`<=` stay recognized-and-rejected between spansets
/// (`NotYetSupported`, M7). Disambiguated purely by parser position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComparisonOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Re,
    Nre,
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ComparisonOp::Eq => "=",
            ComparisonOp::Neq => "!=",
            ComparisonOp::Gt => ">",
            ComparisonOp::Gte => ">=",
            ComparisonOp::Lt => "<",
            ComparisonOp::Lte => "<=",
            ComparisonOp::Re => "=~",
            ComparisonOp::Nre => "!~",
        };
        write!(f, "{s}")
    }
}

/// A comparison right-hand side. Value parsing is field-typed (plan v2
/// F4): `status`/`kind` produce the closed enums by construction,
/// `duration` requires a duration literal, `name` a string, and
/// attributes accept string/number/boolean/duration. Numbers stay raw
/// `String` (no `f64` — preserves `Eq`/`Hash`; T5 parses to `val_num`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    String(String),
    Number(String),
    Duration(Duration),
    Bool(bool),
    Status(StatusValue),
    Kind(SpanKindValue),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", quote(s)),
            Value::Number(n) => write!(f, "{n}"),
            Value::Duration(d) => write!(f, "{d}"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Status(s) => write!(f, "{s}"),
            Value::Kind(k) => write!(f, "{k}"),
        }
    }
}

/// The closed `status` keyword set (task-manager adjudication 2) — an
/// invalid keyword is a positioned grammar error, so T5 receives valid
/// enums by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatusValue {
    Ok,
    Error,
    Unset,
}

impl StatusValue {
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "ok" => Some(Self::Ok),
            "error" => Some(Self::Error),
            "unset" => Some(Self::Unset),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            StatusValue::Ok => "ok",
            StatusValue::Error => "error",
            StatusValue::Unset => "unset",
        }
    }
}

impl fmt::Display for StatusValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// The closed `kind` keyword set (task-manager adjudication 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpanKindValue {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKindValue {
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "internal" => Some(Self::Internal),
            "server" => Some(Self::Server),
            "client" => Some(Self::Client),
            "producer" => Some(Self::Producer),
            "consumer" => Some(Self::Consumer),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            SpanKindValue::Internal => "internal",
            SpanKindValue::Server => "server",
            SpanKindValue::Client => "client",
            SpanKindValue::Producer => "producer",
            SpanKindValue::Consumer => "consumer",
        }
    }
}

impl fmt::Display for SpanKindValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A pipeline stage after `|`. M4 implements the search aggregate
/// filters, `select`, and (issue #59, T7) the metrics functions.
/// [`PipelineStage::Metric`] carries the first-stage metrics function with
/// its optional `by(...)` grouping and `with(...)` hints (issue #182);
/// [`PipelineStage::MetricSecondStage`] carries `topk(n)`/`bottomk(n)`
/// applied after a `|` to a first-stage metric's series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PipelineStage {
    /// `count() cmp value` (zero-arity — `field: None`) or
    /// `avg|sum|min|max(field) cmp value` (one-arity, numeric-aggregatable
    /// fields only: `duration` or an attribute).
    Aggregate {
        op: AggregateOp,
        field: Option<Field>,
        cmp: ComparisonOp,
        value: Value,
    },
    /// `select(field, ...)` — one or more fields; `select()` is a
    /// positioned parse error.
    Select { fields: Vec<Field> },
    /// `by(field, ...)` — a spanset-level grouping stage (issue #185,
    /// `pipeline.by`): regroups the matched spans into per-key spansets.
    /// Distinct from the metric `by(...)` clause carried by
    /// [`MetricStage`]. Empty `by()` is a positioned parse error.
    By { fields: Vec<Field> },
    /// `coalesce()` — a spanset-level stage (issue #185, `pipeline.coalesce`)
    /// that merges the spanset arrays. Zero-arity.
    Coalesce,
    /// A first-stage metrics function (`rate()`, `count_over_time()`, the
    /// `*_over_time` family) with its optional `by(...)` grouping and
    /// trailing `with(...)` hints. Served exclusively by the
    /// `/api/traces/v1/metrics/*` endpoints; the search planner rejects
    /// this stage (issue #59/#182).
    Metric(MetricStage),
    /// A second-stage metrics operator (`topk(n)` / `bottomk(n)`, issue
    /// #182): reduces the series set a first-stage metric produced. Only
    /// valid after a metrics stage; the metrics planner enforces that.
    MetricSecondStage(SecondStage),
    /// `compare({ selection })` (issue #182): a standalone metrics
    /// function that partitions the outer spanset into a `selection` (the
    /// inner filter) and a `baseline` (everything) and emits per-attribute
    /// meta-series. Its argument is a spanset filter, not a field; it
    /// accepts trailing `with(...)` hints (e.g. `with(exemplars=…)`).
    Compare {
        selection: Box<SpansetFilter>,
        hints: Vec<MetricHint>,
    },
}

impl fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineStage::Aggregate {
                op,
                field,
                cmp,
                value,
            } => match field {
                Some(field) => write!(f, "{op}({field}) {cmp} {value}"),
                None => write!(f, "{op}() {cmp} {value}"),
            },
            PipelineStage::Select { fields } => {
                write!(f, "select(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{field}")?;
                }
                write!(f, ")")
            }
            PipelineStage::By { fields } => {
                write!(f, "by(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{field}")?;
                }
                write!(f, ")")
            }
            PipelineStage::Coalesce => write!(f, "coalesce()"),
            PipelineStage::Metric(stage) => write!(f, "{stage}"),
            PipelineStage::MetricSecondStage(stage) => write!(f, "{stage}"),
            PipelineStage::Compare { selection, hints } => {
                write!(f, "compare({selection})")?;
                if !hints.is_empty() {
                    write!(f, " with(")?;
                    for (i, hint) in hints.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{hint}")?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
        }
    }
}

/// A first-stage metrics-function call with its optional `by(...)`
/// grouping and trailing `with(...)` hints (issue #182). Ungrouped,
/// hint-less calls carry empty `by`/`hints` vectors — the `rate()` /
/// `count_over_time()` M4 shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetricStage {
    pub func: MetricFn,
    /// `by (fields)` grouping keys; empty means ungrouped.
    pub by: Vec<Field>,
    /// `with (k=v, ...)` hints; empty means none.
    pub hints: Vec<MetricHint>,
    /// A trailing metrics-result comparison filter (`… > 5`, issue #182 —
    /// the `metrics.result_comparison` construct): keeps only the series
    /// samples satisfying `<op> <value>`. `None` when absent. Rendered
    /// attached to the metric (no `|`) so the round-trip oracle holds.
    pub result_filter: Option<(ComparisonOp, Value)>,
}

impl fmt::Display for MetricStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.func)?;
        if !self.by.is_empty() {
            write!(f, " by(")?;
            for (i, field) in self.by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{field}")?;
            }
            write!(f, ")")?;
        }
        if !self.hints.is_empty() {
            write!(f, " with(")?;
            for (i, hint) in self.hints.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{hint}")?;
            }
            write!(f, ")")?;
        }
        if let Some((op, value)) = &self.result_filter {
            write!(f, " {op} {value}")?;
        }
        Ok(())
    }
}

/// The TraceQL first-stage metrics functions (issue #59 shipped the
/// zero-arity `rate`/`count_over_time`; issue #182 completes the
/// `*_over_time` family to Tempo v3.0.2 parity). Each `*_over_time`
/// function carries a numeric aggregation target field; `quantile_over_time`
/// additionally carries one or more quantile literals (kept raw as
/// [`Value::Number`] so the AST stays `Eq`/`Hash`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetricFn {
    Rate,
    CountOverTime,
    SumOverTime(Field),
    MinOverTime(Field),
    MaxOverTime(Field),
    AvgOverTime(Field),
    QuantileOverTime { field: Field, quantiles: Vec<Value> },
    HistogramOverTime(Field),
}

impl MetricFn {
    /// The bare function name (no arguments) — the `Display` head and the
    /// disposition/registry probe key.
    pub fn name(&self) -> &'static str {
        match self {
            MetricFn::Rate => "rate",
            MetricFn::CountOverTime => "count_over_time",
            MetricFn::SumOverTime(_) => "sum_over_time",
            MetricFn::MinOverTime(_) => "min_over_time",
            MetricFn::MaxOverTime(_) => "max_over_time",
            MetricFn::AvgOverTime(_) => "avg_over_time",
            MetricFn::QuantileOverTime { .. } => "quantile_over_time",
            MetricFn::HistogramOverTime(_) => "histogram_over_time",
        }
    }
}

impl fmt::Display for MetricFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name())?;
        match self {
            MetricFn::Rate | MetricFn::CountOverTime => {}
            MetricFn::SumOverTime(field)
            | MetricFn::MinOverTime(field)
            | MetricFn::MaxOverTime(field)
            | MetricFn::AvgOverTime(field)
            | MetricFn::HistogramOverTime(field) => write!(f, "{field}")?,
            MetricFn::QuantileOverTime { field, quantiles } => {
                write!(f, "{field}")?;
                for q in quantiles {
                    write!(f, ", {q}")?;
                }
            }
        }
        write!(f, ")")
    }
}

/// A `with(...)` hint on a metrics stage (issue #182) — one `key=value`
/// pair. Values keep their raw lexical form (numbers stay `String`, like
/// [`Value`]) so the whole AST stays `Eq`/`Hash` for the plan cache and
/// the `Display` round-trip oracle.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetricHint {
    pub key: String,
    pub value: HintValue,
}

impl fmt::Display for MetricHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

/// A `with(...)` hint value (issue #182). Numbers stay raw `String` to
/// preserve `Eq`/`Hash` on the AST (the [`Value`] convention); the
/// planner parses them where it needs an `f64`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HintValue {
    Bool(bool),
    Number(String),
    String(String),
    Duration(Duration),
}

impl fmt::Display for HintValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HintValue::Bool(b) => write!(f, "{b}"),
            HintValue::Number(n) => write!(f, "{n}"),
            HintValue::String(s) => write!(f, "{}", quote(s)),
            HintValue::Duration(d) => write!(f, "{d}"),
        }
    }
}

/// A second-stage metrics operator (issue #182): applied after a `|` to
/// the series a first-stage metric produced. `compare()` and the
/// metrics-result comparison filter route through the review-gated design
/// spike (P6) and are not yet parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecondStage {
    /// `topk(n)` — the `n` series with the largest value per step.
    TopK(u64),
    /// `bottomk(n)` — the `n` series with the smallest value per step.
    BottomK(u64),
}

impl fmt::Display for SecondStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecondStage::TopK(n) => write!(f, "topk({n})"),
            SecondStage::BottomK(n) => write!(f, "bottomk({n})"),
        }
    }
}

/// The M4 search aggregate-filter operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateOp {
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "count" => Some(Self::Count),
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            AggregateOp::Count => "count",
            AggregateOp::Sum => "sum",
            AggregateOp::Avg => "avg",
            AggregateOp::Min => "min",
            AggregateOp::Max => "max",
        }
    }
}

impl fmt::Display for AggregateOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A duration in nanoseconds — a `u64`-nanos newtype, not
/// `std::time::Duration`, so T5's arithmetic stays exact `u64` math.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Duration(u64);

impl Duration {
    pub(crate) fn from_nanos(nanos: u64) -> Self {
        Duration(nanos)
    }

    pub fn as_nanos(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            return write!(f, "0s");
        }
        // The canonical rendering is a *single-group* literal (the
        // grammar rejects compound literals — docs/api.md §4.2): the
        // largest unit that divides the value exactly. `ns` always does,
        // so this never fails, emits no fraction, and reparses to the
        // same nanosecond value — the `Display` round-trip oracle.
        const COMPONENTS: [(u64, &str); 6] = [
            (3_600_000_000_000, "h"),
            (60_000_000_000, "m"),
            (1_000_000_000, "s"),
            (1_000_000, "ms"),
            (1_000, "us"),
            (1, "ns"),
        ];
        for (unit_ns, suffix) in COMPONENTS {
            if self.0.is_multiple_of(unit_ns) {
                return write!(f, "{}{suffix}", self.0 / unit_ns);
            }
        }
        unreachable!("the 1ns component divides every u64 value")
    }
}

/// Escapes a raw string value into a double-quoted TraceQL string literal
/// for canonical `Display` rendering — only `\` and `"` (plus the
/// whitespace escapes the lexer decodes) need escaping for the round-trip
/// oracle to hold.
fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// The frozen scope-boundary registry: every recognized-but-unsupported
/// construct, paired with the milestone/task that owns it. Each entry's
/// first element is the exact `construct` string carried by the
/// [`crate::TraceQlError::NotYetSupported`] the parser must produce; the
/// golden corpus's `unsupported/` cases map one-to-one onto this table
/// (both directions asserted mechanically in `tests/corpus.rs`), so
/// scope drift in either direction fails CI.
pub const BOUNDARY_CONSTRUCTS: &[(&str, &str)] = &[
    ("structural operator '>='", "M7"),
    ("structural operator '<='", "M7"),
    // `parent.` is a PERMANENT reject-parity construct (issue #185, Cat B):
    // the pinned reference rejects it too, so agreement — not an interim
    // gap owned by any sub-issue. The second element is the disposition
    // class, not an owning issue.
    ("parent scope", "reject-parity"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_display_picks_the_largest_exact_single_unit() {
        assert_eq!(Duration::from_nanos(2_000_000_000).to_string(), "2s");
        assert_eq!(Duration::from_nanos(500_000_000).to_string(), "500ms");
        assert_eq!(Duration::from_nanos(5_400_000_000_000).to_string(), "90m");
        assert_eq!(Duration::from_nanos(3_600_000_000_000).to_string(), "1h");
        assert_eq!(Duration::from_nanos(500_000).to_string(), "500us");
        assert_eq!(Duration::from_nanos(7).to_string(), "7ns");
    }

    #[test]
    fn duration_display_of_zero_is_zero_seconds() {
        assert_eq!(Duration::from_nanos(0).to_string(), "0s");
    }

    #[test]
    fn value_display_quotes_and_escapes_strings() {
        let v = Value::String("a\"b\\c".to_string());
        assert_eq!(v.to_string(), r#""a\"b\\c""#);
    }

    #[test]
    fn intrinsics_round_trip_through_from_ident_and_display() {
        // Bare-spelled intrinsics: `from_ident` resolves them and their
        // canonical `Display` is that same bare word.
        for (name, intrinsic) in [
            ("name", Intrinsic::Name),
            ("duration", Intrinsic::Duration),
            ("status", Intrinsic::Status),
            ("kind", Intrinsic::Kind),
            ("nestedSetParent", Intrinsic::NestedSetParent),
            ("nestedSetLeft", Intrinsic::NestedSetLeft),
            ("nestedSetRight", Intrinsic::NestedSetRight),
            ("statusMessage", Intrinsic::StatusMessage),
            ("traceDuration", Intrinsic::TraceDuration),
            ("rootName", Intrinsic::RootName),
            ("rootServiceName", Intrinsic::RootServiceName),
        ] {
            assert_eq!(Intrinsic::from_ident(name), Some(intrinsic));
            assert_eq!(intrinsic.to_string(), name);
        }
        assert_eq!(Intrinsic::from_ident("service"), None);
        assert_eq!(Intrinsic::from_ident("nestedSet"), None);
        // The scope-only intrinsics have NO bare spelling.
        assert_eq!(Intrinsic::from_ident("childCount"), None);
        assert_eq!(Intrinsic::from_ident("id"), None);
        assert_eq!(Intrinsic::from_ident("parentID"), None);
    }

    #[test]
    fn colon_scoped_intrinsics_resolve_and_display_canonically() {
        for (scope, ident, intrinsic, canonical) in [
            ("span", "name", Intrinsic::Name, "name"),
            ("span", "duration", Intrinsic::Duration, "duration"),
            ("span", "status", Intrinsic::Status, "status"),
            ("span", "kind", Intrinsic::Kind, "kind"),
            (
                "span",
                "statusMessage",
                Intrinsic::StatusMessage,
                "statusMessage",
            ),
            (
                "span",
                "childCount",
                Intrinsic::ChildCount,
                "span:childCount",
            ),
            ("span", "id", Intrinsic::SpanId, "span:id"),
            ("span", "parentID", Intrinsic::ParentId, "span:parentID"),
            ("trace", "id", Intrinsic::TraceId, "trace:id"),
            (
                "trace",
                "duration",
                Intrinsic::TraceDuration,
                "traceDuration",
            ),
            ("trace", "rootName", Intrinsic::RootName, "rootName"),
            (
                "trace",
                "rootService",
                Intrinsic::RootServiceName,
                "rootServiceName",
            ),
            (
                "instrumentation",
                "name",
                Intrinsic::InstrumentationName,
                "instrumentation:name",
            ),
            (
                "instrumentation",
                "version",
                Intrinsic::InstrumentationVersion,
                "instrumentation:version",
            ),
            ("event", "name", Intrinsic::EventName, "event:name"),
            (
                "event",
                "timeSinceStart",
                Intrinsic::EventTimeSinceStart,
                "event:timeSinceStart",
            ),
        ] {
            assert_eq!(Intrinsic::from_scoped(scope, ident), Some(intrinsic));
            assert_eq!(intrinsic.to_string(), canonical);
        }
        // Unknown colon scopes stay unresolved (generic error at parse).
        // event: now resolves (issue #192 PR-B) — see the loop above; link:
        // stays interim (PR-C).
        assert_eq!(Intrinsic::from_scoped("link", "spanID"), None);
        // instrumentation:/event: resolve (issue #192) — see the loop above.
        assert_eq!(Intrinsic::from_scoped("instrumentation", "bogus"), None);
        assert_eq!(Intrinsic::from_scoped("event", "bogus"), None);
        assert_eq!(Intrinsic::from_scoped("span", "bogus"), None);
        assert_eq!(
            Intrinsic::from_scoped("trace", "rootName"),
            Some(Intrinsic::RootName)
        );
    }

    #[test]
    fn status_values_round_trip_and_the_set_is_closed() {
        for (name, status) in [
            ("ok", StatusValue::Ok),
            ("error", StatusValue::Error),
            ("unset", StatusValue::Unset),
        ] {
            assert_eq!(StatusValue::from_ident(name), Some(status));
            assert_eq!(status.to_string(), name);
        }
        assert_eq!(StatusValue::from_ident("bogus"), None);
    }

    #[test]
    fn kind_values_round_trip_and_the_set_is_closed() {
        for (name, kind) in [
            ("internal", SpanKindValue::Internal),
            ("server", SpanKindValue::Server),
            ("client", SpanKindValue::Client),
            ("producer", SpanKindValue::Producer),
            ("consumer", SpanKindValue::Consumer),
        ] {
            assert_eq!(SpanKindValue::from_ident(name), Some(kind));
            assert_eq!(kind.to_string(), name);
        }
        assert_eq!(SpanKindValue::from_ident("frobnicate"), None);
    }

    #[test]
    fn aggregate_ops_round_trip_through_from_ident_and_display() {
        for (name, op) in [
            ("count", AggregateOp::Count),
            ("sum", AggregateOp::Sum),
            ("avg", AggregateOp::Avg),
            ("min", AggregateOp::Min),
            ("max", AggregateOp::Max),
        ] {
            assert_eq!(AggregateOp::from_ident(name), Some(op));
            assert_eq!(op.to_string(), name);
        }
        assert_eq!(AggregateOp::from_ident("rate"), None);
    }

    #[test]
    fn metric_fn_names_are_not_search_aggregates() {
        for name in [
            "rate",
            "count_over_time",
            "sum_over_time",
            "min_over_time",
            "max_over_time",
            "avg_over_time",
            "quantile_over_time",
            "histogram_over_time",
        ] {
            assert_eq!(AggregateOp::from_ident(name), None);
        }
    }

    #[test]
    fn metric_fn_display_renders_the_call_with_its_arguments() {
        assert_eq!(MetricFn::Rate.to_string(), "rate()");
        assert_eq!(MetricFn::CountOverTime.to_string(), "count_over_time()");
        assert_eq!(
            MetricFn::SumOverTime(Field::Intrinsic(Intrinsic::Duration)).to_string(),
            "sum_over_time(duration)"
        );
        assert_eq!(
            MetricFn::QuantileOverTime {
                field: Field::Intrinsic(Intrinsic::Duration),
                quantiles: vec![
                    Value::Number("0.5".to_string()),
                    Value::Number("0.9".to_string())
                ],
            }
            .to_string(),
            "quantile_over_time(duration, 0.5, 0.9)"
        );
    }

    #[test]
    fn metric_stage_display_renders_by_and_with_clauses() {
        let stage = MetricStage {
            func: MetricFn::Rate,
            by: vec![Field::Attribute {
                scope: AttrScope::Resource,
                key: "service.name".to_string(),
            }],
            hints: vec![MetricHint {
                key: "exemplars".to_string(),
                value: HintValue::Number("100".to_string()),
            }],
            result_filter: None,
        };
        assert_eq!(
            PipelineStage::Metric(stage).to_string(),
            "rate() by(resource.service.name) with(exemplars=100)"
        );
        assert_eq!(
            PipelineStage::MetricSecondStage(SecondStage::TopK(10)).to_string(),
            "topk(10)"
        );
        // Compare stage + result-comparison filter round-trip.
        let compare = Query {
            spanset: SpansetExpr::Filter(SpansetFilter { body: None }),
            pipeline: vec![PipelineStage::Compare {
                selection: Box::new(SpansetFilter {
                    body: Some(FieldExpr::Comparison {
                        field: Field::Attribute {
                            scope: AttrScope::Span,
                            key: "http.status_code".to_string(),
                        },
                        op: ComparisonOp::Eq,
                        value: Value::Number("500".to_string()),
                    }),
                }),
                hints: vec![],
            }],
            hints: vec![],
        };
        assert_eq!(
            compare.to_string(),
            "{} | compare({ span.http.status_code = 500 })"
        );
        let rc = MetricStage {
            func: MetricFn::Rate,
            by: vec![],
            hints: vec![],
            result_filter: Some((ComparisonOp::Gt, Value::Number("5".to_string()))),
        };
        assert_eq!(PipelineStage::Metric(rc).to_string(), "rate() > 5");
    }

    #[test]
    fn boundary_registry_entries_are_unique() {
        for (i, (construct, _)) in BOUNDARY_CONSTRUCTS.iter().enumerate() {
            for (other, _) in &BOUNDARY_CONSTRUCTS[i + 1..] {
                assert_ne!(construct, other);
            }
        }
    }

    #[test]
    fn spanset_and_field_display_fully_parenthesize_binaries() {
        let cmp = |key: &str| FieldExpr::Comparison {
            field: Field::Attribute {
                scope: AttrScope::Unscoped,
                key: key.to_string(),
            },
            op: ComparisonOp::Eq,
            value: Value::Number("1".to_string()),
        };
        let body = FieldExpr::Binary {
            op: BoolOp::And,
            lhs: Box::new(cmp("a")),
            rhs: Box::new(FieldExpr::Binary {
                op: BoolOp::Or,
                lhs: Box::new(cmp("b")),
                rhs: Box::new(cmp("c")),
            }),
        };
        let query = Query {
            spanset: SpansetExpr::Filter(SpansetFilter { body: Some(body) }),
            pipeline: vec![],
            hints: vec![],
        };
        assert_eq!(query.to_string(), "{ (.a = 1 && (.b = 1 || .c = 1)) }");
    }
}
