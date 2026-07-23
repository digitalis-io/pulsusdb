//! The TraceQL AST — the stable contract the T5 planner/SQL generator
//! consumes (docs/architecture.md §5.4). Every type derives `Debug`,
//! `Clone`, `PartialEq`, `Eq`, `Hash` (the last so the AST can key a
//! plan cache; also load-bearing for the `Display` round-trip oracle:
//! `parse(ast.to_string()) == ast`).
//!
//! [`PipelineStage`] is the designated additive growth point: T7's
//! metrics pipeline functions landed as the additive
//! [`PipelineStage::Metric`] variant (`rate()`, `count_over_time()` —
//! the committed M4 set), never a reshape of the existing types or
//! fields. The deferred `*_over_time` functions and metrics grouping
//! `by` are recognized and reported as `NotYetSupported` (M7 — see
//! [`UNSUPPORTED_METRIC_FNS`] / [`BOUNDARY_CONSTRUCTS`]).

use std::fmt;

/// A parsed TraceQL search query: a spanset expression plus its pipeline
/// of aggregate filters / `select` stages (docs/features.md §4, M4
/// coverage line).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Query {
    pub spanset: SpansetExpr,
    pub pipeline: Vec<PipelineStage>,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.spanset)?;
        for stage in &self.pipeline {
            write!(f, " | {stage}")?;
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
    /// `{A} > {B}` / `{A} >> {B}` / `{A} ~ {B}` — parent/descendant/
    /// sibling relations evaluated over one trace's span graph. Binds
    /// tighter than `&&`/`||`, left-associative; the result set is the
    /// RIGHT-hand side's matching spans only (docs/api.md §4.2).
    Structural {
        op: StructuralOp,
        lhs: Box<SpansetExpr>,
        rhs: Box<SpansetExpr>,
    },
}

impl fmt::Display for SpansetExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpansetExpr::Filter(filter) => write!(f, "{filter}"),
            SpansetExpr::Binary { op, lhs, rhs } => write!(f, "({lhs} {op} {rhs})"),
            SpansetExpr::Structural { op, lhs, rhs } => write!(f, "({lhs} {op} {rhs})"),
        }
    }
}

/// The implemented structural relations (issue #172). `<` (parent), `<<`
/// (ancestor), and the negated/union forms stay in
/// [`BOUNDARY_CONSTRUCTS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StructuralOp {
    /// `>` — spans matching the RHS whose direct parent matches the LHS.
    Child,
    /// `>>` — spans matching the RHS with any transitive ancestor
    /// matching the LHS.
    Descendant,
    /// `~` — spans matching the RHS sharing a parent with a *distinct*
    /// span matching the LHS (all-zero `parent_id` roots never match).
    Sibling,
}

impl fmt::Display for StructuralOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            StructuralOp::Child => ">",
            StructuralOp::Descendant => ">>",
            StructuralOp::Sibling => "~",
        };
        write!(f, "{s}")
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
            Field::Attribute { scope, key } => write!(f, "{scope}{key}"),
        }
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
}

impl Intrinsic {
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "name" => Some(Self::Name),
            "duration" => Some(Self::Duration),
            "status" => Some(Self::Status),
            "kind" => Some(Self::Kind),
            "nestedSetParent" => Some(Self::NestedSetParent),
            "nestedSetLeft" => Some(Self::NestedSetLeft),
            "nestedSetRight" => Some(Self::NestedSetRight),
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
}

impl fmt::Display for AttrScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            AttrScope::Span => "span.",
            AttrScope::Resource => "resource.",
            AttrScope::Unscoped => ".",
        };
        write!(f, "{s}")
    }
}

/// The full M4 comparison-operator set. `>`/`>=`/`<`/`<=` are comparisons
/// only *inside* a field expression — the same characters between
/// spansets are structural operators: `>` is the implemented child
/// relation (issue #172), while `>=`/`<`/`<=` stay recognized-and-rejected
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
/// filters, `select`, and (issue #59, T7) the zero-arity metrics
/// functions — [`PipelineStage::Metric`] is the additive fill of the
/// designated growth point, never a reshape.
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
    /// `rate()` / `count_over_time()` (zero-arity — a stray argument is a
    /// positioned parse error). Served exclusively by the
    /// `/api/traces/v1/metrics/*` endpoints; the search planner rejects
    /// this stage (issue #59).
    Metric(MetricFn),
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
            PipelineStage::Metric(func) => write!(f, "{func}()"),
        }
    }
}

/// The committed M4 TraceQL metrics functions (issue #59, task-manager
/// adjudication 1): `rate()` and `count_over_time()` only. The five
/// deferred `*_over_time` functions stay in [`UNSUPPORTED_METRIC_FNS`]
/// (M7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricFn {
    Rate,
    CountOverTime,
}

impl MetricFn {
    pub(crate) fn from_ident(name: &str) -> Option<Self> {
        match name {
            "rate" => Some(Self::Rate),
            "count_over_time" => Some(Self::CountOverTime),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            MetricFn::Rate => "rate",
            MetricFn::CountOverTime => "count_over_time",
        }
    }
}

impl fmt::Display for MetricFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
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

/// The deferred metrics pipeline functions (issue #59, task-manager
/// adjudication 1: re-owned to **M7**): recognized at pipeline position,
/// rejected as `NotYetSupported` with a position. `rate` and
/// `count_over_time` left this registry when T7 implemented them via
/// [`PipelineStage::Metric`].
pub(crate) const UNSUPPORTED_METRIC_FNS: &[&str] = &[
    "avg_over_time",
    "min_over_time",
    "max_over_time",
    "quantile_over_time",
    "histogram_over_time",
];

/// The frozen scope-boundary registry: every recognized-but-unsupported
/// construct, paired with the milestone/task that owns it. Each entry's
/// first element is the exact `construct` string carried by the
/// [`crate::TraceQlError::NotYetSupported`] the parser must produce; the
/// golden corpus's `unsupported/` cases map one-to-one onto this table
/// (both directions asserted mechanically in `tests/corpus.rs`), so
/// scope drift in either direction fails CI.
pub const BOUNDARY_CONSTRUCTS: &[(&str, &str)] = &[
    ("structural operator '<'", "M7"),
    ("structural operator '<<'", "M7"),
    ("structural operator '>='", "M7"),
    ("structural operator '<='", "M7"),
    ("negation operator '!'", "M7"),
    ("arithmetic operator '+'", "M7"),
    ("arithmetic operator '-'", "M7"),
    ("arithmetic operator '*'", "M7"),
    ("arithmetic operator '/'", "M7"),
    ("parent scope", "M7"),
    ("bracketed attribute", "M7"),
    ("bare attribute expression", "M7"),
    ("metrics function 'avg_over_time'", "M7"),
    ("metrics function 'min_over_time'", "M7"),
    ("metrics function 'max_over_time'", "M7"),
    ("metrics function 'quantile_over_time'", "M7"),
    ("metrics function 'histogram_over_time'", "M7"),
    ("metrics grouping 'by'", "M7"),
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
        for (name, intrinsic) in [
            ("name", Intrinsic::Name),
            ("duration", Intrinsic::Duration),
            ("status", Intrinsic::Status),
            ("kind", Intrinsic::Kind),
            ("nestedSetParent", Intrinsic::NestedSetParent),
            ("nestedSetLeft", Intrinsic::NestedSetLeft),
            ("nestedSetRight", Intrinsic::NestedSetRight),
        ] {
            assert_eq!(Intrinsic::from_ident(name), Some(intrinsic));
            assert_eq!(intrinsic.to_string(), name);
        }
        assert_eq!(Intrinsic::from_ident("service"), None);
        assert_eq!(Intrinsic::from_ident("nestedSet"), None);
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
    fn metric_fns_are_not_recognized_as_implemented_aggregates() {
        for name in UNSUPPORTED_METRIC_FNS {
            assert_eq!(AggregateOp::from_ident(name), None);
            assert_eq!(MetricFn::from_ident(name), None);
        }
    }

    #[test]
    fn metric_fns_round_trip_through_from_ident_and_display() {
        for (name, func) in [
            ("rate", MetricFn::Rate),
            ("count_over_time", MetricFn::CountOverTime),
        ] {
            assert_eq!(MetricFn::from_ident(name), Some(func));
            assert_eq!(func.to_string(), name);
            assert_eq!(AggregateOp::from_ident(name), None);
        }
        assert_eq!(MetricFn::from_ident("quantile_over_time"), None);
    }

    #[test]
    fn metric_stage_display_renders_the_zero_arity_call() {
        assert_eq!(PipelineStage::Metric(MetricFn::Rate).to_string(), "rate()");
        assert_eq!(
            PipelineStage::Metric(MetricFn::CountOverTime).to_string(),
            "count_over_time()"
        );
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
        };
        assert_eq!(query.to_string(), "{ (.a = 1 && (.b = 1 || .c = 1)) }");
    }
}
