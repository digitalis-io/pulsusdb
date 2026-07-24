//! The shared span-filter compiler (issue #57 — the load-bearing
//! extraction T7's metrics endpoints consume): classifies every TraceQL
//! leaf comparison into its Phase-1 **candidate generator** class (the
//! bounded, index-served ranked top-K query `search_sql::generator_sql`
//! renders) and its Phase-2 **exact-evaluation** shape (a physical-column
//! predicate over hydrated spans, or an attribute-index membership probe).
//!
//! Field → column lowering (docs/schemas.md §4.1/§4.2, architecture.md
//! §5.4, verified against the writer's `protocols/otlp_traces.rs`):
//!
//! - `name` → `name` (String); `duration` → `duration_ns`; `status` →
//!   `status_code` Int8 = OTEL StatusCode {unset=0, ok=1, error=2};
//!   `kind` → `kind` Int8 = OTEL SpanKind {internal=1, server=2,
//!   client=3, producer=4, consumer=5}.
//! - `resource.service.name` is the sole attribute promoted to the
//!   physical `service` column: `=` generates via the `service_time`
//!   projection PREWHERE; positive `=~` generates via its **indexed**
//!   attr-index row (`key='service.name' AND scope='resource'` — the
//!   writer indexes it like any other resource attribute, plan v4 delta
//!   3); `!=`/`!~` fall back to the time-range generator. Evaluation is
//!   always on the hydrated physical column. Unscoped/`span.`-scoped
//!   `service.name` resolves via the attribute index like any other
//!   attribute (task-manager adjudication 5).
//! - Every other attribute → `trace_attrs_idx`: string/bool equality on
//!   the `(key, val[, scope])` prefix; numeric and regex comparisons as
//!   key-only `(key)` prefix scans (`val_num <op> N` / anchored
//!   `match(val, '^(?:…)$')` — full-value anchoring, task-manager
//!   adjudication 3); `!=`/`!~` have no positive generator (absence is
//!   not indexable) and pair with the time-range fallback, with Phase 2
//!   evaluating the ratified negation rule (a span matches iff **no**
//!   index row for it satisfies the positive predicate — absent-key spans
//!   match).
//!
//! Injection boundary: every user-controlled key/value/regex flows
//! through [`crate::logql::escape`] before it reaches a SQL fragment.

use pulsus_traceql::{
    ArithOp, AttrScope, ComparisonOp, Field, FieldExpr, Intrinsic, Operand, SpanKindValue,
    SpansetFilter, StatusValue, Value,
};

use crate::logql::escape;

use super::search_sql::byte_cap_expr;

/// Table-name context for one compilation — `trace_spans{_dist}` /
/// `trace_attrs_idx{_dist}` exactly as `chconfig` derives them.
#[derive(Debug, Clone, Copy)]
pub struct SpanFilterCtx<'a> {
    pub spans_table: &'a str,
    pub attrs_table: &'a str,
}

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

/// The static leaf-class selectivity priority (issue #57 plan v3: "a
/// fixed static leaf-class priority, never a runtime probe") — lower is
/// more selective. Drives the deterministic per-disjunct generator choice
/// in [`crate::traces::search_plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GenClass {
    /// Attr string/bool equality — `(key, val[, scope])` prefix.
    AttrEq = 0,
    /// `resource.service.name =` — `service_time` projection PREWHERE.
    ServiceEq = 1,
    /// Attr numeric / regex — key-only `(key)` prefix scan.
    AttrKeyScan = 2,
    /// `duration <op>` — `idx_duration` minmax within the projection.
    Duration = 3,
    /// `name`/`status`/`kind` predicates — bounded time-window span scan.
    SpanScan = 4,
    /// No positive leaf (negations / `{}` match-all) — the complete
    /// time-range superset, bounded by the scan budget.
    TimeRange = 5,
}

/// Which table a generator reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenTable {
    Spans,
    Attrs,
}

/// One leaf's Phase-1 candidate generator: class + pre-escaped predicate
/// fragments (no time bounds — [`crate::traces::search_sql::generator_sql`]
/// adds the window/date pruning and the ranked `LIMIT`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafGenerator {
    pub class: GenClass,
    pub table: GenTable,
    /// Pre-escaped `WHERE` fragment (empty for [`GenClass::TimeRange`]).
    pub predicate: String,
    /// PREWHERE-eligible fragment ([`GenClass::ServiceEq`] only).
    pub prewhere: Option<String>,
}

impl LeafGenerator {
    pub(crate) fn time_range() -> Self {
        LeafGenerator {
            class: GenClass::TimeRange,
            table: GenTable::Spans,
            predicate: String::new(),
            prewhere: None,
        }
    }
}

/// The positive value predicate of one attribute membership probe —
/// rendered against `trace_attrs_idx` by
/// [`crate::traces::search_sql::membership_sql`].
#[derive(Debug, Clone, PartialEq)]
pub enum ValuePred {
    /// `val = '<v>'` — string/bool equality (prefix-served).
    StringEq(String),
    /// `match(val, '^(?:<pat>)$')` — anchored full-value regex.
    Regex(String),
    /// `val_num <op> <n>` — numeric comparison (key-only scan).
    Num { op: ComparisonOp, value: f64 },
    /// Key existence only — any value for the key satisfies it (issue #185
    /// `existence.*`). Renders as the no-op `1` predicate so a matching
    /// span is any span carrying the key (key-only `(key)` prefix scan).
    KeyExists,
    /// A pre-rendered boolean arithmetic predicate over `val_num` (issue
    /// #185 `arith.*`): single-attribute arithmetic with literal
    /// coefficients (e.g. `.duration_ms * 1000 > 5000` renders as
    /// `(val_num * 1000) > 5000`) pushed column-side onto the numeric attr
    /// column, like the metric path — not post-hydration. Built only from
    /// `val_num`, numeric literals, and total operators (`+ - *`), so it
    /// carries no user text and cannot diverge from the Rust evaluator.
    NumExpr(String),
}

/// One distinct attribute-index membership read: the positive `(key
/// [, scope], value-predicate)` probe Phase 2 evaluates spans against.
/// Negated leaves (`!=`/`!~`) share the probe of their positive form —
/// the evaluator inverts membership (the ratified negation rule).
#[derive(Debug, Clone, PartialEq)]
pub struct AttrProbe {
    pub key: String,
    /// `Some("span")` / `Some("resource")` for scoped selectors; `None`
    /// for the unscoped `.attr` form (prunes on the bare `(key, val)`
    /// prefix — docs/schemas.md §4.1).
    pub scope: Option<&'static str>,
    pub pred: ValuePred,
}

/// A physical-column comparison, evaluated on hydrated span rows in
/// Phase 2 (`traces::search_eval`).
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPredicate {
    /// `name` — Eq/Neq/Re/Nre.
    Name { op: ComparisonOp, value: String },
    /// `resource.service.name` — Eq/Neq/Re/Nre on the promoted column.
    Service { op: ComparisonOp, value: String },
    /// `duration` — the six ordering/equality operators, in nanoseconds.
    DurationNs { op: ComparisonOp, nanos: i64 },
    /// `status` — Eq/Neq against the OTEL wire code.
    Status { op: ComparisonOp, code: i8 },
    /// `kind` — Eq/Neq against the OTEL wire code.
    Kind { op: ComparisonOp, code: i8 },
    /// `statusMessage` / `span:statusMessage` (issue #184) — Eq/Neq/Re/Nre
    /// on the `status_message` String column. Phase-1 SQL compares the
    /// byte-capped rendering (the shared `search_sql` cap helper), matching
    /// the capped value Phase 2 hydrates and evaluates.
    StatusMessage { op: ComparisonOp, value: String },
    /// `span:id` (issue #184) — Eq/Neq/Re/Nre against the lowercase hex
    /// rendering of the 8-byte `span_id`. `value` is stored lowercased for
    /// Eq/Neq (hex is case-insensitive); Re/Nre keep the raw pattern.
    SpanIdHex { op: ComparisonOp, value: String },
    /// `span:parentID` (issue #184) — as [`PhysicalPredicate::SpanIdHex`]
    /// but over the `parent_id` column.
    ParentIdHex { op: ComparisonOp, value: String },
    /// `instrumentation:name` (issue #192) — Eq/Neq/Re/Nre on the
    /// `scope_name` `LowCardinality(String)` column. Phase-1 SQL compares the
    /// byte-capped rendering (the shared `search_sql` cap helper), matching
    /// the capped value Phase 2 hydrates and evaluates — the `statusMessage`
    /// precedent.
    InstrumentationName { op: ComparisonOp, value: String },
    /// `instrumentation:version` (issue #192) — as
    /// [`PhysicalPredicate::InstrumentationName`] but over the
    /// `scope_version` column.
    InstrumentationVersion { op: ComparisonOp, value: String },
}

/// Which nested-set structural intrinsic a leaf compares (issue #181).
/// The value is computed query-time from the hydrated `parent_id` forest
/// (`traces::search_eval`), so there is no physical column and no
/// Phase-1 pushdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NestedSetField {
    /// `nestedSetParent` — the parent span's `left`, or `-1` for a root.
    Parent,
    /// `nestedSetLeft` — the span's modified-preorder `left` boundary.
    Left,
    /// `nestedSetRight` — the span's modified-preorder `right` boundary.
    Right,
}

/// One operand of a field-vs-field comparison (issue #183
/// `comparison.rhs_attribute`): a physical intrinsic (read from the
/// hydrated span columns) or an attribute (read from `trace_attrs_idx`
/// via `val`/`val_num`). `resource.service.name` lowers to the physical
/// `service` column, like everywhere else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompareOperand {
    Name,
    Service,
    Duration,
    Status,
    Kind,
    Attr {
        key: String,
        scope: Option<&'static str>,
    },
}

/// A trace-level intrinsic comparison (issue #184), evaluated engine-side
/// against the per-trace context co-load (`traces::search_eval`'s
/// `TraceEvalCtx`) — window-independent, full-trace-exact. No hydrated span
/// column carries these values, so each leaf pairs with whatever Phase-1
/// generator its compile helper selects ([`compile_root_leaf`] /
/// [`compile_trace_num_leaf`] / [`compile_trace_id_leaf`]).
#[derive(Debug, Clone, PartialEq)]
pub enum TraceCtxPred {
    /// `span:childCount` — the number of direct children of the span
    /// (from the child-count co-load, keyed by the parent's span id).
    ChildCount { op: ComparisonOp, value: f64 },
    /// `traceDuration` / `trace:duration` — the whole trace's span (end −
    /// start), in nanoseconds.
    TraceDurationNs { op: ComparisonOp, nanos: i64 },
    /// `rootName` / `trace:rootName` — Eq/Neq/Re/Nre on the trace root
    /// span's (byte-capped) name from the trace-context co-load.
    RootName { op: ComparisonOp, value: String },
    /// `rootServiceName` / `trace:rootService` — as
    /// [`TraceCtxPred::RootName`] over the root span's service.
    RootServiceName { op: ComparisonOp, value: String },
    /// `trace:id` — Eq/Neq/Re/Nre against the lowercase hex rendering of the
    /// 16-byte `trace_id`. `value` is lowercased for Eq/Neq.
    TraceId { op: ComparisonOp, value: String },
}

/// A compiled arithmetic operand tree (issue #185 `arith.*`): numeric
/// literals fold to `f64`; field operands (an attribute's `val_num`, or a
/// numeric physical intrinsic) resolve engine-side per candidate span, so
/// no per-row work reaches the client for constant subexpressions.
#[derive(Debug, Clone, PartialEq)]
pub enum ArithNode {
    /// A folded numeric literal (a number, or a duration in nanoseconds).
    Value(f64),
    /// A field operand resolved per span (`val_num` for an attribute, the
    /// physical numeric column for `duration`/`status`/`kind`).
    Operand(CompareOperand),
    /// Unary negation.
    Neg(Box<ArithNode>),
    /// A binary arithmetic composition.
    Bin {
        op: ArithOp,
        lhs: Box<ArithNode>,
        rhs: Box<ArithNode>,
    },
}

/// How Phase 2 evaluates one leaf.
#[derive(Debug, Clone, PartialEq)]
pub enum LeafEval {
    Physical(PhysicalPredicate),
    /// A trace-level intrinsic comparison (issue #184), evaluated against
    /// the per-trace context co-load.
    TraceCtx(TraceCtxPred),
    /// Membership in `probe`'s result set; `negated` inverts it (the
    /// ratified `!=`/`!~` absent-key rule).
    Attr {
        probe: AttrProbe,
        negated: bool,
    },
    /// A nested-set structural intrinsic comparison (issue #181),
    /// evaluated engine-side against the query-time numbering. No
    /// generator column exists, so the leaf pairs with the time-range
    /// candidate generator.
    NestedSet {
        field: NestedSetField,
        op: ComparisonOp,
        value: f64,
    },
    /// A field-vs-field comparison (issue #183 `comparison.rhs_attribute`):
    /// both operands resolved per candidate span and compared engine-side.
    FieldCompare {
        lhs: CompareOperand,
        rhs: CompareOperand,
        op: ComparisonOp,
    },
    /// An arithmetic comparison (issue #185 `arith.*`): both operand trees
    /// resolve to a numeric value per candidate span and are compared
    /// engine-side.
    Arith {
        lhs: ArithNode,
        op: ComparisonOp,
        rhs: ArithNode,
    },
}

/// One fully classified leaf comparison.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledLeaf {
    pub generator: LeafGenerator,
    pub eval: LeafEval,
}

pub(crate) const OP_SYMBOLS: [(ComparisonOp, &str); 6] = [
    (ComparisonOp::Eq, "="),
    (ComparisonOp::Neq, "!="),
    (ComparisonOp::Gt, ">"),
    (ComparisonOp::Gte, ">="),
    (ComparisonOp::Lt, "<"),
    (ComparisonOp::Lte, "<="),
];

fn sql_op(op: ComparisonOp) -> Option<&'static str> {
    OP_SYMBOLS.iter().find(|(o, _)| *o == op).map(|(_, s)| *s)
}

fn status_code(v: StatusValue) -> i8 {
    match v {
        StatusValue::Unset => 0,
        StatusValue::Ok => 1,
        StatusValue::Error => 2,
    }
}

fn kind_code(v: SpanKindValue) -> i8 {
    match v {
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
fn parse_num(raw: &str) -> Result<f64, PlanError> {
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

fn string_op_leaf(
    field_name: &str,
    op: ComparisonOp,
    value: &Value,
) -> Result<(ComparisonOp, String), PlanError> {
    let Value::String(s) = value else {
        return Err(PlanError::TypeMismatch(format!(
            "{field_name} requires a string value"
        )));
    };
    match op {
        ComparisonOp::Eq | ComparisonOp::Neq | ComparisonOp::Re | ComparisonOp::Nre => {
            Ok((op, s.clone()))
        }
        _ => Err(PlanError::TypeMismatch(format!(
            "{field_name} supports only = != =~ !~"
        ))),
    }
}

/// Renders a physical predicate as its pre-escaped SQL fragment (used by
/// the generator queries; Phase-2 evaluation uses the typed form).
pub(crate) fn physical_sql(p: &PhysicalPredicate) -> String {
    match p {
        PhysicalPredicate::Name { op, value } => string_column_sql("name", *op, value),
        PhysicalPredicate::Service { op, value } => string_column_sql("service", *op, value),
        PhysicalPredicate::DurationNs { op, nanos } => {
            let sym = sql_op(*op).expect("duration ops are ordering/equality by construction");
            format!("duration_ns {sym} {nanos}")
        }
        PhysicalPredicate::Status { op, code } => {
            let sym = sql_op(*op).expect("status ops are Eq/Neq by construction");
            format!("status_code {sym} {code}")
        }
        PhysicalPredicate::Kind { op, code } => {
            let sym = sql_op(*op).expect("kind ops are Eq/Neq by construction");
            format!("kind {sym} {code}")
        }
        PhysicalPredicate::StatusMessage { op, value } => {
            // Issue #184 code review: compare the CAPPED column — the
            // shared `byte_cap_expr` helper, the single source of the cap
            // — so Phase-1 candidate selection agrees byte-for-byte with
            // the capped `status_message` Phase 2 hydrates and evaluates
            // (a raw comparison silently dropped any over-cap message
            // whose capped rendering equals the literal). No index is
            // lost: `status_message` has none (SpanScan class — the
            // bounded time-window scan prunes on `timestamp_ns` alone).
            string_column_sql(&byte_cap_expr("status_message"), *op, value)
        }
        PhysicalPredicate::SpanIdHex { op, value } => hex_column_sql("span_id", *op, value),
        PhysicalPredicate::ParentIdHex { op, value } => hex_column_sql("parent_id", *op, value),
        PhysicalPredicate::InstrumentationName { op, value } => {
            // Issue #192: compare the CAPPED column (the `statusMessage`
            // precedent) so Phase-1 candidate selection agrees byte-for-byte
            // with the capped `scope_name` Phase 2 hydrates and evaluates. No
            // index is lost: `scope_name` has none (SpanScan class — the
            // bounded time-window scan prunes on `timestamp_ns` alone).
            string_column_sql(&byte_cap_expr("scope_name"), *op, value)
        }
        PhysicalPredicate::InstrumentationVersion { op, value } => {
            string_column_sql(&byte_cap_expr("scope_version"), *op, value)
        }
    }
}

/// The all-zero `parent_id`/`trace_id` sentinel rendering the codebase uses
/// for root detection (`trace_edges_mv`, `catalog.rs`) — an 8-byte fixed
/// string of zeros. Keeping the exact spelling means a root leaf reads the
/// same "no parent" convention the writer/graph MV emit.
pub(crate) const ZERO_PARENT_SQL: &str = "toFixedString(unhex('0000000000000000'), 8)";

/// Renders a hex-string comparison against a raw `FixedString` id column
/// (`span_id`/`parent_id`) — `lower(hex(col))` vs the (Eq/Neq: lowercased,
/// Re/Nre: raw) value, so the SQL predicate matches the engine-side hex
/// comparison in [`crate::traces::search_eval`].
fn hex_column_sql(column: &str, op: ComparisonOp, value: &str) -> String {
    match op {
        ComparisonOp::Eq => format!("lower(hex({column})) = {}", escape::ch_string(value)),
        ComparisonOp::Neq => format!("lower(hex({column})) != {}", escape::ch_string(value)),
        ComparisonOp::Re => format!(
            "match(lower(hex({column})), {})",
            escape::ch_regex_anchored(value)
        ),
        ComparisonOp::Nre => format!(
            "NOT match(lower(hex({column})), {})",
            escape::ch_regex_anchored(value)
        ),
        _ => unreachable!("hex id columns accept only = != =~ !~ (checked at compile_leaf)"),
    }
}

fn string_column_sql(column: &str, op: ComparisonOp, value: &str) -> String {
    match op {
        ComparisonOp::Eq => format!("{column} = {}", escape::ch_string(value)),
        ComparisonOp::Neq => format!("{column} != {}", escape::ch_string(value)),
        ComparisonOp::Re => format!("match({column}, {})", escape::ch_regex_anchored(value)),
        ComparisonOp::Nre => format!("NOT match({column}, {})", escape::ch_regex_anchored(value)),
        _ => unreachable!("string columns accept only = != =~ !~ (checked at compile_leaf)"),
    }
}

/// Renders an attribute probe's value predicate as its pre-escaped SQL
/// fragment.
pub(crate) fn value_pred_sql(pred: &ValuePred) -> String {
    match pred {
        ValuePred::StringEq(v) => format!("val = {}", escape::ch_string(v)),
        ValuePred::Regex(pat) => format!("match(val, {})", escape::ch_regex_anchored(pat)),
        ValuePred::Num { op, value } => {
            let sym = sql_op(*op).expect("numeric ops are ordering/equality by construction");
            format!("val_num {sym} {}", render_num(*value))
        }
        // Key existence: any value satisfies it — the no-op `1` predicate
        // leaves a pure `(key)` prefix scan (issue #185).
        ValuePred::KeyExists => "1".to_string(),
        // A pre-rendered `val_num` arithmetic predicate (issue #185).
        ValuePred::NumExpr(sql) => sql.clone(),
    }
}

fn attr_scope_literal(scope: AttrScope) -> Option<&'static str> {
    match scope {
        AttrScope::Span => Some("span"),
        AttrScope::Resource => Some("resource"),
        AttrScope::Unscoped => None,
        // Issue #192: `instrumentation.<key>` attributes are index-served
        // under the writer's `scope='instrumentation'` discriminator.
        AttrScope::Instrumentation => Some("instrumentation"),
    }
}

/// Compiles one attribute leaf (anything but `resource.service.name`).
fn compile_attr_leaf(
    scope: AttrScope,
    key: &str,
    op: ComparisonOp,
    value: &Value,
) -> Result<CompiledLeaf, PlanError> {
    let scope_lit = attr_scope_literal(scope);
    let (pred, negated, class) = match (op, value) {
        (ComparisonOp::Eq, Value::String(s)) => {
            (ValuePred::StringEq(s.clone()), false, GenClass::AttrEq)
        }
        (ComparisonOp::Neq, Value::String(s)) => {
            (ValuePred::StringEq(s.clone()), true, GenClass::TimeRange)
        }
        (ComparisonOp::Eq, Value::Bool(b)) => {
            (ValuePred::StringEq(b.to_string()), false, GenClass::AttrEq)
        }
        (ComparisonOp::Neq, Value::Bool(b)) => (
            ValuePred::StringEq(b.to_string()),
            true,
            GenClass::TimeRange,
        ),
        (ComparisonOp::Re, Value::String(s)) => {
            (ValuePred::Regex(s.clone()), false, GenClass::AttrKeyScan)
        }
        (ComparisonOp::Nre, Value::String(s)) => {
            (ValuePred::Regex(s.clone()), true, GenClass::TimeRange)
        }
        (op, Value::Number(raw)) if sql_op(op).is_some() => {
            let n = parse_num(raw)?;
            match op {
                ComparisonOp::Neq => (
                    ValuePred::Num {
                        op: ComparisonOp::Eq,
                        value: n,
                    },
                    true,
                    GenClass::TimeRange,
                ),
                _ => (
                    ValuePred::Num { op, value: n },
                    false,
                    GenClass::AttrKeyScan,
                ),
            }
        }
        (op, Value::Duration(d)) if sql_op(op).is_some() => {
            let n = d.as_nanos() as f64;
            match op {
                ComparisonOp::Neq => (
                    ValuePred::Num {
                        op: ComparisonOp::Eq,
                        value: n,
                    },
                    true,
                    GenClass::TimeRange,
                ),
                _ => (
                    ValuePred::Num { op, value: n },
                    false,
                    GenClass::AttrKeyScan,
                ),
            }
        }
        _ => {
            return Err(PlanError::TypeMismatch(format!(
                "attribute {key:?} does not support operator {op} on this value type"
            )));
        }
    };
    let probe = AttrProbe {
        key: key.to_string(),
        scope: scope_lit,
        pred,
    };
    let generator = if class == GenClass::TimeRange {
        LeafGenerator::time_range()
    } else {
        LeafGenerator {
            class,
            table: GenTable::Attrs,
            predicate: attr_generator_predicate(&probe, class),
            prewhere: None,
        }
    };
    Ok(CompiledLeaf {
        generator,
        eval: LeafEval::Attr { probe, negated },
    })
}

/// The attr-index generator predicate: `key = '<k>'` (+ scope when
/// scoped) plus the value predicate — the value side is prefix-served for
/// [`GenClass::AttrEq`] and a key-only filter for
/// [`GenClass::AttrKeyScan`] (docs/schemas.md §4.2's generator table).
fn attr_generator_predicate(probe: &AttrProbe, _class: GenClass) -> String {
    let mut parts = vec![format!("key = {}", escape::ch_string(&probe.key))];
    parts.push(value_pred_sql(&probe.pred));
    if let Some(scope) = probe.scope {
        parts.push(format!("scope = {}", escape::ch_string(scope)));
    }
    parts.join(" AND ")
}

/// Compiles the `resource.service.name` fast path (adjudication 5: only
/// the resource-scoped form lowers to the physical `service` column).
fn compile_service_leaf(op: ComparisonOp, value: &Value) -> Result<CompiledLeaf, PlanError> {
    let (op, s) = string_op_leaf("resource.service.name", op, value)?;
    let physical = PhysicalPredicate::Service {
        op,
        value: s.clone(),
    };
    let generator = match op {
        ComparisonOp::Eq => LeafGenerator {
            class: GenClass::ServiceEq,
            table: GenTable::Spans,
            predicate: String::new(),
            prewhere: Some(format!("service = {}", escape::ch_string(&s))),
        },
        // Positive regex: the writer indexes `service.name` at
        // scope='resource' like any other resource attribute, so `=~`
        // generates via the key-prefixed index (plan v4 delta 3) —
        // evaluation still runs on the physical column.
        ComparisonOp::Re => {
            let probe = AttrProbe {
                key: "service.name".to_string(),
                scope: Some("resource"),
                pred: ValuePred::Regex(s.clone()),
            };
            LeafGenerator {
                class: GenClass::AttrKeyScan,
                table: GenTable::Attrs,
                predicate: attr_generator_predicate(&probe, GenClass::AttrKeyScan),
                prewhere: None,
            }
        }
        // Negations: absence is not indexable — complete time-range
        // superset, exact Phase-2 evaluation.
        _ => LeafGenerator::time_range(),
    };
    Ok(CompiledLeaf {
        generator,
        eval: LeafEval::Physical(physical),
    })
}

/// Lowercases a hex string value for the case-insensitive Eq/Neq id
/// comparisons; regex operators keep the raw pattern.
fn hex_value(op: ComparisonOp, s: &str) -> String {
    match op {
        ComparisonOp::Eq | ComparisonOp::Neq => s.to_lowercase(),
        _ => s.to_string(),
    }
}

/// Compiles a `span:id` / `span:parentID` leaf (issue #184): a hex-string
/// comparison (only `= != =~ !~`) over the raw id column, exact in Phase 2
/// and paired with a bounded-window `SpanScan` generator.
fn compile_span_hex_leaf(
    column_kind: SpanHexColumn,
    op: ComparisonOp,
    value: &Value,
) -> Result<CompiledLeaf, PlanError> {
    let field_name = column_kind.field_name();
    let (op, s) = string_op_leaf(field_name, op, value)?;
    let stored = hex_value(op, &s);
    let physical = match column_kind {
        SpanHexColumn::SpanId => PhysicalPredicate::SpanIdHex { op, value: stored },
        SpanHexColumn::ParentId => PhysicalPredicate::ParentIdHex { op, value: stored },
    };
    Ok(CompiledLeaf {
        generator: spans_generator_for(&physical),
        eval: LeafEval::Physical(physical),
    })
}

/// Which raw id column a `span:id` / `span:parentID` leaf reads.
#[derive(Debug, Clone, Copy)]
enum SpanHexColumn {
    SpanId,
    ParentId,
}

impl SpanHexColumn {
    fn field_name(self) -> &'static str {
        match self {
            SpanHexColumn::SpanId => "span:id",
            SpanHexColumn::ParentId => "span:parentID",
        }
    }
}

/// Compiles a numeric trace-level leaf (`span:childCount`,
/// `traceDuration`/`trace:duration`) — the six ordering/equality operators,
/// evaluated engine-side against the per-trace co-load. No column pushdown,
/// so it pairs with the trace-wide time-range generator.
fn compile_trace_num_leaf(
    which: TraceNumField,
    op: ComparisonOp,
    value: &Value,
) -> Result<CompiledLeaf, PlanError> {
    if sql_op(op).is_none() {
        return Err(PlanError::TypeMismatch(format!(
            "{} does not support regex operators",
            which.field_name()
        )));
    }
    let pred = match which {
        TraceNumField::ChildCount => {
            let Value::Number(raw) = value else {
                return Err(PlanError::TypeMismatch(
                    "span:childCount requires a numeric value".to_string(),
                ));
            };
            TraceCtxPred::ChildCount {
                op,
                value: parse_num(raw)?,
            }
        }
        TraceNumField::TraceDuration => {
            let Value::Duration(d) = value else {
                return Err(PlanError::TypeMismatch(
                    "traceDuration requires a duration literal".to_string(),
                ));
            };
            let nanos = i64::try_from(d.as_nanos()).map_err(|_| {
                PlanError::TypeMismatch("duration literal exceeds the i64 range".to_string())
            })?;
            TraceCtxPred::TraceDurationNs { op, nanos }
        }
    };
    Ok(CompiledLeaf {
        generator: LeafGenerator::time_range(),
        eval: LeafEval::TraceCtx(pred),
    })
}

/// Which numeric trace-level intrinsic a leaf compares.
#[derive(Debug, Clone, Copy)]
enum TraceNumField {
    ChildCount,
    TraceDuration,
}

impl TraceNumField {
    fn field_name(self) -> &'static str {
        match self {
            TraceNumField::ChildCount => "span:childCount",
            TraceNumField::TraceDuration => "traceDuration",
        }
    }
}

/// Compiles a `rootName` / `rootServiceName` leaf (issue #184): a string
/// comparison against the trace root span's value, exact in Phase 2 via
/// the trace-wide context co-load. **`TimeRange`-class for every
/// operator** (plan v2 §Performance — trace-level leaves generate no
/// candidates themselves): a windowed root-span scan
/// (`parent_id = <zero> AND <pred>`) would silently MISS any trace whose
/// true root predates the search window — exactly the window-spanning
/// traces the co-load exists to evaluate correctly — so the complete
/// window superset is the only sound generator. Sole-predicate scale
/// characterization is #25-routed (same class as `{}` today).
fn compile_root_leaf(
    which: RootField,
    op: ComparisonOp,
    value: &Value,
) -> Result<CompiledLeaf, PlanError> {
    let (op, s) = string_op_leaf(which.field_name(), op, value)?;
    let pred = match which {
        RootField::Name => TraceCtxPred::RootName { op, value: s },
        RootField::ServiceName => TraceCtxPred::RootServiceName { op, value: s },
    };
    Ok(CompiledLeaf {
        generator: LeafGenerator::time_range(),
        eval: LeafEval::TraceCtx(pred),
    })
}

/// Which trace root string a `rootName` / `rootServiceName` leaf compares.
#[derive(Debug, Clone, Copy)]
enum RootField {
    Name,
    ServiceName,
}

impl RootField {
    fn field_name(self) -> &'static str {
        match self {
            RootField::Name => "rootName",
            RootField::ServiceName => "rootServiceName",
        }
    }
}

/// Compiles a `trace:id` leaf (issue #184): a hex comparison over the
/// `trace_id` column. `=` renders `trace_id = unhex('…')` — the
/// `ORDER BY (trace_id, timestamp_ns)` PK-prefix prune (Tier-1
/// EXPLAIN-provable); the other operators stay bounded-window `SpanScan`s.
/// Evaluated exactly in Phase 2 against the candidate trace's id.
fn compile_trace_id_leaf(op: ComparisonOp, value: &Value) -> Result<CompiledLeaf, PlanError> {
    let (op, s) = string_op_leaf("trace:id", op, value)?;
    let stored = hex_value(op, &s);
    let predicate = match op {
        ComparisonOp::Eq => format!("trace_id = unhex({})", escape::ch_string(&stored)),
        ComparisonOp::Neq => format!("trace_id != unhex({})", escape::ch_string(&stored)),
        ComparisonOp::Re => format!(
            "match(lower(hex(trace_id)), {})",
            escape::ch_regex_anchored(&stored)
        ),
        ComparisonOp::Nre => format!(
            "NOT match(lower(hex(trace_id)), {})",
            escape::ch_regex_anchored(&stored)
        ),
        _ => unreachable!("trace:id accepts only = != =~ !~"),
    };
    Ok(CompiledLeaf {
        generator: LeafGenerator {
            class: GenClass::SpanScan,
            table: GenTable::Spans,
            predicate,
            prewhere: None,
        },
        eval: LeafEval::TraceCtx(TraceCtxPred::TraceId { op, value: stored }),
    })
}

/// Classifies one leaf comparison — the shared compiler entry point (T5
/// search and T7 metrics both consume it).
pub fn compile_leaf(
    field: &Field,
    op: ComparisonOp,
    value: &Value,
) -> Result<CompiledLeaf, PlanError> {
    match field {
        Field::Intrinsic(Intrinsic::Name) => {
            let (op, s) = string_op_leaf("name", op, value)?;
            let physical = PhysicalPredicate::Name { op, value: s };
            Ok(CompiledLeaf {
                generator: spans_generator_for(&physical),
                eval: LeafEval::Physical(physical),
            })
        }
        Field::Intrinsic(Intrinsic::Duration) => {
            let Value::Duration(d) = value else {
                return Err(PlanError::TypeMismatch(
                    "duration requires a duration literal".to_string(),
                ));
            };
            if sql_op(op).is_none() {
                return Err(PlanError::TypeMismatch(
                    "duration does not support regex operators".to_string(),
                ));
            }
            let nanos = i64::try_from(d.as_nanos()).map_err(|_| {
                PlanError::TypeMismatch("duration literal exceeds the i64 range".to_string())
            })?;
            let physical = PhysicalPredicate::DurationNs { op, nanos };
            Ok(CompiledLeaf {
                generator: LeafGenerator {
                    class: GenClass::Duration,
                    table: GenTable::Spans,
                    predicate: physical_sql(&physical),
                    prewhere: None,
                },
                eval: LeafEval::Physical(physical),
            })
        }
        Field::Intrinsic(Intrinsic::Status) => {
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
            let physical = PhysicalPredicate::Status {
                op,
                code: status_code(*s),
            };
            Ok(CompiledLeaf {
                generator: spans_generator_for(&physical),
                eval: LeafEval::Physical(physical),
            })
        }
        Field::Intrinsic(Intrinsic::Kind) => {
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
            let physical = PhysicalPredicate::Kind {
                op,
                code: kind_code(*k),
            };
            Ok(CompiledLeaf {
                generator: spans_generator_for(&physical),
                eval: LeafEval::Physical(physical),
            })
        }
        Field::Intrinsic(Intrinsic::NestedSetParent) => {
            compile_nested_set_leaf(NestedSetField::Parent, op, value)
        }
        Field::Intrinsic(Intrinsic::NestedSetLeft) => {
            compile_nested_set_leaf(NestedSetField::Left, op, value)
        }
        Field::Intrinsic(Intrinsic::NestedSetRight) => {
            compile_nested_set_leaf(NestedSetField::Right, op, value)
        }
        // -- issue #184: the colon-scope intrinsic namespace -------------
        Field::Intrinsic(Intrinsic::StatusMessage) => {
            let (op, s) = string_op_leaf("statusMessage", op, value)?;
            let physical = PhysicalPredicate::StatusMessage { op, value: s };
            Ok(CompiledLeaf {
                generator: spans_generator_for(&physical),
                eval: LeafEval::Physical(physical),
            })
        }
        Field::Intrinsic(Intrinsic::SpanId) => {
            compile_span_hex_leaf(SpanHexColumn::SpanId, op, value)
        }
        Field::Intrinsic(Intrinsic::ParentId) => {
            compile_span_hex_leaf(SpanHexColumn::ParentId, op, value)
        }
        Field::Intrinsic(Intrinsic::TraceId) => compile_trace_id_leaf(op, value),
        Field::Intrinsic(Intrinsic::TraceDuration) => {
            compile_trace_num_leaf(TraceNumField::TraceDuration, op, value)
        }
        Field::Intrinsic(Intrinsic::ChildCount) => {
            compile_trace_num_leaf(TraceNumField::ChildCount, op, value)
        }
        Field::Intrinsic(Intrinsic::RootName) => compile_root_leaf(RootField::Name, op, value),
        Field::Intrinsic(Intrinsic::RootServiceName) => {
            compile_root_leaf(RootField::ServiceName, op, value)
        }
        // -- issue #192: the instrumentation-scope intrinsics — hydrated
        // physical columns, the `statusMessage` precedent -----------------
        Field::Intrinsic(Intrinsic::InstrumentationName) => {
            let (op, s) = string_op_leaf("instrumentation:name", op, value)?;
            let physical = PhysicalPredicate::InstrumentationName { op, value: s };
            Ok(CompiledLeaf {
                generator: spans_generator_for(&physical),
                eval: LeafEval::Physical(physical),
            })
        }
        Field::Intrinsic(Intrinsic::InstrumentationVersion) => {
            let (op, s) = string_op_leaf("instrumentation:version", op, value)?;
            let physical = PhysicalPredicate::InstrumentationVersion { op, value: s };
            Ok(CompiledLeaf {
                generator: spans_generator_for(&physical),
                eval: LeafEval::Physical(physical),
            })
        }
        Field::Attribute { scope, key } => {
            if *scope == AttrScope::Resource && key == "service.name" {
                compile_service_leaf(op, value)
            } else {
                compile_attr_leaf(*scope, key, op, value)
            }
        }
    }
}

/// Compiles one nested-set intrinsic leaf (issue #181): a numeric
/// comparison against the query-time modified-preorder numbering. The
/// six ordering/equality operators are allowed; regex operators are a
/// [`PlanError::TypeMismatch`]. There is no candidate generator column,
/// so the leaf pairs with the complete time-range superset generator
/// (evaluation is exact in Phase 2) — a nested-set-only query is as broad
/// as `{}`, bounded by the scan budget.
fn compile_nested_set_leaf(
    field: NestedSetField,
    op: ComparisonOp,
    value: &Value,
) -> Result<CompiledLeaf, PlanError> {
    if sql_op(op).is_none() {
        return Err(PlanError::TypeMismatch(
            "nested-set intrinsics do not support regex operators".to_string(),
        ));
    }
    let Value::Number(raw) = value else {
        return Err(PlanError::TypeMismatch(
            "nested-set intrinsics require a numeric value".to_string(),
        ));
    };
    let value = parse_num(raw)?;
    Ok(CompiledLeaf {
        generator: LeafGenerator::time_range(),
        eval: LeafEval::NestedSet { field, op, value },
    })
}

/// Maps one comparison operand `Field` to its [`CompareOperand`]
/// resolution (issue #183). Nested-set intrinsics have no comparable
/// value on the field-vs-field path and are rejected.
fn compare_operand(field: &Field) -> Result<CompareOperand, PlanError> {
    match field {
        Field::Intrinsic(Intrinsic::Name) => Ok(CompareOperand::Name),
        Field::Intrinsic(Intrinsic::Duration) => Ok(CompareOperand::Duration),
        Field::Intrinsic(Intrinsic::Status) => Ok(CompareOperand::Status),
        Field::Intrinsic(Intrinsic::Kind) => Ok(CompareOperand::Kind),
        Field::Intrinsic(
            Intrinsic::NestedSetParent | Intrinsic::NestedSetLeft | Intrinsic::NestedSetRight,
        ) => Err(PlanError::TypeMismatch(
            "nested-set intrinsics are not supported in a field-vs-field comparison".to_string(),
        )),
        // Issue #184: the trace-level/scoped intrinsics resolve from the
        // per-trace co-load (or an id rendering), not a per-span column
        // value — out of scope on the field-vs-field path (a clean 400,
        // mirroring nested-set).
        Field::Intrinsic(
            Intrinsic::StatusMessage
            | Intrinsic::ChildCount
            | Intrinsic::SpanId
            | Intrinsic::ParentId
            | Intrinsic::TraceId
            | Intrinsic::TraceDuration
            | Intrinsic::RootName
            | Intrinsic::RootServiceName
            | Intrinsic::InstrumentationName
            | Intrinsic::InstrumentationVersion,
        ) => Err(PlanError::TypeMismatch(
            "this intrinsic is not supported in a field-vs-field comparison".to_string(),
        )),
        Field::Attribute { scope, key }
            if *scope == AttrScope::Resource && key == "service.name" =>
        {
            Ok(CompareOperand::Service)
        }
        Field::Attribute { scope, key } => Ok(CompareOperand::Attr {
            key: key.clone(),
            scope: attr_scope_literal(*scope),
        }),
    }
}

/// A key-existence Phase-1 generator for a field-vs-field comparison
/// (issue #183): a `key = '<k>'` (+ scope) key-only `(key)` prefix scan —
/// an index-served SUPERSET (a matching span must possess the key), never
/// a bare time-range fallback.
fn key_existence_generator(key: &str, scope: Option<&'static str>) -> LeafGenerator {
    let mut predicate = format!("key = {}", escape::ch_string(key));
    if let Some(s) = scope {
        predicate.push_str(&format!(" AND scope = {}", escape::ch_string(s)));
    }
    LeafGenerator {
        class: GenClass::AttrKeyScan,
        table: GenTable::Attrs,
        predicate,
        prewhere: None,
    }
}

/// Compiles a field-vs-field comparison leaf (issue #183
/// `comparison.rhs_attribute`). Regex operators never reach here (the
/// parser rejects a field RHS for `=~`/`!~`), but `compile` is a public
/// surface over any AST, so they are rejected defensively. Phase-1
/// pruning is the key-existence scan of an attribute operand (a matching
/// span must possess that key); if both operands are physical intrinsics
/// there is no attr key to prune on, so the leaf pairs with the complete
/// time-range superset.
fn compile_field_compare(
    lhs: &Field,
    op: ComparisonOp,
    rhs: &Field,
) -> Result<CompiledLeaf, PlanError> {
    if matches!(op, ComparisonOp::Re | ComparisonOp::Nre) {
        return Err(PlanError::TypeMismatch(
            "a field-vs-field comparison does not support regex operators".to_string(),
        ));
    }
    let lhs = compare_operand(lhs)?;
    let rhs = compare_operand(rhs)?;
    // Prune on whichever operand carries an attribute key (the LHS wins a
    // tie — deterministic). Both-intrinsic compares have no index to prune.
    let generator = match (&lhs, &rhs) {
        (CompareOperand::Attr { key, scope }, _) => key_existence_generator(key, *scope),
        (_, CompareOperand::Attr { key, scope }) => key_existence_generator(key, *scope),
        _ => LeafGenerator::time_range(),
    };
    Ok(CompiledLeaf {
        generator,
        eval: LeafEval::FieldCompare { lhs, rhs, op },
    })
}

/// Compiles an attribute-existence leaf (issue #185 `existence.*`): the
/// span possesses the attribute key. Served by the scoped attribute index
/// as a key-only `(key)` prefix scan (PREWHERE-eligible, granule-pruning).
/// `resource.service.name` existence goes through the index like any other
/// resource attribute (the writer indexes it). Intrinsic existence
/// (`name`, `duration`, …) is always trivially true and out of scope — a
/// clean `400`.
fn compile_exists(field: &Field) -> Result<CompiledLeaf, PlanError> {
    let (scope, key) = match field {
        Field::Attribute { scope, key } => (*scope, key.clone()),
        Field::Intrinsic(_) => {
            return Err(PlanError::TypeMismatch(
                "existence checks are only supported on attributes".to_string(),
            ));
        }
    };
    let probe = AttrProbe {
        key,
        scope: attr_scope_literal(scope),
        pred: ValuePred::KeyExists,
    };
    let generator = LeafGenerator {
        class: GenClass::AttrKeyScan,
        table: GenTable::Attrs,
        predicate: attr_generator_predicate(&probe, GenClass::AttrKeyScan),
        prewhere: None,
    };
    Ok(CompiledLeaf {
        generator,
        eval: LeafEval::Attr {
            probe,
            negated: false,
        },
    })
}

/// Compiles an arithmetic operand tree (issue #185 `arith.*`): numeric
/// literals fold, field operands resolve engine-side. A `Value` literal
/// that is not numeric (string/bool/status/kind) is a type mismatch.
fn compile_arith_node(operand: &Operand) -> Result<ArithNode, PlanError> {
    match operand {
        Operand::Literal(Value::Number(raw)) => Ok(ArithNode::Value(parse_num(raw)?)),
        Operand::Literal(Value::Duration(d)) => Ok(ArithNode::Value(d.as_nanos() as f64)),
        Operand::Literal(_) => Err(PlanError::TypeMismatch(
            "arithmetic operands must be numeric (a number, duration, or numeric field)"
                .to_string(),
        )),
        Operand::Field(field) => Ok(ArithNode::Operand(compare_operand(field)?)),
        Operand::Neg(inner) => Ok(ArithNode::Neg(Box::new(compile_arith_node(inner)?))),
        Operand::Arith { op, lhs, rhs } => Ok(ArithNode::Bin {
            op: *op,
            lhs: Box::new(compile_arith_node(lhs)?),
            rhs: Box::new(compile_arith_node(rhs)?),
        }),
    }
}

/// Constant-folds an operand tree to a scalar when it references no field
/// (all-literal subexpressions fold at plan time — no column work).
/// Returns `None` when a field operand is present, or when a division /
/// modulo by zero makes the fold undefined.
fn fold_operand(operand: &Operand) -> Option<f64> {
    match operand {
        Operand::Literal(Value::Number(raw)) => raw.parse::<f64>().ok().filter(|n| n.is_finite()),
        Operand::Literal(Value::Duration(d)) => Some(d.as_nanos() as f64),
        Operand::Literal(_) => None,
        Operand::Field(_) => None,
        Operand::Neg(inner) => fold_operand(inner).map(|v| -v),
        Operand::Arith { op, lhs, rhs } => {
            let l = fold_operand(lhs)?;
            let r = fold_operand(rhs)?;
            apply_arith(*op, l, r)
        }
    }
}

/// Applies one arithmetic operator to two finite operands. A division or
/// modulo by zero yields `None` (no match), never an infinity/NaN
/// predicate.
pub(crate) fn apply_arith(op: ArithOp, l: f64, r: f64) -> Option<f64> {
    let v = match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div => {
            if r == 0.0 {
                return None;
            }
            l / r
        }
        ArithOp::Mod => {
            if r == 0.0 {
                return None;
            }
            l % r
        }
        ArithOp::Pow => l.powf(r),
    };
    v.is_finite().then_some(v)
}

/// Compiles an arithmetic comparison leaf (issue #185 `arith.*`). A lone
/// attribute compared with an all-literal folded scalar (the common probe
/// form `{ .a = 1 + 2 }`) lowers to the ordinary numeric attribute leaf
/// (`val_num` pushdown, index-served) — the literal fold erases the
/// arithmetic. Any other shape (a field inside the arithmetic) keeps the
/// operand trees and evaluates engine-side, pruning on a referenced
/// attribute key when one exists.
fn compile_field_arith(
    lhs: &Operand,
    op: ComparisonOp,
    rhs: &Operand,
) -> Result<CompiledLeaf, PlanError> {
    if matches!(op, ComparisonOp::Re | ComparisonOp::Nre) {
        return Err(PlanError::TypeMismatch(
            "arithmetic comparisons do not support regex operators".to_string(),
        ));
    }
    // Fold `attr <op> <all-literal>` (and the mirror) to a numeric attr
    // leaf so the common probe forms get the `val_num` pushdown + goldens
    // of a plain numeric comparison.
    if let Operand::Field(field @ Field::Attribute { .. }) = lhs
        && let Some(n) = fold_operand(rhs)
    {
        return compile_leaf(field, op, &Value::Number(render_num(n)));
    }
    if let Operand::Field(field @ Field::Attribute { .. }) = rhs
        && let Some(n) = fold_operand(lhs)
    {
        return compile_leaf(field, flip_comparison(op), &Value::Number(render_num(n)));
    }
    let lhs_node = compile_arith_node(lhs)?;
    let rhs_node = compile_arith_node(rhs)?;

    // Classify the operands across both sides.
    let mut attrs: Vec<(String, Option<&'static str>)> = Vec::new();
    let mut has_physical = false;
    let mut has_string = false;
    analyze_arith(&lhs_node, &mut attrs, &mut has_physical, &mut has_string);
    analyze_arith(&rhs_node, &mut attrs, &mut has_physical, &mut has_string);
    // Only total operators (`+ - *`) push column-side: `/ % ^` can produce
    // a division-by-zero / NaN the Rust evaluator maps to no-match, so
    // rendering them into SQL would diverge — those stay post-hydration.
    let total = arith_is_total(&lhs_node) && arith_is_total(&rhs_node);

    // Single-attribute arithmetic with literal coefficients → a column-side
    // `val_num` predicate (the query-performance mandate): index-served,
    // no per-row client work — like the metric path.
    if total
        && !has_string
        && !has_physical
        && attrs.len() == 1
        && let Some(lhs_sql) = render_arith_sql(&lhs_node, &val_num_col)
        && let Some(rhs_sql) = render_arith_sql(&rhs_node, &val_num_col)
    {
        let (key, scope) = attrs[0].clone();
        // `!=` keeps the ratified absent-key rule: the positive (`=`) probe
        // negated over the time-range superset (absent-key spans match).
        let (pred_sql, negated) = if op == ComparisonOp::Neq {
            (format!("{lhs_sql} = {rhs_sql}"), true)
        } else {
            let sym = sql_op(op).expect("arith comparison ops are the six by construction");
            (format!("{lhs_sql} {sym} {rhs_sql}"), false)
        };
        let probe = AttrProbe {
            key,
            scope,
            pred: ValuePred::NumExpr(pred_sql),
        };
        let generator = if negated {
            LeafGenerator::time_range()
        } else {
            LeafGenerator {
                class: GenClass::AttrKeyScan,
                table: GenTable::Attrs,
                predicate: attr_generator_predicate(&probe, GenClass::AttrKeyScan),
                prewhere: None,
            }
        };
        return Ok(CompiledLeaf {
            generator,
            eval: LeafEval::Attr { probe, negated },
        });
    }

    // Single physical-intrinsic arithmetic (`duration * 2 > 1s`) → a
    // column-side `SpanScan` predicate that prunes candidates; Phase 2
    // confirms the same arithmetic in Rust over the hydrated span.
    if total
        && !has_string
        && attrs.is_empty()
        && has_physical
        && let Some(lhs_sql) = render_arith_sql(&lhs_node, &physical_col)
        && let Some(rhs_sql) = render_arith_sql(&rhs_node, &physical_col)
    {
        let sym = sql_op(op).expect("arith comparison ops are the six by construction");
        return Ok(CompiledLeaf {
            generator: LeafGenerator {
                class: GenClass::SpanScan,
                table: GenTable::Spans,
                predicate: format!("{lhs_sql} {sym} {rhs_sql}"),
                prewhere: None,
            },
            eval: LeafEval::Arith {
                lhs: lhs_node,
                op,
                rhs: rhs_node,
            },
        });
    }

    // General case (genuinely cross-attribute `.a + .b`, mixed attr +
    // intrinsic, or a non-total `/ % ^` operator): resolve both operand
    // trees engine-side. Prune on the first referenced attribute key (an
    // index-served superset) when one exists; else the time-range superset.
    let generator = match first_attr_key(&lhs_node).or_else(|| first_attr_key(&rhs_node)) {
        Some((key, scope)) => key_existence_generator(&key, scope),
        None => LeafGenerator::time_range(),
    };
    Ok(CompiledLeaf {
        generator,
        eval: LeafEval::Arith {
            lhs: lhs_node,
            op,
            rhs: rhs_node,
        },
    })
}

/// The `val_num` column for an attribute operand (single-attribute
/// arithmetic pushdown, issue #185); non-attribute operands are not
/// pushable to the attr index.
fn val_num_col(operand: &CompareOperand) -> Option<&'static str> {
    match operand {
        CompareOperand::Attr { .. } => Some("val_num"),
        _ => None,
    }
}

/// The physical numeric column for an intrinsic operand (single-physical
/// arithmetic pushdown, issue #185); attributes and string intrinsics are
/// not pushable to the spans table.
fn physical_col(operand: &CompareOperand) -> Option<&'static str> {
    match operand {
        CompareOperand::Duration => Some("duration_ns"),
        CompareOperand::Status => Some("status_code"),
        CompareOperand::Kind => Some("kind"),
        CompareOperand::Name | CompareOperand::Service | CompareOperand::Attr { .. } => None,
    }
}

/// Renders a total (`+ - *`) arithmetic operand tree to a ClickHouse
/// expression, mapping field operands to columns via `col`. Returns `None`
/// if any operand is not mappable (falls back to the Rust evaluator).
fn render_arith_sql(
    node: &ArithNode,
    col: &impl Fn(&CompareOperand) -> Option<&'static str>,
) -> Option<String> {
    match node {
        ArithNode::Value(v) => Some(render_num(*v)),
        ArithNode::Operand(operand) => col(operand).map(str::to_string),
        ArithNode::Neg(inner) => render_arith_sql(inner, col).map(|s| format!("-({s})")),
        ArithNode::Bin { op, lhs, rhs } => {
            let sym = match op {
                ArithOp::Add => "+",
                ArithOp::Sub => "-",
                ArithOp::Mul => "*",
                // Non-total ops never reach here (guarded by `arith_is_total`).
                ArithOp::Div | ArithOp::Mod | ArithOp::Pow => return None,
            };
            let l = render_arith_sql(lhs, col)?;
            let r = render_arith_sql(rhs, col)?;
            Some(format!("({l} {sym} {r})"))
        }
    }
}

/// Collects the distinct attribute `(key, scope)` operands and flags
/// whether any physical numeric intrinsic (`duration`/`status`/`kind`) or
/// string operand (`name`/`resource.service.name`) is present in an
/// arithmetic operand tree (issue #185 pushdown classification).
fn analyze_arith(
    node: &ArithNode,
    attrs: &mut Vec<(String, Option<&'static str>)>,
    has_physical: &mut bool,
    has_string: &mut bool,
) {
    match node {
        ArithNode::Value(_) => {}
        ArithNode::Operand(operand) => match operand {
            CompareOperand::Attr { key, scope } => {
                let entry = (key.clone(), *scope);
                if !attrs.contains(&entry) {
                    attrs.push(entry);
                }
            }
            CompareOperand::Duration | CompareOperand::Status | CompareOperand::Kind => {
                *has_physical = true
            }
            CompareOperand::Name | CompareOperand::Service => *has_string = true,
        },
        ArithNode::Neg(inner) => analyze_arith(inner, attrs, has_physical, has_string),
        ArithNode::Bin { lhs, rhs, .. } => {
            analyze_arith(lhs, attrs, has_physical, has_string);
            analyze_arith(rhs, attrs, has_physical, has_string);
        }
    }
}

/// Whether every binary operator in the tree is total (`+ - *`) — safe to
/// render column-side (no division-by-zero / NaN that would diverge from
/// the Rust evaluator). `/ % ^` are not total and stay post-hydration.
fn arith_is_total(node: &ArithNode) -> bool {
    match node {
        ArithNode::Value(_) | ArithNode::Operand(_) => true,
        ArithNode::Neg(inner) => arith_is_total(inner),
        ArithNode::Bin { op, lhs, rhs } => {
            matches!(op, ArithOp::Add | ArithOp::Sub | ArithOp::Mul)
                && arith_is_total(lhs)
                && arith_is_total(rhs)
        }
    }
}

/// The first attribute `(key, scope)` referenced in an operand tree (for
/// the key-existence pruning generator).
fn first_attr_key(node: &ArithNode) -> Option<(String, Option<&'static str>)> {
    match node {
        ArithNode::Operand(CompareOperand::Attr { key, scope }) => Some((key.clone(), *scope)),
        ArithNode::Operand(_) | ArithNode::Value(_) => None,
        ArithNode::Neg(inner) => first_attr_key(inner),
        ArithNode::Bin { lhs, rhs, .. } => first_attr_key(lhs).or_else(|| first_attr_key(rhs)),
    }
}

/// Reflects a comparison operator across its operands (`a < b` ⇒ `b > a`)
/// so a folded `<scalar> <op> <attr>` becomes an `<attr>`-first numeric
/// leaf. Equality/inequality are symmetric.
fn flip_comparison(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::Gte => ComparisonOp::Lte,
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::Lte => ComparisonOp::Gte,
        other => other,
    }
}

/// `name`/`status`/`kind` generators: no selective index — a bounded
/// time-window span scan with the predicate applied (complete over the
/// window; the scan budget bounds its cost — docs/schemas.md §4.2).
fn spans_generator_for(physical: &PhysicalPredicate) -> LeafGenerator {
    LeafGenerator {
        class: GenClass::SpanScan,
        table: GenTable::Spans,
        predicate: physical_sql(physical),
        prewhere: None,
    }
}

/// Compiles every comparison of one `{...}` spanset filter in pre-order
/// (the deterministic traversal `search_eval` replays), plus the filter's
/// complete Phase-1 generator set — the shared compiler surface (T7
/// consumes this for its single-spanset metrics filters).
pub fn compile_span_filter(filter: &SpansetFilter) -> Result<CompiledSpanFilter, PlanError> {
    let mut leaves = Vec::new();
    let generators = match &filter.body {
        None => vec![LeafGenerator::time_range()],
        Some(body) => collect(body, &mut leaves)?,
    };
    Ok(CompiledSpanFilter { leaves, generators })
}

/// One compiled `{...}` spanset filter: its pre-order leaf classification
/// (Phase-2 evaluation order) and its complete generator set (Phase-1
/// candidate sources — a superset of the filter's matches by
/// construction).
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledSpanFilter {
    pub leaves: Vec<CompiledLeaf>,
    pub generators: Vec<LeafGenerator>,
}

/// Recursive completeness-preserving generator choice (issue #57 plan v3
/// + round-3 nested-boolean gap):
///
/// - a leaf contributes its own generator (negative leaves contribute the
///   time-range fallback);
/// - `a || b` needs **both** sides' sets (a match may satisfy either);
/// - `a && b` may use either side alone (matches(a && b) ⊆ matches(side))
///   — the statically better set wins: fewest worst-class generators,
///   then the smaller set, then lhs (byte-deterministic, never a runtime
///   probe).
fn collect(
    expr: &FieldExpr,
    leaves: &mut Vec<CompiledLeaf>,
) -> Result<Vec<LeafGenerator>, PlanError> {
    match expr {
        FieldExpr::Comparison { field, op, value } => {
            let leaf = compile_leaf(field, *op, value)?;
            let generator = leaf.generator.clone();
            leaves.push(leaf);
            Ok(vec![generator])
        }
        // Field-vs-field comparison (issue #183): one leaf, one generator
        // (the key-existence scan of an attribute operand — or the
        // time-range superset when both are physical intrinsics).
        FieldExpr::FieldCompare { lhs, op, rhs } => {
            let leaf = compile_field_compare(lhs, *op, rhs)?;
            let generator = leaf.generator.clone();
            leaves.push(leaf);
            Ok(vec![generator])
        }
        // Attribute existence (issue #185): one leaf, one index-served
        // key-only generator.
        FieldExpr::Exists(field) => {
            let leaf = compile_exists(field)?;
            let generator = leaf.generator.clone();
            leaves.push(leaf);
            Ok(vec![generator])
        }
        // An arithmetic comparison (issue #185): one leaf, one generator
        // (a numeric attr scan for the folded probe form, else the
        // key-existence / time-range superset).
        FieldExpr::ArithCompare { lhs, op, rhs } => {
            let leaf = compile_field_arith(lhs, *op, rhs)?;
            let generator = leaf.generator.clone();
            leaves.push(leaf);
            Ok(vec![generator])
        }
        // A bare boolean static (issue #183): no leaf and no positive
        // index — `{ true }`/`{ false }` are as broad as `{}` in Phase 1
        // (exactness is Phase-2's job), so the time-range superset covers
        // both. `{ false }` returns no spans in Phase 2 (never a match).
        FieldExpr::BoolStatic(_) => Ok(vec![LeafGenerator::time_range()]),
        // Unary field negation (issue #183 `logic.not`): the inner leaves
        // are compiled (Phase-2 alignment) but negation is not positively
        // indexable — the ratified `!=` rule generalizes — so the leaf
        // pairs with the time-range superset.
        FieldExpr::Not(inner) => {
            collect(inner, leaves)?;
            Ok(vec![LeafGenerator::time_range()])
        }
        FieldExpr::Binary { op, lhs, rhs } => {
            // Both sides are always compiled (leaf order is the pre-order
            // traversal Phase 2 replays), regardless of which side's
            // generators win an `&&` choice.
            let left = collect(lhs, leaves)?;
            let right = collect(rhs, leaves)?;
            match op {
                pulsus_traceql::BoolOp::Or => {
                    let mut all = left;
                    all.extend(right);
                    Ok(all)
                }
                pulsus_traceql::BoolOp::And => {
                    if gen_set_score(&right) < gen_set_score(&left) {
                        Ok(right)
                    } else {
                        Ok(left)
                    }
                }
            }
        }
    }
}

/// Static score for a generator set: (worst class present, set size).
/// Lower is better; ties keep the lhs.
fn gen_set_score(set: &[LeafGenerator]) -> (GenClass, usize) {
    let worst = set
        .iter()
        .map(|g| g.class)
        .max()
        .unwrap_or(GenClass::TimeRange);
    (worst, set.len())
}

#[cfg(test)]
mod tests {
    use pulsus_traceql::parse;

    use super::*;

    fn first_filter(q: &str) -> SpansetFilter {
        match parse(q).expect("parse").spanset {
            pulsus_traceql::SpansetExpr::Filter(f) => f,
            other => panic!("expected a single spanset filter, got {other:?}"),
        }
    }

    #[test]
    fn service_equality_compiles_to_the_projection_prewhere_fast_path() {
        let f = first_filter(r#"{ resource.service.name = "checkout" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators.len(), 1);
        let generator = &compiled.generators[0];
        assert_eq!(generator.class, GenClass::ServiceEq);
        assert_eq!(generator.prewhere.as_deref(), Some("service = 'checkout'"));
        assert!(matches!(
            &compiled.leaves[0].eval,
            LeafEval::Physical(PhysicalPredicate::Service { .. })
        ));
    }

    #[test]
    fn positive_service_regex_uses_the_indexed_attr_generator_not_the_fallback() {
        let f = first_filter(r#"{ resource.service.name =~ "check.*" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        let generator = &compiled.generators[0];
        assert_eq!(generator.class, GenClass::AttrKeyScan);
        assert_eq!(generator.table, GenTable::Attrs);
        assert!(generator.predicate.contains("key = 'service.name'"));
        assert!(generator.predicate.contains("scope = 'resource'"));
        assert!(generator.predicate.contains("match(val, '^(?:check.*)$')"));
    }

    #[test]
    fn unscoped_service_name_goes_through_the_attr_index_not_the_physical_column() {
        let f = first_filter(r#"{ .service.name = "checkout" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::AttrEq);
        match &compiled.leaves[0].eval {
            LeafEval::Attr { probe, negated } => {
                assert_eq!(probe.key, "service.name");
                assert_eq!(probe.scope, None);
                assert!(!negated);
            }
            other => panic!("expected an attr eval, got {other:?}"),
        }
    }

    #[test]
    fn negated_attr_compiles_to_the_time_range_fallback_with_a_positive_probe() {
        let f = first_filter(r#"{ .env != "prod" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::TimeRange);
        match &compiled.leaves[0].eval {
            LeafEval::Attr { probe, negated } => {
                assert!(*negated);
                assert_eq!(probe.pred, ValuePred::StringEq("prod".to_string()));
            }
            other => panic!("expected an attr eval, got {other:?}"),
        }
    }

    #[test]
    fn numeric_attr_comparison_is_a_key_only_val_num_scan() {
        let f = first_filter("{ span.http.status_code >= 500 }");
        let compiled = compile_span_filter(&f).unwrap();
        let generator = &compiled.generators[0];
        assert_eq!(generator.class, GenClass::AttrKeyScan);
        assert_eq!(
            generator.predicate,
            "key = 'http.status_code' AND val_num >= 500 AND scope = 'span'"
        );
    }

    #[test]
    fn bool_attr_equality_renders_the_writers_true_false_strings() {
        let f = first_filter("{ span.retryable = true }");
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::AttrEq);
        assert!(compiled.generators[0].predicate.contains("val = 'true'"));
    }

    #[test]
    fn duration_leaf_uses_the_duration_generator_class() {
        let f = first_filter("{ duration > 2s }");
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::Duration);
        assert_eq!(compiled.generators[0].predicate, "duration_ns > 2000000000");
    }

    #[test]
    fn status_and_kind_lower_to_the_otel_wire_codes() {
        let f = first_filter("{ status = error }");
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators[0].predicate, "status_code = 2");

        let f = first_filter("{ kind = server }");
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators[0].predicate, "kind = 2");
    }

    #[test]
    fn a_conjunction_picks_the_statically_most_selective_side() {
        // ServiceEq (1) beats AttrKeyScan (2) beats Duration (3).
        let f = first_filter(
            r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }"#,
        );
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators.len(), 1);
        assert_eq!(compiled.generators[0].class, GenClass::ServiceEq);
        // All three leaves are still compiled for Phase-2 evaluation.
        assert_eq!(compiled.leaves.len(), 3);
    }

    #[test]
    fn a_disjunction_keeps_both_sides_generators() {
        let f = first_filter(r#"{ duration > 2s || span.foo = "x" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        let classes: Vec<GenClass> = compiled.generators.iter().map(|g| g.class).collect();
        assert_eq!(classes, vec![GenClass::Duration, GenClass::AttrEq]);
    }

    #[test]
    fn nested_boolean_structure_keeps_a_complete_generator_set() {
        // (A || B) && (C || D): either OR-set is complete for the
        // conjunction; both sets tie on (class, size) so the lhs wins.
        let f = first_filter(r#"{ (.a = "1" || .b = "2") && (.c = "3" || .d = "4") }"#);
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators.len(), 2);
        assert!(compiled.generators[0].predicate.contains("key = 'a'"));
        assert!(compiled.generators[1].predicate.contains("key = 'b'"));
        assert_eq!(compiled.leaves.len(), 4, "all leaves compile for Phase 2");
    }

    #[test]
    fn a_disjunct_with_only_negated_leaves_degrades_to_the_time_range_generator() {
        let f = first_filter(r#"{ .env != "prod" || .region !~ "us-.*" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        assert!(
            compiled
                .generators
                .iter()
                .all(|g| g.class == GenClass::TimeRange)
        );
    }

    #[test]
    fn match_all_compiles_to_the_time_range_generator() {
        let f = first_filter("{}");
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators, vec![LeafGenerator::time_range()]);
        assert!(compiled.leaves.is_empty());
    }

    #[test]
    fn ordering_operators_on_string_fields_are_type_mismatches() {
        let f = first_filter(r#"{ name > "x" }"#);
        assert!(matches!(
            compile_span_filter(&f),
            Err(PlanError::TypeMismatch(_))
        ));
        let f = first_filter(r#"{ resource.service.name < "x" }"#);
        assert!(matches!(
            compile_span_filter(&f),
            Err(PlanError::TypeMismatch(_))
        ));
    }

    #[test]
    fn regex_on_duration_is_a_type_mismatch() {
        // The parser types duration values (a `duration =~` never parses),
        // but `compile_leaf` is a public API taking any AST — extract a
        // parsed `Duration` value and hand it back with a regex operator.
        let f = first_filter("{ duration > 1s }");
        let Some(FieldExpr::Comparison { value, .. }) = f.body else {
            panic!("expected a duration comparison");
        };
        let err = compile_leaf(
            &Field::Intrinsic(Intrinsic::Duration),
            ComparisonOp::Re,
            &value,
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)));
    }

    #[test]
    fn nested_set_root_compiles_to_the_time_range_generator_and_a_nested_set_eval() {
        for (q, field) in [
            ("{ nestedSetParent < 0 }", NestedSetField::Parent),
            ("{ nestedSetLeft > 0 }", NestedSetField::Left),
            ("{ nestedSetRight >= 1 }", NestedSetField::Right),
        ] {
            let f = first_filter(q);
            let compiled = compile_span_filter(&f).unwrap();
            assert_eq!(
                compiled.generators,
                vec![LeafGenerator::time_range()],
                "{q}"
            );
            match &compiled.leaves[0].eval {
                LeafEval::NestedSet {
                    field: got, value, ..
                } => {
                    assert_eq!(*got, field, "{q}");
                    assert!(value.is_finite());
                }
                other => panic!("{q}: expected a nested-set eval, got {other:?}"),
            }
        }
    }

    #[test]
    fn regex_on_a_nested_set_intrinsic_is_a_type_mismatch() {
        // The parser rejects `nestedSetLeft =~ "x"` (string not a number),
        // but `compile_leaf` is a public API over any AST — feed it a
        // number value with a regex operator.
        let err = compile_leaf(
            &Field::Intrinsic(Intrinsic::NestedSetLeft),
            ComparisonOp::Re,
            &Value::Number("5".to_string()),
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)));
    }

    // -- issue #184: trace-level / colon-scoped intrinsic leaves ---------

    #[test]
    fn status_message_compiles_to_a_span_scan_on_the_capped_status_message_column() {
        // Issue #184 code review: the Phase-1 predicate compares the
        // CAPPED column (via the shared `search_sql::byte_cap_expr`
        // helper, its single source of truth), so candidate selection
        // agrees with the capped value Phase 2 hydrates and evaluates —
        // a raw comparison would silently drop an over-cap message whose
        // capped rendering equals the literal.
        let f = first_filter(r#"{ statusMessage = "boom" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        let generator = &compiled.generators[0];
        assert_eq!(generator.class, GenClass::SpanScan);
        assert_eq!(
            generator.predicate,
            "if(length(status_message) <= 8192, status_message, \
             substringUTF8(status_message, 1, 2048)) = 'boom'"
        );
        assert_eq!(
            generator.predicate,
            format!("{} = 'boom'", byte_cap_expr("status_message")),
            "the predicate is built from the shared cap helper"
        );
        // The regex form wraps the SAME capped expression.
        let re = compile_span_filter(&first_filter(r#"{ statusMessage =~ "bo.*" }"#)).unwrap();
        assert_eq!(
            re.generators[0].predicate,
            format!("match({}, '^(?:bo.*)$')", byte_cap_expr("status_message"))
        );
        assert!(matches!(
            &compiled.leaves[0].eval,
            LeafEval::Physical(PhysicalPredicate::StatusMessage { .. })
        ));
        // The scoped spelling compiles identically.
        let scoped = compile_span_filter(&first_filter(r#"{ span:statusMessage = "boom" }"#));
        assert_eq!(scoped.unwrap().generators[0].predicate, generator.predicate);
    }

    #[test]
    fn span_id_leaf_lowercases_hex_for_equality_and_keeps_regex_raw() {
        let f = first_filter(r#"{ span:id = "0A1B2C3D4E5F6071" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(
            compiled.generators[0].predicate,
            "lower(hex(span_id)) = '0a1b2c3d4e5f6071'"
        );
        match &compiled.leaves[0].eval {
            LeafEval::Physical(PhysicalPredicate::SpanIdHex { op, value }) => {
                assert_eq!(*op, ComparisonOp::Eq);
                assert_eq!(value, "0a1b2c3d4e5f6071", "Eq value stored lowercased");
            }
            other => panic!("expected a span-id hex eval, got {other:?}"),
        }
        let f = first_filter(r#"{ span:parentID =~ "0a.*" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(
            compiled.generators[0].predicate,
            "match(lower(hex(parent_id)), '^(?:0a.*)$')"
        );
    }

    #[test]
    fn trace_id_equality_renders_the_pk_prefix_unhex_predicate() {
        let f = first_filter(r#"{ trace:id = "000102030405060708090A0B0C0D0E0F" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(
            compiled.generators[0].predicate,
            "trace_id = unhex('000102030405060708090a0b0c0d0e0f')"
        );
        assert!(matches!(
            &compiled.leaves[0].eval,
            LeafEval::TraceCtx(TraceCtxPred::TraceId { .. })
        ));
    }

    #[test]
    fn root_leaves_pair_with_the_time_range_superset_for_every_operator() {
        // Plan v2 §Performance: a WINDOWED root-span scan would miss any
        // trace whose true root predates the search window (the exact
        // window-spanning case the co-load exists for), so every operator
        // takes the complete time-range superset; exactness lives in the
        // Phase-2 co-load evaluation.
        for q in [
            r#"{ rootServiceName = "gw" }"#,
            r#"{ rootServiceName =~ "gw.*" }"#,
            r#"{ rootName = "GET /" }"#,
            r#"{ rootName != "GET /" }"#,
            r#"{ rootName !~ "GET.*" }"#,
        ] {
            let f = first_filter(q);
            let compiled = compile_span_filter(&f).unwrap();
            assert_eq!(
                compiled.generators,
                vec![LeafGenerator::time_range()],
                "{q}"
            );
            assert!(
                matches!(
                    &compiled.leaves[0].eval,
                    LeafEval::TraceCtx(
                        TraceCtxPred::RootName { .. } | TraceCtxPred::RootServiceName { .. }
                    )
                ),
                "{q}"
            );
        }
    }

    #[test]
    fn trace_duration_and_child_count_pair_with_the_time_range_generator() {
        let f = first_filter("{ traceDuration > 2s }");
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators, vec![LeafGenerator::time_range()]);
        match &compiled.leaves[0].eval {
            LeafEval::TraceCtx(TraceCtxPred::TraceDurationNs { op, nanos }) => {
                assert_eq!(*op, ComparisonOp::Gt);
                assert_eq!(*nanos, 2_000_000_000);
            }
            other => panic!("expected a trace-duration eval, got {other:?}"),
        }
        let f = first_filter("{ span:childCount >= 3 }");
        let compiled = compile_span_filter(&f).unwrap();
        assert_eq!(compiled.generators, vec![LeafGenerator::time_range()]);
        assert!(matches!(
            &compiled.leaves[0].eval,
            LeafEval::TraceCtx(TraceCtxPred::ChildCount { .. })
        ));
    }

    #[test]
    fn regex_or_wrong_value_types_on_the_new_intrinsics_are_type_mismatches() {
        // The parser rejects these spellings itself; `compile_leaf` is a
        // public API over any AST, so the guards are exercised directly.
        for (field, op, value) in [
            (
                Field::Intrinsic(Intrinsic::TraceDuration),
                ComparisonOp::Re,
                Value::String("x".to_string()),
            ),
            (
                Field::Intrinsic(Intrinsic::ChildCount),
                ComparisonOp::Re,
                Value::Number("5".to_string()),
            ),
            (
                Field::Intrinsic(Intrinsic::ChildCount),
                ComparisonOp::Gt,
                Value::String("5".to_string()),
            ),
            (
                Field::Intrinsic(Intrinsic::TraceDuration),
                ComparisonOp::Gt,
                Value::Number("5".to_string()),
            ),
            (
                Field::Intrinsic(Intrinsic::StatusMessage),
                ComparisonOp::Gt,
                Value::String("boom".to_string()),
            ),
            (
                Field::Intrinsic(Intrinsic::SpanId),
                ComparisonOp::Lt,
                Value::String("0a".to_string()),
            ),
            (
                Field::Intrinsic(Intrinsic::RootName),
                ComparisonOp::Gte,
                Value::String("x".to_string()),
            ),
        ] {
            let err = compile_leaf(&field, op, &value).unwrap_err();
            assert!(
                matches!(err, PlanError::TypeMismatch(_)),
                "{field:?} {op:?} must be a type mismatch"
            );
        }
    }

    #[test]
    fn new_intrinsics_are_rejected_in_field_vs_field_comparisons() {
        for intrinsic in [
            Intrinsic::StatusMessage,
            Intrinsic::ChildCount,
            Intrinsic::SpanId,
            Intrinsic::ParentId,
            Intrinsic::TraceId,
            Intrinsic::TraceDuration,
            Intrinsic::RootName,
            Intrinsic::RootServiceName,
        ] {
            let err = compile_field_compare(
                &Field::Intrinsic(intrinsic),
                ComparisonOp::Eq,
                &Field::Intrinsic(Intrinsic::Name),
            )
            .unwrap_err();
            assert!(matches!(err, PlanError::TypeMismatch(_)), "{intrinsic:?}");
        }
    }

    // -- issue #185: existence + arithmetic --------------------------------

    #[test]
    fn attribute_existence_compiles_to_a_key_only_index_scan() {
        // `.a != nil` / bare `.a` — present ⇒ key-only AttrKeyScan.
        for q in [r#"{ .a != nil }"#, r#"{ .a }"#] {
            let compiled = compile_span_filter(&first_filter(q)).unwrap();
            assert_eq!(compiled.generators[0].class, GenClass::AttrKeyScan);
            assert_eq!(compiled.generators[0].predicate, "key = 'a' AND 1");
            match &compiled.leaves[0].eval {
                LeafEval::Attr { probe, negated } => {
                    assert_eq!(probe.pred, ValuePred::KeyExists);
                    assert!(!negated, "{q}");
                }
                other => panic!("{q}: expected an attr existence eval, got {other:?}"),
            }
        }
    }

    #[test]
    fn absent_attribute_existence_is_the_negated_time_range_form() {
        // `.a = nil` ⇒ absence ⇒ `Not(Exists)`: the inner existence leaf is
        // a positive key-existence probe (the `Not` negates it at eval
        // time), and negation forces the time-range superset generator.
        let compiled = compile_span_filter(&first_filter(r#"{ .a = nil }"#)).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::TimeRange);
        match &compiled.leaves[0].eval {
            LeafEval::Attr { probe, negated } => {
                assert_eq!(probe.pred, ValuePred::KeyExists);
                assert!(!negated, "the inner existence probe stays positive");
            }
            other => panic!("expected an attr existence eval, got {other:?}"),
        }
    }

    #[test]
    fn scoped_service_name_existence_uses_the_resource_scoped_index() {
        let compiled =
            compile_span_filter(&first_filter(r#"{ resource.service.name != nil }"#)).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::AttrKeyScan);
        assert_eq!(
            compiled.generators[0].predicate,
            "key = 'service.name' AND 1 AND scope = 'resource'"
        );
    }

    #[test]
    fn intrinsic_existence_is_a_type_mismatch() {
        let err = compile_exists(&Field::Intrinsic(Intrinsic::Name)).unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)));
    }

    #[test]
    fn all_literal_arithmetic_folds_to_a_numeric_attr_leaf() {
        // `{ .a = 1 + 2 }` ≡ `{ .a = 3 }`: a folded numeric attr leaf with
        // the `val_num = 3` key-scan pushdown of a plain numeric comparison.
        for (q, sql) in [
            (r#"{ .a = 1 + 2 }"#, "key = 'a' AND val_num = 3"),
            (r#"{ .a = 2 * 3 }"#, "key = 'a' AND val_num = 6"),
            (r#"{ .a = 5 % 2 }"#, "key = 'a' AND val_num = 1"),
            (r#"{ .a = 2 ^ 3 }"#, "key = 'a' AND val_num = 8"),
            (r#"{ .a = -1 }"#, "key = 'a' AND val_num = -1"),
        ] {
            let compiled = compile_span_filter(&first_filter(q)).unwrap();
            assert_eq!(compiled.generators[0].class, GenClass::AttrKeyScan, "{q}");
            assert_eq!(compiled.generators[0].predicate, sql, "{q}");
        }
    }

    #[test]
    fn single_attribute_arithmetic_pushes_to_a_val_num_column_predicate() {
        // `{ .duration_ms * 1000 > 5000 }` (issue #185): ONE attr with
        // literal coefficients → a column-side `val_num` predicate on the
        // attr index (index-served, no per-row client work), NOT a Rust
        // post-hydration Arith leaf.
        let compiled =
            compile_span_filter(&first_filter(r#"{ .duration_ms * 1000 > 5000 }"#)).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::AttrKeyScan);
        assert_eq!(
            compiled.generators[0].predicate,
            "key = 'duration_ms' AND (val_num * 1000) > 5000"
        );
        match &compiled.leaves[0].eval {
            LeafEval::Attr { probe, negated } => {
                assert_eq!(
                    probe.pred,
                    ValuePred::NumExpr("(val_num * 1000) > 5000".to_string())
                );
                assert!(!negated);
            }
            other => panic!("expected a pushed val_num attr leaf, got {other:?}"),
        }
        // `!=` keeps the absent-key rule: positive `=` probe, negated over
        // the time-range superset.
        let neq = compile_span_filter(&first_filter(r#"{ .duration_ms * 1000 != 5000 }"#)).unwrap();
        assert_eq!(neq.generators[0].class, GenClass::TimeRange);
        match &neq.leaves[0].eval {
            LeafEval::Attr { probe, negated } => {
                assert_eq!(
                    probe.pred,
                    ValuePred::NumExpr("(val_num * 1000) = 5000".to_string())
                );
                assert!(*negated);
            }
            other => panic!("expected a negated val_num attr leaf, got {other:?}"),
        }
    }

    #[test]
    fn single_physical_intrinsic_arithmetic_pushes_to_a_span_scan_predicate() {
        // `{ duration * 2 > 1s }` (issue #185): one physical intrinsic with
        // literal coefficients → a column-side `SpanScan` predicate that
        // prunes candidates; Phase 2 confirms the same arithmetic in Rust.
        let compiled = compile_span_filter(&first_filter(r#"{ duration * 2 > 1s }"#)).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::SpanScan);
        assert_eq!(
            compiled.generators[0].predicate,
            "(duration_ns * 2) > 1000000000"
        );
        match &compiled.leaves[0].eval {
            LeafEval::Arith { op, .. } => assert_eq!(*op, ComparisonOp::Gt),
            other => panic!("expected an Arith leaf, got {other:?}"),
        }
    }

    #[test]
    fn non_total_division_arithmetic_stays_post_hydration() {
        // `{ .a / 2 > 5 }`: division can divide by zero (Rust ⇒ no match),
        // so it is NOT pushed column-side — it stays an engine-side Arith
        // leaf pruning on the attr key.
        let compiled = compile_span_filter(&first_filter(r#"{ .a / 2 > 5 }"#)).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::AttrKeyScan);
        assert!(compiled.generators[0].predicate.contains("key = 'a'"));
        assert!(matches!(compiled.leaves[0].eval, LeafEval::Arith { .. }));
    }

    #[test]
    fn cross_attribute_arithmetic_prunes_on_a_referenced_attribute_key() {
        // `{ .a * 2 = span.b }`: two distinct attributes cannot resolve to a
        // single attr-index row, so it stays a Rust Arith leaf pruning on
        // the first attribute key (an index-served superset).
        let compiled = compile_span_filter(&first_filter(r#"{ .a * 2 = span.b }"#)).unwrap();
        assert_eq!(compiled.generators[0].class, GenClass::AttrKeyScan);
        assert!(compiled.generators[0].predicate.contains("key = 'a'"));
        assert!(matches!(compiled.leaves[0].eval, LeafEval::Arith { .. }));
    }

    #[test]
    fn injection_attempt_in_an_attr_value_is_neutralized() {
        let f = first_filter(r#"{ .k = "x'; DROP TABLE trace_spans; --" }"#);
        let compiled = compile_span_filter(&f).unwrap();
        let sql = &compiled.generators[0].predicate;
        assert!(
            sql.contains(r"val = 'x\'; DROP TABLE trace_spans; --'"),
            "quote must be escaped, got {sql}"
        );
    }
}
