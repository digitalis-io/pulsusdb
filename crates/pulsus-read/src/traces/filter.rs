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
    AttrScope, ComparisonOp, Field, FieldExpr, Intrinsic, SpanKindValue, SpansetFilter,
    StatusValue, Value,
};

use crate::logql::escape;

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

/// How Phase 2 evaluates one leaf.
#[derive(Debug, Clone, PartialEq)]
pub enum LeafEval {
    Physical(PhysicalPredicate),
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
    }
}

fn attr_scope_literal(scope: AttrScope) -> Option<&'static str> {
    match scope {
        AttrScope::Span => Some("span"),
        AttrScope::Resource => Some("resource"),
        AttrScope::Unscoped => None,
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
