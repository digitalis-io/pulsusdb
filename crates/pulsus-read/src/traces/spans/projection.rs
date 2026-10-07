//! The matched-span projection of the search statement (issue #591 part
//! 1): which fields a query projects onto the spans it matched, the SQL
//! that fills them in the detail read, and their decode into the
//! response's `name` and `attributes` (`docs/api.md` §4.2, "the
//! matched-span projection").
//!
//! **One group per distinct field**, by scope and key, in the order the
//! fields first appear across the filter bodies in pre-order. Each
//! condition that projects its field adds a **source** to that field's
//! group: the condition itself as the gate, and the value it projects. A
//! span takes, per group, the value of the first source whose gate holds
//! for it, so a field is projected only when a condition naming it matched
//! THAT span.
//!
//! **A value is read where its condition reads it**: an intrinsic's
//! column, or the typed `JSON` subcolumn the predicate compiler reads
//! (`attrs.`, `scope_attrs.`). Off the span row (issue #591 part 2): a
//! resource attribute other than `service.name` through the predicate's
//! own resource lookup at the span's `resource_id`; an event or link
//! attribute, or an event or link intrinsic, as the first element whose own
//! condition holds, or — once inside arithmetic — the first element the
//! comparison holds for; the unscoped `.k` at the first scope of the
//! predicate's chain that holds it. A set field compared with itself is
//! its first element carrying the field. One set field twice in a
//! comparison holding arithmetic has no single element and is refused.
//!
//! **Every value's text is cut at today's response ceiling**: more than
//! 8,192 bytes becomes its first 2,048 code points. An array's JSON is
//! rendered in the decode, so the decode cuts it by the same rule.

use pulsus_traceql::{
    AttrScope, ComparisonOp, Field, FieldExpr, FieldOp, Intrinsic, UnaryOp, Value,
};

use super::predicate::{
    CHAIN, PredicateCtx, attr_path, chain_presence_in, compile_span_predicate_in, element_index_in,
    element_select_in, resource_value_in,
};
use super::rows::SearchProjected;
use crate::logql::escape;
use crate::traces::PlanError;
use crate::traces::search_eval::{GroupValue, ProjectedAttribute};

/// The most groups one search projects: the group index travels as a
/// `UInt8`, and `0` is "no value".
pub const MAX_PROJECTION_GROUPS: usize = 255;

/// The response-string ceiling, in bytes, and what a longer value is cut
/// to, in code points (`docs/api.md:1027`).
const CEILING_BYTES: usize = 8192;
const CUT_CODE_POINTS: usize = 2048;

/// The projection a search statement fills and decodes.
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    groups: Vec<Group>,
}

/// One projected field.
#[derive(Debug, Clone, PartialEq)]
struct Group {
    field: Field,
    target: Target,
    sources: Vec<Source>,
}

/// Where a group's value goes in the response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// The span's own `name`.
    Name,
    /// One of the span's `attributes`.
    Attribute,
}

/// One condition that projects its field: its gate, the value's SQL and
/// the SQL of the kind it decodes as.
#[derive(Debug, Clone, PartialEq)]
struct Source {
    gate: String,
    value: String,
    kind: String,
}

/// `T(x)`: today's response-string ceiling, in SQL.
pub(super) fn ceiling_sql(x: &str) -> String {
    format!("if(length({x}) <= {CEILING_BYTES}, {x}, substringUTF8({x}, 1, {CUT_CODE_POINTS}))")
}

/// `T(x)`, in the decode.
fn ceiling(text: String) -> String {
    if text.len() <= CEILING_BYTES {
        text
    } else {
        text.chars().take(CUT_CODE_POINTS).collect()
    }
}

/// What a condition projects for its one field.
enum Projects {
    /// Nothing: the condition supplies no value. That is an attribute
    /// compared with `!=` or `!~` (a span-row column projects itself under
    /// any comparison), any of the envelope's own fields, and a condition
    /// that holds on no span.
    Nothing,
    /// A value's SQL, already cut, and its kind's SQL.
    Value { value: String, kind: String },
}

impl Projection {
    /// The projection of a query that projects nothing.
    pub fn none() -> Projection {
        Projection { groups: Vec::new() }
    }

    /// The SQL the detail read projects per span: an array of
    /// `(group, value, kind)`, one element per group whose condition held.
    pub fn sql(&self) -> String {
        if self.groups.is_empty() {
            return "CAST([], 'Array(Tuple(UInt8, String, String))')".to_string();
        }
        let groups: Vec<String> = self
            .groups
            .iter()
            .enumerate()
            .map(|(i, group)| {
                let g = i + 1;
                let mut args: Vec<String> = Vec::new();
                for source in &group.sources {
                    args.push(source.gate.clone());
                    args.push(format!("({g}, {}, {})", source.value, source.kind));
                }
                args.push("(0, '', '')".to_string());
                format!("multiIf({})", args.join(", "))
            })
            .collect();
        format!("arrayFilter(x -> x.1 != 0, [{}])", groups.join(", "))
    }

    /// The projection of `bodies`, the query's filter bodies in pre-order,
    /// its gates compiled in `ctx`.
    pub(crate) fn of_filters(
        bodies: &[&FieldExpr],
        ctx: &PredicateCtx<'_>,
    ) -> Result<Projection, PlanError> {
        let mut projection = Projection::none();
        for body in bodies {
            projection.walk(body, false, ctx)?;
        }
        if projection.groups.len() > MAX_PROJECTION_GROUPS {
            return Err(PlanError::UnsupportedField(format!(
                "a search projecting more than {MAX_PROJECTION_GROUPS} distinct fields is not \
                 supported"
            )));
        }
        Ok(projection)
    }

    /// Today's `collect_projection_leaves` walk (`search_plan.rs`), adding
    /// each projecting condition as a source. `neg` is whether an odd
    /// number of `!` sits above the node.
    fn walk(
        &mut self,
        expr: &FieldExpr,
        neg: bool,
        ctx: &PredicateCtx<'_>,
    ) -> Result<(), PlanError> {
        match expr {
            FieldExpr::Field(field) => {
                if !neg {
                    self.add(field, truthiness_value(field), expr, ctx)?;
                }
                Ok(())
            }
            FieldExpr::Literal(_) => Ok(()),
            FieldExpr::Exists { field, negated } => {
                if neg == *negated {
                    // Presence, however it is spelled: the gate is the
                    // presence itself, so the value is projected only
                    // where the field is held.
                    let presence = FieldExpr::Exists {
                        field: field.clone(),
                        negated: false,
                    };
                    self.add(field, stored_value(field, ctx)?, &presence, ctx)?;
                }
                Ok(())
            }
            FieldExpr::Unary {
                op: UnaryOp::Not,
                expr: inner,
            } => match inner.as_ref() {
                FieldExpr::Field(_) => Ok(()),
                other => self.walk(other, !neg, ctx),
            },
            FieldExpr::Unary {
                op: UnaryOp::Neg,
                expr: inner,
            } => self.walk(inner, neg, ctx),
            FieldExpr::Binary {
                op: FieldOp::Cmp(op),
                lhs,
                rhs,
            } => {
                if neg {
                    return Ok(());
                }
                let Some(field) = single_field_of_comparison(lhs, rhs) else {
                    return Ok(());
                };
                let projects = comparison_value(field, *op, lhs, rhs, expr, ctx)?;
                self.add(field, projects, expr, ctx)
            }
            FieldExpr::Binary { lhs, rhs, .. } => {
                self.walk(lhs, neg, ctx)?;
                self.walk(rhs, neg, ctx)
            }
        }
    }

    /// Adds one source to `field`'s group, creating the group on its first
    /// source.
    fn add(
        &mut self,
        field: &Field,
        projects: Projects,
        gate: &FieldExpr,
        ctx: &PredicateCtx<'_>,
    ) -> Result<(), PlanError> {
        let (value, kind) = match projects {
            Projects::Nothing => return Ok(()),
            Projects::Value { value, kind } => (value, kind),
        };
        let gate = compile_span_predicate_in(gate, ctx)?.sql().to_string();
        let source = Source { gate, value, kind };
        if let Some(group) = self.groups.iter_mut().find(|g| g.field == *field) {
            group.sources.push(source);
            return Ok(());
        }
        let target = if *field == Field::Intrinsic(Intrinsic::Name) {
            Target::Name
        } else {
            Target::Attribute
        };
        self.groups.push(Group {
            field: field.clone(),
            target,
            sources: vec![source],
        });
        Ok(())
    }

    /// One span's projected values, decoded: its `name` when a `name`
    /// group projected one, and its attributes in group order. A value that
    /// does not parse as its kind is an error, never a default.
    pub(crate) fn decode(
        &self,
        projected: &[SearchProjected],
    ) -> Result<(Option<String>, Vec<ProjectedAttribute>), String> {
        let mut name = None;
        let mut attributes = Vec::new();
        for p in projected {
            let group = usize::from(p.group)
                .checked_sub(1)
                .and_then(|i| self.groups.get(i))
                .ok_or_else(|| format!("projected group {} is not in the projection", p.group))?;
            match group.target {
                Target::Name => name = Some(p.value.clone()),
                Target::Attribute => {
                    let value = typed_value(&p.value, &p.kind)?;
                    attributes.push(ProjectedAttribute::new(&group.field, value));
                }
            }
        }
        Ok((name, attributes))
    }
}

/// A projected attribute's value, by the kind the statement sent with it.
fn typed_value(value: &str, kind: &str) -> Result<GroupValue, String> {
    Ok(match kind {
        "String" => GroupValue::Str(value.to_string()),
        "Int64" => GroupValue::Int(
            value
                .parse::<i64>()
                .map_err(|e| format!("projected Int64 {value:?}: {e}"))?,
        ),
        "Float64" => GroupValue::Double(
            value
                .parse::<f64>()
                .map_err(|e| format!("projected Float64 {value:?}: {e}"))?
                .to_bits(),
        ),
        "Bool" => GroupValue::Bool(value == "true"),
        k if k.starts_with("Array(") => GroupValue::Str(ceiling(array_json(value)?)),
        _ => GroupValue::Str(value.to_string()),
    })
}

/// An array's JSON as the writer stores it, from the statement's
/// `[[type, text], …]`: each element rendered by its own type.
fn array_json(value: &str) -> Result<String, String> {
    serde_json::to_string(&array_values(value)?).map_err(|e| e.to_string())
}

/// The length of `ceiling(serde_json::to_string(values))`, learned
/// without building it (issue #591 part 3). TESTS-FIRST STUB.
fn rendered_len(_values: &[serde_json::Value]) -> usize {
    0
}

/// An array's elements, each by its own type, from the statement's
/// `[[type, text], …]`.
fn array_values(value: &str) -> Result<Vec<serde_json::Value>, String> {
    let elements: Vec<(String, String)> =
        serde_json::from_str(value).map_err(|e| format!("projected array {value:?}: {e}"))?;
    let mut rendered: Vec<serde_json::Value> = Vec::with_capacity(elements.len());
    for (t, text) in elements {
        rendered.push(match t.as_str() {
            "String" => serde_json::Value::String(text),
            "Int64" => serde_json::Value::from(
                text.parse::<i64>()
                    .map_err(|e| format!("array element Int64 {text:?}: {e}"))?,
            ),
            "Float64" => text
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            "Bool" => serde_json::Value::Bool(text == "true"),
            _ => serde_json::Value::Null,
        });
    }
    Ok(rendered)
}

/// Every `Field` a comparison's operand tree names, in source order.
fn operand_fields<'a>(expr: &'a FieldExpr, out: &mut Vec<&'a Field>) {
    match expr {
        FieldExpr::Field(f) => out.push(f),
        FieldExpr::Exists { field, .. } => out.push(field),
        FieldExpr::Literal(_) => {}
        FieldExpr::Unary { expr, .. } => operand_fields(expr, out),
        FieldExpr::Binary { lhs, rhs, .. } => {
            operand_fields(lhs, out);
            operand_fields(rhs, out);
        }
    }
}

/// The field a comparison names, when it names exactly ONE distinct field
/// across both operands (`docs/api.md:1023`; today's
/// `single_field_of_comparison`).
fn single_field_of_comparison<'a>(lhs: &'a FieldExpr, rhs: &'a FieldExpr) -> Option<&'a Field> {
    let mut fields = Vec::new();
    operand_fields(lhs, &mut fields);
    operand_fields(rhs, &mut fields);
    let first = *fields.first()?;
    fields.iter().all(|f| *f == first).then_some(first)
}

/// Where a field's value lives, for the projection.
enum Place {
    /// A span-row column: the value's SQL.
    Column(&'static str),
    /// A span or instrumentation attribute: its `JSON` root.
    Attribute(&'static str),
    /// The response envelope carries it (`duration`, `span:id`,
    /// `trace:id`), or it is #594's and refused before this.
    Envelope,
    /// A resource attribute other than `service.name`: the resource row's.
    Resource,
    /// A set: an `event.` or `link.` attribute, the unscoped `.k`, or an
    /// event or link intrinsic.
    Set,
}

/// **No wildcard arm**: a new scope or intrinsic decides here.
fn place_of(field: &Field) -> Place {
    match field {
        Field::Attribute { scope, key } => match scope {
            AttrScope::Span => Place::Attribute("attrs"),
            AttrScope::Instrumentation => Place::Attribute("scope_attrs"),
            AttrScope::Resource if key == "service.name" => Place::Column("toString(service)"),
            AttrScope::Resource => Place::Resource,
            AttrScope::Event | AttrScope::Link | AttrScope::Unscoped => Place::Set,
        },
        Field::Intrinsic(intrinsic) => match intrinsic {
            Intrinsic::Name => Place::Column("toString(name)"),
            Intrinsic::Status => {
                Place::Column("multiIf(status_code = 1, 'ok', status_code = 2, 'error', 'unset')")
            }
            Intrinsic::Kind => Place::Column(
                "multiIf(kind = 1, 'internal', kind = 2, 'server', kind = 3, 'client', kind = 4, \
                 'producer', kind = 5, 'consumer', 'unspecified')",
            ),
            Intrinsic::StatusMessage => Place::Column("status_message"),
            Intrinsic::ParentId => Place::Column("lower(hex(parent_span_id))"),
            Intrinsic::InstrumentationName => Place::Column("toString(scope_name)"),
            Intrinsic::InstrumentationVersion => Place::Column("toString(scope_version)"),
            Intrinsic::Duration | Intrinsic::SpanId | Intrinsic::TraceId => Place::Envelope,
            Intrinsic::EventName
            | Intrinsic::EventTimeSinceStart
            | Intrinsic::LinkSpanId
            | Intrinsic::LinkTraceId => Place::Set,
            Intrinsic::NestedSetParent
            | Intrinsic::NestedSetLeft
            | Intrinsic::NestedSetRight
            | Intrinsic::ChildCount
            | Intrinsic::TraceDuration
            | Intrinsic::RootName
            | Intrinsic::RootServiceName => Place::Envelope,
        },
    }
}

/// A column's value, cut.
fn column(sql: &str) -> Projects {
    Projects::Value {
        value: ceiling_sql(sql),
        kind: "'String'".to_string(),
    }
}

/// A literal's value, cut.
fn literal(text: &str, kind: &str) -> Projects {
    Projects::Value {
        value: ceiling_sql(&escape::ch_string(text)),
        kind: format!("'{kind}'"),
    }
}

/// `stored(E)`: a `Dynamic` value as stored — an array as the JSON of its
/// typed elements, which the decode renders and cuts; any other value as
/// its text, cut; the kind is its stored type.
fn stored(e: &str) -> Projects {
    let (value, kind) = stored_parts(e);
    Projects::Value { value, kind }
}

/// [`stored`]'s value and kind.
fn stored_parts(e: &str) -> (String, String) {
    (
        format!(
            "if(startsWith(dynamicType({e}), 'Array'), toJSONString(arrayMap(x -> \
             (toString(dynamicType(x)), toString(x)), CAST({e}, 'Array(Dynamic)'))), {})",
            ceiling_sql(&format!("toString({e})"))
        ),
        format!("toString(dynamicType({e}))"),
    )
}

/// An attribute's stored value on the span row.
fn stored_attribute(root: &str, key: &str) -> Projects {
    stored(&attr_path(root, key))
}

/// A string expression's value, cut.
fn string_value(e: &str) -> Projects {
    Projects::Value {
        value: ceiling_sql(e),
        kind: "'String'".to_string(),
    }
}

/// `I(E)`: an event's offset from its span's start, an `Int128`, clamped
/// to `Int64` as the writer clamps it, so the value and its kind agree.
/// The bounds are written as strings, parsed as `Int128`.
fn offset_value(e: &str) -> Projects {
    const MAX: &str = "toInt128('9223372036854775807')";
    const MIN: &str = "toInt128('-9223372036854775808')";
    Projects::Value {
        value: format!("toString(toInt64(if({e} > {MAX}, {MAX}, if({e} < {MIN}, {MIN}, {e}))))"),
        kind: "'Int64'".to_string(),
    }
}

/// The value of a resource key at the span's own resource row.
fn resource_stored(key: &str, ctx: &PredicateCtx<'_>) -> Result<Projects, PlanError> {
    Ok(stored(&resource_value_in(key, ctx)?))
}

/// A field's stored value, as presence projects it: an attribute's own, a
/// column's, the resource row's, or a set's first element carrying it.
fn stored_value(field: &Field, ctx: &PredicateCtx<'_>) -> Result<Projects, PlanError> {
    Ok(match (place_of(field), field) {
        (Place::Attribute(root), Field::Attribute { key, .. }) => stored_attribute(root, key),
        (Place::Column(sql), _) => column(sql),
        (Place::Resource, Field::Attribute { key, .. }) => resource_stored(key, ctx)?,
        (Place::Set, _) => first_carrying(field, ctx)?,
        (Place::Envelope, _) | (Place::Attribute(_) | Place::Resource, Field::Intrinsic(_)) => {
            Projects::Nothing
        }
    })
}

/// `{ f }`, truthiness: an attribute projects the literal `true`; an
/// intrinsic its column. An event or link intrinsic has no truthiness: the
/// predicate refuses it.
fn truthiness_value(field: &Field) -> Projects {
    match place_of(field) {
        Place::Attribute(_) | Place::Resource => literal("true", "Bool"),
        Place::Set if matches!(field, Field::Attribute { .. }) => literal("true", "Bool"),
        Place::Column(sql) => column(sql),
        Place::Envelope | Place::Set => Projects::Nothing,
    }
}

/// The `= "s"` / `= true` literal side opposite `field`, alone.
fn literal_side<'a>(field: &Field, lhs: &'a FieldExpr, rhs: &'a FieldExpr) -> Option<&'a Value> {
    match (lhs, rhs) {
        (FieldExpr::Field(f), FieldExpr::Literal(v))
        | (FieldExpr::Literal(v), FieldExpr::Field(f))
            if f == field =>
        {
            Some(v)
        }
        _ => None,
    }
}

/// A comparison naming `field` alone: a span-row column projects itself
/// under any condition; any other field projects nothing under `!=` and
/// `!~`, the literal when compared `=` with a string or boolean literal
/// (a link id's lowercased, as the predicate compares it), and otherwise
/// its value where the condition read it: an attribute's stored value, a
/// resource row's, or a set's element ([`set_value`]).
fn comparison_value(
    field: &Field,
    op: ComparisonOp,
    lhs: &FieldExpr,
    rhs: &FieldExpr,
    comparison: &FieldExpr,
    ctx: &PredicateCtx<'_>,
) -> Result<Projects, PlanError> {
    let place = place_of(field);
    // Section 3.6's refusal comes before every operator's own rule, `!=`
    // and `!~` included: such a comparison projects nothing, but its
    // predicate matches any pair of elements where today's engine reads
    // each element against itself, so the statement must not serve it.
    if matches!(place, Place::Set) {
        refuse_two_occurrences(field, lhs, rhs)?;
    }
    match place {
        Place::Column(sql) => return Ok(column(sql)),
        Place::Envelope => return Ok(Projects::Nothing),
        Place::Attribute(_) | Place::Resource | Place::Set
            if matches!(op, ComparisonOp::Neq | ComparisonOp::Nre) =>
        {
            return Ok(Projects::Nothing);
        }
        Place::Attribute(_) | Place::Resource | Place::Set => {}
    }
    if op == ComparisonOp::Eq {
        match literal_side(field, lhs, rhs) {
            Some(Value::String(s)) => {
                let text = match field {
                    Field::Intrinsic(Intrinsic::LinkSpanId | Intrinsic::LinkTraceId) => {
                        s.to_lowercase()
                    }
                    _ => s.clone(),
                };
                return Ok(literal(&text, "String"));
            }
            Some(Value::Bool(b)) => {
                return Ok(literal(if *b { "true" } else { "false" }, "Bool"));
            }
            _ => {}
        }
    }
    match (place, field) {
        (Place::Attribute(root), Field::Attribute { key, .. }) => Ok(stored_attribute(root, key)),
        (Place::Resource, Field::Attribute { key, .. }) => resource_stored(key, ctx),
        (Place::Set, _) => set_value(field, lhs, rhs, comparison, ctx),
        _ => Ok(Projects::Nothing),
    }
}

/// Whether `expr` holds an arithmetic node anywhere.
fn has_arithmetic(expr: &FieldExpr) -> bool {
    match expr {
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            ..
        }
        | FieldExpr::Unary {
            op: UnaryOp::Neg, ..
        } => true,
        FieldExpr::Binary { lhs, rhs, .. } => has_arithmetic(lhs) || has_arithmetic(rhs),
        FieldExpr::Unary { expr, .. } => has_arithmetic(expr),
        FieldExpr::Field(_) | FieldExpr::Literal(_) | FieldExpr::Exists { .. } => false,
    }
}

/// Whether `expr` holds literals and arithmetic only.
fn literal_only(expr: &FieldExpr) -> bool {
    match expr {
        FieldExpr::Literal(_) => true,
        FieldExpr::Binary {
            op: FieldOp::Arith(_),
            lhs,
            rhs,
        } => literal_only(lhs) && literal_only(rhs),
        FieldExpr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => literal_only(expr),
        _ => false,
    }
}

/// Whether `expr` is arithmetic over fields and literals only.
fn arithmetic_only(expr: &FieldExpr) -> bool {
    fn operand(expr: &FieldExpr) -> bool {
        match expr {
            FieldExpr::Field(_) | FieldExpr::Literal(_) => true,
            FieldExpr::Binary {
                op: FieldOp::Arith(_),
                lhs,
                rhs,
            } => operand(lhs) && operand(rhs),
            FieldExpr::Unary {
                op: UnaryOp::Neg,
                expr,
            } => operand(expr),
            _ => false,
        }
    }
    has_arithmetic(expr) && operand(expr)
}

/// A set field's value under a comparison naming it alone (sections 3.2,
/// 3.5 and 3.6): opposite a literal-only side, the first element whose own
/// condition holds; once inside arithmetic, the first element the
/// comparison holds for; compared with itself, the first element carrying
/// it. Twice in a comparison holding arithmetic was refused before this
/// ([`refuse_two_occurrences`]); any other shape — the field under `!` or
/// inside a boolean-valued operand — which the design does not name is
/// refused here.
fn set_value(
    field: &Field,
    lhs: &FieldExpr,
    rhs: &FieldExpr,
    comparison: &FieldExpr,
    ctx: &PredicateCtx<'_>,
) -> Result<Projects, PlanError> {
    let mut fields = Vec::new();
    operand_fields(lhs, &mut fields);
    operand_fields(rhs, &mut fields);
    if fields.len() >= 2 {
        if matches!((lhs, rhs), (FieldExpr::Field(_), FieldExpr::Field(_))) {
            return first_carrying(field, ctx);
        }
        return Err(unnamed_shape(field));
    }
    let mut on_left = Vec::new();
    operand_fields(lhs, &mut on_left);
    let (side, other) = if on_left.is_empty() {
        (rhs, lhs)
    } else {
        (lhs, rhs)
    };
    match side {
        FieldExpr::Field(_) if literal_only(other) => first_holding(field, comparison, ctx),
        _ if arithmetic_only(side) && literal_only(other) => {
            let e = element_select_in(comparison, ctx)?;
            if e == "false" {
                return Ok(Projects::Nothing);
            }
            Ok(selected_value(field, &e))
        }
        _ => Err(unnamed_shape(field)),
    }
}

/// Section 3.6: one set field twice in a comparison holding arithmetic has
/// no single element — the predicate matches any pair of elements — and
/// is refused, under every operator.
fn refuse_two_occurrences(
    field: &Field,
    lhs: &FieldExpr,
    rhs: &FieldExpr,
) -> Result<(), PlanError> {
    let mut fields = Vec::new();
    operand_fields(lhs, &mut fields);
    operand_fields(rhs, &mut fields);
    if fields.len() >= 2 && (has_arithmetic(lhs) || has_arithmetic(rhs)) {
        return Err(PlanError::UnsupportedField(format!(
            "projecting {field} from two occurrences of an event, link or unscoped field in one \
             comparison is not supported by the search statement"
        )));
    }
    Ok(())
}

/// A set field in a comparison shape the design does not name: under `!`,
/// or inside a boolean-valued operand.
fn unnamed_shape(field: &Field) -> PlanError {
    PlanError::UnsupportedField(format!(
        "projecting {field} from a comparison that holds it under `!` or inside a boolean-valued \
         operand is not supported by the search statement"
    ))
}

/// Section 3.2: the first element whose own condition holds, for a
/// comparison of `field` against a literal-only side; the unscoped `.k`
/// through the chain, its event and link values the same comparison's on
/// `event.k` and `link.k`.
fn first_holding(
    field: &Field,
    comparison: &FieldExpr,
    ctx: &PredicateCtx<'_>,
) -> Result<Projects, PlanError> {
    if let Field::Attribute {
        scope: AttrScope::Unscoped,
        key,
    } = field
    {
        return chain_value(key, ctx, |scoped| {
            first_holding(scoped, &with_field(comparison, field, scoped), ctx)
        });
    }
    let index = element_index_in(comparison, ctx)?;
    if index == "false" {
        return Ok(Projects::Nothing);
    }
    Ok(element_value(field, &index))
}

/// Section 3.6: the first element carrying `field` — an attribute's first
/// element holding the key, an intrinsic's first element; the unscoped
/// `.k` through the chain.
fn first_carrying(field: &Field, ctx: &PredicateCtx<'_>) -> Result<Projects, PlanError> {
    match field {
        Field::Attribute {
            scope: AttrScope::Unscoped,
            key,
        } => chain_value(key, ctx, |scoped| first_carrying(scoped, ctx)),
        Field::Attribute { .. } => {
            let presence = FieldExpr::Exists {
                field: field.clone(),
                negated: false,
            };
            let index = element_index_in(&presence, ctx)?;
            Ok(element_value(field, &index))
        }
        Field::Intrinsic(_) => Ok(element_value(field, "1")),
    }
}

/// `.key`'s value: the first scope of the chain that holds it decides —
/// span, resource, event, link, instrumentation — as the predicate's chain
/// does. `element` gives the event and link scopes' values, for the field
/// at that scope.
fn chain_value(
    key: &str,
    ctx: &PredicateCtx<'_>,
    element: impl Fn(&Field) -> Result<Projects, PlanError>,
) -> Result<Projects, PlanError> {
    let mut values = Vec::with_capacity(2 * CHAIN.len() + 1);
    let mut kinds = Vec::with_capacity(2 * CHAIN.len() + 1);
    for scope in CHAIN {
        let present = chain_presence_in(scope, key, ctx)?;
        let projects = match scope {
            AttrScope::Span => stored_attribute("attrs", key),
            AttrScope::Instrumentation => stored_attribute("scope_attrs", key),
            AttrScope::Resource if key == "service.name" => {
                let (value, kind) = stored_parts(&resource_value_in(key, ctx)?);
                Projects::Value {
                    value: format!(
                        "if(service_type = 'string', {}, {value})",
                        ceiling_sql("toString(service)")
                    ),
                    kind: format!("if(service_type = 'string', 'String', {kind})"),
                }
            }
            AttrScope::Resource => resource_stored(key, ctx)?,
            AttrScope::Event | AttrScope::Link => element(&Field::Attribute {
                scope,
                key: key.to_string(),
            })?,
            AttrScope::Unscoped => unreachable!("the chain holds no unscoped scope"),
        };
        let (value, kind) = match projects {
            Projects::Value { value, kind } => (value, kind),
            Projects::Nothing => ("''".to_string(), "''".to_string()),
        };
        values.push(present.clone());
        values.push(value);
        kinds.push(present);
        kinds.push(kind);
    }
    Ok(Projects::Value {
        value: format!("multiIf({}, '')", values.join(", ")),
        kind: format!("multiIf({}, '')", kinds.join(", ")),
    })
}

/// `expr` with every occurrence of `from` read as `to`.
fn with_field(expr: &FieldExpr, from: &Field, to: &Field) -> FieldExpr {
    match expr {
        FieldExpr::Field(f) if f == from => FieldExpr::Field(to.clone()),
        FieldExpr::Exists { field, negated } if field == from => FieldExpr::Exists {
            field: to.clone(),
            negated: *negated,
        },
        FieldExpr::Field(_) | FieldExpr::Literal(_) | FieldExpr::Exists { .. } => expr.clone(),
        FieldExpr::Unary { op, expr } => FieldExpr::Unary {
            op: *op,
            expr: Box::new(with_field(expr, from, to)),
        },
        FieldExpr::Binary { op, lhs, rhs } => FieldExpr::Binary {
            op: *op,
            lhs: Box::new(with_field(lhs, from, to)),
            rhs: Box::new(with_field(rhs, from, to)),
        },
    }
}

/// A set field's element at the 1-based `index`: an attribute's stored
/// value; `event:name` and the link ids as text, the ids as lowercase hex;
/// `event:timeSinceStart` through [`offset_value`].
fn element_value(field: &Field, index: &str) -> Projects {
    match field {
        Field::Attribute { scope, key } => {
            let root = match scope {
                AttrScope::Link => "links.attrs",
                _ => "events.attrs",
            };
            stored(&format!("arrayElement({}, {index})", attr_path(root, key)))
        }
        Field::Intrinsic(Intrinsic::EventName) => {
            string_value(&format!("toString(arrayElement(events.name, {index}))"))
        }
        Field::Intrinsic(Intrinsic::EventTimeSinceStart) => offset_value(&format!(
            "arrayElement(arrayMap(t -> toInt128(t) - start_ns, events.time_ns), {index})"
        )),
        Field::Intrinsic(Intrinsic::LinkSpanId) => {
            string_value(&format!("lower(hex(arrayElement(links.span_id, {index})))"))
        }
        Field::Intrinsic(Intrinsic::LinkTraceId) => string_value(&format!(
            "lower(hex(arrayElement(links.trace_id, {index})))"
        )),
        Field::Intrinsic(_) => Projects::Nothing,
    }
}

/// A set field's element as select mode returns it: an attribute's (or
/// the chain's) `Dynamic` element stored; `event:timeSinceStart`'s offset
/// through [`offset_value`]; `event:name` and the link ids, already text.
fn selected_value(field: &Field, element: &str) -> Projects {
    match field {
        Field::Attribute { .. } => stored(element),
        Field::Intrinsic(Intrinsic::EventTimeSinceStart) => offset_value(element),
        Field::Intrinsic(_) => string_value(&format!("toString({element})")),
    }
}

#[cfg(test)]
mod rendered_len_tests {
    use super::*;

    /// Section 6.1: `rendered_len` is the length of the cut render, for a
    /// short four-type array and for one that crosses the ceiling in
    /// multi-byte code points.
    #[test]
    fn rendered_len_is_the_length_of_the_cut_render() {
        let long = format!("[{}]", vec![r#"["String","é€"]"#; 3000].join(","));
        for value in [
            r#"[["String","g"],["Int64","7"],["Float64","1e5"],["Bool","true"]]"#,
            long.as_str(),
        ] {
            let values = array_values(value).expect("a well-formed array");
            let full = serde_json::to_string(&values).expect("a Value serializes");
            let render = ceiling(full.clone());
            eprintln!("uncut {} cut {}", full.len(), render.len());
            assert_eq!(
                rendered_len(&values),
                render.len(),
                "{}",
                &full[..full.len().min(80)]
            );
        }
    }
}
