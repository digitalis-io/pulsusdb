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
//! (`attrs.`, `scope_attrs.`). A field off the span row — a resource
//! attribute other than `service.name`, an event or link attribute, the
//! unscoped `.k`, an event or link intrinsic — is part 2's, and a query
//! that would project one is refused.
//!
//! **Every value's text is cut at today's response ceiling**: more than
//! 8,192 bytes becomes its first 2,048 code points. An array's JSON is
//! rendered in the decode, so the decode cuts it by the same rule.

use pulsus_traceql::{
    AttrScope, ComparisonOp, Field, FieldExpr, FieldOp, Intrinsic, UnaryOp, Value,
};

use super::predicate::{PredicateCtx, attr_path, compile_span_predicate_in};
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
    /// Nothing: the condition supplies no value (`!=`, `!~`, the
    /// envelope's own fields).
    Nothing,
    /// A value's SQL, already cut, and its kind's SQL.
    Value { value: String, kind: String },
    /// A value off the span row: part 2's.
    OffTheRow,
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
                    self.add(field, stored_value(field), &presence, ctx)?;
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
                self.add(field, comparison_value(field, *op, lhs, rhs), expr, ctx)
            }
            FieldExpr::Binary { lhs, rhs, .. } => {
                self.walk(lhs, neg, ctx)?;
                self.walk(rhs, neg, ctx)
            }
        }
    }

    /// Adds one source to `field`'s group, creating the group on its first
    /// source; refuses a value off the span row.
    fn add(
        &mut self,
        field: &Field,
        projects: Projects,
        gate: &FieldExpr,
        ctx: &PredicateCtx<'_>,
    ) -> Result<(), PlanError> {
        let (value, kind) = match projects {
            Projects::Nothing => return Ok(()),
            Projects::OffTheRow => {
                return Err(PlanError::UnsupportedField(format!(
                    "projecting {field}, a value off the span row, is not supported by the \
                     search statement yet (issue #591 part 2)"
                )));
            }
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
    serde_json::to_string(&rendered).map_err(|e| e.to_string())
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
    /// Off the span row: part 2's.
    OffTheRow,
}

/// **No wildcard arm**: a new scope or intrinsic decides here.
fn place_of(field: &Field) -> Place {
    match field {
        Field::Attribute { scope, key } => match scope {
            AttrScope::Span => Place::Attribute("attrs"),
            AttrScope::Instrumentation => Place::Attribute("scope_attrs"),
            AttrScope::Resource if key == "service.name" => Place::Column("toString(service)"),
            AttrScope::Resource | AttrScope::Event | AttrScope::Link | AttrScope::Unscoped => {
                Place::OffTheRow
            }
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
            | Intrinsic::LinkTraceId => Place::OffTheRow,
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

/// An attribute's stored value: an array as the JSON of its typed
/// elements, which the decode renders and cuts; any other value as its
/// text, cut; the kind is its stored type.
fn stored_attribute(root: &str, key: &str) -> Projects {
    let r = attr_path(root, key);
    Projects::Value {
        value: format!(
            "if(startsWith(dynamicType({r}), 'Array'), toJSONString(arrayMap(x -> \
             (toString(dynamicType(x)), toString(x)), CAST({r}, 'Array(Dynamic)'))), {})",
            ceiling_sql(&format!("toString({r})"))
        ),
        kind: format!("toString(dynamicType({r}))"),
    }
}

/// A field's stored value: an attribute's own, or a column's.
fn stored_value(field: &Field) -> Projects {
    match (place_of(field), field) {
        (Place::Attribute(root), Field::Attribute { key, .. }) => stored_attribute(root, key),
        (Place::Column(sql), _) => column(sql),
        (Place::Envelope, _) => Projects::Nothing,
        (Place::OffTheRow, _) | (Place::Attribute(_), Field::Intrinsic(_)) => Projects::OffTheRow,
    }
}

/// `{ f }`, truthiness: an attribute projects the literal `true`; an
/// intrinsic its column.
fn truthiness_value(field: &Field) -> Projects {
    match place_of(field) {
        Place::Attribute(_) => literal("true", "Bool"),
        Place::Column(sql) => column(sql),
        Place::Envelope => Projects::Nothing,
        Place::OffTheRow => Projects::OffTheRow,
    }
}

/// A comparison naming `field` alone: a span-row column projects itself
/// under any condition; an attribute projects nothing under `!=` and `!~`,
/// the literal when compared `=` with a string or boolean literal, and its
/// stored value under any other condition, arithmetic included.
fn comparison_value(field: &Field, op: ComparisonOp, lhs: &FieldExpr, rhs: &FieldExpr) -> Projects {
    let root = match place_of(field) {
        Place::Column(sql) => return column(sql),
        Place::Envelope => return Projects::Nothing,
        Place::OffTheRow | Place::Attribute(_)
            if matches!(op, ComparisonOp::Neq | ComparisonOp::Nre) =>
        {
            return Projects::Nothing;
        }
        Place::OffTheRow => return Projects::OffTheRow,
        Place::Attribute(root) => root,
    };
    if op == ComparisonOp::Eq {
        let literal_side = match (lhs, rhs) {
            (FieldExpr::Field(f), FieldExpr::Literal(v))
            | (FieldExpr::Literal(v), FieldExpr::Field(f))
                if f == field =>
            {
                Some(v)
            }
            _ => None,
        };
        match literal_side {
            Some(Value::String(s)) => return literal(s, "String"),
            Some(Value::Bool(b)) => return literal(if *b { "true" } else { "false" }, "Bool"),
            _ => {}
        }
    }
    match field {
        Field::Attribute { key, .. } => stored_attribute(root, key),
        Field::Intrinsic(_) => Projects::OffTheRow,
    }
}
