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
//! attribute, or an event or link intrinsic, as the LAST element whose own
//! condition holds, or — once inside arithmetic — the last element the
//! comparison holds for (issue #600 part 3, the reference's response); the
//! unscoped `.k` at the first scope of the predicate's chain that holds
//! it. A set field compared with itself, `select()`ed or tested for
//! presence is its last element carrying the field. A search aggregate's
//! argument reads the FIRST element carrying it ([`Pick`]). One set field
//! twice in a comparison holding arithmetic has no single element and is
//! refused.
//!
//! **Every value's text is cut at today's response ceiling**: more than
//! 8,192 bytes becomes its first 2,048 code points. An array's string
//! elements are decoded one by one, so the decode cuts each by the same
//! rule (issue #601 part 1).

use pulsus_traceql::{
    AttrScope, ComparisonOp, Field, FieldExpr, FieldOp, Intrinsic, UnaryOp, Value,
};

use super::numbering::projected_sql;
use super::predicate::{
    CHAIN, HELD, PredicateCtx, SERVICE_IS_STRING, SERVICE_VALUE, attr_path, chain_presence_in,
    compile_span_predicate_in, element_index_in, element_select_in, last_element_index_in,
    resource_value_in, scope_value,
};
use super::rows::SearchProjected;
use crate::logql::error::ReadError;
use crate::logql::escape;
use crate::traces::PlanError;
use crate::traces::exec::ByteBudget;
use crate::traces::filter::flip_comparison;
use crate::traces::values::{GroupValue, ProjectedAttribute, WireKey, go_duration_string};
use pulsus_clickhouse::ChError;

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
    /// The wire key's length, `WireKey::new(&field).as_str().len()`: the
    /// key's charge, known before the key is built (issue #591 part 3).
    key_len: usize,
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

/// Which element of an event or link set a value read takes (issue #600
/// part 3): the projection the last, as the reference's response renders
/// a span's attributes; a search aggregate's argument the first, as the
/// reference's value reader does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pick {
    First,
    Last,
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

    /// Whether a projected value reads `nested_values` (issue #594 parts 2
    /// and 4).
    pub fn reads_numbers(&self) -> bool {
        self.groups
            .iter()
            .any(|g| g.sources.iter().any(|s| s.value.contains("nested_values.")))
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
        selected: &[Field],
        ctx: &PredicateCtx<'_>,
    ) -> Result<Projection, PlanError> {
        let mut projection = Projection::none();
        for body in bodies {
            projection.walk(body, false, ctx)?;
        }
        for field in selected {
            projection.select(field, ctx)?;
        }
        if projection.groups.len() > MAX_PROJECTION_GROUPS {
            return Err(PlanError::UnsupportedField(format!(
                "a search projecting more than {MAX_PROJECTION_GROUPS} distinct fields is not \
                 supported"
            )));
        }
        Ok(projection)
    }

    /// The walk the previous search engine's `collect_projection_leaves`
    /// made (deleted by #602 part 2c-2b-2), adding
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
                    self.add(field, stored_value(field, ctx, Pick::Last)?, &presence, ctx)?;
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

    /// `select(field)` (issue #592 part 1), as today's engine projects it:
    /// a span-row column on every span, `resource.service.name` included,
    /// as the span's own service, empty or not; any other attribute where
    /// the span holds it; nothing for the fields the envelope carries
    /// (`duration`, `span:id`, `trace:id`); any other intrinsic refused.
    fn select(&mut self, field: &Field, ctx: &PredicateCtx<'_>) -> Result<(), PlanError> {
        if let Place::Column(sql) = place_of(field) {
            return self.add(
                field,
                column(sql),
                &FieldExpr::Literal(Value::Bool(true)),
                ctx,
            );
        }
        match field {
            Field::Attribute { .. } => self.walk(
                &FieldExpr::Exists {
                    field: field.clone(),
                    negated: false,
                },
                false,
                ctx,
            ),
            Field::Intrinsic(
                Intrinsic::Duration
                | Intrinsic::SpanId
                | Intrinsic::TraceId
                | Intrinsic::TraceDuration
                | Intrinsic::RootName
                | Intrinsic::RootServiceName
                | Intrinsic::ChildCount,
            ) => Ok(()),
            Field::Intrinsic(
                i @ (Intrinsic::NestedSetLeft
                | Intrinsic::NestedSetRight
                | Intrinsic::NestedSetParent),
            ) => self.add(
                field,
                Projects::Value {
                    value: super::numbering::selected_sql(*i),
                    kind: "'Int64'".to_string(),
                },
                &FieldExpr::Literal(Value::Bool(true)),
                ctx,
            ),
            // The event and link intrinsics: a span carries a collection
            // of them, and the previous planner's text stands.
            Field::Intrinsic(_) => Err(PlanError::TypeMismatch(
                "select() of this intrinsic is not supported".to_string(),
            )),
        }
    }

    /// Adds one source to `field`'s group, creating the group on its first
    /// source. A `resource.service.name` source holds only where the span
    /// row stores it as a string (issue #602 part 1, D2).
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
        let mut gate = compile_span_predicate_in(gate, ctx)?.sql().to_string();
        if matches!(field, Field::Attribute { scope: AttrScope::Resource, key } if key == "service.name")
        {
            gate = format!("({gate}) AND {SERVICE_IS_STRING}");
        }
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
            key_len: WireKey::new(field).as_str().len(),
            target,
            sources: vec![source],
        });
        Ok(())
    }

    /// The attribute buffer's capacity: one slot per attribute group. A
    /// span holds at most one value per group, so the buffer never grows.
    pub(crate) fn attribute_capacity(&self) -> usize {
        self.groups
            .iter()
            .filter(|g| g.target == Target::Attribute)
            .count()
    }

    /// One span's projected values, decoded into `attributes`, returning
    /// its `name` when a `name` group projected one. A name is charged its
    /// length before it is kept; every other value through [`decode_one`]:
    /// its key's length plus its payload — a string its bytes, a number or
    /// boolean nothing, an array its buffer and its elements' payloads. A
    /// value that does not parse as its kind is a decode error, never a
    /// default.
    pub(crate) fn decode_charged(
        &self,
        projected: &[SearchProjected],
        attributes: &mut Vec<ProjectedAttribute>,
        budget: &mut ByteBudget,
    ) -> Result<Option<String>, ReadError> {
        let decode = |m: String| ReadError::Clickhouse(ChError::Decode(m));
        let mut name = None;
        for p in projected {
            let group = usize::from(p.group)
                .checked_sub(1)
                .and_then(|i| self.groups.get(i))
                .ok_or_else(|| {
                    decode(format!(
                        "projected group {} is not in the projection",
                        p.group
                    ))
                })?;
            if group.target == Target::Name {
                budget.charge(p.value.len())?;
                name = Some(p.value.clone());
                continue;
            }
            let value = decode_one(&p.kind, &p.value, group.key_len, budget)?;
            attributes.push(ProjectedAttribute::new(&group.field, value));
        }
        Ok(name)
    }
}

/// A projected value from the statement's text and stored kind (issue #601).
fn typed_value(kind: &str, text: &str) -> Result<GroupValue, String> {
    Ok(match kind {
        "Int64" => GroupValue::Int(
            text.parse()
                .map_err(|e| format!("projected Int64 {text:?}: {e}"))?,
        ),
        "Float64" => GroupValue::Double(
            text.parse::<f64>()
                .map_err(|e| format!("projected Float64 {text:?}: {e}"))?
                .to_bits(),
        ),
        "Bool" => GroupValue::Bool(text == "true"),
        "Duration" => GroupValue::Str(go_duration_string(
            text.parse()
                .map_err(|e| format!("projected Duration {text:?}: {e}"))?,
        )),
        k if k.starts_with("Array(") => GroupValue::Array(array_elements(text, true)?),
        _ => GroupValue::Str(text.to_string()),
    })
}

/// An array's elements from the statement's `[[type, text], …]`, in stored
/// order, read one pair at a time: no vector of pairs is ever built, so a
/// skipped element is dropped as it is read. `cut` applies the 8192-byte rule
/// to each string element (the projection does; a `by()` value does not). An
/// element with no value is left out; any type other than the four is an error.
pub(crate) fn array_elements(text: &str, cut: bool) -> Result<Vec<GroupValue>, String> {
    struct Elements(bool);
    impl<'de> serde::de::Visitor<'de> for Elements {
        type Value = Vec<GroupValue>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an array of [type, text] pairs")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            use serde::de::Error;
            let mut out = Vec::new();
            while let Some((t, v)) = seq.next_element::<(String, String)>()? {
                out.push(match t.as_str() {
                    "String" => GroupValue::Str(if self.0 { ceiling(v) } else { v }),
                    "Int64" => GroupValue::Int(
                        v.parse()
                            .map_err(|e| A::Error::custom(format!("array Int64 {v:?}: {e}")))?,
                    ),
                    "Float64" => GroupValue::Double(
                        v.parse::<f64>()
                            .map_err(|e| A::Error::custom(format!("array Float64 {v:?}: {e}")))?
                            .to_bits(),
                    ),
                    "Bool" => GroupValue::Bool(v == "true"),
                    "None" => continue,
                    other => {
                        return Err(A::Error::custom(format!(
                            "an array element stored as {other} is not decoded"
                        )));
                    }
                });
            }
            out.shrink_to_fit();
            Ok(out)
        }
    }
    let mut de = serde_json::Deserializer::from_str(text);
    let out = serde::Deserializer::deserialize_seq(&mut de, Elements(cut))
        .map_err(|e| format!("array {text:?}: {e}"))?;
    de.end().map_err(|e| format!("array {text:?}: {e}"))?;
    Ok(out)
}

/// One projected value, decoded and charged before `decode_charged` keeps
/// it: the charge is `key_len` plus [`GroupValue::payload_bytes`], which for
/// an array is its vector's capacity — whatever the stream reserved — plus
/// each element's payload.
pub(crate) fn decode_one(
    kind: &str,
    text: &str,
    key_len: usize,
    budget: &mut ByteBudget,
) -> Result<GroupValue, ReadError> {
    let value = typed_value(kind, text).map_err(|m| ReadError::Clickhouse(ChError::Decode(m)))?;
    budget.charge(key_len + value.payload_bytes())?;
    Ok(value)
}

/// Issue #592 part 2: how the search statement reads one `by()` key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupKeySql {
    /// The key's value as text: a column's keyword text, or a span
    /// attribute's stored text, an array as the JSON of its typed elements.
    pub value: String,
    /// The value's stored type, for an attribute or `duration`; a column
    /// key has none.
    pub value_type: Option<String>,
    /// What the key reads beside the span row (issue #594 part 3): the
    /// grouped statement's `group_values`.
    pub values: Option<super::tracelevel::KeyValues>,
}

/// The `by()` keys the search statement serves: `name`, `status`, `kind`,
/// `resource.service.name` and an attribute at every scope (issue #592 part
/// 2, issue #602 part 1); the six span-row intrinsics `duration`,
/// `statusMessage`, `span:id`, `span:parentID`, `instrumentation:name` and
/// `instrumentation:version` (issue #602 part 1, D3); `trace:id` (issue
/// #602 part 2b); and (issue #594 part
/// 3) the three per-trace intrinsics, `span:childCount` and the three
/// nested-set intrinsics, each read from the grouped statement's
/// `group_values`. `None` for the four event and link
/// intrinsics (D4), which the caller refuses. An attribute reads
/// [`scope_value`] (D1); a value the reference does not type is `''` typed
/// `'None'`, the `nil` group (D5). **No wildcard arm**: a new intrinsic
/// decides here.
pub fn group_key_sql(
    field: &Field,
    ctx: &PredicateCtx<'_>,
) -> Result<Option<GroupKeySql>, PlanError> {
    use super::tracelevel::{KeyValues, TraceValue};
    let typed = |x: String| GroupKeySql {
        value: format!(
            "if(NOT (dynamicType({x}) {HELD}), '', if(startsWith(dynamicType({x}), 'Array'), \
             toJSONString(arrayMap(x -> (toString(dynamicType(x)), toString(x)), \
             CAST({x}, 'Array(Dynamic)'))), toString({x})))"
        ),
        value_type: Some(format!(
            "if(dynamicType({x}) {HELD}, toString(dynamicType({x})), 'None')"
        )),
        values: None,
    };
    let column = |sql: &str| GroupKeySql {
        value: sql.to_string(),
        value_type: None,
        values: None,
    };
    let looked_up = |values: KeyValues, value: String, value_type: String| GroupKeySql {
        value,
        value_type: Some(value_type),
        values: Some(values),
    };
    Ok(match field {
        Field::Attribute {
            scope: AttrScope::Resource,
            key,
        } if key == "service.name" => Some(GroupKeySql {
            value: "if(service_type = 'string', toString(service), '')".to_string(),
            value_type: Some("if(service_type = 'string', 'String', 'None')".to_string()),
            values: None,
        }),
        Field::Attribute { scope, key } => Some(typed(scope_value(*scope, key, ctx)?)),
        Field::Intrinsic(Intrinsic::Duration) => Some(GroupKeySql {
            value: "toString(duration_ns)".to_string(),
            value_type: Some("'Duration'".to_string()),
            values: None,
        }),
        Field::Intrinsic(Intrinsic::StatusMessage) => Some(column("status_message")),
        Field::Intrinsic(Intrinsic::SpanId) => Some(column("lower(hex(span_id))")),
        Field::Intrinsic(Intrinsic::ParentId) => Some(column("lower(hex(parent_span_id))")),
        Field::Intrinsic(Intrinsic::TraceId) => Some(column("lower(hex(trace_id))")),
        Field::Intrinsic(Intrinsic::InstrumentationName) => Some(column("toString(scope_name)")),
        Field::Intrinsic(Intrinsic::InstrumentationVersion) => {
            Some(column("toString(scope_version)"))
        }
        Field::Intrinsic(
            Intrinsic::EventName
            | Intrinsic::EventTimeSinceStart
            | Intrinsic::LinkSpanId
            | Intrinsic::LinkTraceId,
        ) => None,
        Field::Intrinsic(i @ (Intrinsic::TraceDuration | Intrinsic::RootName | Intrinsic::RootServiceName)) => {
            let v = TraceValue::of(*i).expect("one of the three");
            let x = v.lookup_sql("group_values");
            let (value, kind) = if v == TraceValue::Duration {
                (format!("ifNull(toString({x}), '')"), "Duration")
            } else {
                (format!("ifNull({x}, '')"), "String")
            };
            Some(looked_up(
                KeyValues::Trace(v),
                value,
                format!("if(isNull({x}), 'None', '{kind}')"),
            ))
        }
        Field::Intrinsic(Intrinsic::ChildCount) => Some(looked_up(
            KeyValues::ChildCount,
            "toString(transform(concat(trace_id, span_id), group_values.1, group_values.2, toUInt64(0)))".to_string(),
            "'Int64'".to_string(),
        )),
        Field::Intrinsic(i @ (Intrinsic::NestedSetLeft | Intrinsic::NestedSetRight | Intrinsic::NestedSetParent)) => Some(looked_up(
            KeyValues::Numbered(*i),
            "toString(transform(concat(trace_id, span_id), group_values.1, group_values.2, toInt64(0)))".to_string(),
            "'Int64'".to_string(),
        )),
        Field::Intrinsic(Intrinsic::Name | Intrinsic::Status | Intrinsic::Kind) => {
            match place_of(field) {
                Place::Column(sql) => Some(column(sql)),
                _ => None,
            }
        }
    })
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
    /// A nested-set intrinsic: its number (issue #594 part 2).
    Numbered(Intrinsic),
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
            Intrinsic::NestedSetParent | Intrinsic::NestedSetLeft | Intrinsic::NestedSetRight => {
                Place::Numbered(*intrinsic)
            }
            Intrinsic::ChildCount
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
/// its text, cut; the kind is its stored type. A value the reference does
/// not type is `''` typed `'None'` (issue #602 part 1, D5).
fn stored(e: &str) -> Projects {
    let (value, kind) = stored_parts(e);
    Projects::Value { value, kind }
}

/// [`stored`]'s value and kind.
fn stored_parts(e: &str) -> (String, String) {
    (
        format!(
            "if(NOT (dynamicType({e}) {HELD}), '', if(startsWith(dynamicType({e}), 'Array'), \
             toJSONString(arrayMap(x -> (toString(dynamicType(x)), toString(x)), \
             CAST({e}, 'Array(Dynamic)'))), {}))",
            ceiling_sql(&format!("toString({e})"))
        ),
        format!("if(dynamicType({e}) {HELD}, toString(dynamicType({e})), 'None')"),
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
/// to `Int64` as the writer clamps it, so the value parses as its kind. The
/// kind is a duration, which the decode renders in Go's duration text
/// (issue #601 part 1). The bounds are written as strings, parsed as
/// `Int128`.
fn offset_value(e: &str) -> Projects {
    const MAX: &str = "toInt128('9223372036854775807')";
    const MIN: &str = "toInt128('-9223372036854775808')";
    Projects::Value {
        value: format!("toString(toInt64(if({e} > {MAX}, {MAX}, if({e} < {MIN}, {MIN}, {e}))))"),
        kind: "'Duration'".to_string(),
    }
}

/// The value of a resource key at the span's own resource row.
fn resource_stored(key: &str, ctx: &PredicateCtx<'_>) -> Result<Projects, PlanError> {
    Ok(stored(&resource_value_in(key, ctx)?))
}

/// A field's stored value, as presence projects it: an attribute's own, a
/// column's, the resource row's, or a set's element carrying it, the one
/// `pick` names.
fn stored_value(field: &Field, ctx: &PredicateCtx<'_>, pick: Pick) -> Result<Projects, PlanError> {
    Ok(match (place_of(field), field) {
        (Place::Attribute(root), Field::Attribute { key, .. }) => stored_attribute(root, key),
        (Place::Column(sql), _) => column(sql),
        (Place::Resource, Field::Attribute { key, .. }) => resource_stored(key, ctx)?,
        (Place::Set, _) => carrying(field, ctx, pick)?,
        (Place::Envelope | Place::Numbered(_), _)
        | (Place::Attribute(_) | Place::Resource, Field::Intrinsic(_)) => Projects::Nothing,
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
        Place::Envelope | Place::Set | Place::Numbered(_) => Projects::Nothing,
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

/// The number constant one side of a comparison holds against the bare
/// `field` on the other — a literal, or a negated literal (`-1`, which the
/// parser keeps as `Neg(1)`) — and whether the field is on the left.
fn numeric_constant_side(field: &Field, lhs: &FieldExpr, rhs: &FieldExpr) -> Option<(Value, bool)> {
    let constant = |e: &FieldExpr| match e {
        FieldExpr::Literal(v @ Value::Number(_)) => Some(v.clone()),
        FieldExpr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => match expr.as_ref() {
            FieldExpr::Literal(Value::Number(raw)) => Some(Value::Number(format!("-{raw}"))),
            _ => None,
        },
        _ => None,
    };
    match (lhs, rhs) {
        (FieldExpr::Field(f), other) if f == field => constant(other).map(|v| (v, true)),
        (other, FieldExpr::Field(f)) if f == field => constant(other).map(|v| (v, false)),
        _ => None,
    }
}

/// A comparison naming `field` alone: nothing when an operand holds a `!`,
/// a comparison or `&&`/`||` ([`boolean_valued`]); a span-row column
/// projects itself under any other condition; any other field projects nothing under `!=` and
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
    if boolean_valued(lhs) || boolean_valued(rhs) {
        return Ok(Projects::Nothing);
    }
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
        // A one-field nested-set condition projects that field's number,
        // whatever else the condition holds (docs/api.md §4.2; measured
        // against the reference, issue #602 part 2c-1). A number constant on
        // one side — a literal, or a negated one, `-1` — is read as the
        // predicate reads it, field first, so a root-only parent comparison
        // keeps its `-1` and reads no numbering; any other condition (the
        // bare field on both sides, arithmetic) projects the span's number.
        Place::Numbered(intrinsic) => {
            let value = match numeric_constant_side(field, lhs, rhs) {
                Some((v, field_first)) => projected_sql(
                    intrinsic,
                    if field_first { op } else { flip_comparison(op) },
                    &v,
                ),
                None => super::numbering::selected_sql(intrinsic),
            };
            return Ok(Projects::Value {
                value,
                kind: "'Int64'".to_string(),
            });
        }
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

/// Whether `expr` holds a `!`, a comparison or `&&`/`||` (issue #602 part
/// 1, D6): a field inside such an operand projects nothing, a span-row
/// column included.
fn boolean_valued(expr: &FieldExpr) -> bool {
    match expr {
        FieldExpr::Unary {
            op: UnaryOp::Not, ..
        }
        | FieldExpr::Binary {
            op: FieldOp::Cmp(_) | FieldOp::Bool(_),
            ..
        } => true,
        FieldExpr::Binary { lhs, rhs, .. } => boolean_valued(lhs) || boolean_valued(rhs),
        FieldExpr::Unary { expr, .. } => boolean_valued(expr),
        FieldExpr::Field(_) | FieldExpr::Literal(_) | FieldExpr::Exists { .. } => false,
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
/// 3.5 and 3.6; issue #600 part 3): opposite a literal-only side, the last
/// element whose own condition holds; once inside arithmetic, the last
/// element the comparison holds for; compared with itself, the last element
/// carrying it. Twice in a comparison holding arithmetic was refused before
/// this ([`refuse_two_occurrences`]); any other shape projects nothing, as
/// the previous engine projects nothing for it (issue #602 part 1, D6).
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
            return carrying(field, ctx, Pick::Last);
        }
        return Ok(Projects::Nothing);
    }
    let mut on_left = Vec::new();
    operand_fields(lhs, &mut on_left);
    let (side, other) = if on_left.is_empty() {
        (rhs, lhs)
    } else {
        (lhs, rhs)
    };
    match side {
        FieldExpr::Field(_) if literal_only(other) => last_holding(field, comparison, ctx),
        _ if arithmetic_only(side) && literal_only(other) => {
            let e = element_select_in(comparison, ctx)?;
            if e == "false" {
                return Ok(Projects::Nothing);
            }
            Ok(selected_value(field, &e))
        }
        _ => Ok(Projects::Nothing),
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

/// Section 3.2: the last element whose own condition holds (issue #600
/// part 3), for a comparison of `field` against a literal-only side; the
/// unscoped `.k` through the chain, its event and link values the same
/// comparison's on `event.k` and `link.k`.
fn last_holding(
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
            last_holding(scoped, &with_field(comparison, field, scoped), ctx)
        });
    }
    let index = last_element_index_in(comparison, ctx)?;
    if index == "false" {
        return Ok(Projects::Nothing);
    }
    Ok(element_value(field, &index))
}

/// Section 3.6: the element carrying `field` that `pick` names — an
/// attribute's first or last element holding the key, an intrinsic's
/// first or last element; the unscoped `.k` through the chain.
fn carrying(field: &Field, ctx: &PredicateCtx<'_>, pick: Pick) -> Result<Projects, PlanError> {
    match field {
        Field::Attribute {
            scope: AttrScope::Unscoped,
            key,
        } => chain_value(key, ctx, |scoped| carrying(scoped, ctx, pick)),
        Field::Attribute { .. } => {
            let presence = FieldExpr::Exists {
                field: field.clone(),
                negated: false,
            };
            let index = match pick {
                Pick::First => element_index_in(&presence, ctx)?,
                Pick::Last => last_element_index_in(&presence, ctx)?,
            };
            Ok(element_value(field, &index))
        }
        Field::Intrinsic(_) => Ok(element_value(
            field,
            match pick {
                Pick::First => "1",
                Pick::Last => "-1",
            },
        )),
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
            AttrScope::Resource if key == "service.name" => Projects::Value {
                value: format!(
                    "if(service_type = 'string', {}, '')",
                    ceiling_sql("toString(service)")
                ),
                kind: "if(service_type = 'string', 'String', 'None')".to_string(),
            },
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

/// A set field's element at the 1-based `index`, `-1` the last: an
/// attribute's stored value; `event:name` and the link ids as text, the ids
/// as lowercase hex; `event:timeSinceStart` through [`offset_value`].
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

/// Issue #592 part 3: an aggregate argument's located element, as the
/// projection reads a field's stored value — an attribute's own at any
/// scope (issue #602 part 1, D7), the unscoped `.k` at the first scope of
/// the chain that holds it — as `(text, stored type)` SQL. `None` for any
/// other field.
pub(crate) fn aggregate_argument_sql(
    field: &Field,
    ctx: &PredicateCtx<'_>,
) -> Result<Option<(String, String)>, PlanError> {
    match field {
        // The span row's copy, held only as a string (D2): `place_of`'s
        // column reads it whatever its arm, which only `add`'s gate stops.
        Field::Attribute {
            scope: AttrScope::Resource,
            key,
        } if key == "service.name" => Ok(Some(stored_parts(SERVICE_VALUE))),
        Field::Attribute { .. } => match stored_value(field, ctx, Pick::First)? {
            Projects::Value { value, kind } => Ok(Some((value, kind))),
            Projects::Nothing => Ok(None),
        },
        _ => Ok(None),
    }
}

#[cfg(test)]
mod typed_value_tests {
    use super::*;
    use crate::logql::error::TooBroadReason;

    /// T2 (issue #601 part 1): a projected duration takes Go's duration
    /// text, an array its elements in their own arms, a `None` element is
    /// left out, a long string element is cut, and an element of any other
    /// type is an error.
    #[test]
    fn a_projected_duration_and_array_take_their_own_arms() {
        assert_eq!(
            typed_value("Duration", "20000000"),
            Ok(GroupValue::Str("20ms".to_string()))
        );
        assert_eq!(
            typed_value("Duration", "-1500000000"),
            Ok(GroupValue::Str("-1.5s".to_string()))
        );
        assert_eq!(
            typed_value(
                "Array(Nullable(String))",
                r#"[["String","x"],["None",""],["String","y"]]"#
            ),
            Ok(GroupValue::Array(vec![
                GroupValue::Str("x".to_string()),
                GroupValue::Str("y".to_string()),
            ]))
        );
        let one = typed_value(
            "Array(Nullable(String))",
            r#"[["None",""],["String","x"],["None",""]]"#,
        )
        .expect("decodes");
        assert_eq!(
            one,
            GroupValue::Array(vec![GroupValue::Str("x".to_string())])
        );
        let GroupValue::Array(v) = &one else {
            unreachable!("asserted above")
        };
        assert_eq!(
            one.payload_bytes(),
            v.capacity() * std::mem::size_of::<GroupValue>() + 1
        );
        assert_eq!(
            typed_value(
                "Array(Nullable(Float64))",
                r#"[["Float64","nan"],["Float64","1.5"]]"#
            ),
            Ok(GroupValue::Array(vec![
                GroupValue::Double(f64::NAN.to_bits()),
                GroupValue::Double(1.5f64.to_bits()),
            ]))
        );
        let long = "é".repeat(5_000);
        assert_eq!(long.len(), 10_000);
        assert_eq!(
            typed_value(
                "Array(Nullable(String))",
                &format!(r#"[["String","{long}"]]"#)
            ),
            Ok(GroupValue::Array(vec![GroupValue::Str("é".repeat(2_048))]))
        );
        assert!(
            typed_value("Array(Nullable(String))", r#"[["Map",""]]"#).is_err(),
            "an element stored as Map is not decoded"
        );
    }

    /// T2b (issue #601 part 1): ten thousand skipped `None` elements cost
    /// nothing — the charge is the key, the kept vector's capacity and the
    /// one kept string — and one byte under that charge is refused.
    #[test]
    fn skipped_array_elements_cost_nothing_at_the_budget_edge() {
        let mut pairs = vec![r#"["None",""]"#; 10_000];
        pairs.push(r#"["String","x"]"#);
        let text = format!("[{}]", pairs.join(","));
        let kind = "Array(Nullable(String))";
        let first = decode_one(kind, &text, 3, &mut ByteBudget::new(usize::MAX))
            .expect("decodes unbounded");
        assert!(
            matches!(&first, GroupValue::Array(_)),
            "an array decodes to an array: {}",
            format!("{first:?}").chars().take(80).collect::<String>()
        );
        let GroupValue::Array(v) = &first else {
            unreachable!("asserted above")
        };
        let want = 3 + v.capacity() * std::mem::size_of::<GroupValue>() + 1;
        assert_eq!(
            decode_one(kind, &text, 3, &mut ByteBudget::new(want)).ok(),
            Some(GroupValue::Array(vec![GroupValue::Str("x".to_string())]))
        );
        let refused = decode_one(kind, &text, 3, &mut ByteBudget::new(want - 1));
        assert!(
            matches!(
                refused,
                Err(ReadError::QueryTooBroad(
                    TooBroadReason::ScanBudgetBytes { .. }
                ))
            ),
            "one byte under the charge is refused: {refused:?}"
        );
    }
}
