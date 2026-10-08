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
use crate::logql::error::ReadError;
use crate::logql::escape;
use crate::traces::PlanError;
use crate::traces::exec::ByteBudget;
use crate::traces::search_eval::{GroupValue, ProjectedAttribute};
use crate::traces::search_plan::WireKey;
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
    /// Issue #592 part 3: per projected attribute, the condition under
    /// which a span holds it off its typed path (`search::off_path_at`).
    off_path: Vec<String>,
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
        Projection {
            groups: Vec::new(),
            off_path: Vec::new(),
        }
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
        let projected = format!("arrayFilter(x -> x.1 != 0, [{}])", groups.join(", "));
        if self.off_path.is_empty() {
            return projected;
        }
        // Issue #592 part 3: a span holding a projected attribute off its
        // typed path hands the request to today's engine.
        format!(
            "if(throwIf({}, {}) = 0, {projected}, CAST([], 'Array(Tuple(UInt8, String, String))'))",
            self.off_path.join(" OR "),
            escape::ch_string(super::search::SELECT_OFF_PATH_DEMAND)
        )
    }

    /// Whether a projected attribute can be held off its typed path, so the
    /// statement carries [`super::search::SELECT_OFF_PATH_DEMAND`].
    pub(crate) fn has_off_path(&self) -> bool {
        !self.off_path.is_empty()
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
                    if let Field::Attribute { scope, key } = field {
                        let off = super::search::off_path_at(*scope, key, ctx);
                        if !self.off_path.contains(&off) {
                            self.off_path.push(off);
                        }
                    }
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
            Field::Intrinsic(Intrinsic::Duration | Intrinsic::SpanId | Intrinsic::TraceId) => {
                Ok(())
            }
            Field::Intrinsic(_) => Err(PlanError::UnsupportedField(format!(
                "select({field}) is not supported by the search statement (issue #592)"
            ))),
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
    /// its `name` when a `name` group projected one. Each value is charged
    /// before it is built, by today's engine's rule (`build_summary`): a
    /// name its length; an attribute its key's length plus its string
    /// payload, a number or boolean its key alone; an array its key plus
    /// the length of its cut render, learned before the render is built.
    /// A value that does not parse as its kind is a decode error, never a
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
            let value = match p.kind.as_str() {
                "Int64" => GroupValue::Int(
                    p.value
                        .parse::<i64>()
                        .map_err(|e| decode(format!("projected Int64 {:?}: {e}", p.value)))?,
                ),
                "Float64" => GroupValue::Double(
                    p.value
                        .parse::<f64>()
                        .map_err(|e| decode(format!("projected Float64 {:?}: {e}", p.value)))?
                        .to_bits(),
                ),
                "Bool" => GroupValue::Bool(p.value == "true"),
                k if k.starts_with("Array(") => {
                    let values = array_values(&p.value).map_err(decode)?;
                    budget.charge(group.key_len + rendered_len(&values))?;
                    let render =
                        serde_json::to_string(&values).map_err(|e| decode(e.to_string()))?;
                    attributes.push(ProjectedAttribute::new(
                        &group.field,
                        GroupValue::Str(ceiling(render)),
                    ));
                    continue;
                }
                _ => {
                    budget.charge(group.key_len + p.value.len())?;
                    attributes.push(ProjectedAttribute::new(
                        &group.field,
                        GroupValue::Str(p.value.clone()),
                    ));
                    continue;
                }
            };
            budget.charge(group.key_len + value.payload_bytes())?;
            attributes.push(ProjectedAttribute::new(&group.field, value));
        }
        Ok(name)
    }
}

/// The length of `ceiling(serde_json::to_string(values))`, learned
/// without building it (issue #591 part 3): the render is serialized into
/// a counter of its bytes, and of the bytes of its first
/// `CUT_CODE_POINTS` code points, which is the cut's length.
fn rendered_len(values: &[serde_json::Value]) -> usize {
    struct Counter {
        bytes: usize,
        code_points: usize,
        cut_bytes: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            for &b in buf {
                // A code point starts at every byte that is not a UTF-8
                // continuation byte.
                if b & 0xC0 != 0x80 {
                    self.code_points += 1;
                }
                if self.code_points <= CUT_CODE_POINTS {
                    self.cut_bytes += 1;
                }
            }
            self.bytes += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter {
        bytes: 0,
        code_points: 0,
        cut_bytes: 0,
    };
    serde_json::to_writer(&mut counter, values).expect("a JSON value always serializes");
    if counter.bytes <= CEILING_BYTES {
        counter.bytes
    } else {
        counter.cut_bytes
    }
}

/// Issue #592 part 2: an array value's text as the writer stored it — the
/// JSON of its elements, each by its own type — from the statement's
/// `[[type, text], …]`. A `by()` key is not cut, as today's group key is
/// not.
pub(crate) fn rendered_array(text: &str) -> Result<String, String> {
    serde_json::to_string(&array_values(text)?).map_err(|e| e.to_string())
}

/// The length of [`rendered_array`]'s text, learned without building it,
/// so the decode charges it first.
pub(crate) fn rendered_array_len(text: &str) -> Result<usize, String> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, &array_values(text)?).map_err(|e| e.to_string())?;
    Ok(counter.0)
}

/// Issue #592 part 2: how the search statement reads one `by()` key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupKeySql {
    /// The key's value as text: a column's keyword text, or a span
    /// attribute's stored text, an array as the JSON of its typed elements.
    pub value: String,
    /// The value's stored type, for a span attribute; a column key has
    /// none.
    pub value_type: Option<String>,
    /// The condition under which the key's value is held off its own path
    /// — a key-value list flattened into paths under the key, or bytes in
    /// `attrs_other` — which the statement cannot read as the key's value.
    pub off_path: Option<String>,
}

/// The `by()` keys the search statement serves (issue #592 part 2): `name`,
/// `status`, `kind`, `resource.service.name` and a `span.` attribute.
/// `None` for any other key, which today's engine answers.
pub fn group_key_sql(field: &Field) -> Option<GroupKeySql> {
    let column = |sql: &str| GroupKeySql {
        value: sql.to_string(),
        value_type: None,
        off_path: None,
    };
    match field {
        Field::Intrinsic(Intrinsic::Name | Intrinsic::Status | Intrinsic::Kind) => {
            match place_of(field) {
                Place::Column(sql) => Some(column(sql)),
                _ => None,
            }
        }
        Field::Attribute {
            scope: AttrScope::Resource,
            key,
        } if key == "service.name" => Some(column("toString(service)")),
        Field::Attribute {
            scope: AttrScope::Span,
            key,
        } => {
            let p = attr_path("attrs", key);
            Some(GroupKeySql {
                value: format!(
                    "if(startsWith(dynamicType({p}), 'Array'), toJSONString(arrayMap(x -> \
                     (toString(dynamicType(x)), toString(x)), CAST({p}, 'Array(Dynamic)'))), \
                     toString({p}))"
                ),
                value_type: Some(format!("toString(dynamicType({p}))")),
                off_path: Some(format!(
                    "dynamicType({p}) = 'None' AND (position(attrs_other, {}) > 0 OR \
                     arrayExists(x -> startsWith(x, {}), JSONAllPaths(attrs)))",
                    escape::ch_string(key),
                    escape::ch_string(&format!(
                        "{}.",
                        pulsus_clickhouse::json_column::escape_json_path(key)
                    ))
                )),
            })
        }
        _ => None,
    }
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

/// Issue #592 part 3: an aggregate argument's located element, as the
/// projection reads a field's stored value — a `span.` attribute's own, or
/// the unscoped `.k` at the first scope of the chain that holds it — as
/// `(text, stored type)` SQL. `None` for any other field.
pub(crate) fn aggregate_argument_sql(
    field: &Field,
    ctx: &PredicateCtx<'_>,
) -> Result<Option<(String, String)>, PlanError> {
    match field {
        Field::Attribute {
            scope: AttrScope::Span | AttrScope::Unscoped,
            ..
        } => match stored_value(field, ctx)? {
            Projects::Value { value, kind } => Ok(Some((value, kind))),
            Projects::Nothing => Ok(None),
        },
        _ => Ok(None),
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
