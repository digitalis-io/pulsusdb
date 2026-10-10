//! Operator-named attribute indexes (issue #595 part 2).
//!
//! `PULSUS_TRACEQL_INDEXED_ATTRIBUTES` names span and span-event attribute
//! keys; `schema/schema.sh` gives `spans` one bloom-filter index per key
//! over every string the key holds
//! ([`pulsus_clickhouse::json_column::span_attr_index_expr`]). A filter
//! whose body requires `key = "<string>"` on such a key carries
//! `indexHint(has(<that expression>, '<string>'))`: `indexHint` is always
//! true and never evaluated, so the answer is the filter's own, and the
//! index skips the granules holding no such string.

use pulsus_clickhouse::json_column::{event_attr_index_expr, span_attr_index_expr};
use pulsus_config::{IndexedScope, parse_indexed_attribute};
use pulsus_traceql::{AttrScope, BoolOp, ComparisonOp, Field, FieldExpr, FieldOp, Value};

use crate::logql::escape;

/// One indexed attribute: its scope and its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedAttr {
    pub scope: IndexedScope,
    pub key: String,
}

impl IndexedAttr {
    /// The validated configuration list, as attributes. An item the
    /// configuration check would refuse is dropped.
    pub fn from_config(items: &[String]) -> Vec<IndexedAttr> {
        items
            .iter()
            .filter_map(|item| parse_indexed_attribute(item).ok())
            .map(|(scope, key)| IndexedAttr { scope, key })
            .collect()
    }

    /// The index's expression, the text the schema renders.
    pub fn expr(&self) -> String {
        match self.scope {
            IndexedScope::Span => span_attr_index_expr(&self.key),
            IndexedScope::Event => event_attr_index_expr(&self.key),
        }
    }
}

/// The hints `body` earns: one per `key = "<string>"` among its top-level
/// `&&` operands, on an indexed key of the same scope. A leaf under `||`
/// or `!`, any other operator, and any other literal type earn none.
pub fn index_hints(body: &FieldExpr, indexed: &[IndexedAttr]) -> Vec<String> {
    let mut out = Vec::new();
    collect(body, indexed, &mut out);
    out
}

fn collect(e: &FieldExpr, indexed: &[IndexedAttr], out: &mut Vec<String>) {
    let FieldExpr::Binary { op, lhs, rhs } = e else {
        return;
    };
    match op {
        FieldOp::Bool(BoolOp::And) => {
            collect(lhs, indexed, out);
            collect(rhs, indexed, out);
        }
        FieldOp::Cmp(ComparisonOp::Eq) => {
            let pair = match (lhs.as_ref(), rhs.as_ref()) {
                (FieldExpr::Field(f), FieldExpr::Literal(Value::String(v)))
                | (FieldExpr::Literal(Value::String(v)), FieldExpr::Field(f)) => Some((f, v)),
                _ => None,
            };
            let Some((Field::Attribute { scope, key }, v)) = pair else {
                return;
            };
            let scope = match scope {
                AttrScope::Span => IndexedScope::Span,
                AttrScope::Event => IndexedScope::Event,
                _ => return,
            };
            if let Some(a) = indexed.iter().find(|a| a.scope == scope && &a.key == key) {
                let hint = format!("indexHint(has({}, {}))", a.expr(), escape::ch_string(v));
                if !out.contains(&hint) {
                    out.push(hint);
                }
            }
        }
        _ => {}
    }
}
