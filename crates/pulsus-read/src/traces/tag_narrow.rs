//! Lowers a §4.3 tag-values `q` to the span-level SQL terms a value read
//! can push down (issue #478, Part 2).
//!
//! **The rule this module exists to keep, stated once**: *tolerate what
//! the client cannot avoid sending; reject what indicates a genuine
//! fault.* The TraceQL editor's autocomplete sends the entire raw editor
//! text as `q` whenever the cursor sits at a value position, so `q`
//! arrives half-typed — `{span.http.status_code=` with an unbalanced
//! brace and a dangling operator — for every distinct prefix a user types
//! through. A client cannot avoid that, so an unlowerable `q` NEVER
//! errors here: it contributes no terms and the read widens.
//! [`narrowing_from_query`] is TOTAL — it has no error type, so nothing
//! at THIS layer can turn a `q` into a status code, whatever it contains.
//!
//! **What that does and does not say, because the absolute form of it is
//! false and a later reader will otherwise read a regression into two
//! correct rejections.** The property is: *a `q` that is well-formed
//! input and does not parse as TraceQL never errors.* Two classes are
//! rejected BELOW this layer, by the HTTP transport, and both are faults
//! a client can avoid — measured on this tree against the shipped binary,
//! on all three values routes:
//!
//! * **raw invalid UTF-8 in the request target** — a lone `0x80`, a bare
//!   `0xFF`, a truncated `0xC3` — is `400` before any handler runs. A
//!   client that percent-encodes the same bytes is served: `q=%80` and
//!   `q=` + `%80` × 4096 both answer `200`. So this rejects a malformed
//!   request line, not a `q` value.
//! * **an enormous `q`**: measured by bisection, 65,493 bytes answers
//!   `200` and 65,494 is refused — the 64 KiB request-target bound. The
//!   LENGTH is the stable part; the refusal's status is `414` or `431`
//!   depending on how the request bytes arrive, and both were observed
//!   for the SAME 524,194-byte request on two machines (`414` here,
//!   `431` in CI). Note the bound is TIGHTER than the §4.2 search
//!   surface's own 128 KiB expression cap
//!   (`traces_api::querytext::MAX_QUERY_EXPRESSION_BYTES`), so on this
//!   route the transport bound is what a client meets first.
//!
//! Neither is a shape an editor emits: it percent-encodes, and the text
//! it sends is the query a human is typing. A malformed `start`, a
//! `start` without an `end` and an inverted range are the other half of
//! the same rule and are rejected in
//! `traces_api::params::parse_range_params`.
//!
//! **Every drop widens, never narrows.** Terms are taken only from the
//! `&&` spine of the root spanset filter, where each is a positive
//! conjunct: dropping a conjunct of a conjunction matches a superset. No
//! term is ever taken from under a `||` or a `!`, which is what makes the
//! statement hold rather than merely being intended.

use pulsus_traceql::{
    BoolOp, FieldExpr, FieldOp, SpansetExpr, SpansetFilter, Value, parse, validate,
};

use crate::logql::escape::ch_string;

use super::filter::{AttrProbe, LeafEval, compile_leaf, physical_sql, value_pred_sql};

/// One conjunct of a `q` that a tag-value read can push down.
///
/// **Both variants carry ALREADY-RENDERED, already-escaped SQL**, and
/// that is what makes the builders that consume them infallible. Every
/// rendering that can fail — `physical_sql`'s checked regex escaper,
/// `value_pred_sql`'s — runs HERE, inside the one total function, where a
/// failure is just one more reason to drop a conjunct and widen. A term
/// that exists is a term that renders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NarrowTerm {
    /// A `trace_spans` column predicate, escaped by
    /// [`super::filter::physical_sql`].
    Physical(String),
    /// A `trace_attrs_idx` membership probe, rendered by
    /// [`super::tags_sql`] as a `(trace_id, span_id) IN (…)` semi-join.
    Attr(AttrTerm),
}

/// One attribute membership probe, with its `WHERE` fragments already
/// escaped — the `(key[, val][, scope])` prefix the search and metrics
/// paths probe on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttrTerm {
    /// `key = '…'`, escaped.
    pub key_sql: String,
    /// The value predicate (`val = '…'`, `val_num > 5`, `match(val, …)`,
    /// or the `1` of a key-existence probe), escaped.
    pub pred_sql: String,
    /// `scope = '…'` for a scoped selector, escaped; `None` for the
    /// unscoped `.attr` form, which probes the key in every scope.
    pub scope_sql: Option<String>,
}

impl AttrTerm {
    /// Renders a probe, or `None` when any fragment does not render —
    /// which drops the conjunct and widens.
    fn from_probe(probe: &AttrProbe) -> Option<Self> {
        Some(AttrTerm {
            key_sql: format!("key = {}", ch_string(&probe.key)),
            pred_sql: value_pred_sql(&probe.pred).ok()?,
            scope_sql: probe
                .scope
                .map(|scope| format!("scope = {}", ch_string(scope))),
        })
    }
}

/// The narrowing one `q` contributes. Empty means "do not narrow", and is
/// the ONLY outcome for a `q` this lowering cannot handle.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TagNarrowing {
    terms: Vec<NarrowTerm>,
}

impl TagNarrowing {
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    /// `pub` rather than crate-private (issue #478): the Tier-1 index
    /// gate in `crates/pulsus-read/tests/traces_tags_explain.rs` runs
    /// `EXPLAIN` over the EXACT SQL these terms produce, and an
    /// integration test sits outside this crate. Reconstructing the terms
    /// there would make the gate assert a plan for SQL the engine does
    /// not issue.
    pub fn terms(&self) -> &[NarrowTerm] {
        &self.terms
    }
}

/// At most this many terms are pushed; later ones are dropped, which can
/// only widen the answer. Deterministic: the first N in pre-order.
pub const TAG_NARROW_MAX_TERMS: usize = 8;

/// Lowers a raw `q` to pushable terms. **Total** — returns
/// [`TagNarrowing::default`] rather than an error for every input,
/// including the empty string.
///
/// Kept, in `&&`-spine pre-order:
///
/// * [`LeafEval::Physical`] → [`NarrowTerm::Physical`] via
///   [`physical_sql`];
/// * [`LeafEval::Attr`] with `negated: false` → [`NarrowTerm::Attr`].
///
/// Dropped (each drop widens, never narrows):
///
/// * a `parse` or `validate` failure — the whole `q`;
/// * a root spanset that is not [`SpansetExpr::Filter`];
/// * `SpansetFilter { body: None }` (`{}`), which contributes nothing;
/// * any subtree under `||`, `!`, or a non-comparison node;
/// * a comparison whose operands are not `<field> <op> <literal>`;
/// * [`LeafEval::Attr`] with `negated: true`, and every other
///   `LeafEval` — `TraceCtx`, `NestedSet`, `FieldCompare`, `BoolTruth`,
///   `Arith`, `Const`;
/// * a [`compile_leaf`] or [`physical_sql`] error;
/// * terms past [`TAG_NARROW_MAX_TERMS`].
///
/// Pipeline stages after the root filter are ignored; ignoring them can
/// only widen.
pub fn narrowing_from_query(raw_q: &str) -> TagNarrowing {
    let Ok(query) = parse(raw_q) else {
        return TagNarrowing::default();
    };
    if validate(&query).is_err() {
        return TagNarrowing::default();
    }
    let SpansetExpr::Filter(SpansetFilter { body: Some(body) }) = &query.spanset else {
        return TagNarrowing::default();
    };
    let mut terms = Vec::new();
    push_spine(body, &mut terms);
    TagNarrowing { terms }
}

/// Walks the `&&` spine in pre-order, pushing what each conjunct lowers
/// to. Anything that is not an `&&` node or a `<field> <op> <literal>`
/// comparison ends the walk down that branch — no term is taken from
/// under `||` or `!`, so the drop-widens property is structural rather
/// than checked per shape.
fn push_spine(expr: &FieldExpr, out: &mut Vec<NarrowTerm>) {
    if out.len() >= TAG_NARROW_MAX_TERMS {
        return;
    }
    match expr {
        FieldExpr::Binary {
            op: FieldOp::Bool(BoolOp::And),
            lhs,
            rhs,
        } => {
            push_spine(lhs, out);
            push_spine(rhs, out);
        }
        FieldExpr::Binary {
            op: FieldOp::Cmp(cmp),
            lhs,
            rhs,
        } => {
            let (FieldExpr::Field(field), FieldExpr::Literal(value)) = (lhs.as_ref(), rhs.as_ref())
            else {
                return;
            };
            if let Some(term) = lower_leaf(field, *cmp, value) {
                out.push(term);
            }
        }
        _ => {}
    }
}

/// One comparison → at most one term. Every failure path returns `None`,
/// which drops the conjunct and widens.
fn lower_leaf(
    field: &pulsus_traceql::Field,
    cmp: pulsus_traceql::ComparisonOp,
    value: &Value,
) -> Option<NarrowTerm> {
    let leaf = compile_leaf(field, cmp, value).ok()?;
    match leaf.eval {
        // A regex that does not compile is a PLAN error (400) on the
        // search path; here it is one more thing a half-typed `q` can
        // contain, so it drops.
        LeafEval::Physical(p) => Some(NarrowTerm::Physical(physical_sql(&p).ok()?)),
        LeafEval::Attr {
            probe,
            negated: false,
        } => Some(NarrowTerm::Attr(AttrTerm::from_probe(&probe)?)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn terms(q: &str) -> Vec<NarrowTerm> {
        narrowing_from_query(q).terms().to_vec()
    }

    fn physical(q: &str) -> Vec<String> {
        terms(q)
            .into_iter()
            .filter_map(|t| match t {
                NarrowTerm::Physical(sql) => Some(sql),
                NarrowTerm::Attr(_) => None,
            })
            .collect()
    }

    /// AC8a, the property that makes the interpretation layer total:
    /// every malformed shape an editor can emit lowers to NO terms and
    /// returns, rather than producing an error value there is nowhere to
    /// put. The twenty shapes are the ones the acceptance matrix issues
    /// as complete requests (Q-I..Q-V, Q-AD).
    #[test]
    fn an_unlowerable_query_contributes_no_terms() {
        for q in [
            "",
            "{span.http.status_code=5",
            "{resource.service.name=\"cart\" && span.http.status_code=",
            "{name=\"pay.charge\" && span.",
            "{span.http.status_code=200 &&",
            "{name=\"pay.charge\" && span.http.",
            "garbage",
            "{",
            "{span.",
            "{resource.service.name=\"",
            "}",
            "   ",
            "{} | ",
            "{.foo=}",
            "{bogus.x=\"y\"}",
            "{}",
            "{ }",
            "{resource.service.name=\"cart\"} >> {span.http.method=\"GET\"}",
            "{resource.service.name=\"cart\" || resource.service.name=\"pay\"}",
            "{!.a}",
        ] {
            assert!(
                narrowing_from_query(q).is_empty(),
                "{q:?} must contribute no terms"
            );
        }
    }

    /// Every PROPER PREFIX of every shape above is itself a plausible
    /// keystroke, and the function must return for each — the generated
    /// sweep the plan pairs with the type property.
    #[test]
    fn every_prefix_of_every_shape_returns() {
        for q in [
            "{resource.service.name=\"cart\" && span.http.method=\"GET\"}",
            "{name=~\".*charge.*\"}",
            "{duration > 1s} | select(span.http.method)",
            "{status=error && kind=server}",
            "{.http.method=\"GET\"}",
        ] {
            for end in 0..=q.len() {
                if !q.is_char_boundary(end) {
                    continue;
                }
                // The assertion is that this returns at all: a panic here
                // is the residue the type property cannot exclude.
                let _ = narrowing_from_query(&q[..end]);
            }
        }
    }

    #[test]
    fn a_physical_intrinsic_lowers_inline() {
        assert_eq!(physical("{name=\"pay.charge\"}"), ["name = 'pay.charge'"]);
        assert_eq!(
            physical("{resource.service.name=\"cart\"}"),
            ["service = 'cart'"]
        );
        assert_eq!(physical("{status=error}"), ["status_code = 2"]);
        assert_eq!(physical("{duration > 1s}"), ["duration_ns > 1000000000"]);
        assert_eq!(
            physical("{name=~\".*charge.*\"}"),
            ["match(name, '^(?:.*charge.*)$')"]
        );
    }

    #[test]
    fn an_attribute_conjunct_lowers_to_a_probe() {
        let terms = terms("{resource.service.name=\"cart\" && span.http.method=\"GET\"}");
        assert_eq!(terms.len(), 2);
        assert_eq!(
            terms[0],
            NarrowTerm::Physical("service = 'cart'".to_string())
        );
        let NarrowTerm::Attr(probe) = &terms[1] else {
            panic!("{terms:?}");
        };
        assert_eq!(probe.key_sql, "key = 'http.method'");
        assert_eq!(probe.pred_sql, "val = 'GET'");
        assert_eq!(probe.scope_sql.as_deref(), Some("scope = 'span'"));
    }

    /// An unscoped `.attr` probe carries NO scope, so it matches the key
    /// in every scope — the divergence ledgered as
    /// `traceql-tag-values-unscoped-attr-narrows-here`.
    #[test]
    fn an_unscoped_attribute_probe_omits_the_scope() {
        let terms = terms("{.http.method=\"GET\"}");
        let NarrowTerm::Attr(probe) = &terms[0] else {
            panic!("{terms:?}");
        };
        assert_eq!(probe.scope_sql, None);
    }

    /// A negated attribute conjunct drops: the probe would have to be
    /// inverted, and dropping widens where inverting could narrow
    /// wrongly.
    #[test]
    fn a_negated_attribute_conjunct_drops() {
        assert!(terms("{span.http.method!=\"GET\"}").is_empty());
    }

    /// Pipeline stages after the root filter are ignored, so the filter's
    /// own conjuncts still push.
    #[test]
    fn a_pipeline_stage_does_not_stop_the_root_filter_pushing() {
        assert_eq!(
            physical("{resource.service.name=\"cart\"} | select(span.http.method)"),
            ["service = 'cart'"]
        );
    }

    /// The cap is a count, and it takes the FIRST terms in pre-order.
    #[test]
    fn the_term_cap_keeps_the_first_terms_in_pre_order() {
        let q = format!(
            "{{{}}}",
            (0..TAG_NARROW_MAX_TERMS + 3)
                .map(|i| format!("span.k{i}=\"v\""))
                .collect::<Vec<_>>()
                .join(" && ")
        );
        let terms = terms(&q);
        assert_eq!(terms.len(), TAG_NARROW_MAX_TERMS);
        let NarrowTerm::Attr(first) = &terms[0] else {
            panic!("{terms:?}");
        };
        assert_eq!(first.key_sql, "key = 'k0'");
        let NarrowTerm::Attr(last) = &terms[TAG_NARROW_MAX_TERMS - 1] else {
            panic!("{terms:?}");
        };
        assert_eq!(
            last.key_sql,
            format!("key = 'k{}'", TAG_NARROW_MAX_TERMS - 1)
        );
    }
}
