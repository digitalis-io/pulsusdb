//! The structural operators on the search statement (issue #593): `>`,
//! `<`, `~` (part 1) and `>>`, `<<` (part 2), each plain, `!` and `&`, over
//! any spanset operands, as one per-span membership predicate.
//!
//! A spanset holding a structural operator compiles to `M(s)`: whether a
//! span `s` of the window is in the spanset's result, as the previous
//! search engine's `eval_spanset` decided it per trace. Each relation
//! reads the other side through a `(trace_id, …) IN (…)` subquery over the
//! same window, so the result is a predicate on one span row and every
//! search template serves it as its selector.
//!
//! In a sliced statement, every relation, every `&&` and each climb's
//! per-trace read reads only the `(bucket, trace_id)` pairs of `scope`, the
//! slice's candidates' keys (issue #602 part 2b). A relation holds within
//! one trace, so a trace's membership over all its buckets in the window is
//! its membership over the window. The leaves, `MembershipSql::
//! leaves`, are the OR of the filters: every member span matches one.

use pulsus_traceql::{BoolOp, FieldExpr, SpansetExpr, StructuralModifier, StructuralOp, Value};

use super::predicate::{PredicateCtx, SpanPredicate, compile_span_predicate_in};
use super::tracelevel::TraceLeaf;
use crate::traces::PlanError;

/// The all-zero parent: a root.
const ZERO: &str = "toFixedString('', 8)";

/// A climb's key condition: the `(bucket, trace_id)` prefix of the sort
/// key over the climb's traces' buckets, as the detail read keys its read.
/// `climb_cand`, so a statement's own `cand` stays visible to the scope.
const CAND_KEYS: &str = "(intDiv(start_ns, 300000000000), trace_id) IN \
     (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), climb_cand.1, climb_cand.2))))";

/// Whether `spanset` holds a structural operator anywhere.
pub fn holds_structural(spanset: &SpansetExpr) -> bool {
    match spanset {
        SpansetExpr::Filter(_) => false,
        SpansetExpr::Binary { lhs, rhs, .. } => holds_structural(lhs) || holds_structural(rhs),
        SpansetExpr::Structural { .. } => true,
    }
}

/// The default of `PULSUS_TRACEQL_MAX_DEPTH`: the most parent links one
/// climb of `>>` or `<<` follows.
pub const DEFAULT_MAX_DEPTH: u32 = 64;

/// The message a climb raises when a span still has a parent to follow
/// after `max_depth` links: a chain deeper than the bound. A cycle ends the
/// climb instead, at the first span it revisits, as today's walk ends. The
/// reader answers it `422 query_too_broad`.
pub const CLIMB_OVERFLOW: &str =
    "a structural climb has more parent links than PULSUS_TRACEQL_MAX_DEPTH (issue #593)";

/// A spanset's membership, whether it climbs (`>>`, `<<`), and its leaves:
/// the OR of its filters, which every member span matches.
pub struct MembershipSql {
    pub predicate: SpanPredicate,
    pub climbs: bool,
    pub leaves: String,
}

/// `M(spanset)` for a spanset holding a structural operator, over
/// `spans_table` in `ctx`'s window, every subquery reading only the
/// `(bucket, trace_id)` pairs of `scope` when one is given; a climb follows
/// at most `max_depth` parent links.
pub fn compile_membership_in(
    spanset: &SpansetExpr,
    ctx: &PredicateCtx<'_>,
    spans_table: &str,
    max_depth: u32,
    scope: Option<&str>,
) -> Result<MembershipSql, PlanError> {
    let mut c = Membership {
        ctx,
        spans_table,
        max_depth,
        scope: scope
            .map(|keys| format!("(intDiv(start_ns, 300000000000), trace_id) IN {keys} AND "))
            .unwrap_or_default(),
        leaves: Vec::new(),
        climbs: false,
        demands: Vec::new(),
        trace_leaves: Vec::new(),
        child_counts: false,
        numbered: 0,
        trace_values: Vec::new(),
    };
    let sql = c.member(spanset)?;
    // Every relation and every `&&` reads a window-bounded subquery.
    Ok(MembershipSql {
        predicate: SpanPredicate::composed(
            sql,
            Some(ctx.window),
            c.demands,
            c.trace_leaves,
            c.child_counts,
            c.numbered,
            c.trace_values,
        ),
        climbs: c.climbs,
        leaves: c.leaves.join(" OR "),
    })
}

struct Membership<'a, 'b> {
    ctx: &'a PredicateCtx<'b>,
    spans_table: &'a str,
    max_depth: u32,
    /// The scope's key condition, ending in ` AND `, or empty.
    scope: String,
    leaves: Vec<String>,
    climbs: bool,
    demands: Vec<String>,
    /// The filters' trace leaves, each once, and whether any reads child
    /// counts (issue #594 part 1): the statement's scalar defines them.
    trace_leaves: Vec<TraceLeaf>,
    child_counts: bool,
    /// The filters' numbered nested-set leaves (issue #594 part 2).
    numbered: usize,
    /// The filters' per-trace operand values, each once (issue #594 part 3).
    trace_values: Vec<super::tracelevel::TraceValue>,
}

impl Membership<'_, '_> {
    /// `SELECT <cols> FROM spans WHERE <window> AND (<m>)`.
    fn over(&self, cols: &str, m: &str, extra: &str) -> String {
        let w = self.ctx.window;
        format!(
            "SELECT {cols} FROM {} WHERE {}{} AND {} AND {} AND ({m}){extra}",
            self.spans_table,
            self.scope,
            w.span_time_clause(),
            w.span_bucket_clause(),
            w.span_day_clause(),
        )
    }

    fn member(&mut self, e: &SpansetExpr) -> Result<String, PlanError> {
        match e {
            SpansetExpr::Filter(f) => {
                let body = f
                    .body
                    .clone()
                    .unwrap_or(FieldExpr::Literal(Value::Bool(true)));
                let p = compile_span_predicate_in(&body, self.ctx)?;
                for d in p.demand_messages() {
                    if !self.demands.contains(d) {
                        self.demands.push(d.clone());
                    }
                }
                for l in p.trace_leaves() {
                    if !self.trace_leaves.contains(l) {
                        self.trace_leaves.push(l.clone());
                    }
                }
                self.child_counts |= p.reads_child_counts();
                self.numbered += p.numbered_leaves();
                for v in p.trace_values() {
                    if !self.trace_values.contains(v) {
                        self.trace_values.push(*v);
                    }
                }
                let leaf = format!("({})", p.sql());
                self.leaves.push(leaf.clone());
                Ok(leaf)
            }
            SpansetExpr::Binary { op, lhs, rhs } => {
                let l = self.member(lhs)?;
                let r = self.member(rhs)?;
                Ok(match op {
                    BoolOp::Or => format!("({l} OR {r})"),
                    BoolOp::And => format!(
                        "(({l} OR {r}) AND trace_id IN ({}) AND trace_id IN ({}))",
                        self.over("trace_id", &l, ""),
                        self.over("trace_id", &r, "")
                    ),
                })
            }
            SpansetExpr::Structural {
                op,
                modifier,
                lhs,
                rhs,
            } => {
                let a = self.member(lhs)?;
                let b = self.member(rhs)?;
                let rhs_part = self.relation(*op, &a, &b)?;
                Ok(match modifier {
                    StructuralModifier::Plain => format!("({b} AND {rhs_part})"),
                    StructuralModifier::Negated => format!("({b} AND NOT {rhs_part})"),
                    StructuralModifier::Union => {
                        let lhs_part = self.relation(mirror(*op), &b, &a)?;
                        format!("(({b} AND {rhs_part}) OR ({a} AND {lhs_part}))")
                    }
                })
            }
        }
    }

    /// Whether the span holds `op` with some span of `seed`, as the previous
    /// search engine's `rel_children` / `rel_parents` / `rel_siblings`
    /// decided it with `seed` as the seed set — the candidate's own
    /// membership is the caller's.
    fn relation(&mut self, op: StructuralOp, seed: &str, cand: &str) -> Result<String, PlanError> {
        Ok(match op {
            // The candidate's parent is a seed span: not a root, not itself.
            StructuralOp::Child => format!(
                "(parent_span_id != {ZERO} AND parent_span_id != span_id \
                 AND (trace_id, parent_span_id) IN ({}))",
                self.over("trace_id, span_id", seed, "")
            ),
            // The candidate is the parent of a seed span that is not a root
            // and not its own parent.
            StructuralOp::Parent => format!(
                "((trace_id, span_id) IN ({}))",
                self.over(
                    "trace_id, parent_span_id",
                    seed,
                    &format!(" AND parent_span_id != {ZERO} AND parent_span_id != span_id")
                )
            ),
            // A seed span other than the candidate shares its non-root
            // parent: two seed spans under it, or one and the candidate
            // not a seed span.
            StructuralOp::Sibling => {
                let under = format!(" AND parent_span_id != {ZERO}");
                format!(
                    "(parent_span_id != {ZERO} AND ((trace_id, parent_span_id) IN ({}) \
                     OR (NOT {seed} AND (trace_id, parent_span_id) IN ({}))))",
                    self.over(
                        "trace_id, parent_span_id",
                        seed,
                        &format!("{under} GROUP BY trace_id, parent_span_id HAVING count() >= 2")
                    ),
                    self.over("trace_id, parent_span_id", seed, &under)
                )
            }
            // The candidate's chain of parents reaches a seed span: climb
            // from the candidates in traces holding a seed span, keep the
            // climbs whose reached span is a seed.
            StructuralOp::Descendant => {
                let climb = self.climb(cand, seed, true);
                format!(
                    "((trace_id, span_id) IN ({climb} \
                     SELECT trace_id, start FROM climb \
                     WHERE (throwIf(depth > {d}, '{CLIMB_OVERFLOW}') + found) = 1))",
                    d = self.max_depth,
                )
            }
            // The candidate is on a seed span's chain of parents: climb
            // from the seed spans in traces holding a candidate.
            StructuralOp::Ancestor => {
                let climb = self.climb(seed, cand, false);
                format!(
                    "((trace_id, span_id) IN ({climb} \
                     SELECT trace_id, cur FROM climb WHERE throwIf(depth > {d}, '{CLIMB_OVERFLOW}') = 0))",
                    d = self.max_depth,
                )
            }
        })
    }
}

impl Membership<'_, '_> {
    /// The window's three clauses on `start_ns`.
    fn window(&self) -> String {
        let w = self.ctx.window;
        format!(
            "{} AND {} AND {}",
            w.span_time_clause(),
            w.span_bucket_clause(),
            w.span_day_clause()
        )
    }

    /// `WITH RECURSIVE climb_cand, climb`. `climb_cand` is, per trace of
    /// the window (of the scope, when given) holding a span `start` keeps
    /// and a span `other` keeps, the
    /// five-minute buckets of its spans, so every read of the climb is a
    /// key read of those traces ([`CAND_KEYS`]). `climb` follows, from
    /// each `start` span, its parent links one at a time, as
    /// `(trace_id, start, cur, depth)`: `cur` is the span `depth` links
    /// above `start`. A link is followed while the span is stored in the
    /// window, its parent is neither the root marker nor itself, and the
    /// parent is not already on the climb's path, as today's
    /// `rel_ancestors` follows them. Rows run to `max_depth + 1`, so a row
    /// past the bound exists exactly when a climb had a link left.
    /// With `stop`, a climb ends at the first `other` span it reaches,
    /// `found = 1`: the start is decided, and no row past it can raise.
    fn climb(&mut self, start: &str, other: &str, stop: bool) -> String {
        self.climbs = true;
        let found = |t: &str, p: &str| {
            if stop {
                format!(
                    "toUInt8(({t}, {p}) IN (SELECT trace_id, span_id FROM {} WHERE {CAND_KEYS} AND {} AND ({other})))",
                    self.spans_table,
                    self.window()
                )
            } else {
                "toUInt8(0)".to_string()
            }
        };
        let (found_base, found_step) = (
            found("trace_id", "parent_span_id"),
            found("x.trace_id", "x.parent_span_id"),
        );
        format!(
            "WITH RECURSIVE (SELECT (groupArrayIf(trace_id, both), groupArrayIf(keys, both)) \
             FROM (SELECT trace_id, groupUniqArray(intDiv(start_ns, 300000000000)) AS keys, \
             countIf({start}) > 0 AND countIf({other}) > 0 AS both \
             FROM {spans} WHERE {scope}{window} GROUP BY trace_id)) AS climb_cand, \
             climb AS (\
             SELECT trace_id, span_id AS start, parent_span_id AS cur, toUInt32(1) AS depth, {found_base} AS found, CAST([], 'Array(FixedString(8))') AS path FROM {spans} \
             WHERE {CAND_KEYS} AND {window} AND ({start}) AND parent_span_id != {ZERO} AND parent_span_id != span_id \
             UNION ALL \
             SELECT c.trace_id, c.start, x.parent_span_id, c.depth + 1, {found_step}, arrayPushBack(c.path, c.cur) \
             FROM (SELECT trace_id, span_id, parent_span_id FROM {spans} WHERE {CAND_KEYS} AND {window}) AS x \
             INNER JOIN climb AS c ON x.trace_id = c.trace_id AND x.span_id = c.cur \
             WHERE c.found = 0 AND c.depth <= {d} AND x.parent_span_id != {ZERO} AND x.parent_span_id != x.span_id \
             AND NOT has(c.path, x.parent_span_id))",
            spans = self.spans_table,
            window = self.window(),
            d = self.max_depth,
            scope = self.scope,
        )
    }
}

/// The relation whose seed is the other side: the union's left-hand
/// participants (`lhs_participants`).
fn mirror(op: StructuralOp) -> StructuralOp {
    match op {
        StructuralOp::Child => StructuralOp::Parent,
        StructuralOp::Parent => StructuralOp::Child,
        StructuralOp::Descendant => StructuralOp::Ancestor,
        StructuralOp::Ancestor => StructuralOp::Descendant,
        StructuralOp::Sibling => StructuralOp::Sibling,
    }
}
