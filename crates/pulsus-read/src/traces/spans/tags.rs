//! The two catalog reads of tag discovery (issue #598 part 1): tag names
//! from `tag_names`, unnarrowed tag values from `tag_values`.
//!
//! Pure SQL builders: pre-escaped fragments → `String`, no `ChClient`, no
//! I/O. Callers pre-escape `scope_literal`/`key_literal` via
//! [`crate::logql::escape::ch_string`] (quotes included); that is the
//! injection boundary, not this module.
//!
//! The table names are private constants of this module, not inputs, so
//! no caller can put a table, an alias or a subquery into the `FROM`
//! clause. Both catalogs are replicated to every node, so there is no
//! `_dist` table to name. `DISTINCT` collapses a not-yet-merged duplicate
//! (both tables are ordered by their whole row, so the duplicate is
//! byte-identical), never `FINAL`. The `LIMIT` is the caller's cap + 1,
//! the truncation probe.
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

use pulsus_traceql::{BoolOp, FieldExpr, FieldOp, SpansetExpr, SpansetFilter, parse, validate};

use super::predicate::{PredicateCtx, SpanPredicate, attr_path, compile_span_predicate_in};
use super::projection::ceiling_sql;
use super::tracelevel::TraceLeaf;
use crate::traces::filter::byte_cap_expr;
use crate::traces::window_sql::WindowSql;

/// The five scopes the writer catalogues for sender-supplied ATTRIBUTES,
/// ascending (issue #475). This constant is the single source of truth
/// for the whole scope surface: the SQL `IN` list below, the `scope=`
/// accept list (`traces_api::params::parse_tags_params`) and the `{tag}`
/// prefix set (`parse_tag_lookup`) all derive from it.
pub const ATTR_SCOPES: [&str; 5] = ["event", "instrumentation", "link", "resource", "span"];

/// The rendered SQL `IN` list for [`ATTR_SCOPES`]. A literal, so the
/// builders stay pure concatenation; pinned against the constant by
/// `attr_scopes_in_list_matches_the_constant`.
pub(crate) const ATTR_SCOPES_IN: &str = "('event', 'instrumentation', 'link', 'resource', 'span')";

const TAG_NAMES_TABLE: &str = "tag_names";
const TAG_VALUES_TABLE: &str = "tag_values";

/// The `GET /api/traces/v1/tags` read: distinct `(scope, key)` pairs from
/// `tag_names`, confined to one scope (a `(scope)` primary-key prefix) or
/// to [`ATTR_SCOPES`] (an `IN` list on the same leading column).
pub fn tag_names_sql(scope_literal: Option<&str>, limit: usize) -> String {
    let mut sql = format!("SELECT DISTINCT scope, key\nFROM {TAG_NAMES_TABLE}\n");
    match scope_literal {
        Some(scope) => sql.push_str(&format!("WHERE scope = {scope}\n")),
        None => sql.push_str(&format!("WHERE scope IN {ATTR_SCOPES_IN}\n")),
    }
    sql.push_str(&format!("ORDER BY scope, key\nLIMIT {limit}"));
    sql
}

/// The unnarrowed `GET /api/traces/v1/tag/{tag}/values` read: distinct
/// `(value, val_type)` pairs for one key from `tag_values`, confined to
/// one scope (a `(scope, key)` prefix) or to [`ATTR_SCOPES`]. `value` is
/// projected as `val`, the field every caller decodes. The pair, not the
/// value alone (issue #476): one key can hold a string `'8080'` and an
/// int `8080`, which the wire reports as two entries, and the `LIMIT`
/// probe counts pairs.
pub fn tag_values_sql(key_literal: &str, scope_literal: Option<&str>, limit: usize) -> String {
    let mut sql = format!(
        "SELECT DISTINCT value AS val, val_type\nFROM {TAG_VALUES_TABLE}\nWHERE key = {key_literal}"
    );
    match scope_literal {
        Some(scope) => sql.push_str(&format!(" AND scope = {scope}")),
        None => sql.push_str(&format!(" AND scope IN {ATTR_SCOPES_IN}")),
    }
    sql.push_str(&format!("\nORDER BY val, val_type\nLIMIT {limit}"));
    sql
}

// ---------------------------------------------------------------------
// Issue #598 part 2: narrowed values and span names from the span store
// ---------------------------------------------------------------------

/// At most this many conjuncts of a `q` are kept; later ones are
/// dropped, which can only widen the answer. The first ones in pre-order.
pub const TAG_NARROW_MAX_TERMS: usize = 8;

/// The ceiling on `R`, the number of newest spans a span-name or
/// narrowed-values read reaches back to (D12): the engine's default.
pub const TAG_STORE_READ_ROWS: u64 = 10_000_000;

/// `R` for a statement making `table_reads` reads of `spans` and
/// `traces`: max(1, min(`ceiling`, `scan_budget_rows` / (2 × `table_reads`))),
/// so every read taking about `R` rows plus a share of one bucket stays
/// inside the scan budget.
pub fn cut_rows(ceiling: u64, scan_budget_rows: u64, table_reads: u64) -> u64 {
    let per_read = scan_budget_rows / (2 * table_reads.max(1));
    ceiling.min(per_read).max(1)
}

/// What a store read lists: span names, or one key's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagShape {
    Names,
    Values(ValueScope),
}

/// The statement's reads of `spans` and `traces`, the cut included (the
/// `resources` read is not counted): 2 for span names and every scoped
/// value read, 3 for an unscoped key, and 2 more when the predicate reads
/// a trace-level leaf.
pub fn table_reads(shape: TagShape, n: &TagNarrowing) -> u64 {
    let base = match shape {
        TagShape::Values(ValueScope::Unscoped) => 3,
        _ => 2,
    };
    let trace = n.predicate().is_some_and(|p| !p.trace_leaves().is_empty());
    base + if trace { 2 } else { 0 }
}

/// The 5-minute bucket that holds the window's `r`-th newest span: an
/// ordered read of `start_ns` alone.
fn cut_sql(spans: &str, w: WindowSql, r: u64) -> String {
    format!(
        "(SELECT min(b) FROM (SELECT intDiv(start_ns, 300000000000) AS b FROM {spans} WHERE {} AND {} AND {} ORDER BY b DESC LIMIT {r})) AS cut",
        w.span_time_clause(),
        w.span_bucket_clause(),
        w.span_day_clause()
    )
}

/// A span is in the buckets from the cut on.
const NEWER: &str = "intDiv(start_ns, 300000000000) >= cut";

/// The narrowing one `q` contributes to a store read.
///
/// `has_body` says whether `q` parsed and validated to a root filter with
/// a body: such a `q` takes the windowed store read, with no predicate if
/// every conjunct dropped. Without one, a values read is part 1's catalog
/// read.
#[derive(Debug, Clone, Default)]
pub struct TagNarrowing {
    has_body: bool,
    pred: Option<SpanPredicate>,
    span_pred: Option<SpanPredicate>,
    terms: usize,
}

impl TagNarrowing {
    /// Whether `q` was a root filter with a body.
    pub fn has_body(&self) -> bool {
        self.has_body
    }

    /// The one predicate the statement carries: the `&&` of the kept
    /// conjuncts.
    pub fn predicate(&self) -> Option<&SpanPredicate> {
        self.pred.as_ref()
    }

    /// The `&&` of the kept conjuncts that read no trace-level leaf, when
    /// some kept conjunct does: what the trace-level prefix restricts its
    /// trace ids by.
    pub fn span_predicate(&self) -> Option<&SpanPredicate> {
        self.span_pred.as_ref()
    }

    /// How many conjuncts were kept.
    pub fn terms(&self) -> usize {
        self.terms
    }
}

/// Lowers a raw `q`. **Total**: every input, the empty string and a
/// half-typed query included, gives a narrowing, never an error.
///
/// Each conjunct of the root filter's `&&` spine — any subtree, `||` and
/// `!` included — is compiled on its own to decide whether it is kept:
/// kept when it compiles and reads neither the nested-set numbering,
/// `span:childCount` nor a per-trace operand value. Trace-level leaves
/// are kept. At most [`TAG_NARROW_MAX_TERMS`], in pre-order. The kept
/// conjuncts are then compiled together, once, so every `!` demand sits in
/// front of the whole expression. Every drop is a conjunct of a
/// conjunction, so it widens inside the window and never outside it.
pub fn narrowing_from_query(raw_q: &str, ctx: &PredicateCtx<'_>) -> TagNarrowing {
    let Ok(query) = parse(raw_q) else {
        return TagNarrowing::default();
    };
    if validate(&query).is_err() {
        return TagNarrowing::default();
    }
    let SpansetExpr::Filter(SpansetFilter { body: Some(body) }) = &query.spanset else {
        return TagNarrowing::default();
    };
    let mut conjuncts: Vec<&FieldExpr> = Vec::new();
    spine(body, &mut conjuncts);
    let mut kept: Vec<(&FieldExpr, bool)> = Vec::new();
    for c in conjuncts {
        if kept.len() >= TAG_NARROW_MAX_TERMS {
            break;
        }
        let Ok(p) = compile_span_predicate_in(c, ctx) else {
            continue;
        };
        if p.reads_numbering() || p.reads_child_counts() || !p.trace_values().is_empty() {
            continue;
        }
        kept.push((c, !p.trace_leaves().is_empty()));
    }
    let compile_all = |exprs: &[&FieldExpr]| -> Option<SpanPredicate> {
        let joined = and_of(exprs)?;
        compile_span_predicate_in(&joined, ctx).ok()
    };
    let all: Vec<&FieldExpr> = kept.iter().map(|(e, _)| *e).collect();
    let pred = compile_all(&all);
    let span_pred = if kept.iter().any(|(_, trace)| *trace) {
        let span_only: Vec<&FieldExpr> = kept
            .iter()
            .filter(|(_, trace)| !*trace)
            .map(|(e, _)| *e)
            .collect();
        compile_all(&span_only)
    } else {
        None
    };
    TagNarrowing {
        has_body: true,
        terms: if pred.is_some() { kept.len() } else { 0 },
        pred,
        span_pred,
    }
}

/// The `&&` spine's conjuncts, in pre-order.
fn spine<'a>(expr: &'a FieldExpr, out: &mut Vec<&'a FieldExpr>) {
    match expr {
        FieldExpr::Binary {
            op: FieldOp::Bool(BoolOp::And),
            lhs,
            rhs,
        } => {
            spine(lhs, out);
            spine(rhs, out);
        }
        other => out.push(other),
    }
}

/// `a && b && …`, left-folded; `None` for no operand.
fn and_of(exprs: &[&FieldExpr]) -> Option<FieldExpr> {
    let mut it = exprs.iter();
    let first = (*it.next()?).clone();
    Some(it.fold(first, |acc, e| FieldExpr::Binary {
        op: FieldOp::Bool(BoolOp::And),
        lhs: Box::new(acc),
        rhs: Box::new((*e).clone()),
    }))
}

/// Where a values read takes its values from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueScope {
    Span,
    Resource,
    Instrumentation,
    Event,
    Link,
    Unscoped,
}

impl ValueScope {
    /// The scope of a `{tag}` prefix; `None` is a bare key.
    pub fn from_attr_scope(scope: Option<&str>) -> Self {
        match scope {
            Some("span") => Self::Span,
            Some("resource") => Self::Resource,
            Some("instrumentation") => Self::Instrumentation,
            Some("event") => Self::Event,
            Some("link") => Self::Link,
            _ => Self::Unscoped,
        }
    }
}

/// Every `(text, type)` a stored attribute value lists, as an array: a
/// scalar is one element, an all-string array each string, an all-int,
/// all-double or all-bool array each element typed by its arm; anything
/// else nothing. `d` is substituted textually.
fn elems(d: &str) -> String {
    format!(
        "multiIf(dynamicType({d}) = 'String', [(assumeNotNull(dynamicElement({d}, 'String')), 'string')], \
dynamicType({d}) = 'Int64', [(toString(assumeNotNull(dynamicElement({d}, 'Int64'))), 'int')], \
dynamicType({d}) = 'Float64', [(toString(assumeNotNull(dynamicElement({d}, 'Float64'))), 'float')], \
dynamicType({d}) = 'Bool', [(toString(assumeNotNull(dynamicElement({d}, 'Bool'))), 'bool')], \
dynamicType({d}) = 'Array(Nullable(String))', arrayMap(x -> (assumeNotNull(x), 'string'), dynamicElement({d}, 'Array(Nullable(String))')), \
dynamicType({d}) = 'Array(Nullable(Int64))', arrayMap(x -> (toString(assumeNotNull(x)), 'int'), dynamicElement({d}, 'Array(Nullable(Int64))')), \
dynamicType({d}) = 'Array(Nullable(Float64))', arrayMap(x -> (toString(assumeNotNull(x)), 'float'), dynamicElement({d}, 'Array(Nullable(Float64))')), \
dynamicType({d}) = 'Array(Nullable(Bool))', arrayMap(x -> (toString(assumeNotNull(x)), 'bool'), dynamicElement({d}, 'Array(Nullable(Bool))')), \
CAST([], 'Array(Tuple(String, String))'))"
    )
}

/// The span's `service.name`, when it is stored as a string.
const SERVICE_ARR: &str = "if(service_type = 'string', [(CAST(service, 'String'), 'string')], CAST([], 'Array(Tuple(String, String))'))";

/// The `AND (pred)` line of a read, or nothing.
fn pred_line(n: &TagNarrowing, indent: &str) -> String {
    match n.predicate() {
        Some(p) => format!("\n{indent}AND ({})", p.sql()),
        None => String::new(),
    }
}

/// The statement's `WITH`: the cut alone, or with the trace-level prefix.
fn with_clause(traces: &str, spans: &str, w: WindowSql, n: &TagNarrowing, r: u64) -> String {
    let tl = trace_prefix(traces, spans, w, n, r);
    if tl.is_empty() {
        format!("WITH {}\n", cut_sql(spans, w, r))
    } else {
        tl
    }
}

/// The `AND (pred)` of a one-line subquery, or nothing.
fn pred_inline(n: &TagNarrowing) -> String {
    match n.predicate() {
        Some(p) => format!(" AND ({})", p.sql()),
        None => String::new(),
    }
}

/// The trace-level prefix (`TL`): each trace leaf's trace set, built from
/// `traces` over the window's days ±1 and restricted to the trace ids of
/// the spans the read covers, the span conjuncts applied. Empty when the
/// predicate reads no trace leaf.
fn trace_prefix(traces: &str, spans: &str, w: WindowSql, n: &TagNarrowing, r: u64) -> String {
    let Some(p) = n.predicate() else {
        return String::new();
    };
    let mut leaves: Vec<&TraceLeaf> = Vec::new();
    for l in p.trace_leaves() {
        if !leaves.iter().any(|x| x.alias == l.alias) {
            leaves.push(l);
        }
    }
    if leaves.is_empty() {
        return String::new();
    }
    let elements = leaves
        .iter()
        .map(|l| format!("groupArrayIf(trace_id, {})", l.cond))
        .collect::<Vec<_>>()
        .join(", ");
    let aliases = leaves
        .iter()
        .enumerate()
        .map(|(i, l)| format!("per_trace.{} AS {}", i + 1, l.alias))
        .collect::<Vec<_>>()
        .join(",\n     ");
    let spred = n.span_predicate().map_or("1", |p| p.sql());
    format!(
        "WITH {cut},\n     (SELECT tuple({elements})\n      FROM (SELECT trace_id, max(end_ns) - min(start_ns) AS duration, min(root) AS r,\n                   if(r.1 = 0, {service}, '') AS root_service, if(r.1 = 0, {name}, '') AS root_name\n            FROM {traces}\n            WHERE {tdays}\n              AND trace_id GLOBAL IN (SELECT trace_id FROM {spans} WHERE {time} AND {bucket} AND {day} AND {NEWER} AND ({spred}))\n            GROUP BY trace_id)) AS per_trace,\n     {aliases}\n",
        cut = cut_sql(spans, w, r),
        service = ceiling_sql("r.4"),
        name = ceiling_sql("r.5"),
        tdays = w.per_trace_day_clause(),
        time = w.span_time_clause(),
        bucket = w.span_bucket_clause(),
        day = w.span_day_clause(),
    )
}

/// The span-name read (N): the capped names of the spans starting in the
/// window, newest bucket first, narrowed by `n`'s predicate.
pub fn name_values_sql(
    spans: &str,
    traces: &str,
    w: WindowSql,
    n: &TagNarrowing,
    r: u64,
    limit: usize,
) -> String {
    format!(
        "{with}SELECT DISTINCT {cap} AS val\nFROM {spans}\nWHERE {time}\n  AND {bucket}\n  AND {day}\n  AND {NEWER}{pred}\nORDER BY val\nLIMIT {limit}",
        with = with_clause(traces, spans, w, n, r),
        cap = byte_cap_expr("name"),
        time = w.span_time_clause(),
        bucket = w.span_bucket_clause(),
        day = w.span_day_clause(),
        pred = pred_line(n, "  "),
    )
}

/// The narrowed values read for one attribute key (V-S, V-R, V-U): the
/// values on the spans the query matches in the window, newest bucket
/// first. `key` is raw; it is escaped here, into a JSON path identifier.
#[allow(clippy::too_many_arguments)]
pub fn narrowed_values_sql(
    spans: &str,
    resources: &str,
    traces: &str,
    scope: ValueScope,
    key: &str,
    w: WindowSql,
    n: &TagNarrowing,
    r: u64,
    limit: usize,
) -> String {
    let prefix = with_clause(traces, spans, w, n, r);
    let time = w.span_time_clause();
    let bucket = w.span_bucket_clause();
    let day = w.span_day_clause();
    let span_arr = |root: &str| elems(&attr_path(root, key));
    let set_arr = |root: &str| {
        format!(
            "arrayFlatten(arrayMap(d -> {}, {}))",
            elems("d"),
            attr_path(root, key)
        )
    };
    let resource_ids = format!(
        "(service, resource_id) IN (SELECT service, resource_id FROM {spans} WHERE {time} AND {bucket} AND {day} AND {NEWER}{})",
        pred_inline(n)
    );
    let from_spans = |arr: String| {
        format!(
            "{prefix}SELECT DISTINCT val, val_type\nFROM (SELECT tupleElement(e, 1) AS val, tupleElement(e, 2) AS val_type\n  FROM {spans}\n  ARRAY JOIN {arr} AS e\n  WHERE {time}\n    AND {bucket}\n    AND {day}\n    AND {NEWER}{pred})\nORDER BY val, val_type\nLIMIT {limit}",
            pred = pred_line(n, "    "),
        )
    };
    match scope {
        ValueScope::Span => from_spans(span_arr("attrs")),
        ValueScope::Instrumentation => from_spans(span_arr("scope_attrs")),
        ValueScope::Event => from_spans(set_arr("events.attrs")),
        ValueScope::Link => from_spans(set_arr("links.attrs")),
        ValueScope::Resource if key == "service.name" => from_spans(SERVICE_ARR.to_string()),
        ValueScope::Resource => format!(
            "{prefix}SELECT DISTINCT tupleElement(e, 1) AS val, tupleElement(e, 2) AS val_type\nFROM {resources}\nARRAY JOIN {arr} AS e\nWHERE {rday}\n  AND {resource_ids}\nORDER BY val, val_type\nLIMIT {limit}",
            arr = elems(&attr_path("attrs", key)),
            rday = w.resources_day_clause(),
        ),
        ValueScope::Unscoped => {
            let mut parts = vec![
                span_arr("attrs"),
                span_arr("scope_attrs"),
                set_arr("events.attrs"),
                set_arr("links.attrs"),
            ];
            if key == "service.name" {
                parts.push(SERVICE_ARR.to_string());
            }
            format!(
                "{prefix}SELECT DISTINCT val, val_type\nFROM (SELECT tupleElement(e, 1) AS val, tupleElement(e, 2) AS val_type\n  FROM {spans}\n  ARRAY JOIN arrayConcat({all}) AS e\n  WHERE {time} AND {bucket} AND {day} AND {NEWER}{pred}\n  UNION ALL\n  SELECT tupleElement(e, 1) AS val, tupleElement(e, 2) AS val_type\n  FROM {resources}\n  ARRAY JOIN {rarr} AS e\n  WHERE {rday} AND {resource_ids})\nORDER BY val, val_type\nLIMIT {limit}",
                all = parts.join(", "),
                pred = pred_inline(n),
                rarr = elems(&attr_path("attrs", key)),
                rday = w.resources_day_clause(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::logql::escape::ch_string;
    use crate::traces::{TAG_NAMES_MAX, TAG_VALUES_MAX};

    /// All four documented forms, byte for byte (the `LIMIT` is the cap
    /// + 1 truncation probe).
    #[test]
    fn scoped_tag_names_sql_is_byte_exact() {
        assert_eq!(
            tag_names_sql(Some(&ch_string("resource")), TAG_NAMES_MAX + 1),
            "SELECT DISTINCT scope, key\n\
             FROM tag_names\n\
             WHERE scope = 'resource'\n\
             ORDER BY scope, key\n\
             LIMIT 10001"
        );
    }

    #[test]
    fn unscoped_tag_names_sql_is_byte_exact() {
        assert_eq!(
            tag_names_sql(None, TAG_NAMES_MAX + 1),
            "SELECT DISTINCT scope, key\n\
             FROM tag_names\n\
             WHERE scope IN ('event', 'instrumentation', 'link', 'resource', 'span')\n\
             ORDER BY scope, key\n\
             LIMIT 10001"
        );
    }

    #[test]
    fn scoped_tag_values_sql_is_byte_exact() {
        assert_eq!(
            tag_values_sql(
                &ch_string("service.name"),
                Some(&ch_string("resource")),
                TAG_VALUES_MAX + 1
            ),
            "SELECT DISTINCT value AS val, val_type\n\
             FROM tag_values\n\
             WHERE key = 'service.name' AND scope = 'resource'\n\
             ORDER BY val, val_type\n\
             LIMIT 1001"
        );
    }

    #[test]
    fn unscoped_tag_values_sql_is_byte_exact() {
        assert_eq!(
            tag_values_sql(&ch_string("service.name"), None, TAG_VALUES_MAX + 1),
            "SELECT DISTINCT value AS val, val_type\n\
             FROM tag_values\n\
             WHERE key = 'service.name' AND scope IN ('event', 'instrumentation', 'link', \
             'resource', 'span')\n\
             ORDER BY val, val_type\n\
             LIMIT 1001"
        );
    }

    /// The injection boundary holds: a hostile key arrives pre-escaped
    /// and stays inside its string literal.
    #[test]
    fn a_pre_escaped_hostile_key_stays_a_string_literal() {
        let sql = tag_values_sql(&ch_string("k'; DROP TABLE x; --"), None, TAG_VALUES_MAX + 1);
        assert!(
            sql.contains("WHERE key = 'k\\'; DROP TABLE x; --'"),
            "{sql}"
        );
    }

    /// Issue #475 AC17: the rendered `IN` list is the constant, not a
    /// second hand-maintained list.
    #[test]
    fn attr_scopes_in_list_matches_the_constant() {
        let rendered = ATTR_SCOPES
            .iter()
            .map(|s| ch_string(s))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(ATTR_SCOPES_IN, format!("({rendered})"));
    }

    /// No argument list makes either catalog builder name a `_dist`
    /// table: both catalogs are replicated to every node (issue #475).
    #[test]
    fn neither_catalog_builder_can_name_a_dist_table() {
        let scope = ch_string("resource");
        let key = ch_string("service.name");
        for sql in [
            tag_names_sql(Some(&scope), TAG_NAMES_MAX + 1),
            tag_names_sql(None, TAG_NAMES_MAX + 1),
            tag_values_sql(&key, Some(&scope), TAG_VALUES_MAX + 1),
            tag_values_sql(&key, None, TAG_VALUES_MAX + 1),
        ] {
            assert!(!sql.is_empty() && !sql.contains("_dist"), "{sql}");
        }
    }

    // ---- issue #598 part 2 -------------------------------------------

    const S: i64 = 1_700_000_000_000_000_000;
    const E: i64 = S + 3_600_000_000_000;

    fn window() -> WindowSql {
        WindowSql::start_closed_end_open(S, E)
    }

    fn ctx() -> PredicateCtx<'static> {
        PredicateCtx {
            window: window(),
            spans_table: "spans",
            resources_table: "resources",
            indexed: &[],
        }
    }

    fn narrow(q: &str) -> TagNarrowing {
        narrowing_from_query(q, &ctx())
    }

    /// §4.1's element function, typed out.
    fn elems_text(d: &str) -> String {
        "multiIf(dynamicType(D) = 'String', [(assumeNotNull(dynamicElement(D, 'String')), 'string')], dynamicType(D) = 'Int64', [(toString(assumeNotNull(dynamicElement(D, 'Int64'))), 'int')], dynamicType(D) = 'Float64', [(toString(assumeNotNull(dynamicElement(D, 'Float64'))), 'float')], dynamicType(D) = 'Bool', [(toString(assumeNotNull(dynamicElement(D, 'Bool'))), 'bool')], dynamicType(D) = 'Array(Nullable(String))', arrayMap(x -> (assumeNotNull(x), 'string'), dynamicElement(D, 'Array(Nullable(String))')), dynamicType(D) = 'Array(Nullable(Int64))', arrayMap(x -> (toString(assumeNotNull(x)), 'int'), dynamicElement(D, 'Array(Nullable(Int64))')), dynamicType(D) = 'Array(Nullable(Float64))', arrayMap(x -> (toString(assumeNotNull(x)), 'float'), dynamicElement(D, 'Array(Nullable(Float64))')), dynamicType(D) = 'Array(Nullable(Bool))', arrayMap(x -> (toString(assumeNotNull(x)), 'bool'), dynamicElement(D, 'Array(Nullable(Bool))')), CAST([], 'Array(Tuple(String, String))'))".replace('D', d)
    }

    const TIME: &str = "start_ns >= 1700000000000000000 AND start_ns < 1700003600000000000";
    const BUCKET: &str = "intDiv(start_ns, 300000000000) BETWEEN intDiv(1700000000000000000, 300000000000) AND intDiv(1700003599999999999, 300000000000)";
    const DAY: &str = "toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') >= toDate('2023-11-14') AND toDate(fromUnixTimestamp64Nano(start_ns), 'UTC') <= toDate('2023-11-14')";
    const RDAY: &str = "day >= toDate(fromUnixTimestamp64Nano(1700000000000000000), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1700003599999999999), 'UTC')";
    const R: u64 = 123_456;
    const NEWER: &str = "intDiv(start_ns, 300000000000) >= cut";

    fn cut() -> String {
        format!(
            "(SELECT min(b) FROM (SELECT intDiv(start_ns, 300000000000) AS b FROM spans WHERE {TIME} AND {BUCKET} AND {DAY} ORDER BY b DESC LIMIT 123456)) AS cut"
        )
    }
    const PRED: &str = "(service_type = 'string' AND service = 'cart')";
    const CAP: &str = "if(length(name) <= 8192, name, substringUTF8(name, 1, 2048))";
    const SERVICE: &str = "if(service_type = 'string', [(CAST(service, 'String'), 'string')], CAST([], 'Array(Tuple(String, String))'))";

    fn v_s(arr: &str) -> String {
        format!(
            "WITH {}\nSELECT DISTINCT val, val_type\nFROM (SELECT tupleElement(e, 1) AS val, tupleElement(e, 2) AS val_type\n  FROM spans\n  ARRAY JOIN {arr} AS e\n  WHERE {TIME}\n    AND {BUCKET}\n    AND {DAY}\n    AND {NEWER}\n    AND ({PRED}))\nORDER BY val, val_type\nLIMIT 1001",
            cut()
        )
    }

    /// U1: §4's statements, byte for byte.
    #[test]
    fn the_name_and_value_statements_are_byte_exact() {
        let n = narrow(r#"{resource.service.name="cart"}"#);
        let none = narrow(r#"{span:childCount > 1}"#);
        let w = window();
        let k = "`http%2Eroute`";
        let span_arr = elems_text(&format!("attrs.{k}"));
        let scope_arr = elems_text(&format!("scope_attrs.{k}"));
        let event_arr = format!(
            "arrayFlatten(arrayMap(d -> {}, events.attrs.{k}))",
            elems_text("d")
        );
        let link_arr = format!(
            "arrayFlatten(arrayMap(d -> {}, links.attrs.{k}))",
            elems_text("d")
        );
        let res_arr = elems_text(&format!("attrs.{k}"));
        let ids = format!(
            "(service, resource_id) IN (SELECT service, resource_id FROM spans WHERE {TIME} AND {BUCKET} AND {DAY} AND {NEWER} AND ({PRED}))"
        );
        // N, unnarrowed and narrowed.
        assert_eq!(
            name_values_sql("spans", "traces", w, &none, R, 1001),
            format!(
                "WITH {}\nSELECT DISTINCT {CAP} AS val\nFROM spans\nWHERE {TIME}\n  AND {BUCKET}\n  AND {DAY}\n  AND {NEWER}\nORDER BY val\nLIMIT 1001",
                cut()
            )
        );
        assert_eq!(
            name_values_sql("spans", "traces", w, &n, R, 1001),
            format!(
                "WITH {}\nSELECT DISTINCT {CAP} AS val\nFROM spans\nWHERE {TIME}\n  AND {BUCKET}\n  AND {DAY}\n  AND {NEWER}\n  AND ({PRED})\nORDER BY val\nLIMIT 1001",
                cut()
            )
        );
        // V-S, for each of the five arrays.
        let vs = |scope: ValueScope, key: &str| {
            narrowed_values_sql("spans", "resources", "traces", scope, key, w, &n, R, 1001)
        };
        assert_eq!(vs(ValueScope::Span, "http.route"), v_s(&span_arr));
        assert_eq!(
            vs(ValueScope::Instrumentation, "http.route"),
            v_s(&scope_arr)
        );
        assert_eq!(vs(ValueScope::Event, "http.route"), v_s(&event_arr));
        assert_eq!(vs(ValueScope::Link, "http.route"), v_s(&link_arr));
        assert_eq!(vs(ValueScope::Resource, "service.name"), v_s(SERVICE));
        // V-R.
        assert_eq!(
            vs(ValueScope::Resource, "http.route"),
            format!(
                "WITH {}\nSELECT DISTINCT tupleElement(e, 1) AS val, tupleElement(e, 2) AS val_type\nFROM resources\nARRAY JOIN {res_arr} AS e\nWHERE {RDAY}\n  AND {ids}\nORDER BY val, val_type\nLIMIT 1001",
                cut()
            )
        );
        // V-U, and V-U for `service.name`.
        let v_u = |all: &str, res: &str| {
            format!(
                "WITH {}\nSELECT DISTINCT val, val_type\nFROM (SELECT tupleElement(e, 1) AS val, tupleElement(e, 2) AS val_type\n  FROM spans\n  ARRAY JOIN arrayConcat({all}) AS e\n  WHERE {TIME} AND {BUCKET} AND {DAY} AND {NEWER} AND ({PRED})\n  UNION ALL\n  SELECT tupleElement(e, 1) AS val, tupleElement(e, 2) AS val_type\n  FROM resources\n  ARRAY JOIN {res} AS e\n  WHERE {RDAY} AND {ids})\nORDER BY val, val_type\nLIMIT 1001",
                cut()
            )
        };
        assert_eq!(
            vs(ValueScope::Unscoped, "http.route"),
            v_u(
                &format!("{span_arr}, {scope_arr}, {event_arr}, {link_arr}"),
                &res_arr
            )
        );
        let sk = "`service%2Ename`";
        let s_span = elems_text(&format!("attrs.{sk}"));
        let s_scope = elems_text(&format!("scope_attrs.{sk}"));
        let s_event = format!(
            "arrayFlatten(arrayMap(d -> {}, events.attrs.{sk}))",
            elems_text("d")
        );
        let s_link = format!(
            "arrayFlatten(arrayMap(d -> {}, links.attrs.{sk}))",
            elems_text("d")
        );
        assert_eq!(
            vs(ValueScope::Unscoped, "service.name"),
            v_u(
                &format!("{s_span}, {s_scope}, {s_event}, {s_link}, {SERVICE}"),
                &elems_text(&format!("attrs.{sk}"))
            )
        );
    }

    /// U2: each conjunct is compiled whole; the kept ones are compiled
    /// together, once.
    #[test]
    fn a_lowering_takes_each_conjunct_whole() {
        for (q, terms) in [
            (r#"{span.a="x" || span.a="y"}"#, 1),
            (r#"{span.k!="v"}"#, 1),
            (r#"{!(span.k="v")}"#, 1),
            (r#"{!.a}"#, 1),
            (
                r#"{resource.service.name="cart" && span:childCount > 1}"#,
                1,
            ),
            (r#"{trace:duration > 1s}"#, 1),
            (r#"{rootName = "x" && span.a="y"}"#, 2),
            (r#"{span:childCount > 1}"#, 0),
            (r#"{nestedSetLeft > 2}"#, 0),
            (r#"{nestedSetParent < 0}"#, 1),
            (r#"{span.a="x"} | select(span.b)"#, 1),
        ] {
            let n = narrow(q);
            assert!(n.has_body(), "{q}: a body");
            assert_eq!(n.terms(), terms, "{q}");
        }
        assert!(
            narrow(r#"{!.a}"#)
                .predicate()
                .expect("a predicate")
                .sql()
                .starts_with("(throwIf("),
            "a `!` over a value is a demand"
        );
        let td = narrow(r#"{trace:duration > 1s}"#);
        assert_eq!(td.predicate().expect("a predicate").trace_leaves().len(), 1);
        assert!(td.span_predicate().is_none(), "no span conjunct");
        let mixed = narrow(r#"{rootName = "x" && span.a="y"}"#);
        let alone = narrow(r#"{span.a="y"}"#);
        assert_eq!(
            mixed.span_predicate().expect("the span conjunct").sql(),
            alone.predicate().expect("a predicate").sql()
        );
        for q in ["garbage", "{}"] {
            assert!(!narrow(q).has_body(), "{q}: no body");
        }
        let eleven = (0..11)
            .map(|i| format!("span.c{i}=\"v{i}\""))
            .collect::<Vec<_>>()
            .join(" && ");
        let n = narrow(&format!("{{{eleven}}}"));
        assert_eq!(n.terms(), 8);
        let sql = n.predicate().expect("a predicate").sql().to_string();
        for i in 0..8 {
            assert!(sql.contains(&format!("'v{i}'")), "conjunct {i} kept: {sql}");
        }
        for i in 8..11 {
            assert!(
                !sql.contains(&format!("'v{i}'")),
                "conjunct {i} dropped: {sql}"
            );
        }
        let both = narrow(r#"{span.a="x" && !span.b}"#);
        let sql = both.predicate().expect("a predicate").sql().to_string();
        assert!(sql.starts_with("(throwIf("), "{sql}");
        let body = &sql[sql.find("toUInt8(").expect("a body")..];
        assert!(!body.contains("throwIf"), "every demand in front: {sql}");
    }

    /// U3, copied from `tag_narrow.rs` (its originals go with that module
    /// in part 2b): the shapes an editor emits half
    /// typed contribute no conjunct, and every prefix returns.
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
        ] {
            assert_eq!(narrow(q).terms(), 0, "{q:?} must contribute no terms");
        }
    }

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
                let _ = narrow(&q[..end]);
            }
        }
    }

    /// U4: `start == end` selects nothing.
    #[test]
    fn a_zero_width_window_selects_nothing() {
        let w = WindowSql::start_closed_end_open(S, S);
        let sql = name_values_sql("spans", "traces", w, &TagNarrowing::default(), R, 1001);
        assert!(
            sql.contains(&format!("start_ns >= {S} AND start_ns < {S}")),
            "{sql}"
        );
    }

    /// U7: the trace-level prefix, byte for byte.
    #[test]
    fn the_trace_prefix_is_byte_exact() {
        let n = narrow(r#"{traceDuration > 1s && resource.service.name="cart"}"#);
        let alias = n.predicate().expect("a predicate").trace_leaves()[0]
            .alias
            .clone();
        let cap = |c: &str| format!("if(length({c}) <= 8192, {c}, substringUTF8({c}, 1, 2048))");
        assert_eq!(
            trace_prefix("traces", "spans", window(), &n, R),
            format!(
                "WITH {cut},\n     (SELECT tuple(groupArrayIf(trace_id, duration > 1000000000))\n      FROM (SELECT trace_id, max(end_ns) - min(start_ns) AS duration, min(root) AS r,\n                   if(r.1 = 0, {svc}, '') AS root_service, if(r.1 = 0, {name}, '') AS root_name\n            FROM traces\n            WHERE day >= toDate(fromUnixTimestamp64Nano(1699913600000000000), 'UTC') AND day <= toDate(fromUnixTimestamp64Nano(1700089999999999999), 'UTC')\n              AND trace_id GLOBAL IN (SELECT trace_id FROM spans WHERE {TIME} AND {BUCKET} AND {DAY} AND {NEWER} AND ({PRED}))\n            GROUP BY trace_id)) AS per_trace,\n     per_trace.1 AS {alias}\n",
                cut = cut(),
                svc = cap("r.4"),
                name = cap("r.5"),
            )
        );
        assert_eq!(
            trace_prefix("traces", "spans", window(), &narrow(r#"{span.a="x"}"#), R),
            ""
        );
    }
}
